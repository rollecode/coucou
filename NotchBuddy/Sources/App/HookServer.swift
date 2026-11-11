import Foundation
import Darwin
import AppKit
import SwiftUI
import CryptoKit

// MARK: - HookServer
// Listens on a Unix domain socket for events from nb-hook (Claude Code hooks).
// Thread-safe: socket I/O on background threads, state updates dispatched to main queue.

final class HookServer: @unchecked Sendable {
    static let shared = HookServer()

    // Support directory paths
    static var supportDir: URL {
        FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent("NotchBuddy")
    }
    static var socketPath: String {
        #if APPSTORE
        // Container home root keeps path ≤ 103 bytes (sun_path limit on macOS is 104 incl. NUL)
        // /Users/louis/Library/Containers/fr.louisraille.Coucou/Data/nb.sock = 66 bytes ✓
        return NSHomeDirectory() + "/nb.sock"
        #else
        return supportDir.appendingPathComponent("nb.sock").path
        #endif
    }
    // hookScriptPath is only used by the non-App Store build.
    // App Store build derives the command from the panel-selected claudeURL in buildHooksData(claudeURL:).
    static var hookScriptPath: String { supportDir.appendingPathComponent("nb-hook").path }

    // No approval blocking state — notch is notification-only, user answers in VS Code

    private static let maxPayload = 1_048_576          // 1 MB — reject oversized messages
    private static let receiveTimeoutSeconds: Int = 5   // SO_RCVTIMEO on client sockets
    private static let maxConnections = 32              // concurrent connection ceiling
    private static let maxRemoteConnections = 8         // all remote hosts together: never starve nb.sock
    private static let maxAgentPills = 12               // dynamic coucou_agent pills at once

    private var serverFD: Int32 = -1
    private let connectionLock = NSLock()
    private var connectionCount = 0
    private var remoteConnectionCount = 0

    // Pending approvals, FIFO. Main actor only. Each entry owns its fd, disconnect source and timeout.
    private var approvalQueue = ApprovalQueue()
    private var approvalGuard = ClickGuard()
    private var approvalResources: [UUID: ApprovalResources] = [:]

    /// AskUserQuestions waiting for an answer, oldest first, one per session; the head is on
    /// the card. Each one holds its nb-hook connection open until it is answered or dropped.
    private var questions = QuestionQueue()
    /// One read source per waiting question: it notices the client hanging up, and its
    /// cancel handler is the only place the question's fd is closed.
    private var questionSources: [UUID: any DispatchSourceRead] = [:]
    private var shownQuestion: UUID? = nil            // the question the card shows
    @MainActor var hasRealPendingApproval: Bool { !approvalQueue.isEmpty }
    /// True when a real nb-hook connection is holding a question fd open.
    @MainActor var hasRealPendingQuestion: Bool { !questions.isEmpty }
    private var focusBeforeQuestion: String? = nil    // saved focus to restore after question
    private var activeSessionId: String? = nil        // current Claude Code session
    private var focusBeforeApproval: String? = nil    // saved focus to restore after approval

    /// A turn that ended (Stop) while background work was still in flight.
    private struct BackgroundWait {
        let pillId: String
        var subagents: Int
        var others: Int
        var total: Int { subagents + others }
    }
    /// Keyed by session id. While set, the pill stays "working" and events fired inside
    /// background subagents (they carry agent_id) no longer overwrite the finished turn.
    private var backgroundWaits: [String: BackgroundWait] = [:]
    /// Bumped whenever a pill gets busy again, so a pending "finished → idle" reset is skipped.
    private var idleResetGeneration: [String: Int] = [:]
    private var focusBeforeAlert: String? = nil       // saved focus while another pill's card shows
    private var alertPill: String? = nil              // the pill that took the focus for its alert

    private init() {}

    // MARK: - Approval fd helpers

    private struct ApprovalResources {
        let source: any DispatchSourceRead
        let timeout: DispatchWorkItem
    }

    /// Where the user is expected to answer for this pill ("Cursor", "Codex", the Claude host…).
    @MainActor
    private func agentLabel(for pillId: String) -> String {
        switch PillCatalog.basePillId(pillId) {
        case "agent_cursor":  return "Cursor"
        case "agent_codex":   return "Codex"
        case "agent_copilot": return "Copilot CLI"
        case "agent_muse":    return "Muse Code"
        case "agent_hermes":  return "Hermes"
        case "agent_antigravity": return "Antigravity"
        default:              return claudeHostName(pill: pillId)
        }
    }

    @MainActor
    private func handledNote(for pillId: String) -> String { "Handled in \(agentLabel(for: pillId))." }

    @MainActor
    private func waitingNote(for pillId: String) -> String { "Still waiting in \(agentLabel(for: pillId))." }

    @MainActor
    private func handledNote(pill: String) -> String { handledNote(for: pill) }

    @MainActor
    private func waitingNote(pill: String) -> String { waitingNote(for: pill) }

    /// True while the peer has not closed its end. A request whose socket is dead must never be shown.
    private static func isAlive(fd: Int32) -> Bool {
        var byte: UInt8 = 0
        let n = recv(fd, &byte, 1, MSG_PEEK | MSG_DONTWAIT)
        if n == 0 { return false }
        if n < 0 { return errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR }
        return true
    }

    /// Removes one entry from the queue and tears down its timeout and source.
    /// The source's cancel handler closes the fd — never close it here.
    /// Returns the removed entry and its source so the caller can still write a decision first.
    @MainActor
    private func detachApproval(id: UUID) -> (entry: QueuedApproval, source: (any DispatchSourceRead)?)? {
        guard let entry = approvalQueue.remove(id: id) else { return nil }
        nbLog("Approval \(entry.tool) closed after \(Int(Date().timeIntervalSince(entry.received))) s")
        let res = approvalResources.removeValue(forKey: id)
        res?.timeout.cancel()
        return (entry, res?.source)
    }

    /// Drops these entries only (client gone, timed out, or handled elsewhere). Silent for the others.
    /// `note` is shown only when the queue becomes empty.
    @MainActor
    private func dismissApprovals(ids: [UUID], note: String) {
        var lastPill: String?
        for id in ids {
            guard let removed = detachApproval(id: id) else { continue }
            removed.source?.cancel()
            lastPill = removed.entry.pillId
        }
        guard let pillId = lastPill else { return }
        refreshApprovalCard(releasedPill: pillId, emptyNote: note)
    }

    /// Brings the card and pill states in line with the queue after any removal.
    /// Non-empty: the next live request becomes the card. Empty: release the island as before.
    @MainActor
    private func refreshApprovalCard(releasedPill: String, emptyNote: String?) {
        let state = AppState.shared
        // Never show a request whose socket is already dead: drop it and move on.
        while let head = approvalQueue.head, !Self.isAlive(fd: head.fd) {
            nbLog("Dropped dead approval \(head.tool) [\(head.pillId)]")
            if let removed = detachApproval(id: head.id) { removed.source?.cancel() }
        }
        // A pill with no request left goes back to work.
        if !approvalQueue.hasEntries(forPill: releasedPill) {
            state.updateTask(id: releasedPill, state: questionWaits(onPill: releasedPill) ? .question : .working)
            clearPillBadge(id: releasedPill)
        }
        guard let head = approvalQueue.head else {
            state.pendingApproval = nil
            state.isPinned = false
            // Restore focus to the pill that was focused before the approval card appeared.
            if let prev = focusBeforeApproval {
                focusBeforeApproval = nil
                if state.focusId == releasedPill, state.tasks.contains(where: { $0.id == prev }) {
                    withAnimation(.spring(response: 0.5, dampingFraction: 0.72)) { state.focusId = prev }
                }
            }
            if questions.head != nil {
                showQuestionHead()
                return
            }
            if let note = emptyNote {
                state.noteMessage = note
                state.view = .note
                DispatchQueue.main.asyncAfter(deadline: .now() + 3) {
                    NotificationCenter.default.post(name: .islandCollapse, object: nil)
                }
            } else {
                state.view = state.tasks.isEmpty ? .empty : .overview
            }
            return
        }
        showApproval(head)
    }

    /// Makes `entry` (the queue head) the card on screen.
    @MainActor
    private func showApproval(_ entry: QueuedApproval) {
        let state = AppState.shared
        if !entry.isChat {
            if entry.pillId.hasPrefix(PillCatalog.sessionPillPrefix) {
                upsertSessionTask(id: entry.pillId, projectName: entry.projectName, cwd: entry.cwd,
                                  bundleId: entry.bundleId, tty: entry.tty)
            } else {
                upsertWorkspaceTask(id: entry.pillId, projectName: entry.projectName, cwd: entry.cwd,
                                    hostApp: entry.hostApp, bundleId: entry.bundleId, tty: entry.tty)
            }
        }
        state.updateTask(id: entry.pillId, state: .approval)
        state.pendingApproval = ApprovalInfo(
            sessionId: entry.sessionId, tool: entry.tool, command: entry.command,
            inputKey: entry.inputKey, pillId: entry.pillId, id: entry.id,
            position: 1, total: approvalQueue.count,
            sourceLabel: approvalQueue.hasMixedSources ? agentLabel(for: entry.pillId) : nil)
        state.isPinned = true
        approvalGuard.show(entry.id, at: ProcessInfo.processInfo.systemUptime)
        if state.focusId != entry.pillId {
            withAnimation(.spring(response: 0.5, dampingFraction: 0.72)) { state.focusId = entry.pillId }
        }
        if state.mode == .expanded { state.view = .approval }
    }

    // MARK: - Question queue helpers

    /// True while a question from a session behind `pill` waits for its answer. The pill keeps
    /// its question state meanwhile: the iPhone and the Live Activity read the question from it.
    @MainActor
    private func questionWaits(onPill pill: String) -> Bool {
        questions.hasEntries(forPill: pill)
    }

    /// Puts the oldest waiting question on the card. When the card already shows it, only
    /// the list the card and the iPhone read is refreshed (no sound, no focus change).
    @MainActor
    private func showQuestionHead() {
        let state = AppState.shared
        state.waitingQuestions = questions.entries
        guard let head = questions.head else { return }
        let needsDisplay = shownQuestion != head.id || state.view != .question
        guard needsDisplay else { return }
        shownQuestion = head.id
        state.pendingQuestion = head.question
        state.isPinned = true
        if focusBeforeQuestion == nil { focusBeforeQuestion = state.focusId }
        withAnimation(.spring(response: 0.5, dampingFraction: 0.72)) { state.focusId = head.pillId }
        expandIfNeeded(to: .question)
    }

    /// Takes one question out of the queue. With `answer`, the line is written to its nb-hook
    /// first; without, the connection just closes and the agent asks again itself. The cancel
    /// handler of its source closes the fd — never close it directly. The next question then
    /// takes the card; when none is left the card closes.
    @MainActor
    private func removeQuestion(id: UUID, answer json: String? = nil) {
        guard let entry = questions.remove(id: id) else { return }
        let source = questionSources.removeValue(forKey: id)
        if let json {
            let fd = entry.fd
            Task.detached { [weak self] in
                // Write while the fd is still valid, then cancel → the cancel handler closes it.
                self?.sendLine(fd: fd, text: json)
                DispatchQueue.main.async { source?.cancel() }
            }
        } else {
            source?.cancel()
        }

        let state = AppState.shared
        if !questions.hasEntries(forPill: entry.pillId) {
            let approval = approvalQueue.hasEntries(forPill: entry.pillId)
            state.updateTask(id: entry.pillId, state: approval ? .approval : .working)
            if !approval { clearPillBadge(id: entry.pillId) }
        }
        if questions.head != nil {
            showQuestionHead()
            return
        }
        state.waitingQuestions = []
        closeQuestionCard(pill: entry.pillId)
    }

    /// No question left: close the card and give the focus back, or return to
    /// an approval waiting behind it (#851).
    @MainActor
    private func closeQuestionCard(pill: String?) {
        let state = AppState.shared
        shownQuestion = nil
        state.pendingQuestion = nil
        state.isPinned = false
        if let prev = focusBeforeQuestion {
            focusBeforeQuestion = nil
            if let pill, state.focusId == pill, state.tasks.contains(where: { $0.id == prev }) {
                withAnimation(.spring(response: 0.5, dampingFraction: 0.72)) { state.focusId = prev }
            }
        }
        if let head = approvalQueue.head {
            showApproval(head)
            expandIfNeeded(to: .approval)
            return
        }
        state.view = state.tasks.isEmpty ? .empty : .overview
    }

    /// The question `id` (the one the card or the iPhone showed), else the card's.
    /// nil when that question no longer waits.
    @MainActor
    private func waitingQuestion(_ id: UUID?) -> QueuedQuestion? {
        guard let id else { return questions.head }
        guard let entry = questions.entry(id: id) else {
            nbLog("Answer for a question no longer waiting, ignored")
            return nil
        }
        return entry
    }

    /// Called by QuestionView and the iPhone. Sends the answers to the session that asked
    /// question `id` (the card's when nil), then the next question takes the card.
    @MainActor
    func sendQuestionAnswers(_ answers: [String: Any], for id: UUID? = nil) {
        // Only intercept a demo question — real questions always have a live fd.
        if DemoEngine.shared.isActive, questions.isEmpty {
            AppState.shared.pendingQuestion = nil
            AppState.shared.isPinned = false
            AppState.shared.view = AppState.shared.tasks.isEmpty ? .empty : .overview
            DemoEngine.shared.handleQuestionAnswered(answers: answers)
            return
        }
        guard !questions.isEmpty else { closeQuestionCard(pill: nil); return }
        guard let entry = waitingQuestion(id) else { return }
        guard let data = try? JSONSerialization.data(withJSONObject: ["permissionDecision": "answer", "answers": answers], options: .withoutEscapingSlashes),
              let json = String(data: data, encoding: .utf8) else {
            removeQuestion(id: entry.id)
            return
        }
        RecapStore.shared.recordQuestionAnswered(sessionId: entry.sessionId)
        removeQuestion(id: entry.id, answer: json)
    }

    /// Called by QuestionView.onDisappear — the card left the screen without an answer.
    /// Every waiting question goes back to its terminal ("ask") at once, since none of them
    /// is on screen any more; does NOT navigate (view already changed).
    @MainActor
    func releaseQuestionFD() {
        guard !questions.isEmpty else { return }
        let state = AppState.shared
        let gone = questions.removeAll()
        for entry in gone {
            let source = questionSources.removeValue(forKey: entry.id)
            let fd = entry.fd
            Task.detached { [weak self] in
                self?.sendLine(fd: fd, text: #"{"permissionDecision":"ask"}"#)
                DispatchQueue.main.async { source?.cancel() }
            }
            if !approvalQueue.hasEntries(forPill: entry.pillId) { state.updateTask(id: entry.pillId, state: .working) }
        }
        shownQuestion = nil
        focusBeforeQuestion = nil
        state.waitingQuestions = []
        state.pendingQuestion = nil
        for pill in Set(gone.map(\.pillId)) { clearPillBadge(id: pill) }
    }

    /// Called by QuestionView "Reply in terminal" button: question `id` (the card's when nil)
    /// goes back to its terminal.
    @MainActor
    func sendQuestionAsk(for id: UUID? = nil) {
        guard !questions.isEmpty else { closeQuestionCard(pill: nil); return }
        guard let entry = waitingQuestion(id) else { return }
        removeQuestion(id: entry.id, answer: #"{"permissionDecision":"ask"}"#)
    }

    /// The events that mean an agent is working with you (MachineVitals' work streak).
    private static let workEvents: Set<String> = ["UserPromptSubmit", "PreToolUse", "PostToolUse", "PostToolUseFailure", "Stop"]

    /// Returns the tool_input serialized as sorted-keys JSON, "" if absent or empty.
    /// Same computation used in processPermissionRequest and processEvent to match PostToolUse.
    private static func approvalInputKey(_ input: [String: Any]) -> String {
        guard !input.isEmpty,
              let data = try? JSONSerialization.data(withJSONObject: input, options: .sortedKeys),
              let str = String(data: data, encoding: .utf8) else { return "" }
        return str
    }

    // MARK: - Start

    func start() {
        // Ensure support directory exists (mode 0700 — not world-readable)
        let dir = Self.supportDir
        try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        try? FileManager.default.setAttributes([.posixPermissions: 0o700 as NSNumber], ofItemAtPath: dir.path)
        #if !APPSTORE
        installHookScript()
        #endif
        Thread.detachNewThread { self.serve(path: Self.socketPath, host: nil) }
        discoverSessions()
    }

    // MARK: - Socket server (background thread)

    /// Remote hosts' listening sockets, by host (RemoteHosts.swift).
    private var remoteFDs: [String: Int32] = [:]

    /// Serves `path` like nb.sock, every event tagged with `host` (an SSH tunnel's end).
    func serveRemote(path: String, host: String) {
        Thread.detachNewThread { self.serve(path: path, host: host) }
    }

    /// Stops a remote host's socket: closing it ends its accept loop.
    func stopRemote(host: String) {
        connectionLock.lock()
        let fd = remoteFDs.removeValue(forKey: host)
        connectionLock.unlock()
        if let fd { close(fd) }
    }

    private func serve(path: String, host: String?) {
        // sun_path on macOS is 104 bytes including the NUL terminator → max 103 usable bytes
        let maxSunPathBytes = MemoryLayout<sockaddr_un>.size - MemoryLayout<sa_family_t>.size - 1
        guard path.utf8.count <= maxSunPathBytes else {
            NSLog("HookServer: socket path too long (\(path.utf8.count) bytes, max \(maxSunPathBytes)): \(path)")
            return
        }
        try? FileManager.default.removeItem(atPath: path)

        let fd = socket(AF_UNIX, SOCK_STREAM, 0)
        guard fd >= 0 else { return }
        if let host {
            connectionLock.lock(); remoteFDs[host] = fd; connectionLock.unlock()
        } else {
            serverFD = fd
        }

        var addr = sockaddr_un()
        addr.sun_family = sa_family_t(AF_UNIX)
        let cpath = Array(path.utf8CString)
        withUnsafeMutableBytes(of: &addr.sun_path) { raw in
            for (i, c) in cpath.enumerated() where i < raw.count { raw[i] = UInt8(bitPattern: c) }
        }

        let bindRC = withUnsafePointer(to: &addr) { ptr in
            ptr.withMemoryRebound(to: sockaddr.self, capacity: 1) { Darwin.bind(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size)) }
        }
        guard bindRC == 0 else { close(fd); return }
        // Restrict socket to owner only
        chmod(path, 0o600)
        guard Darwin.listen(fd, 32) == 0 else { close(fd); return }

        while true {
            let clientFD = Darwin.accept(fd, nil, nil)
            guard clientFD >= 0 else {
                // Never end the server on a failed accept (#853); pause unless it was transient.
                // A listening socket that is gone for good (closed, not a socket) ends the loop.
                let err = errno
                if err == EBADF || err == EINVAL || err == ENOTSOCK { break }
                if err != EINTR && err != ECONNABORTED { usleep(100_000) }
                continue
            }
            // Reject connections from other users (same-UID check)
            var euid: uid_t = 0
            var egid: gid_t = 0
            guard getpeereid(clientFD, &euid, &egid) == 0, euid == getuid() else {
                close(clientFD)
                continue
            }
            // Enforce concurrent connection ceiling
            connectionLock.lock()
            let admitted: Bool
            if host == nil {
                admitted = connectionCount < Self.maxConnections
                if admitted { connectionCount += 1 }
            } else {
                admitted = remoteConnectionCount < Self.maxRemoteConnections
                if admitted { remoteConnectionCount += 1 }
            }
            connectionLock.unlock()
            guard admitted else {
                close(clientFD)
                continue
            }
            Thread.detachNewThread { self.handleClient(fd: clientFD, host: host) }
        }
    }

    // MARK: - Client handler (background thread)

    private func handleClient(fd: Int32, host: String? = nil) {
        defer {
            connectionLock.lock()
            if host == nil { connectionCount -= 1 } else { remoteConnectionCount -= 1 }
            connectionLock.unlock()
        }
        // 5-second receive timeout — unresponsive clients don't hold threads forever
        var tv = timeval(tv_sec: Self.receiveTimeoutSeconds, tv_usec: 0)
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, socklen_t(MemoryLayout<timeval>.size))

        // Read newline-delimited JSON
        var raw = Data()
        var buf = [UInt8](repeating: 0, count: 4096)
        outer: while true {
            let n = recv(fd, &buf, buf.count, 0)
            if n <= 0 { break }
            for i in 0..<n {
                if buf[i] == UInt8(ascii: "\n") { break outer }
                raw.append(buf[i])
            }
            if raw.count > Self.maxPayload { break }
        }

        guard !raw.isEmpty,
              let parsed = try? JSONSerialization.jsonObject(with: raw) as? [String: Any],
              var payload = host.map({ RemoteEvent.admit(parsed, host: $0) }) ?? parsed else {
            sendLine(fd: fd, text: #"{"ok":true}"#)
            close(fd)
            return
        }
        #if !APPSTORE
        // The session's own process, so its first event takes the pill found for it (as on Linux).
        if host == nil, let pid = Self.claudeAncestor(from: payload["hook_ppid"] as? Int, peer: fd) {
            payload["coucou_claude_pid"] = Int(pid)
        }
        #endif

        let coucouKind = payload["coucou_kind"] as? String ?? ""

        // statusline payloads are handled separately — no session, no reveal, no sound
        if coucouKind == "statusline" {
            Task { @MainActor in self.processStatusLine(payload: payload) }
            sendLine(fd: fd, text: #"{"ok":true}"#)
            close(fd)
            return
        }

        // AskUserQuestion via --ask PreToolUse hook — hold fd open like PermissionRequest
        if coucouKind == "ask_user_question" {
            let toolInput = payload["tool_input"] as? [String: Any] ?? [:]
            if let parsed = AskQuestion.parse(toolInput: toolInput) {
                Task { @MainActor in self.processQuestionRequest(fd: fd, parsed: parsed, payload: payload) }
            } else {
                // Malformed payload — fall back: send ask so Claude Code re-asks in terminal
                sendLine(fd: fd, text: #"{"permissionDecision":"ask"}"#)
                close(fd)
            }
            return
        }

        let eventName = payload["hook_event_name"] as? String ?? ""

        // Coucou's own chat runs Claude Code (ClaudeCodeChat.swift): no session to
        // show, but a tool it wants still needs a click on the card. A turn that
        // ends still takes its waiting card down.
        if payload["coucou_chat"] as? Bool == true && eventName != "PermissionRequest" {
            let sessionId = payload["session_id"] as? String ?? ""
            if ["Stop", "StopFailure", "UserPromptSubmit", "SessionEnd"].contains(eventName) {
                Task { @MainActor in
                    let ids = self.approvalQueue.entries.filter { $0.sessionId == sessionId }.map(\.id)
                    self.dismissApprovals(ids: ids, note: "Handled in the chat.")
                }
            }
            sendLine(fd: fd, text: #"{"ok":true}"#)
            close(fd)
            return
        }
        // Antigravity PreToolUse: its hook waits for an answer. "ask" right away leaves the tool
        // to Antigravity's own rules and prompt; a command it has no rule for waits for a click
        // in the notch when that is turned on in Settings.
        if coucouKind == "agy_pre_tool" {
            let gate = Self.agyApprovalGate(payload)
            Task { @MainActor in
                self.processEvent(name: eventName, payload: payload)
                if let gate {
                    self.processAgyApproval(fd: fd, payload: payload, command: gate.command,
                                            waitSeconds: gate.waitSeconds)
                }
            }
            if gate == nil {
                sendLine(fd: fd, text: #"{"permissionDecision":"ask"}"#)
                close(fd)
            }
            return
        }

        if eventName == "PermissionRequest" {
            // Hold fd open — Claude Code waits for our decision (up to 120s)
            Task { @MainActor in self.processPermissionRequest(fd: fd, payload: payload) }
        } else {
            Task { @MainActor in self.processEvent(name: eventName, payload: payload) }
            sendLine(fd: fd, text: #"{"ok":true}"#)
            close(fd)
        }
    }


    // MARK: - Event → AppState
    // Claude Code events route to the permanent "integration_claude" task.
    // Events tagged with a valid coucou_agent route to a dynamic "integration_<agent>" task.
    // View switches only happen if VS Code (or the agent pill) is currently focused.
    // When not focused: state updates animate the mini bot in the pill; badge shown for alerts.

    @MainActor
    private func processEvent(name: String, payload: [String: Any]) {
        let state = AppState.shared
        let sessionId = payload["session_id"] as? String
                     ?? payload["conversation_id"] as? String
                     ?? "unknown"
        let (cwd, projectName) = sessionPlace(payload)
        let isRemote = payload["coucou_remote"] is String

        // Determine which pill this event belongs to.
        // coucou_agent must be lowercase, digits and hyphens, ≤ 24 chars.
        let rawAgent = payload["coucou_agent"] as? String ?? ""
        let validAgent = Self.validateAgent(rawAgent)

        let termProgram = payload["term_program"] as? String ?? ""
        let bundleId    = payload["bundle_id"]    as? String ?? ""
        let tty         = payload["tty"]          as? String ?? ""
        #if !APPSTORE
        if (name == "SessionStart" || name == "UserPromptSubmit") && (payload["coucou_agent"] as? String ?? "").isEmpty {
            SessionLauncher.shared.remember(folder: cwd, terminal: bundleId)
        }
        #endif

        // Cursor identified solely by its stable Electron bundle ID.
        // ToDesktop builds other apps too — do not match on "todesktop" alone.
        let isCursorEditor = bundleId.lowercased() == "com.todesktop.230313mzl4w4u92"
        let isVSCodeEditor = !isCursorEditor && (
            termProgram.lowercased().contains("vscode") ||
            bundleId.lowercased().contains("vscode"))

        // Routing:
        // • "codex" → agent_codex (GitHub build only: workspace pill, approvals in the notch)
        // • other valid coucou_agent → external pill (fire-and-forget, no approval card)
        // • Cursor bundle ID → agent_cursor
        // • VS Code → integration_claude
        // • a known terminal (Warp, Terminal, iTerm…) → integration_claude, host recorded on the task
        #if !APPSTORE
        let isCodexEvent = rawAgent == "codex"
        #else
        let isCodexEvent = false
        #endif
        let baseId: String
        let isExternalAgent: Bool
        var hostApp: String? = nil
        if isCodexEvent {
            baseId = "agent_codex"
            isExternalAgent = false
        } else if let agent = validAgent {
            baseId = "agent_\(agent)"
            isExternalAgent = true
        } else if isRemote {
            baseId = terminalWorkspace()
            isExternalAgent = false
        } else if isCursorEditor {
            baseId = "agent_cursor"
            isExternalAgent = false
        } else if isVSCodeEditor {
            baseId = "integration_claude"
            isExternalAgent = false
        } else if let host = ClaudeHost.terminal(termProgram: termProgram, bundleId: bundleId) {
            baseId = terminalWorkspace()
            isExternalAgent = false
            hostApp = host.bundleId
        } else {
            nbLog("Ignored \(name) from \(termProgram.isEmpty ? bundleId : termProgram) (\(projectName))")
            return
        }
        // Claude Code sessions running at once each get their own pill; agents keep theirs.
        if !isExternalAgent && !isCodexEvent {
            adoptProcess(sessionId: sessionId, cwd: cwd, claudePid: payload["coucou_claude_pid"] as? Int)
        }
        let agentId = isExternalAgent || isCodexEvent ? baseId : claudePill(workspaceId: baseId, sessionId: sessionId)
        let ownSession = agentId.hasPrefix(PillCatalog.sessionPillPrefix)
        state.quietSessionPills.remove(agentId)

        func ensurePill() {
            if ownSession {
                upsertSessionTask(id: agentId, projectName: projectName, cwd: cwd, bundleId: bundleId, tty: tty)
            } else if isExternalAgent {
                upsertExternalAgent(id: agentId, name: validAgent!, bundleId: bundleId)
            } else {
                upsertWorkspaceTask(id: agentId, projectName: projectName, cwd: cwd, hostApp: hostApp, bundleId: bundleId, tty: tty)
            }
        }

        // The agent's activity, never the user's own prompt or session start.
        if !["SessionEnd", "SessionStart", "UserPromptSubmit"].contains(name) { followActivity(pill: agentId) }
        let focused = state.focusId == agentId
        if isRemote, let host = payload["coucou_remote"] as? String {
            RemoteTerminal.remember(pillId: agentId, host: host, payload: payload)
        } else {
            RemoteTerminal.forget(pillId: agentId)
        }
        // The user is looking at the session's terminal: a finish or an error is already in
        // front of them, so the pill only takes a badge (no sound, no reveal). Warp exposes
        // no active tab, so a sibling session in another tab of the same terminal is quiet too.
        let terminalInFront = hostApp != nil
            && NSWorkspace.shared.frontmostApplication?.bundleIdentifier == hostApp
        // For sessions that carry no id, derive a unique key from pill + cwd so that
        // concurrent anonymous sessions are tracked independently in RecapStore.
        let recapSessionId = (sessionId == "unknown" || sessionId.isEmpty)
            ? "\(agentId)+\(cwd)"
            : sessionId

        #if PHONE_LINK
        // The iPhone's "last turn" (prompt, actions, diffs, answer). Not a remote host's:
        // an instruction sent back would run `claude` here, in the host's folder.
        if !isExternalAgent && !isRemote { TurnRecorder.shared.record(event: name, payload: payload, pillId: agentId) }
        #endif

        // While permission requests are pending for this agent, dismiss the ones this event
        // resolves (same session and agent, and same tool + input for PostToolUse), then continue
        // normal processing. Only skip normal processing when nothing was resolved.
        if approvalQueue.hasEntries(forPill: agentId) {
            let tool = payload["tool_name"] as? String ?? ""
            let inputKey = Self.approvalInputKey(payload["tool_input"] as? [String: Any] ?? [:])
            let resolved = approvalQueue.resolved(byEvent: name, sessionId: sessionId, pillId: agentId,
                                                  tool: tool, inputKey: inputKey)
            if resolved.isEmpty { return }
            dismissApprovals(ids: resolved.map(\.id), note: isRemote ? String(localized: "Handled elsewhere.") : handledNote(for: agentId))
            // Approval dismissed — fall through so the resolving event updates state normally.
        }

        // Sessions sharing a pill share its tty: it follows the one that spoke last, so Open
        // after a Stop brings back the tab of the session that just finished.
        if !isExternalAgent, !tty.isEmpty, let idx = state.tasks.firstIndex(where: { $0.id == agentId }) {
            state.tasks[idx].sessionTTY = tty
        }
        // Events fired inside a subagent carry agent_id. Once the turn has ended with
        // background work in flight, they must not replace the turn's state or final line.
        let isSubagentEvent = !(payload["agent_id"] as? String ?? "").isEmpty
        if isSubagentEvent, backgroundWaits[sessionId] != nil, name != "Notification" {
            if name == "PreToolUse" {
                RecapStore.shared.preToolUse(sessionId: recapSessionId, tool: payload["tool_name"] as? String ?? "Tool")
            } else if name == "SubagentStop", !(payload["agent_type"] as? String ?? "").isEmpty {
                // Internal agents (prompt suggestions, /btw) also send SubagentStop, with an empty agent_type.
                subagentFinishedInBackground(sessionId: sessionId)
            }
            return
        }
        if !isSubagentEvent, name != "Notification", backgroundWaits.removeValue(forKey: sessionId) != nil {
            nbLog("Background wait ended by \(name) (\(sessionId.prefix(8)))")
        }
        if name != "Stop", name != "Notification" { cancelIdleReset(id: agentId) }

        if !isExternalAgent && !isCodexEvent {
            recordCode(event: name, pillId: agentId, payload: payload, cwd: cwd, project: projectName)
        }
        if Self.workEvents.contains(name) { MachineVitals.shared.agentWorked() }

        // A sibling session's event must not take the pill out of its approval state,
        // nor out of the question state while a question waits on it.
        defer {
            if approvalQueue.hasEntries(forPill: agentId) {
                state.updateTask(id: agentId, state: .approval)
            } else if questionWaits(onPill: agentId) {
                state.updateTask(id: agentId, state: .question)
            }
        }

        switch name {

        case "SessionStart":
            activeSessionId = sessionId
            ensurePill()
            if let idx = state.tasks.firstIndex(where: { $0.id == agentId }) { state.tasks[idx].finalLine = nil }
            nbLog("SessionStart \(isExternalAgent ? agentId : projectName) (\(sessionId.prefix(8)))")
            if !isExternalAgent { discoverSessions() }
            NotificationCenter.default.post(name: .checkMondayRecap, object: nil)
            if state.isPresent { expandIfNeeded(to: .overview) }
            SoundEngine.shared.play("work")
            if agentId == "agent_hermes", let platform = payload["platform"] as? String,
               !platform.isEmpty, platform != "cli" {
                let capitalized = platform.prefix(1).uppercased() + platform.dropFirst()
                appendStep(id: agentId, step: String(capitalized))
            }

        case "UserPromptSubmit":
            activeSessionId = sessionId
            ensurePill()
            if let idx = state.tasks.firstIndex(where: { $0.id == agentId }) { state.tasks[idx].finalLine = nil }
            state.updateTask(id: agentId, state: .thinking)
            state.endFollow(agentId)
            if let prompt = payload["prompt"] as? String, !prompt.isEmpty {
                appendStep(id: agentId, step: String(prompt.prefix(60)))
            }
            RecapStore.shared.userPromptSubmit(sessionId: recapSessionId, pillId: agentId, project: projectName)
            NotificationCenter.default.post(name: .checkMondayRecap, object: nil)
            // A new turn: the finished card would show the prompt as if it were the answer.
            if state.mode == .expanded && state.view == .finished && state.focusId == agentId { state.view = .overview }
            if state.isPresent { expandIfNeeded(to: .overview) }

        case "PreToolUse":
            activeSessionId = sessionId
            let tool = payload["tool_name"] as? String ?? "Tool"
            if let idx = state.tasks.firstIndex(where: { $0.id == agentId }) { state.tasks[idx].finalLine = nil }
            RecapStore.shared.preToolUse(sessionId: recapSessionId, tool: tool)
            // AskUserQuestion is handled via the dedicated --ask hook.
            // Skip state/step update here to avoid flickering over the question card.
            guard tool != "AskUserQuestion" else { break }
            ensurePill()
            let isQuestion = tool == "ask_question" || tool.hasSuffix(":ask_question")
            state.updateTask(id: agentId, state: isQuestion ? .question : .working)
            let input = payload["tool_input"] as? [String: Any] ?? [:]
            let step = localizedStep(tool: tool, input: input)
            appendStep(id: agentId, step: step)
            if isQuestion {
                SoundEngine.shared.play("question")
                if state.isPresent { expandIfNeeded(to: .overview) }
            }
            nbLog("PreToolUse \(tool)")

        case "PostToolUse":
            state.updateTask(id: agentId, state: .working)
            // Live diff for Edit / MultiEdit / Write
            let diffTool = payload["tool_name"] as? String ?? ""
            let diffInput = payload["tool_input"] as? [String: Any] ?? [:]
            if var diff = buildFileDiff(tool: diffTool, input: diffInput, pillId: agentId) {
                diff.remote = isRemote
                let idx = state.appendSessionDiff(diff, for: agentId)
                let step = String.makeDiffStep(filename: diff.name, added: diff.added, removed: diff.removed, diffId: idx)
                appendStep(id: agentId, step: step)
                RecapStore.shared.recordFileDiff(sessionId: recapSessionId, path: diff.name, added: diff.added, removed: diff.removed)
                followEdit(pillId: agentId, diffId: idx)
            }

        case "PostToolUseFailure":
            state.updateTask(id: agentId, state: .working)
            appendStep(id: agentId, step: "⚠ failed")

        case "Notification":
            let message = payload["message"] as? String ?? ""
            let lower = message.lowercased()
            if lower.contains("rate limit") || lower.contains("limite d") {
                state.updateTask(id: agentId, state: .ratelimit)
                SoundEngine.shared.play("rate")
            } else if payload["notification_type"] as? String == "permission_prompt" {
                // No approval card for this session (terminal cards off, Claude desktop app…):
                // it is blocked on the user, not working.
                state.updateTask(id: agentId, state: .approval)
                appendStep(id: agentId, step: message)
                SoundEngine.shared.play("approval")
            } else if payload["notification_type"] as? String == "elicitation_dialog" || message.hasSuffix("?") {
                state.updateTask(id: agentId, state: .question)
                appendStep(id: agentId, step: message)
            }

        case "Stop":
            if state.liveDiff?.pillId == agentId { state.liveDiff = nil }
            let rawFinal = (payload["last_assistant_message"] as? String)
                ?? (payload["message"] as? String) ?? ""
            let finalText = DiffEngine.toOneLine(rawFinal)
            if !finalText.isEmpty {
                appendStep(id: agentId, step: finalText)
                if let idx = state.tasks.firstIndex(where: { $0.id == agentId }) {
                    state.tasks[idx].finalLine = finalText
                }
            }
            RecapStore.shared.stop(sessionId: recapSessionId)
            // Subagents, shells or monitors still running: the session is paused, not done.
            // Claude Code wakes the main agent when they finish, and its next Stop ends the turn.
            let inFlight = payload["background_tasks"] as? [[String: Any]] ?? []
            if !inFlight.isEmpty {
                let subagents = inFlight.filter { $0["type"] as? String == "subagent" }.count
                backgroundWaits[sessionId] = BackgroundWait(pillId: agentId, subagents: subagents,
                                                            others: inFlight.count - subagents)
                state.updateTask(id: agentId, state: .working)
                appendStep(id: agentId, step: backgroundStep(inFlight.count))
                nbLog("Stop with \(inFlight.count) background task(s) (\(sessionId.prefix(8)))")
                break
            }
            state.updateTask(id: agentId, state: .finished)
            if !terminalInFront { SoundEngine.shared.play("finish") }
            AppReview.noteFinished()
            if !terminalInFront && showsAlert(for: agentId, focused: focused) {
                expandIfNeeded(to: .finished)
            } else if !state.openForAlert(AlertExpansion.agentFinished, pill: agentId, view: .finished) {
                setPillBadge(id: agentId, badge: .finished)
            }
            let generation = idleResetGeneration[agentId, default: 0]
            DispatchQueue.main.asyncAfter(deadline: .now() + 5.2) {
                // Skip if the pill got busy again (another turn or session started meanwhile).
                guard self.idleResetGeneration[agentId, default: 0] == generation else { return }
                if isExternalAgent {
                    AppState.shared.removeTask(id: agentId)
                } else {
                    AppState.shared.updateTask(id: agentId, state: .idle)
                    self.clearPillBadge(id: agentId)
                }
            }

        case "StopFailure":
            RecapStore.shared.stop(sessionId: recapSessionId)
            nbLog("StopFailure \(sessionId.prefix(8)) — \(payload.keys.sorted().joined(separator: ", ")): \(Self.stopFailureDetail(payload) ?? "no detail")")
            state.updateTask(id: agentId, state: .error)
            // The error view shows what went wrong (finalLine), not the last thing said before it.
            if let idx = state.tasks.firstIndex(where: { $0.id == agentId }) {
                state.tasks[idx].finalLine = Self.stopFailureDetail(payload)
            }
            if !terminalInFront { SoundEngine.shared.play("error") }
            if !terminalInFront && showsAlert(for: agentId, focused: focused) {
                expandIfNeeded(to: .error)
            } else if !state.openForAlert(AlertExpansion.agentError, pill: agentId, view: .error) {
                setPillBadge(id: agentId, badge: .error)
            }

        case "Interrupt":
            // Codex: user stopped the turn
            RecapStore.shared.stop(sessionId: recapSessionId)
            activeSessionId = nil
            state.updateTask(id: agentId, state: .idle)
            clearPillBadge(id: agentId)

        case "SessionEnd":
            activeSessionId = nil
            endSession(sessionId)
            if !isExternalAgent { discoverSessions() }
            if let idx = state.tasks.firstIndex(where: { $0.id == agentId }) { state.tasks[idx].finalLine = nil }
            state.endFollow(agentId)
            state.clearSessionDiffs(for: agentId)
            state.removeTask(id: agentId)
            RecapStore.shared.sessionEnd(sessionId: recapSessionId)

        case "SubagentStart":
            appendStep(id: agentId, step: "+ subagent")

        case "SubagentStop":
            appendStep(id: agentId, step: "• subagent done")

        default:
            break
        }
    }

    // MARK: - One pill per Claude Code session

    /// Extra sessions running at once, by session ID: the first keeps the workspace pill,
    /// each other one gets a pill of its own until it ends or goes silent.
    private var sessionPills: [String: String] = [:]
    /// The session each workspace pill shows, by pill ID.
    private var workspaceOwner: [String: String] = [:]
    private var lastHeard: [String: Date] = [:]
    /// A session silent this long, and idle, was closed without a SessionEnd.
    private static let sessionStale: TimeInterval = 30 * 60

    @MainActor
    private func isStale(_ sessionId: String, pillId: String, now: Date) -> Bool {
        // A pill made from a process never goes stale: the next scan says if it is gone.
        if hasProcess(sessionId) { return false }
        let heard = lastHeard[sessionId] ?? .distantPast
        let idle = AppState.shared.tasks.first(where: { $0.id == pillId }).map { $0.state == .idle } ?? true
        return now.timeIntervalSince(heard) > Self.sessionStale && idle
    }

    /// The pill a Claude Code session's events go to.
    @MainActor
    private func claudePill(workspaceId: String, sessionId: String) -> String {
        guard !sessionId.isEmpty, sessionId != "unknown" else { return workspaceId }
        let now = Date()
        for (sid, pid) in sessionPills where isStale(sid, pillId: pid, now: now) {
            sessionPills[sid] = nil
            lastHeard[sid] = nil
            AppState.shared.removeTask(id: pid)
        }
        // Sessions that only ever held a workspace pill are forgotten the same way.
        let owners = Set(workspaceOwner.values)
        for (sid, heard) in lastHeard where sessionPills[sid] == nil && !owners.contains(sid)
            && now.timeIntervalSince(heard) > Self.sessionStale {
            lastHeard[sid] = nil
        }
        lastHeard[sessionId] = now
        if let own = sessionPills[sessionId] { return own }
        if let owner = workspaceOwner[workspaceId], owner != sessionId,
           !isStale(owner, pillId: workspaceId, now: now) {
            let pillId = SessionPillId.make(sessionId, prefix: PillCatalog.sessionPillPrefix,
                                            taken: Set(sessionPills.values))
            sessionPills[sessionId] = pillId
            return pillId
        }
        workspaceOwner[workspaceId] = sessionId
        return workspaceId
    }

    @MainActor
    private func endSession(_ sessionId: String) {
        sessionPills[sessionId] = nil
        lastHeard[sessionId] = nil
        for (pill, owner) in workspaceOwner where owner == sessionId { workspaceOwner[pill] = nil }
        for (key, id) in pidSessions where id == sessionId {
            pidSessions[key] = nil
            pidCwd[key] = nil
        }
        sessionPids[sessionId] = nil
    }

    /// Claude Code in a terminal goes to its own pill when that is the main tool.
    @MainActor
    private func terminalWorkspace() -> String {
        AppState.shared.mainPillId == "workspace_claude" ? "workspace_claude" : "integration_claude"
    }

    // MARK: - Sessions found running
    //
    // A session is only heard of at its next event, and an idle one has none to send:
    // after a launch its pill would be missing until it did something. So the `claude`
    // processes running get their pills at once under `pid-<pid>`, and the session's
    // first event in that folder takes the pill over.

    /// `pid-<pid>` → the session key its pill is under (that same key until adopted).
    private var pidSessions: [String: String] = [:]
    private var pidCwd: [String: String] = [:]
    /// Session key → the `claude` process its events came from.
    private var sessionPids: [String: Int] = [:]

    /// The app the session on `pill` runs in, and its tty, found from its `claude` process.
    @MainActor
    func sessionTerminal(pill: String) -> (bundleId: String, tty: String?)? {
        guard let sid = sessionPills.first(where: { $0.value == pill })?.key ?? workspaceOwner[pill] else { return nil }
        let pid = sessionPids[sid] ?? pidSessions.first(where: { $0.value == sid }).flatMap { Int($0.key.dropFirst(4)) }
        guard let pid else { return nil }
        return Self.terminal(of: Int32(pid))
    }

    /// Up from `pid` to the first app with a window: the terminal or editor it runs in.
    nonisolated static func terminal(of pid: Int32) -> (bundleId: String, tty: String?)? {
        var current = pid
        var tty: String?
        for _ in 0..<16 {
            var kp = kinfo_proc()
            var size = MemoryLayout<kinfo_proc>.stride
            var mib: [Int32] = [CTL_KERN, KERN_PROC, KERN_PROC_PID, current]
            guard sysctl(&mib, 4, &kp, &size, nil, 0) == 0, size > 0 else { return nil }
            if tty == nil, kp.kp_eproc.e_tdev != -1, let name = devname(kp.kp_eproc.e_tdev, S_IFCHR) {
                tty = "/dev/" + String(cString: name)
            }
            if let app = NSRunningApplication(processIdentifier: current), app.activationPolicy == .regular,
               let id = app.bundleIdentifier {
                return (id, tty)
            }
            current = kp.kp_eproc.e_ppid
            if current <= 1 { return nil }
        }
        return nil
    }

    private struct FoundSession: Sendable {
        let pid: Int32
        let cwd: String
        /// The tmux session the user named, "" outside tmux.
        let name: String
    }

    private func hasProcess(_ sessionId: String) -> Bool {
        sessionId.hasPrefix("pid-") || pidSessions.values.contains(sessionId)
    }

    /// The session's first event: the pill made for its process is its. By the process when the
    /// event names it, else by folder (the session may have moved since it started).
    @MainActor
    private func adoptProcess(sessionId: String, cwd: String, claudePid: Int?) {
        guard !sessionId.isEmpty, sessionId != "unknown",
              sessionPills[sessionId] == nil, !workspaceOwner.values.contains(sessionId),
              !pidSessions.values.contains(sessionId) else { return }
        if let claudePid { sessionPids[sessionId] = claudePid }
        let byPid = claudePid.map { "pid-\($0)" }.flatMap { pidSessions[$0] == $0 ? $0 : nil }
        // ponytail: by folder, two idle sessions in one folder may swap pills (same name).
        let byCwd = cwd.isEmpty ? nil : pidSessions.first(where: { $0.key == $0.value && pidCwd[$0.key] == cwd })?.key
        guard let key = claudePid == nil ? byCwd : byPid else { return }
        nbLog("Session \(sessionId.prefix(8)) takes the pill found for \(key)")
        if let own = sessionPills.removeValue(forKey: key) { sessionPills[sessionId] = own }
        for (pill, owner) in workspaceOwner where owner == key { workspaceOwner[pill] = sessionId }
        lastHeard[sessionId] = lastHeard.removeValue(forKey: key)
        pidSessions[key] = sessionId
    }

    /// Pills for the sessions running now; those whose process is gone go.
    func discoverSessions() {
        #if !APPSTORE
        DispatchQueue.global(qos: .utility).async {
            let found = Self.claudeProcesses()
            Task { @MainActor in self.applyDiscovered(found) }
        }
        #endif
    }

    @MainActor
    private func applyDiscovered(_ found: [FoundSession]) {
        let state = AppState.shared
        let alive = Set(found.map { "pid-\($0.pid)" })
        for (key, sessionId) in pidSessions where !alive.contains(key) {
            let pillId = sessionPills[sessionId] ?? workspaceOwner.first(where: { $0.value == sessionId })?.key
            endSession(sessionId)
            guard let pillId else { continue }
            state.clearSessionDiffs(for: pillId)
            state.removeTask(id: pillId)
            state.quietSessionPills.remove(pillId)
        }
        for session in found {
            let key = "pid-\(session.pid)"
            guard pidSessions[key] == nil else { continue }
            // A session already heard from in this folder is this process's: no second pill.
            let known = Set(sessionPills.keys).union(workspaceOwner.values)
                .filter { !$0.hasPrefix("pid-") && !pidSessions.values.contains($0) }
            if let sid = known.first(where: { sessionPids[$0] == Int(session.pid) }) ?? known.first(where: { sid in
                sessionPids[sid] == nil && {
                    let pill = sessionPills[sid] ?? workspaceOwner.first(where: { $0.value == sid })?.key
                    return state.tasks.first(where: { $0.id == pill })?.sessionCwd == session.cwd
                }()
            }) {
                pidSessions[key] = sid
                pidCwd[key] = session.cwd
                continue
            }
            pidSessions[key] = key
            pidCwd[key] = session.cwd
            let raw = session.name.isEmpty ? URL(fileURLWithPath: session.cwd).lastPathComponent : session.name
            let projectName = aliasProjectName(raw.isEmpty ? "Session" : raw)
            let pillId = claudePill(workspaceId: terminalWorkspace(), sessionId: key)
            state.quietSessionPills.insert(pillId)
            if pillId.hasPrefix(PillCatalog.sessionPillPrefix) {
                upsertSessionTask(id: pillId, projectName: projectName, cwd: session.cwd, bundleId: "")
            } else {
                let host = state.tasks.first(where: { $0.id == pillId })?.hostApp
                upsertWorkspaceTask(id: pillId, projectName: projectName, cwd: session.cwd, hostApp: host)
            }
        }
    }

    #if !APPSTORE
    private static func commandOutput(_ path: String, _ args: [String]) -> String {
        let task = Process(); let pipe = Pipe()
        task.executableURL = URL(fileURLWithPath: path)
        task.arguments = args; task.standardOutput = pipe; task.standardError = Pipe()
        guard (try? task.run()) != nil else { return "" }
        let data = pipe.fileHandleForReading.readDataToEndOfFile()
        task.waitUntilExit()
        return String(data: data, encoding: .utf8) ?? ""
    }

    /// Each top-level `claude` process of ours (not one whose parent is `claude`: the
    /// helpers it spawns under its own name), with its folder and tmux session.
    private static func claudeProcesses() -> [FoundSession] {
        let uid = getuid()
        var parent: [Int32: Int32] = [:]
        var name: [Int32: String] = [:]
        for line in commandOutput("/bin/ps", ["-axo", "pid=,ppid=,uid=,comm="]).split(separator: "\n") {
            let f = line.split(separator: " ", omittingEmptySubsequences: true)
            guard f.count >= 4, let pid = Int32(f[0]), let ppid = Int32(f[1]),
                  let owner = UInt32(f[2]), owner == uid else { continue }
            parent[pid] = ppid
            // macOS reports Claude Code as its version in ucomm; comm is its path, may have spaces
            name[pid] = URL(fileURLWithPath: f[3...].joined(separator: " ")).lastPathComponent
        }
        let panes = tmuxPanes()
        var found: [FoundSession] = []
        for (pid, comm) in name where comm == "claude" {
            if let ppid = parent[pid], name[ppid] == "claude" { continue }
            let cwd = commandOutput("/usr/sbin/lsof", ["-a", "-d", "cwd", "-Fn", "-p", String(pid)])
                .split(separator: "\n").first(where: { $0.hasPrefix("n/") }).map { String($0.dropFirst()) }
            guard let cwd else { continue }
            var tmux = ""
            var walk: Int32? = pid
            for _ in 0..<8 {
                guard let current = walk else { break }
                if let session = panes[current] { tmux = session; break }
                walk = parent[current]
            }
            found.append(FoundSession(pid: pid, cwd: cwd, name: claudeSessionName(pid: pid) ?? tmux))
        }
        return found.sorted { $0.pid < $1.pid }
    }

    /// The name `/rename` gave the session, from Claude Code's own `~/.claude/sessions/<pid>.json`.
    nonisolated static func claudeSessionName(pid: Int32) -> String? {
        let url = FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".claude/sessions/\(pid).json")
        guard let data = try? Data(contentsOf: url),
              let json = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let name = json["name"] as? String, !name.isEmpty else { return nil }
        return name
    }

    /// The `claude` process that ran a hook: the farthest one up from the hook's parent, else
    /// from the relay on the socket (gone already when it sent its event and left).
    private static func claudeAncestor(from hookParent: Int?, peer fd: Int32) -> Int32? {
        var pid: pid_t = 0
        var len = socklen_t(MemoryLayout<pid_t>.size)
        if let hookParent, (2...Int(Int32.max)).contains(hookParent) {
            pid = pid_t(hookParent)
        } else {
            guard getsockopt(fd, SOL_LOCAL, LOCAL_PEERPID, &pid, &len) == 0, pid > 1 else { return nil }
        }
        var found: Int32?
        var current = pid
        for _ in 0..<12 {
            guard let ppid = parentPid(current) else { break }
            if commandName(current) == "claude" { found = current }
            guard ppid > 1 else { break }
            current = ppid
        }
        return found
    }

    private static func parentPid(_ pid: pid_t) -> pid_t? {
        var info = kinfo_proc()
        var size = MemoryLayout<kinfo_proc>.stride
        var mib: [Int32] = [CTL_KERN, KERN_PROC, KERN_PROC_PID, pid]
        guard sysctl(&mib, 4, &info, &size, nil, 0) == 0, size > 0 else { return nil }
        return info.kp_eproc.e_ppid
    }

    /// argv[0]'s last part, as `ps -o comm` shows it: macOS names Claude Code by its version otherwise.
    private static func commandName(_ pid: pid_t) -> String? {
        var mib: [Int32] = [CTL_KERN, KERN_PROCARGS2, pid]
        var size = 0
        guard sysctl(&mib, 3, nil, &size, nil, 0) == 0, size > MemoryLayout<Int32>.size else { return nil }
        var buf = [UInt8](repeating: 0, count: size)
        guard sysctl(&mib, 3, &buf, &size, nil, 0) == 0 else { return nil }
        // argc, the executable's path, NUL padding, then argv[0].
        var i = MemoryLayout<Int32>.size
        while i < size, buf[i] != 0 { i += 1 }
        while i < size, buf[i] == 0 { i += 1 }
        let start = i
        while i < size, buf[i] != 0 { i += 1 }
        guard start < i else { return nil }
        return URL(fileURLWithPath: String(decoding: buf[start..<i], as: UTF8.self)).lastPathComponent
    }

    /// The process in each tmux pane → its session's name. Empty without tmux.
    private static func tmuxPanes() -> [Int32: String] {
        let paths = ["/opt/homebrew/bin/tmux", "/usr/local/bin/tmux", "/opt/local/bin/tmux"]
        guard let tmux = paths.first(where: { FileManager.default.isExecutableFile(atPath: $0) }) else { return [:] }
        var panes: [Int32: String] = [:]
        for line in commandOutput(tmux, ["list-panes", "-a", "-F", "#{pane_pid} #{session_name}"]).split(separator: "\n") {
            let f = line.split(separator: " ", maxSplits: 1)
            if f.count == 2, let pid = Int32(f[0]) { panes[pid] = String(f[1]) }
        }
        return panes
    }
    #endif

    /// A session's own pill: named after its folder, in that folder's colour, with what
    /// Open terminal needs.
    @MainActor
    private func upsertSessionTask(id: String, projectName: String, cwd: String, bundleId: String, tty: String = "") {
        upsertExternalAgent(id: id, name: projectName)
        let state = AppState.shared
        guard let idx = state.tasks.firstIndex(where: { $0.id == id }) else { return }
        if state.tasks[idx].name != projectName { state.tasks[idx].name = projectName }
        if !cwd.isEmpty { state.tasks[idx].sessionCwd = cwd }
        if !bundleId.isEmpty { state.tasks[idx].sessionBundleId = bundleId }
        if !tty.isEmpty { state.tasks[idx].sessionTTY = tty }
    }

    // MARK: - Agent validation + dynamic pill

    /// Validates a coucou_agent name: lowercase, digits and hyphens, 1–24 chars.
    /// "claude" is reserved and rejected so it cannot impersonate the Claude Code pill.
    /// Returns the name unchanged if valid, nil otherwise.
    private static func validateAgent(_ raw: String) -> String? {
        guard !raw.isEmpty, raw.count <= 24, raw != "claude" else { return nil }
        for scalar in raw.unicodeScalars {
            let v = scalar.value
            let ok = (v >= 0x61 && v <= 0x7A)  // a-z
                  || (v >= 0x30 && v <= 0x39)   // 0-9
                  || v == 0x2D                   // -
            guard ok else { return nil }
        }
        return raw
    }

    /// Creates a dynamic pill for a third-party agent on first event, then no-ops.
    /// ID format: "agent_<name>" — never collides with "integration_*" pills.
    /// Inserted right after integration_claude so it appears in the visible prefix(4).
    @MainActor
    private func upsertExternalAgent(id: String, name: String, bundleId: String = "") {
        let state = AppState.shared
        if let existingIdx = state.tasks.firstIndex(where: { $0.id == id }) {
            if !bundleId.isEmpty { state.tasks[existingIdx].sessionBundleId = bundleId }
            return
        }
        // A tag whose session never ends would keep its pill forever: past the cap,
        // drop the oldest one not working. Same rule as Windows and Linux (state.ts).
        let dynamic = state.tasks.filter {
            $0.source == .agent && $0.id != state.mainPillId && !state.activeIntegrations.contains($0.id)
        }
        if dynamic.count >= Self.maxAgentPills {
            let busy: Set<BotState> = [.working, .thinking, .searching, .approval, .question]
            guard let stale = dynamic.first(where: { !busy.contains($0.state) }) else { return }
            state.removeTask(id: stale.id)
        }
        let color: String
        if let def = PillCatalog.definition(for: id) {
            color = def.color
        } else {
            color = IslandConst.colorForProject(name)
        }
        var task = AgentTask(id: id, name: name, color: color, state: .idle, steps: [], source: .agent)
        if !bundleId.isEmpty { task.sessionBundleId = bundleId }
        if let claudeIdx = state.tasks.firstIndex(where: { $0.id == "integration_claude" }) {
            state.tasks.insert(task, at: claudeIdx + 1)
        } else {
            state.tasks.append(task)
        }
        if state.focusId == nil { state.focusId = id }
        state.syncMode()
    }

    // MARK: - Helpers

    /// Settings → Follow edits live: the edit just made opens on its diff (LiveEdits.mayFollow).
    @MainActor
    private func followEdit(pillId: String, diffId: Int) {
        let state = AppState.shared
        let follow = LiveEdits.mayFollow(
            enabled: state.followEdits,
            stopped: state.liveStopped.contains(pillId),
            cardPending: state.pendingApproval != nil || state.pendingQuestion != nil,
            expanded: state.mode == .expanded,
            // The code view this session is followed in counts as the overview.
            onOverview: state.view == .overview || (state.view == .code && state.liveDiff?.pillId == pillId),
            typing: NSApp.keyWindow?.firstResponder is NSTextView,
            followingOther: state.liveDiff.map { $0.pillId != pillId } ?? false)
        guard follow else { return }
        withAnimation(.easeOut(duration: 0.22)) {
            state.liveDiff = LiveDiff(pillId: pillId, diffId: diffId)
            state.focusId = pillId
        }
        // Its code view when the session has one; the diff card otherwise (remote hosts, other agents).
        let view: IslandView = state.codeSessions[pillId]?.hasContent == true ? .code : .overview
        if state.mode != .expanded {
            NotificationCenter.default.post(name: .hookExpand, object: view)
        } else if state.view != view {
            withAnimation(.easeIn(duration: 0.16)) { state.view = view }
        }
    }

    /// Settings → Follow the latest activity: the pill that just did something takes the card, and with
    /// "Open the island on new activity" the island opens on it. A view you opened stays, and an approval
    /// or a question keeps the front.
    @MainActor
    private func followActivity(pill id: String) {
        let state = AppState.shared
        guard state.followActivity, state.isPresent, !state.pointerInIsland, state.pendingApproval == nil, state.pendingQuestion == nil,
              state.tasks.contains(where: { $0.id == id }) else { return }
        if state.mode == .expanded && state.view != .overview { return }
        if state.focusId != id {
            withAnimation(.spring(response: 0.5, dampingFraction: 0.72)) { state.setFocus(id) }
        }
        if state.expandOnActivity && state.mode != .expanded {
            NotificationCenter.default.post(name: .hookExpand, object: IslandView.overview)
        }
    }

    /// Whether a finish or an error opens the island. The pill shown always does; another pill
    /// does too with "Show alerts from every pill" (#683): it takes the focus for its card, and
    /// the pill that was shown comes back when the island closes. Never over an approval, a
    /// question, or a card the user is busy in (chat, mail, settings…).
    @MainActor
    private func showsAlert(for pill: String, focused: Bool) -> Bool {
        if focused { return true }
        let state = AppState.shared
        guard state.alertsFromAllPills, state.pendingApproval == nil, state.pendingQuestion == nil else { return false }
        if state.mode == .expanded, ![IslandView.overview, .finished, .error, .empty].contains(state.view) { return false }
        if focusBeforeAlert == nil { focusBeforeAlert = state.focusId }
        alertPill = pill
        withAnimation(.spring(response: 0.5, dampingFraction: 0.72)) { state.focusId = pill }
        return true
    }

    /// The island closed: the pill that was shown before another pill's alert comes back.
    @MainActor
    func restoreAlertFocus() {
        guard let prev = focusBeforeAlert else { return }
        let borrowed = alertPill
        focusBeforeAlert = nil
        alertPill = nil
        let state = AppState.shared
        // Only if the alert's pill is still the one shown (the user may have picked another).
        guard state.pendingApproval == nil, state.focusId == borrowed, borrowed != prev,
              state.tasks.contains(where: { $0.id == prev }) else { return }
        withAnimation(.spring(response: 0.5, dampingFraction: 0.72)) { state.focusId = prev }
    }

    @MainActor
    private func expandIfNeeded(to view: IslandView) {
        let state = AppState.shared
        let isAlert: Bool
        switch view {
        case .approval, .question, .finished, .error, .confused: isAlert = true
        default: isAlert = false
        }
        if state.mode == .expanded {
            // Approval and question always win; other alerts are blocked while a card is showing
            if view == .approval || view == .question {
                state.view = view
            } else if isAlert && approvalQueue.isEmpty {
                state.view = view
            }
        } else if isAlert {
            // Alerts always force-expand
            NotificationCenter.default.post(name: .hookExpand, object: view)
        } else if state.mode == .hidden {
            // Non-alert work events: reveal compact only, never force-expand
            NotificationCenter.default.post(name: .hookReveal, object: nil)
        }
        // Already compact and non-alert: Mochi state update is enough, no expand
    }

    // MARK: - Status line (plan gauge)

    @MainActor
    private func processStatusLine(payload: [String: Any]) {
        guard let incoming = ClaudePlanGauge.parse(payload: payload) else { return }
        let usage = ClaudePlanGauge.merge(AppState.shared.claudePlanUsage, with: incoming)
        // Claude Code calls the status line several times a second: publish (and save to
        // UserDefaults) only when the numbers change, or every 30 s for "updated … ago".
        if let cur = AppState.shared.claudePlanUsage,
           cur.fiveHour == usage.fiveHour, cur.sevenDay == usage.sevenDay,
           usage.updatedAt.timeIntervalSince(cur.updatedAt) < 30 { return }
        AppState.shared.claudePlanUsage = usage
    }

    // MARK: - Permission request (blocking — Claude Code waits for decision)

    @MainActor
    private func processPermissionRequest(fd: Int32, payload: [String: Any]) {
        let state = AppState.shared
        let sessionId = payload["session_id"] as? String
                     ?? payload["conversation_id"] as? String
                     ?? "unknown"
        let (cwd, projectName) = sessionPlace(payload)
        let isRemote = payload["coucou_remote"] is String

        let rawAgent = payload["coucou_agent"] as? String ?? ""
        let termProgram = payload["term_program"] as? String ?? ""
        let bundleId    = payload["bundle_id"]    as? String ?? ""
        let tty         = payload["tty"]          as? String ?? ""
        let isCursorEditor = bundleId.lowercased() == "com.todesktop.230313mzl4w4u92"
        let isVSCodeEditor = !isCursorEditor && (
            termProgram.lowercased().contains("vscode") ||
            bundleId.lowercased().contains("vscode"))

        // Codex, Copilot CLI and Muse Code get the same approval card as Claude Code / Cursor.
        // Other external agents (any other coucou_agent) answer immediately with "ask"
        // so the agent re-asks in its own terminal — they do not get a notch card.
        #if !APPSTORE
        let isCodexRequest   = rawAgent == "codex"
        let isCopilotRequest = rawAgent == "copilot"
        let isMuseRequest    = rawAgent == "muse"
        // isHermesRequest is true only when the plugin sent coucou_has_transport: true,
        // meaning register_approval_transport is wired and Hermes will honour our choice.
        // Without that flag the request falls through to "ask" so Hermes handles it natively.
        let isHermesRequest  = rawAgent == "hermes"
            && UserDefaults.standard.bool(forKey: "hermesApprovalsEnabled")
            && (payload["coucou_has_transport"] as? Bool == true)
        // Coucou's own chat (Claude Code as chat provider): always a card, on the
        // Claude Code pill as it is.
        let isChatRequest    = payload["coucou_chat"] as? Bool == true
        #else
        let isCodexRequest   = false
        let isCopilotRequest = false
        let isMuseRequest    = false
        let isHermesRequest  = false
        let isChatRequest    = false
        #endif
        if !isCodexRequest && !isCopilotRequest && !isMuseRequest && !isHermesRequest && Self.validateAgent(rawAgent) != nil {
            Task.detached { [weak self] in
                self?.sendLine(fd: fd, text: #"{"permissionDecision":"ask"}"#)
                close(fd)
            }
            return
        }

        // Determine which workspace pill owns the request.
        let basePillId: String
        if isCodexRequest {
            basePillId = "agent_codex"
        } else if isCopilotRequest {
            basePillId = "agent_copilot"
        } else if isMuseRequest {
            basePillId = "agent_muse"
        } else if isHermesRequest {
            basePillId = "agent_hermes"
        } else if isCursorEditor {
            basePillId = "agent_cursor"
        } else {
            basePillId = isVSCodeEditor ? "integration_claude" : terminalWorkspace()
        }
        let isClaudeCode = !isCodexRequest && !isCopilotRequest && !isMuseRequest && !isHermesRequest
        let pillId = isClaudeCode ? claudePill(workspaceId: basePillId, sessionId: sessionId) : basePillId
        // Terminal sessions: only when turned on in Settings, else the terminal asks itself.
        let terminalHost = isCursorEditor || isVSCodeEditor ? nil
            : ClaudeHost.terminal(termProgram: termProgram, bundleId: bundleId)
        let isTerminal = terminalHost != nil && ClaudeHost.terminalCardsEnabled
        guard isCodexRequest || isCopilotRequest || isMuseRequest || isHermesRequest || isCursorEditor || isVSCodeEditor || isTerminal || isRemote || isChatRequest else {
            Task.detached { [weak self] in
                self?.sendLine(fd: fd, text: #"{"permissionDecision":"ask"}"#)
                close(fd)
            }
            return
        }

        let tool = payload["tool_name"] as? String ?? "Tool"
        let toolInput = payload["tool_input"] as? [String: Any] ?? [:]
        // A plan is too long for the card: show its title.
        let command = tool == "ExitPlanMode"
            ? Self.planTitle(toolInput["plan"] as? String)
            : toolInput["command"] as? String ?? tool
        let inputKey = Self.approvalInputKey(toolInput)
        nbLog("PermissionRequest \(tool) [\(pillId)]")

        // AskUserQuestion is now handled via the dedicated --ask PreToolUse hook.
        // If it still arrives here as a PermissionRequest, reply "ask" so Claude Code
        // re-asks in the terminal — never show the question twice.
        if tool == "AskUserQuestion" {
            Task.detached { [weak self] in
                self?.sendLine(fd: fd, text: #"{"permissionDecision":"ask"}"#)
                close(fd)
            }
            return
        }

        // On a call the island stays away: the terminal takes the question at once.
        if AppState.shared.inCall {
            Task.detached { [weak self] in
                self?.sendLine(fd: fd, text: #"{"permissionDecision":"ask"}"#)
                close(fd)
            }
            return
        }

        let entry = QueuedApproval(id: UUID(), fd: fd, sessionId: sessionId, pillId: pillId,
                                   projectName: projectName, cwd: cwd, tool: tool,
                                   command: command, inputKey: inputKey,
                                   hostApp: terminalHost?.bundleId, bundleId: bundleId,
                                   tty: tty, isChat: isChatRequest)
        guard approvalQueue.enqueue(entry) == .accepted else {
            // Queue full: the newest request falls back to its terminal ("ask"), as displacement did.
            nbLog("Approval queue full (\(ApprovalQueue.maxPending)): ask for \(tool) [\(pillId)]")
            Task.detached { [weak self] in
                self?.sendLine(fd: fd, text: #"{"permissionDecision":"ask"}"#)
                close(fd)
            }
            return
        }
        activeSessionId = sessionId

        // Monitor fd: if the client closes the connection (handled in the editor), drop only this
        // entry. The cancel handler closes the fd — never close it anywhere else.
        let id = entry.id
        let source = DispatchSource.makeReadSource(fileDescriptor: fd, queue: .main)
        source.setEventHandler { [weak self] in
            guard let self, self.approvalQueue.contains(id: id) else { return }
            self.dismissApprovals(ids: [id], note: self.handledNote(for: pillId))
        }
        source.setCancelHandler { close(fd) }
        source.resume()

        // Safety timeout, on this request's own clock (it was sent at arrival time):
        // cancel without sending a decision. The relay reads EOF and exits; the agent re-asks.
        // Copilot/Muse use 110s (their relay waits 118s but their hook timeout is 120s, leaving little margin).
        let waitTimeout: Double = (isCopilotRequest || isMuseRequest) ? 110 : 115
        let timeout = DispatchWorkItem { [weak self] in
            guard let self, self.approvalQueue.contains(id: id) else { return }
            self.dismissApprovals(ids: [id], note: self.waitingNote(for: pillId))
        }
        DispatchQueue.main.asyncAfter(deadline: .now() + waitTimeout, execute: timeout)
        approvalResources[id] = ApprovalResources(source: source, timeout: timeout)

        nbLog("Approval queued \(approvalQueue.count)/\(ApprovalQueue.maxPending) [\(pillId)]")
        SoundEngine.shared.play("approval")
        // First request: take over the card. Later ones wait their turn (FIFO) and only bump the counter.
        if approvalQueue.count == 1 {
            // Approval always forces the island open — user must be able to respond.
            // Save current focus so we can restore it when the card is dismissed.
            if focusBeforeApproval == nil { focusBeforeApproval = state.focusId }
            showApproval(entry)
            expandIfNeeded(to: .approval)
        } else if let head = approvalQueue.head {
            showApproval(head)
        }
    }

    /// A click on the card's buttons. Ignored for a moment after a new request took the card,
    /// so a double-click meant for the previous request never answers this one.
    @MainActor
    func clickApproval(_ decision: String, for id: UUID) {
        if approvalQueue.head?.id == id, !approvalGuard.accepts(id, at: ProcessInfo.processInfo.systemUptime) {
            return
        }
        sendApprovalDecision(decision, for: id)
    }

    // MARK: - Antigravity command approval (blocking — its PreToolUse hook waits)

    /// UserDefaults key of Settings → Antigravity → Approve commands from the notch.
    static let agyApprovalsKey = "agyApprovalsEnabled"

    /// Antigravity's PreToolUse hook needs this long (seconds) for a card to wait for a click:
    /// the hooks Coucou wrote before had 10 s and must be installed again.
    static let agyApprovalHookTimeout = 120

    /// The command to put on a card, and how long it may wait, when Antigravity is about to
    /// run a shell command it has no rule for; nil when Antigravity decides on its own.
    /// Runs on the socket's thread: reads UserDefaults and Antigravity's small config files.
    private static func agyApprovalGate(_ payload: [String: Any]) -> (command: String, waitSeconds: Double)? {
        #if APPSTORE
        return nil
        #else
        guard UserDefaults.standard.bool(forKey: agyApprovalsKey),
              payload["coucou_agent"] as? String == "antigravity" else { return nil }
        let tool = payload["tool_name"] as? String ?? ""
        guard tool == "run_command" || tool.hasSuffix(":run_command") else { return nil }
        let input = payload["tool_input"] as? [String: Any] ?? [:]
        let command = (input["command"] as? String ?? "").trimmingCharacters(in: .whitespacesAndNewlines)
        guard !command.isEmpty else { return nil }
        // Coucou gives up 10 s before Antigravity stops waiting for the hook.
        guard let timeout = agyPreToolTimeout(), timeout >= agyApprovalHookTimeout else { return nil }
        let rules = AgyPermissionRules.load(cwd: payload["cwd"] as? String ?? "")
        guard rules.verdict(for: command) == .unknown else { return nil }
        return (command, min(Double(timeout - 10), 110))
        #endif
    }

    /// Antigravity is waiting on its PreToolUse hook for a command: puts it on the approval
    /// card. Allow and Deny answer the hook; Reply in terminal, the timeout or Antigravity
    /// giving up hand the choice back to Antigravity's own prompt.
    @MainActor
    private func processAgyApproval(fd: Int32, payload: [String: Any], command: String, waitSeconds: Double) {
        let sessionId = payload["session_id"] as? String ?? "unknown"
        let cwd = payload["cwd"] as? String ?? ""
        let projectName = sessionName(payload, cwd: cwd)
        let pillId = "agent_antigravity"
        let tool = payload["tool_name"] as? String ?? "run_command"
        let inputKey = Self.approvalInputKey(payload["tool_input"] as? [String: Any] ?? [:])
        nbLog("Antigravity command waits for the notch [\(sessionId.prefix(8))]")

        let entry = QueuedApproval(id: UUID(), fd: fd, sessionId: sessionId, pillId: pillId,
                                   projectName: projectName, cwd: cwd, tool: tool,
                                   command: command, inputKey: inputKey,
                                   bundleId: payload["bundle_id"] as? String ?? "")
        guard approvalQueue.enqueue(entry) == .accepted else {
            nbLog("Antigravity command: \(ApprovalQueue.maxPending) already waiting, asked in Antigravity")
            Task.detached { [weak self] in
                self?.sendLine(fd: fd, text: #"{"permissionDecision":"ask"}"#)
                close(fd)
            }
            return
        }
        // Tells the hook the card is up, so it waits for a click instead of giving up after a
        // few seconds. Written before anything else can answer or close this fd.
        sendLine(fd: fd, text: #"{"pending":true}"#)
        activeSessionId = sessionId
        upsertExternalAgent(id: pillId, name: "antigravity", bundleId: payload["bundle_id"] as? String ?? "")
        AppState.shared.updateTask(id: pillId, state: .approval)
        SoundEngine.shared.play("approval")

        // The hook hung up (Antigravity stopped waiting, or the turn was cancelled): drop the
        // card. The cancel handler closes the fd — never close it anywhere else.
        let id = entry.id
        let source = DispatchSource.makeReadSource(fileDescriptor: fd, queue: .main)
        source.setEventHandler { [weak self] in
            guard let self, self.approvalQueue.contains(id: id) else { return }
            self.dismissApprovals(ids: [id], note: self.handledNote(for: pillId))
        }
        source.setCancelHandler { close(fd) }
        source.resume()
        // Give up before Antigravity does: the hook then answers "ask" and Antigravity asks itself.
        let timeout = DispatchWorkItem { [weak self] in
            guard let self, self.approvalQueue.contains(id: id) else { return }
            self.dismissApprovals(ids: [id], note: self.waitingNote(for: pillId))
        }
        DispatchQueue.main.asyncAfter(deadline: .now() + waitSeconds, execute: timeout)
        approvalResources[id] = ApprovalResources(source: source, timeout: timeout)

        if approvalQueue.count == 1 {
            if focusBeforeApproval == nil { focusBeforeApproval = AppState.shared.focusId }
            showApproval(entry)
            expandIfNeeded(to: .approval)
        } else if let head = approvalQueue.head {
            showApproval(head)
        }
    }

    #if !APPSTORE
    /// Timeout (seconds) of Coucou's PreToolUse hook in ~/.gemini/config/hooks.json, nil if absent.
    static func agyPreToolTimeout() -> Int? {
        guard let data = try? Data(contentsOf: agyHooksURL),
              let root = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any],
              let coucou = root["coucou"] as? [String: Any],
              let groups = coucou["PreToolUse"] as? [[String: Any]] else { return nil }
        for group in groups {
            for hook in group["hooks"] as? [[String: Any]] ?? [] {
                guard let cmd = hook["command"] as? String, cmd.contains("nb-hook") else { continue }
                // Antigravity's default when no timeout is written is 30 s.
                return (hook["timeout"] as? NSNumber)?.intValue ?? 30
            }
        }
        return nil
    }
    #endif


    /// Writes the decision to the waiting nb-hook and cleans up.
    /// `id` is the request the card showed: a click is never applied to a different request.
    @MainActor
    func sendApprovalDecision(_ decision: String, for id: UUID) {
        // Only intercept a demo card — a real card has a queue entry behind it.
        if DemoEngine.shared.isActive,
           AppState.shared.pendingApproval?.sessionId == "demo_session",
           approvalQueue.isEmpty {
            let s = AppState.shared
            s.pendingApproval = nil
            s.isPinned = false
            s.view = s.tasks.isEmpty ? .empty : .overview
            DemoEngine.shared.handleApprovalDecision(decision)
            return
        }
        guard approvalQueue.head?.id == id, let removed = detachApproval(id: id) else { return }
        let fd = removed.entry.fd
        let source = removed.source

        let json: String
        switch decision {
        case "allow":  json = #"{"permissionDecision":"allow"}"#
        case "always": json = #"{"permissionDecision":"always"}"#
        case "ask":    json = #"{"permissionDecision":"ask"}"#
        // Plan approval (ExitPlanMode): the hook turns "mode" into a setMode permission update.
        case "plan_auto":    json = #"{"permissionDecision":"allow","mode":"auto"}"#
        case "plan_default": json = #"{"permissionDecision":"allow","mode":"default"}"#
        // Reject the plan and stop Claude, so the change can be typed in the terminal.
        case "plan_change":  json = #"{"permissionDecision":"change_plan"}"#
        default:       json = #"{"permissionDecision":"deny"}"#
        }

        if Self.isAlive(fd: fd) {
            Task.detached { [weak self] in
                // Write decision while fd is still valid, then cancel source → cancel handler closes fd
                self?.sendLine(fd: fd, text: json)
                DispatchQueue.main.async { source?.cancel() }
            }
        } else {
            // Client already gone: nothing to answer. Drop it.
            nbLog("Dropped dead approval on decision \(removed.entry.tool) [\(removed.entry.pillId)]")
            source?.cancel()
        }
        // The recap only counts allow / always / deny.
        let recapDecision = ["plan_auto": "allow", "plan_default": "allow", "plan_change": "deny"][decision] ?? decision
        if decision != "ask" { RecapStore.shared.recordDecision(pillId: removed.entry.pillId, decision: recapDecision) }
        refreshApprovalCard(releasedPill: removed.entry.pillId, emptyNote: nil)
        if decision == "plan_change" { ClaudeHost.activate(removed.entry.hostApp) }
    }

    /// The plan's first non-empty line without its Markdown heading marks, for the card.
    static func planTitle(_ plan: String?) -> String {
        let firstLine = plan?.split(separator: "\n")
            .first { !$0.trimmingCharacters(in: .whitespaces).isEmpty }
        let title = firstLine.map { String($0.drop { $0 == "#" || $0 == " " }) } ?? ""
        return title.isEmpty ? "Plan" : title
    }

    // MARK: - Question request

    @MainActor
    private func processQuestionRequest(fd: Int32, parsed: AskQuestion, payload: [String: Any]) {
        let state = AppState.shared
        if state.inCall {
            Task.detached { [weak self] in
                self?.sendLine(fd: fd, text: #"{"permissionDecision":"ask"}"#)
                close(fd)
            }
            return
        }
        let sessionId = payload["session_id"] as? String
                     ?? payload["conversation_id"] as? String
                     ?? "unknown"
        let (cwd, projectName) = sessionPlace(payload)
        let isRemote = payload["coucou_remote"] is String

        let rawAgent    = payload["coucou_agent"] as? String ?? ""
        let termProgram = payload["term_program"]  as? String ?? ""
        let bundleId    = payload["bundle_id"]     as? String ?? ""
        let tty         = payload["tty"]           as? String ?? ""
        let isCursorEditor = bundleId.lowercased() == "com.todesktop.230313mzl4w4u92"
        let isVSCodeEditor = !isCursorEditor && (
            termProgram.lowercased().contains("vscode") ||
            bundleId.lowercased().contains("vscode"))
        #if !APPSTORE
        let isCodexRequest = rawAgent == "codex"
        #else
        let isCodexRequest = false
        #endif
        let pillId: String
        if isCodexRequest {
            pillId = "agent_codex"
        } else {
            let workspace = isCursorEditor ? "agent_cursor" : isVSCodeEditor ? "integration_claude" : terminalWorkspace()
            pillId = claudePill(workspaceId: workspace, sessionId: sessionId)
        }
        // Terminal sessions: only when turned on in Settings, else the terminal asks itself.
        let terminalHost = isCursorEditor || isVSCodeEditor ? nil
            : ClaudeHost.terminal(termProgram: termProgram, bundleId: bundleId)
        let isTerminal = terminalHost != nil && ClaudeHost.terminalCardsEnabled
        guard isCodexRequest || isCursorEditor || isVSCodeEditor || isTerminal || isRemote else {
            Task.detached { [weak self] in
                self?.sendLine(fd: fd, text: #"{"permissionDecision":"ask"}"#)
                close(fd)
            }
            return
        }

        // Several sessions can wait on a question at once: they queue, the card shows the
        // oldest, and each answer goes back to the session that asked. A session asking again
        // replaces its own older question; past the cap, the newest goes back to its terminal.
        let entry = QueuedQuestion(id: UUID(), fd: fd,
                                   sessionId: (sessionId == "unknown" || sessionId.isEmpty) ? "\(pillId)+\(cwd)" : sessionId,
                                   pillId: pillId, projectName: projectName, cwd: cwd, question: parsed)
        switch questions.enqueue(entry) {
        case .rejectedFull:
            nbLog("Question: \(QuestionQueue.maxPending) already waiting, asked in the terminal")
            Task.detached { [weak self] in
                self?.sendLine(fd: fd, text: #"{"permissionDecision":"ask"}"#)
                close(fd)
            }
            return
        case .accepted(let replaced?):
            let oldSource = questionSources.removeValue(forKey: replaced.id)
            let oldFD = replaced.fd
            Task.detached { [weak self] in
                self?.sendLine(fd: oldFD, text: #"{"permissionDecision":"ask"}"#)
                DispatchQueue.main.async { oldSource?.cancel() }
            }
        case .accepted(nil):
            break
        }
        activeSessionId = sessionId

        if pillId.hasPrefix(PillCatalog.sessionPillPrefix) {
            upsertSessionTask(id: pillId, projectName: projectName, cwd: cwd, bundleId: bundleId, tty: tty)
        } else {
            upsertWorkspaceTask(id: pillId, projectName: projectName, cwd: cwd, hostApp: terminalHost?.bundleId, bundleId: bundleId, tty: tty)
        }
        state.updateTask(id: pillId, state: .question)
        SoundEngine.shared.play("approval")

        // Monitor fd: if the agent closes the connection (stopped or answered in its own UI),
        // drop the question. The cancel handler closes the fd — never close it anywhere else.
        let entryId = entry.id
        let source = DispatchSource.makeReadSource(fileDescriptor: fd, queue: .main)
        source.setEventHandler { [weak self] in
            guard let self, self.questions.contains(id: entryId) else { return }
            self.removeQuestion(id: entryId)
        }
        source.setCancelHandler { close(fd) }
        source.resume()
        questionSources[entryId] = source

        DispatchQueue.main.asyncAfter(deadline: .now() + 120) { [weak self] in
            guard let self, self.questions.contains(id: entryId) else { return }
            // Send "ask" so nb-hook exits cleanly; Claude Code re-asks in terminal.
            self.removeQuestion(id: entryId, answer: #"{"permissionDecision":"ask"}"#)
        }

        showQuestionHead()
    }

    /// "VS Code", "Warp"… — where the session behind a Claude Code pill runs.
    @MainActor
    private func claudeHostName(pill: String) -> String {
        ClaudeHost.name(for: AppState.shared.tasks.first { $0.id == pill }?.hostApp)
    }

    /// Updates or transiently creates a workspace pill (VS Code or Cursor) task.
    /// If the task already exists (persistent), just updates name/cwd.
    /// If missing (transient), creates it and inserts after the main pill.
    @MainActor
    private func upsertWorkspaceTask(id: String, projectName: String, cwd: String = "", hostApp: String? = nil,
                                     bundleId: String = "", tty: String = "") {
        let state = AppState.shared
        if let idx = state.tasks.firstIndex(where: { $0.id == id }) {
            state.tasks[idx].name = projectName
            if !cwd.isEmpty { state.tasks[idx].sessionCwd = cwd }
            if PillCatalog.basePillId(id) == "integration_claude" || id == "workspace_claude" { state.tasks[idx].hostApp = hostApp }
            if !bundleId.isEmpty { state.tasks[idx].sessionBundleId = bundleId }
            if !tty.isEmpty { state.tasks[idx].sessionTTY = tty }
            return
        }
        // Transient: create and insert after the main pill
        let def = PillCatalog.definition(for: id)
        let color = def?.color ?? "#C0C4CC"
        let source = def?.source ?? .agent
        var task = AgentTask(id: id, name: projectName, color: color,
                             state: .idle, steps: [], source: source, isIntegration: true)
        if PillCatalog.basePillId(id) == "integration_claude" || id == "workspace_claude" { task.hostApp = hostApp }
        if !bundleId.isEmpty { task.sessionBundleId = bundleId }
        if !tty.isEmpty { task.sessionTTY = tty }
        if let mainIdx = state.tasks.firstIndex(where: { $0.id == state.mainPillId }) {
            state.tasks.insert(task, at: mainIdx + 1)
        } else {
            state.tasks.insert(task, at: 0)
        }
        if state.focusId == nil { state.focusId = id }
        state.syncMode()
    }

    // MARK: - Badge helpers

    @MainActor
    private func setPillBadge(id: String, badge: PillBadge) {
        let state = AppState.shared
        guard let idx = state.tasks.firstIndex(where: { $0.id == id }) else { return }
        state.tasks[idx].pillBadge = badge
    }

    @MainActor
    private func cancelIdleReset(id: String) {
        idleResetGeneration[id, default: 0] += 1
    }

    /// "⏳ In background: 3" — the ticker line while a finished turn waits on background work.
    private func backgroundStep(_ count: Int) -> String {
        "⏳ " + String(format: String(localized: "step.background %lld"), Int64(count))
    }

    @MainActor
    private func subagentFinishedInBackground(sessionId: String) {
        guard var wait = backgroundWaits[sessionId], wait.subagents > 0 else { return }
        wait.subagents -= 1
        backgroundWaits[sessionId] = wait
        if wait.total > 0 {
            appendStep(id: wait.pillId, step: backgroundStep(wait.total))
            return
        }
        // All done. The main agent normally resumes on its own; if it never does, end quietly.
        let pillId = wait.pillId
        DispatchQueue.main.asyncAfter(deadline: .now() + 60) {
            guard self.backgroundWaits[sessionId]?.total == 0 else { return }
            self.backgroundWaits[sessionId] = nil
            AppState.shared.updateTask(id: pillId, state: .idle)
        }
    }

    @MainActor
    private func clearPillBadge(id: String) {
        let state = AppState.shared
        guard let idx = state.tasks.firstIndex(where: { $0.id == id }) else { return }
        state.tasks[idx].pillBadge = nil
    }

    @MainActor
    private func appendStep(id: String, step: String) {
        let state = AppState.shared
        guard let idx = state.tasks.firstIndex(where: { $0.id == id }) else { return }
        state.tasks[idx].steps.append(step)
        if state.tasks[idx].steps.count > 20 { state.tasks[idx].steps.removeFirst() }
        state.tasks[idx].stepIndex = state.tasks[idx].steps.count - 1
    }

    /// The session's folder and pill name. A remote host's folder is not on this
    /// Mac: the name carries the host, and there is no folder to open here.
    private func sessionPlace(_ payload: [String: Any]) -> (cwd: String, name: String) {
        let cwd = payload["cwd"] as? String ?? ""
        let name = sessionName(payload, cwd: cwd)
        guard let host = payload["coucou_remote"] as? String else { return (cwd, name) }
        return ("", "\(name) · \(SSHDestination.label(host))")
    }

    // MARK: - Code view

    /// Keeps what the code view shows of a Claude Code session: phases, last edit, last command.
    @MainActor
    private func recordCode(event: String, pillId: String, payload: [String: Any], cwd: String, project: String) {
        let state = AppState.shared
        if event == "SessionEnd" {
            state.codeSessions[pillId] = nil
            // The code view has nothing left to show.
            if state.view == .code, state.codeSessions[state.focusId ?? ""]?.hasContent != true {
                state.view = state.tasks.isEmpty ? .empty : .overview
            }
            return
        }
        var session = state.codeSessions[pillId] ?? CodeSession(project: project, root: cwd)
        let tool = payload["tool_name"] as? String ?? ""
        switch event {
        case "UserPromptSubmit":
            session = CodeSession(project: project, root: cwd)
        case "PreToolUse":
            session.toolStarted(tool: tool, input: payload["tool_input"] as? [String: Any] ?? [:], cwd: cwd)
            if readsSnippet(tool, payload, session.edit) { session.awaitSnippet() }
        case "PostToolUse", "PostToolUseFailure":
            let failed = event == "PostToolUseFailure"
            session.toolFinished(tool: tool, response: payload["tool_response"], failed: failed, error: payload["error"] as? String)
            if !failed, readsSnippet(tool, payload, session.edit), let edit = session.edit {
                loadSnippet(edit, pillId: pillId, root: session.root, cwd: cwd)
            }
        case "Stop":
            session.endTurn()
        default:
            return
        }
        state.codeSessions[pillId] = session
    }

    /// Reads the lines around an edit off the main thread, then adds them if the edit is still the one shown.
    @MainActor
    private func loadSnippet(_ edit: CodeEdit, pillId: String, root: String, cwd: String) {
        let base = edit.path.hasPrefix(root + "/") ? root : cwd
        DispatchQueue.global(qos: .utility).async {
            let snippet = CodeView.readSnippet(path: edit.path, root: base, find: edit.added)
            Task { @MainActor in
                AppState.shared.codeSessions[pillId]?.snippetRead(snippet, for: edit)
            }
        }
    }

    /// An Edit made on this Mac: the lines around it are read from the file. A remote
    /// session's files are on its host.
    private func readsSnippet(_ tool: String, _ payload: [String: Any], _ edit: CodeEdit?) -> Bool {
        tool == "Edit" && payload["coucou_remote"] == nil && edit?.added.isEmpty == false
    }

    // MARK: - Project name alias mapping

    /// The session's own name (Claude Code's /rename, read by the hook), else its folder, `~` at home.
    private func sessionName(_ payload: [String: Any], cwd: String) -> String {
        if let name = payload["session_name"] as? String, !name.isEmpty { return name }
        if let title = payload["session_title"] as? String, !title.isEmpty { return title }
        // The folder the session started in, so a cd into a subfolder keeps the name.
        let root = payload["session_root"] as? String ?? ""
        let dir = root.isEmpty ? cwd : root
        if !dir.isEmpty, dir == (payload["home"] as? String ?? NSHomeDirectory()) { return "~" }
        let folder = URL(fileURLWithPath: dir).lastPathComponent
        return aliasProjectName(folder.isEmpty ? "Session" : folder)
    }

    private func aliasProjectName(_ name: String) -> String {
        let aliases: [String: String] = [
            "notch-buddy":  "Notch Buddy",
            "notchbuddy":   "Notch Buddy",
            "notch_buddy":  "Notch Buddy",
        ]
        return aliases[name.lowercased()] ?? name
    }

    // MARK: - Localized step labels

    private func localizedStep(tool: String, input: [String: Any]) -> String {
        let labels: [String: String] = [
            "Bash":         String(localized: "step.runs",       defaultValue: "Runs"),
            "Read":         String(localized: "step.reads",      defaultValue: "Reads"),
            "Write":        String(localized: "step.writes",     defaultValue: "Writes"),
            "Edit":         String(localized: "step.edits",      defaultValue: "Edits"),
            "Glob":         String(localized: "step.searches",   defaultValue: "Searches"),
            "Grep":         String(localized: "step.searches",   defaultValue: "Searches"),
            "WebSearch":    String(localized: "step.web-search", defaultValue: "Searches the web"),
            "WebFetch":     String(localized: "step.fetches",    defaultValue: "Fetches"),
            "TodoWrite":    String(localized: "step.tasks",      defaultValue: "Tasks"),
            "Task":         String(localized: "step.agent",      defaultValue: "Agent"),
            "LS":           String(localized: "step.lists",      defaultValue: "Lists"),
            "MultiEdit":    String(localized: "step.edits",      defaultValue: "Edits"),
            "NotebookEdit": String(localized: "step.notebook",   defaultValue: "Notebook"),
            // Codex tools
            "apply_patch":  String(localized: "step.edits",     defaultValue: "Edits"),
            "update_plan":  String(localized: "step.tasks",     defaultValue: "Tasks"),
            "spawn_agent":  String(localized: "step.agent",     defaultValue: "Agent"),
            // Antigravity tools
            "ask_question": String(localized: "step.needs-input", defaultValue: "Needs input"),
        ]
        let labelTool = tool == "ask_question" || tool.hasSuffix(":ask_question") ? "ask_question" : tool
        var label = labels[labelTool] ?? tool

        // Codex MCP tools arrive as mcp__server__tool — show "server · tool"
        if tool.hasPrefix("mcp__") {
            let rest = String(tool.dropFirst(5))
            let parts = rest.components(separatedBy: "__")
            label = parts.count >= 2 ? "\(parts[0]) · \(parts.dropFirst().joined(separator: "__"))" : rest
        }

        // Bash: infer a more precise verb from the command
        if tool == "Bash", let cmd = input["command"] as? String {
            return "\(bashVerb(cmd)) · \(oneLine(cmd))"
        }

        // apply_patch: extract the first file name from the patch
        if tool == "apply_patch", let patch = input["command"] as? String {
            for line in patch.split(separator: "\n") {
                for prefix in ["*** Update File: ", "*** Add File: ", "*** Delete File: "] {
                    if line.hasPrefix(prefix) {
                        let path = String(line.dropFirst(prefix.count))
                        return "\(label) · \(URL(fileURLWithPath: path).lastPathComponent)"
                    }
                }
            }
            return label
        }

        // ask_question: extract the question string
        if (tool == "ask_question" || tool.hasSuffix(":ask_question")),
           let questions = input["questions"] as? [[String: Any]],
           let firstQ = questions.first?["question"] as? String,
           !firstQ.trimmingCharacters(in: .whitespaces).isEmpty {
            return "\(label) · \(oneLine(firstQ))"
        }

        if let cmd = input["command"] as? String {
            return "\(label) · \(oneLine(cmd))"
        } else if let path = input["path"] as? String {
            return "\(label) · \(URL(fileURLWithPath: path).lastPathComponent)"
        } else if let file = input["file_path"] as? String {
            return "\(label) · \(URL(fileURLWithPath: file).lastPathComponent)"
        } else if let query = input["query"] as? String {
            return "\(label) · \(oneLine(query))"
        }
        return label
    }

    /// Infers a localized verb from a shell command's first word.
    private func bashVerb(_ command: String) -> String {
        let first = command.split(whereSeparator: { $0.isWhitespace }).first.map(String.init) ?? ""
        switch first {
        case "cat", "bat", "head", "tail", "less", "more", "nl": return String(localized: "step.reads",    defaultValue: "Reads")
        case "rg", "grep", "find", "fd", "ls", "tree", "wc":    return String(localized: "step.searches", defaultValue: "Searches")
        default: break
        }
        let testRunners = ["pytest", "vitest", "jest", "npm test", "npm run test",
                           "cargo test", "go test", "swift test", "make test",
                           "xcodebuild test", "unittest"]
        if testRunners.contains(where: { command.contains($0) }) { return String(localized: "step.tests", defaultValue: "Tests") }
        return String(localized: "step.runs", defaultValue: "Runs")
    }

    // MARK: - Live diff helpers

    @MainActor
    private func buildFileDiff(tool: String, input: [String: Any], pillId: String) -> FileDiff? {
        switch tool {
        case "Edit":
            guard let old = input["old_string"] as? String,
                  let new = input["new_string"] as? String,
                  let path = input["file_path"] as? String,
                  !old.isEmpty || !new.isEmpty else { return nil }
            let d = DiffEngine.fromEdit(old: old, new: new, path: path)
            return (d.added > 0 || d.removed > 0) ? d : nil

        case "MultiEdit":
            guard let path = input["file_path"] as? String,
                  let edits = input["edits"] as? [[String: Any]], !edits.isEmpty else { return nil }
            var totalAdded = 0, totalRemoved = 0, allHunks: [DiffHunk] = [], anyLarge = false
            for edit in edits {
                guard let old = edit["old_string"] as? String,
                      let new = edit["new_string"] as? String else { continue }
                let d = DiffEngine.fromEdit(old: old, new: new, path: path)
                totalAdded += d.added; totalRemoved += d.removed
                allHunks.append(contentsOf: d.hunks); if d.tooLarge { anyLarge = true }
            }
            guard totalAdded > 0 || totalRemoved > 0 else { return nil }
            return FileDiff(path: path, added: totalAdded, removed: totalRemoved,
                            hunks: allHunks, tooLarge: anyLarge, isNewFile: false)

        case "Write":
            guard let path = input["file_path"] as? String,
                  let content = input["content"] as? String, !content.isEmpty else { return nil }
            let d = DiffEngine.fromNew(content: content, path: path)
            return (d.added > 0 || d.removed > 0) ? d : nil

        default:
            return nil
        }
    }

    /// Collapses whitespace so a multi-line command stays one ticker row.
    private func oneLine(_ text: String, limit: Int = 60) -> String {
        let collapsed = text.split(whereSeparator: { $0.isNewline || $0 == "\t" })
                            .joined(separator: " ")
        return collapsed.count > limit ? String(collapsed.prefix(limit)) + "…" : collapsed
    }

    // MARK: - Logging

    private func nbLog(_ message: String) {
        appendAppLog("nb.log", message)
    }

    private func sendLine(fd: Int32, text: String) {
        let bytes = Array((text + "\n").utf8)
        bytes.withUnsafeBytes { buffer in
            var sent = 0
            while sent < buffer.count {
                let n = Darwin.send(fd, buffer.baseAddress! + sent, buffer.count - sent, 0)
                if n <= 0 { break }
                sent += n
            }
        }
    }

    // MARK: - nb-hook script installation

    func installHookScript() {
        #if APPSTORE
        // In App Store mode the script is written during settings hook installation
        // (requires NSOpenPanel to ~/.claude chosen by the user)
        #else
        let dir = Self.supportDir
        try? FileManager.default.createDirectory(at: dir, withIntermediateDirectories: true)
        try? FileManager.default.setAttributes([.posixPermissions: 0o700 as NSNumber], ofItemAtPath: dir.path)
        // nb-hook: shell wrapper (always exits 0, calls nb-hook.py via python3)
        let wrapperURL = URL(fileURLWithPath: Self.hookScriptPath)
        try? nbHookShellWrapper.write(to: wrapperURL, atomically: true, encoding: .utf8)
        _ = try? FileManager.default.setAttributes([.posixPermissions: 0o755 as NSNumber], ofItemAtPath: wrapperURL.path)
        // nb-hook.py: Python relay
        let pyURL = wrapperURL.deletingLastPathComponent().appendingPathComponent("nb-hook.py")
        try? nbHookPythonGitHub.write(to: pyURL, atomically: true, encoding: .utf8)
        _ = try? FileManager.default.setAttributes([.posixPermissions: 0o755 as NSNumber], ofItemAtPath: pyURL.path)
        #endif
    }

    // MARK: - Outdated hook detection

    /// Returns true if settings.json has a Coucou hook that needs updating:
    /// either a PermissionRequest hook with timeout < 120s, or the AskUserQuestion
    /// PreToolUse matcher is missing (requires Claude Code 2.1.85+).
    static func hooksNeedUpdate() -> Bool {
        #if APPSTORE
        // Sandboxed: ~/.claude can't be read here (homeDirectoryForCurrentUser is the
        // container). The relay script written at install carries the version it was
        // written with: an older one (1.1 wrote none) misses plan approval, session titles
        // and Claude Desktop routing, so Settings offers "Update hooks".
        guard UserDefaults.standard.bool(forKey: "coucouHooksInstalled") else { return false }
        return UserDefaults.standard.integer(forKey: appStoreHookVersionKey) < appStoreHookVersion
        #else
        let settingsURL = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".claude/settings.json")
        guard let data = try? Data(contentsOf: settingsURL),
              let settings = try? JSONSerialization.jsonObject(with: data) as? [String: Any],
              let hooks = settings["hooks"] as? [String: Any] else {
            return false
        }
        // Track whether any Coucou hook is installed at all
        var hasCoucouHooks = false

        if let permReqHooks = hooks["PermissionRequest"] as? [[String: Any]] {
            for matcher in permReqHooks {
                if let hookList = matcher["hooks"] as? [[String: Any]] {
                    for hook in hookList {
                        if let cmd = hook["command"] as? String, isLocalCoucouHook(cmd) {
                            hasCoucouHooks = true
                            if let timeout = hook["timeout"] as? Int, timeout < 120 { return true }
                        }
                    }
                }
            }
        }

        // Check that the AskUserQuestion PreToolUse entry exists
        if hasCoucouHooks {
            let preToolHooks = hooks["PreToolUse"] as? [[String: Any]] ?? []
            let hasAskEntry = preToolHooks.contains { m in
                (m["matcher"] as? String) == "AskUserQuestion"
                && (m["hooks"] as? [[String: Any]])?.contains {
                    isLocalCoucouHook($0["command"] as? String ?? "")
                } ?? false
            }
            if !hasAskEntry { return true }
        }
        return false
        #endif
    }

    #if APPSTORE
    /// Bump when nbHookPythonAppStore or the hooks written by buildHooksData change in a way
    /// the installed copy must catch up with. 1.1 wrote no version (0); 2.0 is 2.
    static let appStoreHookVersion = 2
    static let appStoreHookVersionKey = "coucouAppStoreHookVersion"
    #endif

    // MARK: - Claude Code settings.json hook installer

    private var _pendingHooksData: Data?
    /// The bytes of settings.json the pending preview was computed from.
    private var _pendingHooksOriginal: Data?

    /// Returns preview JSON without writing — call writeClaudeHooks() to confirm.
    func previewClaudeHooks() throws -> String {
        let (data, original) = try buildHooksData()
        _pendingHooksData = data
        _pendingHooksOriginal = original
        return String(data: data, encoding: .utf8) ?? ""
    }

    /// Writes the hooks to disk (call after user confirms preview).
    /// Refused if settings.json changed since the preview, or cannot be backed up.
    func writeClaudeHooks() throws {
        guard let data = _pendingHooksData else { return }
        let settingsURL = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".claude/settings.json")
        try ClaudeSettingsFile.write(data, to: settingsURL, expecting: _pendingHooksOriginal)
        _pendingHooksData = nil
        _pendingHooksOriginal = nil
    }

    private func buildHooksData() throws -> (data: Data, original: Data?) {
        let settingsURL = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".claude/settings.json")
        // Unreadable or invalid settings must stop here, never count as empty.
        let snapshot = try ClaudeSettingsFile.read(at: settingsURL)
        var settings = snapshot.object
        let hookPath = Self.hookScriptPath
        #if APPSTORE
        // Sandboxed apps create quarantined files; /bin/sh bypasses the quarantine flag
        let quotedCmd = "/bin/sh \"\(hookPath.replacingOccurrences(of: "\"", with: "\\\""))\""
        #else
        let quotedCmd = "\"\(hookPath.replacingOccurrences(of: "\"", with: "\\\""))\""
        #endif
        settings = try Self.withClaudeHooks(settings, command: quotedCmd)
        let data = try JSONSerialization.data(withJSONObject: settings, options: [.prettyPrinted, .sortedKeys])
        return (data, snapshot.bytes)
    }

    /// `settings` with Coucou's hooks for Claude Code, running `quotedCmd`; only the hooks `owns`
    /// claims are replaced. Also used for a remote host's settings.json.
    static func withClaudeHooks(_ original: [String: Any], command quotedCmd: String,
                                owns: (String) -> Bool = isLocalCoucouHook) throws -> [String: Any] {
        var settings = original
        let events: [(String, Int)] = [
            ("SessionStart", 10), ("SessionEnd", 10),
            ("UserPromptSubmit", 10),
            ("PreToolUse", 10), ("PostToolUse", 10), ("PostToolUseFailure", 10),
            ("PermissionRequest", 120),
            ("Notification", 10),
            ("Stop", 10), ("StopFailure", 10),
            ("SubagentStart", 10), ("SubagentStop", 10),
        ]
        // "hooks" in a shape we do not know is refused, never replaced.
        var hooks = try ClaudeSettingsFile.hooks(in: settings, name: "settings.json")
        for (event, timeout) in events {
            var existing = try ClaudeSettingsFile.hookGroups(in: hooks, event: event, name: "settings.json")
            existing = removingHooks(from: existing, where: owns)
            existing.append(["hooks": [["type": "command", "command": quotedCmd, "timeout": timeout]]])
            hooks[event] = existing
        }
        // Dedicated AskUserQuestion PreToolUse hook (Claude Code 2.1.85+, timeout 130s)
        var preToolUse = hooks["PreToolUse"] as? [[String: Any]] ?? []
        preToolUse.append([
            "matcher": "AskUserQuestion",
            "hooks": [["type": "command", "command": "\(quotedCmd) --ask", "timeout": 130]],
        ])
        hooks["PreToolUse"] = preToolUse
        settings["hooks"] = hooks
        return settings
    }

    func uninstallClaudeHooks() throws {
        let settingsURL = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".claude/settings.json")
        let snapshot = try ClaudeSettingsFile.read(at: settingsURL)
        guard snapshot.object["hooks"] is [String: Any] else { return }
        let settings = Self.withoutClaudeHooks(snapshot.object)
        let newData = try JSONSerialization.data(withJSONObject: settings, options: [.prettyPrinted, .sortedKeys])
        try ClaudeSettingsFile.write(newData, to: settingsURL, expecting: snapshot.bytes)
    }

    /// `settings` without the hooks `owns` claims; nothing else changed.
    static func withoutClaudeHooks(_ original: [String: Any],
                                   owns: (String) -> Bool = isLocalCoucouHook) -> [String: Any] {
        var settings = original
        guard var hooks = settings["hooks"] as? [String: Any] else { return settings }

        for key in hooks.keys {
            if let groups = hooks[key] as? [[String: Any]] {
                let matchers = removingHooks(from: groups, where: owns)
                if matchers.isEmpty { hooks.removeValue(forKey: key) }
                else { hooks[key] = matchers }
            }
        }
        settings["hooks"] = hooks
        return settings
    }

    // MARK: - Claude plan status line installer

    private var statusLinePreviousURL: URL {
        Self.supportDir.appendingPathComponent("statusline-previous.json")
    }

    /// Returns true if our statusLine command is installed in ~/.claude/settings.json.
    static func statusLineInstalled() -> Bool {
        let url = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".claude/settings.json")
        guard let data = try? Data(contentsOf: url),
              let settings = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any],
              let sl = settings["statusLine"] as? [String: Any],
              let cmd = sl["command"] as? String else { return false }
        return isLocalCoucouHook(cmd)
    }

    private var _pendingStatusLineData: Data?
    /// The bytes of settings.json the pending preview was computed from.
    private var _pendingStatusLineOriginal: Data?
    private var _pendingPreviousData: Data?
    private var _pendingDeletePrevious: Bool = false

    /// Returns a diff string (only the statusLine key: before → after) without writing anything.
    func previewStatusLine(install: Bool) throws -> String {
        let settingsURL = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".claude/settings.json")
        // Unreadable or invalid settings must stop here, never count as empty.
        let snapshot = try ClaudeSettingsFile.read(at: settingsURL)
        let settings = snapshot.object
        let hookPath = Self.hookScriptPath
        let quotedPath = hookPath.replacingOccurrences(of: "\"", with: "\\\"")
        let quotedCmd = "\"\(quotedPath)\" --statusline"

        // Reset pending side-effects
        _pendingPreviousData = nil
        _pendingDeletePrevious = false

        let oldSL = settings["statusLine"] as? [String: Any]
        let newSL: [String: Any]?

        if install {
            // Check that Python 3 is available (requires Command Line Tools)
            let clCheck = Process()
            clCheck.executableURL = URL(fileURLWithPath: "/usr/bin/xcode-select")
            clCheck.arguments = ["-p"]
            clCheck.standardOutput = FileHandle.nullDevice
            clCheck.standardError = FileHandle.nullDevice
            try? clCheck.run()
            clCheck.waitUntilExit()
            if clCheck.terminationStatus != 0 {
                throw NSError(domain: "Coucou", code: 1,
                              userInfo: [NSLocalizedDescriptionKey:
                                  "Command Line Tools are required but not installed. Run: xcode-select --install"])
            }

            if let existing = oldSL,
               let cmd = existing["command"] as? String, !isLocalCoucouHook(cmd) {
                // Keep existing object but swap command; save old for later restoration
                var updated = existing
                updated["command"] = quotedCmd
                newSL = updated
                _pendingPreviousData = try? JSONSerialization.data(withJSONObject: existing,
                                                                   options: [.prettyPrinted, .sortedKeys])
            } else if let existing = oldSL,
                      let cmd = existing["command"] as? String, isLocalCoucouHook(cmd) {
                // Already installed — rebuild to update path if needed, keep other fields
                var updated = existing
                updated["command"] = quotedCmd
                newSL = updated
            } else {
                newSL = ["type": "command", "command": quotedCmd]
            }
        } else {
            // Uninstall: only if it's ours
            if let cur = oldSL, let cmd = cur["command"] as? String, isLocalCoucouHook(cmd) {
                if let prevData = try? Data(contentsOf: statusLinePreviousURL),
                   let prevObj = (try? JSONSerialization.jsonObject(with: prevData)) as? [String: Any] {
                    newSL = prevObj
                    _pendingDeletePrevious = true
                } else {
                    newSL = nil
                }
            } else {
                newSL = oldSL  // not ours — leave unchanged
            }
        }

        // Build the full settings.json with the new statusLine
        var newSettings = settings
        if let sl = newSL {
            newSettings["statusLine"] = sl
        } else {
            newSettings.removeValue(forKey: "statusLine")
        }
        let data = try JSONSerialization.data(withJSONObject: newSettings,
                                              options: [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes])
        _pendingStatusLineData = data
        _pendingStatusLineOriginal = snapshot.bytes

        // Build a compact diff: show only the statusLine key before → after
        func slJSON(_ val: [String: Any]?) throws -> String {
            guard let v = val else { return "(none)" }
            let d = try JSONSerialization.data(withJSONObject: v, options: [.prettyPrinted, .sortedKeys])
            return String(data: d, encoding: .utf8) ?? "(none)"
        }
        let before = try slJSON(oldSL)
        let after  = try slJSON(newSL)
        return "statusLine\nBefore:\n\(before)\n\nAfter:\n\(after)"
    }

    /// Writes settings.json and commits side effects (call after user confirms).
    func writeStatusLine() throws {
        guard let data = _pendingStatusLineData else { return }
        let settingsURL = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".claude/settings.json")
        try ClaudeSettingsFile.write(data, to: settingsURL, expecting: _pendingStatusLineOriginal)
        // Commit side effects only after successful write
        if let prevData = _pendingPreviousData {
            try? prevData.write(to: statusLinePreviousURL, options: .atomic)
        }
        if _pendingDeletePrevious {
            try? FileManager.default.removeItem(at: statusLinePreviousURL)
        }
        _pendingStatusLineData = nil
        _pendingStatusLineOriginal = nil
        _pendingPreviousData = nil
        _pendingDeletePrevious = false
    }

    // MARK: - App Store: hooks via security-scoped bookmark

    #if APPSTORE
    /// Writes nb-hook script and updates settings.json in one shot.
    /// claudeURL must be a URL from NSOpenPanel (sandbox access is granted immediately — no security scope needed).
    func installAndWriteClaudeHooksAppStore(claudeURL: URL) throws {
        let (data, original) = try buildHooksData(claudeURL: claudeURL)

        // Write nb-hook (shell wrapper) + nb-hook.py (Python relay) into ~/.claude/coucou/
        let coucouDir = claudeURL.appendingPathComponent("coucou")
        try FileManager.default.createDirectory(at: coucouDir, withIntermediateDirectories: true)
        let wrapperURL = coucouDir.appendingPathComponent("nb-hook")
        try nbHookShellWrapper.write(to: wrapperURL, atomically: true, encoding: .utf8)
        _ = try? FileManager.default.setAttributes([.posixPermissions: 0o755 as NSNumber], ofItemAtPath: wrapperURL.path)
        let pyURL = coucouDir.appendingPathComponent("nb-hook.py")
        try nbHookPythonAppStore.write(to: pyURL, atomically: true, encoding: .utf8)
        _ = try? FileManager.default.setAttributes([.posixPermissions: 0o755 as NSNumber], ofItemAtPath: pyURL.path)

        // Write settings.json (with backup)
        let settingsURL = claudeURL.appendingPathComponent("settings.json")
        try ClaudeSettingsFile.write(data, to: settingsURL, expecting: original)
        UserDefaults.standard.set(true, forKey: "coucouHooksInstalled")
        UserDefaults.standard.set(Self.appStoreHookVersion, forKey: Self.appStoreHookVersionKey)
    }

    /// The settings.json Coucou would write, to show before writing (CLAUDE.md: never
    /// write ~/.claude/settings.json without showing the change first).
    func previewClaudeHooksAppStore(claudeURL: URL) throws -> String {
        let (data, _) = try buildHooksData(claudeURL: claudeURL)
        return String(data: data, encoding: .utf8) ?? ""
    }

    func uninstallClaudeHooksAppStore(claudeURL: URL) throws {
        let settingsURL = claudeURL.appendingPathComponent("settings.json")
        let snapshot = try ClaudeSettingsFile.read(at: settingsURL)
        var settings = snapshot.object
        guard var hooks = settings["hooks"] as? [String: Any] else { return }
        for key in hooks.keys {
            if let groups = hooks[key] as? [[String: Any]] {
                let matchers = removingHooks(from: groups, where: isLocalCoucouHook)
                if matchers.isEmpty { hooks.removeValue(forKey: key) }
                else { hooks[key] = matchers }
            }
        }
        settings["hooks"] = hooks
        let newData = try JSONSerialization.data(withJSONObject: settings, options: [.prettyPrinted, .sortedKeys])
        try ClaudeSettingsFile.write(newData, to: settingsURL, expecting: snapshot.bytes)
        UserDefaults.standard.set(false, forKey: "coucouHooksInstalled")
    }

    private func buildHooksData(claudeURL: URL) throws -> (data: Data, original: Data?) {
        let settingsURL = claudeURL.appendingPathComponent("settings.json")
        // Unreadable or invalid settings must stop here, never count as empty.
        let snapshot = try ClaudeSettingsFile.read(at: settingsURL)
        var settings = snapshot.object
        // Derive hook path from the panel-selected claudeURL (real ~/.claude, not container)
        let hookPath = claudeURL.appendingPathComponent("coucou/nb-hook").path
        let quotedCmd = "/bin/sh \"\(hookPath.replacingOccurrences(of: "\"", with: "\\\""))\""
        let events: [(String, Int)] = [
            ("SessionStart", 10), ("SessionEnd", 10),
            ("UserPromptSubmit", 10),
            ("PreToolUse", 10), ("PostToolUse", 10), ("PostToolUseFailure", 10),
            ("PermissionRequest", 120),
            ("Notification", 10),
            ("Stop", 10), ("StopFailure", 10),
            ("SubagentStart", 10), ("SubagentStop", 10),
        ]
        // "hooks" in a shape we do not know is refused, never replaced.
        var hooks = try ClaudeSettingsFile.hooks(in: settings, name: "settings.json")
        for (event, timeout) in events {
            var existing = try ClaudeSettingsFile.hookGroups(in: hooks, event: event, name: "settings.json")
            existing = removingHooks(from: existing, where: isLocalCoucouHook)
            existing.append(["hooks": [["type": "command", "command": quotedCmd, "timeout": timeout]]])
            hooks[event] = existing
        }
        // Dedicated AskUserQuestion PreToolUse hook (Claude Code 2.1.85+, timeout 130s)
        var preToolUse = hooks["PreToolUse"] as? [[String: Any]] ?? []
        preToolUse.append([
            "matcher": "AskUserQuestion",
            "hooks": [["type": "command", "command": "\(quotedCmd) --ask", "timeout": 130]],
        ])
        hooks["PreToolUse"] = preToolUse
        settings["hooks"] = hooks
        let data = try JSONSerialization.data(withJSONObject: settings, options: [.prettyPrinted, .sortedKeys])
        return (data, snapshot.bytes)
    }
    #endif

    // MARK: - Claude Code installed-state detection (both builds)

    /// True when ~/.claude/settings.json already routes Claude Code events to Coucou.
    /// Cursor sessions ride on these same hooks, so they share this state.
    static func claudeHooksInstalled() -> Bool {
        #if APPSTORE
        // Sandboxed: can't read ~/.claude directly — check the install flag set on write.
        return UserDefaults.standard.bool(forKey: "coucouHooksInstalled")
        #else
        let url = FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".claude/settings.json")
        guard let data = try? Data(contentsOf: url),
              let json = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any]
        else { return false }
        return coucouHooksPresent(inSettings: json)
        #endif
    }

    // MARK: - Removing Coucou's own hook entries (both builds)

    /// Removes entries containing "nb-hook" from a Gemini- or Claude-format groups array
    /// (matcher + hooks[]), handling the legacy flat format too (command at top level).
    /// Used by both paths' install/uninstall so only Coucou's own entries are removed;
    /// whole groups are dropped only when every entry in them was ours.
    private func removeNbHookEntries(from groups: [[String: Any]]) -> [[String: Any]] {
        groups.compactMap { group -> [String: Any]? in
            // Legacy flat entry — command at group level
            if let cmd = group["command"] as? String, cmd.contains("nb-hook") { return nil }
            // Group format — filter inner hooks
            if var innerHooks = group["hooks"] as? [[String: Any]] {
                innerHooks.removeAll { ($0["command"] as? String)?.contains("nb-hook") == true }
                if innerHooks.isEmpty { return nil }
                var updated = group
                updated["hooks"] = innerHooks
                return updated
            }
            return group
        }
    }

    // MARK: - Gemini CLI and Antigravity hook installers  (#if !APPSTORE only)

    #if !APPSTORE
    private static var geminiSettingsURL: URL {
        FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".gemini/settings.json")
    }
    private static var agyHooksURL: URL {
        FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".gemini/config/hooks.json")
    }

    // MARK: Installed-state detection

    static func geminiHooksInstalled() -> Bool {
        guard let data = try? Data(contentsOf: geminiSettingsURL),
              let settings = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any],
              let hooks = settings["hooks"] as? [String: Any] else { return false }
        for value in hooks.values {
            guard let groups = value as? [[String: Any]] else { continue }
            for group in groups {
                if let innerHooks = group["hooks"] as? [[String: Any]] {
                    for hook in innerHooks {
                        if let cmd = hook["command"] as? String,
                           cmd.contains("nb-hook"), cmd.contains("--agent gemini") { return true }
                    }
                }
                // Legacy flat entry
                if let cmd = group["command"] as? String,
                   cmd.contains("nb-hook"), cmd.contains("--agent gemini") { return true }
            }
        }
        return false
    }

    static func agyHooksInstalled() -> Bool {
        guard let data = try? Data(contentsOf: agyHooksURL),
              let root = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any],
              let coucou = root["coucou"] else { return false }
        let json = (try? JSONSerialization.data(withJSONObject: coucou))
            .flatMap { String(data: $0, encoding: .utf8) } ?? ""
        return json.contains("nb-hook")
    }

    // MARK: Gemini CLI – preview / write

    private var _pendingGeminiData: Data?
    private var _pendingGeminiFingerprint: String?

    func previewGeminiHooks(install: Bool) throws -> String {
        let url = Self.geminiSettingsURL
        let exists = FileManager.default.fileExists(atPath: url.path)
        if !install && !exists {
            throw NSError(domain: "CoucouNoop", code: 0, userInfo: [
                NSLocalizedDescriptionKey: "No Gemini CLI hooks to remove."
            ])
        }
        let current = exists ? try Data(contentsOf: url) : Data()
        _pendingGeminiFingerprint = sha256Hex(current)
        let newData = install ? try buildGeminiHooksData() : try withoutGeminiHooks()
        _pendingGeminiData = newData
        return String(data: newData, encoding: .utf8) ?? ""
    }

    func writeGeminiHooks() throws {
        guard let data = _pendingGeminiData, let fp = _pendingGeminiFingerprint else { return }
        let url = Self.geminiSettingsURL
        let current = (try? Data(contentsOf: url)) ?? Data()
        guard sha256Hex(current) == fp else {
            throw NSError(domain: "Coucou", code: 1, userInfo: [
                NSLocalizedDescriptionKey: "~/.gemini/settings.json changed since preview. Refresh and try again."
            ])
        }
        try writeJSONFile(data, to: url, suffix: "settings.json")
        _pendingGeminiData = nil
        _pendingGeminiFingerprint = nil
    }

    private func buildGeminiHooksData() throws -> Data {
        var settings = try Self.strictReadJSONObject(at: Self.geminiSettingsURL,
                                                     label: "~/.gemini/settings.json")
        if let raw = settings["hooks"], !(raw is [String: Any]) {
            throw NSError(domain: "Coucou", code: 2, userInfo: [
                NSLocalizedDescriptionKey: "~/.gemini/settings.json: \"hooks\" has an unexpected type — Coucou has not touched it."
            ])
        }
        let base = hookBase()
        // (Gemini event key, normalized event name passed via argv, timeout in ms)
        let events: [(String, String, Int)] = [
            ("SessionStart", "SessionStart", 10000),
            ("SessionEnd",   "SessionEnd",   10000),
            ("BeforeTool",   "PreToolUse",   5000),
            ("AfterTool",    "PostToolUse",  5000),
            ("BeforeAgent",  "UserPromptSubmit", 5000),
            ("AfterAgent",   "Stop",         5000),
        ]
        var hooks = settings["hooks"] as? [String: Any] ?? [:]
        for (geminiEvent, normalizedEvent, timeout) in events {
            if let raw = hooks[geminiEvent], !(raw is [[String: Any]]) {
                throw NSError(domain: "Coucou", code: 2, userInfo: [
                    NSLocalizedDescriptionKey: "~/.gemini/settings.json: \"hooks\"[\"\(geminiEvent)\"] has an unexpected type — Coucou has not touched it."
                ])
            }
            var groups = hooks[geminiEvent] as? [[String: Any]] ?? []
            // Remove legacy flat entries and groups whose inner hooks contain nb-hook
            groups = removeNbHookEntries(from: groups)
            let hookEntry: [String: Any] = [
                "type": "command",
                "command": "\(base) --agent gemini \(normalizedEvent)",
                "timeout": timeout,
            ]
            groups.append(["matcher": "*", "hooks": [hookEntry]])
            hooks[geminiEvent] = groups
        }
        settings["hooks"] = hooks
        return try JSONSerialization.data(withJSONObject: settings,
                                         options: [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes])
    }

    private func withoutGeminiHooks() throws -> Data {
        var settings = try Self.strictReadJSONObject(at: Self.geminiSettingsURL,
                                                     label: "~/.gemini/settings.json")
        if let raw = settings["hooks"], !(raw is [String: Any]) {
            throw NSError(domain: "Coucou", code: 2, userInfo: [
                NSLocalizedDescriptionKey: "~/.gemini/settings.json: \"hooks\" has an unexpected type — Coucou has not touched it."
            ])
        }
        if var hooks = settings["hooks"] as? [String: Any] {
            for key in hooks.keys {
                if let groups = hooks[key] as? [[String: Any]] {
                    let cleaned = removeNbHookEntries(from: groups)
                    if cleaned.isEmpty { hooks.removeValue(forKey: key) } else { hooks[key] = cleaned }
                }
            }
            if hooks.isEmpty { settings.removeValue(forKey: "hooks") } else { settings["hooks"] = hooks }
        }
        return try JSONSerialization.data(withJSONObject: settings,
                                         options: [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes])
    }

    // MARK: Antigravity – preview / write

    private var _pendingAgyData: Data?
    private var _pendingAgyFingerprint: String?

    func previewAgyHooks(install: Bool) throws -> String {
        let url = Self.agyHooksURL
        let exists = FileManager.default.fileExists(atPath: url.path)
        if !install && !exists {
            throw NSError(domain: "CoucouNoop", code: 0, userInfo: [
                NSLocalizedDescriptionKey: "No Antigravity hooks to remove."
            ])
        }
        let current = exists ? try Data(contentsOf: url) : Data()
        _pendingAgyFingerprint = sha256Hex(current)
        let newData = install ? try buildAgyHooksData() : try withoutAgyHooks()
        _pendingAgyData = newData
        return String(data: newData, encoding: .utf8) ?? ""
    }

    func writeAgyHooks() throws {
        guard let data = _pendingAgyData, let fp = _pendingAgyFingerprint else { return }
        let url = Self.agyHooksURL
        let current = (try? Data(contentsOf: url)) ?? Data()
        guard sha256Hex(current) == fp else {
            throw NSError(domain: "Coucou", code: 1, userInfo: [
                NSLocalizedDescriptionKey: "~/.gemini/config/hooks.json changed since preview. Refresh and try again."
            ])
        }
        try writeJSONFile(data, to: url, suffix: "hooks.json")
        _pendingAgyData = nil
        _pendingAgyFingerprint = nil
    }

    private func buildAgyHooksData() throws -> Data {
        var root = try Self.strictReadJSONObject(at: Self.agyHooksURL,
                                                 label: "~/.gemini/config/hooks.json")
        let base = hookBase()
        // PreToolUse / PostToolUse: tool-level hooks — use matcher group
        // PreInvocation / PostInvocation / Stop: lifecycle hooks — direct handler, no matcher
        // PreToolUse may wait for a click in the notch (Approve commands from the notch):
        // it answers in a moment otherwise, so the long timeout costs nothing.
        var coucou: [String: Any] = [:]
        for event in ["PreToolUse", "PostToolUse"] {
            let hook: [String: Any] = ["type": "command",
                                       "command": "\(base) --agent antigravity \(event)",
                                       "timeout": event == "PreToolUse" ? Self.agyApprovalHookTimeout : 10]
            coucou[event] = [["matcher": "*", "hooks": [hook]]]
        }
        for event in ["PreInvocation", "PostInvocation", "Stop"] {
            let hook: [String: Any] = ["type": "command",
                                       "command": "\(base) --agent antigravity \(event)",
                                       "timeout": 10]
            coucou[event] = [hook]
        }
        root["coucou"] = coucou
        return try JSONSerialization.data(withJSONObject: root,
                                         options: [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes])
    }

    private func withoutAgyHooks() throws -> Data {
        var root = try Self.strictReadJSONObject(at: Self.agyHooksURL,
                                                 label: "~/.gemini/config/hooks.json")
        root.removeValue(forKey: "coucou")
        return try JSONSerialization.data(withJSONObject: root,
                                         options: [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes])
    }

    // MARK: Shared helpers

    /// /bin/sh "<hookScriptPath>" — quoted for paths containing spaces (Application Support).
    private func hookBase() -> String {
        let path = Self.hookScriptPath.replacingOccurrences(of: "\"", with: "\\\"")
        return "/bin/sh \"\(path)\""
    }

    /// Reads a JSON object from url.
    /// Absent file → empty dict. Present but invalid → throws with a user-facing message.
    private static func strictReadJSONObject(at url: URL, label: String) throws -> [String: Any] {
        guard FileManager.default.fileExists(atPath: url.path) else { return [:] }
        let data: Data
        do { data = try Data(contentsOf: url) }
        catch {
            throw NSError(domain: "Coucou", code: 2, userInfo: [
                NSLocalizedDescriptionKey: "\(label) cannot be read — Coucou has not touched it."
            ])
        }
        guard let obj = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any] else {
            throw NSError(domain: "Coucou", code: 2, userInfo: [
                NSLocalizedDescriptionKey: "\(label) is not valid JSON — Coucou has not touched it."
            ])
        }
        return obj
    }

    /// Backs up the existing file (throws on failure), creates parent dirs, then atomically writes.
    private func writeJSONFile(_ data: Data, to url: URL, suffix: String) throws {
        let fm = FileManager.default
        if fm.fileExists(atPath: url.path) {
            let fmt = DateFormatter()
            fmt.locale = Locale(identifier: "en_US_POSIX")
            fmt.dateFormat = "yyyyMMdd-HHmmss"
            let backupURL = url.deletingLastPathComponent()
                .appendingPathComponent("\(suffix).bak-\(fmt.string(from: Date()))")
            do { try fm.copyItem(at: url, to: backupURL) }
            catch {
                throw NSError(domain: "Coucou", code: 3, userInfo: [
                    NSLocalizedDescriptionKey: "Could not back up \(url.lastPathComponent): \(error.localizedDescription)"
                ])
            }
        }
        try fm.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        try data.write(to: url, options: .atomic)
    }

    // MARK: - Codex hook installer  (#if !APPSTORE only)

    static var codexHooksURL: URL {
        FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".codex/hooks.json")
    }

    /// True when ~/.codex/hooks.json already routes Codex events to Coucou's nb-hook.
    static func codexHooksInstalled() -> Bool {
        guard let data = try? Data(contentsOf: codexHooksURL),
              let root = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any],
              let hooks = root["hooks"] as? [String: Any] else { return false }
        for value in hooks.values {
            guard let groups = value as? [[String: Any]] else { continue }
            for group in groups {
                if let innerHooks = group["hooks"] as? [[String: Any]] {
                    for hook in innerHooks {
                        if let cmd = hook["command"] as? String,
                           cmd.contains("nb-hook"), cmd.contains("--agent codex") { return true }
                    }
                }
            }
        }
        return false
    }

    private var _pendingCodexData: Data?
    private var _pendingCodexFingerprint: String?

    func previewCodexHooks(install: Bool) throws -> String {
        let url = Self.codexHooksURL
        let exists = FileManager.default.fileExists(atPath: url.path)
        if !install && !exists {
            throw NSError(domain: "CoucouNoop", code: 0, userInfo: [
                NSLocalizedDescriptionKey: "No Codex hooks to remove."
            ])
        }
        let current = exists ? try Data(contentsOf: url) : Data()
        _pendingCodexFingerprint = sha256Hex(current)
        let newData = install ? try buildCodexHooksData() : try withoutCodexHooks()
        _pendingCodexData = newData
        return String(data: newData, encoding: .utf8) ?? ""
    }

    func writeCodexHooks() throws {
        guard let data = _pendingCodexData, let fp = _pendingCodexFingerprint else { return }
        let url = Self.codexHooksURL
        let current = (try? Data(contentsOf: url)) ?? Data()
        guard sha256Hex(current) == fp else {
            throw NSError(domain: "Coucou", code: 1, userInfo: [
                NSLocalizedDescriptionKey: "~/.codex/hooks.json changed since preview. Refresh and try again."
            ])
        }
        try writeJSONFile(data, to: url, suffix: "hooks.json")
        _pendingCodexData = nil
        _pendingCodexFingerprint = nil
    }

    private func buildCodexHooksData() throws -> Data {
        var root = try Self.strictReadJSONObject(at: Self.codexHooksURL, label: "~/.codex/hooks.json")
        if let raw = root["hooks"], !(raw is [String: Any]) {
            throw NSError(domain: "Coucou", code: 2, userInfo: [
                NSLocalizedDescriptionKey: "~/.codex/hooks.json: \"hooks\" has an unexpected type — Coucou has not touched it."
            ])
        }
        let base = hookBase()
        // Events, timeouts in seconds (Codex format).
        // PermissionRequest uses 120s + a statusMessage shown in the Codex UI while waiting.
        let events: [(String, Int, String?)] = [
            ("SessionStart",    10,  nil),
            ("UserPromptSubmit", 10, nil),
            ("PreToolUse",      10,  nil),
            ("PermissionRequest", 120, "Waiting for your answer in the notch (Coucou)"),
            ("PostToolUse",     10,  nil),
            ("Stop",            10,  nil),
            ("SubagentStart",   10,  nil),
            ("SubagentStop",    10,  nil),
            ("Interrupt",        3,  nil),
            ("SessionEnd",       3,  nil),
        ]
        var hooks = root["hooks"] as? [String: Any] ?? [:]
        for (event, timeout, statusMsg) in events {
            if let raw = hooks[event], !(raw is [[String: Any]]) {
                throw NSError(domain: "Coucou", code: 2, userInfo: [
                    NSLocalizedDescriptionKey: "~/.codex/hooks.json: \"hooks\"[\"\(event)\"] has an unexpected type — Coucou has not touched it."
                ])
            }
            var groups = hooks[event] as? [[String: Any]] ?? []
            // Remove existing Coucou entries
            groups = removeNbHookEntries(from: groups)
            var hookEntry: [String: Any] = [
                "type": "command",
                "command": "\(base) --agent codex",
                "timeout": timeout,
            ]
            if let msg = statusMsg { hookEntry["statusMessage"] = msg }
            groups.append(["hooks": [hookEntry]])
            hooks[event] = groups
        }
        root["hooks"] = hooks
        return try JSONSerialization.data(withJSONObject: root,
                                         options: [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes])
    }

    private func withoutCodexHooks() throws -> Data {
        var root = try Self.strictReadJSONObject(at: Self.codexHooksURL, label: "~/.codex/hooks.json")
        if let raw = root["hooks"], !(raw is [String: Any]) {
            throw NSError(domain: "Coucou", code: 2, userInfo: [
                NSLocalizedDescriptionKey: "~/.codex/hooks.json: \"hooks\" has an unexpected type — Coucou has not touched it."
            ])
        }
        if var hooks = root["hooks"] as? [String: Any] {
            for key in hooks.keys {
                if let groups = hooks[key] as? [[String: Any]] {
                    let cleaned = removeNbHookEntries(from: groups)
                    if cleaned.isEmpty { hooks.removeValue(forKey: key) } else { hooks[key] = cleaned }
                }
            }
            if hooks.isEmpty { root.removeValue(forKey: "hooks") } else { root["hooks"] = hooks }
        }
        return try JSONSerialization.data(withJSONObject: root,
                                         options: [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes])
    }

    // MARK: - GitHub Copilot CLI hook installer

    static var copilotHooksURL: URL {
        FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".copilot/hooks/coucou.json")
    }

    /// True when ~/.copilot/hooks/coucou.json already routes Copilot events to Coucou's nb-hook.
    static func copilotHooksInstalled() -> Bool {
        guard let data = try? Data(contentsOf: copilotHooksURL),
              let root = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any],
              let hooks = root["hooks"] as? [String: Any] else { return false }
        for value in hooks.values {
            guard let entries = value as? [[String: Any]] else { continue }
            for entry in entries {
                if let cmd = entry["bash"] as? String,
                   cmd.contains("nb-hook"), cmd.contains("--agent copilot") { return true }
            }
        }
        return false
    }

    private var _pendingCopilotData: Data?
    private var _pendingCopilotFingerprint: String?

    func previewCopilotHooks(install: Bool) throws -> String {
        let url = Self.copilotHooksURL
        let exists = FileManager.default.fileExists(atPath: url.path)
        if !install && !exists {
            throw NSError(domain: "CoucouNoop", code: 0, userInfo: [
                NSLocalizedDescriptionKey: "No Copilot hooks to remove."
            ])
        }
        let current = exists ? try Data(contentsOf: url) : Data()
        _pendingCopilotFingerprint = sha256Hex(current)
        if install {
            let newData = try buildCopilotHooksData()
            _pendingCopilotData = newData
            return String(data: newData, encoding: .utf8) ?? ""
        } else {
            _pendingCopilotData = nil  // nil = delete signal
            return "(will delete \(url.path))"
        }
    }

    func writeCopilotHooks() throws {
        guard let fp = _pendingCopilotFingerprint else { return }
        let url = Self.copilotHooksURL
        let current = (try? Data(contentsOf: url)) ?? Data()
        guard sha256Hex(current) == fp else {
            throw NSError(domain: "Coucou", code: 1, userInfo: [
                NSLocalizedDescriptionKey: "~/.copilot/hooks/coucou.json changed since preview. Refresh and try again."
            ])
        }
        if let data = _pendingCopilotData {
            try writeJSONFile(data, to: url, suffix: "coucou.json")
        } else {
            // Uninstall: delete the file entirely
            try FileManager.default.removeItem(at: url)
        }
        _pendingCopilotData = nil
        _pendingCopilotFingerprint = nil
    }

    private func buildCopilotHooksData() throws -> Data {
        var root = try Self.strictReadJSONObject(at: Self.copilotHooksURL,
                                                  label: "~/.copilot/hooks/coucou.json")
        if let raw = root["hooks"], !(raw is [String: Any]) {
            throw NSError(domain: "Coucou", code: 2, userInfo: [
                NSLocalizedDescriptionKey: "~/.copilot/hooks/coucou.json: \"hooks\" has an unexpected type — Coucou has not touched it."
            ])
        }
        let base = hookBase()
        // Copilot CLI uses camelCase event names; each entry uses "bash" + "timeoutSec".
        // The event name is passed as a positional arg so the relay can fall back to it.
        // Copilot is fail-closed on permissionRequest — must always output valid JSON.
        let events: [(String, Int)] = [
            ("sessionStart",        10),
            ("userPromptSubmitted", 10),
            ("preToolUse",          10),
            ("permissionRequest",  120),
            ("postToolUse",         10),
            ("agentStop",           10),
            ("sessionEnd",           3),
            ("notification",        10),
        ]
        var hooks = root["hooks"] as? [String: Any] ?? [:]
        for (event, timeout) in events {
            if let raw = hooks[event], !(raw is [[String: Any]]) {
                throw NSError(domain: "Coucou", code: 2, userInfo: [
                    NSLocalizedDescriptionKey: "~/.copilot/hooks/coucou.json: \"hooks\"[\"\(event)\"] has an unexpected type — Coucou has not touched it."
                ])
            }
            var entries = hooks[event] as? [[String: Any]] ?? []
            entries.removeAll { ($0["bash"] as? String)?.contains("nb-hook") == true }
            entries.append(["type": "command", "bash": "\(base) --agent copilot \(event)", "timeoutSec": timeout])
            hooks[event] = entries
        }
        root["hooks"] = hooks
        root["version"] = 1
        return try JSONSerialization.data(withJSONObject: root,
                                         options: [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes])
    }

    private func withoutCopilotHooks() throws -> Data {
        var root = try Self.strictReadJSONObject(at: Self.copilotHooksURL,
                                                  label: "~/.copilot/hooks/coucou.json")
        if var hooks = root["hooks"] as? [String: Any] {
            for key in hooks.keys {
                if var entries = hooks[key] as? [[String: Any]] {
                    entries.removeAll { ($0["command"] as? String)?.contains("nb-hook") == true }
                    if entries.isEmpty { hooks.removeValue(forKey: key) } else { hooks[key] = entries }
                }
            }
            if hooks.isEmpty { root.removeValue(forKey: "hooks") } else { root["hooks"] = hooks }
        }
        return try JSONSerialization.data(withJSONObject: root,
                                         options: [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes])
    }

    // MARK: - Muse Code hook installer

    static var museSettingsURL: URL {
        FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".config/muse/settings.json")
    }

    /// True when ~/.config/muse/settings.json already routes Muse events to Coucou's nb-hook.
    static func museHooksInstalled() -> Bool {
        guard let data = try? Data(contentsOf: museSettingsURL),
              let settings = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any],
              let hooks = settings["hooks"] as? [String: Any] else { return false }
        for value in hooks.values {
            guard let groups = value as? [[String: Any]] else { continue }
            for group in groups {
                if let innerHooks = group["hooks"] as? [[String: Any]] {
                    for h in innerHooks {
                        if let cmd = h["command"] as? String,
                           cmd.contains("nb-hook"), cmd.contains("--agent muse") { return true }
                    }
                }
                if let cmd = group["command"] as? String,
                   cmd.contains("nb-hook"), cmd.contains("--agent muse") { return true }
            }
        }
        return false
    }

    private var _pendingMuseData: Data?
    private var _pendingMuseFingerprint: String?

    func previewMuseHooks(install: Bool) throws -> String {
        let url = Self.museSettingsURL
        let exists = FileManager.default.fileExists(atPath: url.path)
        if !install && !exists {
            throw NSError(domain: "CoucouNoop", code: 0, userInfo: [
                NSLocalizedDescriptionKey: "No Muse Code hooks to remove."
            ])
        }
        let current = exists ? try Data(contentsOf: url) : Data()
        _pendingMuseFingerprint = sha256Hex(current)
        let newData = install ? try buildMuseHooksData() : try withoutMuseHooks()
        _pendingMuseData = newData
        return String(data: newData, encoding: .utf8) ?? ""
    }

    func writeMuseHooks() throws {
        guard let data = _pendingMuseData, let fp = _pendingMuseFingerprint else { return }
        let url = Self.museSettingsURL
        let current = (try? Data(contentsOf: url)) ?? Data()
        guard sha256Hex(current) == fp else {
            throw NSError(domain: "Coucou", code: 1, userInfo: [
                NSLocalizedDescriptionKey: "~/.config/muse/settings.json changed since preview. Refresh and try again."
            ])
        }
        try writeJSONFile(data, to: url, suffix: "settings.json")
        _pendingMuseData = nil
        _pendingMuseFingerprint = nil
    }

    private func buildMuseHooksData() throws -> Data {
        var settings = try Self.strictReadJSONObject(at: Self.museSettingsURL,
                                                      label: "~/.config/muse/settings.json")
        if let raw = settings["hooks"], !(raw is [String: Any]) {
            throw NSError(domain: "Coucou", code: 2, userInfo: [
                NSLocalizedDescriptionKey: "~/.config/muse/settings.json: \"hooks\" has an unexpected type — Coucou has not touched it."
            ])
        }
        let base = hookBase()
        // Muse uses PascalCase events. Timeouts in milliseconds (seconds × 1000).
        let events: [(String, Int)] = [
            ("SessionStart",      10),
            ("UserPromptSubmit",   5),
            ("PreToolUse",         5),
            ("PermissionRequest", 120),
            ("PostToolUse",        5),
            ("Stop",               5),
            ("SessionEnd",         3),
        ]
        let isNew = !FileManager.default.fileExists(atPath: Self.museSettingsURL.path)
        var hooks = settings["hooks"] as? [String: Any] ?? [:]
        for (event, timeoutSec) in events {
            if let raw = hooks[event], !(raw is [[String: Any]]) {
                throw NSError(domain: "Coucou", code: 2, userInfo: [
                    NSLocalizedDescriptionKey: "~/.config/muse/settings.json: \"hooks\"[\"\(event)\"] has an unexpected type — Coucou has not touched it."
                ])
            }
            var groups = hooks[event] as? [[String: Any]] ?? []
            groups = removeNbHookEntries(from: groups)
            let hookEntry: [String: Any] = [
                "type": "command",
                "command": "\(base) --agent muse \(event)",
                "timeout": timeoutSec * 1000,
            ]
            groups.append(["matcher": "*", "hooks": [hookEntry]])
            hooks[event] = groups
        }
        settings["hooks"] = hooks
        if isNew { settings["schema_version"] = 1 }
        return try JSONSerialization.data(withJSONObject: settings,
                                         options: [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes])
    }

    private func withoutMuseHooks() throws -> Data {
        var settings = try Self.strictReadJSONObject(at: Self.museSettingsURL,
                                                      label: "~/.config/muse/settings.json")
        if let raw = settings["hooks"], !(raw is [String: Any]) {
            throw NSError(domain: "Coucou", code: 2, userInfo: [
                NSLocalizedDescriptionKey: "~/.config/muse/settings.json: \"hooks\" has an unexpected type — Coucou has not touched it."
            ])
        }
        if var hooks = settings["hooks"] as? [String: Any] {
            for key in hooks.keys {
                if let groups = hooks[key] as? [[String: Any]] {
                    let cleaned = removeNbHookEntries(from: groups)
                    if cleaned.isEmpty { hooks.removeValue(forKey: key) } else { hooks[key] = cleaned }
                }
            }
            if hooks.isEmpty { settings.removeValue(forKey: "hooks") } else { settings["hooks"] = hooks }
        }
        return try JSONSerialization.data(withJSONObject: settings,
                                         options: [.prettyPrinted, .sortedKeys, .withoutEscapingSlashes])
    }

    // MARK: - OpenCode plugin installer

    private var _pendingOpenCodeContent: String?
    private var _pendingOpenCodeFingerprint: String?

    static var openCodePluginURL: URL {
        FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".config/opencode/plugins/coucou.js")
    }

    static func openCodePluginInstalled() -> Bool {
        guard let content = try? String(contentsOf: openCodePluginURL, encoding: .utf8) else { return false }
        return content.contains("nb-hook") && content.contains("opencode")
    }

    private func buildOpenCodePluginContent() -> String {
        let path = Self.hookScriptPath
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "'", with: "\\'")
        return """
// Coucou hook plugin for OpenCode — generated by Coucou.app
// Forwards every event to the Coucou notch (fire-and-forget, never blocks).
import { spawn } from 'node:child_process';

const HOOK = '\(path)';
const EVENT_MAP = {
  'session.created': 'SessionStart',
  'session.idle': 'Stop',
  'session.error': 'StopFailure',
  'session.deleted': 'SessionEnd',
  'permission.asked': 'PermissionRequest',
};

function forward(hook_event_name, payload) {
  const p = spawn('/bin/sh', [HOOK, '--agent', 'opencode'],
                  { stdio: ['pipe', 'ignore', 'ignore'], detached: true });
  p.on('error', () => {});
  p.stdin.on('error', () => {});
  p.stdin.write(JSON.stringify({ hook_event_name, ...payload }) + '\\n');
  p.stdin.end();
  p.unref();
}

export const CoucouPlugin = async (_ctx) => ({
  event: async ({ event }) => {
    const hook_event_name = EVENT_MAP[event.type];
    if (!hook_event_name) return;
    const props = event.properties || {};
    const payload = {
      session_id: event.sessionID || event.session_id || props.sessionID || props.session_id || '',
      cwd: event.cwd || event.directory || props.cwd || props.directory || '',
    };
    if (typeof props.tool === 'string') payload.tool_name = props.tool;
    if (props.input != null) payload.tool_input = props.input;
    forward(hook_event_name, payload);
  },
  'tool.execute.before': async (input) => {
    forward('PreToolUse', {
      session_id: input.sessionID || input.session_id || '',
      cwd: input.cwd || '',
      tool_name: typeof input.tool === 'string' ? input.tool : '',
      tool_input: input.input ?? null,
    });
  },
  'tool.execute.after': async (input, _output) => {
    forward('PostToolUse', {
      session_id: input.sessionID || input.session_id || '',
      cwd: input.cwd || '',
      tool_name: typeof input.tool === 'string' ? input.tool : '',
    });
  },
});
"""
    }

    func previewOpenCodePlugin(install: Bool) throws -> String {
        let url = Self.openCodePluginURL
        let exists = FileManager.default.fileExists(atPath: url.path)
        if !install {
            guard exists else {
                throw NSError(domain: "CoucouNoop", code: 0, userInfo: [
                    NSLocalizedDescriptionKey: "No OpenCode plugin to remove."
                ])
            }
            let current = (try? Data(contentsOf: url)) ?? Data()
            _pendingOpenCodeFingerprint = sha256Hex(current)
            _pendingOpenCodeContent = nil
            return "(will delete \(url.path))"
        }
        let current = exists ? (try? Data(contentsOf: url)) ?? Data() : Data()
        _pendingOpenCodeFingerprint = sha256Hex(current)
        let content = buildOpenCodePluginContent()
        _pendingOpenCodeContent = content
        return content
    }

    func writeOpenCodePlugin() throws {
        guard let fp = _pendingOpenCodeFingerprint else { return }
        let url = Self.openCodePluginURL
        let current = (try? Data(contentsOf: url)) ?? Data()
        guard sha256Hex(current) == fp else {
            throw NSError(domain: "Coucou", code: 1, userInfo: [
                NSLocalizedDescriptionKey: "~/.config/opencode/plugins/coucou.js changed since preview. Refresh and try again."
            ])
        }
        guard let content = _pendingOpenCodeContent else {
            // Uninstall path: checked by removeOpenCodePlugin
            return
        }
        let fm = FileManager.default
        if fm.fileExists(atPath: url.path) {
            let fmt = DateFormatter()
            fmt.locale = Locale(identifier: "en_US_POSIX")
            fmt.dateFormat = "yyyyMMdd-HHmmss"
            let backupURL = url.deletingLastPathComponent()
                .appendingPathComponent("coucou.js.bak-\(fmt.string(from: Date()))")
            try fm.copyItem(at: url, to: backupURL)
        }
        try fm.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        try content.write(to: url, atomically: true, encoding: .utf8)
        _pendingOpenCodeContent = nil
        _pendingOpenCodeFingerprint = nil
    }

    func removeOpenCodePlugin() throws {
        let url = Self.openCodePluginURL
        guard FileManager.default.fileExists(atPath: url.path) else { return }
        let content = (try? String(contentsOf: url, encoding: .utf8)) ?? ""
        guard content.contains("generated by Coucou") else {
            throw NSError(domain: "Coucou", code: 3, userInfo: [
                NSLocalizedDescriptionKey: "~/.config/opencode/plugins/coucou.js was not generated by Coucou — not deleting it."
            ])
        }
        try FileManager.default.removeItem(at: url)
    }

    // MARK: - Amp plugin installer

    private var _pendingAmpContent: String?
    private var _pendingAmpFingerprint: String?

    static var ampPluginURL: URL {
        FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".config/amp/plugins/coucou.ts")
    }

    static func ampPluginInstalled() -> Bool {
        guard let content = try? String(contentsOf: ampPluginURL, encoding: .utf8) else { return false }
        return content.contains("nb-hook") && content.contains("'amp'")
    }

    private func buildAmpPluginContent() -> String {
        let path = Self.hookScriptPath
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "'", with: "\\'")
        return """
// Coucou hook plugin for Amp — generated by Coucou.app
// Forwards every event to the Coucou notch (display only, never blocks).
import { spawn } from 'node:child_process';

const HOOK = '\(path)';

function forward(event_name: string, fields: Record<string, unknown>): void {
  const payload = JSON.stringify({ hook_event_name: event_name, ...fields });
  const p = spawn('/bin/sh', [HOOK, '--agent', 'amp'],
                  { stdio: ['pipe', 'ignore', 'ignore'], detached: true });
  p.on('error', () => {});
  (p.stdin as import('node:stream').Writable).on('error', () => {});
  (p.stdin as import('node:stream').Writable).write(payload + '\\n');
  (p.stdin as import('node:stream').Writable).end();
  p.unref();
}

export default function (amp: any): void {
  amp.on('session.start', (e: any) => { forward('SessionStart',     { session_id: e.thread?.id ?? '' }); });
  amp.on('agent.start',   (e: any) => { forward('UserPromptSubmit', { session_id: e.thread?.id ?? '' }); });
  amp.on('tool.call',     (e: any) => { try { forward('PreToolUse', { session_id: e.thread?.id ?? '', tool_name: typeof e.tool === 'string' ? e.tool : '' }); } finally { return { action: 'allow' }; } });
  amp.on('tool.result',   (e: any) => { forward('PostToolUse',      { session_id: e.thread?.id ?? '' }); });
  amp.on('agent.end',     (e: any) => { forward('Stop',             { session_id: e.thread?.id ?? '' }); });
}
"""
    }

    func previewAmpPlugin(install: Bool) throws -> String {
        let url = Self.ampPluginURL
        let exists = FileManager.default.fileExists(atPath: url.path)
        if !install {
            guard exists else {
                throw NSError(domain: "CoucouNoop", code: 0, userInfo: [
                    NSLocalizedDescriptionKey: "No Amp plugin to remove."
                ])
            }
            let current = (try? Data(contentsOf: url)) ?? Data()
            _pendingAmpFingerprint = sha256Hex(current)
            _pendingAmpContent = nil
            return "(will delete \(url.path))"
        }
        let current = exists ? (try? Data(contentsOf: url)) ?? Data() : Data()
        _pendingAmpFingerprint = sha256Hex(current)
        let content = buildAmpPluginContent()
        _pendingAmpContent = content
        return content
    }

    func writeAmpPlugin() throws {
        guard let fp = _pendingAmpFingerprint else { return }
        let url = Self.ampPluginURL
        let current = (try? Data(contentsOf: url)) ?? Data()
        guard sha256Hex(current) == fp else {
            throw NSError(domain: "Coucou", code: 1, userInfo: [
                NSLocalizedDescriptionKey: "~/.config/amp/plugins/coucou.ts changed since preview. Refresh and try again."
            ])
        }
        guard let content = _pendingAmpContent else {
            // Uninstall path: checked by removeAmpPlugin
            return
        }
        let fm = FileManager.default
        if fm.fileExists(atPath: url.path) {
            let fmt = DateFormatter()
            fmt.locale = Locale(identifier: "en_US_POSIX")
            fmt.dateFormat = "yyyyMMdd-HHmmss"
            let backupURL = url.deletingLastPathComponent()
                .appendingPathComponent("coucou.ts.bak-\(fmt.string(from: Date()))")
            try fm.copyItem(at: url, to: backupURL)
        }
        try fm.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        try content.write(to: url, atomically: true, encoding: .utf8)
        _pendingAmpContent = nil
        _pendingAmpFingerprint = nil
    }

    func removeAmpPlugin() throws {
        let url = Self.ampPluginURL
        guard FileManager.default.fileExists(atPath: url.path) else { return }
        let content = (try? String(contentsOf: url, encoding: .utf8)) ?? ""
        guard content.contains("generated by Coucou") else {
            throw NSError(domain: "Coucou", code: 3, userInfo: [
                NSLocalizedDescriptionKey: "~/.config/amp/plugins/coucou.ts was not generated by Coucou — not deleting it."
            ])
        }
        try FileManager.default.removeItem(at: url)
    }

    // MARK: - Hermes plugin installer

    private var _pendingHermesPluginContent: String?
    private var _pendingHermesPluginFingerprint: String?
    private var _pendingHermesConfigContent: String?
    private var _pendingHermesConfigFingerprint: String?

    static var hermesPluginDir: URL {
        FileManager.default.homeDirectoryForCurrentUser
            .appendingPathComponent(".hermes/plugins/coucou")
    }
    static var hermesInitPyURL: URL { hermesPluginDir.appendingPathComponent("__init__.py") }
    static var hermesPluginYamlURL: URL { hermesPluginDir.appendingPathComponent("plugin.yaml") }
    static var hermesConfigURL: URL {
        FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".hermes/config.yaml")
    }

    static func hermesPluginInstalled() -> Bool {
        guard let content = try? String(contentsOf: hermesInitPyURL, encoding: .utf8) else { return false }
        return content.contains("nb-hook") && content.contains("hermes")
    }

    /// Returns true if the installed Hermes version exposes register_approval_transport.
    /// Runs a quick python3 import check; returns false on any error or if hermes is not installed.
    /// Finds the `hermes` executable in PATH and common install locations.
    private static func hermesExecutablePath() -> String? {
        // Try PATH via `which` first
        let which = Process()
        which.executableURL = URL(fileURLWithPath: "/usr/bin/which")
        which.arguments = ["hermes"]
        let pipe = Pipe()
        which.standardOutput = pipe
        which.standardError  = Pipe()
        if (try? which.run()) != nil {
            which.waitUntilExit()
            if which.terminationStatus == 0 {
                let out = String(data: pipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8)?
                    .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
                if !out.isEmpty && FileManager.default.isExecutableFile(atPath: out) { return out }
            }
        }
        // Explicit common locations
        let home = FileManager.default.homeDirectoryForCurrentUser.path
        for candidate in [
            "\(home)/.local/bin/hermes",
            "\(home)/.hermes/bin/hermes",
            "/usr/local/bin/hermes",
            "/opt/homebrew/bin/hermes",
        ] {
            if FileManager.default.isExecutableFile(atPath: candidate) { return candidate }
        }
        return nil
    }

    /// Reads the shebang of `executablePath` and returns the interpreter path.
    /// Handles `#!/usr/bin/env python3` by resolving via `which`.
    private static func interpreterFromShebang(_ executablePath: String) -> String? {
        guard let fh = FileHandle(forReadingAtPath: executablePath) else { return nil }
        let data = fh.readData(ofLength: 512)
        try? fh.close()
        guard let text = String(data: data, encoding: .utf8),
              text.hasPrefix("#!") else { return nil }
        let line = String(text.prefix(while: { $0 != "\n" }).dropFirst(2))
            .trimmingCharacters(in: .whitespaces)
        if line.hasPrefix("/usr/bin/env ") {
            let name = String(line.dropFirst("/usr/bin/env ".count))
                .trimmingCharacters(in: .whitespaces)
            let task = Process(); let pipe = Pipe()
            task.executableURL = URL(fileURLWithPath: "/usr/bin/which")
            task.arguments = [name]; task.standardOutput = pipe; task.standardError = Pipe()
            if (try? task.run()) != nil {
                task.waitUntilExit()
                let out = String(data: pipe.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8)?
                    .trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
                return out.isEmpty ? nil : out
            }
            return nil
        }
        return line.isEmpty ? nil : line
    }

    /// Returns true if the installed Hermes version exposes register_approval_transport.
    /// Finds the hermes binary, reads its shebang to get the right interpreter (never uses
    /// system python3), and runs an import check with a 3-second timeout.
    /// Returns false if hermes/interpreter not found, or import fails.
    #if !APPSTORE
    static func hermesSupportsApprovalTransport() -> Bool {
        guard let hermesPath = hermesExecutablePath(),
              let pythonPath = interpreterFromShebang(hermesPath),
              FileManager.default.isExecutableFile(atPath: pythonPath) else { return false }
        let task = Process()
        task.executableURL = URL(fileURLWithPath: pythonPath)
        task.arguments     = ["-c", "from hermes_cli.approval_transport import ApprovalRequest"]
        task.standardOutput = Pipe(); task.standardError = Pipe()
        do { try task.run() } catch { return false }
        // Wait up to 3 seconds
        let group = DispatchGroup(); group.enter()
        var exited = false
        DispatchQueue.global(qos: .background).async { task.waitUntilExit(); exited = true; group.leave() }
        if group.wait(timeout: .now() + 3) == .timedOut { task.terminate(); return false }
        return task.terminationStatus == 0
    }
    #endif

    private func buildHermesInitPy() -> String {
        let path = Self.hookScriptPath
            .replacingOccurrences(of: "\\", with: "\\\\")
            .replacingOccurrences(of: "'", with: "\\'")
        return """
# Coucou hook plugin for Hermes Agent — generated by Coucou.app
# Session/tool events → Coucou notch (fire-and-forget, never blocks).
# Approval transport: uses register_approval_transport when available (future Hermes),
# falls back to pre_approval_request observer-only hook (hermes 0.15.x).
import json, os, subprocess, threading
from pathlib import Path

HOOK = Path('\(path)')
_lock = threading.Lock()
# Maps session_id → metadata dict. Keeps correct session when multiple
# sessions run concurrently (gateway mode). _current_session_id is kept as
# a last-seen fallback for hooks that don't supply a session_id.
_sessions: dict = {}
_current_session_id = ''


def _fire(fields: dict) -> None:
    \"\"\"Non-blocking: spawn nb-hook and return immediately. Reaps child to avoid zombies.\"\"\"
    def _run() -> None:
        try:
            p = subprocess.Popen(
                [str(HOOK), '--agent', 'hermes'],
                stdin=subprocess.PIPE,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,   # detach from process group
                env={'__CFBundleIdentifier': 'com.nousresearch.hermes', **os.environ},
            )
            p.stdin.write(json.dumps(fields).encode() + b'\\n')
            p.stdin.close()
            p.wait(timeout=5)             # reap; 5s >> the 0.3s socket timeout
        except Exception:
            pass
    threading.Thread(target=_run, daemon=True).start()


def register(ctx) -> None:
    def on_session_start(**kwargs) -> None:
        global _current_session_id
        sid = kwargs.get('session_id', '')
        meta = {
            'model': kwargs.get('model', ''),
            'platform': kwargs.get('platform', 'cli') or 'cli',
        }
        with _lock:
            _sessions[sid] = meta
            _current_session_id = sid
        _fire({'hook_event_name': 'SessionStart', 'session_id': sid, 'platform': meta['platform']})

    def on_session_end(**kwargs) -> None:
        sid = kwargs.get('session_id', '')
        with _lock:
            _sessions.pop(sid, None)
        # Stop is sent by post_llm_call (which has the last assistant message).
        # Only send StopFailure here when the session was interrupted abnormally.
        if kwargs.get('interrupted'):
            _fire({'hook_event_name': 'StopFailure', 'session_id': sid})

    def post_llm_call(**kwargs) -> None:
        sid = kwargs.get('session_id', '') or _current_session_id
        response = kwargs.get('assistant_response', '')
        _fire({
            'hook_event_name': 'Stop',
            'session_id': sid,
            'last_assistant_message': response,
        })

    def pre_tool_call(**kwargs) -> None:
        sid = kwargs.get('session_id', '') or _current_session_id
        _fire({
            'hook_event_name': 'PreToolUse',
            'session_id': sid,
            'tool_name': kwargs.get('tool_name', ''),
            'tool_input': kwargs.get('args') or {},
        })

    def post_tool_call(**kwargs) -> None:
        sid = kwargs.get('session_id', '') or _current_session_id
        _fire({
            'hook_event_name': 'PostToolUse',
            'session_id': sid,
            'tool_name': kwargs.get('tool_name', ''),
        })

    ctx.register_hook('on_session_start', on_session_start)
    ctx.register_hook('on_session_end',   on_session_end)
    ctx.register_hook('post_llm_call',    post_llm_call)
    ctx.register_hook('pre_tool_call',    pre_tool_call)
    ctx.register_hook('post_tool_call',   post_tool_call)

    if hasattr(ctx, 'register_approval_transport'):
        # Hermes version supports transport API — Coucou shows a real Allow/Deny card
        # and returns the user's choice to Hermes.
        def _present(request) -> object:
            sid = (getattr(request, 'session_id', None)
                   or getattr(request, 'session_key', None)
                   or _current_session_id)
            cmd     = getattr(request, 'command', '')
            desc    = getattr(request, 'description', '')
            timeout = getattr(request, 'timeout_seconds',
                              getattr(request, 'timeout', 30.0))
            allowed = list(getattr(request, 'allowed_choices', ('once', 'deny')))

            payload = json.dumps({
                'hook_event_name': 'PermissionRequest',
                'session_id': sid,
                'coucou_agent': 'hermes',
                'coucou_has_transport': True,
                'tool_name': cmd,
                'tool_input': {'command': cmd, 'description': desc},
            }).encode()
            try:
                result = subprocess.run(
                    [str(HOOK), '--agent', 'hermes'],
                    input=payload,
                    capture_output=True,
                    timeout=max(1.0, float(timeout) - 2.0),
                )
                data   = json.loads(result.stdout)
                choice = data['choice']
                if choice not in allowed:
                    raise ValueError(f'invalid choice: {choice!r}')
                return request.respond(choice)
            except Exception:
                # Fall back to Hermes' native prompt on any error.
                return request.respond('deny')

        ctx.register_approval_transport('coucou', _present)
    else:
        # Observer-only hook (hermes 0.15.x): Hermes still controls the decision.
        # Fire a PreToolUse-style step so the notch shows "⏳ Approval pending in Hermes"
        # in the step list without displaying a fake Allow/Deny card.
        def pre_approval_request(**kwargs) -> None:
            sid = kwargs.get('session_key', '') or _current_session_id
            _fire({
                'hook_event_name': 'PreToolUse',
                'session_id': sid,
                'tool_name': '⏳ Approval pending in Hermes',
                'tool_input': {
                    'command': kwargs.get('command', ''),
                    'description': kwargs.get('description', ''),
                },
            })

        ctx.register_hook('pre_approval_request', pre_approval_request)
"""
    }

    private static let hermesPluginYaml = """
name: coucou
version: "1.0"
description: Coucou notch integration — generated by Coucou.app
"""

    /// Merges Coucou keys into a Hermes config.yaml string without touching other settings.
    ///
    /// Returns the merged YAML string, or nil if the file uses an unsupported structure
    /// (flow maps `{…}`, YAML anchors `&`, multi-document `---`) that the line-level
    /// merger cannot safely handle. Callers should surface an error with the lines
    /// the user needs to add manually.
    ///
    /// Plugin enablement (plugins.enabled) is handled by the `hermes plugins enable/disable`
    /// CLI after the plugin files are written; this function only manages security.approval.
    static func mergedHermesConfig(_ base: String, enableApprovals: Bool) -> String? {
        // Reject structures the simple merger cannot handle safely.
        // Flow maps, anchors, and multi-document markers require a full YAML parser.
        let unsafePatterns = ["{", " &", "\n---"]
        for p in unsafePatterns where base.contains(p) {
            return nil
        }

        var lines = base.components(separatedBy: "\n")

        // Detect file indentation: look for the first indented line and count spaces.
        let indent: Int = {
            for line in lines {
                let leading = line.prefix(while: { $0 == " " }).count
                if leading > 0 && leading <= 8 { return leading }
            }
            return 2  // default
        }()
        let ind  = String(repeating: " ", count: indent)         // e.g. "  " (2) or "    " (4)
        let ind2 = String(repeating: " ", count: indent * 2)     // one extra level

        // Returns the index of the first top-level section header line matching `key`.
        // Top-level = no leading spaces, ends with `:` (optionally with trailing space/comment).
        func topLevelIndex(key: String) -> Int? {
            lines.firstIndex { line in
                let t = line.trimmingCharacters(in: .whitespaces)
                guard !line.hasPrefix(" ") && !line.hasPrefix("\t") else { return false }
                return t == "\(key):" || t.hasPrefix("\(key):")
            }
        }

        // Returns the range of lines that belong to a top-level section (from its header to
        // just before the next top-level section, or the end of the array).
        func sectionRange(from sectionIdx: Int) -> Range<Int> {
            var end = sectionIdx + 1
            while end < lines.count {
                let l = lines[end]
                // A new top-level key: not blank, not a comment, no leading whitespace
                if !l.isEmpty && !l.hasPrefix("#") && !l.hasPrefix(" ") && !l.hasPrefix("\t") {
                    break
                }
                end += 1
            }
            return sectionIdx ..< end
        }

        // --- security.approval ---
        let transportLine = "\(ind2)transport: coucou"
        let fallbackLine  = "\(ind2)transport_fallback: builtin"

        func ensureApprovalTransport() {
            if let secIdx = topLevelIndex(key: "security") {
                let secRange = sectionRange(from: secIdx)
                // Look for approval: within the security section (must be indented)
                if let approvalIdx = (secRange.lowerBound + 1 ..< secRange.upperBound)
                    .first(where: { lines[$0].trimmingCharacters(in: .whitespaces).hasPrefix("approval:") }) {
                    // approval: block exists — update or add transport keys within it
                    let approvalRange = sectionRange(from: approvalIdx)
                    var hasTransport = false
                    var hasFallback  = false
                    for i in (approvalRange.lowerBound + 1 ..< approvalRange.upperBound) {
                        let t = lines[i].trimmingCharacters(in: .whitespaces)
                        if t.hasPrefix("transport:") && !t.hasPrefix("transport_fallback") {
                            lines[i] = transportLine; hasTransport = true
                        } else if t.hasPrefix("transport_fallback:") {
                            lines[i] = fallbackLine; hasFallback = true
                        }
                    }
                    let insertAt = approvalRange.lowerBound + 1
                    if !hasFallback  { lines.insert(fallbackLine,  at: insertAt) }
                    if !hasTransport { lines.insert(transportLine, at: insertAt) }
                } else {
                    // No approval: key — insert right after security:
                    let insertAt = secIdx + 1
                    lines.insert("\(ind)approval:", at: insertAt)
                    lines.insert(transportLine,     at: insertAt + 1)
                    lines.insert(fallbackLine,      at: insertAt + 2)
                }
            } else {
                // No security: section — append
                if lines.last != "" { lines.append("") }
                lines.append("security:")
                lines.append("\(ind)approval:")
                lines.append(transportLine)
                lines.append(fallbackLine)
            }
        }

        func removeApprovalTransport() {
            // Only remove exact Coucou-written transport keys; don't touch unrelated keys.
            lines.removeAll { line in
                let t = line.trimmingCharacters(in: .whitespaces)
                return t == "transport: coucou" || t == "transport_fallback: builtin"
            }
        }

        // --- plugins.enabled ---
        // Note: actual plugin enable/disable is done via `hermes plugins enable/disable coucou`
        // CLI after writing/removing the plugin files. This block ensures the preview
        // shows the complete intended state of config.yaml.
        func ensureCoucouPlugin() {
            // "Already present" = coucou in the plugins.enabled list specifically.
            // Check by finding plugins: section first, then enabled: sub-key within it.
            if let pluginsIdx = topLevelIndex(key: "plugins") {
                let pluginsRange = sectionRange(from: pluginsIdx)
                // Find enabled: within the plugins section
                if let enabledIdx = (pluginsRange.lowerBound + 1 ..< pluginsRange.upperBound)
                    .first(where: { lines[$0].trimmingCharacters(in: .whitespaces).hasPrefix("enabled:") }) {
                    let enabledLine = lines[enabledIdx]
                    let trimmed = enabledLine.trimmingCharacters(in: .whitespaces)
                    if trimmed.contains("[") && trimmed.contains("]") {
                        // Inline list: enabled: [x, y]  or  enabled: []
                        if trimmed.contains("coucou") { return }   // already present
                        if trimmed == "enabled: []" || trimmed == "enabled:[]" {
                            // Empty inline list → expand to block entry
                            let prefix = enabledLine.prefix(while: { $0 == " " })
                            lines[enabledIdx] = "\(prefix)enabled:"
                            lines.insert("\(prefix)\(ind)- coucou", at: enabledIdx + 1)
                        } else {
                            lines[enabledIdx] = enabledLine.replacingOccurrences(of: "]", with: ", coucou]")
                        }
                    } else {
                        // Block list — check if coucou is already a child of this enabled:
                        let enabledRange = sectionRange(from: enabledIdx)
                        let alreadyPresent = (enabledRange.lowerBound + 1 ..< enabledRange.upperBound)
                            .contains { lines[$0].trimmingCharacters(in: .whitespaces) == "- coucou" }
                        if alreadyPresent { return }
                        // Insert after enabled:
                        let prefix = enabledLine.prefix(while: { $0 == " " })
                        lines.insert("\(prefix)\(ind)- coucou", at: enabledIdx + 1)
                    }
                } else {
                    // No enabled: key under plugins: — insert after plugins:
                    lines.insert("\(ind)enabled:", at: pluginsIdx + 1)
                    lines.insert("\(ind)\(ind)- coucou", at: pluginsIdx + 2)
                }
            } else {
                // No plugins: section — append
                if lines.last != "" { lines.append("") }
                lines.append("plugins:")
                lines.append("\(ind)enabled:")
                lines.append("\(ind)\(ind)- coucou")
            }
        }

        func removeCoucouPlugin() {
            // Remove the `- coucou` entry from plugins.enabled only.
            // If that leaves enabled: with no entries, leave the key in place (don't remove it).
            guard let pluginsIdx = topLevelIndex(key: "plugins") else { return }
            let pluginsRange = sectionRange(from: pluginsIdx)
            guard let enabledIdx = (pluginsRange.lowerBound + 1 ..< pluginsRange.upperBound)
                .first(where: { lines[$0].trimmingCharacters(in: .whitespaces).hasPrefix("enabled:") })
            else { return }
            let enabledLine = lines[enabledIdx]
            let trimmed = enabledLine.trimmingCharacters(in: .whitespaces)
            if trimmed.contains("[") && trimmed.contains("]") {
                // Inline list: remove coucou from it
                let cleaned = trimmed
                    .replacingOccurrences(of: ", coucou", with: "")
                    .replacingOccurrences(of: "coucou, ", with: "")
                    .replacingOccurrences(of: "coucou",   with: "")
                let prefix = enabledLine.prefix(while: { $0 == " " })
                lines[enabledIdx] = "\(prefix)\(cleaned)"
            } else {
                // Block list: remove the `- coucou` entry
                let enabledRange = sectionRange(from: enabledIdx)
                // Collect indices to remove first, then remove in reverse to preserve indices.
                let toRemove = (enabledRange.lowerBound + 1 ..< enabledRange.upperBound)
                    .filter { lines[$0].trimmingCharacters(in: .whitespaces) == "- coucou" }
                for i in toRemove.reversed() { lines.remove(at: i) }
            }
        }

        ensureCoucouPlugin()
        if enableApprovals { ensureApprovalTransport() } else { removeApprovalTransport() }
        return lines.joined(separator: "\n")
    }

    func previewHermesPlugin(install: Bool) throws -> String {
        if !install {
            guard FileManager.default.fileExists(atPath: Self.hermesInitPyURL.path) else {
                throw NSError(domain: "CoucouNoop", code: 0, userInfo: [
                    NSLocalizedDescriptionKey: "No Hermes plugin to remove."
                ])
            }
            let current = (try? Data(contentsOf: Self.hermesInitPyURL)) ?? Data()
            _pendingHermesPluginFingerprint = sha256Hex(current)
            _pendingHermesPluginContent = nil
            return "(will delete \(Self.hermesInitPyURL.path))"
        }
        let current = (try? Data(contentsOf: Self.hermesInitPyURL)) ?? Data()
        _pendingHermesPluginFingerprint = sha256Hex(current)
        let content = buildHermesInitPy()
        _pendingHermesPluginContent = content
        return content
    }

    func writeHermesPlugin() throws {
        guard let fp = _pendingHermesPluginFingerprint else { return }
        let url = Self.hermesInitPyURL
        let current = (try? Data(contentsOf: url)) ?? Data()
        guard sha256Hex(current) == fp else {
            throw NSError(domain: "Coucou", code: 1, userInfo: [
                NSLocalizedDescriptionKey: "~/.hermes/plugins/coucou/__init__.py changed since preview. Refresh and try again."
            ])
        }
        let fm = FileManager.default
        if let content = _pendingHermesPluginContent {
            try fm.createDirectory(at: Self.hermesPluginDir, withIntermediateDirectories: true)
            if fm.fileExists(atPath: url.path) {
                let fmt = DateFormatter()
                fmt.locale = Locale(identifier: "en_US_POSIX")
                fmt.dateFormat = "yyyyMMdd-HHmmss"
                let bak = url.deletingLastPathComponent()
                    .appendingPathComponent("__init__.py.bak-\(fmt.string(from: Date()))")
                try fm.copyItem(at: url, to: bak)
            }
            try content.write(to: url, atomically: true, encoding: .utf8)
            try Self.hermesPluginYaml.write(to: Self.hermesPluginYamlURL, atomically: true, encoding: .utf8)
            // Register the plugin with the Hermes CLI so it appears in plugins.enabled.
            // Best-effort — silently ignored if hermes is not on PATH.
            try? Self.runHermesCLI(["plugins", "enable", "coucou"])
        }
        _pendingHermesPluginContent = nil
        _pendingHermesPluginFingerprint = nil
    }

    func removeHermesPlugin() throws {
        let url = Self.hermesInitPyURL
        guard FileManager.default.fileExists(atPath: url.path) else { return }
        let content = (try? String(contentsOf: url, encoding: .utf8)) ?? ""
        guard content.contains("generated by Coucou") else {
            throw NSError(domain: "Coucou", code: 3, userInfo: [
                NSLocalizedDescriptionKey: "~/.hermes/plugins/coucou/__init__.py was not generated by Coucou — not deleting it."
            ])
        }
        // Remove from hermes plugins.enabled first, then delete the files.
        // Best-effort — silently ignored if hermes is not on PATH.
        try? Self.runHermesCLI(["plugins", "disable", "coucou"])
        try FileManager.default.removeItem(at: Self.hermesPluginDir)
    }

    /// Invoke the Hermes CLI with the given arguments.
    /// Searches standard PATH locations for the `hermes` binary.
    @discardableResult
    private static func runHermesCLI(_ args: [String]) throws -> String {
        let hermesPaths = [
            "/opt/homebrew/bin/hermes",
            "/usr/local/bin/hermes",
            "/usr/bin/hermes",
            (ProcessInfo.processInfo.environment["HOME"] ?? "") + "/.local/bin/hermes",
        ]
        guard let hermesBin = hermesPaths.first(where: { FileManager.default.fileExists(atPath: $0) }) else {
            throw NSError(domain: "CoucouNoop", code: 0, userInfo: [
                NSLocalizedDescriptionKey: "hermes CLI not found."
            ])
        }
        let proc = Process()
        proc.executableURL = URL(fileURLWithPath: hermesBin)
        proc.arguments = args
        let out = Pipe()
        proc.standardOutput = out
        proc.standardError  = Pipe()   // discard stderr
        try proc.run()
        proc.waitUntilExit()
        return String(data: out.fileHandleForReading.readDataToEndOfFile(), encoding: .utf8) ?? ""
    }

    func previewHermesConfig(enableApprovals: Bool, supportsTransport: Bool) throws -> String {
        let base = (try? String(contentsOf: Self.hermesConfigURL, encoding: .utf8)) ?? ""
        let current = (try? Data(contentsOf: Self.hermesConfigURL)) ?? Data()
        _pendingHermesConfigFingerprint = sha256Hex(current)
        let effectiveApprovals = enableApprovals && supportsTransport
        guard let merged = Self.mergedHermesConfig(base, enableApprovals: effectiveApprovals) else {
            _pendingHermesConfigContent = nil
            throw NSError(domain: "Coucou", code: 4, userInfo: [
                NSLocalizedDescriptionKey: "~/.hermes/config.yaml uses an unsupported structure (flow maps, YAML anchors, or multi-document). Edit it manually and add:\n  security:\n    approval:\n      transport: coucou\n      transport_fallback: builtin"
            ])
        }
        _pendingHermesConfigContent = merged
        return merged
    }

    func writeHermesConfig() throws {
        guard let fp = _pendingHermesConfigFingerprint,
              let content = _pendingHermesConfigContent else { return }
        let url = Self.hermesConfigURL
        let current = (try? Data(contentsOf: url)) ?? Data()
        guard sha256Hex(current) == fp else {
            throw NSError(domain: "Coucou", code: 1, userInfo: [
                NSLocalizedDescriptionKey: "~/.hermes/config.yaml changed since preview. Refresh and try again."
            ])
        }
        let fm = FileManager.default
        if fm.fileExists(atPath: url.path) {
            let fmt = DateFormatter()
            fmt.locale = Locale(identifier: "en_US_POSIX")
            fmt.dateFormat = "yyyyMMdd-HHmmss"
            let bak = url.deletingLastPathComponent()
                .appendingPathComponent("config.yaml.bak-\(fmt.string(from: Date()))")
            try fm.copyItem(at: url, to: bak)
        }
        try fm.createDirectory(at: url.deletingLastPathComponent(), withIntermediateDirectories: true)
        try content.write(to: url, atomically: true, encoding: .utf8)
        _pendingHermesConfigContent = nil
        _pendingHermesConfigFingerprint = nil
    }

    // MARK: SHA-256 fingerprint

    private func sha256Hex(_ data: Data) -> String {
        SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }
    #endif
}

// MARK: - Notification names for hook server → controller communication

extension Notification.Name {
    static let hookExpand = Notification.Name("notchBuddy.hookExpand")
}

// MARK: - nb-hook shell wrapper (same for both GitHub and App Store)
// Invoked by Claude Code via /bin/sh or directly via shebang.
// Always exits 0 — never blocks Claude Code.
// On macOS, checks xcode-select before running python3 to avoid triggering the
// "install developer tools" dialog on machines without Xcode CLI tools.
// Elsewhere (the hook copied to a Linux machine reached over SSH, docs/REMOTE.md)
// it runs the python3 found in PATH.

private let nbHookShellWrapper = """
#!/bin/sh
# Coucou hook relay — always exits 0, never blocks Claude Code
HOOK_DIR="$(dirname "$0")"
# The agent that ran the hook, still running after this relay has exited
COUCOU_HOOK_PPID=$PPID; export COUCOU_HOOK_PPID
PY=""
if [ "$(uname -s)" = "Darwin" ]; then
    xcode-select -p >/dev/null 2>&1 && PY=/usr/bin/python3
else
    PY=$(command -v python3 2>/dev/null)
fi
out=""
if [ -n "$PY" ]; then
    out=$("$PY" "$HOOK_DIR/nb-hook.py" "$@" 2>/dev/null)
    rc=$?
    if [ "$rc" -ne 0 ] || [ -z "$out" ]; then
        out=""
    fi
fi
if [ -n "$out" ]; then
    printf '%s\\n' "$out"
else
    # Copilot is fail-closed — must always output valid JSON even when python3 is absent or crashes.
    # Antigravity needs a decision on PreToolUse: "ask" leaves it to its own rules and prompt.
    _cop=0; _perm=0; _agy=0; _pre=0
    for _a in "$@"; do
        case "$_a" in
            copilot) _cop=1 ;;
            antigravity) _agy=1 ;;
            permissionRequest|PermissionRequest) _perm=1 ;;
            PreToolUse) _pre=1 ;;
        esac
    done
    if [ "$_cop" -eq 1 ]; then
        if [ "$_perm" -eq 1 ]; then
            printf '{"permissionDecision":"ask"}\\n'
        else
            printf '{}\\n'
        fi
    elif [ "$_agy" -eq 1 ] && [ "$_pre" -eq 1 ]; then
        printf '{"decision":"ask"}\\n'
    fi
fi
exit 0
"""

// MARK: - nb-hook Python relay (GitHub / non-sandboxed version)

let nbHookPythonGitHub = """
#!/usr/bin/env python3
# nb-hook.py — Coucou hook relay for Claude Code and third-party agents (GitHub version)
# Reads JSON from stdin, forwards to Coucou via Unix socket, translates response.
import sys, json, os, re, socket, subprocess, time, stat
from pathlib import Path

def session_tty():
    # The session's terminal tab ("/dev/ttys004"), as Terminal.app names its tabs. Hooks run
    # without a controlling terminal, so walk up to the agent's own process. '' when none has one.
    pid = os.getpid()
    for _ in range(8):
        try:
            out = subprocess.run(['ps', '-o', 'tty=,ppid=', '-p', str(pid)],
                                 capture_output=True, text=True, timeout=1).stdout.split()
        except Exception:
            return ''
        if len(out) != 2:
            return ''
        if out[0].startswith('tty'):
            return '/dev/' + out[0]
        if not out[1].isdigit() or int(out[1]) <= 1:
            return ''
        pid = int(out[1])
    return ''

# The app reads at most 1 MiB per event and drops a bigger one whole, a
# permission request included: past that, keep only what the card needs.
MAX_LINE = 1048576

def wire(p, waits=False):
    line = (json.dumps(p) + '\\n').encode()
    if len(line) <= MAX_LINE or not isinstance(p, dict):
        return line
    if waits:
        # A card made from the cut copy would hide what Allow runs: the terminal asks.
        raise ValueError('too long for a card')
    small = {k: v for k, v in p.items()
             if not isinstance(v, (dict, list)) and not (isinstance(v, str) and len(v) > 2000)}
    tool_input = p.get('tool_input')
    if isinstance(tool_input, dict):
        small['tool_input'] = {k: v[:2000] if isinstance(v, str) else v
                               for k, v in tool_input.items() if not isinstance(v, (dict, list))}
    return (json.dumps(small) + '\\n').encode()

def machine_id():
    # Only for Coucou to tell whether it runs on this same machine: Linux's machine-id, else the Mac's IOPlatformUUID.
    import re
    for path in ('/etc/machine-id', '/var/lib/dbus/machine-id'):
        try:
            with open(path) as f:
                mid = f.read().strip()
            if re.fullmatch(r'[0-9A-Fa-f-]{1,64}', mid):
                return mid
        except Exception:
            pass
    try:
        out = subprocess.run(['/usr/sbin/ioreg', '-rd1', '-c', 'IOPlatformExpertDevice'],
                             capture_output=True, text=True, timeout=1).stdout
        m = re.search(r'"IOPlatformUUID" = "([0-9A-Fa-f-]{1,64})"', out)
        return m.group(1) if m else ''
    except Exception:
        return ''


# --- Codex automatic-review detection ---------------------------------------
# The Coucou hook runs before Codex's native approval routing. When a turn's
# approvals reviewer is automatic, Codex resolves the request on its own, so
# those PermissionRequests must not reach the notch: nobody should answer them.
# Only a positively proven automatic turn (or Codex's internal Guardian reviewer
# session) returns True; anything human, unknown, malformed or per-call MCP
# returns False and keeps the existing relay behaviour.

_AUTO_REVIEWERS = ('auto_review', 'strict_auto_review')
_HEADER_MAX_BYTES = 1024 * 1024            # first record: session_meta
_TAIL_MAX_BYTES = 8 * 1024 * 1024          # recent turn_context records


def _codex_under(path, directory):
    try:
        path.relative_to(directory)
        return True
    except ValueError:
        return False


def _codex_read_segment(fd, offset, limit):
    os.lseek(fd, offset, os.SEEK_SET)
    chunks = []
    remaining = limit
    while remaining > 0:
        chunk = os.read(fd, min(65536, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    return b''.join(chunks)


def _codex_complete_lines(blob, skip_first):
    # Only newline-terminated records count: a cut-off line is not a record.
    if skip_first and blob:
        newline = blob.find(b'\\n')
        blob = b'' if newline < 0 else blob[newline + 1:]
    end = blob.rfind(b'\\n')
    if end < 0:
        return []
    return blob[:end].split(b'\\n')


def _codex_parse_record(line):
    # One complete JSON line -> dict, or None when malformed.
    try:
        record = json.loads(line.decode('utf-8'))
    except Exception:
        return None
    return record if isinstance(record, dict) else None


def _codex_records(lines):
    for line in lines:
        if not line:
            continue
        if b'session_meta' not in line and b'turn_context' not in line:
            continue
        record = _codex_parse_record(line)
        if record is not None:
            yield record


def _codex_read_rollout(path):
    # Returns (session_meta payload or None, list of turn_context payloads).
    # Reads are bounded: the first _HEADER_MAX_BYTES and the last _TAIL_MAX_BYTES.
    flags = os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0)
    fd = os.open(path, flags)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            return None, []
        size = info.st_size
        header = _codex_read_segment(fd, 0, _HEADER_MAX_BYTES)
        tail_start = size - min(size, _TAIL_MAX_BYTES)
        tail = _codex_read_segment(fd, tail_start, _TAIL_MAX_BYTES)
    finally:
        os.close(fd)
    # session_meta must be the very first complete record of the rollout; a
    # malformed first record, or a session_meta appearing later, is not trusted.
    meta = None
    header_lines = _codex_complete_lines(header, False)
    if header_lines:
        record = _codex_parse_record(header_lines[0])
        if (record is not None and record.get('type') == 'session_meta'
                and isinstance(record.get('payload'), dict)):
            meta = record['payload']
    contexts = []
    for record in _codex_records(_codex_complete_lines(tail, tail_start > 0)):
        payload = record.get('payload')
        if record.get('type') == 'turn_context' and isinstance(payload, dict):
            contexts.append(payload)
    return meta, contexts


def codex_uses_automatic_review(payload, codex_home=None):
    # True only for a proven automatic-review Codex PermissionRequest; the caller
    # then skips the relay entirely. Unknown, malformed, missing or human-reviewed
    # turns return False so the normal relay behaviour is preserved.
    try:
        if not isinstance(payload, dict):
            return False
        if payload.get('hook_event_name') != 'PermissionRequest':
            return False
        tool_name = payload.get('tool_name')
        if not isinstance(tool_name, str) or not tool_name:
            return False  # unknown tool -> unknown reviewer routing
        if tool_name.startswith('mcp__'):
            return False  # an MCP server can override the reviewer per call
        session_id = payload.get('session_id')
        transcript_path = payload.get('transcript_path')
        if not isinstance(session_id, str) or not session_id:
            return False
        if not isinstance(transcript_path, str) or not transcript_path:
            return False
        home = codex_home or os.environ.get('CODEX_HOME') or os.path.expanduser('~/.codex')
        base = Path(home).expanduser().resolve()
        if not base.is_dir():
            return False
        transcript = Path(transcript_path).resolve()
        roots = (base.joinpath('sessions').resolve(), base.joinpath('archived_sessions').resolve())
        if not _codex_under(transcript, roots[0]) and not _codex_under(transcript, roots[1]):
            return False
        meta, contexts = _codex_read_rollout(str(transcript))
        if not isinstance(meta, dict):
            return False
        if session_id != meta.get('id') and session_id != meta.get('session_id'):
            return False
        if (meta.get('thread_source') == 'guardian_review'
                and meta.get('source') == {'subagent': {'other': 'guardian'}}):
            # Codex internal Guardian reviewer session: proven automatic, no turn context.
            return True
        turn_id = payload.get('turn_id')
        if not isinstance(turn_id, str) or not turn_id:
            return False
        reviewers = set()
        for context in contexts:
            if context.get('turn_id') != turn_id:
                continue
            reviewer = context.get('approvals_reviewer')
            if not isinstance(reviewer, str) or reviewer not in _AUTO_REVIEWERS:
                return False  # human, unknown or conflicting evidence for this turn
            reviewers.add(reviewer)
        return len(reviewers) == 1
    except Exception:
        return False


def normalize_event(name):
    mapping = {
        'BeforeTool': 'PreToolUse', 'BeforeToolSelection': 'PreToolUse',
        'AfterTool': 'PostToolUse', 'AfterModel': 'PostToolUse',
        'BeforeAgent': 'UserPromptSubmit', 'AfterAgent': 'Stop',
        'startup': 'SessionStart', 'exit': 'SessionEnd',
        'PreInvocation': 'UserPromptSubmit', 'PostInvocation': 'PostToolUse',
        'pre_tool_use': 'PreToolUse', 'post_tool_use': 'PostToolUse',
        'user_prompt_submit': 'UserPromptSubmit', 'session_start': 'SessionStart',
        'session_end': 'SessionEnd', 'stop': 'Stop',
        'sessionStart': 'SessionStart', 'userPromptSubmitted': 'UserPromptSubmit',
        'agentStop': 'Stop', 'notification': 'Notification',
        'preToolUse': 'PreToolUse', 'postToolUse': 'PostToolUse',
        'permissionRequest': 'PermissionRequest', 'sessionEnd': 'SessionEnd',
    }
    return mapping.get(name, name)

def normalize_tool_fields(payload):
    if 'tool_name' not in payload:
        # Copilot sends toolName directly; other agents nest in toolCall
        if payload.get('toolName'):
            payload['tool_name'] = payload['toolName']
        else:
            tool = payload.get('toolCall')
            if not isinstance(tool, dict):
                tool = {}
            name = tool.get('name') or payload.get('tool', '')
            if name:
                payload['tool_name'] = name
    if 'tool_input' not in payload:
        # Copilot sends toolArgs directly
        tool_args = payload.get('toolArgs')
        if isinstance(tool_args, dict):
            payload['tool_input'] = tool_args
        else:
            tool = payload.get('toolCall') or {}
            if isinstance(tool.get('args'), dict):
                flat = dict(tool['args'])
                for src, dst in [('CommandLine', 'command'), ('FilePath', 'file_path'),
                                 ('Path', 'path'), ('Url', 'url'), ('Query', 'query'), ('Pattern', 'pattern')]:
                    if src in flat:
                        flat[dst] = flat[src]
                payload['tool_input'] = flat
    if 'session_id' not in payload:
        for k in ['conversationId', 'conversation_id', 'sessionId', 'GEMINI_SESSION_ID']:
            if payload.get(k):
                payload['session_id'] = payload[k]
                break
        if 'session_id' not in payload:
            sid = os.environ.get('GEMINI_SESSION_ID', '')
            if sid:
                payload['session_id'] = sid
    # Copilot sends workdir for the current working directory
    if not payload.get('cwd') and payload.get('workdir'):
        payload['cwd'] = payload['workdir']

# Several Coucous on one host each run a relay for the same call; the one answered marks it
# here and the others let go. Same files and key as the Linux coucou-hook.
ANSWERED_DIR = os.path.expanduser('~/.coucou/answered')

def answer_key(payload):
    tid = payload.get('tool_use_id')
    if isinstance(tid, str) and 0 < len(tid) <= 128 and re.fullmatch(r'[A-Za-z0-9_-]+', tid):
        return tid
    sid, tool = payload.get('session_id'), payload.get('tool_name')
    text = '%s\\n%s\\n%s' % (sid if isinstance(sid, str) else '', tool if isinstance(tool, str) else '',
        json.dumps(payload.get('tool_input'), sort_keys=True, separators=(',', ':'), ensure_ascii=False))
    h = 0xcbf29ce484222325
    for b in text.encode('utf-8'):
        h = ((h ^ b) * 0x100000001b3) & 0xFFFFFFFFFFFFFFFF
    return 'h%016x' % h

def mark_answered(key):
    try:
        os.makedirs(ANSWERED_DIR, exist_ok=True)
        os.chmod(ANSWERED_DIR, 0o700)
        tmp = os.path.join(ANSWERED_DIR, '.%s.%d' % (key, os.getpid()))
        open(tmp, 'w').close()
        os.replace(tmp, os.path.join(ANSWERED_DIR, key))
        now = time.time()
        for name in os.listdir(ANSWERED_DIR):
            p = os.path.join(ANSWERED_DIR, name)
            if now - os.path.getmtime(p) > 3600:
                os.remove(p)
    except OSError:
        pass

def read_reply(s, key, budget):
    # The app's reply line, or '' once another relay on this host has answered the call.
    s.settimeout(0.2)
    chunks = []
    deadline = time.monotonic() + budget
    while time.monotonic() < deadline:
        try:
            chunk = s.recv(4096)
        except socket.timeout:
            if os.path.exists(os.path.join(ANSWERED_DIR, key)):
                return ''
            continue
        if not chunk:
            break
        chunks.append(chunk)
        if b'\\n' in chunk:
            break
    return b''.join(chunks).decode().strip()
def claude_session_name(sid):
    # The name given with /rename, from Claude Code's session registry
    if not sid:
        return ''
    try:
        d = os.path.expanduser('~/.claude/sessions')
        for f in os.listdir(d):
            if not f.endswith('.json'):
                continue
            with open(os.path.join(d, f)) as fh:
                s = json.load(fh)
            if s.get('sessionId') == sid:
                return str(s.get('name') or '')
    except Exception:
        pass
    return ''
def add_session_name(payload):
    # Name the pill after the session's title, else the folder the session started in,
    # not the current cwd (the agent may cd into subfolders). Both come from the
    # transcript: its first lines, and its last custom-title entry (/rename), else its
    # last ai-title one (the title Claude Code gives a session; VS Code sessions only
    # have that). Entries are told by their type: a message that merely quotes one is not
    # a title. Claude Code writes the title again every 40-80 KB, hence a 256 KB tail.
    path = payload.get('transcript_path') or ''
    if not path:
        return
    try:
        with open(path, 'rb') as f:
            for line in f.read(16384).splitlines():
                try:
                    root = json.loads(line).get('cwd')
                except Exception:
                    continue
                if root:
                    payload['session_root'] = root
                    break
            f.seek(0, os.SEEK_END)
            start = max(0, f.tell() - 262144)
            f.seek(start)
            lines = f.read().splitlines()
            ai_title = ''
            for line in reversed(lines[1:] if start else lines):
                if b'"type":"custom-title"' not in line and b'"type":"ai-title"' not in line:
                    continue
                try:
                    entry = json.loads(line)
                except Exception:
                    continue
                if entry.get('type') == 'custom-title' and entry.get('customTitle'):
                    payload['session_title'] = entry['customTitle']
                    return
                if entry.get('type') == 'ai-title' and not ai_title:
                    ai_title = entry.get('aiTitle') or ''
            if ai_title:
                payload['session_title'] = ai_title
    except Exception:
        pass

def term_program(env):
    # VS Code's Claude Code extension on a remote machine (Remote - SSH) has neither
    # TERM_PROGRAM nor the app's bundle ID; its entrypoint still says where it runs.
    term = env.get('TERM_PROGRAM', '')
    if not term and not env.get('__CFBundleIdentifier') and env.get('CLAUDE_CODE_ENTRYPOINT') == 'claude-vscode':
        return 'vscode'
    return term

def main():
    raw = b''
    payload = {}
    try:
        raw = sys.stdin.buffer.read()
        if not raw:
            if '--statusline' not in sys.argv[1:]:
                return
        else:
            payload = json.loads(raw)
    except Exception:
        if '--statusline' not in sys.argv[1:]:
            return
    # claude -p, the SDK and scheduled jobs: no one watches them, so they stay off the island.
    if os.environ.get('CLAUDE_CODE_ENTRYPOINT', '').startswith('sdk-') and os.environ.get('COUCOU_WATCHED') != '1':
        return

    # Coucou's own chat runs Claude Code: the app shows its permission requests only.
    if os.environ.get('COUCOU_CHAT') == '1' and isinstance(payload, dict):
        payload['coucou_chat'] = True

    # COUCOU_SOCKET points the relay at another socket, for instance the one an
    # `ssh -R` tunnel forwards from the Mac to a remote machine (docs/REMOTE.md).
    socket_path = os.path.expanduser(
        os.environ.get('COUCOU_SOCKET') or '~/Library/Application Support/NotchBuddy/nb.sock'
    )

    # --statusline mode: relay rate_limits to Coucou, then delegate to saved previous
    if '--statusline' in sys.argv[1:]:
        relay = {
            'coucou_kind': 'statusline',
            'session_id': payload.get('session_id', ''),
            'rate_limits': payload.get('rate_limits', {}),
        }
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(0.3)
            s.connect(socket_path)
            s.sendall((json.dumps(relay) + '\\n').encode())
            s.close()
        except Exception:
            pass
        prev_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'statusline-previous.json')
        if os.path.exists(prev_file):
            try:
                import subprocess
                with open(prev_file) as f:
                    prev = json.load(f)
                cmd = prev.get('command', '')
                if cmd:
                    result = subprocess.run(['/bin/sh', '-c', cmd], input=raw,
                                             capture_output=True, timeout=10)
                    if result.stdout:
                        sys.stdout.buffer.write(result.stdout)
                        sys.stdout.buffer.flush()
            except Exception:
                pass
        return

    key = answer_key(payload)

    # --ask mode: dedicated hook for AskUserQuestion via PreToolUse (Claude Code 2.1.85+)
    if '--ask' in sys.argv[1:]:
        tool = payload.get('tool_name', '')
        if tool != 'AskUserQuestion':
            return  # Not an AskUserQuestion invocation — exit cleanly (no output)
        payload['coucou_kind'] = 'ask_user_question'
        env = os.environ
        payload.setdefault('term_program', term_program(env))
        if env.get('COUCOU_HOOK_PPID', '').isdigit():
            payload['hook_ppid'] = int(env['COUCOU_HOOK_PPID'])
        payload.setdefault('iterm_session_id', env.get('ITERM_SESSION_ID', ''))
        payload.setdefault('term_session_id', env.get('TERM_SESSION_ID', ''))
        payload.setdefault('bundle_id', env.get('__CFBundleIdentifier', ''))
        payload.setdefault('tty', session_tty())
        if 'cwd' not in payload or not payload['cwd']:
            paths = payload.get('workspacePaths') or payload.get('workspace_roots', [])
            if isinstance(paths, list) and paths:
                payload['cwd'] = paths[0]
            else:
                payload['cwd'] = os.getcwd()
        payload['session_name'] = claude_session_name(payload.get('session_id', ''))
        payload['home'] = os.path.expanduser('~')
        add_session_name(payload)
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(125)
            s.connect(socket_path)
            s.sendall(wire(payload, waits=True))
            response = read_reply(s, key, s.gettimeout())
            s.close()
            if response:
                try:
                    resp_obj = json.loads(response)
                    decision = resp_obj.get('permissionDecision', '')
                except Exception:
                    decision = ''
                if decision == 'answer':
                    mark_answered(key)
                    answers = resp_obj.get('answers', {})
                    questions = payload.get('tool_input', {}).get('questions', [])
                    out = {'hookSpecificOutput': {'hookEventName': 'PreToolUse', 'permissionDecision': 'allow', 'updatedInput': {'questions': questions, 'answers': answers}}}
                    sys.stdout.write(json.dumps(out) + '\\n')
                    sys.stdout.flush()
                    sys.exit(0)
                # 'ask' or unknown: fall through → no output → Claude Code asks in terminal
        except Exception:
            pass
        return

    # Parse --agent <name> and optional positional event from argv.
    # --agent tags the payload with coucou_agent so the app routes to the right pill.
    # The positional arg is a fallback event name for agents that do not set hook_event_name.
    args = sys.argv[1:]
    agent = ''
    arg_event = ''
    i = 0
    while i < len(args):
        if args[i] == '--agent' and i + 1 < len(args):
            agent = args[i + 1]
            i += 2
        else:
            if not arg_event:
                arg_event = args[i]
            i += 1
    if agent:
        payload.setdefault('coucou_agent', agent)
    # Claude Code sessions from the Claude desktop app (Code tab) report this entrypoint;
    # route them to the Claude Desktop pill instead of dropping them (no VS Code terminal).
    if not payload.get('coucou_agent') and os.environ.get('CLAUDE_CODE_ENTRYPOINT') == 'claude-desktop':
        payload['coucou_agent'] = 'claude-desktop'

    # Enrich with terminal context
    env = os.environ
    payload.setdefault('term_program', term_program(env))
    if env.get('COUCOU_HOOK_PPID', '').isdigit():
        payload['hook_ppid'] = int(env['COUCOU_HOOK_PPID'])
    payload.setdefault('iterm_session_id', env.get('ITERM_SESSION_ID', ''))
    payload.setdefault('term_session_id', env.get('TERM_SESSION_ID', ''))
    payload.setdefault('bundle_id', env.get('__CFBundleIdentifier', ''))
    payload.setdefault('tty', session_tty())
    if 'cwd' not in payload or not payload['cwd']:
        paths = payload.get('workspacePaths') or payload.get('workspace_roots', [])
        if isinstance(paths, list) and paths:
            payload['cwd'] = paths[0]
        else:
            payload['cwd'] = os.getcwd()
    payload['session_name'] = claude_session_name(payload.get('session_id', ''))
    payload['home'] = os.path.expanduser('~')

    # The tmux session, so another machine can attach to it (remote Open terminal)
    if payload.get('hook_event_name') in ('SessionStart', 'UserPromptSubmit') and env.get('TMUX'):
        try:
            import subprocess, re
            pane = ['-t', env['TMUX_PANE']] if env.get('TMUX_PANE') else []
            name = subprocess.run(['tmux', 'display-message', '-p'] + pane + ['#S'],
                                  capture_output=True, text=True, timeout=0.3).stdout.strip()
            if re.fullmatch(r'[A-Za-z0-9._-]{1,64}', name):
                payload['tmux_session'] = name
        except Exception:
            pass

    # On a remote host: who ran the hook, and the machine, so Coucou on this same machine finds its terminal
    if env.get('COUCOU_SOCKET'):
        try:
            payload['hook_ppid'] = int(env.get('COUCOU_HOOK_PPID', ''))
        except ValueError:
            payload['hook_ppid'] = os.getppid()
        if payload.get('hook_event_name') in ('SessionStart', 'UserPromptSubmit'):
            mid = machine_id()
            if mid:
                payload['machine_id'] = mid
    add_session_name(payload)

    # Normalize event name and tool fields (Gemini CLI / Antigravity → canonical names)
    try:
        raw_event = payload.get('hook_event_name', '') or arg_event
        if raw_event:
            payload['hook_event_name'] = normalize_event(raw_event)
        normalize_tool_fields(payload)
    except Exception:
        pass

    event = payload.get('hook_event_name', '')
    # socket_path is already defined above

    # Antigravity runs its PreToolUse hook before its own permission check, and waits for it.
    # Coucou answers at once unless the notch shows the command (Settings → Antigravity →
    # Approve commands from the notch): then {"pending":true} comes first, the click later.
    # Only a click on Allow prints "allow"; anything else ("ask", no answer, Coucou closed)
    # leaves the tool to Antigravity's own rules and prompt.
    if agent == 'antigravity' and event == 'PreToolUse':
        out = {'decision': 'ask'}
        try:
            payload['coucou_kind'] = 'agy_pre_tool'
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(3)
            s.connect(socket_path)
            s.sendall((json.dumps(payload) + '\\n').encode())
            buf = b''
            reply = None
            while reply is None:
                chunk = s.recv(4096)
                if not chunk:
                    break
                buf += chunk
                while reply is None and b'\\n' in buf:
                    line, buf = buf.split(b'\\n', 1)
                    obj = json.loads(line.decode() or '{}')
                    if isinstance(obj, dict) and obj.get('pending'):
                        s.settimeout(115)  # the card is up; Coucou gives up first
                    else:
                        reply = obj
            s.close()
            decision = reply.get('permissionDecision', '') if isinstance(reply, dict) else ''
            if decision in ('allow', 'always'):
                out = {'decision': 'allow'}
            elif decision == 'deny':
                out = {'decision': 'deny', 'reason': 'Denied from Coucou'}
        except Exception:
            pass
        sys.stdout.write(json.dumps(out) + '\\n')
        sys.stdout.flush()
        sys.exit(0)

    if event == 'PermissionRequest':
        # Codex automatic review: this hook runs before Codex's native automatic
        # routing, so relaying these requests would show an approval card nobody
        # should answer. Suppress only proven automatic turns, before any socket
        # connection; human, unknown and per-call MCP requests keep the relay.
        if agent == 'codex':
            try:
                if codex_uses_automatic_review(payload):
                    sys.exit(0)  # empty stdout — Codex proceeds with its own reviewer
            except Exception:
                pass
        # Block and wait for Coucou's decision (Claude Code allows up to 120s)
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(118)
            s.connect(socket_path)
            s.sendall(wire(payload, waits=True))
            response = read_reply(s, key, s.gettimeout())
            s.close()
            if response:
                try:
                    resp_obj = json.loads(response)
                    decision = resp_obj.get('permissionDecision', '')
                except Exception:
                    decision = ''
                if decision in ('allow', 'always', 'deny', 'answer'):
                    mark_answered(key)
                if agent == 'hermes':
                    hermes_choice = 'once' if decision == 'allow' else decision
                    if hermes_choice in ('once', 'always', 'deny'):
                        sys.stdout.write(json.dumps({'choice': hermes_choice}) + '\\n')
                        sys.stdout.flush()
                    sys.exit(0)
                if decision in ('allow', 'always'):
                    # Copilot/Muse use {"permissionDecision":"allow"} directly
                    if agent in ('copilot', 'muse'):
                        out = {'permissionDecision': 'allow'}
                    elif decision == 'always' and agent != 'codex':
                        # Let Claude Code persist the rule via updatedPermissions
                        suggestions = payload.get('permission_suggestions', [])
                        out = {'hookSpecificOutput': {'hookEventName': 'PermissionRequest', 'decision': {'behavior': 'allow', 'updatedPermissions': suggestions}}}
                    else:
                        # Claude Code / Codex plain allow
                        out = {'hookSpecificOutput': {'hookEventName': 'PermissionRequest', 'decision': {'behavior': 'allow'}}}
                    # Claude Code ignores an allow for ExitPlanMode without updatedInput (Allow and Always).
                    if payload.get('tool_name') == 'ExitPlanMode' and 'hookSpecificOutput' in out:
                        out['hookSpecificOutput']['decision']['updatedInput'] = payload.get('tool_input', {})
                        # Mode picked on the plan card: same choice as the terminal's plan dialog.
                        mode = resp_obj.get('mode')
                        if mode in ('auto', 'default'):
                            out['hookSpecificOutput']['decision']['updatedPermissions'] = [{'type': 'setMode', 'mode': mode, 'destination': 'session'}]
                    sys.stdout.write(json.dumps(out) + '\\n')
                    sys.stdout.flush()
                    sys.exit(0)
                elif decision == 'deny':
                    if agent in ('copilot', 'muse'):
                        out = {'permissionDecision': 'deny'}
                    else:
                        out = {'hookSpecificOutput': {'hookEventName': 'PermissionRequest', 'decision': {'behavior': 'deny', 'message': 'Denied from Coucou'}}}
                    sys.stdout.write(json.dumps(out) + '\\n')
                    sys.stdout.flush()
                    sys.exit(0)
                elif decision == 'answer':
                    # AskUserQuestion answered from the notch
                    answers = resp_obj.get('answers', {})
                    questions = payload.get('tool_input', {}).get('questions', [])
                    out = {'hookSpecificOutput': {'hookEventName': 'PermissionRequest', 'decision': {'behavior': 'allow', 'updatedInput': {'questions': questions, 'answers': answers}}}}
                    sys.stdout.write(json.dumps(out) + '\\n')
                    sys.stdout.flush()
                    sys.exit(0)
                elif decision == 'change_plan':
                    # Plan card "Change plan": like the terminal's "Tell Claude what to change",
                    # reject the plan and stop Claude so the prompt is free for the change.
                    out = {'hookSpecificOutput': {'hookEventName': 'PermissionRequest', 'decision': {'behavior': 'deny', 'interrupt': True, 'message': 'The user wants to change the plan and will type the changes in the terminal.'}}}
                    sys.stdout.write(json.dumps(out) + '\\n')
                    sys.stdout.flush()
                    sys.exit(0)
                # 'ask' or unknown: fall through → no output → agent re-asks
        except Exception:
            pass
        # App unreachable, timed out, or no explicit decision — print nothing
        # Copilot is fail-closed: must always output valid JSON so it re-asks rather than deny
        # Hermes: no output → json.loads raises in plugin → transport_fallback: builtin activates
        if agent == 'copilot':
            sys.stdout.write('{"permissionDecision":"ask"}\\n')
            sys.stdout.flush()
        sys.exit(0)

    # All other events: fire-and-forget (0.3s timeout, never blocks)
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(0.3)
        s.connect(socket_path)
        s.sendall(wire(payload))
        s.close()
    except Exception:
        pass  # Always exit cleanly — never block the agent

    # Antigravity's PreToolUse is answered above; its other events, like Gemini CLI's, read "{}".
    if agent in ('gemini', 'antigravity', 'muse', 'copilot'):
        sys.stdout.write('{}\\n')
        sys.stdout.flush()

try:
    main()
except Exception:
    pass
sys.exit(0)
"""

// MARK: - nb-hook Python relay (App Store — socket in sandboxed container)

private let nbHookPythonAppStore = """
#!/usr/bin/env python3
# nb-hook.py — Coucou (App Store) hook relay for Claude Code and third-party agents
# Socket lives inside the sandboxed container; script runs outside the sandbox.
import sys, json, os, re, socket, time

# The app reads at most 1 MiB per event and drops a bigger one whole, a
# permission request included: past that, keep only what the card needs.
MAX_LINE = 1048576

def wire(p, waits=False):
    line = (json.dumps(p) + '\\n').encode()
    if len(line) <= MAX_LINE or not isinstance(p, dict):
        return line
    if waits:
        # A card made from the cut copy would hide what Allow runs: the terminal asks.
        raise ValueError('too long for a card')
    small = {k: v for k, v in p.items()
             if not isinstance(v, (dict, list)) and not (isinstance(v, str) and len(v) > 2000)}
    tool_input = p.get('tool_input')
    if isinstance(tool_input, dict):
        small['tool_input'] = {k: v[:2000] if isinstance(v, str) else v
                               for k, v in tool_input.items() if not isinstance(v, (dict, list))}
    return (json.dumps(small) + '\\n').encode()

def normalize_event(name):
    mapping = {
        'BeforeTool': 'PreToolUse', 'BeforeToolSelection': 'PreToolUse',
        'AfterTool': 'PostToolUse', 'AfterModel': 'PostToolUse',
        'BeforeAgent': 'UserPromptSubmit', 'AfterAgent': 'Stop',
        'startup': 'SessionStart', 'exit': 'SessionEnd',
        'PreInvocation': 'UserPromptSubmit', 'PostInvocation': 'PostToolUse',
        'pre_tool_use': 'PreToolUse', 'post_tool_use': 'PostToolUse',
        'user_prompt_submit': 'UserPromptSubmit', 'session_start': 'SessionStart',
        'session_end': 'SessionEnd', 'stop': 'Stop',
        'sessionStart': 'SessionStart', 'userPromptSubmitted': 'UserPromptSubmit',
        'agentStop': 'Stop', 'notification': 'Notification',
        'preToolUse': 'PreToolUse', 'postToolUse': 'PostToolUse',
        'permissionRequest': 'PermissionRequest', 'sessionEnd': 'SessionEnd',
    }
    return mapping.get(name, name)

def normalize_tool_fields(payload):
    if 'tool_name' not in payload:
        # Copilot sends toolName directly; other agents nest in toolCall
        if payload.get('toolName'):
            payload['tool_name'] = payload['toolName']
        else:
            tool = payload.get('toolCall')
            if not isinstance(tool, dict):
                tool = {}
            name = tool.get('name') or payload.get('tool', '')
            if name:
                payload['tool_name'] = name
    if 'tool_input' not in payload:
        # Copilot sends toolArgs directly
        tool_args = payload.get('toolArgs')
        if isinstance(tool_args, dict):
            payload['tool_input'] = tool_args
        else:
            tool = payload.get('toolCall') or {}
            if isinstance(tool.get('args'), dict):
                flat = dict(tool['args'])
                for src, dst in [('CommandLine', 'command'), ('FilePath', 'file_path'),
                                 ('Path', 'path'), ('Url', 'url'), ('Query', 'query'), ('Pattern', 'pattern')]:
                    if src in flat:
                        flat[dst] = flat[src]
                payload['tool_input'] = flat
    if 'session_id' not in payload:
        for k in ['conversationId', 'conversation_id', 'sessionId', 'GEMINI_SESSION_ID']:
            if payload.get(k):
                payload['session_id'] = payload[k]
                break
        if 'session_id' not in payload:
            sid = os.environ.get('GEMINI_SESSION_ID', '')
            if sid:
                payload['session_id'] = sid
    # Copilot sends workdir for the current working directory
    if not payload.get('cwd') and payload.get('workdir'):
        payload['cwd'] = payload['workdir']

# Several Coucous on one host each run a relay for the same call; the one answered marks it
# here and the others let go. Same files and key as the Linux coucou-hook.
ANSWERED_DIR = os.path.expanduser('~/.coucou/answered')

def answer_key(payload):
    tid = payload.get('tool_use_id')
    if isinstance(tid, str) and 0 < len(tid) <= 128 and re.fullmatch(r'[A-Za-z0-9_-]+', tid):
        return tid
    sid, tool = payload.get('session_id'), payload.get('tool_name')
    text = '%s\\n%s\\n%s' % (sid if isinstance(sid, str) else '', tool if isinstance(tool, str) else '',
        json.dumps(payload.get('tool_input'), sort_keys=True, separators=(',', ':'), ensure_ascii=False))
    h = 0xcbf29ce484222325
    for b in text.encode('utf-8'):
        h = ((h ^ b) * 0x100000001b3) & 0xFFFFFFFFFFFFFFFF
    return 'h%016x' % h

def mark_answered(key):
    try:
        os.makedirs(ANSWERED_DIR, exist_ok=True)
        os.chmod(ANSWERED_DIR, 0o700)
        tmp = os.path.join(ANSWERED_DIR, '.%s.%d' % (key, os.getpid()))
        open(tmp, 'w').close()
        os.replace(tmp, os.path.join(ANSWERED_DIR, key))
        now = time.time()
        for name in os.listdir(ANSWERED_DIR):
            p = os.path.join(ANSWERED_DIR, name)
            if now - os.path.getmtime(p) > 3600:
                os.remove(p)
    except OSError:
        pass

def read_reply(s, key, budget):
    # The app's reply line, or '' once another relay on this host has answered the call.
    s.settimeout(0.2)
    chunks = []
    deadline = time.monotonic() + budget
    while time.monotonic() < deadline:
        try:
            chunk = s.recv(4096)
        except socket.timeout:
            if os.path.exists(os.path.join(ANSWERED_DIR, key)):
                return ''
            continue
        if not chunk:
            break
        chunks.append(chunk)
        if b'\\n' in chunk:
            break
    return b''.join(chunks).decode().strip()
def claude_session_name(sid):
    # The name given with /rename, from Claude Code's session registry
    if not sid:
        return ''
    try:
        d = os.path.expanduser('~/.claude/sessions')
        for f in os.listdir(d):
            if not f.endswith('.json'):
                continue
            with open(os.path.join(d, f)) as fh:
                s = json.load(fh)
            if s.get('sessionId') == sid:
                return str(s.get('name') or '')
    except Exception:
        pass
    return ''
def add_session_name(payload):
    # Name the pill after the session's title, else the folder the session started in,
    # not the current cwd (the agent may cd into subfolders). Both come from the
    # transcript: its first lines, and its last custom-title entry (/rename), else its
    # last ai-title one (the title Claude Code gives a session; VS Code sessions only
    # have that). Entries are told by their type: a message that merely quotes one is not
    # a title. Claude Code writes the title again every 40-80 KB, hence a 256 KB tail.
    path = payload.get('transcript_path') or ''
    if not path:
        return
    try:
        with open(path, 'rb') as f:
            for line in f.read(16384).splitlines():
                try:
                    root = json.loads(line).get('cwd')
                except Exception:
                    continue
                if root:
                    payload['session_root'] = root
                    break
            f.seek(0, os.SEEK_END)
            start = max(0, f.tell() - 262144)
            f.seek(start)
            lines = f.read().splitlines()
            ai_title = ''
            for line in reversed(lines[1:] if start else lines):
                if b'"type":"custom-title"' not in line and b'"type":"ai-title"' not in line:
                    continue
                try:
                    entry = json.loads(line)
                except Exception:
                    continue
                if entry.get('type') == 'custom-title' and entry.get('customTitle'):
                    payload['session_title'] = entry['customTitle']
                    return
                if entry.get('type') == 'ai-title' and not ai_title:
                    ai_title = entry.get('aiTitle') or ''
            if ai_title:
                payload['session_title'] = ai_title
    except Exception:
        pass

def term_program(env):
    # VS Code's Claude Code extension on a remote machine (Remote - SSH) has neither
    # TERM_PROGRAM nor the app's bundle ID; its entrypoint still says where it runs.
    term = env.get('TERM_PROGRAM', '')
    if not term and not env.get('__CFBundleIdentifier') and env.get('CLAUDE_CODE_ENTRYPOINT') == 'claude-vscode':
        return 'vscode'
    return term

def main():
    raw = b''
    payload = {}
    try:
        raw = sys.stdin.buffer.read()
        if not raw:
            if '--statusline' not in sys.argv[1:]:
                return
        else:
            payload = json.loads(raw)
    except Exception:
        if '--statusline' not in sys.argv[1:]:
            return
    # claude -p, the SDK and scheduled jobs: no one watches them, so they stay off the island.
    if os.environ.get('CLAUDE_CODE_ENTRYPOINT', '').startswith('sdk-') and os.environ.get('COUCOU_WATCHED') != '1':
        return

    # COUCOU_SOCKET points the relay at another socket, for instance the one an
    # `ssh -R` tunnel forwards from the Mac to a remote machine (docs/REMOTE.md).
    socket_path = os.path.expanduser(
        os.environ.get('COUCOU_SOCKET') or '~/Library/Containers/fr.louisraille.Coucou/Data/nb.sock'
    )

    # --statusline mode: relay rate_limits to Coucou, then delegate to saved previous
    if '--statusline' in sys.argv[1:]:
        relay = {
            'coucou_kind': 'statusline',
            'session_id': payload.get('session_id', ''),
            'rate_limits': payload.get('rate_limits', {}),
        }
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(0.3)
            s.connect(socket_path)
            s.sendall((json.dumps(relay) + '\\n').encode())
            s.close()
        except Exception:
            pass
        prev_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'statusline-previous.json')
        if os.path.exists(prev_file):
            try:
                import subprocess
                with open(prev_file) as f:
                    prev = json.load(f)
                cmd = prev.get('command', '')
                if cmd:
                    result = subprocess.run(['/bin/sh', '-c', cmd], input=raw,
                                             capture_output=True, timeout=10)
                    if result.stdout:
                        sys.stdout.buffer.write(result.stdout)
                        sys.stdout.buffer.flush()
            except Exception:
                pass
        return

    key = answer_key(payload)

    # --ask mode: dedicated hook for AskUserQuestion via PreToolUse (Claude Code 2.1.85+)
    if '--ask' in sys.argv[1:]:
        tool = payload.get('tool_name', '')
        if tool != 'AskUserQuestion':
            return  # Not an AskUserQuestion invocation — exit cleanly (no output)
        payload['coucou_kind'] = 'ask_user_question'
        env = os.environ
        payload.setdefault('term_program', term_program(env))
        if env.get('COUCOU_HOOK_PPID', '').isdigit():
            payload['hook_ppid'] = int(env['COUCOU_HOOK_PPID'])
        payload.setdefault('iterm_session_id', env.get('ITERM_SESSION_ID', ''))
        payload.setdefault('term_session_id', env.get('TERM_SESSION_ID', ''))
        payload.setdefault('bundle_id', env.get('__CFBundleIdentifier', ''))
        if 'cwd' not in payload or not payload['cwd']:
            paths = payload.get('workspacePaths') or payload.get('workspace_roots', [])
            if isinstance(paths, list) and paths:
                payload['cwd'] = paths[0]
            else:
                payload['cwd'] = os.getcwd()
        payload['session_name'] = claude_session_name(payload.get('session_id', ''))
        payload['home'] = os.path.expanduser('~')
        add_session_name(payload)
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(125)
            s.connect(socket_path)
            s.sendall(wire(payload, waits=True))
            response = read_reply(s, key, s.gettimeout())
            s.close()
            if response:
                try:
                    resp_obj = json.loads(response)
                    decision = resp_obj.get('permissionDecision', '')
                except Exception:
                    decision = ''
                if decision == 'answer':
                    mark_answered(key)
                    answers = resp_obj.get('answers', {})
                    questions = payload.get('tool_input', {}).get('questions', [])
                    out = {'hookSpecificOutput': {'hookEventName': 'PreToolUse', 'permissionDecision': 'allow', 'updatedInput': {'questions': questions, 'answers': answers}}}
                    sys.stdout.write(json.dumps(out) + '\\n')
                    sys.stdout.flush()
                    sys.exit(0)
                # 'ask' or unknown: fall through → no output → Claude Code asks in terminal
        except Exception:
            pass
        return

    # Parse --agent <name> and optional positional event from argv.
    # --agent tags the payload with coucou_agent so the app routes to the right pill.
    # The positional arg is a fallback event name for agents that do not set hook_event_name.
    args = sys.argv[1:]
    agent = ''
    arg_event = ''
    i = 0
    while i < len(args):
        if args[i] == '--agent' and i + 1 < len(args):
            agent = args[i + 1]
            i += 2
        else:
            if not arg_event:
                arg_event = args[i]
            i += 1
    if agent:
        payload.setdefault('coucou_agent', agent)
    # Claude Code sessions from the Claude desktop app (Code tab) report this entrypoint;
    # route them to the Claude Desktop pill instead of dropping them (no VS Code terminal).
    if not payload.get('coucou_agent') and os.environ.get('CLAUDE_CODE_ENTRYPOINT') == 'claude-desktop':
        payload['coucou_agent'] = 'claude-desktop'

    env = os.environ
    payload.setdefault('term_program', term_program(env))
    if env.get('COUCOU_HOOK_PPID', '').isdigit():
        payload['hook_ppid'] = int(env['COUCOU_HOOK_PPID'])
    payload.setdefault('iterm_session_id', env.get('ITERM_SESSION_ID', ''))
    payload.setdefault('term_session_id', env.get('TERM_SESSION_ID', ''))
    payload.setdefault('bundle_id', env.get('__CFBundleIdentifier', ''))
    if 'cwd' not in payload or not payload['cwd']:
        paths = payload.get('workspacePaths') or payload.get('workspace_roots', [])
        if isinstance(paths, list) and paths:
            payload['cwd'] = paths[0]
        else:
            payload['cwd'] = os.getcwd()
    payload['session_name'] = claude_session_name(payload.get('session_id', ''))
    payload['home'] = os.path.expanduser('~')

    # The tmux session, so another machine can attach to it (remote Open terminal)
    if payload.get('hook_event_name') in ('SessionStart', 'UserPromptSubmit') and env.get('TMUX'):
        try:
            import subprocess, re
            pane = ['-t', env['TMUX_PANE']] if env.get('TMUX_PANE') else []
            name = subprocess.run(['tmux', 'display-message', '-p'] + pane + ['#S'],
                                  capture_output=True, text=True, timeout=0.3).stdout.strip()
            if re.fullmatch(r'[A-Za-z0-9._-]{1,64}', name):
                payload['tmux_session'] = name
        except Exception:
            pass
    add_session_name(payload)

    # Normalize event name and tool fields (Gemini CLI / Antigravity → canonical names)
    try:
        raw_event = payload.get('hook_event_name', '') or arg_event
        if raw_event:
            payload['hook_event_name'] = normalize_event(raw_event)
        normalize_tool_fields(payload)
    except Exception:
        pass

    event = payload.get('hook_event_name', '')
    # socket_path is already defined above

    # Antigravity runs its PreToolUse hook before its own permission check, and waits for it.
    # Coucou answers at once unless the notch shows the command (Settings → Antigravity →
    # Approve commands from the notch): then {"pending":true} comes first, the click later.
    # Only a click on Allow prints "allow"; anything else ("ask", no answer, Coucou closed)
    # leaves the tool to Antigravity's own rules and prompt.
    if agent == 'antigravity' and event == 'PreToolUse':
        out = {'decision': 'ask'}
        try:
            payload['coucou_kind'] = 'agy_pre_tool'
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(3)
            s.connect(socket_path)
            s.sendall((json.dumps(payload) + '\\n').encode())
            buf = b''
            reply = None
            while reply is None:
                chunk = s.recv(4096)
                if not chunk:
                    break
                buf += chunk
                while reply is None and b'\\n' in buf:
                    line, buf = buf.split(b'\\n', 1)
                    obj = json.loads(line.decode() or '{}')
                    if isinstance(obj, dict) and obj.get('pending'):
                        s.settimeout(115)  # the card is up; Coucou gives up first
                    else:
                        reply = obj
            s.close()
            decision = reply.get('permissionDecision', '') if isinstance(reply, dict) else ''
            if decision in ('allow', 'always'):
                out = {'decision': 'allow'}
            elif decision == 'deny':
                out = {'decision': 'deny', 'reason': 'Denied from Coucou'}
        except Exception:
            pass
        sys.stdout.write(json.dumps(out) + '\\n')
        sys.stdout.flush()
        sys.exit(0)

    if event == 'PermissionRequest':
        # Block and wait for Coucou's decision (Claude Code allows up to 120s)
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(118)
            s.connect(socket_path)
            s.sendall(wire(payload, waits=True))
            response = read_reply(s, key, s.gettimeout())
            s.close()
            if response:
                try:
                    resp_obj = json.loads(response)
                    decision = resp_obj.get('permissionDecision', '')
                except Exception:
                    decision = ''
                if decision in ('allow', 'always', 'deny', 'answer'):
                    mark_answered(key)
                if agent == 'hermes':
                    hermes_choice = 'once' if decision == 'allow' else decision
                    if hermes_choice in ('once', 'always', 'deny'):
                        sys.stdout.write(json.dumps({'choice': hermes_choice}) + '\\n')
                        sys.stdout.flush()
                    sys.exit(0)
                if decision in ('allow', 'always'):
                    # Copilot/Muse use {"permissionDecision":"allow"} directly
                    if agent in ('copilot', 'muse'):
                        out = {'permissionDecision': 'allow'}
                    elif decision == 'always' and agent != 'codex':
                        # Let Claude Code persist the rule via updatedPermissions
                        suggestions = payload.get('permission_suggestions', [])
                        out = {'hookSpecificOutput': {'hookEventName': 'PermissionRequest', 'decision': {'behavior': 'allow', 'updatedPermissions': suggestions}}}
                    else:
                        # Claude Code / Codex plain allow
                        out = {'hookSpecificOutput': {'hookEventName': 'PermissionRequest', 'decision': {'behavior': 'allow'}}}
                    # Claude Code ignores an allow for ExitPlanMode without updatedInput (Allow and Always).
                    if payload.get('tool_name') == 'ExitPlanMode' and 'hookSpecificOutput' in out:
                        out['hookSpecificOutput']['decision']['updatedInput'] = payload.get('tool_input', {})
                        # Mode picked on the plan card: same choice as the terminal's plan dialog.
                        mode = resp_obj.get('mode')
                        if mode in ('auto', 'default'):
                            out['hookSpecificOutput']['decision']['updatedPermissions'] = [{'type': 'setMode', 'mode': mode, 'destination': 'session'}]
                    sys.stdout.write(json.dumps(out) + '\\n')
                    sys.stdout.flush()
                    sys.exit(0)
                elif decision == 'deny':
                    if agent in ('copilot', 'muse'):
                        out = {'permissionDecision': 'deny'}
                    else:
                        out = {'hookSpecificOutput': {'hookEventName': 'PermissionRequest', 'decision': {'behavior': 'deny', 'message': 'Denied from Coucou'}}}
                    sys.stdout.write(json.dumps(out) + '\\n')
                    sys.stdout.flush()
                    sys.exit(0)
                elif decision == 'answer':
                    # AskUserQuestion answered from the notch
                    answers = resp_obj.get('answers', {})
                    questions = payload.get('tool_input', {}).get('questions', [])
                    out = {'hookSpecificOutput': {'hookEventName': 'PermissionRequest', 'decision': {'behavior': 'allow', 'updatedInput': {'questions': questions, 'answers': answers}}}}
                    sys.stdout.write(json.dumps(out) + '\\n')
                    sys.stdout.flush()
                    sys.exit(0)
                elif decision == 'change_plan':
                    # Plan card "Change plan": like the terminal's "Tell Claude what to change",
                    # reject the plan and stop Claude so the prompt is free for the change.
                    out = {'hookSpecificOutput': {'hookEventName': 'PermissionRequest', 'decision': {'behavior': 'deny', 'interrupt': True, 'message': 'The user wants to change the plan and will type the changes in the terminal.'}}}
                    sys.stdout.write(json.dumps(out) + '\\n')
                    sys.stdout.flush()
                    sys.exit(0)
                # 'ask' or unknown: fall through → no output → agent re-asks
        except Exception:
            pass
        # App unreachable, timed out, or no explicit decision — print nothing
        # Copilot is fail-closed: must always output valid JSON so it re-asks rather than deny
        # Hermes: no output → json.loads raises in plugin → transport_fallback: builtin activates
        if agent == 'copilot':
            sys.stdout.write('{"permissionDecision":"ask"}\\n')
            sys.stdout.flush()
        sys.exit(0)

    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(0.3)
        s.connect(socket_path)
        s.sendall(wire(payload))
        s.close()
    except Exception:
        pass  # Always exit cleanly — never block the agent

    # Antigravity's PreToolUse is answered above; its other events, like Gemini CLI's, read "{}".
    if agent in ('gemini', 'antigravity', 'muse', 'copilot'):
        sys.stdout.write('{}\\n')
        sys.stdout.flush()

try:
    main()
except Exception:
    pass
sys.exit(0)
"""


extension HookServer {
    /// What stopped a Claude Code turn, from its StopFailure hook: Claude Code's own message when
    /// it sends one, else a sentence for the kind of error. nil when nothing says.
    static func stopFailureDetail(_ payload: [String: Any]) -> String? {
        for key in ["error_message", "error_details", "message"] {
            if let text = payload[key] as? String, !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
                return DiffEngine.toOneLine(text)
            }
        }
        if let error = payload["error"] as? [String: Any], let text = error["message"] as? String, !text.isEmpty {
            return DiffEngine.toOneLine(text)
        }
        let kind = (payload["error_type"] as? String) ?? (payload["error"] as? String) ?? ""
        switch kind {
        case "rate_limit":            return String(localized: "Claude's rate limit is reached. Try again in a moment.")
        case "overloaded":            return String(localized: "Claude is overloaded. Try again in a moment.")
        case "authentication_failed", "oauth_org_not_allowed":
                                      return String(localized: "Claude Code is not signed in, or its key is no longer valid.")
        case "billing_error":         return String(localized: "Billing problem on the Claude account.")
        case "server_error":          return String(localized: "Claude's servers had a problem. Try again.")
        case "max_output_tokens":     return String(localized: "The answer was too long and was cut.")
        case "model_not_found":       return String(localized: "This model is not available.")
        default:
            // Claude Code often sends only `error: "unknown"`: its last message then usually
            // carries the API's own words ("API Error: 529 Overloaded…").
            if let last = payload["last_assistant_message"] as? String,
               last.range(of: "error", options: .caseInsensitive) != nil {
                return DiffEngine.toOneLine(last)
            }
            // A readable error is shown as it is; "unknown" or a bare status code says nothing.
            return kind.count > 3 && Int(kind) == nil && kind != "unknown" ? kind : nil
        }
    }
}
