import AppKit

extension TerminalTarget {
    /// Brings the session's terminal to the front: the app the session runs in first,
    /// then the first running known terminal. Returns false if none is running.
    @discardableResult
    static func activate(sessionBundleId: String?, tty: String? = nil) -> Bool {
        let apps = NSWorkspace.shared.runningApplications
        let running = Set(apps.compactMap(\.bundleIdentifier))
        guard let id = pick(sessionBundleId: sessionBundleId, running: running),
              let app = apps.first(where: { $0.bundleIdentifier == id }) else { return false }
        focusSession(bundleId: id, tty: tty)
        return app.activate(options: .activateIgnoringOtherApps)
    }

    /// Selects the tab (and tmux pane) the session runs in, so activating its app shows that
    /// one rather than whichever window was last in front. Best effort, running apps only.
    static func focusSession(bundleId: String, tty: String?) {
        guard let tty, !tty.isEmpty,
              NSWorkspace.shared.runningApplications.contains(where: { $0.bundleIdentifier == bundleId })
        else { return }
        var ttys = [tty]
        // In tmux the session's tty is the pane's: the terminal tab holds the client's tty.
        if let tmux = tmuxPath(),
           let found = tmuxPane(tty: tty, listing: run(tmux, ["list-panes", "-a", "-F", "#{pane_tty} #{pane_id} #{session_id}"])) {
            _ = run(tmux, ["select-window", "-t", found.pane])
            _ = run(tmux, ["select-pane", "-t", found.pane])
            // ponytail: only clients already on the pane's session; switch-client if needed later
            ttys = run(tmux, ["list-clients", "-t", found.session, "-F", "#{client_tty}"])
                .split(separator: "\n").map(String.init)
        }
        for candidate in ttys {
            guard let source = focusScript(bundleId: bundleId, tty: candidate) else { return }
            var error: NSDictionary?
            if NSAppleScript(source: source)?.executeAndReturnError(&error).booleanValue == true { return }
        }
    }

    /// A GUI app has no shell PATH: the usual install locations.
    private static func tmuxPath() -> String? {
        ["/opt/homebrew/bin/tmux", "/usr/local/bin/tmux", "/opt/local/bin/tmux", "/usr/bin/tmux"]
            .first { FileManager.default.isExecutableFile(atPath: $0) }
    }

    private static func run(_ exe: String, _ args: [String]) -> String {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: exe)
        process.arguments = args
        let pipe = Pipe()
        process.standardOutput = pipe
        process.standardError = FileHandle.nullDevice
        guard (try? process.run()) != nil else { return "" }
        let data = pipe.fileHandleForReading.readDataToEndOfFile()
        process.waitUntilExit()
        return String(decoding: data, as: UTF8.self)
    }
}

extension ClaudeHost {
    /// `activate`, with the session's own tab selected first.
    @discardableResult
    static func activate(_ hostBundleId: String?, tty: String?) -> Bool {
        if let id = hostBundleId { TerminalTarget.focusSession(bundleId: id, tty: tty) }
        return activate(hostBundleId)
    }
}
