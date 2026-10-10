import Foundation

/// Picks which app "Open terminal" / ⌃⌥T should bring to the front.
/// Foundation only, so it can be tested without AppKit (see scripts/test-terminal-target.sh).
enum TerminalTarget {
    /// Terminals tried, in order, when the session's own app is unknown or not running.
    static let knownTerminals = [
        "com.apple.Terminal", "com.googlecode.iterm2", "net.kovidgoyal.kitty",
        "com.mitchellh.ghostty", "com.cmuxterm.app", "dev.warp.Warp-Stable",
        "com.github.wez.wezterm", "org.alacritty",
    ]

    /// Bundle IDs to try, in order: the app the session runs in (from the hook's
    /// `bundle_id`, i.e. `__CFBundleIdentifier`), then the known terminals.
    static func candidates(sessionBundleId: String?) -> [String] {
        let own = sessionBundleId?.trimmingCharacters(in: .whitespacesAndNewlines) ?? ""
        guard !own.isEmpty else { return knownTerminals }
        return [own] + knownTerminals.filter { $0 != own }
    }

    /// First candidate that is currently running, or nil.
    static func pick(sessionBundleId: String?, running: Set<String>) -> String? {
        candidates(sessionBundleId: sessionBundleId).first { running.contains($0) }
    }

    /// AppleScript that selects the tab on `tty` and brings its window to the front, returning
    /// true when found. nil for terminals that don't expose a tab's tty, or a malformed tty.
    static func focusScript(bundleId: String, tty: String) -> String? {
        let name = tty.dropFirst("/dev/tty".count)
        guard tty.hasPrefix("/dev/tty"), !name.isEmpty,
              name.allSatisfy({ $0.isASCII && ($0.isLetter || $0.isNumber) }) else { return nil }
        switch bundleId {
        case "com.apple.Terminal":
            return """
            tell application id "com.apple.Terminal"
                repeat with w in windows
                    repeat with t in tabs of w
                        if tty of t is "\(tty)" then
                            set selected tab of w to t
                            set frontmost of w to true
                            return true
                        end if
                    end repeat
                end repeat
            end tell
            return false
            """
        case "com.googlecode.iterm2":
            return """
            tell application id "com.googlecode.iterm2"
                repeat with w in windows
                    repeat with t in tabs of w
                        repeat with s in sessions of t
                            if tty of s is "\(tty)" then
                                select w
                                select t
                                select s
                                return true
                            end if
                        end repeat
                    end repeat
                end repeat
            end tell
            return false
            """
        default:
            return nil
        }
    }

    /// The tmux pane on `tty` and its session id, from
    /// `tmux list-panes -a -F '#{pane_tty} #{pane_id} #{session_id}'`.
    static func tmuxPane(tty: String, listing: String) -> (pane: String, session: String)? {
        for line in listing.split(separator: "\n") {
            let f = line.split(separator: " ")
            if f.count == 3, f[0] == tty { return (String(f[1]), String(f[2])) }
        }
        return nil
    }
}
