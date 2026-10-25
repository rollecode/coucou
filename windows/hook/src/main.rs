//! coucou-hook — the relay Claude Code (and every other agent) runs on each hook
//! event.
//!
//! Reads the hook JSON on stdin, maps the agent's event and field names onto
//! Claude Code's (normalize.rs), adds a little terminal context, and hands it to
//! Coucou over the named pipe `\\.\pipe\coucou-<sid>` (Windows) or the Unix
//! socket `$XDG_RUNTIME_DIR/coucou.sock` (Linux).
//!
//! Hard rule (docs/CLAUDE.md): **never block the agent.**
//! * If the pipe does not exist — Coucou is closed — we exit 0 immediately, with
//!   only the "no opinion" reply the agent expects (reply.rs), and the session
//!   carries on untouched.
//! * Every step runs under a deadline enforced by the main thread, so a pipe that
//!   accepts the connection and then stops reading cannot wedge the session
//!   either: we abandon the worker and exit.
//! * Only `PermissionRequest` waits for an answer, because approving from the
//!   island is the whole point. No answer means no decision, and the agent asks
//!   in its terminal exactly as if Coucou were not installed.
//!
//! Usage: `coucou-hook [--agent <name>] [<EventName>]` (the event name is also
//! read from the JSON; `--agent` is absent for Claude Code), or
//! `coucou-hook --statusline` as Claude Code's status line command (plan usage,
//! see statusline.rs): it passes the plan limits on and runs the status line the
//! user had before, so that keeps working.

use std::io::{Read, Write};
use std::sync::mpsc;
use std::time::Duration;

use serde_json::{Map, Value};

mod answered;
mod normalize;
mod reply;
mod tail;

/// Budget for getting a pipe connection. Beyond this the agent wins, always.
const CONNECT_TIMEOUT: Duration = Duration::from_millis(300);
/// Whole-run budget for an event nobody waits on: connect and write, no more.
const FIRE_AND_FORGET_BUDGET: Duration = Duration::from_secs(2);
/// How long a permission prompt may stay on screen before the terminal takes over.
const DECISION_BUDGET: Duration = Duration::from_secs(110);
/// How often a waiting relay looks for another relay's answer (answered.rs).
const ANSWERED_POLL: Duration = Duration::from_millis(200);

/// Fields that are pointless to forward and can be enormous (a whole file read,
/// a full command output). The island never shows them.
const DROPPED_FIELDS: &[&str] = &["tool_response", "tool_output", "transcript_path"];
/// Longest string forwarded for any single field; the island truncates to far
/// less than this anyway.
const MAX_FIELD_LEN: usize = 2_000;

mod statusline;

/// The live diff needs the whole text of a file edit, once it has happened:
/// PostToolUse of these tools keeps its edit strings far longer than the rest.
const DIFF_TOOLS: &[&str] = &["Edit", "MultiEdit", "Write"];
/// The `tool_input` keys holding the text being replaced or written.
const DIFF_FIELDS: &[&str] = &["old_string", "new_string", "content"];
/// Per edit string. The island stops diffing at 200 KB anyway (DiffEngine).
const MAX_DIFF_FIELD_LEN: usize = 256 * 1024;
/// For all edit strings of one event together, so the line stays well under the
/// 1 MiB the app reads from the pipe even once JSON-escaped.
const MAX_DIFF_TOTAL: usize = 512 * 1024;
/// Stdin is agent-controlled: buffer no more than this. Legit payloads are
/// kilobytes; the rest would only ever be truncated away anyway.
const MAX_STDIN: u64 = 16 * 1024 * 1024;
/// The serialized event must stay under the app's 1 MiB pipe read. Truncation
/// bounds string *values*; this bounds the whole line (keys, field count) and
/// falls back to a minimal event rather than a line the app cuts mid-JSON.
const MAX_LINE: usize = 900 * 1024;

#[cfg(windows)]
mod win;
#[cfg(windows)]
use win::connect;

#[cfg(target_os = "linux")]
mod unix;
#[cfg(target_os = "linux")]
use unix::connect;

/// What the command line says: `--agent <name>` and the event name.
struct Args {
    agent: String,
    event: String,
}

fn args() -> Args {
    let mut agent = String::new();
    let mut event = String::new();
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        if arg == "--agent" {
            agent = it.next().unwrap_or_default();
        } else if event.is_empty() {
            event = arg;
        }
    }
    Args { agent, event }
}

/// One event, ready to forward.
struct Event {
    /// The payload as one line of JSON.
    line: String,
    /// The canonical event name.
    name: String,
    /// For Claude Code's AskUserQuestion, the question as it was asked.
    question: Option<Value>,
    /// The tool call, as every relay on this host names it (answered.rs).
    key: String,
}

fn main() {
    if std::env::args().skip(1).any(|a| a == "--statusline") {
        statusline::run();
    }
    let args = args();
    let mut raw = Vec::new();
    let _ = std::io::stdin().take(MAX_STDIN).read_to_end(&mut raw);
    let env = |key: &str| match key {
        TMUX_SESSION => tmux_session(),
        MACHINE_ID => machine_id(),
        HOOK_PPID => hook_ppid(),
        _ => std::env::var(key).ok(),
    };
    let cwd = std::env::current_dir().map(|p| p.to_string_lossy().to_string()).unwrap_or_default();

    let Some(event) = prepare(&raw, &args, &env, &cwd, &claude_session_name) else {
        // Nothing we could forward. An agent that needs JSON still gets its
        // "no opinion" — Copilot is fail-closed and would deny without it.
        let name = normalize::event(&args.event);
        print(reply::stdout(&args.agent, name, None, None));
        std::process::exit(0);
    };

    // Only an agent whose decisions the island can give waits for one; any other
    // would be held for nothing, its decision being thrown away (reply.rs).
    let asks = event.name == "PreToolUse" && event.question.is_some() && args.agent.is_empty();
    let waits_for_answer = (event.name == "PermissionRequest" || asks) && reply::takes_decisions(&args.agent);
    let budget = if waits_for_answer { DECISION_BUDGET } else { FIRE_AND_FORGET_BUDGET };

    // The worker owns every blocking call. If it overruns the budget we simply
    // stop listening and exit: the process dying takes the pipe handle with it.
    // (No catch_unwind here — the release profile is panic = "abort", so it would
    // be dead code. `talk` is written to have nothing to panic on instead.)
    let (tx, rx) = mpsc::channel::<Option<String>>();
    let line = event.line.clone();
    std::thread::spawn(move || {
        let _ = tx.send(talk(&line, waits_for_answer));
    });

    let decision = if waits_for_answer { wait(&rx, budget, &event.key) } else { rx.recv_timeout(budget).ok().flatten() };
    let out = reply::stdout(&args.agent, &event.name, decision.as_deref(), event.question.as_ref());
    // Answered here: the other Coucous' relays on this host let go.
    if waits_for_answer && out.is_some() {
        answered::mark(&event.key);
    }
    print(out);
    std::process::exit(0);
}

/// Waits for the island's word, giving up as soon as another relay on this
/// host has the answer for the same call.
fn wait(rx: &mpsc::Receiver<Option<String>>, budget: Duration, key: &str) -> Option<String> {
    let deadline = std::time::Instant::now() + budget;
    loop {
        match rx.recv_timeout(ANSWERED_POLL) {
            Ok(decision) => return decision,
            Err(mpsc::RecvTimeoutError::Disconnected) => return None,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if answered::is_answered(key) || std::time::Instant::now() >= deadline {
                    return None;
                }
            }
        }
    }
}

fn print(line: Option<String>) {
    if let Some(line) = line {
        let mut out = std::io::stdout();
        let _ = writeln!(out, "{line}");
        let _ = out.flush();
    }
}

/// The payload to forward, from the raw stdin bytes. `env` reads an environment
/// variable and `cwd` is the working directory, so tests stay pure.
fn prepare(raw: &[u8], args: &Args, env: &dyn Fn(&str) -> Option<String>, cwd: &str,
           names: &dyn Fn(&str) -> Option<String>) -> Option<Event> {
    // Some shells hand us a UTF-8 BOM; serde_json would choke on it.
    let raw = raw.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(raw);
    if headless(env) {
        return None;
    }
    let mut payload = serde_json::from_slice::<Value>(raw).ok()?;
    let map = payload.as_object_mut()?;

    // Which agent this hook was installed for, so the app routes it to the right
    // pill. Absent means Claude Code, so existing hook commands keep working
    // unchanged; invalid names are discarded by the app, not here. A Claude Code
    // session started from the Claude desktop app is tagged `claude-desktop`.
    if let Some(tag) = agent_tag(&args.agent, env) {
        map.insert("coucou_agent".into(), Value::String(tag));
    }
    // Claude Code in Cursor's terminal goes on the Cursor pill (Mac #120).
    if !map.contains_key("term_editor") {
        if let Some(editor) = term_editor(env) {
            map.insert("term_editor".into(), Value::String(editor.into()));
        }
    }
    // Coucou's own chat runs Claude Code (claude_code.rs): the island shows its
    // permission requests, not a session.
    if env("COUCOU_CHAT").as_deref() == Some("1") {
        map.insert("coucou_chat".into(), Value::Bool(true));
    }

    let raw_event = map
        .get("hook_event_name")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| args.event.clone());
    normalize::fields(map, env);
    let name = normalize::refine(normalize::event(&raw_event), map);
    let key = answered::key(map);
    // Claude Code asks at PreToolUse (2.1.x): the island shows it as its question card.
    let asks = name == "PreToolUse" && args.agent.is_empty() && map.get("tool_name").and_then(Value::as_str) == Some("AskUserQuestion");
    let wire = if asks { "PermissionRequest" } else { name.as_str() };
    map.insert("hook_event_name".into(), Value::String(wire.to_string()));

    // Of what a finished command printed, only its last lines go on (tail.rs).
    let tool = map.get("tool_name").and_then(Value::as_str);
    if name == "PostToolUse" && matches!(tool, Some("Bash" | "PowerShell")) {
        if let Some(lines) = map.get("tool_response").and_then(command_tail) {
            map.insert("tool_tail".into(), Value::from(lines));
        }
    }

    for field in DROPPED_FIELDS {
        map.remove(*field);
    }

    if map.get("cwd").and_then(Value::as_str).map(str::is_empty).unwrap_or(true) && !cwd.is_empty() {
        map.insert("cwd".into(), Value::String(cwd.to_string()));
    }

    add_terminal_context(map, env);
    // The tmux session, so another machine can attach to it (remote Open terminal).
    if matches!(name.as_str(), "SessionStart" | "UserPromptSubmit") {
        if let Some(session) = env(TMUX_SESSION).filter(|s| is_tmux_name(s)) {
            map.insert("tmux_session".into(), Value::String(session));
        }
    }
    // As a remote relay: the machine and who ran the hook, so Coucou on this same machine finds the session's terminal.
    if env("COUCOU_SOCKET").is_some_and(|s| !s.is_empty()) {
        if matches!(name.as_str(), "SessionStart" | "UserPromptSubmit") {
            if let Some(id) = env(MACHINE_ID).filter(|id| is_machine_id(id)) {
                map.insert("machine_id".into(), Value::String(id));
            }
        }
        if let Some(ppid) = env(HOOK_PPID).and_then(|p| p.parse::<u32>().ok()) {
            map.insert("hook_ppid".into(), ppid.into());
        }
    }

    // The session's own name (Claude Code's /rename) and the home folder, for the pill's name.
    if let Some(name) = map.get("session_id").and_then(Value::as_str).and_then(|id| names(id)) {
        map.insert("session_name".into(), Value::String(name));
    }
    if let Some(home) = env("HOME").or_else(|| env("USERPROFILE")) {
        map.insert("home".into(), Value::String(home));
    }

    // Kept whole: what goes back to Claude Code must be its own input, not the
    // shortened copy the island is shown.
    let question = (map.get("tool_name").and_then(Value::as_str) == Some("AskUserQuestion"))
        .then(|| map.get("tool_input").cloned())
        .flatten();

    // Every string is capped, except the edit text of a finished Edit /
    // MultiEdit / Write, which the live diff needs whole (within its own limits).
    truncate_payload(&mut payload, &name);

    let mut line = payload.to_string();
    if line.len() > MAX_LINE && name == "PermissionRequest" {
        // The card could only show the tool name, and Allow would run the whole
        // call: the terminal asks instead.
        return None;
    }
    if line.len() > MAX_LINE {
        // Keys and field count escape truncate_strings, so the line can still
        // be too long. Send what the island needs rather than bytes the app
        // would cut mid-line and drop as invalid JSON.
        let mut minimal = Map::new();
        for key in ["hook_event_name", "session_id", "tool_name", "coucou_agent", "cwd"] {
            if let Some(v) = payload.get(key) {
                minimal.insert(key.into(), v.clone());
            }
        }
        minimal.insert("coucou_truncated".into(), Value::Bool(true));
        line = Value::Object(minimal).to_string();
    }
    line.push('\n');
    Some(Event { line, name, question, key })
}

/// A command's `tool_response`: an object with `stdout` / `stderr`, or a string.
fn command_tail(response: &Value) -> Option<Vec<String>> {
    match response {
        Value::String(s) => tail::output_tail(Some(s), None),
        Value::Object(o) => tail::output_tail(
            o.get("stdout").and_then(Value::as_str),
            o.get("stderr").and_then(Value::as_str),
        ),
        _ => None,
    }
}

/// Not an environment variable: `env` asks tmux for the session under this key.
const TMUX_SESSION: &str = "coucou:tmux_session";

/// Not environment variables either: this machine's id and the relay's parent.
const MACHINE_ID: &str = "coucou:machine_id";
const HOOK_PPID: &str = "coucou:hook_ppid";

/// Hex digits and dashes: a Linux machine-id or a Mac's IOPlatformUUID.
fn is_machine_id(s: &str) -> bool {
    (1..=64).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-')
}

#[cfg(target_os = "linux")]
fn machine_id() -> Option<String> {
    ["/etc/machine-id", "/var/lib/dbus/machine-id"]
        .iter()
        .find_map(|path| std::fs::read_to_string(path).ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty()))
}

#[cfg(not(target_os = "linux"))]
fn machine_id() -> Option<String> {
    None
}

#[cfg(target_os = "linux")]
fn hook_ppid() -> Option<String> {
    Some(std::os::unix::process::parent_id().to_string())
}

#[cfg(not(target_os = "linux"))]
fn hook_ppid() -> Option<String> {
    None
}

/// A tmux session name safe to hand to another machine's command line.
fn is_tmux_name(s: &str) -> bool {
    (1..=64).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}

/// The tmux session this pane belongs to, asked with a short deadline.
fn tmux_session() -> Option<String> {
    use std::process::{Command, Stdio};
    use std::time::{Duration, Instant};
    std::env::var_os("TMUX")?;
    let mut cmd = Command::new("tmux");
    cmd.args(["display-message", "-p"]);
    if let Some(pane) = std::env::var_os("TMUX_PANE") {
        cmd.arg("-t").arg(pane);
    }
    let mut child = cmd.arg("#S").stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::null()).spawn().ok()?;
    let deadline = Instant::now() + Duration::from_millis(300);
    while child.try_wait().ok()?.is_none() {
        if Instant::now() >= deadline {
            let _ = child.kill();
            return None;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    let mut out = String::new();
    child.stdout.take()?.read_to_string(&mut out).ok()?;
    Some(out.trim().to_string())
}

/// The name given with /rename, from Claude Code's session registry.
fn claude_session_name(session_id: &str) -> Option<String> {
    let home = std::env::var("HOME").or_else(|_| std::env::var("USERPROFILE")).ok()?;
    let dir = std::path::Path::new(&home).join(".claude").join("sessions");
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&path) else { continue };
        let Ok(v) = serde_json::from_str::<Value>(&text) else { continue };
        if v.get("sessionId").and_then(Value::as_str) == Some(session_id) {
            return v.get("name").and_then(Value::as_str).filter(|n| !n.is_empty()).map(str::to_string);
        }
    }
    None
}

/// `claude -p`, the SDK and scheduled jobs: no one watches them, so they stay
/// off the island. Coucou's own runs set COUCOU_WATCHED=1.
fn headless(env: &dyn Fn(&str) -> Option<String>) -> bool {
    env("CLAUDE_CODE_ENTRYPOINT").is_some_and(|e| e.starts_with("sdk-"))
        && env("COUCOU_WATCHED").as_deref() != Some("1")
}

/// Which terminal the session runs in. Unlike macOS, Coucou here accepts events
/// from every terminal, so this is context only — never a filter.
fn add_terminal_context(map: &mut Map<String, Value>, env: &dyn Fn(&str) -> Option<String>) {
    for (key, var) in [
        ("term_program", "TERM_PROGRAM"),
        ("wt_session", "WT_SESSION"),
        ("term_session_id", "TERM_SESSION_ID"),
        ("vscode_pid", "VSCODE_PID"),
        ("session_pid", "CLAUDE_CODE_SSE_PORT"),
    ] {
        if !map.contains_key(key) {
            map.insert(key.into(), Value::String(env(var).unwrap_or_default()));
        }
    }
}

/// The `coucou_agent` tag: `--agent` when given, otherwise `claude-desktop` for
/// a Claude Code session started from the Claude desktop app, which says so in
/// CLAUDE_CODE_ENTRYPOINT — the same rule as the Mac's relay (#191). Nothing
/// for a plain Claude Code session.
fn agent_tag(arg: &str, env: &dyn Fn(&str) -> Option<String>) -> Option<String> {
    if !arg.is_empty() {
        return Some(arg.to_string());
    }
    (env("CLAUDE_CODE_ENTRYPOINT").as_deref() == Some("claude-desktop"))
        .then(|| "claude-desktop".to_string())
}

/// `cursor` when the session runs in Cursor's integrated terminal. Cursor sets
/// TERM_PROGRAM=vscode like VS Code does, so it is told apart by its own trace
/// variable, or by its executable behind VS Code's git helper.
fn term_editor(env: &dyn Fn(&str) -> Option<String>) -> Option<&'static str> {
    if env("CURSOR_TRACE_ID").is_some_and(|v| !v.is_empty()) {
        return Some("cursor");
    }
    let helper = env("VSCODE_GIT_ASKPASS_NODE").unwrap_or_default();
    let exe = helper.rsplit(['/', '\\']).next().unwrap_or_default().to_ascii_lowercase();
    exe.starts_with("cursor").then_some("cursor")
}

/// Caps the strings of a payload: every field to MAX_FIELD_LEN, except the edit
/// strings of a finished Edit / MultiEdit / Write, which the live diff needs
/// whole. If even those had to be cut, `coucou_diff_truncated` tells the island
/// not to show counts it cannot trust.
fn truncate_payload(payload: &mut serde_json::Value, event: &str) {
    let keeps_diff = event == "PostToolUse"
        && payload
            .get("tool_name")
            .and_then(|v| v.as_str())
            .is_some_and(|tool| DIFF_TOOLS.contains(&tool));
    let input = if keeps_diff {
        payload.as_object_mut().and_then(|map| map.remove("tool_input"))
    } else {
        None
    };

    truncate_strings(payload);

    if let Some(mut input) = input {
        let mut budget = MAX_DIFF_TOTAL;
        let mut cut_any = false;
        cap_diff_strings(&mut input, &mut budget, &mut cut_any);
        if let Some(map) = payload.as_object_mut() {
            map.insert("tool_input".into(), input);
            if cut_any {
                map.insert("coucou_diff_truncated".into(), serde_json::Value::Bool(true));
            }
        }
    }
}

/// `tool_input` of a diff tool: edit strings share MAX_DIFF_TOTAL, each capped at
/// MAX_DIFF_FIELD_LEN; any other string gets the ordinary cap.
fn cap_diff_strings(value: &mut serde_json::Value, budget: &mut usize, cut_any: &mut bool) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, v) in map.iter_mut() {
                match v {
                    serde_json::Value::String(s) if DIFF_FIELDS.contains(&key.as_str()) => {
                        let limit = MAX_DIFF_FIELD_LEN.min(*budget);
                        if cut(s, limit) {
                            *cut_any = true;
                        }
                        *budget = budget.saturating_sub(s.len());
                    }
                    _ => cap_diff_strings(v, budget, cut_any),
                }
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                cap_diff_strings(item, budget, cut_any);
            }
        }
        serde_json::Value::String(s) => {
            cut(s, MAX_FIELD_LEN);
        }
        _ => {}
    }
}

/// Caps every string in the payload. A single Write can carry a whole file.
fn truncate_strings(value: &mut Value) {
    match value {
        Value::String(s) => {
            cut(s, MAX_FIELD_LEN);
        }
        Value::Array(items) => items.iter_mut().for_each(truncate_strings),
        Value::Object(map) => map.values_mut().for_each(truncate_strings),
        _ => {}
    }
}

/// Shortens `s` to at most `max` bytes plus an ellipsis; true if it was cut.
fn cut(s: &mut String, max: usize) -> bool {
    if s.len() <= max {
        return false;
    }
    // Cut on a char boundary; a lone byte index can split UTF-8.
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    s.truncate(end);
    s.push('…');
    true
}

/// Connect, send, and — for a permission request — wait for the island's word.
fn talk(payload: &str, waits_for_answer: bool) -> Option<String> {
    let mut pipe = connect()?;

    if pipe.write_all(payload.as_bytes()).is_err() {
        return None;
    }
    let _ = pipe.flush();

    if !waits_for_answer {
        return None;
    }

    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        match pipe.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                // The biggest legit answer is an {"answers":{…}} line — a peer
                // that keeps streaming is not one we wait on.
                if buf.contains(&b'\n') || buf.len() > 64 * 1024 {
                    break;
                }
            }
            Err(_) => break,
        }
    }
    let answer = String::from_utf8_lossy(&buf).trim().to_string();
    (!answer.is_empty()).then_some(answer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(raw: &str, agent: &str, event: &str) -> (Value, Event) {
        let args = Args { agent: agent.into(), event: event.into() };
        let ev = prepare(raw.as_bytes(), &args, &|_| None, "/home/me/here", &|_| None).expect("forwarded");
        let v = serde_json::from_str(ev.line.trim_end()).unwrap();
        (v, ev)
    }

    #[test]
    fn session_name_and_home_ride_along() {
        let args = Args { agent: "".into(), event: "SessionStart".into() };
        let names = |id: &str| (id == "s1").then(|| "coucou-mac".to_string());
        let raw = br#"{"session_id":"s1","hook_event_name":"SessionStart","cwd":"/home/me"}"#;
        let ev = prepare(raw, &args, &env_of(&[("HOME", "/home/me")]), "/home/me", &names).unwrap();
        let v: Value = serde_json::from_str(ev.line.trim_end()).unwrap();
        assert_eq!(v["session_name"], "coucou-mac");
        assert_eq!(v["home"], "/home/me");
        let ev = prepare(raw, &args, &|_| None, "/home/me", &|_| None).unwrap();
        let v: Value = serde_json::from_str(ev.line.trim_end()).unwrap();
        assert!(v.get("session_name").is_none());
    }

    fn env_of(vars: &'static [(&'static str, &'static str)]) -> impl Fn(&str) -> Option<String> {
        move |name| vars.iter().find(|(k, _)| *k == name).map(|(_, v)| v.to_string())
    }

    #[test]
    fn a_tmux_session_rides_with_session_start_and_prompts_only() {
        let args = |e: &str| Args { agent: String::new(), event: e.into() };
        let env = env_of(&[(TMUX_SESSION, "work-2")]);
        let line = |raw: &str, e: &str| {
            let ev = prepare(raw.as_bytes(), &args(e), &env, "/w", &|_| None).unwrap();
            serde_json::from_str::<Value>(ev.line.trim_end()).unwrap()
        };
        assert_eq!(line(r#"{"hook_event_name":"SessionStart"}"#, "SessionStart")["tmux_session"], "work-2");
        assert_eq!(line(r#"{"hook_event_name":"UserPromptSubmit"}"#, "UserPromptSubmit")["tmux_session"], "work-2");
        assert!(line(r#"{"hook_event_name":"PreToolUse"}"#, "PreToolUse").get("tmux_session").is_none());
        let odd = env_of(&[(TMUX_SESSION, "a b;rm")]);
        let ev = prepare(br#"{"hook_event_name":"SessionStart"}"#, &args("SessionStart"), &odd, "/w", &|_| None).unwrap();
        assert!(!ev.line.contains("tmux_session"));
    }

    #[test]
    fn the_machine_rides_with_session_start_and_prompts_and_the_parent_with_every_event() {
        let args = |e: &str| Args { agent: String::new(), event: e.into() };
        let env = env_of(&[("COUCOU_SOCKET", "/h/.coucou/1.sock"), (MACHINE_ID, "0123abcdef"), (HOOK_PPID, "4242")]);
        let line = |raw: &str, e: &str, env: &dyn Fn(&str) -> Option<String>| {
            let ev = prepare(raw.as_bytes(), &args(e), env, "/w", &|_| None).unwrap();
            serde_json::from_str::<Value>(ev.line.trim_end()).unwrap()
        };
        let start = line(r#"{"hook_event_name":"SessionStart"}"#, "SessionStart", &env);
        assert_eq!((start["machine_id"].as_str(), start["hook_ppid"].as_u64()), (Some("0123abcdef"), Some(4242)));
        let tool = line(r#"{"hook_event_name":"PreToolUse"}"#, "PreToolUse", &env);
        assert!(tool.get("machine_id").is_none());
        assert_eq!(tool["hook_ppid"], 4242);
        let odd = env_of(&[("COUCOU_SOCKET", "/h/.coucou/1.sock"), (MACHINE_ID, "../etc/passwd"), (HOOK_PPID, "1; rm")]);
        let start = line(r#"{"hook_event_name":"SessionStart"}"#, "SessionStart", &odd);
        assert!(start.get("machine_id").is_none() && start.get("hook_ppid").is_none());
        let local = env_of(&[(MACHINE_ID, "0123abcdef"), (HOOK_PPID, "4242")]);
        let start = line(r#"{"hook_event_name":"SessionStart"}"#, "SessionStart", &local);
        assert!(start.get("machine_id").is_none() && start.get("hook_ppid").is_none(), "a local relay sends neither");
    }

    #[test]
    fn claude_desktop_sessions_are_tagged_and_an_explicit_agent_wins() {
        let desktop = env_of(&[("CLAUDE_CODE_ENTRYPOINT", "claude-desktop")]);
        assert_eq!(agent_tag("", &desktop).as_deref(), Some("claude-desktop"));
        assert_eq!(agent_tag("gemini", &desktop).as_deref(), Some("gemini"));
        assert_eq!(agent_tag("", &env_of(&[("CLAUDE_CODE_ENTRYPOINT", "cli")])), None);
        assert_eq!(agent_tag("", &env_of(&[])), None);
    }

    #[test]
    fn headless_runs_are_not_forwarded_unless_watched() {
        let raw = br#"{"hook_event_name":"Stop","session_id":"s"}"#;
        let args = Args { agent: String::new(), event: String::new() };
        let run = |vars: &'static [(&'static str, &'static str)]| prepare(raw, &args, &env_of(vars), "/p", &|_| None).is_some();
        assert!(!run(&[("CLAUDE_CODE_ENTRYPOINT", "sdk-cli")]));
        assert!(!run(&[("CLAUDE_CODE_ENTRYPOINT", "sdk-py")]));
        assert!(run(&[("CLAUDE_CODE_ENTRYPOINT", "sdk-cli"), ("COUCOU_WATCHED", "1")]));
        assert!(run(&[("CLAUDE_CODE_ENTRYPOINT", "cli")]));
        assert!(run(&[]));
    }

    #[test]
    fn cursor_is_told_apart_from_vs_code() {
        assert_eq!(term_editor(&env_of(&[("CURSOR_TRACE_ID", "abc")])), Some("cursor"));
        assert_eq!(
            term_editor(&env_of(&[(
                "VSCODE_GIT_ASKPASS_NODE",
                r"C:\Users\me\AppData\Local\Programs\cursor\Cursor.exe"
            )])),
            Some("cursor")
        );
        assert_eq!(
            term_editor(&env_of(&[(
                "VSCODE_GIT_ASKPASS_NODE",
                r"C:\Users\me\AppData\Local\Programs\Microsoft VS Code\Code.exe"
            )])),
            None
        );
        assert_eq!(term_editor(&env_of(&[("CURSOR_TRACE_ID", "")])), None);
        assert_eq!(term_editor(&env_of(&[("TERM_PROGRAM", "vscode")])), None);
    }

    #[test]
    fn coucous_own_chat_is_tagged() {
        let args = Args { agent: String::new(), event: "PreToolUse".into() };
        let raw = br#"{"hook_event_name":"PreToolUse"}"#;
        let tagged = prepare(raw, &args, &env_of(&[("COUCOU_CHAT", "1")]), "/p", &|_| None).unwrap();
        assert!(tagged.line.contains(r#""coucou_chat":true"#));
        let plain = prepare(raw, &args, &env_of(&[]), "/p", &|_| None).unwrap();
        assert!(!plain.line.contains("coucou_chat"));
    }

    #[test]
    fn long_strings_are_cut_on_a_char_boundary() {
        let mut v = serde_json::json!({ "tool_input": { "content": "é".repeat(4000) } });
        truncate_strings(&mut v);
        let s = v["tool_input"]["content"].as_str().unwrap();
        assert!(s.len() <= MAX_FIELD_LEN + 4);
        assert!(s.ends_with('…'));
    }

    #[test]
    fn claude_code_payloads_are_forwarded_as_they_are() {
        let (v, ev) = run(r#"{"hook_event_name":"PreToolUse","tool_name":"Bash","cwd":"/p"}"#, "", "PreToolUse");
        assert_eq!(ev.name, "PreToolUse");
        assert!(v.get("coucou_agent").is_none());
        assert_eq!(v["cwd"], "/p");
        assert_eq!(v["tool_name"], "Bash");
    }

    #[test]
    fn a_line_that_is_still_too_long_becomes_a_minimal_event() {
        // Keys escape truncate_strings: a giant key name still forwards —
        // as a minimal, well-formed event rather than a line the app drops.
        let mut payload = Map::new();
        payload.insert("hook_event_name".into(), serde_json::json!("PreToolUse"));
        payload.insert("session_id".into(), serde_json::json!("s1"));
        payload.insert("tool_name".into(), serde_json::json!("Bash"));
        payload.insert("k".repeat(MAX_LINE).into(), serde_json::json!("v"));
        let raw = serde_json::to_vec(&payload).unwrap();
        let args = Args { agent: String::new(), event: String::new() };
        let ev = prepare(&raw, &args, &|_| None, "/tmp", &|_| None).expect("forwarded");
        assert!(ev.line.len() < MAX_LINE);
        let v: Value = serde_json::from_str(ev.line.trim_end()).unwrap();
        assert_eq!(v["hook_event_name"], "PreToolUse");
        assert_eq!(v["tool_name"], "Bash");
        assert_eq!(v["coucou_truncated"], true);
    }

    #[test]
    fn a_permission_request_too_long_to_show_is_left_to_the_terminal() {
        // The island could only show the tool name, and Allow would run the whole call.
        let mut payload = Map::new();
        payload.insert("hook_event_name".into(), serde_json::json!("PermissionRequest"));
        payload.insert("tool_name".into(), serde_json::json!("MultiEdit"));
        let edits: Vec<Value> = (0..2000)
            .map(|i| serde_json::json!({ "old_string": format!("{i}{}", "a".repeat(400)), "new_string": "b".repeat(400) }))
            .collect();
        payload.insert("tool_input".into(), serde_json::json!({ "file_path": "/p/a.rs", "edits": edits }));
        let raw = serde_json::to_vec(&payload).unwrap();
        let args = Args { agent: String::new(), event: String::new() };
        assert!(prepare(&raw, &args, &|_| None, "/tmp", &|_| None).is_none());
    }

    #[test]
    fn an_agents_event_is_tagged_and_renamed() {
        // Gemini CLI: its own name in the payload, ours on the command line.
        let (v, ev) = run(r#"{"hook_event_name":"BeforeTool","toolCall":{"name":"shell","args":{"CommandLine":"ls"}}}"#, "gemini", "PreToolUse");
        assert_eq!(ev.name, "PreToolUse");
        assert_eq!(v["hook_event_name"], "PreToolUse");
        assert_eq!(v["coucou_agent"], "gemini");
        assert_eq!(v["tool_input"]["command"], "ls");
        assert_eq!(v["cwd"], "/home/me/here");

        // Copilot CLI sends no event name: the command line's camelCase one is used.
        let (_, ev) = run(r#"{"toolName":"bash"}"#, "copilot", "permissionRequest");
        assert_eq!(ev.name, "PermissionRequest");

        // Cursor: a stop that failed, and its tool output left behind.
        let (v, ev) = run(r#"{"hook_event_name":"stop","status":"error","tool_output":"huge"}"#, "cursor", "");
        assert_eq!(ev.name, "StopFailure");
        assert!(v.get("tool_output").is_none());
    }

    #[test]
    fn a_question_is_kept_whole_and_only_for_ask_user_question() {
        let long = "x".repeat(3000);
        let raw = format!(r#"{{"hook_event_name":"PermissionRequest","tool_name":"AskUserQuestion","tool_input":{{"questions":[{{"question":"{long}"}}]}}}}"#);
        let (v, ev) = run(&raw, "", "");
        assert_eq!(ev.question.unwrap()["questions"][0]["question"].as_str().unwrap().len(), 3000);
        assert!(v["tool_input"]["questions"][0]["question"].as_str().unwrap().ends_with('…'));
        let (_, ev) = run(r#"{"hook_event_name":"PermissionRequest","tool_name":"Bash","tool_input":{"command":"ls"}}"#, "", "");
        assert!(ev.question.is_none());
    }

    #[test]
    fn what_cannot_be_read_is_not_forwarded() {
        let args = Args { agent: "copilot".into(), event: "preToolUse".into() };
        for raw in ["", "not json", "[1,2]"] {
            assert!(prepare(raw.as_bytes(), &args, &|_| None, "/", &|_| None).is_none());
        }
        // A BOM is not a reason to drop the event.
        let mut bom = vec![0xEF, 0xBB, 0xBF];
        bom.extend_from_slice(br#"{"hook_event_name":"Stop"}"#);
        assert!(prepare(&bom, &args, &|_| None, "/", &|_| None).is_some());
    }

    #[test]
    fn the_live_diff_rule_holds_through_the_normalised_relay() {
        let big = "z".repeat(10_000);
        // Claude Code's finished Edit: kept whole.
        let raw = format!(r#"{{"hook_event_name":"PostToolUse","tool_name":"Edit","tool_input":{{"old_string":"{big}","new_string":"a"}}}}"#);
        let (v, _) = run(&raw, "", "");
        assert_eq!(v["tool_input"]["old_string"].as_str().unwrap().len(), big.len());
        // The same edit before it happens, or an agent's event renamed onto
        // PreToolUse: the ordinary cap.
        let raw = format!(r#"{{"hook_event_name":"PreToolUse","tool_name":"Edit","tool_input":{{"old_string":"{big}"}}}}"#);
        let (v, _) = run(&raw, "", "");
        assert!(v["tool_input"]["old_string"].as_str().unwrap().len() <= MAX_FIELD_LEN + 4);
        let raw = format!(r#"{{"hook_event_name":"BeforeTool","tool_name":"Edit","tool_input":{{"content":"{big}"}}}}"#);
        let (v, ev) = run(&raw, "gemini", "");
        assert_eq!(ev.name, "PreToolUse");
        assert!(v["tool_input"]["content"].as_str().unwrap().len() <= MAX_FIELD_LEN + 4);
    }

    #[test]
    fn a_finished_command_forwards_only_its_last_lines() {
        let raw = r#"{"hook_event_name":"PostToolUse","tool_name":"Bash","tool_input":{"command":"npm test"},"tool_response":{"stdout":"1\n2\n3\n\u001b[32m4\u001b[0m\n","stderr":""}}"#;
        let (v, _) = run(raw, "", "");
        assert_eq!(v["tool_tail"], serde_json::json!(["2", "3", "4"]));
        assert!(v.get("tool_response").is_none());
        // A string response, and stderr when stdout is empty.
        let raw = r#"{"hook_event_name":"PostToolUse","tool_name":"PowerShell","tool_response":"only line"}"#;
        assert_eq!(run(raw, "", "").0["tool_tail"], serde_json::json!(["only line"]));
        let raw = r#"{"hook_event_name":"PostToolUse","tool_name":"Bash","tool_response":{"stdout":"","stderr":"boom"}}"#;
        assert_eq!(run(raw, "", "").0["tool_tail"], serde_json::json!(["boom"]));
        // Other tools and other events: nothing.
        let raw = r#"{"hook_event_name":"PostToolUse","tool_name":"Read","tool_response":{"stdout":"x"}}"#;
        assert!(run(raw, "", "").0.get("tool_tail").is_none());
        let raw = r#"{"hook_event_name":"PreToolUse","tool_name":"Bash","tool_response":{"stdout":"x"}}"#;
        assert!(run(raw, "", "").0.get("tool_tail").is_none());
    }

    #[test]
    fn a_finished_edit_keeps_its_text_whole_for_the_live_diff() {
        let big = "line\n".repeat(4_000); // 20 KB, well past MAX_FIELD_LEN
        let mut v = serde_json::json!({
            "tool_name": "Edit",
            "tool_input": { "file_path": "/p/a.ts", "old_string": big, "new_string": big },
            "cwd": "x".repeat(4_000),
        });
        truncate_payload(&mut v, "PostToolUse");
        assert_eq!(v["tool_input"]["old_string"].as_str().unwrap().len(), big.len());
        assert_eq!(v["tool_input"]["new_string"].as_str().unwrap().len(), big.len());
        // Everything else keeps the ordinary cap, and nothing says "cut".
        assert!(v["cwd"].as_str().unwrap().len() <= MAX_FIELD_LEN + 4);
        assert!(v.get("coucou_diff_truncated").is_none());

        let mut multi = serde_json::json!({
            "tool_name": "MultiEdit",
            "tool_input": { "edits": [{ "old_string": big, "new_string": "x" }] },
        });
        truncate_payload(&mut multi, "PostToolUse");
        assert_eq!(multi["tool_input"]["edits"][0]["old_string"].as_str().unwrap().len(), big.len());
    }

    #[test]
    fn edits_are_still_capped_before_they_happen_and_for_other_tools() {
        let big = "é".repeat(4_000);
        for (event, tool) in [("PreToolUse", "Edit"), ("PermissionRequest", "Write"), ("PostToolUse", "Bash")] {
            let mut v = serde_json::json!({ "tool_name": tool, "tool_input": { "content": big } });
            truncate_payload(&mut v, event);
            assert!(v["tool_input"]["content"].as_str().unwrap().len() <= MAX_FIELD_LEN + 4, "{event} {tool}");
        }
    }

    #[test]
    fn an_edit_beyond_the_budget_is_cut_and_flagged() {
        let huge = "x".repeat(MAX_DIFF_FIELD_LEN + 10);
        let mut v = serde_json::json!({
            "tool_name": "Write",
            "tool_input": { "file_path": "/p/big.txt", "content": huge },
        });
        truncate_payload(&mut v, "PostToolUse");
        assert!(v["tool_input"]["content"].as_str().unwrap().len() <= MAX_DIFF_FIELD_LEN + 4);
        assert_eq!(v["coucou_diff_truncated"], serde_json::Value::Bool(true));

        // Together, the edit strings never pass the shared budget.
        let half = "y".repeat(MAX_DIFF_FIELD_LEN - 1);
        let edits: Vec<_> = (0..4)
            .map(|_| serde_json::json!({ "old_string": half, "new_string": half }))
            .collect();
        let mut multi = serde_json::json!({ "tool_name": "MultiEdit", "tool_input": { "edits": edits } });
        truncate_payload(&mut multi, "PostToolUse");
        assert!(multi.to_string().len() < MAX_DIFF_TOTAL + 64 * 1024);
        assert_eq!(multi["coucou_diff_truncated"], serde_json::Value::Bool(true));
    }

    /// Runs `call` (a Python expression over `m`, the relay module, and `c`, one
    /// case) for each case. None when python3 is missing.
    fn python(call: &str, cases: &Value) -> Option<Vec<Value>> {
        use std::process::{Command, Stdio};
        let script = format!(
            "import importlib.util,json,sys\ns=importlib.util.spec_from_file_location('r',sys.argv[1]);m=importlib.util.module_from_spec(s);s.loader.exec_module(m)\nprint(json.dumps([{call} for c in json.loads(sys.stdin.buffer.read())]))"
        );
        let mut child = Command::new("python3")
            .args(["-B", "-c", &script, concat!(env!("CARGO_MANIFEST_DIR"), "/coucou-hook.py")])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .ok()?;
        child.stdin.take()?.write_all(cases.to_string().as_bytes()).ok()?;
        let out = child.wait_with_output().ok()?;
        assert!(out.status.success(), "the Python relay failed");
        serde_json::from_slice(&out.stdout).ok()
    }

    fn as_json(s: Option<&str>) -> Value {
        s.map(|s| serde_json::from_str(s).expect("JSON")).unwrap_or(Value::Null)
    }

    #[test]
    fn the_python_relay_replies_as_reply_rs_does() {
        let question = serde_json::json!({ "questions": [
            { "question": "Which?", "options": [{ "label": "A" }, { "label": "B" }] },
            { "question": "Many?", "multiSelect": true, "options": [{ "label": "X" }, { "label": "Y" }] },
        ] });
        let decisions = [
            Value::Null, "allow".into(), "always".into(), "deny".into(), " allow\n".into(), "ask".into(), "".into(),
            r#"{"answers":{"Which?":"A","Many?":["X","Y"]}}"#.into(),
            r#"{"answers":{"Which?":"C","Many?":["X"]}}"#.into(),
            r#"{"answers":{"Which?":"A","Many?":["X","X"]}}"#.into(),
            r#"{"answers":{"Which?":"A"}}"#.into(),
        ];
        let mut cases = Vec::new();
        for event in ["PermissionRequest", "PreToolUse", "Stop"] {
            for decision in &decisions {
                for q in [Value::Null, question.clone()] {
                    cases.push(serde_json::json!([event, decision, q]));
                }
            }
        }
        let Some(py) = python("m.stdout(*c)", &Value::Array(cases.clone())) else { return };
        for (case, got) in cases.iter().zip(py) {
            let (event, decision) = (case[0].as_str().unwrap(), case[1].as_str());
            let q = (!case[2].is_null()).then_some(&case[2]);
            let want = reply::stdout("", event, decision, q);
            assert_eq!(as_json(got.as_str()), as_json(want.as_deref()), "{case}");
        }
    }

    #[test]
    fn the_python_relay_forwards_what_the_rust_one_does() {
        let long = "é".repeat(1500);
        let edit = "x".repeat(300 * 1024);
        let raws = [
            serde_json::json!({ "hook_event_name": "PreToolUse", "session_id": "s", "cwd": "/w", "tool_name": "Bash", "tool_input": { "command": long }, "transcript_path": "/t" }).to_string(),
            serde_json::json!({ "hook_event_name": "PostToolUse", "session_id": "s", "tool_name": "Edit", "tool_input": { "file_path": "/f", "old_string": edit, "new_string": "y" }, "tool_response": "big" }).to_string(),
            serde_json::json!({ "hook_event_name": "PermissionRequest", "session_id": "s", "tool_name": "AskUserQuestion", "tool_input": { "questions": [] } }).to_string(),
            serde_json::json!({ "hook_event_name": "Stop", "status": "error" }).to_string(),
            serde_json::json!({ "hook_event_name": "PreToolUse", "session_id": "s", "tool_use_id": "toolu_01X", "tool_name": "AskUserQuestion", "tool_input": { "questions": [{ "question": "Q?" }] } }).to_string(),
            serde_json::json!({ "hook_event_name": "PermissionRequest", "session_id": "s", "tool_name": "Bash", "tool_input": { "z": [1, true, null], "a": "ä \"q\" \\ \n\t\u{1}/€", "n": -3 } }).to_string(),
            format!("\u{feff}{}", serde_json::json!({ "session_id": "s", "cwd": "" })),
            serde_json::json!({ "hook_event_name": "UserPromptSubmit", "session_id": "s", "prompt": "hi" }).to_string(),
            "not json".to_string(),
        ];
        // A local relay, then a remote one (COUCOU_SOCKET) with good and with bad machine ids and parents.
        let envs: [&'static [(&'static str, &'static str)]; 3] = [
            &[("CURSOR_TRACE_ID", "1"), ("TERM_PROGRAM", "kitty"), (MACHINE_ID, "0123abcdef"), (HOOK_PPID, "4242")],
            &[("COUCOU_SOCKET", "/h/.coucou/1.sock"), (MACHINE_ID, "0123-ABCdef"), (HOOK_PPID, "4242")],
            &[("COUCOU_SOCKET", "/h/.coucou/1.sock"), (MACHINE_ID, "../etc"), (HOOK_PPID, "1; rm")],
        ];
        let mut cases = Vec::new();
        for vars in envs {
            let env: Map<String, Value> = vars.iter().map(|(k, v)| (k.to_string(), Value::from(*v))).collect();
            cases.extend(raws.iter().map(|r| serde_json::json!([r, "SessionStart", env, "/home/me/here"])));
        }
        let Some(py) = python("m.prepare(c[0].encode(), c[1], c[2], c[3])", &Value::Array(cases)) else { return };
        let args = Args { agent: String::new(), event: "SessionStart".into() };
        let mut py = py.into_iter();
        for vars in envs {
            for raw in &raws {
                let got = py.next().unwrap();
                match prepare(raw.as_bytes(), &args, &env_of(vars), "/home/me/here", &|_| None) {
                    None => assert!(got.is_null(), "{raw:.40}"),
                    Some(ev) => {
                        let line: Value = serde_json::from_str(ev.line.trim_end()).unwrap();
                        let py_line: Value = serde_json::from_str(got[0].as_str().unwrap().trim_end()).unwrap();
                        assert_eq!(py_line, line, "{raw:.40} {vars:?}");
                        assert_eq!(got[1], Value::String(ev.name), "{raw:.40}");
                        assert_eq!(got[2], ev.question.unwrap_or(Value::Null), "{raw:.40}");
                        assert_eq!(got[3], Value::String(ev.key), "both relays name the call alike: {raw:.40}");
                    }
                }
            }
        }
    }
}
