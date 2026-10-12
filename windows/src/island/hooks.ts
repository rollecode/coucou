// Hook events from Claude Code and every other agent → island state.
// Port of HookServer.processEvent / processPermissionRequest from the macOS app.
// Difference from macOS: no terminal filter. On Windows the hook fires from any
// terminal (Windows Terminal, VS Code, PowerShell…) and all of them are handled.
// The relay has already mapped every agent's events and fields onto Claude
// Code's (hook/src/normalize.rs), so one handler serves them all.

import { Bridge, onEvent } from "../core/bridge";
import { buildFileDiff, fileName, makeDiffStep, toOneLine } from "../core/diff";
import { AGENT_ERROR, AGENT_FINISHED, openForAlert } from "../core/alerts";
import { Sound } from "../core/sound";
import { State, type ApprovalInfo, type AskedQuestion } from "../core/state";
import { pillDefinition } from "../core/pills";
import { colorForProject } from "../core/layout";
import { APPROVAL_AGENTS, agentColor, agentName, validateAgent } from "./agents";
import type { Island } from "./island";
import { parseClaudePlan, restorePlanUsage } from "../core/plan";
import { setClaudePlanUsage, storedClaudePlanUsage } from "../views/usage";
import { N_, t } from "../i18n/i18n";
import { sshLabel } from "../core/ssh";
import { beginTurn, dropCode, endTurn, hasCode, toolFinished, toolStarted, type CodeSource } from "../views/session";

const CLAUDE_ID = "integration_claude";
const WORKSPACE_CLAUDE = "workspace_claude";

/** Claude Code's own pill: the terminal one when that is the main tool, else VS Code's. */
function claudeWorkspace(): string {
  return State.mainPillId === WORKSPACE_CLAUDE ? WORKSPACE_CLAUDE : CLAUDE_ID;
}
const CURSOR_ID = "agent_cursor";

/** How long a request can wait: the relay gives up at 110 s, counted from its arrival. */
const APPROVAL_TIMEOUT_MS = 110_000;

/**
 * A request nobody answered before the relay gave up: the card goes if it is
 * on screen, or it leaves the queue silently.
 */
function expireApproval(island: Island, requestId: string): void {
  if (State.pendingApproval?.requestId === requestId) dropPendingCard(island);
  else State.unqueueApprovals((a) => a.requestId === requestId);
}

/** Takes the approval or question card down and gives the island back, or shows the next request. */
export function dropPendingCard(island: Island): void {
  if (!State.pendingApproval) return;
  State.endApproval();
  const next = State.pendingApproval as ApprovalInfo | null;
  if (next) {
    island.alert(next.questions ? "question" : "approval");
    State.notify();
    return;
  }
  island.dropPin();
  if (State.view === "approval" || State.view === "question") {
    island.setView(State.defaultView());
  }
  State.notify();
}

/** The return to idle that Stop arms, per pill, so the next turn can cancel it. */
const stopTimers = new Map<string, number>();

/**
 * Claude Code sessions running at once: the first keeps the workspace pill,
 * every other one gets a pill of its own, by session ID, until it ends.
 */
const sessionPills = new Map<string, string>();
const SESSION_PILL = "session_";
/** When each session was last heard from (performance.now()). */
const lastHeard = new Map<string, number>();
/** A session silent this long, and idle, was closed without saying so. */
const SESSION_STALE_MS = 30 * 60 * 1000;

function isSessionPill(id: string): boolean {
  return id.startsWith(SESSION_PILL);
}

function stale(sessionId: string, pillId: string, now: number): boolean {
  if (hasProcess(sessionId)) return false;
  const heard = lastHeard.get(sessionId) ?? 0;
  const t = State.tasks.find((x) => x.id === pillId);
  return now - heard > SESSION_STALE_MS && (!t || t.state === "idle");
}

/** Drops the pills of sessions that went away without a SessionEnd. */
function pruneSessions(now: number) {
  for (const [sessionId, pillId] of sessionPills) {
    if (!stale(sessionId, pillId, now)) continue;
    sessionPills.delete(sessionId);
    lastHeard.delete(sessionId);
    State.removeTask(pillId);
  }
  // Sessions that only ever held the workspace pill are forgotten the same way.
  for (const [sessionId, heard] of lastHeard) {
    if (sessionPills.has(sessionId) || now - heard <= SESSION_STALE_MS) continue;
    if (State.tasks.some((t) => t.sessionId === sessionId && t.state !== "idle")) continue;
    lastHeard.delete(sessionId);
  }
}

const UUID = /^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$/;
const PILL_ID_PLAIN = 19;

/** FNV-1a, 64 bits, as 16 hex digits. */
function fnv1a64(text: string): string {
  let h = 0xcbf29ce484222325n;
  for (const b of new TextEncoder().encode(text)) {
    h = ((h ^ BigInt(b)) * 0x100000001b3n) & 0xffffffffffffffffn;
  }
  return h.toString(16).padStart(16, "0");
}

/**
 * A session's own pill ID, never shared with another session: a UUID as it is,
 * anything else its first characters and a hash of the whole ID.
 */
function sessionPillId(sessionId: string): string {
  const taken = new Set(sessionPills.values());
  if (UUID.test(sessionId) && !taken.has(SESSION_PILL + sessionId)) return SESSION_PILL + sessionId;
  const plain = sessionId.replace(/[^A-Za-z0-9-]/g, "").slice(0, PILL_ID_PLAIN);
  for (let salt = 0; ; salt++) {
    const id = `${SESSION_PILL}${plain}-${fnv1a64(salt ? `${salt}:${sessionId}` : sessionId)}`;
    if (!taken.has(id)) return id;
  }
}

/** The pill a Claude Code session's events go to. */
function claudePill(workspaceId: string, sessionId: string): string {
  if (!sessionId) return workspaceId;
  const now = performance.now();
  pruneSessions(now);
  lastHeard.set(sessionId, now);
  const own = sessionPills.get(sessionId);
  if (own) return own;
  const owner = State.tasks.find((t) => t.id === workspaceId)?.sessionId;
  if (!owner || owner === sessionId || stale(owner, workspaceId, now)) return workspaceId;
  const pillId = sessionPillId(sessionId);
  sessionPills.set(sessionId, pillId);
  return pillId;
}

function endSession(sessionId: string) {
  sessionPills.delete(sessionId);
  lastHeard.delete(sessionId);
  for (const [key, id] of pidSessions) {
    if (id === sessionId) pidSessions.delete(key);
  }
}

/** Forgets every session, as a fresh island would (the tests start from one). */
export function forgetSessions() {
  sessionPills.clear();
  lastHeard.clear();
  pidSessions.clear();
}

// ── Sessions found running ───────────────────────────────────────────────────
//
// A session is only heard of at its next event, and an idle one has none to
// send: after a launch its pill would be missing until it did something. So
// Rust lists the `claude` processes running, each gets its pill at once under
// the key `pid-<pid>`, and the first event names the session the pill was for.

/** `pid-<pid>` → the session key its pill is under (the same key until adopted). */
const pidSessions = new Map<string, string>();

const pidKey = (pid: number) => `pid-${pid}`;

/** A pill made from a process never goes stale: the next scan says if it is gone. */
function hasProcess(sessionId: string): boolean {
  if (sessionId.startsWith("pid-")) return true;
  for (const id of pidSessions.values()) {
    if (id === sessionId) return true;
  }
  return false;
}

/** The session's first event: its pill was made for its process under `pid-<pid>`. */
function adoptProcess(pid: number, sessionId: string) {
  const key = pidKey(pid);
  if (!sessionId || sessionId === key || sessionPills.has(sessionId)) return;
  if (pidSessions.get(key) !== key) {
    const owner = State.tasks.find((t) => t.sessionId === sessionId);
    if (owner) pidSessions.set(key, sessionId);
    return;
  }
  const own = sessionPills.get(key);
  if (own) {
    sessionPills.set(sessionId, own);
    sessionPills.delete(key);
  }
  const heard = lastHeard.get(key);
  if (heard != null) lastHeard.set(sessionId, heard);
  lastHeard.delete(key);
  const t = State.tasks.find((x) => x.sessionId === key);
  if (t) t.sessionId = sessionId;
  pidSessions.set(key, sessionId);
}

/** Makes the pills of the sessions running, and drops those whose process is gone. */
export async function discoverSessions() {
  const list = (await Bridge.claudeSessions()) ?? [];
  const alive = new Set(list.map((s) => pidKey(s.pid)));
  for (const [key, sessionId] of [...pidSessions]) {
    if (alive.has(key)) continue;
    const pillId = sessionPills.get(sessionId) ?? State.tasks.find((t) => t.sessionId === sessionId)?.id;
    endSession(sessionId);
    if (!pillId) continue;
    cancelStopTimer(pillId);
    State.clearSessionDiffs(pillId);
    if (isSessionPill(pillId)) State.removeTask(pillId);
    else clearSession(pillId);
  }
  for (const { pid, cwd, name } of list) {
    const key = pidKey(pid);
    if (pidSessions.has(key)) continue;
    pidSessions.set(key, key);
    const projectName = aliasProjectName(name || lastPathComponent(cwd) || "Session");
    const pillId = claudePill(claudeWorkspace(), key);
    if (isSessionPill(pillId)) State.upsertExternalAgent(pillId, projectName, colorForProject(projectName));
    else upsert(pillId, projectName, cwd, key);
    const t = State.tasks.find((x) => x.id === pillId);
    if (t) {
      t.sessionCwd = cwd;
      t.sessionId = key;
    }
  }
  State.notify();
}

/**
 * Settings → Follow the latest activity: the pill that just did something takes the card, and
 * with "Open the island on new activity" the island opens on it. A view you opened stays, and
 * an approval or a question keeps the front. Same rule as the Mac.
 */
function followActivity(island: Island, id: string) {
  const s = State.settings;
  if (!s.followActivity || State.paused || State.pendingApproval) return;
  if (!State.tasks.some((t) => t.id === id)) return;
  if (State.mode === "expanded" && State.view !== "overview") return;
  if (State.focusId !== id) State.setFocus(id);
  if (s.expandOnActivity && State.mode !== "expanded" && !State.inCall) island.alert("overview");
}

function cancelStopTimer(id: string): boolean {
  const timer = stopTimers.get(id);
  if (timer == null) return false;
  window.clearTimeout(timer);
  stopTimers.delete(id);
  return true;
}

/**
 * Turns that ended (Stop) with background work still running, by session: the
 * pill stays working, and events from inside its subagents (they carry
 * agent_id) leave it alone until the main agent speaks again.
 */
const backgroundWaits = new Map<string, { pillId: string; subagents: number; others: number }>();
/** How long a pill whose background work all ended waits for the main agent. */
const BACKGROUND_DONE_GRACE_MS = 60_000;

/** "⏳ In background: 3", the ticker line while a turn waits on background work. */
const backgroundStep = (count: number) => `⏳ ${t("In background: {count}", { count: String(count) })}`;

/** A subagent of a waiting turn finished; with none left, the pill idles if the main agent never resumes. */
function subagentDone(sessionId: string) {
  const wait = backgroundWaits.get(sessionId);
  if (!wait || wait.subagents === 0) return;
  wait.subagents -= 1;
  const left = wait.subagents + wait.others;
  if (left > 0) {
    State.appendStep(wait.pillId, backgroundStep(left));
    return;
  }
  window.setTimeout(() => {
    if (backgroundWaits.get(sessionId) !== wait) return;
    backgroundWaits.delete(sessionId);
    State.updateTask(wait.pillId, "idle");
  }, BACKGROUND_DONE_GRACE_MS);
}

/** Events after which a pending permission request of the same session is moot. */
const TURN_OVER = new Set(["Stop", "StopFailure", "UserPromptSubmit", "SessionEnd", "Interrupt"]);

interface HookPayload {
  hook_event_name?: string;
  request_id?: string;
  session_id?: string;
  cwd?: string;
  /** The session's own name (Claude Code's /rename) and the home folder, from the hook. */
  session_name?: string;
  home?: string;
  message?: string;
  /** UserPromptSubmit carries `prompt`; `message` belongs to Notification/Stop. */
  prompt?: string;
  /** Stop: the turn's final answer (Markdown) — Claude Code's, Hermes's, Codex's. */
  last_assistant_message?: string;
  tool_name?: string;
  tool_input?: Record<string, unknown>;
  /** Set by the relay when an edit was too big to forward whole (> 256 KB). */
  coucou_diff_truncated?: boolean;
  /** Optional agent tag: lowercase, digits and hyphens, ≤ 24 chars. */
  coucou_agent?: string;
  /** Hermes: where the session runs (telegram, discord…; "cli" in a terminal). */
  platform?: string;
  /** The SSH host the session runs on, set by the app for a remote host's events. */
  coucou_remote?: string;
  /** Set by the relay for Coucou's own chat (Claude Code as chat provider). */
  coucou_chat?: boolean;
  /** Set on every event fired inside a subagent. */
  agent_id?: string;
  /** The subagent's type; empty for Claude Code's internal agents (prompt suggestions, /btw). */
  agent_type?: string;
  /** Stop: the subagents, shells and monitors still running when the turn ended. */
  background_tasks?: { type?: string }[];
  /** Notification: permission_prompt, elicitation_dialog, idle_prompt… */
  notification_type?: string;
  /** "cursor" when Claude Code runs in Cursor's terminal (set by the relay). */
  term_editor?: string;
  /** StatusLine (the plan usage relay): Claude Code's 5-hour and weekly limits. */
  rate_limits?: unknown;
  /** The session's own `claude` process, set by Rust (pipe.rs). */
  coucou_claude_pid?: number;
  /** PostToolUse of Bash / PowerShell: the last lines it printed (the relay keeps nothing else of its output). */
  tool_tail?: unknown;
  /** PostToolUseFailure: why the tool failed. */
  error?: string;
}

const PROJECT_ALIASES: Record<string, string> = {
  "notch-buddy": "Notch Buddy",
  notchbuddy: "Notch Buddy",
  notch_buddy: "Notch Buddy",
};

function aliasProjectName(name: string): string {
  return PROJECT_ALIASES[name.toLowerCase()] ?? name;
}

function lastPathComponent(p: string): string {
  const cleaned = p.replace(/[\\/]+$/, "");
  const idx = Math.max(cleaned.lastIndexOf("\\"), cleaned.lastIndexOf("/"));
  return idx >= 0 ? cleaned.slice(idx + 1) : cleaned;
}

/**
 * localizedStep() — same labels as the macOS app, in English and shown in the
 * interface language (src/i18n) when the step is recorded.
 */
const TOOL_LABELS: Record<string, string> = {
  Bash: N_("Runs"),
  Read: N_("Reads"),
  Write: N_("Writes"),
  Edit: N_("Edits"),
  Glob: N_("Searches"),
  Grep: N_("Searches"),
  WebSearch: N_("Searches the web"),
  WebFetch: N_("Fetches"),
  TodoWrite: N_("Tasks"),
  Task: N_("Agent"),
  LS: N_("Lists"),
  MultiEdit: N_("Edits"),
  NotebookEdit: N_("Notebook"),
  PowerShell: N_("Runs"),
  // Antigravity's tools (#298).
  run_command: N_("Runs"),
  view_file: N_("Reads"),
  write_to_file: N_("Writes"),
  replace_file_content: N_("Edits"),
  read_url_content: N_("Fetches"),
  search_web: N_("Searches the web"),
};

function stepLabel(tool: string, input: Record<string, unknown>): string {
  const label = TOOL_LABELS[tool] ? t(TOOL_LABELS[tool]) : tool;
  const str = (k: string) => (typeof input[k] === "string" ? (input[k] as string) : null);
  const cmd = str("command");
  if (cmd) return `${label} · ${cmd.slice(0, 40)}`;
  const path = str("path");
  if (path) return `${label} · ${lastPathComponent(path)}`;
  const file = str("file_path");
  if (file) return `${label} · ${lastPathComponent(file)}`;
  const query = str("query");
  if (query) return `${label} · ${query.slice(0, 40)}`;
  return label;
}

/**
 * What the Allow button actually authorises. Approving "Write" tells you nothing
 * — approving `Write · C:\…\.env` tells you everything, and the difference is
 * the whole point of approving from the island rather than blind.
 *
 * Ordered by how specific the field is, so an unfamiliar tool still shows
 * whatever identifying string it carries instead of falling back to its name.
 */
const APPROVAL_FIELDS = [
  "command", // Bash, PowerShell
  "file_path", // Write, Edit, MultiEdit, NotebookEdit
  "path", // Read, LS
  "url", // WebFetch
  "query", // WebSearch
  "pattern", // Glob, Grep
  "prompt", // Task
] as const;

function approvalTarget(tool: string, input: Record<string, unknown>): string {
  for (const field of APPROVAL_FIELDS) {
    const value = input[field];
    if (typeof value === "string" && value.trim()) {
      return `${tool} · ${value.trim()}`;
    }
  }
  return tool;
}

/**
 * The questions of an AskUserQuestion call, if the island can show all of them
 * as options to pick from. Anything it cannot is left to the terminal.
 */
function askedQuestions(tool: string, input: Record<string, unknown>): AskedQuestion[] | null {
  if (tool !== "AskUserQuestion" || !Array.isArray(input.questions)) return null;
  const out: AskedQuestion[] = [];
  for (const raw of input.questions as Record<string, unknown>[]) {
    const question = typeof raw?.question === "string" ? raw.question : "";
    const options = (Array.isArray(raw?.options) ? (raw.options as Record<string, unknown>[]) : [])
      .filter((o) => typeof o?.label === "string" && o.label)
      .map((o) => ({
        label: o.label as string,
        description: typeof o.description === "string" ? o.description : "",
      }));
    // A question cut short by the relay would be answered under the wrong text.
    if (!question || question.endsWith("…") || options.length < 2) return null;
    out.push({ question, options, multiSelect: raw.multiSelect === true });
  }
  return out.length > 0 ? out : null;
}

/** The Claude Code session's pill, named after its project for the session. */
function upsert(id: string, projectName: string, cwd: string, sessionId: string) {
  const t = State.upsertWorkspacePill(id, projectName, cwd);
  if (t && sessionId) t.sessionId = sessionId;
}

/** The session is over: the pill goes back as it was, or away if it was only there for it. */
function clearSession(id: string) {
  const t = State.tasks.find((x) => x.id === id);
  if (!t) return;
  if (!State.isKept(id)) {
    State.removeTask(id);
    return;
  }
  t.state = "idle";
  t.steps = [];
  t.stepIndex = 0;
  delete t.stepSeq;
  t.name = pillDefinition(id)?.name ?? t.name;
  t.pillBadge = null;
  t.sessionId = null;
  t.finalLine = null;
}

/** The final message stays on the card until the next turn starts. */
function clearFinalLine(id: string) {
  const t = State.tasks.find((x) => x.id === id);
  if (t) t.finalLine = null;
}

/**
 * PostToolUse of Edit / MultiEdit / Write → a diff stored for the pill and a
 * ticker step with its +N −M counts. Nothing is kept for a pill that does not
 * exist, so a stray event cannot grow memory. An edit the relay had to cut
 * would give wrong counts: the PreToolUse step ("Edits · file") stands alone.
 */
function recordDiff(agentId: string, payload: HookPayload): number | null {
  if (payload.coucou_diff_truncated) return null;
  if (!State.tasks.some((t) => t.id === agentId)) return null;
  const diff = buildFileDiff(payload.tool_name ?? "", payload.tool_input ?? {});
  if (!diff) return null;
  if (payload.coucou_remote) diff.remote = true;
  const id = State.appendSessionDiff(agentId, diff);
  State.appendStep(agentId, makeDiffStep(fileName(diff.path), diff.added, diff.removed, id));
  return id;
}

/**
 * Settings → Follow edits live: the edit just made opens on its diff. Never over
 * a card, the chat or anything else open, nor while you type, nor over another
 * session's live diff; a pill whose live diff you closed waits for its next prompt.
 */
function followEdit(island: Island, pillId: string, diffId: number) {
  if (!State.settings.followEdits || State.liveStopped.has(pillId) || State.pendingApproval) return;
  const expanded = State.mode === "expanded";
  const typing = /^(INPUT|TEXTAREA)$/.test(globalThis.document?.activeElement?.tagName ?? "");
  // The code view this session is followed in counts as the overview.
  const onOverview = State.view === "overview" || (State.view === "code" && State.liveDiff?.pillId === pillId);
  if (expanded && (!onOverview || typing)) return;
  if (expanded && State.liveDiff && State.liveDiff.pillId !== pillId) return;
  State.liveDiff = { pillId, diffId };
  State.setFocus(pillId);
  // Its code view when the session has one; the diff card otherwise (remote hosts, other agents).
  const view = hasCode(pillId) ? "code" : "overview";
  if (expanded) island.setView(view);
  else if (!State.inCall) island.alert(view);
}

/** The session's turn is over or starts anew: it stops being followed, or may be again. */
function endFollow(pillId: string) {
  if (State.liveDiff?.pillId === pillId) State.liveDiff = null;
  State.liveStopped.delete(pillId);
}

export function registerHookHandlers(island: Island) {
  // The last plan numbers seen survive a restart, as on the Mac.
  State.planUsage ??= restorePlanUsage(storedClaudePlanUsage());
  void onEvent<HookPayload>("hook", (payload) => handleHook(island, payload));
  void discoverSessions();
}

function handleHook(island: Island, payload: HookPayload) {
  // Account-wide numbers from the status line relay, not part of any session:
  // keep the latest, nothing else (no reveal, no sound), paused or not.
  if (payload.hook_event_name === "StatusLine") {
    const usage = parseClaudePlan(payload.rate_limits);
    if (usage) setClaudePlanUsage(usage);
    return;
  }

  // A waiting relay hung up: its request was answered elsewhere.
  if (payload.hook_event_name === "Released") {
    const released = (a: ApprovalInfo) => a.requestId === payload.request_id;
    State.unqueueApprovals(released);
    if (State.pendingApproval && released(State.pendingApproval)) dropPendingCard(island);
    return;
  }

  if (State.paused) {
    // Silence here used to cost Claude Code nearly two minutes: the relay waited
    // for a decision from an island that had already decided not to look. Say so,
    // and the terminal takes the question immediately.
    if (payload.request_id) void Bridge.approvalDecline(payload.request_id);
    return;
  }

  const name = payload.hook_event_name ?? "";
  // A remote session's folder is on its host: named after it, never opened here.
  const remote = payload.coucou_remote ?? "";
  const cwd = remote ? "" : payload.cwd ?? "";
  const folder = payload.cwd && payload.cwd === payload.home ? "~" : lastPathComponent(payload.cwd ?? "");
  const own = payload.session_name || aliasProjectName(folder || "Session");
  const projectName = remote ? `${own} · ${sshLabel(remote)}` : own;

  // Route to the right pill. Valid coucou_agent → dynamic "agent_<name>" pill.
  // "claude" is reserved; absent or invalid → Claude Code's own pill: Cursor's
  // when it runs in Cursor's terminal (Mac #120), VS Code's otherwise.
  const validAgent = validateAgent(payload.coucou_agent);
  const workspaceId = payload.term_editor === "cursor" ? CURSOR_ID : claudeWorkspace();
  const sessionId = payload.session_id ?? "";
  if (!validAgent && typeof payload.coucou_claude_pid === "number") adoptProcess(payload.coucou_claude_pid, sessionId);
  const agentId = validAgent ? `agent_${validAgent}` : claudePill(workspaceId, sessionId);
  const isExternalAgent = validAgent !== null;
  const ownSession = isSessionPill(agentId);

  /** Alerts force the island open; work events only reveal the compact island. */
  const surface = (view: Parameters<Island["alert"]>[0], isAlert: boolean) => {
    if (State.inCall) return;
    if (State.mode === "expanded") {
      if (isAlert) island.setView(view);
    } else if (isAlert) {
      island.alert(view);
    } else if (State.mode === "hidden") {
      island.reveal();
    }
  };

  /** Ensure the agent pill exists (no-op for Claude Code). */
  const ensurePill = () => {
    if (isExternalAgent || ownSession) {
      if (ownSession) {
        State.upsertExternalAgent(agentId, projectName, colorForProject(projectName));
        const pill = State.tasks.find((t) => t.id === agentId);
        if (pill) pill.name = projectName;
      }
      else State.upsertExternalAgent(agentId, agentName(validAgent!), agentColor(validAgent!));
      const t = State.tasks.find((x) => x.id === agentId);
      if (t && cwd) t.sessionCwd = cwd;
      if (t && sessionId) t.sessionId = sessionId;
    } else {
      upsert(agentId, projectName, cwd, sessionId);
    }
  };

  /**
   * Called where a handler is about to replace `finished` with a newer state:
   * the timer Stop armed would otherwise put the pill back to idle over it. The
   * badge that timer was going to clear goes now.
   */
  const supersedeStop = () => {
    if (cancelStopTimer(agentId)) State.setPillBadge(agentId, null);
  };

  // The turn that asked for a permission is over — answered in the terminal,
  // interrupted, or a new prompt — so the card would be lying. It goes, and the
  // relay is released without a decision. Same rule as the Mac.
  const pending = State.pendingApproval;
  const ownTurn = (a: ApprovalInfo) => a.pillId === agentId && a.sessionId === (payload.session_id ?? "");
  if (TURN_OVER.has(name)) {
    for (const q of State.unqueueApprovals(ownTurn)) {
      if (q.requestId) void Bridge.approvalDecline(q.requestId);
    }
  }
  // The tool ran: answered elsewhere (another Coucou, the terminal).
  const toolDone = (name === "PostToolUse" || name === "PostToolUseFailure") && pending?.tool === payload.tool_name;
  if (pending && (TURN_OVER.has(name) || toolDone) && ownTurn(pending)) {
    if (pending.requestId) void Bridge.approvalDecline(pending.requestId);
    dropPendingCard(island);
  }

  // Coucou's own chat runs Claude Code (claude_code.rs): it is no session to
  // show, but a tool it wants to use still needs a click on the card.
  const fromChat = payload.coucou_chat === true;
  if (fromChat && name !== "PermissionRequest") {
    State.notify();
    return;
  }

  // Read after the card above is dropped: its pill may have handed the front
  // back to the pill you were on.
  if (name !== "SessionEnd") followActivity(island, agentId);
  const focused = State.focusId === agentId;

  // A turn waiting on background work: its subagents' events must not replace its
  // state or final line. Their permission requests still get a card.
  const fromSubagent = Boolean(payload.agent_id);
  if (fromSubagent && backgroundWaits.has(sessionId) && name !== "Notification" && name !== "PermissionRequest") {
    if (name === "SubagentStop" && payload.agent_type) subagentDone(sessionId);
    return;
  }
  if (!fromSubagent && name !== "Notification") backgroundWaits.delete(sessionId);
  // The code view's data: Claude Code's own pills only. Opening it is the user's click.
  const code: CodeSource | null = isExternalAgent
    ? null
    : { sessionId, cwd: payload.cwd ?? "", project: projectName, remote: !!payload.coucou_remote };
  /** Leaves the code view when what it shows of this pill is gone. */
  const leaveCode = () => {
    if (focused && State.view === "code") island.setView(State.defaultView());
  };

  switch (name) {
    case "SessionStart":
      ensurePill();
      clearFinalLine(agentId);
      // Hermes through its gateway says where the session comes from.
      if (validAgent === "hermes" && payload.platform && payload.platform !== "cli") {
        State.appendStep(agentId, payload.platform.charAt(0).toUpperCase() + payload.platform.slice(1));
      }
      surface("overview", false);
      Sound.play("work");
      void discoverSessions();
      break;

    case "UserPromptSubmit": {
      ensurePill();
      supersedeStop();
      clearFinalLine(agentId);
      State.updateTask(agentId, "thinking");
      endFollow(agentId);
      // The field is `prompt`; reading `message` meant this step was always blank.
      const asked = payload.prompt ?? payload.message;
      if (asked) State.appendStep(agentId, asked.slice(0, 60));
      if (code) {
        beginTurn(agentId, code);
        leaveCode();
      }
      surface("overview", false);
      break;
    }

    case "PreToolUse": {
      ensurePill();
      supersedeStop();
      clearFinalLine(agentId);
      State.updateTask(agentId, "working");
      const tool = payload.tool_name ?? "Tool";
      State.appendStep(agentId, stepLabel(tool, payload.tool_input ?? {}));
      if (code) toolStarted(agentId, code, tool, payload.tool_input ?? {});
      surface("overview", false);
      break;
    }

    case "PostToolUse":
      supersedeStop();
      // The question was answered in the terminal: the card would be lying.
      if (payload.tool_name === "AskUserQuestion") {
        const answered = (a: ApprovalInfo) => Boolean(a.questions) && a.sessionId === (payload.session_id ?? "");
        State.unqueueApprovals(answered);
        if (State.pendingApproval && answered(State.pendingApproval)) dropPendingCard(island);
      }
      State.updateTask(agentId, "working");
      // The code view's data first, so following can open it on this very edit.
      if (code) {
        const tail = Array.isArray(payload.tool_tail) ? payload.tool_tail : undefined;
        toolFinished(agentId, payload.cwd ?? "", payload.tool_name ?? "", payload.tool_input ?? {}, { tail });
      }
      {
        const diffId = recordDiff(agentId, payload);
        if (diffId != null) followEdit(island, agentId, diffId);
      }
      break;

    case "PostToolUseFailure":
      supersedeStop();
      State.updateTask(agentId, "working");
      State.appendStep(agentId, t("⚠ failed"));
      if (code) {
        toolFinished(agentId, payload.cwd ?? "", payload.tool_name ?? "", payload.tool_input ?? {}, {
          failed: true,
          error: payload.error,
        });
      }
      break;

    case "Notification": {
      const message = payload.message ?? "";
      const lower = message.toLowerCase();
      if (lower.includes("rate limit") || lower.includes("limite d")) {
        supersedeStop();
        State.updateTask(agentId, "ratelimit");
        Sound.play("rate");
      } else if (payload.notification_type === "permission_prompt") {
        // No card for this request (another agent, or one the island declined):
        // the session is blocked on the user, not working.
        supersedeStop();
        State.updateTask(agentId, "approval");
        State.appendStep(agentId, message);
        Sound.play("approval");
      } else if (payload.notification_type === "elicitation_dialog" || message.endsWith("?")) {
        supersedeStop();
        State.updateTask(agentId, "question");
        State.appendStep(agentId, message);
      }
      break;
    }

    case "Stop": {
      if (State.liveDiff?.pillId === agentId) State.liveDiff = null;
      if (code) endTurn(agentId);
      // Claude Code puts the turn's answer in the Stop payload itself, so there is
      // no transcript to read (the relay does not even forward its path). Other
      // agents report their last words the same way (Hermes, Codex) or as
      // `message`.
      const finalText = toOneLine(payload.last_assistant_message ?? payload.message ?? "");
      if (finalText) {
        State.appendStep(agentId, finalText);
        const t = State.tasks.find((x) => x.id === agentId);
        if (t) t.finalLine = finalText;
      }
      // Background work still running: the session is paused, not done. Claude Code
      // wakes the main agent when it ends, and that turn's Stop finishes the pill.
      const inFlight = payload.background_tasks ?? [];
      if (inFlight.length > 0) {
        const subagents = inFlight.filter((task) => task?.type === "subagent").length;
        backgroundWaits.set(sessionId, { pillId: agentId, subagents, others: inFlight.length - subagents });
        supersedeStop();
        State.updateTask(agentId, "working");
        State.appendStep(agentId, backgroundStep(inFlight.length));
        break;
      }
      State.updateTask(agentId, "finished");
      Sound.play("finish");
      // A card waiting for an answer is never covered by another alert.
      if (focused && !State.pendingApproval) {
        surface("finished", true);
      } else if (!openForAlert(island, AGENT_FINISHED, agentId, "finished")) {
        State.setPillBadge(agentId, "finished");
      }
      cancelStopTimer(agentId);
      stopTimers.set(
        agentId,
        window.setTimeout(() => {
          stopTimers.delete(agentId);
          if (isExternalAgent) {
            State.removeTask(agentId);
          } else {
            State.updateTask(agentId, "idle");
            State.setPillBadge(agentId, null);
          }
        }, 5200),
      );
      break;
    }

    case "Interrupt":
      // Codex: the user stopped the turn. Back to idle, nothing to celebrate.
      supersedeStop();
      State.updateTask(agentId, "idle");
      State.setPillBadge(agentId, null);
      break;

    case "StopFailure":
      supersedeStop();
      State.updateTask(agentId, "error");
      Sound.play("error");
      if (focused && !State.pendingApproval) {
        surface("error", true);
      } else if (!openForAlert(island, AGENT_ERROR, agentId, "error")) {
        State.setPillBadge(agentId, "error");
      }
      break;

    case "SessionEnd":
      // Nothing left for the timer to do, and it must not outlive the session: a
      // pill recreated within 5.2 s would be removed by it.
      cancelStopTimer(agentId);
      endFollow(agentId);
      State.clearSessionDiffs(agentId);
      if (sessionId) endSession(sessionId);
      if (!isExternalAgent) void discoverSessions();
      if (code) {
        dropCode(agentId);
        leaveCode();
      }
      if (isExternalAgent || ownSession) {
        State.removeTask(agentId);
      } else {
        State.updateTask(agentId, "idle");
        clearSession(agentId);
      }
      break;

    case "SubagentStart":
      State.appendStep(agentId, t("+ subagent"));
      break;

    case "SubagentStop":
      State.appendStep(agentId, t("• subagent done"));
      break;

    case "PermissionRequest": {
      // On a call the island stays away: the terminal takes the question at once.
      if (State.inCall) {
        if (payload.request_id) void Bridge.approvalDecline(payload.request_id);
        break;
      }
      // Only Claude Code and the agents the relay can answer for (Codex, Copilot
      // CLI, Muse Code — same as the Mac) get a card. Anyone else's request is
      // declined at once, so the agent asks in its own terminal.
      if (isExternalAgent && !APPROVAL_AGENTS.has(validAgent!)) {
        if (payload.request_id) void Bridge.approvalDecline(payload.request_id);
        break;
      }

      const requestId = payload.request_id ?? "";
      const tool = payload.tool_name ?? "Tool";
      const input = payload.tool_input ?? {};
      // Claude Code asking a question is not a permission to grant: the island
      // shows the options and sends back the one that was picked. Only Claude
      // Code asks questions this way.
      const questions = isExternalAgent ? null : askedQuestions(tool, input);
      const view = questions ? "question" : "approval";
      const info: ApprovalInfo = {
        requestId,
        sessionId,
        pillId: agentId,
        tool,
        command: approvalTarget(tool, input),
        ...(questions ? { questions } : {}),
      };
      const expire = () => window.setTimeout(() => expireApproval(island, requestId), APPROVAL_TIMEOUT_MS);
      // A card is already up: this request waits its turn behind it, never
      // replacing it. Past five waiting, the newest goes back to its terminal.
      if (State.pendingApproval && State.pendingApproval.requestId !== requestId) {
        if (!requestId || !State.queueApproval(info)) {
          if (requestId) void Bridge.approvalDecline(requestId);
          break;
        }
        if (!fromChat) ensurePill();
        supersedeStop();
        void Bridge.approvalAck(requestId);
        State.updateTask(agentId, view);
        expire();
        break;
      }
      // The chat's card goes on the Claude Code pill as it is, never renamed.
      if (!fromChat) ensurePill();
      supersedeStop();
      // The card always comes up, even over another pill or an island that is
      // already open: its pill comes to the front, and the one you were on
      // comes back once you answer (Mac #117, #120).
      State.beginApproval(info);
      // The relay's short ack window closes in 800 ms; everything below this
      // line is synchronous, so the card really is up by the time it lands.
      if (requestId) void Bridge.approvalAck(requestId);
      State.updateTask(agentId, view);
      Sound.play(view);
      // Any agent's card (Claude Code, Codex, Copilot CLI, Muse Code) comes up
      // the same way: beginApproval brought its pill to the front.
      island.alert(view);
      // Coucou answers within 108 s or not at all; after that the terminal has
      // taken over and the card would be lying.
      expire();
      break;
    }

    default:
      break;
  }
  State.notify();
}
