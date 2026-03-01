import dotenv from "dotenv";
import express from "express";
import crypto from "node:crypto";
import path from "node:path";
import { fileURLToPath } from "node:url";
import connectPgSimple from "connect-pg-simple";
import bcrypt from "bcryptjs";
import session from "express-session";
import { Pool } from "pg";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const app = express();
const port = Number(process.env.PORT || 3000);
const baseUrl = process.env.APP_BASE_URL || `http://localhost:${port}`;
const IS_PRODUCTION = process.env.NODE_ENV === "production";
const ADMIN_CODE = String(process.env.VENMO_ADMIN_CODE || "").trim() || (IS_PRODUCTION ? "" : "Sage1557");
const DATABASE_URL = String(process.env.DATABASE_URL || "").trim();
const SESSION_SECRET = String(process.env.SESSION_SECRET || "").trim();
const RATE_LIMIT_ENABLED = String(process.env.RATE_LIMIT_ENABLED || "1").trim() !== "0";
const ALLOW_WEAK_ADMIN_CODE = String(process.env.ALLOW_WEAK_ADMIN_CODE || "0").trim() === "1";
const INITIAL_ACCOUNT_BALANCE = 1000;
const MAX_ACCOUNT_BALANCE = 10_000_000;
const MAX_ACCOUNT_SAVINGS = 10_000_000;
const MAX_ACCOUNT_AVG_COST = 1_000_000;
const MAX_ACCOUNT_SHARES = 2_000_000;
const BALANCE_SYNC_BURST_UP = 25_000;
const BALANCE_SYNC_UP_PER_MIN = 50_000;
const SHARES_SYNC_BURST_UP = 5_000;
const SHARES_SYNC_UP_PER_MIN = 10_000;
const MAX_SYNC_GUARD_BYPASS_MINUTES = 10080;

if (!DATABASE_URL) {
  throw new Error("Missing DATABASE_URL environment variable.");
}
if (!SESSION_SECRET) {
  throw new Error("Missing SESSION_SECRET environment variable.");
}
if (!ADMIN_CODE) {
  throw new Error("Missing VENMO_ADMIN_CODE environment variable.");
}
if (IS_PRODUCTION) {
  const normalizedAdminCode = ADMIN_CODE.toLowerCase();
  if (!ALLOW_WEAK_ADMIN_CODE && (normalizedAdminCode === "sage1557" || normalizedAdminCode.length < 8)) {
    throw new Error("Insecure VENMO_ADMIN_CODE. Use a unique production admin code (8+ chars).");
  }
}

const db = new Pool({
  connectionString: DATABASE_URL,
  ssl: DATABASE_URL.includes("localhost") ? false : { rejectUnauthorized: false }
});

const STORE_PACKAGES = Object.freeze({
  small: { usd: 3, funds: 1000, label: "Starter Funds Pack" },
  medium: { usd: 10, funds: 5000, label: "Trader Funds Pack" },
  xlarge: { usd: 20, funds: 10000, label: "Pro Funds Pack" },
  large: { usd: 40, funds: 25000, label: "Whale Funds Pack" }
});
const PgSessionStore = connectPgSimple(session);
const PACKAGE_ALIASES = Object.freeze({
  xl: "xlarge",
  "x-large": "xlarge",
  "extra-large": "xlarge",
  pro: "xlarge",
  whale: "large",
  starter: "small",
  trader: "medium"
});
const LEADERBOARD_STREAM_LIMIT = 12;
const BIG_WIN_ACTIVITY_MIN_AMOUNT = 500;
const MAX_ADMIN_ACTIVITY_EVENTS = 3000;
const MAX_SYSTEM_RUNTIME_EVENTS = 6000;
const MAX_BALANCE_AUDIT_EVENTS = 10000;
const MAX_ADMIN_BACKUPS = 15;
const MAX_ADMIN_POPUP_MESSAGES = 3000;
const MAX_ADMIN_POPUP_MESSAGE_READS = 300000;
const MAX_SITE_VISIT_EVENTS = 500000;
const SITE_VISIT_SERVER_COOLDOWN_MS = 30 * 60 * 1000;
const leaderboardStreamClients = new Set();
let leaderboardBroadcastTimer = null;

function normalizeTxn(value) {
  return String(value || "").trim().toLowerCase().replace(/\s+/g, "");
}

function sanitizePlayerId(value) {
  const id = String(value || "").trim();
  if (!id) return "";
  if (!/^[a-zA-Z0-9_-]{6,120}$/.test(id)) return "";
  return id;
}

function sanitizeUsername(value) {
  const normalized = String(value || "")
    .replace(/\s+/g, " ")
    .trim();
  if (!normalized) return "";
  if (normalized.length < 3 || normalized.length > 16) return "";
  return normalized;
}

function sanitizeVisitorId(value) {
  const visitorId = String(value || "").trim();
  if (!visitorId) return "";
  if (!/^[a-zA-Z0-9_-]{8,120}$/.test(visitorId)) return "";
  return visitorId;
}

function sanitizeVisitPath(value) {
  let rawPath = String(value || "").trim();
  if (!rawPath) return "/";
  if (/^https?:\/\//i.test(rawPath)) {
    try {
      rawPath = new URL(rawPath).pathname || "/";
    } catch {
      rawPath = "/";
    }
  }
  rawPath = rawPath.split("?")[0].split("#")[0].trim();
  if (!rawPath) return "/";
  if (!rawPath.startsWith("/")) rawPath = `/${rawPath}`;
  return rawPath.slice(0, 180);
}

function normalizeUsernameKey(value) {
  return sanitizeUsername(value).toLowerCase();
}

function sanitizeEmail(value) {
  const email = String(value || "")
    .replace(/\u200B/g, "")
    .replace(/\s+/g, "")
    .replace(/^mailto:/i, "")
    .replace(/^['"`<]+|['"`>]+$/g, "")
    .trim()
    .toLowerCase();
  if (!email) return "";
  const atIndex = email.indexOf("@");
  if (atIndex <= 0 || atIndex >= email.length - 1) return "";
  const domain = email.slice(atIndex + 1);
  if (domain.startsWith(".") || domain.endsWith(".")) return "";
  if (!domain.includes(".")) return "";
  return email;
}

function sanitizePassword(value) {
  const password = String(value || "");
  if (!password) return "";
  if (password.length < 8 || password.length > 72) return "";
  return password;
}

function hasOwnField(obj, key) {
  return Boolean(obj && typeof obj === "object" && Object.prototype.hasOwnProperty.call(obj, key));
}

function sanitizeSyncNumber(value, { max = Number.MAX_SAFE_INTEGER, decimals = 2 } = {}) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  if (parsed < 0 || parsed > max) return null;
  const factor = 10 ** Math.max(0, Math.floor(decimals));
  return Math.round(parsed * factor) / factor;
}

function sanitizeSyncShares(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  if (parsed < 0 || parsed > MAX_ACCOUNT_SHARES) return null;
  return Math.floor(parsed);
}

function getSyncAllowedIncrease(elapsedMs, burst, perMinute) {
  const elapsed = Math.max(0, Number(elapsedMs) || 0);
  return burst + (elapsed / 60000) * perMinute;
}

function sanitizeClaimSource(value) {
  const normalized = String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
  if (!normalized) return "venmo";
  return normalized.slice(0, 24);
}

function normalizePackageId(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return "";
  if (STORE_PACKAGES[raw]) return raw;
  const mapped = PACKAGE_ALIASES[raw];
  return mapped && STORE_PACKAGES[mapped] ? mapped : "";
}

function sanitizeGameLabel(value) {
  const normalized = String(value || "")
    .replace(/\s+/g, " ")
    .trim();
  if (!normalized) return "Casino";
  return normalized.slice(0, 24);
}

function sanitizeFeedbackCategory(value) {
  const normalized = String(value || "").trim().toLowerCase();
  if (normalized === "bug" || normalized === "idea" || normalized === "other") return normalized;
  return "idea";
}

function sanitizeFeedbackMessage(value) {
  const normalized = String(value || "")
    .replace(/\r/g, "")
    .replace(/\t/g, " ")
    .replace(/\u200B/g, "")
    .replace(/[ ]{2,}/g, " ")
    .trim();
  if (!normalized) return "";
  return normalized.slice(0, 500);
}

function sanitizeAdminPopupMessage(value) {
  const normalized = String(value || "")
    .replace(/\r/g, "")
    .replace(/\t/g, " ")
    .replace(/\u200B/g, "")
    .replace(/[ ]{2,}/g, " ")
    .trim();
  if (!normalized) return "";
  return normalized.slice(0, 260);
}

const PROFANITY_BLOCKLIST = Object.freeze([
  "fuck",
  "fucking",
  "motherfucker",
  "fucker",
  "bitch",
  "bitches",
  "shit",
  "bullshit",
  "asshole",
  "bastard",
  "dick",
  "dildo",
  "cunt",
  "pussy",
  "slut",
  "whore",
  "porn",
  "nigger",
  "nigga",
  "retard",
  "faggot",
  "kike",
  "spic"
]);

function normalizeProfanityText(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[@4]/g, "a")
    .replace(/[!1|]/g, "i")
    .replace(/[0]/g, "o")
    .replace(/[3]/g, "e")
    .replace(/[5$]/g, "s")
    .replace(/[7]/g, "t")
    .replace(/[^a-z]/g, "");
}

function hasProfanity(value) {
  const compact = normalizeProfanityText(value);
  if (!compact) return false;
  return PROFANITY_BLOCKLIST.some((term) => compact.includes(normalizeProfanityText(term)));
}

function claimWithPack(claim) {
  const pack = STORE_PACKAGES[claim.packId] || STORE_PACKAGES[claim.pack_id] || null;
  return {
    ...claim,
    id: String(claim.id),
    funds: pack?.funds || 0,
    usd: pack?.usd || 0
  };
}

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function mapClaimRow(row) {
  return claimWithPack({
    id: String(row.id),
    playerId: String(row.player_id || ""),
    username: String(row.username || ""),
    email: sanitizeEmail(row.email) || "",
    source: String(row.source || "venmo"),
    packId: String(row.pack_id || ""),
    txnId: String(row.txn_id || ""),
    txnNorm: String(row.txn_norm || ""),
    status: String(row.status || "pending"),
    submittedAt: Number(row.submitted_at) || 0,
    reviewedAt: Number(row.reviewed_at) || 0,
    creditedAt: Number(row.credited_at) || 0
  });
}

function mapWinRow(row) {
  const multiplierValue = row.multiplier === null || row.multiplier === undefined ? null : Number(row.multiplier);
  return {
    id: String(row.id),
    playerId: String(row.player_id || ""),
    username: String(row.username || ""),
    vip: row.vip === true,
    game: String(row.game || "Casino"),
    amount: Math.round(toNumber(row.amount) * 100) / 100,
    multiplier: Number.isFinite(multiplierValue) ? Math.round(multiplierValue * 100) / 100 : null,
    submittedAt: Number(row.submitted_at) || 0
  };
}

function mapUserRow(row) {
  return {
    playerId: String(row.player_id || ""),
    username: sanitizeUsername(row.username) || "Unknown",
    email: sanitizeEmail(row.email) || "",
    usernameKey: String(row.username_key || ""),
    balance: Math.round(toNumber(row.balance) * 100) / 100,
    shares: Math.max(0, Math.floor(toNumber(row.shares))),
    avgCost: Math.round(toNumber(row.avg_cost) * 10000) / 10000,
    savingsBalance: Math.round(toNumber(row.savings_balance) * 100) / 100,
    autoSavingsPercent: Math.round(toNumber(row.auto_savings_percent) * 1000) / 1000,
    balanceUpdatedAt: Number(row.balance_updated_at) || 0,
    lastSeenAt: Number(row.last_seen_at) || 0,
    isAdmin: row.is_admin === true
  };
}

function mapLeaderboardRow(row) {
  return {
    playerId: String(row.player_id || ""),
    username: sanitizeUsername(row.username) || "Unknown",
    balance: Math.round(toNumber(row.balance) * 100) / 100,
    lastSeenAt: Number(row.last_seen_at) || 0,
    rank: Number(row.rank) || 0
  };
}

function mapFeedbackRow(row) {
  return {
    id: String(row.id || ""),
    playerId: String(row.player_id || ""),
    username: sanitizeUsername(row.username) || "Unknown",
    category: sanitizeFeedbackCategory(row.category),
    message: String(row.message || ""),
    status: String(row.status || "open"),
    submittedAt: Number(row.submitted_at) || 0,
    reviewedAt: Number(row.reviewed_at) || 0
  };
}

function mapAdminPopupMessageRow(row) {
  return {
    id: String(row.id || ""),
    targetPlayerId: String(row.target_player_id || ""),
    targetUsername: sanitizeUsername(row.target_username) || "",
    message: sanitizeAdminPopupMessage(row.message),
    createdBy: sanitizePlayerId(row.created_by),
    createdAt: Number(row.created_at) || 0,
    active: row.active !== false,
    readCount: Number(row.read_count) || 0
  };
}

async function fetchUnreadAdminPopupsForPlayer(playerId, { limit = 3 } = {}) {
  const safePlayerId = sanitizePlayerId(playerId);
  if (!safePlayerId) return [];
  const safeLimit = Number.isFinite(Number(limit))
    ? Math.max(1, Math.min(10, Math.floor(Number(limit))))
    : 3;
  const result = await db.query(
    `
      SELECT m.id, m.target_player_id, m.target_username, m.message, m.created_by, m.created_at, m.active
      FROM admin_popup_messages m
      LEFT JOIN admin_popup_message_reads r
        ON r.message_id = m.id
       AND r.player_id = $1
      WHERE m.active = true
        AND (m.target_player_id = '' OR m.target_player_id = $1)
        AND r.message_id IS NULL
      ORDER BY m.created_at DESC, m.id DESC
      LIMIT $2
    `,
    [safePlayerId, safeLimit]
  );
  return result.rows.map(mapAdminPopupMessageRow);
}

async function fetchLeaderboardPlayers({ limit = LEADERBOARD_STREAM_LIMIT, playerId = "", username = "" } = {}) {
  const safeLimit = Number.isFinite(Number(limit))
    ? Math.max(3, Math.min(50, Math.floor(Number(limit))))
    : LEADERBOARD_STREAM_LIMIT;
  const safePlayerId = sanitizePlayerId(playerId);
  const safeUsernameKey = normalizeUsernameKey(username);
  const result = await db.query(
    `
      WITH ranked AS (
        SELECT
          player_id,
          username,
          username_key,
          balance,
          last_seen_at,
          ROW_NUMBER() OVER (ORDER BY balance DESC, last_seen_at DESC, player_id ASC) AS rank
        FROM users
        WHERE COALESCE(banned_at, 0) = 0
      )
      SELECT player_id, username, balance, last_seen_at, rank
      FROM ranked
      WHERE rank <= $1
      ORDER BY rank ASC
    `,
    [safeLimit]
  );
  const players = result.rows.map(mapLeaderboardRow);

  let you = null;
  if (safePlayerId || safeUsernameKey) {
    const youResult = await db.query(
      `
        WITH ranked AS (
          SELECT
            player_id,
            username,
            username_key,
            balance,
            last_seen_at,
            ROW_NUMBER() OVER (ORDER BY balance DESC, last_seen_at DESC, player_id ASC) AS rank
          FROM users
          WHERE COALESCE(banned_at, 0) = 0
        )
        SELECT player_id, username, balance, last_seen_at, rank
        FROM ranked
        WHERE ($1 <> '' AND player_id = $1)
           OR ($2 <> '' AND username_key = $2)
        ORDER BY CASE WHEN player_id = $1 THEN 0 ELSE 1 END, rank ASC
        LIMIT 1
      `,
      [safePlayerId, safeUsernameKey]
    );
    you = youResult.rowCount ? mapLeaderboardRow(youResult.rows[0]) : null;
  }

  return { players, you };
}

function removeLeaderboardStreamClient(client) {
  if (!client) return;
  leaderboardStreamClients.delete(client);
  if (client.heartbeat) {
    clearInterval(client.heartbeat);
    client.heartbeat = null;
  }
}

function broadcastLeaderboardPlayers(players, reason = "update") {
  if (!leaderboardStreamClients.size) return;
  const payload = JSON.stringify({
    ok: true,
    reason: String(reason || "update"),
    ts: Date.now(),
    players: Array.isArray(players) ? players : []
  });
  for (const client of [...leaderboardStreamClients]) {
    try {
      client.res.write(`event: leaderboard\ndata: ${payload}\n\n`);
    } catch {
      removeLeaderboardStreamClient(client);
    }
  }
}

async function publishLeaderboardUpdate(reason = "update") {
  if (!leaderboardStreamClients.size) return;
  try {
    const snapshot = await fetchLeaderboardPlayers({ limit: LEADERBOARD_STREAM_LIMIT });
    broadcastLeaderboardPlayers(snapshot.players, reason);
  } catch (error) {
    console.error("Failed to publish leaderboard update", error);
  }
}

function queueLeaderboardUpdate(reason = "update") {
  if (!leaderboardStreamClients.size) return;
  if (leaderboardBroadcastTimer) return;
  leaderboardBroadcastTimer = setTimeout(() => {
    leaderboardBroadcastTimer = null;
    void publishLeaderboardUpdate(reason);
  }, 120);
}

function sanitizeActivityEventType(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9._-]+/g, "_")
    .slice(0, 64);
}

function mapAdminActivityRow(row) {
  return {
    id: String(row.id || ""),
    eventType: sanitizeActivityEventType(row.event_type) || "event",
    playerId: String(row.player_id || ""),
    username: sanitizeUsername(row.username) || "Unknown",
    actorPlayerId: String(row.actor_player_id || ""),
    details: row.details_json && typeof row.details_json === "object" ? row.details_json : {},
    createdAt: Number(row.created_at) || 0
  };
}

function mapAdminBackupRow(row) {
  return {
    id: String(row.id || ""),
    createdAt: Number(row.created_at) || 0,
    createdBy: String(row.created_by || ""),
    summary: row.summary_json && typeof row.summary_json === "object" ? row.summary_json : {}
  };
}

function safeDetailsObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value;
}

async function trimTableById({ tableName, maxRows }) {
  const safeMax = Math.max(1, Math.floor(Number(maxRows) || 1));
  await db.query(
    `
      DELETE FROM ${tableName}
      WHERE id IN (
        SELECT id
        FROM ${tableName}
        ORDER BY created_at DESC, id DESC
        OFFSET $1
      )
    `,
    [safeMax]
  );
}

async function trimAdminPopupReads(maxRows = MAX_ADMIN_POPUP_MESSAGE_READS) {
  const safeMax = Math.max(1, Math.floor(Number(maxRows) || 1));
  await db.query(
    `
      DELETE FROM admin_popup_message_reads
      WHERE (message_id, player_id) IN (
        SELECT message_id, player_id
        FROM admin_popup_message_reads
        ORDER BY read_at DESC, message_id DESC, player_id DESC
        OFFSET $1
      )
    `,
    [safeMax]
  );
}

function recordAdminActivity({
  eventType,
  playerId = "",
  username = "",
  actorPlayerId = "",
  details = {}
} = {}) {
  const safeEventType = sanitizeActivityEventType(eventType) || "event";
  const safePlayerId = sanitizePlayerId(playerId);
  const safeUsername = sanitizeUsername(username) || "";
  const safeActorPlayerId = sanitizePlayerId(actorPlayerId);
  const detailsJson = JSON.stringify(safeDetailsObject(details));
  const createdAt = Date.now();
  void db
    .query(
      `
        INSERT INTO admin_activity_events (event_type, player_id, username, actor_player_id, details_json, created_at)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6)
      `,
      [safeEventType, safePlayerId, safeUsername, safeActorPlayerId, detailsJson, createdAt]
    )
    .then(() => trimTableById({ tableName: "admin_activity_events", maxRows: MAX_ADMIN_ACTIVITY_EVENTS }))
    .catch((error) => {
      console.error("Failed to record admin activity", error);
    });
}

function recordSystemRuntimeEvent({
  kind,
  path = "",
  statusCode = 0,
  playerId = "",
  message = "",
  details = {}
} = {}) {
  const safeKind = sanitizeActivityEventType(kind) || "event";
  const safePath = String(path || "").trim().slice(0, 180);
  const safeStatusCode = Number.isFinite(Number(statusCode)) ? Math.max(0, Math.floor(Number(statusCode))) : 0;
  const safePlayerId = sanitizePlayerId(playerId);
  const safeMessage = String(message || "").slice(0, 240);
  const detailsJson = JSON.stringify(safeDetailsObject(details));
  const createdAt = Date.now();
  void db
    .query(
      `
        INSERT INTO system_runtime_events (kind, path, status_code, player_id, message, details_json, created_at)
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
      `,
      [safeKind, safePath, safeStatusCode, safePlayerId, safeMessage, detailsJson, createdAt]
    )
    .then(() => trimTableById({ tableName: "system_runtime_events", maxRows: MAX_SYSTEM_RUNTIME_EVENTS }))
    .catch((error) => {
      console.error("Failed to record system runtime event", error);
    });
}

function recordBalanceAuditEvent({
  playerId,
  username = "",
  source = "unknown",
  beforeBalance = 0,
  afterBalance = 0
} = {}) {
  const safePlayerId = sanitizePlayerId(playerId);
  if (!safePlayerId) return;
  const beforeValue = Math.round(toNumber(beforeBalance) * 100) / 100;
  const afterValue = Math.round(toNumber(afterBalance) * 100) / 100;
  const delta = Math.round((afterValue - beforeValue) * 100) / 100;
  if (!Number.isFinite(delta) || Math.abs(delta) < 0.01) return;
  const safeUsername = sanitizeUsername(username) || "";
  const safeSource = String(source || "unknown").trim().slice(0, 60);
  const createdAt = Date.now();
  void db
    .query(
      `
        INSERT INTO balance_audit_events (player_id, username, source, before_balance, after_balance, delta, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
      `,
      [safePlayerId, safeUsername, safeSource, beforeValue, afterValue, delta, createdAt]
    )
    .then(() => trimTableById({ tableName: "balance_audit_events", maxRows: MAX_BALANCE_AUDIT_EVENTS }))
    .catch((error) => {
      console.error("Failed to record balance audit event", error);
    });
}

function mapAdminUserRow(row) {
  const base = mapUserRow(row);
  return {
    ...base,
    isAdmin: row.is_admin === true,
    hasPassword: row.has_password === true,
    bannedAt: Number(row.banned_at) || 0,
    bannedReason: String(row.banned_reason || ""),
    mutedUntil: Number(row.muted_until) || 0,
    mutedReason: String(row.muted_reason || ""),
    syncGuardBypassUntil: Number(row.sync_guard_bypass_until) || 0,
    totalClaims: Number(row.total_claims) || 0,
    pendingClaims: Number(row.pending_claims) || 0,
    approvedClaims: Number(row.approved_claims) || 0,
    totalWins: Number(row.total_wins) || 0,
    lastWinAt: Number(row.last_win_at) || 0
  };
}

function normalizeUserForClient(row) {
  const mapped = mapUserRow(row);
  return {
    playerId: mapped.playerId,
    username: mapped.username,
    email: mapped.email,
    balance: mapped.balance,
    shares: mapped.shares,
    avgCost: mapped.avgCost,
    savingsBalance: mapped.savingsBalance,
    autoSavingsPercent: mapped.autoSavingsPercent,
    balanceUpdatedAt: mapped.balanceUpdatedAt,
    lastSeenAt: mapped.lastSeenAt,
    isAdmin: mapped.isAdmin === true
  };
}

function getAdminCode(req) {
  return String(
    req.body?.adminCode ||
      req.headers?.["x-admin-code"] ||
      req.headers?.["x-admincode"] ||
      ""
  );
}

function normalizeAdminCode(value) {
  return String(value || "")
    .trim()
    .replace(/^['"`]+|['"`]+$/g, "")
    .toLowerCase();
}

function sanitizeAdminDeviceToken(value) {
  const token = String(value || "").trim();
  if (!token) return "";
  if (token.length < 24 || token.length > 256) return "";
  if (!/^[a-zA-Z0-9._-]+$/.test(token)) return "";
  return token;
}

function sanitizeAdminDeviceLabel(value) {
  const label = String(value || "")
    .replace(/\s+/g, " ")
    .trim();
  if (!label) return "Trusted device";
  return label.slice(0, 80);
}

function sanitizeBanReason(value) {
  const reason = String(value || "")
    .replace(/\s+/g, " ")
    .trim();
  if (!reason) return "";
  return reason.slice(0, 180);
}

function isBannedTimestamp(value) {
  return Number(value) > 0;
}

function hashAdminDeviceToken(token) {
  return crypto.createHash("sha256").update(String(token)).digest("hex");
}

function getAdminDeviceToken(req) {
  const rawHeader = req.headers["x-admin-device-token"];
  const headerToken = Array.isArray(rawHeader) ? rawHeader[0] : rawHeader;
  return sanitizeAdminDeviceToken(
    headerToken ||
    req.query?.adminDeviceToken ||
    req.body?.adminDeviceToken
  );
}

function getRequestIp(req) {
  const forwardedRaw = req.headers["x-forwarded-for"];
  const forwarded = Array.isArray(forwardedRaw) ? forwardedRaw[0] : String(forwardedRaw || "");
  const firstForwardedIp = forwarded.split(",")[0]?.trim();
  const fallbackIp = String(req.ip || req.socket?.remoteAddress || "").trim();
  return String(firstForwardedIp || fallbackIp || "").slice(0, 128);
}

function getRequestUserAgent(req) {
  return String(req.headers["user-agent"] || "").slice(0, 255);
}

async function getTrustedAdminDeviceCount() {
  const result = await db.query(
    `
      SELECT COUNT(*)::int AS count
      FROM admin_trusted_devices
      WHERE revoked_at = 0
        AND token_hash IS NOT NULL
        AND token_hash <> ''
    `
  );
  return Number(result.rows?.[0]?.count) || 0;
}

async function getActiveAdminUserCount() {
  const result = await db.query(
    `
      SELECT COUNT(*)::int AS count
      FROM users
      WHERE is_admin = true
        AND COALESCE(banned_at, 0) <= 0
    `
  );
  return Number(result.rows?.[0]?.count) || 0;
}

function mapAdminDeviceRow(row) {
  return {
    id: String(row.id || ""),
    playerId: String(row.player_id || ""),
    label: sanitizeAdminDeviceLabel(row.label),
    trustedAt: Number(row.created_at) || 0,
    lastSeenAt: Number(row.last_seen_at) || 0,
    lastIp: String(row.last_ip || ""),
    lastUserAgent: String(row.last_user_agent || ""),
    revokedAt: Number(row.revoked_at) || 0,
    ownerLocked: row.owner_lock === true,
    active: Number(row.revoked_at) <= 0
  };
}

async function getUserBanStateByPlayerId(playerId) {
  const result = await db.query(
    `
      SELECT player_id, banned_at, banned_reason, muted_until, muted_reason
      FROM users
      WHERE player_id = $1
      LIMIT 1
    `,
    [playerId]
  );
  if (!result.rowCount) return null;
  const row = result.rows[0];
  return {
    playerId: String(row.player_id || ""),
    bannedAt: Number(row.banned_at) || 0,
    bannedReason: String(row.banned_reason || ""),
    mutedUntil: Number(row.muted_until) || 0,
    mutedReason: String(row.muted_reason || "")
  };
}

async function assertUserNotBannedByPlayerId(playerId, res) {
  const banState = await getUserBanStateByPlayerId(playerId);
  if (!banState) return true;
  if (!isBannedTimestamp(banState.bannedAt)) return true;
  const reasonSuffix = banState.bannedReason ? ` (${banState.bannedReason})` : "";
  res.status(403).json({ ok: false, error: `Account is banned${reasonSuffix}.` });
  return false;
}

async function assertUserNotMutedByPlayerId(playerId, res) {
  const moderationState = await getUserBanStateByPlayerId(playerId);
  if (!moderationState) return true;
  const mutedUntil = Number(moderationState.mutedUntil) || 0;
  const now = Date.now();
  if (!mutedUntil || mutedUntil <= now) return true;
  const retryAfterSeconds = Math.max(1, Math.ceil((mutedUntil - now) / 1000));
  const reasonSuffix = moderationState.mutedReason ? ` (${moderationState.mutedReason})` : "";
  res.status(403).json({
    ok: false,
    error: `Account is temporarily muted${reasonSuffix}. Try again in ${retryAfterSeconds} seconds.`,
    retryAfterSeconds
  });
  return false;
}

async function touchTrustedAdminDeviceByHash(tokenHash, playerId, req) {
  await db.query(
    `
      UPDATE admin_trusted_devices
      SET last_seen_at = $1,
          last_ip = $2,
          last_user_agent = $3
      WHERE token_hash = $4
        AND player_id = $5
    `,
    [Date.now(), getRequestIp(req), getRequestUserAgent(req), tokenHash, playerId]
  );
}

async function requireAdminAccess(req, res, { allowBootstrap = false } = {}) {
  try {
    const sessionPlayerId = getSessionPlayerId(req);
    if (!sessionPlayerId) {
      res.status(401).json({ ok: false, error: "Login required for admin access." });
      return null;
    }
    const userLookup = await db.query(
      `
        SELECT player_id, is_admin, banned_at, banned_reason
        FROM users
        WHERE player_id = $1
        LIMIT 1
      `,
      [sessionPlayerId]
    );
    if (!userLookup.rowCount) {
      req.session.destroy(() => {});
      res.status(401).json({ ok: false, error: "Session account not found. Please login again." });
      return null;
    }
    const user = userLookup.rows[0];
    if (isBannedTimestamp(user.banned_at)) {
      const reason = sanitizeBanReason(user.banned_reason);
      res.status(403).json({ ok: false, error: reason ? `Account is banned (${reason}).` : "Account is banned." });
      return null;
    }

    let isAdmin = user.is_admin === true;
    const enteredCode = normalizeAdminCode(getAdminCode(req));
    const expectedCode = normalizeAdminCode(ADMIN_CODE);
    const codeMatches = Boolean(enteredCode) && enteredCode === expectedCode;
    let bootstrap = false;
    if (!isAdmin) {
      if (allowBootstrap && codeMatches) {
        const activeAdminCount = await getActiveAdminUserCount();
        if (activeAdminCount > 0) {
          res.status(403).json({ ok: false, error: "Admin role required." });
          return null;
        }
        await db.query(
          `
            UPDATE users
            SET is_admin = true,
                last_seen_at = $2
            WHERE player_id = $1
          `,
          [sessionPlayerId, Date.now()]
        );
        isAdmin = true;
        bootstrap = true;
      } else {
        res.status(403).json({ ok: false, error: "Admin role required." });
        return null;
      }
    }

    const deviceToken = getAdminDeviceToken(req);
    const tokenHash = deviceToken ? hashAdminDeviceToken(deviceToken) : "";
    const ownerLockLookup = await db.query(
      `
        SELECT token_hash, player_id
        FROM admin_trusted_devices
        WHERE owner_lock = true
          AND revoked_at = 0
        ORDER BY id DESC
        LIMIT 1
      `
    );
    const ownerLock = ownerLockLookup.rowCount ? ownerLockLookup.rows[0] : null;
    const ownerTokenHash = ownerLock ? String(ownerLock.token_hash || "") : "";
    const ownerPlayerId = ownerLock ? sanitizePlayerId(ownerLock.player_id) : "";
    const ownerLockEnabled = Boolean(ownerTokenHash);

    if (ownerLockEnabled) {
      if (!tokenHash || tokenHash !== ownerTokenHash || !ownerPlayerId || ownerPlayerId !== sessionPlayerId) {
        res.status(403).json({ ok: false, error: "Admin panel is locked to the owner device only." });
        return null;
      }
    }

    if (tokenHash) {
      const trustedLookup = await db.query(
        `
          SELECT id, player_id, label, created_at, last_seen_at, last_ip, last_user_agent, revoked_at, owner_lock
          FROM admin_trusted_devices
          WHERE token_hash = $1
            AND player_id = $2
            AND revoked_at = 0
          LIMIT 1
        `,
        [tokenHash, sessionPlayerId]
      );
      if (trustedLookup.rowCount) {
        await touchTrustedAdminDeviceByHash(tokenHash, sessionPlayerId, req);
        return {
          ok: true,
          playerId: sessionPlayerId,
          bootstrap,
          trustedCount: await getTrustedAdminDeviceCount(),
          token: deviceToken,
          tokenHash,
          isAdmin: true,
          viaTrustedDevice: true,
          viaCode: false,
          ownerLockEnabled,
          device: mapAdminDeviceRow(trustedLookup.rows[0])
        };
      }
    }

    if (ownerLockEnabled) {
      res.status(403).json({ ok: false, error: "Admin panel is locked to the owner device only." });
      return null;
    }

    if (!codeMatches) {
      res.status(401).json({ ok: false, error: "Trusted admin device required. Enter admin code." });
      return null;
    }
    const trustedCount = await getTrustedAdminDeviceCount();
    return {
      ok: true,
      playerId: sessionPlayerId,
      bootstrap,
      trustedCount,
      token: deviceToken,
      tokenHash,
      isAdmin: true,
      viaTrustedDevice: false,
      viaCode: true,
      ownerLockEnabled,
      device: null
    };
  } catch (error) {
    console.error("Failed admin access validation", error);
    res.status(500).json({ ok: false, error: "Could not validate admin access." });
    return null;
  }
}

const RATE_LIMIT_RULES = Object.freeze([
  {
    id: "auth-login",
    methods: new Set(["POST"]),
    pattern: /^\/api\/auth\/login$/i,
    scope: "loginOrIp",
    max: 20,
    windowMs: 60 * 1000,
    blockMs: 2 * 60 * 1000
  },
  {
    id: "auth-register",
    methods: new Set(["POST"]),
    pattern: /^\/api\/auth\/register$/i,
    scope: "ip",
    max: 8,
    windowMs: 10 * 60 * 1000,
    blockMs: 10 * 60 * 1000
  },
  {
    id: "auth-write",
    methods: new Set(["POST"]),
    pattern: /^\/api\/auth\/(logout|verify-email|resend-verification)$/i,
    scope: "ip",
    max: 30,
    windowMs: 5 * 60 * 1000,
    blockMs: 5 * 60 * 1000
  },
  {
    id: "auth-session",
    methods: new Set(["GET"]),
    pattern: /^\/api\/auth\/session$/i,
    scope: "sessionOrIp",
    max: 180,
    windowMs: 60 * 1000,
    blockMs: 60 * 1000
  },
  {
    id: "claims-submit",
    methods: new Set(["POST"]),
    pattern: /^\/api\/claims$/i,
    scope: "playerOrIp",
    max: 30,
    windowMs: 5 * 60 * 1000,
    blockMs: 10 * 60 * 1000
  },
  {
    id: "claims-read",
    methods: new Set(["GET"]),
    pattern: /^\/api\/claims(\/credits)?$/i,
    scope: "playerOrIp",
    max: 240,
    windowMs: 60 * 1000,
    blockMs: 60 * 1000
  },
  {
    id: "live-wins-write",
    methods: new Set(["POST"]),
    pattern: /^\/api\/live-wins$/i,
    scope: "playerOrIp",
    max: 120,
    windowMs: 60 * 1000,
    blockMs: 5 * 60 * 1000
  },
  {
    id: "feedback-read",
    methods: new Set(["GET"]),
    pattern: /^\/api\/feedback$/i,
    scope: "sessionOrIp",
    max: 120,
    windowMs: 60 * 1000,
    blockMs: 60 * 1000
  },
  {
    id: "feedback-submit",
    methods: new Set(["POST"]),
    pattern: /^\/api\/feedback$/i,
    scope: "sessionOrIp",
    max: 10,
    windowMs: 10 * 60 * 1000,
    blockMs: 60 * 60 * 1000
  },
  {
    id: "admin-read",
    methods: new Set(["GET"]),
    pattern: /^\/api\/admin\/(claims|users|stats|devices|feedback|messages|activity|health|fraud-flags|backups)$/i,
    scope: "sessionOrIp",
    max: 360,
    windowMs: 60 * 1000,
    blockMs: 2 * 60 * 1000
  },
  {
    id: "admin-write",
    methods: new Set(["POST"]),
    pattern: /^\/api\/admin\//i,
    scope: "sessionOrIp",
    max: 80,
    windowMs: 10 * 60 * 1000,
    blockMs: 15 * 60 * 1000
  },
  {
    id: "user-sync",
    methods: new Set(["POST"]),
    pattern: /^\/api\/users\/sync$/i,
    scope: "sessionOrIp",
    max: 180,
    windowMs: 60 * 1000,
    blockMs: 60 * 1000
  },
  {
    id: "api-default",
    methods: new Set(["GET", "POST"]),
    pattern: /^\/api\//i,
    scope: "ip",
    max: 300,
    windowMs: 60 * 1000,
    blockMs: 60 * 1000
  }
]);

const rateLimitStore = new Map();

function getApiPath(req) {
  return String(req.originalUrl || req.url || "")
    .split("?")[0]
    .trim()
    .toLowerCase();
}

function findRateLimitRule(req) {
  const method = String(req.method || "GET").toUpperCase();
  const path = getApiPath(req);
  return RATE_LIMIT_RULES.find((rule) => rule.methods.has(method) && rule.pattern.test(path)) || null;
}

function getRateLimitScopeValue(req, scope) {
  const sessionPlayerId = getSessionPlayerId(req);
  const requestPlayerId = sanitizePlayerId(req.body?.playerId || req.query?.playerId);
  const loginUsernameKey = normalizeUsernameKey(req.body?.username);
  const ip = getRequestIp(req) || "unknown";
  if (scope === "loginOrIp") {
    return loginUsernameKey ? `login:${loginUsernameKey}` : `ip:${ip}`;
  }
  if (scope === "sessionOrIp") {
    return sessionPlayerId ? `player:${sessionPlayerId}` : `ip:${ip}`;
  }
  if (scope === "playerOrIp") {
    return requestPlayerId ? `player:${requestPlayerId}` : (sessionPlayerId ? `player:${sessionPlayerId}` : `ip:${ip}`);
  }
  return `ip:${ip}`;
}

function consumeRateLimitBucket({ key, max, windowMs, blockMs }) {
  const now = Date.now();
  let entry = rateLimitStore.get(key);
  if (!entry || now >= entry.resetAt) {
    entry = {
      count: 0,
      resetAt: now + windowMs,
      blockedUntil: 0
    };
  }
  if (entry.blockedUntil > now) {
    const retryAfterMs = entry.blockedUntil - now;
    const retryAfterSeconds = Math.max(1, Math.ceil(retryAfterMs / 1000));
    rateLimitStore.set(key, entry);
    return {
      allowed: false,
      retryAfterSeconds,
      remaining: 0,
      resetAt: entry.resetAt
    };
  }

  entry.count += 1;
  const remaining = Math.max(0, max - entry.count);
  if (entry.count > max) {
    entry.blockedUntil = now + blockMs;
    rateLimitStore.set(key, entry);
    return {
      allowed: false,
      retryAfterSeconds: Math.max(1, Math.ceil(blockMs / 1000)),
      remaining: 0,
      resetAt: entry.resetAt
    };
  }

  rateLimitStore.set(key, entry);
  return {
    allowed: true,
    retryAfterSeconds: 0,
    remaining,
    resetAt: entry.resetAt
  };
}

function cleanupRateLimitStore() {
  const now = Date.now();
  for (const [key, entry] of rateLimitStore.entries()) {
    if (!entry) {
      rateLimitStore.delete(key);
      continue;
    }
    if (entry.blockedUntil > now) continue;
    if (entry.resetAt > now) continue;
    rateLimitStore.delete(key);
  }
}

const rateLimitCleanupTimer = setInterval(cleanupRateLimitStore, 5 * 60 * 1000);
if (typeof rateLimitCleanupTimer.unref === "function") {
  rateLimitCleanupTimer.unref();
}

function apiRateLimitMiddleware(req, res, next) {
  if (!RATE_LIMIT_ENABLED) {
    next();
    return;
  }
  if (String(req.method || "").toUpperCase() === "OPTIONS") {
    next();
    return;
  }
  const apiPath = getApiPath(req);
  if (!apiPath.startsWith("/api/")) {
    next();
    return;
  }
  if (apiPath === "/api/health") {
    next();
    return;
  }

  const rule = findRateLimitRule(req);
  if (!rule) {
    next();
    return;
  }

  const scopeValue = getRateLimitScopeValue(req, rule.scope);
  const bucketKey = `${rule.id}|${scopeValue}`;
  const result = consumeRateLimitBucket({
    key: bucketKey,
    max: rule.max,
    windowMs: rule.windowMs,
    blockMs: rule.blockMs
  });

  res.setHeader("X-RateLimit-Limit", String(rule.max));
  res.setHeader("X-RateLimit-Remaining", String(result.remaining));
  res.setHeader("X-RateLimit-Reset", String(Math.ceil(result.resetAt / 1000)));

  if (!result.allowed) {
    recordSystemRuntimeEvent({
      kind: "rate_limit",
      path: apiPath,
      statusCode: 429,
      playerId: getSessionPlayerId(req),
      message: rule.id,
      details: {
        ruleId: rule.id,
        scope: rule.scope,
        retryAfterSeconds: result.retryAfterSeconds
      }
    });
    res.setHeader("Retry-After", String(result.retryAfterSeconds));
    res.status(429).json({
      ok: false,
      error: `Too many requests. Try again in ${result.retryAfterSeconds} seconds.`
    });
    return;
  }

  next();
}

app.use(express.json());
app.use((req, res, next) => {
  const requestOrigin = String(req.headers.origin || "").trim();
  if (requestOrigin) {
    res.setHeader("Access-Control-Allow-Origin", requestOrigin);
    res.setHeader("Vary", "Origin");
  } else {
    res.setHeader("Access-Control-Allow-Origin", "*");
  }
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, X-Admin-Device-Token, X-Admin-Code");
  res.setHeader("Access-Control-Allow-Credentials", "true");
  if (req.method === "OPTIONS") {
    res.sendStatus(204);
    return;
  }
  next();
});
app.set("trust proxy", 1);
app.use(
  session({
    name: "nows.sid",
    store: new PgSessionStore({
      pool: db,
      tableName: "user_sessions",
      createTableIfMissing: true
    }),
    secret: SESSION_SECRET,
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      sameSite: IS_PRODUCTION ? "none" : "lax",
      secure: IS_PRODUCTION,
      maxAge: 1000 * 60 * 60 * 24 * 30
    }
  })
);
app.use((req, res, next) => {
  const requestPath = getApiPath(req);
  if (!requestPath.startsWith("/api/")) {
    next();
    return;
  }
  res.on("finish", () => {
    if (res.statusCode < 500) return;
    recordSystemRuntimeEvent({
      kind: "error",
      path: requestPath,
      statusCode: res.statusCode,
      playerId: sanitizePlayerId(req.session?.playerId),
      message: "api_response_5xx"
    });
  });
  next();
});
app.use(express.static(__dirname));
app.use("/api", apiRateLimitMiddleware);

app.get("/api/health", (_req, res) => {
  res.json({ ok: true, service: "claims", database: "postgres" });
});

app.post("/api/visits", async (req, res) => {
  try {
    const now = Date.now();
    const requestIp = getRequestIp(req) || "";
    const requestUserAgent = getRequestUserAgent(req);
    const visitorFromBody = sanitizeVisitorId(req.body?.visitorId);
    const visitorFallbackHash = crypto
      .createHash("sha256")
      .update(`${requestIp}|${requestUserAgent}`)
      .digest("hex")
      .slice(0, 40);
    const visitorId = visitorFromBody || `anon_${visitorFallbackHash}`;
    const playerId = sanitizePlayerId(req.body?.playerId || getSessionPlayerId(req));
    const username = sanitizeUsername(req.body?.username);
    const visitPath = sanitizeVisitPath(req.body?.path || req.headers.referer || "/");

    const latest = await db.query(
      `
        SELECT visited_at
        FROM site_visit_events
        WHERE visitor_id = $1
        ORDER BY visited_at DESC, id DESC
        LIMIT 1
      `,
      [visitorId]
    );
    const lastVisitedAt = Number(latest.rows?.[0]?.visited_at) || 0;
    if (lastVisitedAt > 0 && now - lastVisitedAt < SITE_VISIT_SERVER_COOLDOWN_MS) {
      res.json({ ok: true, counted: false, visitedAt: lastVisitedAt });
      return;
    }

    await db.query(
      `
        INSERT INTO site_visit_events (visitor_id, player_id, username, path, ip, user_agent, visited_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
      `,
      [visitorId, playerId, username, visitPath, requestIp, requestUserAgent, now]
    );
    void trimTableById({ tableName: "site_visit_events", maxRows: MAX_SITE_VISIT_EVENTS });
    res.json({ ok: true, counted: true, visitedAt: now });
  } catch (error) {
    console.error("Failed to track visit", error);
    res.status(500).json({ ok: false, error: "Could not track visit." });
  }
});

function getSessionPlayerId(req) {
  return sanitizePlayerId(req.session?.playerId);
}

app.get("/api/auth/session", async (req, res) => {
  try {
    const playerId = getSessionPlayerId(req);
    if (!playerId) {
      res.json({ ok: true, authenticated: false });
      return;
    }
    const lookup = await db.query(
      `
        SELECT player_id, username, username_key, email, balance, shares, avg_cost, savings_balance, auto_savings_percent, balance_updated_at, last_seen_at, banned_at, banned_reason, is_admin
        FROM users
        WHERE player_id = $1
        LIMIT 1
      `,
      [playerId]
    );
    if (!lookup.rowCount) {
      req.session.destroy(() => {});
      res.json({ ok: true, authenticated: false });
      return;
    }
    if (isBannedTimestamp(lookup.rows[0].banned_at)) {
      req.session.destroy(() => {});
      res.status(403).json({ ok: false, authenticated: false, error: "Account is banned." });
      return;
    }
    const adminPopups = await fetchUnreadAdminPopupsForPlayer(playerId, { limit: 3 });
    res.json({
      ok: true,
      authenticated: true,
      user: normalizeUserForClient(lookup.rows[0]),
      adminPopups
    });
  } catch (error) {
    console.error("Failed to read auth session", error);
    res.status(500).json({ ok: false, error: "Could not load auth session." });
  }
});

app.post("/api/auth/register", async (req, res) => {
  try {
    const password = sanitizePassword(req.body?.password);
    const username = sanitizeUsername(req.body?.username);
    const usernameKey = normalizeUsernameKey(username);
    if (!password) {
      res.status(400).json({ ok: false, error: "Password must be 8-72 characters." });
      return;
    }
    if (!username || !usernameKey) {
      res.status(400).json({ ok: false, error: "Invalid username." });
      return;
    }
    const existingByUsername = await db.query(
      `
        SELECT player_id, username, username_key, email, password_hash, balance, shares, avg_cost, savings_balance, auto_savings_percent, balance_updated_at, last_seen_at, banned_at, banned_reason, is_admin
        FROM users
        WHERE username_key = $1
        LIMIT 1
      `,
      [usernameKey]
    );
    if (existingByUsername.rowCount) {
      const existing = existingByUsername.rows[0];
      if (isBannedTimestamp(existing.banned_at)) {
        const reason = sanitizeBanReason(existing.banned_reason);
        res.status(403).json({ ok: false, error: reason ? `Account is banned (${reason}).` : "Account is banned." });
        return;
      }
      const existingHash = String(existing.password_hash || "");
      if (!existingHash) {
        const replacementHash = await bcrypt.hash(password, 12);
        const updated = await db.query(
          `
            UPDATE users
            SET username = $1,
                username_key = $2,
                password_hash = $3,
                email = NULL,
                last_seen_at = $4
            WHERE player_id = $5
            RETURNING player_id, username, username_key, email, password_hash, balance, shares, avg_cost, savings_balance, auto_savings_percent, balance_updated_at, last_seen_at, is_admin
          `,
          [username, usernameKey, replacementHash, Date.now(), existing.player_id]
        );
        req.session.playerId = sanitizePlayerId(updated.rows[0]?.player_id || existing.player_id);
        queueLeaderboardUpdate("register");
        recordBalanceAuditEvent({
          playerId: String(updated.rows[0]?.player_id || existing.player_id),
          username: sanitizeUsername(updated.rows[0]?.username || existing.username) || username,
          source: "auth-register-recovery",
          beforeBalance: Number(existing.balance) || INITIAL_ACCOUNT_BALANCE,
          afterBalance: Number(updated.rows[0]?.balance ?? existing.balance ?? INITIAL_ACCOUNT_BALANCE)
        });
        const sessionPlayerId = sanitizePlayerId(updated.rows[0]?.player_id || existing.player_id);
        const adminPopups = await fetchUnreadAdminPopupsForPlayer(sessionPlayerId, { limit: 3 });
        res.json({ ok: true, user: normalizeUserForClient(updated.rows[0] || existing), adminPopups });
        return;
      }
      res.status(409).json({
        ok: false,
        error: "Username already has an account. Please login.",
        loginUsername: sanitizeUsername(existing.username) || ""
      });
      return;
    }
    const passwordHash = await bcrypt.hash(password, 12);
    const playerId = `u_${crypto.randomUUID().replace(/-/g, "")}`;
    const upsert = await db.query(
      `
        INSERT INTO users (player_id, username, username_key, email, password_hash, balance, shares, avg_cost, savings_balance, auto_savings_percent, last_seen_at, balance_updated_at)
        VALUES ($1, $2, $3, NULL, $4, $5, 0, 0, 0, 0, $6, $6)
        RETURNING player_id, username, username_key, email, password_hash, balance, shares, avg_cost, savings_balance, auto_savings_percent, balance_updated_at, last_seen_at, is_admin
      `,
      [playerId, username, usernameKey, passwordHash, INITIAL_ACCOUNT_BALANCE, Date.now()]
    );
    req.session.playerId = sanitizePlayerId(upsert.rows[0]?.player_id || playerId);
    queueLeaderboardUpdate("register");
    recordBalanceAuditEvent({
      playerId: String(upsert.rows[0]?.player_id || playerId),
      username,
      source: "auth-register",
      beforeBalance: 0,
      afterBalance: Number(upsert.rows[0]?.balance) || INITIAL_ACCOUNT_BALANCE
    });
    const sessionPlayerId = sanitizePlayerId(upsert.rows[0]?.player_id || playerId);
    const adminPopups = await fetchUnreadAdminPopupsForPlayer(sessionPlayerId, { limit: 3 });
    res.json({ ok: true, user: normalizeUserForClient(upsert.rows[0]), adminPopups });
  } catch (error) {
    if (error?.code === "23505") {
      res.status(409).json({ ok: false, error: "Username already taken." });
      return;
    }
    console.error("Failed to register auth user", error);
    res.status(500).json({ ok: false, error: "Could not create account." });
  }
});

app.post("/api/auth/login", async (req, res) => {
  try {
    const username = sanitizeUsername(req.body?.username);
    const usernameKey = normalizeUsernameKey(username);
    const password = sanitizePassword(req.body?.password);
    if (!usernameKey || !password) {
      res.status(400).json({ ok: false, error: "Invalid username or password." });
      return;
    }
    const lookup = await db.query(
      `
        SELECT player_id, username, username_key, email, password_hash, balance, shares, avg_cost, savings_balance, auto_savings_percent, balance_updated_at, last_seen_at, banned_at, banned_reason, is_admin
        FROM users
        WHERE username_key = $1
        LIMIT 1
      `,
      [usernameKey]
    );
    if (!lookup.rowCount) {
      res.status(401).json({ ok: false, error: "Invalid username or password." });
      return;
    }
    const user = lookup.rows[0];
    if (isBannedTimestamp(user.banned_at)) {
      const reason = sanitizeBanReason(user.banned_reason);
      res.status(403).json({ ok: false, error: reason ? `Account is banned (${reason}).` : "Account is banned." });
      return;
    }
    const storedHash = String(user.password_hash || "");
    if (!storedHash) {
      res.status(401).json({ ok: false, error: "Account password is not set." });
      return;
    }
    const isMatch = await bcrypt.compare(password, storedHash);
    if (!isMatch) {
      res.status(401).json({ ok: false, error: "Invalid username or password." });
      return;
    }
    await db.query(
      `
        UPDATE users
        SET last_seen_at = $1
        WHERE player_id = $2
      `,
      [Date.now(), user.player_id]
    );
    req.session.playerId = sanitizePlayerId(user.player_id);
    queueLeaderboardUpdate("login");
    recordAdminActivity({
      eventType: "login",
      playerId: user.player_id,
      username: sanitizeUsername(user.username) || username,
      details: {
        ip: getRequestIp(req)
      }
    });
    const sessionPlayerId = sanitizePlayerId(user.player_id);
    const adminPopups = await fetchUnreadAdminPopupsForPlayer(sessionPlayerId, { limit: 3 });
    res.json({ ok: true, user: normalizeUserForClient(user), adminPopups });
  } catch (error) {
    console.error("Failed to login", error);
    res.status(500).json({ ok: false, error: "Could not login." });
  }
});

app.post("/api/auth/verify-email", async (req, res) => {
  res.status(410).json({ ok: false, error: "Email verification is disabled. Use username + password login." });
});

app.post("/api/auth/resend-verification", async (req, res) => {
  res.status(410).json({ ok: false, error: "Email verification is disabled. Use username + password login." });
});

app.post("/api/auth/logout", (req, res) => {
  req.session.destroy((error) => {
    if (error) {
      console.error("Failed to logout", error);
      res.status(500).json({ ok: false, error: "Could not logout." });
      return;
    }
    res.clearCookie("nows.sid");
    res.json({ ok: true });
  });
});

app.post("/api/messages/:id/ack", async (req, res) => {
  try {
    const playerId = getSessionPlayerId(req);
    if (!playerId) {
      res.status(401).json({ ok: false, error: "Login required." });
      return;
    }
    const messageId = String(req.params?.id || "").trim();
    if (!/^\d{1,18}$/.test(messageId)) {
      res.status(400).json({ ok: false, error: "Invalid message id." });
      return;
    }
    const lookup = await db.query(
      `
        SELECT id
        FROM admin_popup_messages
        WHERE id = $1
          AND active = true
          AND (target_player_id = '' OR target_player_id = $2)
        LIMIT 1
      `,
      [messageId, playerId]
    );
    if (!lookup.rowCount) {
      res.status(404).json({ ok: false, error: "Message not found." });
      return;
    }
    await db.query(
      `
        INSERT INTO admin_popup_message_reads (message_id, player_id, read_at)
        VALUES ($1, $2, $3)
        ON CONFLICT (message_id, player_id)
        DO UPDATE SET read_at = EXCLUDED.read_at
      `,
      [messageId, playerId, Date.now()]
    );
    await trimAdminPopupReads();
    res.json({ ok: true, ack: { id: String(messageId) } });
  } catch (error) {
    console.error("Failed to acknowledge admin popup message", error);
    res.status(500).json({ ok: false, error: "Could not acknowledge message." });
  }
});

app.get("/api/feedback", async (req, res) => {
  try {
    const playerId = getSessionPlayerId(req);
    if (!playerId) {
      res.status(401).json({ ok: false, error: "Login required." });
      return;
    }
    const rawLimit = Number(req.query?.limit);
    const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(30, Math.floor(rawLimit))) : 15;
    const result = await db.query(
      `
        SELECT id, player_id, username, category, message, status, submitted_at
        FROM feedback_submissions
        WHERE player_id = $1
        ORDER BY submitted_at DESC, id DESC
        LIMIT $2
      `,
      [playerId, limit]
    );
    res.json({ ok: true, feedback: result.rows.map(mapFeedbackRow) });
  } catch (error) {
    console.error("Failed to load feedback submissions", error);
    res.status(500).json({ ok: false, error: "Could not load feedback." });
  }
});

app.post("/api/feedback", async (req, res) => {
  try {
    const playerId = getSessionPlayerId(req);
    if (!playerId) {
      res.status(401).json({ ok: false, error: "Login required." });
      return;
    }
    const message = sanitizeFeedbackMessage(req.body?.message);
    const category = sanitizeFeedbackCategory(req.body?.category);
    if (!message || message.length < 6) {
      res.status(400).json({ ok: false, error: "Feedback must be at least 6 characters." });
      return;
    }
    if (hasProfanity(message)) {
      res.status(400).json({ ok: false, error: "Please remove profanity and try again." });
      return;
    }
    const userLookup = await db.query(
      `
        SELECT player_id, username, banned_at, banned_reason, muted_until, muted_reason
        FROM users
        WHERE player_id = $1
        LIMIT 1
      `,
      [playerId]
    );
    if (!userLookup.rowCount) {
      req.session.destroy(() => {});
      res.status(401).json({ ok: false, error: "Session account not found. Please login again." });
      return;
    }
    const user = userLookup.rows[0];
    if (isBannedTimestamp(user.banned_at)) {
      const reason = sanitizeBanReason(user.banned_reason);
      res.status(403).json({ ok: false, error: reason ? `Account is banned (${reason}).` : "Account is banned." });
      return;
    }
    const mutedUntil = Number(user.muted_until) || 0;
    if (mutedUntil > Date.now()) {
      const retryAfterSeconds = Math.max(1, Math.ceil((mutedUntil - Date.now()) / 1000));
      const reason = sanitizeBanReason(user.muted_reason);
      res.status(403).json({
        ok: false,
        error: reason
          ? `Account is temporarily muted (${reason}). Try again in ${retryAfterSeconds} seconds.`
          : `Account is temporarily muted. Try again in ${retryAfterSeconds} seconds.`,
        retryAfterSeconds
      });
      return;
    }
    const inserted = await db.query(
      `
        INSERT INTO feedback_submissions (player_id, username, category, message, status, submitted_at, reviewed_at)
        VALUES ($1, $2, $3, $4, 'open', $5, 0)
        RETURNING id, player_id, username, category, message, status, submitted_at
      `,
      [playerId, sanitizeUsername(user.username) || "Player", category, message, Date.now()]
    );
    res.json({ ok: true, feedback: mapFeedbackRow(inserted.rows[0]) });
  } catch (error) {
    console.error("Failed to submit feedback", error);
    res.status(500).json({ ok: false, error: "Could not submit feedback." });
  }
});

app.get("/api/live-wins", async (req, res) => {
  try {
    const rawLimit = Number(req.query?.limit);
    const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(120, Math.floor(rawLimit))) : 40;
    const result = await db.query(
      `
        SELECT id, player_id, username, vip, game, amount, multiplier, submitted_at
        FROM live_wins
        ORDER BY submitted_at DESC, id DESC
        LIMIT $1
      `,
      [limit]
    );
    const wins = result.rows.map(mapWinRow);
    res.json({ ok: true, wins });
  } catch (error) {
    console.error("Failed to load live wins", error);
    res.status(500).json({ ok: false, error: "Could not load live wins." });
  }
});

app.post("/api/live-wins", async (req, res) => {
  try {
    const playerId = sanitizePlayerId(req.body?.playerId);
    const username = sanitizeUsername(req.body?.username);
    const game = sanitizeGameLabel(req.body?.game);
    const amount = Math.round((Number(req.body?.amount) || 0) * 100) / 100;
    const multiplierRaw = Number(req.body?.multiplier);
    const vip = req.body?.vip === true;
    if (!playerId || !username || !Number.isFinite(amount) || amount <= 0) {
      res.status(400).json({ ok: false, error: "Invalid live win payload." });
      return;
    }
    if (!(await assertUserNotBannedByPlayerId(playerId, res))) return;
    if (!(await assertUserNotMutedByPlayerId(playerId, res))) return;
    const multiplier = Number.isFinite(multiplierRaw) && multiplierRaw > 0
      ? Math.round(multiplierRaw * 100) / 100
      : null;
    const submittedAt = Date.now();
    const insertResult = await db.query(
      `
        INSERT INTO live_wins (player_id, username, vip, game, amount, multiplier, submitted_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id, player_id, username, vip, game, amount, multiplier, submitted_at
      `,
      [playerId, username, vip, game, amount, multiplier, submittedAt]
    );
    await db.query(
      `
        DELETE FROM live_wins
        WHERE id IN (
          SELECT id
          FROM live_wins
          ORDER BY submitted_at DESC, id DESC
          OFFSET 300
        )
      `
    );
    const win = mapWinRow(insertResult.rows[0]);
    if (Number(amount) >= BIG_WIN_ACTIVITY_MIN_AMOUNT || (Number.isFinite(multiplier) && Number(multiplier) >= 10)) {
      recordAdminActivity({
        eventType: "big_win",
        playerId,
        username,
        details: {
          game,
          amount: Math.round(Number(amount) * 100) / 100,
          multiplier: Number.isFinite(multiplier) ? Number(multiplier) : null
        }
      });
    }
    res.json({ ok: true, win });
  } catch (error) {
    console.error("Failed to submit live win", error);
    res.status(500).json({ ok: false, error: "Could not submit live win." });
  }
});

app.get("/api/leaderboard", async (req, res) => {
  try {
    const snapshot = await fetchLeaderboardPlayers({
      limit: req.query?.limit,
      playerId: req.query?.playerId,
      username: req.query?.username
    });
    res.json({ ok: true, players: snapshot.players, you: snapshot.you });
  } catch (error) {
    console.error("Failed to load leaderboard", error);
    res.status(500).json({ ok: false, error: "Could not load leaderboard." });
  }
});

app.get("/api/leaderboard/stream", async (req, res) => {
  try {
    res.status(200);
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache, no-transform");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("X-Accel-Buffering", "no");
    if (typeof res.flushHeaders === "function") res.flushHeaders();

    const client = {
      id: crypto.randomUUID(),
      res,
      heartbeat: null
    };
    leaderboardStreamClients.add(client);
    client.heartbeat = setInterval(() => {
      try {
        client.res.write(": ping\n\n");
      } catch {
        removeLeaderboardStreamClient(client);
      }
    }, 25000);
    if (typeof client.heartbeat.unref === "function") client.heartbeat.unref();

    req.on("close", () => removeLeaderboardStreamClient(client));
    req.on("aborted", () => removeLeaderboardStreamClient(client));

    const snapshot = await fetchLeaderboardPlayers({ limit: LEADERBOARD_STREAM_LIMIT });
    broadcastLeaderboardPlayers(snapshot.players, "initial");
  } catch (error) {
    console.error("Failed to open leaderboard stream", error);
    if (!res.headersSent) {
      res.status(500).json({ ok: false, error: "Could not open leaderboard stream." });
    } else {
      try {
        res.end();
      } catch {}
    }
  }
});

app.post("/api/claims", async (req, res) => {
  try {
    const playerId = sanitizePlayerId(req.body?.playerId);
    const username = sanitizeUsername(req.body?.username);
    const packageId = normalizePackageId(
      req.body?.packageId ||
      req.body?.packId ||
      req.body?.pack ||
      req.body?.package
    );
    const source = sanitizeClaimSource(req.body?.source);
    const txnId = String(req.body?.txnId || "").trim();
    const txnNorm = normalizeTxn(txnId);
    const pack = STORE_PACKAGES[packageId];
    if (!playerId) {
      res.status(400).json({ ok: false, error: "Invalid player id." });
      return;
    }
    if (!(await assertUserNotBannedByPlayerId(playerId, res))) return;
    if (!(await assertUserNotMutedByPlayerId(playerId, res))) return;
    if (!pack) {
      res.status(400).json({ ok: false, error: "Invalid package." });
      return;
    }
    if (txnNorm.length < 4) {
      res.status(400).json({ ok: false, error: "Invalid transaction id." });
      return;
    }
    const submittedAt = Date.now();
    const insertResult = await db.query(
      `
        INSERT INTO claims (player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at)
        VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, 0, 0)
        RETURNING id, player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at
      `,
      [playerId, username || playerId, source, packageId, txnId, txnNorm, submittedAt]
    );
    recordAdminActivity({
      eventType: "claim_submitted",
      playerId,
      username: username || playerId,
      details: {
        source,
        packageId,
        txnId: String(txnId || "").slice(0, 64)
      }
    });
    res.json({ ok: true, claim: mapClaimRow(insertResult.rows[0]) });
  } catch (error) {
    if (error?.code === "23505") {
      res.status(409).json({ ok: false, error: "Transaction id already claimed." });
      return;
    }
    console.error("Failed to submit claim", error);
    res.status(500).json({ ok: false, error: "Could not submit claim." });
  }
});

app.get("/api/claims", async (req, res) => {
  try {
    const playerId = sanitizePlayerId(req.query?.playerId);
    if (!playerId) {
      res.status(400).json({ ok: false, error: "Missing or invalid playerId." });
      return;
    }
    const result = await db.query(
      `
        SELECT id, player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at
        FROM claims
        WHERE player_id = $1
        ORDER BY submitted_at DESC, id DESC
      `,
      [playerId]
    );
    const claims = result.rows.map(mapClaimRow);
    res.json({
      ok: true,
      claims
    });
  } catch (error) {
    console.error("Failed to list claims", error);
    res.status(500).json({ ok: false, error: "Could not load claims." });
  }
});

app.get("/api/claims/credits", async (req, res) => {
  try {
    const playerId = sanitizePlayerId(req.query?.playerId);
    if (!playerId) {
      res.status(400).json({ ok: false, error: "Missing or invalid playerId." });
      return;
    }
    const result = await db.query(
      `
        SELECT id, player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at
        FROM claims
        WHERE player_id = $1
          AND status = 'approved'
          AND credited_at = 0
        ORDER BY submitted_at DESC, id DESC
      `,
      [playerId]
    );
    const claims = result.rows.map(mapClaimRow);
    res.json({ ok: true, claims });
  } catch (error) {
    console.error("Failed to list credits", error);
    res.status(500).json({ ok: false, error: "Could not load approved credits." });
  }
});

app.post("/api/claims/credits/:id/ack", async (req, res) => {
  const client = await db.connect();
  try {
    const claimId = String(req.params?.id || "").trim();
    const playerId = sanitizePlayerId(req.body?.playerId);
    if (!claimId || !playerId) {
      res.status(400).json({ ok: false, error: "Missing claim id or player id." });
      return;
    }
    await client.query("BEGIN");
    const lookup = await client.query(
      `
        SELECT id, player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at
        FROM claims
        WHERE id = $1 AND player_id = $2
        LIMIT 1
        FOR UPDATE
      `,
      [claimId, playerId]
    );
    if (!lookup.rowCount) {
      await client.query("ROLLBACK");
      res.status(404).json({ ok: false, error: "Claim not found." });
      return;
    }
    const claim = lookup.rows[0];
    if (claim.status !== "approved") {
      await client.query("ROLLBACK");
      res.status(409).json({ ok: false, error: "Claim is not approved." });
      return;
    }
    const normalizedPackId = normalizePackageId(claim.pack_id);
    const pack = STORE_PACKAGES[normalizedPackId];
    if (!pack || !Number.isFinite(Number(pack.funds)) || Number(pack.funds) <= 0) {
      await client.query("ROLLBACK");
      res.status(409).json({ ok: false, error: "Claim package is invalid." });
      return;
    }

    const now = Date.now();
    let claimRow = claim;
    let creditedNow = false;
    let creditedAmount = 0;
    let userRow = null;

    if (!Number(claim.credited_at)) {
      const updatedClaim = await client.query(
        `
          UPDATE claims
          SET credited_at = $1
          WHERE id = $2
            AND credited_at = 0
          RETURNING id, player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at
        `,
        [now, claimId]
      );
      if (updatedClaim.rowCount) {
        claimRow = updatedClaim.rows[0];
        creditedNow = true;
        creditedAmount = Math.round(Number(pack.funds) * 100) / 100;
        const updatedUser = await client.query(
          `
            UPDATE users
            SET savings_balance = LEAST($1, COALESCE(savings_balance, 0) + $2),
                balance_updated_at = $3
            WHERE player_id = $4
            RETURNING player_id, username, username_key, email, balance, shares, avg_cost, savings_balance, auto_savings_percent, last_seen_at, is_admin
          `,
          [MAX_ACCOUNT_SAVINGS, creditedAmount, now, playerId]
        );
        if (!updatedUser.rowCount) {
          await client.query("ROLLBACK");
          res.status(404).json({ ok: false, error: "User not found for claim credit." });
          return;
        }
        userRow = updatedUser.rows[0];
      }
    }

    if (!userRow) {
      const userLookup = await client.query(
        `
          SELECT player_id, username, username_key, email, balance, shares, avg_cost, savings_balance, auto_savings_percent, last_seen_at, is_admin
          FROM users
          WHERE player_id = $1
          LIMIT 1
        `,
        [playerId]
      );
      if (userLookup.rowCount) {
        userRow = userLookup.rows[0];
      }
    }

    await client.query("COMMIT");
    if (creditedNow) {
      recordAdminActivity({
        eventType: "claim_credited",
        playerId,
        username: sanitizeUsername(claim.username) || "",
        details: {
          claimId: String(claim.id || claimId),
          packId: normalizedPackId,
          creditedAmount
        }
      });
    }
    res.json({
      ok: true,
      claim: mapClaimRow(claimRow),
      creditedNow,
      creditedAmount,
      user: userRow ? mapUserRow(userRow) : null
    });
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    console.error("Failed to ack credit", error);
    res.status(500).json({ ok: false, error: "Could not acknowledge credit." });
  } finally {
    client.release();
  }
});

app.post("/api/users/sync", async (req, res) => {
  try {
    const sessionPlayerId = getSessionPlayerId(req);
    if (!sessionPlayerId) {
      res.status(401).json({ ok: false, error: "Login required." });
      return;
    }
    const playerId = sanitizePlayerId(req.body?.playerId);
    const username = sanitizeUsername(req.body?.username);
    const usernameKey = normalizeUsernameKey(username);
    if (!playerId || !username || !usernameKey) {
      res.status(400).json({ ok: false, error: "Invalid user sync payload." });
      return;
    }
    if (!(await assertUserNotBannedByPlayerId(playerId, res))) return;
    if (sessionPlayerId !== playerId) {
      res.status(403).json({ ok: false, error: "Session does not match this player." });
      return;
    }
    const conflict = await db.query(
      `
        SELECT player_id
        FROM users
        WHERE username_key = $1
          AND player_id <> $2
        LIMIT 1
      `,
      [usernameKey, playerId]
    );
    if (conflict.rowCount) {
      res.status(409).json({ ok: false, error: "Username already taken." });
      return;
    }
    const currentLookup = await db.query(
      `
        SELECT player_id, username, username_key, email, balance, shares, avg_cost, savings_balance, auto_savings_percent, balance_updated_at, last_seen_at, is_admin, sync_guard_bypass_until
        FROM users
        WHERE player_id = $1
        LIMIT 1
      `,
      [playerId]
    );
    if (!currentLookup.rowCount) {
      res.status(404).json({ ok: false, error: "Session account not found." });
      return;
    }
    const current = currentLookup.rows[0];
    const now = Date.now();
    const hasClientBalanceUpdatedAt = hasOwnField(req.body, "clientBalanceUpdatedAt");
    const rawClientBalanceUpdatedAt = hasClientBalanceUpdatedAt ? Number(req.body?.clientBalanceUpdatedAt) : 0;
    const clientBalanceUpdatedAt =
      Number.isFinite(rawClientBalanceUpdatedAt) && rawClientBalanceUpdatedAt > 0
        ? Math.floor(rawClientBalanceUpdatedAt)
        : 0;
    const hasBalance = hasOwnField(req.body, "balance");
    const hasShares = hasOwnField(req.body, "shares");
    const hasAvgCost = hasOwnField(req.body, "avgCost");
    const hasSavingsBalance = hasOwnField(req.body, "savingsBalance");
    const hasAutoSavingsPercent = hasOwnField(req.body, "autoSavingsPercent");
    const hasPortfolioPayload = hasBalance || hasShares || hasAvgCost || hasSavingsBalance || hasAutoSavingsPercent;

    const nextBalance = hasBalance
      ? sanitizeSyncNumber(req.body?.balance, { max: MAX_ACCOUNT_BALANCE, decimals: 2 })
      : Math.round(toNumber(current.balance) * 100) / 100;
    const nextShares = hasShares
      ? sanitizeSyncShares(req.body?.shares)
      : Math.max(0, Math.floor(toNumber(current.shares)));
    const nextAvgCost = hasAvgCost
      ? sanitizeSyncNumber(req.body?.avgCost, { max: MAX_ACCOUNT_AVG_COST, decimals: 4 })
      : Math.round(toNumber(current.avg_cost) * 10000) / 10000;
    const nextSavingsBalance = hasSavingsBalance
      ? sanitizeSyncNumber(req.body?.savingsBalance, { max: MAX_ACCOUNT_SAVINGS, decimals: 2 })
      : Math.round(toNumber(current.savings_balance) * 100) / 100;
    const nextAutoSavingsPercent = hasAutoSavingsPercent
      ? sanitizeSyncNumber(req.body?.autoSavingsPercent, { max: 100, decimals: 3 })
      : Math.round(toNumber(current.auto_savings_percent) * 1000) / 1000;

    if (nextBalance === null || nextShares === null || nextAvgCost === null || nextSavingsBalance === null || nextAutoSavingsPercent === null) {
      res.status(400).json({ ok: false, error: "Invalid portfolio sync payload." });
      return;
    }

    if (hasPortfolioPayload) {
      const currentBalance = Math.round(toNumber(current.balance) * 100) / 100;
      const currentShares = Math.max(0, Math.floor(toNumber(current.shares)));
      const serverBalanceUpdatedAt = Number(current.balance_updated_at) || 0;
      const syncGuardBypassUntil = Number(current.sync_guard_bypass_until) || 0;
      const syncGuardBypassed = syncGuardBypassUntil > now;
      if (clientBalanceUpdatedAt > 0 && serverBalanceUpdatedAt > 0 && clientBalanceUpdatedAt < serverBalanceUpdatedAt) {
        res.status(409).json({
          ok: false,
          error: "Server has a newer portfolio snapshot.",
          user: normalizeUserForClient(current)
        });
        return;
      }
      const lastUpdatedAt = Number(current.balance_updated_at) || Number(current.last_seen_at) || 0;
      const elapsedMs = Math.max(0, now - lastUpdatedAt);
      const allowedBalanceIncrease = getSyncAllowedIncrease(elapsedMs, BALANCE_SYNC_BURST_UP, BALANCE_SYNC_UP_PER_MIN);
      const allowedSharesIncrease = getSyncAllowedIncrease(elapsedMs, SHARES_SYNC_BURST_UP, SHARES_SYNC_UP_PER_MIN);

      if (!syncGuardBypassed && nextBalance > currentBalance + allowedBalanceIncrease) {
        res.status(409).json({
          ok: false,
          error: "Balance update rejected by server guard.",
          user: normalizeUserForClient(current)
        });
        return;
      }
      if (!syncGuardBypassed && nextShares > currentShares + allowedSharesIncrease) {
        res.status(409).json({
          ok: false,
          error: "Share update rejected by server guard.",
          user: normalizeUserForClient(current)
        });
        return;
      }
    }

    const updated = await db.query(
      `
        UPDATE users
        SET username = $2,
            username_key = $3,
            balance = $4,
            shares = $5,
            avg_cost = $6,
            savings_balance = $7,
            auto_savings_percent = $8,
            last_seen_at = $9,
            balance_updated_at = CASE WHEN $10 THEN $9 ELSE COALESCE(balance_updated_at, 0) END
        WHERE player_id = $1
        RETURNING player_id, username, username_key, email, balance, shares, avg_cost, savings_balance, auto_savings_percent, balance_updated_at, last_seen_at, is_admin
      `,
      [playerId, username, usernameKey, nextBalance, nextShares, nextAvgCost, nextSavingsBalance, nextAutoSavingsPercent, now, hasPortfolioPayload]
    );
    if (!updated.rowCount) {
      res.status(404).json({ ok: false, error: "Session account not found." });
      return;
    }
    if (hasPortfolioPayload) {
      recordBalanceAuditEvent({
        playerId,
        username,
        source: "user-sync",
        beforeBalance: toNumber(current.balance),
        afterBalance: nextBalance
      });
    }
    const user = mapUserRow(updated.rows[0]);
    if (hasPortfolioPayload) {
      queueLeaderboardUpdate("sync-balance");
    } else {
      queueLeaderboardUpdate("sync");
    }
    res.json({ ok: true, user });
  } catch (error) {
    if (error?.code === "23505") {
      res.status(409).json({ ok: false, error: "Username already taken." });
      return;
    }
    console.error("Failed to sync user profile", error);
    res.status(500).json({ ok: false, error: "Could not sync user profile." });
  }
});

app.get("/api/admin/device/owner-check", async (req, res) => {
  try {
    const userAgent = String(getRequestUserAgent(req) || "").toLowerCase();
    const isIphoneUserAgent = userAgent.includes("iphone");
    const deviceToken = getAdminDeviceToken(req);
    const tokenHash = deviceToken ? hashAdminDeviceToken(deviceToken) : "";
    if (!isIphoneUserAgent || !tokenHash) {
      res.json({ ok: true, ownerDevice: false, triggerVisible: false });
      return;
    }

    const deviceLookup = await db.query(
      `
        SELECT player_id, revoked_at, owner_lock
        FROM admin_trusted_devices
        WHERE token_hash = $1
        LIMIT 1
      `,
      [tokenHash]
    );
    if (!deviceLookup.rowCount) {
      res.json({ ok: true, ownerDevice: false, triggerVisible: false });
      return;
    }
    const device = deviceLookup.rows[0];
    const ownerLocked = device.owner_lock === true && Number(device.revoked_at) <= 0;
    if (!ownerLocked) {
      res.json({ ok: true, ownerDevice: false, triggerVisible: false });
      return;
    }
    const ownerPlayerId = sanitizePlayerId(device.player_id);
    if (!ownerPlayerId) {
      res.json({ ok: true, ownerDevice: false, triggerVisible: false });
      return;
    }
    const ownerUserLookup = await db.query(
      `
        SELECT is_admin, banned_at
        FROM users
        WHERE player_id = $1
        LIMIT 1
      `,
      [ownerPlayerId]
    );
    if (!ownerUserLookup.rowCount) {
      res.json({ ok: true, ownerDevice: false, triggerVisible: false });
      return;
    }
    const ownerUser = ownerUserLookup.rows[0];
    const ownerIsValid = ownerUser.is_admin === true && !isBannedTimestamp(ownerUser.banned_at);
    res.json({ ok: true, ownerDevice: ownerIsValid, triggerVisible: ownerIsValid });
  } catch (error) {
    console.error("Failed owner device check", error);
    res.status(500).json({ ok: false, error: "Could not verify owner device." });
  }
});

app.get("/api/admin/claims", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const statusFilter = String(req.query?.status || "").trim().toLowerCase();
    const result = statusFilter
      ? await db.query(
          `
            SELECT c.id, c.player_id, c.username, c.source, c.pack_id, c.txn_id, c.txn_norm, c.status, c.submitted_at, c.reviewed_at, c.credited_at, u.email
            FROM claims c
            LEFT JOIN users u ON u.player_id = c.player_id
            WHERE status = $1
            ORDER BY c.submitted_at DESC, c.id DESC
          `,
          [statusFilter]
        )
      : await db.query(
          `
            SELECT c.id, c.player_id, c.username, c.source, c.pack_id, c.txn_id, c.txn_norm, c.status, c.submitted_at, c.reviewed_at, c.credited_at, u.email
            FROM claims c
            LEFT JOIN users u ON u.player_id = c.player_id
            ORDER BY c.submitted_at DESC, c.id DESC
          `
        );
    const claims = result.rows.map(mapClaimRow);
    res.json({ ok: true, claims, trustedDevice: adminAccess.device || null });
  } catch (error) {
    console.error("Failed to load admin claims", error);
    res.status(500).json({ ok: false, error: "Could not load admin claims." });
  }
});

app.get("/api/admin/users", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const result = await db.query(
      `
        SELECT
          u.player_id,
          u.username,
          u.username_key,
          u.email,
          u.balance,
          u.shares,
          u.avg_cost,
          u.savings_balance,
          u.auto_savings_percent,
          u.last_seen_at,
          u.is_admin,
          u.banned_at,
          u.banned_reason,
          u.muted_until,
          u.muted_reason,
          u.sync_guard_bypass_until,
          (u.password_hash IS NOT NULL AND u.password_hash <> '') AS has_password,
          COALESCE(claims.total_claims, 0) AS total_claims,
          COALESCE(claims.pending_claims, 0) AS pending_claims,
          COALESCE(claims.approved_claims, 0) AS approved_claims,
          COALESCE(wins.total_wins, 0) AS total_wins,
          COALESCE(wins.last_win_at, 0) AS last_win_at
        FROM users u
        LEFT JOIN (
          SELECT
            player_id,
            COUNT(*)::int AS total_claims,
            COUNT(*) FILTER (WHERE status = 'pending')::int AS pending_claims,
            COUNT(*) FILTER (WHERE status = 'approved')::int AS approved_claims
          FROM claims
          GROUP BY player_id
        ) claims ON claims.player_id = u.player_id
        LEFT JOIN (
          SELECT
            player_id,
            COUNT(*)::int AS total_wins,
            MAX(submitted_at)::bigint AS last_win_at
          FROM live_wins
          GROUP BY player_id
        ) wins ON wins.player_id = u.player_id
        ORDER BY u.last_seen_at DESC, u.player_id DESC
      `
    );
    const users = result.rows.map(mapAdminUserRow);
    res.json({ ok: true, users, trustedDevice: adminAccess.device || null });
  } catch (error) {
    console.error("Failed to load admin users", error);
    res.status(500).json({ ok: false, error: "Could not load users." });
  }
});

app.get("/api/admin/feedback", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const rawLimit = Number(req.query?.limit);
    const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(200, Math.floor(rawLimit))) : 100;
    const statusFilter = String(req.query?.status || "").trim().toLowerCase();
    const result =
      statusFilter && ["open", "reviewed", "closed"].includes(statusFilter)
        ? await db.query(
            `
              SELECT id, player_id, username, category, message, status, submitted_at, reviewed_at
              FROM feedback_submissions
              WHERE status = $1
              ORDER BY submitted_at DESC, id DESC
              LIMIT $2
            `,
            [statusFilter, limit]
          )
        : await db.query(
            `
              SELECT id, player_id, username, category, message, status, submitted_at, reviewed_at
              FROM feedback_submissions
              ORDER BY submitted_at DESC, id DESC
              LIMIT $1
            `,
            [limit]
          );
    res.json({ ok: true, feedback: result.rows.map(mapFeedbackRow), trustedDevice: adminAccess.device || null });
  } catch (error) {
    console.error("Failed to load admin feedback", error);
    res.status(500).json({ ok: false, error: "Could not load feedback." });
  }
});

app.get("/api/admin/messages", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const rawLimit = Number(req.query?.limit);
    const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(300, Math.floor(rawLimit))) : 120;
    const result = await db.query(
      `
        SELECT
          m.id,
          m.target_player_id,
          m.target_username,
          m.message,
          m.created_by,
          m.created_at,
          m.active,
          COALESCE(COUNT(r.player_id), 0)::int AS read_count
        FROM admin_popup_messages m
        LEFT JOIN admin_popup_message_reads r ON r.message_id = m.id
        GROUP BY m.id
        ORDER BY m.created_at DESC, m.id DESC
        LIMIT $1
      `,
      [limit]
    );
    res.json({
      ok: true,
      messages: result.rows.map(mapAdminPopupMessageRow),
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    console.error("Failed to load admin popup messages", error);
    res.status(500).json({ ok: false, error: "Could not load admin messages." });
  }
});

app.post("/api/admin/messages", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const message = sanitizeAdminPopupMessage(req.body?.message);
    if (!message) {
      res.status(400).json({ ok: false, error: "Message cannot be empty." });
      return;
    }
    const targetPlayerId = sanitizePlayerId(req.body?.targetPlayerId);
    let targetUsername = "";
    if (targetPlayerId) {
      const targetLookup = await db.query(
        `
          SELECT player_id, username
          FROM users
          WHERE player_id = $1
          LIMIT 1
        `,
        [targetPlayerId]
      );
      if (!targetLookup.rowCount) {
        res.status(404).json({ ok: false, error: "Target user not found." });
        return;
      }
      targetUsername = sanitizeUsername(targetLookup.rows[0]?.username) || "";
    }
    const inserted = await db.query(
      `
        INSERT INTO admin_popup_messages (
          target_player_id,
          target_username,
          message,
          created_by,
          created_at,
          active
        )
        VALUES ($1, $2, $3, $4, $5, true)
        RETURNING id, target_player_id, target_username, message, created_by, created_at, active
      `,
      [targetPlayerId || "", targetUsername, message, adminAccess.playerId, Date.now()]
    );
    await db.query(
      `
        DELETE FROM admin_popup_messages
        WHERE id IN (
          SELECT id
          FROM admin_popup_messages
          ORDER BY created_at DESC, id DESC
          OFFSET $1
        )
      `,
      [MAX_ADMIN_POPUP_MESSAGES]
    );
    await db.query(
      `
        DELETE FROM admin_popup_message_reads r
        WHERE NOT EXISTS (
          SELECT 1
          FROM admin_popup_messages m
          WHERE m.id = r.message_id
        )
      `
    );
    await trimAdminPopupReads();
    const messageRow = mapAdminPopupMessageRow(inserted.rows[0]);
    recordAdminActivity({
      eventType: "popup_message_sent",
      actorPlayerId: adminAccess.playerId,
      playerId: targetPlayerId || "",
      username: targetUsername || "",
      details: {
        messageId: messageRow.id,
        target: targetPlayerId ? "individual" : "broadcast",
        messagePreview: message.slice(0, 80)
      }
    });
    res.json({
      ok: true,
      message: messageRow,
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    console.error("Failed to send admin popup message", error);
    res.status(500).json({ ok: false, error: "Could not send admin message." });
  }
});

app.post("/api/admin/messages/:id/clear", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const messageId = String(req.params?.id || "").trim();
    if (!/^\d{1,18}$/.test(messageId)) {
      res.status(400).json({ ok: false, error: "Invalid message id." });
      return;
    }
    const cleared = await db.query(
      `
        UPDATE admin_popup_messages
        SET active = false
        WHERE id = $1
        RETURNING id, target_player_id, target_username, message, created_by, created_at, active
      `,
      [messageId]
    );
    if (!cleared.rowCount) {
      res.status(404).json({ ok: false, error: "Message not found." });
      return;
    }
    await db.query("DELETE FROM admin_popup_message_reads WHERE message_id = $1", [messageId]);
    const clearedMessage = mapAdminPopupMessageRow(cleared.rows[0]);
    recordAdminActivity({
      eventType: "popup_message_cleared",
      actorPlayerId: adminAccess.playerId,
      playerId: clearedMessage.targetPlayerId,
      username: clearedMessage.targetUsername,
      details: {
        messageId: clearedMessage.id
      }
    });
    res.json({
      ok: true,
      cleared: clearedMessage,
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    console.error("Failed to clear admin popup message", error);
    res.status(500).json({ ok: false, error: "Could not clear admin message." });
  }
});

app.get("/api/admin/activity", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const rawLimit = Number(req.query?.limit);
    const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(250, Math.floor(rawLimit))) : 120;
    const result = await db.query(
      `
        SELECT id, event_type, player_id, username, actor_player_id, details_json, created_at
        FROM admin_activity_events
        ORDER BY created_at DESC, id DESC
        LIMIT $1
      `,
      [limit]
    );
    res.json({
      ok: true,
      events: result.rows.map(mapAdminActivityRow),
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    console.error("Failed to load admin activity", error);
    res.status(500).json({ ok: false, error: "Could not load admin activity." });
  }
});

app.post("/api/admin/activity/clear", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const removed = await db.query("DELETE FROM admin_activity_events");
    res.json({
      ok: true,
      cleared: { count: Number(removed.rowCount) || 0 },
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    console.error("Failed to clear admin activity", error);
    res.status(500).json({ ok: false, error: "Could not clear admin activity." });
  }
});

app.get("/api/admin/health", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const now = Date.now();
    const dayAgo = now - 24 * 60 * 60 * 1000;
    const dbPing = await db.query("SELECT 1 AS ok");
    const totals = await db.query(
      `
        SELECT
          COUNT(*) FILTER (WHERE kind = 'error')::int AS errors_24h,
          COUNT(*) FILTER (WHERE kind = 'rate_limit')::int AS rate_limits_24h
        FROM system_runtime_events
        WHERE created_at >= $1
      `,
      [dayAgo]
    );
    const latestBackup = await db.query(
      `
        SELECT id, created_at, created_by, summary_json
        FROM admin_backups
        ORDER BY created_at DESC, id DESC
        LIMIT 1
      `
    );
    const row = totals.rows[0] || {};
    const backupRow = latestBackup.rows[0] || null;
    res.json({
      ok: true,
      health: {
        apiStatus: "ok",
        dbStatus: dbPing.rowCount > 0 ? "ok" : "down",
        uptimeSeconds: Math.max(0, Math.floor(process.uptime())),
        errors24h: Number(row.errors_24h) || 0,
        rateLimitHits24h: Number(row.rate_limits_24h) || 0,
        rateLimitBuckets: rateLimitStore.size,
        lastBackup: backupRow
          ? {
              id: String(backupRow.id || ""),
              createdAt: Number(backupRow.created_at) || 0,
              createdBy: String(backupRow.created_by || ""),
              summary: backupRow.summary_json && typeof backupRow.summary_json === "object" ? backupRow.summary_json : {}
            }
          : null
      },
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    console.error("Failed to load admin health", error);
    res.status(500).json({ ok: false, error: "Could not load system health." });
  }
});

app.get("/api/admin/fraud-flags", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const now = Date.now();
    const dayAgo = now - 24 * 60 * 60 * 1000;
    const tenMinutesAgo = now - 10 * 60 * 1000;
    const fraudStateResult = await db.query(
      `
        SELECT cleared_at
        FROM admin_fraud_flags_state
        WHERE id = 1
        LIMIT 1
      `
    );
    const clearedAt = Number(fraudStateResult.rows?.[0]?.cleared_at) || 0;
    const dayWindowStart = Math.max(dayAgo, clearedAt);
    const tenMinuteWindowStart = Math.max(tenMinutesAgo, clearedAt);
    const flags = [];

    const claimSpam = await db.query(
      `
        SELECT player_id, username, COUNT(*)::int AS claim_count, MAX(submitted_at)::bigint AS last_at
        FROM claims
        WHERE submitted_at >= $1
        GROUP BY player_id, username
        HAVING COUNT(*) >= 5
        ORDER BY claim_count DESC, last_at DESC
        LIMIT 25
      `,
      [dayWindowStart]
    );
    for (const row of claimSpam.rows) {
      flags.push({
        type: "claims_spam",
        severity: "high",
        playerId: String(row.player_id || ""),
        username: sanitizeUsername(row.username) || "Unknown",
        value: Number(row.claim_count) || 0,
        note: "High claim volume in the last 24h",
        createdAt: Number(row.last_at) || 0
      });
    }

    const feedbackSpam = await db.query(
      `
        SELECT player_id, username, COUNT(*)::int AS feedback_count, MAX(submitted_at)::bigint AS last_at
        FROM feedback_submissions
        WHERE submitted_at >= $1
        GROUP BY player_id, username
        HAVING COUNT(*) >= 6
        ORDER BY feedback_count DESC, last_at DESC
        LIMIT 25
      `,
      [tenMinuteWindowStart]
    );
    for (const row of feedbackSpam.rows) {
      flags.push({
        type: "feedback_spam",
        severity: "medium",
        playerId: String(row.player_id || ""),
        username: sanitizeUsername(row.username) || "Unknown",
        value: Number(row.feedback_count) || 0,
        note: "High feedback volume in the last 10 minutes",
        createdAt: Number(row.last_at) || 0
      });
    }

    const rapidBalanceJumps = await db.query(
      `
        SELECT player_id, username, source, before_balance, after_balance, delta, created_at
        FROM balance_audit_events
        WHERE created_at >= $1
          AND ABS(delta) >= 10000
        ORDER BY created_at DESC, id DESC
        LIMIT 30
      `,
      [dayWindowStart]
    );
    for (const row of rapidBalanceJumps.rows) {
      flags.push({
        type: "rapid_balance_jump",
        severity: "high",
        playerId: String(row.player_id || ""),
        username: sanitizeUsername(row.username) || "Unknown",
        value: Math.round(toNumber(row.delta) * 100) / 100,
        note: `Balance changed by ${Math.round(toNumber(row.delta) * 100) / 100} via ${String(row.source || "unknown")}`,
        createdAt: Number(row.created_at) || 0
      });
    }

    const balanceBurst = await db.query(
      `
        SELECT player_id, username, COUNT(*)::int AS event_count, MAX(created_at)::bigint AS last_at
        FROM balance_audit_events
        WHERE created_at >= $1
        GROUP BY player_id, username
        HAVING COUNT(*) >= 12
        ORDER BY event_count DESC, last_at DESC
        LIMIT 25
      `,
      [tenMinuteWindowStart]
    );
    for (const row of balanceBurst.rows) {
      flags.push({
        type: "balance_burst",
        severity: "medium",
        playerId: String(row.player_id || ""),
        username: sanitizeUsername(row.username) || "Unknown",
        value: Number(row.event_count) || 0,
        note: "Very frequent balance updates in 10 minutes",
        createdAt: Number(row.last_at) || 0
      });
    }

    flags.sort((a, b) => Number(b.createdAt) - Number(a.createdAt));
    res.json({
      ok: true,
      flags,
      clearedAt,
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    console.error("Failed to load fraud flags", error);
    res.status(500).json({ ok: false, error: "Could not load fraud flags." });
  }
});

app.post("/api/admin/fraud-flags/clear", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const now = Date.now();
    const actorPlayerId = sanitizePlayerId(adminAccess.playerId);
    await db.query(
      `
        INSERT INTO admin_fraud_flags_state (id, cleared_at, cleared_by, updated_at)
        VALUES (1, $1, $2, $1)
        ON CONFLICT (id)
        DO UPDATE
        SET cleared_at = EXCLUDED.cleared_at,
            cleared_by = EXCLUDED.cleared_by,
            updated_at = EXCLUDED.updated_at
      `,
      [now, actorPlayerId]
    );
    recordAdminActivity({
      eventType: "fraud_flags_cleared",
      actorPlayerId: adminAccess.playerId,
      details: {
        clearedAt: now
      }
    });
    res.json({
      ok: true,
      cleared: { at: now },
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    console.error("Failed to clear fraud flags", error);
    res.status(500).json({ ok: false, error: "Could not clear fraud flags." });
  }
});

app.get("/api/admin/backups", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const rawLimit = Number(req.query?.limit);
    const limit = Number.isFinite(rawLimit) ? Math.max(1, Math.min(50, Math.floor(rawLimit))) : 15;
    const result = await db.query(
      `
        SELECT id, created_at, created_by, summary_json
        FROM admin_backups
        ORDER BY created_at DESC, id DESC
        LIMIT $1
      `,
      [limit]
    );
    res.json({
      ok: true,
      backups: result.rows.map(mapAdminBackupRow),
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    console.error("Failed to load admin backups", error);
    res.status(500).json({ ok: false, error: "Could not load backups." });
  }
});

app.post("/api/admin/backups/create", async (req, res) => {
  const client = await db.connect();
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const now = Date.now();
    await client.query("BEGIN");
    const usersResult = await client.query(
      `
        SELECT player_id, username, username_key, email, password_hash, email_verified_at, is_admin, balance, shares, avg_cost,
               savings_balance, auto_savings_percent, balance_updated_at, last_seen_at, banned_at, banned_reason, muted_until, muted_reason,
               sync_guard_bypass_until
        FROM users
        ORDER BY player_id ASC
      `
    );
    const claimsResult = await client.query(
      `
        SELECT id, player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at
        FROM claims
        ORDER BY id ASC
      `
    );
    const winsResult = await client.query(
      `
        SELECT id, player_id, username, vip, game, amount, multiplier, submitted_at
        FROM live_wins
        ORDER BY id ASC
      `
    );
    const feedbackResult = await client.query(
      `
        SELECT id, player_id, username, category, message, status, submitted_at, reviewed_at
        FROM feedback_submissions
        ORDER BY id ASC
      `
    );
    const devicesResult = await client.query(
      `
        SELECT id, player_id, token_hash, label, created_at, last_seen_at, last_ip, last_user_agent, revoked_at, owner_lock
        FROM admin_trusted_devices
        ORDER BY id ASC
      `
    );
    const popupMessagesResult = await client.query(
      `
        SELECT id, target_player_id, target_username, message, created_by, created_at, active
        FROM admin_popup_messages
        ORDER BY id ASC
      `
    );
    const popupReadsResult = await client.query(
      `
        SELECT message_id, player_id, read_at
        FROM admin_popup_message_reads
        ORDER BY message_id ASC, player_id ASC
      `
    );
    const summary = {
      users: usersResult.rowCount,
      claims: claimsResult.rowCount,
      liveWins: winsResult.rowCount,
      feedback: feedbackResult.rowCount,
      trustedDevices: devicesResult.rowCount,
      popupMessages: popupMessagesResult.rowCount,
      popupReads: popupReadsResult.rowCount
    };
    const data = {
      users: usersResult.rows,
      claims: claimsResult.rows,
      liveWins: winsResult.rows,
      feedback: feedbackResult.rows,
      trustedDevices: devicesResult.rows,
      popupMessages: popupMessagesResult.rows,
      popupReads: popupReadsResult.rows
    };
    const inserted = await client.query(
      `
        INSERT INTO admin_backups (created_by, summary_json, data_json, created_at)
        VALUES ($1, $2::jsonb, $3::jsonb, $4)
        RETURNING id, created_at, created_by, summary_json
      `,
      [adminAccess.playerId, JSON.stringify(summary), JSON.stringify(data), now]
    );
    await client.query(
      `
        DELETE FROM admin_backups
        WHERE id IN (
          SELECT id
          FROM admin_backups
          ORDER BY created_at DESC, id DESC
          OFFSET $1
        )
      `,
      [MAX_ADMIN_BACKUPS]
    );
    await client.query("COMMIT");
    recordAdminActivity({
      eventType: "backup_created",
      actorPlayerId: adminAccess.playerId,
      details: { backupId: String(inserted.rows[0]?.id || ""), ...summary }
    });
    res.json({
      ok: true,
      backup: mapAdminBackupRow(inserted.rows[0]),
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    console.error("Failed to create admin backup", error);
    res.status(500).json({ ok: false, error: "Could not create backup." });
  } finally {
    client.release();
  }
});

app.post("/api/admin/backups/:id/restore", async (req, res) => {
  const client = await db.connect();
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const backupId = String(req.params?.id || "").trim();
    if (!/^\d{1,18}$/.test(backupId)) {
      res.status(400).json({ ok: false, error: "Invalid backup id." });
      return;
    }
    const adminCode = normalizeAdminCode(getAdminCode(req));
    const expectedCode = normalizeAdminCode(ADMIN_CODE);
    if (!adminCode || adminCode !== expectedCode) {
      res.status(401).json({ ok: false, error: "Admin code is required for restore." });
      return;
    }
    const confirmText = String(req.body?.confirmText || "").trim();
    if (confirmText !== "RESTORE BACKUP") {
      res.status(400).json({ ok: false, error: "Confirmation text must be RESTORE BACKUP." });
      return;
    }

    const backupLookup = await client.query(
      `
        SELECT id, created_at, created_by, summary_json, data_json
        FROM admin_backups
        WHERE id = $1
        LIMIT 1
      `,
      [backupId]
    );
    if (!backupLookup.rowCount) {
      res.status(404).json({ ok: false, error: "Backup not found." });
      return;
    }
    const backupRow = backupLookup.rows[0];
    const data = backupRow.data_json && typeof backupRow.data_json === "object" ? backupRow.data_json : {};
    const users = Array.isArray(data.users) ? data.users : [];
    const claims = Array.isArray(data.claims) ? data.claims : [];
    const liveWins = Array.isArray(data.liveWins) ? data.liveWins : [];
    const feedback = Array.isArray(data.feedback) ? data.feedback : [];
    const trustedDevices = Array.isArray(data.trustedDevices) ? data.trustedDevices : [];
    const popupMessages = Array.isArray(data.popupMessages) ? data.popupMessages : [];
    const popupReads = Array.isArray(data.popupReads) ? data.popupReads : [];
    const adminInBackup = users.some((user) => sanitizePlayerId(user?.player_id) === sanitizePlayerId(adminAccess.playerId));
    if (!adminInBackup) {
      res.status(409).json({
        ok: false,
        error: "Restore blocked: current admin account is missing from that backup."
      });
      return;
    }
    const currentSessionId = String(req.sessionID || "").trim();

    await client.query("BEGIN");
    if (currentSessionId) {
      await client.query("DELETE FROM user_sessions WHERE sid <> $1", [currentSessionId]);
    } else {
      await client.query("DELETE FROM user_sessions");
    }
    await client.query("DELETE FROM claims");
    await client.query("DELETE FROM live_wins");
    await client.query("DELETE FROM feedback_submissions");
    await client.query("DELETE FROM admin_trusted_devices");
    await client.query("DELETE FROM admin_popup_message_reads");
    await client.query("DELETE FROM admin_popup_messages");
    await client.query("DELETE FROM users");

    for (const user of users) {
      const safePlayerId = sanitizePlayerId(user?.player_id);
      if (!safePlayerId) continue;
      const safeUsername = sanitizeUsername(user?.username) || "Player";
      const safeUsernameKey =
        normalizeUsernameKey(user?.username_key || safeUsername) ||
        normalizeUsernameKey(`${safeUsername}_${safePlayerId.slice(-6)}`) ||
        safePlayerId.toLowerCase();
      await client.query(
        `
          INSERT INTO users (
            player_id, username, username_key, email, password_hash, email_verified_at, is_admin, balance, shares, avg_cost,
            savings_balance, auto_savings_percent, balance_updated_at, last_seen_at, banned_at, banned_reason, muted_until, muted_reason,
            sync_guard_bypass_until
          )
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
        `,
        [
          safePlayerId,
          safeUsername,
          safeUsernameKey,
          sanitizeEmail(user?.email),
          String(user?.password_hash || ""),
          Number(user?.email_verified_at) || 0,
          user?.is_admin === true,
          Math.round(toNumber(user?.balance) * 100) / 100,
          Math.max(0, Math.floor(toNumber(user?.shares))),
          Math.round(toNumber(user?.avg_cost) * 10000) / 10000,
          Math.round(toNumber(user?.savings_balance) * 100) / 100,
          Math.round(toNumber(user?.auto_savings_percent) * 1000) / 1000,
          Number(user?.balance_updated_at) || 0,
          Number(user?.last_seen_at) || 0,
          Number(user?.banned_at) || 0,
          sanitizeBanReason(user?.banned_reason),
          Number(user?.muted_until) || 0,
          sanitizeBanReason(user?.muted_reason),
          Number(user?.sync_guard_bypass_until) || 0
        ]
      );
    }

    for (const claim of claims) {
      await client.query(
        `
          INSERT INTO claims (id, player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        `,
        [
          Number(claim?.id) || null,
          sanitizePlayerId(claim?.player_id),
          sanitizeUsername(claim?.username) || "Player",
          sanitizeClaimSource(claim?.source),
          normalizePackageId(claim?.pack_id || claim?.packId) || "",
          String(claim?.txn_id || ""),
          normalizeTxn(claim?.txn_norm || claim?.txn_id),
          String(claim?.status || "pending"),
          Number(claim?.submitted_at) || 0,
          Number(claim?.reviewed_at) || 0,
          Number(claim?.credited_at) || 0
        ]
      );
    }

    for (const win of liveWins) {
      await client.query(
        `
          INSERT INTO live_wins (id, player_id, username, vip, game, amount, multiplier, submitted_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        `,
        [
          Number(win?.id) || null,
          sanitizePlayerId(win?.player_id),
          sanitizeUsername(win?.username) || "Player",
          win?.vip === true,
          sanitizeGameLabel(win?.game),
          Math.round(toNumber(win?.amount) * 100) / 100,
          win?.multiplier === null || win?.multiplier === undefined
            ? null
            : Math.round(toNumber(win?.multiplier) * 100) / 100,
          Number(win?.submitted_at) || 0
        ]
      );
    }

    for (const item of feedback) {
      await client.query(
        `
          INSERT INTO feedback_submissions (id, player_id, username, category, message, status, submitted_at, reviewed_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        `,
        [
          Number(item?.id) || null,
          sanitizePlayerId(item?.player_id),
          sanitizeUsername(item?.username) || "Player",
          sanitizeFeedbackCategory(item?.category),
          sanitizeFeedbackMessage(item?.message),
          String(item?.status || "open"),
          Number(item?.submitted_at) || 0,
          Number(item?.reviewed_at) || 0
        ]
      );
    }

    for (const device of trustedDevices) {
      await client.query(
        `
          INSERT INTO admin_trusted_devices (id, player_id, token_hash, label, created_at, last_seen_at, last_ip, last_user_agent, revoked_at, owner_lock)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        `,
        [
          Number(device?.id) || null,
          sanitizePlayerId(device?.player_id),
          String(device?.token_hash || "").trim(),
          sanitizeAdminDeviceLabel(device?.label),
          Number(device?.created_at) || 0,
          Number(device?.last_seen_at) || 0,
          String(device?.last_ip || "").slice(0, 128),
          String(device?.last_user_agent || "").slice(0, 255),
          Number(device?.revoked_at) || 0,
          device?.owner_lock === true
        ]
      );
    }

    for (const popup of popupMessages) {
      const safeMessage = sanitizeAdminPopupMessage(popup?.message);
      if (!safeMessage) continue;
      await client.query(
        `
          INSERT INTO admin_popup_messages (id, target_player_id, target_username, message, created_by, created_at, active)
          VALUES ($1, $2, $3, $4, $5, $6, $7)
        `,
        [
          Number(popup?.id) || null,
          sanitizePlayerId(popup?.target_player_id) || "",
          sanitizeUsername(popup?.target_username) || "",
          safeMessage,
          sanitizePlayerId(popup?.created_by) || "",
          Number(popup?.created_at) || 0,
          popup?.active !== false
        ]
      );
    }

    for (const popupRead of popupReads) {
      const safePlayerId = sanitizePlayerId(popupRead?.player_id);
      const safeMessageId = Number(popupRead?.message_id);
      if (!safePlayerId || !Number.isFinite(safeMessageId) || safeMessageId <= 0) continue;
      await client.query(
        `
          INSERT INTO admin_popup_message_reads (message_id, player_id, read_at)
          VALUES ($1, $2, $3)
          ON CONFLICT (message_id, player_id)
          DO UPDATE SET read_at = EXCLUDED.read_at
        `,
        [safeMessageId, safePlayerId, Number(popupRead?.read_at) || 0]
      );
    }

    await client.query(
      `
        SELECT setval(pg_get_serial_sequence('claims', 'id'), COALESCE((SELECT MAX(id) FROM claims), 1), true)
      `
    );
    await client.query(
      `
        SELECT setval(pg_get_serial_sequence('live_wins', 'id'), COALESCE((SELECT MAX(id) FROM live_wins), 1), true)
      `
    );
    await client.query(
      `
        SELECT setval(pg_get_serial_sequence('feedback_submissions', 'id'), COALESCE((SELECT MAX(id) FROM feedback_submissions), 1), true)
      `
    );
    await client.query(
      `
        SELECT setval(pg_get_serial_sequence('admin_trusted_devices', 'id'), COALESCE((SELECT MAX(id) FROM admin_trusted_devices), 1), true)
      `
    );
    await client.query(
      `
        SELECT setval(pg_get_serial_sequence('admin_popup_messages', 'id'), COALESCE((SELECT MAX(id) FROM admin_popup_messages), 1), true)
      `
    );

    await client.query(
      `
        UPDATE users
        SET is_admin = true, banned_at = 0, banned_reason = ''
        WHERE player_id = $1
      `,
      [adminAccess.playerId]
    );
    await client.query("COMMIT");
    queueLeaderboardUpdate("backup-restore");
    recordAdminActivity({
      eventType: "backup_restored",
      actorPlayerId: adminAccess.playerId,
      details: {
        backupId: String(backupRow.id || backupId),
        users: users.length,
        claims: claims.length,
        liveWins: liveWins.length,
        feedback: feedback.length,
        popupMessages: popupMessages.length
      }
    });
    res.json({
      ok: true,
      restored: {
        backupId: String(backupRow.id || backupId),
        users: users.length,
        claims: claims.length,
        liveWins: liveWins.length,
        feedback: feedback.length,
        popupMessages: popupMessages.length
      },
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    console.error("Failed to restore backup", error);
    res.status(500).json({ ok: false, error: "Could not restore backup." });
  } finally {
    client.release();
  }
});

app.post("/api/admin/claims/:id/decision", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const claimId = String(req.params?.id || "");
    const decision = String(req.body?.decision || "").toLowerCase();
    if (!["approve", "reject"].includes(decision)) {
      res.status(400).json({ ok: false, error: "Decision must be approve or reject." });
      return;
    }
    const updated = await db.query(
      `
        UPDATE claims
        SET status = $1, reviewed_at = $2
        WHERE id = $3
          AND status = 'pending'
        RETURNING id, player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at
      `,
      [decision === "approve" ? "approved" : "rejected", Date.now(), claimId]
    );
    if (!updated.rowCount) {
      const exists = await db.query("SELECT id FROM claims WHERE id = $1 LIMIT 1", [claimId]);
      if (!exists.rowCount) {
        res.status(404).json({ ok: false, error: "Claim not found." });
        return;
      }
      res.status(409).json({ ok: false, error: "Claim is already reviewed." });
      return;
    }
    recordAdminActivity({
      eventType: decision === "approve" ? "claim_approved" : "claim_rejected",
      playerId: updated.rows[0]?.player_id || "",
      username: updated.rows[0]?.username || "",
      actorPlayerId: adminAccess.playerId,
      details: {
        claimId,
        decision
      }
    });
    res.json({ ok: true, claim: mapClaimRow(updated.rows[0]), trustedDevice: adminAccess.device || null });
  } catch (error) {
    console.error("Failed to update claim decision", error);
    res.status(500).json({ ok: false, error: "Could not update claim decision." });
  }
});

app.post("/api/admin/device/trust", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: true });
    if (!adminAccess) return;
    const deviceToken = getAdminDeviceToken(req);
    if (!deviceToken) {
      res.status(400).json({ ok: false, error: "Missing admin device token." });
      return;
    }
    const tokenHash = hashAdminDeviceToken(deviceToken);
    const label = sanitizeAdminDeviceLabel(req.body?.label);
    const now = Date.now();
    const upsert = await db.query(
      `
        INSERT INTO admin_trusted_devices (player_id, token_hash, label, created_at, last_seen_at, last_ip, last_user_agent, revoked_at, owner_lock)
        VALUES ($1, $2, $3, $4, $4, $5, $6, 0, false)
        ON CONFLICT (token_hash)
        DO UPDATE SET
          player_id = EXCLUDED.player_id,
          label = EXCLUDED.label,
          last_seen_at = EXCLUDED.last_seen_at,
          last_ip = EXCLUDED.last_ip,
          last_user_agent = EXCLUDED.last_user_agent,
          revoked_at = 0
        RETURNING id, player_id, label, created_at, last_seen_at, last_ip, last_user_agent, revoked_at, owner_lock
      `,
      [adminAccess.playerId, tokenHash, label, now, getRequestIp(req), getRequestUserAgent(req)]
    );
    recordAdminActivity({
      eventType: "device_trusted",
      actorPlayerId: adminAccess.playerId,
      details: { deviceId: String(upsert.rows[0]?.id || "") }
    });
    res.json({
      ok: true,
      bootstrap: adminAccess.bootstrap === true,
      trustedDevice: mapAdminDeviceRow(upsert.rows[0])
    });
  } catch (error) {
    console.error("Failed to trust admin device", error);
    res.status(500).json({ ok: false, error: "Could not trust this device." });
  }
});

app.get("/api/admin/devices", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const currentToken = getAdminDeviceToken(req);
    const currentTokenHash = currentToken ? hashAdminDeviceToken(currentToken) : "";
    const ownerLockLookup = await db.query(
      `
        SELECT token_hash
        FROM admin_trusted_devices
        WHERE owner_lock = true
          AND revoked_at = 0
        ORDER BY id DESC
        LIMIT 1
      `
    );
    const ownerTokenHash = ownerLockLookup.rowCount ? String(ownerLockLookup.rows[0].token_hash || "") : "";
    const result = await db.query(
      `
        SELECT id, player_id, token_hash, label, created_at, last_seen_at, last_ip, last_user_agent, revoked_at, owner_lock
        FROM admin_trusted_devices
        WHERE player_id = $1
          AND token_hash IS NOT NULL
          AND token_hash <> ''
        ORDER BY created_at DESC, id DESC
      `,
      [adminAccess.playerId]
    );
    const devices = result.rows.map((row) => ({
      ...mapAdminDeviceRow(row),
      current: Boolean(currentTokenHash && row.token_hash === currentTokenHash),
      ownerLocked: Boolean(ownerTokenHash && row.token_hash === ownerTokenHash)
    }));
    res.json({
      ok: true,
      devices,
      ownerLockEnabled: Boolean(ownerTokenHash)
    });
  } catch (error) {
    console.error("Failed to load admin devices", error);
    res.status(500).json({ ok: false, error: "Could not load trusted devices." });
  }
});

app.post("/api/admin/devices/:id/set-owner-lock", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const deviceId = Number(req.params?.id);
    if (!Number.isInteger(deviceId) || deviceId <= 0) {
      res.status(400).json({ ok: false, error: "Invalid device id." });
      return;
    }
    const targetLookup = await db.query(
      `
        SELECT id, player_id, token_hash, label, created_at, last_seen_at, last_ip, last_user_agent, revoked_at, owner_lock
        FROM admin_trusted_devices
        WHERE id = $1
          AND player_id = $2
          AND revoked_at = 0
        LIMIT 1
      `,
      [deviceId, adminAccess.playerId]
    );
    if (!targetLookup.rowCount) {
      res.status(404).json({ ok: false, error: "Active device not found." });
      return;
    }
    await db.query(
      `
        UPDATE admin_trusted_devices
        SET owner_lock = CASE WHEN id = $1 THEN true ELSE false END
      `,
      [deviceId]
    );
    const lockedLookup = await db.query(
      `
        SELECT id, player_id, token_hash, label, created_at, last_seen_at, last_ip, last_user_agent, revoked_at, owner_lock
        FROM admin_trusted_devices
        WHERE id = $1
        LIMIT 1
      `,
      [deviceId]
    );
    recordAdminActivity({
      eventType: "owner_device_locked",
      actorPlayerId: adminAccess.playerId,
      details: { deviceId }
    });
    res.json({
      ok: true,
      ownerLockEnabled: true,
      device: lockedLookup.rowCount ? mapAdminDeviceRow(lockedLookup.rows[0]) : mapAdminDeviceRow(targetLookup.rows[0])
    });
  } catch (error) {
    console.error("Failed to set owner device lock", error);
    res.status(500).json({ ok: false, error: "Could not lock admin to this device." });
  }
});

app.post("/api/admin/devices/clear-owner-lock", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const adminCode = normalizeAdminCode(getAdminCode(req));
    const expectedCode = normalizeAdminCode(ADMIN_CODE);
    if (!adminCode || adminCode !== expectedCode) {
      res.status(401).json({ ok: false, error: "Admin code is required to clear owner lock." });
      return;
    }
    await db.query(
      `
        UPDATE admin_trusted_devices
        SET owner_lock = false
        WHERE owner_lock = true
      `
    );
    recordAdminActivity({
      eventType: "owner_device_lock_cleared",
      actorPlayerId: adminAccess.playerId
    });
    res.json({ ok: true, ownerLockEnabled: false });
  } catch (error) {
    console.error("Failed to clear owner device lock", error);
    res.status(500).json({ ok: false, error: "Could not clear owner device lock." });
  }
});

app.post("/api/admin/devices/:id/ban", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const deviceId = Number(req.params?.id);
    if (!Number.isInteger(deviceId) || deviceId <= 0) {
      res.status(400).json({ ok: false, error: "Invalid device id." });
      return;
    }
    const now = Date.now();
    const updated = await db.query(
      `
        UPDATE admin_trusted_devices
        SET revoked_at = $1
        WHERE id = $2
          AND player_id = $3
          AND revoked_at = 0
        RETURNING id, player_id, label, created_at, last_seen_at, last_ip, last_user_agent, revoked_at, owner_lock
      `,
      [now, deviceId, adminAccess.playerId]
    );
    if (!updated.rowCount) {
      const exists = await db.query("SELECT id FROM admin_trusted_devices WHERE id = $1 AND player_id = $2 LIMIT 1", [deviceId, adminAccess.playerId]);
      if (!exists.rowCount) {
        res.status(404).json({ ok: false, error: "Device not found." });
        return;
      }
      res.status(409).json({ ok: false, error: "Device is already banned." });
      return;
    }
    recordAdminActivity({
      eventType: "device_banned",
      actorPlayerId: adminAccess.playerId,
      details: { deviceId }
    });
    res.json({ ok: true, device: mapAdminDeviceRow(updated.rows[0]) });
  } catch (error) {
    console.error("Failed to ban trusted device", error);
    res.status(500).json({ ok: false, error: "Could not ban device." });
  }
});

app.post("/api/admin/devices/:id/unban", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const deviceId = Number(req.params?.id);
    if (!Number.isInteger(deviceId) || deviceId <= 0) {
      res.status(400).json({ ok: false, error: "Invalid device id." });
      return;
    }
    const updated = await db.query(
      `
        UPDATE admin_trusted_devices
        SET revoked_at = 0
        WHERE id = $1
          AND player_id = $2
          AND revoked_at > 0
        RETURNING id, player_id, label, created_at, last_seen_at, last_ip, last_user_agent, revoked_at, owner_lock
      `,
      [deviceId, adminAccess.playerId]
    );
    if (!updated.rowCount) {
      const exists = await db.query("SELECT id FROM admin_trusted_devices WHERE id = $1 AND player_id = $2 LIMIT 1", [deviceId, adminAccess.playerId]);
      if (!exists.rowCount) {
        res.status(404).json({ ok: false, error: "Device not found." });
        return;
      }
      res.status(409).json({ ok: false, error: "Device is not banned." });
      return;
    }
    recordAdminActivity({
      eventType: "device_unbanned",
      actorPlayerId: adminAccess.playerId,
      details: { deviceId }
    });
    res.json({ ok: true, device: mapAdminDeviceRow(updated.rows[0]) });
  } catch (error) {
    console.error("Failed to unban trusted device", error);
    res.status(500).json({ ok: false, error: "Could not unban device." });
  }
});

app.post("/api/admin/devices/:id/remove", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const deviceId = Number(req.params?.id);
    if (!Number.isInteger(deviceId) || deviceId <= 0) {
      res.status(400).json({ ok: false, error: "Invalid device id." });
      return;
    }
    const removed = await db.query(
      `
        DELETE FROM admin_trusted_devices
        WHERE id = $1
          AND player_id = $2
        RETURNING id, player_id, label, created_at, last_seen_at, last_ip, last_user_agent, revoked_at, owner_lock
      `,
      [deviceId, adminAccess.playerId]
    );
    if (!removed.rowCount) {
      res.status(404).json({ ok: false, error: "Device not found." });
      return;
    }
    recordAdminActivity({
      eventType: "device_removed",
      actorPlayerId: adminAccess.playerId,
      details: { deviceId }
    });
    res.json({ ok: true, device: mapAdminDeviceRow(removed.rows[0]) });
  } catch (error) {
    console.error("Failed to remove trusted device", error);
    res.status(500).json({ ok: false, error: "Could not remove device." });
  }
});

app.post("/api/admin/users/:playerId/ban", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const playerId = sanitizePlayerId(req.params?.playerId);
    if (!playerId) {
      res.status(400).json({ ok: false, error: "Invalid player id." });
      return;
    }
    const reason = sanitizeBanReason(req.body?.reason);
    const now = Date.now();
    const updated = await db.query(
      `
        UPDATE users
        SET banned_at = $1,
            banned_reason = $2
        WHERE player_id = $3
          AND banned_at = 0
        RETURNING player_id
      `,
      [now, reason, playerId]
    );
    if (!updated.rowCount) {
      const exists = await db.query("SELECT player_id, banned_at FROM users WHERE player_id = $1 LIMIT 1", [playerId]);
      if (!exists.rowCount) {
        res.status(404).json({ ok: false, error: "User not found." });
        return;
      }
      res.status(409).json({ ok: false, error: "User is already banned." });
      return;
    }
    queueLeaderboardUpdate("ban");
    recordAdminActivity({
      eventType: "user_banned",
      playerId,
      actorPlayerId: adminAccess.playerId,
      details: {
        reason,
        bannedAt: now
      }
    });
    res.json({ ok: true, playerId, bannedAt: now, bannedReason: reason });
  } catch (error) {
    console.error("Failed to ban user", error);
    res.status(500).json({ ok: false, error: "Could not ban user." });
  }
});

app.post("/api/admin/users/:playerId/unban", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const playerId = sanitizePlayerId(req.params?.playerId);
    if (!playerId) {
      res.status(400).json({ ok: false, error: "Invalid player id." });
      return;
    }
    const updated = await db.query(
      `
        UPDATE users
        SET banned_at = 0,
            banned_reason = ''
        WHERE player_id = $1
          AND banned_at > 0
        RETURNING player_id
      `,
      [playerId]
    );
    if (!updated.rowCount) {
      const exists = await db.query("SELECT player_id, banned_at FROM users WHERE player_id = $1 LIMIT 1", [playerId]);
      if (!exists.rowCount) {
        res.status(404).json({ ok: false, error: "User not found." });
        return;
      }
      res.status(409).json({ ok: false, error: "User is not banned." });
      return;
    }
    queueLeaderboardUpdate("unban");
    recordAdminActivity({
      eventType: "user_unbanned",
      playerId,
      actorPlayerId: adminAccess.playerId
    });
    res.json({ ok: true, playerId });
  } catch (error) {
    console.error("Failed to unban user", error);
    res.status(500).json({ ok: false, error: "Could not unban user." });
  }
});

app.post("/api/admin/users/:playerId/remove", async (req, res) => {
  const client = await db.connect();
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const playerId = sanitizePlayerId(req.params?.playerId);
    if (!playerId) {
      res.status(400).json({ ok: false, error: "Invalid player id." });
      return;
    }
    await client.query("BEGIN");
    await client.query("DELETE FROM claims WHERE player_id = $1", [playerId]);
    await client.query("DELETE FROM live_wins WHERE player_id = $1", [playerId]);
    await client.query(
      `
        DELETE FROM user_sessions
        WHERE COALESCE(sess::json->>'playerId', '') = $1
      `,
      [playerId]
    );
    const removedUser = await client.query(
      `
        DELETE FROM users
        WHERE player_id = $1
        RETURNING player_id, username, email
      `,
      [playerId]
    );
    if (!removedUser.rowCount) {
      await client.query("ROLLBACK");
      res.status(404).json({ ok: false, error: "User not found." });
      return;
    }
    await client.query("COMMIT");
    queueLeaderboardUpdate("remove");
    recordAdminActivity({
      eventType: "user_removed",
      playerId,
      username: sanitizeUsername(removedUser.rows[0]?.username) || "",
      actorPlayerId: adminAccess.playerId
    });
    res.json({
      ok: true,
      removed: {
        playerId: String(removedUser.rows[0].player_id || ""),
        username: sanitizeUsername(removedUser.rows[0].username) || "Unknown",
        email: sanitizeEmail(removedUser.rows[0].email) || ""
      }
    });
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    console.error("Failed to remove user", error);
    res.status(500).json({ ok: false, error: "Could not remove user." });
  } finally {
    client.release();
  }
});

app.post("/api/admin/users/:playerId/force-logout", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const playerId = sanitizePlayerId(req.params?.playerId);
    if (!playerId) {
      res.status(400).json({ ok: false, error: "Invalid player id." });
      return;
    }
    const removedSessions = await db.query(
      `
        DELETE FROM user_sessions
        WHERE COALESCE(sess::json->>'playerId', '') = $1
      `,
      [playerId]
    );
    recordAdminActivity({
      eventType: "force_logout",
      playerId,
      actorPlayerId: adminAccess.playerId,
      details: { sessionsCleared: removedSessions.rowCount }
    });
    res.json({
      ok: true,
      playerId,
      sessionsCleared: removedSessions.rowCount || 0,
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    console.error("Failed to force logout user", error);
    res.status(500).json({ ok: false, error: "Could not force logout user." });
  }
});

app.post("/api/admin/users/:playerId/sync-guard-bypass", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const playerId = sanitizePlayerId(req.params?.playerId);
    if (!playerId) {
      res.status(400).json({ ok: false, error: "Invalid player id." });
      return;
    }
    const minutesValue = Number(req.body?.minutes);
    const minutes = Number.isFinite(minutesValue)
      ? Math.max(1, Math.min(MAX_SYNC_GUARD_BYPASS_MINUTES, Math.floor(minutesValue)))
      : 30;
    const bypassUntil = Date.now() + minutes * 60 * 1000;
    const updated = await db.query(
      `
        UPDATE users
        SET sync_guard_bypass_until = $1
        WHERE player_id = $2
        RETURNING player_id, username, sync_guard_bypass_until
      `,
      [bypassUntil, playerId]
    );
    if (!updated.rowCount) {
      res.status(404).json({ ok: false, error: "User not found." });
      return;
    }
    recordAdminActivity({
      eventType: "sync_guard_bypass_granted",
      playerId,
      username: sanitizeUsername(updated.rows[0]?.username) || "",
      actorPlayerId: adminAccess.playerId,
      details: {
        minutes,
        bypassUntil
      }
    });
    res.json({
      ok: true,
      playerId,
      bypassUntil,
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    console.error("Failed to grant sync guard bypass", error);
    res.status(500).json({ ok: false, error: "Could not grant guard bypass." });
  }
});

app.post("/api/admin/users/:playerId/sync-guard-bypass/clear", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const playerId = sanitizePlayerId(req.params?.playerId);
    if (!playerId) {
      res.status(400).json({ ok: false, error: "Invalid player id." });
      return;
    }
    const updated = await db.query(
      `
        UPDATE users
        SET sync_guard_bypass_until = 0
        WHERE player_id = $1
        RETURNING player_id, username
      `,
      [playerId]
    );
    if (!updated.rowCount) {
      res.status(404).json({ ok: false, error: "User not found." });
      return;
    }
    recordAdminActivity({
      eventType: "sync_guard_bypass_cleared",
      playerId,
      username: sanitizeUsername(updated.rows[0]?.username) || "",
      actorPlayerId: adminAccess.playerId
    });
    res.json({
      ok: true,
      playerId,
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    console.error("Failed to clear sync guard bypass", error);
    res.status(500).json({ ok: false, error: "Could not clear guard bypass." });
  }
});

app.post("/api/admin/users/:playerId/reset-progress", async (req, res) => {
  const client = await db.connect();
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const playerId = sanitizePlayerId(req.params?.playerId);
    if (!playerId) {
      res.status(400).json({ ok: false, error: "Invalid player id." });
      return;
    }
    const userLookup = await client.query(
      `
        SELECT player_id, username, balance
        FROM users
        WHERE player_id = $1
        LIMIT 1
      `,
      [playerId]
    );
    if (!userLookup.rowCount) {
      res.status(404).json({ ok: false, error: "User not found." });
      return;
    }
    const user = userLookup.rows[0];
    const now = Date.now();
    await client.query("BEGIN");
    await client.query("DELETE FROM claims WHERE player_id = $1", [playerId]);
    await client.query("DELETE FROM live_wins WHERE player_id = $1", [playerId]);
    const removedSessions = await client.query(
      `
        DELETE FROM user_sessions
        WHERE COALESCE(sess::json->>'playerId', '') = $1
      `,
      [playerId]
    );
    const updated = await client.query(
      `
        UPDATE users
        SET balance = $2,
            shares = 0,
            avg_cost = 0,
            savings_balance = 0,
            auto_savings_percent = 0,
            sync_guard_bypass_until = 0,
            balance_updated_at = $3
        WHERE player_id = $1
        RETURNING player_id, username, email, username_key, balance, shares, avg_cost, savings_balance, auto_savings_percent, balance_updated_at, last_seen_at, is_admin
      `,
      [playerId, INITIAL_ACCOUNT_BALANCE, now]
    );
    await client.query("COMMIT");
    queueLeaderboardUpdate("reset-progress");
    recordBalanceAuditEvent({
      playerId,
      username: sanitizeUsername(user.username) || "",
      source: "admin-reset-progress",
      beforeBalance: toNumber(user.balance),
      afterBalance: INITIAL_ACCOUNT_BALANCE
    });
    recordAdminActivity({
      eventType: "user_progress_reset",
      playerId,
      username: sanitizeUsername(user.username) || "",
      actorPlayerId: adminAccess.playerId,
      details: {
        fromBalance: Math.round(toNumber(user.balance) * 100) / 100,
        toBalance: INITIAL_ACCOUNT_BALANCE,
        sessionsCleared: removedSessions.rowCount || 0
      }
    });
    res.json({
      ok: true,
      user: mapUserRow(updated.rows[0]),
      sessionsCleared: removedSessions.rowCount || 0,
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    console.error("Failed to reset user progress", error);
    res.status(500).json({ ok: false, error: "Could not reset user progress." });
  } finally {
    client.release();
  }
});

app.post("/api/admin/users/:playerId/mute", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const playerId = sanitizePlayerId(req.params?.playerId);
    if (!playerId) {
      res.status(400).json({ ok: false, error: "Invalid player id." });
      return;
    }
    const minutesValue = Number(req.body?.minutes);
    const minutes = Number.isFinite(minutesValue) ? Math.max(1, Math.min(10080, Math.floor(minutesValue))) : 60;
    const reason = sanitizeBanReason(req.body?.reason || "Temporary cooldown");
    const mutedUntil = Date.now() + minutes * 60 * 1000;
    const updated = await db.query(
      `
        UPDATE users
        SET muted_until = $1,
            muted_reason = $2
        WHERE player_id = $3
        RETURNING player_id, username, muted_until, muted_reason
      `,
      [mutedUntil, reason, playerId]
    );
    if (!updated.rowCount) {
      res.status(404).json({ ok: false, error: "User not found." });
      return;
    }
    const user = updated.rows[0];
    recordAdminActivity({
      eventType: "user_muted",
      playerId,
      username: sanitizeUsername(user.username) || "",
      actorPlayerId: adminAccess.playerId,
      details: {
        minutes,
        mutedUntil,
        reason
      }
    });
    res.json({
      ok: true,
      playerId,
      mutedUntil: Number(user.muted_until) || mutedUntil,
      mutedReason: String(user.muted_reason || reason),
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    console.error("Failed to mute user", error);
    res.status(500).json({ ok: false, error: "Could not mute user." });
  }
});

app.post("/api/admin/users/:playerId/unmute", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const playerId = sanitizePlayerId(req.params?.playerId);
    if (!playerId) {
      res.status(400).json({ ok: false, error: "Invalid player id." });
      return;
    }
    const updated = await db.query(
      `
        UPDATE users
        SET muted_until = 0,
            muted_reason = ''
        WHERE player_id = $1
        RETURNING player_id, username
      `,
      [playerId]
    );
    if (!updated.rowCount) {
      res.status(404).json({ ok: false, error: "User not found." });
      return;
    }
    recordAdminActivity({
      eventType: "user_unmuted",
      playerId,
      username: sanitizeUsername(updated.rows[0]?.username) || "",
      actorPlayerId: adminAccess.playerId
    });
    res.json({ ok: true, playerId, trustedDevice: adminAccess.device || null });
  } catch (error) {
    console.error("Failed to unmute user", error);
    res.status(500).json({ ok: false, error: "Could not unmute user." });
  }
});

app.post("/api/admin/users/:playerId/balance", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const playerId = sanitizePlayerId(req.params?.playerId);
    if (!playerId) {
      res.status(400).json({ ok: false, error: "Invalid player id." });
      return;
    }
    const nextBalance = sanitizeSyncNumber(req.body?.balance, {
      max: MAX_ACCOUNT_BALANCE,
      decimals: 2
    });
    if (nextBalance === null) {
      res.status(400).json({
        ok: false,
        error: `Balance must be between 0 and ${MAX_ACCOUNT_BALANCE.toLocaleString()}.`
      });
      return;
    }
    const existing = await db.query(
      `
        SELECT player_id, username, balance
        FROM users
        WHERE player_id = $1
        LIMIT 1
      `,
      [playerId]
    );
    if (!existing.rowCount) {
      res.status(404).json({ ok: false, error: "User not found." });
      return;
    }
    const previous = existing.rows[0];
    const now = Date.now();
    const updated = await db.query(
      `
        UPDATE users
        SET balance = $1,
            balance_updated_at = $2
        WHERE player_id = $3
        RETURNING player_id, username, email, username_key, balance, shares, avg_cost, savings_balance, auto_savings_percent, last_seen_at, is_admin
      `,
      [nextBalance, now, playerId]
    );
    if (!updated.rowCount) {
      res.status(404).json({ ok: false, error: "User not found." });
      return;
    }
    queueLeaderboardUpdate("admin-balance");
    recordBalanceAuditEvent({
      playerId,
      username: sanitizeUsername(previous.username) || "",
      source: "admin-set-balance",
      beforeBalance: toNumber(previous.balance),
      afterBalance: nextBalance
    });
    recordAdminActivity({
      eventType: "balance_set",
      playerId,
      username: sanitizeUsername(previous.username) || "",
      actorPlayerId: adminAccess.playerId,
      details: {
        beforeBalance: Math.round(toNumber(previous.balance) * 100) / 100,
        afterBalance: nextBalance
      }
    });
    res.json({
      ok: true,
      user: mapUserRow(updated.rows[0]),
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    console.error("Failed to set user balance", error);
    res.status(500).json({ ok: false, error: "Could not update user balance." });
  }
});

app.post("/api/admin/feedback/:id/remove", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const feedbackId = String(req.params?.id || "").trim();
    if (!/^\d{1,18}$/.test(feedbackId)) {
      res.status(400).json({ ok: false, error: "Invalid feedback id." });
      return;
    }
    const removed = await db.query(
      `
        DELETE FROM feedback_submissions
        WHERE id = $1
        RETURNING id
      `,
      [feedbackId]
    );
    if (!removed.rowCount) {
      res.status(404).json({ ok: false, error: "Feedback not found." });
      return;
    }
    recordAdminActivity({
      eventType: "feedback_removed",
      actorPlayerId: adminAccess.playerId,
      details: { feedbackId: String(removed.rows[0].id || feedbackId) }
    });
    res.json({
      ok: true,
      removed: { id: String(removed.rows[0].id || feedbackId) },
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    console.error("Failed to remove feedback", error);
    res.status(500).json({ ok: false, error: "Could not remove feedback." });
  }
});

app.post("/api/admin/system/reset", async (req, res) => {
  const client = await db.connect();
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const adminCode = normalizeAdminCode(getAdminCode(req));
    const expectedCode = normalizeAdminCode(ADMIN_CODE);
    if (!adminCode || adminCode !== expectedCode) {
      res.status(401).json({ ok: false, error: "Admin code is required for full reset." });
      return;
    }
    const confirmText = String(req.body?.confirmText || "").trim();
    if (confirmText !== "RESET EVERYTHING") {
      res.status(400).json({ ok: false, error: "Confirmation text must be RESET EVERYTHING." });
      return;
    }
    const adminPlayerId = sanitizePlayerId(adminAccess.playerId);
    if (!adminPlayerId) {
      res.status(400).json({ ok: false, error: "Invalid admin session." });
      return;
    }
    const tokenHash = String(adminAccess.tokenHash || "").trim();
    const now = Date.now();

    await client.query("BEGIN");
    const claimsDeleted = await client.query("DELETE FROM claims");
    const feedbackDeleted = await client.query("DELETE FROM feedback_submissions");
    const winsDeleted = await client.query("DELETE FROM live_wins");
    const visitsDeleted = await client.query("DELETE FROM site_visit_events");
    const verificationDeleted = await client.query("DELETE FROM email_verification_codes");
    const popupReadsDeleted = await client.query("DELETE FROM admin_popup_message_reads");
    const popupMessagesDeleted = await client.query("DELETE FROM admin_popup_messages");
    const sessionsDeleted = await client.query(
      `
        DELETE FROM user_sessions
        WHERE COALESCE(sess::json->>'playerId', '') <> $1
      `,
      [adminPlayerId]
    );
    const usersDeleted = await client.query(
      `
        DELETE FROM users
        WHERE player_id <> $1
      `,
      [adminPlayerId]
    );
    await client.query(
      `
        UPDATE users
        SET is_admin = true,
            banned_at = 0,
            banned_reason = '',
            sync_guard_bypass_until = 0,
            balance = 1000,
            shares = 0,
            avg_cost = 0,
            savings_balance = 0,
            auto_savings_percent = 0,
            balance_updated_at = $2,
            last_seen_at = $2
        WHERE player_id = $1
      `,
      [adminPlayerId, now]
    );
    const devicesDeleted = tokenHash
      ? await client.query(
          `
            DELETE FROM admin_trusted_devices
            WHERE player_id <> $1
               OR token_hash <> $2
          `,
          [adminPlayerId, tokenHash]
        )
      : await client.query("DELETE FROM admin_trusted_devices WHERE player_id <> $1", [adminPlayerId]);
    if (tokenHash) {
      await client.query(
        `
          UPDATE admin_trusted_devices
          SET revoked_at = 0,
              owner_lock = true,
              last_seen_at = $2
          WHERE player_id = $1
            AND token_hash = $3
        `,
        [adminPlayerId, now, tokenHash]
      );
    }
    await client.query("COMMIT");
    queueLeaderboardUpdate("reset");
    recordAdminActivity({
      eventType: "full_reset",
      actorPlayerId: adminAccess.playerId,
      details: {
        usersDeleted: usersDeleted.rowCount,
        claimsDeleted: claimsDeleted.rowCount,
        feedbackDeleted: feedbackDeleted.rowCount,
        liveWinsDeleted: winsDeleted.rowCount,
        visitsDeleted: visitsDeleted.rowCount,
        popupMessagesDeleted: popupMessagesDeleted.rowCount
      }
    });
    res.json({
      ok: true,
      reset: {
        usersDeleted: usersDeleted.rowCount,
        claimsDeleted: claimsDeleted.rowCount,
        feedbackDeleted: feedbackDeleted.rowCount,
        liveWinsDeleted: winsDeleted.rowCount,
        visitsDeleted: visitsDeleted.rowCount,
        popupMessagesDeleted: popupMessagesDeleted.rowCount,
        popupReadsDeleted: popupReadsDeleted.rowCount,
        pendingVerificationsDeleted: verificationDeleted.rowCount,
        sessionsDeleted: sessionsDeleted.rowCount,
        devicesDeleted: devicesDeleted.rowCount
      }
    });
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    console.error("Failed full system reset", error);
    res.status(500).json({ ok: false, error: "Could not complete full reset." });
  } finally {
    client.release();
  }
});

app.get("/api/admin/stats", async (req, res) => {
  try {
    const adminAccess = await requireAdminAccess(req, res, { allowBootstrap: false });
    if (!adminAccess) return;
    const dayAgo = Date.now() - 24 * 60 * 60 * 1000;
    const totals = await db.query(
      `
        WITH user_totals AS (
          SELECT
            COUNT(*)::int AS total_users,
            COUNT(*) FILTER (WHERE banned_at > 0)::int AS banned_users,
            COUNT(*) FILTER (WHERE email IS NOT NULL AND email <> '')::int AS users_with_email,
            COUNT(*) FILTER (WHERE password_hash IS NOT NULL AND password_hash <> '')::int AS users_with_password,
            COUNT(*) FILTER (WHERE last_seen_at >= $1)::int AS active_users_24h,
            COALESCE(SUM(balance), 0)::numeric AS total_balance
          FROM users
        ),
        claim_totals AS (
          SELECT
            COUNT(*)::int AS total_claims,
            COUNT(*) FILTER (WHERE status = 'pending')::int AS pending_claims,
            COUNT(*) FILTER (WHERE status = 'approved')::int AS approved_claims,
            COUNT(*) FILTER (WHERE status = 'rejected')::int AS rejected_claims
          FROM claims
        ),
        win_totals AS (
          SELECT
            COUNT(*) FILTER (WHERE submitted_at >= $1)::int AS live_wins_24h,
            COALESCE(SUM(amount) FILTER (WHERE submitted_at >= $1), 0)::numeric AS live_win_amount_24h
          FROM live_wins
        ),
        device_totals AS (
          SELECT
            COUNT(*) FILTER (WHERE revoked_at = 0)::int AS trusted_devices_active,
            COUNT(*) FILTER (WHERE revoked_at > 0)::int AS trusted_devices_banned
          FROM admin_trusted_devices
        ),
        visit_totals AS (
          SELECT
            COUNT(*)::int AS site_visits_total,
            COUNT(*) FILTER (WHERE visited_at >= $1)::int AS site_visits_24h,
            COUNT(DISTINCT visitor_id)::int AS unique_visitors_total,
            COUNT(DISTINCT visitor_id) FILTER (WHERE visited_at >= $1)::int AS unique_visitors_24h
          FROM site_visit_events
        )
        SELECT
          user_totals.total_users,
          user_totals.banned_users,
          user_totals.users_with_email,
          user_totals.users_with_password,
          user_totals.active_users_24h,
          user_totals.total_balance,
          claim_totals.total_claims,
          claim_totals.pending_claims,
          claim_totals.approved_claims,
          claim_totals.rejected_claims,
          win_totals.live_wins_24h,
          win_totals.live_win_amount_24h,
          device_totals.trusted_devices_active,
          device_totals.trusted_devices_banned,
          visit_totals.site_visits_total,
          visit_totals.site_visits_24h,
          visit_totals.unique_visitors_total,
          visit_totals.unique_visitors_24h
        FROM user_totals, claim_totals, win_totals, device_totals, visit_totals
      `,
      [dayAgo]
    );
    const row = totals.rows[0] || {};
    res.json({
      ok: true,
      stats: {
        totalUsers: Number(row.total_users) || 0,
        bannedUsers: Number(row.banned_users) || 0,
        usersWithEmail: Number(row.users_with_email) || 0,
        usersWithPassword: Number(row.users_with_password) || 0,
        activeUsers24h: Number(row.active_users_24h) || 0,
        totalBalance: Math.round(toNumber(row.total_balance) * 100) / 100,
        totalClaims: Number(row.total_claims) || 0,
        pendingClaims: Number(row.pending_claims) || 0,
        approvedClaims: Number(row.approved_claims) || 0,
        rejectedClaims: Number(row.rejected_claims) || 0,
        liveWins24h: Number(row.live_wins_24h) || 0,
        liveWinAmount24h: Math.round(toNumber(row.live_win_amount_24h) * 100) / 100,
        trustedDevicesActive: Number(row.trusted_devices_active) || 0,
        trustedDevicesBanned: Number(row.trusted_devices_banned) || 0,
        siteVisitsTotal: Number(row.site_visits_total) || 0,
        siteVisits24h: Number(row.site_visits_24h) || 0,
        uniqueVisitorsTotal: Number(row.unique_visitors_total) || 0,
        uniqueVisitors24h: Number(row.unique_visitors_24h) || 0
      },
      trustedDevice: adminAccess.device || null
    });
  } catch (error) {
    console.error("Failed to load admin stats", error);
    res.status(500).json({ ok: false, error: "Could not load admin stats." });
  }
});

async function ensureSchema() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS users (
      player_id TEXT PRIMARY KEY,
      username TEXT NOT NULL,
      username_key TEXT NOT NULL UNIQUE,
      email TEXT,
      password_hash TEXT,
      email_verified_at BIGINT NOT NULL DEFAULT 0,
      is_admin BOOLEAN NOT NULL DEFAULT false,
      balance NUMERIC(14,2) NOT NULL DEFAULT 0,
      shares INTEGER NOT NULL DEFAULT 0,
      avg_cost NUMERIC(14,4) NOT NULL DEFAULT 0,
      savings_balance NUMERIC(14,2) NOT NULL DEFAULT 0,
      auto_savings_percent NUMERIC(6,3) NOT NULL DEFAULT 0,
      balance_updated_at BIGINT NOT NULL DEFAULT 0,
      last_seen_at BIGINT NOT NULL DEFAULT 0,
      banned_at BIGINT NOT NULL DEFAULT 0,
      banned_reason TEXT NOT NULL DEFAULT '',
      muted_until BIGINT NOT NULL DEFAULT 0,
      muted_reason TEXT NOT NULL DEFAULT '',
      sync_guard_bypass_until BIGINT NOT NULL DEFAULT 0
    )
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS email TEXT
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS password_hash TEXT
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS email_verified_at BIGINT NOT NULL DEFAULT 0
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS username_key TEXT
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS balance NUMERIC(14,2) NOT NULL DEFAULT 0
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS shares INTEGER NOT NULL DEFAULT 0
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS avg_cost NUMERIC(14,4) NOT NULL DEFAULT 0
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS savings_balance NUMERIC(14,2) NOT NULL DEFAULT 0
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS auto_savings_percent NUMERIC(6,3) NOT NULL DEFAULT 0
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS balance_updated_at BIGINT NOT NULL DEFAULT 0
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS last_seen_at BIGINT NOT NULL DEFAULT 0
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS banned_at BIGINT NOT NULL DEFAULT 0
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS banned_reason TEXT NOT NULL DEFAULT ''
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS muted_until BIGINT NOT NULL DEFAULT 0
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS muted_reason TEXT NOT NULL DEFAULT ''
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS sync_guard_bypass_until BIGINT NOT NULL DEFAULT 0
  `);
  await db.query(`
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS is_admin BOOLEAN
  `);
  await db.query(`
    UPDATE users
    SET username_key = lower(username),
        email_verified_at = COALESCE(email_verified_at, 0),
        shares = COALESCE(shares, 0),
        avg_cost = COALESCE(avg_cost, 0),
        savings_balance = COALESCE(savings_balance, 0),
        auto_savings_percent = COALESCE(auto_savings_percent, 0),
        balance_updated_at = COALESCE(balance_updated_at, 0),
        banned_at = COALESCE(banned_at, 0),
        banned_reason = COALESCE(banned_reason, ''),
        muted_until = COALESCE(muted_until, 0),
        muted_reason = COALESCE(muted_reason, ''),
        sync_guard_bypass_until = COALESCE(sync_guard_bypass_until, 0),
        is_admin = COALESCE(is_admin, false)
    WHERE username_key IS NULL OR username_key = ''
       OR email_verified_at IS NULL
       OR shares IS NULL
       OR avg_cost IS NULL
       OR savings_balance IS NULL
       OR auto_savings_percent IS NULL
       OR balance_updated_at IS NULL
       OR banned_at IS NULL
       OR banned_reason IS NULL
       OR muted_until IS NULL
       OR muted_reason IS NULL
       OR sync_guard_bypass_until IS NULL
       OR is_admin IS NULL
  `);
  await db.query(`
    ALTER TABLE users
    ALTER COLUMN sync_guard_bypass_until SET DEFAULT 0
  `);
  await db.query(`
    ALTER TABLE users
    ALTER COLUMN sync_guard_bypass_until SET NOT NULL
  `);
  await db.query(`
    ALTER TABLE users
    ALTER COLUMN is_admin SET DEFAULT false
  `);
  await db.query(`
    ALTER TABLE users
    ALTER COLUMN is_admin SET NOT NULL
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS users_username_key_idx
    ON users (username_key)
  `);
  await db.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS users_email_unique_idx
    ON users (lower(email))
    WHERE email IS NOT NULL
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS email_verification_codes (
      email TEXT PRIMARY KEY,
      player_id TEXT NOT NULL,
      username TEXT NOT NULL,
      username_key TEXT NOT NULL,
      password_hash TEXT NOT NULL,
      balance NUMERIC(14,2) NOT NULL DEFAULT 0,
      code_hash TEXT NOT NULL,
      expires_at BIGINT NOT NULL DEFAULT 0,
      attempts INTEGER NOT NULL DEFAULT 0,
      created_at BIGINT NOT NULL DEFAULT 0,
      last_sent_at BIGINT NOT NULL DEFAULT 0
    )
  `);
  await db.query(`
    ALTER TABLE email_verification_codes
    ADD COLUMN IF NOT EXISTS email TEXT
  `);
  await db.query(`
    ALTER TABLE email_verification_codes
    ADD COLUMN IF NOT EXISTS player_id TEXT
  `);
  await db.query(`
    ALTER TABLE email_verification_codes
    ADD COLUMN IF NOT EXISTS username TEXT
  `);
  await db.query(`
    ALTER TABLE email_verification_codes
    ADD COLUMN IF NOT EXISTS username_key TEXT
  `);
  await db.query(`
    ALTER TABLE email_verification_codes
    ADD COLUMN IF NOT EXISTS password_hash TEXT
  `);
  await db.query(`
    ALTER TABLE email_verification_codes
    ADD COLUMN IF NOT EXISTS balance NUMERIC(14,2)
  `);
  await db.query(`
    ALTER TABLE email_verification_codes
    ADD COLUMN IF NOT EXISTS code_hash TEXT
  `);
  await db.query(`
    ALTER TABLE email_verification_codes
    ADD COLUMN IF NOT EXISTS expires_at BIGINT
  `);
  await db.query(`
    ALTER TABLE email_verification_codes
    ADD COLUMN IF NOT EXISTS attempts INTEGER
  `);
  await db.query(`
    ALTER TABLE email_verification_codes
    ADD COLUMN IF NOT EXISTS created_at BIGINT
  `);
  await db.query(`
    ALTER TABLE email_verification_codes
    ADD COLUMN IF NOT EXISTS last_sent_at BIGINT
  `);
  await db.query(`
    UPDATE email_verification_codes
    SET username = COALESCE(NULLIF(username, ''), 'Player'),
        username_key = COALESCE(NULLIF(username_key, ''), lower(COALESCE(NULLIF(username, ''), 'player'))),
        balance = COALESCE(balance, 0),
        code_hash = COALESCE(code_hash, ''),
        expires_at = COALESCE(expires_at, 0),
        attempts = COALESCE(attempts, 0),
        created_at = COALESCE(created_at, 0),
        last_sent_at = COALESCE(last_sent_at, 0)
  `);
  await db.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS email_verification_codes_email_idx
    ON email_verification_codes (lower(email))
    WHERE email IS NOT NULL AND email <> ''
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS admin_trusted_devices (
      id BIGSERIAL PRIMARY KEY,
      player_id TEXT NOT NULL DEFAULT '',
      token_hash TEXT NOT NULL UNIQUE,
      label TEXT NOT NULL DEFAULT 'Trusted device',
      created_at BIGINT NOT NULL DEFAULT 0,
      last_seen_at BIGINT NOT NULL DEFAULT 0,
      last_ip TEXT NOT NULL DEFAULT '',
      last_user_agent TEXT NOT NULL DEFAULT '',
      revoked_at BIGINT NOT NULL DEFAULT 0,
      owner_lock BOOLEAN NOT NULL DEFAULT false
    )
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ADD COLUMN IF NOT EXISTS player_id TEXT
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ADD COLUMN IF NOT EXISTS token_hash TEXT
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ADD COLUMN IF NOT EXISTS label TEXT
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ADD COLUMN IF NOT EXISTS created_at BIGINT
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ADD COLUMN IF NOT EXISTS last_seen_at BIGINT
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ADD COLUMN IF NOT EXISTS last_ip TEXT
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ADD COLUMN IF NOT EXISTS last_user_agent TEXT
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ADD COLUMN IF NOT EXISTS revoked_at BIGINT
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ADD COLUMN IF NOT EXISTS owner_lock BOOLEAN NOT NULL DEFAULT false
  `);
  await db.query(`
    UPDATE admin_trusted_devices
    SET player_id = COALESCE(player_id, ''),
        label = COALESCE(NULLIF(label, ''), 'Trusted device'),
        created_at = COALESCE(created_at, 0),
        last_seen_at = COALESCE(last_seen_at, 0),
        last_ip = COALESCE(last_ip, ''),
        last_user_agent = COALESCE(last_user_agent, ''),
        revoked_at = COALESCE(revoked_at, 0),
        owner_lock = COALESCE(owner_lock, false)
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ALTER COLUMN player_id SET DEFAULT ''
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ALTER COLUMN player_id SET NOT NULL
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ALTER COLUMN owner_lock SET DEFAULT false
  `);
  await db.query(`
    ALTER TABLE admin_trusted_devices
    ALTER COLUMN owner_lock SET NOT NULL
  `);
  await db.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS admin_trusted_devices_token_hash_idx
    ON admin_trusted_devices (token_hash)
    WHERE token_hash IS NOT NULL AND token_hash <> ''
  `);
  await db.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS admin_trusted_devices_owner_lock_unique_idx
    ON admin_trusted_devices ((owner_lock))
    WHERE owner_lock = true
      AND revoked_at = 0
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS admin_trusted_devices_player_id_idx
    ON admin_trusted_devices (player_id)
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS claims (
      id BIGSERIAL PRIMARY KEY,
      player_id TEXT NOT NULL,
      username TEXT NOT NULL,
      source TEXT NOT NULL DEFAULT 'venmo',
      pack_id TEXT NOT NULL,
      txn_id TEXT NOT NULL,
      txn_norm TEXT NOT NULL UNIQUE,
      status TEXT NOT NULL DEFAULT 'pending',
      submitted_at BIGINT NOT NULL DEFAULT 0,
      reviewed_at BIGINT NOT NULL DEFAULT 0,
      credited_at BIGINT NOT NULL DEFAULT 0
    )
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS player_id TEXT
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS username TEXT
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS source TEXT
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS pack_id TEXT
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS txn_id TEXT
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS txn_norm TEXT
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS status TEXT
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS submitted_at BIGINT
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS reviewed_at BIGINT
  `);
  await db.query(`
    ALTER TABLE claims
    ADD COLUMN IF NOT EXISTS credited_at BIGINT
  `);
  await db.query(`
    UPDATE claims
    SET source = COALESCE(NULLIF(source, ''), 'venmo'),
        status = COALESCE(NULLIF(status, ''), 'pending'),
        submitted_at = COALESCE(submitted_at, 0),
        reviewed_at = COALESCE(reviewed_at, 0),
        credited_at = COALESCE(credited_at, 0)
  `);
  await db.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS claims_txn_norm_unique_idx
    ON claims (txn_norm)
    WHERE txn_norm IS NOT NULL AND txn_norm <> ''
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS feedback_submissions (
      id BIGSERIAL PRIMARY KEY,
      player_id TEXT NOT NULL,
      username TEXT NOT NULL,
      category TEXT NOT NULL DEFAULT 'idea',
      message TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'open',
      submitted_at BIGINT NOT NULL DEFAULT 0,
      reviewed_at BIGINT NOT NULL DEFAULT 0
    )
  `);
  await db.query(`
    ALTER TABLE feedback_submissions
    ADD COLUMN IF NOT EXISTS player_id TEXT
  `);
  await db.query(`
    ALTER TABLE feedback_submissions
    ADD COLUMN IF NOT EXISTS username TEXT
  `);
  await db.query(`
    ALTER TABLE feedback_submissions
    ADD COLUMN IF NOT EXISTS category TEXT
  `);
  await db.query(`
    ALTER TABLE feedback_submissions
    ADD COLUMN IF NOT EXISTS message TEXT
  `);
  await db.query(`
    ALTER TABLE feedback_submissions
    ADD COLUMN IF NOT EXISTS status TEXT
  `);
  await db.query(`
    ALTER TABLE feedback_submissions
    ADD COLUMN IF NOT EXISTS submitted_at BIGINT
  `);
  await db.query(`
    ALTER TABLE feedback_submissions
    ADD COLUMN IF NOT EXISTS reviewed_at BIGINT
  `);
  await db.query(`
    UPDATE feedback_submissions
    SET username = COALESCE(NULLIF(username, ''), 'Player'),
        category = COALESCE(NULLIF(category, ''), 'idea'),
        message = COALESCE(message, ''),
        status = COALESCE(NULLIF(status, ''), 'open'),
        submitted_at = COALESCE(submitted_at, 0),
        reviewed_at = COALESCE(reviewed_at, 0)
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS feedback_submissions_player_submitted_idx
    ON feedback_submissions (player_id, submitted_at DESC, id DESC)
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS feedback_submissions_submitted_idx
    ON feedback_submissions (submitted_at DESC, id DESC)
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS live_wins (
      id BIGSERIAL PRIMARY KEY,
      player_id TEXT NOT NULL,
      username TEXT NOT NULL,
      vip BOOLEAN NOT NULL DEFAULT FALSE,
      game TEXT NOT NULL,
      amount NUMERIC(14,2) NOT NULL,
      multiplier NUMERIC(10,2),
      submitted_at BIGINT NOT NULL DEFAULT 0
    )
  `);
  await db.query(`
    ALTER TABLE live_wins
    ADD COLUMN IF NOT EXISTS player_id TEXT
  `);
  await db.query(`
    ALTER TABLE live_wins
    ADD COLUMN IF NOT EXISTS username TEXT
  `);
  await db.query(`
    ALTER TABLE live_wins
    ADD COLUMN IF NOT EXISTS vip BOOLEAN
  `);
  await db.query(`
    ALTER TABLE live_wins
    ADD COLUMN IF NOT EXISTS game TEXT
  `);
  await db.query(`
    ALTER TABLE live_wins
    ADD COLUMN IF NOT EXISTS amount NUMERIC(14,2)
  `);
  await db.query(`
    ALTER TABLE live_wins
    ADD COLUMN IF NOT EXISTS multiplier NUMERIC(10,2)
  `);
  await db.query(`
    ALTER TABLE live_wins
    ADD COLUMN IF NOT EXISTS submitted_at BIGINT
  `);
  await db.query(`
    UPDATE live_wins
    SET vip = COALESCE(vip, FALSE),
        game = COALESCE(NULLIF(game, ''), 'Casino'),
        amount = COALESCE(amount, 0),
        submitted_at = COALESCE(submitted_at, 0)
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS admin_activity_events (
      id BIGSERIAL PRIMARY KEY,
      event_type TEXT NOT NULL,
      player_id TEXT NOT NULL DEFAULT '',
      username TEXT NOT NULL DEFAULT '',
      actor_player_id TEXT NOT NULL DEFAULT '',
      details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at BIGINT NOT NULL DEFAULT 0
    )
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS admin_activity_events_created_idx
    ON admin_activity_events (created_at DESC, id DESC)
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS admin_activity_events_player_idx
    ON admin_activity_events (player_id, created_at DESC, id DESC)
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS admin_fraud_flags_state (
      id SMALLINT PRIMARY KEY,
      cleared_at BIGINT NOT NULL DEFAULT 0,
      cleared_by TEXT NOT NULL DEFAULT '',
      updated_at BIGINT NOT NULL DEFAULT 0
    )
  `);
  await db.query(`
    ALTER TABLE admin_fraud_flags_state
    ADD COLUMN IF NOT EXISTS id SMALLINT
  `);
  await db.query(`
    ALTER TABLE admin_fraud_flags_state
    ADD COLUMN IF NOT EXISTS cleared_at BIGINT
  `);
  await db.query(`
    ALTER TABLE admin_fraud_flags_state
    ADD COLUMN IF NOT EXISTS cleared_by TEXT
  `);
  await db.query(`
    ALTER TABLE admin_fraud_flags_state
    ADD COLUMN IF NOT EXISTS updated_at BIGINT
  `);
  await db.query(`
    UPDATE admin_fraud_flags_state
    SET cleared_at = COALESCE(cleared_at, 0),
        cleared_by = COALESCE(cleared_by, ''),
        updated_at = COALESCE(updated_at, 0)
  `);
  await db.query(`
    INSERT INTO admin_fraud_flags_state (id, cleared_at, cleared_by, updated_at)
    VALUES (1, 0, '', 0)
    ON CONFLICT (id) DO NOTHING
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS site_visit_events (
      id BIGSERIAL PRIMARY KEY,
      visitor_id TEXT NOT NULL,
      player_id TEXT NOT NULL DEFAULT '',
      username TEXT NOT NULL DEFAULT '',
      path TEXT NOT NULL DEFAULT '/',
      ip TEXT NOT NULL DEFAULT '',
      user_agent TEXT NOT NULL DEFAULT '',
      visited_at BIGINT NOT NULL DEFAULT 0
    )
  `);
  await db.query(`
    ALTER TABLE site_visit_events
    ADD COLUMN IF NOT EXISTS visitor_id TEXT
  `);
  await db.query(`
    ALTER TABLE site_visit_events
    ADD COLUMN IF NOT EXISTS player_id TEXT
  `);
  await db.query(`
    ALTER TABLE site_visit_events
    ADD COLUMN IF NOT EXISTS username TEXT
  `);
  await db.query(`
    ALTER TABLE site_visit_events
    ADD COLUMN IF NOT EXISTS path TEXT
  `);
  await db.query(`
    ALTER TABLE site_visit_events
    ADD COLUMN IF NOT EXISTS ip TEXT
  `);
  await db.query(`
    ALTER TABLE site_visit_events
    ADD COLUMN IF NOT EXISTS user_agent TEXT
  `);
  await db.query(`
    ALTER TABLE site_visit_events
    ADD COLUMN IF NOT EXISTS visited_at BIGINT
  `);
  await db.query(`
    UPDATE site_visit_events
    SET visitor_id = COALESCE(NULLIF(visitor_id, ''), 'unknown'),
        player_id = COALESCE(player_id, ''),
        username = COALESCE(username, ''),
        path = COALESCE(NULLIF(path, ''), '/'),
        ip = COALESCE(ip, ''),
        user_agent = COALESCE(user_agent, ''),
        visited_at = COALESCE(visited_at, 0)
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS site_visit_events_visited_idx
    ON site_visit_events (visited_at DESC, id DESC)
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS site_visit_events_visitor_visited_idx
    ON site_visit_events (visitor_id, visited_at DESC, id DESC)
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS site_visit_events_player_visited_idx
    ON site_visit_events (player_id, visited_at DESC, id DESC)
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS system_runtime_events (
      id BIGSERIAL PRIMARY KEY,
      kind TEXT NOT NULL,
      path TEXT NOT NULL DEFAULT '',
      status_code INTEGER NOT NULL DEFAULT 0,
      player_id TEXT NOT NULL DEFAULT '',
      message TEXT NOT NULL DEFAULT '',
      details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at BIGINT NOT NULL DEFAULT 0
    )
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS system_runtime_events_kind_created_idx
    ON system_runtime_events (kind, created_at DESC, id DESC)
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS system_runtime_events_created_idx
    ON system_runtime_events (created_at DESC, id DESC)
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS balance_audit_events (
      id BIGSERIAL PRIMARY KEY,
      player_id TEXT NOT NULL,
      username TEXT NOT NULL DEFAULT '',
      source TEXT NOT NULL DEFAULT 'unknown',
      before_balance NUMERIC(14,2) NOT NULL DEFAULT 0,
      after_balance NUMERIC(14,2) NOT NULL DEFAULT 0,
      delta NUMERIC(14,2) NOT NULL DEFAULT 0,
      created_at BIGINT NOT NULL DEFAULT 0
    )
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS balance_audit_events_player_created_idx
    ON balance_audit_events (player_id, created_at DESC, id DESC)
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS balance_audit_events_created_idx
    ON balance_audit_events (created_at DESC, id DESC)
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS admin_backups (
      id BIGSERIAL PRIMARY KEY,
      created_by TEXT NOT NULL DEFAULT '',
      summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      data_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at BIGINT NOT NULL DEFAULT 0
    )
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS admin_backups_created_idx
    ON admin_backups (created_at DESC, id DESC)
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS admin_popup_messages (
      id BIGSERIAL PRIMARY KEY,
      target_player_id TEXT NOT NULL DEFAULT '',
      target_username TEXT NOT NULL DEFAULT '',
      message TEXT NOT NULL DEFAULT '',
      created_by TEXT NOT NULL DEFAULT '',
      created_at BIGINT NOT NULL DEFAULT 0,
      active BOOLEAN NOT NULL DEFAULT true
    )
  `);
  await db.query(`
    ALTER TABLE admin_popup_messages
    ADD COLUMN IF NOT EXISTS target_player_id TEXT
  `);
  await db.query(`
    ALTER TABLE admin_popup_messages
    ADD COLUMN IF NOT EXISTS target_username TEXT
  `);
  await db.query(`
    ALTER TABLE admin_popup_messages
    ADD COLUMN IF NOT EXISTS message TEXT
  `);
  await db.query(`
    ALTER TABLE admin_popup_messages
    ADD COLUMN IF NOT EXISTS created_by TEXT
  `);
  await db.query(`
    ALTER TABLE admin_popup_messages
    ADD COLUMN IF NOT EXISTS created_at BIGINT
  `);
  await db.query(`
    ALTER TABLE admin_popup_messages
    ADD COLUMN IF NOT EXISTS active BOOLEAN
  `);
  await db.query(`
    UPDATE admin_popup_messages
    SET target_player_id = COALESCE(target_player_id, ''),
        target_username = COALESCE(target_username, ''),
        message = COALESCE(message, ''),
        created_by = COALESCE(created_by, ''),
        created_at = COALESCE(created_at, 0),
        active = COALESCE(active, true)
  `);
  await db.query(`
    ALTER TABLE admin_popup_messages
    ALTER COLUMN target_player_id SET DEFAULT ''
  `);
  await db.query(`
    ALTER TABLE admin_popup_messages
    ALTER COLUMN target_player_id SET NOT NULL
  `);
  await db.query(`
    ALTER TABLE admin_popup_messages
    ALTER COLUMN target_username SET DEFAULT ''
  `);
  await db.query(`
    ALTER TABLE admin_popup_messages
    ALTER COLUMN target_username SET NOT NULL
  `);
  await db.query(`
    ALTER TABLE admin_popup_messages
    ALTER COLUMN message SET DEFAULT ''
  `);
  await db.query(`
    ALTER TABLE admin_popup_messages
    ALTER COLUMN message SET NOT NULL
  `);
  await db.query(`
    ALTER TABLE admin_popup_messages
    ALTER COLUMN created_by SET DEFAULT ''
  `);
  await db.query(`
    ALTER TABLE admin_popup_messages
    ALTER COLUMN created_by SET NOT NULL
  `);
  await db.query(`
    ALTER TABLE admin_popup_messages
    ALTER COLUMN created_at SET DEFAULT 0
  `);
  await db.query(`
    ALTER TABLE admin_popup_messages
    ALTER COLUMN created_at SET NOT NULL
  `);
  await db.query(`
    ALTER TABLE admin_popup_messages
    ALTER COLUMN active SET DEFAULT true
  `);
  await db.query(`
    ALTER TABLE admin_popup_messages
    ALTER COLUMN active SET NOT NULL
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS admin_popup_messages_active_created_idx
    ON admin_popup_messages (active, created_at DESC, id DESC)
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS admin_popup_messages_target_active_created_idx
    ON admin_popup_messages (target_player_id, active, created_at DESC, id DESC)
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS admin_popup_message_reads (
      message_id BIGINT NOT NULL,
      player_id TEXT NOT NULL,
      read_at BIGINT NOT NULL DEFAULT 0,
      PRIMARY KEY (message_id, player_id)
    )
  `);
  await db.query(`
    ALTER TABLE admin_popup_message_reads
    ADD COLUMN IF NOT EXISTS message_id BIGINT
  `);
  await db.query(`
    ALTER TABLE admin_popup_message_reads
    ADD COLUMN IF NOT EXISTS player_id TEXT
  `);
  await db.query(`
    ALTER TABLE admin_popup_message_reads
    ADD COLUMN IF NOT EXISTS read_at BIGINT
  `);
  await db.query(`
    DELETE FROM admin_popup_message_reads
    WHERE message_id IS NULL
       OR player_id IS NULL
       OR player_id = ''
  `);
  await db.query(`
    UPDATE admin_popup_message_reads
    SET read_at = COALESCE(read_at, 0)
    WHERE read_at IS NULL
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS admin_popup_message_reads_player_read_idx
    ON admin_popup_message_reads (player_id, read_at DESC, message_id DESC)
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS admin_popup_message_reads_message_idx
    ON admin_popup_message_reads (message_id, player_id)
  `);
  await db.query(`
    DELETE FROM admin_popup_message_reads r
    WHERE NOT EXISTS (
      SELECT 1
      FROM admin_popup_messages m
      WHERE m.id = r.message_id
    )
  `);
  await trimAdminPopupReads();
}

async function startServer() {
  try {
    await ensureSchema();
    app.listen(port, () => {
      console.log(`Nights on Wall Street server running at ${baseUrl}`);
    });
  } catch (error) {
    console.error("Failed to start server", error);
    process.exit(1);
  }
}

startServer();
