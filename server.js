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

function mapAdminUserRow(row) {
  const base = mapUserRow(row);
  return {
    ...base,
    isAdmin: row.is_admin === true,
    hasPassword: row.has_password === true,
    bannedAt: Number(row.banned_at) || 0,
    bannedReason: String(row.banned_reason || ""),
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
    active: Number(row.revoked_at) <= 0
  };
}

async function getUserBanStateByPlayerId(playerId) {
  const result = await db.query(
    `
      SELECT player_id, banned_at, banned_reason
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
    bannedReason: String(row.banned_reason || "")
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
    if (tokenHash) {
      const trustedLookup = await db.query(
        `
          SELECT id, player_id, label, created_at, last_seen_at, last_ip, last_user_agent, revoked_at
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
          device: mapAdminDeviceRow(trustedLookup.rows[0])
        };
      }
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
    pattern: /^\/api\/admin\/(claims|users|stats|devices|feedback)$/i,
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
app.use(express.static(__dirname));
app.use("/api", apiRateLimitMiddleware);

app.get("/api/health", (_req, res) => {
  res.json({ ok: true, service: "claims", database: "postgres" });
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
        SELECT player_id, username, username_key, email, balance, shares, avg_cost, savings_balance, auto_savings_percent, last_seen_at, banned_at, banned_reason, is_admin
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
    res.json({
      ok: true,
      authenticated: true,
      user: normalizeUserForClient(lookup.rows[0])
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
        SELECT player_id, username, username_key, email, password_hash, balance, shares, avg_cost, savings_balance, auto_savings_percent, last_seen_at, banned_at, banned_reason, is_admin
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
            RETURNING player_id, username, username_key, email, password_hash, balance, shares, avg_cost, savings_balance, auto_savings_percent, last_seen_at, is_admin
          `,
          [username, usernameKey, replacementHash, Date.now(), existing.player_id]
        );
        req.session.playerId = sanitizePlayerId(updated.rows[0]?.player_id || existing.player_id);
        queueLeaderboardUpdate("register");
        res.json({ ok: true, user: normalizeUserForClient(updated.rows[0] || existing) });
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
        RETURNING player_id, username, username_key, email, password_hash, balance, shares, avg_cost, savings_balance, auto_savings_percent, last_seen_at, is_admin
      `,
      [playerId, username, usernameKey, passwordHash, INITIAL_ACCOUNT_BALANCE, Date.now()]
    );
    req.session.playerId = sanitizePlayerId(upsert.rows[0]?.player_id || playerId);
    queueLeaderboardUpdate("register");
    res.json({ ok: true, user: normalizeUserForClient(upsert.rows[0]) });
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
        SELECT player_id, username, username_key, email, password_hash, balance, shares, avg_cost, savings_balance, auto_savings_percent, last_seen_at, banned_at, banned_reason, is_admin
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
    res.json({ ok: true, user: normalizeUserForClient(user) });
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
        SELECT player_id, username, banned_at, banned_reason
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
  try {
    const claimId = String(req.params?.id || "");
    const playerId = sanitizePlayerId(req.body?.playerId);
    if (!claimId || !playerId) {
      res.status(400).json({ ok: false, error: "Missing claim id or player id." });
      return;
    }
    const lookup = await db.query(
      `
        SELECT id, player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at
        FROM claims
        WHERE id = $1 AND player_id = $2
        LIMIT 1
      `,
      [claimId, playerId]
    );
    if (!lookup.rowCount) {
      res.status(404).json({ ok: false, error: "Claim not found." });
      return;
    }
    const claim = lookup.rows[0];
    if (claim.status !== "approved") {
      res.status(409).json({ ok: false, error: "Claim is not approved." });
      return;
    }
    if (!Number(claim.credited_at)) {
      const updated = await db.query(
        `
          UPDATE claims
          SET credited_at = $1
          WHERE id = $2
          RETURNING id, player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at
        `,
        [Date.now(), claimId]
      );
      res.json({ ok: true, claim: mapClaimRow(updated.rows[0]) });
      return;
    }
    res.json({ ok: true, claim: mapClaimRow(claim) });
  } catch (error) {
    console.error("Failed to ack credit", error);
    res.status(500).json({ ok: false, error: "Could not acknowledge credit." });
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
        SELECT player_id, username, username_key, email, balance, shares, avg_cost, savings_balance, auto_savings_percent, balance_updated_at, last_seen_at, is_admin
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
      const lastUpdatedAt = Number(current.balance_updated_at) || Number(current.last_seen_at) || 0;
      const elapsedMs = Math.max(0, now - lastUpdatedAt);
      const allowedBalanceIncrease = getSyncAllowedIncrease(elapsedMs, BALANCE_SYNC_BURST_UP, BALANCE_SYNC_UP_PER_MIN);
      const allowedSharesIncrease = getSyncAllowedIncrease(elapsedMs, SHARES_SYNC_BURST_UP, SHARES_SYNC_UP_PER_MIN);

      if (nextBalance > currentBalance + allowedBalanceIncrease) {
        res.status(409).json({
          ok: false,
          error: "Balance update rejected by server guard.",
          user: normalizeUserForClient(current)
        });
        return;
      }
      if (nextShares > currentShares + allowedSharesIncrease) {
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
        RETURNING player_id, username, username_key, email, balance, shares, avg_cost, savings_balance, auto_savings_percent, last_seen_at, is_admin
      `,
      [playerId, username, usernameKey, nextBalance, nextShares, nextAvgCost, nextSavingsBalance, nextAutoSavingsPercent, now, hasPortfolioPayload]
    );
    if (!updated.rowCount) {
      res.status(404).json({ ok: false, error: "Session account not found." });
      return;
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
        INSERT INTO admin_trusted_devices (player_id, token_hash, label, created_at, last_seen_at, last_ip, last_user_agent, revoked_at)
        VALUES ($1, $2, $3, $4, $4, $5, $6, 0)
        ON CONFLICT (token_hash)
        DO UPDATE SET
          player_id = EXCLUDED.player_id,
          label = EXCLUDED.label,
          last_seen_at = EXCLUDED.last_seen_at,
          last_ip = EXCLUDED.last_ip,
          last_user_agent = EXCLUDED.last_user_agent,
          revoked_at = 0
        RETURNING id, player_id, label, created_at, last_seen_at, last_ip, last_user_agent
      `,
      [adminAccess.playerId, tokenHash, label, now, getRequestIp(req), getRequestUserAgent(req)]
    );
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
    const result = await db.query(
      `
        SELECT id, player_id, token_hash, label, created_at, last_seen_at, last_ip, last_user_agent, revoked_at
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
      current: Boolean(currentTokenHash && row.token_hash === currentTokenHash)
    }));
    res.json({ ok: true, devices });
  } catch (error) {
    console.error("Failed to load admin devices", error);
    res.status(500).json({ ok: false, error: "Could not load trusted devices." });
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
        RETURNING id, player_id, label, created_at, last_seen_at, last_ip, last_user_agent, revoked_at
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
        RETURNING id, player_id, label, created_at, last_seen_at, last_ip, last_user_agent, revoked_at
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
        RETURNING id, player_id, label, created_at, last_seen_at, last_ip, last_user_agent, revoked_at
      `,
      [deviceId, adminAccess.playerId]
    );
    if (!removed.rowCount) {
      res.status(404).json({ ok: false, error: "Device not found." });
      return;
    }
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
        WHERE sess::text LIKE $1
      `,
      [`%\"playerId\":\"${playerId}\"%`]
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
    const verificationDeleted = await client.query("DELETE FROM email_verification_codes");
    const sessionsDeleted = await client.query(
      `
        DELETE FROM user_sessions
        WHERE sess::text NOT LIKE $1
      `,
      [`%\"playerId\":\"${adminPlayerId}\"%`]
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
              last_seen_at = $2
          WHERE player_id = $1
            AND token_hash = $3
        `,
        [adminPlayerId, now, tokenHash]
      );
    }
    await client.query("COMMIT");
    queueLeaderboardUpdate("reset");
    res.json({
      ok: true,
      reset: {
        usersDeleted: usersDeleted.rowCount,
        claimsDeleted: claimsDeleted.rowCount,
        feedbackDeleted: feedbackDeleted.rowCount,
        liveWinsDeleted: winsDeleted.rowCount,
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
          device_totals.trusted_devices_banned
        FROM user_totals, claim_totals, win_totals, device_totals
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
        trustedDevicesBanned: Number(row.trusted_devices_banned) || 0
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
      banned_reason TEXT NOT NULL DEFAULT ''
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
       OR is_admin IS NULL
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
      revoked_at BIGINT NOT NULL DEFAULT 0
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
    UPDATE admin_trusted_devices
    SET player_id = COALESCE(player_id, ''),
        label = COALESCE(NULLIF(label, ''), 'Trusted device'),
        created_at = COALESCE(created_at, 0),
        last_seen_at = COALESCE(last_seen_at, 0),
        last_ip = COALESCE(last_ip, ''),
        last_user_agent = COALESCE(last_user_agent, ''),
        revoked_at = COALESCE(revoked_at, 0)
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
    CREATE UNIQUE INDEX IF NOT EXISTS admin_trusted_devices_token_hash_idx
    ON admin_trusted_devices (token_hash)
    WHERE token_hash IS NOT NULL AND token_hash <> ''
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
