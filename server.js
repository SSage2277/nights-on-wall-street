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
const ADMIN_CODE = String(process.env.VENMO_ADMIN_CODE || "Sage1557");
const DATABASE_URL = String(process.env.DATABASE_URL || "").trim();
const SESSION_SECRET = String(process.env.SESSION_SECRET || "").trim();

if (!DATABASE_URL) {
  throw new Error("Missing DATABASE_URL environment variable.");
}
if (!SESSION_SECRET) {
  throw new Error("Missing SESSION_SECRET environment variable.");
}

const db = new Pool({
  connectionString: DATABASE_URL,
  ssl: DATABASE_URL.includes("localhost") ? false : { rejectUnauthorized: false }
});

const STORE_PACKAGES = Object.freeze({
  small: { usd: 2, funds: 1000, label: "Starter Funds Pack" },
  medium: { usd: 8, funds: 5000, label: "Trader Funds Pack" },
  xlarge: { usd: 18, funds: 10000, label: "Pro Funds Pack" },
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
  const email = String(value || "").trim().toLowerCase();
  if (!email) return "";
  const isValid = /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
  return isValid ? email : "";
}

function sanitizePassword(value) {
  const password = String(value || "");
  if (!password) return "";
  if (password.length < 8 || password.length > 72) return "";
  return password;
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
    lastSeenAt: Number(row.last_seen_at) || 0
  };
}

function normalizeUserForClient(row) {
  const mapped = mapUserRow(row);
  return {
    playerId: mapped.playerId,
    username: mapped.username,
    email: mapped.email,
    balance: mapped.balance,
    lastSeenAt: mapped.lastSeenAt
  };
}

function getAdminCode(req) {
  return String(req.query?.adminCode || req.body?.adminCode || "");
}

function requireAdmin(req, res) {
  const entered = getAdminCode(req).trim().toLowerCase();
  const expected = ADMIN_CODE.trim().toLowerCase();
  if (!entered || entered !== expected) {
    res.status(401).json({ ok: false, error: "Invalid admin code." });
    return false;
  }
  return true;
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
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
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
      sameSite: "lax",
      secure: process.env.NODE_ENV === "production",
      maxAge: 1000 * 60 * 60 * 24 * 30
    }
  })
);
app.use(express.static(__dirname));

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
        SELECT player_id, username, username_key, email, balance, last_seen_at
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
    const email = sanitizeEmail(req.body?.email);
    const password = sanitizePassword(req.body?.password);
    const username = sanitizeUsername(req.body?.username);
    const usernameKey = normalizeUsernameKey(username);
    const balance = Number(req.body?.balance);
    const providedPlayerId = sanitizePlayerId(req.body?.playerId);
    const playerId = providedPlayerId || `u_${crypto.randomUUID().replace(/-/g, "")}`;
    if (!email) {
      res.status(400).json({ ok: false, error: "Invalid email address." });
      return;
    }
    if (!password) {
      res.status(400).json({ ok: false, error: "Password must be 8-72 characters." });
      return;
    }
    if (!username || !usernameKey) {
      res.status(400).json({ ok: false, error: "Invalid username." });
      return;
    }
    const safeBalance = Number.isFinite(balance) && balance >= 0 ? Math.round(balance * 100) / 100 : 0;
    const passwordHash = await bcrypt.hash(password, 12);
    const existingByPlayerId = await db.query(
      `
        SELECT player_id, email, password_hash
        FROM users
        WHERE player_id = $1
        LIMIT 1
      `,
      [playerId]
    );
    if (existingByPlayerId.rowCount) {
      const existing = existingByPlayerId.rows[0];
      const hasCredentials = Boolean(sanitizeEmail(existing.email) && String(existing.password_hash || "").trim());
      if (hasCredentials) {
        res.status(409).json({ ok: false, error: "Account already exists for this player. Please login." });
        return;
      }
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
    const upsert = await db.query(
      `
        INSERT INTO users (player_id, username, username_key, email, password_hash, balance, last_seen_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (player_id)
        DO UPDATE SET
          username = EXCLUDED.username,
          username_key = EXCLUDED.username_key,
          email = EXCLUDED.email,
          password_hash = EXCLUDED.password_hash,
          balance = CASE
            WHEN users.balance > EXCLUDED.balance THEN users.balance
            ELSE EXCLUDED.balance
          END,
          last_seen_at = EXCLUDED.last_seen_at
        RETURNING player_id, username, username_key, email, balance, last_seen_at
      `,
      [playerId, username, usernameKey, email, passwordHash, safeBalance, Date.now()]
    );
    req.session.playerId = playerId;
    res.json({
      ok: true,
      user: normalizeUserForClient(upsert.rows[0])
    });
  } catch (error) {
    if (error?.code === "23505") {
      const message = String(error?.constraint || "").includes("email")
        ? "Email already in use."
        : "Username already taken.";
      res.status(409).json({ ok: false, error: message });
      return;
    }
    console.error("Failed to register auth user", error);
    res.status(500).json({ ok: false, error: "Could not register account." });
  }
});

app.post("/api/auth/login", async (req, res) => {
  try {
    const email = sanitizeEmail(req.body?.email);
    const password = sanitizePassword(req.body?.password);
    if (!email || !password) {
      res.status(400).json({ ok: false, error: "Invalid email or password." });
      return;
    }
    const lookup = await db.query(
      `
        SELECT player_id, username, username_key, email, password_hash, balance, last_seen_at
        FROM users
        WHERE lower(email) = lower($1)
        LIMIT 1
      `,
      [email]
    );
    if (!lookup.rowCount) {
      res.status(401).json({ ok: false, error: "Invalid email or password." });
      return;
    }
    const user = lookup.rows[0];
    const storedHash = String(user.password_hash || "");
    if (!storedHash) {
      res.status(401).json({ ok: false, error: "Account password is not set." });
      return;
    }
    const isMatch = await bcrypt.compare(password, storedHash);
    if (!isMatch) {
      res.status(401).json({ ok: false, error: "Invalid email or password." });
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
    res.json({ ok: true, user: normalizeUserForClient(user) });
  } catch (error) {
    console.error("Failed to login", error);
    res.status(500).json({ ok: false, error: "Could not login." });
  }
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
    const playerId = sanitizePlayerId(req.body?.playerId);
    const sessionPlayerId = getSessionPlayerId(req);
    const username = sanitizeUsername(req.body?.username);
    const usernameKey = normalizeUsernameKey(username);
    const balance = Number(req.body?.balance);
    if (!playerId || !username || !usernameKey || !Number.isFinite(balance) || balance < 0) {
      res.status(400).json({ ok: false, error: "Invalid user sync payload." });
      return;
    }
    if (sessionPlayerId && sessionPlayerId !== playerId) {
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
    const upsert = await db.query(
      `
        INSERT INTO users (player_id, username, username_key, balance, last_seen_at, email, password_hash)
        VALUES ($1, $2, $3, $4, $5, NULL, NULL)
        ON CONFLICT (player_id)
        DO UPDATE SET
          username = EXCLUDED.username,
          username_key = EXCLUDED.username_key,
          balance = EXCLUDED.balance,
          last_seen_at = EXCLUDED.last_seen_at
        RETURNING player_id, username, username_key, email, balance, last_seen_at
      `,
      [playerId, username, usernameKey, Math.round(balance * 100) / 100, Date.now()]
    );
    const user = mapUserRow(upsert.rows[0]);
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
    if (!requireAdmin(req, res)) return;
    const statusFilter = String(req.query?.status || "").trim().toLowerCase();
    const result = statusFilter
      ? await db.query(
          `
            SELECT id, player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at
            FROM claims
            WHERE status = $1
            ORDER BY submitted_at DESC, id DESC
          `,
          [statusFilter]
        )
      : await db.query(
          `
            SELECT id, player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at
            FROM claims
            ORDER BY submitted_at DESC, id DESC
          `
        );
    const claims = result.rows.map(mapClaimRow);
    res.json({ ok: true, claims });
  } catch (error) {
    console.error("Failed to load admin claims", error);
    res.status(500).json({ ok: false, error: "Could not load admin claims." });
  }
});

app.get("/api/admin/users", async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    const result = await db.query(
      `
        SELECT player_id, username, username_key, balance, last_seen_at
        FROM users
        ORDER BY last_seen_at DESC, player_id DESC
      `
    );
    const users = result.rows.map(mapUserRow);
    res.json({ ok: true, users });
  } catch (error) {
    console.error("Failed to load admin users", error);
    res.status(500).json({ ok: false, error: "Could not load users." });
  }
});

app.post("/api/admin/claims/:id/decision", async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
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
    res.json({ ok: true, claim: mapClaimRow(updated.rows[0]) });
  } catch (error) {
    console.error("Failed to update claim decision", error);
    res.status(500).json({ ok: false, error: "Could not update claim decision." });
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
      balance NUMERIC(14,2) NOT NULL DEFAULT 0,
      last_seen_at BIGINT NOT NULL DEFAULT 0
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
    CREATE UNIQUE INDEX IF NOT EXISTS users_email_unique_idx
    ON users (lower(email))
    WHERE email IS NOT NULL
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
