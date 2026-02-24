import dotenv from "dotenv";
import fs from "node:fs";
import path from "node:path";
import { Pool } from "pg";

dotenv.config();

const DATABASE_URL = String(process.env.DATABASE_URL || "").trim();
if (!DATABASE_URL) {
  throw new Error("Missing DATABASE_URL environment variable.");
}

const rootDir = process.cwd();
const jsonPath = path.join(rootDir, ".venmo-claims-db.json");
if (!fs.existsSync(jsonPath)) {
  console.log(`No legacy JSON db found at ${jsonPath}. Nothing to migrate.`);
  process.exit(0);
}

const parseJson = () => {
  const raw = fs.readFileSync(jsonPath, "utf8");
  const parsed = JSON.parse(raw);
  return {
    users: Array.isArray(parsed?.users) ? parsed.users : [],
    claims: Array.isArray(parsed?.claims) ? parsed.claims : [],
    wins: Array.isArray(parsed?.wins) ? parsed.wins : []
  };
};

const safeUsername = (value) => {
  const username = String(value || "").replace(/\s+/g, " ").trim();
  if (!username) return "";
  if (username.length < 3 || username.length > 16) return "";
  return username;
};

const safePlayerId = (value) => {
  const playerId = String(value || "").trim();
  if (!/^[a-zA-Z0-9_-]{6,120}$/.test(playerId)) return "";
  return playerId;
};

const safeTxnNorm = (value) => String(value || "").trim().toLowerCase().replace(/\s+/g, "");

const toMoney = (value) => {
  const number = Number(value);
  return Number.isFinite(number) ? Math.round(number * 100) / 100 : 0;
};

const toMillis = (value) => {
  const number = Number(value);
  return Number.isFinite(number) && number > 0 ? Math.floor(number) : Date.now();
};

async function run() {
  const pool = new Pool({
    connectionString: DATABASE_URL,
    ssl: DATABASE_URL.includes("localhost") ? false : { rejectUnauthorized: false }
  });

  const payload = parseJson();
  let usersInserted = 0;
  let claimsInserted = 0;
  let winsInserted = 0;

  try {
    for (const user of payload.users) {
      const playerId = safePlayerId(user?.playerId);
      const username = safeUsername(user?.username);
      const usernameKey = username.toLowerCase();
      if (!playerId || !username || !usernameKey) continue;
      const result = await pool.query(
        `
          INSERT INTO users (player_id, username, username_key, balance, last_seen_at)
          VALUES ($1, $2, $3, $4, $5)
          ON CONFLICT (player_id)
          DO UPDATE SET
            username = EXCLUDED.username,
            username_key = EXCLUDED.username_key,
            balance = EXCLUDED.balance,
            last_seen_at = EXCLUDED.last_seen_at
        `,
        [
          playerId,
          username,
          usernameKey,
          toMoney(user?.balance),
          toMillis(user?.lastSeenAt)
        ]
      );
      usersInserted += result.rowCount || 0;
    }

    for (const claim of payload.claims) {
      const playerId = safePlayerId(claim?.playerId);
      const username = safeUsername(claim?.username) || playerId;
      const packId = String(claim?.packId || "").trim().toLowerCase();
      const txnId = String(claim?.txnId || "").trim();
      const txnNorm = safeTxnNorm(claim?.txnNorm || txnId);
      if (!playerId || !username || !packId || !txnId || txnNorm.length < 4) continue;
      const status = ["pending", "approved", "rejected"].includes(String(claim?.status || "").toLowerCase())
        ? String(claim?.status).toLowerCase()
        : "pending";
      const source = String(claim?.source || "venmo").trim().toLowerCase() || "venmo";
      const result = await pool.query(
        `
          INSERT INTO claims (player_id, username, source, pack_id, txn_id, txn_norm, status, submitted_at, reviewed_at, credited_at)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
          ON CONFLICT (txn_norm)
          DO UPDATE SET
            player_id = EXCLUDED.player_id,
            username = EXCLUDED.username,
            source = EXCLUDED.source,
            pack_id = EXCLUDED.pack_id,
            txn_id = EXCLUDED.txn_id,
            status = EXCLUDED.status,
            submitted_at = EXCLUDED.submitted_at,
            reviewed_at = EXCLUDED.reviewed_at,
            credited_at = EXCLUDED.credited_at
        `,
        [
          playerId,
          username,
          source,
          packId,
          txnId,
          txnNorm,
          status,
          toMillis(claim?.submittedAt),
          Math.max(0, Number(claim?.reviewedAt) || 0),
          Math.max(0, Number(claim?.creditedAt) || 0)
        ]
      );
      claimsInserted += result.rowCount || 0;
    }

    for (const win of payload.wins) {
      const playerId = safePlayerId(win?.playerId);
      const username = safeUsername(win?.username);
      if (!playerId || !username) continue;
      const game = String(win?.game || "Casino").replace(/\s+/g, " ").trim().slice(0, 24) || "Casino";
      const amount = toMoney(win?.amount);
      const multiplierRaw = Number(win?.multiplier);
      const multiplier = Number.isFinite(multiplierRaw) && multiplierRaw > 0
        ? Math.round(multiplierRaw * 100) / 100
        : null;
      const submittedAt = toMillis(win?.submittedAt);

      const result = await pool.query(
        `
          INSERT INTO live_wins (player_id, username, vip, game, amount, multiplier, submitted_at)
          SELECT $1, $2, $3, $4, $5, $6, $7
          WHERE NOT EXISTS (
            SELECT 1
            FROM live_wins
            WHERE player_id = $1
              AND username = $2
              AND game = $4
              AND amount = $5
              AND (
                (multiplier IS NULL AND $6 IS NULL)
                OR multiplier = $6
              )
              AND submitted_at = $7
          )
        `,
        [playerId, username, win?.vip === true, game, amount, multiplier, submittedAt]
      );
      winsInserted += result.rowCount || 0;
    }

    console.log(`Migration complete.
- Users upserted: ${usersInserted}
- Claims upserted: ${claimsInserted}
- Live wins inserted: ${winsInserted}`);
  } finally {
    await pool.end();
  }
}

run().catch((error) => {
  console.error("Migration failed:", error);
  process.exit(1);
});
