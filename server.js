import dotenv from "dotenv";
import express from "express";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const app = express();
const port = Number(process.env.PORT || 3000);
const baseUrl = process.env.APP_BASE_URL || `http://localhost:${port}`;
const ADMIN_CODE = String(process.env.VENMO_ADMIN_CODE || "Sage1557");

const STORE_PACKAGES = Object.freeze({
  small: { usd: 2, funds: 1000, label: "Starter Funds Pack" },
  medium: { usd: 8, funds: 5000, label: "Trader Funds Pack" },
  xlarge: { usd: 18, funds: 10000, label: "Pro Funds Pack" },
  large: { usd: 40, funds: 25000, label: "Whale Funds Pack" }
});
const PACKAGE_ALIASES = Object.freeze({
  xl: "xlarge",
  "x-large": "xlarge",
  "extra-large": "xlarge",
  pro: "xlarge",
  whale: "large",
  starter: "small",
  trader: "medium"
});
const claimsDbPath = path.join(__dirname, ".venmo-claims-db.json");

function defaultClaimsDb() {
  return {
    nextId: 1,
    claims: []
  };
}

function loadClaimsDb() {
  try {
    const raw = fs.readFileSync(claimsDbPath, "utf8");
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return defaultClaimsDb();
    if (!Array.isArray(parsed.claims)) parsed.claims = [];
    if (!Number.isFinite(parsed.nextId) || parsed.nextId < 1) parsed.nextId = 1;
    return parsed;
  } catch {
    return defaultClaimsDb();
  }
}

function saveClaimsDb(db) {
  fs.writeFileSync(claimsDbPath, JSON.stringify(db, null, 2), "utf8");
}

function normalizeTxn(value) {
  return String(value || "").trim().toLowerCase().replace(/\s+/g, "");
}

function sanitizePlayerId(value) {
  const id = String(value || "").trim();
  if (!id) return "";
  if (!/^[a-zA-Z0-9_-]{6,120}$/.test(id)) return "";
  return id;
}

function normalizePackageId(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return "";
  if (STORE_PACKAGES[raw]) return raw;
  const mapped = PACKAGE_ALIASES[raw];
  return mapped && STORE_PACKAGES[mapped] ? mapped : "";
}

function claimWithPack(claim) {
  const pack = STORE_PACKAGES[claim.packId] || null;
  return {
    ...claim,
    funds: pack?.funds || 0,
    usd: pack?.usd || 0
  };
}

function getAdminCode(req) {
  return String(req.query?.adminCode || req.body?.adminCode || "");
}

function requireAdmin(req, res) {
  if (getAdminCode(req) !== ADMIN_CODE) {
    res.status(401).json({ ok: false, error: "Invalid admin code." });
    return false;
  }
  return true;
}

app.use(express.json());
app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
  if (req.method === "OPTIONS") {
    res.sendStatus(204);
    return;
  }
  next();
});
app.use(express.static(__dirname));

app.get("/api/health", (_req, res) => {
  res.json({ ok: true, service: "claims" });
});

app.post("/api/claims", (req, res) => {
  try {
    const db = loadClaimsDb();
    const playerId = sanitizePlayerId(req.body?.playerId);
    const packageId = normalizePackageId(
      req.body?.packageId ||
      req.body?.packId ||
      req.body?.pack ||
      req.body?.package
    );
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
    const dup = db.claims.find((claim) => claim.txnNorm === txnNorm);
    if (dup) {
      res.status(409).json({ ok: false, error: "Transaction id already claimed." });
      return;
    }

    const claim = {
      id: String(db.nextId++),
      playerId,
      packId: packageId,
      txnId,
      txnNorm,
      status: "pending",
      submittedAt: Date.now(),
      reviewedAt: 0,
      creditedAt: 0
    };
    db.claims.push(claim);
    saveClaimsDb(db);
    res.json({ ok: true, claim: claimWithPack(claim) });
  } catch (error) {
    console.error("Failed to submit claim", error);
    res.status(500).json({ ok: false, error: "Could not submit claim." });
  }
});

app.get("/api/claims", (req, res) => {
  try {
    const playerId = sanitizePlayerId(req.query?.playerId);
    if (!playerId) {
      res.status(400).json({ ok: false, error: "Missing or invalid playerId." });
      return;
    }
    const db = loadClaimsDb();
    const claims = db.claims
      .filter((claim) => claim.playerId === playerId)
      .sort((a, b) => b.submittedAt - a.submittedAt)
      .map(claimWithPack);
    res.json({
      ok: true,
      claims
    });
  } catch (error) {
    console.error("Failed to list claims", error);
    res.status(500).json({ ok: false, error: "Could not load claims." });
  }
});

app.get("/api/claims/credits", (req, res) => {
  try {
    const playerId = sanitizePlayerId(req.query?.playerId);
    if (!playerId) {
      res.status(400).json({ ok: false, error: "Missing or invalid playerId." });
      return;
    }
    const db = loadClaimsDb();
    const claims = db.claims
      .filter((claim) => claim.playerId === playerId && claim.status === "approved" && !claim.creditedAt)
      .map(claimWithPack);
    res.json({ ok: true, claims });
  } catch (error) {
    console.error("Failed to list credits", error);
    res.status(500).json({ ok: false, error: "Could not load approved credits." });
  }
});

app.post("/api/claims/credits/:id/ack", (req, res) => {
  try {
    const claimId = String(req.params?.id || "");
    const playerId = sanitizePlayerId(req.body?.playerId);
    if (!claimId || !playerId) {
      res.status(400).json({ ok: false, error: "Missing claim id or player id." });
      return;
    }
    const db = loadClaimsDb();
    const claim = db.claims.find((entry) => entry.id === claimId && entry.playerId === playerId);
    if (!claim) {
      res.status(404).json({ ok: false, error: "Claim not found." });
      return;
    }
    if (claim.status !== "approved") {
      res.status(409).json({ ok: false, error: "Claim is not approved." });
      return;
    }
    if (!claim.creditedAt) {
      claim.creditedAt = Date.now();
      saveClaimsDb(db);
    }
    res.json({ ok: true, claim: claimWithPack(claim) });
  } catch (error) {
    console.error("Failed to ack credit", error);
    res.status(500).json({ ok: false, error: "Could not acknowledge credit." });
  }
});

app.get("/api/admin/claims", (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    const statusFilter = String(req.query?.status || "").trim().toLowerCase();
    const db = loadClaimsDb();
    const claims = db.claims
      .filter((claim) => !statusFilter || claim.status === statusFilter)
      .sort((a, b) => b.submittedAt - a.submittedAt)
      .map(claimWithPack);
    res.json({ ok: true, claims });
  } catch (error) {
    console.error("Failed to load admin claims", error);
    res.status(500).json({ ok: false, error: "Could not load admin claims." });
  }
});

app.post("/api/admin/claims/:id/decision", (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    const claimId = String(req.params?.id || "");
    const decision = String(req.body?.decision || "").toLowerCase();
    if (!["approve", "reject"].includes(decision)) {
      res.status(400).json({ ok: false, error: "Decision must be approve or reject." });
      return;
    }
    const db = loadClaimsDb();
    const claim = db.claims.find((entry) => entry.id === claimId);
    if (!claim) {
      res.status(404).json({ ok: false, error: "Claim not found." });
      return;
    }
    if (claim.status !== "pending") {
      res.status(409).json({ ok: false, error: "Claim is already reviewed." });
      return;
    }
    claim.status = decision === "approve" ? "approved" : "rejected";
    claim.reviewedAt = Date.now();
    saveClaimsDb(db);
    res.json({ ok: true, claim: claimWithPack(claim) });
  } catch (error) {
    console.error("Failed to update claim decision", error);
    res.status(500).json({ ok: false, error: "Could not update claim decision." });
  }
});

app.listen(port, () => {
  console.log(`Nights on Wall Street server running at ${baseUrl}`);
});
