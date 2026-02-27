// =====================================================
// =============== GLOBAL GAME STATE ===================
// =====================================================
const canvas = document.getElementById("chart");
const ctx = canvas.getContext("2d");

let cash = 1000;
let shares = 0;
let avgCost = 0;
let price = 100;
let savingsBalance = 0;
let autoSavingsPercent = 0;

let autoPlay = false;
let candles = [];
let marketInterval;
let currentInterval = 750;
let activeCasinoCleanup = null;
const URL_QUERY = new URLSearchParams(window.location.search);
const PHONE_EMBED_GAME_PARAM = URL_QUERY.get("phoneMiniGame");
const IS_PHONE_EMBED_MODE = Boolean(PHONE_EMBED_GAME_PARAM);
const PHONE_EMBED_HOST_PARAM = (URL_QUERY.get("phoneMiniHost") || "").toLowerCase();
const PHONE_EMBED_INITIAL_CASH = Number(URL_QUERY.get("phoneMiniCash"));
if (IS_PHONE_EMBED_MODE && Number.isFinite(PHONE_EMBED_INITIAL_CASH) && PHONE_EMBED_INITIAL_CASH >= 0) {
  cash = Math.round(PHONE_EMBED_INITIAL_CASH * 100) / 100;
}
const IS_PHONE_MINI_IFRAME_HOSTED =
  IS_PHONE_EMBED_MODE &&
  PHONE_EMBED_HOST_PARAM === "panel" &&
  (() => {
    try {
      return window.parent && window.parent !== window;
    } catch (error) {
      return false;
    }
  })();
const PHONE_EMBED_GAME_TITLES = {
  blackjack: "Blackjack",
  slots: "Slots",
  dragontower: "Dragon Tower",
  horseracing: "Horse Racing",
  dice: "Dice",
  slide: "Slide",
  crash: "Crash",
  mines: "Mines",
  crossyroad: "Crossy Road",
  roulette: "Roulette",
  diamonds: "Diamond Match",
  plinko: "Plinko",
  hilo: "Hi-Lo",
  keno: "Keno"
};

const HIGH_BET_RIG_THRESHOLD = 2000;
const HIGH_BET_RIG_BASE_CHANCE = 0.14;
const HIGH_BET_RIG_MAX_CHANCE = 0.78;
const CURRENCY_SYMBOL = "S$";
const USERNAME_STORAGE_KEY = "nows_username_v1";
const USERNAME_STORAGE_FALLBACK_KEYS = Object.freeze([
  USERNAME_STORAGE_KEY,
  "nows_username",
  "username",
  "playerUsername",
  "player_username"
]);
const USERNAME_MIN_LEN = 3;
const USERNAME_MAX_LEN = 16;
const USERNAME_SETUP_DONE_STORAGE_KEY = "nows_username_setup_done_v1";
const USERNAME_SETUP_DONE_FALLBACK_KEYS = Object.freeze([
  USERNAME_SETUP_DONE_STORAGE_KEY,
  "nows_username_setup_done",
  "username_setup_done"
]);
const GUEST_MODE_STORAGE_KEY = "nows_guest_mode_v1";
const FIRST_PLAY_TUTORIAL_VERSION = 2;
const FIRST_PLAY_TUTORIAL_SEEN_STORAGE_KEY = `nows_first_play_tutorial_seen_v${FIRST_PLAY_TUTORIAL_VERSION}`;
const ADMIN_DEVICE_TOKEN_STORAGE_KEY = "nows_admin_device_token_v1";
const ADMIN_DEVICE_LABEL_STORAGE_KEY = "nows_admin_device_label_v1";
const LOANS_ENABLED = false;
const BASE_NET_WORTH = 1000;
const CASINO_UNLOCK_TRADING_TOTAL = 1500;
const CASINO_EARLY_UNLOCK_STORAGE_KEY = "casino_early_unlock_v1";
let casinoEarlyUnlockOverride = false;
let activeCasinoGameKey = "lobby";
const CASINO_LIVE_FEED_LIMIT = 40;
const CASINO_LEADERBOARD_LIMIT = 10;
const CASINO_LEADERBOARD_POLL_MS = 1000;
const casinoLiveFeedEntries = [];
const casinoLeaderboardEntries = [];
let casinoLeaderboardCurrentUser = null;
let casinoLiveFeedTimer = null;
let casinoLeaderboardTimer = null;
let casinoLiveFeedRequestInFlight = false;
let casinoLeaderboardRequestInFlight = false;
let casinoLeaderboardStream = null;
let casinoLeaderboardStreamReconnectTimer = null;
let playerUsername = "";
let usernameGateActive = false;
let firstLaunchAuthMode = "register";
let firstLaunchAuthBusy = false;
let firstPlayTutorialCurrentKey = "";
let firstPlayTutorialStepIndex = 0;
let firstPlayTutorialHighlightedTarget = null;
let tradingLogoutBusy = false;
let authSessionLikelyAuthenticated = false;
let authSessionWatchdogTimer = null;
let authSessionWatchdogBusy = false;
let lastServerBalanceUpdatedAt = 0;
let bannedOverlayActive = false;
let antiTamperGuardsInitialized = false;
let antiTamperLastNoticeAt = 0;
let antiTamperDevtoolsOpen = false;
let antiTamperDevtoolsTimer = null;
let antiTamperDetectedStreak = 0;
let antiTamperClearStreak = 0;
let antiTamperOverlayEl = null;
let antiTamperPresentationReady = false;
let antiTamperScanArmedUntil = 0;
const ANTI_TAMPER_ARM_MS = 45000;
const VIP_COST = 5000;
const VIP_WEEKLY_BONUS = 100;
const VIP_WEEKLY_MS = 7 * 24 * 60 * 60 * 1000;
const FIRST_PLAY_TUTORIAL_STEPS = Object.freeze([
  {
    title: "Welcome to Nights on Wall Street",
    body: "This guide points at the main controls. Click Next to follow the highlights.",
    selector: "#nav"
  },
  {
    title: "Live Cash",
    body: "Watch this cash stat while you trade and play. It updates from your saved account.",
    selector: "#cash"
  },
  {
    title: "Trading Buttons",
    body: "Use Buy/Sell controls to open and close positions. Your net worth updates in real time.",
    selector: "#controls"
  },
  {
    title: "Phone Apps",
    body: "Open the Phone here for Banking, Portfolio, Casino quick launch, Missions, and Feedback.",
    selector: "#phoneBtn"
  },
  {
    title: "Bank & Savings",
    body: "Use Bank to move money to savings, set auto-save %, and manage purchases/claims.",
    selector: "#bankToggleBtn"
  },
  {
    title: "Casino Unlock Rule",
    body: "Important: you must reach at least S$1,500 total on trading before you can enter Casino.",
    selector: "#casinoBtn"
  },
  {
    title: "Done",
    body: "You’re ready. Trade up to S$1,500, then start casino games. Good luck.",
    selector: "#tradingBtn"
  }
]);

const GAME_SOUND_ASSETS = Object.freeze({
  car_screech: "Sound Effects/car_screech.mp3",
  card_deal: "Sound Effects/card_deal.mp3",
  chicken: "Sound Effects/chicken.mp3",
  chicken_dead: "Sound Effects/chicken_dead.mp3",
  click: "Sound Effects/click.mp3",
  horse_start: "Sound Effects/horse_start.mp3",
  loss: "Sound Effects/loss.mp3",
  mines_explosion: "Sound Effects/mines_explosion.mp3",
  plinko_pop: "Sound Effects/plinko_pop.mp3",
  roulette_spin: "Sound Effects/roulette_spin.mp3",
  slide_spin: "Sound Effects/slide_spin.mp3",
  slots_spin: "Sound Effects/slots_spin.mp3",
  win: "Sound Effects/Win.mp3"
});

const GAME_SOUND_DEFAULTS = Object.freeze({
  car_screech: { volume: 0.62, allowOverlap: false, restart: true, throttleMs: 180 },
  card_deal: { volume: 0.6, allowOverlap: true, throttleMs: 50 },
  chicken: { volume: 0.62, allowOverlap: false, restart: true, throttleMs: 90 },
  chicken_dead: { volume: 0.72, allowOverlap: false, restart: true, throttleMs: 220 },
  click: { volume: 0.45, allowOverlap: false, restart: true, throttleMs: 60 },
  horse_start: { volume: 0.72, allowOverlap: false, restart: true, throttleMs: 240 },
  loss: { volume: 0.66, allowOverlap: true, throttleMs: 150 },
  mines_explosion: { volume: 0.78, allowOverlap: false, restart: true, throttleMs: 200 },
  plinko_pop: { volume: 0.58, allowOverlap: true, throttleMs: 80 },
  roulette_spin: { volume: 0.66, allowOverlap: false, restart: true, throttleMs: 300 },
  slide_spin: { volume: 0.66, allowOverlap: false, restart: true, throttleMs: 300 },
  slots_spin: { volume: 0.66, allowOverlap: false, restart: true, throttleMs: 300 },
  win: { volume: 0.72, allowOverlap: true, throttleMs: 120 }
});

const gameSoundBasePool = new Map();
const gameSoundCooldowns = new Map();
const missingGameSoundKeys = new Set();
const GAME_CLICK_SOUND_PATTERN = /\b(bet|spin|deal|draw|roll|drop|raise|call|cash\s*out|start|play)\b/i;
let gameSoundArmed = false;

function clampUnit(value, fallback = 1) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return fallback;
  return Math.max(0, Math.min(1, numeric));
}

function armGameAudio() {
  gameSoundArmed = true;
}

document.addEventListener("pointerdown", armGameAudio, { passive: true, once: true });
document.addEventListener("keydown", armGameAudio, { once: true });

function getGameSoundConfig(soundKey) {
  if (!soundKey) return null;
  const assetPath = GAME_SOUND_ASSETS[soundKey];
  if (!assetPath) return null;
  return {
    key: soundKey,
    assetPath,
    ...GAME_SOUND_DEFAULTS[soundKey]
  };
}

function getGameSoundBase(soundKey) {
  if (gameSoundBasePool.has(soundKey)) return gameSoundBasePool.get(soundKey);
  const config = getGameSoundConfig(soundKey);
  if (!config) return null;

  try {
    const base = new Audio(encodeURI(config.assetPath));
    base.preload = "auto";
    base.volume = clampUnit(config.volume, 1);
    gameSoundBasePool.set(soundKey, base);
    return base;
  } catch (error) {
    if (!missingGameSoundKeys.has(soundKey)) {
      missingGameSoundKeys.add(soundKey);
      console.warn(`[audio] Could not initialize sound "${soundKey}".`, error);
    }
  }
  return null;
}

function playGameSound(soundKey, options = {}) {
  const config = getGameSoundConfig(soundKey);
  if (!config) return;
  if (!gameSoundArmed && options.requireArmed !== false) return;

  const throttleMs = Math.max(0, Number(options.throttleMs ?? config.throttleMs ?? 0));
  if (throttleMs > 0) {
    const cooldownKey = String(options.cooldownKey || soundKey);
    const nowMs = Date.now();
    const coolUntil = Number(gameSoundCooldowns.get(cooldownKey) || 0);
    if (coolUntil > nowMs) return;
    gameSoundCooldowns.set(cooldownKey, nowMs + throttleMs);
  }

  const baseAudio = getGameSoundBase(soundKey);
  if (!baseAudio) return;

  const allowOverlap = options.allowOverlap ?? config.allowOverlap ?? true;
  const restart = options.restart ?? config.restart ?? false;
  const volume = clampUnit(options.volume ?? config.volume, 1);

  try {
    const target = allowOverlap ? baseAudio.cloneNode(true) : baseAudio;
    if (!allowOverlap && restart) target.currentTime = 0;
    target.volume = volume;
    const playback = target.play();
    if (playback && typeof playback.catch === "function") {
      playback.catch(() => {});
    }
  } catch (error) {
    if (!missingGameSoundKeys.has(soundKey)) {
      missingGameSoundKeys.add(soundKey);
      console.warn(`[audio] Failed to play sound "${soundKey}".`, error);
    }
  }
}

function getSoundTextFromElement(element) {
  if (!element) return "";
  const text = [
    element.getAttribute("aria-label") || "",
    element.textContent || "",
    element.id || "",
    element.className || "",
    element.dataset?.action || "",
    element.dataset?.betAction || "",
    element.dataset?.betKey || ""
  ]
    .join(" ")
    .replace(/\s+/g, " ")
    .trim();
  return text.toLowerCase();
}

function maybePlayCasinoBetClickSound(target) {
  const trigger = target?.closest?.('button, [role="button"], input[type="button"], input[type="submit"]');
  if (!trigger || trigger.disabled) return;

  const inActiveCasinoGame =
    IS_PHONE_EMBED_MODE || (typeof activeCasinoGameKey === "string" && activeCasinoGameKey !== "lobby");
  if (!inActiveCasinoGame) return;
  if (activeCasinoGameKey === "horseracing" && trigger.id === "startBtn") return;

  const text = getSoundTextFromElement(trigger);
  if (!GAME_CLICK_SOUND_PATTERN.test(text)) return;
  playGameSound("click");
}

document.addEventListener(
  "click",
  (event) => {
    if (!event.isTrusted) return;
    maybePlayCasinoBetClickSound(event.target);
  },
  true
);

function getHighBetRigChance(betAmount, intensity = 1) {
  const bet = Number(betAmount);
  if (!Number.isFinite(bet) || bet <= HIGH_BET_RIG_THRESHOLD) return 0;

  const over = bet - HIGH_BET_RIG_THRESHOLD;
  const scaled = HIGH_BET_RIG_BASE_CHANCE + over / 12000;
  const raw = Math.min(HIGH_BET_RIG_MAX_CHANCE, scaled);
  return Math.max(0, Math.min(HIGH_BET_RIG_MAX_CHANCE, raw * Math.max(0, intensity)));
}

function shouldRigHighBet(betAmount, intensity = 1) {
  return Math.random() < getHighBetRigChance(betAmount, intensity);
}

function roundCurrency(value) {
  return Math.round((Number(value) || 0) * 100) / 100;
}

function clampPercent(value) {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(100, value));
}

function formatCurrency(value) {
  return `${CURRENCY_SYMBOL}${roundCurrency(value).toFixed(2)}`;
}

function normalizeUsername(value) {
  const normalized = String(value || "").replace(/\s+/g, " ").trim();
  if (normalized.length < USERNAME_MIN_LEN || normalized.length > USERNAME_MAX_LEN) return "";
  return normalized;
}

function normalizeAuthEmail(value) {
  const normalized = String(value || "")
    .replace(/\u200B/g, "")
    .replace(/\s+/g, "")
    .replace(/^mailto:/i, "")
    .replace(/^['"`<]+|['"`>]+$/g, "")
    .trim()
    .toLowerCase();
  if (!normalized) return "";
  const atIndex = normalized.indexOf("@");
  if (atIndex <= 0 || atIndex >= normalized.length - 1) return "";
  const domain = normalized.slice(atIndex + 1);
  if (domain.startsWith(".") || domain.endsWith(".")) return "";
  if (!domain.includes(".")) return "";
  return normalized;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function formatDateTime(value) {
  const timestamp = Number(value);
  if (!Number.isFinite(timestamp) || timestamp <= 0) return "—";
  return new Date(timestamp).toLocaleString();
}

function persistUsernameSetupDone() {
  try {
    USERNAME_SETUP_DONE_FALLBACK_KEYS.forEach((key) => {
      localStorage.setItem(key, "1");
    });
  } catch {}
}

function loadSavedUsername() {
  try {
    for (const key of USERNAME_STORAGE_FALLBACK_KEYS) {
      const candidate = normalizeUsername(localStorage.getItem(key));
      if (!candidate) continue;
      localStorage.setItem(USERNAME_STORAGE_KEY, candidate);
      persistUsernameSetupDone();
      return candidate;
    }
  } catch {
    return "";
  }
  return "";
}

function saveUsername(value) {
  const normalized = normalizeUsername(value);
  if (!normalized) return false;
  try {
    USERNAME_STORAGE_FALLBACK_KEYS.forEach((key) => {
      localStorage.setItem(key, normalized);
    });
    persistUsernameSetupDone();
  } catch {}
  playerUsername = normalized;
  if (typeof savePhoneState === "function") savePhoneState();
  return true;
}

function setGuestModeEnabled(enabled) {
  try {
    if (enabled) {
      localStorage.setItem(GUEST_MODE_STORAGE_KEY, "1");
    } else {
      localStorage.removeItem(GUEST_MODE_STORAGE_KEY);
    }
  } catch {}
}

function loadFirstPlayTutorialSeenMap() {
  try {
    const raw = localStorage.getItem(FIRST_PLAY_TUTORIAL_SEEN_STORAGE_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return {};
    return parsed;
  } catch {
    return {};
  }
}

function saveFirstPlayTutorialSeenMap(mapValue) {
  try {
    localStorage.setItem(FIRST_PLAY_TUTORIAL_SEEN_STORAGE_KEY, JSON.stringify(mapValue));
  } catch {}
}

function getFirstPlayTutorialKey({ playerId = "", scope = "account" } = {}) {
  const normalizedPlayerId = String(playerId || "").trim();
  if (!normalizedPlayerId) return "";
  return `${scope === "guest" ? "guest" : "account"}:${normalizedPlayerId}`;
}

function hasSeenFirstPlayTutorial(tutorialKey) {
  if (!tutorialKey) return true;
  const seenMap = loadFirstPlayTutorialSeenMap();
  return Boolean(seenMap[tutorialKey]);
}

function markFirstPlayTutorialSeen(tutorialKey) {
  if (!tutorialKey) return;
  const seenMap = loadFirstPlayTutorialSeenMap();
  seenMap[tutorialKey] = Date.now();
  saveFirstPlayTutorialSeenMap(seenMap);
}

function generateAdminDeviceToken() {
  try {
    if (window.crypto?.getRandomValues) {
      const bytes = new Uint8Array(32);
      window.crypto.getRandomValues(bytes);
      return Array.from(bytes, (value) => value.toString(16).padStart(2, "0")).join("");
    }
  } catch {}
  return `${Date.now().toString(36)}_${Math.random().toString(36).slice(2)}_${Math.random().toString(36).slice(2)}`;
}

function loadOrCreateAdminDeviceToken() {
  try {
    const existing = String(localStorage.getItem(ADMIN_DEVICE_TOKEN_STORAGE_KEY) || "").trim();
    if (existing.length >= 24) return existing;
    const generated = generateAdminDeviceToken();
    localStorage.setItem(ADMIN_DEVICE_TOKEN_STORAGE_KEY, generated);
    return generated;
  } catch {
    return generateAdminDeviceToken();
  }
}

function loadOrCreateAdminDeviceLabel() {
  const fallback = `${navigator.platform || "device"}-${navigator.userAgent || "browser"}`
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 80);
  try {
    const existing = String(localStorage.getItem(ADMIN_DEVICE_LABEL_STORAGE_KEY) || "").replace(/\s+/g, " ").trim();
    if (existing) return existing.slice(0, 80);
    const label = fallback || "Trusted device";
    localStorage.setItem(ADMIN_DEVICE_LABEL_STORAGE_KEY, label);
    return label;
  } catch {
    return fallback || "Trusted device";
  }
}

function hasCompletedUsernameSetup() {
  const normalizedPlayer = normalizeUsername(playerUsername);
  if (normalizedPlayer) return true;
  return Boolean(loadSavedUsername());
}

function normalizeSageCurrencyText(text) {
  if (typeof text !== "string" || text.length === 0) return text;
  return text.replace(/(?<![A-Za-z])\$/g, CURRENCY_SYMBOL);
}

function normalizeSageCurrencyDom(root) {
  if (!root) return;
  if (root.nodeType === Node.TEXT_NODE) {
    const current = root.nodeValue || "";
    const next = normalizeSageCurrencyText(current);
    if (next !== current) root.nodeValue = next;
    return;
  }
  if (!(root instanceof Element) && root !== document.body) return;
  if (root instanceof Element && (root.tagName === "SCRIPT" || root.tagName === "STYLE")) return;

  const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT);
  let textNode = walker.nextNode();
  while (textNode) {
    const current = textNode.nodeValue || "";
    const next = normalizeSageCurrencyText(current);
    if (next !== current) textNode.nodeValue = next;
    textNode = walker.nextNode();
  }
}

function installSageCurrencyDomNormalizer() {
  if (!document.body || window.__sageCurrencyObserverInstalled) return;
  window.__sageCurrencyObserverInstalled = true;
  let normalizing = false;

  const runNormalize = (target) => {
    if (normalizing) return;
    normalizing = true;
    try {
      normalizeSageCurrencyDom(target || document.body);
    } finally {
      normalizing = false;
    }
  };

  runNormalize(document.body);
  const observer = new MutationObserver((mutations) => {
    if (normalizing) return;
    for (const mutation of mutations) {
      if (mutation.type === "characterData") {
        runNormalize(mutation.target);
        continue;
      }
      mutation.addedNodes.forEach((node) => runNormalize(node));
    }
  });
  observer.observe(document.body, {
    childList: true,
    subtree: true,
    characterData: true
  });
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", installSageCurrencyDomNormalizer, { once: true });
} else {
  installSageCurrencyDomNormalizer();
}

playerUsername = loadSavedUsername();

function setFirstLaunchUsernameError(message = "") {
  const errorEl = document.getElementById("firstLaunchUsernameError");
  if (!errorEl) return;
  errorEl.textContent = String(message || "");
}

function setFirstLaunchAuthBusy(isBusy) {
  firstLaunchAuthBusy = Boolean(isBusy);
  const confirmBtn = document.getElementById("firstLaunchUsernameConfirmBtn");
  const registerBtn = document.getElementById("firstLaunchModeRegisterBtn");
  const loginBtn = document.getElementById("firstLaunchModeLoginBtn");
  const guestBtn = document.getElementById("firstLaunchModeGuestBtn");
  if (confirmBtn) {
    const label = firstLaunchAuthMode === "login"
      ? "Login"
      : firstLaunchAuthMode === "guest"
          ? "Continue as Guest"
        : "Create Account";
    confirmBtn.disabled = firstLaunchAuthBusy;
    confirmBtn.textContent = firstLaunchAuthBusy ? "Please wait..." : label;
  }
  if (registerBtn) registerBtn.disabled = firstLaunchAuthBusy;
  if (loginBtn) loginBtn.disabled = firstLaunchAuthBusy;
  if (guestBtn) guestBtn.disabled = firstLaunchAuthBusy;
}

function setFirstLaunchAuthMode(mode) {
  const requestedMode = String(mode || "").toLowerCase();
  const normalizedMode = requestedMode === "login"
    ? "login"
    : requestedMode === "guest"
        ? "guest"
        : "register";
  firstLaunchAuthMode = normalizedMode;
  const registerBtn = document.getElementById("firstLaunchModeRegisterBtn");
  const loginBtn = document.getElementById("firstLaunchModeLoginBtn");
  const guestBtn = document.getElementById("firstLaunchModeGuestBtn");
  const registerFields = document.getElementById("firstLaunchRegisterFields");
  const loginFields = document.getElementById("firstLaunchLoginFields");
  const guestFields = document.getElementById("firstLaunchGuestFields");
  const subtitle = document.getElementById("firstLaunchSubtitle");
  if (registerBtn) registerBtn.classList.toggle("active", normalizedMode === "register");
  if (loginBtn) loginBtn.classList.toggle("active", normalizedMode === "login");
  if (guestBtn) guestBtn.classList.toggle("active", normalizedMode === "guest");
  if (registerFields) registerFields.classList.toggle("hidden", normalizedMode !== "register");
  if (loginFields) loginFields.classList.toggle("hidden", normalizedMode !== "login");
  if (guestFields) guestFields.classList.toggle("hidden", normalizedMode !== "guest");
  if (subtitle) {
    subtitle.textContent = normalizedMode === "login"
      ? "Login with your username and password."
      : normalizedMode === "guest"
          ? "Play as a guest on this device."
        : "Create an account to continue.";
  }
  setFirstLaunchAuthBusy(firstLaunchAuthBusy);
  setFirstLaunchUsernameError("");
}

function isBannedAccountErrorMessage(value) {
  const message = String(value || "").toLowerCase();
  return message.includes("account is banned") || message.includes("you are banned");
}

function showBannedOverlay(message = "You are banned.") {
  const overlay = document.getElementById("bannedOverlay");
  const messageEl = document.getElementById("bannedOverlayMessage");
  if (!overlay) return;
  bannedOverlayActive = true;
  authSessionLikelyAuthenticated = false;
  stopAuthSessionWatchdog();
  autoPlay = false;
  if (typeof hideFirstLaunchUsernameOverlay === "function") hideFirstLaunchUsernameOverlay();
  if (hiddenAdminPanelOpen && typeof closeHiddenAdminPanel === "function") closeHiddenAdminPanel();
  const loanPanel = document.getElementById("loan-panel");
  if (loanPanel) loanPanel.classList.remove("open");
  overlay.classList.remove("hidden");
  overlay.setAttribute("aria-hidden", "false");
  if (messageEl) messageEl.textContent = String(message || "You are banned.");
}

function hideBannedOverlay() {
  const overlay = document.getElementById("bannedOverlay");
  if (!overlay) return;
  bannedOverlayActive = false;
  overlay.classList.add("hidden");
  overlay.setAttribute("aria-hidden", "true");
}

function pushAntiTamperNotice(message) {
  const now = Date.now();
  if (now - antiTamperLastNoticeAt < 1600) return;
  antiTamperLastNoticeAt = now;
  try {
    setBankMessage(String(message || "Action blocked."));
  } catch {}
}

function ensureAntiTamperPresentation() {
  if (antiTamperPresentationReady) return;
  antiTamperPresentationReady = true;
  const styleEl = document.createElement("style");
  styleEl.id = "antiTamperStyle";
  styleEl.textContent = `
    #antiTamperOverlay {
      position: fixed;
      inset: 0;
      z-index: 999999;
      display: none;
      align-items: center;
      justify-content: center;
      text-align: center;
      padding: 24px;
      background: rgba(3, 8, 14, 0.92);
      color: #ffd896;
      font-weight: 700;
      font-size: 17px;
      line-height: 1.45;
      letter-spacing: 0.01em;
      backdrop-filter: blur(2px);
    }
    #antiTamperOverlay.active {
      display: flex;
    }
    body.anti-tamper-lock {
      overflow: hidden !important;
    }
    body.anti-tamper-lock > *:not(#antiTamperOverlay) {
      pointer-events: none !important;
      user-select: none !important;
      filter: blur(1px);
    }
  `;
  document.head.appendChild(styleEl);

  antiTamperOverlayEl = document.createElement("div");
  antiTamperOverlayEl.id = "antiTamperOverlay";
  antiTamperOverlayEl.setAttribute("aria-hidden", "true");
  antiTamperOverlayEl.textContent = "Developer tools detected. Close DevTools to continue.";
  document.body.appendChild(antiTamperOverlayEl);
}

function setAntiTamperLock(locked) {
  ensureAntiTamperPresentation();
  document.body.classList.toggle("anti-tamper-lock", Boolean(locked));
  if (!antiTamperOverlayEl) return;
  antiTamperOverlayEl.classList.toggle("active", Boolean(locked));
  antiTamperOverlayEl.setAttribute("aria-hidden", locked ? "false" : "true");
}

function isBlockedDevtoolsShortcut(event) {
  const key = String(event?.key || "").toLowerCase();
  const code = String(event?.code || "");
  const hasPrimary = event?.ctrlKey === true || event?.metaKey === true;
  if (key === "f12" || code === "F12") return true;
  if (hasPrimary && event?.shiftKey === true && ["i", "j", "c", "k"].includes(key)) return true;
  if (hasPrimary && event?.altKey === true && ["i", "j", "c", "k"].includes(key)) return true;
  if (hasPrimary && event?.shiftKey === true && ["KeyI", "KeyJ", "KeyC", "KeyK"].includes(code)) return true;
  if (hasPrimary && event?.altKey === true && ["KeyI", "KeyJ", "KeyC", "KeyK"].includes(code)) return true;
  if (hasPrimary && key === "u") return true;
  return false;
}

function detectLikelyDevtoolsOpen() {
  if (!document.hasFocus()) return false;
  const minViewport = Math.min(window.innerWidth || 0, window.innerHeight || 0);
  if (minViewport < 700) return false;
  const widthGap = Math.abs(window.outerWidth - window.innerWidth);
  const heightGap = Math.abs(window.outerHeight - window.innerHeight);
  const widthRatio = widthGap / Math.max(1, window.outerWidth || 1);
  const heightRatio = heightGap / Math.max(1, window.outerHeight || 1);
  return (widthGap > 280 && widthRatio > 0.22) || (heightGap > 220 && heightRatio > 0.2);
}

function armAntiTamperScan(durationMs = ANTI_TAMPER_ARM_MS) {
  const ms = Math.max(1000, Number(durationMs) || ANTI_TAMPER_ARM_MS);
  const until = Date.now() + ms;
  antiTamperScanArmedUntil = Math.max(antiTamperScanArmedUntil, until);
}

function initAntiTamperGuards() {
  if (IS_PHONE_EMBED_MODE || antiTamperGuardsInitialized) return;
  antiTamperGuardsInitialized = true;
  ensureAntiTamperPresentation();

  const blockShortcut = (event) => {
    if (!isBlockedDevtoolsShortcut(event)) return;
    event.preventDefault();
    event.stopPropagation();
    if (typeof event.stopImmediatePropagation === "function") event.stopImmediatePropagation();
    armAntiTamperScan(ANTI_TAMPER_ARM_MS);
    pushAntiTamperNotice("Developer shortcut blocked.");
  };
  window.addEventListener("keydown", blockShortcut, true);
  document.addEventListener("keydown", blockShortcut, true);

  const blockContextMenu = (event) => {
    event.preventDefault();
    if (typeof event.stopImmediatePropagation === "function") event.stopImmediatePropagation();
    armAntiTamperScan(15000);
    pushAntiTamperNotice("Right-click is disabled.");
  };
  window.addEventListener("contextmenu", blockContextMenu, true);
  document.addEventListener("contextmenu", blockContextMenu, true);

  let lastWidthGap = Math.abs(window.outerWidth - window.innerWidth);
  let lastHeightGap = Math.abs(window.outerHeight - window.innerHeight);
  window.addEventListener("resize", () => {
    const widthGap = Math.abs(window.outerWidth - window.innerWidth);
    const heightGap = Math.abs(window.outerHeight - window.innerHeight);
    const widthJump = Math.abs(widthGap - lastWidthGap);
    const heightJump = Math.abs(heightGap - lastHeightGap);
    if (widthJump > 80 || heightJump > 80 || detectLikelyDevtoolsOpen()) {
      armAntiTamperScan(ANTI_TAMPER_ARM_MS);
    }
    lastWidthGap = widthGap;
    lastHeightGap = heightGap;
  });
  window.addEventListener("focus", () => {
    armAntiTamperScan(7000);
  });
  armAntiTamperScan(7000);

  antiTamperDevtoolsTimer = window.setInterval(() => {
    if (Date.now() > antiTamperScanArmedUntil) {
      antiTamperDetectedStreak = 0;
      antiTamperClearStreak += 1;
      if (antiTamperClearStreak >= 2 && antiTamperDevtoolsOpen) {
        antiTamperDevtoolsOpen = false;
        setAntiTamperLock(false);
      }
      return;
    }
    if (detectLikelyDevtoolsOpen()) {
      antiTamperDetectedStreak += 1;
      antiTamperClearStreak = 0;
    } else {
      antiTamperDetectedStreak = 0;
      antiTamperClearStreak += 1;
    }
    if (antiTamperDetectedStreak >= 3 && !antiTamperDevtoolsOpen) {
      antiTamperDevtoolsOpen = true;
      if (hiddenAdminPanelOpen && typeof closeHiddenAdminPanel === "function") closeHiddenAdminPanel();
      if (venmoAdminCodeInputEl) venmoAdminCodeInputEl.value = "";
      setAntiTamperLock(true);
      pushAntiTamperNotice("Developer tools detected.");
    } else if (antiTamperClearStreak >= 3 && antiTamperDevtoolsOpen) {
      antiTamperDevtoolsOpen = false;
      setAntiTamperLock(false);
    }
  }, 1200);
}

async function pollAuthSessionWatchdog() {
  if (authSessionWatchdogBusy || !authSessionLikelyAuthenticated || bannedOverlayActive) return;
  authSessionWatchdogBusy = true;
  try {
    const response = await fetch("/api/auth/session", { credentials: "include" });
    const payload = await response.json().catch(() => ({}));
    const errorMessage = String(payload?.error || "");
    if (response.status === 403 && isBannedAccountErrorMessage(errorMessage)) {
      showBannedOverlay("You are banned.");
      return;
    }
    if (response.ok && payload?.authenticated === true) {
      authSessionLikelyAuthenticated = true;
      return;
    }
    if (response.ok && payload?.authenticated === false) {
      const wasAuthenticated = authSessionLikelyAuthenticated;
      authSessionLikelyAuthenticated = false;
      if (wasAuthenticated) {
        venmoAdminUnlocked = false;
        if (hiddenAdminPanelOpen && typeof closeHiddenAdminPanel === "function") closeHiddenAdminPanel();
        hideFirstPlayTutorialOverlay({ markSeen: false });
        setFirstLaunchAuthMode("login");
        showFirstLaunchUsernameOverlay();
        setFirstLaunchUsernameError("You were logged out by admin.");
      }
    }
  } catch {}
  finally {
    authSessionWatchdogBusy = false;
  }
}

function startAuthSessionWatchdog() {
  if (IS_PHONE_EMBED_MODE || authSessionWatchdogTimer) return;
  authSessionWatchdogTimer = window.setInterval(() => {
    void pollAuthSessionWatchdog();
  }, 1800);
  void pollAuthSessionWatchdog();
}

function stopAuthSessionWatchdog() {
  if (!authSessionWatchdogTimer) return;
  clearInterval(authSessionWatchdogTimer);
  authSessionWatchdogTimer = null;
}

function getPersistedBalanceForUser({ username = "", playerId = "" } = {}) {
  const savedCash = Number(phoneState?.cash);
  if (!Number.isFinite(savedCash) || savedCash < 0) return null;

  const expectedPlayerId = String(playerId || "").trim();
  const savedPlayerId = String(phoneState?.playerId || "").trim();
  if (expectedPlayerId && savedPlayerId && expectedPlayerId !== savedPlayerId) return null;

  const expectedUsername = normalizeUsername(username);
  const savedUsername = normalizeUsername(phoneState?.username || "");
  if (expectedUsername && savedUsername && expectedUsername !== savedUsername) return null;

  return roundCurrency(savedCash);
}

function applyServerPortfolioSnapshot(user, { useServerBalance = true, allowLocalBalanceFallback = false } = {}) {
  const nextServerBalanceUpdatedAt = Number(user?.balanceUpdatedAt);
  if (Number.isFinite(nextServerBalanceUpdatedAt) && nextServerBalanceUpdatedAt > 0) {
    lastServerBalanceUpdatedAt = Math.max(lastServerBalanceUpdatedAt, Math.floor(nextServerBalanceUpdatedAt));
  }

  const serverBalance = Number(user?.balance);
  let resolvedBalance = null;
  if (useServerBalance && Number.isFinite(serverBalance) && serverBalance >= 0) {
    resolvedBalance = roundCurrency(serverBalance);
  }
  if (allowLocalBalanceFallback) {
    const localBalance = getPersistedBalanceForUser({
      username: user?.username || "",
      playerId: user?.playerId || ""
    });
    if (Number.isFinite(localBalance) && localBalance >= 0) {
      resolvedBalance = roundCurrency(localBalance);
    }
  }
  if (Number.isFinite(resolvedBalance) && resolvedBalance >= 0) {
    cash = roundCurrency(resolvedBalance);
  }

  const nextShares = Number(user?.shares);
  if (Number.isFinite(nextShares) && nextShares >= 0) {
    shares = Math.max(0, Math.floor(nextShares));
  }
  const nextAvgCost = Number(user?.avgCost);
  if (Number.isFinite(nextAvgCost) && nextAvgCost >= 0) {
    avgCost = Math.round(nextAvgCost * 10000) / 10000;
  }
  const nextSavings = Number(user?.savingsBalance);
  if (Number.isFinite(nextSavings) && nextSavings >= 0) {
    savingsBalance = roundCurrency(nextSavings);
  }
  const nextAutoSave = Number(user?.autoSavingsPercent);
  if (Number.isFinite(nextAutoSave) && nextAutoSave >= 0) {
    autoSavingsPercent = roundCurrency(clampPercent(nextAutoSave));
  }

  phoneState.cash = roundCurrency(cash);
  phoneState.shares = Math.max(0, Math.floor(Number(shares) || 0));
  phoneState.avgCost = roundCurrency(avgCost);
  phoneState.savingsBalance = roundCurrency(savingsBalance);
  phoneState.autoSavingsPercent = roundCurrency(clampPercent(autoSavingsPercent));
  phoneState.balanceUpdatedAt = Math.max(0, Math.floor(Number(lastServerBalanceUpdatedAt) || 0));
}

function applyAuthenticatedProfile(user, { useServerBalance = true, preferLocalBalance = false } = {}) {
  const username = normalizeUsername(user?.username);
  const playerId = String(user?.playerId || "").trim();
  if (!username || !playerId) return false;
  if (!saveUsername(username)) return false;
  const previousPlayerId = String(venmoClaimPlayerId || "").trim();
  if (previousPlayerId && previousPlayerId !== playerId) {
    lastServerBalanceUpdatedAt = 0;
    phoneState.balanceUpdatedAt = 0;
  }
  setGuestModeEnabled(false);
  venmoClaimPlayerId = playerId;
  try {
    localStorage.setItem(VENMO_PLAYER_ID_STORAGE_KEY, playerId);
  } catch {}
  applyServerPortfolioSnapshot(user, {
    useServerBalance,
    allowLocalBalanceFallback: preferLocalBalance
  });
  authSessionLikelyAuthenticated = true;
  hideBannedOverlay();
  startAuthSessionWatchdog();
  hideFirstLaunchUsernameOverlay();
  showFirstPlayTutorialIfNeeded({ playerId, scope: "account" });
  setFirstLaunchUsernameError("");
  if (typeof initVenmoClaimWorkflow === "function") initVenmoClaimWorkflow();
  if (typeof updateUI === "function") updateUI();
  if (typeof syncCurrentUserProfileToServer === "function") {
    syncCurrentUserProfileToServer({ force: true });
  }
  return true;
}

async function hydrateAuthSessionIfPresent() {
  try {
    const payload = await venmoApiRequest("/api/auth/session");
    if (!payload?.authenticated || !payload?.user) {
      authSessionLikelyAuthenticated = false;
      stopAuthSessionWatchdog();
      return false;
    }
    return applyAuthenticatedProfile(payload.user, { useServerBalance: true, preferLocalBalance: false });
  } catch {
    authSessionLikelyAuthenticated = false;
    stopAuthSessionWatchdog();
    return false;
  }
}

function showFirstLaunchUsernameOverlay() {
  const overlay = document.getElementById("firstLaunchOverlay");
  const registerUsernameInput = document.getElementById("firstLaunchUsernameInput");
  const loginEmailInput = document.getElementById("firstLaunchLoginEmailInput");
  const guestUsernameInput = document.getElementById("firstLaunchGuestUsernameInput");
  if (!overlay || !registerUsernameInput) return;
  usernameGateActive = true;
  overlay.classList.remove("hidden");
  overlay.setAttribute("aria-hidden", "false");
  registerUsernameInput.value = playerUsername || "";
  if (loginEmailInput) loginEmailInput.value = playerUsername || "";
  if (guestUsernameInput) guestUsernameInput.value = playerUsername || "";
  setFirstLaunchUsernameError("");
  setFirstLaunchAuthMode(firstLaunchAuthMode || "register");
  setTimeout(() => {
    const loginEmail = document.getElementById("firstLaunchLoginEmailInput");
    const guestInput = document.getElementById("firstLaunchGuestUsernameInput");
    if (firstLaunchAuthMode === "guest" && guestInput) {
      guestInput.focus();
      return;
    }
    if (firstLaunchAuthMode === "login" && loginEmail) {
      loginEmail.focus();
      return;
    }
    registerUsernameInput.focus();
  }, 0);
  if (typeof syncHiddenAdminTriggerVisibility === "function") syncHiddenAdminTriggerVisibility();
}

function hideFirstLaunchUsernameOverlay() {
  const overlay = document.getElementById("firstLaunchOverlay");
  if (!overlay) return;
  overlay.classList.add("hidden");
  overlay.setAttribute("aria-hidden", "true");
  usernameGateActive = false;
  if (typeof syncHiddenAdminTriggerVisibility === "function") syncHiddenAdminTriggerVisibility();
}

function isTutorialTargetVisible(target) {
  if (!target) return false;
  const rect = target.getBoundingClientRect();
  if (!Number.isFinite(rect.width) || !Number.isFinite(rect.height) || rect.width <= 0 || rect.height <= 0) return false;
  const style = window.getComputedStyle(target);
  if (!style || style.visibility === "hidden" || style.display === "none" || Number(style.opacity) === 0) return false;
  return true;
}

function clearFirstPlayTutorialHighlight() {
  document.querySelectorAll(".tutorial-target-highlight").forEach((element) => {
    element.classList.remove("tutorial-target-highlight");
  });
  if (!firstPlayTutorialHighlightedTarget) return;
  firstPlayTutorialHighlightedTarget.classList.remove("tutorial-target-highlight");
  firstPlayTutorialHighlightedTarget = null;
}

function setFirstPlayTutorialHighlightTarget(target) {
  if (firstPlayTutorialHighlightedTarget === target) return;
  clearFirstPlayTutorialHighlight();
  if (!target) return;
  firstPlayTutorialHighlightedTarget = null;
}

function ensureFirstPlayTutorialDom() {
  let overlay = document.getElementById("firstPlayTutorialOverlay");
  if (overlay) return overlay;
  overlay = document.createElement("div");
  overlay.id = "firstPlayTutorialOverlay";
  overlay.className = "first-play-tutorial-overlay hidden";
  overlay.setAttribute("aria-hidden", "true");
  overlay.style.cssText = "position:fixed;inset:0;z-index:16580;display:none;background:rgba(3,8,13,.9);backdrop-filter:blur(4px);";
  overlay.innerHTML = `
    <div id="firstPlayTutorialSpotlight" class="first-play-tutorial-spotlight hidden" aria-hidden="true" style="position:fixed;left:50%;top:50%;width:150px;height:54px;border-radius:12px;border:2px solid rgba(122,221,255,.95);box-shadow:0 0 0 2px rgba(16,112,145,.55),0 0 22px rgba(72,201,255,.6),inset 0 0 22px rgba(72,201,255,.22);transform:translate(-50%,-50%);pointer-events:none;"></div>
    <div class="first-play-tutorial-card" role="dialog" aria-modal="true" aria-labelledby="firstPlayTutorialTitle" style="position:fixed;left:50%;top:50%;transform:translate(-50%,-50%);width:min(520px,calc(100vw - 30px));border-radius:14px;border:1px solid #31516a;background:linear-gradient(180deg,#13293b,#0e1f2d);box-shadow:0 18px 44px rgba(0,0,0,.56);padding:18px;text-align:left;color:#ecf8ff;">
      <div class="first-play-tutorial-step-meta" id="firstPlayTutorialStepMeta" style="display:inline-flex;min-height:22px;padding:3px 10px;border-radius:999px;border:1px solid #2d5874;background:rgba(8,25,38,.82);color:#8fbedf;font-size:.74rem;font-weight:700;letter-spacing:.04em;text-transform:uppercase;">Step 1 of ${FIRST_PLAY_TUTORIAL_STEPS.length}</div>
      <h2 id="firstPlayTutorialTitle" style="margin:10px 0 4px;color:#ecf8ff;font-size:1.25rem;">Quick Start Tutorial</h2>
      <h3 id="firstPlayTutorialStepTitle" style="margin:0 0 10px;color:#cbebff;font-size:1rem;">Welcome</h3>
      <p id="firstPlayTutorialStepBody" style="margin:0;color:#b5cee2;font-size:.94rem;line-height:1.5;min-height:82px;">This quick walkthrough shows the main controls before you start playing.</p>
      <div class="first-play-tutorial-nav" style="margin-top:16px;display:flex;gap:10px;">
        <button id="firstPlayTutorialSkipBtn" class="first-play-tutorial-secondary" type="button" style="flex:1;padding:10px;border-radius:8px;border:1px solid #35546c;background:#0c1e2c;color:#cfe6f7;font-weight:700;cursor:pointer;">Skip</button>
        <button id="firstPlayTutorialBackBtn" class="first-play-tutorial-secondary" type="button" style="flex:1;padding:10px;border-radius:8px;border:1px solid #35546c;background:#0c1e2c;color:#cfe6f7;font-weight:700;cursor:pointer;">Back</button>
        <button id="firstPlayTutorialNextBtn" type="button" style="flex:1;padding:10px;border-radius:8px;border:1px solid #38c9ff;background:linear-gradient(180deg,#0f7da5,#0a5d7d);color:#effbff;font-weight:700;cursor:pointer;">Next</button>
      </div>
    </div>
  `;
  document.body.appendChild(overlay);
  return overlay;
}

function setFirstPlayTutorialOverlayVisible(overlay, visible) {
  if (!overlay) return;
  overlay.classList.toggle("hidden", !visible);
  overlay.setAttribute("aria-hidden", visible ? "false" : "true");
  overlay.style.display = visible ? "block" : "none";
}

function positionFirstPlayTutorialUi() {
  const overlay = ensureFirstPlayTutorialDom();
  const card = overlay?.querySelector(".first-play-tutorial-card");
  const spotlight = document.getElementById("firstPlayTutorialSpotlight");
  if (!overlay || !card || !spotlight || overlay.classList.contains("hidden")) return;
  const step = FIRST_PLAY_TUTORIAL_STEPS[firstPlayTutorialStepIndex] || FIRST_PLAY_TUTORIAL_STEPS[0];
  const selector = typeof step?.selector === "string" ? step.selector : "";
  const target = selector ? document.querySelector(selector) : null;
  const canHighlight = isTutorialTargetVisible(target);
  setFirstPlayTutorialHighlightTarget(canHighlight ? target : null);

  if (canHighlight) {
    const rect = target.getBoundingClientRect();
    const padX = Math.min(14, Math.max(8, rect.width * 0.08));
    const padY = Math.min(10, Math.max(6, rect.height * 0.14));
    spotlight.classList.remove("hidden");
    spotlight.style.left = `${Math.round(rect.left - padX)}px`;
    spotlight.style.top = `${Math.round(rect.top - padY)}px`;
    spotlight.style.width = `${Math.round(rect.width + padX * 2)}px`;
    spotlight.style.height = `${Math.round(rect.height + padY * 2)}px`;
  } else {
    spotlight.classList.add("hidden");
  }

  const viewportWidth = window.innerWidth || document.documentElement.clientWidth || 1280;
  const viewportHeight = window.innerHeight || document.documentElement.clientHeight || 720;
  const margin = 12;
  const gap = 16;

  card.style.left = "50%";
  card.style.top = "50%";
  card.style.transform = "translate(-50%, -50%)";
  const cardRect = card.getBoundingClientRect();
  const cardWidth = Math.max(280, Math.min(cardRect.width || 520, viewportWidth - margin * 2));
  const cardHeight = Math.max(220, Math.min(cardRect.height || 300, viewportHeight - margin * 2));

  if (!canHighlight) return;

  const rect = target.getBoundingClientRect();
  const centerX = rect.left + rect.width / 2;
  const left = Math.max(margin, Math.min(Math.round(centerX - cardWidth / 2), viewportWidth - cardWidth - margin));

  let top = Math.round(rect.bottom + gap);
  if (top + cardHeight > viewportHeight - margin) {
    const above = Math.round(rect.top - cardHeight - gap);
    if (above >= margin) {
      top = above;
    } else {
      top = Math.max(margin, Math.min(Math.round(viewportHeight * 0.5 - cardHeight * 0.5), viewportHeight - cardHeight - margin));
    }
  }

  card.style.left = `${left}px`;
  card.style.top = `${top}px`;
  card.style.transform = "none";
}

function renderFirstPlayTutorialStep() {
  const titleEl = document.getElementById("firstPlayTutorialStepTitle");
  const bodyEl = document.getElementById("firstPlayTutorialStepBody");
  const metaEl = document.getElementById("firstPlayTutorialStepMeta");
  const skipBtn = document.getElementById("firstPlayTutorialSkipBtn");
  const backBtn = document.getElementById("firstPlayTutorialBackBtn");
  const nextBtn = document.getElementById("firstPlayTutorialNextBtn");
  if (!titleEl || !bodyEl || !metaEl || !backBtn || !nextBtn || !skipBtn) return;
  const lastIndex = FIRST_PLAY_TUTORIAL_STEPS.length - 1;
  const safeIndex = Math.max(0, Math.min(firstPlayTutorialStepIndex, lastIndex));
  firstPlayTutorialStepIndex = safeIndex;
  const step = FIRST_PLAY_TUTORIAL_STEPS[safeIndex] || FIRST_PLAY_TUTORIAL_STEPS[0];
  titleEl.textContent = step?.title || "Tutorial";
  bodyEl.textContent = step?.body || "";
  metaEl.textContent = `Step ${safeIndex + 1} of ${FIRST_PLAY_TUTORIAL_STEPS.length}`;
  skipBtn.disabled = safeIndex >= lastIndex;
  backBtn.disabled = safeIndex === 0;
  nextBtn.textContent = safeIndex >= lastIndex ? "Start Playing" : "Next";
  positionFirstPlayTutorialUi();
}

function hideFirstPlayTutorialOverlay({ markSeen = true } = {}) {
  const overlay = ensureFirstPlayTutorialDom();
  const spotlight = document.getElementById("firstPlayTutorialSpotlight");
  if (!overlay) return;
  setFirstPlayTutorialOverlayVisible(overlay, false);
  if (spotlight) {
    spotlight.classList.add("hidden");
    spotlight.style.left = "";
    spotlight.style.top = "";
    spotlight.style.width = "";
    spotlight.style.height = "";
  }
  const card = overlay.querySelector(".first-play-tutorial-card");
  if (card) {
    card.style.left = "";
    card.style.top = "";
    card.style.transform = "";
  }
  clearFirstPlayTutorialHighlight();
  if (markSeen && firstPlayTutorialCurrentKey) {
    markFirstPlayTutorialSeen(firstPlayTutorialCurrentKey);
  }
  firstPlayTutorialCurrentKey = "";
  firstPlayTutorialStepIndex = 0;
}

function showFirstPlayTutorialIfNeeded({ playerId = "", scope = "account" } = {}) {
  if (IS_PHONE_EMBED_MODE) return;
  if (typeof closePhone === "function") closePhone();
  const tradingRoot = document.getElementById("trading-section");
  const casinoRoot = document.getElementById("casino-section");
  if (tradingRoot && casinoRoot && casinoRoot.style.display !== "none") {
    casinoRoot.style.display = "none";
    tradingRoot.style.display = "block";
    if (typeof syncHiddenAdminTriggerVisibility === "function") syncHiddenAdminTriggerVisibility();
    if (typeof updateTradingUsernameBadge === "function") updateTradingUsernameBadge();
  }
  const overlay = ensureFirstPlayTutorialDom();
  const nextBtn = document.getElementById("firstPlayTutorialNextBtn");
  if (!overlay || !nextBtn) return;
  const tutorialKey = getFirstPlayTutorialKey({ playerId, scope });
  if (!tutorialKey || hasSeenFirstPlayTutorial(tutorialKey)) return;
  firstPlayTutorialCurrentKey = tutorialKey;
  firstPlayTutorialStepIndex = 0;
  renderFirstPlayTutorialStep();
  setFirstPlayTutorialOverlayVisible(overlay, true);
  setTimeout(() => {
    positionFirstPlayTutorialUi();
    nextBtn.focus();
  }, 0);
}

function initFirstPlayTutorial() {
  const overlay = ensureFirstPlayTutorialDom();
  const skipBtn = document.getElementById("firstPlayTutorialSkipBtn");
  const backBtn = document.getElementById("firstPlayTutorialBackBtn");
  const nextBtn = document.getElementById("firstPlayTutorialNextBtn");
  if (!overlay || !skipBtn || !backBtn || !nextBtn) return;
  window.addEventListener("resize", () => {
    positionFirstPlayTutorialUi();
  });
  window.addEventListener("scroll", () => {
    positionFirstPlayTutorialUi();
  }, true);
  skipBtn.addEventListener("click", () => {
    hideFirstPlayTutorialOverlay({ markSeen: true });
  });
  backBtn.addEventListener("click", () => {
    firstPlayTutorialStepIndex = Math.max(0, firstPlayTutorialStepIndex - 1);
    renderFirstPlayTutorialStep();
  });
  nextBtn.addEventListener("click", () => {
    const lastIndex = FIRST_PLAY_TUTORIAL_STEPS.length - 1;
    if (firstPlayTutorialStepIndex >= lastIndex) {
      hideFirstPlayTutorialOverlay({ markSeen: true });
      return;
    }
    firstPlayTutorialStepIndex += 1;
    renderFirstPlayTutorialStep();
  });
}

async function submitFirstLaunchUsername() {
  if (firstLaunchAuthBusy) return false;
  const registerUsernameInput = document.getElementById("firstLaunchUsernameInput");
  const registerPasswordInput = document.getElementById("firstLaunchPasswordInput");
  const loginEmailInput = document.getElementById("firstLaunchLoginEmailInput");
  const loginPasswordInput = document.getElementById("firstLaunchLoginPasswordInput");
  const guestUsernameInput = document.getElementById("firstLaunchGuestUsernameInput");
  if (!registerUsernameInput) return false;

  if (firstLaunchAuthMode === "guest") {
    const guestUsername = normalizeUsername(guestUsernameInput?.value || registerUsernameInput.value);
    if (!guestUsername) {
      setFirstLaunchUsernameError(`Guest username must be ${USERNAME_MIN_LEN}-${USERNAME_MAX_LEN} characters.`);
      if (guestUsernameInput) guestUsernameInput.focus();
      return false;
    }
    setFirstLaunchAuthBusy(true);
    try {
      if (!saveUsername(guestUsername)) {
        setFirstLaunchUsernameError("Could not save guest username.");
        return false;
      }
      setGuestModeEnabled(true);
      hideFirstLaunchUsernameOverlay();
      setFirstLaunchUsernameError("");
      if (!venmoClaimPlayerId) venmoClaimPlayerId = getVenmoClaimPlayerId();
      showFirstPlayTutorialIfNeeded({ playerId: venmoClaimPlayerId, scope: "guest" });
      if (typeof initVenmoClaimWorkflow === "function") initVenmoClaimWorkflow();
      if (typeof updateUI === "function") updateUI();
      return true;
    } finally {
      setFirstLaunchAuthBusy(false);
    }
  }

  if (firstLaunchAuthMode === "login") {
    const username = normalizeUsername(loginEmailInput?.value);
    const password = String(loginPasswordInput?.value || "");
    if (!username) {
      setFirstLaunchUsernameError(`Username must be ${USERNAME_MIN_LEN}-${USERNAME_MAX_LEN} characters.`);
      if (loginEmailInput) loginEmailInput.focus();
      return false;
    }
    if (password.length < 8) {
      setFirstLaunchUsernameError("Password must be at least 8 characters.");
      if (loginPasswordInput) loginPasswordInput.focus();
      return false;
    }
    setFirstLaunchAuthBusy(true);
    try {
      const payload = await venmoApiRequest("/api/auth/login", {
        method: "POST",
        body: { username, password }
      });
      const sessionReady = await ensureAuthSessionReady(payload?.user?.playerId || "");
      if (!sessionReady) {
        throw new Error("Login session was not saved. Open the live site and try again.");
      }
      const ok = applyAuthenticatedProfile(payload?.user, { useServerBalance: true, preferLocalBalance: false });
      if (!ok) {
        setFirstLaunchUsernameError("Login succeeded but profile data was invalid.");
        return false;
      }
      if (loginPasswordInput) loginPasswordInput.value = "";
      return true;
    } catch (error) {
      setFirstLaunchUsernameError(String(error?.message || "Could not login right now."));
      if (loginPasswordInput) loginPasswordInput.focus();
      return false;
    } finally {
      setFirstLaunchAuthBusy(false);
    }
  }

  const candidate = normalizeUsername(registerUsernameInput.value);
  const password = String(registerPasswordInput?.value || "");
  if (!candidate) {
    setFirstLaunchUsernameError(`Username must be ${USERNAME_MIN_LEN}-${USERNAME_MAX_LEN} characters.`);
    registerUsernameInput.focus();
    return false;
  }
  if (password.length < 8) {
    setFirstLaunchUsernameError("Password must be at least 8 characters.");
    if (registerPasswordInput) registerPasswordInput.focus();
    return false;
  }
  setFirstLaunchAuthBusy(true);
  try {
    if (!venmoClaimPlayerId) venmoClaimPlayerId = getVenmoClaimPlayerId();
    const payload = await venmoApiRequest("/api/auth/register", {
      method: "POST",
      body: {
        username: candidate,
        password,
        balance: roundCurrency(cash)
      }
    });
    const sessionReady = await ensureAuthSessionReady(payload?.user?.playerId || "");
    if (!sessionReady) {
      throw new Error("Account created, but login session was not saved. Open the live site and login.");
    }
    const ok = applyAuthenticatedProfile(payload?.user, { useServerBalance: true, preferLocalBalance: false });
    if (!ok) {
      setFirstLaunchUsernameError("Account created but profile data was invalid.");
      return false;
    }
    if (registerPasswordInput) registerPasswordInput.value = "";
    lastUserProfileSyncAt = Date.now();
    return true;
  } catch (error) {
    const message = String(error?.message || "");
    const lowerMessage = message.toLowerCase();
    if (lowerMessage.includes("account already exists for this player")) {
      try {
        localStorage.removeItem(VENMO_PLAYER_ID_STORAGE_KEY);
      } catch {}
      venmoClaimPlayerId = getVenmoClaimPlayerId();
      setFirstLaunchAuthMode("register");
      setFirstLaunchUsernameError("This device had an old profile ID. Click Create Account again.");
      if (registerUsernameInput) registerUsernameInput.focus();
      return false;
    }
    if (lowerMessage.includes("taken")) {
      setFirstLaunchUsernameError("That username is already taken.");
    } else if (
      lowerMessage.includes("already has an account") ||
      lowerMessage.includes("please login") ||
      lowerMessage.includes("already exists")
    ) {
      setFirstLaunchAuthMode("login");
      const hintedUsername = normalizeUsername(error?.details?.loginUsername || "");
      if (loginEmailInput) loginEmailInput.value = hintedUsername || candidate;
      setFirstLaunchUsernameError("Account exists. Switch to Login and enter your password.");
      if (loginPasswordInput) loginPasswordInput.focus();
    } else {
      setFirstLaunchUsernameError(message || "Could not create account right now.");
    }
    if (registerUsernameInput) registerUsernameInput.focus();
    return false;
  } finally {
    setFirstLaunchAuthBusy(false);
  }
}

async function clearLocalAccountOnDevice() {
  setFirstLaunchAuthBusy(true);
  try {
    try {
      await venmoApiRequest("/api/auth/logout", { method: "POST" });
    } catch {}
    try {
      USERNAME_STORAGE_FALLBACK_KEYS.forEach((key) => localStorage.removeItem(key));
      USERNAME_SETUP_DONE_FALLBACK_KEYS.forEach((key) => localStorage.removeItem(key));
      localStorage.removeItem(VENMO_PLAYER_ID_STORAGE_KEY);
      localStorage.removeItem(VENMO_LOCAL_CREDITED_STORAGE_KEY);
    } catch {}
    playerUsername = "";
    setGuestModeEnabled(false);
    venmoClaimPlayerId = "";
    venmoLocallyCreditedClaimIds.clear();
    venmoClaimState.claims = [];
    venmoClaimState.adminClaims = [];
    venmoClaimState.adminUsers = [];
    venmoClaimState.adminStats = null;
    venmoClaimState.adminHealth = null;
    venmoClaimState.adminDevices = [];
    venmoClaimState.adminFeedback = [];
    venmoClaimState.adminActivity = [];
    venmoClaimState.adminFraudFlags = [];
    venmoClaimState.adminBackups = [];
    venmoAdminUnlocked = false;
    authSessionLikelyAuthenticated = false;
    lastServerBalanceUpdatedAt = 0;
    stopAuthSessionWatchdog();
    hideBannedOverlay();
    hideFirstPlayTutorialOverlay({ markSeen: false });
    setFirstLaunchAuthMode("register");
    const registerUsernameInput = document.getElementById("firstLaunchUsernameInput");
    const registerPasswordInput = document.getElementById("firstLaunchPasswordInput");
    const loginEmailInput = document.getElementById("firstLaunchLoginEmailInput");
    const loginPasswordInput = document.getElementById("firstLaunchLoginPasswordInput");
    if (registerUsernameInput) registerUsernameInput.value = "";
    if (registerPasswordInput) registerPasswordInput.value = "";
    if (loginEmailInput) loginEmailInput.value = "";
    if (loginPasswordInput) loginPasswordInput.value = "";
    renderVenmoClaimStatus();
    renderVenmoAdminClaims();
    renderHiddenAdminStats();
    renderHiddenAdminUsers();
    renderHiddenAdminDevices();
    renderHiddenAdminFeedback();
    setHiddenAdminStatus("");
    setFirstLaunchUsernameError("Cleared. Create a new account or login with a different one.");
    if (registerUsernameInput) registerUsernameInput.focus();
  } finally {
    setFirstLaunchAuthBusy(false);
  }
}

async function initFirstLaunchUsernameSetup() {
  const overlay = document.getElementById("firstLaunchOverlay");
  const input = document.getElementById("firstLaunchUsernameInput");
  const loginEmailInput = document.getElementById("firstLaunchLoginEmailInput");
  const registerPasswordInput = document.getElementById("firstLaunchPasswordInput");
  const loginPasswordInput = document.getElementById("firstLaunchLoginPasswordInput");
  const guestUsernameInput = document.getElementById("firstLaunchGuestUsernameInput");
  const confirmBtn = document.getElementById("firstLaunchUsernameConfirmBtn");
  const resetBtn = document.getElementById("firstLaunchResetAccountBtn");
  const registerModeBtn = document.getElementById("firstLaunchModeRegisterBtn");
  const loginModeBtn = document.getElementById("firstLaunchModeLoginBtn");
  const guestModeBtn = document.getElementById("firstLaunchModeGuestBtn");
  if (!overlay || !input || !confirmBtn || !registerModeBtn || !loginModeBtn || !guestModeBtn || !resetBtn) return;

  registerModeBtn.addEventListener("click", () => setFirstLaunchAuthMode("register"));
  loginModeBtn.addEventListener("click", () => setFirstLaunchAuthMode("login"));
  guestModeBtn.addEventListener("click", () => setFirstLaunchAuthMode("guest"));

  confirmBtn.addEventListener("click", () => {
    submitFirstLaunchUsername();
  });
  resetBtn.addEventListener("click", () => {
    clearLocalAccountOnDevice();
  });
  input.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    submitFirstLaunchUsername();
  });
  [registerPasswordInput, loginEmailInput, loginPasswordInput, guestUsernameInput].forEach((field) => {
    if (!field) return;
    field.addEventListener("keydown", (event) => {
      if (event.key !== "Enter") return;
      event.preventDefault();
      submitFirstLaunchUsername();
    });
  });

  const savedUsername = loadSavedUsername();
  if (savedUsername) {
    playerUsername = savedUsername;
    input.value = savedUsername;
    if (guestUsernameInput) guestUsernameInput.value = savedUsername;
  }
  if (loginEmailInput && savedUsername) loginEmailInput.value = savedUsername;
  let defaultMode = savedUsername ? "login" : "register";
  try {
    if (localStorage.getItem(GUEST_MODE_STORAGE_KEY) === "1") {
      defaultMode = "guest";
    }
  } catch {}
  setFirstLaunchAuthMode(defaultMode);
  setFirstLaunchAuthBusy(true);
  const hasSession = await hydrateAuthSessionIfPresent();
  setFirstLaunchAuthBusy(false);
  if (hasSession) return;
  showFirstLaunchUsernameOverlay();
}

function getGameLabelByKey(gameKey) {
  const key = String(gameKey || "").toLowerCase();
  const map = {
    blackjack: "Blackjack",
    slots: "Slots",
    poker: "Poker",
    plinko: "Plinko",
    dragontower: "Dragon",
    keno: "Keno",
    dice: "Dice",
    slide: "Slide",
    crash: "Crash",
    mines: "Mines",
    crossyroad: "Crossy Road",
    roulette: "Roulette",
    diamonds: "Diamonds",
    hilo: "Hi-Lo",
    horseracing: "Horse Racing",
    lobby: "Casino"
  };
  return map[key] || "Casino";
}

function renderCasinoLiveFeed() {
  const listEl = document.getElementById("casinoLiveFeedList");
  const countEl = document.getElementById("casinoLiveFeedCount");
  if (!listEl) return;
  listEl.innerHTML = "";
  const liveDataEnabled = isCasinoLiveDataEnabled();
  if (countEl) countEl.textContent = liveDataEnabled ? String(casinoLiveFeedEntries.length) : "—";
  if (!liveDataEnabled) {
    const offline = document.createElement("li");
    offline.innerHTML = `<span>Offline (disabled)</span><span class="amt">—</span>`;
    listEl.appendChild(offline);
    return;
  }
  if (casinoLiveFeedEntries.length === 0) {
    const empty = document.createElement("li");
    empty.innerHTML = `<span>No wins yet</span><span class="amt">—</span>`;
    listEl.appendChild(empty);
    return;
  }

  const frag = document.createDocumentFragment();
  casinoLiveFeedEntries.forEach((entry) => {
    const li = document.createElement("li");
    const multText = Number.isFinite(entry.multiplier) && entry.multiplier > 0 ? ` • ${entry.multiplier.toFixed(2)}x` : "";
    li.innerHTML = `
      <span class="${entry.vip ? "vip" : ""}">${entry.name} · ${entry.game}${multText}</span>
      <span class="amt">+${formatCurrency(entry.amount)}</span>
    `;
    frag.appendChild(li);
  });
  listEl.appendChild(frag);
}

function renderCasinoLeaderboard() {
  const listEl = document.getElementById("casinoLeaderboardList");
  const countEl = document.getElementById("casinoLeaderboardCount");
  if (!listEl) return;
  listEl.innerHTML = "";
  const liveDataEnabled = isCasinoLiveDataEnabled();
  const topEntries = casinoLeaderboardEntries.slice(0, CASINO_LEADERBOARD_LIMIT);
  if (countEl) countEl.textContent = liveDataEnabled ? String(topEntries.length) : "—";
  if (!liveDataEnabled) {
    const offline = document.createElement("li");
    offline.innerHTML = `<span class="rank">—</span><span class="name">Offline (disabled)</span><span class="amt">—</span>`;
    listEl.appendChild(offline);
    return;
  }
  if (topEntries.length === 0) {
    const empty = document.createElement("li");
    empty.innerHTML = `<span class="rank">—</span><span class="name">No players yet</span><span class="amt">—</span>`;
    listEl.appendChild(empty);
    return;
  }

  const currentIdentity = getLeaderboardIdentity({
    playerId: venmoClaimPlayerId,
    username: playerUsername
  });
  const currentEntry = casinoLeaderboardCurrentUser;
  const frag = document.createDocumentFragment();
  topEntries.forEach((entry, index) => {
    const li = document.createElement("li");
    const entryIdentity = getLeaderboardIdentity(entry);
    const isYou = Boolean(currentIdentity && entryIdentity && currentIdentity === entryIdentity);
    if (isYou) li.classList.add("is-you");
    const rankNumber = Number(entry?.rank) > 0 ? Number(entry.rank) : index + 1;
    li.innerHTML = `
      <span class="rank">${formatLeaderboardRank(rankNumber)}</span>
      <span class="name">${escapeHtml(entry.username || "Player")}</span>
      <span class="amt">${formatCurrency(entry.balance)}</span>
    `;
    frag.appendChild(li);
  });

  const currentIdentityEntry = currentEntry ? getLeaderboardIdentity(currentEntry) : "";
  const isCurrentInTop = Boolean(
    currentIdentityEntry &&
      topEntries.some((entry) => getLeaderboardIdentity(entry) === currentIdentityEntry)
  );
  const shouldShowCurrentBelowTop = Boolean(currentEntry && currentEntry.rank > CASINO_LEADERBOARD_LIMIT && !isCurrentInTop);
  if (shouldShowCurrentBelowTop) {
    const divider = document.createElement("li");
    divider.className = "leaderboard-divider";
    divider.innerHTML = `<span class="rank">...</span><span class="name">Your place</span><span class="amt">...</span>`;
    frag.appendChild(divider);

    const currentRow = document.createElement("li");
    currentRow.className = "is-you is-you-outside-top";
    currentRow.innerHTML = `
      <span class="rank">${formatLeaderboardRank(Number(currentEntry.rank) || CASINO_LEADERBOARD_LIMIT + 1)}</span>
      <span class="name">${escapeHtml(currentEntry.username || "You")}</span>
      <span class="amt">${formatCurrency(currentEntry.balance)}</span>
    `;
    frag.appendChild(currentRow);
  }

  listEl.appendChild(frag);
}

function syncCasinoLiveDataNotice() {
  const notice = document.getElementById("casinoLiveDataNotice");
  if (!notice) return;
  const inLobby = activeCasinoGameKey === "lobby";
  notice.hidden = isCasinoLiveDataEnabled() || !inLobby;
}

function normalizeCasinoLiveFeedEntry(entry) {
  const amount = roundCurrency(Number(entry?.amount));
  if (!Number.isFinite(amount) || amount <= 0) return null;
  const multiplierRaw = Number(entry?.multiplier);
  return {
    name: String(entry?.username || entry?.name || "Player").trim() || "Player",
    vip: Boolean(entry?.vip),
    game: String(entry?.game || "Casino").trim() || "Casino",
    amount,
    multiplier: Number.isFinite(multiplierRaw) && multiplierRaw > 0 ? multiplierRaw : null,
    submittedAt: Number(entry?.submittedAt) || 0
  };
}

function normalizeCasinoLeaderboardEntry(entry) {
  const username = normalizeUsername(entry?.username) || "Player";
  const balance = roundCurrency(Number(entry?.balance));
  if (!Number.isFinite(balance) || balance < 0) return null;
  const rankRaw = Number(entry?.rank);
  return {
    playerId: String(entry?.playerId || ""),
    username,
    balance,
    lastSeenAt: Number(entry?.lastSeenAt) || 0,
    rank: Number.isFinite(rankRaw) && rankRaw > 0 ? Math.floor(rankRaw) : 0
  };
}

function setCasinoLiveFeedEntries(entries) {
  casinoLiveFeedEntries.length = 0;
  entries.slice(0, CASINO_LIVE_FEED_LIMIT).forEach((entry) => casinoLiveFeedEntries.push(entry));
  renderCasinoLiveFeed();
}

function setCasinoLeaderboardEntries(entries, currentUserEntry = undefined) {
  casinoLeaderboardEntries.length = 0;
  entries.slice(0, CASINO_LEADERBOARD_LIMIT).forEach((entry) => casinoLeaderboardEntries.push(entry));
  if (currentUserEntry !== undefined) {
    casinoLeaderboardCurrentUser = currentUserEntry;
  }
  renderCasinoLeaderboard();
}

function getLeaderboardIdentity(entry) {
  const playerId = String(entry?.playerId || "").trim();
  if (playerId) return `player:${playerId}`;
  const usernameKey = normalizeUsername(entry?.username).toLowerCase();
  if (usernameKey) return `user:${usernameKey}`;
  return "";
}

function isCasinoLiveDataEnabled() {
  return typeof navigator === "undefined" ? true : navigator.onLine !== false;
}

function formatLeaderboardRank(rank) {
  const value = Math.max(1, Math.floor(Number(rank) || 1));
  const mod100 = value % 100;
  if (mod100 >= 11 && mod100 <= 13) return `${value}th`;
  const mod10 = value % 10;
  if (mod10 === 1) return `${value}st`;
  if (mod10 === 2) return `${value}nd`;
  if (mod10 === 3) return `${value}rd`;
  return `${value}th`;
}

async function refreshCasinoLiveFeedFromServer() {
  if (casinoLiveFeedRequestInFlight || IS_PHONE_EMBED_MODE) return false;
  if (!isCasinoLiveDataEnabled()) {
    renderCasinoLiveFeed();
    return false;
  }
  casinoLiveFeedRequestInFlight = true;
  try {
    const payload = await venmoApiRequest(`/api/live-wins?limit=${CASINO_LIVE_FEED_LIMIT}`);
    const wins = Array.isArray(payload?.wins) ? payload.wins : [];
    const entries = wins
      .map(normalizeCasinoLiveFeedEntry)
      .filter(Boolean)
      .sort((a, b) => (b.submittedAt || 0) - (a.submittedAt || 0));
    setCasinoLiveFeedEntries(entries);
    return true;
  } catch {
    return false;
  } finally {
    casinoLiveFeedRequestInFlight = false;
  }
}

async function refreshCasinoLeaderboardFromServer() {
  if (casinoLeaderboardRequestInFlight || IS_PHONE_EMBED_MODE) return false;
  if (!isCasinoLiveDataEnabled()) {
    renderCasinoLeaderboard();
    return false;
  }
  casinoLeaderboardRequestInFlight = true;
  try {
    const query = new URLSearchParams({ limit: String(CASINO_LEADERBOARD_LIMIT) });
    const playerId = String(venmoClaimPlayerId || getVenmoClaimPlayerId() || "").trim();
    const username = String(playerUsername || "").trim();
    if (playerId) query.set("playerId", playerId);
    if (username) query.set("username", username);
    const payload = await venmoApiRequest(`/api/leaderboard?${query.toString()}`);
    applyCasinoLeaderboardPayload(payload);
    return true;
  } catch {
    return false;
  } finally {
    casinoLeaderboardRequestInFlight = false;
  }
}

function applyCasinoLeaderboardPayload(payload) {
  const players = Array.isArray(payload?.players) ? payload.players : [];
  const entries = players
    .map(normalizeCasinoLeaderboardEntry)
    .filter(Boolean)
    .sort((a, b) => (b.balance || 0) - (a.balance || 0) || (b.lastSeenAt || 0) - (a.lastSeenAt || 0));
  const hasYouEntry = Object.prototype.hasOwnProperty.call(payload || {}, "you");
  const currentUserEntry = hasYouEntry ? normalizeCasinoLeaderboardEntry(payload?.you) : undefined;
  setCasinoLeaderboardEntries(entries, currentUserEntry);
}

function stopCasinoLeaderboardStream() {
  if (casinoLeaderboardStreamReconnectTimer) {
    clearTimeout(casinoLeaderboardStreamReconnectTimer);
    casinoLeaderboardStreamReconnectTimer = null;
  }
  if (!casinoLeaderboardStream) return;
  try {
    casinoLeaderboardStream.close();
  } catch {}
  casinoLeaderboardStream = null;
}

function scheduleCasinoLeaderboardStreamReconnect() {
  if (casinoLeaderboardStreamReconnectTimer || IS_PHONE_EMBED_MODE) return;
  if (!isCasinoLiveDataEnabled()) return;
  casinoLeaderboardStreamReconnectTimer = setTimeout(() => {
    casinoLeaderboardStreamReconnectTimer = null;
    startCasinoLeaderboardStream();
  }, 1200);
}

function startCasinoLeaderboardStream() {
  if (IS_PHONE_EMBED_MODE) return;
  if (!isCasinoLiveDataEnabled()) return;
  if (typeof window.EventSource !== "function") return;
  if (casinoLeaderboardStream) return;
  try {
    const stream = new EventSource("/api/leaderboard/stream", { withCredentials: true });
    casinoLeaderboardStream = stream;
    stream.addEventListener("leaderboard", (event) => {
      try {
        const payload = JSON.parse(String(event.data || "{}"));
        applyCasinoLeaderboardPayload(payload);
      } catch {}
    });
    stream.onmessage = (event) => {
      try {
        const payload = JSON.parse(String(event.data || "{}"));
        applyCasinoLeaderboardPayload(payload);
      } catch {}
    };
    stream.onerror = () => {
      stopCasinoLeaderboardStream();
      scheduleCasinoLeaderboardStreamReconnect();
    };
  } catch {
    scheduleCasinoLeaderboardStreamReconnect();
  }
}

function pushCasinoLiveWin(amount, multiplier = null, gameKey = activeCasinoGameKey) {
  if (!isCasinoLiveDataEnabled()) return;
  const value = roundCurrency(Number(amount));
  if (!Number.isFinite(value) || value <= 0) return;
  const displayName = playerUsername || "Player";
  const entry = normalizeCasinoLiveFeedEntry({
    name: displayName,
    username: displayName,
    vip: Boolean(phoneState?.vip?.active),
    game: getGameLabelByKey(gameKey),
    amount: value,
    multiplier: Number(multiplier),
    submittedAt: Date.now()
  });
  if (!entry) return;
  casinoLiveFeedEntries.unshift(entry);
  if (casinoLiveFeedEntries.length > CASINO_LIVE_FEED_LIMIT) casinoLiveFeedEntries.length = CASINO_LIVE_FEED_LIMIT;
  renderCasinoLiveFeed();
  const playerId = venmoClaimPlayerId || getVenmoClaimPlayerId();
  void venmoApiRequest("/api/live-wins", {
    method: "POST",
    body: {
      playerId,
      username: displayName,
      vip: Boolean(phoneState?.vip?.active),
      game: getGameLabelByKey(gameKey),
      amount: value,
      multiplier: Number.isFinite(Number(multiplier)) ? Number(multiplier) : null
    }
  })
    .then(() => refreshCasinoLiveFeedFromServer())
    .catch(() => {});
}

function startCasinoLiveFeedTicker() {
  if (casinoLiveFeedTimer || IS_PHONE_EMBED_MODE) return;
  syncCasinoLiveDataNotice();
  renderCasinoLiveFeed();
  renderCasinoLeaderboard();
  if (isCasinoLiveDataEnabled()) {
    startCasinoLeaderboardStream();
    void Promise.all([refreshCasinoLiveFeedFromServer(), refreshCasinoLeaderboardFromServer()]);
  }
  casinoLiveFeedTimer = window.setInterval(() => {
    const casinoRoot = document.getElementById("casino-section");
    if (!casinoRoot || casinoRoot.style.display === "none") return;
    syncCasinoLiveDataNotice();
    if (!isCasinoLiveDataEnabled()) return;
    if (!casinoLeaderboardStream) startCasinoLeaderboardStream();
    void refreshCasinoLiveFeedFromServer();
    if (!casinoLeaderboardStream) void refreshCasinoLeaderboardFromServer();
  }, 4200);

  if (!casinoLeaderboardTimer) {
    casinoLeaderboardTimer = window.setInterval(() => {
      const casinoRoot = document.getElementById("casino-section");
      if (!casinoRoot || casinoRoot.style.display === "none") return;
      if (activeCasinoGameKey !== "lobby") return;
      if (!isCasinoLiveDataEnabled()) return;
      void refreshCasinoLeaderboardFromServer();
    }, CASINO_LEADERBOARD_POLL_MS);
  }
}

function handleCasinoLiveDataConnectionChange() {
  syncCasinoLiveDataNotice();
  renderCasinoLiveFeed();
  renderCasinoLeaderboard();
  if (!isCasinoLiveDataEnabled()) {
    stopCasinoLeaderboardStream();
    return;
  }
  startCasinoLeaderboardStream();
  void Promise.all([refreshCasinoLiveFeedFromServer(), refreshCasinoLeaderboardFromServer()]);
}

let pokerLastSessionProfit = 0;

function formatSignedProfit(value) {
  const amount = Math.abs(roundCurrency(value));
  const sign = value >= 0 ? "+" : "-";
  return `${sign}${CURRENCY_SYMBOL}${amount.toFixed(2)}`;
}

function refreshPokerSessionMeta() {
  const metaEl = document.getElementById("pokerSessionMeta");
  if (!metaEl) return;
  const value = roundCurrency(pokerLastSessionProfit);
  metaEl.textContent = `Profit: ${formatSignedProfit(value)}`;
  metaEl.classList.toggle("is-up", value >= 0);
  metaEl.classList.toggle("is-down", value < 0);
}

function syncCasinoLiveFeedVisibility() {
  const liveFeed = document.getElementById("casinoLiveFeed");
  const leaderboard = document.getElementById("casinoLeaderboard");
  const notice = document.getElementById("casinoLiveDataNotice");
  const showInLobby = activeCasinoGameKey === "lobby";
  if (liveFeed) liveFeed.style.display = showInLobby ? "" : "none";
  if (leaderboard) leaderboard.style.display = showInLobby ? "" : "none";
  if (notice) notice.hidden = !showInLobby || isCasinoLiveDataEnabled();
}

function updateTradingUsernameBadge() {
  const badge = document.getElementById("tradingUsernameBadge");
  const textEl = document.getElementById("tradingUsernameText");
  const logoutBtn = document.getElementById("tradingLogoutBtn");
  if (!badge) return;
  const normalized = normalizeUsername(playerUsername);
  const tradingRoot = document.getElementById("trading-section");
  const tradingVisible = Boolean(tradingRoot && tradingRoot.style.display !== "none");
  const shouldShow = Boolean(normalized) && tradingVisible && !IS_PHONE_EMBED_MODE;
  badge.style.display = shouldShow ? "inline-flex" : "none";
  if (textEl) textEl.textContent = shouldShow ? normalized : "";
  if (logoutBtn) logoutBtn.disabled = tradingLogoutBusy;
}

async function logoutFromTradingBadge() {
  if (tradingLogoutBusy) return;
  tradingLogoutBusy = true;
  updateTradingUsernameBadge();
  try {
    try {
      await venmoApiRequest("/api/auth/logout", { method: "POST" });
    } catch {}
    venmoAdminUnlocked = false;
    authSessionLikelyAuthenticated = false;
    lastServerBalanceUpdatedAt = 0;
    stopAuthSessionWatchdog();
    hideBannedOverlay();
    hideFirstPlayTutorialOverlay({ markSeen: false });
    if (hiddenAdminPanelOpen) closeHiddenAdminPanel();
    setFirstLaunchAuthMode("login");
    showFirstLaunchUsernameOverlay();
    setFirstLaunchUsernameError("Logged out. Login to continue.");
  } finally {
    tradingLogoutBusy = false;
    updateTradingUsernameBadge();
  }
}

function initTradingUsernameBadge() {
  const logoutBtn = document.getElementById("tradingLogoutBtn");
  if (!logoutBtn || logoutBtn.dataset.bound === "true") return;
  logoutBtn.dataset.bound = "true";
  logoutBtn.addEventListener("click", (event) => {
    event.preventDefault();
    event.stopPropagation();
    void logoutFromTradingBadge();
  });
}

function addSageBrand(root, position = "bottom-left", extraClass = "") {
  if (!root || IS_PHONE_EMBED_MODE) return null;
  const badge = document.createElement("div");
  badge.className = `sage-brand sage-${position}${extraClass ? ` ${extraClass}` : ""}`;
  badge.textContent = "SAGE";
  badge.setAttribute("aria-hidden", "true");
  root.appendChild(badge);
  return badge;
}

function getTotalDebt() {
  if (!LOANS_ENABLED) return 0;
  return Math.max(0, roundCurrency(loanPrincipal + loanInterest));
}

function getNetWorth(marketPrice = price) {
  return roundCurrency(cash + savingsBalance + shares * clampPrice(marketPrice));
}

function getCasinoTradingTotal(marketPrice = price) {
  return getNetWorth(marketPrice);
}

function isCasinoTradingUnlocked(marketPrice = price) {
  return casinoEarlyUnlockOverride || getCasinoTradingTotal(marketPrice) >= CASINO_UNLOCK_TRADING_TOTAL;
}

function getCasinoGateState(marketPrice = price) {
  const tradingTotal = getCasinoTradingTotal(marketPrice);
  const unlockProfitTarget = Math.max(0, CASINO_UNLOCK_TRADING_TOTAL - BASE_NET_WORTH);
  const currentProfit = Math.max(0, tradingTotal - BASE_NET_WORTH);
  if (!isCasinoTradingUnlocked(marketPrice)) {
    return {
      ok: false,
      reason: "trading_total",
      message: `Parental lock: casino is locked until you make $${unlockProfitTarget.toFixed(0)}. Current: $${currentProfit.toFixed(2)} / $${unlockProfitTarget.toFixed(2)}.`
    };
  }
  if (getTotalDebt() > 0) {
    return {
      ok: false,
      reason: "loan_debt",
      message: "Casino locked until loan repaid!"
    };
  }
  return { ok: true, reason: "unlocked", message: "" };
}

const CASINO_KICKOUT_DELAY_MS = 2600;
let casinoKickoutTimer = null;
let casinoPlinkoDrainKickoutTimer = null;

function clearCasinoKickoutTimer() {
  if (!casinoKickoutTimer) return;
  clearTimeout(casinoKickoutTimer);
  casinoKickoutTimer = null;
}

function clearCasinoPlinkoDrainKickoutTimer() {
  if (!casinoPlinkoDrainKickoutTimer) return;
  clearTimeout(casinoPlinkoDrainKickoutTimer);
  casinoPlinkoDrainKickoutTimer = null;
}

function scheduleCasinoKickoutAfterPlinkoDrain(getActiveBallCount) {
  if (IS_PHONE_EMBED_MODE) return;
  const activeCountGetter =
    typeof getActiveBallCount === "function" ? getActiveBallCount : () => 0;

  clearCasinoPlinkoDrainKickoutTimer();
  const poll = () => {
    casinoPlinkoDrainKickoutTimer = null;
    const casinoRoot = document.getElementById("casino-section");
    if (!casinoRoot || casinoRoot.style.display === "none") return;

    const activeCount = Math.max(0, Number(activeCountGetter()) || 0);
    if (activeCount > 0) {
      casinoPlinkoDrainKickoutTimer = setTimeout(poll, 120);
      return;
    }
    triggerCasinoKickoutCheckAfterRound();
  };
  casinoPlinkoDrainKickoutTimer = setTimeout(poll, 120);
}

function ensureCasinoKickoutOverlay() {
  const casinoRoot = document.getElementById("casino-section");
  if (!casinoRoot) return null;

  let overlay = casinoRoot.querySelector("#casinoKickoutOverlay");
  if (overlay) return overlay;

  overlay = document.createElement("div");
  overlay.id = "casinoKickoutOverlay";
  overlay.className = "casino-kickout-overlay hidden";
  overlay.innerHTML = `
    <div class="casino-kickout-panel" role="dialog" aria-modal="true" aria-labelledby="casinoKickoutTitle">
      <h3 id="casinoKickoutTitle">Casino Access Locked</h3>
      <p id="casinoKickoutMessage">Did not hit trading quota. Please return to the trading floor.</p>
      <button id="casinoKickoutReturnBtn" type="button">Return to Trading Floor</button>
    </div>
  `;
  casinoRoot.appendChild(overlay);

  const returnBtn = overlay.querySelector("#casinoKickoutReturnBtn");
  if (returnBtn) {
    returnBtn.addEventListener("click", () => {
      clearCasinoKickoutTimer();
      hideCasinoKickoutOverlay();
      exitCasinoGameView();
      const tradingRoot = document.getElementById("trading-section");
      const casinoSectionRoot = document.getElementById("casino-section");
      if (casinoSectionRoot) casinoSectionRoot.style.display = "none";
      if (tradingRoot) tradingRoot.style.display = "block";
    });
  }

  return overlay;
}

function showCasinoKickoutOverlay(
  message = "Did not hit trading quota. Please return to the trading floor."
) {
  const overlay = ensureCasinoKickoutOverlay();
  if (!overlay) return;
  const messageEl = overlay.querySelector("#casinoKickoutMessage");
  if (messageEl) messageEl.textContent = message;
  overlay.classList.remove("hidden");
}

function hideCasinoKickoutOverlay() {
  clearCasinoKickoutTimer();
  clearCasinoPlinkoDrainKickoutTimer();
  const overlay = document.getElementById("casinoKickoutOverlay");
  if (!overlay) return;
  overlay.classList.add("hidden");
}

function enforceCasinoAccessWhileOpen(marketPrice = price) {
  if (IS_PHONE_EMBED_MODE) return;

  const casinoRoot = document.getElementById("casino-section");
  if (!casinoRoot || casinoRoot.style.display === "none") {
    hideCasinoKickoutOverlay();
    return;
  }
  const casinoContainer = document.getElementById("casino-container");
  const blackjackRoundActive =
    Boolean(casinoContainer && casinoContainer.classList.contains("blackjack-fullbleed")) &&
    (!gameOver || blackjackDealInProgress);
  if (blackjackRoundActive) return;

  const gate = getCasinoGateState(marketPrice);
  if (gate.ok || gate.reason !== "trading_total") {
    hideCasinoKickoutOverlay();
    return;
  }

  const existingOverlay = document.getElementById("casinoKickoutOverlay");
  if (existingOverlay && !existingOverlay.classList.contains("hidden")) return;
  if (casinoKickoutTimer) return;

  casinoKickoutTimer = setTimeout(() => {
    casinoKickoutTimer = null;
    const stillOpen = casinoRoot && casinoRoot.style.display !== "none";
    if (!stillOpen) return;
    const latestGate = getCasinoGateState(price);
    if (latestGate.ok || latestGate.reason !== "trading_total") return;
    showCasinoKickoutOverlay("Did not hit trading quota. Please return to the trading floor.");
  }, CASINO_KICKOUT_DELAY_MS);
}

function triggerCasinoKickoutCheckAfterRound() {
  if (IS_PHONE_EMBED_MODE) return;
  const casinoRoot = document.getElementById("casino-section");
  if (!casinoRoot || casinoRoot.style.display === "none") return;
  enforceCasinoAccessWhileOpen(price);
}

function ensureCasinoBettingAllowedNow(options = {}) {
  if (IS_PHONE_EMBED_MODE) return true;
  const casinoRoot = document.getElementById("casino-section");
  if (!casinoRoot || casinoRoot.style.display === "none") return true;

  const gate = getCasinoGateState(price);
  if (gate.ok) return true;

  if (gate.reason === "trading_total") {
    const activeBallCountGetter =
      typeof options.getActiveBallCount === "function" ? options.getActiveBallCount : null;
    const activeBallCount = activeBallCountGetter
      ? Math.max(0, Number(activeBallCountGetter()) || 0)
      : 0;
    if (activeBallCount > 0) {
      scheduleCasinoKickoutAfterPlinkoDrain(activeBallCountGetter);
      return false;
    }
    clearCasinoKickoutTimer();
    showCasinoKickoutOverlay("Did not hit trading quota. Please return to the trading floor.");
  }
  return false;
}

function loadCasinoEarlyUnlockOverride() {
  try {
    casinoEarlyUnlockOverride = localStorage.getItem(CASINO_EARLY_UNLOCK_STORAGE_KEY) === "1";
  } catch (error) {
    casinoEarlyUnlockOverride = false;
  }
}

function setCasinoEarlyUnlockOverride(enabled) {
  casinoEarlyUnlockOverride = Boolean(enabled);
  try {
    if (casinoEarlyUnlockOverride) localStorage.setItem(CASINO_EARLY_UNLOCK_STORAGE_KEY, "1");
    else localStorage.removeItem(CASINO_EARLY_UNLOCK_STORAGE_KEY);
  } catch (error) {}
}

function initCasinoSecretUnlockButton() {
  if (IS_PHONE_EMBED_MODE) return;
  if (document.getElementById("casinoSecretUnlockBtn")) return;

  const button = document.createElement("button");
  button.id = "casinoSecretUnlockBtn";
  button.type = "button";
  button.className = "casino-secret-unlock-btn";
  button.setAttribute("aria-hidden", "true");
  button.tabIndex = -1;
  button.classList.add("active", "in-achievements");

  button.addEventListener("click", (event) => {
    event.preventDefault();
    event.stopPropagation();
    if (casinoEarlyUnlockOverride) return;
    setCasinoEarlyUnlockOverride(true);
    alert("Secret unlock activated.");
    updateUI();
  });

  const targetPanel = document.getElementById("achievements-panel");
  if (targetPanel) targetPanel.appendChild(button);
  else document.body.appendChild(button);
}

loadCasinoEarlyUnlockOverride();

function getSavingsProfitAvailable(marketPrice = price) {
  const tradingAssets = cash + shares * clampPrice(marketPrice);
  const available = tradingAssets - getTotalDebt() - BASE_NET_WORTH;
  return Math.max(0, roundCurrency(available));
}

// ===== MARKET SETTINGS =====
const CANDLE_WIDTH = 6;
const CANDLES_PER_STEP = 5;
const TICKS_PER_CANDLE = 8;
const NEWS_EVENT_CHANCE = 0.006;
const MAX_VOLUME = 180;
const PRICE_FLOOR = 0.01;
const PRICE_CAP = 200;

const MARKET_REGIMES = {
  calm: {
    drift: 0.000005,
    volTarget: 0.00075,
    jumpProbability: 0.0005,
    jumpScale: 0.0015,
    minDuration: 220,
    maxDuration: 420
  },
  balanced: {
    drift: 0.000012,
    volTarget: 0.0011,
    jumpProbability: 0.001,
    jumpScale: 0.002,
    minDuration: 180,
    maxDuration: 340
  },
  bull: {
    drift: 0.000065,
    volTarget: 0.00145,
    jumpProbability: 0.0012,
    jumpScale: 0.0025,
    minDuration: 150,
    maxDuration: 280
  },
  bear: {
    drift: -0.000075,
    volTarget: 0.0017,
    jumpProbability: 0.0015,
    jumpScale: 0.0028,
    minDuration: 150,
    maxDuration: 280
  },
  panic: {
    drift: -0.00004,
    volTarget: 0.0028,
    jumpProbability: 0.0034,
    jumpScale: 0.006,
    minDuration: 70,
    maxDuration: 140
  }
};

const MARKET_TRANSITIONS = {
  calm: [
    ["calm", 0.58],
    ["balanced", 0.3],
    ["bull", 0.07],
    ["bear", 0.04],
    ["panic", 0.01]
  ],
  balanced: [
    ["balanced", 0.52],
    ["calm", 0.21],
    ["bull", 0.13],
    ["bear", 0.12],
    ["panic", 0.02]
  ],
  bull: [
    ["bull", 0.48],
    ["balanced", 0.34],
    ["bear", 0.1],
    ["calm", 0.06],
    ["panic", 0.02]
  ],
  bear: [
    ["bear", 0.46],
    ["balanced", 0.34],
    ["bull", 0.1],
    ["calm", 0.05],
    ["panic", 0.05]
  ],
  panic: [
    ["panic", 0.32],
    ["bear", 0.28],
    ["balanced", 0.28],
    ["calm", 0.07],
    ["bull", 0.05]
  ]
};

const marketModel = {
  regime: "balanced",
  regimeTicksRemaining: 260,
  volatility: MARKET_REGIMES.balanced.volTarget,
  momentum: 0,
  fairValue: price,
  newsShock: 0,
  newsDrift: 0
};

const NEWS_IMPACT_PROFILE = {
  mildPositive: {
    shock: 0.0013,
    drift: 0.00005,
    fairValueShift: 0.0045,
    gap: 0.0012
  },
  mildNegative: {
    shock: 0.0014,
    drift: 0.000055,
    fairValueShift: 0.0048,
    gap: 0.0013
  }
};

function buildNewsPool() {
  const goodMildSectors = [
    "Cloud software",
    "Semiconductor",
    "AI infrastructure",
    "Payment services",
    "Retail sales",
    "EV battery",
    "Cybersecurity",
    "Healthcare device",
    "Industrial automation",
    "Logistics network"
  ];
  const goodMildCatalysts = [
    "beats quarterly estimates",
    "raises near-term guidance",
    "wins a large enterprise contract",
    "announces stronger margin outlook",
    "gets favorable analyst upgrades"
  ];

  const badMildSectors = [
    "Cloud software",
    "Semiconductor",
    "AI infrastructure",
    "Payment services",
    "Retail sales",
    "EV battery",
    "Cybersecurity",
    "Healthcare device",
    "Industrial automation",
    "Logistics network"
  ];
  const badMildHeadwinds = [
    "misses quarterly estimates",
    "cuts near-term guidance",
    "faces a regulatory review",
    "flags softer demand outlook",
    "receives cautious analyst downgrade"
  ];

  const events = [];

  goodMildSectors.forEach((sector) => {
    goodMildCatalysts.forEach((catalyst) => {
      events.push({
        text: `📈 ${sector} ${catalyst}`,
        color: "#3bff6e",
        direction: 1,
        tier: "mild",
        ...NEWS_IMPACT_PROFILE.mildPositive
      });
    });
  });

  badMildSectors.forEach((sector) => {
    badMildHeadwinds.forEach((headwind) => {
      events.push({
        text: `📉 ${sector} ${headwind}`,
        color: "#ff5f76",
        direction: -1,
        tier: "mild",
        ...NEWS_IMPACT_PROFILE.mildNegative
      });
    });
  });

  for (let i = events.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [events[i], events[j]] = [events[j], events[i]];
  }

  return events;
}

const NEWS_EVENTS = buildNewsPool();

// Risk controls
const BASE_SLIPPAGE = 0.001;
const TRADE_FEE = 0.002;

// =====================================================
// =================== UI ELEMENTS =====================
// =====================================================
const cashEl = document.getElementById("cash");
const sharesEl = document.getElementById("shares");
const netEl = document.getElementById("net");
const plEl = document.getElementById("pl");
const sidePriceEl = document.getElementById("side-price");
const avgBuyEl = document.getElementById("avg-buy-price");
const tradingMissionLevelEl = document.getElementById("tradingMissionLevel");
const tradingMissionXpEl = document.getElementById("tradingMissionXp");
const tradingMissionXpToNextEl = document.getElementById("tradingMissionXpToNext");
const tradingMissionLevelFillEl = document.getElementById("tradingMissionLevelFill");
const tradingMissionPerkEl = document.getElementById("tradingMissionPerk");
const newsEl = document.getElementById("news");
const achievementsListEl = document.getElementById("achievements-list");

const ACHIEVEMENTS_STORAGE_KEY = "trading_achievements_v1";
const ACHIEVEMENT_CASH_REWARD_MIN = 10;
const ACHIEVEMENT_CASH_REWARD_MAX = 150;
const ACHIEVEMENT_BASE_CASH_MIN = 40;
const ACHIEVEMENT_BASE_CASH_MAX = 2000;
const achievementState = {
  unlocked: {},
  stats: {
    steps: 0,
    buys: 0,
    sells: 0,
    newsSeen: 0,
    loansTaken: 0,
    casinoWins: 0,
    peakNet: 1000,
    peakCash: 1000
  }
};

const ACHIEVEMENTS = [
  {
    id: "first-buy",
    title: "First Position",
    description: "Buy your first share.",
    check: () => achievementState.stats.buys >= 1
  },
  {
    id: "first-sell",
    title: "First Exit",
    description: "Sell at least one share.",
    check: () => achievementState.stats.sells >= 1
  },
  {
    id: "step-50",
    title: "Desk Time",
    description: "Take 50 market steps.",
    check: () => achievementState.stats.steps >= 50
  },
  {
    id: "step-250",
    title: "Market Veteran",
    description: "Take 250 market steps.",
    check: () => achievementState.stats.steps >= 250
  },
  {
    id: "news-25",
    title: "Headline Watcher",
    description: "See 25 market news alerts.",
    check: () => achievementState.stats.newsSeen >= 25
  },
  {
    id: "casino-win-10",
    title: "Casino Regular",
    description: "Win 10 casino rounds.",
    check: () => achievementState.stats.casinoWins >= 10
  },
  {
    id: "casino-win-50",
    title: "Casino Grinder",
    description: "Win 50 casino rounds.",
    check: () => achievementState.stats.casinoWins >= 50
  },
  {
    id: "casino-win",
    title: "Lucky Hit",
    description: "Win in any casino game.",
    check: () => achievementState.stats.casinoWins >= 1
  },
  {
    id: "net-2k",
    title: "Double Up",
    description: "Reach $2,000 net worth.",
    check: () => getNetWorth() >= 2000
  },
  {
    id: "net-5k",
    title: "High Roller",
    description: "Reach $5,000 net worth.",
    check: () => getNetWorth() >= 5000
  }
];
const BANK_ACHIEVEMENT_CATEGORY = "Casino Mastery";

function formatAchievementRewardBundle(reward, rewardKey = "") {
  if (!reward || typeof reward !== "object") return "";
  const parts = [];
  const cashReward = getScaledAchievementCashReward(Number(reward.cash) || 0, rewardKey);
  const xpReward = Number(reward.xp) || 0;
  const tokenReward = Number(reward.tokens) || 0;

  if (cashReward > 0) parts.push(formatCurrency(cashReward));
  if (xpReward > 0) parts.push(`${Math.floor(xpReward)} XP`);
  if (tokenReward > 0) parts.push(`${Math.floor(tokenReward)} token${Math.floor(tokenReward) === 1 ? "" : "s"}`);
  return parts.join(" • ");
}

function getScaledAchievementCashReward(amount, rewardKey = "") {
  const safeAmount = roundCurrency(Number(amount) || 0);
  if (safeAmount <= 0) return 0;
  const bankKey = String(rewardKey || "");
  if (bankKey && BANK_ACHIEVEMENT_ORDER_INDEX.has(bankKey)) {
    const rank = BANK_ACHIEVEMENT_ORDER_INDEX.get(bankKey);
    const total = Math.max(1, BANK_ACHIEVEMENT_ORDER.length - 1);
    const ratio = clampMarket(rank / total, 0, 1);
    const scaled = ACHIEVEMENT_CASH_REWARD_MIN + ratio * (ACHIEVEMENT_CASH_REWARD_MAX - ACHIEVEMENT_CASH_REWARD_MIN);
    return roundCurrency(Math.round(scaled));
  }

  const normalized = clampMarket(
    (safeAmount - ACHIEVEMENT_BASE_CASH_MIN) / Math.max(1, ACHIEVEMENT_BASE_CASH_MAX - ACHIEVEMENT_BASE_CASH_MIN),
    0,
    1
  );
  const scaled = ACHIEVEMENT_CASH_REWARD_MIN + normalized * (ACHIEVEMENT_CASH_REWARD_MAX - ACHIEVEMENT_CASH_REWARD_MIN);
  return roundCurrency(Math.round(scaled));
}

function getAllAchievementsForSidebar() {
  const base = ACHIEVEMENTS.map((achievement) => ({
    ...achievement,
    category: "Trading"
  }));
  const bankDefs = typeof BANK_MISSION_DEFS !== "undefined" && Array.isArray(BANK_MISSION_DEFS) ? BANK_MISSION_DEFS : [];
  const bankAchievements = bankDefs.map((mission) => {
    const rewardText = formatAchievementRewardBundle(mission.reward, mission.id);
    return {
      id: `bank-achievement-${mission.id}`,
      title: mission.name,
      description: rewardText ? `${mission.description} Reward: ${rewardText}` : mission.description,
      category: BANK_ACHIEVEMENT_CATEGORY,
      bankSortIndex: BANK_ACHIEVEMENT_ORDER_INDEX.has(mission.id)
        ? BANK_ACHIEVEMENT_ORDER_INDEX.get(mission.id)
        : 9999,
      check: () => (typeof isBankMissionCompleted === "function" ? isBankMissionCompleted(mission.id) : false),
      skipUnlockNotification: true
    };
  });
  return base.concat(bankAchievements);
}

function getTradingAchievementProgressMeta(achievementId) {
  const netWorth = getNetWorth();
  switch (achievementId) {
    case "first-buy":
      return { current: achievementState.stats.buys, goal: 1 };
    case "first-sell":
      return { current: achievementState.stats.sells, goal: 1 };
    case "step-50":
      return { current: achievementState.stats.steps, goal: 50 };
    case "step-250":
      return { current: achievementState.stats.steps, goal: 250 };
    case "news-25":
      return { current: achievementState.stats.newsSeen, goal: 25 };
    case "casino-win":
      return { current: achievementState.stats.casinoWins, goal: 1 };
    case "casino-win-10":
      return { current: achievementState.stats.casinoWins, goal: 10 };
    case "casino-win-50":
      return { current: achievementState.stats.casinoWins, goal: 50 };
    case "net-2k":
      return { current: netWorth, goal: 2000 };
    case "net-5k":
      return { current: netWorth, goal: 5000 };
    default:
      return { current: 0, goal: 1 };
  }
}

function getAchievementProgressMeta(achievement) {
  const unlocked = Boolean(achievementState.unlocked[achievement.id]);
  let current = 0;
  let goal = 1;

  if (achievement.category === BANK_ACHIEVEMENT_CATEGORY && achievement.id.startsWith("bank-achievement-")) {
    const missionId = achievement.id.replace("bank-achievement-", "");
    const mission = typeof BANK_MISSION_DEF_MAP !== "undefined" ? BANK_MISSION_DEF_MAP.get(missionId) : null;
    if (mission) {
      goal = Number(mission.goal) || 1;
      if (typeof getBankMissionProgress === "function") {
        current = getBankMissionProgress(missionId);
      }
    }
  } else {
    const base = getTradingAchievementProgressMeta(achievement.id);
    current = Number(base.current) || 0;
    goal = Math.max(1, Number(base.goal) || 1);
  }

  current = clampMarket(current, 0, goal);
  if (unlocked) current = goal;

  const ratio = goal > 0 ? current / goal : 0;
  return {
    unlocked,
    current,
    goal,
    ratio,
    remaining: Math.max(0, goal - current),
    singleAction: goal <= 1
  };
}

function saveAchievements() {
  try {
    localStorage.setItem(ACHIEVEMENTS_STORAGE_KEY, JSON.stringify(achievementState));
  } catch (error) {}
}

function loadAchievements() {
  try {
    const raw = localStorage.getItem(ACHIEVEMENTS_STORAGE_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object") {
      if (parsed.unlocked && typeof parsed.unlocked === "object") {
        achievementState.unlocked = parsed.unlocked;
      }
      if (parsed.stats && typeof parsed.stats === "object") {
        achievementState.stats = {
          ...achievementState.stats,
          ...parsed.stats
        };
      }
    }
  } catch (error) {}
}

function renderAchievements() {
  if (!achievementsListEl) return;
  achievementsListEl.innerHTML = "";

  const fragment = document.createDocumentFragment();
  const allAchievements = getAllAchievementsForSidebar();
  const categories = ["Trading", BANK_ACHIEVEMENT_CATEGORY];

  categories.forEach((category) => {
    const entries = allAchievements
      .filter((achievement) => achievement.category === category)
      .sort((a, b) => {
        const unlockedA = Boolean(achievementState.unlocked[a.id]);
        const unlockedB = Boolean(achievementState.unlocked[b.id]);
        if (unlockedA !== unlockedB) return unlockedA ? 1 : -1;

        if (category === BANK_ACHIEVEMENT_CATEGORY) {
          const metaA = getAchievementProgressMeta(a);
          const metaB = getAchievementProgressMeta(b);
          if (Math.abs(metaA.ratio - metaB.ratio) > 0.0001) return metaB.ratio - metaA.ratio;

          const missionIdA = a.id.replace("bank-achievement-", "");
          const missionIdB = b.id.replace("bank-achievement-", "");
          const missionA = BANK_MISSION_DEF_MAP.get(missionIdA);
          const missionB = BANK_MISSION_DEF_MAP.get(missionIdB);
          const easyRankA =
            missionA && Number(missionA.goal) === 1 && BANK_CASINO_GAME_EASY_ORDER_INDEX.has(String(missionA.casinoGameKey || ""))
              ? BANK_CASINO_GAME_EASY_ORDER_INDEX.get(String(missionA.casinoGameKey || ""))
              : Number.POSITIVE_INFINITY;
          const easyRankB =
            missionB && Number(missionB.goal) === 1 && BANK_CASINO_GAME_EASY_ORDER_INDEX.has(String(missionB.casinoGameKey || ""))
              ? BANK_CASINO_GAME_EASY_ORDER_INDEX.get(String(missionB.casinoGameKey || ""))
              : Number.POSITIVE_INFINITY;
          if (easyRankA !== easyRankB) return easyRankA - easyRankB;

          const goalA = Number(missionA?.goal) || 1;
          const goalB = Number(missionB?.goal) || 1;
          if (goalA !== goalB) return goalA - goalB;
          return a.title.localeCompare(b.title);
        }

        const metaA = getAchievementProgressMeta(a);
        const metaB = getAchievementProgressMeta(b);
        if (Math.abs(metaA.ratio - metaB.ratio) > 0.0001) return metaB.ratio - metaA.ratio;
        if (Math.abs(metaA.remaining - metaB.remaining) > 0.0001) return metaA.remaining - metaB.remaining;
        if (metaA.singleAction !== metaB.singleAction) return metaA.singleAction ? 1 : -1;
        if (metaA.goal !== metaB.goal) return metaA.goal - metaB.goal;
        return a.title.localeCompare(b.title);
      });
    if (entries.length === 0) return;

    const unlockedCount = entries.reduce((count, achievement) => {
      return count + (achievementState.unlocked[achievement.id] ? 1 : 0);
    }, 0);

    const section = document.createElement("li");
    section.className = "achievement-section-title";
    section.innerHTML = `
      <span>${category}</span>
      <span>${unlockedCount}/${entries.length} unlocked</span>
    `;
    fragment.appendChild(section);

    entries.forEach((achievement) => {
      const progressMeta = getAchievementProgressMeta(achievement);
      const unlocked = progressMeta.unlocked;
      const item = document.createElement("li");
      item.className = `achievement-item${unlocked ? " unlocked" : ""}`;
      item.innerHTML = `
        <div class="achievement-title">${unlocked ? "✅ " : "🔒 "}${achievement.title}</div>
        <div class="achievement-desc">${achievement.description}</div>
        <div class="achievement-progress">${Math.floor(progressMeta.current)} / ${Math.floor(progressMeta.goal)}</div>
      `;
      fragment.appendChild(item);
    });
  });

  achievementsListEl.appendChild(fragment);
}

function updateAchievements(currentNet) {
  achievementState.stats.peakCash = Math.max(achievementState.stats.peakCash, cash);
  achievementState.stats.peakNet = Math.max(achievementState.stats.peakNet, currentNet);

  let unlockedNew = false;
  getAllAchievementsForSidebar().forEach((achievement) => {
    if (achievementState.unlocked[achievement.id]) return;
    if (achievement.check()) {
      achievementState.unlocked[achievement.id] = Date.now();
      if (!achievement.skipUnlockNotification) {
        pushPhoneNotification("achievement", `Achievement unlocked: ${achievement.title}`);
      }
      unlockedNew = true;
    }
  });

  if (unlockedNew) saveAchievements();
  renderAchievements();
}

loadAchievements();

const PHONE_STORAGE_KEY = "casino_phone_v1";
const BANK_MISSION_STORAGE_KEY = "casino_bank_missions_v2";
const phoneState = {
  cash: roundCurrency(cash),
  shares: Math.max(0, Math.floor(Number(shares) || 0)),
  avgCost: roundCurrency(avgCost),
  savingsBalance: roundCurrency(savingsBalance),
  balanceUpdatedAt: 0,
  playerId: "",
  username: playerUsername || "",
  unread: 0,
  notifications: [],
  bankHistory: [],
  casinoProfit: 0,
  autoSavingsPercent: 0,
  settings: {
    animations: true,
    popupSeconds: 2.2,
    autoRoundLimit: 0,
    uiScale: 100
  },
  missionsClaimed: {},
  missionXp: 0,
  missionTokens: 0,
  missionLevelMilestonesClaimed: {},
  vip: {
    active: false,
    purchasedAt: 0,
    lastWeeklyBonusAt: 0
  },
  autoRoundCounters: {
    slots: 0,
    roulette: 0
  }
};
let phoneStateLoaded = false;
const TOTAL_RESET_STORAGE_KEYS = [
  ACHIEVEMENTS_STORAGE_KEY,
  PHONE_STORAGE_KEY,
  BANK_MISSION_STORAGE_KEY,
  "dragon_tower_high",
  "horse_race_history_v1",
  "horse_race_balance_v1"
];
const TOTAL_RESET_STORAGE_PREFIXES = ["casino_", "trading_", "horse_race_", "dragon_tower_"];

const BANK_CASINO_GAME_DEFS = [
  { key: "blackjack", label: "Blackjack" },
  { key: "slots", label: "Slots" },
  { key: "dragontower", label: "Dragon Tower" },
  { key: "horseracing", label: "Horse Racing" },
  { key: "dice", label: "Dice" },
  { key: "slide", label: "Slide" },
  { key: "crash", label: "Crash" },
  { key: "mines", label: "Mines" },
  { key: "crossyroad", label: "Crossy" },
  { key: "roulette", label: "Roulette" },
  { key: "diamonds", label: "Diamonds" },
  { key: "plinko", label: "Plinko" },
  { key: "hilo", label: "Hi-Lo" },
  { key: "keno", label: "Keno" }
];
const BANK_CASINO_GAME_EASY_ORDER = [
  "slots",
  "roulette",
  "blackjack",
  "dice",
  "plinko",
  "keno",
  "hilo",
  "dragontower",
  "horseracing",
  "crash",
  "slide",
  "diamonds",
  "mines",
  "crossyroad"
];
const BANK_CASINO_GAME_EASY_ORDER_INDEX = new Map(
  BANK_CASINO_GAME_EASY_ORDER.map((key, index) => [key, index])
);
const BANK_CASINO_TIER_DEFS = [
  { suffix: "Starter", wins: 1, reward: { cash: 50, xp: 25 } },
  { suffix: "Regular", wins: 5, reward: { cash: 140, xp: 60 } },
  { suffix: "Grinder", wins: 15, reward: { cash: 320, xp: 130 } },
  { suffix: "Streak", wins: 35, reward: { cash: 680, xp: 260 } },
  { suffix: "Legend", wins: 75, reward: { cash: 1300, xp: 480 } }
];
const BANK_MISSION_GAME_TIERS = new Map();
const BANK_MISSION_DEFS = [];

BANK_CASINO_GAME_DEFS.forEach((gameDef) => {
  const tierMissionIds = [];
  BANK_CASINO_TIER_DEFS.forEach((tierDef, tierIndex) => {
    const missionId = `bank-casino-${gameDef.key}-t${tierIndex + 1}`;
    tierMissionIds.push(missionId);
    BANK_MISSION_DEFS.push({
      id: missionId,
      name: `${gameDef.label} ${tierDef.suffix}`,
      description: `Win ${tierDef.wins} ${gameDef.label} rounds.`,
      goal: tierDef.wins,
      reward: { ...tierDef.reward },
      casinoGameKey: gameDef.key
    });
  });
  BANK_MISSION_GAME_TIERS.set(gameDef.key, tierMissionIds);
});

const BANK_MISSION_BANKER_ID = "bank-casino-mission-banker";
const BANK_MISSION_LEGEND_ID = "bank-casino-legend";
const BANK_MISSION_CORE_COUNT = BANK_MISSION_DEFS.length;

BANK_MISSION_DEFS.push({
  id: BANK_MISSION_BANKER_ID,
  name: "Mission Banker",
  description: `Claim 20 casino achievement rewards in Missions.`,
  goal: 20,
  reward: { cash: 1200, xp: 500, tokens: 2 }
});
BANK_MISSION_DEFS.push({
  id: BANK_MISSION_LEGEND_ID,
  name: "Casino Legend",
  description: "Claim all casino game achievement rewards.",
  goal: BANK_MISSION_CORE_COUNT + 1,
  reward: { cash: 4000, xp: 1800, tokens: 6 }
});

const BANK_MISSION_DEF_MAP = new Map(BANK_MISSION_DEFS.map((mission) => [mission.id, mission]));
const BANK_ACHIEVEMENT_ORDER = BANK_MISSION_DEFS.map((mission) => mission.id);
const BANK_ACHIEVEMENT_ORDER_INDEX = new Map(BANK_ACHIEVEMENT_ORDER.map((id, index) => [id, index]));
const MISSION_MAX_LEVEL = 50;
const MISSION_LEVEL_MILESTONES = {
  5: { cash: 500, tokens: 1, label: "Bronze Banker" },
  10: { cash: 1500, tokens: 2, label: "Silver Banker" },
  20: { cash: 5000, tokens: 5, label: "Gold Banker" },
  30: { cash: 10000, tokens: 10, label: "Platinum Banker" }
};

function createDefaultBankMissionState() {
  return {
    progress: {},
    completed: {},
    claimed: {},
    counters: {
      loansTakenLifetime: 0,
      blockedLoanAttempts: 0,
      debtStepsTotal: 0,
      debtFreeStepStreak: 0,
      debtClears: 0,
      backToDebtArmed: false,
      interestPaidTotal: 0,
      noLoanStepStreak: 0,
      bankAppOpens: 0,
      bankActionIndex: 0,
      lastBankAction: "",
      lastLoanActionIndex: null,
      sessionLoanStep: null,
      sessionDebtSteps: 0,
      sessionTradeClosed: false,
      sessionTradingProfit: 0,
      sessionCasinoWin: false,
      sessionBrokeBorrowing: false,
      sessionHighCashStreak: 0,
      sessionNoCasinoBetStreak: 0,
      sessionNearCap: false,
      sessionEmergencyFund: false,
      sessionActive: false,
      pendingCasinoLoss: false,
      casinoBetSinceLastStep: false,
      casinoWinsByGame: Object.fromEntries(BANK_CASINO_GAME_DEFS.map((gameDef) => [gameDef.key, 0]))
    }
  };
}

const bankMissionState = createDefaultBankMissionState();
let phoneMissionToastTimer = null;
let phoneMissionCurrentTab = "daily";
let bankMissionLastCashSnapshot = cash;

const phoneUi = {};
let phoneStatusTimer = null;
let phoneHomeAnimTimer = null;
let phoneHomeBarAnimTimer = null;
let phoneAppOpenTimer = null;
let phoneHomeAnimFailSafeTimer = null;
let phoneOverlayAnimTimer = null;
const phoneLastLaunchIconByApp = {};
const phoneDockPinnedApps = new Set(["messages", "bank", "portfolio", "casino"]);
const PHONE_FEEDBACK_MESSAGE_MIN_LEN = 6;
const PHONE_FEEDBACK_MESSAGE_MAX_LEN = 500;
let phoneFeedbackEntries = [];
let phoneFeedbackSubmitBusy = false;
let phoneMiniAppsBound = false;
let phoneMiniFrameMessageBound = false;
let phoneMiniCashBridgeBound = false;
let phoneMiniCashReady = !IS_PHONE_EMBED_MODE;
let phoneMiniCashRequestTimer = null;
let cashPersistTimer = null;
let lastPersistedProfileSig = "";
let persistenceFlushHandlersBound = false;

function getPersistProfileSnapshot() {
  return {
    cash: roundCurrency(cash),
    shares: Math.max(0, Math.floor(Number(shares) || 0)),
    avgCost: roundCurrency(avgCost),
    savingsBalance: roundCurrency(savingsBalance),
    autoSavingsPercent: roundCurrency(clampPercent(autoSavingsPercent)),
    playerId: String(venmoClaimPlayerId || "").trim(),
    username: normalizeUsername(playerUsername) || ""
  };
}

function getPersistProfileSignature(profile) {
  const safe = profile || getPersistProfileSnapshot();
  return [
    safe.cash,
    safe.shares,
    safe.avgCost,
    safe.savingsBalance,
    safe.autoSavingsPercent,
    safe.playerId || "",
    safe.username
  ].join("|");
}
const PHONE_MINI_GAME_APPS = [
  { app: "blackjack", label: "Blackjack", short: "BJ", game: "blackjack", gradient: "linear-gradient(155deg,#202a35,#121922 68%)" },
  { app: "slots", label: "Slots", short: "SL", game: "slots", gradient: "linear-gradient(155deg,#7a4dff,#3f8fff 68%)" },
  { app: "dragontower", label: "Dragon", short: "DT", game: "dragontower", gradient: "linear-gradient(155deg,#ff9b3b,#dd4f4f 68%)" },
  { app: "horseracing", label: "Horse Racing", short: "HR", game: "horseracing", gradient: "linear-gradient(155deg,#2782da,#2f44c9 68%)" },
  { app: "dice", label: "Dice", short: "DC", game: "dice", gradient: "linear-gradient(155deg,#8a44ff,#5f33d8 68%)" },
  { app: "slide", label: "Slide", short: "SD", game: "slide", gradient: "linear-gradient(155deg,#57a9ff,#3f6fdf 68%)" },
  { app: "crash", label: "Crash", short: "CR", game: "crash", gradient: "linear-gradient(155deg,#f7c732,#2f78ff 68%)" },
  { app: "mines", label: "Mines", short: "MN", game: "mines", gradient: "linear-gradient(155deg,#1388ff,#07b4ff 68%)" },
  { app: "crossyroad", label: "Crossy", short: "CY", game: "crossyroad", gradient: "linear-gradient(155deg,#18c45d,#169aa8 68%)" },
  { app: "roulette", label: "Roulette", short: "RT", game: "roulette", gradient: "linear-gradient(155deg,#f06f59,#a43e2f 68%)" },
  { app: "diamonds", label: "Diamonds", short: "DM", game: "diamonds", gradient: "linear-gradient(155deg,#24c7c8,#0d87a3 68%)" },
  { app: "plinko", label: "Plinko", short: "PL", game: "plinko", gradient: "linear-gradient(155deg,#b84bf4,#ff4f8e 68%)" },
  { app: "hilo", label: "Hi-Lo", short: "HL", game: "hilo", gradient: "linear-gradient(155deg,#00b057,#1f7ce7 68%)" },
  { app: "keno", label: "Keno", short: "KN", game: "keno", gradient: "linear-gradient(155deg,#2272ff,#15c2ff 68%)" }
];
const PHONE_MINI_GAME_MAP = Object.fromEntries(PHONE_MINI_GAME_APPS.map((item) => [item.app, item]));
const PHONE_MINI_GAME_ICON_FALLBACK = {
  roulette: `
    <svg viewBox="0 0 100 100">
      <circle cx="50" cy="50" r="30" class="stroke"></circle>
      <circle cx="50" cy="50" r="10" class="stroke"></circle>
      <path d="M50 20v60M20 50h60M29 29l42 42M71 29L29 71" class="stroke"></path>
    </svg>
  `
};

function getCasinoGameIconMarkup(gameKey) {
  const node = document.querySelector(`#casino-games .casino-game-btn[data-game="${gameKey}"] .game-icon`);
  if (node) return node.innerHTML;
  return PHONE_MINI_GAME_ICON_FALLBACK[gameKey] || "";
}

function phonePanelId(appName) {
  const key = String(appName || "");
  return `phoneApp${key.charAt(0).toUpperCase()}${key.slice(1)}`;
}

function buildPhoneMiniGameApps() {
  if (IS_PHONE_EMBED_MODE) return;
  const body = phoneUi.body || document.querySelector("#phoneOverlay .phone-body");
  if (!body) return;
  const homeGrid = body.querySelector("#phoneAppHome .phone-app-grid");
  if (!homeGrid) return;
  const allowedApps = new Set(PHONE_MINI_GAME_APPS.map((item) => item.app));

  homeGrid.querySelectorAll(".phone-home-mini-game-app[data-target-app]").forEach((button) => {
    const appName = button.dataset.targetApp;
    if (!allowedApps.has(appName)) {
      button.remove();
    }
  });

  body.querySelectorAll(".phone-mini-game-app[data-mini-app]").forEach((panel) => {
    const appName = panel.dataset.miniApp;
    if (!allowedApps.has(appName)) {
      panel.remove();
    }
  });

  PHONE_MINI_GAME_APPS.forEach((config) => {
    const iconMarkup = getCasinoGameIconMarkup(config.game);
    const iconInner = iconMarkup
      ? iconMarkup
      : `<span class="phone-mini-game-glyph">${config.short}</span>`;

    let button = homeGrid.querySelector(`.phone-home-app[data-target-app="${config.app}"]`);
    if (!button) {
      button = document.createElement("button");
      button.className = "phone-home-app phone-home-mini-game-app";
      button.type = "button";
      button.setAttribute("data-target-app", config.app);
      homeGrid.appendChild(button);
    }
    button.innerHTML = `
      <span class="phone-home-icon phone-mini-game-icon" data-game="${config.game}" aria-hidden="true">
        ${iconInner}
      </span>
      <span class="phone-home-app-name">${config.label}</span>
    `;

    const panelId = phonePanelId(config.app);
    if (body.querySelector(`#${panelId}`)) return;
    const section = document.createElement("section");
    section.id = panelId;
    section.className = "phone-app phone-mini-game-app";
    section.dataset.miniApp = config.app;
    section.dataset.miniGame = config.game;
    section.innerHTML = `
      <div class="phone-mini-header">
        <strong>${config.label} Mini</strong>
        <div class="phone-mini-header-actions">
          <button class="phone-mini-reload" type="button">Reload</button>
          <button class="phone-mini-open-full" type="button">Open Full</button>
          <button class="phone-mini-close" type="button" aria-label="Close">✕</button>
        </div>
      </div>
      <div class="phone-mini-frame-wrap">
        <div class="phone-mini-frame-loading">Loading ${config.label}...</div>
        <iframe class="phone-mini-game-frame" title="${config.label} Mini" loading="lazy" sandbox="allow-scripts allow-same-origin allow-forms"></iframe>
      </div>
    `;
    body.appendChild(section);
  });
}

function getPhoneMiniGameFrameUrl(gameKey) {
  const url = new URL(window.location.href);
  // Keep existing host-specific query params (CodePen preview tokens, etc.)
  // and only set the mini-game selector.
  url.searchParams.set("phoneMiniGame", String(gameKey || ""));
  url.searchParams.set("phoneMiniHost", "panel");
  url.searchParams.set("phoneMiniCash", roundCurrency(cash).toFixed(2));
  return url.toString();
}

function pushCashToPhoneMiniFrames({ exceptWindow = null } = {}) {
  if (IS_PHONE_EMBED_MODE) return;
  document.querySelectorAll(".phone-mini-game-frame").forEach((frame) => {
    if (!(frame instanceof HTMLIFrameElement)) return;
    if (frame.dataset.loaded !== "true" || !frame.src) return;
    const frameWindow = frame.contentWindow;
    if (!frameWindow || (exceptWindow && frameWindow === exceptWindow)) return;
    try {
      frameWindow.postMessage({ type: "phone-mini-init-cash", cash: roundCurrency(cash) }, "*");
    } catch (error) {}
  });
}

function ensurePhoneMiniGameFrame(panel, { forceReload = false } = {}) {
  if (!panel) return;
  const appName = panel.dataset.miniApp;
  const config = PHONE_MINI_GAME_MAP[appName];
  if (!config) return;

  const frame = panel.querySelector(".phone-mini-game-frame");
  const loading = panel.querySelector(".phone-mini-frame-loading");
  if (!frame) return;

  const setLoading = (message, visible) => {
    if (!loading) return;
    if (typeof message === "string" && message.length) loading.textContent = message;
    loading.hidden = !visible;
  };

  if (forceReload) {
    frame.dataset.loaded = "";
    frame.removeAttribute("src");
    setLoading(`Loading ${config.label}...`, true);
  }

  if (frame.dataset.loaded === "true" && frame.src) {
    try {
      frame.contentWindow?.postMessage({ type: "phone-mini-init-cash", cash: roundCurrency(cash) }, "*");
    } catch (error) {}
    return;
  }
  setLoading(`Loading ${config.label}...`, true);

  frame.onload = () => {
    frame.dataset.loaded = "true";
    setLoading("", false);
    try {
      frame.contentWindow?.postMessage({ type: "phone-mini-init-cash", cash: roundCurrency(cash) }, "*");
    } catch (error) {}
  };
  frame.onerror = () => {
    frame.dataset.loaded = "";
    setLoading(`Could not load ${config.label}.`, true);
  };
  frame.src = getPhoneMiniGameFrameUrl(config.game);
  frame.dataset.loaded = "loading";
}

function bindPhoneMiniGameApps() {
  if (phoneMiniAppsBound || !phoneUi.body) return;
  phoneMiniAppsBound = true;

  phoneUi.body.addEventListener("click", (event) => {
    const panel = event.target.closest(".phone-mini-game-app");
    if (!panel) return;
    const config = PHONE_MINI_GAME_MAP[panel.dataset.miniApp];
    if (!config) return;

    const reloadButton = event.target.closest(".phone-mini-reload");
    if (reloadButton) {
      ensurePhoneMiniGameFrame(panel, { forceReload: true });
      return;
    }

    const openFullButton = event.target.closest(".phone-mini-open-full");
    if (openFullButton) {
      const gate = getCasinoGateState();
      if (!gate.ok) {
        alert(gate.message);
        return;
      }
      tradingSection.style.display = "none";
      casinoSection.style.display = "block";
      loadCasinoGame(config.game);
      closePhone();
      return;
    }

    const closeButton = event.target.closest(".phone-mini-close");
    if (closeButton) {
      goPhoneHomeAnimated();
    }
  });
}

function bindPhoneMiniFrameMessages() {
  if (phoneMiniFrameMessageBound || IS_PHONE_EMBED_MODE) return;
  phoneMiniFrameMessageBound = true;

  window.addEventListener("message", (event) => {
    const data = event?.data;
    if (!data || typeof data !== "object") return;
    if (data.type === "phone-mini-close") {
      const phoneOpen = phoneUi.overlay && !phoneUi.overlay.classList.contains("hidden");
      if (!phoneOpen) return;
      goPhoneHomeAnimated();
      return;
    }
    if (data.type === "phone-mini-open-full") {
      const phoneOpen = phoneUi.overlay && !phoneUi.overlay.classList.contains("hidden");
      if (!phoneOpen) return;
      const gameKey = String(data.game || "").toLowerCase();
      if (!gameKey) return;
      const gate = getCasinoGateState();
      if (!gate.ok) {
        alert(gate.message);
        return;
      }
      tradingSection.style.display = "none";
      casinoSection.style.display = "block";
      loadCasinoGame(gameKey);
      closePhone();
      return;
    }
    if (data.type === "phone-mini-request-cash") {
      const source = event.source;
      if (!source || typeof source.postMessage !== "function") return;
      source.postMessage({ type: "phone-mini-init-cash", cash: roundCurrency(cash) }, "*");
      return;
    }
    if (data.type === "phone-mini-cash-update") {
      const nextCash = Number(data.cash);
      if (!Number.isFinite(nextCash)) return;
      const rounded = roundCurrency(nextCash);
      if (rounded === roundCurrency(cash)) return;
      cash = rounded;
      updateUI();
      pushCashToPhoneMiniFrames({ exceptWindow: event.source || null });
    }
  });
}

function requestPhoneMiniInitialCash() {
  if (!IS_PHONE_EMBED_MODE) return;
  if (!window.parent || window.parent === window) {
    phoneMiniCashReady = true;
    return;
  }

  try {
    window.parent.postMessage({ type: "phone-mini-request-cash" }, "*");
  } catch (error) {}

  if (phoneMiniCashRequestTimer) clearTimeout(phoneMiniCashRequestTimer);
  phoneMiniCashRequestTimer = setTimeout(() => {
    phoneMiniCashReady = true;
  }, 1200);
}

function bindPhoneMiniCashBridge() {
  if (!IS_PHONE_EMBED_MODE || phoneMiniCashBridgeBound) return;
  phoneMiniCashBridgeBound = true;

  window.addEventListener("message", (event) => {
    const data = event?.data;
    if (!data || typeof data !== "object") return;
    if (data.type !== "phone-mini-init-cash") return;

    const nextCash = Number(data.cash);
    if (!Number.isFinite(nextCash)) return;
    if (phoneMiniCashRequestTimer) {
      clearTimeout(phoneMiniCashRequestTimer);
      phoneMiniCashRequestTimer = null;
    }

    cash = roundCurrency(nextCash);
    phoneMiniCashReady = true;
    updateUI();
  });

  requestPhoneMiniInitialCash();
}

function postPhoneMiniCashUpdate() {
  if (!IS_PHONE_EMBED_MODE || !phoneMiniCashReady) return;
  if (!window.parent || window.parent === window) return;
  try {
    window.parent.postMessage({ type: "phone-mini-cash-update", cash: roundCurrency(cash) }, "*");
  } catch (error) {}
}

function savePhoneState() {
  try {
    const profile = getPersistProfileSnapshot();
    phoneState.cash = profile.cash;
    phoneState.shares = profile.shares;
    phoneState.avgCost = profile.avgCost;
    phoneState.savingsBalance = profile.savingsBalance;
    phoneState.autoSavingsPercent = profile.autoSavingsPercent;
    phoneState.balanceUpdatedAt = Math.max(0, Math.floor(Number(lastServerBalanceUpdatedAt) || 0));
    phoneState.playerId = profile.playerId;
    phoneState.username = profile.username;
    phoneState.missionXp = Math.max(0, Math.floor(Number(phoneState.missionXp) || 0));
    phoneState.missionTokens = Math.max(0, Math.floor(Number(phoneState.missionTokens) || 0));
    if (!phoneState.missionLevelMilestonesClaimed || typeof phoneState.missionLevelMilestonesClaimed !== "object") {
      phoneState.missionLevelMilestonesClaimed = {};
    }
    if (!phoneState.vip || typeof phoneState.vip !== "object") {
      phoneState.vip = { active: false, purchasedAt: 0, lastWeeklyBonusAt: 0 };
    }
    localStorage.setItem(PHONE_STORAGE_KEY, JSON.stringify(phoneState));
  } catch (error) {}
}

function persistCashStateSoon() {
  if (IS_PHONE_EMBED_MODE) return;
  const profile = getPersistProfileSnapshot();
  const signature = getPersistProfileSignature(profile);
  if (lastPersistedProfileSig && signature === lastPersistedProfileSig && !cashPersistTimer) return;
  if (cashPersistTimer) return;
  cashPersistTimer = window.setTimeout(() => {
    cashPersistTimer = null;
    const snapshot = getPersistProfileSnapshot();
    const snapshotSig = getPersistProfileSignature(snapshot);
    if (lastPersistedProfileSig && snapshotSig === lastPersistedProfileSig) return;
    phoneState.cash = snapshot.cash;
    phoneState.shares = snapshot.shares;
    phoneState.avgCost = snapshot.avgCost;
    phoneState.savingsBalance = snapshot.savingsBalance;
    phoneState.autoSavingsPercent = snapshot.autoSavingsPercent;
    phoneState.balanceUpdatedAt = Math.max(0, Math.floor(Number(lastServerBalanceUpdatedAt) || 0));
    phoneState.playerId = snapshot.playerId;
    phoneState.username = snapshot.username;
    savePhoneState();
    lastPersistedProfileSig = snapshotSig;
  }, 120);
}

function persistCashStateNow() {
  if (IS_PHONE_EMBED_MODE) return;
  if (cashPersistTimer) {
    clearTimeout(cashPersistTimer);
    cashPersistTimer = null;
  }
  savePhoneState();
  lastPersistedProfileSig = getPersistProfileSignature();
}

function syncCurrentUserProfileOnExit() {
  if (!playerUsername || !venmoClaimPlayerId || IS_PHONE_EMBED_MODE) return;
  const payload = JSON.stringify({
    playerId: venmoClaimPlayerId,
    username: playerUsername,
    clientBalanceUpdatedAt: Math.max(0, Math.floor(Number(lastServerBalanceUpdatedAt) || 0)),
    balance: roundCurrency(cash),
    shares: Math.max(0, Math.floor(Number(shares) || 0)),
    avgCost: Math.max(0, Number(avgCost) || 0),
    savingsBalance: roundCurrency(savingsBalance),
    autoSavingsPercent: roundCurrency(clampPercent(autoSavingsPercent))
  });
  let sentWithBeacon = false;
  try {
    if (typeof navigator !== "undefined" && typeof navigator.sendBeacon === "function") {
      const body = new Blob([payload], { type: "application/json" });
      sentWithBeacon = navigator.sendBeacon("/api/users/sync", body);
    }
  } catch (error) {}
  if (sentWithBeacon) return;
  try {
    fetch("/api/users/sync", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      credentials: "include",
      keepalive: true,
      body: payload
    }).catch(() => {});
  } catch (error) {}
}

function flushPersistenceBeforeExit() {
  persistCashStateNow();
  syncCurrentUserProfileOnExit();
}

function bindPersistenceFlushHandlers() {
  if (IS_PHONE_EMBED_MODE || persistenceFlushHandlersBound) return;
  persistenceFlushHandlersBound = true;
  window.addEventListener("pagehide", flushPersistenceBeforeExit);
  window.addEventListener("beforeunload", flushPersistenceBeforeExit);
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "hidden") {
      flushPersistenceBeforeExit();
    }
  });
}

function loadPhoneState({ force = false } = {}) {
  if (phoneStateLoaded && !force) return;
  try {
    const raw = localStorage.getItem(PHONE_STORAGE_KEY);
    phoneStateLoaded = true;
    if (!raw) return;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return;

    if (Array.isArray(parsed.notifications)) {
      phoneState.notifications = parsed.notifications.slice(0, 120);
    }
    if (Array.isArray(parsed.bankHistory)) {
      phoneState.bankHistory = parsed.bankHistory.slice(0, 40);
    }
    const unread = Number(parsed.unread);
    if (Number.isFinite(unread)) {
      phoneState.unread = Math.max(0, Math.floor(unread));
    }
    const casinoProfit = Number(parsed.casinoProfit);
    if (Number.isFinite(casinoProfit)) {
      phoneState.casinoProfit = roundCurrency(casinoProfit);
    }
    const savedCash = Number(parsed.cash);
    if (
      Number.isFinite(savedCash) &&
      savedCash >= 0 &&
      !(IS_PHONE_EMBED_MODE && Number.isFinite(PHONE_EMBED_INITIAL_CASH) && PHONE_EMBED_INITIAL_CASH >= 0)
    ) {
      cash = roundCurrency(savedCash);
    }
    const savedShares = Number(parsed.shares);
    if (Number.isFinite(savedShares) && savedShares >= 0) {
      shares = Math.max(0, Math.floor(savedShares));
    }
    const savedAvgCost = Number(parsed.avgCost);
    if (Number.isFinite(savedAvgCost) && savedAvgCost >= 0) {
      avgCost = roundCurrency(savedAvgCost);
    }
    const savedSavingsBalance = Number(parsed.savingsBalance);
    if (Number.isFinite(savedSavingsBalance) && savedSavingsBalance >= 0) {
      savingsBalance = roundCurrency(savedSavingsBalance);
    }
    const savedAutoSavingsPercent = Number(parsed.autoSavingsPercent);
    if (Number.isFinite(savedAutoSavingsPercent)) {
      phoneState.autoSavingsPercent = roundCurrency(clampPercent(savedAutoSavingsPercent));
      autoSavingsPercent = phoneState.autoSavingsPercent;
    }
    const savedBalanceUpdatedAt = Number(parsed.balanceUpdatedAt);
    if (Number.isFinite(savedBalanceUpdatedAt) && savedBalanceUpdatedAt > 0) {
      lastServerBalanceUpdatedAt = Math.floor(savedBalanceUpdatedAt);
      phoneState.balanceUpdatedAt = lastServerBalanceUpdatedAt;
    }
    const savedPlayerId = String(parsed.playerId || "").trim();
    if (savedPlayerId) {
      phoneState.playerId = savedPlayerId;
      venmoClaimPlayerId = savedPlayerId;
      try {
        localStorage.setItem(VENMO_PLAYER_ID_STORAGE_KEY, savedPlayerId);
      } catch (error) {}
    }
    const savedUsername = normalizeUsername(parsed.username);
    if (savedUsername) {
      playerUsername = savedUsername;
      try {
        USERNAME_STORAGE_FALLBACK_KEYS.forEach((key) => {
          localStorage.setItem(key, savedUsername);
        });
        persistUsernameSetupDone();
      } catch (error) {}
    }
    if (parsed.settings && typeof parsed.settings === "object") {
      phoneState.settings.animations = parsed.settings.animations !== false;
      phoneState.settings.popupSeconds = clampMarket(Number(parsed.settings.popupSeconds) || 2.2, 1, 8);
      phoneState.settings.autoRoundLimit = Math.max(0, Math.floor(Number(parsed.settings.autoRoundLimit) || 0));
      phoneState.settings.uiScale = clampMarket(Number(parsed.settings.uiScale) || 100, 85, 115);
    }
    if (parsed.missionsClaimed && typeof parsed.missionsClaimed === "object") {
      phoneState.missionsClaimed = { ...parsed.missionsClaimed };
    }
    const missionXp = Number(parsed.missionXp);
    if (Number.isFinite(missionXp)) {
      phoneState.missionXp = Math.max(0, Math.floor(missionXp));
    }
    const missionTokens = Number(parsed.missionTokens);
    if (Number.isFinite(missionTokens)) {
      phoneState.missionTokens = Math.max(0, Math.floor(missionTokens));
    }
    if (parsed.missionLevelMilestonesClaimed && typeof parsed.missionLevelMilestonesClaimed === "object") {
      phoneState.missionLevelMilestonesClaimed = { ...parsed.missionLevelMilestonesClaimed };
    }
    if (parsed.vip && typeof parsed.vip === "object") {
      phoneState.vip = {
        active: parsed.vip.active === true,
        purchasedAt: Math.max(0, Number(parsed.vip.purchasedAt) || 0),
        lastWeeklyBonusAt: Math.max(0, Number(parsed.vip.lastWeeklyBonusAt) || 0)
      };
    }
  } catch (error) {}
  lastPersistedProfileSig = getPersistProfileSignature();
}

function clearTotalProgressStorage() {
  try {
    TOTAL_RESET_STORAGE_KEYS.forEach((key) => {
      localStorage.removeItem(key);
    });
    for (let i = localStorage.length - 1; i >= 0; i -= 1) {
      const key = localStorage.key(i);
      if (!key) continue;
      if (TOTAL_RESET_STORAGE_PREFIXES.some((prefix) => key.startsWith(prefix))) {
        localStorage.removeItem(key);
      }
    }
  } catch (error) {}
}

function resetAchievementStateInMemory() {
  achievementState.unlocked = {};
  achievementState.stats = {
    steps: 0,
    buys: 0,
    sells: 0,
    newsSeen: 0,
    loansTaken: 0,
    casinoWins: 0,
    peakNet: BASE_NET_WORTH,
    peakCash: BASE_NET_WORTH
  };
}

function resetBankMissionStateInMemory() {
  const defaults = createDefaultBankMissionState();
  bankMissionState.progress = { ...defaults.progress };
  bankMissionState.completed = { ...defaults.completed };
  bankMissionState.claimed = { ...defaults.claimed };
  bankMissionState.counters = {
    ...defaults.counters,
    casinoWinsByGame: { ...defaults.counters.casinoWinsByGame }
  };
}

function resetPhoneProgressStateInMemory() {
  phoneState.unread = 0;
  phoneState.notifications = [];
  phoneState.bankHistory = [];
  phoneState.casinoProfit = 0;
  phoneState.autoSavingsPercent = 0;
  phoneState.settings = {
    animations: true,
    popupSeconds: 2.2,
    autoRoundLimit: 0,
    uiScale: 100
  };
  phoneState.missionsClaimed = {};
  phoneState.missionXp = 0;
  phoneState.missionTokens = 0;
  phoneState.missionLevelMilestonesClaimed = {};
  phoneState.vip = {
    active: false,
    purchasedAt: 0,
    lastWeeklyBonusAt: 0
  };
  phoneState.autoRoundCounters = {
    slots: 0,
    roulette: 0
  };
}

function applyProgressResetState() {
  cash = roundCurrency(BASE_NET_WORTH);
  shares = 0;
  avgCost = 0;
  savingsBalance = 0;
  autoSavingsPercent = 0;

  resetPhoneProgressStateInMemory();
  resetBankMissionStateInMemory();
  resetAchievementStateInMemory();

  phoneState.cash = cash;
  phoneState.shares = shares;
  phoneState.avgCost = avgCost;
  phoneState.savingsBalance = savingsBalance;
  phoneState.autoSavingsPercent = autoSavingsPercent;
  bankMissionLastCashSnapshot = cash;
  pokerLastSessionProfit = 0;
  refreshPokerSessionMeta();
  renderAchievements();
}

async function syncResetBalanceToServer() {
  if (userProfileSyncTimer) {
    clearTimeout(userProfileSyncTimer);
    userProfileSyncTimer = null;
  }
  lastUserProfileSyncAt = 0;
  persistCashStateNow();
  if (!playerUsername || !venmoClaimPlayerId || IS_PHONE_EMBED_MODE) return false;
  try {
    return await syncCurrentUserProfileToServer({ force: true });
  } catch {
    return false;
  }
}

async function runTotalProgressReset() {
  const confirmed = window.confirm(
    "Reset everything?\n\nThis will erase your cash, achievements, missions, bank history, and settings."
  );
  if (!confirmed) return;

  applyProgressResetState();
  updateUI();
  await syncResetBalanceToServer();
  clearTotalProgressStorage();

  window.location.reload();
}

function getPhoneTimeLabel(timestamp) {
  const date = new Date(timestamp);
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function refreshPhoneStatusTime() {
  if (!phoneUi.statusTime) return;
  const now = new Date();
  phoneUi.statusTime.textContent = now.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

function pushPhoneBankHistory(text, amount = 0, tone = "neutral") {
  if (!text) return;
  const normalizedTone = tone === "positive" || tone === "negative" ? tone : "neutral";
  phoneState.bankHistory.unshift({
    text: String(text),
    amount: roundCurrency(amount),
    tone: normalizedTone,
    ts: Date.now()
  });
  if (phoneState.bankHistory.length > 40) {
    phoneState.bankHistory.length = 40;
  }
  renderPhoneBankHistory();
  savePhoneState();
}

function renderPhoneBankHistory() {
  if (!phoneUi.bankHistory) return;
  phoneUi.bankHistory.innerHTML = "";
  if (phoneState.bankHistory.length === 0) {
    const empty = document.createElement("div");
    empty.className = "phone-message-item";
    empty.innerHTML = `<div class="meta"><span>BANK</span><span>--:--</span></div><div>No bank activity yet.</div>`;
    phoneUi.bankHistory.appendChild(empty);
    return;
  }

  const fragment = document.createDocumentFragment();
  phoneState.bankHistory.slice(0, 24).forEach((item) => {
    const amount = Number(item.amount) || 0;
    const toneClass = item.tone === "positive" ? "bank-positive" : item.tone === "negative" ? "bank-negative" : "bank";
    const row = document.createElement("div");
    row.className = `phone-message-item ${toneClass}`.trim();
    row.innerHTML = `
      <div class="meta"><span>BANK</span><span>${getPhoneTimeLabel(item.ts)}</span></div>
      <div>${item.text}</div>
      <div class="meta"><span>${amount >= 0 ? "+" : "-"}${formatCurrency(Math.abs(amount))}</span><span></span></div>
    `;
    fragment.appendChild(row);
  });
  phoneUi.bankHistory.appendChild(fragment);
}

function pushPhoneNotification(type, text) {
  if (!text) return;
  phoneState.notifications.unshift({
    type: type || "info",
    text: String(text),
    ts: Date.now()
  });
  if (phoneState.notifications.length > 120) {
    phoneState.notifications.length = 120;
  }

  const isOpen = phoneUi.overlay && !phoneUi.overlay.classList.contains("hidden");
  if (!isOpen) {
    phoneState.unread = Math.max(0, phoneState.unread + 1);
  }

  renderPhoneBadge();
  renderPhoneMessages();
  savePhoneState();
}

function renderPhoneBadge() {
  if (!phoneUi.badge) return;
  const unread = Math.max(0, phoneState.unread);
  phoneUi.badge.textContent = unread > 99 ? "99+" : String(unread);
  phoneUi.badge.classList.toggle("hidden", unread <= 0);
  updatePhoneHomeAppBadges();
}

function getClaimablePhoneMissionCount() {
  let claimable = 0;
  const defs = getPhoneMissionDefs();
  ["daily", "weekly"].forEach((groupKey) => {
    const missions = defs[groupKey] || [];
    missions.forEach((mission) => {
      if (phoneState.missionsClaimed[mission.id]) return;
      if (typeof mission.check === "function" && mission.check()) claimable += 1;
    });
  });

  if (typeof BANK_MISSION_DEFS !== "undefined" && Array.isArray(BANK_MISSION_DEFS)) {
    BANK_MISSION_DEFS.forEach((mission) => {
      if (typeof isBankMissionCompleted !== "function" || typeof isBankMissionClaimed !== "function") return;
      if (isBankMissionCompleted(mission.id) && !isBankMissionClaimed(mission.id)) {
        claimable += 1;
      }
    });
  }

  return claimable;
}

function setPhoneHomeAppBadge(appName, count) {
  if (!phoneUi.homeApps || phoneUi.homeApps.length === 0) return;
  const safeCount = Math.max(0, Math.floor(Number(count) || 0));
  const label = safeCount > 99 ? "99+" : String(safeCount);
  phoneUi.homeApps
    .filter((button) => button.dataset.targetApp === appName)
    .forEach((button) => {
      let badge = button.querySelector(".phone-home-app-badge");
      if (!badge) {
        badge = document.createElement("span");
        badge.className = "phone-home-app-badge hidden";
        button.appendChild(badge);
      }
      if (safeCount <= 0) {
        badge.classList.add("hidden");
        badge.textContent = "";
      } else {
        badge.classList.remove("hidden");
        badge.textContent = label;
      }
    });
}

function updatePhoneHomeAppBadges() {
  const unread = Math.max(0, Math.floor(phoneState.unread || 0));
  const claimableMissions = getClaimablePhoneMissionCount();
  setPhoneHomeAppBadge("messages", unread);
  setPhoneHomeAppBadge("missions", claimableMissions);
}

function setPhoneApp(appName) {
  if (!phoneUi.overlay) return;
  const app = String(appName || "messages");
  if (app === "casino" || PHONE_MINI_GAME_MAP[app]) {
    const gate = getCasinoGateState();
    if (!gate.ok) {
      alert(gate.message);
      return;
    }
  }
  const panelId = phonePanelId(app);
  const targetPanel = document.getElementById(panelId);
  const wasAlreadyActive = Boolean(targetPanel && targetPanel.classList.contains("active"));
  phoneUi.tabs.forEach((tab) => {
    tab.classList.toggle("active", tab.dataset.phoneApp === app);
  });
  phoneUi.apps.forEach((panel) => {
    const active = panel.id === panelId;
    panel.classList.toggle("active", active);
  });

  if (PHONE_MINI_GAME_MAP[app]) {
    const panel = document.getElementById(panelId);
    ensurePhoneMiniGameFrame(panel, { forceReload: true });
    return;
  }

  if (app === "messages") {
    if (phoneState.unread > 0) {
      phoneState.unread = 0;
      savePhoneState();
      renderPhoneBadge();
    }
    renderPhoneMessages();
  }
  if (app === "bank") {
    refreshPhoneBankApp();
    if (!wasAlreadyActive) trackBankMissionBankAppOpened();
  }
  if (app === "portfolio") refreshPhonePortfolioApp();
  if (app === "casino") refreshPhoneCasinoApp();
  if (app === "missions") renderPhoneMissions();
  if (app === "feedback") {
    void refreshPhoneFeedbackFromServer({ silent: true });
  }
}

function openPhone(appName = "home") {
  if (!phoneUi.overlay) return;
  if (phoneOverlayAnimTimer) {
    clearTimeout(phoneOverlayAnimTimer);
    phoneOverlayAnimTimer = null;
  }
  phoneUi.overlay.classList.remove("hidden");
  phoneUi.overlay.classList.add("phone-opening");
  phoneUi.overlay.setAttribute("aria-hidden", "false");
  setPhoneApp(appName);
  phoneOverlayAnimTimer = setTimeout(() => {
    if (phoneUi.overlay) {
      phoneUi.overlay.classList.remove("phone-opening");
    }
    phoneOverlayAnimTimer = null;
  }, 720);
}

function closePhone() {
  if (!phoneUi.overlay) return;
  if (phoneOverlayAnimTimer) {
    clearTimeout(phoneOverlayAnimTimer);
    phoneOverlayAnimTimer = null;
  }
  phoneUi.overlay.classList.remove("phone-opening");
  if (phoneAppOpenTimer) {
    clearTimeout(phoneAppOpenTimer);
    phoneAppOpenTimer = null;
  }
  if (phoneHomeAnimTimer) {
    clearTimeout(phoneHomeAnimTimer);
    phoneHomeAnimTimer = null;
  }
  if (phoneHomeAnimFailSafeTimer) {
    clearTimeout(phoneHomeAnimFailSafeTimer);
    phoneHomeAnimFailSafeTimer = null;
  }
  if (phoneUi.body) {
    phoneUi.body.classList.remove("phone-app-opening");
    phoneUi.body.classList.remove("phone-home-transitioning");
    phoneUi.body.classList.remove("phone-home-smooth");
  }
  if (phoneUi.shell) {
    phoneUi.shell.classList.remove("phone-home-fly-active");
  }
  if (Array.isArray(phoneUi.apps)) {
    phoneUi.apps.forEach((panel) => {
      panel.classList.remove("phone-app-enter-zoom");
      panel.classList.remove("phone-app-underlay-dim");
      panel.classList.remove("phone-home-enter-preview");
      panel.classList.remove("phone-app-exit-up");
      panel.classList.remove("phone-app-exit-to-icon");
      panel.style.transition = "";
      panel.style.transform = "";
      panel.style.opacity = "";
      panel.style.borderRadius = "";
      panel.style.transformOrigin = "";
      panel.style.background = "";
    });
  }
  phoneUi.overlay.classList.add("hidden");
  phoneUi.overlay.setAttribute("aria-hidden", "true");
}

function getPhoneAppNameFromPanel(panel) {
  if (!panel || typeof panel.id !== "string") return "home";
  if (!panel.id.startsWith("phoneApp")) return "home";
  return panel.id.slice("phoneApp".length).toLowerCase();
}

function getHomeIconForApp(appName) {
  const app = String(appName || "");
  if (!app) return null;

  const dockIcon = document.querySelector(`.phone-home-dock .phone-home-app[data-target-app="${app}"]`);
  const gridIcon = document.querySelector(`.phone-app-grid .phone-home-app[data-target-app="${app}"]`);
  const anyIcon = document.querySelector(`.phone-home-app[data-target-app="${app}"]`);
  const lastUsedIcon = phoneLastLaunchIconByApp[app];
  const hasValidLastIcon =
    lastUsedIcon &&
    typeof lastUsedIcon === "object" &&
    typeof lastUsedIcon.isConnected === "boolean" &&
    lastUsedIcon.isConnected &&
    lastUsedIcon.dataset &&
    lastUsedIcon.dataset.targetApp === app;

  if (hasValidLastIcon) {
    return lastUsedIcon;
  }

  if (phoneDockPinnedApps.has(app)) {
    return dockIcon || gridIcon || anyIcon;
  }

  return gridIcon || dockIcon || anyIcon;
}

function openPhoneAppFromIcon(appName, sourceEl) {
  const app = String(appName || "");
  if (!app || app === "home") {
    goPhoneHomeAnimated();
    return;
  }
  if (app === "casino" || PHONE_MINI_GAME_MAP[app]) {
    const gate = getCasinoGateState();
    if (!gate.ok) {
      alert(gate.message);
      return;
    }
  }
  if (!phoneUi.body || !sourceEl) {
    setPhoneApp(app);
    return;
  }

  const targetPanel = document.getElementById(`phoneApp${app.charAt(0).toUpperCase()}${app.slice(1)}`);
  if (!targetPanel) {
    setPhoneApp(app);
    return;
  }

  const activePanel = Array.isArray(phoneUi.apps)
    ? phoneUi.apps.find((panel) => panel.classList.contains("active"))
    : null;
  if (!activePanel || activePanel === targetPanel) {
    setPhoneApp(app);
    return;
  }

  if (sourceEl && sourceEl.dataset && sourceEl.dataset.targetApp === app) {
    phoneLastLaunchIconByApp[app] = sourceEl;
  }

  if (phoneAppOpenTimer) {
    clearTimeout(phoneAppOpenTimer);
    phoneAppOpenTimer = null;
  }

  phoneUi.body.classList.add("phone-app-opening");
  activePanel.classList.add("phone-app-underlay-dim");
  targetPanel.classList.add("phone-app-enter-zoom");

  const bodyRect = phoneUi.body.getBoundingClientRect();
  const sourceRect = sourceEl.getBoundingClientRect();
  const sourceW = Math.max(36, sourceRect.width);
  const sourceH = Math.max(36, sourceRect.height);

  const translateX = sourceRect.left - bodyRect.left + sourceW * 0.5 - bodyRect.width * 0.5;
  const translateY = sourceRect.top - bodyRect.top + sourceH * 0.5 - bodyRect.height * 0.5;
  const scaleX = clampMarket(sourceW / Math.max(1, bodyRect.width), 0.08, 1);
  const scaleY = clampMarket(sourceH / Math.max(1, bodyRect.height), 0.08, 1);

  targetPanel.style.transition = "none";
  targetPanel.style.transformOrigin = "50% 50%";
  targetPanel.style.transform = `translate(${translateX.toFixed(2)}px, ${translateY.toFixed(2)}px) scale(${scaleX.toFixed(4)}, ${scaleY.toFixed(4)})`;
  targetPanel.style.opacity = "0.98";
  targetPanel.style.borderRadius = "18px";
  targetPanel.style.background = "rgba(9, 14, 28, 0.98)";

  requestAnimationFrame(() => {
    requestAnimationFrame(() => {
      targetPanel.style.transition =
        "transform 520ms cubic-bezier(0.18, 0.88, 0.22, 1), opacity 420ms ease, border-radius 520ms cubic-bezier(0.18, 0.88, 0.22, 1)";
      targetPanel.style.transform = "translate(0px, 0px) scale(1, 1)";
      targetPanel.style.opacity = "1";
      targetPanel.style.borderRadius = "0px";
    });
  });

  phoneAppOpenTimer = setTimeout(() => {
    setPhoneApp(app);
    requestAnimationFrame(() => {
      activePanel.classList.remove("phone-app-underlay-dim");
      targetPanel.classList.remove("phone-app-enter-zoom");
      phoneUi.body.classList.remove("phone-app-opening");
      targetPanel.style.transition = "";
      targetPanel.style.transform = "";
      targetPanel.style.opacity = "";
      targetPanel.style.borderRadius = "";
      targetPanel.style.transformOrigin = "";
      targetPanel.style.background = "";
      phoneAppOpenTimer = null;
    });
  }, 620);
}

function goPhoneHomeAnimated() {
  if (phoneUi.body) {
    phoneUi.body.classList.remove("phone-app-opening");
  }
  if (Array.isArray(phoneUi.apps)) {
    phoneUi.apps.forEach((panel) => {
      panel.classList.remove("phone-app-enter-zoom");
      panel.classList.remove("phone-app-underlay-dim");
      if (!panel.classList.contains("phone-app-exit-to-icon") && !panel.classList.contains("phone-app-exit-up")) {
        panel.style.transition = "";
        panel.style.transform = "";
        panel.style.opacity = "";
        panel.style.borderRadius = "";
        panel.style.transformOrigin = "";
        panel.style.background = "";
      }
    });
  }
  if (phoneAppOpenTimer) {
    clearTimeout(phoneAppOpenTimer);
    phoneAppOpenTimer = null;
  }
  if (phoneHomeAnimTimer) {
    clearTimeout(phoneHomeAnimTimer);
    phoneHomeAnimTimer = null;
  }
  if (phoneHomeAnimFailSafeTimer) {
    clearTimeout(phoneHomeAnimFailSafeTimer);
    phoneHomeAnimFailSafeTimer = null;
  }

  if (phoneUi.homeBtn) {
    phoneUi.homeBtn.classList.add("phone-home-bar-hidden");
  }

  if (phoneUi.shell) {
    phoneUi.shell.classList.remove("phone-home-fly-active");
  }
  if (phoneHomeBarAnimTimer) {
    clearTimeout(phoneHomeBarAnimTimer);
    phoneHomeBarAnimTimer = null;
  }
  phoneHomeBarAnimTimer = setTimeout(() => {
    if (phoneUi.homeBtn) {
      phoneUi.homeBtn.classList.remove("phone-home-bar-hidden");
    }
    phoneHomeBarAnimTimer = null;
  }, 760);

  if (!phoneUi.body) {
    setPhoneApp("home");
    return;
  }

  const homePanel = document.getElementById("phoneAppHome");
  const activePanel = Array.isArray(phoneUi.apps)
    ? phoneUi.apps.find((panel) => panel.classList.contains("active"))
    : null;

  if (homePanel && activePanel && activePanel !== homePanel) {
    const currentAppName = getPhoneAppNameFromPanel(activePanel);
    const targetIcon = getHomeIconForApp(currentAppName);
    let didFinalize = false;
    const finalizeHomeTransition = () => {
      if (didFinalize) return;
      didFinalize = true;
      if (!phoneUi.body) return;
      setPhoneApp("home");
      requestAnimationFrame(() => {
        if (!phoneUi.body) return;
        phoneUi.body.classList.remove("phone-home-transitioning");
        homePanel.classList.remove("phone-home-enter-preview");
        activePanel.classList.remove("phone-app-exit-up");
        activePanel.classList.remove("phone-app-exit-to-icon");
        activePanel.classList.remove("phone-app-enter-zoom");
        activePanel.classList.remove("phone-app-underlay-dim");
        activePanel.style.transition = "";
        activePanel.style.transform = "";
        activePanel.style.opacity = "";
        activePanel.style.borderRadius = "";
        activePanel.style.transformOrigin = "";
        if (phoneHomeAnimFailSafeTimer) {
          clearTimeout(phoneHomeAnimFailSafeTimer);
          phoneHomeAnimFailSafeTimer = null;
        }
        phoneHomeAnimTimer = null;
      });
    };

    phoneUi.body.classList.add("phone-home-transitioning");
    homePanel.classList.add("phone-home-enter-preview");
    activePanel.classList.add("phone-app-exit-to-icon");

    if (targetIcon) {
      const bodyRect = phoneUi.body.getBoundingClientRect();
      const iconRect = targetIcon.getBoundingClientRect();
      const iconW = Math.max(36, iconRect.width);
      const iconH = Math.max(36, iconRect.height);
      const translateX = iconRect.left - bodyRect.left + iconW * 0.5 - bodyRect.width * 0.5;
      const translateY = iconRect.top - bodyRect.top + iconH * 0.5 - bodyRect.height * 0.5;
      const scaleX = clampMarket(iconW / Math.max(1, bodyRect.width), 0.08, 1);
      const scaleY = clampMarket(iconH / Math.max(1, bodyRect.height), 0.08, 1);

      activePanel.style.transition = "none";
      activePanel.style.transformOrigin = "50% 50%";
      activePanel.style.transform = "translate(0px, 0px) scale(1, 1)";
      activePanel.style.opacity = "1";
      activePanel.style.borderRadius = "0px";

      requestAnimationFrame(() => {
        requestAnimationFrame(() => {
          activePanel.style.transition =
            "transform 760ms cubic-bezier(0.16, 0.9, 0.22, 1), opacity 620ms ease, border-radius 760ms cubic-bezier(0.16, 0.9, 0.22, 1)";
          activePanel.style.transform =
            `translate(${translateX.toFixed(2)}px, ${translateY.toFixed(2)}px) scale(${scaleX.toFixed(4)}, ${scaleY.toFixed(4)})`;
          activePanel.style.opacity = "0.04";
          activePanel.style.borderRadius = "16px";
        });
      });
    } else {
      activePanel.classList.add("phone-app-exit-up");
    }

    const onCloseTransitionEnd = (event) => {
      if (!event || event.target !== activePanel) return;
      if (event.propertyName !== "transform" && event.propertyName !== "opacity") return;
      activePanel.removeEventListener("transitionend", onCloseTransitionEnd);
      finalizeHomeTransition();
    };
    activePanel.addEventListener("transitionend", onCloseTransitionEnd);

    phoneHomeAnimTimer = setTimeout(finalizeHomeTransition, 780);
    phoneHomeAnimFailSafeTimer = setTimeout(finalizeHomeTransition, 920);
    return;
  }

  setPhoneApp("home");
  phoneUi.body.classList.remove("phone-home-smooth");
  phoneUi.body.offsetHeight;
  phoneUi.body.classList.add("phone-home-smooth");

  phoneHomeAnimTimer = setTimeout(() => {
    if (!phoneUi.body) return;
    phoneUi.body.classList.remove("phone-home-smooth");
    phoneHomeAnimTimer = null;
  }, 620);
}

function renderPhoneMessages() {
  if (!phoneUi.messageList) return;
  phoneUi.messageList.innerHTML = "";
  if (phoneState.notifications.length === 0) {
    const empty = document.createElement("div");
    empty.className = "phone-message-item";
    empty.innerHTML = `<div class="meta"><span>System</span><span>--:--</span></div><div>No alerts yet.</div>`;
    phoneUi.messageList.appendChild(empty);
    return;
  }

  const fragment = document.createDocumentFragment();
  phoneState.notifications.slice(0, 80).forEach((item) => {
    const row = document.createElement("div");
    row.className = `phone-message-item ${item.type || "info"}`.trim();
    row.innerHTML = `
      <div class="meta"><span>${(item.type || "info").toUpperCase()}</span><span>${getPhoneTimeLabel(item.ts)}</span></div>
      <div>${item.text}</div>
    `;
    fragment.appendChild(row);
  });
  phoneUi.messageList.appendChild(fragment);
}

function getPhoneFeedbackCategoryLabel(category) {
  const normalized = String(category || "").trim().toLowerCase();
  if (normalized === "bug") return "Bug";
  if (normalized === "other") return "Other";
  return "Idea";
}

function getPhoneFeedbackStatusLabel(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "closed") return "Closed";
  if (normalized === "reviewed") return "Reviewed";
  return "Open";
}

function setPhoneFeedbackStatus(message, isError = false) {
  if (!phoneUi.feedbackStatus) return;
  phoneUi.feedbackStatus.textContent = String(message || "");
  phoneUi.feedbackStatus.style.color = isError ? "#ff8ea0" : "#9eb6c8";
}

function renderPhoneFeedbackList(entries = phoneFeedbackEntries) {
  if (!phoneUi.feedbackList) return;
  phoneUi.feedbackList.innerHTML = "";
  if (!Array.isArray(entries) || entries.length === 0) {
    const empty = document.createElement("div");
    empty.className = "phone-message-item phone-feedback-item";
    empty.innerHTML = `<div class="meta"><span>Feedback</span><span>--:--</span></div><div>No feedback submitted yet.</div>`;
    phoneUi.feedbackList.appendChild(empty);
    return;
  }

  const fragment = document.createDocumentFragment();
  entries.forEach((entry) => {
    const row = document.createElement("div");
    row.className = "phone-message-item phone-feedback-item";
    row.innerHTML = `
      <div class="meta"><span>${escapeHtml(getPhoneFeedbackCategoryLabel(entry?.category))}</span><span>${escapeHtml(formatDateTime(entry?.submittedAt))}</span></div>
      <div>${escapeHtml(entry?.message || "")}</div>
      <div class="meta"><strong>${escapeHtml(getPhoneFeedbackStatusLabel(entry?.status))}</strong><span></span></div>
    `;
    fragment.appendChild(row);
  });
  phoneUi.feedbackList.appendChild(fragment);
}

async function refreshPhoneFeedbackFromServer({ silent = false } = {}) {
  if (!authSessionLikelyAuthenticated) {
    phoneFeedbackEntries = [];
    renderPhoneFeedbackList(phoneFeedbackEntries);
    if (!silent) setPhoneFeedbackStatus("Login required to view feedback.", true);
    return false;
  }
  try {
    const payload = await venmoApiRequest("/api/feedback?limit=20");
    phoneFeedbackEntries = Array.isArray(payload?.feedback) ? payload.feedback : [];
    renderPhoneFeedbackList(phoneFeedbackEntries);
    if (!silent) {
      setPhoneFeedbackStatus(phoneFeedbackEntries.length > 0 ? "Feedback synced." : "No feedback submitted yet.");
    }
    return true;
  } catch (error) {
    if (!silent) {
      setPhoneFeedbackStatus(String(error?.message || "Could not load feedback."), true);
    }
    return false;
  }
}

async function submitPhoneFeedback() {
  if (phoneFeedbackSubmitBusy) return;
  if (!phoneUi.feedbackInput) return;
  const message = String(phoneUi.feedbackInput.value || "")
    .replace(/\s+/g, " ")
    .trim();
  const category = String(phoneUi.feedbackCategory?.value || "idea")
    .trim()
    .toLowerCase();

  if (!authSessionLikelyAuthenticated) {
    setPhoneFeedbackStatus("Login required to submit feedback.", true);
    return;
  }
  if (message.length < PHONE_FEEDBACK_MESSAGE_MIN_LEN) {
    setPhoneFeedbackStatus(`Feedback must be at least ${PHONE_FEEDBACK_MESSAGE_MIN_LEN} characters.`, true);
    return;
  }
  if (message.length > PHONE_FEEDBACK_MESSAGE_MAX_LEN) {
    setPhoneFeedbackStatus(`Feedback must be ${PHONE_FEEDBACK_MESSAGE_MAX_LEN} characters or less.`, true);
    return;
  }

  phoneFeedbackSubmitBusy = true;
  if (phoneUi.feedbackSubmitBtn) phoneUi.feedbackSubmitBtn.disabled = true;
  setPhoneFeedbackStatus("Submitting feedback...");
  try {
    const payload = await venmoApiRequest("/api/feedback", {
      method: "POST",
      body: { category, message }
    });
    if (phoneUi.feedbackInput) phoneUi.feedbackInput.value = "";
    const created = payload?.feedback;
    if (created && typeof created === "object") {
      const createdId = String(created.id || "");
      phoneFeedbackEntries = [created, ...phoneFeedbackEntries.filter((item) => String(item?.id || "") !== createdId)];
      if (phoneFeedbackEntries.length > 30) {
        phoneFeedbackEntries = phoneFeedbackEntries.slice(0, 30);
      }
      renderPhoneFeedbackList(phoneFeedbackEntries);
    } else {
      await refreshPhoneFeedbackFromServer({ silent: true });
    }
    setPhoneFeedbackStatus("Feedback submitted. Thanks.");
  } catch (error) {
    setPhoneFeedbackStatus(String(error?.message || "Could not submit feedback."), true);
  } finally {
    phoneFeedbackSubmitBusy = false;
    if (phoneUi.feedbackSubmitBtn) phoneUi.feedbackSubmitBtn.disabled = false;
  }
}

function refreshPhoneBankApp() {
  if (!phoneUi.bankSavings) return;
  if (phoneUi.bankDebt) phoneUi.bankDebt.textContent = formatCurrency(getTotalDebt());
  if (phoneUi.bankInterest) phoneUi.bankInterest.textContent = formatCurrency(loanInterest);
  if (phoneUi.bankCredit) phoneUi.bankCredit.textContent = blackMark ? "Black Mark" : "Clean";
  if (phoneUi.bankLoanAge) phoneUi.bankLoanAge.textContent = `${loanAge} / ${DEFAULT_LIMIT}`;
  if (phoneUi.bankProgressBar) {
    const pct = LOANS_ENABLED && loanPrincipal > 0 ? clampMarket((loanAge / DEFAULT_LIMIT) * 100, 0, 100) : 0;
    phoneUi.bankProgressBar.style.width = `${pct.toFixed(1)}%`;
  }
  phoneUi.bankSavings.textContent = formatCurrency(savingsBalance);
  phoneUi.bankAutoSave.textContent = `${autoSavingsPercent.toFixed(2)}%`;
  if (phoneUi.bankAutoInput && document.activeElement !== phoneUi.bankAutoInput) {
    phoneUi.bankAutoInput.value = autoSavingsPercent > 0 ? autoSavingsPercent.toFixed(1) : "";
  }
  if (phoneUi.bankLoanInput && document.activeElement !== phoneUi.bankLoanInput) {
    phoneUi.bankLoanInput.value = "";
  }
  if (phoneUi.bankTakeLoanBtn) phoneUi.bankTakeLoanBtn.disabled = !LOANS_ENABLED || blackMark;
  if (phoneUi.bankRepayLoanBtn) phoneUi.bankRepayLoanBtn.disabled = !LOANS_ENABLED || getTotalDebt() <= 0 || cash <= 0;
  if (phoneUi.bankWithdrawBtn) phoneUi.bankWithdrawBtn.disabled = savingsBalance <= 0;
  renderPhoneBankHistory();
}

function drawPhonePortfolioChart() {
  if (!phoneUi.portfolioChart) return;
  const chart = phoneUi.portfolioChart;
  const chartCtx = chart.getContext("2d");
  if (!chartCtx) return;

  const dpr = window.devicePixelRatio || 1;
  const width = chart.clientWidth || 320;
  const height = chart.clientHeight || 120;
  chart.width = Math.floor(width * dpr);
  chart.height = Math.floor(height * dpr);
  chartCtx.setTransform(dpr, 0, 0, dpr, 0, 0);
  chartCtx.clearRect(0, 0, width, height);

  chartCtx.fillStyle = "#0f1f2a";
  chartCtx.fillRect(0, 0, width, height);

  if (candles.length < 2) return;
  const closes = candles.map((c) => clampPrice(c.close));
  const min = Math.min(...closes);
  const max = Math.max(...closes);
  const spread = Math.max(0.01, max - min);
  const stepX = (width - 14) / Math.max(1, closes.length - 1);

  chartCtx.strokeStyle = "rgba(126, 168, 196, 0.25)";
  chartCtx.lineWidth = 1;
  for (let i = 1; i <= 3; i += 1) {
    const y = (height / 4) * i;
    chartCtx.beginPath();
    chartCtx.moveTo(6, y);
    chartCtx.lineTo(width - 6, y);
    chartCtx.stroke();
  }

  chartCtx.strokeStyle = "#2ea0ff";
  chartCtx.lineWidth = 2;
  chartCtx.beginPath();
  closes.forEach((value, index) => {
    const x = 7 + index * stepX;
    const y = height - 8 - ((value - min) / spread) * (height - 16);
    if (index === 0) chartCtx.moveTo(x, y);
    else chartCtx.lineTo(x, y);
  });
  chartCtx.stroke();
}

function refreshPhonePortfolioApp() {
  if (!phoneUi.portfolioCash) return;
  const marketPrice = clampPrice(price);
  const net = getNetWorth(marketPrice);
  const pl = net - BASE_NET_WORTH;
  phoneUi.portfolioCash.textContent = formatCurrency(cash);
  phoneUi.portfolioShares.textContent = String(shares);
  phoneUi.portfolioAvg.textContent = shares > 0 ? formatCurrency(avgCost) : "N/A";
  phoneUi.portfolioNet.textContent = formatCurrency(net);
  phoneUi.portfolioPL.textContent = `${pl >= 0 ? "+" : "-"}${formatCurrency(Math.abs(pl))}`;
  phoneUi.portfolioPrice.textContent = formatCurrency(marketPrice);
  drawPhonePortfolioChart();
}

function refreshPhoneCasinoApp() {
  if (!phoneUi.casinoWins) return;
  phoneUi.casinoWins.textContent = String(achievementState.stats.casinoWins || 0);
  phoneUi.casinoProfit.textContent = formatCurrency(phoneState.casinoProfit);
}

function updatePhoneCasinoLocks() {
  const gate = getCasinoGateState();
  const locked = !gate.ok;
  const appKeys = ["casino", ...PHONE_MINI_GAME_APPS.map((item) => item.app)];

  appKeys.forEach((appKey) => {
    document.querySelectorAll(`.phone-home-app[data-target-app="${appKey}"]`).forEach((button) => {
      button.disabled = locked;
      button.classList.toggle("is-locked", locked);
      button.title = locked ? gate.message : "";
    });
  });

  if (phoneUi.tabs && phoneUi.tabs.length > 0) {
    phoneUi.tabs.forEach((tab) => {
      if (tab.dataset.phoneApp !== "casino") return;
      tab.disabled = locked;
      tab.classList.toggle("is-locked", locked);
      tab.title = locked ? gate.message : "";
    });
  }

  if (phoneUi.casinoQuickLaunch) {
    phoneUi.casinoQuickLaunch.querySelectorAll("button[data-casino-quick-game]").forEach((button) => {
      button.disabled = locked;
      button.title = locked ? gate.message : "";
    });
  }
}

function refreshTradingMissionHud() {
  if (!tradingMissionLevelEl) return;
  const levelInfo = getMissionLevelInfo(phoneState.missionXp || 0);
  tradingMissionLevelEl.textContent = String(levelInfo.level);
  if (tradingMissionXpEl) tradingMissionXpEl.textContent = String(Math.floor(phoneState.missionXp || 0));
  if (tradingMissionXpToNextEl) {
    tradingMissionXpToNextEl.textContent = levelInfo.maxed
      ? "MAX"
      : `${Math.max(0, levelInfo.xpToNext - levelInfo.xpInLevel)} XP`;
  }
  if (tradingMissionLevelFillEl) {
    tradingMissionLevelFillEl.style.width = `${levelInfo.progressPct}%`;
  }
  if (tradingMissionPerkEl) {
    tradingMissionPerkEl.textContent = getMissionPerkLabel(levelInfo.level);
  }
}

function saveBankMissionState() {
  try {
    localStorage.setItem(BANK_MISSION_STORAGE_KEY, JSON.stringify(bankMissionState));
  } catch (error) {}
}

function loadBankMissionState() {
  try {
    const raw = localStorage.getItem(BANK_MISSION_STORAGE_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return;

    if (parsed.progress && typeof parsed.progress === "object") {
      bankMissionState.progress = { ...bankMissionState.progress, ...parsed.progress };
    }
    if (parsed.completed && typeof parsed.completed === "object") {
      bankMissionState.completed = { ...bankMissionState.completed, ...parsed.completed };
    }
    if (parsed.claimed && typeof parsed.claimed === "object") {
      bankMissionState.claimed = { ...bankMissionState.claimed, ...parsed.claimed };
    }
    if (parsed.counters && typeof parsed.counters === "object") {
      bankMissionState.counters = {
        ...bankMissionState.counters,
        ...parsed.counters
      };
    }

    BANK_MISSION_DEFS.forEach((mission) => {
      bankMissionState.progress[mission.id] = clampMarket(Number(bankMissionState.progress[mission.id]) || 0, 0, mission.goal);
    });

    const defaults = createDefaultBankMissionState().counters;
    Object.keys(defaults).forEach((key) => {
      const defaultValue = defaults[key];
      if (typeof defaultValue === "number") {
        const next = Number(bankMissionState.counters[key]);
        bankMissionState.counters[key] = Number.isFinite(next) ? next : defaultValue;
      } else if (typeof defaultValue === "boolean") {
        bankMissionState.counters[key] = Boolean(bankMissionState.counters[key]);
      } else if (defaultValue && typeof defaultValue === "object") {
        const currentObject = bankMissionState.counters[key];
        bankMissionState.counters[key] =
          currentObject && typeof currentObject === "object"
            ? { ...defaultValue, ...currentObject }
            : { ...defaultValue };
      } else {
        bankMissionState.counters[key] = bankMissionState.counters[key] ?? defaultValue;
      }
    });
  } catch (error) {}
}

function getBankMissionProgress(missionId) {
  const mission = BANK_MISSION_DEF_MAP.get(missionId);
  if (!mission) return 0;
  const raw = Number(bankMissionState.progress[missionId]);
  if (!Number.isFinite(raw)) return 0;
  return clampMarket(raw, 0, mission.goal);
}

function isBankMissionCompleted(missionId) {
  const mission = BANK_MISSION_DEF_MAP.get(missionId);
  if (!mission) return false;
  return Boolean(bankMissionState.completed[missionId]) || getBankMissionProgress(missionId) >= mission.goal;
}

function isBankMissionClaimed(missionId) {
  return Boolean(bankMissionState.claimed[missionId]);
}

function setBankMissionProgress(missionId, value, { allowDecrease = false } = {}) {
  const mission = BANK_MISSION_DEF_MAP.get(missionId);
  if (!mission) return false;
  const current = getBankMissionProgress(missionId);
  const normalized = clampMarket(Number(value) || 0, 0, mission.goal);
  const next = allowDecrease ? normalized : Math.max(current, normalized);
  if (Math.abs(next - current) < 0.0001) return false;
  bankMissionState.progress[missionId] = next;
  if (next >= mission.goal && !bankMissionState.completed[missionId]) {
    bankMissionState.completed[missionId] = Date.now();
    pushPhoneNotification("achievement", `Bank mission complete: ${mission.name}`);
  }
  return true;
}

function completeBankMission(missionId) {
  const mission = BANK_MISSION_DEF_MAP.get(missionId);
  if (!mission) return false;
  return setBankMissionProgress(missionId, mission.goal);
}

function getBankClaimedCount({ exclude = [] } = {}) {
  const excluded = new Set(exclude);
  return BANK_MISSION_DEFS.reduce((count, mission) => {
    if (excluded.has(mission.id)) return count;
    if (isBankMissionClaimed(mission.id)) return count + 1;
    return count;
  }, 0);
}

function syncBankMissionDerivedProgress() {
  const counters = bankMissionState.counters;
  let dirty = false;

  BANK_CASINO_GAME_DEFS.forEach((gameDef) => {
    const missionIds = BANK_MISSION_GAME_TIERS.get(gameDef.key) || [];
    const wins = Math.max(0, Math.floor(Number(counters.casinoWinsByGame?.[gameDef.key]) || 0));
    missionIds.forEach((missionId) => {
      dirty = setBankMissionProgress(missionId, wins) || dirty;
    });
  });

  const claimedForBanker = getBankClaimedCount({ exclude: [BANK_MISSION_BANKER_ID, BANK_MISSION_LEGEND_ID] });
  dirty = setBankMissionProgress(BANK_MISSION_BANKER_ID, claimedForBanker) || dirty;

  const legendMission = BANK_MISSION_DEF_MAP.get(BANK_MISSION_LEGEND_ID);
  if (legendMission) {
    const claimedForLegend = getBankClaimedCount({ exclude: [BANK_MISSION_LEGEND_ID] });
    const legendProgress = isBankMissionClaimed(BANK_MISSION_LEGEND_ID) || isBankMissionCompleted(BANK_MISSION_LEGEND_ID)
      ? legendMission.goal
      : Math.min(legendMission.goal, claimedForLegend);
    dirty = setBankMissionProgress(BANK_MISSION_LEGEND_ID, legendProgress, { allowDecrease: true }) || dirty;
    if (claimedForLegend >= legendMission.goal) {
      dirty = completeBankMission(BANK_MISSION_LEGEND_ID) || dirty;
    }
  }

  return dirty;
}

function showPhoneMissionToast(message) {
  const missionsRoot = document.getElementById("phoneAppMissions");
  if (!missionsRoot || !message) return;

  let toast = phoneUi.missionToast;
  if (!toast || !missionsRoot.contains(toast)) {
    toast = document.createElement("div");
    toast.className = "phone-mission-toast";
    missionsRoot.appendChild(toast);
    phoneUi.missionToast = toast;
  }

  if (phoneMissionToastTimer) {
    clearTimeout(phoneMissionToastTimer);
    phoneMissionToastTimer = null;
  }

  toast.textContent = String(message);
  toast.classList.remove("show");
  void toast.offsetWidth;
  toast.classList.add("show");
  phoneMissionToastTimer = setTimeout(() => {
    toast.classList.remove("show");
    phoneMissionToastTimer = null;
  }, 1400);
}

function getMissionXpRequirementForLevel(level) {
  const safeLevel = Math.max(1, Math.floor(level));
  return Math.floor(80 + (safeLevel - 1) * 35);
}

function getMissionLevelInfo(xpInput = phoneState.missionXp || 0) {
  let xp = Math.max(0, Math.floor(Number(xpInput) || 0));
  let level = 1;
  let req = getMissionXpRequirementForLevel(level);

  while (level < MISSION_MAX_LEVEL && xp >= req) {
    xp -= req;
    level += 1;
    req = getMissionXpRequirementForLevel(level);
  }

  const maxed = level >= MISSION_MAX_LEVEL;
  return {
    level,
    xpInLevel: xp,
    xpToNext: maxed ? 0 : req,
    progressPct: maxed ? 100 : clampMarket((xp / Math.max(1, req)) * 100, 0, 100),
    maxed
  };
}

function getMissionCashPerkPercent(level) {
  if (level >= 20) return 0.12;
  if (level >= 10) return 0.07;
  if (level >= 5) return 0.03;
  return 0;
}

function getMissionPerkLabel(level) {
  const pct = Math.round(getMissionCashPerkPercent(level) * 100);
  if (pct <= 0) return "Perk: None";
  return `Perk: +${pct}% mission cash rewards`;
}

function processMissionLevelMilestones(prevLevel, nextLevel) {
  if (nextLevel <= prevLevel) return;
  if (!phoneState.missionLevelMilestonesClaimed || typeof phoneState.missionLevelMilestonesClaimed !== "object") {
    phoneState.missionLevelMilestonesClaimed = {};
  }

  for (let level = prevLevel + 1; level <= nextLevel; level += 1) {
    const reward = MISSION_LEVEL_MILESTONES[level];
    if (!reward) continue;
    if (phoneState.missionLevelMilestonesClaimed[level]) continue;

    const cashReward = roundCurrency(Number(reward.cash) || 0);
    const tokenReward = Math.max(0, Math.floor(Number(reward.tokens) || 0));
    if (cashReward > 0) cash = roundCurrency(cash + cashReward);
    if (tokenReward > 0) {
      phoneState.missionTokens = Math.max(0, Math.floor(phoneState.missionTokens + tokenReward));
    }
    phoneState.missionLevelMilestonesClaimed[level] = true;

    const tokenText = tokenReward > 0 ? ` + ${tokenReward} token${tokenReward === 1 ? "" : "s"}` : "";
    showPhoneMissionToast(`Level ${level} (${reward.label}) bonus: +${formatCurrency(cashReward)}${tokenText}`);
    pushPhoneNotification("achievement", `Level ${level} reached: ${reward.label}`);
  }
}

function grantMissionReward(reward, rewardKey = "") {
  const beforeLevel = getMissionLevelInfo(phoneState.missionXp || 0).level;
  const cashReward = getScaledAchievementCashReward(Number(reward?.cash) || 0, rewardKey);
  const xpReward = Math.max(0, Math.floor(Number(reward?.xp) || 0));
  const tokenReward = Math.max(0, Math.floor(Number(reward?.tokens) || 0));
  const perkPercent = getMissionCashPerkPercent(beforeLevel);
  const bonusCash = roundCurrency(cashReward * perkPercent);
  const totalCash = roundCurrency(cashReward + bonusCash);

  if (totalCash > 0) {
    cash = roundCurrency(cash + totalCash);
  }
  if (xpReward > 0) {
    phoneState.missionXp = Math.max(0, Math.floor(phoneState.missionXp + xpReward));
  }
  if (tokenReward > 0) {
    phoneState.missionTokens = Math.max(0, Math.floor(phoneState.missionTokens + tokenReward));
  }

  const afterLevel = getMissionLevelInfo(phoneState.missionXp || 0).level;
  processMissionLevelMilestones(beforeLevel, afterLevel);

  return {
    baseCash: cashReward,
    bonusCash,
    cashTotal: totalCash,
    xp: xpReward,
    tokens: tokenReward,
    levelBefore: beforeLevel,
    levelAfter: afterLevel
  };
}

function getPhoneMissionDefs() {
  return {
    daily: [
      {
        id: "daily-trader",
        title: "Make 3 trades",
        desc: "Complete at least 3 buy/sell actions.",
        reward: { cash: 50, xp: 0, tokens: 0 },
        check: () => achievementState.stats.buys + achievementState.stats.sells >= 3
      },
      {
        id: "daily-news",
        title: "Read 3 alerts",
        desc: "Trigger 3 market news events.",
        reward: { cash: 40, xp: 0, tokens: 0 },
        check: () => achievementState.stats.newsSeen >= 3
      },
      {
        id: "daily-casino-win",
        title: "Get one casino win",
        desc: "Win in any casino game once.",
        reward: { cash: 60, xp: 0, tokens: 0 },
        check: () => achievementState.stats.casinoWins >= 1
      }
    ],
    weekly: [
      {
        id: "weekly-2k",
        title: "Reach $2,000 net worth",
        desc: "Grow your account value above $2,000.",
        reward: { cash: 250, xp: 0, tokens: 0 },
        check: () => getNetWorth() >= 2000
      },
      {
        id: "weekly-debtfree",
        title: "Repay all debt",
        desc: "Clear loan principal and interest.",
        reward: { cash: 180, xp: 0, tokens: 0 },
        check: () => getTotalDebt() <= 0 && achievementState.stats.loansTaken >= 1
      },
      {
        id: "weekly-market",
        title: "Take 100 steps",
        desc: "Advance the market 100 steps.",
        reward: { cash: 220, xp: 0, tokens: 0 },
        check: () => achievementState.stats.steps >= 100
      }
    ]
  };
}

function claimPhoneMission(missionId, reward) {
  if (phoneState.missionsClaimed[missionId]) return;
  phoneState.missionsClaimed[missionId] = true;
  const granted = grantMissionReward(reward || {}, missionId);
  updateUI();
  updateLoanUI();
  const bonusText = granted.bonusCash > 0 ? ` (+${formatCurrency(granted.bonusCash)} perk bonus)` : "";
  pushPhoneNotification("achievement", `Mission claimed: +${formatCurrency(granted.cashTotal)}${bonusText}`);
  renderPhoneMissions();
  savePhoneState();
}

function claimBankMission(missionId) {
  const mission = BANK_MISSION_DEF_MAP.get(missionId);
  if (!mission) return false;
  if (!isBankMissionCompleted(missionId)) return false;
  if (isBankMissionClaimed(missionId)) return false;

  bankMissionState.claimed[missionId] = Date.now();
  const granted = grantMissionReward(mission.reward, missionId);
  syncBankMissionDerivedProgress();
  saveBankMissionState();
  savePhoneState();

  const tokenText = granted.tokens ? ` + ${granted.tokens} token${granted.tokens === 1 ? "" : "s"}` : "";
  const bonusText = granted.bonusCash > 0 ? ` (+${formatCurrency(granted.bonusCash)} bonus)` : "";
  showPhoneMissionToast(`Claimed ${mission.name}: +${formatCurrency(granted.cashTotal)} +${granted.xp} XP${tokenText}${bonusText}`);
  pushPhoneNotification("achievement", `Bank reward claimed: ${mission.name}`);

  updateUI();
  updateLoanUI();
  renderPhoneMissions();
  return true;
}

function claimAllBankMissions() {
  let claimedAny = false;
  BANK_MISSION_DEFS.forEach((mission) => {
    if (!isBankMissionCompleted(mission.id) || isBankMissionClaimed(mission.id)) return;
    claimedAny = claimBankMission(mission.id) || claimedAny;
  });

  if (!claimedAny) {
    showPhoneMissionToast("No bank rewards to claim.");
  }
}

function renderPhoneMissionGroup(container, missions) {
  if (!container) return;
  container.innerHTML = "";
  missions.forEach((mission) => {
    const done = Boolean(mission.check());
    const claimed = Boolean(phoneState.missionsClaimed[mission.id]);
    const card = document.createElement("div");
    card.className = `phone-mission${done ? " done" : ""}${claimed ? " claimed" : ""}`;
    const rewardCash = getScaledAchievementCashReward(Number(mission.reward?.cash) || 0, mission.id);
    card.innerHTML = `
      <h4>${mission.title}</h4>
      <p>${mission.desc}</p>
      <div class="row">
        <span>Reward: ${formatCurrency(rewardCash)}</span>
        <button type="button" ${!done || claimed ? "disabled" : ""}>${claimed ? "Claimed" : "Claim"}</button>
      </div>
    `;
    const button = card.querySelector("button");
    if (button && done && !claimed) {
      button.addEventListener("click", () => claimPhoneMission(mission.id, mission.reward));
    }
    container.appendChild(card);
  });
}

function getBankMissionStatus(mission) {
  const progress = getBankMissionProgress(mission.id);
  const completed = isBankMissionCompleted(mission.id);
  const claimed = isBankMissionClaimed(mission.id);
  if (claimed) return "claimed";
  if (completed) return "claimable";
  if (progress > 0) return "progress";
  return "locked";
}

function getBankMissionStatusLabel(status) {
  if (status === "claimable") return "Claimable";
  if (status === "claimed") return "Claimed";
  if (status === "progress") return "In Progress";
  return "Locked";
}

function getMissionRewardText(reward, rewardKey = "") {
  const chunks = [];
  const cashReward = getScaledAchievementCashReward(Number(reward?.cash) || 0, rewardKey);
  const xpReward = Number(reward?.xp) || 0;
  const tokenReward = Number(reward?.tokens) || 0;

  if (cashReward > 0) chunks.push(formatCurrency(cashReward));
  if (xpReward > 0) chunks.push(`${Math.floor(xpReward)} XP`);
  if (tokenReward > 0) chunks.push(`${Math.floor(tokenReward)} token${Math.floor(tokenReward) === 1 ? "" : "s"}`);
  return chunks.join(" • ");
}

function renderPhoneBankMissions() {
  if (!phoneUi.bankMissions) return;
  if (syncBankMissionDerivedProgress()) {
    saveBankMissionState();
  }

  const getStatusSortRank = (status) => {
    if (status === "claimable") return 0;
    if (status === "progress") return 1;
    if (status === "locked") return 2;
    return 3;
  };
  const getSingleWinEasyRank = (mission) => {
    if (!mission || Number(mission.goal) !== 1) return Number.POSITIVE_INFINITY;
    const gameKey = String(mission.casinoGameKey || "");
    if (!BANK_CASINO_GAME_EASY_ORDER_INDEX.has(gameKey)) return Number.POSITIVE_INFINITY;
    return BANK_CASINO_GAME_EASY_ORDER_INDEX.get(gameKey);
  };

  const cards = BANK_MISSION_DEFS.map((mission, index) => {
    const progress = getBankMissionProgress(mission.id);
    const status = getBankMissionStatus(mission);
    const ratio = mission.goal > 0 ? progress / mission.goal : 0;
    return {
      mission,
      progress,
      status,
      ratio,
      statusRank: getStatusSortRank(status),
      singleWinEasyRank: getSingleWinEasyRank(mission),
      index
    };
  }).sort((a, b) => {
    if (a.statusRank !== b.statusRank) return a.statusRank - b.statusRank;
    if (Math.abs(a.ratio - b.ratio) > 0.0001) return b.ratio - a.ratio;
    if (a.singleWinEasyRank !== b.singleWinEasyRank) return a.singleWinEasyRank - b.singleWinEasyRank;
    if (a.mission.goal !== b.mission.goal) return a.mission.goal - b.mission.goal;
    return a.index - b.index;
  });

  phoneUi.bankMissions.innerHTML = "";
  const fragment = document.createDocumentFragment();
  let claimableCount = 0;

  cards.forEach(({ mission, progress, status }) => {
    if (status === "claimable") claimableCount += 1;
    const card = document.createElement("div");
    card.className = `phone-mission bank ${status === "claimable" ? "claimable" : ""}${status === "claimed" ? " claimed" : ""}`;

    const progressValue = progress;
    const progressPct = mission.goal > 0 ? clampMarket((progressValue / mission.goal) * 100, 0, 100) : 0;
    const showProgress = mission.goal > 1 || status === "progress";

    card.innerHTML = `
      <div class="row">
        <h4>${mission.name}</h4>
        <span class="status-chip ${status}">${getBankMissionStatusLabel(status)}</span>
      </div>
      <p>${mission.description}</p>
      ${showProgress ? `
        <div class="progress-track"><div class="progress-fill" style="width:${progressPct}%"></div></div>
        <div class="progress-label">${Math.floor(progressValue)} / ${mission.goal}</div>
      ` : ""}
      <div class="reward-line">Reward: ${getMissionRewardText(mission.reward, mission.id)}</div>
      <div class="row">
        <button type="button" ${status !== "claimable" ? "disabled" : ""}>${status === "claimed" ? "Claimed" : "Claim"}</button>
      </div>
    `;

    const button = card.querySelector("button");
    if (button && status === "claimable") {
      button.addEventListener("click", () => claimBankMission(mission.id));
    }
    fragment.appendChild(card);
  });

  phoneUi.bankMissions.appendChild(fragment);

  if (phoneUi.bankClaimedSummary) {
    const claimed = getBankClaimedCount();
    phoneUi.bankClaimedSummary.textContent = `${claimed} / ${BANK_MISSION_DEFS.length} Claimed`;
  }
  const levelInfo = getMissionLevelInfo(phoneState.missionXp || 0);
  if (phoneUi.missionXp) phoneUi.missionXp.textContent = String(Math.floor(phoneState.missionXp || 0));
  if (phoneUi.missionTokens) phoneUi.missionTokens.textContent = String(Math.floor(phoneState.missionTokens || 0));
  if (phoneUi.missionLevel) phoneUi.missionLevel.textContent = String(levelInfo.level);
  if (phoneUi.missionXpToNext) {
    phoneUi.missionXpToNext.textContent = levelInfo.maxed
      ? "MAX"
      : `${Math.max(0, levelInfo.xpToNext - levelInfo.xpInLevel)} XP`;
  }
  if (phoneUi.missionLevelFill) {
    phoneUi.missionLevelFill.style.width = `${levelInfo.progressPct}%`;
  }
  if (phoneUi.missionPerk) {
    phoneUi.missionPerk.textContent = getMissionPerkLabel(levelInfo.level);
  }
  if (phoneUi.bankClaimAllBtn) phoneUi.bankClaimAllBtn.disabled = claimableCount <= 0;
}

function getMissionMilestoneRewardText(level) {
  const milestone = MISSION_LEVEL_MILESTONES[level];
  if (!milestone) return "Milestone bonus: —";
  const tokens = Math.max(0, Math.floor(Number(milestone.tokens) || 0));
  const tokenText = tokens > 0 ? ` + ${tokens} token${tokens === 1 ? "" : "s"}` : "";
  return `Milestone bonus: ${formatCurrency(Number(milestone.cash) || 0)}${tokenText}`;
}

function renderPhoneMissionPerks() {
  if (!phoneUi.missionPerks) return;
  const levelInfo = getMissionLevelInfo(phoneState.missionXp || 0);

  phoneUi.missionPerks.innerHTML = "";
  const fragment = document.createDocumentFragment();
  for (let level = 1; level <= MISSION_MAX_LEVEL; level += 1) {
    const unlocked = levelInfo.level >= level;
    const bonusPct = Math.round(getMissionCashPerkPercent(level) * 100);
    const milestone = MISSION_LEVEL_MILESTONES[level] || null;
    const milestoneClaimed = milestone ? Boolean(phoneState.missionLevelMilestonesClaimed?.[level]) : false;

    const card = document.createElement("div");
    card.className = `phone-perk-item${unlocked ? " unlocked" : ""}`;
    card.innerHTML = `
      <div class="perk-head">
        <span>Level ${level}</span>
        <span class="status-chip ${unlocked ? "claimable" : "locked"}">${unlocked ? "Unlocked" : "Locked"}</span>
      </div>
      <div class="perk-line">Mission cash bonus: +${bonusPct}%</div>
      <div class="perk-line">${getMissionMilestoneRewardText(level)}${milestone && milestoneClaimed ? " (Collected)" : ""}</div>
    `;
    fragment.appendChild(card);
  }
  phoneUi.missionPerks.appendChild(fragment);
}

function setPhoneMissionTab(tabName) {
  if (!tabName || !phoneUi.missionTabs || !phoneUi.missionPanels) return;
  const normalized = ["daily", "weekly", "bank", "perks"].includes(tabName) ? tabName : "daily";
  phoneMissionCurrentTab = normalized;

  phoneUi.missionTabs.forEach((tab) => {
    tab.classList.toggle("active", tab.dataset.missionTab === normalized);
  });
  phoneUi.missionPanels.forEach((panel) => {
    panel.classList.toggle("active", panel.dataset.missionPanel === normalized);
  });
}

function renderPhoneMissions() {
  if (!phoneUi.dailyMissions || !phoneUi.weeklyMissions) return;
  const defs = getPhoneMissionDefs();
  renderPhoneMissionGroup(phoneUi.dailyMissions, defs.daily);
  renderPhoneMissionGroup(phoneUi.weeklyMissions, defs.weekly);
  renderPhoneBankMissions();
  renderPhoneMissionPerks();
  setPhoneMissionTab(phoneMissionCurrentTab);
  updatePhoneHomeAppBadges();
}

function trackBankMissionCashDelta(cashDelta) {
  if (!Number.isFinite(cashDelta) || Math.abs(cashDelta) < 0.009) return;
  if (!casinoSection || casinoSection.style.display === "none") return;

  const counters = bankMissionState.counters;
  if (cashDelta < 0) {
    counters.pendingCasinoLoss = true;
    counters.casinoBetSinceLastStep = true;
  } else if (cashDelta > 0) {
    counters.pendingCasinoLoss = false;
  }
}

function trackBankMissionLoanBlocked() {
  const counters = bankMissionState.counters;
  counters.blockedLoanAttempts += 1;
  if (getTotalDebt() > 0) {
    counters.sessionNearCap = true;
  }
  completeBankMission("bank-22");
  syncBankMissionDerivedProgress();
  saveBankMissionState();
}

function trackBankMissionLoanTaken(amount) {
  const counters = bankMissionState.counters;
  counters.bankActionIndex += 1;
  counters.lastBankAction = "take-loan";
  counters.lastLoanActionIndex = counters.bankActionIndex;
  counters.loansTakenLifetime += 1;
  counters.noLoanStepStreak = 0;
  counters.pendingCasinoLoss = false;

  if (counters.backToDebtArmed) {
    completeBankMission("bank-17");
    counters.backToDebtArmed = false;
  }

  if (!counters.sessionActive) {
    counters.sessionActive = true;
    counters.sessionLoanStep = achievementState.stats.steps;
    counters.sessionDebtSteps = 0;
    counters.sessionTradeClosed = false;
    counters.sessionTradingProfit = 0;
    counters.sessionCasinoWin = false;
    counters.sessionBrokeBorrowing = false;
    counters.sessionHighCashStreak = 0;
    counters.sessionNoCasinoBetStreak = 0;
    counters.sessionNearCap = false;
    counters.sessionEmergencyFund = false;
    counters.casinoBetSinceLastStep = false;
  }

  if (amount >= 250) completeBankMission("bank-5");
  if (amount >= 1000) completeBankMission("bank-6");
  if (amount >= 5000) completeBankMission("bank-7");
  if (amount <= 100) completeBankMission("bank-8");

  syncBankMissionDerivedProgress();
  saveBankMissionState();
}

function trackBankMissionTradeClosed(realizedProfit) {
  const counters = bankMissionState.counters;
  if (!counters.sessionActive || getTotalDebt() <= 0) return;
  counters.sessionTradeClosed = true;
  counters.sessionTradingProfit = roundCurrency(counters.sessionTradingProfit + (Number(realizedProfit) || 0));
  completeBankMission("bank-24");
  if (counters.sessionTradingProfit >= 250) {
    completeBankMission("bank-25");
  }
  saveBankMissionState();
}

function normalizeBankCasinoGameKey(rawKey) {
  const normalized = String(rawKey || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
  const aliases = {
    blackjack: "blackjack",
    slots: "slots",
    dragontower: "dragontower",
    dragon: "dragontower",
    horseracing: "horseracing",
    horse: "horseracing",
    dice: "dice",
    slide: "slide",
    crash: "crash",
    mines: "mines",
    crossyroad: "crossyroad",
    crossy: "crossyroad",
    roulette: "roulette",
    diamonds: "diamonds",
    plinko: "plinko",
    hilo: "hilo",
    keno: "keno"
  };
  const canonical = aliases[normalized];
  return canonical && BANK_MISSION_GAME_TIERS.has(canonical) ? canonical : null;
}

function trackBankMissionCasinoWin(amount, sourceKey = activeCasinoGameKey) {
  const counters = bankMissionState.counters;
  if (!(Number(amount) > 0)) return;

  const normalizedGameKey = normalizeBankCasinoGameKey(sourceKey);
  if (normalizedGameKey) {
    const currentWins = Math.max(0, Math.floor(Number(counters.casinoWinsByGame?.[normalizedGameKey]) || 0));
    counters.casinoWinsByGame[normalizedGameKey] = currentWins + 1;
  }

  if (counters.sessionActive && getTotalDebt() > 0 && Number(amount) > 0) {
    counters.sessionCasinoWin = true;
  }
  counters.pendingCasinoLoss = false;
  syncBankMissionDerivedProgress();
  saveBankMissionState();
}

function trackBankMissionLoanRepaid(interestPaid, fullyCleared) {
  const counters = bankMissionState.counters;
  counters.bankActionIndex += 1;
  counters.lastBankAction = "repay-loan";
  counters.interestPaidTotal = roundCurrency(counters.interestPaidTotal + Math.max(0, Number(interestPaid) || 0));

  if (!fullyCleared) {
    syncBankMissionDerivedProgress();
    saveBankMissionState();
    return;
  }

  counters.debtClears += 1;
  counters.backToDebtArmed = true;

  const sessionSteps = Math.max(0, Number(counters.sessionDebtSteps) || 0);
  if (sessionSteps <= 5) completeBankMission("bank-9");
  if (sessionSteps <= 10) completeBankMission("bank-10");
  if (sessionSteps >= 100) completeBankMission("bank-11");
  if (sessionSteps >= 300) completeBankMission("bank-12");

  if (cash < 50) completeBankMission("bank-20");
  if (counters.sessionNearCap) completeBankMission("bank-23");
  if (getNetWorth() < BASE_NET_WORTH) completeBankMission("bank-28");
  if (counters.pendingCasinoLoss) completeBankMission("bank-35");

  if (
    Number.isFinite(counters.lastLoanActionIndex) &&
    counters.bankActionIndex === counters.lastLoanActionIndex + 1
  ) {
    completeBankMission("bank-36");
  }

  counters.pendingCasinoLoss = false;
  counters.sessionActive = false;
  counters.sessionLoanStep = null;
  counters.sessionDebtSteps = 0;
  counters.sessionTradeClosed = false;
  counters.sessionTradingProfit = 0;
  counters.sessionCasinoWin = false;
  counters.sessionBrokeBorrowing = false;
  counters.sessionHighCashStreak = 0;
  counters.sessionNoCasinoBetStreak = 0;
  counters.sessionNearCap = false;
  counters.sessionEmergencyFund = false;
  counters.casinoBetSinceLastStep = false;

  syncBankMissionDerivedProgress();
  saveBankMissionState();
}

function trackBankMissionMarketStep() {
  const counters = bankMissionState.counters;
  const debt = getTotalDebt();

  counters.noLoanStepStreak += 1;

  if (debt > 0) {
    counters.debtStepsTotal += 1;
    counters.debtFreeStepStreak = 0;
    counters.sessionActive = true;
    counters.sessionDebtSteps += 1;
    if (cash < 100) {
      counters.sessionBrokeBorrowing = true;
      completeBankMission("bank-21");
    }
    if (cash >= 1000) counters.sessionHighCashStreak += 1;
    else counters.sessionHighCashStreak = 0;
    if (counters.sessionHighCashStreak >= 100) completeBankMission("bank-32");

    if (counters.casinoBetSinceLastStep) counters.sessionNoCasinoBetStreak = 0;
    else counters.sessionNoCasinoBetStreak += 1;
    counters.casinoBetSinceLastStep = false;
    if (counters.sessionNoCasinoBetStreak >= 100) completeBankMission("bank-33");

    if (cash >= 2000) {
      counters.sessionEmergencyFund = true;
      completeBankMission("bank-34");
    }
    if (loanAge >= DEFAULT_LIMIT * 0.95 || blackMark) {
      counters.sessionNearCap = true;
    }
  } else {
    counters.debtFreeStepStreak += 1;
    counters.sessionNoCasinoBetStreak = 0;
    counters.casinoBetSinceLastStep = false;
  }

  syncBankMissionDerivedProgress();
  saveBankMissionState();
}

function trackBankMissionBankAppOpened() {
  bankMissionState.counters.bankAppOpens += 1;
  syncBankMissionDerivedProgress();
  saveBankMissionState();
}

function trackBankMissionDebtSnapshot() {
  const counters = bankMissionState.counters;
  if (getTotalDebt() <= 0) return;
  let dirty = false;
  if (cash < 100) {
    counters.sessionBrokeBorrowing = true;
    dirty = completeBankMission("bank-21") || dirty;
  }
  if (cash >= 2000) {
    counters.sessionEmergencyFund = true;
    dirty = completeBankMission("bank-34") || dirty;
  }
  if (dirty) {
    saveBankMissionState();
  }
}

function applyPhoneSettings() {
  document.body.classList.toggle("reduced-anim", !phoneState.settings.animations);
  const scale = clampMarket(phoneState.settings.uiScale / 100, 0.85, 1.15);
  document.documentElement.style.setProperty("--ui-scale", scale.toFixed(3));

  if (phoneUi.settingAnimations) phoneUi.settingAnimations.checked = phoneState.settings.animations;
  if (phoneUi.settingPopupSeconds) phoneUi.settingPopupSeconds.value = phoneState.settings.popupSeconds.toFixed(1);
  if (phoneUi.settingAutoLimit) phoneUi.settingAutoLimit.value = String(phoneState.settings.autoRoundLimit);
  if (phoneUi.settingUiScale) phoneUi.settingUiScale.value = String(Math.round(phoneState.settings.uiScale));
  if (phoneUi.settingUiScaleLabel) phoneUi.settingUiScaleLabel.textContent = `${Math.round(phoneState.settings.uiScale)}%`;
}

function getCasinoPopupDurationMs(isBigWin = false) {
  const base = Math.round(clampMarket(phoneState.settings.popupSeconds, 1, 8) * 1000);
  return isBigWin ? Math.round(base * 1.35) : base;
}

function getAutoRoundLimit() {
  return Math.max(0, Math.floor(phoneState.settings.autoRoundLimit || 0));
}

function resetAutoRoundCounter(gameKey) {
  if (!gameKey) return;
  phoneState.autoRoundCounters[gameKey] = 0;
}

function consumeAutoRoundCounter(gameKey) {
  const limit = getAutoRoundLimit();
  if (limit <= 0 || !gameKey) return true;
  const next = (phoneState.autoRoundCounters[gameKey] || 0) + 1;
  phoneState.autoRoundCounters[gameKey] = next;
  return next <= limit;
}

function refreshPhonePanels() {
  refreshPhoneBankApp();
  refreshPhonePortfolioApp();
  refreshPhoneCasinoApp();
  updatePhoneCasinoLocks();
  renderPhoneMissions();
}

function initPhone() {
  phoneUi.openBtn = document.getElementById("phoneBtn");
  phoneUi.badge = document.getElementById("phoneBadge");
  phoneUi.overlay = document.getElementById("phoneOverlay");
  phoneUi.shell = phoneUi.overlay ? phoneUi.overlay.querySelector(".phone-shell") : null;
  phoneUi.body = phoneUi.overlay ? phoneUi.overlay.querySelector(".phone-body") : null;
  buildPhoneMiniGameApps();
  phoneUi.statusTime = document.getElementById("phoneStatusTime");
  phoneUi.closeBtn = document.getElementById("phoneCloseBtn");
  phoneUi.homeBtn = document.getElementById("phoneHomeBtn");
  phoneUi.tabs = Array.from(document.querySelectorAll(".phone-tab"));
  phoneUi.apps = Array.from(document.querySelectorAll(".phone-app"));
  phoneUi.homeApps = Array.from(document.querySelectorAll(".phone-home-app"));
  phoneUi.clearMessagesBtn = document.getElementById("phoneClearMessagesBtn");
  phoneUi.messageList = document.getElementById("phoneMessageList");
  phoneUi.bankDebt = document.getElementById("phoneBankDebt");
  phoneUi.bankInterest = document.getElementById("phoneBankInterest");
  phoneUi.bankCredit = document.getElementById("phoneBankCredit");
  phoneUi.bankLoanAge = document.getElementById("phoneBankLoanAge");
  phoneUi.bankProgressBar = document.getElementById("phoneBankProgressBar");
  phoneUi.bankSavings = document.getElementById("phoneBankSavings");
  phoneUi.bankAutoSave = document.getElementById("phoneBankAutoSave");
  phoneUi.bankLoanInput = document.getElementById("phoneBankLoanInput");
  phoneUi.bankTakeLoanBtn = document.getElementById("phoneBankTakeLoanBtn");
  phoneUi.bankRepayLoanBtn = document.getElementById("phoneBankRepayLoanBtn");
  phoneUi.bankAutoInput = document.getElementById("phoneBankAutoPercentInput");
  phoneUi.bankSetAutoBtn = document.getElementById("phoneBankSetAutoBtn");
  phoneUi.bankAutoOffBtn = document.getElementById("phoneBankAutoOffBtn");
  phoneUi.bankWithdrawBtn = document.getElementById("phoneBankWithdrawBtn");
  phoneUi.bankHistory = document.getElementById("phoneBankHistory");
  phoneUi.portfolioCash = document.getElementById("phonePortfolioCash");
  phoneUi.portfolioShares = document.getElementById("phonePortfolioShares");
  phoneUi.portfolioAvg = document.getElementById("phonePortfolioAvg");
  phoneUi.portfolioNet = document.getElementById("phonePortfolioNet");
  phoneUi.portfolioPL = document.getElementById("phonePortfolioPL");
  phoneUi.portfolioPrice = document.getElementById("phonePortfolioPrice");
  phoneUi.portfolioChart = document.getElementById("phonePortfolioChart");
  phoneUi.casinoWins = document.getElementById("phoneCasinoWins");
  phoneUi.casinoProfit = document.getElementById("phoneCasinoProfit");
  phoneUi.casinoQuickLaunch = document.getElementById("phoneCasinoQuickLaunch");
  phoneUi.dailyMissions = document.getElementById("phoneDailyMissions");
  phoneUi.weeklyMissions = document.getElementById("phoneWeeklyMissions");
  phoneUi.bankMissions = document.getElementById("phoneBankMissions");
  phoneUi.missionPerks = document.getElementById("phoneMissionPerks");
  phoneUi.bankClaimAllBtn = document.getElementById("phoneBankClaimAllBtn");
  phoneUi.bankClaimedSummary = document.getElementById("phoneBankClaimedSummary");
  phoneUi.missionXp = document.getElementById("phoneMissionXp");
  phoneUi.missionTokens = document.getElementById("phoneMissionTokens");
  phoneUi.missionLevel = document.getElementById("phoneMissionLevel");
  phoneUi.missionXpToNext = document.getElementById("phoneMissionXpToNext");
  phoneUi.missionLevelFill = document.getElementById("phoneMissionLevelFill");
  phoneUi.missionPerk = document.getElementById("phoneMissionPerk");
  phoneUi.missionTabs = Array.from(document.querySelectorAll("#phoneAppMissions .phone-mission-tab"));
  phoneUi.missionPanels = Array.from(document.querySelectorAll("#phoneAppMissions .phone-mission-panel"));
  phoneUi.settingAnimations = document.getElementById("phoneSettingAnimations");
  phoneUi.settingPopupSeconds = document.getElementById("phoneSettingPopupSeconds");
  phoneUi.settingAutoLimit = document.getElementById("phoneSettingAutoLimit");
  phoneUi.settingUiScale = document.getElementById("phoneSettingUiScale");
  phoneUi.settingUiScaleLabel = document.getElementById("phoneUiScaleLabel");
  phoneUi.totalResetBtn = document.getElementById("phoneTotalResetBtn");
  phoneUi.feedbackCategory = document.getElementById("phoneFeedbackCategory");
  phoneUi.feedbackInput = document.getElementById("phoneFeedbackInput");
  phoneUi.feedbackSubmitBtn = document.getElementById("phoneFeedbackSubmitBtn");
  phoneUi.feedbackStatus = document.getElementById("phoneFeedbackStatus");
  phoneUi.feedbackList = document.getElementById("phoneFeedbackList");

  if (!phoneUi.openBtn || !phoneUi.overlay) return;

  loadPhoneState();
  loadBankMissionState();
  if (syncBankMissionDerivedProgress()) {
    saveBankMissionState();
  }
  applyPhoneSettings();
  refreshPhoneStatusTime();
  if (phoneStatusTimer) clearInterval(phoneStatusTimer);
  phoneStatusTimer = setInterval(refreshPhoneStatusTime, 30000);
  if (autoSavingsInputEl) autoSavingsInputEl.value = autoSavingsPercent > 0 ? autoSavingsPercent.toFixed(1) : "";
  if (phoneUi.bankAutoInput) phoneUi.bankAutoInput.value = autoSavingsPercent > 0 ? autoSavingsPercent.toFixed(1) : "";

  phoneUi.openBtn.addEventListener("click", () => openPhone("home"));
  if (phoneUi.closeBtn) phoneUi.closeBtn.addEventListener("click", closePhone);
  if (phoneUi.homeBtn) phoneUi.homeBtn.addEventListener("click", goPhoneHomeAnimated);
  phoneUi.overlay.addEventListener("click", (event) => {
    if (event.target === phoneUi.overlay) closePhone();
  });
  phoneUi.tabs.forEach((tab) => {
    tab.addEventListener("click", () => setPhoneApp(tab.dataset.phoneApp));
  });
  if (phoneUi.missionTabs.length > 0) {
    phoneUi.missionTabs.forEach((tab) => {
      tab.addEventListener("click", () => {
        setPhoneMissionTab(tab.dataset.missionTab);
      });
    });
  }
  if (phoneUi.bankClaimAllBtn) {
    phoneUi.bankClaimAllBtn.addEventListener("click", claimAllBankMissions);
  }
  if (phoneUi.homeApps.length > 0) {
    phoneUi.homeApps.forEach((button) => {
      button.addEventListener("click", () => {
        const target = button.dataset.targetApp;
        if (target) openPhoneAppFromIcon(target, button);
      });
    });
  }
  bindPhoneMiniGameApps();
  bindPhoneMiniFrameMessages();
  if (phoneUi.clearMessagesBtn) {
    phoneUi.clearMessagesBtn.addEventListener("click", () => {
      phoneState.notifications = [];
      phoneState.unread = 0;
      renderPhoneMessages();
      renderPhoneBadge();
      savePhoneState();
    });
  }

  if (phoneUi.bankSetAutoBtn) {
    phoneUi.bankSetAutoBtn.addEventListener("click", () => {
      const value = roundCurrency(clampPercent(Number(phoneUi.bankAutoInput.value)));
      if (value <= 0) {
        autoSavingsPercent = 0;
        if (autoSavingsInputEl) autoSavingsInputEl.value = "";
        setBankMessage("Auto-save turned off.");
      } else {
        autoSavingsPercent = value;
        if (autoSavingsInputEl) autoSavingsInputEl.value = value.toFixed(1);
        setBankMessage(`Auto-save set to ${value.toFixed(1)}% of each win.`);
      }
      refreshPhoneBankApp();
      savePhoneState();
    });
  }
  if (phoneUi.bankTakeLoanBtn) {
    phoneUi.bankTakeLoanBtn.addEventListener("click", () => {
      const amount = Number(phoneUi.bankLoanInput?.value);
      processTakeLoan(amount);
    });
  }
  if (phoneUi.bankRepayLoanBtn) {
    phoneUi.bankRepayLoanBtn.addEventListener("click", () => {
      processRepayLoan();
    });
  }
  if (phoneUi.bankAutoOffBtn) {
    phoneUi.bankAutoOffBtn.addEventListener("click", () => {
      autoSavingsPercent = 0;
      if (autoSavingsInputEl) autoSavingsInputEl.value = "";
      setBankMessage("Auto-save turned off.");
      refreshPhoneBankApp();
      savePhoneState();
    });
  }
  if (phoneUi.bankWithdrawBtn) {
    phoneUi.bankWithdrawBtn.addEventListener("click", () => {
      if (savingsBalance <= 0) {
        setBankMessage("Savings balance is empty.");
        return;
      }
      const withdrawn = withdrawSavings(savingsBalance);
      setBankMessage(`Withdrew ${formatCurrency(withdrawn)} from savings.`);
      refreshPhoneBankApp();
      savePhoneState();
    });
  }
  if (phoneUi.settingAnimations) {
    phoneUi.settingAnimations.addEventListener("change", () => {
      phoneState.settings.animations = Boolean(phoneUi.settingAnimations.checked);
      applyPhoneSettings();
      savePhoneState();
    });
  }
  if (phoneUi.settingPopupSeconds) {
    phoneUi.settingPopupSeconds.addEventListener("change", () => {
      phoneState.settings.popupSeconds = clampMarket(Number(phoneUi.settingPopupSeconds.value) || 2.2, 1, 8);
      applyPhoneSettings();
      savePhoneState();
    });
  }
  if (phoneUi.settingAutoLimit) {
    phoneUi.settingAutoLimit.addEventListener("change", () => {
      phoneState.settings.autoRoundLimit = Math.max(0, Math.floor(Number(phoneUi.settingAutoLimit.value) || 0));
      resetAutoRoundCounter("slots");
      applyPhoneSettings();
      savePhoneState();
    });
  }
  if (phoneUi.settingUiScale) {
    phoneUi.settingUiScale.addEventListener("input", () => {
      phoneState.settings.uiScale = clampMarket(Number(phoneUi.settingUiScale.value) || 100, 85, 115);
      applyPhoneSettings();
      savePhoneState();
    });
  }
  if (phoneUi.totalResetBtn) {
    phoneUi.totalResetBtn.addEventListener("click", runTotalProgressReset);
  }
  if (phoneUi.feedbackSubmitBtn) {
    phoneUi.feedbackSubmitBtn.addEventListener("click", () => {
      void submitPhoneFeedback();
    });
  }
  if (phoneUi.feedbackInput) {
    phoneUi.feedbackInput.addEventListener("keydown", (event) => {
      if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
        event.preventDefault();
        void submitPhoneFeedback();
      }
    });
  }

  const quickGames = [
    ["blackjack", "Blackjack"],
    ["slots", "Slots"],
    ["horseracing", "Horse Racing"],
    ["roulette", "Roulette"],
    ["keno", "Keno"],
    ["mines", "Mines"],
    ["crash", "Crash"],
    ["hilo", "Hi-Lo"]
  ];
  if (phoneUi.casinoQuickLaunch) {
    phoneUi.casinoQuickLaunch.innerHTML = "";
    quickGames.forEach(([key, label]) => {
      const button = document.createElement("button");
      button.type = "button";
      button.textContent = label;
      button.dataset.casinoQuickGame = key;
      button.addEventListener("click", () => {
        const gate = getCasinoGateState();
        if (!gate.ok) {
          alert(gate.message);
          return;
        }
        tradingSection.style.display = "none";
        casinoSection.style.display = "block";
        loadCasinoGame(key);
        enterCasinoGameView(label);
        closePhone();
      });
      phoneUi.casinoQuickLaunch.appendChild(button);
    });
  }

  renderPhoneBadge();
  renderPhoneMessages();
  renderPhoneFeedbackList(phoneFeedbackEntries);
  setPhoneFeedbackStatus("Submit feedback to help improve the game.");
  refreshPhonePanels();
  pushPhoneNotification("info", "Phone online. Apps ready.");
}

function cleanupActiveCasinoGame() {
  if (typeof activeCasinoCleanup === "function") {
    activeCasinoCleanup();
  }
  activeCasinoCleanup = null;
}

// =====================================================
// ================ FULLSCREEN GAME VIEW ===============
// =====================================================
function updateFullscreenCash() {
  const el = document.getElementById("fsCash");
  if (el) el.textContent = cash.toFixed(2);
}

function enterCasinoGameView(title) {
  const bankPanel = document.getElementById("loan-panel");
  if (bankPanel) bankPanel.classList.remove("open");
  syncCasinoLiveFeedVisibility();

  if (IS_PHONE_EMBED_MODE) {
    document.body.classList.remove("game-fullscreen");
    const existingEmbedBar = document.getElementById("game-exit-bar");
    if (existingEmbedBar) existingEmbedBar.remove();
    return;
  }

  document.body.classList.add("game-fullscreen");

  const container = document.getElementById("casino-container");
  if (!container) return;

  const existing = document.getElementById("game-exit-bar");
  if (existing) existing.remove();

  const bar = document.createElement("div");
  bar.id = "game-exit-bar";
  bar.innerHTML = `
    <button id="exitGameBtn" type="button">← Casino Lobby</button>
    <div class="game-title">${title || ""}</div>
    <div class="game-cash">Cash: $<span id="fsCash">${cash.toFixed(2)}</span></div>
  `;
  container.prepend(bar);
  document.getElementById("exitGameBtn").onclick = exitCasinoGameView;
  updateFullscreenCash();
}

function exitCasinoGameView() {
  activeCasinoGameKey = "lobby";
  cleanupActiveCasinoGame();
  document.body.classList.remove("game-fullscreen");
  const container = document.getElementById("casino-container");
  if (container) {
    container.classList.remove(
      "casino-fullbleed",
      "slots-fullbleed",
      "blackjack-fullbleed",
      "poker-fullbleed",
      "poker-plinko-fullbleed",
      "plinko-fullbleed",
      "plinko-neon-fullbleed",
      "plinko-mini-fullbleed",
      "dragon-fullbleed",
      "crossy-fullbleed",
      "roulette-fullbleed",
      "casino-roulette-classic-fullbleed",
      "horse-fullbleed",
      "dice-fullbleed",
      "slide-fullbleed",
      "diamond-fullbleed",
      "mines-fullbleed",
      "keno-fullbleed"
    );
    container.innerHTML = "";
  }
  syncCasinoLiveFeedVisibility();
  updateUI();
}

let casinoWinPopupTimeoutId = null;
let casinoBigWinFxTimeoutId = null;
let casinoWinCountupRafId = null;
let casinoWinCountupToken = 0;

function formatCasinoWinMoney(value) {
  return `${CURRENCY_SYMBOL}${value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
}

function animateCasinoWinAmount(amountEl, targetAmount, durationMs = 1300) {
  if (!amountEl) return;
  if (casinoWinCountupRafId) {
    cancelAnimationFrame(casinoWinCountupRafId);
    casinoWinCountupRafId = null;
  }

  const token = ++casinoWinCountupToken;
  const safeTarget = Math.max(0, Number(targetAmount) || 0);
  const startAt = performance.now();
  amountEl.textContent = formatCasinoWinMoney(0);

  const tick = (now) => {
    if (token !== casinoWinCountupToken) return;
    const progress = Math.min(1, (now - startAt) / durationMs);
    const eased = 1 - Math.pow(1 - progress, 3);
    amountEl.textContent = formatCasinoWinMoney(safeTarget * eased);

    if (progress < 1) {
      casinoWinCountupRafId = requestAnimationFrame(tick);
      return;
    }

    amountEl.textContent = formatCasinoWinMoney(safeTarget);
    casinoWinCountupRafId = null;
  };

  casinoWinCountupRafId = requestAnimationFrame(tick);
}

function ensureCasinoWinPopupEl() {
  let layer = document.getElementById("casino-win-popup-layer");
  if (!layer) {
    layer = document.createElement("div");
    layer.id = "casino-win-popup-layer";
    document.body.appendChild(layer);
  }

  let popup = layer.querySelector("#casino-win-popup");
  if (!popup) {
    popup = document.createElement("div");
    popup.id = "casino-win-popup";
    layer.appendChild(popup);
  }

  if (!popup.querySelector(".casino-win-mult") || !popup.querySelector(".casino-win-amount")) {
    popup.innerHTML = `
      <div class="casino-win-mult">WIN</div>
      <div class="casino-win-divider"></div>
      <div class="casino-win-amount">$0.00</div>
    `;
  }

  return popup;
}

function triggerCasinoBigWinFx(layer) {
  if (!layer) return;
  if (casinoBigWinFxTimeoutId) {
    clearTimeout(casinoBigWinFxTimeoutId);
    casinoBigWinFxTimeoutId = null;
  }

  layer.classList.add("big-win-flash");
  layer.querySelectorAll(".casino-win-confetti").forEach((node) => node.remove());

  const colors = ["#ffd85f", "#ff6da7", "#68f0ff", "#86ff8a", "#ffffff"];
  const total = 42;
  for (let i = 0; i < total; i += 1) {
    const dot = document.createElement("span");
    dot.className = "casino-win-confetti";
    const angle = (Math.PI * 2 * i) / total + Math.random() * 0.3;
    const distance = 90 + Math.random() * 220;
    const tx = Math.cos(angle) * distance;
    const ty = Math.sin(angle) * distance * 0.72 + 40;
    dot.style.setProperty("--tx", `${tx.toFixed(1)}px`);
    dot.style.setProperty("--ty", `${ty.toFixed(1)}px`);
    dot.style.setProperty("--rot", `${Math.floor(Math.random() * 640 - 320)}deg`);
    dot.style.setProperty("--delay", `${Math.floor(Math.random() * 180)}ms`);
    dot.style.setProperty("--dur", `${1000 + Math.floor(Math.random() * 700)}ms`);
    dot.style.setProperty("--c", colors[i % colors.length]);
    layer.appendChild(dot);
  }

  casinoBigWinFxTimeoutId = setTimeout(() => {
    layer.classList.remove("big-win-flash");
    layer.querySelectorAll(".casino-win-confetti").forEach((node) => node.remove());
    casinoBigWinFxTimeoutId = null;
  }, 2600);
}

function showCasinoWinPopup(amountOrOptions, fallbackMultiplier = null) {
  const options =
    typeof amountOrOptions === "object" && amountOrOptions !== null
      ? amountOrOptions
      : { amount: amountOrOptions, multiplier: fallbackMultiplier };

  const winAmount = Number(options.amount);
  if (!Number.isFinite(winAmount) || winAmount <= 0.009) return 0;
  const autoSaved = maybeAutoSaveFromCasinoWin({
    amount: winAmount,
    multiplier: options.multiplier,
    source: options.source || null
  });

  const mult = Number(options.multiplier);
  const casinoContainer = document.getElementById("casino-container");
  const inCasinoView =
    document.body.classList.contains("game-fullscreen") ||
    (casinoContainer && casinoContainer.children.length > 0);
  if (!inCasinoView) return autoSaved;
  playGameSound("win");
  achievementState.stats.casinoWins += 1;
  saveAchievements();

  const popup = ensureCasinoWinPopupEl();
  if (!popup) return autoSaved;

  const multEl = popup.querySelector(".casino-win-mult");
  const amountEl = popup.querySelector(".casino-win-amount");
  if (!multEl || !amountEl) return autoSaved;

  const isBigWin =
    options.bigWin === true ||
    (Number.isFinite(mult) && mult >= 10) ||
    winAmount >= 500;

  if (casinoWinCountupRafId) {
    cancelAnimationFrame(casinoWinCountupRafId);
    casinoWinCountupRafId = null;
  }
  casinoWinCountupToken += 1;

  if (isBigWin) multEl.textContent = "BIG WIN";
  else if (Number.isFinite(mult) && mult > 0) multEl.textContent = `${mult.toFixed(2)}x`;
  else multEl.textContent = "WIN";
  amountEl.textContent = formatCasinoWinMoney(winAmount);
  popup.classList.toggle("big-win", isBigWin);
  phoneState.casinoProfit = roundCurrency(phoneState.casinoProfit + winAmount);
  trackBankMissionCasinoWin(winAmount, options.source || activeCasinoGameKey);
  refreshPhoneCasinoApp();
  savePhoneState();
  pushPhoneNotification("good", `Casino win: +${formatCurrency(winAmount)}`);
  pushCasinoLiveWin(winAmount, Number.isFinite(mult) ? mult : null, activeCasinoGameKey);

  popup.classList.remove("show");
  void popup.offsetWidth;
  popup.classList.add("show");
  const layer = document.getElementById("casino-win-popup-layer");
  if (isBigWin) triggerCasinoBigWinFx(layer);
  else if (layer) layer.classList.remove("big-win-flash");
  if (isBigWin) animateCasinoWinAmount(amountEl, winAmount);

  if (casinoWinPopupTimeoutId) {
    clearTimeout(casinoWinPopupTimeoutId);
    casinoWinPopupTimeoutId = null;
  }
  casinoWinPopupTimeoutId = setTimeout(() => {
    if (casinoWinCountupRafId) {
      cancelAnimationFrame(casinoWinCountupRafId);
      casinoWinCountupRafId = null;
    }
    casinoWinCountupToken += 1;
    popup.classList.remove("show");
    popup.classList.remove("big-win");
    casinoWinPopupTimeoutId = null;
  }, getCasinoPopupDurationMs(isBigWin));
  return autoSaved;
}

document.addEventListener("keydown", (e) => {
  if (e.key === "Escape" && document.body.classList.contains("game-fullscreen")) {
    exitCasinoGameView();
  }
});

// =====================================================
// ================= MARKET STEP =======================
// =====================================================
function stepMarket() {
  achievementState.stats.steps += 1;
  if (achievementState.stats.steps % 5 === 0) saveAchievements();
  applyInterest();
  trackBankMissionMarketStep();
  for (let i = 0; i < CANDLES_PER_STEP; i++) generateCandle();
  applyCarryCost();
  updateUI();
  updateLoanUI();
  drawChart();
}

function clampMarket(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function clampPrice(value) {
  return clampMarket(value, PRICE_FLOOR, PRICE_CAP);
}

function randomNormal() {
  let u = 0;
  let v = 0;
  while (u === 0) u = Math.random();
  while (v === 0) v = Math.random();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function pickWeighted(entries) {
  const roll = Math.random();
  let cumulative = 0;
  for (let i = 0; i < entries.length; i += 1) {
    cumulative += entries[i][1];
    if (roll <= cumulative) return entries[i][0];
  }
  return entries[entries.length - 1][0];
}

function updateMarketRegime() {
  marketModel.regimeTicksRemaining -= 1;
  if (marketModel.regimeTicksRemaining > 0) return;

  const nextRegime = pickWeighted(MARKET_TRANSITIONS[marketModel.regime] || MARKET_TRANSITIONS.balanced);
  marketModel.regime = nextRegime;

  const regime = MARKET_REGIMES[nextRegime] || MARKET_REGIMES.balanced;
  marketModel.regimeTicksRemaining = randomInt(regime.minDuration, regime.maxDuration);
}

function generateCandle() {
  let open = price;
  let high = open;
  let low = open;
  let close = open;
  let candleAbsMove = 0;

  for (let i = 0; i < TICKS_PER_CANDLE; i += 1) {
    updateMarketRegime();
    const regime = MARKET_REGIMES[marketModel.regime] || MARKET_REGIMES.balanced;

    const fairValueShock = randomNormal() * 0.00035 + regime.drift * 0.3;
    marketModel.fairValue = clampPrice(marketModel.fairValue * Math.exp(fairValueShock));

    const volPull = (regime.volTarget - marketModel.volatility) * 0.08;
    const momentumShock = Math.abs(marketModel.momentum) * 0.16;
    marketModel.volatility = clampMarket(marketModel.volatility + volPull + momentumShock, 0.00045, 0.0065);

    const innovation = randomNormal() * marketModel.volatility;
    marketModel.momentum = marketModel.momentum * 0.82 + innovation * 0.92;
    const momentumTerm = marketModel.momentum * 0.18;
    const meanReversion = ((marketModel.fairValue - price) / Math.max(price, PRICE_FLOOR)) * 0.045;
    const logReturn =
      regime.drift +
      innovation +
      momentumTerm +
      meanReversion +
      marketModel.newsShock +
      marketModel.newsDrift;

    marketModel.newsShock *= 0.84;
    marketModel.newsDrift *= 0.95;

    price = clampPrice(price * Math.exp(logReturn));
    candleAbsMove += Math.abs(logReturn);

    high = Math.max(high, price);
    low = Math.min(low, price);
    close = price;
  }

  if (Math.random() < NEWS_EVENT_CHANCE) {
    const preNewsPrice = price;
    triggerNews();
    const newsMove = Math.abs(Math.log(price / Math.max(preNewsPrice, PRICE_FLOOR)));
    candleAbsMove += newsMove * 1.6;
    high = Math.max(high, price);
    low = Math.min(low, price);
    close = price;
  }

  const bodyMove = Math.abs(Math.log(close / Math.max(open, PRICE_FLOOR)));
  const wickMove = Math.abs(Math.log(high / Math.max(low, PRICE_FLOOR)));
  const activityScore = bodyMove * 900 + wickMove * 350 + marketModel.volatility * 18000 + candleAbsMove * 220;
  const volume = Math.floor(clampMarket(12 + activityScore + Math.random() * 16, 8, MAX_VOLUME));

  candles.push({
    open,
    high,
    low,
    close,
    volume
  });

  const maxCandles = Math.floor(canvas.width / CANDLE_WIDTH);
  if (candles.length > maxCandles) candles.shift();
}

function triggerNews() {
  const event = NEWS_EVENTS[Math.floor(Math.random() * NEWS_EVENTS.length)];
  achievementState.stats.newsSeen += 1;
  saveAchievements();

  newsEl.textContent = event.text;
  newsEl.style.color = event.color;
  newsEl.style.opacity = 1;
  setTimeout(() => (newsEl.style.opacity = 0), 2500);
  pushPhoneNotification(event.direction > 0 ? "good" : "bad", `News: ${event.text}`);

  const directionalShock = event.direction * (event.shock + Math.abs(randomNormal()) * event.shock * 0.28);
  marketModel.newsShock += directionalShock;
  marketModel.newsDrift += event.direction * event.drift;
  marketModel.fairValue = clampPrice(marketModel.fairValue * (1 + event.direction * event.fairValueShift));

  const gapReturn = event.direction * event.gap + randomNormal() * 0.0011;
  price = clampPrice(price * Math.exp(gapReturn));
}

// =====================================================
// =================== TRADING =========================
// =====================================================
function buy(amount) {
  let sharesBought = 0;
  for (let i = 0; i < amount; i++) {
    const slippage = BASE_SLIPPAGE * (1 + shares / 50);
    const cost = price * (1 + slippage + TRADE_FEE);
    if (cash < cost) break;
    avgCost = (avgCost * shares + cost) / (shares + 1);
    cash -= cost;
    shares++;
    sharesBought++;
  }
  if (sharesBought > 0) {
    achievementState.stats.buys += sharesBought;
    saveAchievements();
    updateUI();
  }
}

function sell(amount) {
  let sharesSold = 0;
  let realizedProfit = 0;
  amount = Math.min(amount, shares);
  for (let i = 0; i < amount; i++) {
    const slippage = BASE_SLIPPAGE * (1 + shares / 50);
    const revenue = price * (1 - slippage - TRADE_FEE);
    realizedProfit += revenue - avgCost;
    cash += revenue;
    shares--;
    sharesSold++;
  }
  if (shares === 0) avgCost = 0;
  if (sharesSold > 0) {
    achievementState.stats.sells += sharesSold;
    saveAchievements();
    trackBankMissionTradeClosed(realizedProfit);
    updateUI();
  }
}

function buyAllShares() {
  let sharesBought = 0;
  while (true) {
    const slippage = BASE_SLIPPAGE * (1 + shares / 50);
    const cost = price * (1 + slippage + TRADE_FEE);
    if (cash < cost) break;
    avgCost = (avgCost * shares + cost) / (shares + 1);
    cash -= cost;
    shares++;
    sharesBought++;
  }
  if (sharesBought > 0) {
    achievementState.stats.buys += sharesBought;
    saveAchievements();
  }
  updateUI();
}

const buyBtn = document.getElementById("buy");
const buyAllBtn = document.getElementById("buyAll");
const sellBtn = document.getElementById("sell");
const sellAllBtn = document.getElementById("sellAll");
const nextBtn = document.getElementById("next");
const pauseBtn = document.getElementById("pause");

function runTradingAction(action) {
  return () => {
    try {
      action();
    } catch (error) {
      console.error("Trading action failed", error);
    }
  };
}

if (buyBtn) buyBtn.addEventListener("click", runTradingAction(() => buy(1)));
if (buyAllBtn) buyAllBtn.addEventListener("click", runTradingAction(() => buyAllShares()));
if (sellBtn) sellBtn.addEventListener("click", runTradingAction(() => sell(1)));
if (sellAllBtn) sellAllBtn.addEventListener("click", runTradingAction(() => sell(shares)));

function applyCarryCost() {
  if (shares <= 0) return;
  const exposure = (shares * price) / (cash + shares * price);
  const cost = exposure * 0.0004 * price * shares;
  cash = Math.max(0, cash - cost);
}

// =====================================================
// =================== UI UPDATE =======================
// =====================================================
function updateUI() {
  if (!Number.isFinite(cash)) cash = 0;
  if (!Number.isFinite(shares) || shares < 0) shares = 0;
  if (!Number.isFinite(avgCost) || avgCost < 0) avgCost = 0;

  price = clampPrice(price);
  marketModel.fairValue = clampPrice(marketModel.fairValue);
  const marketPrice = price;

  cashEl.textContent = cash.toFixed(2);
  sharesEl.textContent = shares;
  sidePriceEl.textContent = `$${marketPrice.toFixed(2)}`;
  avgBuyEl.textContent = shares ? `$${avgCost.toFixed(2)}` : "N/A";

  const net = getNetWorth(marketPrice);
  const pnl = net - BASE_NET_WORTH;
  netEl.textContent = net.toFixed(2);
  plEl.textContent = pnl.toFixed(2);

  sidePriceEl.style.color =
    shares && marketPrice > avgCost ? "#0f0" : shares && marketPrice < avgCost ? "#f00" : "#fff";

  const casinoCash = document.getElementById("casinoCash");
  if (casinoCash) casinoCash.textContent = cash.toFixed(2);
  syncCasinoLiveFeedVisibility();
  updateTradingUsernameBadge();
  renderCasinoLiveFeed();

  try {
    updateAchievements(net);
  } catch (error) {
    console.error("updateAchievements failed", error);
  }
  try {
    refreshTradingMissionHud();
  } catch (error) {
    console.error("refreshTradingMissionHud failed", error);
  }
  try {
    updateFullscreenCash();
  } catch (error) {
    console.error("updateFullscreenCash failed", error);
  }
  try {
    updateLoanUI();
  } catch (error) {
    console.error("updateLoanUI failed", error);
  }
  const cashDelta = roundCurrency(cash - bankMissionLastCashSnapshot);
  try {
    trackBankMissionCashDelta(cashDelta);
    trackBankMissionDebtSnapshot();
  } catch (error) {
    console.error("bank mission tracking failed", error);
  }
  bankMissionLastCashSnapshot = cash;
  try {
    refreshPhonePanels();
    postPhoneMiniCashUpdate();
    persistCashStateSoon();
    scheduleUserProfileSync();
    syncHiddenAdminTriggerVisibility();
  } catch (error) {
    console.error("phone panel refresh failed", error);
  }
}

// =====================================================
// =================== CHART ===========================
// =====================================================
function drawChart() {
  const gradient = ctx.createLinearGradient(0, 0, 0, canvas.height);
  gradient.addColorStop(0, "#111");
  gradient.addColorStop(1, "#000");
  ctx.fillStyle = gradient;
  ctx.fillRect(0, 0, canvas.width, canvas.height);

  if (!candles.length) return;

  const prices = candles.flatMap((c) => [
    clampPrice(c.high),
    clampPrice(c.low)
  ]);
  prices.push(clampPrice(price));
  if (shares > 0 && avgCost > 0) {
    prices.push(clampPrice(avgCost));
  }
  const max = Math.max(...prices);
  const min = Math.min(...prices);
  const range = max - min || 1;
  const axisWidth = 64;
  const plotRight = Math.max(0, canvas.width - axisWidth);

  const priceToY = (p) => canvas.height - ((p - min) / range) * canvas.height;
  const tickCount = 6;

  ctx.fillStyle = "rgba(4, 8, 12, 0.62)";
  ctx.fillRect(plotRight, 0, axisWidth, canvas.height);

  ctx.save();
  ctx.font = '11px "Segoe UI", sans-serif';
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  for (let tick = 0; tick <= tickCount; tick += 1) {
    const value = max - (range * tick) / tickCount;
    const yTick = clampMarket(priceToY(value), 0, canvas.height);

    ctx.strokeStyle = "rgba(255, 255, 255, 0.08)";
    ctx.beginPath();
    ctx.moveTo(0, yTick);
    ctx.lineTo(plotRight, yTick);
    ctx.stroke();

    ctx.fillStyle = "rgba(225, 235, 245, 0.9)";
    ctx.fillText(`${CURRENCY_SYMBOL}${value.toFixed(2)}`, canvas.width - 6, yTick);
  }
  ctx.restore();

  candles.forEach((c, i) => {
    const open = clampPrice(c.open);
    const close = clampPrice(c.close);
    const high = clampPrice(c.high);
    const low = clampPrice(c.low);
    const x = i * CANDLE_WIDTH + CANDLE_WIDTH / 2;
    const openY = priceToY(open);
    const closeY = priceToY(close);
    const highY = priceToY(high);
    const lowY = priceToY(low);
    const bullish = close >= open;

    ctx.shadowColor = bullish ? "#0f0" : "#f00";
    ctx.shadowBlur = 8;

    ctx.strokeStyle = bullish ? "#0f0" : "#f00";
    ctx.beginPath();
    ctx.moveTo(x, highY);
    ctx.lineTo(x, lowY);
    ctx.stroke();

    const bodyTop = Math.min(openY, closeY);
    const bodyHeight = Math.abs(openY - closeY) || 1;
    ctx.fillStyle = bullish ? "#0f0" : "#f00";
    ctx.fillRect(x - CANDLE_WIDTH / 2, bodyTop, CANDLE_WIDTH - 1, bodyHeight);

    const volHeight = (c.volume / MAX_VOLUME) * 50;
    ctx.fillStyle = "rgba(255,255,255,0.25)";
    ctx.fillRect(
      x - CANDLE_WIDTH / 2,
      canvas.height - volHeight,
      CANDLE_WIDTH - 1,
      volHeight
    );

    ctx.shadowBlur = 0;
  });

  const y = clampMarket(priceToY(clampPrice(price)), 0, canvas.height);
  ctx.strokeStyle = "#ff0";
  ctx.beginPath();
  ctx.moveTo(0, y);
  ctx.lineTo(plotRight, y);
  ctx.stroke();

  ctx.fillStyle = "#ff0";
  ctx.fillText(`$${clampPrice(price).toFixed(2)}`, 10, y - 6);

  if (shares > 0 && avgCost > 0) {
    const avgY = clampMarket(priceToY(clampPrice(avgCost)), 0, canvas.height);
    ctx.save();
    ctx.setLineDash([6, 4]);
    ctx.strokeStyle = "#4ec8ff";
    ctx.beginPath();
    ctx.moveTo(0, avgY);
    ctx.lineTo(plotRight, avgY);
    ctx.stroke();
    ctx.setLineDash([]);
    ctx.fillStyle = "#4ec8ff";
    ctx.fillText(`Avg $${clampPrice(avgCost).toFixed(2)}`, 10, avgY - 6);
    ctx.restore();
  }
}

// =====================================================
// =================== CONTROLS ========================
// =====================================================
if (nextBtn) nextBtn.addEventListener("click", runTradingAction(() => stepMarket()));

if (pauseBtn) pauseBtn.addEventListener("click", runTradingAction(() => {
  autoPlay = !autoPlay;
  pauseBtn.textContent = autoPlay ? "Pause" : "Play";
}));

function startMarketInterval() {
  clearInterval(marketInterval);
  marketInterval = setInterval(() => autoPlay && stepMarket(), currentInterval);
}

document.querySelectorAll(".speed-btn").forEach((btn) => {
  btn.addEventListener("click", runTradingAction(() => {
    const nextSpeed = Number(btn.dataset.speed);
    if (!Number.isFinite(nextSpeed) || nextSpeed <= 0) return;
    currentInterval = nextSpeed;
    startMarketInterval();
  }));
});

// =====================================================
// ================= NAVIGATION ========================
// =====================================================
const tradingSection = document.getElementById("trading-section");
const casinoSection = document.getElementById("casino-section");

document.getElementById("casinoBtn").onclick = () => {
  hideCasinoKickoutOverlay();
  const gate = getCasinoGateState();
  if (!gate.ok) {
    alert(gate.message);
    return;
  }
  tradingSection.style.display = "none";
  casinoSection.style.display = "block";
  syncHiddenAdminTriggerVisibility();
  updateTradingUsernameBadge();
};

document.getElementById("tradingBtn").onclick =
  document.getElementById("backToTrading").onclick = () => {
    hideCasinoKickoutOverlay();
    exitCasinoGameView();
    casinoSection.style.display = "none";
    tradingSection.style.display = "block";
    syncHiddenAdminTriggerVisibility();
    updateTradingUsernameBadge();
  };

// ------------------ LOAN SYSTEM ------------------
let loanPrincipal = 0;
let loanInterest = 0;
let loanAge = 0;
let blackMark = false;
let bankMessage = "";
let bankMessageTimer = null;

const INTEREST_RATE = 0.025;
const DEFAULT_LIMIT = 60;

const loanAmountEl = document.getElementById("loanAmount");
const loanInterestEl = document.getElementById("loanInterest");
const loanProgressBar = document.getElementById("loan-progress-bar");
const creditStatusEl = document.getElementById("creditStatus");
const loanInputEl = document.getElementById("loanInput");
const takeLoanBtnEl = document.getElementById("takeLoanBtn");
const repayLoanBtnEl = document.getElementById("repayLoanBtn");
const savingsAmountEl = document.getElementById("savingsAmount");
const autoSavingsAmountEl = document.getElementById("autoSavingsAmount");
const withdrawAllSavingsBtn = document.getElementById("withdrawAllSavingsBtn");
const withdrawSavingsInputEl = document.getElementById("withdrawSavingsInput");
const withdrawSavingsBtn = document.getElementById("withdrawSavingsBtn");
const autoSavingsInputEl = document.getElementById("autoSavingsInput");
const setAutoSavingsBtn = document.getElementById("setAutoSavingsBtn");
const clearAutoSavingsBtn = document.getElementById("clearAutoSavingsBtn");
const buyFundsSmallBtn = document.getElementById("buyFundsSmallBtn");
const buyFundsMediumBtn = document.getElementById("buyFundsMediumBtn");
const buyFundsXLBtn = document.getElementById("buyFundsXLBtn");
const buyFundsLargeBtn = document.getElementById("buyFundsLargeBtn");
const venmoClaimPackEl = document.getElementById("venmoClaimPack");
const venmoClaimTxnInputEl = document.getElementById("venmoClaimTxnInput");
const venmoClaimSubmitBtn = document.getElementById("venmoClaimSubmitBtn");
const venmoClaimStatusEl = document.getElementById("venmoClaimStatus");
const hiddenAdminTriggerEl = document.getElementById("hiddenAdminTrigger");
const hiddenAdminOverlayEl = document.getElementById("hiddenAdminOverlay");
const hiddenAdminCloseBtnEl = document.getElementById("hiddenAdminCloseBtn");
const hiddenAdminStatusEl = document.getElementById("hiddenAdminStatus");
const hiddenAdminStatsEl = document.getElementById("hiddenAdminStats");
const hiddenAdminStatsCollapseBtn = document.getElementById("hiddenAdminStatsCollapseBtn");
const hiddenAdminHealthEl = document.getElementById("hiddenAdminHealth");
const hiddenAdminHealthCollapseBtn = document.getElementById("hiddenAdminHealthCollapseBtn");
const hiddenAdminUsersEl = document.getElementById("hiddenAdminUsers");
const hiddenAdminUsersCollapseBtn = document.getElementById("hiddenAdminUsersCollapseBtn");
const hiddenAdminDevicesEl = document.getElementById("hiddenAdminDevices");
const hiddenAdminDevicesCollapseBtn = document.getElementById("hiddenAdminDevicesCollapseBtn");
const hiddenAdminDevicesClearBtn = document.getElementById("hiddenAdminDevicesClearBtn");
const hiddenAdminFeedbackEl = document.getElementById("hiddenAdminFeedback");
const hiddenAdminFeedbackCollapseBtn = document.getElementById("hiddenAdminFeedbackCollapseBtn");
const hiddenAdminFeedbackClearBtn = document.getElementById("hiddenAdminFeedbackClearBtn");
const hiddenAdminActivityEl = document.getElementById("hiddenAdminActivity");
const hiddenAdminActivityCollapseBtn = document.getElementById("hiddenAdminActivityCollapseBtn");
const hiddenAdminActivityClearBtn = document.getElementById("hiddenAdminActivityClearBtn");
const hiddenAdminFraudEl = document.getElementById("hiddenAdminFraud");
const hiddenAdminFraudCollapseBtn = document.getElementById("hiddenAdminFraudCollapseBtn");
const hiddenAdminFraudClearBtn = document.getElementById("hiddenAdminFraudClearBtn");
const hiddenAdminBackupsEl = document.getElementById("hiddenAdminBackups");
const hiddenAdminBackupsCollapseBtn = document.getElementById("hiddenAdminBackupsCollapseBtn");
const hiddenAdminBackupsClearBtn = document.getElementById("hiddenAdminBackupsClearBtn");
const hiddenAdminClaimsCollapseBtn = document.getElementById("hiddenAdminClaimsCollapseBtn");
const hiddenAdminClaimsClearBtn = document.getElementById("hiddenAdminClaimsClearBtn");
const hiddenAdminUsersClearBtn = document.getElementById("hiddenAdminUsersClearBtn");
const venmoAdminCodeInputEl = document.getElementById("venmoAdminCodeInput");
const venmoAdminUnlockBtn = document.getElementById("venmoAdminUnlockBtn");
const venmoAdminTrustDeviceBtn = document.getElementById("venmoAdminTrustDeviceBtn");
const hiddenAdminFullResetBtn = document.getElementById("hiddenAdminFullResetBtn");
const hiddenAdminCreateBackupBtn = document.getElementById("hiddenAdminCreateBackupBtn");
const venmoAdminClaimsEl = document.getElementById("venmoAdminClaims");
const buyVipBtn = document.getElementById("buyVipBtn");
const vipStatusTextEl = document.getElementById("vipStatusText");
const VENMO_PLAYER_ID_STORAGE_KEY = "venmo_claim_player_id_v1";
const VENMO_LOCAL_CREDITED_STORAGE_KEY = "venmo_claim_credited_local_v1";
const VENMO_CLAIM_POLL_MS = 10000;
const HIDDEN_ADMIN_LIVE_POLL_MS = 1000;
const USER_PROFILE_SYNC_INTERVAL_MS = 1200;
const VENMO_API_FALLBACK_BASE = "https://nows-api.onrender.com";
const REAL_MONEY_FUND_PACKS = Object.freeze({
  small: { funds: 1000, usd: 3, venmoLink: "https://venmo.com/SSage000?txn=pay&amount=3&note=S%241000" },
  medium: { funds: 5000, usd: 10, venmoLink: "https://venmo.com/SSage000?txn=pay&amount=10&note=S%245000" },
  xlarge: { funds: 10000, usd: 20, venmoLink: "https://venmo.com/SSage000?txn=pay&amount=20&note=S%2410000" },
  large: { funds: 25000, usd: 40, venmoLink: "https://venmo.com/SSage000?txn=pay&amount=40&note=S%2425000" }
});
const venmoClaimState = {
  claims: [],
  adminClaims: [],
  adminUsers: [],
  adminStats: null,
  adminHealth: null,
  adminDevices: [],
  adminFeedback: [],
  adminActivity: [],
  adminFraudFlags: [],
  adminBackups: []
};
let venmoAdminUnlocked = false;
let venmoAdminDeviceToken = "";
let venmoClaimPlayerId = "";
let venmoClaimPollTimer = null;
const venmoLocallyCreditedClaimIds = new Set();
let userProfileSyncTimer = null;
let lastUserProfileSyncAt = 0;
let userProfileSyncSequence = 0;
let userProfileSyncLatestStartedSequence = 0;
let hiddenAdminPanelOpen = false;
let hiddenAdminLivePollTimer = null;
let hiddenAdminLivePollTick = 0;
let hiddenAdminClaimsCollapsed = false;
let hiddenAdminStatsCollapsed = false;
let hiddenAdminHealthCollapsed = false;
let hiddenAdminUsersCollapsed = false;
let hiddenAdminDevicesCollapsed = false;
let hiddenAdminFeedbackCollapsed = false;
let hiddenAdminActivityCollapsed = false;
let hiddenAdminFraudCollapsed = false;
let hiddenAdminBackupsCollapsed = false;
let hiddenAdminClaimsCleared = false;
let hiddenAdminUsersCleared = false;
let hiddenAdminDevicesCleared = false;
let hiddenAdminFeedbackCleared = false;
let hiddenAdminBackupsCleared = false;

function updateLoanUI() {
  applyVipWeeklyBonusIfDue();
  if (!LOANS_ENABLED) {
    loanPrincipal = 0;
    loanInterest = 0;
    loanAge = 0;
    blackMark = false;
  }
  if (loanAmountEl) loanAmountEl.textContent = loanPrincipal.toFixed(2);
  if (loanInterestEl) loanInterestEl.textContent = loanInterest.toFixed(2);

  const total = LOANS_ENABLED ? loanPrincipal + loanInterest : 0;
  const percent = LOANS_ENABLED && loanPrincipal > 0 ? clampMarket((loanAge / DEFAULT_LIMIT) * 100, 0, 100) : 0;
  if (loanProgressBar) {
    loanProgressBar.style.width = percent + "%";
    loanProgressBar.textContent = percent.toFixed(0) + "%";
  }

  if (creditStatusEl) {
    creditStatusEl.textContent = blackMark ? "Credit: Black Mark" : "Credit: Clean";
    creditStatusEl.style.color = blackMark ? "#f00" : "#0f0";
  }

  if (savingsAmountEl) savingsAmountEl.textContent = savingsBalance.toFixed(2);
  if (autoSavingsAmountEl) autoSavingsAmountEl.textContent = `${autoSavingsPercent.toFixed(2)}%`;

  if (withdrawAllSavingsBtn) withdrawAllSavingsBtn.disabled = savingsBalance <= 0;
  if (withdrawSavingsBtn) withdrawSavingsBtn.disabled = savingsBalance <= 0;
  if (clearAutoSavingsBtn) clearAutoSavingsBtn.disabled = autoSavingsPercent <= 0;
  if (buyVipBtn) buyVipBtn.disabled = Boolean(phoneState.vip?.active) || cash < VIP_COST;
  if (vipStatusTextEl) {
    if (phoneState.vip?.active) {
      const lastPaid = Number(phoneState.vip.lastWeeklyBonusAt) || 0;
      const dueInMs = Math.max(0, VIP_WEEKLY_MS - (Date.now() - lastPaid));
      const days = Math.ceil(dueInMs / (24 * 60 * 60 * 1000));
      vipStatusTextEl.textContent = dueInMs <= 0
        ? "Status: VIP (weekly bonus ready)"
        : `Status: VIP (next bonus ~${days}d)`;
      vipStatusTextEl.style.color = "#f4d27a";
    } else {
      vipStatusTextEl.textContent = "Status: Standard";
      vipStatusTextEl.style.color = "#b9d1df";
    }
  }

  const casinoWarning = document.getElementById("casinoWarning");
  if (casinoWarning) {
    const lockParts = [];
    const tradingTotal = getCasinoTradingTotal();
    const unlockProfitTarget = Math.max(0, CASINO_UNLOCK_TRADING_TOTAL - BASE_NET_WORTH);
    const currentProfit = Math.max(0, tradingTotal - BASE_NET_WORTH);
    if (!isCasinoTradingUnlocked()) {
      lockParts.push(
        `🔒 Casino locked until you make $${unlockProfitTarget.toFixed(0)} (Current: $${currentProfit.toFixed(2)} / $${unlockProfitTarget.toFixed(2)})`
      );
    }
    if (total > 0) {
      lockParts.push("🚫 Casino locked until loan is repaid");
    }
    casinoWarning.textContent = [...lockParts, bankMessage].filter(Boolean).join(" • ");
  }

  const casinoBtn = document.getElementById("casinoBtn");
  if (casinoBtn) {
    const gate = getCasinoGateState();
    casinoBtn.disabled = false;
    casinoBtn.title = gate.ok ? "Casino" : gate.message;
  }

  refreshPhoneBankApp();
}

function applyVipWeeklyBonusIfDue() {
  if (!phoneState.vip?.active) return;
  const now = Date.now();
  const last = Number(phoneState.vip.lastWeeklyBonusAt) || 0;
  if (last > 0 && now - last < VIP_WEEKLY_MS) return;
  phoneState.vip.lastWeeklyBonusAt = now;
  cash = roundCurrency(cash + VIP_WEEKLY_BONUS);
  pushPhoneBankHistory("VIP weekly bonus", VIP_WEEKLY_BONUS, "positive");
  pushPhoneNotification("bank", `VIP weekly bonus: +${formatCurrency(VIP_WEEKLY_BONUS)}`);
  savePhoneState();
}

function setBankMessage(message, durationMs = 2200) {
  if (bankMessageTimer) {
    clearTimeout(bankMessageTimer);
    bankMessageTimer = null;
  }
  bankMessage = typeof message === "string" ? message : "";
  if (bankMessage) pushPhoneNotification("bank", `Bank: ${bankMessage}`);
  if (bankMessage && durationMs > 0) {
    bankMessageTimer = setTimeout(() => {
      bankMessage = "";
      bankMessageTimer = null;
      updateLoanUI();
    }, durationMs);
  }
  updateLoanUI();
}

function getAutoSavingsInput() {
  if (!autoSavingsInputEl) return 0;
  const amount = Number(autoSavingsInputEl.value);
  if (!Number.isFinite(amount) || amount <= 0) return 0;
  return roundCurrency(clampPercent(amount));
}

function depositSavings(amount, { profitOnly = true } = {}) {
  const cleanAmount = roundCurrency(amount);
  const profitAvailable = getSavingsProfitAvailable();
  const cap = profitOnly ? Math.min(cash, profitAvailable) : cash;
  const transferable = roundCurrency(Math.min(cleanAmount, Math.max(0, cap)));
  if (transferable <= 0) return 0;

  cash = roundCurrency(cash - transferable);
  savingsBalance = roundCurrency(savingsBalance + transferable);
  updateUI();
  return transferable;
}

function withdrawSavings(amount) {
  const cleanAmount = roundCurrency(amount);
  const transferable = roundCurrency(Math.min(cleanAmount, Math.max(0, savingsBalance)));
  if (transferable <= 0) return 0;

  savingsBalance = roundCurrency(savingsBalance - transferable);
  cash = roundCurrency(cash + transferable);
  pushPhoneBankHistory("Savings withdrawn", transferable, "positive");
  updateUI();
  return transferable;
}

function processSavingsWithdraw(rawAmount) {
  if (savingsBalance <= 0) {
    setBankMessage("Savings balance is empty.");
    return 0;
  }

  const amount = Number(rawAmount);
  if (!Number.isFinite(amount) || amount <= 0) {
    setBankMessage("Enter a valid withdraw amount.");
    return 0;
  }

  const withdrawn = withdrawSavings(amount);
  if (withdrawn <= 0) {
    setBankMessage("Not enough savings for that withdraw.");
    return 0;
  }

  setBankMessage(`Withdrew ${formatCurrency(withdrawn)} from savings.`);
  return withdrawn;
}

function creditPurchasedFunds(amount) {
  const funds = roundCurrency(Number(amount));
  if (!Number.isFinite(funds) || funds <= 0) return 0;
  savingsBalance = roundCurrency(savingsBalance + funds);
  pushPhoneBankHistory("Funds purchased", funds, "positive");
  setBankMessage(`Purchased ${formatCurrency(funds)} into savings.`);
  updateUI();
  return funds;
}

function beginRealMoneyFundsCheckout(packageId) {
  const pack = REAL_MONEY_FUND_PACKS[packageId];
  if (!pack?.venmoLink) return;
  setBankMessage(`Opening Venmo for ${formatCurrency(pack.funds)} (USD ${Number(pack.usd).toFixed(2)}).`);
  window.open(pack.venmoLink, "_blank", "noopener,noreferrer");
}

function normalizeVenmoTxnId(value) {
  return String(value || "").trim().toLowerCase().replace(/\s+/g, "");
}

function getVenmoClaimPlayerId() {
  try {
    const existing = String(localStorage.getItem(VENMO_PLAYER_ID_STORAGE_KEY) || "").trim();
    if (existing) return existing;
  } catch {}
  const generated = `player_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 9)}`;
  try {
    localStorage.setItem(VENMO_PLAYER_ID_STORAGE_KEY, generated);
  } catch {}
  return generated;
}

function loadVenmoLocalCreditedClaimIds() {
  try {
    const raw = localStorage.getItem(VENMO_LOCAL_CREDITED_STORAGE_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    if (!Array.isArray(parsed)) return;
    venmoLocallyCreditedClaimIds.clear();
    parsed.forEach((id) => {
      const safeId = String(id || "").trim();
      if (safeId) venmoLocallyCreditedClaimIds.add(safeId);
    });
  } catch {}
}

function saveVenmoLocalCreditedClaimIds() {
  try {
    localStorage.setItem(VENMO_LOCAL_CREDITED_STORAGE_KEY, JSON.stringify([...venmoLocallyCreditedClaimIds]));
  } catch {}
}

async function syncCurrentUserProfileToServer({ force = false } = {}) {
  if (!playerUsername || IS_PHONE_EMBED_MODE) return false;
  if (!authSessionLikelyAuthenticated) return false;
  if (!venmoClaimPlayerId) venmoClaimPlayerId = getVenmoClaimPlayerId();
  if (!venmoClaimPlayerId) return false;
  const now = Date.now();
  if (!force && now - lastUserProfileSyncAt < USER_PROFILE_SYNC_INTERVAL_MS) return false;
  lastUserProfileSyncAt = now;
  const syncSequence = ++userProfileSyncSequence;
  userProfileSyncLatestStartedSequence = syncSequence;
  try {
    const payload = await venmoApiRequest("/api/users/sync", {
      method: "POST",
      body: {
        playerId: venmoClaimPlayerId,
        username: playerUsername,
        clientBalanceUpdatedAt: Math.max(0, Math.floor(Number(lastServerBalanceUpdatedAt) || 0)),
        balance: roundCurrency(cash),
        shares: Math.max(0, Math.floor(Number(shares) || 0)),
        avgCost: Math.max(0, Number(avgCost) || 0),
        savingsBalance: roundCurrency(savingsBalance),
        autoSavingsPercent: roundCurrency(clampPercent(autoSavingsPercent))
      }
    });
    const responseBalanceUpdatedAt = Number(payload?.user?.balanceUpdatedAt);
    if (Number.isFinite(responseBalanceUpdatedAt) && responseBalanceUpdatedAt > 0) {
      lastServerBalanceUpdatedAt = Math.max(lastServerBalanceUpdatedAt, Math.floor(responseBalanceUpdatedAt));
      phoneState.balanceUpdatedAt = lastServerBalanceUpdatedAt;
    }
    const isLatestSync = syncSequence === userProfileSyncLatestStartedSequence;
    if (force && payload?.user && isLatestSync) {
      const beforeSig = getPersistProfileSignature();
      applyServerPortfolioSnapshot(payload.user, { useServerBalance: true });
      const afterSig = getPersistProfileSignature();
      if (afterSig !== beforeSig) updateUI();
    }
    if (isCasinoLiveDataEnabled() && !casinoLeaderboardStream) {
      void refreshCasinoLeaderboardFromServer();
    }
    return true;
  } catch (error) {
    const serverUser = error?.details?.user;
    const isLatestSync = syncSequence === userProfileSyncLatestStartedSequence;
    if (serverUser && isLatestSync) {
      const beforeSig = getPersistProfileSignature();
      applyServerPortfolioSnapshot(serverUser, { useServerBalance: true });
      const afterSig = getPersistProfileSignature();
      if (afterSig !== beforeSig) updateUI();
      setBankMessage(String(error?.message || "Server rejected local balance update."));
    }
    return false;
  }
}

function scheduleUserProfileSync() {
  if (IS_PHONE_EMBED_MODE || !playerUsername) return;
  if (userProfileSyncTimer) return;
  userProfileSyncTimer = setTimeout(() => {
    userProfileSyncTimer = null;
    syncCurrentUserProfileToServer();
  }, 900);
}

function getVenmoPackLabel(packId) {
  const pack = REAL_MONEY_FUND_PACKS[packId];
  if (!pack) return "Unknown pack";
  return `${formatCurrency(pack.funds)} (USD ${Number(pack.usd).toFixed(2)})`;
}

async function venmoApiRequest(path, options = {}) {
  const config = { ...options };
  const skipFallback = config.skipFallback === true;
  delete config.skipFallback;
  if (!config.credentials) config.credentials = "include";
  if (config.body && typeof config.body !== "string") {
    config.body = JSON.stringify(config.body);
    config.headers = {
      "Content-Type": "application/json",
      ...(config.headers || {})
    };
  }
  const urlsToTry = [path];
  const isRelativePath = typeof path === "string" && !/^https?:\/\//i.test(path);
  const isApiPath = isRelativePath && String(path).startsWith("/api/");
  const hostname = String(window.location.hostname || "").toLowerCase();
  const isLocalHost = hostname === "localhost" || hostname === "127.0.0.1";
  const isFileProtocol = window.location.protocol === "file:";
  const allowFallback =
    !skipFallback &&
    isRelativePath &&
    (!isApiPath || isFileProtocol || isLocalHost);
  if (allowFallback) {
    urlsToTry.push(`${VENMO_API_FALLBACK_BASE}${path}`);
  }
  let lastError = new Error("Request failed");

  for (const url of urlsToTry) {
    try {
      const response = await fetch(url, config);
      const payload = await response.json().catch(() => ({}));
      if (!response.ok || payload?.ok === false) {
        const requestError = new Error(payload?.error || `Request failed (${response.status})`);
        requestError.details = payload;
        requestError.status = response.status;
        if (isBannedAccountErrorMessage(requestError.message)) {
          showBannedOverlay("You are banned.");
        }
        lastError = requestError;
        continue;
      }
      return payload;
    } catch (error) {
      lastError = error instanceof Error ? error : new Error(String(error));
    }
  }
  throw lastError;
}

async function ensureAuthSessionReady(expectedPlayerId = "") {
  const safeExpectedId = String(expectedPlayerId || "").trim();
  for (let attempt = 0; attempt < 3; attempt += 1) {
    try {
      const sessionPayload = await venmoApiRequest("/api/auth/session", { skipFallback: true });
      const sessionPlayerId = String(sessionPayload?.user?.playerId || "").trim();
      if (sessionPayload?.authenticated && (!safeExpectedId || sessionPlayerId === safeExpectedId)) {
        return true;
      }
    } catch {}
    await new Promise((resolve) => setTimeout(resolve, 150));
  }
  return false;
}

function getAdminDeviceToken() {
  if (!venmoAdminDeviceToken) {
    venmoAdminDeviceToken = loadOrCreateAdminDeviceToken();
  }
  return venmoAdminDeviceToken;
}

function getAdminRequestOptions(options = {}) {
  const token = getAdminDeviceToken();
  const headers = {
    ...(options.headers || {}),
    "X-Admin-Device-Token": token
  };
  return {
    ...options,
    headers
  };
}

function buildAdminApiUrl(path) {
  return String(path || "");
}

function isAdminLoginRequiredError(error) {
  const message = String(error?.message || "").toLowerCase();
  return (
    message.includes("login required for admin access") ||
    message.includes("session account not found") ||
    message.includes("please login again")
  );
}

function handleAdminLoginRequired(error) {
  if (!isAdminLoginRequiredError(error)) return false;
  venmoAdminUnlocked = false;
  setHiddenAdminStatus("Login required. Sign in with username/password, then unlock admin.", true);
  try {
    setFirstLaunchAuthMode("login");
    showFirstLaunchUsernameOverlay();
  } catch {}
  return true;
}

async function trustCurrentAdminDevice({ silent = false, code = "" } = {}) {
  const adminCode = String(code || "").trim();
  if (!adminCode) return false;
  try {
    const payload = await venmoApiRequest("/api/admin/device/trust", getAdminRequestOptions({
      method: "POST",
      body: {
        adminCode,
        label: loadOrCreateAdminDeviceLabel()
      }
    }));
    if (payload?.trustedDevice) {
      venmoClaimState.adminDevices = [payload.trustedDevice, ...venmoClaimState.adminDevices];
      renderHiddenAdminDevices();
    }
    if (venmoAdminCodeInputEl) venmoAdminCodeInputEl.value = "";
    return true;
  } catch (error) {
    const handledLoginRequired = handleAdminLoginRequired(error);
    if (!silent) {
      if (!handledLoginRequired) {
        setHiddenAdminStatus(String(error?.message || "Could not trust this device."), true);
      }
    }
    return false;
  }
}

function mapServerClaim(claim) {
  return {
    id: String(claim?.id || ""),
    playerId: String(claim?.playerId || ""),
    username: String(claim?.username || ""),
    email: normalizeAuthEmail(claim?.email || ""),
    source: String(claim?.source || claim?.claimSource || "venmo"),
    packId: String(claim?.packId || ""),
    txnId: String(claim?.txnId || ""),
    txnNorm: normalizeVenmoTxnId(claim?.txnNorm || claim?.txnId || ""),
    status: ["pending", "approved", "rejected"].includes(String(claim?.status)) ? String(claim.status) : "pending",
    submittedAt: Number(claim?.submittedAt) || Date.now(),
    reviewedAt: Number(claim?.reviewedAt) || 0
  };
}

async function refreshVenmoClaimsFromServer({ silent = false } = {}) {
  if (!venmoClaimPlayerId) return false;
  try {
    const payload = await venmoApiRequest(`/api/claims?playerId=${encodeURIComponent(venmoClaimPlayerId)}`);
    venmoClaimState.claims = Array.isArray(payload.claims) ? payload.claims.map(mapServerClaim) : [];
    renderVenmoClaimStatus();
    return true;
  } catch (error) {
    if (!silent) setBankMessage(`Claim sync failed: ${error.message}`);
    return false;
  }
}

async function refreshVenmoAdminClaimsFromServer({ silent = false, allowWhenLocked = false } = {}) {
  if (!venmoAdminUnlocked && !allowWhenLocked) return false;
  try {
    const payload = await venmoApiRequest(
      buildAdminApiUrl("/api/admin/claims?status=pending"),
      getAdminRequestOptions()
    );
    venmoClaimState.adminClaims = Array.isArray(payload.claims) ? payload.claims.map(mapServerClaim) : [];
    venmoAdminUnlocked = true;
    renderVenmoAdminClaims();
    setHiddenAdminStatus("Admin unlocked. Device verified.");
    return true;
  } catch (error) {
    venmoClaimState.adminClaims = [];
    venmoClaimState.adminUsers = [];
    venmoClaimState.adminStats = null;
    venmoClaimState.adminHealth = null;
    venmoClaimState.adminDevices = [];
    venmoClaimState.adminFeedback = [];
    venmoClaimState.adminActivity = [];
    venmoClaimState.adminFraudFlags = [];
    venmoClaimState.adminBackups = [];
    venmoAdminUnlocked = false;
    renderVenmoAdminClaims();
    renderHiddenAdminUsers();
    renderHiddenAdminStats();
    renderHiddenAdminHealth();
    renderHiddenAdminDevices();
    renderHiddenAdminFeedback();
    renderHiddenAdminActivity();
    renderHiddenAdminFraudFlags();
    renderHiddenAdminBackups();
    const handledLoginRequired = handleAdminLoginRequired(error);
    if (!silent && !handledLoginRequired) setHiddenAdminStatus(String(error?.message || "Unlock failed."), true);
    if (!silent) setBankMessage(`Admin claim fetch failed: ${error.message}`);
    return false;
  }
}

function renderVenmoClaimStatus() {
  if (!venmoClaimStatusEl) return;
  const total = venmoClaimState.claims.length;
  if (total <= 0) {
    venmoClaimStatusEl.textContent = "No claims yet";
    return;
  }
  const pending = venmoClaimState.claims.filter((claim) => claim.status === "pending").length;
  const approved = venmoClaimState.claims.filter((claim) => claim.status === "approved").length;
  const rejected = venmoClaimState.claims.filter((claim) => claim.status === "rejected").length;
  venmoClaimStatusEl.textContent = `Claims: ${total} • Pending: ${pending} • Approved: ${approved} • Rejected: ${rejected}`;
}

function setVenmoClaimInlineStatus(message, isError = false) {
  if (!venmoClaimStatusEl) return;
  venmoClaimStatusEl.textContent = String(message || "");
  venmoClaimStatusEl.style.color = isError ? "#ff8ea0" : "#b9d1df";
}

function setHiddenAdminStatus(message, isError = false) {
  if (!hiddenAdminStatusEl) return;
  hiddenAdminStatusEl.textContent = String(message || "");
  hiddenAdminStatusEl.style.color = isError ? "#ff8ea0" : "#a8c4db";
}

function getVenmoPackFunds(packId) {
  const pack = REAL_MONEY_FUND_PACKS[String(packId || "")];
  return Number(pack?.funds) || 0;
}

function renderVenmoAdminClaims() {
  if (!venmoAdminClaimsEl) return;
  syncHiddenAdminCoreSectionControls();
  if (!venmoAdminUnlocked) {
    venmoAdminClaimsEl.innerHTML = `<div class="hidden-admin-empty">Locked.</div>`;
    return;
  }
  if (hiddenAdminClaimsCollapsed) return;
  if (hiddenAdminClaimsCleared) {
    venmoAdminClaimsEl.innerHTML = `<div class="hidden-admin-empty">Pending Claims view cleared.</div>`;
    return;
  }
  const claims = [...venmoClaimState.adminClaims].sort((a, b) => Number(b.submittedAt) - Number(a.submittedAt));
  if (claims.length === 0) {
    venmoAdminClaimsEl.innerHTML = `<div class="hidden-admin-empty">No pending claims.</div>`;
    return;
  }
  const rows = claims
    .map((claim) => {
      const amount = formatCurrency(getVenmoPackFunds(claim.packId));
      const source = escapeHtml(claim.source || "venmo");
      const txnId = escapeHtml(String(claim.txnId || "—").trim() || "—");
      const submitted = formatDateTime(claim.submittedAt);
      const username = escapeHtml(claim.username || "—");
      const email = escapeHtml(claim.email || "—");
      const claimId = escapeHtml(claim.id);
      return `
        <tr>
          <td>${username}</td>
          <td>${email}</td>
          <td>${amount}</td>
          <td>${source}</td>
          <td class="admin-txn-id">${txnId}</td>
          <td>${submitted}</td>
          <td>
            <button type="button" data-action="approve" data-claim-id="${claimId}">Approve</button>
            <button type="button" data-action="reject" data-claim-id="${claimId}">Reject</button>
          </td>
        </tr>
      `;
    })
    .join("");
  venmoAdminClaimsEl.innerHTML = `
    <table class="hidden-admin-table">
      <thead>
        <tr>
          <th>Username</th>
          <th>Email</th>
          <th>Amount</th>
          <th>Source</th>
          <th>Transaction ID</th>
          <th>Submitted</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderHiddenAdminUsers() {
  if (!hiddenAdminUsersEl) return;
  syncHiddenAdminCoreSectionControls();
  if (!venmoAdminUnlocked) {
    hiddenAdminUsersEl.innerHTML = `<div class="hidden-admin-empty">Locked.</div>`;
    return;
  }
  if (hiddenAdminUsersCollapsed) return;
  if (hiddenAdminUsersCleared) {
    hiddenAdminUsersEl.innerHTML = `<div class="hidden-admin-empty">Users view cleared.</div>`;
    return;
  }
  const users = [...venmoClaimState.adminUsers].sort((a, b) => Number(b.balance) - Number(a.balance));
  if (users.length === 0) {
    hiddenAdminUsersEl.innerHTML = `<div class="hidden-admin-empty">No users synced yet.</div>`;
    return;
  }
  const rows = users
    .map((user) => {
      const username = escapeHtml(user.username || "Unknown");
      const email = escapeHtml(user.email || "—");
      const passwordState = user.hasPassword ? "Set" : "Not set";
      const claims = Number(user.totalClaims) || 0;
      const pendingClaims = Number(user.pendingClaims) || 0;
      const wins = Number(user.totalWins) || 0;
      const lastSeen = formatDateTime(user.lastSeenAt);
      const bannedAt = Number(user.bannedAt) || 0;
      const mutedUntil = Number(user.mutedUntil) || 0;
      const isBanned = bannedAt > 0;
      const isMuted = mutedUntil > Date.now();
      const bannedReason = escapeHtml(user.bannedReason || "");
      const mutedReason = escapeHtml(user.mutedReason || "");
      const playerId = escapeHtml(String(user.playerId || ""));
      const statusLabel = isBanned
        ? `Banned ${formatDateTime(bannedAt)}${bannedReason ? ` (${bannedReason})` : ""}`
        : isMuted
          ? `Muted until ${formatDateTime(mutedUntil)}${mutedReason ? ` (${mutedReason})` : ""}`
          : "Active";
      return `
        <tr>
          <td>${username}</td>
          <td>${email}</td>
          <td>${formatCurrency(Number(user.balance) || 0)}</td>
          <td>${statusLabel}</td>
          <td>${passwordState}</td>
          <td>${claims}</td>
          <td>${pendingClaims}</td>
          <td>${wins}</td>
          <td>${lastSeen}</td>
          <td>
            <button type="button" data-user-action="set-balance" data-player-id="${playerId}" data-current-balance="${(Number(user.balance) || 0).toFixed(2)}">Set Balance</button>
            <button type="button" data-user-action="reset-progress" data-player-id="${playerId}">Reset Progress</button>
            <button type="button" data-user-action="force-logout" data-player-id="${playerId}">Force Logout</button>
            ${
              isMuted
                ? `<button type="button" data-user-action="unmute" data-player-id="${playerId}">Unmute</button>`
                : `<button type="button" data-user-action="mute" data-player-id="${playerId}">Mute</button>`
            }
            ${
              isBanned
                ? `<button type="button" data-user-action="unban" data-player-id="${playerId}">Unban</button>`
                : `<button type="button" data-user-action="ban" data-player-id="${playerId}">Ban</button>`
            }
            <button type="button" data-user-action="remove" data-player-id="${playerId}">Remove</button>
          </td>
        </tr>
      `;
    })
    .join("");
  hiddenAdminUsersEl.innerHTML = `
    <table class="hidden-admin-table">
      <thead>
        <tr>
          <th>Username</th>
          <th>Email</th>
          <th>Current Balance</th>
          <th>Status</th>
          <th>Password</th>
          <th>Claims</th>
          <th>Pending</th>
          <th>Wins</th>
          <th>Last Seen</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
    <div class="hidden-admin-footnote">Passwords are securely hashed and never shown in plaintext.</div>
  `;
}

function renderHiddenAdminStats() {
  if (!hiddenAdminStatsEl) return;
  syncHiddenAdminCoreSectionControls();
  if (!venmoAdminUnlocked) {
    hiddenAdminStatsEl.innerHTML = `<div class="hidden-admin-empty">Locked.</div>`;
    return;
  }
  if (hiddenAdminStatsCollapsed) return;
  const stats = venmoClaimState.adminStats;
  if (!stats) {
    hiddenAdminStatsEl.innerHTML = `<div class="hidden-admin-empty">No stats yet.</div>`;
    return;
  }
  const cards = [
    ["Users", Number(stats.totalUsers) || 0],
    ["Users Banned", Number(stats.bannedUsers) || 0],
    ["Active (24h)", Number(stats.activeUsers24h) || 0],
    ["Claims Pending", Number(stats.pendingClaims) || 0],
    ["Claims Approved", Number(stats.approvedClaims) || 0],
    ["Live Wins (24h)", Number(stats.liveWins24h) || 0],
    ["Devices Active", Number(stats.trustedDevicesActive) || 0],
    ["Devices Banned", Number(stats.trustedDevicesBanned) || 0],
    ["Total Balance", formatCurrency(Number(stats.totalBalance) || 0)]
  ]
    .map(([label, value]) => `
      <div class="hidden-admin-stat-card">
        <div class="hidden-admin-stat-label">${escapeHtml(label)}</div>
        <div class="hidden-admin-stat-value">${escapeHtml(String(value))}</div>
      </div>
    `)
    .join("");
  hiddenAdminStatsEl.innerHTML = `<div class="hidden-admin-stats-grid">${cards}</div>`;
}

function renderHiddenAdminHealth() {
  if (!hiddenAdminHealthEl) return;
  syncHiddenAdminCoreSectionControls();
  if (!venmoAdminUnlocked) {
    hiddenAdminHealthEl.innerHTML = `<div class="hidden-admin-empty">Locked.</div>`;
    return;
  }
  if (hiddenAdminHealthCollapsed) return;
  const health = venmoClaimState.adminHealth;
  if (!health) {
    hiddenAdminHealthEl.innerHTML = `<div class="hidden-admin-empty">No system health data yet.</div>`;
    return;
  }
  const uptimeMinutes = Math.max(0, Math.floor((Number(health.uptimeSeconds) || 0) / 60));
  const latestBackup = health.lastBackup && typeof health.lastBackup === "object" ? health.lastBackup : null;
  const latestBackupText = latestBackup
    ? `${formatDateTime(latestBackup.createdAt)} (${escapeHtml(String(latestBackup.id || ""))})`
    : "No backups yet";
  const cards = [
    ["API", String(health.apiStatus || "unknown").toUpperCase()],
    ["Database", String(health.dbStatus || "unknown").toUpperCase()],
    ["Error Count (24h)", Number(health.errors24h) || 0],
    ["Rate-Limit Hits (24h)", Number(health.rateLimitHits24h) || 0],
    ["Live Rate-Limit Buckets", Number(health.rateLimitBuckets) || 0],
    ["Server Uptime", `${uptimeMinutes} min`],
    ["Latest Backup", latestBackupText]
  ]
    .map(([label, value]) => `
      <div class="hidden-admin-stat-card">
        <div class="hidden-admin-stat-label">${escapeHtml(label)}</div>
        <div class="hidden-admin-stat-value">${escapeHtml(String(value))}</div>
      </div>
    `)
    .join("");
  hiddenAdminHealthEl.innerHTML = `<div class="hidden-admin-stats-grid">${cards}</div>`;
}

function getAdminEventLabel(eventType) {
  const normalized = String(eventType || "")
    .replace(/[_.-]+/g, " ")
    .trim();
  if (!normalized) return "Event";
  return normalized
    .split(" ")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function syncHiddenAdminSectionControl(buttonEl, sectionEl, collapsed) {
  if (buttonEl) {
    buttonEl.textContent = collapsed ? "Expand" : "Collapse";
    buttonEl.disabled = !venmoAdminUnlocked;
  }
  if (sectionEl) {
    sectionEl.style.display = collapsed ? "none" : "";
  }
}

function syncHiddenAdminClearControl(buttonEl, { count = 0, cleared = false } = {}) {
  if (!buttonEl) return;
  buttonEl.textContent = cleared ? "Restore" : "Clear";
  buttonEl.disabled = !venmoAdminUnlocked || (!cleared && Number(count) <= 0);
}

function syncHiddenAdminCoreSectionControls() {
  const claimCount = Array.isArray(venmoClaimState.adminClaims) ? venmoClaimState.adminClaims.length : 0;
  const userCount = Array.isArray(venmoClaimState.adminUsers) ? venmoClaimState.adminUsers.length : 0;
  const deviceCount = Array.isArray(venmoClaimState.adminDevices) ? venmoClaimState.adminDevices.length : 0;
  const feedbackCount = Array.isArray(venmoClaimState.adminFeedback) ? venmoClaimState.adminFeedback.length : 0;
  const backupCount = Array.isArray(venmoClaimState.adminBackups) ? venmoClaimState.adminBackups.length : 0;
  syncHiddenAdminSectionControl(hiddenAdminClaimsCollapseBtn, venmoAdminClaimsEl, hiddenAdminClaimsCollapsed);
  syncHiddenAdminClearControl(hiddenAdminClaimsClearBtn, { count: claimCount, cleared: hiddenAdminClaimsCleared });
  syncHiddenAdminSectionControl(hiddenAdminStatsCollapseBtn, hiddenAdminStatsEl, hiddenAdminStatsCollapsed);
  syncHiddenAdminSectionControl(hiddenAdminHealthCollapseBtn, hiddenAdminHealthEl, hiddenAdminHealthCollapsed);
  syncHiddenAdminSectionControl(hiddenAdminUsersCollapseBtn, hiddenAdminUsersEl, hiddenAdminUsersCollapsed);
  syncHiddenAdminClearControl(hiddenAdminUsersClearBtn, { count: userCount, cleared: hiddenAdminUsersCleared });
  syncHiddenAdminSectionControl(hiddenAdminDevicesCollapseBtn, hiddenAdminDevicesEl, hiddenAdminDevicesCollapsed);
  syncHiddenAdminClearControl(hiddenAdminDevicesClearBtn, { count: deviceCount, cleared: hiddenAdminDevicesCleared });
  syncHiddenAdminSectionControl(hiddenAdminFeedbackCollapseBtn, hiddenAdminFeedbackEl, hiddenAdminFeedbackCollapsed);
  syncHiddenAdminClearControl(hiddenAdminFeedbackClearBtn, { count: feedbackCount, cleared: hiddenAdminFeedbackCleared });
  syncHiddenAdminSectionControl(hiddenAdminBackupsCollapseBtn, hiddenAdminBackupsEl, hiddenAdminBackupsCollapsed);
  syncHiddenAdminClearControl(hiddenAdminBackupsClearBtn, { count: backupCount, cleared: hiddenAdminBackupsCleared });
}

function setHiddenAdminClaimsCollapsed(collapsed) {
  hiddenAdminClaimsCollapsed = Boolean(collapsed);
  syncHiddenAdminCoreSectionControls();
}

function setHiddenAdminClaimsCleared(cleared) {
  hiddenAdminClaimsCleared = Boolean(cleared);
  syncHiddenAdminCoreSectionControls();
}

function setHiddenAdminStatsCollapsed(collapsed) {
  hiddenAdminStatsCollapsed = Boolean(collapsed);
  syncHiddenAdminCoreSectionControls();
}

function setHiddenAdminHealthCollapsed(collapsed) {
  hiddenAdminHealthCollapsed = Boolean(collapsed);
  syncHiddenAdminCoreSectionControls();
}

function setHiddenAdminUsersCollapsed(collapsed) {
  hiddenAdminUsersCollapsed = Boolean(collapsed);
  syncHiddenAdminCoreSectionControls();
}

function setHiddenAdminUsersCleared(cleared) {
  hiddenAdminUsersCleared = Boolean(cleared);
  syncHiddenAdminCoreSectionControls();
}

function setHiddenAdminDevicesCollapsed(collapsed) {
  hiddenAdminDevicesCollapsed = Boolean(collapsed);
  syncHiddenAdminCoreSectionControls();
}

function setHiddenAdminDevicesCleared(cleared) {
  hiddenAdminDevicesCleared = Boolean(cleared);
  syncHiddenAdminCoreSectionControls();
}

function setHiddenAdminFeedbackCollapsed(collapsed) {
  hiddenAdminFeedbackCollapsed = Boolean(collapsed);
  syncHiddenAdminCoreSectionControls();
}

function setHiddenAdminFeedbackCleared(cleared) {
  hiddenAdminFeedbackCleared = Boolean(cleared);
  syncHiddenAdminCoreSectionControls();
}

function setHiddenAdminBackupsCollapsed(collapsed) {
  hiddenAdminBackupsCollapsed = Boolean(collapsed);
  syncHiddenAdminCoreSectionControls();
}

function setHiddenAdminBackupsCleared(cleared) {
  hiddenAdminBackupsCleared = Boolean(cleared);
  syncHiddenAdminCoreSectionControls();
}

function syncHiddenAdminActivityControls() {
  const eventCount = Array.isArray(venmoClaimState.adminActivity) ? venmoClaimState.adminActivity.length : 0;
  if (hiddenAdminActivityCollapseBtn) {
    hiddenAdminActivityCollapseBtn.textContent = hiddenAdminActivityCollapsed
      ? `Expand (${eventCount})`
      : `Collapse (${eventCount})`;
    hiddenAdminActivityCollapseBtn.disabled = !venmoAdminUnlocked;
  }
  if (hiddenAdminActivityClearBtn) {
    hiddenAdminActivityClearBtn.disabled = !venmoAdminUnlocked || eventCount <= 0;
  }
  if (hiddenAdminActivityEl) {
    hiddenAdminActivityEl.style.display = hiddenAdminActivityCollapsed ? "none" : "";
  }
}

function setHiddenAdminActivityCollapsed(collapsed) {
  hiddenAdminActivityCollapsed = Boolean(collapsed);
  syncHiddenAdminActivityControls();
}

function syncHiddenAdminFraudControls() {
  const flagCount = Array.isArray(venmoClaimState.adminFraudFlags) ? venmoClaimState.adminFraudFlags.length : 0;
  if (hiddenAdminFraudCollapseBtn) {
    hiddenAdminFraudCollapseBtn.textContent = hiddenAdminFraudCollapsed
      ? `Expand (${flagCount})`
      : `Collapse (${flagCount})`;
    hiddenAdminFraudCollapseBtn.disabled = !venmoAdminUnlocked;
  }
  if (hiddenAdminFraudClearBtn) {
    hiddenAdminFraudClearBtn.disabled = !venmoAdminUnlocked || flagCount <= 0;
  }
  if (hiddenAdminFraudEl) {
    hiddenAdminFraudEl.style.display = hiddenAdminFraudCollapsed ? "none" : "";
  }
}

function setHiddenAdminFraudCollapsed(collapsed) {
  hiddenAdminFraudCollapsed = Boolean(collapsed);
  syncHiddenAdminFraudControls();
}

function renderHiddenAdminActivity() {
  if (!hiddenAdminActivityEl) return;
  syncHiddenAdminActivityControls();
  if (!venmoAdminUnlocked) {
    hiddenAdminActivityEl.innerHTML = `<div class="hidden-admin-empty">Locked.</div>`;
    return;
  }
  if (hiddenAdminActivityCollapsed) return;
  const events = Array.isArray(venmoClaimState.adminActivity) ? venmoClaimState.adminActivity : [];
  if (!events.length) {
    hiddenAdminActivityEl.innerHTML = `<div class="hidden-admin-empty">No recent activity yet.</div>`;
    return;
  }
  const rows = events
    .map((entry) => {
      const details = entry?.details && typeof entry.details === "object" ? entry.details : {};
      const detailsText = Object.entries(details)
        .slice(0, 4)
        .map(([key, value]) => `${key}: ${String(value)}`)
        .join(" • ");
      return `
        <tr>
          <td>${escapeHtml(getAdminEventLabel(entry?.eventType))}</td>
          <td>${escapeHtml(String(entry?.username || "—"))}</td>
          <td>${escapeHtml(String(entry?.playerId || "—"))}</td>
          <td>${escapeHtml(detailsText || "—")}</td>
          <td>${escapeHtml(formatDateTime(entry?.createdAt))}</td>
        </tr>
      `;
    })
    .join("");
  hiddenAdminActivityEl.innerHTML = `
    <table class="hidden-admin-table">
      <thead>
        <tr>
          <th>Event</th>
          <th>Username</th>
          <th>Player ID</th>
          <th>Details</th>
          <th>Time</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderHiddenAdminFraudFlags() {
  if (!hiddenAdminFraudEl) return;
  syncHiddenAdminFraudControls();
  if (!venmoAdminUnlocked) {
    hiddenAdminFraudEl.innerHTML = `<div class="hidden-admin-empty">Locked.</div>`;
    return;
  }
  if (hiddenAdminFraudCollapsed) return;
  const flags = Array.isArray(venmoClaimState.adminFraudFlags) ? venmoClaimState.adminFraudFlags : [];
  if (!flags.length) {
    hiddenAdminFraudEl.innerHTML = `<div class="hidden-admin-empty">No fraud flags right now.</div>`;
    return;
  }
  const rows = flags
    .map((flag) => {
      const severity = String(flag?.severity || "low").toUpperCase();
      const value = Number(flag?.value);
      const renderedValue = Number.isFinite(value) ? (Math.abs(value) >= 1000 ? formatCurrency(value) : value.toFixed(2)) : "—";
      return `
        <tr>
          <td>${escapeHtml(getAdminEventLabel(flag?.type))}</td>
          <td>${escapeHtml(severity)}</td>
          <td>${escapeHtml(String(flag?.username || "—"))}</td>
          <td>${escapeHtml(String(flag?.playerId || "—"))}</td>
          <td>${escapeHtml(String(renderedValue))}</td>
          <td>${escapeHtml(String(flag?.note || "—"))}</td>
          <td>${escapeHtml(formatDateTime(flag?.createdAt))}</td>
        </tr>
      `;
    })
    .join("");
  hiddenAdminFraudEl.innerHTML = `
    <table class="hidden-admin-table">
      <thead>
        <tr>
          <th>Flag</th>
          <th>Severity</th>
          <th>Username</th>
          <th>Player ID</th>
          <th>Value</th>
          <th>Reason</th>
          <th>Time</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderHiddenAdminBackups() {
  if (!hiddenAdminBackupsEl) return;
  syncHiddenAdminCoreSectionControls();
  if (!venmoAdminUnlocked) {
    hiddenAdminBackupsEl.innerHTML = `<div class="hidden-admin-empty">Locked.</div>`;
    return;
  }
  if (hiddenAdminBackupsCollapsed) return;
  if (hiddenAdminBackupsCleared) {
    hiddenAdminBackupsEl.innerHTML = `<div class="hidden-admin-empty">Backups view cleared.</div>`;
    return;
  }
  const backups = Array.isArray(venmoClaimState.adminBackups) ? venmoClaimState.adminBackups : [];
  if (!backups.length) {
    hiddenAdminBackupsEl.innerHTML = `<div class="hidden-admin-empty">No backups yet.</div>`;
    return;
  }
  const rows = backups
    .map((backup) => {
      const backupId = escapeHtml(String(backup?.id || ""));
      const summary = backup?.summary && typeof backup.summary === "object" ? backup.summary : {};
      const summaryText = `Users: ${Number(summary.users) || 0} • Claims: ${Number(summary.claims) || 0} • Wins: ${Number(summary.liveWins) || 0} • Feedback: ${Number(summary.feedback) || 0}`;
      return `
        <tr>
          <td class="admin-txn-id">${backupId}</td>
          <td>${escapeHtml(formatDateTime(backup?.createdAt))}</td>
          <td>${escapeHtml(String(backup?.createdBy || "—"))}</td>
          <td>${escapeHtml(summaryText)}</td>
          <td><button type="button" data-backup-action="restore" data-backup-id="${backupId}">Restore</button></td>
        </tr>
      `;
    })
    .join("");
  hiddenAdminBackupsEl.innerHTML = `
    <table class="hidden-admin-table">
      <thead>
        <tr>
          <th>Backup ID</th>
          <th>Created</th>
          <th>Created By</th>
          <th>Summary</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function renderHiddenAdminDevices() {
  if (!hiddenAdminDevicesEl) return;
  syncHiddenAdminCoreSectionControls();
  if (!venmoAdminUnlocked) {
    hiddenAdminDevicesEl.innerHTML = `<div class="hidden-admin-empty">Locked.</div>`;
    return;
  }
  if (hiddenAdminDevicesCollapsed) return;
  if (hiddenAdminDevicesCleared) {
    hiddenAdminDevicesEl.innerHTML = `<div class="hidden-admin-empty">Trusted Devices view cleared.</div>`;
    return;
  }
  const devices = Array.isArray(venmoClaimState.adminDevices) ? venmoClaimState.adminDevices : [];
  if (!devices.length) {
    hiddenAdminDevicesEl.innerHTML = `<div class="hidden-admin-empty">No trusted devices found.</div>`;
    return;
  }
  const rows = devices
    .map((device) => {
      const deviceId = escapeHtml(String(device.id || ""));
      const isActive = device.active !== false && Number(device.revokedAt) <= 0;
      const status = isActive ? "Active" : `Banned ${formatDateTime(device.revokedAt)}`;
      return `
        <tr>
          <td>${escapeHtml(device.label || "Trusted device")}${device.current ? " (This device)" : ""}</td>
          <td>${status}</td>
          <td>${formatDateTime(device.trustedAt)}</td>
          <td>${formatDateTime(device.lastSeenAt)}</td>
          <td>
            ${
              isActive
                ? `<button type="button" data-device-action="ban" data-device-id="${deviceId}">Ban</button>`
                : `<button type="button" data-device-action="unban" data-device-id="${deviceId}">Unban</button>`
            }
            <button type="button" data-device-action="remove" data-device-id="${deviceId}">Remove</button>
          </td>
        </tr>
      `;
    })
    .join("");
  hiddenAdminDevicesEl.innerHTML = `
    <table class="hidden-admin-table">
      <thead>
        <tr>
          <th>Device</th>
          <th>Status</th>
          <th>Trusted</th>
          <th>Last Seen</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

function getAdminFeedbackStatusLabel(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (normalized === "closed") return "Closed";
  if (normalized === "reviewed") return "Reviewed";
  return "Open";
}

function renderHiddenAdminFeedback() {
  if (!hiddenAdminFeedbackEl) return;
  syncHiddenAdminCoreSectionControls();
  if (!venmoAdminUnlocked) {
    hiddenAdminFeedbackEl.innerHTML = `<div class="hidden-admin-empty">Locked.</div>`;
    return;
  }
  if (hiddenAdminFeedbackCollapsed) return;
  if (hiddenAdminFeedbackCleared) {
    hiddenAdminFeedbackEl.innerHTML = `<div class="hidden-admin-empty">Player Feedback view cleared.</div>`;
    return;
  }
  const feedbackEntries = Array.isArray(venmoClaimState.adminFeedback) ? venmoClaimState.adminFeedback : [];
  if (!feedbackEntries.length) {
    hiddenAdminFeedbackEl.innerHTML = `<div class="hidden-admin-empty">No feedback submitted yet.</div>`;
    return;
  }
  const rows = feedbackEntries
    .map((entry) => {
      const feedbackId = escapeHtml(String(entry?.id || ""));
      const username = escapeHtml(String(entry?.username || "Unknown"));
      const playerId = escapeHtml(String(entry?.playerId || "—"));
      const category = escapeHtml(getPhoneFeedbackCategoryLabel(entry?.category));
      const message = escapeHtml(String(entry?.message || ""));
      const status = escapeHtml(getAdminFeedbackStatusLabel(entry?.status));
      const submittedAt = escapeHtml(formatDateTime(entry?.submittedAt));
      return `
        <tr>
          <td>${username}</td>
          <td class="admin-txn-id">${playerId}</td>
          <td>${category}</td>
          <td>${message}</td>
          <td>${status}</td>
          <td>${submittedAt}</td>
          <td class="admin-txn-id">${feedbackId}</td>
          <td><button type="button" data-feedback-action="remove" data-feedback-id="${feedbackId}">Remove</button></td>
        </tr>
      `;
    })
    .join("");

  hiddenAdminFeedbackEl.innerHTML = `
    <table class="hidden-admin-table">
      <thead>
        <tr>
          <th>Username</th>
          <th>Player ID</th>
          <th>Category</th>
          <th>Message</th>
          <th>Status</th>
          <th>Submitted</th>
          <th>Feedback ID</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
}

async function refreshHiddenAdminUsersFromServer({ silent = false, allowWhenLocked = false } = {}) {
  if (!venmoAdminUnlocked && !allowWhenLocked) return false;
  try {
    const payload = await venmoApiRequest(
      buildAdminApiUrl("/api/admin/users"),
      getAdminRequestOptions()
    );
    venmoClaimState.adminUsers = Array.isArray(payload.users) ? payload.users : [];
    venmoAdminUnlocked = true;
    renderHiddenAdminUsers();
    return true;
  } catch (error) {
    venmoClaimState.adminUsers = [];
    renderHiddenAdminUsers();
    if (!silent) setHiddenAdminStatus(`Could not load users: ${error.message}`, true);
    return false;
  }
}

async function refreshHiddenAdminStatsFromServer({ silent = false, allowWhenLocked = false } = {}) {
  if (!venmoAdminUnlocked && !allowWhenLocked) return false;
  try {
    const payload = await venmoApiRequest(
      buildAdminApiUrl("/api/admin/stats"),
      getAdminRequestOptions()
    );
    venmoClaimState.adminStats = payload?.stats || null;
    venmoAdminUnlocked = true;
    renderHiddenAdminStats();
    return true;
  } catch (error) {
    venmoClaimState.adminStats = null;
    renderHiddenAdminStats();
    if (!silent) setHiddenAdminStatus(`Could not load stats: ${error.message}`, true);
    return false;
  }
}

async function refreshHiddenAdminHealthFromServer({ silent = false, allowWhenLocked = false } = {}) {
  if (!venmoAdminUnlocked && !allowWhenLocked) return false;
  try {
    const payload = await venmoApiRequest(
      buildAdminApiUrl("/api/admin/health"),
      getAdminRequestOptions()
    );
    venmoClaimState.adminHealth = payload?.health || null;
    venmoAdminUnlocked = true;
    renderHiddenAdminHealth();
    return true;
  } catch (error) {
    venmoClaimState.adminHealth = null;
    renderHiddenAdminHealth();
    if (!silent) setHiddenAdminStatus(`Could not load system health: ${error.message}`, true);
    return false;
  }
}

async function refreshHiddenAdminDevicesFromServer({ silent = false, allowWhenLocked = false } = {}) {
  if (!venmoAdminUnlocked && !allowWhenLocked) return false;
  try {
    const payload = await venmoApiRequest(
      buildAdminApiUrl("/api/admin/devices"),
      getAdminRequestOptions()
    );
    venmoClaimState.adminDevices = Array.isArray(payload.devices) ? payload.devices : [];
    venmoAdminUnlocked = true;
    renderHiddenAdminDevices();
    return true;
  } catch (error) {
    venmoClaimState.adminDevices = [];
    renderHiddenAdminDevices();
    if (!silent) setHiddenAdminStatus(`Could not load devices: ${error.message}`, true);
    return false;
  }
}

async function refreshHiddenAdminActivityFromServer({ silent = false, allowWhenLocked = false } = {}) {
  if (!venmoAdminUnlocked && !allowWhenLocked) return false;
  try {
    const payload = await venmoApiRequest(
      buildAdminApiUrl("/api/admin/activity?limit=120"),
      getAdminRequestOptions()
    );
    venmoClaimState.adminActivity = Array.isArray(payload.events) ? payload.events : [];
    venmoAdminUnlocked = true;
    renderHiddenAdminActivity();
    return true;
  } catch (error) {
    venmoClaimState.adminActivity = [];
    renderHiddenAdminActivity();
    if (!silent) setHiddenAdminStatus(`Could not load admin activity: ${error.message}`, true);
    return false;
  }
}

async function refreshHiddenAdminFeedbackFromServer({ silent = false, allowWhenLocked = false } = {}) {
  if (!venmoAdminUnlocked && !allowWhenLocked) return false;
  try {
    const payload = await venmoApiRequest(
      buildAdminApiUrl("/api/admin/feedback?limit=120"),
      getAdminRequestOptions()
    );
    venmoClaimState.adminFeedback = Array.isArray(payload.feedback) ? payload.feedback : [];
    venmoAdminUnlocked = true;
    renderHiddenAdminFeedback();
    return true;
  } catch (error) {
    venmoClaimState.adminFeedback = [];
    renderHiddenAdminFeedback();
    if (!silent) setHiddenAdminStatus(`Could not load feedback: ${error.message}`, true);
    return false;
  }
}

async function refreshHiddenAdminFraudFlagsFromServer({ silent = false, allowWhenLocked = false } = {}) {
  if (!venmoAdminUnlocked && !allowWhenLocked) return false;
  try {
    const payload = await venmoApiRequest(
      buildAdminApiUrl("/api/admin/fraud-flags"),
      getAdminRequestOptions()
    );
    venmoClaimState.adminFraudFlags = Array.isArray(payload.flags) ? payload.flags : [];
    venmoAdminUnlocked = true;
    renderHiddenAdminFraudFlags();
    return true;
  } catch (error) {
    venmoClaimState.adminFraudFlags = [];
    renderHiddenAdminFraudFlags();
    if (!silent) setHiddenAdminStatus(`Could not load fraud flags: ${error.message}`, true);
    return false;
  }
}

async function refreshHiddenAdminBackupsFromServer({ silent = false, allowWhenLocked = false } = {}) {
  if (!venmoAdminUnlocked && !allowWhenLocked) return false;
  try {
    const payload = await venmoApiRequest(
      buildAdminApiUrl("/api/admin/backups?limit=15"),
      getAdminRequestOptions()
    );
    venmoClaimState.adminBackups = Array.isArray(payload.backups) ? payload.backups : [];
    venmoAdminUnlocked = true;
    renderHiddenAdminBackups();
    return true;
  } catch (error) {
    venmoClaimState.adminBackups = [];
    renderHiddenAdminBackups();
    if (!silent) setHiddenAdminStatus(`Could not load backups: ${error.message}`, true);
    return false;
  }
}

async function tryAutoUnlockAdminFromTrustedDevice() {
  if (venmoAdminUnlocked) return true;
  try {
    getAdminDeviceToken();
  } catch {
    return false;
  }
  const claimsOk = await refreshVenmoAdminClaimsFromServer({ silent: true, allowWhenLocked: true });
  if (!claimsOk) return false;
  await refreshHiddenAdminStatsFromServer({ silent: true, allowWhenLocked: true });
  await refreshHiddenAdminHealthFromServer({ silent: true, allowWhenLocked: true });
  await refreshHiddenAdminUsersFromServer({ silent: true, allowWhenLocked: true });
  await refreshHiddenAdminDevicesFromServer({ silent: true, allowWhenLocked: true });
  await refreshHiddenAdminFeedbackFromServer({ silent: true, allowWhenLocked: true });
  await refreshHiddenAdminActivityFromServer({ silent: true, allowWhenLocked: true });
  await refreshHiddenAdminFraudFlagsFromServer({ silent: true, allowWhenLocked: true });
  await refreshHiddenAdminBackupsFromServer({ silent: true, allowWhenLocked: true });
  setHiddenAdminStatus("Trusted device recognized. Admin unlocked.");
  return true;
}

async function banAdminDevice(deviceId) {
  if (!venmoAdminUnlocked || !deviceId) return false;
  try {
    await venmoApiRequest(
      `/api/admin/devices/${encodeURIComponent(deviceId)}/ban`,
      getAdminRequestOptions({
        method: "POST"
      })
    );
    await refreshHiddenAdminDevicesFromServer({ silent: true });
    await refreshHiddenAdminStatsFromServer({ silent: true });
    await refreshHiddenAdminActivityFromServer({ silent: true });
    setHiddenAdminStatus("Device banned.");
    return true;
  } catch (error) {
    setHiddenAdminStatus(`Could not ban device: ${error.message}`, true);
    return false;
  }
}

async function unbanAdminDevice(deviceId) {
  if (!venmoAdminUnlocked || !deviceId) return false;
  try {
    await venmoApiRequest(
      `/api/admin/devices/${encodeURIComponent(deviceId)}/unban`,
      getAdminRequestOptions({
        method: "POST"
      })
    );
    await refreshHiddenAdminDevicesFromServer({ silent: true });
    await refreshHiddenAdminStatsFromServer({ silent: true });
    await refreshHiddenAdminActivityFromServer({ silent: true });
    setHiddenAdminStatus("Device unbanned.");
    return true;
  } catch (error) {
    setHiddenAdminStatus(`Could not unban device: ${error.message}`, true);
    return false;
  }
}

async function removeAdminDevice(deviceId) {
  if (!venmoAdminUnlocked || !deviceId) return false;
  try {
    await venmoApiRequest(
      `/api/admin/devices/${encodeURIComponent(deviceId)}/remove`,
      getAdminRequestOptions({
        method: "POST"
      })
    );
    await refreshHiddenAdminDevicesFromServer({ silent: true });
    await refreshHiddenAdminStatsFromServer({ silent: true });
    await refreshHiddenAdminActivityFromServer({ silent: true });
    setHiddenAdminStatus("Device removed.");
    return true;
  } catch (error) {
    setHiddenAdminStatus(`Could not remove device: ${error.message}`, true);
    return false;
  }
}

async function banAdminUser(playerId) {
  if (!venmoAdminUnlocked || !playerId) return false;
  try {
    await venmoApiRequest(
      `/api/admin/users/${encodeURIComponent(playerId)}/ban`,
      getAdminRequestOptions({
        method: "POST"
      })
    );
    await refreshHiddenAdminUsersFromServer({ silent: true });
    await refreshHiddenAdminStatsFromServer({ silent: true });
    await refreshVenmoAdminClaimsFromServer({ silent: true });
    await refreshHiddenAdminActivityFromServer({ silent: true });
    await refreshHiddenAdminFraudFlagsFromServer({ silent: true });
    setHiddenAdminStatus("User banned.");
    return true;
  } catch (error) {
    setHiddenAdminStatus(`Could not ban user: ${error.message}`, true);
    return false;
  }
}

async function unbanAdminUser(playerId) {
  if (!venmoAdminUnlocked || !playerId) return false;
  try {
    await venmoApiRequest(
      `/api/admin/users/${encodeURIComponent(playerId)}/unban`,
      getAdminRequestOptions({
        method: "POST"
      })
    );
    await refreshHiddenAdminUsersFromServer({ silent: true });
    await refreshHiddenAdminStatsFromServer({ silent: true });
    await refreshVenmoAdminClaimsFromServer({ silent: true });
    await refreshHiddenAdminActivityFromServer({ silent: true });
    await refreshHiddenAdminFraudFlagsFromServer({ silent: true });
    setHiddenAdminStatus("User unbanned.");
    return true;
  } catch (error) {
    setHiddenAdminStatus(`Could not unban user: ${error.message}`, true);
    return false;
  }
}

async function removeAdminUser(playerId) {
  if (!venmoAdminUnlocked || !playerId) return false;
  try {
    await venmoApiRequest(
      `/api/admin/users/${encodeURIComponent(playerId)}/remove`,
      getAdminRequestOptions({
        method: "POST"
      })
    );
    await refreshHiddenAdminUsersFromServer({ silent: true });
    await refreshHiddenAdminStatsFromServer({ silent: true });
    await refreshVenmoAdminClaimsFromServer({ silent: true });
    await refreshHiddenAdminActivityFromServer({ silent: true });
    await refreshHiddenAdminFraudFlagsFromServer({ silent: true });
    setHiddenAdminStatus("User removed.");
    return true;
  } catch (error) {
    setHiddenAdminStatus(`Could not remove user: ${error.message}`, true);
    return false;
  }
}

async function forceLogoutAdminUser(playerId) {
  if (!venmoAdminUnlocked || !playerId) return false;
  try {
    const payload = await venmoApiRequest(
      `/api/admin/users/${encodeURIComponent(playerId)}/force-logout`,
      getAdminRequestOptions({
        method: "POST"
      })
    );
    const sessionsCleared = Number(payload?.sessionsCleared) || 0;
    setHiddenAdminStatus(
      sessionsCleared > 0
        ? `Forced logout complete. Sessions cleared: ${sessionsCleared}.`
        : "No active sessions found for that user."
    );
    return true;
  } catch (error) {
    setHiddenAdminStatus(`Could not force logout user: ${error.message}`, true);
    return false;
  }
}

async function resetAdminUserProgress(playerId) {
  if (!venmoAdminUnlocked || !playerId) return false;
  const confirmed = window.confirm("Reset this user's progress (balance/shares/savings and claims/wins)?");
  if (!confirmed) return false;
  try {
    const payload = await venmoApiRequest(
      `/api/admin/users/${encodeURIComponent(playerId)}/reset-progress`,
      getAdminRequestOptions({
        method: "POST"
      })
    );
    await refreshHiddenAdminUsersFromServer({ silent: true });
    await refreshHiddenAdminStatsFromServer({ silent: true });
    await refreshHiddenAdminFraudFlagsFromServer({ silent: true });
    const sessionsCleared = Number(payload?.sessionsCleared) || 0;
    setHiddenAdminStatus(
      sessionsCleared > 0
        ? `User progress reset. Sessions cleared: ${sessionsCleared}.`
        : "User progress reset."
    );
    return true;
  } catch (error) {
    setHiddenAdminStatus(`Could not reset user progress: ${error.message}`, true);
    return false;
  }
}

async function muteAdminUser(playerId) {
  if (!venmoAdminUnlocked || !playerId) return false;
  const minutesInput = window.prompt("Mute minutes (1 - 10080):", "60");
  if (minutesInput === null) return false;
  const minutes = Math.max(1, Math.min(10080, Math.floor(Number(String(minutesInput).trim()))));
  if (!Number.isFinite(minutes) || minutes <= 0) {
    setHiddenAdminStatus("Enter a valid mute duration in minutes.", true);
    return false;
  }
  const reason = window.prompt("Mute reason (optional):", "Temporary cooldown");
  try {
    await venmoApiRequest(
      `/api/admin/users/${encodeURIComponent(playerId)}/mute`,
      getAdminRequestOptions({
        method: "POST",
        body: {
          minutes,
          reason: String(reason || "").trim()
        }
      })
    );
    await refreshHiddenAdminUsersFromServer({ silent: true });
    setHiddenAdminStatus(`User muted for ${minutes} minute${minutes === 1 ? "" : "s"}.`);
    return true;
  } catch (error) {
    setHiddenAdminStatus(`Could not mute user: ${error.message}`, true);
    return false;
  }
}

async function unmuteAdminUser(playerId) {
  if (!venmoAdminUnlocked || !playerId) return false;
  try {
    await venmoApiRequest(
      `/api/admin/users/${encodeURIComponent(playerId)}/unmute`,
      getAdminRequestOptions({
        method: "POST"
      })
    );
    await refreshHiddenAdminUsersFromServer({ silent: true });
    setHiddenAdminStatus("User unmuted.");
    return true;
  } catch (error) {
    setHiddenAdminStatus(`Could not unmute user: ${error.message}`, true);
    return false;
  }
}

async function setAdminUserBalance(playerId, currentBalance = "") {
  if (!venmoAdminUnlocked || !playerId) return false;
  const typedValue = window.prompt(
    "Set new balance (0 - 10,000,000):",
    String(currentBalance || "")
  );
  if (typedValue === null) return false;
  const normalized = String(typedValue).replace(/,/g, "").trim();
  const parsed = Number(normalized);
  if (!normalized || !Number.isFinite(parsed) || parsed < 0 || parsed > 10_000_000) {
    setHiddenAdminStatus("Enter a valid balance between 0 and 10,000,000.", true);
    return false;
  }
  const nextBalance = Math.round(parsed * 100) / 100;
  try {
    await venmoApiRequest(
      `/api/admin/users/${encodeURIComponent(playerId)}/balance`,
      getAdminRequestOptions({
        method: "POST",
        body: { balance: nextBalance }
      })
    );
    await refreshHiddenAdminUsersFromServer({ silent: true });
    await refreshHiddenAdminStatsFromServer({ silent: true });
    await refreshHiddenAdminActivityFromServer({ silent: true });
    await refreshHiddenAdminFraudFlagsFromServer({ silent: true });
    setHiddenAdminStatus(`Balance updated to ${formatCurrency(nextBalance)}.`);
    return true;
  } catch (error) {
    setHiddenAdminStatus(`Could not update balance: ${error.message}`, true);
    return false;
  }
}

async function createAdminBackup() {
  if (!venmoAdminUnlocked) {
    setHiddenAdminStatus("Unlock admin first.", true);
    return false;
  }
  try {
    const payload = await venmoApiRequest(
      "/api/admin/backups/create",
      getAdminRequestOptions({
        method: "POST"
      })
    );
    const backupId = String(payload?.backup?.id || "");
    await refreshHiddenAdminBackupsFromServer({ silent: true });
    await refreshHiddenAdminHealthFromServer({ silent: true });
    setHiddenAdminStatus(`Backup created${backupId ? ` (${backupId})` : ""}.`);
    return true;
  } catch (error) {
    setHiddenAdminStatus(`Could not create backup: ${error.message}`, true);
    return false;
  }
}

async function restoreAdminBackup(backupId) {
  if (!venmoAdminUnlocked || !backupId) return false;
  const code = String(venmoAdminCodeInputEl?.value || "").trim();
  if (!code) {
    setHiddenAdminStatus("Enter admin code first, then restore.", true);
    if (venmoAdminCodeInputEl) venmoAdminCodeInputEl.focus();
    return false;
  }
  const confirmed = window.confirm(
    "Restore this backup? This replaces users, claims, wins, feedback, and trusted devices."
  );
  if (!confirmed) return false;
  const typed = window.prompt('Type "RESTORE BACKUP" to confirm restore.');
  if (typed !== "RESTORE BACKUP") {
    setHiddenAdminStatus("Restore cancelled: confirmation text did not match.", true);
    return false;
  }
  try {
    await venmoApiRequest(
      `/api/admin/backups/${encodeURIComponent(backupId)}/restore`,
      getAdminRequestOptions({
        method: "POST",
        body: {
          confirmText: typed,
          adminCode: code
        }
      })
    );
    await refreshHiddenAdminStatsFromServer({ silent: true });
    await refreshHiddenAdminHealthFromServer({ silent: true });
    await refreshHiddenAdminUsersFromServer({ silent: true });
    await refreshHiddenAdminDevicesFromServer({ silent: true });
    await refreshHiddenAdminFeedbackFromServer({ silent: true });
    await refreshHiddenAdminActivityFromServer({ silent: true });
    await refreshHiddenAdminFraudFlagsFromServer({ silent: true });
    await refreshHiddenAdminBackupsFromServer({ silent: true });
    setHiddenAdminStatus("Backup restored.");
    return true;
  } catch (error) {
    setHiddenAdminStatus(`Could not restore backup: ${error.message}`, true);
    return false;
  }
}

async function removeAdminFeedback(feedbackId) {
  if (!venmoAdminUnlocked || !feedbackId) return false;
  const confirmed = window.confirm("Remove this feedback message?");
  if (!confirmed) return false;
  try {
    await venmoApiRequest(
      `/api/admin/feedback/${encodeURIComponent(feedbackId)}/remove`,
      getAdminRequestOptions({
        method: "POST"
      })
    );
    await refreshHiddenAdminFeedbackFromServer({ silent: true });
    await refreshHiddenAdminActivityFromServer({ silent: true });
    setHiddenAdminStatus("Feedback removed.");
    return true;
  } catch (error) {
    setHiddenAdminStatus(`Could not remove feedback: ${error.message}`, true);
    return false;
  }
}

function toggleHiddenAdminClaimsViewClear() {
  if (!venmoAdminUnlocked) {
    setHiddenAdminStatus("Unlock admin first.", true);
    return false;
  }
  if (hiddenAdminClaimsCleared) {
    setHiddenAdminClaimsCleared(false);
    renderVenmoAdminClaims();
    setHiddenAdminStatus("Pending Claims view restored.");
    return true;
  }
  if (!Array.isArray(venmoClaimState.adminClaims) || venmoClaimState.adminClaims.length <= 0) {
    setHiddenAdminStatus("Pending Claims list is already empty.");
    return true;
  }
  setHiddenAdminClaimsCleared(true);
  renderVenmoAdminClaims();
  setHiddenAdminStatus("Pending Claims view cleared.");
  return true;
}

function toggleHiddenAdminUsersViewClear() {
  if (!venmoAdminUnlocked) {
    setHiddenAdminStatus("Unlock admin first.", true);
    return false;
  }
  if (hiddenAdminUsersCleared) {
    setHiddenAdminUsersCleared(false);
    renderHiddenAdminUsers();
    setHiddenAdminStatus("Users view restored.");
    return true;
  }
  if (!Array.isArray(venmoClaimState.adminUsers) || venmoClaimState.adminUsers.length <= 0) {
    setHiddenAdminStatus("Users list is already empty.");
    return true;
  }
  setHiddenAdminUsersCleared(true);
  renderHiddenAdminUsers();
  setHiddenAdminStatus("Users view cleared.");
  return true;
}

function toggleHiddenAdminDevicesViewClear() {
  if (!venmoAdminUnlocked) {
    setHiddenAdminStatus("Unlock admin first.", true);
    return false;
  }
  if (hiddenAdminDevicesCleared) {
    setHiddenAdminDevicesCleared(false);
    renderHiddenAdminDevices();
    setHiddenAdminStatus("Trusted Devices view restored.");
    return true;
  }
  if (!Array.isArray(venmoClaimState.adminDevices) || venmoClaimState.adminDevices.length <= 0) {
    setHiddenAdminStatus("Trusted Devices list is already empty.");
    return true;
  }
  setHiddenAdminDevicesCleared(true);
  renderHiddenAdminDevices();
  setHiddenAdminStatus("Trusted Devices view cleared.");
  return true;
}

function toggleHiddenAdminFeedbackViewClear() {
  if (!venmoAdminUnlocked) {
    setHiddenAdminStatus("Unlock admin first.", true);
    return false;
  }
  if (hiddenAdminFeedbackCleared) {
    setHiddenAdminFeedbackCleared(false);
    renderHiddenAdminFeedback();
    setHiddenAdminStatus("Player Feedback view restored.");
    return true;
  }
  if (!Array.isArray(venmoClaimState.adminFeedback) || venmoClaimState.adminFeedback.length <= 0) {
    setHiddenAdminStatus("Player Feedback list is already empty.");
    return true;
  }
  setHiddenAdminFeedbackCleared(true);
  renderHiddenAdminFeedback();
  setHiddenAdminStatus("Player Feedback view cleared.");
  return true;
}

function toggleHiddenAdminBackupsViewClear() {
  if (!venmoAdminUnlocked) {
    setHiddenAdminStatus("Unlock admin first.", true);
    return false;
  }
  if (hiddenAdminBackupsCleared) {
    setHiddenAdminBackupsCleared(false);
    renderHiddenAdminBackups();
    setHiddenAdminStatus("Backups view restored.");
    return true;
  }
  if (!Array.isArray(venmoClaimState.adminBackups) || venmoClaimState.adminBackups.length <= 0) {
    setHiddenAdminStatus("Backups list is already empty.");
    return true;
  }
  setHiddenAdminBackupsCleared(true);
  renderHiddenAdminBackups();
  setHiddenAdminStatus("Backups view cleared.");
  return true;
}

async function clearHiddenAdminActivityFeed() {
  if (!venmoAdminUnlocked) {
    setHiddenAdminStatus("Unlock admin first.", true);
    return false;
  }
  const eventCount = Array.isArray(venmoClaimState.adminActivity) ? venmoClaimState.adminActivity.length : 0;
  if (eventCount <= 0) {
    setHiddenAdminStatus("Activity feed is already empty.");
    return true;
  }
  const confirmed = window.confirm("Clear all events in the Live Activity Feed?");
  if (!confirmed) return false;
  try {
    const payload = await venmoApiRequest(
      "/api/admin/activity/clear",
      getAdminRequestOptions({
        method: "POST"
      })
    );
    const clearedCount = Number(payload?.cleared?.count) || 0;
    venmoClaimState.adminActivity = [];
    renderHiddenAdminActivity();
    setHiddenAdminStatus(`Live Activity Feed cleared (${clearedCount} removed).`);
    return true;
  } catch (error) {
    setHiddenAdminStatus(`Could not clear activity feed: ${error.message}`, true);
    return false;
  }
}

async function clearHiddenAdminFraudFlags() {
  if (!venmoAdminUnlocked) {
    setHiddenAdminStatus("Unlock admin first.", true);
    return false;
  }
  const flagCount = Array.isArray(venmoClaimState.adminFraudFlags) ? venmoClaimState.adminFraudFlags.length : 0;
  if (flagCount <= 0) {
    setHiddenAdminStatus("Fraud flags are already clear.");
    return true;
  }
  const confirmed = window.confirm("Clear all current Fraud Flags?");
  if (!confirmed) return false;
  try {
    await venmoApiRequest(
      "/api/admin/fraud-flags/clear",
      getAdminRequestOptions({
        method: "POST"
      })
    );
    venmoClaimState.adminFraudFlags = [];
    renderHiddenAdminFraudFlags();
    setHiddenAdminStatus("Fraud flags cleared. New flags will appear as new activity is detected.");
    return true;
  } catch (error) {
    setHiddenAdminStatus(`Could not clear fraud flags: ${error.message}`, true);
    return false;
  }
}

async function runFullAdminReset() {
  if (!venmoAdminUnlocked) {
    setHiddenAdminStatus("Unlock admin first.", true);
    return false;
  }
  const code = String(venmoAdminCodeInputEl?.value || "").trim();
  if (!code) {
    setHiddenAdminStatus("Enter admin code before full reset.", true);
    if (venmoAdminCodeInputEl) venmoAdminCodeInputEl.focus();
    return false;
  }
  const confirmed = window.confirm(
    "Full reset will delete all users, claims, wins, and sessions except your current admin account. Continue?"
  );
  if (!confirmed) return false;
  const typed = window.prompt('Type "RESET EVERYTHING" to confirm full reset.');
  if (typed !== "RESET EVERYTHING") {
    setHiddenAdminStatus("Full reset cancelled: confirmation text did not match.", true);
    return false;
  }
  try {
    const payload = await venmoApiRequest(
      "/api/admin/system/reset",
      getAdminRequestOptions({
        method: "POST",
        body: { confirmText: typed, adminCode: code }
      })
    );
    venmoClaimState.adminClaims = [];
    venmoClaimState.adminUsers = [];
    venmoClaimState.adminStats = null;
    venmoClaimState.adminHealth = null;
    venmoClaimState.adminDevices = [];
    venmoClaimState.adminFeedback = [];
    venmoClaimState.adminActivity = [];
    venmoClaimState.adminFraudFlags = [];
    venmoClaimState.adminBackups = [];
    venmoClaimState.claims = [];
    renderVenmoAdminClaims();
    renderHiddenAdminUsers();
    renderHiddenAdminStats();
    renderHiddenAdminHealth();
    renderHiddenAdminDevices();
    renderHiddenAdminFeedback();
    renderHiddenAdminActivity();
    renderHiddenAdminFraudFlags();
    renderHiddenAdminBackups();
    await refreshHiddenAdminStatsFromServer({ silent: true });
    await refreshHiddenAdminHealthFromServer({ silent: true });
    await refreshHiddenAdminUsersFromServer({ silent: true });
    await refreshHiddenAdminDevicesFromServer({ silent: true });
    await refreshHiddenAdminFeedbackFromServer({ silent: true });
    await refreshHiddenAdminActivityFromServer({ silent: true });
    await refreshHiddenAdminFraudFlagsFromServer({ silent: true });
    await refreshHiddenAdminBackupsFromServer({ silent: true });
    const usersDeleted = Number(payload?.reset?.usersDeleted) || 0;
    const claimsDeleted = Number(payload?.reset?.claimsDeleted) || 0;
    setHiddenAdminStatus(`Full reset complete. Users removed: ${usersDeleted}, claims removed: ${claimsDeleted}. Reloading...`);
    setBankMessage("Full game reset completed. Reloading...");
    applyProgressResetState();
    updateUI();
    await syncResetBalanceToServer();
    clearTotalProgressStorage();
    setTimeout(() => {
      window.location.reload();
    }, 150);
    return true;
  } catch (error) {
    setHiddenAdminStatus(`Could not complete full reset: ${error.message}`, true);
    return false;
  }
}

function closeHiddenAdminPanel() {
  if (!hiddenAdminOverlayEl) return;
  hiddenAdminOverlayEl.classList.add("hidden");
  hiddenAdminOverlayEl.setAttribute("aria-hidden", "true");
  hiddenAdminPanelOpen = false;
  if (hiddenAdminLivePollTimer) {
    clearInterval(hiddenAdminLivePollTimer);
    hiddenAdminLivePollTimer = null;
  }
  hiddenAdminLivePollTick = 0;
}

function startHiddenAdminLivePolling() {
  if (hiddenAdminLivePollTimer) return;
  hiddenAdminLivePollTimer = setInterval(() => {
    if (!hiddenAdminPanelOpen) {
      clearInterval(hiddenAdminLivePollTimer);
      hiddenAdminLivePollTimer = null;
      return;
    }
    if (!venmoAdminUnlocked) return;
    hiddenAdminLivePollTick += 1;
    refreshHiddenAdminUsersFromServer({ silent: true });
    refreshHiddenAdminStatsFromServer({ silent: true });
    refreshHiddenAdminHealthFromServer({ silent: true });
    refreshHiddenAdminDevicesFromServer({ silent: true });
    refreshHiddenAdminFeedbackFromServer({ silent: true });
    refreshHiddenAdminActivityFromServer({ silent: true });
    refreshHiddenAdminFraudFlagsFromServer({ silent: true });
    if (hiddenAdminLivePollTick % 15 === 0) {
      refreshHiddenAdminBackupsFromServer({ silent: true });
    }
  }, HIDDEN_ADMIN_LIVE_POLL_MS);
}

function syncHiddenAdminTriggerVisibility() {
  if (!hiddenAdminTriggerEl) return;
  const tradingRoot = document.getElementById("trading-section");
  const tradingVisible = Boolean(tradingRoot && tradingRoot.style.display !== "none");
  const shouldShow = tradingVisible && !usernameGateActive && !IS_PHONE_EMBED_MODE;
  hiddenAdminTriggerEl.style.display = shouldShow ? "block" : "none";
  hiddenAdminTriggerEl.style.pointerEvents = shouldShow ? "auto" : "none";
  if (!shouldShow && hiddenAdminPanelOpen) closeHiddenAdminPanel();
}

async function openHiddenAdminPanel() {
  if (!hiddenAdminOverlayEl) return;
  hiddenAdminOverlayEl.classList.remove("hidden");
  hiddenAdminOverlayEl.setAttribute("aria-hidden", "false");
  hiddenAdminPanelOpen = true;
  await tryAutoUnlockAdminFromTrustedDevice();
  setHiddenAdminStatus(
    venmoAdminUnlocked ? "Admin unlocked. Device verified." : "Enter admin code to unlock this device."
  );
  renderVenmoAdminClaims();
  renderHiddenAdminStats();
  renderHiddenAdminHealth();
  renderHiddenAdminUsers();
  renderHiddenAdminDevices();
  renderHiddenAdminFeedback();
  renderHiddenAdminActivity();
  renderHiddenAdminFraudFlags();
  renderHiddenAdminBackups();
  if (venmoAdminUnlocked) {
    await refreshVenmoAdminClaimsFromServer({ silent: true });
    await refreshHiddenAdminStatsFromServer({ silent: true });
    await refreshHiddenAdminHealthFromServer({ silent: true });
    await refreshHiddenAdminUsersFromServer({ silent: true });
    await refreshHiddenAdminDevicesFromServer({ silent: true });
    await refreshHiddenAdminFeedbackFromServer({ silent: true });
    await refreshHiddenAdminActivityFromServer({ silent: true });
    await refreshHiddenAdminFraudFlagsFromServer({ silent: true });
    await refreshHiddenAdminBackupsFromServer({ silent: true });
    setHiddenAdminStatus("Admin unlocked. Device verified.");
  }
  startHiddenAdminLivePolling();
}

function initHiddenAdminTrigger() {
  if (!hiddenAdminTriggerEl || hiddenAdminTriggerEl.dataset.bound === "true") return;
  hiddenAdminTriggerEl.dataset.bound = "true";
  hiddenAdminTriggerEl.addEventListener("click", (event) => {
    event.preventDefault();
    event.stopPropagation();
    openHiddenAdminPanel();
  });
  if (hiddenAdminCloseBtnEl) {
    hiddenAdminCloseBtnEl.addEventListener("click", () => closeHiddenAdminPanel());
  }
  if (venmoAdminTrustDeviceBtn) {
    venmoAdminTrustDeviceBtn.addEventListener("click", async () => {
      const typedCode = String(venmoAdminCodeInputEl?.value || "").trim();
      if (!typedCode) {
        setHiddenAdminStatus("Enter admin code first.", true);
        return;
      }
      const ok = await trustCurrentAdminDevice({ code: typedCode });
      if (!ok) return;
      await refreshHiddenAdminDevicesFromServer({ silent: true });
      setHiddenAdminStatus("This device is trusted for admin access.");
    });
  }
  if (hiddenAdminFullResetBtn) {
    hiddenAdminFullResetBtn.addEventListener("click", () => {
      runFullAdminReset();
    });
  }
  if (hiddenAdminClaimsCollapseBtn) {
    hiddenAdminClaimsCollapseBtn.addEventListener("click", () => {
      setHiddenAdminClaimsCollapsed(!hiddenAdminClaimsCollapsed);
      if (!hiddenAdminClaimsCollapsed) renderVenmoAdminClaims();
    });
  }
  if (hiddenAdminClaimsClearBtn) {
    hiddenAdminClaimsClearBtn.addEventListener("click", () => {
      toggleHiddenAdminClaimsViewClear();
    });
  }
  if (hiddenAdminStatsCollapseBtn) {
    hiddenAdminStatsCollapseBtn.addEventListener("click", () => {
      setHiddenAdminStatsCollapsed(!hiddenAdminStatsCollapsed);
      if (!hiddenAdminStatsCollapsed) renderHiddenAdminStats();
    });
  }
  if (hiddenAdminHealthCollapseBtn) {
    hiddenAdminHealthCollapseBtn.addEventListener("click", () => {
      setHiddenAdminHealthCollapsed(!hiddenAdminHealthCollapsed);
      if (!hiddenAdminHealthCollapsed) renderHiddenAdminHealth();
    });
  }
  if (hiddenAdminUsersCollapseBtn) {
    hiddenAdminUsersCollapseBtn.addEventListener("click", () => {
      setHiddenAdminUsersCollapsed(!hiddenAdminUsersCollapsed);
      if (!hiddenAdminUsersCollapsed) renderHiddenAdminUsers();
    });
  }
  if (hiddenAdminUsersClearBtn) {
    hiddenAdminUsersClearBtn.addEventListener("click", () => {
      toggleHiddenAdminUsersViewClear();
    });
  }
  if (hiddenAdminDevicesCollapseBtn) {
    hiddenAdminDevicesCollapseBtn.addEventListener("click", () => {
      setHiddenAdminDevicesCollapsed(!hiddenAdminDevicesCollapsed);
      if (!hiddenAdminDevicesCollapsed) renderHiddenAdminDevices();
    });
  }
  if (hiddenAdminDevicesClearBtn) {
    hiddenAdminDevicesClearBtn.addEventListener("click", () => {
      toggleHiddenAdminDevicesViewClear();
    });
  }
  if (hiddenAdminFeedbackCollapseBtn) {
    hiddenAdminFeedbackCollapseBtn.addEventListener("click", () => {
      setHiddenAdminFeedbackCollapsed(!hiddenAdminFeedbackCollapsed);
      if (!hiddenAdminFeedbackCollapsed) renderHiddenAdminFeedback();
    });
  }
  if (hiddenAdminFeedbackClearBtn) {
    hiddenAdminFeedbackClearBtn.addEventListener("click", () => {
      toggleHiddenAdminFeedbackViewClear();
    });
  }
  if (hiddenAdminBackupsCollapseBtn) {
    hiddenAdminBackupsCollapseBtn.addEventListener("click", () => {
      setHiddenAdminBackupsCollapsed(!hiddenAdminBackupsCollapsed);
      if (!hiddenAdminBackupsCollapsed) renderHiddenAdminBackups();
    });
  }
  if (hiddenAdminBackupsClearBtn) {
    hiddenAdminBackupsClearBtn.addEventListener("click", () => {
      toggleHiddenAdminBackupsViewClear();
    });
  }
  if (hiddenAdminActivityCollapseBtn) {
    hiddenAdminActivityCollapseBtn.addEventListener("click", () => {
      setHiddenAdminActivityCollapsed(!hiddenAdminActivityCollapsed);
      if (!hiddenAdminActivityCollapsed) renderHiddenAdminActivity();
    });
  }
  if (hiddenAdminActivityClearBtn) {
    hiddenAdminActivityClearBtn.addEventListener("click", () => {
      clearHiddenAdminActivityFeed();
    });
  }
  if (hiddenAdminFraudCollapseBtn) {
    hiddenAdminFraudCollapseBtn.addEventListener("click", () => {
      setHiddenAdminFraudCollapsed(!hiddenAdminFraudCollapsed);
      if (!hiddenAdminFraudCollapsed) renderHiddenAdminFraudFlags();
    });
  }
  if (hiddenAdminFraudClearBtn) {
    hiddenAdminFraudClearBtn.addEventListener("click", () => {
      clearHiddenAdminFraudFlags();
    });
  }
  if (hiddenAdminOverlayEl) {
    hiddenAdminOverlayEl.addEventListener("click", (event) => {
      if (event.target === hiddenAdminOverlayEl) closeHiddenAdminPanel();
    });
  }
  if (hiddenAdminUsersEl) {
    hiddenAdminUsersEl.addEventListener("click", (event) => {
      const target = event.target instanceof Element
        ? event.target.closest("button[data-user-action][data-player-id]")
        : null;
      if (!target) return;
      const action = String(target.getAttribute("data-user-action") || "");
      const playerId = String(target.getAttribute("data-player-id") || "");
      if (!action || !playerId) return;
      if (action === "set-balance") {
        const currentBalance = String(target.getAttribute("data-current-balance") || "");
        setAdminUserBalance(playerId, currentBalance);
      }
      if (action === "reset-progress") resetAdminUserProgress(playerId);
      if (action === "force-logout") forceLogoutAdminUser(playerId);
      if (action === "mute") muteAdminUser(playerId);
      if (action === "unmute") unmuteAdminUser(playerId);
      if (action === "ban") banAdminUser(playerId);
      if (action === "unban") unbanAdminUser(playerId);
      if (action === "remove") removeAdminUser(playerId);
    });
  }
  if (hiddenAdminFeedbackEl) {
    hiddenAdminFeedbackEl.addEventListener("click", (event) => {
      const target = event.target instanceof Element
        ? event.target.closest("button[data-feedback-action][data-feedback-id]")
        : null;
      if (!target) return;
      const action = String(target.getAttribute("data-feedback-action") || "");
      const feedbackId = String(target.getAttribute("data-feedback-id") || "");
      if (!action || !feedbackId) return;
      if (action === "remove") removeAdminFeedback(feedbackId);
    });
  }
  if (hiddenAdminCreateBackupBtn) {
    hiddenAdminCreateBackupBtn.addEventListener("click", () => {
      createAdminBackup();
    });
  }
  if (hiddenAdminBackupsEl) {
    hiddenAdminBackupsEl.addEventListener("click", (event) => {
      const target = event.target instanceof Element
        ? event.target.closest("button[data-backup-action][data-backup-id]")
        : null;
      if (!target) return;
      const action = String(target.getAttribute("data-backup-action") || "");
      const backupId = String(target.getAttribute("data-backup-id") || "");
      if (!action || !backupId) return;
      if (action === "restore") restoreAdminBackup(backupId);
    });
  }
  if (hiddenAdminDevicesEl) {
    hiddenAdminDevicesEl.addEventListener("click", (event) => {
      const target = event.target instanceof Element
        ? event.target.closest("button[data-device-action][data-device-id]")
        : null;
      if (!target) return;
      const action = String(target.getAttribute("data-device-action") || "");
      const deviceId = String(target.getAttribute("data-device-id") || "");
      if (!action || !deviceId) return;
      if (action === "ban") banAdminDevice(deviceId);
      if (action === "unban") unbanAdminDevice(deviceId);
      if (action === "remove") removeAdminDevice(deviceId);
    });
  }
  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape" || !hiddenAdminPanelOpen) return;
    closeHiddenAdminPanel();
  });
  syncHiddenAdminCoreSectionControls();
  syncHiddenAdminActivityControls();
  syncHiddenAdminFraudControls();
  syncHiddenAdminTriggerVisibility();
}

async function submitVenmoClaim() {
  const packId = String(venmoClaimPackEl?.value || "");
  const pack = REAL_MONEY_FUND_PACKS[packId];
  if (!pack) {
    const message = "Select a valid funds pack.";
    setBankMessage(message);
    setVenmoClaimInlineStatus(message, true);
    return;
  }

  const txnId = String(venmoClaimTxnInputEl?.value || "").trim();
  const txnNorm = normalizeVenmoTxnId(txnId);
  if (txnNorm.length < 4) {
    const message = "Enter a valid Venmo transaction ID.";
    setBankMessage(message);
    setVenmoClaimInlineStatus(message, true);
    return;
  }

  const duplicate = venmoClaimState.claims.some((claim) => claim.txnNorm === txnNorm && claim.status !== "rejected");
  if (duplicate) {
    const message = "That Venmo transaction ID already has a claim.";
    setBankMessage(message);
    setVenmoClaimInlineStatus(message, true);
    return;
  }

  try {
    if (venmoClaimSubmitBtn) venmoClaimSubmitBtn.disabled = true;
    setVenmoClaimInlineStatus("Submitting claim...");
    await venmoApiRequest("/api/claims", {
      method: "POST",
      body: {
        playerId: venmoClaimPlayerId,
        username: playerUsername || "Player",
        source: "venmo",
        packageId: packId,
        packId,
        txnId
      }
    });
    if (venmoClaimTxnInputEl) venmoClaimTxnInputEl.value = "";
    await refreshVenmoClaimsFromServer({ silent: true });
    if (venmoAdminUnlocked) await refreshVenmoAdminClaimsFromServer({ silent: true });
    const message = "Claim submitted. Waiting for admin approval.";
    setBankMessage(message);
    setVenmoClaimInlineStatus(message);
  } catch (error) {
    const message = `Claim submit failed: ${error.message}`;
    setBankMessage(message);
    setVenmoClaimInlineStatus(message, true);
  } finally {
    if (venmoClaimSubmitBtn) venmoClaimSubmitBtn.disabled = false;
  }
}

async function resolveVenmoClaim(claimId, decision) {
  if (!claimId || !venmoAdminUnlocked) return;
  try {
    await venmoApiRequest(`/api/admin/claims/${encodeURIComponent(claimId)}/decision`, getAdminRequestOptions({
      method: "POST",
      body: { decision }
    }));
    await refreshVenmoAdminClaimsFromServer({ silent: true });
    await refreshHiddenAdminStatsFromServer({ silent: true });
    await refreshHiddenAdminUsersFromServer({ silent: true });
    await refreshHiddenAdminActivityFromServer({ silent: true });
    await refreshHiddenAdminFraudFlagsFromServer({ silent: true });
    await refreshVenmoClaimsFromServer({ silent: true });
    setBankMessage(decision === "approve" ? "Claim approved." : "Claim rejected.");
  } catch (error) {
    setBankMessage(`Claim update failed: ${error.message}`);
  }
}

async function claimApprovedVenmoCredits({ silent = true } = {}) {
  if (!venmoClaimPlayerId) return;
  try {
    const payload = await venmoApiRequest(`/api/claims/credits?playerId=${encodeURIComponent(venmoClaimPlayerId)}`);
    const credits = Array.isArray(payload?.claims) ? payload.claims : [];
    let creditedAny = false;
    let creditedTotal = 0;
    for (const claim of credits) {
      const claimId = String(claim?.id || "").trim();
      if (!claimId) continue;
      try {
        const ackPayload = await venmoApiRequest(`/api/claims/credits/${encodeURIComponent(claimId)}/ack`, {
          method: "POST",
          body: { playerId: venmoClaimPlayerId }
        });
        const creditedNow = ackPayload?.creditedNow === true;
        const creditedAmount = Number(ackPayload?.creditedAmount);
        if (ackPayload?.user) {
          const beforeSig = getPersistProfileSignature();
          applyServerPortfolioSnapshot(ackPayload.user, { useServerBalance: true });
          const afterSig = getPersistProfileSignature();
          if (afterSig !== beforeSig) updateUI();
        }
        if (creditedNow && Number.isFinite(creditedAmount) && creditedAmount > 0) {
          creditedAny = true;
          creditedTotal = roundCurrency(creditedTotal + creditedAmount);
        }
        venmoLocallyCreditedClaimIds.add(claimId);
        saveVenmoLocalCreditedClaimIds();
      } catch (ackError) {
        if (!silent) setBankMessage(`Credit ack pending: ${ackError.message}`);
      }
    }
    if (creditedAny) {
      setBankMessage(`Claim credited: ${formatCurrency(creditedTotal)} added to savings.`);
    }
    if (credits.length > 0) {
      await refreshVenmoClaimsFromServer({ silent: true });
      await syncCurrentUserProfileToServer({ force: true });
    }
  } catch (error) {
    if (!silent) setBankMessage(`Credit check failed: ${error.message}`);
  }
}

function initVenmoClaimWorkflow() {
  venmoAdminDeviceToken = loadOrCreateAdminDeviceToken();
  venmoClaimPlayerId = getVenmoClaimPlayerId();
  loadVenmoLocalCreditedClaimIds();
  syncCurrentUserProfileToServer({ force: true });
  renderVenmoClaimStatus();
  renderVenmoAdminClaims();
  renderHiddenAdminStats();
  renderHiddenAdminHealth();
  renderHiddenAdminUsers();
  renderHiddenAdminDevices();
  renderHiddenAdminFeedback();
  renderHiddenAdminActivity();
  renderHiddenAdminFraudFlags();
  renderHiddenAdminBackups();
  setHiddenAdminStatus("");
  refreshVenmoClaimsFromServer({ silent: true });
  claimApprovedVenmoCredits({ silent: true });
  if (venmoClaimPollTimer) {
    clearInterval(venmoClaimPollTimer);
    venmoClaimPollTimer = null;
  }
  venmoClaimPollTimer = setInterval(() => {
    refreshVenmoClaimsFromServer({ silent: true });
    claimApprovedVenmoCredits({ silent: true });
    if (venmoAdminUnlocked) {
      refreshVenmoAdminClaimsFromServer({ silent: true });
      refreshHiddenAdminStatsFromServer({ silent: true });
      refreshHiddenAdminHealthFromServer({ silent: true });
      refreshHiddenAdminUsersFromServer({ silent: true });
      refreshHiddenAdminDevicesFromServer({ silent: true });
      refreshHiddenAdminFeedbackFromServer({ silent: true });
      refreshHiddenAdminActivityFromServer({ silent: true });
      refreshHiddenAdminFraudFlagsFromServer({ silent: true });
      refreshHiddenAdminBackupsFromServer({ silent: true });
    }
  }, VENMO_CLAIM_POLL_MS);
}

async function promptVenmoAdminUnlock(forcePrompt = false) {
  const typedCode = String(venmoAdminCodeInputEl?.value || "").trim();
  let code = typedCode;
  if (forcePrompt && !typedCode) code = "";
  if (!code) {
    const autoUnlocked = await tryAutoUnlockAdminFromTrustedDevice();
    if (autoUnlocked) return true;
    setHiddenAdminStatus("Enter admin code to unlock this device.", true);
    return false;
  }
  const trusted = await trustCurrentAdminDevice({ silent: false, code });
  if (!trusted) {
    venmoAdminUnlocked = false;
    setBankMessage("Admin unlock failed.");
    return false;
  }
  venmoAdminUnlocked = true;
  const ok = await refreshVenmoAdminClaimsFromServer({ silent: true });
  if (!ok) {
    venmoAdminUnlocked = false;
    if (hiddenAdminLivePollTimer) {
      clearInterval(hiddenAdminLivePollTimer);
      hiddenAdminLivePollTimer = null;
    }
    setBankMessage("Admin unlock failed.");
    setHiddenAdminStatus("Unlock failed. Check admin code and trusted device.", true);
    renderHiddenAdminStats();
    renderHiddenAdminHealth();
    renderHiddenAdminUsers();
    renderHiddenAdminDevices();
    renderHiddenAdminFeedback();
    renderHiddenAdminActivity();
    renderHiddenAdminFraudFlags();
    renderHiddenAdminBackups();
    return false;
  }
  await refreshHiddenAdminStatsFromServer({ silent: true });
  await refreshHiddenAdminHealthFromServer({ silent: true });
  await refreshHiddenAdminUsersFromServer({ silent: true });
  await refreshHiddenAdminDevicesFromServer({ silent: true });
  await refreshHiddenAdminFeedbackFromServer({ silent: true });
  await refreshHiddenAdminActivityFromServer({ silent: true });
  await refreshHiddenAdminFraudFlagsFromServer({ silent: true });
  await refreshHiddenAdminBackupsFromServer({ silent: true });
  if (venmoAdminCodeInputEl) venmoAdminCodeInputEl.value = "";
  setHiddenAdminStatus("Admin unlocked. Device verified.");
  if (hiddenAdminPanelOpen) startHiddenAdminLivePolling();
  setBankMessage("Admin claim panel unlocked.");
  return true;
}

function maybeAutoSaveFromCasinoWin(winAmountOrOptions) {
  const options =
    typeof winAmountOrOptions === "object" && winAmountOrOptions !== null
      ? winAmountOrOptions
      : { amount: winAmountOrOptions };
  const winAmount = Number(options.amount);
  const source = String(options.source || "").toLowerCase();
  const multiplier = Number(options.multiplier);

  const configuredPercent = clampPercent(autoSavingsPercent);
  if (configuredPercent <= 0) return 0;
  if (!Number.isFinite(winAmount) || winAmount <= 0.009) return 0;
  if (source === "plinko" && (!Number.isFinite(multiplier) || multiplier < 2)) return 0;

  const requested = roundCurrency(winAmount * (configuredPercent / 100));
  const moved = depositSavings(requested, { profitOnly: false });
  if (moved > 0) {
    pushPhoneBankHistory("Auto-save from casino win", moved, "neutral");
    setBankMessage(`Auto-saved ${formatCurrency(moved)} (${configuredPercent.toFixed(1)}%) from win.`);
  }
  return moved;
}

function processTakeLoan(rawAmount) {
  if (!LOANS_ENABLED) {
    setBankMessage("Loans are disabled.");
    return false;
  }
  const amount = Number(rawAmount);
  if (!Number.isFinite(amount) || amount <= 0) {
    setBankMessage("Enter a valid loan amount.");
    return false;
  }
  if (blackMark) {
    trackBankMissionLoanBlocked();
    setBankMessage("Loan denied due to bad credit.");
    return false;
  }
  loanPrincipal += amount;
  cash += amount;
  loanAge = 0;
  achievementState.stats.loansTaken += 1;
  saveAchievements();
  pushPhoneBankHistory("Loan funded", amount, "positive");
  trackBankMissionLoanTaken(amount);
  updateUI();
  updateLoanUI();
  setBankMessage(`Loan funded: ${formatCurrency(amount)}.`);
  return true;
}

function processRepayLoan() {
  if (!LOANS_ENABLED) {
    setBankMessage("Loans are disabled.");
    return 0;
  }
  const previousInterest = loanInterest;
  let payment = Math.min(cash, loanPrincipal + loanInterest);
  const originalPayment = payment;
  if (originalPayment <= 0) {
    setBankMessage("No payment could be made.");
    return 0;
  }
  cash -= payment;

  if (payment >= loanInterest) {
    payment -= loanInterest;
    loanInterest = 0;
    loanPrincipal -= payment;
  } else {
    loanInterest -= payment;
  }

  if (loanPrincipal <= 0) {
    loanPrincipal = 0;
    loanInterest = 0;
    blackMark = false;
  }

  const interestPaid = Math.max(0, Math.min(previousInterest, originalPayment));
  const fullyCleared = getTotalDebt() <= 0;
  trackBankMissionLoanRepaid(interestPaid, fullyCleared);

  pushPhoneBankHistory("Loan payment", -originalPayment, "negative");
  updateUI();
  updateLoanUI();
  setBankMessage(`Loan payment made: ${formatCurrency(originalPayment)}.`);
  return originalPayment;
}

if (takeLoanBtnEl) {
  takeLoanBtnEl.onclick = () => {
    const amount = Number(loanInputEl?.value);
    processTakeLoan(amount);
  };
}

if (repayLoanBtnEl) {
  repayLoanBtnEl.onclick = () => {
    processRepayLoan();
  };
}

if (withdrawAllSavingsBtn) {
  withdrawAllSavingsBtn.onclick = () => {
    if (savingsBalance <= 0) {
      setBankMessage("Savings balance is empty.");
      return;
    }

    const withdrawn = withdrawSavings(savingsBalance);
    setBankMessage(`Withdrew ${formatCurrency(withdrawn)} from savings.`);
  };
}

if (withdrawSavingsBtn) {
  withdrawSavingsBtn.onclick = () => {
    processSavingsWithdraw(withdrawSavingsInputEl?.value);
  };
}
if (withdrawSavingsInputEl) {
  withdrawSavingsInputEl.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    processSavingsWithdraw(withdrawSavingsInputEl.value);
  });
}

if (buyFundsSmallBtn) {
  buyFundsSmallBtn.onclick = () => {
    beginRealMoneyFundsCheckout("small");
  };
}
if (buyFundsMediumBtn) {
  buyFundsMediumBtn.onclick = () => {
    beginRealMoneyFundsCheckout("medium");
  };
}
if (buyFundsXLBtn) {
  buyFundsXLBtn.onclick = () => {
    beginRealMoneyFundsCheckout("xlarge");
  };
}
if (buyFundsLargeBtn) {
  buyFundsLargeBtn.onclick = () => {
    beginRealMoneyFundsCheckout("large");
  };
}
if (venmoClaimSubmitBtn) {
  venmoClaimSubmitBtn.onclick = () => {
    submitVenmoClaim();
  };
}
if (venmoClaimTxnInputEl) {
  venmoClaimTxnInputEl.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    submitVenmoClaim();
  });
}
if (venmoAdminUnlockBtn) {
  venmoAdminUnlockBtn.onclick = () => {
    promptVenmoAdminUnlock();
  };
}
if (venmoAdminCodeInputEl) {
  venmoAdminCodeInputEl.addEventListener("keydown", (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    promptVenmoAdminUnlock();
  });
}
if (venmoAdminClaimsEl) {
  venmoAdminClaimsEl.addEventListener("click", (event) => {
    if (!venmoAdminUnlocked) {
      setHiddenAdminStatus("Unlock admin access first.", true);
      return;
    }
    const target = event.target instanceof Element ? event.target.closest("button[data-action][data-claim-id]") : null;
    if (!target) return;
    const action = String(target.getAttribute("data-action") || "");
    const claimId = String(target.getAttribute("data-claim-id") || "");
    if (!claimId) return;
    if (action === "approve") resolveVenmoClaim(claimId, "approve");
    if (action === "reject") resolveVenmoClaim(claimId, "reject");
  });
}
if (buyVipBtn) {
  buyVipBtn.onclick = () => {
    if (phoneState.vip?.active) {
      setBankMessage("VIP already active.");
      return;
    }
    if (cash < VIP_COST) {
      setBankMessage(`Need ${formatCurrency(VIP_COST)} cash to buy VIP.`);
      return;
    }
    cash = roundCurrency(cash - VIP_COST);
    phoneState.vip = {
      active: true,
      purchasedAt: Date.now(),
      lastWeeklyBonusAt: 0
    };
    applyVipWeeklyBonusIfDue();
    pushPhoneNotification("achievement", "VIP unlocked");
    setBankMessage("VIP purchased. Gold name + weekly bonus enabled.");
    updateUI();
    savePhoneState();
  };
}

if (setAutoSavingsBtn) {
  setAutoSavingsBtn.onclick = () => {
    const percent = getAutoSavingsInput();
    if (percent <= 0) {
      autoSavingsPercent = 0;
      if (autoSavingsInputEl) autoSavingsInputEl.value = "";
      if (phoneUi.bankAutoInput) phoneUi.bankAutoInput.value = "";
      setBankMessage("Auto-save turned off.");
      savePhoneState();
      return;
    }

    autoSavingsPercent = percent;
    if (autoSavingsInputEl) autoSavingsInputEl.value = percent.toFixed(1);
    if (phoneUi.bankAutoInput) phoneUi.bankAutoInput.value = percent.toFixed(1);
    setBankMessage(`Auto-save set to ${percent.toFixed(1)}% of each win.`);
    savePhoneState();
  };
}

if (clearAutoSavingsBtn) {
  clearAutoSavingsBtn.onclick = () => {
    autoSavingsPercent = 0;
    if (autoSavingsInputEl) autoSavingsInputEl.value = "";
    if (phoneUi.bankAutoInput) phoneUi.bankAutoInput.value = "";
    setBankMessage("Auto-save turned off.");
    savePhoneState();
  };
}

initVenmoClaimWorkflow();


function applyInterest() {
  if (!LOANS_ENABLED) {
    loanPrincipal = 0;
    loanInterest = 0;
    loanAge = 0;
    blackMark = false;
    return;
  }
  if (loanPrincipal > 0) {
    loanInterest += loanPrincipal * (blackMark ? INTEREST_RATE * 2 : INTEREST_RATE);
    loanAge++;
    if (loanAge > DEFAULT_LIMIT) blackMark = true;
  }

  if (blackMark && Math.random() < 0.1) {
    cash = Math.max(0, cash - loanPrincipal * 0.05);
  }
}

// ================= BANK PANEL TOGGLE =================
const bankPanel = document.getElementById("loan-panel");
const bankToggleBtn = document.getElementById("bankToggleBtn");
const closeBankBtn = document.getElementById("closeBankBtn");

function toggleBank() {
  bankPanel.classList.toggle("open");
}

bankToggleBtn.onclick = toggleBank;
closeBankBtn.onclick = toggleBank;

// =====================================================
// =================== CASINO ==========================
// =====================================================
document.querySelectorAll(".casino-game-btn").forEach((btn) => {
  btn.onclick = () => loadCasinoGame(btn.dataset.game);
});

function loadCasinoGame(game) {
  syncHiddenAdminTriggerVisibility();
  activeCasinoGameKey = String(game || "lobby").toLowerCase();
  syncCasinoLiveFeedVisibility();
  const gate = getCasinoGateState();
  if (!gate.ok) {
    alert(gate.message);
    return;
  }
  if (IS_PHONE_EMBED_MODE) {
    resetPhoneEmbeddedUi();
  }
  const casinoContainer = document.getElementById("casino-container");
  if (casinoContainer) {
    casinoContainer.classList.remove(
      "casino-fullbleed",
      "slots-fullbleed",
      "blackjack-fullbleed",
      "poker-fullbleed",
      "poker-plinko-fullbleed",
      "plinko-fullbleed",
      "plinko-mini-fullbleed",
      "dragon-fullbleed",
      "crossy-fullbleed",
      "roulette-fullbleed",
      "casino-roulette-classic-fullbleed",
      "dice-fullbleed",
      "slide-fullbleed",
      "diamond-fullbleed",
      "mines-fullbleed",
      "keno-fullbleed",
      "horse-fullbleed"
    );
  }
  cleanupActiveCasinoGame();

  if (game === "slots") {
    loadSlots();
    enterCasinoGameView("Slots");
  }
  if (game === "blackjack") {
    loadBlackjack();
    enterCasinoGameView("Blackjack");
  }
  if (game === "poker") {
    if (IS_PHONE_EMBED_MODE) loadAppPoker();
    else loadCasinoPoker();
    enterCasinoGameView("Poker");
  }
  if (game === "hilo") {
    loadHiLo();
    enterCasinoGameView("Hi-Lo");
  }
  if (game === "horseracing") {
    loadHorseRacing();
    enterCasinoGameView("Horse Racing");
  }
  if (game === "dragontower") {
    loadDragonTower();
    enterCasinoGameView("Dragon Tower");
  }
  if (game === "keno") {
    loadKeno();
    enterCasinoGameView("Keno");
  }
  if (game === "dice") {
    loadDice();
    enterCasinoGameView("Dice");
  }
  if (game === "slide") {
    loadSlide();
    enterCasinoGameView("Slide");
  }
  if (game === "crash") {
    loadCrash();
    enterCasinoGameView("Crash");
  }
  if (game === "mines") {
    loadMines();
    enterCasinoGameView("Mines");
  }
  if (game === "crossyroad") {
    loadCrossyRoad();
    enterCasinoGameView("Crossy Road");
  }
  if (game === "roulette") {
    if (IS_PHONE_EMBED_MODE) loadRoulette();
    else loadCasinoRouletteClassic();
    enterCasinoGameView("Roulette");
  }
  if (game === "diamonds") {
    loadDiamondMatch();
    enterCasinoGameView("Diamond Match");
  }
  if (game === "plinko") {
    if (IS_PHONE_EMBED_MODE) loadPlinkoPhoneApp();
    else loadCasinoPlinkoNeon();
    enterCasinoGameView("Plinko");
  }

  if (IS_PHONE_EMBED_MODE) {
    schedulePhoneEmbeddedGameEnhancement(game);
  }
}

let phoneEmbedEnhanceRaf = null;
let phoneEmbedEnhanceTimer = null;
let phoneEmbedResizeBound = false;
const phoneEmbedUiState = {
  gameKey: "",
  root: null,
  movedNodes: [],
  syncTimer: null,
  resizeObserver: null,
  sheetOpen: false,
  crashCanvasRaf: null,
  crashCanvasTimer: null
};

function getPhoneEmbeddedGameTitle(gameKey) {
  const key = String(gameKey || "").toLowerCase();
  return PHONE_EMBED_GAME_TITLES[key] || "Casino";
}

function setPhoneCasinoSheetOpen(open) {
  const sheet = document.getElementById("phoneCasinoSheet");
  const backdrop = document.getElementById("phoneCasinoSheetBackdrop");
  if (!sheet || !backdrop) return;
  const shouldOpen = Boolean(open);
  sheet.classList.toggle("open", shouldOpen);
  sheet.setAttribute("aria-hidden", shouldOpen ? "false" : "true");
  backdrop.hidden = !shouldOpen;
  phoneEmbedUiState.sheetOpen = shouldOpen;
}

function resetPhoneEmbeddedUi() {
  if (phoneEmbedEnhanceRaf) {
    cancelAnimationFrame(phoneEmbedEnhanceRaf);
    phoneEmbedEnhanceRaf = null;
  }
  if (phoneEmbedEnhanceTimer) {
    clearTimeout(phoneEmbedEnhanceTimer);
    phoneEmbedEnhanceTimer = null;
  }
  if (phoneEmbedUiState.syncTimer) {
    clearInterval(phoneEmbedUiState.syncTimer);
    phoneEmbedUiState.syncTimer = null;
  }
  if (phoneEmbedUiState.crashCanvasRaf) {
    cancelAnimationFrame(phoneEmbedUiState.crashCanvasRaf);
    phoneEmbedUiState.crashCanvasRaf = null;
  }
  if (phoneEmbedUiState.crashCanvasTimer) {
    clearTimeout(phoneEmbedUiState.crashCanvasTimer);
    phoneEmbedUiState.crashCanvasTimer = null;
  }
  if (phoneEmbedUiState.resizeObserver) {
    phoneEmbedUiState.resizeObserver.disconnect();
    phoneEmbedUiState.resizeObserver = null;
  }

  for (let i = phoneEmbedUiState.movedNodes.length - 1; i >= 0; i -= 1) {
    const item = phoneEmbedUiState.movedNodes[i];
    if (!item || !item.node) continue;
    if (item.parent && item.parent.isConnected) {
      if (item.nextSibling && item.nextSibling.parentNode === item.parent) {
        item.parent.insertBefore(item.node, item.nextSibling);
      } else {
        item.parent.appendChild(item.node);
      }
    } else if (item.node.parentNode) {
      item.node.parentNode.removeChild(item.node);
    }
  }
  phoneEmbedUiState.movedNodes = [];

  const sheetBody = document.getElementById("phoneCasinoSheetBody");
  if (sheetBody) sheetBody.innerHTML = "";
  const shell = document.querySelector(".phone-casino-shell");
  if (shell) shell.classList.remove("phone-casino-shell-roulette", "phone-casino-shell-plinko-native");
  document.querySelectorAll("#casino-section .phone-casino-outside-brand").forEach((node) => node.remove());
  const actionBar = document.getElementById("phoneCasinoActionbar");
  if (actionBar) {
    actionBar.hidden = false;
    const primaryBtn = actionBar.querySelector(".phone-action-primary");
    const secondaryBtn = actionBar.querySelector(".phone-action-secondary");
    const betInput = actionBar.querySelector(".phone-action-bet");
    if (primaryBtn) primaryBtn.onclick = null;
    if (secondaryBtn) secondaryBtn.onclick = null;
    if (betInput) {
      betInput.oninput = null;
      betInput.onchange = null;
    }
  }

  setPhoneCasinoSheetOpen(false);
  phoneEmbedUiState.root = null;
  phoneEmbedUiState.gameKey = "";
}

function ensurePhoneEmbeddedShell(gameKey = PHONE_EMBED_GAME_PARAM) {
  const casinoSection = document.getElementById("casino-section");
  if (!casinoSection) return null;
  const container = document.getElementById("casino-container");
  if (!container) return null;

  const duplicateShells = Array.from(casinoSection.querySelectorAll(":scope > .phone-casino-shell"));
  if (duplicateShells.length > 1) {
    duplicateShells.slice(1).forEach((node) => node.remove());
  }

  let shell = casinoSection.querySelector(".phone-casino-shell");
  if (!shell) {
    shell = document.createElement("div");
    shell.className = "phone-casino-shell";
    shell.innerHTML = `
      <div class="phone-casino-top">
        <div class="phone-casino-header">
          <h2 id="phoneEmbedTitle">Casino</h2>
          <div class="phone-casino-header-actions">
            <button id="phoneEmbedReloadBtn" type="button">Reload</button>
            <button id="phoneEmbedOpenFullBtn" type="button">Open Full</button>
            <button id="phoneEmbedCloseBtn" type="button" aria-label="Close">✕</button>
          </div>
        </div>
      </div>
      <div id="phoneCasinoStage" class="phone-casino-stage">
        <div id="phoneCasinoContent" class="phone-casino-content"></div>
      </div>
      <div id="phoneCasinoActionbar" class="phone-casino-actionbar">
        <div class="phone-action-balance">Balance: <strong id="phoneActionBalance">$0.00</strong></div>
        <div class="phone-action-row">
          <input id="phoneActionBetInput" class="phone-action-bet" type="number" min="0" step="0.01" inputmode="decimal" placeholder="Bet">
          <button id="phoneActionPrimaryBtn" class="phone-action-primary" type="button">Play</button>
          <button id="phoneActionSecondaryBtn" class="phone-action-secondary" type="button" hidden>Cashout</button>
          <button id="phoneActionMoreBtn" class="phone-action-more" type="button">⋯</button>
        </div>
      </div>
      <div id="phoneCasinoSheetBackdrop" class="phone-casino-sheet-backdrop" hidden></div>
      <section id="phoneCasinoSheet" class="phone-casino-sheet" aria-hidden="true">
        <header>
          <strong>More</strong>
          <button id="phoneCasinoSheetClose" type="button">Close</button>
        </header>
        <div id="phoneCasinoSheetBody" class="phone-casino-sheet-body"></div>
      </section>
    `;

    casinoSection.innerHTML = "";
    casinoSection.appendChild(shell);
    const content = shell.querySelector("#phoneCasinoContent");
    if (content && container.parentElement !== content) content.appendChild(container);

    const closeFn = () => {
      try {
        if (window.parent && window.parent !== window) {
          window.parent.postMessage({ type: "phone-mini-close" }, "*");
          return;
        }
      } catch (error) {}
      if (window.history.length > 1) window.history.back();
    };

    const closeBtn = shell.querySelector("#phoneEmbedCloseBtn");
    if (closeBtn) closeBtn.addEventListener("click", closeFn);

    const reloadBtn = shell.querySelector("#phoneEmbedReloadBtn");
    if (reloadBtn) {
      reloadBtn.addEventListener("click", () => {
        const key = String(phoneEmbedUiState.gameKey || gameKey || "").toLowerCase();
        if (!key) return;
        loadCasinoGame(key);
      });
    }

    const openFullBtn = shell.querySelector("#phoneEmbedOpenFullBtn");
    if (openFullBtn) {
      openFullBtn.addEventListener("click", () => {
        const key = String(phoneEmbedUiState.gameKey || gameKey || "").toLowerCase();
        if (!key) return;
        try {
          if (window.parent && window.parent !== window) {
            window.parent.postMessage({ type: "phone-mini-open-full", game: key }, "*");
            return;
          }
        } catch (error) {}
        const url = new URL(window.location.href);
        url.searchParams.delete("phoneMiniGame");
        window.location.href = url.toString();
      });
    }

    const moreBtn = shell.querySelector("#phoneActionMoreBtn");
    const closeSheetBtn = shell.querySelector("#phoneCasinoSheetClose");
    const backdrop = shell.querySelector("#phoneCasinoSheetBackdrop");
    if (moreBtn) moreBtn.addEventListener("click", () => setPhoneCasinoSheetOpen(true));
    if (closeSheetBtn) closeSheetBtn.addEventListener("click", () => setPhoneCasinoSheetOpen(false));
    if (backdrop) backdrop.addEventListener("click", () => setPhoneCasinoSheetOpen(false));
  }

  if (!casinoSection.querySelector(".phone-casino-outside-brand.side-left")) {
    const leftBrand = document.createElement("div");
    leftBrand.className = "phone-casino-outside-brand side-left";
    leftBrand.setAttribute("aria-hidden", "true");
    leftBrand.textContent = "SAGE";
    casinoSection.appendChild(leftBrand);
  }
  if (!casinoSection.querySelector(".phone-casino-outside-brand.side-right")) {
    const rightBrand = document.createElement("div");
    rightBrand.className = "phone-casino-outside-brand side-right";
    rightBrand.setAttribute("aria-hidden", "true");
    rightBrand.textContent = "SAGE";
    casinoSection.appendChild(rightBrand);
  }
  shell.querySelectorAll(".phone-casino-side-brand").forEach((node) => node.remove());

  const titleEl = shell.querySelector("#phoneEmbedTitle");
  if (titleEl) titleEl.textContent = `${getPhoneEmbeddedGameTitle(gameKey)} Mini`;

  const legacyStatusRow = shell.querySelector(".phone-casino-status");
  if (legacyStatusRow) legacyStatusRow.remove();
  const headerActions = shell.querySelector(".phone-casino-header-actions");
  if (headerActions && !shell.querySelector("#phoneEmbedCloseBtn")) {
    const closeBtn = document.createElement("button");
    closeBtn.id = "phoneEmbedCloseBtn";
    closeBtn.type = "button";
    closeBtn.setAttribute("aria-label", "Close");
    closeBtn.textContent = "✕";
    headerActions.appendChild(closeBtn);
    closeBtn.addEventListener("click", () => {
      try {
        if (window.parent && window.parent !== window) {
          window.parent.postMessage({ type: "phone-mini-close" }, "*");
          return;
        }
      } catch (error) {}
      if (window.history.length > 1) window.history.back();
    });
  }

  const content = shell.querySelector("#phoneCasinoContent");
  if (content) {
    content.querySelectorAll(".phone-casino-shell").forEach((nestedShell) => nestedShell.remove());
    if (container.parentElement !== content) content.appendChild(container);
  }
  // Embedded inside the in-phone app iframe: keep one outer phone header only.
  shell.classList.toggle("phone-casino-shell-hosted", IS_PHONE_MINI_IFRAME_HOSTED);
  return shell;
}

function normalizePhoneEmbeddedInputs(root) {
  if (!root) return;
  root.querySelectorAll("input[type='number']").forEach((input) => {
    input.setAttribute("inputmode", "decimal");
    input.setAttribute("autocomplete", "off");
  });
}

function bindPhoneEmbeddedTouchGuards(root) {
  if (!root) return;
  const guardSelector = "canvas, #gameCanvas, .slot-machine, .reels, .wheel-stage, .grid-stage, .number-board-wrap, .board";
  root.querySelectorAll(guardSelector).forEach((node) => {
    if (node.dataset.phoneTouchGuard === "true") return;
    node.dataset.phoneTouchGuard = "true";
    node.style.touchAction = "none";
    node.addEventListener(
      "touchmove",
      (event) => {
        event.preventDefault();
        event.stopPropagation();
      },
      { passive: false }
    );
    node.addEventListener("pointerdown", (event) => event.stopPropagation());
  });
}

function stripDuplicatePhoneChrome(root) {
  if (!(root instanceof HTMLElement)) return;

  const duplicateSelectors = [
    ".phone-statusbar",
    ".phone-status-time",
    ".phone-dynamic-island",
    ".phone-status-actions",
    ".phone-status-icons",
    ".phone-casino-status",
    ".phone-casino-notch",
    ".phone-casino-time",
    ".phone-casino-status-right"
  ];

  root.querySelectorAll(duplicateSelectors.join(",")).forEach((node) => node.remove());

  root.querySelectorAll("header,section,div").forEach((node) => {
    if (!(node instanceof HTMLElement)) return;
    if (node.children.length > 12) return;
    const text = (node.textContent || "").replace(/\s+/g, " ").trim().toLowerCase();
    if (!text) return;
    const looksLikeStatus =
      text.includes("9:41") ||
      ((text.includes("lte") || text.includes("5g")) && text.includes("%"));
    if (!looksLikeStatus) return;
    if (node.querySelector("button,input,canvas,svg,.bet-btn,.tile,.card,.reel,.wheel,.grid,.board")) return;
    node.remove();
  });
}

function refreshPhoneEmbeddedCanvases(root) {
  if (!root) return;
  const dpr = Math.max(1, Math.min(2, window.devicePixelRatio || 1));
  root.querySelectorAll("canvas").forEach((canvas) => {
    if (canvas.id === "gameCanvas" || canvas.id === "plinkoCanvas" || canvas.id === "curveCanvas") return;
    const rect = canvas.getBoundingClientRect();
    if (rect.width < 2 || rect.height < 2) return;
    const targetWidth = Math.round(rect.width * dpr);
    const targetHeight = Math.round(rect.height * dpr);
    if (canvas.width === targetWidth && canvas.height === targetHeight) return;
    canvas.width = targetWidth;
    canvas.height = targetHeight;
    const context = canvas.getContext("2d");
    if (context) context.setTransform(dpr, 0, 0, dpr, 0, 0);
  });
}

function syncPhoneCrashMiniCanvas(root) {
  if (!(root instanceof HTMLElement)) return;
  if (String(phoneEmbedUiState.gameKey || "").toLowerCase() !== "crash") return;

  const graphWrap = root.querySelector(".crash-root .display-area");
  const canvas = root.querySelector(".crash-root #curveCanvas");
  if (!(graphWrap instanceof HTMLElement) || !(canvas instanceof HTMLCanvasElement)) return;

  const rect = graphWrap.getBoundingClientRect();
  if (rect.width < 2 || rect.height < 2) return;

  const dpr = Math.max(1, window.devicePixelRatio || 1);
  const targetWidth = Math.floor(rect.width * dpr);
  const targetHeight = Math.floor(rect.height * dpr);

  canvas.style.width = `${rect.width}px`;
  canvas.style.height = `${rect.height}px`;

  if (canvas.width !== targetWidth || canvas.height !== targetHeight) {
    canvas.width = targetWidth;
    canvas.height = targetHeight;
    const ctx = canvas.getContext("2d");
    if (ctx) ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  }

  window.dispatchEvent(new Event("resize"));
}

function inferPanelTitle(node) {
  if (!node) return "Panel";
  const id = String(node.id || "").toLowerCase();
  const className = String(node.className || "").toLowerCase();
  if (
    id.includes("bet") ||
    className.includes("bet") ||
    id.includes("mode") ||
    className.includes("mode") ||
    id.includes("difficulty") ||
    className.includes("difficulty") ||
    id.includes("risk") ||
    className.includes("risk") ||
    id.includes("line") ||
    className.includes("line")
  ) {
    return "Bet Settings";
  }
  if (id.includes("history") || className.includes("history")) return "History";
  if (id.includes("auto") || className.includes("auto")) return "Autoplay";
  if (id.includes("payout") || className.includes("payout") || id.includes("odds") || className.includes("odds")) return "Payouts";
  if (className.includes("left-panel") || className.includes("sidebar") || className.includes("control")) return "Controls";
  return "Advanced";
}

function copyCssVariables(sourceNode, targetNode) {
  if (!(sourceNode instanceof HTMLElement) || !(targetNode instanceof HTMLElement)) return;
  const computed = window.getComputedStyle(sourceNode);
  for (let i = 0; i < computed.length; i += 1) {
    const name = computed[i];
    if (!name || !name.startsWith("--")) continue;
    const value = computed.getPropertyValue(name);
    if (!value) continue;
    targetNode.style.setProperty(name, value);
  }
}

function getPhoneSheetSkipSelectors(gameKey) {
  switch (String(gameKey || "").toLowerCase()) {
    case "diamonds":
      return [".diamond-root .payout-ladder", ".diamond-root #payoutRows"];
    case "blackjack":
      return [".blackjack-root .actions"];
    case "slots":
      return [".crypto-slot-root #betBtn", ".crypto-slot-root .bet-action-wrap"];
    case "dragontower":
      return [".dragon-root #startBtn", ".dragon-root #cashOutBtn"];
    case "dice":
      return [".dice-root #rollBtn"];
    case "slide":
      return [".slide-root #bet-btn"];
    default:
      return [];
  }
}

function getOrCreateInlineActionWrap(parent, className, beforeNode = null) {
  if (!(parent instanceof HTMLElement)) return null;
  let wrap = parent.querySelector(`.${className}`);
  if (!wrap) {
    wrap = document.createElement("div");
    wrap.className = `phone-inline-actions ${className}`;
    if (beforeNode && beforeNode.parentElement === parent) {
      parent.insertBefore(wrap, beforeNode);
    } else {
      parent.appendChild(wrap);
    }
  }
  return wrap;
}

function moveNodeToWrap(node, wrap) {
  if (!(node instanceof HTMLElement) || !(wrap instanceof HTMLElement)) return;
  if (node.parentElement !== wrap) wrap.appendChild(node);
}

function applyPhoneInlineGameLayout(root, gameKey) {
  if (!(root instanceof HTMLElement)) return;
  const key = String(gameKey || "").toLowerCase();
  if (root.dataset.phoneInlineLayoutFor === key) return;

  if (key === "blackjack") {
    const tablePanel = root.querySelector(".blackjack-root .table-panel");
    const deckArea = root.querySelector(".blackjack-root .deck-area");
    const actions = root.querySelector(".blackjack-root .actions");
    const wrap = getOrCreateInlineActionWrap(tablePanel, "phone-inline-bj-actions", deckArea);
    moveNodeToWrap(actions, wrap);
  }

  if (key === "slots") {
    const rightPanel = root.querySelector(".crypto-slot-root .right-panel");
    const slotMachine = root.querySelector(".crypto-slot-root .slot-machine");
    const betButton = root.querySelector(".crypto-slot-root #betBtn");
    const wrap = getOrCreateInlineActionWrap(rightPanel, "phone-inline-slots-actions", slotMachine);
    moveNodeToWrap(betButton, wrap);
  }

  if (key === "dragontower") {
    const buttonGroup = root.querySelector(".dragon-root .button-group");
    const resetBtn = root.querySelector(".dragon-root #resetBtn");
    const startBtn = root.querySelector(".dragon-root #startBtn");
    const cashOutBtn = root.querySelector(".dragon-root #cashOutBtn");
    const inlineWrap = root.querySelector(".phone-inline-dragon-actions");

    if (buttonGroup) {
      if (startBtn && startBtn.parentElement !== buttonGroup) {
        if (resetBtn && resetBtn.parentElement === buttonGroup) buttonGroup.insertBefore(startBtn, resetBtn);
        else buttonGroup.appendChild(startBtn);
      }
      if (cashOutBtn && cashOutBtn.parentElement !== buttonGroup) {
        if (resetBtn && resetBtn.parentElement === buttonGroup) buttonGroup.insertBefore(cashOutBtn, resetBtn);
        else buttonGroup.appendChild(cashOutBtn);
      }
    }

    if (inlineWrap && inlineWrap.children.length === 0) {
      inlineWrap.remove();
    }
  }

  if (key === "dice") {
    const gamePanel = root.querySelector(".dice-root .game-panel");
    const sliderArea = root.querySelector(".dice-root .slider-display-area");
    const rollBtn = root.querySelector(".dice-root #rollBtn");
    const wrap = getOrCreateInlineActionWrap(gamePanel, "phone-inline-dice-actions", sliderArea);
    moveNodeToWrap(rollBtn, wrap);
  }

  if (key === "slide") {
    const topBar = root.querySelector(".slide-root .top-bar");
    const balanceDisplay = root.querySelector(".slide-root .balance-display");
    const betBtn = root.querySelector(".slide-root #bet-btn");
    const wrap = getOrCreateInlineActionWrap(topBar, "phone-inline-slide-actions");
    if (balanceDisplay && wrap && balanceDisplay.parentElement === topBar) {
      topBar.insertBefore(wrap, balanceDisplay.nextSibling);
    }
    moveNodeToWrap(betBtn, wrap);
  }

  if (key === "hilo") {
    const main = root.querySelector(".hilo-root .main");
    const cardsZone = root.querySelector(".hilo-root .cards-zone");
    const guessButtons = root.querySelector(".hilo-root .guess-buttons");
    const wrap = getOrCreateInlineActionWrap(main, "phone-inline-hilo-guesses", cardsZone?.nextElementSibling || null);
    moveNodeToWrap(guessButtons, wrap);
  }

  if (key === "mines") {
    const panel = root.querySelector(".mines-root .panel");
    const gameArea = root.querySelector(".mines-root .game-area");
    const grid = root.querySelector(".mines-root .grid");
    const overlay = root.querySelector(".mines-root #roundOverlay");

    if (panel) panel.style.setProperty("display", "none", "important");
    if (overlay) overlay.style.setProperty("display", "none", "important");

    if (gameArea) {
      gameArea.style.setProperty("padding", "0", "important");
      gameArea.style.setProperty("border", "0", "important");
      gameArea.style.setProperty("background", "transparent", "important");
      gameArea.style.setProperty("box-shadow", "none", "important");
      gameArea.style.setProperty("display", "grid", "important");
      gameArea.style.setProperty("place-items", "center", "important");
    }

    if (grid) {
      grid.style.setProperty("width", "min(100%, 330px)", "important");
      grid.style.setProperty("max-width", "100%", "important");
    }
  }

  if (key === "diamonds") {
    const panelGame = root.querySelector(".diamond-root .panel-game");
    const diamondGrid = root.querySelector(".diamond-root .diamond-grid");
    const resultHeader = root.querySelector(".diamond-root .result-header");
    const history = root.querySelector(".diamond-root .history");

    if (resultHeader) resultHeader.style.setProperty("display", "none", "important");
    if (history) history.style.setProperty("display", "none", "important");

    if (panelGame) {
      panelGame.style.setProperty("padding", "0", "important");
      panelGame.style.setProperty("border", "0", "important");
      panelGame.style.setProperty("background", "transparent", "important");
      panelGame.style.setProperty("box-shadow", "none", "important");
      panelGame.style.setProperty("display", "flex", "important");
      panelGame.style.setProperty("flex-direction", "column", "important");
      panelGame.style.setProperty("align-items", "center", "important");
      panelGame.style.setProperty("justify-content", "flex-start", "important");
      panelGame.style.setProperty("gap", "8px", "important");
      panelGame.style.setProperty("min-height", "0", "important");
    }

    if (diamondGrid) {
      diamondGrid.style.setProperty("margin-top", "0", "important");
      diamondGrid.style.setProperty("width", "min(100%, 340px)", "important");
      diamondGrid.style.setProperty("max-width", "100%", "important");
    }
  }

  root.dataset.phoneInlineLayoutFor = key;
}

function moveSecondaryPanelsToSheet(root) {
  const sheetBody = document.getElementById("phoneCasinoSheetBody");
  if (!sheetBody || !root) return;
  const gameKey = String(phoneEmbedUiState.gameKey || PHONE_EMBED_GAME_PARAM || "").toLowerCase();
  if (phoneEmbedUiState.root === root && phoneEmbedUiState.movedNodes.length > 0) {
    if (gameKey === "diamonds") {
      const panelBet = root.querySelector(".diamond-root .panel-bet");
      if (panelBet) {
        const parent = panelBet.parentElement;
        const nextSibling = panelBet.nextElementSibling;
        if (!phoneEmbedUiState.movedNodes.some((entry) => entry?.node === panelBet)) {
          phoneEmbedUiState.movedNodes.push({ node: panelBet, parent, nextSibling });
        }

        const section = document.createElement("section");
        section.className = "phone-sheet-section";
        section.dataset.forcedDiamondPanel = "true";
        copyCssVariables(root, section);
        copyCssVariables(panelBet, section);
        section.innerHTML = `<h3>${inferPanelTitle(panelBet)}</h3>`;
        section.appendChild(panelBet);

        const existing = sheetBody.querySelector("[data-forced-diamond-panel='true']");
        if (existing) existing.replaceWith(section);
        else sheetBody.prepend(section);
      }
    }
    return;
  }
  sheetBody.innerHTML = "";

  const selectors = [
    ".left-panel",
    ".sidebar",
    ".side-panel",
    ".history-panel",
    ".history-column",
    ".controls",
    ".controls-card",
    ".control-card",
    ".control-box",
    ".control-block",
    ".panel-section",
    ".field-block",
    ".bet-box",
    ".actions",
    ".plinko-root .stats",
    ".button-row",
    ".input-group",
    ".auto-controls",
    ".mode-switch",
    ".mode-toggle",
    ".bet-input-row",
    ".bet-control",
    ".difficulty-block",
    ".bet-block",
    ".action-block",
    ".meta-panels",
    ".odds-strip",
    ".bet-feed-card",
    ".potential-profits",
    "#autoControls",
    "#autoSettings",
    "#history-container",
    "#historyList",
    "#hiloHistory",
    "#topHistoryList",
    "#payoutRows",
    "#sidePayouts",
    ".panel-bet",
    ".mines-root .panel"
  ];

  const picked = [];
  const skipSelectors = getPhoneSheetSkipSelectors(gameKey);
  const isSkippedNode = (node) => {
    if (!(node instanceof HTMLElement) || skipSelectors.length === 0) return false;
    return skipSelectors.some((selector) => node.matches(selector) || Boolean(node.closest(selector)));
  };
  const isDisallowedContainer = (node) => {
    if (!(node instanceof HTMLElement)) return true;
    if (!root.contains(node) || node === root) return true;
    return node.matches(
      ".app,.app-layout,.main,.main-panel,.right-panel,.chart-panel,.game-container,.table-panel,.display-area,.board-area,.slot-machine,.reels,.wheel-stage,.grid-stage,.number-board-wrap,.table-stage,.stage-card,.canvas-wrapper,.table-footer,.message-line,.status-line,.game-shell,.phone-casino-shell,.phone-casino-stage"
    );
  };
  const pushCandidate = (node) => {
    if (!(node instanceof HTMLElement)) return;
    if (isDisallowedContainer(node)) return;
    if (isSkippedNode(node)) return;
    if (picked.some((existing) => existing.contains(node) || node.contains(existing))) return;
    picked.push(node);
  };

  selectors.forEach((selector) => {
    root.querySelectorAll(selector).forEach((node) => {
      pushCandidate(node);
    });
  });

  const settingAnchors = root.querySelectorAll(
    [
      "input[id*='bet' i]",
      "input[id*='auto' i]",
      "input[id*='cashout' i]",
      "input[id*='risk' i]",
      "input[id*='difficulty' i]",
      "input[id*='line' i]",
      "select[id*='risk' i]",
      "select[id*='difficulty' i]",
      "select[id*='line' i]",
      "select[id*='row' i]",
      "button[id*='auto' i]",
      "button[id*='rebet' i]",
      "button[id*='half' i]",
      "button[id*='double' i]",
      "button[id*='max' i]",
      "button[id*='clear' i]"
    ].join(",")
  );

  settingAnchors.forEach((anchor) => {
    if (!(anchor instanceof HTMLElement)) return;
    const container = anchor.closest(
      ".control-block,.field-block,.control-box,.auto-controls,.input-group,.bet-box,.actions,.button-row,.bet-control,.bet-input-row,.panel-section,.sidebar,.left-panel,.side-panel,.controls,.controls-card,.meta-panels,.odds-strip,.bet-feed-card,.potential-profits,.control-card,.mode-switch,.mode-toggle,.difficulty-block,.bet-block,.action-block"
    );
    if (container) pushCandidate(container);
  });

  picked.forEach((node) => {
    const parent = node.parentElement;
    if (!parent) return;
    const nextSibling = node.nextElementSibling;
    phoneEmbedUiState.movedNodes.push({ node, parent, nextSibling });

    const section = document.createElement("section");
    section.className = "phone-sheet-section";
    copyCssVariables(root, section);
    copyCssVariables(node, section);
    section.innerHTML = `<h3>${inferPanelTitle(node)}</h3>`;
    section.appendChild(node);
    sheetBody.appendChild(section);
  });

  if (picked.length === 0) {
    const empty = document.createElement("div");
    empty.className = "phone-sheet-empty";
    empty.textContent = "No additional options for this game.";
    sheetBody.appendChild(empty);
  }
}

function findFirstInScopes(scopes, selectors) {
  for (const selector of selectors) {
    for (const scope of scopes) {
      if (!(scope instanceof Element)) continue;
      const node = scope.querySelector(selector);
      if (node) return node;
    }
  }
  return null;
}

function bindPhoneActionBar(root) {
  const actionBar = document.getElementById("phoneCasinoActionbar");
  const gameKey = String(phoneEmbedUiState.gameKey || "");
  if (actionBar) {
    actionBar.hidden = gameKey === "roulette";
  }
  if (gameKey === "roulette") return;

  const balanceOut = document.getElementById("phoneActionBalance");
  const betInput = document.getElementById("phoneActionBetInput");
  const primaryBtn = document.getElementById("phoneActionPrimaryBtn");
  const secondaryBtn = document.getElementById("phoneActionSecondaryBtn");
  const moreBtn = document.getElementById("phoneActionMoreBtn");
  if (!balanceOut || !betInput || !primaryBtn || !secondaryBtn || !root) return;

  const sheetBody = document.getElementById("phoneCasinoSheetBody");
  const scopes = [root];
  if (sheetBody) scopes.push(sheetBody);

  if (moreBtn) {
    moreBtn.hidden = false;
  }

  const balanceSource = findFirstInScopes(scopes, [
    "#balanceValue",
    "#balanceStat",
    "#balanceDisplay",
    "#balanceInline",
    "#hiloBalance",
    "#balance",
    "#bjCash",
    "#phonePortfolioCash"
  ]);
  const betSource = findFirstInScopes(scopes, [
    "#betInput",
    "#bet-input",
    "#betAmount",
    "#hiloBetAmount",
    "#totalBetInput",
    "#currentBet",
    "input[id*='bet'][type='number']"
  ]);
  let primarySource = findFirstInScopes(scopes, [
    "#startButton",
    "#startBtn",
    "#betBtn",
    "#bet-btn",
    "#playBtn",
    "#rollBtn",
    "#spinBtn",
    "#placeBet",
    "#placeBetBtn",
    "#hiloBetButton",
    "#dealBtn",
    "#dropBtn"
  ]);
  if (!primarySource) {
    for (const scope of scopes) {
      if (!(scope instanceof Element)) continue;
      primarySource = Array.from(scope.querySelectorAll("button")).find((btn) => {
        const text = (btn.textContent || "").trim().toLowerCase();
        if (!text) return false;
        if (/(clear|close|back|history|auto|settings|more|half|2x|max)/i.test(text)) return false;
        return /(bet|start|play|spin|roll|deal|draw)/i.test(text);
      });
      if (primarySource) break;
    }
  }
  let cashoutSource = findFirstInScopes(scopes, [
    "#cashOutBtn",
    "#cashOutButton",
    "#hiloCashoutButton",
    "button[id*='cashOut']"
  ]);
  if (!cashoutSource && phoneEmbedUiState.gameKey === "roulette") {
    cashoutSource = findFirstInScopes(scopes, ["#rebetBtn", "#rebetTopBtn"]);
  }

  const sync = () => {
    if (balanceSource) {
      const text = (balanceSource.textContent || "").trim();
      const money = text.match(/\$?\s?[\d,]+(?:\.\d{1,2})?/);
      balanceOut.textContent = money ? (money[0].startsWith("$") ? money[0] : `$${money[0]}`) : text || "$0.00";
    } else {
      balanceOut.textContent = `$${cash.toFixed(2)}`;
    }

    if (betSource) {
      if (document.activeElement !== betInput) {
        betInput.value = String(betSource.value || "");
      }
      betInput.hidden = false;
      betInput.disabled = Boolean(betSource.disabled);
    } else {
      betInput.hidden = true;
    }

    if (primarySource) {
      primaryBtn.hidden = false;
      primaryBtn.disabled = Boolean(primarySource.disabled);
      primaryBtn.textContent = (primarySource.textContent || "Play").trim() || "Play";
    } else {
      primaryBtn.hidden = true;
    }

    if (gameKey === "crossyroad" && primarySource) {
      const sourceText = (primarySource.textContent || "").trim().toLowerCase();
      const roundRunning = sourceText.includes("cash");
      primaryBtn.hidden = false;
      primaryBtn.textContent = "Bet";
      primaryBtn.disabled = roundRunning || Boolean(primarySource.disabled);
      primaryBtn.onclick = () => {
        if (!roundRunning && !primarySource.disabled) primarySource.click();
      };

      secondaryBtn.hidden = false;
      secondaryBtn.textContent = "Cashout";
      secondaryBtn.disabled = !roundRunning || Boolean(primarySource.disabled);
      secondaryBtn.onclick = () => {
        if (roundRunning && !primarySource.disabled) primarySource.click();
      };
      return;
    }

    if (!cashoutSource || cashoutSource === primarySource) {
      const maybeCash = (primarySource?.textContent || "").toLowerCase().includes("cash");
      secondaryBtn.hidden = true;
      if (maybeCash) {
        secondaryBtn.hidden = false;
        secondaryBtn.textContent = "Cashout";
        const sourceDisabled =
          Boolean(primarySource?.disabled) ||
          Boolean(primarySource?.hidden) ||
          Boolean(primarySource?.classList?.contains("hidden"));
        secondaryBtn.disabled = sourceDisabled;
        secondaryBtn.onclick = () => primarySource?.click();
      }
    } else {
      secondaryBtn.hidden = false;
      secondaryBtn.textContent = (cashoutSource.textContent || "Cashout").trim() || "Cashout";
      const sourceDisabled =
        Boolean(cashoutSource.disabled) ||
        Boolean(cashoutSource.hidden) ||
        Boolean(cashoutSource.classList?.contains("hidden"));
      secondaryBtn.disabled = sourceDisabled;
      secondaryBtn.onclick = () => cashoutSource.click();
    }

    primaryBtn.onclick = () => {
      if (primarySource && !primarySource.disabled) primarySource.click();
    };
  };

  betInput.oninput = () => {
    if (!betSource) return;
    betSource.value = betInput.value;
    betSource.dispatchEvent(new Event("input", { bubbles: true }));
  };
  betInput.onchange = () => {
    if (!betSource) return;
    betSource.value = betInput.value;
    betSource.dispatchEvent(new Event("change", { bubbles: true }));
  };

  sync();
  if (phoneEmbedUiState.syncTimer) clearInterval(phoneEmbedUiState.syncTimer);
  phoneEmbedUiState.syncTimer = setInterval(sync, 140);
}

function fitPhoneGameIntoStage(root) {
  const stage = document.getElementById("phoneCasinoStage");
  if (!stage || !root) return;
  const gameKey = String(phoneEmbedUiState.gameKey || "");

  if (gameKey === "dragontower") {
    const dragonRoot = root.querySelector(".dragon-root");
    const app = root.querySelector(".dragon-root .app");
    const boardArea = root.querySelector(".dragon-root .board-area");
    const tower = root.querySelector(".dragon-root .tower");

    root.style.position = "absolute";
    root.style.left = "0px";
    root.style.top = "0px";
    root.style.width = "100%";
    root.style.height = "100%";
    root.style.transform = "none";
    root.style.transformOrigin = "top left";

    if (dragonRoot instanceof HTMLElement) {
      dragonRoot.style.width = "100%";
      dragonRoot.style.height = "100%";
      dragonRoot.style.overflow = "hidden";
    }

    if (app instanceof HTMLElement) {
      app.style.width = "100%";
      app.style.maxWidth = "100%";
      app.style.height = "100%";
      app.style.minHeight = "0";
    }

    if (boardArea instanceof HTMLElement) {
      boardArea.style.height = "100%";
      boardArea.style.minHeight = "0";
      boardArea.style.overflow = "hidden";
      boardArea.style.alignItems = "center";
      boardArea.style.justifyItems = "center";
      boardArea.style.placeItems = "center";
    }

    if (tower instanceof HTMLElement) {
      tower.style.transform = "none";
      tower.style.transformOrigin = "center center";
      tower.style.margin = "0 auto";

      const towerRect = tower.getBoundingClientRect();
      const safeInset = 8;
      const availableWidth = Math.max(1, (boardArea?.clientWidth || stage.clientWidth) - safeInset);
      const availableHeight = Math.max(1, (boardArea?.clientHeight || stage.clientHeight) - safeInset);
      const widthScale = availableWidth / Math.max(1, towerRect.width);
      const heightScale = availableHeight / Math.max(1, towerRect.height);
      const baseScale = Math.min(widthScale, heightScale);
      const scale = Math.max(0.3, Math.min(baseScale * 0.995, 2.4));
      tower.style.transform = `scale(${scale.toFixed(4)})`;
    }
    return;
  }

  if (gameKey === "crash" || gameKey === "roulette" || gameKey === "plinko" || gameKey === "horseracing") {
    root.style.position = "absolute";
    root.style.left = "0px";
    root.style.top = "0px";
    root.style.width = "100%";
    root.style.height = "100%";
    root.style.transform = "none";
    root.style.transformOrigin = "top left";
    return;
  }

  const stageWidth = Math.max(1, stage.clientWidth);
  const stageHeight = Math.max(1, stage.clientHeight);

  root.style.position = "relative";
  root.style.left = "0px";
  root.style.top = "0px";
  root.style.transform = "none";
  root.style.transformOrigin = "top left";
  root.style.width = "100%";
  root.style.height = "auto";

  const rect = root.getBoundingClientRect();
  const naturalWidth = Math.max(1, rect.width);
  const naturalHeight = Math.max(1, rect.height);
  const scale = Math.min(1, stageWidth / naturalWidth, stageHeight / naturalHeight);
  const tx = Math.max(0, (stageWidth - naturalWidth * scale) * 0.5);
  const ty = Math.max(0, (stageHeight - naturalHeight * scale) * 0.5);

  root.style.position = "absolute";
  root.style.left = "0px";
  root.style.top = "0px";
  root.style.width = `${naturalWidth}px`;
  root.style.height = `${naturalHeight}px`;
  root.style.transform = `translate3d(${tx.toFixed(2)}px, ${ty.toFixed(2)}px, 0) scale(${scale.toFixed(4)})`;
}

function applyPhoneEmbeddedGameEnhancement(gameKey = PHONE_EMBED_GAME_PARAM) {
  if (!IS_PHONE_EMBED_MODE) return;
  const normalizedGameKey = String(gameKey || "").toLowerCase();
  const shell = ensurePhoneEmbeddedShell(gameKey);
  if (shell) {
    shell.classList.toggle("phone-casino-shell-roulette", normalizedGameKey === "roulette");
    shell.classList.toggle("phone-casino-shell-horse", normalizedGameKey === "horseracing");
  }
  const container = document.getElementById("casino-container");
  if (!container) return;
  const root = Array.from(container.children).find((node) => node.id !== "game-exit-bar");
  if (!root) return;
  const isPlinkoNative = normalizedGameKey === "plinko" && root.id === "plinkoPhoneApp";
  const isHorseNative = normalizedGameKey === "horseracing" && root.id === "casino-horserace-screen";
  const isNativeLayout = isPlinkoNative || isHorseNative;
  if (shell) {
    shell.classList.toggle("phone-casino-shell-plinko-native", isPlinkoNative);
  }

  phoneEmbedUiState.root = root;
  phoneEmbedUiState.gameKey = normalizedGameKey;
  root.classList.add("phone-game-root", `phone-game-${phoneEmbedUiState.gameKey}`, "phone-embed-scaled");

  stripDuplicatePhoneChrome(root);
  normalizePhoneEmbeddedInputs(root);
  applyPhoneInlineGameLayout(root, phoneEmbedUiState.gameKey);
  bindPhoneEmbeddedTouchGuards(root);
  if (!isNativeLayout) {
    moveSecondaryPanelsToSheet(root);
    bindPhoneActionBar(root);
  } else {
    const actionBar = document.getElementById("phoneCasinoActionbar");
    if (normalizedGameKey === "horseracing") {
      setPhoneCasinoSheetOpen(false);
      if (actionBar) actionBar.hidden = false;
      bindPhoneActionBar(root);
    } else {
      if (actionBar) actionBar.hidden = true;
      setPhoneCasinoSheetOpen(false);
    }
  }
  fitPhoneGameIntoStage(root);
  refreshPhoneEmbeddedCanvases(root);

  if (phoneEmbedUiState.gameKey === "crash") {
    if (phoneEmbedUiState.crashCanvasRaf) cancelAnimationFrame(phoneEmbedUiState.crashCanvasRaf);
    if (phoneEmbedUiState.crashCanvasTimer) clearTimeout(phoneEmbedUiState.crashCanvasTimer);
    phoneEmbedUiState.crashCanvasRaf = requestAnimationFrame(() => {
      syncPhoneCrashMiniCanvas(root);
      phoneEmbedUiState.crashCanvasRaf = null;
    });
    phoneEmbedUiState.crashCanvasTimer = setTimeout(() => {
      syncPhoneCrashMiniCanvas(root);
      phoneEmbedUiState.crashCanvasTimer = null;
    }, 140);
  }

  if (typeof ResizeObserver !== "undefined") {
    if (phoneEmbedUiState.resizeObserver) phoneEmbedUiState.resizeObserver.disconnect();
    phoneEmbedUiState.resizeObserver = new ResizeObserver(() => {
      schedulePhoneEmbeddedGameEnhancement(phoneEmbedUiState.gameKey || PHONE_EMBED_GAME_PARAM);
    });
    phoneEmbedUiState.resizeObserver.observe(root);
    const stage = document.getElementById("phoneCasinoStage");
    if (stage) phoneEmbedUiState.resizeObserver.observe(stage);
  }
}

function schedulePhoneEmbeddedGameEnhancement(gameKey = PHONE_EMBED_GAME_PARAM) {
  if (!IS_PHONE_EMBED_MODE) return;
  if (phoneEmbedEnhanceRaf) cancelAnimationFrame(phoneEmbedEnhanceRaf);
  if (phoneEmbedEnhanceTimer) clearTimeout(phoneEmbedEnhanceTimer);
  phoneEmbedEnhanceRaf = requestAnimationFrame(() => {
    applyPhoneEmbeddedGameEnhancement(gameKey);
    phoneEmbedEnhanceRaf = null;
    phoneEmbedEnhanceTimer = setTimeout(() => {
      applyPhoneEmbeddedGameEnhancement(gameKey);
      phoneEmbedEnhanceTimer = null;
    }, 220);
  });
}

function initPhoneEmbeddedGameView() {
  if (!IS_PHONE_EMBED_MODE) return;
  bindPhoneMiniCashBridge();
  const gameKey = String(PHONE_EMBED_GAME_PARAM || "").toLowerCase();
  const isSupported = PHONE_MINI_GAME_APPS.some((item) => item.game === gameKey);
  if (!isSupported) return;

  document.body.classList.add("phone-embedded-mini-mode");
  const trading = document.getElementById("trading-section");
  const casino = document.getElementById("casino-section");
  const phoneOverlayEl = document.getElementById("phoneOverlay");
  const phoneBtnEl = document.getElementById("phoneBtn");

  if (trading) trading.style.display = "none";
  if (casino) casino.style.display = "block";
  if (phoneBtnEl) phoneBtnEl.style.display = "none";
  if (phoneOverlayEl) phoneOverlayEl.remove();
  // Dedicated mini mode should never scroll the page; lock both root nodes.
  document.documentElement.classList.add("phone-embedded-mini-mode");

  ensurePhoneEmbeddedShell(gameKey);
  loadCasinoGame(gameKey);

  if (!phoneEmbedResizeBound) {
    phoneEmbedResizeBound = true;
    window.addEventListener("resize", () => schedulePhoneEmbeddedGameEnhancement(PHONE_EMBED_GAME_PARAM));
    window.addEventListener("orientationchange", () => schedulePhoneEmbeddedGameEnhancement(PHONE_EMBED_GAME_PARAM));
  }
}

// ------------------ SLOTS ------------------
function loadSlots() {
  const container = document.getElementById("casino-container");
  if (!container) return;
  container.classList.add("casino-fullbleed", "slots-fullbleed");

  container.innerHTML = `
    <div class="crypto-slot-root">
      <div class="app-shell" id="appShell">
        <aside class="left-panel panel">
          <section class="control-block">
            <h2 class="block-title">Mode</h2>
            <div class="mode-toggle" role="tablist" aria-label="Play mode">
              <button id="manualModeBtn" class="mode-btn active" type="button">Manual</button>
              <button id="autoModeBtn" class="mode-btn" type="button">Auto</button>
            </div>
          </section>

          <section class="control-block">
            <h2 class="block-title">Bet Amount</h2>
            <div class="bet-control">
              <button id="betDownBtn" class="adjust-btn" type="button" aria-label="Decrease bet">-</button>
              <input id="totalBetInput" class="numeric-input" type="number" min="0.20" step="0.20" value="10.00">
              <button id="betUpBtn" class="adjust-btn" type="button" aria-label="Increase bet">+</button>
            </div>
          </section>

          <section class="control-block">
            <h2 class="block-title">Lines</h2>
            <div class="select-wrap">
              <select id="linesSelect" class="numeric-select" aria-label="Paylines"></select>
            </div>
          </section>

          <section class="control-block">
            <h2 class="block-title">Bet Per Line</h2>
            <label class="per-line-wrap" for="betPerLineInput">
              <span class="dot-indicator" aria-hidden="true"></span>
              <input id="betPerLineInput" class="numeric-input" type="number" min="0.01" step="0.01" value="0.50">
            </label>
          </section>

          <section class="control-block bet-action-wrap">
            <button id="betBtn" class="bet-btn" type="button">Bet</button>
          </section>

          <section class="control-block stats-box panel">
            <h2 class="block-title">Live Stats</h2>
            <dl class="stats-grid">
              <div><dt>Balance</dt><dd id="balanceStat">$0.00</dd></div>
              <div><dt>Profit</dt><dd id="profitStat">$0.00</dd></div>
              <div><dt>Wagered</dt><dd id="wageredStat">$0.00</dd></div>
              <div><dt>Wins</dt><dd id="winsStat">0</dd></div>
              <div><dt>Losses</dt><dd id="lossesStat">0</dd></div>
              <div><dt>State</dt><dd id="stateStat">Idle</dd></div>
              <div><dt>Free Spins</dt><dd id="freeSpinsStat">0</dd></div>
            </dl>
          </section>

          <section class="control-block chart-box panel">
            <h2 class="block-title">Recent Results</h2>
            <canvas id="resultsChart" width="320" height="120" aria-label="Recent spin results chart"></canvas>
          </section>
        </aside>

        <main class="right-panel panel">
          <div class="payout-display">
            <span>Total Payout</span>
            <strong id="totalPayoutValue">$0.00</strong>
          </div>

          <section class="slot-machine" id="slotMachine">
            <div class="flash-overlay" id="flashOverlay"></div>
            <div class="payline-numbers left" id="paylineLeft"></div>

            <div class="slot-frame" id="slotFrame">
              <div class="frame-glow"></div>
              <svg id="paylineOverlayBack" class="payline-overlay back" viewBox="0 0 100 100" preserveAspectRatio="none" aria-hidden="true"></svg>
              <div class="reels" id="reels" aria-label="Slot reels"></div>
              <svg id="paylineOverlayFront" class="payline-overlay front" viewBox="0 0 100 100" preserveAspectRatio="none" aria-hidden="true"></svg>
            </div>

            <div class="payline-numbers right" id="paylineRight"></div>
          </section>

          <div class="message-line" id="messageLine">Place your bet to spin.</div>
        </main>
      </div>
      <div id="fxOverlay" class="fx-overlay" aria-live="polite" aria-atomic="true"></div>
    </div>
  `;

  const root = container.querySelector(".crypto-slot-root");
  if (!root) return;
  addSageBrand(root, "bottom-left");

  let destroyed = false;
  let spinSession = 0;
  const pendingTimeouts = new Set();

  function setTrackedTimeout(fn, ms) {
    const id = window.setTimeout(() => {
      pendingTimeouts.delete(id);
      if (destroyed) return;
      fn();
    }, ms);
    pendingTimeouts.add(id);
    return id;
  }

  function clearTrackedTimeouts() {
    pendingTimeouts.forEach((id) => window.clearTimeout(id));
    pendingTimeouts.clear();
  }

  const SYMBOLS = {
    club: {
      frequency: 10,
      pays: { 3: 1.5, 4: 3, 5: 6 },
      icon: '<svg class="symbol-icon" viewBox="0 0 100 100" aria-hidden="true"><circle cx="34" cy="36" r="17" fill="#9f77ff"/><circle cx="66" cy="36" r="17" fill="#8c64f5"/><circle cx="50" cy="58" r="18" fill="#7a4fe8"/><rect x="43" y="58" width="14" height="22" rx="6" fill="#7142dc"/><ellipse cx="50" cy="31" rx="22" ry="9" fill="#d7c4ff" opacity="0.48"/></svg>'
    },
    spade: {
      frequency: 10,
      pays: { 3: 1.5, 4: 3, 5: 6 },
      icon: '<svg class="symbol-icon" viewBox="0 0 100 100" aria-hidden="true"><path d="M50 16 C43 27,20 39,20 57 C20 69,29 78,40 78 C46 78,49 76,50 72 C51 76,54 78,60 78 C71 78,80 69,80 57 C80 39,57 27,50 16 Z" fill="#6daeff"/><path d="M50 24 C45 31,28 41,28 55 C28 63,34 70,41 70 C46 70,49 68,50 64 C51 68,54 70,59 70 C66 70,72 63,72 55 C72 41,55 31,50 24 Z" fill="#9dcdff" opacity="0.78"/><rect x="44" y="70" width="12" height="14" rx="5" fill="#4f86df"/></svg>'
    },
    heart: {
      frequency: 9,
      pays: { 3: 2, 4: 4, 5: 8 },
      icon: '<svg class="symbol-icon" viewBox="0 0 100 100" aria-hidden="true"><path d="M50 82 C42 72,17 56,17 35 C17 24,25 16,36 16 C43 16,49 20,50 27 C51 20,57 16,64 16 C75 16,83 24,83 35 C83 56,58 72,50 82 Z" fill="#ff4a73"/><path d="M50 74 C44 67,25 53,25 37 C25 29,31 23,39 23 C44 23,48 25,50 30 C52 25,56 23,61 23 C69 23,75 29,75 37 C75 53,56 67,50 74 Z" fill="#ff8ea7" opacity="0.7"/></svg>'
    },
    triangle: {
      frequency: 8,
      pays: { 3: 3, 4: 6, 5: 12 },
      icon: '<svg class="symbol-icon" viewBox="0 0 100 100" aria-hidden="true"><polygon points="50,82 16,24 84,24" fill="#ffc939"/><polygon points="50,72 27,32 73,32" fill="#ffe07f"/><polygon points="50,82 32,50 68,50" fill="#e4ab23" opacity="0.72"/></svg>'
    },
    pentagon: {
      frequency: 7,
      pays: { 3: 4, 4: 8, 5: 15 },
      icon: '<svg class="symbol-icon" viewBox="0 0 100 100" aria-hidden="true"><polygon points="50,16 82,41 70,82 30,82 18,41" fill="#9167ff"/><polygon points="50,16 82,41 50,52" fill="#b392ff"/><polygon points="18,41 50,52 30,82" fill="#7f53ea"/><polygon points="82,41 50,52 70,82" fill="#6e42dd"/></svg>'
    },
    chip: {
      frequency: 6,
      pays: { 3: 2.5, 4: 5, 5: 10 },
      icon: '<svg class="symbol-icon" viewBox="0 0 100 100" aria-hidden="true"><circle cx="50" cy="50" r="30" fill="#4a9cff"/><circle cx="50" cy="50" r="22" fill="#79bcff"/><circle cx="50" cy="50" r="12" fill="#2e7fdb"/><line x1="50" y1="20" x2="50" y2="30" class="stroke"/><line x1="50" y1="70" x2="50" y2="80" class="stroke"/><line x1="20" y1="50" x2="30" y2="50" class="stroke"/><line x1="70" y1="50" x2="80" y2="50" class="stroke"/></svg>'
    },
    coin: {
      frequency: 5,
      pays: { 3: 5, 4: 12, 5: 25 },
      icon: '<svg class="symbol-icon" viewBox="0 0 100 100" aria-hidden="true"><circle cx="50" cy="50" r="30" fill="#ffd35c"/><circle cx="50" cy="50" r="23" fill="#ffc241"/><circle cx="50" cy="50" r="14" fill="#f4a925"/><text x="50" y="56" fill="#fff6cf" font-size="30" font-weight="800" text-anchor="middle">$</text><ellipse cx="40" cy="38" rx="10" ry="6" fill="#fff1ba" opacity="0.6"/></svg>'
    },
    diamond: {
      frequency: 3,
      pays: { 3: 10, 4: 25, 5: 50 },
      icon: '<svg class="symbol-icon" viewBox="0 0 100 100" aria-hidden="true"><polygon points="50,15 83,50 50,85 17,50" fill="#49dc68"/><polygon points="50,15 69,50 50,66 31,50" fill="#89ef9d"/><polygon points="17,50 50,85 31,50" fill="#38ba53"/></svg>'
    },
    crystal: {
      frequency: 2,
      pays: { 3: 10, 4: 25, 5: 50 },
      icon: '<svg class="symbol-icon" viewBox="0 0 100 100" aria-hidden="true"><polygon points="50,10 82,28 72,78 28,78 18,28" fill="#61efff"/><polygon points="50,10 66,30 50,58 34,30" fill="#b3fbff"/><polygon points="18,28 28,78 50,58" fill="#3ed2e3"/><polygon points="82,28 72,78 50,58" fill="#2abed0"/></svg>'
    },
    wild: {
      frequency: 2,
      pays: { 5: 75 },
      icon: '<svg class="symbol-icon" viewBox="0 0 100 100" aria-hidden="true"><polygon points="50,12 60,38 88,38 65,55 74,82 50,66 26,82 35,55 12,38 40,38" fill="#7ee7ff"/><polygon points="50,20 57,39 77,39 60,51 67,69 50,58 33,69 40,51 23,39 43,39" fill="#d3fbff" opacity="0.78"/></svg>'
    },
    scatter: {
      frequency: 3,
      pays: {},
      icon: '<svg class="symbol-icon" viewBox="0 0 100 100" aria-hidden="true"><circle cx="50" cy="50" r="32" fill="#ffcb4f"/><circle cx="50" cy="50" r="23" fill="#f2a82f"/><text x="50" y="56" fill="#fff4c5" font-size="26" font-weight="800" text-anchor="middle">SC</text><ellipse cx="40" cy="38" rx="9" ry="5" fill="#fff1bc" opacity="0.62"/></svg>'
    }
  };

  const PAYLINES = [
    [1, 1, 1, 1, 1], [0, 0, 0, 0, 0], [2, 2, 2, 2, 2], [0, 1, 2, 1, 0], [2, 1, 0, 1, 2],
    [0, 0, 1, 0, 0], [2, 2, 1, 2, 2], [1, 0, 0, 0, 1], [1, 2, 2, 2, 1], [0, 1, 1, 1, 0],
    [2, 1, 1, 1, 2], [1, 0, 1, 2, 1], [1, 2, 1, 0, 1], [0, 1, 0, 1, 0], [2, 1, 2, 1, 2],
    [0, 1, 2, 2, 2], [2, 1, 0, 0, 0], [0, 0, 0, 1, 2], [2, 2, 2, 1, 0], [1, 1, 0, 1, 1]
  ];

  const REEL_COUNT = 5;
  const ROW_COUNT = 3;
  const REEL_REPEAT = 7;
  const INITIAL_BALANCE = Number(Math.max(0, cash).toFixed(2));
  const MIN_TOTAL_BET = 0.2;
  const BIG_WIN_MULTIPLIER = 25;
  const MEGA_WIN_MULTIPLIER = 75;
  const EPIC_WIN_MULTIPLIER = 150;
  const LEGENDARY_WIN_MULTIPLIER = 300;
  const WILD_SYMBOL = "wild";
  const SCATTER_SYMBOL = "scatter";
  const SCATTER_PAYS = { 3: 2, 4: 10, 5: 25 };
  const FREE_SPINS_BY_SCATTER = { 3: 8, 4: 12, 5: 20 };
  const PAYLINE_COLOR_PALETTE = [
    "#5ec6ff", "#ff9d67", "#b67cff", "#ffd45f", "#72f1c8",
    "#ff78a6", "#88b1ff", "#c1ff64", "#7af0ff", "#ffb47a",
    "#d08eff", "#ffeb6d", "#79ffc9", "#ff90b9", "#9ec1ff",
    "#d8ff80", "#8ff4ff", "#ffc294", "#e5a8ff", "#f7ff8d"
  ];
  const WINNING_LINE_NEON_PALETTE = [
    "#39ff14", "#00f7ff", "#ff4dff", "#ffe600", "#ff7a00",
    "#7d7dff", "#00ffa8", "#ff2f92", "#7df9ff", "#f7ff00"
  ];

  const state = {
    mode: "manual",
    autoRunning: false,
    lastWinningLineWins: [],
    phase: "idle",
    startingBalance: INITIAL_BALANCE,
    balance: INITIAL_BALANCE,
    betPerLine: 0.5,
    lines: 20,
    freeSpins: 0,
    reels: [],
    currentIndices: Array(REEL_COUNT).fill(0),
    totalPayout: 0,
    displayedPayout: 0,
    message: "Ready",
    payoutAnimFrame: null,
    chartPoints: [],
    stats: {
      wagered: 0,
      wins: 0,
      losses: 0,
      freeSpinsWon: 0
    }
  };

  const ui = {
    appShell: root.querySelector("#appShell"),
    manualModeBtn: root.querySelector("#manualModeBtn"),
    autoModeBtn: root.querySelector("#autoModeBtn"),
    betDownBtn: root.querySelector("#betDownBtn"),
    betUpBtn: root.querySelector("#betUpBtn"),
    totalBetInput: root.querySelector("#totalBetInput"),
    linesSelect: root.querySelector("#linesSelect"),
    betPerLineInput: root.querySelector("#betPerLineInput"),
    betBtn: root.querySelector("#betBtn"),
    balanceStat: root.querySelector("#balanceStat"),
    profitStat: root.querySelector("#profitStat"),
    wageredStat: root.querySelector("#wageredStat"),
    winsStat: root.querySelector("#winsStat"),
    lossesStat: root.querySelector("#lossesStat"),
    stateStat: root.querySelector("#stateStat"),
    freeSpinsStat: root.querySelector("#freeSpinsStat"),
    resultsChart: root.querySelector("#resultsChart"),
    totalPayoutValue: root.querySelector("#totalPayoutValue"),
    slotMachine: root.querySelector("#slotMachine"),
    fxOverlay: root.querySelector("#fxOverlay"),
    slotFrame: root.querySelector("#slotFrame"),
    flashOverlay: root.querySelector("#flashOverlay"),
    reels: root.querySelector("#reels"),
    paylineOverlayBack: root.querySelector("#paylineOverlayBack"),
    paylineOverlayFront: root.querySelector("#paylineOverlayFront"),
    paylineLeft: root.querySelector("#paylineLeft"),
    paylineRight: root.querySelector("#paylineRight"),
    messageLine: root.querySelector("#messageLine"),
    reelEls: [],
    reelStrips: []
  };

  function clamp(value, min, max) {
    if (!Number.isFinite(value)) return min;
    return Math.min(max, Math.max(min, value));
  }

  function round(value) {
    return Math.round(value * 100) / 100;
  }

  function shuffleArray(array) {
    for (let i = array.length - 1; i > 0; i -= 1) {
      const j = Math.floor(Math.random() * (i + 1));
      [array[i], array[j]] = [array[j], array[i]];
    }
    return array;
  }

  function easeOutCubic(t) {
    return 1 - Math.pow(1 - t, 3);
  }

  function easeOutBack(t) {
    const s = 1.5;
    const inv = t - 1;
    return 1 + (s + 1) * Math.pow(inv, 3) + s * Math.pow(inv, 2);
  }

  function sleep(ms) {
    return new Promise((resolve) => {
      if (destroyed) {
        resolve();
        return;
      }
      const id = window.setTimeout(() => {
        pendingTimeouts.delete(id);
        resolve();
      }, ms);
      pendingTimeouts.add(id);
    });
  }

  function syncGlobalCash() {
    cash = Number(Math.max(0, state.balance).toFixed(2));
    updateUI();
    updateBlackjackCash();
    updateFullscreenCash();
  }

  function setMessage(text) {
    state.message = text;
  }

  function getStateLabel() {
    if (state.mode === "auto" && state.autoRunning && state.phase === "idle") return "Auto Mode";
    if (state.freeSpins > 0 && state.phase === "idle") return "Free Spins Ready";
    if (state.phase === "spinning") return "Spinning";
    if (state.phase === "evaluating") return "Evaluating";
    if (state.phase === "win") return "Win Animation";
    if (state.phase === "big-win") return "Big Win Animation";
    return "Idle";
  }

  function getTotalBet() {
    return round(state.betPerLine * state.lines);
  }

  function stopAutoMode(message) {
    state.autoRunning = false;
    resetAutoRoundCounter("slots");
    if (message) setMessage(message);
  }

  function pulseWarning() {
    ui.betBtn.classList.remove("warn");
    ui.betBtn.offsetWidth;
    ui.betBtn.classList.add("warn");
    setTrackedTimeout(() => ui.betBtn.classList.remove("warn"), 400);
  }

  function updateBalance(amount) {
    state.balance = round(state.balance + amount);
    syncGlobalCash();
  }

  function setBetFromTotal(totalBetInput) {
    const safeTotal = clamp(round(Number.isFinite(totalBetInput) ? totalBetInput : getTotalBet()), MIN_TOTAL_BET, 100000);
    state.betPerLine = round(safeTotal / state.lines);
    syncInputsFromState();
    renderUI();
  }

  function createShuffledReel() {
    const weighted = [];
    Object.entries(SYMBOLS).forEach(([key, data]) => {
      for (let i = 0; i < data.frequency; i += 1) weighted.push(key);
    });

    let result = [];
    while (result.length < 32) {
      result = result.concat(shuffleArray([...weighted]));
    }
    return shuffleArray(result).slice(0, 32);
  }

  function createSymbolCell(symbolKey) {
    const symbol = document.createElement("div");
    symbol.className = `symbol symbol-${symbolKey}`;
    symbol.dataset.symbol = symbolKey;
    symbol.innerHTML = `<div class="symbol-inner">${SYMBOLS[symbolKey].icon}</div>`;
    return symbol;
  }

  function mountReels() {
    ui.reels.innerHTML = "";
    ui.reelEls = [];
    ui.reelStrips = [];

    state.reels.forEach((reelSymbols) => {
      const reelEl = document.createElement("div");
      reelEl.className = "reel";

      const strip = document.createElement("div");
      strip.className = "reel-strip";

      for (let repeat = 0; repeat < REEL_REPEAT; repeat += 1) {
        reelSymbols.forEach((symbolKey) => {
          strip.appendChild(createSymbolCell(symbolKey));
        });
      }

      reelEl.appendChild(strip);
      ui.reels.appendChild(reelEl);
      ui.reelEls.push(reelEl);
      ui.reelStrips.push(strip);
      strip.style.transform = "translateY(0px)";
    });
  }

  function getCellHeight() {
    const firstStrip = ui.reelStrips[0];
    const firstSymbol = firstStrip ? firstStrip.querySelector(".symbol") : null;
    if (firstSymbol) {
      const symbolHeight = firstSymbol.getBoundingClientRect().height;
      if (Number.isFinite(symbolHeight) && symbolHeight > 0) return symbolHeight;
    }
    const firstReel = ui.reelEls[0];
    if (!firstReel) return 120;
    const reelHeight = firstReel.getBoundingClientRect().height;
    return reelHeight > 0 ? reelHeight / ROW_COUNT : 120;
  }

  function normalizeReelTransforms() {
    const cellHeight = getCellHeight();
    state.currentIndices.forEach((index, reelIndex) => {
      const strip = ui.reelStrips[reelIndex];
      if (!strip) return;
      const offset = Math.round(index * cellHeight * 1000) / 1000;
      strip.style.transform = `translateY(${-offset}px)`;
    });
  }

  function populatePaylineSelector() {
    const fragment = document.createDocumentFragment();
    for (let i = 1; i <= PAYLINES.length; i += 1) {
      const option = document.createElement("option");
      option.value = String(i);
      option.textContent = `${i} ${i === 1 ? "Line" : "Lines"}`;
      fragment.appendChild(option);
    }
    ui.linesSelect.appendChild(fragment);
  }

  function buildPaylineNumbers() {
    ui.paylineLeft.innerHTML = "";
    ui.paylineRight.innerHTML = "";
    const leftOrder = [4, 10, 2, 6, 12, 1, 8, 7, 3, 11];
    const rightOrder = [15, 13, 19, 16, 17, 9, 18, 20, 14, 5];

    leftOrder.forEach((lineNo) => {
      const item = document.createElement("span");
      item.textContent = String(lineNo);
      ui.paylineLeft.appendChild(item);
    });
    rightOrder.forEach((lineNo) => {
      const item = document.createElement("span");
      item.textContent = String(lineNo);
      ui.paylineRight.appendChild(item);
    });
  }

  function getPaylineOverlaySize() {
    const width = ui.reels.clientWidth;
    const height = ui.reels.clientHeight;
    if (width <= 0 || height <= 0) return null;
    return { width, height };
  }

  function getPaylinePoints(pattern, width, height) {
    const reelWidth = width / REEL_COUNT;
    const rowHeight = height / ROW_COUNT;
    return pattern
      .map((row, reel) => {
        const x = reelWidth * (reel + 0.5);
        const y = rowHeight * (row + 0.5);
        return `${x.toFixed(2)},${y.toFixed(2)}`;
      })
      .join(" ");
  }

  function renderBackgroundPaylines() {
    const size = getPaylineOverlaySize();
    if (!size) return;

    const { width, height } = size;
    ui.paylineOverlayBack.innerHTML = "";
    ui.paylineOverlayBack.setAttribute("viewBox", `0 0 ${width} ${height}`);

    const activeLineCount = Math.min(state.lines, PAYLINES.length);
    for (let lineIndex = 0; lineIndex < activeLineCount; lineIndex += 1) {
      const path = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
      path.classList.add("payline-path");
      path.setAttribute("points", getPaylinePoints(PAYLINES[lineIndex], width, height));
      path.style.stroke = PAYLINE_COLOR_PALETTE[lineIndex % PAYLINE_COLOR_PALETTE.length];
      ui.paylineOverlayBack.appendChild(path);
    }
  }

  function clearWinningPaylines() {
    ui.paylineOverlayFront.innerHTML = "";
  }

  function renderWinningPaylines(lineWins = []) {
    const size = getPaylineOverlaySize();
    if (!size) return;

    const { width, height } = size;
    ui.paylineOverlayFront.innerHTML = "";
    ui.paylineOverlayFront.setAttribute("viewBox", `0 0 ${width} ${height}`);

    const winningLineCounts = new Map();
    lineWins.forEach((lineWin) => {
      const lineIndex = lineWin.line - 1;
      if (!winningLineCounts.has(lineIndex) || lineWin.count > winningLineCounts.get(lineIndex)) {
        winningLineCounts.set(lineIndex, lineWin.count);
      }
    });

    Array.from(winningLineCounts.keys()).forEach((lineIndex, orderIndex) => {
      const path = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
      path.classList.add("payline-path", "is-winning");
      path.setAttribute("points", getPaylinePoints(PAYLINES[lineIndex], width, height));
      const neonColor = WINNING_LINE_NEON_PALETTE[orderIndex % WINNING_LINE_NEON_PALETTE.length];
      path.style.stroke = neonColor;
      path.style.filter = `drop-shadow(0 0 8px ${neonColor}) drop-shadow(0 0 18px ${neonColor})`;
      ui.paylineOverlayFront.appendChild(path);
    });
  }

  function getVisibleGrid(indices = state.currentIndices) {
    const grid = [[], [], []];
    for (let reel = 0; reel < REEL_COUNT; reel += 1) {
      const reelSymbols = state.reels[reel];
      const stop = indices[reel];
      for (let row = 0; row < ROW_COUNT; row += 1) {
        grid[row][reel] = reelSymbols[(stop + row) % reelSymbols.length];
      }
    }
    return grid;
  }

  function evaluateLineMatch(lineSymbols) {
    let baseSymbol = null;
    let count = 0;

    for (let reel = 0; reel < lineSymbols.length; reel += 1) {
      const symbol = lineSymbols[reel];
      if (symbol === SCATTER_SYMBOL) break;
      if (symbol === WILD_SYMBOL) {
        count += 1;
        continue;
      }
      if (baseSymbol == null) {
        baseSymbol = symbol;
        count += 1;
        continue;
      }
      if (symbol === baseSymbol) {
        count += 1;
        continue;
      }
      break;
    }

    if (count < 3) return null;
    if (baseSymbol == null) {
      if (count === 5 && (SYMBOLS[WILD_SYMBOL]?.pays?.[5] ?? 0) > 0) return { symbol: WILD_SYMBOL, count: 5 };
      return null;
    }
    return { symbol: baseSymbol, count };
  }

  function evaluateScatter(grid, totalBet) {
    const scatterCells = [];
    for (let row = 0; row < ROW_COUNT; row += 1) {
      for (let reel = 0; reel < REEL_COUNT; reel += 1) {
        if (grid[row][reel] === SCATTER_SYMBOL) scatterCells.push({ reel, row });
      }
    }

    const scatterCount = scatterCells.length;
    if (scatterCount < 3) {
      return { scatterCount, scatterCells, scatterWin: 0, freeSpinsAwarded: 0 };
    }

    const cappedCount = Math.min(scatterCount, 5);
    return {
      scatterCount,
      scatterCells,
      scatterWin: round(totalBet * (SCATTER_PAYS[cappedCount] || 0)),
      freeSpinsAwarded: FREE_SPINS_BY_SCATTER[cappedCount] || 0
    };
  }

  function calculateWinsForIndices(indices, totalBet = getTotalBet()) {
    const grid = getVisibleGrid(indices);
    const lineWins = [];
    let totalWin = 0;

    for (let lineIndex = 0; lineIndex < state.lines; lineIndex += 1) {
      const pattern = PAYLINES[lineIndex];
      const lineSymbols = pattern.map((row, reel) => grid[row][reel]);
      const lineMatch = evaluateLineMatch(lineSymbols);
      if (!lineMatch) continue;

      const { symbol, count } = lineMatch;
      const lineMultiplier = SYMBOLS[symbol]?.pays?.[count] ?? 0;
      if (lineMultiplier <= 0) continue;

      const lineAmount = round(state.betPerLine * lineMultiplier);
      totalWin = round(totalWin + lineAmount);
      lineWins.push({
        line: lineIndex + 1,
        count,
        symbol,
        amount: lineAmount,
        multiplier: lineMultiplier
      });
    }

    const scatterSummary = evaluateScatter(grid, totalBet);
    totalWin = round(totalWin + scatterSummary.scatterWin);
    return {
      totalWin,
      lineWins,
      scatterCount: scatterSummary.scatterCount,
      freeSpinsAwarded: scatterSummary.freeSpinsAwarded
    };
  }

  function calculateWins(totalBet = getTotalBet()) {
    return calculateWinsForIndices(state.currentIndices, totalBet);
  }

  function getVisibleSymbolElement(reel, row) {
    const strip = ui.reelStrips[reel];
    if (!strip) return null;
    return strip.children[state.currentIndices[reel] + row] || null;
  }

  function applyWinningHighlightsFromLines(lineWins) {
    const dedup = new Set();
    lineWins.forEach((lineWin) => {
      const lineIndex = lineWin.line - 1;
      const pattern = PAYLINES[lineIndex];
      const maxCount = Math.min(lineWin.count, REEL_COUNT);
      for (let reel = 0; reel < maxCount; reel += 1) {
        const row = pattern[reel];
        const key = `${reel}:${row}`;
        if (dedup.has(key)) continue;
        dedup.add(key);
        const symbolEl = getVisibleSymbolElement(reel, row);
        if (symbolEl) symbolEl.classList.add("win-highlight");
      }
    });
  }

  function animatePayoutValue(target) {
    if (state.payoutAnimFrame) window.cancelAnimationFrame(state.payoutAnimFrame);

    const startValue = state.displayedPayout;
    const change = target - startValue;
    const duration = 520;
    let startTime = 0;

    function frame(now) {
      if (destroyed) return;
      if (startTime === 0) startTime = now;
      const progress = Math.min((now - startTime) / duration, 1);
      state.displayedPayout = round(startValue + change * easeOutCubic(progress));
      ui.totalPayoutValue.textContent = `$${state.displayedPayout.toFixed(2)}`;

      if (progress < 1) {
        state.payoutAnimFrame = window.requestAnimationFrame(frame);
      } else {
        state.displayedPayout = target;
        ui.totalPayoutValue.textContent = `$${target.toFixed(2)}`;
        state.payoutAnimFrame = null;
      }
    }

    state.payoutAnimFrame = window.requestAnimationFrame(frame);
  }

  function renderChart() {
    const canvas = ui.resultsChart;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const dpr = window.devicePixelRatio || 1;
    const width = canvas.clientWidth;
    const height = canvas.clientHeight;
    canvas.width = Math.floor(width * dpr);
    canvas.height = Math.floor(height * dpr);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);

    ctx.strokeStyle = "rgba(116, 138, 157, 0.14)";
    ctx.lineWidth = 1;
    for (let i = 1; i <= 3; i += 1) {
      const y = (height / 4) * i;
      ctx.beginPath();
      ctx.moveTo(8, y);
      ctx.lineTo(width - 8, y);
      ctx.stroke();
    }

    if (state.chartPoints.length === 0) return;

    const min = Math.min(...state.chartPoints, -1);
    const max = Math.max(...state.chartPoints, 1);
    const spread = Math.max(max - min, 1);
    const xStep = state.chartPoints.length > 1 ? (width - 18) / (state.chartPoints.length - 1) : 0;
    const points = state.chartPoints.map((value, index) => {
      const x = 9 + index * xStep;
      const y = height - 10 - ((value - min) / spread) * (height - 20);
      return { x, y };
    });

    const grad = ctx.createLinearGradient(0, 0, width, height);
    grad.addColorStop(0, "#d71a66");
    grad.addColorStop(1, "#a10e4a");

    const fillGrad = ctx.createLinearGradient(0, 0, 0, height);
    fillGrad.addColorStop(0, "rgba(197, 20, 89, 0.28)");
    fillGrad.addColorStop(1, "rgba(75, 7, 37, 0.06)");

    ctx.strokeStyle = grad;
    ctx.lineWidth = 2.4;
    ctx.lineJoin = "round";
    ctx.lineCap = "round";
    ctx.beginPath();
    ctx.moveTo(points[0].x, points[0].y);
    for (let i = 1; i < points.length; i += 1) {
      const prev = points[i - 1];
      const curr = points[i];
      const controlX = (prev.x + curr.x) / 2;
      ctx.quadraticCurveTo(controlX, prev.y, curr.x, curr.y);
    }
    ctx.lineTo(points[points.length - 1].x, height - 8);
    ctx.lineTo(points[0].x, height - 8);
    ctx.closePath();
    ctx.fillStyle = fillGrad;
    ctx.fill();

    ctx.beginPath();
    ctx.moveTo(points[0].x, points[0].y);
    for (let i = 1; i < points.length; i += 1) {
      const prev = points[i - 1];
      const curr = points[i];
      const controlX = (prev.x + curr.x) / 2;
      ctx.quadraticCurveTo(controlX, prev.y, curr.x, curr.y);
    }
    ctx.stroke();

    const last = points[points.length - 1];
    ctx.fillStyle = "#e24486";
    ctx.beginPath();
    ctx.arc(last.x, last.y, 3.6, 0, Math.PI * 2);
    ctx.fill();
  }

  function spawnParticles(count) {
    const bounds = ui.slotMachine.getBoundingClientRect();
    const centerX = bounds.width / 2;
    const centerY = bounds.height / 2;
    const particles = [];

    for (let i = 0; i < count; i += 1) {
      const particle = document.createElement("div");
      particle.className = "particle";
      ui.slotMachine.appendChild(particle);
      const angle = Math.random() * Math.PI * 2;
      const velocity = 1.5 + Math.random() * 4.5;
      particles.push({
        el: particle,
        x: centerX,
        y: centerY,
        vx: Math.cos(angle) * velocity,
        vy: Math.sin(angle) * velocity - 0.7,
        life: 0,
        maxLife: 34 + Math.random() * 18
      });
    }

    function tick() {
      if (destroyed) {
        particles.forEach((p) => p.el.remove());
        return;
      }
      let active = 0;
      particles.forEach((p) => {
        if (p.life >= p.maxLife) {
          p.el.remove();
          return;
        }
        active += 1;
        p.life += 1;
        p.x += p.vx;
        p.y += p.vy;
        p.vy += 0.08;

        const alpha = 1 - p.life / p.maxLife;
        p.el.style.transform = `translate(${p.x}px, ${p.y}px) scale(${0.6 + alpha * 0.6})`;
        p.el.style.opacity = alpha.toFixed(3);
      });
      if (active > 0) window.requestAnimationFrame(tick);
    }
    window.requestAnimationFrame(tick);
  }

  function runBigWinEffects(tier = "big") {
    const particleCount = tier === "legendary" ? 180 : tier === "epic" ? 130 : tier === "mega" ? 90 : 56;
    const pulseDuration = tier === "legendary" ? 1200 : tier === "epic" ? 980 : tier === "mega" ? 820 : 700;

    ui.appShell.classList.add("big-win-shake");
    ui.slotFrame.classList.add("win-pulse");
    ui.flashOverlay.classList.add("active");

    setTrackedTimeout(() => {
      ui.appShell.classList.remove("big-win-shake");
      ui.slotFrame.classList.remove("win-pulse");
      ui.flashOverlay.classList.remove("active");
    }, pulseDuration);

    if (tier === "epic" || tier === "legendary") {
      setTrackedTimeout(() => ui.flashOverlay.classList.add("active"), 120);
      setTrackedTimeout(() => ui.flashOverlay.classList.remove("active"), 420);
    }

    spawnParticles(particleCount);
  }

  function clearRoundEffects() {
    ui.slotFrame.classList.remove("win-pulse", "loss-tint");
    ui.flashOverlay.classList.remove("active");
    ui.appShell.classList.remove("big-win-shake");
    clearWinningPaylines();
    ui.reels.querySelectorAll(".win-highlight").forEach((node) => node.classList.remove("win-highlight"));
  }

  function getWinTier(totalWin, totalBet) {
    if (totalWin >= totalBet * LEGENDARY_WIN_MULTIPLIER) return "legendary";
    if (totalWin >= totalBet * EPIC_WIN_MULTIPLIER) return "epic";
    if (totalWin >= totalBet * MEGA_WIN_MULTIPLIER) return "mega";
    if (totalWin >= totalBet * BIG_WIN_MULTIPLIER) return "big";
    return "regular";
  }

  function chooseStopIndices() {
    return state.reels.map((reel) => Math.floor(Math.random() * reel.length));
  }

  function animateReels(stopIndices, session) {
    const cellHeight = getCellHeight();
    const animations = stopIndices.map((targetIndex, reelIndex) => {
      const reel = ui.reelEls[reelIndex];
      const strip = ui.reelStrips[reelIndex];
      const reelLength = state.reels[reelIndex].length;
      const startIndex = state.currentIndices[reelIndex];
      const normalizedDelta = (targetIndex - startIndex + reelLength) % reelLength;
      const travel = 4 * reelLength + normalizedDelta;
      const delay = reelIndex * 150;
      const duration = 980 + reelIndex * 190;
      reel.classList.add("is-spinning");

      return new Promise((resolve) => {
        let startTime = 0;
        function frame(now) {
          if (destroyed || session !== spinSession) {
            reel.classList.remove("is-spinning");
            resolve();
            return;
          }

          if (startTime === 0) startTime = now;
          const elapsed = now - startTime - delay;
          if (elapsed < 0) {
            window.requestAnimationFrame(frame);
            return;
          }

          const progress = Math.min(elapsed / duration, 1);
          const eased = easeOutBack(progress);
          const offset = startIndex + travel * eased;
          const offsetPx = Math.round(offset * cellHeight * 1000) / 1000;
          strip.style.transform = `translateY(${-offsetPx}px)`;

          if (progress < 1) {
            window.requestAnimationFrame(frame);
            return;
          }

          state.currentIndices[reelIndex] = targetIndex;
          const snappedOffset = Math.round(targetIndex * cellHeight * 1000) / 1000;
          strip.style.transform = `translateY(${-snappedOffset}px)`;
          reel.classList.remove("is-spinning");
          resolve();
        }
        window.requestAnimationFrame(frame);
      });
    });

    return Promise.all(animations);
  }

  async function spinReels() {
    if (destroyed) return;
    if (state.phase === "spinning" || state.phase === "evaluating") return;
    if (!ensureCasinoBettingAllowedNow()) return;

    const session = spinSession;
    playGameSound("slots_spin", { restart: true, allowOverlap: false });
    const totalBet = getTotalBet();
    const isFreeSpin = state.freeSpins > 0;
    if (state.mode === "auto" && state.autoRunning && !isFreeSpin) {
      const allowed = consumeAutoRoundCounter("slots");
      if (!allowed) {
        stopAutoMode("Auto stopped: round limit reached.");
        renderUI();
        return;
      }
    }

    if (!isFreeSpin && state.balance < totalBet) {
      pulseWarning();
      stopAutoMode("Insufficient balance.");
      setMessage("Insufficient balance for this bet.");
      renderUI();
      return;
    }

    clearRoundEffects();

    if (isFreeSpin) {
      state.freeSpins = Math.max(0, state.freeSpins - 1);
    } else {
      updateBalance(-totalBet);
      state.stats.wagered = round(state.stats.wagered + totalBet);
    }

    state.phase = "spinning";
    setMessage(isFreeSpin ? `Free Spin (${state.freeSpins} left)...` : "Spinning reels...");
    renderUI();

    const stopIndices = chooseStopIndices();
    await animateReels(stopIndices, session);
    if (destroyed || session !== spinSession) return;

    state.phase = "evaluating";
    renderUI();

    const result = calculateWins(totalBet);
    if (shouldRigHighBet(totalBet, 1.05) && result.totalWin > 0) {
      result.totalWin = 0;
      result.winningCells = [];
      result.lineWins = [];
      result.scatterCount = 0;
      result.scatterWin = 0;
      result.freeSpinsAwarded = 0;
    }
    state.lastWinningLineWins = result.lineWins;
    renderWinningPaylines(result.lineWins);
    applyWinningHighlightsFromLines(result.lineWins);

    if (result.freeSpinsAwarded > 0) {
      state.freeSpins += result.freeSpinsAwarded;
      state.stats.freeSpinsWon += result.freeSpinsAwarded;
    }

    const net = round(result.totalWin - (isFreeSpin ? 0 : totalBet));
    state.chartPoints.push(net);
    if (state.chartPoints.length > 28) state.chartPoints.shift();

    let winTier = "regular";

    if (result.totalWin > 0) {
      updateBalance(result.totalWin);
      if (net > 0) {
        showCasinoWinPopup({
          amount: net,
          multiplier: totalBet > 0 ? result.totalWin / totalBet : null
        });
      }
      state.totalPayout = round(state.totalPayout + result.totalWin);
      animatePayoutValue(state.totalPayout);
      state.stats.wins += 1;

      winTier = getWinTier(result.totalWin, totalBet);
      const highTier = winTier !== "regular";
      state.phase = highTier ? "big-win" : "win";

      if (highTier) {
        runBigWinEffects(winTier);
      } else {
        ui.slotFrame.classList.add("win-pulse");
        setTrackedTimeout(() => ui.slotFrame.classList.remove("win-pulse"), 750);
      }

      const tierText = winTier === "regular" ? "" : ` ${winTier.toUpperCase()}!`;
      const lineText = result.lineWins.length > 0 ? ` on ${result.lineWins.length} line${result.lineWins.length === 1 ? "" : "s"}` : "";
      const scatterText = result.scatterCount >= 3 ? ` + ${result.scatterCount} Scatter` : "";
      const freeSpinText = result.freeSpinsAwarded > 0 ? ` +${result.freeSpinsAwarded} Free Spins` : "";
      setMessage(`Win $${result.totalWin.toFixed(2)}${tierText}${lineText}${scatterText}${freeSpinText}.`);
    } else {
      state.lastWinningLineWins = [];
      clearWinningPaylines();
      state.stats.losses += 1;
      state.phase = "idle";
      ui.slotFrame.classList.add("loss-tint");
      setTrackedTimeout(() => ui.slotFrame.classList.remove("loss-tint"), 460);
      if (result.freeSpinsAwarded > 0) {
        setMessage(`${result.scatterCount} Scatter: +${result.freeSpinsAwarded} Free Spins.`);
      } else {
        setMessage("No win this spin.");
      }
      playGameSound("loss");
    }

    renderChart();
    renderUI();
    if (destroyed || session !== spinSession) return;

    if (state.phase === "win" || state.phase === "big-win") {
      state.phase = "idle";
      renderUI();
    }

    triggerCasinoKickoutCheckAfterRound();

    if (state.freeSpins > 0) {
      await sleep(360);
      if (!destroyed && session === spinSession) spinReels();
      return;
    }

    if (state.mode === "auto" && state.autoRunning) {
      if (state.balance >= getTotalBet()) {
        await sleep(320);
        if (!destroyed && session === spinSession && state.mode === "auto" && state.autoRunning) {
          spinReels();
        }
      } else {
        stopAutoMode("Auto stopped: low balance.");
        renderUI();
      }
    }
  }

  function renderUI() {
    const totalBet = getTotalBet();
    ui.manualModeBtn.classList.toggle("active", state.mode === "manual");
    ui.autoModeBtn.classList.toggle("active", state.mode === "auto");

    const isBusy = state.phase === "spinning" || state.phase === "evaluating";
    ui.betBtn.disabled = isBusy;

    if (state.mode === "manual") ui.betBtn.textContent = "Bet";
    else if (state.autoRunning) ui.betBtn.textContent = "Stop Auto";
    else ui.betBtn.textContent = "Auto Bet";

    syncInputsFromState();
    ui.balanceStat.textContent = `$${state.balance.toFixed(2)}`;
    ui.wageredStat.textContent = `$${state.stats.wagered.toFixed(2)}`;
    ui.winsStat.textContent = String(state.stats.wins);
    ui.lossesStat.textContent = String(state.stats.losses);
    ui.freeSpinsStat.textContent = String(state.freeSpins);

    const profit = round(state.balance - state.startingBalance);
    ui.profitStat.textContent = `${profit >= 0 ? "+" : ""}$${profit.toFixed(2)}`;
    ui.profitStat.classList.toggle("profit-pos", profit >= 0);
    ui.profitStat.classList.toggle("profit-neg", profit < 0);
    ui.stateStat.textContent = getStateLabel();
    if (state.payoutAnimFrame == null) ui.totalPayoutValue.textContent = `$${state.totalPayout.toFixed(2)}`;

    ui.messageLine.textContent = state.message;
    ui.totalBetInput.classList.toggle("profit-neg", state.balance < totalBet);
  }

  function syncInputsFromState() {
    ui.linesSelect.value = String(state.lines);
    ui.betPerLineInput.value = state.betPerLine.toFixed(2);
    ui.totalBetInput.value = getTotalBet().toFixed(2);
  }

  function onResize() {
    if (destroyed) return;
    normalizeReelTransforms();
    renderBackgroundPaylines();
    renderWinningPaylines(state.lastWinningLineWins);
    renderChart();
  }

  function bindEvents() {
    ui.manualModeBtn.addEventListener("click", () => {
      if (destroyed || state.mode === "manual") return;
      state.mode = "manual";
      stopAutoMode("Manual mode active.");
      renderUI();
    });

    ui.autoModeBtn.addEventListener("click", () => {
      if (destroyed || state.mode === "auto") return;
      state.mode = "auto";
      setMessage("Auto mode selected.");
      renderUI();
    });

    ui.betDownBtn.addEventListener("click", () => setBetFromTotal(getTotalBet() - 0.2));
    ui.betUpBtn.addEventListener("click", () => setBetFromTotal(getTotalBet() + 0.2));
    ui.totalBetInput.addEventListener("change", () => {
      setBetFromTotal(Number.parseFloat(ui.totalBetInput.value));
    });

    ui.betPerLineInput.addEventListener("change", () => {
      state.betPerLine = round(clamp(Number.parseFloat(ui.betPerLineInput.value), 0.01, 1000));
      syncInputsFromState();
      renderUI();
    });

    ui.linesSelect.addEventListener("change", () => {
      state.lines = Number.parseInt(ui.linesSelect.value, 10);
      if (getTotalBet() < MIN_TOTAL_BET) {
        state.betPerLine = round(MIN_TOTAL_BET / state.lines);
      }
      state.lastWinningLineWins = [];
      clearWinningPaylines();
      renderBackgroundPaylines();
      syncInputsFromState();
      renderUI();
    });

    ui.betBtn.addEventListener("pointerdown", () => {
      if (destroyed) return;
      if (state.phase === "spinning" || state.phase === "evaluating") return;
      if (state.mode === "auto" && state.autoRunning) return;
      playGameSound("slots_spin", { restart: true, allowOverlap: false });
    });

    ui.betBtn.addEventListener("click", () => {
      if (state.mode === "auto") {
        if (state.autoRunning) {
          stopAutoMode("Auto mode stopped.");
          spinSession += 1;
          renderUI();
          return;
        }
        state.autoRunning = true;
        resetAutoRoundCounter("slots");
        setMessage("Auto mode active.");
        renderUI();
      }
      spinReels();
    });

    window.addEventListener("resize", onResize);
  }

  function initialize() {
    populatePaylineSelector();
    buildPaylineNumbers();
    state.reels = Array.from({ length: REEL_COUNT }, () => createShuffledReel());
    mountReels();
    normalizeReelTransforms();
    renderBackgroundPaylines();
    bindEvents();
    syncInputsFromState();
    renderChart();
    syncGlobalCash();
    setMessage("Place your bet to spin.");
    renderUI();
  }

  initialize();

  activeCasinoCleanup = () => {
    destroyed = true;
    spinSession += 1;
    state.autoRunning = false;
    container.classList.remove("casino-fullbleed", "slots-fullbleed");
    clearTrackedTimeouts();
    if (state.payoutAnimFrame) {
      window.cancelAnimationFrame(state.payoutAnimFrame);
      state.payoutAnimFrame = null;
    }
    window.removeEventListener("resize", onResize);
  };
}

// ------------------ BLACKJACK ------------------
let hands = [[]],
  handBets = [0],
  dealerHand = [],
  gameOver = false,
  bet = 0;
let blackjackDealInProgress = false;
let activeHandIndex = 0;
let blackjackFxTimeout = null;
let blackjackRenderSnapshot = { dealer: [], hands: [] };
let blackjackSelectedHandCount = 1;

const blackjackRanks = ["A", "2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K"];
const blackjackSuits = ["♠", "♥", "♦", "♣"];
const BLACKJACK_DECK_COUNT = 1;
const BLACKJACK_RESHUFFLE_THRESHOLD = 15;
const BLACKJACK_MIN_HANDS = 1;
const BLACKJACK_MAX_HANDS = 5;
let blackjackDeck = [];
let blackjackNeedsReshuffle = true;
let blackjackOutOfCardsThisHand = false;

function createBlackjackDeck(deckCount = BLACKJACK_DECK_COUNT) {
  const freshDeck = [];
  const count = Math.max(1, Math.floor(Number(deckCount) || 1));
  for (let shoe = 0; shoe < count; shoe += 1) {
    blackjackRanks.forEach((rank) => {
      blackjackSuits.forEach((suit) => {
        freshDeck.push({ v: rank, s: suit });
      });
    });
  }
  return freshDeck;
}

function shuffleBlackjackDeck(deck) {
  for (let i = deck.length - 1; i > 0; i -= 1) {
    const j = Math.floor(Math.random() * (i + 1));
    [deck[i], deck[j]] = [deck[j], deck[i]];
  }
  return deck;
}

function shouldReshuffleBlackjack() {
  return blackjackNeedsReshuffle || blackjackDeck.length < BLACKJACK_RESHUFFLE_THRESHOLD;
}

function prepareBlackjackShoe({ force = false } = {}) {
  if (!force && !shouldReshuffleBlackjack()) return false;
  blackjackDeck = shuffleBlackjackDeck(createBlackjackDeck(BLACKJACK_DECK_COUNT));
  blackjackNeedsReshuffle = false;
  blackjackOutOfCardsThisHand = false;
  updateBlackjackDeckDisplay();
  return true;
}

function blackjackCardKey(card) {
  if (!card) return "";
  return `${card.v}${card.s}`;
}

function resetBlackjackRenderSnapshot() {
  blackjackRenderSnapshot = { dealer: [], hands: [] };
}

function updateBlackjackDeckDisplay() {
  const deckCount = document.getElementById("bjDeckCount");
  if (!deckCount) return;
  deckCount.textContent = `${blackjackDeck.length} left`;
}

function flashBlackjackDeckDraw() {
  const deckStack = document.getElementById("bjDeckStack");
  if (!deckStack) return;
  deckStack.classList.remove("draw");
  void deckStack.offsetWidth;
  deckStack.classList.add("draw");
}

function loadBlackjack() {
  const blackjackContainer = document.getElementById("casino-container");
  if (blackjackContainer && !IS_PHONE_EMBED_MODE) {
    blackjackContainer.classList.add("casino-fullbleed", "blackjack-fullbleed");
  }

  gameOver = true;
  blackjackDealInProgress = false;
  blackjackDeck = [];
  blackjackNeedsReshuffle = true;
  blackjackOutOfCardsThisHand = false;
  hands = [[]];
  handBets = [0];
  dealerHand = [];
  activeHandIndex = 0;
  resetBlackjackRenderSnapshot();
  if (blackjackFxTimeout) {
    clearTimeout(blackjackFxTimeout);
    blackjackFxTimeout = null;
  }

  document.getElementById("casino-container").innerHTML = `
    <div class="blackjack-root">
      <div class="app">
        <aside class="sidebar">
          <div class="title">Blackjack</div>
          <div class="balance">Balance: $<strong id="bjCash">${cash.toFixed(2)}</strong></div>

          <div class="bet-box">
            <label for="bjHandCount">Hands</label>
            <select id="bjHandCount"></select>
          </div>

          <div class="bet-box bj-bet-list" id="bjBetList"></div>

          <button id="placeBet" class="btn btn-deal" type="button">Deal Hand</button>

          <div class="actions">
            <button id="hitBtn" class="btn btn-hit" type="button">Hit</button>
            <button id="standBtn" class="btn btn-stand" type="button">Stand</button>
            <button id="doubleBtn" class="btn btn-double" type="button">Double</button>
            <button id="splitBtn" class="btn btn-split" type="button">Split</button>
          </div>
        </aside>

        <main class="main">
          <div class="table-panel">
            <div class="deck-area" aria-hidden="true">
              <div class="deck-stack" id="bjDeckStack"></div>
              <div class="deck-count" id="bjDeckCount">0 left</div>
            </div>

            <div class="hand-block">
              <div class="hand-header">
                <div class="hand-title">Dealer</div>
                <div class="hand-score" id="dealerScore">Score: ?</div>
              </div>
              <div id="dealerCards" class="cards-row"></div>
            </div>

            <div class="hand-block">
              <div class="hand-header">
                <div class="hand-title">Player</div>
                <div class="hand-score" id="playerScore">Score: 0</div>
              </div>
              <div id="playerCards" class="player-hands"></div>
            </div>
          </div>

          <div class="message" id="bjMessage"></div>
        </main>
      </div>
    </div>
  `;

  document.getElementById("placeBet").onclick = startGame;
  document.getElementById("hitBtn").onclick = playerHit;
  document.getElementById("standBtn").onclick = () => {
    playerStand();
  };
  document.getElementById("doubleBtn").onclick = () => {
    playerDoubleDown();
  };
  document.getElementById("splitBtn").onclick = playerSplit;
  initializeBlackjackBetInputs();
  const blackjackRoot = document.querySelector(".blackjack-root");
  addSageBrand(blackjackRoot, "bottom-left");
  setBlackjackMessage("Place a bet to deal.");
  updateBlackjackCash();
  updateBlackjackDeckDisplay();
  updateBlackjackControls();
  renderBlackjack();

  activeCasinoCleanup = () => {
    if (blackjackFxTimeout) {
      clearTimeout(blackjackFxTimeout);
      blackjackFxTimeout = null;
    }
    if (blackjackContainer && !IS_PHONE_EMBED_MODE) {
      blackjackContainer.classList.remove("casino-fullbleed", "blackjack-fullbleed");
    }
  };
}

function updateBlackjackCash() {
  const bjCash = document.getElementById("bjCash");
  if (bjCash) bjCash.textContent = cash.toFixed(2);
}

function getBlackjackConfiguredHandCount() {
  const countSelect = document.getElementById("bjHandCount");
  if (!countSelect) return BLACKJACK_MIN_HANDS;
  const value = Number.parseInt(countSelect.value, 10);
  return Math.max(BLACKJACK_MIN_HANDS, Math.min(BLACKJACK_MAX_HANDS, Number.isFinite(value) ? value : BLACKJACK_MIN_HANDS));
}

function initializeBlackjackBetInputs() {
  const countSelect = document.getElementById("bjHandCount");
  if (!countSelect) return;

  countSelect.innerHTML = "";
  for (let handCount = BLACKJACK_MIN_HANDS; handCount <= BLACKJACK_MAX_HANDS; handCount += 1) {
    const option = document.createElement("option");
    option.value = String(handCount);
    option.textContent = `${handCount} Hand${handCount === 1 ? "" : "s"}`;
    countSelect.appendChild(option);
  }

  countSelect.value = String(Math.max(BLACKJACK_MIN_HANDS, Math.min(BLACKJACK_MAX_HANDS, blackjackSelectedHandCount)));
  renderBlackjackBetInputs();

  countSelect.addEventListener("change", () => {
    blackjackSelectedHandCount = getBlackjackConfiguredHandCount();
    renderBlackjackBetInputs();
    gameOver = true;
    blackjackDealInProgress = false;
    blackjackOutOfCardsThisHand = false;
    hands = Array.from({ length: blackjackSelectedHandCount }, () => []);
    handBets = Array(blackjackSelectedHandCount).fill(0);
    dealerHand = [];
    activeHandIndex = 0;
    resetBlackjackRenderSnapshot();
    renderBlackjack();
    updateBlackjackControls();
    setBlackjackMessage(`Hand count set to ${blackjackSelectedHandCount}. Enter bets and deal.`, "neutral");
  });
}

function renderBlackjackBetInputs() {
  const betList = document.getElementById("bjBetList");
  if (!betList) return;
  const targetHands = blackjackSelectedHandCount || getBlackjackConfiguredHandCount();
  const existingValues = Array.from(betList.querySelectorAll("input[data-hand-bet]")).map((input) => Number(input.value) || 10);

  betList.innerHTML = "";
  const title = document.createElement("label");
  title.textContent = targetHands === 1 ? "Bet Amount" : "Bet Amounts";
  betList.appendChild(title);

  for (let handIndex = 0; handIndex < targetHands; handIndex += 1) {
    const row = document.createElement("div");
    row.className = "bj-bet-row";

    const rowLabel = document.createElement("span");
    rowLabel.className = "bj-bet-row-label";
    rowLabel.textContent = targetHands === 1 ? "Hand" : `Hand ${handIndex + 1}`;

    const input = document.createElement("input");
    input.type = "number";
    input.min = "1";
    input.step = "1";
    input.id = handIndex === 0 ? "betInput" : `betInput${handIndex + 1}`;
    input.dataset.handBet = String(handIndex);
    input.value = String(Math.max(1, Math.floor(existingValues[handIndex] || existingValues[0] || 10)));

    row.appendChild(rowLabel);
    row.appendChild(input);
    betList.appendChild(row);
  }
}

function collectBlackjackBets() {
  const betInputs = Array.from(document.querySelectorAll("#bjBetList input[data-hand-bet]"));
  if (!betInputs.length) return null;

  const wagers = betInputs.map((input) => roundCurrency(Number(input.value)));
  if (wagers.some((value) => !Number.isFinite(value) || value <= 0)) return null;
  return wagers;
}

function updateBlackjackControls() {
  const placeBetBtn = document.getElementById("placeBet");
  const hitBtn = document.getElementById("hitBtn");
  const standBtn = document.getElementById("standBtn");
  const doubleBtn = document.getElementById("doubleBtn");
  const splitBtn = document.getElementById("splitBtn");
  const handCountSelect = document.getElementById("bjHandCount");
  const betInputs = Array.from(document.querySelectorAll("#bjBetList input[data-hand-bet]"));
  const tablePanel = document.querySelector(".blackjack-root .table-panel");
  const noCardsLeft = blackjackDeck.length <= 0;
  const lockSetupInputs = blackjackDealInProgress || !gameOver;

  if (placeBetBtn) placeBetBtn.disabled = blackjackDealInProgress || !gameOver;
  if (hitBtn) hitBtn.disabled = gameOver || blackjackDealInProgress || noCardsLeft;
  if (standBtn) standBtn.disabled = gameOver || blackjackDealInProgress;
  if (doubleBtn) doubleBtn.disabled = gameOver || blackjackDealInProgress || !canDoubleCurrentHand();
  if (splitBtn) splitBtn.disabled = gameOver || blackjackDealInProgress || !canSplitCurrentHand();
  if (handCountSelect) handCountSelect.disabled = lockSetupInputs;
  betInputs.forEach((input) => {
    input.disabled = lockSetupInputs;
  });
  if (doubleBtn) doubleBtn.classList.toggle("ready", !doubleBtn.disabled);
  if (splitBtn) splitBtn.classList.toggle("ready", !splitBtn.disabled);
  if (tablePanel) tablePanel.classList.toggle("dealing", blackjackDealInProgress);
}

function runBlackjackTableFx(type) {
  const tablePanel = document.querySelector(".blackjack-root .table-panel");
  if (!tablePanel) return;

  tablePanel.classList.remove("win-burst", "loss-burst", "push-burst");
  void tablePanel.offsetWidth;

  const fxClass =
    type === "win" ? "win-burst" : type === "loss" ? "loss-burst" : "push-burst";
  tablePanel.classList.add(fxClass);

  if (blackjackFxTimeout) clearTimeout(blackjackFxTimeout);
  blackjackFxTimeout = setTimeout(() => {
    tablePanel.classList.remove("win-burst", "loss-burst", "push-burst");
    blackjackFxTimeout = null;
  }, 820);
}

function setBlackjackMessage(text, tone = "neutral", animate = true) {
  const el = document.getElementById("bjMessage");
  if (!el) return;

  el.textContent = text;
  el.classList.remove("win", "loss", "push", "pulse");
  if (tone === "win") el.classList.add("win");
  if (tone === "loss") el.classList.add("loss");
  if (tone === "push") el.classList.add("push");
  if (animate) {
    requestAnimationFrame(() => el.classList.add("pulse"));
  }
}

function sleepBlackjack(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function abortBlackjackInitialDeal() {
  blackjackDealInProgress = false;
  gameOver = true;
  blackjackOutOfCardsThisHand = true;
  blackjackNeedsReshuffle = true;
  updateBlackjackControls();
  renderBlackjack({ revealDealerHole: true });
  setBlackjackMessage("Shoe ran out during deal. Reshuffling for next hand.", "neutral");
}

async function dealBlackjackCard(target, handIndex = 0) {
  const dealt = drawCard();
  if (!dealt) return false;
  if (target === "player") {
    const safeHandIndex = Math.max(0, Math.min(hands.length - 1, handIndex));
    hands[safeHandIndex].push(dealt);
    renderBlackjack({
      animateOwner: "player",
      animateHandIndex: safeHandIndex,
      animateIndex: hands[safeHandIndex].length - 1
    });
  } else {
    dealerHand.push(dealt);
    renderBlackjack({ animateOwner: "dealer", animateIndex: dealerHand.length - 1 });
  }
  await sleepBlackjack(340);
  return true;
}

function drawCard() {
  if (!blackjackDeck.length) {
    blackjackOutOfCardsThisHand = true;
    blackjackNeedsReshuffle = true;
    updateBlackjackDeckDisplay();
    return null;
  }
  const card = blackjackDeck.pop();
  updateBlackjackDeckDisplay();
  flashBlackjackDeckDraw();
  playGameSound("card_deal");
  return card;
}

function isBlackjack(hand) {
  return hand.length === 2 && handValue(hand) === 21;
}

function blackjackCardRank(card) {
  if (!card) return "";
  const rawRank = card.v ?? card.rank ?? card.value ?? card.r;
  if (rawRank == null) return "";
  if (rawRank === 14) return "A";
  const rank = String(rawRank).trim().toUpperCase();
  if (rank === "14" || rank === "1") return "A";
  if (rank === "T") return "10";
  return rank;
}

function splitComparableValue(card) {
  const rank = blackjackCardRank(card);
  if (!rank) return -1;
  if (rank === "A") return 11;
  if (["10", "J", "Q", "K"].includes(rank)) return 10;
  const numeric = Number(rank);
  return Number.isFinite(numeric) ? numeric : -1;
}

function currentHand() {
  return hands[activeHandIndex] || [];
}

function canDoubleCurrentHand() {
  const hand = currentHand();
  const currentBet = handBets[activeHandIndex] || 0;
  return hand.length === 2 && currentBet > 0 && cash >= currentBet && blackjackDeck.length >= 1;
}

function canSplitCurrentHand() {
  const hand = currentHand();
  const currentBet = handBets[activeHandIndex] || 0;
  if (hand.length !== 2 || currentBet <= 0 || cash < currentBet || blackjackDeck.length < 2) return false;
  return splitComparableValue(hand[0]) === splitComparableValue(hand[1]);
}

function handValue(hand) {
  let total = 0,
    aces = 0;
  hand.forEach((c) => {
    const rank = blackjackCardRank(c);
    if (["10", "J", "Q", "K"].includes(rank)) total += 10;
    else if (rank === "A") {
      total += 11;
      aces++;
    } else total += Number(rank);
  });
  while (total > 21 && aces--) total -= 10;
  return total;
}

async function startGame() {
  if (blackjackDealInProgress || !gameOver) return;
  if (!ensureCasinoBettingAllowedNow()) return;
  blackjackSelectedHandCount = getBlackjackConfiguredHandCount();
  const wagers = collectBlackjackBets();
  if (!wagers) {
    setBlackjackMessage("Enter valid bets for all hands.");
    return;
  }
  const totalWager = roundCurrency(wagers.reduce((sum, wager) => sum + wager, 0));
  if (totalWager <= 0) {
    setBlackjackMessage("Enter a valid bet.");
    return;
  }
  if (totalWager > cash) {
    setBlackjackMessage("Not enough cash for all selected hands.");
    return;
  }
  bet = totalWager;

  prepareBlackjackShoe({ force: shouldReshuffleBlackjack() });

  gameOver = false;
  blackjackDealInProgress = true;
  cash -= totalWager;
  updateUI();
  updateBlackjackCash();

  hands = Array.from({ length: blackjackSelectedHandCount }, () => []);
  handBets = wagers.slice(0, blackjackSelectedHandCount);
  dealerHand = [];
  activeHandIndex = 0;
  resetBlackjackRenderSnapshot();
  updateBlackjackControls();
  setBlackjackMessage("Dealing cards...");

  renderBlackjack();
  for (let handIndex = 0; handIndex < blackjackSelectedHandCount; handIndex += 1) {
    if (!(await dealBlackjackCard("player", handIndex))) return abortBlackjackInitialDeal();
  }
  if (!(await dealBlackjackCard("dealer"))) return abortBlackjackInitialDeal();
  for (let handIndex = 0; handIndex < blackjackSelectedHandCount; handIndex += 1) {
    if (!(await dealBlackjackCard("player", handIndex))) return abortBlackjackInitialDeal();
  }
  if (!(await dealBlackjackCard("dealer"))) return abortBlackjackInitialDeal();

  if (isBlackjack(dealerHand)) {
    blackjackDealInProgress = false;
    endGame();
    const pushHands = hands.filter((hand) => isBlackjack(hand)).length;
    if (pushHands <= 0) {
      setBlackjackMessage("Dealer has Blackjack. Automatic loss.", "loss");
    } else {
      const lossHands = Math.max(0, hands.length - pushHands);
      if (lossHands > 0) {
        setBlackjackMessage(`Dealer has Blackjack. ${pushHands} push, ${lossHands} loss.`, "neutral");
      } else {
        setBlackjackMessage("Dealer has Blackjack. Push.", "push");
      }
    }
    updateBlackjackControls();
    return;
  }

  const rigNaturalBlackjack = shouldRigHighBet(totalWager, 0.95);
  if (hands.length === 1 && isBlackjack(hands[0]) && !rigNaturalBlackjack) {
    const payout = bet * 2.5;
    const profit = payout - bet;
    cash += payout;
    showCasinoWinPopup({ amount: profit, multiplier: payout / bet });
    gameOver = true;
    blackjackDealInProgress = false;
    updateUI();
    updateBlackjackCash();
    renderBlackjack({ revealDealerHole: true });
    runBlackjackTableFx("win");
    setBlackjackMessage(`Blackjack! Auto-win paid $${payout.toFixed(2)} (3:2 bonus).`, "win");
    updateBlackjackControls();
    return;
  }

  if (hands.length === 1 && isBlackjack(hands[0]) && rigNaturalBlackjack) {
    setBlackjackMessage("Blackjack dealt. Playing out the hand...", "neutral");
  } else if (hands.length > 1) {
    setBlackjackMessage(`Playing Hand 1 of ${hands.length}.`, "neutral", false);
  } else {
    setBlackjackMessage("Hit, stand, split, or double.", "neutral", false);
  }

  blackjackDealInProgress = false;
  updateBlackjackControls();
}

function playerHit() {
  if (gameOver || blackjackDealInProgress) return;
  if (blackjackDeck.length <= 0) {
    blackjackOutOfCardsThisHand = true;
    blackjackNeedsReshuffle = true;
    setBlackjackMessage("Deck is empty. Stand to finish this hand. New shuffle next hand.", "neutral");
    updateBlackjackControls();
    return;
  }
  const hand = currentHand();
  const dealt = drawCard();
  if (!dealt) {
    setBlackjackMessage("Deck is empty. Stand to finish this hand. New shuffle next hand.", "neutral");
    updateBlackjackControls();
    return;
  }
  hand.push(dealt);
  renderBlackjack({
    animateOwner: "player",
    animateHandIndex: activeHandIndex,
    animateIndex: hand.length - 1
  });

  const score = handValue(hand);
  if (score > 21) {
    runBlackjackTableFx("loss");
    setBlackjackMessage(`Hand ${activeHandIndex + 1} busts.`, "loss");
    advanceToNextHandOrDealer();
    return;
  }
  if (score === 21) {
    setBlackjackMessage(`Hand ${activeHandIndex + 1} has 21.`, "neutral");
    advanceToNextHandOrDealer();
    return;
  }
  updateBlackjackControls();
}

async function playerStand() {
  if (gameOver || blackjackDealInProgress) return;
  setBlackjackMessage(`Hand ${activeHandIndex + 1} stands.`, "neutral");
  await advanceToNextHandOrDealer();
}

async function playerDoubleDown() {
  if (gameOver || blackjackDealInProgress || !canDoubleCurrentHand()) return;
  const hand = currentHand();
  const wager = handBets[activeHandIndex] || 0;
  cash -= wager;
  handBets[activeHandIndex] += wager;
  updateUI();
  updateBlackjackCash();

  const dealt = drawCard();
  if (!dealt) {
    blackjackOutOfCardsThisHand = true;
    blackjackNeedsReshuffle = true;
    setBlackjackMessage("Deck is empty. Double cancelled. Stand to finish hand.", "neutral");
    handBets[activeHandIndex] -= wager;
    cash += wager;
    updateUI();
    updateBlackjackCash();
    updateBlackjackControls();
    return;
  }
  hand.push(dealt);
  renderBlackjack({
    animateOwner: "player",
    animateHandIndex: activeHandIndex,
    animateIndex: hand.length - 1
  });

  setBlackjackMessage(`Hand ${activeHandIndex + 1} doubled down.`, "neutral");
  runBlackjackTableFx("push");
  await advanceToNextHandOrDealer();
}

function playerSplit() {
  if (gameOver || blackjackDealInProgress || !canSplitCurrentHand()) return;
  const hand = currentHand();
  const originalBet = handBets[activeHandIndex];
  const [firstCard, secondCard] = hand;

  cash -= originalBet;
  updateUI();
  updateBlackjackCash();

  const firstDraw = drawCard();
  const secondDraw = drawCard();
  if (!firstDraw || !secondDraw) {
    blackjackOutOfCardsThisHand = true;
    blackjackNeedsReshuffle = true;
    cash += originalBet;
    updateUI();
    updateBlackjackCash();
    setBlackjackMessage("Not enough cards to split. Finish hand and reshuffle next deal.", "neutral");
    updateBlackjackControls();
    return;
  }

  hands[activeHandIndex] = [firstCard, firstDraw];
  hands.splice(activeHandIndex + 1, 0, [secondCard, secondDraw]);
  handBets.splice(activeHandIndex + 1, 0, originalBet);

  renderBlackjack({
    activeSwitch: true,
    suppressAutoAnimate: true,
    animateCards: [
      { owner: "player", handIndex: activeHandIndex, index: 1 },
      { owner: "player", handIndex: activeHandIndex + 1, index: 1 }
    ]
  });
  runBlackjackTableFx("push");
  setBlackjackMessage("Split created. Playing Hand 1.", "neutral");
  updateBlackjackControls();
}

async function advanceToNextHandOrDealer() {
  if (gameOver || blackjackDealInProgress) return;

  if (activeHandIndex < hands.length - 1) {
    activeHandIndex += 1;
    renderBlackjack({ activeSwitch: true });
    setBlackjackMessage(`Now playing Hand ${activeHandIndex + 1}.`, "neutral", false);
    updateBlackjackControls();
    return;
  }

  blackjackDealInProgress = true;
  updateBlackjackControls();

  const allHandsBusted = hands.every((hand) => handValue(hand) > 21);
  if (!allHandsBusted) {
    while (handValue(dealerHand) < 17 && blackjackDeck.length > 0) {
      const dealt = drawCard();
      if (!dealt) break;
      dealerHand.push(dealt);
      renderBlackjack({
        animateOwner: "dealer",
        animateIndex: dealerHand.length - 1
      });
      await sleepBlackjack(360);
    }
    if (handValue(dealerHand) < 17 && blackjackDeck.length <= 0) {
      blackjackOutOfCardsThisHand = true;
      blackjackNeedsReshuffle = true;
    }
  }

  blackjackDealInProgress = false;
  endGame();
}

function endGame() {
  gameOver = true;
  updateBlackjackControls();

  const dealerScore = handValue(dealerHand);
  const totalWager = handBets.reduce((sum, value) => sum + value, 0);
  let totalPayout = 0;
  let winCount = 0;
  let pushCount = 0;
  let lossCount = 0;
  const summaries = [];

  hands.forEach((hand, index) => {
    const playerScore = handValue(hand);
    const wager = handBets[index] || bet;

    if (playerScore > 21) {
      lossCount += 1;
      summaries.push(`H${index + 1} bust`);
      return;
    }

    if (dealerScore > 21 || playerScore > dealerScore) {
      const payout = wager * 2;
      totalPayout += payout;
      winCount += 1;
      summaries.push(`H${index + 1} win $${payout.toFixed(2)}`);
      return;
    }

    if (playerScore === dealerScore) {
      totalPayout += wager;
      pushCount += 1;
      summaries.push(`H${index + 1} push`);
      return;
    }

    lossCount += 1;
    summaries.push(`H${index + 1} lose`);
  });

  cash += totalPayout;
  const net = totalPayout - totalWager;
  if (net > 0) {
    showCasinoWinPopup({
      amount: net,
      multiplier: totalWager > 0 ? totalPayout / totalWager : null
    });
  }
  if (net < 0) playGameSound("loss");
  let message = "";
  let tone = "neutral";
  if (net > 0) tone = "win";
  else if (net < 0) tone = "loss";
  else tone = "push";

  if (hands.length === 1) {
    const playerScore = handValue(hands[0]);
    if (playerScore > 21) message = "Bust! You lose.";
    else if (dealerScore > 21 || playerScore > dealerScore) message = `You win $${totalPayout.toFixed(2)}!`;
    else if (playerScore === dealerScore) message = "Push. Bet returned.";
    else message = "You lose.";
  } else {
    message = `Round over: ${summaries.join(" | ")}.`;
  }

  updateUI();
  updateBlackjackCash();
  renderBlackjack({ revealDealerHole: true });
  runBlackjackTableFx(tone);
  if (blackjackOutOfCardsThisHand) {
    setBlackjackMessage(`${message} Shoe depleted. Reshuffling for next hand.`, tone);
  } else {
    setBlackjackMessage(message, tone);
  }
  triggerCasinoKickoutCheckAfterRound();
}

function createBlackjackCard(card, hidden = false, options = {}) {
  const el = document.createElement("div");
  if (options.deal) {
    el.style.setProperty("--bj-tilt", `${(Math.random() * 10 - 5).toFixed(2)}deg`);
  }

  if (hidden) {
    el.className = "bj-card back";
    el.innerHTML = `
      <span class="rank">A</span>
      <span class="suit">♠</span>
      <span class="rank bottom">A</span>
    `;
    if (options.reveal) el.classList.add("reveal-in");
    if (options.pop && !options.deal) el.classList.add("hit-pop");
    if (options.settle && !options.deal && !options.pop && !options.reveal) el.classList.add("settle-in");
    return el;
  }

  const isRed = card.s === "♥" || card.s === "♦";
  el.className = `bj-card ${isRed ? "red" : "black"}`;
  el.innerHTML = `
    <span class="rank">${card.v}</span>
    <span class="suit">${card.s}</span>
    <span class="rank bottom">${card.v}</span>
  `;
  if (options.reveal) el.classList.add("reveal-in");
  if (options.pop && !options.deal) el.classList.add("hit-pop");
  if (options.settle && !options.deal && !options.pop && !options.reveal) el.classList.add("settle-in");
  return el;
}

function animateBlackjackDeals(cardElements = []) {
  if (!cardElements.length) return;

  const deckStack = document.getElementById("bjDeckStack");
  if (!deckStack) {
    cardElements.forEach((cardEl) => cardEl.classList.add("deal-in"));
    return;
  }

  const deckRect = deckStack.getBoundingClientRect();
  const deckCenterX = deckRect.left + deckRect.width / 2;
  const deckCenterY = deckRect.top + deckRect.height / 2;

  cardElements.forEach((cardEl, index) => {
    const cardRect = cardEl.getBoundingClientRect();
    const cardCenterX = cardRect.left + cardRect.width / 2;
    const cardCenterY = cardRect.top + cardRect.height / 2;
    cardEl.style.setProperty("--deal-from-x", `${(deckCenterX - cardCenterX).toFixed(2)}px`);
    cardEl.style.setProperty("--deal-from-y", `${(deckCenterY - cardCenterY).toFixed(2)}px`);
    cardEl.style.setProperty("--deal-delay", `${(index * 52).toFixed(0)}ms`);
    cardEl.classList.add("deal-in");
  });
}

function renderBlackjack(options = {}) {
  const {
    animateOwner = null,
    animateHandIndex = 0,
    animateIndex = -1,
    popOwner = null,
    popHandIndex = 0,
    popIndex = -1,
    animateCards = [],
    popCards = [],
    suppressAutoAnimate = false,
    revealDealerHole = false,
    activeSwitch = false
  } = options;
  const pc = document.getElementById("playerCards");
  const dc = document.getElementById("dealerCards");
  if (!pc || !dc) return;
  const previousDealer = blackjackRenderSnapshot.dealer || [];
  const previousHands = blackjackRenderSnapshot.hands || [];
  const queuedDeals = [];
  pc.innerHTML = "";
  dc.innerHTML = "";

  hands.forEach((hand, handIndex) => {
    const handWrap = document.createElement("div");
    handWrap.className = `player-hand ${!gameOver && handIndex === activeHandIndex ? "active" : ""} ${
      !gameOver && handIndex === activeHandIndex && activeSwitch ? "active-switch" : ""
    }`;

    const handMeta = document.createElement("div");
    handMeta.className = "player-hand-meta";
    handMeta.textContent = `Hand ${handIndex + 1} • Bet $${(handBets[handIndex] || 0).toFixed(2)} • Score ${handValue(hand)}`;

    const handCards = document.createElement("div");
    handCards.className = "cards-row";
    hand.forEach((card, cardIndex) => {
      const previousCardKey = previousHands[handIndex]?.[cardIndex] || "";
      const isAutoNew = !suppressAutoAnimate && previousCardKey !== blackjackCardKey(card);
      const isExplicitAnimate = animateCards.some(
        (entry) =>
          entry.owner === "player" &&
          entry.handIndex === handIndex &&
          entry.index === cardIndex
      );
      const shouldAnimate =
        (animateOwner === "player" &&
          animateHandIndex === handIndex &&
          animateIndex === cardIndex) ||
        isExplicitAnimate ||
        isAutoNew;
      const isExplicitPop = popCards.some(
        (entry) =>
          entry.owner === "player" &&
          entry.handIndex === handIndex &&
          entry.index === cardIndex
      );
      const shouldPop =
        (popOwner === "player" && popHandIndex === handIndex && popIndex === cardIndex) ||
        isExplicitPop;
      const shouldSettle = !shouldAnimate && !shouldPop;
      const cardEl = createBlackjackCard(card, false, {
        deal: shouldAnimate,
        pop: shouldPop,
        settle: shouldSettle
      });
      handCards.appendChild(cardEl);
      if (shouldAnimate) queuedDeals.push(cardEl);
    });

    handWrap.appendChild(handMeta);
    handWrap.appendChild(handCards);
    pc.appendChild(handWrap);
  });

  dealerHand.forEach((c, i) => {
    const hidden = i === 1 && !gameOver;
    const currentDealerKey = hidden ? "HIDDEN" : blackjackCardKey(c);
    const previousDealerKey = previousDealer[i] || "";
    const isAutoNew = !suppressAutoAnimate && previousDealerKey !== currentDealerKey;
    const shouldReveal = revealDealerHole && i === 1 && !hidden;
    const isExplicitAnimate = animateCards.some(
      (entry) => entry.owner === "dealer" && entry.index === i
    );
    const shouldAnimate = (animateOwner === "dealer" && animateIndex === i) || (isAutoNew && !shouldReveal);
    const isExplicitPop = popCards.some(
      (entry) => entry.owner === "dealer" && entry.index === i
    );
    const shouldPop = popOwner === "dealer" && popIndex === i;
    const finalShouldAnimate = shouldAnimate || isExplicitAnimate;
    const finalShouldPop = shouldPop || isExplicitPop;
    const shouldSettle = !finalShouldAnimate && !shouldReveal && !finalShouldPop;
    const cardEl = createBlackjackCard(c, hidden, {
      deal: finalShouldAnimate,
      reveal: shouldReveal,
      pop: finalShouldPop,
      settle: shouldSettle
    });
    dc.appendChild(cardEl);
    if (finalShouldAnimate) queuedDeals.push(cardEl);
  });

  const playerScoreEl = document.getElementById("playerScore");
  const dealerScoreEl = document.getElementById("dealerScore");

  if (playerScoreEl) {
    if (hands.length === 1) {
      playerScoreEl.textContent = "Score: " + handValue(hands[0]);
    } else {
      playerScoreEl.textContent = `Active: Hand ${activeHandIndex + 1} (${handValue(currentHand())})`;
    }
  }
  if (dealerScoreEl) {
    dealerScoreEl.textContent = gameOver ? "Score: " + handValue(dealerHand) : "Score: ?";
  }

  blackjackRenderSnapshot = {
    dealer: dealerHand.map((card, i) => (i === 1 && !gameOver ? "HIDDEN" : blackjackCardKey(card))),
    hands: hands.map((hand) => hand.map((card) => blackjackCardKey(card)))
  };

  requestAnimationFrame(() => animateBlackjackDeals(queuedDeals));
}

// =====================================================
// =================== INIT ============================
// =====================================================
loadPhoneState();
bankMissionLastCashSnapshot = cash;
refreshPokerSessionMeta();
initAntiTamperGuards();
initTradingUsernameBadge();
bindPersistenceFlushHandlers();
initFirstPlayTutorial();
initFirstLaunchUsernameSetup();
initHiddenAdminTrigger();
initCasinoSecretUnlockButton();
window.addEventListener("online", handleCasinoLiveDataConnectionChange);
window.addEventListener("offline", handleCasinoLiveDataConnectionChange);
startCasinoLiveFeedTicker();
if (!IS_PHONE_EMBED_MODE) {
  startMarketInterval();
  initPhone();
}
updateUI();
updateLoanUI();
if (IS_PHONE_EMBED_MODE) {
  setTimeout(() => {
    initPhoneEmbeddedGameView();
  }, 0);
} else {
  initPhoneEmbeddedGameView();
}

// =====================================================
// =================== HI-LO MODULE ====================
// =====================================================
const hiloState = {
  betAmount: 10,
  currentProfit: 0,
  currentCardValue: 2,
  gameActive: false,
  guessLocked: false,
  skipRemaining: 3,
  suits: ["♦", "♣", "♥", "♠"]
};
const HILO_SKIP_MAX = 3;
const HILO_SKIP_PENALTY = 0.04;

function loadHiLo() {
  const c = document.getElementById("casino-container");
  hiloState.currentCardValue = 2;
  hiloState.currentProfit = 0;
  hiloState.gameActive = false;
  hiloState.guessLocked = false;
  hiloState.skipRemaining = HILO_SKIP_MAX;

  c.innerHTML = `
    <div class="hilo-root">
      <div class="app">
        <aside class="sidebar">
          <div class="balance">Balance: <strong id="hiloBalance">$0.00</strong></div>

          <div class="potential-profits">
            <div>Higher: <span id="hiloPotentialProfitHigher" class="potential-profit"></span></div>
            <div>Lower: <span id="hiloPotentialProfitLower" class="potential-profit"></span></div>
            <div>Same: <span id="hiloPotentialProfitSame" class="potential-profit"></span></div>
          </div>

          <div class="bet-input">
            <span class="currency">$</span>
            <input type="number" step="0.01" value="${hiloState.betAmount.toFixed(
              2
            )}" id="hiloBetAmount"/>
            <div class="bet-actions">
              <button id="hiloHalf" type="button">½</button>
              <button id="hiloDouble" type="button">2×</button>
              <button id="hiloMax" type="button">Max</button>
            </div>
          </div>

          <div class="probabilities">
            <div class="prob-row up"><span>Higher</span><strong id="hiloPercentHigher">0%</strong></div>
            <div class="prob-row down"><span>Lower</span><strong id="hiloPercentLower">0%</strong></div>
            <div class="prob-row same"><span>Same</span><strong id="hiloPercentSame">0%</strong></div>
          </div>

          <button class="btn-bet" id="hiloBetButton" type="button">Bet</button>
          <button class="btn-cashout" id="hiloCashoutButton" type="button">Cashout</button>

          <div class="guess-buttons">
            <button data-guess="higher" class="guess" id="hiloGuessHigher" type="button">Higher</button>
            <button data-guess="same" class="guess" id="hiloGuessSame" type="button">Same</button>
            <button data-guess="lower" class="guess" id="hiloGuessLower" type="button">Lower</button>
            <button class="guess skip-button" id="hiloSkipButton" type="button">Skip (3)</button>
          </div>
        </aside>

        <main class="main">
          <div class="cards-zone">
            <div class="ghost ghost-left">
              <div class="ghost-card">K</div>
              <span>KING BEING THE HIGHEST</span>
            </div>

            <div class="active-card">
              <div class="card" id="hiloCurrentCard"></div>
              <div class="current-card-label">(The current card)</div>
            </div>

            <div class="ghost ghost-right">
              <div class="ghost-card">A</div>
              <span>ACE BEING THE LOWEST</span>
            </div>
          </div>

          <div class="profit-bar">
            <div id="hiloProfitHigher">Profit Higher ($0.00)</div>
            <div id="hiloProfitLower">Profit Lower ($0.00)</div>
            <div id="hiloTotalProfit">Total Profit ($0.00)</div>
          </div>
          <div class="sage-hilo-note" aria-hidden="true">SAGE</div>

          <div class="history" id="hiloHistory"></div>
        </main>
      </div>
    </div>
  `;

  const balanceEl = document.getElementById("hiloBalance");
  const betInputEl = document.getElementById("hiloBetAmount");
  const totalProfitEl = document.getElementById("hiloTotalProfit");
  const percentHigherEl = document.getElementById("hiloPercentHigher");
  const percentLowerEl = document.getElementById("hiloPercentLower");
  const percentSameEl = document.getElementById("hiloPercentSame");
  const profitHigherEl = document.getElementById("hiloProfitHigher");
  const profitLowerEl = document.getElementById("hiloProfitLower");
  const potentialProfitHigherEl = document.getElementById("hiloPotentialProfitHigher");
  const potentialProfitLowerEl = document.getElementById("hiloPotentialProfitLower");
  const potentialProfitSameEl = document.getElementById("hiloPotentialProfitSame");
  const currentCardEl = document.getElementById("hiloCurrentCard");
  const historyEl = document.getElementById("hiloHistory");

  const betButton = document.getElementById("hiloBetButton");
  const cashoutButton = document.getElementById("hiloCashoutButton");

  const halfBtn = document.getElementById("hiloHalf");
  const doubleBtn = document.getElementById("hiloDouble");
  const maxBtn = document.getElementById("hiloMax");

  const guessBtnHigher = document.getElementById("hiloGuessHigher");
  const guessBtnSame = document.getElementById("hiloGuessSame");
  const guessBtnLower = document.getElementById("hiloGuessLower");
  const skipButton = document.getElementById("hiloSkipButton");
  const guessButtons = [guessBtnHigher, guessBtnSame, guessBtnLower];

  const multipliers = { higher: 0, lower: 0, same: 0 };

  function setRoundState(isActive) {
    hiloState.gameActive = isActive;

    betButton.disabled = isActive;
    betButton.textContent = isActive ? "Round Active" : "Bet";

    betInputEl.disabled = isActive;
    halfBtn.disabled = isActive;
    doubleBtn.disabled = isActive;
    maxBtn.disabled = isActive;

    cashoutButton.disabled = !isActive;
    guessButtons.forEach((btn) => (btn.disabled = !isActive));
    if (skipButton) skipButton.disabled = !isActive || hiloState.skipRemaining <= 0;

    if (!isActive) hiloState.guessLocked = false;
  }

  function hiloUpdateUI() {
    const higherCount = Math.max(0, 13 - hiloState.currentCardValue);
    const lowerCount = Math.max(0, hiloState.currentCardValue - 1);
    const sameCount = 1;
    const total = higherCount + lowerCount + sameCount;
    const toPercent = (count) => ((count / total) * 100).toFixed(2);

    multipliers.higher = higherCount > 0 ? total / higherCount : 0;
    multipliers.lower = lowerCount > 0 ? total / lowerCount : 0;
    multipliers.same = total / sameCount;

    balanceEl.textContent = `$${cash.toFixed(2)}`;
    totalProfitEl.textContent = `Total Profit ($${hiloState.currentProfit.toFixed(2)})`;

    profitHigherEl.textContent = `Profit Higher: ${multipliers.higher.toFixed(2)}x ($${(
      hiloState.currentProfit * multipliers.higher
    ).toFixed(2)})`;
    profitLowerEl.textContent = `Profit Lower: ${multipliers.lower.toFixed(2)}x ($${(
      hiloState.currentProfit * multipliers.lower
    ).toFixed(2)})`;

    potentialProfitHigherEl.textContent = `${multipliers.higher.toFixed(2)}x ($${(
      hiloState.currentProfit * multipliers.higher
    ).toFixed(2)})`;
    potentialProfitLowerEl.textContent = `${multipliers.lower.toFixed(2)}x ($${(
      hiloState.currentProfit * multipliers.lower
    ).toFixed(2)})`;
    potentialProfitSameEl.textContent = `${multipliers.same.toFixed(2)}x ($${(
      hiloState.currentProfit * multipliers.same
    ).toFixed(2)})`;

    const higherPct = toPercent(higherCount);
    const lowerPct = toPercent(lowerCount);
    const samePct = toPercent(sameCount);

    percentHigherEl.textContent = `${higherPct}%`;
    percentLowerEl.textContent = `${lowerPct}%`;
    percentSameEl.textContent = `${samePct}%`;

    guessBtnHigher.textContent = `Higher (${higherPct}%)`;
    guessBtnSame.textContent = `Same (${samePct}%)`;
    guessBtnLower.textContent = `Lower (${lowerPct}%)`;
    if (skipButton) skipButton.textContent = `Skip (${hiloState.skipRemaining})`;
  }

  function displayCard(cardValue) {
    const rankMap = { 1: "A", 11: "J", 12: "Q", 13: "K" };
    const rank = rankMap[cardValue] || cardValue;
    const suit = hiloState.suits[Math.floor(Math.random() * hiloState.suits.length)];
    const color = suit === "♦" || suit === "♥" ? "#e53935" : "#000";

    currentCardEl.innerHTML = `
      <span class="rank" style="color:${color}">${rank}</span>
      <span class="suit" style="color:${color}">${suit}</span>
      <span class="rank bottom" style="color:${color}">${rank}</span>
    `;
    playGameSound("card_deal");
  }

  function animateCurrentCard() {
    currentCardEl.classList.remove("card-animate");
    void currentCardEl.offsetWidth;
    currentCardEl.classList.add("card-animate");
  }

  function getRandomCard() {
    return Math.floor(Math.random() * 13) + 1;
  }

  function getRiggedHiLoCard(previousValue, guess) {
    if (guess === "higher") return Math.floor(Math.random() * previousValue) + 1;
    if (guess === "lower") return Math.floor(Math.random() * (13 - previousValue + 1)) + previousValue;
    const nonSame = Math.random() < 0.5 ? previousValue - 1 : previousValue + 1;
    return Math.max(1, Math.min(13, nonSame));
  }

  function placeBet() {
    if (hiloState.gameActive) return;
    if (!ensureCasinoBettingAllowedNow()) return;

    const betAmount = parseFloat(betInputEl.value);
    if (!Number.isFinite(betAmount) || betAmount <= 0) {
      alert("Bet must be greater than $0");
      return;
    }
    if (betAmount > cash) {
      alert("Not enough cash!");
      return;
    }

    hiloState.betAmount = betAmount;
    cash -= betAmount;
    hiloState.currentProfit = betAmount;
    hiloState.guessLocked = false;
    hiloState.skipRemaining = HILO_SKIP_MAX;

    setRoundState(true);
    updateUI();
    hiloUpdateUI();
    displayCard(hiloState.currentCardValue);
    animateCurrentCard();
  }

  function makeGuess(guess) {
    if (!hiloState.gameActive || hiloState.guessLocked) {
      if (!hiloState.gameActive) alert("Place a bet first!");
      return;
    }

    hiloState.guessLocked = true;

    const prev = hiloState.currentCardValue;
    const nextCard = shouldRigHighBet(hiloState.betAmount, 0.95)
      ? getRiggedHiLoCard(prev, guess)
      : getRandomCard();
    let won = false;

    if (guess === "higher" && nextCard > prev) won = true;
    if (guess === "lower" && nextCard < prev) won = true;
    if (guess === "same" && nextCard === prev) won = true;
    if (won) {
      hiloState.currentProfit *= multipliers[guess];
      hiloState.skipRemaining = HILO_SKIP_MAX;
    } else {
      hiloState.currentProfit = 0;
      setRoundState(false);
      playGameSound("loss");
    }

    hiloState.currentCardValue = nextCard;
    displayCard(hiloState.currentCardValue);
    animateCurrentCard();

    const rankMap = { 1: "A", 11: "J", 12: "Q", 13: "K" };
    const rank = rankMap[nextCard] || nextCard;
    const suit = hiloState.suits[Math.floor(Math.random() * hiloState.suits.length)];
    const color = suit === "♦" || suit === "♥" ? "#e53935" : "#000";

    const historyItem = document.createElement("div");
    historyItem.className = "history-item";
    historyItem.innerHTML = `
      <div class="history-card" style="color:${color}">
        <span class="rank">${rank}</span>
        <span class="suit">${suit}</span>
        <span class="rank bottom">${rank}</span>
      </div>
      <div class="pill ${won ? "green" : "red"}">${guess}</div>
    `;
    historyEl.appendChild(historyItem);
    while (historyEl.children.length > 30) {
      historyEl.removeChild(historyEl.firstElementChild);
    }
    historyEl.scrollLeft = historyEl.scrollWidth;

    hiloUpdateUI();
    hiloState.guessLocked = false;
    if (!won) {
      triggerCasinoKickoutCheckAfterRound();
    }
  }

  function skipCard() {
    if (!hiloState.gameActive || hiloState.guessLocked) {
      if (!hiloState.gameActive) alert("Place a bet first!");
      return;
    }
    if (hiloState.skipRemaining <= 0) return;

    hiloState.guessLocked = true;
    hiloState.skipRemaining -= 1;
    hiloState.currentProfit = roundCurrency(Math.max(0, hiloState.currentProfit * (1 - HILO_SKIP_PENALTY)));

    const nextCard = getRandomCard();
    hiloState.currentCardValue = nextCard;
    displayCard(hiloState.currentCardValue);
    animateCurrentCard();

    const rankMap = { 1: "A", 11: "J", 12: "Q", 13: "K" };
    const rank = rankMap[nextCard] || nextCard;
    const suit = hiloState.suits[Math.floor(Math.random() * hiloState.suits.length)];
    const color = suit === "♦" || suit === "♥" ? "#e53935" : "#000";

    const historyItem = document.createElement("div");
    historyItem.className = "history-item";
    historyItem.innerHTML = `
      <div class="history-card" style="color:${color}">
        <span class="rank">${rank}</span>
        <span class="suit">${suit}</span>
        <span class="rank bottom">${rank}</span>
      </div>
      <div class="pill red">skip -${Math.round(HILO_SKIP_PENALTY * 100)}%</div>
    `;
    historyEl.appendChild(historyItem);
    while (historyEl.children.length > 30) {
      historyEl.removeChild(historyEl.firstElementChild);
    }
    historyEl.scrollLeft = historyEl.scrollWidth;

    hiloUpdateUI();
    hiloState.guessLocked = false;
  }

  function cashout() {
    if (!hiloState.gameActive) return;
    const payout = hiloState.currentProfit;
    const netProfit = payout - hiloState.betAmount;
    cash += hiloState.currentProfit;
    if (netProfit > 0) {
      showCasinoWinPopup({
        amount: netProfit,
        multiplier: hiloState.betAmount > 0 ? payout / hiloState.betAmount : null
      });
    } else if (netProfit < 0) {
      playGameSound("loss");
    }
    hiloState.currentProfit = 0;
    setRoundState(false);
    updateUI();
    hiloUpdateUI();
    triggerCasinoKickoutCheckAfterRound();
  }

  halfBtn.addEventListener("click", () => {
    betInputEl.value = ((parseFloat(betInputEl.value) || 0) / 2).toFixed(2);
  });
  doubleBtn.addEventListener("click", () => {
    betInputEl.value = ((parseFloat(betInputEl.value) || 0) * 2).toFixed(2);
  });
  maxBtn.addEventListener("click", () => {
    betInputEl.value = cash.toFixed(2);
  });

  betButton.addEventListener("click", placeBet);
  cashoutButton.addEventListener("click", cashout);

  guessBtnHigher.addEventListener("click", () => makeGuess("higher"));
  guessBtnSame.addEventListener("click", () => makeGuess("same"));
  guessBtnLower.addEventListener("click", () => makeGuess("lower"));
  if (skipButton) skipButton.addEventListener("click", skipCard);

  hiloUpdateUI();
  displayCard(hiloState.currentCardValue);
  setRoundState(hiloState.gameActive);
}

// =====================================================
// =================== POKER MODULE ====================
// =====================================================

// --- CONFIGURATION ---
const MAX_SEATS = 9;
const P_SUITS = ["♠", "♥", "♦", "♣"];
const P_VALUES = ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"];

// --- NAMES & ARCHETYPES ---
const BOT_NAMES = [
  "Spike",
  "Butch",
  "Slick",
  "Lefty",
  "Tiny",
  "Moose",
  "Hawk",
  "Gator",
  "Reno",
  "Dakota",
  "Brooklyn",
  "Jersey",
  "Cash",
  "Chance",
  "Joker",
  "Bishop",
  "King",
  "Sarge",
  "Cap",
  "Tank",
  "Dozer",
  "Hammer",
  "Smiley",
  "Red",
  "Blue",
  "Shadow",
  "Phantom",
  "Wolf",
  "Fox",
  "Cobra"
];

const ARCHETYPES = {
  Shark: { vpip: 0.25, pfr: 0.7, bluff: 0.15, aggr: 0.7 },
  Fish: { vpip: 0.55, pfr: 0.05, bluff: 0.05, aggr: 0.1 },
  Station: { vpip: 0.7, pfr: 0.0, bluff: 0.0, aggr: 0.0 },
  Maniac: { vpip: 0.8, pfr: 0.8, bluff: 0.6, aggr: 0.9 },
  Bully: { vpip: 0.45, pfr: 0.9, bluff: 0.7, aggr: 0.8 },
  Rock: { vpip: 0.1, pfr: 0.05, bluff: 0.0, aggr: 0.2 }
};

// --- POKER STATE ---
let p_deck = [],
  p_community = [],
  p_pot = 0,
  p_highestBet = 0;
let p_turnIndex = 0,
  p_dealerIndex = 0,
  p_phase = 0;
let p_seats = new Array(MAX_SEATS).fill(null);
let p_actionsTaken = 0;
let p_playerRaiseUsedThisRound = false;
let p_aiTimeoutId = null;
let p_handInProgress = false;
let pk = {}; // Element Cache

class PokerCard {
  constructor(s, v) {
    this.s = s;
    this.v = v;
    this.r = P_VALUES.indexOf(v) + 2; // Rank 2-14
  }
  get color() {
    return this.s === "♥" || this.s === "♦" ? "red" : "black";
  }
}

function loadPokerAsNeonPlinko() {
  const container = document.getElementById("casino-container");
  if (!container) return;
  container.classList.add("casino-fullbleed", "plinko-fullbleed", "poker-plinko-fullbleed");

  container.innerHTML = `
    <div id="poker-plinko-screen" class="plinko-root poker-plinko-root">
      <div class="app-layout">
        <div class="game-container">
          <div class="controls">
            <div class="brand">SAGE</div>

            <div class="input-group">
              <label>Bet Amount</label>
              <input type="number" id="betAmount" value="10" min="1" max="1000">
            </div>

            <div class="input-group">
              <label>Rows</label>
              <select id="rowCount">
                <option value="8">8 Rows</option>
                <option value="10">10 Rows</option>
                <option value="12">12 Rows</option>
                <option value="14">14 Rows</option>
                <option value="16" selected>16 Rows</option>
              </select>
            </div>

            <div class="input-group">
              <label>Difficulty</label>
              <select id="difficulty">
                <option value="easy">Easy</option>
                <option value="medium" selected>Medium</option>
                <option value="hard">Hard</option>
              </select>
            </div>

            <div class="input-group">
              <label>Auto Speed</label>
              <input type="range" id="autoSpeed" min="200" max="1000" step="100" value="400">
            </div>

            <button id="autoBtn" class="action-btn auto-btn">AUTO</button>
            <button id="dropBtn" class="action-btn">DROP BALL</button>
          </div>

          <div class="stats">
            <div class="stat-box">
              <span>Balance</span>
              <span id="balanceDisplay" class="highlight">${cash.toFixed(2)}</span>
            </div>
            <div class="stat-box">
              <span>Last Result</span>
              <span id="lastWinDisplay">0.00</span>
            </div>
          </div>

          <div class="canvas-wrapper">
            <div id="plinkoHitPopup" class="plinko-hit-popup" aria-live="polite"></div>
            <canvas id="plinkoCanvas"></canvas>
            <div id="oddsTooltip" class="odds-tooltip">Chance: 0.00%</div>
            <div id="multipliers" class="multiplier-container"></div>
          </div>
        </div>

        <div class="history-column">
          <div class="history-header">LAST DROPS</div>
          <div id="historyList" class="history-list"></div>
        </div>
      </div>
    </div>
  `;

  plinko.rows = 16;
  plinko.difficulty = "medium";
  plinko.layoutMode = "poker";
  plinko.root = container.querySelector("#poker-plinko-screen");
  plinko.boardMaxWidth = 980;
  plinko.maxSpacingY = 60;
  initPlinko();

  activeCasinoCleanup = () => {
    stopAuto();
    if (plinko.hitPopupTimer) {
      clearTimeout(plinko.hitPopupTimer);
      plinko.hitPopupTimer = null;
    }
    if (plinko.resizeHandler) {
      window.removeEventListener("resize", plinko.resizeHandler);
      plinko.resizeHandler = null;
    }
    if (plinko.animationFrame) {
      cancelAnimationFrame(plinko.animationFrame);
      plinko.animationFrame = null;
    }
    plinko.initialized = false;
    plinko.balls = [];
    plinko.pegs = [];
    plinko.lastFrameMs = 0;
    plinko.layoutMode = "default";
    plinko.root = null;
    plinko.boardMaxWidth = 520;
    plinko.maxSpacingY = 30;
    container.classList.remove("casino-fullbleed", "plinko-fullbleed", "poker-plinko-fullbleed");
  };
}

function loadCasinoPoker() {
  const container = document.getElementById("casino-container");
  if (!container) return;
  try {
  pokerLastSessionProfit = 0;
  refreshPokerSessionMeta();

  container.classList.add("casino-fullbleed", "poker-fullbleed", "poker-classic-fullbleed");
  container.innerHTML = `
    <div class="casino-poker-classic">
      <div class="ambient"></div>
      <div id="poker-table">
        <div class="table-hud">
          <div class="hud-item">Hand <span id="hand-no">0</span></div>
          <div class="hud-item">Blinds <span id="blind-level">5 / 10</span></div>
          <div class="hud-item">Players <span id="players-left">0</span></div>
          <div class="pressure-wrap">
            <span>Pressure</span>
            <div class="pressure-bar"><div id="pressure-fill"></div></div>
          </div>
        </div>

        <div class="center-area">
          <div id="pot-wrap">Pot: $<span id="pot">0</span></div>
          <div id="pot-chips" aria-hidden="true"></div>
          <div id="comm-cards"></div>
          <div id="status">Waiting for game...</div>
        </div>

        <div id="deal-overlay">
          <div id="msg-overlay"></div>
          <button id="btn-deal" class="btn-lrg">Deal Hand</button>
          <div id="tie-choice-row" class="hidden">
            <button id="btn-split-pot" class="btn-lrg">Split Pot</button>
            <button id="btn-showdown-pot" class="btn-lrg">Showdown</button>
          </div>
        </div>

        <div id="controls" class="hidden">
          <button id="btn-fold" class="btn-action red">Fold</button>
          <button id="btn-check" class="btn-action">Check</button>
          <button id="btn-call" class="btn-action">Call <span id="amt-call"></span></button>
          <div class="raise-box">
            <label for="raise-input">Raise By</label>
            <input type="number" id="raise-input" value="20" step="5" min="5">
            <button id="btn-raise" class="btn-action gold">Raise</button>
          </div>
        </div>

        <div class="action-log-wrap">
          <div class="action-log-title">Table Talk</div>
          <div id="action-log"></div>
        </div>
      </div>
    </div>
  `;

  const tableRoot = container.querySelector(".casino-poker-classic");
  addSageBrand(tableRoot, "top-left");
  const MAX_SEATS = 9;
  const SUITS = ["♠", "♥", "♦", "♣"];
  const VALUES = ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"];
  const STARTING_STACK = Math.max(1000, roundCurrency(cash || 0));
  const BOT_MIN_STACK = 500;
  const BOT_STACK_SPREAD = 1500;
  const BLIND_UP_EVERY = 5;
  const BOT_NAMES = [
    "Spike", "Butch", "Slick", "Lefty", "Tiny", "Moose", "Hawk", "Gator", "Reno", "Dakota", "Brooklyn",
    "Jersey", "Cash", "Chance", "Joker", "Bishop", "King", "Sarge", "Cap", "Tank", "Dozer", "Hammer",
    "Smiley", "Red", "Blue", "Shadow", "Phantom", "Wolf", "Fox", "Cobra", "Blaze", "Sparky", "Boomer",
    "Buster", "Ziggy", "Bubba", "Muggsy", "Bugsy", "Vinnie", "Sal", "Ringo", "Dutch", "Cassidy", "Yankee",
    "Rebel", "Gunner", "Pistol", "Trigger", "Nitro", "Brick"
  ];
  const ARCHETYPES = {
    Shark: { vpip: 0.25, pfr: 0.7, bluff: 0.15, aggr: 0.7 },
    Fish: { vpip: 0.55, pfr: 0.05, bluff: 0.05, aggr: 0.1 },
    Station: { vpip: 0.7, pfr: 0.0, bluff: 0.0, aggr: 0.0 },
    Maniac: { vpip: 0.8, pfr: 0.8, bluff: 0.6, aggr: 0.9 },
    Bully: { vpip: 0.45, pfr: 0.9, bluff: 0.7, aggr: 0.8 },
    Rock: { vpip: 0.1, pfr: 0.05, bluff: 0.0, aggr: 0.2 },
    Nit: { vpip: 0.05, pfr: 0.02, bluff: 0.0, aggr: 0.1 },
    Loose: { vpip: 0.5, pfr: 0.2, bluff: 0.1, aggr: 0.3 },
    Bluffer: { vpip: 0.35, pfr: 0.5, bluff: 0.8, aggr: 0.6 }
  };

  class Card {
    constructor(suit, value) {
      this.s = suit;
      this.v = value;
      this.r = VALUES.indexOf(value) + 2;
    }
    get color() {
      return this.s === "♥" || this.s === "♦" ? "red" : "black";
    }
  }

  const el = {
    pot: tableRoot.querySelector("#pot"),
    status: tableRoot.querySelector("#status"),
    comm: tableRoot.querySelector("#comm-cards"),
    dealOverlay: tableRoot.querySelector("#deal-overlay"),
    msgOverlay: tableRoot.querySelector("#msg-overlay"),
    btnDeal: tableRoot.querySelector("#btn-deal"),
    btnRestart: tableRoot.querySelector("#btn-restart"),
    tieChoiceRow: tableRoot.querySelector("#tie-choice-row"),
    btnSplitPot: tableRoot.querySelector("#btn-split-pot"),
    btnShowdownPot: tableRoot.querySelector("#btn-showdown-pot"),
    controls: tableRoot.querySelector("#controls"),
    btnFold: tableRoot.querySelector("#btn-fold"),
    btnCheck: tableRoot.querySelector("#btn-check"),
    btnCall: tableRoot.querySelector("#btn-call"),
    btnRaise: tableRoot.querySelector("#btn-raise"),
    inputRaise: tableRoot.querySelector("#raise-input"),
    amtCall: tableRoot.querySelector("#amt-call"),
    table: tableRoot.querySelector("#poker-table"),
    handNo: tableRoot.querySelector("#hand-no"),
    blindLevel: tableRoot.querySelector("#blind-level"),
    playersLeft: tableRoot.querySelector("#players-left"),
    pressureFill: tableRoot.querySelector("#pressure-fill"),
    potWrap: tableRoot.querySelector("#pot-wrap"),
    potChips: tableRoot.querySelector("#pot-chips"),
    actionLog: tableRoot.querySelector("#action-log")
  };

  let deck = [];
  let community = [];
  let pot = 0;
  let highestBet = 0;
  let turnIndex = 0;
  let dealerIndex = 0;
  let phase = 0;
  let actionsTaken = 0;
  let handNo = 0;
  let heroStackAtHandStart = roundCurrency(cash || 0);
  let sessionProfit = 0;
  let blinds = { small: 5, big: 10, level: 1 };
  let seats = new Array(MAX_SEATS).fill(null);
  let aiTimeoutId = null;
  let pendingTieDecision = null;
  let handInProgress = false;
  const seatUi = [];
  const listeners = [];
  const timers = new Set();
  let disposed = false;

  const bind = (node, type, handler, options) => {
    if (!node) return;
    node.addEventListener(type, handler, options);
    listeners.push(() => node.removeEventListener(type, handler, options));
  };
  const schedule = (fn, ms) => {
    const id = window.setTimeout(() => {
      timers.delete(id);
      if (!disposed) fn();
    }, ms);
    timers.add(id);
    return id;
  };

  const setDealButtonLocked = (locked) => {
    if (!el.btnDeal) return;
    el.btnDeal.disabled = Boolean(locked);
  };

  function syncCash() {
    if (!seats[0]) return;
    cash = roundCurrency(seats[0].bank);
    updateUI();
    updateBlackjackCash();
  }

  function initTableLayout() {
    tableRoot.querySelectorAll(".player-seat").forEach((node) => node.remove());
    seatUi.length = 0;
    for (let i = 0; i < MAX_SEATS; i += 1) {
      const div = document.createElement("div");
      div.className = "player-seat";
      if (i === 0) div.classList.add("hero-seat");
      div.innerHTML = `
        <div class="speech-bubble" id="bubble-${i}"></div>
        <div class="p-avatar" id="avatar-${i}">S</div>
        <div class="p-cards" id="cards-${i}"></div>
        <div class="p-hand-readout" id="hand-readout-${i}"></div>
        <div class="p-profit-readout" id="profit-readout-${i}">Profit: +$0.00</div>
        <div class="p-info">
          <div class="p-name">Empty</div>
          <div class="p-bank" id="bank-${i}"></div>
        </div>
      `;
      el.table.appendChild(div);
      seatUi[i] = {
        div,
        cards: div.querySelector(`#cards-${i}`),
        avatar: div.querySelector(`#avatar-${i}`),
        handReadout: div.querySelector(`#hand-readout-${i}`),
        profitReadout: div.querySelector(`#profit-readout-${i}`),
        name: div.querySelector(".p-name"),
        bank: div.querySelector(`#bank-${i}`),
        bubble: div.querySelector(`#bubble-${i}`)
      };
    }
    layoutSeats();
  }

  function layoutSeats() {
    const isMobile = window.innerWidth <= 900;
    for (let i = 0; i < MAX_SEATS; i += 1) {
      const uiSeat = seatUi[i];
      if (!uiSeat) continue;
      const seatWidth = isMobile ? 108 : 148;
      const seatHeight = isMobile ? 148 : 188;
      const halfW = el.table.clientWidth * 0.5;
      const halfH = el.table.clientHeight * 0.5;
      const maxX = Math.max(0, halfW - seatWidth * 0.5 - 2);
      const maxY = Math.max(0, halfH - seatHeight * 0.5 - 2);
      const rx = Math.max(
        isMobile ? 160 : 330,
        halfW - seatWidth * 0.5 - (isMobile ? 2 : 4)
      );
      const ry = Math.max(
        isMobile ? 136 : 228,
        halfH - seatHeight * 0.5 - (isMobile ? 4 : 8)
      );
      const angle = (i / MAX_SEATS) * 2 * Math.PI + Math.PI / 2;
      const sideStrength = Math.abs(Math.cos(angle));
      const sideYSpread = 1 + sideStrength * 0.42;
      const sideXCompress = 1 - sideStrength * 0.12;
      const pushX = Math.cos(angle) * rx * 1.08 * sideXCompress;
      const pushY = Math.sin(angle) * ry * 1.12 * sideYSpread;
      const x = Math.max(-maxX, Math.min(maxX, pushX));
      const heroDownShift = i === 0 ? (isMobile ? 34 : 78) : 0;
      const heroExtraBottomRoom = i === 0 ? (isMobile ? 40 : 110) : 0;
      const yMin = -maxY;
      const yMax = maxY + heroExtraBottomRoom;
      const y = Math.max(yMin, Math.min(yMax, pushY + heroDownShift));
      uiSeat.div.style.width = `${seatWidth}px`;
      uiSeat.div.style.height = `${seatHeight}px`;
      uiSeat.div.style.left = `calc(50% + ${x}px - ${seatWidth / 2}px)`;
      uiSeat.div.style.top = `calc(50% + ${y}px - ${seatHeight / 2}px)`;
    }
  }

  function getBotTargetStack() {
    const heroBank = roundCurrency((seats[0]?.bank ?? cash ?? STARTING_STACK) || STARTING_STACK);
    const floor = Math.max(1200, Math.floor(heroBank * 0.6));
    const normalCap = Math.max(floor + 200, Math.floor(heroBank * 1.35));
    const whaleCap = Math.max(normalCap + 500, Math.floor(heroBank * 3.8));
    const isWhale = Math.random() < 0.24;
    const between = (min, max) => min + Math.random() * Math.max(1, max - min);
    return isWhale
      ? Math.floor(between(normalCap, whaleCap))
      : Math.floor(between(floor, normalCap));
  }

  function generateBot(id) {
    const name = BOT_NAMES[Math.floor(Math.random() * BOT_NAMES.length)];
    const styles = Object.keys(ARCHETYPES);
    const styleName = styles[Math.floor(Math.random() * styles.length)];
    return {
      id,
      name,
      isAi: true,
      bank: getBotTargetStack(),
      hand: [],
      bet: 0,
      folded: false,
      inHand: false,
      style: styleName,
      stats: ARCHETYPES[styleName]
    };
  }

  function fillEmptySeats(count) {
    let filled = 0;
    for (let i = 1; i < MAX_SEATS; i += 1) {
      if (seats[i] === null && filled < count && Math.random() < 0.8) {
        seats[i] = generateBot(i);
        filled += 1;
      }
    }
  }

  function escalateBlindsIfNeeded() {
    if (handNo <= 1) return;
    if ((handNo - 1) % BLIND_UP_EVERY !== 0) return;
    blinds.level += 1;
    blinds.small = Math.max(5, Math.ceil((blinds.small * 1.5) / 5) * 5);
    blinds.big = blinds.small * 2;
    addLog(`Blind level up: ${blinds.small}/${blinds.big}`);
    el.msgOverlay.innerText = `Blind Up ${blinds.small}/${blinds.big}`;
    schedule(() => {
      if (el.msgOverlay.innerText.startsWith("Blind Up")) el.msgOverlay.innerText = "";
    }, 1300);
  }

  function handleTableRotation() {
    const heroBankRef = roundCurrency((seats[0]?.bank ?? STARTING_STACK) || STARTING_STACK);
    const tableRichThreshold = Math.max(STARTING_STACK * 2.5, heroBankRef * 2.2);
    const leaving = [];
    for (let i = 1; i < MAX_SEATS; i += 1) {
      const p = seats[i];
      if (!p) continue;
      let reason = null;
      if (p.bank <= 0) reason = "Bust";
      else if (p.bank >= tableRichThreshold && Math.random() < 0.18) reason = "Win";
      else if (p.bank < blinds.big * 4 && Math.random() < 0.2) reason = "Low";
      else if (Math.random() < 0.05) reason = "Rand";
      if (reason) leaving.push({ idx: i, msg: reason === "Bust" ? "I'm dusted." : "Cash me out." });
    }

    leaving.forEach((item) => {
      say(item.idx, item.msg);
      addLog(`${seats[item.idx].name} leaves the table.`);
      seatUi[item.idx].div.style.opacity = "0";
      schedule(() => {
        seats[item.idx] = null;
        seatUi[item.idx].div.style.opacity = "1";
        renderUi();
      }, 1500);
    });

    schedule(() => {
      const activeCount = seats.filter((p) => p !== null).length;
      if (activeCount < 4) fillEmptySeats(3);
      else if (activeCount < 8 && Math.random() > 0.5) fillEmptySeats(1);
      renderUi();
    }, 2000);
  }

  function getNextPlayableSeat(fromIdx) {
    let idx = fromIdx;
    for (let i = 0; i < MAX_SEATS; i += 1) {
      idx = (idx + 1) % MAX_SEATS;
      const p = seats[idx];
      if (p && p.inHand && !p.folded) return idx;
    }
    return -1;
  }

  function postBlind(idx, amount, tag) {
    const p = seats[idx];
    if (!p || !p.inHand || p.folded) return 0;
    const paid = Math.min(amount, p.bank);
    p.bank -= paid;
    p.bet += paid;
    pot += paid;
    highestBet = Math.max(highestBet, p.bet);
    say(idx, `${tag} $${paid}`);
    addLog(`${p.name} posts ${tag.toLowerCase()} $${paid}`);
    return paid;
  }

  function hideTieDecisionPrompt() {
    pendingTieDecision = null;
    if (el.tieChoiceRow) el.tieChoiceRow.classList.add("hidden");
  }

  function showTieDecisionPrompt(winnerIds, winDesc, earlyWin) {
    pendingTieDecision = {
      winnerIds: winnerIds.slice(),
      winDesc,
      earlyWin: Boolean(earlyWin)
    };
    if (el.tieChoiceRow) el.tieChoiceRow.classList.remove("hidden");
    el.controls.classList.add("hidden");
    el.dealOverlay.classList.remove("hidden");
    el.btnDeal.classList.add("hidden");
    if (el.btnRestart) el.btnRestart.classList.add("hidden");
    el.msgOverlay.innerText = "Tie hand: Split pot or showdown?";
    addLog("Tie hand: waiting for your decision.");
  }

  function createShuffledTieDeck() {
    const tieDeck = [];
    SUITS.forEach((s) => VALUES.forEach((v) => tieDeck.push(new Card(s, v))));
    for (let index = tieDeck.length - 1; index > 0; index -= 1) {
      const swapIndex = Math.floor(Math.random() * (index + 1));
      [tieDeck[index], tieDeck[swapIndex]] = [tieDeck[swapIndex], tieDeck[index]];
    }
    return tieDeck;
  }

  function resolveTieByShowdown(contenders) {
    if (!Array.isArray(contenders) || contenders.length <= 1) {
      return {
        winners: Array.isArray(contenders) ? contenders.slice() : [],
        winDesc: "with High Card (showdown)"
      };
    }

    let tied = contenders.slice();
    let round = 1;

    while (tied.length > 1 && round <= 10) {
      const tieDeck = createShuffledTieDeck();
      const tieCommunity = [tieDeck.pop(), tieDeck.pop(), tieDeck.pop(), tieDeck.pop(), tieDeck.pop()];

      community = tieCommunity.slice();
      el.comm.innerHTML = "";
      community.forEach((card) => renderCardDiv(card, el.comm));

      tied.forEach((player) => {
        player.hand = [tieDeck.pop(), tieDeck.pop()];
        renderCards(player.id, player.hand, false);
      });

      const withEval = tied.map((player) => ({
        player,
        eval: evaluateHand(player.hand, community)
      }));
      withEval.sort((a, b) => compareHands(b.eval, a.eval));
      const topEval = withEval[0]?.eval;
      const roundWinners = withEval
        .filter((entry) => compareHands(entry.eval, topEval) === 0)
        .map((entry) => entry.player);

      const boardText = community.map((card) => `${card.v}${card.s}`).join(" ");
      addLog(`Showdown hand ${round}: board ${boardText}`);
      addLog(
        `Showdown hand ${round}: ${tied.map((player) => {
          const cards = (player.hand || []).map((card) => `${card.v}${card.s}`).join(" ");
          return `${player.name} [${cards}]`;
        }).join(" | ")}`
      );

      tied = roundWinners;
      if (tied.length === 1) {
        const handName = withEval.find((entry) => entry.player.id === tied[0].id)?.eval?.name || "High Card";
        el.status.innerText = `${tied[0].name} wins showdown hand ${round} with ${handName}`;
        return {
          winners: tied,
          winDesc: `with ${handName} (showdown hand)`
        };
      }

      addLog(`Showdown hand ${round}: tie continues (${tied.map((p) => p.name).join(" & ")}).`);
      round += 1;
    }

    const forcedWinner = tied[Math.floor(Math.random() * tied.length)];
    addLog(`Showdown unresolved: random winner ${forcedWinner.name}.`);
    return {
      winners: [forcedWinner],
      winDesc: "by showdown draw"
    };
  }

  function applyWinnerGlow(winners) {
    seatUi.forEach((s) => s.div.classList.remove("winner-glow"));
    winners.forEach((w) => {
      if (w && seatUi[w.id]) seatUi[w.id].div.classList.add("winner-glow");
    });
  }

  function settleWinners(winners, winDesc) {
    applyWinnerGlow(winners);
    if (winners.length <= 0) return;
    const potBeforePay = pot;
    payWinners(winners);
    pot = 0;
    const names = winners.map((w) => w.name).join(" & ");
    el.status.innerText = winners.length > 1 ? `Split Pot: ${names} ${winDesc}` : `${names} wins ${winDesc}`;
    addLog(`${names} scoops $${potBeforePay} ${winDesc}`);
  }

  function finishHandResolution(earlyWin) {
    if (seats[0]) {
      const handDelta = roundCurrency(seats[0].bank - heroStackAtHandStart);
      sessionProfit = roundCurrency(sessionProfit + handDelta);
      const heroProfit = Math.max(0, handDelta);
      if (handDelta < 0) playGameSound("loss");
      if (heroProfit > 0.009) {
        const autoSaved = maybeAutoSaveFromCasinoWin({
          amount: heroProfit,
          multiplier: null,
          source: "poker"
        });
        if (autoSaved > 0) {
          seats[0].bank = roundCurrency(Math.max(0, seats[0].bank - autoSaved));
        }
      }
    }

    renderUi();
    el.dealOverlay.classList.remove("hidden");
    handInProgress = false;
    setDealButtonLocked(false);
    if (earlyWin) addLog("Hand ended early.");
    dealerIndex = (dealerIndex + 1) % MAX_SEATS;
    schedule(handleTableRotation, 3000);
  }

  function resolvePendingTieDecision(mode) {
    if (!pendingTieDecision) return;
    const tieData = pendingTieDecision;
    hideTieDecisionPrompt();
    const contenders = tieData.winnerIds.map((id) => seats[id]).filter(Boolean);
    if (contenders.length === 0) {
      finishHandResolution(tieData.earlyWin);
      return;
    }

    if (mode === "showdown") {
      const showdownResult = resolveTieByShowdown(contenders);
      settleWinners(showdownResult.winners, showdownResult.winDesc);
    } else {
      settleWinners(contenders, tieData.winDesc);
    }
    finishHandResolution(tieData.earlyWin);
  }

  function startHand() {
    if (handInProgress || pendingTieDecision) return;
    handInProgress = true;
    setDealButtonLocked(true);

    const gate = getCasinoGateState();
    if (!gate.ok) {
      if (gate.reason === "trading_total") {
        clearCasinoKickoutTimer();
        showCasinoKickoutOverlay("Did not hit trading quota. Please return to the trading floor.");
      } else {
        el.msgOverlay.innerText = gate.message || "Casino is locked.";
      }
      handInProgress = false;
      setDealButtonLocked(false);
      return;
    }

    const hero = seats[0];
    if (!hero) {
      handInProgress = false;
      setDealButtonLocked(false);
      return;
    }
    hideTieDecisionPrompt();
    hero.bank = roundCurrency(cash || 0);
    heroStackAtHandStart = hero.bank;

    if (hero.bank <= 0) {
      el.msgOverlay.innerText = "You are bust! Go back and earn more cash.";
      el.btnDeal.classList.add("hidden");
      el.dealOverlay.classList.remove("hidden");
      handInProgress = false;
      return;
    }

    handNo += 1;
    escalateBlindsIfNeeded();

    deck = [];
    SUITS.forEach((s) => VALUES.forEach((v) => deck.push(new Card(s, v))));
    deck.sort(() => Math.random() - 0.5);

    community = [];
    pot = 0;
    highestBet = 0;
    phase = 0;
    actionsTaken = 0;
    el.msgOverlay.innerText = "";
    el.inputRaise.value = String(blinds.big);

    seats.forEach((p, i) => {
      if (!p) {
        renderCards(i, []);
        return;
      }
      p.bet = 0;
      p.folded = false;
      p.hand = [];
      if (p.bank > 0) {
        p.inHand = true;
        const firstCard = deck.pop();
        const secondCard = deck.pop();
        p.hand = [firstCard, secondCard];
        playGameSound("card_deal");
        playGameSound("card_deal");
        seatUi[i].div.classList.remove("folded", "turn-active", "winner-glow");
        renderCards(i, p.hand, p.isAi && p.inHand && phase < 4);
      } else {
        p.inHand = false;
        renderCards(i, []);
      }
    });

    const sbIndex = getNextPlayableSeat(dealerIndex);
    const bbIndex = sbIndex === -1 ? -1 : getNextPlayableSeat(sbIndex);
    if (sbIndex === -1 || bbIndex === -1 || sbIndex === bbIndex) {
      el.status.innerText = "Not enough players in hand.";
      el.dealOverlay.classList.remove("hidden");
      renderUi();
      handInProgress = false;
      setDealButtonLocked(false);
      return;
    }

    postBlind(sbIndex, blinds.small, "SB");
    postBlind(bbIndex, blinds.big, "BB");

    el.dealOverlay.classList.add("hidden");
    el.comm.innerHTML = "";
    el.status.innerText = `Pre-Flop | Blinds ${blinds.small}/${blinds.big}`;
    addLog(`Hand ${handNo} started.`);

    turnIndex = bbIndex;
    findNextActivePlayer();
    renderUi();
    nextTurn();
  }

  function findNextActivePlayer() {
    let checked = 0;
    do {
      turnIndex = (turnIndex + 1) % MAX_SEATS;
      checked += 1;
      if (checked > MAX_SEATS + 2) return;
    } while (!seats[turnIndex] || !seats[turnIndex].inHand || seats[turnIndex].folded || seats[turnIndex].bank === 0);
  }

  function nextTurn() {
    const activeInHand = seats.filter((p) => p && p.inHand && !p.folded);
    if (activeInHand.length === 1) {
      forceWin(activeInHand[0]);
      return;
    }
    const p = seats[turnIndex];
    if (!p || p.folded || !p.inHand) {
      findNextActivePlayer();
      nextTurn();
      return;
    }

    renderUi();
    seatUi.forEach((s) => s.div.classList.remove("turn-active"));
    seatUi[turnIndex].div.classList.add("turn-active");

    if (p.isAi) {
      el.controls.classList.add("hidden");
      if (aiTimeoutId) clearTimeout(aiTimeoutId);
      aiTimeoutId = schedule(() => {
        aiTimeoutId = null;
        aiMove(p);
      }, 550 + Math.random() * 650);
      return;
    }

    el.controls.classList.remove("hidden");
    updateHeroActionControls(p);
  }

  function nextPhase() {
    actionsTaken = 0;
    highestBet = 0;
    seats.forEach((p) => {
      if (p) p.bet = 0;
    });
    phase += 1;

    if (phase === 1) {
      const flop = [deck.pop(), deck.pop(), deck.pop()];
      community.push(...flop);
      flop.forEach(() => playGameSound("card_deal"));
    } else if (phase === 2) {
      community.push(deck.pop());
      playGameSound("card_deal");
    } else if (phase === 3) {
      community.push(deck.pop());
      playGameSound("card_deal");
    }
    else {
      showdown();
      return;
    }

    el.comm.innerHTML = "";
    community.forEach((c) => renderCardDiv(c, el.comm));
    el.status.innerText = `${["Pre-Flop", "Flop", "Turn", "River"][phase]} | Pot Pressure Rising`;
    addLog(`Board: ${["Pre-Flop", "Flop", "Turn", "River"][phase]}`);

    turnIndex = dealerIndex;
    findNextActivePlayer();
    nextTurn();
  }

  function getMinRaise() {
    return Math.max(blinds.big, 10);
  }

  function getRaiseMeta(player, raiseByRaw, minRaiseOverride = null) {
    const minRaise = minRaiseOverride ?? getMinRaise();
    let raiseBy = Number.parseInt(raiseByRaw, 10);
    if (Number.isNaN(raiseBy) || raiseBy < minRaise) raiseBy = minRaise;

    const toCall = Math.max(0, highestBet - player.bet);
    const targetBet = highestBet + raiseBy;
    const added = Math.max(0, targetBet - player.bet);
    const shortBy = Math.max(0, added - player.bank);
    const maxRaiseBy = Math.max(0, Math.floor(player.bank + player.bet - highestBet));

    return {
      minRaise,
      raiseBy,
      toCall,
      targetBet,
      added,
      shortBy,
      maxRaiseBy,
      canRaise: added <= player.bank
    };
  }

  function updateHeroActionControls(player) {
    if (!player || player.isAi) return;
    const toCall = Math.max(0, highestBet - player.bet);
    el.btnCheck.classList.toggle("hidden", toCall > 0);
    el.btnCall.classList.toggle("hidden", toCall === 0);
    el.amtCall.innerText = toCall > 0 ? `$${toCall}` : "";

    const raiseMeta = getRaiseMeta(player, el.inputRaise.value);
    if (raiseMeta.canRaise) {
      el.btnRaise.textContent = `Raise To $${raiseMeta.targetBet}`;
      el.btnRaise.title = "";
      return;
    }

    el.btnRaise.textContent = `Raise To $${raiseMeta.targetBet} (Need $${Math.ceil(raiseMeta.shortBy)} more)`;
    el.btnRaise.title = `Not enough chips to raise. Max raise by: $${raiseMeta.maxRaiseBy}.`;
  }

  function registerBetPulse() {
    el.potWrap.classList.remove("live");
    void el.potWrap.offsetWidth;
    el.potWrap.classList.add("live");
    if (el.potChips) {
      el.potChips.classList.remove("live");
      void el.potChips.offsetWidth;
      el.potChips.classList.add("live");
    }
  }

  function act(action) {
    const p = seats[turnIndex];
    if (!p) return;

    if (action === "Raise") {
      let raiseBy = parseInt(el.inputRaise.value, 10);
      const minRaise = getMinRaise();
      if (p.isAi) {
        raiseBy = Math.max(minRaise, Math.floor(Math.max(blinds.big, pot * 0.35)));
        raiseBy = Math.ceil(raiseBy / 5) * 5;
      }
      const raiseMeta = getRaiseMeta(p, raiseBy, minRaise);
      if (!p.isAi && !raiseMeta.canRaise) {
        el.status.innerText = `Not enough chips to raise. Need $${Math.ceil(raiseMeta.shortBy)} more.`;
        say(p.id, "Can't raise");
        addLog(`${p.name} tried to raise without enough chips.`);
        updateHeroActionControls(p);
        return;
      }
      raiseBy = raiseMeta.raiseBy;
      const added = raiseMeta.added;
      if (p.bank >= added) {
        p.bank -= added;
        p.bet += added;
        pot += added;
        highestBet = p.bet;
        actionsTaken = 0;
        say(p.id, `Raise $${raiseBy}`);
        addLog(`${p.name} raises $${raiseBy}`);
        registerBetPulse();
      } else {
        action = "Call";
      }
    }

    actionsTaken += 1;

    if (action === "Fold") {
      p.folded = true;
      seatUi[turnIndex].div.classList.add("folded");
      say(p.id, "Fold");
      addLog(`${p.name} folds`);
    } else if (action === "Check") {
      say(p.id, "Check");
      addLog(`${p.name} checks`);
    } else if (action === "Call") {
      const amt = highestBet - p.bet;
      const pay = Math.min(amt, p.bank);
      p.bank -= pay;
      p.bet += pay;
      pot += pay;
      const tag = pay < amt ? "All-in" : "Call";
      say(p.id, `${tag} $${pay}`);
      addLog(`${p.name} ${tag.toLowerCase()} $${pay}`);
      if (pay > 0) registerBetPulse();
    }

    const activePlayers = seats.filter((pl) => pl && pl.inHand && !pl.folded);
    const allMatched = activePlayers.every((pl) => pl.bet === highestBet || pl.bank === 0);
    if (allMatched && actionsTaken >= activePlayers.length) nextPhase();
    else {
      findNextActivePlayer();
      nextTurn();
    }
  }

  function aiMove(p) {
    const toCall = highestBet - p.bet;
    const potOdds = toCall / (pot + toCall || 1);
    let strength = 0;
    if (phase === 0) {
      strength = rateHoleCards(p.hand);
    } else {
      const ev = evaluateHand(p.hand, community);
      strength = ev.tier * 12 + (ev.kickers[0] || 0) / 2;
      if (ev.tier >= 4) strength = 95;
    }

    const stats = p.stats;
    const roll = Math.random();
    let move = "Fold";
    if (toCall === 0) {
      if (strength > 60 || (roll < stats.bluff && strength < 30)) move = Math.random() < stats.aggr ? "Raise" : "Check";
      else move = "Check";
    } else {
      const required = potOdds * 100;
      const pressure = Math.min(12, Math.floor(handNo / BLIND_UP_EVERY) * 2);
      const perceived = strength + stats.vpip * 25 - pressure;
      if (perceived >= required) move = strength > 78 && Math.random() < stats.aggr ? "Raise" : "Call";
      else move = roll < stats.bluff * 0.1 ? "Raise" : "Fold";
    }
    if (move === "Raise" && p.bank <= toCall + getMinRaise()) move = p.bank > toCall ? "Call" : "Fold";
    act(move);
  }

  function rateHoleCards(hand) {
    if (!hand || hand.length < 2) return 0;
    const c1 = hand[0];
    const c2 = hand[1];
    let score = c1.r + c2.r;
    if (c1.r === c2.r) score += 25;
    if (c1.s === c2.s) score += 5;
    if (Math.abs(c1.r - c2.r) === 1) score += 5;
    if (c1.r >= 11 || c2.r >= 11) score += 4;
    return Math.min(100, score * 2);
  }

  function evaluateHand(hand, comm) {
    const all = hand.concat(comm);
    const getCounts = (cards) => {
      const counts = {};
      cards.forEach((c) => {
        counts[c.r] = (counts[c.r] || 0) + 1;
      });
      return counts;
    };
    const getStraight = (cards) => {
      const unique = [...new Set(cards.map((c) => c.r))].sort((a, b) => b - a);
      for (let i = 0; i < unique.length - 4; i += 1) {
        if (unique[i] - unique[i + 4] === 4) return unique[i];
      }
      if (unique.includes(14) && unique.includes(2) && unique.includes(3) && unique.includes(4) && unique.includes(5)) return 5;
      return 0;
    };

    const suits = {};
    all.forEach((c) => {
      suits[c.s] = (suits[c.s] || 0) + 1;
    });
    const flushSuit = Object.keys(suits).find((s) => suits[s] >= 5);
    let flushCards = [];
    if (flushSuit) {
      flushCards = all.filter((c) => c.s === flushSuit);
      const sfHigh = getStraight(flushCards);
      if (sfHigh > 0) return { tier: 8, kickers: [sfHigh], name: "Straight Flush" };
    }

    const counts = getCounts(all);
    const ranks = all.map((c) => c.r).sort((a, b) => b - a);
    const quads = Object.keys(counts).filter((r) => counts[r] === 4).map(Number).sort((a, b) => b - a);
    const trips = Object.keys(counts).filter((r) => counts[r] === 3).map(Number).sort((a, b) => b - a);
    const pairs = Object.keys(counts).filter((r) => counts[r] === 2).map(Number).sort((a, b) => b - a);
    const getKickers = (n, exclude) => ranks.filter((r) => !exclude.includes(r)).slice(0, n);

    if (quads.length > 0) return { tier: 7, kickers: [quads[0], ...getKickers(1, [quads[0]])], name: "Four of a Kind" };
    if (trips.length > 0 && (trips.length >= 2 || pairs.length > 0)) {
      const t = trips[0];
      const p = trips.length >= 2 ? trips[1] : pairs[0];
      return { tier: 6, kickers: [t, p], name: "Full House" };
    }
    if (flushSuit) return { tier: 5, kickers: flushCards.map((c) => c.r).sort((a, b) => b - a).slice(0, 5), name: "Flush" };
    const straightHigh = getStraight(all);
    if (straightHigh > 0) return { tier: 4, kickers: [straightHigh], name: "Straight" };
    if (trips.length > 0) return { tier: 3, kickers: [trips[0], ...getKickers(2, [trips[0]])], name: "Three of a Kind" };
    if (pairs.length >= 2) return { tier: 2, kickers: [pairs[0], pairs[1], ...getKickers(1, [pairs[0], pairs[1]])], name: "Two Pair" };
    if (pairs.length === 1) return { tier: 1, kickers: [pairs[0], ...getKickers(3, [pairs[0]])], name: "Pair" };
    return { tier: 0, kickers: getKickers(5, []), name: "High Card" };
  }

  function compareHands(a, b) {
    if (a.tier > b.tier) return 1;
    if (b.tier > a.tier) return -1;
    for (let i = 0; i < Math.max(a.kickers.length, b.kickers.length); i += 1) {
      const kA = a.kickers[i] || 0;
      const kB = b.kickers[i] || 0;
      if (kA > kB) return 1;
      if (kB > kA) return -1;
    }
    return 0;
  }

  const showdown = () => endRoundLogic(false);
  const forceWin = (winner) => endRoundLogic(true, winner);

  function payWinners(winners) {
    if (winners.length === 0 || pot === 0) return;
    const splitAmt = Math.floor(pot / winners.length);
    let remainder = pot % winners.length;
    winners.forEach((w) => {
      const bonus = remainder > 0 ? 1 : 0;
      if (remainder > 0) remainder -= 1;
      w.bank += splitAmt + bonus;
    });
  }

  function endRoundLogic(earlyWin, forcedWinner = null) {
    el.controls.classList.add("hidden");
    let winners = [];
    let winDesc = "";

    if (forcedWinner) {
      winners = [forcedWinner];
      winDesc = "(Everyone folded)";
    } else {
      seats.forEach((p, i) => {
        if (p && p.inHand && !p.folded && p.isAi) renderCards(i, p.hand, false);
      });
      const active = seats.filter((p) => p && p.inHand && !p.folded);
      active.forEach((p) => {
        p.eval = evaluateHand(p.hand, community);
      });
      active.sort((a, b) => compareHands(b.eval, a.eval));
      if (active.length > 0) {
        const best = active[0].eval;
        winners = active.filter((p) => compareHands(p.eval, best) === 0);
        winDesc = `with ${best.name}`;
      }
    }

    const heroInTie = winners.length > 1 && winners.some((w) => w.id === 0);
    if (!forcedWinner && heroInTie) {
      applyWinnerGlow(winners);
      renderUi();
      showTieDecisionPrompt(winners.map((w) => w.id), winDesc, earlyWin);
      return;
    }

    settleWinners(winners, winDesc);
    finishHandResolution(earlyWin);
  }

  function restartGame() {
    hideTieDecisionPrompt();
    el.msgOverlay.innerText = "Restart disabled. Go back and earn more cash.";
    el.btnDeal.classList.add("hidden");
    el.dealOverlay.classList.remove("hidden");
  }

  function renderCardDiv(card, target) {
    const node = document.createElement("div");
    node.className = `card ${card.color}`;
    node.innerHTML = `${card.v}<small>${card.s}</small>`;
    target.appendChild(node);
  }

  function renderCards(seatIdx, cards, hideAi = false) {
    const uiSeat = seatUi[seatIdx];
    if (!uiSeat) return;
    uiSeat.cards.innerHTML = "";
    cards.forEach((card) => {
      if (hideAi) {
        const back = document.createElement("div");
        back.className = "card card-back";
        uiSeat.cards.appendChild(back);
        return;
      }
      renderCardDiv(card, uiSeat.cards);
    });
  }

  function calcPressurePercent() {
    const active = seats.filter((p) => p && p.bank > 0);
    if (active.length === 0 || pot <= 0) return 0;
    const avgStack = active.reduce((sum, p) => sum + p.bank, 0) / active.length;
    if (!avgStack) return 0;
    return Math.min(100, Math.round((pot / avgStack) * 100));
  }

  function renderPotChips() {
    if (!el.potChips) return;
    el.potChips.innerHTML = "";
    el.potChips.style.display = "none";
  }

  function renderUi() {
    el.pot.innerText = Math.floor(pot);
    renderPotChips();
    el.handNo.innerText = String(handNo);
    el.blindLevel.innerText = `${blinds.small} / ${blinds.big}`;
    el.playersLeft.innerText = String(seats.filter((p) => p && p.bank > 0).length);
    el.pressureFill.style.width = `${calcPressurePercent()}%`;

    for (let i = 0; i < MAX_SEATS; i += 1) {
      const p = seats[i];
      const uiSeat = seatUi[i];
      if (!uiSeat) continue;
      if (!p) {
        uiSeat.div.classList.add("empty-seat");
        uiSeat.avatar.innerText = "✕";
        uiSeat.name.innerText = "Empty";
        uiSeat.name.style.opacity = "0.55";
        uiSeat.bank.innerText = "";
        uiSeat.handReadout.innerText = "";
        uiSeat.handReadout.classList.remove("show");
        continue;
      }

      uiSeat.div.classList.remove("empty-seat");
      uiSeat.avatar.innerText = p.name?.[0]?.toUpperCase() || "S";
      uiSeat.name.innerText = p.folded ? "Fold" : p.name;
      uiSeat.name.style.opacity = "1";
      uiSeat.bank.innerText = `$${Math.floor(p.bank)}`;
      if (!p.inHand && phase > 0) uiSeat.name.innerText += " (Wait)";

      if (i === 0) {
        if (uiSeat.profitReadout) {
          uiSeat.profitReadout.innerText = `Profit: ${formatSignedProfit(sessionProfit)}`;
          uiSeat.profitReadout.classList.toggle("is-up", sessionProfit >= 0);
          uiSeat.profitReadout.classList.toggle("is-down", sessionProfit < 0);
        }
        if (p.folded) {
          uiSeat.handReadout.innerText = "Folded";
          uiSeat.handReadout.classList.add("show");
        } else if (p.inHand && p.hand && p.hand.length > 0) {
          uiSeat.handReadout.innerText = evaluateHand(p.hand, community).name;
          uiSeat.handReadout.classList.add("show");
        } else {
          uiSeat.handReadout.innerText = "";
          uiSeat.handReadout.classList.remove("show");
        }
      }
    }

    syncCash();
  }

  function addLog(text) {
    const row = document.createElement("div");
    row.textContent = text;
    el.actionLog.prepend(row);
    while (el.actionLog.children.length > 10) el.actionLog.removeChild(el.actionLog.lastChild);
  }

  function say(seatIdx, text) {
    const seat = seatUi[seatIdx];
    if (!seat) return;
    seat.bubble.innerText = text;
    seat.bubble.classList.add("show");
    schedule(() => seat.bubble.classList.remove("show"), 1800);
  }

  function initGame() {
    handInProgress = false;
    setDealButtonLocked(false);
    seats = new Array(MAX_SEATS).fill(null);
    seats[0] = {
      id: 0,
      name: "You",
      isAi: false,
      bank: roundCurrency(cash || STARTING_STACK),
      hand: [],
      bet: 0,
      folded: false,
      inHand: false,
      style: "Human"
    };
    fillEmptySeats(5);
    addLog("Table opened. Big money only.");
    renderUi();
  }

  bind(el.btnDeal, "click", startHand);
  bind(el.btnRestart, "click", restartGame);
  bind(el.btnFold, "click", () => act("Fold"));
  bind(el.btnCheck, "click", () => act("Check"));
  bind(el.btnCall, "click", () => act("Call"));
  bind(el.btnRaise, "click", () => act("Raise"));
  bind(el.inputRaise, "input", () => {
    const p = seats[turnIndex];
    if (!p || p.isAi) return;
    updateHeroActionControls(p);
  });
  bind(el.btnSplitPot, "click", () => resolvePendingTieDecision("split"));
  bind(el.btnShowdownPot, "click", () => resolvePendingTieDecision("showdown"));

  const onResize = () => {
    layoutSeats();
    renderUi();
    seats.forEach((p, i) => {
      if (!p || !p.hand || p.hand.length === 0) return;
      renderCards(i, p.hand, p.isAi && p.inHand && phase < 4);
    });
    el.comm.innerHTML = "";
    community.forEach((c) => renderCardDiv(c, el.comm));
  };
  bind(window, "resize", onResize);

  initTableLayout();
  initGame();

  activeCasinoCleanup = () => {
    pokerLastSessionProfit = roundCurrency(sessionProfit);
    refreshPokerSessionMeta();
    disposed = true;
    if (aiTimeoutId) clearTimeout(aiTimeoutId);
    timers.forEach((id) => clearTimeout(id));
    timers.clear();
    listeners.forEach((off) => off());
    container.classList.remove("casino-fullbleed", "poker-fullbleed", "poker-classic-fullbleed");
  };
  } catch (error) {
    console.error("Casino poker failed to load", error);
    container.classList.remove("casino-fullbleed", "poker-fullbleed", "poker-classic-fullbleed");
    container.innerHTML = `
      <div style="padding:20px;border:1px solid #3a4a5a;border-radius:12px;background:#132230;color:#f0f6ff">
        Poker failed to load. Open dev console and send me the error text.
      </div>
    `;
  }
}

function loadAppPoker() {
  const container = document.getElementById("casino-container");
  if (!container) return;
  container.classList.add("casino-fullbleed", "poker-fullbleed");

  container.innerHTML = `
    <div class="poker-wrapper">
      <div id="poker-table">
        <div class="center-area">
          <div id="pot-wrap">Pot: $<span id="p_pot">0</span></div>
          <div id="pot-chips" aria-hidden="true"></div>
          <div id="comm-cards"></div>
          <div id="poker-status">Waiting for game...</div>
        </div>

        <div id="deal-overlay">
          <div id="msg-overlay"></div>
          <button id="btn-deal" class="btn-lrg">Deal Hand</button>
        </div>

        <div id="poker-controls" class="poker-hidden">
          <button id="btn-fold" class="btn-poker red">Fold</button>
          <button id="btn-check" class="btn-poker">Check</button>
          <button id="btn-call" class="btn-poker">Call <span id="amt-call"></span></button>
          <div class="raise-box">
            <input type="number" id="raise-input" value="50" step="10">
            <button id="btn-raise" class="btn-poker gold">Raise</button>
          </div>
        </div>
      </div>
    </div>
  `;
  addSageBrand(container.querySelector(".poker-wrapper"), "top-left");

  pk = {
    pot: document.getElementById("p_pot"),
    chips: document.getElementById("pot-chips"),
    status: document.getElementById("poker-status"),
    comm: document.getElementById("comm-cards"),
    dealOverlay: document.getElementById("deal-overlay"),
    msgOverlay: document.getElementById("msg-overlay"),
    btnDeal: document.getElementById("btn-deal"),
    controls: document.getElementById("poker-controls"),
    btnFold: document.getElementById("btn-fold"),
    btnCheck: document.getElementById("btn-check"),
    btnCall: document.getElementById("btn-call"),
    btnRaise: document.getElementById("btn-raise"),
    inputRaise: document.getElementById("raise-input"),
    amtCall: document.getElementById("amt-call"),
    table: document.getElementById("poker-table")
  };

  pk.btnDeal.onclick = startHand;
  pk.btnFold.onclick = () => act("Fold");
  pk.btnCheck.onclick = () => act("Check");
  pk.btnCall.onclick = () => act("Call");
  pk.btnRaise.onclick = () => act("Raise");
  pk.inputRaise.oninput = () => {
    const current = p_seats[p_turnIndex];
    if (!current || current.isAi) return;
    updateLegacyPokerActionControls(current);
  };

  initTableLayout();
  initPokerGame();

  const onPokerResize = () => {
    layoutPokerSeats();
    positionPokerControlsNearUser();
  };
  window.addEventListener("resize", onPokerResize);

  activeCasinoCleanup = () => {
    if (p_aiTimeoutId) {
      clearTimeout(p_aiTimeoutId);
      p_aiTimeoutId = null;
    }
    window.removeEventListener("resize", onPokerResize);
    container.classList.remove("casino-fullbleed", "poker-fullbleed");
  };
}

function initTableLayout() {
  const seatWidth = 126;
  const seatHeight = 160;

  for (let i = 0; i < MAX_SEATS; i++) {
    const div = document.createElement("div");
    div.className = "player-seat";
    if (i === 0) div.classList.add("is-user");
    div.innerHTML = `
      <div class="p-avatar">${i === 0 ? "Y" : "S"}</div>
      <div class="speech-bubble" id="bubble-${i}"></div>
      <div class="p-hand-label" id="seat-hand-${i}"></div>
      <div class="p-cards" id="seat-cards-${i}"></div>
      <div class="p-info">
        <div class="p-name">Empty</div>
        <div class="p-bank" id="seat-bank-${i}"></div>
      </div>
    `;

    div.dataset.seatWidth = String(seatWidth);
    div.dataset.seatHeight = String(seatHeight);

    pk.table.appendChild(div);
  }

  layoutPokerSeats();
}

function layoutPokerSeats() {
  if (!pk.table) return;

  const tableWidth = pk.table.clientWidth || 900;
  const tableHeight = pk.table.clientHeight || 600;
  const rx = Math.max(300, Math.min(560, tableWidth * 0.44));
  const ry = Math.max(170, Math.min(320, tableHeight * 0.4));

  for (let i = 0; i < MAX_SEATS; i++) {
    const seatDiv = pk.table.querySelectorAll(".player-seat")[i];
    if (!seatDiv) continue;
    const seatWidth = Number(seatDiv.dataset.seatWidth || 116);
    const seatHeight = Number(seatDiv.dataset.seatHeight || 126);

    const angle = (i / MAX_SEATS) * 2 * Math.PI + Math.PI / 2;
    const x = Math.cos(angle) * rx;
    const y = Math.sin(angle) * ry;

    seatDiv.classList.toggle("seat-top", y < -ry * 0.1);
    seatDiv.style.left = `calc(50% + ${x}px - ${seatWidth / 2}px)`;
    seatDiv.style.top = `calc(50% + ${y}px - ${seatHeight / 2}px)`;
  }
}

function positionPokerControlsNearUser() {
  if (!pk?.table || !pk?.controls) return;
  const userSeat = document.getElementById("seat-cards-0")?.parentElement;
  if (!userSeat) return;

  const tableRect = pk.table.getBoundingClientRect();
  const seatRect = userSeat.getBoundingClientRect();
  const panelWidth = pk.controls.offsetWidth || 160;
  const panelHeight = pk.controls.offsetHeight || 220;
  const gap = 14;
  const pad = 8;
  const extraBottomRoom = 20;

  let left = seatRect.right - tableRect.left + gap;
  let top = seatRect.top - tableRect.top + (seatRect.height - panelHeight) / 2 + 40;

  left = Math.min(left, tableRect.width - panelWidth - pad);
  left = Math.max(pad, left);
  top = Math.max(pad, Math.min(top, tableRect.height - panelHeight + extraBottomRoom));

  pk.controls.style.left = `${left}px`;
  pk.controls.style.top = `${top}px`;
}

function initPokerGame() {
  p_handInProgress = false;
  if (pk?.btnDeal) pk.btnDeal.disabled = false;
  p_seats[0] = {
    id: 0,
    name: "You",
    isAi: false,
    bank: cash,
    hand: [],
    bet: 0,
    folded: false,
    inHand: false,
    style: "Human"
  };

  fillEmptySeats(5);
  updatePokerUI();
  positionPokerControlsNearUser();
}

function fillEmptySeats(count) {
  let filled = 0;
  for (let i = 1; i < MAX_SEATS; i++) {
    if (p_seats[i] === null && filled < count) {
      if (Math.random() < 0.8) {
        p_seats[i] = generateBot(i);
        filled++;
      }
    }
  }
}

function generateBot(id) {
  const name = BOT_NAMES[Math.floor(Math.random() * BOT_NAMES.length)];
  const types = Object.keys(ARCHETYPES);
  const typeName = types[Math.floor(Math.random() * types.length)];
  const randomBank = 500 + Math.floor(Math.random() * 2500);

  return {
    id,
    name,
    isAi: true,
    bank: randomBank,
    hand: [],
    bet: 0,
    folded: false,
    inHand: false,
    style: typeName,
    stats: ARCHETYPES[typeName]
  };
}

function startHand() {
  if (p_handInProgress) return;
  p_handInProgress = true;
  if (pk?.btnDeal) pk.btnDeal.disabled = true;

  const gate = getCasinoGateState();
  if (!gate.ok) {
    if (gate.reason === "trading_total") {
      clearCasinoKickoutTimer();
      showCasinoKickoutOverlay("Did not hit trading quota. Please return to the trading floor.");
    } else if (pk?.msgOverlay) {
      pk.msgOverlay.innerText = gate.message || "Casino is locked.";
    }
    p_handInProgress = false;
    if (pk?.btnDeal) pk.btnDeal.disabled = false;
    return;
  }

  if (p_aiTimeoutId) {
    clearTimeout(p_aiTimeoutId);
    p_aiTimeoutId = null;
  }
  p_seats[0].bank = cash;
  p_playerRaiseUsedThisRound = false;

  if (p_seats[0].bank <= 0) {
    pk.msgOverlay.innerText = "You are bust! Go trade more.";
    pk.btnDeal.classList.add("poker-hidden");
    p_handInProgress = false;
    return;
  }

  p_deck = [];
  for (const s of P_SUITS) for (const v of P_VALUES) p_deck.push(new PokerCard(s, v));
  p_deck.sort(() => Math.random() - 0.5);

  p_community = [];
  p_pot = 0;
  p_highestBet = 10;
  p_phase = 0;
  p_actionsTaken = 0;
  pk.msgOverlay.innerText = "";
  pk.inputRaise.value = 50;

  p_seats.forEach((p, i) => {
    if (!p) {
      renderPokerCards(i, []);
      return;
    }

    if (p.bank > 0) {
      p.inHand = true;
      p.folded = false;
      const firstCard = p_deck.pop();
      const secondCard = p_deck.pop();
      p.hand = [firstCard, secondCard];
      playGameSound("card_deal");
      playGameSound("card_deal");

      const ante = Math.min(10, p.bank);
      p.bank -= ante;
      p.bet = ante;
      p_pot += ante;

      if (!p.isAi) cash = p.bank;

      document
        .getElementById(`seat-cards-${i}`)
        .parentElement.classList.remove("seat-folded", "turn-active", "winner-glow");
      renderPokerCards(i, p.hand, p.isAi);
    } else {
      p.inHand = false;
    }
  });

  updateUI();
  pk.dealOverlay.classList.add("poker-hidden");
  pk.comm.innerHTML = "";
  pk.status.innerText = "Pre-Flop";

  p_turnIndex = p_dealerIndex;
  findNextActivePlayer();
  nextTurn();
}

function findNextActivePlayer() {
  let checked = 0;
  do {
    p_turnIndex = (p_turnIndex + 1) % MAX_SEATS;
    checked++;
    if (checked > MAX_SEATS + 2) return;
  } while (!p_seats[p_turnIndex] || !p_seats[p_turnIndex].inHand || p_seats[p_turnIndex].folded);
}

function updateLegacyPokerActionControls(player) {
  if (!pk || !player || player.isAi) return;
  const toCall = Math.max(0, p_highestBet - player.bet);
  pk.btnCheck.style.display = toCall > 0 ? "none" : "block";
  pk.btnCall.style.display = toCall === 0 ? "none" : "block";
  pk.amtCall.innerText = toCall > 0 ? `$${toCall}` : "";

  let raiseBy = Number.parseInt(pk.inputRaise.value, 10);
  if (Number.isNaN(raiseBy) || raiseBy < 10) raiseBy = 10;
  const targetBet = p_highestBet + raiseBy;
  const added = Math.max(0, targetBet - player.bet);
  const shortBy = Math.max(0, added - player.bank);
  const maxRaiseBy = Math.max(0, Math.floor(player.bank + player.bet - p_highestBet));

  if (shortBy > 0) {
    pk.btnRaise.textContent = `Raise To $${targetBet} (Need $${Math.ceil(shortBy)} more)`;
    pk.btnRaise.title = `Not enough chips to raise. Max raise by: $${maxRaiseBy}.`;
  } else {
    pk.btnRaise.textContent = `Raise To $${targetBet}`;
    pk.btnRaise.title = "";
  }
  pk.btnRaise.disabled = p_playerRaiseUsedThisRound;
}

function nextTurn() {
  const activeInHand = p_seats.filter((p) => p && p.inHand && !p.folded);
  if (activeInHand.length === 1) {
    forceWin(activeInHand[0]);
    return;
  }

  const p = p_seats[p_turnIndex];
  updatePokerUI();

  document.querySelectorAll(".player-seat").forEach((d) => d.classList.remove("turn-active"));
  document.getElementById(`seat-cards-${p_turnIndex}`).parentElement.classList.add("turn-active");

  if (p.isAi) {
    pk.controls.classList.add("poker-hidden");
    if (p_aiTimeoutId) clearTimeout(p_aiTimeoutId);
    p_aiTimeoutId = setTimeout(() => {
      p_aiTimeoutId = null;
      aiMove(p);
    }, 900 + Math.random() * 900);
  } else {
    if (p_aiTimeoutId) {
      clearTimeout(p_aiTimeoutId);
      p_aiTimeoutId = null;
    }
    pk.controls.classList.remove("poker-hidden");
    positionPokerControlsNearUser();
    updateLegacyPokerActionControls(p);
  }
}

function nextPhase() {
  p_actionsTaken = 0;
  p_highestBet = 0;
  p_playerRaiseUsedThisRound = false;
  p_seats.forEach((p) => {
    if (p) p.bet = 0;
  });
  p_phase++;

  if (p_phase === 1) {
    const flop = [p_deck.pop(), p_deck.pop(), p_deck.pop()];
    p_community.push(...flop);
    flop.forEach(() => playGameSound("card_deal"));
  } else if (p_phase === 2) {
    p_community.push(p_deck.pop());
    playGameSound("card_deal");
  } else if (p_phase === 3) {
    p_community.push(p_deck.pop());
    playGameSound("card_deal");
  }
  else {
    showdown();
    return;
  }

  pk.comm.innerHTML = "";
  p_community.forEach((c) => renderPokerCardDiv(c, pk.comm));
  pk.status.innerText = ["Pre-Flop", "Flop", "Turn", "River"][p_phase];

  p_turnIndex = p_dealerIndex;
  findNextActivePlayer();
  nextTurn();
}

function act(action) {
  const p = p_seats[p_turnIndex];
  if (!p) return;
  if (action === "Raise" && !p.isAi && p_playerRaiseUsedThisRound) {
    pk.status.innerText = "You can only raise once per round.";
    return;
  }

  p_actionsTaken++;

  if (action === "Raise") {
    let val = parseInt(pk.inputRaise.value);
    if (p.isAi) val = Math.max(50, Math.floor(p_pot * 0.5));
    if (isNaN(val) || val < 10) val = 10;

    const total = p_highestBet + val;
    const added = total - p.bet;

    if (!p.isAi && p.bank < added) {
      const shortBy = Math.ceil(added - p.bank);
      pk.status.innerText = `Not enough chips to raise. Need $${shortBy} more.`;
      say(p.id, "Can't raise");
      updateLegacyPokerActionControls(p);
      p_actionsTaken = Math.max(0, p_actionsTaken - 1);
      return;
    }

    if (p.bank >= added) {
      p.bank -= added;
      p.bet += added;
      p_pot += added;
      p_highestBet = p.bet;
      p_actionsTaken = 0;
      say(p.id, `Raise ${val}`);
      if (!p.isAi) p_playerRaiseUsedThisRound = true;
    } else action = "Call";
  }

  if (action === "Fold") {
    p.folded = true;
    document.getElementById(`seat-cards-${p_turnIndex}`).parentElement.classList.add("seat-folded");
    say(p.id, "Fold");
  } else if (action === "Check") {
    say(p.id, "Check");
  } else if (action === "Call") {
    const amt = p_highestBet - p.bet;
    const pay = Math.min(amt, p.bank);
    p.bank -= pay;
    p.bet += pay;
    p_pot += pay;
    say(p.id, "Call");
  }

  if (!p.isAi) cash = p.bank;

  const activePlayers = p_seats.filter((pl) => pl && pl.inHand && !pl.folded);
  const allMatched = activePlayers.every((pl) => pl.bet === p_highestBet || pl.bank === 0);

  if (allMatched && p_actionsTaken >= activePlayers.length) nextPhase();
  else {
    findNextActivePlayer();
    nextTurn();
  }
}

function aiMove(p) {
  const toCall = p_highestBet - p.bet;
  const potOdds = toCall / (p_pot + toCall || 1);

  let strength = 0;
  if (p_phase === 0) strength = rateHoleCards(p.hand);
  else {
    const ev = evaluatePokerHand(p.hand, p_community);
    strength = ev.tier * 12 + (ev.kickers[0] || 0) / 2;
    if (ev.tier >= 4) strength = 95;
  }

  const stats = p.stats;
  const roll = Math.random();
  let move = "Fold";

  if (toCall === 0) {
    if (strength > 60 || (roll < stats.bluff && strength < 30)) {
      move = Math.random() < stats.aggr ? "Raise" : "Check";
    } else move = "Check";
  } else {
    const req = potOdds * 100;
    const perceived = strength + stats.vpip * 25;
    if (perceived >= req) {
      if (strength > 80 && Math.random() < stats.aggr) move = "Raise";
      else move = "Call";
    } else {
      if (roll < stats.bluff * 0.1) move = "Raise";
      else move = "Fold";
    }
  }

  if (move === "Raise" && p.bank < 50) move = "Call";
  act(move);
}

function rateHoleCards(hand) {
  if (!hand || hand.length < 2) return 0;
  const c1 = hand[0],
    c2 = hand[1];
  let s = c1.r + c2.r;
  if (c1.r === c2.r) s += 25;
  if (c1.s === c2.s) s += 5;
  if (Math.abs(c1.r - c2.r) === 1) s += 5;
  return Math.min(100, s * 2);
}

function evaluatePokerHand(hand, comm) {
  const all = hand.concat(comm);
  const getCounts = (cards) => {
    const counts = {};
    cards.forEach((c) => (counts[c.r] = (counts[c.r] || 0) + 1));
    return counts;
  };
  const getStraight = (cards) => {
    const unique = [...new Set(cards.map((c) => c.r))].sort((a, b) => b - a);
    for (let i = 0; i < unique.length - 4; i++) {
      if (unique[i] - unique[i + 4] === 4) return unique[i];
    }
    if (
      unique.includes(14) &&
      unique.includes(2) &&
      unique.includes(3) &&
      unique.includes(4) &&
      unique.includes(5)
    )
      return 5;
    return 0;
  };

  const suits = {};
  all.forEach((c) => (suits[c.s] = (suits[c.s] || 0) + 1));
  const flushSuit = Object.keys(suits).find((s) => suits[s] >= 5);
  let flushCards = [];
  if (flushSuit) {
    flushCards = all.filter((c) => c.s === flushSuit);
    const sfHigh = getStraight(flushCards);
    if (sfHigh > 0) return { tier: 8, kickers: [sfHigh], name: "Straight Flush" };
  }

  const counts = getCounts(all);
  const ranks = all.map((c) => c.r).sort((a, b) => b - a);
  const quads = Object.keys(counts)
    .filter((r) => counts[r] === 4)
    .map(Number)
    .sort((a, b) => b - a);
  const trips = Object.keys(counts)
    .filter((r) => counts[r] === 3)
    .map(Number)
    .sort((a, b) => b - a);
  const pairs = Object.keys(counts)
    .filter((r) => counts[r] === 2)
    .map(Number)
    .sort((a, b) => b - a);
  const getKickers = (n, exclude) => ranks.filter((r) => !exclude.includes(r)).slice(0, n);

  if (quads.length > 0)
    return {
      tier: 7,
      kickers: [quads[0], ...getKickers(1, [quads[0]])],
      name: "Four of a Kind"
    };
  if (trips.length > 0 && (trips.length >= 2 || pairs.length > 0)) {
    const t = trips[0],
      p = trips.length >= 2 ? trips[1] : pairs[0];
    return { tier: 6, kickers: [t, p], name: "Full House" };
  }
  if (flushSuit) {
    const top5 = flushCards
      .map((c) => c.r)
      .sort((a, b) => b - a)
      .slice(0, 5);
    return { tier: 5, kickers: top5, name: "Flush" };
  }
  const strHigh = getStraight(all);
  if (strHigh > 0) return { tier: 4, kickers: [strHigh], name: "Straight" };
  if (trips.length > 0)
    return {
      tier: 3,
      kickers: [trips[0], ...getKickers(2, [trips[0]])],
      name: "Three of a Kind"
    };
  if (pairs.length >= 2)
    return {
      tier: 2,
      kickers: [pairs[0], pairs[1], ...getKickers(1, [pairs[0], pairs[1]])],
      name: "Two Pair"
    };
  if (pairs.length === 1)
    return { tier: 1, kickers: [pairs[0], ...getKickers(3, [pairs[0]])], name: "Pair" };
  return { tier: 0, kickers: getKickers(5, []), name: "High Card" };
}

function compareHands(a, b) {
  if (a.tier > b.tier) return 1;
  if (b.tier > a.tier) return -1;
  for (let i = 0; i < Math.max(a.kickers.length, b.kickers.length); i++) {
    const kA = a.kickers[i] || 0,
      kB = b.kickers[i] || 0;
    if (kA > kB) return 1;
    if (kB > kA) return -1;
  }
  return 0;
}

function showdown() {
  endRoundLogic(false);
}
function forceWin(winner) {
  endRoundLogic(true, winner);
}

function endRoundLogic(earlyWin, forceWinner = null) {
  if (p_aiTimeoutId) {
    clearTimeout(p_aiTimeoutId);
    p_aiTimeoutId = null;
  }
  pk.controls.classList.add("poker-hidden");

  let winners = [];
  let winDesc = "";
  let playerWonHand = false;

  if (forceWinner) {
    winners = [forceWinner];
    winDesc = "(Opponents Folded)";
  } else {
    p_seats.forEach((p, i) => {
      if (p && p.inHand && !p.folded && p.isAi) renderPokerCards(i, p.hand, false);
    });

    const active = p_seats.filter((p) => p && p.inHand && !p.folded);
    active.forEach((p) => (p.eval = evaluatePokerHand(p.hand, p_community)));
    active.sort((a, b) => compareHands(b.eval, a.eval));

    if (active.length > 0) {
      const bestHand = active[0].eval;
      winners = active.filter((p) => compareHands(p.eval, bestHand) === 0);
      winDesc = `with ${bestHand.name}`;
    }
  }

  document.querySelectorAll(".player-seat").forEach((s) => s.classList.remove("winner-glow"));
  winners.forEach((w) => {
    document.getElementById(`seat-cards-${w.id}`).parentElement.classList.add("winner-glow");
  });

  if (winners.length > 0) {
    const splitAmt = Math.floor(p_pot / winners.length);
    winners.forEach((w) => (w.bank += splitAmt));
    const names = winners.map((w) => w.name).join(" & ");
    pk.status.innerText =
      winners.length > 1 ? `Split Pot! ${names} ${winDesc}` : `${names} Wins ${winDesc}`;

    playerWonHand = winners.some((w) => !w.isAi);
    if (playerWonHand) {
      const prevCash = cash;
      cash = p_seats[0].bank;
      const profit = cash - prevCash;
      if (profit > 0) {
        const autoSaved = showCasinoWinPopup({ amount: profit, multiplier: null, source: "poker" });
        if (autoSaved > 0) {
          p_seats[0].bank = roundCurrency(Math.max(0, p_seats[0].bank - autoSaved));
          cash = roundCurrency(Math.max(0, cash - autoSaved));
        }
      }
    }
  }

  updateUI();
  updatePokerUI();
  if (!playerWonHand) {
    playGameSound("loss");
    triggerCasinoKickoutCheckAfterRound();
  }
  p_handInProgress = false;
  if (pk?.btnDeal) pk.btnDeal.disabled = false;
  pk.dealOverlay.classList.remove("poker-hidden");
  pk.btnDeal.classList.remove("poker-hidden");

  p_dealerIndex = (p_dealerIndex + 1) % MAX_SEATS;

  setTimeout(() => {
    const leaving = [];
    for (let i = 1; i < MAX_SEATS; i++) {
      const p = p_seats[i];
      if (!p) continue;
      if (p.bank <= 0) {
        say(i, "I'm out!");
        leaving.push(i);
      }
    }
    leaving.forEach((idx) => {
      p_seats[idx] = null;
      updatePokerUI();
    });

    const activeCount = p_seats.filter((p) => p !== null).length;
    if (activeCount < 4) fillEmptySeats(3);
    updatePokerUI();
  }, 2000);
}

function renderPokerCardDiv(c, target) {
  const d = document.createElement("div");
  d.className = `poker-card ${c.color}`;
  d.innerHTML = `${c.v}<small>${c.s}</small>`;
  target.appendChild(d);
}

function renderPokerCards(seatIdx, cards, hideAi = false) {
  const container = document.getElementById(`seat-cards-${seatIdx}`);
  container.innerHTML = "";
  cards.forEach((c) => {
    if (hideAi) {
      const d = document.createElement("div");
      d.className = "poker-card card-back";
      container.appendChild(d);
    } else renderPokerCardDiv(c, container);
  });
}

function renderPotChips() {
  if (!pk.chips) return;

  const potAmount = Math.max(0, Math.floor(p_pot));
  const targetCount = potAmount <= 0 ? 0 : Math.max(1, Math.min(56, Math.floor(Math.sqrt(potAmount) * 0.9)));
  const currentCount = pk.chips.children.length;

  const chipPalette = [{ color: "#ff174c", edge: "#ffd6df" }];

  const createChipEl = (index) => {
    const goldenAngle = 2.399963229728653;
    const angle = index * goldenAngle;
    const spread = Math.sqrt(index + 1);
    const radiusX = 16 * spread;
    const radiusY = 10 * spread;
    const jitterX = ((index * 13) % 7) - 3;
    const jitterY = (index * 9) % 4;
    const baseX = Math.cos(angle) * radiusX + jitterX;
    const baseY = 8 + Math.abs(Math.sin(angle) * radiusY) + jitterY;
    const rotation = ((index * 19) % 18) - 9;
    const palette = chipPalette[index % chipPalette.length];

    const chipEl = document.createElement("span");
    chipEl.className = "pot-chip";
    chipEl.style.setProperty("--chip-x", `${baseX.toFixed(2)}px`);
    chipEl.style.setProperty("--chip-y", `${baseY.toFixed(2)}px`);
    chipEl.style.setProperty("--chip-rot", `${rotation}deg`);
    chipEl.style.setProperty("--chip-color", palette.color);
    chipEl.style.setProperty("--chip-edge", palette.edge);
    chipEl.style.animationDelay = `${(index * 11).toFixed(0)}ms`;
    chipEl.style.zIndex = String(100 + index);
    return chipEl;
  };

  if (targetCount > currentCount) {
    for (let index = currentCount; index < targetCount; index += 1) {
      pk.chips.appendChild(createChipEl(index));
    }
  } else if (targetCount < currentCount) {
    for (let index = currentCount; index > targetCount; index -= 1) {
      pk.chips.lastElementChild?.remove();
    }
  }
}

function updatePokerUI() {
  if (!pk.pot) return;
  pk.pot.innerText = p_pot;
  renderPotChips();

  for (let i = 0; i < MAX_SEATS; i++) {
    const p = p_seats[i];
    const handEl = document.getElementById(`seat-hand-${i}`);
    const nameEl = document.querySelector(`#seat-cards-${i} + .p-info .p-name`);
    const bankEl = document.getElementById(`seat-bank-${i}`);
    const seatDiv = document.getElementById(`seat-cards-${i}`).parentElement;
    const avatarEl = seatDiv.querySelector(".p-avatar");

    if (!p) {
      nameEl.innerText = "Empty";
      nameEl.style.opacity = "0.5";
      bankEl.innerText = "";
      if (handEl) handEl.innerText = "";
      seatDiv.classList.add("seat-empty");
      seatDiv.classList.remove("seat-occupied");
      if (avatarEl) avatarEl.textContent = "✕";
    } else {
      nameEl.innerText = p.name;
      nameEl.style.opacity = "1";
      bankEl.innerText = `$${Math.floor(p.bank)}`;
      seatDiv.classList.add("seat-occupied");
      seatDiv.classList.remove("seat-empty");
      if (avatarEl) avatarEl.textContent = i === 0 ? "Y" : p.name.charAt(0).toUpperCase();
      if (handEl) {
        if (i === 0 && p.inHand && !p.folded && p.hand && p.hand.length > 0) {
          handEl.innerText = evaluatePokerHand(p.hand, p_community).name;
        } else if (i === 0 && p.folded) {
          handEl.innerText = "Folded";
        } else {
          handEl.innerText = "";
        }
      }
      if (!p.inHand && p_phase > 0) nameEl.innerText += " (Wait)";
    }
  }
}

function say(seatIdx, txt) {
  const b = document.getElementById(`bubble-${seatIdx}`);
  if (!b) return;
  b.innerText = txt;
  b.classList.add("show");
  setTimeout(() => b.classList.remove("show"), 2000);
}

function loadCasinoPlinkoNeon() {
  const container = document.getElementById("casino-container");
  if (!container) return;
  container.classList.add("casino-fullbleed", "plinko-neon-fullbleed");

  container.innerHTML = `
    <div id="plinkoCasinoNeon" class="plinko-casino-neon">
      <div class="app-layout">
        <div class="game-container">
          <div class="controls">
            <div class="brand">SAGE</div>

            <div class="input-group">
              <label>Bet Amount</label>
              <input type="number" id="betAmount" value="10" min="1" max="1000">
            </div>

            <div class="input-group">
              <label>Rows</label>
              <select id="rowCount">
                <option value="8">8 Rows</option>
                <option value="10">10 Rows</option>
                <option value="12">12 Rows</option>
                <option value="14">14 Rows</option>
                <option value="16" selected>16 Rows</option>
              </select>
            </div>

            <div class="input-group">
              <label>Difficulty</label>
              <select id="difficulty">
                <option value="easy">Easy</option>
                <option value="medium" selected>Medium</option>
                <option value="hard">Hard</option>
              </select>
            </div>

            <div class="input-group">
              <label>Auto Speed</label>
              <input type="range" id="autoSpeed" min="200" max="1000" step="100" value="400">
            </div>

            <button id="autoBtn" class="action-btn auto-btn">AUTO</button>
            <button id="dropBtn" class="action-btn">DROP BALL</button>
          </div>

          <div class="stats">
            <div class="stat-box">
              <span>Balance</span>
              <span id="balanceDisplay" class="highlight">${roundCurrency(cash).toFixed(2)}</span>
            </div>
            <div class="stat-box">
              <span>Last Result</span>
              <span id="lastWinDisplay">0.00</span>
            </div>
          </div>

          <div class="canvas-wrapper">
            <canvas id="plinkoCanvas"></canvas>
            <div id="oddsTooltip" class="odds-tooltip">Chance: 0.00%</div>
            <div id="multipliers" class="multiplier-container"></div>
          </div>
        </div>

        <div class="history-column">
          <div class="history-header">LAST DROPS</div>
          <div id="historyList" class="history-list"></div>
        </div>
      </div>
    </div>
  `;

  const root = container.querySelector("#plinkoCasinoNeon");
  const canvas = root?.querySelector("#plinkoCanvas");
  if (!root || !canvas) return;
  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  const WIDTH = 600;
  const PEG_RADIUS = 4;
  const BALL_RADIUS = 7;
  const SPACING_Y = 35;
  const PADDING_TOP = 60;
  const PADDING_BOTTOM = 60;

  let pegs = [];
  let balls = [];
  let rows = 16;
  let slotWidth = 32;
  let balance = roundCurrency(cash);
  let difficulty = "medium";
  let MULTIPLIERS = [];
  let autoInterval = null;
  let animationFrame = 0;

  function getActiveBallCount() {
    return balls.reduce((count, ball) => count + (ball.done ? 0 : 1), 0);
  }

  function updatePlinkoControlLocks() {
    const locked = getActiveBallCount() > 0 || Boolean(autoInterval);
    if (rowCountSelect) rowCountSelect.disabled = locked;
    if (difficultySelect) difficultySelect.disabled = locked;
  }

  const CUSTOM_WEIGHTS = {
    8: [0.39, 3.13, 10.94, 21.88, 27.34, 21.88, 10.94, 3.13, 0.39],
    10: [0.098, 0.977, 4.395, 11.72, 20.51, 24.61, 20.51, 11.72, 4.395, 0.977, 0.098],
    12: [0.024, 0.293, 1.613, 5.371, 12.09, 19.34, 22.37, 19.34, 12.09, 5.371, 1.613, 0.293, 0.024],
    14: [0.006, 0.086, 0.555, 2.22, 5.44, 9.69, 12.64, 13.04, 12.64, 9.69, 5.44, 2.22, 0.555, 0.086, 0.006],
    16: [0.0015, 0.0244, 0.183, 0.854, 2.78, 6.67, 12.22, 17.47, 19.64, 17.47, 12.22, 6.67, 2.78, 0.854, 0.183, 0.0244, 0.0015]
  };

  const MULTIPLIER_DATA = {
    8: {
      easy: [5.6, 2.1, 1.1, 1, 0.5, 1, 1.1, 2.1, 5.6],
      medium: [13, 3, 1.3, 0.7, 0.4, 0.7, 1.3, 3, 13],
      hard: [29, 4, 1.5, 0.5, 0.2, 0.5, 1.5, 4, 29]
    },
    10: {
      easy: [8.9, 3, 1.5, 1.2, 1, 0.5, 1, 1.2, 1.5, 3, 8.9],
      medium: [22, 5, 2, 1.4, 0.6, 0.4, 0.6, 1.4, 2, 5, 22],
      hard: [76, 9, 3, 1, 0.4, 0.2, 0.4, 1, 3, 9, 76]
    },
    12: {
      easy: [10, 3.5, 1.6, 1.3, 1.1, 0.8, 0.5, 0.8, 1.1, 1.3, 1.6, 3.5, 10],
      medium: [33, 11, 4, 2, 1.1, 0.6, 0.3, 0.6, 1.1, 2, 4, 11, 33],
      hard: [170, 12, 5, 2, 0.5, 0.3, 0.2, 0.3, 0.5, 2, 5, 12, 170]
    },
    14: {
      easy: [7.1, 2.8, 1.4, 1.1, 0.9, 0.7, 0.6, 0.5, 0.6, 0.7, 0.9, 1.1, 1.4, 2.8, 7.1],
      medium: [58, 15, 7, 4, 1.9, 1, 0.5, 0.2, 0.5, 1, 1.9, 4, 7, 15, 58],
      hard: [420, 18, 7, 3, 1, 0.5, 0.3, 0.2, 0.3, 0.5, 1, 3, 7, 18, 420]
    },
    16: {
      easy: [8, 4, 2, 1.6, 1.4, 1.25, 1.1, 1, 0.5, 1, 1.1, 1.25, 1.4, 1.6, 2, 4, 8],
      medium: [110, 41, 10, 5, 3, 1.5, 1, 0.5, 0.2, 0.5, 1, 1.5, 3, 5, 10, 41, 110],
      hard: [1000, 130, 26, 9, 4, 2, 0.5, 0.3, 0.2, 0.3, 0.5, 2, 4, 9, 26, 130, 1000]
    }
  };

  const balanceDisplay = root.querySelector("#balanceDisplay");
  const lastWinDisplay = root.querySelector("#lastWinDisplay");
  const multipliersEl = root.querySelector("#multipliers");
  const oddsTooltip = root.querySelector("#oddsTooltip");
  const historyList = root.querySelector("#historyList");
  const betInput = root.querySelector("#betAmount");
  const rowCountSelect = root.querySelector("#rowCount");
  const difficultySelect = root.querySelector("#difficulty");
  const speedInput = root.querySelector("#autoSpeed");
  const autoBtn = root.querySelector("#autoBtn");
  const dropBtn = root.querySelector("#dropBtn");

  const cleanupFns = [];
  const bind = (node, event, handler, options) => {
    if (!node) return;
    node.addEventListener(event, handler, options);
    cleanupFns.push(() => node.removeEventListener(event, handler, options));
  };

  function syncGlobalCash() {
    balance = roundCurrency(balance);
    cash = balance;
    updateUI();
    updateBlackjackCash();
  }

  function weightedPick(weights) {
    const total = weights.reduce((a, b) => a + b, 0);
    let r = Math.random() * total;
    let sum = 0;
    for (let i = 0; i < weights.length; i += 1) {
      sum += weights[i];
      if (r <= sum) return i;
    }
    return Math.floor(weights.length / 2);
  }

  function getWeightsForRows(rowCount) {
    if (CUSTOM_WEIGHTS[rowCount]) return CUSTOM_WEIGHTS[rowCount];
    const fallback = [];
    for (let i = 0; i <= rowCount; i += 1) fallback.push(binomial(rowCount, i));
    return fallback;
  }

  function getResultBasedOnWeights() {
    const weights = getWeightsForRows(rows);
    return weightedPick(weights);
  }

  function factorial(n) {
    let result = 1;
    for (let i = 2; i <= n; i += 1) result *= i;
    return result;
  }

  function binomial(n, k) {
    return factorial(n) / (factorial(k) * factorial(n - k));
  }

  function initBoard() {
    pegs = [];
    const calculatedHeight = PADDING_TOP + rows * SPACING_Y + PADDING_BOTTOM;

    canvas.width = WIDTH;
    canvas.height = calculatedHeight;
    canvas.style.height = `${calculatedHeight}px`;

    let rowData = MULTIPLIER_DATA[rows];
    if (!rowData) rowData = MULTIPLIER_DATA[16];
    MULTIPLIERS = rowData[difficulty] || rowData.medium;

    const slotCount = rows + 1;
    slotWidth = (WIDTH - 40) / slotCount;
    if (slotWidth > 45) slotWidth = 45;

    for (let r = 0; r < rows; r += 1) {
      const count = r + 3;
      const rowWidth = (count - 1) * slotWidth;
      const offsetX = (WIDTH - rowWidth) / 2;
      for (let c = 0; c < count; c += 1) {
        pegs.push({ x: offsetX + c * slotWidth, y: PADDING_TOP + r * SPACING_Y });
      }
    }

    renderMultipliers();
  }

  function getColor(value) {
    if (value < 1) return "#ff003c";
    if (value < 2) return "#d9ed92";
    if (value < 10) return "#00e701";
    return "#f3ba2f";
  }

  function renderMultipliers() {
    if (!multipliersEl || !oddsTooltip) return;
    multipliersEl.innerHTML = "";
    multipliersEl.style.display = "grid";

    const weights = getWeightsForRows(rows);
    const totalWeight = weights.reduce((sum, value) => sum + value, 0);
    const bottomPegCount = Math.max(2, rows + 2);
    const fallbackLeftPegX = (WIDTH - (rows + 1) * slotWidth) / 2;
    const fallbackDx = slotWidth;
    let leftPegX = fallbackLeftPegX;
    let dx = fallbackDx;

    if (pegs.length >= bottomPegCount) {
      const bottomRow = pegs.slice(-bottomPegCount);
      leftPegX = bottomRow[0].x;
      const rightPegX = bottomRow[bottomRow.length - 1].x;
      const span = rightPegX - leftPegX;
      dx = bottomRow.length > 1 ? span / (bottomRow.length - 1) : slotWidth;
    }

    const bins = Math.max(1, MULTIPLIERS.length);
    const leftBoundary = leftPegX;
    const canvasRect = canvas.getBoundingClientRect();
    const displayScale = canvasRect.width > 0 ? canvasRect.width / WIDTH : 1;
    const wrapperWidth = multipliersEl.parentElement?.clientWidth || canvasRect.width || WIDTH;
    const canvasOffsetX = Math.max(0, (wrapperWidth - (canvasRect.width || WIDTH)) / 2);
    const scaledDx = dx * displayScale;
    const scaledLeftBoundary = leftBoundary * displayScale;
    multipliersEl.style.left = `${canvasOffsetX + scaledLeftBoundary}px`;
    multipliersEl.style.width = `${scaledDx * bins}px`;
    multipliersEl.style.gridTemplateColumns = `repeat(${bins}, minmax(0, 1fr))`;

    const baseHeight = rows <= 10 ? 34 : rows <= 14 ? 32 : 30;
    const multiplierHeight = baseHeight * Math.max(1, Math.min(1.45, displayScale));
    const baseFontPx = rows <= 10 ? 11 : rows <= 14 ? 10 : 8;
    const multiplierFontPx = Math.max(8, baseFontPx * Math.max(1, Math.min(1.45, displayScale)));
    multipliersEl.style.height = `${Math.round(multiplierHeight)}px`;

    MULTIPLIERS.forEach((multiplier, index) => {
      const div = document.createElement("div");
      div.className = "multiplier";
      div.textContent = `${multiplier}x`;
      div.style.background = getColor(multiplier);
      div.style.margin = "0";
      div.style.width = "100%";
      div.style.height = `${multiplierHeight}px`;
      div.style.fontSize = `${multiplierFontPx}px`;

      const p = weights[index] || 0;
      const pct = ((p / totalWeight) * 100).toFixed(4);

      div.addEventListener("mouseenter", () => {
        const betAmount = Math.max(0, Number.parseFloat(betInput?.value || "0") || 0);
        const payout = roundCurrency(betAmount * Number(multiplier || 0));
        oddsTooltip.textContent = `Chance: ${pct}% • Land here → Win $${payout.toFixed(2)}`;
        oddsTooltip.style.opacity = "1";
      });
      div.addEventListener("mouseleave", () => {
        oddsTooltip.style.opacity = "0";
      });

      multipliersEl.appendChild(div);
    });
  }

  function simulateCasinoPlinko(drops = 100000, rowCount = rows) {
    const targetRows = Number.parseInt(String(rowCount), 10);
    const weights = getWeightsForRows(targetRows);
    const counts = new Array(weights.length).fill(0);
    const safeDrops = Math.max(1, Number.parseInt(String(drops), 10) || 100000);
    const totalWeight = weights.reduce((sum, value) => sum + value, 0);

    for (let i = 0; i < safeDrops; i += 1) {
      const picked = weightedPick(weights);
      counts[picked] += 1;
    }

    const table = counts.map((count, index) => ({
      slot: index,
      expectedPct: Number(((weights[index] / totalWeight) * 100).toFixed(4)),
      observedPct: Number(((count / safeDrops) * 100).toFixed(4))
    }));

    console.table(table);
    return table;
  }

  window.simulateCasinoPlinko = simulateCasinoPlinko;

  class Ball {
    constructor(targetSlot, bet) {
      this.slot = targetSlot;
      this.bet = bet;
      this.done = false;
      const rights = targetSlot;
      const lefts = rows - targetSlot;
      const directions = [];
      for (let i = 0; i < rights; i += 1) directions.push(1);
      for (let i = 0; i < lefts; i += 1) directions.push(-1);
      directions.sort(() => Math.random() - 0.5);

      this.path = [];
      let currentX = WIDTH / 2;
      let currentY = 20;
      this.path.push({ x: currentX, y: currentY });

      for (let i = 0; i < rows; i += 1) {
        currentY = PADDING_TOP + i * SPACING_Y;
        const jitter = (Math.random() - 0.5) * 10;
        currentX += directions[i] * (slotWidth / 2) + jitter;
        this.path.push({ x: currentX, y: currentY });
      }

      const totalBoardWidth = (rows + 1) * slotWidth;
      const boardStartX = (WIDTH - totalBoardWidth) / 2;
      const finalX = boardStartX + this.slot * slotWidth + slotWidth / 2;
      const finalY = PADDING_TOP + rows * SPACING_Y + 20;
      this.path.push({ x: finalX, y: finalY });

      this.nodeIdx = 0;
      this.progress = 0;
      this.speed = 2.4;
      this.x = this.path[0].x;
      this.y = this.path[0].y;
    }

    update(deltaSeconds = 1 / 60) {
      if (this.done) return;
      this.progress += this.speed * deltaSeconds;

      if (this.progress >= 1) {
        this.progress = 0;
        this.nodeIdx += 1;
        if (this.nodeIdx >= this.path.length - 1) {
          this.done = true;
          this.x = this.path[this.path.length - 1].x;
          this.y = this.path[this.path.length - 1].y;
          resolveWin(this.slot, this.bet);
          return;
        }
      }

      const p1 = this.path[this.nodeIdx];
      const p2 = this.path[this.nodeIdx + 1];
      this.x = p1.x + (p2.x - p1.x) * this.progress;
      const linearY = p1.y + (p2.y - p1.y) * this.progress;
      const bounceHeight = -22;
      const arc = Math.sin(this.progress * Math.PI) * bounceHeight;
      this.y = linearY + arc;
    }

    draw() {
      ctx.beginPath();
      ctx.arc(this.x, this.y, BALL_RADIUS, 0, Math.PI * 2);
      ctx.fillStyle = "#f5073f";
      ctx.shadowBlur = 8;
      ctx.shadowColor = "#f5073f";
      ctx.fill();
      ctx.shadowBlur = 0;
    }
  }

  function resolveWin(slot, bet) {
    if (MULTIPLIERS[slot] == null) return;
    playGameSound("plinko_pop");

    const multiplier = MULTIPLIERS[slot];
    const win = roundCurrency(bet * multiplier);
    const profit = roundCurrency(win - bet);
    balance = roundCurrency(balance + win);
    syncGlobalCash();
    if (profit > 0.009) {
      maybeAutoSaveFromCasinoWin({ amount: profit, multiplier, source: "plinko" });
      balance = roundCurrency(cash);
    }
    if (balanceDisplay) balanceDisplay.textContent = balance.toFixed(2);

    if (lastWinDisplay) {
      lastWinDisplay.textContent = win.toFixed(2);
      lastWinDisplay.style.color = win >= bet ? "#00e701" : "#ff003c";
    }

    const slotsDOM = root.querySelectorAll(".multiplier");
    if (slotsDOM[slot]) {
      slotsDOM[slot].classList.add("active");
      setTimeout(() => slotsDOM[slot].classList.remove("active"), 250);
    }

    if (historyList) {
      const historyItem = document.createElement("div");
      historyItem.className = "history-item";
      historyItem.textContent = `${multiplier}x`;
      historyItem.style.background = getColor(multiplier);
      historyList.prepend(historyItem);
    }
    scheduleCasinoKickoutAfterPlinkoDrain(
      () => balls.reduce((count, ball) => count + (ball.done ? 0 : 1), 0)
    );

  }

  function triggerDrop() {
    const activeBalls = getActiveBallCount();
    if (activeBalls <= 0 && !ensureCasinoBettingAllowedNow()) {
      stopAuto();
      return;
    }
    const bet = Number.parseFloat(betInput?.value || "0");

    if (!Number.isFinite(bet) || bet <= 0) {
      alert("Invalid bet amount");
      stopAuto();
      return;
    }

    if (balance < bet) {
      if (autoInterval) {
        stopAuto();
        alert("Insufficient Balance!");
      }
      return;
    }

    balance = roundCurrency(balance - bet);
    syncGlobalCash();
    if (balanceDisplay) balanceDisplay.textContent = balance.toFixed(2);

    const targetSlot = getResultBasedOnWeights();
    balls.push(new Ball(targetSlot, bet));
    updatePlinkoControlLocks();
  }

  function startAuto() {
    stopAuto();
    const baseDelay = Number.parseInt(speedInput?.value || "400", 10);
    const delay = baseDelay;
    triggerDrop();
    autoInterval = window.setInterval(triggerDrop, delay);
    updatePlinkoControlLocks();
    if (autoBtn) {
      autoBtn.textContent = "STOP";
      autoBtn.classList.add("running");
    }
  }

  function stopAuto() {
    if (autoInterval) clearInterval(autoInterval);
    autoInterval = null;
    updatePlinkoControlLocks();
    if (autoBtn) {
      autoBtn.textContent = "AUTO";
      autoBtn.classList.remove("running");
    }
  }

  let lastFrameMs = 0;
  function animate(now = performance.now()) {
    if (!lastFrameMs) lastFrameMs = now;
    const deltaSeconds = Math.min((now - lastFrameMs) / 1000, 1 / 24);
    lastFrameMs = now;

    ctx.clearRect(0, 0, WIDTH, canvas.height);
    ctx.fillStyle = "#f4fbff";
    pegs.forEach((peg) => {
      ctx.beginPath();
      ctx.arc(peg.x, peg.y, PEG_RADIUS, 0, Math.PI * 2);
      ctx.fill();
    });

    balls.forEach((ball) => {
      ball.update(deltaSeconds);
      ball.draw();
    });
    balls = balls.filter((ball) => !ball.done);
    updatePlinkoControlLocks();

    animationFrame = requestAnimationFrame(animate);
  }

  bind(dropBtn, "click", triggerDrop);
  bind(autoBtn, "click", () => {
    if (autoInterval) stopAuto();
    else startAuto();
  });
  bind(speedInput, "input", () => {
    if (autoInterval) startAuto();
  });
  bind(rowCountSelect, "change", (event) => {
    if (rowCountSelect?.disabled) {
      event.target.value = String(rows);
      return;
    }
    rows = Number.parseInt(event.target.value, 10);
    initBoard();
  });
  bind(difficultySelect, "change", (event) => {
    if (difficultySelect?.disabled) {
      event.target.value = difficulty;
      return;
    }
    difficulty = event.target.value;
    initBoard();
  });

  initBoard();
  updatePlinkoControlLocks();
  animate();

  activeCasinoCleanup = () => {
    stopAuto();
    if (animationFrame) cancelAnimationFrame(animationFrame);
    if (window.simulateCasinoPlinko === simulateCasinoPlinko) {
      delete window.simulateCasinoPlinko;
    }
    cleanupFns.forEach((fn) => fn());
    container.classList.remove("casino-fullbleed", "plinko-neon-fullbleed");
  };
}

function loadPlinkoPhoneApp() {
  const container = document.getElementById("casino-container");
  if (!container) return;
  container.classList.add("casino-fullbleed", "plinko-phone-fullbleed");

  container.innerHTML = `
    <div id="plinkoPhoneApp" class="plinko-phone-app">
      <div class="phone-shell">
        <header class="app-header">
          <button id="reloadBtn" class="icon-btn" type="button" aria-label="Reload">↻</button>
          <h1 class="app-title">
            <span class="title-main">Plinko</span>
            <span class="title-brand">SAGE</span>
          </h1>
          <div class="header-actions">
            <button id="closeBtn" class="icon-btn" type="button" aria-label="Close">✕</button>
            <button id="headerMoreBtn" class="icon-btn" type="button" aria-label="More">…</button>
          </div>
        </header>

        <main class="board-panel">
          <div class="status-row">
            <div class="stat-pill">
              <span class="stat-label">Balance</span>
              <strong id="balanceDisplay" class="stat-value">$${roundCurrency(cash).toFixed(2)}</strong>
            </div>
            <div class="stat-pill">
              <span class="stat-label">Last Result</span>
              <strong id="lastWinDisplay" class="stat-value">$0.00</strong>
            </div>
          </div>

          <div id="boardStage" class="board-stage">
            <canvas id="plinkoCanvas" aria-label="Plinko board"></canvas>
            <div id="binsRow" class="bins-row" aria-label="Multiplier bins"></div>
            <div id="hitBanner" class="hit-banner" aria-live="polite"></div>
            <div id="oddsTooltip" class="odds-tooltip" aria-hidden="true">Chance: 0.00%</div>
          </div>
        </main>

        <footer class="control-bar">
          <label class="bet-group" for="betAmount">
            <span>Bet</span>
            <input id="betAmount" type="number" min="0.01" step="0.01" value="10.00" inputmode="decimal">
          </label>
          <button id="dropBtn" class="action-btn drop-btn" type="button">DROP</button>
          <button id="autoBtn" class="action-btn auto-btn" type="button">AUTO</button>
          <button id="moreBtn" class="icon-btn control-more" type="button" aria-label="Advanced settings">…</button>
        </footer>
      </div>

      <div id="sheetBackdrop" class="sheet-backdrop"></div>
      <section id="settingsSheet" class="settings-sheet" aria-label="Advanced settings">
        <header class="sheet-header">
          <h2>Advanced Settings</h2>
          <button id="sheetCloseBtn" class="icon-btn" type="button" aria-label="Close settings">✕</button>
        </header>

        <div class="sheet-body">
          <div class="sheet-field">
            <label for="rowCount">Rows</label>
            <select id="rowCount">
              <option value="8">8</option>
              <option value="10">10</option>
              <option value="12">12</option>
              <option value="14">14</option>
              <option value="16" selected>16</option>
            </select>
          </div>

          <div class="sheet-field">
            <label for="difficulty">Risk</label>
            <select id="difficulty">
              <option value="easy">Low</option>
              <option value="medium" selected>Medium</option>
              <option value="hard">High</option>
            </select>
          </div>

          <div class="sheet-field">
            <label for="autoSpeed">Auto Delay <span id="autoSpeedValue">400ms</span></label>
            <input id="autoSpeed" type="range" min="200" max="1200" step="100" value="400">
          </div>

          <div class="sheet-field">
            <label for="autoDrops">Auto Drops (0 = unlimited)</label>
            <input id="autoDrops" type="number" min="0" step="1" value="0" inputmode="numeric">
          </div>

          <div class="sheet-field">
            <label for="stopProfit">Stop on Profit ($)</label>
            <input id="stopProfit" type="number" min="0" step="0.01" value="0" inputmode="decimal">
          </div>

          <div class="sheet-field">
            <label for="stopLoss">Stop on Loss ($)</label>
            <input id="stopLoss" type="number" min="0" step="0.01" value="0" inputmode="decimal">
          </div>
        </div>
      </section>
    </div>
  `;

  const root = container.querySelector("#plinkoPhoneApp");
  const canvasEl = root?.querySelector("#plinkoCanvas");
  if (!root || !canvasEl) return;
  const context = canvasEl.getContext("2d");
  if (!context) return;

  const els = {
    root,
    boardStage: root.querySelector("#boardStage"),
    binsRow: root.querySelector("#binsRow"),
    hitBanner: root.querySelector("#hitBanner"),
    oddsTooltip: root.querySelector("#oddsTooltip"),
    balanceDisplay: root.querySelector("#balanceDisplay"),
    lastWinDisplay: root.querySelector("#lastWinDisplay"),
    betAmount: root.querySelector("#betAmount"),
    dropBtn: root.querySelector("#dropBtn"),
    autoBtn: root.querySelector("#autoBtn"),
    moreBtn: root.querySelector("#moreBtn"),
    headerMoreBtn: root.querySelector("#headerMoreBtn"),
    reloadBtn: root.querySelector("#reloadBtn"),
    closeBtn: root.querySelector("#closeBtn"),
    sheetBackdrop: root.querySelector("#sheetBackdrop"),
    sheetCloseBtn: root.querySelector("#sheetCloseBtn"),
    rowCount: root.querySelector("#rowCount"),
    difficulty: root.querySelector("#difficulty"),
    autoSpeed: root.querySelector("#autoSpeed"),
    autoSpeedValue: root.querySelector("#autoSpeedValue"),
    autoDrops: root.querySelector("#autoDrops"),
    stopProfit: root.querySelector("#stopProfit"),
    stopLoss: root.querySelector("#stopLoss")
  };

  const PEG_RADIUS_LOCAL = 3.8;
  const BALL_RADIUS_LOCAL = 5.8;
  const BALL_SPEED = 2.4;
  const HIT_BANNER_MS = 1300;
  const BIN_HIT_MS = 620;
  const MAX_BALLS = 40;

  const CUSTOM_WEIGHTS = {
    8: [0.39, 3.13, 10.94, 21.88, 27.34, 21.88, 10.94, 3.13, 0.39],
    10: [0.098, 0.977, 4.395, 11.72, 20.51, 24.61, 20.51, 11.72, 4.395, 0.977, 0.098],
    12: [0.024, 0.293, 1.613, 5.371, 12.09, 19.34, 22.37, 19.34, 12.09, 5.371, 1.613, 0.293, 0.024],
    14: [0.006, 0.086, 0.555, 2.22, 5.44, 9.69, 12.64, 13.04, 12.64, 9.69, 5.44, 2.22, 0.555, 0.086, 0.006],
    16: [0.0015, 0.0244, 0.183, 0.854, 2.78, 6.67, 12.22, 17.47, 19.64, 17.47, 12.22, 6.67, 2.78, 0.854, 0.183, 0.0244, 0.0015]
  };

  const PAYOUTS = {
    8: { easy: [5.6, 2.1, 1.1, 1, 0.5, 1, 1.1, 2.1, 5.6], medium: [13, 3, 1.3, 0.7, 0.4, 0.7, 1.3, 3, 13], hard: [29, 4, 1.5, 0.5, 0.2, 0.5, 1.5, 4, 29] },
    10: { easy: [8.9, 3, 1.5, 1.2, 1, 0.5, 1, 1.2, 1.5, 3, 8.9], medium: [22, 5, 2, 1.4, 0.6, 0.4, 0.6, 1.4, 2, 5, 22], hard: [76, 9, 3, 1, 0.4, 0.2, 0.4, 1, 3, 9, 76] },
    12: { easy: [10, 3.5, 1.6, 1.3, 1.1, 0.8, 0.5, 0.8, 1.1, 1.3, 1.6, 3.5, 10], medium: [33, 11, 4, 2, 1.1, 0.6, 0.3, 0.6, 1.1, 2, 4, 11, 33], hard: [170, 12, 5, 2, 0.5, 0.3, 0.2, 0.3, 0.5, 2, 5, 12, 170] },
    14: { easy: [7.1, 2.8, 1.4, 1.1, 0.9, 0.7, 0.6, 0.5, 0.6, 0.7, 0.9, 1.1, 1.4, 2.8, 7.1], medium: [58, 15, 7, 4, 1.9, 1, 0.5, 0.2, 0.5, 1, 1.9, 4, 7, 15, 58], hard: [420, 18, 7, 3, 1, 0.5, 0.3, 0.2, 0.3, 0.5, 1, 3, 7, 18, 420] },
    16: { easy: [8, 4, 2, 1.6, 1.4, 1.25, 1.1, 1, 0.5, 1, 1.1, 1.25, 1.4, 1.6, 2, 4, 8], medium: [110, 41, 10, 5, 3, 1.5, 1, 0.5, 0.2, 0.5, 1, 1.5, 3, 5, 10, 41, 110], hard: [1000, 130, 26, 9, 4, 2, 0.5, 0.3, 0.2, 0.3, 0.5, 2, 4, 9, 26, 130, 1000] }
  };

  const state = {
    rows: 16,
    difficulty: "medium",
    balance: roundCurrency(cash),
    lastWin: 0,
    lastBet: 10,
    multipliers: [],
    pegs: [],
    balls: [],
    geometry: null,
    hitTimer: null,
    resizeRaf: 0,
    frameRaf: 0,
    lastTs: 0,
    auto: {
      running: false,
      delay: 400,
      maxDrops: 0,
      stopProfit: 0,
      stopLoss: 0,
      drops: 0,
      nextDropAt: 0,
      startBalance: roundCurrency(cash)
    }
  };
  const binHitTimers = new WeakMap();

  const clampLocal = (value, min, max) => Math.max(min, Math.min(max, value));
  const SUPPORTED_ROWS = new Set([8, 10, 12, 14, 16]);
  const parsePositive = (value, fallback) => {
    const parsed = Number.parseFloat(value);
    return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
  };
  const formatMultiplier = (value) => Number(value).toFixed(2).replace(/\.00$/, "").replace(/(\.\d)0$/, "$1");

  function syncGlobalCash() {
    cash = roundCurrency(state.balance);
    updateUI();
    updateBlackjackCash();
  }

  function updateStats() {
    els.balanceDisplay.textContent = `$${state.balance.toFixed(2)}`;
    els.lastWinDisplay.textContent = `$${state.lastWin.toFixed(2)}`;
    els.lastWinDisplay.style.color = state.lastWin >= state.lastBet ? "var(--accent)" : "var(--danger)";
  }

  function setSheetOpen(open) {
    els.root.classList.toggle("sheet-open", Boolean(open));
  }

  function showHit(message) {
    if (state.hitTimer) clearTimeout(state.hitTimer);
    els.hitBanner.textContent = message;
    els.hitBanner.classList.add("show");
    state.hitTimer = setTimeout(() => {
      els.hitBanner.classList.remove("show");
      state.hitTimer = null;
    }, HIT_BANNER_MS);
  }

  function getCurrentMultipliers() {
    const activeRows = SUPPORTED_ROWS.has(state.rows) ? state.rows : 16;
    const rowSet = PAYOUTS[activeRows] || PAYOUTS[16];
    return rowSet[state.difficulty] || rowSet.medium;
  }

  function getActiveWeights(rowCount = state.rows) {
    const activeRows = SUPPORTED_ROWS.has(rowCount) ? rowCount : 16;
    return CUSTOM_WEIGHTS[activeRows];
  }

  function weightedPick(weights) {
    const total = weights.reduce((a, b) => a + b, 0);
    let r = Math.random() * total;
    let sum = 0;
    for (let i = 0; i < weights.length; i += 1) {
      sum += weights[i];
      if (r <= sum) return i;
    }
    return Math.floor(weights.length / 2);
  }

  function chooseResultIndex() {
    const weights = getActiveWeights(state.rows);
    return weightedPick(weights);
  }

  function getMultiplierColor(index, count) {
    const center = (Math.max(2, count) - 1) / 2;
    const distance = Math.abs(index - center) / Math.max(1, center);
    const t = clampLocal(distance, 0, 1);
    const from = [255, 225, 90];
    const to = [245, 7, 63];
    const r = Math.round(from[0] + (to[0] - from[0]) * t);
    const g = Math.round(from[1] + (to[1] - from[1]) * t);
    const b = Math.round(from[2] + (to[2] - from[2]) * t);
    return `rgb(${r}, ${g}, ${b})`;
  }

  function computeGeometry() {
    const rect = els.boardStage.getBoundingClientRect();
    if (!rect.width || !rect.height) return;

    const width = rect.width;
    const height = rect.height;
    const dpr = window.devicePixelRatio || 1;

    canvasEl.width = Math.floor(width * dpr);
    canvasEl.height = Math.floor(height * dpr);
    canvasEl.style.width = `${width}px`;
    canvasEl.style.height = `${height}px`;
    context.setTransform(dpr, 0, 0, dpr, 0, 0);

    const topPad = Math.max(22, height * 0.07);
    const bottomPad = Math.max(4, height * 0.015);
    const binHeight = Math.max(28, Math.min(38, height * 0.095));
    const laneHeight = Math.max(120, height - topPad - bottomPad - binHeight);
    const sidePad = Math.max(6, width * 0.03);
    const baseWidth = Math.max(170, Math.min(width - sidePad * 2, laneHeight * 1.62));
    const slotWidth = baseWidth / (state.rows + 1);
    const rowSpacing = laneHeight / state.rows;
    const baseLeft = (width - baseWidth) / 2;
    const pegTop = topPad;
    const binTop = pegTop + state.rows * rowSpacing;

    state.geometry = {
      width,
      height,
      centerX: width / 2,
      baseLeft,
      baseWidth,
      slotWidth,
      rowSpacing,
      pegTop,
      binTop,
      binHeight,
      spawnY: Math.max(8, pegTop - rowSpacing * 0.92),
      finishY: binTop + binHeight * 0.4
    };
  }

  function buildPegs() {
    if (!state.geometry) return;
    const pegs = [];
    for (let row = 0; row < state.rows; row += 1) {
      const count = row + 1;
      const y = state.geometry.pegTop + row * state.geometry.rowSpacing;
      const startX = state.geometry.centerX - (row * state.geometry.slotWidth) / 2;
      for (let col = 0; col < count; col += 1) {
        pegs.push({ x: startX + col * state.geometry.slotWidth, y });
      }
    }
    state.pegs = pegs;
  }

  function renderBins() {
    if (!state.geometry) return;
    const binsCount = state.rows + 1;
    const weights = getActiveWeights(state.rows);
    const totalWeight = weights.reduce((sum, value) => sum + value, 0);
    els.binsRow.innerHTML = "";
    els.binsRow.style.left = `${state.geometry.baseLeft}px`;
    els.binsRow.style.top = `${state.geometry.binTop}px`;
    els.binsRow.style.width = `${state.geometry.baseWidth}px`;
    els.binsRow.style.height = `${state.geometry.binHeight}px`;
    els.binsRow.style.gridTemplateColumns = `repeat(${binsCount}, 1fr)`;

    for (let i = 0; i < binsCount; i += 1) {
      const bin = document.createElement("div");
      const value = state.multipliers[i] ?? 0;
      bin.className = "bin";
      bin.textContent = `${formatMultiplier(value)}x`;
      bin.style.background = getMultiplierColor(i, binsCount);
      const chance = totalWeight > 0 ? (weights[i] / totalWeight) * 100 : 0;
      bin.dataset.chance = String(chance);
      bin.dataset.multiplier = String(value);
      if (state.rows >= 14) bin.style.fontSize = "0.58rem";
      els.binsRow.appendChild(bin);
    }
  }

  function formatChance(value) {
    return Number(value).toFixed(4).replace(/\.?0+$/, "");
  }

  function showOddsTooltip(bin, pointerEvent) {
    if (!els.oddsTooltip || !els.boardStage || !bin) return;
    const chance = Number(bin.dataset.chance || 0);
    const multiplier = Number(bin.dataset.multiplier || 0);
    const betAmount = Math.max(0, parsePositive(els.betAmount?.value, state.lastBet || 0));
    const payout = roundCurrency(betAmount * multiplier);
    const boardRect = els.boardStage.getBoundingClientRect();
    const tooltip = els.oddsTooltip;
    tooltip.textContent = `Chance: ${formatChance(chance)}% • Land here → Win $${payout.toFixed(2)}`;
    const clientX = pointerEvent?.clientX ?? (bin.getBoundingClientRect().left + bin.getBoundingClientRect().width / 2);
    const relativeX = clientX - boardRect.left;
    const tooltipWidth = tooltip.offsetWidth || 96;
    const clampedX = clampLocal(relativeX, tooltipWidth / 2 + 8, boardRect.width - tooltipWidth / 2 - 8);
    tooltip.style.left = `${clampedX}px`;
    tooltip.classList.add("show");
  }

  function hideOddsTooltip() {
    if (!els.oddsTooltip) return;
    els.oddsTooltip.classList.remove("show");
  }

  function buildPath(slot, directions) {
    const g = state.geometry;
    const path = [];
    let x = g.centerX;
    const xMin = g.baseLeft + g.slotWidth * 0.5;
    const xMax = g.baseLeft + g.baseWidth - g.slotWidth * 0.5;
    path.push({ x, y: g.spawnY });

    for (let i = 0; i < state.rows; i += 1) {
      const direction = directions[i] || (Math.random() < 0.5 ? -1 : 1);
      const jitter = (Math.random() - 0.5) * g.slotWidth * 0.1;
      x = clampLocal(x + direction * (g.slotWidth / 2) + jitter, xMin, xMax);
      path.push({ x, y: g.pegTop + i * g.rowSpacing });
    }

    path.push({
      x: g.baseLeft + (slot + 0.5) * g.slotWidth,
      y: g.finishY
    });
    return path;
  }

  function reflowBalls() {
    if (!state.geometry || state.balls.length === 0) return;
    for (const ball of state.balls) {
      const traveled = ball.nodeIdx + ball.progress;
      ball.path = buildPath(ball.slot, ball.directions);
      const maxTravel = ball.path.length - 1.001;
      const nextTravel = clampLocal(traveled, 0, maxTravel);
      ball.nodeIdx = Math.floor(nextTravel);
      ball.progress = nextTravel - ball.nodeIdx;
    }
  }

  function updateControls() {
    const busy = state.balls.length > 0;
    els.dropBtn.disabled = state.auto.running || state.balls.length >= MAX_BALLS;
    els.betAmount.disabled = state.auto.running;
    els.rowCount.disabled = busy || state.auto.running;
    els.difficulty.disabled = busy || state.auto.running;
    els.autoBtn.classList.toggle("is-running", state.auto.running);
    els.autoBtn.textContent = state.auto.running ? "STOP" : "AUTO";
  }

  function triggerDrop(mode) {
    if (!state.geometry) return false;
    if (state.balls.length >= MAX_BALLS) return false;

    const bet = parsePositive(els.betAmount.value, 0);
    if (!bet) {
      if (state.auto.running) stopAuto();
      if (mode === "manual") showHit("Enter a valid bet");
      return false;
    }
    if (state.balance < bet) {
      if (state.auto.running) stopAuto();
      if (mode === "manual") showHit("Insufficient balance");
      return false;
    }

    state.lastBet = bet;
    state.balance = roundCurrency(state.balance - bet);
    syncGlobalCash();
    updateStats();

    const slot = chooseResultIndex();
    const rights = slot;
    const lefts = state.rows - slot;
    const directions = [];
    for (let i = 0; i < rights; i += 1) directions.push(1);
    for (let i = 0; i < lefts; i += 1) directions.push(-1);
    directions.sort(() => Math.random() - 0.5);

    const path = buildPath(slot, directions);
    state.balls.push({
      slot,
      bet,
      directions,
      path,
      nodeIdx: 0,
      progress: 0
    });
    updateControls();
    return true;
  }

  function resolveBall(ball) {
    const multiplier = state.multipliers[ball.slot] || 0;
    const win = roundCurrency(ball.bet * multiplier);
    state.balance = roundCurrency(state.balance + win);
    state.lastWin = win;
    syncGlobalCash();
    updateStats();

    const bin = els.binsRow.children[ball.slot];
    if (bin) {
      const existing = binHitTimers.get(bin);
      if (existing) clearTimeout(existing);
      bin.classList.remove("is-hit");
      void bin.offsetWidth;
      bin.classList.add("is-hit");
      const timer = setTimeout(() => {
        bin.classList.remove("is-hit");
        binHitTimers.delete(bin);
      }, BIN_HIT_MS);
      binHitTimers.set(bin, timer);
    }

    showHit(`HIT ${formatMultiplier(multiplier)}x • +$${win.toFixed(2)}`);
  }

  function updateBalls(dt) {
    if (state.balls.length === 0) return;
    const kept = [];
    for (const ball of state.balls) {
      ball.progress += dt * BALL_SPEED;
      while (ball.progress >= 1) {
        ball.progress -= 1;
        ball.nodeIdx += 1;
        if (ball.nodeIdx >= ball.path.length - 1) {
          resolveBall(ball);
          ball.nodeIdx = ball.path.length - 1;
          break;
        }
      }
      if (ball.nodeIdx >= ball.path.length - 1) continue;
      kept.push(ball);
    }
    state.balls = kept;
    updateControls();
  }

  function drawBoard() {
    if (!state.geometry) return;
    context.clearRect(0, 0, state.geometry.width, state.geometry.height);
    context.fillStyle = "rgba(227, 239, 255, 0.92)";
    state.pegs.forEach((peg) => {
      context.beginPath();
      context.arc(peg.x, peg.y, PEG_RADIUS_LOCAL, 0, Math.PI * 2);
      context.fill();
    });

    state.balls.forEach((ball) => {
      const p1 = ball.path[ball.nodeIdx];
      const p2 = ball.path[Math.min(ball.path.length - 1, ball.nodeIdx + 1)];
      const t = ball.progress;
      const arcHeight = -(14 + state.geometry.rowSpacing * 0.42);
      const sway = Math.sin(t * Math.PI * 2) * 1.4;
      const x = p1.x + (p2.x - p1.x) * t + sway;
      const y = p1.y + (p2.y - p1.y) * t + Math.sin(t * Math.PI) * arcHeight;

      context.beginPath();
      context.arc(x, y, BALL_RADIUS_LOCAL, 0, Math.PI * 2);
      context.fillStyle = "#f5073f";
      context.shadowBlur = 12;
      context.shadowColor = "#f5073f";
      context.fill();
      context.shadowBlur = 0;
    });
  }

  function resetRound() {
    stopAuto();
    state.balance = roundCurrency(cash);
    state.lastWin = 0;
    state.lastBet = parsePositive(els.betAmount.value, 10) || 10;
    state.balls = [];
    syncGlobalCash();
    updateStats();
    updateControls();
    showHit("Board reset");
  }

  function readAutoSettings() {
    const baseDelay = Math.round(clampLocal(parsePositive(els.autoSpeed.value, 400), 200, 2000));
    state.auto.delay = baseDelay;
    state.auto.maxDrops = Math.floor(Math.max(0, parsePositive(els.autoDrops.value, 0)));
    state.auto.stopProfit = parsePositive(els.stopProfit.value, 0);
    state.auto.stopLoss = parsePositive(els.stopLoss.value, 0);
  }

  function shouldStopAuto() {
    if (state.auto.maxDrops > 0 && state.auto.drops >= state.auto.maxDrops) return true;
    const net = state.balance - state.auto.startBalance;
    if (state.auto.stopProfit > 0 && net >= state.auto.stopProfit) return true;
    if (state.auto.stopLoss > 0 && -net >= state.auto.stopLoss) return true;
    return false;
  }

  function startAuto() {
    readAutoSettings();
    state.auto.running = true;
    state.auto.drops = 0;
    state.auto.startBalance = state.balance;
    state.auto.nextDropAt = 0;
    updateControls();
  }

  function stopAuto() {
    state.auto.running = false;
    updateControls();
  }

  function updateAuto(ts) {
    if (!state.auto.running) return;
    if (state.balls.length >= MAX_BALLS) return;
    if (shouldStopAuto()) {
      stopAuto();
      return;
    }
    if (ts < state.auto.nextDropAt) return;
    const dropped = triggerDrop("auto");
    if (!dropped) {
      stopAuto();
      return;
    }
    state.auto.drops += 1;
    state.auto.nextDropAt = ts + state.auto.delay;
  }

  function fullInitBoard() {
    state.multipliers = getCurrentMultipliers();
    computeGeometry();
    buildPegs();
    reflowBalls();
    renderBins();
    drawBoard();
  }

  function closeMiniIfEmbedded() {
    if (IS_PHONE_EMBED_MODE) {
      try {
        window.parent.postMessage({ type: "phone-mini-close" }, "*");
      } catch (error) {}
      return;
    }
    document.getElementById("backToTrading")?.click();
  }

  const cleanupFns = [];
  const bind = (node, type, handler, options) => {
    if (!node) return;
    node.addEventListener(type, handler, options);
    cleanupFns.push(() => node.removeEventListener(type, handler, options));
  };

  bind(els.dropBtn, "click", () => triggerDrop("manual"));
  bind(els.autoBtn, "click", () => {
    if (state.auto.running) stopAuto();
    else startAuto();
  });
  bind(els.autoSpeed, "input", () => {
    const speed = Math.round(parsePositive(els.autoSpeed.value, 400));
    els.autoSpeedValue.textContent = `${speed}ms`;
    if (state.auto.running) readAutoSettings();
  });
  bind(els.rowCount, "change", (event) => {
    const nextRows = Number.parseInt(event.target.value, 10);
    if (!Number.isFinite(nextRows) || !SUPPORTED_ROWS.has(nextRows)) return;
    stopAuto();
    state.rows = nextRows;
    fullInitBoard();
    updateControls();
  });
  bind(els.difficulty, "change", (event) => {
    stopAuto();
    state.difficulty = String(event.target.value || "medium");
    fullInitBoard();
    updateControls();
  });
  bind(els.moreBtn, "click", () => setSheetOpen(true));
  bind(els.headerMoreBtn, "click", () => setSheetOpen(true));
  bind(els.sheetCloseBtn, "click", () => setSheetOpen(false));
  bind(els.sheetBackdrop, "click", () => setSheetOpen(false));
  bind(els.closeBtn, "click", closeMiniIfEmbedded);
  bind(els.reloadBtn, "click", () => {
    resetRound();
    fullInitBoard();
  });
  bind(els.binsRow, "pointermove", (event) => {
    const bin = event.target.closest(".bin");
    if (!bin || !els.binsRow.contains(bin)) {
      hideOddsTooltip();
      return;
    }
    showOddsTooltip(bin, event);
  });
  bind(els.binsRow, "pointerleave", hideOddsTooltip);

  const onResize = () => {
    if (state.resizeRaf) cancelAnimationFrame(state.resizeRaf);
    state.resizeRaf = requestAnimationFrame(() => {
      fullInitBoard();
    });
  };
  bind(window, "resize", onResize);
  bind(window, "orientationchange", onResize);

  const loop = (ts) => {
    if (!state.lastTs) state.lastTs = ts;
    const dt = Math.min(0.05, (ts - state.lastTs) / 1000);
    state.lastTs = ts;
    updateAuto(ts);
    updateBalls(dt);
    drawBoard();
    state.frameRaf = requestAnimationFrame(loop);
  };

  els.rowCount.value = String(state.rows);
  els.difficulty.value = state.difficulty;
  els.autoSpeedValue.textContent = `${Math.round(parsePositive(els.autoSpeed.value, 400))}ms`;
  updateStats();
  fullInitBoard();
  updateControls();
  state.frameRaf = requestAnimationFrame(loop);

  activeCasinoCleanup = () => {
    stopAuto();
    if (state.frameRaf) cancelAnimationFrame(state.frameRaf);
    if (state.resizeRaf) cancelAnimationFrame(state.resizeRaf);
    if (state.hitTimer) clearTimeout(state.hitTimer);
    cleanupFns.forEach((fn) => fn());
    container.classList.remove("casino-fullbleed", "plinko-phone-fullbleed");
  };
}

// =====================
// ====== PLINKO =======
// =====================
let plinko = {
  initialized: false,
  layoutMode: "default",
  root: null,
  canvas: null,
  ctx: null,
  pegs: [],
  balls: [],
  rows: 16,
  slotWidth: 32,
  boardWidth: 520,
  boardMaxWidth: 520,
  maxSpacingY: 30,
  spacingY: 30,
  difficulty: "medium",
  finishY: 0,
  MULTIPLIERS: [],
  autoInterval: null,
  animationFrame: null,
  lastFrameMs: 0,
  resizeHandler: null,
  hitPopupTimer: null
};
const plinkoMultiplierHitTimers = new WeakMap();
const plinkoMultiplierHitTransitions = new WeakMap();

const PEG_RADIUS = 4;
const BALL_RADIUS = 7;
const PLINKO_BOARD_WIDTH = 520;
const SPACING_Y = 30;
const PADDING_TOP = 48;
const PADDING_BOTTOM = 52;
const PLINKO_DEBUG_ALIGNMENT = false;

const CUSTOM_WEIGHTS = {
  8: [0.39, 3.13, 10.94, 21.88, 27.34, 21.88, 10.94, 3.13, 0.39],
  10: [0.098, 0.977, 4.395, 11.72, 20.51, 24.61, 20.51, 11.72, 4.395, 0.977, 0.098],
  12: [0.024, 0.293, 1.613, 5.371, 12.09, 19.34, 22.37, 19.34, 12.09, 5.371, 1.613, 0.293, 0.024],
  14: [0.006, 0.086, 0.555, 2.22, 5.44, 9.69, 12.64, 13.04, 12.64, 9.69, 5.44, 2.22, 0.555, 0.086, 0.006],
  16: [
    0.0015, 0.0244, 0.183, 0.854, 2.78, 6.67, 12.22, 17.47, 19.64, 17.47, 12.22, 6.67, 2.78,
    0.854, 0.183, 0.0244, 0.0015
  ]
};

const MULTIPLIER_DATA = {
  8: {
    easy: [5.6, 2.1, 1.1, 1, 0.5, 1, 1.1, 2.1, 5.6],
    medium: [13, 3, 1.3, 0.7, 0.4, 0.7, 1.3, 3, 13],
    hard: [29, 4, 1.5, 0.5, 0.2, 0.5, 1.5, 4, 29]
  },
  10: {
    easy: [8.9, 3, 1.5, 1.2, 1, 0.5, 1, 1.2, 1.5, 3, 8.9],
    medium: [22, 5, 2, 1.4, 0.6, 0.4, 0.6, 1.4, 2, 5, 22],
    hard: [76, 9, 3, 1, 0.4, 0.2, 0.4, 1, 3, 9, 76]
  },
  12: {
    easy: [10, 3.5, 1.6, 1.3, 1.1, 0.8, 0.5, 0.8, 1.1, 1.3, 1.6, 3.5, 10],
    medium: [33, 11, 4, 2, 1.1, 0.6, 0.3, 0.6, 1.1, 2, 4, 11, 33],
    hard: [170, 12, 5, 2, 0.5, 0.3, 0.2, 0.3, 0.5, 2, 5, 12, 170]
  },
  14: {
    easy: [7.1, 2.8, 1.4, 1.1, 0.9, 0.7, 0.6, 0.5, 0.6, 0.7, 0.9, 1.1, 1.4, 2.8, 7.1],
    medium: [58, 15, 7, 4, 1.9, 1, 0.5, 0.2, 0.5, 1, 1.9, 4, 7, 15, 58],
    hard: [420, 18, 7, 3, 1, 0.5, 0.3, 0.2, 0.3, 0.5, 1, 3, 7, 18, 420]
  },
  16: {
    easy: [8, 4, 2, 1.6, 1.4, 1.25, 1.1, 1, 0.5, 1, 1.1, 1.25, 1.4, 1.6, 2, 4, 8],
    medium: [110, 41, 10, 5, 3, 1.5, 1, 0.5, 0.2, 0.5, 1, 1.5, 3, 5, 10, 41, 110],
    hard: [1000, 130, 26, 9, 4, 2, 0.5, 0.3, 0.2, 0.3, 0.5, 2, 4, 9, 26, 130, 1000]
  }
};

function factorial(n) {
  let r = 1;
  for (let i = 2; i <= n; i++) r *= i;
  return r;
}
function binomial(n, k) {
  return factorial(n) / (factorial(k) * factorial(n - k));
}

function getResultBasedOnWeights() {
  let weights = [];
  if (CUSTOM_WEIGHTS[plinko.rows]) weights = CUSTOM_WEIGHTS[plinko.rows];
  else for (let i = 0; i <= plinko.rows; i++) weights.push(binomial(plinko.rows, i));

  const totalWeight = weights.reduce((a, b) => a + b, 0);
  let r = Math.random() * totalWeight;
  let sum = 0;
  for (let i = 0; i < weights.length; i++) {
    sum += weights[i];
    if (r <= sum) return i;
  }
  return Math.floor(plinko.rows / 2);
}

function getPlinkoRoot() {
  if (plinko.root instanceof HTMLElement && document.body.contains(plinko.root)) {
    return plinko.root;
  }
  if (plinko.canvas) {
    const foundFromCanvas = plinko.canvas.closest(".plinko-root");
    if (foundFromCanvas instanceof HTMLElement) {
      plinko.root = foundFromCanvas;
      return foundFromCanvas;
    }
  }
  const selector =
    plinko.layoutMode === "mini"
      ? "#plinko-mini-screen"
      : plinko.layoutMode === "full"
        ? "#plinko-casino-screen"
        : plinko.layoutMode === "poker"
          ? "#poker-plinko-screen"
          : ".plinko-root";
  const found = document.querySelector(selector) || document.querySelector(".plinko-root");
  plinko.root = found instanceof HTMLElement ? found : null;
  return plinko.root;
}

function getPlinkoScopedElement(id) {
  const root = getPlinkoRoot();
  if (root instanceof HTMLElement) {
    const scoped = root.querySelector(`#${id}`);
    if (scoped) return scoped;
  }
  return document.getElementById(id);
}

function resizePlinkoMini(rootEl) {
  const wrapper = rootEl?.querySelector(".canvas-wrapper");
  const gameContainer = rootEl?.querySelector(".game-container");
  const controls = rootEl?.querySelector(".controls");
  const stats = rootEl?.querySelector(".stats");
  const availableWidth = Math.floor(wrapper?.clientWidth || gameContainer?.clientWidth || rootEl?.clientWidth || PLINKO_BOARD_WIDTH);
  const maxBoardWidth = Math.max(220, Number(plinko.boardMaxWidth) || 520);
  const width = Math.max(220, Math.min(maxBoardWidth, availableWidth || maxBoardWidth));
  const slotCount = plinko.rows + 1;
  const slotWidth = Math.max(18, Math.min(45, (width - 40) / slotCount));

  const gameGap = parseFloat(getComputedStyle(gameContainer || rootEl).gap || "0") || 0;
  const availableHeightRaw =
    (gameContainer?.clientHeight || rootEl?.clientHeight || 0) -
    (controls?.offsetHeight || 0) -
    (stats?.offsetHeight || 0) -
    gameGap * 2 -
    12;
  const availableHeight = Math.max(220, availableHeightRaw || 0);
  const byHeight = Math.floor((availableHeight - PADDING_TOP - PADDING_BOTTOM) / Math.max(1, plinko.rows));
  const spacingY = Math.max(16, Math.min(30, byHeight > 0 ? byHeight : 30, Math.round(slotWidth * 0.9)));
  const height = Math.max(220, PADDING_TOP + plinko.rows * spacingY + PADDING_BOTTOM);

  return { width, height, slotWidth, spacingY, wrapper };
}

function resizePlinkoCasino(rootEl) {
  const wrapper = rootEl?.querySelector(".canvas-wrapper");
  const gameContainer = rootEl?.querySelector(".game-container");
  const controls = rootEl?.querySelector(".controls");
  const stats = rootEl?.querySelector(".stats");
  const isFullLayout = plinko.layoutMode === "full";
  const isPokerLayout = plinko.layoutMode === "poker";
  const maxBoardWidth = Math.max(320, Number(plinko.boardMaxWidth) || PLINKO_BOARD_WIDTH);
  const fallbackHeight = Math.max(280, Math.floor(window.innerHeight - (document.body.classList.contains("game-fullscreen") ? 96 : 180)));
  const gameGap = parseFloat(getComputedStyle(gameContainer || rootEl).gap || "0") || 0;
  const availableWidth = Math.floor(wrapper?.clientWidth || gameContainer?.clientWidth || rootEl?.clientWidth || maxBoardWidth);
  const availableHeight = Math.floor(
    wrapper?.clientHeight ||
      (gameContainer?.clientHeight || rootEl?.clientHeight || fallbackHeight) -
        (controls?.offsetHeight || 0) -
        (stats?.offsetHeight || 0) -
        gameGap * 2 -
        14
  );
  const boundedWidth = Math.max(320, Math.min(maxBoardWidth, availableWidth || maxBoardWidth));
  const boundedHeight = Math.max(240, availableHeight || fallbackHeight);

  const naturalWidth = PLINKO_BOARD_WIDTH;
  const naturalSlotWidth = (PLINKO_BOARD_WIDTH - 40) / Math.max(1, plinko.rows + 1);
  const naturalSpacingY = SPACING_Y;
  const naturalHeight = PADDING_TOP + plinko.rows * naturalSpacingY + PADDING_BOTTOM;

  // Keep one uniform board scale for X/Y so peg geometry never stretches.
  const rawScale = Math.min(boundedWidth / naturalWidth, boundedHeight / naturalHeight);
  const minScale = isPokerLayout ? 0.72 : isFullLayout ? 0.8 : 0.74;
  const scale = Math.max(minScale, Math.min(2.2, rawScale || 1));

  const width = Math.round(naturalWidth * scale);
  const slotWidth = Math.max(16, Math.round(naturalSlotWidth * scale));
  const spacingY = Math.max(14, Math.round(naturalSpacingY * scale));
  const height = Math.max(240, Math.round(PADDING_TOP + plinko.rows * spacingY + PADDING_BOTTOM));

  return { width, height, slotWidth, spacingY, wrapper };
}

function initBoard() {
  if (!plinko.canvas) return;
  const rootEl = getPlinkoRoot();
  if (!rootEl) return;
  const isMiniLayout = plinko.layoutMode === "mini";
  const sizing = isMiniLayout ? resizePlinkoMini(rootEl) : resizePlinkoCasino(rootEl);
  const WIDTH = sizing.width;
  const calculatedHeight = sizing.height;
  const wrapper = sizing.wrapper || plinko.canvas.parentElement;
  plinko.slotWidth = sizing.slotWidth;
  plinko.spacingY = sizing.spacingY;
  plinko.boardWidth = WIDTH;
  plinko.pegs = [];

  plinko.canvas.width = WIDTH;
  plinko.canvas.height = calculatedHeight;
  plinko.canvas.style.width = `${WIDTH}px`;
  plinko.canvas.style.height = `${calculatedHeight}px`;

  if (wrapper) {
    wrapper.style.removeProperty("width");
    wrapper.style.removeProperty("height");
    wrapper.style.removeProperty("max-width");
    wrapper.style.removeProperty("min-height");
    wrapper.style.removeProperty("flex");
    wrapper.style.removeProperty("margin");
  }

  plinko.finishY = calculatedHeight - 30;
  let rowData = MULTIPLIER_DATA[plinko.rows];
  if (!rowData) rowData = MULTIPLIER_DATA[16];
  plinko.MULTIPLIERS = rowData[plinko.difficulty] || rowData.medium;

  for (let r = 0; r < plinko.rows; r++) {
    const count = r + 1;
    const rowHalfSpan = ((count - 1) * plinko.slotWidth) / 2;
    const rowCenterX = WIDTH / 2;
    for (let c = 0; c < count; c++) {
      const x = rowCenterX - rowHalfSpan + c * plinko.slotWidth;
      plinko.pegs.push({ x, y: PADDING_TOP + r * plinko.spacingY });
    }
  }
  renderMultipliers();
  renderPlinkoScene(0, false);
}

function getColor(v) {
  if (v < 1) return "#ff003c";
  if (v < 2) return "#d9ed92";
  if (v < 10) return "#00e701";
  return "#f3ba2f";
}

function getPlinkoBottomGeometry() {
  const boardWidth = Number(plinko.boardWidth) || PLINKO_BOARD_WIDTH;
  const bins = Array.isArray(plinko.MULTIPLIERS) && plinko.MULTIPLIERS.length > 0 ? plinko.MULTIPLIERS.length : plinko.rows + 1;
  // Current peg builder creates rows 1..N, so the bottom row has N pegs (not N+1).
  const bottomPegCount = Math.max(1, plinko.rows);

  if (!Array.isArray(plinko.pegs) || plinko.pegs.length < bottomPegCount || bottomPegCount <= 0) {
    const dxFallback = Number(plinko.slotWidth) || (boardWidth - 40) / Math.max(1, bins);
    const leftBoundaryFallback = (boardWidth - dxFallback * bins) * 0.5;
    return {
      bins,
      dx: dxFallback,
      leftBoundary: leftBoundaryFallback,
      width: dxFallback * bins,
      leftPegX: leftBoundaryFallback + dxFallback * 0.5,
      rightPegX: leftBoundaryFallback + dxFallback * (bins - 0.5)
    };
  }

  const bottomRow = plinko.pegs.slice(-bottomPegCount);
  const leftPegX = bottomRow[0].x;
  const rightPegX = bottomRow[bottomRow.length - 1].x;
  const span = rightPegX - leftPegX;
  const dx = bottomRow.length > 1 ? span / (bottomRow.length - 1) : Number(plinko.slotWidth) || 1;
  // Slot centers are one half-step outside bottom pegs on both sides in this board geometry.
  const leftBoundary = leftPegX - dx;
  const width = dx * bins;

  return {
    bins,
    dx,
    leftBoundary,
    width,
    leftPegX,
    rightPegX
  };
}

function renderMultipliers() {
  const container = getPlinkoScopedElement("multipliers");
  const tooltip = getPlinkoScopedElement("oddsTooltip");
  if (!container) return;
  container.innerHTML = "";
  const geometry = getPlinkoBottomGeometry();
  const boardWidth = Number(plinko.boardWidth) || PLINKO_BOARD_WIDTH;
  const wrapper = container.parentElement;
  const canvasEl = plinko.canvas;

  let canvasOffsetX = 0;
  let scaleX = 1;
  if (wrapper instanceof HTMLElement && canvasEl instanceof HTMLCanvasElement) {
    const wrapperRect = wrapper.getBoundingClientRect();
    const canvasRect = canvasEl.getBoundingClientRect();
    if (wrapperRect.width > 0 && canvasRect.width > 0) {
      canvasOffsetX = canvasRect.left - wrapperRect.left;
      scaleX = canvasRect.width / boardWidth;
    }
  }

  const canvasWidth = canvasEl?.getBoundingClientRect().width || Math.max(1, boardWidth * scaleX);
  const scaledLeft = Math.max(0, Math.min(canvasWidth, geometry.leftBoundary * scaleX));
  const scaledWidth = Math.max(1, Math.min(canvasWidth, geometry.width * scaleX));
  const scaledRight = Math.max(0, canvasWidth - (scaledLeft + scaledWidth));

  container.style.width = `${Math.max(1, canvasWidth)}px`;
  container.style.left = `${canvasOffsetX}px`;
  container.style.paddingLeft = `${scaledLeft}px`;
  container.style.paddingRight = `${scaledRight}px`;
  container.style.boxSizing = "border-box";
  container.style.transform = "none";
  container.style.display = "grid";
  container.style.gridTemplateColumns = `repeat(${geometry.bins}, minmax(0, 1fr))`;
  container.style.setProperty("--plinko-bins", String(geometry.bins));

  let weights = CUSTOM_WEIGHTS[plinko.rows] || [];
  if (weights.length === 0) for (let i = 0; i <= plinko.rows; i++) weights.push(binomial(plinko.rows, i));
  const totalW = weights.reduce((a, b) => a + b, 0);

  plinko.MULTIPLIERS.forEach((m, i) => {
    const div = document.createElement("div");
    div.className = "multiplier";
    div.textContent = m + "x";
    div.style.background = getColor(m);
    div.style.position = "relative";
    div.style.left = "";
    div.style.top = "";
    div.style.width = "100%";
    div.style.fontSize = plinko.rows > 12 ? "0.5rem" : "0.7rem";

    const p = weights[i] || 0;
    const pct = ((p / totalW) * 100).toFixed(4);

    div.onmouseenter = () => {
      if (!tooltip) return;
      const betAmount = Math.max(
        0,
        Number.parseFloat(getPlinkoScopedElement("betAmount")?.value || String(plinko.lastBet || 0)) || 0
      );
      const payout = roundCurrency(betAmount * Number(m || 0));
      tooltip.textContent = `Chance: ${pct}% • Land here → Win $${payout.toFixed(2)}`;
      tooltip.style.opacity = 1;
    };
    div.onmouseleave = () => {
      if (tooltip) tooltip.style.opacity = 0;
    };

    container.appendChild(div);
  });
}

class Ball {
  constructor(targetSlot, bet) {
    this.slot = targetSlot;
    this.bet = bet;
    this.done = false;

    const rights = targetSlot;
    const lefts = plinko.rows - targetSlot;
    const directions = [];
    for (let i = 0; i < rights; i++) directions.push(1);
    for (let i = 0; i < lefts; i++) directions.push(-1);
    directions.sort(() => Math.random() - 0.5);

    this.path = [];
    const boardWidth = plinko.boardWidth || PLINKO_BOARD_WIDTH;
    let currentX = boardWidth / 2;
    let currentY = 20;
    this.path.push({ x: currentX, y: currentY });

    for (let i = 0; i < plinko.rows; i++) {
      currentY = PADDING_TOP + i * (plinko.spacingY || SPACING_Y);
      const jitter = (Math.random() - 0.5) * 10;
      currentX += directions[i] * (plinko.slotWidth / 2) + jitter;
      this.path.push({ x: currentX, y: currentY });
    }

    const geometry = getPlinkoBottomGeometry();
    const finalX = geometry.leftBoundary + this.slot * geometry.dx + geometry.dx * 0.5;
    const finalY = PADDING_TOP + plinko.rows * (plinko.spacingY || SPACING_Y) + 20;
    this.path.push({ x: finalX, y: finalY });

    this.nodeIdx = 0;
    this.progress = 0;
    this.speed = 2.4;
    this.x = this.path[0].x;
    this.y = this.path[0].y;
  }

  update(deltaSeconds = 1 / 60) {
    if (this.done) return;
    this.progress += this.speed * deltaSeconds;

    if (this.progress >= 1) {
      this.progress = 0;
      this.nodeIdx++;
    }

    if (this.nodeIdx >= this.path.length - 1) {
      this.done = true;
      this.x = this.path[this.path.length - 1].x;
      this.y = this.path[this.path.length - 1].y;
      resolveWin(this.slot, this.bet);
      return;
    }

    const p1 = this.path[this.nodeIdx];
    const p2 = this.path[this.nodeIdx + 1];

    this.x = p1.x + (p2.x - p1.x) * this.progress;
    const linearY = p1.y + (p2.y - p1.y) * this.progress;
    const bounceHeight = -22;
    const arc = Math.sin(this.progress * Math.PI) * bounceHeight;
    this.y = linearY + arc;
  }

  draw() {
    plinko.ctx.beginPath();
    plinko.ctx.arc(this.x, this.y, BALL_RADIUS, 0, Math.PI * 2);
    plinko.ctx.fillStyle = "#ff5100";
    plinko.ctx.shadowBlur = 8;
    plinko.ctx.shadowColor = "#ff5100";
    plinko.ctx.fill();
    plinko.ctx.shadowBlur = 0;
  }
}

function resolveWin(slot, bet) {
  const multiplier = plinko.MULTIPLIERS[slot];
  if (!multiplier) return;
  playGameSound("plinko_pop");

  const win = bet * multiplier;
  const profit = roundCurrency(win - bet);
  cash += win;
  if (profit > 0.009) {
    maybeAutoSaveFromCasinoWin({ amount: profit, multiplier, source: "plinko" });
  }
  document.getElementById("balanceDisplay").textContent = cash.toFixed(2);
  updateUI();
  updateBlackjackCash();

  const lastWinEl = getPlinkoScopedElement("lastWinDisplay");
  lastWinEl.textContent = win.toFixed(2);
  lastWinEl.style.color = win >= bet ? "#00e701" : "#ff003c";
  showPlinkoHitPopup(multiplier, win);

  const slotsDOM =
    document.querySelectorAll(".plinko-root .multiplier-container .multiplier").length > 0
      ? document.querySelectorAll(".plinko-root .multiplier-container .multiplier")
      : document.querySelectorAll(".multiplier");
  const hitSlot = slotsDOM[slot];
  if (hitSlot) {
    const existingTimer = plinkoMultiplierHitTimers.get(hitSlot);
    if (existingTimer) clearTimeout(existingTimer);
    const existingTransitionTimers = plinkoMultiplierHitTransitions.get(hitSlot);
    if (existingTransitionTimers && Array.isArray(existingTransitionTimers)) {
      existingTransitionTimers.forEach((timerId) => clearTimeout(timerId));
      plinkoMultiplierHitTransitions.delete(hitSlot);
    }

    hitSlot.classList.remove("active");
    hitSlot.style.transition = "none";
    hitSlot.style.transform = "translateY(0) scale(1)";
    hitSlot.style.filter = "";
    void hitSlot.offsetWidth;
    hitSlot.classList.add("active");

    requestAnimationFrame(() => {
      hitSlot.style.transition =
        "transform 120ms cubic-bezier(0.2, 0.9, 0.25, 1.08), filter 120ms ease-out";
      hitSlot.style.transform = "translateY(11px) scale(1.48)";
      hitSlot.style.filter = "brightness(1.9) saturate(1.2)";
    });

    const t1 = setTimeout(() => {
      hitSlot.style.transition =
        "transform 180ms cubic-bezier(0.22, 1, 0.36, 1), filter 180ms ease-out";
      hitSlot.style.transform = "translateY(-5px) scale(1.2)";
      hitSlot.style.filter = "brightness(1.55) saturate(1.15)";
    }, 130);

    const t2 = setTimeout(() => {
      hitSlot.style.transition = "transform 220ms ease, filter 220ms ease";
      hitSlot.style.transform = "translateY(0) scale(1)";
      hitSlot.style.filter = "";
    }, 330);

    plinkoMultiplierHitTransitions.set(hitSlot, [t1, t2]);

    const removeTimer = setTimeout(() => {
      hitSlot.classList.remove("active");
      hitSlot.style.transition = "";
      hitSlot.style.transform = "";
      hitSlot.style.filter = "";
      plinkoMultiplierHitTimers.delete(hitSlot);
      plinkoMultiplierHitTransitions.delete(hitSlot);
    }, 760);
    plinkoMultiplierHitTimers.set(hitSlot, removeTimer);
  }

  const historyList = getPlinkoScopedElement("historyList");
  if (historyList) {
    const h = document.createElement("div");
    h.className = "history-item";
    h.textContent = multiplier + "x";
    h.style.background = getColor(multiplier);
    historyList.prepend(h);
  }

  plinko.balls = plinko.balls.filter((b) => !b.done);
  scheduleCasinoKickoutAfterPlinkoDrain(
    () => plinko.balls.reduce((count, ball) => count + (ball.done ? 0 : 1), 0)
  );
}

function showPlinkoHitPopup(multiplier, win) {
  const popup = getPlinkoScopedElement("plinkoHitPopup");
  if (!popup) return;

  if (plinko.hitPopupTimer) {
    clearTimeout(plinko.hitPopupTimer);
    plinko.hitPopupTimer = null;
  }

  popup.textContent = `HIT ${Number(multiplier).toFixed(2)}x • +$${Number(win).toFixed(2)}`;
  popup.classList.remove("show");
  void popup.offsetWidth;
  popup.classList.add("show");

  plinko.hitPopupTimer = setTimeout(() => {
    popup.classList.remove("show");
    plinko.hitPopupTimer = null;
  }, 950);
}

function renderPlinkoScene(deltaSeconds = 0, advanceBalls = false) {
  if (!plinko.initialized || !plinko.canvas || !plinko.ctx) return;
  const WIDTH = plinko.boardWidth || PLINKO_BOARD_WIDTH;
  plinko.ctx.clearRect(0, 0, WIDTH, plinko.canvas.height);

  plinko.ctx.fillStyle = "#f4fbff";
  plinko.pegs.forEach((p) => {
    plinko.ctx.beginPath();
    plinko.ctx.arc(p.x, p.y, PEG_RADIUS, 0, Math.PI * 2);
    plinko.ctx.fill();
  });

  plinko.balls.forEach((b) => {
    if (advanceBalls) b.update(deltaSeconds);
    b.draw();
  });

  if (PLINKO_DEBUG_ALIGNMENT) {
    const geometry = getPlinkoBottomGeometry();
    plinko.ctx.save();
    plinko.ctx.strokeStyle = "rgba(255, 236, 122, 0.62)";
    plinko.ctx.lineWidth = 1;
    for (let i = 0; i <= geometry.bins; i += 1) {
      const x = geometry.leftBoundary + i * geometry.dx;
      plinko.ctx.beginPath();
      plinko.ctx.moveTo(x, 0);
      plinko.ctx.lineTo(x, plinko.canvas.height);
      plinko.ctx.stroke();
    }
    plinko.ctx.fillStyle = "#ff5f79";
    plinko.ctx.beginPath();
    const bottomRowY = PADDING_TOP + (plinko.rows - 1) * (plinko.spacingY || SPACING_Y);
    plinko.ctx.arc(geometry.leftPegX, bottomRowY, 3, 0, Math.PI * 2);
    plinko.ctx.arc(geometry.rightPegX, bottomRowY, 3, 0, Math.PI * 2);
    plinko.ctx.fill();
    plinko.ctx.restore();
  }
}

function triggerDrop() {
  const activeBalls = plinko.balls.reduce((count, ball) => count + (ball.done ? 0 : 1), 0);
  if (activeBalls <= 0 && !ensureCasinoBettingAllowedNow()) {
    stopAuto();
    return;
  }
  const betInput = getPlinkoScopedElement("betAmount");
  const bet = parseFloat(betInput.value);

  if (isNaN(bet) || bet <= 0) {
    alert("Invalid bet amount");
    stopAuto();
    return;
  }

  if (cash < bet) {
    if (plinko.autoInterval) stopAuto();
    alert("Insufficient Balance!");
    return;
  }

  cash -= bet;
  const balanceDisplay = getPlinkoScopedElement("balanceDisplay");
  if (balanceDisplay) balanceDisplay.textContent = cash.toFixed(2);
  updateUI();
  updateBlackjackCash();

  const targetSlot = getResultBasedOnWeights();
  plinko.balls.push(new Ball(targetSlot, bet));
  renderPlinkoScene(0, false);
}

function startAuto() {
  stopAuto();
  const autoSpeedInput = getPlinkoScopedElement("autoSpeed");
  const baseDelay = parseInt(autoSpeedInput?.value || "400");
  const delay = baseDelay;
  triggerDrop();
  plinko.autoInterval = setInterval(triggerDrop, delay);
  const autoBtn = getPlinkoScopedElement("autoBtn");
  if (autoBtn) {
    autoBtn.textContent = "STOP";
    autoBtn.classList.add("running");
  }
}

function stopAuto() {
  clearInterval(plinko.autoInterval);
  plinko.autoInterval = null;
  const autoBtn = getPlinkoScopedElement("autoBtn");
  if (autoBtn) {
    autoBtn.textContent = "AUTO";
    autoBtn.classList.remove("running");
  }
}

function animatePlinko(now = performance.now()) {
  if (!plinko.initialized || !plinko.canvas || !plinko.ctx) return;
  if (!plinko.lastFrameMs) plinko.lastFrameMs = now;
  const deltaSeconds = Math.min((now - plinko.lastFrameMs) / 1000, 1 / 24);
  plinko.lastFrameMs = now;
  renderPlinkoScene(deltaSeconds, true);
  plinko.animationFrame = requestAnimationFrame(animatePlinko);
}

function initPlinko() {
  plinko.root = getPlinkoRoot();
  plinko.canvas = getPlinkoScopedElement("plinkoCanvas");
  if (!plinko.canvas && plinko.root) {
    plinko.canvas = plinko.root.querySelector("#plinkoCanvas");
  }
  if (!plinko.canvas) return;
  plinko.root = plinko.canvas?.closest(".plinko-root") || plinko.root || getPlinkoRoot();
  plinko.ctx = plinko.canvas.getContext("2d");
  plinko.initialized = true;

  const dropBtn = getPlinkoScopedElement("dropBtn");
  const autoBtn = getPlinkoScopedElement("autoBtn");
  const autoSpeedInput = getPlinkoScopedElement("autoSpeed");
  const rowCountInput = getPlinkoScopedElement("rowCount");
  const difficultyInput = getPlinkoScopedElement("difficulty");

  if (!dropBtn || !autoBtn || !autoSpeedInput || !rowCountInput || !difficultyInput) return;

  dropBtn.onclick = triggerDrop;
  autoBtn.onclick = () => {
    if (plinko.autoInterval) stopAuto();
    else startAuto();
  };
  autoSpeedInput.oninput = () => {
    if (plinko.autoInterval) startAuto();
  };
  rowCountInput.onchange = (e) => {
    plinko.rows = +e.target.value;
    initBoard();
  };
  difficultyInput.onchange = (e) => {
    plinko.difficulty = e.target.value;
    initBoard();
  };

  if (plinko.resizeHandler) {
    window.removeEventListener("resize", plinko.resizeHandler);
  }
  plinko.resizeHandler = () => {
    if (!plinko.initialized) return;
    initBoard();
  };
  window.addEventListener("resize", plinko.resizeHandler);

  initBoard();
  plinko.lastFrameMs = performance.now();
  animatePlinko();
}

function loadPlinkoMini() {
  const container = document.getElementById("casino-container");
  if (!container) return;
  const existingMiniRoot = container.querySelector("#plinko-mini-screen");
  // Guard against duplicate mini mounts when phone routing re-applies enhancements.
  if (existingMiniRoot && plinko.initialized && plinko.layoutMode === "mini") {
    plinko.root = existingMiniRoot;
    initBoard();
    return;
  }
  container.classList.add("casino-fullbleed", "plinko-fullbleed", "plinko-mini-fullbleed");

  container.innerHTML = `
    <div id="plinko-mini-screen" class="plinko-root plinko-mini plinko-mini-root">
      <div class="app-layout">
        <div class="game-container">
          <div class="controls">
            <div class="input-group">
              <label>Bet Amount</label>
              <input type="number" id="betAmount" value="10" min="1" max="1000">
            </div>
            <div class="input-group">
              <label>Rows</label>
              <select id="rowCount">
                <option value="8">8 Rows</option>
                <option value="10">10 Rows</option>
                <option value="12">12 Rows</option>
                <option value="14">14 Rows</option>
                <option value="16" selected>16 Rows</option>
              </select>
            </div>
            <div class="input-group">
              <label>Difficulty</label>
              <select id="difficulty">
                <option value="easy">Easy</option>
                <option value="medium" selected>Medium</option>
                <option value="hard">Hard</option>
              </select>
            </div>
            <div class="input-group">
              <label>Auto Speed</label>
              <input type="range" id="autoSpeed" min="200" max="1000" step="100" value="400">
            </div>
            <button id="autoBtn" class="action-btn auto-btn">AUTO</button>
            <button id="dropBtn" class="action-btn">DROP BALL</button>
          </div>

          <div class="stats">
            <div class="stat-box">
              <span>Balance</span>
              <span id="balanceDisplay" class="highlight">${cash.toFixed(2)}</span>
            </div>
            <div class="stat-box">
              <span>Last Result</span>
              <span id="lastWinDisplay">0.00</span>
            </div>
          </div>

          <div class="canvas-wrapper">
            <div id="plinkoHitPopup" class="plinko-hit-popup" aria-live="polite"></div>
            <canvas id="plinkoCanvas"></canvas>
            <div id="oddsTooltip" class="odds-tooltip">Chance: 0.00%</div>
            <div id="multipliers" class="multiplier-container"></div>
          </div>
        </div>
      </div>

      <div id="historyList" class="history-list plinko-mini-history" aria-hidden="true"></div>
    </div>
  `;

  plinko.rows = 16;
  plinko.difficulty = "medium";
  plinko.layoutMode = "mini";
  plinko.root = container.querySelector("#plinko-mini-screen");
  plinko.boardMaxWidth = 520;
  plinko.maxSpacingY = 30;
  initPlinko();

  activeCasinoCleanup = () => {
    stopAuto();
    if (plinko.hitPopupTimer) {
      clearTimeout(plinko.hitPopupTimer);
      plinko.hitPopupTimer = null;
    }
    if (plinko.resizeHandler) {
      window.removeEventListener("resize", plinko.resizeHandler);
      plinko.resizeHandler = null;
    }
    if (plinko.animationFrame) {
      cancelAnimationFrame(plinko.animationFrame);
      plinko.animationFrame = null;
    }
    plinko.initialized = false;
    plinko.balls = [];
    plinko.pegs = [];
    plinko.lastFrameMs = 0;
    plinko.layoutMode = "default";
    plinko.root = null;
    plinko.boardMaxWidth = 520;
    plinko.maxSpacingY = 30;
    container.classList.remove("casino-fullbleed", "plinko-fullbleed", "plinko-mini-fullbleed");
  };
}

function loadPlinkoFull() {
  const container = document.getElementById("casino-container");
  if (!container) return;
  container.classList.add("casino-fullbleed", "plinko-fullbleed");

  container.innerHTML = `
    <div id="plinko-casino-screen" class="plinko-root plinko-casino plinko-full-root">
      <div class="app-layout">
        <div class="game-container">
          <div class="controls">
            <div class="brand">SAGE</div>
            <div class="input-group">
              <label>Bet Amount</label>
              <input type="number" id="betAmount" value="10" min="1" max="1000">
            </div>
            <div class="input-group">
              <label>Rows</label>
              <select id="rowCount">
                <option value="8">8 Rows</option>
                <option value="10">10 Rows</option>
                <option value="12">12 Rows</option>
                <option value="14">14 Rows</option>
                <option value="16" selected>16 Rows</option>
              </select>
            </div>
            <div class="input-group">
              <label>Difficulty</label>
              <select id="difficulty">
                <option value="easy">Easy</option>
                <option value="medium" selected>Medium</option>
                <option value="hard">Hard</option>
              </select>
            </div>
            <div class="input-group">
              <label>Auto Speed</label>
              <input type="range" id="autoSpeed" min="200" max="1000" step="100" value="400">
            </div>
            <button id="autoBtn" class="action-btn auto-btn">AUTO</button>
            <button id="dropBtn" class="action-btn">DROP BALL</button>
          </div>

          <div class="stats">
            <div class="stat-box">
              <span>Balance</span>
              <span id="balanceDisplay" class="highlight">${cash.toFixed(2)}</span>
            </div>
            <div class="stat-box">
              <span>Last Result</span>
              <span id="lastWinDisplay">0.00</span>
            </div>
          </div>

          <div class="canvas-wrapper">
            <div id="plinkoHitPopup" class="plinko-hit-popup" aria-live="polite"></div>
            <canvas id="plinkoCanvas"></canvas>
            <div id="oddsTooltip" class="odds-tooltip">Chance: 0.00%</div>
            <div id="multipliers" class="multiplier-container"></div>
          </div>
        </div>

        <div class="history-column">
          <div class="history-header">LAST DROPS</div>
          <div id="historyList" class="history-list"></div>
        </div>

      </div>
    </div>
  `;

  plinko.boardMaxWidth = 2200;
  plinko.layoutMode = "full";
  plinko.root = container.querySelector("#plinko-casino-screen");
  plinko.maxSpacingY = 64;
  initPlinko();

  activeCasinoCleanup = () => {
    stopAuto();
    if (plinko.hitPopupTimer) {
      clearTimeout(plinko.hitPopupTimer);
      plinko.hitPopupTimer = null;
    }
    if (plinko.resizeHandler) {
      window.removeEventListener("resize", plinko.resizeHandler);
      plinko.resizeHandler = null;
    }
    if (plinko.animationFrame) {
      cancelAnimationFrame(plinko.animationFrame);
      plinko.animationFrame = null;
    }
    plinko.initialized = false;
    plinko.balls = [];
    plinko.pegs = [];
    plinko.lastFrameMs = 0;
    plinko.layoutMode = "default";
    plinko.root = null;
    plinko.boardMaxWidth = 520;
    plinko.maxSpacingY = 30;
    container.classList.remove("casino-fullbleed", "plinko-fullbleed");
  };
}

function loadPlinko() {
  if (IS_PHONE_EMBED_MODE) {
    loadPlinkoMini();
    return;
  }
  loadPlinkoFull();
}

function loadDragonTower() {
  const container = document.getElementById("casino-container");
  if (!container) return;
  container.classList.add("casino-fullbleed", "dragon-fullbleed");

  container.innerHTML = `
    <div class="dragon-root">
      <div class="app">
        <aside class="side-panel" aria-label="Controls">
          <header class="panel-header">
            <div class="game-title">Dragon Tower</div>
            <div class="panel-tabs" role="tablist" aria-label="Mode">
              <button
                class="tab active"
                type="button"
                role="tab"
                aria-selected="true"
                data-mode="manual"
              >
                Manual
              </button>
              <button class="tab" type="button" role="tab" aria-selected="false" data-mode="auto">
                Auto
              </button>
            </div>
          </header>

          <div class="panel-section">
            <div class="field">
              <label for="betInput">Bet Amount</label>
              <div class="field-row">
                <input
                  type="number"
                  id="betInput"
                  min="1"
                  step="0.01"
                  value="10.00"
                  placeholder="Enter bet"
                />
                <button class="chip" type="button" data-chip="0.5">1/2</button>
                <button class="chip" type="button" data-chip="2">2×</button>
              </div>
            </div>

            <div class="field">
              <label for="difficulty">Difficulty</label>
              <select id="difficulty">
                <option value="easy">Easy</option>
                <option value="medium">Medium</option>
                <option value="hard">Hard</option>
              </select>
            </div>

            <div class="auto-only" aria-label="Auto settings">
              <div class="field">
                <label for="autoCashoutRow">Auto Cash Out</label>
                <select id="autoCashoutRow">
                  <option value="0">Never (until lose)</option>
                  <option value="1">After 1 row</option>
                  <option value="2">After 2 rows</option>
                  <option value="3">After 3 rows</option>
                  <option value="4">After 4 rows</option>
                  <option value="5" selected>After 5 rows</option>
                  <option value="6">After 6 rows</option>
                  <option value="7">After 7 rows</option>
                  <option value="8">After 8 rows</option>
                  <option value="9">At top</option>
                </select>
              </div>

              <div class="field">
                <label for="autoRounds">Rounds</label>
                <input type="number" id="autoRounds" min="0" placeholder="0 = infinite" />
              </div>

              <div class="field">
                <label for="autoSpeed">Speed</label>
                <select id="autoSpeed">
                  <option value="fast">Fast</option>
                  <option value="normal" selected>Normal</option>
                  <option value="slow">Slow</option>
                </select>
              </div>

              <div class="field">
                <label for="autoStop">Stop</label>
                <select id="autoStop">
                  <option value="never" selected>Never</option>
                  <option value="win">After win</option>
                  <option value="loss">After loss</option>
                  <option value="either">After win or loss</option>
                </select>
              </div>

              <div class="field">
                <label for="autoPickMode">Auto Pick</label>
                <select id="autoPickMode">
                  <option value="random" selected>Random (AI chooses)</option>
                  <option value="lane-1">Always tile 1</option>
                  <option value="lane-2">Always tile 2</option>
                  <option value="lane-3">Always tile 3</option>
                  <option value="lane-4">Always tile 4</option>
                  <option value="pattern">Pattern</option>
                </select>
              </div>

              <div class="field" id="autoPickPatternWrap" hidden>
                <label for="autoPickPattern">Pattern (pick on tower)</label>
                <input
                  type="text"
                  id="autoPickPattern"
                  value="1,2,3,4"
                  placeholder="Click tower tiles by row"
                  autocomplete="off"
                />
              </div>
            </div>
          </div>

          <div class="panel-section">
            <div class="button-group">
              <button id="startBtn" class="btn primary" type="button">Bet</button>
              <button id="cashOutBtn" class="btn accent" type="button" disabled>Cash Out</button>
              <button id="resetBtn" class="btn ghost" type="button">Reset</button>
            </div>
          </div>

          <div class="panel-section stats">
            <div class="stat">
              <span class="k">Balance</span>
              <span class="v">$<span id="balance">0</span></span>
            </div>
            <div class="stat">
              <span class="k">Multiplier</span>
              <span class="v"><span id="multiplier">1.00x</span></span>
            </div>
            <div class="stat">
              <span class="k">High Score</span>
              <span class="v"><span id="highScore">0</span>x</span>
            </div>
          </div>

          <div class="message" id="message" role="status" aria-live="polite">${IS_PHONE_EMBED_MODE ? "Place a bet to start" : "SAGE"}</div>
        </aside>

        <main class="board-area" aria-label="Game board">
          <div class="tower">
            <div class="tower-chrome" aria-hidden="true"></div>
            <div class="tower-inner">
              <section id="gameBoard" class="board" aria-label="Tower grid"></section>
            </div>
          </div>
        </main>
      </div>
    </div>
  `;
  if (IS_PHONE_EMBED_MODE) {
    addSageBrand(container.querySelector(".dragon-root"), "bottom-left");
  }

  let destroyed = false;
  let balance = cash;
  let betAmount = 0;
  let multiplier = 1;
  let currentRow = 0;
  let towerData = [];
  let gameActive = false;
  let difficulty = "easy";
  let highScore = 0;
  let scheduledCashoutId = null;

  const ROWS = 9;
  const TILES = 4;

  const DIFF = {
    easy: { safe: 3 },
    medium: { safe: 2 },
    hard: { safe: 1 }
  };

  const MULTIPLIERS = {
    easy: [1.31, 1.75, 2.33, 3.11, 4.14, 5.52, 7.36, 9.81, 13.89],
    medium: [1.47, 2.21, 3.31, 4.96, 7.44, 11.16, 16.74, 25.12, 37.67],
    hard: [1.96, 3.92, 7.84, 15.68, 31.36, 62.72, 125.44, 250.88, 501.76]
  };

  let uiMode = "manual";
  let autoRunning = false;
  let autoRunId = 0;
  let lastOutcome = null;
  let rowElements = [];
  const autoPatternByRow = Array.from({ length: ROWS }, () => null);

  const balanceEl = container.querySelector("#balance");
  const multiplierEl = container.querySelector("#multiplier");
  const highScoreEl = container.querySelector("#highScore");
  const boardEl = container.querySelector("#gameBoard");
  const msgEl = container.querySelector("#message");
  const betInput = container.querySelector("#betInput");
  const diffSelect = container.querySelector("#difficulty");
  const cashBtn = container.querySelector("#cashOutBtn");
  const startBtn = container.querySelector("#startBtn");
  const resetBtn = container.querySelector("#resetBtn");
  const towerEl = container.querySelector(".tower");
  const appEl = container.querySelector(".app");
  const tabs = Array.from(container.querySelectorAll(".panel-tabs .tab"));
  const autoCashoutRowEl = container.querySelector("#autoCashoutRow");
  const autoRoundsEl = container.querySelector("#autoRounds");
  const autoSpeedEl = container.querySelector("#autoSpeed");
  const autoStopEl = container.querySelector("#autoStop");
  const autoPickModeEl = container.querySelector("#autoPickMode");
  const autoPickPatternWrapEl = container.querySelector("#autoPickPatternWrap");
  const autoPickPatternEl = container.querySelector("#autoPickPattern");

  function save() {
    localStorage.setItem("dragon_tower_high", String(highScore));
  }

  function load() {
    balance = cash;
    highScore = parseFloat(localStorage.getItem("dragon_tower_high")) || 0;
  }

  function syncGlobalCash() {
    cash = Math.max(0, balance);
    updateUI();
    updateBlackjackCash();
  }

  function updateBalance() {
    if (destroyed || !balanceEl) return;
    balanceEl.textContent = balance.toFixed(2);
    bumpValue(balanceEl);
  }

  function updateMultiplier() {
    if (destroyed || !multiplierEl) return;
    multiplierEl.textContent = multiplier.toFixed(2) + "x";
    bumpValue(multiplierEl);
  }

  function message(text) {
    if (destroyed || !msgEl) return;
    msgEl.textContent = text;
    msgEl.classList.toggle("is-brand", !IS_PHONE_EMBED_MODE && text === "SAGE");
    msgEl.classList.remove("update");
    requestAnimationFrame(() => msgEl.classList.add("update"));
  }

  function bumpValue(el) {
    el.classList.remove("bump");
    requestAnimationFrame(() => el.classList.add("bump"));
    window.setTimeout(() => {
      if (destroyed) return;
      el.classList.remove("bump");
    }, 240);
  }

  function rumbleTower() {
    if (destroyed || !towerEl) return;
    towerEl.classList.remove("rumble");
    requestAnimationFrame(() => towerEl.classList.add("rumble"));
    window.setTimeout(() => {
      if (destroyed) return;
      towerEl.classList.remove("rumble");
    }, 520);
  }

  function setTabsEnabled(enabled) {
    tabs.forEach((tab) => {
      tab.disabled = !enabled;
    });
  }

  function setControlsEnabled(enabled) {
    const allow = enabled && !autoRunning;
    if (betInput) betInput.disabled = !allow;
    if (diffSelect) diffSelect.disabled = !allow;
    if (startBtn) startBtn.disabled = !allow;
    if (autoCashoutRowEl) autoCashoutRowEl.disabled = !allow;
    if (autoRoundsEl) autoRoundsEl.disabled = !allow;
    if (autoSpeedEl) autoSpeedEl.disabled = !allow;
    if (autoStopEl) autoStopEl.disabled = !allow;
    if (autoPickModeEl) autoPickModeEl.disabled = !allow;
    if (autoPickPatternEl) autoPickPatternEl.disabled = !allow || (autoPickModeEl?.value || "random") !== "pattern";
    setTabsEnabled(allow);
    syncAutoPatternBoardUi();
  }

  function syncAutoPickUi() {
    const mode = autoPickModeEl?.value || "random";
    const showPattern = mode === "pattern";
    if (autoPickPatternWrapEl) autoPickPatternWrapEl.hidden = !showPattern;
    if (autoPickPatternEl) autoPickPatternEl.disabled = !showPattern || autoRunning;
    syncAutoPatternBoardUi();
    if (showPattern && isAutoPatternSelectMode()) {
      const picked = getAutoPatternSelectedCount();
      message(picked > 0 ? `Pattern ready (${picked}/${ROWS} rows)` : "Pick tower tiles to build your auto pattern");
    }
  }

  function parseAutoPattern() {
    const raw = String(autoPickPatternEl?.value || "");
    const values = raw
      .split(/[^\d]+/g)
      .map((token) => Number.parseInt(token, 10))
      .filter((value) => Number.isInteger(value) && value >= 1 && value <= TILES)
      .map((value) => value - 1);
    return values;
  }

  function isAutoPatternSelectMode() {
    return uiMode === "auto" && !autoRunning && !gameActive && (autoPickModeEl?.value || "random") === "pattern";
  }

  function getAutoPatternSelectedCount() {
    return autoPatternByRow.filter((value) => Number.isInteger(value)).length;
  }

  function syncAutoPatternInputFromBoard() {
    if (!autoPickPatternEl) return;
    const picks = autoPatternByRow
      .filter((value) => Number.isInteger(value))
      .map((value) => String(value + 1));
    autoPickPatternEl.value = picks.join(",");
  }

  function syncAutoPatternBoardUi() {
    const allowSelection = isAutoPatternSelectMode();
    rowElements.forEach((rowEl, rowIndex) => {
      const hasSelection = allowSelection && Number.isInteger(autoPatternByRow[rowIndex]);
      rowEl.classList.toggle("auto-pattern-row", hasSelection);
      rowEl.querySelectorAll(".tile").forEach((tileEl, colIndex) => {
        const selected = hasSelection && autoPatternByRow[rowIndex] === colIndex;
        tileEl.classList.toggle("auto-pattern-selected", selected);
        tileEl.setAttribute("aria-pressed", selected ? "true" : "false");
      });
    });
  }

  function selectAutoPatternTile(rowIndex, colIndex) {
    if (rowIndex < 0 || rowIndex >= ROWS || colIndex < 0 || colIndex >= TILES) return;
    autoPatternByRow[rowIndex] = autoPatternByRow[rowIndex] === colIndex ? null : colIndex;
    syncAutoPatternInputFromBoard();
    syncAutoPatternBoardUi();
    const picked = getAutoPatternSelectedCount();
    message(picked > 0 ? `Pattern ready (${picked}/${ROWS} rows)` : "Pick tiles to set an auto pattern");
  }

  function getAutoPickColumn(config, pickIndex, rowIndex) {
    if (config.pickMode === "lane" && Number.isInteger(config.fixedCol)) {
      return config.fixedCol;
    }
    if (config.pickMode === "pattern") {
      const byRow = config.patternByRow?.[rowIndex];
      if (config.usePatternByRow && Number.isInteger(byRow)) return byRow;
      if (Array.isArray(config.pattern) && config.pattern.length > 0) {
        return config.pattern[pickIndex % config.pattern.length];
      }
    }
    return Math.floor(Math.random() * TILES);
  }

  function syncButtons() {
    if (!startBtn || !cashBtn) return;

    if (uiMode === "manual") {
      startBtn.textContent = "Bet";
      cashBtn.textContent = "Cash Out";
      cashBtn.classList.remove("danger");
      cashBtn.classList.add("accent");
      cashBtn.disabled = !gameActive;
      return;
    }

    startBtn.textContent = autoRunning ? "Auto Running..." : "Start Auto";
    cashBtn.textContent = "Stop Auto";
    cashBtn.classList.remove("accent");
    cashBtn.classList.add("danger");
    cashBtn.disabled = !autoRunning;
  }

  function setMode(mode) {
    if (mode !== "manual" && mode !== "auto") return;
    if (autoRunning) stopAuto({ cashoutIfActive: false });

    uiMode = mode;
    if (appEl) appEl.classList.toggle("mode-auto", uiMode === "auto");

    tabs.forEach((tab) => {
      const isActive = (tab.getAttribute("data-mode") || "manual") === uiMode;
      tab.classList.toggle("active", isActive);
      tab.setAttribute("aria-selected", isActive ? "true" : "false");
    });

    syncButtons();
    message(uiMode === "auto" ? "Configure auto and start" : (IS_PHONE_EMBED_MODE ? "Place a bet to start" : "SAGE"));
    syncAutoPatternBoardUi();
  }

  function sleep(ms) {
    return new Promise((resolve) => window.setTimeout(resolve, ms));
  }

  function getAutoConfig() {
    const cashoutRow = Math.max(0, Math.min(ROWS, parseInt(autoCashoutRowEl?.value || "0", 10) || 0));
    const rounds = Math.max(0, parseInt(autoRoundsEl?.value || "0", 10) || 0);
    const speed = autoSpeedEl?.value || "normal";
    const stop = autoStopEl?.value || "never";
    const pickModeRaw = autoPickModeEl?.value || "random";
    const pattern = pickModeRaw === "pattern" ? parseAutoPattern() : [];
    const patternByRow =
      pickModeRaw === "pattern"
        ? autoPatternByRow.map((value) => (Number.isInteger(value) ? value : null))
        : [];
    const usePatternByRow = patternByRow.some((value) => Number.isInteger(value));

    let delay = 420;
    if (speed === "fast") delay = 260;
    if (speed === "slow") delay = 650;

    let pickMode = "random";
    let fixedCol = null;
    if (pickModeRaw.startsWith("lane-")) {
      const col = Number.parseInt(pickModeRaw.split("-")[1] || "", 10) - 1;
      if (Number.isInteger(col) && col >= 0 && col < TILES) {
        pickMode = "lane";
        fixedCol = col;
      }
    } else if (pickModeRaw === "pattern" && (pattern.length > 0 || usePatternByRow)) {
      pickMode = "pattern";
    }

    return {
      cashoutRow,
      rounds,
      delay,
      stop,
      pickMode,
      fixedCol,
      pattern,
      patternByRow,
      usePatternByRow,
      pickModeRaw
    };
  }

  async function startAuto() {
    if (autoRunning || destroyed) return;
    const config = getAutoConfig();
    if (config.pickModeRaw === "pattern" && !config.usePatternByRow && config.pattern.length === 0) {
      message("Pick at least one tower tile for Pattern auto mode");
      return;
    }

    autoRunning = true;
    lastOutcome = null;
    autoRunId += 1;
    const runId = autoRunId;

    setControlsEnabled(false);
    syncButtons();
    message("Auto running...");

    let roundsPlayed = 0;

    while (!destroyed && autoRunning && (config.rounds === 0 || roundsPlayed < config.rounds)) {
      if (runId !== autoRunId) return;

      startGame();
      if (!gameActive) {
        message("Auto stopped (invalid bet)");
        break;
      }

      let roundPickIndex = 0;
      while (!destroyed && autoRunning && gameActive) {
        if (runId !== autoRunId) return;

        if (currentRow >= ROWS) {
          await sleep(650);
          continue;
        }

        if (config.cashoutRow > 0 && currentRow >= config.cashoutRow) {
          await sleep(Math.max(140, Math.floor(config.delay * 0.55)));
          cashOut();
          break;
        }

        await sleep(config.delay);
        const col = getAutoPickColumn(config, roundPickIndex, currentRow);
        roundPickIndex += 1;
        clickTile(currentRow, col);
      }

      roundsPlayed += 1;
      await sleep(Math.max(180, Math.floor(config.delay * 0.5)));

      if (!autoRunning || destroyed) break;

      if (config.stop === "either" && lastOutcome) break;
      if (config.stop === "win" && lastOutcome === "win") break;
      if (config.stop === "loss" && lastOutcome === "loss") break;
    }

    stopAuto({ cashoutIfActive: false });
  }

  function stopAuto({ cashoutIfActive }) {
    autoRunning = false;
    autoRunId += 1;

    if (scheduledCashoutId) {
      window.clearTimeout(scheduledCashoutId);
      scheduledCashoutId = null;
    }

    if (cashoutIfActive && gameActive) cashOut();
    if (destroyed) return;

    setControlsEnabled(true);
    syncButtons();
    if (uiMode === "auto") message("Auto stopped");
  }

  function renderBoard() {
    if (!boardEl) return;

    boardEl.innerHTML = "";
    rowElements = [];

    for (let rowIndex = 0; rowIndex < ROWS; rowIndex++) {
      const row = document.createElement("div");
      row.className = "row";

      for (let colIndex = 0; colIndex < TILES; colIndex++) {
        const slot = document.createElement("div");
        slot.className = "slot";

        const tile = document.createElement("div");
        tile.className = "tile";
        tile.setAttribute("role", "button");
        tile.setAttribute("tabindex", "0");
        tile.setAttribute("aria-label", `Row ${rowIndex + 1}, tile ${colIndex + 1}`);

        const onPick = () => {
          if (isAutoPatternSelectMode()) {
            selectAutoPatternTile(rowIndex, colIndex);
            return;
          }
          clickTile(rowIndex, colIndex);
        };
        tile.addEventListener("click", onPick);
        tile.addEventListener("keydown", (event) => {
          if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            onPick();
          }
        });

        slot.appendChild(tile);
        row.appendChild(slot);
      }

      boardEl.appendChild(row);
      rowElements.push(row);
    }
    syncAutoPatternBoardUi();
  }

  function generateTower() {
    towerData = [];
    const rigPenalty = shouldRigHighBet(betAmount, 1) ? 1 : 0;
    const safeCount = Math.max(1, DIFF[difficulty].safe - rigPenalty);

    for (let rowIndex = 0; rowIndex < ROWS; rowIndex++) {
      const row = Array(TILES).fill(false);
      let placed = 0;

      while (placed < safeCount) {
        const idx = Math.floor(Math.random() * TILES);
        if (!row[idx]) {
          row[idx] = true;
          placed += 1;
        }
      }

      towerData.push(row);
    }
  }

  function resetBoardVisuals() {
    rowElements.forEach((row) => {
      row.classList.remove("active");
      row.querySelectorAll(".tile").forEach((tile) => {
        tile.classList.remove("safe", "fail", "reveal", "picked");
        tile.style.removeProperty("--i");
      });
    });
  }

  function activateRow(index) {
    const row = rowElements[index];
    if (!row) return;

    row.classList.add("active", "activate");
    window.setTimeout(() => {
      if (destroyed) return;
      row.classList.remove("activate");
    }, 260);
  }

  function startGame() {
    if (gameActive || destroyed) return;
    if (!ensureCasinoBettingAllowedNow()) return;

    if (scheduledCashoutId) {
      window.clearTimeout(scheduledCashoutId);
      scheduledCashoutId = null;
    }

    betAmount = parseFloat(betInput?.value || "0");
    difficulty = diffSelect?.value || "easy";

    if (!betAmount || betAmount <= 0 || betAmount > balance) {
      message("Invalid bet");
      return;
    }

    balance -= betAmount;
    syncGlobalCash();
    updateBalance();

    multiplier = 1;
    currentRow = 0;
    gameActive = true;
    lastOutcome = null;

    if (uiMode === "manual") cashBtn.disabled = false;

    setControlsEnabled(false);
    generateTower();
    resetBoardVisuals();
    activateRow(currentRow);

    updateMultiplier();
    message(uiMode === "manual" ? "Choose a tile" : "Auto picking...");
  }

  function revealRow(rowIndex, pickedCol) {
    const rowEl = rowElements[rowIndex];
    if (!rowEl) return;

    rowEl.classList.remove("revealing");
    requestAnimationFrame(() => rowEl.classList.add("revealing"));

    window.setTimeout(() => {
      if (destroyed) return;
      rowEl.classList.remove("revealing");
    }, 680);

    const tiles = rowEl.querySelectorAll(".tile");
    tiles.forEach((tile, idx) => {
      tile.style.setProperty("--i", String(idx));
      tile.classList.remove("reveal");
      tile.classList.remove("picked");
      tile.classList.add(towerData[rowIndex][idx] ? "safe" : "fail");
      if (idx === pickedCol) tile.classList.add("picked");
    });

    requestAnimationFrame(() => {
      tiles.forEach((tile) => tile.classList.add("reveal"));
    });
  }

  function growMultiplier() {
    multiplier = MULTIPLIERS[difficulty][currentRow];
    updateMultiplier();
  }

  function endGame(win) {
    gameActive = false;

    if (scheduledCashoutId) {
      window.clearTimeout(scheduledCashoutId);
      scheduledCashoutId = null;
    }

    if (!win) lastOutcome = "loss";
    if (uiMode === "manual") cashBtn.disabled = true;

    multiplier = 1;
    updateMultiplier();
    setControlsEnabled(true);
    syncButtons();

    if (!win) message("Game Over");
    triggerCasinoKickoutCheckAfterRound();
  }

  function cashOut() {
    if (!gameActive || destroyed) return;

    if (scheduledCashoutId) {
      window.clearTimeout(scheduledCashoutId);
      scheduledCashoutId = null;
    }

    const win = betAmount * multiplier;
    const netProfit = win - betAmount;
    lastOutcome = "win";

    balance += win;
    syncGlobalCash();

    if (multiplier > highScore) {
      highScore = multiplier;
      if (highScoreEl) highScoreEl.textContent = highScore.toFixed(2);
      save();
    }

    updateBalance();
    if (netProfit > 0) showCasinoWinPopup({ amount: netProfit, multiplier });
    message(`Won $${win.toFixed(2)}`);
    endGame(true);
  }

  function clickTile(rowIndex, colIndex) {
    if (destroyed || !gameActive || rowIndex !== currentRow) return;

    const slot = rowElements[rowIndex]?.children?.[colIndex];
    if (slot) {
      slot.classList.remove("picked");
      requestAnimationFrame(() => slot.classList.add("picked"));
      window.setTimeout(() => {
        if (destroyed) return;
        slot.classList.remove("picked");
      }, 240);
    }

    revealRow(rowIndex, colIndex);

    if (towerData[rowIndex][colIndex]) {
      growMultiplier();
      rowElements[rowIndex]?.classList.remove("active");

      currentRow += 1;

      if (currentRow >= ROWS) {
        if (scheduledCashoutId) window.clearTimeout(scheduledCashoutId);
        scheduledCashoutId = window.setTimeout(() => {
          scheduledCashoutId = null;
          cashOut();
        }, 600);
        return;
      }

      activateRow(currentRow);
      message("Safe! Climb higher!");
      return;
    }

    rumbleTower();
    endGame(false);
  }

  function init() {
    load();
    updateBalance();
    updateMultiplier();
    if (highScoreEl) highScoreEl.textContent = highScore.toFixed(2);
    renderBoard();
    setControlsEnabled(true);
    setMode("manual");
    syncButtons();
  }

  if (startBtn) {
    startBtn.onclick = () => {
      if (uiMode === "auto") {
        startAuto();
      } else {
        startGame();
      }
    };
  }

  if (cashBtn) {
    cashBtn.onclick = () => {
      if (uiMode === "auto") {
        stopAuto({ cashoutIfActive: true });
      } else {
        cashOut();
      }
    };
  }

  if (resetBtn) {
    resetBtn.onclick = () => {
      stopAuto({ cashoutIfActive: false });
      gameActive = false;
      multiplier = 1;
      currentRow = 0;
      highScore = 0;
      save();
      init();
    };
  }

  tabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      const mode = tab.getAttribute("data-mode") || "manual";
      setMode(mode);
    });
  });

  if (autoPickModeEl) {
    autoPickModeEl.addEventListener("change", () => {
      syncAutoPickUi();
    });
  }

  container.querySelectorAll("[data-chip]").forEach((button) => {
    button.addEventListener("click", () => {
      const factor = parseFloat(button.getAttribute("data-chip") || "1");
      const current = parseFloat(betInput?.value || "");
      const base = Number.isFinite(current) && current > 0 ? current : 1;
      const next = factor === 0.5 ? base / 2 : base * factor;
      const formatted = Math.max(1, next);
      if (betInput) {
        betInput.value = Number.isInteger(formatted) ? String(formatted) : formatted.toFixed(2);
        betInput.focus();
      }
    });
  });

  init();
  syncAutoPickUi();

  activeCasinoCleanup = () => {
    destroyed = true;
    stopAuto({ cashoutIfActive: false });
    if (scheduledCashoutId) {
      window.clearTimeout(scheduledCashoutId);
      scheduledCashoutId = null;
    }
    container.classList.remove("casino-fullbleed", "dragon-fullbleed");
  };
}

function loadDice() {
  const container = document.getElementById("casino-container");
  if (!container) return;
  container.classList.add("casino-fullbleed", "dice-fullbleed");

  container.innerHTML = `
    <div class="dice-root">
      <nav class="top-nav">
        <div class="logo" aria-hidden="true"></div>
        <div class="nav-links">
          <div class="balance-container">
            <span style="color:#b1bad3; margin-right:5px; font-size:0.9rem;">Balance:</span>
            <span id="balanceDisplay">$1000.00</span>
          </div>
          <button class="wallet-btn" type="button">Wallet</button>
        </div>
      </nav>

      <div class="main-layout">
        <div class="sidebar">
          <div class="control-group">
            <label>Bet Amount</label>
            <div class="input-complex">
              <div class="input-icon">$</div>
              <input type="number" id="betInput" value="10.00" step="0.01" min="0">
              <div class="input-actions">
                <button id="btnHalf" type="button">½</button>
                <button id="btnDouble" type="button">2×</button>
              </div>
            </div>
          </div>

          <div class="control-group">
            <label>Profit on Win</label>
            <div class="input-complex">
              <div class="input-icon">$</div>
              <input type="number" id="profitInput" value="0.00" readonly>
            </div>
          </div>

          <div class="control-group">
            <label>Game Mode</label>
            <div class="mode-toggle">
              <button class="mode-btn" id="btnModeUnder" type="button">Roll Under</button>
              <button class="mode-btn active" id="btnModeOver" type="button">Roll Over</button>
            </div>
          </div>

          <button id="rollBtn" class="action-btn" type="button">Bet</button>
        </div>

        <div class="game-panel">
          <div class="slider-display-area">
            <div id="resultPopup" class="result-popup">
              <span id="resultValue">50.00</span>
            </div>

            <div class="range-markers">
              <span>0</span>
              <span>25</span>
              <span>50</span>
              <span>75</span>
              <span>100</span>
            </div>

            <div class="slider-wrapper">
              <input type="range" id="targetSlider" min="2" max="98" step="1" value="50">
              <div id="resultMarker" class="result-marker"></div>
            </div>
          </div>

          <div class="stats-container">
            <div class="stat-box">
              <label>Multiplier</label>
              <div class="stat-input-wrapper">
                <input type="text" id="multiplierInput" readonly>
              </div>
            </div>
            <div class="stat-box">
              <label>Target</label>
              <div class="stat-input-wrapper">
                <input type="text" id="rollTargetInput" readonly>
              </div>
            </div>
            <div class="stat-box">
              <label>Win Chance</label>
              <div class="stat-input-wrapper">
                <input type="text" id="winChanceInput" readonly>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  `;
  addSageBrand(container.querySelector(".dice-root"), "bottom-left");

  const HOUSE_EDGE = 1.0;
  let destroyed = false;
  let pendingRollTimeout = null;
  let pendingResetTimeout = null;

  const slider = container.querySelector("#targetSlider");
  const betInput = container.querySelector("#betInput");
  const profitInput = container.querySelector("#profitInput");
  const balanceDisplay = container.querySelector("#balanceDisplay");
  const rollBtn = container.querySelector("#rollBtn");
  const resultMarker = container.querySelector("#resultMarker");
  const resultPopup = container.querySelector("#resultPopup");
  const resultValueSpan = container.querySelector("#resultValue");
  const multiplierInput = container.querySelector("#multiplierInput");
  const rollTargetInput = container.querySelector("#rollTargetInput");
  const winChanceInput = container.querySelector("#winChanceInput");
  const btnHalf = container.querySelector("#btnHalf");
  const btnDouble = container.querySelector("#btnDouble");
  const btnModeOver = container.querySelector("#btnModeOver");
  const btnModeUnder = container.querySelector("#btnModeUnder");

  const gameState = {
    balance: cash,
    currentBet: 10.0,
    target: 50,
    isRollOver: true,
    multiplier: 1.98,
    winChance: 49.5
  };

  function syncGlobalCash() {
    cash = Math.max(0, gameState.balance);
    updateUI();
    updateBlackjackCash();
  }

  function updateProfit() {
    if (!profitInput) return;
    const profit = gameState.currentBet * gameState.multiplier - gameState.currentBet;
    profitInput.value = profit.toFixed(2);
  }

  function updateMath() {
    if (gameState.isRollOver) {
      gameState.winChance = 100 - gameState.target;
    } else {
      gameState.winChance = gameState.target;
    }

    if (gameState.winChance <= 1) gameState.winChance = 1;
    if (gameState.winChance >= 99) gameState.winChance = 99;

    gameState.multiplier = (100 - HOUSE_EDGE) / gameState.winChance;

    if (rollTargetInput) rollTargetInput.value = gameState.target.toFixed(2);
    if (multiplierInput) multiplierInput.value = gameState.multiplier.toFixed(4) + "x";
    if (winChanceInput) winChanceInput.value = gameState.winChance.toFixed(2) + "%";

    updateProfit();
  }

  function updateVisuals() {
    if (!balanceDisplay || !slider) return;

    balanceDisplay.textContent = "$" + gameState.balance.toFixed(2);

    const val = gameState.target;
    const green = "#00e701";
    const red = "#ff4d4d";

    if (gameState.isRollOver) {
      slider.style.background = `linear-gradient(to right, ${red} 0%, ${red} ${val}%, ${green} ${val}%, ${green} 100%)`;
    } else {
      slider.style.background = `linear-gradient(to right, ${green} 0%, ${green} ${val}%, ${red} ${val}%, ${red} 100%)`;
    }
  }

  function resetPendingTimers() {
    if (pendingRollTimeout) {
      window.clearTimeout(pendingRollTimeout);
      pendingRollTimeout = null;
    }
    if (pendingResetTimeout) {
      window.clearTimeout(pendingResetTimeout);
      pendingResetTimeout = null;
    }
  }

  function rollDice() {
    if (destroyed) return;
    if (!ensureCasinoBettingAllowedNow()) return;

    if (gameState.currentBet > gameState.balance) {
      alert("Insufficient Balance");
      return;
    }
    if (gameState.currentBet <= 0) {
      alert("Enter a valid bet");
      return;
    }

    if (!rollBtn || !resultPopup || !resultMarker || !resultValueSpan) return;

    rollBtn.disabled = true;
    rollBtn.textContent = "Rolling...";

    resultPopup.classList.remove("show", "win", "lose");
    resultMarker.classList.remove("show", "win", "lose");

    const raw = Math.random() * 100;
    let result = parseFloat(raw.toFixed(2));

    let isWin = false;
    if (gameState.isRollOver) {
      isWin = result > gameState.target;
    } else {
      isWin = result < gameState.target;
    }

    if (isWin && shouldRigHighBet(gameState.currentBet, 1.05)) {
      isWin = false;
      if (gameState.isRollOver) {
        result = Number((Math.max(0, gameState.target - Math.random() * 8)).toFixed(2));
      } else {
        result = Number((Math.min(99.99, gameState.target + Math.random() * 8)).toFixed(2));
      }
    }

    resetPendingTimers();

    pendingRollTimeout = window.setTimeout(() => {
      if (destroyed) return;

      resultMarker.style.left = result + "%";
      resultMarker.classList.add("show");
      resultMarker.classList.add(isWin ? "win" : "lose");

      resultPopup.classList.add("show");
      resultValueSpan.textContent = result.toFixed(2);

      if (isWin) {
        const profit = gameState.currentBet * gameState.multiplier - gameState.currentBet;
        const payout = gameState.currentBet * gameState.multiplier;
        gameState.balance += profit;
        showCasinoWinPopup({ amount: profit, multiplier: gameState.multiplier });
        resultPopup.classList.add("win");
        rollBtn.style.backgroundColor = "#00e701";
      } else {
        gameState.balance -= gameState.currentBet;
        resultPopup.classList.add("lose");
        rollBtn.style.backgroundColor = "#ff4d4d";
      }

      syncGlobalCash();
      updateVisuals();
      triggerCasinoKickoutCheckAfterRound();

      pendingResetTimeout = window.setTimeout(() => {
        if (destroyed) return;
        rollBtn.disabled = false;
        rollBtn.textContent = "Bet";
        rollBtn.style.backgroundColor = "";
        resultPopup.classList.remove("show");
      }, 1500);
    }, 100);
  }

  if (slider) {
    slider.addEventListener("input", (event) => {
      gameState.target = parseInt(event.target.value, 10);
      resultMarker?.classList.remove("show");
      updateMath();
      updateVisuals();
    });
  }

  if (betInput) {
    betInput.value = gameState.currentBet.toFixed(2);
    betInput.addEventListener("input", (event) => {
      let val = parseFloat(event.target.value);
      if (Number.isNaN(val) || val < 0) val = 0;
      gameState.currentBet = val;
      updateProfit();
    });
  }

  if (btnModeOver && btnModeUnder) {
    btnModeOver.addEventListener("click", () => {
      gameState.isRollOver = true;
      btnModeOver.classList.add("active");
      btnModeUnder.classList.remove("active");
      updateMath();
      updateVisuals();
    });

    btnModeUnder.addEventListener("click", () => {
      gameState.isRollOver = false;
      btnModeUnder.classList.add("active");
      btnModeOver.classList.remove("active");
      updateMath();
      updateVisuals();
    });
  }

  if (btnHalf && betInput) {
    btnHalf.addEventListener("click", () => {
      const val = parseFloat(betInput.value) || 0;
      betInput.value = (val / 2).toFixed(2);
      betInput.dispatchEvent(new Event("input"));
    });
  }

  if (btnDouble && betInput) {
    btnDouble.addEventListener("click", () => {
      const val = parseFloat(betInput.value) || 0;
      betInput.value = (val * 2).toFixed(2);
      betInput.dispatchEvent(new Event("input"));
    });
  }

  if (rollBtn) {
    rollBtn.addEventListener("click", rollDice);
  }

  updateMath();
  updateVisuals();

  activeCasinoCleanup = () => {
    destroyed = true;
    resetPendingTimers();
    container.classList.remove("casino-fullbleed", "dice-fullbleed");
  };
}

function loadSlide() {
  const container = document.getElementById("casino-container");
  if (!container) return;
  container.classList.add("casino-fullbleed", "slide-fullbleed");

  container.innerHTML = `
    <div class="slide-root">
      <div class="sidebar">
        <h2 style="margin-top: 0">Slide</h2>
        <p class="sidebar-subtext">Set your bet and spin for a random multiplier.</p>

        <div class="control-group">
          <label for="bet-input">Bet Amount</label>
          <div class="input-box">
            <input type="number" id="bet-input" value="10" min="0.01" step="0.01" />
          </div>
        </div>

        <div class="quick-bet-row" aria-label="Quick bet controls">
          <button type="button" class="quick-bet-btn" data-action="half">1/2</button>
          <button type="button" class="quick-bet-btn" data-action="double">2x</button>
          <button type="button" class="quick-bet-btn" data-action="max">Max</button>
        </div>

        <div class="slide-stats-panel" aria-label="Slide stats">
          <div class="slide-stat-line">
            <span class="k">Last Hit</span>
            <span class="v" id="slide-last-hit">-</span>
          </div>
          <div class="slide-stat-line">
            <span class="k">Best Hit</span>
            <span class="v" id="slide-best-hit">0.00x</span>
          </div>
          <div class="slide-stat-line">
            <span class="k">Win Rate</span>
            <span class="v" id="slide-win-rate">0%</span>
          </div>
          <div class="slide-stat-line">
            <span class="k">Net</span>
            <span class="v" id="slide-net">$0.00</span>
          </div>
        </div>

        <div class="slide-auto-panel" aria-label="Auto settings">
          <div class="slide-auto-head">Auto</div>
          <div class="slide-auto-grid">
            <label for="auto-delay-input">Delay (ms)</label>
            <input type="number" id="auto-delay-input" value="900" min="200" max="5000" step="100" />
            <label for="auto-rounds-input">Rounds (0 = ∞)</label>
            <input type="number" id="auto-rounds-input" value="0" min="0" step="1" />
            <label for="auto-profit-input">Stop Profit ($)</label>
            <input type="number" id="auto-profit-input" value="0" min="0" step="0.01" />
            <label for="auto-loss-input">Stop Loss ($)</label>
            <input type="number" id="auto-loss-input" value="0" min="0" step="0.01" />
          </div>
          <button id="auto-btn" type="button" class="auto-btn">Start Auto</button>
          <p id="auto-status" class="auto-status">Auto is off.</p>
        </div>

        <button id="bet-btn" type="button">Bet</button>
      </div>

      <div class="main-content">
        <div class="top-bar">
          <div class="history-row" id="history-container"></div>
          <div class="balance-display">
            Balance: <span id="balance">1000.00</span>
          </div>
        </div>

        <div class="game-area" id="game-area">
          <div class="center-line" aria-hidden="true"></div>
          <div class="center-marker" aria-hidden="true"></div>
          <div class="track-viewport" id="track-viewport" aria-label="Multiplier track">
            <div class="slide-track" id="track"></div>
          </div>
          <div id="result-message" role="status" aria-live="polite"></div>
        </div>
      </div>
    </div>
  `;

  let destroyed = false;
  let transitionEndHandler = null;

  let balance = cash;

  const trackViewport = container.querySelector("#track-viewport");
  const track = container.querySelector("#track");
  const balanceSpan = container.querySelector("#balance");
  const betBtn = container.querySelector("#bet-btn");
  const autoBtn = container.querySelector("#auto-btn");
  const resultMsg = container.querySelector("#result-message");
  const historyContainer = container.querySelector("#history-container");
  const betInput = container.querySelector("#bet-input");
  const autoDelayInput = container.querySelector("#auto-delay-input");
  const autoRoundsInput = container.querySelector("#auto-rounds-input");
  const autoProfitInput = container.querySelector("#auto-profit-input");
  const autoLossInput = container.querySelector("#auto-loss-input");
  const autoStatus = container.querySelector("#auto-status");
  const slideRoot = container.querySelector(".slide-root");
  if (IS_PHONE_EMBED_MODE) {
    addSageBrand(slideRoot, "top-left");
    addSageBrand(slideRoot, "bottom-left");
  } else {
    const gameAreaEl = container.querySelector("#game-area");
    if (gameAreaEl) {
      const topBrand = document.createElement("div");
      topBrand.className = "slide-line-brand slide-line-brand-top";
      topBrand.setAttribute("aria-hidden", "true");
      topBrand.textContent = "SAGE";

      const bottomBrand = document.createElement("div");
      bottomBrand.className = "slide-line-brand slide-line-brand-bottom";
      bottomBrand.setAttribute("aria-hidden", "true");
      bottomBrand.textContent = "SAGE";

      gameAreaEl.append(topBrand, bottomBrand);
    }
  }
  const quickBetButtons = Array.from(container.querySelectorAll(".quick-bet-btn"));
  const lastHitEl = container.querySelector("#slide-last-hit");
  const bestHitEl = container.querySelector("#slide-best-hit");
  const winRateEl = container.querySelector("#slide-win-rate");
  const netEl = container.querySelector("#slide-net");

  const SPIN_DURATION_MS = 4200;
  const STRIP_SIZE = 90;
  const WIN_INDEX_MIN = 55;
  const WIN_INDEX_MAX = 72;
  const START_INDEX = 4;

  const startingBalance = balance;
  let roundsPlayed = 0;
  let winsCount = 0;
  let bestMultiplier = 0;
  let lastMultiplier = null;
  let autoRunning = false;
  let autoRoundsPlayed = 0;
  let autoBaseBalance = balance;
  let autoConfig = null;
  let autoTimerId = null;
  let isSpinning = false;

  const LOOT_TABLE = [
    { mult: 0, weight: 24 },
    { mult: 0.1, weight: 20 },
    { mult: 0.2, weight: 16 },
    { mult: 0.3, weight: 14 },
    { mult: 0.5, weight: 12 },
    { mult: 0.75, weight: 11 },
    { mult: 1, weight: 11 },
    { mult: 1.25, weight: 7 },
    { mult: 1.5, weight: 6 },
    { mult: 2, weight: 4.5 },
    { mult: 3, weight: 3 },
    { mult: 5, weight: 1.8 },
    { mult: 10, weight: 0.9 },
    { mult: 20, weight: 0.45 },
    { mult: 50, weight: 0.14 },
    { mult: 100, weight: 0.05, maxPerSpin: 1 },
    { mult: 200, weight: 0.025, maxPerSpin: 1 },
    { mult: 500, weight: 0.008, maxPerSpin: 1 },
    { mult: 1000, weight: 0.003, maxPerSpin: 1 },
    { mult: 50000, weight: 0.000015, maxPerSpin: 1 }
  ];

  function syncGlobalCash() {
    cash = Math.max(0, balance);
    updateUI();
    updateBlackjackCash();
  }

  function formatMult(mult) {
    if (Number.isInteger(mult)) return `${mult}x`;
    return `${mult.toFixed(2)}x`;
  }

  function randInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
  }

  function updateBalance(amount) {
    balance += amount;
    if (balanceSpan) balanceSpan.innerText = balance.toFixed(2);
    syncGlobalCash();
    updateSidebarStats();
  }

  function updateSidebarStats() {
    if (lastHitEl) lastHitEl.textContent = lastMultiplier == null ? "-" : formatMult(lastMultiplier);
    if (bestHitEl) bestHitEl.textContent = formatMult(bestMultiplier);
    if (winRateEl) {
      const rate = roundsPlayed > 0 ? (winsCount / roundsPlayed) * 100 : 0;
      winRateEl.textContent = `${rate.toFixed(0)}%`;
    }
    if (netEl) {
      const net = balance - startingBalance;
      netEl.textContent = `${net >= 0 ? "+" : "-"}$${Math.abs(net).toFixed(2)}`;
      netEl.style.color = net >= 0 ? "#00e701" : "#ff4444";
    }
  }

  function setAutoStatus(message) {
    if (autoStatus) autoStatus.textContent = message;
  }

  function clearAutoTimer() {
    if (autoTimerId) {
      window.clearTimeout(autoTimerId);
      autoTimerId = null;
    }
  }

  function parseAutoConfig() {
    const delayRaw = Number.parseInt(autoDelayInput?.value || "900", 10);
    const roundsRaw = Number.parseInt(autoRoundsInput?.value || "0", 10);
    const stopProfitRaw = Number.parseFloat(autoProfitInput?.value || "0");
    const stopLossRaw = Number.parseFloat(autoLossInput?.value || "0");

    return {
      delay: Math.max(200, Math.min(5000, Number.isFinite(delayRaw) ? delayRaw : 900)),
      rounds: Math.max(0, Number.isFinite(roundsRaw) ? roundsRaw : 0),
      stopProfit: Math.max(0, Number.isFinite(stopProfitRaw) ? stopProfitRaw : 0),
      stopLoss: Math.max(0, Number.isFinite(stopLossRaw) ? stopLossRaw : 0)
    };
  }

  function autoStopThresholdMet(config) {
    const net = Number((balance - autoBaseBalance).toFixed(2));
    const hitProfit = config.stopProfit > 0 && net >= config.stopProfit;
    const hitLoss = config.stopLoss > 0 && net <= -config.stopLoss;
    return hitProfit || hitLoss;
  }

  function updateAutoUi() {
    const lockConfig = autoRunning;
    if (autoDelayInput) autoDelayInput.disabled = lockConfig;
    if (autoRoundsInput) autoRoundsInput.disabled = lockConfig;
    if (autoProfitInput) autoProfitInput.disabled = lockConfig;
    if (autoLossInput) autoLossInput.disabled = lockConfig;
    if (autoBtn) {
      autoBtn.textContent = autoRunning ? "Stop Auto" : "Start Auto";
      autoBtn.classList.toggle("running", autoRunning);
    }
  }

  function stopAuto(message = "Auto stopped.") {
    if (!autoRunning) return;
    autoRunning = false;
    autoConfig = null;
    clearAutoTimer();
    updateAutoUi();
    setBetControlsDisabled(isSpinning);
    setAutoStatus(message);
  }

  function queueNextAutoRound() {
    if (!autoRunning || destroyed || !autoConfig) return;

    if (autoStopThresholdMet(autoConfig)) {
      stopAuto("Auto stopped: threshold reached.");
      return;
    }

    if (autoConfig.rounds > 0 && autoRoundsPlayed >= autoConfig.rounds) {
      stopAuto("Auto completed.");
      return;
    }

    clearAutoTimer();
    autoTimerId = window.setTimeout(() => {
      autoTimerId = null;
      if (!autoRunning || destroyed) return;
      const started = startGame({ fromAuto: true });
      if (!started) {
        stopAuto("Auto stopped.");
        return;
      }
      autoRoundsPlayed += 1;
    }, autoConfig.delay);
  }

  function startAuto() {
    if (destroyed || autoRunning) return;
    autoConfig = parseAutoConfig();
    autoRoundsPlayed = 0;
    autoBaseBalance = balance;
    autoRunning = true;
    updateAutoUi();
    setBetControlsDisabled(isSpinning);
    setAutoStatus(isSpinning ? "Auto queued after current spin." : "Auto running...");
    if (!isSpinning) queueNextAutoRound();
  }

  function toggleAuto() {
    if (autoRunning) {
      stopAuto("Auto stopped.");
      return;
    }
    startAuto();
  }

  function stopAutoIfInvalidRound(reason) {
    if (autoRunning) stopAuto(reason);
  }

  function validateBetAmount(fromAuto = false) {
    const betAmount = parseFloat(betInput?.value || "");
    if (Number.isNaN(betAmount) || betAmount <= 0) {
      if (!fromAuto) alert("Invalid bet.");
      else stopAutoIfInvalidRound("Auto stopped: invalid bet amount.");
      return null;
    }
    if (betAmount > balance) {
      if (!fromAuto) alert("Insufficient funds.");
      else stopAutoIfInvalidRound("Auto stopped: insufficient balance.");
      return null;
    }
    return betAmount;
  }

  function setBetControlsDisabled(disabled) {
    const lockBetControls = disabled || autoRunning;
    if (betBtn) betBtn.disabled = lockBetControls;
    if (betInput) betInput.disabled = lockBetControls;
    quickBetButtons.forEach((button) => {
      button.disabled = lockBetControls;
    });
  }

  function applyQuickBet(action) {
    if (!betInput || betInput.disabled) return;
    const currentValue = Number.parseFloat(betInput.value || "0");
    const current = Number.isFinite(currentValue) ? currentValue : 0;

    if (action === "half") {
      betInput.value = Math.max(0.01, current * 0.5).toFixed(2);
      return;
    }

    if (action === "double") {
      betInput.value = Math.max(0.01, current * 2).toFixed(2);
      return;
    }

    if (action === "max") {
      betInput.value = Math.max(0.01, balance).toFixed(2);
    }
  }

  function clearLandedStyles() {
    track?.querySelectorAll(".card.landed").forEach((el) => el.classList.remove("landed"));
  }

  function weightedPick(items) {
    let total = 0;
    for (const item of items) total += item.weight;
    let randomValue = Math.random() * total;
    for (const item of items) {
      randomValue -= item.weight;
      if (randomValue <= 0) return item;
    }
    return items[items.length - 1];
  }

  function pickFromLootTable(counts) {
    for (let attempts = 0; attempts < 20; attempts++) {
      const picked = weightedPick(LOOT_TABLE);
      const key = String(picked.mult);
      const current = counts.get(key) ?? 0;
      const cap = picked.maxPerSpin ?? Infinity;
      if (current < cap) {
        counts.set(key, current + 1);
        return picked.mult;
      }
    }

    counts.set("0.1", (counts.get("0.1") ?? 0) + 1);
    return 0.1;
  }

  function generateStrip(count) {
    const counts = new Map();
    const strip = [];
    for (let i = 0; i < count; i++) {
      strip.push(pickFromLootTable(counts));
    }
    return strip;
  }

  function createCard(multiplier) {
    const div = document.createElement("div");
    div.className = "card";
    div.dataset.multiplier = String(multiplier);

    if (multiplier >= 50) div.classList.add("is-gold");
    else if (multiplier >= 2) div.classList.add("is-blue");
    else div.classList.add("is-grey");

    const span = document.createElement("span");
    span.innerText = formatMult(multiplier);
    div.appendChild(span);

    return div;
  }

  function getCardMetrics() {
    const styles = getComputedStyle(slideRoot || document.documentElement);
    const cardWidth = parseFloat(styles.getPropertyValue("--card-width")) || 120;
    const cardGap = parseFloat(styles.getPropertyValue("--card-gap")) || 10;
    return { cardWidth, cardGap, totalCardWidth: cardWidth + cardGap };
  }

  function positionTrackAtIndex(index) {
    if (!trackViewport || !track) return;
    const { cardWidth, totalCardWidth } = getCardMetrics();
    const centerPoint = trackViewport.clientWidth / 2;
    track.style.transition = "none";
    track.style.transform = `translateX(${centerPoint - (index * totalCardWidth + cardWidth / 2)}px)`;
  }

  function renderStaticTrack() {
    if (!track) return;
    track.innerHTML = "";
    const strip = generateStrip(24);
    for (const mult of strip) track.appendChild(createCard(mult));
    positionTrackAtIndex(START_INDEX);
  }

  function getCardAtCenter() {
    if (!track || !trackViewport) return null;

    const cards = track.querySelectorAll(".card");
    if (!cards.length) return null;

    const viewportRect = trackViewport.getBoundingClientRect();
    const centerX = viewportRect.left + viewportRect.width / 2;

    let bestCard = null;
    let bestDiff = Infinity;

    cards.forEach((card) => {
      const rect = card.getBoundingClientRect();
      const cardCenterX = rect.left + rect.width / 2;
      const diff = Math.abs(cardCenterX - centerX);
      if (diff < bestDiff) {
        bestDiff = diff;
        bestCard = card;
      }
    });

    if (!bestCard) return null;
    const mult = parseFloat(bestCard.dataset.multiplier || "1");
    return { card: bestCard, multiplier: mult };
  }

  function addToHistory(multiplier) {
    if (!historyContainer) return;

    const pill = document.createElement("div");
    pill.className = "history-pill";
    pill.innerText = formatMult(multiplier);

    if (multiplier > 1) pill.classList.add("pill-win");
    else if (multiplier < 1) pill.classList.add("pill-loss");

    historyContainer.prepend(pill);
    while (historyContainer.children.length > 8) {
      historyContainer.removeChild(historyContainer.lastChild);
    }
  }

  function finishGame(multiplier, betAmount) {
    if (destroyed) return;
    isSpinning = false;

    const payout = betAmount * multiplier;
    const profit = payout - betAmount;
    lastMultiplier = multiplier;
    bestMultiplier = Math.max(bestMultiplier, multiplier);
    if (multiplier > 1) winsCount += 1;
    updateBalance(payout);
    if (profit > 0) showCasinoWinPopup({ amount: profit, multiplier });

    const isWin = multiplier > 1;
    const isPush = multiplier === 1;
    if (multiplier < 1) playGameSound("loss");

    if (resultMsg) {
      if (isWin) {
        resultMsg.textContent = `HIT ${formatMult(multiplier)}`;
        resultMsg.style.color = "#00e701";
      } else if (isPush) {
        resultMsg.textContent = `HIT ${formatMult(multiplier)}`;
        resultMsg.style.color = "#8b9bb4";
      } else {
        resultMsg.textContent = `HIT ${formatMult(multiplier)}`;
        resultMsg.style.color = "#ff4444";
      }

      resultMsg.style.opacity = "1";
    }

    addToHistory(multiplier);
    setBetControlsDisabled(false);
    triggerCasinoKickoutCheckAfterRound();

    if (autoRunning) {
      queueNextAutoRound();
    }
  }

  function startGame(options = {}) {
    if (destroyed || isSpinning || !track || !trackViewport || !betBtn || !betInput) return false;
    if (!ensureCasinoBettingAllowedNow()) return false;
    const fromAuto = Boolean(options.fromAuto);
    playGameSound("slide_spin", { restart: true, allowOverlap: false });
    const betAmount = validateBetAmount(fromAuto);
    if (betAmount == null) return false;

    roundsPlayed += 1;
    updateBalance(-betAmount);
    setBetControlsDisabled(true);
    isSpinning = true;
    if (resultMsg) resultMsg.style.opacity = "0";
    clearLandedStyles();

    const multipliers = generateStrip(STRIP_SIZE);
    const winningIndex = randInt(WIN_INDEX_MIN, Math.min(WIN_INDEX_MAX, multipliers.length - 1));
    if (fromAuto && Math.random() < 0.58) {
      const autoLowOutcomes = [0, 0, 0.1, 0.1, 0.2, 0.2, 0.3, 0.5, 0.75, 1];
      multipliers[winningIndex] = autoLowOutcomes[Math.floor(Math.random() * autoLowOutcomes.length)];
    }
    if (shouldRigHighBet(betAmount, 1.1)) {
      const lowOutcomes = [0, 0.1, 0.2, 0.3, 0.5, 0.75, 1];
      multipliers[winningIndex] = lowOutcomes[Math.floor(Math.random() * lowOutcomes.length)];
    }

    track.innerHTML = "";
    track.style.transition = "none";

    multipliers.forEach((mult) => track.appendChild(createCard(mult)));

    const { cardWidth, totalCardWidth } = getCardMetrics();
    const centerPoint = trackViewport.clientWidth / 2;
    const startTranslate = centerPoint - (START_INDEX * totalCardWidth + cardWidth / 2);
    track.style.transform = `translateX(${startTranslate}px)`;

    const finalTranslate = centerPoint - (winningIndex * totalCardWidth + cardWidth / 2);

    track.offsetHeight;
    track.style.transition = `transform ${SPIN_DURATION_MS}ms cubic-bezier(0.12, 0.92, 0.08, 1)`;
    track.style.transform = `translateX(${finalTranslate}px)`;

    if (transitionEndHandler) {
      track.removeEventListener("transitionend", transitionEndHandler);
      transitionEndHandler = null;
    }

    transitionEndHandler = () => {
      if (destroyed) return;
      const landed = getCardAtCenter();
      if (!landed) {
        isSpinning = false;
        setBetControlsDisabled(false);
        stopAutoIfInvalidRound("Auto stopped: spin failed.");
        return false;
      }
      landed.card.classList.add("landed");
      finishGame(landed.multiplier, betAmount);
      transitionEndHandler = null;
    };

    track.addEventListener("transitionend", transitionEndHandler, { once: true });
    return true;
  }

  function handleResize() {
    if (destroyed || !betBtn || betBtn.disabled || isSpinning) return;
    positionTrackAtIndex(START_INDEX);
  }

  if (betBtn) {
    betBtn.addEventListener("pointerdown", () => {
      if (destroyed || isSpinning) return;
      playGameSound("slide_spin", { restart: true, allowOverlap: false });
    });
    betBtn.addEventListener("click", startGame);
  }
  if (autoBtn) {
    autoBtn.addEventListener("click", toggleAuto);
  }
  quickBetButtons.forEach((button) => {
    button.addEventListener("click", () => {
      applyQuickBet(button.dataset.action || "");
    });
  });

  window.addEventListener("resize", handleResize);

  updateSidebarStats();
  updateAutoUi();
  setAutoStatus("Auto is off.");
  updateBalance(0);
  renderStaticTrack();

  activeCasinoCleanup = () => {
    destroyed = true;
    clearAutoTimer();
    autoRunning = false;
    window.removeEventListener("resize", handleResize);
    if (track && transitionEndHandler) {
      track.removeEventListener("transitionend", transitionEndHandler);
      transitionEndHandler = null;
    }
    container.classList.remove("casino-fullbleed", "slide-fullbleed");
  };
}

function loadHorseRacing() {
  const container = document.getElementById("casino-container");
  if (!container) return;
  container.classList.add("casino-fullbleed", "horse-fullbleed");
  const showHistoryPanel = !IS_PHONE_EMBED_MODE;

  container.innerHTML = `
    <main id="casino-horserace-screen"${showHistoryPanel ? "" : ' class="phone-no-history"'}>
      <section class="horserace-root" aria-label="Horse Racing Game">
        ${IS_PHONE_EMBED_MODE
          ? '<div id="balanceValue" class="hr-balance-source" aria-hidden="true">$0.00</div>'
          : `<header class="hr-header">
          <h1>Horse Racing</h1>
          <div class="hr-balance">Balance: <strong id="balanceValue">$0.00</strong></div>
        </header>`}

        <section class="hr-layout">
          <aside class="hr-controls card" aria-label="Betting controls">
            <div class="field">
              <label for="betInput">Bet Amount</label>
              <input id="betInput" type="number" min="0.01" step="0.01" value="10" inputmode="decimal" placeholder="Enter bet" />
            </div>

            <div id="horseOptions" class="horse-options" aria-label="Horse selection"></div>

            <div class="hr-buttons">
              <button id="startBtn" type="button">START RACE</button>
              <button id="clearBtn" type="button" class="secondary">CLEAR BET</button>
              <button id="rebetBtn" type="button" class="alt">REBET</button>
            </div>

            <p id="statusText" class="status">Select one horse and place a bet.</p>
          </aside>

          <section class="hr-race card" aria-label="Race track">
            <div class="race-hud">
              <span id="raceState">READY</span>
              <span>Leader: <strong id="leaderValue">-</strong></span>
            </div>

            <div class="track-wrap">
              <div class="finish-line" aria-hidden="true"></div>
              <div id="lanes" class="lanes"></div>
            </div>
          </section>

          ${showHistoryPanel
            ? `
          <aside class="hr-history card" aria-label="Race history">
            <h2>Last 10 Races</h2>
            <div id="historyList" class="history-list"></div>
          </aside>`
            : ""}
        </section>

        <section id="resultPanel" class="result-panel" hidden aria-live="polite">
          <h3 id="resultTitle">Race Complete</h3>
          <p id="resultLine"></p>
          <p id="resultNet"></p>
          <button id="playAgainBtn" type="button">Play Again</button>
        </section>
        <div class="sage-horse-footer" aria-hidden="true">SAGE</div>
      </section>
    </main>
  `;

  const STORAGE_KEYS = {
    history: "horse_race_history_v1"
  };

  const HISTORY_LIMIT = 10;
  const PROFILE_SEGMENT_SEC = 0.2;
  const HORSE_COUNT = 6;
  const MAX_SHARED_ODDS = 3;
  const ODDS_LADDER = ["2/1", "3/1", "5/1", "8/1", "15/1", "30/1"];
  const HORSE_COLORS = ["#ff6b4a", "#4cc9f0", "#ffd166", "#80ed99", "#c77dff", "#f28482"];
  const RADIO_GROUP = `horse-pick-${Math.random().toString(36).slice(2, 8)}`;

  const root = container.querySelector("#casino-horserace-screen .horserace-root");
  const els = {
    balanceValue: container.querySelector("#balanceValue"),
    betInput: container.querySelector("#betInput"),
    horseOptions: container.querySelector("#horseOptions"),
    startBtn: container.querySelector("#startBtn"),
    clearBtn: container.querySelector("#clearBtn"),
    rebetBtn: container.querySelector("#rebetBtn"),
    statusText: container.querySelector("#statusText"),
    raceState: container.querySelector("#raceState"),
    leaderValue: container.querySelector("#leaderValue"),
    lanes: container.querySelector("#lanes"),
    historyList: container.querySelector("#historyList"),
    resultPanel: container.querySelector("#resultPanel"),
    resultTitle: container.querySelector("#resultTitle"),
    resultLine: container.querySelector("#resultLine"),
    resultNet: container.querySelector("#resultNet"),
    playAgainBtn: container.querySelector("#playAgainBtn")
  };

  const state = {
    balance: roundCurrency(cash),
    racing: false,
    startPending: false,
    startTimerId: null,
    rafId: null,
    race: null,
    leaderId: null,
    runwayMotion: {
      offset: 0,
      speed: 0,
      targetSpeed: 0,
      lastTs: 0
    },
    history: [],
    lastBet: null,
    destroyed: false
  };

  const horses = Array.from({ length: HORSE_COUNT }, (_, index) => ({
    id: index + 1,
    name: `Horse ${index + 1}`,
    strength: 0,
    prob: 0,
    multiplier: 0,
    impliedChance: 0,
    fractionalOdds: "2/1",
    color: HORSE_COLORS[index],
    optionInput: null,
    runwayEl: null,
    horseEl: null,
    finishPx: 0,
    positionPx: 0
  }));
  const horseById = new Map(horses.map((horse) => [horse.id, horse]));
  const cleanupFns = [];

  function addCleanup(fn) {
    cleanupFns.push(fn);
  }

  function bind(target, event, handler, options) {
    if (!target) return;
    target.addEventListener(event, handler, options);
    addCleanup(() => target.removeEventListener(event, handler, options));
  }

  function roundMoney(value) {
    return Math.round((Number(value) || 0) * 100) / 100;
  }

  function formatCurrency(value) {
    return `${CURRENCY_SYMBOL}${roundMoney(value).toFixed(2)}`;
  }

  function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
  }

  function rand(min, max) {
    return min + Math.random() * (max - min);
  }

  function randInt(min, max) {
    return Math.floor(rand(min, max + 1));
  }

  function smoothStep(value) {
    return value * value * (3 - 2 * value);
  }

  function parseFractionalOddsLabel(label) {
    const [numRaw, denRaw] = String(label).split("/");
    const num = Number(numRaw);
    const den = Number(denRaw);
    if (!Number.isFinite(num) || !Number.isFinite(den) || den <= 0) return null;
    return { num, den };
  }

  function toDecimalFromFraction(label) {
    const parsed = parseFractionalOddsLabel(label);
    if (!parsed) return 2;
    return parsed.num / parsed.den + 1;
  }

  function pickRandom(array) {
    return array[Math.floor(Math.random() * array.length)];
  }

  function syncGlobalCash() {
    cash = Math.max(0, roundMoney(state.balance));
    updateUI();
    updateBlackjackCash();
  }

  function assignHorseOdds() {
    const usageByOdds = new Map();
    const profiles = ODDS_LADDER.map((label) => ({
      label,
      decimal: toDecimalFromFraction(label),
      impliedChance: 1 / toDecimalFromFraction(label)
    }));

    horses.forEach((horse) => {
      const availableProfiles = profiles.filter((profile) => (usageByOdds.get(profile.label) || 0) < MAX_SHARED_ODDS);
      const profile = pickRandom(availableProfiles.length ? availableProfiles : profiles);

      usageByOdds.set(profile.label, (usageByOdds.get(profile.label) || 0) + 1);
      horse.fractionalOdds = profile.label === "1/1" ? "Evens" : profile.label;
      horse.multiplier = profile.decimal;
      horse.impliedChance = profile.impliedChance;
      horse.strength = profile.impliedChance;
    });

    const totalStrength = horses.reduce((sum, horse) => sum + horse.strength, 0) || 1;
    horses.forEach((horse) => {
      horse.prob = horse.strength / totalStrength;
    });
  }

  function setStatus(message) {
    if (els.statusText) els.statusText.textContent = message;
  }

  function setRaceState(text) {
    if (els.raceState) els.raceState.textContent = text;
  }

  function getSelectedHorseId() {
    const checked = els.horseOptions?.querySelector(`input[name="${RADIO_GROUP}"]:checked`);
    if (!checked) return null;
    const id = Number(checked.value);
    return Number.isFinite(id) ? id : null;
  }

  function getBetAmount() {
    const raw = Number(els.betInput?.value);
    if (!Number.isFinite(raw)) return NaN;
    return roundMoney(raw);
  }

  function updateBalanceView() {
    if (els.balanceValue) els.balanceValue.textContent = formatCurrency(state.balance);
  }

  function setControlsDisabled(disabled) {
    root?.classList.toggle("is-racing", disabled);
    if (els.betInput) els.betInput.disabled = disabled;
    horses.forEach((horse) => {
      if (horse.optionInput) horse.optionInput.disabled = disabled;
    });
    updateButtons();
  }

  function saveHistory() {
    try {
      localStorage.setItem(STORAGE_KEYS.history, JSON.stringify(state.history.slice(0, HISTORY_LIMIT)));
    } catch {}
  }

  function loadStorage() {
    try {
      const rawHistory = localStorage.getItem(STORAGE_KEYS.history);
      if (!rawHistory) return;
      const parsedHistory = JSON.parse(rawHistory);
      if (!Array.isArray(parsedHistory)) return;
      state.history = parsedHistory.slice(0, HISTORY_LIMIT).map((entry) => ({
        winnerId: Number(entry.winnerId) || 1,
        pickId: Number(entry.pickId) || 1,
        bet: roundMoney(Number(entry.bet) || 0),
        net: roundMoney(Number(entry.net) || 0)
      }));
    } catch {}
  }

  function updateButtons() {
    const pickId = getSelectedHorseId();
    const bet = getBetAmount();
    const betValid = Number.isFinite(bet) && bet > 0 && bet <= state.balance;
    if (els.startBtn) els.startBtn.disabled = state.racing || state.startPending || !pickId || !betValid;
    if (els.clearBtn) els.clearBtn.disabled = state.racing;

    const canRebet =
      !state.racing &&
      Boolean(state.lastBet) &&
      state.lastBet.amount > 0 &&
      state.lastBet.amount <= state.balance;
    if (els.rebetBtn) els.rebetBtn.disabled = !canRebet;
  }

  function renderHorseOptions() {
    if (!els.horseOptions) return;
    els.horseOptions.innerHTML = "";
    horses.forEach((horse) => {
      const card = document.createElement("label");
      card.className = "horse-card";
      card.innerHTML = `
        <input type="radio" name="${RADIO_GROUP}" value="${horse.id}" />
        <span class="horse-main">
          <span class="horse-chip" style="--horse-color:${horse.color}"></span>
          <span class="horse-name">${horse.name}</span>
        </span>
        <span class="horse-odds">${horse.fractionalOdds}</span>
        <span class="horse-mult">${horse.multiplier.toFixed(2)}x • ${(horse.impliedChance * 100).toFixed(1)}%</span>
      `;
      horse.optionInput = card.querySelector("input");
      els.horseOptions.append(card);
    });
  }

  function renderLanes() {
    if (!els.lanes) return;
    els.lanes.innerHTML = "";
    horses.forEach((horse) => {
      const lane = document.createElement("div");
      lane.className = "lane";
      lane.innerHTML = `
        <div class="lane-head">
          <span class="horse-chip" style="--horse-color:${horse.color}"></span>
          <span>${horse.name}</span>
          <span>${horse.fractionalOdds}</span>
        </div>
        <div class="runway">
          <div class="horse" style="--horse-color:${horse.color}">
            <span class="horse-emoji" aria-hidden="true">🐎</span>
            <span class="horse-id">${horse.id}</span>
          </div>
        </div>
      `;
      horse.runwayEl = lane.querySelector(".runway");
      horse.horseEl = lane.querySelector(".horse");
      els.lanes.append(lane);
    });
    updateLaneMetrics();
  }

  function updateLaneMetrics() {
    horses.forEach((horse) => {
      if (!horse.runwayEl || !horse.horseEl) return;
      const runwayHeight = Math.max(24, horse.runwayEl.clientHeight || 52);
      const bodyHeight = clamp(Math.round(runwayHeight - 8), 26, 46);
      const bodyTop = Math.max(1, Math.round((runwayHeight - bodyHeight) / 2));
      const fontPx = clamp(Math.round(bodyHeight * 0.58), 16, 28);
      const minWidth = clamp(Math.round(bodyHeight * 1.9), 58, 92);
      const padX = clamp(Math.round(bodyHeight * 0.22), 6, 10);

      horse.horseEl.style.height = `${bodyHeight}px`;
      horse.horseEl.style.top = `${bodyTop}px`;
      horse.horseEl.style.fontSize = `${fontPx}px`;
      horse.horseEl.style.minWidth = `${minWidth}px`;
      horse.horseEl.style.padding = `0 ${padX}px`;

      const runwayWidth = horse.runwayEl.clientWidth;
      const horseWidth = horse.horseEl.offsetWidth || horse.horseEl.clientWidth || 46;
      horse.finishPx = Math.max(120, runwayWidth - horseWidth - 16);

      if (!state.racing) {
        horse.positionPx = 0;
        horse.horseEl.style.transform = "translate3d(0px, 0px, 0px)";
      }
    });
  }

  function renderHistory() {
    if (!els.historyList) return;
    els.historyList.innerHTML = "";
    if (state.history.length === 0) {
      const empty = document.createElement("div");
      empty.className = "history-empty";
      empty.textContent = "No races yet.";
      els.historyList.append(empty);
      return;
    }

    state.history.forEach((item) => {
      const row = document.createElement("div");
      row.className = `history-item ${item.net >= 0 ? "win" : "loss"}`;
      row.innerHTML = `
        <span>W: H${item.winnerId}</span>
        <span>P: H${item.pickId}</span>
        <span>Bet ${formatCurrency(item.bet)}</span>
        <span>${item.net >= 0 ? "+" : "-"}${formatCurrency(Math.abs(item.net))}</span>
      `;
      els.historyList.append(row);
    });
  }

  function setResultVisible(visible) {
    if (els.resultPanel) els.resultPanel.hidden = !visible;
  }

  function weightedWinnerPick() {
    const roll = Math.random();
    let cumulative = 0;
    for (const horse of horses) {
      cumulative += horse.prob;
      if (roll <= cumulative) return horse;
    }
    return horses[horses.length - 1];
  }

  function buildPaceProfile(segmentCount, role) {
    const pace = [];
    let momentum = rand(0.86, 1.12);
    const burstA = randInt(2, Math.max(3, segmentCount - 5));
    const burstB = randInt(4, Math.max(5, segmentCount - 2));
    const dip = randInt(3, Math.max(4, segmentCount - 4));

    for (let i = 0; i < segmentCount; i += 1) {
      const phase = i / Math.max(1, segmentCount - 1);
      momentum += rand(-0.12, 0.12);
      momentum = clamp(momentum * 0.985 + rand(-0.06, 0.08), 0.5, 1.58);

      let value = momentum;
      if (Math.abs(i - burstA) <= 1) value += rand(0.13, 0.34);
      if (Math.abs(i - burstB) <= 1) value += rand(0.08, 0.22);
      if (Math.abs(i - dip) <= 1) value -= rand(0.1, 0.24);

      if (role === "winner") {
        value += phase > 0.56 ? 0.22 * ((phase - 0.56) / 0.44) : -0.045;
      } else {
        value += phase < 0.24 ? 0.05 : 0;
        value -= phase > 0.72 ? 0.04 * ((phase - 0.72) / 0.28) : 0;
      }

      pace.push(clamp(value, 0.42, 1.95));
    }

    return pace;
  }

  function buildRaceModel(winnerId, pickId, betAmount) {
    const winnerFinish = rand(5.1, 6.9);
    const finishTimes = new Map();
    finishTimes.set(winnerId, winnerFinish);

    const chasers = horses
      .filter((horse) => horse.id !== winnerId)
      .map((horse) => ({ ...horse, score: horse.strength + rand(-0.24, 0.24) }))
      .sort((a, b) => b.score - a.score);

    let offset = rand(0.08, 0.2);
    chasers.forEach((horse) => {
      offset += rand(0.16, 0.44);
      finishTimes.set(horse.id, winnerFinish + offset);
    });

    const models = new Map();
    let maxFinish = winnerFinish;
    let minFinish = Number.POSITIVE_INFINITY;

    horses.forEach((horse) => {
      const finishTime = finishTimes.get(horse.id) || winnerFinish + rand(0.3, 0.9);
      const segmentCount = Math.max(22, Math.round(finishTime / PROFILE_SEGMENT_SEC));
      const pace = buildPaceProfile(segmentCount, horse.id === winnerId ? "winner" : "chaser");
      const cumulative = [0];
      for (let i = 0; i < pace.length; i += 1) {
        cumulative.push(cumulative[cumulative.length - 1] + pace[i]);
      }
      const totalWeight = cumulative[cumulative.length - 1] || 1;

      models.set(horse.id, { finishTime, segmentCount, pace, cumulative, totalWeight });
      if (finishTime > maxFinish) maxFinish = finishTime;
      if (finishTime < minFinish) minFinish = finishTime;
    });

    return {
      winnerId,
      pickId,
      betAmount,
      startAt: performance.now(),
      firstFinishTime: Number.isFinite(minFinish) ? minFinish : winnerFinish,
      maxFinish,
      models
    };
  }

  function applyRacePositions(elapsedSec) {
    if (!state.race) return;

    let leaderId = null;
    let leaderPos = -1;

    horses.forEach((horse) => {
      const model = state.race.models.get(horse.id);
      if (!model || !horse.horseEl) return;

      let progress = 0;
      if (elapsedSec >= model.finishTime) {
        progress = 1;
      } else if (elapsedSec > 0) {
        const scaled = (elapsedSec / model.finishTime) * model.segmentCount;
        const idx = clamp(Math.floor(scaled), 0, model.segmentCount - 1);
        const local = clamp(scaled - idx, 0, 1);
        const weighted = model.cumulative[idx] + model.pace[idx] * smoothStep(local);
        progress = clamp(weighted / model.totalWeight, 0, 1);
      }

      const px = horse.finishPx * progress;
      horse.positionPx = px;
      const bob = Math.sin(elapsedSec * 8 + horse.id * 0.8) * 1.7;
      horse.horseEl.style.transform = `translate3d(${px.toFixed(2)}px, ${bob.toFixed(2)}px, 0px)`;
      horse.horseEl.classList.toggle("is-finished", elapsedSec >= model.finishTime - 0.02);

      if (px > leaderPos) {
        leaderPos = px;
        leaderId = horse.id;
      }
    });

    state.leaderId = leaderId;
    horses.forEach((horse) => {
      if (!horse.horseEl) return;
      horse.horseEl.classList.toggle("is-leader", horse.id === leaderId);
    });

    if (els.leaderValue) els.leaderValue.textContent = leaderId ? `Horse ${leaderId}` : "-";
  }

  function updateRunwayMotion(nowMs, shouldMoveFast) {
    const motion = state.runwayMotion;
    if (!motion.lastTs) motion.lastTs = nowMs;
    const dt = clamp((nowMs - motion.lastTs) / 1000, 0, 0.05);
    motion.lastTs = nowMs;
    motion.targetSpeed = shouldMoveFast ? 122 : 0;
    const response = shouldMoveFast ? 8.5 : 4.6;
    motion.speed += (motion.targetSpeed - motion.speed) * Math.min(1, dt * response);
    motion.offset -= motion.speed * dt;

    const bgX = `${motion.offset.toFixed(2)}px`;
    horses.forEach((horse) => {
      if (!horse.runwayEl) return;
      horse.runwayEl.style.backgroundPosition = `${bgX} 0px, 0px 0px`;
    });
  }

  function resetRunwayMotion() {
    state.runwayMotion.offset = 0;
    state.runwayMotion.speed = 0;
    state.runwayMotion.targetSpeed = 0;
    state.runwayMotion.lastTs = 0;
    horses.forEach((horse) => {
      if (!horse.runwayEl) return;
      horse.runwayEl.style.backgroundPosition = "0px 0px, 0px 0px";
    });
  }

  function stopRaceSilently() {
    if (state.rafId) {
      cancelAnimationFrame(state.rafId);
      state.rafId = null;
    }
    state.racing = false;
    state.startPending = false;
    state.race = null;
    state.leaderId = null;
    if (state.startTimerId) {
      clearTimeout(state.startTimerId);
      state.startTimerId = null;
    }
    resetRunwayMotion();
  }

  function finishRace() {
    if (!state.race) return;

    const race = state.race;
    const winner = horseById.get(race.winnerId);
    const picked = horseById.get(race.pickId);
    if (!winner || !picked) return;

    const won = winner.id === picked.id;
    const payout = won ? roundMoney(race.betAmount * picked.multiplier) : 0;
    const net = won ? roundMoney(payout - race.betAmount) : -race.betAmount;

    if (won) {
      state.balance = roundMoney(state.balance + payout);
      if (net > 0) showCasinoWinPopup({ amount: net, multiplier: picked.multiplier });
    } else {
      playGameSound("loss");
    }

    state.history.unshift({ winnerId: winner.id, pickId: picked.id, bet: race.betAmount, net });
    if (state.history.length > HISTORY_LIMIT) state.history.length = HISTORY_LIMIT;
    saveHistory();

    stopRaceSilently();
    assignHorseOdds();
    renderHorseOptions();
    renderLanes();
    setControlsDisabled(false);
    updateBalanceView();
    renderHistory();
    updateButtons();
    syncGlobalCash();
    triggerCasinoKickoutCheckAfterRound();

    setRaceState("RACE COMPLETE");
    if (els.leaderValue) els.leaderValue.textContent = `Horse ${winner.id}`;
    if (els.resultTitle) els.resultTitle.textContent = `${winner.name} wins!`;
    if (els.resultLine) {
      els.resultLine.textContent = won
        ? `You picked ${picked.name}. Payout: ${formatCurrency(payout)} (${picked.fractionalOdds}, ${picked.multiplier.toFixed(2)}x)`
        : `You picked ${picked.name}. Better luck next race.`;
    }
    if (els.resultNet) {
      els.resultNet.textContent = `${won ? "+" : "-"}${formatCurrency(Math.abs(net))}`;
      els.resultNet.classList.toggle("win", won);
      els.resultNet.classList.toggle("loss", !won);
    }

    setResultVisible(true);
    setStatus(won ? `Winner: ${winner.name}. Nice hit.` : `Winner: ${winner.name}.`);
  }

  function raceLoop(now) {
    if (state.destroyed || !state.racing || !state.race) return;

    const elapsed = Math.max(0, (now - state.race.startAt) / 1000);
    updateRunwayMotion(now, elapsed < state.race.firstFinishTime);
    applyRacePositions(elapsed);

    if (elapsed >= state.race.maxFinish + 0.24) {
      finishRace();
      return;
    }

    state.rafId = requestAnimationFrame(raceLoop);
  }

  function startRace() {
    if (state.racing || state.startPending || state.destroyed) return;
    if (!ensureCasinoBettingAllowedNow()) return;

    const pickId = getSelectedHorseId();
    if (!pickId) {
      setStatus("Pick one horse before starting.");
      return;
    }

    const bet = getBetAmount();
    if (!Number.isFinite(bet) || bet <= 0) {
      setStatus("Enter a valid bet amount greater than 0.");
      return;
    }
    if (bet > state.balance) {
      setStatus("Insufficient balance for that bet.");
      return;
    }
    state.startPending = true;
    setControlsDisabled(true);
    setRaceState("STARTING");
    setStatus("Race starting...");
    updateButtons();
    playGameSound("horse_start", { restart: true, allowOverlap: false });

    state.startTimerId = window.setTimeout(() => {
      state.startTimerId = null;
      if (state.destroyed) return;
      state.startPending = false;
      state.lastBet = { horseId: pickId, amount: bet };
      state.balance = roundMoney(state.balance - bet);
      updateBalanceView();
      syncGlobalCash();

      updateLaneMetrics();
      const winner = weightedWinnerPick();
      state.race = buildRaceModel(winner.id, pickId, bet);
      state.racing = true;

      setResultVisible(false);
      setRaceState("RACE IN PROGRESS");
      if (els.leaderValue) els.leaderValue.textContent = "-";
      setStatus("Race started. Watch the live leader as the horses battle for first.");

      setControlsDisabled(true);
      resetRunwayMotion();
      applyRacePositions(0);
      state.rafId = requestAnimationFrame(raceLoop);
    }, 420);
  }

  function clearBet() {
    if (state.racing || state.destroyed) return;
    if (els.betInput) els.betInput.value = "10";
    const checked = els.horseOptions?.querySelector(`input[name="${RADIO_GROUP}"]:checked`);
    if (checked) checked.checked = false;
    setResultVisible(false);
    setRaceState("READY");
    if (els.leaderValue) els.leaderValue.textContent = "-";
    setStatus("Bet cleared.");
    updateButtons();
  }

  function rebet() {
    if (state.racing || state.destroyed || !state.lastBet) return;
    if (state.lastBet.amount > state.balance) {
      setStatus("Insufficient balance to rebet.");
      return;
    }
    if (els.betInput) els.betInput.value = state.lastBet.amount.toFixed(2);
    const radio = els.horseOptions?.querySelector(`input[name="${RADIO_GROUP}"][value="${state.lastBet.horseId}"]`);
    if (radio) radio.checked = true;
    setResultVisible(false);
    setStatus(`Rebet loaded: Horse ${state.lastBet.horseId} for ${formatCurrency(state.lastBet.amount)}.`);
    updateButtons();
  }

  function playAgain() {
    if (state.destroyed) return;
    setResultVisible(false);
    setRaceState("READY");
    if (els.leaderValue) els.leaderValue.textContent = "-";
    setStatus("Place your next bet.");
  }

  function weightedWinnerPick() {
    const roll = Math.random();
    let cumulative = 0;
    for (const horse of horses) {
      cumulative += horse.prob;
      if (roll <= cumulative) return horse;
    }
    return horses[horses.length - 1];
  }

  function init() {
    loadStorage();
    assignHorseOdds();
    renderHorseOptions();
    renderLanes();
    renderHistory();
    updateBalanceView();
    updateButtons();
    setResultVisible(false);

    bind(els.betInput, "input", () => {
      const raw = Number(els.betInput.value);
      if (Number.isFinite(raw) && raw > 0) {
        els.betInput.value = String(clamp(roundMoney(raw), 0, 999999999));
      }
      updateButtons();
    });
    bind(els.horseOptions, "change", updateButtons);
    bind(els.startBtn, "click", startRace);
    bind(els.clearBtn, "click", clearBet);
    bind(els.rebetBtn, "click", rebet);
    bind(els.playAgainBtn, "click", playAgain);
    bind(window, "resize", updateLaneMetrics);
    bind(window, "beforeunload", stopRaceSilently);
    bind(document, "visibilitychange", () => {
      if (document.visibilityState === "hidden") stopRaceSilently();
    });
  }

  init();

  activeCasinoCleanup = () => {
    state.destroyed = true;
    stopRaceSilently();
    cleanupFns.forEach((fn) => fn());
    container.classList.remove("casino-fullbleed", "horse-fullbleed");
  };
}

function loadDiamondMatch() {
  const container = document.getElementById("casino-container");
  if (!container) return;
  container.classList.add("casino-fullbleed", "diamond-fullbleed");

  container.innerHTML = `
    <div class="diamond-root">
      <main class="app">
        <section class="panel panel-bet">
          <h1 class="title">Diamond Match</h1>
          <p class="subtitle">Place your bet and reveal 5 gems instantly.</p>

          <div class="mode-toggle" role="group" aria-label="Game mode">
            <button type="button" id="manualBtn" class="mode-btn active" aria-pressed="true">Manual</button>
            <button type="button" id="autoBtn" class="mode-btn" aria-pressed="false">Auto</button>
          </div>

          <div class="stats">
            <div class="stat-card">
              <span class="label">Balance</span>
              <strong id="balance">$0.00</strong>
            </div>
            <div class="stat-card">
              <span class="label">Win Streak</span>
              <strong id="streak">0</strong>
            </div>
          </div>

          <label class="bet-label" for="betInput">Bet Amount</label>
          <div class="bet-row">
            <input id="betInput" type="number" min="1" step="0.01" value="10">
            <button type="button" class="chip-btn" data-chip="half">1/2</button>
            <button type="button" class="chip-btn" data-chip="double">2x</button>
            <button type="button" class="chip-btn" data-chip="max">Max</button>
          </div>

          <div class="controls">
            <button id="playBtn" class="btn btn-play" type="button">Bet</button>
            ${IS_PHONE_EMBED_MODE ? '<button id="resetBtn" class="btn btn-reset" type="button">Reset Balance</button>' : ""}
          </div>

          <p id="message" class="message">Ready for next round.</p>
        </section>

        <section class="panel panel-game">
          <div class="result-header">
            <div>
              <p class="small-label">Result</p>
              <h2 id="comboName">-</h2>
            </div>
            <div class="last-payout">
              <p class="small-label">Profit</p>
              <strong id="lastPayout">$0.00</strong>
            </div>
          </div>

          <section class="payout-ladder">
            <p class="small-label table-label">Payout Ladder</p>
            <div id="payoutRows" class="payout-rows"></div>
          </section>

          <div class="diamond-grid" id="diamondGrid" aria-live="polite"></div>

          <div class="history">
            <p class="small-label">Last 10 Rounds</p>
            <ol id="historyList"></ol>
          </div>
        </section>
      </main>
    </div>
  `;
  addSageBrand(container.querySelector(".diamond-root"), "bottom-left");

  const STARTING_BALANCE = Math.max(1, Number(cash.toFixed(2)));
  const REVEAL_DELAY_MS = 140;
  const AUTO_ROUND_GAP_MS = 650;
  const ACTIVE_TYPE_COUNT = 16;

  const DIAMOND_TYPES = [
    { name: "Ruby", color: "#ff2d55" },
    { name: "Carnelian", color: "#ff7a1a" },
    { name: "Citrine", color: "#ffbf00" },
    { name: "Yellow Sapphire", color: "#ffe600" },
    { name: "Peridot", color: "#b2d732" },
    { name: "Emerald", color: "#00c853" },
    { name: "Turquoise", color: "#00b8a9" },
    { name: "Aquamarine", color: "#4dd0e1" },
    { name: "Sapphire", color: "#1565c0" },
    { name: "Tanzanite", color: "#6a4cff" },
    { name: "Amethyst", color: "#9c4dff" },
    { name: "Rose Quartz", color: "#ff82b2" },
    { name: "Garnet", color: "#b71c4a" },
    { name: "Coral", color: "#ff6f61" },
    { name: "Moonstone", color: "#dfe7f2" },
    { name: "Onyx", color: "#2f3542" },
    { name: "Jade", color: "#00a86b" },
    { name: "Opal", color: "#b7fff2" },
    { name: "Blue Topaz", color: "#4f8cff" },
    { name: "Pink Tourmaline", color: "#ff2fa5" },
    { name: "Sunstone", color: "#c77d3b" },
    { name: "Hematite", color: "#6d7787" },
    { name: "Lapis Lazuli", color: "#2c3fa8" },
    { name: "Malachite", color: "#1f9d6a" }
  ];

  const PAYOUTS = [
    { name: "No Match", multiplier: 0 },
    { name: "Pair", multiplier: 0.5 },
    { name: "Two Pair", multiplier: 2 },
    { name: "Three of a Kind", multiplier: 3 },
    { name: "Full House", multiplier: 5 },
    { name: "Four of a Kind", multiplier: 10 },
    { name: "Five of a Kind", multiplier: 50 }
  ];

  const ui = {
    balance: container.querySelector("#balance"),
    streak: container.querySelector("#streak"),
    manualBtn: container.querySelector("#manualBtn"),
    betInput: container.querySelector("#betInput"),
    playBtn: container.querySelector("#playBtn"),
    autoBtn: container.querySelector("#autoBtn"),
    resetBtn: container.querySelector("#resetBtn"),
    message: container.querySelector("#message"),
    comboName: container.querySelector("#comboName"),
    lastPayout: container.querySelector("#lastPayout"),
    diamondGrid: container.querySelector("#diamondGrid"),
    historyList: container.querySelector("#historyList"),
    payoutRows: container.querySelector("#payoutRows"),
    chipButtons: [...container.querySelectorAll(".chip-btn")]
  };

  const game = {
    balance: STARTING_BALANCE,
    state: "idle",
    autoPlay: false,
    streak: 0,
    lastPayout: 0,
    history: [],
    round: 0
  };

  let destroyed = false;
  let roundSettleTimer = null;

  function syncGlobalCash() {
    cash = Math.max(0, Number(game.balance.toFixed(2)));
    updateUI();
    updateBlackjackCash();
  }

  function currency(value) {
    return `$${value.toFixed(2)}`;
  }

  function clampBet(value) {
    if (!Number.isFinite(value) || value <= 0) return 1;
    return Math.floor(value * 100) / 100;
  }

  function getBet() {
    return clampBet(Number.parseFloat(ui.betInput.value));
  }

  function setMessage(text) {
    if (!ui.message) return;
    ui.message.textContent = text;
  }

  function updateTopUi() {
    ui.balance.textContent = currency(game.balance);
    ui.streak.textContent = String(game.streak);
    ui.lastPayout.textContent = currency(game.lastPayout);
  }

  function updateControls() {
    const busy = game.state !== "idle";
    ui.playBtn.disabled = busy || game.autoPlay;
    ui.betInput.disabled = busy;
    if (ui.resetBtn) ui.resetBtn.disabled = busy;
    ui.autoBtn.disabled = false;
    ui.manualBtn.disabled = false;
    ui.chipButtons.forEach((button) => {
      button.disabled = busy;
    });
    ui.autoBtn.classList.toggle("active", game.autoPlay);
    ui.manualBtn.classList.toggle("active", !game.autoPlay);
    ui.autoBtn.setAttribute("aria-pressed", String(game.autoPlay));
    ui.manualBtn.setAttribute("aria-pressed", String(!game.autoPlay));
  }

  function comboPattern(name) {
    if (name === "Five of a Kind") return ["purple", "purple", "purple", "purple", "purple"];
    if (name === "Four of a Kind") return ["purple", "purple", "purple", "purple", "muted"];
    if (name === "Full House") return ["purple", "purple", "purple", "red", "red"];
    if (name === "Three of a Kind") return ["purple", "purple", "purple", "muted", "muted"];
    if (name === "Two Pair") return ["purple", "purple", "red", "red", "muted"];
    if (name === "Pair") return ["purple", "purple", "muted", "muted", "muted"];
    return ["muted", "muted", "muted", "muted", "muted"];
  }

  function renderPayoutLadder() {
    ui.payoutRows.innerHTML = [...PAYOUTS]
      .reverse()
      .map((row) => {
        const icons = comboPattern(row.name)
          .map((value) => {
            if (value === "purple") return '<span class="mini-diamond filled filled-purple"></span>';
            if (value === "red") return '<span class="mini-diamond filled filled-red"></span>';
            return '<span class="mini-diamond"></span>';
          })
          .join("");
        return `<div class="payout-row"><div class="payout-left"><div class="mini-diamonds">${icons}</div><span class="payout-name">${row.name}</span></div><span class="payout-multi">${row.multiplier.toFixed(2)}x</span></div>`;
      })
      .join("");
  }

  function makeSlot() {
    const slot = document.createElement("div");
    slot.className = "slot placeholder";

    const diamond = document.createElement("div");
    diamond.className = "diamond";
    diamond.setAttribute("aria-hidden", "true");

    const stand = document.createElement("div");
    stand.className = "stand";

    slot.append(diamond, stand);
    return slot;
  }

  function renderPlaceholderBoard() {
    ui.diamondGrid.innerHTML = "";
    for (let i = 0; i < 5; i += 1) {
      ui.diamondGrid.appendChild(makeSlot());
    }
  }

  function adjustHex(hex, amount) {
    const value = hex.replace("#", "");
    const channels = [0, 2, 4].map((idx) => {
      const channel = Number.parseInt(value.slice(idx, idx + 2), 16);
      return Math.max(0, Math.min(255, channel + amount));
    });
    return `rgb(${channels[0]} ${channels[1]} ${channels[2]})`;
  }

  function gemGradient(hex) {
    return [
      "linear-gradient(132deg, rgba(255,255,255,0.52), rgba(255,255,255,0) 44%)",
      `conic-gradient(from 220deg at 52% 52%, ${adjustHex(hex, -52)}, ${hex}, ${adjustHex(hex, 35)}, ${adjustHex(hex, -25)})`
    ].join(", ");
  }

  function revealAt(index, typeIndex) {
    const slot = ui.diamondGrid.children[index];
    if (!slot) return;

    const type = DIAMOND_TYPES[typeIndex];
    const diamond = slot.querySelector(".diamond");
    diamond.style.background = gemGradient(type.color);
    diamond.style.filter = `drop-shadow(0 0 10px ${adjustHex(type.color, 30)})`;
    slot.dataset.color = type.color;
    slot.classList.remove("placeholder");
    slot.classList.add("revealed");
    slot.title = type.name;
  }

  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  function freshRoll() {
    const roll = [];
    const poolSize = Math.min(ACTIVE_TYPE_COUNT, DIAMOND_TYPES.length);
    for (let i = 0; i < 5; i += 1) {
      roll.push(Math.floor(Math.random() * poolSize));
    }
    return roll;
  }

  function freshLosingRoll() {
    for (let attempt = 0; attempt < 50; attempt += 1) {
      const candidate = freshRoll();
      if (evaluateRoll(candidate).multiplier <= 0) return candidate;
    }

    const poolSize = Math.min(ACTIVE_TYPE_COUNT, DIAMOND_TYPES.length);
    if (poolSize >= 5) return [0, 1, 2, 3, 4];

    return [0, 1, 2, 3, 0].map((value) => value % Math.max(1, poolSize));
  }

  function evaluateRoll(roll) {
    const positionsByType = new Map();

    roll.forEach((type, index) => {
      if (!positionsByType.has(type)) positionsByType.set(type, []);
      positionsByType.get(type).push(index);
    });

    const groups = [...positionsByType.values()].sort((a, b) => b.length - a.length);
    const lengths = groups.map((group) => group.length);
    const max = lengths[0] || 0;

    if (max === 5) return { name: "Five of a Kind", multiplier: 50, indices: groups[0] };
    if (max === 4) return { name: "Four of a Kind", multiplier: 10, indices: groups[0] };
    if (lengths[0] === 3 && lengths[1] === 2) return { name: "Full House", multiplier: 5, indices: groups[0].concat(groups[1]) };
    if (max === 3) return { name: "Three of a Kind", multiplier: 3, indices: groups[0] };
    if (lengths[0] === 2 && lengths[1] === 2) return { name: "Two Pair", multiplier: 2, indices: groups[0].concat(groups[1]) };
    if (max === 2) return { name: "Pair", multiplier: 0.5, indices: groups[0] };
    return { name: "No Match", multiplier: 0, indices: [] };
  }

  function highlight(indices) {
    [...ui.diamondGrid.children].forEach((slot, index) => {
      slot.classList.toggle("match", indices.includes(index));
    });
  }

  function paintWinningStands(roll, winningIndices, isWin) {
    [...ui.diamondGrid.children].forEach((slot) => {
      slot.style.background = "";
      slot.style.borderColor = "";
      slot.style.boxShadow = "";
      const stand = slot.querySelector(".stand");
      stand.style.background = "";
      stand.style.borderColor = "";
      stand.style.boxShadow = "";
    });

    if (!isWin) return;

    winningIndices.forEach((index) => {
      const slot = ui.diamondGrid.children[index];
      if (!slot) return;
      const stand = slot.querySelector(".stand");
      const color = DIAMOND_TYPES[roll[index]].color;
      slot.style.background = `linear-gradient(180deg, ${adjustHex(color, 10)}, ${adjustHex(color, -46)})`;
      slot.style.borderColor = adjustHex(color, -32);
      slot.style.boxShadow = `0 0 0 1px ${adjustHex(color, -8)}, 0 0 18px ${adjustHex(color, -4)}`;
      stand.style.background = `linear-gradient(180deg, ${adjustHex(color, 30)}, ${adjustHex(color, -22)})`;
      stand.style.borderColor = adjustHex(color, -26);
      stand.style.boxShadow = `0 0 10px ${adjustHex(color, 4)}`;
    });
  }

  function pushHistory(entry) {
    game.history.unshift(entry);
    if (game.history.length > 10) game.history.pop();
    ui.historyList.innerHTML = game.history
      .map((row) => {
        const signClass = row.payout > 0 ? "gain" : "loss";
        const typeNames = row.roll.map((value) => DIAMOND_TYPES[value].name).join(", ");
        return `<li>#${row.round} ${row.name} (${row.multiplier}x) <span class="${signClass}">${currency(row.payout)}</span> [${typeNames}]</li>`;
      })
      .join("");
  }

  function validateBet() {
    const bet = getBet();
    ui.betInput.value = String(bet);
    if (bet > game.balance) {
      setMessage("Bet exceeds your balance.");
      return null;
    }
    return bet;
  }

  async function playRound() {
    if (destroyed || game.state !== "idle") return;
    if (!ensureCasinoBettingAllowedNow()) return;

    const bet = validateBet();
    if (!bet) return;

    game.state = "rolling";
    updateControls();
    game.balance = Number((game.balance - bet).toFixed(2));
    game.round += 1;
    game.lastPayout = 0;
    syncGlobalCash();
    updateTopUi();

    ui.comboName.textContent = "Revealing...";
    setMessage(`Round #${game.round}: rolling 5 diamonds...`);
    renderPlaceholderBoard();

    let roll = freshRoll();
    if (shouldRigHighBet(bet, 1.1) && evaluateRoll(roll).multiplier > 0) {
      roll = freshLosingRoll();
    }

    for (let i = 0; i < roll.length; i += 1) {
      await sleep(REVEAL_DELAY_MS);
      if (destroyed) return;
      revealAt(i, roll[i]);
    }

    const result = evaluateRoll(roll);
    const payout = Number((bet * result.multiplier).toFixed(2));
    const netProfit = payout - bet;
    game.balance = Number((game.balance + payout).toFixed(2));
    game.lastPayout = payout;
    game.streak = payout > 0 ? game.streak + 1 : 0;

    ui.comboName.textContent = `${result.name} (${result.multiplier}x)`;
    setMessage(
      payout > 0
        ? `${result.name} - ${result.multiplier}x Win! +${currency(payout)}`
        : "No payout this round."
    );
    highlight(result.indices);
    paintWinningStands(roll, result.indices, payout > 0);
    if (netProfit > 0) showCasinoWinPopup({ amount: netProfit, multiplier: result.multiplier });
    syncGlobalCash();
    updateTopUi();
    triggerCasinoKickoutCheckAfterRound();
    pushHistory({
      round: game.round,
      name: result.name,
      multiplier: result.multiplier,
      payout,
      roll
    });

    game.state = "result";
    updateControls();

    if (roundSettleTimer) {
      clearTimeout(roundSettleTimer);
      roundSettleTimer = null;
    }

    roundSettleTimer = setTimeout(() => {
      if (destroyed) return;
      game.state = "idle";
      updateControls();
      if (game.autoPlay) {
        if (getBet() > game.balance) {
          game.autoPlay = false;
          updateControls();
          setMessage("Auto-play stopped: balance too low for current bet.");
          return;
        }
        playRound();
      }
    }, AUTO_ROUND_GAP_MS);
  }

  function toggleAuto() {
    if (destroyed) return;

    game.autoPlay = !game.autoPlay;
    if (game.autoPlay && getBet() > game.balance) {
      game.autoPlay = false;
      setMessage("Auto-play not started: bet exceeds current balance.");
      updateControls();
      return;
    }

    updateControls();
    if (!game.autoPlay) {
      setMessage("Auto-play disabled.");
      return;
    }

    setMessage("Auto-play enabled.");
    if (game.state === "idle") playRound();
  }

  function setManualMode() {
    if (!game.autoPlay) return;
    game.autoPlay = false;
    updateControls();
    setMessage("Manual mode enabled.");
  }

  function resetGame() {
    game.balance = STARTING_BALANCE;
    game.state = "idle";
    game.autoPlay = false;
    game.streak = 0;
    game.lastPayout = 0;
    game.history = [];
    game.round = 0;

    if (roundSettleTimer) {
      clearTimeout(roundSettleTimer);
      roundSettleTimer = null;
    }

    ui.historyList.innerHTML = "";
    ui.comboName.textContent = "-";
    setMessage("Balance reset. Ready for next round.");
    syncGlobalCash();
    updateTopUi();
    updateControls();
    renderPlaceholderBoard();
  }

  function handleChipButton(event) {
    const value = event.currentTarget.dataset.chip;
    const current = getBet();

    if (value === "max") {
      ui.betInput.value = String(Math.max(1, Math.floor(game.balance * 100) / 100));
      return;
    }

    let next = current;
    if (value === "half") next = Math.max(1, current / 2);
    if (value === "double") next = current * 2;
    next = clampBet(next);
    ui.betInput.value = String(next);
  }

  function init() {
    renderPayoutLadder();
    renderPlaceholderBoard();
    syncGlobalCash();
    updateTopUi();
    updateControls();

    ui.playBtn.addEventListener("click", playRound);
    ui.autoBtn.addEventListener("click", toggleAuto);
    ui.manualBtn.addEventListener("click", setManualMode);
    if (ui.resetBtn) ui.resetBtn.addEventListener("click", resetGame);
    ui.betInput.addEventListener("change", () => {
      ui.betInput.value = String(getBet());
    });
    ui.chipButtons.forEach((button) => button.addEventListener("click", handleChipButton));
  }

  init();

  activeCasinoCleanup = () => {
    destroyed = true;
    if (roundSettleTimer) {
      clearTimeout(roundSettleTimer);
      roundSettleTimer = null;
    }
    game.autoPlay = false;
    container.classList.remove("casino-fullbleed", "diamond-fullbleed");
  };
}

function loadCrash() {
  const container = document.getElementById("casino-container");
  if (!container) return;

  container.innerHTML = `
    <div class="crash-root">
      <div id="app" class="state-idle">
        <main class="game-shell">
          <section class="left-panel">
            <h1>Crash</h1>

            <div class="card balance-card">
              <span>Balance</span>
              <strong id="balance">$0.00</strong>
            </div>

            <div class="card controls-card">
              <div class="mode-switch" role="group" aria-label="Bet mode">
                <button id="manualModeBtn" class="mode-btn active" type="button">Manual</button>
                <button id="autoModeBtn" class="mode-btn" type="button">Auto</button>
              </div>
              <input id="autoCashoutEnabled" class="visually-hidden" type="checkbox" checked />

              <label for="betAmount">Bet Amount</label>
              <div class="bet-row">
                <input id="betAmount" type="number" min="1" step="0.01" value="10" />
                <button id="halfBet" class="mini-btn" type="button">1/2</button>
                <button id="doubleBet" class="mini-btn" type="button">2x</button>
              </div>

              <div class="auto-row">
                <label class="toggle-label" for="autoCashoutAt">Auto Cashout At</label>
                <div class="auto-input-wrap">
                  <input id="autoCashoutAt" type="number" min="1.01" max="100000" step="0.01" value="6.00" />
                  <span>x</span>
                </div>
              </div>

              <div class="profit-row">
                <span>Profit on Win</span>
                <strong id="profitOnWin">$0.00</strong>
              </div>

              <p id="betHint" class="hint">Enter a bet and start a round.</p>

              <div class="button-row">
                <button id="placeBetBtn" class="primary-btn" type="button">Place Bet</button>
                <button id="cashOutBtn" class="cashout-btn hidden" type="button">Cash Out</button>
              </div>
            </div>

            <div class="card bet-feed-card">
              <div class="feed-top">
                <span><b id="feedCount">0</b> Live Bets</span>
                <strong id="feedTotal">$0.00</strong>
              </div>
              <ul id="feedList" class="feed-list"></ul>
            </div>
          </section>

          <section class="chart-panel">
            <div class="chart-header">
              <span id="stateBadge" class="badge">Idle</span>
              <span id="message" class="message">Place a bet to begin.</span>
            </div>

            <div class="top-history-wrap">
              <div id="topHistoryList" class="top-history-list"></div>
              <div class="user-pill">You</div>
            </div>

            <div class="display-area">
              <canvas id="curveCanvas" width="900" height="500" aria-hidden="true"></canvas>
              <div id="multiplierDisplay" class="multiplier">1.00×</div>
              <div id="crashOverlay" class="crash-overlay hidden">CRASHED</div>
              <ul id="sidePayouts" class="side-payouts"></ul>
            </div>
          </section>
        </main>
      </div>
    </div>
  `;
  addSageBrand(container.querySelector(".crash-root"), "bottom-left");

  const STARTING_BALANCE = Number(Math.max(0, cash).toFixed(2));
  const MIN_BET = 1;
  const HOUSE_EDGE = 0.01;
  const CRASH_SOFT_CURVE = 2.2;
  const CRASH_MULTIPLIER_CAP = 100000;
  const TIME_WINDOW_SECONDS = 8;
  const TIME_LEAD_SECONDS = 1.1;
  const CAMERA_X_DAMPING = 8.5;
  const CAMERA_Y_DAMPING = 6.5;
  const Y_RESCALE_TRIGGER = 0.95;
  const Y_RESCALE_INCREMENT = 0.1;
  const BOT_MIN_COUNT = 16;
  const BOT_MAX_COUNT = 30;
  const BOT_VISIBLE_ROWS = 5;
  const BOT_FEED_STEP_MS = 230;
  const BOT_BUST_CHANCE = 0.28;
  const SIDE_VISIBLE_ROWS = 5;
  const SIDE_FEED_STEP_MS = 340;

  const BOT_NAMES = [
    "Hidden",
    "Traviswins1",
    "bongskie16",
    "Mura",
    "Paramjit",
    "SundaramX",
    "AceRoll",
    "NovaBet",
    "RiskyRay",
    "FlashMint",
    "TideRun",
    "CoinPilot",
    "Orbital",
    "HexaCash"
  ];

  let destroyed = false;
  const pendingTimeouts = new Set();

  const crashAppEl = container.querySelector("#app");
  const balanceEl = container.querySelector("#balance");
  const betAmountEl = container.querySelector("#betAmount");
  const betHintEl = container.querySelector("#betHint");
  const placeBetBtn = container.querySelector("#placeBetBtn");
  const cashOutBtn = container.querySelector("#cashOutBtn");
  const halfBetBtn = container.querySelector("#halfBet");
  const doubleBetBtn = container.querySelector("#doubleBet");
  const manualModeBtn = container.querySelector("#manualModeBtn");
  const autoModeBtn = container.querySelector("#autoModeBtn");
  const autoCashoutEnabledEl = container.querySelector("#autoCashoutEnabled");
  const autoCashoutAtEl = container.querySelector("#autoCashoutAt");
  const profitOnWinEl = container.querySelector("#profitOnWin");
  const feedCountEl = container.querySelector("#feedCount");
  const feedTotalEl = container.querySelector("#feedTotal");
  const feedListEl = container.querySelector("#feedList");
  const sidePayoutsEl = container.querySelector("#sidePayouts");
  const multiplierDisplayEl = container.querySelector("#multiplierDisplay");
  const crashOverlayEl = container.querySelector("#crashOverlay");
  const stateBadgeEl = container.querySelector("#stateBadge");
  const messageEl = container.querySelector("#message");
  const topHistoryListEl = container.querySelector("#topHistoryList");
  const canvas = container.querySelector("#curveCanvas");
  if (!canvas) return;
  const ctx = canvas.getContext("2d");
  if (!ctx) return;

  const game = {
    state: "idle",
    balance: STARTING_BALANCE,
    currentBet: 0,
    multiplier: 1,
    crashPoint: 1,
    playerCashedOut: false,
    playerRoundOutcome: "neutral",
    botPlayers: [],
    botFeedQueue: [],
    botFeedProcessing: false,
    botFeedSession: 0,
    sideFeedQueue: [],
    sideFeedProcessing: false,
    sideFeedSession: 0,
    lastCrashPoint: null,
    rafId: null,
    roundStartMs: 0,
    lastFrameMs: 0,
    points: [{ t: 0, m: 1 }],
    crashHistory: [],
    usedCrashPoints: new Set(),
    autoCashoutEnabled: true,
    autoCashoutAt: 6,
    viewTMin: 0,
    viewMMax: 3,
    viewMMaxTarget: 3,
    canvasWidth: canvas.width,
    canvasHeight: canvas.height
  };

  function setSafeTimeout(callback, delay) {
    const timeoutId = window.setTimeout(() => {
      pendingTimeouts.delete(timeoutId);
      if (destroyed) return;
      callback();
    }, delay);
    pendingTimeouts.add(timeoutId);
    return timeoutId;
  }

  function clearAllTimeouts() {
    pendingTimeouts.forEach((timeoutId) => clearTimeout(timeoutId));
    pendingTimeouts.clear();
  }

  function syncGlobalCash() {
    cash = Number(Math.max(0, game.balance).toFixed(2));
    updateUI();
    updateBlackjackCash();
  }

  function formatMoney(amount) {
    return `${CURRENCY_SYMBOL}${amount.toLocaleString(undefined, {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    })}`;
  }

  function randomBetween(min, max) {
    return min + Math.random() * (max - min);
  }

  function createBotName() {
    const base = BOT_NAMES[Math.floor(Math.random() * BOT_NAMES.length)];
    if (base === "Hidden" || Math.random() < 0.45) return base;
    return `${base}${Math.floor(randomBetween(10, 99))}`;
  }

  function createBotWave(crashPoint) {
    const count = Math.floor(randomBetween(BOT_MIN_COUNT, BOT_MAX_COUNT + 1));
    const bots = [];

    for (let index = 0; index < count; index += 1) {
      const betSize = Math.round(randomBetween(12, 620) * (Math.random() < 0.18 ? 2.8 : 1));
      const strategyRoll = Math.random();
      let baseTarget = 1.05;

      if (strategyRoll < 0.45) baseTarget = randomBetween(1.05, 2.1);
      else if (strategyRoll < 0.78) baseTarget = randomBetween(1.3, 3.8);
      else if (strategyRoll < 0.93) baseTarget = randomBetween(1.8, 7.2);
      else baseTarget = randomBetween(3.2, 16);

      const crashAnchor = crashPoint * randomBetween(0.58, 1.22);
      let target =
        baseTarget * randomBetween(0.62, 0.92) + crashAnchor * randomBetween(0.38, 0.68);

      if (Math.random() < BOT_BUST_CHANCE) {
        target *= randomBetween(1.12, 1.55);
      }

      target = Number(Math.min(CRASH_MULTIPLIER_CAP, Math.max(1.01, target)).toFixed(2));

      bots.push({
        id: index + 1,
        name: createBotName(),
        bet: betSize,
        target,
        status: "live",
        payout: 0,
        cashoutMultiplier: null
      });
    }

    return bots;
  }

  function updateFeedSummary() {
    if (!feedCountEl || !feedTotalEl) return;
    const totalBots = game.botPlayers.length;
    const totalWon = game.botPlayers
      .filter((bot) => bot.status === "won")
      .reduce((sum, bot) => sum + bot.payout, 0);

    feedCountEl.textContent = String(totalBots);
    feedTotalEl.textContent = totalWon > 0 ? `+${formatMoney(totalWon)}` : "$0.00";
  }

  function buildSideRow(bot, status) {
    if (status !== "won") return null;

    const listItem = document.createElement("li");
    listItem.classList.add("won");
    listItem.appendChild(document.createTextNode(bot.name));

    const valueEl = document.createElement("strong");
    valueEl.textContent = `${bot.cashoutMultiplier.toFixed(2)}x`;
    listItem.appendChild(valueEl);

    return listItem;
  }

  function pushSideRow(entry) {
    if (!sidePayoutsEl) return;
    const row = buildSideRow(entry.bot, entry.status);
    if (!row) return;

    row.classList.add("side-enter");
    sidePayoutsEl.prepend(row);
    requestAnimationFrame(() => {
      if (!destroyed) row.classList.add("is-visible");
    });

    if (sidePayoutsEl.children.length > SIDE_VISIBLE_ROWS) {
      const last = sidePayoutsEl.lastElementChild;
      if (last) {
        last.classList.add("side-exit");
        setSafeTimeout(() => {
          if (last.parentElement === sidePayoutsEl) last.remove();
        }, SIDE_FEED_STEP_MS);
      }
    }
  }

  function drainSideQueue() {
    if (game.sideFeedProcessing) return;

    game.sideFeedProcessing = true;
    const session = game.sideFeedSession;

    const step = () => {
      if (destroyed || session !== game.sideFeedSession) {
        game.sideFeedProcessing = false;
        return;
      }
      const next = game.sideFeedQueue.shift();
      if (!next) {
        game.sideFeedProcessing = false;
        return;
      }
      pushSideRow(next);
      setSafeTimeout(step, SIDE_FEED_STEP_MS);
    };

    step();
  }

  function enqueueSideEntry(bot, status) {
    if (!sidePayoutsEl) return;
    game.sideFeedQueue.push({ bot, status });
    drainSideQueue();
  }

  function resetSideTicker() {
    if (!sidePayoutsEl) return;
    game.sideFeedSession += 1;
    game.sideFeedQueue = [];
    game.sideFeedProcessing = false;
    sidePayoutsEl.innerHTML = "";
  }

  function buildFeedRow(bot, status) {
    const listItem = document.createElement("li");
    listItem.classList.add(status);

    const nameEl = document.createElement("span");
    nameEl.textContent = bot.name;

    const multiEl = document.createElement("em");
    const amountEl = document.createElement("strong");

    if (status === "won") {
      multiEl.textContent = `${bot.cashoutMultiplier.toFixed(2)}x`;
      amountEl.textContent = `+${formatMoney(bot.payout)}`;
    } else if (status === "bust") {
      multiEl.textContent = "BUST";
      amountEl.textContent = `-${formatMoney(bot.bet)}`;
    } else {
      multiEl.textContent = "LIVE";
      amountEl.textContent = formatMoney(bot.bet);
    }

    listItem.append(nameEl, multiEl, amountEl);
    return listItem;
  }

  function pushFeedRow(entry) {
    if (!feedListEl) return;
    const row = buildFeedRow(entry.bot, entry.status);
    row.classList.add("feed-enter");
    feedListEl.prepend(row);
    requestAnimationFrame(() => {
      if (!destroyed) row.classList.add("is-visible");
    });

    if (feedListEl.children.length > BOT_VISIBLE_ROWS) {
      const last = feedListEl.lastElementChild;
      if (last) {
        last.classList.add("feed-exit");
        setSafeTimeout(() => {
          if (last.parentElement === feedListEl) last.remove();
        }, BOT_FEED_STEP_MS);
      }
    }
  }

  function drainFeedQueue() {
    if (game.botFeedProcessing) return;

    game.botFeedProcessing = true;
    const session = game.botFeedSession;

    const step = () => {
      if (destroyed || session !== game.botFeedSession) {
        game.botFeedProcessing = false;
        return;
      }
      const next = game.botFeedQueue.shift();
      if (!next) {
        game.botFeedProcessing = false;
        return;
      }
      pushFeedRow(next);
      setSafeTimeout(step, BOT_FEED_STEP_MS);
    };

    step();
  }

  function enqueueFeedEntry(bot, status) {
    game.botFeedQueue.push({ bot, status });
    drainFeedQueue();
  }

  function resetFeedTicker() {
    if (!feedListEl) return;
    game.botFeedSession += 1;
    game.botFeedQueue = [];
    game.botFeedProcessing = false;
    feedListEl.innerHTML = "";
    resetSideTicker();
  }

  function seedFeedTicker() {
    const initial = [...game.botPlayers]
      .sort((a, b) => a.target - b.target)
      .slice(0, BOT_VISIBLE_ROWS);
    initial.forEach((bot) => enqueueFeedEntry(bot, "live"));
  }

  function renderBotFeed() {
    if (!feedListEl) return;
    updateFeedSummary();

    if (game.botPlayers.length === 0 && feedListEl.children.length === 0) {
      const waitingRow = document.createElement("li");
      waitingRow.className = "live is-visible";
      waitingRow.innerHTML = "<span>Waiting...</span><em>--</em><strong>$0.00</strong>";
      feedListEl.appendChild(waitingRow);
    }
  }

  function processBotCashouts() {
    let changed = false;
    for (const bot of game.botPlayers) {
      if (bot.status !== "live") continue;
      if (game.multiplier >= bot.target && game.multiplier < game.crashPoint) {
        bot.status = "won";
        bot.cashoutMultiplier = bot.target;
        bot.payout = Number((bot.bet * bot.cashoutMultiplier).toFixed(2));
        enqueueFeedEntry(bot, "won");
        enqueueSideEntry(bot, "won");
        changed = true;
      }
    }
    if (changed) updateFeedSummary();
  }

  function bustRemainingBots() {
    let changed = false;
    for (const bot of game.botPlayers) {
      if (bot.status === "live") {
        bot.status = "bust";
        enqueueFeedEntry(bot, "bust");
        changed = true;
      }
    }
    if (changed) updateFeedSummary();
  }

  function setState(nextState) {
    crashAppEl.classList.remove("state-idle", "state-running", "state-cashed_out", "state-crashed");
    game.state = nextState;
    crashAppEl.classList.add(`state-${nextState}`);
    updateStateBadge();
    syncControls();
  }

  function updateStateBadge() {
    if (!stateBadgeEl) return;
    const labels = {
      idle: "Idle",
      running: "Running",
      cashed_out: "Cashed Out",
      crashed: "Crashed"
    };
    stateBadgeEl.textContent = labels[game.state] || "Idle";
  }

  function syncControls() {
    const isRunning = game.state === "running";
    const isIdle = game.state === "idle";
    const autoMode = !!autoCashoutEnabledEl.checked;

    betAmountEl.disabled = isRunning;
    halfBetBtn.disabled = isRunning;
    doubleBetBtn.disabled = isRunning;
    autoCashoutEnabledEl.disabled = isRunning;
    autoCashoutAtEl.disabled = isRunning || !autoMode;
    manualModeBtn.disabled = isRunning;
    autoModeBtn.disabled = isRunning;
    placeBetBtn.disabled = !isIdle;

    manualModeBtn.classList.toggle("active", !autoMode);
    autoModeBtn.classList.toggle("active", autoMode);

    const canCashOut = isRunning && !game.playerCashedOut;
    cashOutBtn.classList.toggle("hidden", !canCashOut);
    cashOutBtn.disabled = !canCashOut;
  }

  function updateBalanceDisplay() {
    if (!balanceEl) return;
    balanceEl.textContent = formatMoney(game.balance);
  }

  function updateMultiplierDisplay() {
    if (!multiplierDisplayEl) return;
    multiplierDisplayEl.textContent = `${game.multiplier.toFixed(2)}×`;
    const growScale = 1 + Math.min((game.multiplier - 1) * 0.03, 0.38);
    multiplierDisplayEl.style.transform = `scale(${growScale.toFixed(3)})`;
  }

  function updateMessage(text) {
    if (!messageEl) return;
    messageEl.textContent = text;
  }

  function updateHint(text, tone = "neutral") {
    if (!betHintEl) return;
    betHintEl.textContent = text;
    if (tone === "good") {
      betHintEl.style.color = "#9ff4c8";
      return;
    }
    if (tone === "bad") {
      betHintEl.style.color = "#ffafaf";
      return;
    }
    betHintEl.style.color = "#8eb0d6";
  }

  function setCanvasSize() {
    const ratio = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    const width = Math.max(320, Math.floor(rect.width));
    const height = Math.max(260, Math.floor(rect.height));

    canvas.width = Math.floor(width * ratio);
    canvas.height = Math.floor(height * ratio);
    ctx.setTransform(ratio, 0, 0, ratio, 0, 0);

    game.canvasWidth = width;
    game.canvasHeight = height;
  }

  function getNiceStep(rawStep) {
    if (!Number.isFinite(rawStep) || rawStep <= 0) return 0.1;
    const exponent = Math.floor(Math.log10(rawStep));
    const magnitude = Math.pow(10, exponent);
    const fraction = rawStep / magnitude;
    if (fraction <= 1) return 1 * magnitude;
    if (fraction <= 2) return 2 * magnitude;
    if (fraction <= 2.5) return 2.5 * magnitude;
    if (fraction <= 4) return 4 * magnitude;
    if (fraction <= 5) return 5 * magnitude;
    return 10 * magnitude;
  }

  function getStepDecimals(step) {
    let decimals = 0;
    let scaled = step;
    while (decimals < 3 && Math.abs(Math.round(scaled) - scaled) > 1e-6) {
      scaled *= 10;
      decimals += 1;
    }
    return decimals;
  }

  function buildYAxisTicks(minValue, maxValue) {
    const minLabels = 6;
    const maxLabels = 8;
    const targetLabels = 7;
    const safeMax = Math.max(maxValue, minValue + 0.1);
    const range = safeMax - minValue;
    let step = getNiceStep(range / Math.max(targetLabels - 1, 1));

    const makeTicks = (stepValue) => {
      const ticks = [minValue];
      const firstMajor = Math.ceil((minValue + 1e-9) / stepValue) * stepValue;
      for (let value = firstMajor; value <= safeMax + 1e-9; value += stepValue) {
        const rounded = Number(value.toFixed(6));
        if (Math.abs(rounded - minValue) > stepValue * 0.5) ticks.push(rounded);
      }
      if (ticks.length < 2) ticks.push(Number(safeMax.toFixed(2)));
      return ticks;
    };

    let ticks = makeTicks(step);
    while (ticks.length > maxLabels) {
      step = getNiceStep(step * 1.6);
      ticks = makeTicks(step);
    }
    while (ticks.length < minLabels) {
      const smallerStep = getNiceStep(step / 2.1);
      if (smallerStep === step) break;
      const denserTicks = makeTicks(smallerStep);
      if (denserTicks.length > maxLabels) break;
      step = smallerStep;
      ticks = denserTicks;
    }
    const yDecimals = getStepDecimals(step);
    return { ticks, yDecimals };
  }

  function drawGrid(left, top, plotWidth, plotHeight, yTicks, mMax) {
    ctx.save();
    ctx.strokeStyle = "rgba(158, 196, 232, 0.06)";
    ctx.lineWidth = 1;

    for (const value of yTicks) {
      const normalized = (value - 1) / Math.max(mMax - 1, 0.0001);
      const y = top + (1 - normalized) * plotHeight;
      ctx.beginPath();
      ctx.moveTo(left, y);
      ctx.lineTo(left + plotWidth, y);
      ctx.stroke();
    }

    for (let i = 0; i <= 5; i += 1) {
      const x = left + (plotWidth * i) / 5;
      ctx.beginPath();
      ctx.moveTo(x, top);
      ctx.lineTo(x, top + plotHeight);
      ctx.stroke();
    }

    ctx.restore();
  }

  function drawRoundedRect(x, y, width, height, radius) {
    const cornerRadius = Math.min(radius, width / 2, height / 2);
    ctx.beginPath();
    ctx.moveTo(x + cornerRadius, y);
    ctx.lineTo(x + width - cornerRadius, y);
    ctx.quadraticCurveTo(x + width, y, x + width, y + cornerRadius);
    ctx.lineTo(x + width, y + height - cornerRadius);
    ctx.quadraticCurveTo(x + width, y + height, x + width - cornerRadius, y + height);
    ctx.lineTo(x + cornerRadius, y + height);
    ctx.quadraticCurveTo(x, y + height, x, y + height - cornerRadius);
    ctx.lineTo(x, y + cornerRadius);
    ctx.quadraticCurveTo(x, y, x + cornerRadius, y);
    ctx.closePath();
  }

  function drawAxisLabels(left, top, plotWidth, plotHeight, tMin, tMax, mMax, yTicks, yDecimals) {
    ctx.save();
    ctx.font = "700 12px Segoe UI, sans-serif";

    for (const value of yTicks) {
      const normalized = (value - 1) / Math.max(mMax - 1, 0.0001);
      const centerY = top + (1 - normalized) * plotHeight;
      const label = `${value.toFixed(yDecimals)}x`;
      const boxWidth = Math.max(48, ctx.measureText(label).width + 16);
      const boxHeight = 24;
      const boxX = 12;
      const boxY = centerY - boxHeight / 2;

      ctx.fillStyle = "rgba(61, 86, 110, 0.82)";
      ctx.strokeStyle = "rgba(98, 125, 152, 0.62)";
      drawRoundedRect(boxX, boxY, boxWidth, boxHeight, 7);
      ctx.fill();
      ctx.stroke();

      ctx.fillStyle = "rgba(231, 242, 252, 0.95)";
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillText(label, boxX + boxWidth / 2, centerY);
    }

    ctx.fillStyle = "rgba(229, 239, 249, 0.92)";
    ctx.font = "700 14px Segoe UI, sans-serif";
    ctx.textBaseline = "alphabetic";
    for (let i = 0; i <= 5; i += 1) {
      const seconds = tMin + ((tMax - tMin) * i) / 5;
      const x = left + (plotWidth * i) / 5;
      if (i === 5) {
        ctx.textAlign = "right";
        ctx.fillText(`Total ${seconds.toFixed(0)}s`, left + plotWidth + 10, top + plotHeight + 30);
      } else {
        ctx.textAlign = "center";
        ctx.fillText(`${seconds.toFixed(0)}s`, x, top + plotHeight + 30);
      }
    }

    ctx.restore();
  }

  function traceSmoothCurve(points, startWithMove = true) {
    if (points.length < 2) return;
    if (startWithMove) ctx.moveTo(points[0].x, points[0].y);
    else ctx.lineTo(points[0].x, points[0].y);

    for (let index = 1; index < points.length - 1; index += 1) {
      const current = points[index];
      const next = points[index + 1];
      const nx = (current.x + next.x) / 2;
      const ny = (current.y + next.y) / 2;
      ctx.quadraticCurveTo(current.x, current.y, nx, ny);
    }

    const penultimate = points[points.length - 2];
    const last = points[points.length - 1];
    ctx.quadraticCurveTo(penultimate.x, penultimate.y, last.x, last.y);
  }

  function drawCurve(dt = 1 / 60) {
    const w = game.canvasWidth;
    const h = game.canvasHeight;
    const left = 74;
    const right = 30;
    const top = 26;
    const bottom = 48;
    const plotWidth = w - left - right;
    const plotHeight = h - top - bottom;

    ctx.clearRect(0, 0, w, h);

    if (game.points.length < 2) {
      const baseMMax = 3;
      const { ticks, yDecimals } = buildYAxisTicks(1, baseMMax);
      drawGrid(left, top, plotWidth, plotHeight, ticks, baseMMax);
      drawAxisLabels(left, top, plotWidth, plotHeight, 0, TIME_WINDOW_SECONDS, baseMMax, ticks, yDecimals);
      return;
    }

    const latest = game.points[game.points.length - 1];
    const targetTMin = Math.max(0, latest.t - (TIME_WINDOW_SECONDS - TIME_LEAD_SECONDS));
    const xLerp = 1 - Math.exp(-CAMERA_X_DAMPING * dt);
    if (game.state === "running") game.viewTMin += (targetTMin - game.viewTMin) * xLerp;
    else game.viewTMin = targetTMin;

    const tMin = Math.max(0, game.viewTMin);
    const tMax = tMin + TIME_WINDOW_SECONDS;
    const tSpan = tMax - tMin;

    if (latest.m > game.viewMMaxTarget * Y_RESCALE_TRIGGER) {
      const requiredTop = latest.m / Y_RESCALE_TRIGGER;
      while (game.viewMMaxTarget < requiredTop) {
        game.viewMMaxTarget = Number((game.viewMMaxTarget + Y_RESCALE_INCREMENT).toFixed(1));
      }
    }

    const yLerp = 1 - Math.exp(-CAMERA_Y_DAMPING * dt);
    game.viewMMax += (game.viewMMaxTarget - game.viewMMax) * yLerp;
    const mMax = Math.max(2.5, game.viewMMax);
    const { ticks, yDecimals } = buildYAxisTicks(1, mMax);

    drawGrid(left, top, plotWidth, plotHeight, ticks, mMax);

    const visiblePoints = [];
    let previousPoint = null;
    for (const point of game.points) {
      if (point.t < tMin) {
        previousPoint = point;
        continue;
      }
      if (point.t <= tMax) visiblePoints.push(point);
    }
    if (previousPoint) visiblePoints.unshift(previousPoint);

    if (visiblePoints.length < 2) {
      drawAxisLabels(left, top, plotWidth, plotHeight, tMin, tMax, mMax, ticks, yDecimals);
      return;
    }

    const mapped = visiblePoints.map((point) => {
      const normalizedT = Math.max(0, Math.min((point.t - tMin) / tSpan, 1));
      const normalizedM = Math.max(0, Math.min((point.m - 1) / (mMax - 1), 1));
      return {
        x: left + normalizedT * plotWidth,
        y: top + (1 - normalizedM) * plotHeight
      };
    });

    const first = mapped[0];
    const last = mapped[mapped.length - 1];
    const baseline = top + plotHeight;

    const areaGradient = ctx.createLinearGradient(0, top, 0, baseline);
    areaGradient.addColorStop(0, "rgba(255, 183, 38, 0.86)");
    areaGradient.addColorStop(1, "rgba(255, 138, 4, 0.72)");

    ctx.beginPath();
    ctx.moveTo(first.x, baseline);
    traceSmoothCurve(mapped, false);
    ctx.lineTo(last.x, baseline);
    ctx.closePath();
    ctx.fillStyle = areaGradient;
    ctx.fill();

    ctx.beginPath();
    traceSmoothCurve(mapped);
    ctx.lineCap = "round";
    ctx.lineJoin = "round";
    ctx.strokeStyle = "#eef6ff";
    ctx.lineWidth = 6;
    ctx.stroke();

    ctx.beginPath();
    ctx.arc(last.x, last.y, 8, 0, Math.PI * 2);
    ctx.fillStyle = game.state === "crashed" ? "#ff4d4d" : "#f7fcff";
    ctx.fill();

    drawAxisLabels(left, top, plotWidth, plotHeight, tMin, tMax, mMax, ticks, yDecimals);
  }

  function generateCrashPoint() {
    let crash = 1;
    let guard = 0;
    do {
      const randomBase = Math.random();
      const adjustedRandom = Math.pow(randomBase, CRASH_SOFT_CURVE);
      const heavyTailValue = (1 - HOUSE_EDGE) / (1 - adjustedRandom);
      crash = Number(Math.min(CRASH_MULTIPLIER_CAP, Math.max(1, heavyTailValue)).toFixed(4));
      guard += 1;
    } while ((crash === game.lastCrashPoint || game.usedCrashPoints.has(crash)) && guard < 500);

    game.lastCrashPoint = crash;
    game.usedCrashPoints.add(crash);
    return crash;
  }

  function addCrashToHistory(crashValue, outcome) {
    game.crashHistory.unshift({ value: crashValue, outcome });
    renderHistory();
  }

  function renderHistory() {
    if (!topHistoryListEl) return;
    topHistoryListEl.innerHTML = "";

    if (game.crashHistory.length === 0) {
      const chip = document.createElement("span");
      chip.className = "history-chip neutral";
      chip.textContent = "--";
      topHistoryListEl.appendChild(chip);
      return;
    }

    for (const entry of game.crashHistory) {
      const chip = document.createElement("span");
      chip.classList.add("history-chip");
      chip.classList.add(entry.outcome || "neutral");
      chip.textContent = `${entry.value.toFixed(2)}x`;
      topHistoryListEl.appendChild(chip);
    }
  }

  function clearRoundAnimation() {
    if (game.rafId !== null) {
      cancelAnimationFrame(game.rafId);
      game.rafId = null;
    }
  }

  function resetForIdleState() {
    clearRoundAnimation();
    game.currentBet = 0;
    game.multiplier = 1;
    game.crashPoint = 1;
    game.playerCashedOut = false;
    game.playerRoundOutcome = "neutral";
    game.points = [{ t: 0, m: 1 }];
    game.viewTMin = 0;
    game.viewMMax = 3;
    game.viewMMaxTarget = 3;

    crashOverlayEl.classList.add("hidden");
    crashAppEl.classList.remove("flash");

    setState("idle");
    updateMessage("Place a bet to begin.");
    updateHint("Enter a bet and start a round.");
    updateMultiplierDisplay();
    drawCurve();
  }

  function endRoundAsCrash() {
    clearRoundAnimation();
    bustRemainingBots();

    setState("crashed");
    crashOverlayEl.classList.remove("hidden");
    crashAppEl.classList.add("flash");

    const hadBet = game.currentBet > 0;
    const didWin = game.playerRoundOutcome === "win";
    updateMessage(`Crashed at ${game.crashPoint.toFixed(2)}x`);

    if (!hadBet) updateHint("No bet on this round.", "neutral");
    else if (didWin) updateHint("Round ended. You already cashed out.", "good");
    else updateHint(`You lost ${formatMoney(game.currentBet)}.`, "bad");

    const historyOutcome = !hadBet ? "neutral" : didWin ? "win" : "lose";
    addCrashToHistory(game.crashPoint, historyOutcome);
    if (hadBet && !didWin) {
      triggerCasinoKickoutCheckAfterRound();
    }

    drawCurve();
    setSafeTimeout(() => {
      if (game.state === "crashed") resetForIdleState();
    }, 1700);
  }

  function settleCashout(trigger = "manual") {
    if (game.state !== "running" || game.playerCashedOut) return;

    const payout = game.currentBet * game.multiplier;
    const netProfit = payout - game.currentBet;
    game.balance += payout;
    if (netProfit > 0) showCasinoWinPopup({ amount: netProfit, multiplier: game.multiplier });
    syncGlobalCash();
    updateBalanceDisplay();
    triggerCasinoKickoutCheckAfterRound();

    game.playerCashedOut = true;
    game.playerRoundOutcome = "win";
    crashAppEl.classList.add("state-cashed_out");
    syncControls();

    crashOverlayEl.classList.add("hidden");
    if (trigger === "auto") {
      updateMessage(`Auto-cashed at ${game.multiplier.toFixed(2)}x. Waiting for crash...`);
    } else {
      updateMessage(`Cashed out at ${game.multiplier.toFixed(2)}x. Waiting for crash...`);
    }
    updateHint(`Won ${formatMoney(payout)}.`, "good");
  }

  function tick(nowMs) {
    if (destroyed || game.state !== "running") return;

    const dt = Math.min((nowMs - game.lastFrameMs) / 1000, 0.05);
    const elapsed = (nowMs - game.roundStartMs) / 1000;

    let growthPerSecond = 0;
    if (game.multiplier < 2) growthPerSecond = 0.07 + 0.08 * (game.multiplier - 1);
    else growthPerSecond = 0.18 + 0.09 * Math.pow(game.multiplier - 2, 1.45);
    growthPerSecond = Math.min(growthPerSecond, 10);

    game.multiplier += growthPerSecond * dt;
    game.lastFrameMs = nowMs;
    if (game.multiplier >= game.crashPoint) game.multiplier = game.crashPoint;

    game.points.push({ t: elapsed, m: game.multiplier });
    if (game.points.length > 1600) game.points.shift();

    updateMultiplierDisplay();
    drawCurve(dt);
    processBotCashouts();

    if (game.multiplier >= game.crashPoint) {
      endRoundAsCrash();
      return;
    }

    if (!game.playerCashedOut && game.autoCashoutEnabled && game.multiplier >= game.autoCashoutAt) {
      settleCashout("auto");
    }

    game.rafId = requestAnimationFrame(tick);
  }

  function parseBetInput() {
    const bet = Number.parseFloat(betAmountEl.value);
    if (!Number.isFinite(bet)) return NaN;
    return Number(bet.toFixed(2));
  }

  function parseAutoCashoutInput() {
    const target = Number.parseFloat(autoCashoutAtEl.value);
    if (!Number.isFinite(target)) return NaN;
    return Number(target.toFixed(2));
  }

  function updateProjectedProfit() {
    if (!profitOnWinEl) return;
    const bet = parseBetInput();
    const target = parseAutoCashoutInput();
    if (!Number.isFinite(bet) || !Number.isFinite(target) || target <= 1) {
      profitOnWinEl.textContent = "$0.00";
      return;
    }
    const projected = Math.max(0, bet * target - bet);
    profitOnWinEl.textContent = formatMoney(projected);
  }

  function startRound() {
    if (destroyed || game.state !== "idle") return;
    if (!ensureCasinoBettingAllowedNow()) return;

    const bet = parseBetInput();
    if (!Number.isFinite(bet) || bet < MIN_BET) {
      updateHint(`Bet must be at least ${formatMoney(MIN_BET)}.`, "bad");
      return;
    }
    if (bet > game.balance) {
      updateHint("Insufficient balance for that bet.", "bad");
      return;
    }

    const autoEnabled = autoCashoutEnabledEl.checked;
    const autoTarget = parseAutoCashoutInput();
    if (autoEnabled) {
      if (!Number.isFinite(autoTarget) || autoTarget <= 1 || autoTarget > CRASH_MULTIPLIER_CAP) {
        updateHint(`Auto cashout must be between 1.01x and ${CRASH_MULTIPLIER_CAP.toLocaleString()}x.`, "bad");
        return;
      }
    }

    game.currentBet = bet;
    game.balance -= bet;
    game.multiplier = 1;
    game.crashPoint = generateCrashPoint();
    if (shouldRigHighBet(bet, 1.15)) {
      const forcedCrash = Number(randomBetween(1.01, 1.65).toFixed(4));
      game.crashPoint = Math.min(game.crashPoint, forcedCrash);
    }
    game.playerCashedOut = false;
    game.playerRoundOutcome = "lose";
    game.autoCashoutEnabled = autoEnabled;
    game.autoCashoutAt = autoEnabled ? autoTarget : 0;
    game.botPlayers = createBotWave(game.crashPoint);

    resetFeedTicker();
    renderBotFeed();
    seedFeedTicker();

    game.points = [{ t: 0, m: 1 }];
    game.viewTMin = 0;
    game.viewMMax = 3;
    game.viewMMaxTarget = 3;
    game.roundStartMs = performance.now();
    game.lastFrameMs = game.roundStartMs;

    syncGlobalCash();
    updateBalanceDisplay();
    if (game.autoCashoutEnabled) {
      updateHint(`Bet locked: ${formatMoney(bet)}. Auto cashout at ${game.autoCashoutAt.toFixed(2)}x.`, "neutral");
    } else {
      updateHint(`Bet locked: ${formatMoney(bet)}.`, "neutral");
    }

    updateMessage("Round live. Cash out before the crash.");
    crashOverlayEl.classList.add("hidden");
    setState("running");
    updateMultiplierDisplay();
    drawCurve();
    game.rafId = requestAnimationFrame(tick);
  }

  function cashOut() {
    settleCashout("manual");
  }

  function adjustBet(factor) {
    if (game.state !== "idle") return;
    const current = Math.max(MIN_BET, parseBetInput() || MIN_BET);
    const next = factor < 1 ? current / 2 : current * 2;
    const clamped = Math.max(MIN_BET, Math.min(next, 1000000));
    betAmountEl.value = clamped.toFixed(2);
    updateProjectedProfit();
  }

  function setBetMode(autoMode) {
    if (game.state === "running") return;
    autoCashoutEnabledEl.checked = autoMode;
    syncControls();
    updateProjectedProfit();
  }

  const onResize = () => {
    if (destroyed) return;
    setCanvasSize();
    drawCurve();
  };

  function wireEvents() {
    placeBetBtn.addEventListener("click", startRound);
    cashOutBtn.addEventListener("click", cashOut);
    halfBetBtn.addEventListener("click", () => adjustBet(0.5));
    doubleBetBtn.addEventListener("click", () => adjustBet(2));
    manualModeBtn.addEventListener("click", () => setBetMode(false));
    autoModeBtn.addEventListener("click", () => setBetMode(true));
    autoCashoutEnabledEl.addEventListener("change", syncControls);
    betAmountEl.addEventListener("input", updateProjectedProfit);
    autoCashoutAtEl.addEventListener("input", updateProjectedProfit);

    betAmountEl.addEventListener("keydown", (event) => {
      if (event.key === "Enter" && game.state === "idle") startRound();
    });

    autoCashoutAtEl.addEventListener("keydown", (event) => {
      if (event.key === "Enter" && game.state === "idle") startRound();
    });

    window.addEventListener("resize", onResize);
  }

  function init() {
    setCanvasSize();
    game.botPlayers = createBotWave(2.4);
    resetFeedTicker();
    renderBotFeed();
    seedFeedTicker();
    renderHistory();
    syncGlobalCash();
    updateBalanceDisplay();
    updateProjectedProfit();
    updateMultiplierDisplay();
    updateStateBadge();
    syncControls();
    drawCurve();
    wireEvents();
  }

  init();

  activeCasinoCleanup = () => {
    destroyed = true;
    clearRoundAnimation();
    clearAllTimeouts();
    window.removeEventListener("resize", onResize);
    game.botFeedSession += 1;
    game.sideFeedSession += 1;
  };
}

function loadMines() {
  const container = document.getElementById("casino-container");
  if (!container) return;
  container.classList.add("casino-fullbleed", "mines-fullbleed");

  container.innerHTML = `
    <div class="mines-root">
      <div class="app">
        <aside class="panel">
          <div class="panel-card">
            <div class="toggle-wrap" aria-label="Play mode">
              <button type="button" class="mode-btn active" id="manualMode">Manual</button>
              <button type="button" class="mode-btn" id="autoMode">Auto</button>
            </div>

            <div class="field-head">
              <label for="betAmount">Bet Amount</label>
              <strong id="balanceInline">$0.00</strong>
            </div>
            <div class="bet-row">
              <input id="betAmount" type="number" min="0.01" step="0.01" value="10.00" />
              <div class="bet-adjust">
                <button id="halfBetButton" type="button">1/2</button>
                <button id="doubleBetButton" type="button">2x</button>
              </div>
            </div>

            <div class="field-head single">
              <label for="mineCount">Mines</label>
            </div>
            <select id="mineCount" aria-label="Mine count"></select>

            <div id="autoSettings" class="auto-settings hidden">
              <div class="field-head single">
                <label for="autoTargetGems">Auto Cashout (Gems)</label>
              </div>
              <input id="autoTargetGems" type="number" min="1" step="1" value="2" />
              <div class="field-head single">
                <label for="autoRoundDelay">Action Delay (ms)</label>
              </div>
              <input id="autoRoundDelay" type="number" min="50" step="50" value="300" />
            </div>

            <button id="startButton" class="btn primary" type="button">Bet</button>
            <button id="cashOutButton" class="btn success hidden" type="button">Cash Out</button>
            <button id="resetButton" class="btn hidden" type="button">Reset</button>
          </div>

          <div class="card stats">
            <div class="row">
              <span>Balance</span>
              <strong id="balance">$0.00</strong>
            </div>
            <div class="row">
              <span>Current Bet</span>
              <strong id="currentBet">$0.00</strong>
            </div>
            <div class="row">
              <span>Multiplier</span>
              <strong id="multiplier">1.00x</strong>
            </div>
            <div class="row">
              <span>Current Profit</span>
              <strong id="currentProfit">$0.00</strong>
            </div>
            <div class="row">
              <span>Next</span>
              <strong id="nextMultiplier">-</strong>
            </div>
            <div class="row">
              <span>State</span>
              <strong id="stateText">idle</strong>
            </div>
          </div>

          <div class="card history-card">
            <h2>Last 10 Rounds</h2>
            <ul id="historyList" class="history"></ul>
          </div>

          <p id="message" class="message">Set your bet and start a round.</p>
        </aside>

        <main class="game-area">
          <div id="roundOverlay" class="overlay hidden" aria-live="polite"></div>
          <div id="grid" class="grid" aria-label="Mines game board"></div>
        </main>
      </div>
    </div>
  `;

  const GRID_SIZE = 5;
  const TOTAL_TILES = GRID_SIZE * GRID_SIZE;
  const STARTING_BALANCE = Number(Math.max(0, cash).toFixed(2));
  const HOUSE_EDGE = 0.98;
  const HISTORY_LIMIT = 10;

  let destroyed = false;

  const state = {
    gameState: "idle",
    playMode: "manual",
    autoRunning: false,
    autoTimerId: null,
    balance: STARTING_BALANCE,
    currentBet: 0,
    mines: 5,
    gemsFound: 0,
    multiplier: 1,
    board: [],
    history: []
  };

  const GEM_ICON_SVG = `
    <svg class="gem-svg" viewBox="0 0 100 100" aria-hidden="true">
      <polygon points="50,4 88,22 50,96 12,22" fill="#67ef4a"></polygon>
      <polygon points="50,4 12,22 28,44 50,30" fill="#9eff78" opacity="0.9"></polygon>
      <polygon points="50,4 88,22 72,44 50,30" fill="#56d848" opacity="0.85"></polygon>
      <polygon points="50,30 28,44 50,96" fill="#58da48" opacity="0.95"></polygon>
      <polygon points="50,30 72,44 50,96" fill="#41bf3a" opacity="0.95"></polygon>
    </svg>
  `;

  const MINE_ICON_SVG = `
    <svg class="mine-svg" viewBox="0 0 100 100" aria-hidden="true">
      <circle cx="50" cy="50" r="34" fill="#ea4452"></circle>
      <circle cx="40" cy="38" r="20" fill="#ff7e88" opacity="0.25"></circle>
      <path d="M53 36 L58 50 L69 45 L61 55 L73 58 L58 59 L61 72 L53 62 L45 72 L48 59 L36 58 L47 55 L39 45 L50 50 Z" fill="#ffe9a8"></path>
      <circle cx="53" cy="54" r="5" fill="#fff"></circle>
    </svg>
  `;

  const elements = {
    balance: container.querySelector("#balance"),
    balanceInline: container.querySelector("#balanceInline"),
    betAmount: container.querySelector("#betAmount"),
    mineCount: container.querySelector("#mineCount"),
    autoSettings: container.querySelector("#autoSettings"),
    autoTargetGems: container.querySelector("#autoTargetGems"),
    autoRoundDelay: container.querySelector("#autoRoundDelay"),
    startButton: container.querySelector("#startButton"),
    cashOutButton: container.querySelector("#cashOutButton"),
    resetButton: container.querySelector("#resetButton"),
    halfBetButton: container.querySelector("#halfBetButton"),
    doubleBetButton: container.querySelector("#doubleBetButton"),
    currentBet: container.querySelector("#currentBet"),
    multiplier: container.querySelector("#multiplier"),
    currentProfit: container.querySelector("#currentProfit"),
    nextMultiplier: container.querySelector("#nextMultiplier"),
    stateText: container.querySelector("#stateText"),
    message: container.querySelector("#message"),
    gameArea: container.querySelector(".game-area"),
    grid: container.querySelector("#grid"),
    overlay: container.querySelector("#roundOverlay"),
    historyList: container.querySelector("#historyList"),
    manualMode: container.querySelector("#manualMode"),
    autoMode: container.querySelector("#autoMode")
  };

  function syncGlobalCash() {
    cash = Number(Math.max(0, state.balance).toFixed(2));
    updateUI();
    updateBlackjackCash();
  }

  function formatMoney(value) {
    return `${CURRENCY_SYMBOL}${value.toFixed(2)}`;
  }

  function formatSignedMoney(value) {
    if (Math.abs(value) < 0.005) return `${CURRENCY_SYMBOL}0.00`;
    return `${value >= 0 ? "+" : "-"}${CURRENCY_SYMBOL}${Math.abs(value).toFixed(2)}`;
  }

  function clampMines(mines) {
    return Math.max(1, Math.min(TOTAL_TILES - 1, mines));
  }

  function clampAutoTarget(target) {
    const maxSafe = TOTAL_TILES - state.mines;
    return Math.max(1, Math.min(maxSafe, target));
  }

  function getAutoTargetGems() {
    const parsed = Math.floor(Number(elements.autoTargetGems.value));
    const safe = Number.isFinite(parsed) ? parsed : 1;
    const clamped = clampAutoTarget(safe);
    elements.autoTargetGems.value = String(clamped);
    return clamped;
  }

  function getAutoDelay() {
    const parsed = Math.floor(Number(elements.autoRoundDelay.value));
    const safe = Number.isFinite(parsed) ? parsed : 300;
    const clamped = Math.max(50, Math.min(3000, safe));
    elements.autoRoundDelay.value = String(clamped);
    return clamped;
  }

  function createMineOptions() {
    for (let i = 1; i <= TOTAL_TILES - 1; i += 1) {
      const option = document.createElement("option");
      option.value = String(i);
      option.textContent = String(i);
      if (i === state.mines) option.selected = true;
      elements.mineCount.append(option);
    }
  }

  function setMessage(text) {
    if (!elements.message) return;
    elements.message.textContent = text;
  }

  function setOverlay(text = "", mode = "") {
    if (!elements.overlay) return;
    if (!text) {
      elements.overlay.className = "overlay hidden";
      elements.overlay.textContent = "";
      return;
    }
    elements.overlay.className = `overlay ${mode}`.trim();
    elements.overlay.textContent = text;
  }

  function calculateMultiplier(gemsFound, mines) {
    let multiplier = 1;
    for (let i = 0; i < gemsFound; i += 1) {
      const tilesLeft = TOTAL_TILES - i;
      const safeTilesLeft = TOTAL_TILES - mines - i;
      multiplier *= tilesLeft / safeTilesLeft;
    }
    return multiplier * HOUSE_EDGE;
  }

  function calculateNextMultiplier() {
    const maxSafe = TOTAL_TILES - state.mines;
    if (state.gemsFound >= maxSafe) return null;
    return calculateMultiplier(state.gemsFound + 1, state.mines);
  }

  function updateStatsUI() {
    elements.balance.textContent = formatMoney(state.balance);
    elements.balanceInline.textContent = formatMoney(state.balance);
    elements.currentBet.textContent = formatMoney(state.currentBet);
    elements.multiplier.textContent = `${state.multiplier.toFixed(2)}x`;
    const liveProfit = state.currentBet > 0 ? state.currentBet * state.multiplier - state.currentBet : 0;
    elements.currentProfit.textContent = formatSignedMoney(liveProfit);

    const next = calculateNextMultiplier();
    elements.nextMultiplier.textContent = next ? `${next.toFixed(2)}x` : "MAX";
    elements.stateText.textContent = state.gameState;
  }

  function updateControlsUI() {
    const active = state.gameState === "active";
    const roundEnded = state.gameState === "lost" || state.gameState === "cashed";
    const idle = state.gameState === "idle";
    const autoMode = state.playMode === "auto";

    elements.autoSettings.classList.toggle("hidden", !autoMode);

    const lockInputs = active || state.autoRunning;
    elements.betAmount.disabled = lockInputs;
    elements.mineCount.disabled = lockInputs;
    elements.halfBetButton.disabled = lockInputs;
    elements.doubleBetButton.disabled = lockInputs;
    elements.autoTargetGems.disabled = lockInputs;
    elements.autoRoundDelay.disabled = lockInputs;

    elements.startButton.disabled = state.autoRunning ? false : active;
    elements.startButton.textContent = autoMode
      ? state.autoRunning
        ? "Stop Auto"
        : "Start Auto"
      : "Bet";

    elements.cashOutButton.classList.toggle("hidden", !active || state.autoRunning);
    elements.resetButton.classList.toggle("hidden", !roundEnded || state.autoRunning);
  }

  function renderHistory() {
    elements.historyList.innerHTML = "";
    state.history.forEach((item) => {
      const listItem = document.createElement("li");
      listItem.className = item.outcome;
      listItem.innerHTML = `<span>${item.outcome === "win" ? "Cash Out" : "Mine Hit"}</span><strong>${item.amount}</strong>`;
      elements.historyList.append(listItem);
    });
  }

  function pushHistoryEntry(outcome, amountText) {
    state.history.unshift({ outcome, amount: amountText });
    state.history = state.history.slice(0, HISTORY_LIMIT);
    renderHistory();
  }

  function createShuffledBoard(mines) {
    const board = [];
    for (let i = 0; i < mines; i += 1) board.push({ isMine: true, revealed: false });
    for (let i = mines; i < TOTAL_TILES; i += 1) board.push({ isMine: false, revealed: false });

    for (let i = board.length - 1; i > 0; i -= 1) {
      const j = Math.floor(Math.random() * (i + 1));
      [board[i], board[j]] = [board[j], board[i]];
    }
    return board;
  }

  function clearBoardVisuals() {
    Array.from(elements.grid.children).forEach((tileEl) => {
      tileEl.className = "tile";
      tileEl.disabled = false;
      const icon = tileEl.querySelector(".icon");
      if (icon) icon.innerHTML = "";
    });
  }

  function ensureBoardTiles() {
    if (elements.grid.children.length === TOTAL_TILES) return;
    renderIdleBoard();
  }

  function renderIdleBoard() {
    elements.grid.classList.remove("board-enter");
    elements.grid.innerHTML = "";
    for (let index = 0; index < TOTAL_TILES; index += 1) {
      const button = document.createElement("button");
      button.className = "tile";
      button.type = "button";
      button.dataset.index = String(index);
      button.style.setProperty("--stagger", `${index * 28}ms`);
      button.style.setProperty("--pulse-delay", `${index * 110}ms`);
      button.setAttribute("aria-label", `Tile ${index + 1}`);
      button.innerHTML = '<span class="icon"></span>';
      elements.grid.append(button);
    }

    requestAnimationFrame(() => {
      if (!destroyed) elements.grid.classList.add("board-enter");
    });
  }

  function revealTile(index, { showMineIcon = false } = {}) {
    const tileData = state.board[index];
    if (!tileData || tileData.revealed) return;

    tileData.revealed = true;
    const tileEl = elements.grid.children[index];
    tileEl.classList.add("revealed", "disabled");
    tileEl.disabled = true;

    const icon = tileEl.querySelector(".icon");
    if (tileData.isMine || showMineIcon) {
      tileEl.classList.add("mine");
      icon.innerHTML = MINE_ICON_SVG;
    } else {
      tileEl.classList.add("gem", "sparkle");
      icon.innerHTML = GEM_ICON_SVG;
      window.setTimeout(() => {
        if (!destroyed) tileEl.classList.remove("sparkle");
      }, 700);
    }
  }

  function revealAllMines(triggeredIndex = -1) {
    state.board.forEach((tile, index) => {
      if (tile.isMine) {
        revealTile(index, { showMineIcon: true });
        if (index === triggeredIndex) elements.grid.children[index].classList.add("explode");
        else elements.grid.children[index].classList.add("muted");
      }
    });
  }

  function disableGrid() {
    Array.from(elements.grid.children).forEach((tile) => {
      tile.classList.add("disabled");
      tile.disabled = true;
    });
  }

  function setGameState(newState) {
    state.gameState = newState;
    updateStatsUI();
    updateControlsUI();
  }

  function startRound() {
    if (state.gameState === "active") return;
    if (!ensureCasinoBettingAllowedNow()) return;
    if (state.gameState === "lost" || state.gameState === "cashed") {
      resetAfterRound({ silent: true });
    }

    const bet = Number(elements.betAmount.value);
    const mines = clampMines(Number(elements.mineCount.value));

    if (!Number.isFinite(bet) || bet <= 0) {
      setMessage("Enter a valid bet greater than 0.");
      return;
    }
    if (bet > state.balance) {
      setMessage("Bet cannot exceed your balance.");
      return;
    }

    state.currentBet = bet;
    state.mines = mines;
    state.gemsFound = 0;
    state.multiplier = 1;
    state.board = createShuffledBoard(mines);
    state.balance -= bet;
    syncGlobalCash();

    ensureBoardTiles();
    clearBoardVisuals();
    setOverlay();
    setGameState("active");
    setMessage("Round active. Reveal gems or cash out.");
  }

  function clearAutoTimer() {
    if (state.autoTimerId) {
      clearTimeout(state.autoTimerId);
      state.autoTimerId = null;
    }
  }

  function stopAutoPlay(message = "Auto mode stopped.") {
    clearAutoTimer();
    state.autoRunning = false;
    updateControlsUI();
    if (message) setMessage(message);
  }

  function queueAutoAction(callback, delay) {
    clearAutoTimer();
    state.autoTimerId = window.setTimeout(() => {
      state.autoTimerId = null;
      if (destroyed || !state.autoRunning) return;
      callback();
    }, delay);
  }

  function runAutoTurn() {
    if (!state.autoRunning || state.gameState !== "active") return;

    const targetGems = getAutoTargetGems();
    if (state.gemsFound >= targetGems) {
      endRoundAsCashOut(`Auto cashed out at ${state.multiplier.toFixed(2)}x.`);
      return;
    }

    const unrevealed = [];
    state.board.forEach((tile, index) => {
      if (!tile.revealed) unrevealed.push(index);
    });
    if (unrevealed.length === 0) return;

    let pick = unrevealed[Math.floor(Math.random() * unrevealed.length)];
    const unrevealedMines = unrevealed.filter((index) => state.board[index]?.isMine);
    const autoMineChance = Math.min(0.78, 0.26 + state.gemsFound * 0.09);
    if (unrevealedMines.length > 0 && Math.random() < autoMineChance) {
      pick = unrevealedMines[Math.floor(Math.random() * unrevealedMines.length)];
    } else if (state.gemsFound === 0 && shouldRigHighBet(state.currentBet, 1.1)) {
      state.board[pick].isMine = true;
    }
    revealTile(pick);
    const tileData = state.board[pick];

    if (tileData.isMine) {
      endRoundAsLoss(pick);
      return;
    }

    handleGemReveal();
    if (!state.autoRunning || state.gameState !== "active") return;
    queueAutoAction(runAutoTurn, getAutoDelay());
  }

  function queueNextAutoRound() {
    queueAutoAction(() => {
      if (!state.autoRunning) return;
      if (state.balance <= 0) {
        stopAutoPlay("Auto stopped: balance depleted.");
        return;
      }

      resetAfterRound({ silent: true });
      startRound();
      if (state.gameState !== "active") {
        stopAutoPlay("Auto stopped: check bet amount and balance.");
        return;
      }
      queueAutoAction(runAutoTurn, getAutoDelay());
    }, getAutoDelay());
  }

  function startAutoPlay() {
    if (state.autoRunning || state.gameState === "active") return;
    if (state.gameState === "lost" || state.gameState === "cashed") {
      resetAfterRound({ silent: true });
    }

    state.autoRunning = true;
    updateControlsUI();
    setMessage("Auto mode running.");

    startRound();
    if (state.gameState !== "active") {
      stopAutoPlay("Auto stopped: check bet amount and balance.");
      return;
    }
    queueAutoAction(runAutoTurn, getAutoDelay());
  }

  function animateMultiplier() {
    elements.multiplier.classList.remove("multiplier-pop");
    void elements.multiplier.offsetWidth;
    elements.multiplier.classList.add("multiplier-pop");
  }

  function handleGemReveal() {
    state.gemsFound += 1;
    state.multiplier = calculateMultiplier(state.gemsFound, state.mines);
    animateMultiplier();
    updateStatsUI();

    if (state.gemsFound === TOTAL_TILES - state.mines) {
      endRoundAsCashOut("All safe tiles found. Max payout secured.");
      return;
    }

    setMessage(`Gem found. Multiplier is now ${state.multiplier.toFixed(2)}x.`);
  }

  function endRoundAsLoss(triggeredIndex) {
    playGameSound("mines_explosion", { restart: true, allowOverlap: false });
    playGameSound("loss");
    revealAllMines(triggeredIndex);
    elements.gameArea.classList.remove("board-shake");
    void elements.gameArea.offsetWidth;
    elements.gameArea.classList.add("board-shake");
    window.setTimeout(() => {
      if (!destroyed) elements.gameArea.classList.remove("board-shake");
    }, 450);

    disableGrid();
    setGameState("lost");
    setOverlay("Mine hit! You lost this bet.", "loss");
    setMessage(
      state.autoRunning
        ? "Auto lost this round. Preparing next round..."
        : "Round lost. Press Bet to start a new round."
    );
    pushHistoryEntry("loss", `-${formatMoney(state.currentBet)}`);
    triggerCasinoKickoutCheckAfterRound();
    if (state.autoRunning) queueNextAutoRound();
  }

  function endRoundAsCashOut(customMessage) {
    const payout = state.currentBet * state.multiplier;
    const netProfit = payout - state.currentBet;
    state.balance += payout;
    if (netProfit > 0) showCasinoWinPopup({ amount: netProfit, multiplier: state.multiplier });
    syncGlobalCash();

    disableGrid();
    setGameState("cashed");
    setOverlay("Cashed Out", "win");
    setMessage(customMessage || `You secured ${formatMoney(payout)} at ${state.multiplier.toFixed(2)}x.`);
    pushHistoryEntry("win", `+${formatMoney(payout)}`);
    updateStatsUI();
    triggerCasinoKickoutCheckAfterRound();
    if (state.autoRunning) queueNextAutoRound();
  }

  function handleTileClick(event) {
    const tile = event.target.closest(".tile");
    if (!tile || state.gameState !== "active" || state.autoRunning) return;

    const index = Number(tile.dataset.index);
    const tileData = state.board[index];
    if (!tileData || tileData.revealed) return;
    if (state.gemsFound === 0 && shouldRigHighBet(state.currentBet, 1.1)) {
      tileData.isMine = true;
    }

    revealTile(index);
    if (tileData.isMine) {
      endRoundAsLoss(index);
      return;
    }
    handleGemReveal();
  }

  function resetAfterRound({ silent = false } = {}) {
    state.currentBet = 0;
    state.gemsFound = 0;
    state.multiplier = 1;
    state.board = [];
    ensureBoardTiles();
    clearBoardVisuals();
    setOverlay();
    setGameState("idle");
    if (!silent) setMessage("Set your bet and start a round.");
  }

  function wireEvents() {
    elements.startButton.addEventListener("click", () => {
      if (state.playMode === "auto") {
        if (state.autoRunning) {
          stopAutoPlay("Auto mode stopped.");
        } else {
          startAutoPlay();
        }
        return;
      }
      startRound();
    });

    elements.halfBetButton.addEventListener("click", () => {
      if (state.gameState !== "idle") return;
      const current = Number(elements.betAmount.value) || 0;
      const next = Math.max(0.01, current / 2);
      elements.betAmount.value = next.toFixed(2);
    });

    elements.doubleBetButton.addEventListener("click", () => {
      if (state.gameState !== "idle") return;
      const current = Number(elements.betAmount.value) || 0;
      const next = Math.min(state.balance, Math.max(0.01, current * 2));
      elements.betAmount.value = next.toFixed(2);
    });

    elements.cashOutButton.addEventListener("click", () => {
      if (state.gameState !== "active") return;
      endRoundAsCashOut();
    });

    elements.resetButton.addEventListener("click", () => resetAfterRound());
    elements.grid.addEventListener("click", handleTileClick);

    elements.mineCount.addEventListener("change", () => {
      state.mines = clampMines(Number(elements.mineCount.value));
      getAutoTargetGems();
      updateStatsUI();
    });

    elements.autoTargetGems.addEventListener("change", () => {
      getAutoTargetGems();
    });

    elements.autoRoundDelay.addEventListener("change", () => {
      getAutoDelay();
    });

    elements.manualMode.addEventListener("click", () => {
      if (state.autoRunning) stopAutoPlay();
      state.playMode = "manual";
      elements.manualMode.classList.add("active");
      elements.autoMode.classList.remove("active");
      setMessage("Manual mode selected.");
      updateControlsUI();
    });

    elements.autoMode.addEventListener("click", () => {
      state.playMode = "auto";
      elements.autoMode.classList.add("active");
      elements.manualMode.classList.remove("active");
      setMessage("Auto mode selected. Configure and press Start Auto.");
      updateControlsUI();
    });
  }

  function initialize() {
    createMineOptions();
    renderIdleBoard();
    wireEvents();
    syncGlobalCash();
    updateStatsUI();
    updateControlsUI();
    renderHistory();
    setMessage("Set your bet and start a round.");
  }

  initialize();

  activeCasinoCleanup = () => {
    destroyed = true;
    clearAutoTimer();
    state.autoRunning = false;
    container.classList.remove("casino-fullbleed", "mines-fullbleed");
  };
}

function loadKeno() {
  const container = document.getElementById("casino-container");
  if (!container) return;
  container.classList.add("casino-fullbleed", "keno-fullbleed");

  container.innerHTML = `
    <div class="keno-root">
      <div class="app-shell">
        <aside class="sidebar">
          <div class="mode-switch" role="tablist" aria-label="Play mode">
            <button id="manualModeBtn" class="mode-btn active" type="button">Manual</button>
            <button id="autoModeBtn" class="mode-btn" type="button">Auto</button>
          </div>

          <div class="control-box">
            <div class="field-block">
              <div class="field-head">
                <label for="betAmount">Bet Amount</label>
                <strong id="balance">$0.00</strong>
              </div>
              <div class="bet-row">
                <input id="betAmount" type="number" min="0.01" step="0.01" value="10.00" />
                <button id="halfBetBtn" class="chip-btn" type="button">&frac12;</button>
                <button id="doubleBetBtn" class="chip-btn" type="button">2&times;</button>
              </div>
            </div>

            <button id="autoPickBtn" class="control-btn" type="button">Auto Pick</button>
            <button id="clearBtn" class="control-btn" type="button">Clear Table</button>
            <button id="playBtn" class="bet-btn" type="button">Bet</button>

            <div id="autoControls" class="auto-controls">
              <label class="inline-toggle" for="autoEnabled">
                <input id="autoEnabled" type="checkbox" />
                Enable Autoplay
              </label>

              <div class="auto-inputs">
                <label for="autoDelay">Delay (ms)</label>
                <input id="autoDelay" type="number" min="200" max="5000" step="100" value="900" />

                <label for="stopProfit">Stop on Profit ($)</label>
                <input id="stopProfit" type="number" min="0" step="0.01" value="0" />

                <label for="stopLoss">Stop on Loss ($)</label>
                <input id="stopLoss" type="number" min="0" step="0.01" value="0" />
              </div>

              <button id="autoBtn" class="control-btn auto-action" type="button">Start Auto</button>
            </div>

            <label class="inline-toggle instant-toggle" for="instantDraw">
              <input id="instantDraw" type="checkbox" />
              Instant Draw
            </label>
          </div>
        </aside>

        <main class="board-area">
          <section class="grid-stage">
            <section id="grid" class="grid" aria-label="Keno numbers"></section>
            <div id="profitOverlay" class="profit-overlay" aria-live="polite">
              <div id="overlayBigWinIcon" class="overlay-big-win-icon" aria-hidden="true">
                <span class="icon-glyph">◆</span>
                <span class="icon-text">BIG WIN</span>
              </div>
              <p id="overlayMultiplier">0.00x</p>
              <p id="overlayProfit">$0.00</p>
            </div>
          </section>

          <section id="oddsStrip" class="odds-strip" aria-label="10 pick payout odds">
            <div id="oddsMultiplierRow" class="odds-row"></div>
            <div id="oddsHitRow" class="odds-row"></div>
          </section>

          <section class="status-panel">
            <div class="status-row">
              <span id="stateBadge" class="badge idle">IDLE</span>
              <span id="matchCount">Hits: 0 / 0</span>
              <span id="payoutSummary">Multiplier: 0x | Payout: $0.00</span>
            </div>
            <p id="message">Select 1-10 numbers and press Bet.</p>
          </section>

          <section class="bottom-strip">
            <p id="pickCounter">Select 1 - 10 numbers to play</p>
          </section>

          <section class="meta-panels">
            <div id="drawnNumbers" class="drawn-numbers"></div>
            <ul id="historyList"></ul>
          </section>
        </main>
      </div>
    </div>
  `;
  addSageBrand(container.querySelector(".keno-root"), "bottom-left");

  const CONFIG = {
    totalNumbers: 40,
    maxPicks: 10,
    minPicks: 1,
    drawCount: 10,
    drawStepMs: 120,
    historySize: 10
  };
  const BIG_WIN_MULTIPLIER_THRESHOLD = 10;
  const BIG_WIN_PAYOUT_THRESHOLD = 500;
  const BASE_PAYOUT_TABLE = {
    1: { 1: 2 },
    2: { 2: 5 },
    3: { 2: 1, 3: 25 },
    4: { 3: 5, 4: 50 },
    5: { 3: 2, 4: 10, 5: 100 },
    6: { 3: 1, 4: 4, 5: 25, 6: 250 },
    7: { 3: 1, 4: 3, 5: 12, 6: 60, 7: 500 },
    8: { 4: 2, 5: 8, 6: 35, 7: 150, 8: 1000 },
    9: { 4: 1, 5: 4, 6: 20, 7: 80, 8: 400, 9: 2000 },
    10: { 5: 2, 6: 10, 7: 40, 8: 200, 9: 1000, 10: 5000 }
  };
  const KENO_10_PICK_PAYOUTS = { 0: 0, 1: 0, 2: 0, 3: 0.5, 4: 1, 5: 2, 6: 5, 7: 15, 8: 50, 9: 250, 10: 1000 };
  const KENO_10_PICK_HIT_ODDS = [
    { hits: 0, probability: 1.548 },
    { hits: 1, probability: 9.778 },
    { hits: 2, probability: 24.2 },
    { hits: 3, probability: 30.73 },
    { hits: 4, probability: 22.0 },
    { hits: 5, probability: 9.183 },
    { hits: 6, probability: 2.232 },
    { hits: 7, probability: 0.306 },
    { hits: 8, probability: 0.0221 },
    { hits: 9, probability: 0.000726 },
    { hits: 10, probability: 0.00000779 }
  ];
  const PAYOUT_TABLE = BASE_PAYOUT_TABLE;

  const ui = {
    grid: container.querySelector("#grid"),
    balance: container.querySelector("#balance"),
    betAmount: container.querySelector("#betAmount"),
    halfBetBtn: container.querySelector("#halfBetBtn"),
    doubleBetBtn: container.querySelector("#doubleBetBtn"),
    autoPickBtn: container.querySelector("#autoPickBtn"),
    pickCounter: container.querySelector("#pickCounter"),
    playBtn: container.querySelector("#playBtn"),
    clearBtn: container.querySelector("#clearBtn"),
    instantDraw: container.querySelector("#instantDraw"),
    autoEnabled: container.querySelector("#autoEnabled"),
    autoDelay: container.querySelector("#autoDelay"),
    stopProfit: container.querySelector("#stopProfit"),
    stopLoss: container.querySelector("#stopLoss"),
    autoBtn: container.querySelector("#autoBtn"),
    manualModeBtn: container.querySelector("#manualModeBtn"),
    autoModeBtn: container.querySelector("#autoModeBtn"),
    autoControls: container.querySelector("#autoControls"),
    statusPanel: container.querySelector(".status-panel"),
    profitOverlay: container.querySelector("#profitOverlay"),
    overlayMultiplier: container.querySelector("#overlayMultiplier"),
    overlayProfit: container.querySelector("#overlayProfit"),
    overlayBigWinIcon: container.querySelector("#overlayBigWinIcon"),
    oddsStrip: container.querySelector("#oddsStrip"),
    oddsMultiplierRow: container.querySelector("#oddsMultiplierRow"),
    oddsHitRow: container.querySelector("#oddsHitRow"),
    stateBadge: container.querySelector("#stateBadge"),
    matchCount: container.querySelector("#matchCount"),
    message: container.querySelector("#message"),
    payoutSummary: container.querySelector("#payoutSummary"),
    drawnNumbers: container.querySelector("#drawnNumbers"),
    historyList: container.querySelector("#historyList")
  };

  const game = {
    state: "idle",
    balance: cash,
    selected: new Set(),
    history: [],
    isAutoRunning: false,
    autoBaseBalance: 0,
    autoStopRequested: false,
    roundCounter: 0,
    mode: "manual"
  };

  let destroyed = false;
  let overlayTimerId = null;
  let countupRafId = null;

  function clamp(value, min, max) {
    if (!Number.isFinite(value)) return min;
    return Math.max(min, Math.min(max, value));
  }

  function round2(value) {
    return Math.round((Number(value) || 0) * 100) / 100;
  }

  function formatMoney(value) {
    return `${CURRENCY_SYMBOL}${(Number(value) || 0).toLocaleString("en-US", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    })}`;
  }

  function syncGlobalCash() {
    cash = round2(game.balance);
    updateUI();
    updateBlackjackCash();
  }

  function setMessage(text, negative = false) {
    if (!ui.message) return;
    ui.message.textContent = text;
    ui.message.style.color = negative ? "var(--danger)" : "var(--text-main)";
  }

  function updateBalanceUi() {
    if (ui.balance) ui.balance.textContent = formatMoney(game.balance);
  }

  function setState(nextState) {
    game.state = nextState;
    if (ui.stateBadge) {
      ui.stateBadge.classList.remove("idle", "betting", "result");
      ui.stateBadge.classList.add(nextState);
      ui.stateBadge.textContent = nextState.toUpperCase();
    }

    const bettingLocked = nextState === "betting" || game.isAutoRunning;
    const autoInputsLocked = nextState === "betting";
    if (ui.grid) ui.grid.classList.toggle("locked", bettingLocked);
    if (ui.betAmount) ui.betAmount.disabled = bettingLocked;
    if (ui.halfBetBtn) ui.halfBetBtn.disabled = bettingLocked;
    if (ui.doubleBetBtn) ui.doubleBetBtn.disabled = bettingLocked;
    if (ui.autoPickBtn) ui.autoPickBtn.disabled = bettingLocked;
    if (ui.playBtn) ui.playBtn.disabled = bettingLocked || game.mode !== "manual";
    if (ui.clearBtn) ui.clearBtn.disabled = bettingLocked;
    if (ui.instantDraw) ui.instantDraw.disabled = bettingLocked;
    if (ui.autoEnabled) ui.autoEnabled.disabled = bettingLocked;
    if (ui.autoDelay) ui.autoDelay.disabled = autoInputsLocked;
    if (ui.stopProfit) ui.stopProfit.disabled = autoInputsLocked;
    if (ui.stopLoss) ui.stopLoss.disabled = autoInputsLocked;
    if (ui.manualModeBtn) ui.manualModeBtn.disabled = bettingLocked;
    if (ui.autoModeBtn) ui.autoModeBtn.disabled = bettingLocked;
    if (ui.autoBtn) ui.autoBtn.disabled = autoInputsLocked && !game.isAutoRunning;
  }

  function createGrid() {
    if (!ui.grid) return;
    ui.grid.innerHTML = "";
    const fragment = document.createDocumentFragment();
    for (let i = 1; i <= CONFIG.totalNumbers; i += 1) {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "tile";
      button.dataset.number = String(i);
      button.innerHTML = `<span class="tile-label">${i}</span>`;
      fragment.appendChild(button);
    }
    ui.grid.appendChild(fragment);
  }

  function updatePickCounter() {
    if (!ui.pickCounter) return;
    if (game.selected.size === 0) {
      ui.pickCounter.textContent = `Select ${CONFIG.minPicks} - ${CONFIG.maxPicks} numbers to play`;
      return;
    }
    ui.pickCounter.textContent = `${game.selected.size} selected`;
  }

  function updateGridStyles() {
    if (!ui.grid) return;
    ui.grid.querySelectorAll(".tile").forEach((tile) => {
      const number = Number(tile.dataset.number);
      tile.classList.toggle("selected", game.selected.has(number));
    });
  }

  function clearRoundVisuals() {
    if (ui.drawnNumbers) ui.drawnNumbers.innerHTML = "";
    if (!ui.grid) return;
    ui.grid.querySelectorAll(".tile").forEach((tile) => {
      tile.classList.remove("drawn", "drawn-miss", "match");
    });
  }

  function resetResultPanel() {
    if (ui.statusPanel) ui.statusPanel.classList.remove("win");
    if (ui.matchCount) ui.matchCount.textContent = "Hits: 0 / 0";
    if (ui.payoutSummary) ui.payoutSummary.textContent = "Multiplier: 0x | Payout: $0.00";
    hideProfitOverlay();
    updateOddsStripResult(0, null);
  }

  function toggleSelection(number) {
    if (game.selected.has(number)) {
      game.selected.delete(number);
    } else {
      if (game.selected.size >= CONFIG.maxPicks) {
        setMessage(`Maximum ${CONFIG.maxPicks} picks allowed.`, true);
        return;
      }
      game.selected.add(number);
    }

    updatePickCounter();
    updateGridStyles();
    updateOddsStripMode();

    if (game.state === "result") {
      clearRoundVisuals();
      resetResultPanel();
      setState("idle");
      setMessage("Select 1-10 numbers and press Bet.");
    }
  }

  function shuffle(values) {
    for (let i = values.length - 1; i > 0; i -= 1) {
      const j = Math.floor(Math.random() * (i + 1));
      [values[i], values[j]] = [values[j], values[i]];
    }
  }

  function countMatches(selectedSet, draw) {
    let total = 0;
    for (const num of draw) {
      if (selectedSet.has(num)) total += 1;
    }
    return total;
  }

  function sampleTenPickHitCount() {
    const totalWeight = KENO_10_PICK_HIT_ODDS.reduce((sum, row) => sum + row.probability, 0);
    let roll = Math.random() * totalWeight;
    for (const row of KENO_10_PICK_HIT_ODDS) {
      roll -= row.probability;
      if (roll <= 0) return row.hits;
    }
    return 10;
  }

  function generateTenPickDraw(selectedSet) {
    const selectedNumbers = Array.from(selectedSet);
    const nonSelectedNumbers = [];
    for (let i = 1; i <= CONFIG.totalNumbers; i += 1) {
      if (!selectedSet.has(i)) nonSelectedNumbers.push(i);
    }
    const targetHits = sampleTenPickHitCount();
    shuffle(selectedNumbers);
    shuffle(nonSelectedNumbers);
    const draw = selectedNumbers.slice(0, targetHits).concat(nonSelectedNumbers.slice(0, CONFIG.drawCount - targetHits));
    shuffle(draw);
    return draw;
  }

  function generateLosingDraw(selectedSet) {
    const pool = [];
    for (let i = 1; i <= CONFIG.totalNumbers; i += 1) {
      if (!selectedSet.has(i)) pool.push(i);
    }
    shuffle(pool);
    return pool.slice(0, CONFIG.drawCount);
  }

  function generateDraw(selectedSet, betAmount, isAutoRound = false) {
    if (selectedSet.size === 10) {
      const draw = generateTenPickDraw(selectedSet);
      if (isAutoRound && Math.random() < 0.42) {
        return generateLosingDraw(selectedSet);
      }
      if (shouldRigHighBet(betAmount, 1.08) && countMatches(selectedSet, draw) >= 5) {
        return generateLosingDraw(selectedSet);
      }
      return draw;
    }

    const numbers = Array.from({ length: CONFIG.totalNumbers }, (_, index) => index + 1);
    shuffle(numbers);
    const draw = numbers.slice(0, CONFIG.drawCount);
    if (isAutoRound && countMatches(selectedSet, draw) >= Math.max(1, selectedSet.size - 3) && Math.random() < 0.5) {
      return generateLosingDraw(selectedSet);
    }
    if (shouldRigHighBet(betAmount, 1.02) && countMatches(selectedSet, draw) >= Math.max(1, selectedSet.size - 2)) {
      return generateLosingDraw(selectedSet);
    }
    return draw;
  }

  function getActivePayoutTable() {
    return PAYOUT_TABLE;
  }

  function getRoundMultiplier(picks, matches) {
    if (picks === 10) return KENO_10_PICK_PAYOUTS[matches] ?? 0;
    const table = getActivePayoutTable();
    return table[picks]?.[matches] ?? 0;
  }

  function formatMultiplier(multiplier) {
    const fixed = Number((Number(multiplier) || 0).toFixed(2));
    return Number.isInteger(fixed) ? String(fixed) : fixed.toFixed(2);
  }

  function validateRound() {
    if (game.selected.size < CONFIG.minPicks || game.selected.size > CONFIG.maxPicks) {
      setMessage(`Pick between ${CONFIG.minPicks} and ${CONFIG.maxPicks} numbers.`, true);
      return null;
    }
    const betValue = Number(ui.betAmount?.value);
    if (!Number.isFinite(betValue) || betValue <= 0) {
      setMessage("Enter a valid bet amount.", true);
      return null;
    }
    const bet = round2(betValue);
    if (bet > game.balance) {
      setMessage("Bet cannot exceed your balance.", true);
      return null;
    }
    return bet;
  }

  function revealDrawNumber(number) {
    const tile = ui.grid?.querySelector(`[data-number="${number}"]`);
    if (tile) {
      if (game.selected.has(number)) tile.classList.add("match");
      else tile.classList.add("drawn-miss");
    }

    if (ui.drawnNumbers) {
      const chip = document.createElement("span");
      chip.className = "draw-chip";
      if (game.selected.has(number)) chip.classList.add("match");
      chip.textContent = String(number);
      ui.drawnNumbers.appendChild(chip);
    }
  }

  function pushHistory(entry) {
    game.history.unshift(entry);
    game.history = game.history.slice(0, CONFIG.historySize);
    if (!ui.historyList) return;
    ui.historyList.innerHTML = "";
    game.history.forEach((item) => {
      const li = document.createElement("li");
      li.className = "history-item";
      li.innerHTML = `
        <span>#${item.round}</span>
        <span>Bet ${formatMoney(item.bet)} | ${item.matches}/${item.picks} hits | ${formatMultiplier(item.multiplier)}x</span>
        <span class="result ${item.payout > 0 ? "win" : "loss"}">${item.payout > 0 ? "+" : "-"}${formatMoney(item.payout > 0 ? item.payout : item.bet)}</span>
      `;
      ui.historyList.appendChild(li);
    });
  }

  function stopCountup() {
    if (countupRafId) {
      cancelAnimationFrame(countupRafId);
      countupRafId = null;
    }
  }

  function startCountup(targetAmount) {
    stopCountup();
    const safeTarget = Math.max(0, Number(targetAmount) || 0);
    const startAt = performance.now();
    const duration = 1550;

    const tick = (now) => {
      const progress = Math.min(1, (now - startAt) / duration);
      const eased = 1 - Math.pow(1 - progress, 3);
      if (ui.overlayProfit) ui.overlayProfit.textContent = formatMoney(safeTarget * eased);
      if (progress < 1) {
        countupRafId = requestAnimationFrame(tick);
        return;
      }
      if (ui.overlayProfit) ui.overlayProfit.textContent = formatMoney(safeTarget);
      countupRafId = null;
    };

    countupRafId = requestAnimationFrame(tick);
  }

  function hideProfitOverlay() {
    if (overlayTimerId) {
      clearTimeout(overlayTimerId);
      overlayTimerId = null;
    }
    stopCountup();
    if (!ui.profitOverlay || !ui.overlayMultiplier || !ui.overlayProfit) return;
    ui.profitOverlay.classList.remove("show", "loss", "big-win", "big-win-banner");
    ui.overlayMultiplier.textContent = "0.00x";
    ui.overlayProfit.textContent = "$0.00";
  }

  function setProfitOverlay(multiplier, payout, isWin) {
    if (!ui.profitOverlay || !ui.overlayMultiplier || !ui.overlayProfit) return;
    const safePayout = round2(payout);
    const bigWin = isWin && (multiplier >= BIG_WIN_MULTIPLIER_THRESHOLD || safePayout >= BIG_WIN_PAYOUT_THRESHOLD);

    ui.overlayMultiplier.textContent = bigWin ? "BIG WIN" : `${Number(multiplier).toFixed(2)}x`;
    ui.overlayProfit.textContent = isWin ? formatMoney(safePayout) : formatMoney(0);
    ui.profitOverlay.classList.remove("show");
    ui.profitOverlay.offsetHeight;
    ui.profitOverlay.classList.add("show");
    ui.profitOverlay.classList.toggle("loss", !isWin);
    ui.profitOverlay.classList.toggle("big-win", bigWin);
    ui.profitOverlay.classList.toggle("big-win-banner", bigWin);
    if (bigWin) startCountup(safePayout);
    else stopCountup();

    if (overlayTimerId) clearTimeout(overlayTimerId);
    overlayTimerId = setTimeout(() => {
      if (destroyed) return;
      hideProfitOverlay();
    }, bigWin ? 2500 : 1700);
  }

  function setResultText(matches, picks, multiplier, payout, bet) {
    if (ui.matchCount) ui.matchCount.textContent = `Hits: ${matches} / ${picks}`;
    if (ui.payoutSummary) {
      ui.payoutSummary.textContent = `${matches} / ${picks} - ${formatMultiplier(multiplier)}x | Payout: ${formatMoney(payout)}`;
    }
    if (multiplier > 0) {
      if (ui.statusPanel) ui.statusPanel.classList.add("win");
      setMessage(`Win! Hits: ${matches} / ${picks} | +${formatMoney(payout)}`);
    } else {
      if (ui.statusPanel) ui.statusPanel.classList.remove("win");
      setMessage(`No Win | Hits: ${matches} / ${picks} | Lost ${formatMoney(bet)}`, true);
    }
  }

  async function runRound() {
    if (!ensureCasinoBettingAllowedNow()) return false;
    const bet = validateRound();
    if (bet == null) return false;
    setState("betting");
    clearRoundVisuals();
    hideProfitOverlay();
    clearOddsStripHighlight();

    game.balance = round2(game.balance - bet);
    updateBalanceUi();
    syncGlobalCash();

    const draw = generateDraw(game.selected, bet, game.isAutoRunning);
    if (ui.instantDraw?.checked) {
      draw.forEach((number) => revealDrawNumber(number));
    } else {
      for (const number of draw) {
        if (destroyed) return false;
        revealDrawNumber(number);
        await sleep(CONFIG.drawStepMs);
      }
    }

    const picks = game.selected.size;
    const matches = countMatches(game.selected, draw);
    const multiplier = getRoundMultiplier(picks, matches);
    const payout = round2(bet * multiplier);
    const isWin = multiplier > 0;
    if (isWin) {
      game.balance = round2(game.balance + payout);
    } else {
      playGameSound("loss");
    }
    updateBalanceUi();
    syncGlobalCash();

    game.roundCounter += 1;
    pushHistory({
      round: game.roundCounter,
      bet,
      picks,
      matches,
      multiplier,
      payout,
      balance: game.balance
    });

    setResultText(matches, picks, multiplier, payout, bet);
    setProfitOverlay(multiplier, payout, isWin);
    updateOddsStripResult(picks, matches);
    setState("result");

    const profit = round2(payout - bet);
    if (profit > 0.009) showCasinoWinPopup({ amount: profit, multiplier });
    triggerCasinoKickoutCheckAfterRound();

    return true;
  }

  function getAutoDelay() {
    return clamp(Math.floor(Number(ui.autoDelay?.value) || 900), 200, 5000);
  }

  function autoStopThresholdMet() {
    const profitTarget = Math.max(0, Number(ui.stopProfit?.value) || 0);
    const lossLimit = Math.max(0, Number(ui.stopLoss?.value) || 0);
    const net = round2(game.balance - game.autoBaseBalance);
    const hitProfit = profitTarget > 0 && net >= profitTarget;
    const hitLoss = lossLimit > 0 && net <= -lossLimit;
    return hitProfit || hitLoss;
  }

  function stopAutoplay(message) {
    game.isAutoRunning = false;
    game.autoStopRequested = false;
    if (ui.autoBtn) ui.autoBtn.textContent = "Start Auto";
    setState(game.state);
    setMessage(message);
  }

  async function startAutoplay() {
    if (game.isAutoRunning || game.state === "betting") return;
    if (!ui.autoEnabled?.checked) {
      setMessage("Enable autoplay first.", true);
      return;
    }
    game.isAutoRunning = true;
    game.autoStopRequested = false;
    game.autoBaseBalance = game.balance;
    if (ui.autoBtn) ui.autoBtn.textContent = "Stop Auto";
    setState(game.state);
    setMessage("Autoplay started.");

    while (game.isAutoRunning && !game.autoStopRequested && !destroyed) {
      const ok = await runRound();
      if (!ok) break;
      if (autoStopThresholdMet()) break;
      await sleep(getAutoDelay());
    }

    if (destroyed) return;
    if (game.autoStopRequested) stopAutoplay("Autoplay stopped.");
    else if (autoStopThresholdMet()) stopAutoplay("Autoplay stopped by threshold.");
    else stopAutoplay("Autoplay ended.");
  }

  function setPlayMode(mode) {
    game.mode = mode === "auto" ? "auto" : "manual";
    if (ui.manualModeBtn) ui.manualModeBtn.classList.toggle("active", game.mode === "manual");
    if (ui.autoModeBtn) ui.autoModeBtn.classList.toggle("active", game.mode === "auto");
    if (ui.autoControls) ui.autoControls.classList.toggle("show", game.mode === "auto");
    if (ui.playBtn) {
      ui.playBtn.textContent = game.mode === "manual" ? "Bet" : "Auto via panel";
      ui.playBtn.disabled = game.mode !== "manual" || game.state === "betting" || game.isAutoRunning;
    }
  }

  function adjustBet(multiplier) {
    if (!ui.betAmount) return;
    const current = Number(ui.betAmount.value);
    const base = Number.isFinite(current) && current > 0 ? current : 1;
    const next = clamp(round2(base * multiplier), 0.01, game.balance > 0 ? game.balance : 0.01);
    ui.betAmount.value = next.toFixed(2);
  }

  function autoPickSelection() {
    if (game.state === "betting" || game.isAutoRunning) return;
    const target = clamp(game.selected.size || CONFIG.maxPicks, CONFIG.minPicks, CONFIG.maxPicks);
    const numbers = Array.from({ length: CONFIG.totalNumbers }, (_, i) => i + 1);
    shuffle(numbers);
    game.selected = new Set(numbers.slice(0, target));
    clearRoundVisuals();
    updatePickCounter();
    updateGridStyles();
    updateOddsStripMode();
    resetResultPanel();
    setState("idle");
    setMessage(`Auto-picked ${target} numbers.`);
  }

  function buildOddsStrip() {
    if (!ui.oddsMultiplierRow || !ui.oddsHitRow) return;
    ui.oddsMultiplierRow.innerHTML = "";
    ui.oddsHitRow.innerHTML = "";
    for (let hit = 0; hit <= 10; hit += 1) {
      const multCell = document.createElement("div");
      multCell.className = "odds-cell multiplier";
      multCell.dataset.hit = String(hit);
      multCell.textContent = `${formatMultiplier(KENO_10_PICK_PAYOUTS[hit])}x`;
      ui.oddsMultiplierRow.appendChild(multCell);

      const hitCell = document.createElement("div");
      hitCell.className = "odds-cell hits";
      hitCell.dataset.hit = String(hit);
      hitCell.textContent = `${hit} hit`;
      ui.oddsHitRow.appendChild(hitCell);
    }
    updateOddsStripMode();
  }

  function clearOddsStripHighlight() {
    if (!ui.oddsStrip) return;
    ui.oddsStrip.querySelectorAll(".odds-cell.active").forEach((cell) => {
      cell.classList.remove("active");
    });
  }

  function updateOddsStripMode() {
    if (!ui.oddsStrip) return;
    const active = game.selected.size === 10;
    ui.oddsStrip.querySelectorAll(".odds-cell").forEach((cell) => {
      cell.classList.toggle("dim", !active);
    });
    if (!active) clearOddsStripHighlight();
  }

  function updateOddsStripResult(picks, hits) {
    clearOddsStripHighlight();
    if (picks !== 10 || hits == null) return;
    const multCell = ui.oddsMultiplierRow?.querySelector(`[data-hit="${hits}"]`);
    const hitCell = ui.oddsHitRow?.querySelector(`[data-hit="${hits}"]`);
    if (multCell) multCell.classList.add("active");
    if (hitCell) hitCell.classList.add("active");
  }

  function bindEvents() {
    ui.grid?.addEventListener("click", (event) => {
      const tile = event.target.closest(".tile");
      if (!tile || game.state === "betting" || game.isAutoRunning) return;
      const number = Number(tile.dataset.number);
      toggleSelection(number);
    });

    ui.playBtn?.addEventListener("click", () => {
      if (game.mode !== "manual" || game.state === "betting" || game.isAutoRunning) return;
      void runRound();
    });

    ui.clearBtn?.addEventListener("click", () => {
      if (game.state === "betting" || game.isAutoRunning) return;
      game.selected.clear();
      clearRoundVisuals();
      updatePickCounter();
      updateGridStyles();
      resetResultPanel();
      setState("idle");
      setMessage("Selections cleared.");
    });

    ui.autoBtn?.addEventListener("click", () => {
      if (game.isAutoRunning) {
        game.autoStopRequested = true;
        setMessage("Autoplay will stop after this round.");
      } else {
        void startAutoplay();
      }
    });

    ui.halfBetBtn?.addEventListener("click", () => {
      if (game.state === "betting" || game.isAutoRunning) return;
      adjustBet(0.5);
    });

    ui.doubleBetBtn?.addEventListener("click", () => {
      if (game.state === "betting" || game.isAutoRunning) return;
      adjustBet(2);
    });

    ui.autoPickBtn?.addEventListener("click", () => autoPickSelection());

    ui.manualModeBtn?.addEventListener("click", () => {
      if (!game.isAutoRunning) setPlayMode("manual");
    });

    ui.autoModeBtn?.addEventListener("click", () => {
      if (!game.isAutoRunning) setPlayMode("auto");
    });
  }

  function renderAll() {
    updateBalanceUi();
    updatePickCounter();
    updateGridStyles();
    resetResultPanel();
    setState("idle");
  }

  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  createGrid();
  buildOddsStrip();
  bindEvents();
  renderAll();
  setPlayMode("manual");
  setMessage("Select 1-10 numbers and press Bet.");

  activeCasinoCleanup = () => {
    destroyed = true;
    game.isAutoRunning = false;
    game.autoStopRequested = true;
    if (overlayTimerId) {
      clearTimeout(overlayTimerId);
      overlayTimerId = null;
    }
    stopCountup();
    container.classList.remove("casino-fullbleed", "keno-fullbleed");
  };
}

function loadCrossyRoad() {
  const container = document.getElementById("casino-container");
  if (!container) return;
  container.classList.add("casino-fullbleed", "crossy-fullbleed");

  container.innerHTML = `
    <div class="crossy-root">
      <main class="road-shell">
        <section class="stage-card" aria-label="Game board">
          <canvas id="gameCanvas" aria-label="Crossy Road game"></canvas>
          <div id="resultPanel" class="result-panel hidden" role="status" aria-live="polite">
            <h2>Round Complete</h2>
            <p>Result: <span id="reasonValue">-</span></p>
            <p>Final multiplier: <span id="finalMultiplierValue">x0.00</span></p>
            <p>Payout: $<span id="payoutValue">0.00</span></p>
            <p>Net: <span id="netValue">$0.00</span></p>
          </div>
        </section>

        <section class="control-card" aria-label="Game controls">
          <div class="control-row">
            <section class="control-block bet-block">
              <div class="block-title">Bet Amount</div>
              <div class="bet-input-row">
                <span class="bet-prefix">$</span>
                <input id="betInput" type="number" min="0" step="0.01" value="10.00" inputmode="decimal" aria-label="Bet amount">
                <button type="button" class="mini-btn" data-bet-action="half">1/2</button>
                <button type="button" class="mini-btn" data-bet-action="double">2X</button>
                <button type="button" class="mini-btn" data-bet-action="max">Max</button>
              </div>
              <div class="sub-stats">
                Balance: $<span id="balanceValue">0.00</span>
              </div>
            </section>

            <section class="control-block difficulty-block">
              <div class="block-title">Difficulty</div>
              <div class="difficulty-row" role="radiogroup" aria-label="Difficulty">
                <button type="button" class="difficulty-btn" data-difficulty="easy">Easy</button>
                <button type="button" class="difficulty-btn active" data-difficulty="medium">Medium</button>
                <button type="button" class="difficulty-btn" data-difficulty="hard">Hard</button>
                <button type="button" class="difficulty-btn" data-difficulty="extreme">Extreme</button>
              </div>
              <div class="sub-stats">
                Live Multiplier: <span id="multiplierValue">x1.00</span>
              </div>
            </section>

            <section class="control-block action-block">
              <div class="notice-pill">Minimum bet is $0.01</div>
              <button id="startBtn" type="button" class="start-btn">Start Game</button>
              <div class="sub-stats right-text">
                Reached Lane: <span id="scoreValue">0</span>
              </div>
            </section>
          </div>

          <div class="mode-tabs" aria-label="Mode tabs">
            <button class="tab-btn active" type="button">Manual</button>
            <button class="tab-btn" type="button" disabled aria-disabled="true">Auto</button>
          </div>
        </section>

        <p id="statusText" class="status-text">${IS_PHONE_EMBED_MODE ? "Set your bet and press Start Game. Lanes 1-19 are road lanes." : ""}</p>
        <div class="sage-crossy-row" aria-hidden="true">
          <span>SAGE</span>
          <span>SAGE</span>
          <span>SAGE</span>
          <span>SAGE</span>
        </div>
      </main>
    </div>
  `;

  const root = container.querySelector(".crossy-root");
  const canvas = root.querySelector("#gameCanvas");
  const ctx = canvas.getContext("2d");

  const betInput = root.querySelector("#betInput");
  const startBtn = root.querySelector("#startBtn");
  const balanceValue = root.querySelector("#balanceValue");
  const scoreValue = root.querySelector("#scoreValue");
  const multiplierValue = root.querySelector("#multiplierValue");
  const statusText = root.querySelector("#statusText");
  const resultPanel = root.querySelector("#resultPanel");
  const reasonValue = root.querySelector("#reasonValue");
  const finalMultiplierValue = root.querySelector("#finalMultiplierValue");
  const payoutValue = root.querySelector("#payoutValue");
  const netValue = root.querySelector("#netValue");
  const stageCard = root.querySelector(".stage-card");

  let destroyed = false;
  let animationId = null;
  let resizeObserver = null;
  let resizeFrameId = 0;

  const laneCount = 20;
  const visibleLaneCount = 5;
  const rowCount = 6;
  const startBalance = Number(Math.max(0, cash).toFixed(2));
  const CHICKEN_SIZE_SCALE = 1.7;

  const multiplierStepByDifficulty = {
    easy: 0.05,
    medium: 0.1,
    hard: 0.2,
    extreme: 0.5
  };

  const carProbabilityByDifficulty = {
    easy: [0.05, 0.1, 0.25, 0.4, 0.6],
    medium: [0.1, 0.2, 0.5, 0.7, 0.85],
    hard: [0.15, 0.3, 0.7, 0.85, 0.95],
    extreme: [0.2, 0.4, 0.6, 0.8, 0.99]
  };

  const carStylePool = [
    { body: "#f6b929", roof: "#f9e9b5", accent: "#18355e", glass: "#5eaeea" },
    { body: "#1bb299", roof: "#b9eee5", accent: "#103d38", glass: "#63b8ef" },
    { body: "#0fa4ef", roof: "#b9e3f8", accent: "#18385a", glass: "#7fc7f5" },
    { body: "#f77f00", roof: "#ffd8ae", accent: "#4c2410", glass: "#69b6e9" },
    { body: "#d95f8f", roof: "#f4ccde", accent: "#4a1f32", glass: "#76bce9" }
  ];

  const geometry = {
    width: 0,
    height: 0,
    leftZone: 0,
    sidewalk: 0,
    roadLeft: 0,
    roadWidth: 0,
    laneWidth: 0,
    topPad: 24,
    bottomPad: 24,
    rowHeight: 0
  };

  const state = {
    running: false,
    balance: startBalance,
    bet: 0,
    laneReached: 0,
    currentMultiplier: 0,
    cameraLane: 0,
    cameraTargetLane: 0,
    difficulty: "medium",
    laneTerrain: [],
    laneThreats: [],
    safeLaneBarriers: [],
    safeLaneBarrierRevealAt: [],
    moveQueue: [],
    nextMoveAt: 0,
    hoverLane: -1,
    carAttack: {
      active: false,
      phase: "idle",
      lane: -1,
      y: 0,
      targetY: 0,
      dir: 1,
      baseSpeed: 520,
      speed: 520,
      warningTime: 0,
      impactTime: 0,
      style: carStylePool[0]
    },
    chickenSquished: false,
    chickenSquishTime: 0,
    profitPopup: {
      active: false,
      text: "",
      time: 0,
      duration: 0
    },
    deathFx: {
      active: false,
      time: 0,
      duration: 0,
      particles: []
    },
    deathStain: {
      active: false,
      lane: -1,
      row: 3,
      intensity: 0
    },
    time: 0,
    crashFlash: 0,
    crashLane: -1
  };

  const chicken = {
    lane: -1,
    row: 3,
    drawLane: -1,
    drawRow: 3,
    hopT: 0
  };

  function round2(value) {
    return Math.round(value * 100) / 100;
  }

  function formatMoney(value) {
    return `${CURRENCY_SYMBOL}${round2(value).toFixed(2)}`;
  }

  function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
  }

  function syncGlobalCash() {
    cash = Number(Math.max(0, state.balance).toFixed(2));
    updateUI();
    updateBlackjackCash();
  }

  function setStatus(message) {
    statusText.textContent = message;
  }

  function resizeCanvas() {
    const rect = canvas.getBoundingClientRect();
    const dpr = window.devicePixelRatio || 1;
    if (rect.width <= 0 || rect.height <= 0) return;

    canvas.width = Math.max(1, Math.floor(rect.width * dpr));
    canvas.height = Math.max(1, Math.floor(rect.height * dpr));
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    geometry.width = rect.width;
    geometry.height = rect.height;
    geometry.leftZone = Math.floor(rect.width * 0.16);
    geometry.sidewalk = Math.floor(geometry.leftZone * 0.5);
    geometry.roadLeft = geometry.leftZone;
    geometry.roadWidth = rect.width - geometry.roadLeft;
    geometry.laneWidth = geometry.roadWidth / visibleLaneCount;

    const safeHeight = rect.height - geometry.topPad - geometry.bottomPad;
    geometry.rowHeight = safeHeight / rowCount;
  }

  function scheduleResizeCanvas() {
    if (destroyed || resizeFrameId) return;
    resizeFrameId = window.requestAnimationFrame(() => {
      resizeFrameId = 0;
      resizeCanvas();
    });
  }

  function getCanvasPointerPosition(event) {
    const rect = canvas.getBoundingClientRect();
    const scaleX = rect.width > 0 ? geometry.width / rect.width : 1;
    const scaleY = rect.height > 0 ? geometry.height / rect.height : 1;
    return {
      x: (event.clientX - rect.left) * scaleX,
      y: (event.clientY - rect.top) * scaleY
    };
  }

  function resetChicken() {
    chicken.lane = -1;
    chicken.row = Math.floor(rowCount / 2);
    chicken.drawLane = chicken.lane;
    chicken.drawRow = chicken.row;
    chicken.hopT = 0;
    state.laneTerrain = Array.from({ length: laneCount }, () => "road");
    state.laneThreats = Array.from({ length: laneCount }, () => null);
    state.safeLaneBarriers = Array.from({ length: laneCount }, () => false);
    state.safeLaneBarrierRevealAt = Array.from({ length: laneCount }, () => -1);
    state.moveQueue = [];
    state.nextMoveAt = 0;
    state.crashLane = -1;
    state.carAttack.active = false;
    state.carAttack.phase = "idle";
    state.carAttack.lane = -1;
    state.chickenSquished = false;
    state.chickenSquishTime = 0;
    state.profitPopup.active = false;
    state.profitPopup.text = "";
    state.profitPopup.time = 0;
    state.profitPopup.duration = 0;
    state.deathFx.active = false;
    state.deathFx.time = 0;
    state.deathFx.duration = 0;
    state.deathFx.particles = [];
    state.deathStain.active = false;
    state.deathStain.lane = -1;
    state.deathStain.row = chicken.row;
    state.deathStain.intensity = 0;
    state.cameraLane = 0;
    state.cameraTargetLane = 0;
  }

  function generateLaneTerrain() {
    for (let lane = 0; lane < laneCount; lane += 1) {
      state.laneTerrain[lane] = lane === laneCount - 1 ? "finish" : "road";
    }
  }

  function getLaneTerrain(lane) {
    if (lane < 0 || lane >= laneCount) return "offroad";
    return state.laneTerrain[lane] || "road";
  }

  function updateHud() {
    balanceValue.textContent = formatMoney(state.balance);
    scoreValue.textContent = String(state.laneReached);
    multiplierValue.textContent = "x" + state.currentMultiplier.toFixed(2);
    updateFullscreenCash();
  }

  function setDifficultyLocked(locked) {
    root.querySelectorAll(".difficulty-btn").forEach((btn) => {
      btn.disabled = locked;
      btn.setAttribute("aria-disabled", locked ? "true" : "false");
    });
  }

  function setBettingLocked(locked) {
    betInput.disabled = locked;
    betInput.setAttribute("aria-disabled", locked ? "true" : "false");
    root.querySelectorAll("[data-bet-action]").forEach((btn) => {
      btn.disabled = locked;
      btn.setAttribute("aria-disabled", locked ? "true" : "false");
    });
  }

  function getMultiplierStep() {
    return multiplierStepByDifficulty[state.difficulty] || multiplierStepByDifficulty.medium;
  }

  function getMultiplierForCrossedLanes(crossedLanes) {
    const lanes = clamp(crossedLanes, 0, laneCount);
    return round2(1 + lanes * getMultiplierStep());
  }

  function getCurrentCarProbability(multiplier) {
    const table = carProbabilityByDifficulty[state.difficulty] || carProbabilityByDifficulty.medium;
    let chance = table[4];
    if (multiplier < 1.2) chance = table[0];
    else if (multiplier < 1.5) chance = table[1];
    else if (multiplier < 2.0) chance = table[2];
    else if (multiplier < 3.0) chance = table[3];

    if (state.bet > HIGH_BET_RIG_THRESHOLD) {
      chance += getHighBetRigChance(state.bet, 0.45);
    }

    return Math.min(0.995, chance);
  }

  function generateTrack(fromLaneIndex) {
    const from = clamp(fromLaneIndex, 0, laneCount - 1);
    for (let lane = from; lane < laneCount; lane += 1) {
      const terrain = getLaneTerrain(lane);
      if (terrain !== "road" || lane === laneCount - 1) {
        state.laneThreats[lane] = { type: "safe" };
        continue;
      }

      const currentMultiplier = getMultiplierForCrossedLanes(lane);
      const carChance = getCurrentCarProbability(currentMultiplier);
      if (Math.random() < carChance) {
        state.laneThreats[lane] = {
          type: "car",
          style: carStylePool[Math.floor(Math.random() * carStylePool.length)]
        };
      } else {
        state.laneThreats[lane] = { type: "safe" };
      }
    }
  }

  function startRun() {
    if (state.running) {
      if (state.carAttack.active) {
        setStatus("Incoming car. You cannot cash out during collision animation.");
        return;
      }
      finishRun("cashout");
      return;
    }
    if (!ensureCasinoBettingAllowedNow()) return;

    const parsedBet = Number(betInput.value);
    if (!Number.isFinite(parsedBet) || parsedBet < 0) {
      setStatus("Enter a valid bet amount.");
      return;
    }

    const cleanBet = round2(parsedBet);
    if (cleanBet < 0.01) {
      setStatus("Minimum bet is $0.01.");
      return;
    }

    if (cleanBet > state.balance) {
      setStatus("Bet is higher than your balance.");
      return;
    }

    state.bet = cleanBet;
    state.balance = round2(state.balance - cleanBet);
    syncGlobalCash();

    state.running = true;
    state.laneReached = 0;
    state.currentMultiplier = getMultiplierForCrossedLanes(0);
    state.cameraLane = 0;
    state.cameraTargetLane = 0;
    state.crashFlash = 0;
    state.chickenSquished = false;
    state.chickenSquishTime = 0;
    resultPanel.classList.add("hidden");

    resetChicken();
    generateLaneTerrain();
    generateTrack(0);
    startBtn.textContent = "Cash Out";
    setDifficultyLocked(true);
    setBettingLocked(true);
    setStatus("Round live: 19 road lanes, cash out anytime, or reach lane 20 to finish.");
    updateHud();
  }

  function finishRun(reason) {
    if (!state.running) return;

    state.running = false;
    startBtn.textContent = "Start Game";
    setDifficultyLocked(false);
    setBettingLocked(false);

    const multiplier = state.currentMultiplier;
    let payout = 0;
    if (reason !== "crash_car") {
      payout = round2(state.bet * multiplier);
      state.balance = round2(state.balance + payout);
      syncGlobalCash();
    }

    const net = round2(payout - state.bet);
    if (reason === "crash_car" || net < 0) playGameSound("loss");
    if (net > 0) showCasinoWinPopup({ amount: net, multiplier });
    triggerCasinoKickoutCheckAfterRound();
    state.moveQueue = [];
    state.nextMoveAt = 0;
    if (reason !== "crash_car") {
      state.chickenSquishTime = 0;
      state.chickenSquished = false;
    }
    state.carAttack.active = false;
    state.carAttack.phase = "idle";
    if (reason !== "crash_car") state.crashLane = -1;

    if (net > 0) {
      state.profitPopup.active = true;
      state.profitPopup.text = "+$" + net.toFixed(2) + " PROFIT";
      state.profitPopup.duration = 1.45;
      state.profitPopup.time = state.profitPopup.duration;
    } else {
      state.profitPopup.active = false;
      state.profitPopup.text = "";
      state.profitPopup.time = 0;
      state.profitPopup.duration = 0;
    }

    const textMap = {
      crash_car: "Hit by car",
      cashout: "Cashed out",
      finish: "Reached lane 20"
    };

    reasonValue.textContent = textMap[reason] || "Round ended";
    finalMultiplierValue.textContent = "x" + multiplier.toFixed(2);
    payoutValue.textContent = payout.toFixed(2);
    netValue.textContent = (net >= 0 ? "+$" : "-$") + Math.abs(net).toFixed(2);
    netValue.style.color = net >= 0 ? "#86ffb3" : "#ff8f8f";
    resultPanel.classList.remove("hidden");

    if (reason === "crash_car") setStatus("Crashed. Start again when ready.");
    else if (reason === "finish") setStatus("Finish reached on lane 20. Payout settled.");
    else setStatus("Cash out complete.");

    updateHud();
  }

  function laneCenterX(laneIndex) {
    const startLaneLeftX = geometry.roadLeft - state.cameraLane * geometry.laneWidth;
    const startWalkWidth = geometry.leftZone - geometry.sidewalk;
    const startCenter = startLaneLeftX - startWalkWidth * 0.5;
    const laneZeroCenter = startLaneLeftX + geometry.laneWidth * 0.5;
    if (laneIndex <= -1) return startCenter;
    if (laneIndex < 0) return startCenter + (laneZeroCenter - startCenter) * (laneIndex + 1);
    return geometry.roadLeft + geometry.laneWidth * (laneIndex - state.cameraLane + 0.5);
  }

  function rowCenterY(rowIndex) {
    return geometry.topPad + geometry.rowHeight * (rowIndex + 0.5);
  }

  function getMultiplierCircle(lane) {
    return {
      x: laneCenterX(lane),
      y: geometry.height * 0.6,
      radius: geometry.laneWidth * 0.18
    };
  }

  function findMultiplierAt(screenX, screenY) {
    const fromLane = Math.max(0, Math.floor(state.cameraLane) - 1);
    const toLane = Math.min(laneCount - 1, Math.ceil(state.cameraLane + visibleLaneCount) + 1);
    for (let lane = fromLane; lane <= toLane; lane += 1) {
      const circle = getMultiplierCircle(lane);
      const dx = screenX - circle.x;
      const dy = screenY - circle.y;
      const hitRadius = circle.radius * 1.45;
      if (dx * dx + dy * dy <= hitRadius * hitRadius) return lane;
    }

    const circleY = geometry.height * 0.6;
    const inLaneBand =
      screenY >= circleY - geometry.rowHeight * 0.72 && screenY <= circleY + geometry.rowHeight * 0.72;
    const inRoadX = screenX >= geometry.roadLeft && screenX <= geometry.roadLeft + geometry.roadWidth;
    if (!inLaneBand || !inRoadX) return -1;
    const lane = Math.floor((screenX - geometry.roadLeft) / geometry.laneWidth + state.cameraLane);
    return clamp(lane, 0, laneCount - 1);
  }

  function queueMoveToLane(targetLane) {
    if (!state.running || state.carAttack.active) return;
    if (targetLane < 0 || targetLane >= laneCount || targetLane <= chicken.lane) return;

    state.moveQueue = [];
    for (let i = 0; i < targetLane - chicken.lane; i += 1) {
      state.moveQueue.push(1);
    }
    state.nextMoveAt = 0;
  }

  function getLaneThreat(lane) {
    if (lane < 0 || lane >= laneCount) return { type: "safe" };
    if (!state.laneThreats[lane]) {
      if (lane === laneCount - 1 || getLaneTerrain(lane) !== "road") {
        state.laneThreats[lane] = { type: "safe" };
      } else {
        const chance = getCurrentCarProbability(getMultiplierForCrossedLanes(lane));
        if (Math.random() < chance) {
          state.laneThreats[lane] = {
            type: "car",
            style: carStylePool[Math.floor(Math.random() * carStylePool.length)]
          };
        } else {
          state.laneThreats[lane] = { type: "safe" };
        }
      }
    }
    return state.laneThreats[lane];
  }

  function getCrashSpeedByDifficulty() {
    if (state.difficulty === "easy") return 620;
    if (state.difficulty === "medium") return 820;
    if (state.difficulty === "hard") return 1040;
    return 1280;
  }

  function startCarAttack(lane, threat) {
    const attack = state.carAttack;
    attack.active = true;
    attack.phase = "warning";
    attack.lane = lane;
    attack.style = threat && threat.style ? threat.style : carStylePool[0];
    attack.targetY = rowCenterY(chicken.row);
    attack.dir = Math.random() < 0.5 ? 1 : -1;
    attack.baseSpeed = getCrashSpeedByDifficulty();
    attack.speed = attack.baseSpeed;
    attack.warningTime = 0.16;
    attack.impactTime = 0;
    attack.y = attack.dir > 0 ? -geometry.rowHeight * 1.2 : geometry.height + geometry.rowHeight * 1.2;
    state.crashLane = lane;
    playGameSound("car_screech", { restart: true, allowOverlap: false });
    setStatus("Car incoming!");
  }

  function triggerDeathEffects() {
    const fx = state.deathFx;
    const chickenSize = Math.min(geometry.laneWidth, geometry.rowHeight) * CHICKEN_SIZE_SCALE;
    const cx = laneCenterX(chicken.lane);
    const cy = rowCenterY(chicken.row) + geometry.rowHeight * 0.08;

    fx.active = true;
    fx.duration = 0.85;
    fx.time = fx.duration;
    fx.particles = [];

    for (let i = 0; i < 42; i += 1) {
      const angle = Math.random() * Math.PI * 2;
      const speed = 90 + Math.random() * 340;
      fx.particles.push({
        x: cx,
        y: cy,
        vx: Math.cos(angle) * speed,
        vy: Math.sin(angle) * speed - 70,
        size: chickenSize * (0.06 + Math.random() * 0.08),
        life: 0.32 + Math.random() * 0.65
      });
    }

    state.deathStain.active = true;
    state.deathStain.lane = chicken.lane;
    state.deathStain.row = chicken.row;
    state.deathStain.intensity = 1;
  }

  function updateDeathEffects(dt) {
    const fx = state.deathFx;
    if (!fx.active && fx.particles.length === 0) return;

    fx.time = Math.max(0, fx.time - dt);
    const next = [];
    for (let i = 0; i < fx.particles.length; i += 1) {
      const p = fx.particles[i];
      p.life -= dt;
      if (p.life <= 0) continue;
      p.vy += 420 * dt;
      p.x += p.vx * dt;
      p.y += p.vy * dt;
      p.vx *= 0.985;
      next.push(p);
    }
    fx.particles = next;
    if (fx.time <= 0 && fx.particles.length === 0) fx.active = false;
  }

  function updateCarAttack(dt) {
    const attack = state.carAttack;
    if (!attack.active) return;

    if (attack.phase === "warning") {
      attack.warningTime -= dt;
      if (attack.warningTime <= 0) attack.phase = "charge";
      return;
    }

    if (attack.phase === "charge") {
      const speedCap = attack.baseSpeed * 1.9;
      attack.speed = Math.min(speedCap, attack.speed + attack.baseSpeed * 3.4 * dt);
      attack.y += attack.dir * attack.speed * dt;
      const reached = attack.dir > 0 ? attack.y >= attack.targetY : attack.y <= attack.targetY;
      if (reached) {
        attack.y = attack.targetY;
        attack.phase = "impact";
        attack.impactTime = 0.1;
        state.crashFlash = 0.34;
        state.chickenSquished = true;
        state.chickenSquishTime = 0.28;
        playGameSound("chicken_dead", { restart: true, allowOverlap: false });
        triggerDeathEffects();
      }
      return;
    }

    if (attack.phase === "impact") {
      attack.impactTime -= dt;
      if (attack.impactTime <= 0) {
        attack.active = false;
        finishRun("crash_car");
      }
    }
  }

  function processQueuedMovement(now) {
    if (state.carAttack.active || state.moveQueue.length === 0 || now < state.nextMoveAt) return;

    const step = state.moveQueue.shift();
    if (typeof step !== "number") return;

    const targetLane = clamp(chicken.lane + step, -1, laneCount - 1);
    const threat = getLaneThreat(targetLane);
    if (threat.type === "car") {
      chicken.lane = targetLane;
      chicken.hopT = 0.12;
      playGameSound("chicken", { restart: true, allowOverlap: false });
      state.laneReached = Math.max(0, chicken.lane + 1);
      state.currentMultiplier = getMultiplierForCrossedLanes(state.laneReached);
      state.cameraTargetLane = clamp(chicken.lane - 1, 0, laneCount - visibleLaneCount);
      updateHud();
      state.moveQueue = [];
      state.nextMoveAt = 0;
      startCarAttack(targetLane, threat);
      return;
    }

    chicken.lane = targetLane;
    chicken.hopT = 0.18;
    playGameSound("chicken", { restart: true, allowOverlap: false });
    state.laneReached = Math.max(0, chicken.lane + 1);
    state.currentMultiplier = getMultiplierForCrossedLanes(state.laneReached);
    state.cameraTargetLane = clamp(chicken.lane - 1, 0, laneCount - visibleLaneCount);
    if (getLaneTerrain(targetLane) === "road" && threat.type === "safe" && !state.safeLaneBarriers[targetLane]) {
      state.safeLaneBarriers[targetLane] = true;
      state.safeLaneBarrierRevealAt[targetLane] = state.time;
    }
    state.nextMoveAt = now + 105;
    updateHud();
    if (state.laneReached >= laneCount) finishRun("finish");
  }

  function drawRoundedRect(x, y, w, h, radius, color) {
    const r = Math.min(radius, w * 0.5, h * 0.5);
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.lineTo(x + w - r, y);
    ctx.quadraticCurveTo(x + w, y, x + w, y + r);
    ctx.lineTo(x + w, y + h - r);
    ctx.quadraticCurveTo(x + w, y + h, x + w - r, y + h);
    ctx.lineTo(x + r, y + h);
    ctx.quadraticCurveTo(x, y + h, x, y + h - r);
    ctx.lineTo(x, y + r);
    ctx.quadraticCurveTo(x, y, x + r, y);
    ctx.closePath();
    ctx.fillStyle = color;
    ctx.fill();
  }

  function drawBackground() {
    const w = geometry.width;
    const h = geometry.height;

    ctx.fillStyle = "#32386f";
    ctx.fillRect(0, 0, w, h);

    const startLaneLeftX = geometry.roadLeft - state.cameraLane * geometry.laneWidth;
    const zoneX = startLaneLeftX - geometry.leftZone;
    const grassX = zoneX;
    const grassWidth = geometry.sidewalk;
    const walkX = zoneX + geometry.sidewalk;
    const walkWidth = geometry.roadLeft - geometry.sidewalk;

    if (grassX < w && grassX + grassWidth > 0) {
      ctx.fillStyle = "#0ca18f";
      ctx.fillRect(grassX, 0, grassWidth, h);
    }
    if (walkX < w && walkX + walkWidth > 0) {
      ctx.fillStyle = "#aeb8c2";
      ctx.fillRect(walkX, 0, walkWidth, h);
    }

    const laneFrom = Math.max(0, Math.floor(state.cameraLane) - 2);
    const laneTo = Math.min(laneCount - 1, Math.ceil(state.cameraLane + visibleLaneCount) + 2);
    for (let lane = laneFrom; lane <= laneTo; lane += 1) {
      const laneX = geometry.roadLeft + (lane - state.cameraLane) * geometry.laneWidth;
      if (laneX + geometry.laneWidth < geometry.roadLeft || laneX > geometry.roadLeft + geometry.roadWidth) {
        continue;
      }

      const terrain = getLaneTerrain(lane);
      if (terrain === "road") {
        ctx.fillStyle = lane % 2 === 0 ? "#2f3368" : "#32386f";
        ctx.fillRect(laneX, 0, geometry.laneWidth, h);
      } else if (terrain === "finish") {
        ctx.fillStyle = "#aeb8c2";
        ctx.fillRect(laneX, 0, geometry.laneWidth, h);
        const finishGrassW = geometry.laneWidth * 0.32;
        ctx.fillStyle = "#0ca18f";
        ctx.fillRect(laneX + geometry.laneWidth - finishGrassW, 0, finishGrassW, h);
      }
    }

    const edgeFrom = Math.max(0, Math.floor(state.cameraLane) - 1);
    const edgeTo = Math.min(laneCount, Math.ceil(state.cameraLane + visibleLaneCount) + 1);
    for (let edge = edgeFrom; edge <= edgeTo; edge += 1) {
      const x = geometry.roadLeft + (edge - state.cameraLane) * geometry.laneWidth;
      if (x < geometry.roadLeft - 2 || x > geometry.roadLeft + geometry.roadWidth + 2) continue;

      const leftTerrain = getLaneTerrain(edge - 1);
      const rightTerrain = getLaneTerrain(edge);
      if (!(leftTerrain === "road" && rightTerrain === "road")) continue;

      ctx.strokeStyle = "rgba(218, 228, 255, 0.92)";
      ctx.lineWidth = Math.max(2, geometry.width * 0.0024);
      ctx.setLineDash([18, 16]);
      ctx.beginPath();
      ctx.moveTo(x, 16);
      ctx.lineTo(x, h - 16);
      ctx.stroke();
      ctx.setLineDash([]);
    }
  }

  function drawSidewalkProps() {
    const startLaneLeftX = geometry.roadLeft - state.cameraLane * geometry.laneWidth;
    const zoneX = startLaneLeftX - geometry.leftZone;
    const grassX = zoneX;
    const grassWidth = geometry.sidewalk;
    const walkX = zoneX + geometry.sidewalk;
    const walkWidth = geometry.roadLeft - geometry.sidewalk;

    const drawBush = (x, y, scale) => {
      ctx.fillStyle = "#0a7d65";
      ctx.beginPath();
      ctx.arc(x - 14 * scale, y + 3 * scale, 14 * scale, 0, Math.PI * 2);
      ctx.arc(x, y - 8 * scale, 18 * scale, 0, Math.PI * 2);
      ctx.arc(x + 15 * scale, y + 5 * scale, 13 * scale, 0, Math.PI * 2);
      ctx.fill();

      ctx.fillStyle = "#0b8f73";
      ctx.beginPath();
      ctx.arc(x - 8 * scale, y, 10 * scale, 0, Math.PI * 2);
      ctx.arc(x + 7 * scale, y - 2 * scale, 9 * scale, 0, Math.PI * 2);
      ctx.fill();
    };

    const drawHydrant = (x, y) => {
      drawRoundedRect(x - 15, y - 20, 30, 24, 12, "#f84834");
      drawRoundedRect(x - 11, y - 39, 22, 22, 11, "#ff5a41");
      drawRoundedRect(x - 5, y - 45, 10, 7, 4, "#ff7a64");
    };

    const bushBaseX = grassX + grassWidth * 0.5;
    if (grassX < geometry.width && grassX + grassWidth > -40) {
      drawBush(bushBaseX, geometry.height * 0.16, 1.1);
      drawBush(bushBaseX + 2, geometry.height * 0.33, 0.95);
      drawBush(bushBaseX - 1, geometry.height * 0.86, 1.05);
    }

    const hydrantY = geometry.height - 58;
    const hydrantX = walkX + walkWidth * 0.35;
    if (hydrantX > -40 && hydrantX < geometry.width + 40) {
      drawHydrant(hydrantX, hydrantY);
    }

    const finishLane = laneCount - 1;
    const finishLaneX = geometry.roadLeft + (finishLane - state.cameraLane) * geometry.laneWidth;
    const finishGrassW = geometry.laneWidth * 0.32;
    const finishGrassX = finishLaneX + geometry.laneWidth - finishGrassW;
    const finishWalkX = finishLaneX;
    const finishWalkW = geometry.laneWidth - finishGrassW;
    const finishBushX = finishGrassX + finishGrassW * 0.5;
    const finishHydrantX = finishWalkX + finishWalkW * 0.65;

    if (finishGrassX < geometry.width + 40 && finishGrassX + finishGrassW > -40) {
      drawBush(finishBushX, geometry.height * 0.16, 1.1);
      drawBush(finishBushX + 2, geometry.height * 0.33, 0.95);
      drawBush(finishBushX - 1, geometry.height * 0.86, 1.05);
    }

    if (finishHydrantX > -40 && finishHydrantX < geometry.width + 40) {
      drawHydrant(finishHydrantX, hydrantY);
    }
  }

  function drawMultipliers() {
    const fromLane = Math.max(0, Math.floor(state.cameraLane) - 1);
    const toLane = Math.min(laneCount - 1, Math.ceil(state.cameraLane + visibleLaneCount) + 1);
    for (let lane = fromLane; lane <= toLane; lane += 1) {
      const circle = getMultiplierCircle(lane);
      const x = circle.x;
      const y = circle.y;
      const radius = circle.radius;
      const active = chicken.lane >= lane;
      const hovered = state.hoverLane === lane && state.running;
      const terrain = getLaneTerrain(lane);

      if (terrain === "finish") {
        const finishText = getMultiplierForCrossedLanes(lane + 1).toFixed(2) + "x";
        const labelW = radius * 2.3;
        const labelH = radius * 0.9;
        drawRoundedRect(
          x - labelW * 0.5,
          y - labelH * 0.5,
          labelW,
          labelH,
          labelH * 0.45,
          active ? "#30d47d" : "#239f5e"
        );
        ctx.font = "800 " + Math.max(11, geometry.width * 0.009) + "px Trebuchet MS";
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillStyle = "#effff5";
        ctx.fillText(finishText, x, y + 1);
        continue;
      }

      const outerFill = active ? "#6e789f" : hovered ? "#5f698f" : "#4f5879";
      const innerFill = active ? "#4f587f" : hovered ? "#454d70" : "#3a4160";
      const grateColor = active ? "#a6b4e8" : hovered ? "#9caadb" : "#7f89b5";
      const rimStroke = active ? "#c6d2ff" : hovered ? "#b5c0ea" : "#8c97c2";

      ctx.beginPath();
      ctx.arc(x, y, radius, 0, Math.PI * 2);
      ctx.fillStyle = outerFill;
      ctx.fill();
      ctx.lineWidth = hovered ? 4 : 3;
      ctx.strokeStyle = rimStroke;
      ctx.stroke();

      ctx.beginPath();
      ctx.arc(x, y, radius * 0.8, 0, Math.PI * 2);
      ctx.fillStyle = innerFill;
      ctx.fill();

      ctx.beginPath();
      ctx.arc(x, y, radius * 0.54, 0, Math.PI * 2);
      ctx.lineWidth = Math.max(1, radius * 0.12);
      ctx.strokeStyle = "rgba(22, 28, 46, 0.65)";
      ctx.stroke();

      ctx.strokeStyle = grateColor;
      ctx.lineWidth = Math.max(1, radius * 0.07);
      for (let i = -2; i <= 2; i += 1) {
        const gx = x + i * radius * 0.18;
        ctx.beginPath();
        ctx.moveTo(gx, y - radius * 0.34);
        ctx.lineTo(gx, y + radius * 0.34);
        ctx.stroke();
      }

      for (let bolt = 0; bolt < 6; bolt += 1) {
        const angle = (Math.PI * 2 * bolt) / 6;
        const bx = x + Math.cos(angle) * radius * 0.66;
        const by = y + Math.sin(angle) * radius * 0.66;
        ctx.beginPath();
        ctx.arc(bx, by, Math.max(1.5, radius * 0.06), 0, Math.PI * 2);
        ctx.fillStyle = "#d9e2ff";
        ctx.fill();
      }

      ctx.font = "700 " + Math.max(10, geometry.width * 0.0084) + "px Trebuchet MS";
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillStyle = active ? "#f3f6ff" : "#d1d8ef";
      ctx.fillText(getMultiplierForCrossedLanes(lane + 1).toFixed(2) + "x", x, y + 1);
    }
  }

  function drawLaneCar(lane, centerY, motionPhase, carStyle) {
    const cx = laneCenterX(lane);
    const y = typeof centerY === "number" ? centerY : rowCenterY(chicken.row);
    const style = carStyle || carStylePool[0];
    const carScale = Math.min(geometry.rowHeight * 1.26, geometry.laneWidth * 0.66);
    const h = carScale * 0.98;
    const w = carScale * 0.62;
    const x = cx - w * 0.5;
    const bob = Math.sin(motionPhase * 18) * h * 0.02;
    const top = y - h * 0.5 + bob;

    drawRoundedRect(x + w * 0.04, top + h * 0.04, w * 0.92, h * 0.92, Math.min(22, w * 0.28), style.body);
    drawRoundedRect(x + w * 0.17, top + h * 0.18, w * 0.66, h * 0.52, Math.min(14, w * 0.2), style.roof);
    drawRoundedRect(x + w * 0.2, top + h * 0.24, w * 0.6, h * 0.18, Math.min(10, w * 0.15), style.glass);
    drawRoundedRect(x + w * 0.2, top + h * 0.46, w * 0.6, h * 0.12, Math.min(8, w * 0.12), style.glass);
    drawRoundedRect(x + w * 0.31, top + h * 0.03, w * 0.38, h * 0.11, Math.min(8, w * 0.1), style.accent);
    drawRoundedRect(x + w * 0.02, top + h * 0.45, w * 0.07, h * 0.12, 4, style.accent);
    drawRoundedRect(x + w * 0.91, top + h * 0.45, w * 0.07, h * 0.12, 4, style.accent);

    const wheelColor = "#1b1f33";
    drawRoundedRect(x + w * 0.09, top + h * 0.11, w * 0.11, h * 0.11, 4, wheelColor);
    drawRoundedRect(x + w * 0.8, top + h * 0.11, w * 0.11, h * 0.11, 4, wheelColor);
    drawRoundedRect(x + w * 0.09, top + h * 0.78, w * 0.11, h * 0.11, 4, wheelColor);
    drawRoundedRect(x + w * 0.8, top + h * 0.78, w * 0.11, h * 0.11, 4, wheelColor);

    const lampPulse = 0.45 + Math.abs(Math.sin(motionPhase * 20)) * 0.55;
    drawRoundedRect(
      x + w * 0.11,
      top + h * 0.85,
      w * 0.13,
      h * 0.07,
      4,
      "rgba(255, 243, 170, " + lampPulse.toFixed(3) + ")"
    );
    drawRoundedRect(
      x + w * 0.76,
      top + h * 0.85,
      w * 0.13,
      h * 0.07,
      4,
      "rgba(255, 243, 170, " + lampPulse.toFixed(3) + ")"
    );
  }

  function drawLaneThreats() {
    const attack = state.carAttack;
    if (attack.active) {
      const laneX = laneCenterX(attack.lane) - geometry.laneWidth * 0.5;
      if (attack.phase === "warning") {
        ctx.fillStyle = "rgba(255, 63, 63, 0.22)";
        ctx.fillRect(laneX, 0, geometry.laneWidth, geometry.height);
      }
      drawLaneCar(attack.lane, attack.y, state.time, attack.style);
      return;
    }

    if (state.crashLane < 0) return;
    const threat = state.laneThreats[state.crashLane];
    drawLaneCar(state.crashLane, rowCenterY(chicken.row), state.time, threat && threat.style ? threat.style : carStylePool[0]);
  }

  function drawSafeLaneBarriers() {
    const fromLane = Math.max(0, Math.floor(state.cameraLane) - 1);
    const toLane = Math.min(laneCount - 1, Math.ceil(state.cameraLane + visibleLaneCount) + 1);
    const sewerY = geometry.height * 0.6;
    const cy = (geometry.topPad + sewerY) * 0.5;
    const w = Math.min(geometry.laneWidth * 0.68, geometry.rowHeight * 1.42);
    const h = Math.max(11, geometry.rowHeight * 0.18);

    for (let lane = fromLane; lane <= toLane; lane += 1) {
      if (!state.safeLaneBarriers[lane]) continue;
      const cx = laneCenterX(lane);
      if (cx < geometry.roadLeft - geometry.laneWidth || cx > geometry.roadLeft + geometry.roadWidth + geometry.laneWidth) {
        continue;
      }

      const revealStart = state.safeLaneBarrierRevealAt[lane];
      const revealAge = revealStart >= 0 ? Math.max(0, state.time - revealStart) : 999;
      const revealT = clamp(revealAge / 0.34, 0, 1);
      const revealEase = 1 - Math.pow(1 - revealT, 3);
      const bounce = Math.sin(revealT * Math.PI) * (1 - revealT) * 0.12;
      const introScale = 0.7 + revealEase * 0.3 + bounce;
      const introRise = (1 - revealEase) * h * 0.95;
      const alpha = 0.18 + revealEase * 0.82;

      const phase = state.time * 3.4 + lane * 0.65;
      const bob = Math.sin(phase) * h * 0.07;
      const y = cy + bob + introRise;
      const pulse = 0.55 + 0.45 * Math.abs(Math.sin(state.time * 5.2 + lane * 0.9));
      const drawW = w * introScale;
      const drawH = h * introScale;

      ctx.save();
      ctx.globalAlpha = alpha;

      ctx.beginPath();
      ctx.ellipse(cx, y + drawH * 1.05, drawW * 0.45, drawH * 0.33, 0, 0, Math.PI * 2);
      ctx.fillStyle = "rgba(7, 10, 20, 0.32)";
      ctx.fill();

      const postW = drawW * 0.09;
      const postH = drawH * 1.15;
      drawRoundedRect(cx - drawW * 0.5 + postW * 0.2, y - postH * 0.45, postW, postH, 3, "#152543");
      drawRoundedRect(cx + drawW * 0.5 - postW * 1.2, y - postH * 0.45, postW, postH, 3, "#152543");

      drawRoundedRect(cx - drawW * 0.5, y - drawH * 0.45, drawW, drawH * 0.82, drawH * 0.28, "#ee6c4d");
      drawRoundedRect(cx - drawW * 0.5, y + drawH * 0.2, drawW, drawH * 0.28, drawH * 0.12, "#1f355a");

      const stripeW = drawW * 0.15;
      const stripeGap = drawW * 0.11;
      const stripeY = y - drawH * 0.23;
      for (let i = -1; i <= 1; i += 1) {
        drawRoundedRect(
          cx + i * (stripeW + stripeGap) - stripeW * 0.5,
          stripeY,
          stripeW,
          drawH * 0.44,
          3,
          "#f7f8ff"
        );
      }

      const sweepX = cx - drawW * 0.55 + ((state.time * 110 + lane * 26) % (drawW * 1.1));
      ctx.save();
      ctx.beginPath();
      ctx.rect(cx - drawW * 0.5, y - drawH * 0.45, drawW, drawH * 0.82);
      ctx.clip();
      ctx.fillStyle = "rgba(255, 255, 255, 0.16)";
      ctx.fillRect(sweepX, y - drawH * 0.55, drawW * 0.18, drawH * 1.1);
      ctx.restore();

      const lightR = drawH * 0.14;
      const lightY = y - drawH * 0.56;
      const lightA = "rgba(255, 223, 110, " + (0.35 + pulse * 0.65).toFixed(3) + ")";
      ctx.beginPath();
      ctx.arc(cx - drawW * 0.28, lightY, lightR, 0, Math.PI * 2);
      ctx.arc(cx + drawW * 0.28, lightY, lightR, 0, Math.PI * 2);
      ctx.fillStyle = lightA;
      ctx.fill();

      ctx.restore();
    }
  }

  function drawDeathEffects() {
    if (state.deathStain.active && state.deathStain.lane >= 0) {
      const chickenSize = Math.min(geometry.laneWidth, geometry.rowHeight) * CHICKEN_SIZE_SCALE;
      const cx = laneCenterX(state.deathStain.lane);
      const cy = rowCenterY(state.deathStain.row) + geometry.rowHeight * 0.14;
      const r = chickenSize * 0.4;

      ctx.beginPath();
      ctx.ellipse(cx, cy, r * 1.7, r * 0.68, 0, 0, Math.PI * 2);
      ctx.fillStyle = "rgba(126, 18, 18, 0.56)";
      ctx.fill();

      ctx.fillStyle = "rgba(178, 28, 28, 0.5)";
      for (let i = 0; i < 10; i += 1) {
        const a = (Math.PI * 2 * i) / 10 + 0.34;
        const sx = cx + Math.cos(a) * r * (0.8 + (i % 2) * 0.35);
        const sy = cy + Math.sin(a) * r * (0.25 + (i % 3) * 0.14);
        ctx.beginPath();
        ctx.arc(sx, sy, r * (0.16 + (i % 3) * 0.04), 0, Math.PI * 2);
        ctx.fill();
      }
    }

    const fx = state.deathFx;
    if (!fx.active && fx.particles.length === 0) return;

    for (let i = 0; i < fx.particles.length; i += 1) {
      const p = fx.particles[i];
      const a = clamp(p.life / 0.5, 0, 1);
      ctx.beginPath();
      ctx.ellipse(p.x, p.y, p.size, p.size * 0.8, 0, 0, Math.PI * 2);
      ctx.fillStyle = "rgba(195, 22, 22, " + a.toFixed(3) + ")";
      ctx.fill();
    }
  }

  function drawChicken() {
    const x = laneCenterX(chicken.drawLane);
    const y = rowCenterY(chicken.drawRow);
    const size = Math.min(geometry.laneWidth, geometry.rowHeight) * CHICKEN_SIZE_SCALE;
    const hop = chicken.hopT > 0 ? Math.sin((1 - chicken.hopT / 0.18) * Math.PI) * size * 0.14 : 0;
    const cx = x;
    const cy = y - hop;
    const showSquished = state.chickenSquishTime > 0 || (state.chickenSquished && !state.running);

    if (showSquished) {
      const intensity = Math.min(1, state.chickenSquishTime / 0.28);
      const squishW = size * (0.66 + intensity * 0.24);
      const squishH = size * (0.2 + (1 - intensity) * 0.05);

      ctx.beginPath();
      ctx.ellipse(cx, cy + size * 0.2, squishW * 1.2, squishH * 1.05, 0, 0, Math.PI * 2);
      ctx.fillStyle = "rgba(120, 18, 18, 0.42)";
      ctx.fill();
      ctx.beginPath();
      ctx.ellipse(cx, cy + size * 0.1, squishW * 0.92, squishH * 0.88, 0, 0, Math.PI * 2);
      ctx.fillStyle = "#f4f5fb";
      ctx.fill();

      ctx.fillStyle = "rgba(174, 26, 26, 0.6)";
      for (let i = 0; i < 5; i += 1) {
        const a = (Math.PI * 2 * i) / 5 + 0.4;
        const px = cx + Math.cos(a) * squishW * (0.85 + (i % 2) * 0.22);
        const py = cy + size * 0.14 + Math.sin(a) * squishH * 0.55;
        ctx.beginPath();
        ctx.arc(px, py, squishH * (0.28 + (i % 3) * 0.07), 0, Math.PI * 2);
        ctx.fill();
      }
      return;
    }

    ctx.beginPath();
    ctx.ellipse(cx, cy + size * 0.12, size * 0.34, size * 0.27, 0, 0, Math.PI * 2);
    ctx.fillStyle = "rgba(18, 24, 34, 0.22)";
    ctx.fill();

    ctx.beginPath();
    ctx.ellipse(cx, cy, size * 0.33, size * 0.28, 0, 0, Math.PI * 2);
    ctx.fillStyle = "#fbfcff";
    ctx.fill();

    ctx.beginPath();
    ctx.ellipse(cx - size * 0.11, cy + size * 0.02, size * 0.15, size * 0.13, -0.35, 0, Math.PI * 2);
    ctx.fillStyle = "#e8edf7";
    ctx.fill();

    const headX = cx + size * 0.14;
    const headY = cy - size * 0.13;
    ctx.beginPath();
    ctx.arc(headX, headY, size * 0.13, 0, Math.PI * 2);
    ctx.fillStyle = "#ffffff";
    ctx.fill();

    ctx.fillStyle = "#e84f4f";
    ctx.beginPath();
    ctx.arc(headX - size * 0.07, headY - size * 0.12, size * 0.045, 0, Math.PI * 2);
    ctx.arc(headX - size * 0.015, headY - size * 0.14, size * 0.05, 0, Math.PI * 2);
    ctx.arc(headX + size * 0.045, headY - size * 0.12, size * 0.045, 0, Math.PI * 2);
    ctx.fill();

    ctx.fillStyle = "#f5bd2d";
    ctx.beginPath();
    ctx.moveTo(headX + size * 0.12, headY);
    ctx.lineTo(headX + size * 0.22, headY + size * 0.03);
    ctx.lineTo(headX + size * 0.12, headY + size * 0.06);
    ctx.closePath();
    ctx.fill();

    ctx.fillStyle = "#1a223a";
    ctx.beginPath();
    ctx.arc(headX + size * 0.035, headY - size * 0.015, size * 0.018, 0, Math.PI * 2);
    ctx.fill();
  }

  function drawProfitPopup() {
    const popup = state.profitPopup;
    if (!popup.active || popup.time <= 0) return;

    const life = popup.duration > 0 ? popup.time / popup.duration : 0;
    const progress = 1 - life;
    const fadeIn = Math.min(1, progress / 0.18);
    const fadeOut = Math.min(1, life / 0.35);
    const alpha = Math.min(fadeIn, fadeOut);
    const y = geometry.height * 0.24 - progress * 36;
    const x = geometry.width * 0.5;

    ctx.save();
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.font = "900 " + Math.max(22, geometry.width * 0.03) + "px Trebuchet MS";
    ctx.fillStyle = "rgba(18, 255, 122, " + alpha.toFixed(3) + ")";
    ctx.strokeStyle = "rgba(6, 36, 20, " + (alpha * 0.85).toFixed(3) + ")";
    ctx.lineWidth = Math.max(2, geometry.width * 0.003);
    ctx.strokeText(popup.text, x, y);
    ctx.fillText(popup.text, x, y);
    ctx.restore();
  }

  function drawScene() {
    drawBackground();
    drawSidewalkProps();
    drawSafeLaneBarriers();
    drawMultipliers();
    drawDeathEffects();

    const carShouldOverlayChicken =
      state.carAttack.active || (state.crashLane >= 0 && (state.chickenSquishTime > 0 || state.chickenSquished));
    if (carShouldOverlayChicken) {
      drawChicken();
      drawLaneThreats();
    } else {
      drawLaneThreats();
      drawChicken();
    }
    drawProfitPopup();

    if (state.crashFlash > 0) {
      ctx.fillStyle = "rgba(255, 58, 58, " + (state.crashFlash * 2.5).toFixed(3) + ")";
      ctx.fillRect(0, 0, geometry.width, geometry.height);
    }
  }

  function setDifficulty(nextDifficulty) {
    if (state.running) return;
    if (!multiplierStepByDifficulty[nextDifficulty]) return;

    state.difficulty = nextDifficulty;
    state.currentMultiplier = getMultiplierForCrossedLanes(state.laneReached);
    root.querySelectorAll(".difficulty-btn").forEach((btn) => {
      btn.classList.toggle("active", btn.getAttribute("data-difficulty") === nextDifficulty);
    });
    updateHud();
  }

  function handleBetShortcut(action) {
    const raw = Number(betInput.value);
    const current = Number.isFinite(raw) ? raw : 0;
    if (action === "half") betInput.value = round2(current * 0.5).toFixed(2);
    if (action === "double") betInput.value = round2(current * 2).toFixed(2);
    if (action === "max") betInput.value = round2(state.balance).toFixed(2);
  }

  const onStartClick = () => {
    if (!destroyed) startRun();
  };
  const onCanvasPointerMove = (event) => {
    if (destroyed) return;
    const { x, y } = getCanvasPointerPosition(event);
    state.hoverLane = findMultiplierAt(x, y);
    canvas.style.cursor = state.running && state.hoverLane >= 0 ? "pointer" : "default";
  };
  const onCanvasPointerLeave = () => {
    if (destroyed) return;
    state.hoverLane = -1;
    canvas.style.cursor = "default";
  };
  const onCanvasPointerDown = (event) => {
    if (destroyed) return;
    const { x, y } = getCanvasPointerPosition(event);
    const lane = findMultiplierAt(x, y);
    if (lane < 0) return;
    event.preventDefault();
    if (!state.running) {
      setStatus("Press Start Game first, then click a multiplier circle.");
      return;
    }
    queueMoveToLane(lane);
  };
  const onWindowResize = () => {
    if (!destroyed) scheduleResizeCanvas();
  };
  const onWindowKeyDown = (event) => {
    if (destroyed || event.code !== "Space") return;
    event.preventDefault();
    if (!state.running) {
      startRun();
      return;
    }
    if (state.carAttack.active) return;
    if (chicken.lane < laneCount - 1) queueMoveToLane(chicken.lane + 1);
    else finishRun("finish");
  };

  function bindEvents() {
    startBtn.addEventListener("click", onStartClick);

    betInput.addEventListener("change", () => {
      if (destroyed) return;
      const value = Number(betInput.value);
      if (!Number.isFinite(value) || value < 0) betInput.value = "0.00";
      else betInput.value = round2(value).toFixed(2);
    });

    root.querySelectorAll("[data-bet-action]").forEach((btn) => {
      btn.addEventListener("click", () => {
        if (!destroyed) handleBetShortcut(btn.getAttribute("data-bet-action"));
      });
    });

    root.querySelectorAll(".difficulty-btn").forEach((btn) => {
      const applyDifficulty = () => {
        if (!destroyed) setDifficulty(btn.getAttribute("data-difficulty"));
      };
      btn.addEventListener("pointerdown", (event) => {
        event.preventDefault();
        applyDifficulty();
      });
      btn.addEventListener("keydown", (event) => {
        if (event.code !== "Enter" && event.code !== "Space") return;
        event.preventDefault();
        applyDifficulty();
      });
    });

    canvas.addEventListener("pointermove", onCanvasPointerMove);
    canvas.addEventListener("pointerleave", onCanvasPointerLeave);
    canvas.addEventListener("pointerdown", onCanvasPointerDown);
    window.addEventListener("resize", onWindowResize);
    window.addEventListener("keydown", onWindowKeyDown);
    if (typeof ResizeObserver === "function" && stageCard) {
      resizeObserver = new ResizeObserver(() => {
        scheduleResizeCanvas();
      });
      resizeObserver.observe(stageCard);
    }
  }

  let previousTime = performance.now();
  function gameLoop(now) {
    if (destroyed) return;

    const dt = Math.min(0.033, (now - previousTime) / 1000);
    previousTime = now;
    state.time += dt;

    if (state.running) {
      processQueuedMovement(now);
      updateCarAttack(dt);
    }

    updateDeathEffects(dt);
    state.cameraLane += (state.cameraTargetLane - state.cameraLane) * Math.min(1, dt * 8);
    chicken.drawLane += (chicken.lane - chicken.drawLane) * Math.min(1, dt * 17);
    chicken.drawRow += (chicken.row - chicken.drawRow) * Math.min(1, dt * 17);

    if (chicken.hopT > 0) chicken.hopT = Math.max(0, chicken.hopT - dt);
    if (state.crashFlash > 0) state.crashFlash = Math.max(0, state.crashFlash - dt);
    if (state.chickenSquishTime > 0) state.chickenSquishTime = Math.max(0, state.chickenSquishTime - dt);
    if (state.profitPopup.active) {
      state.profitPopup.time = Math.max(0, state.profitPopup.time - dt);
      if (state.profitPopup.time <= 0) {
        state.profitPopup.active = false;
        state.profitPopup.text = "";
        state.profitPopup.duration = 0;
      }
    }

    drawScene();
    animationId = window.requestAnimationFrame(gameLoop);
  }

  resizeCanvas();
  resetChicken();
  state.currentMultiplier = getMultiplierForCrossedLanes(0);
  syncGlobalCash();
  updateHud();
  bindEvents();
  setDifficultyLocked(false);
  setBettingLocked(false);
  setStatus(IS_PHONE_EMBED_MODE ? "Set your bet and press Start Game. Lanes 1-19 are road lanes." : "");
  window.requestAnimationFrame(() => {
    if (destroyed) return;
    resizeCanvas();
    window.requestAnimationFrame(() => {
      if (!destroyed) resizeCanvas();
    });
  });
  animationId = window.requestAnimationFrame(gameLoop);

  activeCasinoCleanup = () => {
    destroyed = true;
    if (resizeFrameId) {
      window.cancelAnimationFrame(resizeFrameId);
      resizeFrameId = 0;
    }
    if (animationId !== null) {
      window.cancelAnimationFrame(animationId);
      animationId = null;
    }
    if (resizeObserver) {
      resizeObserver.disconnect();
      resizeObserver = null;
    }
    window.removeEventListener("resize", onWindowResize);
    window.removeEventListener("keydown", onWindowKeyDown);
    canvas.removeEventListener("pointermove", onCanvasPointerMove);
    canvas.removeEventListener("pointerleave", onCanvasPointerLeave);
    canvas.removeEventListener("pointerdown", onCanvasPointerDown);
    startBtn.removeEventListener("click", onStartClick);
    container.classList.remove("casino-fullbleed", "crossy-fullbleed");
  };
}

function loadCasinoRouletteClassic() {
  const container = document.getElementById("casino-container");
  if (!container) return;
  container.classList.add("casino-fullbleed", "casino-roulette-classic-fullbleed");
  container.innerHTML = `
    <section id="casino-roulette-screen" class="casino-screen">
      <div class="casino-roulette-root" data-instance="casino">
        <main class="app-shell">
          <aside class="left-panel">
            <div class="mode-tabs">
              <button class="mode-btn active" data-role="manualModeBtn" data-mode="manual" type="button">Manual</button>
              <button class="mode-btn" data-role="autoModeBtn" data-mode="auto" type="button">Auto</button>
            </div>

            <div class="control-block">
              <p class="label-line">
                Chip Value
                <span data-role="chipValueLabel">$10.00</span>
              </p>

              <div class="chip-strip">
                <button class="strip-arrow" type="button" aria-hidden="true">&lsaquo;</button>
                <div class="chips-row" data-role="chipsRow">
                  <button class="chip-option" data-chip="1" type="button">$1</button>
                  <button class="chip-option" data-chip="5" type="button">$5</button>
                  <button class="chip-option active" data-chip="10" type="button">$10</button>
                  <button class="chip-option" data-chip="25" type="button">$25</button>
                  <button class="chip-option" data-chip="100" type="button">$100</button>
                  <button class="chip-option" data-chip="500" type="button">$500</button>
                  <button class="chip-option" data-chip="1000" type="button">$1K</button>
                </div>
                <button class="strip-arrow" type="button" aria-hidden="true">&rsaquo;</button>
              </div>
            </div>

            <div class="control-block">
              <p class="label-line">Total Amount</p>
              <div class="total-box">
                <span data-role="totalBetValue">$0.00</span>
              </div>
            </div>

            <button class="play-btn" data-role="spinBtn" type="button">Play</button>
            <button class="play-btn secondary-btn" data-role="rebetTopBtn" type="button">Rebet</button>

            <div class="meta-lines">
              <p>Balance <strong data-role="balanceValue">$1,000.00</strong></p>
              <p>Potential <strong data-role="potentialValue">$0.00</strong></p>
            </div>
          </aside>

          <section class="main-panel" data-role="tablePanel">
            <div class="profit-popup" data-role="profitPopup" aria-live="polite">
              <div class="profit-multiplier" data-role="profitMultiplier">0.00x</div>
              <div class="profit-divider"></div>
              <div class="profit-amount-row">
                <span class="profit-amount" data-role="profitAmount">$0.00</span>
                <span class="profit-currency">$</span>
              </div>
            </div>

            <header class="top-stage">
              <div class="result-pill" data-role="resultPill">Result: -</div>

              <div class="wheel-stage">
                <div class="wheel" data-role="wheel"></div>
                <div class="ball-orbit" data-role="ballOrbit">
                  <div class="ball"></div>
                </div>
                <div class="spokes" aria-hidden="true">
                  <span></span>
                  <span></span>
                  <span></span>
                </div>
              </div>

              <p class="last-result">Last Result: <span data-role="lastResult">-</span></p>
              <p class="status-line" data-role="statusLine">Place your bets and spin.</p>
            </header>

            <section class="table-stage">
              <div class="number-board-wrap" data-role="numbersZone">
                <button class="bet-area zero-cell" data-bet-id="straight-0" type="button">0</button>

                <div class="number-grid" data-role="numberGrid"></div>
                <div class="overlay-layer" data-role="overlayLayer"></div>

                <div class="column-bets">
                  <button class="bet-area side-bet" data-bet-id="column-3" type="button">2:1</button>
                  <button class="bet-area side-bet" data-bet-id="column-2" type="button">2:1</button>
                  <button class="bet-area side-bet" data-bet-id="column-1" type="button">2:1</button>
                </div>
              </div>

              <div class="dozens-row">
                <button class="bet-area outside-bet" data-bet-id="dozen-1" type="button">1 to 12</button>
                <button class="bet-area outside-bet" data-bet-id="dozen-2" type="button">13 to 24</button>
                <button class="bet-area outside-bet" data-bet-id="dozen-3" type="button">25 to 36</button>
              </div>

              <div class="outside-row">
                <button class="bet-area outside-bet" data-bet-id="outside-low" type="button">1 to 18</button>
                <button class="bet-area outside-bet" data-bet-id="outside-even" type="button">Even</button>
                <button class="bet-area outside-bet red-bet" data-bet-id="outside-red" type="button" aria-label="Red"></button>
                <button class="bet-area outside-bet black-bet" data-bet-id="outside-black" type="button" aria-label="Black"></button>
                <button class="bet-area outside-bet" data-bet-id="outside-odd" type="button">Odd</button>
                <button class="bet-area outside-bet" data-bet-id="outside-high" type="button">19 to 36</button>
              </div>
            </section>

            <footer class="table-footer">
              <button class="footer-btn" data-role="rebetBtn" type="button">Rebet</button>
              <button class="footer-btn" data-role="clearBtn" type="button">Clear</button>
            </footer>
          </section>

          <aside class="history-panel">
            <h2>History</h2>
            <div class="history-list" data-role="historyList"></div>
          </aside>
        </main>
      </div>
    </section>
  `;

  const root = container.querySelector("#casino-roulette-screen .casino-roulette-root");
  if (!root) return;
  addSageBrand(root, "bottom-left", "sage-roulette-corner");
  const instance = createCasinoRouletteInstance(root, {
    getBalance: () => roundCurrency(cash),
    setBalance: (nextBalance) => {
      cash = roundCurrency(Math.max(0, nextBalance));
      updateUI();
      updateBlackjackCash();
      updateFullscreenCash();
    }
  });

  activeCasinoCleanup = () => {
    if (instance && typeof instance.destroy === "function") instance.destroy();
    container.classList.remove("casino-fullbleed", "casino-roulette-classic-fullbleed");
  };
}

function createCasinoRouletteInstance(rootEl, options = {}) {
  const START_BALANCE = Math.max(0, Number(options.getBalance?.() ?? 1000));
  const SPIN_DURATION_MS = 3400;
  const AUTO_ROUND_DELAY_MS = 260;
  const RESULT_HISTORY_LIMIT = 24;
  const PROFIT_POPUP_MS = 2400;
  const WHEEL_GRADIENT_START_ANGLE = -90;
  const RED_NUMBERS = new Set([1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36]);
  const WHEEL_ORDER = [
    0, 32, 15, 19, 4, 21, 2, 25, 17, 34, 6, 27, 13, 36, 11, 30, 8, 23, 10, 5, 24, 16, 33, 1, 20, 14, 31, 9, 22,
    18, 29, 7, 28, 12, 35, 3, 26
  ];

  const state = {
    balance: START_BALANCE,
    selectedChip: 10,
    mode: "manual",
    autoRunning: false,
    autoTemplateBets: new Map(),
    autoRunId: 0,
    isSpinning: false,
    wheelRotation: 0,
    ballRotation: 0,
    resultHistory: [],
    placedBets: new Map(),
    lastBets: new Map(),
    betDefinitions: new Map(),
    comboBetIds: new Set(),
    destroyed: false
  };

  const queryRole = (role) => rootEl.querySelector(`[data-role="${role}"]`);
  const tablePanel = queryRole("tablePanel");
  const numbersZone = queryRole("numbersZone");
  const numberGrid = queryRole("numberGrid");
  const overlayLayer = queryRole("overlayLayer");
  const wheel = queryRole("wheel");
  const ballOrbit = queryRole("ballOrbit");
  const chipsRow = queryRole("chipsRow");
  const chipStripArrows = Array.from(rootEl.querySelectorAll(".strip-arrow"));
  const leftChipArrow = chipStripArrows[0] || null;
  const rightChipArrow = chipStripArrows[1] || null;
  const chipValueLabel = queryRole("chipValueLabel");
  const resultPill = queryRole("resultPill");
  const lastResult = queryRole("lastResult");
  const historyList = queryRole("historyList");
  const profitPopup = queryRole("profitPopup");
  const profitMultiplier = queryRole("profitMultiplier");
  const profitAmount = queryRole("profitAmount");
  const statusLine = queryRole("statusLine");
  const balanceValue = queryRole("balanceValue");
  const totalBetValue = queryRole("totalBetValue");
  const potentialValue = queryRole("potentialValue");
  const spinBtn = queryRole("spinBtn");
  const rebetTopBtn = queryRole("rebetTopBtn");
  const manualModeBtn = queryRole("manualModeBtn");
  const autoModeBtn = queryRole("autoModeBtn");
  const clearBtn = queryRole("clearBtn");
  const rebetBtn = queryRole("rebetBtn");

  const betElementById = new Map();
  const numberCellByNumber = new Map();
  const timerIds = new Set();
  const controller = new AbortController();
  const { signal } = controller;
  let profitPopupTimer = null;

  const setTimer = (fn, ms) => {
    const id = window.setTimeout(() => {
      timerIds.delete(id);
      if (!state.destroyed) fn();
    }, ms);
    timerIds.add(id);
    return id;
  };

  const clearTimers = () => {
    timerIds.forEach((id) => clearTimeout(id));
    timerIds.clear();
  };

  const wait = (ms) => new Promise((resolve) => {
    setTimer(resolve, ms);
  });

  const syncBalance = () => {
    options.setBalance?.(state.balance);
  };

  const range = (start, end, step = 1) => {
    const values = [];
    for (let value = start; value <= end; value += step) values.push(value);
    return values;
  };

  const sumMapValues = (map) => {
    let total = 0;
    for (const value of map.values()) total += value;
    return total;
  };

  const formatMoney = (value) =>
    `$${Number(value).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;

  const normalizeAngle = (value) => ((value % 360) + 360) % 360;
  const capitalize = (text) => text.charAt(0).toUpperCase() + text.slice(1);
  const getTotalBet = () => sumMapValues(state.placedBets);
  const getNumberColor = (number) => (number === 0 ? "green" : RED_NUMBERS.has(number) ? "red" : "black");
  const getWheelSliceColor = (number) => (number === 0 ? "#3f9f42" : RED_NUMBERS.has(number) ? "#ff0f44" : "#34495a");
  const getNumberAt = (col, row) => col * 3 + (3 - row);
  const getRectForNumber = (number) => numberCellByNumber.get(number)?.getBoundingClientRect() || null;

  const setStatus = (message) => {
    if (statusLine) statusLine.textContent = message;
  };

  const clearResultVisuals = () => {
    rootEl.querySelectorAll(".result-win").forEach((node) => node.classList.remove("result-win"));
    rootEl.querySelectorAll(".number-hit").forEach((node) => node.classList.remove("number-hit"));
    rootEl.querySelectorAll(".chip-win").forEach((node) => node.classList.remove("chip-win"));
  };

  const hideProfitPopup = (immediate = false) => {
    if (!profitPopup) return;
    if (profitPopupTimer) {
      clearTimeout(profitPopupTimer);
      timerIds.delete(profitPopupTimer);
      profitPopupTimer = null;
    }
    if (immediate) {
      profitPopup.classList.remove("show");
      return;
    }
    profitPopup.classList.remove("show");
  };

  const showProfitPopup = (netProfit, multiplier) => {
    if (!profitPopup || !profitMultiplier || !profitAmount) return;
    hideProfitPopup(true);
    profitMultiplier.textContent = `${Math.max(0, multiplier).toFixed(2)}x`;
    profitAmount.textContent = formatMoney(Math.max(0, netProfit));
    profitPopup.classList.add("show");
    profitPopupTimer = setTimer(() => {
      profitPopup?.classList.remove("show");
      profitPopupTimer = null;
    }, PROFIT_POPUP_MS);
  };

  const addBetDefinition = ({ id, type, numbers, payout, zone }) => {
    state.betDefinitions.set(id, {
      id,
      type,
      numbers: Object.freeze([...numbers]),
      numberSet: new Set(numbers),
      payout,
      zone
    });
  };

  const registerStaticBetDefinitions = () => {
    addBetDefinition({ id: "straight-0", type: "straight", numbers: [0], payout: 35, zone: "inside" });
    addBetDefinition({ id: "outside-low", type: "outside", numbers: range(1, 18), payout: 1, zone: "outside" });
    addBetDefinition({ id: "outside-even", type: "outside", numbers: range(2, 36, 2), payout: 1, zone: "outside" });
    addBetDefinition({ id: "outside-red", type: "outside", numbers: [...RED_NUMBERS], payout: 1, zone: "outside" });
    addBetDefinition({
      id: "outside-black",
      type: "outside",
      numbers: range(1, 36).filter((value) => !RED_NUMBERS.has(value)),
      payout: 1,
      zone: "outside"
    });
    addBetDefinition({ id: "outside-odd", type: "outside", numbers: range(1, 35, 2), payout: 1, zone: "outside" });
    addBetDefinition({ id: "outside-high", type: "outside", numbers: range(19, 36), payout: 1, zone: "outside" });
    addBetDefinition({ id: "dozen-1", type: "dozen", numbers: range(1, 12), payout: 2, zone: "outside" });
    addBetDefinition({ id: "dozen-2", type: "dozen", numbers: range(13, 24), payout: 2, zone: "outside" });
    addBetDefinition({ id: "dozen-3", type: "dozen", numbers: range(25, 36), payout: 2, zone: "outside" });
    addBetDefinition({ id: "column-1", type: "column", numbers: range(1, 34, 3), payout: 2, zone: "outside" });
    addBetDefinition({ id: "column-2", type: "column", numbers: range(2, 35, 3), payout: 2, zone: "outside" });
    addBetDefinition({ id: "column-3", type: "column", numbers: range(3, 36, 3), payout: 2, zone: "outside" });
  };

  const buildNumberGrid = () => {
    if (!numberGrid) return;
    const fragment = document.createDocumentFragment();
    for (let row = 0; row < 3; row += 1) {
      for (let col = 0; col < 12; col += 1) {
        const number = getNumberAt(col, row);
        const cell = document.createElement("button");
        cell.type = "button";
        cell.className = `bet-area number-cell ${getNumberColor(number)}`;
        cell.textContent = String(number);
        cell.dataset.betId = `straight-${number}`;
        fragment.appendChild(cell);
        numberCellByNumber.set(number, cell);
        addBetDefinition({
          id: `straight-${number}`,
          type: "straight",
          numbers: [number],
          payout: 35,
          zone: "inside"
        });
      }
    }
    numberGrid.appendChild(fragment);
  };

  const mapBetElements = () => {
    rootEl.querySelectorAll(".bet-area[data-bet-id]").forEach((element) => {
      betElementById.set(element.dataset.betId, element);
    });
  };

  const createComboSpot = ({ id, type, numbers, payout, left, top, width, height }) => {
    if (!overlayLayer) return;
    const spot = document.createElement("button");
    spot.type = "button";
    spot.className = "bet-area combo-spot";
    spot.dataset.betId = id;
    spot.style.left = `${left}px`;
    spot.style.top = `${top}px`;
    spot.style.width = `${Math.max(8, width)}px`;
    spot.style.height = `${Math.max(8, height)}px`;
    overlayLayer.appendChild(spot);
    addBetDefinition({ id, type, numbers, payout, zone: "inside-combo" });
    betElementById.set(id, spot);
    state.comboBetIds.add(id);
  };

  const createSplitSpot = (n1, n2, overlayRect) => {
    const rect1 = getRectForNumber(n1);
    const rect2 = getRectForNumber(n2);
    if (!rect1 || !rect2) return;
    const id = `split-${Math.min(n1, n2)}-${Math.max(n1, n2)}`;
    const horizontal = Math.abs(rect1.top - rect2.top) < 3;

    if (horizontal) {
      createComboSpot({
        id,
        type: "split",
        numbers: [n1, n2],
        payout: 17,
        left: rect1.right - overlayRect.left - 5,
        top: rect1.top - overlayRect.top + 4,
        width: rect2.left - rect1.right + 10,
        height: rect1.height - 8
      });
      return;
    }

    createComboSpot({
      id,
      type: "split",
      numbers: [n1, n2],
      payout: 17,
      left: rect1.left - overlayRect.left + 4,
      top: rect2.top - overlayRect.top - 5,
      width: rect1.width - 8,
      height: rect1.bottom - rect2.top + 10
    });
  };

  const createCornerSpot = (a, b, c, d, overlayRect) => {
    const ra = getRectForNumber(a);
    const rb = getRectForNumber(b);
    const rc = getRectForNumber(c);
    if (!ra || !rb || !rc) return;
    createComboSpot({
      id: `corner-${[a, b, c, d].sort((x, y) => x - y).join("-")}`,
      type: "corner",
      numbers: [a, b, c, d],
      payout: 8,
      left: rb.left - overlayRect.left - 5,
      top: rc.top - overlayRect.top - 5,
      width: 10,
      height: 10
    });
  };

  const buildInsideCombinationSpots = () => {
    if (!overlayLayer || !numbersZone) return;
    for (const comboId of state.comboBetIds) {
      state.betDefinitions.delete(comboId);
      betElementById.delete(comboId);
    }
    state.comboBetIds.clear();
    overlayLayer.innerHTML = "";

    const overlayRect = overlayLayer.getBoundingClientRect();
    if (overlayRect.width === 0 || overlayRect.height === 0) return;

    for (let row = 0; row < 3; row += 1) {
      for (let col = 0; col < 11; col += 1) {
        createSplitSpot(getNumberAt(col, row), getNumberAt(col + 1, row), overlayRect);
      }
    }

    for (let col = 0; col < 12; col += 1) {
      const top = getNumberAt(col, 0);
      const mid = getNumberAt(col, 1);
      const bottom = getNumberAt(col, 2);
      createSplitSpot(top, mid, overlayRect);
      createSplitSpot(mid, bottom, overlayRect);

      const topRect = getRectForNumber(top);
      if (topRect) {
        createComboSpot({
          id: `street-${bottom}`,
          type: "street",
          numbers: [bottom, mid, top],
          payout: 11,
          left: topRect.left - overlayRect.left + 4,
          top: topRect.top - overlayRect.top - 12,
          width: topRect.width - 8,
          height: 10
        });
      }
    }

    for (let col = 0; col < 11; col += 1) {
      const topLeft = getNumberAt(col, 0);
      const topRight = getNumberAt(col + 1, 0);
      const midLeft = getNumberAt(col, 1);
      const midRight = getNumberAt(col + 1, 1);
      const bottomLeft = getNumberAt(col, 2);
      const bottomRight = getNumberAt(col + 1, 2);

      createCornerSpot(topLeft, topRight, midLeft, midRight, overlayRect);
      createCornerSpot(midLeft, midRight, bottomLeft, bottomRight, overlayRect);

      const topRect = getRectForNumber(topLeft);
      const rightRect = getRectForNumber(topRight);
      const bottomRect = getRectForNumber(bottomLeft);
      if (topRect && rightRect && bottomRect) {
        createComboSpot({
          id: `sixline-${bottomLeft}-${bottomRight}`,
          type: "sixline",
          numbers: [bottomLeft, getNumberAt(col, 1), topLeft, bottomRight, getNumberAt(col + 1, 1), topRight],
          payout: 5,
          left: topRect.right - overlayRect.left - 4,
          top: topRect.top - overlayRect.top + 4,
          width: rightRect.left - topRect.right + 8,
          height: bottomRect.bottom - topRect.top - 8
        });
      }
    }

    for (const [betId] of state.placedBets) {
      if (state.comboBetIds.has(betId)) renderBetChip(betId);
    }
  };

  const renderBetChip = (betId) => {
    const area = betElementById.get(betId);
    if (!area) return;
    const amount = state.placedBets.get(betId);
    if (!amount) return;
    let chip = area.querySelector(".placed-chip");
    if (!chip) {
      chip = document.createElement("div");
      chip.className = "placed-chip";
      area.appendChild(chip);
    }
    chip.textContent = formatMoney(amount);
  };

  const clearCurrentBets = () => {
    for (const [betId] of state.placedBets) {
      const node = betElementById.get(betId);
      if (!node) continue;
      node.querySelectorAll(".placed-chip").forEach((chip) => chip.remove());
    }
    state.placedBets.clear();
    updateHud();
  };

  const computePotentialMaxReturn = () => {
    if (state.placedBets.size === 0) return 0;
    let best = 0;
    for (let outcome = 0; outcome <= 36; outcome += 1) {
      let total = 0;
      for (const [betId, amount] of state.placedBets) {
        const bet = state.betDefinitions.get(betId);
        if (bet && bet.numberSet.has(outcome)) total += amount * (bet.payout + 1);
      }
      if (total > best) best = total;
    }
    return best;
  };

  const updateChipSelection = () => {
    if (!chipsRow) return;
    chipsRow.querySelectorAll(".chip-option").forEach((button) => {
      button.classList.toggle("active", Number(button.dataset.chip) === state.selectedChip);
    });
  };

  const updateChipScrollArrows = () => {
    if (!chipsRow || !leftChipArrow || !rightChipArrow) return;
    const maxScrollLeft = Math.max(0, chipsRow.scrollWidth - chipsRow.clientWidth);
    const atStart = chipsRow.scrollLeft <= 1;
    const atEnd = chipsRow.scrollLeft >= maxScrollLeft - 1;
    leftChipArrow.disabled = state.isSpinning || state.autoRunning || atStart;
    rightChipArrow.disabled = state.isSpinning || state.autoRunning || atEnd;
  };

  const scrollChipStrip = (direction) => {
    if (!chipsRow || state.isSpinning || state.autoRunning) return;
    const step = Math.max(80, Math.floor(chipsRow.clientWidth * 0.7));
    chipsRow.scrollBy({ left: direction * step, behavior: "smooth" });
  };

  const updateModeUI = () => {
    manualModeBtn?.classList.toggle("active", state.mode === "manual");
    autoModeBtn?.classList.toggle("active", state.mode === "auto");
    if (!spinBtn) return;
    if (state.mode === "auto") {
      spinBtn.textContent = state.autoRunning ? "Stop Auto" : "Start Auto";
      spinBtn.classList.toggle("auto-running", state.autoRunning);
    } else {
      spinBtn.textContent = "Play";
      spinBtn.classList.remove("auto-running");
    }
  };

  const getAutoStartTemplate = () => {
    if (state.placedBets.size > 0) return new Map(state.placedBets);
    if (state.lastBets.size > 0) return new Map(state.lastBets);
    return null;
  };

  const updateHud = () => {
    const totalBet = getTotalBet();
    const potentialMax = computePotentialMaxReturn();
    const autoStartTemplate = getAutoStartTemplate();
    const autoStartTotal = autoStartTemplate ? sumMapValues(autoStartTemplate) : 0;

    if (chipValueLabel) chipValueLabel.textContent = formatMoney(state.selectedChip);
    if (balanceValue) balanceValue.textContent = formatMoney(state.balance);
    if (totalBetValue) totalBetValue.textContent = formatMoney(totalBet);
    if (potentialValue) potentialValue.textContent = formatMoney(potentialMax);
    updateModeUI();

    if (spinBtn) {
      if (state.mode === "manual") {
        spinBtn.disabled = state.isSpinning || totalBet <= 0 || totalBet > state.balance;
      } else {
        spinBtn.disabled = state.autoRunning
          ? false
          : state.isSpinning || !autoStartTemplate || autoStartTotal <= 0 || autoStartTotal > state.balance;
      }
    }

    if (clearBtn) clearBtn.disabled = state.isSpinning || state.autoRunning || totalBet <= 0;
    const rebetDisabled = state.isSpinning || state.autoRunning || state.lastBets.size === 0;
    if (rebetBtn) rebetBtn.disabled = rebetDisabled;
    if (rebetTopBtn) rebetTopBtn.disabled = rebetDisabled;
    chipsRow?.querySelectorAll(".chip-option").forEach((chipButton) => {
      chipButton.disabled = state.isSpinning || state.autoRunning;
    });
    updateChipScrollArrows();
  };

  const setMode = (mode) => {
    if (mode === state.mode) return;
    if (mode === "manual" && state.autoRunning) stopAuto("Auto stopped.");
    state.mode = mode;
    if (mode === "auto") setStatus("Auto mode enabled. Press Start Auto.");
    else if (!state.isSpinning) setStatus("Manual mode enabled.");
    updateHud();
  };

  const placeBet = (betId) => {
    if (state.isSpinning || state.autoRunning) {
      if (state.autoRunning) setStatus("Stop Auto to edit bets.");
      return;
    }
    if (!state.betDefinitions.has(betId)) return;
    clearResultVisuals();
    if (getTotalBet() + state.selectedChip > state.balance) {
      setStatus("Insufficient balance for that chip.");
      return;
    }
    state.placedBets.set(betId, (state.placedBets.get(betId) || 0) + state.selectedChip);
    renderBetChip(betId);
    setStatus(`Bet placed: ${formatMoney(state.selectedChip)}.`);
    updateHud();
  };

  const applyBetsFromMap = (bets) => {
    clearCurrentBets();
    for (const [betId, amount] of bets) {
      if (!state.betDefinitions.has(betId) || amount <= 0) continue;
      state.placedBets.set(betId, amount);
      renderBetChip(betId);
    }
    updateHud();
  };

  const rebetLastRound = () => {
    const needed = sumMapValues(state.lastBets);
    if (needed === 0) return;
    if (needed > state.balance) {
      setStatus("Not enough balance to rebet the previous round.");
      return;
    }
    clearCurrentBets();
    clearResultVisuals();
    for (const [betId, amount] of state.lastBets) {
      if (!state.betDefinitions.has(betId)) continue;
      state.placedBets.set(betId, amount);
      renderBetChip(betId);
    }
    setStatus("Previous bets restored.");
    updateHud();
  };

  const resolveBets = (winningNumber) => {
    let totalReturn = 0;
    const winningBetIds = [];
    for (const [betId, amount] of state.placedBets) {
      const bet = state.betDefinitions.get(betId);
      if (!bet || !bet.numberSet.has(winningNumber)) continue;
      totalReturn += amount * (bet.payout + 1);
      winningBetIds.push(betId);
    }
    return { totalReturn, winningBetIds };
  };

  const renderHistory = () => {
    if (!historyList) return;
    historyList.innerHTML = "";
    if (state.resultHistory.length === 0) {
      const empty = document.createElement("div");
      empty.className = "history-empty";
      empty.textContent = "No spins yet";
      historyList.appendChild(empty);
      return;
    }
    const fragment = document.createDocumentFragment();
    for (const item of state.resultHistory) {
      const chip = document.createElement("div");
      chip.className = `history-chip ${item.color}`;
      chip.textContent = String(item.number);
      fragment.appendChild(chip);
    }
    historyList.appendChild(fragment);
  };

  const updateResultDisplay = (number, color) => {
    const colorName = capitalize(color);
    if (resultPill) {
      resultPill.textContent = `Result: ${number} (${colorName})`;
      resultPill.classList.remove("red", "black", "green");
      resultPill.classList.add(color);
    }
    if (lastResult) lastResult.textContent = `${number} ${colorName}`;
  };

  const addResultToHistory = (number, color) => {
    state.resultHistory.unshift({ number, color });
    if (state.resultHistory.length > RESULT_HISTORY_LIMIT) state.resultHistory.length = RESULT_HISTORY_LIMIT;
    renderHistory();
  };

  const highlightWinners = (winningNumber, winningBetIds) => {
    const straight = betElementById.get(`straight-${winningNumber}`);
    if (straight) straight.classList.add("result-win", "number-hit");
    for (const bet of state.betDefinitions.values()) {
      if (bet.zone !== "outside" || !bet.numberSet.has(winningNumber)) continue;
      const node = betElementById.get(bet.id);
      if (node) node.classList.add("result-win");
    }
    for (const betId of winningBetIds) {
      const node = betElementById.get(betId);
      if (!node) continue;
      node.classList.add("result-win");
      const chip = node.querySelector(".placed-chip");
      if (chip) chip.classList.add("chip-win");
    }
  };

  const animateWheelToResult = async (winningNumber) => {
    if (!wheel || !ballOrbit) return;
    const index = WHEEL_ORDER.indexOf(winningNumber);
    const slice = 360 / WHEEL_ORDER.length;
    const winningPocketCenter = WHEEL_GRADIENT_START_ANGLE + (index + 0.5) * slice;
    const spinTravel = -(2520 + Math.floor(Math.random() * 721));
    const spinEndAngle = state.ballRotation + spinTravel;
    const landingAngle = normalizeAngle(spinEndAngle);
    const targetMod = normalizeAngle(landingAngle - winningPocketCenter);
    const currentMod = normalizeAngle(state.wheelRotation);
    const delta = normalizeAngle(targetMod - currentMod);
    state.wheelRotation += 1800 + delta;

    wheel.style.transitionDuration = `${SPIN_DURATION_MS}ms`;
    ballOrbit.classList.add("is-spinning");
    ballOrbit.style.transition = "none";
    ballOrbit.style.transform = `rotate(${state.ballRotation}deg)`;

    requestAnimationFrame(() => {
      wheel.style.transform = `rotate(${state.wheelRotation}deg)`;
      ballOrbit.style.transition = `transform ${SPIN_DURATION_MS}ms cubic-bezier(0.08, 0.78, 0.2, 1)`;
      ballOrbit.style.transform = `rotate(${spinEndAngle}deg)`;
    });

    await wait(SPIN_DURATION_MS);
    ballOrbit.classList.remove("is-spinning");
    state.ballRotation = spinEndAngle;
  };

  const buildWheel = () => {
    if (!wheel) return;
    const slice = 360 / WHEEL_ORDER.length;
    const segments = WHEEL_ORDER.map((num, index) => {
      const start = (index * slice).toFixed(4);
      const end = ((index + 1) * slice).toFixed(4);
      return `${getWheelSliceColor(num)} ${start}deg ${end}deg`;
    }).join(", ");

    wheel.style.background = `conic-gradient(from ${WHEEL_GRADIENT_START_ANGLE}deg, ${segments})`;
    wheel.querySelectorAll(".pocket-label").forEach((label) => label.remove());
    WHEEL_ORDER.forEach((num, index) => {
      const label = document.createElement("span");
      label.className = `pocket-label ${getNumberColor(num)}`;
      label.dataset.index = String(index);
      label.textContent = String(num);
      wheel.appendChild(label);
    });
    positionWheelLabels();
  };

  const positionWheelLabels = () => {
    if (!wheel) return;
    const size = wheel.clientWidth;
    if (size <= 0) return;
    const center = size / 2;
    const radius = size / 2 - 16;
    const slice = 360 / WHEEL_ORDER.length;
    wheel.querySelectorAll(".pocket-label").forEach((label) => {
      const index = Number(label.dataset.index);
      const cssAngle = WHEEL_GRADIENT_START_ANGLE + (index + 0.5) * slice;
      const rad = (cssAngle * Math.PI) / 180;
      label.style.left = `${center + radius * Math.sin(rad)}px`;
      label.style.top = `${center - radius * Math.cos(rad)}px`;
    });
  };

  const spinRound = async () => {
    if (state.isSpinning) return;
    if (!ensureCasinoBettingAllowedNow()) return;
    const totalBet = getTotalBet();
    if (totalBet <= 0 || totalBet > state.balance) return;
    playGameSound("roulette_spin", { restart: true, allowOverlap: false });
    state.isSpinning = true;
    state.lastBets = new Map(state.placedBets);
    clearResultVisuals();
    hideProfitPopup(true);
    state.balance -= totalBet;
    syncBalance();
    updateHud();
    setStatus("Spinning...");

    let winningNumber = Math.floor(Math.random() * 37);
    if (typeof shouldRigHighBet === "function" && shouldRigHighBet(totalBet, 1.2)) {
      const losing = [];
      for (let candidate = 0; candidate <= 36; candidate += 1) {
        if (resolveBets(candidate).totalReturn <= 0) losing.push(candidate);
      }
      if (losing.length > 0) {
        winningNumber = losing[Math.floor(Math.random() * losing.length)];
      }
    }

    await animateWheelToResult(winningNumber);
    const winningColor = getNumberColor(winningNumber);
    const { totalReturn, winningBetIds } = resolveBets(winningNumber);
    if (totalReturn > 0) state.balance += totalReturn;
    syncBalance();

    addResultToHistory(winningNumber, winningColor);
    updateResultDisplay(winningNumber, winningColor);
    highlightWinners(winningNumber, winningBetIds);

    if (totalReturn > 0) {
      const net = totalReturn - totalBet;
      setStatus(`WIN: ${formatMoney(totalReturn)} (Net ${net >= 0 ? "+" : "-"}${formatMoney(Math.abs(net))})`);
      showProfitPopup(Math.max(0, net), totalBet > 0 ? totalReturn / totalBet : 0);
      if (net > 0 && typeof showCasinoWinPopup === "function") {
        showCasinoWinPopup({ amount: net, multiplier: totalBet > 0 ? totalReturn / totalBet : 0 });
      }
    } else {
      setStatus("LOST");
      hideProfitPopup();
      playGameSound("loss");
    }

    updateHud();
    triggerCasinoKickoutCheckAfterRound();
    await wait(1000);
    clearCurrentBets();
    state.isSpinning = false;
    updateHud();
  };

  const startAuto = () => {
    if (state.isSpinning || state.autoRunning) return;
    const template = getAutoStartTemplate();
    if (!template || template.size === 0) {
      setStatus("Place bets first or use Rebet before starting Auto.");
      updateHud();
      return;
    }
    const templateTotal = sumMapValues(template);
    if (templateTotal > state.balance) {
      setStatus("Insufficient balance to start Auto.");
      updateHud();
      return;
    }
    state.autoTemplateBets = new Map(template);
    if (state.placedBets.size === 0) applyBetsFromMap(state.autoTemplateBets);
    state.autoRunning = true;
    const runId = ++state.autoRunId;
    updateHud();
    setStatus("Auto started.");
    runAutoLoop(runId);
  };

  const stopAuto = (message = "") => {
    if (!state.autoRunning) return;
    state.autoRunning = false;
    state.autoRunId += 1;
    updateHud();
    if (message) setStatus(message);
  };

  const runAutoLoop = async (runId) => {
    while (!state.destroyed && state.autoRunning && runId === state.autoRunId) {
      const templateTotal = sumMapValues(state.autoTemplateBets);
      if (templateTotal <= 0) return stopAuto("Auto stopped: no active bet template.");
      if (state.balance < templateTotal) return stopAuto("Auto stopped: insufficient balance.");
      if (getTotalBet() === 0) applyBetsFromMap(state.autoTemplateBets);
      if (getTotalBet() === 0) return stopAuto("Auto stopped: template bets are no longer valid.");
      await spinRound();
      if (state.destroyed || !state.autoRunning || runId !== state.autoRunId) return;
      await wait(AUTO_ROUND_DELAY_MS);
    }
  };

  const handleRebetClick = () => {
    if (state.isSpinning || state.autoRunning || state.lastBets.size === 0) return;
    rebetLastRound();
  };

  const bindEvents = () => {
    manualModeBtn?.addEventListener("click", () => setMode("manual"), { signal });
    autoModeBtn?.addEventListener("click", () => setMode("auto"), { signal });

    tablePanel?.addEventListener("click", (event) => {
      const betButton = event.target.closest(".bet-area");
      if (!betButton || !tablePanel.contains(betButton)) return;
      const betId = betButton.dataset.betId;
      if (!betId) return;
      placeBet(betId);
    }, { signal });

    chipsRow?.addEventListener("click", (event) => {
      const chipButton = event.target.closest(".chip-option");
      if (!chipButton) return;
      state.selectedChip = Number(chipButton.dataset.chip);
      updateChipSelection();
      updateHud();
    }, { signal });

    chipsRow?.addEventListener("scroll", updateChipScrollArrows, { signal, passive: true });
    leftChipArrow?.addEventListener("click", () => scrollChipStrip(-1), { signal });
    rightChipArrow?.addEventListener("click", () => scrollChipStrip(1), { signal });

    clearBtn?.addEventListener("click", () => {
      if (state.isSpinning || state.autoRunning) return;
      clearCurrentBets();
      clearResultVisuals();
      setStatus("Bets cleared.");
    }, { signal });

    rebetBtn?.addEventListener("click", handleRebetClick, { signal });
    rebetTopBtn?.addEventListener("click", handleRebetClick, { signal });

    spinBtn?.addEventListener("click", async () => {
      if (state.mode === "auto") {
        if (state.autoRunning) stopAuto("Auto stopped.");
        else startAuto();
        return;
      }
      await spinRound();
    }, { signal });

    spinBtn?.addEventListener("pointerdown", () => {
      if (state.isSpinning) return;
      if (state.mode === "auto" && state.autoRunning) return;
      playGameSound("roulette_spin", { restart: true, allowOverlap: false });
    }, { signal });

    let resizeTimer = null;
    window.addEventListener("resize", () => {
      if (resizeTimer) {
        clearTimeout(resizeTimer);
        timerIds.delete(resizeTimer);
      }
      resizeTimer = setTimer(() => {
        buildInsideCombinationSpots();
        positionWheelLabels();
        updateChipScrollArrows();
      }, 120);
    }, { signal });
  };

  buildNumberGrid();
  registerStaticBetDefinitions();
  mapBetElements();
  buildWheel();
  buildInsideCombinationSpots();
  bindEvents();
  renderHistory();
  updateChipSelection();
  updateHud();

  return {
    destroy() {
      state.destroyed = true;
      stopAuto();
      hideProfitPopup(true);
      clearTimers();
      controller.abort();
    }
  };
}

function loadRoulette() {
  const container = document.getElementById("casino-container");
  if (!container) return;
  container.classList.add("casino-fullbleed", "roulette-fullbleed");
  container.innerHTML = `
    <div class="roulette-root mobile-roulette">
      <main class="app" aria-label="Mobile Roulette Game">
        <section class="wheel-area" aria-label="Roulette wheel">
          <div class="wheel-pointer" aria-hidden="true"></div>
          <div class="wheel-shell">
            <div class="wheel-track" id="wheelTrack">
              <div class="wheel-labels" id="wheelLabels"></div>
              <div class="wheel-center"></div>
            </div>
          </div>
        </section>

        <section class="zero-row" id="zeroRow"></section>
        <section class="numbers-section" id="numbersSection"></section>
        <section class="outside-row" id="outsideRow"></section>
        <section class="dozens-row" id="dozensRow"></section>

        <section class="info-panel" aria-label="Round info">
          <div class="last-hit-card">
            <div class="panel-label">Last Number</div>
            <div class="last-hit-main">
              <span class="last-hit-value" id="lastNumberValue">--</span>
              <span class="last-hit-badge" id="lastNumberBadge">WAITING</span>
            </div>
          </div>
          <div class="history-card">
            <div class="panel-label">Recent</div>
            <div class="history-track" id="historyTrack">
              <span class="history-empty">No spins yet</span>
            </div>
          </div>
        </section>

        <section class="total-row">
          <div class="total-pill">Balance: <span id="balanceValue">1,000</span></div>
          <button class="rebet-btn" id="rebetBtn" type="button">REBET</button>
          <button class="spin-btn inline-spin" id="spinBtn" type="button">SPIN</button>
          <button class="clear-btn" id="clearBtn" type="button" aria-label="Clear all bets">
            <span class="clear-icon">x</span>
            <span>Clear</span>
          </button>
        </section>
      </main>
    </div>
  `;

  const root = container.querySelector(".mobile-roulette");
  if (!root) return;
  root.classList.remove("phone-game-roulette");
  addSageBrand(root, "bottom-left");

  const START_BALANCE = Number(Math.max(0, cash).toFixed(2));
  const BET_UNIT = 10;
  const redNumbers = new Set([1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36]);
  const wheelOrder = [
    "0", 28, 9, 26, 30, 11, 7, 20, 32, 17,
    5, 22, 34, 15, 3, 24, 36, 13, 1, "00",
    27, 10, 25, 29, 12, 8, 19, 31, 18, 6,
    21, 33, 16, 4, 23, 35, 14, 2
  ];

  const state = {
    balance: START_BALANCE,
    spinning: false,
    rotation: 0,
    winningValue: null,
    lastRoundBets: {},
    resultHistory: [],
    bets: {}
  };

  const ui = {
    balanceValue: root.querySelector("#balanceValue"),
    zeroRow: root.querySelector("#zeroRow"),
    numbersSection: root.querySelector("#numbersSection"),
    outsideRow: root.querySelector("#outsideRow"),
    dozensRow: root.querySelector("#dozensRow"),
    clearBtn: root.querySelector("#clearBtn"),
    rebetBtn: root.querySelector("#rebetBtn"),
    spinBtn: root.querySelector("#spinBtn"),
    lastNumberValue: root.querySelector("#lastNumberValue"),
    lastNumberBadge: root.querySelector("#lastNumberBadge"),
    historyTrack: root.querySelector("#historyTrack"),
    wheelTrack: root.querySelector("#wheelTrack"),
    wheelLabels: root.querySelector("#wheelLabels")
  };

  const outsideDefs = [
    { key: "outside:red", label: "RED", odds: "2:1", cls: "red" },
    { key: "outside:black", label: "BLACK", odds: "2:1", cls: "black" },
    { key: "outside:odd", label: "ODD", odds: "2:1", cls: "odd" },
    { key: "outside:even", label: "EVEN", odds: "2:1", cls: "even" }
  ];

  const dozenDefs = [
    { key: "dozen:1-12", label: "1-12", odds: "3:1" },
    { key: "dozen:13-24", label: "13-24", odds: "3:1" },
    { key: "dozen:25-36", label: "25-36", odds: "3:1" }
  ];

  let destroyed = false;

  function syncGlobalCash() {
    cash = Number(Math.max(0, state.balance).toFixed(2));
    updateUI();
    updateBlackjackCash();
    updateFullscreenCash();
  }

  function formatNumber(value) {
    return Number(value).toLocaleString("en-US");
  }

  function getTotalBet() {
    return Object.values(state.bets).reduce((sum, value) => sum + value, 0);
  }

  function getBetMapTotal(bets) {
    return Object.values(bets).reduce((sum, value) => sum + value, 0);
  }

  function isRed(value) {
    return redNumbers.has(value);
  }

  function getNumberClass(value) {
    if (value === 0 || value === "0" || value === "00") return "green";
    return isRed(Number(value)) ? "red" : "black";
  }

  function createBetButton({ key, label, odds, className }) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `bet-btn ${className}`;
    button.dataset.betKey = key;
    button.innerHTML = `
      <span class="bet-main">${label}</span>
      <span class="bet-odds">${odds}</span>
      <span class="chip">0</span>
    `;
    button.addEventListener("click", () => placeBet(key));
    return button;
  }

  function buildBoard() {
    ["0", "00"].forEach((value) => {
      ui.zeroRow.append(
        createBetButton({
          key: `straight:${value}`,
          label: value,
          odds: "36:1",
          className: "zero"
        })
      );
    });

    for (let value = 1; value <= 36; value += 1) {
      ui.numbersSection.append(
        createBetButton({
          key: `straight:${value}`,
          label: String(value),
          odds: "36:1",
          className: `straight ${getNumberClass(value)}`
        })
      );
    }

    outsideDefs.forEach((item) => {
      ui.outsideRow.append(
        createBetButton({
          key: item.key,
          label: item.label,
          odds: item.odds,
          className: `outside ${item.cls}`
        })
      );
    });

    dozenDefs.forEach((item) => {
      ui.dozensRow.append(
        createBetButton({
          key: item.key,
          label: item.label,
          odds: item.odds,
          className: "dozen"
        })
      );
    });
  }

  function renderWheel() {
    const segmentAngle = 360 / wheelOrder.length;
    let start = -segmentAngle / 2;
    const gradientParts = [];

    wheelOrder.forEach((value) => {
      const end = start + segmentAngle;
      const color = value === "0" || value === "00"
        ? "#2fd35b"
        : isRed(Number(value))
          ? "#ef304f"
          : "#1b1e2b";
      gradientParts.push(`${color} ${start}deg ${end}deg`);
      start = end;
    });

    ui.wheelTrack.style.background = `conic-gradient(${gradientParts.join(",")})`;
    ui.wheelLabels.innerHTML = "";

    wheelOrder.forEach((value, index) => {
      const angle = segmentAngle * index;
      const label = document.createElement("div");
      label.className = `wheel-label ${getNumberClass(value)}`;
      label.dataset.value = String(value);
      label.textContent = String(value);
      label.style.transform = `
        translate(-50%, -50%)
        rotate(${angle}deg)
        translateY(calc(var(--wheel-size) * -0.44))
        rotate(${-angle}deg)
      `;
      ui.wheelLabels.append(label);
    });
  }

  function setInteractiveDisabled(disabled) {
    root.querySelectorAll(".bet-btn, .clear-btn, .rebet-btn").forEach((button) => {
      button.disabled = disabled;
    });
  }

  function renderSpinInfo() {
    const latest = state.resultHistory[0];
    if (ui.lastNumberValue) ui.lastNumberValue.textContent = latest || "--";

    if (ui.lastNumberBadge) {
      const statusClass = latest ? getNumberClass(latest) : "";
      ui.lastNumberBadge.className = `last-hit-badge ${statusClass}`.trim();
      ui.lastNumberBadge.textContent = latest ? statusClass.toUpperCase() : "WAITING";
    }

    if (!ui.historyTrack) return;
    ui.historyTrack.innerHTML = "";

    if (state.resultHistory.length === 0) {
      const empty = document.createElement("span");
      empty.className = "history-empty";
      empty.textContent = "No spins yet";
      ui.historyTrack.append(empty);
      return;
    }

    state.resultHistory.slice(0, 10).forEach((value) => {
      const pill = document.createElement("span");
      pill.className = `history-pill ${getNumberClass(value)}`;
      pill.textContent = value;
      ui.historyTrack.append(pill);
    });
  }

  function updateBoardUI() {
    if (ui.balanceValue) {
      ui.balanceValue.textContent = formatNumber(state.balance);
    }
    if (ui.rebetBtn) {
      const lastRoundTotal = getBetMapTotal(state.lastRoundBets);
      ui.rebetBtn.disabled = state.spinning || lastRoundTotal === 0 || state.balance < lastRoundTotal;
    }

    root.querySelectorAll(".bet-btn").forEach((button) => {
      const key = button.dataset.betKey;
      const amount = state.bets[key] || 0;
      button.classList.toggle("has-bet", amount > 0);
      const chip = button.querySelector(".chip");
      if (chip) chip.textContent = formatNumber(amount);
    });

    renderSpinInfo();
  }

  function placeBet(key) {
    if (destroyed || state.spinning) return;
    if (state.balance < BET_UNIT) return;

    state.balance -= BET_UNIT;
    state.bets[key] = (state.bets[key] || 0) + BET_UNIT;
    syncGlobalCash();
    updateBoardUI();
  }

  function clearWinHighlights() {
    root.querySelectorAll(".win").forEach((node) => node.classList.remove("win"));
  }

  function clearBets() {
    if (destroyed || state.spinning) return;
    const refund = getTotalBet();
    state.balance += refund;
    state.bets = {};
    clearWinHighlights();
    syncGlobalCash();
    updateBoardUI();
  }

  function rebet() {
    if (destroyed || state.spinning) return;

    const lastRoundTotal = getBetMapTotal(state.lastRoundBets);
    if (lastRoundTotal === 0) return;

    const currentTotal = getTotalBet();
    const availableAfterRefund = state.balance + currentTotal;
    if (availableAfterRefund < lastRoundTotal) return;

    state.balance = availableAfterRefund - lastRoundTotal;
    state.bets = { ...state.lastRoundBets };
    syncGlobalCash();
    updateBoardUI();
  }

  function getWinningKeys(result) {
    const winningKeys = new Set([`straight:${result}`]);
    const value = Number(result);

    if (result !== "0" && result !== "00") {
      winningKeys.add(value % 2 === 0 ? "outside:even" : "outside:odd");
      winningKeys.add(isRed(value) ? "outside:red" : "outside:black");
      if (value >= 1 && value <= 12) winningKeys.add("dozen:1-12");
      if (value >= 13 && value <= 24) winningKeys.add("dozen:13-24");
      if (value >= 25 && value <= 36) winningKeys.add("dozen:25-36");
    }

    return winningKeys;
  }

  function calculatePayout(result) {
    const winningKeys = getWinningKeys(result);
    let payout = 0;

    Object.entries(state.bets).forEach(([key, amount]) => {
      if (!winningKeys.has(key)) return;

      if (key.startsWith("straight:")) payout += amount * 36;
      else if (key.startsWith("outside:")) payout += amount * 2;
      else if (key.startsWith("dozen:")) payout += amount * 3;
    });

    return { payout, winningKeys };
  }

  function highlightWinningUI(result, winningKeys) {
    root.querySelectorAll(`[data-bet-key="straight:${result}"]`).forEach((button) => {
      button.classList.add("win");
    });

    winningKeys.forEach((key) => {
      const button = root.querySelector(`[data-bet-key="${key}"]`);
      if (button) button.classList.add("win");
    });

    const winnerLabel = root.querySelector(`.wheel-label[data-value="${result}"]`);
    if (winnerLabel) winnerLabel.classList.add("win");
  }

  function resolveSpin(result) {
    const { payout, winningKeys } = calculatePayout(result);
    state.balance += payout;
    state.resultHistory.unshift(String(result));
    if (state.resultHistory.length > 14) state.resultHistory.length = 14;
    state.bets = {};
    clearWinHighlights();
    highlightWinningUI(result, winningKeys);
    syncGlobalCash();
    updateBoardUI();

    const roundTotalBet = getBetMapTotal(state.lastRoundBets);
    const net = payout - roundTotalBet;
    if (net > 0 && typeof showCasinoWinPopup === "function") {
      const multiplier = roundTotalBet > 0 ? payout / roundTotalBet : 0;
      showCasinoWinPopup({ amount: net, multiplier });
    } else if (net < 0) {
      playGameSound("loss");
    }
    triggerCasinoKickoutCheckAfterRound();

    setTimeout(() => {
      if (!destroyed) clearWinHighlights();
    }, 1300);
  }

  function spin() {
    if (destroyed || state.spinning) return;
    if (!ensureCasinoBettingAllowedNow()) return;
    const totalBet = getTotalBet();
    if (totalBet === 0) return;
    playGameSound("roulette_spin", { restart: true, allowOverlap: false });

    state.lastRoundBets = { ...state.bets };
    state.spinning = true;
    ui.spinBtn.disabled = true;
    ui.spinBtn.textContent = "SPINNING";
    ui.spinBtn.classList.add("spinning");
    setInteractiveDisabled(true);

    let result = wheelOrder[Math.floor(Math.random() * wheelOrder.length)];
    if (typeof shouldRigHighBet === "function" && shouldRigHighBet(totalBet, 1.2)) {
      const losing = wheelOrder.filter((candidate) => calculatePayout(candidate).payout <= 0);
      if (losing.length > 0) {
        result = losing[Math.floor(Math.random() * losing.length)];
      }
    }

    state.winningValue = result;
    const segmentAngle = 360 / wheelOrder.length;
    const winnerIndex = wheelOrder.indexOf(result);
    const finalNorm = (360 - (winnerIndex * segmentAngle)) % 360;
    const currentNorm = ((state.rotation % 360) + 360) % 360;
    const delta = (finalNorm - currentNorm + 360) % 360;
    const extraTurns = 6 + Math.floor(Math.random() * 3);
    const durationMs = 3200 + Math.floor(Math.random() * 1800);
    const endRotation = state.rotation + extraTurns * 360 + delta;

    ui.wheelTrack.style.transition = `transform ${durationMs}ms cubic-bezier(0.08, 0.75, 0.16, 1)`;
    ui.wheelTrack.style.transform = `rotate(${endRotation}deg)`;

    const onDone = () => {
      if (destroyed) return;
      state.rotation = endRotation;
      resolveSpin(result);
      state.spinning = false;
      ui.spinBtn.disabled = false;
      ui.spinBtn.textContent = "SPIN";
      ui.spinBtn.classList.remove("spinning");
      setInteractiveDisabled(false);
    };

    ui.wheelTrack.addEventListener("transitionend", onDone, { once: true });
  }

  function onResize() {
    if (destroyed) return;
    renderWheel();
  }

  function init() {
    buildBoard();
    renderWheel();
    syncGlobalCash();
    updateBoardUI();
    ui.clearBtn.addEventListener("click", clearBets);
    ui.rebetBtn.addEventListener("click", rebet);
    ui.spinBtn.addEventListener("pointerdown", () => {
      if (destroyed || state.spinning) return;
      if (getTotalBet() === 0) return;
      playGameSound("roulette_spin", { restart: true, allowOverlap: false });
    });
    ui.spinBtn.addEventListener("click", spin);
    window.addEventListener("resize", onResize);
  }

  init();

  activeCasinoCleanup = () => {
    destroyed = true;
    window.removeEventListener("resize", onResize);
    container.classList.remove("casino-fullbleed", "roulette-fullbleed");
  };
}
