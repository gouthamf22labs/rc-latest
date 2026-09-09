import { WAVersion } from '@whiskeysockets/baileys';
import axios from 'axios';

const v = {
  version: [] as unknown as WAVersion,
  isLatest: false,
};

/**
 * A version is only usable if it is a 3-tuple of finite numbers. Baileys is handed
 * whatever this module holds, and `version` is always present in socketConfig — so an
 * empty or partial array does NOT fall back to Baileys' own default, it overrides it.
 * An empty array reaches WhatsApp and is refused at the auth node (405, location
 * 'cln'/'atn'), which the instance then reports as creds lost and requires a QR re-scan.
 */
const isUsable = (ver: unknown): ver is WAVersion =>
  Array.isArray(ver) &&
  ver.length === 3 &&
  ver.every((n) => typeof n === 'number' && Number.isFinite(n));

/**
 * Fallback for every moment the scrape has not produced a usable version: before the
 * first fetch resolves (instances restore immediately at boot and would otherwise race
 * it), and after a failure. WA_VERSION is the operator's pin; the literal is a last
 * resort so a missing or malformed env var still yields something connectable. Stale but
 * valid connects — empty does not.
 */
const FALLBACK_VERSION: WAVersion = (() => {
  try {
    const pinned = JSON.parse(process.env.WA_VERSION ?? '[]');
    if (isUsable(pinned)) {
      return pinned;
    }
  } catch {
    // WA_VERSION is not valid JSON — use the literal below.
  }
  return [2, 3000, 1047064765];
})();

const extract = async () => {
  const resp = await axios.get<string>('https://web.whatsapp.com/sw.js');
  if (resp) {
    const re = /JSON\.parse\(\s*(?:\/\*[^]*?\*\/\s*)?("(?:(?:\\.|[^"\\])*)")\s*\)/;
    const m = re.exec(resp.data);
    if (m) {
      const escaped = m[1];
      const jsonText = JSON.parse(escaped);
      const obj = JSON.parse(jsonText);
      const v = obj?.dynamic_data?.dynamic_modules?.SiteData?.client_revision as number;

      if (v) {
        return +v;
      }

      const resp = await axios.get<string>(
        'https://raw.githubusercontent.com/code-chat-br/whatsapp-api/main/_v/version',
      );

      return +resp.data;
    }
  }
};

const REFRESH_MS = 60 * 60 * 1000 * 27 * 3;
const RETRY_BASE_MS = 30 * 1000;
const RETRY_MAX_MS = 30 * 60 * 1000;

let failures = 0;
let timer: ReturnType<typeof setTimeout> | undefined;

const scheduleNext = (ms: number) => {
  if (timer) {
    clearTimeout(timer);
  }
  timer = setTimeout(() => void refresh(), ms);
  // Never hold the process open for a version refresh.
  timer.unref?.();
};

/**
 * Self-scheduling rather than setInterval: a failed fetch has to retry sooner than the
 * normal refresh, and the previous code had no retry at all. It also left the rejection
 * unhandled, so a single network blip at boot pinned `version` to [] for the life of the
 * process — every socket built after it was refused.
 *
 * Backoff is capped: if WhatsApp changes sw.js and the regex stops matching, this must
 * not settle into a request every 30s forever.
 */
const refresh = async () => {
  try {
    const revision = await extract();
    if (typeof revision === 'number' && Number.isFinite(revision)) {
      v.version = [2, 3000, revision];
      v.isLatest = true;
      failures = 0;
      scheduleNext(REFRESH_MS);
      return;
    }
    // Resolved without a revision (regex missed, or the payload changed shape).
    v.isLatest = false;
  } catch {
    // Network/parse failure. Keep whatever we last had; the getter falls back if empty.
    v.isLatest = false;
  }
  failures += 1;
  scheduleNext(Math.min(RETRY_BASE_MS * 2 ** (failures - 1), RETRY_MAX_MS));
};

void refresh();

export const fetchLatestBaileysVersionV2 = () =>
  isUsable(v.version) ? v : { version: FALLBACK_VERSION, isLatest: false };
