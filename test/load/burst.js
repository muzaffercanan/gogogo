// Burst of batch creates against the Compose stack: `make load`.
//
// BATCHES virtual users each send one batch of BATCH_SIZE notifications at the same moment, spread
// evenly over the three channels with 2 in 10 high, 5 in 10 normal and 3 in 10 low priority. The
// end-of-test summary shows how long the API took to accept a batch (http_req_duration of
// "create batch"). Teardown then waits until every notification of the run has left the pipeline,
// reads them all back through the API and prints how fast each channel drained and when each priority
// was sent. Times are taken from the notifications' created_at and sent_at, both written by the
// database, so the k6 clock is not involved.
import http from "k6/http";
import { check, fail, sleep } from "k6";
import exec from "k6/execution";

const API = __ENV.API_URL || "http://localhost:8080";
const BATCHES = Number(__ENV.BATCHES || 10);
const BATCH_SIZE = Number(__ENV.BATCH_SIZE || 1000);
const DRAIN_TIMEOUT_SECONDS = Number(__ENV.DRAIN_TIMEOUT_SECONDS || 600);

const CHANNELS = ["sms", "email", "push"];
const PRIORITIES = ["high", "normal", "low"];
// Indexed by the notification's number modulo 10. With the channel taken modulo 3, every channel gets
// the same mix of priorities.
const PRIORITY_OF = ["high", "high", "normal", "normal", "normal", "normal", "normal", "low", "low", "low"];
const OPEN_STATUSES = ["pending", "queued", "sending", "retry_scheduled"];

export const options = {
  scenarios: {
    burst: { executor: "per-vu-iterations", vus: BATCHES, iterations: 1, maxDuration: "2m" },
  },
  teardownTimeout: `${DRAIN_TIMEOUT_SECONDS + 120}s`,
  summaryTrendStats: ["min", "med", "p(95)", "max"],
  thresholds: {
    // Every batch is accepted whole. The second threshold never fails: it makes the summary show the
    // latency of batch creates apart from the requests teardown makes.
    checks: ["rate==1"],
    "http_req_duration{name:create batch}": ["max>=0"],
  },
};

export function setup() {
  const res = http.get(`${API}/health/ready`, { tags: { name: "ready" } });
  if (res.status !== 200) {
    fail(`API at ${API} is not ready (${res.status}); start the stack with make up`);
  }
  // Tells this run's notifications apart from earlier ones: idempotency keys, content and the
  // created_from filter. Notifications created before it are not this run's.
  return { run: Date.now().toString(36), since: new Date().toISOString() };
}

export default function (data) {
  const vu = exec.vu.idInTest;
  const notifications = [];
  for (let i = 0; i < BATCH_SIZE; i++) {
    const n = (vu - 1) * BATCH_SIZE + i;
    const channel = CHANNELS[n % CHANNELS.length];
    notifications.push({
      channel,
      recipient: recipient(channel, n),
      content: `Load test ${data.run}, notification ${n}`,
      priority: PRIORITY_OF[n % PRIORITY_OF.length],
    });
  }
  const res = http.post(`${API}/v1/notifications/batch`, JSON.stringify({ notifications }), {
    headers: { "Content-Type": "application/json", "Idempotency-Key": `load-${data.run}-${vu}` },
    tags: { name: "create batch" },
    timeout: "60s",
  });
  check(res, {
    "batch accepted whole": (r) => r.status === 202 && r.json("accepted") === BATCH_SIZE,
  });
}

function recipient(channel, n) {
  switch (channel) {
    case "sms":
      return `+90555${String(n).padStart(7, "0")}`;
    case "email":
      return `load-${n}@example.com`;
    default:
      return `device-token-${n}`;
  }
}

export function teardown(data) {
  const deadline = Date.now() + DRAIN_TIMEOUT_SECONDS * 1000;
  let all;
  for (;;) {
    if (!OPEN_STATUSES.some((s) => list(data, { status: s, limit: 1 }).items.length > 0)) {
      // A notification can move back to queued between two of those requests; reading them all
      // settles it.
      all = readAll(data);
      if (!all.some((n) => OPEN_STATUSES.includes(n.status))) break;
    }
    if (Date.now() > deadline) fail(`notifications still open after ${DRAIN_TIMEOUT_SECONDS}s`);
    sleep(1);
  }
  report(all, BATCHES * BATCH_SIZE);
}

// list returns one page of this run's notifications.
function list(data, params) {
  const query = Object.entries({ created_from: data.since, ...params })
    .map(([k, v]) => `${k}=${encodeURIComponent(v)}`)
    .join("&");
  const res = http.get(`${API}/v1/notifications?${query}`, { tags: { name: "list notifications" } });
  if (res.status !== 200) fail(`listing notifications: ${res.status} ${res.body}`);
  return res.json();
}

function readAll(data) {
  const all = [];
  let cursor;
  do {
    const page = list(data, cursor ? { limit: 100, cursor } : { limit: 100 });
    all.push(...page.items);
    cursor = page.next_cursor;
  } while (cursor);
  return all;
}

function report(all, expected) {
  const start = all.reduce((min, n) => Math.min(min, Date.parse(n.created_at)), Infinity);
  const counts = {};
  for (const n of all) counts[n.status] = (counts[n.status] || 0) + 1;
  const lines = [
    "",
    `notifications: ${all.length} of ${expected} expected; ${Object.entries(counts).map(([s, c]) => `${s} ${c}`).join(", ")}`,
    "seconds are counted from the first notification's created_at",
    "",
    "channel  sent  first sent  last sent  drain rate (sent per second between first and last)",
  ];
  const sent = all.filter((n) => n.status === "sent").map((n) => ({ ...n, at: (Date.parse(n.sent_at) - start) / 1000 }));
  for (const channel of CHANNELS) {
    const times = sent.filter((n) => n.channel === channel).map((n) => n.at).sort((a, b) => a - b);
    if (times.length === 0) continue;
    const first = times[0];
    const last = times[times.length - 1];
    const rate = times.length > 1 ? (times.length - 1) / (last - first) : NaN;
    lines.push(`${pad(channel, 7)}  ${pad(times.length, 4)}  ${pad(first.toFixed(2), 10)}  ${pad(last.toFixed(2), 9)}  ${rate.toFixed(1)}`);
  }
  lines.push("", "channel  priority  sent  median sent  p95 sent  last sent");
  for (const channel of CHANNELS) {
    for (const priority of PRIORITIES) {
      const times = sent.filter((n) => n.channel === channel && n.priority === priority).map((n) => n.at).sort((a, b) => a - b);
      if (times.length === 0) continue;
      lines.push(
        `${pad(channel, 7)}  ${pad(priority, 8)}  ${pad(times.length, 4)}  ${pad(quantile(times, 0.5).toFixed(2), 11)}  ${pad(quantile(times, 0.95).toFixed(2), 8)}  ${times[times.length - 1].toFixed(2)}`,
      );
    }
  }
  console.log(lines.join("\n"));
  check(all, { "every notification of the run was sent": (a) => a.length === expected && (counts.sent || 0) === expected });
}

function quantile(sorted, q) {
  return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))];
}

function pad(v, width) {
  return String(v).padStart(width);
}
