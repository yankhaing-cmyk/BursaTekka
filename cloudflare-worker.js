/**
 * Cloudflare Worker — Bursa Screener Trigger
 * ==========================================
 * GET/POST https://<your-worker>.workers.dev/run?key=<RUN_KEY>
 *   -> fires a GitHub repository_dispatch event ("run-screener"),
 *      which starts the GitHub Actions screener workflow.
 *
 * Also works as a Telegram webhook: point your bot's webhook here and
 * send "/run" in the chat to trigger the screener.
 *
 * Secrets to set (wrangler secret put <NAME>):
 *   GH_TOKEN   - GitHub fine-grained PAT with repo "Contents: RW" +
 *                "Actions/Workflows" permission (or classic PAT w/ repo scope)
 *   GH_OWNER   - GitHub username/org, e.g. "yourname"
 *   GH_REPO    - repository name, e.g. "bursa-screener"
 *   RUN_KEY    - shared secret for the /run URL
 *   TG_TOKEN   - (optional) Telegram bot token, for webhook confirmations
 */

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // ---- Health check ---------------------------------------------------
    if (url.pathname === "/") {
      return new Response("Bursa screener trigger: use /run?key=...", { status: 200 });
    }

    // ---- Direct trigger: /run?key=SECRET ---------------------------------
    if (url.pathname === "/run") {
      const key = url.searchParams.get("key") || request.headers.get("x-run-key");
      if (!env.RUN_KEY || key !== env.RUN_KEY) {
        return new Response("Unauthorized", { status: 401 });
      }
      const result = await triggerWorkflow(env);
      return new Response(JSON.stringify(result), {
        status: result.ok ? 200 : 502,
        headers: { "content-type": "application/json" },
      });
    }

    // ---- Telegram webhook: user sends /run in the bot chat ---------------
    if (url.pathname === "/telegram" && request.method === "POST") {
      const update = await request.json().catch(() => null);
      const msg = update?.message;
      const text = (msg?.text || "").trim().toLowerCase();

      if (text === "/run" || text.startsWith("/run@")) {
        const result = await triggerWorkflow(env);
        if (env.TG_TOKEN && msg?.chat?.id) {
          await fetch(`https://api.telegram.org/bot${env.TG_TOKEN}/sendMessage`, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              chat_id: msg.chat.id,
              text: result.ok
                ? "🚀 Screener triggered — results will arrive when the scan completes (can take a while for ~1000 stocks)."
                : `⚠️ Trigger failed: ${result.error}`,
            }),
          });
        }
      }
      // Always 200 so Telegram doesn't retry
      return new Response("ok", { status: 200 });
    }

    return new Response("Not found", { status: 404 });
  },
};

async function triggerWorkflow(env) {
  try {
    const resp = await fetch(
      `https://api.github.com/repos/${env.GH_OWNER}/${env.GH_REPO}/dispatches`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${env.GH_TOKEN}`,
          Accept: "application/vnd.github+json",
          "User-Agent": "bursa-screener-worker",
          "X-GitHub-Api-Version": "2022-11-28",
        },
        body: JSON.stringify({ event_type: "run-screener" }),
      },
    );
    if (resp.status === 204) return { ok: true, message: "workflow dispatched" };
    const body = await resp.text();
    return { ok: false, error: `GitHub API ${resp.status}: ${body}` };
  } catch (e) {
    return { ok: false, error: String(e) };
  }
}
