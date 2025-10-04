# Insight Foundry – React + Databricks Agentic Reporting Workbench

This project scaffolds a **React-based, agent-assisted reporting workspace** that can orchestrate Databricks agents, capture iterative user feedback, and save richly formatted engagement reports for future retrieval. It includes:

- A Vite + React frontend with Tailwind styling for managing sessions, conversing with the agent, and curating executive-ready reports.
- A lightweight Node/Express API that persists sessions and reports, exposes a proxy endpoint for Databricks agent invocations, and can be swapped with real workspace credentials when you are ready.
- File-backed storage so that reports and message history survive server restarts during prototyping.

The UI, API, and storage all run locally for free. When you are ready to integrate with a real Databricks workspace you simply provide credentials via environment variables or your preferred secret manager.

---

## Architecture Overview

```
apps/reporting-dashboard   # React single-page application (Vite + Tailwind)
services/reporting-api     # Express API with Databricks proxy & JSON storage
```

Key capabilities:

1. **Session management** – create, list, and resume engagements. Each session tracks the latest report and conversation history.
2. **Agent hand-off** – the UI calls `/api/agent/invoke`, which the API proxies to a simulated Databricks agent. Swap the proxy implementation with real `agents` API calls when credentials are available.
3. **Iterative report building** – every agent response updates an editable report (executive summary, sections, recommendations) that can be exported as Markdown or revisited later.
4. **Persistence** – the Express service writes sessions and reports to `services/reporting-api/data/store.json` so you can resume work across restarts during development.

---

## Local Development

### 1. Prerequisites

- Node.js 18+
- pnpm, npm, or yarn (examples below use `npm`)

### 2. Back-end API (Databricks proxy & persistence)

```bash
cd services/reporting-api
cp .env.example .env        # Update with Databricks workspace details when ready
npm install
npm run dev                # Starts http://localhost:4000 with hot reload
```

The proxy currently **mimics** Databricks responses so you can test the UI without workspace access. To connect to a real workspace later:

1. Generate a Databricks personal access token with access to the Agents API.
2. Update `.env` with `DATABRICKS_WORKSPACE_URL` and `DATABRICKS_TOKEN`.
3. Replace the logic in `src/databricksProxy.ts` with actual REST calls to `https://<workspace>/api/2.0/ai/agents/...`.

The API exposes the following endpoints:

| Method | Path                          | Purpose                                   |
| ------ | ----------------------------- | ----------------------------------------- |
| GET    | `/health`                     | Service heartbeat                         |
| GET    | `/sessions`                   | List sessions (most recent first)         |
| POST   | `/sessions`                   | Create a new session                      |
| GET    | `/sessions/:id/report`        | Fetch latest report for a session         |
| PUT    | `/sessions/:id/report`        | Persist report changes                    |
| POST   | `/agent/invoke`               | Proxy a prompt to the (mock) Databricks agent |

Data is stored in `services/reporting-api/data/store.json`. Delete the file to reset.

### 3. Front-end UI

```bash
cd apps/reporting-dashboard
npm install
npm run dev                # Starts Vite dev server on http://localhost:5173
```

The Vite dev server proxies `/api/*` calls to `http://localhost:4000`, so run the API first. The interface offers:

- A left-hand session sidebar to create/restore engagements.
- A conversation panel for iterative prompts.
- A structured report workspace with export to Markdown.

### 4. Coordinated Startup Script (optional)

For convenience you can run both services in parallel using two terminals or by wiring a workspace runner (e.g., `npm-run-all`) if desired.

---

## Deploying the UI for Free

Because the frontend is a static Vite build, you can deploy it to any static host:

1. **Vercel** – Connect the repo, set the root to `apps/reporting-dashboard`, and use the build command `npm install && npm run build` with output directory `dist`.
2. **Netlify** – Similar configuration: base directory `apps/reporting-dashboard`, build `npm run build`, publish directory `dist`.
3. **GitHub Pages** – Build locally (`npm run build`) and push the `/dist` folder to a `gh-pages` branch.

For production you will also need to host the API. Cost-effective options include:

- **Render.com free tier** or **Railway.app** for small Node services.
- **Azure Container Apps** or **Databricks serverless functions** if you prefer to run near your lakehouse.

Remember to configure CORS and environment variables (`DATABRICKS_WORKSPACE_URL`, `DATABRICKS_TOKEN`) on the hosting provider.

---

## Replacing the Proxy with Real Databricks Agents

`services/reporting-api/src/databricksProxy.ts` centralises all agent calls. When you are ready:

1. Import `node-fetch` or `axios` and authenticate with the Databricks workspace using the token.
2. Replace the mock response with requests to the Agents REST API (or the SQL/MLflow endpoints you need).
3. Map Databricks responses to the `AgentInvocationResponse` shape expected by the React UI.
4. Optionally stream tokens back to the UI for real-time updates (Server Sent Events/WebSockets).

This isolation keeps the React app unchanged while you harden the backend.

---

## Persistence & Session Management Notes

- Reports include conversation history so users can reopen a session and continue where they left off.
- Each invocation updates both the report body and the saved conversation on the server.
- Reports can be exported as Markdown from the UI for easy sharing.

To store reports elsewhere (e.g., Databricks Unity Catalog, Azure Blob Storage) replace the simple JSON persistence in `storage.ts` with calls to your preferred datastore.

---

## Testing & Linting

No automated tests are configured yet. You can integrate Playwright, Vitest, or supertest depending on your needs.

---

## Next Steps

- Add authentication (e.g., Auth0, Azure AD) if you need multi-user access.
- Enhance the Databricks proxy to call real agents, Delta tables, or dashboards.
- Automate deployments using GitHub Actions.

Happy building! 🚀
