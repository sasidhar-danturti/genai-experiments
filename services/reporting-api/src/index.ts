import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { nanoid } from 'nanoid';
import dayjs from 'dayjs';
import {
  getReport,
  getReportBySession,
  getSession,
  listSessions,
  saveReport,
  saveSession
} from './storage.js';
import { AgentInvocationPayload, InsightReport, SessionSummary } from './types.js';
import { invokeDatabricksAgent } from './databricksProxy.js';

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

const port = process.env.PORT ?? 4000;

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

app.get('/sessions', (_req, res) => {
  const sessions = listSessions().sort((a, b) => dayjs(b.updatedAt).valueOf() - dayjs(a.updatedAt).valueOf());
  res.json(sessions);
});

app.post('/sessions', (req, res) => {
  const { name } = req.body as { name?: string };
  if (!name) {
    res.status(400).json({ message: 'Session name is required' });
    return;
  }

  const timestamp = new Date().toISOString();
  const session: SessionSummary = {
    id: nanoid(),
    name,
    createdAt: timestamp,
    updatedAt: timestamp,
    reportId: null
  };
  saveSession(session);
  res.status(201).json(session);
});

app.get('/sessions/:sessionId/report', (req, res) => {
  const { sessionId } = req.params;
  const report = getReportBySession(sessionId);
  if (!report) {
    res.status(204).send();
    return;
  }
  res.json(report);
});

app.put('/sessions/:sessionId/report', (req, res) => {
  const { sessionId } = req.params;
  const existingSession = getSession(sessionId);
  if (!existingSession) {
    res.status(404).json({ message: 'Session not found' });
    return;
  }

  const payload = req.body as InsightReport;
  const timestamp = new Date().toISOString();
  const report: InsightReport = {
    ...payload,
    sessionId,
    lastUpdated: timestamp
  };
  saveReport(report);

  existingSession.reportId = report.id;
  existingSession.updatedAt = timestamp;
  saveSession(existingSession);

  res.json(report);
});

app.post('/agent/invoke', async (req, res) => {
  const payload = req.body as AgentInvocationPayload;
  if (!payload.prompt) {
    res.status(400).json({ message: 'Prompt is required' });
    return;
  }

  const session = getSession(payload.sessionId);
  if (!session) {
    res.status(404).json({ message: 'Session not found' });
    return;
  }

  const report = payload.reportId ? getReport(payload.reportId) : getReportBySession(payload.sessionId);

  const response = await invokeDatabricksAgent(payload, report ?? null, {
    workspaceUrl: process.env.DATABRICKS_WORKSPACE_URL,
    token: process.env.DATABRICKS_TOKEN
  });

  session.updatedAt = new Date().toISOString();
  saveSession(session);

  res.json(response);
});

app.listen(port, () => {
  console.log(`Reporting API listening on port ${port}`);
});
