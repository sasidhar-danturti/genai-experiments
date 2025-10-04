import { readFileSync, writeFileSync, existsSync, mkdirSync } from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { DatabaseSchema, InsightReport, SessionSummary } from './types.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const DATA_DIR = path.resolve(__dirname, '../data');
const DATA_FILE = path.join(DATA_DIR, 'store.json');

const defaultData: DatabaseSchema = {
  sessions: [],
  reports: []
};

const load = (): DatabaseSchema => {
  if (!existsSync(DATA_DIR)) {
    mkdirSync(DATA_DIR, { recursive: true });
  }

  if (!existsSync(DATA_FILE)) {
    writeFileSync(DATA_FILE, JSON.stringify(defaultData, null, 2), 'utf-8');
    return { ...defaultData };
  }

  const raw = readFileSync(DATA_FILE, 'utf-8');
  try {
    const parsed = JSON.parse(raw) as DatabaseSchema;
    return {
      sessions: parsed.sessions ?? [],
      reports: parsed.reports ?? []
    };
  } catch (err) {
    console.error('Failed to parse store, resetting to defaults', err);
    writeFileSync(DATA_FILE, JSON.stringify(defaultData, null, 2), 'utf-8');
    return { ...defaultData };
  }
};

let database = load();

const persist = () => {
  writeFileSync(DATA_FILE, JSON.stringify(database, null, 2), 'utf-8');
};

export const listSessions = () => database.sessions;

export const getSession = (id: string) => database.sessions.find((session) => session.id === id) ?? null;

export const saveSession = (session: SessionSummary) => {
  const index = database.sessions.findIndex((item) => item.id === session.id);
  if (index >= 0) {
    database.sessions[index] = session;
  } else {
    database.sessions.push(session);
  }
  persist();
};

export const saveReport = (report: InsightReport) => {
  const index = database.reports.findIndex((item) => item.id === report.id);
  if (index >= 0) {
    database.reports[index] = report;
  } else {
    database.reports.push(report);
  }
  persist();
};

export const getReportBySession = (sessionId: string) =>
  database.reports.find((report) => report.sessionId === sessionId) ?? null;

export const getReport = (reportId: string) => database.reports.find((report) => report.id === reportId) ?? null;

export const resetStore = () => {
  database = { ...defaultData };
  persist();
};
