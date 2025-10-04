export interface Message {
  id: string;
  role: 'user' | 'assistant' | 'system';
  content: string;
  createdAt: string;
}

export interface ReportSection {
  id: string;
  title: string;
  body: string;
}

export interface InsightReport {
  id: string;
  sessionId: string;
  title: string;
  executiveSummary: string;
  sections: ReportSection[];
  recommendations: string[];
  conversation: Message[];
  lastUpdated: string;
}

export interface SessionSummary {
  id: string;
  name: string;
  createdAt: string;
  updatedAt: string;
  reportId: string | null;
}

export interface DatabaseSchema {
  sessions: SessionSummary[];
  reports: InsightReport[];
}

export interface AgentInvocationPayload {
  prompt: string;
  context: string;
  sessionId: string;
  reportId: string | null;
}
