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
  lastUpdated: string;
  conversation: Message[];
}

export interface SessionSummary {
  id: string;
  name: string;
  createdAt: string;
  updatedAt: string;
  reportId: string | null;
}

export interface AgentInvocationPayload {
  prompt: string;
  context: string;
  sessionId: string;
  reportId: string | null;
}

export interface AgentInvocationResponse {
  messages: Message[];
  reportDelta?: Partial<InsightReport>;
}
