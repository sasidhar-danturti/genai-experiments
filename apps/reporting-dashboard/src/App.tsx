import { useEffect, useMemo, useState } from 'react';
import dayjs from './lib/dayjs';
import { SessionSidebar } from './components/SessionSidebar';
import { ChatPanel } from './components/ChatPanel';
import { ReportWorkspace } from './components/ReportWorkspace';
import { useSessions } from './hooks/useSessions';
import { Message, InsightReport, SessionSummary } from './types';
import { invokeAgent } from './api/agent';
import { getSessionReport, saveSessionReport } from './api/sessions';
import { nanoid } from 'nanoid';

const createEmptyReport = (session: SessionSummary): InsightReport => ({
  id: nanoid(),
  sessionId: session.id,
  title: `${session.name} | Insight Report`,
  executiveSummary: 'The executive summary will be generated once the agent is prompted.',
  sections: [],
  recommendations: [],
  conversation: [],
  lastUpdated: new Date().toISOString()
});

function App() {
  const { sessions, isLoading: isSessionsLoading, refresh, create } = useSessions();
  const [selectedSession, setSelectedSession] = useState<SessionSummary | null>(null);
  const [messages, setMessages] = useState<Message[]>([]);
  const [report, setReport] = useState<InsightReport | null>(null);
  const [isInvoking, setIsInvoking] = useState(false);
  const [statusMessage, setStatusMessage] = useState('Select or create a session to begin.');

  useEffect(() => {
    if (sessions.length > 0 && !selectedSession) {
      setSelectedSession(sessions[0]);
    }
  }, [sessions, selectedSession]);

  useEffect(() => {
    const loadReport = async () => {
      if (!selectedSession) return;
      try {
        const data = await getSessionReport(selectedSession.id);
        if (data) {
          setReport(data);
          setMessages(data.conversation ?? []);
        } else {
          const emptyReport = createEmptyReport(selectedSession);
          setReport(emptyReport);
          setMessages([]);
        }
      } catch (err) {
        console.error(err);
        const emptyReport = createEmptyReport(selectedSession);
        setReport(emptyReport);
        setMessages([]);
      }
    };
    void loadReport();
  }, [selectedSession]);

  const handleCreateSession = async (name: string) => {
    const session = await create(name);
    setSelectedSession(session);
    const emptyReport = createEmptyReport(session);
    setReport(emptyReport);
    setMessages([]);
  };

  const sessionName = selectedSession?.name ?? null;

  const handleSend = async (prompt: string) => {
    if (!selectedSession) return;

    const userMessage: Message = {
      id: nanoid(),
      role: 'user',
      content: prompt,
      createdAt: new Date().toISOString()
    };

    setMessages((prev) => [...prev, userMessage]);
    setIsInvoking(true);
    setStatusMessage('Generating agent response...');

    try {
      const response = await invokeAgent({
        prompt,
        context: report ? JSON.stringify(report) : '',
        sessionId: selectedSession.id,
        reportId: report?.id ?? null
      });

      const updatedMessages = [...(report?.conversation ?? []), userMessage, ...(response.messages ?? [])];
      const updatedReport: InsightReport = {
        ...(report ?? createEmptyReport(selectedSession)),
        conversation: updatedMessages,
        ...(response.reportDelta ?? {}),
        lastUpdated: new Date().toISOString()
      };

      setMessages(updatedMessages);
      setReport(updatedReport);
      await saveSessionReport(selectedSession.id, updatedReport);
      setStatusMessage('Agent response received. Continue iterating or export your report.');
    } catch (err) {
      console.error(err);
      setStatusMessage('Agent invocation failed. Retry or adjust the prompt.');
    } finally {
      setIsInvoking(false);
    }
  };

  const handleExport = () => {
    if (!report) return;
    const markdown = [
      `# ${report.title}`,
      '',
      `Last updated ${dayjs(report.lastUpdated).format('MMMM D, YYYY h:mm A')}`,
      '',
      '## Executive Summary',
      report.executiveSummary,
      '',
      ...report.sections.flatMap((section) => [`## ${section.title}`, section.body, '']),
      '## Recommendations',
      ...report.recommendations.map((item, index) => `${index + 1}. ${item}`)
    ].join('\n');

    const blob = new Blob([markdown], { type: 'text/markdown;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = `${report.title.replace(/\s+/g, '-')}.md`;
    anchor.click();
    URL.revokeObjectURL(url);
  };

  const layoutState = useMemo(
    () => ({
      hasSessions: sessions.length > 0,
      hasReport: Boolean(report)
    }),
    [sessions.length, report]
  );

  return (
    <div className="flex h-screen bg-slate-100">
      <SessionSidebar
        sessions={sessions}
        selectedId={selectedSession?.id ?? null}
        onSelect={(session) => {
          setSelectedSession(session);
          setMessages([]);
        }}
        onCreate={async (name) => {
          await handleCreateSession(name);
        }}
        onRefresh={refresh}
        isLoading={isSessionsLoading}
      />
      <main className="flex flex-1 flex-col">
        <header className="flex items-center justify-between border-b border-slate-200 bg-white/70 px-8 py-5 backdrop-blur">
          <div>
            <h1 className="text-2xl font-semibold text-slate-900">Insight Foundry</h1>
            <p className="text-sm text-slate-500">A React + Databricks agentic reporting workbench</p>
          </div>
          <div className="text-right">
            <p className="text-xs uppercase text-slate-400">Status</p>
            <p className="text-sm font-medium text-slate-600">{statusMessage}</p>
          </div>
        </header>
        <div className="grid flex-1 grid-cols-2 divide-x divide-slate-200">
          <ChatPanel messages={messages} onSend={handleSend} isLoading={isInvoking} disabled={!selectedSession} />
          <ReportWorkspace report={report} onExport={handleExport} sessionName={sessionName} />
        </div>
        {!layoutState.hasSessions ? (
          <div className="border-t border-slate-200 bg-amber-50 px-8 py-3 text-sm text-amber-700">
            Create your first session to begin collaborating with the agent.
          </div>
        ) : null}
      </main>
    </div>
  );
}

export default App;
