import { Download, FileText, History, Sparkles, StickyNote, Users } from 'lucide-react';
import { InsightReport } from '../types';
import dayjs from '../lib/dayjs';

interface ReportWorkspaceProps {
  report: InsightReport | null;
  onExport: () => void;
  sessionName: string | null;
}

export const ReportWorkspace = ({ report, onExport, sessionName }: ReportWorkspaceProps) => {
  if (!report) {
    return (
      <section className="flex h-full flex-col">
        <header className="border-b border-slate-200 px-6 py-4">
          <h3 className="text-lg font-semibold text-slate-900">Insight Report Workspace</h3>
          <p className="text-sm text-slate-500">
            Once the agent drafts sections, they will appear here for iterative review.
          </p>
        </header>
        <div className="flex-1 flex flex-col items-center justify-center gap-4 px-6 text-center">
          <div className="rounded-full bg-primary/10 p-5 text-primary">
            <Sparkles className="h-8 w-8" />
          </div>
          <div className="space-y-2">
            <h4 className="text-xl font-semibold text-slate-900">No report yet</h4>
            <p className="text-sm text-slate-500">
              Start the conversation with the agent to build the executive summary, insights, and recommendations.
            </p>
          </div>
        </div>
      </section>
    );
  }

  return (
    <section className="flex h-full flex-col overflow-hidden">
      <header className="flex items-center justify-between border-b border-slate-200 px-6 py-4">
        <div>
          <div className="flex items-center gap-2 text-xs uppercase tracking-wide text-slate-400">
            <FileText className="h-4 w-4" /> Report Workspace
          </div>
          <h3 className="text-xl font-semibold text-slate-900">{report.title}</h3>
          <p className="text-sm text-slate-500">
            Linked engagement: <span className="font-medium text-slate-600">{sessionName ?? 'Unnamed session'}</span>
          </p>
        </div>
        <button
          onClick={onExport}
          className="inline-flex items-center gap-2 rounded-md border border-slate-200 bg-white px-4 py-2 text-sm font-medium text-slate-700 shadow-sm hover:bg-slate-100"
        >
          <Download className="h-4 w-4" /> Export Markdown
        </button>
      </header>
      <div className="flex-1 overflow-y-auto px-8 py-6 space-y-8">
        <section className="rounded-xl border border-slate-200 bg-white p-6 shadow-sm">
          <div className="flex items-center gap-2 text-sm font-semibold text-slate-700">
            <StickyNote className="h-5 w-5 text-accent" /> Executive Summary
          </div>
          <p className="mt-3 text-sm leading-6 text-slate-600 whitespace-pre-line">{report.executiveSummary}</p>
        </section>
        {report.sections.map((section) => (
          <section key={section.id} className="rounded-xl border border-slate-200 bg-white p-6 shadow-sm">
            <div className="flex items-center gap-2 text-sm font-semibold text-slate-700">
              <Users className="h-5 w-5 text-secondary" /> {section.title}
            </div>
            <p className="mt-3 text-sm leading-6 text-slate-600 whitespace-pre-line">{section.body}</p>
          </section>
        ))}
        <section className="rounded-xl border border-slate-200 bg-white p-6 shadow-sm">
          <div className="flex items-center gap-2 text-sm font-semibold text-slate-700">
            <History className="h-5 w-5 text-primary" /> Key Recommendations
          </div>
          <ul className="mt-3 list-disc space-y-2 pl-6 text-sm text-slate-600">
            {report.recommendations.map((recommendation, index) => (
              <li key={index}>{recommendation}</li>
            ))}
          </ul>
        </section>
        <p className="text-xs text-slate-400">Last updated {dayjs(report.lastUpdated).format('MMM D, YYYY h:mm A')}</p>
      </div>
    </section>
  );
};
