import { PlusCircle, RefreshCcw } from 'lucide-react';
import clsx from 'clsx';
import { SessionSummary } from '../types';
import dayjs from '../lib/dayjs';
import { useState } from 'react';

interface SessionSidebarProps {
  sessions: SessionSummary[];
  selectedId: string | null;
  onSelect: (session: SessionSummary) => void;
  onCreate: (name: string) => Promise<void>;
  onRefresh: () => void;
  isLoading: boolean;
}

export const SessionSidebar = ({
  sessions,
  selectedId,
  onSelect,
  onCreate,
  onRefresh,
  isLoading
}: SessionSidebarProps) => {
  const [isCreating, setIsCreating] = useState(false);
  const [sessionName, setSessionName] = useState('');

  const handleCreate = async () => {
    if (!sessionName.trim()) return;
    setIsCreating(true);
    await onCreate(sessionName.trim());
    setSessionName('');
    setIsCreating(false);
  };

  return (
    <aside className="w-80 border-r border-slate-200 bg-white/70 backdrop-blur overflow-y-auto h-screen">
      <div className="px-4 py-5 border-b border-slate-200">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold text-slate-900">Engagements</h2>
            <p className="text-sm text-slate-500">Track each client interaction</p>
          </div>
          <button
            onClick={onRefresh}
            className="inline-flex items-center gap-1 rounded-md border border-slate-200 px-2 py-1 text-sm text-slate-600 hover:bg-slate-100"
          >
            <RefreshCcw className={clsx('h-4 w-4', { 'animate-spin': isLoading })} />
          </button>
        </div>
        <div className="mt-4 space-y-2">
          <input
            className="w-full rounded-md border border-slate-200 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
            placeholder="New engagement name"
            value={sessionName}
            onChange={(event) => setSessionName(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === 'Enter') {
                event.preventDefault();
                void handleCreate();
              }
            }}
          />
          <button
            onClick={() => void handleCreate()}
            disabled={isCreating}
            className="w-full inline-flex items-center justify-center gap-2 rounded-md bg-primary px-3 py-2 text-sm font-medium text-white shadow-sm hover:bg-primary/90 disabled:cursor-not-allowed disabled:bg-primary/60"
          >
            <PlusCircle className="h-4 w-4" />
            Create session
          </button>
        </div>
      </div>
      <nav className="px-2 py-4 space-y-1">
        {sessions.map((session) => (
          <button
            key={session.id}
            onClick={() => onSelect(session)}
            className={clsx(
              'w-full rounded-lg border px-3 py-3 text-left transition hover:border-primary/40 hover:bg-primary/5',
              selectedId === session.id
                ? 'border-primary bg-primary/10 text-primary'
                : 'border-transparent bg-white text-slate-700 shadow-sm'
            )}
          >
            <div className="flex items-center justify-between text-sm font-medium">
              <span>{session.name}</span>
              <span className="text-xs text-slate-400">{dayjs(session.updatedAt).fromNow()}</span>
            </div>
            <p className="mt-1 text-xs text-slate-500">
              {session.reportId ? 'Has draft report' : 'No report yet'}
            </p>
          </button>
        ))}
        {sessions.length === 0 && !isLoading ? (
          <p className="px-2 text-sm text-slate-500">No sessions yet. Create one to get started.</p>
        ) : null}
      </nav>
    </aside>
  );
};
