import { FormEvent, useState } from 'react';
import { Loader2, Send } from 'lucide-react';
import { Message } from '../types';

interface ChatPanelProps {
  messages: Message[];
  onSend: (prompt: string) => Promise<void>;
  isLoading: boolean;
  disabled: boolean;
}

export const ChatPanel = ({ messages, onSend, isLoading, disabled }: ChatPanelProps) => {
  const [input, setInput] = useState('');
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    if (!input.trim()) return;

    try {
      setError(null);
      await onSend(input.trim());
      setInput('');
    } catch (err) {
      console.error(err);
      setError('Agent invocation failed');
    }
  };

  return (
    <section className="flex h-full flex-col">
      <header className="border-b border-slate-200 px-6 py-4">
        <h3 className="text-lg font-semibold text-slate-900">Agent Conversation</h3>
        <p className="text-sm text-slate-500">
          Ask clarifying questions and iteratively refine the deliverable.
        </p>
      </header>
      <div className="flex-1 space-y-4 overflow-y-auto px-6 py-6">
        {messages.map((message) => (
          <div key={message.id} className="flex flex-col">
            <span className="text-xs font-medium text-slate-400">{message.role.toUpperCase()}</span>
            <div
              className={
                message.role === 'user'
                  ? 'mt-1 rounded-lg bg-primary/10 px-4 py-3 text-sm text-slate-900'
                  : 'mt-1 rounded-lg bg-white px-4 py-3 text-sm text-slate-700 shadow'
              }
            >
              {message.content}
            </div>
          </div>
        ))}
        {messages.length === 0 ? (
          <div className="rounded-lg border border-dashed border-slate-300 bg-white px-4 py-6 text-center text-sm text-slate-500">
            Start by asking the agent about the client's goals or available data sources.
          </div>
        ) : null}
      </div>
      <footer className="border-t border-slate-200 px-6 py-4">
        <form className="space-y-3" onSubmit={handleSubmit}>
          <textarea
            className="h-28 w-full resize-none rounded-lg border border-slate-300 px-3 py-3 text-sm shadow-sm focus:outline-none focus:ring-2 focus:ring-primary"
            placeholder="Ask the agent to expand the executive summary, request visualisations, or supply more data context"
            value={input}
            disabled={disabled}
            onChange={(event) => setInput(event.target.value)}
          />
          {error ? <p className="text-sm text-rose-500">{error}</p> : null}
          <div className="flex items-center justify-between">
            <p className="text-xs text-slate-400">Shift + Enter to insert a newline.</p>
            <button
              type="submit"
              disabled={disabled || isLoading}
              className="inline-flex items-center gap-2 rounded-md bg-secondary px-4 py-2 text-sm font-medium text-white shadow-sm hover:bg-secondary/90 disabled:cursor-not-allowed disabled:bg-secondary/60"
            >
              {isLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
              Send to agent
            </button>
          </div>
        </form>
      </footer>
    </section>
  );
};
