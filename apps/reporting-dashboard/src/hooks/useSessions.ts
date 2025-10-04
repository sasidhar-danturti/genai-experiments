import { useEffect, useState } from 'react';
import { createSession, listSessions } from '../api/sessions';
import { SessionSummary } from '../types';

export const useSessions = () => {
  const [sessions, setSessions] = useState<SessionSummary[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const refresh = async () => {
    try {
      setIsLoading(true);
      const data = await listSessions();
      setSessions(data);
      setError(null);
    } catch (err) {
      console.error(err);
      setError('Unable to load sessions');
    } finally {
      setIsLoading(false);
    }
  };

  const create = async (name: string) => {
    const session = await createSession(name);
    setSessions((prev) => [session, ...prev]);
    return session;
  };

  useEffect(() => {
    void refresh();
  }, []);

  return { sessions, isLoading, error, refresh, create };
};
