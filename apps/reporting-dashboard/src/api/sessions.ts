import client from './client';
import { InsightReport, SessionSummary } from '../types';

export const listSessions = async () => {
  const { data } = await client.get<SessionSummary[]>('/sessions');
  return data;
};

export const createSession = async (name: string) => {
  const { data } = await client.post<SessionSummary>('/sessions', { name });
  return data;
};

export const getSessionReport = async (sessionId: string) => {
  const response = await client.get<InsightReport | null>(`/sessions/${sessionId}/report`, {
    validateStatus: (status) => [200, 204].includes(status)
  });
  if (response.status === 204) {
    return null;
  }
  return response.data;
};

export const saveSessionReport = async (sessionId: string, report: InsightReport) => {
  const { data } = await client.put<InsightReport>(`/sessions/${sessionId}/report`, report);
  return data;
};
