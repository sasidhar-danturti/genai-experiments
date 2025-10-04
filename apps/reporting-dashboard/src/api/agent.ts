import client from './client';
import { AgentInvocationPayload, AgentInvocationResponse } from '../types';

export const invokeAgent = async (payload: AgentInvocationPayload) => {
  const { data } = await client.post<AgentInvocationResponse>('/agent/invoke', payload);
  return data;
};
