import dayjs from 'dayjs';
import { nanoid } from 'nanoid';
import { AgentInvocationPayload, InsightReport, Message } from './types.js';

interface ProxyOptions {
  workspaceUrl?: string;
  token?: string;
}

const defaultRecommendations = [
  'Prioritise data quality remediation to increase model reliability.',
  'Establish executive sponsorship and a governance cadence for the programme.',
  'Define measurable KPIs and align tooling investments with business value.'
];

export const invokeDatabricksAgent = async (
  payload: AgentInvocationPayload,
  existingReport: InsightReport | null,
  options: ProxyOptions
) => {
  const { prompt } = payload;
  const isFirstPass = !existingReport || existingReport.sections.length === 0;
  const timestamp = new Date().toISOString();

  const assistantMessage: Message = {
    id: nanoid(),
    role: 'assistant',
    createdAt: timestamp,
    content: `Simulated Databricks agent response. Received prompt: "${prompt}".` +
      (options.workspaceUrl
        ? ` Routed through workspace ${options.workspaceUrl}.`
        : ' Configure DATABRICKS_WORKSPACE_URL to connect to a real workspace.')
  };

  if (isFirstPass) {
    const newReport: Partial<InsightReport> = {
      title: existingReport?.title ?? 'Agent Generated Insight Report',
      executiveSummary:
        existingReport?.executiveSummary ??
        `On ${dayjs(timestamp).format('MMMM D, YYYY')}, the Databricks agent analysed the engagement context and drafted an actionable executive summary.`,
      sections: [
        {
          id: nanoid(),
          title: 'Business Context',
          body: 'Summarise strategic objectives, target stakeholders, and success measures gathered from discovery.'
        },
        {
          id: nanoid(),
          title: 'Data Landscape',
          body: 'Detail the key data sources, quality considerations, and architecture landscape available in the lakehouse.'
        },
        {
          id: nanoid(),
          title: 'Analytics Opportunities',
          body: 'Highlight high-impact analytics and AI use cases that align to the stated goals. Include quick wins and long-term bets.'
        }
      ],
      recommendations: defaultRecommendations
    };

    return {
      messages: [assistantMessage],
      reportDelta: newReport
    };
  }

  const updatedSections = existingReport.sections.map((section) => ({
    ...section,
    body: `${section.body}\n\nAgent note (${dayjs(timestamp).format('h:mm A')}): ${prompt}`
  }));

  const reportDelta: Partial<InsightReport> = {
    executiveSummary:
      existingReport.executiveSummary +
      `\n\nAgent addendum (${dayjs(timestamp).format('h:mm A')}): ${prompt}.` +
      ' Recommended next step: validate assumptions with stakeholders.',
    sections: updatedSections,
    recommendations: existingReport.recommendations.concat(`Incorporate follow-up action: ${prompt}`)
  };

  return {
    messages: [assistantMessage],
    reportDelta
  };
};
