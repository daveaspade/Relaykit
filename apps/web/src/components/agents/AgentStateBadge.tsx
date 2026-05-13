import React from 'react';

export type AgentState = 'idle' | 'planning' | 'executing' | 'waiting' | 'verifying' | 'done' | 'error';

interface AgentStateBadgeProps {
  state: AgentState;
}

const AgentStateBadge: React.FC<AgentStateBadgeProps> = ({ state }) => {
  const stateConfigs: Record<AgentState, { label: string; dotColor: string; textColor: string }> = {
    idle: { label: 'Idle', dotColor: 'bg-state-idle', textColor: 'text-state-idle' },
    planning: { label: 'Planning', dotColor: 'bg-state-planning', textColor: 'text-state-planning' },
    executing: { label: 'Executing', dotColor: 'bg-state-executing animate-pulse', textColor: 'text-state-executing' },
    waiting: { label: 'Waiting', dotColor: 'bg-state-waiting', textColor: 'text-state-waiting' },
    verifying: { label: 'Verifying', dotColor: 'bg-state-verifying', textColor: 'text-state-verifying' },
    done: { label: 'Done', dotColor: 'bg-state-done', textColor: 'text-state-done' },
    error: { label: 'Error', dotColor: 'bg-state-error', textColor: 'text-state-error' },
  };

  const { label, dotColor, textColor } = stateConfigs[state];

  return (
    <div className={`flex items-center gap-1.5 px-2 py-1 rounded-md text-xs font-medium ${textColor}`}>
      <span className={`h-1.5 w-1.5 rounded-full ${dotColor}`} />
      <span>{label}</span>
    </div>
  );
};

export default AgentStateBadge;
