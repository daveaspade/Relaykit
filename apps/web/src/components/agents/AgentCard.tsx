import React from 'react';
import AgentStateBadge, { AgentState } from './AgentStateBadge';
import ModelTierBadge from './ModelTierBadge';

export interface AgentCardProps {
  id: string;
  label: string;
  purpose: string;
  state: AgentState;
  modelTier: 'local' | 'network' | 'premium';
  currentTask?: string;
  modelId?: string;
}

const AgentCard: React.FC<AgentCardProps> = ({
  label,
  purpose,
  state,
  modelTier,
  currentTask,
  modelId,
}) => {
  return (
    <div className="bg-surface border border-border rounded-lg shadow-card p-3.5 hover:shadow-panel transition duration-200 w-full flex flex-col gap-2">
      <div className="flex items-center justify-between gap-2">
        <div className="flex-1 min-w-0">
          <h3 className="text-sm font-semibold text-text-primary truncate px-1 -mx-1 rounded hover:bg-surface-subtle cursor-text transition-colors">
            {label}
          </h3>
        </div>
        <AgentStateBadge state={state} />
      </div>

      <p className="text-xs text-text-muted truncate" title={purpose}>
        {purpose}
      </p>

      {currentTask && (
        <div className="bg-surface-subtle border border-border-subtle rounded p-2 mt-1">
          <p className="text-[10px] font-mono text-text-secondary truncate">
            {currentTask}
          </p>
        </div>
      )}

      <div className="flex items-center justify-between mt-1 pt-1 border-t border-border-subtle">
        <ModelTierBadge tier={modelTier} />
        {modelId && (
          <span className="text-[10px] font-mono text-text-muted truncate max-w-[50%]">
            {modelId}
          </span>
        )}
      </div>
    </div>
  );
};

export default AgentCard;
