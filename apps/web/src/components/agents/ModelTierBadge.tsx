import React from 'react';

interface ModelTierBadgeProps {
  tier: 'local' | 'network' | 'premium';
}

const ModelTierBadge: React.FC<ModelTierBadgeProps> = ({ tier }) => {
  const configs = {
    local: {
      label: 'Local',
      className: 'text-tier-local bg-green-50 border-green-100', // Assuming these exist or fallback
    },
    network: {
      label: 'Network',
      className: 'text-tier-network bg-amber-50 border-amber-100',
    },
    premium: {
      label: 'Premium',
      className: 'text-tier-premium bg-purple-50 border-purple-100',
    },
  };

  const { label, className } = configs[tier];

  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ${className}`}>
      {label}
    </span>
  );
};

export default ModelTierBadge;
