/**
 * Repository Analysis Debug Info Component
 * Shows debug information for repository analysis workflow
 */

import React from 'react';
import { Box, Typography, Chip } from '@mui/material';
import { RepositoryAnalysisJob } from '../../types/repositoryAnalysis';

interface RepositoryDebugInfoProps {
  currentJobId: string | null;
  jobStatus: RepositoryAnalysisJob | null;
  isAnalysisRunning: boolean;
  isJobRunning: boolean;
  isJobCompleted: boolean;
}

const RepositoryDebugInfo: React.FC<RepositoryDebugInfoProps> = ({
  currentJobId,
  jobStatus,
  isAnalysisRunning,
  isJobRunning,
  isJobCompleted,
}) => {
  return (
    <Box sx={{ mb: 3, p: 2, bgcolor: 'grey.50', borderRadius: 1 }}>
      <Typography variant="subtitle2" gutterBottom>
        Debug Info
      </Typography>
      <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 1 }}>
        <Chip
          label={`Job ID: ${currentJobId || 'null'}`}
          size="small"
          variant="outlined"
        />
        <Chip
          label={`Analysis Running: ${isAnalysisRunning}`}
          size="small"
          variant="outlined"
          color={isAnalysisRunning ? 'info' : 'default'}
        />
        <Chip
          label={`Job Running: ${isJobRunning}`}
          size="small"
          variant="outlined"
          color={isJobRunning ? 'warning' : 'default'}
        />
        <Chip
          label={`Job Completed: ${isJobCompleted}`}
          size="small"
          variant="outlined"
          color={isJobCompleted ? 'success' : 'default'}
        />
      </Box>
    </Box>
  );
};

export default RepositoryDebugInfo;