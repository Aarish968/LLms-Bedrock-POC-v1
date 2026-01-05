import React from 'react';
import { Box, Typography, Paper, Chip } from '@mui/material';

interface DebugInfoProps {
  currentJobId: string | null;
  jobStatus: any;
  isAnalysisRunning: boolean;
  isJobRunning: boolean;
  isJobCompleted: boolean;
}

const DebugInfo: React.FC<DebugInfoProps> = ({
  currentJobId,
  jobStatus,
  isAnalysisRunning,
  isJobRunning,
  isJobCompleted,
}) => {
  return (
    <Paper sx={{ p: 2, mb: 2, bgcolor: 'grey.50' }}>
      <Typography variant="h6" gutterBottom>
        Debug Info
      </Typography>
      
      <Box display="flex" flexWrap="wrap" gap={1} mb={1}>
        <Chip 
          label={`Job ID: ${currentJobId || 'null'}`} 
          color={currentJobId ? 'primary' : 'default'}
          size="small"
        />
        <Chip 
          label={`Analysis Running: ${isAnalysisRunning}`} 
          color={isAnalysisRunning ? 'warning' : 'default'}
          size="small"
        />
        <Chip 
          label={`Job Running: ${isJobRunning}`} 
          color={isJobRunning ? 'info' : 'default'}
          size="small"
        />
        <Chip 
          label={`Job Completed: ${isJobCompleted}`} 
          color={isJobCompleted ? 'success' : 'default'}
          size="small"
        />
      </Box>
      
      {jobStatus && (
        <Box>
          <Typography variant="body2" gutterBottom>
            Job Status Data:
          </Typography>
          <pre style={{ fontSize: '12px', overflow: 'auto' }}>
            {JSON.stringify(jobStatus, null, 2)}
          </pre>
        </Box>
      )}
    </Paper>
  );
};

export default DebugInfo;