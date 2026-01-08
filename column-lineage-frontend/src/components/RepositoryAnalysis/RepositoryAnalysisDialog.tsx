/**
 * Repository Analysis Dialog Component
 * Dialog for starting and monitoring repository analysis
 */

import React, { useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Typography,
  Box,
  Alert,
  Chip,
} from '@mui/material';
import {
  PlayArrow,
  Refresh,
} from '@mui/icons-material';

import { RepositoryAnalysisService } from '../../api/repositoryAnalysisService';
import useRepositoryAnalysis from '../../hooks/useRepositoryAnalysis';
import {
  RepositoryAnalysisResponse,
  RepositoryAnalysisJob,
  AnalysisStatus,
} from '../../types/repositoryAnalysis';

// Debug Info Component
const DebugInfo: React.FC<{
  currentJobId: string | null;
  jobStatus: RepositoryAnalysisJob | null;
  isAnalysisRunning: boolean;
  isJobRunning: boolean;
  isJobCompleted: boolean;
}> = ({ currentJobId, jobStatus, isAnalysisRunning, isJobRunning, isJobCompleted }) => {
  return (
    <Box sx={{ mb: 3, p: 2, bgcolor: 'grey.50', borderRadius: 1 }}>
      <Typography variant="subtitle2" sx={{ mb: 1, fontWeight: 600 }}>
        Debug Info
      </Typography>
      <Box sx={{ display: 'flex', gap: 1, flexWrap: 'wrap' }}>
        <Chip
          label={`Job ID: ${currentJobId || 'null'}`}
          size="small"
          variant="outlined"
          sx={{ bgcolor: 'white' }}
        />
        <Chip
          label={`Analysis Running: ${isAnalysisRunning}`}
          size="small"
          variant="outlined"
          sx={{ bgcolor: 'white' }}
        />
        <Chip
          label={`Job Running: ${isJobRunning}`}
          size="small"
          variant="outlined"
          sx={{ bgcolor: 'white' }}
        />
        <Chip
          label={`Job Completed: ${isJobCompleted}`}
          size="small"
          variant="outlined"
          sx={{ bgcolor: 'white' }}
        />
      </Box>
    </Box>
  );
};

interface RepositoryAnalysisDialogProps {
  open: boolean;
  onClose: () => void;
  onAnalysisStarted?: (response: RepositoryAnalysisResponse) => void;
}

const RepositoryAnalysisDialog: React.FC<RepositoryAnalysisDialogProps> = ({
  open,
  onClose,
  onAnalysisStarted,
}) => {
  const [currentJobId, setCurrentJobId] = useState<string | null>(null);
  const [jobStatus, setJobStatus] = useState<RepositoryAnalysisJob | null>(null);
  const [isStarting, setIsStarting] = useState(false);
  const [isRedirecting, setIsRedirecting] = useState(false);
  const { hasRunningJob, startAnalysis } = useRepositoryAnalysis();

  // Derived states
  const isAnalysisRunning = isStarting;
  const isJobRunning = jobStatus?.status === AnalysisStatus.PENDING || 
                      jobStatus?.status === AnalysisStatus.CLONING || 
                      jobStatus?.status === AnalysisStatus.RUNNING;
  const isJobCompleted = jobStatus?.status === AnalysisStatus.COMPLETED;
  const canStartNewAnalysis = !hasRunningJob && !isJobRunning;

  const handleStartAnalysis = async () => {
    setIsStarting(true);

    try {
      // Use hook's startAnalysis method which handles immediate state update
      const response = await startAnalysis();
      
      if (!response) {
        throw new Error('Failed to start analysis');
      }

      setCurrentJobId(response.job_id);
      setJobStatus({
        job_id: response.job_id,
        status: response.status,
        message: response.message,
        output_file: response.output_file,
        started_at: response.started_at,
      });
      
      // Notify parent component immediately for instant redirect
      if (onAnalysisStarted) {
        onAnalysisStarted(response);
      }

      // Show redirect message
      setIsRedirecting(true);

      // Start polling for job status
      pollJobStatus(response.job_id);

      // Auto-close dialog after 1 second for faster redirect
      setTimeout(() => {
        handleClose();
      }, 1000);

    } catch (err: any) {
      // Handle error silently or show minimal error
      console.error('Failed to start analysis:', err);
    } finally {
      setIsStarting(false);
    }
  };

  const pollJobStatus = async (jobId: string) => {
    try {
      const status = await RepositoryAnalysisService.getJobStatus(jobId);
      setJobStatus(status);

      // Continue polling if job is still running
      if (status.status === AnalysisStatus.PENDING || 
          status.status === AnalysisStatus.CLONING || 
          status.status === AnalysisStatus.RUNNING) {
        setTimeout(() => pollJobStatus(jobId), 2000);
      }
    } catch (err) {
      // Continue polling even if there's an error
      setTimeout(() => pollJobStatus(jobId), 5000);
    }
  };

  const resetWorkflow = () => {
    setCurrentJobId(null);
    setJobStatus(null);
    setIsRedirecting(false);
  };

  const handleClose = () => {
    // Only allow closing if no job is running
    if (isJobRunning) {
      return;
    }
    resetWorkflow();
    onClose();
  };

  const handleNewAnalysis = () => {
    resetWorkflow();
  };

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      maxWidth="md"
      fullWidth
      disableEscapeKeyDown={isJobRunning} // Prevent closing during job execution
    >
      <DialogTitle>
        <Box display="flex" alignItems="center" justifyContent="space-between">
          <Typography variant="h6">Repository Analysis</Typography>
          {currentJobId && (
            <Chip
              label={`Job: ${currentJobId.slice(0, 8)}...`}
              size="small"
              variant="outlined"
            />
          )}
        </Box>
      </DialogTitle>

      <DialogContent>
        {/* Debug Info */}
        <DebugInfo
          currentJobId={currentJobId}
          jobStatus={jobStatus}
          isAnalysisRunning={isAnalysisRunning}
          isJobRunning={isJobRunning}
          isJobCompleted={isJobCompleted}
        />

        {!currentJobId ? (
          // Configuration Phase
          <Box>
            <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
              Start Analyze Action To Endpoint Analysis.
            </Typography>
            
            {!canStartNewAnalysis && (
              <Alert severity="warning" sx={{ mb: 2 }}>
                Another analysis is currently in progress. Please wait for it to complete.
              </Alert>
            )}
          </Box>
        ) : (
          // Job Status Phase - Minimal display
          <Box>
            {isRedirecting && (
              <Alert severity="info" sx={{ mt: 2 }}>
                Analysis started successfully! Redirecting to Repository Analysis Jobs section...
              </Alert>
            )}
          </Box>
        )}
      </DialogContent>

      <DialogActions>
        <Button 
          onClick={handleClose}
          disabled={isJobRunning}
        >
          {isJobRunning ? 'Running...' : 'Close'}
        </Button>

        {!currentJobId ? (
          <Button
            variant="contained"
            startIcon={<PlayArrow />}
            onClick={handleStartAnalysis}
            disabled={!canStartNewAnalysis}
          >
            {isAnalysisRunning ? 'Starting...' : 'Start Analysis'}
          </Button>
        ) : (
          <Button
            variant="outlined"
            startIcon={<Refresh />}
            onClick={handleNewAnalysis}
            disabled={isJobRunning}
          >
            New Analysis
          </Button>
        )}
      </DialogActions>
    </Dialog>
  );
};

export default RepositoryAnalysisDialog;