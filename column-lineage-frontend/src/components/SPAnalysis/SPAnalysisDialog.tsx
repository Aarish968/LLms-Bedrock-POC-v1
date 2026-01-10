/**
 * Stored Procedure Analysis Dialog Component
 * Dialog for starting and monitoring stored procedure analysis
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
  Storage,
  Refresh,
} from '@mui/icons-material';

import { SPAnalysisService } from '../../api/spAnalysisService';
import useSPAnalysis from '../../hooks/useSPAnalysis';
import {
  SPAnalysisResponse,
  SPAnalysisJob,
  SPJobStatus,
  SPAnalysisRequest,
} from '../../types/spAnalysis';

// Debug Info Component
const DebugInfo: React.FC<{
  currentJobId: string | null;
  jobStatus: SPAnalysisJob | null;
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

interface SPAnalysisDialogProps {
  open: boolean;
  onClose: () => void;
  onAnalysisStarted?: (response: SPAnalysisResponse) => void;
}

const SPAnalysisDialog: React.FC<SPAnalysisDialogProps> = ({
  open,
  onClose,
  onAnalysisStarted,
}) => {
  const [currentJobId, setCurrentJobId] = useState<string | null>(null);
  const [jobStatus, setJobStatus] = useState<SPAnalysisJob | null>(null);
  const [isStarting, setIsStarting] = useState(false);
  const [isRedirecting, setIsRedirecting] = useState(false);
  
  const { hasRunningJob, startAnalysis } = useSPAnalysis();

  // Derived states
  const isAnalysisRunning = isStarting;
  const isJobRunning = jobStatus?.status === SPJobStatus.PENDING || 
                      jobStatus?.status === SPJobStatus.RUNNING;
  const isJobCompleted = jobStatus?.status === SPJobStatus.COMPLETED;
  const canStartNewAnalysis = !hasRunningJob && !isJobRunning;

  const handleStartAnalysis = async () => {
    setIsStarting(true);

    try {
      const request: SPAnalysisRequest = {
        sf_environment: 'prod',
        max_workers: 4,
        resume_from_partial: true,
      };

      // Use hook's startAnalysis method which handles immediate state update
      const response = await startAnalysis(request);
      
      if (!response) {
        throw new Error('Failed to start SP analysis');
      }

      setCurrentJobId(response.job_id);
      setJobStatus({
        job_id: response.job_id,
        status: response.status,
        sf_environment: 'prod',
        max_workers: 4,
        total_procedures: 0,
        completed_procedures: 0,
        failed_procedures: 0,
        started_at: response.started_at,
        request_params: request,
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
      console.error('Failed to start SP analysis:', err);
    } finally {
      setIsStarting(false);
    }
  };

  const pollJobStatus = async (jobId: string) => {
    try {
      const status = await SPAnalysisService.getJobStatus(jobId);
      setJobStatus(status);

      // Continue polling if job is still running
      if (status.status === SPJobStatus.PENDING || 
          status.status === SPJobStatus.RUNNING) {
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
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            {/* <Storage color="primary" /> */}
            <Typography variant="h6">
              Stored Procedure Analysis
            </Typography>
          </Box>
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
              Analyze stored procedures to extract table-column relationships using AI-powered analysis.
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
                Analysis started successfully! Redirecting to SP Analysis Jobs section...
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

export default SPAnalysisDialog;