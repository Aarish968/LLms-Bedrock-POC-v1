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
  CircularProgress,
  Alert,
  Chip,
  Divider,
  LinearProgress,
} from '@mui/material';
import {
  PlayArrow,
  Code,
  CheckCircle,
  Error as ErrorIcon,
  Cancel,
  Refresh,
} from '@mui/icons-material';

import { RepositoryAnalysisService } from '../../api/repositoryAnalysisService';
import useRepositoryAnalysis from '../../hooks/useRepositoryAnalysis';
import {
  RepositoryAnalysisResponse,
  RepositoryAnalysisJob,
  AnalysisStatus,
} from '../../types/repositoryAnalysis';
import RepositoryDebugInfo from './RepositoryDebugInfo';

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
  const [error, setError] = useState<string | null>(null);
  const [isRedirecting, setIsRedirecting] = useState(false);
  const { hasRunningJob } = useRepositoryAnalysis();

  // Derived states
  const isAnalysisRunning = isStarting;
  const isJobRunning = jobStatus?.status === AnalysisStatus.PENDING || 
                      jobStatus?.status === AnalysisStatus.CLONING || 
                      jobStatus?.status === AnalysisStatus.RUNNING;
  const isJobCompleted = jobStatus?.status === AnalysisStatus.COMPLETED;
  const isJobFailed = jobStatus?.status === AnalysisStatus.FAILED;
  const canStartNewAnalysis = !hasRunningJob && !isJobRunning;

  const handleStartAnalysis = async () => {
    setIsStarting(true);
    setError(null);

    try {
      const response = await RepositoryAnalysisService.startAnalysis({
        async_processing: true,
      });

      setCurrentJobId(response.job_id);
      setJobStatus({
        job_id: response.job_id,
        status: response.status,
        message: response.message,
        output_file: response.output_file,
        started_at: response.started_at,
      });
      
      // Notify parent component
      if (onAnalysisStarted) {
        onAnalysisStarted(response);
      }

      // Show redirect message
      setIsRedirecting(true);

      // Start polling for job status
      pollJobStatus(response.job_id);

      // Auto-close dialog after 2 seconds to redirect to jobs section
      setTimeout(() => {
        handleClose();
      }, 2000);

    } catch (err: any) {
      console.error('Failed to start repository analysis:', err);
      setError(
        err.response?.data?.detail || 
        err.message || 
        'Failed to start repository analysis'
      );
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
      console.error('Failed to get job status:', err);
      // Continue polling even if there's an error
      setTimeout(() => pollJobStatus(jobId), 5000);
    }
  };

  const handleViewResults = async () => {
    if (!currentJobId) return;

    try {
      const results = await RepositoryAnalysisService.getResults(currentJobId);
      // Just log results, don't show them in the dialog
      console.log('Repository analysis results:', results);
    } catch (err) {
      console.error('Failed to load results:', err);
    }
  };

  const handleClose = () => {
    // Only allow closing if no job is running
    if (isJobRunning) {
      return;
    }
    resetWorkflow();
    onClose();
  };

  const resetWorkflow = () => {
    setCurrentJobId(null);
    setJobStatus(null);
    setError(null);
    setIsRedirecting(false);
  };

  const handleNewAnalysis = () => {
    resetWorkflow();
  };

  const getStatusMessage = () => {
    if (!jobStatus) return '';
    
    switch (jobStatus.status) {
      case AnalysisStatus.PENDING:
        return 'Repository analysis is starting...';
      case AnalysisStatus.CLONING:
        return 'Cloning repositories...';
      case AnalysisStatus.RUNNING:
        return 'Analyzing repository structure and dependencies...';
      case AnalysisStatus.COMPLETED:
        return 'Repository analysis completed successfully!';
      case AnalysisStatus.FAILED:
        return 'Repository analysis failed. Please check the error message below.';
      case AnalysisStatus.CANCELLED:
        return 'Repository analysis was cancelled.';
      default:
        return jobStatus.message || '';
    }
  };

  const getStatusColor = (status: AnalysisStatus) => {
    switch (status) {
      case AnalysisStatus.PENDING:
        return 'warning';
      case AnalysisStatus.CLONING:
      case AnalysisStatus.RUNNING:
        return 'info';
      case AnalysisStatus.COMPLETED:
        return 'success';
      case AnalysisStatus.FAILED:
        return 'error';
      case AnalysisStatus.CANCELLED:
        return 'default';
      default:
        return 'default';
    }
  };

  const getStatusIcon = (status: AnalysisStatus) => {
    switch (status) {
      case AnalysisStatus.PENDING:
      case AnalysisStatus.CLONING:
      case AnalysisStatus.RUNNING:
        return <CircularProgress size={16} />;
      case AnalysisStatus.COMPLETED:
        return <CheckCircle />;
      case AnalysisStatus.FAILED:
        return <ErrorIcon />;
      case AnalysisStatus.CANCELLED:
        return <Cancel />;
      default:
        return undefined;
    }
  };

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      maxWidth="md"
      fullWidth
      disableEscapeKeyDown={isJobRunning} // Prevent closing during job execution
      PaperProps={{
        sx: { borderRadius: 2 }
      }}
    >
      <DialogTitle>
        <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Code color="primary" />
            <Typography variant="h6">
              Repository Analysis
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
        <RepositoryDebugInfo
          currentJobId={currentJobId}
          jobStatus={jobStatus}
          isAnalysisRunning={isAnalysisRunning}
          isJobRunning={isJobRunning}
          isJobCompleted={isJobCompleted}
        />

        <Box sx={{ py: 1 }}>
          {!currentJobId ? (
            // Configuration Phase
            <Box>
              {hasRunningJob && (
                <Alert severity="warning" sx={{ mb: 2 }}>
                  There is already a repository analysis job running. Please wait for it to complete before starting a new one.
                </Alert>
              )}

              <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
                Start Analyze Action To Endpoint Analysis..
              </Typography>
              
              {!canStartNewAnalysis && (
                <Alert severity="warning" sx={{ mb: 2 }}>
                  Another analysis is currently in progress. Please wait for it to complete.
                </Alert>
              )}
            </Box>
          ) : (
            // Job Status Phase
            <Box>
              <Box sx={{ mb: 3 }}>
                <Box display="flex" alignItems="center" justifyContent="space-between" sx={{ mb: 1 }}>
                  <Typography variant="subtitle1">Analysis Status</Typography>
                  <Chip
                    {...(getStatusIcon(jobStatus?.status || AnalysisStatus.PENDING) && { 
                      icon: getStatusIcon(jobStatus?.status || AnalysisStatus.PENDING) 
                    })}
                    label={jobStatus?.status?.toUpperCase() || 'Unknown'}
                    color={getStatusColor(jobStatus?.status || AnalysisStatus.PENDING)}
                    size="small"
                  />
                </Box>

                <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                  {getStatusMessage()}
                </Typography>

                {jobStatus && (jobStatus.status === AnalysisStatus.PENDING || 
                              jobStatus.status === AnalysisStatus.CLONING || 
                              jobStatus.status === AnalysisStatus.RUNNING) && (
                  <Box>
                    <LinearProgress sx={{ mb: 2 }} />
                    <Typography variant="caption" color="text.secondary">
                      Started: {new Date(jobStatus.started_at).toLocaleString()}
                    </Typography>
                  </Box>
                )}

                {isJobFailed && jobStatus?.error_message && (
                  <Alert severity="error" sx={{ mt: 2 }}>
                    {jobStatus.error_message}
                  </Alert>
                )}

                {isJobCompleted && (
                  <Alert severity="success" sx={{ mt: 2 }}>
                    Repository analysis completed successfully! 
                    {jobStatus?.output_file && ` Output file: ${jobStatus.output_file}`}
                  </Alert>
                )}

                {isRedirecting && (
                  <Alert severity="info" sx={{ mt: 2 }}>
                    Analysis started successfully! Redirecting to Repository Analyze Job section...
                  </Alert>
                )}
              </Box>
            </Box>
          )}

          {error && (
            <Alert severity="error" sx={{ mt: 2 }}>
              <Typography variant="body2">
                {error}
              </Typography>
            </Alert>
          )}
        </Box>
      </DialogContent>

      <DialogActions sx={{ px: 3, pb: 2 }}>
        <Button 
          onClick={handleClose}
          disabled={isJobRunning}
          color="inherit"
        >
          {isJobRunning ? 'Running...' : 'Close'}
        </Button>

        {!currentJobId ? (
          <Button
            onClick={handleStartAnalysis}
            variant="contained"
            startIcon={isStarting ? <CircularProgress size={16} /> : <PlayArrow />}
            disabled={isStarting || !canStartNewAnalysis}
            title={hasRunningJob ? 'Please wait for current analysis to complete' : undefined}
          >
            {isStarting ? 'Starting...' : 'Start Analysis'}
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