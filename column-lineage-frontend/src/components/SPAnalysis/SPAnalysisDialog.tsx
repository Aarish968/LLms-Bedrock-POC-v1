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
  CircularProgress,
  Alert,
  Chip,
  LinearProgress,
  FormControl,
  InputLabel,
  Select,
  MenuItem,
  TextField,
  FormControlLabel,
  Switch,
} from '@mui/material';
import {
  PlayArrow,
  Storage,
  CheckCircle,
  Error as ErrorIcon,
  Cancel,
  Refresh,
  Settings,
} from '@mui/icons-material';

import { SPAnalysisService } from '../../api/spAnalysisService';
import useSPAnalysis from '../../hooks/useSPAnalysis';
import {
  SPAnalysisResponse,
  SPAnalysisJob,
  SPJobStatus,
  SPAnalysisRequest,
} from '../../types/spAnalysis';

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
  const [error, setError] = useState<string | null>(null);
  const [isRedirecting, setIsRedirecting] = useState(false);
  
  // Configuration state
  const [sfEnvironment, setSfEnvironment] = useState('prod');
  const [maxWorkers, setMaxWorkers] = useState(4);
  const [resumeFromPartial, setResumeFromPartial] = useState(true);
  
  const { hasRunningJob, startAnalysis } = useSPAnalysis();

  // Derived states
  const isJobRunning = jobStatus?.status === SPJobStatus.PENDING || 
                      jobStatus?.status === SPJobStatus.RUNNING;
  const isJobCompleted = jobStatus?.status === SPJobStatus.COMPLETED;
  const isJobFailed = jobStatus?.status === SPJobStatus.FAILED;
  const canStartNewAnalysis = !hasRunningJob && !isJobRunning;

  const handleStartAnalysis = async () => {
    setIsStarting(true);
    setError(null);

    try {
      const request: SPAnalysisRequest = {
        sf_environment: sfEnvironment,
        max_workers: maxWorkers,
        resume_from_partial: resumeFromPartial,
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
        sf_environment: sfEnvironment,
        max_workers: maxWorkers,
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
      setError(
        err.response?.data?.detail || 
        err.message || 
        'Failed to start stored procedure analysis'
      );
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
    setError(null);
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

  const getStatusMessage = () => {
    if (!jobStatus) return '';
    
    switch (jobStatus.status) {
      case SPJobStatus.PENDING:
        return 'Stored procedure analysis is starting...';
      case SPJobStatus.RUNNING:
        return `Analyzing stored procedures... (${jobStatus.completed_procedures}/${jobStatus.total_procedures} completed)`;
      case SPJobStatus.COMPLETED:
        return 'Stored procedure analysis completed successfully!';
      case SPJobStatus.FAILED:
        return 'Stored procedure analysis failed. Please check the error message below.';
      case SPJobStatus.CANCELLED:
        return 'Stored procedure analysis was cancelled.';
      default:
        return 'Processing...';
    }
  };

  const getStatusColor = (status: SPJobStatus) => {
    switch (status) {
      case SPJobStatus.PENDING:
        return 'warning';
      case SPJobStatus.RUNNING:
        return 'info';
      case SPJobStatus.COMPLETED:
        return 'success';
      case SPJobStatus.FAILED:
        return 'error';
      case SPJobStatus.CANCELLED:
        return 'default';
      default:
        return 'default';
    }
  };

  const getStatusIcon = (status: SPJobStatus) => {
    switch (status) {
      case SPJobStatus.PENDING:
      case SPJobStatus.RUNNING:
        return <CircularProgress size={16} />;
      case SPJobStatus.COMPLETED:
        return <CheckCircle />;
      case SPJobStatus.FAILED:
        return <ErrorIcon />;
      case SPJobStatus.CANCELLED:
        return <Cancel />;
      default:
        return undefined;
    }
  };

  const getProgressValue = () => {
    if (!jobStatus || jobStatus.total_procedures === 0) return 0;
    return (jobStatus.completed_procedures / jobStatus.total_procedures) * 100;
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
            <Storage color="primary" />
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
        <Box sx={{ py: 1 }}>
          {!currentJobId ? (
            // Configuration Phase
            <Box>
              {hasRunningJob && (
                <Alert severity="warning" sx={{ mb: 2 }}>
                  There is already a stored procedure analysis job running. Please wait for it to complete before starting a new one.
                </Alert>
              )}

              <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
                Analyze stored procedures to extract table-column relationships using AI-powered analysis.
              </Typography>

              {/* Configuration Options */}
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 2 }}>
                <Settings fontSize="small" color="action" />
                <Typography variant="subtitle2">Analysis Configuration</Typography>
              </Box>

              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, mb: 3 }}>
                <FormControl size="small">
                  <InputLabel>Snowflake Environment</InputLabel>
                  <Select
                    value={sfEnvironment}
                    label="Snowflake Environment"
                    onChange={(e) => setSfEnvironment(e.target.value)}
                    disabled={!canStartNewAnalysis}
                  >
                    <MenuItem value="dev">Development</MenuItem>
                    <MenuItem value="stage">Staging</MenuItem>
                    <MenuItem value="prod">Production</MenuItem>
                  </Select>
                </FormControl>

                <TextField
                  label="Max Workers"
                  type="number"
                  size="small"
                  value={maxWorkers}
                  onChange={(e) => setMaxWorkers(Math.max(1, Math.min(10, parseInt(e.target.value) || 4)))}
                  disabled={!canStartNewAnalysis}
                  inputProps={{ min: 1, max: 10 }}
                  helperText="Number of parallel workers (1-10)"
                />

                <FormControlLabel
                  control={
                    <Switch
                      checked={resumeFromPartial}
                      onChange={(e) => setResumeFromPartial(e.target.checked)}
                      disabled={!canStartNewAnalysis}
                    />
                  }
                  label="Resume from partial results"
                />
              </Box>
              
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
                    {...(getStatusIcon(jobStatus?.status || SPJobStatus.PENDING) && { 
                      icon: getStatusIcon(jobStatus?.status || SPJobStatus.PENDING) 
                    })}
                    label={jobStatus?.status || 'Unknown'}
                    color={getStatusColor(jobStatus?.status || SPJobStatus.PENDING)}
                    size="small"
                  />
                </Box>

                <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                  {getStatusMessage()}
                </Typography>

                {jobStatus && (jobStatus.status === SPJobStatus.PENDING || 
                              jobStatus.status === SPJobStatus.RUNNING) && (
                  <Box>
                    <LinearProgress 
                      variant={jobStatus.total_procedures > 0 ? "determinate" : "indeterminate"}
                      value={getProgressValue()}
                      sx={{ mb: 2 }} 
                    />
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 1 }}>
                      <Typography variant="caption" color="text.secondary">
                        Started: {new Date(jobStatus.started_at).toLocaleString()}
                      </Typography>
                      {jobStatus.total_procedures > 0 && (
                        <Typography variant="caption" color="text.secondary">
                          Progress: {jobStatus.completed_procedures}/{jobStatus.total_procedures}
                        </Typography>
                      )}
                    </Box>
                  </Box>
                )}

                {isJobFailed && jobStatus?.error_message && (
                  <Alert severity="error" sx={{ mt: 2 }}>
                    {jobStatus.error_message}
                  </Alert>
                )}

                {isJobCompleted && (
                  <Alert severity="success" sx={{ mt: 2 }}>
                    Stored procedure analysis completed successfully! 
                    {jobStatus?.result_file && ` Result file: ${jobStatus.result_file}`}
                    <br />
                    Total procedures analyzed: {jobStatus?.total_procedures || 0}
                  </Alert>
                )}

                {isRedirecting && (
                  <Alert severity="info" sx={{ mt: 2 }}>
                    Analysis started successfully! Redirecting to SP Analysis Jobs section...
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

export default SPAnalysisDialog;