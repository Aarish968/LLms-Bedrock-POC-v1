/**
 * Prefect Analysis Jobs Component
 * Displays list of Prefect analysis jobs with smart polling
 */

import React, { useState } from 'react';
import {
  Box,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Chip,
  IconButton,
  Button,
  Typography,
  CircularProgress,
  Tooltip,
  LinearProgress,
  Alert,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
} from '@mui/material';
import { 
  Download, 
  Cancel, 
  Refresh, 
  AccessTime,
  Visibility,
  VisibilityOff,
  CheckCircle,
  Schedule,
  Error as ErrorIcon,
} from '@mui/icons-material';
import { usePrefectAnalysis } from '../../hooks/usePrefectAnalysis';
import { PrefectAnalysisService } from '../../api/prefectAnalysisService';
import { 
  PrefectAnalysisStatus, 
  PrefectAnalysisJob,
  PrefectAnalysisResults,
  getPollingInterval,
} from '../../types/prefectAnalysis';

interface PrefectAnalysisJobsProps {
  onNewAnalysis: () => void;
}

export const PrefectAnalysisJobs: React.FC<PrefectAnalysisJobsProps> = ({ onNewAnalysis }) => {
  const { 
    jobs, 
    loading, 
    error, 
    hasRunningJob, 
    lastUpdated,
    isTabVisible,
    refresh,
    cancelJob 
  } = usePrefectAnalysis();

  const [jobResults, setJobResults] = useState<PrefectAnalysisResults | null>(null);
  const [resultsDialogOpen, setResultsDialogOpen] = useState(false);
  const [loadingResults, setLoadingResults] = useState(false);

  const getStatusColor = (status: PrefectAnalysisStatus) => {
    switch (status) {
      case PrefectAnalysisStatus.PENDING:
        return 'warning'; // Orange color for PENDING
      case PrefectAnalysisStatus.COMPLETED:
        return 'success';
      case PrefectAnalysisStatus.FAILED:
        return 'error';
      case PrefectAnalysisStatus.CANCELLED:
        return 'default';
      case PrefectAnalysisStatus.CLONING:
        return 'info';
      case PrefectAnalysisStatus.ANALYZING:
        return 'info';
      default:
        return 'primary';
    }
  };

  const getStatusIcon = (status: PrefectAnalysisStatus) => {
    switch (status) {
      case PrefectAnalysisStatus.PENDING:
        return <Schedule />;
      case PrefectAnalysisStatus.CLONING:
        return <CircularProgress size={16} />;
      case PrefectAnalysisStatus.ANALYZING:
        return <CircularProgress size={16} />;
      case PrefectAnalysisStatus.COMPLETED:
        return <CheckCircle />;
      case PrefectAnalysisStatus.FAILED:
        return <ErrorIcon />;
      case PrefectAnalysisStatus.CANCELLED:
        return <Cancel />;
      default:
        return undefined;
    }
  };

  const getProgressInfo = (job: PrefectAnalysisJob) => {
    if (job.status === PrefectAnalysisStatus.CLONING && job.repos_cloned > 0) {
      const total = job.total_repos_found || 99;
      const progress = (job.repos_cloned / total) * 100;
      return (
        <Box sx={{ width: '100%', mt: 0.5 }}>
          <LinearProgress variant="determinate" value={progress} />
          <Typography variant="caption" color="text.secondary">
            {job.repos_cloned}/{total} repos
          </Typography>
        </Box>
      );
    }
    return null;
  };

  const formatElapsedTime = (startedAt: string) => {
    const elapsed = Math.floor((Date.now() - new Date(startedAt).getTime()) / 60000);
    if (elapsed < 1) return 'Just now';
    if (elapsed === 1) return '1 minute ago';
    if (elapsed < 60) return `${elapsed} minutes ago`;
    const hours = Math.floor(elapsed / 60);
    return hours === 1 ? '1 hour ago' : `${hours} hours ago`;
  };

  const handleDownload = async (jobId: string) => {
    try {
      await PrefectAnalysisService.downloadResults(jobId);
    } catch (err) {
      console.error('Download failed:', err);
    }
  };

  const handleCancel = async (jobId: string) => {
    if (window.confirm('Are you sure you want to cancel this job? This cannot be undone.')) {
      try {
        await cancelJob(jobId);
      } catch (err) {
        console.error('Cancel failed:', err);
      }
    }
  };

  const handleViewResults = async (job: PrefectAnalysisJob) => {
    if (job.status !== PrefectAnalysisStatus.COMPLETED) {
      return;
    }

    setLoadingResults(true);
    setResultsDialogOpen(true);

    try {
      const results = await PrefectAnalysisService.getResults(job.job_id);
      setJobResults(results);
    } catch (err: any) {
      setJobResults(null);
      console.error('Failed to load results:', err);
    } finally {
      setLoadingResults(false);
    }
  };

  const handleCloseResultsDialog = () => {
    setResultsDialogOpen(false);
    setJobResults(null);
  };

  const formatFileSize = (bytes: number) => {
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    if (bytes === 0) return '0 Bytes';
    const i = Math.floor(Math.log(bytes) / Math.log(1024));
    return Math.round(bytes / Math.pow(1024, i) * 100) / 100 + ' ' + sizes[i];
  };

  const getPollingInfo = () => {
    if (!hasRunningJob) return null;

    const runningJobs = jobs.filter(
      job => job.status === PrefectAnalysisStatus.PENDING ||
             job.status === PrefectAnalysisStatus.CLONING ||
             job.status === PrefectAnalysisStatus.ANALYZING
    );

    if (runningJobs.length === 0) return null;

    const intervals = runningJobs
      .map(job => getPollingInterval(job.status))
      .filter((interval): interval is number => interval !== null);

    if (intervals.length === 0) return null;

    const shortestInterval = Math.min(...intervals);
    const seconds = shortestInterval / 1000;

    return (
      <Alert 
        severity="info" 
        icon={isTabVisible ? <Visibility /> : <VisibilityOff />}
        sx={{ mb: 2 }}
      >
        <Typography variant="body2">
          {isTabVisible 
            ? `Auto-refreshing every ${seconds} seconds while jobs are running`
            : 'Auto-refresh paused (tab not visible)'}
        </Typography>
      </Alert>
    );
  };

  return (
    <Box>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
        <Box>
          <Typography variant="h6">Prefect Analysis Jobs</Typography>
          {lastUpdated && (
            <Typography variant="caption" color="text.secondary">
              Last updated: {lastUpdated.toLocaleTimeString()}
            </Typography>
          )}
        </Box>
        <Box sx={{ display: 'flex', gap: 1 }}>
          <Tooltip title="Refresh jobs list">
            <IconButton onClick={refresh} disabled={loading} color="primary">
              <Refresh />
            </IconButton>
          </Tooltip>
          <Button 
            variant="contained" 
            onClick={onNewAnalysis} 
            disabled={hasRunningJob}
          >
            New Analysis
          </Button>
        </Box>
      </Box>

      {getPollingInfo()}

      {error && (
        <Alert severity="error" sx={{ mb: 2 }}>
          {error}
        </Alert>
      )}

      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Job ID</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Environment</TableCell>
              <TableCell>Progress</TableCell>
              <TableCell>References</TableCell>
              <TableCell>Started</TableCell>
              <TableCell>Actions</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {loading && jobs.length === 0 ? (
              <TableRow>
                <TableCell colSpan={7} align="center">
                  <CircularProgress />
                </TableCell>
              </TableRow>
            ) : jobs.length === 0 ? (
              <TableRow>
                <TableCell colSpan={7} align="center">
                  <Box sx={{ py: 4 }}>
                    <Typography variant="body1" color="text.secondary" gutterBottom>
                      No Prefect analysis jobs found
                    </Typography>
                    <Button 
                      variant="outlined" 
                      onClick={onNewAnalysis}
                      sx={{ mt: 2 }}
                    >
                      Start Your First Analysis
                    </Button>
                  </Box>
                </TableCell>
              </TableRow>
            ) : (
              jobs.map((job) => (
                <TableRow key={job.job_id} hover>
                  <TableCell>
                    <Tooltip title={job.job_id}>
                      <Typography variant="body2" sx={{ fontFamily: 'monospace' }}>
                        {job.job_id.substring(0, 8)}...
                      </Typography>
                    </Tooltip>
                  </TableCell>
                  <TableCell>
                    <Chip 
                      {...(getStatusIcon(job.status) && { icon: getStatusIcon(job.status) })}
                      label={job.status} 
                      color={getStatusColor(job.status)} 
                      size="small"
                    />
                  </TableCell>
                  <TableCell>
                    <Chip label={job.sf_environment} size="small" variant="outlined" />
                  </TableCell>
                  <TableCell>
                    <Box>
                      <Typography variant="body2">
                        {job.total_repos_found > 0 && `${job.total_repos_found} repos found`}
                        {job.repos_cloned > 0 && ` • ${job.repos_cloned} cloned`}
                      </Typography>
                      {getProgressInfo(job)}
                    </Box>
                  </TableCell>
                  <TableCell>
                    {job.total_references > 0 ? (
                      <Box>
                        <Typography variant="body2">
                          {job.total_references.toLocaleString()} refs
                        </Typography>
                        <Typography variant="caption" color="text.secondary">
                          {job.unique_tables} tables
                        </Typography>
                      </Box>
                    ) : (
                      <Typography variant="body2" color="text.secondary">
                        -
                      </Typography>
                    )}
                  </TableCell>
                  <TableCell>
                    <Tooltip title={new Date(job.started_at).toLocaleString()}>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
                        <AccessTime fontSize="small" color="action" />
                        <Typography variant="body2">
                          {formatElapsedTime(job.started_at)}
                        </Typography>
                      </Box>
                    </Tooltip>
                  </TableCell>
                  <TableCell>
                    <Box sx={{ display: 'flex', gap: 0.5 }}>
                      {job.status === PrefectAnalysisStatus.COMPLETED && (
                        <Tooltip title="View Results">
                          <IconButton 
                            onClick={() => handleViewResults(job)} 
                            size="small"
                            color="primary"
                          >
                            <Visibility />
                          </IconButton>
                        </Tooltip>
                      )}
                      {job.status === PrefectAnalysisStatus.COMPLETED && job.output_file && (
                        <Tooltip title="Download results CSV">
                          <IconButton 
                            onClick={() => handleDownload(job.job_id)} 
                            size="small"
                            color="primary"
                          >
                            <Download />
                          </IconButton>
                        </Tooltip>
                      )}
                      {(job.status === PrefectAnalysisStatus.PENDING ||
                        job.status === PrefectAnalysisStatus.CLONING ||
                        job.status === PrefectAnalysisStatus.ANALYZING) && (
                        <Tooltip title="Cancel job">
                          <IconButton 
                            onClick={() => handleCancel(job.job_id)} 
                            size="small"
                            color="error"
                          >
                            <Cancel />
                          </IconButton>
                        </Tooltip>
                      )}
                    </Box>
                  </TableCell>
                </TableRow>
              ))
            )}
          </TableBody>
        </Table>
      </TableContainer>

      {jobs.length > 0 && (
        <Box sx={{ mt: 2, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="caption" color="text.secondary">
            Showing {jobs.length} job{jobs.length !== 1 ? 's' : ''}
          </Typography>
          {hasRunningJob && (
            <Typography variant="caption" color="text.secondary">
              Smart polling active • Minimal backend load
            </Typography>
          )}
        </Box>
      )}

      {/* Results Dialog */}
      <Dialog
        open={resultsDialogOpen}
        onClose={handleCloseResultsDialog}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          <Typography variant="h6">
            Prefect Analysis Results
          </Typography>
        </DialogTitle>
        
        <DialogContent>
          {loadingResults ? (
            <Box sx={{ display: 'flex', justifyContent: 'center', py: 4 }}>
              <CircularProgress />
            </Box>
          ) : jobResults ? (
            <Box sx={{ py: 1 }}>
              <Typography variant="body1" gutterBottom>
                <strong>Job ID:</strong> {jobResults.job_id}
              </Typography>
              <Typography variant="body1" gutterBottom>
                <strong>Status:</strong>{' '}
                <Chip
                  label={jobResults.status.toUpperCase()}
                  color="success"
                  size="small"
                />
              </Typography>
              <Typography variant="body1" gutterBottom>
                <strong>Total References:</strong> {jobResults.total_references.toLocaleString()}
              </Typography>
              <Typography variant="body1" gutterBottom>
                <strong>Unique Tables:</strong> {jobResults.unique_tables}
              </Typography>
              <Typography variant="body1" gutterBottom>
                <strong>Unique Repos:</strong> {jobResults.unique_repos}
              </Typography>
              <Typography variant="body1" gutterBottom>
                <strong>Unique Functions:</strong> {jobResults.unique_functions}
              </Typography>
              <Typography variant="body1" gutterBottom>
                <strong>Output File:</strong> {jobResults.output_file}
              </Typography>
              <Typography variant="body1" gutterBottom>
                <strong>File Size:</strong> {formatFileSize(jobResults.file_size)}
              </Typography>
              <Typography variant="body1" gutterBottom>
                <strong>Created:</strong> {new Date(jobResults.created_at).toLocaleString()}
              </Typography>
              
              {jobResults.sample_references && jobResults.sample_references.length > 0 && (
                <Box sx={{ mt: 2 }}>
                  <Typography variant="subtitle2" gutterBottom>
                    Sample References:
                  </Typography>
                  {jobResults.sample_references.slice(0, 5).map((ref, index) => (
                    <Box key={index} sx={{ mb: 1, p: 1, bgcolor: 'grey.50', borderRadius: 1 }}>
                      <Typography variant="body2">
                        <strong>{ref.table_name}.{ref.column_name}</strong>
                      </Typography>
                      <Typography variant="caption" color="text.secondary">
                        Repo: {ref.repo_name} • Function: {ref.function_name}
                      </Typography>
                    </Box>
                  ))}
                </Box>
              )}
            </Box>
          ) : (
            <Alert severity="error">
              Failed to load analysis results.
            </Alert>
          )}
        </DialogContent>
        
        <DialogActions>
          <Button onClick={handleCloseResultsDialog}>
            Close
          </Button>
          {jobResults && jobResults.output_file && (
            <Button 
              variant="contained" 
              onClick={() => handleDownload(jobResults.job_id)}
              startIcon={<Download />}
            >
              Download CSV
            </Button>
          )}
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default PrefectAnalysisJobs;
