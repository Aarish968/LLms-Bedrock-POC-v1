/**
 * Prefect Analysis Jobs Component
 * Displays list of Prefect analysis jobs with smart polling
 */

import React from 'react';
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
} from '@mui/material';
import { 
  Download, 
  Cancel, 
  Refresh, 
  AccessTime,
  Visibility,
  VisibilityOff,
} from '@mui/icons-material';
import { usePrefectAnalysis } from '../../hooks/usePrefectAnalysis';
import { PrefectAnalysisService } from '../../api/prefectAnalysisService';
import { 
  PrefectAnalysisStatus, 
  PrefectAnalysisJob,
  getEstimatedCompletionTime,
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

  const getStatusColor = (status: PrefectAnalysisStatus) => {
    switch (status) {
      case PrefectAnalysisStatus.COMPLETED:
        return 'success';
      case PrefectAnalysisStatus.FAILED:
        return 'error';
      case PrefectAnalysisStatus.CANCELLED:
        return 'default';
      case PrefectAnalysisStatus.CLONING:
        return 'info';
      case PrefectAnalysisStatus.ANALYZING:
        return 'warning';
      default:
        return 'primary';
    }
  };

  const getStatusIcon = (job: PrefectAnalysisJob) => {
    const isRunning = [
      PrefectAnalysisStatus.PENDING,
      PrefectAnalysisStatus.CLONING,
      PrefectAnalysisStatus.ANALYZING,
    ].includes(job.status);

    if (!isRunning) return null;

    const skipDiscovery = job.request_params?.skip_discovery || false;
    const estimate = getEstimatedCompletionTime(job.status, job.started_at, skipDiscovery);
    
    return (
      <Tooltip title={estimate.message}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
          <CircularProgress size={16} />
          <Typography variant="caption" color="text.secondary">
            ~{estimate.minutes}m
          </Typography>
        </Box>
      </Tooltip>
    );
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
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                      <Chip 
                        label={job.status} 
                        color={getStatusColor(job.status)} 
                        size="small" 
                      />
                      {getStatusIcon(job)}
                    </Box>
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
    </Box>
  );
};

export default PrefectAnalysisJobs;
