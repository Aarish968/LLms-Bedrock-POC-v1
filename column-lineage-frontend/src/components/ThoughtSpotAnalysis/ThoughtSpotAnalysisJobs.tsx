/**
 * ThoughtSpot Analysis Jobs Component
 * Displays list of ThoughtSpot analysis jobs with smart polling
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
  Refresh, 
  AccessTime,
  Visibility,
  VisibilityOff,
  CheckCircle,
  Schedule,
  Error as ErrorIcon,
  ContentCopy,
} from '@mui/icons-material';
import { useThoughtSpotAnalysis } from '../../hooks/useThoughtSpotAnalysis';
import { ThoughtSpotAnalysisService } from '../../api/thoughtspotAnalysisService';
import { 
  TSAnalysisStatus, 
  TSAnalysisJob,
  getPollingInterval,
} from '../../types/thoughtspotAnalysis';

interface ThoughtSpotAnalysisJobsProps {
  onNewAnalysis: () => void;
}

export const ThoughtSpotAnalysisJobs: React.FC<ThoughtSpotAnalysisJobsProps> = ({ onNewAnalysis }) => {
  const { 
    jobs, 
    loading, 
    error, 
    hasRunningJob, 
    lastUpdated,
    isTabVisible,
    refresh,
  } = useThoughtSpotAnalysis();

  const [selectedJobForData, setSelectedJobForData] = useState<TSAnalysisJob | null>(null);
  const [showJobData, setShowJobData] = useState(false);

  const getStatusColor = (status: TSAnalysisStatus) => {
    switch (status) {
      case TSAnalysisStatus.PENDING:
        return 'warning'; // Orange color for PENDING
      case TSAnalysisStatus.COMPLETED:
        return 'success';
      case TSAnalysisStatus.FAILED:
        return 'error';
      case TSAnalysisStatus.CANCELLED:
        return 'default';
      case TSAnalysisStatus.RUNNING:
        return 'info';
      default:
        return 'primary';
    }
  };

  const getStatusIcon = (status: TSAnalysisStatus) => {
    switch (status) {
      case TSAnalysisStatus.PENDING:
        return <Schedule />;
      case TSAnalysisStatus.RUNNING:
        return <CircularProgress size={16} />;
      case TSAnalysisStatus.COMPLETED:
        return <CheckCircle />;
      case TSAnalysisStatus.FAILED:
        return <ErrorIcon />;
      default:
        return undefined;
    }
  };

  const getProgressInfo = (job: TSAnalysisJob) => {
    if (job.status === TSAnalysisStatus.RUNNING && job.processed_tables > 0) {
      const total = job.total_tables || 150;
      const progress = (job.processed_tables / total) * 100;
      return (
        <Box sx={{ width: '100%', mt: 0.5 }}>
          <LinearProgress variant="determinate" value={progress} />
          <Typography variant="caption" color="text.secondary">
            {job.processed_tables}/{total} tables
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
      await ThoughtSpotAnalysisService.downloadResults(jobId);
    } catch (err) {
      console.error('Download failed:', err);
    }
  };

  const handleViewResults = async (job: TSAnalysisJob) => {
    if (job.status !== TSAnalysisStatus.COMPLETED) {
      return;
    }

    // Show job data dialog
    setSelectedJobForData(job);
    setShowJobData(true);
  };

  const handleCloseJobData = () => {
    setShowJobData(false);
    setSelectedJobForData(null);
  };

  const getJobDataAsJson = (job: TSAnalysisJob) => {
    return {
      job_id: job.job_id,
      status: job.status,
      message: job.message || null,
      started_at: job.started_at,
      completed_at: job.completed_at || null,
      sf_environment: job.sf_environment,
      max_workers: job.max_workers,
      include_views: job.include_views,
      force_prod_urls: job.force_prod_urls,
      table_pattern: job.table_pattern || null,
      total_tables: job.total_tables,
      processed_tables: job.processed_tables,
      total_relationships: job.total_relationships,
      result_file: job.result_file || null,
      error_message: job.error_message || null,
      request_params: job.request_params || {},
    };
  };

  const copyToClipboard = () => {
    if (selectedJobForData) {
      const jsonData = JSON.stringify(getJobDataAsJson(selectedJobForData), null, 2);
      navigator.clipboard.writeText(jsonData);
    }
  };

  const getPollingInfo = () => {
    if (!hasRunningJob) return null;

    const runningJobs = jobs.filter(
      job => job.status === TSAnalysisStatus.PENDING ||
             job.status === TSAnalysisStatus.RUNNING
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
          <Typography variant="h6">ThoughtSpot Analysis Jobs</Typography>
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
              <TableCell>Relationships</TableCell>
              <TableCell>Started</TableCell>
              <TableCell align="right">Actions</TableCell>
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
                      No ThoughtSpot analysis jobs found
                    </Typography>
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
                        {job.total_tables > 0 && `${job.total_tables} tables`}
                        {job.processed_tables > 0 && ` • ${job.processed_tables} processed`}
                      </Typography>
                      {getProgressInfo(job)}
                    </Box>
                  </TableCell>
                  <TableCell>
                    {job.total_relationships > 0 ? (
                      <Box>
                        <Typography variant="body2">
                          {job.total_relationships.toLocaleString()} relationships
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
                  <TableCell align="right">
                    <Box sx={{ display: 'flex', gap: 0.5, justifyContent: 'flex-end' }}>
                      {job.status === TSAnalysisStatus.COMPLETED && (
                        <>
                          <Tooltip title="View Job Data">
                            <IconButton 
                              onClick={() => handleViewResults(job)} 
                              size="small"
                            >
                              <Visibility />
                            </IconButton>
                          </Tooltip>
                          {job.result_file && (
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
                        </>
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

      {/* Job Data Dialog */}
      <Dialog
        open={showJobData}
        onClose={handleCloseJobData}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          Job Data - {selectedJobForData?.job_id.slice(0, 8)}...
        </DialogTitle>
        <DialogContent>
          <Paper
            elevation={0}
            sx={{
              p: 2,
              backgroundColor: 'grey.50',
              border: '1px solid',
              borderColor: 'grey.300',
              borderRadius: 1,
              fontFamily: 'monospace',
              fontSize: '0.875rem',
              overflow: 'auto',
              maxHeight: '400px'
            }}
          >
            <pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>
              {selectedJobForData ? JSON.stringify(getJobDataAsJson(selectedJobForData), null, 2) : ''}
            </pre>
          </Paper>
        </DialogContent>
        <DialogActions>
          <Button
            startIcon={<ContentCopy />}
            onClick={copyToClipboard}
            variant="outlined"
          >
            Copy JSON
          </Button>
          <Button onClick={handleCloseJobData}>
            Close
          </Button>
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default ThoughtSpotAnalysisJobs;
