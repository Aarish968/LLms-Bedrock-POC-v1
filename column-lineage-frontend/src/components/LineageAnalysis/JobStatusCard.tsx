import React, { useState } from 'react';
import {
  Card,
  CardContent,
  Box,
  Typography,
  Chip,
  LinearProgress,
  Button,
  Alert,
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Paper,
} from '@mui/material';
import { Visibility, Refresh, Cancel, ContentCopy } from '@mui/icons-material';

interface JobStatusCardProps {
  jobId: string;
  status: string;
  totalViews: number;
  processedViews: number;
  resultsCount: number;
  createdAt: string;
  startedAt?: string;
  completedAt?: string;
  successfulViews?: number;
  failedViews?: number;
  requestParams?: any;
  errorMessage?: string;
  onViewResults: () => void;
  onRefresh: () => void;
  onCancel?: () => void;
  isLoading?: boolean;
}

const JobStatusCard: React.FC<JobStatusCardProps> = ({
  jobId,
  status,
  totalViews,
  processedViews,
  resultsCount,
  createdAt,
  startedAt,
  completedAt,
  successfulViews = 0,
  failedViews = 0,
  requestParams,
  errorMessage,
  onViewResults,
  onRefresh,
  onCancel,
  isLoading = false,
}) => {
  const [showJobData, setShowJobData] = useState(false);
  const getStatusColor = (status: string) => {
    switch (status) {
      case 'PENDING':
        return 'warning';
      case 'RUNNING':
        return 'info';
      case 'COMPLETED':
        return 'success';
      case 'FAILED':
        return 'error';
      case 'CANCELLED':
        return 'default';
      default:
        return 'default';
    }
  };

  const getProgressValue = () => {
    if (totalViews === 0) return 0;
    return (processedViews / totalViews) * 100;
  };

  const isJobRunning = status === 'PENDING' || status === 'RUNNING';
  const isJobCompleted = status === 'COMPLETED';
  const isJobFailed = status === 'FAILED';

  const handleViewResults = () => {
    setShowJobData(true);
  };

  const handleCloseJobData = () => {
    setShowJobData(false);
  };

  const getJobDataAsJson = () => {
    // Filter out unwanted fields from request_params
    const filteredRequestParams = { ...requestParams };
    if (filteredRequestParams) {
      delete filteredRequestParams.view_names;
      delete filteredRequestParams.include_system_views;
      delete filteredRequestParams.max_views;
      delete filteredRequestParams.async_processing;
      delete filteredRequestParams.include_metadata;
    }

    return {
      job_id: jobId,
      status: status,
      created_at: createdAt,
      started_at: startedAt || null,
      completed_at: completedAt || null,
      total_views: totalViews,
      processed_views: processedViews,
      successful_views: successfulViews,
      failed_views: failedViews,
      error_message: errorMessage || null,
      results_count: resultsCount,
      request_params: filteredRequestParams || {}
    };
  };

  const copyToClipboard = () => {
    const jsonData = JSON.stringify(getJobDataAsJson(), null, 2);
    navigator.clipboard.writeText(jsonData);
  };

  return (
    <Card variant="outlined" sx={{ mb: 2 }}>
      <CardContent>
        <Box display="flex" alignItems="center" justifyContent="space-between" sx={{ mb: 2 }}>
          <Typography variant="h6" component="div">
            Job: {jobId.slice(0, 8)}...
          </Typography>
          <Chip
            label={status}
            color={getStatusColor(status)}
            size="small"
          />
        </Box>

        <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
          Started: {new Date(createdAt).toLocaleString()}
        </Typography>

        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
          Progress: {processedViews} / {totalViews} views processed
        </Typography>

        {totalViews > 0 && (
          <LinearProgress
            variant="determinate"
            value={getProgressValue()}
            sx={{ mb: 2 }}
          />
        )}

        {isJobFailed && errorMessage && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {errorMessage}
          </Alert>
        )}

        {isJobCompleted && (
          <Alert severity="success" sx={{ mb: 2 }}>
            Analysis completed! Found {resultsCount} lineage relationships.
          </Alert>
        )}

        <Box display="flex" gap={1}>
          {isJobCompleted && (
            <Button
              variant="contained"
              startIcon={<Visibility />}
              onClick={handleViewResults}
              size="small"
            >
              View Results
            </Button>
          )}
          
          <Button
            variant="outlined"
            startIcon={<Refresh />}
            onClick={onRefresh}
            disabled={isLoading}
            size="small"
          >
            Refresh
          </Button>

          {isJobRunning && onCancel && (
            <Button
              variant="outlined"
              color="error"
              startIcon={<Cancel />}
              onClick={onCancel}
              size="small"
            >
              Cancel
            </Button>
          )}
        </Box>
      </CardContent>

      {/* Job Data Dialog */}
      <Dialog
        open={showJobData}
        onClose={handleCloseJobData}
        maxWidth="md"
        fullWidth
      >
        <DialogTitle>
          Job Data - {jobId.slice(0, 8)}...
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
              {JSON.stringify(getJobDataAsJson(), null, 2)}
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
    </Card>
  );
};

export default JobStatusCard;