import React, { useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  Box,
  Typography,
  Chip,
  LinearProgress,
  Alert,
  Divider,
} from '@mui/material';
import { PlayArrow, Visibility, Refresh } from '@mui/icons-material';
import { useLineageWorkflow } from '@/hooks/lineage/useLineageAnalysis';
import DebugInfo from './DebugInfo';

interface LineageAnalysisDialogProps {
  open: boolean;
  onClose: () => void;
  onAnalysisStarted?: () => void;
}

const LineageAnalysisDialog: React.FC<LineageAnalysisDialogProps> = ({ open, onClose, onAnalysisStarted }) => {
  const {
    currentJobId,
    showResults,
    jobStatus,
    results,
    resultsLoading,
    startAnalysis,
    viewResults,
    resetWorkflow,
    isAnalysisRunning,
    isJobRunning,
    isJobCompleted,
    isJobFailed,
    canStartNewAnalysis,
  } = useLineageWorkflow();

  const handleStartAnalysis = async () => {
    await startAnalysis({
      async_processing: true,
      include_metadata: true,
    });
    
    // Notify parent component that analysis has started
    if (onAnalysisStarted) {
      onAnalysisStarted();
    }
  };

  const handleClose = () => {
    // Only allow closing if no job is running
    if (isJobRunning) {
      // Don't close, but could show a warning
      return;
    }
    resetWorkflow();
    onClose();
  };

  const handleNewAnalysis = () => {
    resetWorkflow();
  };

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
    if (!jobStatus) return 0;
    if (jobStatus.total_views === 0) return 0;
    return (jobStatus.processed_views / jobStatus.total_views) * 100;
  };

  const getStatusMessage = () => {
    if (!jobStatus) return '';
    
    switch (jobStatus.status) {
      case 'PENDING':
        return 'Analysis is starting...';
      case 'RUNNING':
        return `Processing views: ${jobStatus.processed_views} / ${jobStatus.total_views}`;
      case 'COMPLETED':
        return `Analysis completed! Found ${jobStatus.results_count} lineage relationships.`;
      case 'FAILED':
        return 'Analysis failed. Please check the error message below.';
      default:
        return '';
    }
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
          <Typography variant="h6">Lineage Analysis</Typography>
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
        {/* Debug Info - Remove this in production */}
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
              Start column lineage analysis for your database views.
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
                  label={jobStatus?.status || 'Unknown'}
                  color={getStatusColor(jobStatus?.status || '')}
                  size="small"
                />
              </Box>

              <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                {getStatusMessage()}
              </Typography>

              {jobStatus && (jobStatus.status === 'PENDING' || jobStatus.status === 'RUNNING') && (
                <Box>
                  <LinearProgress
                    variant={jobStatus.total_views > 0 ? "determinate" : "indeterminate"}
                    value={getProgressValue()}
                    sx={{ mb: 2 }}
                  />
                  <Typography variant="caption" color="text.secondary">
                    Started: {new Date(jobStatus.created_at).toLocaleString()}
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
                  Analysis completed successfully! Found {jobStatus?.results_count} lineage relationships.
                </Alert>
              )}
            </Box>

            {/* Results Preview */}
            {showResults && results && (
              <Box>
                <Divider sx={{ my: 2 }} />
                <Typography variant="subtitle1" gutterBottom>
                  Analysis Results
                </Typography>
                <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                  Total Results: {results.total_results}
                </Typography>
                
                {results.results.slice(0, 5).map((result, index) => (
                  <Box key={index} sx={{ mb: 1, p: 1, bgcolor: 'grey.50', borderRadius: 1 }}>
                    <Typography variant="body2">
                      <strong>{result.view_name}.{result.view_column}</strong> ← {result.source_table}.{result.source_column}
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      Confidence: {(result.confidence_score * 100).toFixed(1)}%
                    </Typography>
                  </Box>
                ))}
                
                {results.results.length > 5 && (
                  <Typography variant="caption" color="text.secondary">
                    ... and {results.results.length - 5} more results
                  </Typography>
                )}
              </Box>
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
          <Box>
            {isJobCompleted && !showResults && (
              <Button
                variant="contained"
                startIcon={<Visibility />}
                onClick={viewResults}
                disabled={resultsLoading}
                sx={{ mr: 1 }}
              >
                {resultsLoading ? 'Loading...' : 'View Results'}
              </Button>
            )}
            
            <Button
              variant="outlined"
              startIcon={<Refresh />}
              onClick={handleNewAnalysis}
              disabled={isJobRunning}
            >
              New Analysis
            </Button>
          </Box>
        )}
      </DialogActions>
    </Dialog>
  );
};

export default LineageAnalysisDialog;