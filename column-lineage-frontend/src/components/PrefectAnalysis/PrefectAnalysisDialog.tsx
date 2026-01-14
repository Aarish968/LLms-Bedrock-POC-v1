/**
 * Prefect Analysis Dialog Component
 * Dialog for starting Prefect repository analysis with debug info
 * Matches the UI/UX of LineageAnalysisDialog
 */

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
  Paper,
  Alert,
} from '@mui/material';
import { PlayArrow } from '@mui/icons-material';
import { PrefectAnalysisService } from '../../api/prefectAnalysisService';
import { PrefectAnalysisRequest, PrefectAnalysisResponse } from '../../types/prefectAnalysis';

interface PrefectAnalysisDialogProps {
  open: boolean;
  onClose: () => void;
  onAnalysisStarted: (response: PrefectAnalysisResponse) => void;
}

// Debug Info Component - Matches LineageAnalysisDialog style
const DebugInfo: React.FC<{
  currentJobId: string | null;
  isAnalysisRunning: boolean;
  isJobRunning: boolean;
  isJobCompleted: boolean;
}> = ({ currentJobId, isAnalysisRunning, isJobRunning, isJobCompleted }) => {
  return (
    <Paper sx={{ p: 2, mb: 2, bgcolor: 'grey.50' }}>
      <Typography variant="h6" gutterBottom>
        Debug Info
      </Typography>
      
      <Box display="flex" flexWrap="wrap" gap={1}>
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
    </Paper>
  );
};

export const PrefectAnalysisDialog: React.FC<PrefectAnalysisDialogProps> = ({
  open,
  onClose,
  onAnalysisStarted,
}) => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [isRedirecting, setIsRedirecting] = useState(false);
  const [currentJobId, setCurrentJobId] = useState<string | null>(null);

  const handleStartAnalysis = async () => {
    try {
      setLoading(true);
      setError(null);
      
      // Use default configuration
      const request: PrefectAnalysisRequest = {
        sf_environment: 'prod',
        max_workers: 4,
        target_directory: 'prefect_repos',
        skip_discovery: false,
        skip_naming_check: false,
        clone_all_repos: false,
        async_processing: true,
      };
      
      const response = await PrefectAnalysisService.startAnalysis(request);
      setCurrentJobId(response.job_id);
      onAnalysisStarted(response);
      
      // Show redirect message
      setIsRedirecting(true);

      // Auto-close dialog after 2 seconds
      setTimeout(() => {
        setIsRedirecting(false);
        setCurrentJobId(null);
        setLoading(false);
        onClose();
      }, 2000);
    } catch (err: any) {
      setError(err.message || 'Failed to start analysis');
      setLoading(false);
    }
  };

  const handleClose = () => {
    if (!loading) {
      setError(null);
      setIsRedirecting(false);
      setCurrentJobId(null);
      onClose();
    }
  };

  return (
    <Dialog 
      open={open} 
      onClose={handleClose} 
      maxWidth="md" 
      fullWidth
      disableEscapeKeyDown={loading}
    >
      <DialogTitle>
        <Box display="flex" alignItems="center" justifyContent="space-between">
          <Typography variant="h6">Prefect Repository Analysis</Typography>
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
          isAnalysisRunning={loading}
          isJobRunning={loading}
          isJobCompleted={isRedirecting}
        />

        <Box>
          <Typography variant="body2" color="text.secondary" sx={{ mb: 3 }}>
            Start Prefect repository analysis to discover and analyze table-column references.
          </Typography>
          
          {error && (
            <Alert severity="error" sx={{ mb: 2 }}>
              {error}
            </Alert>
          )}
          
          {isRedirecting && (
            <Alert severity="info" sx={{ mt: 2 }}>
              Analysis started successfully! Redirecting to Prefect Analysis jobs section...
            </Alert>
          )}
        </Box>
      </DialogContent>

      <DialogActions>
        <Button 
          onClick={handleClose}
          disabled={loading}
        >
          {loading ? 'Running...' : 'Close'}
        </Button>

        <Button
          variant="contained"
          startIcon={<PlayArrow />}
          onClick={handleStartAnalysis}
          disabled={loading || isRedirecting}
        >
          {loading ? 'Starting...' : 'Start Analysis'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default PrefectAnalysisDialog;
