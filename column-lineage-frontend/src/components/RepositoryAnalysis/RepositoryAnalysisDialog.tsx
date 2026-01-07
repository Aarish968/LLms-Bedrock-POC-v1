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
} from '@mui/material';
import {
  PlayArrow,
  Code,
  Storage,
  CheckCircle,
  Error as ErrorIcon,
  Cancel,
} from '@mui/icons-material';

import { RepositoryAnalysisService } from '../../api/repositoryAnalysisService';
import {
  RepositoryAnalysisResponse,
  AnalysisStatus,
} from '../../types/repositoryAnalysis';

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
  const [isStarting, setIsStarting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [analysisResponse, setAnalysisResponse] = useState<RepositoryAnalysisResponse | null>(null);

  const handleStartAnalysis = async () => {
    setIsStarting(true);
    setError(null);

    try {
      const response = await RepositoryAnalysisService.startAnalysis({
        async_processing: true,
      });

      setAnalysisResponse(response);
      
      // Notify parent component
      if (onAnalysisStarted) {
        onAnalysisStarted(response);
      }

      // Auto-close after 2 seconds on success
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

  const handleClose = () => {
    setError(null);
    setAnalysisResponse(null);
    onClose();
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
        return null;
    }
  };

  return (
    <Dialog
      open={open}
      onClose={handleClose}
      maxWidth="sm"
      fullWidth
      PaperProps={{
        sx: { borderRadius: 2 }
      }}
    >
      <DialogTitle>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
          <Code color="primary" />
          <Typography variant="h6">
            Repository Analysis
          </Typography>
        </Box>
      </DialogTitle>

      <DialogContent>
        <Box sx={{ py: 1 }}>
          {!analysisResponse && !error && (
            <>
              <Typography variant="body1" gutterBottom>
                Start analyzing your repository structure and dependencies.
              </Typography>
              
              <Box sx={{ mt: 2, mb: 2 }}>
                <Typography variant="body2" color="text.secondary" gutterBottom>
                  This analysis will:
                </Typography>
                <Box sx={{ ml: 2 }}>
                  <Typography variant="body2" color="text.secondary" sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                    <Storage fontSize="small" />
                    Clone and analyze repository structure
                  </Typography>
                  <Typography variant="body2" color="text.secondary" sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
                    <Code fontSize="small" />
                    Identify code dependencies and relationships
                  </Typography>
                  <Typography variant="body2" color="text.secondary" sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                    <CheckCircle fontSize="small" />
                    Generate comprehensive analysis report
                  </Typography>
                </Box>
              </Box>

              <Divider sx={{ my: 2 }} />

              <Typography variant="caption" color="text.secondary">
                The analysis will run in the background and you can monitor progress in the Analysis Jobs tab.
              </Typography>
            </>
          )}

          {analysisResponse && (
            <Box>
              <Alert severity="success" sx={{ mb: 2 }}>
                Repository analysis started successfully!
              </Alert>
              
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <Typography variant="body2" color="text.secondary">
                    Job ID:
                  </Typography>
                  <Typography variant="body2" fontFamily="monospace">
                    {analysisResponse.job_id}
                  </Typography>
                </Box>

                <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                  <Typography variant="body2" color="text.secondary">
                    Status:
                  </Typography>
                  <Chip
                    icon={getStatusIcon(analysisResponse.status)}
                    label={analysisResponse.status.toUpperCase()}
                    color={getStatusColor(analysisResponse.status)}
                    size="small"
                  />
                </Box>

                <Typography variant="body2" color="text.secondary">
                  {analysisResponse.message}
                </Typography>
              </Box>
            </Box>
          )}

          {error && (
            <Alert severity="error">
              <Typography variant="body2">
                {error}
              </Typography>
            </Alert>
          )}
        </Box>
      </DialogContent>

      <DialogActions sx={{ px: 3, pb: 2 }}>
        <Button onClick={handleClose} color="inherit">
          {analysisResponse ? 'Close' : 'Cancel'}
        </Button>
        
        {!analysisResponse && (
          <Button
            onClick={handleStartAnalysis}
            variant="contained"
            startIcon={isStarting ? <CircularProgress size={16} /> : <PlayArrow />}
            disabled={isStarting}
          >
            {isStarting ? 'Starting...' : 'Start Analysis'}
          </Button>
        )}
      </DialogActions>
    </Dialog>
  );
};

export default RepositoryAnalysisDialog;