/**
 * Prefect Analysis Dialog Component
 * Dialog for starting a new Prefect repository analysis
 */

import React, { useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  Button,
  TextField,
  FormControlLabel,
  Switch,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Box,
  Typography,
  Alert,
  Chip,
} from '@mui/material';
import { Info } from '@mui/icons-material';
import { PrefectAnalysisService } from '../../api/prefectAnalysisService';
import { PrefectAnalysisRequest, PrefectAnalysisResponse } from '../../types/prefectAnalysis';

interface PrefectAnalysisDialogProps {
  open: boolean;
  onClose: () => void;
  onAnalysisStarted: (response: PrefectAnalysisResponse) => void;
}

export const PrefectAnalysisDialog: React.FC<PrefectAnalysisDialogProps> = ({
  open,
  onClose,
  onAnalysisStarted,
}) => {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [formData, setFormData] = useState<PrefectAnalysisRequest>({
    sf_environment: 'prod',
    max_workers: 4,
    target_directory: 'prefect_repos',
    skip_discovery: false, // Comprehensive mode by default
    skip_naming_check: false,
    clone_all_repos: false,
    async_processing: true,
  });

  const handleSubmit = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await PrefectAnalysisService.startAnalysis(formData);
      onAnalysisStarted(response);
      onClose();
      
      // Reset form
      setFormData({
        sf_environment: 'prod',
        max_workers: 4,
        target_directory: 'prefect_repos',
        skip_discovery: false,
        skip_naming_check: false,
        clone_all_repos: false,
        async_processing: true,
      });
    } catch (err: any) {
      setError(err.message || 'Failed to start analysis');
    } finally {
      setLoading(false);
    }
  };

  const estimatedTime = formData.skip_discovery ? '~11 minutes' : '~20 minutes';
  const estimatedRepos = formData.skip_discovery ? '~20 repositories' : '~99 repositories';

  return (
    <Dialog open={open} onClose={onClose} maxWidth="sm" fullWidth>
      <DialogTitle>Start Prefect Repository Analysis</DialogTitle>
      <DialogContent>
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, mt: 2 }}>
          {error && <Alert severity="error">{error}</Alert>}
          
          <Alert severity="info" icon={<Info />}>
            This will discover Prefect repositories from AWS CodeCommit, clone them, and analyze table-column references.
          </Alert>

          <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
            <Chip 
              label={estimatedRepos} 
              color="primary" 
              size="small" 
            />
            <Chip 
              label={estimatedTime} 
              color="secondary" 
              size="small" 
            />
          </Box>
          
          <FormControl fullWidth>
            <InputLabel>Snowflake Environment</InputLabel>
            <Select
              value={formData.sf_environment}
              label="Snowflake Environment"
              onChange={(e) => setFormData({ ...formData, sf_environment: e.target.value })}
            >
              <MenuItem value="dev">Development</MenuItem>
              <MenuItem value="stage">Staging</MenuItem>
              <MenuItem value="prod">Production</MenuItem>
            </Select>
          </FormControl>

          <TextField
            label="Max Workers"
            type="number"
            value={formData.max_workers}
            onChange={(e) => setFormData({ ...formData, max_workers: parseInt(e.target.value) || 4 })}
            inputProps={{ min: 1, max: 10 }}
            helperText="Number of parallel workers for cloning and analysis (1-10)"
            fullWidth
          />

          <TextField
            label="Target Directory"
            value={formData.target_directory}
            onChange={(e) => setFormData({ ...formData, target_directory: e.target.value })}
            helperText="Directory where repositories will be cloned"
            fullWidth
          />

          <Box sx={{ border: '1px solid #e0e0e0', borderRadius: 1, p: 2 }}>
            <Typography variant="subtitle2" gutterBottom>
              Discovery Options
            </Typography>

            <FormControlLabel
              control={
                <Switch
                  checked={!formData.skip_discovery}
                  onChange={(e) => setFormData({ ...formData, skip_discovery: !e.target.checked })}
                />
              }
              label={
                <Box>
                  <Typography variant="body2">
                    Content-Based Discovery (Recommended)
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    Checks all repos for Prefect patterns. Finds ~99 repos but takes longer.
                  </Typography>
                </Box>
              }
            />

            <FormControlLabel
              control={
                <Switch
                  checked={formData.skip_naming_check}
                  onChange={(e) => setFormData({ ...formData, skip_naming_check: e.target.checked })}
                />
              }
              label={
                <Box>
                  <Typography variant="body2">
                    Skip Naming Pattern Check
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    Skip filtering by repository name patterns (flow-*, *-flows, etc.)
                  </Typography>
                </Box>
              }
            />

            <FormControlLabel
              control={
                <Switch
                  checked={formData.clone_all_repos}
                  onChange={(e) => setFormData({ ...formData, clone_all_repos: e.target.checked })}
                />
              }
              label={
                <Box>
                  <Typography variant="body2">
                    Clone All Repositories
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    Clone ALL repositories regardless of Prefect patterns (slowest but most comprehensive)
                  </Typography>
                </Box>
              }
            />
          </Box>

          <Alert severity="warning" sx={{ mt: 1 }}>
            <Typography variant="body2">
              <strong>Note:</strong> This is a long-running operation. You can close this dialog and check progress in the jobs list.
              The system uses smart polling to minimize backend load.
            </Typography>
          </Alert>
        </Box>
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose} disabled={loading}>
          Cancel
        </Button>
        <Button 
          onClick={handleSubmit} 
          variant="contained" 
          disabled={loading}
        >
          {loading ? 'Starting...' : 'Start Analysis'}
        </Button>
      </DialogActions>
    </Dialog>
  );
};

export default PrefectAnalysisDialog;
