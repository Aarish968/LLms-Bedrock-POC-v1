import React, { useState } from 'react';
import {
  Dialog,
  DialogTitle,
  DialogContent,
  DialogActions,
  TextField,
  Button,
  Box,
  Alert,
  CircularProgress,
} from '@mui/material';
import { useCreateBaseView } from '../../hooks/useBaseView';

interface AddBaseViewModalProps {
  open: boolean;
  onClose: () => void;
}

export const AddBaseViewModal: React.FC<AddBaseViewModalProps> = ({ open, onClose }) => {
  const [basePrimaryId, setBasePrimaryId] = useState<string>('');
  const [tableName, setTableName] = useState<string>('');
  const [errors, setErrors] = useState<{ basePrimaryId?: string; tableName?: string }>({});

  const createMutation = useCreateBaseView();

  const validateForm = () => {
    const newErrors: { basePrimaryId?: string; tableName?: string } = {};

    if (!basePrimaryId.trim()) {
      newErrors.basePrimaryId = 'Primary ID is required';
    } else if (isNaN(Number(basePrimaryId)) || Number(basePrimaryId) <= 0) {
      newErrors.basePrimaryId = 'Primary ID must be a positive number';
    }

    if (!tableName.trim()) {
      newErrors.tableName = 'Table name is required';
    } else if (tableName.length > 255) {
      newErrors.tableName = 'Table name must be less than 255 characters';
    }

    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!validateForm()) {
      return;
    }

    try {
      await createMutation.mutateAsync({
        base_primary_id: Number(basePrimaryId),
        table_name: tableName.trim(),
      });

      // Reset form and close modal on success
      setBasePrimaryId('');
      setTableName('');
      setErrors({});
      onClose();
    } catch (error) {
      // Error is handled by the mutation hook
      console.error('Create failed:', error);
    }
  };

  const handleClose = () => {
    if (!createMutation.isPending) {
      setBasePrimaryId('');
      setTableName('');
      setErrors({});
      createMutation.reset();
      onClose();
    }
  };

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
      <form onSubmit={handleSubmit}>
        <DialogTitle>Add New Record</DialogTitle>
        
        <DialogContent>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, pt: 1 }}>
            {createMutation.error && (
              <Alert severity="error">
                {createMutation.error.message}
              </Alert>
            )}

            <TextField
              label="Primary ID"
              type="number"
              value={basePrimaryId}
              onChange={(e) => setBasePrimaryId(e.target.value)}
              error={!!errors.basePrimaryId}
              helperText={errors.basePrimaryId}
              disabled={createMutation.isPending}
              required
              fullWidth
              inputProps={{ min: 1 }}
            />

            <TextField
              label="Table Name"
              value={tableName}
              onChange={(e) => setTableName(e.target.value)}
              error={!!errors.tableName}
              helperText={errors.tableName}
              disabled={createMutation.isPending}
              required
              fullWidth
              inputProps={{ maxLength: 255 }}
            />
          </Box>
        </DialogContent>

        <DialogActions>
          <Button 
            onClick={handleClose} 
            disabled={createMutation.isPending}
          >
            Cancel
          </Button>
          <Button
            type="submit"
            variant="contained"
            disabled={createMutation.isPending}
            startIcon={createMutation.isPending ? <CircularProgress size={20} /> : null}
          >
            {createMutation.isPending ? 'Adding...' : 'Add Record'}
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  );
};

export default AddBaseViewModal;