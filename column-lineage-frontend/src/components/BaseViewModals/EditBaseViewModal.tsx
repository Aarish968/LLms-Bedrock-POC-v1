import React, { useState, useEffect } from 'react';
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
  Typography,
} from '@mui/material';
import { useUpdateBaseView } from '../../hooks/useBaseView';
import { BaseViewRecord } from '../../api/baseViewService';

interface EditBaseViewModalProps {
  open: boolean;
  onClose: () => void;
  record: BaseViewRecord | null;
}

export const EditBaseViewModal: React.FC<EditBaseViewModalProps> = ({ 
  open, 
  onClose, 
  record 
}) => {
  const [tableName, setTableName] = useState<string>('');
  const [errors, setErrors] = useState<{ tableName?: string }>({});

  const updateMutation = useUpdateBaseView();

  // Update form when record changes
  useEffect(() => {
    if (record) {
      setTableName(record.table_name);
    }
  }, [record]);

  const validateForm = () => {
    const newErrors: { tableName?: string } = {};

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

    if (!record || !validateForm()) {
      return;
    }

    try {
      await updateMutation.mutateAsync({
        basePrimaryId: record.base_primary_id,
        request: {
          table_name: tableName.trim(),
        },
      });

      // Reset form and close modal on success
      setErrors({});
      onClose();
    } catch (error) {
      // Error is handled by the mutation hook
      console.error('Update failed:', error);
    }
  };

  const handleClose = () => {
    if (!updateMutation.isPending) {
      setTableName(record?.table_name || '');
      setErrors({});
      updateMutation.reset();
      onClose();
    }
  };

  if (!record) {
    return null;
  }

  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
      <form onSubmit={handleSubmit}>
        <DialogTitle>Edit Record</DialogTitle>
        
        <DialogContent>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, pt: 1 }}>
            {updateMutation.error && (
              <Alert severity="error">
                {updateMutation.error.message}
              </Alert>
            )}

            <Box>
              <Typography variant="body2" color="text.secondary" gutterBottom>
                Primary ID
              </Typography>
              <Typography variant="h6" sx={{ 
                p: 1.5, 
                bgcolor: 'grey.100', 
                borderRadius: 1,
                fontFamily: 'monospace'
              }}>
                {record.base_primary_id}
              </Typography>
            </Box>

            <TextField
              label="Table Name"
              value={tableName}
              onChange={(e) => setTableName(e.target.value)}
              error={!!errors.tableName}
              helperText={errors.tableName}
              disabled={updateMutation.isPending}
              required
              fullWidth
              inputProps={{ maxLength: 255 }}
            />
          </Box>
        </DialogContent>

        <DialogActions>
          <Button 
            onClick={handleClose} 
            disabled={updateMutation.isPending}
          >
            Cancel
          </Button>
          <Button
            type="submit"
            variant="contained"
            disabled={updateMutation.isPending || tableName.trim() === record.table_name}
            startIcon={updateMutation.isPending ? <CircularProgress size={20} /> : null}
          >
            {updateMutation.isPending ? 'Updating...' : 'Update Record'}
          </Button>
        </DialogActions>
      </form>
    </Dialog>
  );
};

export default EditBaseViewModal;