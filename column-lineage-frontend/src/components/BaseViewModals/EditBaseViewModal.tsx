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
import { Delete as DeleteIcon, Edit as EditIcon } from '@mui/icons-material';
import { useUpdateBaseView, useDeleteBaseView } from '../../hooks/useBaseView';
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
  const [showDeleteConfirm, setShowDeleteConfirm] = useState<boolean>(false);


  const updateMutation = useUpdateBaseView();
  const deleteMutation = useDeleteBaseView();


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


  const handleDelete = async () => {
    if (!record) return;


    try {
      await deleteMutation.mutateAsync(record.base_primary_id);
     
      // Reset form and close modal on success
      setShowDeleteConfirm(false);
      setErrors({});
      onClose();
    } catch (error) {
      // Error is handled by the mutation hook
      console.error('Delete failed:', error);
    }
  };


  const handleClose = () => {
    if (!updateMutation.isPending && !deleteMutation.isPending) {
      setTableName(record?.table_name || '');
      setErrors({});
      setShowDeleteConfirm(false);
      updateMutation.reset();
      deleteMutation.reset();
      onClose();
    }
  };


  if (!record) {
    return null;
  }


  return (
    <Dialog open={open} onClose={handleClose} maxWidth="sm" fullWidth>
      <form onSubmit={handleSubmit}>
        <DialogTitle>
          {showDeleteConfirm ? 'Delete Record' : 'Edit Record'}
        </DialogTitle>
       
        <DialogContent>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2, pt: 1 }}>
            {(updateMutation.error || deleteMutation.error) && (
              <Alert severity="error">
                {updateMutation.error?.message || deleteMutation.error?.message}
              </Alert>
            )}


            {showDeleteConfirm ? (
              <Box>
                <Alert severity="warning" sx={{ mb: 2 }}>
                  <Typography variant="h6" gutterBottom>
                    Confirm Deletion
                  </Typography>
                  <Typography variant="body2">
                    Are you sure you want to delete this record? This action cannot be undone.
                  </Typography>
                </Alert>
               
                <Box sx={{
                  p: 2,
                  bgcolor: 'grey.50',
                  borderRadius: 1,
                  border: '1px solid',
                  borderColor: 'grey.300'
                }}>
                  <Typography variant="body2" color="text.secondary" gutterBottom>
                    Primary ID
                  </Typography>
                  <Typography variant="h6" sx={{ fontFamily: 'monospace', mb: 1 }}>
                    {record.base_primary_id}
                  </Typography>
                  <Typography variant="body2" color="text.secondary" gutterBottom>
                    Table Name
                  </Typography>
                  <Typography variant="body1" sx={{ fontFamily: 'monospace' }}>
                    {record.table_name}
                  </Typography>
                </Box>
              </Box>
            ) : (
              <>
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
                  disabled={updateMutation.isPending || deleteMutation.isPending}
                  required
                  fullWidth
                  inputProps={{ maxLength: 255 }}
                />
              </>
            )}
          </Box>
        </DialogContent>


        <DialogActions sx={{ justifyContent: 'space-between', px: 3, py: 2 }}>
          {showDeleteConfirm ? (
            <>
              <Button
                onClick={() => setShowDeleteConfirm(false)}
                disabled={deleteMutation.isPending}
              >
                Cancel Delete
              </Button>
              <Box sx={{ display: 'flex', gap: 1 }}>
                <Button
                  onClick={handleClose}
                  disabled={deleteMutation.isPending}
                >
                  Close
                </Button>
                <Button
                  onClick={handleDelete}
                  variant="contained"
                  color="error"
                  disabled={deleteMutation.isPending}
                  startIcon={deleteMutation.isPending ? <CircularProgress size={20} /> : <DeleteIcon />}
                >
                  {deleteMutation.isPending ? 'Deleting...' : 'Confirm Delete'}
                </Button>
              </Box>
            </>
          ) : (
            <>
              <Button
                onClick={() => setShowDeleteConfirm(true)}
                color="error"
                variant="outlined"
                startIcon={<DeleteIcon />}
                disabled={updateMutation.isPending || deleteMutation.isPending}
              >
                Delete
              </Button>
              <Box sx={{ display: 'flex', gap: 1 }}>
                <Button
                  onClick={handleClose}
                  disabled={updateMutation.isPending || deleteMutation.isPending}
                >
                  Cancel
                </Button>
                <Button
                  type="submit"
                  variant="contained"
                  disabled={updateMutation.isPending || deleteMutation.isPending || tableName.trim() === record.table_name}
                  startIcon={updateMutation.isPending ? <CircularProgress size={20} /> : <EditIcon />}
                >
                  {updateMutation.isPending ? 'Updating...' : 'Update Record'}
                </Button>
              </Box>
            </>
          )}
        </DialogActions>
      </form>
    </Dialog>
  );
};


export default EditBaseViewModal;
