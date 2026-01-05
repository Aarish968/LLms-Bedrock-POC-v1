import React, { useState, useEffect } from 'react';
import {
  Box,
  TextField,
  MenuItem,
  Typography,
  CircularProgress,
  Alert,
} from '@mui/material';
import { api } from '@/api/client';

interface DatabaseSchemaSelectorProps {
  selectedDatabase: string;
  selectedSchema: string;
  onDatabaseChange: (database: string) => void;
  onSchemaChange: (schema: string) => void;
  disabled?: boolean;
}

const DatabaseSchemaSelector: React.FC<DatabaseSchemaSelectorProps> = ({
  selectedDatabase,
  selectedSchema,
  onDatabaseChange,
  onSchemaChange,
  disabled = false,
}) => {
  const [databases, setDatabases] = useState<string[]>([]);
  const [schemas, setSchemas] = useState<string[]>([]);
  const [loadingDatabases, setLoadingDatabases] = useState(false);
  const [loadingSchemas, setLoadingSchemas] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Load databases on mount
  useEffect(() => {
    loadDatabases();
  }, []);

  // Load schemas when database changes
  useEffect(() => {
    if (selectedDatabase) {
      loadSchemas(selectedDatabase);
    } else {
      setSchemas([]);
      onSchemaChange('');
    }
  }, [selectedDatabase]);

  const loadDatabases = async () => {
    setLoadingDatabases(true);
    setError(null);
    try {
      const response = await api.get<string[]>('/api/v1/lineage/public/databases');
      setDatabases(response.data);
    } catch (err) {
      setError('Failed to load databases');
      console.error('Error loading databases:', err);
    } finally {
      setLoadingDatabases(false);
    }
  };

  const loadSchemas = async (database: string) => {
    setLoadingSchemas(true);
    setError(null);
    try {
      const response = await api.get<string[]>('/api/v1/lineage/public/schemas', {
        params: { database_filter: database }
      });
      setSchemas(response.data);
    } catch (err) {
      setError('Failed to load schemas');
      console.error('Error loading schemas:', err);
    } finally {
      setLoadingSchemas(false);
    }
  };

  const handleDatabaseChange = (database: string) => {
    onDatabaseChange(database);
    onSchemaChange(''); // Reset schema when database changes
  };

  if (error) {
    return (
      <Alert severity="error" sx={{ mb: 2 }}>
        {error}
      </Alert>
    );
  }

  return (
    <Box>
      <Typography variant="subtitle2" gutterBottom>
        Database & Schema Selection
      </Typography>
      
      {/* Database Selector */}
      <TextField
        select
        fullWidth
        label="Database *"
        value={selectedDatabase}
        onChange={(e) => handleDatabaseChange(e.target.value)}
        disabled={disabled || loadingDatabases}
        sx={{ mb: 2 }}
        helperText="Select the database to analyze"
        InputProps={{
          endAdornment: loadingDatabases ? <CircularProgress size={20} /> : null,
        }}
      >
        {databases.map((database) => (
          <MenuItem key={database} value={database}>
            {database}
          </MenuItem>
        ))}
      </TextField>

      {/* Schema Selector */}
      <TextField
        select
        fullWidth
        label="Schema *"
        value={selectedSchema}
        onChange={(e) => onSchemaChange(e.target.value)}
        disabled={disabled || !selectedDatabase || loadingSchemas}
        sx={{ mb: 2 }}
        helperText="Select the schema to analyze"
        InputProps={{
          endAdornment: loadingSchemas ? <CircularProgress size={20} /> : null,
        }}
      >
        {schemas.map((schema) => (
          <MenuItem key={schema} value={schema}>
            {schema}
          </MenuItem>
        ))}
      </TextField>
    </Box>
  );
};

export default DatabaseSchemaSelector;