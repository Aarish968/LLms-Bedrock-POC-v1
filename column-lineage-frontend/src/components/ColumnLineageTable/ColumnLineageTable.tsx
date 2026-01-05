import { useMemo, useState } from 'react'
import { DataGrid, GridColDef, GridRowsProp } from '@mui/x-data-grid'
import { Box, Chip, Alert, CircularProgress, Typography, TextField, Button, Stack } from '@mui/material'
import { Add as AddIcon, Edit as EditIcon, Search as SearchIcon } from '@mui/icons-material'
import { useBaseView } from '../../hooks/useBaseView'
import { BaseViewRecord } from '../../api/baseViewService'
import AddBaseViewModal from '../BaseViewModals/AddBaseViewModal'
import EditBaseViewModal from '../BaseViewModals/EditBaseViewModal'

interface ColumnLineageTableProps {
  searchQuery: string
}

const ColumnLineageTable = ({ searchQuery }: ColumnLineageTableProps) => {
  const [localSearchQuery, setLocalSearchQuery] = useState('')
  const [addModalOpen, setAddModalOpen] = useState(false)
  const [editModalOpen, setEditModalOpen] = useState(false)
  const [selectedRecord, setSelectedRecord] = useState<BaseViewRecord | null>(null)
  const [selectedRows, setSelectedRows] = useState<number[]>([])  // Track selected row IDs

  // Fetch base view data from API (always enabled, no mock mode)
  const {
    data: apiData,
    isLoading,
    error,
    isError,
    refetch,
  } = useBaseView({
    mock: false,
    limit: 100 
  })

  // Transform API data to match the simplified table structure
  const transformedApiData: GridRowsProp = useMemo(() => {
    if (!apiData?.records) return []
    
    return apiData.records.map((record: BaseViewRecord) => ({
      id: record.base_primary_id,
      basePrimaryId: record.base_primary_id,
      tableName: record.table_name,
    }))
  }, [apiData])

  // Use API data
  const dataToUse = transformedApiData

  // Simplified columns - only Primary ID and Table Name
  const columns: GridColDef[] = [
    {
      field: 'basePrimaryId',
      headerName: 'Primary ID',
      width: 150,
      align: 'center',
      headerAlign: 'center',
      renderCell: (params) => (
        <Chip
          label={params.value}
          size="small"
          color="primary"
          variant="outlined"
        />
      ),
    },
    {
      field: 'tableName',
      headerName: 'Table Name',
      flex: 1,
      minWidth: 300,
      renderCell: (params) => (
        <Box sx={{ 
          fontFamily: 'monospace', 
          fontSize: '0.875rem', 
          fontWeight: 'medium',
          color: 'text.primary'
        }}>
          {params.value}
        </Box>
      ),
    },
  ]

  // Filter data based on both search queries (prop and local)
  const filteredData = useMemo(() => {
    const combinedQuery = (searchQuery + ' ' + localSearchQuery).trim().toLowerCase()
    
    if (!combinedQuery) {
      return dataToUse
    }

    return dataToUse.filter((row) =>
      Object.values(row).some((value) =>
        String(value).toLowerCase().includes(combinedQuery)
      )
    )
  }, [searchQuery, localSearchQuery, dataToUse])

  // Handle button clicks
  const handleAdd = () => {
    setAddModalOpen(true)
  }

  const handleEdit = () => {
    if (selectedRows.length === 0) {
      alert('Please select a record to edit')
      return
    }

    if (selectedRows.length > 1) {
      alert('Please select only one record to edit')
      return
    }

    // Find the original record from API data using the selected row ID (base_primary_id)
    const selectedBasePrimaryId = selectedRows[0]
    const recordToEdit = apiData?.records.find(record => record.base_primary_id === selectedBasePrimaryId)
    
    if (recordToEdit) {
      setSelectedRecord(recordToEdit)
      setEditModalOpen(true)
    } else {
      alert('Selected record not found')
    }
  }

  // Error state
  if (isError) {
    return (
      <Box sx={{ mb: 2 }}>
        <Alert 
          severity="error" 
          action={
            <Button onClick={() => refetch()} size="small">
              Retry
            </Button>
          }
        >
          <Typography variant="h6" gutterBottom>
            Failed to load base view data
          </Typography>
          <Typography variant="body2">
            {error?.message || 'An unexpected error occurred'}
          </Typography>
        </Alert>
      </Box>
    )
  }

  return (
    <Box>
      {/* Header with title and controls on same line */}
      <Box sx={{ 
        display: 'flex', 
        justifyContent: 'space-between', 
        alignItems: 'center',
        mb: 2,
        flexWrap: 'wrap',
        gap: 2
      }}>
        <Typography variant="h6" component="h2">
          DC Base Tables
        </Typography>
        
        <Stack direction="row" spacing={2} alignItems="center">
          <TextField
            placeholder="Search by serial number or table name..."
            value={localSearchQuery}
            onChange={(e) => setLocalSearchQuery(e.target.value)}
            size="small"
            sx={{ minWidth: 300 }}
            slotProps={{
              input: {
                startAdornment: <SearchIcon sx={{ mr: 1, color: 'text.secondary' }} />,
              },
            }}
          />
          
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={handleAdd}
            size="small"
          >
            Add
          </Button>
          
          <Button
            variant="outlined"
            startIcon={<EditIcon />}
            onClick={handleEdit}
            size="small"
            disabled={selectedRows.length !== 1}
          >
            Edit {selectedRows.length === 1 ? '' : `(${selectedRows.length} selected)`}
          </Button>
        </Stack>
      </Box>

      {/* Data Grid */}
      <Box sx={{ height: 500, width: '100%' }}>
        <DataGrid
          rows={filteredData}
          columns={columns}
          loading={isLoading}
          initialState={{
            pagination: {
              paginationModel: {
                pageSize: 10,
              },
            },
          }}
          pageSizeOptions={[5, 10, 25, 50]}
          checkboxSelection
          disableRowSelectionOnClick
          onRowSelectionModelChange={(newSelection) => {
            setSelectedRows(newSelection as number[])
          }}
          rowSelectionModel={selectedRows}
          sx={{
            '& .MuiDataGrid-cell': {
              borderBottom: '1px solid #f0f0f0',
            },
            '& .MuiDataGrid-columnHeaders': {
              backgroundColor: '#f8f9fa',
              borderBottom: '2px solid #e0e0e0',
              fontWeight: 600,
            },
            '& .MuiDataGrid-row:hover': {
              backgroundColor: '#f5f5f5',
            },
            '& .MuiDataGrid-root': {
              border: '1px solid #e0e0e0',
            },
          }}
          slots={{
            noRowsOverlay: () => (
              <Box
                display="flex"
                flexDirection="column"
                alignItems="center"
                justifyContent="center"
                height="100%"
              >
                <Typography variant="h6" color="text.secondary">
                  No data available
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  {localSearchQuery || searchQuery ? 'No records match your search' : 'No records found'}
                </Typography>
              </Box>
            ),
          }}
        />
      </Box>

      {/* Modals */}
      <AddBaseViewModal
        open={addModalOpen}
        onClose={() => {
          setAddModalOpen(false)
          setSelectedRows([]) // Clear selection after adding
        }}
      />
      
      <EditBaseViewModal
        open={editModalOpen}
        onClose={() => {
          setEditModalOpen(false)
          setSelectedRecord(null)
          setSelectedRows([]) // Clear selection after editing
        }}
        record={selectedRecord}
      />
    </Box>
  )
}

export default ColumnLineageTable