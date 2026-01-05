import { useState } from 'react'
import {
  Box,
  Typography,
  TextField,
  Button,
  Card,
  CardContent,
  Grid,
  InputAdornment,
  Chip,
  Tabs,
  Tab,
} from '@mui/material'
import { Search, PlayArrow, Person, Work, Analytics } from '@mui/icons-material'

import ColumnLineageTable from '../components/ColumnLineageTable/ColumnLineageTable'
import LineageAnalysisDialog from '../components/LineageAnalysis/LineageAnalysisDialog'
import JobsDashboard from '../components/LineageAnalysis/JobsDashboard'
import useUserContext from '@/hooks/users/useUserContext'

const DashboardPage = () => {
  const [searchQuery, setSearchQuery] = useState('')
  const [analysisDialogOpen, setAnalysisDialogOpen] = useState(false)
  const [currentTab, setCurrentTab] = useState(0)
  const user = useUserContext()

  const handleSearch = (e: React.ChangeEvent<HTMLInputElement>) => {
    setSearchQuery(e.target.value)
  }

  const handleStartAnalysis = () => {
    setAnalysisDialogOpen(true)
  }

  const handleAnalysisStarted = () => {
    // Switch to Analysis Jobs tab when analysis starts
    setCurrentTab(1)
    setAnalysisDialogOpen(false)
  }

  const handleCloseAnalysisDialog = () => {
    setAnalysisDialogOpen(false)
  }

  const handleTabChange = (event: React.SyntheticEvent, newValue: number) => {
    setCurrentTab(newValue)
  }

  return (
    <Box>
      {/* Page Header */}
      <Box sx={{ mb: 3 }}>
        <Typography variant="h4" component="h1" gutterBottom>
          Dashboard
        </Typography>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, mb: 2 }}>
          <Typography variant="body1" color="text.secondary">
            Database Lineage Analysis System
          </Typography>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Person fontSize="small" color="action" />
            <Typography variant="body2" color="text.secondary">
              Welcome, {user.email}
            </Typography>
            {user.isAdmin && (
              <Chip label="Admin" size="small" color="primary" variant="outlined" />
            )}
            {user.isAnalyst && (
              <Chip label="Analyst" size="small" color="secondary" variant="outlined" />
            )}
          </Box>
        </Box>
      </Box>

      {/* Controls Section */}
      <Grid container spacing={3} sx={{ mb: 4 }}>
        {/* Search Box */}
        <Grid item xs={12}>
          <Card>
            <CardContent>
              <TextField
                fullWidth
                placeholder="Search views, tables, or columns..."
                value={searchQuery}
                onChange={handleSearch}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <Search />
                    </InputAdornment>
                  ),
                }}
                sx={{ mb: 1 }}
              />
              <Typography variant="caption" color="text.secondary">
                Search functionality will be implemented later
              </Typography>
            </CardContent>
          </Card>
        </Grid>
      </Grid>

      {/* Main Content Tabs */}
      <Card>
        <Box sx={{ borderBottom: 1, borderColor: 'divider' }}>
          <Tabs value={currentTab} onChange={handleTabChange}>
            <Tab 
              icon={<Analytics />} 
              label="Column Lineage" 
              iconPosition="start"
            />
            <Tab 
              icon={<Work />} 
              label="Analysis Jobs" 
              iconPosition="start"
            />
          </Tabs>
        </Box>

        <CardContent>
          {currentTab === 0 && (
            <Box>
              {/* Search Box for Column Lineage */}
              {/* <TextField
                fullWidth
                placeholder="Search views, tables, or columns..."
                value={searchQuery}
                onChange={handleSearch}
                InputProps={{
                  startAdornment: (
                    <InputAdornment position="start">
                      <Search />
                    </InputAdornment>
                  ),
                }}
                sx={{ mb: 3 }}
              /> */}
              <ColumnLineageTable searchQuery={searchQuery} />
              
              {/* Start Analysis Button - Moved after table */}
              <Box sx={{ 
                display: 'flex', 
                justifyContent: 'center', 
                alignItems: 'center',
                mt: 3,
                p: 2
              }}>
                <Box sx={{ textAlign: 'center' }}>
                  <Button
                    variant="contained"
                    size="large"
                    startIcon={<PlayArrow />}
                    onClick={handleStartAnalysis}
                    sx={{ mb: 1 }}
                  >
                    Start View to Column Lineage
                  </Button>
                  <Typography variant="caption" color="text.secondary" display="block">
                    Analyze database view dependencies
                  </Typography>
                </Box>
              </Box>
            </Box>
          )}

          {currentTab === 1 && (
            <JobsDashboard onNewAnalysis={handleStartAnalysis} />
          )}
        </CardContent>
      </Card>

      {/* Lineage Analysis Dialog */}
      <LineageAnalysisDialog
        open={analysisDialogOpen}
        onClose={handleCloseAnalysisDialog}
        onAnalysisStarted={handleAnalysisStarted}
      />
    </Box>
  )
}

export default DashboardPage