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
import { Search, PlayArrow, Person, Work, Analytics, Code, Storage } from '@mui/icons-material'

import ColumnLineageTable from '../components/ColumnLineageTable/ColumnLineageTable'
import LineageAnalysisDialog from '../components/LineageAnalysis/LineageAnalysisDialog'
import JobsDashboard from '../components/LineageAnalysis/JobsDashboard'
import { RepositoryAnalysisDialog, RepositoryAnalysisJobs } from '../components/RepositoryAnalysis'
import { SPAnalysisDialog, SPAnalysisJobs } from '../components/SPAnalysis'
import useUserContext from '@/hooks/users/useUserContext'
import useRepositoryAnalysis from '../hooks/useRepositoryAnalysis'
import useSPAnalysis from '../hooks/useSPAnalysis'
import { RepositoryAnalysisResponse, RepositoryAnalysisJob, AnalysisStatus } from '../types/repositoryAnalysis'
import { SPAnalysisResponse } from '../types/spAnalysis'

const DashboardPage = () => {
  const [searchQuery, setSearchQuery] = useState('')
  const [analysisDialogOpen, setAnalysisDialogOpen] = useState(false)
  const [repoAnalysisDialogOpen, setRepoAnalysisDialogOpen] = useState(false)
  const [spAnalysisDialogOpen, setSPAnalysisDialogOpen] = useState(false)
  const [currentTab, setCurrentTab] = useState(0)
  const [analysisJobsSubTab, setAnalysisJobsSubTab] = useState(0)
  const user = useUserContext()
  const { hasRunningJob: hasRunningRepoJob, addJobToState } = useRepositoryAnalysis()
  const { hasRunningJob: hasRunningSPJob } = useSPAnalysis()

  const handleSearch = (e: React.ChangeEvent<HTMLInputElement>) => {
    setSearchQuery(e.target.value)
  }

  const handleStartAnalysis = () => {
    setAnalysisDialogOpen(true)
  }

  const handleStartRepoAnalysis = () => {
    if (hasRunningRepoJob) {
      // Show warning but still allow opening dialog for better UX
    }
    setRepoAnalysisDialogOpen(true)
  }

  const handleStartSPAnalysis = () => {
    if (hasRunningSPJob) {
      // Show warning but still allow opening dialog for better UX
    }
    setSPAnalysisDialogOpen(true)
  }

  const handleAnalysisStarted = () => {
    // Switch to Analysis Jobs tab and Column Lineage sub-tab when analysis starts
    setCurrentTab(1)
    setAnalysisJobsSubTab(0)
    setAnalysisDialogOpen(false)
  }

  const handleRepoAnalysisStarted = (response: RepositoryAnalysisResponse) => {
    // Immediately add job to state for instant UI feedback
    const newJob: RepositoryAnalysisJob = {
      job_id: response.job_id,
      status: response.status as AnalysisStatus,
      message: response.message,
      output_file: response.output_file,
      started_at: response.started_at,
    };
    
    addJobToState(newJob);
    
    // Switch to Analysis Jobs tab and Repository Analyze Job sub-tab when repo analysis starts
    setCurrentTab(1)
    setAnalysisJobsSubTab(1)
    setRepoAnalysisDialogOpen(false)
  }

  const handleSPAnalysisStarted = (_response: SPAnalysisResponse) => {
    // Switch to Analysis Jobs tab and SP Analysis sub-tab when SP analysis starts
    setCurrentTab(1)
    setAnalysisJobsSubTab(2)
    setSPAnalysisDialogOpen(false)
  }

  const handleCloseAnalysisDialog = () => {
    setAnalysisDialogOpen(false)
  }

  const handleCloseRepoAnalysisDialog = () => {
    setRepoAnalysisDialogOpen(false)
  }

  const handleCloseSPAnalysisDialog = () => {
    setSPAnalysisDialogOpen(false)
  }

  const handleTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setCurrentTab(newValue)
  }

  const handleAnalysisJobsSubTabChange = (_event: React.SyntheticEvent, newValue: number) => {
    setAnalysisJobsSubTab(newValue)
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
              label="ANALYSIS JOBS" 
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
              
              {/* Start Analysis Buttons - Moved after table */}
              <Box sx={{ 
                display: 'flex', 
                justifyContent: 'center', 
                alignItems: 'center',
                gap: 3,
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

                <Box sx={{ textAlign: 'center' }}>
                  <Button
                    variant="contained"
                    size="large"
                    startIcon={<PlayArrow />}
                    onClick={handleStartRepoAnalysis}
                    disabled={hasRunningRepoJob}
                    title={hasRunningRepoJob ? 'Please wait for current repository analysis to complete' : 'Start repository analysis'}
                    sx={{ mb: 1 }}
                  >
                    Start Action To Endpoint Lineage
                  </Button>
                  <Typography variant="caption" color="text.secondary" display="block">
                    Analyze action to endpoint analysis
                  </Typography>
                </Box>

                <Box sx={{ textAlign: 'center' }}>
                  <Button
                    variant="contained"
                    size="large"
                    startIcon={<PlayArrow />}
                    onClick={handleStartSPAnalysis}
                    disabled={hasRunningSPJob}
                    title={hasRunningSPJob ? 'Please wait for current SP analysis to complete' : 'Start stored procedure analysis'}
                    sx={{ mb: 1 }}
                  >
                    Start SP Lineage
                  </Button>
                  <Typography variant="caption" color="text.secondary" display="block">
                    Analyze stored procedure relationships
                  </Typography>
                </Box>
              </Box>
            </Box>
          )}

          {currentTab === 1 && (
            <Box>
              {/* Sub-tabs for Analysis Jobs */}
              <Box sx={{ borderBottom: 1, borderColor: 'divider', mb: 2 }}>
                <Tabs value={analysisJobsSubTab} onChange={handleAnalysisJobsSubTabChange}>
                  <Tab 
                    icon={<Work />} 
                    label="Column Lineage" 
                    iconPosition="start"
                  />
                  <Tab 
                    icon={<Code />} 
                    label="Repository Analyze Job" 
                    iconPosition="start"
                  />
                  <Tab 
                    icon={<Storage />} 
                    label="Start SP Lineage" 
                    iconPosition="start"
                  />
                </Tabs>
              </Box>

              {/* Sub-tab content */}
              {analysisJobsSubTab === 0 && (
                <JobsDashboard 
                  onNewAnalysis={handleStartAnalysis}
                />
              )}

              {analysisJobsSubTab === 1 && (
                <RepositoryAnalysisJobs 
                  onNewAnalysis={handleStartRepoAnalysis}
                />
              )}

              {analysisJobsSubTab === 2 && (
                <SPAnalysisJobs 
                  onNewAnalysis={handleStartSPAnalysis}
                />
              )}
            </Box>
          )}
        </CardContent>
      </Card>

      {/* Lineage Analysis Dialog */}
      <LineageAnalysisDialog
        open={analysisDialogOpen}
        onClose={handleCloseAnalysisDialog}
        onAnalysisStarted={handleAnalysisStarted}
      />

      {/* Repository Analysis Dialog */}
      <RepositoryAnalysisDialog
        open={repoAnalysisDialogOpen}
        onClose={handleCloseRepoAnalysisDialog}
        onAnalysisStarted={handleRepoAnalysisStarted}
      />

      {/* SP Analysis Dialog */}
      <SPAnalysisDialog
        open={spAnalysisDialogOpen}
        onClose={handleCloseSPAnalysisDialog}
        onAnalysisStarted={handleSPAnalysisStarted}
      />
    </Box>
  )
}

export default DashboardPage