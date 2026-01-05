import { Box, CircularProgress, Typography } from '@mui/material'
import { Analytics } from '@mui/icons-material'

const LoadingSpinnerFullPage = () => {
  return (
    <Box
      sx={{
        display: 'flex',
        flexDirection: 'column',
        justifyContent: 'center',
        alignItems: 'center',
        minHeight: '100vh',
        backgroundColor: 'background.default',
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', mb: 3 }}>
        <Analytics sx={{ mr: 1, fontSize: 32, color: 'primary.main' }} />
        <Typography variant="h4" component="h1" color="primary">
          Lineage Analysis
        </Typography>
      </Box>
      
      <CircularProgress size={40} />
      
      <Typography variant="body1" sx={{ mt: 2, color: 'text.secondary' }}>
        Loading...
      </Typography>
    </Box>
  )
}

export default LoadingSpinnerFullPage