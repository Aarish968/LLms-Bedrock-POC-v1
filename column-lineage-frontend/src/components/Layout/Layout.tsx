import { Outlet } from 'react-router-dom'
import {
  AppBar,
  Toolbar,
  Typography,
  Box,
  Container,
  IconButton,
  Menu,
  MenuItem,
  Avatar,
  Divider,
} from '@mui/material'
import { Analytics, AccountCircle, Logout } from '@mui/icons-material'
import { Auth } from 'aws-amplify'
import { useState } from 'react'

import useUserContext from '@/hooks/users/useUserContext'

const Layout = () => {
  const user = useUserContext()
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null)

  const handleMenu = (event: React.MouseEvent<HTMLElement>) => {
    setAnchorEl(event.currentTarget)
  }

  const handleClose = () => {
    setAnchorEl(null)
  }

  const handleLogout = async () => {
    try {
      await Auth.signOut()
      // Clear local storage
      localStorage.removeItem('jwt')
      localStorage.removeItem('authToken')
      // Refresh the page to trigger re-authentication
      window.location.reload()
    } catch (error) {
      console.error('Error signing out:', error)
    }
    handleClose()
  }

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      {/* Header */}
      <AppBar position="static" elevation={1}>
        <Toolbar>
          <Analytics sx={{ mr: 2 }} />
          <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
            Lineage Analysis
          </Typography>
          
          {/* User Menu */}
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Typography variant="body2" sx={{ mr: 2, display: { xs: 'none', sm: 'block' } }}>
              {user.email}
            </Typography>
            <IconButton
              size="large"
              aria-label="account of current user"
              aria-controls="menu-appbar"
              aria-haspopup="true"
              onClick={handleMenu}
              color="inherit"
            >
              <Avatar sx={{ width: 32, height: 32 }}>
                <AccountCircle />
              </Avatar>
            </IconButton>
            <Menu
              id="menu-appbar"
              anchorEl={anchorEl}
              anchorOrigin={{
                vertical: 'top',
                horizontal: 'right',
              }}
              keepMounted
              transformOrigin={{
                vertical: 'top',
                horizontal: 'right',
              }}
              open={Boolean(anchorEl)}
              onClose={handleClose}
            >
              <MenuItem disabled>
                <Typography variant="body2" color="text.secondary">
                  {user.email}
                </Typography>
              </MenuItem>
              <Divider />
              <MenuItem onClick={handleLogout}>
                <Logout sx={{ mr: 1 }} fontSize="small" />
                Logout
              </MenuItem>
            </Menu>
          </Box>
        </Toolbar>
      </AppBar>

      {/* Main Content */}
      <Box component="main" sx={{ flexGrow: 1, py: 3 }}>
        <Container maxWidth="xl">
          <Outlet />
        </Container>
      </Box>
    </Box>
  )
}

export default Layout