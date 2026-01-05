import { useMemo } from 'react'
import { Routes, Route, Navigate } from 'react-router-dom'
import { Box } from '@mui/material'

import Layout from './components/Layout/Layout'
import DashboardPage from './pages/DashboardPage'
import LoadingSpinnerFullPage from './components/Loading/LoadingSpinnerFullPage'
import useAuthUser from './hooks/users/useAuthUser'
import { UserContext } from './hooks/users/userContext'

type AuthenticatedAppProps = Pick<
  ReturnType<typeof useAuthUser>,
  "user" | "setUser"
>;

function AuthenticatedApp({ user, setUser }: AuthenticatedAppProps) {
  const userContextValue = useMemo(() => ({ user, setUser }), [user, setUser]);

  return (
    <UserContext.Provider value={userContextValue}>
      <Box sx={{ minHeight: '100vh' }}>
        <Routes>
          <Route path="/" element={<Layout />}>
            <Route index element={<Navigate to="/dashboard" replace />} />
            <Route path="dashboard" element={<DashboardPage />} />
          </Route>
        </Routes>
      </Box>
    </UserContext.Provider>
  );
}

function App() {
  const { user, setUser, isLoading: isLoadingUser } = useAuthUser();

  if (isLoadingUser || !user) {
    return <LoadingSpinnerFullPage />;
  }

  return <AuthenticatedApp user={user} setUser={setUser} />;
}

export default App