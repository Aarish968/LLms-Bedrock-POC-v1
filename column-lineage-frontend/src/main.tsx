import './polyfills'
import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import { QueryClient, QueryClientProvider, QueryCache, MutationCache } from '@tanstack/react-query'
import { ThemeProvider } from '@mui/material/styles'
import CssBaseline from '@mui/material/CssBaseline'
import { Amplify } from 'aws-amplify'
import { toast, Toaster } from 'sonner'
import CheckCircleIcon from '@mui/icons-material/CheckCircle'
import ErrorIcon from '@mui/icons-material/Error'
import InfoIcon from '@mui/icons-material/Info'
import WarningIcon from '@mui/icons-material/Warning'

import App from './App'
import { theme } from './theme/theme'
import { isNotAuthenticatedError } from './utils/isNotAuthenticatedError'
import isDev from './utils/isDev'
import './index.css'

// Configure Amplify
Amplify.configure({
  Auth: {
    region: import.meta.env.VITE_AWS_REGION,
    userPoolId: import.meta.env.VITE_AMPLIFY_USERPOOL_ID,
    userPoolWebClientId: import.meta.env.VITE_AMPLIFY_WEBCLIENT,
    mandatorySignIn: false,
    oauth: {
      domain: import.meta.env.VITE_AWS_COGNITO_OAUTH_DOMAIN,
      scope: ["openid", "email", "profile"],
      redirectSignIn: import.meta.env.VITE_SSO_REDIRECT_URI,
      redirectSignOut: import.meta.env.VITE_SSO_REDIRECT_URI,
      responseType: "code",
    },
  },
})

// Create a client with error handling
const queryClient = new QueryClient({
  queryCache: new QueryCache({
    onError: (error) => {
      if (isNotAuthenticatedError(error)) {
        toast.error("You are not authenticated. Please refresh the page.", {
          duration: 10000,
        });
      }
    },
  }),
  mutationCache: new MutationCache({
    onError: (error) => {
      if (isNotAuthenticatedError(error)) {
        toast.error("You are not authenticated. Please refresh the page.", {
          duration: 10000,
        });
      } else {
        if (isDev) console.error(error);
      }
    },
  }),
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      staleTime: 300,
      retry: (failureCount, error) => {
        if (isDev) console.error(error);
        return failureCount <= 0;
      },
    },
  },
})

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <BrowserRouter>
          <App />
        </BrowserRouter>
        <Toaster
          richColors
          icons={{
            success: <CheckCircleIcon />,
            error: <ErrorIcon />,
            info: <InfoIcon />,
            warning: <WarningIcon />,
          }}
        />
      </ThemeProvider>
    </QueryClientProvider>
  </React.StrictMode>,
)