# Lineage Analysis Frontend

A React-based frontend application for Database Lineage Analysis System with AWS Amplify authentication.

## Features

- **AWS Amplify Authentication**: Secure authentication using AWS Cognito
- **Dashboard**: Main interface with search functionality and analysis controls
- **Column Lineage Table**: Interactive data grid showing view-to-source column mappings
- **User Management**: Role-based access with Admin, Analyst, and Viewer roles
- **Responsive Design**: Built with Material-UI for modern, responsive interface

## Technology Stack

- **React 18.3.0** with TypeScript
- **AWS Amplify 5.3.15** for authentication
- **Vite** for fast development and building
- **Material-UI (MUI)** for UI components
- **TanStack React Query** for data fetching
- **React Router** for navigation
- **MUI X Data Grid** for advanced table functionality

## Getting Started

### Prerequisites

- Node.js 18.0.0 or higher
- npm or yarn
- AWS Amplify CLI (optional, for auth configuration)

### Installation

1. Clone the repository
2. Install dependencies:
   ```bash
   npm install
   ```

3. Configure environment variables in `.env`:
   ```env
   VITE_AWS_REGION=us-east-1
   VITE_AWS_COGNITO_OAUTH_DOMAIN=your-cognito-domain.auth.us-east-1.amazoncognito.com
   VITE_SSO_REDIRECT_URI=http://localhost:3000
   VITE_AMPLIFY_USERPOOL_ID=us-east-1_xxxxxxxxx
   VITE_AMPLIFY_WEBCLIENT=your-client-id
   ```

4. Start the development server:
   ```bash
   npm run dev
   ```

5. Open [http://localhost:3000](http://localhost:3000) in your browser

### Authentication

The application uses AWS Cognito for authentication. Users will be redirected to the Cognito hosted UI for login. The application supports:

- **Email-based authentication**
- **OAuth 2.0 flow**
- **Role-based access control**
- **Automatic token refresh**

## Project Structure

```
src/
├── api/                 # API client and service functions
├── components/          # Reusable React components
├── domain/              # Domain models and types
├── hooks/               # Custom React hooks
│   └── users/           # User authentication hooks
├── pages/              # Page components
├── theme/              # Material-UI theme configuration
├── types/              # TypeScript type definitions
├── utils/              # Utility functions
├── test/               # Test setup and utilities
├── App.tsx             # Main application component
└── main.tsx            # Application entry point
```

## Available Scripts

- `npm run dev` - Start development server
- `npm run build` - Build for production
- `npm run preview` - Preview production build
- `npm run lint` - Run ESLint
- `npm run lint:fix` - Fix ESLint issues
- `npm run test` - Run tests
- `npm run test:coverage` - Run tests with coverage

## Environment Variables

Required environment variables:

```env
# App Configuration
VITE_APP_TITLE=Lineage Analysis
VITE_API_BASE_URL=http://localhost:8000
VITE_ENV=development
VITE_API_V1_PREFIX=/api/v1

# AWS Amplify Configuration
VITE_AWS_REGION=us-east-1
VITE_AWS_COGNITO_OAUTH_DOMAIN=your-domain.auth.us-east-1.amazoncognito.com
VITE_SSO_REDIRECT_URI=http://localhost:3000
VITE_AMPLIFY_USERPOOL_ID=us-east-1_xxxxxxxxx
VITE_AMPLIFY_WEBCLIENT=your-client-id

# Development/Testing
VITE_MOCKING=false
```

## Authentication Flow

1. **Initial Load**: App checks for existing authentication session
2. **Unauthenticated**: User is redirected to Cognito hosted UI
3. **Authentication**: User logs in via Cognito
4. **Token Management**: JWT tokens are automatically managed
5. **API Calls**: All API requests include authentication headers
6. **Session Expiry**: Automatic token refresh and re-authentication

## User Roles

The application supports three user roles:

- **Admin**: Full access to all features
- **Analyst**: Can perform analysis and view data
- **Viewer**: Read-only access to data

## API Integration

The frontend is designed to work with the Column Lineage API backend. Key endpoints:

- `/health` - Health check
- `/api/v1/lineage/columns` - Get column lineage data
- `/api/v1/lineage/analyze` - Analyze view lineage
- `/api/v1/lineage/search` - Search lineage data
- `/api/v1/users/me` - Get current user info

## Sample Data

The application includes sample data showing:
- View names (TEST_SEA_RAW_FQ, TEST_SEA_RAW)
- Column types (DIRECT, DERIVED)
- Source tables (CPS_DSCI_BR.SEA_BOOKINGS)
- Expression types (SUM, WINDOW)

## Development

### Code Style

- ESLint configuration for TypeScript and React
- Prettier for code formatting
- Pre-commit hooks for code quality

### Testing

- Vitest for unit testing
- React Testing Library for component testing
- Jest DOM matchers for assertions

## Deployment

The application can be deployed using:

1. **Static Hosting**: Build and deploy the `dist` folder
2. **Docker**: Use the included Dockerfile
3. **AWS Amplify Hosting**: Direct integration with Amplify Console
4. **AWS/Azure/GCP**: Deploy as a static web app

## AWS Amplify Configuration

The application includes Amplify configuration files:

- `amplify/cli.json` - Amplify CLI configuration
- `amplify/backend/backend-config.json` - Backend configuration
- `amplify/team-provider-info.json` - Environment-specific settings
- `amplify/backend/auth/` - Authentication configuration

## Future Enhancements

- Advanced search and filtering
- Data visualization charts
- Export functionality
- Real-time updates
- Advanced lineage analysis features
- Multi-tenant support