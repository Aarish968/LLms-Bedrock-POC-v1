import '@testing-library/jest-dom'

// Mock environment variables
Object.defineProperty(import.meta, 'env', {
  value: {
    VITE_APP_TITLE: 'Lineage Analysis',
    VITE_API_BASE_URL: 'http://localhost:8000',
    VITE_ENV: 'test',
    VITE_API_V1_PREFIX: '/api/v1',
  },
  writable: true,
})