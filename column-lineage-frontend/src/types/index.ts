// Column Lineage Types
export interface ColumnLineage {
  id: number
  viewName: string
  viewColumn: string
  columnType: 'DIRECT' | 'DERIVED'
  sourceTable: string
  sourceColumn: string
  expressionType: string
}

// API Response Types
export interface ApiResponse<T> {
  data: T
  message?: string
  status: 'success' | 'error'
}

// Health Check Types
export interface HealthStatus {
  status: string
  timestamp: string
  version: string
  environment: string
}

export interface DetailedHealthStatus extends HealthStatus {
  services: Record<string, {
    status: string
    type: string
    details: string | object
  }>
}

// Authentication Types
export interface LoginCredentials {
  username: string
  password: string
}

export interface AuthUser {
  id: string
  username: string
  email?: string
  roles?: string[]
}

// API Error Types
export interface ApiError {
  message: string
  code?: string
  details?: unknown
}