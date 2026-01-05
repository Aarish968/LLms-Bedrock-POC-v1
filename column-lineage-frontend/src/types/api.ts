// Common API response types
export interface ApiResponse<T = any> {
  data: T;
  message?: string;
  status: number;
}

export interface PaginatedResponse<T = any> {
  data: T[];
  total: number;
  page: number;
  limit: number;
  has_next: boolean;
  has_prev: boolean;
}

export interface ErrorResponse {
  detail: string;
  error?: string;
  message?: string;
  status_code?: number;
}

// Common request parameters
export interface PaginationParams {
  page?: number;
  limit?: number;
  offset?: number;
}

export interface SearchParams {
  query?: string;
  search?: string;
}

export interface FilterParams {
  filter?: Record<string, any>;
  sort?: string;
  order?: 'asc' | 'desc';
}

// API configuration types
export interface ApiConfig {
  baseURL: string;
  timeout: number;
  headers: Record<string, string>;
}

// Request/Response interceptor types
export interface RequestInterceptor {
  onFulfilled?: (config: any) => any;
  onRejected?: (error: any) => any;
}

export interface ResponseInterceptor {
  onFulfilled?: (response: any) => any;
  onRejected?: (error: any) => any;
}