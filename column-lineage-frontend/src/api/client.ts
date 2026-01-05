import axios, { AxiosInstance, AxiosRequestConfig, AxiosResponse, AxiosError } from 'axios';
import { Auth } from 'aws-amplify';

// API configuration interface
interface ApiConfig {
  baseURL: string;
  timeout: number;
  headers: Record<string, string>;
}

// Default API configuration
const defaultConfig: ApiConfig = {
  baseURL: import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000',
  timeout: 10000,
  headers: {
    'Content-Type': 'application/json',
  },
};

// Create axios instance
const apiClient: AxiosInstance = axios.create(defaultConfig);

// Request interceptor
apiClient.interceptors.request.use(
  async (config: AxiosRequestConfig) => {
    // Add timestamp to prevent caching
    if (config.params) {
      config.params._t = Date.now();
    } else {
      config.params = { _t: Date.now() };
    }

    // Add Cognito JWT token if available
    try {
      const session = await Auth.currentSession();
      const token = session.getIdToken().getJwtToken();
      if (token && config.headers) {
        config.headers.Authorization = `Bearer ${token}`;
      }
    } catch (error) {
      // User not authenticated - this is fine for public endpoints
      if (import.meta.env.DEV) {
        console.log('No auth session available:', error);
      }
    }

    // Log request in development
    if (import.meta.env.DEV) {
      console.log('🚀 API Request:', {
        method: config.method?.toUpperCase(),
        url: config.url,
        params: config.params,
        data: config.data,
      });
    }

    return config;
  },
  (error: AxiosError) => {
    console.error('❌ Request Error:', error);
    return Promise.reject(error);
  }
);

// Response interceptor
apiClient.interceptors.response.use(
  (response: AxiosResponse) => {
    // Log response in development
    if (import.meta.env.DEV) {
      console.log('✅ API Response:', {
        status: response.status,
        url: response.config.url,
        data: response.data,
      });
    }

    return response;
  },
  async (error: AxiosError) => {
    // Enhanced error handling
    const errorMessage = getErrorMessage(error);
    
    console.error('❌ API Error:', {
      status: error.response?.status,
      message: errorMessage,
      url: error.config?.url,
      data: error.response?.data,
    });

    // Handle specific error cases
    if (error.response?.status === 401) {
      // Handle unauthorized - redirect to login
      try {
        await Auth.signOut();
        window.location.href = '/';
      } catch (signOutError) {
        console.error('Error signing out:', signOutError);
      }
    }

    if (error.response?.status === 403) {
      // Handle forbidden - show permission error
      console.warn('Access forbidden - insufficient permissions');
    }

    if (error.response?.status >= 500) {
      // Handle server errors
      console.error('Server error - please try again later');
    }

    return Promise.reject(error);
  }
);

// Helper function to extract error messages
function getErrorMessage(error: AxiosError): string {
  if (error.response?.data) {
    const data = error.response.data as any;
    
    // Handle different error response formats
    if (typeof data === 'string') {
      return data;
    }
    
    if (data.detail) {
      return data.detail;
    }
    
    if (data.message) {
      return data.message;
    }
    
    if (data.error) {
      return data.error;
    }
  }
  
  if (error.message) {
    return error.message;
  }
  
  return 'An unexpected error occurred';
}

// API client methods
export const api = {
  // GET request
  get: <T = any>(url: string, config?: AxiosRequestConfig): Promise<AxiosResponse<T>> => {
    return apiClient.get<T>(url, config);
  },

  // POST request
  post: <T = any>(url: string, data?: any, config?: AxiosRequestConfig): Promise<AxiosResponse<T>> => {
    return apiClient.post<T>(url, data, config);
  },

  // PUT request
  put: <T = any>(url: string, data?: any, config?: AxiosRequestConfig): Promise<AxiosResponse<T>> => {
    return apiClient.put<T>(url, data, config);
  },

  // PATCH request
  patch: <T = any>(url: string, data?: any, config?: AxiosRequestConfig): Promise<AxiosResponse<T>> => {
    return apiClient.patch<T>(url, data, config);
  },

  // DELETE request
  delete: <T = any>(url: string, config?: AxiosRequestConfig): Promise<AxiosResponse<T>> => {
    return apiClient.delete<T>(url, config);
  },

  // Upload file
  upload: <T = any>(url: string, formData: FormData, config?: AxiosRequestConfig): Promise<AxiosResponse<T>> => {
    return apiClient.post<T>(url, formData, {
      ...config,
      headers: {
        ...config?.headers,
        'Content-Type': 'multipart/form-data',
      },
    });
  },

  // Download file
  download: (url: string, filename?: string, config?: AxiosRequestConfig): Promise<void> => {
    return apiClient.get(url, {
      ...config,
      responseType: 'blob',
    }).then((response) => {
      const blob = new Blob([response.data]);
      const downloadUrl = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = downloadUrl;
      link.download = filename || 'download';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(downloadUrl);
    });
  },
};

// Export the axios instance for direct use if needed
export { apiClient };

// Export types
export type { AxiosRequestConfig, AxiosResponse, AxiosError };

// Utility functions
export const apiUtils = {
  // Check if error is network error
  isNetworkError: (error: AxiosError): boolean => {
    return !error.response && error.code === 'NETWORK_ERROR';
  },

  // Check if error is timeout
  isTimeoutError: (error: AxiosError): boolean => {
    return error.code === 'ECONNABORTED';
  },

  // Check if error is client error (4xx)
  isClientError: (error: AxiosError): boolean => {
    return error.response ? error.response.status >= 400 && error.response.status < 500 : false;
  },

  // Check if error is server error (5xx)
  isServerError: (error: AxiosError): boolean => {
    return error.response ? error.response.status >= 500 : false;
  },

  // Get error status code
  getErrorStatus: (error: AxiosError): number | null => {
    return error.response?.status || null;
  },

  // Get error message
  getErrorMessage,
};