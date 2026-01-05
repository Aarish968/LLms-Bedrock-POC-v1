import { api, apiUtils, AxiosError } from './client';

// Types for the base view API
export interface BaseViewRecord {
  base_primary_id: number;
  table_name: string;
}

export interface BaseViewResponse {
  total_records: number;
  records: BaseViewRecord[];
}

export interface BaseViewParams {
  mock?: boolean;
}

export interface BaseViewCreateRequest {
  base_primary_id: number;
  table_name: string;
}

export interface BaseViewUpdateRequest {
  table_name: string;
}

export const baseViewService = {
  /**
   * Fetch base view data from the API
   */
  async getBaseViewData(params: BaseViewParams = {}): Promise<BaseViewResponse> {
    try {
      const response = await api.get<BaseViewResponse>('/api/v1/lineage/public/base-view', {
        params: {
          ...(params.mock && { mock: params.mock }),
        },
      });

      return response.data;
    } catch (error) {
      const axiosError = error as AxiosError;
      
      // Use utility functions for better error handling
      if (apiUtils.isNetworkError(axiosError)) {
        throw new Error('Network error - please check your connection');
      }
      
      if (apiUtils.isTimeoutError(axiosError)) {
        throw new Error('Request timeout - please try again');
      }
      
      if (apiUtils.isServerError(axiosError)) {
        throw new Error('Server error - please try again later');
      }
      
      // Use the utility function to get error message
      const errorMessage = apiUtils.getErrorMessage(axiosError);
      throw new Error(errorMessage || 'Failed to fetch base view data');
    }
  },

  /**
   * Create a new base view record
   */
  async createBaseViewRecord(request: BaseViewCreateRequest): Promise<BaseViewRecord> {
    try {
      const response = await api.post<BaseViewRecord>('/api/v1/lineage/public/base-view', request);
      return response.data;
    } catch (error) {
      const axiosError = error as AxiosError;
      
      if (apiUtils.getErrorStatus(axiosError) === 409) {
        throw new Error(`Record with primary ID ${request.base_primary_id} already exists`);
      }
      
      if (apiUtils.getErrorStatus(axiosError) === 503) {
        throw new Error('Database connection not available');
      }
      
      const errorMessage = apiUtils.getErrorMessage(axiosError);
      throw new Error(errorMessage || 'Failed to create record');
    }
  },

  /**
   * Update an existing base view record
   */
  async updateBaseViewRecord(basePrimaryId: number, request: BaseViewUpdateRequest): Promise<BaseViewRecord> {
    try {
      const response = await api.put<BaseViewRecord>(`/api/v1/lineage/public/base-view/${basePrimaryId}`, request);
      return response.data;
    } catch (error) {
      const axiosError = error as AxiosError;
      
      if (apiUtils.getErrorStatus(axiosError) === 404) {
        throw new Error(`Record with primary ID ${basePrimaryId} not found`);
      }
      
      if (apiUtils.getErrorStatus(axiosError) === 503) {
        throw new Error('Database connection not available');
      }
      
      const errorMessage = apiUtils.getErrorMessage(axiosError);
      throw new Error(errorMessage || 'Failed to update record');
    }
  },

  /**
   * Delete a base view record
   */
  async deleteBaseViewRecord(basePrimaryId: number): Promise<void> {
    try {
      await api.delete(`/api/v1/lineage/public/base-view/${basePrimaryId}`);
    } catch (error) {
      const axiosError = error as AxiosError;
      
      if (apiUtils.getErrorStatus(axiosError) === 404) {
        throw new Error(`Record with primary ID ${basePrimaryId} not found`);
      }
      
      if (apiUtils.getErrorStatus(axiosError) === 503) {
        throw new Error('Database connection not available');
      }
      
      const errorMessage = apiUtils.getErrorMessage(axiosError);
      throw new Error(errorMessage || 'Failed to delete record');
    }
  },
};