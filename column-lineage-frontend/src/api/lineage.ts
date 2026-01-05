import { apiClient } from './client'
import { ColumnLineage, ApiResponse } from '../types'

export const lineageApi = {
  // Get column lineage data
  getColumnLineage: async (): Promise<ColumnLineage[]> => {
    const response = await apiClient.get<ApiResponse<ColumnLineage[]>>('/api/v1/lineage/columns')
    return response.data.data
  },

  // Analyze view lineage
  analyzeViewLineage: async (viewName: string): Promise<ColumnLineage[]> => {
    const response = await apiClient.post<ApiResponse<ColumnLineage[]>>(
      '/api/v1/lineage/analyze',
      { view_name: viewName }
    )
    return response.data.data
  },

  // Get lineage for specific view
  getViewLineage: async (viewName: string): Promise<ColumnLineage[]> => {
    const response = await apiClient.get<ApiResponse<ColumnLineage[]>>(
      `/api/v1/lineage/views/${encodeURIComponent(viewName)}`
    )
    return response.data.data
  },

  // Search lineage data
  searchLineage: async (query: string): Promise<ColumnLineage[]> => {
    const response = await apiClient.get<ApiResponse<ColumnLineage[]>>(
      '/api/v1/lineage/search',
      { params: { q: query } }
    )
    return response.data.data
  },
}