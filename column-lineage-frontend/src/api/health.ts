import { apiClient } from './client'
import { HealthStatus, DetailedHealthStatus } from '../types'

export const healthApi = {
  // Basic health check
  getHealth: async (): Promise<HealthStatus> => {
    const response = await apiClient.get('/health')
    return response.data
  },

  // Detailed health check
  getDetailedHealth: async (): Promise<DetailedHealthStatus> => {
    const response = await apiClient.get('/health/detailed')
    return response.data
  },

  // Readiness check
  getReadiness: async (): Promise<{ status: string; mode: string }> => {
    const response = await apiClient.get('/health/ready')
    return response.data
  },

  // Liveness check
  getLiveness: async (): Promise<{ status: string }> => {
    const response = await apiClient.get('/health/live')
    return response.data
  },
}