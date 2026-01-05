import { apiClient } from './client'
import { IUser } from '@/domain/Users'

export const usersApi = {
  // Get current user info
  getCurrentUser: async (): Promise<IUser> => {
    const response = await apiClient.get('/api/v1/users/me')
    return response.data
  },

  // Get user by ID
  getUserById: async (userId: number): Promise<IUser> => {
    const response = await apiClient.get(`/api/v1/users/${userId}`)
    return response.data
  },
}

// Helper functions for authentication
export const fetchWhoAmI = async (): Promise<number> => {
  try {
    const user = await usersApi.getCurrentUser()
    return user.user_id
  } catch (error) {
    console.error('Failed to fetch user info:', error)
    throw error
  }
}

export const clearWhoAmI = async (): Promise<void> => {
  localStorage.removeItem('jwt')
  localStorage.removeItem('authToken')
}