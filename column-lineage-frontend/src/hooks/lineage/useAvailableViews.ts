import { useQuery } from '@tanstack/react-query';
import { lineageService, ViewInfo } from '@/api/lineageService';

export const useAvailableViews = (
  databaseFilter: string,
  schemaFilter: string,
  limit: number = 100,
  offset: number = 0
) => {
  return useQuery({
    queryKey: ['available-views', databaseFilter, schemaFilter, limit, offset],
    queryFn: () => lineageService.getAvailableViews(databaseFilter, schemaFilter, limit, offset),
    enabled: !!(databaseFilter && schemaFilter), // Only run when both filters are provided
    staleTime: 5 * 60 * 1000, // 5 minutes
    retry: 2,
  });
};