import { useQuery, useMutation, useQueryClient, UseQueryOptions } from '@tanstack/react-query';
import { baseViewService, BaseViewResponse, BaseViewParams, BaseViewCreateRequest, BaseViewUpdateRequest, BaseViewRecord } from '../api/baseViewService';

export const BASE_VIEW_QUERY_KEY = 'baseView';

export interface UseBaseViewOptions extends BaseViewParams {
  enabled?: boolean;
  refetchInterval?: number;
}

export const useBaseView = (
  options: UseBaseViewOptions = {},
  queryOptions?: Omit<UseQueryOptions<BaseViewResponse, Error>, 'queryKey' | 'queryFn'>
) => {
  const { enabled = true, refetchInterval, ...params } = options;

  return useQuery<BaseViewResponse, Error>({
    queryKey: [BASE_VIEW_QUERY_KEY, params],
    queryFn: () => baseViewService.getBaseViewData(params),
    enabled,
    refetchInterval,
    staleTime: 5 * 60 * 1000, // 5 minutes
    retry: (failureCount, error) => {
      // Retry up to 3 times for network errors, but not for 4xx errors
      if (failureCount >= 3) return false;
      if (error.message.includes('404') || error.message.includes('400')) return false;
      return true;
    },
    retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 30000),
    ...queryOptions,
  });
};

// Hook for creating a new base view record
export const useCreateBaseView = () => {
  const queryClient = useQueryClient();

  return useMutation<BaseViewRecord, Error, BaseViewCreateRequest>({
    mutationFn: (request: BaseViewCreateRequest) => baseViewService.createBaseViewRecord(request),
    onSuccess: () => {
      // Invalidate and refetch base view data
      queryClient.invalidateQueries({ queryKey: [BASE_VIEW_QUERY_KEY] });
    },
    onError: (error) => {
      console.error('Failed to create base view record:', error);
    },
  });
};

// Hook for updating a base view record
export const useUpdateBaseView = () => {
  const queryClient = useQueryClient();

  return useMutation<BaseViewRecord, Error, { basePrimaryId: number; request: BaseViewUpdateRequest }>({
    mutationFn: ({ basePrimaryId, request }) => baseViewService.updateBaseViewRecord(basePrimaryId, request),
    onSuccess: () => {
      // Invalidate and refetch base view data
      queryClient.invalidateQueries({ queryKey: [BASE_VIEW_QUERY_KEY] });
    },
    onError: (error) => {
      console.error('Failed to update base view record:', error);
    },
  });
};

// Hook for deleting a base view record
export const useDeleteBaseView = () => {
  const queryClient = useQueryClient();

  return useMutation<void, Error, number>({
    mutationFn: (basePrimaryId: number) => baseViewService.deleteBaseViewRecord(basePrimaryId),
    onSuccess: () => {
      // Invalidate and refetch base view data
      queryClient.invalidateQueries({ queryKey: [BASE_VIEW_QUERY_KEY] });
    },
    onError: (error) => {
      console.error('Failed to delete base view record:', error);
    },
  });
};