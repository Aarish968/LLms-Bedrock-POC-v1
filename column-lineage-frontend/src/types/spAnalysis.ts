/**
 * Stored Procedure Analysis Types
 * TypeScript definitions for stored procedure analysis API
 */

export enum SPJobStatus {
  PENDING = 'PENDING',
  RUNNING = 'RUNNING',
  COMPLETED = 'COMPLETED',
  FAILED = 'FAILED',
  CANCELLED = 'CANCELLED',
}

export enum SPLanguage {
  SQL = 'SQL',
  PYTHON = 'PYTHON',
  MIXED = 'MIXED',
}

export interface TableColumnRelationship {
  table_name: string;
  column_name: string;
  relationship_types: string;
}

export interface StoredProcedureAnalysis {
  sp_name: string;
  sp_schema: string;
  sp_language: string;
  relationships: TableColumnRelationship[];
  variables_detected: Record<string, string>;
  temp_tables_created: string[];
  cursors_detected: string[];
}

export interface SPAnalysisRequest {
  sf_environment?: string;
  max_workers?: number;
  resume_from_partial?: boolean;
  procedure_names?: string[];
}

export interface SingleProcedureRequest {
  procedure_name: string;
  procedure_definition: string;
  procedure_schema: string;
}

export interface SPAnalysisResponse {
  job_id: string;
  status: SPJobStatus;
  message: string;
  results_url: string;
  started_at: string;
}

export interface SPAnalysisJob {
  job_id: string;
  status: SPJobStatus;
  sf_environment: string;
  max_workers: number;
  total_procedures: number;
  completed_procedures: number;
  failed_procedures: number;
  result_file?: string;
  error_message?: string;
  started_at: string;
  completed_at?: string;
  request_params: Record<string, any>;
}

export interface SPResultsResponse {
  job_id: string;
  status: SPJobStatus;
  total_procedures: number;
  total_relationships: number;
  unique_tables: number;
  result_file?: string;
  download_url?: string;
  summary: Record<string, any>;
}

export interface ProcedureInfo {
  name: string;
  procedure_schema: string;
  definition_length: number;
}

export interface ProcedureListResponse {
  count: number;
  procedures: ProcedureInfo[];
}

export interface SPAnalysisJobsResponse {
  jobs: SPAnalysisJob[];
  total: number;
}