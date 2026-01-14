"""
Runtime Data Lineage Decorator - Extracts metadata dynamically during execution
"""
import functools
import inspect
import json
import logging
import re
import time
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from .lineage_decorator import (
    _extract_context,
    _extract_trace_id,
    _log_lineage_event,
    _sanitize_data,
    _serialize_args,
    _serialize_kwargs,
    _serialize_result
)

logger = logging.getLogger("api")


def extract_stored_procedure_name_from_execution(func: Callable, args: tuple, kwargs: dict) -> Optional[str]:
    """
    Extract stored procedure name by inspecting the actual function execution
    """
    try:
        # Get the function source code
        source = inspect.getsource(func)
        
        # Look for stored procedure patterns in the source
        patterns = [
            r'proc_name\s*=\s*["\']([^"\']+)["\']',
            r'make_stored_proc_statement.*proc_name\s*=\s*["\']([^"\']+)["\']',
            r'CALL\s+IDENTIFIER\(\s*["\']([^"\']+)["\']',
            r'bindparams\(\s*proc_name\s*=\s*["\']([^"\']+)["\']'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, source, re.IGNORECASE | re.MULTILINE)
            if match:
                return match.group(1)
                
    except Exception as e:
        logger.debug(f"Could not extract stored procedure name from {func.__name__}: {e}")
    
    return None


def query_stored_procedure_metadata(session, proc_name: str) -> Dict[str, Any]:
    """
    Query the database to get actual metadata about what tables the stored procedure affects
    """
    try:
        # Query to get stored procedure definition and analyze it
        query = f"""
        SELECT GET_DDL('PROCEDURE', '{proc_name.upper()}') as procedure_ddl
        """
        
        result = session.execute(query).scalar()
        
        if result:
            # Parse the DDL to extract table names and operations
            tables_affected = extract_tables_from_ddl(result)
            operations = extract_operations_from_ddl(result)
            
            return {
                'tables_affected': tables_affected,
                'operations': operations,
                'procedure_ddl': result
            }
            
    except Exception as e:
        logger.debug(f"Could not query metadata for procedure {proc_name}: {e}")
    
    return {}


def extract_tables_from_ddl(ddl: str) -> List[str]:
    """
    Extract table names from stored procedure DDL
    """
    tables = set()
    
    # Patterns to find table references
    table_patterns = [
        r'FROM\s+([A-Z_][A-Z0-9_]*)',
        r'JOIN\s+([A-Z_][A-Z0-9_]*)',
        r'INSERT\s+INTO\s+([A-Z_][A-Z0-9_]*)',
        r'UPDATE\s+([A-Z_][A-Z0-9_]*)',
        r'DELETE\s+FROM\s+([A-Z_][A-Z0-9_]*)',
        r'MERGE\s+INTO\s+([A-Z_][A-Z0-9_]*)',
        r'CREATE.*TABLE\s+([A-Z_][A-Z0-9_]*)',
    ]
    
    for pattern in table_patterns:
        matches = re.findall(pattern, ddl, re.IGNORECASE | re.MULTILINE)
        tables.update(matches)
    
    return list(tables)


def extract_operations_from_ddl(ddl: str) -> List[str]:
    """
    Extract operation types from stored procedure DDL
    """
    operations = set()
    
    operation_patterns = [
        (r'\bINSERT\b', 'INSERT'),
        (r'\bUPDATE\b', 'UPDATE'),
        (r'\bDELETE\b', 'DELETE'),
        (r'\bSELECT\b', 'SELECT'),
        (r'\bMERGE\b', 'MERGE'),
        (r'\bCREATE\b', 'CREATE'),
        (r'\bDROP\b', 'DROP'),
    ]
    
    for pattern, operation in operation_patterns:
        if re.search(pattern, ddl, re.IGNORECASE):
            operations.add(operation)
    
    return list(operations)


def extract_metadata_from_result(result: Any) -> Dict[str, Any]:
    """
    Extract metadata from stored procedure result if it contains useful information
    """
    metadata = {}
    
    try:
        if hasattr(result, 'message'):
            # Some stored procedures return structured results
            if 'SUCCESS' in str(result.message):
                metadata['execution_status'] = 'SUCCESS'
            else:
                metadata['execution_status'] = 'FAILED'
                
        if hasattr(result, 'rowcount') and result.rowcount:
            metadata['rows_affected'] = result.rowcount
            
        # If result is a dict-like object, extract useful metadata
        if isinstance(result, dict):
            metadata.update({
                k: v for k, v in result.items() 
                if k in ['rowcount', 'sqlid', 'message', 'success', 'code']
            })
            
    except Exception as e:
        logger.debug(f"Could not extract metadata from result: {e}")
    
    return metadata


def track_lineage_runtime(
    component_type: str = "api",
    track_input: bool = True,
    track_output: bool = True,
    sensitive_fields: List[str] = None,
    extract_from_db: bool = True
):
    """
    Runtime lineage decorator that extracts metadata during actual execution
    """
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await _execute_with_runtime_lineage_async(
                func, args, kwargs, component_type,
                track_input, track_output, sensitive_fields, extract_from_db
            )
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            return _execute_with_runtime_lineage_sync(
                func, args, kwargs, component_type,
                track_input, track_output, sensitive_fields, extract_from_db
            )
        
        # Return appropriate wrapper based on function type
        if inspect.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


async def _execute_with_runtime_lineage_async(func, args, kwargs, component_type, 
                                            track_input, track_output, sensitive_fields, extract_from_db):
    """Execute async function with runtime lineage tracking"""
    
    # Generate event ID and extract trace ID
    event_id = str(uuid.uuid4())
    trace_id = _extract_trace_id(args, kwargs)
    
    # Extract function metadata
    function_name = func.__name__
    module_name = func.__module__
    file_path = inspect.getfile(func)
    
    # Extract additional context
    context = _extract_context(args, kwargs)
    
    # Try to extract stored procedure name before execution
    proc_name = extract_stored_procedure_name_from_execution(func, args, kwargs)
    
    # Get session from args/kwargs for database queries
    session = None
    for arg in args:
        if hasattr(arg, 'execute'):  # Likely a database session
            session = arg
            break
    
    # Query database metadata if we have a session and procedure name
    db_metadata = {}
    if session and proc_name and extract_from_db:
        db_metadata = query_stored_procedure_metadata(session, proc_name)
    
    # Prepare input data
    input_data = {}
    if track_input:
        input_data = _sanitize_data({
            'args': _serialize_args(args),
            'kwargs': _serialize_kwargs(kwargs),
            'context': context,
            'detected_procedure': proc_name
        }, sensitive_fields)
    
    # Log function entry with initial metadata
    start_time = time.time()
    entry_event = {
        'event_id': event_id,
        'trace_id': trace_id,
        'event_type': f'{component_type}_function_entry',
        'component_type': component_type,
        'function_name': function_name,
        'module_name': module_name,
        'file_path': file_path,
        'input_data': input_data,
        'metadata': {
            'stored_procedure_name': proc_name,
            'tables_affected': db_metadata.get('tables_affected', []),
            'operations_detected': db_metadata.get('operations', []),
            'user_id': context.get('user_id'),
            'engagement_id': context.get('engagement_id'),
            'request_id': context.get('request_id')
        },
        'timestamp': datetime.utcnow().isoformat(),
        'status': 'started'
    }
    
    _log_lineage_event(entry_event)
    
    try:
        # Execute the async function
        result = await func(*args, **kwargs)
        
        # Calculate execution time
        execution_time = (time.time() - start_time) * 1000
        
        # Extract metadata from the actual result
        result_metadata = extract_metadata_from_result(result)
        
        # Prepare output data
        output_data = {}
        if track_output:
            output_data = _sanitize_data(_serialize_result(result), sensitive_fields)
        
        # Determine primary table and operation from runtime analysis
        primary_table = None
        primary_operation = None
        
        if db_metadata.get('tables_affected'):
            # Use the first table as primary, or look for main target tables
            tables = db_metadata['tables_affected']
            # Prioritize certain table patterns
            for table in tables:
                if any(pattern in table.upper() for pattern in ['_HDR', '_CORE', '_SCHEDULED', '_OWED']):
                    primary_table = table
                    break
            if not primary_table:
                primary_table = tables[0]
        
        if db_metadata.get('operations'):
            operations = db_metadata['operations']
            # Prioritize write operations over read operations
            priority_ops = ['INSERT', 'UPDATE', 'DELETE', 'MERGE', 'CREATE']
            for op in priority_ops:
                if op in operations:
                    primary_operation = op
                    break
            if not primary_operation:
                primary_operation = operations[0]
        
        # Log function success with runtime metadata
        success_event = {
            'event_id': event_id,
            'trace_id': trace_id,
            'event_type': f'{component_type}_function_success',
            'component_type': component_type,
            'function_name': function_name,
            'module_name': module_name,
            'output_data': output_data,
            'execution_time_ms': execution_time,
            'metadata': {
                'stored_procedure_name': proc_name,
                'table_name': primary_table,  # Dynamically determined
                'operation_type': primary_operation,  # Dynamically determined
                'tables_affected': db_metadata.get('tables_affected', []),
                'operations_performed': db_metadata.get('operations', []),
                'rows_affected': result_metadata.get('rows_affected'),
                'execution_status': result_metadata.get('execution_status'),
                'user_id': context.get('user_id'),
                'engagement_id': context.get('engagement_id'),
                **result_metadata  # Include all result metadata
            },
            'timestamp': datetime.utcnow().isoformat(),
            'status': 'success'
        }
        
        _log_lineage_event(success_event)
        
        return result
        
    except Exception as e:
        # Calculate execution time
        execution_time = (time.time() - start_time) * 1000
        
        # Log function error
        error_event = {
            'event_id': event_id,
            'trace_id': trace_id,
            'event_type': f'{component_type}_function_error',
            'component_type': component_type,
            'function_name': function_name,
            'module_name': module_name,
            'error_message': str(e),
            'error_type': type(e).__name__,
            'execution_time_ms': execution_time,
            'metadata': {
                'stored_procedure_name': proc_name,
                'tables_affected': db_metadata.get('tables_affected', []),
                'user_id': context.get('user_id'),
                'engagement_id': context.get('engagement_id')
            },
            'timestamp': datetime.utcnow().isoformat(),
            'status': 'error'
        }
        
        _log_lineage_event(error_event)
        
        raise


def _execute_with_runtime_lineage_sync(func, args, kwargs, component_type,
                                     track_input, track_output, sensitive_fields, extract_from_db):
    """Execute sync function with runtime lineage tracking"""
    
    # Generate event ID and extract trace ID
    event_id = str(uuid.uuid4())
    trace_id = _extract_trace_id(args, kwargs)
    
    # Extract function metadata
    function_name = func.__name__
    module_name = func.__module__
    file_path = inspect.getfile(func)
    
    # Extract additional context
    context = _extract_context(args, kwargs)
    
    # Try to extract stored procedure name before execution
    proc_name = extract_stored_procedure_name_from_execution(func, args, kwargs)
    
    # Get session from args/kwargs for database queries
    session = None
    for arg in args:
        if hasattr(arg, 'execute'):  # Likely a database session
            session = arg
            break
    
    # Query database metadata if we have a session and procedure name
    db_metadata = {}
    if session and proc_name and extract_from_db:
        db_metadata = query_stored_procedure_metadata(session, proc_name)
    
    # Prepare input data
    input_data = {}
    if track_input:
        input_data = _sanitize_data({
            'args': _serialize_args(args),
            'kwargs': _serialize_kwargs(kwargs),
            'context': context,
            'detected_procedure': proc_name
        }, sensitive_fields)
    
    # Log function entry with initial metadata
    start_time = time.time()
    entry_event = {
        'event_id': event_id,
        'trace_id': trace_id,
        'event_type': f'{component_type}_function_entry',
        'component_type': component_type,
        'function_name': function_name,
        'module_name': module_name,
        'file_path': file_path,
        'input_data': input_data,
        'metadata': {
            'stored_procedure_name': proc_name,
            'tables_affected': db_metadata.get('tables_affected', []),
            'operations_detected': db_metadata.get('operations', []),
            'user_id': context.get('user_id'),
            'engagement_id': context.get('engagement_id'),
            'request_id': context.get('request_id')
        },
        'timestamp': datetime.utcnow().isoformat(),
        'status': 'started'
    }
    
    _log_lineage_event(entry_event)
    
    try:
        # Execute the sync function
        result = func(*args, **kwargs)
        
        # Calculate execution time
        execution_time = (time.time() - start_time) * 1000
        
        # Extract metadata from the actual result
        result_metadata = extract_metadata_from_result(result)
        
        # Prepare output data
        output_data = {}
        if track_output:
            output_data = _sanitize_data(_serialize_result(result), sensitive_fields)
        
        # Determine primary table and operation from runtime analysis
        primary_table = None
        primary_operation = None
        
        if db_metadata.get('tables_affected'):
            # Use the first table as primary, or look for main target tables
            tables = db_metadata['tables_affected']
            # Prioritize certain table patterns
            for table in tables:
                if any(pattern in table.upper() for pattern in ['_HDR', '_CORE', '_SCHEDULED', '_OWED']):
                    primary_table = table
                    break
            if not primary_table:
                primary_table = tables[0]
        
        if db_metadata.get('operations'):
            operations = db_metadata['operations']
            # Prioritize write operations over read operations
            priority_ops = ['INSERT', 'UPDATE', 'DELETE', 'MERGE', 'CREATE']
            for op in priority_ops:
                if op in operations:
                    primary_operation = op
                    break
            if not primary_operation:
                primary_operation = operations[0]
        
        # Log function success with runtime metadata
        success_event = {
            'event_id': event_id,
            'trace_id': trace_id,
            'event_type': f'{component_type}_function_success',
            'component_type': component_type,
            'function_name': function_name,
            'module_name': module_name,
            'output_data': output_data,
            'execution_time_ms': execution_time,
            'metadata': {
                'stored_procedure_name': proc_name,
                'table_name': primary_table,  # Dynamically determined
                'operation_type': primary_operation,  # Dynamically determined
                'tables_affected': db_metadata.get('tables_affected', []),
                'operations_performed': db_metadata.get('operations', []),
                'rows_affected': result_metadata.get('rows_affected'),
                'execution_status': result_metadata.get('execution_status'),
                'user_id': context.get('user_id'),
                'engagement_id': context.get('engagement_id'),
                **result_metadata  # Include all result metadata
            },
            'timestamp': datetime.utcnow().isoformat(),
            'status': 'success'
        }
        
        _log_lineage_event(success_event)
        
        return result
        
    except Exception as e:
        # Calculate execution time
        execution_time = (time.time() - start_time) * 1000
        
        # Log function error
        error_event = {
            'event_id': event_id,
            'trace_id': trace_id,
            'event_type': f'{component_type}_function_error',
            'component_type': component_type,
            'function_name': function_name,
            'module_name': module_name,
            'error_message': str(e),
            'error_type': type(e).__name__,
            'execution_time_ms': execution_time,
            'metadata': {
                'stored_procedure_name': proc_name,
                'tables_affected': db_metadata.get('tables_affected', []),
                'user_id': context.get('user_id'),
                'engagement_id': context.get('engagement_id')
            },
            'timestamp': datetime.utcnow().isoformat(),
            'status': 'error'
        }
        
        _log_lineage_event(error_event)
        
        raise


# Convenience decorators
def track_stored_procedure_runtime(**kwargs):
    """Track stored procedure calls with runtime metadata extraction"""
    return track_lineage_runtime(component_type="database", **kwargs)


def track_api_runtime(**kwargs):
    """Track API endpoints with runtime metadata extraction"""
    return track_lineage_runtime(component_type="api", **kwargs)