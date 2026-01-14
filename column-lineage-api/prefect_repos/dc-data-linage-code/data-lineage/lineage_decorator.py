# Universal Data Lineage Decorator System - Fixed Version
import functools
import json
import time
import uuid
from datetime import datetime
from typing import Any, Dict, Optional, List
import inspect
import logging
import traceback
from pathlib import Path

# Setup lineage logger
def setup_lineage_logger():
    """Setup dedicated logger for data lineage"""
    logger = logging.getLogger('data_lineage')
    
    if not logger.handlers:  # Avoid duplicate handlers
        # Create logs directory if it doesn't exist
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        
        # File handler for lineage events
        handler = logging.FileHandler('logs/data_lineage.log')
        formatter = logging.Formatter('%(asctime)s - LINEAGE - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
        # Console handler for debugging
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(' LINEAGE: %(message)s')
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)
        
        logger.setLevel(logging.INFO)
        logger.propagate = False  # Don't propagate to root logger
    
    return logger

# Global lineage logger
lineage_logger = setup_lineage_logger()

def track_lineage(
    component_type: str = "api",  # "api", "service", "database", "external"
    track_input: bool = True,
    track_output: bool = True,
    sensitive_fields: List[str] = None,
    table_name: str = None,
    operation_type: str = None
):
    """
    Universal decorator that can be applied to ANY function to track lineage
    
    Args:
        component_type: Type of component ("api", "service", "database", "external")
        track_input: Whether to track input parameters
        track_output: Whether to track output data
        sensitive_fields: List of field names to redact from logging
        table_name: Database table name (for database operations)
        operation_type: Type of operation (SELECT, INSERT, UPDATE, DELETE)
    """
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            return await _execute_with_lineage_async(
                func, args, kwargs, component_type, 
                track_input, track_output, sensitive_fields, 
                table_name, operation_type
            )
        
        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            return _execute_with_lineage_sync(
                func, args, kwargs, component_type,
                track_input, track_output, sensitive_fields,
                table_name, operation_type
            )
        
        # Return appropriate wrapper based on function type
        if inspect.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator

async def _execute_with_lineage_async(func, args, kwargs, component_type, track_input, 
                                     track_output, sensitive_fields, table_name, 
                                     operation_type):
    """Execute async function with automatic lineage tracking"""
    
    # Generate event ID and extract trace ID
    event_id = str(uuid.uuid4())
    trace_id = _extract_trace_id(args, kwargs)
    
    # Extract function metadata
    function_name = func.__name__
    module_name = func.__module__
    file_path = inspect.getfile(func)
    line_number = inspect.getsourcelines(func)[1] if hasattr(func, '__code__') else 0
    
    # Extract additional context
    context = _extract_context(args, kwargs)
    
    # Prepare input data
    input_data = {}
    if track_input:
        input_data = _sanitize_data({
            'args': _serialize_args(args),
            'kwargs': _serialize_kwargs(kwargs),
            'context': context
        }, sensitive_fields)
    
    # Log function entry
    start_time = time.time()
    entry_event = {
        'event_id': event_id,
        'trace_id': trace_id,
        'event_type': f'{component_type}_function_entry',
        'component_type': component_type,
        'function_name': function_name,
        'module_name': module_name,
        'file_path': file_path,
        'line_number': line_number,
        'input_data': input_data,
        'metadata': {
            'table_name': table_name,
            'operation_type': operation_type,
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
        
        # Prepare output data
        output_data = {}
        if track_output:
            output_data = _sanitize_data(_serialize_result(result), sensitive_fields)
        
        # Log function success
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
                'table_name': table_name,
                'operation_type': operation_type,
                'rows_affected': _extract_rows_affected(result),
                'user_id': context.get('user_id'),
                'engagement_id': context.get('engagement_id')
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
            'error_traceback': traceback.format_exc(),
            'execution_time_ms': execution_time,
            'metadata': {
                'table_name': table_name,
                'operation_type': operation_type,
                'user_id': context.get('user_id'),
                'engagement_id': context.get('engagement_id')
            },
            'timestamp': datetime.utcnow().isoformat(),
            'status': 'error'
        }
        
        _log_lineage_event(error_event)
        
        raise

def _execute_with_lineage_sync(func, args, kwargs, component_type, track_input, 
                              track_output, sensitive_fields, table_name, 
                              operation_type):
    """Execute sync function with automatic lineage tracking"""
    
    # Generate event ID and extract trace ID
    event_id = str(uuid.uuid4())
    trace_id = _extract_trace_id(args, kwargs)
    
    # Extract function metadata
    function_name = func.__name__
    module_name = func.__module__
    file_path = inspect.getfile(func)
    line_number = inspect.getsourcelines(func)[1] if hasattr(func, '__code__') else 0
    
    # Extract additional context
    context = _extract_context(args, kwargs)
    
    # Prepare input data
    input_data = {}
    if track_input:
        input_data = _sanitize_data({
            'args': _serialize_args(args),
            'kwargs': _serialize_kwargs(kwargs),
            'context': context
        }, sensitive_fields)
    
    # Log function entry
    start_time = time.time()
    entry_event = {
        'event_id': event_id,
        'trace_id': trace_id,
        'event_type': f'{component_type}_function_entry',
        'component_type': component_type,
        'function_name': function_name,
        'module_name': module_name,
        'file_path': file_path,
        'line_number': line_number,
        'input_data': input_data,
        'metadata': {
            'table_name': table_name,
            'operation_type': operation_type,
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
        
        # Prepare output data
        output_data = {}
        if track_output:
            output_data = _sanitize_data(_serialize_result(result), sensitive_fields)
        
        # Log function success
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
                'table_name': table_name,
                'operation_type': operation_type,
                'rows_affected': _extract_rows_affected(result),
                'user_id': context.get('user_id'),
                'engagement_id': context.get('engagement_id')
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
            'error_traceback': traceback.format_exc(),
            'execution_time_ms': execution_time,
            'metadata': {
                'table_name': table_name,
                'operation_type': operation_type,
                'user_id': context.get('user_id'),
                'engagement_id': context.get('engagement_id')
            },
            'timestamp': datetime.utcnow().isoformat(),
            'status': 'error'
        }
        
        _log_lineage_event(error_event)
        
        raise

def _extract_trace_id(args, kwargs) -> str:
    """Extract trace ID from function arguments"""
    # Try to find trace_id in kwargs
    if 'trace_id' in kwargs:
        return kwargs['trace_id']
    
    # Try to find Request object in args (for FastAPI endpoints)
    for arg in args:
        if hasattr(arg, 'headers'):  # FastAPI Request object
            trace_id = arg.headers.get('x-trace-id')
            if trace_id:
                return trace_id
    
    # Try to find trace_id in request state
    for arg in args:
        if hasattr(arg, 'state') and hasattr(arg.state, 'trace_id'):
            return arg.state.trace_id
    
    # Generate new trace ID if not found
    return str(uuid.uuid4())

def _extract_context(args, kwargs) -> Dict[str, Any]:
    """Extract context information from function arguments"""
    context = {}
    
    # Try to extract from FastAPI Request object
    for arg in args:
        if hasattr(arg, 'headers'):  # FastAPI Request object
            headers = dict(arg.headers)
            context.update({
                'user_id': headers.get('x-user-id'),
                'engagement_id': headers.get('x-engagement-id'),
                'request_id': headers.get('x-request-id'),
                'user_agent': headers.get('user-agent'),
                'endpoint': str(arg.url.path) if hasattr(arg, 'url') else None,
                'method': getattr(arg, 'method', None)
            })
            break
    
    # Extract from common parameter names
    common_fields = ['user_id', 'engagement_id', 'canvas_id', 'request_id']
    for field in common_fields:
        if field in kwargs:
            context[field] = kwargs[field]
    
    return context

def _serialize_args(args) -> List[Any]:
    """Serialize function arguments"""
    serialized = []
    for i, arg in enumerate(args):
        try:
            # Skip complex objects like Request, Session, etc.
            if hasattr(arg, '__dict__') and any(cls_name in str(type(arg)) 
                                              for cls_name in ['Request', 'Session', 'Connection']):
                serialized.append(f'<{type(arg).__name__}>')
            elif isinstance(arg, (str, int, float, bool, type(None))):
                serialized.append(arg)
            elif isinstance(arg, (list, dict)):
                # Limit size of collections
                if isinstance(arg, list) and len(arg) > 5:
                    serialized.append(arg[:5] + ['...truncated'])
                elif isinstance(arg, dict) and len(arg) > 10:
                    limited_dict = dict(list(arg.items())[:10])
                    limited_dict['...truncated'] = f'{len(arg) - 10} more items'
                    serialized.append(limited_dict)
                else:
                    serialized.append(arg)
            else:
                serialized.append(f'<{type(arg).__name__}>')
        except Exception:
            serialized.append(f'<non-serializable-arg-{i}>')
    return serialized

def _serialize_kwargs(kwargs) -> Dict[str, Any]:
    """Serialize function keyword arguments"""
    serialized = {}
    for key, value in kwargs.items():
        try:
            if isinstance(value, (str, int, float, bool, type(None))):
                serialized[key] = value
            elif isinstance(value, (list, dict)):
                # Limit size of collections
                if isinstance(value, list) and len(value) > 5:
                    serialized[key] = value[:5] + ['...truncated']
                elif isinstance(value, dict) and len(value) > 10:
                    limited_dict = dict(list(value.items())[:10])
                    limited_dict['...truncated'] = f'{len(value) - 10} more items'
                    serialized[key] = limited_dict
                else:
                    serialized[key] = value
            elif hasattr(value, '__dict__'):
                serialized[key] = f'<{type(value).__name__}>'
            else:
                serialized[key] = str(type(value))
        except Exception:
            serialized[key] = f'<non-serializable>'
    return serialized

def _serialize_result(result) -> Any:
    """Serialize function result"""
    try:
        if result is None:
            return None
        elif isinstance(result, (str, int, float, bool)):
            return result
        elif isinstance(result, dict):
            # Limit dict size
            if len(result) > 10:
                limited_dict = dict(list(result.items())[:10])
                limited_dict['...truncated'] = f'{len(result) - 10} more items'
                return limited_dict
            return result
        elif isinstance(result, (list, tuple)):
            # Limit list size and serialize items
            if len(result) > 5:
                return [_serialize_result(item) for item in result[:5]] + ['...truncated']
            return [_serialize_result(item) for item in result]
        elif hasattr(result, '__dict__'):
            # For objects with __dict__, return a summary
            obj_dict = result.__dict__
            if len(obj_dict) > 5:
                limited_dict = dict(list(obj_dict.items())[:5])
                limited_dict['...truncated'] = f'{len(obj_dict) - 5} more fields'
                return limited_dict
            return obj_dict
        else:
            return f'<{type(result).__name__}>'
    except Exception:
        return '<non-serializable-result>'

def _extract_rows_affected(result) -> Optional[int]:
    """Extract number of rows affected from database result"""
    try:
        if hasattr(result, 'rowcount'):
            return result.rowcount
        elif hasattr(result, 'rows_affected'):
            return result.rows_affected
        elif isinstance(result, (list, tuple)):
            return len(result)
        else:
            return None
    except Exception:
        return None

def _sanitize_data(data: Any, sensitive_fields: List[str] = None) -> Any:
    """Remove sensitive data from lineage tracking"""
    if not sensitive_fields:
        sensitive_fields = [
            'password', 'token', 'secret', 'key', 'auth', 'authorization',
            'cookie', 'session', 'csrf', 'api_key', 'access_token', 'refresh_token'
        ]
    
    if isinstance(data, dict):
        sanitized = {}
        for key, value in data.items():
            if any(sensitive in key.lower() for sensitive in sensitive_fields):
                sanitized[key] = '***REDACTED***'
            else:
                sanitized[key] = _sanitize_data(value, sensitive_fields)
        return sanitized
    elif isinstance(data, (list, tuple)):
        return [_sanitize_data(item, sensitive_fields) for item in data]
    else:
        return data

def _log_lineage_event(event_data: Dict[str, Any]):
    """Log lineage event to file and potentially database"""
    try:
        # Log to file
        lineage_logger.info(json.dumps(event_data, default=str))
        
        # TODO: Also store in database for querying
        # This would be implemented based on your database setup
        # _store_in_database(event_data)
        
    except Exception as e:
        # Don't let lineage logging break the application
        print(f"Failed to log lineage event: {e}")

def _store_in_database(event_data: Dict[str, Any]):
    """Store lineage event in database for querying (TODO: Implement)"""
    # This would insert into a lineage table like:
    # INSERT INTO dc_data_lineage (trace_id, event_type, function_name, ...)
    # VALUES (?, ?, ?, ...)
    pass

# Utility functions for querying lineage
def get_trace_lineage(trace_id: str) -> List[Dict[str, Any]]:
    """Get all lineage events for a specific trace ID"""
    events = []
    try:
        with open('logs/data_lineage.log', 'r') as f:
            for line in f:
                try:
                    # Parse log line to extract JSON
                    if ' - LINEAGE - ' in line:
                        json_part = line.split(' - LINEAGE - ', 1)[1].strip()
                        event = json.loads(json_part)
                        if event.get('trace_id') == trace_id:
                            events.append(event)
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        pass
    
    return sorted(events, key=lambda x: x.get('timestamp', ''))

def get_function_lineage(function_name: str, limit: int = 100) -> List[Dict[str, Any]]:
    """Get recent lineage events for a specific function"""
    events = []
    try:
        with open('logs/data_lineage.log', 'r') as f:
            for line in f:
                try:
                    if ' - LINEAGE - ' in line:
                        json_part = line.split(' - LINEAGE - ', 1)[1].strip()
                        event = json.loads(json_part)
                        if event.get('function_name') == function_name:
                            events.append(event)
                            if len(events) >= limit:
                                break
                except json.JSONDecodeError:
                    continue
    except FileNotFoundError:
        pass
    
    return sorted(events, key=lambda x: x.get('timestamp', ''), reverse=True)

def print_trace_summary(trace_id: str):
    """Print a human-readable summary of a trace"""
    events = get_trace_lineage(trace_id)
    
    if not events:
        print(f"No events found for trace ID: {trace_id}")
        return
    
    print(f"\n Trace Summary: {trace_id}")
    print("=" * 60)
    
    for i, event in enumerate(events, 1):
        status_icon = "" if event.get('status') == 'success' else "" if event.get('status') == 'error' else ""
        execution_time = event.get('execution_time_ms', 0)
        
        print(f"{i}. {status_icon} {event.get('component_type', '').upper()}: {event.get('function_name', 'unknown')}")
        print(f"   Time: {execution_time:.2f}ms | Module: {event.get('module_name', 'unknown')}")
        
        if event.get('error_message'):
            print(f"   Error: {event.get('error_message')}")
        
        if event.get('metadata', {}).get('table_name'):
            print(f"   Table: {event['metadata']['table_name']} | Operation: {event['metadata'].get('operation_type', 'unknown')}")
    
    total_time = sum(e.get('execution_time_ms', 0) for e in events if e.get('status') == 'success')
    print(f"\nTotal execution time: {total_time:.2f}ms")
    print(f"Total events: {len(events)}")

# Export main decorator and utility functions
__all__ = [
    'track_lineage',
    'get_trace_lineage', 
    'get_function_lineage',
    'print_trace_summary'
]