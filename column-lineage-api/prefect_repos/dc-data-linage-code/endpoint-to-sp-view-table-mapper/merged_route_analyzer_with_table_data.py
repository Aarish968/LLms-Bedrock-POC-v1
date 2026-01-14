import ast
import os
import json
import builtins
import glob
import re
from typing import List, Dict, Any, Set
from collections import defaultdict

ROUTER_DECORATORS = {
    "get": "GET",
    "post": "POST",
    "put": "PUT",
    "delete": "DELETE",
    "patch": "PATCH",
    "options": "OPTIONS",
    "head": "HEAD",
}

SKIP_FUNCTIONS = {
    "ServiceException", "HTTPException", "SDPLifeCycle", "select", "text",
    "list", "any", "map", "max", "min", "Depends", "setattr"
}

BUILTIN_FUNCTIONS = {
    name for name in dir(builtins)
    if isinstance(getattr(builtins, name), type(abs))
}

SQL_KEYWORDS = {
    # Standard SQL keywords
    "select", "from", "where", "join", "inner", "left", "right", "full", "outer", "on",
    "insert", "into", "values", "update", "set", "delete", "create", "alter", "drop",
    "table", "view", "index", "and", "or", "not", "in", "is", "null", "like", "as",
    "group", "by", "order", "having", "limit", "offset", "union", "distinct", "case",
    "when", "then", "else", "end", "exists", "count", "sum", "avg", "min", "max", "for",
    "if", "with", "primary", "key", "foreign", "references", "constraint",
    # Common CTE/alias/utility words to filter
    "static", "deleted", "the", "super", "temp", "test", "backup", "current", "stg",
    "metrics", "cluster", "clusters", "details", "hdr", "info", "data", "snapshot",
    "report", "object", "json", "parquet", "thoughtspot", "contracts", "booking", "sub",
    "extension", "extensions", "input", "output", "row", "col", "column", "columns"
}

def extract_tables_from_sql(sql_text: str, known_tables: set = None) -> set:
    tables = set()
    known_tables = known_tables or set()

    # Find CTE names: e.g., WITH sub AS (...), contracts AS (...) 
    cte_pattern = r"with\s+([a-zA-Z0-9_]+)\s+as\s*\("  # matches 'with sub as ('
    cte_names = set(re.findall(cte_pattern, sql_text, flags=re.IGNORECASE))

    # Also support chained CTEs: with a as (...), b as (...)
    chained_cte_pattern = r",\s*([a-zA-Z0-9_]+)\s+as\s*\("
    cte_names.update(re.findall(chained_cte_pattern, sql_text, flags=re.IGNORECASE))

    # More precise table extraction patterns - only after specific SQL keywords
    patterns = [
        r'\bFROM\s+([a-zA-Z0-9_]+)(?:\s|$|\)|,)',
        r'\bJOIN\s+([a-zA-Z0-9_]+)(?:\s|$|\)|,)',
        r'\bUPDATE\s+([a-zA-Z0-9_]+)(?:\s|$|\)|,)',
        r'\bINSERT\s+INTO\s+([a-zA-Z0-9_]+)(?:\s|$|\)|,)',
        r'\bMERGE\s+INTO\s+([a-zA-Z0-9_]+)(?:\s|$|\)|,)',
        r'\bDELETE\s+FROM\s+([a-zA-Z0-9_]+)(?:\s|$|\)|,)',
    ]
    
    for pattern in patterns:
        for match in re.findall(pattern, sql_text, re.IGNORECASE):
            table_name = match.strip()
            
            # Handle schema.table format
            if '.' in table_name:
                table_name = table_name.split('.')[-1]  # Take table name part
            
            # Only include if it's in our known tables list OR passes basic validation
            if table_name in known_tables or is_valid_table_candidate(table_name, cte_names):
                tables.add(table_name)
    
    return tables

def is_valid_table_candidate(name: str, cte_names: set) -> bool:
    """Basic validation without hardcoded patterns"""
    if not name or len(name) < 3:
        return False
    
    # Skip if it's a CTE name
    if name in cte_names:
        return False
    
    # Skip SQL keywords
    if name.lower() in SQL_KEYWORDS:
        return False
    
    # Skip pure numbers
    if name.isdigit():
        return False
    
    # Only accept names that look like table names (contain uppercase or underscores)
    if name.isupper() or '_' in name or (name[0].isupper() and len(name) > 4):
        return True
    
    return False

def collect_all_possible_table_names(root_dir="."):
    """
    Scan all .py files for __tablename__ = 'XXX' declarations.
    This gives us the authoritative list of actual table names.
    """
    table_names = set()
    tablename_pattern = re.compile(r'__tablename__\s*=\s*["\']([a-zA-Z0-9_]+)["\']')
    
    for dirpath, _, files in os.walk(root_dir):
        # Skip .venv directory
        if '.venv' in dirpath or '__pycache__' in dirpath:
            continue
        for file in files:
            if file.endswith(".py"):
                path = os.path.join(dirpath, file)
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        content = f.read()
                    # Only collect from __tablename__ declarations - these are definitive
                    for match in tablename_pattern.findall(content):
                        table_names.add(match)
                except Exception:
                    continue
    return table_names

class FixedRouteAnalyzer(ast.NodeVisitor):
    def __init__(self, model_table_map=None, all_known_tables=None, all_function_definitions=None):
        self.router_prefix = ""
        self.routes_info = []
        self.flow_service_aliases = {"flow_service"}
        self.call_graph = defaultdict(set)
        self.defined_functions = set()
        self.current_function = None
        self.proc_calls = defaultdict(list)
        self.table_calls = defaultdict(set)
        self.current_route_info = None
        
        # New: Deep analysis capabilities
        self.function_definitions = all_function_definitions or {}
        self.analyzed_functions = set()
        self.service_methods = defaultdict(set)  # Track service method calls
        self.cross_file_functions = {}  # Store functions from other files

        # Use shared model-to-table mapping
        if model_table_map is not None:
            self.model_table_map = model_table_map
        else:
            self.model_table_map = self._build_model_table_map()

        self.all_known_tables = set(all_known_tables) if all_known_tables is not None else set()

    def _build_model_table_map(self) -> dict:
        """Build model-to-table mapping by scanning ORM files"""
        mapping = {}
        patterns = ["api/**/orm/**/*.py", "**/orm/**/*.py", "**/models/**/*.py"]
        for pattern in patterns:
            for file_path in glob.glob(pattern, recursive=True):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    class_blocks = re.findall(
                        r'class\s+(\w+)[^\n]*:(.*?)(?=^class\s|\Z)',
                        content,
                        re.DOTALL | re.MULTILINE
                    )
                    for class_name, class_body in class_blocks:
                        match = re.search(r'__tablename__\s*=\s*["\']([^"\']+)["\']', class_body)
                        if match:
                            table_name = match.group(1)
                            mapping[class_name] = table_name
                except Exception:
                    continue
        return mapping

    def analyze_called_function(self, func_name: str, depth: int = 0) -> set:
        """Recursively analyze a called function to find its table usage"""
        if depth > 5 or func_name in self.analyzed_functions or func_name in SKIP_FUNCTIONS:
            return set()
        
        if func_name in BUILTIN_FUNCTIONS:
            return set()
            
        self.analyzed_functions.add(func_name)
        tables = set()
        
        # Look for function definition
        func_node = self.function_definitions.get(func_name)
        
        if func_node:
            # Save current context
            prev_function = self.current_function
            prev_route_info = self.current_route_info
            
            self.current_function = func_name
            self.current_route_info = None
            
            # Analyze the function body
            self.generic_visit(func_node)
            
            # Collect tables found in this function
            tables.update(self.table_calls.get(func_name, set()))
            
            # Recursively analyze called functions
            for called_func in self.call_graph.get(func_name, []):
                deep_tables = self.analyze_called_function(called_func, depth + 1)
                tables.update(deep_tables)
            
            # Restore context
            self.current_function = prev_function
            self.current_route_info = prev_route_info
        
        return tables

    def visit_Assign(self, node):
        if isinstance(node.value, ast.Call) and getattr(node.value.func, "id", None) == "APIRouter":
            for kw in node.value.keywords:
                if kw.arg == "prefix":
                    if isinstance(kw.value, ast.Constant):
                        self.router_prefix = kw.value.value
                    elif isinstance(kw.value, ast.Str):
                        self.router_prefix = kw.value.s
        self.generic_visit(node)

    def visit_With(self, node: ast.With):
        for item in node.items:
            if isinstance(item.context_expr, ast.Name) and item.context_expr.id == "flow_service":
                if isinstance(item.optional_vars, ast.Name):
                    self.flow_service_aliases.add(item.optional_vars.id)
        self.generic_visit(node)

    def visit_FunctionDef(self, node):
        func_name = node.name
        self.defined_functions.add(func_name)
        
        # Store function definition for deep analysis
        self.function_definitions[func_name] = node

        route_info = None
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call) and hasattr(decorator.func, "attr"):
                method_lower = decorator.func.attr.lower()
                if method_lower in ROUTER_DECORATORS:
                    path = "/"
                    if decorator.args:
                        arg0 = decorator.args[0]
                        if isinstance(arg0, ast.Constant):
                            path = arg0.value
                        elif isinstance(arg0, ast.Str):
                            path = arg0.s
                    full_path = self.router_prefix + path if self.router_prefix else path
                    route_info = {
                        "method": ROUTER_DECORATORS[method_lower],
                        "path": full_path,
                        "function": func_name,
                        "flow_calls": [],
                        "function_calls": [],
                        "stored_procedures": [],
                        "tables": [],
                        "call_hierarchy": [],
                        "category": "Backend, Frontend"
                    }
                    self.routes_info.append(route_info)
                    break
        prev_function = self.current_function
        prev_route_info = self.current_route_info
        self.current_function = func_name
        self.current_route_info = route_info
        self.generic_visit(node)
        self.current_function = prev_function
        self.current_route_info = prev_route_info

    def  visit_AsyncFunctionDef(self, node):
        self.visit_FunctionDef(node)

    def visit_Call(self, node):
        if not self.current_function:
            self.generic_visit(node)
            return
        base_call_name = self.get_called_name(node.func)
        if base_call_name:
            if base_call_name in BUILTIN_FUNCTIONS or base_call_name in SKIP_FUNCTIONS:
                self.generic_visit(node)
                return
            if isinstance(node.func, ast.Attribute):
                attr_name = node.func.attr
                base = node.func.value
                if isinstance(base, ast.Name) and base.id in self.flow_service_aliases:
                    if self.current_route_info:
                        self.current_route_info["flow_calls"].append(attr_name)
                if attr_name == "bindparams" and isinstance(base, ast.Call):
                    inner_func = self.get_called_name(base.func)
                    if inner_func == "make_stored_proc_statement":
                        self.call_graph[self.current_function].add(inner_func)
                        for kw in node.keywords:
                            if kw.arg == "proc_name":
                                if isinstance(kw.value, ast.Constant):
                                    proc_name = kw.value.value
                                    self.proc_calls[self.current_function].append(proc_name)
                                    if self.current_route_info:
                                        self.current_route_info["stored_procedures"].append(proc_name)
            elif isinstance(node.func, ast.Name):
                self.call_graph[self.current_function].add(base_call_name)
                if self.current_route_info:
                    self.current_route_info["function_calls"].append(base_call_name)
            if isinstance(node.func, ast.Attribute) and node.func.attr == "add_task":
                base = node.func.value
                if isinstance(base, ast.Name) and base.id == "background_tasks":
                    if node.args:
                        first_arg = node.args[0]
                        if isinstance(first_arg, ast.Name):
                            bg_func_name = first_arg.id
                            self.call_graph[self.current_function].add(bg_func_name)
                            if self.current_route_info:
                                self.current_route_info["function_calls"].append(bg_func_name)
                            if bg_func_name == "process_sea_upload":
                                self.proc_calls[self.current_function].append("load_sea_data")
                                if self.current_route_info:
                                    self.current_route_info["stored_procedures"].append("load_sea_data")
                            elif bg_func_name == "process_macd_upload":
                                self.proc_calls[self.current_function].append("load_macd_data")
                                if self.current_route_info:
                                    self.current_route_info["stored_procedures"].append("load_macd_data")
            if base_call_name in [
                "run_stored_procedure",
                "run_v2_stored_procedure",
                "run_put_time_entries_stored_procedure",
                "make_stored_proc_statement"
            ]:
                if base_call_name == "run_put_time_entries_stored_procedure":
                    proc_name_val = "put_user_time_entries"
                    self.proc_calls[self.current_function].append(proc_name_val)
                    if self.current_route_info:
                        self.current_route_info["stored_procedures"].append(proc_name_val)
                for kw in node.keywords:
                    if kw.arg == "proc_name":
                        proc_name_val = self.extract_proc_name(kw.value)
                        if proc_name_val:
                            self.proc_calls[self.current_function].append(proc_name_val)
                            if self.current_route_info:
                                self.current_route_info["stored_procedures"].append(proc_name_val)
                if base_call_name == "make_stored_proc_statement" and node.args:
                    first_arg = node.args[0] if node.args else None
                    if first_arg:
                        proc_name_val = self.extract_proc_name(first_arg)
                        if proc_name_val:
                            self.proc_calls[self.current_function].append(proc_name_val)
                            if self.current_route_info:
                                 self.current_route_info["stored_procedures"].append(proc_name_val)
            if isinstance(node.func, ast.Attribute):
                attr_name = node.func.attr
                base = node.func.value
                if isinstance(base, ast.Name) and base.id == "V2ProcedureNames":
                    self.proc_calls[self.current_function].append(attr_name)
                    if self.current_route_info:
                        self.current_route_info["stored_procedures"].append(attr_name)
            self.detect_database_tables(node)
        self.generic_visit(node)

    def detect_database_tables(self, node):
        if not self.current_function:
            return
        tables = set()
        
        if isinstance(node, ast.Call):
            func_name = self.get_called_name(node.func)
            
            # Check if function name maps to a table
            if func_name and func_name in self.model_table_map:
                table = self.model_table_map[func_name]
                tables.add(table)
            
            # Enhanced: Check for service method calls
            if isinstance(node.func, ast.Attribute):
                attr_name = node.func.attr
                base = node.func.value
                
                # Pattern: service.method_name() or variable.method()
                if isinstance(base, ast.Name):
                    base_name = base.id
                    # Check if this is a service call
                    if 'service' in base_name.lower() or base_name in self.flow_service_aliases:
                        # This is a service method call - analyze it
                        service_method = f"{base_name}.{attr_name}"
                        if service_method in self.function_definitions:
                            service_tables = self.analyze_called_function(service_method)
                            tables.update(service_tables)
                
                # Pattern: SomeService().method_name()
                elif isinstance(base, ast.Call) and isinstance(base.func, ast.Name):
                    service_class = base.func.id
                    if 'service' in service_class.lower():
                        service_method = f"{service_class}.{attr_name}"
                        if service_method in self.function_definitions:
                            service_tables = self.analyze_called_function(service_method)
                            tables.update(service_tables)
            
            # Enhanced: Use comprehensive model reference extraction
            # This will catch select(ClassName), func(ClassName.field), etc.
            for arg in node.args:
                model_tables = self.extract_model_references(arg)
                tables.update(model_tables)
                
                # Also check for SQL strings
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                    sql_text = arg.value
                    if self.looks_like_sql(sql_text):
                        sql_tables = extract_tables_from_sql(sql_text, self.all_known_tables)
                        tables.update(sql_tables)
            
            # Check keyword arguments
            for kw in getattr(node, 'keywords', []):
                model_tables = self.extract_model_references(kw.value)
                tables.update(model_tables)
                
                # Also check for SQL strings in keyword arguments
                if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str):
                    sql_text = kw.value.value
                    if self.looks_like_sql(sql_text):
                        sql_tables = extract_tables_from_sql(sql_text, self.all_known_tables)
                        tables.update(sql_tables)
        
        # Store found tables - but only if they're in our known tables or look like real table names
        for table in tables:
            if table and (table in self.all_known_tables or self.is_likely_real_table(table)):
                self.table_calls[self.current_function].add(table)
                if self.current_route_info and table not in self.current_route_info["tables"]:
                    self.current_route_info["tables"].append(table)

    def is_likely_real_table(self, name: str) -> bool:
        """Final validation to check if a name is likely a real table"""
        if not name or len(name) < 3:
            return False
        
        # Skip SQL keywords
        if name.lower() in SQL_KEYWORDS:
            return False
        
        # Prefer names that follow database table naming conventions:
        # 1. All uppercase with underscores (DC_USER, CONTRACT_DATA)
        # 2. Mixed case with underscores (User_Profile)
        # 3. All uppercase without underscores but longer (CONTRACTS)
        if (name.isupper() and ('_' in name or len(name) > 5)) or \
           ('_' in name and any(c.isupper() for c in name)):
            return True
        
        return False

    def looks_like_sql(self, text: str) -> bool:
        """Check if a string looks like SQL without hardcoded patterns"""
        if not text or len(text) < 10:
            return False
        
        text_upper = text.upper()
        
        # Check for SQL keywords
        sql_keywords = ['SELECT', 'FROM', 'INSERT', 'UPDATE', 'DELETE', 'WITH', 'JOIN', 'WHERE', 'CREATE', 'ALTER', 'DROP']
        keyword_count = sum(1 for keyword in sql_keywords if keyword in text_upper)
        
        # If it has multiple SQL keywords, likely SQL
        if keyword_count >= 2:
            return True
        
        # Check for SQL-like patterns
        sql_patterns = [
            r'\bSELECT\s+.*\bFROM\b',
            r'\bINSERT\s+INTO\b',
            r'\bUPDATE\s+.*\bSET\b',
            r'\bDELETE\s+FROM\b',
            r'\bWITH\s+\w+\s+AS\s*\(',
        ]
        
        for pattern in sql_patterns:
            if re.search(pattern, text_upper):
                return True
        
        return False

    def extract_model_references(self, node) -> set:
        """Recursively extract model class references from any AST node"""
        tables = set()
        
        if isinstance(node, ast.Name) and node.id in self.model_table_map:
            # Direct model reference: ClassName
            table = self.model_table_map[node.id]
            tables.add(table)
        elif isinstance(node, ast.Attribute):
            # Model attribute: ClassName.field
            if isinstance(node.value, ast.Name) and node.value.id in self.model_table_map:
                table = self.model_table_map[node.value.id]
                tables.add(table)
            # Recursively check the base
            tables.update(self.extract_model_references(node.value))
        elif isinstance(node, ast.Call):
            # Function call: might be model constructor or contain model references
            if isinstance(node.func, ast.Name) and node.func.id in self.model_table_map:
                table = self.model_table_map[node.func.id]
                tables.add(table)
            # Check function arguments
            for arg in node.args:
                tables.update(self.extract_model_references(arg))
            # Check keyword arguments
            for kw in node.keywords:
                tables.update(self.extract_model_references(kw.value))
        elif isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            # Collections: [ClassName, OtherClass]
            for elt in node.elts:
                tables.update(self.extract_model_references(elt))
        elif isinstance(node, ast.Dict):
            # Dictionaries: {"key": ClassName}
            for key, value in zip(node.keys, node.values):
                if key:
                    tables.update(self.extract_model_references(key))
                tables.update(self.extract_model_references(value))
        elif hasattr(node, '__dict__'):
            # For any other node type, recursively check all attributes
            for attr_name, attr_value in node.__dict__.items():
                if isinstance(attr_value, list):
                    for item in attr_value:
                        if isinstance(item, ast.AST):
                            tables.update(self.extract_model_references(item))
                elif isinstance(attr_value, ast.AST):
                    tables.update(self.extract_model_references(attr_value))
        
        return tables

    def extract_proc_name(self, node):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        elif isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return self.get_full_attribute_name(node)
        return None

    def get_full_attribute_name(self, node):
        parts = []
        while isinstance(node, ast.Attribute):
            parts.append(node.attr)
            node = node.value
        if isinstance(node, ast.Name):
            parts.append(node.id)
        parts.reverse()
        return ".".join(parts)

    def get_called_name(self, node):
        if isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Call):
            return self.get_called_name(node.func)
        elif isinstance(node, ast.Attribute):
            return node.attr
        return None

    def visit_Constant(self, node):
        if isinstance(node.value, str) and self.current_function:
            sql_text = node.value
            if self.looks_like_sql(sql_text):
                tables = extract_tables_from_sql(sql_text, self.all_known_tables)
                for table in tables:
                    self.table_calls[self.current_function].add(table)
                    if self.current_route_info and table not in self.current_route_info["tables"]:
                        self.current_route_info["tables"].append(table)
        self.generic_visit(node)
    
    def visit_Str(self, node):  # For older Python versions
        """Handle string literals in older AST format"""
        if self.current_function:
            sql_text = node.s
            if self.looks_like_sql(sql_text):
                tables = extract_tables_from_sql(sql_text, self.all_known_tables)
                for table in tables:
                    self.table_calls[self.current_function].add(table)
                    if self.current_route_info and table not in self.current_route_info["tables"]:
                        self.current_route_info["tables"].append(table)
        self.generic_visit(node)

def build_call_hierarchy(func_name: str, call_graph: Dict, proc_calls: Dict,
                         visited: Set[str] = None, depth: int = 0) -> List[Dict]:
    if visited is None:
        visited = set()
    if func_name in visited or depth > 10:
        return []
    visited.add(func_name)
    hierarchy = []
    for called_func in sorted(call_graph.get(func_name, [])):
        call_info = {
            "type": "function_call",
            "name": called_func,
            "depth": depth,
            "children": build_call_hierarchy(called_func, call_graph, proc_calls, visited.copy(), depth + 1)
        }
        hierarchy.append(call_info)
    for proc_name in sorted(set(proc_calls.get(func_name, []))):
        proc_info = {
            "type": "stored_procedure",
            "name": proc_name,
            "depth": depth,
            "children": []
        }
        hierarchy.append(proc_info)
    return hierarchy

def analyze_file(filepath: str, model_table_map=None, all_known_tables=None, all_function_definitions=None) -> Dict[str, Any]:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            source = f.read()
        tree = ast.parse(source, filename=filepath)
        analyzer = FixedRouteAnalyzer(model_table_map, all_known_tables=all_known_tables, all_function_definitions=all_function_definitions)
        analyzer.visit(tree)
        
        # Enhanced: Perform deep analysis for each route
        for route_info in analyzer.routes_info:
            func_name = route_info["function"]
            hierarchy = build_call_hierarchy(func_name, analyzer.call_graph, analyzer.proc_calls)
            route_info["call_hierarchy"] = hierarchy
            
            # Collect stored procedures from all called functions
            all_stored_procedures = set(route_info["stored_procedures"])
            for called_func in analyzer.call_graph.get(func_name, []):
                all_stored_procedures.update(analyzer.proc_calls.get(called_func, []))
            
            # Enhanced: Deep table analysis - follow function calls
            all_tables = set(route_info["tables"])
            
            # Reset analyzed functions for each route to ensure complete analysis
            analyzer.analyzed_functions = set()
            
            # Analyze each called function deeply
            for called_func in analyzer.call_graph.get(func_name, []):
                # Add tables from direct calls
                all_tables.update(analyzer.table_calls.get(called_func, []))
                
                # Perform deep recursive analysis
                deep_tables = analyzer.analyze_called_function(called_func)
                all_tables.update(deep_tables)
            
            # Clean up and deduplicate
            route_info["flow_calls"] = list(set(route_info["flow_calls"]))
            route_info["function_calls"] = list(set(route_info["function_calls"]))
            route_info["stored_procedures"] = list(all_stored_procedures)
            route_info["tables"] = list(all_tables)
            
        return {
            "file_path": filepath,
            "routes": analyzer.routes_info,
            "function_definitions": analyzer.function_definitions
        }
    except Exception as e:
        return {"file_path": filepath, "error": str(e)}

def analyze_directory(folder_path: str) -> Dict[str, Any]:
    results = {}
    all_known_tables = collect_all_possible_table_names(folder_path)
    shared_model_table_map = FixedRouteAnalyzer()._build_model_table_map()
    
    print(f"Found {len(all_known_tables)} known tables: {sorted(list(all_known_tables))[:10]}...")  # Debug info
    
    # First pass: collect all function definitions across all files
    all_function_definitions = {}
    python_files = []
    
    print("First pass: Collecting function definitions...")
    for root, _, files in os.walk(folder_path):
        # Skip .venv directory
        if '.venv' in root or '__pycache__' in root:
            continue
        for file in files:
            if file.endswith(".py"):
                full_path = os.path.join(root, file)
                relative_path = os.path.relpath(full_path, folder_path)
                python_files.append((full_path, relative_path))
                
                # Quick parse to extract function definitions
                try:
                    with open(full_path, "r", encoding="utf-8") as f:
                        source = f.read()
                    tree = ast.parse(source, filename=full_path)
                    
                    # Extract function definitions (including class methods)
                    for node in ast.walk(tree):
                        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                            all_function_definitions[node.name] = node
                        elif isinstance(node, ast.ClassDef):
                            # Extract class methods
                            for class_node in node.body:
                                if isinstance(class_node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                                    method_name = f"{node.name}.{class_node.name}"
                                    all_function_definitions[method_name] = class_node
                except Exception as e:
                    print(f"Error parsing {relative_path}: {e}")
                    continue
    
    print(f"Collected {len(all_function_definitions)} function definitions")
    
    # Second pass: analyze files with complete function context
    print("Second pass: Analyzing with deep function context...")
    for full_path, relative_path in python_files:
        analysis = analyze_file(
            full_path, 
            model_table_map=shared_model_table_map, 
            all_known_tables=all_known_tables,
            all_function_definitions=all_function_definitions
        )
        if "error" not in analysis and analysis.get("routes"):
            results[relative_path] = analysis["routes"]
            print(f"Analyzed {relative_path}: {len(analysis['routes'])} routes")
    
    return results

def main():
    import sys
    folder_path = sys.argv[1] if len(sys.argv) > 1 else r'.'
    print(f"Analyzing directory: {folder_path}")
    try:
        analysis_results = analyze_directory(folder_path)
        simplified_output = {}
        for file_path, routes in analysis_results.items():
            simplified_output[file_path] = []
            for route in routes:
                simplified_route = {
                    "method": route["method"],
                    "path": route["path"],
                    "function": route["function"],
                    "Category": "Backend, Frontend"
                }
                for field in ["flow_calls", "stored_procedures", "tables"]:
                    data = route.get(field)
                    if data:
                        simplified_route[field] = data
                simplified_output[file_path].append(simplified_route)
        output_file = "merged_route_analysis_with_table_data.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(simplified_output, f, indent=2, ensure_ascii=False)
        print(f"Analysis complete! Results saved to {output_file}")
        total_routes = sum(len(routes) for routes in simplified_output.values())
        files_with_tables = sum(1 for routes in simplified_output.values()
                               for route in routes if route.get("tables"))
        print(f"Summary:")
        print(f"  - Total routes: {total_routes}")
        print(f"  - Files analyzed: {len(simplified_output)}")
        print(f"  - Routes with tables: {files_with_tables}")
    except Exception as e:
        print(f"Error during analysis: {e}")
        return 1
    return 0

if __name__ == "__main__":
    exit(main())