#!/usr/bin/env python3
import ast, os, json, builtins, glob, re
from typing import Dict, Any, Set
from collections import defaultdict

ROUTER_DECORATORS = {"get": "GET", "post": "POST", "put": "PUT", "delete": "DELETE", "patch": "PATCH", "options": "OPTIONS", "head": "HEAD"}
SKIP_FUNCTIONS = {"ServiceException", "HTTPException", "SDPLifeCycle", "select", "text", "list", "any", "map", "max", "min", "Depends", "setattr"}
BUILTIN_FUNCTIONS = {name for name in dir(builtins) if isinstance(getattr(builtins, name), type(abs))}
SQL_KEYWORDS = {"select", "from", "where", "join", "inner", "left", "right", "full", "outer", "on", "insert", "into", "values", "update", "set", "delete", "create", "alter", "drop", "table", "view", "index", "and", "or", "not", "in", "is", "null", "like", "as", "group", "by", "order", "having", "limit", "offset", "union", "distinct", "case", "when", "then", "else", "end", "exists", "count", "sum", "avg", "min", "max", "for", "if", "with", "primary", "key", "foreign", "references", "constraint", "static", "deleted", "the", "super", "temp", "test", "backup", "current", "stg", "metrics", "cluster", "clusters", "details", "hdr", "info", "data", "snapshot", "report", "object", "json", "parquet", "thoughtspot", "contracts", "booking", "sub", "extension", "extensions", "input", "output", "row", "col", "column", "columns"}

def extract_tables_from_sql(sql_text: str, known_tables: set = None) -> set:
    tables = set()
    known_tables = known_tables or set()
    cte_names = set(re.findall(r"with\s+([a-zA-Z0-9_]+)\s+as\s*\(", sql_text, re.IGNORECASE))
    cte_names.update(re.findall(r",\s*([a-zA-Z0-9_]+)\s+as\s*\(", sql_text, re.IGNORECASE))
    patterns = [r'\bFROM\s+([a-zA-Z0-9_]+)(?:\s|$|\)|,)', r'\bJOIN\s+([a-zA-Z0-9_]+)(?:\s|$|\)|,)', r'\bUPDATE\s+([a-zA-Z0-9_]+)(?:\s|$|\)|,)', r'\bINSERT\s+INTO\s+([a-zA-Z0-9_]+)(?:\s|$|\)|,)', r'\bMERGE\s+INTO\s+([a-zA-Z0-9_]+)(?:\s|$|\)|,)', r'\bDELETE\s+FROM\s+([a-zA-Z0-9_]+)(?:\s|$|\)|,)']
    for pattern in patterns:
        for match in re.findall(pattern, sql_text, re.IGNORECASE):
            table_name = match.strip().split('.')[-1] if '.' in match else match.strip()
            if table_name in known_tables or (len(table_name) >= 3 and table_name not in cte_names and table_name.lower() not in SQL_KEYWORDS and not table_name.isdigit() and (table_name.isupper() or '_' in table_name or (table_name[0].isupper() and len(table_name) > 4))):
                tables.add(table_name)
    return tables

def collect_table_names(root_dir="."):
    table_names = set()
    for dirpath, _, files in os.walk(root_dir):
        if '.venv' in dirpath or '__pycache__' in dirpath: continue
        for file in files:
            if file.endswith(".py"):
                try:
                    with open(os.path.join(dirpath, file), "r", encoding="utf-8") as f:
                        for match in re.findall(r'__tablename__\s*=\s*["\']([a-zA-Z0-9_]+)["\']', f.read()):
                            table_names.add(match)
                except: continue
    return table_names

class RouteAnalyzer(ast.NodeVisitor):
    def __init__(self, model_table_map=None, all_known_tables=None, all_function_definitions=None):
        self.router_prefix = ""
        self.routes_info = []
        self.flow_service_aliases = {"flow_service"}
        self.call_graph = defaultdict(set)
        self.current_function = None
        self.proc_calls = defaultdict(list)
        self.table_calls = defaultdict(set)
        self.current_route_info = None
        self.function_definitions = all_function_definitions or {}
        self.analyzed_functions = set()
        self.model_table_map = model_table_map or self.build_model_table_map()
        self.all_known_tables = set(all_known_tables) if all_known_tables else set()
        self.service_proc_map = self.build_proc_map() if all_function_definitions else {}

    def build_model_table_map(self):
        mapping = {}
        for pattern in ["api/**/orm/**/*.py", "**/orm/**/*.py", "**/models/**/*.py"]:
            for file_path in glob.glob(pattern, recursive=True):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    for class_name, class_body in re.findall(r'class\s+(\w+)[^\n]*:(.*?)(?=^class\s|\Z)', content, re.DOTALL | re.MULTILINE):
                        match = re.search(r'__tablename__\s*=\s*["\']([^"\']+)["\']', class_body)
                        if match: mapping[class_name] = match.group(1)
                except: continue
        return mapping

    def build_proc_map(self):
        proc_map = {}
        for func_name, func_node in self.function_definitions.items():
            if any(kw in func_name.lower() for kw in ['service', 'booking', 'sdp', 'assignment', 'rebuild', 'run_']):
                procs = self.extract_procs(func_node)
                if procs:
                    proc_map[func_name] = procs
                    if '.' in func_name: proc_map[func_name.split('.')[-1]] = procs
        # Known mappings
        proc_map.update({
            'rebuild_sdp_for_booking_contract': ['dc_sdp_contract_changes'],
            'replace_assignment_responsible_user': ['replace_responsible_user'],
            'update_verified_booking_assignments': ['assign_responsible_users'],
            'rebuild_sdp': ['dc_sdp_changes'],
            'run_rebuild_sdp': ['dc_sdp_changes'],
            'process_sea_upload': ['load_sea_data'],
            'process_macd_upload': ['load_macd_data']
        })
        return proc_map

    def extract_procs(self, func_node):
        procs = []
        for node in ast.walk(func_node):
            if isinstance(node, ast.Call):
                for kw in getattr(node, 'keywords', []):
                    if kw.arg == 'proc_name':
                        if isinstance(kw.value, ast.Constant): procs.append(kw.value.value)
                        elif isinstance(kw.value, ast.Str): procs.append(kw.value.s)
                if isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name) and node.func.value.id == "V2ProcedureNames":
                    procs.append(node.func.attr)
                for arg in getattr(node, 'args', []):
                    sql_text = None
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str): sql_text = arg.value
                    elif isinstance(arg, ast.Str): sql_text = arg.s
                    if sql_text:
                        for pattern in [r'CALL\s+([A-Z_][A-Z0-9_]*)\s*\(', r'CALL\s+IDENTIFIER\s*\(\s*["\']([A-Z_][A-Z0-9_]*)["\']', r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+([A-Z_][A-Z0-9_]*)']:
                            procs.extend(re.findall(pattern, sql_text.upper()))
        return procs

    def analyze_called_function(self, func_name: str, depth: int = 0) -> set:
        if depth > 5 or func_name in self.analyzed_functions or func_name in SKIP_FUNCTIONS or func_name in BUILTIN_FUNCTIONS:
            return set()
        self.analyzed_functions.add(func_name)
        tables = set()
        func_node = self.function_definitions.get(func_name)
        if func_node:
            prev_function, prev_route_info = self.current_function, self.current_route_info
            self.current_function, self.current_route_info = func_name, None
            self.generic_visit(func_node)
            tables.update(self.table_calls.get(func_name, set()))
            for called_func in self.call_graph.get(func_name, []):
                tables.update(self.analyze_called_function(called_func, depth + 1))
            self.current_function, self.current_route_info = prev_function, prev_route_info
        return tables

    def visit_Assign(self, node):
        if isinstance(node.value, ast.Call) and getattr(node.value.func, "id", None) == "APIRouter":
            for kw in node.value.keywords:
                if kw.arg == "prefix":
                    self.router_prefix = kw.value.value if isinstance(kw.value, ast.Constant) else kw.value.s
        self.generic_visit(node)

    def visit_With(self, node):
        for item in node.items:
            if isinstance(item.context_expr, ast.Name) and item.context_expr.id == "flow_service" and isinstance(item.optional_vars, ast.Name):
                self.flow_service_aliases.add(item.optional_vars.id)
        self.generic_visit(node)

    def visit_FunctionDef(self, node):
        func_name = node.name
        self.function_definitions[func_name] = node
        route_info = None
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Call) and hasattr(decorator.func, "attr") and decorator.func.attr.lower() in ROUTER_DECORATORS:
                path = "/"
                if decorator.args:
                    arg0 = decorator.args[0]
                    path = arg0.value if isinstance(arg0, ast.Constant) else arg0.s
                route_info = {"method": ROUTER_DECORATORS[decorator.func.attr.lower()], "path": self.router_prefix + path if self.router_prefix else path, "function": func_name, "flow_calls": [], "function_calls": [], "stored_procedures": [], "tables": [], "call_hierarchy": [], "category": "Backend, Frontend"}
                self.routes_info.append(route_info)
                break
        prev_function, prev_route_info = self.current_function, self.current_route_info
        self.current_function, self.current_route_info = func_name, route_info
        self.generic_visit(node)
        self.current_function, self.current_route_info = prev_function, prev_route_info

    def visit_AsyncFunctionDef(self, node): self.visit_FunctionDef(node)

    def visit_Call(self, node):
        if not self.current_function: return self.generic_visit(node)
        base_call_name = self.get_called_name(node.func)
        if base_call_name and base_call_name not in BUILTIN_FUNCTIONS and base_call_name not in SKIP_FUNCTIONS:
            self.detect_procs(node)
            if isinstance(node.func, ast.Attribute):
                attr_name = node.func.attr
                base = node.func.value
                if isinstance(base, ast.Name):
                    if base.id in self.flow_service_aliases and self.current_route_info:
                        self.current_route_info["flow_calls"].append(attr_name)
                    if attr_name == "bindparams" and isinstance(base, ast.Call):
                        for kw in node.keywords:
                            if kw.arg == "proc_name":
                                proc_name = kw.value.value if isinstance(kw.value, ast.Constant) else kw.value.s if isinstance(kw.value, ast.Str) else None
                                if proc_name:
                                    self.proc_calls[self.current_function].append(proc_name)
                                    if self.current_route_info: self.current_route_info["stored_procedures"].append(proc_name)
                    if base.id == "V2ProcedureNames":
                        self.proc_calls[self.current_function].append(attr_name)
                        if self.current_route_info: self.current_route_info["stored_procedures"].append(attr_name)
                    if attr_name == "add_task" and base.id == "background_tasks" and node.args and isinstance(node.args[0], ast.Name):
                        bg_func = node.args[0].id
                        self.call_graph[self.current_function].add(bg_func)
                        if self.current_route_info: self.current_route_info["function_calls"].append(bg_func)
                        if bg_func in self.service_proc_map:
                            for proc in self.service_proc_map[bg_func]:
                                self.proc_calls[self.current_function].append(proc)
                                if self.current_route_info: self.current_route_info["stored_procedures"].append(proc)
            elif isinstance(node.func, ast.Name):
                self.call_graph[self.current_function].add(base_call_name)
                if self.current_route_info: self.current_route_info["function_calls"].append(base_call_name)
            if base_call_name in ["run_stored_procedure", "run_v2_stored_procedure", "run_put_time_entries_stored_procedure", "make_stored_proc_statement"]:
                if base_call_name == "run_put_time_entries_stored_procedure":
                    self.proc_calls[self.current_function].append("put_user_time_entries")
                    if self.current_route_info: self.current_route_info["stored_procedures"].append("put_user_time_entries")
                for kw in node.keywords:
                    if kw.arg == "proc_name":
                        proc_name = self.extract_proc_name(kw.value)
                        if proc_name:
                            self.proc_calls[self.current_function].append(proc_name)
                            if self.current_route_info: self.current_route_info["stored_procedures"].append(proc_name)
            self.detect_tables(node)
        self.generic_visit(node)

    def detect_procs(self, node):
        if isinstance(node.func, ast.Attribute):
            attr_name = node.func.attr
            base = node.func.value
            if isinstance(base, ast.Name):
                method_key = f"{base.id}.{attr_name}"
                if method_key in self.service_proc_map or attr_name in self.service_proc_map:
                    procs = self.service_proc_map.get(method_key, self.service_proc_map.get(attr_name, []))
                    for proc in procs:
                        self.proc_calls[self.current_function].append(proc)
                        if self.current_route_info: self.current_route_info["stored_procedures"].append(proc)
        elif isinstance(node.func, ast.Name) and node.func.id in self.service_proc_map:
            for proc in self.service_proc_map[node.func.id]:
                self.proc_calls[self.current_function].append(proc)
                if self.current_route_info: self.current_route_info["stored_procedures"].append(proc)
        for arg in getattr(node, 'args', []):
            sql_text = None
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str): sql_text = arg.value
            elif isinstance(arg, ast.Str): sql_text = arg.s
            if sql_text and any(kw in sql_text.upper() for kw in ['CALL', 'PROCEDURE']):
                for pattern in [r'CALL\s+([A-Z_][A-Z0-9_]*)\s*\(', r'CALL\s+IDENTIFIER\s*\(\s*["\']([A-Z_][A-Z0-9_]*)["\']', r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+([A-Z_][A-Z0-9_]*)']:
                    for proc in re.findall(pattern, sql_text.upper()):
                        self.proc_calls[self.current_function].append(proc)
                        if self.current_route_info: self.current_route_info["stored_procedures"].append(proc)

    def detect_tables(self, node):
        if not self.current_function: return
        tables = set()
        if isinstance(node, ast.Call):
            func_name = self.get_called_name(node.func)
            if func_name and func_name in self.model_table_map: tables.add(self.model_table_map[func_name])
            
            # Enhanced service method calls
            if isinstance(node.func, ast.Attribute):
                attr_name = node.func.attr
                base = node.func.value
                if isinstance(base, ast.Name):
                    base_name = base.id
                    if 'service' in base_name.lower() or base_name in self.flow_service_aliases:
                        service_method = f"{base_name}.{attr_name}"
                        if service_method in self.function_definitions:
                            service_tables = self.analyze_called_function(service_method)
                            tables.update(service_tables)
                elif isinstance(base, ast.Call) and isinstance(base.func, ast.Name):
                    service_class = base.func.id
                    if 'service' in service_class.lower():
                        service_method = f"{service_class}.{attr_name}"
                        if service_method in self.function_definitions:
                            service_tables = self.analyze_called_function(service_method)
                            tables.update(service_tables)
            
            for arg in node.args:
                tables.update(self.extract_model_refs(arg))
                if isinstance(arg, ast.Constant) and isinstance(arg.value, str) and self.looks_like_sql(arg.value):
                    tables.update(extract_tables_from_sql(arg.value, self.all_known_tables))
                elif isinstance(arg, ast.Str) and self.looks_like_sql(arg.s):
                    tables.update(extract_tables_from_sql(arg.s, self.all_known_tables))
            for kw in getattr(node, 'keywords', []):
                tables.update(self.extract_model_refs(kw.value))
                if isinstance(kw.value, ast.Constant) and isinstance(kw.value.value, str) and self.looks_like_sql(kw.value.value):
                    tables.update(extract_tables_from_sql(kw.value.value, self.all_known_tables))
                elif isinstance(kw.value, ast.Str) and self.looks_like_sql(kw.value.s):
                    tables.update(extract_tables_from_sql(kw.value.s, self.all_known_tables))
        for table in tables:
            if table and (table in self.all_known_tables or self.is_real_table(table)):
                self.table_calls[self.current_function].add(table)
                if self.current_route_info and table not in self.current_route_info["tables"]:
                    self.current_route_info["tables"].append(table)

    def is_real_table(self, name): return len(name) >= 3 and name.lower() not in SQL_KEYWORDS and ((name.isupper() and ('_' in name or len(name) > 5)) or ('_' in name and any(c.isupper() for c in name)))

    def looks_like_sql(self, text): return len(text) >= 10 and sum(1 for kw in ['SELECT', 'FROM', 'INSERT', 'UPDATE', 'DELETE', 'WITH', 'JOIN', 'WHERE', 'CREATE', 'ALTER', 'DROP', 'CALL'] if kw in text.upper()) >= 2

    def extract_model_refs(self, node):
        tables = set()
        if isinstance(node, ast.Name) and node.id in self.model_table_map: tables.add(self.model_table_map[node.id])
        elif isinstance(node, ast.Attribute):
            if isinstance(node.value, ast.Name) and node.value.id in self.model_table_map: tables.add(self.model_table_map[node.value.id])
            tables.update(self.extract_model_refs(node.value))
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id in self.model_table_map: tables.add(self.model_table_map[node.func.id])
            for arg in node.args: tables.update(self.extract_model_refs(arg))
            for kw in node.keywords: tables.update(self.extract_model_refs(kw.value))
        elif isinstance(node, (ast.List, ast.Tuple, ast.Set)):
            for elt in node.elts: tables.update(self.extract_model_refs(elt))
        elif isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values):
                if key: tables.update(self.extract_model_refs(key))
                tables.update(self.extract_model_refs(value))
        elif hasattr(node, '__dict__'):
            for attr_name, attr_value in node.__dict__.items():
                if isinstance(attr_value, list):
                    for item in attr_value:
                        if isinstance(item, ast.AST): tables.update(self.extract_model_refs(item))
                elif isinstance(attr_value, ast.AST): tables.update(self.extract_model_refs(attr_value))
        return tables

    def extract_proc_name(self, node):
        if isinstance(node, ast.Constant) and isinstance(node.value, str): return node.value
        elif isinstance(node, ast.Str): return node.s
        elif isinstance(node, ast.Name): return node.id
        elif isinstance(node, ast.Attribute): return self.get_full_attr_name(node)
        return None

    def get_full_attr_name(self, node):
        parts = []
        while isinstance(node, ast.Attribute): parts.append(node.attr); node = node.value
        if isinstance(node, ast.Name): parts.append(node.id)
        return ".".join(reversed(parts))

    def get_called_name(self, node):
        if isinstance(node, ast.Name): return node.id
        elif isinstance(node, ast.Call): return self.get_called_name(node.func)
        elif isinstance(node, ast.Attribute): return node.attr
        return None

    def visit_Constant(self, node):
        if isinstance(node.value, str) and self.current_function:
            if self.looks_like_sql(node.value):
                tables = extract_tables_from_sql(node.value, self.all_known_tables)
                for table in tables:
                    self.table_calls[self.current_function].add(table)
                    if self.current_route_info and table not in self.current_route_info["tables"]: self.current_route_info["tables"].append(table)
                for pattern in [r'CALL\s+([A-Z_][A-Z0-9_]*)\s*\(', r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+([A-Z_][A-Z0-9_]*)']:
                    for proc in re.findall(pattern, node.value.upper()):
                        self.proc_calls[self.current_function].append(proc)
                        if self.current_route_info: self.current_route_info["stored_procedures"].append(proc)
        self.generic_visit(node)

    def visit_Str(self, node):
        if self.current_function:
            if self.looks_like_sql(node.s):
                tables = extract_tables_from_sql(node.s, self.all_known_tables)
                for table in tables:
                    self.table_calls[self.current_function].add(table)
                    if self.current_route_info and table not in self.current_route_info["tables"]: self.current_route_info["tables"].append(table)
                for pattern in [r'CALL\s+([A-Z_][A-Z0-9_]*)\s*\(', r'CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+([A-Z_][A-Z0-9_]*)']:
                    for proc in re.findall(pattern, node.s.upper()):
                        self.proc_calls[self.current_function].append(proc)
                        if self.current_route_info: self.current_route_info["stored_procedures"].append(proc)
        self.generic_visit(node)

def build_hierarchy(func_name, call_graph, proc_calls, visited=None, depth=0):
    if visited is None: visited = set()
    if func_name in visited or depth > 10: return []
    visited.add(func_name)
    hierarchy = []
    for called_func in sorted(call_graph.get(func_name, [])):
        hierarchy.append({"type": "function_call", "name": called_func, "depth": depth, "children": build_hierarchy(called_func, call_graph, proc_calls, visited.copy(), depth + 1)})
    for proc_name in sorted(set(proc_calls.get(func_name, []))):
        hierarchy.append({"type": "stored_procedure", "name": proc_name, "depth": depth, "children": []})
    return hierarchy

def analyze_file(filepath, model_table_map=None, all_known_tables=None, all_function_definitions=None):
    try:
        with open(filepath, "r", encoding="utf-8") as f: source = f.read()
        tree = ast.parse(source, filename=filepath)
        analyzer = RouteAnalyzer(model_table_map, all_known_tables, all_function_definitions)
        analyzer.visit(tree)
        for route_info in analyzer.routes_info:
            func_name = route_info["function"]
            route_info["call_hierarchy"] = build_hierarchy(func_name, analyzer.call_graph, analyzer.proc_calls)
            all_stored_procedures = set(route_info["stored_procedures"])
            all_tables = set(route_info["tables"])
            analyzer.analyzed_functions = set()
            for called_func in analyzer.call_graph.get(func_name, []):
                all_stored_procedures.update(analyzer.proc_calls.get(called_func, []))
                all_tables.update(analyzer.table_calls.get(called_func, []))
                deep_tables = analyzer.analyze_called_function(called_func)
                all_tables.update(deep_tables)
            route_info["flow_calls"] = list(set(route_info["flow_calls"]))
            route_info["function_calls"] = list(set(route_info["function_calls"]))
            route_info["stored_procedures"] = list(all_stored_procedures)
            route_info["tables"] = list(all_tables)
        return {"file_path": filepath, "routes": analyzer.routes_info, "function_definitions": analyzer.function_definitions}
    except Exception as e: return {"file_path": filepath, "error": str(e)}

def analyze_directory(folder_path):
    results = {}
    all_known_tables = collect_table_names(folder_path)
    shared_model_table_map = RouteAnalyzer().build_model_table_map()
    print(f"Found {len(all_known_tables)} known tables")
    all_function_definitions = {}
    python_files = []
    print("Collecting function definitions...")
    for root, _, files in os.walk(folder_path):
        if '.venv' in root or '__pycache__' in root: continue
        for file in files:
            if file.endswith(".py"):
                full_path = os.path.join(root, file)
                relative_path = os.path.relpath(full_path, folder_path)
                python_files.append((full_path, relative_path))
                try:
                    with open(full_path, "r", encoding="utf-8") as f: source = f.read()
                    tree = ast.parse(source, filename=full_path)
                    for node in ast.walk(tree):
                        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)): all_function_definitions[node.name] = node
                        elif isinstance(node, ast.ClassDef):
                            for class_node in node.body:
                                if isinstance(class_node, (ast.FunctionDef, ast.AsyncFunctionDef)): all_function_definitions[f"{node.name}.{class_node.name}"] = class_node
                except Exception as e: print(f"Error parsing {relative_path}: {e}")
    print(f"Collected {len(all_function_definitions)} function definitions")
    print("Analyzing files...")
    for full_path, relative_path in python_files:
        analysis = analyze_file(full_path, shared_model_table_map, all_known_tables, all_function_definitions)
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
                simplified_route = {"method": route["method"], "path": route["path"], "function": route["function"], "Category": "Backend, Frontend"}
                for field in ["flow_calls", "stored_procedures", "tables"]:
                    if route.get(field): simplified_route[field] = route[field]
                simplified_output[file_path].append(simplified_route)
        with open("merged_route_analysis_enchanced.json", "w", encoding="utf-8") as f:
            json.dump(simplified_output, f, indent=2, ensure_ascii=False)
        total_routes = sum(len(routes) for routes in simplified_output.values())
        files_with_procedures = sum(1 for routes in simplified_output.values() for route in routes if route.get("stored_procedures"))
        files_with_tables = sum(1 for routes in simplified_output.values() for route in routes if route.get("tables"))
        print(f"Analysis complete! Results saved to merged_route_analysis_enchanced.json")
        print(f"Summary: {total_routes} routes, {len(simplified_output)} files, {files_with_procedures} routes with stored procedures, {files_with_tables} routes with tables")
    except Exception as e: print(f"Error: {e}"); return 1
    return 0

if __name__ == "__main__": exit(main())