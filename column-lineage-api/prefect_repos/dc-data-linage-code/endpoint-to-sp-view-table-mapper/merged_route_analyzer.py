"""
Merged Route Analyzer

Analyzes routes, flow service calls, and stored procedure usage in FastAPI-style projects,
and outputs only relevant route data in a simplified JSON format.
"""

import ast
import os
import json
import builtins
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


class ComprehensiveRouteAnalyzer(ast.NodeVisitor):
    def __init__(self):
        self.router_prefix = ""
        self.routes_info = []
        self.flow_service_aliases = {"flow_service"}
        self.call_graph = defaultdict(set)
        self.defined_functions = set()
        self.func_to_route = {}
        self.current_function = None
        self.proc_calls = defaultdict(list)
        self.current_route_info = None
        

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

    def visit_AsyncFunctionDef(self, node):
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

            # Pattern: Background task calls (background_tasks.add_task)
            if isinstance(node.func, ast.Attribute) and node.func.attr == "add_task":
                base = node.func.value
                if isinstance(base, ast.Name) and base.id == "background_tasks":
                    # First argument is the function to be called in background
                    if node.args:
                        first_arg = node.args[0]
                        if isinstance(first_arg, ast.Name):
                            bg_func_name = first_arg.id
                            self.call_graph[self.current_function].add(bg_func_name)
                            if self.current_route_info:
                                self.current_route_info["function_calls"].append(bg_func_name)
                            
                            # Special handling for known service functions
                            if bg_func_name == "process_sea_upload":
                                # This function uses V2ProcedureNames.load_sea_data -> IMPORT_SEA_ENTRIES
                                self.proc_calls[self.current_function].append("load_sea_data")
                                if self.current_route_info:
                                    self.current_route_info["stored_procedures"].append("load_sea_data")
                            elif bg_func_name == "process_macd_upload":
                                # This function uses V2ProcedureNames.load_macd_data -> LOAD_MACD_DATA
                                self.proc_calls[self.current_function].append("load_macd_data")
                                if self.current_route_info:
                                    self.current_route_info["stored_procedures"].append("load_macd_data")

            # Enhanced stored procedure detection - all patterns
            if base_call_name in [
                "run_stored_procedure", 
                "run_v2_stored_procedure",
                "run_put_time_entries_stored_procedure",
                "make_stored_proc_statement"
            ]:
                # Special case: run_put_time_entries_stored_procedure has hardcoded proc_name
                if base_call_name == "run_put_time_entries_stored_procedure":
                    proc_name_val = "put_user_time_entries"
                    self.proc_calls[self.current_function].append(proc_name_val)
                    if self.current_route_info:
                        self.current_route_info["stored_procedures"].append(proc_name_val)
                
                # Pattern 1: Extract proc_name from keyword arguments
                for kw in node.keywords:
                    if kw.arg == "proc_name":
                        proc_name_val = self.extract_proc_name(kw.value)
                        if proc_name_val:
                            self.proc_calls[self.current_function].append(proc_name_val)
                            if self.current_route_info:
                                self.current_route_info["stored_procedures"].append(proc_name_val)

                # Pattern 2: For make_stored_proc_statement, check positional args
                if base_call_name == "make_stored_proc_statement" and node.args:
                    first_arg = node.args[0] if node.args else None
                    if first_arg:
                        proc_name_val = self.extract_proc_name(first_arg)
                        if proc_name_val:
                            self.proc_calls[self.current_function].append(proc_name_val)
                            if self.current_route_info:
                                self.current_route_info["stored_procedures"].append(proc_name_val)

            # Pattern 3: V2ProcedureNames enum usage (e.g., V2ProcedureNames.load_sea_data)
            if isinstance(node.func, ast.Attribute):
                attr_name = node.func.attr
                base = node.func.value
                if isinstance(base, ast.Name) and base.id == "V2ProcedureNames":
                    self.proc_calls[self.current_function].append(attr_name)
                    if self.current_route_info:
                        self.current_route_info["stored_procedures"].append(attr_name)

        self.generic_visit(node)

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


def analyze_file(filepath: str) -> Dict[str, Any]:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            source = f.read()

        tree = ast.parse(source, filename=filepath)
        analyzer = ComprehensiveRouteAnalyzer()
        analyzer.visit(tree)

        for route_info in analyzer.routes_info:
            func_name = route_info["function"]
            hierarchy = build_call_hierarchy(func_name, analyzer.call_graph, analyzer.proc_calls)
            route_info["call_hierarchy"] = hierarchy

            # Collect stored procedures from called functions (including background tasks)
            all_stored_procedures = set(route_info["stored_procedures"])
            for called_func in analyzer.call_graph.get(func_name, []):
                all_stored_procedures.update(analyzer.proc_calls.get(called_func, []))

            route_info["flow_calls"] = list(set(route_info["flow_calls"]))
            route_info["function_calls"] = list(set(route_info["function_calls"]))
            route_info["stored_procedures"] = list(all_stored_procedures)

        return {
            "file_path": filepath,
            "routes": analyzer.routes_info,
        }

    except Exception as e:
        return {"file_path": filepath, "error": str(e)}


def analyze_directory(folder_path: str) -> Dict[str, Any]:
    results = {}

    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".py"):
                full_path = os.path.join(root, file)
                relative_path = os.path.relpath(full_path, folder_path)
                analysis = analyze_file(full_path)

                if "error" not in analysis and analysis.get("routes"):
                    results[relative_path] = analysis["routes"]

    return results


def main():
    import sys
    folder_path = sys.argv[1] if len(sys.argv) > 1 else r'.'
    try:
        analysis_results = analyze_directory(folder_path)

        simplified_output = {}

        for file_path, routes in analysis_results.items():
            # Use full relative path to avoid conflicts with duplicate filenames
            simplified_output[file_path] = []
            for route in routes:
                simplified_route = {
                    "method": route["method"],
                    "path": route["path"],
                    "function": route["function"],
                    "Category": "Backend, Frontend"
                }
                
                flow_calls = route.get("flow_calls")
                stored_procedures = route.get("stored_procedures")
                if flow_calls:
                    simplified_route["flow_calls"] = flow_calls

                if stored_procedures:
                    simplified_route["stored_procedures"] = stored_procedures
                
                simplified_output[file_path].append(simplified_route)


        output_file = "merged_route_analyzer.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(simplified_output, f, indent=2, ensure_ascii=False)

    except Exception as e:
        print(f" Error during analysis: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
