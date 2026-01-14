import os
import re
import json
import libcst as cst
from tree_sitter import Language, Parser
import tree_sitter_typescript as ts_typescript
from typing import List, Dict, Any


# CONFIG
IGNORE_DIRS = {"__pycache__", ".venv", ".git", "dist", "build", "node_modules", "tests"}

# FRONTEND PARSER 

TS_LANGUAGE = Language(ts_typescript.language_typescript())
ts_parser = Parser(TS_LANGUAGE)

def normalize_url(url: str) -> str:
    """Replace dynamic path params with {param} for fuzzy matching"""
    if not url:
        return ""
    
    # Remove quotes and backticks
    url = url.strip('"\'`')
    
    # Handle template literals: ${V2_URL}/announcements -> /announcements
    url = re.sub(r'\$\{[^}]+\}', '', url)
    
    # Handle path parameters: /announcements/{id} -> /announcements/{param}
    url = re.sub(r'\{[^}]+\}', '{param}', url)
    
    # Handle variable interpolation: /announcements/${id} -> /announcements/{param}
    url = re.sub(r'/\$\{[^}]+\}', '/{param}', url)
    
    # Clean up multiple slashes and trailing slashes
    url = re.sub(r'/+', '/', url)
    url = url.rstrip('/')
    
    # Ensure starts with /
    if url and not url.startswith('/'):
        url = '/' + url
        
    return url

def extract_url_from_template_literal(template_str: str) -> str:
    """Extract URL from template literal like `${V2_URL}/announcements`"""
    template_str = template_str.strip('`')
    template_str = re.sub(r'\$\{V2_URL\}', '', template_str)
    template_str = re.sub(r'\$\{[^}]+\}', '{param}', template_str)
    return template_str

def extract_url_from_args(args_node, source_code) -> str:
    """Extract URL from function arguments"""
    if not args_node or len(args_node.children) == 0:
        return ""
    
    first_arg = None
    for child in args_node.children:
        if child.type not in ("(", ")", ","):
            first_arg = child
            break
    
    if not first_arg:
        return ""
    
    url_text = source_code[first_arg.start_byte:first_arg.end_byte].decode()
    
    if first_arg.type == "template_string":
        return extract_url_from_template_literal(url_text)
    elif first_arg.type == "string":
        return url_text.strip('"\'')
    else:
        return url_text

def find_function_context(node, source_code):
    """Find the function name that contains this node"""
    current = node.parent
    
    while current:
        if current.type == "variable_declarator":
            name_node = current.child_by_field_name("name")
            value_node = current.child_by_field_name("value")
            
            if name_node and value_node:
                if value_node.type == "arrow_function" or (
                    value_node.type == "call_expression" and 
                    len(value_node.children) > 0 and 
                    "async" in source_code[value_node.start_byte:value_node.end_byte].decode()
                ):
                    return source_code[name_node.start_byte:name_node.end_byte].decode()
        
        elif current.type == "function_declaration":
            name_node = current.child_by_field_name("name")
            if name_node:
                return source_code[name_node.start_byte:name_node.end_byte].decode()
        
        current = current.parent
    
    return None

def traverse_ts_fixed(node, source_code, results=None):
    """Fixed traverse function that properly finds function context"""
    if results is None:
        results = []

    if node.type == "call_expression":
        func_node = node.child_by_field_name("function")
        if func_node and func_node.type == "member_expression":
            obj_node = func_node.child_by_field_name("object")
            prop_node = func_node.child_by_field_name("property")
            
            if obj_node and prop_node:
                obj_name = source_code[obj_node.start_byte:obj_node.end_byte].decode()
                prop_name = source_code[prop_node.start_byte:prop_node.end_byte].decode()

                if obj_name == "client":
                    function_name = find_function_context(node, source_code)
                    
                    if function_name:
                        args_node = node.child_by_field_name("arguments")
                        args_text = ""
                        url = ""
                        
                        if args_node:
                            args_text = source_code[args_node.start_byte:args_node.end_byte].decode()
                            
                            if prop_name in ["get", "post", "put", "delete", "patch"]:
                                url = extract_url_from_args(args_node, source_code)

                        results.append({
                            "action_function": function_name,
                            "frontend_service": f"{obj_name}.{prop_name}",
                            "frontend_args": args_text,
                            "url": normalize_url(url)
                        })

    for child in node.children:
        traverse_ts_fixed(child, source_code, results)

    return results

def parse_frontend_fixed(root_dir: str):
    """Parse all frontend files with fixed logic"""
    results = []
    
    for dirpath, dirnames, filenames in os.walk(root_dir):
        dirnames[:] = [d for d in dirnames if d not in IGNORE_DIRS]
        
        for file in filenames:
            if file.endswith((".ts", ".tsx", ".js")):
                path = os.path.join(dirpath, file)
                try:
                    with open(path, "rb") as f:
                        source_code = f.read()
                    tree = ts_parser.parse(source_code)
                    file_results = traverse_ts_fixed(tree.root_node, source_code)
                    results.extend(file_results)
                except Exception as e:
                    print(f"Error parsing {path}: {e}")
    
    return results


# BACKEND PARSER


def extract_router_prefixes_simple(init_file_path: str) -> Dict[str, str]:
    """Extract router prefixes using simple string parsing"""
    file_to_prefix = {}
    
    try:
        with open(init_file_path, 'r', encoding='utf8') as f:
            content = f.read()
        
        import_pattern = r'from\s+\.(\w+)\s+import\s+router\s+as\s+(\w+)'
        imports = re.findall(import_pattern, content)
        
        router_to_file = {router_alias: file_name for file_name, router_alias in imports}
        
        include_pattern = r'router\.include_router\(\s*(\w+),\s*prefix=["\']([^"\']+)["\']'
        includes = re.findall(include_pattern, content)
        
        for router_alias, prefix in includes:
            if router_alias in router_to_file:
                file_name = router_to_file[router_alias]
                file_to_prefix[file_name] = prefix
        
        return file_to_prefix
        
    except Exception as e:
        print(f"Error extracting router prefixes from {init_file_path}: {e}")
        return {}

class FastAPIVisitorWithPrefix(cst.CSTVisitor):
    """Visitor to extract FastAPI routes with router context"""
    
    def __init__(self, file_name: str, router_prefix: str = ""):
        self.endpoints = []
        self.file_name = file_name
        self.router_prefix = router_prefix.rstrip('/')

    def visit_FunctionDef(self, node: cst.FunctionDef) -> None:
        """Visit function definitions to find route decorators"""
        if not node.decorators:
            return

        for decorator in node.decorators:
            try:
                decorator_code = cst.Module([]).code_for_node(decorator.decorator)
                
                if decorator_code.startswith("router."):
                    method_and_route = decorator_code[7:]
                    
                    if "(" in method_and_route:
                        method = method_and_route.split("(")[0].upper()
                        route_part = method_and_route.split("(", 1)[1]
                        
                        route = self._extract_route_from_args(route_part)
                        full_route = self._combine_prefix_and_route(self.router_prefix, route)
                        
                        self.endpoints.append({
                            "function": node.name.value,
                            "method": method,
                            "route": self._normalize_route(full_route),
                            "file": self.file_name,
                            "router_prefix": self.router_prefix,
                            "local_route": route
                        })
            except Exception:
                continue

    def _extract_route_from_args(self, args_str: str) -> str:
        """Extract route from decorator arguments"""
        args_str = args_str.rstrip(")")
        
        if args_str.strip() == '""' or args_str.strip() == "''":
            return ""
        
        if args_str.startswith('"') or args_str.startswith("'"):
            quote_char = args_str[0]
            end_quote = args_str.find(quote_char, 1)
            if end_quote != -1:
                return args_str[1:end_quote]
        
        if "," in args_str:
            first_arg = args_str.split(",")[0].strip()
            return first_arg.strip('"\'')
        
        return args_str.strip('"\'')
    
    def _combine_prefix_and_route(self, prefix: str, route: str) -> str:
        """Combine router prefix with local route"""
        if not prefix:
            return route if route else "/"
        
        if not route or route == "":
            return prefix
        
        if not prefix.startswith('/'):
            prefix = '/' + prefix
        
        if route.startswith('/'):
            return prefix + route
        else:
            return prefix + '/' + route
    
    def _normalize_route(self, route: str) -> str:
        """Normalize route for consistent formatting"""
        if not route:
            return "/"
        
        if not route.startswith('/'):
            route = '/' + route
        
        route = re.sub(r'\{[^}]+\}', '{param}', route)
        route = re.sub(r'/+', '/', route)
        
        if route != '/' and route.endswith('/'):
            route = route.rstrip('/')
        
        return route

def parse_backend_with_prefixes_fixed(root_dir: str):
    """Parse all backend files considering router prefixes"""
    backend_endpoints = []
    
    router_init_path = os.path.join(root_dir, "v2", "routers", "__init__.py")
    router_prefixes = {}
    
    if os.path.exists(router_init_path):
        router_prefixes = extract_router_prefixes_simple(router_init_path)
    
    routers_dir = os.path.join(root_dir, "v2", "routers")
    
    if os.path.exists(routers_dir):
        for file in os.listdir(routers_dir):
            if file.endswith(".py") and file != "__init__.py":
                file_path = os.path.join(routers_dir, file)
                file_name = file.replace('.py', '')
                prefix = router_prefixes.get(file_name, "")
                
                try:
                    with open(file_path, encoding="utf8") as f:
                        content = f.read()
                    
                    module = cst.parse_module(content)
                    visitor = FastAPIVisitorWithPrefix(file_name, prefix)
                    module.visit(visitor)
                    backend_endpoints.extend(visitor.endpoints)
                except Exception as e:
                    print(f"Error parsing {file_path}: {e}")
    
    return backend_endpoints

# MAPPING LOGIC


def calculate_route_similarity(frontend_url: str, backend_route: str) -> float:
    """Calculate similarity between frontend URL and backend route"""
    if not frontend_url or not backend_route:
        return 0.0
    
    if frontend_url == backend_route:
        return 1.0
    
    frontend_segments = [s for s in frontend_url.split('/') if s]
    backend_segments = [s for s in backend_route.split('/') if s]
    
    if len(frontend_segments) != len(backend_segments):
        return 0.0
    
    matches = 0
    for f_seg, b_seg in zip(frontend_segments, backend_segments):
        if f_seg == b_seg:
            matches += 1
        elif f_seg == "{param}" or b_seg == "{param}":
            matches += 0.8
    
    return matches / len(frontend_segments) if frontend_segments else 0.0

def map_frontend_to_backend_final(frontend_calls: List[Dict], backend_endpoints: List[Dict]) -> List[Dict]:
    """Map frontend calls to backend endpoints with improved logic"""
    mapping = []
    
    for call in frontend_calls:
        if not call.get('url'):
            continue
            
        frontend_method = call['frontend_service'].split('.')[-1].upper()
        best_match = None
        best_score = 0.0
        
        for endpoint in backend_endpoints:
            if endpoint['method'] != frontend_method:
                continue
            
            similarity = calculate_route_similarity(call['url'], endpoint['route'])
            
            if similarity > best_score and similarity > 0.7:
                best_score = similarity
                best_match = endpoint
        
        if best_match:
            mapping.append({
                "action_function": call['action_function'],
                "frontend_service": call['frontend_service'],
                "frontend_args": call['frontend_args'],
                "frontend_url": call['url'],
                "backend_function": best_match['function'],
                "backend_route": best_match['route'],
                "backend_method": best_match['method'],
                "backend_file": best_match['file'],
                "similarity_score": best_score
            })
    
    return mapping


# MAIN EXECUTION


def main():
    frontend_root = "../guided-workflow/src"
    backend_root = "./api"
    
    print(" Parsing frontend calls...")
    frontend_calls = parse_frontend_fixed(frontend_root)
    print(f" Found {len(frontend_calls)} frontend calls")
    
    print(" Parsing backend endpoints...")
    backend_endpoints = parse_backend_with_prefixes_fixed(backend_root)
    print(f" Found {len(backend_endpoints)} backend endpoints")
    
    print(" Mapping frontend to backend...")
    api_mapping = map_frontend_to_backend_final(frontend_calls, backend_endpoints)
    print(f" Created {len(api_mapping)} mappings")
    
    # Save results
    with open("frontend_calls_final.json", "w") as f:
        json.dump(frontend_calls, f, indent=2)
    
    with open("backend_endpoints_final.json", "w") as f:
        json.dump(backend_endpoints, f, indent=2)
    
    with open("api_mapping_final.json", "w") as f:
        json.dump(api_mapping, f, indent=2)
    
    # Test specific cases
    print("\n Testing specific mappings:")
    
    # Look for createTagActions -> thought_spot_tag
    create_tag_actions = [m for m in api_mapping if m['action_function'] == 'createTagActions']
    if create_tag_actions:
        print(" createTagActions mapping:")
        print(json.dumps(create_tag_actions[0], indent=2))
    else:
        print(" createTagActions mapping not found")
        # Check if frontend call exists
        create_tag_frontend = [c for c in frontend_calls if c['action_function'] == 'createTagActions']
        if create_tag_frontend:
            print("Frontend call found:")
            print(json.dumps(create_tag_frontend[0], indent=2))
        
        # Check if backend endpoint exists
        thought_spot_backend = [e for e in backend_endpoints if 'thought_spot_tag' in e['route']]
        if thought_spot_backend:
            print("Backend endpoint found:")
            print(json.dumps(thought_spot_backend[0], indent=2))
    
    # Show mapping statistics
    print(f"\n Final Statistics:")
    print(f"Frontend calls: {len(frontend_calls)}")
    print(f"Backend endpoints: {len(backend_endpoints)}")
    print(f"Successful mappings: {len(api_mapping)}")
    print(f"Mapping rate: {len(api_mapping)/len(frontend_calls)*100:.1f}%")
    
    return frontend_calls, backend_endpoints, api_mapping

if __name__ == "__main__":
    frontend_calls, backend_endpoints, api_mapping = main()