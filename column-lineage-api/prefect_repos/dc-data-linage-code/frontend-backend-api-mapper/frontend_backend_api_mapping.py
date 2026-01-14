#!/usr/bin/env python3
"""
Frontend-Backend API Mapping Script

This script analyzes the frontend TypeScript API files and backend Python route files
to create  mapping between frontend API calls and backend endpoints.

Usage:
    uv run python map_frontend_backend.py
    uv run python map_frontend_backend.py --output mapping.json
    uv run python map_frontend_backend.py --format csv --output mapping.csv
"""
import re
import json
import argparse
from pathlib import Path
from typing import List, Optional
from dataclasses import dataclass

@dataclass
class FrontendCall:
    file: str
    function_name: str
    method: str
    url_pattern: str
    line_number: int
    raw_code: str

@dataclass
class BackendRoute:
    file: str
    method: str
    route_pattern: str
    line_number: int
    function_name: str
    tags: List[str]
    raw_code: str

@dataclass
class Mapping:
    frontend: FrontendCall
    backend: Optional[BackendRoute]

class FrontendAnalyzer:
    def __init__(self, frontend_path: str):
        self.src_path = Path(frontend_path) / "src"
        
    def extract_frontend_calls(self) -> List[FrontendCall]:
        calls = []
        if not self.src_path.exists():
            return calls
        
        # Search all .ts and .tsx files
        for ext in ["*.ts", "*.tsx"]:
            for file_path in self.src_path.rglob(ext):
                if file_path.name in ["utils.ts", "vite-env.d.ts"]:
                    continue
                calls.extend(self._parse_file(file_path))
        return calls
    
    def _parse_file(self, file_path: Path) -> List[FrontendCall]:
        try:
            lines = file_path.read_text(encoding='utf-8').split('\n')
        except:
            return []
        
        calls = []
        current_function = None
        found_lines = set()  
        
        for i, line in enumerate(lines, 1):
            #  function tracking
            if 'export' in line:
                func_patterns = [
                    r'export\s+(?:const|async\s+function|function)\s+(\w+)',
                    r'export\s+const\s+(\w+)\s*=\s*async',
                    r'export\s+async\s+function\s+(\w+)',
                    r'export\s+function\s+(\w+)',
                ]
                for pattern in func_patterns:
                    func_match = re.search(pattern, line)
                    if func_match:
                        current_function = func_match.group(1)
                        break
            
            # track arrow functions and const declarations
            elif re.search(r'const\s+\w+\s*=\s*async', line) and not current_function:
                func_match = re.search(r'const\s+(\w+)\s*=\s*async', line)
                if func_match:
                    current_function = func_match.group(1)
            
            # Primary API call detection
            if ('client.' in line or 'tsClient.' in line) and i not in found_lines:
                api_patterns = [
                    r'(?:client|tsClient)\.(get|post|put|patch|delete)',
                    r'await\s+(?:client|tsClient)\.(get|post|put|patch|delete)',
                    r'return\s+(?:await\s+)?(?:client|tsClient)\.(get|post|put|patch|delete)',
                    r'const\s+\w+\s*=\s*(?:await\s+)?(?:client|tsClient)\.(get|post|put|patch|delete)',
                    r'=\s*(?:await\s+)?(?:client|tsClient)\.(get|post|put|patch|delete)',
                    r'\.then\(\s*(?:await\s+)?(?:client|tsClient)\.(get|post|put|patch|delete)',  
                    r'Promise\.all\([^)]*(?:client|tsClient)\.(get|post|put|patch|delete)', 
                ]
                
                for pattern in api_patterns:
                    api_match = re.search(pattern, line)
                    if api_match:
                        method = api_match.group(1).upper()
                        url = self._extract_url(line, lines, i-1)
                        if url:
                            calls.append(FrontendCall(
                                file=file_path.name,
                                function_name=current_function or "unknown",
                                method=method,
                                url_pattern=url,
                                line_number=i,
                                raw_code=line.strip()
                            ))
                            found_lines.add(i)
                        break
            
           
            if ('client' in line or 'tsClient' in line) and i not in found_lines:
               
                fallback_patterns = [
                    r'(client|tsClient)\.(\w+)',  
                    r'(client|tsClient)\s*\[\s*["\'](\w+)["\']\s*\]',  
                ]
                
                for pattern in fallback_patterns:
                    fallback_match = re.search(pattern, line)
                    if fallback_match:
                        method_name = fallback_match.group(2)
                        if method_name.lower() in ['get', 'post', 'put', 'patch', 'delete']:
                            method = method_name.upper()
                            url = self._extract_url_aggressive(line, lines, i-1)
                            if url:
                                calls.append(FrontendCall(
                                    file=file_path.name,
                                    function_name=current_function or "unknown",
                                    method=method,
                                    url_pattern=url,
                                    line_number=i,
                                    raw_code=line.strip()
                                ))
                                found_lines.add(i)
                            break
        
        return calls
    
    def _extract_url(self, line: str, all_lines: List[str], line_idx: int) -> str:
        
        if 'blob' in line.lower() or 'responseType:' in line:
            return ""
        
        # URL patterns
        patterns = [
            r'[`"](\$\{[^}]+\}[^`"]*)[`"]',  # Template strings
            r'[`"\']([/\w\-{}$\.]+[^`"\']*)[`"\']',  # Regular strings
            r'url\s*[=:]\s*[`"\'](.*?)[`"\']',  # url = "..." or url: "..."
            r'endpoint\s*[=:]\s*[`"\'](.*?)[`"\']',  # endpoint = "..."
            r'[`"\'](/api/[^`"\']*)[`"\']',  # Any /api/ path
            r'[`"\'](\$\{V2_WORKFLOW_URL\}[^`"\']*)[`"\']',  # Workflow URLs
            r'formData\.append\([^,]+,\s*[`"\'](.*?)[`"\']',  # FormData URLs
            r'path\s*[=:]\s*[`"\'](.*?)[`"\']',  # path = "..."
            r'route\s*[=:]\s*[`"\'](.*?)[`"\']',  # route = "..."
        ]
        
        # Search current line and next lines 
        for i in range(line_idx, min(line_idx + 30, len(all_lines))):
            current_line = all_lines[i]
            
            # Skip comments and empty lines
            if current_line.strip().startswith('//') or not current_line.strip():
                continue
                
            for pattern in patterns:
                match = re.search(pattern, current_line)
                if match:
                    url = match.group(1)
                    if url and (url.startswith('/') or '${' in url or 'api' in url or 'workflow' in url.lower()):
                        normalized = self._normalize_url(url)
                        if normalized and len(normalized) > 3:
                            return normalized
        
        # variable detection
        var_patterns = [
            r'(?:client|tsClient)\.\w+(?:<[^>]*>)?\s*\(\s*(\w+)',
            r'(?:client|tsClient)\.\w+\s*\(\s*(\w+)',
            r'(?:client|tsClient)\.\w+\s*\(\s*`([^`]+)`',
            r'(?:client|tsClient)\.\w+\s*\(\s*"([^"]+)"',
            r'(?:client|tsClient)\.\w+\s*\(\s*\'([^\']+)\'',
        ]
        
        for pattern in var_patterns:
            var_match = re.search(pattern, line)
            if var_match:
                var_name = var_match.group(1)
                excluded = ['data', 'body', 'payload', 'params', 'formData', 'config', 'options', 'headers', 'rest', 'args']
                if var_name not in excluded:
                    for j in range(max(0, line_idx - 80), line_idx):
                        search_line = all_lines[j]
                        var_def_patterns = [
                            f'(?:const|let|var)\\s+{var_name}\\s*=\\s*[`"\'](.*?)[`"\']',
                            f'{var_name}\\s*=\\s*[`"\'](.*?)[`"\']',
                            f'\\b{var_name}\\s*=\\s*`([^`]+)`',
                            f'\\b{var_name}\\s*:\\s*[`"\'](.*?)[`"\']', 
                        ]
                        for var_def_pattern in var_def_patterns:
                            var_def = re.search(var_def_pattern, search_line)
                            if var_def:
                                found_url = self._normalize_url(var_def.group(1))
                                if found_url:
                                    return found_url
                break
        
        return ""
    
    def _extract_url_aggressive(self, line: str, all_lines: List[str], line_idx: int) -> str:
        """Ultra-aggressive URL extraction for edge cases"""
        
        if 'blob' in line.lower() or 'responseType:' in line:
            return ""
        
        # more patterns for edge cases
        aggressive_patterns = [
            r'[`"](\$\{[^}]+\}[^`"]*)[`"]',
            r'[`"\']([/\w\-{}$\.]+[^`"\']*)[`"\']',
            r'[`"\'](\/[^`"\']*)[`"\']',  # Any path starting with /
            r'[`"\']([^`"\']*api[^`"\']*)[`"\']',  # Any string containing 'api'
            r'[`"\']([^`"\']*workflow[^`"\']*)[`"\']',  # Any string containing 'workflow'
            r'[`"\']([^`"\']*\$\{[^}]+\}[^`"\']*)[`"\']',  # Any template string
        ]
        
        # Search wider range - up to 40 lines
        for i in range(line_idx, min(line_idx + 40, len(all_lines))):
            current_line = all_lines[i]
            
            if current_line.strip().startswith('//') or not current_line.strip():
                continue
                
            for pattern in aggressive_patterns:
                match = re.search(pattern, current_line)
                if match:
                    url = match.group(1)
                    if url and len(url) > 2:  
                        normalized = self._normalize_url(url)
                        if normalized and len(normalized) > 3 and ('api' in normalized or normalized.startswith('/')):
                            return normalized
        
        return ""
    
    def _normalize_url(self, url: str) -> str:
        if not url:
            return ""
        
        # Replace template variables
        url = url.replace('${V2_URL}', '/api/v2')
        url = url.replace('${V2_WORKFLOW_URL}', '/api/v2/workflows')
        
        # Handle complex interpolations
        url = re.sub(r'\$\{[\w.]+\.(\w+)\}', r'{\1}', url)
        url = re.sub(r'\$\{(\w+)\}', lambda m: '{' + self._camel_to_snake(m.group(1)) + '}', url)
        
        # Remove query parameters
        url = re.sub(r'\?.*$', '', url)
        
        return url.strip()
    
    @staticmethod
    def _camel_to_snake(name: str) -> str:
        name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()

class BackendAnalyzer:
    def __init__(self, backend_path: str):
        self.routers_path = Path(backend_path) / "api" / "v2" / "routers"
        
    def extract_backend_routes(self) -> List[BackendRoute]:
        if not self.routers_path.exists():
            return []
        
        routes = []
        for file_path in self.routers_path.rglob("*.py"):
            if file_path.name != "__init__.py":
                routes.extend(self._parse_file(file_path))
        return routes
    
    def _parse_file(self, file_path: Path) -> List[BackendRoute]:
        try:
            lines = file_path.read_text(encoding='utf-8').split('\n')
        except:
            return []
        
        routes = []
        rel_path = file_path.relative_to(self.routers_path)
        base_prefix = self._get_prefix(rel_path)
        
        i = 0
        while i < len(lines):
            line = lines[i].strip()
            
            if line.startswith('@router.'):
                # Collect multi-line decorator
                decorator_lines = [line]
                j = i + 1
                while j < len(lines) and ')' not in ''.join(decorator_lines):
                    decorator_lines.append(lines[j].strip())
                    j += 1
                
                full_decorator = ' '.join(decorator_lines)
                route_match = re.search(r'@router\.(get|post|put|patch|delete)\s*\(\s*["\']([^"\']*)["\']', full_decorator, re.IGNORECASE)
                
                if route_match:
                    method = route_match.group(1).upper()
                    route = route_match.group(2)
                    func_name = self._get_function_name(lines, j)
                    full_route = base_prefix + route if route else base_prefix
                    
                    routes.append(BackendRoute(
                        file=str(rel_path),
                        method=method,
                        route_pattern=full_route,
                        line_number=i + 1,
                        function_name=func_name,
                        tags=[],
                        raw_code=full_decorator[:100]
                    ))
                
                i = j
            else:
                i += 1
        
        return routes
    
    def _get_prefix(self, rel_path: Path) -> str:
        parts = rel_path.parts[:-1]
        filename = rel_path.stem
        prefix = "/api/v2"
        
        # Add directory prefixes
        for part in ["workflows", "admin", "manager", "support", "sdp"]:
            if part in parts:
                prefix += f"/{part}"
        
        # Add specific file mappings
        file_routes = {
            # Workflows
            "signoff": "/sign_off", "actions": "/actions", "notifications": "/notifications",
            "downloads": "/downloads", "evidence_uploads": "/evidence_uploads", 
            "lookups": "/lookups", "macd": "/macd",
            # Admin
            "unverified": "/bookings/unverified", "verified": "/bookings/verified",
            "revenue": "/revenue", "contracts": "/contracts",
            "tasks": "/tasks", "subtasks": "/subtasks", "deliverables": "/deliverables",
            # Manager  
            "bookings": "/bookings", "scv": "/scv", "super_customers": "/scv",
            "users": "/users", "pool": "/pool_manager",
            # Support
            "user": "/cases", "agent": "/agent",
            # SDP
            "completions": "/completions", "time_tracking": "/time_tracking",
            # Top-level
            "engagements": "/engagements", "tags": "/tags", "tagsets": "/tagsets",
            "stakeholders": "/stakeholders", "canvas": "/canvas", "links": "/links",
            "thought_spot": "/thought_spot", "thought_spot_tag": "/thought_spot_tag",
            "dc_types": "/dc_types", "user_defined_types": "/udt", 
            "documentation": "/documentation", "announcements": "/announcements",
            "file_management": "/file_management", "static": "/static"
        }
        
        for key, route in file_routes.items():
            if key in filename:
                prefix += route
                break
        
        # Handle edge cases
        if "financial" in str(rel_path) and "/financial" not in prefix:
            prefix += "/financial"
        if filename == "tagsets":
            prefix = "/api/v2/tagsets"
        if filename == "thought_spot_tag":
            return "/api/v2/thought_spot_tag"
        if filename == "user_defined_types":
            return "/api/v2/udt"
        
        return prefix
    
    def _get_function_name(self, lines: List[str], start_idx: int) -> str:
        for i in range(start_idx, min(start_idx + 5, len(lines))):
            if i < len(lines) and not lines[i].strip().startswith('@'):
                func_match = re.search(r'def\s+(\w+)', lines[i])
                if func_match:
                    return func_match.group(1)
        return "unknown"

class APIMapper:
    def __init__(self, frontend_calls: List[FrontendCall], backend_routes: List[BackendRoute]):
        self.frontend_calls = frontend_calls
        self.backend_routes = backend_routes
    
    def create_mappings(self) -> List[Mapping]:
        mappings = []
        for frontend_call in self.frontend_calls:
            backend_route = self._find_match(frontend_call)
            mappings.append(Mapping(frontend=frontend_call, backend=backend_route))
        return mappings
    def _find_match(self, frontend_call: FrontendCall) -> Optional[BackendRoute]:
       
        for backend_route in self.backend_routes:
            if self._routes_match(frontend_call, backend_route):
                return backend_route
        
        
        best_match = None
        best_score = 0.6
        
        for backend_route in self.backend_routes:
            if backend_route.method == frontend_call.method:
                score = self._similarity(frontend_call.url_pattern, backend_route.route_pattern)
                if score > best_score:
                    best_score = score
                    best_match = backend_route
        
        return best_match
    
    def _routes_match(self, frontend: FrontendCall, backend: BackendRoute) -> bool:
        if frontend.method != backend.method:
            return False
        
        # Normalize both URLs
        fe_url = frontend.url_pattern.replace('/api/v2', '').strip('/')
        be_url = backend.route_pattern.replace('/api/v2', '').strip('/')
        
        if fe_url == be_url:
            return True
        
        fe_parts = fe_url.split('/') if fe_url else []
        be_parts = be_url.split('/') if be_url else []
        
        if len(fe_parts) != len(be_parts):
            return False
        
        for fe_part, be_part in zip(fe_parts, be_parts):
            if (fe_part.startswith('{') and be_part.startswith('{')) or fe_part == be_part:
                continue
            else:
                return False
        return True
    
    def _similarity(self, str1: str, str2: str) -> float:
        parts1 = set(str1.split('/'))
        parts2 = set(str2.split('/'))
        if not parts1 or not parts2:
            return 0.0
        intersection = parts1.intersection(parts2)
        union = parts1.union(parts2)
        return len(intersection) / len(union)
    
def generate_reports(mappings: List[Mapping], output_base: str, formats: List[str]):
    if "json" in formats:
        data = []
        for m in mappings:
            data.append({
                "frontend": {
                    "file": m.frontend.file,
                    "function": m.frontend.function_name,
                    "method": m.frontend.method,
                    "url": m.frontend.url_pattern,
                    "line": m.frontend.line_number
                },
                "backend": {
                    "file": m.backend.file if m.backend else None,
                    "function": m.backend.function_name if m.backend else None,
                    "method": m.backend.method if m.backend else None,
                    "route": m.backend.route_pattern if m.backend else None,
                    "line": m.backend.line_number if m.backend else None
                } if m.backend else None
            })
        
        with open(f"{output_base}.json", 'w') as f:
            json.dump(data, f, indent=2)
    
    if "csv" in formats:
        import csv
        with open(f"{output_base}.csv", 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Frontend File", "Frontend Function", "HTTP Method", "Frontend URL", "Backend File", "Backend Function", "Backend Route"])
            for m in mappings:
                writer.writerow([
                    m.frontend.file, m.frontend.function_name, m.frontend.method, m.frontend.url_pattern,
                    m.backend.file if m.backend else "", m.backend.function_name if m.backend else "", m.backend.route_pattern if m.backend else ""
                ])

def print_summary(mappings: List[Mapping]):
    total = len(mappings)
    matched = sum(1 for m in mappings if m.backend)
    unmatched = total - matched
    
    print("API MAPPING SUMMARY")
    print(f"Total Frontend Calls: {total}")
    print(f"Matched:              {matched} ({matched/total*100:.1f}%)")
    print(f"Unmatched:            {unmatched} ({unmatched/total*100:.1f}%)")

def main():
    parser = argparse.ArgumentParser(description="Map frontend API calls to backend endpoints")
    parser.add_argument("--frontend", default="guided-workflow", help="Frontend project path")
    parser.add_argument("--backend", default="guided-workflow-backend", help="Backend project path")
    parser.add_argument("--output", default="api_mapping", help="Output file base name")
    parser.add_argument("--format", choices=["json", "csv", "all"], default="all", help="Output format")
    
    args = parser.parse_args()
    
    print("Frontend-Backend API Mapping Tool")
    
    # Analyze frontend
    print("Analyzing frontend")
    frontend_analyzer = FrontendAnalyzer(args.frontend)
    frontend_calls = frontend_analyzer.extract_frontend_calls()
    print(f"Found {len(frontend_calls)} frontend API calls")
    
    # Analyze backend
    print("Analyzing backend")
    backend_analyzer = BackendAnalyzer(args.backend)
    backend_routes = backend_analyzer.extract_backend_routes()
    print(f"Found {len(backend_routes)} backend routes")
    
    # Create mappings
    mapper = APIMapper(frontend_calls, backend_routes)
    mappings = mapper.create_mappings()
    
    # Generate reports
    formats = ["json", "csv"] if args.format == "all" else [args.format]
    generate_reports(mappings, args.output, formats)
    
    print_summary(mappings)
    print("Done")

if __name__ == "__main__":
    main()
