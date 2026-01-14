import os
import ast
import importlib
import sys
import re
from src.common_tasks import __version__


class ImportRemover(ast.NodeTransformer):
    
    def __init__(self, package_name):
        self.package_name = package_name
    
    def visit_Import(self, node):
        new_aliases = [alias for alias in node.names if
                       not alias.name.startswith(self.package_name)]
        if new_aliases:
            node.names = new_aliases
            return node
        return None
    
    def visit_ImportFrom(self, node):
        if node.module and node.module.startswith(self.package_name):
            return None
        return node


def find_imports(file_path, package_name):
    with open(file_path, 'r', encoding='utf-8') as file:
        tree = ast.parse(file.read())
    
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith(package_name):
                    imports.add(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module and node.module.startswith(package_name):
                imports.add(node.module)
    
    return imports


def resolve_module_path(module_name, base_dir):
    try:
        spec = importlib.util.find_spec(module_name)
        if spec is None:
            return None
        module_path = spec.origin
        if module_path.endswith('__init__.py'):
            return os.path.dirname(module_path)
        return os.path.dirname(os.path.dirname(module_path))
    except ImportError:
        # If import fails, try to find the module manually
        parts = module_name.split('.')
        current_path = base_dir
        for part in parts:
            current_path = os.path.join(current_path, part)
            if os.path.isfile(os.path.join(current_path, '__init__.py')):
                continue
            elif os.path.isfile(current_path + '.py'):
                return os.path.dirname(current_path)
            else:
                return None
        return current_path


def get_python_files(directory):
    python_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                python_files.append(os.path.join(root, file))
    return python_files


def remove_local_imports(content, package_name):
    tree = ast.parse(content, type_comments=True)
    transformer = ImportRemover(package_name)
    modified_tree = transformer.visit(tree)
    return ast.unparse(modified_tree)


def post_process_content(content):
    # Remove remaining dotted imports
    content = re.sub(r'from \..+$', '', content, flags=re.MULTILINE)
    # Add version to User-Agent
    return content.strip()

def update_user_agent(fp):
    with open(fp, 'r', encoding='utf-8') as f:
        content = f.read()
    new_content = re.sub(r"([\"'])User-Agent(\1):\s(\1)(NotificationHandler)(\1)", f"'User-Agent': 'NotificationHandler/{__version__.__version__!s}'", content)
    if new_content == content:
        print("User-Agent not found in file")
    with open(fp, 'w', encoding='utf-8') as f:
        f.write(new_content)


def build_package(entry_points, package_name, output_file, base_dir):
    processed_modules = set()
    to_process = set(entry_points)
    
    with open(output_file, 'w', encoding='utf-8') as outfile:
        while to_process:
            module = to_process.pop()
            if module in processed_modules:
                continue
            
            module_path = resolve_module_path(module, base_dir)
            if not module_path:
                print(f"Warning: Could not resolve path for module {module}")
                continue
            
            processed_modules.add(module)
            
            # Process all Python files in the module directory
            for file_path in get_python_files(module_path):
                with open(file_path, 'r', encoding='utf-8') as infile:
                    content = infile.read()
                    # Remove local imports
                    content = remove_local_imports(content, package_name)
                    # Post-process to remove remaining dotted imports and comments
                    content = post_process_content(content)
                    outfile.write(content)
                outfile.write("\n\n")
                
                imports = find_imports(file_path, package_name)
                to_process.update(imports)
    
                
    update_user_agent(output_file)
    ruff_process_content(output_file)
    print(f"Package built successfully. Output file: {output_file}")
    
def ruff_process_content(fp):
    # Run ruff on the file
    os.system(f"ruff check --fix {fp}")
    os.system(f"ruff format {fp}")
    os.system(f"git add {fp}")


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print(
            "Usage: python script.py <package_name> <output_file> <base_directory> <entry_point1> [<entry_point2> ...]"
        )
        sys.exit(1)
    
    package_name = sys.argv[1]
    output_file = sys.argv[2]
    base_dir = os.path.abspath(sys.argv[3])
    entry_points = sys.argv[4:]
    
    # Add base_dir to Python path to allow imports
    sys.path.insert(0, base_dir)
    
    build_package(entry_points, package_name, output_file, base_dir)