import sqlglot
from sqlglot import expressions as exp
import re

class SQLParser:
    def __init__(self):
        pass

    def parse(self, files: list[dict]) -> dict:
        results = []
        
        print(f"Total files to check: {len(files)}")
        
        for i, f in enumerate(files):
            file_path = f["file_path"]
            
            # Only process .sql files for now
            if not file_path.lower().endswith('.sql'):
                continue
            
            print(f"Processing SQL file: {file_path}")
            
            try:
                # SUPER AGGRESSIVE CONTENT CLEANING
                content = f["content"]
                
                # Remove ALL ANSI escape codes
                content = re.sub(r'\x1b\[[0-9;]*[mGKH]', '', content)
                
                # Remove comment blocks with # symbols
                content = re.sub(r'^#{3,}.*?#{3,}$', '', content, flags=re.MULTILINE)
                content = re.sub(r'^#{10,}.*$', '', content, flags=re.MULTILINE)
                
                # Remove lines that are just # characters
                lines = content.split('\n')
                cleaned_lines = []
                for line in lines:
                    # Skip lines that are mostly # characters
                    if line.strip() and not re.match(r'^#{3,}', line.strip()):
                        cleaned_lines.append(line)
                
                content = '\n'.join(cleaned_lines)
                
                # Split by semicolons (proper SQL statement separation)
                statements = content.split(';')
                
                for stmt_content in statements:
                    stmt_content = stmt_content.strip()
                    
                    # Skip empty or very short statements
                    if not stmt_content or len(stmt_content) < 15:
                        continue
                    
                    # Skip comment-only statements
                    if stmt_content.startswith('--') or stmt_content.startswith('#'):
                        continue
                    
                    # Only process statements that look like complete SQL
                    if not any(stmt_content.upper().strip().startswith(keyword) 
                              for keyword in ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'WITH']):
                        continue
                    
                    try:
                        # Try parsing with Snowflake dialect
                        tree = sqlglot.parse(stmt_content, dialect="snowflake")
                        

                        for stmt in tree:
                            if stmt:

                                # print(stmt)
                                result = {
                                    "file": file_path,
                                    "type": self._safe_get_type(stmt),
                                    "tables": self._safe_get_tables(stmt),
                                    "columns": self._safe_get_columns(stmt),
                                    "joins": self._safe_get_joins(stmt),
                                    "sql_preview": stmt_content[:150]
                                }
                                results.append(result)
                                
                    except Exception as stmt_error:
                        # Only log significant errors
                        error_msg = str(stmt_error)
                        if len(stmt_content) > 30 and "Unexpected token" in error_msg:
                            results.append({
                                "file": file_path,
                                "error": f"Parse error: {error_msg[:100]}",
                                "statement_preview": stmt_content[:80]
                            })
                        
            except Exception as e:
                results.append({
                    "file": file_path,
                    "error": f"File error: {str(e)}"
                })
        
        successful = len([r for r in results if 'error' not in r])
        errors = len([r for r in results if 'error' in r])
        
        print(f"\nSuccessfully parsed statements: {successful}")
        print(f"Parsing errors: {errors}")
        
        return {"sql": results}
    
    def _safe_get_type(self, stmt):
        try:
            return stmt.key.upper() if hasattr(stmt, 'key') else "UNKNOWN"
        except:
            return "UNKNOWN"
    
    def _safe_get_tables(self, stmt):
        try:
            tables = []
            for t in stmt.find_all(exp.Table):
                try:
                    table_name = str(t.name) if hasattr(t, 'name') else str(t)
                    table_name = table_name.replace('"', '').replace("'", '')
                    if table_name and table_name != 'UNKNOWN':
                        tables.append(table_name)
                except:
                    pass
            return tables
        except:
            return []
    
    def _safe_get_columns(self, stmt):
        try:
            columns = []
            for c in stmt.find_all(exp.Column):
                try:
                    col_name = str(c.name) if hasattr(c, 'name') else str(c)
                    col_name = col_name.replace('"', '').replace("'", '')
                    if col_name and col_name != 'UNKNOWN':
                        columns.append(col_name)
                except:
                    pass
            return columns
        except:
            return []
    
    def _safe_get_joins(self, stmt):
        try:
            joins = []
            for j in stmt.find_all(exp.Join):
                try:
                    join_type = "INNER"
                    if hasattr(j, 'args') and 'kind' in j.args:
                        join_type = str(j.args.get("kind", "INNER")).upper()
                    
                    right_table = None
                    if hasattr(j, 'this') and j.this:
                        right_table = str(j.this.name) if hasattr(j.this, 'name') else str(j.this)
                    
                    if right_table:
                        joins.append({
                            "type": join_type,
                            "table": right_table
                        })
                except:
                    pass
            return joins
        except:
            return []

# Test the improved parser
if __name__ == "__main__":
    sample = files[:5]
    parser = SQLParser()
    result = parser.parse(sample)
    
    print("\n" + "="*60)
    print("FINAL RESULTS")
    print("="*60)
    
    successful = [r for r in result['sql'] if 'error' not in r]
    print(f"\nSuccessful parses: {len(successful)}")
    
    # Group by statement type
    by_type = {}
    for r in successful:
        stmt_type = r['type']
        if stmt_type not in by_type:
            by_type[stmt_type] = []
        by_type[stmt_type].append(r)
    
    print("\nStatement types found:")
    for stmt_type, statements in by_type.items():
        print(f"  {stmt_type}: {len(statements)} statements")
    import json
    
    print(json.dumps(by_type,indent =2))
    # Show sample of each type
    print("\nSample statements:")
    for stmt_type, statements in by_type.items():
        print(f"\n{stmt_type} Example:")
        r = statements[0]
        print(f"  Tables: {r['tables']}")
        print(f"  Columns: {r['columns'][:3]}...")
        if r['joins']:
            print(f"  Joins: {r['joins']}")
        print(f"  SQL: {r['sql_preview'][:100]}...")
    
    # Show remaining errors (should be much fewer)
    errors = [r for r in result['sql'] if 'error' in r]
    print(f"\nRemaining errors: {len(errors)}")