import re
import pandas as pd
import uuid
from datetime import datetime
from pathlib import Path


# ---------------- CONFIG ----------------
SQL_FILE = Path("scripts\sql\TagInstances11.sql")


def gen_id():
   return str(uuid.uuid4())


def read_file(path: Path) -> str:
   return path.read_text(encoding="utf-8")


def clean_sql(sql: str) -> str:
   sql = re.sub(r'--.*?(\n|$)', ' ', sql)
   sql = re.sub(r'/\*.*?\*/', ' ', sql, flags=re.S)
   sql = re.sub(r'\s+', ' ', sql)
   return sql.strip()


#  handles parentheses in procedure declaration
def extract_procedure_name(sql: str) -> str:
   m = re.search(r'create\s+or\s+replace\s+procedure\s+([a-zA-Z0-9_.]+)\s*\(', sql, flags=re.I)
   return m.group(1).upper() if m else "UNKNOWN_PROCEDURE"


# handle DECLARE variables inside block
def extract_var_assignments(sql: str) -> dict:
   vars_map = {}
   declare_block = re.search(r'declare(.*?)begin', sql, flags=re.I | re.S)
   if declare_block:
       block = declare_block.group(1)
       for m in re.finditer(r'(\b[A-Z0-9_]+\b)\s*:?=\s*(?:\'([^\']+)\'|"([^"]+)")\s*;', block, flags=re.I):
           var = m.group(1).upper()
           val = m.group(2) or m.group(3)
           if val:
               vars_map[var] = val.strip()
   return vars_map


# extract SQL body between $$ ... $$
def extract_dollar_body(sql: str) -> str:
   m = re.search(r'\$\$(.*)\$\$', sql, flags=re.S | re.I)
   return m.group(1) if m else sql


def extract_relationships(sql_text: str):
   sql_clean = clean_sql(sql_text)
   proc_name = extract_procedure_name(sql_clean)
   body = extract_dollar_body(sql_clean)


   var_map = extract_var_assignments(sql_clean)


   rels = []
   now = datetime.now()


   # Tables created
   for m in re.finditer(r'create\s+(?:or\s+replace\s+)?(?:transient\s+)?table\s+identifier\s*\(\s*:(\w+)\s*\)', body, flags=re.I):
       var = m.group(1).upper()
       rels.append({
           "id": gen_id(),
           "procedure": proc_name,
           "target_var": var,
           "target_table": var_map.get(var, var),
           "relationship_type": "PROCEDURE_CREATES_TABLE",
           "extracted_at": now
       })


   # Tables used
   for m in re.finditer(r'identifier\s*\(\s*:(\w+)\s*\)', body, flags=re.I):
       var = m.group(1).upper()
       rels.append({
           "id": gen_id(),
           "procedure": proc_name,
           "target_var": var,
           "target_table": var_map.get(var, var),
           "relationship_type": "USES_TABLE",
           "extracted_at": now
       })


   # JOINs
   for m in re.finditer(r'(left|inner)?\s*join\s+(?:identifier\s*\(\s*:(\w+)\s*\)|([A-Z0-9_.]+))', body, flags=re.I):
       join_type = (m.group(1) or 'INNER').upper()
       var = (m.group(2) or m.group(3) or '').upper()
       rels.append({
           "id": gen_id(),
           "procedure": proc_name,
           "target_var": var,
           "target_table": var_map.get(var, var),
           "relationship_type": f"{join_type}_JOIN_WITH",
           "extracted_at": now
       })


   df = pd.DataFrame(rels).drop_duplicates()
   return proc_name, var_map, df




if __name__ == "__main__":
   sql_text = read_file(SQL_FILE)
   proc, var_map, df = extract_relationships(sql_text)


   print(f"Procedure: {proc}")
   print("\nVariable -> Table mapping:")
   for k, v in var_map.items():
       print(f"  {k}: {v}")


   print("\n Extracted Relationships ---")
   if df.empty:
       print(" No relationships found.")
   else:
       print(df.to_string(index=False))

