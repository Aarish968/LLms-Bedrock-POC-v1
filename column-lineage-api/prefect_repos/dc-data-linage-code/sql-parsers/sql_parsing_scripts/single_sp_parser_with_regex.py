import re
import pandas as pd


TARGET_COLUMNS = [
    "CTE_name",
    "CTE_alias",
    "source_table",
    "source_table_alias",
    "source_column",
    "source_column_Alias",
    "target_table",
    "target_column",
    "relationship_type",
    "context",
    "statement_type",
    "procedure_name",
]


def extract_all_relationships(sql_text: str, procedure_name: str = "UNKNOWN_PROCEDURE") -> pd.DataFrame:
    """Parse SQL text and emit lineage-style rows directly in the unified schema.

    Output columns (always present):
    CTE_name, CTE_alias, source_table, source_table_alias, source_column, source_column_Alias,
    target_table, target_column, relationship_type, context,
        statement_type, procedure_name.
    """
    # --- Normalise & strip comments ---
    sql_cleaned = re.sub(r"--.*", "", sql_text)
    sql_cleaned = re.sub(r"/\*[\s\S]*?\*/", "", sql_cleaned)
    sql_cleaned = sql_cleaned.replace("\n", " ")

    upper_sql = sql_text.upper()
    statement_type = "CREATE_TABLE" if "CREATE OR REPLACE TRANSIENT TABLE" in upper_sql else "OTHER"

    # Dynamically detect procedure creation for PROCEDURE_CREATED relation type
    proc_match = re.search(r"CREATE\s+OR\s+REPLACE\s+PROCEDURE\s+([A-Za-z0-9_\.]+)\s*\(", sql_text, re.IGNORECASE)
    detected_proc_name = proc_match.group(1) if proc_match else procedure_name

    rows = []

    def blank_row(**overrides):
        base = {c: "" for c in TARGET_COLUMNS}
        base.update(overrides)
        return base

  
    # TABLE / ALIAS DISCOVERY
   
    alias_mapping: dict[str, str] = {}
    RESERVED = {"JOIN", "ON", "WHERE", "GROUP", "ORDER", "HAVING", "WHEN", "THEN", "AS", "SELECT", "WITH", "USING", "MERGE", "UPDATE", "INSERT"}
    alias_pattern = re.compile(r"(?:FROM|JOIN)\s+([A-Za-z0-9_\.]+)(?:\s+(?:AS\s+)?([A-Za-z0-9_]+))?", re.IGNORECASE)

    for match in alias_pattern.finditer(sql_cleaned):
        table_name = match.group(1)
        alias = match.group(2)
        if alias and alias.upper() in RESERVED:
            continue
        if alias:
            alias_mapping[alias] = table_name
            # Table alias relationship (source side only)
            rows.append(blank_row(
                source_table=table_name,
                source_table_alias=alias,
                relationship_type="TABLE_ALIAS_RELATIONSHIP",
                context=f"{table_name} AS {alias}",
                statement_type=statement_type,
                procedure_name=detected_proc_name,
            ))

    
    # CTE DEFINITIONS
    
    existing_cte_names: set[str] = set()
    multi_cte_pattern = re.compile(r"WITH\s+(.+?)\bSELECT\b", re.IGNORECASE | re.DOTALL)
    for m in multi_cte_pattern.finditer(sql_cleaned):
        segment = m.group(1)
        for cte in re.findall(r"([A-Za-z0-9_]+)\s+AS\s*\(", segment, re.IGNORECASE):
            if cte in existing_cte_names:
                continue
            existing_cte_names.add(cte)
            rows.append(blank_row(
                CTE_name=cte,
                relationship_type="CTE_DEFINITION",
                context=f"CTE {cte}",
                statement_type=statement_type,
                procedure_name=detected_proc_name,
            ))

    # Nested CTE scan (avoid duplicates)
    for m in re.finditer(r"WITH\s+", sql_cleaned, flags=re.IGNORECASE):
        tail = sql_cleaned[m.end():]
        collected, buf, depth = [], [], 0
        for i, ch in enumerate(tail):
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(depth - 1, 0)
            elif depth == 0 and ch.upper() == 'S' and tail[i:i+6].upper() == 'SELECT':
                if buf:
                    collected.append(''.join(buf))
                break
            if depth == 0 and ch == ',':
                collected.append(''.join(buf))
                buf = []
            else:
                buf.append(ch)
        for spec in collected:
            spec = spec.strip()
            mname = re.match(r"([A-Za-z0-9_]+)\s+AS\s*\(", spec, flags=re.IGNORECASE)
            if not mname:
                continue
            name = mname.group(1)
            if name in existing_cte_names:
                continue
            existing_cte_names.add(name)
            rows.append(blank_row(
                CTE_name=name,
                relationship_type="CTE_DEFINITION",
                context=f"CTE {name}",
                statement_type=statement_type,
                procedure_name=detected_proc_name,
            ))

    cte_names = existing_cte_names.copy()

    # Derived tables (treat as table aliases with synthetic table name = alias)
    derived_pattern = re.compile(r"(?:FROM|JOIN)\s*\(\s*SELECT[\s\S]*?\)\s+(?:AS\s+)?([A-Za-z0-9_]+)", re.IGNORECASE)
    for d_alias in derived_pattern.findall(sql_cleaned):
        if d_alias not in alias_mapping:
            alias_mapping[d_alias] = d_alias  # self-resolve
        rows.append(blank_row(
            source_table=d_alias,
            source_table_alias=d_alias,
            relationship_type="DERIVED_TABLE_ALIAS",
            context=f"DERIVED {d_alias}",
            statement_type=statement_type,
            procedure_name=detected_proc_name,
        ))

    # Include USES_TABLE for each alias mapping (table or CTE reference)
    emitted_uses = set()
    for alias, base in alias_mapping.items():
        key = (base, alias)
        if key in emitted_uses:
            continue
        emitted_uses.add(key)
        if base in cte_names:
            rows.append(blank_row(
                CTE_name=base,
                CTE_alias=alias,
                relationship_type="USES_TABLE",
                context=f"USES_TABLE {alias}",
                statement_type=statement_type,
                procedure_name=detected_proc_name,
            ))
        else:
            rows.append(blank_row(
                source_table=base,
                source_table_alias=alias,
                relationship_type="USES_TABLE",
                context=f"USES_TABLE {alias}",
                statement_type=statement_type,
                procedure_name=detected_proc_name,
            ))

    # Base tables without alias (heuristic)
    base_table_pattern = re.compile(r"(?:FROM|JOIN)\s+([A-Za-z0-9_\.]+)", re.IGNORECASE)
    for base_only in base_table_pattern.findall(sql_cleaned):
        if base_only in alias_mapping.values():
            continue
        if (base_only, None) in emitted_uses:
            continue
        emitted_uses.add((base_only, None))
        if base_only in cte_names:
            rows.append(blank_row(
                CTE_name=base_only,
                relationship_type="USES_TABLE",
                context=f"USES_TABLE {base_only}",
                statement_type=statement_type,
                procedure_name=detected_proc_name,
            ))
        else:
            rows.append(blank_row(
                source_table=base_only,
                relationship_type="USES_TABLE",
                context=f"USES_TABLE {base_only}",
                statement_type=statement_type,
                procedure_name=detected_proc_name,
            ))

    def resolve(token: str) -> str:
        return alias_mapping.get(token, token)

    
    # JOIN conditions
    
    join_pattern = re.compile(r"JOIN\s+([A-Za-z0-9_\.]+)(?:\s+(?:AS\s+)?([A-Za-z0-9_]+))?\s+ON\s*\((.*?)\)(?=\s+JOIN|\s*;|$)", re.IGNORECASE)
    join_pattern_no_paren = re.compile(r"JOIN\s+([A-Za-z0-9_\.]+)(?:\s+(?:AS\s+)?([A-Za-z0-9_]+))?\s+ON\s+([^;]+?)(?=\s+JOIN|\s*;|$)", re.IGNORECASE)
    cond_regex = re.compile(r"([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\s*=\s*([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)")
    join_col_pairs: set[tuple[str, str]] = set()
    for pattern in (join_pattern, join_pattern_no_paren):
        for match in pattern.finditer(sql_cleaned):
            on_clause = match.group(3).strip()
            for a_tab, a_col, b_tab, b_col in cond_regex.findall(on_clause):
                join_col_pairs.add((a_tab, a_col))
                join_col_pairs.add((b_tab, b_col))
                # Source side mapping
                s_resolved = resolve(a_tab)
                t_resolved = resolve(b_tab)
                if s_resolved in cte_names:
                    cte_name, cte_alias = s_resolved, (a_tab if s_resolved != a_tab else "")
                    src_table = src_alias = ""
                else:
                    cte_name = cte_alias = ""
                    src_table = s_resolved
                    src_alias = a_tab if s_resolved != a_tab else ""
                rows.append(blank_row(
                    CTE_name=cte_name,
                    CTE_alias=cte_alias,
                    source_table=src_table,
                    source_table_alias=src_alias,
                    source_column=a_col,
                    target_table=t_resolved if t_resolved not in cte_names else "",
                    target_column=b_col,
                    relationship_type="USED_AS_JOIN",
                    context=f"JOIN ON {a_tab}.{a_col} = {b_tab}.{b_col}",
                    statement_type=statement_type,
                    procedure_name=detected_proc_name,
                ))

    
    # SELECT columns
    
    select_blocks = re.findall(r"SELECT\s+(.*?)\s+FROM", sql_cleaned, re.IGNORECASE | re.DOTALL)
    col_token_regex = re.compile(r"([A-Za-z0-9_]+\.[A-Za-z0-9_]+)")
    for block in select_blocks:
        if isinstance(block, tuple):
            block = block[0]
        for full in col_token_regex.findall(block):
            tab, col = full.split('.', 1)
            resolved = resolve(tab)
            if resolved in cte_names:
                rows.append(blank_row(
                    CTE_name=resolved,
                    CTE_alias=tab if resolved != tab else "",
                    source_column=col,
                    relationship_type="USED_AS_SELECT",
                    context=f"SELECT {full}",
                    statement_type=statement_type,
                    procedure_name=detected_proc_name,
                ))
            else:
                rows.append(blank_row(
                    source_table=resolved,
                    source_table_alias=tab if resolved != tab else "",
                    source_column=col,
                    relationship_type="USED_AS_SELECT",
                    context=f"SELECT {full}",
                    statement_type=statement_type,
                    procedure_name=detected_proc_name,
                ))

    # Additional heuristic pass: capture unqualified columns in single-source SELECT lists (nested CTE friendly)
    select_from_pattern = re.compile(
        r"SELECT\s+(?P<select>.*?)\s+FROM\s+(?P<from>.*?)(?=WHERE|GROUP\s+BY|ORDER\s+BY|HAVING|UNION|INTERSECT|EXCEPT|;|$)",
        re.IGNORECASE | re.DOTALL,
    )
    from_source_pattern = re.compile(r"(?:FROM|JOIN)\s+([A-Za-z0-9_\.]+)(?:\s+(?:AS\s+)?([A-Za-z0-9_]+))?", re.IGNORECASE)
    bare_identifier_regex = re.compile(r"(?<!\.)\b([A-Za-z_][A-Za-z0-9_]*)\b(?!\s*\.)")
    reserved_simple = {"SELECT","DISTINCT","ALL","AS","CASE","WHEN","THEN","ELSE","END","ON","AND","OR","NOT","NULL","TRUE","FALSE","LIKE","IN","IS"}

    for m in select_from_pattern.finditer(sql_cleaned):
        sel_part = m.group('select')
        from_part = m.group('from')
        # gather sources in this FROM/JOIN region
        local_sources = {}
        for fm in from_source_pattern.finditer("FROM " + from_part):
            base = fm.group(1)
            alias = fm.group(2) or base
            local_sources[alias] = resolve(alias)
        if len(local_sources) != 1:
            continue  # only unambiguous single-source blocks
        sole_alias, sole_resolved = next(iter(local_sources.items()))
        qualified_cols = {c.split('.',1)[1] for c in col_token_regex.findall(sel_part)}
        # remove DISTINCT/ALL prefix for scanning
        sel_core = re.sub(r"^(?i)\s*(DISTINCT|ALL)\s+", "", sel_part)
        for id_m in bare_identifier_regex.finditer(sel_core):
            tok = id_m.group(1)
            if tok.upper() in reserved_simple:
                continue
            if tok in qualified_cols:
                continue
            # skip apparent alias targets (expr AS tok)
            if re.search(rf"(?i)AS\s+{re.escape(tok)}\b", sel_part):
                continue
            # skip function names tok(...)
            if re.search(rf"(?i)\b{re.escape(tok)}\s*\(", sel_part):
                continue
            if sole_resolved in cte_names:
                rows.append(blank_row(
                    CTE_name=sole_resolved,
                    CTE_alias=sole_alias if sole_resolved != sole_alias else "",
                    source_column=tok,
                    relationship_type="USED_AS_SELECT",
                    context=f"SELECT {tok}",
                    statement_type=statement_type,
                    procedure_name=detected_proc_name,
                ))
            else:
                rows.append(blank_row(
                    source_table=sole_resolved,
                    source_table_alias=sole_alias if sole_resolved != sole_alias else "",
                    source_column=tok,
                    relationship_type="USED_AS_SELECT",
                    context=f"SELECT {tok}",
                    statement_type=statement_type,
                    procedure_name=detected_proc_name,
                ))

    
    # WHERE / FILTER

    where_blocks = re.findall(r"WHERE\s+(.*?)(?:GROUP\s+BY|ORDER\s+BY|HAVING|;|$)", sql_cleaned, re.IGNORECASE | re.DOTALL)
    for block in where_blocks:
        cleaned_block = re.sub(r"JOIN\s+[^)]*?ON\s*\([^)]*\)", "", block, flags=re.IGNORECASE)
        for tab, col in re.findall(r"([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)", cleaned_block):
            if (tab, col) in join_col_pairs:
                continue
            resolved = resolve(tab)
            if resolved in cte_names:
                rows.append(blank_row(
                    CTE_name=resolved,
                    CTE_alias=tab if resolved != tab else "",
                    source_column=col,
                    relationship_type="USED_AS_FILTER",
                    context=f"WHERE {tab}.{col}",
                    statement_type=statement_type,
                    procedure_name=detected_proc_name,
                ))
            else:
                rows.append(blank_row(
                    source_table=resolved,
                    source_table_alias=tab if resolved != tab else "",
                    source_column=col,
                    relationship_type="USED_AS_FILTER",
                    context=f"WHERE {tab}.{col}",
                    statement_type=statement_type,
                    procedure_name=detected_proc_name,
                ))

    # GROUP BY
    group_blocks = re.findall(r"GROUP\s+BY\s+(.*?)(?:HAVING|ORDER\s+BY|;|$)", sql_cleaned, re.IGNORECASE | re.DOTALL)
    for block in group_blocks:
        for full in col_token_regex.findall(block):
            tab, col = full.split('.', 1)
            resolved = resolve(tab)
            if resolved in cte_names:
                rows.append(blank_row(
                    CTE_name=resolved,
                    CTE_alias=tab if resolved != tab else "",
                    source_column=col,
                    relationship_type="USED_AS_GROUP_BY",
                    context=f"GROUP BY {full}",
                    statement_type=statement_type,
                    procedure_name=detected_proc_name,
                ))
            else:
                rows.append(blank_row(
                    source_table=resolved,
                    source_table_alias=tab if resolved != tab else "",
                    source_column=col,
                    relationship_type="USED_AS_GROUP_BY",
                    context=f"GROUP BY {full}",
                    statement_type=statement_type,
                    procedure_name=detected_proc_name,
                ))

    # ORDER BY
    order_blocks = re.findall(r"ORDER\s+BY\s+(.*?)(?:;|$)", sql_cleaned, re.IGNORECASE | re.DOTALL)
    for block in order_blocks:
        for full in col_token_regex.findall(block):
            tab, col = full.split('.', 1)
            resolved = resolve(tab)
            if resolved in cte_names:
                rows.append(blank_row(
                    CTE_name=resolved,
                    CTE_alias=tab if resolved != tab else "",
                    source_column=col,
                    relationship_type="USED_AS_ORDER_BY",
                    context=f"ORDER BY {full}",
                    statement_type=statement_type,
                    procedure_name=detected_proc_name,
                ))
            else:
                rows.append(blank_row(
                    source_table=resolved,
                    source_table_alias=tab if resolved != tab else "",
                    source_column=col,
                    relationship_type="USED_AS_ORDER_BY",
                    context=f"ORDER BY {full}",
                    statement_type=statement_type,
                    procedure_name=detected_proc_name,
                ))

    # HAVING
    having_blocks = re.findall(r"HAVING\s+(.*?)(?=ORDER\s+BY|GROUP\s+BY|;|$)", sql_cleaned, re.IGNORECASE | re.DOTALL)
    for block in having_blocks:
        for full in col_token_regex.findall(block):
            tab, col = full.split('.', 1)
            resolved = resolve(tab)
            if resolved in cte_names:
                rows.append(blank_row(
                    CTE_name=resolved,
                    CTE_alias=tab if resolved != tab else "",
                    source_column=col,
                    relationship_type="USED_AS_HAVING",
                    context=f"HAVING {full}",
                    statement_type=statement_type,
                    procedure_name=detected_proc_name,
                ))
            else:
                rows.append(blank_row(
                    source_table=resolved,
                    source_table_alias=tab if resolved != tab else "",
                    source_column=col,
                    relationship_type="USED_AS_HAVING",
                    context=f"HAVING {full}",
                    statement_type=statement_type,
                    procedure_name=detected_proc_name,
                ))

    # CREATE TABLE side effects
    for tbl in re.findall(r"CREATE\s+OR\s+REPLACE\s+TRANSIENT\s+TABLE\s+([A-Za-z0-9_]+)", sql_cleaned, re.IGNORECASE):
        rows.append(blank_row(
            target_table=tbl,
            relationship_type="PROCEDURE_CREATES_TABLE",
            context=f"CREATE TABLE {tbl}",
            statement_type=statement_type,
            procedure_name=detected_proc_name,
        ))

    # INSERT / MERGE detection
    merge_target = None
    merge_target_match = re.search(r"MERGE\s+INTO\s+([A-Za-z0-9_\.]+)", sql_cleaned, re.IGNORECASE)
    if merge_target_match:
        merge_target = merge_target_match.group(1)

    insert_blocks = re.findall(r"INSERT\s+INTO\s+([A-Za-z0-9_]+)\s*\((.*?)\)", sql_cleaned, re.IGNORECASE | re.DOTALL)
    for tbl, cols_str in insert_blocks:
        for col in [c.strip() for c in cols_str.split(',') if c.strip()]:
            rows.append(blank_row(
                target_table=tbl,
                target_column=col,
                relationship_type="USED_AS_INSERT",
                context=f"INSERT INTO {tbl}({col})",
                statement_type=statement_type,
                procedure_name=detected_proc_name,
            ))

    merge_insert_blocks = re.findall(r"WHEN\s+NOT\s+MATCHED\s+THEN\s+INSERT\s*\((.*?)\)", sql_cleaned, re.IGNORECASE | re.DOTALL)
    for cols_str in merge_insert_blocks:
        for col in [c.strip() for c in cols_str.split(',') if c.strip()]:
            rows.append(blank_row(
                target_table=merge_target or "",
                target_column=col,
                relationship_type="USED_AS_INSERT",
                context=f"MERGE INSERT {col}",
                statement_type=statement_type,
                procedure_name=detected_proc_name,
            ))

    # UPDATE + COLUMN_UPDATED_BY
    update_blocks = re.findall(r"UPDATE\s+([A-Za-z0-9_]+)\s+SET\s+(.*?)\s*(WHERE|;|$)", sql_cleaned, re.IGNORECASE | re.DOTALL)
    for tbl, set_clause, _ in update_blocks:
        assignments = [c for c in set_clause.split(',') if c.strip()]
        for assign in assignments:
            col = assign.split('=')[0].strip()
            rows.append(blank_row(
                target_table=tbl,
                target_column=col,
                relationship_type="USED_AS_UPDATE",
                context=f"UPDATE {tbl} SET {col}",
                statement_type=statement_type,
                procedure_name=detected_proc_name,
            ))
            rhs = assign.split('=', 1)[1] if '=' in assign else ''
            for s_tab, s_col in re.findall(r"([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)", rhs):
                resolved = resolve(s_tab)
                if resolved in cte_names:
                    rows.append(blank_row(
                        CTE_name=resolved,
                        CTE_alias=s_tab if resolved != s_tab else "",
                        source_column=s_col,
                        target_table=tbl,
                        target_column=col,
                        relationship_type="COLUMN_UPDATED_BY",
                        context=f"UPDATE {tbl} SET {col} = {rhs.strip()}",
                        statement_type=statement_type,
                        procedure_name=detected_proc_name,
                    ))
                else:
                    rows.append(blank_row(
                        source_table=resolved,
                        source_table_alias=s_tab if resolved != s_tab else "",
                        source_column=s_col,
                        target_table=tbl,
                        target_column=col,
                        relationship_type="COLUMN_UPDATED_BY",
                        context=f"UPDATE {tbl} SET {col} = {rhs.strip()}",
                        statement_type=statement_type,
                        procedure_name=detected_proc_name,
                    ))

    merge_update_blocks = re.findall(r"WHEN\s+MATCHED\s+THEN\s+UPDATE\s+SET\s+(.*?)(?:WHERE|;|$)", sql_cleaned, re.IGNORECASE | re.DOTALL)
    for set_clause in merge_update_blocks:
        assignments = [c for c in set_clause.split(',') if c.strip()]
        for assign in assignments:
            col = assign.split('=')[0].strip()
            rows.append(blank_row(
                target_table=merge_target or "",
                target_column=col,
                relationship_type="USED_AS_UPDATE",
                context=f"MERGE UPDATE {col}",
                statement_type=statement_type,
                procedure_name=detected_proc_name,
            ))
            rhs = assign.split('=', 1)[1] if '=' in assign else ''
            for s_tab, s_col in re.findall(r"([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)", rhs):
                resolved = resolve(s_tab)
                if resolved in cte_names:
                    rows.append(blank_row(
                        CTE_name=resolved,
                        CTE_alias=s_tab if resolved != s_tab else "",
                        source_column=s_col,
                        target_table=merge_target or "",
                        target_column=col,
                        relationship_type="COLUMN_UPDATED_BY",
                        context=f"MERGE UPDATE {col} = {rhs.strip()}",
                        statement_type=statement_type,
                        procedure_name=detected_proc_name,
                    ))
                else:
                    rows.append(blank_row(
                        source_table=resolved,
                        source_table_alias=s_tab if resolved != s_tab else "",
                        source_column=s_col,
                        target_table=merge_target or "",
                        target_column=col,
                        relationship_type="COLUMN_UPDATED_BY",
                        context=f"MERGE UPDATE {col} = {rhs.strip()}",
                        statement_type=statement_type,
                        procedure_name=detected_proc_name,
                    ))

    # Transformations (non-trivial SELECT expressions with AS alias)
    select_blocks_full = re.findall(r"SELECT\s+(.*?)\s+FROM", sql_cleaned, re.IGNORECASE | re.DOTALL)

    def split_select_list(block: str):
        items, current, depth = [], [], 0
        for ch in block:
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth = max(depth - 1, 0)
            if ch == ',' and depth == 0:
                items.append(''.join(current).strip())
                current = []
            else:
                current.append(ch)
        if current:
            items.append(''.join(current).strip())
        return [i for i in items if i]

    alias_expr_pattern = re.compile(r"^(?i)(.+?)\s+AS\s+([A-Za-z0-9_]+)$")
    simple_col_pattern = re.compile(r"^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$")
    for block in select_blocks_full:
        for item in split_select_list(block):
            m = alias_expr_pattern.match(item)
            if not m:
                continue
            expr, out_alias = m.group(1), m.group(2)
            if simple_col_pattern.match(expr.strip()):
                # simple rename -> capture as SELECT + target alias
                tab, col = expr.strip().split('.', 1)
                resolved = resolve(tab)
                if resolved in cte_names:
                    rows.append(blank_row(
                        CTE_name=resolved,
                        CTE_alias=tab if resolved != tab else "",
                        source_column=col,
                        source_column_Alias=out_alias,
                        relationship_type="USED_AS_SELECT",
                        context=f"{expr} AS {out_alias}",
                        statement_type=statement_type,
                        procedure_name=detected_proc_name,
                    ))
                else:
                    rows.append(blank_row(
                        source_table=resolved,
                        source_table_alias=tab if resolved != tab else "",
                        source_column=col,
                        source_column_Alias=out_alias,
                        relationship_type="USED_AS_SELECT",
                        context=f"{expr} AS {out_alias}",
                        statement_type=statement_type,
                        procedure_name=detected_proc_name,
                    ))
                continue
            sources = re.findall(r"([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)", expr)
            uniq = {(resolve(t), t if resolve(t) != t else "", c) for t, c in sources}
            if not uniq:
                rows.append(blank_row(
                    target_column=out_alias,
                    relationship_type="IS_TRANSFORMED_AS",
                    context=f"{expr} AS {out_alias}",
                    statement_type=statement_type,
                    procedure_name=detected_proc_name,
                ))
            else:
                for s_tab, s_alias, s_col in uniq:
                    if s_tab in cte_names:
                        rows.append(blank_row(
                            CTE_name=s_tab,
                            CTE_alias=s_alias,
                            source_column=s_col,
                            target_column=out_alias,
                            relationship_type="IS_TRANSFORMED_AS",
                            context=f"{expr} AS {out_alias}",
                            statement_type=statement_type,
                            procedure_name=detected_proc_name,
                        ))
                    else:
                        rows.append(blank_row(
                            source_table=s_tab,
                            source_table_alias=s_alias,
                            source_column=s_col,
                            target_column=out_alias,
                            relationship_type="IS_TRANSFORMED_AS",
                            context=f"{expr} AS {out_alias}",
                            statement_type=statement_type,
                            procedure_name=detected_proc_name,
                        ))

    # PROCEDURE_CREATED relation (single row if procedure definition found)
    if proc_match:
        rows.append(blank_row(
            relationship_type="PROCEDURE_CREATED",
            context=f"CREATE OR REPLACE PROCEDURE {detected_proc_name}",
            procedure_name=detected_proc_name,
            statement_type="CREATE_PROCEDURE",
        ))

    df = pd.DataFrame(rows).drop_duplicates()
    # Ensure column order
    df = df.reindex(columns=TARGET_COLUMNS)
    return df


if __name__ == "__main__":
    sql_path = "scripts\sql\DcSDPChanges.sql"
    try:
        with open(sql_path, "r", encoding="utf-8") as f:
            sql_text = f.read()
    except FileNotFoundError:
        print(f"Could not find SQL file at {sql_path}")
        raise

    df = extract_all_relationships(sql_text, procedure_name="DC_SDP_CHANGES")
    out_path = "single_sp_parser.csv"
    df.to_csv(out_path, index=False)
    print(f"Extracted {len(df)} unified lineage rows -> {out_path}")
