"""
View Analyzer for DC_QUALIFIED_SIGNOFF
This version captures all relationships: SELECT, JOIN, and FILTER
"""

import pandas as pd
import re
from typing import Dict, List, Tuple, Set, Optional
from dataclasses import dataclass
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class ColumnMapping:
    table_id: int
    view_id: int
    table_join_id: str
    relationship_id: int
    relationship_type: str
    source_type: str
    target_type: str
    alias_name: str
    stored_procedure: str
    business_logic: str

class FinalViewAnalyzer:
    def __init__(self, table_metadata_file: str, view_metadata_file: str):
        """Initialize with table and view metadata"""
        self.table_metadata = pd.read_csv(table_metadata_file)
        self.view_metadata = pd.read_csv(view_metadata_file)
        
        # Build lookup dictionaries
        self.table_columns = self._build_table_column_lookup()
        self.view_columns = self._build_view_column_lookup()
        
        logger.info(f"Loaded {len(self.table_metadata)} table columns")
        logger.info(f"Loaded {len(self.view_metadata)} view columns")
        
    def _build_table_column_lookup(self) -> Dict[str, Dict[str, int]]:
        """Build lookup: table_name -> {column_name: primary_key}"""
        lookup = {}
        for _, row in self.table_metadata.iterrows():
            if pd.isna(row['table_name']) or pd.isna(row['column_names']):
                continue
                
            table_name = str(row['table_name']).upper()
            column_name = str(row['column_names']).upper()
            primary_key = row['primary_key']
            
            if table_name not in lookup:
                lookup[table_name] = {}
            lookup[table_name][column_name] = primary_key
        
        return lookup
    
    def _build_view_column_lookup(self) -> Dict[str, Dict[str, int]]:
        """Build lookup: view_name -> {column_name: primary_key}"""
        lookup = {}
        for _, row in self.view_metadata.iterrows():
            if pd.isna(row['table_name']) or pd.isna(row['column_names']):
                continue
                
            view_name = str(row['table_name']).upper()
            column_name = str(row['column_names']).upper()
            primary_key = row['primary_key']
            
            if view_name not in lookup:
                lookup[view_name] = {}
            lookup[view_name][column_name] = primary_key
        
        return lookup
    
    def analyze_dc_qualified_signoff(self, view_ddl: str) -> List[ColumnMapping]:
        """Complete analysis of DC_QUALIFIED_SIGNOFF view"""
        relationships = []
        relationship_id = 1
        
        # Clean SQL
        sql = self._clean_sql(view_ddl)
        
        # 1. Extract SELECT relationships from final query
        select_rels = self._extract_final_select_relationships(sql, relationship_id)
        relationships.extend(select_rels)
        relationship_id += len(select_rels)
        
        # 2. Extract JOIN relationships from CTE
        join_rels = self._extract_join_relationships(sql, relationship_id)
        relationships.extend(join_rels)
        relationship_id += len(join_rels)
        
        # 3. Extract WHERE/FILTER relationships
        filter_rels = self._extract_filter_relationships(sql, relationship_id)
        relationships.extend(filter_rels)
        
        return relationships
    
    def _extract_final_select_relationships(self, sql: str, start_id: int) -> List[ColumnMapping]:
        """Extract relationships from the final SELECT statement"""
        relationships = []
        relationship_id = start_id
        
        # Find the final SELECT
        final_select_match = re.search(
            r'select\s+distinct\s+(.*?)\s+from\s+so\s*;?\s*$', 
            sql, 
            re.IGNORECASE | re.DOTALL
        )
        
        if not final_select_match:
            return relationships
        
        select_clause = final_select_match.group(1).strip()
        select_items = self._parse_select_items(select_clause)
        
        # Map to view columns in order
        view_columns = [
            'BOOKING_CONTRACT', 'IBV_METHOD', 'IBV_IDENTITY', 'IBV_EVENT', 
            'NOTES', 'QUALIFIED_IBV', 'DAYS_SINCE_LAST_SIGNOFF_EVENT', 'LAST_SIGNOFF_DATE'
        ]
        
        # Define the source mappings based on CTE analysis
        source_mappings = {
            'BOOKING_CONTRACT': ('DC_WF_IB_SIGNOFF', 'BOOKING_CONTRACT', ''),
            'IBV_METHOD': ('DC_TYP_SIGNOFF_METHOD', 'SIGNOFF_METHOD', ''),
            'IBV_IDENTITY': ('DC_TYP_SIGN_OFF_IDENTITY', 'SIGN_OFF_IDENTITY', ''),
            'IBV_EVENT': ('DC_TYP_SIGNOFF_EVENT', 'SIGNOFF_EVENT', ''),
            'NOTES': ('DC_WF_IB_SIGNOFF', 'NOTES', ''),
            'QUALIFIED_IBV': ('DC_WF_IB_SIGNOFF', 'SIGNOFF_METHOD_ID', 'case when DATEDIFF(day, last_signoff_date,current_date) > 90 then \'sign_off_overdue\' else signoff_type end'),
            'DAYS_SINCE_LAST_SIGNOFF_EVENT': ('DC_WF_IB_SIGNOFF', 'CREATE_DTM', 'DATEDIFF(day, last_signoff_date,current_date)'),
            'LAST_SIGNOFF_DATE': ('DC_WF_IB_SIGNOFF', 'CREATE_DTM', '')
        }
        
        for view_col in view_columns:
            if view_col in source_mappings:
                source_table, source_column, business_logic = source_mappings[view_col]
                
                table_id = self._get_table_id(source_table, source_column)
                view_id = self._get_view_id('DC_QUALIFIED_SIGNOFF', view_col)
                
                if table_id and view_id:
                    relationships.append(ColumnMapping(
                        table_id=table_id,
                        view_id=view_id,
                        table_join_id="",
                        relationship_id=relationship_id,
                        relationship_type="select",
                        source_type=source_table,
                        target_type='DC_QUALIFIED_SIGNOFF',
                        alias_name="",
                        stored_procedure="",
                        business_logic=business_logic
                    ))
                    relationship_id += 1
        
        return relationships
    
    def _extract_join_relationships(self, sql: str, start_id: int) -> List[ColumnMapping]:
        """Extract JOIN relationships from the CTE"""
        relationships = []
        relationship_id = start_id
        
        # Define the joins based on the SQL structure
        joins = [
            # m.SIGNOFF_METHOD_ID=s.SIGNOFF_METHOD_ID
            ('DC_TYP_SIGNOFF_METHOD', 'SIGNOFF_METHOD_ID', 'DC_WF_IB_SIGNOFF', 'SIGNOFF_METHOD_ID'),
            # i.SIGN_OFF_IDENTITY_ID = s.SIGN_OFF_IDENTITY_ID  
            ('DC_TYP_SIGN_OFF_IDENTITY', 'SIGN_OFF_IDENTITY_ID', 'DC_WF_IB_SIGNOFF', 'SIGN_OFF_IDENTITY_ID'),
            # e.SIGNOFF_EVENT_ID = s.signoff_event_id
            ('DC_TYP_SIGNOFF_EVENT', 'SIGNOFF_EVENT_ID', 'DC_WF_IB_SIGNOFF', 'SIGNOFF_EVENT_ID'),
            # mx_date.BOOKING_CONTRACT=s.BOOKING_CONTRACT
            ('DC_WF_IB_SIGNOFF', 'BOOKING_CONTRACT', 'DC_WF_IB_SIGNOFF', 'BOOKING_CONTRACT'),
            # c.BOOKING_CONTRACT = s.BOOKING_CONTRACT
            ('DC_BOOKINGS_CONTRACTS', 'BOOKING_CONTRACT', 'DC_WF_IB_SIGNOFF', 'BOOKING_CONTRACT')
        ]
        
        for left_table, left_col, right_table, right_col in joins:
            left_table_id = self._get_table_id(left_table, left_col)
            right_table_id = self._get_table_id(right_table, right_col)
            
            if left_table_id and right_table_id:
                relationships.append(ColumnMapping(
                    table_id=left_table_id,
                    view_id=0,
                    table_join_id=str(right_table_id),
                    relationship_id=relationship_id,
                    relationship_type="join",
                    source_type=left_table,
                    target_type=right_table,
                    alias_name="",
                    stored_procedure="",
                    business_logic=""
                ))
                relationship_id += 1
        
        return relationships
    
    def _extract_filter_relationships(self, sql: str, start_id: int) -> List[ColumnMapping]:
        """Extract WHERE/FILTER relationships"""
        relationships = []
        relationship_id = start_id
        
        # Define the filter conditions based on the WHERE clause
        filters = [
            # current_date between c.AGREEMENT_START_DATE and dateadd(day, 30, c.AGREEMENT_END_DATE)
            ('DC_BOOKINGS_CONTRACTS', 'AGREEMENT_START_DATE'),
            ('DC_BOOKINGS_CONTRACTS', 'AGREEMENT_END_DATE'),
            # s.is_deleted = 'F'
            ('DC_WF_IB_SIGNOFF', 'IS_DELETED'),
            # c.is_deleted = 'F' (from join condition)
            ('DC_BOOKINGS_CONTRACTS', 'IS_DELETED')
        ]
        
        for table, column in filters:
            table_id = self._get_table_id(table, column)
            if table_id:
                relationships.append(ColumnMapping(
                    table_id=table_id,
                    view_id=0,
                    table_join_id="",
                    relationship_id=relationship_id,
                    relationship_type="filter",
                    source_type=table,
                    target_type='DC_QUALIFIED_SIGNOFF',
                    alias_name="",
                    stored_procedure="",
                    business_logic="Y"
                ))
                relationship_id += 1
        
        return relationships
    
    def _clean_sql(self, sql: str) -> str:
        """Clean and normalize SQL"""
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        sql = re.sub(r'\s+', ' ', sql)
        sql = re.sub(r'CPS_DSCI_API\.', '', sql, flags=re.IGNORECASE)
        return sql.strip()
    
    def _parse_select_items(self, select_clause: str) -> List[str]:
        """Parse SELECT clause into individual items"""
        items = []
        current_item = ""
        paren_count = 0
        
        for char in select_clause:
            if char == '(':
                paren_count += 1
            elif char == ')':
                paren_count -= 1
            elif char == ',' and paren_count == 0:
                if current_item.strip():
                    items.append(current_item.strip())
                current_item = ""
                continue
            
            current_item += char
        
        if current_item.strip():
            items.append(current_item.strip())
        
        return items
    
    def _validate_column(self, table: str, column: str) -> bool:
        """Validate that table.column exists in metadata"""
        return table in self.table_columns and column in self.table_columns[table]
    
    def _get_table_id(self, table: str, column: str) -> Optional[int]:
        """Get table_id for table.column from metadata"""
        if self._validate_column(table, column):
            return self.table_columns[table][column]
        return None
    
    def _get_view_id(self, view_name: str, column: str) -> Optional[int]:
        """Get view_id for view.column from metadata"""
        view_name = view_name.upper()
        column = column.upper()
        if view_name in self.view_columns and column in self.view_columns[view_name]:
            return self.view_columns[view_name][column]
        return None
    
    def generate_relationship_csv(self, view_ddl: str, output_file: str):
        """Generate comprehensive relationship mapping CSV"""
        relationships = self.analyze_dc_qualified_signoff(view_ddl)
        
        # Convert to DataFrame matching your exact format
        df_data = []
        for rel in relationships:
            df_data.append({
                'table_id': rel.table_id,
                'view__id': rel.view_id,
                'table_join_id': rel.table_join_id,
                'relationship_id': rel.relationship_id,
                'relationship_type': rel.relationship_type,
                'source_type': rel.source_type,
                'target_type': rel.target_type,
                'alias_name': rel.alias_name,
                'stored_procedure': rel.stored_procedure,
                'bussiness_logic': rel.business_logic
            })
        
        df = pd.DataFrame(df_data)
        df.to_csv(output_file, index=False)
        
        logger.info(f"Generated {len(relationships)} relationships")
        logger.info(f"Saved to {output_file}")
        
        # Print summary
        print(f"\nRelationship Summary:")
        print(f"Total relationships: {len(relationships)}")
        print(f"SELECT relationships: {len([r for r in relationships if r.relationship_type == 'select'])}")
        print(f"JOIN relationships: {len([r for r in relationships if r.relationship_type == 'join'])}")
        print(f"FILTER relationships: {len([r for r in relationships if r.relationship_type == 'filter'])}")
        
        return df

def main():
    view_ddl = """
    create or replace view DC_QUALIFIED_SIGNOFF(
    BOOKING_CONTRACT,
    IBV_METHOD,
    IBV_IDENTITY,
    IBV_EVENT,
    NOTES,
    QUALIFIED_IBV,
    DAYS_SINCE_LAST_SIGNOFF_EVENT,
    LAST_SIGNOFF_DATE
    ) as
    with so as ( -- this and qualified SO need to be crisp granularity of booking contract level across 2 events signoff and disconnect... so is it really 1?
    with mx_date as (-- resolve to tru last event
    select s.BOOKING_CONTRACT, max(s.CREATE_DTM) as last_signoff_date
    from CPS_DSCI_API.DC_WF_IB_SIGNOFF s
    group by BOOKING_CONTRACT
    ) -- get the unique last event details
    select distinct s.BOOKING_CONTRACT,
    case
    when s.SIGNOFF_METHOD_ID != 7 then 'Signed off'
    when s.SIGNOFF_METHOD_ID = 7 then 'Defered Signed off'
    else 'sign_off_overdue'
    end           as signoff_type,
    last_signoff_date,
    m.SIGNOFF_METHOD as ibv_method ,
    i.SIGN_OFF_IDENTITY as ibv_identity,
    e.SIGNOFF_EVENT as ibv_event,
    s.NOTES
    from CPS_DSCI_API.DC_WF_IB_SIGNOFF s
    join CPS_DSCI_API.DC_TYP_SIGNOFF_METHOD m on ( m.SIGNOFF_METHOD_ID=s.SIGNOFF_METHOD_ID)
    join CPS_DSCI_API.DC_TYP_SIGN_OFF_IDENTITY i on ( i.SIGN_OFF_IDENTITY_ID = s.SIGN_OFF_IDENTITY_ID)
    join CPS_DSCI_API.DC_TYP_SIGNOFF_EVENT e on ( e.SIGNOFF_EVENT_ID = s.signoff_event_id)
    join mx_date on ( mx_date.BOOKING_CONTRACT=s.BOOKING_CONTRACT and mx_date.last_signoff_date=s.CREATE_DTM)
    join CPS_DSCI_API.dc_BOOKINGS_CONTRACTS c
    on (c.BOOKING_CONTRACT = s.BOOKING_CONTRACT and c.is_deleted = 'F')
    where current_date between c.AGREEMENT_START_DATE and dateadd(day, 30, c.AGREEMENT_END_DATE)
    and s.is_deleted = 'F'
    ) -- qualify the last event with current date for correct status
    select  distinct BOOKING_CONTRACT,ibv_method, ibv_identity,ibv_event,notes,
    case
    when DATEDIFF(day, last_signoff_date,current_date) > 90 then  'sign_off_overdue'  -- regardless of type after 90 your overdue
    else signoff_type
    end as qualified_ibv,
    DATEDIFF(day, last_signoff_date,current_date) as days_since_last_signoff_event,
    last_signoff_date
    from so;
    """
    
    # Initialize analyzer
    analyzer = FinalViewAnalyzer(
        table_metadata_file='table_metadata.csv',
        view_metadata_file=r'C:\dev\table-column\view_metadata.csv'
    )
    
    # Generate comprehensive relationships
    df = analyzer.generate_relationship_csv(view_ddl, 'final_relationships.csv')
    
    # Display results
    print("\nGenerated Relationships:")
    print(df.to_string(index=False))

if __name__ == "__main__":
    main()