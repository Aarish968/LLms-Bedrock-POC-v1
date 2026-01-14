import csv
import uuid
import json
from datetime import datetime
from typing import List, Dict, Optional

class ComprehensiveSQLToCSVGenerator:
    def __init__(self):
        self.entities = []
        self.relationships = []
        self.operations = []
        self.relationship_types = []
        self.entity_lookup = {}
        
        self._init_relationship_types()
    
    def _init_relationship_types(self):
        """Initialize comprehensive relationship types"""
        types = [
            ("CONTAINS_TABLE", "STRUCTURE", "Schema contains table"),
            ("CONTAINS_COLUMN", "STRUCTURE", "Table contains column"),
            ("REFERENCES_TABLE", "USAGE", "Statement references table"),
            ("JOINS_WITH", "USAGE", "Table joins with another table"),
            ("INSERTS_INTO", "EXECUTION", "Statement inserts into table"),
            ("SELECTS_FROM", "USAGE", "Statement selects from table"),
            ("CREATES_TABLE", "EXECUTION", "Statement creates table"),
            ("UPDATES_TABLE", "EXECUTION", "Statement updates table"),
            ("DELETES_FROM", "EXECUTION", "Statement deletes from table"),
            ("USES_COLUMN", "USAGE", "Statement uses column"),
            ("DEFINES_COLUMN", "STRUCTURE", "Table defines column"),
        ]
        
        for name, category, description in types:
            self.relationship_types.append({
                'relationship_type_id': str(uuid.uuid4()),
                'name': name,
                'category': category,
                'description': description
            })
    
    def process_sql_results(self, sql_results: Dict):
        """Comprehensively process SQL parsing results"""
        
        for result in sql_results['sql']:
            if 'error' in result:
                continue
            
            file_path = result['file']
            stmt_type = result['type']
            tables = result['tables']
            columns = result['columns']
            joins = result.get('joins', [])
            sql_preview = result.get('sql_preview', '')
            
            # Create file entity
            file_entity_id = self._add_entity(
                entity_name=file_path.split('\\')[-1],
                entity_type="SQL_FILE",
                qualified_name=file_path,
                description=f"SQL file containing {stmt_type} statements"
            )
            
            # Process each table
            table_entities = []
            for table_name in tables:
                # Handle schema.table format
                schema_name = None
                if '.' in table_name:
                    parts = table_name.split('.')
                    if len(parts) >= 2:
                        schema_name = '.'.join(parts[:-1])
                        table_simple_name = parts[-1]
                    else:
                        table_simple_name = table_name
                else:
                    table_simple_name = table_name
                
                # Create schema entity if exists
                schema_entity_id = None
                if schema_name:
                    schema_entity_id = self._add_entity(
                        entity_name=schema_name,
                        entity_type="SCHEMA",
                        qualified_name=schema_name,
                        description=f"Database schema"
                    )
                
                # Create table entity
                table_entity_id = self._add_entity(
                    entity_name=table_simple_name,
                    entity_type="TABLE",
                    qualified_name=table_name,
                    description=f"Table referenced in {stmt_type} statement",
                    parent_entity_id=schema_entity_id
                )
                table_entities.append(table_entity_id)
                
                # Create schema contains table relationship
                if schema_entity_id:
                    self._add_relationship(
                        source_entity_id=schema_entity_id,
                        target_entity_id=table_entity_id,
                        relationship_type="CONTAINS_TABLE",
                        detail=f"Schema contains table {table_simple_name}"
                    )
                
                # Create statement-table relationships based on type
                if stmt_type == "INSERT":
                    self._add_relationship(
                        source_entity_id=file_entity_id,
                        target_entity_id=table_entity_id,
                        relationship_type="INSERTS_INTO",
                        detail=f"INSERT statement targets table"
                    )
                elif stmt_type == "SELECT":
                    self._add_relationship(
                        source_entity_id=file_entity_id,
                        target_entity_id=table_entity_id,
                        relationship_type="SELECTS_FROM",
                        detail=f"SELECT statement reads from table"
                    )
                elif stmt_type == "CREATE":
                    self._add_relationship(
                        source_entity_id=file_entity_id,
                        target_entity_id=table_entity_id,
                        relationship_type="CREATES_TABLE",
                        detail=f"CREATE statement defines table"
                    )
                else:
                    self._add_relationship(
                        source_entity_id=file_entity_id,
                        target_entity_id=table_entity_id,
                        relationship_type="REFERENCES_TABLE",
                        detail=f"{stmt_type} statement references table"
                    )
            
            # Process columns
            for column_name in columns:
                # Try to associate column with table
                parent_table_id = table_entities[0] if table_entities else None
                
                # Handle table.column format
                if '.' in column_name:
                    parts = column_name.split('.')
                    if len(parts) >= 2:
                        table_part = parts[0]
                        col_part = parts[-1]
                        # Find matching table entity
                        for table_name in tables:
                            if table_part.lower() in table_name.lower():
                                parent_table_id = self.entity_lookup.get(table_name)
                                break
                        column_name = col_part
                
                # Create column entity
                column_qualified_name = f"{tables[0] if tables else 'unknown'}.{column_name}"
                column_entity_id = self._add_entity(
                    entity_name=column_name,
                    entity_type="COLUMN",
                    qualified_name=column_qualified_name,
                    description=f"Column used in {stmt_type} statement",
                    parent_entity_id=parent_table_id
                )
                
                # Create table contains column relationship
                if parent_table_id:
                    self._add_relationship(
                        source_entity_id=parent_table_id,
                        target_entity_id=column_entity_id,
                        relationship_type="CONTAINS_COLUMN",
                        detail=f"Table contains column {column_name}"
                    )
                
                # Create statement uses column relationship
                self._add_relationship(
                    source_entity_id=file_entity_id,
                    target_entity_id=column_entity_id,
                    relationship_type="USES_COLUMN",
                    detail=f"{stmt_type} statement uses column"
                )
            
            # Process joins
            for i, join in enumerate(joins):
                if i < len(table_entities) - 1:
                    self._add_relationship(
                        source_entity_id=table_entities[i],
                        target_entity_id=table_entities[i + 1],
                        relationship_type="JOINS_WITH",
                        detail=f"{join['type']} join between tables"
                    )
            
            # Create comprehensive operation record
            operation_metadata = {
                "statement_type": stmt_type,
                "tables_count": len(tables),
                "columns_count": len(columns),
                "joins_count": len(joins),
                "file_path": file_path
            }
            
            self._add_operation(
                operation_type=stmt_type,
                source_entity_id=file_entity_id,
                target_entity_id=table_entities[0] if table_entities else None,
                sql_snippet=sql_preview,
                execution_context=file_path,
                metadata=operation_metadata
            )
    
    def _add_entity(self, entity_name: str, entity_type: str, qualified_name: str, 
                   description: str = "", parent_entity_id: Optional[str] = None) -> str:
        """Add entity with comprehensive metadata"""
        if qualified_name in self.entity_lookup:
            return self.entity_lookup[qualified_name]
        
        entity_id = str(uuid.uuid4())
        entity = {
            'entity_id': entity_id,
            'entity_name': entity_name,
            'entity_type': entity_type,
            'qualified_name': qualified_name,
            'parent_entity_id': parent_entity_id or '',
            'description': description,
            'metadata': json.dumps({
                "entity_type": entity_type,
                "created_by": "SQL_PARSER",
                "source": "automated_analysis"
            }),
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        self.entities.append(entity)
        self.entity_lookup[qualified_name] = entity_id
        return entity_id
    
    def _add_relationship(self, source_entity_id: str, target_entity_id: str, 
                         relationship_type: str, detail: str = ""):
        """Add relationship with metadata"""
        rel_type_id = None
        for rt in self.relationship_types:
            if rt['name'] == relationship_type:
                rel_type_id = rt['relationship_type_id']
                break
        
        if not rel_type_id:
            return
        
        relationship = {
            'relationship_id': str(uuid.uuid4()),
            'source_entity_id': source_entity_id,
            'target_entity_id': target_entity_id,
            'relationship_type_id': rel_type_id,
            'relationship_detail': detail,
            'metadata': json.dumps({
                "relationship_type": relationship_type,
                "created_by": "SQL_PARSER"
            }),
            'created_at': datetime.now().isoformat()
        }
        self.relationships.append(relationship)
    
    def _add_operation(self, operation_type: str, source_entity_id: str, 
                      target_entity_id: Optional[str], sql_snippet: str,
                      execution_context: str, metadata: Dict):
        """Add comprehensive operation record"""
        operation = {
            'operation_id': str(uuid.uuid4()),
            'operation_type': operation_type,
            'source_entity_id': source_entity_id,
            'target_entity_id': target_entity_id or '',
            'performed_at': datetime.now().isoformat(),
            'performed_by': 'SQL_PARSER',
            'sql_snippet': sql_snippet,
            'execution_context': execution_context,
            'metadata': json.dumps(metadata)
        }
        self.operations.append(operation)
    
    def export_to_csv(self, output_dir: str = "comprehensive_csv"):
        """Export comprehensive data to CSV files"""
        from pathlib import Path
        
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Export entities
        with open(output_path / "entities.csv", 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['entity_id', 'entity_name', 'entity_type', 'qualified_name',
                         'parent_entity_id', 'description', 'metadata', 'created_at', 'updated_at']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.entities)
        
        # Export operations  
        with open(output_path / "operations.csv", 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['operation_id', 'operation_type', 'source_entity_id', 'target_entity_id',
                         'performed_at', 'performed_by', 'sql_snippet', 'execution_context', 'metadata']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.operations)
        
        # Export relationships
        with open(output_path / "relationships.csv", 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['relationship_id', 'source_entity_id', 'target_entity_id',
                         'relationship_type_id', 'relationship_detail', 'metadata', 'created_at']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.relationships)
        
        # Export relationship types
        with open(output_path / "relationship_types.csv", 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['relationship_type_id', 'name', 'category', 'description']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.relationship_types)
        
        print(f"\n Comprehensive CSV files generated in '{output_dir}/':")
        print(f"   entities.csv ({len(self.entities)} records)")
        print(f"   operations.csv ({len(self.operations)} records)")
        print(f"   relationships.csv ({len(self.relationships)} records)")
        print(f"   relationship_types.csv ({len(self.relationship_types)} records)")
        
        # Print summary
        entity_types = {}
        for entity in self.entities:
            et = entity['entity_type']
            entity_types[et] = entity_types.get(et, 0) + 1
        
        print(f"\n Entity Summary:")
        for entity_type, count in entity_types.items():
            print(f"   • {entity_type}: {count}")

# Use this comprehensive version
if __name__ == "__main__":
    # Run your SQL parser
    sample = files[:5]
    parser = SQLParser()
    sql_results = parser.parse(sample)
    print(sql_results[0])
    # Generate comprehensive CSV files
    csv_generator = ComprehensiveSQLToCSVGenerator()
    csv_generator.process_sql_results(sql_results)
    csv_generator.export_to_csv("comprehensive_analysis")