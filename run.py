#!/usr/bin/env python3
"""
PostgreSQL Schema DDL Generator
Generates clean DDL statements from a PostgreSQL schema with proper dependency ordering
Includes tables, views, functions, indexes, and constraints
"""


import psycopg2
import argparse
import sys
from collections import defaultdict, deque



def connect_db(host, port, database, user, password):
    """Connect to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}", file=sys.stderr)
        sys.exit(1)

def get_schema_dependencies(cursor, schemas):
    """Get cross-schema foreign key dependencies using pg_catalog"""
    query = """
        SELECT DISTINCT
            source_ns.nspname AS source_schema,
            target_ns.nspname AS target_schema
        FROM pg_constraint con
        JOIN pg_class source_rel ON con.conrelid = source_rel.oid
        JOIN pg_namespace source_ns ON source_rel.relnamespace = source_ns.oid
        JOIN pg_class target_rel ON con.confrelid = target_rel.oid
        JOIN pg_namespace target_ns ON target_rel.relnamespace = target_ns.oid
        WHERE con.contype = 'f'
        AND source_ns.nspname = ANY(%s)
        AND target_ns.nspname = ANY(%s)
        AND source_ns.nspname != target_ns.nspname;
    """
    cursor.execute(query, (schemas, schemas))
    
    dependencies = defaultdict(set)
    for source_schema, target_schema in cursor.fetchall():
        dependencies[source_schema].add(target_schema)
    
    return dependencies

def topological_sort_schemas(schemas, schema_dependencies):
    """Sort schemas based on cross-schema foreign key dependencies"""
    # Build adjacency list and in-degree count
    graph = defaultdict(list)
    in_degree = {schema: 0 for schema in schemas}

    for schema in schemas:
        deps = schema_dependencies.get(schema, set())
        for dep in deps:
            if dep in in_degree and dep != schema:
                graph[dep].append(schema)
                in_degree[schema] += 1
                
    # DEBUG: Print the graph
    # print("DEBUG: Dependency graph for resource and organizations:")
    # print(f"  resource -> {graph.get('resource', [])}")
    # print(f"  organizations -> {graph.get('organizations', [])}")
    # print(f"  resource in_degree: {in_degree.get('resource', 0)}")
    # print(f"  organizations in_degree: {in_degree.get('organizations', 0)}")
    # print(f"  organizations dependencies: {schema_dependencies.get('organizations', set())}")
    # print(f"  resource dependencies: {schema_dependencies.get('resource', set())}")

    # Kahn's algorithm for topological sorting
    queue = deque([schema for schema in schemas if in_degree[schema] == 0])
    sorted_schemas = []

    while queue:
        current = queue.popleft()
        sorted_schemas.append(current)

        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    # Handle circular dependencies - add remaining schemas
    remaining = [schema for schema in schemas if schema not in sorted_schemas]
    if remaining:
        sorted_schemas.extend(sorted(remaining))

    return sorted_schemas


def get_enums(cursor, schema):
    """Get all ENUM types in the schema"""
    query = """
        SELECT 
            t.typname as enum_name,
            array_agg(e.enumlabel ORDER BY e.enumsortorder) as enum_values
        FROM pg_type t
        JOIN pg_enum e ON t.oid = e.enumtypid
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE n.nspname = %s
        GROUP BY t.typname
        ORDER BY t.typname;
    """
    cursor.execute(query, (schema,))
    return cursor.fetchall()


def get_composite_types(cursor, schema):
    """Get all composite types in the schema"""
    query = """
        SELECT
            t.typname as type_name,
            array_agg(
                a.attname || ' ' || 
                format_type(a.atttypid, a.atttypmod)
                ORDER BY a.attnum
            ) as attributes
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        JOIN pg_attribute a ON a.attrelid = t.typrelid
        WHERE n.nspname = %s
        AND t.typtype = 'c'
        AND a.attnum > 0
        -- filter out regular tables, views, and mat views - leaving only true composite types
        AND NOT EXISTS (
            SELECT 1 FROM pg_class c 
            WHERE c.oid = t.typrelid 
            AND c.relkind IN ('r', 'v', 'm')
        )
        AND NOT a.attisdropped
        GROUP BY t.typname
        ORDER BY t.typname;
    """
    cursor.execute(query, (schema,))
    return cursor.fetchall()


def get_tables(cursor, schema):
    """Get all tables in the schema"""
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """
    cursor.execute(query, (schema,))
    return [row[0] for row in cursor.fetchall()]



def get_views(cursor, schema):
    """Get all views in the schema"""
    query = """
        SELECT table_name, view_definition
        FROM information_schema.views
        WHERE table_schema = %s
        ORDER BY table_name;
    """
    cursor.execute(query, (schema,))
    return cursor.fetchall()



def get_functions(cursor, schema):
    """Get all functions in the schema"""
    query = """
        SELECT 
            p.proname as function_name,
            pg_get_functiondef(p.oid) as function_def
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = %s
        AND p.prokind = 'f'
        ORDER BY p.proname;
    """
    cursor.execute(query, (schema,))
    return cursor.fetchall()



def get_single_column_unique_constraints(cursor, schema, table):
    """Get single-column unique constraints for inlining"""
    query = """
        SELECT
            kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'UNIQUE'
        AND tc.table_schema = %s
        AND tc.table_name = %s
        GROUP BY tc.constraint_name, kcu.column_name
        HAVING COUNT(*) = 1;
    """
    cursor.execute(query, (schema, table))
    return [row[0] for row in cursor.fetchall()]



def get_unique_constraints_columns(cursor, schema, table):
    """Get all unique constraints with their column lists for filtering indexes"""
    query = """
        SELECT
            string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as columns
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'UNIQUE'
        AND tc.table_schema = %s
        AND tc.table_name = %s
        GROUP BY tc.constraint_name;
    """
    cursor.execute(query, (schema, table))
    return [row[0] for row in cursor.fetchall()]



def get_indexes_for_table(cursor, schema, table, unique_columns, unique_constraint_columns, pk_columns):
    """Get all indexes for a specific table (excluding primary keys and unique indexes with constraints)"""
    
    # First, get all constraint names for this table (PK and UNIQUE)
    constraint_query = """
        SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_schema = %s
        AND table_name = %s
        AND constraint_type IN ('PRIMARY KEY', 'UNIQUE');
    """
    cursor.execute(constraint_query, (schema, table))
    constraint_names = {row[0] for row in cursor.fetchall()}
    
    # Now get indexes
    query = """
        SELECT
            indexname,
            indexdef
        FROM pg_indexes
        WHERE schemaname = %s
        AND tablename = %s
        AND indexdef NOT LIKE '%%PRIMARY KEY%%'
        ORDER BY indexname;
    """
    cursor.execute(query, (schema, table))
    indexes = cursor.fetchall()

    # Filter out unique indexes that have corresponding constraints
    filtered_indexes = []
    for index_name, index_def in indexes:
        # Skip if this index name matches a constraint name
        if index_name in constraint_names:
            continue
            
        # Skip if it's a unique index on just the id column
        if 'UNIQUE' in index_def.upper() and '(id)' in index_def:
            continue

        # Skip if it's a unique index on any single-column primary key
        skip_pk_index = False
        if 'UNIQUE' in index_def.upper() and len(pk_columns) == 1:
            for pk_col in pk_columns:
                if f'({pk_col})' in index_def and 'WHERE' not in index_def.upper():
                    skip_pk_index = True
                    break
        
        if skip_pk_index:
            continue

        # Skip if it's a single-column unique index on any column that's inlined
        is_single_col_unique = False
        if 'UNIQUE' in index_def.upper():
            for col in unique_columns:
                # Check if index is on this single column (without WHERE clause for filtered indexes)
                if f'({col})' in index_def and 'WHERE' not in index_def.upper():
                    is_single_col_unique = True
                    break

        if is_single_col_unique:
            continue

        # Skip if it's a multi-column unique index that matches a unique constraint
        # Extract columns from index definition
        if 'UNIQUE' in index_def.upper() and 'WHERE' not in index_def.upper():
            # Try to match against unique constraint columns
            skip_index = False
            for constraint_cols in unique_constraint_columns:
                # Normalize column list from index def
                # Example: "USING btree (permission_group_id, permission_id)"
                if '(' in index_def and ')' in index_def:
                    cols_part = index_def[index_def.rfind('('):index_def.rfind(')')+1]
                    # Remove parentheses and clean up
                    index_cols = cols_part.strip('()').replace(' ', '')
                    constraint_cols_clean = constraint_cols.replace(' ', '')

                    if index_cols == constraint_cols_clean:
                        skip_index = True
                        break

            if skip_index:
                continue

        filtered_indexes.append((index_name, index_def))

    return filtered_indexes


def get_unique_constraints_for_table(cursor, schema, table):
    """Get unique constraints for a specific table (multi-column only)"""
    query = """
        SELECT
            tc.constraint_name,
            string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) as columns,
            COUNT(*) as column_count
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'UNIQUE'
        AND tc.table_schema = %s
        AND tc.table_name = %s
        GROUP BY tc.constraint_name
        HAVING COUNT(*) > 1
        ORDER BY tc.constraint_name;
    """
    cursor.execute(query, (schema, table))
    return [(name, cols) for name, cols, _ in cursor.fetchall()]



def get_check_constraints_for_table(cursor, schema, table):
    """Get check constraints for a specific table"""
    query = """
        SELECT
            tc.constraint_name,
            cc.check_clause,
            (SELECT COUNT(*) 
             FROM information_schema.constraint_column_usage ccu
             WHERE ccu.constraint_name = tc.constraint_name
             AND ccu.constraint_schema = tc.table_schema) as column_count
        FROM information_schema.table_constraints tc
        JOIN information_schema.check_constraints cc
            ON tc.constraint_name = cc.constraint_name
            AND tc.table_schema = cc.constraint_schema
        WHERE tc.constraint_type = 'CHECK'
        AND tc.table_schema = %s
        AND tc.table_name = %s
        ORDER BY tc.constraint_name;
    """
    cursor.execute(query, (schema, table))


    # Filter out single-column NOT NULL checks
    constraints = []
    for constraint_name, check_clause, column_count in cursor.fetchall():
        # Skip if it's a simple NOT NULL check on one column
        check_lower = check_clause.lower().strip()
        if column_count == 1 and 'is not null' in check_lower and check_lower.count('(') <= 2:
            continue
        constraints.append((constraint_name, check_clause))


    return constraints



def get_table_columns(cursor, schema, table):
    """Get columns for a table with their properties"""
    query = """
        SELECT
            c.column_name,
            c.data_type,
            c.udt_name,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.is_nullable,
            c.column_default,
            c.ordinal_position,
            CASE 
                WHEN c.data_type = 'USER-DEFINED' THEN c.udt_schema
                ELSE NULL
            END as udt_schema
        FROM information_schema.columns c
        WHERE c.table_schema = %s
        AND c.table_name = %s
        ORDER BY c.ordinal_position;
    """
    cursor.execute(query, (schema, table))
    return cursor.fetchall()



def get_primary_key(cursor, schema, table):
    """Get primary key constraint"""
    query = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = %s
        AND tc.table_name = %s
        ORDER BY kcu.ordinal_position;
    """
    cursor.execute(query, (schema, table))
    return [row[0] for row in cursor.fetchall()]


def get_foreign_keys(cursor, schema, table):
    """Get foreign key constraints including cross-schema references using pg_catalog"""
    query = """
        SELECT
            a.attname AS column_name,
            target_ns.nspname AS foreign_table_schema,
            target_class.relname AS foreign_table_name,
            target_a.attname AS foreign_column_name,
            con.conname AS constraint_name
        FROM pg_constraint con
        JOIN pg_class source_class ON con.conrelid = source_class.oid
        JOIN pg_namespace source_ns ON source_class.relnamespace = source_ns.oid
        JOIN pg_class target_class ON con.confrelid = target_class.oid
        JOIN pg_namespace target_ns ON target_class.relnamespace = target_ns.oid
        JOIN pg_attribute a ON a.attnum = ANY(con.conkey) AND a.attrelid = con.conrelid
        JOIN pg_attribute target_a ON target_a.attnum = ANY(con.confkey) AND target_a.attrelid = con.confrelid
        WHERE con.contype = 'f'
        AND source_ns.nspname = %s
        AND source_class.relname = %s
        ORDER BY a.attnum;
    """
    cursor.execute(query, (schema, table))
    return cursor.fetchall()


def topological_sort_tables(tables, table_dependencies):
    """Sort tables based on foreign key dependencies using topological sort"""
    # Build adjacency list and in-degree count
    graph = defaultdict(list)
    in_degree = {table: 0 for table in tables}


    for table in tables:
        deps = table_dependencies.get(table, set())
        for dep in deps:
            if dep in in_degree and dep != table:  # Ignore self-references
                graph[dep].append(table)
                in_degree[table] += 1


    # Kahn's algorithm for topological sorting
    queue = deque([table for table in tables if in_degree[table] == 0])
    sorted_tables = []


    while queue:
        current = queue.popleft()
        sorted_tables.append(current)


        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)


    # Handle circular dependencies - add remaining tables
    remaining = [table for table in tables if table not in sorted_tables]
    if remaining:
        sorted_tables.extend(sorted(remaining))


    return sorted_tables

def format_column_type(data_type, udt_name, char_length, num_precision, num_scale, column_default, udt_schema):
    """Format column type with proper syntax"""
    # Handle SERIAL types
    if column_default and 'nextval' in column_default:
        if data_type == 'integer':
            return 'serial'
        elif data_type == 'bigint':
            return 'bigserial'
        elif data_type == 'smallint':
            return 'smallserial'

    # Handle ARRAY types
    if data_type == 'ARRAY':
        # udt_name will be like '_text' for text[], '_int4' for integer[]
        array_type_map = {
            '_text': 'text[]',
            '_varchar': 'varchar[]',
            '_char': 'char[]',
            '_int2': 'smallint[]',
            '_int4': 'integer[]',
            '_int8': 'bigint[]',
            '_float4': 'real[]',
            '_float8': 'double precision[]',
            '_numeric': 'numeric[]',
            '_bool': 'boolean[]',
            '_date': 'date[]',
            '_time': 'time[]',
            '_timetz': 'timetz[]',
            '_timestamp': 'timestamp[]',
            '_timestamptz': 'timestamptz[]',
            '_uuid': 'uuid[]',
            '_jsonb': 'jsonb[]',
            '_json': 'json[]',
        }
        return array_type_map.get(udt_name, f'{udt_name.lstrip("_")}[]')

    # Handle standard types
    type_map = {
        'character varying': f'varchar({char_length})' if char_length else 'varchar',
        'character': f'char({char_length})' if char_length else 'char',
        'timestamp without time zone': 'timestamp',
        'timestamp with time zone': 'timestamptz',
        'time without time zone': 'time',
        'time with time zone': 'timetz',
        'double precision': 'double precision',
    }

    if data_type in type_map:
        return type_map[data_type]

    # Handle numeric with precision
    if data_type == 'numeric' and num_precision:
        if num_scale:
            return f'numeric({num_precision},{num_scale})'
        return f'numeric({num_precision})'

    # Use udt_name for custom types (enums)
    if data_type == 'USER-DEFINED':
        return f'{udt_schema}.{udt_name}'

    return data_type

def generate_create_table(schema, table, columns, pk_columns, fk_data, unique_columns):
    """Generate CREATE TABLE statement"""
    lines = []
    lines.append(f"CREATE TABLE {schema}.{table} (")


    column_defs = []
    fk_map = {fk[0]: (fk[1], fk[2], fk[3]) for fk in fk_data}


    for col in columns:
        col_name, data_type, udt_name, char_len, num_prec, num_scale, nullable, default, position, udt_schema = col

        col_type = format_column_type(data_type, udt_name, char_len, num_prec, num_scale, default, udt_schema)
        col_def = f"    {col_name} {col_type}"


        # Add primary key inline for single column PKs
        if col_name in pk_columns and len(pk_columns) == 1:
            col_def += " PRIMARY KEY"


        # Add foreign key inline (with schema prefix if cross-schema)
        if col_name in fk_map:
            ref_schema, ref_table, ref_column = fk_map[col_name]
            col_def += f" REFERENCES {ref_schema}.{ref_table}({ref_column})"


        # Add NOT NULL
        if nullable == 'NO' and col_name not in pk_columns:
            col_def += " NOT NULL"


        # Add UNIQUE for single-column unique constraints
        if col_name in unique_columns:
            col_def += " UNIQUE"


        # Add DEFAULT (skip for serial types)
        if default and 'nextval' not in default:
            col_def += f" DEFAULT {default}"


        column_defs.append(col_def)


    # Add composite primary key constraint
    if len(pk_columns) > 1:
        pk_def = f"    PRIMARY KEY ({', '.join(pk_columns)})"
        column_defs.append(pk_def)


    # Join column definitions with commas and newlines
    columns_text = ",\n".join(column_defs)
    lines.append(columns_text)
    lines.append(");")


    return "\n".join(lines)



def generate_schema_ddl(cursor, schema, include_views, include_functions):
    """Generate DDL for a single schema"""
    ddl_output = []
    ddl_output.append(f"-- Schema: {schema}")
    ddl_output.append(f"CREATE SCHEMA IF NOT EXISTS {schema};\n")


    # Generate composite types
    composite_types = get_composite_types(cursor, schema)
    if composite_types:
        ddl_output.append("-- Composite Types")
        for type_name, attributes in composite_types:
            attrs_str = ',\n    '.join(attributes)
            ddl_output.append(f"CREATE TYPE {schema}.{type_name} AS (\n    {attrs_str}\n);")
        ddl_output.append("")

    # Generate ENUM types
    enums = get_enums(cursor, schema)
    if enums:
        ddl_output.append("-- ENUM Types")
        for enum_name, enum_values in enums:
            values_str = ', '.join([f"'{v}'" for v in enum_values])
            ddl_output.append(f"CREATE TYPE {schema}.{enum_name} AS ENUM ({values_str});")
        ddl_output.append("")


    # Get all tables
    tables = get_tables(cursor, schema)


    if tables:
        ddl_output.append("-- Tables")


        # Build dependency map
        table_dependencies = {}
        table_data = {}


        for table in tables:
            columns = get_table_columns(cursor, schema, table)
            pk_columns = get_primary_key(cursor, schema, table)
            fk_data = get_foreign_keys(cursor, schema, table)
            unique_columns = get_single_column_unique_constraints(cursor, schema, table)
            unique_constraint_columns = get_unique_constraints_columns(cursor, schema, table)


            # Store table data
            table_data[table] = {
                'columns': columns,
                'pk_columns': pk_columns,
                'fk_data': fk_data,
                'unique_columns': unique_columns,
                'unique_constraint_columns': unique_constraint_columns
            }


            # Extract dependencies (only for same-schema references for ordering)
            dependencies = set()
            for fk in fk_data:
                col_name, ref_schema, ref_table, ref_column, constraint_name = fk
                # Only track dependencies within the same schema for topological sort
                if ref_schema == schema and ref_table != table:
                    dependencies.add(ref_table)
            table_dependencies[table] = dependencies


        # Sort tables by dependencies
        sorted_tables = topological_sort_tables(tables, table_dependencies)


        # Generate CREATE TABLE statements with their indexes and constraints
        for table in sorted_tables:
            data = table_data[table]
            ddl_output.append("")
            ddl_output.append(generate_create_table(
                schema, 
                table, 
                data['columns'], 
                data['pk_columns'], 
                data['fk_data'],
                data['unique_columns']
            ))


            # Add indexes for this table (pass constraint columns to filter)
            indexes = get_indexes_for_table(cursor, schema, table, data['unique_columns'], data['unique_constraint_columns'], data['pk_columns'])
            for index_name, index_def in indexes:
                ddl_output.append(f"{index_def};")


            # Add unique constraints for this table (multi-column only now)
            unique_constraints = get_unique_constraints_for_table(cursor, schema, table)
            for constraint_name, columns in unique_constraints:
                ddl_output.append(f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {constraint_name} UNIQUE ({columns});")


            # Add check constraints for this table
            check_constraints = get_check_constraints_for_table(cursor, schema, table)
            for constraint_name, check_clause in check_constraints:
                ddl_output.append(f"ALTER TABLE {schema}.{table} ADD CONSTRAINT {constraint_name} CHECK ({check_clause});")


    # Generate functions if requested
    if include_functions:
        functions = get_functions(cursor, schema)
        if functions:
            ddl_output.append("\n-- Functions")
            for func_name, func_def in functions:
                ddl_output.append(f"{func_def};")
                ddl_output.append("")


    # Generate views if requested
    if include_views:
        views = get_views(cursor, schema)
        if views:
            ddl_output.append("\n-- Views")
            for view_name, view_def in views:
                view_def = view_def.strip()
                if not view_def.endswith(';'):
                    view_def += ';'
                ddl_output.append(f"CREATE OR REPLACE VIEW {schema}.{view_name} AS\n{view_def}")
                ddl_output.append("")


    return ddl_output


def generate_ddl(host, port, database, user, password, schemas, output_file, include_views, include_functions):
    """Main function to generate DDL for multiple schemas"""
    conn = connect_db(host, port, database, user, password)
    cursor = conn.cursor()


    # Sort schemas by dependencies
    schema_dependencies = get_schema_dependencies(cursor, schemas)
    sorted_schemas = topological_sort_schemas(schemas, schema_dependencies)


    ddl_output = []
    ddl_output.append(f"-- DDL for schemas: {', '.join(sorted_schemas)}")
    ddl_output.append(f"-- Generated from database: {database}")
    ddl_output.append(f"-- Schema ordering based on foreign key dependencies\n")


    # Generate DDL for each schema
    for i, schema in enumerate(sorted_schemas):
        if i > 0:
            ddl_output.append("\n\n" + "--" + "="*80)
            ddl_output.append("")

        schema_ddl = generate_schema_ddl(cursor, schema, include_views, include_functions)
        ddl_output.extend(schema_ddl)


    cursor.close()
    conn.close()


    # Write to file or stdout
    output = "\n".join(ddl_output)
    if output_file:
        with open(output_file, 'w') as f:
            f.write(output)
        print(f"DDL written to {output_file}")
    else:
        print(output)



def main():
    parser = argparse.ArgumentParser(
        description='Generate clean DDL from PostgreSQL schema with proper dependency ordering',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  # Single schema
  python pg_ddl_generator.py --host localhost --port 5432 --database mydb \\
                              --user postgres --password secret --schema public \\
                              --output schema.sql --include-views --include-functions

  # Multiple schemas (automatically sorted by dependencies)
  python pg_ddl_generator.py --host localhost --port 5432 --database mydb \\
                              --user postgres --password secret --schema access,person,common \\
                              --output all_schemas.sql
        """
    )


    parser.add_argument('--host', default='localhost', help='Database host (default: localhost)')
    parser.add_argument('--port', default='5432', help='Database port (default: 5432)')
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--user', required=True, help='Database user')
    parser.add_argument('--password', required=True, help='Database password')
    parser.add_argument('--schema', required=True, help='Schema name(s) to generate DDL for (comma-separated for multiple)')
    parser.add_argument('--output', '-o', help='Output file (default: stdout)')
    parser.add_argument('--include-views', action='store_true', help='Include views in the output')
    parser.add_argument('--include-functions', action='store_true', help='Include functions in the output')


    args = parser.parse_args()

    # Parse schemas (split by comma, strip whitespace)
    schemas = [s.strip() for s in args.schema.split(',')]


    generate_ddl(
        args.host,
        args.port,
        args.database,
        args.user,
        args.password,
        schemas,
        args.output,
        args.include_views,
        args.include_functions
    )



if __name__ == '__main__':
    main()
