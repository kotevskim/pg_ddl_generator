#!/usr/bin/env python3
"""
PostgreSQL Schema DDL Generator
Generates clean DDL statements from a PostgreSQL schema with proper dependency ordering
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
            c.ordinal_position
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
    """Get foreign key constraints"""
    query = """
        SELECT
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name,
            tc.constraint_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_schema = %s
        AND tc.table_name = %s
        ORDER BY kcu.ordinal_position;
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


def format_column_type(data_type, udt_name, char_length, num_precision, num_scale, column_default):
    """Format column type with proper syntax"""
    # Handle SERIAL types
    if column_default and 'nextval' in column_default:
        if data_type == 'integer':
            return 'serial'
        elif data_type == 'bigint':
            return 'bigserial'
        elif data_type == 'smallint':
            return 'smallserial'

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
        return udt_name

    return data_type


def generate_create_table(schema, table, columns, pk_columns, fk_data):
    """Generate CREATE TABLE statement"""
    lines = []
    lines.append(f"CREATE TABLE {schema}.{table} (")

    column_defs = []
    fk_map = {fk[0]: (fk[1], fk[2]) for fk in fk_data}

    for col in columns:
        col_name, data_type, udt_name, char_len, num_prec, num_scale, nullable, default, position = col

        col_type = format_column_type(data_type, udt_name, char_len, num_prec, num_scale, default)
        col_def = f"    {col_name} {col_type}"

        # Add primary key inline for single column PKs
        if col_name in pk_columns and len(pk_columns) == 1:
            col_def += " PRIMARY KEY"

        # Add foreign key inline
        if col_name in fk_map:
            ref_table, ref_column = fk_map[col_name]
            col_def += f" REFERENCES {schema}.{ref_table}({ref_column})"

        # Add NOT NULL
        if nullable == 'NO' and col_name not in pk_columns:
            col_def += " NOT NULL"

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


def generate_ddl(host, port, database, user, password, schema, output_file):
    """Main function to generate DDL"""
    conn = connect_db(host, port, database, user, password)
    cursor = conn.cursor()

    ddl_output = []
    ddl_output.append(f"-- DDL for schema: {schema}")
    ddl_output.append(f"-- Generated from database: {database}\n")
    ddl_output.append(f"CREATE SCHEMA IF NOT EXISTS {schema};\n")  

    # Generate ENUM types
    ddl_output.append("-- ENUM Types")
    enums = get_enums(cursor, schema)
    if enums:
        for enum_name, enum_values in enums:
            values_str = ', '.join([f"'{v}'" for v in enum_values])
            ddl_output.append(f"CREATE TYPE {schema}.{enum_name} AS ENUM ({values_str});")
        ddl_output.append("")
    else:
        ddl_output.append("-- No ENUM types found\n")

    # Get all tables
    ddl_output.append("-- Tables")
    tables = get_tables(cursor, schema)

    if not tables:
        print(f"No tables found in schema '{schema}'", file=sys.stderr)
        cursor.close()
        conn.close()
        sys.exit(1)

    # Build dependency map
    table_dependencies = {}
    table_data = {}

    for table in tables:
        columns = get_table_columns(cursor, schema, table)
        pk_columns = get_primary_key(cursor, schema, table)
        fk_data = get_foreign_keys(cursor, schema, table)

        # Store table data
        table_data[table] = {
            'columns': columns,
            'pk_columns': pk_columns,
            'fk_data': fk_data
        }

        # Extract dependencies (referenced tables)
        dependencies = set()
        for fk in fk_data:
            ref_table = fk[1]
            if ref_table != table:  # Ignore self-references
                dependencies.add(ref_table)
        table_dependencies[table] = dependencies

    # Sort tables by dependencies
    sorted_tables = topological_sort_tables(tables, table_dependencies)

    # Generate CREATE TABLE statements in dependency order
    for table in sorted_tables:
        data = table_data[table]
        ddl_output.append(f"\n-- Table: {table}")
        ddl_output.append(generate_create_table(
            schema, 
            table, 
            data['columns'], 
            data['pk_columns'], 
            data['fk_data']
        ))

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
  python pg_ddl_generator.py --host localhost --port 5432 --database mydb \\
                              --user postgres --password secret --schema public \\
                              --output schema.sql
        """
    )

    parser.add_argument('--host', default='localhost', help='Database host (default: localhost)')
    parser.add_argument('--port', default='5432', help='Database port (default: 5432)')
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--user', required=True, help='Database user')
    parser.add_argument('--password', required=True, help='Database password')
    parser.add_argument('--schema', required=True, help='Schema name to generate DDL for')
    parser.add_argument('--output', '-o', help='Output file (default: stdout)')

    args = parser.parse_args()

    generate_ddl(
        args.host,
        args.port,
        args.database,
        args.user,
        args.password,
        args.schema,
        args.output
    )


if __name__ == '__main__':
    main()
