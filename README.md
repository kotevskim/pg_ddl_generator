# PostgreSQL Schema DDL Generator

Python tool that generates clean DDL from a PostgreSQL schema – suitable for squashed migrations and readable schema snapshots.

## What this tool does

- Produces **pretty SQL** with:
  - Inlined primary keys (`bigserial PRIMARY KEY`)
  - Inlined foreign keys (`REFERENCES schema.table(id)`)
  - Inlined `NOT NULL` constraints
- Exports:
  - ENUM types (`CREATE TYPE ... AS ENUM`)
  - Tables (`CREATE TABLE ...`)
  - (Optional) Views
  - (Optional) Functions
- Orders tables using a topological sort so referenced tables are created before dependants.
- Excludes unnecessary indexes:
  - Primary key indexes on `id`
  - Single-column unique indexes that mirror constraints

The result is DDL that is easy to read, review, and use as a squashed migration:

```sql
CREATE TABLE people.family_members (
    id bigserial PRIMARY KEY,                         -- inlined PK with bigserial
    person_id bigint REFERENCES people.people(id),   -- inlined FK
    to_person_id bigint REFERENCES people.people(id),-- inlined FK
    is_primary boolean NOT NULL,                      -- inlined NOT NULL
    date_created timestamptz NOT NULL DEFAULT now()   -- inlined NOT NULL + DEFAULT
);

-- Non-redundant indexes are kept
CREATE UNIQUE INDEX unique_from_and_to_person
    ON people.family_members USING btree (person_id, to_person_id); -- multi-column unique index

CREATE UNIQUE INDEX idx_family_members_to_person_primary
    ON people.family_members USING btree (to_person_id)
    WHERE is_primary;  -- filtered index is preserved

-- Explicit unique constraint also exported
ALTER TABLE people.family_members
    ADD CONSTRAINT unique_from_and_to_person UNIQUE (person_id, to_person_id);
```

## Usage
```bash
python3 run.py \
  --database mydb \
  --user myuser \
  --password mypass \
  --schema person \
  --output person.sql
```

Optional Flags:
- `--host` – Database host (default: localhost)  
- `--port` – Database port (default: 5432)  
- `--output` – Output file (default: stdout)   
- `--include-views` – Include views in the output (default: off)  
- `--include-functions` – Include functions in the output (default: off)  