-- SQLite test database for transformer-v2
DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    value REAL
);

-- Example data:
INSERT INTO test_table (name, value) VALUES ('sample1', 123.45);
INSERT INTO test_table (name, value) VALUES ('sample2', 678.90);
