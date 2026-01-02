CREATE TABLE test_timestamp (
    idx INTEGER,
    res TIMESTAMP_NTZ
);

INSERT INTO test_timestamp (idx, res) VALUES (1, TIMESTAMP_NTZ '2023-05-15 13:45:30');
INSERT INTO test_timestamp (idx, res) VALUES (2, TIMESTAMP_NTZ '2000-01-01 00:00:00');
INSERT INTO test_timestamp (idx, res) VALUES (3, TIMESTAMP_NTZ '1969-07-20 20:17:40');
INSERT INTO test_timestamp (idx, res) VALUES (4, TIMESTAMP_NTZ '9999-12-31 23:59:59');
INSERT INTO test_timestamp (idx, res) VALUES (5, NULL);
