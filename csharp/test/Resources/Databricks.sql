
 -- Copyright (c) 2025 ADBC Drivers Contributors
 --
 -- This file has been modified from its original version, which is
 -- under the Apache License:
 --
 -- Licensed to the Apache Software Foundation (ASF) under one
 -- or more contributor license agreements.  See the NOTICE file
 -- distributed with this work for additional information
 -- regarding copyright ownership.  The ASF licenses this file
 -- to you under the Apache License, Version 2.0 (the
 -- "License"); you may not use this file except in compliance
 -- with the License.  You may obtain a copy of the License at

 --    http://www.apache.org/licenses/LICENSE-2.0

 -- Unless required by applicable law or agreed to in writing, software
 -- distributed under the License is distributed on an "AS IS" BASIS,
 -- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 -- See the License for the specific language governing permissions and
 -- limitations under the License.

CREATE OR REPLACE TABLE {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (
  id LONG,
  byte BYTE,
  short SHORT,
  integer INT,
  float FLOAT,
  number DOUBLE,
  decimal NUMERIC(38, 9),
  is_active BOOLEAN,
  name STRING,
  data BINARY,
  date DATE,
  timestamp TIMESTAMP,
  timestamp_ntz TIMESTAMP_NTZ,
  timestamp_ltz TIMESTAMP_LTZ,
  numbers ARRAY<LONG>,
  person STRUCT <
    name STRING,
    age LONG
  >,
  map MAP <
    INT,
    STRING
  >,
  varchar VARCHAR(255),
  char CHAR(10)
) USING DELTA;

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (
    id,
    byte, short, integer, float, number, decimal,
    is_active,
    name, data,
    date, timestamp, timestamp_ntz, timestamp_ltz,
    numbers,
    person,
    map,
    varchar,
    char
)
VALUES (
    1,
    2, 3, 4, 7.89, 1.23, 4.56,
    TRUE,
    'John Doe',
    -- hex-encoded value `abc123`
    X'616263313233',
    '2023-09-08', '2023-09-08 12:34:56', '2023-09-08 12:34:56', '2023-09-08 12:34:56+00:00',
    ARRAY(1, 2, 3),
    STRUCT('John Doe', 30),
    MAP(1, 'John Doe'),
    'John Doe',
    'John Doe'
);

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (
    id,
    byte, short, integer, float, number, decimal,
    is_active,
    name, data,
    date, timestamp, timestamp_ntz, timestamp_ltz,
    numbers,
    person,
    map,
    varchar,
    char
)
VALUES (
    2,
    127, 32767, 2147483647, 3.4028234663852886e+38, 1.7976931348623157e+308, 9.99999999999999999999999999999999E+28BD,
    FALSE,
    'Jane Doe',
    -- hex-encoded `def456`
    X'646566343536',
    '2023-09-09', '2023-09-09 13:45:57', '2023-09-09 13:45:57', '2023-09-09 13:45:57+00:00',
    ARRAY(4, 5, 6),
    STRUCT('Jane Doe', 40),
    MAP(1, 'John Doe'),
    'Jane Doe',
    'Jane Doe'
);

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (
    id,
    byte, short, integer, float, number, decimal,
    is_active,
    name, data,
    date, timestamp, timestamp_ntz, timestamp_ltz,
    numbers,
    person,
    map,
    varchar,
    char
)
VALUES (
    3,
    -128, -32768, -2147483648, -3.4028234663852886e+38, -1.7976931348623157e+308, -9.99999999999999999999999999999999E+28BD,
    FALSE,
    'Jack Doe',
    -- hex-encoded `def456`
    X'646566343536',
    '1556-01-02', '1970-01-01 00:00:00', '1970-01-01 00:00:00', '9999-12-31 23:59:59+00:00',
    ARRAY(7, 8, 9),
    STRUCT('Jack Doe', 50),
    MAP(1, 'John Doe'),
    'Jack Doe',
    'Jack Doe'
);

-- Add 10 more rows to bring total to 12 (to fix fractional batch size calculation)
INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (id, byte, short, integer, float, number, decimal, is_active, name, data, date, timestamp, timestamp_ntz, timestamp_ltz, numbers, person, map, varchar, char)
VALUES (4, 10, 100, 1000, 1.1, 2.2, 3.3, TRUE, 'User 4', X'757365723034', '2023-09-10', '2023-09-10 10:00:00', '2023-09-10 10:00:00', '2023-09-10 10:00:00+00:00', ARRAY(10, 11, 12), STRUCT('User 4', 25), MAP(4, 'User 4'), 'User 4', 'User 4');

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (id, byte, short, integer, float, number, decimal, is_active, name, data, date, timestamp, timestamp_ntz, timestamp_ltz, numbers, person, map, varchar, char)
VALUES (5, 11, 110, 1100, 1.2, 2.3, 3.4, FALSE, 'User 5', X'757365723035', '2023-09-11', '2023-09-11 11:00:00', '2023-09-11 11:00:00', '2023-09-11 11:00:00+00:00', ARRAY(13, 14, 15), STRUCT('User 5', 26), MAP(5, 'User 5'), 'User 5', 'User 5');

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (id, byte, short, integer, float, number, decimal, is_active, name, data, date, timestamp, timestamp_ntz, timestamp_ltz, numbers, person, map, varchar, char)
VALUES (6, 12, 120, 1200, 1.3, 2.4, 3.5, TRUE, 'User 6', X'757365723036', '2023-09-12', '2023-09-12 12:00:00', '2023-09-12 12:00:00', '2023-09-12 12:00:00+00:00', ARRAY(16, 17, 18), STRUCT('User 6', 27), MAP(6, 'User 6'), 'User 6', 'User 6');

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (id, byte, short, integer, float, number, decimal, is_active, name, data, date, timestamp, timestamp_ntz, timestamp_ltz, numbers, person, map, varchar, char)
VALUES (7, 13, 130, 1300, 1.4, 2.5, 3.6, FALSE, 'User 7', X'757365723037', '2023-09-13', '2023-09-13 13:00:00', '2023-09-13 13:00:00', '2023-09-13 13:00:00+00:00', ARRAY(19, 20, 21), STRUCT('User 7', 28), MAP(7, 'User 7'), 'User 7', 'User 7');

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (id, byte, short, integer, float, number, decimal, is_active, name, data, date, timestamp, timestamp_ntz, timestamp_ltz, numbers, person, map, varchar, char)
VALUES (8, 14, 140, 1400, 1.5, 2.6, 3.7, TRUE, 'User 8', X'757365723038', '2023-09-14', '2023-09-14 14:00:00', '2023-09-14 14:00:00', '2023-09-14 14:00:00+00:00', ARRAY(22, 23, 24), STRUCT('User 8', 29), MAP(8, 'User 8'), 'User 8', 'User 8');

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (id, byte, short, integer, float, number, decimal, is_active, name, data, date, timestamp, timestamp_ntz, timestamp_ltz, numbers, person, map, varchar, char)
VALUES (9, 15, 150, 1500, 1.6, 2.7, 3.8, FALSE, 'User 9', X'757365723039', '2023-09-15', '2023-09-15 15:00:00', '2023-09-15 15:00:00', '2023-09-15 15:00:00+00:00', ARRAY(25, 26, 27), STRUCT('User 9', 30), MAP(9, 'User 9'), 'User 9', 'User 9');

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (id, byte, short, integer, float, number, decimal, is_active, name, data, date, timestamp, timestamp_ntz, timestamp_ltz, numbers, person, map, varchar, char)
VALUES (10, 16, 160, 1600, 1.7, 2.8, 3.9, TRUE, 'User 10', X'75736572313030', '2023-09-16', '2023-09-16 16:00:00', '2023-09-16 16:00:00', '2023-09-16 16:00:00+00:00', ARRAY(28, 29, 30), STRUCT('User 10', 31), MAP(10, 'User 10'), 'User 10', 'User 10');

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (id, byte, short, integer, float, number, decimal, is_active, name, data, date, timestamp, timestamp_ntz, timestamp_ltz, numbers, person, map, varchar, char)
VALUES (11, 17, 170, 1700, 1.8, 2.9, 4.0, FALSE, 'User 11', X'75736572313131', '2023-09-17', '2023-09-17 17:00:00', '2023-09-17 17:00:00', '2023-09-17 17:00:00+00:00', ARRAY(31, 32, 33), STRUCT('User 11', 32), MAP(11, 'User 11'), 'User 11', 'User 11');

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (id, byte, short, integer, float, number, decimal, is_active, name, data, date, timestamp, timestamp_ntz, timestamp_ltz, numbers, person, map, varchar, char)
VALUES (12, 18, 180, 1800, 1.9, 3.0, 4.1, TRUE, 'User 12', X'75736572313232', '2023-09-18', '2023-09-18 18:00:00', '2023-09-18 18:00:00', '2023-09-18 18:00:00+00:00', ARRAY(34, 35, 36), STRUCT('User 12', 33), MAP(12, 'User 12'), 'User 12', 'User 12');

INSERT INTO {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE} (id, byte, short, integer, float, number, decimal, is_active, name, data, date, timestamp, timestamp_ntz, timestamp_ltz, numbers, person, map, varchar, char)
VALUES (13, 19, 190, 1900, 2.0, 3.1, 4.2, FALSE, 'User 13', X'75736572313333', '2023-09-19', '2023-09-19 19:00:00', '2023-09-19 19:00:00', '2023-09-19 19:00:00+00:00', ARRAY(37, 38, 39), STRUCT('User 13', 34), MAP(13, 'User 13'), 'User 13', 'User 13');

UPDATE {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE}
    SET short = 0
    WHERE id = 3;

DELETE FROM {ADBC_CATALOG}.{ADBC_DATASET}.{ADBC_TABLE}
    WHERE id = 3;
