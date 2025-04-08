CREATE TABLE `my_table` (
  `field_primary_key`      INT PRIMARY KEY,      -- Primary key
  `field_not_null`         INT NOT NULL,         -- Not null
  
  -- Text types
  `field_char`             CHAR(10),            -- Fixed-length string
  `field_varchar`          VARCHAR(100),        -- Variable-length string
  `field_text`             TEXT,                -- Large variable-length string
  
  -- Number types
  `field_tinyint`          TINYINT,             -- Integer (0-255)
  `field_smallint`         SMALLINT,            -- Integer (-32,768 to 32,767)
  `field_int`              INT,                 -- Integer (-2.1B to 2.1B)
  `field_bigint`           BIGINT,              -- Large integer (-9 quintillion to 9 quintillion)
  `field_decimal`          DECIMAL(10, 2),      -- Fixed precision decimal
  `field_float`            FLOAT,               -- Approximate floating-point
  `field_real`             REAL,                -- Smaller floating point (equivalent to FLOAT in MySQL)
  
  -- Date and time types
  `field_date`             DATE,                -- Date only (YYYY-MM-DD)
  `field_time`             TIME,                -- Time only (HH:MM:SS)
  `field_datetime`         DATETIME,            -- Standard datetime
  `field_timestamp`        TIMESTAMP            -- Timestamp (auto-updating)
);
