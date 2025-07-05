CREATE DATABASE movies;

USE movies;

CREATE OR REPLACE TABLE raw_movie_bookings (
    booking_id STRING,
    customer_id STRING,
    movie_id STRING,
    booking_date TIMESTAMP,
    status STRING, -- "BOOKED", "CANCELLED", "COMPLETED"
    ticket_count INT,
    ticket_price NUMBER(10, 2)
);

CREATE OR REPLACE STREAM movie_bookings_stream
ON TABLE raw_movie_bookings;

CREATE OR REPLACE TABLE movie_booking_cdc_events (
    booking_id STRING,
    customer_id STRING,
    movie_id STRING,
    booking_date TIMESTAMP,
    status STRING,
    ticket_count INT,
    ticket_price NUMBER(10, 2),
    change_type STRING,
    is_update BOOLEAN,
    change_timestamp TIMESTAMP
);

truncate table movie_booking_cdc_events;

CREATE OR REPLACE TASK ingest_cdc_events_task
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = '1 MINUTE'
AS
INSERT INTO movie_booking_cdc_events
SELECT 
    booking_id,
    customer_id,
    movie_id,
    booking_date,
    status,
    ticket_count,
    ticket_price,
    METADATA$ACTION AS change_type,
    METADATA$ISUPDATE AS is_update,
    CURRENT_TIMESTAMP() AS change_timestamp
FROM movie_bookings_stream;

ALTER TASK ingest_cdc_events_task SUSPEND;

select * from movie_bookings_stream;
select * from movie_booking_cdc_events;


CREATE OR REPLACE DYNAMIC TABLE movie_bookings_filtered
WAREHOUSE = 'COMPUTE_WH'
TARGET_LAG = DOWNSTREAM
AS
SELECT
    booking_id,
    customer_id,
    movie_id,
    booking_date,
    status,
    ticket_count,
    ticket_price,
    MAX(change_timestamp) AS latest_change_timestamp
FROM movie_booking_cdc_events
WHERE change_type IN ('INSERT', 'DELETE')
GROUP BY booking_id, customer_id, movie_id, booking_date, status, ticket_count, ticket_price;

CREATE OR REPLACE DYNAMIC TABLE movie_booking_insights
WAREHOUSE = 'COMPUTE_WH'
TARGET_LAG = DOWNSTREAM
AS
SELECT
    movie_id,
    COUNT(booking_id) AS total_bookings,
    SUM(CASE WHEN status = 'COMPLETED' THEN ticket_count ELSE 0 END) AS total_tickets_sold,
    SUM(CASE WHEN status = 'COMPLETED' THEN ticket_price ELSE 0 END) AS total_revenue,
    COUNT(CASE WHEN status = 'CANCELLED' THEN 1 ELSE NULL END) AS total_cancellations,
    CURRENT_TIMESTAMP() AS refresh_timestamp
FROM movie_bookings_filtered
GROUP BY movie_id;

Select * from movie_booking_insights;

CREATE OR REPLACE TASK refresh_movie_booking_insights
WAREHOUSE = 'COMPUTE_WH'
SCHEDULE = '2 MINUTE'
AS
ALTER DYNAMIC TABLE movie_booking_insights REFRESH;

ALTER TASK refresh_movie_booking_insights SUSPEND;

-- Insert New Movie Bookings
INSERT INTO raw_movie_bookings (booking_id, customer_id, movie_id, booking_date, status, ticket_count, ticket_price)
VALUES
    ('B001', 'C001', 'M001', '2024-12-29 10:00:00', 'BOOKED', 2, 15.00),
    ('B002', 'C002', 'M002', '2024-12-29 10:10:00', 'BOOKED', 1, 12.00),
    ('B003', 'C003', 'M003', '2024-12-29 10:15:00', 'BOOKED', 3, 20.00),
    ('B004', 'C004', 'M004', '2024-12-29 10:20:00', 'BOOKED', 4, 25.00),
    ('B005', 'C005', 'M005', '2024-12-29 10:25:00', 'BOOKED', 1, 10.00);

-- Update Booking Status to COMPLETED
UPDATE raw_movie_bookings
SET status = 'COMPLETED'
WHERE booking_id IN ('B001', 'B003');

-- Update Ticket Count for Specific Bookings
UPDATE raw_movie_bookings
SET ticket_count = 3
WHERE booking_id = 'B002';


SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME=>'ingest_cdc_events_task')) ORDER BY SCHEDULED_TIME;

