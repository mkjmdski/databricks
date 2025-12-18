-- =============================================================================
-- INCREMENTAL LOADING QUERIES FOR WHEELIE DATA WAREHOUSE
-- =============================================================================
-- This file contains SQL queries for incremental loading from source database
-- (wheelie) to data warehouse.
--
-- IMPORTANT NOTES:
-- - Source database cannot be modified (no ALTER TABLE allowed)
-- - Use available timestamp columns (create_date, last_update, technical_timestamp)
-- - For tables without create_date, use last_update to detect new records
-- - All PKs in warehouse are named "ID"
-- - All FKs in warehouse are named "<dim>_key" (e.g., customer_key, car_key)
-- =============================================================================

-- =============================================================================
-- 1. DIMENSIONS - STATIC (dim_date, dim_eq)
-- =============================================================================

-- dim_date: Static dimension, load once (2018-2030), then incremental for new dates
-- Note: Usually loaded once, but included for completeness
INSERT INTO dim_date (ID, date, day_of_week, day_of_week_name, day_of_month, week_of_year,
                      month, month_name, quarter, year, is_weekend, is_pre_covid, is_covid, is_post_covid)
SELECT
  NULL as ID,  -- Auto-increment
  date_value as date,
  DAYOFWEEK(date_value) as day_of_week,
  DAYNAME(date_value) as day_of_week_name,
  DAY(date_value) as day_of_month,
  WEEK(date_value) as week_of_year,
  MONTH(date_value) as month,
  MONTHNAME(date_value) as month_name,
  QUARTER(date_value) as quarter,
  YEAR(date_value) as year,
  CASE WHEN DAYOFWEEK(date_value) IN (1, 7) THEN TRUE ELSE FALSE END as is_weekend,
  CASE WHEN date_value < '2020-03-01' THEN TRUE ELSE FALSE END as is_pre_covid,
  CASE WHEN date_value >= '2020-03-01' AND date_value <= '2022-06-30' THEN TRUE ELSE FALSE END as is_covid,
  CASE WHEN date_value > '2022-06-30' THEN TRUE ELSE FALSE END as is_post_covid
FROM (
  -- Generate dates from 2018-01-01 to 2030-12-31
  SELECT DATE_ADD('2018-01-01', INTERVAL seq DAY) as date_value
  FROM (SELECT @row := @row + 1 as seq FROM (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t1,
        (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t2,
        (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t3,
        (SELECT 0 UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) t4,
        (SELECT @row := -1) r
        WHERE @row < 4748) seq_table
) date_series
WHERE date_value <= '2030-12-31'
  AND date_value NOT IN (SELECT date FROM dim_date);  -- Skip existing dates

-- dim_eq: Incremental load based on create_date or last_update
INSERT INTO dim_eq (ID, equipment_id, name, type, version, create_date, last_update)
SELECT
  NULL as ID,  -- Auto-increment
  e.equipment_id,
  e.name,
  e.type,
  e.version,
  e.create_date,
  e.last_update
FROM wheelie.equipment e
WHERE e.create_date > COALESCE((SELECT last_processed_timestamp FROM etl_watermarks WHERE table_name = 'dim_eq'), '1900-01-01')
   OR e.last_update > COALESCE((SELECT last_processed_timestamp FROM etl_watermarks WHERE table_name = 'dim_eq'), '1900-01-01')
  AND e.equipment_id NOT IN (SELECT equipment_id FROM dim_eq);

-- Update existing dim_eq records
UPDATE dim_eq d
JOIN wheelie.equipment e ON d.equipment_id = e.equipment_id
SET
  d.name = e.name,
  d.type = e.type,
  d.version = e.version,
  d.last_update = e.last_update
WHERE e.last_update > d.last_update;

-- =============================================================================
-- 2. DIMENSIONS - TYPE 1 (dim_store, dim_staff, dim_car)
-- =============================================================================

-- dim_store: MERGE strategy (UPDATE if exists, INSERT if new)
-- Note: No create_date in source, use last_update
INSERT INTO dim_store (ID, store_id, address_id, address, address2, city, country, postal_code, store_manager_id, last_update)
SELECT
  NULL as ID,  -- Auto-increment
  s.store_id,
  s.address_id,
  a.address,
  a.address2,
  ci.city,
  co.country,
  a.postal_code,
  s.store_manager_id,
  s.last_update
FROM wheelie.store s
LEFT JOIN wheelie.address a ON s.address_id = a.address_id
LEFT JOIN wheelie.city ci ON a.city_id = ci.city_id
LEFT JOIN wheelie.country co ON ci.country_id = co.country_id
WHERE s.last_update > COALESCE((SELECT last_processed_timestamp FROM etl_watermarks WHERE table_name = 'dim_store'), '1900-01-01')
  AND s.store_id NOT IN (SELECT store_id FROM dim_store);

-- Update existing dim_store records
UPDATE dim_store d
JOIN wheelie.store s ON d.store_id = s.store_id
LEFT JOIN wheelie.address a ON s.address_id = a.address_id
LEFT JOIN wheelie.city ci ON a.city_id = ci.city_id
LEFT JOIN wheelie.country co ON ci.country_id = co.country_id
SET
  d.address_id = s.address_id,
  d.address = a.address,
  d.address2 = a.address2,
  d.city = ci.city,
  d.country = co.country,
  d.postal_code = a.postal_code,
  d.store_manager_id = s.store_manager_id,
  d.last_update = s.last_update
WHERE s.last_update > d.last_update;

-- dim_staff: MERGE strategy (UPDATE if exists, INSERT if new)
-- Note: No create_date in source, use last_update
INSERT INTO dim_staff (ID, staff_id, first_name, last_name, email, hired_date, store_id, address_id, city, country, manager_id, manager_staff_key, manager_name, last_update)
SELECT
  NULL as ID,  -- Auto-increment
  s.staff_id,
  s.first_name,
  s.last_name,
  s.email,
  s.hired_date,
  s.store_id,
  s.address_id,
  ci.city,
  co.country,
  s.manager_id,
  m.ID as manager_staff_key,  -- FK to dim_staff.ID
  CONCAT(m.first_name, ' ', m.last_name) as manager_name,
  s.last_update
FROM wheelie.staff s
LEFT JOIN wheelie.address a ON s.address_id = a.address_id
LEFT JOIN wheelie.city ci ON a.city_id = ci.city_id
LEFT JOIN wheelie.country co ON ci.country_id = co.country_id
LEFT JOIN dim_staff m ON s.manager_id = m.staff_id
WHERE s.last_update > COALESCE((SELECT last_processed_timestamp FROM etl_watermarks WHERE table_name = 'dim_staff'), '1900-01-01')
  AND s.staff_id NOT IN (SELECT staff_id FROM dim_staff);

-- Update existing dim_staff records
UPDATE dim_staff d
JOIN wheelie.staff s ON d.staff_id = s.staff_id
LEFT JOIN wheelie.address a ON s.address_id = a.address_id
LEFT JOIN wheelie.city ci ON a.city_id = ci.city_id
LEFT JOIN wheelie.country co ON ci.country_id = co.country_id
LEFT JOIN dim_staff m ON s.manager_id = m.staff_id
SET
  d.first_name = s.first_name,
  d.last_name = s.last_name,
  d.email = s.email,
  d.hired_date = s.hired_date,
  d.store_id = s.store_id,
  d.address_id = s.address_id,
  d.city = ci.city,
  d.country = co.country,
  d.manager_id = s.manager_id,
  d.manager_staff_key = m.ID,
  d.manager_name = CONCAT(m.first_name, ' ', m.last_name),
  d.last_update = s.last_update
WHERE s.last_update > d.last_update;

-- dim_car: MERGE strategy (combines car + inventory tables)
INSERT INTO dim_car (ID, inventory_id, car_id, producer, model, rental_rate, production_year, fuel_type, license_plates, purchase_price, sell_price, store_id, create_date, last_update)
SELECT
  NULL as ID,  -- Auto-increment
  i.inventory_id,
  i.car_id,
  c.producer,
  c.model,
  c.rental_rate,
  i.production_year,
  i.fuel_type,
  i.license_plates,
  i.purchase_price,
  i.sell_price,
  i.store_id,
  i.create_date,
  i.last_update
FROM wheelie.inventory i
JOIN wheelie.car c ON i.car_id = c.car_id
WHERE i.create_date > COALESCE((SELECT last_processed_timestamp FROM etl_watermarks WHERE table_name = 'dim_car'), '1900-01-01')
   OR i.last_update > COALESCE((SELECT last_processed_timestamp FROM etl_watermarks WHERE table_name = 'dim_car'), '1900-01-01')
  AND i.inventory_id NOT IN (SELECT inventory_id FROM dim_car);

-- Update existing dim_car records
UPDATE dim_car d
JOIN wheelie.inventory i ON d.inventory_id = i.inventory_id
JOIN wheelie.car c ON i.car_id = c.car_id
SET
  d.car_id = i.car_id,
  d.producer = c.producer,
  d.model = c.model,
  d.rental_rate = c.rental_rate,
  d.production_year = i.production_year,
  d.fuel_type = i.fuel_type,
  d.license_plates = i.license_plates,
  d.purchase_price = i.purchase_price,
  d.sell_price = i.sell_price,
  d.store_id = i.store_id,
  d.last_update = i.last_update
WHERE i.last_update > d.last_update;

-- =============================================================================
-- 3. DIMENSIONS - SCD TYPE 2 (dim_customer)
-- =============================================================================

-- dim_customer: SCD Type 2 - handle address changes
-- Step 1: Close old versions where address changed
UPDATE dim_customer d
JOIN wheelie.customer c ON d.customer_id = c.customer_id
JOIN wheelie.address a ON c.address_id = a.address_id
JOIN wheelie.city ci ON a.city_id = ci.city_id
JOIN wheelie.country co ON ci.country_id = co.country_id
SET
  d.is_current = FALSE,
  d.end_date = CURRENT_TIMESTAMP
WHERE d.is_current = TRUE
  AND (d.city != ci.city OR d.country != co.country)
  AND c.last_update > COALESCE((SELECT last_processed_timestamp FROM etl_watermarks WHERE table_name = 'dim_customer'), '1900-01-01');

-- Step 2: Insert new versions for changed customers
INSERT INTO dim_customer (ID, customer_id, first_name, last_name, email, birth_date, city, country, effective_date, end_date, is_current, create_date, last_update)
SELECT
  NULL as ID,  -- Auto-increment
  c.customer_id,
  c.first_name,
  c.last_name,
  c.email,
  c.birth_date,
  ci.city,
  co.country,
  CURRENT_TIMESTAMP as effective_date,
  NULL as end_date,
  TRUE as is_current,
  c.create_date,
  c.last_update
FROM wheelie.customer c
JOIN wheelie.address a ON c.address_id = a.address_id
JOIN wheelie.city ci ON a.city_id = ci.city_id
JOIN wheelie.country co ON ci.country_id = co.country_id
WHERE c.last_update > COALESCE((SELECT last_processed_timestamp FROM etl_watermarks WHERE table_name = 'dim_customer'), '1900-01-01')
  AND EXISTS (
    SELECT 1 FROM dim_customer d
    WHERE d.customer_id = c.customer_id
      AND d.is_current = TRUE
      AND (d.city != ci.city OR d.country != co.country)
  );

-- Step 3: Insert new customers (first time)
INSERT INTO dim_customer (ID, customer_id, first_name, last_name, email, birth_date, city, country, effective_date, end_date, is_current, create_date, last_update)
SELECT
  NULL as ID,  -- Auto-increment
  c.customer_id,
  c.first_name,
  c.last_name,
  c.email,
  c.birth_date,
  ci.city,
  co.country,
  c.create_date as effective_date,
  NULL as end_date,
  TRUE as is_current,
  c.create_date,
  c.last_update
FROM wheelie.customer c
JOIN wheelie.address a ON c.address_id = a.address_id
JOIN wheelie.city ci ON a.city_id = ci.city_id
JOIN wheelie.country co ON ci.country_id = co.country_id
WHERE c.create_date > COALESCE((SELECT last_processed_timestamp FROM etl_watermarks WHERE table_name = 'dim_customer'), '1900-01-01')
  AND c.customer_id NOT IN (SELECT customer_id FROM dim_customer);

-- =============================================================================
-- 4. BRIDGE TABLES (rebuild when dependencies change)
-- =============================================================================

-- bridge_staff_hierarchy: Rebuild closure table
-- Note: Rebuild when dim_staff.manager_id changes
TRUNCATE TABLE bridge_staff_hierarchy;

INSERT INTO bridge_staff_hierarchy (staff_key, ancestor_staff_key, depth)
WITH RECURSIVE staff_hierarchy AS (
  -- Base case: self-references (depth = 0)
  SELECT
    ID as staff_key,
    ID as ancestor_staff_key,
    0 as depth
  FROM dim_staff

  UNION ALL

  -- Recursive case: traverse up the hierarchy
  SELECT
    sh.staff_key,
    s.manager_staff_key as ancestor_staff_key,
    sh.depth + 1 as depth
  FROM staff_hierarchy sh
  JOIN dim_staff s ON sh.ancestor_staff_key = s.ID
  WHERE s.manager_staff_key IS NOT NULL
    AND sh.depth < 10  -- Safety limit
)
SELECT DISTINCT staff_key, ancestor_staff_key, depth
FROM staff_hierarchy;

-- dim_eq_group and bridge_eq_group: Rebuild when inventory_equipment changes
-- Step 1: Create equipment groups for unique combinations
TRUNCATE TABLE dim_eq_group;
TRUNCATE TABLE bridge_eq_group;

INSERT INTO dim_eq_group (ID, eq_key)
SELECT DISTINCT
  NULL as ID,  -- Auto-increment
  e.ID as eq_key
FROM wheelie.inventory_equipment ie
JOIN dim_car c ON ie.inventory_id = c.inventory_id
JOIN wheelie.equipment eq ON ie.equipment_id = eq.equipment_id
JOIN dim_eq e ON eq.equipment_id = e.equipment_id
GROUP BY ie.inventory_id, e.ID
ORDER BY ie.inventory_id, e.ID;

-- Step 2: Create bridge between cars and equipment groups
-- Note: This is simplified - in practice, you'd need to group equipment per car
-- and create unique eq_group_key for each combination
INSERT INTO bridge_eq_group (car_key, eq_group_key)
SELECT DISTINCT
  c.ID as car_key,
  eg.ID as eq_group_key
FROM dim_car c
JOIN wheelie.inventory_equipment ie ON c.inventory_id = ie.inventory_id
JOIN wheelie.equipment eq ON ie.equipment_id = eq.equipment_id
JOIN dim_eq e ON eq.equipment_id = e.equipment_id
JOIN dim_eq_group eg ON e.ID = eg.eq_key;

-- =============================================================================
-- 5. FACT TABLE (fact_rental)
-- =============================================================================

-- fact_rental: Incremental load with late-arriving data handling
INSERT INTO fact_rental (
  rental_id, customer_key, car_key, staff_key, store_key,
  rental_date_key, return_date_key, payment_date_key, payment_deadline_date_key,
  rental_date, return_date, payment_date, payment_deadline_date,
  rental_amount, payment_amount, rental_rate, rental_duration, rental_delay_days
)
SELECT
  r.rental_id,
  c.ID as customer_key,  -- SCD Type 2 temporal join
  car.ID as car_key,
  s.ID as staff_key,
  st.ID as store_key,
  d_rental.ID as rental_date_key,
  d_return.ID as return_date_key,
  d_payment.ID as payment_date_key,
  d_deadline.ID as payment_deadline_date_key,
  r.rental_date,
  r.return_date,
  p.payment_date,
  r.payment_deadline as payment_deadline_date,
  r.rental_rate as rental_amount,
  p.amount as payment_amount,
  r.rental_rate,
  CASE WHEN r.return_date IS NOT NULL THEN DATEDIFF(day, r.rental_date, r.return_date) ELSE NULL END as rental_duration,
  NULL as rental_delay_days  -- TODO: Calculate based on expected return date
FROM wheelie.rental r
LEFT JOIN wheelie.payment p ON r.rental_id = p.rental_id
JOIN dim_customer c ON r.customer_id = c.customer_id
  AND r.rental_date BETWEEN c.effective_date AND COALESCE(c.end_date, '9999-12-31')
JOIN dim_car car ON r.inventory_id = car.inventory_id
JOIN dim_staff s ON r.staff_id = s.staff_id
JOIN dim_store st ON r.store_id = st.store_id
JOIN dim_date d_rental ON r.rental_date = d_rental.date
LEFT JOIN dim_date d_return ON r.return_date = d_return.date
LEFT JOIN dim_date d_payment ON p.payment_date = d_payment.date
JOIN dim_date d_deadline ON r.payment_deadline = d_deadline.date
WHERE (r.create_date > COALESCE((SELECT last_processed_timestamp FROM etl_watermarks WHERE table_name = 'fact_rental' AND source_table = 'rental'), '1900-01-01')
   OR r.technical_timestamp > COALESCE((SELECT last_processed_timestamp FROM etl_watermarks WHERE table_name = 'fact_rental' AND source_table = 'rental'), '1900-01-01'))
  AND r.rental_id NOT IN (SELECT rental_id FROM fact_rental);

-- Handle late-arriving data: Update existing rentals with return_date
UPDATE fact_rental f
JOIN wheelie.rental r ON f.rental_id = r.rental_id
LEFT JOIN dim_date d_return ON r.return_date = d_return.date
SET
  f.return_date = r.return_date,
  f.return_date_key = d_return.ID,
  f.rental_duration = DATEDIFF(day, f.rental_date, r.return_date)
WHERE f.return_date IS NULL
  AND r.return_date IS NOT NULL
  AND r.last_update > COALESCE((SELECT last_processed_timestamp FROM etl_watermarks WHERE table_name = 'fact_rental' AND source_table = 'rental'), '1900-01-01');

-- Handle late-arriving data: Update existing rentals with payment
UPDATE fact_rental f
JOIN wheelie.payment p ON f.rental_id = p.rental_id
LEFT JOIN dim_date d_payment ON p.payment_date = d_payment.date
SET
  f.payment_date = p.payment_date,
  f.payment_date_key = d_payment.ID,
  f.payment_amount = p.amount
WHERE f.payment_date IS NULL
  AND p.payment_date IS NOT NULL
  AND p.last_update > COALESCE((SELECT last_processed_timestamp FROM etl_watermarks WHERE table_name = 'fact_rental' AND source_table = 'payment'), '1900-01-01');

-- =============================================================================
-- 6. UPDATE WATERMARKS
-- =============================================================================

-- Update watermarks after successful load
-- Note: Execute these after each table load in transaction

-- Example for dim_customer:
-- UPDATE etl_watermarks
-- SET last_processed_timestamp = CURRENT_TIMESTAMP,
--     last_processed_id = (SELECT MAX(customer_id) FROM wheelie.customer WHERE last_update <= CURRENT_TIMESTAMP),
--     last_run_timestamp = CURRENT_TIMESTAMP,
--     records_processed = (SELECT COUNT(*) FROM wheelie.customer WHERE last_update > COALESCE(last_processed_timestamp, '1900-01-01')),
--     status = 'SUCCESS',
--     updated_at = CURRENT_TIMESTAMP
-- WHERE table_name = 'dim_customer';

-- Example for fact_rental:
-- UPDATE etl_watermarks
-- SET last_processed_timestamp = CURRENT_TIMESTAMP,
--     last_processed_id = (SELECT MAX(rental_id) FROM wheelie.rental WHERE create_date <= CURRENT_TIMESTAMP),
--     last_run_timestamp = CURRENT_TIMESTAMP,
--     records_processed = (SELECT COUNT(*) FROM wheelie.rental WHERE create_date > COALESCE(last_processed_timestamp, '1900-01-01')),
--     status = 'SUCCESS',
--     updated_at = CURRENT_TIMESTAMP
-- WHERE table_name = 'fact_rental' AND source_table = 'rental';

-- =============================================================================
-- END OF INCREMENTAL LOADING QUERIES
-- =============================================================================
