"""
Silver layer transformation functions.
Shared business logic for data cleaning and denormalization across full and incremental loads.
"""

import logging
from datetime import date
from typing import List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, lower, concat_ws, regexp_replace, xxhash64,
    collect_set, array_sort, lit, datediff, explode, posexplode,
    array, size, element_at, array_contains, concat, sequence,
    dayofweek, dayofmonth, weekofyear, month, quarter, year
)

logger = logging.getLogger("wheelie_etl")


def clean_address_silver(address_bronze: DataFrame) -> DataFrame:
    """
    Clean address data: concatenate address + address2 into full_address.

    Args:
        address_bronze: Raw address DataFrame

    Returns:
        Cleaned address DataFrame with full_address column
    """
    return address_bronze.withColumn(
        "full_address",
        when(col("address2").isNotNull(), concat_ws(", ", col("address"), col("address2")))
        .otherwise(col("address"))
    )


def clean_customer_silver(customer_bronze: DataFrame) -> DataFrame:
    """
    Clean customer data: fix email format (remove spaces, fix double dots).

    Args:
        customer_bronze: Raw customer DataFrame

    Returns:
        Cleaned customer DataFrame
    """
    return customer_bronze.withColumn(
        "email",
        regexp_replace(
            regexp_replace(col("email"), r"\s+", ""),  # Remove all spaces
            r"\.{2,}", "."  # Replace multiple dots with single dot
        )
    )


def clean_inventory_silver(inventory_bronze: DataFrame) -> DataFrame:
    """
    Clean inventory data: fix fuel_type typos.

    Args:
        inventory_bronze: Raw inventory DataFrame

    Returns:
        Cleaned inventory DataFrame
    """
    return inventory_bronze.withColumn(
        "fuel_type",
        when(lower(col("fuel_type")) == "diesle", "Diesel")
        .when(lower(col("fuel_type")) == "petol", "Petrol")
        .otherwise(col("fuel_type"))
    )


def build_address_with_location(address_silver: DataFrame, city_bronze: DataFrame,
                                  country_bronze: DataFrame) -> DataFrame:
    """
    Build denormalized address with location hierarchy (city + country).

    Args:
        address_silver: Cleaned address DataFrame
        city_bronze: City DataFrame
        country_bronze: Country DataFrame

    Returns:
        Address DataFrame with city and country denormalized
    """
    city_with_country = city_bronze.join(country_bronze, "country_id", "left")
    return address_silver.join(city_with_country, "city_id", "left")


def transform_customer_to_gold(customer_silver: DataFrame, address_with_location: DataFrame) -> DataFrame:
    """
    Transform customer to gold dimension.

    Args:
        customer_silver: Cleaned customer DataFrame
        address_with_location: Denormalized address DataFrame

    Returns:
        Gold dim_customer DataFrame with surrogate key
    """
    return customer_silver.alias("cust").join(
        address_with_location.alias("addr"), "address_id", "left"
    ).select(
        col("cust.customer_id").alias("customer_id"),
        col("cust.first_name").alias("customer_first_name"),
        col("cust.last_name").alias("customer_last_name"),
        col("cust.email").alias("customer_email"),
        col("cust.birth_date").alias("birth_date"),
        col("addr.full_address").alias("customer_address"),
        col("addr.city").alias("customer_city"),
        col("addr.country").alias("customer_country")
    ).withColumn("customer_key", xxhash64(col("customer_id")))


def transform_staff_to_gold(staff_bronze: DataFrame, address_with_location: DataFrame) -> DataFrame:
    """
    Transform staff to gold dimension.

    Args:
        staff_bronze: Raw staff DataFrame
        address_with_location: Denormalized address DataFrame

    Returns:
        Gold dim_staff DataFrame
    """
    # Self-join to get manager information
    staff_with_manager = staff_bronze.alias("staff").join(
        staff_bronze.select(
            col("staff_id").alias("mgr_id"),
            col("first_name").alias("staff_manager_first_name"),
            col("last_name").alias("staff_manager_last_name")
        ).alias("mgr"),
        col("staff.manager_id") == col("mgr.mgr_id"),
        "left"
    )
    
    return staff_with_manager.join(
        address_with_location.alias("addr"), "address_id", "left"
    ).select(
        col("staff.staff_id").alias("staff_id"),
        col("staff.store_id").alias("store_id"),
        col("staff.first_name").alias("staff_first_name"),
        col("staff.last_name").alias("staff_last_name"),
        col("staff.email").alias("staff_email"),
        col("staff.hired_date").alias("hired_date"),
        col("addr.full_address").alias("staff_address"),
        col("addr.city").alias("staff_city"),
        col("addr.country").alias("staff_country"),
        col("mgr.staff_manager_first_name").alias("staff_manager_first_name"),
        col("mgr.staff_manager_last_name").alias("staff_manager_last_name")
    ).withColumn("staff_key", xxhash64(col("staff_id")))


def transform_store_to_gold(store_bronze: DataFrame, address_with_location: DataFrame,
                             staff_bronze: DataFrame) -> DataFrame:
    """
    Transform store to gold dimension with manager denormalization.

    Args:
        store_bronze: Raw store DataFrame
        address_with_location: Denormalized address DataFrame
        staff_bronze: Staff DataFrame for manager lookup

    Returns:
        Gold dim_store DataFrame with surrogate key (includes SCD2 columns)
    """
    from pyspark.sql.functions import current_timestamp

    return (
        store_bronze.alias("store")
        .join(address_with_location.alias("addr"), "address_id", "left")
        .join(
            staff_bronze.select(
                col("staff_id").alias("store_manager_id"),
                col("first_name").alias("store_manager_first_name"),
                col("last_name").alias("store_manager_last_name")
            ).alias("mgr"),
            col("store.store_manager_id") == col("mgr.store_manager_id"),
            "left"
        )
        .select(
            col("store.store_id").alias("store_id"),
            col("mgr.store_manager_id").alias("store_manager_id"),
            col("mgr.store_manager_first_name").alias("store_manager_first_name"),
            col("mgr.store_manager_last_name").alias("store_manager_last_name"),
            col("addr.full_address").alias("store_address"),
            col("addr.city").alias("city"),
            col("addr.country").alias("country"),
            col("addr.postal_code").alias("postal_code"),
            col("store.last_update").alias("last_update")
        )
        .withColumn("store_key", xxhash64(col("store_id")))
        .withColumn("effective_date", current_timestamp())
        .withColumn("end_date", lit(None).cast("timestamp"))
        .withColumn("is_current", lit(True))
    )


def transform_car_to_gold(inventory_silver: DataFrame, car_bronze: DataFrame,
                          inventory_equipment_bronze: DataFrame, equipment_bronze: DataFrame) -> DataFrame:
    """
    Transform car/inventory to gold dimension with equipment bridge.

    Args:
        inventory_silver: Cleaned inventory DataFrame
        car_bronze: Car DataFrame
        inventory_equipment_bronze: Inventory-Equipment bridge
        equipment_bronze: Equipment DataFrame

    Returns:
        Gold dim_car DataFrame
    """
    # Aggregate equipment list per inventory
    equipment_agg = (
        inventory_equipment_bronze.alias("inv_eq")
        .join(equipment_bronze.alias("eq"), "equipment_id", "left")
        .groupBy("inv_eq.inventory_id")
        .agg(
            concat_ws(", ", array_sort(collect_set(col("eq.name")))).alias("equipment_list")
        )
    )

    # Join inventory with car and equipment
    return (
        inventory_silver.alias("inv")
        .join(car_bronze.alias("car"), "car_id", "left")
        .join(equipment_agg.alias("eq_agg"), "inventory_id", "left")
        .select(
            col("inv.inventory_id").alias("inventory_id"),
            col("inv.car_id").alias("car_id"),
            col("car.producer").alias("producer"),
            col("car.model").alias("model"),
            col("car.rental_rate").alias("rental_rate"),
            col("inv.production_year").alias("production_year"),
            col("inv.fuel_type").alias("fuel_type"),
            col("inv.license_plates").alias("license_plates"),
            col("inv.purchase_price").alias("purchase_price"),
            col("inv.sell_price").alias("sell_price"),
            col("inv.store_id").alias("store_id"),
            col("inv.last_update").alias("last_update")
        )
        .withColumn("car_key", xxhash64(col("inventory_id")))
    )


# High-level wrapper functions for easy notebook usage
def transform_customer_full_pipeline(customer_bronze: DataFrame, address_bronze: DataFrame,
                                      city_bronze: DataFrame, country_bronze: DataFrame) -> DataFrame:
    """
    Full pipeline: customer bronze → silver → gold.

    Args:
        customer_bronze: Raw customer DataFrame
        address_bronze: Raw address DataFrame
        city_bronze: City DataFrame
        country_bronze: Country DataFrame

    Returns:
        Gold dim_customer DataFrame
    """
    customer_silver = clean_customer_silver(customer_bronze)
    address_silver = clean_address_silver(address_bronze)
    address_with_location = build_address_with_location(address_silver, city_bronze, country_bronze)
    return transform_customer_to_gold(customer_silver, address_with_location)


def transform_staff_full_pipeline(staff_bronze: DataFrame, address_bronze: DataFrame,
                                   city_bronze: DataFrame, country_bronze: DataFrame) -> DataFrame:
    """
    Full pipeline: staff bronze → silver → gold.

    Args:
        staff_bronze: Raw staff DataFrame
        address_bronze: Raw address DataFrame
        city_bronze: City DataFrame
        country_bronze: Country DataFrame

    Returns:
        Gold dim_staff DataFrame
    """
    address_silver = clean_address_silver(address_bronze)
    address_with_location = build_address_with_location(address_silver, city_bronze, country_bronze)
    return transform_staff_to_gold(staff_bronze, address_with_location)


def transform_store_full_pipeline(store_bronze: DataFrame, staff_bronze: DataFrame,
                                   address_bronze: DataFrame, city_bronze: DataFrame,
                                   country_bronze: DataFrame) -> DataFrame:
    """
    Full pipeline: store bronze → silver → gold.

    Args:
        store_bronze: Raw store DataFrame
        staff_bronze: Staff DataFrame
        address_bronze: Raw address DataFrame
        city_bronze: City DataFrame
        country_bronze: Country DataFrame

    Returns:
        Gold dim_store DataFrame
    """
    address_silver = clean_address_silver(address_bronze)
    address_with_location = build_address_with_location(address_silver, city_bronze, country_bronze)
    return transform_store_to_gold(store_bronze, address_with_location, staff_bronze)


def transform_car_full_pipeline(inventory_bronze: DataFrame, car_bronze: DataFrame,
                                 inventory_equipment_bronze: DataFrame,
                                 equipment_bronze: DataFrame) -> DataFrame:
    """
    Full pipeline: car/inventory bronze → silver → gold.

    Args:
        inventory_bronze: Raw inventory DataFrame
        car_bronze: Car DataFrame
        inventory_equipment_bronze: Inventory-Equipment bridge
        equipment_bronze: Equipment DataFrame

    Returns:
        Gold dim_car DataFrame
    """
    inventory_silver = clean_inventory_silver(inventory_bronze)
    return transform_car_to_gold(inventory_silver, car_bronze,
                                  inventory_equipment_bronze, equipment_bronze)


# ==============================================================================
# DATE DIMENSION GENERATION
# ==============================================================================

def generate_date_dimension(
    spark: SparkSession,
    start_date: date,
    end_date: date,
    covid_start: date = date(2020, 3, 1),
    covid_end: date = date(2022, 6, 30)
) -> DataFrame:
    """
    Generate date dimension with all attributes.

    Args:
        spark: SparkSession instance
        start_date: Start date of dimension range
        end_date: End date of dimension range
        covid_start: COVID period start date (default: March 1, 2020)
        covid_end: COVID period end date (default: June 30, 2022)

    Returns:
        DataFrame with date dimension including:
        - Date hierarchy (day, week, month, quarter, year)
        - Business flags (is_weekend, COVID periods)
        - date_key (surrogate key)
    """
    logger.info(f"Generating date dimension from {start_date} to {end_date}")

    # Generate date sequence
    dates_df = spark.range(1).select(
        explode(
            sequence(
                lit(start_date),
                lit(end_date)
            )
        ).alias('date')
    )

    # Build date dimension with all attributes
    dim_date = dates_df.select(
        col('date'),

        # Date hierarchy
        dayofweek('date').alias('day_of_week'),
        when(dayofweek('date') == 1, 'Monday')
         .when(dayofweek('date') == 2, 'Tuesday')
         .when(dayofweek('date') == 3, 'Wednesday')
         .when(dayofweek('date') == 4, 'Thursday')
         .when(dayofweek('date') == 5, 'Friday')
         .when(dayofweek('date') == 6, 'Saturday')
         .otherwise('Sunday').alias('day_of_week_name'),

        dayofmonth('date').alias('day_of_month'),
        weekofyear('date').alias('week_of_year'),
        month('date').alias('month'),

        when(month('date') ==  1, 'January')
         .when(month('date') ==  2, 'February')
         .when(month('date') ==  3, 'March')
         .when(month('date') ==  4, 'April')
         .when(month('date') ==  5, 'May')
         .when(month('date') ==  6, 'June')
         .when(month('date') ==  7, 'July')
         .when(month('date') ==  8, 'August')
         .when(month('date') ==  9, 'September')
         .when(month('date') == 10, 'October')
         .when(month('date') == 11, 'November')
         .otherwise('December').alias('month_name'),

        quarter('date').alias('quarter'),
        year('date').alias('year'),

        # Business flags
        (dayofweek('date').isin(6, 7)).alias('is_weekend'),

        # COVID period flags
        (col('date') < lit(covid_start)).alias('is_pre_covid'),
        (col('date').between(lit(covid_start), lit(covid_end))).alias('is_covid'),
        (col('date') > lit(covid_end)).alias('is_post_covid')
    ).withColumn('date_key', xxhash64(col('date')))

    logger.info(f"Date dimension generated: {dim_date.count():,} rows")
    return dim_date


def generate_role_playing_date_dimensions(
    base_dim_date: DataFrame,
    role_names: List[Tuple[str, str]]
) -> List[Tuple[str, DataFrame]]:
    """
    Generate role-playing date dimensions from base date dimension.

    Args:
        base_dim_date: Base date dimension DataFrame
        role_names: List of tuples (table_name, key_column_name)
                    Example: [("dim_rental_date", "rental_date_key"),
                              ("dim_return_date", "return_date_key")]

    Returns:
        List of tuples (table_name, DataFrame) for each role-playing dimension
    """
    logger.info(f"Generating {len(role_names)} role-playing date dimensions")

    result = []
    for table_name, key_column_name in role_names:
        # Rename date_key to role-specific key
        role_dim = base_dim_date.withColumnRenamed("date_key", key_column_name)
        result.append((table_name, role_dim))
        logger.info(f"Created role-playing dimension: {table_name} ({key_column_name})")

    return result


# ==============================================================================
# STAFF HIERARCHY TRANSFORMATION
# ==============================================================================

def build_staff_hierarchy_bridge(staff_bronze: DataFrame, max_depth: int = 10) -> DataFrame:
    """
    Build staff hierarchy bridge table with manager levels.

    Args:
        staff_bronze: Raw staff DataFrame
        max_depth: Maximum hierarchy depth to traverse (default: 10)

    Returns:
        Bridge table with staff_key, staff_manager_key, level, manager names
    """
    logger.info("Building staff hierarchy bridge")

    # Build staff hierarchy with surrogate keys
    staff_hierarchy = staff_bronze.select(
        col("staff_id"),
        col("manager_id"),
        xxhash64(col("staff_id")).alias("staff_key"),
        xxhash64(col("manager_id")).alias("staff_manager_key"),
        col("first_name").alias("staff_first_name"),
        col("last_name").alias("staff_last_name")
    )

    # Iteratively build hierarchy paths
    hierarchy = staff_hierarchy.withColumn(
        "path",
        when(col("staff_manager_key").isNotNull(), array(col("staff_manager_key")))
        .otherwise(array())
    ).withColumn(
        "manager_keys",
        when(col("staff_manager_key").isNotNull(), array(col("staff_manager_key")))
        .otherwise(array())
    )

    for i in range(1, max_depth + 1):
        hierarchy = hierarchy.withColumn(
            "last_manager_key",
            when(size(col("manager_keys")) >= i, element_at(col("manager_keys"), i))
        )

        hierarchy = hierarchy.join(
            staff_hierarchy.select(
                col("staff_key").alias(f"mgr_key_{i}"),
                col("staff_manager_key").alias(f"next_mgr_key_{i}")
            ),
            col("last_manager_key") == col(f"mgr_key_{i}"),
            "left"
        ).withColumn(
            "new_manager_key",
            when(
                (col(f"next_mgr_key_{i}").isNotNull()) &
                (~array_contains(col("manager_keys"), col(f"next_mgr_key_{i}"))),
                col(f"next_mgr_key_{i}")
            )
        ).withColumn(
            "manager_keys",
            when(
                col("new_manager_key").isNotNull(),
                concat(col("manager_keys"), array(col("new_manager_key")))
            ).otherwise(col("manager_keys"))
        ).withColumn(
            "path",
            col("manager_keys")
        ).drop(f"mgr_key_{i}", f"next_mgr_key_{i}", "new_manager_key", "last_manager_key")

        new_additions = hierarchy.filter(size(col("manager_keys")) > i).count()
        if new_additions == 0:
            break

    # Flatten hierarchy into bridge table
    bridge_staff_hierarchy = hierarchy.select(
        col("staff_key"),
        col("staff_first_name"),
        col("staff_last_name"),
        posexplode(col("path")).alias("level_idx", "staff_manager_key")
    ).withColumn("level", col("level_idx") + 1).drop("level_idx").filter(
        col("staff_manager_key").isNotNull()
    )

    # Add manager names to bridge
    bridge_staff_hierarchy = bridge_staff_hierarchy.join(
        staff_bronze.select(
            xxhash64(col("staff_id")).alias("staff_manager_key"),
            col("first_name").alias("staff_manager_first_name"),
            col("last_name").alias("staff_manager_last_name")
        ),
        "staff_manager_key",
        "left"
    )

    logger.info(f"Staff hierarchy bridge built: {bridge_staff_hierarchy.count():,} rows")
    return bridge_staff_hierarchy


# ==============================================================================
# EQUIPMENT BRIDGE TRANSFORMATIONS
# ==============================================================================

def transform_equipment_dimension(equipment_bronze: DataFrame) -> DataFrame:
    """
    Transform equipment to gold dimension.

    Args:
        equipment_bronze: Raw equipment DataFrame

    Returns:
        Gold dim_equipment DataFrame
    """
    return equipment_bronze.withColumn(
        "equipment_key", xxhash64(col("equipment_id"))
    ).select(
        "equipment_key",
        "equipment_id",
        "name",
        "type",
        "version"
    )


def build_equipment_bridges(
    inventory_equipment_bronze: DataFrame
) -> Tuple[DataFrame, DataFrame]:
    """
    Build equipment group bridge tables.

    Args:
        inventory_equipment_bronze: Inventory-Equipment bridge from bronze

    Returns:
        Tuple of (bridge_equipment_group_equipment, bridge_car_equipment)
    """
    logger.info("Building equipment bridge tables")

    # Build equipment groups: unique combinations of equipment assigned to cars
    equipment_groups = inventory_equipment_bronze.groupBy("inventory_id").agg(
        array_sort(collect_set("equipment_id")).alias("equipments_array")
    ).withColumn(
        "equipments", concat_ws(",", col("equipments_array"))
    ).withColumn(
        "equipment_group_key", xxhash64(col("equipments"))
    ).select("equipment_group_key", "equipments", "equipments_array").distinct()

    # Normalize equipment groups: explode to individual equipment
    bridge_equipment_group_equipment = equipment_groups.withColumn(
        "equipment_id", explode(col("equipments_array"))
    ).withColumn(
        "equipment_key", xxhash64(col("equipment_id"))
    ).select("equipment_group_key", "equipment_key").distinct()

    # Bridge from car to equipment group
    bridge_car_equipment = inventory_equipment_bronze.groupBy("inventory_id").agg(
        array_sort(collect_set("equipment_id")).alias("equipments_array")
    ).withColumn(
        "car_key", xxhash64(col("inventory_id"))
    ).withColumn(
        "equipments", concat_ws(",", col("equipments_array"))
    ).withColumn(
        "equipment_group_key", xxhash64(col("equipments"))
    ).select("car_key", "equipment_group_key").distinct()

    logger.info(f"Equipment bridges built: {bridge_equipment_group_equipment.count():,} group-equipment, {bridge_car_equipment.count():,} car-equipment")
    return (bridge_equipment_group_equipment, bridge_car_equipment)


# ==============================================================================
# FACT TABLE TRANSFORMATIONS
# ==============================================================================

def transform_service_fact(service_bronze: DataFrame) -> DataFrame:
    """
    Transform service to fact table.

    Args:
        service_bronze: Raw service DataFrame

    Returns:
        Gold fact_service DataFrame
    """
    logger.info("Transforming service to fact table")

    return service_bronze.select(
        "service_id",
        "service_date",
        "service_type",
        "service_cost",
        "inventory_id"
    ).withColumn(
        "service_key", xxhash64(col("service_id"))
    ).withColumn(
        "car_key", xxhash64(col("inventory_id"))
    ).drop("inventory_id")


def transform_rental_fact(
    rental_bronze: DataFrame,
    staff_bronze: DataFrame,
    inventory_bronze: DataFrame,
    payment_bronze: DataFrame
) -> DataFrame:
    """
    Transform rental to fact table with all joins and calculations.

    Args:
        rental_bronze: Raw rental DataFrame
        staff_bronze: Staff DataFrame (for store_id)
        inventory_bronze: Inventory DataFrame (for car_id)
        payment_bronze: Payment DataFrame

    Returns:
        Gold fact_rental DataFrame with surrogate keys and calculated metrics
    """
    logger.info("Transforming rental to fact table")

    # Join rental with staff to get store_id
    rental_with_staff = rental_bronze.alias("rental").join(
        staff_bronze.select(
            col("staff_id"),
            col("store_id")
        ).alias("staff"),
        col("rental.staff_id") == col("staff.staff_id"),
        "left"
    ).select(
        col("rental.*"),
        col("staff.store_id")
    )

    # Join with inventory to get car_id
    rental_with_inventory = rental_with_staff.alias("rental_staff").join(
        inventory_bronze.select("inventory_id", "car_id").alias("inv"),
        col("rental_staff.inventory_id") == col("inv.inventory_id"),
        "left"
    ).select(
        col("rental_staff.*"),
        col("inv.car_id")
    )

    # Join with payment
    rental_silver = rental_with_inventory.alias("rental_inv").join(
        payment_bronze.select(
            col("rental_id"),
            col("payment_date"),
            col("amount").alias("payment_amount")
        ).alias("payment"),
        col("rental_inv.rental_id") == col("payment.rental_id"),
        "left"
    ).select(
        col("rental_inv.*"),
        col("payment.payment_date"),
        col("payment.payment_amount")
    )

    # Build fact table with surrogate keys and calculated columns
    fact_rental = (
        rental_silver.select(
            "rental_id",
            "rental_rate",
            "payment_amount",
            "customer_id",
            "car_id",
            "staff_id",
            "store_id",
            "rental_date",
            "return_date",
            "payment_date",
            "payment_deadline"
        )
        # Add surrogate keys
        .withColumn("rental_key", xxhash64(col("rental_id")))
        .withColumn("customer_key", xxhash64(col("customer_id")))
        .withColumn("car_key", xxhash64(col("car_id")))
        .withColumn("staff_key", xxhash64(col("staff_id")))
        .withColumn("store_key", xxhash64(col("store_id")))

        # Add date surrogate keys (handle nulls)
        .withColumn("rental_date_key", xxhash64(col("rental_date")))
        .withColumn("return_date_key",
            when(col("return_date").isNotNull(), xxhash64(col("return_date"))).otherwise(None))
        .withColumn("payment_date_key",
            when(col("payment_date").isNotNull(), xxhash64(col("payment_date"))).otherwise(None))
        .withColumn("payment_deadline_date_key", xxhash64(col("payment_deadline")))

        # Add calculated business metrics
        .withColumn(
            "rental_amount",
            col("rental_rate") * datediff(col("return_date"), col("rental_date"))
        )
        .withColumn("rental_duration", datediff(col("return_date"), col("rental_date")))
        .withColumn(
            "payment_delay_duration",
            datediff(col("payment_date"), col("payment_deadline"))
        )

        # Drop business keys (keep only surrogate keys)
        .drop(
            "customer_id",
            "car_id",
            "staff_id",
            "store_id",
            "return_date",
            "payment_date",
            "payment_deadline"
        )
    )

    logger.info(f"Rental fact transformed: {fact_rental.count():,} rows")
    return fact_rental
