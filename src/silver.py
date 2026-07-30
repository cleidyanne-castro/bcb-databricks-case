from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


BRONZE_SELIC_TABLE = "beanalytic_case.bronze.selic"
BRONZE_IPCA_TABLE = "beanalytic_case.bronze.ipca"

SILVER_SELIC_TABLE = "beanalytic_case.silver.selic"
SILVER_IPCA_TABLE = "beanalytic_case.silver.ipca"


def assert_not_empty(
    dataframe: DataFrame,
    dataset_name: str,
) -> None:
    if dataframe.limit(1).count() == 0:
        raise ValueError(
            f"{dataset_name} cannot be empty."
        )


def assert_no_nulls(
    dataframe: DataFrame,
    columns: list[str],
    dataset_name: str,
) -> None:
    null_condition = None

    for column_name in columns:
        current_condition = F.col(column_name).isNull()

        if null_condition is None:
            null_condition = current_condition
        else:
            null_condition = (
                null_condition | current_condition
            )

    null_count = dataframe.filter(
        null_condition
    ).count()

    if null_count > 0:
        raise ValueError(
            f"{dataset_name} contains "
            f"{null_count} rows with null values "
            f"in required columns: {columns}."
        )


def assert_unique_key(
    dataframe: DataFrame,
    business_key: str,
    dataset_name: str,
) -> None:
    duplicated_keys = (
        dataframe
        .groupBy(business_key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicated_keys > 0:
        raise ValueError(
            f"{dataset_name} contains "
            f"{duplicated_keys} duplicated "
            f"business keys."
        )


def assert_rate_range(
    dataframe: DataFrame,
    value_column: str,
    minimum: float,
    maximum: float,
    dataset_name: str,
) -> None:
    invalid_count = (
        dataframe
        .filter(
            ~F.col(value_column).between(
                minimum,
                maximum,
            )
        )
        .count()
    )

    if invalid_count > 0:
        raise ValueError(
            f"{dataset_name} contains "
            f"{invalid_count} values outside "
            f"the expected range "
            f"[{minimum}, {maximum}]."
        )


def transform_selic(
    bronze_dataframe: DataFrame,
) -> DataFrame:
    typed_dataframe = (
        bronze_dataframe
        .select(
            F.to_date(
                F.col("data_raw"),
                "dd/MM/yyyy",
            ).alias("reference_date"),
            F.regexp_replace(
                F.col("valor_raw"),
                ",",
                ".",
            )
            .cast("decimal(18, 8)")
            .alias("selic_daily_pct"),
            F.col("source_file"),
            F.col("source_path"),
            F.col("ingestion_timestamp"),
        )
    )

    window = (
        Window
        .partitionBy("reference_date")
        .orderBy(
            F.col(
                "ingestion_timestamp"
            ).desc()
        )
    )

    return (
        typed_dataframe
        .withColumn(
            "row_number",
            F.row_number().over(window),
        )
        .filter(F.col("row_number") == 1)
        .drop("row_number")
        .withColumn(
            "silver_updated_at",
            F.current_timestamp(),
        )
    )


def transform_ipca(
    bronze_dataframe: DataFrame,
) -> DataFrame:
    typed_dataframe = (
        bronze_dataframe
        .select(
            F.to_date(
                F.col("data_raw"),
                "dd/MM/yyyy",
            ).alias("reference_date"),
            F.regexp_replace(
                F.col("valor_raw"),
                ",",
                ".",
            )
            .cast("decimal(18, 8)")
            .alias("ipca_monthly_pct"),
            F.col("source_file"),
            F.col("source_path"),
            F.col("ingestion_timestamp"),
        )
        .withColumn(
            "reference_month",
            F.trunc(
                F.col("reference_date"),
                "month",
            ),
        )
    )

    window = (
        Window
        .partitionBy("reference_month")
        .orderBy(
            F.col(
                "ingestion_timestamp"
            ).desc()
        )
    )

    return (
        typed_dataframe
        .withColumn(
            "row_number",
            F.row_number().over(window),
        )
        .filter(F.col("row_number") == 1)
        .drop("row_number")
        .withColumn(
            "silver_updated_at",
            F.current_timestamp(),
        )
    )


def merge_to_silver(
    spark: SparkSession,
    source_dataframe: DataFrame,
    target_table: str,
    business_key: str,
) -> None:
    if not spark.catalog.tableExists(target_table):
        (
            source_dataframe.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )
        return

    target = DeltaTable.forName(
        spark,
        target_table,
    )

    merge_condition = (
        f"target.{business_key} = "
        f"source.{business_key}"
    )

    (
        target.alias("target")
        .merge(
            source_dataframe.alias("source"),
            merge_condition,
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def validate_selic(
    dataframe: DataFrame,
) -> None:
    assert_not_empty(
        dataframe,
        "Silver SELIC",
    )

    assert_no_nulls(
        dataframe=dataframe,
        columns=[
            "reference_date",
            "selic_daily_pct",
        ],
        dataset_name="Silver SELIC",
    )

    assert_unique_key(
        dataframe=dataframe,
        business_key="reference_date",
        dataset_name="Silver SELIC",
    )

    assert_rate_range(
        dataframe=dataframe,
        value_column="selic_daily_pct",
        minimum=-5,
        maximum=10,
        dataset_name="Silver SELIC",
    )


def validate_ipca(
    dataframe: DataFrame,
) -> None:
    assert_not_empty(
        dataframe,
        "Silver IPCA",
    )

    assert_no_nulls(
        dataframe=dataframe,
        columns=[
            "reference_month",
            "ipca_monthly_pct",
        ],
        dataset_name="Silver IPCA",
    )

    assert_unique_key(
        dataframe=dataframe,
        business_key="reference_month",
        dataset_name="Silver IPCA",
    )

    assert_rate_range(
        dataframe=dataframe,
        value_column="ipca_monthly_pct",
        minimum=-20,
        maximum=50,
        dataset_name="Silver IPCA",
    )


def run_silver(
    spark: SparkSession,
) -> None:
    bronze_selic = spark.table(
        BRONZE_SELIC_TABLE
    )

    bronze_ipca = spark.table(
        BRONZE_IPCA_TABLE
    )

    silver_selic = transform_selic(
        bronze_selic
    )

    silver_ipca = transform_ipca(
        bronze_ipca
    )

    validate_selic(silver_selic)
    validate_ipca(silver_ipca)

    merge_to_silver(
        spark=spark,
        source_dataframe=silver_selic,
        target_table=SILVER_SELIC_TABLE,
        business_key="reference_date",
    )

    merge_to_silver(
        spark=spark,
        source_dataframe=silver_ipca,
        target_table=SILVER_IPCA_TABLE,
        business_key="reference_month",
    )