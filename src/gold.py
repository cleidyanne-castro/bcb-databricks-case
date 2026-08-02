from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


SILVER_SELIC_TABLE = "beanalytic_case.silver.selic"
SILVER_IPCA_TABLE = "beanalytic_case.silver.ipca"

GOLD_TABLE = (
    "beanalytic_case.gold."
    "interest_inflation_monthly"
)


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
        current_condition = F.col(
            column_name
        ).isNull()

        null_condition = (
            current_condition
            if null_condition is None
            else null_condition | current_condition
        )

    invalid_count = dataframe.filter(
        null_condition
    ).count()

    if invalid_count > 0:
        raise ValueError(
            f"{dataset_name} contains "
            f"{invalid_count} rows with null values "
            f"in required columns: {columns}."
        )


def assert_unique_month(
    dataframe: DataFrame,
) -> None:
    duplicated_months = (
        dataframe
        .groupBy("reference_month")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicated_months > 0:
        raise ValueError(
            "Gold table contains duplicated months."
        )


def build_monthly_selic(
    silver_selic: DataFrame,
) -> DataFrame:
    return (
        silver_selic
        .withColumn(
            "reference_month",
            F.trunc(
                F.col("reference_date"),
                "month",
            ),
        )
        .groupBy("reference_month")
        .agg(
            F.avg(
                F.col("selic_daily_pct")
            ).alias(
                "selic_daily_average_pct"
            ),
            (
                (
                    F.exp(
                        F.sum(
                            F.log(
                                F.lit(1.0)
                                + (
                                    F.col(
                                        "selic_daily_pct"
                                    ).cast("double")
                                    / F.lit(100.0)
                                )
                            )
                        )
                    )
                    - F.lit(1.0)
                )
                * F.lit(100.0)
            ).alias(
                "selic_monthly_effective_pct"
            ),
            F.count("*").alias(
                "selic_observation_count"
            ),
        )
    )


def build_monthly_ipca(
    silver_ipca: DataFrame,
) -> DataFrame:
    return silver_ipca.select(
        "reference_month",
        F.col("ipca_monthly_pct").cast(
            "double"
        ).alias("ipca_monthly_pct"),
    )


def build_gold(
    silver_selic: DataFrame,
    silver_ipca: DataFrame,
) -> DataFrame:
    monthly_selic = build_monthly_selic(
        silver_selic
    )

    monthly_ipca = build_monthly_ipca(
        silver_ipca
    )

    monthly_window = (
        Window
        .orderBy("reference_month")
        .rowsBetween(-11, 0)
    )

    joined = (
        monthly_selic
        .join(
            monthly_ipca,
            on="reference_month",
            how="inner",
        )
        .withColumn(
            "real_interest_monthly_pct",
            (
                (
                    (
                        F.lit(1.0)
                        + (
                            F.col(
                                "selic_monthly_effective_pct"
                            )
                            / F.lit(100.0)
                        )
                    )
                    /
                    (
                        F.lit(1.0)
                        + (
                            F.col(
                                "ipca_monthly_pct"
                            )
                            / F.lit(100.0)
                        )
                    )
                )
                - F.lit(1.0)
            )
            * F.lit(100.0),
        )
        .withColumn(
            "months_in_window",
            F.count("*").over(monthly_window),
        )
    )

    with_accumulated_rates = (
        joined
        .withColumn(
            "selic_accumulated_12m_raw",
            (
                F.exp(
                    F.sum(
                        F.log(
                            F.lit(1.0)
                            + (
                                F.col(
                                    "selic_monthly_effective_pct"
                                )
                                / F.lit(100.0)
                            )
                        )
                    ).over(monthly_window)
                )
                - F.lit(1.0)
            )
            * F.lit(100.0),
        )
        .withColumn(
            "ipca_accumulated_12m_raw",
            (
                F.exp(
                    F.sum(
                        F.log(
                            F.lit(1.0)
                            + (
                                F.col(
                                    "ipca_monthly_pct"
                                )
                                / F.lit(100.0)
                            )
                        )
                    ).over(monthly_window)
                )
                - F.lit(1.0)
            )
            * F.lit(100.0),
        )
    )

    return (
        with_accumulated_rates
        .withColumn(
            "selic_accumulated_12m_pct",
            F.when(
                F.col("months_in_window") == 12,
                F.col(
                    "selic_accumulated_12m_raw"
                ),
            ),
        )
        .withColumn(
            "ipca_accumulated_12m_pct",
            F.when(
                F.col("months_in_window") == 12,
                F.col(
                    "ipca_accumulated_12m_raw"
                ),
            ),
        )
        .withColumn(
            "real_interest_accumulated_12m_pct",
            F.when(
                F.col("months_in_window") == 12,
                (
                    (
                        (
                            F.lit(1.0)
                            + (
                                F.col(
                                    "selic_accumulated_12m_raw"
                                )
                                / F.lit(100.0)
                            )
                        )
                        /
                        (
                            F.lit(1.0)
                            + (
                                F.col(
                                    "ipca_accumulated_12m_raw"
                                )
                                / F.lit(100.0)
                            )
                        )
                    )
                    - F.lit(1.0)
                )
                * F.lit(100.0),
            ),
        )
        .select(
            "reference_month",
            F.round(
                "selic_daily_average_pct",
                8,
            ).alias(
                "selic_daily_average_pct"
            ),
            F.round(
                "selic_monthly_effective_pct",
                8,
            ).alias(
                "selic_monthly_effective_pct"
            ),
            F.round(
                "ipca_monthly_pct",
                8,
            ).alias(
                "ipca_monthly_pct"
            ),
            F.round(
                "real_interest_monthly_pct",
                8,
            ).alias(
                "real_interest_monthly_pct"
            ),
            F.round(
                "selic_accumulated_12m_pct",
                8,
            ).alias(
                "selic_accumulated_12m_pct"
            ),
            F.round(
                "ipca_accumulated_12m_pct",
                8,
            ).alias(
                "ipca_accumulated_12m_pct"
            ),
            F.round(
                "real_interest_accumulated_12m_pct",
                8,
            ).alias(
                "real_interest_accumulated_12m_pct"
            ),
            "selic_observation_count",
            F.current_timestamp().alias(
                "gold_updated_at"
            ),
        )
    )


def validate_gold(
    dataframe: DataFrame,
) -> None:
    assert_not_empty(
        dataframe,
        "Gold monthly table",
    )

    assert_no_nulls(
        dataframe=dataframe,
        columns=[
            "reference_month",
            "selic_daily_average_pct",
            "selic_monthly_effective_pct",
            "ipca_monthly_pct",
            "real_interest_monthly_pct",
        ],
        dataset_name="Gold monthly table",
    )

    assert_unique_month(dataframe)

    invalid_observations = (
        dataframe
        .filter(
            F.col(
                "selic_observation_count"
            ) <= 0
        )
        .count()
    )

    if invalid_observations > 0:
        raise ValueError(
            "Gold table contains months without "
            "SELIC observations."
        )


def merge_to_gold(
    spark: SparkSession,
    source_dataframe: DataFrame,
) -> None:
    if not spark.catalog.tableExists(
        GOLD_TABLE
    ):
        (
            source_dataframe.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(GOLD_TABLE)
        )
        return

    target = DeltaTable.forName(
        spark,
        GOLD_TABLE,
    )

    (
        target.alias("target")
        .merge(
            source_dataframe.alias("source"),
            (
                "target.reference_month = "
                "source.reference_month"
            ),
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_gold(
    spark: SparkSession,
) -> None:
    silver_selic = spark.table(
        SILVER_SELIC_TABLE
    )

    silver_ipca = spark.table(
        SILVER_IPCA_TABLE
    )

    gold_dataframe = build_gold(
        silver_selic=silver_selic,
        silver_ipca=silver_ipca,
    )

    validate_gold(gold_dataframe)

    merge_to_gold(
        spark=spark,
        source_dataframe=gold_dataframe,
    )