from src.utils.spark_session import get_spark_session
from src.quality.data_quality_checks import (
    check_not_null,
    check_duplicates,
    validate_allowed_values
)

def test_clickstream_data_quality():
    spark = get_spark_session("Data Quality Test")

    data = [
        ("evt_1", "user_1", "purchase"),
        ("evt_2", "user_2", "page_view"),
        ("evt_2", "user_2", "page_view"),
    ]

    columns = ["event_id", "user_id", "event_type"]
    df = spark.createDataFrame(data, columns)

    null_results = check_not_null(df, ["event_id", "user_id"])
    duplicate_results = check_duplicates(df, "event_id")
    allowed_results = validate_allowed_values(
        df,
        "event_type",
        ["page_view", "product_view", "add_to_cart", "purchase"]
    )

    assert null_results["event_id"] == 0
    assert duplicate_results["duplicate_count"] == 1
    assert allowed_results["invalid_count"] == 0