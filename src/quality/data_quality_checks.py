from pyspark.sql.functions import col

def check_not_null(df, columns):
    results = {}

    for column in columns:
        null_count = df.filter(col(column).isNull()).count()
        results[column] = null_count

    return results


def check_duplicates(df, primary_key):
    total_count = df.count()
    distinct_count = df.dropDuplicates([primary_key]).count()

    return {
        "total_count": total_count,
        "distinct_count": distinct_count,
        "duplicate_count": total_count - distinct_count
    }


def validate_allowed_values(df, column, allowed_values):
    invalid_count = df.filter(~col(column).isin(allowed_values)).count()

    return {
        "column": column,
        "invalid_count": invalid_count,
        "allowed_values": allowed_values
    }