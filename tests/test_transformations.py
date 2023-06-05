from pyspark_tests.transformations import sort_columns
from chispa.dataframe_comparer import assert_df_equality
import pyspark.sql.functions as F
import pyspark_tests.transformations as T
import pyspark_tests.string_helpers as SH


def test_sort_columns_asc(spark):
    source_data = [
        ("jose", "oak", "switch"),
        ("li", "redwood", "xbox"),
        ("luisa", "maple", "ps4"),
    ]
    source_df = spark.createDataFrame(source_data, ["name", "tree", "gaming_system"])

    actual_df = sort_columns(source_df, "asc")

    expected_data = [
        ("switch", "jose", "oak"),
        ("xbox", "li", "redwood"),
        ("ps4", "luisa", "maple"),
    ]
    expected_df = spark.createDataFrame(expected_data, ["gaming_system", "name", "tree"])

    assert_df_equality(actual_df, expected_df)


def test_sort_columns_desc(spark):
    source_data = [
        ("jose", "oak", "switch"),
        ("li", "redwood", "xbox"),
        ("luisa", "maple", "ps4"),
    ]
    source_df = spark.createDataFrame(source_data, ["name", "tree", "gaming_system"])

    actual_df = sort_columns(source_df, "desc")

    expected_data = [
        ("oak", "jose", "switch"),
        ("redwood", "li", "xbox"),
        ("maple", "luisa", "ps4"),
    ]
    expected_df = spark.createDataFrame(expected_data, ["tree", "name", "gaming_system"])

    assert_df_equality(actual_df, expected_df)


def test_modify_column_names_error(spark):
    source_data = [
        ("jose", 8),
        ("li", 23),
        ("luisa", 48),
    ]
    source_df = spark.createDataFrame(source_data, ["first.name", "person.favorite.number"])

    actual_df = T.modify_column_names(source_df, SH.dots_to_underscores)

    expected_data = [
        ("jose", 8),
        ("li", 23),
        ("luisa", 48),
    ]
    expected_df = spark.createDataFrame(expected_data, ["first_name", "person_favorite_number"])

    assert_df_equality(actual_df, expected_df)


def test_schema_mismatch_message(spark):
    data1 = [
        (1, "a"),
        (2, "b"),
        (3, "c"),
        (None, None)
    ]
    df1 = spark.createDataFrame(data1, ["num", "letter"])

    data2 = [
        (1, 6),
        (2, 7),
        (3, 8),
        (None, None)
    ]
    df2 = spark.createDataFrame(data2, ["num", "num2"])

    assert_df_equality(df1, df2)