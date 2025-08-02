from pyspark.sql.functions import col, upper
from src.utility.validation import write_output


def null_value_check(df, columns):
    """Check for null or placeholder values (NULL, NA, NONE, or empty) in specified columns."""
    if isinstance(columns, str):
        columns = [columns]
    for column in columns:
        # Build condition for null or empty values
        cond = (col(column).isNull() |
                (upper(col(column)) == 'NULL') |
                (upper(col(column)) == 'NA') |
                (upper(col(column)) == 'NONE') |
                (col(column) == ''))
        count_nulls = df.filter(cond).count()
        if count_nulls > 0:
            status = 'fail'
            details = f'Null/NA values found in column {column}, count: {count_nulls}'
            write_output(validation_type='null_value_check', status=status, details=details)
            return status
    status = 'pass'
    details = 'No null/NA values found'
    write_output(validation_type='null_value_check', status=status, details=details)
    return status
