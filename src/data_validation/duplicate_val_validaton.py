from src.utility.validation import write_output


def duplicate_check(df, columns):
    if isinstance(columns, str):
        columns = [columns]
    for col_name in columns:
        dup_count = df.groupBy(col_name).count().filter("count > 1").count()
        if dup_count > 0:
            status = 'fail'
            details = f'Duplicate found in column {col_name}, count: {dup_count}'
            write_output(validation_type='duplicate_check', status=status, details=details)
            return status
    status = 'pass'
    details = 'No duplicates found'
    write_output(validation_type='duplicate_check', status=status, details=details)
    return status
