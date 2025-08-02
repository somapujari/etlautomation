from src.utility.validation import write_output


def uniqueness_check(df, key_columns):
    """Check that the combination of key_columns is unique in the DataFrame."""
    if isinstance(key_columns, str):
        key_columns = [key_columns]
    dup_df = df.groupBy(*key_columns).count().filter('count > 1')
    dup_count = dup_df.count()
    if dup_count > 0:
        status = 'fail'
        details = f'Duplicate key combinations found: count={dup_count}'
    else:
        status = 'pass'
        details = 'All key combinations are unique'
    write_output(validation_type='uniqueness_check', status=status, details=details)
    return status
