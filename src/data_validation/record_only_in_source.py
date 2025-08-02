from src.utility.validation import write_output


def record_only_in_source(source, target, key_columns):
    """Check for records present in source but missing in target (data completeness)."""
    missing_df = source.select(*key_columns).exceptAll(target.select(*key_columns))
    missing_count = missing_df.count()
    if missing_count > 0:
        status = 'fail'
        details = f'{missing_count} records in source not found in target'
    else:
        status = 'pass'
        details = 'All source records found in target'
    write_output(validation_type='record_only_in_source', status=status, details=details)
    return status
