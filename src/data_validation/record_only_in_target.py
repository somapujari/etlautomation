from src.utility.validation import write_output


def record_only_in_target(source, target, key_columns):
    """Check for records present in target but missing in source (unexpected records)."""
    extra_df = target.select(*key_columns).exceptAll(source.select(*key_columns))
    extra_count = extra_df.count()
    if extra_count > 0:
        status = 'fail'
        details = f'{extra_count} records in target not found in source'
    else:
        status = 'pass'
        details = 'No unexpected records in target'
    write_output(validation_type='record_only_in_target', status=status, details=details)
    return status
