from src.utility.validation import write_output


def count_check(source, target):
    src_count = source.count()
    trg_count = target.count()
    if src_count == trg_count:
        status = 'pass'
        details = f'Row count match: {src_count}'
    else:
        status = 'fail'
        details = f'Row count mismatch: source={src_count}, target={trg_count}'
    write_output(validation_type='count_check', status=status, details=details)
    return status
