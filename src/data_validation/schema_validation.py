def schema_check(source, target):
    """Check if source and target schemas are the same."""
    if source.schema == target.schema:
        status = 'pass'
    else:
        status = 'fail'
    return status
