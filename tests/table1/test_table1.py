import pytest

from src.data_validation.count_validation import count_check
from src.data_validation.null_val_validation import null_value_check
from src.data_validation.duplicate_val_validaton import duplicate_check
from src.data_validation.uniqueness_validation import uniqueness_check
from src.data_validation.record_only_in_source import record_only_in_source
from src.data_validation.record_only_in_target import record_only_in_target
from src.data_validation.schema_validation import schema_check


@pytest.mark.usefixtures("read_data", "read_config")
class TestETL:

    def test_count_check(self,  read_data, read_config):
        source, target = read_data
        # source = source.limit(10)
        config = read_config
        key_columns = config['validations']['count_check']['key_columns']
        print("key col", key_columns)
        assert count_check(source=source, target=target) == 'pass'

    def test_duplicate_check(self, read_data, read_config):
        source, target = read_data
        config = read_config
        columns = config['validations']['duplicate_check']['key_columns']
        print("key col", columns)
        assert duplicate_check(df=target, columns=columns) == 'pass'

    def test_null_check(self, read_data, read_config):
        source, target = read_data
        config = read_config
        null_cols = config['validations']['null_check']['null_columns']
        print("null_cols", null_cols)
        assert null_value_check(df=target, columns=null_cols) == 'pass'

    def test_uniqueness_check(self, read_data, read_config):
        source, target = read_data
        config = read_config
        unique_cols = config['validations']['uniqueness_check']['unique_columns']
        print("unique_cols", unique_cols)
        assert uniqueness_check(df=target, key_columns=unique_cols) == 'pass'

    def test_only_in_source(self, read_data, read_config):
        if read_config['test_cases']['record_only_in_source'] == 'Y':
            source_df, target_df = read_data
            keys = read_config['key_columns']
            assert record_only_in_source(source_df, target_df, keys) == 'pass'

    def test_only_in_target(self, read_data, read_config):
        if read_config['test_cases']['record_only_in_target'] == 'Y':
            source_df, target_df = read_data
            keys = read_config['key_columns']
            assert record_only_in_target(source_df, target_df, keys) == 'pass'

    def test_schema_check(self, read_data, read_config):
        if read_config['test_cases']['schema_check'] == 'Y':
            source_df, target_df = read_data
            assert schema_check(source_df, target_df) == 'pass'



