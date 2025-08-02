import yaml
from pathlib import Path
import yaml


# def load_credentials(path='config/cred_config.yml'):
#     """Load credentials from YAML file."""
#     with open(path, 'r') as f:
#         creds = yaml.safe_load(f)
#     return creds

# def load_credentials(path='config/cred_config.yml'):
#     """Load credentials from YAML file, resolving path from project root."""
#     base_dir = Path(__file__).resolve().parent.parent  # Adjust based on your folder depth
#     config_path = base_dir / path
#     if not config_path.exists():
#         raise FileNotFoundError(f"❌ Credentials file not found at: {config_path}")
#     with open(config_path, 'r') as f:
#         return yaml.safe_load(f)


def write_output(*args, **kwargs):
    """Placeholder for writing validation results. In real usage, append to report or log."""
    if args or kwargs:
        print(f"Validation output: args={args}, kwargs={kwargs}")
