# config/config_loader.py

import yaml
from pathlib import Path

# class ConfigLoader:
#     def __init__(self, base_dir="config"):
#         self.base_dir = Path(base_dir)

#     def _load_yaml(self, path: Path):
#         p = Path(path)
#         if not p.exists():
#             raise FileNotFoundError(f"YAML not found: {p}")
#         return yaml.safe_load(p.read_text()) or {}

#     def list_environments(self):
#         envs = []
#         if not self.base_dir.exists():
#             return envs
#         for item in self.base_dir.iterdir():
#             if item.is_dir() and (item / f"{item.name}.yaml").exists():
#                 envs.append(item.name)
#         return sorted(envs)

#     def load_environment(self, env_name: str, explicit_path: str = None):
#         from pathlib import Path

#         if explicit_path:
#             explicit_path = Path(explicit_path)
#             if explicit_path.is_file():
#                 return self._load_yaml(explicit_path)
#             yaml_path = explicit_path / f"{env_name}.yaml"
#             return self._load_yaml(yaml_path)

#         # ✅ Correct default lookup (your project structure)
#         default_path = Path("config") / env_name / f"{env_name}.yaml"
#         return self._load_yaml(default_path)
    

class ConfigLoader:
    def __init__(self, base_dir="config"):
        self.base_dir = Path(base_dir)

    def _load_yaml(self, path: Path):
        if not Path(path).exists():
            raise FileNotFoundError(f"YAML not found: {path}")
        return yaml.safe_load(Path(path).read_text()) or {}

    def list_environments(self):
        """
        List all available environments by scanning config directory.
        
        Returns:
            list: Sorted list of environment names
        """
        envs = []
        if not self.base_dir.exists():
            return envs
        
        for item in self.base_dir.iterdir():
            if item.is_dir() and (item / f"{item.name}.yaml").exists():
                envs.append(item.name)
        
        return sorted(envs)

    def load_environment(self, env_name: str, explicit_path: str = None):
        """
        Correct behavior:
        1) If explicit_path is FILE → load that file.
        2) If explicit_path is DIR → load DIR/<env>.yaml
        3) Otherwise → load config/<env>/<env>.yaml
        """
        if env_name is None:
            raise ValueError("Environment name cannot be None")
        
        if explicit_path:
            exp = Path(explicit_path)

            # CASE 1 — explicit_path = file
            if exp.is_file():
                return self._load_yaml(exp)

            # CASE 2 — explicit_path = directory
            yaml_path = exp / f"{env_name}.yaml"
            return self._load_yaml(yaml_path)

        # CASE 3 — normal runtime path: config/<env>/<env>.yaml
        default_path = self.base_dir / env_name / f"{env_name}.yaml"
        return self._load_yaml(default_path)
    
    def load_reasoning_budget(self, environment: str = None) -> dict:
        """
        Load reasoning budget configuration with optional environment-specific overrides.
        
        Args:
            environment: Optional environment name (e.g., "prod", "qa") for overrides
        
        Returns:
            dict: Reasoning budget configuration
        
        Configuration hierarchy:
        1. Load base config from config/reasoning_budget.yaml
        2. If environment specified, load override from config/<env>/reasoning_budget.yaml
        3. Merge overrides into base config
        """
        # Load base configuration
        base_path = self.base_dir / "reasoning_budget.yaml"
        if not base_path.exists():
            raise FileNotFoundError(f"Base reasoning budget config not found: {base_path}")
        
        base_config = self._load_yaml(base_path)
        
        # Load environment-specific override if provided
        if environment:
            override_path = self.base_dir / environment / "reasoning_budget.yaml"
            if override_path.exists():
                override_config = self._load_yaml(override_path)
                # Deep merge: override nested dicts
                base_config = self._deep_merge(base_config, override_config)
        
        return base_config
    
    def _deep_merge(self, base: dict, override: dict) -> dict:
        """
        Deep merge two dictionaries, with override taking precedence.
        
        Args:
            base: Base dictionary
            override: Override dictionary
        
        Returns:
            dict: Merged dictionary
        """
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result