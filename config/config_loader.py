# config/config_loader.py

import os
import yaml
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

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
    def __init__(self, base_dir=None):
        # Optional override: GRCAI_CONFIG_HOME. If unset, use default (e.g. host $HOME/config or container /config set by runtime).
        if base_dir is None:
            base_dir = os.environ.get("GRCAI_CONFIG_HOME", "config")
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
    
    def get_config_expectations(self, env_name: str, explicit_path: str = None) -> dict:
        """
        Extract config_expectations from environment YAML.
        
        Handles both formats:
        1. Top-level services (legacy): services.nginx.config_expectations
        2. Host-nested services (current): hosts[].services.nginx.config_expectations
        
        Args:
            env_name: Environment name
            explicit_path: Optional explicit path (file or directory)
        
        Returns:
            {
                "nginx": {
                    "enabled": True,
                    "responsibilities": ["routing", "reverse_proxy"],
                    "notes": "..."
                },
                ...
            }
            
        Returns empty dict if no config_expectations found or if environment doesn't exist.
        """
        try:
            env_config = self.load_environment(env_name, explicit_path)
            logger.debug(f"Loaded environment config for '{env_name}': keys={list(env_config.keys())}")
        except (FileNotFoundError, ValueError) as e:
            logger.warning(f"Failed to load environment '{env_name}': {e}")
            return {}
        
        config_expectations = {}
        
        # First, check for top-level services (legacy format)
        services = env_config.get("services", {})
        if isinstance(services, dict) and services:
            logger.debug(f"Found top-level services: {list(services.keys())}")
            for component_name, component_config in services.items():
                if not isinstance(component_config, dict):
                    continue
                
                expectations = component_config.get("config_expectations")
                if isinstance(expectations, dict) and expectations.get("enabled", False):
                    config_expectations[component_name] = {
                        "enabled": True,
                        "responsibilities": expectations.get("responsibilities", []),
                        "notes": expectations.get("notes", "")
                    }
                    logger.debug(f"Found config_expectations for '{component_name}' in top-level services")
        
        # Then, check for services nested under hosts[] (current format)
        hosts = env_config.get("hosts", [])
        if isinstance(hosts, list) and hosts:
            logger.debug(f"Found {len(hosts)} hosts, checking for nested services")
            for host in hosts:
                if not isinstance(host, dict):
                    continue
                
                host_name = host.get("name") or host.get("address", "unknown")
                host_services = host.get("services", {})
                if not isinstance(host_services, dict):
                    continue
                
                logger.debug(f"Host '{host_name}' has services: {list(host_services.keys())}")
                for component_name, component_config in host_services.items():
                    if not isinstance(component_config, dict):
                        continue
                    
                    expectations = component_config.get("config_expectations")
                    if isinstance(expectations, dict) and expectations.get("enabled", False):
                        # If component already exists, merge responsibilities (avoid duplicates)
                        if component_name in config_expectations:
                            existing_resp = set(config_expectations[component_name]["responsibilities"])
                            new_resp = set(expectations.get("responsibilities", []))
                            merged_resp = list(existing_resp | new_resp)
                            config_expectations[component_name]["responsibilities"] = merged_resp
                            logger.debug(f"Merged config_expectations for '{component_name}' from host '{host_name}' (responsibilities: {merged_resp})")
                        else:
                            config_expectations[component_name] = {
                                "enabled": True,
                                "responsibilities": expectations.get("responsibilities", []),
                                "notes": expectations.get("notes", "")
                            }
                            logger.debug(f"Found config_expectations for '{component_name}' from host '{host_name}'")
        
        if config_expectations:
            logger.info(f"Extracted config_expectations for {len(config_expectations)} components: {list(config_expectations.keys())}")
        else:
            logger.warning(f"No config_expectations found in environment '{env_name}'")
        
        return config_expectations