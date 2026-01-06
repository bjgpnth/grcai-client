# connectors/registry.py
"""
Connector registry.
Maps component name -> adapter class.
"""

from typing import Dict, Type
from connectors.base_connector import BaseConnector

CONNECTOR_REGISTRY: Dict[str, Type[BaseConnector]] = {}

def _safe_register(name: str, module_path: str, class_name: str):
    """
    Safely import adapter class and register it.
    If missing, silently skip (so unused connectors don't break CLI/UI).
    """
    try:
        module = __import__(module_path, fromlist=[class_name])
        cls = getattr(module, class_name)

        # ensure class inherits from BaseConnector (safety check)
        if not issubclass(cls, BaseConnector):
            raise TypeError(f"{class_name} does not subclass BaseConnector")

        CONNECTOR_REGISTRY[name] = cls

    except Exception as e:
        # Log import failures for debugging
        import logging
        logger = logging.getLogger("grcai.registry")
        logger.debug(f"Registry skip {name}: {e}", exc_info=True)
        # Also print for visibility during development
        print(f"⚠️ Registry: Failed to register '{name}': {e}")


# ----------------------------------------------------------
# REGISTER AVAILABLE ADAPTERS
# ----------------------------------------------------------
_safe_register("tomcat", "connectors.adapters.tomcat_adapter", "TomcatAdapter")
_safe_register("nginx", "connectors.adapters.nginx_adapter", "NginxAdapter")
_safe_register("os",     "connectors.adapters.os_adapter", "OSAdapter")
_safe_register("postgres", "connectors.adapters.postgres_adapter", "PostgresAdapter")
_safe_register("redis", "connectors.adapters.redis_adapter", "RedisAdapter")
_safe_register("kafka", "connectors.adapters.kafka_adapter", "KafkaAdapter")
_safe_register("mssql", "connectors.adapters.mssql_adapter", "MSSQLAdapter")
_safe_register("nodejs", "connectors.adapters.nodejs_adapter", "NodeJSAdapter")
_safe_register("docker", "connectors.adapters.docker_adapter", "DockerAdapter")

# future:
# _safe_register("db", "connectors.adapters.db_adapter", "DBAdapter")




