# connectors/schema.py

from typing import TypedDict, List, Optional

# --------------------------
# Base host definition
# --------------------------

class Host(TypedDict):
    name: str
    address: str
    type: str
    tags: List[str]


# --------------------------
# Tomcat Instance Schema
# --------------------------

class TomcatInstance(TypedDict):
    name: str               # logical instance name
    host: str               # reference to host.name
    catalina_home: str      # mandatory absolute path
    systemd_service: Optional[str]  # optional
    port: Optional[int]     # optional


class TomcatServiceConfig(TypedDict):
    instances: List[TomcatInstance]


# --------------------------
# Full Environment Schema
# --------------------------

class EnvironmentConfig(TypedDict):
    access: dict
    hosts: List[Host]
    services: dict  # keys like "tomcat", "nginx", "db", etc.
