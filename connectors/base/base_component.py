# connectors/base/base_component.py

class BaseComponent:
    """
    Base class that ALL component adapters inherit.
    Standardizes the constructor so adapters can be created uniformly.
    """

    def __init__(self, issue_time, name=None, env_config=None, component_config=None):
        self.issue_time = issue_time
        self.name = name or "unnamed"
        self.env_config = env_config or {}
        self.component_config = component_config or {}

    def collect(self):
        raise NotImplementedError("collect() must be implemented by subclasses")