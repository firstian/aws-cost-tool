import importlib
import inspect
import pkgutil

from aws_cost_tool.service_base import ServiceBase

# All these function can potentially be in the same module as ServiceBase. The
# reasons to keep them separate are:
# 1. Harder to test load_services independently.
# 2. The location of the services module is not coupled with the base class.
# 3. Avoid any potential circular import problem in the future.


# Private registry to prevent modification
_registry: dict[str, ServiceBase] = {}


def load_services(package_name: str = "services") -> None:
    """
    Function to be called on program startup to enumerate and load all the
    services modules.
    The package_name is the directory name that contains all the services modules.
    """
    global _registry
    tmp_registry = {}

    # Dynamically find the package specification. This avoids hard coding the
    # directory, which is both more flexible and easier to test.
    package = importlib.import_module(package_name)
    package_path = package.__path__

    # Iterate through modules in the 'services' folder
    for loader, module_name, is_pkg in pkgutil.iter_modules(package_path):
        full_module_name = f"{package_name}.{module_name}"
        module = importlib.import_module(full_module_name)

        # Look for classes inside the module
        for name, obj in inspect.getmembers(module, inspect.isclass):
            # Check if it's a subclass of our base, but NOT the base itself
            if issubclass(obj, ServiceBase) and obj is not ServiceBase:
                instance = obj()
                tmp_registry[instance.name] = instance

    # Ensure that the final registry is sorted by the name.
    _registry = {k: tmp_registry[k] for k in sorted(tmp_registry.keys())}


def services_names() -> list[str]:
    """Returns the list of all the services found."""
    return list(_registry.keys())


def get_service(name: str) -> ServiceBase:
    """
    Returns a specific service by name. It will raise an exception in the
    service implementation is not found.
    """
    return _registry[name]
