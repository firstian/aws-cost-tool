import sys
from textwrap import dedent

import pytest

import aws_cost_tool.service_loader as service_loader
from aws_cost_tool.service_loader import (
    ServiceBase,
    get_service,
    load_services,
    services_names,
)


@pytest.fixture(autouse=True)
def clean_registry():
    """Ensure a fresh registry for every test."""
    service_loader._registry = {}
    yield
    # Tear down code goes here if needed.


def test_load_services_dynamic_discovery(tmp_path, monkeypatch):
    """
    Tests loading services without any physical files present. This test needs
    to use temp files because importlib is very sensitive to mocking and fake
    in-memory filesystems.
    """

    # Create a temporary 'services' directory and make it a package.
    pkg_dir = tmp_path / "services"
    pkg_dir.mkdir()
    (pkg_dir / "__init__.py").write_text("")

    # 2. Create a fake service file
    service_file = pkg_dir / "aws_service.py"
    service_file.write_text(
        dedent(
            """
        from aws_cost_tool.service_loader import ServiceBase

        class ComputeService(ServiceBase):
            @property
            def name(self): return "Compute"
            @property
            def shortname(self): return "EC2"
            def categorize_usage(self, df): return df
        
        class StorageService(ServiceBase):
            @property
            def name(self): return "Storage"
            @property
            def shortname(self): return "S3"
            def categorize_usage(self, df): return df
        """
        )
    )

    # Add the temp directory to sys.path so Python can 'import' it
    monkeypatch.syspath_prepend(str(tmp_path))

    # Remove 'services' from sys.modules if it exists (prevents old mocks or
    # real folders from interfering)
    monkeypatch.delitem(sys.modules, "services", raising=False)

    # Load services
    load_services("services")

    names = services_names()
    # Verify sorting (C before S)
    assert names == ["Compute", "Storage"]
    s = get_service("Compute")
    assert isinstance(s, ServiceBase)
    assert s.__class__.__name__ == "ComputeService"
    assert s.name == "Compute"

    s = get_service("Storage")
    assert isinstance(s, ServiceBase)
    assert s.__class__.__name__ == "StorageService"
    assert s.name == "Storage"
