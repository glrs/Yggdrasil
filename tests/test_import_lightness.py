"""Importing the realm-facing packages and the contract has no side effects.

The realm-facing packages leave logging and the CouchDB client alone; the
contract package loads nothing beyond the standard library.
"""

import json
import subprocess
import sys
import unittest

# Runs in a fresh interpreter so modules imported by other tests cannot mask a regression.
_PROBE = """
import json
import logging
import sys

import yggdrasil.flow
import yggdrasil.watchers

print(json.dumps({
    "ibmcloudant": sorted(m for m in sys.modules if m.split(".")[0] == "ibmcloudant"),
    "logging_utils": "yggdrasil.logging_utils" in sys.modules,
    "debug_name": logging.getLevelName(logging.DEBUG),
    "pil_level": logging.getLogger("PIL").level,
    "cloudant_level": logging.getLogger("ibmcloudant.cloudant_v1").level,
}))
"""

_LOGGING_UTILS_PROBE = """
import json
import logging

import yggdrasil.logging_utils

names = [
    "matplotlib", "numba", "h5py", "PIL", "watchdog",
    "ibm-cloud-sdk-core", "ibmcloudant.cloudant_v1", "urllib3.connectionpool",
]
root = logging.getLogger()
print(json.dumps({
    "level_names": [
        logging.getLevelName(level)
        for level in (logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL)
    ],
    "logger_levels": {name: logging.getLogger(name).level for name in names},
    "root_level": root.level,
    "root_handlers": len(root.handlers),
}))
"""


# Measured after ``import yggdrasil``, so the package initializer's own version
# lookup is not counted against the contract. Reading a published schema needs
# no validation library either; only validating does.
_CONTRACT_PROBE = """
import json
import logging
import sys

import yggdrasil

before = set(sys.modules)

import yggdrasil.contract
import yggdrasil.contract.controls
import yggdrasil.contract.diagnostics
import yggdrasil.contract.fields
import yggdrasil.contract.problems
import yggdrasil.contract.read
import yggdrasil.contract.read._correlate
import yggdrasil.contract.read._eligibility
import yggdrasil.contract.read._ids
import yggdrasil.contract.read._plan
import yggdrasil.contract.read._requests
import yggdrasil.contract.read._results
import yggdrasil.contract.read._snapshot
import yggdrasil.contract.read._steps
import yggdrasil.contract.read._values
import yggdrasil.contract.schema
import yggdrasil.contract.timestamps
import yggdrasil.contract.versions

yggdrasil.contract.schema.schema_for("plan_document", 1)

loaded = sorted(set(sys.modules) - before)
print(json.dumps({
    "loaded": loaded,
    "foreign": [
        name for name in loaded
        if name.split(".")[0] not in sys.stdlib_module_names
        and name != "yggdrasil.contract"
        and not name.startswith("yggdrasil.contract.")
    ],
    "debug_name": logging.getLevelName(logging.DEBUG),
    "root_level": logging.getLogger().level,
    "root_handlers": len(logging.getLogger().handlers),
}))
"""


def _within(module: str, package: str) -> bool:
    """Return whether ``module`` is ``package`` itself or one of its submodules."""
    return module == package or module.startswith(f"{package}.")


def _run_probe(probe: str) -> dict:
    """Run ``probe`` in a fresh interpreter and return the JSON it prints."""
    result = subprocess.run(
        [sys.executable, "-c", probe],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


class TestImportLightness(unittest.TestCase):
    """``import yggdrasil.flow, yggdrasil.watchers`` has no process-wide side effects."""

    @classmethod
    def setUpClass(cls):
        cls.state = _run_probe(_PROBE)

    def test_couchdb_client_not_imported(self):
        self.assertEqual(self.state["ibmcloudant"], [])

    def test_logging_utils_not_imported(self):
        self.assertFalse(self.state["logging_utils"])

    def test_level_names_untouched(self):
        self.assertEqual(self.state["debug_name"], "DEBUG")

    def test_third_party_logger_levels_untouched(self):
        self.assertEqual(self.state["pil_level"], 0)  # logging.NOTSET
        self.assertEqual(self.state["cloudant_level"], 0)


class TestLoggingUtilsImport(unittest.TestCase):
    """Importing ``yggdrasil.logging_utils`` itself leaves logging untouched.

    Process-wide setup belongs in ``configure_logging()``; this catches it
    creeping back to module scope, which the guard above cannot see because
    ``logging_utils`` is outside the realm-facing import graph.
    """

    @classmethod
    def setUpClass(cls):
        cls.state = _run_probe(_LOGGING_UTILS_PROBE)

    def test_level_names_untouched(self):
        self.assertEqual(
            self.state["level_names"], ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        )

    def test_third_party_logger_levels_untouched(self):
        for name, level in self.state["logger_levels"].items():
            with self.subTest(logger=name):
                self.assertEqual(level, 0)  # logging.NOTSET

    def test_root_logger_untouched(self):
        self.assertEqual(self.state["root_level"], 30)  # logging.WARNING, the default
        self.assertEqual(self.state["root_handlers"], 0)


class TestContractImport(unittest.TestCase):
    """``yggdrasil.contract`` sits in the bottom layer: standard library only.

    That includes its schemas, whose reading loads no validation library, and
    its reader of plan documents and snapshots, which a program outside
    Yggdrasil imports without the daemon or the CouchDB client.
    """

    @classmethod
    def setUpClass(cls):
        cls.state = _run_probe(_CONTRACT_PROBE)

    def test_the_probe_measured_the_contract(self):
        self.assertIn("yggdrasil.contract.versions", self.state["loaded"])
        self.assertIn("yggdrasil.contract.diagnostics", self.state["loaded"])
        self.assertIn("yggdrasil.contract.schema", self.state["loaded"])
        self.assertIn("yggdrasil.contract.fields", self.state["loaded"])
        self.assertIn("yggdrasil.contract.problems", self.state["loaded"])
        for module in (
            "read",
            "read._correlate",
            "read._eligibility",
            "read._ids",
            "read._plan",
            "read._requests",
            "read._results",
            "read._snapshot",
            "read._steps",
            "read._values",
        ):
            with self.subTest(module=module):
                self.assertIn(f"yggdrasil.contract.{module}", self.state["loaded"])

    def test_only_standard_library_modules_are_loaded(self):
        self.assertEqual(self.state["foreign"], [])

    def test_no_higher_layer_or_third_party_module_is_loaded(self):
        for package in (
            "ibmcloudant",
            "jsonschema",
            "referencing",
            "yggdrasil.logging_utils",
            "yggdrasil.flow",
            "yggdrasil.storage",
            "yggdrasil.daemon",
        ):
            with self.subTest(package=package):
                self.assertEqual(
                    [m for m in self.state["loaded"] if _within(m, package)], []
                )

    def test_logging_untouched(self):
        self.assertEqual(self.state["debug_name"], "DEBUG")
        self.assertEqual(self.state["root_level"], 30)  # logging.WARNING, the default
        self.assertEqual(self.state["root_handlers"], 0)


if __name__ == "__main__":
    unittest.main()
