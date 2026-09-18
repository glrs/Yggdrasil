import logging
import os
import sys
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch

from requests.exceptions import ConnectionError as RequestsConnectionError

from lib.core_utils.daemon_lock import DaemonLockError
from lib.core_utils.errors import (
    ExternalSystemUnavailableError,
    InternalStorageConfigurationError,
)
from lib.couchdb.couchdb_connection import CouchDBClientFactory
from lib.watchers.config_validation import (
    WatcherConfigurationError,
    WatcherConfigValidationIssue,
)
from yggdrasil.cli import main


class TestYggdrasilCLI(unittest.TestCase):
    """
    Comprehensive tests for Yggdrasil CLI - the application entry point.

    Tests argument parsing, mode selection, configuration loading, session management,
    daemon mode, run-doc mode, error handling, and integration scenarios.
    """

    def setUp(self):
        """Set up test fixtures and reset global state."""
        # Reset YggSession state before each test
        from lib.core_utils.ygg_session import YggSession

        YggSession._YggSession__dev_mode = False  # type: ignore
        YggSession._YggSession__dev_already_set = False  # type: ignore
        YggSession._YggSession__manual_submit = False  # type: ignore
        YggSession._YggSession__manual_already_set = False  # type: ignore

        # Store original sys.argv
        self.original_argv = sys.argv.copy()

        # Mock objects that will be used across tests
        self.mock_config = {"test": "config", "couchdb_poll_interval": 5}

    def tearDown(self):
        """Clean up after each test."""
        # Restore original sys.argv
        sys.argv = self.original_argv

        # Reset YggSession state
        from lib.core_utils.ygg_session import YggSession

        YggSession._YggSession__dev_mode = False  # type: ignore
        YggSession._YggSession__dev_already_set = False  # type: ignore
        YggSession._YggSession__manual_submit = False  # type: ignore
        YggSession._YggSession__manual_already_set = False  # type: ignore

    # =====================================================
    # ARGUMENT PARSING TESTS
    # =====================================================

    def test_no_arguments_shows_help(self):
        """Test that running without arguments shows help and returns."""
        sys.argv = ["yggdrasil"]

        # Capture stdout to verify help is printed
        with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            main()

        # Verify help was printed (should contain usage information)
        stdout_content = mock_stdout.getvalue()
        self.assertIn("usage: yggdrasil", stdout_content)
        self.assertIn("daemon", stdout_content)
        self.assertIn("run-doc", stdout_content)

    def test_help_argument(self):
        """Test that --help shows help and exits."""
        sys.argv = ["yggdrasil", "--help"]

        with self.assertRaises(SystemExit) as context:
            with patch("sys.stdout", new_callable=StringIO):
                main()

        # argparse exits with code 0 for help
        self.assertEqual(context.exception.code, 0)

    def test_daemon_mode_arguments(self):
        """Test daemon mode argument parsing."""
        sys.argv = ["yggdrasil", "daemon"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("asyncio.run") as mock_asyncio_run,
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core_class.return_value = mock_core

            main()

            # Verify daemon mode was processed correctly
            mock_core.setup_realms.assert_called_once()
            mock_core.setup_watchers.assert_called_once()
            mock_asyncio_run.assert_called_once_with(mock_core.start())

    def test_daemon_mode_acquires_local_lock(self):
        """Test daemon mode acquires the local runtime lock before watchers start."""
        sys.argv = ["yggdrasil", "daemon"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.DaemonLock.acquire") as mock_acquire,
            patch("asyncio.run"),
        ):
            mock_loader = mock_config_loader.return_value
            mock_loader.load_config.return_value = self.mock_config
            mock_loader.loaded_path = Path("/tmp/main.json")
            mock_lock = MagicMock()
            mock_core = Mock()
            mock_core_class.return_value = mock_core

            def acquire_lock(*args, **kwargs):
                mock_core_class.assert_not_called()
                return mock_lock

            mock_acquire.side_effect = acquire_lock

            main()

            mock_acquire.assert_called_once_with(
                dev_mode=False,
                config_path=Path("/tmp/main.json"),
            )
            mock_lock.__enter__.assert_called_once()
            mock_core.setup_realms.assert_called_once()
            mock_core.setup_watchers.assert_called_once()

    def test_dev_daemon_mode_acquires_same_local_lock(self):
        """Test --dev daemon also acquires the coarse local runtime lock."""
        sys.argv = ["yggdrasil", "--dev", "daemon"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.DaemonLock.acquire") as mock_acquire,
            patch("asyncio.run"),
        ):
            mock_loader = mock_config_loader.return_value
            mock_loader.load_config.return_value = self.mock_config
            mock_loader.loaded_path = Path("/tmp/dev_main.json")
            mock_acquire.return_value = MagicMock()
            mock_core_class.return_value = Mock()

            main()

            mock_acquire.assert_called_once_with(
                dev_mode=True,
                config_path=Path("/tmp/dev_main.json"),
            )

    def test_daemon_lock_failure_exits_before_watcher_setup(self):
        """Test a held local lock exits before watchers are configured."""
        sys.argv = ["yggdrasil", "daemon"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.DaemonLock.acquire") as mock_acquire,
            patch("asyncio.run") as mock_asyncio_run,
        ):
            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_acquire.side_effect = DaemonLockError(
                Path("/tmp/yggdrasil/daemon.lock"),
                {"pid": 12345},
            )

            with self.assertRaises(SystemExit) as context:
                main()

            self.assertEqual(context.exception.code, 1)
            mock_core_class.assert_not_called()
            mock_asyncio_run.assert_not_called()

    def test_invalid_internal_storage_config_exits_cleanly(self):
        """Invalid internal_storage config exits 1 (no uncaught traceback)."""
        sys.argv = ["yggdrasil", "daemon"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.DaemonLock.acquire"),
            patch("asyncio.run"),
        ):
            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core_class.side_effect = InternalStorageConfigurationError(
                "internal_storage.backend='sqlite' is only supported in dev mode"
            )

            with self.assertRaises(SystemExit) as context:
                main()

            self.assertEqual(context.exception.code, 1)

    def test_daemon_watcher_config_error_exits_cleanly(self):
        """Watcher config errors are logged without starting the daemon."""
        sys.argv = ["yggdrasil", "daemon"]

        issue = WatcherConfigValidationIssue(
            kind="invalid_connection",
            realms=("dmx_realm",),
            backend="couchdb",
            connection="demux_sample_info_db",
            detail="connection is not configured",
        )
        error = WatcherConfigurationError(
            [issue],
            config_path=Path("/tmp/main.json"),
        )

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.DaemonLock.acquire") as mock_acquire,
            patch("yggdrasil.cli.custom_logger") as mock_custom_logger,
            patch("asyncio.run") as mock_asyncio_run,
        ):
            mock_loader = mock_config_loader.return_value
            mock_loader.load_config.return_value = self.mock_config
            mock_loader.loaded_path = Path("/tmp/main.json")
            mock_acquire.return_value = MagicMock()
            mock_logger = Mock()
            mock_custom_logger.return_value = mock_logger
            mock_core = Mock()
            mock_core.setup_watchers.side_effect = error
            mock_core_class.return_value = mock_core

            with self.assertRaises(SystemExit) as context:
                main()

            self.assertEqual(context.exception.code, 1)
            self.assertIsNone(context.exception.__cause__)
            mock_core_class.assert_called_once_with(
                self.mock_config,
                config_path=Path("/tmp/main.json"),
            )
            mock_core.setup_realms.assert_called_once()
            mock_core.setup_watchers.assert_called_once()
            mock_asyncio_run.assert_not_called()
            mock_logger.error.assert_called_once_with("%s", error)

    def test_daemon_external_system_unavailable_exits_cleanly(self):
        """Unreachable external systems are logged without a traceback."""
        sys.argv = ["yggdrasil", "daemon"]

        error = ExternalSystemUnavailableError(
            "CouchDB",
            "http://localhost:5984",
            hint="Check network/VPN connectivity.",
        )

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.DaemonLock.acquire") as mock_acquire,
            patch("yggdrasil.cli.custom_logger") as mock_custom_logger,
            patch("asyncio.run") as mock_asyncio_run,
        ):
            mock_loader = mock_config_loader.return_value
            mock_loader.load_config.return_value = self.mock_config
            mock_loader.loaded_path = Path("/tmp/dev_main.json")
            mock_acquire.return_value = MagicMock()
            mock_logger = Mock()
            mock_custom_logger.return_value = mock_logger
            # Simulate connection failure during core construction
            # (YggdrasilCore.__init__ -> OpsConsumerService -> OpsWriter)
            mock_core_class.side_effect = error

            with self.assertRaises(SystemExit) as context:
                main()

            self.assertEqual(context.exception.code, 1)
            self.assertIsNone(context.exception.__cause__)
            mock_asyncio_run.assert_not_called()
            # The actually-loaded config file is reported alongside the error
            mock_logger.error.assert_called_once_with(
                "%s (config file: %s)", error, Path("/tmp/dev_main.json")
            )

    def test_daemon_couchdb_failure_emits_one_error_record(self):
        """The CLI is the sole ERROR-level reporter for connection failures."""
        sys.argv = ["yggdrasil", "daemon"]

        def fail_during_core_construction(*args, **kwargs):
            return CouchDBClientFactory.create_client(
                url="http://localhost:5984",
                user_env="TEST_USER",
                pass_env="TEST_PASS",
            )

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.configure_logging"),
            patch(
                "yggdrasil.cli.YggdrasilCore",
                side_effect=fail_during_core_construction,
            ),
            patch("yggdrasil.cli.DaemonLock.acquire") as mock_acquire,
            patch("lib.couchdb.couchdb_connection.CouchDbSessionAuthenticator"),
            patch(
                "lib.couchdb.couchdb_connection.cloudant_v1.CloudantV1",
                side_effect=RequestsConnectionError("Connection refused"),
            ),
            patch.dict(
                os.environ,
                {"TEST_USER": "admin", "TEST_PASS": "secret"},
            ),
        ):
            mock_loader = mock_config_loader.return_value
            mock_loader.load_config.return_value = self.mock_config
            mock_loader.loaded_path = Path("/tmp/main.json")
            mock_acquire.return_value = MagicMock()

            with self.assertLogs(level="DEBUG") as captured:
                with self.assertRaises(SystemExit) as context:
                    main()

        self.assertEqual(context.exception.code, 1)
        error_records = [
            record for record in captured.records if record.levelno == logging.ERROR
        ]
        self.assertEqual(len(error_records), 1)
        self.assertEqual(error_records[0].name, "yggdrasil.cli")
        self.assertIn("Cannot reach CouchDB", error_records[0].getMessage())

        factory_records = [
            record
            for record in captured.records
            if record.name == "lib.couchdb.couchdb_connection"
            and "Failed to connect to CouchDB" in record.getMessage()
        ]
        self.assertEqual(len(factory_records), 1)
        self.assertEqual(factory_records[0].levelno, logging.DEBUG)

    def test_run_doc_external_system_unavailable_exits_cleanly(self):
        """run-doc mode gets the same clean handling as daemon mode."""
        sys.argv = ["yggdrasil", "run-doc", "DOC123"]

        error = ExternalSystemUnavailableError(
            "CouchDB",
            "http://localhost:5984",
        )

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.custom_logger") as mock_custom_logger,
        ):
            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_logger = Mock()
            mock_custom_logger.return_value = mock_logger
            mock_core_class.side_effect = error

            with self.assertRaises(SystemExit) as context:
                main()

            self.assertEqual(context.exception.code, 1)
            self.assertIsNone(context.exception.__cause__)
            mock_logger.error.assert_called_once_with("%s", error)

    def test_daemon_mode_with_dev_flag(self):
        """Test daemon mode with development flag."""
        sys.argv = ["yggdrasil", "--dev", "daemon"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession") as mock_session,
            patch("asyncio.run"),
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core_class.return_value = Mock()

            main()

            # Verify dev mode was set
            mock_session.init_dev_mode.assert_called_once_with(True)

    def test_run_doc_mode_arguments(self):
        """Test run-doc mode defaults to --plan-only and calls create_plan_from_doc."""
        sys.argv = ["yggdrasil", "run-doc", "test_doc_id"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession") as mock_session,
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.create_plan_from_doc.return_value = "pln_test_123"
            mock_core_class.return_value = mock_core

            main()

            # Verify run-doc mode was processed correctly (defaults to plan-only)
            mock_core.setup_realms.assert_called_once()
            mock_core.create_plan_from_doc.assert_called_once_with(
                doc_id="test_doc_id",
                force_overwrite=False,
            )
            mock_session.init_manual_submit.assert_called_once_with(False)

    def test_run_doc_mode_does_not_acquire_daemon_lock(self):
        """Test one-off run-doc mode does not take the daemon runtime lock."""
        sys.argv = ["yggdrasil", "run-doc", "test_doc_id", "--plan-only"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.DaemonLock.acquire") as mock_acquire,
        ):
            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.create_plan_from_doc.return_value = "pln_test_123"
            mock_core_class.return_value = mock_core

            main()

            mock_acquire.assert_not_called()
            mock_core.create_plan_from_doc.assert_called_once()

    def test_run_doc_mode_with_manual_submit(self):
        """Test run-doc mode with manual submit flag uses plan-only."""
        sys.argv = ["yggdrasil", "run-doc", "test_doc_id", "--manual-submit"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession") as mock_session,
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.create_plan_from_doc.return_value = "pln_test_123"
            mock_core_class.return_value = mock_core

            main()

            # Verify manual submit was set to True and plan-only used
            mock_session.init_manual_submit.assert_called_once_with(True)
            mock_core.create_plan_from_doc.assert_called_once_with(
                doc_id="test_doc_id",
                force_overwrite=False,
            )

    def test_run_doc_mode_short_manual_submit_flag(self):
        """Test run-doc mode with short -m flag defaults to plan-only."""
        sys.argv = ["yggdrasil", "run-doc", "test_doc_id", "-m"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession") as mock_session,
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.create_plan_from_doc.return_value = "pln_test_123"
            mock_core_class.return_value = mock_core

            main()

            # Verify manual submit was set to True
            mock_session.init_manual_submit.assert_called_once_with(True)
            mock_core.create_plan_from_doc.assert_called_once()

    def test_run_doc_missing_doc_id(self):
        """Test run-doc mode without document ID."""
        sys.argv = ["yggdrasil", "run-doc"]

        with self.assertRaises(SystemExit) as context:
            with patch("sys.stderr", new_callable=StringIO):
                main()

        # argparse should exit with error code for missing argument
        self.assertEqual(context.exception.code, 2)

    def test_invalid_mode(self):
        """Test invalid mode argument."""
        sys.argv = ["yggdrasil", "invalid_mode"]

        with self.assertRaises(SystemExit) as context:
            with patch("sys.stderr", new_callable=StringIO):
                main()

        # argparse should exit with error code for invalid choice
        self.assertEqual(context.exception.code, 2)

    def test_silent_flag_logging_configuration(self):
        """Test that --silent flag configures logging correctly."""
        sys.argv = ["yggdrasil", "--silent", "daemon"]

        with (
            patch("yggdrasil.cli.configure_logging") as mock_configure_logging,
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("asyncio.run"),
        ):
            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core_class.return_value = Mock()

            main()

            # Verify configure_logging was called with console=False for silent mode
            mock_configure_logging.assert_called_once_with(debug=False, console=False)

    def test_dev_flag_logging_configuration(self):
        """Test that --dev flag configures logging correctly."""
        sys.argv = ["yggdrasil", "--dev", "daemon"]

        with (
            patch("yggdrasil.cli.configure_logging") as mock_configure_logging,
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("asyncio.run"),
        ):
            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core_class.return_value = Mock()

            main()

            # Verify configure_logging was called with debug=True, console=True for dev mode
            mock_configure_logging.assert_called_once_with(debug=True, console=True)

    def test_normal_mode_logging_configuration(self):
        """Test that normal mode (no flags) configures logging correctly."""
        sys.argv = ["yggdrasil", "daemon"]

        with (
            patch("yggdrasil.cli.configure_logging") as mock_configure_logging,
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("asyncio.run"),
        ):
            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core_class.return_value = Mock()

            main()

            # Verify configure_logging was called with debug=False, console=True for normal mode
            mock_configure_logging.assert_called_once_with(debug=False, console=True)

    # =====================================================
    # CONFIGURATION AND INITIALIZATION TESTS
    # =====================================================

    def test_config_loading(self):
        """Test configuration loading process."""
        sys.argv = ["yggdrasil", "daemon"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("asyncio.run"),
        ):

            mock_config_loader_instance = Mock()
            mock_config_loader.return_value = mock_config_loader_instance
            mock_config_loader_instance.load_config.return_value = self.mock_config
            mock_core_class.return_value = Mock()

            main()

            # Verify config loading
            mock_config_loader.assert_called_once()
            mock_config_loader_instance.load_config.assert_called_once_with("main.json")
            mock_core_class.assert_called_once_with(self.mock_config)

    def test_yggdrasil_core_initialization(self):
        """Test YggdrasilCore initialization."""
        sys.argv = ["yggdrasil", "daemon"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("asyncio.run"),
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core_class.return_value = mock_core

            main()

            # Verify core initialization and setup
            mock_core_class.assert_called_once_with(self.mock_config)
            mock_core.setup_realms.assert_called_once()

    def test_session_initialization_order(self):
        """Test that YggSession is initialized before core setup."""
        sys.argv = ["yggdrasil", "--dev", "run-doc", "test_doc", "--manual-submit"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession") as mock_session,
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core_class.return_value = Mock()

            main()

            # Verify session is initialized before core operations
            expected_calls = [call.init_dev_mode(True), call.init_manual_submit(True)]
            mock_session.assert_has_calls(expected_calls)

    # =====================================================
    # DAEMON MODE TESTS
    # =====================================================

    def test_daemon_mode_full_flow(self):
        """Test complete daemon mode execution flow."""
        sys.argv = ["yggdrasil", "daemon"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("asyncio.run") as mock_asyncio_run,
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core_class.return_value = mock_core

            main()

            # Verify complete daemon setup flow
            mock_core.setup_realms.assert_called_once()
            mock_core.setup_watchers.assert_called_once()
            mock_asyncio_run.assert_called_once_with(mock_core.start())

    def test_daemon_mode_keyboard_interrupt(self):
        """Test daemon mode handling of KeyboardInterrupt."""
        sys.argv = ["yggdrasil", "daemon"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("asyncio.run") as mock_asyncio_run,
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core_class.return_value = mock_core

            # First call (start) raises KeyboardInterrupt, second call (stop) succeeds
            mock_asyncio_run.side_effect = [KeyboardInterrupt(), None]

            main()

            # Verify both start and stop were called
            expected_calls = [call(mock_core.start()), call(mock_core.stop())]
            mock_asyncio_run.assert_has_calls(expected_calls)

    # =====================================================
    # RUN-DOC MODE TESTS
    # =====================================================

    def test_run_doc_mode_full_flow(self):
        """Test complete run-doc mode execution flow with explicit --plan-only."""
        sys.argv = ["yggdrasil", "run-doc", "test_document_id", "--plan-only"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession") as mock_session,
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.create_plan_from_doc.return_value = "pln_test_123"
            mock_core_class.return_value = mock_core

            main()

            # Verify complete run-doc flow (plan-only mode)
            mock_session.init_manual_submit.assert_called_once_with(False)
            mock_core.setup_realms.assert_called_once()
            mock_core.create_plan_from_doc.assert_called_once_with(
                doc_id="test_document_id",
                force_overwrite=False,
            )

            # Verify watchers are NOT set up in run-doc mode
            mock_core.setup_watchers.assert_not_called()

    def test_run_doc_with_special_characters_in_doc_id(self):
        """Test run-doc mode with special characters in document ID."""
        special_doc_id = "test-doc_id.123@domain"
        sys.argv = ["yggdrasil", "run-doc", special_doc_id]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession"),
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.create_plan_from_doc.return_value = "pln_test_123"
            mock_core_class.return_value = mock_core

            main()

            # Verify special characters are passed correctly
            mock_core.create_plan_from_doc.assert_called_once_with(
                doc_id=special_doc_id,
                force_overwrite=False,
            )

    # =====================================================
    # ERROR HANDLING TESTS
    # =====================================================

    def test_config_loading_error(self):
        """Test error handling when config loading fails."""
        sys.argv = ["yggdrasil", "daemon"]

        with patch("yggdrasil.cli.ConfigLoader") as mock_config_loader:
            mock_config_loader.return_value.load_config.side_effect = Exception(
                "Config load failed"
            )

            with self.assertRaises(Exception) as context:
                main()

            self.assertIn("Config load failed", str(context.exception))

    def test_yggdrasil_core_initialization_error(self):
        """Test error handling when YggdrasilCore initialization fails."""
        sys.argv = ["yggdrasil", "daemon"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core_class.side_effect = Exception("Core init failed")

            with self.assertRaises(Exception) as context:
                main()

            self.assertIn("Core init failed", str(context.exception))

    def test_session_already_set_error(self):
        """Test error handling when YggSession is already set."""
        sys.argv = ["yggdrasil", "--dev", "daemon"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession") as mock_session,
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core_class.return_value = Mock()
            mock_session.init_dev_mode.side_effect = RuntimeError(
                "Dev mode already set"
            )

            with self.assertRaises(RuntimeError) as context:
                main()

            self.assertIn("Dev mode already set", str(context.exception))

    def test_asyncio_error_in_daemon_mode(self):
        """Test error handling when asyncio operations fail in daemon mode."""
        sys.argv = ["yggdrasil", "daemon"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("asyncio.run") as mock_asyncio_run,
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core_class.return_value = Mock()
            mock_asyncio_run.side_effect = Exception("Asyncio error")

            with self.assertRaises(Exception) as context:
                main()

            self.assertIn("Asyncio error", str(context.exception))

    # =====================================================
    # LOGGING AND DEBUG TESTS
    # =====================================================

    def test_logging_configuration(self):
        """Test that logging configuration happens correctly during main() execution."""
        sys.argv = ["yggdrasil", "daemon"]

        with (
            patch("yggdrasil.cli.configure_logging") as mock_configure_logging,
            patch("yggdrasil.cli.custom_logger") as mock_custom_logger,
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("asyncio.run"),
        ):
            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core_class.return_value = Mock()
            mock_logger = Mock()
            mock_custom_logger.return_value = mock_logger

            main()

            # Verify that logging was configured and logger was created
            mock_configure_logging.assert_called_once_with(debug=False, console=True)
            mock_custom_logger.assert_called_once_with("yggdrasil.cli")
            # Verify that debug logging was called (indicates logging is working)
            mock_logger.debug.assert_called_once_with("Yggdrasil: Starting up...")

    def test_dev_mode_affects_session_only(self):
        """Test that --dev flag only affects YggSession, not other components directly."""
        sys.argv = ["yggdrasil", "--dev", "daemon"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession") as mock_session,
            patch("asyncio.run"),
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core_class.return_value = Mock()

            main()

            # Verify only the session is affected by dev mode
            mock_session.init_dev_mode.assert_called_once_with(True)
            # ConfigLoader and YggdrasilCore should be called the same way regardless
            mock_config_loader.return_value.load_config.assert_called_once_with(
                "main.json"
            )

    # =====================================================
    # INTEGRATION AND EDGE CASE TESTS
    # =====================================================

    def test_all_components_integration(self):
        """Test integration between all CLI components."""
        sys.argv = ["yggdrasil", "--dev", "run-doc", "integration_test_doc", "-m"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession") as mock_session,
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.create_plan_from_doc.return_value = "pln_integration_123"
            mock_core_class.return_value = mock_core

            main()

            # Verify complete integration flow
            mock_session.init_dev_mode.assert_called_once_with(True)
            mock_config_loader.return_value.load_config.assert_called_once_with(
                "main.json"
            )
            mock_core_class.assert_called_once_with(self.mock_config)
            mock_core.setup_realms.assert_called_once()
            mock_session.init_manual_submit.assert_called_once_with(True)
            mock_core.create_plan_from_doc.assert_called_once_with(
                doc_id="integration_test_doc",
                force_overwrite=False,
            )

    def test_minimal_daemon_invocation(self):
        """Test minimal daemon mode invocation."""
        sys.argv = ["yggdrasil", "daemon"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession") as mock_session,
            patch("asyncio.run"),
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core_class.return_value = Mock()

            main()

            # Verify minimal setup works
            mock_session.init_dev_mode.assert_called_once_with(
                False
            )  # Default dev=False

    def test_minimal_run_doc_invocation(self):
        """Test minimal run-doc mode invocation defaults to plan-only."""
        sys.argv = ["yggdrasil", "run-doc", "minimal_doc"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession") as mock_session,
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.create_plan_from_doc.return_value = "pln_minimal_123"
            mock_core_class.return_value = mock_core

            main()

            # Verify minimal setup works (defaults to plan-only)
            mock_session.init_dev_mode.assert_called_once_with(False)
            mock_session.init_manual_submit.assert_called_once_with(False)
            mock_core.create_plan_from_doc.assert_called_once()

    def test_command_line_order_independence(self):
        """Test that global flags must come before subcommands (argparse behavior)."""
        # Test that --dev must come before the subcommand (this is argparse's expected behavior)
        valid_order = ["yggdrasil", "--dev", "run-doc", "test_doc", "--manual-submit"]
        invalid_orders = [
            ["yggdrasil", "run-doc", "--dev", "test_doc", "--manual-submit"],
            ["yggdrasil", "run-doc", "test_doc", "--dev", "--manual-submit"],
            ["yggdrasil", "run-doc", "test_doc", "--manual-submit", "--dev"],
        ]

        # Test valid order works
        sys.argv = valid_order
        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession") as mock_session,
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.create_plan_from_doc.return_value = "pln_test_123"
            mock_core_class.return_value = mock_core

            main()

            # Verify the valid order works
            mock_session.init_dev_mode.assert_called_with(True)
            mock_session.init_manual_submit.assert_called_with(True)

        # Test that invalid orders fail (this is expected argparse behavior)
        for argv in invalid_orders:
            with self.subTest(argv=argv):
                sys.argv = argv

                with self.assertRaises(SystemExit) as context:
                    with patch("sys.stderr", new_callable=StringIO):
                        main()

                # argparse should exit with error code 2 for invalid argument order
                self.assertEqual(context.exception.code, 2)

                # Reset for next iteration
                self.setUp()

    # =====================================================
    # ARGUMENT VALIDATION TESTS
    # =====================================================

    def test_empty_doc_id(self):
        """Test run-doc mode with empty document ID."""
        sys.argv = ["yggdrasil", "run-doc", ""]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession"),
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.create_plan_from_doc.return_value = "pln_empty_123"
            mock_core_class.return_value = mock_core

            main()

            # Even empty string should be passed through
            mock_core.create_plan_from_doc.assert_called_once_with(
                doc_id="",
                force_overwrite=False,
            )

    def test_very_long_doc_id(self):
        """Test run-doc mode with very long document ID."""
        long_doc_id = "a" * 1000  # Very long document ID
        sys.argv = ["yggdrasil", "run-doc", long_doc_id]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession"),
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.create_plan_from_doc.return_value = "pln_long_123"
            mock_core_class.return_value = mock_core

            main()

            # Long document ID should be handled
            mock_core.create_plan_from_doc.assert_called_once_with(
                doc_id=long_doc_id,
                force_overwrite=False,
            )

    def test_daemon_mode_with_manual_submit_error(self):
        """Test daemon mode doesn't have manual_submit attribute (defensive code coverage)."""
        # Note: The manual_submit check in daemon mode (line 70) is defensive code that
        # cannot be reached in practice because argparse only defines --manual-submit
        # for the run-doc subcommand. The getattr(args, "manual_submit", False) will
        # always return False for daemon mode since the attribute doesn't exist.
        # This test verifies the normal daemon behavior (no manual_submit attribute).

        sys.argv = ["yggdrasil", "daemon"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("asyncio.run"),
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core_class.return_value = mock_core

            main()

            # Verify daemon mode works normally (defensive check doesn't trigger)
            mock_core.setup_realms.assert_called_once()
            mock_core.setup_watchers.assert_called_once()

    def test_main_module_execution(self):
        """Test that main can be imported and called (covers entry point functionality)."""
        # Note: The if __name__ == '__main__' guard (line 90) is only executed when
        # running the script directly, not during import or exec. This test verifies
        # that the main function works correctly when called directly, which is the
        # same code path that would be triggered by the __main__ guard.

        sys.argv = ["yggdrasil", "daemon"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("asyncio.run"),
        ):

            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core_class.return_value = Mock()

            # Call main() directly to verify it works (same as __main__ guard would do)
            main()

            # Verify main() executed correctly
            mock_config_loader.assert_called_once()
            mock_core_class.assert_called_once()


class TestRunDocModeFlags(unittest.TestCase):
    """
    Tests for the new run-doc mode flags introduced in Phase 3:
    - --plan-only (-p): Create plan only, no execution
    - --run-once (-r): Create and execute plan via scoped watcher
    - --force (-f): Overwrite existing plan without confirmation
    """

    def setUp(self):
        """Set up test fixtures and reset global state."""
        from lib.core_utils.ygg_session import YggSession

        YggSession._YggSession__dev_mode = False  # type: ignore
        YggSession._YggSession__dev_already_set = False  # type: ignore
        YggSession._YggSession__manual_submit = False  # type: ignore
        YggSession._YggSession__manual_already_set = False  # type: ignore

        self.original_argv = sys.argv.copy()
        self.mock_config = {"test": "config"}

    def tearDown(self):
        """Clean up after each test."""
        sys.argv = self.original_argv
        from lib.core_utils.ygg_session import YggSession

        YggSession._YggSession__dev_mode = False  # type: ignore
        YggSession._YggSession__dev_already_set = False  # type: ignore
        YggSession._YggSession__manual_submit = False  # type: ignore
        YggSession._YggSession__manual_already_set = False  # type: ignore

    def test_run_doc_without_mode_defaults_to_plan_only(self):
        """Test that run-doc without mode flag defaults to --plan-only."""
        sys.argv = ["yggdrasil", "run-doc", "P12345"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession"),
        ):
            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.create_plan_from_doc.return_value = "pln_test_123"
            mock_core_class.return_value = mock_core

            main()

            # Should call create_plan_from_doc (plan-only mode)
            mock_core.create_plan_from_doc.assert_called_once_with(
                doc_id="P12345",
                force_overwrite=False,
            )
            # Should NOT call run_once_with_watcher
            mock_core.run_once_with_watcher.assert_not_called()

    def test_run_doc_plan_only_flag(self):
        """Test explicit --plan-only flag."""
        sys.argv = ["yggdrasil", "run-doc", "P12345", "--plan-only"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession"),
        ):
            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.create_plan_from_doc.return_value = "pln_test_123"
            mock_core_class.return_value = mock_core

            main()

            mock_core.create_plan_from_doc.assert_called_once_with(
                doc_id="P12345",
                force_overwrite=False,
            )

    def test_run_doc_plan_only_short_flag(self):
        """Test short -p flag for plan-only mode."""
        sys.argv = ["yggdrasil", "run-doc", "P12345", "-p"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession"),
        ):
            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.create_plan_from_doc.return_value = "pln_test_123"
            mock_core_class.return_value = mock_core

            main()

            mock_core.create_plan_from_doc.assert_called_once()

    def test_run_doc_run_once_flag(self):
        """Test --run-once flag calls run_once_with_watcher."""
        sys.argv = ["yggdrasil", "run-doc", "P12345", "--run-once"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession"),
        ):
            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.run_once_with_watcher.return_value = 0  # Success exit code
            mock_core_class.return_value = mock_core

            # run_once mode raises SystemExit with the return code
            with self.assertRaises(SystemExit) as context:
                main()
            self.assertEqual(context.exception.code, 0)

            # Should call run_once_with_watcher with timeout_seconds
            mock_core.run_once_with_watcher.assert_called_once_with(
                doc_id="P12345",
                force_overwrite=False,
                timeout_seconds=1800,  # Default timeout
            )
            # Should NOT call create_plan_from_doc
            mock_core.create_plan_from_doc.assert_not_called()

    def test_run_doc_run_once_failure_exits_nonzero(self):
        """A failed run-once execution's exit code reaches the process unchanged.

        run_once_with_watcher returns 1 when a plan's execution did not
        succeed, including a continue_independent plan that finished its
        healthy branches but failed overall.
        """
        sys.argv = ["yggdrasil", "run-doc", "P12345", "--run-once"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession"),
        ):
            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.run_once_with_watcher.return_value = 1
            mock_core_class.return_value = mock_core

            with self.assertRaises(SystemExit) as context:
                main()
            self.assertEqual(context.exception.code, 1)

    def test_run_doc_run_once_short_flag(self):
        """Test short -r flag for run-once mode."""
        sys.argv = ["yggdrasil", "run-doc", "P12345", "-r"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession"),
        ):
            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.run_once_with_watcher.return_value = 0  # Success exit code
            mock_core_class.return_value = mock_core

            # run_once mode raises SystemExit with the return code
            with self.assertRaises(SystemExit) as context:
                main()
            self.assertEqual(context.exception.code, 0)

            mock_core.run_once_with_watcher.assert_called_once()

    def test_run_doc_plan_only_and_run_once_mutually_exclusive(self):
        """Test that --plan-only and --run-once are mutually exclusive."""
        sys.argv = ["yggdrasil", "run-doc", "P12345", "--plan-only", "--run-once"]

        with self.assertRaises(SystemExit) as context:
            with patch("sys.stderr", new_callable=StringIO):
                main()

        # argparse exits with code 2 for mutually exclusive violations
        self.assertEqual(context.exception.code, 2)

    def test_run_doc_force_flag_with_plan_only(self):
        """Test --force flag is passed to create_plan_from_doc."""
        sys.argv = ["yggdrasil", "run-doc", "P12345", "--plan-only", "--force"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession"),
        ):
            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.create_plan_from_doc.return_value = "pln_test_123"
            mock_core_class.return_value = mock_core

            main()

            mock_core.create_plan_from_doc.assert_called_once_with(
                doc_id="P12345",
                force_overwrite=True,
            )

    def test_run_doc_force_flag_with_run_once(self):
        """Test --force flag is passed to run_once_with_watcher."""
        sys.argv = ["yggdrasil", "run-doc", "P12345", "--run-once", "--force"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession"),
        ):
            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.run_once_with_watcher.return_value = 0  # Success exit code
            mock_core_class.return_value = mock_core

            # run_once mode raises SystemExit with the return code
            with self.assertRaises(SystemExit) as context:
                main()
            self.assertEqual(context.exception.code, 0)

            mock_core.run_once_with_watcher.assert_called_once_with(
                doc_id="P12345",
                force_overwrite=True,
                timeout_seconds=1800,  # Default timeout
            )

    def test_run_doc_force_short_flag(self):
        """Test short -f flag for force overwrite."""
        sys.argv = ["yggdrasil", "run-doc", "P12345", "-p", "-f"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession"),
        ):
            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.create_plan_from_doc.return_value = "pln_test_123"
            mock_core_class.return_value = mock_core

            main()

            mock_core.create_plan_from_doc.assert_called_once_with(
                doc_id="P12345",
                force_overwrite=True,
            )

    def test_run_doc_plan_creation_failure_exits_with_error(self):
        """Test that plan creation failure (returns None) exits with code 1."""
        sys.argv = ["yggdrasil", "run-doc", "P12345", "--plan-only"]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession"),
        ):
            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.create_plan_from_doc.return_value = None  # Failure case
            mock_core_class.return_value = mock_core

            with self.assertRaises(SystemExit) as context:
                main()

            self.assertEqual(context.exception.code, 1)

    def test_run_doc_all_flags_combined(self):
        """Test combining multiple flags: --plan-only --force --manual-submit."""
        sys.argv = [
            "yggdrasil",
            "run-doc",
            "P12345",
            "--plan-only",
            "--force",
            "--manual-submit",
        ]

        with (
            patch("yggdrasil.cli.ConfigLoader") as mock_config_loader,
            patch("yggdrasil.cli.YggdrasilCore") as mock_core_class,
            patch("yggdrasil.cli.YggSession") as mock_session,
        ):
            mock_config_loader.return_value.load_config.return_value = self.mock_config
            mock_core = Mock()
            mock_core.create_plan_from_doc.return_value = "pln_test_123"
            mock_core_class.return_value = mock_core

            main()

            mock_session.init_manual_submit.assert_called_once_with(True)
            mock_core.create_plan_from_doc.assert_called_once_with(
                doc_id="P12345",
                force_overwrite=True,
            )


if __name__ == "__main__":
    unittest.main()
