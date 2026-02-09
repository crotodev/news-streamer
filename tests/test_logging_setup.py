import logging

from logging_setup import configure_logging


def _clear_root_handlers():
    root = logging.getLogger()
    handlers = list(root.handlers)
    for handler in handlers:
        root.removeHandler(handler)
    return handlers


def test_configure_logging_sets_level_and_is_idempotent(monkeypatch):
    _clear_root_handlers()
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    configure_logging()

    root = logging.getLogger()
    assert root.level == logging.DEBUG
    assert len(root.handlers) == 1

    configure_logging("WARNING")
    assert root.level == logging.WARNING
    assert len(root.handlers) == 1

    _clear_root_handlers()
