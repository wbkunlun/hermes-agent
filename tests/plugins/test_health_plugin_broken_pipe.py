"""B6 (fork 2026-11-13, DPMS 561213): a health prober that hangs up before the response is
written is not a health failure. Production 2026-11-11 logged two BrokenPipeError stacks —
the only ERROR-level lines of the whole three-day window — for a routine probe disconnect.
"""
import logging

from plugins.health.server import make_handler


class _GonePipe:
    """A socket the client already closed: every write raises BrokenPipeError."""

    def write(self, _data):
        raise BrokenPipeError(32, "Broken pipe")

    def flush(self):
        pass


def _handler_instance(logger):
    handler_cls = make_handler(build_detail=lambda: {"status": "ok", "checked_at": "now"},
                               logger=logger)
    handler = handler_cls.__new__(handler_cls)
    handler.wfile = _GonePipe()
    handler.requestline = "GET /health HTTP/1.1"
    handler.command = "GET"
    handler.path = "/health"
    handler.request_version = "HTTP/1.1"
    handler.client_address = ("127.0.0.1", 42274)
    handler._headers_buffer = []
    handler.close_connection = True
    return handler


def test_a_disconnected_prober_raises_nothing_and_stays_out_of_the_error_log(caplog):
    logger = logging.getLogger("hermes_plugins.health")
    handler = _handler_instance(logger)

    with caplog.at_level(logging.DEBUG, logger="hermes_plugins.health"):
        handler._respond(200, {"status": "ok"})

    assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert any("prober disconnected" in r.getMessage() for r in caplog.records)


def test_a_payload_build_failure_is_still_reported_as_an_error(caplog):
    logger = logging.getLogger("hermes_plugins.health")
    handler_cls = make_handler(
        build_detail=lambda: (_ for _ in ()).throw(RuntimeError("boom")),
        logger=logger,
    )
    handler = handler_cls.__new__(handler_cls)
    handler.wfile = _GonePipe()
    handler.requestline = "GET /health HTTP/1.1"
    handler.command = "GET"
    handler.path = "/health"
    handler.request_version = "HTTP/1.1"
    handler.client_address = ("127.0.0.1", 42274)
    handler._headers_buffer = []
    handler.close_connection = True

    with caplog.at_level(logging.DEBUG, logger="hermes_plugins.health"):
        handler._dispatch()

    assert any("payload build failed" in r.getMessage() for r in caplog.records)
    assert [r for r in caplog.records if r.levelno >= logging.ERROR]
