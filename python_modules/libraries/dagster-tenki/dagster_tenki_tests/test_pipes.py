import os
import subprocess
import sys
import tempfile

import pytest
from dagster import AssetExecutionContext, asset, materialize
from dagster._core.errors import DagsterExecutionInterruptedError, DagsterPipesExecutionError
from dagster._core.pipes.client import PipesMessageReader
from dagster_pipes import PipesDefaultMessageWriter, PipesParams
from dagster_tenki.pipes import PipesTenkiClient

# External scripts run inside the (fake) sandbox. They depend only on `dagster-pipes`,
# read the Pipes context from environment variables, and write messages to stdout.

_MATERIALIZE_SCRIPT = """
from dagster_pipes import open_dagster_pipes

with open_dagster_pipes() as pipes:
    pipes.log.info("hello from the tenki sandbox")
    print("plain user stdout line")
    pipes.report_asset_materialization(metadata={"row_count": 100, "foo": "bar"})
"""

_FAILURE_SCRIPT = """
import sys

from dagster_pipes import open_dagster_pipes

with open_dagster_pipes() as pipes:
    pipes.report_asset_materialization(metadata={"row_count": 1})

sys.exit(3)
"""


class _FakeResult:
    def __init__(self, exit_code: int):
        self.exit_code = exit_code
        self.signal = None

    @property
    def ok(self) -> bool:
        return self.exit_code == 0 and not self.signal


class _FakeProcess:
    """Runs the command as a local subprocess with the injected Pipes env vars, exposing
    the same ``stdout``/``stderr`` streaming + ``wait()`` surface as ``tenki_sandbox.Process``.
    """

    def __init__(self, argv, env):
        self._popen = subprocess.Popen(
            list(argv),
            env={**os.environ, **env},
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # Popen pipes are iterable line-by-line, yielding bytes, matching ProcessStream.
        self.stdout = self._popen.stdout
        self.stderr = self._popen.stderr

    def close_stdin(self) -> None:
        if self._popen.stdin:
            self._popen.stdin.close()

    def wait(self, timeout=None) -> _FakeResult:
        return _FakeResult(self._popen.wait(timeout=timeout))


class _FakeFS:
    """Stands in for ``sb.fs``; writes to the local filesystem so the injected
    ``dagster_pipes`` source is genuinely importable by the local subprocess.
    """

    def __init__(self):
        self.writes = {}

    def mkdir(self, path, *, recursive=True, mode=0o755):
        os.makedirs(path, exist_ok=recursive)

    def write_bytes(self, path, data):
        self.writes[path] = data
        with open(path, "wb") as f:
            f.write(data)


class _FakeSandbox:
    def __init__(self, env):
        self._env = env
        self.fs = _FakeFS()
        self.closed = False

    @property
    def id(self) -> str:
        return "sandbox-abc123"

    def start(self, *argv):
        return _FakeProcess(argv, self._env)

    def close_if_open(self) -> None:
        self.closed = True

    def close(self) -> None:
        self.closed = True


class _FakeClient:
    def __init__(self):
        self.last_sandbox = None

    def create(self, *, env, **kwargs):
        self.create_kwargs = kwargs
        self.last_sandbox = _FakeSandbox(env)
        return self.last_sandbox


def test_pipes_tenki_client_materialization():
    fake_client = _FakeClient()

    with tempfile.TemporaryDirectory() as source_dir:

        @asset
        def my_asset(context: AssetExecutionContext, tenki_pipes: PipesTenkiClient):
            return tenki_pipes.run(
                context=context,
                command=[sys.executable, "-c", _MATERIALIZE_SCRIPT],
                sandbox_kwargs={"cpu_cores": 2},
            ).get_materialize_result()

        result = materialize(
            [my_asset],
            resources={
                "tenki_pipes": PipesTenkiClient(client=fake_client, pipes_source_dir=source_dir)
            },
            raise_on_error=False,
        )

        assert result.success
        mats = result.asset_materializations_for_node(my_asset.op.name)
        assert len(mats) == 1
        metadata = mats[0].metadata
        # metadata reported by the external process surfaces on the materialization
        assert metadata["row_count"].value == 100
        assert metadata["foo"].value == "bar"
        # completion metadata attached by the client surfaces on every materialization
        assert metadata["tenki_session_id"].value == "sandbox-abc123"
        # sandbox_kwargs are forwarded to Sandbox.create
        assert fake_client.create_kwargs["cpu_cores"] == 2
        assert fake_client.last_sandbox.closed is True
        # dagster_pipes source was injected into the sandbox and put on PYTHONPATH
        assert os.path.exists(os.path.join(source_dir, "dagster_pipes", "__init__.py"))
        assert fake_client.last_sandbox._env["PYTHONPATH"].startswith(source_dir)  # noqa: SLF001


def test_pipes_tenki_client_inject_disabled():
    fake_client = _FakeClient()

    with tempfile.TemporaryDirectory() as source_dir:

        @asset
        def my_asset(context: AssetExecutionContext, tenki_pipes: PipesTenkiClient):
            return tenki_pipes.run(
                context=context,
                command=[sys.executable, "-c", _MATERIALIZE_SCRIPT],
            ).get_materialize_result()

        result = materialize(
            [my_asset],
            resources={
                "tenki_pipes": PipesTenkiClient(
                    client=fake_client,
                    inject_pipes_source=False,
                    pipes_source_dir=source_dir,
                )
            },
            raise_on_error=False,
        )

        assert result.success
        # nothing was written to the sandbox and PYTHONPATH was left untouched
        assert fake_client.last_sandbox.fs.writes == {}
        assert "PYTHONPATH" not in fake_client.last_sandbox._env  # noqa: SLF001


def test_pipes_tenki_client_nonzero_exit_raises():
    fake_client = _FakeClient()

    @asset
    def failing_asset(context: AssetExecutionContext, tenki_pipes: PipesTenkiClient):
        return tenki_pipes.run(
            context=context,
            command=[sys.executable, "-c", _FAILURE_SCRIPT],
        ).get_materialize_result()

    result = materialize(
        [failing_asset],
        resources={"tenki_pipes": PipesTenkiClient(client=fake_client)},
        raise_on_error=False,
    )

    assert not result.success
    # sandbox is still torn down on failure
    assert fake_client.last_sandbox.closed is True


def test_pipes_tenki_client_raises_on_error():
    fake_client = _FakeClient()

    @asset
    def failing_asset(context: AssetExecutionContext, tenki_pipes: PipesTenkiClient):
        return tenki_pipes.run(
            context=context,
            command=[sys.executable, "-c", _FAILURE_SCRIPT],
        ).get_materialize_result()

    with pytest.raises(DagsterPipesExecutionError, match="Tenki sandbox command failed"):
        materialize(
            [failing_asset],
            resources={"tenki_pipes": PipesTenkiClient(client=fake_client)},
            raise_on_error=True,
        )


_HANG_SCRIPT = """
import time
from dagster_pipes import open_dagster_pipes

with open_dagster_pipes() as pipes:
    time.sleep(60)
"""


class _InterruptingSandbox:
    """A sandbox whose start() returns a process that immediately raises
    DagsterExecutionInterruptedError when its stdout is consumed, simulating
    an orchestrator cancellation.
    """

    def __init__(self, env):
        self._env = env
        self.fs = _FakeFS()
        self.closed = False

    @property
    def id(self) -> str:
        return "sandbox-interrupted"

    def start(self, *argv):
        return _InterruptingProcess()

    def close_if_open(self) -> None:
        self.closed = True

    def close(self) -> None:
        self.closed = True


class _InterruptingProcess:
    """A fake process whose stdout iteration raises DagsterExecutionInterruptedError."""

    def __init__(self):
        self.stdout = self._raise_on_iter()
        self.stderr = iter([])

    def close_stdin(self) -> None:
        pass

    def _raise_on_iter(self):
        raise DagsterExecutionInterruptedError()
        yield  # make this a generator

    def wait(self, timeout=None) -> _FakeResult:
        return _FakeResult(0)


class _InterruptingClient:
    def __init__(self):
        self.last_sandbox = None

    def create(self, *, env, **kwargs):
        self.last_sandbox = _InterruptingSandbox(env)
        return self.last_sandbox


def test_pipes_tenki_client_forward_termination():
    """When forward_termination=True (default), the sandbox is closed on interruption."""
    fake_client = _InterruptingClient()

    @asset
    def my_asset(context: AssetExecutionContext, tenki_pipes: PipesTenkiClient):
        return tenki_pipes.run(
            context=context,
            command=["python", "-c", _HANG_SCRIPT],
        ).get_materialize_result()

    result = materialize(
        [my_asset],
        resources={
            "tenki_pipes": PipesTenkiClient(
                client=fake_client, forward_termination=True, inject_pipes_source=False
            )
        },
        raise_on_error=False,
    )

    assert not result.success
    assert fake_client.last_sandbox.closed is True


def test_pipes_tenki_client_custom_message_reader():
    """When a non-PipesTenkiMessageReader is used, consume_sandbox_logs is skipped."""
    fake_client = _FakeClient()

    from collections.abc import Iterator
    from contextlib import contextmanager

    from dagster._core.pipes.context import PipesMessageHandler

    class _NoOpMessageReader(PipesMessageReader):
        """A minimal PipesMessageReader that is NOT a PipesTenkiMessageReader."""

        @contextmanager
        def read_messages(self, handler: PipesMessageHandler) -> Iterator[PipesParams]:
            yield {PipesDefaultMessageWriter.STDIO_KEY: PipesDefaultMessageWriter.STDOUT}

        def no_messages_debug_text(self) -> str:
            return "no-op"

    _SIMPLE_SCRIPT = """
import sys
print("hello from custom reader test")
"""

    @asset
    def my_asset(context: AssetExecutionContext, tenki_pipes: PipesTenkiClient):
        return tenki_pipes.run(
            context=context,
            command=[sys.executable, "-c", _SIMPLE_SCRIPT],
        ).get_materialize_result()

    result = materialize(
        [my_asset],
        resources={
            "tenki_pipes": PipesTenkiClient(
                client=fake_client,
                message_reader=_NoOpMessageReader(),
                inject_pipes_source=False,
            )
        },
        raise_on_error=False,
    )

    # The run completes successfully — the isinstance guard skips consume_sandbox_logs
    # but the process still runs and the exit code is checked.
    assert result.success
    assert fake_client.last_sandbox.closed is True
