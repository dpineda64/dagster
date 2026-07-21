import os
import subprocess
import sys

import pytest
from dagster import AssetExecutionContext, asset, materialize
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
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        # Popen pipes are iterable line-by-line, yielding bytes, matching ProcessStream.
        self.stdout = self._popen.stdout
        self.stderr = self._popen.stderr

    def close_stdin(self) -> None:
        pass

    def wait(self, timeout=None) -> _FakeResult:
        return _FakeResult(self._popen.wait(timeout=timeout))


class _FakeSandbox:
    def __init__(self, env):
        self._env = env
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

    @asset
    def my_asset(context: AssetExecutionContext, tenki_pipes: PipesTenkiClient):
        return tenki_pipes.run(
            context=context,
            command=[sys.executable, "-c", _MATERIALIZE_SCRIPT],
            sandbox_kwargs={"cpu_cores": 2},
        ).get_materialize_result()

    result = materialize(
        [my_asset],
        resources={"tenki_pipes": PipesTenkiClient(client=fake_client)},
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

    with pytest.raises(Exception, match="Tenki sandbox command failed"):
        materialize(
            [failing_asset],
            resources={"tenki_pipes": PipesTenkiClient(client=fake_client)},
            raise_on_error=True,
        )
