import shlex
import sys
import threading
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Optional

from dagster import (
    OpExecutionContext,
    _check as check,
)
from dagster._annotations import public
from dagster._core.definitions.resource_annotation import TreatAsResourceParam
from dagster._core.errors import DagsterExecutionInterruptedError, DagsterPipesExecutionError
from dagster._core.execution.context.asset_execution_context import AssetExecutionContext
from dagster._core.pipes.client import (
    PipesClient,
    PipesClientCompletedInvocation,
    PipesContextInjector,
    PipesMessageReader,
)
from dagster._core.pipes.context import PipesMessageHandler
from dagster._core.pipes.utils import (
    PipesEnvContextInjector,
    extract_message_or_forward_to_stdout,
    open_pipes_session,
)
from dagster_pipes import PipesDefaultMessageWriter, PipesExtras, PipesParams

if TYPE_CHECKING:
    from tenki_sandbox import Client
    from tenki_sandbox.process import Process


class PipesTenkiMessageReader(PipesMessageReader):
    """Message reader that extracts Dagster Pipes messages from the stdout stream of a
    command running inside a Tenki sandbox.

    The external process is instructed to write Pipes messages to stdout; this reader
    consumes the streaming ``proc.stdout`` iterator, extracts the messages, and forwards
    any non-message lines to the orchestration process's stdout. The sandbox process's
    stderr stream is forwarded to stdout in the background so it is not lost.
    """

    def __init__(self):
        self._handler: PipesMessageHandler | None = None

    @contextmanager
    def read_messages(
        self,
        handler: PipesMessageHandler,
    ) -> Iterator[PipesParams]:
        self._handler = handler
        try:
            # instruct the external process to write Pipes messages to stdout
            yield {PipesDefaultMessageWriter.STDIO_KEY: PipesDefaultMessageWriter.STDOUT}
        finally:
            self._handler = None

    def consume_sandbox_logs(self, proc: "Process") -> None:
        handler = check.not_none(
            self._handler, "Can only consume logs within context manager scope."
        )

        # Forward the sandbox process's stderr concurrently so diagnostics are not lost
        # while we block reading messages from stdout.
        stderr_thread = threading.Thread(target=self._forward_stderr, args=(proc,), daemon=True)
        stderr_thread.start()

        # ``proc.stdout`` yields raw byte chunks that are not guaranteed to align on line
        # boundaries, so we buffer and split on newlines before handing complete lines to
        # the message extractor.
        buffer = ""
        for chunk in proc.stdout:
            buffer += _to_text(chunk)
            lines = buffer.split("\n")
            buffer = lines.pop()
            for line in lines:
                extract_message_or_forward_to_stdout(handler, line)
        if buffer:
            extract_message_or_forward_to_stdout(handler, buffer)

        stderr_thread.join()

    def _forward_stderr(self, proc: "Process") -> None:
        try:
            for chunk in proc.stderr:
                sys.stdout.write(_to_text(chunk))
        except Exception:
            # stderr forwarding is best-effort; failures here should not mask the primary
            # error surfaced from stdout consumption or ``proc.wait()``.
            pass

    def no_messages_debug_text(self) -> str:
        return "Attempted to read messages by extracting them from the Tenki sandbox stdout stream."


def _to_text(chunk: bytes | bytearray | str) -> str:
    if isinstance(chunk, (bytes, bytearray)):
        return chunk.decode("utf-8", errors="replace")
    return chunk


def _as_argv(command: str | Sequence[str]) -> list[str]:
    if isinstance(command, str):
        return shlex.split(command)
    return [str(part) for part in command]


class PipesTenkiClient(PipesClient, TreatAsResourceParam):
    """A pipes client that runs external processes inside a Tenki sandbox (an isolated
    remote cloud VM).

    By default context is injected via environment variables and messages are parsed out
    of the sandbox process's stdout stream, with other logs forwarded to the stdout of the
    orchestration process.

    Args:
        client (Optional[tenki_sandbox.Client]): An optional Tenki client to use to create
            sandboxes. If not provided, a client is constructed from the ambient
            ``TENKI_AUTH_TOKEN`` / ``TENKI_API_KEY`` environment variables when a sandbox
            is created.
        env (Optional[Mapping[str, str]]): An optional dict of environment variables to
            pass to the sandbox.
        context_injector (Optional[PipesContextInjector]): A context injector to use to
            inject context into the sandbox process. Defaults to
            :py:class:`PipesEnvContextInjector`.
        message_reader (Optional[PipesMessageReader]): A message reader to use to read
            messages from the sandbox process. Defaults to
            :py:class:`PipesTenkiMessageReader`.
        forward_termination (bool): Whether to terminate the Tenki sandbox if the
            orchestration process is interrupted or canceled. Defaults to True.
    """

    def __init__(
        self,
        client: Optional["Client"] = None,
        env: Mapping[str, str] | None = None,
        context_injector: PipesContextInjector | None = None,
        message_reader: PipesMessageReader | None = None,
        forward_termination: bool = True,
    ):
        self._client = client
        self.env = check.opt_mapping_param(env, "env", key_type=str, value_type=str)
        self.context_injector = (
            check.opt_inst_param(
                context_injector,
                "context_injector",
                PipesContextInjector,
            )
            or PipesEnvContextInjector()
        )
        self.message_reader = (
            check.opt_inst_param(message_reader, "message_reader", PipesMessageReader)
            or PipesTenkiMessageReader()
        )
        self.forward_termination = check.bool_param(forward_termination, "forward_termination")

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return True

    @public
    def run(  # ty: ignore[invalid-method-override]
        self,
        *,
        context: OpExecutionContext | AssetExecutionContext,
        command: str | Sequence[str],
        extras: PipesExtras | None = None,
        env: Mapping[str, str] | None = None,
        sandbox_kwargs: Mapping[str, Any] | None = None,
    ) -> PipesClientCompletedInvocation:
        """Create a Tenki sandbox and run a command in it to completion, enriched with the
        pipes protocol.

        Args:
            context (Union[OpExecutionContext, AssetExecutionContext]): The context from the
                executing op or asset.
            command (Union[str, Sequence[str]]): The command to run in the sandbox.
            extras (Optional[PipesExtras]): Extra values to pass along as part of the ext
                protocol.
            env (Optional[Mapping[str, str]]): A mapping of environment variable names to
                values to set in the sandbox, on top of those configured on the resource.
            sandbox_kwargs (Optional[Mapping[str, Any]]): Additional keyword arguments to
                forward to ``Sandbox.create`` (e.g. ``cpu_cores``, ``memory_mb``,
                ``allow_outbound``).

        Returns:
            PipesClientCompletedInvocation: Wrapper containing results reported by the
                external process.
        """
        with open_pipes_session(
            context=context,
            context_injector=self.context_injector,
            message_reader=self.message_reader,
            extras=extras,
        ) as pipes_session:
            sandbox_env = {
                **self.env,
                **(env or {}),
                **pipes_session.get_bootstrap_env_vars(),
            }
            sb = self._create_sandbox(env=sandbox_env, sandbox_kwargs=sandbox_kwargs)
            session_id = sb.id
            try:
                proc = sb.start(*_as_argv(command))
                # We provide no stdin; close it so a command that reads stdin does not hang.
                proc.close_stdin()

                if isinstance(self.message_reader, PipesTenkiMessageReader):
                    self.message_reader.consume_sandbox_logs(proc)

                result = proc.wait()
                if not result.ok:
                    raise DagsterPipesExecutionError(
                        f"Tenki sandbox command failed (exit_code={result.exit_code}, "
                        f"signal={result.signal})."
                    )
                sb.close_if_open()
            except DagsterExecutionInterruptedError:
                if self.forward_termination:
                    context.log.info("[pipes] execution interrupted, terminating Tenki sandbox.")
                    sb.close_if_open()
                raise
            except BaseException:
                sb.close_if_open()
                raise
        return PipesClientCompletedInvocation(
            pipes_session, metadata={"tenki_session_id": session_id}
        )

    def _create_sandbox(
        self,
        *,
        env: Mapping[str, str],
        sandbox_kwargs: Mapping[str, Any] | None,
    ):
        kwargs = dict(sandbox_kwargs or {})
        kwargs_env = dict(kwargs.pop("env", {}) or {})
        create_kwargs: dict[str, Any] = {
            "env": {**kwargs_env, **env},
            **kwargs,
        }

        if self._client is not None:
            return self._client.create(**create_kwargs)

        from tenki_sandbox import Sandbox

        return Sandbox.create(**create_kwargs)
