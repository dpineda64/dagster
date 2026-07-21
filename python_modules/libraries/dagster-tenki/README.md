# dagster-tenki

A Dagster integration for running assets and ops inside a [Tenki](https://tenki.sh)
Sandbox (an isolated remote cloud VM) via Dagster Pipes.

`PipesTenkiClient` launches an external command in a Tenki Sandbox from within an
asset/op body. Pipes context flows in via environment variables and results
(materializations, checks, logs, metadata) stream back over the sandbox process's
stdout.

By default the client vendors the orchestrator's own `dagster_pipes` source into the
sandbox (over `sb.fs`) and puts it on `PYTHONPATH`, so your command can
`import dagster_pipes` with nothing pre-installed — no custom image, snapshot, or
network access required. Pass `inject_pipes_source=False` if your image/snapshot
already provides `dagster-pipes`.
