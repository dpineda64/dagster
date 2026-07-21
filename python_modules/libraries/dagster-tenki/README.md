# dagster-tenki

A Dagster integration for running assets and ops inside a [Tenki](https://tenki.sh)
Sandbox (an isolated remote cloud VM) via Dagster Pipes.

`PipesTenkiClient` launches an external command in a Tenki Sandbox from within an
asset/op body. Only the tiny, dependency-free `dagster-pipes` package needs to be
available inside the sandbox. Pipes context flows in via environment variables and
results (materializations, checks, logs, metadata) stream back over the sandbox
process's stdout.
