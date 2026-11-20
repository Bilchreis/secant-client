# SecantClient

[![Hex.pm](https://img.shields.io/hexpm/v/secant_client.svg)](https://hex.pm/packages/secant_client)
[![Docs](https://img.shields.io/badge/hex-docs-blue.svg)](https://hexdocs.pm/secant_client)

Client for [SECoP](https://github.com/SampleEnvironment/SECoP) (Sample
Environment Communication Protocol) nodes.

## Installation

Add `secant_client` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:secant_client, "~> 0.4.0"}
  ]
end
```

secant_client is a library without an application of its own. Start
`SecantClient.Supervisor` in your supervision tree:

```elixir
children = [
  SecantClient.Supervisor
]
```

Documentation is at <https://hexdocs.pm/secant_client>.

## Development

Run `mix precommit` before committing. It compiles with warnings as errors,
drops unused entries from `mix.lock`, formats, runs Credo's warning checks and
the tests. CI runs the same checks in check-only mode.

## Releasing

Bump `@version` in `mix.exs` and merge to `main`. On a new minor or major
version, update the requirement in the Installation section too; a test fails
until the two agree. The release workflow then publishes the package and its
docs to hex.pm and creates the `v<version>` tag and GitHub release. Pushes that
don't change the version release nothing.

## License

secant_client is free software, licensed under the GNU Lesser General Public
License, version 3 or (at your option) any later version. See `LICENSE` for the
LGPL and `LICENSE.GPL-3.0` for the GNU General Public License it builds on.
