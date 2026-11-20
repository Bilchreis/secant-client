# SecantClient

**TODO: Add description**

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `secant_client` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:secant_client, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/secant_client>.

## Development

Run `mix precommit` before committing. It compiles with warnings as errors,
drops unused entries from `mix.lock`, formats, runs Credo's warning checks and
the tests. CI runs the same checks in check-only mode.

## Releasing

Bump `@version` in `mix.exs` and merge to `main`. The release workflow then
publishes the package and its docs to hex.pm and creates the `v<version>` tag
and GitHub release. Pushes that don't change the version release nothing.

## License

secant_client is free software, licensed under the GNU Lesser General Public
License, version 3 or (at your option) any later version. See `LICENSE` for the
LGPL and `LICENSE.GPL-3.0` for the GNU General Public License it builds on.
