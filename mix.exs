defmodule SecantClient.MixProject do
  use Mix.Project

  # Bumping this is the release: once it lands on main, the release workflow
  # publishes to hex.pm and creates the v<version> tag and GitHub release.
  @version "0.4.0"
  @source_url "https://github.com/Bilchreis/secant-client"

  def project do
    [
      app: :secant_client,
      version: @version,
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      aliases: aliases(),
      deps: deps(),
      description: "Client for SECoP (Sample Environment Communication Protocol) nodes.",
      package: package(),
      docs: docs()
    ]
  end

  def cli do
    [preferred_envs: [precommit: :test]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  #
  # No application callback: secant_client is a library. Host applications
  # start `SecantClient.Supervisor` in their own supervision tree.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:jason, "~> 1.4"},
      {:ecto, "~> 3.12"},
      {:map_diff, "~> 1.3"},
      {:phoenix_pubsub, "~> 2.0"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:mox, "~> 1.0", only: :test},
      {:ex_doc, "~> 0.34", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["LGPL-3.0-or-later"],
      links: %{"GitHub" => @source_url}
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md"],
      source_url: @source_url,
      source_ref: "v#{@version}"
    ]
  end

  defp aliases do
    [
      # Run before committing. CI runs the same checks in check-only mode.
      precommit: [
        "compile --warnings-as-errors",
        "deps.unlock --unused",
        "format",
        "credo --only warning --strict",
        "test"
      ]
    ]
  end
end
