defmodule SecantClient.ReadmeTest do
  use ExUnit.Case, async: true

  # The README is the landing page on hex.pm and hexdocs, so its install
  # snippet has to name a requirement that matches the version being released.
  test "the install snippet matches the mix.exs version" do
    version = Version.parse!(Mix.Project.config()[:version])
    snippet = ~s({:secant_client, "~> #{version.major}.#{version.minor}.0"})

    assert File.read!("README.md") =~ snippet,
           "README.md should tell users to install #{snippet}. " <>
             "Update the Installation section after bumping @version in mix.exs."
  end
end
