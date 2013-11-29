defmodule Ecto.Mixfile do
  use Mix.Project

  def project do
    [ app: :ecto,
      version: "0.0.1",
      deps: deps(Mix.env),
      env: envs,
      name: "Ecto",
      elixir: "~> 0.11.2-dev",
      source_url: "https://github.com/elixir-lang/ecto",
      docs: fn -> [
        source_ref: System.cmd("git rev-parse --verify --quiet HEAD"),
        main: "overview",
        readme: true ]
      end ]
  end

  def application do
    []
  end

  defp deps(:prod) do
    [ { :poolboy, github: "devinus/poolboy" },
      { :postgrex, "~> 0.2.0", github: "ericmj/postgrex", optional: true },
     
      ## Riak dependencies
      { :ej, github: "seth/ej", optional: true },
      { :iso8601, github: "seansawyer/erlang_iso8601", compile: "rebar compile", optional: true },
      { :jiffy, github: "davisp/jiffy", optional: true },
      { :pooler, github: "seth/pooler" , optional: true },
      { :riakc, github: "basho/riak-erlang-client", optional: true } ]
  end

  defp deps(_) do
    deps(:prod) ++
      [ { :ex_doc, github: "elixir-lang/ex_doc" } ]
  end

  defp envs do
    [ pg: [ test_paths: ["integration_test/pg"] ],
      riak: [ test_paths: ["integration_test/riak"] ],
      all: [ test_paths: ["test", "integration_test/pg"] ] ]
  end
end
