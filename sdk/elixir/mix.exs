defmodule Dilock.MixProject do
  use Mix.Project

  def project do
    [
      app: :dilock,
      version: "0.1.0",
      elixir: "~> 1.18",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Distributed leases for Redis and etcd",
      package: [
        licenses: ["Apache-2.0"],
        links: %{"Hex.pm" => "https://hex.pm/packages/dilock"},
        files: ~w(lib priv proto config mix.exs README.md LICENSE)
      ],
      docs: [main: "Dilock"]
    ]
  end

  def application, do: [extra_applications: [:logger, :crypto]]

  defp deps do
    [
      {:redix, "~> 1.5.2"},
      {:grpc, "== 1.0.3"},
      # grpc's Mint adapter is optional upstream; select it explicitly for etcd.
      {:mint, "~> 1.9"},
      {:protobuf, "== 0.17.0"},
      {:jason, "~> 1.4.4"},
      {:ex_doc, "~> 0.38", only: :dev, runtime: false}
    ]
  end
end
