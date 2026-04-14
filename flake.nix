{
  description = "Local tooling for scraping and ranking SUUMO used-mansion listings";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { self, nixpkgs }:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
      pythonEnv = pkgs.python313.withPackages (ps: [
        ps.requests
        ps.beautifulsoup4
      ]);
    in {
      devShells.${system}.default = pkgs.mkShell {
        packages = [
          pythonEnv
          pkgs.sqlite
        ];
      };
    };
}
