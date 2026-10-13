version = "0.1.0"
author = "Dilock contributors"
description = "Experimental Nim binding over the local Dilock C ABI"
license = "Apache-2.0"
srcDir = "src"
requires "nim >= 2.2.12"

task test, "Run ABI binding contract tests":
  exec "nim c -r --path:src --passC:-I../../.local/dilock-c/include --passL:-L../../.local/dilock-c/lib --passL:-ldilock_c tests/test_dilock.nim"
