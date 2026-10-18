# Python package consumers

`tools/package_consumers.py` builds the wheel and source archive from this SDK,
installs each exact archive with all Redis, etcd and PostgreSQL optional backend extras into separate new
virtual environments, then runs `artifact_smoke.py` from outside the checkout.
The smoke test checks the installed path, a published identity vector, public
options and provider constructors, and packaged protocol/runtime resources. It
does not contact Redis, etcd or PostgreSQL; real backend behavior is covered by
the SDK and cross-process test suites.

Run from the repository root with a Python environment containing the pinned
Hatchling build backend and a local wheelhouse containing the exact runtime
wheels pinned by `pyproject.toml`:

```sh
python tools/package_consumers.py --sdk python \
  --build-python C:/toolchains/dilock-build/python.exe \
  --wheelhouse C:/caches/dilock-wheelhouse \
  --output validation/implementation/python-package-consumer.json
```

`--build-python` and `--wheelhouse` can instead be configured with
`DILOCK_PYTHON_BUILD_PYTHON` and `DILOCK_PYTHON_WHEELHOUSE`. The repository-local
`.tools` paths are defaults for this checkout only. The wheelhouse is mandatory,
and pip remains in no-index mode; a missing wheel fails the consumer check.

The report contains the wheel and sdist SHA-256 values, retained consumer
workspace path, and every executed command's exit status. Existing report paths
are never overwritten, and a non-empty consumer workspace is preserved and
rejected. The driver does not contact package indexes. It installs the exact
wheel into a clean Python 3.14 environment, then installs the exact sdist with
pinned Hatchling available for the source build and the declared runtime extras
installed in that consumer environment. Both smoke runs execute outside the
checkout. Other SDKs have separate checked-in artifact consumer drivers, while
Go remains blocked pending a verified owner-qualified module identity.
