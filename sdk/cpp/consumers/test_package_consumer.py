from pathlib import Path

import pytest

from package_consumer import (
    ConsumerError,
    consumer_configure_args,
    sdk_configure_args,
    toolchain_cmake_args,
    validate_toolchain_file,
)


def test_toolchain_file_is_forwarded_to_both_cmake_configure_phases() -> None:
    toolchain = Path(__file__).resolve()

    resolved = validate_toolchain_file(str(toolchain))
    expected = f"-DCMAKE_TOOLCHAIN_FILE={toolchain}"
    sdk_args = sdk_configure_args("cmake", Path("sdk"), Path("build"), Path("stage"), resolved)
    consumer_args = consumer_configure_args(
        "cmake", Path("consumer"), Path("consumer-build"), Path("prefix"), resolved,
    )

    assert resolved == toolchain
    assert sdk_args.count(expected) == 1
    assert consumer_args.count(expected) == 1


def test_no_toolchain_preserves_default_cmake_arguments() -> None:
    assert toolchain_cmake_args(None) == []
    assert "-DCMAKE_TOOLCHAIN_FILE" not in sdk_configure_args(
        "cmake", Path("sdk"), Path("build"), Path("stage"), None,
    )
    assert "-DCMAKE_TOOLCHAIN_FILE" not in consumer_configure_args(
        "cmake", Path("consumer"), Path("consumer-build"), Path("prefix"), None,
    )


def test_missing_toolchain_file_is_rejected() -> None:
    missing = Path(__file__).resolve().with_name("missing-toolchain-file.cmake")
    with pytest.raises(ConsumerError, match="does not exist"):
        validate_toolchain_file(str(missing))


def test_toolchain_directory_is_rejected() -> None:
    directory = Path(__file__).resolve().parent
    with pytest.raises(ConsumerError, match="not a file"):
        validate_toolchain_file(str(directory))
