import unittest

from tools.run_style_examples import is_process_launch_block


class ProcessLaunchBlockTests(unittest.TestCase):
    def test_windows_createfile_denial_from_nested_runtime_is_blocked(self):
        output = (
            "CreateFile failed 5 (Acceso denegado)\n"
            "ProcessException: Acceso denegado\n"
        )
        self.assertTrue(is_process_launch_block(output))

    def test_unrelated_nonzero_program_output_remains_a_failure(self):
        self.assertFalse(is_process_launch_block("assertion failed: access denied"))
        self.assertFalse(is_process_launch_block("compiler error: invalid syntax"))


if __name__ == "__main__":
    unittest.main()
