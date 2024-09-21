import unittest
import subprocess
import os

class TestDatabase(unittest.TestCase):
    def setUp(self):
        if os.path.exists("test.db"):
            os.remove("test.db")

    def run_script(self, commands):
        process = subprocess.Popen(
            ['./main', 'test.db'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        input_commands = '\n'.join(commands)
        stdout, _ = process.communicate(input=input_commands)
        
        return stdout.strip().split('\n')

    def test_insert_and_retrieve_row(self):
        result = self.run_script([
            "insert 1 user1 person1@example.com",
            "select",
            ".exit\n",
        ])
        
        expected_output = [
            "db > Executed.",
            "db > (1, user1, person1@example.com)",
            "Executed.",
            "db >",
        ]
        
        self.assertEqual(result, expected_output)

    def test_table_full_error(self):
        script = [f"insert {i} user{i} person{i}@example.com" for i in range(1, 1402)]
        script.append(".exit\n")
        result = self.run_script(script)
        self.assertEqual(result[-2], 'db > Error: Table full.')

    def test_max_length_strings(self):
        long_username = "a" * 32
        long_email = "a" * 255
        script = [
            f"insert 1 {long_username} {long_email}",
            "select",
            ".exit\n",
        ]
        result = self.run_script(script)
        expected_output = [
            "db > Executed.",
            f"db > (1, {long_username}, {long_email})",
            "Executed.",
            "db >",
        ]
        self.assertEqual(result, expected_output)

    def test_strings_too_long_error(self):
        long_username = "a" * 33
        long_email = "a" * 256
        script = [
            f"insert 1 {long_username} {long_email}",
            "select",
            ".exit\n",
        ]
        result = self.run_script(script)
        expected_output = [
            "db > String is too long.",
            "db > Executed.",
            "db >",
        ]
        self.assertEqual(result, expected_output)

    def test_negative_id_error(self):
        script = [
            "insert -1 cstack foo@bar.com",
            "select",
            ".exit\n",
        ]
        result = self.run_script(script)
        expected_output = [
            "db > ID must be positive.",
            "db > Executed.",
            "db >",
        ]
        self.assertEqual(result, expected_output)

    def test_persistent_data(self):
        script1 = [
            "insert 1 user1 person1@example.com",
            ".exit\n",
        ]
        result1 = self.run_script(script1)
        expected_output = [
            "db > Executed.",
            "db >",
        ]
        self.assertEqual(result1, expected_output)

        script2 = [
            "select",
            ".exit\n",
        ]
        result2 = self.run_script(script2)
        expected_output = [
            "db > (1, user1, person1@example.com)",
            "Executed.",
            "db >",
        ]
        self.assertEqual(result2, expected_output)

if __name__ == '__main__':
    unittest.main()

