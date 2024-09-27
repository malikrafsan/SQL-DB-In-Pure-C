import unittest
import subprocess
import os

class TestDatabase(unittest.TestCase):
    def setUp(self):
        # remove .table files inside data directory
        for file in os.listdir('data'):
            if file.endswith('.table'):
                os.remove(f'data/{file}')

    def run_script(self, commands):
        process = subprocess.Popen(
            ['./main', 'db.schema'],
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
            "insert into users values (1, user1, person1@example.com)",
            "select * from users",
            ".exit\n",
        ])
        
        expected_output = [
            "db > Executed.",
            "db > (1, user1, person1@example.com)",
            "Executed.",
            "db >",
        ]
        
        self.assertEqual(result, expected_output)

    def test_insert_and_select_some_columns(self):
        result = self.run_script([
            "insert into users values (1, user1, person1@example.com)",
            "select id, username from users",
            ".exit\n",
        ])
        
        expected_output = [
            "db > Executed.",
            "db > (1, user1)",
            "Executed.",
            "db >",
        ]
        
        self.assertEqual(result, expected_output)
    
    def test_insert_multiple_rows(self):
        script = [
            "insert into users values (1, user1, person1@example.com)",
            "insert into users values (2, user2, person2@example.com)",
            "select * from users",
            ".exit\n",
        ]

        result = self.run_script(script)
        expected_output = [
            "db > Executed.",
            "db > Executed.",
            "db > (1, user1, person1@example.com)",
            "(2, user2, person2@example.com)",
            "Executed.",
            "db >",
        ]

        self.assertEqual(result, expected_output)
    
    def test_select_with_where_clause(self):
        script = [
            "insert into users values (1, user1, person1@example.com)",
            "insert into users values (2, user2, person2@example.com)",
            "select * from users where username = 'user2'",
            ".exit\n",
        ]

        result = self.run_script(script)
        expected_output = [
            "db > Executed.",
            "db > Executed.",
            "db > (2, user2, person2@example.com)",
            "Executed.",
            "db >",
        ]

        self.assertEqual(result, expected_output)

    def test_select_with_where_clause_no_match(self):
        script = [
            "insert into users values (1, user1, person1@example.com)",
            "insert into users values (2, user2, person2@example.com)",
            "select * from users where id = 3",
            ".exit\n",
        ]

        result = self.run_script(script)

        expected_output = [
            "db > Executed.",
            "db > Executed.",
            "db > Executed.",
            "db >",
        ]

        self.assertEqual(result, expected_output)

    def test_table_full_error(self):
        script = [f"insert into users values ({i}, user{i}, person{i}@example.com)" for i in range(1, 1402)]
        script.append(".exit\n")
        result = self.run_script(script)
        self.assertEqual(result[-2], 'db > Error: Table full.')

    def test_max_length_strings(self):
        long_username = "a" * 32
        long_email = "a" * 255
        script = [
            f"insert into users values (1, {long_username}, {long_email})",
            "select * from users",
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
            f"insert into users values (1, {long_username}, {long_email})",
            "select * from users",
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
            "insert into users values (-1, cstack, foo@bar.com)",
            "select * from users",
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
            "insert into users values (1, user1, person1@example.com)",
            ".exit\n",
        ]
        result1 = self.run_script(script1)
        expected_output = [
            "db > Executed.",
            "db >",
        ]
        self.assertEqual(result1, expected_output)

        script2 = [
            "select * from users",
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

