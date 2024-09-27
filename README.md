# [C-SQL] SQL Database from Scratch in Pure C

**C-SQL** is a lightweight database system implemented in pure C. This system supports basic SQL operations such as inserting, updating, deleting, and selecting rows from tables. It uses a file-based storage system with paging to manage data across disk efficiently.

## Features
- Supports basic SQL-like operations:
  - **INSERT**: Add a new row to the table.
  - **UPDATE**: Modify existing rows based on criteria.
  - **DELETE**: Remove rows from the table.
  - **SELECT**: Retrieve data from the table.
- File-based data persistence with a **paging mechanism** to handle memory and storage efficiently.
- No virtualization and external dependencies, written in pure C for portability, efficiency, and simplicity.
- Error handling for unrecognized commands, syntax errors, negative IDs, table full scenarios, etc.

## Paging Mechanism
C-SQL implements a paging mechanism to manage the memory efficiently. Each table is divided into pages, where each page has a fixed size. The **PAGE_SIZE** is defined as `4096 bytes`, and the table can consist of up to `100 pages` (controlled by `TABLE_MAX_PAGES`).

### Key Points:
- **Pages**: Each page stores multiple rows of the table. Data is organized into these fixed-size pages for efficient storage and retrieval.
- **Memory Allocation**: Instead of loading the entire table into memory, MiniDB only loads the necessary pages as required. This reduces memory usage and ensures scalability as the table grows.
- **File-based Storage**: When a page is full, additional data is stored in subsequent pages, and when the table reaches its maximum page limit (100 pages), no more rows can be inserted, and the system returns an `EXECUTE_TABLE_FULL` error.
  
This paging mechanism mimics how larger, more advanced database systems manage their data by breaking it into smaller, manageable chunks (pages).

## Compilation
To compile the program, run the following command in the terminal:
```bash
make cleans
make build
```

## Usage
After compilation, you can execute the program to interact with the MiniDB system.

Example:
```bash
make run
```

Supported commands and their format:
1. **Insert**: `insert into <table-name> values (<column1>, <column2>, ...)`
2. **Update**: `update <table-name> set <column1> = <value> where <column2> <operator> <value>`
3. **Delete**: `delete from <table-name> where <column> <operator> <value>`
4. **Select**: `select < * | column1 [, column2, ...] > from <table-name> [where <column> <operator> <value>]`

To exit the program, type `.exit`.

In order to add or modify the schema of the database, we can modify the `db.schema` file. The schema file contains the table definitions, including the table name, column names, and data types. The file format is as follows:
```
<num-tables>
<table1-name>;<table1-num-columns>;<column1-name>:<column1-size>:<column1-type>,<column2-name>:<column2-size>:<column2-type>,...
<table2-name>;<table2-num-columns>;<column1-name>:<column1-size>:<column1-type>,<column2-name>:<column2-size>:<column2-type>,...
...
```

## Tests
The system includes a test suite to validate the core functionality of the database system. The test cases can be found in the `test.py` file. To run the tests, execute the following command:
```bash
make test
```

The test suite covers various scenarios for inserting, updating, deleting, and selecting rows from tables. It ensures that the system behaves as expected and handles different types of queries correctly.

## Error Handling
The system provides robust error handling several error cases, such ass:
- **Unrecognized Command**: If an invalid or unsupported command is entered.
- **Syntax Errors**: If the user enters a malformed SQL-like command.
- **Negative IDs**: Prevents insertion of negative IDs for rows.
- **Table Full**: If the maximum number of pages is reached, no further rows can be inserted, and the system returns an appropriate error.

## Future Improvements
- **Support for Transactions**: Introduce transaction management to support atomic operations, rollback, and commit functionality. This will ensure the system can handle multiple operations securely and reliably.
  
- **Indexing**: Implement indexing mechanisms to improve the performance of search and retrieval (especially for `SELECT` operations). Indexes will speed up query execution by reducing the number of rows/pages scanned.

- **Support for Joins and Foreign Key**: Although this system support for multiple tables, this system doesn't yet support for joins and foreign key. Extend the system to support foreign key and SQL joins (e.g., inner join, outer join) to enable querying across multiple tables.

- **Query Optimization**: Add query optimization techniques to improve the execution time for complex queries. This could involve caching frequently accessed data or reusing pre-computed results.

- **B-Tree Paging**: Implement a B-Tree paging mechanism for more efficient data storage and retrieval. B-Trees are commonly used in databases to maintain sorted data and optimize search operations.

- **More Data Types**: Currently, the system supports basic types like `INTEGER`, `VARCHAR`, and `REAL`. In the future, the system can be expanded to support additional data types such as `DATE`, `BLOB`, and arrays.
  
- **Concurrency Support**: Introduce locking mechanisms or multi-threaded processing to allow concurrent reads and writes, which is vital for real-world use cases in multi-user environments.

## Supports
This project is open-source and available under the MIT License. Feel free to contribute, report issues, or suggest improvements. Your feedback is highly appreciated!

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgements
- This project is modified from and inspired by the [Let's Build a Simple Database](https://cstack.github.io/db_tutorial/) tutorial by [cstack](https://github.com/cstack)

