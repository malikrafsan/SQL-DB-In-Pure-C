#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

#define DIR_PREFIX "data/"

#define TABLE_MAX_PAGES 100

#define MAX_LINE_LENGTH 1024
#define MAX_NAME_LENGTH 256

const uint32_t PAGE_SIZE = 4096;

typedef enum {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
  PREPARE_SUCCESS,
  PREPARE_NEGATIVE_ID,
  PREPARE_STRING_TOO_LONG,
  PREPARE_SYNTAX_ERROR,
  PREPARE_UNRECOGNIZED_STATEMENT,
  PREPARE_TABLE_NOT_FOUND,
  PREPARE_INTERNAL_ERROR
} PrepareResult;

typedef enum { EXECUTE_SUCCESS, EXECUTE_TABLE_FULL } ExecuteResult;

typedef enum { 
  STATEMENT_INSERT,
  STATEMENT_UPDATE,
  STATEMENT_DELETE,
  STATEMENT_SELECT
} StatementType;

typedef enum { INTEGER, VARCHAR, REAL } ColumnType;

typedef enum {
  OP_EQUAL,
  OP_NOT_EQUAL,
  OP_GREATER_THAN,
  OP_LESS_THAN,
  OP_GREATER_THAN_OR_EQUAL,
  OP_LESS_THAN_OR_EQUAL
} Operator;

typedef struct {
  char* data;
  int length;
} Bytes;

typedef struct {
  char* name;
  uint32_t size;
  uint32_t offset;
  ColumnType type;
} ColumnDefinition;

typedef struct {
  void* pages[TABLE_MAX_PAGES];
  int file_descriptor;
  uint32_t file_length;
} Pager;

typedef struct {
  ColumnDefinition* columns;
  char* filename;
  char* table_name;
  Pager* pager;

  uint32_t num_rows;
  uint32_t num_columns;
  uint32_t row_size;
  uint32_t rows_per_page;
  uint32_t max_rows;
} Table;

typedef struct {
  Table* tables;
  uint32_t num_tables;
} Schema;

typedef struct {
  Table* table;
  uint32_t row_num;
  bool end_of_table;
} Cursor;

typedef Bytes Row;

typedef struct {
  char* buffer;
  ssize_t input_length;
  size_t buffer_length;
} InputBuffer;

typedef struct {
  ColumnDefinition* column;
  Operator op;
  Bytes value;
} WhereClause;

typedef struct {
  Row row_to_insert;
} InsertStatement;

typedef struct {
  ColumnDefinition* column;
  Bytes value;
  WhereClause* where_clause;
} UpdateStatement;

typedef struct {
  WhereClause* where_clause;
} DeleteStatement;

typedef struct {
  ColumnDefinition* columns;
  uint32_t num_columns;
  bool is_select_all;
  WhereClause* where_clause;
} SelectStatement;

typedef struct {
  Table* table;
  void* statementDetail;
  StatementType type;
} Statement;

char* str_to_lower(const char* str) {
  char* lower = strdup(str);
  for (char* p = lower; *p; p++) {
    *p = tolower(*p);
  }
  return lower;
}

void trim(char* str) {
  char* start = str;
  char* end = str + strlen(str) - 1;

  while (isspace(*start)) start++;
  while (end > start && isspace(*end)) end--;

  *(end + 1) = '\0';
  memmove(str, start, end - start + 2);
}

ColumnType string_to_column_type(const char* str) {
  if (strcmp(str, "int") == 0) return INTEGER;
  if (strcmp(str, "varchar") == 0) return VARCHAR;
  if (strcmp(str, "real") == 0) return REAL;
  fprintf(stderr, "Unknown column type: %s\n", str);
  exit(1);
}

void free_table(Table* table) {
  if (!table) {
    return;
  }

  free(table->table_name);
  free(table->filename);
  if (!table->columns) {
    return;
  }

  for (uint32_t j = 0; j < table->num_columns; j++) {
    free(table->columns[j].name);
  }
  free(table->columns);
}

void free_schema(Schema* schema) {
  if (!schema) {
    return;
  }

  if (schema->tables) {
    for (uint32_t i = 0; i < schema->num_tables; i++) {
      free_table(&schema->tables[i]);
    }
    free(schema->tables);
  }
  free(schema);
}

Pager* pager_open(const char* filename) {
  int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
  if (fd < 0) {
    printf("Unable to open file\n");
    exit(EXIT_FAILURE);
  }

  off_t file_length = lseek(fd, 0, SEEK_END);

  Pager* pager = malloc(sizeof(Pager));
  pager->file_descriptor = fd;
  pager->file_length = file_length;

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    pager->pages[i] = NULL;
  }

  return pager;
}

void pager_flush(Pager* pager, uint32_t page_num, uint32_t size) {
  if (pager->pages[page_num] == NULL) {
    printf("Tried to flush null page\n");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
  if (offset < 0) {
    printf("Error seeking: %d\n", errno);
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written =
      write(pager->file_descriptor, pager->pages[page_num], size);
  if (bytes_written < 0) {
    printf("Error writing: %d\n", errno);
    exit(EXIT_FAILURE);
  }
}

Schema* schema_open(const char* filename) {
  FILE* file = fopen(filename, "r");
  if (file == NULL) {
    printf("Error opening schema file\n");
  }

  Schema* schema = malloc(sizeof(Schema));
  if (schema == NULL) {
    printf("Memory allocation error\n");
    fclose(file);
    return NULL;
  }

  schema->tables = NULL;
  schema->num_tables = 0;

  char line[MAX_LINE_LENGTH];
  if (fgets(line, sizeof(line), file) == NULL) {
    printf("Error reading number of tables\n");
    fclose(file);
    free_schema(schema);
    return NULL;
  }

  schema->num_tables = atoi(line);
  schema->tables = malloc(schema->num_tables * sizeof(Table));
  if (schema->tables == NULL) {
    printf("Memory allocation error\n");
    fclose(file);
    free_schema(schema);
    return NULL;
  }

  for (uint32_t i = 0; i < schema->num_tables; i++) {
    schema->tables[i].table_name = NULL;
    schema->tables[i].columns = NULL;
    schema->tables[i].num_columns = 0;
  }

  for (uint32_t i = 0; i < schema->num_tables; i++) {
    if (fgets(line, sizeof(line), file) == NULL) {
      printf("Error reading table definition\n");
      fclose(file);
      free_schema(schema);
      return NULL;
    }

    size_t len = strlen(line);
    if (len > 0 && line[len - 1] == '\n') {
      line[len - 1] = '\0';
    }

    Table* table = &schema->tables[i];
    char* token;

    token = strtok(line, ";");
    table->table_name = strdup(token);
    if (table->table_name == NULL) {
      printf("Memory allocation error\n");
      fclose(file);
      free_schema(schema);
      return NULL;
    }

    token = strtok(NULL, ";");
    table->num_columns = atoi(token);

    table->columns = malloc(table->num_columns * sizeof(ColumnDefinition));
    if (table->columns == NULL) {
      printf("Memory allocation error\n");
      fclose(file);
      free_schema(schema);
      return NULL;
    }

    char* outer_ptr = NULL;
    char* inner_ptr = NULL;

    char* column_defs = strtok(NULL, ";");
    char* column_defs_cpy = strdup(column_defs);
    for (uint32_t j = 0; j < table->num_columns; j++) {
      char* column_def =
          strtok_r(j == 0 ? column_defs_cpy : NULL, ",", &outer_ptr);
      if (column_def == NULL) {
        printf("Error parsing column definition\n");
        fclose(file);
        free_schema(schema);
        return NULL;
      }

      char* column_name = strtok_r(column_def, ":", &inner_ptr);
      char* column_size = strtok_r(NULL, ":", &inner_ptr);
      char* column_type = strtok_r(NULL, ":", &inner_ptr);

      table->columns[j].name = strdup(column_name);
      if (table->columns[j].name == NULL) {
        fprintf(stderr, "Memory allocation error\n");
        fclose(file);
        free_schema(schema);
        return NULL;
      }
      table->columns[j].size = atoi(column_size);
      table->columns[j].type = string_to_column_type(column_type);
    }
  }

  fclose(file);
  return schema;
}

void schema_fill(Schema* schema) {
  for (uint32_t i = 0; i < schema->num_tables; i++) {
    Table* table = &schema->tables[i];
    uint32_t row_size = 0;
    for (uint32_t j = 0; j < table->num_columns; j++) {
      table->columns[j].offset = row_size;
      row_size += table->columns[j].size;
    }

    uint32_t rows_per_page = PAGE_SIZE / row_size;
    uint32_t table_max_rows = rows_per_page * TABLE_MAX_PAGES;

    char* filename = malloc(strlen(table->table_name) + strlen(DIR_PREFIX) + 7);
    if (filename == NULL) {
      printf("Memory allocation error\n");
      free_schema(schema);
      return;
    }
    sprintf(filename, "%s%s.table", DIR_PREFIX, table->table_name);
    table->filename = filename;

    Pager* pager = pager_open(table->filename);
    uint32_t num_pages = pager->file_length / PAGE_SIZE;
    uint32_t bytes_remaining = pager->file_length % PAGE_SIZE;
    uint32_t num_rows =
        (num_pages * rows_per_page) + (bytes_remaining / row_size);

    table->pager = pager;
    table->row_size = row_size;
    table->rows_per_page = rows_per_page;
    table->max_rows = table_max_rows;
    table->num_rows = num_rows;
  }
}

Schema* db_open(const char* filename) {
  Schema* schema = schema_open(filename);
  if (schema == NULL) {
    printf("Error opening schema\n");
    return NULL;
  }
  schema_fill(schema);

  return schema;
}

void table_close(Table* table) {
  Pager* pager = table->pager;
  uint32_t num_full_pages = table->num_rows / table->rows_per_page;

  for (uint32_t i = 0; i < num_full_pages; i++) {
    if (pager->pages[i] == NULL) {
      continue;
    }
    pager_flush(pager, i, PAGE_SIZE);
    free(pager->pages[i]);
    pager->pages[i] = NULL;
  }

  // There may be a partial page to write to the end of the file
  uint32_t num_additional_rows = table->num_rows % table->rows_per_page;
  if (num_additional_rows > 0) {
    uint32_t page_num = num_full_pages;
    if (pager->pages[page_num] != NULL) {
      pager_flush(pager, page_num, num_additional_rows * table->row_size);
      free(pager->pages[page_num]);
      pager->pages[page_num] = NULL;
    }
  }

  int result = close(pager->file_descriptor);
  if (result < 0) {
    printf("Error closing db file.\n");
    exit(EXIT_FAILURE);
  }

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
    void* page = pager->pages[i];
    if (page) {
      free(page);
      pager->pages[i] = NULL;
    }
  }

  free(pager);
}

void db_close(Schema* schema) {
  for (uint32_t i = 0; i < schema->num_tables; i++) {
    Table* table = &(schema->tables[i]);
    table_close(table);
  }

  free(schema->tables);
  free(schema);
}

void* get_page(Pager* pager, uint32_t page_num) {
  if (page_num > TABLE_MAX_PAGES) {
    printf("Tried to fetch page number out of bounds. %d > %d\n", page_num,
           TABLE_MAX_PAGES);
    exit(EXIT_FAILURE);
  }

  if (pager->pages[page_num] == NULL) {
    // Cache miss. Allocate memory and load from file.
    void* page = malloc(PAGE_SIZE);

    uint32_t num_pages = pager->file_length / PAGE_SIZE;
    if (pager->file_length % PAGE_SIZE) {
      // We might save a partial page at the end of the file
      num_pages += 1;
    }

    if (page_num <= num_pages) {
      lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
      ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
      if (bytes_read < 0) {
        printf("Error reading file: %d\n", errno);
        exit(EXIT_FAILURE);
      }
    }

    pager->pages[page_num] = page;
  }

  return pager->pages[page_num];
}

Cursor* table_start(Table* table) {
  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->row_num = 0;
  cursor->end_of_table = (table->num_rows == 0);

  return cursor;
}

Cursor* table_end(Table* table) {
  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  cursor->row_num = table->num_rows;
  cursor->end_of_table = true;

  return cursor;
}

void* cursor_value(Cursor* cursor) {
  uint32_t row_num = cursor->row_num;
  uint32_t page_num = row_num / cursor->table->rows_per_page;
  void* page = get_page(cursor->table->pager, page_num);
  uint32_t row_offset = row_num % cursor->table->rows_per_page;
  uint32_t byte_offset = row_offset * cursor->table->row_size;
  return page + byte_offset;
}

void cursor_advance(Cursor* cursor) {
  cursor->row_num += 1;
  if (cursor->row_num >= cursor->table->num_rows) {
    cursor->end_of_table = true;
  }
}

void serialize_row(Row* source, void* destination, Table* table) {
  memcpy(destination, source->data, table->row_size);
}

void deserialize_row(void* source, Row* destination, Table* table) {
  destination->data = malloc(table->row_size);
  for (uint32_t i = 0; i < table->num_columns; i++) {
    ColumnDefinition column = table->columns[i];
    memcpy(destination->data + column.offset, source + column.offset,
           column.size);
  }
}

InputBuffer* new_input_buffer() {
  InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;

  return input_buffer;
}

void read_input(InputBuffer* input_buffer) {
  ssize_t bytes_read =
      getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

  if (bytes_read <= 0) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
  }

  // Ignore trailing newline
  input_buffer->input_length = bytes_read - 1;
  input_buffer->buffer[bytes_read - 1] = 0;
}

MetaCommandResult do_meta_cmd(InputBuffer* input_buffer, Schema* schema) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    db_close(schema);
    exit(EXIT_SUCCESS);
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

Operator string_to_operator(char* str_op) {
  if (strcmp(str_op, "=") == 0) {
    return OP_EQUAL;
  } else if (strcmp(str_op, "!=") == 0) {
    return OP_NOT_EQUAL;
  } else if (strcmp(str_op, ">") == 0) {
    return OP_GREATER_THAN;
  } else if (strcmp(str_op, "<") == 0) {
    return OP_LESS_THAN;
  } else if (strcmp(str_op, ">=") == 0) {
    return OP_GREATER_THAN_OR_EQUAL;
  } else if (strcmp(str_op, "<=") == 0) {
    return OP_LESS_THAN_OR_EQUAL;
  } else {
    return -1;
  }
}

PrepareResult copy_value_into_bytes(ColumnDefinition* column, Bytes* bytes, char* value) {
  switch (column->type) {
    case INTEGER: {
      int int_value = atoi(value);
      bytes->length = column->size;
      bytes->data = malloc(bytes->length);
      memcpy(bytes->data, &int_value, bytes->length);
      return PREPARE_SUCCESS;
    }
    case VARCHAR: {
      if (value[0] != '\'' || value[strlen(value) - 1] != '\'') {
        return PREPARE_SYNTAX_ERROR;
      }

      value[strlen(value) - 1] = '\0';
      value++;

      bytes->length = column->size;
      bytes->data = malloc(bytes->length);
      memcpy(bytes->data, value, bytes->length);

      return PREPARE_SUCCESS;
    }
    case REAL: {
      double real_value = atof(value);
      bytes->length = column->size;
      bytes->data = malloc(bytes->length);
      memcpy(bytes->data, &real_value, bytes->length);

      return PREPARE_SUCCESS;
    }
  }
}

PrepareResult parse_where_clause(char* where_part, WhereClause* where_clause,
                                 Table* table) {
  char* outer_ptr = NULL;
  char* inner_ptr = NULL;

  char* column_name = strtok_r(where_part, " ", &outer_ptr);
  char* op = strtok_r(NULL, " ", &outer_ptr);
  char* value = strtok_r(NULL, " ", &outer_ptr);

  for (uint32_t i = 0; i < table->num_columns; i++) {
    if (strcmp(column_name, table->columns[i].name) == 0) {
      where_clause->column = &table->columns[i];
      break;
    }
  }

  if (where_clause->column == NULL) {
    return PREPARE_SYNTAX_ERROR;
  }

  where_clause->op = string_to_operator(op);
  if (where_clause->op == -1) {
    return PREPARE_SYNTAX_ERROR;
  }

  if (where_clause->column->type == VARCHAR) {
    if (value[0] != '\'' || value[strlen(value) - 1] != '\'') {
      return PREPARE_SYNTAX_ERROR;
    }
  }

  PrepareResult copy_result = copy_value_into_bytes(where_clause->column, &where_clause->value, value);
  if (copy_result != PREPARE_SUCCESS) {
    return copy_result;
  }

  return PREPARE_SUCCESS;
}

PrepareResult prepare_delete(InputBuffer* input_buffer, Statement* statement, Schema* schema) {
  statement->type = STATEMENT_DELETE;
  DeleteStatement* delete_statement = malloc(sizeof(DeleteStatement));
  if (delete_statement == NULL) {
    return PREPARE_INTERNAL_ERROR;
  }

  memset(delete_statement, 0, sizeof(DeleteStatement));

  char* cpy = strdup(input_buffer->buffer);
  if (!cpy) {
    free(delete_statement);
    return PREPARE_INTERNAL_ERROR;
  }

  trim(cpy);
  char* lower_sql = str_to_lower(cpy);

  if (strncmp(lower_sql, "delete from", 11) != 0) {
    free(cpy);
    free(lower_sql);
    free(delete_statement);
    return PREPARE_SYNTAX_ERROR;
  }

  char* where_pos = strstr(lower_sql, " where ");
  if (!where_pos) {
    free(cpy);
    free(lower_sql);
    free(delete_statement);
    return PREPARE_SYNTAX_ERROR;
  }

  size_t table_name_length = where_pos - (lower_sql + 12);
  char* table_name = malloc(table_name_length + 1);
  strncpy(table_name, cpy + 12, table_name_length);
  table_name[table_name_length] = '\0';
  trim(table_name);

  // check if table exists
  Table* table = NULL;
  for (uint32_t i = 0; i < schema->num_tables; i++) {
    if (strcmp(schema->tables[i].table_name, table_name) == 0) {
      table = &(schema->tables[i]);
      break;
    }
  }
  if (table == NULL) {
    free(cpy);
    free(lower_sql);
    free(table_name);
    free(delete_statement);
    return PREPARE_TABLE_NOT_FOUND;
  }

  statement->table = table;

  char* where_part = where_pos + 7;
  WhereClause* where_clause = malloc(sizeof(WhereClause));
  if (where_clause == NULL) {
    free(cpy);
    free(lower_sql);
    free(table_name);
    free(delete_statement);
    return PREPARE_INTERNAL_ERROR;
  }

  PrepareResult prepare_result = parse_where_clause(where_part, where_clause, table);
  if (prepare_result != PREPARE_SUCCESS) {
    free(cpy);
    free(lower_sql);
    free(table_name);
    free(delete_statement);
    return prepare_result;
  }

  delete_statement->where_clause = where_clause;
  statement->statementDetail = delete_statement;

  return PREPARE_SUCCESS;
}

PrepareResult prepare_update(InputBuffer* input_buffer, Statement* statement, Schema* schema) {
  statement->type = STATEMENT_UPDATE;
  UpdateStatement* update_statement = malloc(sizeof(UpdateStatement));
  if (update_statement == NULL) {
    return PREPARE_INTERNAL_ERROR;
  }

  memset(update_statement, 0, sizeof(UpdateStatement));

  char* cpy = strdup(input_buffer->buffer);
  if (!cpy) {
    free(update_statement);
    printf("strdup cpy\n");
    return PREPARE_INTERNAL_ERROR;
  }

  trim(cpy);
  char* lower_sql = str_to_lower(cpy);

  if (strncmp(lower_sql, "update", 6) != 0) {
    free(cpy);
    free(lower_sql);
    free(update_statement);
    printf("update\n");
    return PREPARE_SYNTAX_ERROR;
  }

  char* set_pos = strstr(lower_sql, " set ");
  if (!set_pos) {
    free(cpy);
    free(lower_sql);
    free(update_statement);
    printf("set_pos\n");
    return PREPARE_SYNTAX_ERROR;
  }

  char* where_pos = strstr(lower_sql, " where ");
  if (!where_pos) {
    free(cpy);
    free(lower_sql);
    free(update_statement);
    printf("where_pos\n");
    return PREPARE_SYNTAX_ERROR;
  }

  size_t table_name_length = set_pos - (lower_sql + 7);
  char* table_name = malloc(table_name_length + 1);
  strncpy(table_name, cpy + 7, table_name_length);
  table_name[table_name_length] = '\0';
  trim(table_name);

  // check if table exists
  Table* table = NULL;
  for (uint32_t i = 0; i < schema->num_tables; i++) {
    if (strcmp(schema->tables[i].table_name, table_name) == 0) {
      table = &(schema->tables[i]);
      break;
    }
  }
  if (table == NULL) {
    free(cpy);
    free(lower_sql);
    free(table_name);
    free(update_statement);
    return PREPARE_TABLE_NOT_FOUND;
  }

  statement->table = table;

  char* set_part = set_pos + 5;
  char* where_part = where_pos + 7;

  // assume only update one column
  char* column_name = strtok(set_part, " = ");
  char* value = strtok(NULL, " = ");

  ColumnDefinition* column = NULL;
  for (uint32_t i = 0; i < table->num_columns; i++) {
    if (strcmp(table->columns[i].name, column_name) == 0) {
      column = &table->columns[i];
      break;
    }
  }

  if (column == NULL) {
    free(cpy);
    free(lower_sql);
    free(table_name);
    free(update_statement);
    printf("column\n");
    return PREPARE_SYNTAX_ERROR;
  }

  update_statement->column = column;

  WhereClause* where_clause = malloc(sizeof(WhereClause));
  if (where_clause == NULL) {
    free(cpy);
    free(lower_sql);
    free(table_name);
    free(update_statement);
    return PREPARE_INTERNAL_ERROR;
  }

  PrepareResult prepare_result = parse_where_clause(where_part, where_clause, table);
  if (prepare_result != PREPARE_SUCCESS) {
    free(cpy);
    free(lower_sql);
    free(table_name);
    free(update_statement);
    return prepare_result;
  }

  update_statement->where_clause = where_clause;
  
  PrepareResult copy_result = copy_value_into_bytes(column, &update_statement->value, value);
  if (copy_result != PREPARE_SUCCESS) {
    free(cpy);
    free(lower_sql);
    free(table_name);
    free(update_statement);
    return copy_result;
  }

  statement->statementDetail = update_statement;

  return PREPARE_SUCCESS;
}

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement,
                             Schema* schema) {
  statement->type = STATEMENT_INSERT;
  InsertStatement* insert_statement = malloc(sizeof(InsertStatement));
  if (insert_statement == NULL) {
    return PREPARE_INTERNAL_ERROR;
  }

  memset(insert_statement, 0, sizeof(InsertStatement));

  char* cpy = strdup(input_buffer->buffer);
  if (!cpy) {
    free(insert_statement);
    return PREPARE_INTERNAL_ERROR;
  }

  trim(cpy);
  char* lower_sql = str_to_lower(cpy);

  if (strncmp(lower_sql, "insert into", 11) != 0) {
    free(cpy);
    free(lower_sql);
    free(insert_statement);
    return PREPARE_SYNTAX_ERROR;
  }

  char* values_pos = strstr(lower_sql, " values ");
  if (!values_pos) {
    free(cpy);
    free(lower_sql);
    free(insert_statement);
    return PREPARE_SYNTAX_ERROR;
  }

  size_t table_name_length = values_pos - (lower_sql + 12);
  char* table_name = malloc(table_name_length + 1);
  strncpy(table_name, cpy + 12, table_name_length);
  table_name[table_name_length] = '\0';
  trim(table_name);

  // check if table exists
  Table* table = NULL;
  for (uint32_t i = 0; i < schema->num_tables; i++) {
    if (strcmp(schema->tables[i].table_name, table_name) == 0) {
      table = &(schema->tables[i]);
      break;
    }
  }
  if (table == NULL) {
    free(cpy);
    free(lower_sql);
    free(insert_statement);
    return PREPARE_SYNTAX_ERROR;
  }

  statement->table = table;

  char* values_start = strchr(values_pos, '(');
  char* values_end = strrchr(values_pos, ')');
  if (!values_start || !values_end || values_end <= values_start) {
    free(cpy);
    free(lower_sql);
    free(table_name);
    free(insert_statement);
    return PREPARE_SYNTAX_ERROR;
  }

  size_t values_length = values_end - (values_start + 1);
  char* values_part = malloc(values_length + 1);
  strncpy(values_part, values_start + 1, values_length);
  values_part[values_length] = '\0';

  char* value = strtok(values_part, ",");
  int num_values = 0;
  char** values = NULL;
  while (value) {
    num_values++;
    values = realloc(values, sizeof(char*) * num_values);
    values[num_values - 1] = strdup(value);
    trim(values[num_values - 1]);
    value = strtok(NULL, ",");
  }

  free(cpy);
  free(lower_sql);
  free(table_name);
  free(values_part);

  // check whether the number of values matches the number of columns
  if (num_values != table->num_columns) {
    for (int i = 0; i < num_values; i++) {
      free(values[i]);
    }
    free(values);
    free(insert_statement);
    return PREPARE_SYNTAX_ERROR;
  }

  Row row;
  row.data = malloc(table->row_size);
  memset(row.data, 0, table->row_size);
  for (int i = 0; i < num_values; i++) {
    ColumnDefinition column = table->columns[i];
    char* value = values[i];
    if (column.type == INTEGER) {
      int int_value = atoi(value);
      if (int_value <= 0 && strcmp(column.name, "id") == 0) {
        for (int i = 0; i < num_values; i++) {
          free(values[i]);
        }
        free(values);
        free(insert_statement);
        return PREPARE_NEGATIVE_ID;
      }

      memcpy(row.data + column.offset, &int_value, column.size);
    } else if (column.type == VARCHAR) {
      int length = strlen(value);
      if (length > column.size) {
        for (int i = 0; i < num_values; i++) {
          free(values[i]);
        }
        free(values);
        free(insert_statement);
        return PREPARE_STRING_TOO_LONG;
      }

      memcpy(row.data + column.offset, value, strlen(value));
    } else if (column.type == REAL) {
      float real_value = atof(value);

      memcpy(row.data + column.offset, &real_value, column.size);
    }
  }

  insert_statement->row_to_insert = row;
  statement->statementDetail = insert_statement;

  return PREPARE_SUCCESS;
}

PrepareResult prepare_select(InputBuffer* input_buffer, Statement* statement,
                             Schema* schema) {
  statement->type = STATEMENT_SELECT;
  SelectStatement* select_statement = malloc(sizeof(SelectStatement));
  if (select_statement == NULL) {
    return PREPARE_INTERNAL_ERROR;
  }

  memset(select_statement, 0, sizeof(SelectStatement));

  char* cpy = strdup(input_buffer->buffer);
  if (!cpy) {
    free(select_statement);
    return PREPARE_INTERNAL_ERROR;
  }

  trim(cpy);
  char* lower_sql = str_to_lower(cpy);

  if (strncmp(lower_sql, "select", 6) != 0) {
    free(cpy);
    free(lower_sql);
    free(select_statement);
    return PREPARE_SYNTAX_ERROR;
  }

  char* from_pos = strstr(lower_sql, " from ");
  if (!from_pos) {
    free(cpy);
    free(lower_sql);
    free(select_statement);
    return PREPARE_SYNTAX_ERROR;
  }

  size_t columns_length = from_pos - (lower_sql + 7);
  char* columns_part = malloc(columns_length + 1);
  strncpy(columns_part, cpy + 7, columns_length);
  columns_part[columns_length] = '\0';
  trim(columns_part);

  char** columns = NULL;
  uint32_t num_columns = 0;
  if (strcmp(columns_part, "*") == 0) {
    select_statement->is_select_all = true;
  } else {
    select_statement->is_select_all = false;
    char* column = strtok(columns_part, ",");
    while (column) {
      num_columns++;
      columns = realloc(columns, sizeof(char*) * num_columns);
      columns[num_columns - 1] = strdup(column);
      trim(columns[num_columns - 1]);
      column = strtok(NULL, ",");
    }
  }

  // parse where clause
  char* where_pos = strstr(lower_sql, " where ");
  size_t table_name_length = 0;
  // char* table_name = NULL;
  if (where_pos) {
    table_name_length = where_pos - (from_pos + 6);
  } else {
    table_name_length = strlen(from_pos + 6);
  }

  char* table_name = malloc(table_name_length + 1);
  strncpy(table_name, from_pos + 6, table_name_length);
  table_name[table_name_length] = '\0';
  trim(table_name);

  Table* table = NULL;
  for (uint32_t i = 0; i < schema->num_tables; i++) {
    if (strcmp(schema->tables[i].table_name, table_name) == 0) {
      table = &(schema->tables[i]);
      break;
    }
  }
  if (table == NULL) {
    for (uint32_t i = 0; i < num_columns; i++) {
      free(columns[i]);
    }
    free(columns);
    free(cpy);
    free(lower_sql);
    free(columns_part);
    free(select_statement);
    return PREPARE_TABLE_NOT_FOUND;
  }

  statement->table = table;

  // check whether the columns are all valid
  select_statement->num_columns = num_columns;
  select_statement->columns = malloc(sizeof(ColumnDefinition) * num_columns);
  for (uint32_t i = 0; i < num_columns; i++) {
    bool valid = false;
    for (uint32_t j = 0; j < table->num_columns; j++) {
      if (strcmp(columns[i], table->columns[j].name) == 0) {
        valid = true;
        select_statement->columns[i] = table->columns[j];
        break;
      }
    }
    if (!valid) {
      for (uint32_t i = 0; i < num_columns; i++) {
        free(columns[i]);
      }
      free(columns);
      free(cpy);
      free(lower_sql);
      free(columns_part);
      free(select_statement);
      return PREPARE_SYNTAX_ERROR;
    }
  }

  if (where_pos) {
    WhereClause* where_clause = malloc(sizeof(WhereClause));
    if (where_clause == NULL) {
      for (uint32_t i = 0; i < num_columns; i++) {
        free(columns[i]);
      }
      free(columns);
      free(cpy);
      free(lower_sql);
      free(columns_part);
      free(select_statement);
      return PREPARE_INTERNAL_ERROR;
    }

    char* where_part = strdup(where_pos + 7);
    if (!where_part) {
      for (uint32_t i = 0; i < num_columns; i++) {
        free(columns[i]);
      }
      free(columns);
      free(cpy);
      free(lower_sql);
      free(columns_part);
      free(select_statement);
      return PREPARE_INTERNAL_ERROR;
    }

    PrepareResult prepare_result =
        parse_where_clause(where_part, where_clause, statement->table);
    if (prepare_result != PREPARE_SUCCESS) {
      for (uint32_t i = 0; i < num_columns; i++) {
        free(columns[i]);
      }
      free(columns);
      free(cpy);
      free(lower_sql);
      free(columns_part);
      free(select_statement);
      free(where_clause);
      free(where_part);
      return prepare_result;
    }

    select_statement->where_clause = where_clause;
  }

  statement->statementDetail = select_statement;

  free(cpy);
  free(lower_sql);
  free(columns_part);
  for (uint32_t i = 0; i < num_columns; i++) {
    free(columns[i]);
  }
  free(columns);

  return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement,
                                Schema* schema) {
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    return prepare_insert(input_buffer, statement, schema);
  }
  if (strncmp(input_buffer->buffer, "select", 6) == 0) {
    return prepare_select(input_buffer, statement, schema);
  }
  if (strncmp(input_buffer->buffer, "update", 6) == 0) {
    return prepare_update(input_buffer, statement, schema);
  }
  if (strncmp(input_buffer->buffer, "delete", 6) == 0) {
    return prepare_delete(input_buffer, statement, schema);
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement* statement) {
  InsertStatement* insert_statement = statement->statementDetail;
  Table* table = statement->table;

  if (table->num_rows >= table->max_rows) {
    return EXECUTE_TABLE_FULL;
  }

  Row* row_to_insert = &(insert_statement->row_to_insert);
  Cursor* cursor = table_end(table);

  serialize_row(row_to_insert, cursor_value(cursor), table);
  table->num_rows += 1;

  free(cursor);

  return EXECUTE_SUCCESS;
}

void print_row(Row* row, Table* table, SelectStatement* select_statement) {
  uint32_t num_columns = select_statement->is_select_all
                             ? table->num_columns
                             : select_statement->num_columns;
  ColumnDefinition* columns = select_statement->is_select_all
                                  ? table->columns
                                  : select_statement->columns;

  printf("(");
  for (uint32_t i = 0; i < num_columns; i++) {
    ColumnDefinition column = columns[i];
    if (column.type == INTEGER) {
      int value;
      memcpy(&value, row->data + column.offset, column.size);
      printf("%d", value);
    } else if (column.type == VARCHAR) {
      char* value = malloc(column.size + 1);
      memcpy(value, row->data + column.offset, column.size);
      value[column.size] = '\0';
      printf("%s", value);
    } else if (column.type == REAL) {
      float value;
      memcpy(&value, row->data + column.offset, column.size);
      printf("%f", value);
    }
    if (i < num_columns - 1) {
      printf(", ");
    }
  }

  printf(")\n");
}

bool valid_where_clause(Row* row, WhereClause* where_clause) {
  ColumnDefinition* column = where_clause->column;
  Operator op = where_clause->op;
  Bytes value = where_clause->value;

  switch (column->type) {
    case INTEGER: {
      int int_value;
      memcpy(&int_value, row->data + column->offset, column->size);
      int where_int_value;
      memcpy(&where_int_value, value.data, value.length);

      switch (op) {
        case OP_EQUAL:
          return int_value == where_int_value;
        case OP_NOT_EQUAL:
          return int_value != where_int_value;
        case OP_GREATER_THAN:
          return int_value > where_int_value;
        case OP_LESS_THAN:
          return int_value < where_int_value;
        case OP_GREATER_THAN_OR_EQUAL:
          return int_value >= where_int_value;
        case OP_LESS_THAN_OR_EQUAL:
          return int_value <= where_int_value;
      }
    }
    case VARCHAR: {
      char* str_value = malloc(column->size + 1);
      memcpy(str_value, row->data + column->offset, column->size);
      str_value[column->size] = '\0';

      switch (op) {
        case OP_EQUAL:
          return strcmp(str_value, value.data) == 0;
        case OP_NOT_EQUAL:
          return strcmp(str_value, value.data) != 0;
      }
    }
    case REAL: {
      double real_value;
      memcpy(&real_value, row->data + column->offset, column->size);
      double where_real_value;
      memcpy(&where_real_value, value.data, value.length);

      switch (op) {
        case OP_EQUAL:
          return real_value == where_real_value;
        case OP_NOT_EQUAL:
          return real_value != where_real_value;
        case OP_GREATER_THAN:
          return real_value > where_real_value;
        case OP_LESS_THAN:
          return real_value < where_real_value;
        case OP_GREATER_THAN_OR_EQUAL:
          return real_value >= where_real_value;
        case OP_LESS_THAN_OR_EQUAL:
          return real_value <= where_real_value;
      }
    }
  }
}

ExecuteResult execute_select(Statement* statement) {
  SelectStatement* select_statement = statement->statementDetail;
  Table* table = statement->table;
  WhereClause* where_clause = select_statement->where_clause;

  Cursor* cursor = table_start(table);

  Row row;
  while (!(cursor->end_of_table)) {
    deserialize_row(cursor_value(cursor), &row, table);

    if (where_clause && !valid_where_clause(&row, where_clause)) {
      cursor_advance(cursor);
      continue;
    }

    print_row(&row, table, select_statement);
    cursor_advance(cursor);
  }

  free(cursor);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_update(Statement* statement) {
  UpdateStatement* update_statement = statement->statementDetail;
  Table* table = statement->table;

  Cursor* cursor = table_start(table);

  Row row;
  while (!(cursor->end_of_table)) {
    deserialize_row(cursor_value(cursor), &row, table);

    if (!valid_where_clause(&row, update_statement->where_clause)) {
      cursor_advance(cursor);
      continue;
    }

    ColumnDefinition* column = update_statement->column;
    memcpy(row.data + column->offset, update_statement->value.data, column->size);

    serialize_row(&row, cursor_value(cursor), table);

    cursor_advance(cursor);
  }

  free(cursor);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_delete(Statement* statement) {
  DeleteStatement* delete_statement = statement->statementDetail;
  Table* table = statement->table;

  Cursor* cursor = table_start(table);

  int num_deleted_rows = 0;

  Row row;
  while (!(cursor->end_of_table)) {
    deserialize_row(cursor_value(cursor), &row, table);

    if (!valid_where_clause(&row, delete_statement->where_clause)) {
      cursor_advance(cursor);
      continue;
    }

    memset(cursor_value(cursor), 0, table->row_size);
    cursor_advance(cursor);
    num_deleted_rows++;
  }

  cursor = table_start(table);
  Cursor* cursor2 = malloc(sizeof(Cursor));
  cursor2->table = table;
  cursor2->row_num = -1;
  cursor2->end_of_table = false;

  unsigned char* empty_row = malloc(table->row_size);
  memset(empty_row, 0, table->row_size);

  while (!(cursor->end_of_table)) {
    deserialize_row(cursor_value(cursor), &row, table);

    if (memcmp(cursor_value(cursor), empty_row, table->row_size) == 0) {
      cursor2->row_num = cursor->row_num;
      cursor2->end_of_table = cursor->end_of_table;
      cursor_advance(cursor);
      continue;
    }

    if (memcmp(cursor_value(cursor), empty_row, table->row_size) != 0) {
      if (cursor2->row_num != -1) {
        memcpy(cursor_value(cursor2), cursor_value(cursor), table->row_size);
        memset(cursor_value(cursor), 0, table->row_size);
        cursor_advance(cursor);
        cursor_advance(cursor2);
      } else {
        cursor_advance(cursor);
      }
    }
  }

  table->num_rows -= num_deleted_rows;
  free(cursor);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement) {
  switch (statement->type) {
    case STATEMENT_INSERT:
      return execute_insert(statement);
    case STATEMENT_SELECT:
      return execute_select(statement);
    case STATEMENT_UPDATE:
      return execute_update(statement);
    case STATEMENT_DELETE:
      return execute_delete(statement);
  }
}

void print_prompt() { printf("db > "); }

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printf("Must supply a database filename.\n");
    exit(EXIT_FAILURE);
  }

  char* filename = argv[1];
  Schema* schema = db_open(filename);

  InputBuffer* input_buffer = new_input_buffer();
  while (true) {
    print_prompt();
    read_input(input_buffer);

    if (input_buffer->buffer[0] == '.') {
      MetaCommandResult meta_command_result = do_meta_cmd(input_buffer, schema);
      switch (meta_command_result) {
        case META_COMMAND_SUCCESS:
          continue;
        case META_COMMAND_UNRECOGNIZED_COMMAND:
          printf("Unrecognized command '%s'\n", input_buffer->buffer);
          continue;
      }
    }

    Statement statement;
    PrepareResult prepare_result =
        prepare_statement(input_buffer, &statement, schema);
    switch (prepare_result) {
      case PREPARE_SUCCESS:
        break;
      case PREPARE_NEGATIVE_ID:
        printf("ID must be positive.\n");
        continue;
      case PREPARE_STRING_TOO_LONG:
        printf("String is too long.\n");
        continue;
      case PREPARE_UNRECOGNIZED_STATEMENT:
        printf("Unrecognized keyword at start of '%s'.\n",
               input_buffer->buffer);
        continue;
      case PREPARE_INTERNAL_ERROR:
        printf("Internal error.\n");
        continue;
      case PREPARE_SYNTAX_ERROR:
        printf("Syntax error.\n");
        continue;
      case PREPARE_TABLE_NOT_FOUND:
        printf("Table not found.\n");
        continue;
    }

    ExecuteResult execute_result = execute_statement(&statement);
    switch (execute_result) {
      case EXECUTE_SUCCESS:
        printf("Executed.\n");
        break;
      case EXECUTE_TABLE_FULL:
        printf("Error: Table full.\n");
        break;
    }
  }
}
