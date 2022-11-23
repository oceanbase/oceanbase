/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>

#define TEST_LARGE

#define MAX_COLUMN_NAME_LENGTH 128 //??
#define MAX_TABLE_NAME_LENGTH 512 //???
#define MAX_VALUE_LENGTH 2048
#define MAX_DB_NAME_LENGTH 64
#define MAX_KEY_PART 15 // 64
#define MAX_BUF_LENGTH 2048*2048

#ifdef TEST_MIN
#define MAX_TABLES 1
#define MAX_INDEXES 3
#define MAX_COLUMNS 12
#define N_ROWS 1
#endif

#ifdef TEST_SMALL
#define MAX_TABLES 50
#define MAX_INDEXES 8
#define MAX_COLUMNS 30
#define N_ROWS 10
#endif

#ifdef TEST_LARGE
#define MAX_TABLES 5000
#define MAX_INDEXES 10
#define MAX_COLUMNS 30
#define N_ROWS 100
#endif

#ifdef TEST_BOUNDARY
#define MAX_TABLES 10
#define MAX_INDEXES 128
#define MAX_COLUMNS 512
#define N_ROWS 10
#endif

enum enum_data_type {
  INT = 0,
  VARCHAR,
  // more ...
  MAX_DATA_TYPE
};
typedef enum enum_data_type data_type_t;

enum enum_action {
  CREATE_TABLE = -1,
 // DROP_TABLE,
 // RENAME_TABLE,
  ADD_OR_DROP_COLUMN,
  ADD_OR_DROP_INDEX,
  //RENAME_COLUMN,
  // more ...
  MAX_ACTION
};
typedef enum enum_action action_t;

struct struct_type_value_pair {
  data_type_t type;
  char type_str[64];
  char value[MAX_VALUE_LENGTH];
};
typedef struct struct_type_value_pair type_value_pair;

const type_value_pair pairs[MAX_DATA_TYPE] = {
  {INT, "int", "5"},
  {VARCHAR, "varchar(200)", "\"oceanbase\""}
  // more...
};

struct struct_column_schema {
  bool enabled;
  char cname[MAX_COLUMN_NAME_LENGTH];
  data_type_t data_type;
  bool not_null;
  bool has_default;
};
typedef struct struct_column_schema column_schema;

struct struct_index_schema {
  bool enabled;
  char index_name[MAX_TABLE_NAME_LENGTH];
  int n_cols;
  int col_indexes[MAX_COLUMNS];
};
typedef struct struct_index_schema index_schema;

// TODO: index will be usd in index array of column
struct struct_table_schema {
  bool enabled;
  char table_name[MAX_TABLE_NAME_LENGTH];
  column_schema column_schemas[MAX_COLUMNS];
  index_schema index_schemas[MAX_INDEXES];
};
typedef struct struct_table_schema table_schema;

struct struct_env {
  char db_name[MAX_DB_NAME_LENGTH];
  table_schema schema[MAX_TABLES];
  int seq;
  int n_rows;
  int n_indexes;
};

/* global variables */
struct struct_env env;
bool stop = false;

/* common methods */
void log_msg(char *msg) {
  fprintf(stderr, "%s\n", msg);
}

bool sr() {
  return rand()%10<5;
}

int init_conn() {
  // init tenant and connection
  return 0;
}

int init_schema() {
  int t = 0;
  int i = 0;
  int j = 0;
  snprintf(env.db_name, MAX_DB_NAME_LENGTH, "db%d", getppid());
  env.seq = 0;
  for (t=0; t<MAX_TABLES; t++) {
    table_schema *tbl = &env.schema[t];
    tbl->enabled = true;
    snprintf(tbl->table_name, MAX_TABLE_NAME_LENGTH, "t%d", t);
    for (j=0; j<MAX_COLUMNS; j++) {
      column_schema *col = &tbl->column_schemas[j];
      col->enabled = sr();
      snprintf(col->cname, MAX_COLUMN_NAME_LENGTH, "c%ld", j);
      col->data_type = j%MAX_DATA_TYPE;
      col->not_null = sr();
      col->has_default = sr();
    }

    for (i=0; i<MAX_INDEXES; i++) {
      index_schema *idx = &tbl->index_schemas[i];
      idx->enabled = sr();
      snprintf(idx->index_name, MAX_TABLE_NAME_LENGTH, "idx%d", i);
      int last_idx = 0;
      for (j=0; j<MAX_COLUMNS; j++) {
        idx->col_indexes[j] = -1;
        if (sr()) continue;
        if (last_idx>=MAX_KEY_PART) continue;
        if (!tbl->column_schemas[j].enabled) continue;
        idx->col_indexes[last_idx++] = j;
      }
      idx->n_cols = last_idx;
    }
  }
  return 0;
}

// TODO: check at least one column in base table
void dump_schema() {
  int t = 0;
  int i = 0;
  int j = 0;
  fprintf(stderr, "\n");
  for (t=0; t<MAX_TABLES; t++) {
    table_schema *tbl = &env.schema[t];
    assert(tbl->enabled);
    fprintf(stderr, "#base table %s\n", tbl->table_name);
    for (j=0; j<MAX_COLUMNS; j++) {
      const column_schema *col = &tbl->column_schemas[j];
      if (!col->enabled) continue;
      fprintf(stderr, "#  %s %s\n", col->cname, pairs[col->data_type].type_str);
    }
    fprintf(stderr, "\n");

    for (i=0; i<MAX_INDEXES; i++) {
      const index_schema *idx = &tbl->index_schemas[i];
      if (!idx->enabled) continue;
      fprintf(stderr, "#index table %s\n", idx->index_name);
      fprintf(stderr, "#   ");
      for (j=0; j<MAX_COLUMNS; j++) {
        if (idx->col_indexes[j] == -1) continue;
        const column_schema *col = &tbl->column_schemas[idx->col_indexes[j]];
        assert(col->enabled);
        fprintf(stderr, "%s ", col->cname);
      }
      fprintf(stderr, "\n");
    }
    fprintf(stderr, "\n");
  }
}

void init_table(int tid) {
  const table_schema *tbl = &env.schema[tid];
  char table_define[MAX_BUF_LENGTH];
  memset(table_define, 0, MAX_BUF_LENGTH);
  snprintf(table_define, MAX_BUF_LENGTH, "DROP TABLE IF EXISTS %s;", tbl->table_name);
  log_msg(table_define);
  int pos = snprintf(table_define, MAX_BUF_LENGTH, "CREATE TABLE %s(", tbl->table_name);
  bool need_comma = false;
  int t = 0;
  int i = 0;
  int j = 0;

  for (j=0; j<MAX_COLUMNS; j++) {
    const column_schema *col = &tbl->column_schemas[j];
    if (!col->enabled) continue;
    char defaults[MAX_BUF_LENGTH];
    snprintf(defaults, MAX_BUF_LENGTH, "DEFAULT '%s'", pairs[col->data_type].value);
    if (need_comma) pos += snprintf(table_define+pos, MAX_BUF_LENGTH, ", ");
    pos += snprintf(table_define+pos, MAX_BUF_LENGTH, "%s %s %s %s",
        col->cname,
        pairs[col->data_type].type_str,
        col->not_null? "NOT NULL": "",
        col->has_default? defaults: "");
    need_comma = true;
  }

  for (i=0; i<MAX_INDEXES; i++) {
    const index_schema *idx = &tbl->index_schemas[i];
    if (!idx->enabled) continue;
    if (idx->n_cols == 0) continue;
    char index_define[MAX_BUF_LENGTH];
    int index_pos = 0;
    memset(index_define, 0, MAX_BUF_LENGTH);
    bool index_need_comma = false;
    index_pos += snprintf(index_define, MAX_BUF_LENGTH, " INDEX %s(", idx->index_name);
    for (j=0; j<idx->n_cols; j++) {
      const column_schema *col = &tbl->column_schemas[idx->col_indexes[j]];
      assert(col->enabled);
      if (j!=0) index_pos += snprintf(index_define+index_pos, MAX_BUF_LENGTH, ",");
      index_pos += snprintf(index_define+index_pos, MAX_BUF_LENGTH, "%s", col->cname);
    }
    snprintf(index_define+index_pos, MAX_BUF_LENGTH, ")");
    if (need_comma) pos += snprintf(table_define+pos, MAX_BUF_LENGTH, ",");
    pos += snprintf(table_define+pos, MAX_BUF_LENGTH, "%s", index_define);
    need_comma = true;
  }

  snprintf(table_define+pos, MAX_BUF_LENGTH, ");");
  log_msg(table_define);
}

void init_db() {
  char db_define[MAX_BUF_LENGTH];
  snprintf(db_define, MAX_BUF_LENGTH, "DROP DATABASE IF EXISTS %s; CREATE DATABASE %s; use %s;\n", env.db_name, env.db_name, env.db_name);
  log_msg(db_define);
}

void init_ob() {
 int t = 0;
 init_db();
 for (t=0; t<MAX_TABLES; t++)
   init_table(t);
}

int init() {
  init_schema();
  init_conn();
  dump_schema();
  init_ob();
  return 0;
}

action_t gen_action() {
  action_t  action = random() % MAX_ACTION;
  return action;
}

bool any_index_has_col(int tid, int cid) {
  int i, j;
  const table_schema *tbl = &env.schema[tid];
  for (i=0; i<MAX_INDEXES; i++) {
    const index_schema *idx = &tbl->index_schemas[i];
    for (j=0; j<MAX_COLUMNS; j++) {
      if (idx->col_indexes[j] == cid)
        return true;
    }
  }
  return false;
}

void add_or_drop_column(int tid) {
  char alter_define[MAX_BUF_LENGTH];
  memset(alter_define, 0, MAX_BUF_LENGTH);
  table_schema *tbl = &env.schema[tid];
  bool ok = false;
  int pos = snprintf(alter_define, MAX_BUF_LENGTH, "ALTER TABLE %s /* add_or_drop_column %d */", tbl->table_name, env.seq++);
  bool need_comma = false;
  int i = 0;
  int j = 0;

  for (i=0; i<MAX_COLUMNS; i++) {
    if (sr()) continue;
    column_schema *col = &tbl->column_schemas[i];
    if (col->enabled) {
      // drop column
      // ob-1.0 don't allow drop column that belongs to some index, therefore, will not act the same with mysql that,
      // index will be automatedly dropped if owned columns are all dropped.
      if (any_index_has_col(tid, i)) continue;
      if (need_comma) pos += snprintf(alter_define+pos, MAX_BUF_LENGTH, ",");
      pos += snprintf(alter_define+pos, MAX_BUF_LENGTH, " DROP COLUMN %s ", col->cname);
      if (!ok) ok = true;
      need_comma = true;
      col->enabled = false;
    } else if (!col->enabled) {
      char defaults[MAX_BUF_LENGTH];
      snprintf(defaults, MAX_BUF_LENGTH, "DEFAULT '%s'", pairs[col->data_type].value);
      if (need_comma) pos += snprintf(alter_define+pos, MAX_BUF_LENGTH, ",");
      pos += snprintf(alter_define+pos, MAX_BUF_LENGTH, " ADD COLUMN %s %s %s %s",
          col->cname,
          pairs[col->data_type].type_str,
          col->not_null? "NOT NULL": "",
          col->has_default? defaults: "");
      if (!ok) ok = true;
      need_comma = true;
      col->enabled = true;
    }
  }
  snprintf(alter_define+pos, MAX_BUF_LENGTH, ";");

  // update index if column is dropped
  for (i=0; i<MAX_INDEXES; i++) {
    index_schema *idx = &tbl->index_schemas[i];
    int n_drops = 0;
    for (j=0; j<idx->n_cols; j++) {
      if (!tbl->column_schemas[idx->col_indexes[j]].enabled) {
        idx->col_indexes[j] = -1;
        n_drops++;
      }
    }

    int t_drops = n_drops;
    while (t_drops--) {
      for (j=0; j<idx->n_cols; j++) {
        if (idx->col_indexes[j] == -1) {
          idx->col_indexes[j] = idx->col_indexes[j+1];
          idx->col_indexes[j+1] = -1;
        }
      }
    }
    idx->n_cols -= n_drops;
  }

#if 0
  if (!ok) {
    // drop all indexes
    for (i=1; i<MAX_INDEXES; i++) {
      table_schema *tbl = &env.schema[i];
      tbl->enabled = false;
    }
  }
#endif

  log_msg(alter_define);
}


void shuffer_index(int tid) {
  int i = 1;
  int j = 0;
  table_schema *tbl = &env.schema[tid];

  for (i=0; i<MAX_INDEXES; i++) {
    if (sr()) continue;
    index_schema *idx = &tbl->index_schemas[i];
    if (idx->enabled) continue;
    int last_idx = 0;
    for (j=0; j<MAX_COLUMNS; j++) {
      idx->col_indexes[j] = -1;
      if (sr()) continue;
      if (last_idx>=MAX_KEY_PART) continue;
      if (!tbl->column_schemas[j].enabled) continue;
      idx->col_indexes[last_idx++] = j;
    }
    idx->n_cols = last_idx;
  }
}

void add_or_drop_index(int tid) {
  char alter_define[MAX_BUF_LENGTH];
  memset(alter_define, 0, MAX_BUF_LENGTH);
  bool ok = false;
  table_schema *tbl = &env.schema[tid];
  int pos = snprintf(alter_define, MAX_BUF_LENGTH, "ALTER TABLE %s /* add_or_drop_index %d */", tbl->table_name, env.seq++);
  bool need_comma = false;
  int i = 0;
  int j = 0;

  for (i=0; i<MAX_INDEXES; i++) {
    if(sr()) continue;

    index_schema *idx = &tbl->index_schemas[i];
    if (idx->enabled) {
      // drop index
      if (need_comma) pos += snprintf(alter_define+pos, MAX_BUF_LENGTH, ",");
      pos += snprintf(alter_define+pos, MAX_BUF_LENGTH, " DROP INDEX %s", idx->index_name);
      need_comma = true;
      idx->enabled = false;
    } else {
      // add index
      if (idx->n_cols == 0) continue;

      char add_index_str[MAX_BUF_LENGTH];
      memset(add_index_str, 0, MAX_BUF_LENGTH);
      int add_index_pos = 0;
      bool index_need_comma = false;
      add_index_pos += snprintf(add_index_str+add_index_pos, MAX_BUF_LENGTH, 
          " ADD INDEX %s(", idx->index_name);
      for (j=0; j<idx->n_cols; j++) {
        const column_schema *col = &tbl->column_schemas[idx->col_indexes[j]];
        assert(col->enabled);
        if (j!=0) add_index_pos += snprintf(add_index_str+add_index_pos, MAX_BUF_LENGTH, ",");
        add_index_pos += snprintf(add_index_str+add_index_pos, MAX_BUF_LENGTH, " %s", col->cname);
      }
      idx->enabled = true;

      snprintf(add_index_str+add_index_pos, MAX_BUF_LENGTH, ")");
      if (need_comma) pos += snprintf(alter_define+pos, MAX_BUF_LENGTH, ",");
      pos += snprintf(alter_define+pos, MAX_BUF_LENGTH, "%s", add_index_str);
      need_comma = true;
    }
  }

  snprintf(alter_define+pos, MAX_BUF_LENGTH, ";");

#if 0
  if (!need_comma) {
    // alter table, drop all indexes
    for (i=1; i<MAX_INDEXES; i++) {
      table_schema *tbl = &env.schema[i];
      tbl->enabled = false;
    }
  }
#endif
  log_msg(alter_define);
}

void fill_table(int tid, int n) {
  int i = 0;
  table_schema *tbl = &env.schema[tid];
  char insert_define[MAX_BUF_LENGTH];
  memset(insert_define, 0, MAX_BUF_LENGTH);

  bool need_comma = false;
  int pos = snprintf(insert_define, MAX_BUF_LENGTH, "INSERT INTO %s(", tbl->table_name);
  for (i=0; i<MAX_COLUMNS; i++) {
      const column_schema *col = &tbl->column_schemas[i];
      if (!col->enabled) continue;
      if (need_comma) pos += snprintf(insert_define+pos, MAX_BUF_LENGTH, ", ");
      pos += snprintf(insert_define+pos, MAX_BUF_LENGTH, "%s", col->cname);
      need_comma = true;
  }
  pos += snprintf(insert_define+pos, MAX_BUF_LENGTH, ") VALUES(", tbl->table_name);

  while (n-->0) {
    bool need_comma = false;
    for (i=0; i<MAX_COLUMNS; i++) {
      const column_schema *col = &tbl->column_schemas[i];
      if (col->enabled) {
        if (need_comma) pos += snprintf(insert_define+pos, MAX_BUF_LENGTH, ",");
        pos += snprintf(insert_define+pos, MAX_BUF_LENGTH, "%s",
            pairs[col->data_type].value);
        need_comma = true;
      }
    }
    if (n>0) pos += snprintf(insert_define+pos, MAX_BUF_LENGTH, "), (");
  }
  pos += snprintf(insert_define+pos, MAX_BUF_LENGTH, ");");
  log_msg(insert_define);
}

void fill_data(int n) {
  int t = 0;
  for (t=0; t<MAX_TABLES; t++) {
    fill_table(t, n);
  }
}

int do_action(action_t action) {
  int tid = rand() % MAX_TABLES;
  switch (action) {
    case ADD_OR_DROP_COLUMN:
      add_or_drop_column(tid);
      break;
    case ADD_OR_DROP_INDEX:
      shuffer_index(tid);
      add_or_drop_index(tid);
    default:
      break;
  }
  return 0;
}

int do_work() {
 action_t action;
 action = gen_action();
 do_action(action);
}

int start(int n) {
  int loops = 0;
  while (!stop) {
    do_work();
    dump_schema();
    fill_data(N_ROWS);
    if (++loops%n == 0) {
      log_msg("#finished");
      stop = true;
    }
  }
  return 0;
}

int main(int argc, void *argv[]) {
  init();
  start(atoi(argv[1]));
}

