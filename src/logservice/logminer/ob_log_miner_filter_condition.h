/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 */

#ifndef OCEANBASE_LOG_MINER_FILTER_CONDITION_H_
#define OCEANBASE_LOG_MINER_FILTER_CONDITION_H_

#include "lib/json/ob_json.h"
#include "lib/string/ob_string_buffer.h"
#include "libobcdc.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/string/ob_fixed_length_string.h"
#include "ob_log_miner_utils.h"

namespace oceanbase
{
namespace oblogminer
{

struct ObLogMinerColVal
{
  ObLogMinerColVal():
      col_(),
      val_(),
      is_null_(false),
      allocator_(nullptr) { }
  ~ObLogMinerColVal() { destroy(); }
  int init(const ObString &column_name,
      const ObString &value,
      const bool is_null,
      ObIAllocator *alloc);
  void destroy();

  TO_STRING_KV(K(col_), K(val_));

  ColumnName col_;
  ObString val_;
  bool is_null_;
private:
  ObIAllocator *allocator_;
};

typedef ObSEArray<ObLogMinerColVal, 1> ObLogMinerColumnVals;
struct DbAndTableWrapper
{
  DbAndTableWrapper(const char *db_name, const char *tbl_name):
      db_name_(db_name),
      table_name_(tbl_name) {}

  int hash(uint64_t &val) const;

  bool operator==(const DbAndTableWrapper &that) const {
    return db_name_ == that.db_name_ && table_name_ == that.table_name_;
  }

  TO_STRING_KV(
    K_(db_name),
    K_(table_name)
  );

  DbName    db_name_;
  TableName table_name_;
};

// Indicates column conditions of one table
struct ObLogMinerTableColumnCond
{
public:
  static const char *DATABASE_KEY;
  static const char *TABLE_KEY;
  static const char *COLUMN_COND_KEY;
public:
  explicit ObLogMinerTableColumnCond():
      db_name_(), table_name_(), column_conds_(), alloc_(nullptr) { }
  int init(ObIAllocator *alloc, json::Object &obj);
  int init(ObIAllocator *alloc, const ObString &db, const ObString &tbl);
  void destroy();

  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;


  TO_STRING_KV(
    K_(db_name),
    K_(table_name),
    K_(column_conds)
  );

private:
  int add_column_cond_(json::Object &obj);

public:
  DbName    db_name_;
  TableName table_name_;
  ObSEArray<ObLogMinerColumnVals, 1> column_conds_;
private:
  ObIAllocator *alloc_;
};
// expect table cond is a json string with format below:
// [
//   {
//     "database_name":"db1",
//     "table_name":"tbl1",
//     "column_cond":[
//       {
//         "col1":"val1",
//         "col2":"val2"
//       },
//       {
//         "col3":"val3",
//         "col4":null
//       }
//     ]
//   },
//   {
//     "database_name":"db1",
//     "table_name":"tbl2"
//   }
// ]

typedef ObLinearHashMap<DbAndTableWrapper, ObLogMinerTableColumnCond*>  TableColumnCondMap;

struct ObLogMinerMultiTableColumnCond
{
  explicit ObLogMinerMultiTableColumnCond(ObIAllocator *alloc):
    table_column_conds_(), alloc_(alloc) { }
  int init(const char *table_cond_str);
  void destroy();

  TO_STRING_KV("table_column_conds count ", table_column_conds_.count());

private:
  int add_table_cond_(json::Object &obj);

  int parse_json_cond_(const char *table_cond_str);

public:
  TableColumnCondMap table_column_conds_;
private:
  ObIAllocator *alloc_;
};


struct ObLogMinerOpCond
{
public:
  // RecordType need to be processsed
  static const int64_t INSERT_BITCODE = 1 << 0;
  static const int64_t UPDATE_BITCODE = 1 << 1;
  static const int64_t DELETE_BITCODE = 1 << 2;
  static const int64_t DDL_BITCODE = 1 << 3;
  static const int64_t HEARTBEAT_BITCODE = 1 << 4;
  static const int64_t BEGIN_BITCODE = 1 << 5;
  static const int64_t COMMIT_BITCODE = 1 << 6;
  static const int64_t UNKNOWN_BITCODE = 1 << 63;

  static const char *INSERT_OP_STR;
  static const char *UPDATE_OP_STR;
  static const char *DELETE_OP_STR;
  static const char *OP_DELIMITER;

  static int64_t record_type_to_bitcode(const RecordType type);

public:
  ObLogMinerOpCond():
      op_cond_(0) { }
  int init(const char *op_cond);
  void reset();
  bool is_record_type_match(const RecordType type) const;
  TO_STRING_KV(K_(op_cond));
public:
  int64_t op_cond_;
};

}
}

#endif