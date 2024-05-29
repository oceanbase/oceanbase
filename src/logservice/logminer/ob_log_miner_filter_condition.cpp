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

#define USING_LOG_PREFIX LOGMNR

#include "ob_log_miner_filter_condition.h"

namespace oceanbase
{
namespace oblogminer
{

int ObLogMinerColVal::init(const ObString &column_name,
    const ObString &value,
    const bool is_null,
    ObIAllocator *alloc)
{
  int ret = OB_SUCCESS;
  char *value_buf = nullptr;
  allocator_ = alloc;
  if (OB_ISNULL(alloc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get null allocator, unexpected", K(alloc), K(column_name), K(value));
  } else if (OB_FAIL(col_.assign(column_name))) {
    LOG_ERROR("assign column_name failed", K(column_name));
  } else {
    if (is_null) {
      is_null_ = true;
    } else {
      if (OB_ISNULL(value_buf = static_cast<char *>(alloc->alloc(value.length() + 1)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("allocate memory failed for value buf", K(value));
      } else {
        MEMCPY(value_buf, value.ptr(), value.length());
        value_buf[value.length()] = '\0';
        val_.assign(value_buf, value.length());
        is_null_ = false;
      }
    }
  }
  return ret;
}

void ObLogMinerColVal::destroy()
{
  col_.reset();
  if (nullptr != allocator_) {
    allocator_->free(val_.ptr());
  }
  allocator_ = nullptr;
}

int DbAndTableWrapper::hash(uint64_t &val) const
{
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(db_name_.ptr(), db_name_.size(), hash_val);
    hash_val = common::murmurhash(table_name_.ptr(), table_name_.size(), hash_val);
    return OB_SUCCESS;
  }

const char *ObLogMinerTableColumnCond::DATABASE_KEY = "database_name";
const char *ObLogMinerTableColumnCond::TABLE_KEY = "table_name";
const char *ObLogMinerTableColumnCond::COLUMN_COND_KEY = "column_cond";

int ObLogMinerTableColumnCond::init(ObIAllocator *alloc, json::Object &obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(alloc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("allocator fo oblogminer table cond is null", K(alloc));
  } else {
    alloc_ = alloc;
  }
  DLIST_FOREACH(it, obj) {
    if (OB_ISNULL(it)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("get nulll pair in json, unexpected", K(it));
    } else if (0 == it->name_.case_compare(DATABASE_KEY)) {
      if (OB_ISNULL(it->value_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("get database key get null value", "key", it->name_,
            "value", it->value_);
      } else if (json::JT_STRING != it->value_->get_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("get database key but value type is not expected", "key", it->name_,
            "value", it->value_, "value_type", it->value_->get_type());
      } else if (! db_name_.is_empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("get database key but db_name has been set already", K(db_name_), "key", it->name_,
            "value", it->value_->get_string());
      } else if (OB_FAIL(db_name_.assign(it->value_->get_string()))) {
        LOG_ERROR("assign db_name failed", K(db_name_), "value", it->value_->get_string());
      } else {
        // succ
      }
    } else if (0 == it->name_.case_compare(TABLE_KEY)) {
      if (OB_ISNULL(it->value_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("get table key get null value", "key", it->name_,
            "value", it->value_);
      } else if (json::JT_STRING != it->value_->get_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("get table key but value type is not expected", "key", it->name_,
            "value", it->value_, "value_type", it->value_->get_type());
      } else if (! table_name_.is_empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("get table key but table_name has been set already", K(table_name_), "key", it->name_,
            "value", it->value_->get_string());
      } else if (OB_FAIL(table_name_.assign(it->value_->get_string()))) {
        LOG_ERROR("assign table_name failed", K(table_name_), "value", it->value_->get_string());
      } else {
        // succ
      }
    } else if (0 == it->name_.case_compare(COLUMN_COND_KEY)) {
      if (OB_NOT_NULL(it->value_)) {
        if (json::JT_ARRAY != it->value_->get_type()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("get condtion key, expect value of array type, but got unexpected type", "key", it->name_,
              "value_type", it->value_->get_type());
        } else {
          DLIST_FOREACH(col_cond, it->value_->get_array()) {
            if (OB_ISNULL(col_cond)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_ERROR("column_cond key is specified, get null column cond, unexpetced", "key", it->name_,
                  K(col_cond));
            } else if (json::JT_OBJECT != col_cond->get_type()) {
              ret = OB_INVALID_ARGUMENT;
              LOG_ERROR("get column cond, but its type is not as expected, object type is expected",
                  "column_cond_type", col_cond->get_type());
            } else if (OB_FAIL(add_column_cond_(col_cond->get_object()))) {
              LOG_ERROR("add column condition faile", "col_cond", col_cond->get_object());
            } else {
              // succ
            }
          }
        }
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("get invalid key for table cond", "key", it->name_);
    }
  }

  if (table_name_.is_empty() || db_name_.is_empty() || column_conds_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("It's unexpected to not specify column_conds or table_name or db_name", K(table_name_),
        K(db_name_), K(column_conds_));
  }
  return ret;
}

int ObLogMinerTableColumnCond::init(ObIAllocator *alloc,
    const ObString &db,
    const ObString &tbl)
{
  int ret = OB_SUCCESS;

  alloc_ = alloc;
  if (OB_FAIL(db_name_.assign(db))) {
    LOG_ERROR("failed to assign db into db_name", K(db));
  } else if (OB_FAIL(table_name_.assign(tbl))) {
    LOG_ERROR("failed to assign tbl into table_name", K(tbl));
  }

  return ret;
}

int ObLogMinerTableColumnCond::add_column_cond_(json::Object &obj)
{
  int ret = OB_SUCCESS;
  ObLogMinerColumnVals col_cond;
  DLIST_FOREACH(col_val, obj) {
    ObLogMinerColVal col_val_pair;
    bool is_null_value = json::JT_NULL == col_val->value_->get_type();
    if (OB_ISNULL(col_val)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("get null col_val, unexpected for json parser", K(col_val));
    } else if (OB_ISNULL(col_val->value_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("get invalid column condition, whose value for column is null", "column_name", col_val->name_);
    } else if (json::JT_STRING != col_val->value_->get_type() && !is_null_value) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("get unexpected column value type", "column_name", col_val->name_,
          "column_val_type", col_val->value_->get_type());
    } else if (OB_FAIL(col_val_pair.init(col_val->name_,
        col_val->value_->get_string(), is_null_value, alloc_))) {
      LOG_ERROR("column_name-column_value pair failed to init", "name", col_val->name_,
          "value", col_val->value_->get_string());
    } else if (OB_FAIL(col_cond.push_back(col_val_pair))) {
      LOG_ERROR("failed to push back col_val_pair into column_conds", K(col_val_pair));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(column_conds_.push_back(col_cond))) {
      LOG_ERROR("failed to push back column_cond into column cond array", K(col_cond));
    }
  }

  return ret;
}

void ObLogMinerTableColumnCond::destroy()
{
  db_name_.reset();
  table_name_.reset();
  column_conds_.destroy();
  alloc_ = nullptr;
}

uint64_t ObLogMinerTableColumnCond::hash() const
{
  uint64_t hash_val = 0;
  hash_val = common::murmurhash(db_name_.ptr(), db_name_.size(), hash_val);
  hash_val = common::murmurhash(table_name_.ptr(), table_name_.size(), hash_val);
  return hash_val;
}

int ObLogMinerTableColumnCond::hash(uint64_t &hash_val) const
{
  hash_val = hash();
  return OB_SUCCESS;
}

int ObLogMinerMultiTableColumnCond::add_table_cond_(json::Object &obj)
{
  int ret = OB_SUCCESS;
  ObLogMinerTableColumnCond *cond =
      static_cast<ObLogMinerTableColumnCond*>(alloc_->alloc(sizeof(ObLogMinerTableColumnCond)));
  if (OB_ISNULL(cond)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory for ObLogMinerTableColumnCond failed", K(cond));
  } else {
    cond = new (cond) ObLogMinerTableColumnCond;
    if (OB_FAIL(cond->init(alloc_, obj))) {
      LOG_ERROR("table_cond init failed", KP(cond));
    } else if (OB_FAIL(table_column_conds_.insert(
        DbAndTableWrapper(cond->db_name_.ptr(), cond->table_name_.ptr()), cond))) {
      LOG_ERROR("failed to push back table cond into table_cond_array", K(cond));
    }
  }

  return ret;
}

int ObLogMinerMultiTableColumnCond::parse_json_cond_(const char *table_cond_str)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::JSON_PARSER);
  json::Parser parser;
  json::Value *root = nullptr;
  if (OB_ISNULL(table_cond_str)) {
    LOG_INFO("get empty table cond str, don't filter any table", K(table_cond_str));
  } else if (OB_FAIL(parser.init(&allocator))) {
    LOG_ERROR("init json parser for multi table cond failed");
  } else if (OB_FAIL(parser.parse(table_cond_str, strlen(table_cond_str), root))) {
    LOG_ERROR("parse table_cond failed", K(table_cond_str), K(root));
    // rewrite the error code
    ret = OB_INVALID_ARGUMENT;
  } else {
    if (json::JT_ARRAY == root->get_type()) {
      DLIST_FOREACH(it, root->get_array()) {
        if (OB_ISNULL(it)) {
          ret = OB_ERR_UNEXPECTED;
        } else if (json::JT_OBJECT == it->get_type()) {
          if (OB_FAIL(add_table_cond_(it->get_object()))) {
            LOG_ERROR("failed to add table cond", K(table_cond_str));
          } else {
            // succ
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("get unexpected table cond type in json", "type", it->get_type());
        }
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("get unexpected root type in json, expected array", "type", root->get_type());
    }
  }

  return ret;
}

int ObLogMinerMultiTableColumnCond::init(const char *table_cond_str)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObModIds::JSON_PARSER);
  json::Parser parser;
  json::Value *root = nullptr;
  if (OB_ISNULL(table_cond_str) || 0 == strcmp(table_cond_str, "")) {
    LOG_INFO("get empty table cond str, don't filter any table", K(table_cond_str));
  } else if (OB_FAIL(table_column_conds_.init("LogMnrColCond"))) {
    LOG_ERROR("failed to init table_column_conds_", K(ret));
  } else if (OB_FAIL(parse_json_cond_(table_cond_str))) {
    LOG_ERROR("failed to parse json cond", K(table_cond_str));
  }

  return ret;
}

void ObLogMinerMultiTableColumnCond::destroy()
{
  table_column_conds_.destroy();
}

const char *ObLogMinerOpCond::INSERT_OP_STR = "insert";
const char *ObLogMinerOpCond::UPDATE_OP_STR = "update";
const char *ObLogMinerOpCond::DELETE_OP_STR = "delete";
const char *ObLogMinerOpCond::OP_DELIMITER = "|";

int64_t ObLogMinerOpCond::record_type_to_bitcode(const RecordType type) {
  int64_t ret = UNKNOWN_BITCODE;
  switch (type) {
    case EINSERT: {
      ret = INSERT_BITCODE;
      break;
    }

    case EUPDATE: {
      ret = UPDATE_BITCODE;
      break;
    }

    case EDELETE: {
      ret = DELETE_BITCODE;
      break;
    }

    case EDDL: {
      ret = DDL_BITCODE;
      break;
    }

    case HEARTBEAT: {
      ret = HEARTBEAT_BITCODE;
      break;
    }

    case EBEGIN: {
      ret = BEGIN_BITCODE;
      break;
    }

    case ECOMMIT: {
      ret = COMMIT_BITCODE;
      break;
    }

    default: {
      ret = UNKNOWN_BITCODE;
      break;
    }
  }
  return ret;
}

int ObLogMinerOpCond::init(const char *op_cond)
{
  int ret = OB_SUCCESS;
  char *saveptr = nullptr;
  char *op = nullptr;
  int64_t arg_len = op_cond == nullptr ? 0 : strlen(op_cond);
  char *tmp_buffer = static_cast<char*>(ob_malloc(arg_len + 1, "LogMnrOpCond"));
  if (OB_ISNULL(op_cond)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ObLogMinerOpCond init with a null op_cond, unexpected", K(op_cond), K(arg_len));
  } else if (OB_ISNULL(tmp_buffer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate temp buffer failed", K(tmp_buffer), K(arg_len), K(op_cond));
  } else {
    MEMCPY(tmp_buffer, op_cond, arg_len);
    tmp_buffer[arg_len] = '\0';
    // ddl, begin, commit, heartbeat need to be processed by default
    op_cond_ |= (DDL_BITCODE | BEGIN_BITCODE | COMMIT_BITCODE | HEARTBEAT_BITCODE);
    op = STRTOK_R(tmp_buffer, OP_DELIMITER, &saveptr);
    while (nullptr != op && OB_SUCC(ret)) {
      op = str_trim(op);
      if (0 == strcasecmp(op, INSERT_OP_STR)) {
        op_cond_ |= INSERT_BITCODE;
      } else if (0 == strcasecmp(op, UPDATE_OP_STR)) {
        op_cond_ |= UPDATE_BITCODE;
      } else if (0 == strcasecmp(op, DELETE_OP_STR)) {
        op_cond_ |= DELETE_BITCODE;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_ERROR("get unexpected operation", K(op_cond_), K(op_cond), K(op));
        LOGMINER_STDOUT("parse operations failed\n");
        break;
      }
      op = STRTOK_R(nullptr , OP_DELIMITER, &saveptr);
    }
  }

  if (OB_NOT_NULL(tmp_buffer)) {
    ob_free(tmp_buffer);
  }
  return ret;
}

bool ObLogMinerOpCond::is_record_type_match(const RecordType type) const
{
  bool bret = false;
  const int64_t record_bitcode = record_type_to_bitcode(type);
  if (record_bitcode & op_cond_) {
    bret = true;
  }
  return bret;
}

void ObLogMinerOpCond::reset()
{
  op_cond_ = 0;
}

}
}