/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_audit.h"

using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::rpc;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;
using namespace oceanbase::sql::stmt;
using namespace oceanbase::obmysql;
using namespace oceanbase::transaction;

StmtType ObTableAuditUtils::get_stmt_type(ObTableOperationType::Type op_type)
{
  StmtType stmt_type = StmtType::T_MAX;

  switch(op_type) {
    case ObTableOperationType::GET: {
      stmt_type = StmtType::T_KV_GET;
      break;
    }
    case ObTableOperationType::INSERT: {
      stmt_type = StmtType::T_KV_INSERT;
      break;
    }
    case ObTableOperationType::DEL: {
      stmt_type = StmtType::T_KV_DELETE;
      break;
    }
    case ObTableOperationType::UPDATE: {
      stmt_type = StmtType::T_KV_UPDATE;
      break;
    }
    case ObTableOperationType::INSERT_OR_UPDATE: {
      stmt_type = StmtType::T_KV_INSERT_OR_UPDATE;
      break;
    }
    case ObTableOperationType::REPLACE: {
      stmt_type = StmtType::T_KV_REPLACE;
      break;
    }
    case ObTableOperationType::INCREMENT: {
      stmt_type = StmtType::T_KV_INCREMENT;
      break;
    }
    case ObTableOperationType::APPEND: {
      stmt_type = StmtType::T_KV_APPEND;
      break;
    }
    case ObTableOperationType::PUT: {
      stmt_type = StmtType::T_KV_PUT;
      break;
    }
    case ObTableOperationType::REDIS: {
      stmt_type = StmtType::T_REDIS;
      break;
    }
    default: {
      stmt_type = StmtType::T_MAX;
      break;
    }
  }

  return stmt_type;
}

// statement is "multi $op_name $table_name col1, col2, col3"
int64_t ObTableAuditMultiOp::get_stmt_length(const ObString &table_name) const
{
  int64_t len = 0;
  ObString tmp_table_name = table_name.empty() ? ObString::make_string("(null)") : table_name;

  len += strlen("multi"); // "multi"
  len += 1; // blank
  len += strlen(ObTableOperation::get_op_name(op_type_)); // "$op_name"
  len += 1; // blank
  len += tmp_table_name.length(); // "$table_name"
  len += 1; // blank

  if (!ops_.empty()) {
    const ObIArray<ObString> *propertiy_names = ops_.at(0).entity().get_all_properties_names();
    if (OB_NOT_NULL(propertiy_names)) {
      int64_t N = propertiy_names->count();
      for (int64_t index = 0; index < N - 1; ++index) {
        len += propertiy_names->at(index).length(); // col1
        len += 2; // "\"\"" double quote
        len += 2; // ", "
      }
      if (0 < N) {
        len += propertiy_names->at(N - 1).length(); // col_n
        len += 2; // "\"\"" double quote
      }
    }
  }

  return len;
}

int ObTableAuditMultiOp::generate_stmt(const ObString &table_name, char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObString tmp_table_name = table_name.empty() ? ObString::make_string("(null)") : table_name;

  const char *op_name = ObTableOperation::get_op_name(op_type_);
  int64_t prefix_len = MULTI_PREFIX_LEN + strlen(op_name) + tmp_table_name.length() + 3; // 3 * ' '
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is bull", KR(ret));
  } else if (buf_len <= prefix_len) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer not enough", K(ret), K(buf_len), K(strlen(op_name)), K(tmp_table_name));
  } else {
    int64_t n = snprintf(buf + pos, prefix_len + 1, "multi %s %s ", op_name, tmp_table_name.ptr()); // "multi $op_name $table_name"
    pos += prefix_len;
    if (!ops_.empty()) {
      const ObIArray<ObString> *propertiy_names = ops_.at(0).entity().get_all_properties_names();
      if (OB_NOT_NULL(propertiy_names)) {
        int64_t N = propertiy_names->count();
        for (int64_t index = 0; index < N - 1; ++index) {
          BUF_PRINTO(propertiy_names->at(index)); // pos will change in BUF_PRINTO
          J_COMMA(); // ", "
        }
        if (0 < N) {
          BUF_PRINTO(propertiy_names->at(N - 1));
        }
      }
    }
  }

  return ret;
}

// statement is "$cmd_name"
int64_t ObTableAuditRedisOp::get_stmt_length(const ObString &table_name) const
{
  UNUSED(table_name);
  ObString tmp_cmd_name = cmd_name_.empty() ? ObString::make_string("(null)") : cmd_name_;
  return tmp_cmd_name.length();
}

// statement is "$cmd_name"
int ObTableAuditRedisOp::generate_stmt(const ObString &table_name, char *buf, int64_t buf_len, int64_t &pos) const
{
  UNUSED(table_name);
  int ret = OB_SUCCESS;
  ObString tmp_cmd_name = cmd_name_.empty() ? ObString::make_string("(null)") : cmd_name_;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buf is bull", KR(ret));
  } else if (buf_len - pos < tmp_cmd_name.length() + 1) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("buffer not enough", K(ret), K(buf_len), K(pos), K(tmp_cmd_name));
  } else {
    // snprintf return value:
    // buf_size > str_size, return str_size
    // buf_size < str_size, truncate str and return str_size
    int64_t n = snprintf(buf + pos, tmp_cmd_name.length() + 1, "%s", tmp_cmd_name.ptr());
    pos += tmp_cmd_name.length();
  }

  return ret;
}