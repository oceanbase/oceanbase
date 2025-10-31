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
 */

#define USING_LOG_PREFIX SQL

#include "sql/table_format/iceberg/spec/schema_field.h"

#include "share/ob_define.h"
#include "sql/table_format/iceberg/spec/type.h"

namespace oceanbase
{
namespace sql
{
namespace iceberg
{

int SchemaField::to_json_kv_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OZ(J_OBJ_START());
  OZ(databuff_printf(buf, buf_len, pos, R"("id": %d)", field_id_));
  OZ(J_COMMA());
  OZ(databuff_printf(buf, buf_len, pos, R"("name": "%.*s")", name_.length(), name_.ptr()));
  OZ(J_COMMA());
  OZ(databuff_printf(buf, buf_len, pos, R"("required": %s)", STR_BOOL(!optional_)));
  OZ(J_COMMA());
  OZ(databuff_printf(buf, buf_len, pos, R"("type": )"));
  OZ(type_->to_json_kv_string(buf, buf_len, pos));
  if (!doc_.empty()) {
    OZ(J_COMMA());
    OZ(databuff_printf(buf, buf_len, pos, R"("doc": "%.*s")", doc_.length(), doc_.ptr()));
  }
  OZ(J_OBJ_END());
  return ret;
}

} // namespace iceberg

} // namespace sql

} // namespace oceanbase