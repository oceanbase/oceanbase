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

#define USING_LOG_PREFIX RS
#include "rootserver/ob_schema2ddl_sql.h"

#include "lib/string/ob_sql_string.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_column_schema.h"
#include "share/ob_get_compat_mode.h"
#include "common/ob_smart_var.h"

namespace oceanbase
{
namespace rootserver
{
using namespace common;
using namespace share::schema;

ObSchema2DDLSql::ObSchema2DDLSql()
{
}

ObSchema2DDLSql::~ObSchema2DDLSql()
{
}

int ObSchema2DDLSql::convert(
  const ObTableSchema &orig_table_schema,
  char *sql_buf,
  const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  HEAP_VARS_2((char[OB_MAX_SQL_LENGTH], primary_key_buf),
              (char[OB_MAX_SQL_LENGTH], type_str_buf)) {
    int64_t sql_buf_write = 0;
    int64_t key_buf_write = 0;
    int64_t n = 0;
    bool is_first_rowkey = true;
    ObTableSchema table_schema;

    if (OB_FAIL(table_schema.assign(orig_table_schema))) {
      LOG_WARN("fail to assign table schema", K(ret), K(orig_table_schema));
    } else if (!table_schema.is_valid() || NULL == sql_buf || buf_size <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(table_schema), KP(sql_buf), K(buf_size), K(ret));
    }

    // table name part
    if (OB_SUCC(ret)) {
      n = snprintf(sql_buf + sql_buf_write, buf_size - sql_buf_write,
          "create table %s(", table_schema.get_table_name());
      if (n < 0) {
        ret = OB_ERR_SYS;
        LOG_WARN("snprintf failed", K(n), K(ret));
      } else if (n >= buf_size - sql_buf_write) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("sql buf is not long enough:", "remain_buf_size", buf_size - sql_buf_write,
            "need", n, K(ret));
      } else {
        sql_buf_write += n;
      }
    }

    // columns part
    for (ObTableSchema::const_column_iterator iter = table_schema.column_begin();
        OB_SUCCESS == ret && iter != table_schema.column_end(); iter++) {
      // avoid mysql 1071: Specified key was too long; max key length is 767 bytes
      const ObColumnSchemaV2 *column = *iter;
      if (NULL == column) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column is null", KP(column), K(ret));
      } else {
        if (column->get_rowkey_position() > 0 && ObStringTC == column->get_data_type_class()) {
          (const_cast<ObColumnSchemaV2*>(column))->set_data_length(
              std::min(column->get_data_length(), 64));
        }
        if (OB_FAIL(type2str(*column, type_str_buf, sizeof(type_str_buf)))) {
          LOG_WARN("transform type to str failed", "column", *column, K(ret));
        } else {
          n = snprintf(sql_buf + sql_buf_write, buf_size - sql_buf_write,
              "%s`%s` %s", table_schema.column_begin() != iter ? ", " : "",
              column->get_column_name(), type_str_buf);
          if (n < 0) {
            ret = OB_ERR_SYS;
            LOG_WARN("snprintf failed", K(n), K(ret));
          } else if (n >= buf_size - sql_buf_write) {
            ret = OB_BUF_NOT_ENOUGH;
            LOG_WARN("sql buf is not long enough", "remain_buf_size", buf_size - sql_buf_write,
                "need", n, K(ret));
          } else {
            sql_buf_write += n;
          }
        }

        if (OB_SUCC(ret)) {
          if (0 < column->get_rowkey_position()) {
            n = snprintf(primary_key_buf + key_buf_write, OB_MAX_SQL_LENGTH - key_buf_write,
                "%s`%s`", is_first_rowkey ? "" : ", ", column->get_column_name());
            is_first_rowkey = false;
            if (n < 0) {
              ret = OB_ERR_SYS;
              LOG_WARN("snprintf failed", K(n), K(ret));
            } else if (n >= OB_MAX_SQL_LENGTH - key_buf_write) {
              ret = OB_BUF_NOT_ENOUGH;
              LOG_WARN("primary key buf is not long enough",
                  "remain_buf_size", OB_MAX_SQL_LENGTH - key_buf_write, "need", n, K(ret));
            } else {
              key_buf_write += n;
            }
          }
        }
      }
    }

    // partition definition part
    ObSqlString part_def;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(part_def.assign(" "))) {
      LOG_WARN("assign define failed", K(ret));
    } else {
      //TODO This place never real support partitioned table
      if (table_schema.get_all_part_num() > 1) {
        ObSqlString partition_key_str;
        const ObPartitionKeyInfo &partition_key_info =
            table_schema.get_partition_key_info();
        for (int64_t i = 0; OB_SUCC(ret) && i < partition_key_info.get_size(); ++i) {
          const uint64_t column_id = partition_key_info.get_column(i)->column_id_;
          if (OB_FAIL(partition_key_str.append_fmt("%s%s", (0 == i) ? "" : " ,",
              table_schema.get_column_schema(column_id)->get_column_name()))) {
            LOG_WARN("append_fmt failed", K(ret));
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(part_def.append_fmt("partition by key (%s) partitions %ld",
              partition_key_str.ptr(), table_schema.get_all_part_num()))) {
            LOG_WARN("append string failed", K(ret));
          }
        }
      }
    }

    // primary key part
    if (OB_SUCC(ret)) {
      if (key_buf_write > 0) {
        n = snprintf(sql_buf + sql_buf_write, buf_size - sql_buf_write,
            ", primary key(%s)) engine = innodb%s", primary_key_buf, part_def.ptr());
      } else {
        n = snprintf(sql_buf + sql_buf_write, buf_size - sql_buf_write,
            ") engine = innodb%s", part_def.ptr());
      }
      if (n < 0) {
        ret = OB_ERR_SYS;
        LOG_WARN("snprintf failed", K(n), K(ret));
      } else if (n >= buf_size - sql_buf_write) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("sql buf is not long enough", "remain_buf_size", buf_size - sql_buf_write,
            "need", n, K(ret));
      }
    }
  }
  return ret;
}

int ObSchema2DDLSql::type2str(
  const ObColumnSchemaV2 &column_schema,
  char *str_buf,
  const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  int64_t n = 0;
  int64_t nwrite = 0;
  if (!column_schema.is_valid() || NULL == str_buf || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(column_schema), KP(str_buf), K(buf_size), K(ret));
  } else {
    bool is_oracle_mode = false;
    if (OB_FAIL(share::ObCompatModeGetter::check_is_oracle_mode_with_table_id(
        column_schema.get_tenant_id(), column_schema.get_table_id(), is_oracle_mode))) {
      LOG_WARN("fail to check is oracle mode", K(ret));
    } else {
      switch (column_schema.get_data_type()) {
        case ObTinyIntType: {
          n = snprintf(str_buf, buf_size, "tinyint");
          break;
        }
        case ObIntType: {
          n = snprintf(str_buf, buf_size, "bigint");
          break;
        }
        case ObTimestampType:
        case ObTimestampNanoType: {
          n = snprintf(str_buf, buf_size, "timestamp");
          break;
        }
        case ObTimestampTZType: {
          n = snprintf(str_buf, buf_size, "timestamp with time zone");
          break;
        }
        case ObTimestampLTZType: {
          n = snprintf(str_buf, buf_size, "timestamp with local time zone");
          break;
        }
        case ObVarcharType: {
          // avoid mysql 1074: Column length too big for column 'view_definition' (max = 21845)
          int64_t max_varchar_length = 512;
          if (0 == STRCMP(column_schema.get_column_name(), "view_definition")) {
            max_varchar_length = 8192;
          }
          if (is_oracle_mode) {
            if (is_oracle_byte_length(is_oracle_mode, column_schema.get_length_semantics())) {
              n = snprintf(str_buf, buf_size, "varchar2(%d %s)",
                  std::min(column_schema.get_data_length(), static_cast<int32_t>(max_varchar_length)),
                  get_length_semantics_str(column_schema.get_length_semantics()));
            } else {
              n = snprintf(str_buf, buf_size, "varchar2(%d)",
                  std::min(column_schema.get_data_length(), static_cast<int32_t>(max_varchar_length)));
            }
          } else {
            n = snprintf(str_buf, buf_size, "varchar(%d)",
                std::min(column_schema.get_data_length(), static_cast<int32_t>(max_varchar_length)));
          }
          break;
        }
        case ObRawType: {
          n = snprintf(str_buf, buf_size, "raw(%d)",
              std::min(column_schema.get_data_length(),
                       static_cast<int32_t>(OB_MAX_ORACLE_RAW_SQL_COL_LENGTH)));
          break;
        }
        case ObFloatType: {
          n = snprintf(str_buf, buf_size, is_oracle_mode ? "binary_float" : "float");
          break;
        }
        case ObDoubleType: {
          n = snprintf(str_buf, buf_size, is_oracle_mode ? "binary_double" : "double");
          break;
        }
        case ObUInt64Type: {
          n = snprintf(str_buf, buf_size, "bigint unsigned");
          break;
        }
        case ObNumberType: {
          n = snprintf(str_buf, buf_size, "decimal(%d,%d)",
              column_schema.get_data_precision(), column_schema.get_data_scale());
          break;
        }
        case ObNumberFloatType: {
          n = snprintf(str_buf, buf_size, "float(%d)", column_schema.get_data_precision());
          break;
        }
        case ObLongTextType: {
          n = snprintf(str_buf, buf_size, "longtext");
          break;
        }
        case ObJsonType: {
          n = snprintf(str_buf, buf_size, "json");
          break;
        }
        case ObGeometryType: {
          n = snprintf(str_buf, buf_size, "geometry");
          break;
        }
        default: {
          break;
        }
      }
      if (n < 0) {
        ret = OB_ERR_SYS;
        LOG_WARN("snprintf failed", K(n), K(ret));
      } else if (0 == n) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("the type is not supported", "data_type", column_schema.get_data_type(), K(ret));
      } else if (buf_size <= n) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("type string buffer is not enough", "data_type", column_schema.get_data_type(),
            K(buf_size), "need_size", n, K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    nwrite += n;
    n = 0;
    if (!column_schema.is_nullable() && 0 != STRCMP(column_schema.get_column_name(), "gmt_create")
        && 0 != STRCMP(column_schema.get_column_name(), "gmt_modified")) {
      n = snprintf(str_buf + nwrite, buf_size - nwrite, " not null");
    } else if (0 == STRCMP(column_schema.get_column_name(), "gmt_create")) {
      if (column_schema.is_nullable()) {
        n = snprintf(str_buf + nwrite, buf_size - nwrite, " null");
      }
    } else if (0 == STRCMP(column_schema.get_column_name(), "gmt_modified")) {
      n = snprintf(str_buf + nwrite, buf_size - nwrite,
          " not null default current_timestamp on update current_timestamp");
    }

    if (n < 0) {
      ret = OB_ERR_SYS;
      LOG_WARN("snprintf failed", K(n), K(ret));
    } else if (buf_size - nwrite <= n) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("type string buffer is not enough", "data_type", column_schema.get_data_type(),
          K(buf_size), "need_size", n, K(ret));
    }
  }

  // default value
  if (OB_SUCC(ret)) {
    nwrite += n;
    n = 0;
    if (!column_schema.get_orig_default_value().is_null()
        && column_schema.get_orig_default_value().is_valid_type()
        && column_schema.get_data_type() != ObLongTextType) {
      if (ObIntType == column_schema.get_data_type()) {
        int64_t value = 0;
        if (OB_FAIL(column_schema.get_orig_default_value().get_int(value))) {
          LOG_WARN("get_int failed", "obj", column_schema.get_orig_default_value(), K(ret));
        } else {
          n = snprintf(str_buf + nwrite, buf_size - nwrite, " default %ld", value);
        }
      } else if (ObTinyIntType == column_schema.get_data_type()) {
        int8_t value = 0;
        if (OB_FAIL(column_schema.get_orig_default_value().get_tinyint(value))) {
          LOG_WARN("get_int failed", "obj", column_schema.get_orig_default_value(), K(ret));
        } else {
          n = snprintf(str_buf + nwrite, buf_size - nwrite, " default %d", value);
        }

      } else if (ObUInt64Type == column_schema.get_data_type()) {
        uint64_t value = 0;
        if (OB_FAIL(column_schema.get_orig_default_value().get_uint64(value))) {
          LOG_WARN("get_int failed", "obj", column_schema.get_orig_default_value(), K(ret));
        } else {
          n = snprintf(str_buf + nwrite, buf_size - nwrite, " default %ld", value);
        }
      } else if (ObVarcharType == column_schema.get_data_type()) {
        ObString value;
        if (OB_FAIL(column_schema.get_orig_default_value().get_varchar(value))) {
          LOG_WARN("get_varchar failed", "obj", column_schema.get_orig_default_value(), K(ret));
        } else {
          n = snprintf(str_buf + nwrite, buf_size - nwrite,
              " default '%.*s'", value.length(), value.ptr());
        }
      } else if (ObRawType == column_schema.get_data_type()) {
        int64_t tmp_n = 0;
        if ((tmp_n = snprintf(str_buf + nwrite, buf_size - nwrite, " default '")) < 0) {
          ret = OB_ERR_SYS;
          LOG_WARN("snprintf failed", K(tmp_n), K(ret));
        } else {
          nwrite += tmp_n;
          n = nwrite;
          if (OB_FAIL(column_schema.get_cur_default_value().print_plain_str_literal(
                      str_buf , buf_size - nwrite, nwrite, NULL))) {
            LOG_WARN("failed to print cur default value", K(ret), K(buf_size - nwrite), K(nwrite));
          } else {
            n = nwrite - n + tmp_n;
            if ((tmp_n = snprintf(str_buf + nwrite, buf_size - nwrite, "'")) < 0) {
              ret = OB_ERR_SYS;
              LOG_WARN("snprintf failed", K(tmp_n), K(ret));
            } else {
              n += tmp_n;
            }
          }
        }
      } else {
        // TODO(jingqian): support other type
      }
      if (OB_FAIL(ret)) {
      } else if (n < 0) {
        ret = OB_ERR_SYS;
        LOG_WARN("snprintf failed", K(n), K(ret));
      } else if (0 == n) {
        if (ObTimestampType != column_schema.get_data_type()) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("the type is not supported",
              "data_type", column_schema.get_data_type(), K(ret));
        }
      } else if (buf_size <= n) {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("type string buffer is not enough", "data_type", column_schema.get_data_type(),
            K(buf_size), "need_size", n, K(ret));
      }
    }
  }

  return ret;
}

} //end namespace rootserver
} //end namespace oceanbase
