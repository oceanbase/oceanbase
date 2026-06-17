/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "share/schema/ob_schema_printer.h"
#include "share/schema/ob_mview_info.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_server_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
using namespace std;
using namespace common;

int ObSchemaPrinter::print_materialized_view_definition(const uint64_t tenant_id,
                                                        const uint64_t table_id,
                                                        char *buf,
                                                        const int64_t &buf_len,
                                                        int64_t &pos,
                                                        const ObTimeZoneInfo *tz_info,
                                                        bool agent_mode,
                                                        ObSQLMode sql_mode) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "buf is null", KR(ret));
  } else if (OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_SCHEMA_LOG(WARN, "buf_len should bigger than 0", KR(ret));
  } else {
    const ObTableSchema *table_schema = nullptr;
    const ObTableSchema *container_table_schema = nullptr;
    uint64_t container_table_id = OB_INVALID_ID;
    ObMViewInfo mview_info;
    bool is_oracle_mode = false;
    common::ObSEArray<uint64_t, 16> column_ids;
    if (OB_FAIL(schema_guard_.get_table_schema(tenant_id, table_id, table_schema))) {
      SHARE_SCHEMA_LOG(WARN, "fail to get table schema", KR(ret), K(tenant_id), K(table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      SHARE_SCHEMA_LOG(WARN, "Unknow table", KR(ret), K(table_id));
    } else if (OB_INVALID_ID == (container_table_id = table_schema->get_data_table_id())) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "fail to get container_table_id", KR(ret), K(tenant_id), K(table_id));
    } else if (OB_FAIL(schema_guard_.get_table_schema(tenant_id, container_table_id, container_table_schema))) {
      SHARE_SCHEMA_LOG(WARN, "fail to get container_table_schema", KR(ret), K(tenant_id), K(table_id), K(container_table_id));
    } else if (OB_ISNULL(container_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      SHARE_SCHEMA_LOG(WARN, "Unknow container table", KR(ret), K(table_id), K(container_table_id));
    } else if (OB_FAIL(table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to check oracle mode", KR(ret), KPC(table_schema));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "CREATE MATERIALIZED VIEW "))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print materialized view definition", KR(ret));
    } else if (OB_FAIL(print_identifier(buf, buf_len, pos, table_schema->get_table_name(), is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print materialized view name", KR(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " "))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print space", KR(ret));
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "("))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print view definition", K(ret));
    } else if (OB_FAIL(table_schema->get_column_ids(column_ids))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print view definition", K(ret));
    } else if (OB_FAIL(print_column_list(*table_schema, column_ids, buf, buf_len, pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print view definition", K(ret));
    }
    if (OB_SUCC(ret) && !strict_compat_) {
      const ObRowkeyInfo &rowkey_info = container_table_schema->get_rowkey_info();
      bool is_first_col = true;
      for (int64_t j = 0; OB_SUCC(ret) && j < rowkey_info.get_size(); ++j) {
        const ObColumnSchemaV2 *col = nullptr;
        if (OB_ISNULL(rowkey_info.get_column(j))) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_SCHEMA_LOG(WARN, "fail to get column", KR(ret), K(tenant_id));
        } else if (OB_ISNULL(col = schema_guard_.get_column_schema(tenant_id,
                                                    container_table_id,
                                                    rowkey_info.get_column(j)->column_id_))) {
          ret = OB_ERR_UNEXPECTED;
          SHARE_SCHEMA_LOG(WARN, "fail to get column schema", KR(ret), K(tenant_id),
                      "column_id", rowkey_info.get_column(j)->column_id_);
        } else if (OB_SUCC(ret)
                   && col->get_column_id() != OB_HIDDEN_SESSION_ID_COLUMN_ID
                   && !col->is_shadow_column()
                   && !col->is_hidden()) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s%.*s%s",
                                      is_first_col ? ", PRIMARY KEY (" : "",
                                      col->get_column_name_str().length(),
                                      col->get_column_name_str().ptr(),
                                      j < rowkey_info.get_size() - 1 ? ", " : ""))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print primary key", KR(ret), K(col->get_column_name()));
          }
          is_first_col = false;
          if (OB_SUCC(ret) && rowkey_info.get_size() - 1 == j
              && OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print materialized view rowkey", KR(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ") "))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print view definition", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(print_table_definition_table_options(*container_table_schema,
                                                            buf,
                                                            buf_len,
                                                            pos,
                                                            false,
                                                            agent_mode,
                                                            sql_mode))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print table options", KR(ret), K(*container_table_schema));
    } else if (!strict_compat_
               && OB_FAIL(print_table_definition_partition_options(*container_table_schema,
                                                                   buf,
                                                                   buf_len,
                                                                   pos,
                                                                   agent_mode,
                                                                   tz_info))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print partition options", KR(ret), K(*container_table_schema));
    } else if (OB_FAIL(print_table_definition_column_group(*container_table_schema,
                                                            buf,
                                                            buf_len,
                                                            pos))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print materialized view column group", KR(ret), K(*container_table_schema));
    } else if (OB_FAIL(ObMViewInfo::fetch_mview_info(*GCTX.sql_proxy_, tenant_id, table_id, mview_info))) {
      SHARE_SCHEMA_LOG(WARN, "fail to fecth materialized view info", KR(ret), K(table_id));
    } else if (!strict_compat_) {
      switch (mview_info.get_refresh_method()) {
        case ObMVRefreshMethod::NEVER:
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " NEVER REFRESH "))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print refresh method never", KR(ret));
          }
          break;
        case ObMVRefreshMethod::COMPLETE:
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " REFRESH COMPLETE "))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print refresh method complete", KR(ret));
          }
          break;
        case ObMVRefreshMethod::FAST:
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " REFRESH FAST "))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print refresh method fast", KR(ret));
          }
          break;
        case ObMVRefreshMethod::FORCE:
          if (OB_FAIL(databuff_printf(buf, buf_len, pos, " REFRESH FORCE "))) {
            SHARE_SCHEMA_LOG(WARN, "fail to print refresh method force", KR(ret));
          }
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SHARE_SCHEMA_LOG(WARN, "unsupported refresh method", KR(ret), K(mview_info.get_refresh_method()));
          break;
      }
      if (OB_SUCC(ret) && mview_info.get_refresh_dop() > 0) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, "PARALLEL %ld ",
                                    mview_info.get_refresh_dop()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print materialized view refresh parallel", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        switch (mview_info.get_refresh_mode()) {
          case ObMVRefreshMode::NEVER:
            // nothing to print
            break;
          case ObMVRefreshMode::DEMAND:
          case ObMVRefreshMode::MAJOR_COMPACTION:
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, "ON DEMAND "))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print refresh mode", KR(ret));
            }
            break;
          case ObMVRefreshMode::COMMIT:
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, "ON COMMIT "))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print refresh mode", KR(ret));
            }
            break;
          case ObMVRefreshMode::STATEMENT:
            if (OB_FAIL(databuff_printf(buf, buf_len, pos, "ON STATEMENT "))) {
              SHARE_SCHEMA_LOG(WARN, "fail to print refresh mode", KR(ret));
            }
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            SHARE_SCHEMA_LOG(WARN, "unsupported refresh method", KR(ret), K(mview_info.get_refresh_mode()));
            break;
        }
      }
    }
    if (OB_SUCC(ret) && !strict_compat_) {
      if (OB_NOT_NULL(mview_info.get_refresh_next().ptr())) {
        if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                    "START WITH sysdate%s",
                                    is_oracle_mode ? " + 0" : "()"))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print materialized view refresh start", KR(ret));
        } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " NEXT %.*s ",
                                           mview_info.get_refresh_next().length(),
                                           mview_info.get_refresh_next().ptr()))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print mv refresh next", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (table_schema->mv_enable_query_rewrite()
            && OB_FAIL(databuff_printf(buf, buf_len, pos,
                                       "ENABLE QUERY REWRITE "))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print materialized view enable qurey rewrite", KR(ret));
        } else if (table_schema->mv_on_query_computation()
                   && OB_FAIL(databuff_printf(buf, buf_len, pos,
                                             "ENABLE ON QUERY COMPUTATION "))) {
          SHARE_SCHEMA_LOG(WARN, "fail to print materialized view on query computation", KR(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, " AS "))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print view definition", K(ret));
    } else if (OB_FAIL(print_view_define_str(buf, buf_len, pos, is_oracle_mode,
                                            table_schema->get_view_schema().get_view_definition_str()))) {
      SHARE_SCHEMA_LOG(WARN, "fail to print view definition", K(ret));
    } else if (!table_schema->get_view_schema().get_view_is_updatable() && is_oracle_mode) {
      // with read only is only supported in syntax in oracle mode, but some inner system view is
      // nonupdatable, so we have to check compatible mode here.
      // view in oracle mode can't be both with read only and with check option.
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, " WITH READ ONLY"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print view definition with read only", K(ret));
      }
    } else if (VIEW_CHECK_OPTION_CASCADED == table_schema->get_view_schema().get_view_check_option()) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, " WITH CHECK OPTION"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print view definition with check option", K(ret));
      }
    } else if (VIEW_CHECK_OPTION_LOCAL == table_schema->get_view_schema().get_view_check_option()) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, " WITH LOCAL CHECK OPTION"))) {
        SHARE_SCHEMA_LOG(WARN, "fail to print view definition with local check option", K(ret));
      }
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
