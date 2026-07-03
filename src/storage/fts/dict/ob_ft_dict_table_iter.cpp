/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/fts/dict/ob_ft_dict_table_iter.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/utility.h"
#include "share/ob_force_print_log.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/fts/dict/ob_ft_range_dict.h"

#define USING_LOG_PREFIX STORAGE_FTS

namespace oceanbase
{
namespace storage
{
ObFTDictTableIter::ObFTDictTableIter(ObISQLClient::ReadResult &result)
    : ObIFTDictIterator(), is_inited_(false), res_(result)
{
}

int ObFTDictTableIter::get_key(ObString &str)
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited.", K(ret));
  } else if (OB_FAIL(res_.get_result()->get_varchar("word", str))) {
    LOG_WARN("Failed to get varchar", K(ret));
  }
  return ret;
}

int ObFTDictTableIter::get_value()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObFTDictTableIter::next()
{
  int ret = OB_SUCCESS;
  if (!IS_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not inited.", K(ret));
  } else if (OB_FAIL(res_.get_result()->next())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("Failed to get next row", K(ret));
    }
  }
  return ret;
}

int ObFTDictTableIter::append_where_clause(ObSqlString &sql_string,
                                           const bool need_casedown,
                                           const ObIArray<ObMissingRangeInfo> *partial_ranges)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(partial_ranges) || partial_ranges->empty()) {
  } else {
    bool has_appended = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < partial_ranges->count(); ++i) {
      const ObMissingRangeInfo &range = partial_ranges->at(i);
      const ObString &start_token_str = range.start_token_.get_token();
      const ObString &end_token_str = range.end_token_.get_token();
      bool is_valid_range = !start_token_str.empty() || !end_token_str.empty();
      bool with_and = !start_token_str.empty() && !end_token_str.empty();

      if (!is_valid_range) {
        continue;
      }

      if (!has_appended && OB_FAIL(sql_string.append(" WHERE "))) {
        LOG_WARN("Failed to append WHERE", K(ret));
      } else if (has_appended && OB_FAIL(sql_string.append(" OR "))) {
        LOG_WARN("Failed to append OR", K(ret));
      } else if (OB_FAIL(sql_string.append("("))) {
        LOG_WARN("Failed to append opening parenthesis", K(ret));
      } else if (!start_token_str.empty()) {
        if (need_casedown && OB_FAIL(sql_string.append("LEFT(LOWER(word), 1) > "))) {
          LOG_WARN("Failed to append start_token condition", K(ret));
        } else if (!need_casedown && OB_FAIL(sql_string.append("LEFT(word, 1) > "))) {
          LOG_WARN("Failed to append start_token condition", K(ret));
        } else if (OB_FAIL(sql_append_hex_escape_str(start_token_str, sql_string))) {
          LOG_WARN("Failed to append start_token value", K(ret), K(start_token_str));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (with_and && OB_FAIL(sql_string.append(" AND "))) {
        LOG_WARN("Failed to append AND", K(ret));
      } else if (!end_token_str.empty()) {
        if (need_casedown && OB_FAIL(sql_string.append("LOWER(word) < "))) {
          LOG_WARN("Failed to append end_token condition", K(ret));
        } else if (!need_casedown && OB_FAIL(sql_string.append("word < "))) {
          LOG_WARN("Failed to append end_token condition", K(ret));
        } else if (OB_FAIL(sql_append_hex_escape_str(end_token_str, sql_string))) {
          LOG_WARN("Failed to append end_token value", K(ret), K(end_token_str));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(sql_string.append(")"))) {
        LOG_WARN("Failed to append closing parenthesis", K(ret));
      } else if (!has_appended) {
        has_appended = true;
      }
    }
  }

  return ret;
}

int ObFTDictTableIter::init(const ObString &table_name,
                            const uint64_t tenant_id,
                            const int64_t snapshot_version,
                            const bool need_casedown,
                            const ObIArray<ObMissingRangeInfo> *partial_ranges)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy *sql_proxy = MTL(transaction::ObTransService *)->get_mysql_proxy();

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Inited twice", K(ret));
  } else if (table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid table_name", K(ret), K(table_name));
  } else if (snapshot_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid snapshot_version", K(ret), K(snapshot_version));
  } else {
    SMART_VAR(ObSqlString, sql_string)
    {
      if (need_casedown && OB_FAIL(sql_string.append_fmt(
              "SELECT DISTINCT LOWER(word) AS word FROM %.*s AS OF SNAPSHOT %ld",
              table_name.length(), table_name.ptr(), snapshot_version))) {
        LOG_WARN("Failed to build sql prefix", K(ret), K(table_name), K(snapshot_version), K(need_casedown));
      } else if (!need_casedown && OB_FAIL(sql_string.append_fmt(
                     "SELECT word FROM %.*s AS OF SNAPSHOT %ld",
                     table_name.length(), table_name.ptr(), snapshot_version))) {
        LOG_WARN("Failed to build sql prefix", K(ret), K(table_name), K(snapshot_version), K(need_casedown));
      } else if (OB_FAIL(append_where_clause(sql_string, need_casedown, partial_ranges))) {
        LOG_WARN("Failed to append partial ranges WHERE clause", K(ret));
      } else if (OB_FAIL(sql_string.append(" ORDER BY word"))) {
        LOG_WARN("Failed to append ORDER BY", K(ret));
      } else {
        ObSessionParam session_param;
        session_param.sql_mode_ = nullptr;
        session_param.tz_info_wrap_ = nullptr;
        InnerDDLInfo ddl_info;
        ddl_info.set_is_dummy_ddl_for_inner_visibility(true);
        ddl_info.set_source_table_hidden(false);
        ddl_info.set_dest_table_hidden(false);
        if (OB_FAIL(session_param.ddl_info_.init(ddl_info, 0))) {
          LOG_WARN("fail to init ddl info", K(ret), K(ddl_info));
        } else if (OB_FAIL(sql_proxy->read(res_, tenant_id, sql_string.ptr(), &session_param))) {
          FLOG_WARN("Failed to execute sql, table may not exist or access denied",
                    K(ret), K(table_name), K(snapshot_version), K(sql_string), K(tenant_id));
        }
      }
    }

    if (OB_FAIL(ret)) {
      // already logged
    } else if (OB_ISNULL(res_.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Failed to get result", K(ret));
    } else if (OB_FAIL(res_.get_result()->next())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("Failed to get next row", K(ret));
      } else {
        is_inited_ = true;
      }
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

void ObFTDictTableIter::reset()
{
  res_.close();
  is_inited_ = false;
}

} //  namespace storage
} //  namespace oceanbase
