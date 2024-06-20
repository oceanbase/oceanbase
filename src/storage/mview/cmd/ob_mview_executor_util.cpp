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

#define USING_LOG_PREFIX STORAGE

#include "storage/mview/cmd/ob_mview_executor_util.h"
#include "share/ob_ddl_common.h"
#include "sql/ob_sql_utils.h"
#include "sql/parser/parse_node.h"
#include "storage/ob_common_id_utils.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace lib;
using namespace number;
using namespace share;
using namespace share::schema;
using namespace sql;

int ObMViewExecutorUtil::number_to_int64(const ObNumber &number, int64_t &int64)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!number.is_valid_int64(int64))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("number is not int64", KR(ret), K(number));
  }
  return ret;
}

int ObMViewExecutorUtil::number_to_uint64(const ObNumber &number, uint64_t &uint64)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!number.is_valid_uint64(uint64))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("number is not uint64", KR(ret), K(number));
  }
  return ret;
}

int ObMViewExecutorUtil::split_table_list(const ObString &table_list, ObIArray<ObString> &tables)
{
  int ret = OB_SUCCESS;
  static const char split_character = ',';
  tables.reset();
  ObString list_str = table_list;
  const char *p = nullptr;
  ObString table_str;
  do {
    p = list_str.find(split_character);
    if (nullptr != p) {
      table_str = list_str.split_on(p);
    } else {
      table_str = list_str;
    }
    if (!table_str.empty() && OB_FAIL(tables.push_back(table_str))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  } while (OB_SUCC(ret) && OB_NOT_NULL(p));
  return ret;
}

int ObMViewExecutorUtil::check_min_data_version(const uint64_t tenant_id,
                                                const uint64_t min_data_version, const char *errmsg)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(compat_version < min_data_version)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("version lower than 4.3 does not support this operation", KR(ret), K(tenant_id),
             K(compat_version), K(min_data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, errmsg);
  }
  return ret;
}

int ObMViewExecutorUtil::resolve_table_name(const ObCollationType cs_type,
                                            const ObNameCaseMode case_mode,
                                            const bool is_oracle_mode, const ObString &name,
                                            ObString &database_name, ObString &table_name)
{
  int ret = OB_SUCCESS;
  static const char split_character = '.';
  database_name.reset();
  table_name.reset();
  if (OB_UNLIKELY(name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(name));
  } else {
    ObString name_str = name;
    const char *p = name_str.find(split_character);
    if (p == nullptr) {
      table_name = name_str;
    } else {
      database_name = name_str.split_on(p);
      table_name = name_str;
      if (OB_UNLIKELY(database_name.empty() || table_name.empty() ||
                      nullptr != table_name.find(split_character))) {
        ret = OB_WRONG_TABLE_NAME;
        LOG_WARN("wrong table name", KR(ret), K(name));
      }
    }
    if (OB_SUCC(ret)) {
      const bool preserve_lettercase =
        is_oracle_mode ? true : (case_mode != OB_LOWERCASE_AND_INSENSITIVE);
      upper_db_table_name(case_mode, is_oracle_mode, database_name);
      upper_db_table_name(case_mode, is_oracle_mode, table_name);
      if (!database_name.empty() && OB_FAIL(ObSQLUtils::check_and_convert_db_name(
                                      cs_type, preserve_lettercase, database_name))) {
        LOG_WARN("fail to check and convert database name", KR(ret), K(database_name));
      } else if (OB_FAIL(ObSQLUtils::check_and_convert_table_name(cs_type, preserve_lettercase,
                                                                  table_name, is_oracle_mode))) {
        LOG_WARN("fail to check and convert table name", KR(ret), K(cs_type),
                 K(preserve_lettercase), K(table_name));
      }
    }
  }
  return ret;
}

void ObMViewExecutorUtil::upper_db_table_name(const ObNameCaseMode case_mode,
                                              const bool is_oracle_mode, ObString &name)
{
  if (is_oracle_mode) {
    str_toupper(name.ptr(), name.length());
  } else {
    if (OB_LOWERCASE_AND_INSENSITIVE == case_mode) {
      str_tolower(name.ptr(), name.length());
    }
  }
}

int ObMViewExecutorUtil::to_refresh_method(const char c, ObMVRefreshMethod &refresh_method)
{
  int ret = OB_SUCCESS;
  refresh_method = ObMVRefreshMethod::MAX;
  switch (c) {
    case 'a':
    case 'A':
    case 'c':
    case 'C':
      refresh_method = ObMVRefreshMethod::COMPLETE;
      break;
    case 'f':
    case 'F':
      refresh_method = ObMVRefreshMethod::FAST;
      break;
    case '?':
      refresh_method = ObMVRefreshMethod::FORCE;
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid method", KR(ret), K(c));
      break;
  }
  return ret;
}

int ObMViewExecutorUtil::to_collection_level(const ObString &str,
                                             ObMVRefreshStatsCollectionLevel &collection_level)
{
  int ret = OB_SUCCESS;
  collection_level = ObMVRefreshStatsCollectionLevel::MAX;
  if (0 == str.case_compare("NONE")) {
    collection_level = ObMVRefreshStatsCollectionLevel::NONE;
  } else if (0 == str.case_compare("TYPICAL")) {
    collection_level = ObMVRefreshStatsCollectionLevel::TYPICAL;
  } else if (0 == str.case_compare("ADVANCED")) {
    collection_level = ObMVRefreshStatsCollectionLevel::ADVANCED;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid collection level", KR(ret), K(str));
  }
  return ret;
}

int ObMViewExecutorUtil::generate_refresh_id(const uint64_t tenant_id, int64_t &refresh_id)
{
  int ret = OB_SUCCESS;
  ObCommonID unique_id;
  MTL_SWITCH(tenant_id)
  {
    if (OB_FAIL(ObCommonIDUtils::gen_unique_id(tenant_id, unique_id))) {
      LOG_WARN("failed to gen unique id", KR(ret));
    }
  }
  else
  {
    if (OB_FAIL(ObCommonIDUtils::gen_unique_id_by_rpc(tenant_id, unique_id))) {
      LOG_WARN("failed to gen unique id", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    refresh_id = unique_id.id();
  }
  return ret;
}

bool ObMViewExecutorUtil::is_mview_refresh_retry_ret_code(int ret_code)
{
  return OB_OLD_SCHEMA_VERSION == ret_code || OB_EAGAIN == ret_code ||
         OB_INVALID_QUERY_TIMESTAMP == ret_code || OB_TASK_EXPIRED == ret_code ||
         is_master_changed_error(ret_code) || is_partition_change_error(ret_code) ||
         is_ddl_stmt_packet_retry_err(ret_code);
}

} // namespace storage
} // namespace oceanbase
