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

#define USING_LOG_PREFIX STORAGE

#include "ob_backup_param_operator.h"
#include "common/ob_smart_var.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "storage/backup/ob_backup_data_store.h"


using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase {
namespace backup {

int ObBackupParamOperator::get_backup_parameters_info(const uint64_t tenant_id,
  ObExternParamInfoDesc &param_infos, common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_INVALID_TENANT_ID == tenant_id || is_meta_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(construct_query_sql_(tenant_id, sql))) {
    LOG_WARN("failed to construct query sql", K(ret));
  } else {
    HEAP_VAR(ObMySQLProxy::ReadResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_client.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql), K(tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql), K(tenant_id));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("failed to get next row", K(ret), K(sql), K(tenant_id));
            }
          } else if (OB_FAIL(handle_one_result_(param_infos, *result))) {
            LOG_WARN("failed to handle result", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupParamOperator::backup_cluster_parameters(const ObBackupPathString &backup_dest_str)
{
  int ret = OB_SUCCESS;
  ObExternParamInfoDesc cluster_params;
  ObBackupDataStore store;
  if (OB_UNLIKELY(NULL == GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[DATA_BACKUP]invalid arg", K(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(store.init(backup_dest_str.ptr()))) {
    LOG_WARN("[DATA_BACKUP]failed to init cluster param store", K(ret));
  } else if (OB_FAIL(ObBackupParamOperator::get_backup_parameters_info(
    OB_SYS_TENANT_ID, cluster_params, *GCTX.sql_proxy_))) {
    LOG_WARN("[DATA_BACKUP]failed to get cluster parameters info", K(ret));
  } else {
    cluster_params.tenant_id_ = OB_SYS_TENANT_ID;
    if(OB_FAIL(store.write_cluster_param_info(cluster_params))) {
      LOG_WARN("[DATA_BACKUP]failed to write backup cluster parameters info", K(ret),
        K(cluster_params));
    }
  }
  return ret;
}

const char *ObBackupParamOperator::TENANT_BLACK_PARAMETER_LIST[] = {
  "external_kms_info",
  "tde_method",
};

const char *ObBackupParamOperator::CLUSTER_BLACK_PARAMETER_LIST[] = {
  "all_server_list",
  "rootservice_list",
  "debug_sync_timeout",
};

int ObBackupParamOperator::construct_tenant_param_sql_(const uint64_t tenant_id,
  common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(sql.assign_fmt("select name, value from %s", OB_TENANT_PARAMETER_TNAME))) {
    LOG_WARN("failed to assign fmt", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" where tenant_id=%lu", tenant_id))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (OB_FAIL(sql.append(" and zone='' and svr_ip='ANY' and scope='TENANT'"))) {
    LOG_WARN("failed to append sql", K(ret));
  } else {
    const int64_t list_len = sizeof(TENANT_BLACK_PARAMETER_LIST) / sizeof(char *);
    for (int64_t i = 0; OB_SUCC(ret) && i < list_len; i++) {
      const char *name = TENANT_BLACK_PARAMETER_LIST[i];
      if (0 == i && OB_FAIL(sql.append_fmt(" and name not in ('%s'", name))) {
        LOG_WARN("failed to apend sql", K(ret));
      } else if (0 < i && OB_FAIL(sql.append_fmt(",'%s'", name))) {
        LOG_WARN("failed to apend sql", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (i == list_len - 1 && OB_FAIL(sql.append(")"))) {
        LOG_WARN("failed to apend sql", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupParamOperator::construct_cluster_param_sql_(common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sql.assign_fmt("select name, value from %s", OB_ALL_SYS_PARAMETER_TNAME))) {
    LOG_WARN("failed to assign fmt", K(ret));
  } else if (OB_FAIL(sql.append(" where zone='' and svr_ip='ANY' and scope='CLUSTER'"))) {
    LOG_WARN("failed to apend sql", K(ret));
  } else {
    const int64_t list_len = sizeof(CLUSTER_BLACK_PARAMETER_LIST) / sizeof(char *);
    for (int64_t i = 0; OB_SUCC(ret) && i < list_len; i++) {
      const char *name = CLUSTER_BLACK_PARAMETER_LIST[i];
      if (0 == i && OB_FAIL(sql.append_fmt(" and name not in ('%s'", name))) {
        LOG_WARN("failed to apend sql", K(ret));
      } else if (0 < i && OB_FAIL(sql.append_fmt(",'%s'", name))) {
        LOG_WARN("failed to apend sql", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (i == list_len - 1 && OB_FAIL(sql.append(")"))) {
        LOG_WARN("failed to apend sql", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupParamOperator::construct_query_sql_(const uint64_t tenant_id,
  common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  if (OB_SYS_TENANT_ID == tenant_id) {
    if (OB_FAIL(construct_cluster_param_sql_(sql))) {
      LOG_WARN("failed to construct cluster parameters sql", K(ret), K(tenant_id));
    }
  } else {
    if (OB_FAIL(construct_tenant_param_sql_(tenant_id, sql))) {
      LOG_WARN("failed to construct tenant parameters sql", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObBackupParamOperator::handle_one_result_(ObExternParamInfoDesc &param_info,
  common::sqlclient::ObMySQLResult &result)
{
  int ret = OB_SUCCESS;
  int64_t name_str_len = 0;
  int64_t value_str_len = 0;
  char name[common::OB_MAX_CONFIG_NAME_LEN + 1] = "";

  HEAP_VAR(ObBackupParam, param) {
    HEAP_VAR(char [common::OB_MAX_CONFIG_VALUE_LEN + 1], value) {
      EXTRACT_STRBUF_FIELD_MYSQL(result, "name", name,common::OB_MAX_CONFIG_NAME_LEN, name_str_len);
      EXTRACT_STRBUF_FIELD_MYSQL(result, "value", value, common::OB_MAX_CONFIG_VALUE_LEN, value_str_len);
      if (FAILEDx(param.name_.assign(name))) {
        LOG_WARN("failed to assign name", K(ret));
      } else if (OB_FALSE_IT(param.value_.assign_ptr(value, value_str_len))) {
      } else if (OB_FAIL(param_info.push(param))) {
        LOG_WARN("failed to push back parameter", K(ret));
      }
    }
  }
  return ret;
}
}  // namespace backup
}  // namespace oceanbase
