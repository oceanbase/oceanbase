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
#include "rootserver/ob_create_index_on_empty_table_helper.h"
#include "rootserver/ob_ddl_service.h"
#include "storage/tx/ob_ts_mgr.h"
#include "common/ob_timeout_ctx.h"
#include "share/ob_share_util.h"
#include "share/scn.h"
namespace oceanbase
{
using namespace share;
using namespace share::schema;
namespace rootserver
{
int ObCreateIndexOnEmptyTableHelper::check_create_index_on_empty_table_opt(
    rootserver::ObDDLService &ddl_service,
    ObMySQLTransaction &trans,
    const share::schema::ObSysVariableSchema &sys_var_schema,
    const ObString &database_name,
    const share::schema::ObTableSchema &table_schema,
    ObIndexType index_type,
    const uint64_t executor_data_version,
    const ObSQLMode sql_mode,
    bool &is_create_index_on_empty_table_opt) {
  int ret = OB_SUCCESS;
  is_create_index_on_empty_table_opt = false;
  if (DATA_VERSION_SUPPORT_EMPTY_TABLE_CREATE_INDEX_OPT(executor_data_version)) {
    if (!share::schema::is_index_support_empty_table_opt(index_type) && index_type != ObIndexType::INDEX_TYPE_IS_NOT) {
    } else if (OB_FAIL(ObDDLUtil::check_table_empty(sys_var_schema, database_name,
                                                    table_schema,
                                                    sql_mode,
                                                    is_create_index_on_empty_table_opt))) {
      LOG_WARN("failed to check table empty", KR(ret), K(database_name), K(table_schema));
    } else if (!is_create_index_on_empty_table_opt) {
    } else if (OB_FAIL(ddl_service.lock_table(trans, table_schema))) {
      if (OB_TRY_LOCK_ROW_CONFLICT == ret || OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret || OB_EAGAIN == ret) {
        ret = OB_SUCCESS;
        is_create_index_on_empty_table_opt = false;
      } else {
        LOG_WARN("failed to lock table", KR(ret), K(table_schema));
      }
    } else if (OB_FAIL(ObDDLUtil::check_table_empty(sys_var_schema, database_name,
                                                    table_schema,
                                                    sql_mode,
                                                    is_create_index_on_empty_table_opt))) {
      LOG_WARN("failed to check table empty", KR(ret), K(database_name), K(table_schema));
    }
  }
  LOG_TRACE("check_create_index_on_empty_table_opt", K(ret), K(is_create_index_on_empty_table_opt),
    "name_case_mode", sys_var_schema.get_name_case_mode(),
    K(database_name), "table_name", table_schema.get_table_name_str());
  return ret;
}

int ObCreateIndexOnEmptyTableHelper::get_major_frozen_scn(const uint64_t tenant_id, share::SCN &major_frozen_scn)
{
  int ret = OB_SUCCESS;
  bool is_external_consistent = false;
  ObTimeoutCtx ctx;
  if (OB_UNLIKELY(common::OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
    LOG_WARN("fail to set timeout ctx", KR(ret));
  } else if (OB_FAIL(OB_TS_MGR.get_ts_sync(tenant_id,
                                           ctx.get_timeout(),
                                           major_frozen_scn,
                                           is_external_consistent))) {
    LOG_WARN("fail to get gts sync", KR(ret), K(tenant_id), K(ctx.get_timeout()), K(major_frozen_scn), K(is_external_consistent));
  }
  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase
