/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SERVER

#include "ob_table_system_variable.h"
#include "ob_table_sess_pool.h"
#include "share/table/ob_table_config_util.h"

using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::sql;
using namespace oceanbase::omt;

namespace oceanbase
{
namespace table
{

int ObTableRelatedSysVars::init()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ObLockGuard<ObSpinLock> guard(lock_);
    if (!is_inited_) { // double check
      if (OB_FAIL(update_sys_vars(false/*only_update_dynamic_vars*/))) {
        LOG_WARN("fail to init sys vars", K(ret));
      } else {
        is_inited_ = true;
      }
    }
  }

  return ret;
}

int ObTableRelatedSysVars::update_sys_vars(bool only_update_dynamic_vars)
{
  int ret = OB_SUCCESS;

  if (!is_inited_ && only_update_dynamic_vars) {
    // do nothing
  } else {
    int64_t tenant_id = MTL_ID();
    SMART_VAR(ObSQLSessionInfo, sess_info) {
      ObSchemaGetterGuard schema_guard;
      const ObTenantSchema *tenant_schema = nullptr;
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
        LOG_WARN("fail to get schema guard", K(ret), K(tenant_id));
      } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
        LOG_WARN("fail to get tenant schema", K(ret), K(tenant_id));
      } else if (OB_ISNULL(tenant_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("tenant schema is null", K(ret));
      } else if (OB_FAIL(ObTableApiSessUtil::init_sess_info(tenant_id,
                                                            tenant_schema->get_tenant_name_str(),
                                                            schema_guard,
                                                            sess_info))) {
        LOG_WARN("fail to init sess info", K(ret), K(tenant_id));
      } else {
        // static vars
        if (!only_update_dynamic_vars) {
          int64_t sess_mode_val = 0;
          if (OB_FAIL(sess_info.get_sys_variable(SYS_VAR_OB_KV_MODE, sess_mode_val))) {
            LOG_WARN("fail to get ob_kv_mode variable", K(ret));
          } else {
            static_vars_.set_kv_mode(static_cast<ObKvModeType>(sess_mode_val));
          }
        }

        // dynamic vars
        if (OB_SUCC(ret)) {
          int64_t binlog_row_image = -1;
          int64_t query_record_size_limit = sess_info.get_tenant_query_record_size_limit();
          bool enable_query_response_time_stats = sess_info.enable_query_response_time_stats();
          if (OB_FAIL(sess_info.get_sys_variable(SYS_VAR_BINLOG_ROW_IMAGE, binlog_row_image))) {
            LOG_WARN("fail to get binlog_row_image variable", K(ret));
          } else {
            dynamic_vars_.set_binlog_row_image(binlog_row_image);
            dynamic_vars_.set_query_record_size_limit(query_record_size_limit);
            dynamic_vars_.set_enable_query_response_time_stats(enable_query_response_time_stats);
            omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
            if (tenant_config.is_valid()) {
              // group commit
              int64_t batch_size = tenant_config->kv_group_commit_batch_size;
              dynamic_vars_.set_kv_group_commit_batch_size(batch_size);
              ObString rw_mode = tenant_config->kv_group_commit_rw_mode.get_value_string();
              if (batch_size > 1) {
                if (rw_mode.case_compare("all") == 0) { // 'ALL'
                  dynamic_vars_.set_group_rw_mode(ObTableGroupRwMode::ALL);
                } else if (rw_mode.case_compare("read") == 0) {
                  dynamic_vars_.set_group_rw_mode(ObTableGroupRwMode::READ);
                } else if (rw_mode.case_compare("write") == 0) {
                  dynamic_vars_.set_group_rw_mode(ObTableGroupRwMode::WRITE);
                }
              }
              // distributed execute
              dynamic_vars_.set_support_distributed_execute(tenant_config->_obkv_enable_distributed_execution);
            }
          }
        }
      }
    }
  }

  return ret;
}

}  // namespace table
}  // namespace oceanbase
