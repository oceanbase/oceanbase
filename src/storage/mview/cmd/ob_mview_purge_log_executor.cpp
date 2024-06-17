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

#include "storage/mview/cmd/ob_mview_purge_log_executor.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/mview/cmd/ob_mview_executor_util.h"
#include "storage/mview/ob_mlog_purge.h"

namespace oceanbase
{
namespace storage
{
using namespace share;
using namespace share::schema;
using namespace sql;

ObMViewPurgeLogExecutor::ObMViewPurgeLogExecutor()
  : ctx_(nullptr),
    session_info_(nullptr),
    tenant_id_(OB_INVALID_TENANT_ID),
    master_table_id_(OB_INVALID_ID)
{
}

ObMViewPurgeLogExecutor::~ObMViewPurgeLogExecutor() {}

int ObMViewPurgeLogExecutor::execute(ObExecContext &ctx, const ObMViewPurgeLogArg &arg)
{
  int ret = OB_SUCCESS;
  ctx_ = &ctx;
  CK(OB_NOT_NULL(session_info_ = ctx.get_my_session()));
  CK(OB_NOT_NULL(ctx.get_sql_ctx()->schema_guard_));
  OV(OB_LIKELY(arg.is_valid()), OB_INVALID_ARGUMENT, arg);
  OZ(schema_checker_.init(*ctx.get_sql_ctx()->schema_guard_, session_info_->get_sessid()));
  OX(tenant_id_ = session_info_->get_effective_tenant_id());
  OZ(ObMViewExecutorUtil::check_min_data_version(
    tenant_id_, DATA_VERSION_4_3_0_0, "tenant's data version is below 4.3.0.0, purge mlog is"));
  OZ(resolve_arg(arg));

  if (OB_SUCC(ret)) {
    ObMLogPurgeParam purge_param;
    ObMLogPurger purger;
    purge_param.tenant_id_ = tenant_id_;
    purge_param.master_table_id_ = master_table_id_;
    purge_param.purge_log_parallel_ = arg.purge_log_parallel_;
    if (OB_FAIL(purger.init(ctx, purge_param))) {
      LOG_WARN("fail to init mlog purger", KR(ret), K(purge_param));
    } else if (OB_FAIL(purger.purge())) {
      LOG_WARN("fail to do purge", KR(ret), K(purge_param));
    }
  }
  return ret;
}

int ObMViewPurgeLogExecutor::resolve_arg(const ObMViewPurgeLogArg &arg)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;
  if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("fail to get name case mode", KR(ret));
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation_connection", KR(ret));
  }
  // resolve master
  if (OB_SUCC(ret)) {
    ObString database_name, table_name;
    bool has_synonym = false;
    ObString new_db_name, new_tbl_name;
    const ObTableSchema *table_schema = nullptr;
    if (OB_FAIL(ObMViewExecutorUtil::resolve_table_name(cs_type, case_mode, lib::is_oracle_mode(),
                                                        arg.master_, database_name, table_name))) {
      LOG_WARN("fail to resolve table name", KR(ret), K(cs_type), K(case_mode), K(arg.master_));
      LOG_USER_ERROR(OB_WRONG_TABLE_NAME, static_cast<int>(arg.master_.length()),
                     arg.master_.ptr());
    } else if (database_name.empty() &&
               FALSE_IT(database_name = session_info_->get_database_name())) {
    } else if (OB_UNLIKELY(database_name.empty())) {
      ret = OB_ERR_NO_DB_SELECTED;
      LOG_WARN("No database selected", KR(ret));
    } else if (OB_FAIL(schema_checker_.get_table_schema_with_synonym(
                 tenant_id_, database_name, table_name, false /*is_index_table*/, has_synonym,
                 new_db_name, new_tbl_name, table_schema))) {
      LOG_WARN("fail to get table schema with synonym", KR(ret), K(database_name), K(table_name));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", KR(ret), K(arg));
    } else if (OB_UNLIKELY(OB_INVALID_ID == table_schema->get_mlog_tid())) {
      ret = OB_ERR_TABLE_NO_MLOG;
      LOG_WARN("table does not have materialized view log", KR(ret), KPC(table_schema));
    } else {
      master_table_id_ = table_schema->get_table_id();
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase