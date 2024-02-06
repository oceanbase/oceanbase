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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/cmd/ob_table_direct_insert_ctx.h"
#include "sql/engine/ob_exec_context.h"
#include "observer/table_load/ob_table_load_exec_ctx.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "observer/table_load/ob_table_load_instance.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "observer/omt/ob_tenant.h"

namespace oceanbase
{
using namespace common;
using namespace observer;
using namespace storage;
using namespace share;

namespace sql
{
ObTableDirectInsertCtx::~ObTableDirectInsertCtx()
{
  destroy();
}

int ObTableDirectInsertCtx::init(ObExecContext *exec_ctx,
    const uint64_t table_id, const int64_t parallel)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableDirectInsertCtx init twice", KR(ret));
  } else if (OB_ISNULL(exec_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("exec_ctx cannot be null", KR(ret));
  } else {
    if (OB_ISNULL(load_exec_ctx_ = OB_NEWx(ObTableLoadSqlExecCtx, &exec_ctx->get_allocator()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadSqlExecCtx", KR(ret));
    } else if (OB_ISNULL(table_load_instance_ =
                           OB_NEWx(ObTableLoadInstance, &exec_ctx->get_allocator()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadInstance", KR(ret));
    } else {
      load_exec_ctx_->exec_ctx_ = exec_ctx;
      uint64_t sql_mode = 0;
      ObSEArray<int64_t, 16> store_column_idxs;
      omt::ObTenant *tenant = nullptr;
      if (OB_FAIL(GCTX.omt_->get_tenant(MTL_ID(), tenant))) {
        LOG_WARN("fail to get tenant handle", KR(ret), K(MTL_ID()));
      } else if (OB_FAIL(init_store_column_idxs(MTL_ID(), table_id, store_column_idxs))) {
        LOG_WARN("failed to init store column idxs", KR(ret));
      } else if (OB_FAIL(exec_ctx->get_my_session()->get_sys_variable(SYS_VAR_SQL_MODE, sql_mode))) {
        LOG_WARN("fail to get sys variable", KR(ret));
      } else {
        ObTableLoadParam param;
        param.column_count_ = store_column_idxs.count();
        param.tenant_id_ = MTL_ID();
        param.table_id_ = table_id;
        param.batch_size_ = 100;
        param.parallel_ = parallel;
        param.session_count_ = MIN(parallel, (int64_t)tenant->unit_max_cpu() * 2);
        param.px_mode_ = true;
        param.online_opt_stat_gather_ = false;
        param.need_sort_ = true;
        param.max_error_row_count_ = 0;
        param.dup_action_ = sql::ObLoadDupActionType::LOAD_STOP_ON_DUP;
        param.sql_mode_ = sql_mode;
        if (OB_FAIL(table_load_instance_->init(param, store_column_idxs, load_exec_ctx_))) {
          LOG_WARN("failed to init direct loader", KR(ret));
        } else {
          is_inited_ = true;
          LOG_DEBUG("succeeded to init direct loader", K(param));
        }
      }
    }
  }
  return ret;
}

// commit() should be called before finish()
int ObTableDirectInsertCtx::commit()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableDirectInsertCtx is not init", KR(ret));
  } else if (OB_FAIL(table_load_instance_->px_commit_data())) {
    LOG_WARN("failed to do px_commit_data", KR(ret));
  }
  return ret;
}

// finish() should be called after commit()
int ObTableDirectInsertCtx::finish()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableDirectInsertCtx is not init", KR(ret));
  } else if (OB_FAIL(table_load_instance_->px_commit_ddl())) {
    LOG_WARN("failed to do px_commit_ddl", KR(ret));
  } else {
    table_load_instance_->destroy();
    LOG_DEBUG("succeeded to finish direct loader");
  }
  return ret;
}

void ObTableDirectInsertCtx::destroy()
{
  if (OB_NOT_NULL(table_load_instance_)) {
    table_load_instance_->~ObTableLoadInstance();
    table_load_instance_ = nullptr;
  }
  if (OB_NOT_NULL(load_exec_ctx_)) {
    load_exec_ctx_->~ObTableLoadSqlExecCtx();
    load_exec_ctx_ = nullptr;
  }
}

int ObTableDirectInsertCtx::init_store_column_idxs(const uint64_t tenant_id,
    const uint64_t table_id, ObIArray<int64_t> &store_column_idxs)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  ObSEArray<ObColDesc, 64> column_descs;

  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id,
                                                                                  schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(table_schema->get_column_ids(column_descs))) {
    STORAGE_LOG(WARN, "fail to get column descs", KR(ret), KPC(table_schema));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (i < column_descs.count()); ++i) {
      const ObColDesc &col_desc = column_descs.at(i);
      const ObColumnSchemaV2 *col_schema = table_schema->get_column_schema(col_desc.col_id_);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema", KR(ret), K(col_desc));
      } else if (col_schema->is_hidden()) {
      } else if (OB_FAIL(store_column_idxs.push_back(i))) {
        LOG_WARN("failed to push back store column idxs", KR(ret), K(i));
      }
    }
  }

  return ret;
}

} // namespace sql
} // namespace oceanbase
