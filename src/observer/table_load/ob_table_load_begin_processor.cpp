// Copyright (c) 2018-present Alibaba Inc. All Rights Reserved.
// Author:
//   Junquan Chen <jianming.cjq@alipay.com>

#define USING_LOG_PREFIX SERVER

#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_tenant.h"
#include "observer/table_load/ob_table_load_begin_processor.h"
#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_store.h"
#include "observer/table_load/ob_table_load_table_ctx.h"

namespace oceanbase
{
namespace observer
{
using namespace table;
using namespace sql;
using namespace omt;

int ObTableLoadBeginP::process()
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  ObTableLoadArray<ObString> column_names;
  int32_t session_count = 0;

  if (OB_FAIL(check_user_access(arg_.credential_))) {
    LOG_WARN("fail to check_user_access", KR(ret), K_(arg));
  }

  if (OB_SUCC(ret)) {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table_schema = nullptr;
    if (OB_FAIL(ObTableLoadSchema::get_table_schema(credential_.tenant_id_,
                                                    credential_.database_id_, arg_.table_name_,
                                                    schema_guard, table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K_(arg));
    } else {
      table_id = table_schema->get_table_id();
      if (OB_FAIL(ObTableLoadSchema::get_column_names(table_schema, allocator_, column_names))) {
        LOG_WARN("fail to get column name", KR(ret), K_(arg));
      } else if (OB_UNLIKELY(column_names.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty column names", KR(ret), K_(arg));
      } else if (OB_FAIL(init_idx_array(table_schema))) {
        LOG_WARN("failed to init idx array", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObTenant *tenant = nullptr;
    if (OB_FAIL(GCTX.omt_->get_tenant(credential_.tenant_id_, tenant))) {
      LOG_WARN("fail to get tenant handle", KR(ret), K(credential_.tenant_id_));
    } else {
      session_count = MIN(arg_.config_.session_count_, (int32_t)tenant->unit_max_cpu());
    }
  }

  if (OB_SUCC(ret)) {
    ObTableLoadTableCtx *table_ctx = nullptr;
    bool is_new = false;
    ObTableLoadParam param;
    param.tenant_id_ = credential_.tenant_id_;
    param.table_id_ = table_id;
    param.batch_size_ = arg_.config_.batch_size_;
    param.session_count_ = session_count;
    param.max_error_row_count_ = arg_.config_.max_error_row_count_;
    param.column_count_ = column_names.count();
    param.need_sort_ = arg_.config_.flag_.is_need_sort_;
    param.px_mode_ = false;
    param.online_opt_stat_gather_ = false;
    param.dup_action_ = ObLoadDupActionType::LOAD_STOP_ON_DUP;
    if (OB_FAIL(param.normalize())) {
      LOG_WARN("fail to normalize param", KR(ret));
    } else if (arg_.config_.flag_.data_type_ >= static_cast<uint64_t>(ObTableLoadDataType::MAX_DATA_TYPE)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data type is error", KR(ret), K(arg_.config_.flag_.data_type_));
    } else {
      param.data_type_ = static_cast<ObTableLoadDataType>(arg_.config_.flag_.data_type_);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObTableLoadService::create_ctx(param, table_ctx, is_new))) {
      LOG_WARN("fail to create table ctx", KR(ret), K(param));
    } else {
      if (is_new) {  // 新建的ctx需要初始化
        if (OB_FAIL(table_ctx->init_session_info(credential_.user_id_))) {
          LOG_WARN("fail to init session info", KR(ret));
        } else if (OB_FAIL(ObTableLoadCoordinator::init_ctx(table_ctx, idx_array_, nullptr))) {
          LOG_WARN("fail to coordinator init ctx", KR(ret));
        }
      } else {  // 已存在的ctx检查是否已初始化
        if (OB_UNLIKELY(!ObTableLoadCoordinator::is_ctx_inited(table_ctx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected uninited coordinator ctx", KR(ret));
        }
      }
      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(ObTableLoadService::remove_ctx(table_ctx))) {
          LOG_WARN("fail to remove table ctx", KR(tmp_ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObTableLoadCoordinator coordinator(table_ctx);
      if (OB_FAIL(coordinator.init())) {
        LOG_WARN("fail to init coordinator", KR(ret));
      } else if (is_new && OB_FAIL(coordinator.begin())) {  // 新建的ctx需要begin
        LOG_WARN("fail to coordinator begin", KR(ret));
      } else if (OB_FAIL(coordinator.get_status(result_.status_, result_.error_code_))) {
        LOG_WARN("fail to coordinator get status", KR(ret));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }

  if (OB_SUCC(ret)) {
    result_.table_id_ = table_id;
    result_.column_names_ = column_names;
  }

  return ret;
}

int ObTableLoadBeginP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

int ObTableLoadBeginP::init_idx_array(const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColDesc, 64> column_descs;
  if (OB_FAIL(table_schema->get_column_ids(column_descs))) {
    LOG_WARN("fail to get column ids", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (i < column_descs.count()); ++i) {
      ObColDesc &col_desc = column_descs.at(i);
      const ObColumnSchemaV2 *column_schema = table_schema->get_column_schema(col_desc.col_id_);
      if (!column_schema->is_hidden()) {
        int64_t idx = col_desc.col_id_ - OB_APP_MIN_COLUMN_ID;
        if (OB_FAIL(idx_array_.push_back(idx))) {
          LOG_WARN("failed to push back idx to array", K(ret), K(idx));
        }
      }
    }
  }

  return ret;
}

/**
 * ObTableLoadPreBeginPeerP
 */
int ObTableLoadPreBeginPeerP::deserialize()
{
  arg_.partition_id_array_.set_allocator(allocator_);
  arg_.target_partition_id_array_.set_allocator(allocator_);
  return ParentType::deserialize();
}

int ObTableLoadPreBeginPeerP::process()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_user_access(arg_.credential_))) {
    LOG_WARN("fail to check_user_access", KR(ret), K_(arg));
  }

  if (OB_SUCC(ret)) {
    ObTableLoadTableCtx *table_ctx = nullptr;
    bool is_new = false;
    ObTableLoadParam param;
    param.tenant_id_ = credential_.tenant_id_;
    param.table_id_ = arg_.table_id_;
    param.target_table_id_ = arg_.target_table_id_;
    param.batch_size_ = arg_.config_.batch_size_;
    param.session_count_ = arg_.config_.session_count_;
    param.max_error_row_count_ = arg_.config_.max_error_row_count_;
    param.column_count_ = arg_.column_count_;
    param.need_sort_ = arg_.config_.flag_.is_need_sort_;
    param.px_mode_ = arg_.px_mode_;
    param.online_opt_stat_gather_ = arg_.online_opt_stat_gather_;
    param.dup_action_ = arg_.dup_action_;
    if (OB_FAIL(ObTableLoadService::create_ctx(param, table_ctx, is_new))) {
      LOG_WARN("fail to create table ctx", KR(ret), K(param));
    } else if (OB_UNLIKELY(!is_new)) { // 数据节点不能重复begin
      ret = OB_ENTRY_EXIST;
      LOG_WARN("table ctx exists", KR(ret));
    } else {
      if (OB_FAIL(ObTableLoadStore::init_ctx(table_ctx, arg_.ddl_task_id_, arg_.partition_id_array_,
                                             arg_.target_partition_id_array_))) {
        LOG_WARN("fail to store init ctx", KR(ret));
      }
      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(ObTableLoadService::remove_ctx(table_ctx))) {
          LOG_WARN("fail to remove table ctx", KR(tmp_ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObTableLoadStore store(table_ctx);
      if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(store.pre_begin())) {
        LOG_WARN("fail to store pre begin", KR(ret));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }

  return ret;
}

int ObTableLoadPreBeginPeerP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

/**
 * ObTableLoadConfirmBeginPeerP
 */

int ObTableLoadConfirmBeginPeerP::process()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_user_access(arg_.credential_))) {
    LOG_WARN("fail to check_user_access", KR(ret), K_(arg));
  }

  if (OB_SUCC(ret)) {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadKey key(credential_.tenant_id_, arg_.table_id_);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ObTableLoadStore store(table_ctx);
      if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(store.confirm_begin())) {
        LOG_WARN("fail to store confirm begin", KR(ret));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }

  return ret;
}

int ObTableLoadConfirmBeginPeerP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

} // namespace observer
} // namespace oceanbase
