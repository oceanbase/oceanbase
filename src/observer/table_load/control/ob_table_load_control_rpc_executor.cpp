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

#define USING_LOG_PREFIX SERVER

#include "ob_table_load_control_rpc_executor.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_store.h"
#include "observer/table_load/ob_table_load_table_ctx.h"

namespace oceanbase
{
namespace observer
{
using namespace table;

// pre_begin
int ObDirectLoadControlPreBeginExecutor::deserialize()
{
  arg_.partition_id_array_.set_allocator(allocator_);
  arg_.target_partition_id_array_.set_allocator(allocator_);
  return ParentType::deserialize();
}

int ObDirectLoadControlPreBeginExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObDirectLoadControlPreBeginExecutor::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("table load control pre begin", K_(arg));
  if (OB_FAIL(ObTableLoadService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadParam param;
    param.tenant_id_ = MTL_ID();
    param.table_id_ = arg_.table_id_;
    param.batch_size_ = arg_.config_.batch_size_;
    param.parallel_ = arg_.config_.parallel_;
    param.session_count_ = arg_.config_.parallel_;
    param.max_error_row_count_ = arg_.config_.max_error_row_count_;
    param.column_count_ = arg_.column_count_;
    param.need_sort_ = arg_.config_.is_need_sort_;
    param.px_mode_ = arg_.px_mode_;
    param.online_opt_stat_gather_ = arg_.online_opt_stat_gather_;
    param.dup_action_ = arg_.dup_action_;
    ObTableLoadDDLParam ddl_param;
    uint64_t data_version = 0;
    ddl_param.dest_table_id_ = arg_.dest_table_id_;
    ddl_param.task_id_ = arg_.task_id_;
    ddl_param.schema_version_ = arg_.schema_version_;
    ddl_param.snapshot_version_ = arg_.snapshot_version_;
    ddl_param.data_version_ = arg_.data_version_;
    if (OB_FAIL(create_table_ctx(param, ddl_param, table_ctx))) {
      LOG_WARN("fail to create table ctx", KR(ret));
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

int ObDirectLoadControlPreBeginExecutor::create_table_ctx(const ObTableLoadParam &param,
                                                          const ObTableLoadDDLParam &ddl_param,
                                                          ObTableLoadTableCtx *&table_ctx)
{
  int ret = OB_SUCCESS;
  table_ctx = nullptr;
  if (OB_ISNULL(table_ctx = ObTableLoadService::alloc_ctx())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc table ctx", KR(ret), K(param));
  } else if (OB_FAIL(table_ctx->init(param, ddl_param, arg_.session_info_))) {
    LOG_WARN("fail to init table ctx", KR(ret));
  } else if (OB_FAIL(ObTableLoadStore::init_ctx(table_ctx, arg_.partition_id_array_,
                                                arg_.target_partition_id_array_))) {
    LOG_WARN("fail to store init ctx", KR(ret));
  } else if (OB_FAIL(ObTableLoadService::add_ctx(table_ctx))) {
    LOG_WARN("fail to add ctx", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != table_ctx) {
      ObTableLoadService::free_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

// confirm_begin
int ObDirectLoadControlConfirmBeginExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObDirectLoadControlConfirmBeginExecutor::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("table load control confirm begin", K_(arg));
  if (OB_FAIL(ObTableLoadService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
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

// pre_merge
int ObDirectLoadControlPreMergeExecutor::deserialize()
{
  arg_.committed_trans_id_array_.set_allocator(allocator_);
  return ParentType::deserialize();
}

int ObDirectLoadControlPreMergeExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObDirectLoadControlPreMergeExecutor::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("table load control pre merge", K_(arg));
  if (OB_FAIL(ObTableLoadService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ObTableLoadStore store(table_ctx);
      if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(store.pre_merge(arg_.committed_trans_id_array_))) {
        LOG_WARN("fail to store pre merge", KR(ret));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

// start_merge
int ObDirectLoadControlStartMergeExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObDirectLoadControlStartMergeExecutor::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("table load control start merge", K_(arg));
  if (OB_FAIL(ObTableLoadService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ObTableLoadStore store(table_ctx);
      if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(store.start_merge())) {
        LOG_WARN("fail to store start merge", KR(ret));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

// commit
int ObDirectLoadControlCommitExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObDirectLoadControlCommitExecutor::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("table load control commit", K_(arg));
  if (OB_FAIL(ObTableLoadService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ObTableLoadStore store(table_ctx);
      if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(store.commit(res_.result_info_))) {
        LOG_WARN("fail to store commit", KR(ret));
      } else if (OB_FAIL(ObTableLoadService::remove_ctx(table_ctx))) {
        LOG_WARN("fail to remove table ctx", KR(ret), K(key));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

// abort
int ObDirectLoadControlAbortExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObDirectLoadControlAbortExecutor::process()
{
  int ret = OB_SUCCESS;
  LOG_INFO("table load control abort", K_(arg));
  ObTableLoadTableCtx *table_ctx = nullptr;
  ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
  if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ret = OB_SUCCESS;
      res_.is_stopped_ = true;
    }
  } else {
    ObTableLoadStore::abort_ctx(table_ctx, res_.is_stopped_);
    if (res_.is_stopped_ && OB_FAIL(ObTableLoadService::remove_ctx(table_ctx))) {
      LOG_WARN("fail to remove table ctx", KR(ret), K(key));
    }
  }
  if (OB_NOT_NULL(table_ctx)) {
    ObTableLoadService::put_ctx(table_ctx);
    table_ctx = nullptr;
  }
  return ret;
}

// get_status
int ObDirectLoadControlGetStatusExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObDirectLoadControlGetStatusExecutor::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ObTableLoadStore store(table_ctx);
      if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(store.get_status(res_.status_, res_.error_code_))) {
        LOG_WARN("fail to store get status", KR(ret));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

// heart_beath
int ObDirectLoadControlHeartBeatExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObDirectLoadControlHeartBeatExecutor::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ObTableLoadStore store(table_ctx);
      if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(store.heart_beat())) {
        LOG_WARN("fail to heart beat store", KR(ret));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

/// trans
// pre_start_trans
int ObDirectLoadControlPreStartTransExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_ ||
                  !arg_.trans_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObDirectLoadControlPreStartTransExecutor::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ObTableLoadStore store(table_ctx);
      if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(store.pre_start_trans(arg_.trans_id_))) {
        LOG_WARN("fail to store pre start trans", KR(ret), K(arg_.trans_id_));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

// confirm_start_trans
int ObDirectLoadControlConfirmStartTransExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_ ||
                  !arg_.trans_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObDirectLoadControlConfirmStartTransExecutor::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ObTableLoadStore store(table_ctx);
      if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(store.confirm_start_trans(arg_.trans_id_))) {
        LOG_WARN("fail to store confirm start trans", KR(ret), K(arg_.trans_id_));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

// pre_finish_trans
int ObDirectLoadControlPreFinishTransExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_ ||
                  !arg_.trans_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObDirectLoadControlPreFinishTransExecutor::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ObTableLoadStore store(table_ctx);
      if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(store.pre_finish_trans(arg_.trans_id_))) {
        LOG_WARN("fail to store pre finish trans", KR(ret), K(arg_.trans_id_));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

// confirm_finish_trans
int ObDirectLoadControlConfirmFinishTransExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_ ||
                  !arg_.trans_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObDirectLoadControlConfirmFinishTransExecutor::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ObTableLoadStore store(table_ctx);
      if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(store.confirm_finish_trans(arg_.trans_id_))) {
        LOG_WARN("fail to store confirm finish trans", KR(ret), K(arg_.trans_id_));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

// abandon_trans
int ObDirectLoadControlAbandonTransExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_ ||
                  !arg_.trans_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObDirectLoadControlAbandonTransExecutor::process()
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCtx *table_ctx = nullptr;
  ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
  if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
    LOG_WARN("fail to get table ctx", KR(ret), K(key));
  } else {
    ObTableLoadStore store(table_ctx);
    if (OB_FAIL(store.init())) {
      LOG_WARN("fail to init store", KR(ret));
    } else if (OB_FAIL(store.abandon_trans(arg_.trans_id_))) {
      LOG_WARN("fail to store abandon trans", KR(ret), K(arg_.trans_id_));
    }
  }
  if (OB_NOT_NULL(table_ctx)) {
    ObTableLoadService::put_ctx(table_ctx);
    table_ctx = nullptr;
  }
  return ret;
}

// get_trans_status
int ObDirectLoadControlGetTransStatusExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_ ||
                  !arg_.trans_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObDirectLoadControlGetTransStatusExecutor::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ObTableLoadStore store(table_ctx);
      if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(
                   store.get_trans_status(arg_.trans_id_, res_.trans_status_, res_.error_code_))) {
        LOG_WARN("fail to store get trans status", KR(ret));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

// insert_trans
int ObDirectLoadControlInsertTransExecutor::check_args()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == arg_.table_id_ || 0 == arg_.task_id_ ||
                  !arg_.trans_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(arg_));
  }
  return ret;
}

int ObDirectLoadControlInsertTransExecutor::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObTableLoadService::check_tenant())) {
    LOG_WARN("fail to check tenant", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
    ObTableLoadSharedAllocatorHandle allocator_handle =
      ObTableLoadSharedAllocatorHandle::make_handle("TLD_share_alloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    int64_t data_len = arg_.payload_.length();
    char *buf = nullptr;
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else if (!allocator_handle) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to make allocator handle", KR(ret));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator_handle->alloc(data_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", KR(ret));
    } else {
      int64_t pos = 0;
      ObTableLoadStore store(table_ctx);
      ObTableLoadTabletObjRowArray row_array;
      row_array.set_allocator(allocator_handle);
      MEMCPY(buf, arg_.payload_.ptr(), data_len);
      if (OB_FAIL(row_array.deserialize(buf, data_len, pos))) {
        LOG_WARN("failed to deserialize obj rows", KR(ret));
      } else if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(
                   store.write(arg_.trans_id_, arg_.session_id_, arg_.sequence_no_, row_array))) {
        LOG_WARN("fail to store write", KR(ret), K_(arg));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase
