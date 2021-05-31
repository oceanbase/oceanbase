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

#include "ob_trans_factory.h"
#include "lib/objectpool/ob_resource_pool.h"
#include "ob_trans_rpc.h"
#include "ob_trans_define.h"

#include "ob_trans_ctx.h"
#include "ob_trans_sche_ctx.h"
#include "ob_trans_coord_ctx.h"
#include "ob_trans_part_ctx.h"
#include "ob_trans_slave_ctx.h"
#include "ob_trans_ctx_mgr.h"
#include "ob_trans_version_mgr.h"
#include "ob_clog_adapter.h"
#include "ob_trans_result_info_mgr.h"
#include "ob_trans_elr_task.h"
#include "lib/objectpool/ob_server_object_pool.h"
#include "storage/transaction/ob_dup_table.h"
#include "storage/transaction/ob_gts_rpc.h"
#include "storage/transaction/ob_trans_task_worker.h"
#include "storage/transaction/ob_trans_end_trans_callback.h"
#include "observer/ob_server.h"

namespace oceanbase {

using namespace common;
using namespace obrpc;

namespace transaction {
int64_t ObTransCtxFactory::active_sche_ctx_count_ CACHE_ALIGNED = 0;
int64_t ObTransCtxFactory::active_coord_ctx_count_ CACHE_ALIGNED = 0;
int64_t ObTransCtxFactory::active_part_ctx_count_ CACHE_ALIGNED = 0;
int64_t ObTransCtxFactory::total_release_part_ctx_count_ CACHE_ALIGNED = 0;
const char* ObTransCtxFactory::mod_type_ = "OB_TRANS_CTX";

int64_t ObPartitionTransCtxMgrFactory::alloc_count_ = 0;
int64_t ObPartitionTransCtxMgrFactory::release_count_ = 0;
const char* ObPartitionTransCtxMgrFactory::mod_type_ = "OB_PARTITION_TRANS_CTX_MGR";

static TransObjFactory<TransRpcTask> trans_rpc_task_factory("OB_TRANS_RPC_TASK");

#define OB_FREE(object, ...) ob_free(object)
#define RP_FREE(object, LABEL) rp_free(object, LABEL)
#define OB_ALLOC(object, LABEL) new (ob_malloc(sizeof(object), LABEL)) object()
#define RP_ALLOC(object, LABEL) rp_alloc(object, LABEL)

#define MAKE_FACTORY_CLASS_IMPLEMENT(object_name, LABEL, allocator_type, arg...)   \
  int64_t object_name##Factory::alloc_count_ = 0;                                  \
  int64_t object_name##Factory::release_count_ = 0;                                \
  const char* object_name##Factory::mod_type_ = #LABEL;                            \
  object_name* object_name##Factory::alloc(arg)                                    \
  {                                                                                \
    object_name* object = NULL;                                                    \
    if (REACH_TIME_INTERVAL(TRANS_MEM_STAT_INTERVAL)) {                            \
      TRANS_LOG(INFO,                                                              \
          "trans factory statistics",                                              \
          "object_name",                                                           \
          #object_name,                                                            \
          "label",                                                                 \
          #LABEL,                                                                  \
          K_(alloc_count),                                                         \
          K_(release_count),                                                       \
          "used",                                                                  \
          alloc_count_ - release_count_);                                          \
    }                                                                              \
    if (!ObTransErrsim::is_memory_errsim() &&                                      \
        NULL != (object = allocator_type##_ALLOC(object_name, ObModIds::LABEL))) { \
      (void)ATOMIC_FAA(&alloc_count_, 1);                                          \
    }                                                                              \
    return object;                                                                 \
  }                                                                                \
  void object_name##Factory::release(object_name* object)                          \
  {                                                                                \
    if (OB_ISNULL(object)) {                                                       \
      TRANS_LOG(WARN, "object is null", KP(object));                               \
    } else {                                                                       \
      object->destroy();                                                           \
      allocator_type##_FREE(object, ObModIds::LABEL);                              \
      object = NULL;                                                               \
      (void)ATOMIC_FAA(&release_count_, 1);                                        \
    }                                                                              \
  }                                                                                \
  int64_t object_name##Factory::get_alloc_count()                                  \
  {                                                                                \
    return alloc_count_;                                                           \
  }                                                                                \
  int64_t object_name##Factory::get_release_count()                                \
  {                                                                                \
    return release_count_;                                                         \
  }                                                                                \
  const char* object_name##Factory::get_mod_type()                                 \
  {                                                                                \
    return mod_type_;                                                              \
  }

#define MAKE_FACTORY_CLASS_IMPLEMENT_USE_OB_ALLOC(object_name, LABEL, arg...) \
  MAKE_FACTORY_CLASS_IMPLEMENT(object_name, LABEL, OB, arg)
#define MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(object_name, LABEL, arg...) \
  MAKE_FACTORY_CLASS_IMPLEMENT(object_name, LABEL, RP, arg)

ObTransCtx* ObTransCtxFactory::alloc(const int64_t ctx_type)
{
  int tmp_ret = OB_SUCCESS;
  ObTransCtx* ctx = NULL;

  if (OB_LIKELY(!ObTransErrsim::is_memory_errsim())) {
    if (ObTransCtxType::SCHEDULER == ctx_type) {
      if (OB_UNLIKELY(ATOMIC_LOAD(&active_sche_ctx_count_) > MAX_SCHE_CTX_COUNT)) {
        TRANS_LOG(ERROR, "scheduler context memory alloc failed", K_(active_sche_ctx_count));
        tmp_ret = OB_TRANS_CTX_COUNT_REACH_LIMIT;
      } else if (NULL != (ctx = op_reclaim_alloc(ObScheTransCtx))) {
        (void)ATOMIC_FAA(&active_sche_ctx_count_, 1);
      } else {
        // do nothing
      }
    } else if (ObTransCtxType::COORDINATOR == ctx_type) {
      if (NULL != (ctx = op_reclaim_alloc(ObCoordTransCtx))) {
        (void)ATOMIC_FAA(&active_coord_ctx_count_, 1);
      }
    } else if (ObTransCtxType::PARTICIPANT == ctx_type) {
      // During restart, the number of transaction contexts is relatively large
      // and cannot be limited, otherwise there will be circular dependencies
      if (ATOMIC_LOAD(&active_part_ctx_count_) > MAX_PART_CTX_COUNT && GCTX.status_ == observer::SS_SERVING) {
        TRANS_LOG(ERROR, "participant context memory alloc failed", K_(active_part_ctx_count));
        tmp_ret = OB_TRANS_CTX_COUNT_REACH_LIMIT;
        //} else if (NULL != (ctx = op_reclaim_alloc(ObPartTransCtx))) {
      } else if (NULL != (ctx = sop_borrow(ObPartTransCtx))) {
        (void)ATOMIC_FAA(&active_part_ctx_count_, 1);
      } else {
        // do nothing
      }
    } else if (ObTransCtxType::SLAVE_PARTICIPANT == ctx_type) {
      // During restart, the number of transaction contexts is relatively large
      // and cannot be limited, otherwise there will be circular dependencies
      if (ATOMIC_LOAD(&active_part_ctx_count_) > MAX_PART_CTX_COUNT && GCTX.status_ == observer::SS_SERVING) {
        TRANS_LOG(ERROR, "slave participant context memory alloc failed", K_(active_part_ctx_count));
        tmp_ret = OB_TRANS_CTX_COUNT_REACH_LIMIT;
      } else if (NULL != (ctx = op_reclaim_alloc(ObSlaveTransCtx))) {
        (void)ATOMIC_FAA(&active_part_ctx_count_, 1);
      } else {
        // do nothing
      }
    } else {
      TRANS_LOG(ERROR, "unexpected error when context alloc", K(ctx_type));
      tmp_ret = OB_ERR_UNEXPECTED;
    }
  }
  if (REACH_TIME_INTERVAL(TRANS_MEM_STAT_INTERVAL)) {
    TRANS_LOG(INFO,
        "ObTransCtx statistics",
        K_(active_sche_ctx_count),
        K_(active_coord_ctx_count),
        K_(active_part_ctx_count),
        K_(total_release_part_ctx_count));
    (void)ATOMIC_STORE(&total_release_part_ctx_count_, 0);
  }

  (void)tmp_ret;  // make compiler happy
  return ctx;
}

void ObTransCtxFactory::release(ObTransCtx* ctx)
{
  if (OB_ISNULL(ctx)) {
    TRANS_LOG(ERROR, "context pointer is null when released", KP(ctx));
  } else {
    const int64_t ctx_type = ctx->get_type();
    ctx->destroy();
    if (ObTransCtxType::SCHEDULER == ctx_type) {
      op_reclaim_free(static_cast<ObScheTransCtx*>(ctx));
      (void)ATOMIC_FAA(&active_sche_ctx_count_, -1);
    } else if (ObTransCtxType::COORDINATOR == ctx_type) {
      op_reclaim_free(static_cast<ObCoordTransCtx*>(ctx));
      (void)ATOMIC_FAA(&active_coord_ctx_count_, -1);
    } else if (ObTransCtxType::PARTICIPANT == ctx_type) {
      // op_reclaim_free(static_cast<ObPartTransCtx *>(ctx));
      // static_cast<ObPartTransCtx *>(ctx)->reset();
      sop_return(ObPartTransCtx, static_cast<ObPartTransCtx*>(ctx));
      (void)ATOMIC_FAA(&active_part_ctx_count_, -1);
      (void)ATOMIC_FAA(&total_release_part_ctx_count_, 1);
    } else if (ObTransCtxType::SLAVE_PARTICIPANT == ctx_type) {
      op_reclaim_free(static_cast<ObSlaveTransCtx*>(ctx));
      (void)ATOMIC_FAA(&active_part_ctx_count_, -1);
      (void)ATOMIC_FAA(&total_release_part_ctx_count_, 1);
    } else {
      TRANS_LOG(ERROR, "unexpected error when transaction context released", K(ctx_type));
    }
    ctx = NULL;
  }
}

// ObPartitionTransCtxMgrFactory
ObPartitionTransCtxMgr* ObPartitionTransCtxMgrFactory::alloc(const uint64_t tenant_id)
{
  void* ptr = NULL;
  ObPartitionTransCtxMgr* partition_trans_ctx_mgr = NULL;
  ObMemAttr memattr(tenant_id, ObModIds::OB_PARTITION_TRANS_CTX_MGR, ObCtxIds::TRANS_CTX_MGR_ID);
  if (REACH_TIME_INTERVAL(TRANS_MEM_STAT_INTERVAL)) {
    TRANS_LOG(INFO,
        "ObPartitionTransCtxMgr statistics",
        K_(alloc_count),
        K_(release_count),
        "used",
        alloc_count_ - release_count_);
  }
  if (!is_valid_tenant_id(tenant_id)) {
    TRANS_LOG(WARN, "invalid tenant_id", K(tenant_id));
  } else if (NULL != (ptr = ob_malloc(sizeof(ObPartitionTransCtxMgr), memattr))) {
    partition_trans_ctx_mgr = new (ptr) ObPartitionTransCtxMgr;
    (void)ATOMIC_FAA(&alloc_count_, 1);
  }
  return partition_trans_ctx_mgr;
}

void ObPartitionTransCtxMgrFactory::release(ObPartitionTransCtxMgr* partition_trans_ctx_mgr)
{
  if (OB_ISNULL(partition_trans_ctx_mgr)) {
    TRANS_LOG(ERROR, "ObPartitionTransCtxMgr pointer is null when released", KP(partition_trans_ctx_mgr));
  } else {
    partition_trans_ctx_mgr->~ObPartitionTransCtxMgr();
    ob_free(partition_trans_ctx_mgr);
    partition_trans_ctx_mgr = NULL;
    (void)ATOMIC_FAA(&release_count_, 1);
  }
}

int64_t ObPartitionTransCtxMgrFactory::get_alloc_count()
{
  return alloc_count_;
}

int64_t ObPartitionTransCtxMgrFactory::get_release_count()
{
  return release_count_;
}

const char* ObPartitionTransCtxMgrFactory::get_mod_type()
{
  return mod_type_;
}

// TransRpcTaskFactory
TransRpcTask* TransRpcTaskFactory::alloc()
{
  return trans_rpc_task_factory.alloc();
}

void TransRpcTaskFactory::release(TransRpcTask* task)
{
  if (OB_ISNULL(task)) {
    TRANS_LOG(ERROR, "TransRpcTask pointer is null when released", KP(task));
  } else {
    trans_rpc_task_factory.release(task);
    task = NULL;
  }
}

int64_t TransRpcTaskFactory::get_alloc_count()
{
  return trans_rpc_task_factory.get_alloc_count();
}

int64_t TransRpcTaskFactory::get_release_count()
{
  return trans_rpc_task_factory.get_release_count();
}

const char* TransRpcTaskFactory::get_mod_type()
{
  return trans_rpc_task_factory.get_mod_type();
}

MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(ClogBuf, OB_TRANS_CLOG_BUF)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(MutatorBuf, OB_TRANS_MUTATOR_BUF)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(SubmitLogTask, OB_TRANS_SUBMIT_LOG_TASK)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(AllocLogIdTask, OB_TRANS_ALLOC_LOG_ID_TASK)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(BigTransCallbackTask, OB_TRANS_ALLOC_LOG_ID_TASK)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(BackfillNopLogTask, OB_TRANS_BACKFILL_NOP_LOG_TASK)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(WaitTransEndTask, OB_TRANS_WAIT_TRANS_END_TASK)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(CallbackTransTask, OB_TRANS_CALLBACK_TRANS_TASK)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(ObTransResultInfo, OB_TRANS_RESULT_INFO)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(ObTransResultInfoMgr, OB_TRANS_RESULT_INFO_MGR)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(ObTransTraceLog, OB_TRANS_AUDIT_RECORD)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(ObPartitionAuditInfo, OB_PARTITION_AUDIT_INFO)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(ObCoreLocalPartitionAuditInfo, OB_CORE_LOCAL_STORAGE)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(ObDupTablePartitionInfo, OB_DUP_TABLE_PARTITION_INFO)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_OB_ALLOC(ObDupTablePartitionMgr, OB_DUP_TABLE_PARTITION_MGR)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(ObDupTableLeaseInfo, OB_DUP_TABLE_LEASE_INFO)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(ObDupTableRedoSyncTask, OB_DUP_TABLE_REDO_SYNC_TASK)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_OB_ALLOC(ObGtsRpcProxy, OB_GTS_RPC_PROXY)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_OB_ALLOC(ObGtsRequestRpc, OB_GTS_REQUEST_RPC)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(EndTransCallbackTask, OB_END_TRANS_CB_TASK)

}  // namespace transaction
}  // namespace oceanbase
