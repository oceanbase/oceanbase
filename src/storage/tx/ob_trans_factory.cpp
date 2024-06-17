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
#include "ob_trans_part_ctx.h"
#include "ob_trans_ctx_mgr.h"
#include "ob_trans_version_mgr.h"
#include "lib/objectpool/ob_server_object_pool.h"
#include "storage/tx/ob_dup_table.h"
#include "storage/tx/ob_gts_rpc.h"
#include "storage/tx/ob_gti_rpc.h"
#include "storage/tx/ob_trans_end_trans_callback.h"
#include "storage/tx/ob_leak_checker.h"
#include "observer/ob_server.h"

namespace oceanbase
{

using namespace common;
using namespace obrpc;

namespace transaction
{
int64_t ObTransCtxFactory::active_coord_ctx_count_ CACHE_ALIGNED = 0;
int64_t ObTransCtxFactory::active_part_ctx_count_ CACHE_ALIGNED = 0;
int64_t ObTransCtxFactory::total_release_part_ctx_count_ CACHE_ALIGNED = 0;
const char *ObTransCtxFactory::mod_type_ = "OB_TRANS_CTX";

int64_t ObLSTxCtxMgrFactory::alloc_count_ = 0;
int64_t ObLSTxCtxMgrFactory::release_count_ = 0;
const char *ObLSTxCtxMgrFactory::mod_type_ = "OB_PARTITION_TRANS_CTX_MGR";


//static TransObjFactory<TransRpcTask> trans_rpc_task_factory("OB_TRANS_RPC_TASK");

#define OB_FREE(object, ...) ob_free(object)
#define RP_FREE(object, LABEL) rp_free(object, LABEL)
#define OB_ALLOC(object, LABEL) object##alloc()

#define RP_ALLOC(object, LABEL) rp_alloc(object, LABEL)

#define MAKE_OB_ALLOC(object_name, LABEL)  \
  object_name *object_name##alloc() \
  {  \
    object_name *object = NULL;  \
    object = (object_name*)ob_malloc(sizeof(object_name), LABEL);  \
    return object == NULL ? object : new(object) object_name(); \
  }  \


#define MAKE_FACTORY_CLASS_IMPLEMENT(object_name, LABEL, allocator_type, arg...)  \
  int64_t object_name##Factory::alloc_count_ = 0; \
  int64_t object_name##Factory::release_count_ = 0; \
  const char *object_name##Factory::mod_type_ = #LABEL; \
  object_name *object_name##Factory::alloc(arg)  \
  {  \
    object_name *object = NULL;    \
    if (REACH_TIME_INTERVAL(TRANS_MEM_STAT_INTERVAL)) {  \
      TRANS_LOG(INFO, "trans factory statistics",  \
                "object_name", #object_name,       \
                "label", #LABEL,                   \
                K_(alloc_count), K_(release_count), "used", alloc_count_ - release_count_);  \
    }  \
    if (!ObTransErrsim::is_memory_errsim() && NULL != (object = allocator_type##_ALLOC(object_name, LABEL))) { \
      (void)ATOMIC_FAA(&alloc_count_, 1);  \
    }  \
    return object;  \
  }                                             \
  void object_name##Factory::release(object_name *object)  \
  {\
    if (OB_ISNULL(object)) {\
      TRANS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "object is null", KP(object));\
    } else {\
      object->destroy();  \
      allocator_type##_FREE(object, LABEL);        \
      object = NULL;\
      (void)ATOMIC_FAA(&release_count_, 1);\
    }\
  }\
  int64_t object_name##Factory::get_alloc_count()  \
  {  \
    return alloc_count_;  \
  }  \
  int64_t object_name##Factory::get_release_count()\
  {\
    return release_count_;\
  }\
  const char *object_name##Factory::get_mod_type()\
  {\
    return mod_type_;\
  }\

#define MAKE_FACTORY_CLASS_IMPLEMENT_USE_OB_ALLOC(object_name, LABEL, arg...) MAKE_FACTORY_CLASS_IMPLEMENT(object_name, LABEL, OB, arg)
#define MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(object_name, LABEL, arg...) MAKE_FACTORY_CLASS_IMPLEMENT(object_name, LABEL, RP, arg)

ObTransCtx *ObTransCtxFactory::alloc(const int64_t ctx_type)
{
  int tmp_ret = OB_SUCCESS;
  ObTransCtx *ctx = NULL;

  if (OB_LIKELY(!ObTransErrsim::is_memory_errsim())) {
    if (ObTransCtxType::PARTICIPANT == ctx_type) {
      // During restart, the number of transaction contexts is relatively large
      // and cannot be limited, otherwise there will be circular dependencies
      if (ATOMIC_LOAD(&active_part_ctx_count_) > MAX_PART_CTX_COUNT && GCTX.status_ == observer::SS_SERVING) {
        TRANS_LOG_RET(ERROR, tmp_ret, "participant context memory alloc failed", K_(active_part_ctx_count));
        tmp_ret = OB_TRANS_CTX_COUNT_REACH_LIMIT;
      } else if (NULL != (ctx = mtl_sop_borrow(ObPartTransCtx))) {
        (void)ATOMIC_FAA(&active_part_ctx_count_, 1);
        TRANS_LOG(DEBUG,
                  "[Tx Ctx] alloc part_ctx success",
                  KP(ctx),
                  K(active_part_ctx_count_),
                  K(total_release_part_ctx_count_));
      } else {
        // do nothing
      }
    } else {
      tmp_ret = OB_ERR_UNEXPECTED;
      TRANS_LOG_RET(ERROR, tmp_ret, "unexpected error when context alloc", K(ctx_type));
    }
  }
  if (REACH_TIME_INTERVAL(TRANS_MEM_STAT_INTERVAL)) {
    TRANS_LOG(INFO, "ObTransCtx statistics",
      K_(active_coord_ctx_count),
      K_(active_part_ctx_count),
      K_(total_release_part_ctx_count));
      (void)ATOMIC_STORE(&total_release_part_ctx_count_, 0);
  }

  (void) tmp_ret; // make compiler happy
  return ctx;
}

void ObTransCtxFactory::release(ObTransCtx *ctx)
{
  if (OB_ISNULL(ctx)) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "context pointer is null when released", KP(ctx));
  } else {
    ObPartTransCtx *part_ctx = static_cast<ObPartTransCtx *>(ctx);
    part_ctx->destroy();
    mtl_sop_return(ObPartTransCtx, part_ctx);
    (void)ATOMIC_FAA(&active_part_ctx_count_, -1);
    (void)ATOMIC_FAA(&total_release_part_ctx_count_, 1);
    TRANS_LOG(DEBUG,
              "[Tx Ctx] release part_ctx success",
              KP(ctx),
              K(active_part_ctx_count_),
              K(total_release_part_ctx_count_));
    ctx = NULL;
  }
}

//ObLSTxCtxMgrFactory
ObLSTxCtxMgr *ObLSTxCtxMgrFactory::alloc(const uint64_t tenant_id)
{
  void *ptr = NULL;
  ObLSTxCtxMgr *partition_trans_ctx_mgr = NULL;
  ObMemAttr memattr(tenant_id, ObModIds::OB_PARTITION_TRANS_CTX_MGR, ObCtxIds::TRANS_CTX_MGR_ID);
  if (REACH_TIME_INTERVAL(TRANS_MEM_STAT_INTERVAL)) {
    TRANS_LOG(INFO, "ObLSTxCtxMgr statistics",
      K_(alloc_count), K_(release_count), "used", alloc_count_ - release_count_);
  }
  if (!is_valid_tenant_id(tenant_id)) {
    TRANS_LOG_RET(WARN, OB_INVALID_ARGUMENT, "invalid tenant_id", K(tenant_id));
  } else if (NULL != (ptr = ob_malloc(sizeof(ObLSTxCtxMgr), memattr))) {
    partition_trans_ctx_mgr = new(ptr) ObLSTxCtxMgr;
    (void)ATOMIC_FAA(&alloc_count_, 1);
  }
  TRANS_LOG(INFO, "alloc ls tx ctx mgr", KP(partition_trans_ctx_mgr));
  return partition_trans_ctx_mgr;
}

void ObLSTxCtxMgrFactory::release(ObLSTxCtxMgr *partition_trans_ctx_mgr)
{
  if (OB_ISNULL(partition_trans_ctx_mgr)) {
    TRANS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "ObLSTxCtxMgr pointer is null when released",
      KP(partition_trans_ctx_mgr));
  } else {
    partition_trans_ctx_mgr->~ObLSTxCtxMgr();
    ob_free(partition_trans_ctx_mgr);
    TRANS_LOG(INFO, "release ls tx ctx mgr", KP(partition_trans_ctx_mgr));
    partition_trans_ctx_mgr = NULL;
    (void)ATOMIC_FAA(&release_count_, 1);
  }
}

int64_t ObLSTxCtxMgrFactory::get_alloc_count()
{
  return alloc_count_;
}

int64_t ObLSTxCtxMgrFactory::get_release_count()
{
  return release_count_;
}

const char *ObLSTxCtxMgrFactory::get_mod_type()
{
  return mod_type_;
}

/*
//TransRpcTaskFactory
TransRpcTask *TransRpcTaskFactory::alloc()
{
  return trans_rpc_task_factory.alloc();
}

void TransRpcTaskFactory::release(TransRpcTask *task)
{
  if (OB_ISNULL(task)) {
    // ignore ret
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

const char *TransRpcTaskFactory::get_mod_type()
{
  return trans_rpc_task_factory.get_mod_type();
}
*/

MAKE_OB_ALLOC(ObDupTablePartitionMgr, ObModIds::OB_DUP_TABLE_PARTITION_MGR)
MAKE_OB_ALLOC(ObGtsRpcProxy, ObModIds::OB_GTS_RPC_PROXY)
MAKE_OB_ALLOC(ObGtsRequestRpc, ObModIds::OB_GTS_REQUEST_RPC)
MAKE_OB_ALLOC(ObGtiRpcProxy, ObModIds::OB_GTI_RPC_PROXY)
MAKE_OB_ALLOC(ObGtiRequestRpc, ObModIds::OB_GTI_REQUEST_RPC)

MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(ClogBuf, ObModIds::OB_TRANS_CLOG_BUF)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(MutatorBuf, ObModIds::OB_TRANS_MUTATOR_BUF)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(ObTransTraceLog, ObModIds::OB_TRANS_AUDIT_RECORD)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(ObPartitionAuditInfo, ObModIds::OB_PARTITION_AUDIT_INFO)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(ObCoreLocalPartitionAuditInfo, ObModIds::OB_CORE_LOCAL_STORAGE)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(ObDupTablePartitionInfo, ObModIds::OB_DUP_TABLE_PARTITION_INFO)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_OB_ALLOC(ObDupTablePartitionMgr, ObModIds::OB_DUP_TABLE_PARTITION_MGR)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(ObDupTableLeaseInfo, ObModIds::OB_DUP_TABLE_LEASE_INFO)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(ObDupTableRedoSyncTask, ObModIds::OB_DUP_TABLE_REDO_SYNC_TASK)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_OB_ALLOC(ObGtsRpcProxy, ObModIds::OB_GTS_RPC_PROXY)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_OB_ALLOC(ObGtsRequestRpc, ObModIds::OB_GTS_REQUEST_RPC)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_OB_ALLOC(ObGtiRpcProxy, ObModIds::OB_GTI_RPC_PROXY)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_OB_ALLOC(ObGtiRequestRpc, ObModIds::OB_GTI_REQUEST_RPC)
MAKE_FACTORY_CLASS_IMPLEMENT_USE_RP_ALLOC(ObTxCommitCallbackTask, ObModIds::OB_END_TRANS_CB_TASK)

void *MultiTxDataFactory::alloc(const int64_t len, const uint64_t arg1, const uint64_t arg2)
{
  const char *mod_name = "MultiTxData";
  void *ptr = mtl_malloc(len, mod_name);
  if (NULL != ptr) {
    ObLeakChecker::reg((uint64_t)ptr, arg1, arg2, mod_name);
  }
  return ptr;
}

void MultiTxDataFactory::free(void *ptr)
{
  if (NULL != ptr) {
    ObLeakChecker::unreg((uint64_t)ptr);
    mtl_free(ptr);
  }
}

} // transaction
} // oceanbase
