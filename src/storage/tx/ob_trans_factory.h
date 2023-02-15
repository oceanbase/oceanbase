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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_FACTORY_
#define OCEANBASE_TRANSACTION_OB_TRANS_FACTORY_

#include <stdint.h>
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "storage/tx/ob_trans_define.h"
// #include "ob_trans_log.h"

namespace oceanbase
{

namespace obrpc
{
class ObGtsRpcProxy;
class ObGtiRpcProxy;
}
namespace transaction
{
class ObTransCtx;
class ObPartTransCtx;
//class ObPartitionTransCtxMgr;
class ObLSTxCtxMgr;
//class TransRpcTask;
class SubmitLogTask;
class FreezeTask;
class AllocLogIdTask;
class RollbackTransTask;
class CallbackTransTask;
class WaitTransEndTask;
class ObCoreLocalPartitionAuditInfo;
class ObDupTablePartitionInfo;
class ObDupTablePartitionMgr;
class ObDupTableLeaseInfo;
class ObDupTableRedoSyncTask;
class ObGtsRequestRpc;
class ObGtiRequestRpc;
class ObTxCommitCallbackTask;

class ObTransCtxFactory
{
public:
  static ObTransCtx *alloc(const int64_t ctx_type);
  static void release(ObTransCtx *ctx);
  static int64_t get_alloc_count() { return ATOMIC_LOAD(&active_part_ctx_count_); }
  static int64_t get_release_count() { return 0; }
  static const char *get_mod_type() { return mod_type_; }
  static int64_t get_active_part_ctx_cunt() { return ATOMIC_LOAD(&active_part_ctx_count_); }
private:
  static const char *mod_type_;
  static int64_t active_sche_ctx_count_;
  static int64_t active_coord_ctx_count_;
  static int64_t active_part_ctx_count_;
  static int64_t total_release_part_ctx_count_;
};

template <typename T, int64_t STATISTIC_INTERVAL = TRANS_MEM_STAT_INTERVAL>
class TransObjFactory
{
public:
  explicit TransObjFactory(const char *mod_type) : alloc_count_(0), release_count_(0)
  {
    (void)snprintf(mod_type_, sizeof(mod_type_) - 1, "%s", mod_type);
  }
  ~TransObjFactory() {}
  T *alloc();
  void release(T *obj);
  int64_t get_alloc_count() { return alloc_count_; }
  int64_t get_release_count() { return release_count_; }
  char *get_mod_type() { return mod_type_; }
private:
  static const int64_t MOD_TYPE_SIZE = 64;
  char mod_type_[MOD_TYPE_SIZE];
  int64_t alloc_count_;
  int64_t release_count_;
};

template <typename T, int64_t STATISTIC_INTERVAL>
T *TransObjFactory<T, STATISTIC_INTERVAL>::alloc()
{
  T *task= NULL;

  if (REACH_TIME_INTERVAL(STATISTIC_INTERVAL)) {
    TRANS_LOG(INFO, "transaction memory statistics", "mod_type", mod_type_,
      K_(alloc_count), K_(release_count), "used", alloc_count_ - release_count_);
  }

  if (NULL == (task = op_reclaim_alloc(T))) {
    TRANS_LOG_RET(WARN, common::OB_ALLOCATE_MEMORY_FAILED, "obj alloc fail", KP(task));
  } else {
    (void)ATOMIC_FAA(&alloc_count_, 1);
  }

  return task;
}

template <typename T, int64_t STATISTIC_INTERVAL>
void TransObjFactory<T, STATISTIC_INTERVAL>::release(T *obj)
{
  if (NULL == obj) {
    TRANS_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "task which should be released is null");
  } else {
    op_reclaim_free(obj);
    obj = NULL;
    (void)ATOMIC_FAA(&release_count_, 1);
  }
}

#define MAKE_FACTORY_CLASS_DEFINE_(object_name, object_name2)  \
  class object_name##Factory    \
  { \
  public: \
    static object_name2 *alloc();   \
    static void release(object_name2 *obj);  \
    static int64_t get_alloc_count(); \
    static int64_t get_release_count(); \
    static const char *get_mod_type(); \
  private: \
    static const char *mod_type_; \
    static int64_t alloc_count_; \
    static int64_t release_count_; \
  }; \

#define MAKE_FACTORY_CLASS_DEFINE(object_name) MAKE_FACTORY_CLASS_DEFINE_(object_name, object_name)
#define MAKE_FACTORY_CLASS_DEFINE_V2(object_name, object_name2) MAKE_FACTORY_CLASS_DEFINE_(object_name, object_name2)

class ObLSTxCtxMgrFactory
{
public:
  static ObLSTxCtxMgr *alloc(const uint64_t tenant_id);
  static void release(ObLSTxCtxMgr *mgr);
  static int64_t get_alloc_count();
  static int64_t get_release_count();
  static const char *get_mod_type();
private:
  static const char *mod_type_;
  static int64_t alloc_count_;
  static int64_t release_count_;
};

template <int64_t BUFSIZE>
class LogBuf
{
public:
  LogBuf() { reset(); }
  ~LogBuf() {}
  void reset() {}
  void destroy() {}
  char *get_buf() { return buf_; }
  int64_t get_size() const { return BUFSIZE; }
public:
  // When ob_resource_pool is used to allocate clog_buf and mutator buf, total number is 128 by default,
  // after this value is exceeded, ob_malloc/ob_free will be used for operation
  static const int64_t RP_TOTAL_NUM = 256;
  static const int64_t RP_RESERVE_NUM = 64;
  static const int64_t OP_LOCAL_NUM = 1;
private:
  char buf_[BUFSIZE];
};

typedef LogBuf<common::OB_MAX_LOG_ALLOWED_SIZE> ClogBuf;
typedef LogBuf<common::OB_MAX_LOG_ALLOWED_SIZE> MutatorBuf;

MAKE_FACTORY_CLASS_DEFINE(ClogBuf)
//MAKE_FACTORY_CLASS_DEFINE(TransRpcTask)
MAKE_FACTORY_CLASS_DEFINE(MutatorBuf)
MAKE_FACTORY_CLASS_DEFINE(WaitTransEndTask)
MAKE_FACTORY_CLASS_DEFINE(CallbackTransTask)
MAKE_FACTORY_CLASS_DEFINE(ObTransTraceLog)
MAKE_FACTORY_CLASS_DEFINE(ObPartitionAuditInfo)
MAKE_FACTORY_CLASS_DEFINE(ObCoreLocalPartitionAuditInfo)
MAKE_FACTORY_CLASS_DEFINE(ObDupTablePartitionInfo)
MAKE_FACTORY_CLASS_DEFINE(ObDupTablePartitionMgr)
MAKE_FACTORY_CLASS_DEFINE(ObDupTableLeaseInfo)
MAKE_FACTORY_CLASS_DEFINE(ObDupTableRedoSyncTask)
MAKE_FACTORY_CLASS_DEFINE(ObGtsRequestRpc)
MAKE_FACTORY_CLASS_DEFINE_V2(ObGtsRpcProxy, obrpc::ObGtsRpcProxy)
MAKE_FACTORY_CLASS_DEFINE(ObGtiRequestRpc)
MAKE_FACTORY_CLASS_DEFINE_V2(ObGtiRpcProxy, obrpc::ObGtiRpcProxy)
MAKE_FACTORY_CLASS_DEFINE(ObTxCommitCallbackTask)

} // transaction
} // oceanbase

#endif // OCEANBASE_TRANSACTION_OB_TRANS_FACTORY_
