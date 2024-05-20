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

#ifndef OCEANBASE_TRANSACTION_OB_TX_RETAIN_CTX_MGR_
#define OCEANBASE_TRANSACTION_OB_TX_RETAIN_CTX_MGR_

#include "lib/list/ob_dlink_node.h"
#include "lib/list/ob_list.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{

namespace storage
{
class ObLS;
}

namespace transaction
{

class ObPartTransCtx;
class ObTxRetainCtxMgr;

class ObAdvanceLSCkptTask : public ObTransTask
{
public:
  ObAdvanceLSCkptTask(share::ObLSID ls_id, share::SCN target_ts);
  ~ObAdvanceLSCkptTask() { reset(); }
  void reset();

  int try_advance_ls_ckpt_ts();

private:
  share::ObLSID ls_id_;
  share::SCN target_ckpt_ts_;
};

/**
 * OB_SUCCESS : can be removed
 * OB_EGAIN: need wait
 * **/
class ObIRetainCtxCheckFunctor : public common::ObDLinkBase<ObIRetainCtxCheckFunctor>
// : public common::ObDLinkBase<ObIRetainCtxCheckFunctor>
{
public:
  ObIRetainCtxCheckFunctor();
  ~ObIRetainCtxCheckFunctor();
  int init(ObPartTransCtx *ctx, RetainCause cause);
  // invoke before removed from retain ctx mgr
  // virtual int gc_retain_ctx() = 0;
  virtual int operator()(storage::ObLS *ls, ObTxRetainCtxMgr *retain_mgr) = 0;
  RetainCause get_retain_cause() { return cause_; }
  virtual bool is_valid() { return OB_NOT_NULL(tx_ctx_) && cause_ != RetainCause::UNKOWN; }
  int del_retain_ctx();

  TO_STRING_KV(K(cause_), K(tx_id_), K(ls_id_), KP(tx_ctx_));

protected:
  RetainCause cause_;
  ObPartTransCtx *tx_ctx_;
  ObTransID tx_id_;
  share::ObLSID ls_id_;
};

class ObMDSRetainCtxFunctor : public ObIRetainCtxCheckFunctor
{
public:
  ObMDSRetainCtxFunctor() : ObIRetainCtxCheckFunctor()
  {
    final_log_ts_.reset();
  }
  int init(ObPartTransCtx *ctx,
           RetainCause cause,
           const share::SCN &final_log_ts);

  virtual int operator()(storage::ObLS *ls, ObTxRetainCtxMgr *retain_mgr) override;
  virtual bool is_valid() override;

private:
  share::SCN final_log_ts_;
};

typedef common::ObDList<ObIRetainCtxCheckFunctor> RetainCtxList;

class ObTxRetainCtxMgr
{
public:
  // const int64_t PRINT_INFO_INTERVAL = 5 * 1000 * 1000; // 5s
  const int64_t TRIGGER_RETAIN_CTX_GC_LIMIT = 50 * 1000;

  typedef int (ObTxRetainCtxMgr::*RetainFuncHandler)(ObIRetainCtxCheckFunctor *func_ptr,
                                                     bool &need_remove,
                                                     storage::ObLS *ls);

public:
  ObTxRetainCtxMgr() : retain_ctx_list_() { reset(); }
  void reset();

  static void *alloc_object(const int64_t size);
  static void free_object(void *ptr);

  int push_retain_ctx(ObIRetainCtxCheckFunctor *retain_func, int64_t timeout_us);
  int try_gc_retain_ctx(storage::ObLS *ls);
  int print_retain_ctx_info(share::ObLSID ls_id);
  int force_gc_retain_ctx();

  void set_max_ckpt_ts(share::SCN ckpt_ts) { max_wait_ckpt_ts_.inc_update(ckpt_ts); }
  // share::SCN get_max_ckpt_ts() { return ATOMIC_LOAD(&max_wait_ckpt_ts_); }
  int64_t get_retain_ctx_cnt() { return retain_ctx_list_.get_size(); }

  void try_advance_retain_ctx_gc(share::ObLSID ls_id);

  TO_STRING_KV(K(retain_ctx_list_.get_size()),
               K(max_wait_ckpt_ts_),
               K(last_push_gc_task_ts_),
               K(skip_remove_cnt_));

private:
  int remove_ctx_func_(ObIRetainCtxCheckFunctor *remove_iter);
  int for_each_remove_(RetainFuncHandler remove_handler,
                       storage::ObLS *ls,
                       const int64_t max_run_us);
  int try_gc_(ObIRetainCtxCheckFunctor *func_ptr, bool &need_remove, storage::ObLS *ls);
  int force_gc_(ObIRetainCtxCheckFunctor *func_ptr, bool &need_remove, storage::ObLS *ls);

private:
  common::SpinRWLock retain_ctx_lock_;
  RetainCtxList retain_ctx_list_;

  share::SCN max_wait_ckpt_ts_;
  int64_t last_push_gc_task_ts_;

  int64_t skip_remove_cnt_;

  TransModulePageAllocator reserve_allocator_;
};

} // namespace transaction
} // namespace oceanbase
#endif
