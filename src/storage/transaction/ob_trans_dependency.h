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

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_DEPENDENCY_
#define OCEANBASE_TRANSACTION_OB_TRANS_DEPENDENCY_

#include "ob_trans_define.h"

namespace oceanbase {

namespace transaction {
class ObPartTransCtx;
class ObTransID;
class ObElrTransArrGuard;
}  // namespace transaction

namespace transaction {
class ObPartTransCtxDependencyWrap {
  friend class ObElrPrevTransArrIterator;
  friend class ObElrNextTransArrIterator;

public:
  ObPartTransCtxDependencyWrap()
  {
    reset();
  }
  ~ObPartTransCtxDependencyWrap()
  {
    destroy();
  }
  int init(ObPartTransCtx* ctx);
  void reset();
  void destroy();
  void reset_prev_trans_arr();
  void reset_next_trans_arr();
  int prev_trans_arr_assign(const ObElrTransInfoArray& info_arr);
  int get_prev_trans_arr_result(int& state);
  int64_t get_prev_trans_arr_count();
  int64_t get_next_trans_arr_count();
  int32_t get_prev_trans_commit_count() const
  {
    return prev_trans_commit_count_;
  }
  int callback_next_trans_arr(int state);
  int register_dependency_item(const uint32_t ctx_id, ObTransCtx* prev_trans_ctx);
  int unregister_dependency_item(const uint32_t ctx_id, ObTransCtx* prev_trans_ctx);
  int remove_prev_trans_item(const ObTransID& trans_id);
  int mark_prev_trans_result(const ObTransID& trans_id, int state, int& result);
  int check_can_depend_prev_elr_trans(const int64_t commit_version);
  bool is_already_callback() const
  {
    return ATOMIC_LOAD(&is_already_callback_);
  }
  int get_prev_trans_arr_guard(ObElrTransArrGuard& guard);
  int get_next_trans_arr_guard(ObElrTransArrGuard& guard);
  void clear_cur_stmt_prev_trans_item();
  int merge_cur_stmt_prev_trans_arr();

public:
  TO_STRING_KV(K_(prev_trans_arr), K_(next_trans_arr), K_(prev_trans_commit_count));

private:
  // merge prev transaction and clear duplicate item
  int add_cur_stmt_prev_trans_item_(const ObTransID& trans_id, uint32_t ctx_id, int64_t commit_version);
  int remove_prev_trans_item_(const ObTransID& trans_id, uint32_t ctx_id, int64_t commit_version);
  // merge prev transaction and clear duplicate item
  int add_next_trans_item_(const ObTransID& trans_id, uint32_t ctx_id, int64_t commit_version);
  int remove_next_trans_item_(const ObTransID& trans_id, uint32_t ctx_id, int64_t commit_version);
  int get_trans_arr_copy_(const ObElrTransInfoArray& from, ObElrTransInfoArray& to);
  int cas_prev_trans_info_state_(ObElrTransInfo& info, int state);
  int cas_next_trans_info_state_(ObElrTransInfo& info, int state);
  int get_prev_trans_arr_result_(int& state);
  const ObElrTransInfoArray& get_prev_trans_arr_() const
  {
    return prev_trans_arr_;
  }
  const ObElrTransInfoArray& get_next_trans_arr_() const
  {
    return next_trans_arr_;
  }
  int merge_cur_stmt_prev_trans_item_();

private:
  ObPartTransCtx* part_ctx_;
  ObElrTransInfoArray prev_trans_arr_;
  ObElrTransInfoArray cur_stmt_prev_trans_arr_;
  int64_t min_prev_trans_version_;
  ObElrTransInfoArray next_trans_arr_;
  common::SpinRWLock prev_trans_lock_;
  common::SpinRWLock next_trans_lock_;
  int32_t prev_trans_commit_count_;
  bool is_already_callback_;
};

class ObElrTransArrGuard {
public:
  ObElrTransArrGuard() : trans_arr_(NULL), trans_lock_(NULL)
  {}
  ~ObElrTransArrGuard();
  int set_args(ObElrTransInfoArray& trans_arr, common::SpinRWLock& trans_lock);
  ObElrTransInfoArray& get_elr_trans_arr() const
  {
    return *trans_arr_;
  }

private:
  ObElrTransInfoArray* trans_arr_;
  common::SpinRWLock* trans_lock_;
};

class ObElrTransArrIterator {
public:
  ObElrTransArrIterator(ObElrTransInfoArray& trans_arr, common::SpinRWLock& trans_lock)
      : trans_arr_(trans_arr), trans_lock_(trans_lock), index_(0)
  {}
  ~ObElrTransArrIterator()
  {}
  ObElrTransInfo* next();

private:
  ObElrTransInfoArray& trans_arr_;
  common::SpinRWLock& trans_lock_;
  int index_;
};

class ObElrPrevTransArrIterator : public ObElrTransArrIterator {
public:
  ObElrPrevTransArrIterator(ObPartTransCtxDependencyWrap& ctx_dependency_wrap)
      : ObElrTransArrIterator(ctx_dependency_wrap.prev_trans_arr_, ctx_dependency_wrap.prev_trans_lock_)
  {}
  ~ObElrPrevTransArrIterator()
  {}
};

class ObElrNextTransArrIterator : public ObElrTransArrIterator {
public:
  ObElrNextTransArrIterator(ObPartTransCtxDependencyWrap& ctx_dependency_wrap)
      : ObElrTransArrIterator(ctx_dependency_wrap.next_trans_arr_, ctx_dependency_wrap.next_trans_lock_)
  {}
  ~ObElrNextTransArrIterator()
  {}
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_TRANS_DEPENDENCY_
