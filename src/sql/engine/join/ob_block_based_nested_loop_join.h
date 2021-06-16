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

#ifndef OCEANBASE_SQL_ENGINE_JOIN_OB_BLOCK_BASED_NESTED_LOOP_JOIN_
#define OCEANBASE_SQL_ENGINE_JOIN_OB_BLOCK_BASED_NESTED_LOOP_JOIN_

#include "common/row/ob_row.h"
#include "common/row/ob_row_store.h"
#include "sql/engine/join/ob_basic_nested_loop_join.h"
namespace oceanbase {
namespace sql {
class ObExecContext;
class ObTaskInfo;

class ParamaterWrapper {
public:
  ParamaterWrapper() : paramater_list_()
  {}

  inline uint64_t hash() const
  {
    uint64_t hash_id = 0;
    for (int64_t i = 0; i < paramater_list_.count(); ++i) {
      const common::ObObjParam& param = paramater_list_.at(i);
      hash_id =
          (param.is_string_type() ? param.varchar_hash(param.get_collation_type(), hash_id) : param.hash(hash_id));
    }
    return hash_id;
  }

  inline bool operator==(const ParamaterWrapper& other) const
  {
    bool bool_ret = true;
    if (bool_ret && paramater_list_.count() != other.paramater_list_.count()) {
      bool_ret = false;
    }
    for (int64_t i = 0; bool_ret && i < paramater_list_.count(); ++i) {
      if (paramater_list_.at(i).compare(
              other.paramater_list_.at(i), other.paramater_list_.at(i).get_collation_type()) != 0) {
        bool_ret = false;
      }
    }
    return bool_ret;
  }

  inline bool operator!=(const ParamaterWrapper& other) const
  {
    return !operator==(other);
  }
  common::ObSArray<common::ObObjParam> paramater_list_;
  TO_STRING_KV("paramaters", paramater_list_);
};

class ObBLKNestedLoopJoin : public ObBasicNestedLoopJoin {
  OB_UNIS_VERSION_V(1);

private:
  enum ObJoinState {
    JS_JOIN_BEGIN = 0,
    JS_JOIN_LEFT_JOIN,
    JS_JOIN_RIGHT_JOIN,
    JS_JOIN_READ_CACHE,
    JS_JOIN_OUTER_JOIN,
    JS_JOIN_END,
    JS_STATE_COUNT
  };
  enum ObFuncType { FT_ITER_GOING, FT_ITER_END, FT_TYPE_COUNT };
  enum ObBNLJoinType { BNL_INNER_JOIN = 0, BNL_OUTER_JOIN, BNL_SEMI_JOIN, BNL_ANTI_JOIN };
  class ObBLKNestedLoopJoinCtx : public ObBasicNestedLoopJoinCtx {
    friend class ObBLKNestedLoopJoin;

  public:
    ObBLKNestedLoopJoinCtx(ObExecContext& ctx)
        : ObBasicNestedLoopJoinCtx(ctx),
          state_(JS_JOIN_BEGIN),
          stored_row_(NULL),
          left_cache_(),
          left_cache_index_(0),
          has_pass_left_end(false),
          bnl_jointype_(BNL_INNER_JOIN),
          mem_limit_(DEFAULT_MEM_LIMIT)
    {}
    virtual ~ObBLKNestedLoopJoinCtx()
    {
      clear_left_has_joined();
    }
    void reset()
    {
      state_ = JS_JOIN_BEGIN;
      stored_row_ = NULL;
      left_cache_.reset();
      left_cache_iter_.reset();
      clear_left_has_joined();
      has_pass_left_end = false;
      bnl_jointype_ = BNL_INNER_JOIN;
      mem_limit_ = DEFAULT_MEM_LIMIT;
    }
    inline bool is_left_end()
    {
      return has_pass_left_end;
    }
    inline int set_left_end_flag(bool is_end)
    {
      has_pass_left_end = is_end;
      return common::OB_SUCCESS;
    }
    inline bool is_full()
    {
      return left_cache_.get_used_mem_size() >= mem_limit_;
    }
    int init_left_has_joined(int64_t size);
    int clear_left_has_joined();
    int get_left_row_joined(int64_t index, bool& has_joined);
    int set_left_row_joined(int64_t index, bool& is_join);
    virtual void destroy()
    {
      left_cache_.~ObRowStore();
      left_has_joined_.~ObArray<bool>();
      ObBasicNestedLoopJoinCtx::destroy();
    }

  private:
    ObJoinState state_;
    common::ObNewRow left_cache_row_buf_;
    common::ObRowStore::StoredRow* stored_row_;
    common::ObRowStore left_cache_;
    common::ObRowStore::Iterator left_cache_iter_;
    common::ObArray<bool> left_has_joined_;
    int64_t left_cache_index_;
    bool has_pass_left_end;
    ObBNLJoinType bnl_jointype_;
    int64_t mem_limit_;
  };
  typedef int (ObBLKNestedLoopJoin::*state_operation_func_type)(ObBLKNestedLoopJoinCtx& join_ctx) const;
  typedef int (ObBLKNestedLoopJoin::*state_function_func_type)(
      ObBLKNestedLoopJoinCtx& join_ctx, const common::ObNewRow*& row) const;

public:
  ObBLKNestedLoopJoin(common::ObIAllocator& alloc);
  virtual ~ObBLKNestedLoopJoin();
  virtual void reset();
  virtual void resuse();
  virtual int rescan(ObExecContext& exec_ctx) const;

private:
  virtual int inner_get_next_row(ObExecContext& exec_ctx, const common::ObNewRow*& row) const;
  virtual int inner_open(ObExecContext& exec_ctx) const;
  virtual int inner_create_operator_ctx(ObExecContext& exec_ctx, ObPhyOperatorCtx*& op_ctx) const;
  int join_end_operate(ObBLKNestedLoopJoinCtx& join_ctx) const;
  int join_end_func_end(ObBLKNestedLoopJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int join_begin_operate(ObBLKNestedLoopJoinCtx& join_ctx) const;
  int join_begin_func_going(ObBLKNestedLoopJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int join_begin_func_end(ObBLKNestedLoopJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int join_left_operate(ObBLKNestedLoopJoinCtx& join_ctx) const;
  int join_left_func_going(ObBLKNestedLoopJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int join_left_func_end(ObBLKNestedLoopJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int join_right_operate(ObBLKNestedLoopJoinCtx& join_ctx) const;
  int join_right_func_going(ObBLKNestedLoopJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int join_right_func_end(ObBLKNestedLoopJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int join_read_cache_operate(ObBLKNestedLoopJoinCtx& join_ctx) const;
  int join_read_cache_func_going(ObBLKNestedLoopJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int join_read_cache_func_end(ObBLKNestedLoopJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int join_outer_join_operate(ObBLKNestedLoopJoinCtx& join_ctx) const;
  int join_outer_join_func_going(ObBLKNestedLoopJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int join_outer_join_func_end(ObBLKNestedLoopJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int read_row_from_left_cache(ObBLKNestedLoopJoinCtx& join_ctx) const;
  int init_right_child(ObBLKNestedLoopJoinCtx& join_ctx) const;
  int make_paramater_warpper(ObBLKNestedLoopJoinCtx& join_ctx, ParamaterWrapper& params) const;

private:
  state_operation_func_type state_operation_func_[JS_STATE_COUNT];
  state_function_func_type state_function_func_[JS_STATE_COUNT][FT_TYPE_COUNT];
};
}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_ENGINE_JOIN_OB_BLOCK_BASED_NESTED_LOOP_JOIN_
