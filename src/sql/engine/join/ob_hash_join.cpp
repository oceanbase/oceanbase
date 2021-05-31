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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/join/ob_hash_join.h"

#include "common/row/ob_row_util.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/join/ob_hj_partition.h"
#include "common/object/ob_object.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase {
using namespace omt;
using namespace common;
namespace sql {
using namespace join;

int64_t ObHashJoin::PART_COUNT = 100;
int64_t ObHashJoin::MAX_PAGE_COUNT = 1600;  // unused, use GCONF._hash_area_size to calc one
int8_t ObHashJoin::HJ_PROCESSOR_ALGO = 0;
bool ObHashJoin::TEST_NEST_LOOP_TO_RECURSIVE = false;

ObHashJoin::ObPartHashJoinCtx::ObPartHashJoinCtx(ObExecContext& ctx)
    : ObHashJoinCtx(ctx),
      state_(JS_READ_RIGHT),
      hash_value_(0),
      right_has_matched_(false),
      tuple_need_join_(false),
      first_get_row_(true),
      cur_bkid_(0),
      remain_data_memory_size_(0),
      nth_nest_loop_(0),
      cur_nth_row_(0),
      hash_table_(),
      cur_tuple_(NULL),
      cur_left_row_(),
      mem_context_(nullptr),
      alloc_(nullptr),
      need_cache_info_(true),
      equal_cond_info_(),
      nth_right_row_(-1),
      cur_dumped_partition_(MAX_PART_COUNT),
      nest_loop_state_(HJNestLoopState::START),
      is_last_chunk_(false),
      has_right_bitset_(false),
      hj_part_array_(NULL),
      right_hj_part_array_(NULL),
      left_read_row_(NULL),
      right_read_row_(NULL),
      bitset_filter_cnt_(0),
      probe_cnt_(0),
      hash_equal_cnt_(0),
      hash_link_cnt_(0)
{}

int ObHashJoin::PartHashJoinTable::init(ObIAllocator& alloc)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    void* alloc_buf = alloc.alloc(sizeof(ModulePageAllocator));
    void* bucket_buf = alloc.alloc(sizeof(BucketArray));
    void* cell_buf = alloc.alloc(sizeof(AllCellArray));
    void* collision_buf = alloc.alloc(sizeof(CollisionCntArray));
    if (OB_ISNULL(bucket_buf) || OB_ISNULL(cell_buf) || OB_ISNULL(collision_buf) || OB_ISNULL(alloc_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      if (OB_NOT_NULL(bucket_buf)) {
        alloc.free(bucket_buf);
      }
      if (OB_NOT_NULL(cell_buf)) {
        alloc.free(cell_buf);
      }
      if (OB_NOT_NULL(collision_buf)) {
        alloc.free(collision_buf);
      }
      if (OB_NOT_NULL(alloc_buf)) {
        alloc.free(alloc_buf);
      }
      LOG_WARN("failed to alloc memory", K(ret));
    } else {
      ht_alloc_ = new (alloc_buf) ModulePageAllocator(alloc);
      ht_alloc_->set_label("HtAlloc");
      buckets_ = new (bucket_buf) BucketArray(*ht_alloc_);
      all_cells_ = new (cell_buf) AllCellArray(*ht_alloc_);
      collision_cnts_ = new (collision_buf) CollisionCntArray(*ht_alloc_);
    }
  }
  return ret;
}

ObHashJoin::ObHashJoin(ObIAllocator& alloc) : ObJoin(alloc)
{
  /*
                         read_left_row -> build_hash_table

                                        state
  --------------->--------------- --JS_READ_RIGHT
  |                                       |
  |                       ----------------|---------------------
  |                       |                                    |
  |                     succ                       -------iter_end------------
  |                      |                         |           |             |
  |          func [FT_ITER_GOING]                anti   need_lef_join       else
  |                     |                         |            |             |
  |        cal_right_hash_value      |-->JS_LEFT_ANTI_SEMI JS_FILL_LEFT  JS_JOIN_END
  |                     |            |     |        |         |     |
  |    -->--------READ_HASH_ROW      |   succ   iter_end     succ iter_end
  |    |            |       |        |     |        |         |     |
  |    |           succ   iter_end   ---<-going JS_JOIN_END   |     |
  |    |            |       |                               going JS_JOIN_END
  |     -----<----going  JS_ERAD_RIGHT                        |
  |                        |                        it's too hard to draw cycle
  |                        |                                  |
  ----------------<---------                              JS_FILL_LEFT
  */

  state_operation_func_[JS_JOIN_END] = &ObHashJoin::join_end_operate;
  state_function_func_[JS_JOIN_END][FT_ITER_GOING] = NULL;
  state_function_func_[JS_JOIN_END][FT_ITER_END] = &ObHashJoin::join_end_func_end;
  state_operation_func_[JS_READ_RIGHT] = &ObHashJoin::read_right_operate;
  state_function_func_[JS_READ_RIGHT][FT_ITER_GOING] = &ObHashJoin::calc_right_hash_value;
  state_function_func_[JS_READ_RIGHT][FT_ITER_END] = &ObHashJoin::read_right_func_end;
  state_operation_func_[JS_READ_HASH_ROW] = &ObHashJoin::read_hashrow;
  state_function_func_[JS_READ_HASH_ROW][FT_ITER_GOING] = &ObHashJoin::read_hashrow_func_going;
  state_function_func_[JS_READ_HASH_ROW][FT_ITER_END] = &ObHashJoin::read_hashrow_func_end;
  // for anti
  state_operation_func_[JS_LEFT_ANTI_SEMI] = &ObHashJoin::left_anti_semi_operate;
  state_function_func_[JS_LEFT_ANTI_SEMI][FT_ITER_GOING] = &ObHashJoin::left_anti_semi_going;
  state_function_func_[JS_LEFT_ANTI_SEMI][FT_ITER_END] = &ObHashJoin::left_anti_semi_end;
  // for unmatched left row, for left-outer,full-outer,left-semi
  state_operation_func_[JS_FILL_LEFT] = &ObHashJoin::fill_left_operate;
  state_function_func_[JS_FILL_LEFT][FT_ITER_GOING] = &ObHashJoin::fill_left_going;
  state_function_func_[JS_FILL_LEFT][FT_ITER_END] = &ObHashJoin::fill_left_end;

  mem_limit_ = DEFAULT_MEM_LIMIT;
}

ObHashJoin::~ObHashJoin()
{}

void ObHashJoin::reset()
{
  ObJoin::reset();
  mem_limit_ = DEFAULT_MEM_LIMIT;
}

void ObHashJoin::reuse()
{
  ObJoin::reuse();
  mem_limit_ = DEFAULT_MEM_LIMIT;
}

int ObHashJoin::init_join_partition(ObPartHashJoinCtx* join_ctx) const
{
  int ret = OB_SUCCESS;
  if (0 >= join_ctx->part_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition count is less then 0", K(join_ctx->part_count_));
  } else {
    int64_t used = sizeof(join::ObHJPartition) * join_ctx->part_count_;
    void* buf = join_ctx->alloc_->alloc(used);
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf for hj part");
    } else {
      join_ctx->hj_part_array_ = new (buf) join::ObHJPartition[join_ctx->part_count_];
      for (int64_t i = 0; i < join_ctx->part_count_ && OB_SUCC(ret); i++) {
        if (OB_FAIL(join_ctx->hj_part_array_[i].init(join_ctx->part_level_,
                static_cast<int32_t>(i),
                true,
                join_ctx->buf_mgr_,
                join_ctx->batch_mgr_,
                join_ctx->left_op_,
                left_op_,
                &join_ctx->sql_mem_processor_,
                join_ctx->sql_mem_processor_.get_dir_id()))) {
          LOG_WARN("failed to init partition", K(join_ctx->part_level_));
        }
      }
    }

    if (OB_SUCCESS == ret) {
      int64_t used = sizeof(join::ObHJPartition) * join_ctx->part_count_;
      void* buf = join_ctx->alloc_->alloc(used);
      if (NULL == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc buf for hj part");
      } else {
        join_ctx->right_hj_part_array_ = new (buf) join::ObHJPartition[join_ctx->part_count_];
        for (int64_t i = 0; i < join_ctx->part_count_ && OB_SUCC(ret); i++) {
          if (OB_FAIL(join_ctx->right_hj_part_array_[i].init(join_ctx->part_level_,
                  static_cast<int32_t>(i),
                  false,
                  join_ctx->buf_mgr_,
                  join_ctx->batch_mgr_,
                  join_ctx->right_op_,
                  right_op_,
                  &join_ctx->sql_mem_processor_,
                  join_ctx->sql_mem_processor_.get_dir_id()))) {
            LOG_WARN("failed to init partition");
          }
        }
      }
    }
  }
  if (nullptr != join_ctx->left_op_) {
    LOG_TRACE("trace init partition",
        K(join_ctx->part_count_),
        K(join_ctx->part_level_),
        K(join_ctx->left_op_->get_part_level()),
        K(join_ctx->left_op_->get_batchno()),
        K(join_ctx->buf_mgr_->get_page_size()));
  } else {
    LOG_TRACE("trace init partition",
        K(join_ctx->part_count_),
        K(join_ctx->part_level_),
        K(join_ctx->buf_mgr_->get_page_size()));
  }
  return ret;
}

int ObHashJoin::init_join_ctx(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObPartHashJoinCtx* join_ctx = NULL;
  if (OB_ISNULL(join_ctx = GET_PHY_OPERATOR_CTX(ObPartHashJoinCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get hash join ctx", K(ret));
  } else {
    ObSQLSessionInfo* session_info = exec_ctx.get_my_session();
    if (session_info == NULL) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session_info is null", K(ret));
    } else {
      if (OB_FAIL(init_join_partition(join_ctx))) {
        LOG_WARN("failed to init join partition", K(ret));
      }
    }
  }
  return ret;
}

int ObHashJoin::inner_open(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObPartHashJoinCtx* join_ctx = NULL;
  ObSQLSessionInfo* session = NULL;
  if ((OB_UNLIKELY(equal_join_conds_.get_size()) <= 0 || OB_ISNULL(left_op_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no equal join conds or left op is null", K(ret));
  } else if (OB_FAIL(ObJoin::inner_open(exec_ctx))) {
    LOG_WARN("failed to open in base class", K(ret));
  } else if (OB_ISNULL(join_ctx = GET_PHY_OPERATOR_CTX(ObPartHashJoinCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get hash join ctx", K(ret));
  } else if (OB_ISNULL(session = exec_ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(join_ctx->init_mem_context(session->get_effective_tenant_id()))) {
    LOG_WARN("fail to init base join ctx", K(ret));
  } else if (OB_FAIL(join_ctx->hash_table_.init(*join_ctx->alloc_))) {
    LOG_WARN("fail to init hash table", K(ret));
  } else {
    join_ctx->set_tenant_id(session->get_effective_tenant_id());
    join_ctx->cur_left_row_.projector_ = const_cast<int32_t*>(left_op_->get_projector());
    join_ctx->cur_left_row_.projector_size_ = left_op_->get_projector_size();
    join_ctx->first_get_row_ = true;
    ObTenantConfigGuard tenant_config(TENANT_CONF(session->get_effective_tenant_id()));
    if (tenant_config.is_valid()) {
      join_ctx->force_hash_join_spill_ = tenant_config->_force_hash_join_spill;
      join_ctx->hash_join_hasher_ = tenant_config->_enable_hash_join_hasher;
      join_ctx->hash_join_processor_ = tenant_config->_enable_hash_join_processor;
      if (0 == (join_ctx->hash_join_processor_ & HJ_PROCESSOR_MASK)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect hash join processor", K(ret), K(join_ctx->hash_join_processor_));
      } else if (0 == (join_ctx->hash_join_hasher_ & HASH_FUNCTION_MASK)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect hash join function", K(ret), K(join_ctx->hash_join_hasher_));
      }
    } else {
      if (0 != HJ_PROCESSOR_ALGO) {
        // only for test
        join_ctx->force_hash_join_spill_ = false;
        join_ctx->hash_join_hasher_ = DEFAULT_MURMUR_HASH;
        join_ctx->hash_join_processor_ = HJ_PROCESSOR_ALGO;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tenant config", K(ret));
      }
    }
    join_ctx->hash_join_ = &join_ctx->default_hash_join_;
    if (INNER_JOIN == join_type_) {
      join_ctx->hash_join_ = &join_ctx->inner_hash_join_;
    }
  }

  uint64_t tenant_id = OB_INVALID_ID;
  if (OB_SUCC(ret)) {
    ObSQLSessionInfo* session_info = exec_ctx.get_my_session();
    if (session_info == NULL) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session_info is null", K(ret));
    } else {
      tenant_id = session_info->get_effective_tenant_id();
    }
  }

  if (OB_SUCC(ret)) {
    join_ctx->part_count_ = 0;
    join_ctx->hj_state_ = ObHashJoin::ObPartHashJoinCtx::INIT;

    void* buf = join_ctx->alloc_->alloc(sizeof(ObHJBufMgr));
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem");
    } else {
      // at least one page for each l/r part
      join_ctx->buf_mgr_ = new (buf) ObHJBufMgr();
      join_ctx->buf_mgr_->set_page_size(OB_MALLOC_MIDDLE_BLOCK_SIZE);
    }
  }

  if (OB_SUCC(ret)) {
    void* buf = join_ctx->alloc_->alloc(sizeof(ObHJBatchMgr));
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem");
    } else {
      join_ctx->batch_mgr_ = new (buf) ObHJBatchMgr(*join_ctx->alloc_, join_ctx->buf_mgr_, tenant_id);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(setup_equal_condition_info(*join_ctx))) {
      LOG_WARN("failed to setup equal condition info", K(ret));
    }
  }
  return ret;
}

int ObHashJoin::inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObHashJoin::ObPartHashJoinCtx* join_ctx = NULL;
  if ((OB_ISNULL(join_ctx = GET_PHY_OPERATOR_CTX(ObHashJoin::ObPartHashJoinCtx, ctx, get_id())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get hash join ctx", K(ret));
  }

  bool exit_while = false;
  while (OB_SUCCESS == ret && !exit_while) {
    switch (join_ctx->hj_state_) {
      case ObHashJoin::ObPartHashJoinCtx::INIT: {
        join_ctx->hj_state_ = ObHashJoin::ObPartHashJoinCtx::NORMAL;
        break;
      }
      case ObHashJoin::ObPartHashJoinCtx::NORMAL: {
        ret = next(ctx, row);
        if (OB_ITER_END == ret) {
          join_ctx->hj_state_ = ObHashJoin::ObPartHashJoinCtx::NEXT_BATCH;
          ret = OB_SUCCESS;
        } else if (OB_SUCCESS == ret) {
          exit_while = true;
        } else {
          LOG_WARN("fail to get next row", K(ret));
        }
        break;
      }
      case ObHashJoin::ObPartHashJoinCtx::NEXT_BATCH: {
        join_ctx->batch_mgr_->remove_undumped_batch();
        if (join_ctx->left_op_ != NULL) {
          join_ctx->left_op_->close();
          join_ctx->batch_mgr_->free(join_ctx->left_op_);
          join_ctx->left_op_ = NULL;
        }
        if (join_ctx->right_op_ != NULL) {
          join_ctx->right_op_->close();
          join_ctx->batch_mgr_->free(join_ctx->right_op_);
          join_ctx->right_op_ = NULL;
        }
        ObHJBatchPair batch_pair;
        if (OB_FAIL(part_rescan(ctx, false))) {
          LOG_WARN("fail to reopen hj", K(ret));
        } else if (OB_FAIL(join_ctx->batch_mgr_->next_batch(batch_pair))) {
        } else if (0 != join_ctx->buf_mgr_->get_total_alloc_size()) {
          LOG_WARN("expect memory count is ok", K(ret), K(join_ctx->buf_mgr_->get_total_alloc_size()));
        }
        if (OB_ITER_END == ret) {
          exit_while = true;
        } else if (OB_SUCCESS == ret) {
          join_ctx->left_op_ = batch_pair.left_;
          join_ctx->right_op_ = batch_pair.right_;

          join_ctx->part_level_ = batch_pair.left_->get_part_level() + 1;
          join_ctx->part_shift_ = (join_ctx->part_level_ + MAX_PART_LEVEL) << 3;

          batch_pair.left_->open();
          batch_pair.right_->open();

          if (MAX_PART_LEVEL < join_ctx->part_level_) {
            // avoid loop recursively
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("too deep part level", K(ret), K(join_ctx->part_level_));
          } else {
            join_ctx->hj_state_ = ObHashJoin::ObPartHashJoinCtx::NORMAL;
          }
          LOG_TRACE("trace batch",
              K(batch_pair.left_->get_batchno()),
              K(batch_pair.right_->get_batchno()),
              K(join_ctx->part_level_));
        } else {
          LOG_WARN("fail get next batch", K(ret));
        }
        break;
      }
    }
  }
  return ret;
}

int ObHashJoin::rescan(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(part_rescan(exec_ctx, true))) {
    LOG_WARN("part rescan failed", K(ret));
  } else if (OB_FAIL(ObJoin::rescan(exec_ctx))) {
    LOG_WARN("join rescan failed", K(ret));
  }
  LOG_TRACE("hash join rescan", K(ret));
  return ret;
}

int ObHashJoin::part_rescan(ObExecContext& exec_ctx, bool reset_all) const
{
  int ret = OB_SUCCESS;
  ObPartHashJoinCtx* join_ctx = NULL;
  ObSQLSessionInfo* session = NULL;
  if (OB_ISNULL(join_ctx = GET_PHY_OPERATOR_CTX(ObPartHashJoinCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get hash join ctx", K(ret));
  } else if (OB_ISNULL(left_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left op is null", K(ret));
  } else if (OB_ISNULL(session = exec_ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else {
    if (reset_all) {
      join_ctx->reset();
      join_ctx->part_count_ = 0;
    } else {
      join_ctx->part_rescan();
    }
    if (OB_SUCC(ret)) {
      // reset join ctx
      join_ctx->left_row_ = NULL;
      join_ctx->right_row_ = NULL;
      join_ctx->last_left_row_ = NULL;
      join_ctx->last_right_row_ = NULL;
      join_ctx->left_row_joined_ = false;
      // init join_ctx
      join_ctx->set_tenant_id(session->get_effective_tenant_id());
      join_ctx->cur_left_row_.projector_ = const_cast<int32_t*>(left_op_->get_projector());
      join_ctx->cur_left_row_.projector_size_ = left_op_->get_projector_size();
    }
  }
  return ret;
}

void ObHashJoin::ObPartHashJoinCtx::clean_batch_mgr()
{
  if (nullptr != batch_mgr_) {
    batch_mgr_->reset();
    if (left_op_ != NULL) {
      batch_mgr_->free(left_op_);
      left_op_ = NULL;
    }
    if (right_op_ != NULL) {
      batch_mgr_->free(right_op_);
      right_op_ = NULL;
    }
  }
}

int ObHashJoin::inner_close(ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  ObPartHashJoinCtx* join_ctx = NULL;
  if (OB_ISNULL(join_ctx = GET_PHY_OPERATOR_CTX(ObPartHashJoinCtx, exec_ctx, get_id()))) {
    LOG_DEBUG("The operator has not been opened.", K(ret), K_(id), "op_type", ob_phy_operator_type_str(get_type()));
  } else {
    join_ctx->sql_mem_processor_.unregister_profile();
    join_ctx->reset();
    clean_equal_condition_info(*join_ctx, exec_ctx);
    if (join_ctx->batch_mgr_ != NULL) {
      join_ctx->batch_mgr_->~ObHJBatchMgr();
      join_ctx->alloc_->free(join_ctx->batch_mgr_);
      join_ctx->batch_mgr_ = NULL;
    }
    if (join_ctx->buf_mgr_ != NULL) {
      join_ctx->buf_mgr_->~ObHJBufMgr();
      join_ctx->alloc_->free(join_ctx->buf_mgr_);
      join_ctx->buf_mgr_ = NULL;
    }
    if (nullptr != join_ctx->alloc_) {
      join_ctx->hash_table_.free(join_ctx->alloc_);
      join_ctx->alloc_->reset();
    }
    if (nullptr != join_ctx->mem_context_) {
      join_ctx->mem_context_->reuse();
    }
  }
  return ret;
}

int ObHashJoin::inner_create_operator_ctx(ObExecContext& exec_ctx, ObPhyOperatorCtx*& op_ctx) const
{
  int ret = OB_SUCCESS;
  ObHashJoin::ObPartHashJoinCtx* join_ctx = NULL;
  if (OB_FAIL(CREATE_PHY_OPERATOR_CTX(ObHashJoin::ObPartHashJoinCtx, exec_ctx, get_id(), get_type(), join_ctx))) {
    LOG_WARN("failed to create nested loop join ctx", K(ret));
  } else {
    op_ctx = join_ctx;
  }
  return ret;
}

int ObHashJoin::next(ObExecContext& exec_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObPartHashJoinCtx* join_ctx = NULL;
  if ((OB_ISNULL(join_ctx = GET_PHY_OPERATOR_CTX(ObPartHashJoinCtx, exec_ctx, get_id())))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get hash join ctx", K(ret));
  }

  if (OB_SUCC(ret)) {
    state_operation_func_type state_operation = NULL;
    state_function_func_type state_function = NULL;
    ObJoinState& state = join_ctx->state_;
    row = NULL;
    int func = -1;
    while (OB_SUCC(ret) && NULL == row) {
      state_operation = this->ObHashJoin::state_operation_func_[state];
      if (OB_ISNULL(state_operation)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("state_operation is null", K(ret), K(state));
      } else if (OB_ITER_END == (ret = (this->*state_operation)(*join_ctx))) {
        func = FT_ITER_END;
        ret = OB_SUCCESS;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed state operation", K(ret), K(state));
      } else {
        func = FT_ITER_GOING;
      }
      if (OB_SUCC(ret)) {
        state_function = this->ObHashJoin::state_function_func_[state][func];
        if (OB_FAIL((this->*state_function)(*join_ctx, row))) {
          if (OB_ITER_END == ret) {
            // iter_end ,break;
          } else {
            LOG_WARN("failed state function", K(ret), K(state), K(func));
          }
        }
      }
    }  // while end
  }

  if (OB_SUCC(ret) && (LEFT_SEMI_JOIN == join_type_ || LEFT_ANTI_JOIN == join_type_ || RIGHT_SEMI_JOIN == join_type_ ||
                          RIGHT_ANTI_JOIN == join_type_)) {
    if (OB_FAIL(copy_cur_row(*join_ctx, row))) {
      LOG_WARN("copy current row failed", K(ret));
    }

    LOG_DEBUG("hash join output row", KPC(row), K(join_type_));
  }

  return ret;
}

int ObHashJoin::force_dump(ObPartHashJoinCtx& join_ctx, bool for_left) const
{
  return finish_dump(join_ctx, for_left, true, true);
}

int ObHashJoin::finish_dump(ObPartHashJoinCtx& join_ctx, bool for_left, bool need_dump, bool force /* false */) const
{
  int ret = OB_SUCCESS;
  if (ObHashJoin::ObPartHashJoinCtx::RECURSIVE == join_ctx.hj_processor_) {
    ObHJPartition* part_array = NULL;
    if (for_left) {
      part_array = join_ctx.hj_part_array_;
    } else {
      part_array = join_ctx.right_hj_part_array_;
    }
    int64_t total_size = 0;
    int64_t dumped_row_count = 0;
    for (int64_t i = 0; i < join_ctx.part_count_ && OB_SUCC(ret); i++) {
      total_size += part_array[i].get_size_in_memory() + part_array[i].get_size_on_disk();
    }
    for (int64_t i = 0; i < join_ctx.part_count_ && OB_SUCC(ret); i++) {
      if (force) {
        // need to dump first
        if (OB_FAIL(part_array[i].dump(true))) {
          LOG_WARN("failed to dump", K(ret));
        }
      }
      if (part_array[i].is_dumped()) {
        if (OB_SUCC(ret)) {
          // either all dumped or all in memory
          if (OB_FAIL(part_array[i].finish_dump(true))) {
            LOG_WARN("finish dump failed", K(i), K(for_left));
          } else if (OB_FAIL(part_array[i].record_pre_batch_info(
                         join_ctx.part_count_, join_ctx.hash_table_.nbuckets_, total_size))) {
            LOG_WARN("failed to record pre-batch info",
                K(ret),
                K(join_ctx.part_count_),
                K(join_ctx.hash_table_.nbuckets_),
                K(total_size));
          }
        }
        dumped_row_count += part_array[i].get_row_count_on_disk();
      }
    }
    if (force && for_left) {
      // mark dump all left partitions
      join_ctx.cur_dumped_partition_ = -1;
    }
    LOG_TRACE("finish dump: ",
        K(join_ctx.part_level_),
        K(join_ctx.buf_mgr_->get_reserve_memory_size()),
        K(join_ctx.buf_mgr_->get_total_alloc_size()),
        K(join_ctx.buf_mgr_->get_dumped_size()),
        K(join_ctx.cur_dumped_partition_),
        K(for_left),
        K(need_dump),
        K(force),
        K(total_size),
        K(dumped_row_count),
        K(join_ctx.part_count_));
  }
  return ret;
}

// dump partition that has only little data
int ObHashJoin::dump_remain_partition(ObPartHashJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  // dump last batch rows and only remain all in-memory data
  if (MAX_PART_COUNT > join_ctx.cur_dumped_partition_) {
    int64_t dumped_size = 0;
    int64_t in_memory_size = 0;
    int64_t total_dumped_size = 0;
    for (int64_t i = join_ctx.part_count_ - 1; i >= 0 && OB_SUCC(ret); --i) {
      ObHJPartition& left_part = join_ctx.hj_part_array_[i];
      if (i > join_ctx.cur_dumped_partition_) {
        dumped_size += left_part.get_size_in_memory();
        if (0 < left_part.get_size_in_memory() && OB_FAIL(left_part.dump(true))) {
          LOG_WARN("finish dump failed", K(i), K(ret));
        } else if (0 != left_part.get_size_in_memory()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expected memory size is 0", K(ret), K(i), K(left_part.get_size_in_memory()));
        } else if (0 == left_part.get_size_on_disk()) {
          if (left_part.is_dumped()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("disk size is 0", K(ret), K(i), K(left_part.get_size_on_disk()));
          }
        }
      } else if (0 <= i) {
        in_memory_size += left_part.get_size_in_memory();
      }
      total_dumped_size += left_part.get_size_on_disk();
    }
    LOG_TRACE("trace total dump info in last stage",
        K(dumped_size),
        K(in_memory_size),
        K(total_dumped_size),
        K(join_ctx.cur_dumped_partition_),
        K(join_ctx.remain_data_memory_size_),
        K(join_ctx.part_level_),
        K(join_ctx.buf_mgr_->get_dumped_size()));
  }
  return ret;
}

int ObHashJoin::update_remain_data_memory_size_periodically(
    ObPartHashJoinCtx& join_ctx, int64_t row_count, bool& need_dump) const
{
  int ret = OB_SUCCESS;
  bool updated = false;
  need_dump = false;
  if (OB_FAIL(join_ctx.sql_mem_processor_.update_max_available_mem_size_periodically(
          join_ctx.alloc_, [&](int64_t cur_cnt) { return row_count > cur_cnt; }, updated))) {
    LOG_WARN("failed to update max usable memory size periodically", K(ret), K(row_count));
  } else if (updated) {
    update_remain_data_memory_size(join_ctx, row_count, join_ctx.sql_mem_processor_.get_mem_bound(), need_dump);
    join_ctx.predict_row_cnt_ <<= 1;
    LOG_TRACE("trace need more remain memory size",
        K(join_ctx.profile_.get_expect_size()),
        K(row_count),
        K(join_ctx.buf_mgr_->get_total_alloc_size()),
        K(join_ctx.predict_row_cnt_));
  }
  return ret;
}

void ObHashJoin::update_remain_data_memory_size(
    ObPartHashJoinCtx& join_ctx, int64_t row_count, int64_t total_mem_size, bool& need_dump) const
{
  need_dump = false;
  double ratio = 1.0;
  need_dump = need_more_remain_data_memory_size(join_ctx, row_count, total_mem_size, ratio);
  int64_t estimate_remain_size = total_mem_size * ratio;
  join_ctx.remain_data_memory_size_ = estimate_remain_size;
  join_ctx.buf_mgr_->set_reserve_memory_size(join_ctx.remain_data_memory_size_);
  LOG_TRACE("trace need more remain memory size",
      K(total_mem_size),
      K(row_count),
      K(join_ctx.buf_mgr_->get_total_alloc_size()),
      K(estimate_remain_size),
      K(join_ctx.predict_row_cnt_));
}

bool ObHashJoin::need_more_remain_data_memory_size(
    ObPartHashJoinCtx& join_ctx, int64_t row_count, int64_t total_mem_size, double& data_ratio) const
{
  int64_t bucket_cnt = calc_bucket_number(row_count);
  int64_t extra_memory_size = bucket_cnt * (sizeof(HashTableCell*) + sizeof(uint8_t));
  extra_memory_size += (row_count * sizeof(HashTableCell));
  int64_t predict_total_memory_size = extra_memory_size + join_ctx.get_data_mem_used();
  bool need_more = (total_mem_size < predict_total_memory_size);
  double guess_data_ratio = 0;
  if (0 < join_ctx.get_mem_used()) {
    guess_data_ratio = 1.0 * join_ctx.get_data_mem_used() / predict_total_memory_size;
  }
  data_ratio = MAX(guess_data_ratio, 0.5);
  LOG_TRACE("trace need more remain memory size",
      K(total_mem_size),
      K(predict_total_memory_size),
      K(extra_memory_size),
      K(bucket_cnt),
      K(row_count),
      K(join_ctx.buf_mgr_->get_total_alloc_size()),
      K(join_ctx.predict_row_cnt_),
      K(data_ratio),
      K(join_ctx.get_mem_used()),
      K(guess_data_ratio),
      K(join_ctx.get_data_mem_used()));
  return need_more;
}

int ObHashJoin::dump_build_table(ObPartHashJoinCtx& join_ctx, int64_t row_count) const
{
  int ret = OB_SUCCESS;
  bool need_dump = false;
  if (OB_FAIL(update_remain_data_memory_size_periodically(join_ctx, row_count, need_dump))) {
    LOG_WARN("failed to update remain memory size periodically", K(ret));
  } else if (OB_LIKELY(join_ctx.need_dump() || need_dump)) {
    // judge whether reach max memory bound size
    // every time expend 20%
    if (MAX_PART_COUNT != join_ctx.cur_dumped_partition_) {
      // it has dumped already
      need_dump = true;
    } else if (OB_FAIL(join_ctx.sql_mem_processor_.extend_max_memory_size(
                   join_ctx.alloc_,
                   [&](int64_t max_memory_size) {
                     UNUSED(max_memory_size);
                     update_remain_data_memory_size(
                         join_ctx, row_count, join_ctx.sql_mem_processor_.get_mem_bound(), need_dump);
                     return need_dump;
                   },
                   need_dump,
                   join_ctx.get_mem_used()))) {
      LOG_WARN("failed to extend max memory size", K(ret));
    }
    if (need_dump) {
      LOG_TRACE("need dump",
          K(join_ctx.buf_mgr_->get_reserve_memory_size()),
          K(join_ctx.buf_mgr_->get_total_alloc_size()),
          K(join_ctx.profile_.get_expect_size()));
      // dump from last partition to the first partition
      int64_t cur_dumped_partition = join_ctx.part_count_ - 1;
      while ((join_ctx.need_dump() || join_ctx.all_dumped()) && 0 <= cur_dumped_partition && OB_SUCC(ret)) {
        ObHJPartition& left_part = join_ctx.hj_part_array_[cur_dumped_partition];
        if (0 < left_part.get_size_in_memory()) {
          if (OB_FAIL(left_part.dump(false))) {
            LOG_WARN("failed to dump partition", K(join_ctx.part_level_), K(cur_dumped_partition));
          } else if (left_part.is_dumped()) {
            left_part.get_batch()->set_memory_limit(1);
            join_ctx.sql_mem_processor_.set_number_pass(join_ctx.part_level_ + 1);
          }
        }
        --cur_dumped_partition;
        if (cur_dumped_partition < join_ctx.cur_dumped_partition_) {
          join_ctx.cur_dumped_partition_ = cur_dumped_partition;
        }
      }
      LOG_TRACE("trace left need dump",
          K(join_ctx.part_level_),
          K(join_ctx.buf_mgr_->get_reserve_memory_size()),
          K(join_ctx.buf_mgr_->get_total_alloc_size()),
          K(cur_dumped_partition),
          K(join_ctx.buf_mgr_->get_dumped_size()),
          K(join_ctx.cur_dumped_partition_));
    }
  }
  return ret;
}

// right dump depend on the dumped partition, if left partition don't dump, then right partition will not do
int ObHashJoin::dump_probe_table(ObPartHashJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(join_ctx.need_dump())) {
    LOG_TRACE(
        "need dump", K(join_ctx.buf_mgr_->get_reserve_memory_size()), K(join_ctx.buf_mgr_->get_total_alloc_size()));
    // dump from last partition to the first partition
    int64_t cur_dumped_partition = join_ctx.part_count_ - 1;
    while ((join_ctx.need_dump() || join_ctx.all_dumped()) && 0 <= cur_dumped_partition && OB_SUCC(ret)) {
      ObHJPartition& right_part = join_ctx.right_hj_part_array_[cur_dumped_partition];
      if (join_ctx.hj_part_array_[cur_dumped_partition].is_dumped()) {
        if (0 < right_part.get_size_in_memory()) {
          if (OB_FAIL(right_part.dump(false))) {
            LOG_WARN("failed to dump partition", K(join_ctx.part_level_), K(cur_dumped_partition));
          } else if (right_part.is_dumped()) {
            right_part.get_batch()->set_memory_limit(1);
          }
        }
      }
      --cur_dumped_partition;
    }
    LOG_TRACE("trace right need dump",
        K(join_ctx.part_level_),
        K(join_ctx.buf_mgr_->get_reserve_memory_size()),
        K(join_ctx.buf_mgr_->get_total_alloc_size()),
        K(cur_dumped_partition),
        K(join_ctx.buf_mgr_->get_dumped_size()),
        K(join_ctx.cur_dumped_partition_));
  }
  return ret;
}

// estimate partition count by input size
// part_cnt = sqrt(input_size * ObChunkRowStore::BLOCK_SIZE)
// one_pass_size = part_cnt * ObChunkRowStore::BLOCK_SIZE
int64_t ObHashJoin::auto_calc_partition_count(int64_t input_size, int64_t min_need_size, int64_t max_part_count) const
{
  int64_t partition_cnt = 8;
  if (input_size > min_need_size) {
    // one pass
    int64_t need_part_cnt = min_need_size / ObChunkRowStore::BLOCK_SIZE;
    while (partition_cnt < need_part_cnt) {
      partition_cnt <<= 1;
    }
  } else {
    // all in memory, use the least memory
    int64_t max_chunk_size = input_size / ObChunkRowStore::BLOCK_SIZE;
    int64_t square_partition_cnt = partition_cnt * partition_cnt;
    while (square_partition_cnt < max_chunk_size) {
      partition_cnt <<= 1;
      square_partition_cnt = partition_cnt * partition_cnt;
    }
  }
  if (max_part_count < partition_cnt) {
    partition_cnt = max_part_count;
  }
  return partition_cnt;
}

// given partition size and input size to calculate partition count
int64_t ObHashJoin::calc_partition_count(int64_t input_size, int64_t part_size, int64_t max_part_count) const
{
  int64_t estimate_part_count = input_size / part_size + 1;
  int64_t partition_cnt = 8;
  while (partition_cnt < estimate_part_count) {
    partition_cnt <<= 1;
  }
  if (max_part_count < partition_cnt) {
    partition_cnt = max_part_count;
  }
  return partition_cnt;
}

void ObHashJoin::clean_nest_loop_chunk(ObPartHashJoinCtx& join_ctx) const
{
  join_ctx.hash_table_.reset();
  join_ctx.hj_bit_set_.reset();
  join_ctx.right_bit_set_.reset();
  join_ctx.alloc_->reuse();
  join_ctx.reset_nest_loop();
  join_ctx.nest_loop_state_ = HJNestLoopState::START;
}

int ObHashJoin::prepare_hash_table(ObPartHashJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  PartHashJoinTable& hash_table = join_ctx.hash_table_;
  // first time, the bucket number is already calculated
  // calculate the buckets number of hash table
  if (OB_FAIL(calc_basic_info(join_ctx))) {
    LOG_WARN("failed to calc basic info", K(ret));
  } else {
    hash_table.nbuckets_ = join_ctx.profile_.get_bucket_size();
    hash_table.row_count_ = join_ctx.profile_.get_row_count();
  }
  int64_t buckets_mem_size = 0;
  int64_t collision_cnts_mem_size = 0;
  int64_t all_cells_mem_size = 0;
  hash_table.buckets_->reuse();
  hash_table.collision_cnts_->reuse();
  hash_table.all_cells_->reuse();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(join_ctx.hj_bit_set_.reserve(hash_table.nbuckets_))) {
    LOG_WARN("fail to reserve bucket", K(ret));
  } else if (OB_FAIL(hash_table.buckets_->init(hash_table.nbuckets_))) {
    LOG_WARN("alloc bucket array failed", K(ret), K(hash_table.nbuckets_));
  } else if (OB_FAIL(hash_table.collision_cnts_->init(hash_table.nbuckets_))) {
    LOG_WARN("alloc collision array failed", K(ret), K(hash_table.nbuckets_));
  } else if (0 < hash_table.row_count_ && OB_FAIL(hash_table.all_cells_->init(hash_table.row_count_))) {
    LOG_WARN("alloc hash cell failed", K(ret), K(hash_table.row_count_));
  } else {
    if (ObHashJoin::ObPartHashJoinCtx::NEST_LOOP == join_ctx.hj_processor_ && OB_NOT_NULL(join_ctx.right_op_)) {
      join_ctx.has_right_bitset_ = need_right_bitset();
      if (join_ctx.has_right_bitset_ &&
          (TEST_NEST_LOOP_TO_RECURSIVE ||
              OB_FAIL(join_ctx.right_bit_set_.reserve(join_ctx.right_op_->get_row_count_on_disk())))) {
        // revert to recursive process
        // only for TEST_NEST_LOOP_TO_RECURSIVE
        join_ctx.nest_loop_state_ = HJNestLoopState::RECURSIVE;
        if (TEST_NEST_LOOP_TO_RECURSIVE) {
          // for test, force return error code
          ret = OB_ERR_UNEXPECTED;
        }
        LOG_WARN("failed to reserve right bitset",
            K(ret),
            K(hash_table.nbuckets_),
            K(join_ctx.right_op_->get_row_count_on_disk()));
      }
    }
    LOG_TRACE("trace prepare hash table",
        K(ret),
        K(hash_table.nbuckets_),
        K(hash_table.row_count_),
        K(join_ctx.part_count_),
        K(join_ctx.buf_mgr_->get_reserve_memory_size()),
        K(join_ctx.total_extra_size_),
        K(join_ctx.buf_mgr_->get_total_alloc_size()),
        K(join_ctx.profile_.get_expect_size()));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("trace failed to  prepare hash table",
        K(join_ctx.buf_mgr_->get_total_alloc_size()),
        K(join_ctx.profile_.get_expect_size()),
        K(join_ctx.buf_mgr_->get_reserve_memory_size()),
        K(hash_table.nbuckets_),
        K(hash_table.row_count_),
        K(join_ctx.get_mem_used()),
        K(join_ctx.sql_mem_processor_.get_mem_bound()));
  } else {
    if (OB_FAIL(join_ctx.sql_mem_processor_.update_used_mem_size(join_ctx.get_mem_used()))) {
      LOG_WARN("failed to update used mem size", K(ret));
    }
  }
  LOG_TRACE("trace prepare hash table",
      K(ret),
      K(hash_table.nbuckets_),
      K(hash_table.row_count_),
      K(buckets_mem_size),
      K(join_ctx.part_count_),
      K(join_ctx.buf_mgr_->get_reserve_memory_size()),
      K(join_ctx.total_extra_size_),
      K(join_ctx.buf_mgr_->get_total_alloc_size()),
      K(join_ctx.profile_.get_expect_size()),
      K(collision_cnts_mem_size),
      K(all_cells_mem_size));
  return ret;
}

// get every row and split into correspond partition
int ObHashJoin::split_partition(ObPartHashJoinCtx& join_ctx, int64_t& num_left_rows) const
{
  int ret = OB_SUCCESS;
  uint64_t hash_value = 0;
  join::ObStoredJoinRow* stored_row = NULL;
  int64_t row_count_on_disk = 0;
  if (nullptr != join_ctx.left_op_) {
    // read all data, use default iterator
    if (OB_FAIL(join_ctx.left_op_->set_iterator(false))) {
      LOG_WARN("failed to init iterator", K(ret));
    } else {
      row_count_on_disk = join_ctx.left_op_->get_row_count_on_disk();
    }
  }
  num_left_rows = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(get_next_left_row(join_ctx))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next left row failed", K(ret));
      }
    } else if (OB_ISNULL(join_ctx.left_row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left row is  null", K(ret));
    } else {
      if (NULL == join_ctx.left_read_row_) {
        if (OB_FAIL(get_left_hash_value(join_ctx, *join_ctx.left_row_, hash_value))) {
          LOG_WARN("get left row hash_value failed", K(ret), K(*join_ctx.left_row_));
        }
      } else {
        hash_value = join_ctx.left_read_row_->get_hash_value();
      }
    }
    if (OB_SUCC(ret)) {
      ++num_left_rows;
      const int64_t part_idx = join_ctx.get_part_idx(hash_value);
      if (OB_FAIL(join_ctx.hj_part_array_[part_idx].add_row(*join_ctx.left_row_, stored_row))) {
        // if oom, then dump and add row again
        if (OB_ALLOCATE_MEMORY_FAILED == ret) {
          if (GCONF.is_sql_operator_dump_enabled()) {
            ret = OB_SUCCESS;
            if (OB_FAIL(force_dump(join_ctx, true))) {
              LOG_WARN("fail to dump", K(ret));
            } else if (OB_FAIL(join_ctx.hj_part_array_[part_idx].add_row(*join_ctx.left_row_, stored_row))) {
              LOG_WARN("add row to row store failed", K(ret), K(*join_ctx.left_row_));
            }
          } else {
            LOG_WARN("add row to row store failed", K(ret), K(*join_ctx.left_row_));
          }
        } else {
          LOG_WARN("add row to row store failed", K(ret), K(*join_ctx.left_row_));
        }
      }
      if (OB_SUCC(ret)) {
        stored_row->set_is_match(false);
        stored_row->set_hash_value(hash_value);
        if (GCONF.is_sql_operator_dump_enabled()) {
          if (OB_FAIL(dump_build_table(join_ctx, num_left_rows))) {
            LOG_WARN("fail to dump", K(ret));
          }
        }
      }
    }
  }
  // overwrite OB_ITER_END error code
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    if (nullptr != join_ctx.left_op_) {
      join_ctx.left_op_->rescan();
    }
    if (join_ctx.sql_mem_processor_.is_auto_mgr()) {
      // last stage for dump build table
      if (OB_FAIL(calc_basic_info(join_ctx))) {
        LOG_WARN("failed to calc basic info", K(ret));
      } else if (OB_FAIL(dump_build_table(join_ctx, join_ctx.profile_.get_row_count()))) {
        LOG_WARN("fail to dump", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (join_ctx.top_part_level() && join_ctx.force_hash_join_spill_) {
      // force partition dump
      if (OB_FAIL(force_dump(join_ctx, true))) {
        LOG_WARN("fail to finish dump", K(ret));
      } else {
        // for all partition to dump
        join_ctx.cur_dumped_partition_ = -1;
        for (int64_t i = 0; OB_SUCC(ret) && i < join_ctx.part_count_; ++i) {
          int64_t row_count_in_memory = join_ctx.hj_part_array_[i].get_row_count_in_memory();
          if (0 != row_count_in_memory) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect no data in memory", K(ret), K(row_count_in_memory), K(i));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(dump_remain_partition(join_ctx))) {
      LOG_WARN("failed to dump remain partition");
    } else if (OB_FAIL(finish_dump(join_ctx, true, false))) {
      LOG_WARN("fail to finish dump", K(ret));
    } else if (nullptr != join_ctx.left_op_ && num_left_rows != row_count_on_disk) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect read all data", K(ret), K(num_left_rows), K(row_count_on_disk));
    }
  }
  LOG_TRACE("trace split partition", K(ret), K(num_left_rows), K(row_count_on_disk));
  return ret;
}

int ObHashJoin::split_partition_and_build_hash_table(ObPartHashJoinCtx& join_ctx, int64_t& num_left_rows) const
{
  int ret = OB_SUCCESS;
  // load data to partitions
  num_left_rows = 0;
  if (OB_FAIL(split_partition(join_ctx, num_left_rows))) {
    LOG_WARN("failed split partition", K(ret), K(join_ctx.part_level_));
  } else if (OB_FAIL(prepare_hash_table(join_ctx))) {
    LOG_WARN("failed to prepare hash table", K(ret));
  } else if (OB_FAIL(build_hash_table_for_recursive(join_ctx))) {
    LOG_WARN("failed to build hash table", K(ret), K(join_ctx.part_level_));
  }
  return ret;
}

int ObHashJoin::reuse_for_next_chunk(ObPartHashJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  // first time, the bucket number is already calculated
  // calculate the buckets number of hash table
  if (join_ctx.top_part_level() || 0 == join_ctx.hash_table_.nbuckets_ ||
      ObHashJoin::ObPartHashJoinCtx::NEST_LOOP != join_ctx.hj_processor_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected hash buckets number is 0", K(ret), K(join_ctx.part_level_));
  } else if (OB_FAIL(calc_basic_info(join_ctx))) {
    LOG_WARN("failed to calc basic info", K(ret));
  } else {
    join_ctx.hj_bit_set_.reuse();
    // reuse buckets
    PartHashJoinTable& hash_table = join_ctx.hash_table_;
    hash_table.buckets_->set_all(NULL);
    hash_table.collision_cnts_->set_all(0);

    int64_t row_count = join_ctx.profile_.get_row_count();
    if (row_count > hash_table.row_count_) {
      hash_table.row_count_ = row_count;
      hash_table.all_cells_->reuse();
      if (OB_FAIL(hash_table.all_cells_->init(hash_table.row_count_))) {
        LOG_WARN("failed to init hash_table.all_cells_", K(ret), K(hash_table.row_count_));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(join_ctx.right_op_->rescan())) {
      LOG_WARN("failed to rescan right", K(ret));
    } else {
      join_ctx.nth_right_row_ = -1;
    }
    LOG_TRACE("trace hash table",
        K(ret),
        K(hash_table.nbuckets_),
        K(row_count),
        K(join_ctx.nth_right_row_),
        K(join_ctx.profile_.get_row_count()));
  }
  return ret;
}

int ObHashJoin::load_next_chunk(ObPartHashJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  ++join_ctx.nth_nest_loop_;
  if (1 == join_ctx.nth_nest_loop_ && OB_FAIL(join_ctx.left_op_->set_iterator(true))) {
    LOG_WARN("failed to set iterator", K(ret), K(join_ctx.nth_nest_loop_));
  } else if (OB_FAIL(join_ctx.left_op_->load_next_chunk())) {
    LOG_WARN("failed to load next chunk", K(ret), K(join_ctx.nth_nest_loop_));
  } else if (1 == join_ctx.nth_nest_loop_ && OB_FAIL(prepare_hash_table(join_ctx))) {
    LOG_WARN("failed to prepare hash table", K(ret), K(join_ctx.nth_nest_loop_));
  } else if (1 < join_ctx.nth_nest_loop_ && OB_FAIL(reuse_for_next_chunk(join_ctx))) {
    LOG_WARN("failed to reset info for block", K(ret), K(join_ctx.nth_nest_loop_));
  }
  return ret;
}

int ObHashJoin::build_hash_table_for_nest_loop(ObPartHashJoinCtx& join_ctx, int64_t& num_left_rows) const
{
  int ret = OB_SUCCESS;
  uint64_t hash_value = 0;
  num_left_rows = 0;
  join::ObStoredJoinRow* stored_row = NULL;
  HashTableCell* tuple = NULL;
  join_ctx.nest_loop_state_ = HJNestLoopState::GOING;
  join_ctx.is_last_chunk_ = false;
  if (OB_FAIL(load_next_chunk(join_ctx))) {
    LOG_WARN("failed to reset info for block", K(ret));
  } else {
    PartHashJoinTable& hash_table = join_ctx.hash_table_;
    int64_t cell_index = 0;
    int64_t bucket_id = 0;
    ObHJBatch* hj_batch = join_ctx.left_op_;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(get_next_left_row(join_ctx))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next left row failed", K(ret));
        } else if (!hj_batch->has_next()) {
          // last chunk
          join_ctx.is_last_chunk_ = true;
          join_ctx.nest_loop_state_ = HJNestLoopState::END;
          if (join_ctx.cur_nth_row_ != hj_batch->get_row_count_on_disk()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN(
                "expect row count is match", K(ret), K(join_ctx.cur_nth_row_), K(hj_batch->get_row_count_on_disk()));
          }
        }
      } else if (OB_ISNULL(join_ctx.left_row_) || OB_ISNULL(join_ctx.left_read_row_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("left row is  null", K(ret));
      } else if (num_left_rows >= hash_table.row_count_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected row count", K(ret), K(num_left_rows), K(hash_table.row_count_));
      } else {
        stored_row = const_cast<join::ObStoredJoinRow*>(join_ctx.left_read_row_);
        hash_value = stored_row->get_hash_value();
        bucket_id = join_ctx.get_bucket_idx(hash_value);
        join_ctx.hj_bit_set_.add_member(bucket_id);

        tuple = &(hash_table.all_cells_->at(cell_index));
        tuple->stored_row_ = stored_row;
        tuple->next_tuple_ = hash_table.buckets_->at(bucket_id);
        hash_table.buckets_->at(bucket_id) = tuple;
        hash_table.inc_collision(bucket_id);
        ++cell_index;
        ++num_left_rows;
        ++join_ctx.cur_nth_row_;
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    trace_hash_table_collision(join_ctx, num_left_rows);
  }
  LOG_TRACE("trace block hash join",
      K(join_ctx.nth_nest_loop_),
      K(join_ctx.part_level_),
      K(ret),
      K(num_left_rows),
      K(join_ctx.hash_table_.nbuckets_));
  return ret;
}

int ObHashJoin::build_hash_table_in_memory(ObPartHashJoinCtx& join_ctx, int64_t& num_left_rows) const
{
  int ret = OB_SUCCESS;
  uint64_t hash_value = 0;
  num_left_rows = 0;
  join::ObStoredJoinRow* stored_row = NULL;
  ObHJBatch* hj_batch = join_ctx.left_op_;
  if (OB_FAIL(join_ctx.left_op_->set_iterator(true))) {
    LOG_WARN("failed to set iterator", K(ret));
  } else if (OB_FAIL(hj_batch->load_next_chunk())) {
    LOG_WARN("failed to load next chunk", K(ret));
  } else if (hj_batch->has_next()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected batch",
        K(ret),
        K(hj_batch->get_size_on_disk()),
        K(hj_batch->get_row_count_in_memory()),
        K(hj_batch->get_row_count_on_disk()),
        K(join_ctx.buf_mgr_->get_reserve_memory_size()));
  } else if (OB_FAIL(prepare_hash_table(join_ctx))) {
    LOG_WARN("failed to prepare hash table", K(ret));
  } else {
    HashTableCell* tuple = NULL;
    PartHashJoinTable& hash_table = join_ctx.hash_table_;
    int64_t cell_index = 0;
    int64_t bucket_id = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(get_next_left_row(join_ctx))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next left row failed", K(ret));
        } else if (hj_batch->get_row_count_on_disk() != num_left_rows) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expect row count is match", K(hj_batch->get_row_count_on_disk()), K(num_left_rows));
        }
      } else if (OB_ISNULL(join_ctx.left_row_) || OB_ISNULL(join_ctx.left_read_row_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("left row is  null", K(ret));
      } else if (num_left_rows >= hash_table.row_count_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row count exceed total row count", K(ret), K(num_left_rows), K(hash_table.row_count_));
      } else {
        stored_row = const_cast<join::ObStoredJoinRow*>(join_ctx.left_read_row_);
        hash_value = stored_row->get_hash_value();
        bucket_id = join_ctx.get_bucket_idx(hash_value);
        join_ctx.hj_bit_set_.add_member(bucket_id);
        tuple = &(hash_table.all_cells_->at(cell_index));
        tuple->stored_row_ = stored_row;
        tuple->next_tuple_ = hash_table.buckets_->at(bucket_id);
        hash_table.buckets_->at(bucket_id) = tuple;
        hash_table.inc_collision(bucket_id);
        ++cell_index;
        ++num_left_rows;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      trace_hash_table_collision(join_ctx, num_left_rows);
    }
  }
  join_ctx.is_last_chunk_ = true;
  LOG_TRACE("trace to finish build hash table in memory",
      K(ret),
      K(num_left_rows),
      K(hj_batch->get_row_count_on_disk()),
      K(join_ctx.hash_table_.nbuckets_));
  return ret;
}

void ObHashJoin::trace_hash_table_collision(ObPartHashJoinCtx& join_ctx, int64_t row_cnt) const
{
  int64_t total_cnt = 0;
  uint8_t min_cnt = 0;
  uint8_t max_cnt = 0;
  int64_t used_bucket_cnt = 0;
  int64_t nbuckets = join_ctx.hash_table_.nbuckets_;
  join_ctx.hash_table_.get_collision_info(min_cnt, max_cnt, total_cnt, used_bucket_cnt);
  LOG_TRACE("trace hash table collision",
      K(get_id()),
      K(nbuckets),
      "avg_cnt",
      ((double)total_cnt / (double)used_bucket_cnt),
      K(min_cnt),
      K(max_cnt),
      K(total_cnt),
      K(row_cnt),
      K(used_bucket_cnt));
  join_ctx.op_monitor_info_.otherstat_1_value_ = min_cnt;
  join_ctx.op_monitor_info_.otherstat_2_value_ = max_cnt;
  join_ctx.op_monitor_info_.otherstat_3_value_ = total_cnt;
  join_ctx.op_monitor_info_.otherstat_4_value_ = nbuckets;
  join_ctx.op_monitor_info_.otherstat_5_value_ = used_bucket_cnt;
  join_ctx.op_monitor_info_.otherstat_6_value_ = row_cnt;
  join_ctx.op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::HASH_SLOT_MIN_COUNT;
  ;
  join_ctx.op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::HASH_SLOT_MAX_COUNT;
  join_ctx.op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::HASH_SLOT_TOTAL_COUNT;
  join_ctx.op_monitor_info_.otherstat_4_id_ = ObSqlMonitorStatIds::HASH_BUCKET_COUNT;
  join_ctx.op_monitor_info_.otherstat_5_id_ = ObSqlMonitorStatIds::HASH_NON_EMPTY_BUCKET_COUNT;
  join_ctx.op_monitor_info_.otherstat_6_id_ = ObSqlMonitorStatIds::HASH_ROW_COUNT;
}

int ObHashJoin::build_hash_table_for_recursive(ObPartHashJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  uint64_t hash_value = 0;
  const join::ObStoredJoinRow* stored_row = NULL;
  HashTableCell* tuple = NULL;
  int64_t bucket_id = 0;
  int64_t total_row_count = 0;
  PartHashJoinTable& hash_table = join_ctx.hash_table_;
  int64_t cell_index = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < join_ctx.part_count_; ++i) {
    ObHJPartition& hj_part = join_ctx.hj_part_array_[i];
    int64_t row_count_in_memory = hj_part.get_row_count_in_memory();
    int64_t nth_row = 0;
    if (0 < row_count_in_memory) {
      if (0 < hj_part.get_row_count_on_disk()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect it has row on disk", K(ret));
      } else if (OB_FAIL(hj_part.init_iterator(false))) {
        LOG_WARN("failed to init iterator", K(ret));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(hj_part.get_next_row(stored_row))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next row failed", K(ret));
            }
          } else if (OB_ISNULL(stored_row)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("returned stored row is NULL", K(ret));
          } else if (total_row_count + nth_row >= hash_table.row_count_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN(
                "row count exceed total row count", K(ret), K(nth_row), K(total_row_count), K(hash_table.row_count_));
          } else {
            hash_value = stored_row->get_hash_value();
            bucket_id = join_ctx.get_bucket_idx(hash_value);
            join_ctx.hj_bit_set_.add_member(bucket_id);

            tuple = &(hash_table.all_cells_->at(cell_index));
            tuple->stored_row_ = const_cast<join::ObStoredJoinRow*>(stored_row);
            tuple->next_tuple_ = hash_table.buckets_->at(bucket_id);
            hash_table.buckets_->at(bucket_id) = tuple;
            hash_table.inc_collision(bucket_id);
            ++cell_index;
            ++nth_row;
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      total_row_count += nth_row;
      ret = OB_SUCCESS;
      if (nth_row != row_count_in_memory) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect row count is match", K(nth_row), K(row_count_in_memory), K(hj_part.get_row_count_on_disk()));
      }
    }
  }
  join_ctx.is_last_chunk_ = true;
  if (OB_SUCC(ret)) {
    trace_hash_table_collision(join_ctx, total_row_count);
  }
  LOG_TRACE("trace to finish build hash table for recursive",
      K(join_ctx.part_count_),
      K(join_ctx.part_level_),
      K(total_row_count),
      K(join_ctx.hash_table_.nbuckets_));
  return ret;
}

int ObHashJoin::get_cast_hash_value(
    const ObObj& obj, const EqualConditionInfo& info, ObPartHashJoinCtx& join_ctx, uint64_t& hash_value) const
{
  int ret = OB_SUCCESS;
  ObObj buf_obj;
  const ObObj* res_obj = NULL;
  EXPR_DEFINE_CAST_CTX(join_ctx.expr_ctx_, CM_NONE);
  cast_ctx.dest_collation_ = info.ctype_;
  if (OB_FAIL(ObObjCaster::to_type(info.cmp_type_, cast_ctx, obj, buf_obj, res_obj))) {
    LOG_WARN("failed to cast obj", K(ret), K(info.cmp_type_), K(obj));
  } else if (OB_ISNULL(res_obj)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("res_obj is null", K(ret));
  } else {
    if (OB_FAIL(info.obj_hash_->hash(*res_obj, hash_value))) {
      LOG_WARN("failed to calc hash value", K(ret));
    }
  }
  return ret;
}

int ObHashJoin::get_left_row(ObJoinCtx& join_ctx) const
{
  int ret = common::OB_SUCCESS;
  if (NULL != join_ctx.last_left_row_) {
    join_ctx.left_row_ = join_ctx.last_left_row_;
    join_ctx.last_left_row_ = NULL;
  } else {
    ObPartHashJoinCtx& part_join_ctx = reinterpret_cast<ObPartHashJoinCtx&>(join_ctx);
    part_join_ctx.left_row_joined_ = false;
    if (part_join_ctx.left_op_ == NULL) {
      if (OB_FAIL(OB_I(t1) left_op_->get_next_row(part_join_ctx.exec_ctx_, part_join_ctx.left_row_))) {
        part_join_ctx.left_row_ = NULL;
      } else {
      }
    } else {
      if (OB_FAIL(try_check_status(join_ctx.exec_ctx_))) {
        LOG_WARN("failed to check status", K(ret));
      } else if (OB_FAIL(OB_I(t1) part_join_ctx.left_op_->get_next_row(
                     part_join_ctx.left_row_, part_join_ctx.left_read_row_))) {
        part_join_ctx.left_row_ = NULL;
        part_join_ctx.left_read_row_ = NULL;
      } else {
      }
    }
  }
  return ret;
}

int ObHashJoin::get_right_row(ObJoinCtx& join_ctx) const
{
  int ret = common::OB_SUCCESS;
  if (NULL != join_ctx.last_right_row_) {
    join_ctx.right_row_ = join_ctx.last_right_row_;
    join_ctx.last_right_row_ = NULL;
  } else {
    ObPartHashJoinCtx& part_join_ctx = reinterpret_cast<ObPartHashJoinCtx&>(join_ctx);
    if (part_join_ctx.right_op_ == NULL) {
      if (OB_FAIL(OB_I(t1) right_op_->get_next_row(part_join_ctx.exec_ctx_, part_join_ctx.right_row_))) {
        part_join_ctx.right_row_ = NULL;
      } else {
      }
    } else {
      if (OB_FAIL(try_check_status(join_ctx.exec_ctx_))) {
        LOG_WARN("failed to check status", K(ret));
      } else if (OB_FAIL(OB_I(t1) part_join_ctx.right_op_->get_next_row(
                     part_join_ctx.right_row_, part_join_ctx.right_read_row_))) {
        part_join_ctx.right_row_ = NULL;
        part_join_ctx.right_read_row_ = NULL;
      } else {
        ++part_join_ctx.nth_right_row_;
      }
    }
  }
  return ret;
}

int ObHashJoin::get_next_left_row(ObJoinCtx& join_ctx) const
{
  int ret = common::OB_SUCCESS;
  ObPartHashJoinCtx& part_join_ctx = reinterpret_cast<ObPartHashJoinCtx&>(join_ctx);
  part_join_ctx.left_row_joined_ = false;
  if (part_join_ctx.left_op_ == NULL) {
    if (OB_FAIL(OB_I(t1) left_op_->get_next_row(part_join_ctx.exec_ctx_, part_join_ctx.left_row_))) {
      part_join_ctx.left_row_ = NULL;
      if (OB_ITER_END != ret) {
        LOG_WARN("get left row from child failed", K(ret));
      }
    } else {
    }
    LOG_DEBUG("left op get left row", K(part_join_ctx.left_row_), K(ret));
  } else {
    if (OB_FAIL(try_check_status(join_ctx.exec_ctx_))) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(OB_I(t1) part_join_ctx.left_op_->get_next_row(
                   part_join_ctx.left_row_, part_join_ctx.left_read_row_))) {
      part_join_ctx.left_row_ = NULL;
      part_join_ctx.left_read_row_ = NULL;
      if (OB_ITER_END != ret) {
        LOG_WARN("get left row from partition failed", K(ret));
      }
    } else {
    }
    LOG_DEBUG("part join ctx get left row", K(part_join_ctx.left_row_), K(ret));
  }
  return ret;
}

int ObHashJoin::get_next_right_row(ObJoinCtx& join_ctx) const
{
  int ret = common::OB_SUCCESS;
  ObPartHashJoinCtx& part_join_ctx = reinterpret_cast<ObPartHashJoinCtx&>(join_ctx);
  if (part_join_ctx.right_op_ == NULL) {
    if (OB_FAIL(OB_I(t1) right_op_->get_next_row(part_join_ctx.exec_ctx_, part_join_ctx.right_row_))) {
      part_join_ctx.right_row_ = NULL;
      if (OB_ITER_END != ret) {
        LOG_WARN("get right row from child failed", K(ret));
      }
    } else {
    }
  } else {
    if (OB_FAIL(try_check_status(join_ctx.exec_ctx_))) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(OB_I(t1) part_join_ctx.right_op_->get_next_row(
                   part_join_ctx.right_row_, part_join_ctx.right_read_row_))) {
      part_join_ctx.right_row_ = NULL;
      part_join_ctx.right_read_row_ = NULL;
      if (OB_ITER_END != ret) {
        LOG_WARN("get right row from partition failed", K(ret));
      }
    } else {
      ++part_join_ctx.nth_right_row_;
    }
  }
  return ret;
}

void ObHashJoin::clean_equal_condition_info(ObPartHashJoinCtx& join_ctx, ObExecContext& exec_ctx) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = join_ctx.equal_cond_info_.count() - 1; i >= 0; --i) {
    EqualConditionInfo* info = nullptr;
    if (OB_FAIL(join_ctx.equal_cond_info_.pop_back(info))) {
      LOG_ERROR("failed to pop back equal condition info", K(ret));
    } else {
      info->~EqualConditionInfo();
      exec_ctx.get_allocator().free(info);
    }
  }
}

int ObHashJoin::set_hash_function(EqualConditionInfo* info, int8_t hash_join_hasher) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("equal condition info is null", K(ret));
  } else {
    if (ob_is_string_type(info->cmp_type_)) {
      info->obj_hash_ = &info->string_hash_fun_;
      LOG_TRACE("trace hash join hash function: varchar", K(ret));
    } else if (0 != (hash_join_hasher & ENABLE_CRC64_V3)) {
      info->default_hash_fun_.hash_ptr_ = ObObjUtil::get_crc64_v3(info->cmp_type_);
      info->obj_hash_ = &info->default_hash_fun_;
      LOG_TRACE("trace hash join hash function: crc64", K(ret));
    } else if (0 != (hash_join_hasher & ENABLE_XXHASH64)) {
      info->default_hash_fun_.hash_ptr_ = ObObjUtil::get_xxhash64(info->cmp_type_);
      info->obj_hash_ = &info->default_hash_fun_;
      LOG_TRACE("trace hash join hash function: xxhash", K(ret));
    } else if (0 != (hash_join_hasher & DEFAULT_MURMUR_HASH)) {
      info->default_hash_fun_.hash_ptr_ = ObObjUtil::get_murmurhash_v2(info->cmp_type_);
      info->obj_hash_ = &info->default_hash_fun_;
      LOG_TRACE("trace hash join hash function: murmurhash", K(ret));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no hash function", K(ret), K(hash_join_hasher));
    }
  }
  return ret;
}

int ObHashJoin::setup_equal_condition_info(ObPartHashJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("left_op is null", K(ret));
  }
  DLIST_FOREACH_X(node, equal_join_conds_, OB_SUCC(ret))
  {
    void* buf = join_ctx.exec_ctx_.get_allocator().alloc(sizeof(EqualConditionInfo));
    if (nullptr == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc equal condition info", K(ret));
    } else {
      EqualConditionInfo* info = new (buf) EqualConditionInfo;
      int64_t col1 = OB_INVALID_INDEX;
      int64_t col2 = OB_INVALID_INDEX;
      bool is_null_safe = false;
      if (OB_ISNULL(node)) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("node or node expr is null", K(ret));
      } else {
        if (node->is_equijoin_cond(col1, col2, info->cmp_type_, info->ctype_, is_null_safe)) {
          const int64_t left_col_cnt = left_op_->get_column_count();
          info->col1_idx_ = std::min(col1, col2);
          info->col2_idx_ = std::max(col1, col2) - left_col_cnt;
          if (info->col1_idx_ >= left_col_cnt || info->col2_idx_ < 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid column idx", K(ret), K(col1), K(col2), K(left_col_cnt));
          } else if (OB_FAIL(set_hash_function(info, join_ctx.hash_join_hasher_))) {
            LOG_WARN("failed to set hash function", K(ret));
          } else if (OB_FAIL(join_ctx.equal_cond_info_.push_back(info))) {
            LOG_WARN("push back info failed", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not equal join condition", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObHashJoin::get_left_hash_value(ObPartHashJoinCtx& join_ctx, const ObNewRow& row, uint64_t& hash_value) const
{
  int ret = OB_SUCCESS;
  hash_value = HASH_SEED;
  if (0 == join_ctx.equal_cond_info_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("equal condition info is null", K(ret));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < join_ctx.equal_cond_info_.count(); ++idx) {
      const EqualConditionInfo* info = join_ctx.equal_cond_info_.at(idx);
      const ObObj& cell = row.cells_[info->col1_idx_];
      if (OB_FAIL(calc_hash_value(join_ctx, cell, *info, hash_value))) {
        LOG_WARN("get cast hash value failed", K(ret));
      }
    }
    hash_value = hash_value & ObStoredJoinRow::HASH_VAL_MASK;
  }
  return ret;
}

int ObHashJoin::get_right_hash_value(ObPartHashJoinCtx& join_ctx, const ObNewRow& row, uint64_t& hash_value) const
{
  int ret = OB_SUCCESS;
  hash_value = HASH_SEED;
  if (0 == join_ctx.equal_cond_info_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("equal condition info is null", K(ret));
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < join_ctx.equal_cond_info_.count(); ++idx) {
      const EqualConditionInfo* info = join_ctx.equal_cond_info_.at(idx);
      const ObObj& cell = row.cells_[info->col2_idx_];
      if (OB_FAIL(calc_hash_value(join_ctx, cell, *info, hash_value))) {
        LOG_WARN("get cast hash value failed", K(ret));
      }
    }
    hash_value = hash_value & ObStoredJoinRow::HASH_VAL_MASK;
  }
  return ret;
}

int ObHashJoin::StringHashFunction::hash(const common::ObObj& obj, uint64_t& seed)
{
  int ret = OB_SUCCESS;
  seed = obj.varchar_murmur_hash(obj.get_collation_type(), seed);
  return ret;
}

int ObHashJoin::DefaultHashFunction::hash(const common::ObObj& obj, uint64_t& seed)
{
  int ret = OB_SUCCESS;
  seed = hash_ptr_(obj, seed);
  return ret;
}

int ObHashJoin::calc_hash_value(
    ObPartHashJoinCtx& join_ctx, const ObObj& cell, const EqualConditionInfo& info, uint64_t& hash_value) const
{
  int ret = OB_SUCCESS;
  if (cell.is_null()) {
    hash_value = cell.hash(hash_value);
  } else if (cell.get_type() == info.cmp_type_) {
    // need to do cast if character set is not the same
    if (cell.get_type_class() == ObStringTC && cell.get_collation_type() != info.ctype_) {
      OZ(get_cast_hash_value(cell, info, join_ctx, hash_value));
    } else {
      OZ(info.obj_hash_->hash(cell, hash_value));
    }
  } else {
    OZ(get_cast_hash_value(cell, info, join_ctx, hash_value));
  }
  return ret;
}

int ObHashJoin::find_next_unmatched_tuple(ObPartHashJoinCtx& join_ctx, HashTableCell*& tuple) const
{
  int ret = OB_SUCCESS;
  PartHashJoinTable& htable = join_ctx.hash_table_;
  while (OB_SUCC(ret)) {
    if (NULL != tuple) {
      if (tuple->stored_row_->is_match()) {
        tuple = tuple->next_tuple_;
      } else {
        break;
      }
    } else {
      int64_t bucket_id = join_ctx.cur_bkid_ + 1;
      if (bucket_id < htable.nbuckets_) {
        tuple = htable.buckets_->at(bucket_id);
        join_ctx.cur_bkid_ = bucket_id;
      } else {
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

int ObHashJoin::find_next_matched_tuple(ObPartHashJoinCtx& join_ctx, HashTableCell*& tuple) const
{
  int ret = OB_SUCCESS;
  PartHashJoinTable& htable = join_ctx.hash_table_;
  while (OB_SUCC(ret)) {
    if (NULL != tuple) {
      if (!tuple->stored_row_->is_match()) {
        tuple = tuple->next_tuple_;
      } else {
        break;
      }
    } else {
      int64_t bucket_id = join_ctx.cur_bkid_ + 1;
      if (bucket_id < htable.nbuckets_) {
        tuple = htable.buckets_->at(bucket_id);
        join_ctx.cur_bkid_ = bucket_id;
      } else {
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

// convert tuple to row
int ObHashJoin::convert_tuple(ObPartHashJoinCtx& join_ctx, const HashTableCell& tuple) const
{
  int ret = OB_SUCCESS;
  join::ObStoredJoinRow* store_row = tuple.stored_row_;
  if (OB_ISNULL(store_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("store row is null", K(ret));
  } else {
    join_ctx.cur_left_row_.count_ = store_row->cnt_;
    join_ctx.cur_left_row_.cells_ = const_cast<ObObj*>(store_row->cells());
    join_ctx.left_row_ = &join_ctx.cur_left_row_;
  }
  return ret;
}

int ObHashJoin::join_end_operate(ObPartHashJoinCtx& join_ctx) const
{
  UNUSED(join_ctx);
  return OB_ITER_END;
}

int ObHashJoin::join_end_func_end(ObPartHashJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  UNUSED(join_ctx);
  UNUSED(row);

  LOG_TRACE("trace hash join probe statistics",
      K(join_ctx.bitset_filter_cnt_),
      K(join_ctx.probe_cnt_),
      K(join_ctx.hash_equal_cnt_),
      K(join_ctx.hash_link_cnt_));
  // nest loop process one block, and need next block
  if (ObPartHashJoinCtx::NEST_LOOP == join_ctx.hj_processor_ && HJNestLoopState::GOING == join_ctx.nest_loop_state_) {
    join_ctx.state_ = JS_READ_RIGHT;
    join_ctx.first_get_row_ = true;
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObHashJoin::read_hashrow(ObPartHashJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  const int64_t bucket_id = join_ctx.get_bucket_idx(join_ctx.hash_value_);

  bool is_matched = false;
  int64_t cmp = 0;

  ++join_ctx.probe_cnt_;
  bool pre_matched = false;
  bool has_next = true;
  if (nullptr != join_ctx.cur_tuple_) {
    pre_matched = true;
    has_next = (nullptr != join_ctx.cur_tuple_->next_tuple_);
  }
  if (!has_next || (!pre_matched && !join_ctx.hj_bit_set_.has_member(bucket_id))) {
    ++join_ctx.bitset_filter_cnt_;
    join_ctx.cur_tuple_ = NULL;
    ret = OB_ITER_END;  // is end for scanning this bucket
  } else if (MAX_PART_COUNT != join_ctx.cur_dumped_partition_) {
    int64_t part_idx = join_ctx.get_part_idx(join_ctx.hash_value_);
    if (part_idx > join_ctx.cur_dumped_partition_) {
      // part index is greater than cur_dumped_partition_, than the partition has no memory data
      if (0 < join_ctx.hj_part_array_[part_idx].get_size_in_memory()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect no memory data in the partition",
            K(ret),
            K(part_idx),
            K(join_ctx.hj_part_array_[part_idx].get_size_in_memory()),
            K(join_ctx.cur_dumped_partition_),
            K(join_ctx.part_level_),
            K(join_ctx.part_count_));
      } else {
        join_ctx.cur_tuple_ = NULL;
        ret = OB_ITER_END;  // is end for scanning this bucket
      }
    }
  }

  if (OB_SUCC(ret)) {
    HashTableCell* tuple = NULL;
    if (NULL != join_ctx.cur_tuple_) {
      tuple = join_ctx.cur_tuple_->next_tuple_;
    } else {
      tuple = join_ctx.hash_table_.buckets_->at(bucket_id);
    }
    while (!is_matched && NULL != tuple && OB_SUCC(ret)) {
      ++join_ctx.hash_link_cnt_;
      if (join_ctx.hash_value_ == tuple->stored_row_->get_hash_value()) {
        ++join_ctx.hash_equal_cnt_;
        cmp = 0;
        if (OB_FAIL(convert_tuple(join_ctx, *tuple))) {
          LOG_WARN("check if matched failed", K(ret));
        } else if (OB_FAIL(calc_equal_conds(join_ctx, cmp))) {
          LOG_WARN("calc equal conds failed", K(ret));
        } else if (0 == cmp && OB_FAIL(calc_other_conds(join_ctx, is_matched))) {
          LOG_WARN("calc other conds failed", K(ret));
        } else {
          // do nothing
        }
      }
      if (OB_SUCC(ret) && !is_matched) {
        tuple = tuple->next_tuple_;
      }
    }  // while end

    if (OB_FAIL(ret)) {
    } else if (!is_matched) {
      join_ctx.cur_tuple_ = NULL;
      ret = OB_ITER_END;  // is end for scanning this bucket
    } else {
      join_ctx.cur_tuple_ = tuple;  // last matched tuple
      join_ctx.right_has_matched_ = true;

      if (INNER_JOIN != join_type_) {
        tuple->stored_row_->set_is_match(true);
      }
    }
  }
  return ret;
}

int ObHashJoin::read_hashrow_func_going(ObPartHashJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(join_ctx.hash_join_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("join algo is null", K(ret));
  } else if (OB_FAIL(join_ctx.hash_join_->read_hashrow_func_going(this, join_ctx, row))) {
    LOG_WARN("failed to read hashrow func end", K(ret));
  }
  return ret;
}

int ObHashJoin::HashInnerJoin::read_hashrow_func_going(
    const ObHashJoin* hash_join, ObPartHashJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  const int64_t part_idx = join_ctx.get_part_idx(join_ctx.hash_value_);
  if (OB_FAIL(hash_join->join_rows(join_ctx, row))) {
    LOG_WARN("join rows failed", K(ret), K_(join_ctx.left_row), K_(join_ctx.right_row));
  } else if (nullptr == join_ctx.cur_tuple_->next_tuple_ &&
             (ObHashJoin::ObPartHashJoinCtx::RECURSIVE != join_ctx.hj_processor_ ||
                 !join_ctx.hj_part_array_[part_idx].is_dumped())) {
    // inner join, it don't need to process right match
    // if hash link has no other tuple, then read right after return data
    join_ctx.state_ = JS_READ_RIGHT;
    join_ctx.cur_tuple_ = nullptr;
  }
  return ret;
}

int ObHashJoin::DefaultHashJoin::read_hashrow_func_going(
    const ObHashJoin* hash_join, ObPartHashJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (LEFT_SEMI_JOIN == hash_join->join_type_ || LEFT_ANTI_JOIN == hash_join->join_type_) {
    // do nothing
  } else if (RIGHT_SEMI_JOIN == hash_join->join_type_) {
    // mark this row is match, and return already
    if (ObHashJoin::ObPartHashJoinCtx::NEST_LOOP == join_ctx.hj_processor_) {
      if (!join_ctx.right_bit_set_.has_member(join_ctx.nth_right_row_)) {
        join_ctx.right_bit_set_.add_member(join_ctx.nth_right_row_);
        row = join_ctx.right_row_;
      } else {
        row = nullptr;
      }
    } else {
      row = join_ctx.right_row_;
    }
    join_ctx.cur_tuple_ = NULL;
    join_ctx.state_ = JS_READ_RIGHT;
  } else if (RIGHT_ANTI_JOIN == hash_join->join_type_) {
    if (ObHashJoin::ObPartHashJoinCtx::NEST_LOOP == join_ctx.hj_processor_) {
      if (!join_ctx.right_bit_set_.has_member(join_ctx.nth_right_row_)) {
        join_ctx.right_bit_set_.add_member(join_ctx.nth_right_row_);
      }
    }
    row = NULL;
    join_ctx.cur_tuple_ = NULL;
    join_ctx.state_ = JS_READ_RIGHT;
  } else if (OB_FAIL(hash_join->join_rows(join_ctx, row))) {
    LOG_WARN("join rows failed", K(ret), K_(join_ctx.left_row), K_(join_ctx.right_row));
  } else {
    // do nothing
  }

  return ret;
}

int ObHashJoin::read_hashrow_func_end(ObPartHashJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(join_ctx.hash_join_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("join algo is null", K(ret));
  } else if (OB_FAIL(join_ctx.hash_join_->read_hashrow_func_end(this, join_ctx, row))) {
    LOG_WARN("failed to read hashrow func end", K(ret));
  }
  return ret;
}

int ObHashJoin::HashInnerJoin::read_hashrow_func_end(
    const ObHashJoin* hash_join, ObPartHashJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  const int64_t part_idx = join_ctx.get_part_idx(join_ctx.hash_value_);
  if (ObHashJoin::ObPartHashJoinCtx::RECURSIVE == join_ctx.hj_processor_ &&
      join_ctx.hj_part_array_[part_idx].is_dumped()) {
    join::ObStoredJoinRow* stored_row = NULL;
    if (OB_FAIL(join_ctx.right_hj_part_array_[part_idx].add_row(*join_ctx.right_row_, stored_row))) {
      LOG_WARN("fail to add row", K(ret));
    } else {
      stored_row->set_is_match(join_ctx.right_has_matched_);
      stored_row->set_hash_value(join_ctx.hash_value_);
      if (OB_FAIL(hash_join->dump_probe_table(join_ctx))) {
        LOG_WARN("failed to dump right", K(ret));
      }
    }
  }
  LOG_DEBUG("read hashrow func end", KPC(row), K(hash_join->join_type_), K(join_ctx.right_has_matched_));
  join_ctx.state_ = JS_READ_RIGHT;
  return ret;
}

int ObHashJoin::DefaultHashJoin::read_hashrow_func_end(
    const ObHashJoin* hash_join, ObPartHashJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  const int64_t part_idx = join_ctx.get_part_idx(join_ctx.hash_value_);
  if (ObHashJoin::ObPartHashJoinCtx::RECURSIVE == join_ctx.hj_processor_ &&
      join_ctx.hj_part_array_[part_idx].is_dumped()) {
    if (join_ctx.right_has_matched_ && RIGHT_SEMI_JOIN == hash_join->join_type_) {
      // do nothing, if is right semi join and matched.
      // ret = OB_ERR_UNEXPECTED;
      // LOG_WARN("right semi already return row, so it can't be here", K(ret));
    } else {
      join::ObStoredJoinRow* stored_row = NULL;
      if (OB_FAIL(join_ctx.right_hj_part_array_[part_idx].add_row(*join_ctx.right_row_, stored_row))) {
        LOG_WARN("fail to add row", K(ret));
      } else {
        stored_row->set_is_match(join_ctx.right_has_matched_);
        stored_row->set_hash_value(join_ctx.hash_value_);
        if (OB_FAIL(hash_join->dump_probe_table(join_ctx))) {
          LOG_WARN("failed to dump right", K(ret));
        }
      }
    }
  } else if (!join_ctx.is_last_chunk_) {
    // not the last chunk rows
    if (join_ctx.has_right_bitset_ && join_ctx.right_has_matched_) {
      join_ctx.right_bit_set_.add_member(join_ctx.nth_right_row_);
    }
  } else if (!join_ctx.right_has_matched_) {
    if (hash_join->need_right_join()) {
      if (OB_FAIL(hash_join->right_join_rows(join_ctx, row))) {  // right outer join, null-rightrow
        LOG_WARN("failed to right join rows", K(ret));
      }
    } else if (RIGHT_ANTI_JOIN == hash_join->join_type_) {
      row = join_ctx.right_row_;
    }
  }
  LOG_DEBUG("read hashrow func end", KPC(row), K(hash_join->join_type_), K(join_ctx.right_has_matched_));
  join_ctx.state_ = JS_READ_RIGHT;

  return ret;
}

bool ObHashJoin::all_in_memory(ObPartHashJoinCtx& join_ctx, int64_t size) const
{
  return size < join_ctx.remain_data_memory_size_;
}

int ObHashJoin::in_memory_process(ObPartHashJoinCtx& join_ctx, bool& need_not_read_right) const
{
  int ret = OB_SUCCESS;
  need_not_read_right = false;
  int64_t num_left_rows = 0;
  if (OB_FAIL(build_hash_table_in_memory(join_ctx, num_left_rows))) {
    LOG_WARN("failed to build hash table", K(ret), K(join_ctx.part_level_));
  }
  if (OB_SUCC(ret) && 0 == num_left_rows && RIGHT_ANTI_JOIN != join_type_ && RIGHT_OUTER_JOIN != join_type_ &&
      FULL_OUTER_JOIN != join_type_) {
    need_not_read_right = true;
    LOG_DEBUG("[HASH JOIN]Left table is empty, skip reading right table.", K(num_left_rows), K(join_type_));
  }
  return ret;
}

int ObHashJoin::recursive_process(ObPartHashJoinCtx& join_ctx, bool& need_not_read_right) const
{
  int ret = OB_SUCCESS;
  need_not_read_right = false;
  int64_t num_left_rows = 0;
  if (OB_FAIL(init_join_ctx(join_ctx.exec_ctx_))) {
    LOG_WARN("fail to init join ctx", K(ret));
  } else if (OB_FAIL(split_partition_and_build_hash_table(join_ctx, num_left_rows))) {
    LOG_WARN("failed to build hash table", K(ret), K(join_ctx.part_level_));
  }
  if (OB_SUCC(ret) && 0 == num_left_rows && RIGHT_ANTI_JOIN != join_type_ && RIGHT_OUTER_JOIN != join_type_ &&
      FULL_OUTER_JOIN != join_type_) {
    need_not_read_right = true;
    LOG_DEBUG("[HASH JOIN]Left table is empty, skip reading right table.", K(num_left_rows), K(join_type_));
  }
  return ret;
}

int ObHashJoin::nest_loop_process(ObPartHashJoinCtx& join_ctx, bool& need_not_read_right) const
{
  int ret = OB_SUCCESS;
  need_not_read_right = false;
  int64_t num_left_rows = 0;
  if (OB_FAIL(build_hash_table_for_nest_loop(join_ctx, num_left_rows))) {
    LOG_WARN("failed to build hash table", K(ret), K(join_ctx.part_level_));
  }
  if (OB_SUCC(ret) && 0 == num_left_rows && RIGHT_ANTI_JOIN != join_type_ && RIGHT_OUTER_JOIN != join_type_ &&
      FULL_OUTER_JOIN != join_type_) {
    need_not_read_right = true;
    LOG_DEBUG("[HASH JOIN]Left table is empty, skip reading right table.", K(num_left_rows), K(join_type_));
  }
  return ret;
}

// set partition memory limit to avoid dump due to dump condition of chunk row store
void ObHashJoin::set_partition_memory_limit(ObPartHashJoinCtx& join_ctx, const int64_t mem_limit) const
{
  if (nullptr != join_ctx.hj_part_array_) {
    for (int64_t i = 0; i < join_ctx.part_count_; i++) {
      join_ctx.hj_part_array_[i].get_batch()->set_memory_limit(mem_limit);
    }
    LOG_TRACE("trace set partition memory limit", K(join_ctx.part_count_), K(mem_limit));
  }
}

int64_t ObHashJoin::calc_max_data_size(ObPartHashJoinCtx& join_ctx, const int64_t extra_memory_size) const
{
  int64_t expect_size = join_ctx.profile_.get_expect_size();
  int64_t data_size = expect_size - extra_memory_size;
  if (expect_size < join_ctx.profile_.get_cache_size()) {
    data_size =
        expect_size * (join_ctx.profile_.get_cache_size() - extra_memory_size) / join_ctx.profile_.get_cache_size();
  }
  if (MIN_MEM_SIZE >= data_size) {
    data_size = MIN_MEM_SIZE;
  }
  return data_size;
}

int64_t ObHashJoin::get_extra_memory_size(ObPartHashJoinCtx& join_ctx) const
{
  int64_t row_count = join_ctx.profile_.get_row_count();
  int64_t bucket_cnt = join_ctx.profile_.get_bucket_size();
  int64_t extra_memory_size = bucket_cnt * (sizeof(HashTableCell*) + sizeof(uint8_t));
  extra_memory_size += (row_count * sizeof(HashTableCell));
  return extra_memory_size;
}

// 1 manual, get memory size by _hash_area_size
// 2 auto, get memory size by sql memory manager
int ObHashJoin::get_max_memory_size(ObPartHashJoinCtx& join_ctx, int64_t input_size) const
{
  int ret = OB_SUCCESS;
  int64_t hash_area_size = 0;
  int64_t tenant_id = join_ctx.exec_ctx_.get_my_session()->get_effective_tenant_id();
  int64_t extra_memory_size = get_extra_memory_size(join_ctx);
  int64_t memory_size = extra_memory_size + input_size;
  if (OB_FAIL(ObSqlWorkareaUtil::get_workarea_size(ObSqlWorkAreaType::HASH_WORK_AREA, tenant_id, hash_area_size))) {
    LOG_WARN("failed to get workarea size", K(ret), K(tenant_id));
  } else if (FALSE_IT(join_ctx.remain_data_memory_size_ = hash_area_size * 80 / 100)) {
    // default data memory size: 80%
  } else if (OB_FAIL(join_ctx.sql_mem_processor_.init(
                 join_ctx.alloc_, tenant_id, memory_size, get_type(), get_id(), &join_ctx.exec_ctx_))) {
    LOG_WARN("failed to init sql mem mgr", K(ret));
  } else if (join_ctx.sql_mem_processor_.is_auto_mgr()) {
    join_ctx.remain_data_memory_size_ = calc_max_data_size(join_ctx, extra_memory_size);
    join_ctx.part_count_ = calc_partition_count(input_size, memory_size, MAX_PART_COUNT);
    if (!join_ctx.top_part_level()) {
      if (OB_ISNULL(join_ctx.left_op_) || OB_ISNULL(join_ctx.right_op_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect status: left op or right op is null", K(join_ctx.left_op_), K(join_ctx.right_op_));
      } else {
        // switch callback for count memory size
        join_ctx.left_op_->set_callback(&join_ctx.sql_mem_processor_);
      }
    }
    LOG_TRACE("trace auto memory manager",
        K(hash_area_size),
        K(join_ctx.part_count_),
        K(input_size),
        K(extra_memory_size),
        K(join_ctx.profile_.get_expect_size()),
        K(join_ctx.profile_.get_cache_size()));
  } else {
    join_ctx.part_count_ =
        calc_partition_count(input_size, join_ctx.sql_mem_processor_.get_mem_bound(), MAX_PART_COUNT);
    LOG_TRACE("trace auto memory manager", K(hash_area_size), K(join_ctx.part_count_), K(input_size));
  }
  join_ctx.buf_mgr_->reuse();
  join_ctx.buf_mgr_->set_reserve_memory_size(join_ctx.remain_data_memory_size_);
  return ret;
}

// calculate bucket number by real row count
int ObHashJoin::calc_basic_info(ObPartHashJoinCtx& join_ctx) const
{
  int64_t ret = OB_SUCCESS;
  int64_t buckets_number = 0;
  int64_t row_count = 0;
  int64_t input_size = 0;
  int64_t out_row_count = 0;
  if (ObHashJoin::ObPartHashJoinCtx::NONE == join_ctx.hj_processor_) {
    // estimate
    if (join_ctx.top_part_level()) {
      if (OB_ISNULL(left_op_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("left op is null");
      } else {
        // use estimate value with px if hava
        if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
                &join_ctx.exec_ctx_, px_est_size_factor_, left_op_->get_rows(), row_count))) {
          LOG_WARN("failed to get px size", K(ret));
        } else {
          LOG_TRACE("trace left row count", K(row_count));
          if (row_count < MIN_ROW_COUNT) {
            row_count = MIN_ROW_COUNT;
          }
          // estimated rows: left_row_count * row_size
          input_size = row_count * left_op_->get_width();
        }
      }
    } else {
      if (OB_ISNULL(join_ctx.left_op_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("left op is null", K(ret));
      } else {
        // use actual value
        // it need to be considered swapping left and right
        input_size = join_ctx.left_op_->get_size_on_disk();
        row_count = join_ctx.left_op_->get_row_count_on_disk();
      }
    }
  } else if (ObHashJoin::ObPartHashJoinCtx::RECURSIVE == join_ctx.hj_processor_) {
    if (nullptr != join_ctx.hj_part_array_ && nullptr != join_ctx.right_hj_part_array_) {
      for (int64_t i = 0; i < join_ctx.part_count_; ++i) {
        row_count += join_ctx.hj_part_array_[i].get_row_count_in_memory();
        input_size += join_ctx.hj_part_array_[i].get_size_in_memory();
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect path for calculate bucket number", K(ret));
    }
  } else if (nullptr != join_ctx.left_op_) {
    row_count = join_ctx.left_op_->get_cur_chunk_row_cnt();
    input_size = join_ctx.left_op_->get_cur_chunk_size();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect path for calculate bucket number");
  }
  if (OB_SUCC(ret)) {
    out_row_count = row_count;
    buckets_number = calc_bucket_number(row_count);
    // save basic info
    join_ctx.profile_.set_basic_info(out_row_count, input_size, buckets_number);
  }
  return ret;
}

int64_t ObHashJoin::calc_bucket_number(const int64_t row_count) const
{
  return next_pow2(row_count * RATIO_OF_BUCKETS);
}

int ObHashJoin::get_processor_type(ObPartHashJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  bool enable_nest_loop = join_ctx.hash_join_processor_ & ENABLE_HJ_NEST_LOOP;
  bool enable_in_memory = join_ctx.hash_join_processor_ & ENABLE_HJ_IN_MEMORY;
  bool enable_recursive = join_ctx.hash_join_processor_ & ENABLE_HJ_RECURSIVE;
  int64_t l_size = 0;
  int64_t r_size = 0;
  int64_t recursive_cost = 0;
  int64_t nest_loop_cost = 0;
  bool is_skew = false;
  int64_t pre_total_size = 0;
  int64_t nest_loop_count = 0;
  if (OB_FAIL(calc_basic_info(join_ctx))) {
    LOG_WARN("failed to get input size", K(ret), K(join_ctx.part_level_));
  } else if (OB_FAIL(get_max_memory_size(join_ctx, join_ctx.profile_.get_input_size()))) {
    LOG_WARN("failed to get max memory size", K(ret), K(join_ctx.remain_data_memory_size_));
  } else if (!join_ctx.top_part_level()) {
    // case 1: in-memory
    if (OB_ISNULL(join_ctx.left_op_) || OB_ISNULL(join_ctx.right_op_) || 0 != join_ctx.left_op_->get_size_in_memory() ||
        0 != join_ctx.right_op_->get_size_in_memory()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect: partition is null or partition has memory row", K(ret));
    } else if (enable_in_memory && all_in_memory(join_ctx, join_ctx.left_op_->get_size_on_disk())
        /*|| all_in_memory(join_ctx, join_ctx.right_op_->get_size_on_disk())*/) {
      // TODO: swap
      // load all data in memory, read all rows from chunk row store to memory
      join_ctx.set_processor(ObHashJoin::ObPartHashJoinCtx::IN_MEMORY);
      l_size = join_ctx.left_op_->get_size_on_disk();
    } else {
      l_size = join_ctx.left_op_->get_size_on_disk();
      r_size = join_ctx.right_op_->get_size_on_disk();
      static const int64_t UNIT_COST = 1;
      static const int64_t READ_COST = UNIT_COST;
      static const int64_t WRITE_COST = 2 * UNIT_COST;
      static const int64_t DUMP_RATIO = 70;
      // 2 read and 1 write for left and right
      recursive_cost = READ_COST * (l_size + r_size) + (READ_COST + WRITE_COST) *
                                                           (1 - 0.9 * join_ctx.remain_data_memory_size_ / l_size) *
                                                           (l_size + r_size);
      nest_loop_count = l_size / (join_ctx.remain_data_memory_size_ - 1) + 1;
      nest_loop_cost = (l_size + nest_loop_count * r_size) * READ_COST;
      pre_total_size = join_ctx.left_op_->get_pre_total_size();
      is_skew = (pre_total_size * DUMP_RATIO / 100 < l_size) ||
                (3 <= join_ctx.part_level_ && MAX_PART_COUNT != join_ctx.left_op_->get_pre_part_count() &&
                    pre_total_size * 30 / 100 < l_size);
      if (enable_recursive && recursive_cost < nest_loop_cost && !is_skew && MAX_PART_LEVEL > join_ctx.part_level_) {
        // case 2: recursive process
        join_ctx.set_processor(ObHashJoin::ObPartHashJoinCtx::RECURSIVE);
      } else if (enable_nest_loop) {
        // case 3: nest loop process
        if (!need_right_bitset() || MAX_NEST_LOOP_RIGHT_ROW_COUNT >= join_ctx.right_op_->get_row_count_on_disk()) {
          join_ctx.set_processor(ObHashJoin::ObPartHashJoinCtx::NEST_LOOP);
        } else {
          join_ctx.set_processor(ObHashJoin::ObPartHashJoinCtx::RECURSIVE);
        }
      }
    }
    // if only one processor, then must choose it, for test
    if (enable_nest_loop && !enable_in_memory && !enable_recursive) {
      join_ctx.set_processor(ObHashJoin::ObPartHashJoinCtx::NEST_LOOP);
    } else if (!enable_nest_loop && enable_in_memory && !enable_recursive) {
      // force remain more memory
      int64_t hash_area_size = 0;
      if (OB_FAIL(ObSqlWorkareaUtil::get_workarea_size(ObSqlWorkAreaType::HASH_WORK_AREA,
              join_ctx.exec_ctx_.get_my_session()->get_effective_tenant_id(),
              hash_area_size))) {
        LOG_WARN("failed to get workarea size", K(ret));
      }
      join_ctx.remain_data_memory_size_ = hash_area_size * 10;
      join_ctx.buf_mgr_->set_reserve_memory_size(join_ctx.remain_data_memory_size_);
      join_ctx.set_processor(ObHashJoin::ObPartHashJoinCtx::IN_MEMORY);
    } else if (!enable_nest_loop && !enable_in_memory && enable_recursive && MAX_PART_LEVEL > join_ctx.part_level_) {
      join_ctx.set_processor(ObHashJoin::ObPartHashJoinCtx::RECURSIVE);
    } else if (ObHashJoin::ObPartHashJoinCtx::NONE == join_ctx.hj_processor_) {
      if (MAX_PART_LEVEL >= join_ctx.part_level_) {
        join_ctx.set_processor(ObHashJoin::ObPartHashJoinCtx::NEST_LOOP);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no processor",
            K(join_ctx.part_level_),
            K(join_ctx.hj_processor_),
            K(join_ctx.part_level_),
            K(join_ctx.part_count_),
            K(join_ctx.hash_table_.nbuckets_),
            K(pre_total_size),
            K(recursive_cost),
            K(nest_loop_cost),
            K(is_skew),
            K(l_size),
            K(r_size));
      }
    }
  } else {
    join_ctx.set_processor(ObPartHashJoinCtx::RECURSIVE);
  }
  LOG_TRACE("hash join process type",
      K(join_ctx.part_level_),
      K(join_ctx.hj_processor_),
      K(join_ctx.part_level_),
      K(join_ctx.part_count_),
      K(join_ctx.hash_table_.nbuckets_),
      K(pre_total_size),
      K(recursive_cost),
      K(nest_loop_cost),
      K(is_skew),
      K(l_size),
      K(r_size),
      K(join_ctx.remain_data_memory_size_),
      K(join_ctx.profile_.get_expect_size()),
      K(join_ctx.profile_.get_bucket_size()),
      K(join_ctx.profile_.get_cache_size()),
      K(join_ctx.profile_.get_row_count()),
      K(join_ctx.profile_.get_input_size()),
      K(left_op_->get_width()));
  return ret;
}

int ObHashJoin::adaptive_process(ObPartHashJoinCtx& join_ctx, bool& need_not_read_right) const
{
  int ret = OB_SUCCESS;
  need_not_read_right = false;
  if (OB_FAIL(get_processor_type(join_ctx))) {
    LOG_WARN("failed to get processor", K(join_ctx.hj_processor_), K(ret));
  } else {
    switch (join_ctx.hj_processor_) {
      case ObPartHashJoinCtx::IN_MEMORY: {
        if (OB_FAIL(in_memory_process(join_ctx, need_not_read_right)) && OB_ITER_END != ret) {
          LOG_WARN("failed to process in memory", K(ret));
        }
        break;
      }
      case ObPartHashJoinCtx::RECURSIVE: {
        if (OB_FAIL(recursive_process(join_ctx, need_not_read_right)) && OB_ITER_END != ret) {
          LOG_WARN("failed to recursive process",
              K(ret),
              K(join_ctx.part_level_),
              K(join_ctx.part_count_),
              K(join_ctx.hash_table_.nbuckets_));
        }
        break;
      }
      case ObPartHashJoinCtx::NEST_LOOP: {
        if (OB_FAIL(nest_loop_process(join_ctx, need_not_read_right)) && OB_ITER_END != ret) {
          if (HJNestLoopState::RECURSIVE == join_ctx.nest_loop_state_ && MAX_PART_LEVEL > join_ctx.part_level_) {
            ret = OB_SUCCESS;
            clean_nest_loop_chunk(join_ctx);
            join_ctx.set_processor(ObPartHashJoinCtx::RECURSIVE);
            if (OB_FAIL(recursive_process(join_ctx, need_not_read_right)) && OB_ITER_END != ret) {
              LOG_WARN("failed to process in memory",
                  K(ret),
                  K(join_ctx.part_level_),
                  K(join_ctx.part_count_),
                  K(join_ctx.hash_table_.nbuckets_));
            }
            LOG_TRACE("trace recursive process",
                K(join_ctx.part_level_),
                K(join_ctx.part_level_),
                K(join_ctx.part_count_),
                K(join_ctx.hash_table_.nbuckets_));
          } else {
            LOG_WARN("failed to process in memory",
                K(ret),
                K(join_ctx.part_level_),
                K(join_ctx.part_count_),
                K(join_ctx.hash_table_.nbuckets_));
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect processor", K(ret), K(join_ctx.hj_processor_));
        break;
      }
    }
  }
  LOG_TRACE("trace process type",
      K(join_ctx.part_level_),
      K(join_ctx.part_count_),
      K(join_ctx.hash_table_.nbuckets_),
      K(join_ctx.remain_data_memory_size_));
  return ret;
}

int ObHashJoin::read_right_operate(ObPartHashJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (join_ctx.first_get_row_) {
    int tmp_ret = OB_SUCCESS;
    bool need_not_read_right = false;
    if (ObPartHashJoinCtx::NEST_LOOP == join_ctx.hj_processor_) {
      if (OB_SUCCESS != (tmp_ret = nest_loop_process(join_ctx, need_not_read_right))) {
        ret = tmp_ret;
        LOG_WARN("build hash table failed", K(ret));
      } else {
        join_ctx.first_get_row_ = false;
      }
    } else if (OB_SUCCESS != (tmp_ret = adaptive_process(join_ctx, need_not_read_right))) {
      ret = tmp_ret;
      LOG_WARN("build hash table failed", K(ret));
    } else {
      join_ctx.first_get_row_ = false;
    }
    if (OB_SUCC(ret)) {
      if (need_not_read_right) {
        ret = OB_ITER_END;
        if (ObPartHashJoinCtx::NEST_LOOP == join_ctx.hj_processor_) {
          join_ctx.nest_loop_state_ = HJNestLoopState::END;
        }
      } else {
        if (nullptr != join_ctx.right_op_) {
          if (OB_FAIL(join_ctx.right_op_->set_iterator(false))) {
            LOG_WARN("failed to set iterator", K(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(get_next_right_row(join_ctx)) && OB_ITER_END != ret) {
      LOG_WARN("failed to get next right row", K(ret));
    }
  }
  // for right semi join, if match, then return, so for nest looop process, it only return once
  join_ctx.right_has_matched_ =
      NULL != join_ctx.right_read_row_ &&
      (join_ctx.right_read_row_->is_match() ||
          (join_ctx.has_right_bitset_ && join_ctx.right_bit_set_.has_member(join_ctx.nth_right_row_)));
  return ret;
}

int ObHashJoin::calc_right_hash_value(ObPartHashJoinCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(join_ctx.right_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("right row is null", K(ret));
  } else {
    if (NULL == join_ctx.right_read_row_) {
      if (OB_FAIL(get_right_hash_value(join_ctx, *join_ctx.right_row_, join_ctx.hash_value_))) {
        LOG_WARN("get hash value failed", K(ret), K(join_ctx.right_row_));
      }
    } else {
      join_ctx.hash_value_ = join_ctx.right_read_row_->get_hash_value();
    }
    join_ctx.state_ = JS_READ_HASH_ROW;
  }
  return ret;
}

int ObHashJoin::read_right_func_end(ObPartHashJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  UNUSED(row);
  if (LEFT_ANTI_JOIN == join_type_ || LEFT_SEMI_JOIN == join_type_) {
    join_ctx.state_ = JS_LEFT_ANTI_SEMI;
    join_ctx.cur_tuple_ = join_ctx.hash_table_.buckets_->at(0);
    join_ctx.cur_bkid_ = 0;
  } else if (need_left_join()) {
    join_ctx.state_ = JS_FILL_LEFT;
    join_ctx.cur_tuple_ = join_ctx.hash_table_.buckets_->at(0);
    join_ctx.cur_bkid_ = 0;
  } else {
    join_ctx.state_ = JS_JOIN_END;
  }

  if (ObHashJoin::ObPartHashJoinCtx::RECURSIVE == join_ctx.hj_processor_) {
    for (int64_t i = 0; i < join_ctx.part_count_ && OB_SUCC(ret); i++) {
      if (join_ctx.hj_part_array_[i].is_dumped()) {
        if (OB_FAIL(join_ctx.right_hj_part_array_[i].dump(true))) {
          LOG_WARN("failed to dump", K(ret), K(i));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(finish_dump(join_ctx, false, true))) {
    LOG_WARN("fail to finish dump", K(ret));
  }
  return ret;
}

int ObHashJoin::left_anti_semi_operate(ObPartHashJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  if (LEFT_ANTI_JOIN == join_type_) {
    ret = find_next_unmatched_tuple(join_ctx, join_ctx.cur_tuple_);
  } else if (LEFT_SEMI_JOIN == join_type_) {
    ret = find_next_matched_tuple(join_ctx, join_ctx.cur_tuple_);
  }

  return ret;
}

int ObHashJoin::left_anti_semi_going(ObPartHashJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  HashTableCell* tuple = join_ctx.cur_tuple_;
  if (LEFT_ANTI_JOIN == join_type_ && (NULL != tuple && !tuple->stored_row_->is_match())) {
    if (OB_FAIL(convert_tuple(join_ctx, *tuple))) {
      LOG_WARN("convert tuple failed", K(ret));
    } else {
      tuple->stored_row_->set_is_match(true);
      row = join_ctx.left_row_;
    }
  } else if (LEFT_SEMI_JOIN == join_type_ && (NULL != tuple && tuple->stored_row_->is_match())) {
    if (OB_FAIL(convert_tuple(join_ctx, *tuple))) {
      LOG_WARN("convert tuple failed", K(ret));
    } else {
      tuple->stored_row_->set_is_match(false);
      row = join_ctx.left_row_;
    }
  }
  return ret;
}

int ObHashJoin::left_anti_semi_end(ObPartHashJoinCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  join_ctx.state_ = JS_JOIN_END;
  return ret;
}

int ObHashJoin::fill_left_operate(ObPartHashJoinCtx& join_ctx) const
{
  int ret = OB_SUCCESS;
  ret = find_next_unmatched_tuple(join_ctx, join_ctx.cur_tuple_);
  return ret;
}

int ObHashJoin::fill_left_going(ObPartHashJoinCtx& join_ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  HashTableCell* tuple = join_ctx.cur_tuple_;
  if (NULL != tuple && !tuple->stored_row_->is_match()) {
    if (OB_FAIL(convert_tuple(join_ctx, *tuple))) {
      LOG_WARN("convert tuple failed", K(ret));
    } else if (OB_FAIL(left_join_rows(join_ctx, row))) {
      LOG_WARN("left join rows failed", K(ret));
    } else {
      tuple->stored_row_->set_is_match(true);
    }
  }
  return ret;
}

int ObHashJoin::fill_left_end(ObPartHashJoinCtx& join_ctx, const ObNewRow*& row) const
{
  UNUSED(row);
  int ret = OB_SUCCESS;
  join_ctx.state_ = JS_JOIN_END;
  return ret;
}

OB_SERIALIZE_MEMBER((ObHashJoin, ObJoin), mem_limit_);
}  // namespace sql
}  // namespace oceanbase
