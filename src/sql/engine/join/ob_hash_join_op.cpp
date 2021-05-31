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

#include "sql/engine/join/ob_hash_join_op.h"
#include "sql/engine/px/ob_px_util.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/engine/px/ob_px_util.h"
#include "share/diagnosis/ob_sql_monitor_statname.h"

namespace oceanbase {
using namespace omt;
using namespace common;
namespace sql {

ObHashJoinSpec::ObHashJoinSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
    : ObJoinSpec(alloc, type),
      equal_join_conds_(alloc),
      all_join_keys_(alloc),
      all_hash_funcs_(alloc),
      has_join_bf_(false)
{}

OB_SERIALIZE_MEMBER((ObHashJoinSpec, ObJoinSpec), equal_join_conds_, all_join_keys_, all_hash_funcs_, has_join_bf_);

int ObHashJoinOp::PartHashJoinTable::init(ObIAllocator& alloc)
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
      ht_alloc_->set_label("HtOpAlloc");
      buckets_ = new (bucket_buf) BucketArray(*ht_alloc_);
      all_cells_ = new (cell_buf) AllCellArray(*ht_alloc_);
      collision_cnts_ = new (collision_buf) CollisionCntArray(*ht_alloc_);
    }
  }
  return ret;
}

ObHashJoinOp::ObHashJoinOp(ObExecContext& ctx_, const ObOpSpec& spec, ObOpInput* input)
    : ObJoinOp(ctx_, spec, input),
      hj_state_(INIT),
      hj_processor_(NONE),
      buf_mgr_(NULL),
      batch_mgr_(NULL),
      left_batch_(NULL),
      right_batch_(NULL),
      tmp_hash_funcs_(ctx_.get_allocator()),
      left_hash_funcs_(),
      right_hash_funcs_(),
      left_join_keys_(),
      right_join_keys_(),
      going_func_(nullptr),
      end_func_(nullptr),
      part_level_(0),
      part_shift_(MAX_PART_LEVEL << 3),
      part_count_(0),
      force_hash_join_spill_(false),
      hash_join_processor_(7),
      tenant_id_(-1),
      input_size_(0),
      total_extra_size_(0),
      predict_row_cnt_(1024),
      profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
      sql_mem_processor_(profile_),
      state_(JS_READ_RIGHT),
      cur_right_hash_value_(0),
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
      bloom_filter_alloc_(nullptr),
      bloom_filter_(nullptr),
      nth_right_row_(-1),
      ltb_size_(INIT_LTB_SIZE),
      l2_cache_size_(INIT_L2_CACHE_SIZE),
      price_per_row_(48),
      max_partition_count_per_level_(ltb_size_ << 1),
      cur_dumped_partition_(max_partition_count_per_level_),
      nest_loop_state_(HJLoopState::LOOP_START),
      is_last_chunk_(false),
      has_right_bitset_(false),
      hj_part_array_(NULL),
      right_hj_part_array_(NULL),
      left_read_row_(NULL),
      right_read_row_(NULL),
      postprocessed_left_(false),
      has_fill_right_row_(false),
      has_fill_left_row_(false),
      right_last_row_(),
      need_return_(false),
      iter_end_(false),
      opt_cache_aware_(false),
      has_right_material_data_(false),
      enable_bloom_filter_(false),
      part_histograms_(nullptr),
      cur_full_right_partition_(INT64_MAX),
      right_iter_end_(false),
      cur_bucket_idx_(0),
      max_bucket_idx_(0),
      enable_batch_(false),
      level1_bit_(0),
      level1_part_count_(0),
      level2_part_count_(0),
      right_splitter_(),
      cur_left_hist_(nullptr),
      cur_right_hist_(nullptr),
      cur_probe_row_idx_(0),
      max_right_bucket_idx_(0),
      probe_cnt_(0),
      bitset_filter_cnt_(0),
      hash_link_cnt_(0),
      hash_equal_cnt_(0)
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
  state_operation_func_[JS_JOIN_END] = &ObHashJoinOp::join_end_operate;
  state_function_func_[JS_JOIN_END][FT_ITER_GOING] = NULL;
  state_function_func_[JS_JOIN_END][FT_ITER_END] = &ObHashJoinOp::join_end_func_end;

  state_operation_func_[JS_READ_RIGHT] = &ObHashJoinOp::read_right_operate;
  state_function_func_[JS_READ_RIGHT][FT_ITER_GOING] = &ObHashJoinOp::calc_right_hash_value;
  state_function_func_[JS_READ_RIGHT][FT_ITER_END] = &ObHashJoinOp::read_right_func_end;
  // do probe
  state_operation_func_[JS_READ_HASH_ROW] = &ObHashJoinOp::read_hashrow;
  state_function_func_[JS_READ_HASH_ROW][FT_ITER_GOING] = &ObHashJoinOp::read_hashrow_func_going;
  state_function_func_[JS_READ_HASH_ROW][FT_ITER_END] = &ObHashJoinOp::read_hashrow_func_end;
  // for anti
  state_operation_func_[JS_LEFT_ANTI_SEMI] = &ObHashJoinOp::left_anti_semi_operate;
  state_function_func_[JS_LEFT_ANTI_SEMI][FT_ITER_GOING] = &ObHashJoinOp::left_anti_semi_going;
  state_function_func_[JS_LEFT_ANTI_SEMI][FT_ITER_END] = &ObHashJoinOp::left_anti_semi_end;
  // for unmatched left row, for left-outer,full-outer,left-semi
  state_operation_func_[JS_FILL_LEFT] = &ObHashJoinOp::fill_left_operate;
  state_function_func_[JS_FILL_LEFT][FT_ITER_GOING] = &ObHashJoinOp::fill_left_going;
  state_function_func_[JS_FILL_LEFT][FT_ITER_END] = &ObHashJoinOp::fill_left_end;
}

int ObHashJoinOp::set_hash_function(int8_t hash_join_hasher)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<common::ObHashFunc>* cur_hash_funcs = &MY_SPEC.all_hash_funcs_;
  if (DEFAULT_MURMUR_HASH != (hash_join_hasher & HASH_FUNCTION_MASK)) {
    ObHashFunc hash_func;
    if (OB_FAIL(tmp_hash_funcs_.init(MY_SPEC.all_hash_funcs_.count()))) {
      LOG_WARN("failed to init tmp hash func", K(ret));
    } else {
      for (int64_t i = 0; i < MY_SPEC.all_hash_funcs_.count() && OB_SUCC(ret); ++i) {
        if (ENABLE_WY_HASH == (hash_join_hasher & HASH_FUNCTION_MASK)) {
          hash_func.hash_func_ = MY_SPEC.all_join_keys_.at(i)->basic_funcs_->wy_hash_;
        } else if (ENABLE_XXHASH64 == (hash_join_hasher & HASH_FUNCTION_MASK)) {
          hash_func.hash_func_ = MY_SPEC.all_join_keys_.at(i)->basic_funcs_->xx_hash_;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: hash join hasher is invalid", K(ret), K(hash_join_hasher));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(tmp_hash_funcs_.push_back(hash_func))) {
          LOG_WARN("failed to push back wy hash func", K(ret));
        } else {
          LOG_DEBUG("debug hash function", K(hash_func), K(i), K(*MY_SPEC.all_join_keys_.at(i)));
        }
      }
      cur_hash_funcs = &tmp_hash_funcs_;
    }
  }
  if (OB_SUCC(ret)) {
    int64_t all_cnt = cur_hash_funcs->count();
    left_hash_funcs_.init(all_cnt / 2, const_cast<ObHashFunc*>(&cur_hash_funcs->at(0)), all_cnt / 2);
    right_hash_funcs_.init(all_cnt / 2,
        const_cast<ObHashFunc*>(&cur_hash_funcs->at(0) + left_hash_funcs_.count()),
        cur_hash_funcs->count() - left_hash_funcs_.count());
    if (left_hash_funcs_.count() != right_hash_funcs_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: hash func is not match",
          K(ret),
          K(cur_hash_funcs->count()),
          K(left_hash_funcs_.count()),
          K(right_hash_funcs_.count()));
    } else if (MY_SPEC.all_join_keys_.count() != cur_hash_funcs->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: hash func is not match",
          K(ret),
          K(cur_hash_funcs->count()),
          K(MY_SPEC.all_join_keys_.count()));
    }
  }
  return ret;
}

int ObHashJoinOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = NULL;
  if ((OB_UNLIKELY(MY_SPEC.all_join_keys_.count() <= 0 ||
                   MY_SPEC.all_join_keys_.count() != MY_SPEC.all_hash_funcs_.count() || OB_ISNULL(left_)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no equal join conds or left op is null", K(ret));
  } else if (OB_FAIL(ObJoinOp::inner_open())) {
    LOG_WARN("failed to open in base class", K(ret));
  } else if (OB_ISNULL(session = ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(init_mem_context(session->get_effective_tenant_id()))) {
    LOG_WARN("fail to init base join ctx", K(ret));
  } else if (OB_FAIL(hash_table_.init(*alloc_))) {
    LOG_WARN("fail to init hash table", K(ret));
  } else {
    init_system_parameters();
    tenant_id_ = session->get_effective_tenant_id();
    first_get_row_ = true;
    ObTenantConfigGuard tenant_config(TENANT_CONF(session->get_effective_tenant_id()));
    if (tenant_config.is_valid()) {
      force_hash_join_spill_ = tenant_config->_force_hash_join_spill;
      hash_join_processor_ = tenant_config->_enable_hash_join_processor;
      if (0 == (hash_join_processor_ & HJ_PROCESSOR_MASK)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect hash join processor", K(ret), K(hash_join_processor_));
      } else if (OB_FAIL(set_hash_function(tenant_config->_enable_hash_join_hasher))) {
        LOG_WARN("unexpect hash join function", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tenant config", K(ret));
    }
    going_func_ = &ObHashJoinOp::other_join_read_hashrow_func_going;
    end_func_ = &ObHashJoinOp::other_join_read_hashrow_func_end;
    if (INNER_JOIN == MY_SPEC.join_type_) {
      going_func_ = &ObHashJoinOp::inner_join_read_hashrow_func_going;
      end_func_ = &ObHashJoinOp::inner_join_read_hashrow_func_end;
    }
  }
  if (OB_SUCC(ret)) {
    int64_t all_cnt = MY_SPEC.all_join_keys_.count();
    left_join_keys_.init(all_cnt / 2, const_cast<ObExpr**>(&MY_SPEC.all_join_keys_.at(0)), all_cnt / 2);
    right_join_keys_.init(all_cnt - left_join_keys_.count(),
        const_cast<ObExpr**>(&MY_SPEC.all_join_keys_.at(0) + left_join_keys_.count()),
        all_cnt - left_join_keys_.count());
    if (left_join_keys_.count() != right_join_keys_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: left and right join is not match",
          K(ret),
          K(left_join_keys_.count()),
          K(right_join_keys_.count()));
    }
    LOG_DEBUG("trace join keys",
        K(left_join_keys_),
        K(right_join_keys_),
        K(left_join_keys_.count()),
        K(right_join_keys_.count()));
  }
  if (OB_SUCC(ret)) {
    part_count_ = 0;
    hj_state_ = ObHashJoinOp::INIT;
    void* buf = alloc_->alloc(sizeof(ObHashJoinBufMgr));
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem");
    } else {
      // at least one page for each l/r part
      buf_mgr_ = new (buf) ObHashJoinBufMgr();
      buf_mgr_->set_page_size(OB_MALLOC_MIDDLE_BLOCK_SIZE);
    }
  }
  if (OB_SUCC(ret)) {
    void* buf = alloc_->alloc(sizeof(ObHashJoinBatchMgr));
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem");
    } else {
      batch_mgr_ = new (buf) ObHashJoinBatchMgr(*alloc_, buf_mgr_, tenant_id_);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(right_last_row_.init(*alloc_, right_->get_spec().output_.count()))) {
      LOG_WARN("failed to init right last row", K(ret));
    }
  }
  return ret;
}

void ObHashJoinOp::reset_base()
{
  hj_state_ = INIT;
  hj_processor_ = NONE;
  part_level_ = 0;
  part_shift_ = MAX_PART_LEVEL << 3;
  part_count_ = 0;
  input_size_ = 0;
  predict_row_cnt_ = 1024;
  left_batch_ = nullptr;
  right_batch_ = nullptr;
}

void ObHashJoinOp::reset()
{
  free_bloom_filter();
  clean_batch_mgr();
  part_rescan();
  reset_base();
}

void ObHashJoinOp::part_rescan()
{
  state_ = JS_READ_RIGHT;
  cur_right_hash_value_ = 0;
  right_has_matched_ = false;
  tuple_need_join_ = false;
  first_get_row_ = true;
  cur_bkid_ = 0;
  hash_table_.reset();
  cur_tuple_ = NULL;
  if (nullptr != bloom_filter_) {
    bloom_filter_->reset();
  }
  right_bit_set_.reset();
  nest_loop_state_ = HJLoopState::LOOP_START;
  is_last_chunk_ = false;
  has_right_bitset_ = false;
  remain_data_memory_size_ = 0;
  reset_nest_loop();
  hj_processor_ = NONE;
  cur_dumped_partition_ = max_partition_count_per_level_;
  reset_statistics();
  predict_row_cnt_ = 1024;
  if (hj_part_array_ != NULL) {
    for (int64_t i = 0; i < part_count_; i++) {
      hj_part_array_[i].~ObHashJoinPartition();
    }
    alloc_->free(hj_part_array_);
    hj_part_array_ = NULL;
  }
  if (right_hj_part_array_ != NULL) {
    for (int64_t i = 0; i < part_count_; i++) {
      right_hj_part_array_[i].~ObHashJoinPartition();
    }
    alloc_->free(right_hj_part_array_);
    right_hj_part_array_ = NULL;
  }
  int64_t tmp_part_count = 0 < level2_part_count_ ? level1_part_count_ * level2_part_count_ : level1_part_count_;
  if (OB_NOT_NULL(part_histograms_)) {
    for (int64_t i = 0; i < tmp_part_count; i++) {
      part_histograms_[i].~HashJoinHistogram();
    }
    alloc_->free(part_histograms_);
    part_histograms_ = NULL;
  }
  left_read_row_ = NULL;
  right_read_row_ = NULL;
  postprocessed_left_ = false;
  has_fill_right_row_ = false;
  has_fill_left_row_ = false;
  need_return_ = false;
  opt_cache_aware_ = false;
  has_right_material_data_ = false;
  cur_full_right_partition_ = INT64_MAX;
  right_iter_end_ = false;
  cur_bucket_idx_ = 0;
  max_bucket_idx_ = 0;
  enable_batch_ = false;
  level1_bit_ = 0;
  level1_part_count_ = 0;
  level2_part_count_ = 0;
  cur_left_hist_ = nullptr;
  cur_right_hist_ = nullptr;
  right_splitter_.reset();
  cur_probe_row_idx_ = 0;
  max_right_bucket_idx_ = 0;
}

int ObHashJoinOp::part_rescan(bool reset_all)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo* session = NULL;
  if (OB_ISNULL(session = ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else {
    if (reset_all) {
      reset();
      part_count_ = 0;
    } else {
      part_rescan();
    }
    if (OB_SUCC(ret)) {
      // reset join ctx
      left_row_joined_ = false;
    }
  }
  return ret;
}

int ObHashJoinOp::rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(part_rescan(true))) {
    LOG_WARN("part rescan failed", K(ret));
  } else if (OB_FAIL(ObJoinOp::rescan())) {
    LOG_WARN("join rescan failed", K(ret));
  } else {
    iter_end_ = false;
  }
  LOG_TRACE("hash join rescan", K(ret));
  return ret;
}

int ObHashJoinOp::next()
{
  int ret = OB_SUCCESS;
  state_operation_func_type state_operation = NULL;
  state_function_func_type state_function = NULL;
  ObJoinState& state = state_;
  int func = -1;
  while (OB_SUCC(ret) && !need_return_) {
    state_operation = this->ObHashJoinOp::state_operation_func_[state];
    if (OB_ISNULL(state_operation)) {
      ret = OB_BAD_NULL_ERROR;
      LOG_WARN("state_operation is null", K(ret), K(state));
    } else if (OB_ITER_END == (ret = (this->*state_operation)())) {
      func = FT_ITER_END;
      ret = OB_SUCCESS;
    } else if (OB_FAIL(ret)) {
      LOG_WARN("failed state operation", K(ret), K(state));
    } else {
      func = FT_ITER_GOING;
    }
    if (OB_SUCC(ret)) {
      state_function = this->ObHashJoinOp::state_function_func_[state][func];
      if (OB_FAIL((this->*state_function)())) {
        if (OB_ITER_END == ret) {
          // iter_end ,break;
        } else {
          LOG_WARN("failed state function", K(ret), K(state), K(func));
        }
      }
    }
  }  // while end
  return ret;
}

int ObHashJoinOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool exit_while = false;
  need_return_ = false;
  has_fill_left_row_ = false;
  clear_evaluated_flag();
  if (OB_UNLIKELY(iter_end_)) {
    ret = OB_ITER_END;
  }
  while (OB_SUCCESS == ret && !exit_while) {
    switch (hj_state_) {
      case ObHashJoinOp::HJState::INIT: {
        hj_state_ = ObHashJoinOp::HJState::NORMAL;
        break;
      }
      case ObHashJoinOp::HJState::NORMAL: {
        ret = next();
        if (OB_ITER_END == ret) {
          hj_state_ = ObHashJoinOp::HJState::NEXT_BATCH;
          ret = OB_SUCCESS;
        } else if (OB_SUCCESS == ret) {
          exit_while = true;
        } else {
          LOG_WARN("fail to get next row", K(ret));
        }
        break;
      }
      case ObHashJoinOp::HJState::NEXT_BATCH: {
        batch_mgr_->remove_undumped_batch();
        if (left_batch_ != NULL) {
          left_batch_->close();
          batch_mgr_->free(left_batch_);
          left_batch_ = NULL;
        }
        if (right_batch_ != NULL) {
          right_batch_->close();
          batch_mgr_->free(right_batch_);
          right_batch_ = NULL;
        }
        ObHashJoinBatchPair batch_pair;
        if (OB_FAIL(part_rescan(false))) {
          LOG_WARN("fail to reopen hj", K(ret));
        } else if (OB_FAIL(batch_mgr_->next_batch(batch_pair))) {
        } else if (0 != buf_mgr_->get_total_alloc_size()) {
          LOG_WARN("expect memory count is ok", K(ret), K(buf_mgr_->get_total_alloc_size()));
        }
        if (OB_ITER_END == ret) {
          exit_while = true;
          // free resource like memory
          iter_end_ = true;
          part_rescan(true);
        } else if (OB_SUCCESS == ret) {
          left_batch_ = batch_pair.left_;
          right_batch_ = batch_pair.right_;

          part_level_ = batch_pair.left_->get_part_level() + 1;
          part_shift_ = (part_level_ + MAX_PART_LEVEL) << 3;

          batch_pair.left_->open();
          batch_pair.right_->open();

          if (MAX_PART_LEVEL < part_level_) {
            // avoid loop recursively
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("too deep part level", K(ret), K(part_level_));
          } else {
            hj_state_ = ObHashJoinOp::HJState::NORMAL;
          }
          LOG_DEBUG(
              "trace batch", K(batch_pair.left_->get_batchno()), K(batch_pair.right_->get_batchno()), K(part_level_));
        } else {
          LOG_WARN("fail get next batch", K(ret));
        }
        break;
      }
    }
  }
  return ret;
}

void ObHashJoinOp::destroy()
{
  if (OB_LIKELY(nullptr != alloc_)) {
    alloc_ = nullptr;
  }
  if (OB_LIKELY(NULL != mem_context_)) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = NULL;
  }
  ObJoinOp::destroy();
}

int ObHashJoinOp::inner_close()
{
  int ret = OB_SUCCESS;
  sql_mem_processor_.unregister_profile();
  reset();
  tmp_hash_funcs_.reset();
  if (batch_mgr_ != NULL) {
    batch_mgr_->~ObHashJoinBatchMgr();
    if (OB_NOT_NULL(alloc_)) {
      alloc_->free(batch_mgr_);
    }
    batch_mgr_ = NULL;
  }
  if (buf_mgr_ != NULL) {
    buf_mgr_->~ObHashJoinBufMgr();
    if (OB_NOT_NULL(alloc_)) {
      alloc_->free(buf_mgr_);
    }
    buf_mgr_ = NULL;
  }
  right_last_row_.reset();
  if (nullptr != alloc_) {
    hash_table_.free(alloc_);
    alloc_->reset();
  }
  if (nullptr != mem_context_) {
    mem_context_->reuse();
  }
  if (OB_FAIL(ObJoinOp::inner_close())) {}
  return ret;
}

int ObHashJoinOp::join_end_operate()
{
  return OB_ITER_END;
}

int ObHashJoinOp::join_end_func_end()
{
  int ret = OB_SUCCESS;
  LOG_TRACE(
      "trace hash join probe statistics", K(bitset_filter_cnt_), K(probe_cnt_), K(hash_equal_cnt_), K(hash_link_cnt_));
  // nest loop process one block, and need next block
  if (HJProcessor::NEST_LOOP == hj_processor_ && HJLoopState::LOOP_GOING == nest_loop_state_) {
    state_ = JS_READ_RIGHT;
    first_get_row_ = true;
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObHashJoinOp::get_next_left_row()
{
  int ret = common::OB_SUCCESS;
  left_row_joined_ = false;
  if (left_batch_ == NULL) {
    if (OB_FAIL(OB_I(t1) left_->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get left row from child failed", K(ret));
      }
    }
  } else {
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(OB_I(t1) left_batch_->get_next_row(left_->get_spec().output_, eval_ctx_, left_read_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get left row from partition failed", K(ret));
      }
    }
    LOG_DEBUG("part join ctx get left row", K(ret));
  }
  return ret;
}

int ObHashJoinOp::reuse_for_next_chunk()
{
  int ret = OB_SUCCESS;
  // first time, the bucket number is already calculated
  // calculate the buckets number of hash table
  if (top_part_level() || 0 == hash_table_.nbuckets_ || NEST_LOOP != hj_processor_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected hash buckets number is 0", K(ret), K(part_level_));
  } else if (OB_FAIL(calc_basic_info())) {
    LOG_WARN("failed to calc basic info", K(ret));
  } else {
    // reuse buckets
    PartHashJoinTable& hash_table = hash_table_;
    hash_table.buckets_->set_all(NULL);
    hash_table.collision_cnts_->set_all(0);

    int64_t row_count = profile_.get_row_count();
    if (row_count > hash_table.row_count_) {
      hash_table.row_count_ = row_count;
      hash_table.all_cells_->reuse();
      if (OB_FAIL(hash_table.all_cells_->init(hash_table.row_count_))) {
        LOG_WARN("failed to init hash_table.all_cells_", K(ret), K(hash_table.row_count_));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_bloom_filter(mem_context_->get_malloc_allocator(), hash_table_.nbuckets_))) {
      LOG_WARN("failed to create bloom filter", K(ret));
    } else if (OB_FAIL(right_batch_->rescan())) {
      LOG_WARN("failed to rescan right", K(ret));
    } else {
      nth_right_row_ = -1;
    }
    LOG_TRACE("trace hash table",
        K(ret),
        K(hash_table.nbuckets_),
        K(row_count),
        K(nth_right_row_),
        K(profile_.get_row_count()));
  }
  return ret;
}

int ObHashJoinOp::load_next_chunk()
{
  int ret = OB_SUCCESS;
  ++nth_nest_loop_;
  if (1 == nth_nest_loop_ && OB_FAIL(left_batch_->set_iterator(true))) {
    LOG_WARN("failed to set iterator", K(ret), K(nth_nest_loop_));
  } else if (OB_FAIL(left_batch_->load_next_chunk())) {
    LOG_WARN("failed to load next chunk", K(ret), K(nth_nest_loop_));
  } else if (1 == nth_nest_loop_ && OB_FAIL(prepare_hash_table())) {
    LOG_WARN("failed to prepare hash table", K(ret), K(nth_nest_loop_));
  } else if (1 < nth_nest_loop_ && OB_FAIL(reuse_for_next_chunk())) {
    LOG_WARN("failed to reset info for block", K(ret), K(nth_nest_loop_));
  }
  return ret;
}

int ObHashJoinOp::build_hash_table_for_nest_loop(int64_t& num_left_rows)
{
  int ret = OB_SUCCESS;
  uint64_t hash_value = 0;
  num_left_rows = 0;
  ObHashJoinStoredJoinRow* stored_row = nullptr;
  HashTableCell* tuple = NULL;
  nest_loop_state_ = HJLoopState::LOOP_GOING;
  is_last_chunk_ = false;
  if (OB_FAIL(load_next_chunk())) {
    LOG_WARN("failed to reset info for block", K(ret));
  } else {
    PartHashJoinTable& hash_table = hash_table_;
    int64_t cell_index = 0;
    int64_t bucket_id = 0;
    ObHashJoinBatch* hj_batch = left_batch_;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(get_next_left_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next left row failed", K(ret));
        } else if (!hj_batch->has_next()) {
          // last chunk
          is_last_chunk_ = true;
          nest_loop_state_ = HJLoopState::LOOP_END;
          if (cur_nth_row_ != hj_batch->get_row_count_on_disk()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expect row count is match", K(ret), K(cur_nth_row_), K(hj_batch->get_row_count_on_disk()));
          }
        }
      } else if (num_left_rows >= hash_table.row_count_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected row count", K(ret), K(num_left_rows), K(hash_table.row_count_));
      } else {
        stored_row = const_cast<ObHashJoinStoredJoinRow*>(left_read_row_);
        hash_value = stored_row->get_hash_value();
        bucket_id = get_bucket_idx(hash_value);
        if (enable_bloom_filter_ && OB_FAIL(bloom_filter_->set(hash_value))) {
          LOG_WARN("add hash value to bloom failed", K(ret));
        }
        tuple = &(hash_table.all_cells_->at(cell_index));
        tuple->stored_row_ = stored_row;
        tuple->next_tuple_ = hash_table.buckets_->at(bucket_id);
        hash_table.buckets_->at(bucket_id) = tuple;
        hash_table.inc_collision(bucket_id);
        ++cell_index;
        ++num_left_rows;
        ++cur_nth_row_;
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    trace_hash_table_collision(num_left_rows);
  }
  LOG_TRACE(
      "trace block hash join", K(nth_nest_loop_), K(part_level_), K(ret), K(num_left_rows), K(hash_table_.nbuckets_));
  return ret;
}

int ObHashJoinOp::nest_loop_process(bool& need_not_read_right)
{
  int ret = OB_SUCCESS;
  need_not_read_right = false;
  int64_t num_left_rows = 0;
  postprocessed_left_ = true;
  if (OB_FAIL(build_hash_table_for_nest_loop(num_left_rows))) {
    LOG_WARN("failed to build hash table", K(ret), K(part_level_));
  }
  if (OB_SUCC(ret) && 0 == num_left_rows && RIGHT_ANTI_JOIN != MY_SPEC.join_type_ &&
      RIGHT_OUTER_JOIN != MY_SPEC.join_type_ && FULL_OUTER_JOIN != MY_SPEC.join_type_) {
    need_not_read_right = true;
    LOG_DEBUG("[HASH JOIN]Left table is empty, skip reading right table.", K(num_left_rows), K(MY_SPEC.join_type_));
  }
  return ret;
}

// given partition size and input size to calculate partition count
int64_t ObHashJoinOp::calc_partition_count(int64_t input_size, int64_t part_size, int64_t max_part_count)
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

int64_t ObHashJoinOp::calc_partition_count_by_cache_aware(
    int64_t row_count, int64_t max_part_count, int64_t global_mem_bound_size)
{
  int64_t price_per_row = price_per_row_;
  int64_t row_count_cache_aware = l2_cache_size_ / price_per_row;
  int64_t partition_cnt = next_pow2(row_count / row_count_cache_aware);
  if (max_part_count < partition_cnt) {
    partition_cnt = max_part_count;
  }
  while (partition_cnt * PAGE_SIZE > global_mem_bound_size) {
    partition_cnt >>= 1;
  }
  partition_cnt = partition_cnt < MIN_PART_COUNT ? MIN_PART_COUNT : partition_cnt;
  return partition_cnt;
}

int64_t ObHashJoinOp::calc_max_data_size(const int64_t extra_memory_size)
{
  int64_t expect_size = profile_.get_expect_size();
  int64_t data_size = expect_size - extra_memory_size;
  if (expect_size < profile_.get_cache_size()) {
    data_size = expect_size * (profile_.get_cache_size() - extra_memory_size) / profile_.get_cache_size();
  }
  if (MIN_MEM_SIZE >= data_size) {
    data_size = MIN_MEM_SIZE;
  }
  return data_size;
}

// 1 manual, get memory size by _hash_area_size
// 2 auto, get memory size by sql memory manager
int ObHashJoinOp::get_max_memory_size(int64_t input_size)
{
  int ret = OB_SUCCESS;
  int64_t hash_area_size = 0;
  const int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  // default data memory size: 80%
  int64_t extra_memory_size = get_extra_memory_size();
  int64_t memory_size = extra_memory_size + input_size;
  if (OB_FAIL(ObSqlWorkareaUtil::get_workarea_size(ObSqlWorkAreaType::HASH_WORK_AREA, tenant_id, hash_area_size))) {
    LOG_WARN("failed to get workarea size", K(ret), K(tenant_id));
  } else if (FALSE_IT(remain_data_memory_size_ = hash_area_size * 80 / 100)) {
    // default data memory size: 80%
  } else if (OB_FAIL(sql_mem_processor_.init(alloc_, tenant_id, memory_size, MY_SPEC.type_, MY_SPEC.id_, &ctx_))) {
    LOG_WARN("failed to init sql mem mgr", K(ret));
  } else if (sql_mem_processor_.is_auto_mgr()) {
    remain_data_memory_size_ = calc_max_data_size(extra_memory_size);
    // part_count_ = calc_partition_count(input_size, memory_size, max_partition_count_per_level_);
    part_count_ =
        calc_partition_count_by_cache_aware(profile_.get_row_count(), max_partition_count_per_level_, memory_size);
    if (!top_part_level()) {
      if (OB_ISNULL(left_batch_) || OB_ISNULL(right_batch_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect status: left op or right op is null", K(left_batch_), K(right_batch_));
      } else {
        // switch callback for count memory size
        left_batch_->set_callback(&sql_mem_processor_);
      }
    }
    LOG_TRACE("trace auto memory manager",
        K(hash_area_size),
        K(part_count_),
        K(input_size),
        K(extra_memory_size),
        K(profile_.get_expect_size()),
        K(profile_.get_cache_size()));
  } else {
    // part_count_ = calc_partition_count(
    //   input_size, sql_mem_processor_.get_mem_bound(), max_partition_count_per_level_);
    part_count_ = calc_partition_count_by_cache_aware(
        profile_.get_row_count(), max_partition_count_per_level_, sql_mem_processor_.get_mem_bound());
    LOG_TRACE("trace auto memory manager", K(hash_area_size), K(part_count_), K(input_size));
  }
  buf_mgr_->reuse();
  buf_mgr_->set_reserve_memory_size(remain_data_memory_size_);
  return ret;
}

int64_t ObHashJoinOp::calc_bucket_number(const int64_t row_count)
{
  return next_pow2(row_count * RATIO_OF_BUCKETS);
}

// calculate bucket number by real row count
int ObHashJoinOp::calc_basic_info()
{
  int64_t ret = OB_SUCCESS;
  int64_t buckets_number = 0;
  int64_t row_count = 0;
  int64_t input_size = 0;
  int64_t out_row_count = 0;
  if (NONE == hj_processor_) {
    // estimate
    if (top_part_level()) {
      // use estimate value with px if hava
      if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
              &ctx_, MY_SPEC.px_est_size_factor_, left_->get_spec().rows_, row_count))) {
        LOG_WARN("failed to get px size", K(ret));
      } else {
        LOG_TRACE("trace left row count", K(row_count));
        if (row_count < MIN_ROW_COUNT) {
          row_count = MIN_ROW_COUNT;
        }
        // estimated rows: left_row_count * row_size
        input_size = row_count * left_->get_spec().width_;
      }
    } else {
      if (OB_ISNULL(left_batch_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("left op is null", K(ret));
      } else {
        // use actual value
        // it need to be considered swapping left and right
        input_size = left_batch_->get_size_on_disk();
        row_count = left_batch_->get_row_count_on_disk();
      }
    }
  } else if (RECURSIVE == hj_processor_) {
    if (nullptr != hj_part_array_ && nullptr != right_hj_part_array_) {
      for (int64_t i = 0; i < part_count_; ++i) {
        row_count += hj_part_array_[i].get_row_count_in_memory();
        input_size += hj_part_array_[i].get_size_in_memory();
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect path for calculate bucket number", K(ret));
    }
  } else if (nullptr != left_batch_) {
    row_count = left_batch_->get_cur_chunk_row_cnt();
    input_size = left_batch_->get_cur_chunk_size();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect path for calculate bucket number");
  }
  if (OB_SUCC(ret)) {
    out_row_count = row_count;
    buckets_number = calc_bucket_number(row_count);
    // save basic info
    profile_.set_basic_info(out_row_count, input_size, buckets_number);
  }
  return ret;
}

int ObHashJoinOp::get_processor_type()
{
  int ret = OB_SUCCESS;
  bool enable_nest_loop = hash_join_processor_ & ENABLE_HJ_NEST_LOOP;
  bool enable_in_memory = hash_join_processor_ & ENABLE_HJ_IN_MEMORY;
  bool enable_recursive = hash_join_processor_ & ENABLE_HJ_RECURSIVE;
  int64_t l_size = 0;
  int64_t r_size = 0;
  int64_t recursive_cost = 0;
  int64_t nest_loop_cost = 0;
  bool is_skew = false;
  int64_t pre_total_size = 0;
  int64_t nest_loop_count = 0;
  if (OB_FAIL(calc_basic_info())) {
    LOG_WARN("failed to get input size", K(ret), K(part_level_));
  } else if (OB_FAIL(get_max_memory_size(profile_.get_input_size()))) {
    LOG_WARN("failed to get max memory size", K(ret), K(remain_data_memory_size_));
  } else if (!top_part_level()) {
    // case 1: in-memory
    if (OB_ISNULL(left_batch_) || OB_ISNULL(right_batch_) || 0 != left_batch_->get_size_in_memory() ||
        0 != right_batch_->get_size_in_memory()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect: partition is null or partition has memory row", K(ret));
    } else if (enable_in_memory && all_in_memory(left_batch_->get_size_on_disk())
        /*|| all_in_memory(right_batch_->get_size_on_disk())*/) {
      // TODO: swap
      // load all data in memory, read all rows from chunk row store to memory
      set_processor(IN_MEMORY);
      l_size = left_batch_->get_size_on_disk();
    } else {
      l_size = left_batch_->get_size_on_disk();
      r_size = right_batch_->get_size_on_disk();
      static const int64_t UNIT_COST = 1;
      static const int64_t READ_COST = UNIT_COST;
      static const int64_t WRITE_COST = 2 * UNIT_COST;
      static const int64_t DUMP_RATIO = 70;
      // 2 read and 1 write for left and right
      recursive_cost = READ_COST * (l_size + r_size) +
                       (READ_COST + WRITE_COST) * (1 - 0.9 * remain_data_memory_size_ / l_size) * (l_size + r_size);
      nest_loop_count = l_size / (remain_data_memory_size_ - 1) + 1;
      nest_loop_cost = (l_size + nest_loop_count * r_size) * READ_COST;
      pre_total_size = left_batch_->get_pre_total_size();
      is_skew = (pre_total_size * DUMP_RATIO / 100 < l_size) ||
                (3 <= part_level_ && max_partition_count_per_level_ != left_batch_->get_pre_part_count() &&
                    pre_total_size * 30 / 100 < l_size);
      if (enable_recursive && recursive_cost < nest_loop_cost && !is_skew && MAX_PART_LEVEL > part_level_) {
        // case 2: recursive process
        set_processor(RECURSIVE);
      } else if (enable_nest_loop) {
        // case 3: nest loop process
        if (!need_right_bitset() || MAX_NEST_LOOP_RIGHT_ROW_COUNT >= right_batch_->get_row_count_on_disk()) {
          set_processor(NEST_LOOP);
        } else {
          set_processor(RECURSIVE);
        }
      }
    }
    // if only one processor, then must choose it, for test
    if (enable_nest_loop && !enable_in_memory && !enable_recursive) {
      set_processor(NEST_LOOP);
    } else if (!enable_nest_loop && enable_in_memory && !enable_recursive) {
      // force remain more memory
      int64_t hash_area_size = 0;
      if (OB_FAIL(ObSqlWorkareaUtil::get_workarea_size(
              ObSqlWorkAreaType::HASH_WORK_AREA, ctx_.get_my_session()->get_effective_tenant_id(), hash_area_size))) {
        LOG_WARN("failed to get workarea size", K(ret));
      }
      remain_data_memory_size_ = hash_area_size * 10;
      buf_mgr_->set_reserve_memory_size(remain_data_memory_size_);
      set_processor(IN_MEMORY);
    } else if (!enable_nest_loop && !enable_in_memory && enable_recursive && MAX_PART_LEVEL > part_level_) {
      set_processor(RECURSIVE);
    } else if (NONE == hj_processor_) {
      if (MAX_PART_LEVEL >= part_level_) {
        set_processor(NEST_LOOP);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no processor",
            K(part_level_),
            K(hj_processor_),
            K(part_level_),
            K(part_count_),
            K(hash_table_.nbuckets_),
            K(pre_total_size),
            K(recursive_cost),
            K(nest_loop_cost),
            K(is_skew),
            K(l_size),
            K(r_size));
      }
    }
  } else {
    set_processor(RECURSIVE);
  }
  LOG_TRACE("hash join process type",
      K(part_level_),
      K(hj_processor_),
      K(part_level_),
      K(part_count_),
      K(hash_table_.nbuckets_),
      K(pre_total_size),
      K(recursive_cost),
      K(nest_loop_cost),
      K(is_skew),
      K(l_size),
      K(r_size),
      K(remain_data_memory_size_),
      K(profile_.get_expect_size()),
      K(profile_.get_bucket_size()),
      K(profile_.get_cache_size()),
      K(profile_.get_row_count()),
      K(profile_.get_input_size()),
      K(left_->get_spec().width_));
  return ret;
}

int ObHashJoinOp::build_hash_table_in_memory(int64_t& num_left_rows)
{
  int ret = OB_SUCCESS;
  uint64_t hash_value = 0;
  num_left_rows = 0;
  ObHashJoinStoredJoinRow* stored_row = nullptr;
  ObHashJoinBatch* hj_batch = left_batch_;
  if (OB_FAIL(left_batch_->set_iterator(true))) {
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
        K(buf_mgr_->get_reserve_memory_size()));
  } else if (OB_FAIL(prepare_hash_table())) {
    LOG_WARN("failed to prepare hash table", K(ret));
  } else {
    HashTableCell* tuple = NULL;
    PartHashJoinTable& hash_table = hash_table_;
    int64_t cell_index = 0;
    int64_t bucket_id = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(get_next_left_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next left row failed", K(ret));
        } else if (hj_batch->get_row_count_on_disk() != num_left_rows) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expect row count is match", K(hj_batch->get_row_count_on_disk()), K(num_left_rows));
        }
      } else if (num_left_rows >= hash_table.row_count_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row count exceed total row count", K(ret), K(num_left_rows), K(hash_table.row_count_));
      } else {
        stored_row = const_cast<ObHashJoinStoredJoinRow*>(left_read_row_);
        hash_value = stored_row->get_hash_value();
        if (enable_bloom_filter_ && OB_FAIL(bloom_filter_->set(hash_value))) {
          LOG_WARN("add hash value to bloom failed", K(ret));
        }
        bucket_id = get_bucket_idx(hash_value);
        tuple = &(hash_table.all_cells_->at(cell_index));
        tuple->stored_row_ = stored_row;
        tuple->next_tuple_ = hash_table.buckets_->at(bucket_id);
        hash_table.buckets_->at(bucket_id) = tuple;
        hash_table.inc_collision(bucket_id);
        ++cell_index;
        ++num_left_rows;
        LOG_DEBUG("trace hash key", K(hash_value), K(ROWEXPR2STR(eval_ctx_, left_join_keys_)));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      trace_hash_table_collision(num_left_rows);
    }
  }
  is_last_chunk_ = true;
  LOG_TRACE("trace to finish build hash table in memory",
      K(ret),
      K(num_left_rows),
      K(hj_batch->get_row_count_on_disk()),
      K(hash_table_.nbuckets_));
  return ret;
}

int ObHashJoinOp::in_memory_process(bool& need_not_read_right)
{
  int ret = OB_SUCCESS;
  need_not_read_right = false;
  int64_t num_left_rows = 0;
  postprocessed_left_ = true;
  if (OB_FAIL(build_hash_table_in_memory(num_left_rows))) {
    LOG_WARN("failed to build hash table", K(ret), K(part_level_));
  }
  if (OB_SUCC(ret) && 0 == num_left_rows && RIGHT_ANTI_JOIN != MY_SPEC.join_type_ &&
      RIGHT_OUTER_JOIN != MY_SPEC.join_type_ && FULL_OUTER_JOIN != MY_SPEC.join_type_) {
    need_not_read_right = true;
    LOG_DEBUG("[HASH JOIN]Left table is empty, skip reading right table.", K(num_left_rows), K(MY_SPEC.join_type_));
  }
  return ret;
}

int ObHashJoinOp::init_join_partition()
{
  int ret = OB_SUCCESS;
  if (0 >= part_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition count is less then 0", K(part_count_));
  } else {
    int64_t used = sizeof(ObHashJoinPartition) * part_count_;
    void* buf = alloc_->alloc(used);
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf for hj part");
    } else {
      hj_part_array_ = new (buf) ObHashJoinPartition[part_count_];
      for (int64_t i = 0; i < part_count_ && OB_SUCC(ret); i++) {
        if (OB_FAIL(hj_part_array_[i].init(part_level_,
                static_cast<int32_t>(i),
                true,
                buf_mgr_,
                batch_mgr_,
                left_batch_,
                left_,
                &sql_mem_processor_,
                sql_mem_processor_.get_dir_id()))) {
          LOG_WARN("failed to init partition", K(part_level_));
        }
      }
    }

    if (OB_SUCCESS == ret) {
      int64_t used = sizeof(ObHashJoinPartition) * part_count_;
      void* buf = alloc_->alloc(used);
      if (NULL == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc buf for hj part");
      } else {
        right_hj_part_array_ = new (buf) ObHashJoinPartition[part_count_];
        for (int64_t i = 0; i < part_count_ && OB_SUCC(ret); i++) {
          if (OB_FAIL(right_hj_part_array_[i].init(part_level_,
                  static_cast<int32_t>(i),
                  false,
                  buf_mgr_,
                  batch_mgr_,
                  right_batch_,
                  right_,
                  &sql_mem_processor_,
                  sql_mem_processor_.get_dir_id()))) {
            LOG_WARN("failed to init partition");
          }
        }
      }
    }
  }
  if (nullptr != left_batch_) {
    LOG_TRACE("trace init partition",
        K(part_count_),
        K(part_level_),
        K(left_batch_->get_part_level()),
        K(left_batch_->get_batchno()),
        K(buf_mgr_->get_page_size()));
  } else {
    LOG_TRACE("trace init partition", K(part_count_), K(part_level_), K(buf_mgr_->get_page_size()));
  }
  return ret;
}

int ObHashJoinOp::force_dump(bool for_left)
{
  return finish_dump(for_left, true, true);
}

void ObHashJoinOp::update_remain_data_memory_size(int64_t row_count, int64_t total_mem_size, bool& need_dump)
{
  need_dump = false;
  double ratio = 1.0;
  need_dump = need_more_remain_data_memory_size(row_count, total_mem_size, ratio);
  int64_t estimate_remain_size = total_mem_size * ratio;
  remain_data_memory_size_ = estimate_remain_size;
  buf_mgr_->set_reserve_memory_size(remain_data_memory_size_);
  LOG_TRACE("trace need more remain memory size",
      K(total_mem_size),
      K(row_count),
      K(buf_mgr_->get_total_alloc_size()),
      K(estimate_remain_size),
      K(predict_row_cnt_));
}

bool ObHashJoinOp::need_more_remain_data_memory_size(int64_t row_count, int64_t total_mem_size, double& data_ratio)
{
  int64_t bucket_cnt = calc_bucket_number(row_count);
  int64_t extra_memory_size = bucket_cnt * (sizeof(HashTableCell*) + sizeof(uint8_t));
  extra_memory_size += (row_count * sizeof(HashTableCell));
  int64_t predict_total_memory_size = extra_memory_size + get_data_mem_used();
  bool need_more = (total_mem_size < predict_total_memory_size);
  double guess_data_ratio = 0;
  if (0 < get_mem_used()) {
    guess_data_ratio = 1.0 * get_data_mem_used() / predict_total_memory_size;
  }
  data_ratio = MAX(guess_data_ratio, 0.5);
  LOG_TRACE("trace need more remain memory size",
      K(total_mem_size),
      K(predict_total_memory_size),
      K(extra_memory_size),
      K(bucket_cnt),
      K(row_count),
      K(buf_mgr_->get_total_alloc_size()),
      K(predict_row_cnt_),
      K(data_ratio),
      K(get_mem_used()),
      K(guess_data_ratio),
      K(get_data_mem_used()));
  return need_more;
}

int ObHashJoinOp::update_remain_data_memory_size_periodically(int64_t row_count, bool& need_dump)
{
  int ret = OB_SUCCESS;
  bool updated = false;
  need_dump = false;
  if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
          alloc_, [&](int64_t cur_cnt) { return row_count > cur_cnt; }, updated))) {
    LOG_WARN("failed to update max usable memory size periodically", K(ret), K(row_count));
  } else if (updated) {
    update_remain_data_memory_size(row_count, sql_mem_processor_.get_mem_bound(), need_dump);
    predict_row_cnt_ <<= 1;
    LOG_TRACE("trace need more remain memory size",
        K(profile_.get_expect_size()),
        K(row_count),
        K(buf_mgr_->get_total_alloc_size()),
        K(predict_row_cnt_));
  }
  return ret;
}

int ObHashJoinOp::dump_build_table(int64_t row_count)
{
  int ret = OB_SUCCESS;
  bool need_dump = false;
  if (OB_FAIL(update_remain_data_memory_size_periodically(row_count, need_dump))) {
    LOG_WARN("failed to update remain memory size periodically", K(ret));
  } else if (OB_LIKELY(this->need_dump() || need_dump)) {
    // judge whether reach max memory bound size
    // every time expend 20%
    if (max_partition_count_per_level_ != cur_dumped_partition_) {
      // it has dumped already
      need_dump = true;
    } else if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(
                   alloc_,
                   [&](int64_t max_memory_size) {
                     UNUSED(max_memory_size);
                     update_remain_data_memory_size(row_count, sql_mem_processor_.get_mem_bound(), need_dump);
                     return need_dump;
                   },
                   need_dump,
                   get_mem_used()))) {
      LOG_WARN("failed to extend max memory size", K(ret));
    }
    if (need_dump) {
      LOG_TRACE("need dump",
          K(buf_mgr_->get_reserve_memory_size()),
          K(buf_mgr_->get_total_alloc_size()),
          K(profile_.get_expect_size()));
      // dump from last partition to the first partition
      int64_t cur_dumped_partition = part_count_ - 1;
      while ((this->need_dump() || all_dumped()) && 0 <= cur_dumped_partition && OB_SUCC(ret)) {
        ObHashJoinPartition& left_part = hj_part_array_[cur_dumped_partition];
        if (0 < left_part.get_size_in_memory()) {
          if (OB_FAIL(left_part.dump(false))) {
            LOG_WARN("failed to dump partition", K(part_level_), K(cur_dumped_partition));
          } else if (left_part.is_dumped()) {
            left_part.get_batch()->set_memory_limit(1);
            sql_mem_processor_.set_number_pass(part_level_ + 1);
          }
        }
        --cur_dumped_partition;
        if (cur_dumped_partition < cur_dumped_partition_) {
          cur_dumped_partition_ = cur_dumped_partition;
        }
      }
      LOG_TRACE("trace left need dump",
          K(part_level_),
          K(buf_mgr_->get_reserve_memory_size()),
          K(buf_mgr_->get_total_alloc_size()),
          K(cur_dumped_partition),
          K(buf_mgr_->get_dumped_size()),
          K(cur_dumped_partition_));
    }
  }
  return ret;
}

// dump partition that has only little data
int ObHashJoinOp::dump_remain_partition()
{
  int ret = OB_SUCCESS;
  // dump last batch rows and only remain all in-memory data
  if (max_partition_count_per_level_ > cur_dumped_partition_) {
    int64_t dumped_size = 0;
    int64_t in_memory_size = 0;
    int64_t total_dumped_size = 0;
    for (int64_t i = part_count_ - 1; i >= 0 && OB_SUCC(ret); --i) {
      ObHashJoinPartition& left_part = hj_part_array_[i];
      if (i > cur_dumped_partition_) {
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
        K(cur_dumped_partition_),
        K(remain_data_memory_size_),
        K(part_level_),
        K(buf_mgr_->get_dumped_size()));
  }
  return ret;
}

int ObHashJoinOp::split_partition(int64_t& num_left_rows)
{
  int ret = OB_SUCCESS;
  uint64_t hash_value = 0;
  ObHashJoinStoredJoinRow* stored_row = nullptr;
  int64_t row_count_on_disk = 0;
  if (nullptr != left_batch_) {
    // read all data, use default iterator
    if (OB_FAIL(left_batch_->set_iterator(false))) {
      LOG_WARN("failed to init iterator", K(ret));
    } else {
      row_count_on_disk = left_batch_->get_row_count_on_disk();
    }
  }
  num_left_rows = 0;
  while (OB_SUCC(ret)) {
    clear_evaluated_flag();
    if (OB_FAIL(get_next_left_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next left row failed", K(ret));
      }
    } else {
      if (NULL == left_read_row_) {
        if (OB_FAIL(calc_hash_value(left_join_keys_, left_hash_funcs_, hash_value))) {
          LOG_WARN("get left row hash_value failed", K(ret));
        }
      } else {
        hash_value = left_read_row_->get_hash_value();
      }
    }
    if (OB_SUCC(ret)) {
      ++num_left_rows;
      const int64_t part_idx = get_part_idx(hash_value);
      if (OB_FAIL(hj_part_array_[part_idx].add_row(left_->get_spec().output_, &eval_ctx_, stored_row))) {
        // if oom, then dump and add row again
        if (OB_ALLOCATE_MEMORY_FAILED == ret) {
          if (GCONF.is_sql_operator_dump_enabled()) {
            ret = OB_SUCCESS;
            if (OB_FAIL(force_dump(true))) {
              LOG_WARN("fail to dump", K(ret));
            } else if (OB_FAIL(hj_part_array_[part_idx].add_row(left_->get_spec().output_, &eval_ctx_, stored_row))) {
              LOG_WARN("add row to row store failed", K(ret));
            }
          } else {
            LOG_WARN("add row to row store failed", K(ret));
          }
        } else {
          LOG_WARN("add row to row store failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        stored_row->set_is_match(false);
        stored_row->set_hash_value(hash_value);
        if (GCONF.is_sql_operator_dump_enabled()) {
          if (OB_FAIL(dump_build_table(num_left_rows))) {
            LOG_WARN("fail to dump", K(ret));
          }
        }
      }
    }
  }
  // overwrite OB_ITER_END error code
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    if (nullptr != left_batch_) {
      left_batch_->rescan();
    }
    if (sql_mem_processor_.is_auto_mgr()) {
      // last stage for dump build table
      if (OB_FAIL(calc_basic_info())) {
        LOG_WARN("failed to calc basic info", K(ret));
      } else if (OB_FAIL(dump_build_table(profile_.get_row_count()))) {
        LOG_WARN("fail to dump", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (top_part_level() && force_hash_join_spill_) {
      // force partition dump
      if (OB_FAIL(force_dump(true))) {
        LOG_WARN("fail to finish dump", K(ret));
      } else {
        // for all partition to dump
        cur_dumped_partition_ = -1;
        for (int64_t i = 0; OB_SUCC(ret) && i < part_count_; ++i) {
          int64_t row_count_in_memory = hj_part_array_[i].get_row_count_in_memory();
          if (0 != row_count_in_memory) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect no data in memory", K(ret), K(row_count_in_memory), K(i));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(dump_remain_partition())) {
      LOG_WARN("failed to dump remain partition");
    } else if (OB_FAIL(finish_dump(true, false))) {
      LOG_WARN("fail to finish dump", K(ret));
    } else if (nullptr != left_batch_ && num_left_rows != row_count_on_disk) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect read all data", K(ret), K(num_left_rows), K(row_count_on_disk));
    }
  }
  LOG_TRACE("trace split partition", K(ret), K(num_left_rows), K(row_count_on_disk));
  return ret;
}

void ObHashJoinOp::free_bloom_filter()
{
  if (nullptr != bloom_filter_) {
    bloom_filter_->~ObGbyBloomFilter();
    mem_context_->get_malloc_allocator().free(bloom_filter_);
    bloom_filter_alloc_->reset();
    bloom_filter_alloc_->~ModulePageAllocator();
    mem_context_->get_malloc_allocator().free(bloom_filter_alloc_);
  }
  bloom_filter_alloc_ = nullptr;
  bloom_filter_ = NULL;
}

int ObHashJoinOp::init_bloom_filter(ObIAllocator& alloc, int64_t bucket_cnt)
{
  int ret = OB_SUCCESS;
  if (!enable_bloom_filter_) {
  } else if (nullptr == bloom_filter_) {
    void* alloc_buf = alloc.alloc(sizeof(ModulePageAllocator));
    void* mem = alloc.alloc(sizeof(ObGbyBloomFilter));
    if (OB_ISNULL(alloc_buf) || OB_ISNULL(mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      if (OB_NOT_NULL(alloc_buf)) {
        alloc.free(alloc_buf);
      }
      if (OB_NOT_NULL(mem)) {
        alloc.free(mem);
      }
    } else {
      bloom_filter_alloc_ = new (alloc_buf) ModulePageAllocator(alloc);
      bloom_filter_alloc_->set_label("HashBloomFilter");
      bloom_filter_ = new (mem) ObGbyBloomFilter(*bloom_filter_alloc_);
      if (OB_FAIL(bloom_filter_->init(bucket_cnt, 2))) {
        LOG_WARN("bloom filter init failed", K(ret));
      }
    }
  } else {
    bloom_filter_->reuse();
    if (OB_FAIL(bloom_filter_->init(bucket_cnt, 2))) {
      LOG_WARN("bloom filter init failed", K(ret));
    }
  }
  return ret;
}

void ObHashJoinOp::calc_cache_aware_partition_count()
{
  // 48: 16([key, value]), 16([key, value]), 16(histogram)
  int64_t price_per_row = price_per_row_;
  int64_t row_count_cache_aware = l2_cache_size_ / price_per_row;
  int64_t total_row_count = 0;
  int64_t cur_partition_in_memory =
      max_partition_count_per_level_ == cur_dumped_partition_ ? part_count_ : cur_dumped_partition_ + 1;
  for (int64_t i = 0; i < cur_partition_in_memory; ++i) {
    ObHashJoinPartition& hj_part = hj_part_array_[i];
    // maybe empty partition
    total_row_count += hj_part.get_row_count_in_memory();
  }
  int64_t total_partition_cnt = next_pow2(total_row_count / row_count_cache_aware);
  total_partition_cnt = total_partition_cnt < MIN_PART_COUNT ? MIN_PART_COUNT : total_partition_cnt;
  int64_t tmp_partition_cnt_per_level = max_partition_count_per_level_;
  if (total_partition_cnt > tmp_partition_cnt_per_level) {
    level1_part_count_ = part_count_;
    level1_bit_ = __builtin_ctz(level1_part_count_);
    level2_part_count_ = total_partition_cnt / level1_part_count_;
    level2_part_count_ =
        level2_part_count_ > tmp_partition_cnt_per_level ? tmp_partition_cnt_per_level : level2_part_count_;
  } else {
    level1_part_count_ = total_partition_cnt > part_count_ ? total_partition_cnt : part_count_;
    level1_bit_ = __builtin_ctz(level1_part_count_);
  }
  LOG_TRACE("partition count",
      K(total_partition_cnt),
      K(tmp_partition_cnt_per_level),
      K(part_count_),
      K(level1_part_count_),
      K(level1_bit_),
      K(level2_part_count_),
      K(total_row_count),
      K(row_count_cache_aware));
}

// TEST TPCH 1TB, Q09 improve 5s-6s, about 25%~30%
// When partition count is greater or equan than 128, then enable cache aware hash join
bool ObHashJoinOp::can_use_cache_aware_opt()
{
  int ret = OB_SUCCESS;
  int64_t total_memory_size = 0;
  bool dumped = false;
  bool enable_cache_aware = false;
  // 48: 16([key, value]), 16([key, value]), 16(histogram)
  int64_t price_per_row = price_per_row_;
  int64_t row_count_cache_aware = l2_cache_size_ / price_per_row;
  int64_t total_row_count = 0;
  for (int64_t i = 0; i < part_count_; ++i) {
    ObHashJoinPartition& hj_part = hj_part_array_[i];
    // maybe empty partition
    total_memory_size +=
        sizeof(HashJoinHistogram) + HashJoinHistogram::calc_memory_size(hj_part.get_row_count_in_memory());
    if (!dumped) {
      dumped = hj_part.get_row_count_on_disk() > 0;
    }
    if (!dumped) {
      total_row_count += hj_part.get_row_count_in_memory();
    }
  }
  // cache aware use to switch buffer for chunk datum streo, so need 2 buffer
  total_memory_size += get_mem_used() + 2 * part_count_ * PAGE_SIZE;
  enable_cache_aware = total_memory_size < sql_mem_processor_.get_mem_bound();
  if (!enable_cache_aware) {
    bool need_dump = true;
    int64_t pre_cache_size = profile_.get_cache_size();
    if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(
            alloc_,
            [&](int64_t max_memory_size) { return total_memory_size > max_memory_size; },
            need_dump,
            get_mem_used()))) {
      LOG_WARN("failed to extend max memory size", K(ret));
    } else {
      enable_cache_aware = !need_dump;
      if (!enable_cache_aware) {
        if (OB_FAIL(sql_mem_processor_.update_cache_size(alloc_, pre_cache_size))) {
          LOG_WARN("failed to upadte cache size", K(ret), K(pre_cache_size));
        }
      }
    }
  }
  calc_cache_aware_partition_count();
  int64_t total_partition_cnt = 0 < level2_part_count_ ? level1_part_count_ * level2_part_count_ : level1_part_count_;
  bool force_enable = false;
  if (OB_FAIL(ret)) {
  } else {
    ret = E(EventTable::EN_ENABLE_HASH_JOIN_CACHE_AWARE) ret;
    if (OB_FAIL(ret)) {
      ret = -ret;
      enable_cache_aware = false;
      force_enable = (0 == ret % 2) ? true : false;
      LOG_INFO("trace enable cache aware",
          K(total_partition_cnt),
          K(enable_cache_aware),
          K(force_enable),
          K(MY_SPEC.id_),
          K(ret));
    }
    ret = OB_SUCCESS;
  }
  enable_cache_aware = ((enable_cache_aware && total_partition_cnt >= CACHE_AWARE_PART_CNT) || force_enable) &&
                       INNER_JOIN == MY_SPEC.join_type_;
  LOG_TRACE("trace check cache aware opt",
      K(total_memory_size),
      K(total_row_count),
      K(row_count_cache_aware),
      K(sql_mem_processor_.get_mem_bound()),
      K(part_count_),
      K(cur_dumped_partition_));
  if (!enable_cache_aware) {
    level1_part_count_ = 0;
    level2_part_count_ = 0;
    level1_bit_ = 0;
  }
  opt_cache_aware_ = enable_cache_aware;
  return enable_cache_aware;
}

int ObHashJoinOp::prepare_hash_table()
{
  int ret = OB_SUCCESS;
  PartHashJoinTable& hash_table = hash_table_;
  // first time, the bucket number is already calculated
  // calculate the buckets number of hash table
  if (OB_FAIL(calc_basic_info())) {
    LOG_WARN("failed to calc basic info", K(ret));
  } else {
    hash_table.nbuckets_ = profile_.get_bucket_size();
    hash_table.row_count_ = profile_.get_row_count();
  }
  int64_t buckets_mem_size = 0;
  int64_t collision_cnts_mem_size = 0;
  int64_t all_cells_mem_size = 0;
  hash_table.buckets_->reuse();
  hash_table.collision_cnts_->reuse();
  hash_table.all_cells_->reuse();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_bloom_filter(mem_context_->get_malloc_allocator(), hash_table_.nbuckets_))) {
    LOG_WARN("failed to create bloom filter", K(ret));
  } else if (OB_FAIL(hash_table.buckets_->init(hash_table.nbuckets_))) {
    LOG_WARN("alloc bucket array failed", K(ret), K(hash_table.nbuckets_));
  } else if (OB_FAIL(hash_table.collision_cnts_->init(hash_table.nbuckets_))) {
    LOG_WARN("alloc collision array failed", K(ret), K(hash_table.nbuckets_));
  } else if (0 < hash_table.row_count_ && OB_FAIL(hash_table.all_cells_->init(hash_table.row_count_))) {
    LOG_WARN("alloc hash cell failed", K(ret), K(hash_table.row_count_));
  } else {
    if (NEST_LOOP == hj_processor_ && OB_NOT_NULL(right_batch_)) {
      has_right_bitset_ = need_right_bitset();
      if (has_right_bitset_ && OB_FAIL(right_bit_set_.reserve(right_batch_->get_row_count_on_disk()))) {
        // revert to recursive process
        // only for TEST_NEST_LOOP_TO_RECURSIVE
        nest_loop_state_ = HJLoopState::LOOP_RECURSIVE;
        LOG_WARN("failed to reserve right bitset",
            K(ret),
            K(hash_table.nbuckets_),
            K(right_batch_->get_row_count_on_disk()));
      }
    }
    LOG_TRACE("trace prepare hash table",
        K(ret),
        K(hash_table.nbuckets_),
        K(hash_table.row_count_),
        K(part_count_),
        K(buf_mgr_->get_reserve_memory_size()),
        K(total_extra_size_),
        K(buf_mgr_->get_total_alloc_size()),
        K(profile_.get_expect_size()));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("trace failed to  prepare hash table",
        K(buf_mgr_->get_total_alloc_size()),
        K(profile_.get_expect_size()),
        K(buf_mgr_->get_reserve_memory_size()),
        K(hash_table.nbuckets_),
        K(hash_table.row_count_),
        K(get_mem_used()),
        K(sql_mem_processor_.get_mem_bound()));
  } else {
    if (OB_FAIL(sql_mem_processor_.update_used_mem_size(get_mem_used()))) {
      LOG_WARN("failed to update used mem size", K(ret));
    }
  }
  LOG_TRACE("trace prepare hash table",
      K(ret),
      K(hash_table.nbuckets_),
      K(hash_table.row_count_),
      K(buckets_mem_size),
      K(part_count_),
      K(buf_mgr_->get_reserve_memory_size()),
      K(total_extra_size_),
      K(buf_mgr_->get_total_alloc_size()),
      K(profile_.get_expect_size()),
      K(collision_cnts_mem_size),
      K(all_cells_mem_size));
  return ret;
}

void ObHashJoinOp::trace_hash_table_collision(int64_t row_cnt)
{
  int64_t total_cnt = 0;
  uint8_t min_cnt = 0;
  uint8_t max_cnt = 0;
  int64_t used_bucket_cnt = 0;
  int64_t nbuckets = hash_table_.nbuckets_;
  hash_table_.get_collision_info(min_cnt, max_cnt, total_cnt, used_bucket_cnt);
  LOG_TRACE("trace hash table collision",
      K(spec_.get_id()),
      K(spec_.get_name()),
      K(nbuckets),
      "avg_cnt",
      ((double)total_cnt / (double)used_bucket_cnt),
      K(min_cnt),
      K(max_cnt),
      K(total_cnt),
      K(row_cnt),
      K(used_bucket_cnt));
  op_monitor_info_.otherstat_1_value_ = min_cnt;
  op_monitor_info_.otherstat_2_value_ = max_cnt;
  op_monitor_info_.otherstat_3_value_ = total_cnt;
  op_monitor_info_.otherstat_4_value_ = nbuckets;
  op_monitor_info_.otherstat_5_value_ = used_bucket_cnt;
  op_monitor_info_.otherstat_6_value_ = row_cnt;
  op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::HASH_SLOT_MIN_COUNT;
  ;
  op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::HASH_SLOT_MAX_COUNT;
  op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::HASH_SLOT_TOTAL_COUNT;
  op_monitor_info_.otherstat_4_id_ = ObSqlMonitorStatIds::HASH_BUCKET_COUNT;
  op_monitor_info_.otherstat_5_id_ = ObSqlMonitorStatIds::HASH_NON_EMPTY_BUCKET_COUNT;
  op_monitor_info_.otherstat_6_id_ = ObSqlMonitorStatIds::HASH_ROW_COUNT;
}

int ObHashJoinOp::build_hash_table_for_recursive()
{
  int ret = OB_SUCCESS;
  uint64_t hash_value = 0;
  const ObHashJoinStoredJoinRow* stored_row = nullptr;
  HashTableCell* tuple = NULL;
  int64_t bucket_id = 0;
  int64_t total_row_count = 0;
  PartHashJoinTable& hash_table = hash_table_;
  int64_t cell_index = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_count_; ++i) {
    ObHashJoinPartition& hj_part = hj_part_array_[i];
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
            bucket_id = get_bucket_idx(hash_value);
            if (enable_bloom_filter_ && OB_FAIL(bloom_filter_->set(hash_value))) {
              LOG_WARN("add hash value to bloom failed", K(ret));
            }
            tuple = &(hash_table.all_cells_->at(cell_index));
            tuple->stored_row_ = const_cast<ObHashJoinStoredJoinRow*>(stored_row);
            tuple->next_tuple_ = hash_table.buckets_->at(bucket_id);
            hash_table.buckets_->at(bucket_id) = tuple;

            // HashTableCell *first_tuple = hash_table.buckets_->at(bucket_id);
            // if (nullptr == first_tuple) {
            //   tuple->next_tuple_ = nullptr;
            //   hash_table.buckets_->at(bucket_id) = tuple;
            // } else {
            //   while (nullptr != first_tuple->next_tuple_) {
            //     first_tuple = first_tuple->next_tuple_;
            //   }
            //   first_tuple->next_tuple_ = tuple;
            //   tuple->next_tuple_ = nullptr;
            // }
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
  is_last_chunk_ = true;
  if (OB_SUCC(ret)) {
    trace_hash_table_collision(total_row_count);
  }
  LOG_TRACE("trace to finish build hash table for recursive",
      K(part_count_),
      K(part_level_),
      K(total_row_count),
      K(hash_table_.nbuckets_));
  return ret;
}

int ObHashJoinOp::HashJoinHistogram::init(
    ObIAllocator* alloc, int64_t row_count, int64_t bucket_cnt, bool enable_bloom_filter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hist_alloc_) && 0 < row_count) {
    void* hist_alloc = alloc->alloc(sizeof(ModulePageAllocator));
    void* h1 = alloc->alloc(sizeof(HistItemArray));
    void* h2 = alloc->alloc(sizeof(HistItemArray));
    void* prefix_hist_count = alloc->alloc(sizeof(HistPrefixArray));
    void* prefix_hist_count2 = alloc->alloc(sizeof(HistPrefixArray));
    void* bf = alloc->alloc(sizeof(ObGbyBloomFilter));
    alloc_ = alloc;
    if (OB_ISNULL(hist_alloc) || OB_ISNULL(h1) || OB_ISNULL(h2) || OB_ISNULL(prefix_hist_count) ||
        OB_ISNULL(prefix_hist_count2) || OB_ISNULL(bf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      if (OB_NOT_NULL(hist_alloc)) {
        alloc->free(hist_alloc);
      }
      if (OB_NOT_NULL(h1)) {
        alloc->free(h1);
      }
      if (OB_NOT_NULL(h2)) {
        alloc->free(h2);
      }
      if (OB_NOT_NULL(prefix_hist_count)) {
        alloc->free(prefix_hist_count);
      }
      if (OB_NOT_NULL(prefix_hist_count2)) {
        alloc->free(prefix_hist_count2);
      }
      if (OB_NOT_NULL(bf)) {
        alloc->free(bf);
      }
      LOG_WARN("failed to alloc memory", K(ret));
    } else {
      enable_bloom_filter_ = enable_bloom_filter;
      row_count_ = row_count;
      bucket_cnt_ = bucket_cnt;
      hist_alloc_ = new (hist_alloc) ModulePageAllocator(*alloc);
      hist_alloc_->set_label("HistOpAllocTest");
      h1_ = new (h1) HistItemArray(*hist_alloc_);
      h2_ = new (h2) HistItemArray(*hist_alloc_);
      prefix_hist_count_ = new (prefix_hist_count) HistPrefixArray(*hist_alloc_);
      prefix_hist_count2_ = new (prefix_hist_count2) HistPrefixArray(*hist_alloc_);
      bloom_filter_ = new (bf) ObGbyBloomFilter(*hist_alloc_);
      if (OB_FAIL(h1_->init(row_count))) {
        LOG_WARN("failed to init histogram5", K(ret));
      } else if (OB_FAIL(h2_->init(row_count))) {
        LOG_WARN("failed to init histogram", K(ret));
      } else if (OB_FAIL(prefix_hist_count_->init(bucket_cnt_))) {
        LOG_WARN("failed to init prefix hist counts", K(ret));
      } else if (enable_bloom_filter && OB_FAIL(bloom_filter_->init(bucket_cnt_, 2))) {
        LOG_WARN("bloom filter init failed", K(ret));
      } else {
        LOG_TRACE("trace set histogram bucket", K(row_count_), K(bucket_cnt_));
      }
    }
  } else {
    LOG_DEBUG("debug set histogram bucket", K(row_count), KP(hist_alloc_));
  }
  return ret;
}

int ObHashJoinOp::HashJoinHistogram::calc_prefix_histogram()
{
  int ret = OB_SUCCESS;
  int32_t prefix = 0;
  if (OB_ISNULL(prefix_hist_count_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("prefix hist count is null", K(ret));
  } else {
    for (int64_t i = 0; i < prefix_hist_count_->count(); ++i) {
      int32_t tmp = prefix_hist_count_->at(i);
      if (0 == i) {
        prefix_hist_count_->at(i) = 0;
      } else {
        prefix_hist_count_->at(i) = prefix;
      }
      prefix += tmp;
    }
    LOG_DEBUG("trace calc prefix histogram", K(row_count_), K(bucket_cnt_));
  }
  return ret;
}

int ObHashJoinOp::HashJoinHistogram::reorder_histogram(BucketFunc bucket_func)
{
  int ret = OB_SUCCESS;
  // calc prefix histogram
  if (OB_ISNULL(h1_) || OB_ISNULL(h2_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("histogram is null", K(ret), K(h1_), K(h2_));
  } else if (OB_FAIL(calc_prefix_histogram())) {
    LOG_WARN("failed to calculate histogram", K(ret));
  } else {
    for (int64_t i = 0; i < h1_->count(); ++i) {
      HistItem& hist_item = h1_->at(i);
      int64_t bucket_id = 0;
      if (OB_ISNULL(bucket_func)) {
        bucket_id = get_bucket_idx(hist_item.hash_value_);
      } else {
        bucket_id = bucket_func(hist_item.hash_value_, i);
      }
      LOG_DEBUG("debug reorder", K(bucket_id), K(hist_item.hash_value_), K(i), K(h1_->count()));
      h2_->at(prefix_hist_count_->at(bucket_id)) = hist_item;
      ++prefix_hist_count_->at(bucket_id);
    }
#ifndef NDEBUG
    for (int64_t i = 0; i < prefix_hist_count_->count(); ++i) {
      LOG_DEBUG("debug prefix histogram", K(i), K(prefix_hist_count_->at(i)));
    }
#endif
    if (enable_bloom_filter_) {
      for (int64_t i = 0; i < h2_->count(); ++i) {
        HistItem& hist_item = h2_->at(i);
        if (OB_FAIL(bloom_filter_->set(hist_item.hash_value_))) {
          LOG_WARN("add hash value to bloom failed", K(ret));
        }
      }
    }
    LOG_DEBUG(
        "trace reorder histogram", K(row_count_), K(bucket_cnt_), K(h1_->count()), K(prefix_hist_count_->count()));
  }
  return ret;
}

void ObHashJoinOp::HashJoinHistogram::switch_histogram()
{
  HistItemArray* tmp_hist = h1_;
  h1_ = h2_;
  h2_ = tmp_hist;
}

void ObHashJoinOp::HashJoinHistogram::switch_prefix_hist_count()
{
  HistPrefixArray* tmp_prefix_hist_count = prefix_hist_count_;
  prefix_hist_count_ = prefix_hist_count2_;
  prefix_hist_count2_ = tmp_prefix_hist_count;
}

int ObHashJoinOp::PartitionSplitter::init(ObIAllocator* alloc, int64_t part_count, ObHashJoinPartition* hj_parts,
    int64_t max_level, int64_t part_shift, int64_t level1_part_count, int64_t level2_part_count)
{
  int ret = OB_SUCCESS;
  alloc_ = alloc;
  part_count_ = part_count;
  hj_parts_ = hj_parts;
  max_level_ = max_level;
  int64_t max_partition_cnt = 0 < level2_part_count ? level2_part_count * level1_part_count : level1_part_count;
  set_part_count(part_shift, level1_part_count, level2_part_count);
  int64_t total_row_count = 0;
  for (int64_t i = 0; i < part_count; ++i) {
    ObHashJoinPartition& hj_part = hj_parts[i];
    // maybe empty partition
    total_row_count += hj_part.get_row_count_in_memory();
  }
  total_row_count_ = total_row_count;
  if (OB_FAIL(part_histogram_.init(alloc, total_row_count, max_partition_cnt, false))) {
    LOG_WARN("failed to init part histogram", K(ret));
  }
  LOG_TRACE("debug split histogram", K(total_row_count), K(max_partition_cnt), K(part_count_));
  return ret;
}

int ObHashJoinOp::PartitionSplitter::repartition_by_part_array(const int64_t part_level)
{
  int ret = OB_SUCCESS;
  HashJoinHistogram::HistItemArray* dst_hist_array = part_histogram_.h1_;
  HashJoinHistogram::HistPrefixArray* dst_prefix_hist_counts = part_histogram_.prefix_hist_count_;
  int64_t partition_count = 1 == part_level ? level_one_part_count_ : level_one_part_count_ * level_two_part_count_;
  dst_prefix_hist_counts->reset();
  if (OB_FAIL(dst_prefix_hist_counts->init(partition_count))) {
    LOG_WARN("failed to init histogram", K(ret));
  }
  auto bucket_func = [&](int64_t hash_value, int64_t nth_part) {
    int64_t bucket_id = 0;
    UNUSED(nth_part);
    if (1 == part_level) {
      bucket_id = get_part_level_one_idx(hash_value);
    } else {
      int64_t level1_part_idx = get_part_level_one_idx(hash_value);
      bucket_id = get_part_level_two_idx(hash_value);
      bucket_id = bucket_id + level1_part_idx * level_two_part_count_;
    }
    return bucket_id;
  };
  int64_t total_nth_row = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_count_; ++i) {
    ObHashJoinPartition& hj_part = hj_parts_[i];
    int64_t row_count_in_memory = hj_part.get_row_count_in_memory();
    const ObHashJoinStoredJoinRow* stored_row = nullptr;
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
          } else {
            HistItem& hist_item = dst_hist_array->at(total_nth_row);
            hist_item.store_row_ = const_cast<ObHashJoinStoredJoinRow*>(stored_row);
            int64_t hash_value = stored_row->get_hash_value();
            hist_item.hash_value_ = hash_value;
            int64_t bucket_id = bucket_func(hash_value, i);
            ++dst_prefix_hist_counts->at(bucket_id);
            ++nth_row;
            ++total_nth_row;
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        if (nth_row != row_count_in_memory) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expect row count is match",
              K(nth_row),
              K(row_count_in_memory),
              K(hj_part.get_row_count_on_disk()),
              K(ret));
        }
      }
    }
    LOG_DEBUG("debug partition build histogram", K(i), K(row_count_in_memory));
  }
  // step2: traverse all histogram to reorder all items
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(part_histogram_.reorder_histogram(bucket_func))) {
    LOG_WARN("failed to reorder histogram", K(ret));
  } else {
    // h2 & prefix_hist_count2_ has origin data
    part_histogram_.switch_prefix_hist_count();
  }
  return ret;
}

int ObHashJoinOp::PartitionSplitter::repartition_by_part_histogram(const int64_t part_level)
{
  int ret = OB_SUCCESS;
  HashJoinHistogram::HistPrefixArray* dst_prefix_hist_counts = part_histogram_.prefix_hist_count_;
  HashJoinHistogram::HistItemArray* org_hist_array = part_histogram_.h2_;
  HashJoinHistogram::HistPrefixArray* org_prefix_hist_counts = part_histogram_.prefix_hist_count2_;
  int64_t partition_count = 1 == part_level ? level_one_part_count_ : level_one_part_count_ * level_two_part_count_;
  dst_prefix_hist_counts->reset();
  if (OB_FAIL(dst_prefix_hist_counts->init(partition_count))) {
    LOG_WARN("failed to init histogram", K(ret));
  }
  auto bucket_func = [&](int64_t hash_value, int64_t nth_part) {
    int64_t bucket_id = 0;
    UNUSED(nth_part);
    if (1 == part_level) {
      bucket_id = get_part_level_one_idx(hash_value);
    } else {
      int64_t level1_part_idx = get_part_level_one_idx(hash_value);
      bucket_id = get_part_level_two_idx(hash_value);
      bucket_id = bucket_id + (level1_part_idx << level2_bit_);
    }
    return bucket_id;
  };
  for (int64_t i = 0; OB_SUCC(ret) && i < org_prefix_hist_counts->count() && OB_SUCC(ret); ++i) {
    int64_t start_idx = 0;
    int64_t end_idx = org_prefix_hist_counts->at(i);
    if (0 != i) {
      start_idx = org_prefix_hist_counts->at(i - 1);
    }
    for (int64_t j = start_idx; j < end_idx; ++j) {
      HistItem& hist_item = org_hist_array->at(j);
      int64_t bucket_id = bucket_func(hist_item.hash_value_, i);
      ++dst_prefix_hist_counts->at(bucket_id);
    }
    LOG_DEBUG("debug partition build histogram", K(i));
  }
  // step2: traverse all histogram to reorder all items
  part_histogram_.switch_histogram();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(part_histogram_.reorder_histogram(bucket_func))) {
    LOG_WARN("failed to reorder histogram", K(ret));
  } else {
    // h2 & prefix_hist_count2_ has origin data
    part_histogram_.switch_prefix_hist_count();
  }
  return ret;
}

int ObHashJoinOp::PartitionSplitter::build_hash_table_by_part_hist(
    HashJoinHistogram* all_part_hists, bool enable_bloom_filter)
{
  int ret = OB_SUCCESS;
  HashJoinHistogram::HistItemArray* org_hist_array = part_histogram_.h2_;
  HashJoinHistogram::HistPrefixArray* org_prefix_hist_counts = part_histogram_.prefix_hist_count2_;
  int64_t total_nth_row = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < org_prefix_hist_counts->count(); ++i) {
    HashJoinHistogram* hist = new (&all_part_hists[i]) HashJoinHistogram();
    int64_t nth_row = 0;
    int64_t start_idx = 0;
    int64_t end_idx = org_prefix_hist_counts->at(i);
    if (0 != i) {
      start_idx = org_prefix_hist_counts->at(i - 1);
    }
    LOG_DEBUG("debug build hash talbe by part histogram",
        K(i),
        K(org_hist_array->count()),
        K(start_idx),
        K(end_idx),
        K(org_prefix_hist_counts->count()));
    if (end_idx > start_idx) {
      int64_t row_count = end_idx - start_idx;
      if (OB_FAIL(hist->init(alloc_, row_count, next_pow2(row_count * RATIO_OF_BUCKETS), enable_bloom_filter))) {
        LOG_WARN("failed to init histogram", K(ret));
      } else {
        for (int64_t j = start_idx; j < end_idx && OB_SUCC(ret); ++j) {
          HistItem& org_hist_item = org_hist_array->at(j);
          HistItem& hist_item = hist->h1_->at(nth_row);
          int64_t hash_value = org_hist_item.hash_value_;
          if (OB_ISNULL(org_hist_item.store_row_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("store row is null", K(ret), K(i), K(j));
          } else {
            hist_item = org_hist_item;
            int64_t bucket_id = hist->get_bucket_idx(hash_value);
            ++hist->prefix_hist_count_->at(bucket_id);
            ++nth_row;
            ++total_nth_row;
          }
        }
      }
      // step2: traverse all histogram to reorder all items
      if (OB_FAIL(ret)) {
      } else if (row_count != nth_row) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row count is not match", K(ret), K(row_count), K(nth_row));
      } else if (OB_FAIL(hist->reorder_histogram(nullptr))) {
        LOG_WARN("failed to reorder histogram", K(ret));
      }
    }
  }
  if (total_nth_row != org_prefix_hist_counts->at(part_histogram_.prefix_hist_count2_->count() - 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row count is not match",
        K(total_nth_row),
        K(org_prefix_hist_counts->at(part_histogram_.prefix_hist_count2_->count() - 1)));
  }
  return ret;
}

int ObHashJoinOp::PartitionSplitter::build_hash_table_by_part_array(
    HashJoinHistogram* all_part_hists, bool enable_bloom_filter)
{
  int ret = OB_SUCCESS;
  // step1: traverse all partition data to generate initial histogram
  for (int64_t i = 0; OB_SUCC(ret) && i < part_count_; ++i) {
    ObHashJoinPartition& hj_part = hj_parts_[i];
    int64_t row_count_in_memory = hj_part.get_row_count_in_memory();
    int64_t nth_row = 0;
    const ObHashJoinStoredJoinRow* stored_row = nullptr;
    HashJoinHistogram* hist = new (&all_part_hists[i]) HashJoinHistogram();
    if (0 < row_count_in_memory) {
      if (OB_FAIL(hist->init(
              alloc_, row_count_in_memory, next_pow2(row_count_in_memory * RATIO_OF_BUCKETS), enable_bloom_filter))) {
        LOG_WARN("failed to init histogram", K(ret));
      } else if (0 < hj_part.get_row_count_on_disk()) {
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
          } else {
            HistItem& hist_item = hist->h1_->at(nth_row);
            hist_item.store_row_ = const_cast<ObHashJoinStoredJoinRow*>(stored_row);
            int64_t hash_value = stored_row->get_hash_value();
            hist_item.hash_value_ = hash_value;
            int64_t bucket_id = hist->get_bucket_idx(hash_value);
            ++hist->prefix_hist_count_->at(bucket_id);
            ++nth_row;
          }
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        if (nth_row != row_count_in_memory) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expect row count is match", K(nth_row), K(row_count_in_memory), K(hj_part.get_row_count_on_disk()));
        }
      }
      LOG_DEBUG("debug partition build histogram", K(i), K(row_count_in_memory));
      // step2: traverse all histogram to reorder all items
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(hist->reorder_histogram(nullptr))) {
        LOG_WARN("failed to reorder histogram", K(ret));
      }
    }
  }
  return ret;
}

int ObHashJoinOp::init_histograms(HashJoinHistogram*& part_histograms, int64_t part_count)
{
  int ret = OB_SUCCESS;
  void* all_part_hists = alloc_->alloc(sizeof(HashJoinHistogram) * part_count);
  if (OB_ISNULL(all_part_hists)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    part_histograms = reinterpret_cast<HashJoinHistogram*>(all_part_hists);
    MEMSET(part_histograms, 0, sizeof(HashJoinHistogram) * part_count);
  }
  return ret;
}

int ObHashJoinOp::repartition(PartitionSplitter& part_splitter, HashJoinHistogram*& part_histograms,
    ObHashJoinPartition* hj_parts, bool is_build_side)
{
  int ret = OB_SUCCESS;
  int64_t part_count = 0 < level2_part_count_ ? level2_part_count_ * level1_part_count_ : level1_part_count_;
  int64_t cur_partition_in_memory =
      max_partition_count_per_level_ == cur_dumped_partition_ ? part_count_ : cur_dumped_partition_ + 1;
  part_histograms = nullptr;
  if (OB_FAIL(part_splitter.init(alloc_,
          cur_partition_in_memory,
          hj_parts,
          0 < level2_part_count_ ? PART_SPLIT_LEVEL_TWO : PART_SPLIT_LEVEL_ONE,
          part_shift_,
          level1_part_count_,
          level2_part_count_))) {
    LOG_WARN("failed to init part splitter", K(ret));
  } else if (is_build_side && OB_FAIL(init_histograms(part_histograms, part_count))) {
    LOG_WARN("failed to initialize histograms", K(ret));
  } else if (0 >= part_splitter.get_total_row_count()) {
    LOG_TRACE("trace empty side", K(is_build_side));
  } else if (0 < level2_part_count_) {
    // level2
    if (part_count_ == level1_part_count_) {
      if (OB_FAIL(part_splitter.repartition_by_part_array(PART_SPLIT_LEVEL_TWO))) {
        LOG_WARN("failed to repartition by part array", K(ret));
      } else if (!is_build_side) {
      } else if (OB_FAIL(part_splitter.build_hash_table_by_part_hist(part_histograms, enable_bloom_filter_))) {
        LOG_WARN("failed to build hash table by part histogram", K(ret));
      } else {
        LOG_TRACE("trace level2 repartition", K(level1_part_count_), K(level2_part_count_));
      }
    } else {
      if (OB_FAIL(part_splitter.repartition_by_part_array(PART_SPLIT_LEVEL_ONE))) {
        LOG_WARN("failed to repartition by part array", K(ret));
      } else if (OB_FAIL(part_splitter.repartition_by_part_histogram(PART_SPLIT_LEVEL_TWO))) {
        LOG_WARN("failed to repartition by part array", K(ret));
      } else if (!is_build_side) {
      } else if (OB_FAIL(part_splitter.build_hash_table_by_part_hist(part_histograms, enable_bloom_filter_))) {
        LOG_WARN("failed to build hash table by part histogram", K(ret));
      } else {
        LOG_TRACE("trace level2 repartition", K(level1_part_count_), K(level2_part_count_));
      }
    }
  } else {
    // level1
    if (part_count_ == level1_part_count_) {
      if (!is_build_side) {
        if (OB_FAIL(part_splitter.repartition_by_part_array(PART_SPLIT_LEVEL_ONE))) {
          LOG_WARN("failed to repartition by part array", K(ret));
        }
      } else if (OB_FAIL(part_splitter.build_hash_table_by_part_array(part_histograms, enable_bloom_filter_))) {
        LOG_WARN("failed to build hash table by part histogram", K(ret));
      } else {
        // one bloom filter per partition
        LOG_TRACE("trace level2 repartition", K(level1_part_count_), K(level2_part_count_));
      }
    } else {
      if (OB_FAIL(part_splitter.repartition_by_part_array(PART_SPLIT_LEVEL_ONE))) {
        LOG_WARN("failed to repartition by part array", K(ret));
      } else if (!is_build_side) {
      } else if (OB_FAIL(part_splitter.build_hash_table_by_part_hist(part_histograms, enable_bloom_filter_))) {
        LOG_WARN("failed to build hash table by part histogram", K(ret));
      } else {
        LOG_TRACE("trace level2 repartition", K(level1_part_count_), K(level2_part_count_));
      }
    }
  }
  return ret;
}

int ObHashJoinOp::partition_and_build_histograms()
{
  int ret = OB_SUCCESS;
  enable_batch_ = 0 == level2_part_count_ && level1_part_count_ == part_count_;
  ret = E(EventTable::EN_SET_DISABLE_HASH_JOIN_BATCH) ret;
  if (OB_FAIL(ret)) {
    ret = -ret;
    enable_batch_ = (0 == ret % 2) ? true : false;
    LOG_INFO(
        "trace enable batch cache aware", K(level2_part_count_), K(level1_part_count_), K(part_count_), K(MY_SPEC.id_));
  }
  ret = OB_SUCCESS;
  if (enable_batch_) {
    PartitionSplitter part_splitter;
    if (OB_FAIL(repartition(part_splitter, part_histograms_, hj_part_array_, true))) {
      LOG_WARN("failed to repartition", K(ret));
    }
  }
  LOG_TRACE("debug partition", K(level1_part_count_), K(level1_bit_), K(level2_part_count_));
  return ret;
}

void ObHashJoinOp::init_system_parameters()
{
  ltb_size_ = INIT_LTB_SIZE;
  l2_cache_size_ = INIT_L2_CACHE_SIZE;
  max_partition_count_per_level_ = ltb_size_ << 1;
  // enable_bloom_filter_ = !MY_SPEC.has_join_bf_;
  enable_bloom_filter_ = true;
}

int ObHashJoinOp::recursive_postprocess()
{
  int ret = OB_SUCCESS;
  if (opt_cache_aware_) {
    LOG_TRACE("trace use cache aware optimization");
    if (OB_FAIL(partition_and_build_histograms())) {
      LOG_WARN("failed to prepare cache aware histogram", K(ret));
    }
  } else {
    if (OB_FAIL(prepare_hash_table())) {
      LOG_WARN("failed to prepare hash table", K(ret));
    } else if (OB_FAIL(build_hash_table_for_recursive())) {
      LOG_WARN("failed to build hash table", K(ret), K(part_level_));
    }
  }
  postprocessed_left_ = true;
  return ret;
}

int ObHashJoinOp::split_partition_and_build_hash_table(int64_t& num_left_rows)
{
  int ret = OB_SUCCESS;
  // load data to partitions
  num_left_rows = 0;
  if (OB_FAIL(split_partition(num_left_rows))) {
    LOG_WARN("failed split partition", K(ret), K(part_level_));
  } else {
    can_use_cache_aware_opt();
    if (0 == num_left_rows && OB_FAIL(recursive_postprocess())) {
      LOG_WARN("failed to post process left", K(ret));
    }
  }
  return ret;
}

int ObHashJoinOp::recursive_process(bool& need_not_read_right)
{
  int ret = OB_SUCCESS;
  need_not_read_right = false;
  int64_t num_left_rows = 0;
  if (OB_FAIL(init_join_partition())) {
    LOG_WARN("fail to init join ctx", K(ret));
  } else if (OB_FAIL(split_partition_and_build_hash_table(num_left_rows))) {
    LOG_WARN("failed to build hash table", K(ret), K(part_level_));
  }
  if (OB_SUCC(ret) && 0 == num_left_rows && RIGHT_ANTI_JOIN != MY_SPEC.join_type_ &&
      RIGHT_OUTER_JOIN != MY_SPEC.join_type_ && FULL_OUTER_JOIN != MY_SPEC.join_type_) {
    need_not_read_right = true;
    LOG_DEBUG("[HASH JOIN]Left table is empty, skip reading right table.", K(num_left_rows), K(MY_SPEC.join_type_));
  }
  return ret;
}

int ObHashJoinOp::adaptive_process(bool& need_not_read_right)
{
  int ret = OB_SUCCESS;
  need_not_read_right = false;
  if (OB_FAIL(get_processor_type())) {
    LOG_WARN("failed to get processor", K(hj_processor_), K(ret));
  } else {
    switch (hj_processor_) {
      case IN_MEMORY: {
        if (OB_FAIL(in_memory_process(need_not_read_right)) && OB_ITER_END != ret) {
          LOG_WARN("failed to process in memory", K(ret));
        }
        break;
      }
      case RECURSIVE: {
        if (OB_FAIL(recursive_process(need_not_read_right)) && OB_ITER_END != ret) {
          LOG_WARN("failed to recursive process", K(ret), K(part_level_), K(part_count_), K(hash_table_.nbuckets_));
        }
        break;
      }
      case NEST_LOOP: {
        if (OB_FAIL(nest_loop_process(need_not_read_right)) && OB_ITER_END != ret) {
          if (HJLoopState::LOOP_RECURSIVE == nest_loop_state_ && MAX_PART_LEVEL > part_level_) {
            ret = OB_SUCCESS;
            clean_nest_loop_chunk();
            set_processor(RECURSIVE);
            postprocessed_left_ = false;
            if (OB_FAIL(recursive_process(need_not_read_right)) && OB_ITER_END != ret) {
              LOG_WARN("failed to process in memory", K(ret), K(part_level_), K(part_count_), K(hash_table_.nbuckets_));
            }
            LOG_TRACE(
                "trace recursive process", K(part_level_), K(part_level_), K(part_count_), K(hash_table_.nbuckets_));
          } else {
            LOG_WARN("failed to process in memory", K(ret), K(part_level_), K(part_count_), K(hash_table_.nbuckets_));
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect processor", K(ret), K(hj_processor_));
        break;
      }
    }
  }
  LOG_TRACE(
      "trace process type", K(part_level_), K(part_count_), K(hash_table_.nbuckets_), K(remain_data_memory_size_));
  return ret;
}

int ObHashJoinOp::get_next_right_row()
{
  int ret = common::OB_SUCCESS;
  if (right_batch_ == NULL) {
    has_fill_right_row_ = true;
    clear_evaluated_flag();
    if (OB_FAIL(OB_I(t1) right_->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get right row from child failed", K(ret));
      }
    }
  } else {
    has_fill_right_row_ = false;
    has_fill_left_row_ = false;
    clear_evaluated_flag();
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(OB_I(t1) right_batch_->get_next_row(right_read_row_))) {
      right_read_row_ = NULL;
      if (OB_ITER_END != ret) {
        LOG_WARN("get right row from partition failed", K(ret));
      }
    } else {
      ++nth_right_row_;
    }
  }
  if (!postprocessed_left_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = recursive_postprocess())) {
      if (OB_SUCC(ret) || OB_ITER_END == ret) {
        ret = tmp_ret;
      }
      LOG_WARN("failed to post process left", K(ret), K(tmp_ret));
    }
  }
  return ret;
}

int ObHashJoinOp::insert_batch_row(const int64_t cur_partition_in_memory)
{
  int ret = OB_SUCCESS;
  bool need_material = true;
  bool dumped_partiton = false;
  ObHashJoinStoredJoinRow* stored_row = nullptr;
  const int64_t part_idx = get_part_idx(cur_right_hash_value_);
  if (part_idx < cur_partition_in_memory) {
    if (part_histograms_[part_idx].empty()) {
      need_material = false;
    } else if (enable_bloom_filter_) {
      if (!part_histograms_[part_idx].bloom_filter_->exist(cur_right_hash_value_)) {
        need_material = false;
      }
    }
  } else {
    dumped_partiton = true;
  }
  if (!need_material) {
  } else if (nullptr != right_read_row_) {
    if (OB_FAIL(right_hj_part_array_[part_idx].add_row(right_read_row_, stored_row))) {
      LOG_WARN("fail to add row", K(ret));
    } else {
      if (!dumped_partiton && right_hj_part_array_[part_idx].has_switch_block()) {
        cur_full_right_partition_ = part_idx;
        cur_left_hist_ = &part_histograms_[cur_full_right_partition_];
      }
    }
  } else {
    if (OB_FAIL(right_hj_part_array_[part_idx].add_row(right_->get_spec().output_, &eval_ctx_, stored_row))) {
      LOG_WARN("fail to add row", K(ret));
    } else {
      stored_row->set_hash_value(cur_right_hash_value_);
      if (!dumped_partiton && right_hj_part_array_[part_idx].has_switch_block()) {
        cur_full_right_partition_ = part_idx;
        cur_left_hist_ = &part_histograms_[cur_full_right_partition_];
        // need right to probe, it may return left and right data, so it need save temporarily
        if (OB_FAIL(right_last_row_.shadow_copy(right_->get_spec().output_, eval_ctx_))) {
          LOG_WARN("failed to shadow copy right row", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObHashJoinOp::insert_all_right_row(const int64_t row_count)
{
  int ret = OB_SUCCESS;
  // material all data and probe
  ObHashJoinStoredJoinRow* stored_row = nullptr;
  const int64_t part_idx = get_part_idx(cur_right_hash_value_);
  if (nullptr != right_read_row_) {
    if (OB_FAIL(right_hj_part_array_[part_idx].add_row(right_read_row_, stored_row))) {
      LOG_WARN("fail to add row", K(ret));
    }
  } else {
    if (OB_FAIL(right_hj_part_array_[part_idx].add_row(right_->get_spec().output_, &eval_ctx_, stored_row))) {
      LOG_WARN("fail to add row", K(ret));
    } else {
      stored_row->set_hash_value(cur_right_hash_value_);
    }
  }
  // auto memory manager
  bool updated = false;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
                 alloc_, [&](int64_t cur_cnt) { return row_count > cur_cnt; }, updated))) {
    LOG_WARN("failed to update max usable memory size periodically", K(ret), K(row_count));
  } else if (OB_LIKELY(need_dump())) {
    bool need_dumped = false;
    if (max_partition_count_per_level_ != cur_dumped_partition_) {
      // it has dumped already
      need_dumped = true;
    } else if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(
                   alloc_,
                   [&](int64_t max_memory_size) {
                     UNUSED(max_memory_size);
                     return need_dump();
                   },
                   need_dumped,
                   get_mem_used()))) {
      LOG_WARN("failed to extend max memory size", K(ret));
    }
    // dump from last partition to the first partition
    int64_t cur_dumped_partition = part_count_ - 1;
    while ((need_dump() || all_dumped()) && 0 <= cur_dumped_partition && OB_SUCC(ret)) {
      ObHashJoinPartition& right_part = right_hj_part_array_[cur_dumped_partition];
      if (0 < right_part.get_size_in_memory()) {
        if (OB_FAIL(right_part.dump(false))) {
          LOG_WARN("failed to dump partition", K(part_level_), K(cur_dumped_partition));
        } else if (0 >= hj_part_array_[cur_dumped_partition].get_row_count_on_disk() &&
                   OB_FAIL(hj_part_array_[cur_dumped_partition].dump(true))) {
        } else if (right_part.is_dumped()) {
          right_part.get_batch()->set_memory_limit(1);
          hj_part_array_[cur_dumped_partition].get_batch()->set_memory_limit(1);
        }
      }
      --cur_dumped_partition;
    }
    if (cur_dumped_partition < cur_dumped_partition_) {
      cur_dumped_partition_ = cur_dumped_partition;
    }
    LOG_TRACE("trace right need dump",
        K(part_level_),
        K(buf_mgr_->get_reserve_memory_size()),
        K(buf_mgr_->get_total_alloc_size()),
        K(cur_dumped_partition),
        K(buf_mgr_->get_dumped_size()),
        K(cur_dumped_partition_));
  }
  return ret;
}

int ObHashJoinOp::get_next_batch_right_rows()
{
  int ret = OB_SUCCESS;
  int64_t cur_partition_in_memory =
      max_partition_count_per_level_ == cur_dumped_partition_ ? part_count_ : cur_dumped_partition_ + 1;
  cur_full_right_partition_ = INT64_MAX;
  right_read_row_ = nullptr;
  int64_t row_count = 0;
  if (enable_batch_) {
    OZ(right_last_row_.restore(right_->get_spec().output_, eval_ctx_));
  }
  while (OB_SUCC(ret) && INT64_MAX == cur_full_right_partition_) {
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(get_next_right_row())) {
      if (OB_ITER_END == ret) {
        right_iter_end_ = true;
      } else {
        LOG_WARN("failed to get next right row", K(ret));
      }
    } else {
      if (NULL == right_read_row_) {
        if (OB_FAIL(calc_hash_value(right_join_keys_, right_hash_funcs_, cur_right_hash_value_))) {
          LOG_WARN("get hash value failed", K(ret));
        }
      } else {
        cur_right_hash_value_ = right_read_row_->get_hash_value();
      }
    }
    if (OB_SUCC(ret)) {
      if (enable_batch_) {
        if (OB_FAIL(insert_batch_row(cur_partition_in_memory))) {}
      } else {
        if (OB_FAIL(insert_all_right_row(row_count))) {}
      }
    }
    ++row_count;
  }
  has_fill_right_row_ = false;
  has_fill_left_row_ = false;
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    // probe left all right rows from 0 partition to last partiton
    cur_full_right_partition_ = -1;
    LOG_DEBUG("debug partition start", K(cur_full_right_partition_));
    if (!enable_batch_) {
      if (-1 != cur_dumped_partition_) {
        // recalc cache aware partition count
        calc_cache_aware_partition_count();
        LOG_TRACE("debug partition", K(level1_part_count_), K(level1_bit_), K(level2_part_count_));
        HashJoinHistogram* tmp_part_histograms = nullptr;
        PartitionSplitter part_splitter;
        if (OB_FAIL(repartition(part_splitter, part_histograms_, hj_part_array_, true))) {
          LOG_WARN("failed to repartition", K(ret));
        } else if (OB_FAIL(repartition(right_splitter_, tmp_part_histograms, right_hj_part_array_, false))) {
          LOG_WARN("failed to repartition", K(ret));
        }
      } else {
        level1_part_count_ = 0;
        level2_part_count_ = 0;
        level1_bit_ = 0;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_next_probe_partition())) {
      if (ret != OB_ITER_END) {
        LOG_WARN("failed to get next probe partition", K(ret));
      }
    }
  }
  return ret;
}

int ObHashJoinOp::get_next_probe_partition()
{
  int ret = OB_SUCCESS;
  // partition may dumped, use in memory partition
  int64_t part_count = 0 < level2_part_count_ ? level1_part_count_ * level2_part_count_ : level1_part_count_;
  int64_t dump_part_count =
      max_partition_count_per_level_ == cur_dumped_partition_ ? part_count_ : cur_dumped_partition_ + 1;
  do {
    ++cur_full_right_partition_;
    if (cur_full_right_partition_ >= part_count) {
      ret = OB_ITER_END;
    } else if (!part_histograms_[cur_full_right_partition_].empty()) {
      if (right_splitter_.is_valid()) {
        HashJoinHistogram::HistPrefixArray* prefix_hist_count = right_splitter_.part_histogram_.prefix_hist_count2_;
        if (OB_ISNULL(right_splitter_.part_histogram_.h2_) || OB_ISNULL(prefix_hist_count)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("h2 is null", K(ret));
        } else {
          if (cur_full_right_partition_ >= prefix_hist_count->count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid prefix histogram", K(ret), K(cur_full_right_partition_), K(prefix_hist_count->count()));
          }
          cur_left_hist_ = &part_histograms_[cur_full_right_partition_];
          cur_right_hist_ = &right_splitter_.part_histogram_;
          if (0 == cur_full_right_partition_) {
            cur_probe_row_idx_ = 0;
          } else {
            cur_probe_row_idx_ = prefix_hist_count->at(cur_full_right_partition_ - 1);
          }
          max_right_bucket_idx_ = prefix_hist_count->at(cur_full_right_partition_);
          if (max_right_bucket_idx_ > cur_probe_row_idx_) {
            break;
          }
        }
      } else {
        if (0 == level2_part_count_) {
          // one level
          if (cur_full_right_partition_ < dump_part_count) {
            cur_left_hist_ = &part_histograms_[cur_full_right_partition_];
            break;
          }
        } else {
          // two level
          int64_t level1_part_idx = (cur_full_right_partition_ >> (__builtin_ctz(level2_part_count_)));
          if (level1_part_idx < dump_part_count) {
            cur_left_hist_ = &part_histograms_[cur_full_right_partition_];
            break;
          }
        }
      }
    }
  } while (cur_full_right_partition_ < part_count);
  return ret;
}

int ObHashJoinOp::get_next_right_row_for_batch(NextFunc next_func)
{
  int ret = OB_SUCCESS;
  if (INT64_MAX == cur_full_right_partition_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: cur right partition", K(ret));
  } else {
    bool is_matched = false;
    right_read_row_ = nullptr;
    while (OB_SUCC(ret) && !is_matched) {
      clear_evaluated_flag();
      has_fill_right_row_ = false;
      if (OB_FAIL(try_check_status())) {
        LOG_WARN("failed to check status", K(ret));
      } else if (OB_FAIL(next_func(right_read_row_))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          if (right_iter_end_) {
            LOG_TRACE("debug partition iter end", K(cur_full_right_partition_));
            // return iter end after last partition
            if (OB_FAIL(get_next_probe_partition())) {
              if (ret != OB_ITER_END) {
                LOG_WARN("failed to get next probe partition", K(ret));
              }
            }
          } else if (OB_FAIL(get_next_batch_right_rows())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to get next batch right rows", K(ret));
            }
          }
        } else {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else if (OB_ISNULL(right_read_row_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("right read row is null", K(cur_full_right_partition_), K(cur_probe_row_idx_));
      } else if (cur_left_hist_->empty()) {
        // left partition is empty
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("left partition is empty",
            K(ret),
            K(cur_full_right_partition_),
            K(cur_dumped_partition_),
            K(enable_batch_));
      } else {
        // cur_right_hash_value_ is set when next_func
        const int64_t bucket_id = cur_left_hist_->get_bucket_idx(cur_right_hash_value_);
        if (0 == bucket_id) {
          cur_bucket_idx_ = 0;
        } else {
          cur_bucket_idx_ = cur_left_hist_->prefix_hist_count_->at(bucket_id - 1);
        }
        max_bucket_idx_ = cur_left_hist_->prefix_hist_count_->at(bucket_id);
        if (max_bucket_idx_ > cur_bucket_idx_ && OB_FAIL(get_match_row(is_matched))) {}
        LOG_DEBUG("debug get next right row",
            K(cur_right_hash_value_),
            K(cur_bucket_idx_),
            K(max_bucket_idx_),
            K(bucket_id),
            K(ret),
            K(cur_full_right_partition_));
      }
    }
  }
  return ret;
}

int ObHashJoinOp::read_right_operate()
{
  int ret = OB_SUCCESS;
  if (first_get_row_) {
    int tmp_ret = OB_SUCCESS;
    bool need_not_read_right = false;
    if (HJProcessor::NEST_LOOP == hj_processor_) {
      if (OB_SUCCESS != (tmp_ret = nest_loop_process(need_not_read_right))) {
        ret = tmp_ret;
        LOG_WARN("build hash table failed", K(ret));
      } else {
        first_get_row_ = false;
      }
    } else if (OB_SUCCESS != (tmp_ret = adaptive_process(need_not_read_right))) {
      ret = tmp_ret;
      LOG_WARN("build hash table failed", K(ret));
    } else {
      first_get_row_ = false;
    }
    if (OB_SUCC(ret)) {
      if (need_not_read_right) {
        ret = OB_ITER_END;
        if (HJProcessor::NEST_LOOP == hj_processor_) {
          nest_loop_state_ = HJLoopState::LOOP_END;
        }
      } else {
        if (nullptr != right_batch_) {
          if (OB_FAIL(right_batch_->set_iterator(false))) {
            if (OB_ITER_END == ret) {
              int tmp_ret = OB_SUCCESS;
              if (!postprocessed_left_ && OB_SUCCESS != (tmp_ret = recursive_postprocess())) {
                ret = tmp_ret;
                LOG_WARN("failed to post process left", K(ret), K(tmp_ret));
              }
            } else {
              LOG_WARN("failed to set iterator", K(ret));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (opt_cache_aware_) {
      if (has_right_material_data_) {
        if (right_iter_end_) {
          ret = OB_ITER_END;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: right must be iter end", K(ret));
        }
      } else {
        // firstly get next batch right rows
        if (OB_FAIL(get_next_batch_right_rows())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next batch right rows", K(ret));
          }
        } else {
          has_right_material_data_ = true;
          for (int64_t i = 0; i < part_count_ && OB_SUCC(ret); ++i) {
            if (OB_FAIL(right_hj_part_array_[i].init_progressive_iterator())) {
              LOG_WARN("failed to init progressive iterator", K(ret));
            }
          }
        }
      }
    } else if (OB_FAIL(get_next_right_row()) && OB_ITER_END != ret) {
      LOG_WARN("failed to get next right row", K(ret));
    }
  }
  // for right semi join, if match, then return, so for nest looop process, it only return once
  right_has_matched_ = NULL != right_read_row_ && (right_read_row_->is_match() ||
                                                      (has_right_bitset_ && right_bit_set_.has_member(nth_right_row_)));
  return ret;
}

int ObHashJoinOp::calc_hash_value(
    const ObIArray<ObExpr*>& join_keys, const ObIArray<ObHashFunc>& hash_funcs, uint64_t& hash_value)
{
  int ret = OB_SUCCESS;
  hash_value = HASH_SEED;
  ObDatum* datum = nullptr;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < join_keys.count(); ++idx) {
    if (OB_FAIL(join_keys.at(idx)->eval(eval_ctx_, datum))) {
      LOG_WARN("failed to eval datum", K(ret));
    } else {
      hash_value = hash_funcs.at(idx).hash_func_(*datum, hash_value);
    }
  }
  hash_value = hash_value & ObHashJoinStoredJoinRow::HASH_VAL_MASK;
  LOG_DEBUG("trace calc hash value", K(hash_value), K(join_keys.count()), K(hash_funcs.count()));
  return ret;
}

int ObHashJoinOp::calc_right_hash_value()
{
  int ret = OB_SUCCESS;
  if (opt_cache_aware_) {
    // has already calculated hash value
  } else if (NULL == right_read_row_) {
    if (OB_FAIL(calc_hash_value(right_join_keys_, right_hash_funcs_, cur_right_hash_value_))) {
      LOG_WARN("get hash value failed", K(ret));
    }
  } else {
    cur_right_hash_value_ = right_read_row_->get_hash_value();
  }
  state_ = JS_READ_HASH_ROW;
  return ret;
}

int ObHashJoinOp::finish_dump(bool for_left, bool need_dump, bool force /* false */)
{
  int ret = OB_SUCCESS;
  if (RECURSIVE == hj_processor_) {
    ObHashJoinPartition* part_array = NULL;
    if (for_left) {
      part_array = hj_part_array_;
    } else {
      part_array = right_hj_part_array_;
    }
    int64_t total_size = 0;
    int64_t dumped_row_count = 0;
    for (int64_t i = 0; i < part_count_ && OB_SUCC(ret); i++) {
      total_size += part_array[i].get_size_in_memory() + part_array[i].get_size_on_disk();
    }
    for (int64_t i = 0; i < part_count_ && OB_SUCC(ret); i++) {
      if (force) {
        if (OB_FAIL(part_array[i].dump(true))) {
          LOG_WARN("failed to dump", K(ret));
        } else if (cur_dumped_partition_ >= i) {
          cur_dumped_partition_ = i - 1;
        }
      }
      if (part_array[i].is_dumped()) {
        if (OB_SUCC(ret)) {
          // all dump or all in-memory
          if (OB_FAIL(part_array[i].finish_dump(true))) {
            LOG_WARN("finish dump failed", K(i), K(for_left));
          } else if (for_left) {
            // enable pair right partition dump
            right_hj_part_array_[i].get_batch()->set_memory_limit(1);
          }
        }
        dumped_row_count += part_array[i].get_row_count_on_disk();
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(part_array[i].record_pre_batch_info(part_count_, hash_table_.nbuckets_, total_size))) {
        LOG_WARN("failed to record pre-batch info", K(ret), K(part_count_), K(hash_table_.nbuckets_), K(total_size));
      }
    }
    if (force && for_left) {
      // mark dump all left partitions
      cur_dumped_partition_ = -1;
    }
    LOG_TRACE("finish dump: ",
        K(part_level_),
        K(buf_mgr_->get_reserve_memory_size()),
        K(buf_mgr_->get_total_alloc_size()),
        K(buf_mgr_->get_dumped_size()),
        K(cur_dumped_partition_),
        K(for_left),
        K(need_dump),
        K(force),
        K(total_size),
        K(dumped_row_count),
        K(part_count_));
  }
  return ret;
}

int ObHashJoinOp::read_right_func_end()
{
  int ret = OB_SUCCESS;
  if (LEFT_ANTI_JOIN == MY_SPEC.join_type_ || LEFT_SEMI_JOIN == MY_SPEC.join_type_) {
    state_ = JS_LEFT_ANTI_SEMI;
    cur_tuple_ = hash_table_.buckets_->at(0);
    cur_bkid_ = 0;
  } else if (need_left_join()) {
    state_ = JS_FILL_LEFT;
    cur_tuple_ = hash_table_.buckets_->at(0);
    cur_bkid_ = 0;
  } else {
    state_ = JS_JOIN_END;
  }

  if (RECURSIVE == hj_processor_) {
    for (int64_t i = 0; i < part_count_ && OB_SUCC(ret); i++) {
      if (hj_part_array_[i].is_dumped()) {
        if (OB_FAIL(right_hj_part_array_[i].dump(true))) {
          LOG_WARN("failed to dump", K(ret), K(i));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(finish_dump(false, true))) {
    LOG_WARN("fail to finish dump", K(ret));
  }
  return ret;
}

int ObHashJoinOp::calc_equal_conds(bool& is_match)
{
  int ret = OB_SUCCESS;
  is_match = true;
  const ObIArray<ObExpr*>& conds = MY_SPEC.equal_join_conds_;
  ObDatum* cmp_res = NULL;
  ARRAY_FOREACH(conds, i)
  {
    if (OB_FAIL(conds.at(i)->eval(eval_ctx_, cmp_res))) {
      LOG_WARN("fail to calc other join condition", K(ret), K(*conds.at(i)));
    } else if (cmp_res->is_null() || 0 == cmp_res->get_int()) {
      is_match = false;
      break;
    }
  }
  LOG_DEBUG(
      "trace calc equal conditions", K(ret), K(is_match), K(conds), K(ROWEXPR2STR(eval_ctx_, MY_SPEC.all_join_keys_)));
  return ret;
}

int ObHashJoinOp::get_match_row(bool& is_matched)
{
  int ret = OB_SUCCESS;
  is_matched = false;
  while (cur_bucket_idx_ < max_bucket_idx_ && !is_matched && OB_SUCC(ret)) {
    HistItem& item = cur_left_hist_->h2_->at(cur_bucket_idx_);
    LOG_DEBUG("trace match",
        K(ret),
        K(is_matched),
        K(cur_right_hash_value_),
        K(ROWEXPR2STR(eval_ctx_, MY_SPEC.all_join_keys_)),
        K(item.hash_value_));
    if (cur_right_hash_value_ == item.hash_value_) {
      clear_evaluated_flag();
      has_fill_left_row_ = false;
      if (OB_FAIL(convert_exprs(item.store_row_, left_->get_spec().output_, has_fill_left_row_))) {
        LOG_WARN("failed to fill left row", K(ret));
      } else if (OB_FAIL(only_join_right_row())) {
        LOG_WARN("failed to fill right row", K(ret));
      } else if (OB_FAIL(calc_equal_conds(is_matched))) {
        LOG_WARN("calc equal conds failed", K(ret));
      } else if (is_matched && OB_FAIL(calc_other_conds(is_matched))) {
        LOG_WARN("calc other conds failed", K(ret));
      } else if (is_matched) {
        has_fill_left_row_ = true;
        right_has_matched_ = true;
        // do nothing
        LOG_DEBUG("trace match",
            K(ret),
            K(is_matched),
            K(cur_right_hash_value_),
            K(ROWEXPR2STR(eval_ctx_, MY_SPEC.all_join_keys_)));
      }
    }
    ++cur_bucket_idx_;
  }
  if (OB_SUCC(ret) && !is_matched) {
    cur_bucket_idx_ = 0;
    max_bucket_idx_ = 0;
    right_has_matched_ = false;
  }
  return ret;
}

int ObHashJoinOp::read_hashrow_for_cache_aware(NextFunc next_func)
{
  int ret = OB_SUCCESS;
  if (INT64_MAX == cur_full_right_partition_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: ", K(ret));
  } else {
    bool is_matched = false;
    if (cur_bucket_idx_ < max_bucket_idx_ && OB_FAIL(get_match_row(is_matched))) {
      LOG_WARN("failed to get result", K(ret));
    } else if (!is_matched && OB_FAIL(get_next_right_row_for_batch(next_func))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next right row", K(ret));
      }
    }
  }
  return ret;
}

int ObHashJoinOp::read_hashrow()
{
  int ret = OB_SUCCESS;
  if (opt_cache_aware_) {
    if (enable_batch_) {
      auto next_func = [&](const ObHashJoinStoredJoinRow*& right_read_row) {
        int ret = OB_SUCCESS;
        ObHashJoinPartition* hj_part = &right_hj_part_array_[cur_full_right_partition_];
        if (OB_FAIL(hj_part->get_next_block_row(right_read_row))) {
        } else {
          cur_right_hash_value_ = right_read_row->get_hash_value();
        }
        return ret;
      };
      ret = read_hashrow_for_cache_aware(next_func);
    } else {
      auto next_func = [&](const ObHashJoinStoredJoinRow*& right_read_row) {
        int ret = OB_SUCCESS;
        if (cur_right_hist_->empty()) {
          ret = OB_ITER_END;
        } else if (cur_probe_row_idx_ >= max_right_bucket_idx_) {
          ret = OB_ITER_END;
        } else {
          HistItem& hist_item = cur_right_hist_->h2_->at(cur_probe_row_idx_);
          right_read_row = hist_item.store_row_;
          cur_right_hash_value_ = hist_item.hash_value_;
          ++cur_probe_row_idx_;
        }
        return ret;
      };
      ret = read_hashrow_for_cache_aware(next_func);
    }
  } else {
    ret = read_hashrow_normal();
  }
  return ret;
}

int ObHashJoinOp::read_hashrow_normal()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_id = get_bucket_idx(cur_right_hash_value_);
  ++probe_cnt_;
  if (enable_bloom_filter_ && !bloom_filter_->exist(cur_right_hash_value_)) {
    ++bitset_filter_cnt_;
    has_fill_left_row_ = false;
    cur_tuple_ = NULL;
    ret = OB_ITER_END;  // is end for scanning this bucket
  } else if (max_partition_count_per_level_ != cur_dumped_partition_) {
    int64_t part_idx = get_part_idx(cur_right_hash_value_);
    if (part_idx > cur_dumped_partition_) {
      // part index is greater than cur_dumped_partition_, than the partition has no memory data
      if (0 < hj_part_array_[part_idx].get_size_in_memory()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect no memory data in the partition",
            K(ret),
            K(part_idx),
            K(hj_part_array_[part_idx].get_size_in_memory()),
            K(cur_dumped_partition_),
            K(part_level_),
            K(part_count_));
      } else {
        cur_tuple_ = NULL;
        has_fill_left_row_ = false;
        ret = OB_ITER_END;  // is end for scanning this bucket
      }
    }
  }

  if (OB_SUCC(ret)) {
    bool is_matched = false;
    HashTableCell* tuple = NULL;
    if (NULL == cur_tuple_) {
      tuple = hash_table_.buckets_->at(bucket_id);
    } else {
      tuple = cur_tuple_->next_tuple_;
    }
    while (!is_matched && NULL != tuple && OB_SUCC(ret)) {
      ++hash_link_cnt_;
      if (cur_right_hash_value_ == tuple->stored_row_->get_hash_value()) {
        ++hash_equal_cnt_;
        clear_evaluated_flag();
        if (OB_FAIL(convert_exprs(tuple->stored_row_, left_->get_spec().output_, has_fill_left_row_))) {
          LOG_WARN("failed to fill left row", K(ret));
        } else if (OB_FAIL(only_join_right_row())) {
          LOG_WARN("failed to fill right row", K(ret));
        } else if (OB_FAIL(calc_equal_conds(is_matched))) {
          LOG_WARN("calc equal conds failed", K(ret));
        } else if (is_matched && OB_FAIL(calc_other_conds(is_matched))) {
          LOG_WARN("calc other conds failed", K(ret));
        } else {
          // do nothing
          LOG_DEBUG("trace match",
              K(ret),
              K(is_matched),
              K(cur_right_hash_value_),
              K(ROWEXPR2STR(eval_ctx_, MY_SPEC.all_join_keys_)));
        }
      }
      if (OB_SUCC(ret) && !is_matched) {
        tuple = tuple->next_tuple_;
      }
    }  // while end

    if (OB_FAIL(ret)) {
    } else if (!is_matched) {
      has_fill_left_row_ = false;
      cur_tuple_ = NULL;
      ret = OB_ITER_END;  // is end for scanning this bucket
    } else {
      cur_tuple_ = tuple;  // last matched tuple
      right_has_matched_ = true;
      has_fill_left_row_ = true;
      if (INNER_JOIN != MY_SPEC.join_type_) {
        tuple->stored_row_->set_is_match(true);
      }
    }
  }
  return ret;
}

int ObHashJoinOp::join_rows_with_right_null()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(blank_right_row())) {
    LOG_WARN("failed to blank right null", K(ret));
  } else if (OB_FAIL(only_join_left_row())) {
    LOG_WARN("failed to blank left row", K(ret));
  } else {
    has_fill_right_row_ = true;
  }
  return ret;
}

int ObHashJoinOp::join_rows_with_left_null()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(blank_left_row())) {
    LOG_WARN("failed to blank right null", K(ret));
  } else if (OB_FAIL(only_join_right_row())) {
    LOG_WARN("failed to fill right row", K(ret));
  } else {
    has_fill_left_row_ = true;
  }
  return ret;
}

int ObHashJoinOp::only_join_right_row()
{
  int ret = OB_SUCCESS;
  if (right_read_row_ != NULL && !has_fill_right_row_) {
    if (OB_FAIL(convert_exprs(right_read_row_, right_->get_spec().output_, has_fill_right_row_))) {
      LOG_WARN("failed to convert right exprs", K(ret));
    }
  }
  return ret;
}

int ObHashJoinOp::only_join_left_row()
{
  int ret = OB_SUCCESS;
  if (left_read_row_ != NULL && !has_fill_left_row_) {
    if (OB_FAIL(convert_exprs(left_read_row_, left_->get_spec().output_, has_fill_left_row_))) {
      LOG_WARN("failed to convert right exprs", K(ret));
    }
  }
  return ret;
}

int ObHashJoinOp::other_join_read_hashrow_func_going()
{
  int ret = OB_SUCCESS;
  if (LEFT_SEMI_JOIN == MY_SPEC.join_type_ || LEFT_ANTI_JOIN == MY_SPEC.join_type_) {
    // do nothing
  } else if (RIGHT_SEMI_JOIN == MY_SPEC.join_type_) {
    // mark this row is match, and return already
    if (NEST_LOOP == hj_processor_) {
      if (!right_bit_set_.has_member(nth_right_row_)) {
        right_bit_set_.add_member(nth_right_row_);
        mark_return();
      }
    } else {
      mark_return();
    }
    cur_tuple_ = NULL;
    state_ = JS_READ_RIGHT;
  } else if (RIGHT_ANTI_JOIN == MY_SPEC.join_type_) {
    if (NEST_LOOP == hj_processor_) {
      if (!right_bit_set_.has_member(nth_right_row_)) {
        right_bit_set_.add_member(nth_right_row_);
      }
    }
    cur_tuple_ = NULL;
    state_ = JS_READ_RIGHT;
  } else {
    mark_return();
  }
  return ret;
}

// right dump depend on the dumped partition, if left partition don't dump,
//  then right partition will not do
int ObHashJoinOp::dump_probe_table()
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(need_dump())) {
    LOG_TRACE("need dump", K(buf_mgr_->get_reserve_memory_size()), K(buf_mgr_->get_total_alloc_size()));
    // dump from last partition to the first partition
    int64_t cur_dumped_partition = part_count_ - 1;
    while ((need_dump() || all_dumped()) && 0 <= cur_dumped_partition && OB_SUCC(ret)) {
      ObHashJoinPartition& right_part = right_hj_part_array_[cur_dumped_partition];
      if (hj_part_array_[cur_dumped_partition].is_dumped()) {
        if (0 < right_part.get_size_in_memory()) {
          if (OB_FAIL(right_part.dump(false))) {
            LOG_WARN("failed to dump partition", K(part_level_), K(cur_dumped_partition));
          } else if (right_part.is_dumped()) {
            right_part.get_batch()->set_memory_limit(1);
          }
        }
      }
      --cur_dumped_partition;
    }
    LOG_TRACE("trace right need dump",
        K(part_level_),
        K(buf_mgr_->get_reserve_memory_size()),
        K(buf_mgr_->get_total_alloc_size()),
        K(cur_dumped_partition),
        K(buf_mgr_->get_dumped_size()),
        K(cur_dumped_partition_));
  }
  return ret;
}

int ObHashJoinOp::other_join_read_hashrow_func_end()
{
  int ret = OB_SUCCESS;
  const int64_t part_idx = get_part_idx(cur_right_hash_value_);
  if (RECURSIVE == hj_processor_ && hj_part_array_[part_idx].is_dumped()) {
    if (right_has_matched_ && RIGHT_SEMI_JOIN == MY_SPEC.join_type_) {
      // do nothing, if is right semi join and matched.
      // ret = OB_ERR_UNEXPECTED;
      // LOG_WARN("right semi already return row, so it can't be here", K(ret));
    } else {
      ObHashJoinStoredJoinRow* stored_row = nullptr;
      if (nullptr != right_read_row_) {
        if (OB_FAIL(right_hj_part_array_[part_idx].add_row(right_read_row_, stored_row))) {
          LOG_WARN("fail to add row", K(ret));
        }
      } else {
        if (OB_FAIL(only_join_right_row())) {
          LOG_WARN("failed to join right row", K(ret));
        } else if (OB_FAIL(
                       right_hj_part_array_[part_idx].add_row(right_->get_spec().output_, &eval_ctx_, stored_row))) {
          LOG_WARN("fail to add row", K(ret));
        }
      }
      if (nullptr != stored_row) {
        stored_row->set_is_match(right_has_matched_);
        stored_row->set_hash_value(cur_right_hash_value_);
        if (OB_FAIL(dump_probe_table())) {
          LOG_WARN("failed to dump right", K(ret));
        }
      }
    }
  } else if (!is_last_chunk_) {
    // not the last chunk rows
    if (has_right_bitset_ && right_has_matched_) {
      right_bit_set_.add_member(nth_right_row_);
    }
  } else if (!right_has_matched_) {
    if (need_right_join()) {
      if (OB_FAIL(join_rows_with_left_null())) {  // right outer join, null-rightrow
        LOG_WARN("failed to right join rows", K(ret));
      } else {
        mark_return();
      }
    } else if (RIGHT_ANTI_JOIN == MY_SPEC.join_type_) {
      if (OB_FAIL(only_join_right_row())) {
        LOG_WARN("failed to convert right row", K(ret));
      } else {
        mark_return();
      }
    }
  }
  LOG_DEBUG("read hashrow func end", K(MY_SPEC.join_type_), K(right_has_matched_));
  state_ = JS_READ_RIGHT;
  return ret;
}

int ObHashJoinOp::inner_join_read_hashrow_func_going()
{
  int ret = OB_SUCCESS;
  if (!opt_cache_aware_) {
    const int64_t part_idx = get_part_idx(cur_right_hash_value_);
    if (nullptr == cur_tuple_->next_tuple_ && (RECURSIVE != hj_processor_ || !hj_part_array_[part_idx].is_dumped())) {
      // inner join, it don't need to process right match
      // if hash link has no other tuple, then read right after return data
      state_ = JS_READ_RIGHT;
      cur_tuple_ = nullptr;
    }
  }
  mark_return();
  return ret;
}

int ObHashJoinOp::inner_join_read_hashrow_func_end()
{
  int ret = OB_SUCCESS;
  const int64_t part_idx = get_part_idx(cur_right_hash_value_);
  if (opt_cache_aware_) {
  } else if (RECURSIVE == hj_processor_ && hj_part_array_[part_idx].is_dumped()) {
    ObHashJoinStoredJoinRow* stored_row = nullptr;
    if (nullptr != right_read_row_) {
      if (OB_FAIL(right_hj_part_array_[part_idx].add_row(right_read_row_, stored_row))) {
        LOG_WARN("fail to add row", K(ret));
      }
    } else {
      if (OB_FAIL(only_join_right_row())) {
        LOG_WARN("failed to join right row", K(ret));
      } else if (OB_FAIL(right_hj_part_array_[part_idx].add_row(right_->get_spec().output_, &eval_ctx_, stored_row))) {
        LOG_WARN("fail to add row", K(ret));
      }
    }
    if (nullptr != stored_row) {
      stored_row->set_is_match(right_has_matched_);
      stored_row->set_hash_value(cur_right_hash_value_);
      if (OB_FAIL(dump_probe_table())) {
        LOG_WARN("failed to dump right", K(ret));
      }
    }
  }
  LOG_DEBUG("read hashrow func end", K(MY_SPEC.join_type_), K(right_has_matched_));
  state_ = JS_READ_RIGHT;
  return ret;
}

int ObHashJoinOp::read_hashrow_func_going()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((this->*going_func_)())) {
    LOG_WARN("failed to read hashrow func end", K(ret));
  }
  return ret;
}

int ObHashJoinOp::read_hashrow_func_end()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((this->*end_func_)())) {
    LOG_WARN("failed to read hashrow func end", K(ret));
  }
  return ret;
}

int ObHashJoinOp::find_next_matched_tuple(HashTableCell*& tuple)
{
  int ret = OB_SUCCESS;
  PartHashJoinTable& htable = hash_table_;
  while (OB_SUCC(ret)) {
    if (NULL != tuple) {
      if (!tuple->stored_row_->is_match()) {
        tuple = tuple->next_tuple_;
      } else {
        break;
      }
    } else {
      int64_t bucket_id = cur_bkid_ + 1;
      if (bucket_id < htable.nbuckets_) {
        tuple = htable.buckets_->at(bucket_id);
        cur_bkid_ = bucket_id;
      } else {
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

int ObHashJoinOp::left_anti_semi_operate()
{
  int ret = OB_SUCCESS;
  if (LEFT_ANTI_JOIN == MY_SPEC.join_type_) {
    ret = find_next_unmatched_tuple(cur_tuple_);
  } else if (LEFT_SEMI_JOIN == MY_SPEC.join_type_) {
    ret = find_next_matched_tuple(cur_tuple_);
  }
  return ret;
}

int ObHashJoinOp::left_anti_semi_going()
{
  int ret = OB_SUCCESS;
  HashTableCell* tuple = cur_tuple_;
  if (LEFT_ANTI_JOIN == MY_SPEC.join_type_ && (NULL != tuple && !tuple->stored_row_->is_match())) {
    if (OB_FAIL(convert_exprs(tuple->stored_row_, left_->get_spec().output_, has_fill_left_row_))) {
      LOG_WARN("convert tuple failed", K(ret));
    } else {
      tuple->stored_row_->set_is_match(true);
      mark_return();
    }
  } else if (LEFT_SEMI_JOIN == MY_SPEC.join_type_ && (NULL != tuple && tuple->stored_row_->is_match())) {
    if (OB_FAIL(convert_exprs(tuple->stored_row_, left_->get_spec().output_, has_fill_left_row_))) {
      LOG_WARN("convert tuple failed", K(ret));
    } else {
      tuple->stored_row_->set_is_match(false);
      mark_return();
    }
  }
  return ret;
}

int ObHashJoinOp::left_anti_semi_end()
{
  int ret = OB_SUCCESS;
  state_ = JS_JOIN_END;
  return ret;
}

int ObHashJoinOp::find_next_unmatched_tuple(HashTableCell*& tuple)
{
  int ret = OB_SUCCESS;
  PartHashJoinTable& htable = hash_table_;
  while (OB_SUCC(ret)) {
    if (NULL != tuple) {
      if (tuple->stored_row_->is_match()) {
        tuple = tuple->next_tuple_;
      } else {
        break;
      }
    } else {
      int64_t bucket_id = cur_bkid_ + 1;
      if (bucket_id < htable.nbuckets_) {
        tuple = htable.buckets_->at(bucket_id);
        cur_bkid_ = bucket_id;
      } else {
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

int ObHashJoinOp::fill_left_operate()
{
  int ret = OB_SUCCESS;
  ret = find_next_unmatched_tuple(cur_tuple_);
  return ret;
}

int ObHashJoinOp::convert_exprs(
    const ObHashJoinStoredJoinRow* store_row, const ObIArray<ObExpr*>& exprs, bool& has_fill)
{
  int ret = OB_SUCCESS;
  has_fill = true;
  if (OB_ISNULL(store_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("store row is null", K(ret));
  } else {
    for (uint32_t i = 0; i < store_row->cnt_; ++i) {
      exprs.at(i)->locate_expr_datum(eval_ctx_) = store_row->cells()[i];
      exprs.at(i)->get_eval_info(eval_ctx_).evaluated_ = true;
    }
  }
  return ret;
}

int ObHashJoinOp::fill_left_going()
{
  int ret = OB_SUCCESS;
  HashTableCell* tuple = cur_tuple_;
  if (NULL != tuple && !tuple->stored_row_->is_match()) {
    if (OB_FAIL(convert_exprs(tuple->stored_row_, left_->get_spec().output_, has_fill_left_row_))) {
      LOG_WARN("convert tuple failed", K(ret));
    } else if (OB_FAIL(join_rows_with_right_null())) {
      LOG_WARN("left join rows failed", K(ret));
    } else {
      mark_return();
      tuple->stored_row_->set_is_match(true);
    }
  }
  return ret;
}

int ObHashJoinOp::fill_left_end()
{
  int ret = OB_SUCCESS;
  state_ = JS_JOIN_END;
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
