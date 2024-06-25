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

#include "sql/engine/join/hash_join/ob_hash_join_vec_op.h"
#include "sql/engine/px/ob_px_util.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/engine/px/ob_px_util.h"
#include "share/diagnosis/ob_sql_monitor_statname.h"

namespace oceanbase
{
using namespace omt;
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER(ObHashJoinVecInput, shared_hj_info_);

//ctx is owned by thread, sync_event is shared
int ObHashJoinVecInput::sync_wait(ObExecContext &ctx, int64_t &sync_event, EventPred pred, bool ignore_interrupt, bool is_open)
{
  int ret = OB_SUCCESS;
  ObHJSharedTableInfo *shared_hj_info = reinterpret_cast<ObHJSharedTableInfo *>(shared_hj_info_);
  if (OB_ISNULL(shared_hj_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: shared hash join info is null", K(ret));
  } else if (OB_ISNULL(pred)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: pred is null", K(ret));
  } else {
    bool has_process = false;
    int64_t loop = 0;
    int64_t exit_cnt = shared_hj_info->sqc_thread_count_ *
                        (1 + sync_event / shared_hj_info->sqc_thread_count_);
    while (OB_SUCC(ret)) {
      ++loop;
      if (!has_process) {
        ObSpinLockGuard guard(shared_hj_info->lock_);
        // got lock to process and only one thread can process pred()
        // it may enter by multiple threads
        LOG_DEBUG("before pred", K(sync_event), K(exit_cnt), K(shared_hj_info->sqc_thread_count_));
        pred(ATOMIC_LOAD(&sync_event) % shared_hj_info->sqc_thread_count_);
        // it must be here to guarantee next wait loop to get lock for one thread
        has_process = true;
        if (ATOMIC_LOAD(&sync_event) + 1 >= exit_cnt) {
          // last thread, it will singal and exit by self
          ATOMIC_INC(&sync_event);
          shared_hj_info->cond_.signal();
          LOG_DEBUG("debug signal event", K(ret), K(lbt()), K(sync_event));
          break;
        }
        ATOMIC_INC(&sync_event);
        LOG_DEBUG("debug sync event", K(ret), K(sync_event), K(exit_cnt), K(lbt()));
      }
      if (!ignore_interrupt && OB_SUCCESS != shared_hj_info->ret_) {
        // the thread already return error
        ret = shared_hj_info->ret_;
      } else if (!ignore_interrupt && 0 == loop % 8 && OB_UNLIKELY(IS_INTERRUPTED())) {
        // handle interrupt error code, overwrite ret
        ObInterruptCode code = GET_INTERRUPT_CODE();
        ret = code.code_;
        LOG_WARN("received a interrupt", K(code), K(ret));
      } else if (!ignore_interrupt && 0 == loop % 16 && OB_FAIL(ctx.fast_check_status())) {
        LOG_WARN("failed to check status", K(ret));
      } else if (ATOMIC_LOAD(&sync_event) >= exit_cnt) {
        // timeout, and signal has done
        LOG_DEBUG("debug sync event done", K(ret), K(lbt()), K(sync_event),
          K(loop), K(exit_cnt));
        break;
      } else if (ignore_interrupt /*close stage*/
                 && OB_SUCCESS != ATOMIC_LOAD(&shared_hj_info->open_ret_)) {
        LOG_WARN("some op have failed in open stage", K(ATOMIC_LOAD(&shared_hj_info->open_ret_)));
        break;
      } else {
        auto key = shared_hj_info->cond_.get_key();
        // wait one time per 1000 us
        LOG_DEBUG("cond wait", K(key), K(sync_event), K(lbt()));
        shared_hj_info->cond_.wait(key, 1000);
      }
    } // end while
    if (OB_FAIL(ret) && is_open) {
      set_open_ret(ret);
    }
  }
  return ret;
}

ObHashJoinVecSpec::ObHashJoinVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObJoinVecSpec(alloc, type),
  join_conds_(alloc),
  build_keys_(alloc),
  probe_keys_(alloc),
  build_key_proj_(alloc),
  probe_key_proj_(alloc),
  can_prob_opt_(false),
  is_naaj_(false),
  is_sna_(false),
  is_shared_ht_(false),
  is_ns_equal_cond_(alloc)
{
}

OB_SERIALIZE_MEMBER((ObHashJoinVecSpec, ObJoinVecSpec),
                    join_conds_,
                    build_keys_,
                    probe_keys_,
                    build_key_proj_,
                    probe_key_proj_,
                    can_prob_opt_,
                    is_naaj_,
                    is_sna_,
                    is_shared_ht_,
                    is_ns_equal_cond_);

ObHashJoinVecOp::ObHashJoinVecOp(ObExecContext &ctx_, const ObOpSpec &spec, ObOpInput *input)
  : ObJoinVecOp(ctx_, spec, input),
  max_output_cnt_(0),
  hj_state_(HJState::INIT),
  hj_processor_(NONE),
  force_hash_join_spill_(false),
  hash_join_processor_(7),
  tenant_id_(-1),
  profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
  sql_mem_processor_(profile_, op_monitor_info_),
  state_(JS_PROCESS_LEFT),
  drain_mode_(HashJoinDrainMode::NONE_DRAIN),
  remain_data_memory_size_(0),
  nth_nest_loop_(0),
  dumped_fixed_mem_size_(0),
  jt_ctx_(),
  join_table_(),
  mem_context_(nullptr),
  alloc_(nullptr),
  nest_loop_state_(HJLoopState::LOOP_START),
  is_last_chunk_(false),
  need_return_(false),
  iter_end_(false),
  is_shared_(false),
  cur_join_table_(nullptr),
  left_part_array_(NULL),
  right_part_array_(NULL),
  left_part_(NULL),
  right_part_(NULL),
  part_mgr_(NULL),
  part_level_(0),
  part_shift_(MAX_PART_LEVEL << 3),
  part_count_(0),
  part_round_(1),
  part_stored_rows_(NULL),
  part_added_rows_(NULL),
  cur_dumped_partition_(MAX_PART_COUNT_PER_LEVEL),
  hash_vals_(NULL),
  null_skip_bitmap_(NULL),
  child_brs_(),
  part_selectors_(NULL),
  part_selector_sizes_(NULL),
  probe_batch_rows_(),
  right_batch_traverse_cnt_(0),
  read_null_in_naaj_(false),
  non_preserved_side_is_not_empty_(false),
  null_random_hash_value_(0),
  skip_left_null_(false),
  skip_right_null_(false),
  data_ratio_(1.0),
  output_info_()
{
}

uint64_t add_alloc_size() { return 0; }

template <typename X, typename Y, typename ...TS>
uint64_t add_alloc_size(const X &, const Y &y, const TS &...args)
{
  return y + add_alloc_size(args...);
}

void alloc_ptr_one_by_one(void *) { return; }

template<typename X, typename Y, typename ...TS>
void alloc_ptr_one_by_one(void *ptr, const X &x, const Y &y, const TS &...args)
{
  const_cast<X &>(x) = static_cast<X>(ptr);
  alloc_ptr_one_by_one(static_cast<char *>(ptr) + y, args...);
}

// allocate pointers which passed <ptr, size> paires
template <typename ...TS>
int vec_alloc_ptrs(ObIAllocator *alloc, const TS &...args)
{
  int ret = OB_SUCCESS;
  int64_t size = add_alloc_size(args...);
  void *ptr = alloc->alloc(size);
  if (NULL == ptr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMSET(ptr, 0, size);
    alloc_ptr_one_by_one(ptr, args...);
  }
  return ret;
}

int ObHashJoinVecOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  if (OB_FAIL(set_shared_info())) {
    LOG_WARN("failed to set shared info", K(ret));
  } else if (is_shared_ && OB_FAIL(sync_wait_open())) {
    is_shared_ = false;
    LOG_WARN("failed to sync open for shared hj", K(ret));
  } else if (OB_FAIL(ObJoinVecOp::inner_open())) {
    LOG_WARN("failed to open in base class", K(ret));
  } else if (OB_ISNULL(session = ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get my session", K(ret));
  } else if (OB_FAIL(init_mem_context(session->get_effective_tenant_id()))) {
    LOG_WARN("fail to init base join ctx", K(ret));
  } else if (OB_FAIL(init_join_table_ctx())) {
    LOG_WARN("fail to init join table ctx", K(ret));
  } else if (OB_FAIL(join_table_.init(jt_ctx_, *alloc_))) {
    LOG_WARN("fail to init hash table", K(ret));
  } else {
    tenant_id_ = session->get_effective_tenant_id();
    ObTenantConfigGuard tenant_config(TENANT_CONF(session->get_effective_tenant_id()));
    if (tenant_config.is_valid()) {
      force_hash_join_spill_ = tenant_config->_force_hash_join_spill;
      hash_join_processor_ = tenant_config->_enable_hash_join_processor;
      if (0 == (hash_join_processor_ & HJ_PROCESSOR_MASK)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect hash join processor", K(ret), K(hash_join_processor_));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tenant config", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    skip_left_null_ = (INNER_JOIN == MY_SPEC.join_type_) || (LEFT_SEMI_JOIN == MY_SPEC.join_type_)
                      || (RIGHT_SEMI_JOIN == MY_SPEC.join_type_) || (RIGHT_OUTER_JOIN == MY_SPEC.join_type_);
    skip_right_null_ = (INNER_JOIN == MY_SPEC.join_type_) || (LEFT_SEMI_JOIN == MY_SPEC.join_type_)
                      || (RIGHT_SEMI_JOIN == MY_SPEC.join_type_) || (LEFT_OUTER_JOIN == MY_SPEC.join_type_);
    part_count_ = 0;
    hj_state_ = HJState::INIT;
    void* buf = alloc_->alloc(sizeof(ObHJPartitionMgr));
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem");
    } else {
      part_mgr_ = new (buf) ObHJPartitionMgr(*alloc_, tenant_id_);
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t batch_size = MY_SPEC.max_batch_size_;
    OZ (vec_alloc_ptrs(&mem_context_->get_arena_allocator(),
                       part_stored_rows_, sizeof(* part_stored_rows_) * batch_size,
                       hash_vals_, sizeof(*hash_vals_) * batch_size,
                       child_brs_.skip_, ObBitVector::memory_size(batch_size),
                       part_added_rows_, sizeof(part_added_rows_) * batch_size,
                       null_skip_bitmap_, ObBitVector::memory_size(batch_size),
                       output_info_.left_result_rows_, sizeof(*output_info_.left_result_rows_) * batch_size,
                       output_info_.selector_, sizeof(*output_info_.selector_) * batch_size,
                       probe_batch_rows_.stored_rows_, sizeof(*probe_batch_rows_.stored_rows_) * batch_size,
                       probe_batch_rows_.hash_vals_, sizeof(*probe_batch_rows_.hash_vals_) * batch_size,
                       probe_batch_rows_.brs_.skip_, ObBitVector::memory_size(batch_size),
                       probe_batch_rows_.key_data_, join_table_.get_normalized_key_size() * batch_size,
                       jt_ctx_.stored_rows_, sizeof(*jt_ctx_.stored_rows_) * batch_size,
                       jt_ctx_.cur_items_, sizeof(*jt_ctx_.cur_items_) * batch_size));
    const ObExprPtrIArray &left_output = left_->get_spec().output_;
    left_vectors_.set_allocator(&mem_context_->get_arena_allocator());
    OZ (left_vectors_.init(left_output.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < left_output.count(); ++i) {
      OZ (left_vectors_.push_back(left_output.at(i)->get_vector(eval_ctx_)));
    }
  }
  cur_join_table_ = &join_table_;
  return ret;
}

int ObHashJoinVecOp::init_join_table_ctx()
{
  int ret = OB_SUCCESS;
  jt_ctx_.eval_ctx_ = &eval_ctx_;
  jt_ctx_.join_type_ = MY_SPEC.join_type_;
  jt_ctx_.join_conds_ = &MY_SPEC.join_conds_;
  jt_ctx_.build_keys_ = &MY_SPEC.build_keys_;
  jt_ctx_.probe_keys_ = &MY_SPEC.probe_keys_;
  jt_ctx_.build_key_proj_ = &MY_SPEC.build_key_proj_;
  jt_ctx_.probe_key_proj_ = &MY_SPEC.probe_key_proj_;
  jt_ctx_.build_output_ = &left_->get_spec().output_;
  jt_ctx_.probe_output_ = &right_->get_spec().output_;
  jt_ctx_.calc_exprs_ = &MY_SPEC.calc_exprs_;

  jt_ctx_.build_row_meta_.set_allocator(&eval_ctx_.exec_ctx_.get_allocator());
  jt_ctx_.probe_row_meta_.set_allocator(&eval_ctx_.exec_ctx_.get_allocator());
  OZ(jt_ctx_.build_row_meta_.init(left_->get_spec().output_, sizeof(ObHJStoredRow::ExtraInfo)));
  OZ(jt_ctx_.probe_row_meta_.init(right_->get_spec().output_, sizeof(ObHJStoredRow::ExtraInfo)));
  if (OB_SUCC(ret)) {
    jt_ctx_.is_shared_ = MY_SPEC.is_shared_ht_;
    jt_ctx_.max_output_cnt_ = &max_output_cnt_;
    jt_ctx_.max_batch_size_ = MY_SPEC.max_batch_size_;
    jt_ctx_.output_info_ = &output_info_;
    jt_ctx_.probe_batch_rows_ = &probe_batch_rows_;
    jt_ctx_.probe_opt_ = MY_SPEC.can_prob_opt_;
  }
  jt_ctx_.contain_ns_equal_ = false;
  for (int64_t i = 0; !jt_ctx_.contain_ns_equal_ && i < MY_SPEC.is_ns_equal_cond_.count(); i++) {
    if (MY_SPEC.is_ns_equal_cond_.at(i)) {
      jt_ctx_.contain_ns_equal_ = true;
    }
  }

  return ret;
}

int ObHashJoinVecOp::set_shared_info()
{
  int ret = OB_SUCCESS;
  ObHashJoinVecInput *hj_input = static_cast<ObHashJoinVecInput*>(input_);
  if (!MY_SPEC.is_shared_ht_) {
    // none shared, nothing to do
  } else if (OB_ISNULL(hj_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: hash join input is null", K(ret));
  } else if (0 == hj_input->shared_hj_info_ || OB_ISNULL(hj_input->get_shared_hj_info())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: hash join shared info is null", K(ret));
  } else {
    // only more thant one thread, use shared hash join
    is_shared_ = 1 < hj_input->get_sqc_thread_count();
    if (is_shared_) {
      if (IS_LEFT_STYLE_JOIN(MY_SPEC.join_type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed shared hash join not support", K(ret), K(MY_SPEC.join_type_));
      } else if (hj_input->task_id_ >= hj_input->get_sqc_thread_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task_id is more than thread count", K(ret),
          K(hj_input->task_id_), K(hj_input->get_sqc_thread_count()));
      } else {
        LOG_TRACE("debug enable shared hash join", K(ret), K(spec_.id_));
      }
    }
  }
  return ret;
}

void ObHashJoinVecOp::reset_base()
{
  drain_mode_ = HashJoinDrainMode::NONE_DRAIN;
  hj_state_ = HJState::INIT;
  hj_processor_ = NONE;
  part_level_ = 0;
  part_shift_ = MAX_PART_LEVEL << 3;
  part_count_ = 0;
  left_part_ = nullptr;
  right_part_ = nullptr;
  dumped_fixed_mem_size_ = 0;
}

void ObHashJoinVecOp::reset()
{
  clean_part_mgr();
  part_rescan();
  reset_base();
}

void ObHashJoinVecOp::part_rescan()
{
  state_ = JS_PROCESS_LEFT;
  join_table_.reset();
  jt_ctx_.reuse();
  output_info_.reuse();
  nest_loop_state_ = HJLoopState::LOOP_START;
  is_last_chunk_ = false;
  remain_data_memory_size_ = 0;
  reset_nest_loop();
  hj_processor_ = NONE;
  cur_dumped_partition_ = MAX_PART_COUNT_PER_LEVEL;
  //reset_statistics();
  if (left_part_array_ != NULL) {
    alloc_->free(left_part_array_);
    left_part_array_ = NULL;
    right_part_array_ = NULL;
  }
  if (OB_NOT_NULL(part_selectors_)) {
    alloc_->free(part_selectors_);
    part_selectors_ = nullptr;
    part_selector_sizes_ = nullptr;
  }
  need_return_ = false;
  probe_batch_rows_.from_stored_ = false;
  right_batch_traverse_cnt_ = 0;
  null_random_hash_value_ = 0;
}

void ObHashJoinVecOp::part_rescan(bool reset_all)
{
  int ret = OB_SUCCESS;
  if (reset_all) {
    reset();
    part_count_ = 0;
  } else {
    part_rescan();
  }
}

int ObHashJoinVecOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  part_rescan(true);
 if (OB_FAIL(ObJoinVecOp::inner_rescan())) {
    LOG_WARN("join rescan failed", K(ret));
  } else {
    part_round_ = 1;
    iter_end_ = false;
    read_null_in_naaj_ = false;
    non_preserved_side_is_not_empty_ = false;
  }
  LOG_TRACE("hash join rescan", K(ret), K(spec_.id_));
  return ret;
}

// 这里有几个逻辑需要理下：
// 1）当返回一行时，如果left对应bucket没有元素，即cur_tuple为nullpr，会继续get_right_row，所以不需要save
// 2）如果不会拿下一个right，则需要restore上次保存的right，继续看是否匹配left其他行
//      这里有两种情况：
//        a）如果没有dump，恢复output
//        b）如果dump，则仅恢复right_read_row_，至于是否放入到right output，看下一次是否返回避免多余copy

//
//  begin  INIT->PROCESS_PARTITION->LOAD_NEXT_PARTITION--> iter_end
//                 ^                   |
//                 |     next          |
//                 + ---<--------------+
//
int ObHashJoinVecOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  max_output_cnt_ = min(max_row_cnt, MY_SPEC.max_batch_size_);
  bool exit_while = false;
  if (OB_UNLIKELY(iter_end_)) {
    brs_.size_ = 0;
    brs_.end_ = true;
    exit_while = true;
  }
  clear_evaluated_flag();
  while (OB_SUCCESS == ret && !exit_while) {
    switch (hj_state_) {
      case HJState::INIT: {
        hj_state_ = HJState::PROCESS_PARTITION;
        break;
      }
      case HJState::PROCESS_PARTITION: {
        ret = process_partition();
        if (OB_ITER_END == ret) {
          hj_state_ = HJState::LOAD_NEXT_PARTITION;
          ret = OB_SUCCESS;
        } else if (OB_SUCCESS == ret) {
          exit_while = true;
        } else {
          LOG_WARN("fail to get next row", K(ret));
        }
        break;
      }
      //TODO shengle simplify this code
      case HJState::LOAD_NEXT_PARTITION: {
        // It must firstly sync wait, and then remove undumped batch
        if (is_shared_ && OB_FAIL(sync_wait_fetch_next_batch())) {
          LOG_WARN("failed to sync wait fetch next batch", K(ret));
        } else {
          part_mgr_->remove_undumped_part(is_shared_ ? cur_dumped_partition_ : INT64_MAX, part_round_);
          if (left_part_ != NULL) {
            left_part_->close();
            part_mgr_->free(left_part_);
            left_part_ = NULL;
          }
          if (right_part_ != NULL) {
            right_part_->close();
            part_mgr_->free(right_part_);
            right_part_ = NULL;
          }
        }
        ObHJPartitionPair part_pair;
        if (OB_FAIL(ret)) {
        } else if (FALSE_IT(part_rescan(false))) {
          // do nothing
        } else if (read_null_in_naaj_) {
          // if read null in naaj, return the empty set directly
          ret = OB_ITER_END;
        } else {
          ret = part_mgr_->next_part_pair(part_pair);
        }
        //TODO shengle ?
        dumped_fixed_mem_size_ = get_cur_mem_used();
        if (OB_ITER_END == ret) {
          exit_while = true;
          iter_end_ = true;
          part_rescan(true);
          brs_.size_ = 0;
          brs_.end_ = true;
          ret = OB_SUCCESS;
          LOG_TRACE("hash join iter end", K(spec_.id_));
        } else if (OB_SUCCESS == ret) {
          ++part_round_;
          left_part_ = part_pair.left_;
          right_part_ = part_pair.right_;
          part_level_ = part_pair.left_->get_part_level() + 1;
          part_shift_ = part_pair.left_->get_part_shift();

          // asynchronously wait to write while only read the dumped partition
          if (OB_FAIL(left_part_->get_row_store().finish_add_row(true))) {
            LOG_WARN("finish dump failed", K(ret));
          } else if (OB_FAIL(right_part_->get_row_store().finish_add_row(true))) {
            LOG_WARN("finish dump failed", K(ret));
          } else {
            left_part_->open();
            right_part_->open();
            if (sizeof(uint64_t) * CHAR_BIT <= part_shift_) {
              // avoid loop recursively
              // At most MAX_PART_LEVEL, the last level is either nest loop or in-memory
              // hash join dumped too many times, the part level is greater than 32 bit
              // we report 4013 instead of 4016, and remind user to increase memory
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("too deep part level", K(ret), K(part_level_), K(part_shift_));
            } else {
              hj_state_ = HJState::PROCESS_PARTITION;
            }
            LOG_DEBUG("trace partition", K(left_part_->get_partno()),
              K(right_part_->get_partno()), K(part_level_), K(part_round_));
          }
        } else {
          LOG_WARN("fail get next partition", K(ret));
        }
        break;
      }
    }
  }
  return ret;
}

int ObHashJoinVecOp::process_partition()
{
  int ret = OB_SUCCESS;
  need_return_ = false;
  while (OB_SUCCESS == ret && !need_return_) {
    switch (state_) {
      case JS_PROCESS_LEFT: {
        bool need_not_read_right = false;
        if (OB_FAIL(process_left(need_not_read_right))) {
          LOG_WARN("fail to process left", K(ret), K(need_not_read_right));
        } else if (need_not_read_right) {
          state_ = output_unmatch_left() ? JS_LEFT_UNMATCH_RESULT : JS_JOIN_END;
        } else {
          state_ = JS_READ_RIGHT;
        }
        break;
      }
      case JS_READ_RIGHT: {
        // 右边获取行后, 创建hash表
        ret = read_right_operate();
        if (OB_SUCC(ret)) {
          state_ = (0 == output_info_.selector_cnt_) ? JS_READ_RIGHT : JS_PROBE_RESULT;
        } else if (OB_ITER_END == ret) {
          state_ = output_unmatch_left() && !read_null_in_naaj_ ? JS_LEFT_UNMATCH_RESULT : JS_JOIN_END;
          ret = OB_SUCCESS;
        }
        break;
      }
      case JS_PROBE_RESULT: {
        ret = probe();
        if (OB_ITER_END == ret) {
          state_ = JS_READ_RIGHT;
          ret = OB_SUCCESS;
        }
        break;
      }
      case JS_LEFT_UNMATCH_RESULT: {
        ret = fill_left_unmatched_result();
        if (OB_ITER_END == ret) {
          state_ = JS_JOIN_END;
          ret = OB_SUCCESS;
        }
        break;
      }
      case JS_JOIN_END: {
        if (HJProcessor::NEST_LOOP == hj_processor_ && HJLoopState::LOOP_GOING == nest_loop_state_
            && !read_null_in_naaj_ && !is_shared_) {
          if (nullptr != right_part_) {
            if (OB_FAIL(right_part_->begin_iterator())) {
              LOG_WARN("fail to begin iterator", K(right_part_), K(ret));
            }
          }
          state_ = JS_PROCESS_LEFT;
        } else {
          ret = OB_ITER_END;
        }
        break;
      }
    }
  }

  return ret;
}

int ObHashJoinVecOp::process_left(bool &need_not_read_right)
{
  int ret= OB_SUCCESS;
  need_not_read_right = false;
  int64_t num_left_rows = 0;
  if (HJProcessor::NEST_LOOP == hj_processor_) {
    if (OB_FAIL(nest_loop_process(num_left_rows))) {
      LOG_WARN("build hash table failed", K(ret));
    }
  } else if (OB_FAIL(adaptive_process(num_left_rows))) {
    LOG_WARN("build hash table failed", K(ret));
  }

  if (OB_SUCC(ret)
     && !is_shared_
     && ((0 == num_left_rows
         && RIGHT_ANTI_JOIN != MY_SPEC.join_type_
         && RIGHT_OUTER_JOIN != MY_SPEC.join_type_
         && FULL_OUTER_JOIN != MY_SPEC.join_type_) || read_null_in_naaj_)) {
     // 由于左表为空，不需要再读右表，模拟右表已经读完的状态
     need_not_read_right = true;
     if (HJProcessor::NEST_LOOP == hj_processor_) {
       nest_loop_state_ = HJLoopState::LOOP_END;
     }
     LOG_DEBUG("[HASH JOIN]Left table is empty, skip reading right table.",
         K(num_left_rows), K(MY_SPEC.join_type_), K(read_null_in_naaj_), K(need_not_read_right));
   }

  return ret;
}

// copy ObOperator::drain_exch
// It's same as the base operator, but only need add sync to wait exit for shared hash join
int ObHashJoinVecOp::do_drain_exch()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  /**
   * 1. try to open this operator
   * 2. try to drain all children
   */
  if (OB_FAIL(try_open())) {
    LOG_WARN("fail to open operator", K(ret));
  } else if (!exch_drained_) {
    // don't sync to wait all when operator call drain_exch by self
    tmp_ret = do_sync_wait_all();
    exch_drained_ = true;
    for (int64_t i = 0; i < child_cnt_ && OB_SUCC(ret); i++) {
      if (OB_ISNULL(children_[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL child found", K(ret), K(i));
      } else if (OB_FAIL(children_[i]->drain_exch())) {
        LOG_WARN("drain exch failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to do sync wait all", K(ret));
      }
    }
  }
  return ret;
}

void ObHashJoinVecOp::destroy()
{
  sql_mem_processor_.unregister_profile_if_necessary();
  jt_ctx_.reset();
  if (OB_LIKELY(nullptr != alloc_)) {
    alloc_ = nullptr;
  }
  if (OB_LIKELY(NULL != mem_context_)) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = NULL;
  }
  ObJoinVecOp::destroy();
}

int ObHashJoinVecOp::inner_close()
{
  int ret = OB_SUCCESS;
  sql_mem_processor_.unregister_profile();
  if (is_shared_) {
    IGNORE_RETURN sync_wait_close();
  }
  reset();
  if (part_mgr_ != NULL) {
    part_mgr_->~ObHJPartitionMgr();
    if (OB_NOT_NULL(alloc_)) {
      alloc_->free(part_mgr_);
    }
    part_mgr_ = NULL;
  }
  //right_last_row_.reset();
  if (nullptr != alloc_) {
    join_table_.free(alloc_);
  }
  if (nullptr != mem_context_) {
    mem_context_->reuse();
  }
  if (OB_FAIL(ObJoinVecOp::inner_close())) {
  }
  return ret;
}

int ObHashJoinVecOp::get_next_left_row_batch(bool is_from_row_store, const ObBatchRows *&child_brs)
{
  int ret = OB_SUCCESS;
  bool is_left = true;
  if (!is_from_row_store) {
    if (OB_FAIL(left_->get_next_batch(max_output_cnt_, child_brs))) {
      LOG_WARN("get left row from child failed", K(ret));
    } else if (child_brs->end_ && 0 == child_brs->size_) {
      // do nothing
    } else if (MY_SPEC.is_naaj_) {
      bool has_null = false;
      if (FALSE_IT(non_preserved_side_is_not_empty_
                   |= (RIGHT_ANTI_JOIN == MY_SPEC.join_type_))) {
      // mark this to forbid null value output in get_next_right_batch_na
      } else if (is_right_naaj()
                && OB_FAIL(check_join_key_for_naaj_batch(is_left, has_null, child_brs))) {
        LOG_WARN("failed to check null for right naaj", K(ret));
      } else if (has_null) {
        read_null_in_naaj_ = true;
        ret = OB_ITER_END;
        if (is_shared_) {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = left_->drain_exch())) {
            LOG_WARN("failed to drain exch", K(ret), K(tmp_ret));
            ret = tmp_ret;
          }
        }
        LOG_TRACE("right naaj null break", K(ret), K(spec_.id_));
      }
    }
  } else {
    int64_t read_size = 0;
    child_brs = &child_brs_;
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(left_part_->get_next_batch(
                       left_->get_spec().output_, eval_ctx_, MY_SPEC.max_batch_size_,
                       read_size, part_stored_rows_))) {
      if (OB_ITER_END == ret) {
        const_cast<ObBatchRows *>(child_brs)->size_ = 0;
        const_cast<ObBatchRows *>(child_brs)->end_ = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next batch", K(ret));
      }
    } else {
      const_cast<ObBatchRows *>(child_brs)->size_ = read_size;
      const_cast<ObBatchRows *>(child_brs)->end_ = false;
      const_cast<ObBatchRows *>(child_brs)->skip_->reset(read_size);
    }
    LOG_DEBUG("part join ctx get left row", K(ret));
  }
  return ret;
}

int ObHashJoinVecOp::reuse_for_next_chunk()
{
  int ret = OB_SUCCESS;
  // first time, the bucket number is already calculated
  // calculate the buckets number of hash table
  if (top_part_level() || 0 == cur_join_table_->get_nbuckets()
    || NEST_LOOP != hj_processor_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected hash buckets number is 0", K(ret), K(part_level_));
  } else if (OB_FAIL(calc_basic_info())) {
    LOG_WARN("failed to calc basic info", K(ret));
  } else {
    // reuse buckets
    OZ(cur_join_table_->build_prepare(jt_ctx_, profile_.get_row_count(), profile_.get_bucket_size()));
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(right_part_->rescan())) {
      LOG_WARN("failed to rescan right", K(ret));
    }
    LOG_TRACE("trace hash table", K(ret), K(profile_.get_bucket_size()),
                                  K(profile_.get_row_count()), K(spec_.id_));
  }
  return ret;
}

int ObHashJoinVecOp::load_next()
{
  int ret = OB_SUCCESS;
  ++nth_nest_loop_;
  // 目前通过设置一定读固定大小方式来读取内容，后续会改掉
  if (1 == nth_nest_loop_ && OB_FAIL(left_part_->begin_iterator())) {
    LOG_WARN("failed to set iterator", K(ret), K(nth_nest_loop_));
  } else if (1 == nth_nest_loop_ && OB_FAIL(prepare_hash_table())) {
    LOG_WARN("failed to prepare hash table", K(ret), K(nth_nest_loop_));
  } else if (1 < nth_nest_loop_ && OB_FAIL(reuse_for_next_chunk())) {
    LOG_WARN("failed to reset info for block", K(ret), K(nth_nest_loop_));
  }
  return ret;
}

int ObHashJoinVecOp::build_hash_table_for_nest_loop(int64_t &num_left_rows)
{
  int ret = OB_SUCCESS;
  num_left_rows = 0;
  nest_loop_state_ = HJLoopState::LOOP_GOING;
  is_last_chunk_ = false;
  JoinHashTable &hash_table = join_table_;
  ObHJPartition *hj_part = left_part_;
  if (OB_FAIL(load_next())) {
    LOG_WARN("failed to reset info for block", K(ret));
  } else {
    // at least hold 1 block in memory
    int64_t memory_bound = std::max(remain_data_memory_size_,
                                    hj_part->get_row_store().get_max_blk_size());
    const int64_t row_bound = min(hash_table.get_row_count(), hash_table.get_nbuckets() / 2);
    hj_part->set_iteration_age(iter_age_);
    iter_age_.inc();
    JoinPartitionRowIter left_iter(hj_part, row_bound, memory_bound);
    ret = hash_table.build(left_iter, jt_ctx_);
  }
  if (OB_SUCC(ret)) {
    num_left_rows = hash_table.get_row_count();
    trace_hash_table_collision(num_left_rows);
    if (!hj_part->has_next()) {
      is_last_chunk_ = true;
      nest_loop_state_ = HJLoopState::LOOP_END;
    }
  }
  LOG_TRACE("trace block hash join", K(nth_nest_loop_), K(part_level_), K(ret), K(num_left_rows),
            K(join_table_.get_nbuckets()), K(spec_.id_));
  return ret;
}

int ObHashJoinVecOp::nest_loop_process(int64_t &num_left_rows)
{
  int ret = OB_SUCCESS;
  num_left_rows = 0;
  if (OB_FAIL(build_hash_table_for_nest_loop(num_left_rows))) {
    LOG_WARN("failed to build hash table", K(ret), K(part_level_));
  }

  return ret;
}

int64_t ObHashJoinVecOp::calc_partition_count_by_cache_aware(
  int64_t row_count, int64_t max_part_count, int64_t global_mem_bound_size)
{
  int64_t row_count_cache_aware = INIT_L2_CACHE_SIZE/ PRICE_PER_ROW;
  int64_t partition_cnt = next_pow2(row_count / row_count_cache_aware);
  if (max_part_count < partition_cnt) {
    partition_cnt = max_part_count;
  }
  global_mem_bound_size = max(0, global_mem_bound_size);
  while (partition_cnt * PAGE_SIZE > global_mem_bound_size) {
    partition_cnt >>= 1;
  }
  partition_cnt = partition_cnt < MIN_PART_COUNT ?
                        MIN_PART_COUNT :
                        partition_cnt;
  return partition_cnt;
}

int64_t ObHashJoinVecOp::calc_max_data_size(const int64_t extra_memory_size)
{
  int64_t expect_size = profile_.get_expect_size();
  int64_t data_size = expect_size - extra_memory_size;
  if (expect_size < profile_.get_cache_size()) {
    data_size = expect_size *
            (profile_.get_cache_size() - extra_memory_size) / profile_.get_cache_size();
  }
  if (MIN_MEM_SIZE >= data_size) {
    data_size = MIN_MEM_SIZE;
  }
  return data_size;
}

// 1 manual，get memory size by _hash_area_size
// 2 auto, get memory size by sql memory manager
int ObHashJoinVecOp::get_max_memory_size(int64_t input_size)
{
  int ret = OB_SUCCESS;
  int64_t hash_area_size = 0;
  const int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  // default data memory size: 80%
  int64_t extra_memory_size = get_extra_memory_size();
  int64_t memory_size = (extra_memory_size + input_size) < 0 ? input_size : (extra_memory_size + input_size);
  if (OB_FAIL(ObSqlWorkareaUtil::get_workarea_size(
      ObSqlWorkAreaType::HASH_WORK_AREA, tenant_id, &ctx_, hash_area_size))) {
    LOG_WARN("failed to get workarea size", K(ret), K(tenant_id));
  } else if (FALSE_IT(remain_data_memory_size_ = hash_area_size * 80 / 100)) {
    // default data memory size: 80%
  } else if (OB_FAIL(sql_mem_processor_.init(
      alloc_, tenant_id, memory_size, MY_SPEC.type_, MY_SPEC.id_, &ctx_))) {
    LOG_WARN("failed to init sql mem mgr", K(ret));
  } else if (sql_mem_processor_.is_auto_mgr()) {
    remain_data_memory_size_ = calc_max_data_size(extra_memory_size);
    part_count_ = calc_partition_count_by_cache_aware(
      profile_.get_row_count(), MAX_PART_COUNT_PER_LEVEL, memory_size);
    if (!top_part_level()) {
      if (OB_ISNULL(left_part_) || OB_ISNULL(right_part_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect status: left op or right op is null", K(left_part_),
          K(right_part_));
      } else {
        // switch callback for count memory size
        //left_part_->set_callback(&sql_mem_processor_);
      }
    }
    LOG_TRACE("trace auto memory manager", K(hash_area_size), K(part_count_),
      K(input_size), K(extra_memory_size), K(profile_.get_expect_size()),
      K(profile_.get_cache_size()), K(spec_.id_));
  } else {
    part_count_ = calc_partition_count_by_cache_aware(
      profile_.get_row_count(), MAX_PART_COUNT_PER_LEVEL, sql_mem_processor_.get_mem_bound());
    LOG_TRACE("trace auto memory manager", K(hash_area_size), K(part_count_),
      K(input_size), K(spec_.id_));
  }
  data_ratio_ = 1.0;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sync_wait_part_count())) {
      LOG_WARN("failed to sync part count", K(ret));
    }
  }
  char *buf = NULL;
  if (OB_FAIL(ret)) {
  } else if (part_count_ <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(part_count_));
  } else if (NULL == (buf = (char *)mem_context_->get_malloc_allocator().alloc(
             sizeof(uint16_t) * (MY_SPEC.max_batch_size_ * part_count_ + part_count_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(MY_SPEC.max_batch_size_), K(part_count_));
  } else {
    part_selectors_ = reinterpret_cast<uint16_t *>(buf);
    part_selector_sizes_ = reinterpret_cast<uint16_t *>(buf +
                           sizeof(uint16_t) * (MY_SPEC.max_batch_size_ * part_count_));
  }

  return ret;
}

int64_t ObHashJoinVecOp::calc_bucket_number(const int64_t row_count)
{
  int64_t bucket_cnt = next_pow2(row_count * RATIO_OF_BUCKETS);
  // some test data from ssb:
  //    extra_ratio = 2 -> cpu used downgrades 16.7% from 24.6%
  //    extra_ratio = 4 -> cpu used downgrades 13.9% from 16.7%
  int64_t one_bucket_size = cur_join_table_->get_one_bucket_size();
  if (bucket_cnt * 2 * one_bucket_size <= INIT_L2_CACHE_SIZE) {
    bucket_cnt *= 2;
  }
  return bucket_cnt;
}

// calculate row_count, input_size, bucket_num, and set to profile
int ObHashJoinVecOp::calc_basic_info(bool global_info)
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
        LOG_TRACE("trace left row count", K(row_count), K(spec_.id_));
        if (row_count < MIN_ROW_COUNT) {
          row_count = MIN_ROW_COUNT;
        }
        // estimated rows: left_row_count * row_size
        input_size = row_count * left_->get_spec().width_;
      }
    } else {
      if (OB_ISNULL(left_part_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("left op is null", K(ret));
      } else {
        // use actual value
        // it need to be considered swapping left and right
        input_size = left_part_->get_size_on_disk();
        row_count = left_part_->get_row_count_on_disk();
      }
    }
  } else if (RECURSIVE == hj_processor_) {
    if (nullptr != left_part_array_ && nullptr != right_part_array_) {
      if (global_info) {
        if (!is_shared_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: global info must be shared hash join", K(ret));
        } else {
          ObHashJoinVecInput *hj_input = static_cast<ObHashJoinVecInput*>(input_);
          row_count = hj_input->get_total_memory_row_count();
          input_size = hj_input->get_total_memory_size();
          LOG_DEBUG("debug row_count and input_size",
            K(hj_input->get_total_memory_row_count()),
            K(hj_input->get_total_memory_size()));
        }
      } else {
        for (int64_t i = 0; i < part_count_; ++i) {
          row_count += left_part_array_[i]->get_row_count_in_memory();
          input_size += left_part_array_[i]->get_size_in_memory();
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect path for calculate bucket number", K(ret));
    }
  } else if (IN_MEMORY == hj_processor_) {
    if (global_info) {
      if (!is_shared_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: global info must be shared hash join", K(ret));
      } else {
        ObHashJoinVecInput *hj_input = static_cast<ObHashJoinVecInput*>(input_);
        row_count = hj_input->get_total_memory_row_count();
        input_size = hj_input->get_total_memory_size();
        LOG_DEBUG("debug hj_input row_count and input_size",
          K(hj_input->get_total_memory_row_count()),
          K(hj_input->get_total_memory_size()));
      }
    } else if (nullptr != left_part_) {
      row_count = left_part_->get_row_store().get_row_cnt();
      // INMEMORY processor, read entire partition into memory
      input_size = left_part_->get_row_store().get_file_size();
      LOG_DEBUG("debug left_batch row_count and input_size",
                 K(left_part_->get_row_store().get_row_cnt()),
                 K(left_part_->get_row_store().get_file_size()));
    }
    LOG_DEBUG("debug row_count and input_size", K(row_count), K(global_info), K(is_shared_));
  } else if (nullptr != left_part_) {
    // NESTLOOP processor, our memory is not enough to hold entire left partition,
    // try to estimate max row count we can hold
    row_count = 0;
    if (left_part_->get_row_store().get_row_cnt() > 0) {
      double avg_len = static_cast<double> (left_part_->get_row_store().get_file_size())
                        / left_part_->get_row_store().get_row_cnt();
      row_count = std::max(static_cast<int64_t> (MIN_BATCH_ROW_CNT_NESTLOOP),
                                    static_cast<int64_t> (remain_data_memory_size_ / avg_len));
    } else {
      row_count = MIN_BATCH_ROW_CNT_NESTLOOP;
    }
    LOG_TRACE("calc row count", K(left_part_->get_row_store().get_file_size()),
                                K(left_part_->get_row_store().get_row_cnt()),
                                K(remain_data_memory_size_), K(spec_.id_));
    input_size = remain_data_memory_size_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect path for calculate bucket number");
  }
  if (OB_SUCC(ret)) {
    out_row_count = row_count;
    buckets_number = calc_bucket_number(row_count);
    // save basic info
    profile_.set_basic_info(out_row_count, input_size, buckets_number);
    LOG_DEBUG("profile set basic info", K(out_row_count), K(input_size), K(buckets_number));
  }
  return ret;
}

int ObHashJoinVecOp::get_processor_type()
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
    if (OB_ISNULL(left_part_) || OB_ISNULL(right_part_)
        || 0 != left_part_->get_size_in_memory() || 0 != right_part_->get_size_in_memory()) {
      ret = OB_ERR_UNEXPECTED;
      if (OB_NOT_NULL(left_part_)) {
        LOG_WARN("unexpect: partition has memory row", K(left_part_->get_size_in_memory()));
      }
      if (OB_NOT_NULL(right_part_)) {
        LOG_WARN("unexpect: partition has memory row", K(right_part_->get_size_in_memory()));
      }
      LOG_WARN("unexpect: partition is null or partition has memory row", K(ret));
    } else if (enable_in_memory && all_in_memory(left_part_->get_size_on_disk())) {
      set_processor(IN_MEMORY);
      l_size = left_part_->get_size_on_disk();
    } else {
      l_size = left_part_->get_size_on_disk();
      r_size = right_part_->get_size_on_disk();
      static const int64_t UNIT_COST = 1;
      static const int64_t READ_COST = UNIT_COST;
      static const int64_t WRITE_COST = 2 * UNIT_COST;
      static const int64_t DUMP_RATIO = 70;
      // 2 read and 1 write for left and right
      // 这里假设dump的数据是内存部分与left dump部分的比例，即right也可以过滤成比例的数据
      recursive_cost = READ_COST * (l_size + r_size)
                        + (READ_COST + WRITE_COST) * (1 - 0.9 * remain_data_memory_size_ / l_size)
                        * (l_size + r_size);
      nest_loop_count = l_size / (remain_data_memory_size_ - 1) + 1;
      nest_loop_cost = (l_size + nest_loop_count * r_size) * READ_COST;
      pre_total_size = left_part_->get_pre_total_size();
      // 认为skew场景
      // 1. 占比超过总数的70%
      // 2. 第二轮开始按照了真实size进行partitioning，理论上第三轮one pass会结束
      //      所以在分区个数不是max_partition_count情况下，认为到了level比较高，认为可能有skew
      is_skew = (pre_total_size * DUMP_RATIO / 100 < l_size)
              || (3 <= part_level_ && MAX_PART_COUNT_PER_LEVEL != left_part_->get_pre_part_count()
                  && pre_total_size * 30 / 100 < l_size);
      if (enable_recursive && recursive_cost < nest_loop_cost
          && !is_skew
          && MAX_PART_LEVEL > part_level_) {
        // case 2: recursive process
        set_processor(RECURSIVE);
      } else if (enable_nest_loop) {
        // case 3: nest loop process
        if (!need_right_bitset()
            && MAX_NEST_LOOP_RIGHT_ROW_COUNT >= right_part_->get_row_count_on_disk()
            && !is_shared_) {
          set_processor(NEST_LOOP);
        } else {
          set_processor(RECURSIVE);
        }
      }
    }
    // if only one processor, then must choose it, for test
    if (enable_nest_loop && !enable_in_memory && !enable_recursive && !is_shared_) {
      set_processor(NEST_LOOP);
    } else if (!enable_nest_loop && enable_in_memory && !enable_recursive) {
      // force remain more memory
      int64_t hash_area_size = 0;
      if (OB_FAIL(ObSqlWorkareaUtil::get_workarea_size(
          ObSqlWorkAreaType::HASH_WORK_AREA,
          ctx_.get_my_session()->get_effective_tenant_id(), &ctx_, hash_area_size))) {
        LOG_WARN("failed to get workarea size", K(ret));
      }
      remain_data_memory_size_ = hash_area_size * 10;
      data_ratio_ = 1.0;
      set_processor(IN_MEMORY);
    } else if (!enable_nest_loop && !enable_in_memory
        && enable_recursive && MAX_PART_LEVEL > part_level_) {
      set_processor(RECURSIVE);
    } else if (NONE == hj_processor_) {
      if (MAX_PART_LEVEL >= part_level_) {
        if (is_shared_) {
          // NEST_LOOP mode not support shared hash join
          set_processor(RECURSIVE);
          LOG_TRACE("shared hash join not support NEST_LOOP mode, set to RECURSIVE mode.",
            K(is_shared_), K(part_level_), K(hj_processor_),
            K(part_level_), K(part_count_), K(cur_join_table_->get_nbuckets()),
            K(pre_total_size), K(recursive_cost), K(nest_loop_cost), K(is_skew),
            K(l_size), K(r_size), K(spec_.id_));
        } else {
          set_processor(NEST_LOOP);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no processor", K(part_level_), K(hj_processor_),
          K(part_level_), K(part_count_), K(cur_join_table_->get_nbuckets()),
          K(pre_total_size), K(recursive_cost), K(nest_loop_cost), K(is_skew),
          K(l_size), K(r_size));
      }
    }
  } else {
    set_processor(RECURSIVE);
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sync_wait_processor_type())) {
      LOG_WARN("failed to sync hj_processor_", K(ret), K(hj_processor_));
    }
  }
  LOG_TRACE("hash join process type", K(part_level_), K(hj_processor_),
      K(part_level_), K(part_count_), K(cur_join_table_->get_nbuckets()),
      K(pre_total_size), K(recursive_cost), K(nest_loop_cost), K(is_skew),
      K(l_size), K(r_size), K(remain_data_memory_size_), K(profile_.get_expect_size()),
      K(profile_.get_bucket_size()), K(profile_.get_cache_size()), K(profile_.get_row_count()),
      K(profile_.get_input_size()), K(left_->get_spec().width_), K(spec_.id_));
  return ret;
}

int ObHashJoinVecOp::build_hash_table_in_memory(int64_t &num_left_rows)
{
  int ret = OB_SUCCESS;
  num_left_rows = 0;
  ObHJPartition *hj_part = left_part_;
  JoinHashTable &hash_table = *cur_join_table_;
  if (OB_FAIL(hj_part->begin_iterator())) {
    LOG_WARN("failed to set iterator", K(ret));
  } else if (OB_FAIL(prepare_hash_table())) {
    LOG_WARN("failed to prepare hash table", K(ret));
  } else {
    hj_part->set_iteration_age(iter_age_);
    iter_age_.inc();
    JoinPartitionRowIter left_iter(hj_part);
    ret = hash_table.build(left_iter, jt_ctx_);
  }

  if (OB_SUCC(ret)) {
    num_left_rows = hash_table.get_row_count();
    trace_hash_table_collision(num_left_rows);
    is_last_chunk_ = true;
    if (is_shared_) {
      if (OB_FAIL(sync_wait_finish_build_hash())) {
        LOG_WARN("failed to wait finish build hash", K(ret));
      }
    }
  }

  LOG_TRACE("trace to finish build hash table in memory", K(ret), K(num_left_rows),
    K(hj_part->get_row_count_on_disk()), K(cur_join_table_->get_nbuckets()), K(spec_.id_));
  return ret;
}

int ObHashJoinVecOp::init_left_vectors()
{
  int ret = OB_SUCCESS;
  const ExprFixedArray &exprs = left_->get_spec().output_;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    const ObExpr *expr = exprs.at(i);
    ret = expr->init_vector_default(eval_ctx_, MY_SPEC.max_batch_size_);
  }
  return ret;
}

int ObHashJoinVecOp::in_memory_process(int64_t &num_left_rows)
{
  int ret = OB_SUCCESS;
  num_left_rows = 0;
  if (OB_FAIL(build_hash_table_in_memory(num_left_rows))) {
    LOG_WARN("failed to build hash table", K(ret), K(part_level_));
  }

  return ret;
}

int ObHashJoinVecOp::create_partition(bool is_left, int64_t part_id, ObHJPartition *&part)
{
  int ret = OB_SUCCESS;
  int64_t tmp_batch_round = part_round_;
  int64_t part_shift = part_shift_;
  part_shift += min(__builtin_ctz(part_count_), 8);
  if (OB_FAIL(part_mgr_->get_or_create_part(part_level_, part_shift,
                              (tmp_batch_round << 32) + part_id, is_left, part))) {
    LOG_WARN("fail to get partition", K(ret), K(part_level_), K(part_id), K(is_left));
  } else if (OB_FAIL(part->init(is_left ? left_->get_spec().output_ : right_->get_spec().output_,
                                MY_SPEC.max_batch_size_, MY_SPEC.compress_type_))) {
    LOG_WARN("fail to init batch", K(ret), K(part_level_), K(part_id), K(is_left));
  } else {
    part->get_row_store().set_dir_id(sql_mem_processor_.get_dir_id());
    part->get_row_store().set_io_event_observer(&io_event_observer_);
    LOG_DEBUG("debug init batch", K(part_level_), K(part_id),
      K((tmp_batch_round << 32) + part_id), K(tmp_batch_round));
  }

  return ret;
}

int ObHashJoinVecOp::init_join_partition()
{
  int ret = OB_SUCCESS;
  if (0 >= part_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition count is less then 0", K(part_count_), K(ret));
  } else {
    char *mem = static_cast<char *>(alloc_->alloc(sizeof(ObHJPartition *) * part_count_ * 2));
    if (NULL == mem) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem for hj part");
    } else {
      left_part_array_ = reinterpret_cast<ObHJPartition **>(mem);
      right_part_array_ = reinterpret_cast<ObHJPartition **>(mem + sizeof(ObHJPartition *) * part_count_);
      for (int64_t i = 0; i < part_count_ && OB_SUCC(ret); i ++) {
        if (OB_FAIL(create_partition(true/*is_left*/, i /*part_id*/, left_part_array_[i]))) {
          LOG_WARN("fail to create partition", K(ret), K(i));
        } else if (OB_FAIL(create_partition(false/*is_left*/, i/*part_id*/, right_part_array_[i]))) {
          LOG_WARN("fail to create partition", K(ret), K(i));
        }
      } // for end
    }
  }
  if (nullptr != left_part_) {
    LOG_TRACE("trace init partition", K(part_count_), K(part_level_),
      K(left_part_->get_part_level()), K(left_part_->get_partno()), K(spec_.id_));
  } else {
    LOG_TRACE("trace init partition", K(part_count_), K(part_level_), K(spec_.id_));
  }
  return ret;
}

int ObHashJoinVecOp::force_dump(bool for_left)
{
  return finish_dump(for_left, true, true);
}

void ObHashJoinVecOp::update_remain_data_memory_size(
  int64_t row_count,
  int64_t total_mem_size, bool &tmp_need_dump)
{
  double ratio = 1.0;
  need_more_remain_data_memory_size(row_count, total_mem_size, ratio);
  int64_t estimate_remain_size = total_mem_size * ratio;
  remain_data_memory_size_ = estimate_remain_size;
  data_ratio_ = ratio;
  tmp_need_dump = need_dump();
  LOG_TRACE("trace need more remain memory size", K(total_mem_size), K(row_count),
    K(estimate_remain_size), K(tmp_need_dump), K(spec_.id_));
}

bool ObHashJoinVecOp::need_more_remain_data_memory_size(
  int64_t row_count,
  int64_t total_mem_size,
  double &data_ratio)
{
  int64_t extra_memory_size = 0;
  int64_t bucket_cnt = 0;
  bucket_cnt = calc_bucket_number(row_count);
  extra_memory_size += bucket_cnt * cur_join_table_->get_one_bucket_size();
  int64_t predict_total_memory_size = extra_memory_size + get_data_mem_used();
  bool need_more = (total_mem_size < predict_total_memory_size);
  double guess_data_ratio = 0;
  if (0 < get_data_mem_used()) {
    guess_data_ratio = 1.0 * get_data_mem_used() / predict_total_memory_size;
    data_ratio = guess_data_ratio;
  } else {
      // by default, it's .9, it may reduce after process data
      // and especially if we has only little memroy(eg: 1M),
      // and partition chunk has already 0.5M(8 * 64K),
      // so we need more data memory
      data_ratio = MAX(guess_data_ratio, 0.9);
  }
  LOG_TRACE("trace need more remain memory size", K(total_mem_size), K(predict_total_memory_size),
    K(extra_memory_size), K(bucket_cnt), K(row_count), K(data_ratio), K(get_mem_used()),
    K(guess_data_ratio),  K(get_data_mem_used()), K(sql_mem_processor_.get_mem_bound()), K(lbt()), K(spec_.id_));
  return need_more;
}

int ObHashJoinVecOp::update_remain_data_memory_size_periodically(int64_t row_count, bool &need_dump, bool force_update)
{
  int ret = OB_SUCCESS;
  bool updated = false;
  need_dump = false;
  // if force_update, then periodic_cnt has mulitply 2, and update 64 times that periodic_cnt is overflow
  int64_t tmp_periodic_row_count = sql_mem_processor_.get_periodic_cnt();
  if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
    alloc_, [&](int64_t cur_cnt){ return force_update || row_count > cur_cnt; }, updated))) {
    LOG_WARN("failed to update max usable memory size periodically", K(ret), K(row_count));
  } else if (updated) {
    update_remain_data_memory_size(
      row_count, sql_mem_processor_.get_mem_bound(), need_dump);
    if (force_update) {
      sql_mem_processor_.set_periodic_cnt(tmp_periodic_row_count);
    }
    LOG_TRACE("trace need more remain memory size", K(profile_.get_expect_size()),
      K(row_count), K(force_update), K(spec_.id_));
  }
  return ret;
}

// Dump one buffer per partition and one by one partition
// when dump one buffer of the partition, then nextly dump buffer pf the next partition
// and dump the partition again, then asynchronously wait to write that overlap the dump other partitions
// If dumped_size is max, then dump all
int ObHashJoinVecOp::asyn_dump_partition(
  int64_t dumped_size,
  bool is_left,
  bool dump_all,
  int64_t start_dumped_part_idx,
  PredFunc pred)
{
  int ret = OB_SUCCESS;
  bool tmp_need_dump = false;
  // firstly find all need dumped partition
  int64_t pre_total_dumped_size = 0;
  int64_t last_dumped_partition_idx = start_dumped_part_idx;
  bool tmp_dump_all = (INT64_MAX == dumped_size) || dump_all;
  ObHJPartition **dumped_parts = nullptr;
  if (is_left) {
    dumped_parts = left_part_array_;
  } else {
    dumped_parts = right_part_array_;
  }
  for (int64_t i = part_count_ - 1; i >= start_dumped_part_idx && OB_SUCC(ret); --i) {
    ObHJPartition *dump_part = dumped_parts[i];
    // don'e dump last buffer
    if ((nullptr == pred || pred(i))) {
      if (tmp_dump_all) {
        pre_total_dumped_size += dump_part->get_size_in_memory();
      } else {
        pre_total_dumped_size += dump_part->get_size_in_memory() - dump_part->get_last_buffer_mem_size();
      }
      if (pre_total_dumped_size > dumped_size) {
        last_dumped_partition_idx = i;
        break;
      }
    }
  }
  LOG_TRACE("debug dump partition", K(is_left), K(start_dumped_part_idx),
    K(last_dumped_partition_idx), K(cur_dumped_partition_),
    K(pre_total_dumped_size), K(dumped_size), K(dump_all), K(lbt()), K(spec_.id_));
  // secondly dump one buffer per partiton one by one
  bool finish_dump = false;
  while (OB_SUCC(ret) && !finish_dump) {
    finish_dump = true;
    for (int64_t i = last_dumped_partition_idx; i < part_count_ && OB_SUCC(ret); ++i) {
      ObHJPartition *dump_part = dumped_parts[i];
      bool tmp_part_dump = dump_part->is_dumped();
      if ((nullptr == pred || pred(i)) && (dump_part->has_switch_block() || (tmp_dump_all && 0 < dump_part->get_size_in_memory()))) {
        if (OB_FAIL(dump_part->dump(tmp_dump_all, 1))) {
          LOG_WARN("failed to dump partition", K(part_level_), K(i));
        } else if (dump_part->is_dumped() && !tmp_part_dump) {
          // 设置一个limit，后续自动dump
          dump_part->set_memory_limit(1);
          sql_mem_processor_.set_number_pass(part_level_ + 1);
          // recalculate available memory bound after dump one partition
          if (tmp_dump_all && sql_mem_processor_.is_auto_mgr()) {
            if (OB_FAIL(calc_basic_info())) {
              LOG_WARN("failed to calc basic info", K(ret));
            } else if (OB_FAIL(update_remain_data_memory_size_periodically(
                profile_.get_row_count(), tmp_need_dump, tmp_dump_all))) {
              LOG_WARN("failed to update remain memory size periodically", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret) && (nullptr == pred || pred(i)) &&
          (dump_part->has_switch_block() || (tmp_dump_all && 0 < dump_part->get_size_in_memory()))) {
        finish_dump = false;
      }
    } // end for
  } // end while
  LOG_TRACE("debug finish dump partition", K(is_left), K(start_dumped_part_idx),
    K(last_dumped_partition_idx), K(cur_dumped_partition_), K(lbt()), K(spec_.id_));
  if (OB_SUCC(ret) && last_dumped_partition_idx - 1 < cur_dumped_partition_) {
    cur_dumped_partition_ = last_dumped_partition_idx - 1;
  }
  return ret;
}

int ObHashJoinVecOp::dump_build_table(int64_t row_count, bool force_update)
{
  int ret = OB_SUCCESS;
  bool tmp_need_dump = false;
  int64_t mem_used = get_cur_mem_used();
  if (OB_FAIL(update_remain_data_memory_size_periodically(
              row_count, tmp_need_dump, force_update))) {
    LOG_WARN("failed to update remain memory size periodically", K(ret));
  } else if (OB_LIKELY(need_dump(mem_used))) {
    // judge whether reach max memory bound size
    // every time expend 20%
    if (MAX_PART_COUNT_PER_LEVEL != cur_dumped_partition_) {
      // it has dumped already
      tmp_need_dump = true;
    } else if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(
      alloc_,
      [&](int64_t max_memory_size) {
        UNUSED(max_memory_size);
        update_remain_data_memory_size(
          row_count, sql_mem_processor_.get_mem_bound(), tmp_need_dump);
        return tmp_need_dump;
      },
      tmp_need_dump, mem_used))) {
      LOG_WARN("failed to extend max memory size", K(ret));
    }
    if (tmp_need_dump) {
      LOG_DEBUG("need dump",
        K(profile_.get_expect_size()),
        K(sql_mem_processor_.get_mem_bound()),
        K(cur_dumped_partition_),
        K(mem_used),
        K(data_ratio_));
      // dump from last partition to the first partition
      int64_t cur_dumped_partition = part_count_ - 1;
      if ((all_dumped() || need_dump(mem_used)) && 0 <= cur_dumped_partition && OB_SUCC(ret)) {
        // firstly find all need dumped partition
        int64_t dumped_size = get_need_dump_size(mem_used);
        if (OB_FAIL(asyn_dump_partition(dumped_size, true, false, 0, nullptr))) {
          LOG_WARN("failed to asyn dump table", K(ret));
        }
        LOG_TRACE("trace left need dump", K(part_level_),
          K(cur_dumped_partition),
          K(sql_mem_processor_.get_mem_bound()),
          K(cur_dumped_partition_),
          K(mem_used),
          K(data_ratio_),
          K(spec_.id_));
      }
    }
  }
  return ret;
}

int ObHashJoinVecOp::update_dumped_partition_statistics(bool is_left)
{
  int ret = OB_SUCCESS;
  int64_t total_size = 0;
  ObHJPartition **part_array = nullptr;
  if (is_left) {
    part_array = left_part_array_;
  } else {
    part_array = right_part_array_;
  }
  for (int64_t i = 0; i < part_count_ && OB_SUCC(ret); i ++) {
    total_size += part_array[i]->get_size_in_memory() + part_array[i]->get_size_on_disk();
  }
  for (int64_t i = part_count_ - 1; i >= 0 && OB_SUCC(ret); --i) {
    ObHJPartition *dumped_part = part_array[i];
    if (i > cur_dumped_partition_) {
      if (dumped_part->is_dumped()) {
        if (is_left) {
          right_part_array_[i]->set_memory_limit(1);
        }
        if (0 != dumped_part->get_size_in_memory()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expected memory size is 0", K(ret), K(i), K(dumped_part->get_size_in_memory()),
            K(part_count_), K(cur_dumped_partition_), K(dumped_part->get_row_count_in_memory()),
            K(is_left));
          if (!is_left) {
            LOG_WARN("left size", K(left_part_array_[i]->get_size_in_memory()),
              K(left_part_array_[i]->get_row_count_in_memory()),
              K(left_part_array_[i]->get_row_count_on_disk()),
              K(left_part_array_[i]->get_size_on_disk()));
          }
        } else if (0 == dumped_part->get_size_on_disk()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("disk size is 0", K(ret), K(i), K(dumped_part->get_size_on_disk()),
            K(part_count_), K(cur_dumped_partition_), K(dumped_part->get_row_count_in_memory()));
        }
      } else if (0 != dumped_part->get_size_in_memory()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expected memory size is 0", K(ret), K(i), K(dumped_part->get_size_in_memory()),
          K(part_count_), K(cur_dumped_partition_), K(dumped_part->get_row_count_in_memory()));
        if (!is_left) {
          LOG_WARN("left size", K(left_part_array_[i]->get_size_in_memory()),
            K(left_part_array_[i]->get_row_count_in_memory()),
            K(left_part_array_[i]->get_row_count_on_disk()),
            K(left_part_array_[i]->get_size_on_disk()));
        }
      }
    }
    if (OB_SUCC(ret)) {
      dumped_part->record_pre_batch_info(part_count_, cur_join_table_->get_nbuckets(), total_size);
    }
  } // for end
  return ret;
}

int ObHashJoinVecOp::do_sync_wait_all()
{
  int ret = OB_SUCCESS;
  if (!batch_reach_end_ && !row_reach_end_ && is_shared_) {
    // In shared hash join, it only has two state: one is that it don't build hash table,
    // and one is that it return result after probe
    // so we only do:
    // 1. sync to build hash table
    // 2. set join_end and sync to do next_batch

    // don't get any right data, we think the right is iter_end
    if (HJState::INIT == hj_state_) {
      // don't build hash table
      // it must build hash table to guarantee to hold all data
      drain_mode_ = HashJoinDrainMode::BUILD_HT;
    } else if (HJState::PROCESS_PARTITION == hj_state_) {
      //TODO shengle ??
      // set state_ = JOIN_END and going to get_next_row/batch_row
      state_ = JS_JOIN_END;
      drain_mode_ = HashJoinDrainMode::RIGHT_DRAIN;
      if (OB_FAIL(right_->drain_exch())) {
        LOG_WARN("drain exch failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: hash join state", K(state_), K(hj_state_));
    }
    if (OB_SUCC(ret)) {
      // sync others thread
      const ObBatchRows *child_brs = &child_brs_;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(get_next_batch(MY_SPEC.max_batch_size_, child_brs))) {
          if (OB_ITER_END == ret) {
            LOG_WARN("failed to inner get next row", K(ret));
          }
        } else if (brs_.end_) {
          break;
        }
      } // end while
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    LOG_TRACE("debug shared hash join wait all to drain", K(ret));
  }

  return ret;
}



int ObHashJoinVecOp::sync_wait_processor_type()
{
  int ret = OB_SUCCESS;
  ObHashJoinVecInput *hj_input = static_cast<ObHashJoinVecInput*>(input_);
  if (OB_ISNULL(hj_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: shared hash join info is null", K(ret));
  } else if (is_shared_) {
      if (OB_FAIL(hj_input->sync_wait(
          ctx_, hj_input->get_process_cnt(),
          [&](int64_t n_times){
            // RECURSIVE = 2,IN_MEMORY = 4. If hj_processor_ of any thread is RECURSIVE, set all thread to RECURSIVE.
            hj_input->sync_set_min(n_times, hj_processor_);
        }))) {
      LOG_WARN("failed to sync wait", K(ret));
    } else {
      // sync partion_count
      set_processor(static_cast<HJProcessor>(hj_input->get_sync_val()));
      LOG_TRACE("debug sync hj_processor_", K(hj_processor_), K(hj_input->get_sync_val()),
        K(spec_.id_));
    }
  }
  return ret;
}

int ObHashJoinVecOp::sync_wait_part_count()
{
  int ret = OB_SUCCESS;
  ObHashJoinVecInput *hj_input = static_cast<ObHashJoinVecInput*>(input_);
  if (OB_ISNULL(hj_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: shared hash join info is null", K(ret));
  } else if (is_shared_) {
      if (OB_FAIL(hj_input->sync_wait(
          ctx_, hj_input->get_process_cnt(),
          [&](int64_t n_times){
            hj_input->sync_set_max(n_times, part_count_);
        }))) {
      LOG_WARN("failed to sync wait", K(ret));
    } else {
      // sync partion_count
      part_count_ = hj_input->get_sync_val();
      LOG_TRACE("debug sync partition_count", K(part_count_), K(hj_input->get_sync_val()),
        K(spec_.id_));
    }
  }
  return ret;
}

int ObHashJoinVecOp::sync_wait_cur_dumped_partition_idx()
{
  int ret = OB_SUCCESS;
  ObHashJoinVecInput *hj_input = static_cast<ObHashJoinVecInput*>(input_);
  if (OB_ISNULL(hj_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: shared hash join info is null", K(ret));
  } else if (OB_FAIL(hj_input->sync_wait(
      ctx_, hj_input->get_process_cnt(),
      [&](int64_t n_times) {
        hj_input->sync_set_min(n_times, cur_dumped_partition_);
      }))) {
    LOG_WARN("failed to sync wait cur_dumped_partition", K(ret));
  } else {
    cur_dumped_partition_ = hj_input->get_sync_val();
    LOG_TRACE("debug cur_dumped_partition", K(hj_input->get_sync_val()),
      K(cur_dumped_partition_), K(spec_.id_));
  }
  return ret;
}

int ObHashJoinVecOp::sync_wait_basic_info(uint64_t &build_ht_thread_ptr)
{
  int ret = OB_SUCCESS;
  ObHashJoinVecInput *hj_input = static_cast<ObHashJoinVecInput*>(input_);
  if (OB_ISNULL(hj_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: shared hash join info is null", K(ret));
  } else if (OB_FAIL(hj_input->sync_wait(
      ctx_, hj_input->get_process_cnt(),
      [&](int64_t n_times) {
        hj_input->sync_basic_info(n_times, profile_.get_row_count(), profile_.get_input_size());
        hj_input->sync_set_first(n_times, reinterpret_cast<int64_t>(this));
        hj_input->sync_info_for_naaj(n_times, read_null_in_naaj_, non_preserved_side_is_not_empty_);
      }))) {
    LOG_WARN("failed to sync wait basic info", K(ret));
  } else if (OB_FAIL(calc_basic_info(true))) {
    LOG_WARN("failed to calc basic info", K(ret));
  } else {
    build_ht_thread_ptr = hj_input->get_sync_val();
    read_null_in_naaj_ = hj_input->get_null_in_naaj();
    non_preserved_side_is_not_empty_ = hj_input->get_non_preserved_side_naaj();
    LOG_TRACE("debug sync basic info", K(spec_.id_));
  }
  return ret;
}

int ObHashJoinVecOp::sync_wait_init_build_hash(const uint64_t build_ht_thread_ptr)
{
  int ret = OB_SUCCESS;
  ObHashJoinVecInput *hj_input = static_cast<ObHashJoinVecInput*>(input_);
  if (OB_ISNULL(hj_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: shared hash join info is null", K(ret));
  } else if (OB_FAIL(hj_input->sync_wait(
      ctx_, hj_input->get_process_cnt(),
      [&](int64_t n_times) {
        UNUSED(n_times);
      }))) {
    LOG_WARN("failed to sync wait init build hash", K(ret));
  } else {
    ObHashJoinVecOp *build_hj_op = reinterpret_cast<ObHashJoinVecOp*>(build_ht_thread_ptr);
    // use the same hash table
    cur_join_table_ = part_round_ <= 1 ? &(build_hj_op->get_hash_table()) : cur_join_table_;
    LOG_TRACE("debug sync wait init build hash", K(cur_join_table_), K(spec_.id_));
  }
  return ret;
}

int ObHashJoinVecOp::sync_wait_finish_build_hash()
{
  int ret = OB_SUCCESS;
  ObHashJoinVecInput *hj_input = static_cast<ObHashJoinVecInput*>(input_);
  if (OB_ISNULL(hj_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: shared hash join info is null", K(ret));
  } else if (OB_FAIL(hj_input->sync_wait(
      ctx_, hj_input->get_process_cnt(),
      [&](int64_t n_times) {
        UNUSED(n_times);
      }))) {
    LOG_WARN("failed to sync wait finish build hash table", K(ret));
  } else {
    LOG_TRACE("debug sync finish build hash", K(cur_join_table_), K(spec_.id_));
  }
  return ret;
}

int ObHashJoinVecOp::sync_wait_fetch_next_batch()
{
  int ret = OB_SUCCESS;
  ObHashJoinVecInput *hj_input = static_cast<ObHashJoinVecInput*>(input_);
  if (OB_ISNULL(hj_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: shared hash join info is null", K(ret));
  } else if (OB_FAIL(hj_input->sync_wait(
      ctx_, hj_input->get_process_cnt(),
      [&](int64_t n_times) {
        UNUSED(n_times);
        hj_input->reset();
      }))) {
    LOG_WARN("failed to sync fetch next batch", K(ret), K(spec_.id_));
  } else {
    LOG_TRACE("debug sync fetch next batch", K(ret), K(spec_.id_));
  }
  return ret;
}

// If exit normally, then wait all close
// if has some error when run sql, then px interrupt will be checked
int ObHashJoinVecOp::sync_wait_close()
{
  int ret = OB_SUCCESS;
  ObHashJoinVecInput *hj_input = static_cast<ObHashJoinVecInput*>(input_);
  if (OB_ISNULL(hj_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: shared hash join info is null", K(ret));
  } else if (OB_FAIL(hj_input->sync_wait(
      ctx_, hj_input->get_close_cnt(),
      [&](int64_t n_times) {
        UNUSED(n_times);
      }, true))) {
    LOG_WARN("failed to sync fetch next batch", K(ret), K(spec_.id_));
  } else {
    LOG_TRACE("debug sync fetch next batch", K(ret), K(spec_.id_));
  }
  return ret;
}

int ObHashJoinVecOp::sync_wait_open()
{
  int ret = OB_SUCCESS;
  ObHashJoinVecInput *hj_input = static_cast<ObHashJoinVecInput*>(input_);
  if (OB_ISNULL(hj_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: shared hash join info is null", K(ret));
  } else if (OB_FAIL(hj_input->sync_wait(
      ctx_, hj_input->get_open_cnt(),
      [&](int64_t n_times) {
        UNUSED(n_times);
      }, false /*ignore_interrupt*/, true /*is_open*/))) {
    LOG_WARN("failed to sync open", K(ret), K(spec_.id_));
  } else {
    LOG_TRACE("debug sync sync open", K(ret), K(spec_.id_));
  }
  return ret;
}

// dump partition that has only little data
int ObHashJoinVecOp::dump_remain_partition()
{
  int ret = OB_SUCCESS;
  // dump last batch rows and only remain all in-memory data
  if (is_shared_ && OB_FAIL(sync_wait_cur_dumped_partition_idx())) {
    LOG_WARN("failed to sync cur dumped partition idx", K(ret));
  } else if (MAX_PART_COUNT_PER_LEVEL > cur_dumped_partition_) {
    if (OB_FAIL(asyn_dump_partition(INT64_MAX, true, true, cur_dumped_partition_ + 1, nullptr))) {
      LOG_WARN("failed to asyn dump partition", K(ret));
    } else if (OB_FAIL(update_dumped_partition_statistics(true))) {
      LOG_WARN("failed to update dumped partition statistics", K(ret));
    }
    LOG_TRACE("dump remain partition", K(ret), K(cur_dumped_partition_),
      "the last partition dumped size", left_part_array_[part_count_ - 1]->get_size_on_disk(),
      K(spec_.id_));
  }
  return ret;
}

int ObHashJoinVecOp::fill_partition_batch(int64_t &num_left_rows)
{
  int ret = OB_SUCCESS;
  //TODO shengle If it is from row store, add the row directly to the store
  //  No need to project into expression frame first
  bool is_from_row_store = (left_part_ != NULL);
  bool is_left = true;
  while (OB_SUCC(ret)) {
    clear_evaluated_flag();
    const ObBatchRows *child_brs = NULL;
    if (OB_FAIL(get_next_left_row_batch(is_from_row_store, child_brs))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next left row failed", K(ret), K(child_brs));
      }
    } else if (0 == child_brs->size_ && child_brs->end_) {
      break;
    } else if (OB_FAIL(calc_hash_value_and_skip_null(
                       MY_SPEC.build_keys_, child_brs, is_from_row_store,
                       hash_vals_, part_stored_rows_, is_left))) {
      LOG_WARN("fail to calc hash value batch", K(ret));
    } else if (true /*child_brs->size_ > 16 * part_count_*/) {
      // add partition by batch
      if (OB_FAIL(calc_part_selector(hash_vals_, *child_brs))) {
        LOG_WARN("Fail to calc batch idx", K(ret));
      }
      for (int64_t part_idx = 0; OB_SUCC(ret) && part_idx < part_count_; part_idx++) {
        if (part_selector_sizes_[part_idx] <= 0) { continue; }
        num_left_rows += part_selector_sizes_[part_idx];
        if (OB_FAIL(left_part_array_[part_idx]->add_batch(
                                     left_vectors_,
                                     part_selectors_ + part_idx * MY_SPEC.max_batch_size_,
                                     part_selector_sizes_[part_idx],
                                     part_added_rows_))) {
          LOG_WARN("fail to add rows", K(ret), K(part_idx));
        } else {
          for (int64_t i = 0; i < part_selector_sizes_[part_idx]; i++) {
            part_added_rows_[i]->set_is_match(jt_ctx_.build_row_meta_, false);
            part_added_rows_[i]->set_hash_value(jt_ctx_.build_row_meta_,
                     hash_vals_[part_selectors_[part_idx * MY_SPEC.max_batch_size_ + i]]);
            LOG_DEBUG("fill part hash vals", K(part_idx), K(MY_SPEC.max_batch_size_), K(i),
                K(part_selectors_[part_idx * MY_SPEC.max_batch_size_ + i]),
                K(hash_vals_[part_selectors_[part_idx * MY_SPEC.max_batch_size_ + i]]),
                KP(part_added_rows_[i]),
                K(ToStrCompactRow(jt_ctx_.build_row_meta_, *part_added_rows_[i], jt_ctx_.build_output_)));
          }
          if (GCONF.is_sql_operator_dump_enabled()) {
            if (OB_FAIL(dump_build_table(num_left_rows))) {
              LOG_WARN("fail to dump", K(ret));
            }
          }
        }
      } // for end
    } else {
     // // add row to partition
     // ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
     // batch_info_guard.set_batch_size(child_brs->size_);
     // for (int64_t i = 0; OB_SUCC(ret) && i < child_brs->size_; i++) {
     //   ObHJStoredRow *stored_row = nullptr;
     //   if (child_brs->skip_->exist(i)) {
     //     continue;
     //   }
     //   ++num_left_rows;
     //   const int64_t part_idx = get_part_idx(hash_vals_[i]);
     //   batch_info_guard.set_batch_idx(i);
     //   if (OB_FAIL(left_part_array_[part_idx].add_row(
     //       left_->get_spec().output_, &eval_ctx_, stored_row))) {
     //     LOG_WARN("failed to add row", K(ret));
     //   }
     //   if (OB_SUCC(ret)) {
     //     stored_row->set_is_match(false);
     //     stored_row->set_hash_value(hash_vals_[i]);
     //     if (GCONF.is_sql_operator_dump_enabled()) {
     //       if (OB_FAIL(dump_build_table(num_left_rows))) {
     //         LOG_WARN("fail to dump", K(ret));
     //       }
     //     }
     //   }
     // } // for end
    }
    if (OB_SUCC(ret) && child_brs->end_) {
      break;
    }
  }

  return ret;
}

int ObHashJoinVecOp::calc_part_selector(uint64_t *hash_vals, const ObBatchRows &child_brs)
{
  int ret = OB_SUCCESS;
  MEMSET(part_selector_sizes_, 0, sizeof(uint16_t) * part_count_);
  auto op = [&](const int64_t i) __attribute__((always_inline)) {
    const int64_t part_idx = get_part_idx(hash_vals[i]);
    part_selectors_[part_idx * MY_SPEC.max_batch_size_ + part_selector_sizes_[part_idx]] = i;
    part_selector_sizes_[part_idx]++;
    return OB_SUCCESS;
  };
  if (OB_FAIL(ObBitVector::flip_foreach(*child_brs.skip_, child_brs.size_, op))) {
    LOG_WARN("fail to convert skip to selector", K(ret));
  }

  return ret;
}

int ObHashJoinVecOp::split_partition(int64_t &num_left_rows)
{
  int ret = OB_SUCCESS;
  int64_t row_count_on_disk = 0;
  num_left_rows = 0;
  if (nullptr != left_part_) {
    // read all data，use default iterator
    if (OB_FAIL(left_part_->begin_iterator())) {
      LOG_WARN("failed to init iterator", K(ret));
    } else {
      row_count_on_disk = left_part_->get_row_count_on_disk();
    }
  }
  if (OB_SUCC(ret)) {
    ret = fill_partition_batch(num_left_rows);
  }
  if (OB_ITER_END == ret && (!read_null_in_naaj_ || is_shared_)) {
    ret = OB_SUCCESS;
    if (nullptr != left_part_) {
      left_part_->rescan();
    }
    if (sql_mem_processor_.is_auto_mgr() && GCONF.is_sql_operator_dump_enabled()) {
      // last stage for dump build table
      if (OB_FAIL(calc_basic_info())) {
        LOG_WARN("failed to calc basic info", K(ret));
      } else if (OB_FAIL(dump_build_table(profile_.get_row_count(), true))) {
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
          int64_t row_count_in_memory = left_part_array_[i]->get_row_count_in_memory();
          if (0 != row_count_in_memory) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect no data in memory", K(ret), K(row_count_in_memory), K(i));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (nullptr != left_part_ && num_left_rows != row_count_on_disk) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect read all data", K(ret), K(num_left_rows), K(row_count_on_disk));
    }
  }
  LOG_TRACE("trace split partition", K(ret), K(num_left_rows), K(row_count_on_disk),
    K(cur_dumped_partition_), K(read_null_in_naaj_), K(spec_.id_));
  return ret;
}

int ObHashJoinVecOp::prepare_hash_table()
{
  int ret = OB_SUCCESS;

  uint64_t build_ht_thread_ptr = 0;
  // first time, the bucket number is already calculated
  // calculate the buckets number of hash table
  if (HJProcessor::RECURSIVE == hj_processor_) {
    if (OB_FAIL(calc_basic_info())) {
      LOG_WARN("failed to calc basic info", K(ret));
    } else if (GCONF.is_sql_operator_dump_enabled() &&
               OB_FAIL(dump_build_table(profile_.get_row_count(), true))) {
      LOG_WARN("fail to dump", K(ret));
    } else if (OB_FAIL(dump_remain_partition())) {
      LOG_WARN("failed to dump remain partition");
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(calc_basic_info())) {
    LOG_WARN("failed to calc basic info", K(ret));
  } else if (is_shared_ && OB_FAIL(sync_wait_basic_info(build_ht_thread_ptr))) {
    LOG_WARN("failed to sync cur dumped partition idx", K(ret));
  }
  bool need_build_hash_table = true;
  if (is_shared_) {
    ObHashJoinVecInput *hj_input = static_cast<ObHashJoinVecInput*>(input_);
    need_build_hash_table = part_round_ <= 1
                          ? (build_ht_thread_ptr == reinterpret_cast<uint64_t>(this))
                          : cur_join_table_ == &join_table_;
    LOG_DEBUG("debug build hash table", K(need_build_hash_table),
              K(build_ht_thread_ptr), K(reinterpret_cast<uint64_t>(this)));
  }
  if (OB_SUCC(ret) && need_build_hash_table) {
    if (OB_FAIL(cur_join_table_->build_prepare(jt_ctx_, profile_.get_row_count(), profile_.get_bucket_size()))) {
      LOG_WARN("trace failed to  prepare hash table",
               K(profile_.get_expect_size()), K(profile_.get_bucket_size()), K(profile_.get_row_count()),
               K(get_mem_used()), K(sql_mem_processor_.get_mem_bound()), K(cur_dumped_partition_));
    } else if (OB_FAIL(sql_mem_processor_.update_used_mem_size(get_mem_used()))) {
      LOG_WARN("failed to update used mem size", K(ret));
    }
    LOG_TRACE("trace prepare hash table", K(ret), K(profile_.get_bucket_size()), K(profile_.get_row_count()),
              K(part_count_), K(profile_.get_expect_size()), K(spec_.id_));
  }
  if (OB_SUCC(ret) && is_shared_ && OB_FAIL(sync_wait_init_build_hash(build_ht_thread_ptr))) {
    LOG_WARN("failed to sync wait init hash table", K(ret));
  }

  return ret;
}

void ObHashJoinVecOp::trace_hash_table_collision(int64_t row_cnt)
{
  int64_t total_cnt = cur_join_table_->get_collisions();
  int64_t used_bucket_cnt = cur_join_table_->get_used_buckets();
  int64_t nbuckets = cur_join_table_->get_nbuckets();
  LOG_TRACE("trace hash table collision", K(spec_.get_id()), K(spec_.get_name()), K(nbuckets),
    "avg_cnt", ((double)total_cnt/(double)used_bucket_cnt), K(total_cnt),
    K(row_cnt), K(used_bucket_cnt), K(spec_.id_));
  op_monitor_info_.otherstat_1_value_ = 0;
  op_monitor_info_.otherstat_2_value_ = 0;
  op_monitor_info_.otherstat_3_value_ = total_cnt;
  op_monitor_info_.otherstat_4_value_ = nbuckets;
  op_monitor_info_.otherstat_5_value_ = used_bucket_cnt;
  op_monitor_info_.otherstat_6_value_ = row_cnt;
  op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::HASH_SLOT_MIN_COUNT;
  op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::HASH_SLOT_MAX_COUNT;
  op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::HASH_SLOT_TOTAL_COUNT;
  op_monitor_info_.otherstat_4_id_ = ObSqlMonitorStatIds::HASH_BUCKET_COUNT;
  op_monitor_info_.otherstat_5_id_ = ObSqlMonitorStatIds::HASH_NON_EMPTY_BUCKET_COUNT;
  op_monitor_info_.otherstat_6_id_ = ObSqlMonitorStatIds::HASH_ROW_COUNT;
}

int ObHashJoinVecOp::build_hash_table_for_recursive()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(prepare_hash_table())) {
    LOG_WARN("failed to prepare hash table", K(ret));
  }
  int64_t start_id = is_shared_ ? (static_cast<ObHashJoinVecInput*>(input_))->get_task_id() : 0;
  JoinHashTable &hash_table = *cur_join_table_;
  for (int64_t i = start_id, idx = 0; OB_SUCC(ret) && idx < part_count_; ++idx, ++i) {
    i = i % part_count_;
    ObHJPartition &hj_part = *left_part_array_[i];
    if (0 < hj_part.get_row_count_in_memory()) {
      if (0 < hj_part.get_row_count_on_disk()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect it has row on disk", K(ret), K(hj_part.get_row_count_in_memory()),
                                                        K(hj_part.get_row_count_on_disk()));
      } else if (OB_FAIL(hj_part.begin_iterator())) {
        // It is assumed here that the partition must be either all dumped or all in memory,
        // so just use row iter.
        // There is no need to use chunk row store. If part is in memory and part is in disk,
        // you can use chunk to load the data in memory first.
        LOG_WARN("failed to init iterator", K(ret));
      } else {
        JoinPartitionRowIter left_iter(&hj_part);
        ret = hash_table.build(left_iter, jt_ctx_);
        ret = OB_ITER_END == ret ? OB_SUCCESS : ret;
      }
    }
  } // for end


  // In order to be consistent with the nest loop method, this flag indicates that
  // in recursive mode, if there is no dump, it must be in-memory.
  //
  // In the case of in-memory, it is necessary to return data according
  // to the join type (right (anti, outer, etc.) join).
  //
  // In the case of nest loop, only the last chunk is needed,
  // and the recursive in-memory data must be needed, so set it to true here.
  if (OB_SUCC(ret)) {
    is_last_chunk_ = true;
    if (is_shared_) {
      if (OB_FAIL(sync_wait_finish_build_hash())) {
        LOG_WARN("failed to wait finish build hash", K(ret));
      }
    }
    trace_hash_table_collision(hash_table.get_row_count());
  }

  LOG_TRACE("trace to finish build hash table for recursive", K(part_count_), K(part_level_),
    K(cur_join_table_->get_row_count()), K(cur_join_table_->get_nbuckets()), K(spec_.id_), K(start_id));
  return ret;
}

int ObHashJoinVecOp::recursive_process(int64_t &num_left_rows)
{
  int ret = OB_SUCCESS;
  num_left_rows = 0;
  if (OB_FAIL(init_join_partition())) {
    LOG_WARN("fail to init join ctx", K(ret));
  } else if (OB_FAIL(split_partition(num_left_rows))) {
    LOG_WARN("failed split partition", K(ret), K(part_level_));
  } else {
    // TODO shengle If do not need to read the right table and do not need to return unmatch data,
    // you do not need to create a hash table
    // If read_null_in_naaj_ is true, there is no need to create a hash table
    if (OB_FAIL(build_hash_table_for_recursive())) {
      LOG_WARN("failed to post process left", K(ret), K(num_left_rows));
    }
  }
  return ret;
}

int ObHashJoinVecOp::adaptive_process(int64_t &num_left_rows)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_processor_type())) {
    LOG_WARN("failed to get processor", K(hj_processor_), K(ret));
  } else {
    switch (hj_processor_) {
      case IN_MEMORY: {
        if (OB_FAIL(in_memory_process(num_left_rows))) {
          LOG_WARN("failed to process in memory", K(ret));
        }
        break;
      }
      case RECURSIVE: {
        if (OB_FAIL(recursive_process(num_left_rows))) {
          LOG_WARN("failed to recursive process", K(ret), K(part_level_), K(part_count_),
                                                  K(join_table_.get_nbuckets()));
        }
        break;
      }
      case NEST_LOOP: {
        if (OB_FAIL(nest_loop_process(num_left_rows))) {
          if (HJLoopState::LOOP_RECURSIVE == nest_loop_state_ && MAX_PART_LEVEL > part_level_) {
            ret = OB_SUCCESS;
            clean_nest_loop_chunk();
            set_processor(RECURSIVE);
            if (OB_FAIL(recursive_process(num_left_rows))) {
              LOG_WARN("failed to process in memory", K(ret), K(part_level_), K(part_count_),
                                                      K(join_table_.get_nbuckets()));
            }
            LOG_TRACE("trace recursive process", K(part_level_), K(part_level_), K(part_count_),
                                                 K(join_table_.get_nbuckets()), K(spec_.id_));
          } else {
            LOG_WARN("failed to process in memory", K(ret), K(part_level_), K(part_count_),
                                                    K(join_table_.get_nbuckets()));
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

  LOG_TRACE("trace process type", K(hj_processor_), K(part_level_), K(part_count_),
            K(num_left_rows), K(remain_data_memory_size_), K(is_shared_), K(spec_.id_));
  return ret;
}

int ObHashJoinVecOp::get_next_right_batch()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  LOG_TRACE("hash join last traverse cnt", K(ret), K(right_batch_traverse_cnt_), K(spec_.id_));
  right_batch_traverse_cnt_ = 0;
  output_info_.reuse();
  bool is_left = false;
  const ObBatchRows *probe_brs = NULL;
  if (right_part_ == NULL) {
    if (HashJoinDrainMode::BUILD_HT == drain_mode_ || (read_null_in_naaj_ && is_shared_)) {
      if (OB_FAIL(right_->drain_exch())) {
        LOG_WARN("failed to drain the right child", K(ret), K(spec_.id_));
      } else {
        drain_mode_ = HashJoinDrainMode::RIGHT_DRAIN;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (HashJoinDrainMode::RIGHT_DRAIN == drain_mode_
               || (read_null_in_naaj_ && is_shared_)) {
      probe_brs = &probe_batch_rows_.brs_;
      const_cast<ObBatchRows *>(probe_brs)->size_ = 0;
      const_cast<ObBatchRows *>(probe_brs)->end_ = true;
    } else {
      if (OB_FAIL(OB_I(t1) right_->get_next_batch(max_output_cnt_, probe_brs))) {
        LOG_WARN("get right row from child failed", K(ret));
      } else if (probe_brs->end_ && 0 == probe_brs->size_) {
        // do nothing
      } else if (MY_SPEC.is_naaj_) {
        bool has_null = false;
        if (FALSE_IT(non_preserved_side_is_not_empty_
                     |= (LEFT_ANTI_JOIN == MY_SPEC.join_type_))) {
          // mark this to forbid null value output in fill result batch
        } else if (is_left_naaj()
                   && OB_FAIL(check_join_key_for_naaj_batch(is_left, has_null, probe_brs))) {
          LOG_WARN("failed to check null value for naaj", K(ret));
        } else if (has_null) {
          brs_.size_ = 0;
          brs_.end_ = true;
          read_null_in_naaj_ = true;
          ret = OB_SUCCESS;
          LOG_TRACE("null break for left naaj", K(ret), K(spec_.id_));
        }
      }
      if (OB_SUCC(ret)) {
        probe_batch_rows_.brs_.copy(probe_brs);
        LOG_DEBUG("get next right batch", K(*probe_brs));
      }
    }
    probe_batch_rows_.from_stored_ = false;
  } else {
    int64_t read_size = 0;
    probe_brs = &probe_batch_rows_.brs_;
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (HashJoinDrainMode::BUILD_HT == drain_mode_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: drain mode is build_hash_table", K(ret), K(spec_.id_));
    } else if (HashJoinDrainMode::RIGHT_DRAIN == drain_mode_
               || (read_null_in_naaj_ && is_shared_)) {
      const_cast<ObBatchRows *>(probe_brs)->size_ = 0;
      const_cast<ObBatchRows *>(probe_brs)->end_ = true;
      ret = OB_SUCCESS;
    } else if (OB_FAIL(OB_I(t1) right_part_->get_next_batch(probe_batch_rows_.stored_rows_,
                                                            max_output_cnt_,
                                                            read_size))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get right row from partition failed", K(ret));
      } else {
        const_cast<ObBatchRows *>(probe_brs)->size_ = 0;
        const_cast<ObBatchRows *>(probe_brs)->end_ = true;
        ret = OB_SUCCESS;
      }
    } else {
      const_cast<ObBatchRows *>(probe_brs)->size_ = read_size;
      const_cast<ObBatchRows *>(probe_brs)->end_ = false;
      const_cast<ObBatchRows *>(probe_brs)->skip_->reset(read_size);
      const_cast<ObBatchRows *>(probe_brs)->all_rows_active_ = true;
    }
    probe_batch_rows_.from_stored_ = true;
  }

  return ret;
}

int ObHashJoinVecOp::read_right_operate()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_next_right_batch())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next right row batch", K(ret));
    }
  } else if ((probe_batch_rows_.brs_.size_ == 0 && probe_batch_rows_.brs_.end_)
             || read_null_in_naaj_) {
    ret = OB_ITER_END;
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(calc_hash_value_and_skip_null(MY_SPEC.probe_keys_, &probe_batch_rows_.brs_,
                                              probe_batch_rows_.from_stored_,
                                              probe_batch_rows_.hash_vals_,
                                              probe_batch_rows_.stored_rows_,
                                              false /*is_left*/))) {
      LOG_WARN("fail to calc hash value batch", K(ret));
    } else if (OB_FAIL(skip_rows_in_dumped_part())) {
      LOG_WARN("fail to skip rows in dumped part", K(ret));
    } else if (OB_FAIL(convert_skip_to_selector())) {
      LOG_WARN("fail to convert skip to selector", K(ret));
    }
  }

  if (OB_ITER_END == ret) {
    int tmp_ret = dump_right_partition_for_recursive();
    if (OB_SUCCESS != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("fail to process read right end", K(ret));
    }
  }

  return ret;
}

int ObHashJoinVecOp::calc_hash_value_and_skip_null(const ObIArray<ObExpr*> &join_keys,
                                                   const ObBatchRows *brs,
                                                   const bool is_from_row_store,
                                                   uint64_t *hash_vals,
                                                   const ObHJStoredRow **store_rows,
                                                   bool is_left)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(brs));
  if (OB_FAIL(ret)) {
  } else if (!is_from_row_store) {
    uint64_t seed = HASH_SEED;
    bool enable_skip_null = (is_left && skip_left_null_) || (!is_left && skip_right_null_);
    if (enable_skip_null) {
      null_skip_bitmap_->reset(brs->size_);
    } else {
      null_skip_bitmap_->deep_copy(*brs->skip_, brs->size_);
    }
    for (int64_t idx = 0; OB_SUCC(ret) && idx < join_keys.count() ; ++idx) {
      ObExpr *expr = join_keys.at(idx); // expr ptr check in cg, not check here
      if (OB_FAIL(expr->eval_vector(eval_ctx_, *brs))) {
        LOG_WARN("eval failed", K(ret));
      } else {
        const bool is_batch_seed = (idx > 0);
        ObIVector *vector = expr->get_vector(eval_ctx_);
        if (enable_skip_null) {
          if (vector->has_null() && !MY_SPEC.is_ns_equal_cond_.at(idx)) {
            VectorFormat format = vector->get_format();
            if (is_uniform_format(format)) {
              const uint64_t idx_mask = VEC_UNIFORM_CONST == format ? 0 : UINT64_MAX;
              const ObDatum *datums = static_cast<ObUniformBase *>(vector)->get_datums();
              for (int64_t i = 0; i < brs->size_; i++) {
                if (datums[i & idx_mask].is_null()) {
                  null_skip_bitmap_->set(i);
                }
              }
            } else {
              null_skip_bitmap_->bit_calculate(*null_skip_bitmap_,
                                               *static_cast<ObBitmapNullVectorBase *>(vector)->get_nulls(),
                                               brs->size_,
                                               [](const uint64_t l,
                                               const uint64_t r) { return (l | r); });
            }
          }
        }
        brs->skip_->bit_calculate(*brs->skip_, *null_skip_bitmap_, brs->size_,
                                  [](const uint64_t l, const uint64_t r) { return (l | r); });
        if (brs->all_rows_active_) {
          const_cast<ObBatchRows *>(brs)->all_rows_active_ = null_skip_bitmap_
                                                             ->is_all_false(brs->size_);
        }
        ret = vector->murmur_hash_v3(*expr, hash_vals, *brs->skip_,
                                     EvalBound(brs->size_, brs->all_rows_active_),
                                     is_batch_seed ? hash_vals : &seed,
                                     is_batch_seed);
      }
    }
    if (OB_SUCC(ret)) {
      //TODO shengle handle null random hash value
      //bool need_null_random = (MY_SPEC.join_type_ != LEFT_ANTI_JOIN && MY_SPEC.join_type_ != RIGHT_ANTI_JOIN);
      auto mask_hash_val_op = [&](const int64_t i) __attribute__((always_inline)) {
        hash_vals[i] = hash_vals[i] & ObHJStoredRow::HASH_VAL_MASK;
        return OB_SUCCESS;
      };
      ObBitVector::flip_foreach(*brs->skip_, brs->size_, mask_hash_val_op);
    }
  } else {
    for (int64_t i = 0; i < brs->size_; i++) {
      OB_ASSERT(OB_NOT_NULL(store_rows[i]));
      hash_vals[i] = store_rows[i]->get_hash_value(
          is_left ? jt_ctx_.build_row_meta_: jt_ctx_.probe_row_meta_);
    }
  }
  return ret;
}

int ObHashJoinVecOp::skip_rows_in_dumped_part()
{
  int ret = OB_SUCCESS;
  if (RECURSIVE == hj_processor_ && MAX_PART_COUNT_PER_LEVEL != cur_dumped_partition_) {
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    for (int64_t i = 0; OB_SUCC(ret) && i < probe_batch_rows_.brs_.size_; i++) {
      if (probe_batch_rows_.brs_.skip_->exist(i)) {
        continue;
      }
      int64_t part_idx = get_part_idx(probe_batch_rows_.hash_vals_[i]);
      if (check_right_need_dump(part_idx)) {
        LOG_DEBUG("right need dump row", K(i), K(part_idx), K(cur_dumped_partition_));
        probe_batch_rows_.brs_.skip_->set(i);
        probe_batch_rows_.brs_.all_rows_active_ = false;
        ObCompactRow *stored_row = nullptr;
        if (probe_batch_rows_.from_stored_) {
          if (OB_FAIL(right_part_array_[part_idx]->add_row(
              probe_batch_rows_.stored_rows_[i], stored_row))) {
            LOG_WARN("fail to add row", K(ret));
          }
        } else {
          guard.set_batch_idx(i);
          if (OB_FAIL(right_part_array_[part_idx]->add_row(
              right_->get_spec().output_, eval_ctx_, stored_row))) {
            LOG_WARN("fail to add row", K(ret));
          } else {
            static_cast<ObHJStoredRow *>(stored_row)->set_hash_value(jt_ctx_.probe_row_meta_,
                                          probe_batch_rows_.hash_vals_[i]);
          }
        }
      }
    }  //for end;
  }

  return ret;
}

int ObHashJoinVecOp::convert_skip_to_selector()
{
  int ret = OB_SUCCESS;
  int64_t selector_idx = 0;
  auto convertor_op = [&](const int64_t i) __attribute__((always_inline)) {
      output_info_.selector_[selector_idx++] = i;
    return OB_SUCCESS;
  };
  if (OB_FAIL(ObBitVector::flip_foreach(*probe_batch_rows_.brs_.skip_,
                                        probe_batch_rows_.brs_.size_,
                                        convertor_op))) {
    LOG_WARN("fail to convert skip to selector", K(ret));
  } else {
    output_info_.selector_cnt_ = selector_idx;
  }
  return ret;
}

int ObHashJoinVecOp::finish_dump(bool for_left, bool need_dump, bool force /* false */)
{
  int ret = OB_SUCCESS;
  if (RECURSIVE == hj_processor_) {
    ObHJPartition **part_array = NULL;
    if (for_left) {
      part_array = left_part_array_;
    } else {
      part_array = right_part_array_;
    }
    int64_t total_size = 0;
    int64_t dumped_row_count = 0;
    for (int64_t i = 0; i < part_count_ && OB_SUCC(ret); i ++) {
      total_size += part_array[i]->get_size_in_memory() + part_array[i]->get_size_on_disk();
    }
    for (int64_t i = 0; i < part_count_ && OB_SUCC(ret); i ++) {
      if (force) {
        // finish dump在之前没有dump情况下，不会强制dump，所以这里需要先dump
        if (OB_FAIL(part_array[i]->dump(true, INT64_MAX))) {
          LOG_WARN("failed to dump", K(ret));
        } else if (cur_dumped_partition_ >= i) {
          cur_dumped_partition_ = i - 1;
        }
      }
      if (part_array[i]->is_dumped()) {
        if (OB_SUCC(ret)) {
          // 认为要么全部dump，要么全部in-memory
          if (OB_FAIL(part_array[i]->finish_dump(true))) {
            LOG_WARN("finish dump failed", K(i), K(for_left));
          } else if (for_left) {
            // enable pair right partition dump
            right_part_array_[i]->set_memory_limit(1);
          }
        }
        dumped_row_count += part_array[i]->get_row_count_on_disk();
      }
      if (OB_SUCC(ret)) {
        part_array[i]->record_pre_batch_info(part_count_,
                                             cur_join_table_->get_nbuckets(),
                                             total_size);
      }
    }
    if (force && for_left) {
      // mark dump all left partitions
      cur_dumped_partition_ = -1;
    }
    LOG_TRACE("finish dump: ", K(part_level_),
      K(cur_dumped_partition_), K(for_left), K(need_dump), K(force), K(total_size),
      K(dumped_row_count), K(part_count_), K(spec_.id_));
  }
  return ret;
}

int ObHashJoinVecOp::dump_right_partition_for_recursive()
{
  int ret = OB_SUCCESS;
  if (RECURSIVE == hj_processor_) {
    // 保证left被dump过，则对应的right一定会被dump
    if (OB_FAIL(asyn_dump_partition(INT64_MAX, false, true, cur_dumped_partition_ + 1, nullptr))) {
      LOG_WARN("failed to asyn dump partition", K(ret));
    } else if (OB_FAIL(update_dumped_partition_statistics(false))) {
      LOG_WARN("failed to update dumped partition statistics", K(ret));
    }
  }
  return ret;
}

int ObHashJoinVecOp::probe()
{
  int ret = OB_SUCCESS;
  //ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
  right_batch_traverse_cnt_++;
  OZ(init_left_vectors());
  if (1 == right_batch_traverse_cnt_) {
    //TODO shengle opt
    //Probe side data, for only equal scenarios, you can also delay the conversion,
    //and only need to convert the matching data.
    if (probe_batch_rows_.from_stored_) {
      const ObExprPtrIArray &exprs = right_->get_spec().output_;
      OZ(ObHJStoredRow::attach_rows(exprs, eval_ctx_, jt_ctx_.probe_row_meta_,
                                    probe_batch_rows_.stored_rows_,
                                    output_info_.selector_, output_info_.selector_cnt_));
    }
    OZ (cur_join_table_->probe_prepare(jt_ctx_, output_info_));
  }

  OZ (cur_join_table_->probe_batch(jt_ctx_, output_info_));

  if (OB_SUCC(ret)) {
    if (output_info_.selector_cnt_ == 0 && !IS_RIGHT_STYLE_JOIN(MY_SPEC.join_type_)) {
      ret = OB_ITER_END;
      LOG_DEBUG("probe batch end");
    } else {
      ret = probe_batch_output();
    }
  }

  return ret;
}

int ObHashJoinVecOp::probe_batch_output()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (INNER_JOIN == MY_SPEC.join_type_) {
    ret = inner_join_output();
  } else if (LEFT_SEMI_JOIN == MY_SPEC.join_type_) {
    ret = left_semi_output();
  } else if (IS_RIGHT_SEMI_ANTI_JOIN(MY_SPEC.join_type_)) {
    ret = right_anti_semi_output();
  } else if (IS_OUTER_JOIN(MY_SPEC.join_type_)) {
    ret = outer_join_output();
  } else if (LEFT_ANTI_JOIN == MY_SPEC.join_type_) {
    // do nothing
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid join type", K(MY_SPEC.join_type_), K(ret));
  }

  return ret;
}

int ObHashJoinVecOp::left_semi_output()
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  if (brs_.size_ > 0) {
    brs_.skip_->reset(brs_.size_);
  }
  if (OB_FAIL(ObTempRowStore::Iterator::attach_rows(
            *jt_ctx_.build_output_, eval_ctx_, jt_ctx_.build_row_meta_,
            reinterpret_cast<const ObCompactRow **>(output_info_.left_result_rows_),
            output_info_.selector_cnt_))) {
    LOG_WARN("fail to attach rows", K(ret));
  } else {
    brs_.size_ = output_info_.selector_cnt_;
    mark_return();
  }

  return ret;
}

// right anti/semi only need traverse right batch once
int ObHashJoinVecOp::right_anti_semi_output()
{
  int ret = OB_SUCCESS;
  bool need_mark_return = false;
  if (RIGHT_SEMI_JOIN == MY_SPEC.join_type_) {
    if (output_info_.selector_cnt_ > 0) {
      brs_.size_ = probe_batch_rows_.brs_.size_;
      brs_.skip_->set_all(brs_.size_);
      need_mark_return = true;
      brs_.all_rows_active_ = output_info_.selector_cnt_ == brs_.size_;
      for (int64_t i = 0; i < output_info_.selector_cnt_; i++) {
        brs_.skip_->unset(output_info_.selector_[i]);
      }
    }
  } else if (RIGHT_ANTI_JOIN == MY_SPEC.join_type_) {
    if (output_info_.selector_cnt_ != probe_batch_rows_.brs_.size_) {
      need_mark_return = true;
      brs_.copy(&probe_batch_rows_.brs_);
      for (int64_t i = 0; i < output_info_.selector_cnt_; i++) {
        brs_.skip_->set(output_info_.selector_[i]);
      }
      brs_.all_rows_active_ = output_info_.selector_cnt_ > 0 ? false : brs_.all_rows_active_;
    }
    // skip null for right naaj
    if (OB_SUCC(ret) && need_mark_return && MY_SPEC.is_naaj_ && non_preserved_side_is_not_empty_) {
      need_mark_return = false;
      clear_evaluated_flag();
      if (OB_FAIL(MY_SPEC.probe_keys_.at(0)->eval_vector(eval_ctx_, brs_))) {
        LOG_WARN("failed to eval right join key", K(ret));
      } else {
        const ObIVector *vec = MY_SPEC.probe_keys_.at(0)->get_vector(eval_ctx_);
        for (int64_t i = 0; i < brs_.size_; ++i) {
          if (brs_.skip_->at(i)) {
            continue;
          }
          if (vec->is_null(i)) {
            brs_.skip_->set(i);
            if (brs_.all_rows_active_) {
              brs_.all_rows_active_ = false;
            }
          } else {
            need_mark_return = true;
          }
        } // for end
      }
    }
  } else {
    LOG_WARN("invalid join type", K(MY_SPEC.join_type_));
  }
  if (need_mark_return) {
    set_output_eval_info();
    mark_return();
  } else {
    // reset skip vector if not return
    brs_.skip_->reset(brs_.size_);
    brs_.size_ = 0;
  }
  ret = OB_SUCC(ret) ? OB_ITER_END : ret;

  return ret;
}

int ObHashJoinVecOp::outer_join_output()
{
  int ret = OB_SUCCESS;
  brs_.size_ = probe_batch_rows_.brs_.size_;
  brs_.skip_->set_all(brs_.size_);

  bool need_mark_return = output_info_.selector_cnt_ > 0;
  for (int64_t i = 0; i < output_info_.selector_cnt_; i++) {
    int64_t batch_idx = output_info_.selector_[i];
    brs_.skip_->unset(batch_idx);
  }

  // fill NULL to left row for right/outer join if right not match
  if (1 == right_batch_traverse_cnt_ && need_right_join()) {
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    for (int64_t i = 0; OB_SUCC(ret) && i < probe_batch_rows_.brs_.size_; i++) {
      if (probe_batch_rows_.brs_.skip_->exist(i)) {
        continue;
      }
      guard.set_batch_idx(i);
      if (brs_.skip_->exist(i)) {// right not match
        need_mark_return = true;
        blank_row_batch_one(left_->get_spec().output_); // right outer join, null-left row
        brs_.skip_->unset(i);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (need_mark_return) {
      set_output_eval_info();
      mark_return();
    } else {
      // reset skip vector if not return
      brs_.skip_->reset(brs_.size_);
      brs_.size_ = 0;
      ret = OB_ITER_END;
    }
  }

  return ret;
}

void ObHashJoinVecOp::set_output_eval_info()
{
  for (int64_t i = 0; i < left_->get_spec().output_.count(); i++) {
    ObEvalInfo &info = left_->get_spec().output_.at(i)->get_eval_info(eval_ctx_);
    info.evaluated_ = true;
    info.projected_ = true;
  }
  for (int64_t i = 0; i < right_->get_spec().output_.count(); i++) {
    ObEvalInfo &info = right_->get_spec().output_.at(i)->get_eval_info(eval_ctx_);
    info.evaluated_ = true;
    info.projected_ = true;
  }
}

int ObHashJoinVecOp::inner_join_output()
{
  int ret = OB_SUCCESS;
  brs_.size_ = probe_batch_rows_.brs_.size_;
  if (output_info_.selector_cnt_ == probe_batch_rows_.brs_.size_) {
    brs_.all_rows_active_ = true;
    brs_.skip_->reset(brs_.size_);
  } else {
    brs_.skip_->set_all(brs_.size_);
    for (int64_t i = 0; i < output_info_.selector_cnt_; i++) {
      brs_.skip_->unset(output_info_.selector_[i]);
    }
  }
  if (jt_ctx_.probe_opt_) {
    ret = cur_join_table_->project_matched_rows(jt_ctx_, output_info_);
  }
  if (OB_SUCC(ret)) {
    set_output_eval_info();
    mark_return();
  }

  return ret;
}

int ObHashJoinVecOp::fill_left_unmatched_result()
{
  int ret = OB_SUCCESS;
  if (brs_.size_ > 0) {
    brs_.skip_->reset(brs_.size_);
    brs_.all_rows_active_ = true;
  }
  if (OB_FAIL(cur_join_table_->get_unmatched_rows(jt_ctx_, output_info_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get unmatched batch", K(ret));
    }
  }
  if (OB_ITER_END == ret && 0 != output_info_.selector_cnt_) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCCESS == ret) {
    ObTempRowStore::Iterator::attach_rows(
              *jt_ctx_.build_output_, eval_ctx_, jt_ctx_.build_row_meta_,
              reinterpret_cast<const ObCompactRow **>(output_info_.left_result_rows_),
              output_info_.selector_cnt_);
    if (need_left_join()) {
      OZ(blank_row_batch(right_->get_spec().output_, output_info_.selector_cnt_));
    } else if (MY_SPEC.is_naaj_) {
      if (OB_FAIL(check_join_key_for_naaj_batch_output(output_info_.selector_cnt_))) {
        LOG_WARN("failed to check join key", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    brs_.size_ = output_info_.selector_cnt_;
    set_output_eval_info();
    mark_return();
  }

  return ret;
}

int ObHashJoinVecOp::check_join_key_for_naaj_batch(const bool is_left,
                                                   bool &has_null,
                                                   const ObBatchRows *child_brs)
{
  int ret = OB_SUCCESS;
  ObHashJoinVecInput *hj_input = static_cast<ObHashJoinVecInput*>(input_);
  clear_evaluated_flag();
  const ObExprPtrIArray &curr_join_keys = is_left ? MY_SPEC.build_keys_ : MY_SPEC.probe_keys_;
  CK (1 == curr_join_keys.count());
  CK (!has_null);
  CK (nullptr != child_brs);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(hj_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: shared hash join info is null", K(ret));
  } else if (is_shared_ && FALSE_IT(has_null = hj_input->get_null_in_naaj())) {
  } else if (has_null) {
  } else if (OB_FAIL(curr_join_keys.at(0)->eval_vector(eval_ctx_, *child_brs))) {
    LOG_WARN("failed to eval batch curr join key", K(ret));
  } else {
    ObIVector *vec = curr_join_keys.at(0)->get_vector(eval_ctx_);
    for (int64_t i = 0; OB_SUCC(ret) && i < child_brs->size_; ++i) {
      if (child_brs->skip_->at(i)) {
        continue;
      }
      if (vec->is_null(i)) {
        if ((is_left && is_right_naaj_na()) || (!is_left && is_left_naaj_na())) {
          has_null = true;
          break;
        } else if ((is_left && is_right_naaj_sna()) || (!is_left && is_left_naaj_sna())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("get null value in non preserved side, may generated wrong plan",
                    K(ret), K(MY_SPEC.join_type_), K(MY_SPEC.is_naaj_), K(MY_SPEC.is_sna_));
        }
      }
    } // for end
  }
  return ret;
}

int ObHashJoinVecOp::check_join_key_for_naaj_batch_output(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (!non_preserved_side_is_not_empty_) {
    // do nothing
  } else if (OB_FAIL(MY_SPEC.build_keys_.at(0)->eval_vector(eval_ctx_,
                                                            *brs_.skip_,
                                                            batch_size,
                                                            false))) {
    LOG_WARN("failed to eval join key for naaj", K(ret));
  } else {
    ObIVector *vec = MY_SPEC.build_keys_.at(0)->get_vector(eval_ctx_);
    VectorFormat format = vec->get_format();
    if (is_uniform_format(format)) {
      const uint64_t idx_mask = VEC_UNIFORM_CONST == format ? 0 : UINT64_MAX;
      const ObDatum *datums = static_cast<ObUniformBase *>(vec)->get_datums();
      for (int64_t i = 0; i < batch_size; i++) {
        if (datums[i & idx_mask].is_null()) {
          brs_.skip_->set(i);
        }
      }
    } else {
      brs_.skip_->bit_calculate(*brs_.skip_,
                                *static_cast<ObBitmapNullVectorBase *>(vec)->get_nulls(),
                                batch_size,
                                [](const uint64_t l, const uint64_t r) { return (l | r); });
    }
    brs_.all_rows_active_ = (0 == brs_.skip_->accumulate_bit_cnt(batch_size));
  }
  return ret;
}

int ObHashJoinVecOp::init_mem_context(uint64_t tenant_id)
{
  int ret = common::OB_SUCCESS;
  if (OB_LIKELY(NULL == mem_context_)) {
    lib::ContextParam param;
    param.set_properties(lib::USE_TL_PAGE_OPTIONAL)
      .set_mem_attr(tenant_id, common::ObModIds::OB_ARENA_HASH_JOIN,
                     common::ObCtxIds::WORK_AREA);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      SQL_ENG_LOG(WARN, "create entity failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      SQL_ENG_LOG(WARN, "mem entity is null", K(ret));
    } else {
      alloc_ = &mem_context_->get_malloc_allocator();
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
