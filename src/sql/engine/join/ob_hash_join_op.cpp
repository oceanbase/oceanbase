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

namespace oceanbase
{
using namespace omt;
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER(ObHashJoinInput, shared_hj_info_);

// The ctx is owned by thread
// sync_event is shared
int ObHashJoinInput::sync_wait(ObExecContext &ctx, int64_t &sync_event, EventPred pred, bool ignore_interrupt, bool is_open)
{
  int ret = OB_SUCCESS;
  ObHashTableSharedTableInfo *shared_hj_info = reinterpret_cast<ObHashTableSharedTableInfo *>(shared_hj_info_);
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
        pred(ATOMIC_LOAD(&sync_event) % shared_hj_info->sqc_thread_count_);
        // it must be here to guarantee next wait loop to get lock for one thread
        has_process = true;
        if (ATOMIC_LOAD(&sync_event) + 1 >= exit_cnt) {
          // last thread, it will singal and exit by self
          ATOMIC_INC(&sync_event);
          shared_hj_info->cond_.signal();
          LOG_DEBUG("debug sync event", K(ret), K(lbt()), K(sync_event));
          break;
        }
        ATOMIC_INC(&sync_event);
        LOG_DEBUG("debug sync event", K(ret), K(lbt()), K(sync_event));
      }
      if (!ignore_interrupt && OB_SUCCESS != shared_hj_info->ret_) {
        // the thread already return error
        ret = shared_hj_info->ret_;
      } else if (!ignore_interrupt && 0 == loop % 8 && OB_UNLIKELY(IS_INTERRUPTED())) {
        // 中断错误处理
        // overwrite ret
        ObInterruptCode code = GET_INTERRUPT_CODE();
        ret = code.code_;
        LOG_WARN("received a interrupt", K(code), K(ret));
      } else if (!ignore_interrupt && 0 == loop % 16 && OB_FAIL(ctx.fast_check_status())) {
        LOG_WARN("failed to check status", K(ret));
      } else if (ATOMIC_LOAD(&sync_event) >= exit_cnt) {
        // timeout, and signal has done
        LOG_DEBUG("debug sync event", K(ret), K(lbt()), K(sync_event),
          K(loop), K(exit_cnt));
        break;
      } else if (ignore_interrupt /*close stage*/ && OB_SUCCESS != ATOMIC_LOAD(&shared_hj_info->open_ret_)) {
        LOG_WARN("some op have failed in open stage", K(ATOMIC_LOAD(&shared_hj_info->open_ret_)));
        break;
      } else {
        auto key = shared_hj_info->cond_.get_key();
        // wait one time per 1000 us
        shared_hj_info->cond_.wait(key, 1000);
      }
    } // end while
    if (OB_FAIL(ret) && is_open) {
      set_open_ret(ret);
    }
  }
  return ret;
}

ObHashJoinSpec::ObHashJoinSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObJoinSpec(alloc, type),
  equal_join_conds_(alloc),
  all_join_keys_(alloc),
  all_hash_funcs_(alloc),
  can_prob_opt_(false),
  is_naaj_(false),
  is_sna_(false),
  is_shared_ht_(false),
  is_ns_equal_cond_(alloc)
{
}

OB_SERIALIZE_MEMBER((ObHashJoinSpec, ObJoinSpec),
                    equal_join_conds_,
                    all_join_keys_,
                    all_hash_funcs_,
                    can_prob_opt_,
                    is_naaj_,
                    is_sna_,
                    is_shared_ht_,
                    is_ns_equal_cond_);

int ObHashJoinOp::PartHashJoinTable::init(ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    void *alloc_buf = alloc.alloc(sizeof(ModulePageAllocator));
    void *bucket_buf = alloc.alloc(sizeof(BucketArray));
    if (OB_ISNULL(bucket_buf) || OB_ISNULL(alloc_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      if (OB_NOT_NULL(bucket_buf)) {
        alloc.free(bucket_buf);
      }
      if (OB_NOT_NULL(alloc_buf)) {
        alloc.free(alloc_buf);
      }
      LOG_WARN("failed to alloc memory", K(ret));
    } else {
      ht_alloc_ = new (alloc_buf) ModulePageAllocator(alloc);
      ht_alloc_->set_label("HtOpAlloc");
      buckets_ = new (bucket_buf) BucketArray(*ht_alloc_);
      magic_ = MAGIC_CODE;
      bit_cnt_ = __builtin_ctz(BucketArray::BLOCK_CAPACITY);
    }
  }
  return ret;
}

ObHashJoinOp::ObHashJoinOp(ObExecContext &ctx_, const ObOpSpec &spec, ObOpInput *input)
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
  sql_mem_processor_(profile_, op_monitor_info_),
  state_(JS_READ_RIGHT),
  cur_right_hash_value_(0),
  right_has_matched_(false),
  tuple_need_join_(false),
  first_get_row_(true),
  drain_mode_(HashJoinDrainMode::NONE_DRAIN),
  cur_bkid_(0),
  remain_data_memory_size_(0),
  nth_nest_loop_(0),
  cur_nth_row_(0),
  dumped_fixed_mem_size_(0),
  hash_table_(),
  cur_hash_table_(nullptr),
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
  batch_round_(1),
  nest_loop_state_(HJLoopState::LOOP_START),
  is_shared_(false),
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
  hash_equal_cnt_(0),
  max_output_cnt_(0),
  hj_part_stored_rows_(NULL),
  hash_vals_(NULL),
  right_brs_(NULL),
  right_hj_part_stored_rows_(NULL),
  right_read_from_stored_(false),
  right_hash_vals_(NULL),
  cur_tuples_(NULL),
  right_batch_traverse_cnt_(0),
  hj_part_added_rows_(NULL),
  part_selectors_(NULL),
  part_selector_sizes_(NULL),
  right_selector_(NULL),
  right_selector_cnt_(0),
  read_null_in_naaj_(false),
  get_next_right_row_func_(nullptr),
  get_next_left_row_func_(nullptr),
  get_next_right_batch_func_(nullptr),
  get_next_left_batch_func_(nullptr),
  non_preserved_side_is_not_empty_(false),
  null_random_hash_value_(0),
  skip_left_null_(false),
  skip_right_null_(false)
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
  state_function_func_[JS_JOIN_END][FT_ITER_GOING] =  &ObHashJoinOp::join_end_operate;
  state_function_func_[JS_JOIN_END][FT_ITER_END] = &ObHashJoinOp::join_end_func_end;
  //open时已对left table的数据做了hash操作，这里对右表进行扫描和计算hash值
  state_operation_func_[JS_READ_RIGHT] = &ObHashJoinOp::read_right_operate;
  state_function_func_[JS_READ_RIGHT][FT_ITER_GOING] = &ObHashJoinOp::calc_right_hash_value;
  state_function_func_[JS_READ_RIGHT][FT_ITER_END] = &ObHashJoinOp::read_right_func_end;
  // 根据hash value去扫描buckets,如果得到匹配的row,组合row并返回。
  // 桶里可能会有多个元組匹配，需要依次扫描。
  state_operation_func_[JS_READ_HASH_ROW] = &ObHashJoinOp::read_hashrow;
  state_function_func_[JS_READ_HASH_ROW][FT_ITER_GOING] = &ObHashJoinOp::read_hashrow_func_going;
  state_function_func_[JS_READ_HASH_ROW][FT_ITER_END] = &ObHashJoinOp::read_hashrow_func_end;
  //for anti
  state_operation_func_[JS_LEFT_ANTI_SEMI] = &ObHashJoinOp::left_anti_semi_operate;
  state_function_func_[JS_LEFT_ANTI_SEMI][FT_ITER_GOING] = &ObHashJoinOp::left_anti_semi_going;
  state_function_func_[JS_LEFT_ANTI_SEMI][FT_ITER_END] = &ObHashJoinOp::left_anti_semi_end;
  //for unmatched left row, for left-outer,full-outer,left-semi
  state_operation_func_[JS_FILL_LEFT] = &ObHashJoinOp::fill_left_operate;
  state_function_func_[JS_FILL_LEFT][FT_ITER_GOING] = &ObHashJoinOp::fill_left_going;
  state_function_func_[JS_FILL_LEFT][FT_ITER_END] = &ObHashJoinOp::fill_left_end;
  
  get_next_right_row_func_ = &ObHashJoinOp::get_next_right_row;
  get_next_left_row_func_ = &ObHashJoinOp::get_next_left_row;
  get_next_right_batch_func_ = &ObHashJoinOp::get_next_right_batch;
  get_next_left_batch_func_ = &ObHashJoinOp::get_next_left_row_batch;
}

int ObHashJoinOp::set_hash_function(int8_t hash_join_hasher)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<common::ObHashFunc> *cur_hash_funcs = &MY_SPEC.all_hash_funcs_;
  if (DEFAULT_MURMUR_HASH != (hash_join_hasher & HASH_FUNCTION_MASK)) {
    ObHashFunc hash_func;
    if (OB_FAIL(tmp_hash_funcs_.init(MY_SPEC.all_hash_funcs_.count()))) {
      LOG_WARN("failed to init tmp hash func", K(ret));
    } else {
      for (int64_t i = 0; i < MY_SPEC.all_hash_funcs_.count() && OB_SUCC(ret); ++i) {
        if (ENABLE_WY_HASH == (hash_join_hasher & HASH_FUNCTION_MASK)) {
          hash_func.hash_func_ = MY_SPEC.all_join_keys_.at(i)->basic_funcs_->wy_hash_;
        } else  if (ENABLE_XXHASH64 == (hash_join_hasher & HASH_FUNCTION_MASK)) {
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
    left_hash_funcs_.init(
      all_cnt / 2, const_cast<ObHashFunc*>(&cur_hash_funcs->at(0)), all_cnt / 2);
    right_hash_funcs_.init(all_cnt / 2,
      const_cast<ObHashFunc*>(&cur_hash_funcs->at(0) + left_hash_funcs_.count()),
      cur_hash_funcs->count() - left_hash_funcs_.count());
    if (left_hash_funcs_.count() != right_hash_funcs_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: hash func is not match", K(ret), K(cur_hash_funcs->count()),
        K(left_hash_funcs_.count()), K(right_hash_funcs_.count()));
    } else if (MY_SPEC.all_join_keys_.count() != cur_hash_funcs->count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: hash func is not match", K(ret), K(cur_hash_funcs->count()),
        K(MY_SPEC.all_join_keys_.count()));
    }
  }
  return ret;
}

uint64_t sum_alloc_size() { return 0; }

template <typename X, typename Y, typename ...TS>
uint64_t sum_alloc_size(const X &, const Y &y, const TS &...args)
{
  return y + sum_alloc_size(args...);
}

void assign_alloc_ptr(void *) { return; }

template<typename X, typename Y, typename ...TS>
void assign_alloc_ptr(void *ptr, const X &x, const Y &y, const TS &...args)
{
  const_cast<X &>(x) = static_cast<X>(ptr);
  assign_alloc_ptr(static_cast<char *>(ptr) + y, args...);
}

// allocate pointers which passed <ptr, size> paires
template <typename ...TS>
int alloc_ptrs(ObIAllocator &alloc, const TS &...args)
{
  int ret = OB_SUCCESS;
  int64_t size = sum_alloc_size(args...);
  void *ptr = alloc.alloc(size);
  if (NULL == ptr) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(size));
  } else {
    MEMSET(ptr, 0, size);
    assign_alloc_ptr(ptr, args...);
  }
  return ret;
}

int ObHashJoinOp::inner_open()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
  if (OB_FAIL(set_shared_info())) {
    LOG_WARN("failed to set shared info", K(ret));
  } else if (is_shared_ && OB_FAIL(sync_wait_open())) {
    is_shared_ = false;
    LOG_WARN("failed to sync open for shared hj", K(ret));
  } else if ((OB_UNLIKELY(MY_SPEC.all_join_keys_.count() <= 0
      || MY_SPEC.all_join_keys_.count() != MY_SPEC.all_hash_funcs_.count()
      || OB_ISNULL(left_)))) {
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
    if (is_vectorized()) {
      if (INNER_JOIN == MY_SPEC.join_type_) {
        going_func_ = &ObHashJoinOp::inner_join_read_hashrow_going_batch;
        end_func_ = &ObHashJoinOp::inner_join_read_hashrow_end_batch;
      } else if (IS_LEFT_SEMI_ANTI_JOIN(MY_SPEC.join_type_)) {
        going_func_ = &ObHashJoinOp::left_anti_semi_read_hashrow_going_batch;
        end_func_ = &ObHashJoinOp::left_anti_semi_read_hashrow_end_batch;
      } else if (IS_RIGHT_SEMI_ANTI_JOIN(MY_SPEC.join_type_)) {
        going_func_ = &ObHashJoinOp::right_anti_semi_read_hashrow_going_batch;
        end_func_ = &ObHashJoinOp::right_anti_semi_read_hashrow_end_batch;
      } else if (IS_OUTER_JOIN(MY_SPEC.join_type_)) {
        going_func_ = &ObHashJoinOp::outer_join_read_hashrow_going_batch;
        end_func_ = &ObHashJoinOp::outer_join_read_hashrow_end_batch;
      }
    } else {
      if (INNER_JOIN == MY_SPEC.join_type_) {
        going_func_ = &ObHashJoinOp::inner_join_read_hashrow_func_going;
        end_func_ = &ObHashJoinOp::inner_join_read_hashrow_func_end;
      } else {
        going_func_ = &ObHashJoinOp::other_join_read_hashrow_func_going;
        end_func_ = &ObHashJoinOp::other_join_read_hashrow_func_end;
      }
    }
    if (MY_SPEC.is_naaj_) {
      state_function_func_[JS_LEFT_ANTI_SEMI][FT_ITER_GOING] = &ObHashJoinOp::left_anti_naaj_going;
      get_next_right_row_func_ = &ObHashJoinOp::get_next_right_row_na;
      get_next_left_row_func_ = &ObHashJoinOp::get_next_left_row_na;
      get_next_right_batch_func_ = &ObHashJoinOp::get_next_right_batch_na;
      get_next_left_batch_func_ = &ObHashJoinOp::get_next_left_row_batch_na;
    }
    skip_left_null_ = (INNER_JOIN == MY_SPEC.join_type_) || (LEFT_SEMI_JOIN == MY_SPEC.join_type_)
                      || (RIGHT_SEMI_JOIN == MY_SPEC.join_type_) || (RIGHT_OUTER_JOIN == MY_SPEC.join_type_);
    skip_right_null_ = (INNER_JOIN == MY_SPEC.join_type_) || (LEFT_SEMI_JOIN == MY_SPEC.join_type_)
                      || (RIGHT_SEMI_JOIN == MY_SPEC.join_type_) || (LEFT_OUTER_JOIN == MY_SPEC.join_type_);
  }
  if (OB_SUCC(ret)) {
    int64_t all_cnt = MY_SPEC.all_join_keys_.count();
    left_join_keys_.init(
      all_cnt / 2, const_cast<ObExpr**>(&MY_SPEC.all_join_keys_.at(0)), all_cnt / 2);
    right_join_keys_.init(all_cnt - left_join_keys_.count(),
      const_cast<ObExpr**>(&MY_SPEC.all_join_keys_.at(0) + left_join_keys_.count()),
      all_cnt - left_join_keys_.count());
    if (left_join_keys_.count() != right_join_keys_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: left and right join is not match", K(ret),
        K(left_join_keys_.count()), K(right_join_keys_.count()));
    } else if (MY_SPEC.is_naaj_ && 1 != left_join_keys_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null aware anti join only support 1 join key", K(ret));
    }
    LOG_DEBUG("trace join keys", K(left_join_keys_), K(right_join_keys_),
      K(left_join_keys_.count()), K(right_join_keys_.count()));
  }
  if (OB_SUCC(ret)) {
    part_count_ = 0;
    hj_state_ = ObHashJoinOp::INIT;
    void* buf = alloc_->alloc(sizeof(ObHashJoinBufMgr));
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc mem");
    } else {
      //at least one page for each l/r part
      buf_mgr_ = new (buf) ObHashJoinBufMgr();
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
  if (OB_SUCC(ret) && is_vectorized()) {
    const int64_t batch_size = MY_SPEC.max_batch_size_;
    OZ(alloc_ptrs(mem_context_->get_arena_allocator(),
                  hj_part_stored_rows_, sizeof(*hj_part_stored_rows_) * batch_size,
                  hash_vals_, sizeof(*hash_vals_) * batch_size,
                  right_hj_part_stored_rows_, sizeof(*right_hj_part_stored_rows_) * batch_size,
                  right_hash_vals_, sizeof(*right_hash_vals_) * batch_size,
                  cur_tuples_, sizeof(*cur_tuples_) * batch_size,
                  child_brs_.skip_, ObBitVector::memory_size(batch_size),
                  hj_part_added_rows_, sizeof(hj_part_added_rows_) * batch_size,
                  right_selector_, sizeof(*right_selector_) * batch_size));
  }
  cur_hash_table_ = &hash_table_;
  return ret;
}

int ObHashJoinOp::set_shared_info()
{
  int ret = OB_SUCCESS;
  ObHashJoinInput *hj_input = static_cast<ObHashJoinInput*>(input_);
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

void ObHashJoinOp::reset_base()
{
  drain_mode_ = HashJoinDrainMode::NONE_DRAIN;
  hj_state_ = INIT;
  hj_processor_ = NONE;
  part_level_ = 0;
  part_shift_ = MAX_PART_LEVEL << 3;
  part_count_ = 0;
  input_size_ = 0;
  predict_row_cnt_ = 1024;
  left_batch_ = nullptr;
  right_batch_ = nullptr;
  dumped_fixed_mem_size_ = 0;
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
    for (int64_t i = 0; i < part_count_; i ++) {
      hj_part_array_[i].~ObHashJoinPartition();
    }
    if (OB_NOT_NULL(alloc_)) {
      alloc_->free(hj_part_array_);
    }
    hj_part_array_ = NULL;
  }
  if (right_hj_part_array_ != NULL) {
    for (int64_t i = 0; i < part_count_; i ++) {
      right_hj_part_array_[i].~ObHashJoinPartition();
    }
    if (OB_NOT_NULL(alloc_)) {
      alloc_->free(right_hj_part_array_);
    }
    right_hj_part_array_ = NULL;
  }
  int64_t tmp_part_count = 0 < level2_part_count_ ?
                          level1_part_count_ * level2_part_count_ :
                          level1_part_count_;
  if (OB_NOT_NULL(part_histograms_)) {
    for (int64_t i = 0; i < tmp_part_count; i ++) {
      part_histograms_[i].~HashJoinHistogram();
    }
    if (OB_NOT_NULL(alloc_)) {
      alloc_->free(part_histograms_);
    }
    part_histograms_ = NULL;
  }
  if (OB_NOT_NULL(part_selectors_)) {
    if (OB_NOT_NULL(alloc_)) {
      alloc_->free(part_selectors_);
    }
    part_selectors_ = nullptr;
    part_selector_sizes_ = nullptr;
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
  right_read_from_stored_ = false;
  right_batch_traverse_cnt_ = 0;
  null_random_hash_value_ = 0;
}

int ObHashJoinOp::part_rescan(bool reset_all)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session = NULL;
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

int ObHashJoinOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(part_rescan(true))) {
    LOG_WARN("part rescan failed", K(ret));
  } else if (OB_FAIL(ObJoinOp::inner_rescan())) {
    LOG_WARN("join rescan failed", K(ret));
  } else {
    batch_round_ = 1;
    iter_end_ = false;
    read_null_in_naaj_ = false;
    non_preserved_side_is_not_empty_ = false;
  }
  LOG_TRACE("hash join rescan", K(ret));
  return ret;
}

int ObHashJoinOp::do_sync_wait_all()
{
  int ret = OB_SUCCESS;
  if (!batch_reach_end_ && !row_reach_end_ && is_shared_) {
    // In shared hash join, it only has two state: one is that it don't build hash table,
    // and one is that it return result after probe
    // so we only do:
    // 1. sync to build hash table
    // 2. set join_end and sync to do next_batch

    // don't get any right data, we think the right is iter_end
    if (ObHashJoinOp::HJState::INIT == hj_state_) {
      // don't build hash table
      // it must build hash table to guarantee to hold all data
      drain_mode_ = HashJoinDrainMode::BUILD_HT;
    } else if (ObHashJoinOp::HJState::NORMAL == hj_state_) {
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
      if (!is_vectorized()) {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(get_next_row())) {
            if (OB_ITER_END == ret) {
              LOG_WARN("failed to inner get next row", K(ret));
            }
          }
        } // end while
      } else {
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
    }
    LOG_TRACE("debug shared hash join wait all to drain", K(ret));
  }
  return ret;
}

// need add sync to wait exit for shared hash join
int ObHashJoinOp::inner_drain_exch()
{
  return do_sync_wait_all();
}

int ObHashJoinOp::next()
{
  int ret = OB_SUCCESS;
  state_operation_func_type state_operation = NULL;
  state_function_func_type state_function = NULL;
  ObJoinState &state =  state_;
  int func = -1;
  while(OB_SUCC(ret) && !need_return_) {
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
          //iter_end ,break;
        } else {
          LOG_WARN("failed state function", K(ret), K(state), K(func));
        }
      }
    }
  } //while end
  return ret;
}

// 这里有几个逻辑需要理下：
// 1）当返回一行时，如果left对应bucket没有元素，即cur_tuple为nullpr，会继续get_right_row，所以不需要save
// 2）如果不会拿下一个right，则需要restore上次保存的right，继续看是否匹配left其他行
//      这里有两种情况：
//        a）如果没有dump，恢复output
//        b）如果dump，则仅恢复right_read_row_，至于是否放入到right output，看下一次是否返回避免多余copy
int ObHashJoinOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool exit_while = false;
  need_return_ = false;
  if (is_vectorized()) {
    if (OB_UNLIKELY(iter_end_)) {
      brs_.size_ = 0;
      brs_.end_ = true;
      exit_while = true;
    }
  } else {
    has_fill_left_row_ = false;
  }
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
      // It must firstly sync wait, and then remove undumped batch
      if (is_shared_ && OB_FAIL(sync_wait_fetch_next_batch())) {
        LOG_WARN("failed to sync wait fetch next batch", K(ret));
      } else {
        batch_mgr_->remove_undumped_batch(is_shared_ ? cur_dumped_partition_ : INT64_MAX, batch_round_);
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
      }
      ObHashJoinBatchPair batch_pair;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(part_rescan(false))) {
        LOG_WARN("fail to reopen hj", K(ret));
      } else if (read_null_in_naaj_) {
        // if read null in naaj, return the empty set directly
        ret = OB_ITER_END;
      } else if (OB_FAIL(batch_mgr_->next_batch(batch_pair))) {
      } else if (0 != buf_mgr_->get_total_alloc_size()) {
        LOG_WARN("expect memory count is ok", K(ret), K(buf_mgr_->get_total_alloc_size()));
      }
      dumped_fixed_mem_size_ = get_cur_mem_used();
      if (OB_ITER_END == ret) {
        exit_while = true;
        // free resource like memory
        iter_end_ = true;
        part_rescan(true);
        if (is_vectorized()) {
          brs_.size_ = 0;
          brs_.end_ = true;
          ret = OB_SUCCESS;
        }
        LOG_TRACE("debug iter end", K(spec_.id_));
      } else if (OB_SUCCESS == ret) {
        ++batch_round_;
        left_batch_ = batch_pair.left_;
        right_batch_ = batch_pair.right_;

        part_level_ = batch_pair.left_->get_part_level() + 1;
        part_shift_ = batch_pair.left_->get_part_shift();

        // asynchronously wait to write while only read the dumped partition
        if (OB_FAIL(left_batch_->get_chunk_row_store().finish_add_row(true))) {
          LOG_WARN("finish dump failed", K(ret));
        } else if (OB_FAIL(right_batch_->get_chunk_row_store().finish_add_row(true))) {
          LOG_WARN("finish dump failed", K(ret));
        }
        batch_pair.left_->open();
        batch_pair.right_->open();

        if (sizeof(uint64_t) * CHAR_BIT <= part_shift_) {
          // avoid loop recursively
          // 至多MAX_PART_LEVEL,最后一层要么nest loop，要么in-memory
          // hash join dumped too many times, the part level is greater than 32 bit
          // we report 4013 instead of 4016, and remind user to increase memory
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("too deep part level", K(ret), K(part_level_), K(part_shift_));
        } else {
          hj_state_ = ObHashJoinOp::HJState::NORMAL;
        }
        LOG_DEBUG("trace batch", K(batch_pair.left_->get_batchno()),
          K(batch_pair.right_->get_batchno()), K(part_level_), K(batch_round_));
      } else {
        LOG_WARN("fail get next batch", K(ret));
      }
      break;
    }
    }
  }
  clear_evaluated_flag();
  return ret;
}

void ObHashJoinOp::destroy()
{
  sql_mem_processor_.unregister_profile_if_necessary();
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
  if (is_shared_) {
    IGNORE_RETURN sync_wait_close();
  }
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
  }
  if (nullptr != mem_context_) {
    mem_context_->reuse();
  }
  if (OB_FAIL(ObJoinOp::inner_close())) {
  }
  return ret;
}

int ObHashJoinOp::join_end_operate()
{
  return OB_ITER_END;
}

int ObHashJoinOp::join_end_func_end()
{
  int ret = OB_SUCCESS;
  LOG_TRACE("trace hash join probe statistics", K(bitset_filter_cnt_), K(probe_cnt_),
    K(hash_equal_cnt_), K(hash_link_cnt_));
  // nest loop process one block, and need next block
  if (HJProcessor::NEST_LOOP == hj_processor_
      && HJLoopState::LOOP_GOING == nest_loop_state_ && !read_null_in_naaj_ && !is_shared_) {
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
    } else if (OB_FAIL(OB_I(t1) left_batch_->get_next_row(
        left_->get_spec().output_, eval_ctx_, left_read_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get left row from partition failed", K(ret));
      }
    }
    LOG_DEBUG("part join ctx get left row", K(ret));
  }
  return ret;
}

int ObHashJoinOp::get_next_left_row_na()
{
  int ret = common::OB_SUCCESS;
  left_row_joined_ = false;
  int got_row = false;
  bool is_left = true;
  if (left_batch_ == NULL) {
    while (OB_SUCC(ret) && !got_row) {
      bool is_null = false;
      if (OB_FAIL(OB_I(t1) left_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get left row from child failed", K(ret));
        }
      } else if (FALSE_IT(non_preserved_side_is_not_empty_
                          |= (RIGHT_ANTI_JOIN == MY_SPEC.join_type_))) {
        // mark this to forbid null value output in get_next_right_row_na
      } else if (is_right_naaj() && OB_FAIL(check_join_key_for_naaj(is_left, is_null))) {
        LOG_WARN("failed to check null for right naaj", K(ret));
      } else if (is_null) {
        //right_anti_join_na : return iter_end
        //right_anti_join_sna : impossible to get null
        //left_anti_join : got row
        if (is_right_naaj_na()) {
          read_null_in_naaj_ = true;
          ret = OB_ITER_END;
          if (is_shared_) {
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = left_->drain_exch())) {
              LOG_WARN("failed to drain exch", K(ret), K(tmp_ret));
              ret = tmp_ret;
            }
          }
          LOG_TRACE("right naaj null break");
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("get null value in non preserved side, may generated wrong plan",
                    K(ret), K(MY_SPEC.join_type_), K(MY_SPEC.is_naaj_), K(MY_SPEC.is_sna_));
        }
      } else {
        got_row = true;
      }
    }
  } else {
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(OB_I(t1) left_batch_->get_next_row(
        left_->get_spec().output_, eval_ctx_, left_read_row_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get left row from partition failed", K(ret));
      }
    }
    LOG_DEBUG("part join ctx get left row", K(ret));
  }
  return ret;
}

int ObHashJoinOp::get_next_left_row_batch(bool is_from_row_store,
                                          const ObBatchRows *&child_brs)
{
  int ret = common::OB_SUCCESS;
  left_row_joined_ = false;
  if (!is_from_row_store) {
    if (OB_FAIL(left_->get_next_batch(max_output_cnt_, child_brs))) {
      LOG_WARN("get left row from child failed", K(ret));
    } else if (child_brs->end_ && 0 == child_brs->size_) {
      // When reach here, projected flag has been set to false.
      // In the hash join operator, the datum corresponding to
      // the child output expr may be covered from the datum store;
      // for opt, we not set evaluted flag when convert exprs,
      // and depended projected flag is true
      FOREACH_CNT_X(e, left_->get_spec().output_, OB_SUCC(ret)) {
        (*e)->get_eval_info(eval_ctx_).projected_ = true;
      }
    }
  } else {
    int64_t read_size = 0;
    child_brs = &child_brs_;
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(left_batch_->get_next_batch(
                       left_->get_spec().output_, eval_ctx_, MY_SPEC.max_batch_size_,
                       read_size, hj_part_stored_rows_))) {
      if (OB_ITER_END == ret) {
        const_cast<ObBatchRows *>(child_brs)->size_ = 0;
        const_cast<ObBatchRows *>(child_brs)->end_ = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next batch", K(ret));
      }
    } else {
      LOG_TRACE("get_row from row_store", K(left_read_row_));
      const_cast<ObBatchRows *>(child_brs)->size_ = read_size;
      const_cast<ObBatchRows *>(child_brs)->end_ = false;
      const_cast<ObBatchRows *>(child_brs)->skip_->reset(read_size);
    }
    LOG_DEBUG("part join ctx get left row", K(ret));
  }
  return ret;
}

int ObHashJoinOp::get_next_left_row_batch_na(bool is_from_row_store, const ObBatchRows *&child_brs)
{
  int ret = OB_SUCCESS;
  left_row_joined_ = false;
  bool is_left = true;
  bool has_null = false;
  if (!is_from_row_store) {
    if (OB_FAIL(left_->get_next_batch(max_output_cnt_, child_brs))) {
      LOG_WARN("get left row from child failed", K(ret));
    } else if (child_brs->end_ && 0 == child_brs->size_) {
      // When reach here, projected flag has been set to false.
      // In the hash join operator, the datum corresponding to
      // the child output expr may be covered from the datum store;
      // for opt, we not set evaluted flag when convert exprs,
      // and depended projected flag is true
      FOREACH_CNT_X(e, left_->get_spec().output_, OB_SUCC(ret)) {
        (*e)->get_eval_info(eval_ctx_).projected_ = true;
      }
    } else if (FALSE_IT(non_preserved_side_is_not_empty_
                        |= (RIGHT_ANTI_JOIN == MY_SPEC.join_type_))) {
      // mark this to forbid null value output in get_next_right_batch_na
    } else if (is_right_naaj()
              && OB_FAIL(check_join_key_for_naaj_batch(is_left, child_brs->size_,
                                                       has_null, child_brs))) {
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
      LOG_TRACE("right naaj null break", K(ret));
    }
  } else {
    int64_t read_size = 0;
    child_brs = &child_brs_;
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(left_batch_->get_next_batch(
                       left_->get_spec().output_, eval_ctx_, MY_SPEC.max_batch_size_,
                       read_size, hj_part_stored_rows_))) {
      if (OB_ITER_END == ret) {
        const_cast<ObBatchRows *>(child_brs)->size_ = 0;
        const_cast<ObBatchRows *>(child_brs)->end_ = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next batch", K(ret));
      }
    } else {
      LOG_TRACE("get_row from row_store", K(left_read_row_));
      const_cast<ObBatchRows *>(child_brs)->size_ = read_size;
      const_cast<ObBatchRows *>(child_brs)->end_ = false;
      const_cast<ObBatchRows *>(child_brs)->skip_->reset(read_size);
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
  if (top_part_level() || 0 == hash_table_.nbuckets_
    || NEST_LOOP != hj_processor_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected hash buckets number is 0", K(ret), K(part_level_));
  } else if (OB_FAIL(calc_basic_info())) {
    LOG_WARN("failed to calc basic info", K(ret));
  } else {
    // reuse buckets
    PartHashJoinTable &hash_table = hash_table_;

    int64_t row_count = profile_.get_row_count();
    if (row_count > hash_table.row_count_) {
      hash_table.row_count_ = row_count;
    }
    if (profile_.get_bucket_size() > hash_table.nbuckets_) {
      hash_table.nbuckets_ = profile_.get_bucket_size();
    }
    // set bucket to zero.
    hash_table.buckets_->reuse();
    OZ(hash_table.buckets_->init(hash_table.nbuckets_));
    hash_table.collisions_ = 0;
    hash_table.used_buckets_ = 0;

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_bloom_filter(mem_context_->get_malloc_allocator(), hash_table_.nbuckets_))) {
      LOG_WARN("failed to create bloom filter", K(ret));
    } else if (OB_FAIL(right_batch_->rescan())) {
      LOG_WARN("failed to rescan right", K(ret));
    } else {
      nth_right_row_ = -1;
    }
    LOG_TRACE("trace hash table", K(ret), K(hash_table.nbuckets_), K(row_count),
      K(nth_right_row_), K(profile_.get_row_count()));
  }
  return ret;
}

int ObHashJoinOp::load_next()
{
  int ret = OB_SUCCESS;
  ++nth_nest_loop_;
  // 目前通过设置一定读固定大小方式来读取内容，后续会改掉
  if (1 == nth_nest_loop_ && OB_FAIL(left_batch_->set_iterator())) {
    LOG_WARN("failed to set iterator", K(ret), K(nth_nest_loop_));
  } else if (1 == nth_nest_loop_ && OB_FAIL(prepare_hash_table())) {
    LOG_WARN("failed to prepare hash table", K(ret), K(nth_nest_loop_));
  } else if (1 < nth_nest_loop_ && OB_FAIL(reuse_for_next_chunk())) {
    LOG_WARN("failed to reset info for block", K(ret), K(nth_nest_loop_));
  }
  return ret;
}

int ObHashJoinOp::build_hash_table_for_nest_loop(int64_t &num_left_rows)
{
  int ret = OB_SUCCESS;
  uint64_t hash_value = 0;
  num_left_rows = 0;
  ObHashJoinStoredJoinRow *stored_row = nullptr;
  nest_loop_state_ = HJLoopState::LOOP_GOING;
  is_last_chunk_ = false;
  PartHashJoinTable &hash_table = hash_table_;
  ObHashJoinBatch *hj_batch = left_batch_;
  const int64_t PREFETCH_BATCH_SIZE = 64;
  const ObHashJoinStoredJoinRow *left_stored_rows[PREFETCH_BATCH_SIZE];
  if (OB_FAIL(load_next())) {
    LOG_WARN("failed to reset info for block", K(ret));
  } else {
    int64_t curr_ht_row_cnt = 0;
    int64_t curr_ht_memory_size = 0;
    // at least hold 1 block in memory
    int64_t memory_bound = std::max(remain_data_memory_size_,
                                    hj_batch->get_chunk_row_store().get_max_blk_size());
    const int64_t row_bound = hash_table.nbuckets_ / 2;
    hj_batch->set_iteration_age(iter_age_);
    iter_age_.inc();
    while (OB_SUCC(ret)) {
      int64_t read_size = 0;
      if (OB_FAIL(hj_batch->get_next_batch(left_stored_rows,
                                           std::min(PREFETCH_BATCH_SIZE,
                                                    row_bound - curr_ht_row_cnt),
                                           read_size))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next batch failed", K(ret), K(row_bound), K(curr_ht_row_cnt), K(hash_table.nbuckets_));
        }
      } else if (OB_ISNULL(left_stored_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("returned left_stored_rows is NULL", K(ret));
      } else {
        if (enable_bloom_filter_) {
          for (int64_t i = 0; OB_SUCC(ret) && i < read_size; ++i) {
            if (OB_FAIL(bloom_filter_->set(left_stored_rows[i]->get_hash_value()))) {
              LOG_WARN("add hash value to bloom failed", K(ret), K(i));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_UNLIKELY(NULL == hash_table.buckets_)) {
            // do nothing
          } else {
            auto mask = hash_table.nbuckets_ - 1;
            for(auto i = 0; i < read_size; i++) {
              __builtin_prefetch((&hash_table.buckets_->at(left_stored_rows[i]->get_hash_value() & mask)), 1 /* write */, 3 /* high temporal locality*/);
            }
            for (int64_t i = 0; OB_SUCC(ret) && i < read_size; ++i) {
              curr_ht_memory_size += left_stored_rows[i]->row_size_;
              hash_table.set(left_stored_rows[i]->get_hash_value(), const_cast<ObHashJoinStoredJoinRow *>(left_stored_rows[i]));
            }
          }
        }
        if (OB_SUCC(ret)) {
          curr_ht_row_cnt += read_size;
          num_left_rows += read_size;
          cur_nth_row_ += read_size;
          if (curr_ht_row_cnt >= row_bound
              || curr_ht_memory_size >= memory_bound) {
            ret = OB_ITER_END;
          }
        }
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    if (!hj_batch->has_next()) {
      // last chunk
      is_last_chunk_ = true;
      nest_loop_state_ = HJLoopState::LOOP_END;
      if (cur_nth_row_ != hj_batch->get_row_count_on_disk()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect row count is match", K(ret), K(cur_nth_row_),
          K(hj_batch->get_row_count_on_disk()));
      }
    }
  }
  if (OB_SUCC(ret)) {
    trace_hash_table_collision(num_left_rows);
  }
  LOG_TRACE("trace block hash join", K(nth_nest_loop_), K(part_level_), K(ret), K(num_left_rows),
    K(hash_table_.nbuckets_));
  return ret;
}

int ObHashJoinOp::nest_loop_process(bool &need_not_read_right)
{
  int ret = OB_SUCCESS;
  need_not_read_right = false;
  int64_t num_left_rows = 0;
  postprocessed_left_ = true;
  if (OB_FAIL(build_hash_table_for_nest_loop(num_left_rows))) {
    LOG_WARN("failed to build hash table", K(ret), K(part_level_));
  }
  if (OB_SUCC(ret)
      && ((0 == num_left_rows
          && RIGHT_ANTI_JOIN != MY_SPEC.join_type_
          && RIGHT_OUTER_JOIN != MY_SPEC.join_type_
          && FULL_OUTER_JOIN != MY_SPEC.join_type_) || read_null_in_naaj_)) {
    need_not_read_right = true;
    LOG_DEBUG("[HASH JOIN]Left table is empty, skip reading right table.",
        K(num_left_rows), K(MY_SPEC.join_type_));
  }
  return ret;
}

// given partition size and input size to calculate partition count
int64_t ObHashJoinOp::calc_partition_count(
  int64_t input_size, int64_t part_size, int64_t max_part_count)
{
  int64_t estimate_part_count = input_size / part_size + 1;
  int64_t partition_cnt = 8;
  estimate_part_count = max(0, estimate_part_count);
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
  global_mem_bound_size = max(0, global_mem_bound_size);
  while (partition_cnt * PAGE_SIZE > global_mem_bound_size) {
    partition_cnt >>= 1;
  }
  partition_cnt = partition_cnt < MIN_PART_COUNT ?
                        MIN_PART_COUNT :
                        partition_cnt;
  return partition_cnt;
}

int64_t ObHashJoinOp::calc_max_data_size(const int64_t extra_memory_size)
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
int ObHashJoinOp::get_max_memory_size(int64_t input_size)
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
    // part_count_ = calc_partition_count(input_size, memory_size, max_partition_count_per_level_);
    part_count_ = calc_partition_count_by_cache_aware(
      profile_.get_row_count(), max_partition_count_per_level_, memory_size);
    if (!top_part_level()) {
      if (OB_ISNULL(left_batch_) || OB_ISNULL(right_batch_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect status: left op or right op is null", K(left_batch_),
          K(right_batch_));
      } else {
        // switch callback for count memory size
        left_batch_->set_callback(&sql_mem_processor_);
      }
    }
    LOG_TRACE("trace auto memory manager", K(hash_area_size), K(part_count_),
      K(input_size), K(extra_memory_size), K(profile_.get_expect_size()),
      K(profile_.get_cache_size()));
  } else {
    // part_count_ = calc_partition_count(
    //   input_size, sql_mem_processor_.get_mem_bound(), max_partition_count_per_level_);
    part_count_ = calc_partition_count_by_cache_aware(
      profile_.get_row_count(), max_partition_count_per_level_, sql_mem_processor_.get_mem_bound());
    LOG_TRACE("trace auto memory manager", K(hash_area_size), K(part_count_),
      K(input_size));
  }
  buf_mgr_->reuse();
  buf_mgr_->set_reserve_memory_size(remain_data_memory_size_, 1.0);
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sync_wait_part_count())) {
      LOG_WARN("failed to sync part count", K(ret));
    }
  }
  if (OB_SUCC(ret) && is_vectorized()) {
    char *buf = NULL;
    if (part_count_ <= 0) {
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
  }

  return ret;
}

int64_t ObHashJoinOp::calc_bucket_number(const int64_t row_count)
{
  int64_t bucket_cnt = next_pow2(row_count * RATIO_OF_BUCKETS);
  // some test data from ssb:
  //    extra_ratio = 2 -> cpu used downgrades 16.7% from 24.6%
  //    extra_ratio = 4 -> cpu used downgrades 13.9% from 16.7%
  if (bucket_cnt * 2 * sizeof(HTBucket) <= INIT_L2_CACHE_SIZE) {
    bucket_cnt *= 2;
  }
  return bucket_cnt;
}

// calculate bucket number by real row count
int ObHashJoinOp::calc_basic_info(bool global_info)
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
      if (global_info) {
        if (!is_shared_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: global info must be shared hash join", K(ret));
        } else {
          ObHashJoinInput *hj_input = static_cast<ObHashJoinInput*>(input_);
          row_count = hj_input->get_total_memory_row_count();
          input_size = hj_input->get_total_memory_size();
          LOG_DEBUG("debug row_count and input_size",
            K(hj_input->get_total_memory_row_count()),
            K(hj_input->get_total_memory_size()));
        }
      } else {
        for (int64_t i = 0; i < part_count_; ++i) {
          row_count += hj_part_array_[i].get_row_count_in_memory();
          input_size += hj_part_array_[i].get_size_in_memory();
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
        ObHashJoinInput *hj_input = static_cast<ObHashJoinInput*>(input_);
        row_count = hj_input->get_total_memory_row_count();
        input_size = hj_input->get_total_memory_size();
        LOG_DEBUG("debug hj_input row_count and input_size",
          K(hj_input->get_total_memory_row_count()),
          K(hj_input->get_total_memory_size()));
      }
    } else if (nullptr != left_batch_) {
      row_count = left_batch_->get_chunk_row_store().get_row_cnt();
      // INMEMORY processor, read entire partition into memory
      input_size = left_batch_->get_chunk_row_store().get_file_size();
      LOG_DEBUG("debug left_batch row_count and input_size",
                 K(left_batch_->get_chunk_row_store().get_row_cnt()),
                 K(left_batch_->get_chunk_row_store().get_file_size()));
    }
    LOG_DEBUG("debug row_count and input_size", K(row_count), K(global_info), K(is_shared_));
  } else if (nullptr != left_batch_) {
    // NESTLOOP processor, our memory is not enough to hold entire left partition,
    // try to estimate max row count we can hold
    row_count = 0;
    if (left_batch_->get_chunk_row_store().get_row_cnt() > 0) {
      double avg_len = static_cast<double> (left_batch_->get_chunk_row_store().get_file_size())
                        / left_batch_->get_chunk_row_store().get_row_cnt();
      row_count = std::max(static_cast<int64_t> (MIN_BATCH_ROW_CNT_NESTLOOP),
                                    static_cast<int64_t> (remain_data_memory_size_ / avg_len));

    } else {
      row_count = MIN_BATCH_ROW_CNT_NESTLOOP;
    }
    LOG_TRACE("calc row count", K(left_batch_->get_chunk_row_store().get_file_size()),
                                K(left_batch_->get_chunk_row_store().get_row_cnt()),
                                K(remain_data_memory_size_));
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
    if (OB_ISNULL(left_batch_) || OB_ISNULL(right_batch_)
        || 0 != left_batch_->get_size_in_memory() || 0 != right_batch_->get_size_in_memory()) {
      ret = OB_ERR_UNEXPECTED;
      if (OB_NOT_NULL(left_batch_)) {
        LOG_WARN("unexpect: partition has memory row", K(left_batch_->get_size_in_memory()));
      }
      if (OB_NOT_NULL(right_batch_)) {
        LOG_WARN("unexpect: partition has memory row", K(right_batch_->get_size_in_memory()));
      }
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
      // 这里假设dump的数据是内存部分与left dump部分的比例，即right也可以过滤成比例的数据
      recursive_cost = READ_COST * (l_size + r_size)
                        + (READ_COST + WRITE_COST) * (1 - 0.9 * remain_data_memory_size_ / l_size)
                        * (l_size + r_size);
      nest_loop_count = l_size / (remain_data_memory_size_ - 1) + 1;
      nest_loop_cost = (l_size + nest_loop_count * r_size) * READ_COST;
      pre_total_size = left_batch_->get_pre_total_size();
      // 认为skew场景
      // 1. 占比超过总数的70%
      // 2. 第二轮开始按照了真实size进行partitioning，理论上第三轮one pass会结束
      //      所以在分区个数不是max_partition_count情况下，认为到了level比较高，认为可能有skew
      is_skew = (pre_total_size * DUMP_RATIO / 100 < l_size)
              || (3 <= part_level_ && max_partition_count_per_level_ != left_batch_->get_pre_part_count()
                  && pre_total_size * 30 / 100 < l_size);
      if (enable_recursive && recursive_cost < nest_loop_cost
          && !is_skew
          && MAX_PART_LEVEL > part_level_) {
        // case 2: recursive process
        set_processor(RECURSIVE);
      } else if (enable_nest_loop) {
        // case 3: nest loop process
        if (!need_right_bitset()
            && MAX_NEST_LOOP_RIGHT_ROW_COUNT >= right_batch_->get_row_count_on_disk()
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
      buf_mgr_->set_reserve_memory_size(remain_data_memory_size_, 1.0);
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
            K(part_level_), K(part_count_), K(hash_table_.nbuckets_),
            K(pre_total_size), K(recursive_cost), K(nest_loop_cost), K(is_skew),
            K(l_size), K(r_size));
        } else {
          set_processor(NEST_LOOP);
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no processor", K(part_level_), K(hj_processor_),
          K(part_level_), K(part_count_), K(hash_table_.nbuckets_),
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
      K(part_level_), K(part_count_), K(hash_table_.nbuckets_),
      K(pre_total_size), K(recursive_cost), K(nest_loop_cost), K(is_skew),
      K(l_size), K(r_size), K(remain_data_memory_size_), K(profile_.get_expect_size()),
      K(profile_.get_bucket_size()), K(profile_.get_cache_size()), K(profile_.get_row_count()),
      K(profile_.get_input_size()), K(left_->get_spec().width_));
  return ret;
}

int ObHashJoinOp::build_hash_table_in_memory(int64_t &num_left_rows)
{
  int ret = OB_SUCCESS;
  uint64_t hash_value = 0;
  num_left_rows = 0;
  ObHashJoinStoredJoinRow *stored_row = nullptr;
  ObHashJoinBatch *hj_batch = left_batch_;
  const int64_t PREFETCH_BATCH_SIZE = 64;
  const ObHashJoinStoredJoinRow *left_stored_rows[PREFETCH_BATCH_SIZE];
  int64_t used_buckets = 0;
  int64_t collisions = 0;
  if (OB_FAIL(left_batch_->set_iterator())) {
    LOG_WARN("failed to set iterator", K(ret));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(prepare_hash_table())) {
      LOG_WARN("failed to prepare hash table", K(ret));
    }
  } else if (is_shared_ && OB_ITER_END == ret) {
    if (OB_FAIL(prepare_hash_table())) {
      LOG_WARN("failed to prepare hash table", K(ret));
    } else if (OB_FAIL(sync_wait_finish_build_hash())) {
      LOG_WARN("failed to wait finish build hash", K(ret));
    } else {
      ret = OB_ITER_END;
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    PartHashJoinTable &hash_table = *cur_hash_table_;
    hj_batch->set_iteration_age(iter_age_);
    iter_age_.inc();
    while (OB_SUCC(ret)) {
      int64_t read_size = 0;
      if (OB_FAIL(hj_batch->get_next_batch(left_stored_rows,
                                           PREFETCH_BATCH_SIZE,
                                           read_size))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next batch failed", K(ret));
        }
      } else if (OB_ISNULL(left_stored_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("returned left_stored_rows is NULL", K(ret));
      } else {
        if (enable_bloom_filter_) {
          for (int64_t i = 0; OB_SUCC(ret) && i < read_size; ++i) {
            if (OB_FAIL(bloom_filter_->set(left_stored_rows[i]->get_hash_value()))) {
              LOG_WARN("add hash value to bloom failed", K(ret), K(i));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_UNLIKELY(NULL == hash_table.buckets_)) {
            // do nothing
          } else {
            auto mask = hash_table.nbuckets_ - 1;
            for(auto i = 0; i < read_size; i++) {
              __builtin_prefetch((&hash_table.buckets_->at(left_stored_rows[i]->get_hash_value() & mask)), 1 /* write */, 3 /* high temporal locality*/);
            }
            if (is_shared_) {
              for (int64_t i = 0; OB_SUCC(ret) && i < read_size; ++i) {
                hash_table.atomic_set(left_stored_rows[i]->get_hash_value(),
                  const_cast<ObHashJoinStoredJoinRow *>(left_stored_rows[i]),
                  used_buckets,
                  collisions);
              }
            } else {
              for (int64_t i = 0; OB_SUCC(ret) && i < read_size; ++i) {
                hash_table.set(left_stored_rows[i]->get_hash_value(), const_cast<ObHashJoinStoredJoinRow *>(left_stored_rows[i]));
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          num_left_rows += read_size;
        }
      }
    }
    if (OB_SUCC(ret) || OB_ITER_END == ret) {
      if (is_shared_) {
        ATOMIC_AAF(&hash_table.used_buckets_, used_buckets);
        ATOMIC_AAF(&hash_table.collisions_, collisions);
        if (OB_FAIL(sync_wait_finish_build_hash())) {
          LOG_WARN("failed to wait finish build hash", K(ret));
        }
      }
      trace_hash_table_collision(num_left_rows);
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  is_last_chunk_ = true;
  LOG_TRACE("trace to finish build hash table in memory", K(ret), K(num_left_rows),
    K(hj_batch->get_row_count_on_disk()), K(hash_table_.nbuckets_));
  return ret;
}

int ObHashJoinOp::in_memory_process(bool &need_not_read_right)
{
  int ret = OB_SUCCESS;
  need_not_read_right = false;
  int64_t num_left_rows = 0;
  postprocessed_left_ = true;
  if (OB_FAIL(build_hash_table_in_memory(num_left_rows))) {
    LOG_WARN("failed to build hash table", K(ret), K(part_level_));
  }
  if (OB_SUCC(ret)
      && !is_shared_
      && ((0 == num_left_rows
          && RIGHT_ANTI_JOIN != MY_SPEC.join_type_
          && RIGHT_OUTER_JOIN != MY_SPEC.join_type_
          && FULL_OUTER_JOIN != MY_SPEC.join_type_) || read_null_in_naaj_)) {
    need_not_read_right = true;
    LOG_DEBUG("[HASH JOIN]Left table is empty, skip reading right table.",
        K(num_left_rows), K(MY_SPEC.join_type_));
  }
  return ret;
}

int ObHashJoinOp::init_join_partition()
{
  int ret = OB_SUCCESS;
  if (0 >= part_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition count is less then 0", K(part_count_), K(ret));
  } else {
    int64_t part_shift = part_shift_;
    int64_t used = sizeof(ObHashJoinPartition) * part_count_;
    void *buf = alloc_->alloc(used);
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf for hj part");
    } else {
      if (part_count_ == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part_count is 0", K(ret), K(part_count_), K(part_level_));
      } else {
        part_shift += min(__builtin_ctz(part_count_), 8);
      }
      hj_part_array_ = new (buf) ObHashJoinPartition[part_count_];
      for (int64_t i = 0; i < part_count_ && OB_SUCC(ret); i ++) {
        if (OB_FAIL(hj_part_array_[i].init(
          part_level_,
          part_shift,
          static_cast<int32_t>(i),
          batch_round_,
          true,
          buf_mgr_,
          batch_mgr_,
          left_batch_,
          left_,
          &sql_mem_processor_,
          sql_mem_processor_.get_dir_id(),
          &io_event_observer_))) {
          LOG_WARN("failed to init partition", K(part_level_));
        }
      }
    }

    if (OB_SUCCESS == ret) {
      int64_t used = sizeof(ObHashJoinPartition) * part_count_;
      void *buf = alloc_->alloc(used);
      if (NULL == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc buf for hj part");
      } else {
        right_hj_part_array_ = new (buf) ObHashJoinPartition[part_count_];
        for (int64_t i = 0; i < part_count_ && OB_SUCC(ret); i ++) {
          if (OB_FAIL(right_hj_part_array_[i].init(
            part_level_,
            part_shift,
            static_cast<int32_t>(i),
            batch_round_,
            false,
            buf_mgr_,
            batch_mgr_,
            right_batch_,
            right_,
            &sql_mem_processor_,
            sql_mem_processor_.get_dir_id(),
            &io_event_observer_))) {
            LOG_WARN("failed to init partition");
          }
        }
      }
    }
  }
  if (nullptr != left_batch_) {
    LOG_TRACE("trace init partition", K(part_count_), K(part_level_),
      K(left_batch_->get_part_level()), K(left_batch_->get_batchno()),
      K(buf_mgr_->get_page_size()));
  } else {
    LOG_TRACE("trace init partition", K(part_count_), K(part_level_),
      K(buf_mgr_->get_page_size()));
  }
  return ret;
}

int ObHashJoinOp::force_dump(bool for_left)
{
  return finish_dump(for_left, true, true);
}

void ObHashJoinOp::update_remain_data_memory_size(
  int64_t row_count,
  int64_t total_mem_size, bool &tmp_need_dump)
{
  double ratio = 1.0;
  need_more_remain_data_memory_size(row_count, total_mem_size, ratio);
  int64_t estimate_remain_size = total_mem_size * ratio;
  remain_data_memory_size_ = estimate_remain_size;
  buf_mgr_->set_reserve_memory_size(remain_data_memory_size_, ratio);
  tmp_need_dump = need_dump();
  LOG_TRACE("trace need more remain memory size", K(total_mem_size), K(row_count),
    K(buf_mgr_->get_total_alloc_size()), K(estimate_remain_size),
    K(predict_row_cnt_), K(tmp_need_dump));
}

bool ObHashJoinOp::need_more_remain_data_memory_size(
  int64_t row_count,
  int64_t total_mem_size,
  double &data_ratio)
{
  int64_t extra_memory_size = 0;
  int64_t bucket_cnt = 0;
  if (!opt_cache_aware_) {
    bucket_cnt = calc_bucket_number(row_count);
    extra_memory_size += bucket_cnt * (sizeof(HTBucket));
    if (enable_bloom_filter_) {
      extra_memory_size += 2 * bucket_cnt / 8;
    }
  } else {
    // estimate memory used when use cache aware hash join
    int64_t cur_partition_in_memory = max_partition_count_per_level_ == cur_dumped_partition_ ?
                                    part_count_ : cur_dumped_partition_ + 1;
    int64_t total_mem_row_count = 0;
    for (int64_t i = 0; i < cur_partition_in_memory; ++i) {
      ObHashJoinPartition &hj_part = right_hj_part_array_[i];
      // maybe empty partition
      total_mem_row_count += hj_part.get_row_count_in_memory();
      extra_memory_size += sizeof(HashJoinHistogram)
                        + HashJoinHistogram::calc_memory_size(hj_part.get_row_count_in_memory());
    }
    if (enable_bloom_filter_) {
      bucket_cnt = calc_bucket_number(total_mem_row_count);
      extra_memory_size += 2 * bucket_cnt / 8;
    }
  }
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
    K(extra_memory_size), K(bucket_cnt), K(row_count), K(buf_mgr_->get_total_alloc_size()),
    K(predict_row_cnt_), K(data_ratio), K(get_mem_used()), K(guess_data_ratio),
    K(get_data_mem_used()), K(sql_mem_processor_.get_mem_bound()),
    K(lbt()));
  return need_more;
}

int ObHashJoinOp::update_remain_data_memory_size_periodically(int64_t row_count, bool &need_dump, bool force_update)
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
    predict_row_cnt_ <<= 1;
    if (force_update) {
      sql_mem_processor_.set_periodic_cnt(tmp_periodic_row_count);
    }
    LOG_TRACE("trace need more remain memory size", K(profile_.get_expect_size()),
      K(row_count), K(buf_mgr_->get_total_alloc_size()), K(predict_row_cnt_),
      K(force_update));
  }
  return ret;
}

// Dump one buffer per partition and one by one partition
// when dump one buffer of the partition, then nextly dump buffer pf the next partition
// and dump the partition again, then asynchronously wait to write that overlap the dump other partitions
// If dumped_size is max, then dump all
int ObHashJoinOp::asyn_dump_partition(
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
  ObHashJoinPartition *dumped_parts = nullptr;
  if (is_left) {
    dumped_parts = hj_part_array_;
  } else {
    dumped_parts = right_hj_part_array_;
  }
  for (int64_t i = part_count_ - 1; i >= start_dumped_part_idx && OB_SUCC(ret); --i) {
    ObHashJoinPartition &dump_part = dumped_parts[i];
    // don'e dump last buffer
    if ((nullptr == pred || pred(i))) {
      if (tmp_dump_all) {
        pre_total_dumped_size += dump_part.get_size_in_memory();
      } else {
        pre_total_dumped_size += dump_part.get_size_in_memory() - dump_part.get_last_buffer_mem_size();
      }
      if (pre_total_dumped_size > dumped_size) {
        last_dumped_partition_idx = i;
        break;
      }
    }
  }
  LOG_TRACE("debug dump partition", K(is_left), K(start_dumped_part_idx),
    K(last_dumped_partition_idx), K(cur_dumped_partition_),
    K(pre_total_dumped_size), K(dumped_size), K(dump_all), K(lbt()));
  // secondly dump one buffer per partition one by one
  bool finish_dump = false;
  while (OB_SUCC(ret) && !finish_dump) {
    finish_dump = true;
    for (int64_t i = last_dumped_partition_idx; i < part_count_ && OB_SUCC(ret); ++i) {
      ObHashJoinPartition &dump_part = dumped_parts[i];
      bool tmp_part_dump = dump_part.is_dumped();
      if ((nullptr == pred || pred(i)) && (dump_part.has_switch_block() || (tmp_dump_all && 0 < dump_part.get_size_in_memory()))) {
        if (OB_FAIL(dump_part.dump(tmp_dump_all, 1))) {
          LOG_WARN("failed to dump partition", K(part_level_), K(i));
        } else if (dump_part.is_dumped() && !tmp_part_dump) {
          // 设置一个limit，后续自动dump
          dump_part.get_batch()->set_memory_limit(1);
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
          (dump_part.has_switch_block() || (tmp_dump_all && 0 < dump_part.get_size_in_memory()))) {
        finish_dump = false;
      }
    } // end for
  } // end while
  LOG_TRACE("debug finish dump partition", K(is_left), K(start_dumped_part_idx),
    K(last_dumped_partition_idx), K(cur_dumped_partition_), K(lbt()));
  if (OB_SUCC(ret) && last_dumped_partition_idx - 1 < cur_dumped_partition_) {
    cur_dumped_partition_ = last_dumped_partition_idx - 1;
  }
  return ret;
}

int ObHashJoinOp::dump_build_table(int64_t row_count, bool force_update)
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
    if (max_partition_count_per_level_ != cur_dumped_partition_) {
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
      LOG_DEBUG("need dump", K(buf_mgr_->get_reserve_memory_size()),
        K(buf_mgr_->get_total_alloc_size()),
        K(profile_.get_expect_size()),
        K(sql_mem_processor_.get_mem_bound()),
        K(buf_mgr_->get_data_ratio()),
        K(max_partition_count_per_level_),
        K(cur_dumped_partition_),
        K(mem_used));
      // dump from last partition to the first partition
      int64_t cur_dumped_partition = part_count_ - 1;
      if ((all_dumped() || need_dump(mem_used)) && 0 <= cur_dumped_partition && OB_SUCC(ret)) {
        // firstly find all need dumped partition
        int64_t dumped_size = get_need_dump_size(mem_used);
        if (OB_FAIL(asyn_dump_partition(dumped_size, true, false, 0, nullptr))) {
          LOG_WARN("failed to asyn dump table", K(ret));
        }
        LOG_TRACE("trace left need dump", K(part_level_),
          K(buf_mgr_->get_reserve_memory_size()),
          K(buf_mgr_->get_total_alloc_size()),
          K(cur_dumped_partition),
          K(buf_mgr_->get_dumped_size()),
          K(buf_mgr_->get_data_ratio()),
          K(sql_mem_processor_.get_mem_bound()),
          K(cur_dumped_partition_),
          K(mem_used));
      }
    }
  }
  return ret;
}

int ObHashJoinOp::update_dumped_partition_statistics(bool is_left)
{
  int ret = OB_SUCCESS;
  int64_t total_size = 0;
  ObHashJoinPartition *part_array = nullptr;
  if (is_left) {
    part_array = hj_part_array_;
  } else {
    part_array = right_hj_part_array_;
  }
  for (int64_t i = 0; i < part_count_ && OB_SUCC(ret); i ++) {
    total_size += part_array[i].get_size_in_memory() + part_array[i].get_size_on_disk();
  }
  for (int64_t i = part_count_ - 1; i >= 0 && OB_SUCC(ret); --i) {
    ObHashJoinPartition &dumped_part = part_array[i];
    if (i > cur_dumped_partition_) {
      if (dumped_part.is_dumped()) {
        if (is_left) {
          right_hj_part_array_[i].get_batch()->set_memory_limit(1);
        }
        if (0 != dumped_part.get_size_in_memory()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expected memory size is 0", K(ret), K(i), K(dumped_part.get_size_in_memory()),
            K(part_count_), K(cur_dumped_partition_), K(dumped_part.get_row_count_in_memory()),
            K(is_left));
          if (!is_left) {
            LOG_WARN("left size", K(hj_part_array_[i].get_size_in_memory()),
              K(hj_part_array_[i].get_row_count_in_memory()),
              K(hj_part_array_[i].get_row_count_on_disk()),
              K(hj_part_array_[i].get_size_on_disk()));
          }
        } else if (0 == dumped_part.get_size_on_disk()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("disk size is 0", K(ret), K(i), K(dumped_part.get_size_on_disk()),
            K(part_count_), K(cur_dumped_partition_), K(dumped_part.get_row_count_in_memory()));
        }
      } else if (0 != dumped_part.get_size_in_memory()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expected memory size is 0", K(ret), K(i), K(dumped_part.get_size_in_memory()),
          K(part_count_), K(cur_dumped_partition_), K(dumped_part.get_row_count_in_memory()));
        if (!is_left) {
          LOG_WARN("left size", K(hj_part_array_[i].get_size_in_memory()),
            K(hj_part_array_[i].get_row_count_in_memory()),
            K(hj_part_array_[i].get_row_count_on_disk()),
            K(hj_part_array_[i].get_size_on_disk()));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(dumped_part.record_pre_batch_info(
      part_count_,
      cur_hash_table_->nbuckets_,
      total_size))) {
      LOG_WARN("failed to record pre-batch info", K(ret), K(part_count_),
        K(cur_hash_table_->nbuckets_), K(total_size));
    }
  }
  return ret;
}

int ObHashJoinOp::sync_wait_processor_type()
{
  int ret = OB_SUCCESS;
  ObHashJoinInput *hj_input = static_cast<ObHashJoinInput*>(input_);
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

int ObHashJoinOp::sync_wait_part_count()
{
  int ret = OB_SUCCESS;
  ObHashJoinInput *hj_input = static_cast<ObHashJoinInput*>(input_);
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

int ObHashJoinOp::sync_wait_cur_dumped_partition_idx()
{
  int ret = OB_SUCCESS;
  ObHashJoinInput *hj_input = static_cast<ObHashJoinInput*>(input_);
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

int ObHashJoinOp::sync_wait_basic_info(uint64_t &build_ht_thread_ptr)
{
  int ret = OB_SUCCESS;
  ObHashJoinInput *hj_input = static_cast<ObHashJoinInput*>(input_);
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

int ObHashJoinOp::sync_wait_init_build_hash(const uint64_t build_ht_thread_ptr)
{
  int ret = OB_SUCCESS;
  ObHashJoinInput *hj_input = static_cast<ObHashJoinInput*>(input_);
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
    ObHashJoinOp *build_hj_op = reinterpret_cast<ObHashJoinOp*>(build_ht_thread_ptr);
    // use the same hash table
    cur_hash_table_ = batch_round_ <= 1 ? &(build_hj_op->get_hash_table()) : cur_hash_table_;
    if (PartHashJoinTable::MAGIC_CODE != cur_hash_table_->magic_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: magic code is wrong", K(ret), K(spec_.id_));
    }
    LOG_TRACE("debug sync wait init build hash", K(cur_hash_table_), K(spec_.id_));
  }
  return ret;
}

int ObHashJoinOp::sync_wait_finish_build_hash()
{
  int ret = OB_SUCCESS;
  ObHashJoinInput *hj_input = static_cast<ObHashJoinInput*>(input_);
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
    LOG_TRACE("debug sync finish build hash", K(cur_hash_table_), K(spec_.id_));
  }
  return ret;
}

int ObHashJoinOp::sync_wait_fetch_next_batch()
{
  int ret = OB_SUCCESS;
  ObHashJoinInput *hj_input = static_cast<ObHashJoinInput*>(input_);
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
int ObHashJoinOp::sync_wait_close()
{
  int ret = OB_SUCCESS;
  ObHashJoinInput *hj_input = static_cast<ObHashJoinInput*>(input_);
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

int ObHashJoinOp::sync_wait_open()
{
  int ret = OB_SUCCESS;
  ObHashJoinInput *hj_input = static_cast<ObHashJoinInput*>(input_);
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
int ObHashJoinOp::dump_remain_partition()
{
  int ret = OB_SUCCESS;
  // dump last batch rows and only remain all in-memory data
  if (is_shared_ && OB_FAIL(sync_wait_cur_dumped_partition_idx())) {
    LOG_WARN("failed to sync cur dumped partition idx", K(ret));
  } else if (max_partition_count_per_level_ > cur_dumped_partition_) {
    if (OB_FAIL(asyn_dump_partition(INT64_MAX, true, true, cur_dumped_partition_ + 1, nullptr))) {
      LOG_WARN("failed to asyn dump partition", K(ret));
    } else if (OB_FAIL(update_dumped_partition_statistics(true))) {
      LOG_WARN("failed to update dumped partition statistics", K(ret));
    }
    LOG_TRACE("dump remain partition", K(ret), K(cur_dumped_partition_),
      "the last partition dumped size", hj_part_array_[part_count_ - 1].get_size_on_disk());
  }
  return ret;
}

int ObHashJoinOp::fill_partition(int64_t &num_left_rows)
{
  int ret = OB_SUCCESS;
  uint64_t hash_value = 0;
  ObHashJoinStoredJoinRow *stored_row = nullptr;
  bool skipped = false;
  bool is_left_side = true;
  while (OB_SUCC(ret)) {
    clear_evaluated_flag();
    if (OB_FAIL((this->*get_next_left_row_func_)())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next left row failed", K(ret));
      }
    } else {
      if (NULL == left_read_row_) {
        if (OB_FAIL(calc_hash_value(left_join_keys_, left_hash_funcs_, hash_value,
                                    is_left_side, skipped))) {
          LOG_WARN("get left row hash_value failed", K(ret));
        } else if (skipped) {
          continue;
        }
      } else {
        hash_value = left_read_row_->get_hash_value();
      }
    }
    if (OB_SUCC(ret)) {
      ++num_left_rows;
      const int64_t part_idx = get_part_idx(hash_value);
      if (OB_FAIL(hj_part_array_[part_idx].add_row(
          left_->get_spec().output_, &eval_ctx_, stored_row))) {
        LOG_WARN("failed to add row", K(ret));
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

  return ret;
}

int ObHashJoinOp::fill_partition_batch(int64_t &num_left_rows)
{
  int ret = OB_SUCCESS;
  bool is_from_row_store = (left_batch_ != NULL);
  ObHashJoinStoredJoinRow *stored_row = nullptr;
  bool is_left_side = true;
  while (OB_SUCC(ret)) {
    clear_evaluated_flag();
    const ObBatchRows *child_brs = NULL;
    // next batch
    if (OB_FAIL((this->*get_next_left_batch_func_)(is_from_row_store, child_brs))) {
      LOG_WARN("get next left row failed", K(ret), K(child_brs));
    } else if (OB_FAIL(calc_hash_value_batch(left_join_keys_, child_brs, is_from_row_store,
                                            hash_vals_, hj_part_stored_rows_,
                                            is_left_side))) {
      LOG_WARN("fail to calc hash value batch", K(ret));
    } else if (child_brs->size_ > 16 * part_count_) {
      // add partition by batch
      if (OB_FAIL(calc_part_idx_batch(hash_vals_, *child_brs))) {
        LOG_WARN("Fail to calc batch idx", K(ret));
      }
      for (int64_t part_idx = 0; OB_SUCC(ret) && part_idx < part_count_; part_idx++) {
        if (part_selector_sizes_[part_idx] <= 0) { continue; }
        num_left_rows += part_selector_sizes_[part_idx];
        if (OB_FAIL(hj_part_array_[part_idx].add_batch(
                                     left_->get_spec().output_,
                                     eval_ctx_,
                                     *child_brs->skip_,
                                     child_brs->size_,
                                     part_selectors_ + part_idx * MY_SPEC.max_batch_size_,
                                     part_selector_sizes_[part_idx],
                                     hj_part_added_rows_))) {
          LOG_WARN("fail to add rows", K(ret), K(part_idx));
        } else {
          for (int64_t i = 0; i < part_selector_sizes_[part_idx]; i++) {
            hj_part_added_rows_[i]->set_is_match(false);
            hj_part_added_rows_[i]->set_hash_value(
                     hash_vals_[part_selectors_[part_idx * MY_SPEC.max_batch_size_ + i]]);
          }
          if (GCONF.is_sql_operator_dump_enabled()) {
            if (OB_FAIL(dump_build_table(num_left_rows))) {
              LOG_WARN("fail to dump", K(ret));
            }
          }
        }
      } // for end
    } else {
      // add row to partition
      ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
      batch_info_guard.set_batch_size(child_brs->size_);
      for (int64_t i = 0; OB_SUCC(ret) && i < child_brs->size_; i++) {
        if (child_brs->skip_->exist(i)) {
          continue;
        }
        ++num_left_rows;
        const int64_t part_idx = get_part_idx(hash_vals_[i]);
        batch_info_guard.set_batch_idx(i);
        if (OB_FAIL(hj_part_array_[part_idx].add_row(
            left_->get_spec().output_, &eval_ctx_, stored_row))) {
          LOG_WARN("failed to add row", K(ret));
        }
        if (OB_SUCC(ret)) {
          stored_row->set_is_match(false);
          stored_row->set_hash_value(hash_vals_[i]);
          if (GCONF.is_sql_operator_dump_enabled()) {
            if (OB_FAIL(dump_build_table(num_left_rows))) {
              LOG_WARN("fail to dump", K(ret));
            }
          }
        }
      } // for end
    }
    if (OB_SUCC(ret) && child_brs->end_) {
      ret = OB_ITER_END;
    }
  }

  return ret;
}

int ObHashJoinOp::calc_part_idx_batch(uint64_t *hash_vals, const ObBatchRows &child_brs)
{
  int ret = OB_SUCCESS;
  MEMSET(part_selector_sizes_, 0, sizeof(uint16_t) * part_count_);
  for (int64_t i = 0; i < child_brs.size_; i++) {
    if (child_brs.skip_->exist(i)) {
      continue;
    }
    const int64_t part_idx = get_part_idx(hash_vals[i]);
    part_selectors_[part_idx * MY_SPEC.max_batch_size_ + part_selector_sizes_[part_idx]] = i;
    part_selector_sizes_[part_idx]++;
  }

  return ret;
}

int ObHashJoinOp::split_partition(int64_t &num_left_rows)
{
  int ret = OB_SUCCESS;
  int64_t row_count_on_disk = 0;
  num_left_rows = 0;
  if (nullptr != left_batch_) {
    // read all data， use default iterator
    if (OB_FAIL(left_batch_->set_iterator())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to init iterator", K(ret));
      }
    } else {
      row_count_on_disk = left_batch_->get_row_count_on_disk();
    }
  }
  if (OB_SUCC(ret)) {
    if (is_vectorized()) {
      ret = fill_partition_batch(num_left_rows);
    } else {
      ret = fill_partition(num_left_rows);
    }
  }
  // overwrite OB_ITER_END error code
  if (OB_ITER_END == ret && (!read_null_in_naaj_ || is_shared_)) {
    ret = OB_SUCCESS;
    if (nullptr != left_batch_) {
      left_batch_->rescan();
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
          int64_t row_count_in_memory = hj_part_array_[i].get_row_count_in_memory();
          if (0 != row_count_in_memory) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpect no data in memory", K(ret), K(row_count_in_memory), K(i));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (nullptr != left_batch_ && num_left_rows != row_count_on_disk) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect read all data", K(ret), K(num_left_rows), K(row_count_on_disk));
    }
  }
  LOG_TRACE("trace split partition", K(ret), K(num_left_rows), K(row_count_on_disk),
    K(cur_dumped_partition_));
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

int ObHashJoinOp::init_bloom_filter(ObIAllocator &alloc, int64_t bucket_cnt)
{
  int ret = OB_SUCCESS;
  if (!enable_bloom_filter_) {
  } else if (nullptr == bloom_filter_) {
    void *alloc_buf = alloc.alloc(sizeof(ModulePageAllocator));
    void *mem = alloc.alloc(sizeof(ObGbyBloomFilter));
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
      bloom_filter_ = new (mem) ObGbyBloomFilter (*bloom_filter_alloc_);
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
  int64_t cur_partition_in_memory = max_partition_count_per_level_ == cur_dumped_partition_ ?
                                    part_count_ : cur_dumped_partition_ + 1;
  for (int64_t i = 0; i < cur_partition_in_memory; ++i) {
    ObHashJoinPartition &hj_part = hj_part_array_[i];
    // maybe empty partition
    total_row_count += hj_part.get_row_count_in_memory();
  }
  int64_t total_partition_cnt = next_pow2(total_row_count / row_count_cache_aware);
  total_partition_cnt = total_partition_cnt < MIN_PART_COUNT ?
                        MIN_PART_COUNT :
                        total_partition_cnt;
  int64_t tmp_partition_cnt_per_level = max_partition_count_per_level_;
  if (total_partition_cnt > tmp_partition_cnt_per_level) {
    // 这里第一层一定使用part_count保证分区至多一次
    level1_part_count_ = part_count_;
    OB_ASSERT(0 != level1_part_count_);
    level1_bit_ = __builtin_ctz(level1_part_count_);
    level2_part_count_ = total_partition_cnt / level1_part_count_;
    level2_part_count_ = level2_part_count_ > tmp_partition_cnt_per_level ?
                          tmp_partition_cnt_per_level :
                          level2_part_count_;
  } else {
    level1_part_count_ = total_partition_cnt > part_count_ ? total_partition_cnt : part_count_;
    OB_ASSERT(0 != level1_part_count_);
    level1_bit_ = __builtin_ctz(level1_part_count_);
  }
  LOG_TRACE("partition count", K(total_partition_cnt), K(tmp_partition_cnt_per_level),
    K(part_count_), K(level1_part_count_), K(level1_bit_), K(level2_part_count_),
    K(total_row_count), K(row_count_cache_aware));
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
    ObHashJoinPartition &hj_part = hj_part_array_[i];
    // maybe empty partition
    total_memory_size += sizeof(HashJoinHistogram)
                        + HashJoinHistogram::calc_memory_size(hj_part.get_row_count_in_memory());
    if (!dumped) {
      dumped = hj_part.get_row_count_on_disk() > 0;
    }
    if (!dumped) {
      total_row_count += hj_part.get_row_count_in_memory();
    }
  }
  // cache aware use to switch buffer for chunk datum streo, so need 2 buffer
  total_memory_size += get_cur_mem_used() + 2 * part_count_ * PAGE_SIZE;
  enable_cache_aware = total_memory_size < sql_mem_processor_.get_mem_bound();
  if (!enable_cache_aware) {
    bool need_dump = true;
    int64_t pre_cache_size = profile_.get_cache_size();
    if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(
        alloc_,
        [&](int64_t max_memory_size) {
          return total_memory_size > max_memory_size;
        },
        need_dump, get_cur_mem_used()))) {
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
  int64_t total_partition_cnt = 0 < level2_part_count_
                              ? level1_part_count_ * level2_part_count_
                              : level1_part_count_;
  bool force_enable = false;
  if (OB_FAIL(ret)) {
  } else {
    uint64_t opt = std::abs(EVENT_CALL(EventTable::EN_HASH_JOIN_OPTION));
    if (0 != opt) {
      enable_cache_aware = false;
      force_enable = !!(opt & HJ_TP_OPT_ENABLE_CACHE_AWARE);
    }
  }
  enable_cache_aware = ((enable_cache_aware
                    && total_partition_cnt >= CACHE_AWARE_PART_CNT) || force_enable)
                    && INNER_JOIN == MY_SPEC.join_type_
                    && !is_shared_;
  LOG_TRACE("trace check cache aware opt", K(total_memory_size), K(total_row_count),
    K(row_count_cache_aware), K(enable_cache_aware),
    K(sql_mem_processor_.get_mem_bound()), K(part_count_), K(cur_dumped_partition_),
    K(level1_part_count_), K(level2_part_count_));
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
  uint64_t build_ht_thread_ptr = 0;
  PartHashJoinTable &hash_table = hash_table_;
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
  } else {
    hash_table.nbuckets_ = profile_.get_bucket_size();
    hash_table.row_count_ = profile_.get_row_count();
  }
  bool need_build_hash_table = true;
  if (is_shared_) {
    ObHashJoinInput *hj_input = static_cast<ObHashJoinInput*>(input_);
    need_build_hash_table = batch_round_ <= 1
                          ? (build_ht_thread_ptr == reinterpret_cast<uint64_t>(this))
                          : cur_hash_table_ == &hash_table_;
    LOG_DEBUG("debug build hash table",
      K(need_build_hash_table),
      K(build_ht_thread_ptr),
      K(reinterpret_cast<uint64_t>(this)));
  }
  if (need_build_hash_table) {
    int64_t buckets_mem_size = 0;
    int64_t collision_cnts_mem_size = 0;
    hash_table.buckets_->reuse();
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_bloom_filter(mem_context_->get_malloc_allocator(), hash_table_.nbuckets_))) {
      LOG_WARN("failed to create bloom filter", K(ret));
    } else if (OB_FAIL(hash_table.buckets_->init(hash_table.nbuckets_))) {
      LOG_WARN("alloc bucket array failed", K(ret), K(hash_table.nbuckets_));
    } else {
      hash_table.collisions_ = 0;
      hash_table.used_buckets_ = 0;
      if (NEST_LOOP == hj_processor_
        && OB_NOT_NULL(right_batch_)) {
        has_right_bitset_ = need_right_bitset();
        if (has_right_bitset_
          && OB_FAIL(right_bit_set_.reserve(right_batch_->get_row_count_on_disk()))) {
          // revert to recursive process
          // only for TEST_NEST_LOOP_TO_RECURSIVE
          nest_loop_state_ = HJLoopState::LOOP_RECURSIVE;
          LOG_WARN("failed to reserve right bitset", K(ret), K(hash_table.nbuckets_),
            K(right_batch_->get_row_count_on_disk()));
        }
      }
      LOG_TRACE("trace prepare hash table", K(ret), K(hash_table.nbuckets_), K(hash_table.row_count_),
        K(part_count_), K(buf_mgr_->get_reserve_memory_size()),
        K(total_extra_size_), K(buf_mgr_->get_total_alloc_size()),
        K(profile_.get_expect_size()));
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("trace failed to  prepare hash table", K(buf_mgr_->get_total_alloc_size()),
        K(profile_.get_expect_size()), K(buf_mgr_->get_reserve_memory_size()),
        K(hash_table.nbuckets_), K(hash_table.row_count_), K(get_mem_used()),
        K(sql_mem_processor_.get_mem_bound()), K(buf_mgr_->get_data_ratio()),
        K(cur_dumped_partition_), K(enable_bloom_filter_));
    } else {
      if (OB_FAIL(sql_mem_processor_.update_used_mem_size(get_mem_used()))) {
        LOG_WARN("failed to update used mem size", K(ret));
      }
    }
    LOG_TRACE("trace prepare hash table", K(ret), K(hash_table.nbuckets_), K(hash_table.row_count_),
      K(buckets_mem_size), K(part_count_), K(buf_mgr_->get_reserve_memory_size()),
      K(total_extra_size_), K(buf_mgr_->get_total_alloc_size()),
      K(profile_.get_expect_size()), K(collision_cnts_mem_size));
  }
  if (OB_SUCC(ret) && is_shared_ && OB_FAIL(sync_wait_init_build_hash(build_ht_thread_ptr))) {
    LOG_WARN("failed to sync wait init hash table", K(ret));
  }
  return ret;
}

void ObHashJoinOp::trace_hash_table_collision(int64_t row_cnt)
{
  int64_t total_cnt = cur_hash_table_->collisions_;
  int64_t used_bucket_cnt = cur_hash_table_->used_buckets_;
  int64_t nbuckets = cur_hash_table_->nbuckets_;
  LOG_TRACE("trace hash table collision", K(spec_.get_id()), K(spec_.get_name()), K(nbuckets),
    "avg_cnt", ((double)total_cnt/(double)used_bucket_cnt), K(total_cnt),
    K(row_cnt), K(used_bucket_cnt));
  // 记录到虚拟表供查询
  op_monitor_info_.otherstat_1_value_ = 0;
  op_monitor_info_.otherstat_2_value_ = 0;
  op_monitor_info_.otherstat_3_value_ = total_cnt;
  op_monitor_info_.otherstat_4_value_ = nbuckets;
  op_monitor_info_.otherstat_5_value_ = used_bucket_cnt;
  op_monitor_info_.otherstat_6_value_ = row_cnt;
  op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::HASH_SLOT_MIN_COUNT;;
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
  const ObHashJoinStoredJoinRow *stored_row = nullptr;
  int64_t total_row_count = 0;
  PartHashJoinTable &hash_table = *cur_hash_table_;
  const int64_t PREFETCH_BATCH_SIZE = 64;
  const ObHashJoinStoredJoinRow *part_stored_rows[PREFETCH_BATCH_SIZE];
  int64_t start_id = is_shared_ ? (static_cast<ObHashJoinInput*>(input_))->get_task_id() : 0;
  int64_t step = 64;
  int64_t used_buckets = 0;
  int64_t collisions = 0;
  for (int64_t i = start_id, idx = 0; OB_SUCC(ret) && idx < part_count_; ++idx, ++i) {
    i = i % part_count_;
    ObHashJoinPartition &hj_part = hj_part_array_[i];
    int64_t row_count_in_memory = hj_part.get_row_count_in_memory();
    int64_t nth_row = 0;
    if (0 < row_count_in_memory) {
      if (0 < hj_part.get_row_count_on_disk()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect it has row on disk", K(ret), K(row_count_in_memory), K(hj_part.get_row_count_on_disk()));
      } else if (OB_FAIL(hj_part.init_iterator())) {
        // 这里假设partition一定是要么全部dump，要么全部在内存里，所以直接用row iter即可
        // 没有必要用chunk row store，如果部分在内存，部分在disk，可以使用chunk先load 内存的数据
        LOG_WARN("failed to init iterator", K(ret));
      } else {
        while (OB_SUCC(ret)) {
          int64_t read_size = 0;
          if (OB_FAIL(hj_part.get_next_batch(part_stored_rows, PREFETCH_BATCH_SIZE, read_size))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("get next batch failed", K(ret));
            }
          } else if (OB_ISNULL(part_stored_rows)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("returned hj_part_stored_rows_ is NULL", K(ret));
          } else if (total_row_count + nth_row >= hash_table.row_count_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row count exceed total row count", K(ret), K(nth_row), K(total_row_count),
              K(hash_table.row_count_));
          } else if (read_null_in_naaj_) {
            // don't insert any value
            nth_row += read_size;
          } else {
            if (enable_bloom_filter_) {
              for (int64_t i = 0; OB_SUCC(ret) && i < read_size; ++i) {
                if (OB_FAIL(bloom_filter_->set(part_stored_rows[i]->get_hash_value()))) {
                  LOG_WARN("add hash value to bloom failed", K(ret), K(i));
                }
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_UNLIKELY(NULL == hash_table.buckets_)) {
                // do nothing
              } else {
                auto mask = hash_table.nbuckets_ - 1;
                for(auto i = 0; i < read_size; i++) {
                  __builtin_prefetch((&hash_table.buckets_->at(part_stored_rows[i]->get_hash_value() & mask)), 1 /* w */, 3 /* high */);
                }
                if (is_shared_) {
                  for (int64_t i = 0; OB_SUCC(ret) && i < read_size; ++i) {
                    hash_table.atomic_set(part_stored_rows[i]->get_hash_value(),
                      const_cast<ObHashJoinStoredJoinRow *>(part_stored_rows[i]),
                      used_buckets,
                      collisions);
                  }
                } else {
                  for (int64_t i = 0; OB_SUCC(ret) && i < read_size; ++i) {
                    hash_table.set(part_stored_rows[i]->get_hash_value(), const_cast<ObHashJoinStoredJoinRow *>(part_stored_rows[i]));
                  }
                }
              }
            }
            if (OB_SUCC(ret)) {
              nth_row += read_size;
            }
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      total_row_count += nth_row;
      ret = OB_SUCCESS;
      if (nth_row != row_count_in_memory) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect row count is match", K(nth_row), K(row_count_in_memory),
          K(hj_part.get_row_count_on_disk()));
      }
    }
  }
  // 为了和nest loop方式统一，该flag表示recursive模式下，如果没有dump，则一定是in-memory
  // 在in-memory情况下需要根据join type(right (anti,outer等) join)是否需要返回数据
  // nest loop情况下只有最后一个chunk才需要，而recursive的in-memory数据一定需要，所以这里设为true
  is_last_chunk_ = true;
  if (OB_SUCC(ret)) {
    if (is_shared_ ) {
      ATOMIC_AAF(&hash_table.used_buckets_, used_buckets);
      ATOMIC_AAF(&hash_table.collisions_, collisions);
      if (OB_FAIL(sync_wait_finish_build_hash())) {
        LOG_WARN("failed to wait finish build hash", K(ret));
      }
    }
    trace_hash_table_collision(total_row_count);
  }
  LOG_TRACE("trace to finish build hash table for recursive", K(part_count_), K(part_level_),
    K(total_row_count), K(hash_table_.nbuckets_), K(spec_.id_), K(start_id));
  return ret;
}

int ObHashJoinOp::HashJoinHistogram::init(
  ObIAllocator *alloc, int64_t row_count, int64_t bucket_cnt, bool enable_bloom_filter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hist_alloc_) && 0 < row_count) {
    void *hist_alloc = alloc->alloc(sizeof(ModulePageAllocator));
    void *h1 = alloc->alloc(sizeof(HistItemArray));
    void *h2 = alloc->alloc(sizeof(HistItemArray));
    void *prefix_hist_count = alloc->alloc(sizeof(HistPrefixArray));
    void *prefix_hist_count2 = alloc->alloc(sizeof(HistPrefixArray));
    void *bf = alloc->alloc(sizeof(ObGbyBloomFilter));
    alloc_ = alloc;
    if (OB_ISNULL(hist_alloc) || OB_ISNULL(h1)
        || OB_ISNULL(h2) || OB_ISNULL(prefix_hist_count)
        || OB_ISNULL(prefix_hist_count2) || OB_ISNULL(bf)) {
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
      bloom_filter_ = new (bf) ObGbyBloomFilter (*hist_alloc_);
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
    for (int64_t i = 0; i < h1_->count() ; ++i) {
      HistItem &hist_item = h1_->at(i);
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
      for (int64_t i = 0; OB_SUCC(ret) && i < h2_->count(); ++i) {
        HistItem &hist_item = h2_->at(i);
        if (OB_FAIL(bloom_filter_->set(hist_item.hash_value_))) {
          LOG_WARN("add hash value to bloom failed", K(ret));
        }
      }
    }
    LOG_DEBUG("trace reorder histogram", K(row_count_), K(bucket_cnt_), K(h1_->count()),
      K(prefix_hist_count_->count()));
  }
  return ret;
}

void ObHashJoinOp::HashJoinHistogram::switch_histogram()
{
  HistItemArray *tmp_hist = h1_;
  h1_ = h2_;
  h2_ = tmp_hist;
}

void ObHashJoinOp::HashJoinHistogram::switch_prefix_hist_count()
{
  HistPrefixArray *tmp_prefix_hist_count = prefix_hist_count_;
  prefix_hist_count_ = prefix_hist_count2_;
  prefix_hist_count2_ = tmp_prefix_hist_count;
}

int ObHashJoinOp::PartitionSplitter::init(
  ObIAllocator *alloc,
  int64_t part_count,
  ObHashJoinPartition *hj_parts,
  int64_t max_level,
  int64_t part_shift,
  int64_t level1_part_count,
  int64_t level2_part_count)
{
  int ret = OB_SUCCESS;
  alloc_ = alloc;
  part_count_ = part_count;
  hj_parts_ = hj_parts;
  max_level_ = max_level;
  int64_t max_partition_cnt = 0 < level2_part_count ?
                              level2_part_count * level1_part_count :
                              level1_part_count;
  set_part_count(part_shift, level1_part_count, level2_part_count);
  int64_t total_row_count = 0;
  for (int64_t i = 0; i < part_count; ++i) {
    ObHashJoinPartition &hj_part = hj_parts[i];
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
  HashJoinHistogram::HistItemArray *dst_hist_array = part_histogram_.h1_;
  HashJoinHistogram::HistPrefixArray *dst_prefix_hist_counts = part_histogram_.prefix_hist_count_;
  int64_t partition_count = 1 == part_level ?
                            level_one_part_count_ :
                            level_one_part_count_ * level_two_part_count_;
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
    ObHashJoinPartition &hj_part = hj_parts_[i];
    int64_t row_count_in_memory = hj_part.get_row_count_in_memory();
    const ObHashJoinStoredJoinRow *stored_row = nullptr;
    int64_t nth_row = 0;
    if (0 < row_count_in_memory) {
      if (0 < hj_part.get_row_count_on_disk()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect it has row on disk", K(ret));
      } else if (OB_FAIL(hj_part.init_iterator())) {
        // 这里假设partition一定是要么全部dump，要么全部在内存里，所以直接用row iter即可
        // 没有必要用chunk row store，如果部分在内存，部分在disk，可以使用chunk先load 内存的数据
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
            HistItem &hist_item = dst_hist_array->at(total_nth_row);
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
          LOG_WARN("expect row count is match", K(nth_row), K(row_count_in_memory),
            K(hj_part.get_row_count_on_disk()), K(ret));
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
  HashJoinHistogram::HistPrefixArray *dst_prefix_hist_counts = part_histogram_.prefix_hist_count_;
  HashJoinHistogram::HistItemArray *org_hist_array = part_histogram_.h2_;
  HashJoinHistogram::HistPrefixArray *org_prefix_hist_counts = part_histogram_.prefix_hist_count2_;
  int64_t partition_count = 1 == part_level ?
                            level_one_part_count_ :
                            level_one_part_count_ * level_two_part_count_;
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
      HistItem &hist_item = org_hist_array->at(j);
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
  HashJoinHistogram *all_part_hists, bool enable_bloom_filter)
{
  int ret = OB_SUCCESS;
  HashJoinHistogram::HistItemArray *org_hist_array = part_histogram_.h2_;
  HashJoinHistogram::HistPrefixArray *org_prefix_hist_counts = part_histogram_.prefix_hist_count2_;
  int64_t total_nth_row = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < org_prefix_hist_counts->count(); ++i) {
    HashJoinHistogram *hist = new (&all_part_hists[i]) HashJoinHistogram();
    int64_t nth_row = 0;
    int64_t start_idx = 0;
    int64_t end_idx = org_prefix_hist_counts->at(i);
    if (0 != i) {
      start_idx = org_prefix_hist_counts->at(i - 1);
    }
    LOG_DEBUG("debug build hash talbe by part histogram", K(i), K(org_hist_array->count()),
      K(start_idx), K(end_idx), K(org_prefix_hist_counts->count()));
    if (end_idx > start_idx) {
      int64_t row_count = end_idx - start_idx;
      if (OB_FAIL(hist->init(alloc_, row_count, next_pow2(row_count * RATIO_OF_BUCKETS), enable_bloom_filter))) {
        LOG_WARN("failed to init histogram", K(ret));
      } else {
        for (int64_t j = start_idx; j < end_idx && OB_SUCC(ret); ++j) {
          HistItem &org_hist_item = org_hist_array->at(j);
          HistItem &hist_item = hist->h1_->at(nth_row);
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
    LOG_WARN("row count is not match", K(total_nth_row),
      K(org_prefix_hist_counts->at(part_histogram_.prefix_hist_count2_->count() - 1)));
  }
  return ret;
}

int ObHashJoinOp::PartitionSplitter::build_hash_table_by_part_array(
  HashJoinHistogram *all_part_hists, bool enable_bloom_filter)
{
  int ret = OB_SUCCESS;
  // step1: traverse all partition data to generate initial histogram
  for (int64_t i = 0; OB_SUCC(ret) && i < part_count_; ++i) {
    ObHashJoinPartition &hj_part = hj_parts_[i];
    int64_t row_count_in_memory = hj_part.get_row_count_in_memory();
    int64_t nth_row = 0;
    const ObHashJoinStoredJoinRow *stored_row = nullptr;
    HashJoinHistogram *hist = new (&all_part_hists[i]) HashJoinHistogram();
    if (0 < row_count_in_memory) {
      if (OB_FAIL(hist->init(
          alloc_, row_count_in_memory, next_pow2(row_count_in_memory * RATIO_OF_BUCKETS), enable_bloom_filter))) {
        LOG_WARN("failed to init histogram", K(ret));
      } else if (0 < hj_part.get_row_count_on_disk()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect it has row on disk", K(ret));
      } else if (OB_FAIL(hj_part.init_iterator())) {
        // 这里假设partition一定是要么全部dump，要么全部在内存里，所以直接用row iter即可
        // 没有必要用chunk row store，如果部分在内存，部分在disk，可以使用chunk先load 内存的数据
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
            HistItem &hist_item = hist->h1_->at(nth_row);
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
          LOG_WARN("expect row count is match", K(nth_row), K(row_count_in_memory),
            K(hj_part.get_row_count_on_disk()));
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

int ObHashJoinOp::init_histograms(HashJoinHistogram *&part_histograms, int64_t part_count)
{
  int ret = OB_SUCCESS;
  void *all_part_hists = alloc_->alloc(sizeof(HashJoinHistogram) * part_count);
  if (OB_ISNULL(all_part_hists)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else {
    part_histograms = reinterpret_cast<HashJoinHistogram*>(all_part_hists);
    MEMSET(part_histograms, 0, sizeof(HashJoinHistogram) * part_count);
  }
  return ret;
}

int ObHashJoinOp::repartition(
  PartitionSplitter &part_splitter,
  HashJoinHistogram *&part_histograms,
  ObHashJoinPartition *hj_parts,
  bool is_build_side)
{
  int ret = OB_SUCCESS;
  int64_t part_count = 0 < level2_part_count_ ?
                      level2_part_count_ * level1_part_count_ :
                      level1_part_count_;
  int64_t cur_partition_in_memory = max_partition_count_per_level_ == cur_dumped_partition_ ?
                                    part_count_ : cur_dumped_partition_ + 1;
  part_histograms = nullptr;
  if (OB_FAIL(part_splitter.init(alloc_, cur_partition_in_memory, hj_parts,
      0 < level2_part_count_ ? PART_SPLIT_LEVEL_TWO : PART_SPLIT_LEVEL_ONE,
      part_shift_, level1_part_count_, level2_part_count_))) {
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
      } else if (OB_FAIL(part_splitter.build_hash_table_by_part_hist(
          part_histograms, enable_bloom_filter_))) {
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
      } else if (OB_FAIL(part_splitter.build_hash_table_by_part_hist(
          part_histograms, enable_bloom_filter_))) {
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
      } else if (OB_FAIL(part_splitter.build_hash_table_by_part_array(
          part_histograms, enable_bloom_filter_))) {
        LOG_WARN("failed to build hash table by part histogram", K(ret));
      } else {
        // one bloom filter per partition
        LOG_TRACE("trace level2 repartition", K(level1_part_count_), K(level2_part_count_));
      }
    } else {
      if (OB_FAIL(part_splitter.repartition_by_part_array(PART_SPLIT_LEVEL_ONE))) {
        LOG_WARN("failed to repartition by part array", K(ret));
      } else if (!is_build_side) {
      } else if (OB_FAIL(part_splitter.build_hash_table_by_part_hist(
          part_histograms, enable_bloom_filter_))) {
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
  enable_batch_ = false; // enable_batch is not supported now
  ret = OB_SUCCESS;
  if (enable_batch_) {
    PartitionSplitter part_splitter;
    if (OB_FAIL(repartition(part_splitter, part_histograms_, hj_part_array_, true))) {
      LOG_WARN("failed to repartition", K(ret));
    }
  } else {
    // if cache aware and disable batch, then cache all right rows, so we need reset sql_mem_processor
    sql_mem_processor_.set_periodic_cnt(1024);
  }
  LOG_TRACE("debug partition", K(level1_part_count_), K(level1_bit_), K(level2_part_count_));
  return ret;
}

void ObHashJoinOp::init_system_parameters()
{
  ltb_size_ = INIT_LTB_SIZE;
  l2_cache_size_ = INIT_L2_CACHE_SIZE;
  max_partition_count_per_level_ = ltb_size_ << 1;
  enable_bloom_filter_ = !is_vectorized();
  uint64_t opt = std::abs(EVENT_CALL(EventTable::EN_HASH_JOIN_OPTION));
  if (0 != opt) {
    enable_bloom_filter_ = !!(opt & HJ_TP_OPT_ENABLE_BLOOM_FILTER);
  }
  if (is_shared_) {
    enable_bloom_filter_ = false;
  }
}

int ObHashJoinOp::recursive_postprocess()
{
  int ret = OB_SUCCESS;
  if (opt_cache_aware_) {
    LOG_TRACE("trace use cache aware optimization");
    // need to finish dump
    if (HJProcessor::RECURSIVE == hj_processor_) {
      if (OB_FAIL(dump_remain_partition())) {
        LOG_WARN("failed to dump remain partition");
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(partition_and_build_histograms())) {
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

int ObHashJoinOp::split_partition_and_build_hash_table(int64_t &num_left_rows)
{
  int ret = OB_SUCCESS;
  // load data to partitions
  num_left_rows = 0;
  if (OB_FAIL(split_partition(num_left_rows))) {
    if (OB_ITER_END == ret && read_null_in_naaj_) {
      ret = OB_SUCCESS;
      if (is_vectorized()) {
        brs_.size_ = 0;
        brs_.end_ = true;
      }
      LOG_TRACE("null break for right naaj");
    } else {
      LOG_WARN("failed split partition", K(ret), K(part_level_));
    }
  } else {
    if (is_vectorized()) {
      opt_cache_aware_ = false;
    } else {
      can_use_cache_aware_opt();
    }
    if ((0 == num_left_rows || is_shared_) && OB_FAIL(recursive_postprocess())) {
      LOG_WARN("failed to post process left", K(ret));
    }
  }
  return ret;
}

int ObHashJoinOp::recursive_process(bool &need_not_read_right)
{
  int ret = OB_SUCCESS;
  need_not_read_right = false;
  int64_t num_left_rows = 0;
  if (OB_FAIL(init_join_partition())) {
    LOG_WARN("fail to init join ctx", K(ret));
  } else if (OB_FAIL(split_partition_and_build_hash_table(num_left_rows))) {
    LOG_WARN("failed to build hash table", K(ret), K(part_level_));
  }
  if (OB_SUCC(ret)
      && !is_shared_
      && ((0 == num_left_rows
          && RIGHT_ANTI_JOIN != MY_SPEC.join_type_
          && RIGHT_OUTER_JOIN != MY_SPEC.join_type_
          && FULL_OUTER_JOIN != MY_SPEC.join_type_) || read_null_in_naaj_)) {
    need_not_read_right = true;
    LOG_DEBUG("[HASH JOIN]Left table is empty, skip reading right table.",
      K(num_left_rows), K(MY_SPEC.join_type_));
  }
  return ret;
}

int ObHashJoinOp::adaptive_process(bool &need_not_read_right)
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
          LOG_WARN("failed to recursive process", K(ret),
            K(part_level_), K(part_count_), K(hash_table_.nbuckets_));
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
              LOG_WARN("failed to process in memory", K(ret),
                K(part_level_), K(part_count_), K(hash_table_.nbuckets_));
            }
            LOG_TRACE("trace recursive process", K(part_level_),
              K(part_level_), K(part_count_), K(hash_table_.nbuckets_));
          } else {
            LOG_WARN("failed to process in memory", K(ret),
              K(part_level_), K(part_count_), K(hash_table_.nbuckets_));
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
  LOG_TRACE("trace process type", K(part_level_), K(part_count_),
    K(hash_table_.nbuckets_), K(remain_data_memory_size_));
  return ret;
}

int ObHashJoinOp::get_next_right_batch_na()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  right_read_from_stored_ =  right_batch_ != NULL;
  LOG_TRACE("hash join last traverse cnt", K(ret), K(right_batch_traverse_cnt_));
  right_batch_traverse_cnt_ = 0;
  bool is_left = false;
  bool has_null = false;
  if (!right_read_from_stored_) {
    if (HashJoinDrainMode::BUILD_HT == drain_mode_ || (read_null_in_naaj_ && is_shared_)) {
      if (OB_FAIL(right_->drain_exch())) {
        LOG_WARN("failed to drain the right child", K(ret), K(spec_.id_));
      } else {
        drain_mode_ = HashJoinDrainMode::RIGHT_DRAIN;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (HashJoinDrainMode::RIGHT_DRAIN == drain_mode_ ||
        (read_null_in_naaj_ && is_shared_)) {
      FOREACH_CNT_X(e, right_->get_spec().output_, OB_SUCC(ret)) {
        (*e)->get_eval_info(eval_ctx_).projected_ = true;
      }
      right_brs_ = &child_brs_;
      const_cast<ObBatchRows *>(right_brs_)->size_ = 0;
      const_cast<ObBatchRows *>(right_brs_)->end_ = true;
    } else if (OB_FAIL(OB_I(t1) right_->get_next_batch(max_output_cnt_, right_brs_))) {
      LOG_WARN("get right row from child failed", K(ret));
    } else if (right_brs_->end_ && 0 == right_brs_->size_) {
      // When reach here, projected flag has been set to false.
      // In the hash join operator, the datum corresponding to
      // the child output expr may be covered from the datum store;
      // for opt, we not set evaluted flag when convert exprs,
      // and depended projected flag is true
      FOREACH_CNT_X(e, right_->get_spec().output_, OB_SUCC(ret)) {
        (*e)->get_eval_info(eval_ctx_).projected_ = true;
      }
    } else if (FALSE_IT(non_preserved_side_is_not_empty_
                        |= (LEFT_ANTI_JOIN == MY_SPEC.join_type_))) {
      // mark this to forbid null value output in fill result batch
    } else if (is_left_naaj() && OB_FAIL(check_join_key_for_naaj_batch(is_left, right_brs_->size_,
                                                                        has_null, right_brs_))) {
      LOG_WARN("failed to check null value for naaj", K(ret));
    } else if (has_null) {
      brs_.size_ = 0;
      brs_.end_ = true;
      read_null_in_naaj_ = true;
      state_ = JS_JOIN_END;
      LOG_TRACE("null break for left naaj", K(ret));
    }
  } else {
    int64_t read_size = 0;
    right_brs_ = &child_brs_;
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (HashJoinDrainMode::BUILD_HT == drain_mode_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: drain mode is build_hash_table", K(ret), K(spec_.id_));
    } else if (HashJoinDrainMode::RIGHT_DRAIN == drain_mode_ ||
        (read_null_in_naaj_ && is_shared_)) {
      const_cast<ObBatchRows *>(right_brs_)->size_ = 0;
      const_cast<ObBatchRows *>(right_brs_)->end_ = true;
      ret = OB_SUCCESS;
    } else if (OB_FAIL(OB_I(t1) right_batch_->get_next_batch(right_hj_part_stored_rows_,
                                                             max_output_cnt_,
                                                             read_size))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get right row from partition failed", K(ret));
      } else {
        const_cast<ObBatchRows *>(right_brs_)->size_ = 0;
        const_cast<ObBatchRows *>(right_brs_)->end_ = true;
        ret = OB_SUCCESS;
      }
    } else {
      nth_right_row_ += read_size;
      const_cast<ObBatchRows *>(right_brs_)->size_ = read_size;
      const_cast<ObBatchRows *>(right_brs_)->end_ = false;
      const_cast<ObBatchRows *>(right_brs_)->skip_->reset(read_size);
    }
  }
  if (!postprocessed_left_) {
    int tmp_ret = OB_SUCCESS;
    //load data and build hash table
    if (OB_SUCCESS != (tmp_ret = recursive_postprocess())) {
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
      LOG_WARN("failed to post process left", K(ret), K(tmp_ret));
    }
  }
  return ret;
}

int ObHashJoinOp::get_next_right_batch()
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  right_read_from_stored_ =  right_batch_ != NULL;
  LOG_TRACE("hash join last traverse cnt", K(ret), K(right_batch_traverse_cnt_));
  right_batch_traverse_cnt_ = 0;
  if (!right_read_from_stored_) {
    if (HashJoinDrainMode::BUILD_HT == drain_mode_ || (read_null_in_naaj_ && is_shared_)) {
      if (OB_FAIL(right_->drain_exch())) {
        LOG_WARN("failed to drain the right child", K(ret), K(spec_.id_));
      } else {
        drain_mode_ = HashJoinDrainMode::RIGHT_DRAIN;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (HashJoinDrainMode::RIGHT_DRAIN == drain_mode_) {
      FOREACH_CNT_X(e, right_->get_spec().output_, OB_SUCC(ret)) {
        (*e)->get_eval_info(eval_ctx_).projected_ = true;
      }
      right_brs_ = &child_brs_;
      const_cast<ObBatchRows *>(right_brs_)->size_ = 0;
      const_cast<ObBatchRows *>(right_brs_)->end_ = true;
    } else if (OB_FAIL(OB_I(t1) right_->get_next_batch(max_output_cnt_, right_brs_))) {
      LOG_WARN("get right row from child failed", K(ret));
    } else if (right_brs_->end_ && 0 == right_brs_->size_) {
      // When reach here, projected flag has been set to false.
      // In the hash join operator, the datum corresponding to
      // the child output expr may be covered from the datum store;
      // for opt, we not set evaluted flag when convert exprs,
      // and depended projected flag is true
      FOREACH_CNT_X(e, right_->get_spec().output_, OB_SUCC(ret)) {
        (*e)->get_eval_info(eval_ctx_).projected_ = true;
      }
    }
  } else {
    int64_t read_size = 0;
    right_brs_ = &child_brs_;
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (HashJoinDrainMode::BUILD_HT == drain_mode_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: drain mode is build_hash_table", K(ret), K(spec_.id_));
    } else if (HashJoinDrainMode::RIGHT_DRAIN == drain_mode_) {
      const_cast<ObBatchRows *>(right_brs_)->size_ = 0;
      const_cast<ObBatchRows *>(right_brs_)->end_ = true;
      ret = OB_SUCCESS;
    } else if (OB_FAIL(OB_I(t1) right_batch_->get_next_batch(right_hj_part_stored_rows_,
                                                             max_output_cnt_,
                                                             read_size))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get right row from partition failed", K(ret));
      } else {
        const_cast<ObBatchRows *>(right_brs_)->size_ = 0;
        const_cast<ObBatchRows *>(right_brs_)->end_ = true;
        ret = OB_SUCCESS;
      }
    } else {
      nth_right_row_ += read_size;
      const_cast<ObBatchRows *>(right_brs_)->size_ = read_size;
      const_cast<ObBatchRows *>(right_brs_)->end_ = false;
      const_cast<ObBatchRows *>(right_brs_)->skip_->reset(read_size);
    }
  }
  if (!postprocessed_left_) {
    int tmp_ret = OB_SUCCESS;
    //load data and build hash table
    if (OB_SUCCESS != (tmp_ret = recursive_postprocess())) {
      if (OB_SUCC(ret) || OB_ITER_END == ret) {
        ret = tmp_ret;
      }
      LOG_WARN("failed to post process left", K(ret), K(tmp_ret));
    }
  }
  return ret;
}

int ObHashJoinOp::get_next_right_row()
{
  int ret = common::OB_SUCCESS;
  if (right_batch_ == NULL) {
    has_fill_right_row_ = true;
    clear_evaluated_flag();
    if (HashJoinDrainMode::BUILD_HT == drain_mode_) {
      if (OB_FAIL(right_->drain_exch())) {
        LOG_WARN("failed to drain the right child", K(ret), K(spec_.id_));
      } else {
        drain_mode_ = HashJoinDrainMode::RIGHT_DRAIN;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (HashJoinDrainMode::RIGHT_DRAIN == drain_mode_) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(OB_I(t1) right_->get_next_row())) {
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
    } else if (HashJoinDrainMode::BUILD_HT == drain_mode_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: drain mode is build_hash_table", K(ret), K(spec_.id_));
    } else if (HashJoinDrainMode::RIGHT_DRAIN == drain_mode_) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(OB_I(t1) right_batch_->get_next_row(
        right_read_row_))) {
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

int ObHashJoinOp::get_next_right_row_na()
{
  int ret = common::OB_SUCCESS;
  if (right_batch_ == NULL) {
    has_fill_right_row_ = true;
    bool got_row = false;
    bool is_left = false;
    if (HashJoinDrainMode::BUILD_HT == drain_mode_) {
      if (OB_FAIL(right_->drain_exch())) {
        LOG_WARN("failed to drain the right child", K(ret), K(spec_.id_));
      } else {
        drain_mode_ = HashJoinDrainMode::RIGHT_DRAIN;
      }
    }
    while (OB_SUCC(ret) && !got_row) {
      bool is_null = false;
      clear_evaluated_flag();
      if (HashJoinDrainMode::RIGHT_DRAIN == drain_mode_ || (read_null_in_naaj_ && is_shared_)) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(OB_I(t1) right_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get right row from child failed", K(ret));
        }
      } else if (FALSE_IT(non_preserved_side_is_not_empty_
                          |= (LEFT_ANTI_JOIN == MY_SPEC.join_type_))) {
        // mark this to forbid null value output in fill result batch
      } else if (OB_FAIL(check_join_key_for_naaj(is_left, is_null))) {
        LOG_WARN("failed to check null value for naaj", K(ret));
      } else if (is_null) {
        //left_anti_join_na : return iter_end
        //left_anti_join_sna : impossible to get null
        //right_anti_join : if non_preserved_side_is_not_empty_, skip, else got row
        if (is_left_naaj_na()) {
          ret = OB_ITER_END;
          read_null_in_naaj_ = true;
          state_ = JS_JOIN_END;
          LOG_TRACE("null break for left naaj", K(ret));
        } else if (is_left_naaj_sna()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("get null value in non preserved side, may generated wrong plan",
                   K(ret), K(MY_SPEC.join_type_), K(MY_SPEC.is_naaj_), K(MY_SPEC.is_sna_));
        } else if (!non_preserved_side_is_not_empty_) {
          got_row = true;
        } else {
          // if non-preserved-side is not empty, naaj do not output null value
        }
      } else {
        got_row = true;
      }
    }
  } else {
    has_fill_right_row_ = false;
    has_fill_left_row_ = false;
    clear_evaluated_flag();
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (HashJoinDrainMode::BUILD_HT == drain_mode_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: drain mode is build_hash_table", K(ret), K(spec_.id_));
    } else if (HashJoinDrainMode::RIGHT_DRAIN == drain_mode_ ||
        (read_null_in_naaj_ && is_shared_)) {
      ret = OB_ITER_END;
    } else if (OB_FAIL(OB_I(t1) right_batch_->get_next_row(
        right_read_row_))) {
      right_read_row_ = NULL;
      if (OB_ITER_END != ret) {
        LOG_WARN("get right row from partition failed", K(ret));
      }
    } else {
      ++nth_right_row_;
    }
  }
  if (!postprocessed_left_ && !read_null_in_naaj_) {
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
  bool dumped_partition = false;
  ObHashJoinStoredJoinRow *stored_row = nullptr;
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
    dumped_partition = true;
  }
  if (!need_material) {
  } else if (nullptr != right_read_row_) {
    if (OB_FAIL(right_hj_part_array_[part_idx].add_row(right_read_row_, stored_row))) {
      LOG_WARN("fail to add row", K(ret));
    } else {
      if (!dumped_partition && right_hj_part_array_[part_idx].has_switch_block()) {
        cur_full_right_partition_ = part_idx;
        cur_left_hist_ = &part_histograms_[cur_full_right_partition_];
      }
    }
  } else {
    if (OB_FAIL(right_hj_part_array_[part_idx].add_row(
        right_->get_spec().output_, &eval_ctx_, stored_row))) {
      LOG_WARN("fail to add row", K(ret));
    } else {
      stored_row->set_hash_value(cur_right_hash_value_);
      if (!dumped_partition && right_hj_part_array_[part_idx].has_switch_block()) {
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
  ObHashJoinStoredJoinRow *stored_row = nullptr;
  const int64_t part_idx = get_part_idx(cur_right_hash_value_);
  if (nullptr != right_read_row_) {
    if (OB_FAIL(right_hj_part_array_[part_idx].add_row(right_read_row_, stored_row))) {
      LOG_WARN("fail to add row", K(ret));
    }
  } else {
    if (OB_FAIL(right_hj_part_array_[part_idx].add_row(
        right_->get_spec().output_, &eval_ctx_, stored_row))) {
      LOG_WARN("fail to add row", K(ret));
    } else {
      stored_row->set_hash_value(cur_right_hash_value_);
    }
  }
  // auto memory manager
  bool updated = false;
  int64_t mem_used = get_cur_mem_used();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
    alloc_, [&](int64_t cur_cnt){ return row_count > cur_cnt; }, updated))) {
    LOG_WARN("failed to update max usable memory size periodically", K(ret), K(row_count));
  } else if (OB_LIKELY(need_dump(mem_used))) {
    bool need_dumped = false;
    if (max_partition_count_per_level_ != cur_dumped_partition_) {
      // it has dumped already
      need_dumped = true;
    } else if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(
        alloc_, [&](int64_t max_memory_size) {
          UNUSED(max_memory_size);
          update_remain_data_memory_size(
          row_count, sql_mem_processor_.get_mem_bound(), need_dumped);
          return need_dumped;
        },
        need_dumped, mem_used))) {
      LOG_WARN("failed to extend max memory size", K(ret));
    }
    // dump from last partition to the first partition
    int64_t cur_dumped_partition = part_count_ - 1;
    bool dumped = false;
    if ((need_dump(mem_used) || all_dumped()) && 0 <= cur_dumped_partition && OB_SUCC(ret)) {
      int64_t dumped_size = get_need_dump_size(mem_used);
      if (OB_FAIL(asyn_dump_partition(dumped_size, false, false, 0, nullptr))) {
        LOG_WARN("failed to asyn dump partition", K(ret));
      } else if (OB_FAIL(asyn_dump_partition(INT64_MAX, true, true, cur_dumped_partition_ + 1, nullptr))) {
        LOG_WARN("failed to asyn dump partition", K(ret));
      }
    }
    LOG_TRACE("trace right need dump", K(part_level_),
      K(buf_mgr_->get_reserve_memory_size()),
      K(buf_mgr_->get_total_alloc_size()),
      K(cur_dumped_partition), K(buf_mgr_->get_dumped_size()),
      K(cur_dumped_partition_));
  }
  return ret;
}

int ObHashJoinOp::dump_remain_part_for_cache_aware()
{
  int ret = OB_SUCCESS;
  if (max_partition_count_per_level_ > cur_dumped_partition_) {
    // If it has dumped partition, we ensure the partition must has dumped
    // dump the left and right partition that the index is granter then cur_dumped_partition_
    if (OB_FAIL(asyn_dump_partition(INT64_MAX, false, true, cur_dumped_partition_ + 1, nullptr))) {
      LOG_WARN("failed to asyn dump partition", K(ret));
    } else if (OB_FAIL(asyn_dump_partition(INT64_MAX, true, true, cur_dumped_partition_ + 1, nullptr))) {
      LOG_WARN("failed to asyn dump partition", K(ret));
    } else if (OB_FAIL(update_dumped_partition_statistics(true))) {
      LOG_WARN("failed to update dumped partition statistics", K(ret));
    } else if (OB_FAIL(update_dumped_partition_statistics(false))) {
      LOG_WARN("failed to update dumped partition statistics", K(ret));
    }
  }
  return ret;
}

int ObHashJoinOp::get_next_batch_right_rows()
{
  int ret = OB_SUCCESS;
  int64_t cur_partition_in_memory = max_partition_count_per_level_ == cur_dumped_partition_ ?
                                    part_count_ : cur_dumped_partition_ + 1;
  cur_full_right_partition_ = INT64_MAX;
  right_read_row_ = nullptr;
  int64_t row_count = 0;
  bool is_left_side = false;
  if (enable_batch_) {
    OZ(right_last_row_.restore(right_->get_spec().output_, eval_ctx_));
  }
  while (OB_SUCC(ret) && INT64_MAX == cur_full_right_partition_) {
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL((this->*get_next_right_row_func_)())) {
      if (OB_ITER_END == ret) {
        right_iter_end_ = true;
      } else {
        LOG_WARN("failed to get next right row", K(ret));
      }
    } else {
      bool skipped = false;
      if (NULL == right_read_row_) {
        if (OB_FAIL(calc_hash_value(right_join_keys_, right_hash_funcs_,
                                    cur_right_hash_value_, is_left_side,
                                    skipped))) {
          LOG_WARN("get hash value failed", K(ret));
        } else if (skipped) {
          continue;
        }
      } else {
        cur_right_hash_value_ = right_read_row_->get_hash_value();
      }
    }
    if (OB_SUCC(ret)) {
      if (enable_batch_) {
        if (OB_FAIL(insert_batch_row(cur_partition_in_memory))) {
        }
      } else {
        if (OB_FAIL(insert_all_right_row(row_count))) {
        }
      }
    }
    ++row_count;
  }
  has_fill_right_row_ = false;
  has_fill_left_row_ = false;
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    // probe left all right rows from 0 partition to last partition
    cur_full_right_partition_ = -1;
    LOG_DEBUG("debug partition start", K(cur_full_right_partition_));
    if (!enable_batch_) {
      if (-1 != cur_dumped_partition_) {
        // recalc cache aware partition count
        calc_cache_aware_partition_count();
        LOG_TRACE("debug partition", K(level1_part_count_), K(level1_bit_), K(level2_part_count_),
          K(row_count));
        HashJoinHistogram *tmp_part_histograms = nullptr;
        PartitionSplitter part_splitter;
        if (OB_FAIL(dump_remain_part_for_cache_aware())) {
          LOG_WARN("failed to dump remain part", K(ret));
        } else if (OB_FAIL(repartition(part_splitter, part_histograms_, hj_part_array_, true))) {
          LOG_WARN("failed to repartition", K(ret));
        } else if (OB_FAIL(repartition(
            right_splitter_, tmp_part_histograms, right_hj_part_array_, false))) {
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
  int64_t part_count = 0 < level2_part_count_ ?
                       level1_part_count_ * level2_part_count_ :
                       level1_part_count_;
  int64_t dump_part_count = max_partition_count_per_level_ == cur_dumped_partition_ ?
                                    part_count_ : cur_dumped_partition_ + 1;
  do {
    ++cur_full_right_partition_;
    if (cur_full_right_partition_ >= part_count) {
      ret = OB_ITER_END;
    } else if (!part_histograms_[cur_full_right_partition_].empty()) {
      if (right_splitter_.is_valid()) {
        HashJoinHistogram::HistPrefixArray *prefix_hist_count = right_splitter_.part_histogram_.prefix_hist_count2_;
        if (0 == right_splitter_.get_total_row_count()) {
          ret = OB_ITER_END;
          LOG_DEBUG("hj_part_array_ has no row in memory", K(ret));
        } else if (OB_ISNULL(right_splitter_.part_histogram_.h2_) || OB_ISNULL(prefix_hist_count)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("h2 is null", K(ret), K(level1_part_count_), K(level2_part_count_), K(part_count));
        } else {
          if (cur_full_right_partition_ >= prefix_hist_count->count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid prefix histogram", K(ret), K(cur_full_right_partition_),
              K(prefix_hist_count->count()));
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
          int64_t level2_bit = (0 != level2_part_count_) ? __builtin_ctz(level2_part_count_) : 0;
          int64_t level1_part_idx = (cur_full_right_partition_ >> level2_bit);
          if (level1_part_idx < dump_part_count) {
            cur_left_hist_ = &part_histograms_[cur_full_right_partition_];
            break;
          }
        }
      }
    }
  } while (OB_SUCC(ret) && cur_full_right_partition_ < part_count);
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
      //每次取新的右行时需要重设填充标志
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
        LOG_WARN("left partition is empty", K(ret), K(cur_full_right_partition_),
          K(cur_dumped_partition_), K(enable_batch_));
      } else {
        // cur_right_hash_value_ is set when next_func
        const int64_t bucket_id = cur_left_hist_->get_bucket_idx(cur_right_hash_value_);
        if (0 == bucket_id) {
          cur_bucket_idx_ = 0;
        } else {
          cur_bucket_idx_ = cur_left_hist_->prefix_hist_count_->at(bucket_id - 1);
        }
        max_bucket_idx_ = cur_left_hist_->prefix_hist_count_->at(bucket_id);
        if (max_bucket_idx_ > cur_bucket_idx_ && OB_FAIL(get_match_row(is_matched))) {
        }
        LOG_DEBUG("debug get next right row", K(cur_right_hash_value_),
          K(cur_bucket_idx_), K(max_bucket_idx_), K(bucket_id), K(ret),
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
        // 由于左表为空，不需要再读右表，模拟右表已经读完的状态，返回OB_ITER_END
        ret = OB_ITER_END;
        if (HJProcessor::NEST_LOOP == hj_processor_) {
          nest_loop_state_ = HJLoopState::LOOP_END;
        }
      } else {
        if (nullptr != right_batch_) {
          if (OB_FAIL(right_batch_->set_iterator())) {
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
    if (is_vectorized()) {
      if (OB_FAIL((this->*get_next_right_batch_func_)())) {
        LOG_WARN("fail to get next right row batch", K(ret));
      } else if (right_brs_->size_ == 0 && right_brs_->end_) {
        ret = OB_ITER_END;
      } else if (read_null_in_naaj_) {
        ret = OB_ITER_END;
      }
    } else if (opt_cache_aware_) {
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
    } else if (OB_FAIL((this->*get_next_right_row_func_)()) && OB_ITER_END != ret) {
      LOG_WARN("failed to get next right row", K(ret));
    }
  }
  // for right semi join, if match, then return, so for nest loop process, it only return once
  if (!is_vectorized()) {
    right_has_matched_ = NULL != right_read_row_
        && (right_read_row_->is_match()
            || (has_right_bitset_ && right_bit_set_.has_member(nth_right_row_)));
  }
  return ret;
}

int ObHashJoinOp::calc_hash_value(
  const ObIArray<ObExpr*> &join_keys,
  const ObIArray<ObHashFunc> &hash_funcs,
  uint64_t &hash_value,
  bool is_left_side,
  bool &skipped)
{
  int ret = OB_SUCCESS;
  hash_value = HASH_SEED;
  ObDatum *datum = nullptr;
  skipped = false;
  bool need_null_random = false;
  bool skip_null = (is_left_side && skip_left_null_) || (!is_left_side && skip_right_null_);
  for (int64_t idx = 0; OB_SUCC(ret) && idx < join_keys.count() ; ++idx) {
    if (OB_FAIL(join_keys.at(idx)->eval(eval_ctx_, datum))) {
      LOG_WARN("failed to eval datum", K(ret));
    } else {
      need_null_random |= (datum->is_null() && !MY_SPEC.is_ns_equal_cond_.at(idx));
      if (OB_FAIL(hash_funcs.at(idx).hash_func_(*datum, hash_value, hash_value))) {
        LOG_WARN("failed to do hash", K(ret));
      }
    }
  }
  need_null_random &= (MY_SPEC.join_type_ != LEFT_ANTI_JOIN && MY_SPEC.join_type_ != RIGHT_ANTI_JOIN);
  if (need_null_random) {
    if (skip_null) {
      skipped = true;
    } else {
      hash_value = null_random_hash_value_++;
    }
  }
  hash_value = hash_value & ObHashJoinStoredJoinRow::HASH_VAL_MASK;
  LOG_DEBUG("trace calc hash value", K(hash_value), K(join_keys.count()), K(hash_funcs.count()));
  return ret;
}

int ObHashJoinOp::calc_hash_value_batch(const ObIArray<ObExpr*> &join_keys,
                                        const ObBatchRows *brs,
                                        const bool is_from_row_store,
                                        uint64_t *hash_vals,
                                        const ObHashJoinStoredJoinRow **store_rows,
                                        bool is_left_side)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(brs));
  if (OB_FAIL(ret)) {
  } else if (!is_from_row_store) {
    uint64_t seed = HASH_SEED;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < join_keys.count() ; ++idx) {
      ObExpr *expr = join_keys.at(idx); // expr ptr check in cg, not check here
      if (OB_FAIL(expr->eval_batch(eval_ctx_, *brs->skip_, brs->size_))) {
        LOG_WARN("eval failed", K(ret));
      } else {
        ObBatchDatumHashFunc hash_func = expr->basic_funcs_->murmur_hash_v2_batch_;
        const bool is_batch_seed = (idx > 0);
        hash_func(hash_vals,
                  expr->locate_batch_datums(eval_ctx_), expr->is_batch_result(),
                  *brs->skip_, brs->size_,
                  is_batch_seed ? hash_vals : &seed,
                  is_batch_seed);
      }
    }
    if (OB_SUCC(ret)) {
      uint16_t selector_idx = 0;
      bool skip_null = (is_left_side && skip_left_null_) || (!is_left_side && skip_right_null_);
      auto op = [&](const int64_t i) __attribute__((always_inline)) {
        bool need_null_random = false;
        for (int64_t j = 0; j < join_keys.count() && !need_null_random; ++j) {
          ObDatum &curr_key = join_keys.at(j)->locate_batch_datums(eval_ctx_)[i];
          need_null_random |= (curr_key.is_null() && !MY_SPEC.is_ns_equal_cond_.at(j));
        }
        need_null_random &= (MY_SPEC.join_type_ != LEFT_ANTI_JOIN && MY_SPEC.join_type_ != RIGHT_ANTI_JOIN);
        if (need_null_random) {
          if (skip_null) {
          } else {
            hash_vals[i] = null_random_hash_value_++;
            right_selector_[selector_idx++] = i;
          }
        } else {
          right_selector_[selector_idx++] = i;
        }
        hash_vals[i] = hash_vals[i] & ObHashJoinStoredJoinRow::HASH_VAL_MASK;
        return OB_SUCCESS;
      };
      ObBitVector::flip_foreach(*brs->skip_, brs->size_, op);
      right_selector_cnt_ = selector_idx;
    }
  } else {
    for (int64_t i = 0; i < brs->size_; i++) {
      OB_ASSERT(OB_NOT_NULL(store_rows[i]));
      hash_vals[i] = store_rows[i]->get_hash_value();
      right_selector_[i] = i;
    }
    right_selector_cnt_ = brs->size_;
  }
  return ret;
}

int ObHashJoinOp::calc_right_hash_value()
{
  int ret = OB_SUCCESS;
  bool is_left = false;
  bool skipped = false;
  if (is_vectorized()) {
    if (OB_FAIL(calc_hash_value_batch(right_join_keys_, right_brs_, right_read_from_stored_,
                                      right_hash_vals_, right_hj_part_stored_rows_,
                                      is_left))) {
      LOG_WARN("fail to calc hash value batch", K(ret));
    }
  } else if (opt_cache_aware_) {
    // has already calculated hash value
  } else if (NULL == right_read_row_) {
    if (OB_FAIL(calc_hash_value(right_join_keys_, right_hash_funcs_, cur_right_hash_value_,
                                is_left, skipped))) {
      LOG_WARN("get hash value failed", K(ret));
    }
  } else {
    cur_right_hash_value_ = right_read_row_->get_hash_value();
  }
  if (skipped) {
    // read next row
    state_ = JS_READ_RIGHT;
  } else {
    state_ = JS_READ_HASH_ROW;
  }
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
    for (int64_t i = 0; i < part_count_ && OB_SUCC(ret); i ++) {
      total_size += part_array[i].get_size_in_memory() + part_array[i].get_size_on_disk();
    }
    for (int64_t i = 0; i < part_count_ && OB_SUCC(ret); i ++) {
      if (force) {
        // finish dump在之前没有dump情况下，不会强制dump，所以这里需要先dump
        if (OB_FAIL(part_array[i].dump(true, INT64_MAX))) {
          LOG_WARN("failed to dump", K(ret));
        } else if (cur_dumped_partition_ >= i) {
          cur_dumped_partition_ = i - 1;
        }
      }
      if (part_array[i].is_dumped()) {
        if (OB_SUCC(ret)) {
          // 认为要么全部dump，要么全部in-memory
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
      } else if (OB_FAIL(part_array[i].record_pre_batch_info(
        part_count_,
        cur_hash_table_->nbuckets_,
        total_size))) {
        LOG_WARN("failed to record pre-batch info", K(ret), K(part_count_),
          K(cur_hash_table_->nbuckets_), K(total_size));
      }
    }
    if (force && for_left) {
      // mark dump all left partitions
      cur_dumped_partition_ = -1;
    }
    LOG_TRACE("finish dump: ", K(part_level_), K(buf_mgr_->get_reserve_memory_size()),
      K(buf_mgr_->get_total_alloc_size()), K(buf_mgr_->get_dumped_size()),
      K(cur_dumped_partition_), K(for_left), K(need_dump), K(force), K(total_size),
      K(dumped_row_count), K(part_count_));
  }
  return ret;
}

int ObHashJoinOp::read_right_func_end()
{
  int ret = OB_SUCCESS;
  if (LEFT_ANTI_JOIN == MY_SPEC.join_type_) {
    state_ = JS_LEFT_ANTI_SEMI;
    cur_tuple_ = hash_table_.buckets_->at(0).get_stored_row();
    cur_bkid_ = 0;
  } else if (need_left_join()) {
    state_ = JS_FILL_LEFT;
    cur_tuple_ = hash_table_.buckets_->at(0).get_stored_row();
    cur_bkid_ = 0;
  } else {
    state_  = JS_JOIN_END;
  }

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


int ObHashJoinOp::calc_equal_conds(bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = true;
  const ObIArray<ObExpr *> &conds = MY_SPEC.equal_join_conds_;
  ObDatum *cmp_res = NULL;
  ARRAY_FOREACH(conds, i) {
    if (OB_FAIL(conds.at(i)->eval(eval_ctx_, cmp_res))) {
      LOG_WARN("fail to calc other join condition", K(ret), K(*conds.at(i)));
    } else if (cmp_res->is_null() || 0 == cmp_res->get_int()) {
      is_match = false;
      break;
    }
  }
  LOG_DEBUG("trace calc equal conditions", K(ret), K(is_match), K(conds),
    K(ROWEXPR2STR(eval_ctx_, MY_SPEC.all_join_keys_)));
  return ret;
}

int ObHashJoinOp::get_match_row(bool &is_matched)
{
  int ret = OB_SUCCESS;
  is_matched = false;
  while (cur_bucket_idx_ < max_bucket_idx_ && !is_matched && OB_SUCC(ret)) {
    HistItem &item = cur_left_hist_->h2_->at(cur_bucket_idx_);
    LOG_DEBUG("trace match", K(ret), K(is_matched), K(cur_right_hash_value_),
          K(ROWEXPR2STR(eval_ctx_, MY_SPEC.all_join_keys_)), K(item.hash_value_));
    if (cur_right_hash_value_ == item.hash_value_) {
      clear_evaluated_flag();
      has_fill_left_row_ = false;
      if (OB_FAIL(convert_exprs(
          item.store_row_, left_->get_spec().output_, has_fill_left_row_))) {
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
        LOG_DEBUG("trace match", K(ret), K(is_matched), K(cur_right_hash_value_),
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
      auto next_func = [&](const ObHashJoinStoredJoinRow *&right_read_row) {
        int ret = OB_SUCCESS;
        ObHashJoinPartition *hj_part = &right_hj_part_array_[cur_full_right_partition_];
        if (OB_FAIL(hj_part->get_next_row(right_read_row))) {
        } else {
          cur_right_hash_value_ = right_read_row->get_hash_value();
        }
        return ret;
      };
      ret = read_hashrow_for_cache_aware(next_func);
    } else {
      auto next_func = [&](const ObHashJoinStoredJoinRow *&right_read_row) {
        int ret = OB_SUCCESS;
        if (cur_right_hist_->empty()) {
          ret = OB_ITER_END;
        } else if (cur_probe_row_idx_ >= max_right_bucket_idx_) {
          ret = OB_ITER_END;
        } else {
          HistItem &hist_item = cur_right_hist_->h2_->at(cur_probe_row_idx_);
          right_read_row = hist_item.store_row_;
          cur_right_hash_value_ = hist_item.hash_value_;
          ++cur_probe_row_idx_;
        }
        return ret;
      };
      ret = read_hashrow_for_cache_aware(next_func);
    }
  } else if (is_vectorized()) {
    if (LEFT_SEMI_JOIN == MY_SPEC.join_type_ || LEFT_ANTI_JOIN == MY_SPEC.join_type_) {
      ret = read_hashrow_batch_for_left_semi_anti();
    } else {
      ret = read_hashrow_batch();
    }
  } else {
    ret = read_hashrow_normal();
  }
  return ret;
}

int ObHashJoinOp::read_hashrow_batch()
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
  right_batch_traverse_cnt_++;
  probe_cnt_ +=  right_selector_cnt_;
  if (1 == right_batch_traverse_cnt_) {
    // check bloom filter
    if (enable_bloom_filter_) {
      int64_t idx = 0;
      for (int64_t i = 0; i < right_selector_cnt_; i++) {
        if (bloom_filter_->exist(right_hash_vals_[right_selector_[i]])) {
          right_selector_[idx++] = right_selector_[i];
        }
      }
      right_selector_cnt_ = idx;
    }

    // will not match hash table if partition idx greater than cur_dumped_partition_
    if (max_partition_count_per_level_ != cur_dumped_partition_) {
      int64_t idx = 0;
      for (int64_t i = 0; i < right_selector_cnt_; i++) {
        if (get_part_idx(right_hash_vals_[right_selector_[i]]) <= cur_dumped_partition_) {
          right_selector_[idx++] = right_selector_[i];
        }
      }
      right_selector_cnt_ = idx;
    }

    // probe hash table
    {
      // group prefetch
      for (int64_t i = 0; i < right_selector_cnt_; i++) {
        uint64_t mask = cur_hash_table_->nbuckets_ - 1;
        __builtin_prefetch(&cur_hash_table_->buckets_->at(mask & right_hash_vals_[right_selector_[i]]),
                           0, // for read
                           1); // low temporal locality
      }

      int64_t idx = 0;
      ObHashJoinStoredJoinRow *tuple = NULL;
      for (int64_t i = 0; i < right_selector_cnt_; i++) {
        tuple = cur_hash_table_->get(right_hash_vals_[right_selector_[i]]);
        if (NULL != tuple) {
          cur_tuples_[idx] = tuple;
          right_selector_[idx++] = right_selector_[i];
        }
      }
      right_selector_cnt_ = idx;
    }
    // convert right rows from stored row
    if (right_read_from_stored_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < right_selector_cnt_; i++) {
        batch_info_guard.set_batch_idx(right_selector_[i]);
        OZ (convert_exprs_batch_one(right_hj_part_stored_rows_[right_selector_[i]],
                                right_->get_spec().output_));
      }
    }
  }

  // prefetch store row
  // FIXME bin.lb:
  // 1. Try pipeline prefetch and emit less prefetchs
  // 2. No prefetch for small hash table
  const int64_t L1_CACHE_SIZE = 64;
  if (OB_FAIL(ret)) {
  } else if (sizeof(ObHashJoinStoredJoinRow)
      + left_->get_spec().output_.count() * sizeof(ObDatum) <= L1_CACHE_SIZE) {
    for (int64_t i = 0; i < right_selector_cnt_; i++) {
      __builtin_prefetch(cur_tuples_[i], 0 /* for read */, 3 /* high temporal locality */);
    }
  } else {
    // FIXME bin.lb: prefetch the begin and the end of stored row?
    // emit 2 prefetch if stored row exceed L1_CACHE_SIZE
    for (int64_t i = 0; i < right_selector_cnt_; i++) {
      __builtin_prefetch(cur_tuples_[i], 0 /* for read */, 3 /* high temporal locality */);
      __builtin_prefetch(reinterpret_cast<char *>(cur_tuples_[i]) + L1_CACHE_SIZE,
                         0 /* for read */, 3 /* high temporal locality */);
    }
  }

  // calculate equal and other conditions
  uint64_t idx = 0;
  batch_info_guard.set_batch_size(right_brs_->size_);

  if (OB_FAIL(ret)) {
  } else if (MY_SPEC.can_prob_opt_) { // no other conditions && do equal compare directly
    for (int64_t i = 0; i < right_selector_cnt_; i++) {
      int64_t batch_idx = right_selector_[i];
      batch_info_guard.set_batch_idx(batch_idx);
      bool matched = false;
      int cmp_ret = 0;
      ObHashJoinStoredJoinRow *tuple = cur_tuples_[i];
      while (!matched && NULL != tuple && OB_SUCC(ret)) {
        ++hash_link_cnt_;
        ++hash_equal_cnt_;
        OZ (convert_exprs_batch_one(tuple, left_->get_spec().output_));
        matched = true;
        FOREACH_CNT_X(e, MY_SPEC.equal_join_conds_, (matched && (OB_SUCCESS == ret))) {
          // we check children's output_ are consistant with join key in cg,
          // so we can locate datums without eval again
          ObDatum &l = (*e)->args_[0]->locate_batch_datums(eval_ctx_)[batch_idx];
          ObDatum &r = (*e)->args_[1]->locate_batch_datums(eval_ctx_)[batch_idx];
          if (!l.is_null()) {
            if (OB_FAIL((*e)->args_[0]->basic_funcs_->null_first_cmp_(l, r, cmp_ret))) {
              LOG_WARN("failed to compare", K(ret));
            } else {
              matched = (cmp_ret == 0);
            }
          } else {
            matched = false;
          }
        }
        if (!matched) {
          tuple = tuple->get_next();
        }
      }
      if (matched) {
        cur_tuples_[idx] = tuple;
        right_selector_[idx++] = right_selector_[i];
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < right_selector_cnt_; i++) {
      int64_t batch_idx = right_selector_[i];
      batch_info_guard.set_batch_idx(batch_idx);
      bool matched = false;
      ObHashJoinStoredJoinRow *tuple = cur_tuples_[i];
      while (!matched && NULL != tuple && OB_SUCC(ret)) {
        ++hash_link_cnt_;
        ++hash_equal_cnt_;
        clear_datum_eval_flag();
        if (OB_FAIL(convert_exprs_batch_one(tuple, left_->get_spec().output_))) {
          LOG_WARN("failed to convert expr", K(ret));
        } else if (OB_FAIL(calc_equal_conds(matched))) {
          LOG_WARN("calc equal conditions failed", K(ret));
        } else if (matched && OB_FAIL(calc_other_conds(matched))) {
          LOG_WARN("calc other conditions failed", K(ret));
        } else {
          LOG_DEBUG("trace match", K(ret), K(matched), K(batch_idx),
                    K(ROWEXPR2STR(eval_ctx_, MY_SPEC.all_join_keys_)));
        }
        if (!matched) {
          tuple = tuple->get_next();
        }
      }
      if (matched) {
        cur_tuples_[idx] = tuple;
        right_selector_[idx++] = right_selector_[i];
      }
    }
  }

  right_selector_cnt_ = idx;

  if (OB_SUCC(ret) && right_selector_cnt_ == 0 && !IS_RIGHT_STYLE_JOIN(MY_SPEC.join_type_)) {
    ret = OB_ITER_END;
  }

  return ret;
}

int ObHashJoinOp::read_hashrow_normal()
{
  int ret = OB_SUCCESS;
  ++probe_cnt_;
  // 之前已经match，后续是继续遍历bucket链表其他tuple数据，不需要再次判断bucket id是否可以过滤，一定返回true
  if (enable_bloom_filter_ && !bloom_filter_->exist(cur_right_hash_value_)) {
    ++bitset_filter_cnt_;
    has_fill_left_row_ = false;
    cur_tuple_ = NULL;
    ret = OB_ITER_END; // is end for scanning this bucket
  } else if (max_partition_count_per_level_ != cur_dumped_partition_) {
    int64_t part_idx = get_part_idx(cur_right_hash_value_);
    if (part_idx > cur_dumped_partition_) {
      // part index is greater than cur_dumped_partition_, than the partition has no memory data
      if (0 < hj_part_array_[part_idx].get_size_in_memory()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expect no memory data in the partition", K(ret), K(part_idx),
          K(hj_part_array_[part_idx].get_size_in_memory()), K(cur_dumped_partition_),
          K(part_level_), K(part_count_));
      } else {
        cur_tuple_ = NULL;
        has_fill_left_row_ = false;
        ret = OB_ITER_END; // is end for scanning this bucket
      }
    }
  }

  if (OB_SUCC(ret)) {
    bool is_matched = false;
    ObHashJoinStoredJoinRow *tuple = NULL;
    if (NULL == cur_tuple_) {
      tuple = cur_hash_table_->get(cur_right_hash_value_);
    } else {
      tuple = cur_tuple_->get_next();
    }
    while (!is_matched && NULL != tuple && OB_SUCC(ret)) {
      ++hash_link_cnt_;
      ++hash_equal_cnt_;
      clear_evaluated_flag();
      if(OB_FAIL(convert_exprs(tuple, left_->get_spec().output_, has_fill_left_row_))) {
        LOG_WARN("failed to fill left row", K(ret));
      } else if (OB_FAIL(only_join_right_row())) {
        LOG_WARN("failed to fill right row", K(ret));
      } else if (OB_FAIL(calc_equal_conds(is_matched))){
        LOG_WARN("calc equal conds failed", K(ret));
      } else if (is_matched && OB_FAIL(calc_other_conds(is_matched))) {
        LOG_WARN("calc other conds failed", K(ret));
      } else {
        // do nothing
        LOG_DEBUG("trace match", K(ret), K(is_matched), K(cur_right_hash_value_),
          K(ROWEXPR2STR(eval_ctx_, MY_SPEC.all_join_keys_)));
      }
      if (OB_SUCC(ret) && !is_matched) {
        tuple = tuple->get_next();
      }
    } // while end

    if (OB_FAIL(ret)) {
    } else if (!is_matched) {
      has_fill_left_row_ = false;
      cur_tuple_ = NULL;
      ret = OB_ITER_END; // is end for scanning this bucket
    } else {
      cur_tuple_ = tuple; // last matched tuple
      right_has_matched_ = true;
      has_fill_left_row_ = true;
      if (INNER_JOIN != MY_SPEC.join_type_) {
        tuple->set_is_match(true);
        if (LEFT_SEMI_JOIN == MY_SPEC.join_type_) {
          cur_hash_table_->del(cur_right_hash_value_, tuple);
          mark_return();
        } else if (LEFT_ANTI_JOIN == MY_SPEC.join_type_) {
          cur_hash_table_->del(cur_right_hash_value_, tuple);
        }
      }
    }
  }
  return ret;
}

int ObHashJoinOp::join_rows_with_right_null()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(blank_row(right_->get_spec().output_))) {
    LOG_WARN("failed to blank right null", K(ret));
  } else if (OB_FAIL(only_join_left_row())) {
    LOG_WARN("failed to blank left row", K(ret));
  } else {
    has_fill_right_row_ = true;
  }
  return ret;
}

int ObHashJoinOp::join_rows_with_left_null_batch_one(int64_t batch_idx)
{
  blank_row_batch_one(left_->get_spec().output_);
  return convert_right_exprs_batch_one(batch_idx);
}

int ObHashJoinOp::join_rows_with_left_null()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(blank_row(left_->get_spec().output_))) {
    LOG_WARN("failed to blank right null", K(ret));
  } else if (OB_FAIL(only_join_right_row())) {
    LOG_WARN("failed to fill right row", K(ret));
  } else {
    has_fill_left_row_ = true;
  }
  return ret;
}

int ObHashJoinOp::convert_right_exprs_batch_one(int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  if (right_read_from_stored_) {
    ret = convert_exprs_batch_one(right_hj_part_stored_rows_[batch_idx],
                                  right_->get_spec().output_);
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

int ObHashJoinOp::left_anti_semi_read_hashrow_going_batch()
{
  int64_t idx = 0;
  if (LEFT_SEMI_JOIN == MY_SPEC.join_type_) {
    for (int64_t i = 0; i < right_selector_cnt_; i++) {
      auto tuple = cur_tuples_[i];
      if (NULL != tuple) {
        cur_tuples_[idx] = tuple;
        right_selector_[idx++] = right_selector_[i];
      }
    }
    right_selector_cnt_ = idx;
  } else {
    for (int64_t i = 0; i < right_selector_cnt_; i++) {
      auto tuple = cur_tuples_[i];
      auto next = tuple->get_next();
      if (NULL != next) {
        cur_tuples_[idx] = next;
        right_selector_[idx++] = right_selector_[i];
      }
    }
    right_selector_cnt_ = idx;
  }
  return OB_SUCCESS;
}

int ObHashJoinOp::left_anti_semi_read_hashrow_end_batch()
{
  int ret = OB_SUCCESS;
  if (RECURSIVE == hj_processor_) {
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    for (int64_t i = 0; OB_SUCC(ret) && i < right_brs_->size_; i++) {
      if (right_brs_->skip_->exist(i)) {
        continue;
      }
      const int64_t part_idx = get_part_idx(right_hash_vals_[i]);
      if (check_right_need_dump(part_idx)) {
        guard.set_batch_idx(i);
        OZ(dump_right_row_batch_one(part_idx, i));
      }
    }
  }
  state_ = JS_READ_RIGHT;
  return ret;
}

// right anti/semi only need traverse right batch once
int ObHashJoinOp::right_anti_semi_read_hashrow_going_batch()
{
  int ret = OB_SUCCESS;
  bool need_mark_return = false;
  brs_.size_ = right_brs_->size_;

  if (RIGHT_SEMI_JOIN == MY_SPEC.join_type_) {
    if (right_selector_cnt_ > 0) {
      brs_.skip_->set_all(brs_.size_);
      need_mark_return = true;
      for (int64_t i = 0; i < right_selector_cnt_; i++) {
        brs_.skip_->unset(right_selector_[i]);
      }
    }
    if (RECURSIVE == hj_processor_) {
      ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
      for (int64_t i = 0; OB_SUCC(ret) && i < right_brs_->size_; i++) {
        if (right_brs_->skip_->exist(i)) {
          continue;
        }
        const int64_t part_idx = get_part_idx(right_hash_vals_[i]);
        if (check_right_need_dump(part_idx)) {
          guard.set_batch_idx(i);
          OZ(dump_right_row_batch_one(part_idx, i));
        }
      }
    }
  } else {
    // RIGHT_ANTI_JOIN
    brs_.skip_->reset(brs_.size_);
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    for (int64_t i = 0; i < right_selector_cnt_; i++) {
      brs_.skip_->set(right_selector_[i]);
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < right_brs_->size_; i++) {
      if (right_brs_->skip_->exist(i)) {
        brs_.skip_->set(i);
        continue;
      }
      if (brs_.skip_->exist(i)) { // matched
        continue;
      }
      const int64_t part_idx = get_part_idx(right_hash_vals_[i]);
      guard.set_batch_idx(i);
      if (RECURSIVE == hj_processor_ && check_right_need_dump(part_idx)) {
        brs_.skip_->set(i);
        OZ(dump_right_row_batch_one(part_idx, i));
      } else {
        need_mark_return = true;
        OZ (convert_right_exprs_batch_one(i));
      }
    }
    if (OB_SUCC(ret) && need_mark_return && MY_SPEC.is_naaj_ && non_preserved_side_is_not_empty_) {
      need_mark_return = false;
      clear_evaluated_flag();
      if (OB_FAIL(right_join_keys_.at(0)->eval_batch(eval_ctx_, *brs_.skip_, brs_.size_))) {
        LOG_WARN("failed to eval right join key", K(ret));
      } else {
        ObDatumVector key_datum = right_join_keys_.at(0)->locate_expr_datumvector(eval_ctx_);
        for (int64_t i = 0; i < brs_.size_; ++i) {
          if (brs_.skip_->at(i)) {
            continue;
          }
          if (key_datum.at(i)->is_null()) {
            brs_.skip_->set(i);
          } else {
            need_mark_return = true;
          }
        }
      }
    }
  }

  if (need_mark_return) {
    set_output_eval_info();
    mark_return();
  } else {
    // reset skip vector if not return
    brs_.skip_->reset(brs_.size_);
    brs_.size_ = 0;
  }
  state_ = JS_READ_RIGHT;

  return ret;
}

int ObHashJoinOp::dump_right_row_batch_one(int64_t part_idx, int64_t batch_idx)
{
  int ret = OB_SUCCESS;
  ObHashJoinStoredJoinRow *stored_row = nullptr;
  if (right_read_from_stored_) {
    if (OB_FAIL(right_hj_part_array_[part_idx].add_row(
        right_hj_part_stored_rows_[batch_idx], stored_row))) {
      LOG_WARN("fail to add row", K(ret));
    }
  } else {
    // eval_ctx_.batch_idx_ = batch_idx; // batch_idx should be set by caller
    if (OB_FAIL(right_hj_part_array_[part_idx].add_row(
        right_->get_spec().output_, &eval_ctx_, stored_row))) {
      LOG_WARN("fail to add row", K(ret));
    }
  }
  if (NULL != stored_row) {
    stored_row->set_hash_value(right_hash_vals_[batch_idx]);
  }

  return ret;
}

int ObHashJoinOp::right_anti_semi_read_hashrow_end_batch()
{
  int ret = OB_SUCCESS;
  state_ = JS_READ_RIGHT;
  return ret;
}

int ObHashJoinOp::outer_join_read_hashrow_going_batch()
{
  int ret = OB_SUCCESS;
  brs_.size_ = right_brs_->size_;
  brs_.skip_->set_all(brs_.size_);

  bool need_mark_return = right_selector_cnt_ > 0;
  int64_t idx = 0;
  for (int64_t i = 0; i < right_selector_cnt_; i++) {
    int64_t batch_idx = right_selector_[i];
    if (RIGHT_OUTER_JOIN != MY_SPEC.join_type_) {
      cur_tuples_[i]->set_is_match(true);
    }
    brs_.skip_->unset(batch_idx);
    auto tuple = cur_tuples_[i]->get_next();
    if (NULL != tuple) {
      cur_tuples_[idx] = tuple;
      right_selector_[idx++] = right_selector_[i];
    }
  }
  right_selector_cnt_ = idx;

  if (1 == right_batch_traverse_cnt_) {
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    for (int64_t i = 0; OB_SUCC(ret) && i < right_brs_->size_; i++) {
      if (right_brs_->skip_->exist(i)) {
        continue;
      }
      const int64_t part_idx = get_part_idx(right_hash_vals_[i]);
      guard.set_batch_idx(i);
      // for row in dumped partition
      if (RECURSIVE == hj_processor_ && check_right_need_dump(part_idx)) {
        if (OB_FAIL(dump_right_row_batch_one(part_idx, i))) {
          LOG_WARN("fail to dump right row", K(ret), K(part_idx), K(i));
        }
      } else if (need_right_join()) {
        if (brs_.skip_->exist(i)) { // not match
          need_mark_return = true;
          OZ (join_rows_with_left_null_batch_one(i)); // right outer join, null-left row
          brs_.skip_->unset(i);
        }
      }
    }
  }

  if (need_mark_return) {
    set_output_eval_info();
    mark_return();
  } else {
    // reset skip vector if not return
    brs_.skip_->reset(brs_.size_);
    brs_.size_ = 0;

    state_ = JS_READ_RIGHT;
  }

  return ret;
}

void ObHashJoinOp::set_output_eval_info() {
  for (int64_t i = 0; i < left_->get_spec().output_.count(); i++) {
    ObEvalInfo &info = left_->get_spec().output_.at(i)->get_eval_info(eval_ctx_);
    info.evaluated_ = true;
    info.projected_ = true;
    info.notnull_ = false;
    info.point_to_frame_ = false;
  }
  for (int64_t i = 0; i < right_->get_spec().output_.count(); i++) {
    ObEvalInfo &info = right_->get_spec().output_.at(i)->get_eval_info(eval_ctx_);
    info.evaluated_ = true;
    info.projected_ = true;
    info.notnull_ = false;
    info.point_to_frame_ = false;
  }
}

int ObHashJoinOp::outer_join_read_hashrow_end_batch()
{
  int ret = OB_SUCCESS;
  if (1 == right_batch_traverse_cnt_ && RECURSIVE == hj_processor_) {
    // dump rows, left outer join may reach here
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    for (int64_t i = 0; OB_SUCC(ret) && i < right_brs_->size_; i++) {
      if (right_brs_->skip_->exist(i)) {
        continue;
      }
      const int64_t part_idx = get_part_idx(right_hash_vals_[i]);
      if (check_right_need_dump(part_idx)) {
        guard.set_batch_idx(i);
        OZ(dump_right_row_batch_one(part_idx, i));
      }
    }
  }
  state_ = JS_READ_RIGHT;
  return ret;
}

int ObHashJoinOp::other_join_read_hashrow_func_going()
{
  int ret = OB_SUCCESS;
  if (LEFT_SEMI_JOIN == MY_SPEC.join_type_
      || LEFT_ANTI_JOIN == MY_SPEC.join_type_) {
    //do nothing
  } else if (RIGHT_SEMI_JOIN == MY_SPEC.join_type_) {
    // mark this row is match, and return already
    // 由于在匹配情况下，一定会先将left和right的expr填好再计算，所以这种情况下，其实不用再填
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

int ObHashJoinOp::other_join_read_hashrow_func_end()
{
  int ret = OB_SUCCESS;
  const int64_t part_idx = get_part_idx(cur_right_hash_value_);
  if (RECURSIVE == hj_processor_ && check_right_need_dump(part_idx)) {
    if (right_has_matched_ && RIGHT_SEMI_JOIN == MY_SPEC.join_type_) {
      // do nothing, if is right semi join and matched.
      // ret = OB_ERR_UNEXPECTED;
      // LOG_WARN("right semi already return row, so it can't be here", K(ret));
    } else {
      ObHashJoinStoredJoinRow *stored_row = nullptr;
      if (nullptr != right_read_row_) {
        if (OB_FAIL(right_hj_part_array_[part_idx].add_row(
            right_read_row_, stored_row))) {
          LOG_WARN("fail to add row", K(ret));
        }
      } else {
        if (OB_FAIL(only_join_right_row())) {
          LOG_WARN("failed to join right row", K(ret));
        } else if (OB_FAIL(right_hj_part_array_[part_idx].add_row(
            right_->get_spec().output_, &eval_ctx_, stored_row))) {
          LOG_WARN("fail to add row", K(ret));
        }
      }
      if (nullptr != stored_row) {
        stored_row->set_is_match(right_has_matched_);
        stored_row->set_hash_value(cur_right_hash_value_);
      }
    }
  } else if (!is_last_chunk_) {
    // not the last chunk rows
    if (has_right_bitset_ && right_has_matched_) {
      right_bit_set_.add_member(nth_right_row_);
    }
  } else if (!right_has_matched_) {
    if (need_right_join()) {
      if (OB_FAIL(join_rows_with_left_null())) { // right outer join, null-rightrow
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

int ObHashJoinOp::inner_join_read_hashrow_going_batch()
{
  int ret = OB_SUCCESS;
  brs_.size_ = right_brs_->size_;
  brs_.skip_->set_all(brs_.size_);
  int64_t idx = 0;
  for (int64_t i = 0; i < right_selector_cnt_; i++) {
    brs_.skip_->unset(right_selector_[i]);
    auto tuple = cur_tuples_[i]->get_next();
    if (NULL != tuple) {
      cur_tuples_[idx] = tuple;
      right_selector_[idx++] = right_selector_[i];
    }
  }
  right_selector_cnt_ = idx;
  set_output_eval_info();
  mark_return();

  return ret;
}

int ObHashJoinOp::inner_join_read_hashrow_func_going()
{
  int ret = OB_SUCCESS;
  if (!opt_cache_aware_) {
    const int64_t part_idx = get_part_idx(cur_right_hash_value_);
    if (nullptr == cur_tuple_->get_next() && (RECURSIVE != hj_processor_ ||
        (max_partition_count_per_level_ != cur_dumped_partition_ && cur_dumped_partition_ >= part_idx))) {
      // inner join, it don't need to process right match
      // if hash link has no other tuple, then read right after return data
      state_ = JS_READ_RIGHT;
      cur_tuple_ = nullptr;
    }
  }
  mark_return();
  return ret;
}

// when read hashrow end, need dump rows which in dumped partition
int ObHashJoinOp::inner_join_read_hashrow_end_batch()
{
  int ret = OB_SUCCESS;
  if (RECURSIVE == hj_processor_) {
    ObEvalCtx::BatchInfoScopeGuard guard(eval_ctx_);
    for (int64_t i = 0; OB_SUCC(ret) && i < right_brs_->size_; i++) {
      if (right_brs_->skip_->exist(i)) {
        continue;
      }
      int64_t part_idx = get_part_idx(right_hash_vals_[i]);
      if (check_right_need_dump(part_idx)) {
        ObHashJoinStoredJoinRow *stored_row = nullptr;
        if (right_read_from_stored_) {
          if (OB_FAIL(right_hj_part_array_[part_idx].add_row(
              right_hj_part_stored_rows_[i], stored_row))) {
            LOG_WARN("fail to add row", K(ret));
          }
        } else {
          guard.set_batch_idx(i);
          if (OB_FAIL(right_hj_part_array_[part_idx].add_row(
              right_->get_spec().output_, &eval_ctx_, stored_row))) {
            LOG_WARN("fail to add row", K(ret));
          }
        }
        if (nullptr != stored_row) {
          stored_row->set_hash_value(right_hash_vals_[i]);
        }
      }
    }
  }
  state_ = JS_READ_RIGHT;

  return ret;
}

int ObHashJoinOp::inner_join_read_hashrow_func_end()
{
  int ret = OB_SUCCESS;
  const int64_t part_idx = get_part_idx(cur_right_hash_value_);
  if (opt_cache_aware_) {
  } else if (RECURSIVE == hj_processor_ && check_right_need_dump(part_idx)) {
    // for single thread, if left parti is not dumped, then don't dump right
    // but shared hash join, the same left part of other thread  may has data, so we need dump
    ObHashJoinStoredJoinRow *stored_row = nullptr;
    if (nullptr != right_read_row_) {
      if (OB_FAIL(right_hj_part_array_[part_idx].add_row(
          right_read_row_, stored_row))) {
        LOG_WARN("fail to add row", K(ret));
      }
    } else {
      if (OB_FAIL(only_join_right_row())) {
        LOG_WARN("failed to join right row", K(ret));
      } else if (OB_FAIL(right_hj_part_array_[part_idx].add_row(
          right_->get_spec().output_, &eval_ctx_, stored_row))) {
        LOG_WARN("fail to add row", K(ret));
      }
    }
    if (nullptr != stored_row) {
      stored_row->set_is_match(right_has_matched_);
      stored_row->set_hash_value(cur_right_hash_value_);
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

int ObHashJoinOp::find_next_matched_tuple(ObHashJoinStoredJoinRow *&tuple)
{
  int ret = OB_SUCCESS;
  PartHashJoinTable &htable = hash_table_;
  while (OB_SUCC(ret)) {
    if (NULL != tuple) {
      if (!tuple->is_match()) {
        tuple = tuple->get_next();
      } else {
        break;
      }
    } else {
      int64_t bucket_id = cur_bkid_ + 1;
      if (bucket_id < htable.nbuckets_) {
        tuple = htable.buckets_->at(bucket_id).get_stored_row();
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
  // 遍历hash table,
  // 对于anti join, 输出没有匹配的行
  // 对于semi join, 输出有被匹配过的行
  if (is_vectorized()) {
    ret = fill_left_join_result_batch();
  } else if (LEFT_ANTI_JOIN == MY_SPEC.join_type_) {
    ret = find_next_unmatched_tuple(cur_tuple_);
  } else if (LEFT_SEMI_JOIN == MY_SPEC.join_type_) {
    ret = find_next_matched_tuple(cur_tuple_);
  }
  return ret;
}

int ObHashJoinOp::left_anti_semi_going()
{
  int ret = OB_SUCCESS;
  if (is_vectorized()) {
    mark_return();
  } else {
    ObHashJoinStoredJoinRow *tuple = cur_tuple_;
    if (LEFT_ANTI_JOIN == MY_SPEC.join_type_
        && (NULL != tuple && !tuple->is_match())) {
      if (OB_FAIL(convert_exprs(tuple, left_->get_spec().output_, has_fill_left_row_))) {
        LOG_WARN("convert tuple failed", K(ret));
      } else {
        tuple->set_is_match(true);
        mark_return();
      }
    } else if (LEFT_SEMI_JOIN == MY_SPEC.join_type_
               && (NULL != tuple && tuple->is_match())) {
      if (OB_FAIL(convert_exprs(tuple, left_->get_spec().output_, has_fill_left_row_))) {
        LOG_WARN("convert tuple failed", K(ret));
      } else {
        tuple->set_is_match(false);
        mark_return();
      }
    }
  }
  return ret;
}

int ObHashJoinOp::left_anti_naaj_going()
{
  int ret = OB_SUCCESS;
  bool is_left = true;
  bool is_null = false;
  if (is_vectorized()) {
    mark_return();
  } else {
    ObHashJoinStoredJoinRow *tuple = cur_tuple_;
    if (NULL != tuple && !tuple->is_match()) {
      if (OB_FAIL(convert_exprs(tuple, left_->get_spec().output_, has_fill_left_row_))) {
        LOG_WARN("convert tuple failed", K(ret));
      } else if (!non_preserved_side_is_not_empty_) {
        tuple->set_is_match(true);
        mark_return();
      } else if (OB_FAIL(check_join_key_for_naaj(is_left, is_null))) {
        LOG_WARN("failed to check left join key for naaj", K(ret));
      } else if (!is_null) {
        tuple->set_is_match(true);
        mark_return();
      } else {
        tuple->set_is_match(true);
      }
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

int ObHashJoinOp::find_next_unmatched_tuple(ObHashJoinStoredJoinRow *&tuple)
{
  int ret = OB_SUCCESS;
  PartHashJoinTable &htable = hash_table_;
  while (OB_SUCC(ret)) {
    if (NULL != tuple) {
      if (tuple->is_match()) {
        tuple = tuple->get_next();
      } else {
        break;
      }
    } else {
      int64_t bucket_id = cur_bkid_ + 1;
      if (bucket_id < htable.nbuckets_) {
        tuple = htable.buckets_->at(bucket_id).get_stored_row();
        cur_bkid_ = bucket_id;
      } else {
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

int ObHashJoinOp::fill_left_join_result_batch()
{
  int ret = OB_SUCCESS;
  PartHashJoinTable &htable = hash_table_;
  ObHashJoinStoredJoinRow *tuple = cur_tuple_;
  int64_t batch_idx = 0;
  if (brs_.size_ > 0) {
    brs_.skip_->reset(brs_.size_);
  }
  const ObHashJoinStoredJoinRow **left_result_rows = hj_part_stored_rows_;
  while (OB_SUCC(ret) && batch_idx < max_output_cnt_) {
    if (NULL != tuple) {
      if ((LEFT_ANTI_JOIN == MY_SPEC.join_type_ && !tuple->is_match())
         || (need_left_join() && !tuple->is_match()))  {
        left_result_rows[batch_idx] = tuple;
        batch_idx++;
      }
      tuple = tuple->get_next();
    } else {
      int64_t bucket_id = cur_bkid_ + 1;
      if (bucket_id < htable.nbuckets_) {
        tuple = htable.buckets_->at(bucket_id).get_stored_row();
        cur_bkid_ = bucket_id;
      } else {
        ret = OB_ITER_END;
      }
    }
  }
  cur_tuple_ = tuple;
  if (OB_ITER_END == ret && 0 != batch_idx) {
    ret = OB_SUCCESS;
  }
  if (OB_SUCCESS == ret) {
    ObChunkDatumStore::Iterator::attach_rows(left_->get_spec().output_, eval_ctx_,
                   reinterpret_cast<const ObChunkDatumStore::StoredRow **>(left_result_rows),
                   batch_idx);
    if (need_left_join()) {
      blank_row_batch(right_->get_spec().output_, batch_idx);
    } else if (MY_SPEC.is_naaj_) {
      if (OB_FAIL(check_join_key_for_naaj_batch_output(batch_idx))) {
        LOG_WARN("failed to check join key", K(ret));
      }
    }
  }

  brs_.size_ = batch_idx;

  return ret;
}

int ObHashJoinOp::fill_left_operate()
{
  int ret = OB_SUCCESS;
  if (is_vectorized()) {
    ret = fill_left_join_result_batch();
  } else {
    ret = find_next_unmatched_tuple(cur_tuple_);
  }
  return ret;
}

int ObHashJoinOp::convert_exprs(
  const ObHashJoinStoredJoinRow *store_row, const ObIArray<ObExpr*> &exprs, bool &has_fill)
{
  int ret = OB_SUCCESS;
  has_fill = true;
  if (OB_ISNULL(store_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("store row is null", K(ret));
  } else if (OB_FAIL(store_row->to_expr(exprs, eval_ctx_))) {
    LOG_WARN("failed to project", K(ret));
  }
  return ret;
}

int ObHashJoinOp::convert_exprs_batch_one(const ObHashJoinStoredJoinRow *store_row,
                                          const ObIArray<ObExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("store row is null", K(ret));
  } else if (OB_FAIL(store_row->to_expr(exprs, eval_ctx_))) {
    LOG_WARN("failed to project", K(ret));
  }
  return ret;
}

int ObHashJoinOp::fill_left_going()
{
  int ret = OB_SUCCESS;
  if (is_vectorized()) {
    mark_return();
  } else {
    ObHashJoinStoredJoinRow *tuple = cur_tuple_;
    if (NULL != tuple && !tuple->is_match()) {
      if (OB_FAIL(convert_exprs(tuple, left_->get_spec().output_, has_fill_left_row_))) {
        LOG_WARN("convert tuple failed", K(ret));
      } else if (OB_FAIL(join_rows_with_right_null())) {
        LOG_WARN("left join rows failed", K(ret));
      } else {
        mark_return();
        tuple->set_is_match(true);
      }
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

int ObHashJoinOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  max_output_cnt_ = min(max_row_cnt, MY_SPEC.max_batch_size_);
  return inner_get_next_row();
}

int ObHashJoinOp::read_hashrow_batch_for_left_semi_anti()
{
  int ret = OB_SUCCESS;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
  right_batch_traverse_cnt_++;
  probe_cnt_ +=  right_selector_cnt_;
  if (1 == right_batch_traverse_cnt_) {
    // check bloom filter
    if (enable_bloom_filter_) {
      int64_t idx = 0;
      for (int64_t i = 0; i < right_selector_cnt_; i++) {
        if (bloom_filter_->exist(right_hash_vals_[right_selector_[i]])) {
          right_selector_[idx++] = right_selector_[i];
        }
      }
      right_selector_cnt_ = idx;
    }

    // will not match hash table if partition idx greater than cur_dumped_partition_
    if (max_partition_count_per_level_ != cur_dumped_partition_) {
      int64_t idx = 0;
      for (int64_t i = 0; i < right_selector_cnt_; i++) {
        if (get_part_idx(right_hash_vals_[right_selector_[i]]) <= cur_dumped_partition_) {
          right_selector_[idx++] = right_selector_[i];
        }
      }
      right_selector_cnt_ = idx;
    }

    // probe hash table
    {
      // group prefetch
      for (int64_t i = 0; i < right_selector_cnt_; i++) {
        uint64_t mask = hash_table_.nbuckets_ - 1;
        __builtin_prefetch(&hash_table_.buckets_->at(mask & right_hash_vals_[right_selector_[i]]),
                           0, // for read
                           1); // low temporal locality
      }
    }
    // convert right rows from stored row
    if (right_read_from_stored_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < right_selector_cnt_; i++) {
        batch_info_guard.set_batch_idx(right_selector_[i]);
        OZ (convert_exprs_batch_one(right_hj_part_stored_rows_[right_selector_[i]],
                                right_->get_spec().output_));
      }
    }
  }

  int64_t idx = 0;
  ObHashJoinStoredJoinRow *tuple = NULL;
  int64_t result_idx = 0;
  const ObHashJoinStoredJoinRow **left_result_rows = hj_part_stored_rows_;
  for (int64_t i = 0; OB_SUCC(ret) && i < right_selector_cnt_; i++) {
    HTBucket *bkt = nullptr;
    hash_table_.get(right_hash_vals_[right_selector_[i]], bkt);
    if (NULL != bkt) {
      tuple = bkt->get_stored_row();
    }
    if (NULL != tuple) {
      cur_tuples_[idx] = tuple;
      right_selector_[idx++] = right_selector_[i];

      batch_info_guard.set_batch_size(right_brs_->size_);
      int64_t batch_idx = right_selector_[i];
      batch_info_guard.set_batch_idx(batch_idx);
      bool matched = false;
      ObHashJoinStoredJoinRow *pre = nullptr;
      while(!matched && NULL != tuple && OB_SUCC(ret)) {
        ++hash_link_cnt_;
        ++hash_equal_cnt_;
        clear_datum_eval_flag();
        if (OB_FAIL(convert_exprs_batch_one(tuple, left_->get_spec().output_))) {
          LOG_WARN("failed to convert expr", K(ret));
        } else if (OB_FAIL(calc_equal_conds(matched))) {
          LOG_WARN("calc equal conditions failed", K(ret));
        } else if (matched && OB_FAIL(calc_other_conds(matched))) {
          LOG_WARN("calc other conditions failed", K(ret));
        }
        if (!matched) {
          pre = tuple;
          tuple = tuple->get_next();
        }
      }
      if (matched) {
        LOG_DEBUG("trace match", K(ret), K(batch_idx),
                    K(ROWEXPR2STR(eval_ctx_, MY_SPEC.all_join_keys_)));
        //for semi join, we delete tuple and not use
        //for anti join, we delete tuple and use it to return
        if (LEFT_SEMI_JOIN == MY_SPEC.join_type_) {
          cur_tuples_[result_idx] = tuple->get_next();
          right_selector_[result_idx] = right_selector_[i];
          left_result_rows[result_idx++] = tuple;
        } else {
          cur_tuples_[result_idx] = tuple;
          right_selector_[result_idx++] = right_selector_[i];
        }
        if (NULL== pre) {
          bkt->set_stored_row(tuple->get_next());
        } else {
          pre->set_next(tuple->get_next());
        }
        hash_table_.row_count_ -= 1;
      }
    }
  }
  if (OB_SUCC(ret) && LEFT_SEMI_JOIN == MY_SPEC.join_type_ && result_idx > 0) {
    if (brs_.size_ > 0) {
      brs_.skip_->reset(brs_.size_);
    }
    ObChunkDatumStore::Iterator::attach_rows(left_->get_spec().output_, eval_ctx_,
                  reinterpret_cast<const ObChunkDatumStore::StoredRow **>(left_result_rows),
                  result_idx);
    brs_.size_ = result_idx;
    mark_return();
  }
  right_selector_cnt_ = result_idx;
  if (OB_SUCC(ret) && 0 == result_idx) {
    ret = OB_ITER_END;
  }
  return ret;
}

int ObHashJoinOp::check_join_key_for_naaj(const bool is_left, bool &is_null)
{
  int ret = OB_SUCCESS;
  ObHashJoinInput *hj_input = static_cast<ObHashJoinInput*>(input_);
  clear_evaluated_flag();
  common::ObArrayHelper<ObExpr*> &curr_join_keys = is_left ? left_join_keys_ : right_join_keys_;
  CK (1 == curr_join_keys.count());
  CK (!is_null);
  ObDatum *key_datum = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(hj_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: shared hash join info is null", K(ret));
  } else if (is_shared_ && FALSE_IT(is_null = hj_input->get_null_in_naaj())) {
  } else if (is_null) {
  } else if (OB_FAIL(curr_join_keys.at(0)->eval(eval_ctx_, key_datum))) {
    LOG_WARN("failed to eval curr join key", K(ret));
  } else {
    is_null = key_datum->is_null();
  }
  return ret;
}

int ObHashJoinOp::check_join_key_for_naaj_batch(const bool is_left,
                                                const int64_t batch_size,
                                                bool &has_null,
                                                const ObBatchRows *child_brs)
{
  int ret = OB_SUCCESS;
  ObHashJoinInput *hj_input = static_cast<ObHashJoinInput*>(input_);
  clear_evaluated_flag();
  common::ObArrayHelper<ObExpr*> &curr_join_keys = is_left ? left_join_keys_ : right_join_keys_;
  CK (1 == curr_join_keys.count());
  CK (!has_null);
  CK (nullptr != child_brs);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(hj_input)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: shared hash join info is null", K(ret));
  } else if (is_shared_ && FALSE_IT(has_null = hj_input->get_null_in_naaj())) {
  } else if (has_null) {
  } else if (OB_FAIL(curr_join_keys.at(0)->eval_batch(eval_ctx_, *child_brs->skip_, batch_size))) {
    LOG_WARN("failed to eval batch curr join key", K(ret));
  } else {
    ObDatumVector key_datums = curr_join_keys.at(0)->locate_expr_datumvector(eval_ctx_);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (child_brs->skip_->at(i)) {
        continue;
      }
      if (key_datums.at(i)->is_null()) {
        if ((is_left && is_right_naaj_na()) || (!is_left && is_left_naaj_na())) {
          has_null = true;
          break;
        } else if ((is_left && is_right_naaj_sna()) || (!is_left && is_left_naaj_sna())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("get null value in non preserved side, may generated wrong plan",
                    K(ret), K(MY_SPEC.join_type_), K(MY_SPEC.is_naaj_), K(MY_SPEC.is_sna_));
        }
      }
    }
  }
  return ret;
}

int ObHashJoinOp::check_join_key_for_naaj_batch_output(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  clear_evaluated_flag();
  if (!non_preserved_side_is_not_empty_) {
    // do nothing
  } else if (OB_FAIL(left_join_keys_.at(0)->eval_batch(eval_ctx_, *brs_.skip_, batch_size))) {
    LOG_WARN("failed to eval join key for naaj", K(ret));
  } else {
    ObDatumVector key_datums = left_join_keys_.at(0)->locate_expr_datumvector(eval_ctx_);
    for (int64_t i = 0; i < batch_size; ++i) {
      if (key_datums.at(i)->is_null()) {
        brs_.skip_->set(i);
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
