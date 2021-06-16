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
#include "sql/engine/aggregate/ob_hash_groupby.h"
#include "share/ob_define.h"
#include "lib/utility/serialization.h"
#include "lib/list/ob_list.h"
#include "common/row/ob_row_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/aggregate/ob_exec_hash_struct.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/aggregate/ob_aggregate_function.h"
#include "sql/engine/expr/ob_sql_expression.h"
#include "lib/list/ob_dlink_node.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "lib/cpu/ob_endian.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/px/ob_px_util.h"

namespace oceanbase {
using namespace common;
using namespace common::hash;
namespace sql {

struct ObGbyPartition : public common::ObDLinkBase<ObGbyPartition> {
public:
  ObGbyPartition(common::ObIAllocator* alloc = nullptr) : row_store_(alloc), level_(0)
  {}
  ObChunkRowStore row_store_;
  int64_t level_;
};

class ObHashGroupBy::ObHashGroupByCtx : public ObGroupByCtx, public ObHashCtx<ObGbyHashCols> {
public:
  explicit ObHashGroupByCtx(ObExecContext& exec_ctx)
      : ObGroupByCtx(exec_ctx),
        ObHashCtx(),
        group_idx_(0),
        mem_context_(NULL),
        agged_group_cnt_(0),
        agged_row_cnt_(0),
        profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
        sql_mem_processor_(profile_)
  {}
  ~ObHashGroupByCtx()
  {}

  void destroy_all_parts()
  {
    if (NULL != mem_context_) {
      while (!all_parts_.is_empty()) {
        ObGbyPartition* p = all_parts_.remove_first();
        if (NULL != p) {
          p->~ObGbyPartition();
          mem_context_->get_malloc_allocator().free(p);
        }
      }
    }
  }

  int init_mem_context()
  {
    int ret = OB_SUCCESS;
    if (NULL == mem_context_) {
      lib::ContextParam param;
      param.set_mem_attr(exec_ctx_.get_my_session()->get_effective_tenant_id(),
          ObModIds::OB_HASH_NODE_GROUP_ROWS,
          ObCtxIds::WORK_AREA);
      if (OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(mem_context_, param))) {
        LOG_WARN("memory entity create failed", K(ret));
      }
    }
    return ret;
  }

  virtual void destroy()
  {
    sql_mem_processor_.destroy();
    if (NULL != mem_context_) {
      destroy_all_parts();
    }
    group_idx_ = 0;
    ObHashCtx::destroy();
    ObGroupByCtx::destroy();
    // last free mem,ObHashCtx depends on mem_context
    if (NULL != mem_context_) {
      DESTROY_CONTEXT(mem_context_);
      mem_context_ = NULL;
    }
  }

  void calc_data_mem_ratio(int64_t part_cnt, double& data_ratio);
  void adjust_part_cnt(int64_t& part_cnt);
  int64_t get_ctx_mem_size()
  {
    return ObHashCtx::get_used_mem_size();
  }
  int64_t get_part_mem_size()
  {
    return (NULL == mem_context_ ? 0 : mem_context_->used());
  }
  int64_t get_part_hold_size()
  {
    return (NULL == mem_context_ ? 0 : mem_context_->hold());
  }
  int64_t get_used_mem_size()
  {
    return aggr_func_.get_used_mem_size() + ObHashCtx::get_used_mem_size() +
           (NULL == mem_context_ ? 0 : mem_context_->used());
  }

  int64_t estimate_hash_bucket_cnt_by_mem_size(int64_t bucket_cnt, int64_t max_mem_size, double extra_ratio);
  int64_t estimate_hash_bucket_size(int64_t bucket_cnt);
  int64_t get_data_hold_size()
  {
    return aggr_func_.get_hold_mem_size();
  }
  int64_t get_data_size()
  {
    return aggr_func_.get_used_mem_size();
  }
  int64_t get_extra_size()
  {
    return ObHashCtx::get_used_mem_size() + (NULL == mem_context_ ? 0 : mem_context_->used());
  }
  int64_t get_mem_bound()
  {
    return sql_mem_processor_.get_mem_bound();
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObHashGroupByCtx);
  friend class ObHashGroupBy;

private:
  static const int64_t FIX_SIZE_PER_PART = sizeof(ObGbyPartition) + ObChunkRowStore::BLOCK_SIZE;
  int64_t group_idx_;

  // memory allocator for group by partitions
  lib::MemoryContext* mem_context_;

  ObDList<ObGbyPartition> all_parts_;

  int64_t agged_group_cnt_;
  int64_t agged_row_cnt_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
};

// Calculate data and extra memory ratio
// Here, the memory occupied by the chunk row store that needs to be dumped + ctx (hash table) memory is used as extra
// memory and the memory involved in the aggregate function is data memory Calculate the ratio of the two here, and then
// determine how much data can be stored according to the mem bound If you directly use data_size/(extra_size +
// data_size), the extra is very large, but the data is very small, which will lead to inserting a few rows of data to
// estimate the data_ratio is very small, there is a problem
void ObHashGroupBy::ObHashGroupByCtx::calc_data_mem_ratio(int64_t part_cnt, double& data_ratio)
{
  int64_t extra_size = (get_ctx_mem_size() + part_cnt * FIX_SIZE_PER_PART) * (1 + EXTRA_MEM_RATIO);
  int64_t data_size = max(get_data_size(), (get_mem_bound() - extra_size) * 0.8);
  data_ratio = data_size * 1.0 / (extra_size + data_size);
  sql_mem_processor_.set_data_ratio(data_ratio);
  LOG_TRACE("trace calc data ratio", K(data_ratio), K(extra_size), K(part_cnt), K(data_size), K(get_data_size()));
}

void ObHashGroupBy::ObHashGroupByCtx::adjust_part_cnt(int64_t& part_cnt)
{
  int64_t mem_used = get_used_mem_size();
  int64_t mem_bound = get_mem_bound();
  int64_t dumped_remain_mem_size = mem_bound - mem_used;
  int64_t max_part_cnt = dumped_remain_mem_size / FIX_SIZE_PER_PART;
  if (max_part_cnt <= MIN_PARTITION_CNT) {
    part_cnt = MIN_PARTITION_CNT;
  } else {
    while (part_cnt > max_part_cnt) {
      part_cnt >>= 1;
    }
  }
  LOG_TRACE(
      "trace adjust part cnt", K(part_cnt), K(max_part_cnt), K(dumped_remain_mem_size), K(mem_bound), K(mem_used));
}

int64_t ObHashGroupBy::ObHashGroupByCtx::estimate_hash_bucket_size(int64_t bucket_cnt)
{
  return next_pow2(ObExtendHashTable<ObGbyHashCols>::SIZE_BUCKET_SCALE * bucket_cnt) * sizeof(void*);
}

int64_t ObHashGroupBy::ObHashGroupByCtx::estimate_hash_bucket_cnt_by_mem_size(
    int64_t bucket_cnt, int64_t max_mem_size, double extra_ratio)
{
  int64_t mem_size = estimate_hash_bucket_size(bucket_cnt);
  int64_t max_hash_size = max_mem_size * extra_ratio;
  if (0 < max_hash_size) {
    while (mem_size > max_hash_size) {
      mem_size >>= 1;
    }
  }
  return mem_size / sizeof(void*) / ObExtendHashTable<ObGbyHashCols>::SIZE_BUCKET_SCALE;
}
ObHashGroupBy::ObHashGroupBy(ObIAllocator& alloc) : ObGroupBy(alloc)
{}

ObHashGroupBy::~ObHashGroupBy()
{}

void ObHashGroupBy::reset()
{
  ObGroupBy::reset();
  distinct_set_bucket_num_ = DISTINCT_SET_NUM;
  prepare_row_num_ = PREPARE_ROW_NUM;
}

void ObHashGroupBy::reuse()
{
  ObGroupBy::reuse();
  distinct_set_bucket_num_ = DISTINCT_SET_NUM;
  prepare_row_num_ = PREPARE_ROW_NUM;
}

int ObHashGroupBy::get_hash_groupby_row_count(ObExecContext& exec_ctx, int64_t& hash_groupby_row_count) const
{
  int ret = OB_SUCCESS;
  ObHashGroupByCtx* hash_groupby_ctx = NULL;
  if (OB_ISNULL(hash_groupby_ctx = GET_PHY_OPERATOR_CTX(ObHashGroupByCtx, exec_ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "get hash_groupby ctx failed", K(ret));
  } else {
    hash_groupby_row_count = hash_groupby_ctx->group_rows_.size();
  }
  return ret;
}

int ObHashGroupBy::inner_create_operator_ctx(ObExecContext& ctx, ObPhyOperatorCtx*& op_ctx) const
{
  return CREATE_PHY_OPERATOR_CTX(ObHashGroupByCtx, ctx, get_id(), get_type(), op_ctx);
}

int64_t ObHashGroupBy::to_string_kv(char* buf, const int64_t buf_len) const
{
  return ObGroupBy::to_string_kv(buf, buf_len);
}

int ObHashGroupBy::inner_open(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObGroupBy::init_group_by(ctx))) {
    LOG_WARN("failed to init hash group by", K(ret));
  }
  return ret;
}

int ObHashGroupBy::rescan(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObHashGroupByCtx* groupby_ctx = NULL;
  if (OB_ISNULL(groupby_ctx = GET_PHY_OPERATOR_CTX(ObHashGroupByCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get operator context failed");
  } else {
    groupby_ctx->sql_mem_processor_.reset();
    groupby_ctx->started_ = false;
    // Why is reuse not reset or destroy here?
    // This is because group_rows_ will not be init again after rescan, only resize
    groupby_ctx->group_rows_.reuse();
    groupby_ctx->aggr_func_.reuse();
    groupby_ctx->destroy_all_parts();
    if (OB_FAIL(ObGroupBy::rescan(ctx))) {
      LOG_WARN("rescan child operator failed", K(ret));
    }
  }
  return ret;
}

inline bool ObHashGroupBy::need_start_dump(
    ObHashGroupByCtx& gby_ctx, const int64_t input_rows, int64_t& est_part_cnt, bool check_dump) const
{
  bool dump = false;
  const int64_t mem_used = gby_ctx.get_used_mem_size();
  const int64_t mem_bound = gby_ctx.get_mem_bound();
  double data_ratio = gby_ctx.sql_mem_processor_.get_data_ratio();
  if (OB_UNLIKELY(0 == est_part_cnt)) {
    est_part_cnt = detect_part_cnt(input_rows, gby_ctx);
    gby_ctx.calc_data_mem_ratio(est_part_cnt, data_ratio);
  }
  // We continue do aggregation after we start dumping, reserve 1/8 memory for it.
  if (gby_ctx.get_data_size() > data_ratio * mem_bound || check_dump) {
    int ret = OB_SUCCESS;
    dump = true;
    // When you think a dump is about to happen,
    // try to expand to get more memory, then decide whether to dump
    if (OB_FAIL(gby_ctx.sql_mem_processor_.extend_max_memory_size(
            &gby_ctx.exec_ctx_.get_allocator(),
            [&](int64_t max_memory_size) { return gby_ctx.get_data_size() > data_ratio * max_memory_size; },
            dump,
            mem_used))) {
      dump = true;
      LOG_WARN("failed to extend max memory size", K(ret), K(dump));
    } else if (OB_FAIL(gby_ctx.sql_mem_processor_.update_used_mem_size(mem_used))) {
      LOG_WARN("failed to update used memory size", K(ret), K(mem_used), K(mem_bound), K(dump));
    } else {
      est_part_cnt = detect_part_cnt(input_rows, gby_ctx);
      gby_ctx.calc_data_mem_ratio(est_part_cnt, data_ratio);
      LOG_TRACE("trace extend max memory size",
          K(ret),
          K(data_ratio),
          K(gby_ctx.get_data_size()),
          K(gby_ctx.get_used_mem_size()),
          K(gby_ctx.get_extra_size()),
          K(mem_used),
          K(mem_bound),
          K(dump),
          K(est_part_cnt));
    }
  }
  return dump;
}

int ObHashGroupBy::update_mem_status_periodically(
    ObHashGroupByCtx* gby_ctx, int64_t nth_cnt, const int64_t input_row, int64_t& est_part_cnt, bool& need_dump) const
{
  int ret = OB_SUCCESS;
  bool updated = false;
  need_dump = false;
  if (OB_FAIL(gby_ctx->sql_mem_processor_.update_max_available_mem_size_periodically(
          &gby_ctx->exec_ctx_.get_allocator(), [&](int64_t cur_cnt) { return nth_cnt > cur_cnt; }, updated))) {
    LOG_WARN("failed to update usable memory size periodically", K(ret));
  } else if (updated) {
    if (OB_FAIL(gby_ctx->sql_mem_processor_.update_used_mem_size(gby_ctx->get_used_mem_size()))) {
      LOG_WARN("failed to update used memory size", K(ret));
    } else {
      double data_ratio = gby_ctx->sql_mem_processor_.get_data_ratio();
      ;
      est_part_cnt = detect_part_cnt(input_row, *gby_ctx);
      gby_ctx->calc_data_mem_ratio(est_part_cnt, data_ratio);
      need_dump = (gby_ctx->get_data_size() > gby_ctx->get_mem_bound() * data_ratio);
    }
  }
  return ret;
}

int ObHashGroupBy::load_data(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  const ObChunkRowStore::StoredRow* srow = NULL;
  const ObNewRow* row = NULL;
  ObGbyHashCols gby_cols;
  const ObGbyHashCols* exist_gby_cols = NULL;
  ObHashGroupByCtx* gby_ctx = NULL;
  ObGbyPartition* cur_part = NULL;
  ObChunkRowStore::Iterator row_store_iter;
  int64_t level = 0;
  int64_t part_shift = sizeof(uint64_t) * CHAR_BIT / 2;  // low half bits for hash table lookup.
  ObGbyPartition* parts[MAX_PARTITION_CNT];
  int64_t part_cnt = 0;
  int64_t est_part_cnt = 0;
  bool start_dump = false;
  bool check_dump = false;
  bool is_dump_enabled = GCONF.is_sql_operator_dump_enabled();
  int64_t input_rows = child_op_->get_rows();
  int64_t input_size = child_op_->get_width() * input_rows;
  ObGbyBloomFilter* bloom_filter = NULL;

  static_assert(MAX_PARTITION_CNT <= (1 << (CHAR_BIT)), "max partition cnt is too big");
  if (OB_ISNULL(gby_ctx = GET_PHY_OPERATOR_CTX(ObHashGroupByCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get operator context failed");
  } else {
    if (gby_ctx->aggr_func_.has_distinct() || gby_ctx->aggr_func_.has_sort()) {
      // We use sort based group by for aggregation which need distinct or sort right now,
      // disable operator dump for compatibility.
      is_dump_enabled = false;
    }
    if (!gby_ctx->all_parts_.is_empty()) {
      gby_ctx->aggr_func_.reuse();
      cur_part = gby_ctx->all_parts_.remove_first();
      if (OB_ISNULL(cur_part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pop head partition failed", K(ret));
      } else if (OB_FAIL(row_store_iter.init(&cur_part->row_store_, ObChunkRowStore::BLOCK_SIZE))) {
        LOG_WARN("init chunk row store iterator failed", K(ret));
      } else {
        input_rows = cur_part->row_store_.get_row_cnt();
        level = cur_part->level_;
        part_shift = part_shift + level * CHAR_BIT;
        input_size = cur_part->row_store_.get_file_size();
        if (OB_FAIL(gby_ctx->group_rows_.resize(&gby_ctx->mem_context_->get_malloc_allocator(), max(2, input_rows)))) {
          LOG_WARN("failed to reuse extended hash table", K(ret));
        } else if (OB_FAIL(init_sql_mem_mgr(gby_ctx, input_size))) {
          LOG_WARN("failed to init sql mem manager", K(ret));
        }
      }
      LOG_TRACE("scan new partition",
          K(level),
          K(input_rows),
          K(input_size),
          K(gby_ctx->group_rows_.size()),
          K(gby_ctx->get_used_mem_size()),
          K(gby_ctx->aggr_func_.get_used_mem_size()),
          K(gby_ctx->get_mem_bound()));
    }
  }
  int64_t nth_cnt = 0;
  for (int64_t loop_cnt = 1; OB_SUCC(ret); ++loop_cnt, ++nth_cnt) {
    if (NULL == cur_part) {
      ret = child_op_->get_next_row(ctx, row);
    } else {
      if (OB_FAIL(row_store_iter.get_next_row(srow))) {
      } else if (NULL == srow) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("return NULL store row", K(ret));
      } else if (OB_FAIL(row_store_iter.convert_to_row(srow, *const_cast<ObNewRow**>(&row)))) {
        LOG_WARN("convert store row to new row failed", K(ret));
      }
    }
    if (OB_SUCCESS != ret || OB_ISNULL(row)) {
      if (OB_SUCCESS == ret) {
        ret = OB_ERR_UNEXPECTED;
      }
      if (OB_ITER_END != ret) {
        LOG_WARN("get input row failed", K(ret), KP(row));
      }
      break;
    }
    if (loop_cnt % 1024 == 0) {
      if (OB_FAIL(ctx.check_status())) {
        LOG_WARN("check status failed", K(ret));
      }
    }

    ObSQLSessionInfo* my_session = gby_ctx->exec_ctx_.get_my_session();
    const ObTimeZoneInfo* tz_info = (NULL != my_session) ? my_session->get_timezone_info() : NULL;
    if (OB_FAIL(ret)) {
    } else if (!start_dump &&
               OB_FAIL(update_mem_status_periodically(gby_ctx, nth_cnt, input_rows, est_part_cnt, check_dump))) {
      LOG_WARN("failed to update usable memory size periodically", K(ret));
    } else if (OB_FAIL(gby_cols.init(
                   row, &group_col_idxs_, NULL == srow ? 0 : *static_cast<uint64_t*>(srow->get_extra_payload())))) {
      LOG_WARN("init group by hash columns failed", K(ret));
    } else if ((NULL == bloom_filter || bloom_filter->exist(gby_cols.hash())) &&
               NULL != (exist_gby_cols = gby_ctx->group_rows_.get(gby_cols))) {
      gby_ctx->agged_row_cnt_ += 1;
      if (OB_FAIL(gby_ctx->aggr_func_.process(*row, tz_info, exist_gby_cols->group_id_))) {
        LOG_WARN("fail to process row", K(ret), K(*row), K(exist_gby_cols->group_id_));
      }
    } else {
      if (!is_dump_enabled || gby_ctx->group_rows_.size() < MIN_INMEM_GROUPS ||
          (!start_dump && !need_start_dump(*gby_ctx, input_rows, est_part_cnt, check_dump))) {
        gby_ctx->agged_row_cnt_ += 1;
        gby_ctx->agged_group_cnt_ += 1;
        ObGbyHashCols* store_gby_cols =
            static_cast<ObGbyHashCols*>(gby_ctx->aggr_func_.get_stored_row_buf().alloc(sizeof(ObGbyHashCols)));
        if (OB_ISNULL(store_gby_cols)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else {
          *store_gby_cols = gby_cols;
          store_gby_cols->group_id_ = gby_ctx->group_rows_.size();
          ObRowStore::StoredRow* output_row = NULL;
          if (OB_FAIL(gby_ctx->aggr_func_.prepare(*row, store_gby_cols->group_id_, &output_row))) {
            LOG_WARN("fail to prepare row", K(ret), K(*row), K(store_gby_cols->group_id_));
          } else {
            store_gby_cols->set_stored_row(output_row);
            if (OB_FAIL(gby_ctx->group_rows_.set(*store_gby_cols))) {
              LOG_WARN("hash table set failed", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
          }
        }
      } else {
        if (OB_UNLIKELY(!start_dump)) {
          if (OB_FAIL(setup_dump_env(level, max(input_rows, nth_cnt), parts, part_cnt, bloom_filter, *gby_ctx))) {
            LOG_WARN("setup dump environment failed", K(ret));
          } else {
            start_dump = true;
            gby_ctx->sql_mem_processor_.set_number_pass(level + 1);
          }
        }
        if (OB_SUCC(ret)) {
          const int64_t part_idx = (gby_cols.hash() >> part_shift) & (part_cnt - 1);
          // TODO : copy store row to new row store directly.
          ObChunkRowStore::StoredRow* stored_row = NULL;
          if (OB_FAIL(parts[part_idx]->row_store_.add_row(*row, &stored_row))) {
            LOG_WARN("add row failed", K(ret), K(part_cnt));
          } else {
            *static_cast<uint64_t*>(stored_row->get_extra_payload()) = gby_cols.hash();
          }
        }
      }
    }
  }

  // must be reset first, otherwise the memory will be released when the automatic destruction occurs,
  // and there will be problems
  row_store_iter.reset();
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }

  if (NULL != gby_ctx) {
    // cleanup_dump_env() must be called whether success or not
    int tmp_ret = cleanup_dump_env(common::OB_SUCCESS == ret, level, parts, part_cnt, bloom_filter, *gby_ctx);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("cleanup dump environment failed", K(ret));
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }

    if (NULL != gby_ctx->mem_context_ && NULL != cur_part) {
      cur_part->~ObGbyPartition();
      gby_ctx->mem_context_->get_malloc_allocator().free(cur_part);
      cur_part = NULL;
    }
    IGNORE_RETURN gby_ctx->sql_mem_processor_.update_used_mem_size(gby_ctx->get_used_mem_size());
  }

  return ret;
}

int64_t ObHashGroupBy::detect_part_cnt(const int64_t rows, ObHashGroupByCtx& gby_ctx) const
{
  const double group_mem_avg = (double)gby_ctx.get_data_size() / gby_ctx.group_rows_.size();
  int64_t data_size = rows * ((double)gby_ctx.agged_group_cnt_ / gby_ctx.agged_row_cnt_) * group_mem_avg;
  int64_t mem_bound = gby_ctx.get_mem_bound();
  const double part_skew_factor = 1.2;
  data_size = data_size * part_skew_factor;
  int64_t part_cnt = (data_size + mem_bound) / mem_bound;
  part_cnt = next_pow2(part_cnt);
  // There is only 75% utilization,
  // and then change it to segment array to see if it can be removed
  int64_t availble_mem_size = min(mem_bound - gby_ctx.get_data_hold_size(), mem_bound * MAX_PART_MEM_RATIO);
  int64_t est_dump_size = part_cnt * ObChunkRowStore::BLOCK_SIZE;
  if (0 < availble_mem_size) {
    while (est_dump_size > availble_mem_size) {
      est_dump_size >>= 1;
    }
    part_cnt = est_dump_size / ObChunkRowStore::BLOCK_SIZE;
  } else {
    // Excessive memory usage
    part_cnt = MIN_PARTITION_CNT;
  }
  part_cnt = next_pow2(part_cnt);
  part_cnt = std::max(part_cnt, (int64_t)MIN_PARTITION_CNT);
  part_cnt = std::min(part_cnt, (int64_t)MAX_PARTITION_CNT);
  LOG_TRACE("trace detect partition cnt",
      K(data_size),
      K(group_mem_avg),
      K(gby_ctx.get_used_mem_size()),
      K(gby_ctx.get_mem_bound()),
      K(part_skew_factor),
      K(gby_ctx.agged_group_cnt_),
      K(gby_ctx.agged_row_cnt_),
      K(gby_ctx.group_rows_.size()),
      K(part_cnt),
      K(gby_ctx.get_data_size()),
      K(gby_ctx.get_ctx_mem_size()),
      K(gby_ctx.get_part_mem_size()),
      K(gby_ctx.get_data_hold_size()),
      K(gby_ctx.get_part_hold_size()),
      K(rows),
      K(gby_ctx.mem_context_),
      K(availble_mem_size),
      K(est_dump_size));
  return part_cnt;
}

int ObHashGroupBy::setup_dump_env(const int64_t level, const int64_t input_rows, ObGbyPartition** parts,
    int64_t& part_cnt, ObGbyBloomFilter*& bloom_filter, ObHashGroupByCtx& gby_ctx) const
{
  int ret = OB_SUCCESS;
  if (input_rows < 0 || OB_ISNULL(parts)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(input_rows), K(parts));
  } else {
    int64_t pre_part_cnt = 0;
    part_cnt = pre_part_cnt = detect_part_cnt(input_rows, gby_ctx);
    gby_ctx.adjust_part_cnt(part_cnt);
    MEMSET(parts, 0, sizeof(parts[0]) * part_cnt);
    const int64_t max_level = sizeof(uint64_t) / 2 - 1;  // half for partition half for hash table
    if (level > max_level) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("reach max recursion depth", K(ret), K(level));
    }
    if (OB_SUCC(ret) && NULL == bloom_filter) {
      void* mem = gby_ctx.mem_context_->get_malloc_allocator().alloc(sizeof(ObGbyBloomFilter));
      if (OB_ISNULL(mem)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        ModulePageAllocator mod_alloc(ObModIds::OB_HASH_NODE_GROUP_ROWS,
            gby_ctx.exec_ctx_.get_my_session()->get_effective_tenant_id(),
            ObCtxIds::WORK_AREA);
        mod_alloc.set_allocator(&gby_ctx.mem_context_->get_malloc_allocator());
        bloom_filter = new (mem) ObGbyBloomFilter(mod_alloc);
        if (OB_FAIL(bloom_filter->init(gby_ctx.group_rows_.size()))) {
          LOG_WARN("bloom filter init failed", K(ret));
        } else {
          auto cb = [&](ObGbyHashCols& cols) {
            int ret = OB_SUCCESS;
            if (OB_FAIL(bloom_filter->set(cols.hash()))) {
              LOG_WARN("add hash value to bloom failed", K(ret));
            }
            return ret;
          };
          if (OB_FAIL(gby_ctx.group_rows_.foreach (cb))) {
            LOG_WARN("fill bloom filter failed", K(ret));
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < part_cnt; i++) {
      void* mem = gby_ctx.mem_context_->get_malloc_allocator().alloc(sizeof(ObGbyPartition));
      if (OB_ISNULL(mem)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        parts[i] = new (mem) ObGbyPartition(&gby_ctx.mem_context_->get_malloc_allocator());
        parts[i]->level_ = level + 1;
        const int64_t extra_size = sizeof(uint64_t);  // for hash value
        if (OB_FAIL(parts[i]->row_store_.init(1 /* memory limit, dump immediately */,
                gby_ctx.exec_ctx_.get_my_session()->get_effective_tenant_id(),
                ObCtxIds::WORK_AREA,
                ObModIds::OB_HASH_NODE_GROUP_ROWS,
                true /* enable dump */,
                ObChunkRowStore::FULL,
                extra_size))) {
          LOG_WARN("init chunk row store failed");
        } else {
          parts[i]->row_store_.set_dir_id(gby_ctx.sql_mem_processor_.get_dir_id());
          parts[i]->row_store_.set_callback(&gby_ctx.sql_mem_processor_);
        }
      }
    }
    // If the hash group is dumped, each subsequent partition will allocate a buffer page,about 64K.
    // If there are 32 partitions, 2M memory is required
    // At the same time, the dump logic needs some memory
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(gby_ctx.sql_mem_processor_.get_max_available_mem_size(&gby_ctx.exec_ctx_.get_allocator()))) {
      LOG_WARN("failed to get max available memory size", K(ret));
    } else if (OB_FAIL(gby_ctx.sql_mem_processor_.update_used_mem_size(gby_ctx.get_used_mem_size()))) {
      LOG_WARN("failed to update mem size", K(ret));
    }
    LOG_TRACE("trace setup dump", K(part_cnt), K(pre_part_cnt), K(level));
  }
  return ret;
}

int ObHashGroupBy::cleanup_dump_env(const bool dump_success, const int64_t level, ObGbyPartition** parts,
    int64_t& part_cnt, ObGbyBloomFilter*& bloom_filter, ObHashGroupByCtx& gby_ctx) const
{
  int ret = OB_SUCCESS;
  // add spill partitions to partition list
  if (dump_success && part_cnt > 0) {
    int64_t part_rows[part_cnt];
    int64_t part_file_size[part_cnt];
    for (int64_t i = 0; OB_SUCC(ret) && i < part_cnt; i++) {
      ObGbyPartition*& p = parts[i];
      if (p->row_store_.get_row_cnt() > 0) {
        if (OB_FAIL(p->row_store_.dump(false, true))) {
          LOG_WARN("failed to dump partition", K(ret), K(i));
        } else if (OB_FAIL(p->row_store_.finish_add_row(true /* do dump */))) {
          LOG_WARN("do dump failed", K(ret));
        } else {
          part_rows[i] = p->row_store_.get_row_cnt();
          part_file_size[i] = p->row_store_.get_file_size();
          if (!gby_ctx.all_parts_.add_first(p)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("add to list failed", K(ret));
          } else {
            p = NULL;
          }
        }
      } else {
        part_rows[i] = 0;
        part_file_size[i] = 0;
      }
    }
    LOG_TRACE("hash group by dumped",
        K(level),
        K(gby_ctx.group_rows_.size()),
        K(gby_ctx.get_used_mem_size()),
        K(gby_ctx.aggr_func_.get_used_mem_size()),
        K(gby_ctx.get_mem_bound()),
        K(part_cnt),
        "part rows",
        ObArrayWrap<int64_t>(part_rows, part_cnt),
        "part file sizes",
        ObArrayWrap<int64_t>(part_file_size, part_cnt));
  }

  LOG_TRACE("trace hash group by info",
      K(level),
      K(gby_ctx.group_rows_.size()),
      K(gby_ctx.get_used_mem_size()),
      K(gby_ctx.aggr_func_.get_used_mem_size()),
      K(gby_ctx.get_mem_bound()));

  ObArrayWrap<ObGbyPartition*> part_array(parts, part_cnt);
  if (NULL != gby_ctx.mem_context_) {
    FOREACH_CNT(p, part_array)
    {
      if (NULL != *p) {
        (*p)->~ObGbyPartition();
        gby_ctx.mem_context_->get_malloc_allocator().free(*p);
        *p = NULL;
      }
    }
    if (NULL != bloom_filter) {
      bloom_filter->~ObGbyBloomFilter();
      gby_ctx.mem_context_->get_malloc_allocator().free(bloom_filter);
      bloom_filter = NULL;
    }
  }

  return ret;
}

int ObHashGroupBy::inner_close(ObExecContext& ctx) const
{
  int ret = OB_SUCCESS;
  ObHashGroupByCtx* groupby_ctx = NULL;
  if (OB_ISNULL(groupby_ctx = GET_PHY_OPERATOR_CTX(ObHashGroupByCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get hash groupby context failed", K_(id));
  } else {
    groupby_ctx->sql_mem_processor_.unregister_profile();
  }
  return ret;
}

int ObHashGroupBy::init_sql_mem_mgr(ObHashGroupByCtx* gby_ctx, int64_t input_size) const
{
  int ret = OB_SUCCESS;
  gby_ctx->sql_mem_processor_.reset();
  if (OB_FAIL(gby_ctx->sql_mem_processor_.init(&gby_ctx->exec_ctx_.get_allocator(),
          gby_ctx->exec_ctx_.get_my_session()->get_effective_tenant_id(),
          input_size,
          get_type(),
          get_id(),
          &gby_ctx->exec_ctx_))) {
    LOG_WARN("failed to init sql mem processor", K(ret));
  }
  return ret;
}

int ObHashGroupBy::inner_get_next_row(ObExecContext& ctx, const ObNewRow*& row) const
{
  int ret = OB_SUCCESS;
  ObHashGroupByCtx* groupby_ctx = NULL;
  ObSQLSessionInfo* my_session = ctx.get_my_session();
  const ObTimeZoneInfo* tz_info = (my_session != NULL) ? my_session->get_timezone_info() : NULL;

  if (OB_ISNULL(groupby_ctx = GET_PHY_OPERATOR_CTX(ObHashGroupByCtx, ctx, get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get hash groupby context failed", K_(id));
  } else {
    if (!groupby_ctx->started_) {
      if (OB_FAIL(groupby_ctx->init_mem_context())) {
        LOG_WARN("failed to create entity", K(ret));
      } else if (!groupby_ctx->bkt_created_) {
        // create bucket
        int64_t est_group_cnt = est_group_cnt_;
        if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(&ctx, px_est_size_factor_, est_group_cnt_, est_group_cnt))) {
          LOG_WARN("failed to get px size", K(ret));
        }
        int64_t est_hash_mem_size = groupby_ctx->estimate_hash_bucket_size(est_group_cnt);
        int64_t estimate_mem_size = est_hash_mem_size + get_width() * est_group_cnt;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(init_sql_mem_mgr(groupby_ctx, estimate_mem_size))) {
          LOG_WARN("failed to init sql mem manager", K(ret));
        } else {
          int64_t init_size = groupby_ctx->estimate_hash_bucket_cnt_by_mem_size(est_group_cnt,
              groupby_ctx->sql_mem_processor_.get_mem_bound(),
              est_hash_mem_size * 1. / estimate_mem_size);
          init_size = std::max((int64_t)MIN_GROUP_HT_INIT_SIZE, init_size);
          init_size = std::min((int64_t)MAX_GROUP_HT_INIT_SIZE, init_size);
          ObMemAttr attr(
              ctx.get_my_session()->get_effective_tenant_id(), ObModIds::OB_HASH_NODE_GROUP_ROWS, ObCtxIds::WORK_AREA);
          if (OB_FAIL(
                  groupby_ctx->group_rows_.init(&groupby_ctx->mem_context_->get_malloc_allocator(), attr, init_size))) {
            LOG_WARN("fail to init hash map", K(ret));
          } else {
            groupby_ctx->op_monitor_info_.otherstat_1_value_ = init_size;
            groupby_ctx->op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::HASH_INIT_BUCKET_COUNT;
            IGNORE_RETURN groupby_ctx->sql_mem_processor_.update_used_mem_size(groupby_ctx->get_used_mem_size());
            groupby_ctx->bkt_created_ = true;
            LOG_TRACE("trace init hash table",
                K(init_size),
                K(est_group_cnt_),
                K(est_group_cnt),
                K(est_hash_mem_size),
                K(estimate_mem_size),
                K(groupby_ctx->profile_.get_expect_size()),
                K(groupby_ctx->profile_.get_cache_size()),
                K(groupby_ctx->sql_mem_processor_.get_mem_bound()));
          }
        }
      }
      // load data
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(load_data(ctx))) {
        LOG_WARN("load data to hash group by map failed", K(ret));
      } else {
        groupby_ctx->group_idx_ = 0;
        groupby_ctx->started_ = true;
      }
    } else {
      ++groupby_ctx->group_idx_;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(groupby_ctx->group_idx_ >= groupby_ctx->group_rows_.size())) {
      if (groupby_ctx->all_parts_.is_empty()) {
        ret = OB_ITER_END;
        groupby_ctx->op_monitor_info_.otherstat_2_value_ = groupby_ctx->agged_group_cnt_;
        groupby_ctx->op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::HASH_ROW_COUNT;
        groupby_ctx->op_monitor_info_.otherstat_3_value_ =
            max(groupby_ctx->group_rows_.get_bucket_num(), groupby_ctx->op_monitor_info_.otherstat_3_value_);
        groupby_ctx->op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::HASH_BUCKET_COUNT;
      } else {
        if (OB_FAIL(load_data(ctx))) {
          LOG_WARN("load partitioned data failed", K(ret));
        } else {
          groupby_ctx->op_monitor_info_.otherstat_3_value_ =
              max(groupby_ctx->group_rows_.get_bucket_num(), groupby_ctx->op_monitor_info_.otherstat_3_value_);
          groupby_ctx->op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::HASH_BUCKET_COUNT;
          groupby_ctx->group_idx_ = 0;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(groupby_ctx->aggr_func_.get_result(groupby_ctx->get_cur_row(), tz_info, groupby_ctx->group_idx_))) {
        LOG_WARN("failed to get aggr result", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = &groupby_ctx->get_cur_row();
  }
  return ret;
}
}  // namespace sql
}  // namespace oceanbase
