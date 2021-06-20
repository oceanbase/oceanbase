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

#ifndef OCEANBASE_BASIC_OB_HASH_GROUPBY_OP_H_
#define OCEANBASE_BASIC_OB_HASH_GROUPBY_OP_H_

#include "common/row/ob_row_store.h"
#include "sql/engine/aggregate/ob_groupby_op.h"
#include "sql/engine/aggregate/ob_exec_hash_struct.h"
#include "lib/list/ob_list.h"
#include "lib/list/ob_dlink_node.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"

namespace oceanbase {
namespace sql {
class ObHashGroupBySpec : public ObGroupBySpec {
  OB_UNIS_VERSION_V(1);

public:
  ObHashGroupBySpec(common::ObIAllocator& alloc, const ObPhyOperatorType type)
      : ObGroupBySpec(alloc, type), group_exprs_(alloc), cmp_funcs_(alloc), est_group_cnt_(0)
  {}

  DECLARE_VIRTUAL_TO_STRING;
  inline int init_group_exprs(const int64_t count)
  {
    return group_exprs_.init(count);
  }
  int add_group_expr(ObExpr* expr);
  inline void set_est_group_cnt(const int64_t cnt)
  {
    est_group_cnt_ = cnt;
  }

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHashGroupBySpec);

public:
  ExprFixedArray group_exprs_;  // group by column
  ObCmpFuncs cmp_funcs_;
  int64_t est_group_cnt_;
};

// input rows is already sorted by groupby columns
class ObHashGroupByOp : public ObGroupByOp {
public:
  struct DatumStoreLinkPartition : public common::ObDLinkBase<DatumStoreLinkPartition> {
  public:
    DatumStoreLinkPartition(common::ObIAllocator* alloc = nullptr) : datum_store_(alloc), part_id_(0)
    {}
    ObChunkDatumStore datum_store_;
    int64_t part_id_;
  };

public:
  static const int64_t MIN_PARTITION_CNT = 8;
  static const int64_t MAX_PARTITION_CNT = 256;

  // min in memory groups
  static const int64_t MIN_INMEM_GROUPS = 4;
  static const int64_t MIN_GROUP_HT_INIT_SIZE = 1 << 10;  // 1024
  static const int64_t MAX_GROUP_HT_INIT_SIZE = 1 << 20;  // 1048576
  static constexpr const double MAX_PART_MEM_RATIO = 0.5;
  static constexpr const double EXTRA_MEM_RATIO = 0.25;
  static const int64_t FIX_SIZE_PER_PART = sizeof(DatumStoreLinkPartition) + ObChunkRowStore::BLOCK_SIZE;

public:
  ObHashGroupByOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input)
      : ObGroupByOp(exec_ctx, spec, input),
        curr_group_id_(common::OB_INVALID_INDEX),
        mem_context_(NULL),
        agged_group_cnt_(0),
        agged_row_cnt_(0),
        agged_dumped_cnt_(0),
        profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
        sql_mem_processor_(profile_),
        iter_end_(false)
  {}
  void reset();
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int rescan() override;
  virtual int switch_iterator() override;
  virtual int inner_get_next_row() override;
  virtual void destroy() override;
  int load_data();

  int check_same_group(int64_t& diff_pos);
  int restore_groupby_datum(const int64_t diff_pos);
  int rollup_and_calc_results(const int64_t group_id);

  int64_t get_hash_groupby_row_count() const
  {
    return local_group_rows_.size();
  }

  OB_INLINE int64_t get_aggr_used_size() const
  {
    return aggr_processor_.get_aggr_used_size();
  }
  OB_INLINE int64_t get_aggr_hold_size() const
  {
    return aggr_processor_.get_aggr_hold_size();
  }
  OB_INLINE int64_t get_local_hash_used_size() const
  {
    return local_group_rows_.mem_used();
  }
  OB_INLINE int64_t get_dumped_part_used_size() const
  {
    return (NULL == mem_context_ ? 0 : mem_context_->used());
  }
  OB_INLINE int64_t get_dump_part_hold_size() const
  {
    return (NULL == mem_context_ ? 0 : mem_context_->hold());
  }
  OB_INLINE int64_t get_extra_size() const
  {
    return get_local_hash_used_size() + get_dumped_part_used_size();
  }
  OB_INLINE int64_t get_mem_used_size() const
  {
    return get_aggr_used_size() + get_extra_size();
  }
  OB_INLINE int64_t get_mem_bound_size() const
  {
    return sql_mem_processor_.get_mem_bound();
  }
  OB_INLINE int64_t estimate_hash_bucket_size(const int64_t bucket_cnt) const
  {
    return next_pow2(ObGroupRowHashTable::SIZE_BUCKET_SCALE * bucket_cnt) * sizeof(void*);
  }
  OB_INLINE int64_t estimate_hash_bucket_cnt_by_mem_size(
      const int64_t bucket_cnt, const int64_t max_mem_size, const double extra_ratio) const
  {
    int64_t mem_size = estimate_hash_bucket_size(bucket_cnt);
    int64_t max_hash_size = max_mem_size * extra_ratio;
    if (0 < max_hash_size) {
      while (mem_size > max_hash_size) {
        mem_size >>= 1;
      }
    }
    return (mem_size / sizeof(void*) / ObGroupRowHashTable::SIZE_BUCKET_SCALE);
  }
  int update_mem_status_periodically(
      const int64_t nth_cnt, const int64_t input_row, int64_t& est_part_cnt, bool& need_dump);
  int64_t detect_part_cnt(const int64_t rows) const;
  void calc_data_mem_ratio(const int64_t part_cnt, double& data_ratio);
  void adjust_part_cnt(int64_t& part_cnt);
  int calc_groupby_exprs_hash(uint64_t& hash_value);
  int init_group_row_item(const ObGroupRowItem& curr_item, ObGroupRowItem*& gr_row_item);
  bool need_start_dump(const int64_t input_rows, int64_t& est_part_cnt, const bool check_dump);
  // Setup: memory entity, bloom filter, spill partitions
  int setup_dump_env(const int64_t part_id, const int64_t input_rows, DatumStoreLinkPartition** parts,
      int64_t& part_cnt, ObGbyBloomFilter*& bloom_filter);

  int cleanup_dump_env(const bool dump_success, const int64_t part_id, DatumStoreLinkPartition** parts,
      int64_t& part_cnt, ObGbyBloomFilter*& bloom_filter);
  void destroy_all_parts();
  int restore_groupby_datum();
  int init_mem_context(void);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHashGroupByOp);

private:
  ObGroupRowHashTable local_group_rows_;
  int64_t curr_group_id_;

  // memory allocator for group by partitions
  lib::MemoryContext* mem_context_;
  ObDList<DatumStoreLinkPartition> dumped_group_parts_;
  ObChunkDatumStore group_store_;

  int64_t agged_group_cnt_;
  int64_t agged_row_cnt_;
  int64_t agged_dumped_cnt_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  bool iter_end_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_BASIC_OB_HASH_GROUPBY_OP_H_
