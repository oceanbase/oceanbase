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

#ifndef OCEANBASE_SQL_ENGINE_STAT_COLLECTOR_OP_H_
#define OCEANBASE_SQL_ENGINE_STAT_COLLECTOR_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/sort/ob_sort_op_impl.h"
#include "sql/engine/sort/ob_sort_basic_info.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "lib/container/ob_2d_array.h"

namespace oceanbase
{
namespace sql
{

/*
 * This operator is currently used for px object dynamic sampling during execution,
 * and try to evenly extract the data distribution range of sample data of the current thread,
 * currently only used in online ddl scenarios, such as
 * creating indexes, changing partitions online, etc.
 * */

class ObStatCollectorSpec : public ObOpSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObStatCollectorSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
  TO_STRING_KV(K_(sort_exprs),
               K_(sort_collations),
               K_(sort_cmp_funs),
               K_(sort_collations_inverted),
               K_(sort_cmp_funs_inverted),
               K_(type));
public:
 /*
 * Is_none_partition is used to mark whether the target table is a partitioned table,
 * if target table is a partitioned table,
 * the first expression in the sort exprs array is calc_part_id_expr,
 * because it should be used as sort key when sorting;
 * if it is not a partitioned table,
 * there is no part id expr.
 * */
  bool is_none_partition_;
 /*
 * The sort expr is the partition key(if any) and index column
 * */
  ExprFixedArray sort_exprs_;
  ObSortCollations sort_collations_;
  ObSortFuncs sort_cmp_funs_;
 /*
 * Type is designed for future expansion
 * */
  ObStatCollectorType type_;
 /*
  * For FTS DDL sampling, inverted index (word, doc_id) needs a different sort key set.
  * When these arrays are non-empty, sampler will maintain a second sorter and
  * generate inverted ranges based on them.
* */
  ExprFixedArray sort_exprs_inverted_;
  ObSortCollations sort_collations_inverted_;
  ObSortFuncs sort_cmp_funs_inverted_;
};

struct TabletDocidRow
{
public:
  TabletDocidRow() : tablet_id_(OB_INVALID_ID), border_vals_() {}
  int assign(const TabletDocidRow &other);
  TO_STRING_KV(K_(tablet_id), K_(border_vals));
public:
  int64_t tablet_id_;
  ObPxTabletRange::DatumKey border_vals_;
};

class ObStatCollectorOp : public ObOperator
{
public:
  ObStatCollectorOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;
  virtual int inner_close() override;
  typedef hash::ObHashMap<int64_t, int64_t *, hash::
          NoPthreadDefendMode>PartitionCountMap;
private:
  static const int64_t DEFAULT_HASH_MAP_BUCKETS_COUNT = 10000; //1w
  int generate_sample_partition_range(int64_t batch_size = 0);
  int split_partition_range(const bool is_inverted = false);
  int split_fts_forward_partition_range();
  int collect_row_count_in_partitions(
      bool is_vectorized = false,
      const ObBatchRows *child_brs = NULL,
      int64_t batch_size = 0);
  int collect_tablet_docid_counts(ObSortOpImpl &sort_impl,
      const ExprFixedArray &sort_exprs,
      ObArray<TabletDocidRow> &tablet_docid_rows,
      common::hash::ObHashMap<int64_t, int64_t> &partition_docid_count_map);
  bool is_none_partition();
  int update_partition_row_count();
  int get_tablet_id(const ExprFixedArray &sort_exprs, int64_t &tablet_id);
  int set_no_need_sample();
  int find_sample_scan(ObOperator *op, ObOperator *&tsc);
  int64_t get_one_thread_sampling_count_by_parallel(const int64_t parallel);
  int report_sample_ranges_to_dag(const bool is_inverted, const common::Ob2DArray<sql::ObPxTabletRange> &part_ranges);
  bool need_report_sample_to_dag() const { return !MY_SPEC.sort_collations_inverted_.empty(); }

private:
  ObSortOpImpl sort_impl_;
  ObSortOpImpl sort_impl_inverted_;
  bool iter_end_;
  bool by_pass_;
  bool exist_sample_row_;
  PartitionCountMap partition_row_count_map_;
  int64_t non_partition_row_count_; // non-partition row count
  int64_t inverted_sample_row_count_; // for partition local ddl fulltext index inverted table
};

}
}
#endif
