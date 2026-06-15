/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_ENGINE_AGGREGATE_OB_HASH_DISTINCT_VEC_OP_H_
#define OCEANBASE_SRC_SQL_ENGINE_AGGREGATE_OB_HASH_DISTINCT_VEC_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/aggregate/ob_distinct_op.h"
#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/basic/ob_hp_infras_vec_op.h"
#include "sql/engine/aggregate/ob_adaptive_bypass_ctrl.h"

namespace oceanbase
{
namespace sql
{
class ObHashPartInfrasVecMgr;
class ObHashPartInfrastructureVecImpl;

class ObHashDistinctVecSpec : public ObOpSpec
{
  // TODO: bump version when adding new serialized fields (e.g. skew_detection_enabled_)
  OB_UNIS_VERSION_V(1);
public:
  ObHashDistinctVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec,
                       K_(distinct_exprs),
                       K_(is_block_mode),
                       K_(by_pass_enabled),
                       K_(is_push_down),
                       K_(grouping_id),
                       K_(has_non_distinct_aggr_params),
                       K_(skew_detection_enabled));
  bool is_ordered_group_output() const { return !is_block_mode_ && group_distinct_exprs_.count() > 0; }
  // data members
  common::ObFixedArray<ObExpr*, common::ObIAllocator> distinct_exprs_;
  ObSortCollations sort_collations_;
  bool is_block_mode_;
  bool by_pass_enabled_;
  bool is_push_down_;
  common::ObFixedArray<ExprFixedArray, common::ObIAllocator> group_distinct_exprs_;
  ObExpr *grouping_id_;
  common::ObFixedArray<ObSortCollations, common::ObIAllocator> group_sort_collations_;
  bool has_non_distinct_aggr_params_;
  bool llc_ndv_est_enabled_;
  bool skew_detection_enabled_;
};

class ObHashDistinctVecOp : public ObOperator
{
public:
  ObHashDistinctVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  ~ObHashDistinctVecOp() {}

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override { return common::OB_NOT_IMPLEMENT; }
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;

private:
  typedef int (ObHashDistinctVecOp::*Build_distinct_data_batch_func)(const int64_t batch_size, bool is_block);
  void reset();
  int do_unblock_distinct_for_batch(const int64_t batch_size);
  int do_block_distinct_for_batch(const int64_t batch_size);
  int init_hash_partition_infras();
  int init_hash_partition_infras_for_batch();
  int build_distinct_data_for_batch(const int64_t batch_size, bool is_block);
  int build_distinct_data_for_batch_by_pass(const int64_t batch_size, bool is_block);
  int by_pass_get_next_batch(const int64_t batch_size);
  int init_mem_context();
  int init_popular_values();
  int export_heap_to_popular_map(int64_t dynamic_threshold);

  int do_group_distinct_for_batch(const int64_t batch_size);

  int build_group_distinct_data_for_batch(const int64_t batch_size);

  int init_group_hp_infras();

  template <typename ColumnFmt>
  int group_child_input(const ObBatchRows &child_brs, ObIVector *grouping_id_vec,
                        int64_t &min_group, int64_t &max_group);

  int insert_group_distinct_data(const ObBatchRows &child_brs, const int64_t group_start, const int64_t group_end);

  int insert_group_distinct_data_from_dump_part(ObHashPartInfrastructureVecImpl *hp_infras);

  int read_group_distinct_data(int64_t batch_size);

  int read_group_distinct_data(const int64_t max_row_cnt, ObHashPartInfrastructureVecImpl *hp_infras);

  int setup_null_expr_and_grouping_id(const int64_t read_rows);

  int64_t get_hash_bucket_num() const;

  int read_non_distinct_aggr_params_data(const int64_t max_row_cnt);

  bool need_dump_non_distinct_store();

  int process_non_distinct_store_dump();
private:
  friend struct NonDistinctStoreDumpCheckOp;
private:
  struct NonDistinctStoreDumpCheckOp
  {
    NonDistinctStoreDumpCheckOp(ObHashDistinctVecOp &op) : op_(op) {}
    bool operator()(int64_t cur_cnt) const
    {
      return op_.need_dump_non_distinct_store();
    }
  private:
    ObHashDistinctVecOp &op_;
  };
private:
  enum class GroupDistinctState {
    OPEN_HASH_PART,
    READ_ROWS,
    ITER_END
  };

private:
  typedef int (ObHashDistinctVecOp::*GetNextRowBatchFunc)(const int64_t batch_size);
  static const int64_t MIN_BUCKET_COUNT = 1L << 14;  //16384;
  static const int64_t MAX_BUCKET_COUNT = 1L << 19; //524288;
  // Skew detection constants
  static const int64_t SKEW_HEAP_SIZE = 15;
  static const int64_t SKEW_ITEM_CNT_TOLERANCE = 64;
  static const int64_t SKEW_TEST_STEP_SIZE = 5;
  static const uint64_t MIN_CHECK_POPULAR_VALID_ROWS = 10000;
  constexpr static const float SKEW_POPULAR_MAX_RATIO = 0.5;
  static const int64_t INIT_BUCKET_COUNT_FOR_POPULAR = 32;
  bool enable_sql_dumped_;
  bool first_got_row_;
  bool has_got_part_;
  bool iter_end_;
  bool child_op_is_end_;
  bool need_init_; //init 3 arrays before got_row, do not reset in rescan()
  GetNextRowBatchFunc get_next_batch_func_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  ObHashPartInfrastructureVecImpl hp_infras_;
  int64_t group_cnt_;
  uint64_t *hash_values_for_batch_;
  int64_t tenant_id_;
  int64_t extend_bkt_num_push_down_;
  Build_distinct_data_batch_func build_distinct_data_batch_func_;
  ObAdaptiveByPassCtrl bypass_ctrl_;
  lib::MemoryContext mem_context_;
  ObHashPartInfrasVecMgr *hp_infras_mgr_;
  ObFixedArray<ObHashPartInfrastructureVecImpl *, common::ObIAllocator> hp_infras_arr_;
  ObFixedArray<char *, common::ObIAllocator> group_selector_arr_;
  int64_t group_iter_idx_;
  int64_t min_stored_group_idx_;
  GroupDistinctState group_distinct_state_;
  ObTempColumnStore *non_distinct_aggr_params_store_;
  ObTempColumnStore::Iterator *non_distinct_aggr_params_iter_;
  int64_t group_distinct_bucket_cnt_;
  // Skew detection members
  bool skew_detection_enabled_;
  uint64_t by_pass_rows_;
  uint64_t total_load_rows_;
  typedef common::hash::ObHashMap<uint64_t, uint64_t, hash::NoPthreadDefendMode> PopularMapType;
  PopularMapType popular_map_;
  common::ObArray<std::pair<uint64_t, int32_t>> popular_array_temp_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_ENGINE_AGGREGATE_OB_HASH_DISTINCT_VEC_OP_H_ */