/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_I_LAKE_TABLE_FILE_PRUNER_H
#define _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_I_LAKE_TABLE_FILE_PRUNER_H

#include "sql/das/ob_das_define.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "sql/engine/table/ob_external_table_pushdown_filter.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/table_format/iceberg/ob_iceberg_type_fwd.h"
#include "storage/blocksstable/index_block/ob_skip_index_filter_executor.h"

namespace oceanbase
{
namespace sql
{
class ObRawExpr;
class ObSqlSchemaGuard;

struct ObFieldBound
{
public:
  OB_UNIS_VERSION(1);
public:
  ObFieldBound();
  int deep_copy(common::ObIAllocator &allocator, ObFieldBound &src);
  int from_range(ObNewRange &range);

  int from_data_file_partition(ObIArray<ObObj> &partition, int64_t offset);
  bool is_intersect(const ObFieldBound &r_bound);
  TO_STRING_KV(K_(is_valid_range), K_(contains_null), K_(include_lower), K_(include_upper),
               K_(lower_bound), K_(upper_bound));

  // 通过ObNewRange构造ObFieldBound时会把range中的NULL拆出来通过contains_null_表示。
  // 如果range为[null; null]，NULL拆出来之后这个range已经无法通过上界和下界表示了，因此
  // 会把is_valid_range_置为true。
  bool is_valid_range_;
  bool contains_null_;
  bool include_lower_;
  bool include_upper_;
  ObObj lower_bound_;
  ObObj upper_bound_;
private:
  DISABLE_COPY_ASSIGN(ObFieldBound);
};

/// Shared partition-field bound for the lake table file pruners (hive / ODPS /
/// iceberg): one entry per partition key column, holding the query ranges of
/// that column converted into field bounds plus the raw predicates that
/// produced them. The iceberg pruner additionally uses transform_type_ (the
/// partition transform of the spec field); hive and ODPS leave it at the
/// default.
struct ObLakePartFieldBound
{
public:
  OB_UNIS_VERSION(1);
public:
  ObLakePartFieldBound(common::ObIAllocator &allocator);
  void reset();
  int assign(const ObLakePartFieldBound &other);
  int deep_copy(ObLakePartFieldBound &src);
  TO_STRING_KV(K_(column_id), K_(transform_type), K_(is_whole_range), K_(is_always_false),
               K_(bounds), K_(range_exprs));

  common::ObIAllocator &allocator_;
  uint64_t column_id_;
  iceberg::TransformType transform_type_;
  bool is_whole_range_;
  bool is_always_false_;
  ObFixedArray<ObFieldBound *, ObIAllocator> bounds_;
  // Optimizer-only pointers to predicates that produced precise ranges.  They
  // are intentionally not serialized because row-filter elimination happens
  // while the logical plan is built, before the pruner is sent to workers.
  ObFixedArray<ObRawExpr *, ObIAllocator> range_exprs_;

private:
  DISABLE_COPY_ASSIGN(ObLakePartFieldBound);
};

struct ObLakeTablePushDownFilterSpec
{
public:
  OB_UNIS_VERSION(1);

public:
  ObLakeTablePushDownFilterSpec(common::ObIAllocator &allocator)
      : pd_expr_spec_(nullptr), expr_frame_info_(allocator)
  {
  }
  int deep_copy(common::ObIAllocator &allocator, const ObLakeTablePushDownFilterSpec &other);
  TO_STRING_EMPTY();
  ObPushdownExprSpec *pd_expr_spec_;
  ObExprFrameInfo expr_frame_info_;
};

class ObILakeTableFilePruner
{
public:
  explicit ObILakeTableFilePruner(common::ObIAllocator &allocator);
  virtual ~ObILakeTableFilePruner() { }

  inline uint64_t get_table_id() const { return loc_meta_.table_loc_id_; }
  inline uint64_t get_ref_table_id() const { return loc_meta_.ref_table_id_; }
  inline const ObDASTableLocMeta &get_loc_meta() const { return loc_meta_; }
  inline ObDASTableLocMeta &get_loc_meta() { return loc_meta_; }
  inline bool is_partitioned() const { return is_partitioned_; }
  inline bool all_partitions_selected() const { return all_partitions_selected_; }
  inline share::schema::ObPartitionLevel get_part_level() const
  {
    return is_partitioned_ ? share::schema::ObPartitionLevel::PARTITION_LEVEL_ONE
                             : share::schema::ObPartitionLevel::PARTITION_LEVEL_ZERO;
  }
  virtual int assign(const ObILakeTableFilePruner &o) = 0;
  // Allocate a same-dynamic-type pruner on `allocator` and deep-copy *this into it.
  virtual int clone(common::ObIAllocator &allocator, ObILakeTableFilePruner *&pruner) const = 0;
  virtual void reset();
  int generate_column_meta_info(const ObDMLStmt &stmt);
  // Return partition predicates that were converted into exact file-pruning
  // ranges.  The optimizer may remove these predicates from the row scan when
  // the concrete table format guarantees that every selected file satisfies
  // them (for example, Hive partition columns and Iceberg identity transforms).
  virtual int get_part_id_and_range_exprs(
      common::ObIArray<uint64_t> &part_column_ids,
      common::ObIArray<ObRawExpr *> &range_exprs)
  {
    UNUSED(part_column_ids);
    UNUSED(range_exprs);
    return common::OB_SUCCESS;
  }

public:
  bool is_partitioned_;
  bool inited_;
  bool need_all_;
  bool all_partitions_selected_;
  common::ObIAllocator &allocator_; //used for deep copy other table location
  ObDASTableLocMeta loc_meta_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> column_ids_;
  common::ObFixedArray<ObColumnMeta, common::ObIAllocator> column_metas_;
  ObLakeTablePushDownFilterSpec file_filter_spec_;
  common::ObFixedArray<ObString, common::ObIAllocator> partition_values_;

protected:
  // Shared partition bound machinery of the lake table pruners: extracts the
  // per partition key column query ranges from the filters and converts them
  // into one ObLakePartFieldBound per column (in partition key order). A
  // partition column not referenced by the query is marked whole-range (the
  // iceberg semantics) — the column may have no column item in the stmt (e.g.
  // the ODPS metadata$external_partition[i] pseudo columns).
  int generate_partition_bound(const ObDMLStmt &stmt,
                               ObExecContext *exec_ctx,
                               const share::schema::ObTableSchema *table_schema,
                               const common::ObIArray<ObRawExpr *> &filter_exprs,
                               common::ObFixedArray<ObLakePartFieldBound *, common::ObIAllocator> &part_bounds);
  static int build_field_bound_from_ranges(common::ObIAllocator &allocator,
                                           common::ObIArray<ObNewRange *> &ranges,
                                           ObLakePartFieldBound &part_field_bound);
  bool check_one_row_part_column(const common::ObNewRow &ob_part_row,
                                 const common::ObIArray<ObLakePartFieldBound *> &part_bounds);
  // Virtual so the hive pruner can additionally accept its default-partition
  // string as a NULL partition value (see ObHiveFilePruner::check_one_part).
  virtual bool check_one_part(const common::ObObj &part_val,
                              const ObLakePartFieldBound &field_bounds);
};

struct ObTempFrameInfoCtxReplaceGuard
{
public:
  ObTempFrameInfoCtxReplaceGuard(ObExecContext &exec_ctx);
  ~ObTempFrameInfoCtxReplaceGuard();

private:
  ObExecContext &exec_ctx_;
  char **frames_;
  uint64_t frame_cnt_;
  uint64_t expr_op_size_;
  ObExprOperatorCtx **expr_op_ctx_store_;
};


class ObLakeTablePushDownFilter : public ObExternalTablePushdownFilter
{
public:
  ObLakeTablePushDownFilter(ObExecContext &exec_ctx,
                            ObLakeTablePushDownFilterSpec &file_filter_spec)
      : mem_attr_(), allocator_(), ctx_guard_(exec_ctx), exec_ctx_(exec_ctx),
        file_filter_spec_(file_filter_spec), eval_ctx_(nullptr), pd_expr_op_(nullptr),
        pushdown_filter_(nullptr), temp_allocator_()
  {
  }

  virtual ~ObLakeTablePushDownFilter();

  static int generate_pd_filter_spec(ObIAllocator &allocator,
                                     ObExecContext &exec_ctx,
                                     const ObDMLStmt *stmt,
                                     const ObIArray<ObRawExpr *> &filter_exprs,
                                     ObLakeTablePushDownFilterSpec &file_filter_sepc);

  int init(ObIArray<uint64_t> &column_ids, ObIArray<ObColumnMeta> &column_metas);

  static int normalization_column_id(uint64_t ob_column_id);

private:
  int generate_pd_filter();
  int prepare_filter_col_meta(ObIArray<uint64_t> &column_ids, ObIArray<ObColumnMeta> &column_metas);

public:
  lib::ObMemAttr mem_attr_;
  common::ObArenaAllocator allocator_;
  ObTempFrameInfoCtxReplaceGuard ctx_guard_;
  ObExecContext &exec_ctx_;
  ObLakeTablePushDownFilterSpec &file_filter_spec_;
  ObEvalCtx *eval_ctx_;
  ObPushdownOperator *pd_expr_op_;
  sql::ObPushdownFilterExecutor *pushdown_filter_;
  common::ObArenaAllocator temp_allocator_;
};

/// Pushdown filter over a LIST partition row (min = max = the partition cell).
/// Used by hive and ODPS: both materialize one schema partition as one list row.
class ObLakePartRowPushDownFilter : public ObLakeTablePushDownFilter
{
public:
  ObLakePartRowPushDownFilter(ObExecContext &exec_ctx,
                              ObLakeTablePushDownFilterSpec &file_filter_spec,
                              common::ObFixedArray<uint64_t, common::ObIAllocator> *part_column_ids)
      : ObLakeTablePushDownFilter(exec_ctx, file_filter_spec), part_column_ids_(part_column_ids)
  {
  }

  virtual ~ObLakePartRowPushDownFilter()
  {
  }

private:
  class PartRowFilterParamBuilder : public MinMaxFilterParamBuilder
  {
  public:
    explicit PartRowFilterParamBuilder(
        ObNewRow &row,
        common::ObFixedArray<uint64_t, common::ObIAllocator> &part_column_ids)
        : row_(row), part_column_ids_(part_column_ids)
    {
    }
    virtual ~PartRowFilterParamBuilder()
    {
    }
    int build(const int32_t ext_tbl_col_id,
              const ObColumnMeta &column_meta,
              blocksstable::ObMinMaxFilterParam &param) override;
    int next_range(const int64_t column_id, int64_t &offset, int64_t &rows)
    {
      return OB_NOT_SUPPORTED;
    }

  private:
    ObNewRow &row_;
    common::ObFixedArray<uint64_t, common::ObIAllocator> &part_column_ids_;
  };

public:
  int filter(ObNewRow row, bool &is_filtered);

private:
  common::ObFixedArray<uint64_t, common::ObIAllocator> *part_column_ids_;
};

} // namespace sql
} // namespace oceanbase
#endif
