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

#ifndef _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_I_LAKE_TABLE_FILE_PRUNER_H
#define _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_I_LAKE_TABLE_FILE_PRUNER_H

#include "sql/das/ob_das_define.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "sql/engine/table/ob_external_table_pushdown_filter.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
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

typedef enum {
  ICEBERG,
  HIVE
} PrunnerType;

class ObILakeTableFilePruner
{
public:
  explicit ObILakeTableFilePruner(common::ObIAllocator &allocator, PrunnerType type);
  virtual ~ObILakeTableFilePruner() { }

  inline uint64_t get_table_id() const { return loc_meta_.table_loc_id_; }
  inline uint64_t get_ref_table_id() const { return loc_meta_.ref_table_id_; }
  inline const ObDASTableLocMeta &get_loc_meta() const { return loc_meta_; }
  inline ObDASTableLocMeta &get_loc_meta() { return loc_meta_; }
  inline bool is_partitioned() const { return is_partitioned_; }
  inline share::schema::ObPartitionLevel get_part_level() const
  {
    return is_partitioned_ ? share::schema::ObPartitionLevel::PARTITION_LEVEL_ONE
                             : share::schema::ObPartitionLevel::PARTITION_LEVEL_ZERO;
  }
  virtual int assign(const ObILakeTableFilePruner &o) = 0;
  virtual void reset();
  int generate_column_meta_info(const ObDMLStmt &stmt);

public:
  bool is_partitioned_;
  bool inited_;
  bool need_all_;
  common::ObIAllocator &allocator_; //used for deep copy other table location
  ObDASTableLocMeta loc_meta_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> column_ids_;
  common::ObFixedArray<ObColumnMeta, common::ObIAllocator> column_metas_;
  ObLakeTablePushDownFilterSpec file_filter_spec_;
  PrunnerType type_;
  common::ObFixedArray<ObString, common::ObIAllocator> partition_values_;
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

  virtual ~ObLakeTablePushDownFilter()
  {
  }

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

} // namespace sql
} // namespace oceanbase
#endif
