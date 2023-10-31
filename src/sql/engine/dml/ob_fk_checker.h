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

#ifndef OBDEV_SRC_SQL_ENGINE_DML_OB_FOREIGN_KEY_CHECKER_H_
#define OBDEV_SRC_SQL_ENGINE_DML_OB_FOREIGN_KEY_CHECKER_H_
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/das/ob_das_scan_op.h"
#include "ob_dml_ctx_define.h"

namespace oceanbase
{
namespace sql
{

class ObForeignKeyChecker
{
public:
  ObForeignKeyChecker(ObEvalCtx &eval_ctx, const ObForeignKeyCheckerCtdef &checker_ctdef)
    : eval_ctx_(eval_ctx),
      das_scan_rtdef_(),
      checker_ctdef_(checker_ctdef),
      das_ref_(eval_ctx, eval_ctx.exec_ctx_),
      table_loc_(nullptr),
      local_tablet_loc_(nullptr),
      se_rowkey_dist_ctx_(nullptr),
      table_rowkey_(),
      batch_distinct_fk_cnt_(0),
      allocator_(nullptr)
  {
  }

  ~ObForeignKeyChecker() {
  }

  TO_STRING_KV(K_(das_scan_rtdef),
               K_(checker_ctdef),
               KPC_(table_loc),
               KPC_(local_tablet_loc),
               KPC_(se_rowkey_dist_ctx),
               K_(table_rowkey),
               K_(batch_distinct_fk_cnt),
               K_(clear_exprs));

  int reset();
  int reuse();
  int do_fk_check_single_row(const ObIArray<ObForeignKeyColumn> &columns,
                             const ObExprPtrIArray &row,
                             bool &has_result);

  int do_fk_check_batch(bool &all_has_result);

  int init_foreign_key_checker(int64_t estimate_row,
                               const ObExprFrameInfo *expr_frame_info,
                               ObForeignKeyCheckerCtdef &fk_ctdef,
                               const ObExprPtrIArray &row,
                               ObIAllocator *allocator);

  int build_fk_check_das_task(const ObIArray<ObForeignKeyColumn> &columns,
                                    const ObExprPtrIArray &row,
                                    bool &need_check);
private:
  int init_das_scan_rtdef();
  int calc_lookup_tablet_loc(ObDASTabletLoc *&tablet_loc);
  int get_das_scan_op(ObDASTabletLoc *tablet_loc, ObDASScanOp *&das_scan_op);
  // map fk column to the rowkey column of parent/parent index table
  int build_table_range(const ObIArray<ObForeignKeyColumn> &columns,
                        const ObExprPtrIArray &row,
                        ObNewRange &lookup_range,
                        bool &need_check);
  int build_index_table_range(const ObIArray<ObForeignKeyColumn> &columns,
                              const ObExprPtrIArray &row,
                              ObNewRange &lookup_range,
                              bool &need_check);
  int build_index_table_range_need_shadow_column(const ObIArray<ObForeignKeyColumn> &columns,
                                                const ObExprPtrIArray &row,
                                                ObNewRange &lookup_range,
                                                bool &need_check);
  int build_primary_table_range(const ObIArray<ObForeignKeyColumn> &columns,
                                const ObExprPtrIArray &row,
                                ObNewRange &lookup_range,
                                bool &need_check);
  int check_need_shadow_columns(const ObIArray<ObForeignKeyColumn> &columns,
                                const ObExprPtrIArray &row,
                                bool &need_shadow_columns);
  int check_fk_columns_has_null(const ObIArray<ObForeignKeyColumn> &columns,
                                const ObExprPtrIArray &row,
                                bool &is_all_null,
                                bool &has_null);
  int get_scan_result_count(int64_t &get_row_count);
  int init_clear_exprs(ObForeignKeyCheckerCtdef &fk_ctdef, const ObExprPtrIArray &row);
  int check_fk_column_type(const ObObjMeta &col_obj_meta,
                           const ObObjMeta &dst_obj_meta,
                           const ObPrecision col_precision,
                           const ObPrecision dst_precision,
                           bool &need_extra_cast);
public:
  ObEvalCtx &eval_ctx_; // 用于表达式的计算
  ObDASScanRtDef das_scan_rtdef_;
  // 存储计算table loc以及构造das-task需要的分区键，主键expr等
  const ObForeignKeyCheckerCtdef  &checker_ctdef_;
  // 外键检查回表构造das-task用
  ObDASRef das_ref_;
  ObDASTableLoc *table_loc_;
  ObDASTabletLoc *local_tablet_loc_; //用于外键检查的表是非分区表的时候用
  SeRowkeyDistCtx *se_rowkey_dist_ctx_;
  ObRowkey table_rowkey_;
  int64_t batch_distinct_fk_cnt_;
  ObSEArray<ObExpr *, 4> clear_exprs_;
  ObIAllocator *allocator_;
};

} // namespace sql
} // namespace oceanbase
#endif /* OBDEV_SRC_SQL_ENGINE_DML_OB_CONFLICT_ROW_CHECKER_H_ */