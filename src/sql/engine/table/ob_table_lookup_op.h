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

#ifndef OCEANBASE_SQL_ENGINE_TABLE_OB_TABLE_LOOKUP_OP_
#define OCEANBASE_SQL_ENGINE_TABLE_OB_TABLE_LOOKUP_OP_

#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/engine/ob_operator.h"
#include "sql/executor/ob_mini_task_executor.h"
#include "sql/executor/ob_task.h"
#include "sql/das/ob_das_ref.h"
#include "sql/das/ob_data_access_service.h"

namespace oceanbase
{
namespace sql
{
class ObDASScanOp;
class ObTableLookupSpec: public ObOpSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObTableLookupSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      calc_part_id_expr_(NULL),
      loc_meta_(alloc),
      scan_ctdef_(alloc),
      flashback_item_(),
      batch_rescan_(false),
      rowkey_exprs_(alloc)
  { }

  virtual ~ObTableLookupSpec() { }
public:
  ObExpr *calc_part_id_expr_;
  ObDASTableLocMeta loc_meta_;
  ObDASScanCtDef scan_ctdef_;
  FlashBackItem flashback_item_;
  bool batch_rescan_;
  ExprFixedArray rowkey_exprs_;
public:
  INHERIT_TO_STRING_KV("op_spec", ObOpSpec,
                       KPC_(calc_part_id_expr),
                       K_(loc_meta),
                       K_(scan_ctdef),
                       K_(flashback_item),
                       K_(batch_rescan),
                       K_(rowkey_exprs));
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableLookupSpec);
};

class ObTableLookupOp : public ObOperator
{
public:
  ObTableLookupOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  ~ObTableLookupOp() { destroy(); }

  int inner_open() override;
  int rescan() override;
  int inner_get_next_row() override;
  int inner_get_next_batch(const int64_t max_row_cnt) override;
  int inner_close() override;
  void destroy() override;
private:
  enum LookupState {
    INDEX_SCAN,
    DISTRIBUTED_LOOKUP,
    OUTPUT_ROWS,
    EXECUTION_FINISHED
  };
  void set_index_end(bool end) { index_end_ = end; }
  bool need_next_index_batch() const;
  void reset_for_rescan();
  int process_data_table_rowkey();
  int process_data_table_rowkeys(const ObBatchRows *brs, ObEvalCtx::BatchInfoScopeGuard &guard);
  int build_data_table_range(common::ObNewRange &lookup_range);
  int do_table_lookup();
  int get_next_data_table_row();
  int get_next_data_table_rows(int64_t &count, const int64_t capacity);
  int init_pushdown_storage_filter();
  int init_das_lookup_rtdef();
  int init_das_group_range(int64_t cur_group_idx, int64_t group_size);
  int switch_lookup_result_iter();
  bool has_das_scan_op(const ObDASTabletLoc *tablet_loc, ObDASScanOp *&das_op);
  int check_lookup_row_cnt();
private:
  const static int64_t DEFAULT_BATCH_ROW_COUNT = 10000;
  const static int64_t MAX_ROWKEY_GROUP_CNT = 1000;

  ObDASScanRtDef scan_rtdef_;
  common::ObArenaAllocator allocator_;
  ObDASRef das_ref_;
  DASOpResultIter lookup_result_;
  int64_t index_group_cnt_;  // number of groups fetched from index table
  int64_t lookup_group_cnt_; // number of groups fetched from lookup table
  int64_t lookup_rowkey_cnt_; // number of rows fetched from index table
  int64_t lookup_row_cnt_;
  LookupState state_;
  bool index_end_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableLookupOp);
};

} // end namespace sql
} // end namespace oceanbase
#endif
