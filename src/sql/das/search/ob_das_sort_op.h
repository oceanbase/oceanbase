/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SORT_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SORT_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_scalar_scan_op.h"
#include "sql/engine/sort/ob_sort_op_impl.h"

namespace oceanbase
{
namespace sql
{

class ObDASSortOpParam : public ObIDASSearchOpParam
{
public:
  ObDASSortOpParam(ObIDASSearchOp *child)
  : ObIDASSearchOpParam(DAS_SEARCH_OP_SORT),
    child_(child)
  { }
  virtual ~ObDASSortOpParam() {}
  OB_INLINE ObIDASSearchOp *get_child() const { return child_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;

private:
  ObIDASSearchOp *child_;
};

class ObDASSortOp : public ObIDASSearchOp
{
public:
  ObDASSortOp(ObDASSearchCtx &search_ctx)
  : ObIDASSearchOp(search_ctx),
    sort_finished_(false),
    scan_op_(nullptr),
    rowid_store_(nullptr),
    rowid_store_iter_(),
    sort_impl_(nullptr),
    sort_collations_(ctx_allocator()),
    sort_cmp_funcs_(ctx_allocator()),
    sort_memctx_()
  { }
  virtual ~ObDASSortOp() {}

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;

private:
  int do_sort();

private:
  bool sort_finished_;
  ObDASScalarScanOp *scan_op_;
  RowIDStore *rowid_store_;
  RowIDStore::Iterator rowid_store_iter_;
  // TODO: use ObSortVecOpImpl instead of ObSortOpImpl
  ObSortOpImpl *sort_impl_;
  ObSortCollations sort_collations_;
  ObSortFuncs sort_cmp_funcs_;
  lib::MemoryContext sort_memctx_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SORT_OP_H_