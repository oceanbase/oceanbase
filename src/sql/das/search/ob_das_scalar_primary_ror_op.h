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

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_PRIMARY_ROR_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_PRIMARY_ROR_OP_H_

#include "sql/das/search/ob_das_scalar_ror_op.h"

namespace oceanbase
{
namespace sql
{

class ObDASScalarPrimaryROROpParam : public ObIDASSearchOpParam
{
public:
  ObDASScalarPrimaryROROpParam(const ObDASScalarScanCtDef *scan_ctdef,
                               ObDASScalarScanRtDef *scan_rtdef)
    : ObIDASSearchOpParam(DAS_SEARCH_OP_SCALAR_PRIMARY_ROR), scan_ctdef_(scan_ctdef), scan_rtdef_(scan_rtdef) {}
  ~ObDASScalarPrimaryROROpParam() {}
  OB_INLINE const ObDASScalarScanCtDef *get_scan_ctdef() const { return scan_ctdef_; }
  OB_INLINE ObDASScalarScanRtDef *get_scan_rtdef() const { return scan_rtdef_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;
private:
  const ObDASScalarScanCtDef *scan_ctdef_;
  ObDASScalarScanRtDef *scan_rtdef_;
};

class ObDASScalarPrimaryROROp : public ObDASScalarROROp
{
public:
  ObDASScalarPrimaryROROp(ObDASSearchCtx &search_ctx)
    : ObDASScalarROROp(search_ctx),
      get_param_(),
      get_result_(nullptr),
      last_get_id_()
  { }

protected:
  int do_open() override;
  int do_rescan() override;
  int do_close() override;
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;

  int init_get_pd_op();
  int point_get(const ObDASRowID &target, bool &found);
  virtual int advance_skip_scan(const ObDASRowID &target) override;

private:
  storage::ObTableScanParam get_param_;
  common::ObNewRowIterator *get_result_;
  ObDASRowID last_get_id_;

private:
  union {
    // Independent ObPushdownOperator for get_param_ to avoid state sharing with scan_param_
    ObPushdownOperator get_pd_expr_op_;
  };
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_PRIMARY_ROR_OP_H_