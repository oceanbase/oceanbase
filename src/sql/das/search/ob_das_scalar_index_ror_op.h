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

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_INDEX_ROR_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_INDEX_ROR_OP_H_

#include "sql/das/search/ob_das_scalar_ror_op.h"

namespace oceanbase
{
namespace sql
{

class ObDASScalarIndexROROpParam : public ObIDASSearchOpParam
{
public:
  ObDASScalarIndexROROpParam(const ObDASScalarScanCtDef *scan_ctdef, ObDASScalarScanRtDef *scan_rtdef)
    : ObIDASSearchOpParam(DAS_SEARCH_OP_SCALAR_INDEX_ROR), scan_ctdef_(scan_ctdef), scan_rtdef_(scan_rtdef) {}
  ~ObDASScalarIndexROROpParam() {}
  OB_INLINE const ObDASScalarScanCtDef *get_scan_ctdef() const { return scan_ctdef_; }
  OB_INLINE ObDASScalarScanRtDef *get_scan_rtdef() const { return scan_rtdef_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;
private:
  const ObDASScalarScanCtDef *scan_ctdef_;
  ObDASScalarScanRtDef *scan_rtdef_;
};

class ObDASScalarIndexROROp : public ObDASScalarROROp
{
public:
  ObDASScalarIndexROROp(ObDASSearchCtx &search_ctx)
    : ObDASScalarROROp(search_ctx)
  { }

protected:
  virtual int advance_skip_scan(const ObDASRowID &target) override;

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_INDEX_ROR_OP_H_
