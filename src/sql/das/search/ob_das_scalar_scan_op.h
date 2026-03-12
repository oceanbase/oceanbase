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

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_SCAN_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_SCAN_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_scalar_define.h"

namespace oceanbase
{
namespace sql
{

class ObDASScalarScanOpParam : public ObIDASSearchOpParam
{
public:
  ObDASScalarScanOpParam(const ObDASScalarScanCtDef *scan_ctdef,
                         ObDASScalarScanRtDef *scan_rtdef)
    : ObIDASSearchOpParam(DAS_SEARCH_OP_SCALAR_SCAN), scan_ctdef_(scan_ctdef), scan_rtdef_(scan_rtdef) {}
  ~ObDASScalarScanOpParam() {}
  OB_INLINE const ObDASScalarScanCtDef *get_scan_ctdef() const { return scan_ctdef_; }
  OB_INLINE ObDASScalarScanRtDef *get_scan_rtdef() const { return scan_rtdef_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override;
private:
  const ObDASScalarScanCtDef *scan_ctdef_;
  ObDASScalarScanRtDef *scan_rtdef_;
};

class ObDASScalarScanOp : public ObIDASSearchOp
{
public:
  ObDASScalarScanOp(ObDASSearchCtx &search_ctx)
    : ObIDASSearchOp(search_ctx),
      scan_param_(),
      scalar_ctdef_(nullptr),
      scalar_rtdef_(nullptr),
      result_(nullptr),
      tsc_service_(nullptr),
      tablet_id_()
  { }
  int get_next_rows(int64_t &count, const int64_t max_batch_size);
  const ObDASScalarScanCtDef *get_scalar_ctdef() const { return scalar_ctdef_; }

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override { return OB_NOT_SUPPORTED; };
  int do_next_rowid(ObDASRowID &next_id, double &score) override { return OB_NOT_SUPPORTED; };

private:
  int prepare_scan_ranges(const ObDASScalarScanRtDef *rtdef);

private:
  storage::ObTableScanParam scan_param_;
  const ObDASScalarScanCtDef *scalar_ctdef_;
  ObDASScalarScanRtDef *scalar_rtdef_;
  common::ObNewRowIterator *result_;
  storage::ObAccessService *tsc_service_;
  common::ObTabletID tablet_id_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_SCAN_OP_H_