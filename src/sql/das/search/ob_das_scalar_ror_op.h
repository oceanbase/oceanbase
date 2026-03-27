/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_ROR_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_ROR_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_scalar_define.h"

namespace oceanbase
{
namespace sql
{

class ObDASScalarROROp : public ObIDASSearchOp
{
public:
  ObDASScalarROROp(ObDASSearchCtx &search_ctx)
    : ObIDASSearchOp(search_ctx),
      scan_param_(),
      scalar_ctdef_(nullptr),
      scalar_rtdef_(nullptr),
      result_(nullptr),
      tsc_service_(nullptr),
      tablet_id_(),
      rowid_store_(nullptr),
      rowid_store_iter_()
  { }
  virtual ~ObDASScalarROROp() {}

protected:
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;
  virtual int advance_skip_scan(const ObDASRowID &target) = 0;
  virtual int prepare_scan_ranges(const ObDASScalarScanRtDef *rtdef);

protected:
  storage::ObTableScanParam scan_param_;
  const ObDASScalarScanCtDef *scalar_ctdef_;
  ObDASScalarScanRtDef *scalar_rtdef_;
  common::ObNewRowIterator *result_;
  storage::ObAccessService *tsc_service_;
  common::ObTabletID tablet_id_;

  RowIDStore *rowid_store_;
  RowIDStore::Iterator rowid_store_iter_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OPERATOR_OB_DAS_SCALAR_ROR_OP_H_