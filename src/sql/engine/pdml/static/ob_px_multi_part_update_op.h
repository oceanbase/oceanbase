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

#ifndef _OB_SQL_ENGINE_PDML_PX_MULTI_PART_UPDATE_OP_H_
#define _OB_SQL_ENGINE_PDML_PX_MULTI_PART_UPDATE_OP_H_


#include "lib/container/ob_fixed_array.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/pdml/static/ob_pdml_op_data_driver.h"

namespace oceanbase
{
namespace sql
{
class ObPxMultiPartUpdateOpInput : public ObPxMultiPartModifyOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxMultiPartUpdateOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObPxMultiPartModifyOpInput(ctx, spec)
  {}
  int init(ObTaskInfo &task_info) override
  {
    return ObPxMultiPartModifyOpInput::init(task_info);
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartUpdateOpInput);
};

class ObPxMultiPartUpdateSpec : public ObTableModifySpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxMultiPartUpdateSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObTableModifySpec(alloc, type),
    row_desc_(),
    upd_ctdef_(alloc)
  {
  }
  //This interface is only allowed to be used in a single-table DML operator,
  //it is invalid when multiple tables are modified in one DML operator
  virtual int get_single_dml_ctdef(const ObDMLBaseCtDef *&dml_ctdef) const override
  {
    dml_ctdef = &upd_ctdef_;
    return common::OB_SUCCESS;
  }

  virtual bool is_pdml_operator() const override { return true; }
public:
  ObDMLOpRowDesc row_desc_;  // 记录partition id column所在row的第几个cell
  ObUpdCtDef upd_ctdef_;
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartUpdateSpec);
};

class ObPxMultiPartUpdateOp : public ObDMLOpDataReader,
                            public ObDMLOpDataWriter,
                            public ObTableModifyOp
{
  OB_UNIS_VERSION(1);
public:
  ObPxMultiPartUpdateOp(ObExecContext &exec_ctx,
                        const ObOpSpec &spec,
                        ObOpInput *input)
  : ObTableModifyOp(exec_ctx, spec, input),
    data_driver_(&ObOperator::get_eval_ctx(), exec_ctx.get_allocator(), op_monitor_info_),
    upd_rtdef_()
  {}
  virtual ~ObPxMultiPartUpdateOp() = default;
public:
  int read_row(ObExecContext &ctx,
               const ObExprPtrIArray *&row,
               common::ObTabletID &tablet_id,
               bool &is_skipped) override;

  int write_rows(ObExecContext &ctx,
                 const ObDASTabletLoc *tablet_loc,
                 ObPDMLOpRowIterator &iterator) override;

  virtual int inner_get_next_row();
  virtual int inner_open();
  virtual int inner_close();
private:
  int update_row_to_das(const ObDASTabletLoc *tablet_loc);
private:
  ObPDMLOpDataDriver data_driver_;
  ObUpdRtDef upd_rtdef_;
  // 用于对child output row进行calc，并返回DML操作需要的old_row与new_row
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartUpdateOp);
};
}
}
#endif /* _OB_SQL_ENGINE_PDML_PX_MULTI_PART_UPDATE_OP_H_ */
