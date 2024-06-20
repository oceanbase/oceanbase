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

#ifndef _OB_SQL_ENGINE_PDML_PX_MULTI_PART_DELETE_OP_H_
#define _OB_SQL_ENGINE_PDML_PX_MULTI_PART_DELETE_OP_H_

#include "lib/container/ob_fixed_array.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/pdml/static/ob_pdml_op_data_driver.h"

namespace oceanbase
{
namespace storage
{
class ObDMLBaseParam;
}
namespace sql
{
class ObPxMultiPartDeleteOpInput : public ObPxMultiPartModifyOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxMultiPartDeleteOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObPxMultiPartModifyOpInput(ctx, spec)
  {}
  int init(ObTaskInfo &task_info) override
  {
    return ObPxMultiPartModifyOpInput::init(task_info);
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartDeleteOpInput);
};

class ObPxMultiPartDeleteSpec : public ObTableModifySpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxMultiPartDeleteSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObTableModifySpec(alloc, type),
    row_desc_(),
    del_ctdef_(alloc),
    with_barrier_(false)
  {
  }
  //This interface is only allowed to be used in a single-table DML operator,
  //it is invalid when multiple tables are modified in one DML operator
  virtual int get_single_dml_ctdef(const ObDMLBaseCtDef *&dml_ctdef) const override
  {
    dml_ctdef = &del_ctdef_;
    return common::OB_SUCCESS;
  }

  void set_with_barrier(bool w) { with_barrier_ = w; }
  virtual bool is_pdml_operator() const override { return true; } // pdml delete既是dml，又是pdml
  int register_to_datahub(ObExecContext &ctx) const override;
public:
  ObDMLOpRowDesc row_desc_;  // 记录partition id column所在row的第几个cell
  ObDelCtDef del_ctdef_;
  // 由于update row movement的存在，update计划会展开成：
  //  INSERT
  //    DELETE
  //
  // INSERT需要等待DELETE算子全部执行完毕，才能够output数据
  bool with_barrier_;
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartDeleteSpec);
};

class ObPxMultiPartDeleteOp : public ObDMLOpDataReader,
                            public ObDMLOpDataWriter,
                            public ObTableModifyOp
{
  OB_UNIS_VERSION(1);
public:
  ObPxMultiPartDeleteOp(ObExecContext &exec_ctx,
                        const ObOpSpec &spec,
                        ObOpInput *input)
  : ObTableModifyOp(exec_ctx, spec, input),
    data_driver_(&ObOperator::get_eval_ctx(), exec_ctx.get_allocator(), op_monitor_info_)
  {
  }

public:
  virtual bool has_foreign_key() const  { return false; } // 默认实现，先不考虑外键的问题
  // impl. ObDMLDataReader
  // 从 child op 读入一行数据缓存到 ObPxMultiPartDelete 算子
  // 同时还负责计算出这一行对应的 partition_id
  int read_row(ObExecContext &ctx,
               const ObExprPtrIArray *&row,
               common::ObTabletID &tablet_id,
               bool &is_skipped) override;
  // impl. ObDMLDataWriter
  // 将缓存的数据批量写入到存储层
  int write_rows(ObExecContext &ctx,
                 const ObDASTabletLoc *tablet_loc,
                 ObPDMLOpRowIterator &iterator) override;

  // 上层 op 从 ObPxMultiPartDelete 读出一行
  virtual int inner_get_next_row();
  virtual int inner_open();
  virtual int inner_close();
private:
  ObPDMLOpDataDriver data_driver_;
  ObDelRtDef del_rtdef_;
  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartDeleteOp);
};
}

}
#endif /* _OB_SQL_ENGINE_PDML_PX_MULTI_PART_DELETE_OP_H_ */
//// end of header file

