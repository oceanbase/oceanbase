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

#ifndef OCEANBASE_DML_OB_TABLE_UPDATE_OP_H_
#define OCEANBASE_DML_OB_TABLE_UPDATE_OP_H_

#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/dml/ob_err_log_service.h"

namespace oceanbase
{
namespace sql
{

class ObTableUpdateOpInput : public ObTableModifyOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObTableUpdateOpInput(ObExecContext &ctx, const ObOpSpec &spec)
      : ObTableModifyOpInput(ctx, spec)
  {
  }
  virtual int init(ObTaskInfo &task_info) override
  {
    return ObTableModifyOpInput::init(task_info);
  }
};

class ObTableUpdateSpec : public ObTableModifySpec
{
  OB_UNIS_VERSION_V(1);
public:
  typedef common::ObArrayWrap<ObUpdCtDef*> UpdCtDefArray;
  typedef common::ObArrayWrap<UpdCtDefArray> UpdCtDef2DArray;

  ObTableUpdateSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);
  ~ObTableUpdateSpec();

  //This interface is only allowed to be used in a single-table DML operator,
  //it is invalid when multiple tables are modified in one DML operator
  virtual int get_single_dml_ctdef(const ObDMLBaseCtDef *&dml_ctdef) const override
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(upd_ctdefs_.count() != 1)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "table ctdef is invalid", K(ret), K(upd_ctdefs_.count()));
    } else if (OB_UNLIKELY(upd_ctdefs_.at(0).count() != 1)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "table ctdef is invalid", K(ret), K(upd_ctdefs_.at(0).count()));
    } else {
      dml_ctdef = upd_ctdefs_.at(0).at(0);
    }
    return ret;
  }

  INHERIT_TO_STRING_KV("table_modify_spec", ObTableModifySpec,
                       K_(upd_ctdefs));

public:
  //upd_ctdef is a 2D array：
  //the first layer represents the information of multiple table update
  //eg: update t1, t2 set t1.b=1, t2.b=1 where t1.a=t2.a;
  //the secondary layer represents the global index information in a table
  //eg: create table t1(a int primary key, b int) partition by hash(a) partitions 2;
  //    create index gkey on t1(b); global index gkey is a upd ctdef in the secondary layer
  UpdCtDef2DArray upd_ctdefs_;
private:
  common::ObIAllocator &alloc_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableUpdateSpec);
};

class ObTableUpdateOp : public ObTableModifyOp
{
  typedef common::ObArrayWrap<ObUpdRtDef> UpdRtDefArray;
  typedef common::ObArrayWrap<UpdRtDefArray> UpdRtDef2DArray;
public:
  friend ObTableModifyOp;
  ObTableUpdateOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);

  int inner_open() override;
  int inner_rescan() override;
  int inner_switch_iterator() override;
  int inner_close() override;
protected:
  int inner_open_with_das();
  int update_row_to_das();
  int upd_rows_post_proc();
  int calc_tablet_loc(const ObUpdCtDef &upd_ctdef,
                      ObUpdRtDef &upd_rtdef,
                      ObDASTabletLoc *&old_tablet_loc,
                      ObDASTabletLoc *&new_tablet_loc);
  int calc_multi_tablet_id(const ObUpdCtDef &upd_ctdef,
                           ObExpr &part_id_expr,
                           common::ObTabletID &tablet_id,
                           bool check_exist = false);
  int open_table_for_each();
  int close_table_for_each();
  int check_update_affected_row();
  virtual int write_row_to_das_buffer() override;
  virtual int write_rows_post_proc(int last_errno) override;
  virtual int check_need_exec_single_row() override;

protected:
  UpdRtDef2DArray upd_rtdefs_;  //see the comment of UpdCtDef2DArray
  common::ObArrayWrap<ObInsRtDef> ins_rtdefs_;
  ObErrLogService err_log_service_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_DML_OB_TABLE_UPDATE_OP_H_
