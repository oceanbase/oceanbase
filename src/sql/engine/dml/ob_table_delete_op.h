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

#ifndef OCEANBASE_SQL_ENGINE_DML_OB_TABLE_DELETE_OP_
#define OCEANBASE_SQL_ENGINE_DML_OB_TABLE_DELETE_OP_

#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/dml/ob_err_log_service.h"

namespace oceanbase
{
namespace sql
{

class ObTableDeleteSpec : public ObTableModifySpec
{
  OB_UNIS_VERSION(1);
public:
  typedef common::ObArrayWrap<ObDelCtDef*> DelCtDefArray;
  typedef common::ObArrayWrap<DelCtDefArray> DelCtDef2DArray;
  ObTableDeleteSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObTableModifySpec(alloc, type),
      del_ctdefs_(),
      alloc_(alloc)
  { }

  //This interface is only allowed to be used in a single-table DML operator,
  //it is invalid when multiple tables are modified in one DML operator
  virtual int get_single_dml_ctdef(const ObDMLBaseCtDef *&dml_ctdef) const override
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(del_ctdefs_.count() != 1)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "table ctdef is invalid", K(ret), K(del_ctdefs_.count()));
    } else if (OB_UNLIKELY(del_ctdefs_.at(0).count() != 1)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "table ctdef is invalid", K(ret), K(del_ctdefs_.at(0).count()));
    } else {
      dml_ctdef = del_ctdefs_.at(0).at(0);
    }
    return ret;
  }

  INHERIT_TO_STRING_KV("ObTableModifySpec", ObTableModifySpec,
                       K_(del_ctdefs));

  //del_ctdef is a 2D array：
  //the first layer represents the information of multiple table delete
  //eg: delete t1, t2 from t1, t2 where t1.a=t2.a;
  //the secondary layer represents the global index information in a table
  //eg: create table t1(a int primary key, b int) partition by hash(a) partitions 2;
  //    create index gkey on t1(b); global index gkey is a del ctdef in the secondary layer
  DelCtDef2DArray del_ctdefs_;
private:
  common::ObIAllocator &alloc_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableDeleteSpec);
};

class ObTableDeleteOp : public ObTableModifyOp
{
  typedef common::ObArrayWrap<ObDelRtDef> DelRtDefArray;
  typedef common::ObArrayWrap<DelRtDefArray> DelRtDef2DArray;
public:
  ObTableDeleteOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObTableModifyOp(exec_ctx, spec, input),
      del_rtdefs_(),
      err_log_service_(get_eval_ctx())
  { }

  virtual ~ObTableDeleteOp() {}
protected:
  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_close() override;
protected:
  int inner_open_with_das();
  int delete_row_to_das();
  virtual int write_rows_post_proc(int last_errno);
  int calc_tablet_loc(const ObDelCtDef &del_ctdef,
                      ObDelRtDef &del_rtdef,
                      ObDASTabletLoc *&tablet_loc);
  int open_table_for_each();
  int close_table_for_each();
  int check_delete_affected_row();
  virtual int write_row_to_das_buffer() override;
  virtual int check_need_exec_single_row() override;
protected:
  DelRtDef2DArray del_rtdefs_;  //see the comment of DelCtDef2DArray
  ObErrLogService err_log_service_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableDeleteOp);
};

class ObTableDeleteOpInput : public ObTableModifyOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObTableDeleteOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObTableModifyOpInput(ctx, spec)
  {}
  int init(ObTaskInfo &task_info) override
  {
    return ObTableModifyOpInput::init(task_info);
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableDeleteOpInput);
};

} // namespace sql
} // namespace oceanbase

#endif
