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

#ifndef OCEANBASE_SQL_ENGINE_DML_OB_TABLE_INSERET_OP_
#define OCEANBASE_SQL_ENGINE_DML_OB_TABLE_INSERET_OP_

#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/dml/ob_err_log_service.h"

namespace oceanbase
{
namespace sql
{
class ObTableInsertSpec : public ObTableModifySpec
{
  OB_UNIS_VERSION_V(1);
public:
  typedef common::ObArrayWrap<ObInsCtDef*> InsCtDefArray;
  typedef common::ObArrayWrap<InsCtDefArray> InsCtDef2DArray;
  ObTableInsertSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObTableModifySpec(alloc, type),
      ins_ctdefs_(),
      alloc_(alloc)
  { }

  //This interface is only allowed to be used in a single-table DML operator,
  //it is invalid when multiple tables are modified in one DML operator
  virtual int get_single_dml_ctdef(const ObDMLBaseCtDef *&dml_ctdef) const override
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(ins_ctdefs_.count() != 1)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "table ctdef is invalid", K(ret), K(ins_ctdefs_.count()));
    } else if (OB_UNLIKELY(ins_ctdefs_.at(0).count() != 1)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "table ctdef is invalid", K(ret), K(ins_ctdefs_.at(0).count()));
    } else {
      dml_ctdef = ins_ctdefs_.at(0).at(0);
    }
    return ret;
  }

  INHERIT_TO_STRING_KV("ObTableModifySpec", ObTableModifySpec,
                       K_(ins_ctdefs));

  //common::ObFixedArray<ObInsCtDef*, common::ObIAllocator> ins_ctdefs_;
  //ins_ctdef is a 2D array：
  //the first layer represents the information of table
  //the secondary layer represents the global index information in a table
  InsCtDef2DArray ins_ctdefs_;
protected:
  common::ObIAllocator &alloc_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableInsertSpec);
};

class ObTableInsertOp : public ObTableModifyOp
{
public:
  typedef common::ObArrayWrap<ObInsRtDef> InsRtDefArray;
  typedef common::ObArrayWrap<InsRtDefArray> InsRtDef2DArray;
  ObTableInsertOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input)
      : ObTableModifyOp(ctx, spec, input),
        ins_rtdefs_(),
        err_log_service_(get_eval_ctx())
  {
  }
  virtual ~ObTableInsertOp() {};
protected:
  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_close() override;
protected:
  int inner_open_with_das();
  int insert_row_to_das();
  virtual int write_row_to_das_buffer() override;
  virtual int write_rows_post_proc(int last_errno) override;
  int calc_tablet_loc(const ObInsCtDef &ins_ctdef,
                      ObInsRtDef &ins_rtdef,
                      ObDASTabletLoc *&tablet_loc);
  int open_table_for_each();
  int close_table_for_each();

  int check_insert_affected_row();
  virtual void record_err_for_load_data(int err_ret, int row_num) override;
  virtual int check_need_exec_single_row() override;
protected:
  InsRtDef2DArray ins_rtdefs_; //see the comment of InsCtDef2DArray
private:
  ObErrLogService err_log_service_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableInsertOp);
};

class ObTableInsertOpInput : public ObTableModifyOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObTableInsertOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObTableModifyOpInput(ctx, spec)
  {}
  int init(ObTaskInfo &task_info) override
  {
    return ObTableModifyOpInput::init(task_info);
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableInsertOpInput);
};

} // end namespace sql
} // end namespace oceanbase
#endif
