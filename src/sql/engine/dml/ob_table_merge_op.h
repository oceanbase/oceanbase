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

#ifndef _SQL_ENGINE_DML_OB_TABLE_MERGE_OP_H
#define _SQL_ENGINE_DML_OB_TABLE_MERGE_OP_H

#include "sql/engine/ob_operator.h"
#include "lib/container/ob_fixed_array.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "storage/access/ob_dml_param.h"

namespace oceanbase
{
namespace storage
{
class ObDMLBaseParam;
}
namespace sql
{

class ObTableMergeOpInput : public ObTableModifyOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObTableMergeOpInput(ObExecContext &ctx, const ObOpSpec &spec)
  : ObTableModifyOpInput(ctx, spec)
  {}
  virtual ~ObTableMergeOpInput() {}
};

class ObTableMergeSpec : public ObTableModifySpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObTableMergeSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  //This interface is only allowed to be used in a single-table DML operator,
  //it is invalid when multiple tables are modified in one DML operator
  virtual int get_single_dml_ctdef(const ObDMLBaseCtDef *&dml_ctdef) const override;

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec,
                       K_(has_insert_clause),
                       K_(has_update_clause),
                       K_(delete_conds),
                       K_(update_conds),
                       K_(insert_conds),
                       K_(update_conds),
                       K_(distinct_key_exprs));
private:
  template <class T> inline int add_id_to_array(T& array, uint64_t id);

public:
  bool has_insert_clause_;
  bool has_update_clause_;
  ExprFixedArray delete_conds_;
  ExprFixedArray update_conds_;
  ExprFixedArray insert_conds_;
  // 堆表: distinct_key_exprs_ = rowkey + part_key
  // 其他表: distinct_key_exprs_ = rowkey
  ExprFixedArray distinct_key_exprs_;
  common::ObFixedArray<ObMergeCtDef*, common::ObIAllocator> merge_ctdefs_;

private:
  common::ObIAllocator &alloc_;
};

class ObTableMergeOp: public ObTableModifyOp
{
public:
  ObTableMergeOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObTableMergeOp() {}

  virtual int inner_open();
  virtual int inner_close();
  virtual int inner_rescan();
  int check_is_match(bool &is_match);
  int calc_condition(const ObIArray<ObExpr*> &exprs, bool &is_true_cond);
  int calc_delete_condition(const ObIArray<ObExpr*> &exprs, bool &is_true_cond);
  void release_merge_rtdef();

  // 这些接口仅仅兼容接口，不实际改动
  void inc_affected_rows() { }
  void inc_found_rows() { }
  void inc_changed_rows() { }
  int do_update();
  int update_row_das();
  int delete_row_das();
  int do_insert();
protected:
  int check_is_distinct(bool &conflict);

  // when update with partition key, we will delete row from old partition first,
  // then insert new row to new partition
  int calc_update_tablet_loc(const ObUpdCtDef &upd_ctdef,
                             ObUpdRtDef &upd_rtdef,
                             ObDASTabletLoc *&old_tablet_loc,
                             ObDASTabletLoc *&new_tablet_loc);

  int calc_insert_tablet_loc(const ObInsCtDef &ins_ctdef,
                             ObInsRtDef &ins_rtdef,
                             ObDASTabletLoc *&tablet_loc);
  int calc_delete_tablet_loc(const ObDelCtDef &del_ctdef,
                             ObDelRtDef &del_rtdef,
                             ObDASTabletLoc *&tablet_loc);
  int inner_open_with_das();
  int open_table_for_each();
  int close_table_for_each();
  virtual int write_row_to_das_buffer() override;
  virtual int write_rows_post_proc(int last_errno) override;
  virtual int check_need_exec_single_row() override;

protected:
  int64_t affected_rows_;
  common::ObArrayWrap<ObMergeRtDef> merge_rtdefs_;
};

template <class T>
int ObTableMergeSpec::add_id_to_array(T& array, uint64_t id)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!common::is_valid_id(id))){
    ret = common::OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(id));
  } else if (OB_FAIL(array.push_back(id))) {
    SQL_ENG_LOG(WARN, "fail to push back column id", K(ret));
  }
  return ret;
}

}//sql
}//oceanbase

#endif /* _SQL_ENGINE_DML_OB_TABLE_MERGE_OP_H */
