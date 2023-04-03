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

#ifndef OCEANBASE_SQL_ENGINE_DML_OB_TABLE_INSERET_ALL_OP_
#define OCEANBASE_SQL_ENGINE_DML_OB_TABLE_INSERET_ALL_OP_
#include "sql/engine/dml/ob_table_insert_op.h"
namespace oceanbase
{
namespace sql
{

struct InsertAllTableInfo
{
  OB_UNIS_VERSION(1);
public:
  InsertAllTableInfo(common::ObIAllocator &alloc)
  : match_conds_exprs_(alloc),
    match_conds_idx_(-1)
  {
  }

  TO_STRING_KV(K_(match_conds_exprs),
               K_(match_conds_idx));
  ExprFixedArray match_conds_exprs_;
  int64_t match_conds_idx_;//used to mark current table info is belong to insert all conditions
};

class ObTableInsertAllSpec :public ObTableInsertSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObTableInsertAllSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObTableInsertSpec(alloc, type),
      is_insert_all_with_conditions_(false),
      is_insert_all_first_(false),
      insert_table_infos_(alloc)
  {}

  INHERIT_TO_STRING_KV("ObTableInsertSpec", ObTableInsertSpec,
                       K_(is_insert_all_with_conditions),
                       K_(is_insert_all_first),
                       K_(insert_table_infos));

  bool is_insert_all_with_conditions_;
  bool is_insert_all_first_;
  common::ObFixedArray<InsertAllTableInfo*, common::ObIAllocator> insert_table_infos_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableInsertAllSpec);
};

class ObTableInsertAllOp : public ObTableInsertOp
{
  public:
  ObTableInsertAllOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input)
      : ObTableInsertOp(ctx, spec, input)
  {
  }
  virtual ~ObTableInsertAllOp() {};

  virtual int switch_iterator(ObExecContext &ctx);
  virtual void destroy() override
  {
    ObTableInsertOp::destroy();
  }

protected:
  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_close() override;
protected:
  virtual int write_row_to_das_buffer() override;
  virtual int check_need_exec_single_row() override;
private:
  int check_match_conditions(const int64_t tbl_idx,
                             const bool have_insert_row,
                             int64_t &pre_match_idx,
                             bool &no_need_insert,
                             bool &is_continued);

  int check_row_match_conditions(const ExprFixedArray &match_conds_exprs,
                                 bool &is_match);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableInsertAllOp);
};

class ObTableInsertAllOpInput: public ObTableInsertOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObTableInsertAllOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObTableInsertOpInput(ctx, spec)
  {}
  int init(ObTaskInfo &task_info) override
  {
    return ObTableInsertOpInput::init(task_info);
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableInsertAllOpInput);
};



}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_DML_OB_TABLE_INSERET_ALL_OP_ */
