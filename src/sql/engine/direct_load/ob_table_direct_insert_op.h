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

#ifndef OB_TABLE_DIRECT_INSERT_OP_H_
#define OB_TABLE_DIRECT_INSERT_OP_H_

#include "lib/container/ob_fixed_array.h"
#include "sql/engine/dml/ob_table_modify_op.h"
#include "sql/engine/pdml/static/ob_px_multi_part_modify_op.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "observer/table_load/ob_table_load_store.h"
#include "observer/table_load/ob_table_load_store_trans_px_writer.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTableCtx;
}
namespace sql
{
class ObTableDirectInsertOpInput : public ObPxMultiPartModifyOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObTableDirectInsertOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObPxMultiPartModifyOpInput(ctx, spec),
      error_code_(OB_SUCCESS)
  {}
  int init(ObTaskInfo &task_info) override
  {
    return ObPxMultiPartModifyOpInput::init(task_info);
  }
  void reset() override
  {
    ObPxMultiPartModifyOpInput::reset();
    error_code_ = OB_SUCCESS;
  }
  inline void set_error_code(const int error_code) { error_code_ = error_code; }
  inline int get_error_code() const { return error_code_; }
  TO_STRING_KV(K_(error_code));
private:
  int error_code_;
  DISALLOW_COPY_AND_ASSIGN(ObTableDirectInsertOpInput);
};

class ObTableDirectInsertSpec : public ObTableModifySpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObTableDirectInsertSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObTableModifySpec(alloc, type),
    row_desc_(),
    ins_ctdef_(alloc)
  {
  }
  //This interface is only allowed to be used in a single-table DML operator,
  //it is invalid when multiple tables are modified in one DML operator
  virtual int get_single_dml_ctdef(const ObDMLBaseCtDef *&dml_ctdef) const override
  {
    dml_ctdef = &ins_ctdef_;
    return common::OB_SUCCESS;
  }
  virtual bool is_pdml_operator() const override { return true; }
public:
  ObDMLOpRowDesc row_desc_;  // 记录partition id column所在row的第几个cell
  ObInsCtDef ins_ctdef_;
  DISALLOW_COPY_AND_ASSIGN(ObTableDirectInsertSpec);
};

class ObTableDirectInsertOp : public ObTableModifyOp
{
  OB_UNIS_VERSION(1);
public:
  ObTableDirectInsertOp(ObExecContext &exec_ctx,
                        const ObOpSpec &spec,
                        ObOpInput *input);
  virtual bool has_foreign_key() const  { return false; } // 默认实现，先不考虑外键的问题

  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;
private:
  int init_px_writer();
  int next_vector(const int64_t max_row_cnt);
  int next_batch(const int64_t max_row_cnt);
  int next_row();
  int write_vectors(common::ObIVector *tablet_id_vector,
                    const common::IVectorPtrs &vectors,
                    const sql::ObBatchRows &batch_rows);
  int write_batch(const common::ObDatumVector &tablet_id_datum_vector,
                  const common::ObIArray<common::ObDatumVector> &datum_vectors,
                  const sql::ObBatchRows &batch_rows);
  int write_row(const common::ObTabletID *tablet_id_ptr,
                const sql::ExprFixedArray &expr_array);
protected:
  ObInsRtDef ins_rtdef_;
  common::ObArenaAllocator allocator_;
  int64_t px_task_id_;
  int64_t ddl_task_id_;
  observer::ObTableLoadTableCtx *table_ctx_;
  blocksstable::ObDatumRow *datum_row_;
  observer::ObTableLoadStoreTransPXWriter *px_writer_;
  ObTabletID tablet_id_;
  bool is_partitioned_table_;
  DISALLOW_COPY_AND_ASSIGN(ObTableDirectInsertOp);
};


}  // namespace sql
}  // namespace oceanbase
#endif /* OB_TABLE_DIRECT_INSERT_OP_H_*/
