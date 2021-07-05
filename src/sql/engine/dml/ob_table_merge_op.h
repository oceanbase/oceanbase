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
#include "storage/ob_dml_param.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
class ObDMLBaseParam;
}  // namespace storage
namespace sql {

class ObTableMergeOpInput : public ObTableModifyOpInput {
  OB_UNIS_VERSION_V(1);

public:
  ObTableMergeOpInput(ObExecContext& ctx, const ObOpSpec& spec) : ObTableModifyOpInput(ctx, spec)
  {}
  virtual ~ObTableMergeOpInput()
  {}
};

class ObTableMergeSpec : public ObTableModifySpec {
  OB_UNIS_VERSION_V(1);

public:
  ObTableMergeSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type);

  INHERIT_TO_STRING_KV("op_spec", ObOpSpec, K_(has_insert_clause), K_(has_update_clause), K_(delete_conds),
      K_(update_conds), K_(insert_conds), K_(update_conds), K_(rowkey_exprs));

private:
  template <class T>
  inline int add_id_to_array(T& array, uint64_t id);

public:
  inline int add_delete_column_id(uint64_t column_id)
  {
    return add_id_to_array(delete_column_ids_, column_id);
  }
  int set_updated_column_info(
      int64_t array_index, uint64_t column_id, uint64_t project_index, bool auto_filled_timestamp);
  int init_updated_column_count(common::ObIAllocator& allocator, int64_t count);

public:
  bool has_insert_clause_;
  bool has_update_clause_;

  ExprFixedArray delete_conds_;
  ExprFixedArray update_conds_;
  ExprFixedArray insert_conds_;

  ExprFixedArray rowkey_exprs_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> delete_column_ids_;

  common::ObFixedArray<uint64_t, common::ObIAllocator> updated_column_ids_;
  common::ObFixedArray<ColumnContent, common::ObIAllocator> updated_column_infos_;
  ExprFixedArray old_row_;
  ExprFixedArray new_row_;
};

////////////////////////////////////// ObTableMergeOp //////////////////////////////////////
class ObTableMergeOp : public ObTableModifyOp {
public:
  ObTableMergeOp(ObExecContext& ctx, const ObOpSpec& spec, ObOpInput* input);
  virtual ~ObTableMergeOp()
  {}

  virtual int inner_open();
  virtual int inner_close();
  virtual int rescan();
  virtual int inner_get_next_row();
  virtual void destroy()
  {
    insert_row_store_.reset();
    affected_rows_ = 0;
    ObTableModifyOp::destroy();
  }
  int check_is_match(bool& is_match);
  int calc_condition(const ObIArray<ObExpr*>& exprs, bool& is_true_cond);
  int calc_delete_condition(const ObIArray<ObExpr*>& exprs, bool& is_true_cond);
  int process_insert(storage::ObPartitionService* partition_service, const transaction::ObTransDesc& trans_desc,
      const storage::ObDMLBaseParam& dml_param, const common::ObPartitionKey& pkey);
  int insert_all_rows(storage::ObPartitionService* partition_service, const transaction::ObTransDesc& trans_desc,
      const storage::ObDMLBaseParam& dml_param, const common::ObPartitionKey& pkey);

  void inc_affected_rows()
  {}
  void inc_found_rows()
  {}
  void inc_changed_rows()
  {}

protected:
  int generate_origin_row(bool& conflict);

private:
  int process_update(storage::ObPartitionService* partition_service, const common::ObPartitionKey& pkey,
      storage::ObDMLBaseParam& dml_param);
  int update_row(storage::ObPartitionService* partition_service, const common::ObPartitionKey& pkey,
      storage::ObDMLBaseParam& dml_param);
  int lock_row(storage::ObPartitionService* partition_service, const common::ObPartitionKey& pkey,
      storage::ObDMLBaseParam& dml_param);
  void reset()
  {
    affected_rows_ = 0;
    part_infos_.reset();
  }
  int do_table_merge();

protected:
  common::ObNewRow insert_row_;
  common::ObRowStore insert_row_store_;
  common::ObNewRow old_row_;
  common::ObNewRow new_row_;
  int64_t affected_rows_;
  storage::ObDMLBaseParam dml_param_;
  common::ObSEArray<DMLPartInfo, 1> part_infos_;
};

template <class T>
int ObTableMergeSpec::add_id_to_array(T& array, uint64_t id)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!common::is_valid_id(id))) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(id));
  } else if (OB_FAIL(array.push_back(id))) {
    SQL_ENG_LOG(WARN, "fail to push back column id", K(ret));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

#endif /* _SQL_ENGINE_DML_OB_TABLE_MERGE_OP_H */
