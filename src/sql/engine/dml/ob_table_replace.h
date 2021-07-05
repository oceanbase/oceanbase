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

#ifndef OCEANBASE_SQL_OB_TABLE_REPLACE_H
#define OCEANBASE_SQL_OB_TABLE_REPLACE_H
#include "sql/engine/dml/ob_table_modify.h"
namespace oceanbase {
namespace storage {
class ObTableScanParam;
class ObPartitionService;
}  // namespace storage

namespace share {
class ObPartitionLocation;
class ObPartitionReplicaLocation;
}  // namespace share

namespace sql {
class ObTableReplaceInput : public ObTableModifyInput {
  friend class ObTableUpdate;

public:
  ObTableReplaceInput() : ObTableModifyInput()
  {}
  virtual ~ObTableReplaceInput()
  {}
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_REPLACE;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableReplaceInput);
};

class ObTableReplace : public ObTableModify {
protected:
  class ObTableReplaceCtx : public ObTableModifyCtx {
  public:
    explicit ObTableReplaceCtx(ObExecContext& ctx)
        : ObTableModifyCtx(ctx),
          insert_row_(),
          record_(0),
          affected_rows_(0),
          delete_count_(0),
          dml_param_(),
          cast_row_()
    {}
    ~ObTableReplaceCtx()
    {}
    int init(int64_t insert_column_count)
    {
      return (alloc_row_cells(insert_column_count, insert_row_));
    }
    virtual void destroy()
    {
      ObTableModifyCtx::destroy();
      dml_param_.~ObDMLBaseParam();
      part_infos_.reset();
    }

  public:
    common::ObNewRow insert_row_;
    int64_t record_;
    int64_t affected_rows_;
    int64_t delete_count_;
    storage::ObDMLBaseParam dml_param_;
    common::ObSEArray<DMLPartInfo, 1> part_infos_;
    common::ObNewRow cast_row_;
  };

public:
  explicit ObTableReplace(common::ObIAllocator& alloc)
      : ObTableModify(alloc), only_one_unique_key_(false), res_obj_types_(alloc)
  {}
  virtual ~ObTableReplace()
  {}

  virtual void reset();
  virtual void reuse();
  void set_only_one_unique_key(bool only_one)
  {
    only_one_unique_key_ = only_one;
  }
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int64_t to_string_kv(char* buf, int64_t buf_len) const;
  virtual int add_filter(ObSqlExpression* expr);
  virtual int get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  virtual int init_op_ctx(ObExecContext& ctx) const;
  int init_column_res_type_count(int64_t count)
  {
    return init_array_size<>(res_obj_types_, count);
  }
  int add_column_res_type(const ObObjType type);
  OB_UNIS_VERSION_V(1);

protected:
  int do_table_replace(ObExecContext& ctx) const;
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  int try_insert(ObExecContext& ctx, common::ObExprCtx& expr_ctx, const common::ObNewRow* insert_row,
      const ObPartitionKey& part_key, const storage::ObDMLBaseParam& dml_param,
      common::ObNewRowIterator*& duplicated_rows) const;

  int do_replace(ObExecContext& ctx, const ObPartitionKey& part_key, const common::ObNewRowIterator* duplicated_rows,
      const storage::ObDMLBaseParam& dml_param) const;

  int scan_row(ObExecContext& ctx, const ObPartitionKey& part_key, storage::ObPartitionService* partition_service,
      storage::ObTableScanParam& scan_param, const common::ObNewRowIterator* duplicated_rows,
      common::ObNewRowIterator** result) const;
  int check_values(const common::ObNewRow& old_row, const common::ObNewRow& new_row, bool& is_equal) const;
  virtual int rescan(ObExecContext& ctx) const;
  /**
   * in the resolver stage, will set the result type of value_desc to the result type of
   * column_ref. causes a cast error during column convert, so here need to convert the
   * row to the output result type of child op
   */
  int do_type_cast(ObExecContext& ctx, const common::ObNewRow*& row) const;
  int shallow_copy_row(ObExecContext& ctx, const common::ObNewRow*& src_row, common::ObNewRow& dst_row) const;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableReplace);
  // function members
protected:
  bool only_one_unique_key_;
  common::ObFixedArray<ObObjType, common::ObIAllocator> res_obj_types_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif /* OCEANBASE_SQL_OB_TABLE_REPLACE_H */
