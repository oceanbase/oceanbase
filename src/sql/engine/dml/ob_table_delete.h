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

#ifndef OCEANBASE_SQL_ENGINE_DML_OB_TABLE_DELETE_H_
#define OCEANBASE_SQL_ENGINE_DML_OB_TABLE_DELETE_H_
#include "sql/engine/dml/ob_table_modify.h"
#include "sql/engine/ob_exec_context.h"
namespace oceanbase {
namespace sql {
class ObTableDeleteInput : public ObTableModifyInput {
  friend class ObTableDelete;

public:
  ObTableDeleteInput() : ObTableModifyInput()
  {}
  virtual ~ObTableDeleteInput()
  {}
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_DELETE;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableDeleteInput);
};

class ObTableDelete : public ObTableModify {
protected:
  class ObTableDeleteCtx : public ObTableModifyCtx {
    friend class ObTableDelete;

  public:
    explicit ObTableDeleteCtx(ObExecContext& ctx) : ObTableModifyCtx(ctx), part_row_cnt_(0), dml_param_()
    {}
    ~ObTableDeleteCtx()
    {}
    virtual void destroy()
    {
      ObTableModifyCtx::destroy();
      dml_param_.~ObDMLBaseParam();
      part_infos_.reset();
    }

  protected:
    int64_t part_row_cnt_;
    storage::ObDMLBaseParam dml_param_;
    common::ObSEArray<DMLPartInfo, 4> part_infos_;
  };

public:
  explicit ObTableDelete(common::ObIAllocator& alloc);
  ~ObTableDelete();

  int add_compute(ObColumnExpression* expr);

protected:
  /**
   * @brief called by get_next_row(), get a row from the child operator or row_store
   * @param ctx[in], execute context
   * @param row[out], ObSqlRow an obj array and row_size
   */
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open(ObExecContext& ctx) const;

private:
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int rescan(ObExecContext& ctx) const;
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;
  inline int delete_rows(ObExecContext& ctx, storage::ObDMLBaseParam& dml_param,
      const common::ObIArray<DMLPartInfo>& part_infos, int64_t& affected_rows) const;
  inline int do_table_delete(ObExecContext& ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableDelete);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_DML_OB_TABLE_DELETE_H_ */
