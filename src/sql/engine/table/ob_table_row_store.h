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

#ifndef SQL_ENGINE_BASIC_OB_MULTI_VALUES_H_
#define SQL_ENGINE_BASIC_OB_MULTI_VALUES_H_
#include "lib/container/ob_array_wrap.h"
#include "common/row/ob_row_store.h"
#include "sql/engine/ob_no_children_phy_operator.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
namespace sql {
class ObTableRowStoreInput : public ObIPhyOperatorInput {
  friend class ObTableRowStore;
  OB_UNIS_VERSION_V(1);

public:
  ObTableRowStoreInput() : multi_row_store_(), allocator_(NULL)
  {}
  virtual ~ObTableRowStoreInput()
  {}
  virtual void reset() override
  {
    multi_row_store_.reset();
    // deserialize_allocator_ cannot be reset because it is only set once when creating operator input
  }
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op);
  virtual ObPhyOperatorType get_phy_op_type() const
  {
    return PHY_TABLE_ROW_STORE;
  }
  /**
   * @brief set allocator which is used for deserialize, but not all objects will use allocator
   * while deserializing, so you can override it if you need.
   */
  virtual void set_deserialize_allocator(common::ObIAllocator* allocator);

private:
  common::ObFixedArray<common::ObRowStore*, common::ObIAllocator> multi_row_store_;
  common::ObIAllocator* allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableRowStoreInput);
};

class ObTableRowStore : public ObNoChildrenPhyOperator {
  OB_UNIS_VERSION(1);
  class ObTableRowStoreCtx;

public:
  explicit ObTableRowStore(common::ObIAllocator& alloc)
      : ObNoChildrenPhyOperator(alloc), table_id_(common::OB_INVALID_ID)
  {}
  ~ObTableRowStore()
  {}

  int rescan(ObExecContext& ctx) const;
  inline void set_table_id(uint64_t table_id)
  {
    table_id_ = table_id;
  }
  inline uint64_t get_table_id() const
  {
    return table_id_;
  }
  virtual int create_operator_input(ObExecContext& ctx) const;

private:
  /**
   * @brief init operator context, will create a physical operator context (and a current row space)
   * @param ctx[in], execute context
   * @return if success, return OB_SUCCESS, otherwise, return errno
   */
  virtual int init_op_ctx(ObExecContext& ctx) const;
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
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close(ObExecContext& ctx) const;
  virtual int64_t to_string_kv(char* buf, const int64_t buf_len) const;
  DISALLOW_COPY_AND_ASSIGN(ObTableRowStore);

private:
  uint64_t table_id_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* SQL_ENGINE_BASIC_OB_MULTI_VALUES_H_ */
