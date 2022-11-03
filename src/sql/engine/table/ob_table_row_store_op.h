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

#ifndef SQL_ENGINE_BASIC_OB_TABLE_ROW_STORE_
#define SQL_ENGINE_BASIC_OB_TABLE_ROW_STORE_
#include "lib/container/ob_array_wrap.h"
#include "common/row/ob_row_store.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_operator.h"

namespace oceanbase
{
namespace sql
{
class ObTableRowStoreOpInput : public ObOpInput
{
  friend class ObTableRowStoreOp;
  OB_UNIS_VERSION(1);
public:
  ObTableRowStoreOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObOpInput(ctx, spec),
      multi_row_store_(),
      allocator_(NULL)
  { }
  virtual ~ObTableRowStoreOpInput() {}

  virtual void reset() override
  {
    multi_row_store_.reset();
    //deserialize_allocator_ cannot be reset
    //because it is only set once when creating operator input
  }
  virtual int init(ObTaskInfo &task_info) override;
  /**
   * @brief set allocator which is used for deserialize, but not all objects will use allocator
   * while deserializing, so you can override it if you need.
   */
  virtual void set_deserialize_allocator(common::ObIAllocator *allocator);
private:
  //一个分区对应一个row store
  common::ObFixedArray<ObChunkDatumStore *, common::ObIAllocator> multi_row_store_;
  common::ObIAllocator *allocator_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableRowStoreOpInput);
};

class ObTableRowStoreSpec : public ObOpSpec
{
  OB_UNIS_VERSION(1);
public:
  ObTableRowStoreSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type),
      table_id_(common::OB_INVALID_ID)
  {}
  ~ObTableRowStoreSpec() {}
public:
  uint64_t table_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableRowStoreSpec);
};

class ObTableRowStoreOp : public ObOperator
{
public:
  ObTableRowStoreOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
      row_store_idx_(0)
  {}

  virtual ~ObTableRowStoreOp() { destroy(); }

  virtual void destroy() { ObOperator::destroy(); }
  int inner_rescan();
  /**
   * @brief called by get_next_row(), get a row from the child operator or row_store
   * @param ctx[in], execute context
   * @param row[out], ObSqlRow an obj array and row_size
   */
  virtual int inner_get_next_row();
  /**
   * @brief open operator, not including children operators.
   * called by open.
   * Every op should implement this method.
   */
  virtual int inner_open();
  /**
   * @brief close operator, not including children operators.
   * Every op should implement this method.
   */
  virtual int inner_close();
  virtual int64_t to_string_kv(char *buf, const int64_t buf_len);

  int fetch_stored_row();
private:
  ObChunkDatumStore::Iterator row_store_it_;
  int64_t row_store_idx_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableRowStoreOp);
};
}  // namespace sql
}  // namespace oceanbase
#endif /* SQL_ENGINE_BASIC_OB_TABLE_ROW_STORE_*/
