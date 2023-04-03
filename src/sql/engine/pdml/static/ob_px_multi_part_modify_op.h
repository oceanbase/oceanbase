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

#ifndef _OB_SQL_ENGINE_PDML_PX_MULTI_PART_MODIFY_OP_H_
#define _OB_SQL_ENGINE_PDML_PX_MULTI_PART_MODIFY_OP_H_

#include "sql/engine/ob_operator.h"

namespace oceanbase
{
namespace sql
{
class ObDMLOpRowDesc
{
  OB_UNIS_VERSION(1);
public:
  ObDMLOpRowDesc()
    : part_id_index_(common::OB_INVALID_INDEX_INT64)
  {}
  ~ObDMLOpRowDesc() = default;

  bool is_valid() const
  { return common::OB_INVALID_INDEX_INT64 != part_id_index_; }
  void set_part_id_index(int64_t index)
  { part_id_index_ = index; }
  int64_t get_part_id_index() const
  { return part_id_index_; }

  TO_STRING_KV(K_(part_id_index));
private:
  //定义 part id 伪列在输入行中的偏移地址，用于从 row 中取 part_id 值
  int64_t part_id_index_;
};

class ObPxMultiPartModifyOpInput : public ObOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObPxMultiPartModifyOpInput(ObExecContext &ctx, const ObOpSpec &spec)
      : ObOpInput(ctx, spec)
  {
  }
  virtual int init(ObTaskInfo &task_info) override
  {
    UNUSED(task_info);
    return common::OB_SUCCESS;
  }
  virtual void reset() {}
  virtual void set_task_id(int64_t task_id) { task_id_ = task_id; }
  virtual void set_sqc_id(int64_t sqc_id) { sqc_id_ = sqc_id; }
  virtual void set_dfo_id(int64_t dfo_id) { dfo_id_ = dfo_id; }
  int64_t get_task_id() const { return task_id_; }
  int64_t get_sqc_id() const { return sqc_id_; }
  int64_t get_dfo_id() const { return dfo_id_; }

protected:
  int64_t task_id_;
  int64_t sqc_id_;
  int64_t dfo_id_;

  DISALLOW_COPY_AND_ASSIGN(ObPxMultiPartModifyOpInput);
};

}
}
#endif /* _OB_SQL_ENGINE_PDML_PX_MULTI_PART_MODIFY_OP_H_ */
//// end of header file
