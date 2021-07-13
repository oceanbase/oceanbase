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

#ifndef __OB_SQL_ENGINE_PDML_PX_MULTI_PART_MODIFY_H__
#define __OB_SQL_ENGINE_PDML_PX_MULTI_PART_MODIFY_H__

#include "lib/utility/ob_unify_serialize.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/engine/ob_phy_operator.h"

namespace oceanbase {
namespace sql {

class ObDMLTableDesc {
  OB_UNIS_VERSION(1);

public:
  ObDMLTableDesc() : index_tid_(common::OB_INVALID_ID), partition_cnt_(-1)
  {}
  ~ObDMLTableDesc() = default;
  bool is_valid() const
  {
    return common::OB_INVALID_ID != index_tid_ && 0 <= partition_cnt_;
  }
  TO_STRING_KV(K_(index_tid), K_(partition_cnt));

public:
  // index table's physical table_id
  uint64_t index_tid_;
  // partition counts recorded in schema
  int64_t partition_cnt_;
};

class ObDMLRowDesc {
  OB_UNIS_VERSION(1);

public:
  ObDMLRowDesc() : part_id_index_(common::OB_INVALID_INDEX_INT64)
  {}
  ~ObDMLRowDesc() = default;

  bool is_valid() const
  {
    return common::OB_INVALID_INDEX_INT64 != part_id_index_;
  }
  void set_part_id_index(int64_t index)
  {
    part_id_index_ = index;
  }
  int64_t get_part_id_index() const
  {
    return part_id_index_;
  }

  TO_STRING_KV(K_(part_id_index));

private:
  int64_t part_id_index_;
};

class ObPxModifyInput : public ObIPhyOperatorInput {
  OB_UNIS_VERSION_V(1);

public:
  ObPxModifyInput() : task_id_(common::OB_INVALID_ID), sqc_id_(common::OB_INVALID_ID), dfo_id_(common::OB_INVALID_ID)
  {}
  virtual ~ObPxModifyInput() = default;
  virtual void reset() override
  { /*@TODO fix reset member*/
  }
  virtual int init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op);
  virtual void set_task_id(int64_t task_id)
  {
    task_id_ = task_id;
  }
  virtual void set_sqc_id(int64_t sqc_id)
  {
    sqc_id_ = sqc_id;
  }
  virtual void set_dfo_id(int64_t dfo_id)
  {
    dfo_id_ = dfo_id;
  }
  int64_t get_task_id() const
  {
    return task_id_;
  }
  int64_t get_sqc_id() const
  {
    return sqc_id_;
  }
  int64_t get_dfo_id() const
  {
    return dfo_id_;
  }

protected:
  int64_t task_id_;
  int64_t sqc_id_;
  int64_t dfo_id_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* __OB_SQL_ENGINE_PDML_PX_MULTI_PART_MODIFY_H__ */
//// end of header file
