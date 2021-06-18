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

#include "sql/engine/pdml/ob_px_multi_part_modify.h"

namespace oceanbase {
namespace sql {
int64_t ObDMLTableDesc::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(index_tid), K_(partition_cnt));
  J_OBJ_END();
  return pos;
}
int64_t ObDMLRowDesc::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(part_id_index));
  J_OBJ_END();
  return pos;
}
OB_SERIALIZE_MEMBER(ObDMLTableDesc, index_tid_, partition_cnt_);
OB_SERIALIZE_MEMBER(ObDMLRowDesc, part_id_index_);
OB_SERIALIZE_MEMBER(ObPxModifyInput, task_id_, sqc_id_, dfo_id_);

int ObPxModifyInput::init(ObExecContext& ctx, ObTaskInfo& task_info, const ObPhyOperator& op)
{
  int ret = common::OB_SUCCESS;
  UNUSED(ctx);
  UNUSED(task_info);
  UNUSED(op);
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
