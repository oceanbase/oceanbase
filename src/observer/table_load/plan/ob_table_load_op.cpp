/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/plan/ob_table_load_op.h"

namespace oceanbase
{
namespace observer
{
using namespace common;

ObTableLoadOp::ObTableLoadOp(ObTableLoadPlan *plan)
  : plan_(plan),
    op_type_(ObTableLoadOpType::INVALID_OP_TYPE),
    parent_(nullptr),
    start_time_(0),
    end_time_(0)
{
  childs_.set_block_allocator(ModulePageAllocator(plan_->get_allocator()));
}

ObTableLoadOp::ObTableLoadOp(ObTableLoadOp *parent)
  : plan_(parent->plan_),
    op_type_(ObTableLoadOpType::INVALID_OP_TYPE),
    parent_(parent),
    start_time_(0),
    end_time_(0)
{
  childs_.set_block_allocator(ModulePageAllocator(plan_->get_allocator()));
}

int64_t ObTableLoadOp::simple_to_string(char *buf, int64_t buf_len, const bool show_childs) const
{
  int64_t pos = 0;
  J_OBJ_START();
  databuff_printf(buf, buf_len, pos, "%p", reinterpret_cast<const void *>(this));
  J_COMMA();
  databuff_printf(buf, buf_len, pos, "%s", ObTableLoadOpType::get_type_string(op_type_));
  if (show_childs && !childs_.empty()) {
    J_COMMA();
    J_NAME("childs");
    J_COLON();
    J_ARRAY_START();
    databuff_printf(buf, buf_len, pos, "cnt:%ld, ", childs_.count());
    for (int64_t i = 0; i < childs_.count(); ++i) {
      ObTableLoadOp *child = childs_.at(i);
      if (0 == i) {
        databuff_printf(buf, buf_len, pos, "%ld:", i);
      } else {
        databuff_printf(buf, buf_len, pos, ", %ld:", i);
      }
      pos += child->simple_to_string(buf + pos, buf_len - pos);
    }
    J_ARRAY_END();
  }
  J_OBJ_END();
  return pos;
}

} // namespace observer
} // namespace oceanbase
