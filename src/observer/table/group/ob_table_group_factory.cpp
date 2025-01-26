/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SERVER

#include "ob_table_group_factory.h"
namespace oceanbase
{

namespace table
{
int ObTableGroupOpFactory::alloc(ObTableGroupType op_type, ObITableOp *&op)
{
  int ret = OB_SUCCESS;
  if (op_type < 0 || op_type >= ObTableGroupType::TYPE_MAX) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "invalid op type", K(ret), K(op_type));
  } else {
    ObLockGuard<ObSpinLock> guard(locks_[op_type]);
    op = nullptr;
    op = free_list_[op_type].remove_first();
    if (OB_ISNULL(op)) {
      if (OB_FAIL(GROUP_OP_ALLOC[op_type](allocator_, op))) {
        LOG_WARN("fail to alloc op", K(ret), K(op_type));
      }
    }
    used_list_[op_type].add_last(op);
  }
  return ret;
}

void ObTableGroupOpFactory::free(ObITableOp *op)
{
 if (NULL != op) {
    ObTableGroupType op_type = op->type();
    if (op_type > 0 && op_type < ObTableGroupType::TYPE_MAX) {
      op->reset();
      ObLockGuard<ObSpinLock> guard(locks_[op_type]);
      used_list_[op_type].remove(op);
      free_list_[op_type].add_last(op);
    } else {
      LOG_DEBUG("[ObTableGroupOpFactory] invalid op type", K(op_type));
    }
  }
}

void ObTableGroupOpFactory::free_and_reuse(ObTableGroupType type)
{
  if (type > 0 && type < ObTableGroupType::TYPE_MAX) {
    while(!used_list_[type].is_empty()) {
      this->free(used_list_[type].get_first());
    }
  } else {
    LOG_DEBUG("[ObTableGroupOpFactory] invalid op type", K(type));
  }
}

void ObTableGroupOpFactory::free_all()
{
  for (int64_t i = 0; i < ObTableGroupType::TYPE_MAX; i++) {
    this->free_and_reuse(static_cast<ObTableGroupType>(i));
  }
}
} // end namespace table
} // end namespace oceanbase
