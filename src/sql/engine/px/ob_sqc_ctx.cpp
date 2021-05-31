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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/px/ob_sqc_ctx.h"
#include "lib/lock/ob_spin_lock.h"

using namespace oceanbase::sql;

int ObSqcCtx::add_whole_msg_provider(uint64_t op_id, ObPxDatahubDataProvider& provider)
{
  provider.op_id_ = op_id;
  return whole_msg_provider_list_.push_back(&provider);
}

int ObSqcCtx::get_whole_msg_provider(uint64_t op_id, ObPxDatahubDataProvider*& provider)
{
  int ret = OB_SUCCESS;
  provider = nullptr;
  for (int i = 0; OB_SUCC(ret) && i < whole_msg_provider_list_.count(); ++i) {
    if (OB_ISNULL(whole_msg_provider_list_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should never be nullptr, unexpected", K(ret));
    } else if (op_id == whole_msg_provider_list_.at(i)->op_id_) {
      provider = whole_msg_provider_list_.at(i);
      break;
    }
  }
  // EXPECTED: traversal operators and register provider when sqc is starting
  if (nullptr == provider) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should have a whole msg provider for op", K(op_id), K(ret));
  }
  return ret;
}
