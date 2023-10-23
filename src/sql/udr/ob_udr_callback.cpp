/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_QRR
#include "sql/udr/ob_udr_callback.h"

namespace oceanbase
{
namespace sql
{

int ObUDRAtomicOp::get_value(ObUDRItemMgr::UDRKeyNodePair *&rule_node)
{
  int ret = OB_SUCCESS;
  rule_node = nullptr;
  if (OB_ISNULL(rule_node_)) {
    ret = OB_NOT_INIT;
   LOG_WARN("invalid argument", K(rule_node_));
  } else if (OB_SUCC(lock(*rule_node_))) {
    rule_node = rule_node_;
  } else {
    if (NULL != rule_node_) {
      rule_node_->dec_ref_count();
      rule_node_ = NULL;
    }
  }
  return ret;
}

void ObUDRAtomicOp::operator()(RuleItemKV &entry)
{
  if (NULL != entry.second) {
    entry.second->inc_ref_count();
    rule_node_ = entry.second;
  }
}

ObUDRWlockAndRefGuard::~ObUDRWlockAndRefGuard()
{
  if (NULL != rule_node_) {
    rule_node_->unlock();
    rule_node_->dec_ref_count();
  }
}

ObUDRRlockAndRefGuard::~ObUDRRlockAndRefGuard()
{
  if (NULL != rule_node_) {
    rule_node_->unlock();
    rule_node_->dec_ref_count();
  }
}

} // namespace sql end
} // namespace oceanbase end
