// Copyright 2015-2016 Alibaba Inc. All Rights Reserved.
// Author:
//     LuoFan 
// Normalizer:
//     LuoFan 

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
    rule_node_->dec_ref_count();
    rule_node_->unlock();
  }
}

ObUDRRlockAndRefGuard::~ObUDRRlockAndRefGuard()
{
  if (NULL != rule_node_) {
    rule_node_->dec_ref_count();
    rule_node_->unlock();
  }
}

} // namespace sql end
} // namespace oceanbase end
