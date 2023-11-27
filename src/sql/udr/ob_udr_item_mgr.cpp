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
#include "sql/udr/ob_udr_item_mgr.h"

namespace oceanbase
{
namespace sql
{

struct DecAllRuleNodeRefFunc
{
public:
  DecAllRuleNodeRefFunc() {}
  int operator()(const hash::HashMapPair<ObUDRItemMgr::UDRKey, ObUDRItemMgr::UDRKeyNodePair*> &kv)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(kv.second)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("is null", K(ret));
    } else {
      kv.second->dec_ref_count();
    }
    return ret;
  }
};

struct MatchSamePatternDigest
{
public:
  MatchSamePatternDigest(const uint64_t pattern_digest) :
    is_exists_(false), pattern_digest_(pattern_digest)
  {}
  int operator()(
    const hash::HashMapPair<ObUDRItemMgr::UDRKey, ObUDRItemMgr::UDRKeyNodePair *> &entry)
  {
    int ret = OB_SUCCESS;
    if (!is_exists_ && pattern_digest_ == entry.first.pattern_digest_) {
      is_exists_ = true;
    }
    return ret;
  }
  bool has_same_pattern_digest() const
  {
    return is_exists_;
  }

public:
  bool is_exists_;
  uint64_t pattern_digest_;
};

int ObUDRItemMgr::UDRKey::deep_copy(common::ObIAllocator &allocator, const UDRKey &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ob_write_string(allocator, other.db_name_, db_name_))) {
    LOG_WARN("failed to deep copy db name", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, other.normalized_pattern_, normalized_pattern_))) {
    LOG_WARN("failed to deep copy normalized pattern", K(ret));
  } else {
    tenant_id_ = other.tenant_id_;
    pattern_digest_ = other.pattern_digest_;
  }
  return ret;
}

void ObUDRItemMgr::UDRKey::destory(common::ObIAllocator &allocator)
{
#define SAFE_FREE_STR(name)     \
if (NULL != name.ptr()) {       \
  allocator.free(name.ptr());   \
  name.reset();                 \
}
  tenant_id_ = OB_INVALID_ID;
  pattern_digest_ = 0;
  SAFE_FREE_STR(db_name_);
  SAFE_FREE_STR(normalized_pattern_);
#undef SAFE_FREE_STR
}

int ObUDRItemMgr::UDRKeyNodePair::lock(bool is_rdlock)
{
  int ret = OB_SUCCESS;
  if (is_rdlock) {
    if (OB_FAIL(ref_lock_.rdlock())) {
      LOG_WARN("failed to read lock", K(ret));
    }
  } else {
    if (OB_FAIL(ref_lock_.wrlock())) {
      LOG_WARN("failed to write lock", K(ret));
    }
  }
  return ret;
}

void ObUDRItemMgr::UDRKeyNodePair::reset()
{
  ObUDRItem *item = nullptr;
  while (!rule_item_list_.empty()) {
    rule_item_list_.pop_front(item);
    if (OB_ISNULL(item)) {
      //do nothing
    } else {
      item->dec_ref_count();
      item = nullptr;
    }
  }
  rule_key_.destory(allocator_);
  rule_item_list_.reset();
}

int64_t ObUDRItemMgr::UDRKeyNodePair::inc_ref_count()
{
  return ATOMIC_AAF(&ref_count_, 1);
}

int64_t ObUDRItemMgr::UDRKeyNodePair::dec_ref_count()
{
  int64_t ref_count = ATOMIC_SAF(&ref_count_, 1);
  if (ref_count > 0) {
    // do nothing
  } else if (0 == ref_count) {
    LOG_DEBUG("remove rule node", K(ref_count), K(this));
    this->~UDRKeyNodePair();
    allocator_.free(this);// I'm sure this is the last line, so it's safe here
  } else {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "invalid ref count", K(ref_count));
  }
  return ref_count;
}

int ObUDRItemMgr::UDRKeyNodePair::deep_copy_rule_key(const UDRKey &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rule_key_.deep_copy(allocator_, other))) {
    LOG_WARN("failed to deep copy rule key", K(ret));
  }
  return ret;
}

int ObUDRItemMgr::UDRKeyNodePair::add_rule_item(ObUDRItem *rule_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rule_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is null", K(ret));
  } else if (OB_FAIL(rule_item_list_.push_back(rule_item))) {
    LOG_WARN("failed to add rule item", K(ret));
  } else {
    rule_item->inc_ref_count();
  }
  return ret;
}

int ObUDRItemMgr::UDRKeyNodePair::remove_rule_item(const int64_t rule_id)
{
  int ret = OB_SUCCESS;
  RuleItemList::iterator iter = rule_item_list_.begin();
  for (; OB_SUCC(ret) && iter != rule_item_list_.end(); iter++) {
    ObUDRItem *item = *iter;
    if (rule_id == item->get_rule_id()) {
      if (OB_FAIL(rule_item_list_.erase(iter))) {
        LOG_WARN("failed to erase rule item", K(ret));
      } else {
        item->dec_ref_count();
      }
      break;
    }
  }
  return ret;
}

int ObUDRItemMgr::UDRKeyNodePair::get_rule_item_by_id(const int64_t rule_id,
                                                      ObUDRItem *&rule_item)
{
  int ret = OB_SUCCESS;
  rule_item = NULL;
  RuleItemList::iterator iter = rule_item_list_.begin();
  for (; OB_SUCC(ret) && iter != rule_item_list_.end(); iter++) {
    ObUDRItem *item = *iter;
    if (rule_id == item->get_rule_id()) {
      rule_item = item;
      break;
    }
  }
  return ret;
}

ObUDRItemMgr::~ObUDRItemMgr()
{
  destroy();
}

void ObUDRItemMgr::destroy()
{
  reuse();
  inited_ = false;
}

int ObUDRItemMgr::reuse()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    DecAllRuleNodeRefFunc callback;
    if (OB_FAIL(rule_key_node_map_.foreach_refactored(callback))) {
      LOG_WARN("traversal rule_key_node_map_ failed", K(ret));
    }
  }
  rule_key_node_map_.reuse();
  return ret;
}

int ObUDRItemMgr::init(uint64_t tenant_id, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int bucket_size = 40960;
  ObMemAttr attr(tenant_id, "RewriteRuleMap");
  if (OB_UNLIKELY(rule_key_node_map_.created())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(rule_key_node_map_.create(bucket_size, attr, attr))) {
    LOG_WARN("failed create rule map", K(ret));
  } else {
    inited_ = true;
    tenant_id_ = tenant_id;
    allocator_ = &allocator;
  }
  LOG_INFO("init rewrite rule item mapping manager", K(ret));
  return ret;
}

template<typename T,
         typename ...Args>
int ObUDRItemMgr::alloc(UDRRefObjGuard<T> &guard, Args&& ...args)
{
  int ret = OB_SUCCESS;
  char *ptr = NULL;
  if (NULL == (ptr = static_cast<char *>(allocator_->alloc(sizeof(T))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to allocate memory for lib cache node", K(ret));
  } else {
    guard.ref_obj_ = new(ptr) T(std::forward<Args>(args)...);
    guard.ref_obj_->inc_ref_count();
  }
  return ret;
}

template<typename Info>
void ObUDRItemMgr::cons_shallow_copy_rule_key(const Info &rule_info,
                                              UDRKey &rule_key) const
{
  rule_key.tenant_id_ = rule_info.tenant_id_;
  rule_key.pattern_digest_ = rule_info.pattern_digest_;
  rule_key.db_name_ = rule_info.db_name_;
  rule_key.normalized_pattern_ = rule_info.normalized_pattern_;
}

int ObUDRItemMgr::get_value(const UDRKey &rule_key,
                            UDRKeyNodePair *&rule_node,
                            ObUDRAtomicOp &op)
{
  int ret = OB_SUCCESS;
  int hash_err = rule_key_node_map_.read_atomic(rule_key, op);
  switch (hash_err) {
    case OB_SUCCESS: {
      if (OB_FAIL(op.get_value(rule_node))) {
        LOG_DEBUG("failed to lock rule node", K(ret), K(rule_key));
      }
      break;
    }
    case OB_HASH_NOT_EXIST: {
      LOG_DEBUG("entry does not exist.", K(rule_key));
      break;
    }
    default: {
      LOG_WARN("failed to get cache node", K(ret), K(rule_key));
      ret = hash_err;
      break;
    }
  }
  return ret;
}

int ObUDRItemMgr::cons_deep_copy_rule_item(const ObUDRInfo &rule_info,
                                           ObUDRItem &rule_item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rule_item.deep_copy(rule_info))) {
    LOG_WARN("failed to deep copy rule info", K(ret));
  } else if (OB_FAIL(rule_item.deserialize_fixed_param_info_array(rule_info.fixed_param_infos_str_))) {
    LOG_WARN("failed to deserialize_fixed_param_info_array", K(ret));
  } else if (OB_FAIL(rule_item.deserialize_dynamic_param_info_array(rule_info.dynamic_param_infos_str_))) {
    LOG_WARN("failed to deserialize_dynamic_param_info_array", K(ret));
  } else if (OB_FAIL(rule_item.deserialize_question_mark_by_name_ctx(rule_info.question_mark_ctx_str_))) {
    LOG_WARN("failed to deserialize_question_mark_by_name_ctx", K(ret));
  }
  return ret;
}

int ObUDRItemMgr::add_rule_node(const UDRKey &rule_key, UDRKeyNodePair *rule_node)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(rule_key_node_map_.set_refactored(rule_key, rule_node))) {
    LOG_WARN("failed to add rule node", K(ret), K(rule_key));
  } else {
    rule_node->inc_ref_count();
  }
  return ret;
}

int ObUDRItemMgr::add_item(const UDRKey &rule_key, const ObUDRInfo &rule_info)
{
  int ret = OB_SUCCESS;
  UDRKeyNodePair *rule_node = nullptr;
  UDRItemRefGuard rule_item_guard;
  ObUDRWlockAndRefGuard w_ref_lock_guard;
  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("allocator is null", K(ret));
  } else if (OB_FAIL(alloc<ObUDRItem>(rule_item_guard, *allocator_))
    || OB_ISNULL(rule_item_guard.ref_obj_)) {
    LOG_WARN("failed to alloc rule item", K(ret));
  } else if (OB_FAIL(cons_deep_copy_rule_item(rule_info, *rule_item_guard.ref_obj_))) {
    LOG_WARN("failed to construct deep copy rule item", K(ret), K(rule_info));
  } else if (OB_FAIL(get_value(rule_key, rule_node, w_ref_lock_guard))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get rule node", K(ret));
  } else if (nullptr == rule_node) {
    UDRRefObjGuard<UDRKeyNodePair> rule_node_guard;
    if (OB_FAIL(alloc<UDRKeyNodePair>(rule_node_guard, *allocator_))
      || OB_ISNULL(rule_node_guard.ref_obj_)) {
      LOG_WARN("failed to alloc rule node", K(ret));
    } else if (OB_FAIL(rule_node_guard.ref_obj_->deep_copy_rule_key(rule_key))) {
      LOG_WARN("failed to construct deep copy rule key", K(ret), K(rule_key));
    } else if (OB_FAIL(rule_node_guard.ref_obj_->add_rule_item(rule_item_guard.ref_obj_))) {
      LOG_WARN("failed to add rule item", K(ret));
    } else if (OB_FAIL(add_rule_node(rule_node_guard.ref_obj_->rule_key_,
                                     rule_node_guard.ref_obj_))) {
      LOG_WARN("failed to add item", K(ret), K(rule_key), K(rule_info));
    }
  } else {
    if (OB_FAIL(rule_node->add_rule_item(rule_item_guard.ref_obj_))) {
      LOG_WARN("failed to add rule item", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succ to add item", K(rule_key),
    KPC(rule_item_guard.ref_obj_), K(rule_item_guard.ref_obj_->get_ref_count()));
  }
  return ret;
}

int ObUDRItemMgr::remove_rule_node(const UDRKey &rule_key)
{
  int ret = OB_SUCCESS;
  UDRKeyNodePair *del_node = nullptr;
  if (OB_FAIL(rule_key_node_map_.erase_refactored(rule_key, &del_node))) {
    LOG_WARN("failed to erase rule item", K(ret), K(rule_key));
  } else if (OB_ISNULL(del_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("is null", K(ret));
  } else {
    del_node->dec_ref_count();
  }
  return ret;
}

int ObUDRItemMgr::erase_item(const UDRKey &rule_key,  const int64_t rule_id)
{
  int ret = OB_SUCCESS;
  ObUDRItem *rule_item = nullptr;
  UDRKeyNodePair *rule_node = nullptr;
  ObUDRWlockAndRefGuard w_ref_lock_guard;
  if (OB_FAIL(get_value(rule_key, rule_node, w_ref_lock_guard))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get rule node", K(ret));
  } else if (nullptr == rule_node) {
    ret = OB_SUCCESS;
    LOG_INFO("item already not exist", K(rule_key));
  } else {
    if (OB_FAIL(rule_node->remove_rule_item(rule_id))) {
      LOG_WARN("failed to remove rule item", K(ret));
    } else if (rule_node->is_empty() && OB_FAIL(remove_rule_node(rule_key))) {
      LOG_WARN("failed to remove rule node", K(ret), K(rule_key));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succ to erase item", K(rule_key));
  }
  return ret;
}

int ObUDRItemMgr::match_fixed_param_list(const ObUDRContext &rule_ctx,
                                         const ObUDRItem *rule_item,
                                         bool &is_match) const
{
  int ret = OB_SUCCESS;
  is_match = true;
  const FixedParamValueArray &fixed_param_list = rule_item->get_fixed_param_value_array();
  for (int64_t i = 0; OB_SUCC(ret) && is_match && i < fixed_param_list.count(); ++i) {
    ParseNode *raw_param = NULL;
    ObPCParam *pc_param = NULL;
    const FixedParamValue &fixed_param = fixed_param_list.at(i);
    if (fixed_param.idx_ >= rule_ctx.raw_param_list_.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid idx", K(fixed_param.idx_), K(rule_ctx.raw_param_list_.count()));
    } else if (OB_ISNULL(pc_param = rule_ctx.raw_param_list_.at(fixed_param.idx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("is null", K(pc_param));
    } else if (NULL == (raw_param = pc_param->node_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("is null", K(raw_param));
    } else if (0 != fixed_param.raw_text_.compare(ObString(raw_param->text_len_, raw_param->raw_text_))) {
      is_match = false;
    }
  }
  return ret;
}

int ObUDRItemMgr::match_rule_item(const ObUDRContext &rule_ctx,
                                  const ObUDRItem *rule_item,
                                  bool &is_match) const
{
  int ret = OB_SUCCESS;
  is_match = false;
  if (rule_item->get_rule_status() != ObUDRInfo::ENABLE_STATUS) {
    LOG_DEBUG("invalid rule item", K(rule_item->get_rule_status()));
  } else if (OB_FAIL(match_fixed_param_list(rule_ctx, rule_item, is_match))) {
    LOG_WARN("failed to match fixed param list", K(ret));
  }
  return ret;
}

int ObUDRItemMgr::fuzzy_check_by_pattern_digest(const uint64_t pattern_digest,
                                                bool &is_exists)
{
  int ret = OB_SUCCESS;
  is_exists = false;
  MatchSamePatternDigest callback(pattern_digest);
  if (OB_FAIL(rule_key_node_map_.foreach_refactored(callback))) {
    LOG_WARN("traversal rule_key_node_map_ failed", K(ret));
  } else {
    is_exists = callback.has_same_pattern_digest();
  }
  return ret;
}

int ObUDRItemMgr::get_udr_item(const ObUDRContext &rule_ctx,
                               UDRItemRefGuard &item_guard,
                               PatternConstConsList *cst_cons_list)
{
  int ret = OB_SUCCESS;
  UDRKey rule_key;
  item_guard.ref_obj_ = NULL;
  UDRKeyNodePair *rule_node = NULL;
  ObUDRRlockAndRefGuard r_ref_lock_guard;
  cons_shallow_copy_rule_key<ObUDRContext>(rule_ctx, rule_key);
  if (OB_FAIL(get_value(rule_key, rule_node, r_ref_lock_guard))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get rule node", K(ret));
  } else if (nullptr == rule_node) {
    ret = OB_SUCCESS;
    LOG_DEBUG("item not exist", K(rule_key));
  } else if (NULL != cst_cons_list && OB_FAIL(get_all_cst_cons_by_key(rule_key, *cst_cons_list))) {
    LOG_WARN("failed to get all cst cons by key", K(ret));
  } else {
    bool is_match = false;
    UDRKeyNodePair::RuleItemList::iterator iter = rule_node->rule_item_list_.begin();
    for (; OB_SUCC(ret) && !is_match && iter != rule_node->rule_item_list_.end(); iter++) {
      ObUDRItem *item = *iter;
      if (OB_FAIL(match_rule_item(rule_ctx, item, is_match))) {
        LOG_WARN("failed to match rule item", K(ret));
      } else if (is_match) {
        item->inc_ref_count();
        item_guard.ref_obj_ = item;
        LOG_TRACE("succ to match rewrite rule item", KPC(item));
        break;
      }
    }
  }
  LOG_TRACE("get udr item", K(ret), K(rule_key));
  return ret;
}

int ObUDRItemMgr::get_all_cst_cons_by_key(const UDRKey &rule_key,
                                          PatternConstConsList &cst_cons_list)
{
  int ret = OB_SUCCESS;
  UDRKeyNodePair *rule_node = NULL;
  ObUDRRlockAndRefGuard r_ref_lock_guard;
  if (OB_FAIL(get_value(rule_key, rule_node, r_ref_lock_guard))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get rule node", K(ret));
  } else if (nullptr == rule_node) {
    ret = OB_SUCCESS;
    LOG_DEBUG("item not exist", K(rule_key));
  } else {
    UDRKeyNodePair::RuleItemList::iterator iter = rule_node->rule_item_list_.begin();
    for (; OB_SUCC(ret) && iter != rule_node->rule_item_list_.end(); iter++) {
      ObUDRItem *item = *iter;
      if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("is null", K(ret));
      } else if (!item->get_fixed_param_value_array().empty()
        && OB_FAIL(cst_cons_list.push_back(item->get_fixed_param_value_array()))) {
        LOG_WARN("failed to add fixed param value list", K(ret));
      }
    }
  }
  return ret;
}

int ObUDRItemMgr::get_rule_item_by_id(const UDRKey &rule_key,
                                      const int64_t rule_id,
                                      ObUDRItem *&rule_item)
{
  int ret = OB_SUCCESS;
  rule_item = NULL;
  UDRKeyNodePair *rule_node = nullptr;
  ObUDRRlockAndRefGuard r_ref_lock_guard;
  if (OB_FAIL(get_value(rule_key, rule_node, r_ref_lock_guard))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get rule node", K(ret));
  } else if (nullptr == rule_node) {
    ret = OB_SUCCESS;
    LOG_DEBUG("item not exist", K(rule_key));
  } else if (OB_FAIL(rule_node->get_rule_item_by_id(rule_id, rule_item))) {
    LOG_WARN("failed to remove rule item", K(ret));
  }
  return ret;
}

int ObUDRItemMgr::sync_local_cache_rules(const ObIArray<ObUDRInfo>& rule_infos)
{
  int ret = OB_SUCCESS;
  LOG_INFO("sync local cache rules", K(tenant_id_), K(rule_infos));
  for (int64_t i = 0; i < rule_infos.count() && OB_SUCC(ret); ++i) {
    const ObUDRInfo &rule_info = rule_infos.at(i);
    UDRKey rule_key;
    ObUDRItem *rule_item = NULL;
    cons_shallow_copy_rule_key<ObUDRInfo>(rule_info, rule_key);
    if (rule_info.rule_status_ == ObUDRInfo::DELETE_STATUS) {
      if (OB_FAIL(erase_item(rule_key, rule_info.rule_id_))) {
        LOG_WARN("failed to erase item", K(ret), K(rule_key));
      }
    } else if (OB_FAIL(get_rule_item_by_id(rule_key, rule_info.rule_id_, rule_item))) {
      LOG_WARN("failed to get rule item by name", K(ret), K(rule_key), K(rule_info));
    } else if (NULL != rule_item) {
      // modify rule status
      rule_item->set_rule_status(rule_info.rule_status_);
    } else if (OB_FAIL(add_item(rule_key, rule_info))) {
      LOG_WARN("failed to add item", K(ret), K(rule_key));
    }
  }
  return ret;
}

} // namespace sql end
} // namespace oceanbase end
