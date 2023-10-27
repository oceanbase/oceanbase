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


#ifndef OB_SQL_UDR_OB_UDR_ITEM_MGR_H_
#define OB_SQL_UDR_OB_UDR_ITEM_MGR_H_

#include "lib/hash/ob_hashmap.h"
#include "sql/udr/ob_udr_item.h"
#include "sql/udr/ob_udr_context.h"

namespace oceanbase
{
namespace sql
{

class ObUDRAtomicOp;

class ObUDRItemMgr
{
public:
  template<typename T>
  class UDRRefObjGuard
  {
  friend class ObUDRItemMgr;
  public:
    UDRRefObjGuard() : ref_obj_(NULL)
    {
    }
    ~UDRRefObjGuard()
    {
      if (NULL != ref_obj_) {
        ref_obj_->dec_ref_count();
        ref_obj_ = NULL;
      }
    }
    const T* get_ref_obj() const { return ref_obj_; }
    bool is_valid() const { return ref_obj_ != NULL; }

    TO_STRING_KV(K_(ref_obj));
  private:
    T* ref_obj_;
  };

  struct UDRKey
  {
    UDRKey()
    : tenant_id_(OB_INVALID_ID),
      pattern_digest_(OB_INVALID_ID),
      db_name_(),
      normalized_pattern_() {}

    UDRKey(uint64_t tenant_id,
            uint64_t pattern_digest,
            ObCollationType coll_type,
            const common::ObString& db_name,
            const common::ObString& normalized_pattern)
    : tenant_id_(tenant_id),
      pattern_digest_(pattern_digest),
      db_name_(db_name),
      normalized_pattern_(normalized_pattern) {}

    inline uint64_t hash() const
    {
      uint64_t hash_ret = murmurhash(&tenant_id_, sizeof(uint64_t), 0);
      hash_ret = murmurhash(&pattern_digest_, sizeof(uint64_t), hash_ret);
      hash_ret = db_name_.hash(hash_ret);
      hash_ret = normalized_pattern_.hash(hash_ret);
      return hash_ret;
    }
    inline int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
    inline bool operator==(const UDRKey& key) const
    {
      return tenant_id_ == key.tenant_id_
             && pattern_digest_ == key.pattern_digest_
             && 0 == db_name_.compare(key.db_name_)
             && 0 == normalized_pattern_.compare(key.normalized_pattern_);
    }
    int deep_copy(common::ObIAllocator &allocator, const UDRKey &other);
    void destory(common::ObIAllocator &allocator);
    TO_STRING_KV(K_(tenant_id),
                 K_(pattern_digest),
                 K_(db_name),
                 K_(normalized_pattern));

    uint64_t tenant_id_;
    uint64_t pattern_digest_;
    common::ObString db_name_;
    common::ObString normalized_pattern_;
  };

  struct UDRKeyNodePair
  {
    typedef common::ObList<ObUDRItem*, ObIAllocator> RuleItemList;
    UDRKeyNodePair(common::ObIAllocator &allocator)
    : allocator_(allocator),
      ref_count_(0),
      rule_key_(),
      ref_lock_(common::ObLatchIds::REWRITE_RULE_ITEM_LOCK),
      rule_item_list_(allocator) {}
    ~UDRKeyNodePair() { reset(); }

    void reset();
    int64_t inc_ref_count();
    int64_t dec_ref_count();
    int64_t get_ref_count() const { return ref_count_; }
    int lock(bool is_rdlock);
    int unlock() { return ref_lock_.unlock(); }
    int deep_copy_rule_key(const UDRKey &other);
    bool is_empty() const { return rule_item_list_.empty(); }
    int add_rule_item(ObUDRItem *rule_item);
    int remove_rule_item(const int64_t rule_id);
    int get_rule_item_by_id(const int64_t rule_id, ObUDRItem *&rule_item);
    const RuleItemList &get_rule_item_list() const { return rule_item_list_; }

    TO_STRING_KV(K_(rule_item_list));
    common::ObIAllocator &allocator_;
    int64_t ref_count_;
    UDRKey rule_key_;
    common::SpinRWLock ref_lock_;
    RuleItemList rule_item_list_;
  };

  typedef common::hash::ObHashMap<UDRKey, UDRKeyNodePair*> RuleKeyNodeMap;
  typedef UDRRefObjGuard<ObUDRItem> UDRItemRefGuard;

  ObUDRItemMgr()
  : inited_(false),
    allocator_(NULL),
    tenant_id_(OB_INVALID_ID) {}
  ~ObUDRItemMgr();
  void destroy();
  int reuse();
  int init(uint64_t tenant_id, common::ObIAllocator &allocator);
  int sync_local_cache_rules(const ObIArray<ObUDRInfo>& rule_infos);
  int get_udr_item(const ObUDRContext &rule_ctx,
                   UDRItemRefGuard &item_guard,
                   PatternConstConsList *cst_cons_list = NULL);
  int fuzzy_check_by_pattern_digest(const uint64_t pattern_digest,
                                    bool &is_exists);

private:
  template<typename T, typename ...Args>
  int alloc(UDRRefObjGuard<T> &guard, Args&& ...args);
  int get_value(const UDRKey &rule_key,
                UDRKeyNodePair *&rule_node,
                ObUDRAtomicOp &op);
  int erase_item(const UDRKey &rule_key,  const int64_t rule_id);
  int add_item(const UDRKey &rule_key, const ObUDRInfo &rule_info);
  int add_rule_node(const UDRKey &rule_key, UDRKeyNodePair *rule_node);
  int remove_rule_node(const UDRKey &rule_key);
  int match_rule_item(const ObUDRContext &rule_ctx,
                      const ObUDRItem *rule_item,
                      bool &is_match) const;
  int match_fixed_param_list(const ObUDRContext &rule_ctx,
                             const ObUDRItem *rule_item,
                             bool &is_match) const;
  int get_rule_item_by_id(const UDRKey &rule_key,
                          const int64_t rule_id,
                          ObUDRItem *&rule_item);
  int cons_deep_copy_rule_item(const ObUDRInfo &rule_info,
                               ObUDRItem &rule_item);
  int get_all_cst_cons_by_key(const UDRKey &rule_key,
                              PatternConstConsList &cst_cons_list);
  template<typename Info>
  void cons_shallow_copy_rule_key(const Info &rule_info,
                                  UDRKey &rule_key) const;

private:
  bool inited_;
  common::ObIAllocator *allocator_;
  uint64_t tenant_id_;
  RuleKeyNodeMap rule_key_node_map_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObUDRItemMgr);
};
} // namespace sql end
} // namespace oceanbase end

#endif