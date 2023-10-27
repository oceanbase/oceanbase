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


#ifndef _OB_SHARE_RESOURCE_PLAN_OB_RESOURCE_MAPPING_RULE_CACHE_H_
#define _OB_SHARE_RESOURCE_PLAN_OB_RESOURCE_MAPPING_RULE_CACHE_H_

#include "share/ob_thread_pool.h"
#include "share/ob_thread_mgr.h"

#include "lib/utility/ob_macro_utils.h"
#include "common/data_buffer.h"
#include "lib/string/ob_string.h"
#include "lib/hash/ob_hashmap.h"
#include "share/ob_define.h"
#include "share/resource_manager/ob_resource_plan_info.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace common
{
class ObString;
}
namespace sql
{
class ObPlanCache;
}
namespace share
{
typedef common::ObIntWarp ObResTenantId;
typedef common::LinkHashNode<ObResTenantId> ObResColMapInfoNode;
typedef common::LinkHashValue<ObResTenantId> ObResColMapInfoValue;
class ObResourceManagerProxy;
class ObTenantResColMappingInfo : public ObResColMapInfoValue
{
private:
  struct TableNameHashWrapper
  {
    TableNameHashWrapper()
        : tenant_id_(common::OB_INVALID_ID), database_id_(common::OB_INVALID_ID),
          name_case_mode_(common::OB_NAME_CASE_INVALID), table_name_()
    {
    }
    TableNameHashWrapper(const uint64_t tenant_id, const uint64_t database_id,
                         const common::ObNameCaseMode mode, const common::ObString &table_name)
        : tenant_id_(tenant_id), database_id_(database_id),
          name_case_mode_(mode), table_name_(table_name)
    {
    }
    ~TableNameHashWrapper() {}
    inline uint64_t hash(uint64_t seed) const
    {
      uint64_t hash_ret = seed;
      common::ObCollationType cs_type = schema::ObSchema::get_cs_type_with_cmp_mode(name_case_mode_);
      hash_ret = common::ObCharset::hash(cs_type, table_name_, hash_ret);
      return hash_ret;
    }
    bool operator ==(const TableNameHashWrapper &other) const
    {
      schema::ObCompareNameWithTenantID name_cmp(tenant_id_, name_case_mode_, database_id_);
      return (0 == name_cmp.compare(table_name_, other.table_name_));
    }
    TO_STRING_KV(K(tenant_id_), K(database_id_), K(name_case_mode_), K(table_name_));

    uint64_t tenant_id_;
    uint64_t database_id_;
    common::ObNameCaseMode name_case_mode_;
    common::ObString table_name_;
  };

  struct ColumnNameHashWrapper
  {
    ColumnNameHashWrapper()
        : column_name_()
    {
    }
    ColumnNameHashWrapper(const common::ObString &column_name)
        : column_name_(column_name)
    {
    }
    ~ColumnNameHashWrapper() {}
    inline int hash(uint64_t &hash_val, uint64_t seed = 0) const
    {
      hash_val = seed;
      //case insensitive
      hash_val = common::ObCharset::hash(common::CS_TYPE_UTF8MB4_GENERAL_CI, column_name_, hash_val);
      return OB_SUCCESS;
    }
    bool operator ==(const ColumnNameHashWrapper &other) const
    {
      schema::ObCompareNameWithTenantID name_cmp;
      return (0 == name_cmp.compare(column_name_, other.column_name_));
    }
    TO_STRING_KV(K(column_name_));
    common::ObString column_name_;
  };

  struct ColumnNameKey
  {
    ColumnNameKey() : database_id_(0), table_name_(), column_name_() {}
    ColumnNameKey(uint64_t tenant_id, uint64_t database_id,
                  common::ObString table_name, common::ObString column_name,
                  common::ObNameCaseMode name_case_mode) :
      database_id_(database_id),
      table_name_(tenant_id, database_id, name_case_mode, table_name),
      column_name_(column_name)
    { }
    uint64_t database_id_;
    TableNameHashWrapper table_name_;
    ColumnNameHashWrapper column_name_;
    inline uint64_t hash() const
    {
      uint64_t hash_val = database_id_;
      hash_val = table_name_.hash(hash_val);
      column_name_.hash(hash_val, hash_val);
      return hash_val;
    }
    inline int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
    inline bool operator==(const ColumnNameKey& key) const
    {
      return database_id_ == key.database_id_
              && table_name_ == key.table_name_
              && column_name_ == key.column_name_;
    }
    TO_STRING_KV(K(database_id_), K(table_name_), K(column_name_));
  };
  struct RuleValueKey
  {
    RuleValueKey() : rule_id_(common::OB_INVALID_ID), user_name_(), literal_value_() {}
    RuleValueKey(uint64_t rule_id, common::ObString user_name, common::ObString literal_value) :
      rule_id_(rule_id), user_name_(user_name), literal_value_(literal_value)
    { }
    uint64_t rule_id_;
    // user name is case sensitive.
    common::ObString user_name_;
    common::ObString literal_value_;
    inline uint64_t hash() const
    {
      uint64_t hash_val = 0;
      hash_val = common::murmurhash(&rule_id_, sizeof(uint64_t), hash_val);
      hash_val = common::murmurhash(user_name_.ptr(), user_name_.length(), hash_val);
      hash_val = common::murmurhash(literal_value_.ptr(), literal_value_.length(), hash_val);
      return hash_val;
    }
    inline int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
    inline bool operator==(const RuleValueKey& key) const
    {
      return rule_id_ == key.rule_id_
             && 0 == user_name_.compare(key.user_name_)
             && 0 == literal_value_.compare(key.literal_value_);
    }
    TO_STRING_KV(K(rule_id_), K(user_name_), K(literal_value_));
  };
  struct RuleValue
  {
    RuleValue() : group_id_(common::OB_INVALID_ID), user_name_ptr_(NULL), literal_value_ptr_(NULL) {}
    RuleValue(uint64_t group_id, common::ObString user_name, common::ObString literal_value) :
      group_id_(group_id), user_name_ptr_(user_name.ptr()), literal_value_ptr_(literal_value.ptr())
    { }
    void release_memory(common::ObIAllocator &allocator)
    {
      if (NULL != user_name_ptr_) {
        allocator.free(user_name_ptr_);
        user_name_ptr_ = NULL;
      }
      if (OB_NOT_NULL(literal_value_ptr_)) {
        allocator.free(literal_value_ptr_);
        literal_value_ptr_ = NULL;
      }
    }
    uint64_t group_id_;
    char *user_name_ptr_;
    char *literal_value_ptr_;
    TO_STRING_KV(K(group_id_), KP(user_name_ptr_), KP(literal_value_ptr_));
  };
public:
  ObTenantResColMappingInfo()
  {}
  int init(uint64_t tenant_id);
  int refresh(sql::ObPlanCache *plan_cache, const common::ObString &plan);
  uint64_t get_tenant_id() const { return tenant_id_; }
  int get_rule_id(uint64_t tenant_id, uint64_t database_id, const common::ObString &table_name,
                       const common::ObString &column_name, common::ObNameCaseMode case_mode,
                       uint64_t &rule_id);
  int get_group_id(uint64_t rule_id, const ObString &user_name,
                        const common::ObString &literal_value, uint64_t &group_id);
  int64_t get_version() { return version_; }
private:
  uint64_t tenant_id_;
  //avoid concurrent modification on maps.
  obutil::Mutex lock_;
  ObArenaAllocator allocator_;
  common::hash::ObHashMap<ColumnNameKey, uint64_t> rule_id_map_;
  common::hash::ObHashMap<RuleValueKey, RuleValue> group_id_map_;
  int64_t version_;
};


class ObResourceColMappingRuleManager
{
private:
  class AllocHandle
  {
  public:
    static ObTenantResColMappingInfo *alloc_value() { return NULL; }
    static void free_value(ObTenantResColMappingInfo *info)
    {
      if (NULL != info) {
        info->~ObTenantResColMappingInfo();
        ob_free(info);
        info = NULL;
      }
    }
    static ObResColMapInfoNode *alloc_node(ObTenantResColMappingInfo *p)
    {
      return OB_NEW(ObResColMapInfoNode, ObMemAttr(p->get_tenant_id(), "ResRuleInfoMap"));
    }
    static void free_node(ObResColMapInfoNode *node)
    {
      if (NULL != node) {
        OB_DELETE(ObResColMapInfoNode, "ResRuleInfoMap", node);
        node = NULL;
      }
    }
  };
public:
  ObResourceColMappingRuleManager(): inited_(false)
  { }
  virtual ~ObResourceColMappingRuleManager() = default;
  int init();
  int add_tenant(uint64_t tenant_id);
  int drop_tenant(uint64_t tenant_id);
  int refresh_resource_column_mapping_rule(uint64_t tenant_id,
                                           sql::ObPlanCache *plan_cache,
                                           const common::ObString &plan);
  uint64_t get_column_mapping_rule_id(uint64_t tenant_id, uint64_t database_id,
                                     const common::ObString &table_name,
                                     const common::ObString &column_name,
                                     common::ObNameCaseMode case_mode);
  uint64_t get_column_mapping_group_id(uint64_t tenant_id, uint64_t rule_id,
                                       const common::ObString &user_name,
                                       const common::ObString &literal_value);
  int64_t get_column_mapping_version(uint64_t tenant_id);
private:
  common::ObLinkHashMap<ObResTenantId, ObTenantResColMappingInfo, AllocHandle> tenant_rule_infos_;
  bool inited_;

  DISALLOW_COPY_AND_ASSIGN(ObResourceColMappingRuleManager);
};
}
}
#endif /* _OB_SHARE_RESOURCE_PLAN_OB_RESOURCE_MAPPING_RULE_CACHE_H_ */
//// end of header file
