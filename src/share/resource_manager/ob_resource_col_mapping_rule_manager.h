// Copyright 2022 Alibaba Inc. All Rights Reserved.
// Author:
//  dachuan.sdc@antgroup.com
// This file is for resource mapping rule cache module.


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
class ObResourceColMappingRuleManager
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
    inline uint64_t hash(uint64_t seed = 0) const
    {
      uint64_t hash_ret = seed;
      //case insensitive
      hash_ret = common::ObCharset::hash(common::CS_TYPE_UTF8MB4_GENERAL_CI, column_name_, hash_ret);
      return hash_ret;
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
    ColumnNameKey() : tenant_id_(0), database_id_(0), table_name_(), column_name_() {}
    ColumnNameKey(uint64_t tenant_id, uint64_t database_id,
                  common::ObString table_name, common::ObString column_name,
                  common::ObNameCaseMode name_case_mode) :
      tenant_id_(tenant_id), database_id_(database_id),
      table_name_(tenant_id_, database_id_, name_case_mode, table_name),
      column_name_(column_name)
    { }
    uint64_t tenant_id_;
    uint64_t database_id_;
    TableNameHashWrapper table_name_;
    ColumnNameHashWrapper column_name_;
    inline uint64_t hash() const
    {
      uint64_t hash_val = (tenant_id_ << 32) | database_id_;
      hash_val = table_name_.hash(hash_val);
      hash_val = column_name_.hash(hash_val);
      return hash_val;
    }
    inline bool operator==(const ColumnNameKey& key) const
    {
      return tenant_id_ == key.tenant_id_
              && database_id_ == key.database_id_
              && table_name_ == key.table_name_
              && column_name_ == key.column_name_;
    }
    TO_STRING_KV(K(tenant_id_), K(database_id_), K(table_name_), K(column_name_));
  };
  struct RuleValueKey
  {
    RuleValueKey() : tenant_id_(common::OB_INVALID_ID), rule_id_(common::OB_INVALID_ID),
      user_name_(), literal_value_() {}
    RuleValueKey(uint64_t tenant_id, uint64_t rule_id, common::ObString user_name, common::ObString literal_value) :
      tenant_id_(tenant_id), rule_id_(rule_id), user_name_(user_name), literal_value_(literal_value)
    { }
    uint64_t tenant_id_;
    uint64_t rule_id_;
    // user name is case sensitive.
    common::ObString user_name_;
    common::ObString literal_value_;
    inline uint64_t hash() const
    {
      uint64_t hash_val = tenant_id_;
      hash_val = common::murmurhash(&rule_id_, sizeof(uint64_t), hash_val);
      hash_val = common::murmurhash(user_name_.ptr(), user_name_.length(), hash_val);
      hash_val = common::murmurhash(literal_value_.ptr(), literal_value_.length(), hash_val);
      return hash_val;
    }
    inline bool operator==(const RuleValueKey& key) const
    {
      return tenant_id_ == key.tenant_id_
             && rule_id_ == key.rule_id_
             && 0 == user_name_.compare(key.user_name_)
             && 0 == literal_value_.compare(key.literal_value_);
    }
    TO_STRING_KV(K(tenant_id_), K(rule_id_), K(user_name_), K(literal_value_));
  };
  struct RuleValue
  {
    RuleValue() : group_id_(common::OB_INVALID_ID), user_name_ptr_(NULL), literal_value_ptr_(NULL) {}
    RuleValue(uint64_t group_id, common::ObString user_name, common::ObString literal_value) :
      group_id_(group_id), user_name_ptr_(user_name.ptr()), literal_value_ptr_(literal_value.ptr())
    { }
    void release_memory()
    {
      if (NULL != user_name_ptr_) {
        ob_free(user_name_ptr_);
        user_name_ptr_ = NULL;
      }
      if (OB_NOT_NULL(literal_value_ptr_)) {
        ob_free(literal_value_ptr_);
        literal_value_ptr_ = NULL;
      }
    }
    uint64_t group_id_;
    char *user_name_ptr_;
    char *literal_value_ptr_;
    TO_STRING_KV(K(group_id_), KP(user_name_ptr_), KP(literal_value_ptr_));
  };
public:
  ObResourceColMappingRuleManager(): inited_(false)
  { }
  virtual ~ObResourceColMappingRuleManager() = default;
  int init();
  int refresh_resource_column_mapping_rule(uint64_t tenant_id,
                                           sql::ObPlanCache *plan_cache,
                                           const common::ObString &plan);
  uint64_t get_column_mapping_rule_id(uint64_t tenant_id, uint64_t database_id,
                                     const common::ObString &table_name,
                                     const common::ObString &column_name,
                                     common::ObNameCaseMode case_mode) const;
  uint64_t get_column_mapping_group_id(uint64_t tenant_id, uint64_t rule_id,
                                       const common::ObString &user_name,
                                       const common::ObString &literal_value);
  int64_t get_column_mapping_version(uint64_t tenant_id);
private:
  // get version of inner table data in __all_sys_stat.
  int query_inner_table_version(int64_t &current_version);
private:
  //avoid concurrent modification on maps.
  obutil::Mutex lock_;
  common::hash::ObHashMap<ColumnNameKey, uint64_t> rule_id_map_;
  common::hash::ObHashMap<RuleValueKey, RuleValue> group_id_map_;
  common::hash::ObHashMap<uint64_t, int64_t> tenant_version_;
  bool inited_;

  DISALLOW_COPY_AND_ASSIGN(ObResourceColMappingRuleManager);
};
}
}
#endif /* _OB_SHARE_RESOURCE_PLAN_OB_RESOURCE_MAPPING_RULE_CACHE_H_ */
//// end of header file
