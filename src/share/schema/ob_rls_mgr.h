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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_RLS_MGR_H_
#define OCEANBASE_SHARE_SCHEMA_OB_RLS_MGR_H_

#include "lib/utility/ob_macro_utils.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "share/ob_define.h"
#include "share/schema/ob_schema_struct.h"
#include "lib/container/ob_vector.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObRlsPolicyNameHashKey
{
public:
  ObRlsPolicyNameHashKey()
    : tenant_id_(common::OB_INVALID_ID), table_id_(common::OB_INVALID_ID),
                 rls_group_id_(common::OB_INVALID_ID), name_()
  {
  }
  ObRlsPolicyNameHashKey(uint64_t tenant_id, uint64_t table_id, uint64_t rls_group_id,
                         common::ObString name)
    : tenant_id_(tenant_id), table_id_(table_id), rls_group_id_(rls_group_id), name_(name)
  {}
  ~ObRlsPolicyNameHashKey() {}
  uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
    hash_ret = common::murmurhash(&table_id_, sizeof(uint64_t), hash_ret);
    hash_ret = common::murmurhash(&rls_group_id_, sizeof(uint64_t), hash_ret);
    hash_ret = common::murmurhash(name_.ptr(), name_.length(), hash_ret);
    return hash_ret;
  }
  bool operator == (const ObRlsPolicyNameHashKey &rv) const
  {
    return tenant_id_ == rv.tenant_id_ && table_id_ == rv.table_id_ &&
           rls_group_id_ == rv.rls_group_id_ && name_ == rv.name_;
  }
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_table_id(uint64_t table_id) { table_id_ = table_id; }
  void set_rls_group_id(uint64_t rls_group_id) { rls_group_id_ = rls_group_id; }
  void set_name(const common::ObString &name) { name_ = name;}
  uint64_t get_tenant_id() const { return tenant_id_; }
  uint64_t get_table_id() const { return table_id_; }
  uint64_t get_rls_group_id() const { return rls_group_id_; }
  const common::ObString &get_name() const { return name_; }
private:
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t rls_group_id_;
  common::ObString name_;
};

template<class T, class V>
struct ObGetRlsPolicyKey
{
  void operator()(const T &t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetRlsPolicyKey<ObRlsPolicyNameHashKey, ObRlsPolicySchema *>
{
  ObRlsPolicyNameHashKey operator()(const ObRlsPolicySchema *schema) const
  {
    return OB_ISNULL(schema) ?
          ObRlsPolicyNameHashKey()
        : ObRlsPolicyNameHashKey(schema->get_tenant_id(), schema->get_table_id(),
                                 schema->get_rls_group_id(), schema->get_policy_name());
  }
};

template<>
struct ObGetRlsPolicyKey<uint64_t, ObRlsPolicySchema *>
{
  uint64_t operator()(const ObRlsPolicySchema *schema) const
  {
    return OB_ISNULL(schema) ? common::OB_INVALID_ID : schema->get_rls_policy_id();
  }
};

class ObRlsPolicyMgr
{
public:
  ObRlsPolicyMgr();
  explicit ObRlsPolicyMgr(common::ObIAllocator &allocator);
  virtual ~ObRlsPolicyMgr();
  ObRlsPolicyMgr &operator=(const ObRlsPolicyMgr &other);
  int init();
  void reset();
  int assign(const ObRlsPolicyMgr &other);
  int deep_copy(const ObRlsPolicyMgr &other);
  int add_rls_policy(const ObRlsPolicySchema &schema);
  int add_rls_policys(const common::ObIArray<ObRlsPolicySchema> &schemas);
  int del_rls_policy(const ObTenantRlsPolicyId &id);
  int get_schema_by_id(const uint64_t rls_policy_id,
                       const ObRlsPolicySchema *&schema) const;
  int get_schema_by_name(const uint64_t tenant_id,
                         const uint64_t table_id,
                         const uint64_t rls_group_id,
                         const common::ObString &name,
                         const ObRlsPolicySchema *&schema) const;
  int get_schemas_in_tenant(const uint64_t tenant_id,
                            common::ObIArray<const ObRlsPolicySchema *> &schemas) const;
  int get_schemas_in_table(const uint64_t tenant_id,
                           const uint64_t table_id,
                           common::ObIArray<const ObRlsPolicySchema *> &schemas) const;
  int get_schemas_in_group(const uint64_t tenant_id,
                           const uint64_t table_id,
                           const uint64_t rls_group_id,
                           common::ObIArray<const ObRlsPolicySchema *> &schemas) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  int get_schema_count(int64_t &schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
private:
  int rebuild_rls_policy_hashmap();
  static bool schema_compare(const ObRlsPolicySchema *lhs, const ObRlsPolicySchema *rhs);
  static bool schema_equal(const ObRlsPolicySchema *lhs, const ObRlsPolicySchema *rhs);
  static bool compare_with_sort_key(const ObRlsPolicySchema *lhs, const ObTenantRlsPolicyId &key);
  static bool equal_to_sort_key(const ObRlsPolicySchema *lhs, const ObTenantRlsPolicyId &key);
public:
  typedef common::ObSortedVector<ObRlsPolicySchema *> RlsPolicyInfos;
  typedef common::hash::ObPointerHashMap<ObRlsPolicyNameHashKey,
                                         ObRlsPolicySchema *,
                                         ObGetRlsPolicyKey, 128> ObRlsPolicyNameMap;
  typedef common::hash::ObPointerHashMap<uint64_t,
                                         ObRlsPolicySchema *,
                                         ObGetRlsPolicyKey, 128> ObRlsPolicyIdMap;
  typedef RlsPolicyInfos::iterator RlsPolicyIter;
  typedef RlsPolicyInfos::const_iterator ConstRlsPolicyIter;
private:
  static const char *RLS_POLICY_MGR;
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  RlsPolicyInfos rls_policy_infos_;
  ObRlsPolicyNameMap rls_policy_name_map_;
  ObRlsPolicyIdMap rls_policy_id_map_;
};

OB_INLINE bool ObRlsPolicyMgr::schema_compare(const ObRlsPolicySchema *lhs, const ObRlsPolicySchema *rhs)
{
  return lhs->get_sort_key() < rhs->get_sort_key();
}

OB_INLINE bool ObRlsPolicyMgr::schema_equal(const ObRlsPolicySchema *lhs, const ObRlsPolicySchema *rhs)
{
  return lhs->get_sort_key() == rhs->get_sort_key();
}

OB_INLINE bool ObRlsPolicyMgr::compare_with_sort_key(const ObRlsPolicySchema *lhs, const ObTenantRlsPolicyId &key)
{
  return NULL != lhs ? (lhs->get_sort_key() < key) : false;
}

OB_INLINE bool ObRlsPolicyMgr::equal_to_sort_key(const ObRlsPolicySchema *lhs, const ObTenantRlsPolicyId &key)
{
  return NULL != lhs ? (lhs->get_sort_key() == key) : false;
}

class ObRlsGroupNameHashKey
{
public:
  ObRlsGroupNameHashKey()
    : tenant_id_(common::OB_INVALID_TENANT_ID), table_id_(common::OB_INVALID_ID), group_name_()
  {
  }
  ObRlsGroupNameHashKey(uint64_t tenant_id, uint64_t table_id, common::ObString group_name)
    : tenant_id_(tenant_id), table_id_(table_id), group_name_(group_name)
  {
  }
  ~ObRlsGroupNameHashKey()
  {
  }
  uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
    hash_ret = common::murmurhash(&table_id_, sizeof(uint64_t), hash_ret);
    hash_ret = common::murmurhash(group_name_.ptr(), group_name_.length(), hash_ret);
    return hash_ret;
  }
  bool operator == (const ObRlsGroupNameHashKey &rv) const
  {
    return tenant_id_ == rv.tenant_id_ && table_id_ == rv.table_id_ &&
           group_name_ == rv.group_name_;
  }
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_table_id(uint64_t table_id) { table_id_ = table_id; }
  void set_group_name(const common::ObString &group_name) { group_name_ = group_name;}
  uint64_t get_tenant_id() const { return tenant_id_; }
  uint64_t get_table_id() const { return table_id_; }
  const common::ObString &get_group_name() const { return group_name_; }
private:
  uint64_t tenant_id_;
  uint64_t table_id_;
  common::ObString group_name_;
};

template<class T, class V>
struct ObGetRlsGroupKey
{
  void operator()(const T &t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetRlsGroupKey<ObRlsGroupNameHashKey, ObRlsGroupSchema *>
{
  ObRlsGroupNameHashKey operator()(const ObRlsGroupSchema *schema) const
  {
    return OB_ISNULL(schema) ?
          ObRlsGroupNameHashKey()
        : ObRlsGroupNameHashKey(schema->get_tenant_id(), schema->get_table_id(),
                                schema->get_policy_group_name());
  }
};

template<>
struct ObGetRlsGroupKey<uint64_t, ObRlsGroupSchema *>
{
  uint64_t operator()(const ObRlsGroupSchema *schema) const
  {
    return OB_ISNULL(schema) ? common::OB_INVALID_ID : schema->get_rls_group_id();
  }
};

class ObRlsGroupMgr
{
public:
  ObRlsGroupMgr();
  explicit ObRlsGroupMgr(common::ObIAllocator &allocator);
  virtual ~ObRlsGroupMgr();
  ObRlsGroupMgr &operator=(const ObRlsGroupMgr &other);
  int init();
  void reset();
  int assign(const ObRlsGroupMgr &other);
  int deep_copy(const ObRlsGroupMgr &other);
  int add_rls_group(const ObRlsGroupSchema &schema);
  int add_rls_groups(const common::ObIArray<ObRlsGroupSchema> &schemas);
  int del_rls_group(const ObTenantRlsGroupId &id);
  int get_schema_by_id(const uint64_t rls_group_id,
                       const ObRlsGroupSchema *&schema) const;
  int get_schema_by_name(const uint64_t tenant_id,
                         const uint64_t table_id,
                         const common::ObString &name,
                         const ObRlsGroupSchema *&schema) const;
  int get_schemas_in_tenant(const uint64_t tenant_id,
                            common::ObIArray<const ObRlsGroupSchema *> &schemas) const;
  int get_schemas_in_table(const uint64_t tenant_id,
                           const uint64_t table_id,
                           common::ObIArray<const ObRlsGroupSchema *> &schemas) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  int get_schema_count(int64_t &schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
private:
  int rebuild_rls_group_hashmap();
  static bool schema_compare(const ObRlsGroupSchema *lhs, const ObRlsGroupSchema *rhs);
  static bool schema_equal(const ObRlsGroupSchema *lhs, const ObRlsGroupSchema *rhs);
  static bool compare_with_sort_key(const ObRlsGroupSchema *lhs, const ObTenantRlsGroupId &key);
  static bool equal_to_sort_key(const ObRlsGroupSchema *lhs, const ObTenantRlsGroupId &key);
public:
  typedef common::ObSortedVector<ObRlsGroupSchema *> RlsGroupInfos;
  typedef common::hash::ObPointerHashMap<ObRlsGroupNameHashKey,
                                         ObRlsGroupSchema *,
                                         ObGetRlsGroupKey, 128> ObRlsGroupNameMap;
  typedef common::hash::ObPointerHashMap<uint64_t,
                                         ObRlsGroupSchema *,
                                         ObGetRlsGroupKey, 128> ObRlsGroupIdMap;
  typedef RlsGroupInfos::iterator RlsGroupIter;
  typedef RlsGroupInfos::const_iterator ConstRlsGroupIter;
private:
  static const char *RLS_GROUP_MGR;
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  RlsGroupInfos rls_group_infos_;
  ObRlsGroupNameMap rls_group_name_map_;
  ObRlsGroupIdMap rls_group_id_map_;
};

OB_INLINE bool ObRlsGroupMgr::schema_compare(const ObRlsGroupSchema *lhs, const ObRlsGroupSchema *rhs)
{
  return lhs->get_sort_key() < rhs->get_sort_key();
}

OB_INLINE bool ObRlsGroupMgr::schema_equal(const ObRlsGroupSchema *lhs, const ObRlsGroupSchema *rhs)
{
  return lhs->get_sort_key() == rhs->get_sort_key();
}

OB_INLINE bool ObRlsGroupMgr::compare_with_sort_key(const ObRlsGroupSchema *lhs, const ObTenantRlsGroupId &key)
{
  return NULL != lhs ? (lhs->get_sort_key() < key) : false;
}

OB_INLINE bool ObRlsGroupMgr::equal_to_sort_key(const ObRlsGroupSchema *lhs, const ObTenantRlsGroupId &key)
{
  return NULL != lhs ? (lhs->get_sort_key() == key) : false;
}

class ObRlsContextNameHashKey
{
public:
  ObRlsContextNameHashKey()
    : tenant_id_(common::OB_INVALID_TENANT_ID), table_id_(common::OB_INVALID_ID),
                 context_name_(), attribute_()
  {
  }
  ObRlsContextNameHashKey(uint64_t tenant_id, uint64_t table_id, common::ObString context_name,
                          common::ObString attribute)
    : tenant_id_(tenant_id), table_id_(table_id), context_name_(context_name), attribute_(attribute)
  {
  }
  ~ObRlsContextNameHashKey()
  {
  }
  uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
    hash_ret = common::murmurhash(&table_id_, sizeof(uint64_t), 0);
    hash_ret = common::murmurhash(context_name_.ptr(), context_name_.length(), hash_ret);
    hash_ret = common::murmurhash(attribute_.ptr(), attribute_.length(), hash_ret);
    return hash_ret;
  }
  bool operator == (const ObRlsContextNameHashKey &rv) const
  {
    return tenant_id_ == rv.tenant_id_ && table_id_ == rv.table_id_ &&
           context_name_ == rv.context_name_ && attribute_ == rv.attribute_;
  }
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_table_id(uint64_t table_id) { table_id_ = table_id; }
  void set_context_name(const common::ObString &context_name) { context_name_ = context_name;}
  void set_attribute(const common::ObString &attribute) { attribute_ = attribute;}
  uint64_t get_tenant_id() const { return tenant_id_; }
  uint64_t get_table_id() const { return table_id_; }
  const common::ObString &get_context_name() const { return context_name_; }
  const common::ObString &get_attribute() const { return attribute_; }
private:
  uint64_t tenant_id_;
  uint64_t table_id_;
  common::ObString context_name_;
  common::ObString attribute_;
};

template<class T, class V>
struct ObGetRlsContextKey
{
  void operator()(const T &t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetRlsContextKey<ObRlsContextNameHashKey, ObRlsContextSchema *>
{
  ObRlsContextNameHashKey operator()(const ObRlsContextSchema *schema) const
  {
    return OB_ISNULL(schema) ?
          ObRlsContextNameHashKey()
        : ObRlsContextNameHashKey(schema->get_tenant_id(), schema->get_table_id(),
                                  schema->get_context_name(), schema->get_attribute());
  }
};

template<>
struct ObGetRlsContextKey<uint64_t, ObRlsContextSchema *>
{
  uint64_t operator()(const ObRlsContextSchema *schema) const
  {
    return OB_ISNULL(schema) ? common::OB_INVALID_ID : schema->get_rls_context_id();
  }
};

class ObRlsContextMgr
{
public:
  ObRlsContextMgr();
  explicit ObRlsContextMgr(common::ObIAllocator &allocator);
  virtual ~ObRlsContextMgr();
  ObRlsContextMgr &operator=(const ObRlsContextMgr &other);
  int init();
  void reset();
  int assign(const ObRlsContextMgr &other);
  int deep_copy(const ObRlsContextMgr &other);
  int add_rls_context(const ObRlsContextSchema &schema);
  int add_rls_contexts(const common::ObIArray<ObRlsContextSchema> &schemas);
  int del_rls_context(const ObTenantRlsContextId &id);
  int get_schema_by_id(const uint64_t rls_context_id,
                       const ObRlsContextSchema *&schema) const;
  int get_schema_by_name(const uint64_t tenant_id,
                         const uint64_t table_id,
                         const common::ObString &name,
                         const common::ObString &attribute,
                         const ObRlsContextSchema *&schema) const;
  int get_schemas_in_tenant(const uint64_t tenant_id,
                            common::ObIArray<const ObRlsContextSchema *> &schemas) const;
  int get_schemas_in_table(const uint64_t tenant_id,
                           const uint64_t table_id,
                           common::ObIArray<const ObRlsContextSchema *> &schemas) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  int get_schema_count(int64_t &schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
private:
  int rebuild_rls_context_hashmap();
  static bool schema_compare(const ObRlsContextSchema *lhs, const ObRlsContextSchema *rhs);
  static bool schema_equal(const ObRlsContextSchema *lhs, const ObRlsContextSchema *rhs);
  static bool compare_with_sort_key(const ObRlsContextSchema *lhs, const ObTenantRlsContextId &key);
  static bool equal_to_sort_key(const ObRlsContextSchema *lhs, const ObTenantRlsContextId &key);
public:
  typedef common::ObSortedVector<ObRlsContextSchema *> RlsContextInfos;
  typedef common::hash::ObPointerHashMap<ObRlsContextNameHashKey,
                                         ObRlsContextSchema *,
                                         ObGetRlsContextKey, 128> ObRlsContextNameMap;
  typedef common::hash::ObPointerHashMap<uint64_t,
                                         ObRlsContextSchema *,
                                         ObGetRlsContextKey, 128> ObRlsContextIdMap;
  typedef RlsContextInfos::iterator RlsContextIter;
  typedef RlsContextInfos::const_iterator ConstRlsContextIter;
private:
  static const char *RLS_CONTEXT_MGR;
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  RlsContextInfos rls_context_infos_;
  ObRlsContextNameMap rls_context_name_map_;
  ObRlsContextIdMap rls_context_id_map_;
};

OB_INLINE bool ObRlsContextMgr::schema_compare(const ObRlsContextSchema *lhs, const ObRlsContextSchema *rhs)
{
  return lhs->get_sort_key() < rhs->get_sort_key();
}

OB_INLINE bool ObRlsContextMgr::schema_equal(const ObRlsContextSchema *lhs, const ObRlsContextSchema *rhs)
{
  return lhs->get_sort_key() == rhs->get_sort_key();
}

OB_INLINE bool ObRlsContextMgr::compare_with_sort_key(const ObRlsContextSchema *lhs, const ObTenantRlsContextId &key)
{
  return NULL != lhs ? (lhs->get_sort_key() < key) : false;
}

OB_INLINE bool ObRlsContextMgr::equal_to_sort_key(const ObRlsContextSchema *lhs, const ObTenantRlsContextId &key)
{
  return NULL != lhs ? (lhs->get_sort_key() == key) : false;
}

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SCHEMA_OB_RLS_MGR_H_
