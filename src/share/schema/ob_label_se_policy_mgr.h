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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_OLS_POLICY_MGR_H
#define OCEANBASE_SHARE_SCHEMA_OB_OLS_POLICY_MGR_H

#include "share/ob_define.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/container/ob_vector.h"
#include "lib/allocator/page_arena.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObLabelSeNameHashKey
{
public:
  ObLabelSeNameHashKey() : tenant_id_(common::OB_INVALID_ID) {}
  ObLabelSeNameHashKey(uint64_t tenant_id, common::ObString name)
    : tenant_id_(tenant_id),
      name_(name)
  {}
  ~ObLabelSeNameHashKey() {}
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
    hash_ret = common::murmurhash(name_.ptr(),
                                  name_.length(),
                                  hash_ret);
    return hash_ret;
  }
  inline bool operator == (const ObLabelSeNameHashKey &rv) const
  {
    return tenant_id_ == rv.tenant_id_
          && name_ == rv.name_;
  }
private:
  uint64_t tenant_id_;
  common::ObString name_;
};

class ObLabelSeColumnNameHashKey : public ObLabelSeNameHashKey
{
public:
  ObLabelSeColumnNameHashKey() : ObLabelSeNameHashKey() {}
  ObLabelSeColumnNameHashKey(uint64_t tenant_id, common::ObString name)
    : ObLabelSeNameHashKey(tenant_id, name) {}
};

template<class T, class V>
struct ObGetLabelSePolicyKey {
  void operator()(const T &t, const V &v) const {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetLabelSePolicyKey<ObLabelSeNameHashKey, ObLabelSePolicySchema *>
{
  ObLabelSeNameHashKey operator() (const ObLabelSePolicySchema *schema) const {
    return OB_ISNULL(schema) ?
          ObLabelSeNameHashKey()
        : ObLabelSeNameHashKey(schema->get_tenant_id(), schema->get_policy_name_str());
  }
};

template<>
struct ObGetLabelSePolicyKey<ObLabelSeColumnNameHashKey, ObLabelSePolicySchema *>
{
  ObLabelSeColumnNameHashKey operator() (const ObLabelSePolicySchema *schema) const {
    return OB_ISNULL(schema) ?
          ObLabelSeColumnNameHashKey()
        : ObLabelSeColumnNameHashKey(schema->get_tenant_id(), schema->get_column_name_str());
  }
};


template<>
struct ObGetLabelSePolicyKey<uint64_t, ObLabelSePolicySchema *>
{
  uint64_t operator()(const ObLabelSePolicySchema *schema) const
  {
    return OB_ISNULL(schema) ? common::OB_INVALID_ID : schema->get_label_se_policy_id();
  }
};



class ObLabelSePolicyMgr
{
public:
  typedef common::ObSortedVector<ObLabelSePolicySchema *> LabelSePolicyInfos;
  typedef common::hash::ObPointerHashMap<ObLabelSeNameHashKey, ObLabelSePolicySchema *,
                                         ObGetLabelSePolicyKey, 128> ObLabelSePolicyNameMap;
  typedef common::hash::ObPointerHashMap<ObLabelSeColumnNameHashKey, ObLabelSePolicySchema *,
                                         ObGetLabelSePolicyKey, 128> ObLabelSeColumnNameMap;
  typedef common::hash::ObPointerHashMap<uint64_t, ObLabelSePolicySchema *,
                                         ObGetLabelSePolicyKey, 128> ObLabelSePolicyIdMap;
  typedef LabelSePolicyInfos::iterator LabelSePolicyIter;
  typedef LabelSePolicyInfos::const_iterator ConstLabelSePolicyIter;
  ObLabelSePolicyMgr();
  explicit ObLabelSePolicyMgr(common::ObIAllocator &allocator);
  virtual ~ObLabelSePolicyMgr();
  int init();
  void reset();
  ObLabelSePolicyMgr &operator = (const ObLabelSePolicyMgr &other);
  int assign(const ObLabelSePolicyMgr &other);
  int deep_copy(const ObLabelSePolicyMgr &other);
  void dump() const;
  int add_label_se_policy(const ObLabelSePolicySchema &schema);
  int add_label_se_policys(const common::ObIArray<ObLabelSePolicySchema> &schemas);
  int del_label_se_policy(const ObTenantLabelSePolicyId &id);

  int get_schema_by_id(const uint64_t label_se_policy_id,
                       const ObLabelSePolicySchema *&schema) const;
  int get_schema_version_by_id(uint64_t label_se_policy_id, int64_t &schema_version) const;
  int get_schema_by_name(const uint64_t tenant_id,
                         const common::ObString &name,
                         const ObLabelSePolicySchema *&schema) const;
  int get_schema_by_column_name(const uint64_t tenant_id,
                                const common::ObString &column_name,
                                const ObLabelSePolicySchema *&schema) const;
  int get_schemas_in_tenant(const uint64_t tenant_id,
                            common::ObIArray<const ObLabelSePolicySchema *> &schemas) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  inline static bool schema_cmp(const ObLabelSePolicySchema *lhs,
                                const ObLabelSePolicySchema *rhs) {
    return lhs->get_tenant_id() != rhs->get_tenant_id() ?
          lhs->get_tenant_id() < rhs->get_tenant_id()
        : lhs->get_label_se_policy_id() < rhs->get_label_se_policy_id();
  }
  inline static bool schema_equal(const ObLabelSePolicySchema *lhs,
                                  const ObLabelSePolicySchema *rhs) {
    return lhs->get_tenant_id() == rhs->get_tenant_id()
        && lhs->get_label_se_policy_id() == rhs->get_label_se_policy_id();
  }

  static bool compare_with_tenant_label_se_policy_id(const ObLabelSePolicySchema *lhs, const ObTenantLabelSePolicyId &id);
  static bool equal_to_tenant_label_se_policy_id(const ObLabelSePolicySchema *lhs, const ObTenantLabelSePolicyId &id);

  int get_schema_count(int64_t &schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;

private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  LabelSePolicyInfos schema_infos_;
  ObLabelSePolicyNameMap policy_name_map_;
  ObLabelSeColumnNameMap column_name_map_;
  ObLabelSePolicyIdMap id_map_;
};

//Component schema

//一个租户下某个策略中，某种类型的component的名字不能重复
class ObLabelSeCompNameHashKey
{
public:
  ObLabelSeCompNameHashKey() : id_(), comp_type_(common::OB_INVALID_ID), name_() {}
  ObLabelSeCompNameHashKey(const ObTenantLabelSePolicyId &id, int64_t comp_type, const common::ObString &name)
    : id_(id),
      comp_type_(comp_type),
      name_(name)
  {}
  ~ObLabelSeCompNameHashKey() {}
  inline uint64_t hash() const
  {
    uint64_t hash_ret = id_.hash();
    hash_ret = common::murmurhash(&comp_type_, sizeof(int64_t), hash_ret);
    hash_ret = common::murmurhash(name_.ptr(),
                                  name_.length(),
                                  hash_ret);
    return hash_ret;
  }
  inline bool operator == (const ObLabelSeCompNameHashKey &rv) const
  {
    return id_ == rv.id_
        && comp_type_ == rv.comp_type_
        && name_ == rv.name_;
  }

private:
  ObTenantLabelSePolicyId id_;
  int64_t comp_type_;
  common::ObString name_;
};


class ObLabelSeCompShortNameHashKey : public ObLabelSeCompNameHashKey
{
public:
  ObLabelSeCompShortNameHashKey() : ObLabelSeCompNameHashKey() {}
  ObLabelSeCompShortNameHashKey(const ObTenantLabelSePolicyId &id, int64_t comp_type, const common::ObString &name)
    : ObLabelSeCompNameHashKey(id, comp_type, name)
  {}

};

class ObLabelSeCompLongNameHashKey : public ObLabelSeCompNameHashKey
{
public:
  ObLabelSeCompLongNameHashKey() : ObLabelSeCompNameHashKey() {}
  ObLabelSeCompLongNameHashKey(const ObTenantLabelSePolicyId &id, int64_t comp_type, const common::ObString &name)
    : ObLabelSeCompNameHashKey(id, comp_type, name)
  {}
};

class ObLabelSeCompNumHashKey
{
public:
  ObLabelSeCompNumHashKey() : id_(), comp_type_(common::OB_INVALID_ID), comp_num_(common::OB_INVALID_ID) {}
  ObLabelSeCompNumHashKey(const ObTenantLabelSePolicyId &id, int64_t comp_type, int64_t comp_num)
    : id_(id), comp_type_(comp_type), comp_num_(comp_num)
  {}
  inline uint64_t hash() const;
  inline bool operator == (const ObLabelSeCompNumHashKey &rv) const;

private:
  ObTenantLabelSePolicyId id_;
  int64_t comp_type_;
  int64_t comp_num_;

};

inline bool ObLabelSeCompNumHashKey::operator == (const ObLabelSeCompNumHashKey &rv) const
{
  return id_ == rv.id_
      && comp_type_ == rv.comp_type_
      && comp_num_ == rv.comp_num_;
}

inline uint64_t ObLabelSeCompNumHashKey::hash() const
{
  uint64_t hash_ret = id_.hash();
  hash_ret = common::murmurhash(&comp_type_, sizeof(int64_t), hash_ret);
  hash_ret = common::murmurhash(&comp_num_, sizeof(int64_t), hash_ret);
  return hash_ret;
}

template<class T, class V>
struct ObGetLabelSeCompKey {
  void operator()(const T &t, const V &v) const {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetLabelSeCompKey<ObLabelSeCompShortNameHashKey, ObLabelSeComponentSchema *>
{
  ObLabelSeCompShortNameHashKey operator() (const ObLabelSeComponentSchema *schema) const
  {
    return OB_ISNULL(schema) ?
          ObLabelSeCompShortNameHashKey()
        : ObLabelSeCompShortNameHashKey(schema->get_tenant_label_se_policy_id(), schema->get_comp_type(), schema->get_short_name_str());
  }
};

template<>
struct ObGetLabelSeCompKey<ObLabelSeCompLongNameHashKey, ObLabelSeComponentSchema *>
{
  ObLabelSeCompLongNameHashKey operator() (const ObLabelSeComponentSchema *schema) const
  {
    return OB_ISNULL(schema) ?
          ObLabelSeCompLongNameHashKey()
        : ObLabelSeCompLongNameHashKey(schema->get_tenant_label_se_policy_id(), schema->get_comp_type(), schema->get_long_name_str());
  }
};

template<>
struct ObGetLabelSeCompKey<ObLabelSeCompNumHashKey, ObLabelSeComponentSchema *>
{
  ObLabelSeCompNumHashKey operator() (const ObLabelSeComponentSchema *schema) const {
    return OB_ISNULL(schema) ?
          ObLabelSeCompNumHashKey()
        : ObLabelSeCompNumHashKey(schema->get_tenant_label_se_policy_id(), schema->get_comp_type(), schema->get_comp_num());
  }
};

template<>
struct ObGetLabelSeCompKey<uint64_t, ObLabelSeComponentSchema *>
{
  uint64_t operator() (const ObLabelSeComponentSchema *schema) const {
    return OB_ISNULL(schema) ?
          common::OB_INVALID_ID
        : schema->get_label_se_component_id();
  }
};

class ObLabelSeCompMgr
{
public:
  typedef common::ObSortedVector<ObLabelSeComponentSchema *> LabelSeCompInfos;
  typedef common::hash::ObPointerHashMap<ObLabelSeCompShortNameHashKey, ObLabelSeComponentSchema *,
                                         ObGetLabelSeCompKey, 128> ObLabelSeCompShortNameMap;

  typedef common::hash::ObPointerHashMap<ObLabelSeCompLongNameHashKey, ObLabelSeComponentSchema *,
                                         ObGetLabelSeCompKey, 128> ObLabelSeCompLongNameMap;

  typedef common::hash::ObPointerHashMap<uint64_t, ObLabelSeComponentSchema *,
                                         ObGetLabelSeCompKey, 128> ObLabelSeCompIdMap;

  typedef common::hash::ObPointerHashMap<ObLabelSeCompNumHashKey, ObLabelSeComponentSchema *,
                                         ObGetLabelSeCompKey, 128> ObLabelSeCompNumMap;
  typedef LabelSeCompInfos::iterator LabelSeCompIter;
  typedef LabelSeCompInfos::const_iterator ConstLabelSeCompIter;
  ObLabelSeCompMgr();
  explicit ObLabelSeCompMgr(common::ObIAllocator &allocator);
  virtual ~ObLabelSeCompMgr();
  int init();
  void reset();
  ObLabelSeCompMgr &operator = (const ObLabelSeCompMgr &other);
  int assign(const ObLabelSeCompMgr &other);
  int deep_copy(const ObLabelSeCompMgr &other);
  void dump() const;
  int add_label_se_component(const ObLabelSeComponentSchema &schema);
  int add_label_se_components(const common::ObIArray<ObLabelSeComponentSchema> &schemas);
  int del_label_se_component(const ObTenantLabelSeComponentId &id);

  int get_schema_by_id(const uint64_t label_se_comp_id, const ObLabelSeComponentSchema *&schema) const;
  int get_schema_version_by_id(const uint64_t label_se_comp_id, int64_t &schema_version) const;
  int get_schema_by_short_name(const ObTenantLabelSePolicyId &id,
                               const int64_t comp_type,
                               const common::ObString &name,
                               const ObLabelSeComponentSchema *&schema) const;
  int get_schema_by_long_name(const ObTenantLabelSePolicyId &id,
                              const int64_t comp_type,
                              const common::ObString &name,
                              const ObLabelSeComponentSchema *&schema) const;
  int get_schema_by_comp_num(const ObTenantLabelSePolicyId &id,
                             const int64_t comp_type,
                             const int64_t comp_num,
                             const ObLabelSeComponentSchema *&schema) const;
  int get_schemas_in_tenant(const uint64_t tenant_id,
                            common::ObIArray<const ObLabelSeComponentSchema *> &schemas) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  int get_schemas_in_policy(const uint64_t tenant_id,
                            const uint64_t policy_id,
                            common::ObIArray<const ObLabelSeComponentSchema *> &schemas) const;
  int del_schemas_in_policy(const uint64_t tenant_id,
                            const uint64_t policy_id);
  inline static bool schema_cmp(const ObLabelSeComponentSchema *lhs,
                                const ObLabelSeComponentSchema *rhs) {
    return lhs->get_tenant_label_se_component_id() < rhs->get_tenant_label_se_component_id();
  }
  inline static bool schema_equal(const ObLabelSeComponentSchema *lhs,
                                  const ObLabelSeComponentSchema *rhs) {
    return lhs->get_tenant_label_se_component_id() == rhs->get_tenant_label_se_component_id();
  }

  static bool compare_with_tenant_label_se_comp_id(const ObLabelSeComponentSchema *lhs, const ObTenantLabelSeComponentId &id);
  static bool equal_to_tenant_label_se_comp_id(const ObLabelSeComponentSchema *lhs, const ObTenantLabelSeComponentId &id);

  int get_schema_count(int64_t &schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;

private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  LabelSeCompInfos schema_infos_;
  ObLabelSeCompShortNameMap short_name_map_;
  ObLabelSeCompLongNameMap long_name_map_;
  ObLabelSeCompIdMap id_map_;
  ObLabelSeCompNumMap num_map_;
};


template<class T, class V>
struct ObGetLabelSeLabelKey {
  void operator()(const T &t, const V &v) const {
    UNUSED(t);
    UNUSED(v);
  }
};


template<>
struct ObGetLabelSeLabelKey<uint64_t, ObLabelSeLabelSchema *>
{
  uint64_t operator() (const ObLabelSeLabelSchema *schema) const {
    return OB_ISNULL(schema) ?
          common::OB_INVALID_ID
        : schema->get_label_se_label_id();
  }
};

template<>
struct ObGetLabelSeLabelKey<ObLabelSeNameHashKey, ObLabelSeLabelSchema *>
{
  ObLabelSeNameHashKey operator() (const ObLabelSeLabelSchema *schema) const {
    return OB_ISNULL(schema) ?
          ObLabelSeNameHashKey()
        : ObLabelSeNameHashKey(schema->get_tenant_id(), schema->get_label_str());
  }
};

class ObLabelSeLabelTagHashKey
{
public:
  ObLabelSeLabelTagHashKey() : tenant_id_(common::OB_INVALID_ID), tag_(common::OB_INVALID_ID) {}
  ObLabelSeLabelTagHashKey(uint64_t tenant_id, int64_t tag)
    : tenant_id_(tenant_id), tag_(tag)
  {}
  inline uint64_t hash() const;
  inline bool operator == (const ObLabelSeLabelTagHashKey &rv) const;

private:
  uint64_t tenant_id_;
  int64_t tag_;

};

inline bool ObLabelSeLabelTagHashKey::operator == (const ObLabelSeLabelTagHashKey &rv) const
{
  return tenant_id_ == rv.tenant_id_
      && tag_ == rv.tag_;
}

inline uint64_t ObLabelSeLabelTagHashKey::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::murmurhash(&tag_, sizeof(int64_t), hash_ret);
  return hash_ret;
}

template<>
struct ObGetLabelSeLabelKey<ObLabelSeLabelTagHashKey, ObLabelSeLabelSchema *>
{
  ObLabelSeLabelTagHashKey operator() (const ObLabelSeLabelSchema *schema) const {
    return OB_ISNULL(schema) ?
          ObLabelSeLabelTagHashKey()
        : ObLabelSeLabelTagHashKey(schema->get_tenant_id(), schema->get_label_tag());
  }
};


class ObLabelSeLabelMgr
{
public:
  typedef common::ObSortedVector<ObLabelSeLabelSchema *> LabelSeLabelInfos;
  typedef common::hash::ObPointerHashMap<ObLabelSeNameHashKey, ObLabelSeLabelSchema *,
                                         ObGetLabelSeLabelKey, 128> ObLabelSeLabelLabelMap;

  typedef common::hash::ObPointerHashMap<uint64_t, ObLabelSeLabelSchema *,
                                         ObGetLabelSeLabelKey, 128> ObLabelSeLabelIdMap;

  typedef common::hash::ObPointerHashMap<ObLabelSeLabelTagHashKey, ObLabelSeLabelSchema *,
                                         ObGetLabelSeLabelKey, 128> ObLabelSeLabelTagMap;

  typedef LabelSeLabelInfos::iterator LabelSeLabelIter;
  typedef LabelSeLabelInfos::const_iterator ConstLabelSeLabelIter;
  ObLabelSeLabelMgr();
  explicit ObLabelSeLabelMgr(common::ObIAllocator &allocator);
  virtual ~ObLabelSeLabelMgr();
  int init();
  void reset();
  ObLabelSeLabelMgr &operator = (const ObLabelSeLabelMgr &other);
  int assign(const ObLabelSeLabelMgr &other);
  int deep_copy(const ObLabelSeLabelMgr &other);
  void dump() const;
  int add_label_se_label(const ObLabelSeLabelSchema &schema);
  int add_label_se_labels(const common::ObIArray<ObLabelSeLabelSchema> &schemas);
  int del_label_se_label(const ObTenantLabelSeLabelId &id);

  int get_schema_by_id(const uint64_t label_se_label_id,
                       const ObLabelSeLabelSchema *&schema) const;
  int get_schema_version_by_id(const uint64_t label_se_label_id, int64_t &schema_version) const;
  int get_schema_by_label(uint64_t tenant_id,
                          const common::ObString &label,
                          const ObLabelSeLabelSchema *&schema) const;
  int get_schema_by_label_tag(const uint64_t tenant_id,
                              const int64_t label_tag,
                              const ObLabelSeLabelSchema *&schema) const;
  int get_schemas_in_tenant(const uint64_t tenant_id,
                            common::ObIArray<const ObLabelSeLabelSchema *> &schemas) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  int get_schemas_in_policy(const uint64_t tenant_id,
                            const uint64_t policy_id,
                            common::ObIArray<const ObLabelSeLabelSchema *> &schemas) const;
  int del_schemas_in_policy(const uint64_t tenant_id,
                            const uint64_t policy_id);
  inline static bool schema_cmp(const ObLabelSeLabelSchema *lhs,
                                const ObLabelSeLabelSchema *rhs) {
    return lhs->get_tenant_label_se_label_id() < rhs->get_tenant_label_se_label_id();
  }
  inline static bool schema_equal(const ObLabelSeLabelSchema *lhs,
                                  const ObLabelSeLabelSchema *rhs) {
    return lhs->get_tenant_label_se_label_id() == rhs->get_tenant_label_se_label_id();
  }

  static bool compare_with_tenant_label_se_label_id(const ObLabelSeLabelSchema *lhs, const ObTenantLabelSeLabelId &id);
  static bool equal_to_tenant_label_se_label_id(const ObLabelSeLabelSchema *lhs, const ObTenantLabelSeLabelId &id);

  int get_schema_count(int64_t &schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;

private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  LabelSeLabelInfos schema_infos_;
  ObLabelSeLabelLabelMap label_map_;
  ObLabelSeLabelIdMap id_map_;
  ObLabelSeLabelTagMap tag_map_;
};


template<class T, class V>
struct ObGetLabelSeUserLevelKey {
  void operator()(const T &t, const V &v) const {
    UNUSED(t);
    UNUSED(v);
  }
};

class ObLabelSeUserLevelHashKey
{
public:
  ObLabelSeUserLevelHashKey() : tenant_id_(common::OB_INVALID_ID),
                                user_id_(common::OB_INVALID_ID),
                                policy_id_(common::OB_INVALID_ID)
  {}
  ObLabelSeUserLevelHashKey(uint64_t tenant_id, uint64_t user_id, uint64_t policy_id)
    : tenant_id_(tenant_id), user_id_(user_id), policy_id_(policy_id)
  {}
  inline uint64_t hash() const;
  inline bool operator == (const ObLabelSeUserLevelHashKey &rv) const;

private:
  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t policy_id_;

};

inline bool ObLabelSeUserLevelHashKey::operator == (const ObLabelSeUserLevelHashKey &rv) const
{
  return tenant_id_ == rv.tenant_id_
      && user_id_ == rv.user_id_
      && policy_id_ == rv.policy_id_;
}

inline uint64_t ObLabelSeUserLevelHashKey::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_ret);
  hash_ret = common::murmurhash(&user_id_, sizeof(user_id_), hash_ret);
  hash_ret = common::murmurhash(&policy_id_, sizeof(policy_id_), hash_ret);
  return hash_ret;
}

template<>
struct ObGetLabelSeUserLevelKey<ObLabelSeUserLevelHashKey, ObLabelSeUserLevelSchema *>
{
  ObLabelSeUserLevelHashKey operator() (const ObLabelSeUserLevelSchema *schema) const {
    return OB_ISNULL(schema) ?
          ObLabelSeUserLevelHashKey()
        : ObLabelSeUserLevelHashKey(schema->get_tenant_id(),
                                    schema->get_user_id(),
                                    schema->get_label_se_policy_id());
  }
};

template<>
struct ObGetLabelSeUserLevelKey<uint64_t, ObLabelSeUserLevelSchema *>
{
  uint64_t operator() (const ObLabelSeUserLevelSchema *schema) const {
    return OB_ISNULL(schema) ?
          common::OB_INVALID_ID
        : schema->get_label_se_user_level_id();
  }
};

class ObLabelSeUserLevelMgr
{
public:
  typedef common::ObSortedVector<ObLabelSeUserLevelSchema *> LabelSeUserLevelInfos;

  typedef common::hash::ObPointerHashMap<ObLabelSeUserLevelHashKey, ObLabelSeUserLevelSchema *,
                                         ObGetLabelSeUserLevelKey, 128> ObLabelSeUserLevelKeyMap;

  typedef common::hash::ObPointerHashMap<uint64_t, ObLabelSeUserLevelSchema *,
                                         ObGetLabelSeUserLevelKey, 128> ObLabelSeUserLevelIdMap;

  typedef LabelSeUserLevelInfos::iterator LabelSeUserLevelIter;
  typedef LabelSeUserLevelInfos::const_iterator ConstLabelSeUserLevelIter;
  ObLabelSeUserLevelMgr();
  explicit ObLabelSeUserLevelMgr(common::ObIAllocator &allocator);
  virtual ~ObLabelSeUserLevelMgr();
  int init();
  void reset();
  ObLabelSeUserLevelMgr &operator = (const ObLabelSeUserLevelMgr &other);
  int assign(const ObLabelSeUserLevelMgr &other);
  int deep_copy(const ObLabelSeUserLevelMgr &other);
  void dump() const;
  int add_label_se_user_level(const ObLabelSeUserLevelSchema &schema);
  int add_label_se_user_levels(const common::ObIArray<ObLabelSeUserLevelSchema> &schemas);
  int del_label_se_user_level(const ObTenantLabelSeUserLevelId &id);

  int get_schema_by_id(const uint64_t label_se_user_level_id,
                       const ObLabelSeUserLevelSchema *&schema) const;
  int get_schema_version_by_id(const uint64_t label_se_user_level_id, int64_t &schema_version) const;
  int get_schema_by_user_policy_id(const uint64_t tenant_id,
                                   const uint64_t user_id,
                                   const uint64_t label_se_policy_id,
                                   const ObLabelSeUserLevelSchema *&schema) const;
  int get_schemas_in_tenant(const uint64_t tenant_id,
                            common::ObIArray<const ObLabelSeUserLevelSchema *> &schemas) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  int get_schemas_in_policy(const uint64_t tenant_id,
                            const uint64_t policy_id,
                            common::ObIArray<const ObLabelSeUserLevelSchema *> &schemas) const;
  int del_schemas_in_policy(const uint64_t tenant_id,
                            const uint64_t policy_id);
  int get_schemas_in_user(const uint64_t tenant_id,
                          const uint64_t user_id,
                          common::ObIArray<const ObLabelSeUserLevelSchema *> &schemas) const;
  int del_schemas_in_user(const uint64_t tenant_id,
                          const uint64_t user_id);
  inline static bool schema_cmp(const ObLabelSeUserLevelSchema *lhs,
                                const ObLabelSeUserLevelSchema *rhs) {
    return lhs->get_tenant_label_se_user_level_id() < rhs->get_tenant_label_se_user_level_id();
  }
  inline static bool schema_equal(const ObLabelSeUserLevelSchema *lhs,
                                  const ObLabelSeUserLevelSchema *rhs) {
    return lhs->get_tenant_label_se_user_level_id() == rhs->get_tenant_label_se_user_level_id();
  }

  static bool compare_with_tenant_label_se_user_level_id(const ObLabelSeUserLevelSchema *lhs, const ObTenantLabelSeUserLevelId &id);
  static bool equal_to_tenant_label_se_user_level_id(const ObLabelSeUserLevelSchema *lhs, const ObTenantLabelSeUserLevelId &id);

  int get_schema_count(int64_t &schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;

private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  LabelSeUserLevelInfos schema_infos_;
  ObLabelSeUserLevelKeyMap user_level_map_;
  ObLabelSeUserLevelIdMap id_map_;
};

} //end of schema
} //end of share
} //end of oceanbase
#endif //OCEANBASE_SHARE_SCHEMA_OB_OLS_POLICY_MGR_H
