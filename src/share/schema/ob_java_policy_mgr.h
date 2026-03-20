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

#ifndef OCEANBASE_SRC_SHARE_SCHEMA_OB_JAVA_POLICY_MGR_H_
#define OCEANBASE_SRC_SHARE_SCHEMA_OB_JAVA_POLICY_MGR_H_

#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_java_policy_schema_struct.h"
#include "lib/hash/ob_pointer_hashmap.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObSimpleJavaPolicySchema final : public ObSchema
{
public:
  enum class JavaPolicyKind : int64_t {
    INVALID = -1,
    GRANT = 0,
    RESTRICT = 1,
  };

  enum class JavaPolicyStatus : int64_t {
    INVALID = -1,
    ENABLED = 2,
    DISABLED = 3,
  };

  static bool is_valid_java_policy_kind(const JavaPolicyKind kind)
  {
    return JavaPolicyKind::GRANT == kind || JavaPolicyKind::RESTRICT == kind;
  }

  static bool is_valid_java_policy_status(const JavaPolicyStatus status)
  {
    return JavaPolicyStatus::ENABLED == status || JavaPolicyStatus::DISABLED == status;
  }

public:
  ObSimpleJavaPolicySchema() { reset(); }
  explicit ObSimpleJavaPolicySchema(ObIAllocator *allocator) : ObSchema(allocator) { reset(); }
  ~ObSimpleJavaPolicySchema() override = default;

  explicit ObSimpleJavaPolicySchema(const ObSimpleJavaPolicySchema &other) = delete;
  ObSimpleJavaPolicySchema &operator=(const ObSimpleJavaPolicySchema &other) = delete;

  int assign(const ObSimpleJavaPolicySchema &other);

  bool is_valid() const override;

  void reset() override;

  // getters
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_key() const { return key_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline JavaPolicyKind get_kind() const { return kind_; }
  inline uint64_t get_grantee() const { return grantee_; }
  inline uint64_t get_type_schema() const { return type_schema_; }
  inline const ObString &get_type_name() const { return type_name_; }
  inline const ObString &get_name() const { return name_; }
  inline const ObString &get_action() const { return action_; }
  inline JavaPolicyStatus get_status() const { return status_; }

  // setters
  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_key(const uint64_t key) { key_ = key; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline void set_kind(const JavaPolicyKind kind) { kind_ = kind; }
  inline void set_grantee(const uint64_t grantee) { grantee_ = grantee; }
  inline void set_type_schema(const uint64_t type_schema) { type_schema_ = type_schema; }
  inline int set_type_name(const ObString &type_name) { return deep_copy_str(type_name, type_name_); }
  inline int set_name(const ObString &name) { return deep_copy_str(name, name_); }
  inline int set_action(const ObString &action) { return deep_copy_str(action, action_); }
  inline void set_status(const JavaPolicyStatus status) { status_ = status; }

  inline ObTenantJavaPolicyId get_tenant_java_policy_id() const { return ObTenantJavaPolicyId(tenant_id_, key_); }

  inline bool is_grant() const { return kind_ == JavaPolicyKind::GRANT; }

  inline bool is_enabled() const { return status_ == JavaPolicyStatus::ENABLED; }

  TO_STRING_KV(K_(tenant_id),
               K_(key),
               K_(schema_version),
               K_(kind),
               K_(grantee),
               K_(type_schema),
               K_(type_name),
               K_(name),
               K_(action),
               K_(status));

private:
  uint64_t tenant_id_;
  uint64_t key_;
  int64_t schema_version_;
  JavaPolicyKind kind_;
  uint64_t grantee_;
  uint64_t type_schema_;
  ObString type_name_;
  ObString name_;
  ObString action_;
  JavaPolicyStatus status_;
};

template<typename K, typename V>
struct ObGetJavaPolicyKey;

template<>
struct ObGetJavaPolicyKey<uint64_t, ObSimpleJavaPolicySchema*>
{
  uint64_t operator()(const ObSimpleJavaPolicySchema *schema) const
  {
    return nullptr == schema ? OB_INVALID_ID : schema->get_key();
  }
};

class ObJavaPolicyMgr final
{
public:
  using JavaPolicyInfos = ObSortedVector<ObSimpleJavaPolicySchema *>;
  using JavaPolicyIdMap = common::hash::ObPointerHashMap<uint64_t, ObSimpleJavaPolicySchema*, ObGetJavaPolicyKey, 128>;
  using JavaPolicyIter = JavaPolicyInfos::iterator;
  using JavaPolicyConstIter = JavaPolicyInfos::const_iterator;

  ObJavaPolicyMgr();
  explicit ObJavaPolicyMgr(common::ObIAllocator &allocator);

  ObJavaPolicyMgr(const ObJavaPolicyMgr &other) = delete;
  ObJavaPolicyMgr &operator=(const ObJavaPolicyMgr &other) = delete;

  virtual ~ObJavaPolicyMgr() = default;

  int init();
  void reset();

  inline bool check_inner_stat() const { return is_inited_; }

  int assign(const ObJavaPolicyMgr &other);
  int deep_copy(const ObJavaPolicyMgr &other);

  int get_java_policy_schema_count(int64_t &count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;

  int add_java_policy(const ObSimpleJavaPolicySchema &schema);
  int add_java_policies(const common::ObIArray<ObSimpleJavaPolicySchema> &schemas);
  int del_java_policy(const ObTenantJavaPolicyId &id);

  int get_java_policy_schema(const uint64_t tenant_id,
                             const uint64_t key,
                             const ObSimpleJavaPolicySchema *&schema) const;

  int get_java_policy_schemas_in_tenant(const uint64_t tenant_id,
                                        common::ObIArray<const ObSimpleJavaPolicySchema *> &schemas) const;

  int get_java_policy_schemas_of_grantee(const uint64_t tenant_id,
                                         const uint64_t grantee_id,
                                         common::ObIArray<const ObSimpleJavaPolicySchema *> &schemas) const;

  int del_schemas_in_tenant(const uint64_t tenant_id);

  static bool compare_java_policy(const ObSimpleJavaPolicySchema *lhs,
                                  const ObSimpleJavaPolicySchema *rhs);

  static bool equal_java_policy(const ObSimpleJavaPolicySchema *lhs,
                                const ObSimpleJavaPolicySchema *rhs);

private:
  inline static bool compare_with_tenant_java_policy_id(const ObSimpleJavaPolicySchema *lhs,
                                                        const ObTenantJavaPolicyId &rhs)
  {
    return OB_ISNULL(lhs) ? false : (lhs->get_tenant_java_policy_id() < rhs);
  }

  inline static bool equal_to_tenant_java_policy_id(const ObSimpleJavaPolicySchema *lhs,
                                                    const ObTenantJavaPolicyId &rhs)
  {
    return OB_ISNULL(lhs) ? false : (lhs->get_tenant_java_policy_id() == rhs);
  }

  int rebuild_java_policy_hashmap();

private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  JavaPolicyInfos java_policy_infos_;
  JavaPolicyIdMap java_policy_id_map_;
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SRC_SHARE_SCHEMA_OB_JAVA_POLICY_MGR_H_
