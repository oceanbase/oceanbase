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

#ifndef OCEANBASE_SRC_SHARE_SCHEMA_OB_EXTERNAL_RESOURCE_MGR_H_
#define OCEANBASE_SRC_SHARE_SCHEMA_OB_EXTERNAL_RESOURCE_MGR_H_

#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_external_resource_schema_struct.h"

namespace oceanbase
{

namespace share
{

namespace schema
{

class ObSimpleExternalResourceSchema final : public ObSchema
{
  OB_UNIS_VERSION(1);

public:
  enum ResourceType : uint8_t
  {
    INVALID_TYPE = 0,
    JAVA_JAR_TYPE = 1,
    PYTHON_PY_TYPE = 2,
  };

public:
  ObSimpleExternalResourceSchema() { reset(); }
  explicit ObSimpleExternalResourceSchema(ObIAllocator *allocator) : ObSchema(allocator) { reset(); }
  ~ObSimpleExternalResourceSchema() override = default;

  explicit ObSimpleExternalResourceSchema(const ObSimpleExternalResourceSchema &other) = delete;
  ObSimpleExternalResourceSchema &operator=(const ObSimpleExternalResourceSchema &other) = delete;

  int assign(const ObSimpleExternalResourceSchema &other)
  {
    int ret = OB_SUCCESS;

    if (this != &other) {
      reset();
      set_tenant_id(other.tenant_id_);
      set_resource_id(other.resource_id_);
      set_schema_version(other.schema_version_);
      set_database_id(other.database_id_);
      set_type(other.type_);

      if (OB_FAIL(set_name(other.name_))) {
        SHARE_SCHEMA_LOG(WARN, "failed to deep copy resource name", K(ret), K(*this), K(other));
      }
    }

    return ret;
  }

  bool is_valid() const override
  {
    bool bret = true;

    if (!ObSchema::is_valid()
        || !is_valid_tenant_id(tenant_id_)
        || !is_valid_id(resource_id_)
        || OB_INVALID_VERSION == schema_version_
        || !is_valid_id(database_id_)
        || INVALID_TYPE == type_
        || name_.empty()) {
      bret = false;
    }

    return bret;
  }

  void reset() override
  {
    tenant_id_ = OB_INVALID_ID;
    resource_id_ = OB_INVALID_ID;
    schema_version_ = OB_INVALID_VERSION;
    database_id_ = OB_INVALID_ID;
    type_ = INVALID_TYPE;
    reset_string(name_);
    ObSchema::reset();
  }

  // getters
  inline void set_tenant_id(const uint64_t &tenant_id) { tenant_id_ = tenant_id; }
  inline void set_resource_id(const uint64_t &resource_id) { resource_id_ = resource_id; }
  inline void set_schema_version(const int64_t &schema_version) { schema_version_ = schema_version; }
  inline void set_database_id(const uint64_t &database_id) { database_id_ = database_id; }
  inline void set_type(const ResourceType &type) { type_ = type; }
  inline int set_name(const ObString &name) { return deep_copy_str(name, name_); }

  // setters
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_resource_id() const { return resource_id_; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline ResourceType get_type() const { return type_; }
  inline const ObString &get_name() const { return name_; }

  inline ObTenantExternalResourceId get_tenant_external_resource_id() const { return ObTenantExternalResourceId(tenant_id_, resource_id_); }

  TO_STRING_KV(K_(tenant_id), K_(resource_id), K_(schema_version), K_(database_id), K_(type), K_(name));

private:
  uint64_t tenant_id_;
  uint64_t resource_id_;
  int64_t schema_version_;
  uint64_t database_id_;
  ResourceType type_;
  ObString name_;
};

class ObExternalResourceHashWrapper final
{
public:
  ObExternalResourceHashWrapper() { reset(); };
  ObExternalResourceHashWrapper(uint64_t tenant_id,
                                uint64_t database_id,
                                const ObString &name)
    : tenant_id_(tenant_id),
      database_id_(database_id),
      name_(name)
  {
    set_case_mode();
  }

  ObExternalResourceHashWrapper(const ObExternalResourceHashWrapper &other) = default;
  ObExternalResourceHashWrapper &operator=(const ObExternalResourceHashWrapper &other) = default;

  ~ObExternalResourceHashWrapper() = default;

  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_database_id(uint64_t database_id) { database_id_ = database_id; }
  inline void set_resource_name(const common::ObString &resource_name) { name_ = resource_name; }

  inline void set_case_mode()
  {
    case_mode_ = lib::is_mysql_mode() ? common::OB_ORIGIN_AND_INSENSITIVE
                                      : common::OB_ORIGIN_AND_SENSITIVE;
  }

  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline const common::ObString &get_resource_name() const { return name_; }
  inline common::ObNameCaseMode get_case_mode() const { return case_mode_; }

  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    database_id_ = OB_INVALID_ID;
    name_.reset();
    case_mode_ = common::OB_NAME_CASE_INVALID;
  }

  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;

    hash_ret = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_ret);
    hash_ret = murmurhash(&database_id_, sizeof(database_id_), hash_ret);
    common::ObCollationType cs_type = ObSchema::get_cs_type_with_cmp_mode(case_mode_);
    hash_ret = common::ObCharset::hash(cs_type, name_, hash_ret);

    return hash_ret;
  }

  inline bool operator==(const ObExternalResourceHashWrapper &other) const
  {
    ObCompareNameWithTenantID name_cmp(tenant_id_, case_mode_);

    return (tenant_id_ == other.tenant_id_)
            && (database_id_ == other.database_id_)
            && (case_mode_ == other.case_mode_)
            && (0 == name_cmp.compare(name_, other.name_));
  }

  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(name));

private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  ObString name_;
  common::ObNameCaseMode case_mode_;
};

template<typename K, typename V>
struct ObGetExternalResourceKey;

template<>
struct ObGetExternalResourceKey<uint64_t, ObSimpleExternalResourceSchema*>
{
  uint64_t operator()(const ObSimpleExternalResourceSchema *resource_info) const
  {
    return nullptr == resource_info ? OB_INVALID_ID : resource_info->get_resource_id();
  }
};

template<>
struct ObGetExternalResourceKey<ObExternalResourceHashWrapper, ObSimpleExternalResourceSchema*>
{
  ObExternalResourceHashWrapper operator()(const ObSimpleExternalResourceSchema *resource_info) const
  {
    ObExternalResourceHashWrapper wrapper;

    if (OB_NOT_NULL(resource_info)) {
      wrapper.set_tenant_id(resource_info->get_tenant_id());
      wrapper.set_database_id(resource_info->get_database_id());
      wrapper.set_resource_name(resource_info->get_name());
      wrapper.set_case_mode();
    }

    return wrapper;
  }
};

class ObExternalResourceMgr final
{
public:
  using ExternalResourceInfos = ObSortedVector<ObSimpleExternalResourceSchema*>;
  using ExternalResourceIdMap = common::hash::ObPointerHashMap<uint64_t, ObSimpleExternalResourceSchema*, ObGetExternalResourceKey, 128>;
  using ExternalResourceNameMap = common::hash::ObPointerHashMap<ObExternalResourceHashWrapper, ObSimpleExternalResourceSchema*, ObGetExternalResourceKey, 128>;
  using ExternalResourceIter = ExternalResourceInfos::iterator;
  using ExternalResourceConstIter = ExternalResourceInfos::const_iterator;

  ObExternalResourceMgr();
  explicit ObExternalResourceMgr(common::ObIAllocator &allocator);

  ObExternalResourceMgr(const ObExternalResourceMgr &other) = delete;
  ObExternalResourceMgr &operator=(const ObExternalResourceMgr &other) = delete;

  virtual ~ObExternalResourceMgr() = default;

  int init();
  void reset();

  inline bool check_inner_stat() const { return is_inited_; }

  int assign(const ObExternalResourceMgr &other);
  int deep_copy(const ObExternalResourceMgr &other);

  int get_external_resource_schema_count(int64_t &external_resource_schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;

  int add_external_resource(const ObSimpleExternalResourceSchema &external_resource_schema);
  int add_external_resources(const common::ObIArray<ObSimpleExternalResourceSchema> &external_resource_schemas);
  int del_external_resource(const ObTenantExternalResourceId &external_resource);
  int get_external_resource_schema(const uint64_t external_resource_id,
                                   const ObSimpleExternalResourceSchema *&external_resource_schema) const;
  int get_external_resource_schema(const uint64_t tenant_id,
                                   const uint64_t database_id,
                                   const common::ObString &name,
                                   const ObSimpleExternalResourceSchema *&external_resource_schema) const;
  int get_external_resource_schemas_in_tenant(const uint64_t tenant_id,
                                              common::ObIArray<const ObSimpleExternalResourceSchema *> &external_resource_schemas) const;
  int get_external_resource_schemas_in_database(const uint64_t tenant_id,
                                                const uint64_t database_id,
                                                common::ObIArray<const ObSimpleExternalResourceSchema *> &external_resource_schemas) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);

  inline static bool compare_external_resource(const ObSimpleExternalResourceSchema *lhs,
                                               const ObSimpleExternalResourceSchema *rhs)
  {
    return lhs->get_resource_id() < rhs->get_resource_id();
  }

  inline static bool equal_external_resource(const ObSimpleExternalResourceSchema *lhs,
                                   const ObSimpleExternalResourceSchema *rhs) {
    return lhs->get_resource_id() == rhs->get_resource_id();
  }

private:
  inline static bool compare_with_tenant_external_resource_id(const ObSimpleExternalResourceSchema *lhs,
                                                              const ObTenantExternalResourceId &rhs)
  {
    return OB_ISNULL(lhs) ? false : (lhs->get_tenant_external_resource_id() < rhs);
  }

  inline static bool equal_to_tenant_external_resource_id(const ObSimpleExternalResourceSchema *lhs,
                                                          const ObTenantExternalResourceId &rhs)
  {
    return OB_ISNULL(lhs) ? false : (lhs->get_tenant_external_resource_id() == rhs);
  }

  int rebuild_external_resource_hashmap();

private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  ExternalResourceInfos external_resource_infos_;
  ExternalResourceIdMap external_resource_id_map_;
  ExternalResourceNameMap external_resource_name_map_;
};

}
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SRC_SHARE_SCHEMA_OB_EXTERNAL_RESOURCE_MGR_H_
