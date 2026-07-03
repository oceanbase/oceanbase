/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SHARE_SCHEMA_OB_AI_PROVIDER_MGR_H_
#define OCEANBASE_SRC_SHARE_SCHEMA_OB_AI_PROVIDER_MGR_H_

#include "share/schema/ob_schema_struct.h"
#include "share/ai_service/ob_ai_service_struct.h"

namespace oceanbase
{

namespace share
{

namespace schema
{

class ObTenantAIProviderId final
{
public:
  ObTenantAIProviderId() : tenant_id_(OB_INVALID_TENANT_ID), provider_id_(OB_INVALID_ID) {}
  ObTenantAIProviderId(const uint64_t tenant_id, const uint64_t provider_id) : tenant_id_(tenant_id), provider_id_(provider_id) {}
  bool operator==(const ObTenantAIProviderId &other) const
  {
    return tenant_id_ == other.tenant_id_ && provider_id_ == other.provider_id_;
  }
  bool is_valid() const { return tenant_id_ != OB_INVALID_TENANT_ID && provider_id_ != OB_INVALID_ID; }
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE uint64_t get_provider_id() const { return provider_id_; }
  TO_STRING_KV(K_(tenant_id), K_(provider_id));

private:
  uint64_t tenant_id_;
  uint64_t provider_id_;
};

class ObAIProviderSchema final : public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  ObAIProviderSchema() { reset(); }
  ~ObAIProviderSchema() = default;
  explicit ObAIProviderSchema(common::ObIAllocator *allocator) : ObSchema(allocator) { reset(); }
  bool is_valid() const override
  {
    return ObSchema::is_valid()
          && tenant_id_ != OB_INVALID_TENANT_ID
          && provider_id_ != OB_INVALID_ID
          && !name_.empty()
          && !protocol_.empty()
          && !base_url_.empty()
          && schema_version_ != OB_INVALID_VERSION;
  }

  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    provider_id_ = OB_INVALID_ID;
    name_.reset();
    protocol_.reset();
    base_url_.reset();
    access_key_.reset();
    schema_version_ = OB_INVALID_VERSION;
    case_mode_ = common::OB_NAME_CASE_INVALID;
  }

  int assign(const ObAIProviderSchema &other);
  int64_t get_convert_size() const override;

  OB_INLINE uint64_t get_provider_id() const { return provider_id_; }
  OB_INLINE uint64_t get_ai_provider_id() const { return provider_id_; }
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE ObString get_name() const { return name_; }
  OB_INLINE ObString get_protocol() const { return protocol_; }
  OB_INLINE ObString get_base_url() const { return base_url_; }
  OB_INLINE ObString get_access_key() const { return access_key_; }
  OB_INLINE int64_t get_schema_version() const { return schema_version_; }
  OB_INLINE common::ObNameCaseMode get_case_mode() const { return case_mode_; }

  OB_INLINE void set_tenant_id(const uint64_t &tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE void set_provider_id(const uint64_t &provider_id) { provider_id_ = provider_id; }
  OB_INLINE int set_name(const ObString &name) { return deep_copy_str(name, name_); }
  OB_INLINE int set_protocol(const ObString &protocol) { return deep_copy_str(protocol, protocol_); }
  OB_INLINE int set_base_url(const ObString &base_url) { return deep_copy_str(base_url, base_url_); }
  OB_INLINE int set_access_key(const ObString &access_key) { return deep_copy_str(access_key, access_key_); }
  OB_INLINE void set_schema_version(const int64_t &schema_version) { schema_version_ = schema_version; }
  OB_INLINE void set_case_mode(const common::ObNameCaseMode &case_mode) { case_mode_ = case_mode; }

  TO_STRING_KV(K_(tenant_id),
               K_(provider_id),
               K_(name),
               K_(protocol),
               K_(base_url),
               K_(schema_version));

private:
  uint64_t tenant_id_;
  uint64_t provider_id_;
  ObString name_;
  ObString protocol_;
  ObString base_url_;
  ObString access_key_;
  int64_t schema_version_;
  common::ObNameCaseMode case_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObAIProviderSchema);
};

class ObAIProviderHashWrapper final
{
public:
  ObAIProviderHashWrapper() { reset(); };
  ObAIProviderHashWrapper(uint64_t tenant_id, const ObString &name, common::ObNameCaseMode case_mode)
    : tenant_id_(tenant_id), name_(name), case_mode_(case_mode)
  {
  }
  ObAIProviderHashWrapper(const ObAIProviderHashWrapper &other) = default;
  ObAIProviderHashWrapper &operator=(const ObAIProviderHashWrapper &other) = default;
  ~ObAIProviderHashWrapper() = default;

  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_provider_name(const common::ObString &name) { name_ = name; }
  inline void set_case_mode(const common::ObNameCaseMode &case_mode) { case_mode_ = case_mode; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline const common::ObString &get_provider_name() const { return name_; }

  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    name_.reset();
    case_mode_ = common::OB_NAME_CASE_INVALID;
  }

  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;

    hash_ret = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_ret);
    common::ObCollationType cs_type = ObSchema::get_cs_type_with_cmp_mode(case_mode_);
    hash_ret = common::ObCharset::hash(cs_type, name_, hash_ret);

    return hash_ret;
  }

  inline bool operator==(const ObAIProviderHashWrapper &other) const
  {
    ObCompareNameWithTenantID name_cmp(tenant_id_, case_mode_);

    return (tenant_id_ == other.tenant_id_)
            && (case_mode_ == other.case_mode_)
            && (0 == name_cmp.compare(name_, other.name_));
  }

  TO_STRING_KV(K_(tenant_id), K_(name), K_(case_mode));
private:
  uint64_t tenant_id_;
  ObString name_;
  common::ObNameCaseMode case_mode_;
};

template<typename K, typename V>
struct ObGetAIProviderKey;

template<>
struct ObGetAIProviderKey<uint64_t, ObAIProviderSchema*>
{
  uint64_t operator()(const ObAIProviderSchema *provider_schema) const
  {
    return nullptr == provider_schema ? OB_INVALID_ID : provider_schema->get_provider_id();
  }
};

template<>
struct ObGetAIProviderKey<ObAIProviderHashWrapper, ObAIProviderSchema*>
{
  ObAIProviderHashWrapper operator()(const ObAIProviderSchema *provider_schema) const
  {
    ObAIProviderHashWrapper wrapper;

    if (OB_NOT_NULL(provider_schema)) {
      wrapper.set_tenant_id(provider_schema->get_tenant_id());
      wrapper.set_provider_name(provider_schema->get_name());
      wrapper.set_case_mode(provider_schema->get_case_mode());
    }
    return wrapper;
  }
};

class ObAIProviderMgr final
{
public:
  using ProviderInfos = ObSortedVector<ObAIProviderSchema*>;
  using ProviderIdMap = common::hash::ObPointerHashMap<uint64_t, ObAIProviderSchema*, ObGetAIProviderKey, 80>;
  using ProviderNameMap = common::hash::ObPointerHashMap<ObAIProviderHashWrapper, ObAIProviderSchema*, ObGetAIProviderKey, 80>;
  using ProviderIter = ProviderInfos::iterator;
  using ProviderConstIter = ProviderInfos::const_iterator;

  ObAIProviderMgr();
  explicit ObAIProviderMgr(common::ObIAllocator &allocator);
  virtual ~ObAIProviderMgr() = default;

  int init();
  void reset();
  int assign(const ObAIProviderMgr &other);
  int deep_copy(const ObAIProviderMgr &other);
  int get_ai_provider_schema_count(int64_t &provider_schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
  int get_ai_provider_schema(const uint64_t provider_id, const ObAIProviderSchema *&provider_schema) const;
  int get_ai_provider_schema(const uint64_t tenant_id, const common::ObString &name, const common::ObNameCaseMode case_mode, const ObAIProviderSchema *&provider_schema) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  int add_ai_provider(const ObAIProviderSchema &provider_schema, common::ObNameCaseMode case_mode);
  int add_ai_providers(const common::ObIArray<ObAIProviderSchema> &provider_schemas, common::ObNameCaseMode case_mode);
  int del_ai_provider(const ObTenantAIProviderId &tenant_provider_id);

private:
  int get_ai_provider_schemas_in_tenant(const uint64_t tenant_id, common::ObIArray<const ObAIProviderSchema *> &provider_schemas) const;
  int rebuild_ai_provider_hashmap();

  OB_INLINE static bool compare_provider(const ObAIProviderSchema *lhs, const ObAIProviderSchema *rhs)
  {
    return lhs->get_provider_id() < rhs->get_provider_id();
  }

  OB_INLINE static bool equal_provider(const ObAIProviderSchema *lhs, const ObAIProviderSchema *rhs) {
    return lhs->get_provider_id() == rhs->get_provider_id();
  }
  OB_INLINE static bool equal_to_tenant_provider_id(const ObAIProviderSchema *lhs, const ObTenantAIProviderId &rhs) {
    return lhs->get_tenant_id() == rhs.get_tenant_id() && lhs->get_provider_id() == rhs.get_provider_id();
  }
  OB_INLINE static bool compare_with_tenant_provider_id(const ObAIProviderSchema *lhs, const ObTenantAIProviderId &rhs) {
    return lhs->get_tenant_id() < rhs.get_tenant_id() || (lhs->get_tenant_id() == rhs.get_tenant_id() && lhs->get_provider_id() < rhs.get_provider_id());
  }
private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  ProviderInfos provider_infos_;
  ProviderIdMap provider_id_map_;
  ProviderNameMap provider_name_map_;
  DISALLOW_COPY_AND_ASSIGN(ObAIProviderMgr);
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SRC_SHARE_SCHEMA_OB_AI_PROVIDER_MGR_H_
