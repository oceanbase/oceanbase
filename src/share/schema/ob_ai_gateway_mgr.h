/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SHARE_SCHEMA_OB_AI_GATEWAY_MGR_H_
#define OCEANBASE_SRC_SHARE_SCHEMA_OB_AI_GATEWAY_MGR_H_

#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{

namespace share
{

namespace schema
{

class ObTenantAIGatewayId final
{
public:
  ObTenantAIGatewayId() : tenant_id_(OB_INVALID_TENANT_ID), gateway_id_(OB_INVALID_ID) {}
  ObTenantAIGatewayId(const uint64_t tenant_id, const uint64_t gateway_id) : tenant_id_(tenant_id), gateway_id_(gateway_id) {}
  bool operator==(const ObTenantAIGatewayId &other) const
  {
    return tenant_id_ == other.tenant_id_ && gateway_id_ == other.gateway_id_;
  }
  bool is_valid() const { return tenant_id_ != OB_INVALID_TENANT_ID && gateway_id_ != OB_INVALID_ID; }
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE uint64_t get_gateway_id() const { return gateway_id_; }
  TO_STRING_KV(K_(tenant_id), K_(gateway_id));

private:
  uint64_t tenant_id_;
  uint64_t gateway_id_;
};

class ObAIGatewaySchema final : public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  ObAIGatewaySchema() { reset(); }
  ~ObAIGatewaySchema() = default;
  explicit ObAIGatewaySchema(common::ObIAllocator *allocator) : ObSchema(allocator) { reset(); }
  bool is_valid() const override
  {
    return ObSchema::is_valid()
          && tenant_id_ != OB_INVALID_TENANT_ID
          && gateway_id_ != OB_INVALID_ID
          && !name_.empty()
          && !endpoints_.empty()
          && schema_version_ != OB_INVALID_VERSION;
  }

  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    gateway_id_ = OB_INVALID_ID;
    name_.reset();
    endpoints_.reset();
    circuit_breaker_.reset();
    schema_version_ = OB_INVALID_VERSION;
    case_mode_ = common::OB_NAME_CASE_INVALID;
  }

  int assign(const ObAIGatewaySchema &other);
  int64_t get_convert_size() const override;

  OB_INLINE uint64_t get_gateway_id() const { return gateway_id_; }
  OB_INLINE uint64_t get_ai_gateway_id() const { return gateway_id_; }
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE ObString get_name() const { return name_; }
  OB_INLINE ObString get_endpoints() const { return endpoints_; }
  OB_INLINE ObString get_circuit_breaker() const { return circuit_breaker_; }
  OB_INLINE int64_t get_schema_version() const { return schema_version_; }
  OB_INLINE common::ObNameCaseMode get_case_mode() const { return case_mode_; }

  OB_INLINE void set_tenant_id(const uint64_t &tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE void set_gateway_id(const uint64_t &gateway_id) { gateway_id_ = gateway_id; }
  OB_INLINE int set_name(const ObString &name) { return deep_copy_str(name, name_); }
  OB_INLINE int set_endpoints(const ObString &endpoints) { return deep_copy_str(endpoints, endpoints_); }
  OB_INLINE int set_circuit_breaker(const ObString &circuit_breaker) { return deep_copy_str(circuit_breaker, circuit_breaker_); }
  OB_INLINE void set_schema_version(const int64_t &schema_version) { schema_version_ = schema_version; }
  OB_INLINE void set_case_mode(const common::ObNameCaseMode &case_mode) { case_mode_ = case_mode; }

  TO_STRING_KV(K_(tenant_id),
               K_(gateway_id),
               K_(name),
               K_(endpoints),
               K_(circuit_breaker),
               K_(schema_version));

private:
  uint64_t tenant_id_;
  uint64_t gateway_id_;
  ObString name_;
  ObString endpoints_;
  ObString circuit_breaker_;
  int64_t schema_version_;
  common::ObNameCaseMode case_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObAIGatewaySchema);
};

class ObAIGatewayHashWrapper final
{
public:
  ObAIGatewayHashWrapper() { reset(); };
  ObAIGatewayHashWrapper(uint64_t tenant_id, const ObString &name, common::ObNameCaseMode case_mode)
    : tenant_id_(tenant_id), name_(name), case_mode_(case_mode)
  {
  }
  ObAIGatewayHashWrapper(const ObAIGatewayHashWrapper &other) = default;
  ObAIGatewayHashWrapper &operator=(const ObAIGatewayHashWrapper &other) = default;
  ~ObAIGatewayHashWrapper() = default;

  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_gateway_name(const common::ObString &name) { name_ = name; }
  inline void set_case_mode(const common::ObNameCaseMode &case_mode) { case_mode_ = case_mode; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline const common::ObString &get_gateway_name() const { return name_; }

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

  inline bool operator==(const ObAIGatewayHashWrapper &other) const
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
struct ObGetAIGatewayKey;

template<>
struct ObGetAIGatewayKey<uint64_t, ObAIGatewaySchema*>
{
  uint64_t operator()(const ObAIGatewaySchema *gateway_schema) const
  {
    return nullptr == gateway_schema ? OB_INVALID_ID : gateway_schema->get_gateway_id();
  }
};

template<>
struct ObGetAIGatewayKey<ObAIGatewayHashWrapper, ObAIGatewaySchema*>
{
  ObAIGatewayHashWrapper operator()(const ObAIGatewaySchema *gateway_schema) const
  {
    ObAIGatewayHashWrapper wrapper;

    if (OB_NOT_NULL(gateway_schema)) {
      wrapper.set_tenant_id(gateway_schema->get_tenant_id());
      wrapper.set_gateway_name(gateway_schema->get_name());
      wrapper.set_case_mode(gateway_schema->get_case_mode());
    }
    return wrapper;
  }
};

class ObAIGatewayMgr final
{
public:
  using GatewayInfos = ObSortedVector<ObAIGatewaySchema*>;
  using GatewayIdMap = common::hash::ObPointerHashMap<uint64_t, ObAIGatewaySchema*, ObGetAIGatewayKey, 80>;
  using GatewayNameMap = common::hash::ObPointerHashMap<ObAIGatewayHashWrapper, ObAIGatewaySchema*, ObGetAIGatewayKey, 80>;
  using GatewayIter = GatewayInfos::iterator;
  using GatewayConstIter = GatewayInfos::const_iterator;

  ObAIGatewayMgr();
  explicit ObAIGatewayMgr(common::ObIAllocator &allocator);
  virtual ~ObAIGatewayMgr() = default;

  int init();
  void reset();
  int assign(const ObAIGatewayMgr &other);
  int deep_copy(const ObAIGatewayMgr &other);
  int get_ai_gateway_schema_count(int64_t &gateway_schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
  int get_ai_gateway_schema(const uint64_t gateway_id, const ObAIGatewaySchema *&gateway_schema) const;
  int get_ai_gateway_schema(const uint64_t tenant_id, const common::ObString &name, const common::ObNameCaseMode case_mode, const ObAIGatewaySchema *&gateway_schema) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  int add_ai_gateway(const ObAIGatewaySchema &gateway_schema, common::ObNameCaseMode case_mode);
  int add_ai_gateways(const common::ObIArray<ObAIGatewaySchema> &gateway_schemas, common::ObNameCaseMode case_mode);
  int del_ai_gateway(const ObTenantAIGatewayId &tenant_gateway_id);

private:
  int get_ai_gateway_schemas_in_tenant(const uint64_t tenant_id, common::ObIArray<const ObAIGatewaySchema *> &gateway_schemas) const;
  int rebuild_ai_gateway_hashmap();

  OB_INLINE static bool compare_gateway(const ObAIGatewaySchema *lhs, const ObAIGatewaySchema *rhs)
  {
    return lhs->get_gateway_id() < rhs->get_gateway_id();
  }

  OB_INLINE static bool equal_gateway(const ObAIGatewaySchema *lhs, const ObAIGatewaySchema *rhs) {
    return lhs->get_gateway_id() == rhs->get_gateway_id();
  }
  OB_INLINE static bool equal_to_tenant_gateway_id(const ObAIGatewaySchema *lhs, const ObTenantAIGatewayId &rhs) {
    return lhs->get_tenant_id() == rhs.get_tenant_id() && lhs->get_gateway_id() == rhs.get_gateway_id();
  }
  OB_INLINE static bool compare_with_tenant_gateway_id(const ObAIGatewaySchema *lhs, const ObTenantAIGatewayId &rhs) {
    return lhs->get_tenant_id() < rhs.get_tenant_id() || (lhs->get_tenant_id() == rhs.get_tenant_id() && lhs->get_gateway_id() < rhs.get_gateway_id());
  }
private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  GatewayInfos gateway_infos_;
  GatewayIdMap gateway_id_map_;
  GatewayNameMap gateway_name_map_;
  DISALLOW_COPY_AND_ASSIGN(ObAIGatewayMgr);
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SRC_SHARE_SCHEMA_OB_AI_GATEWAY_MGR_H_
