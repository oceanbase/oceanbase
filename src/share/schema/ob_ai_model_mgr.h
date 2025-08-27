/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SRC_SHARE_SCHEMA_OB_AI_MODEL_MGR_H_
#define OCEANBASE_SRC_SHARE_SCHEMA_OB_AI_MODEL_MGR_H_

#include "share/schema/ob_schema_struct.h"
#include "share/ai_service/ob_ai_service_struct.h"

namespace oceanbase
{

namespace share
{

namespace schema
{

class ObTenantAiModelId final
{
public:
  ObTenantAiModelId() : tenant_id_(OB_INVALID_TENANT_ID), ai_model_id_(OB_INVALID_ID) {}
  ObTenantAiModelId(const uint64_t tenant_id, const uint64_t ai_model_id) : tenant_id_(tenant_id), ai_model_id_(ai_model_id) {}
  bool operator==(const ObTenantAiModelId &other) const
  {
    return tenant_id_ == other.tenant_id_ && ai_model_id_ == other.ai_model_id_;
  }
  bool is_valid() const { return tenant_id_ != OB_INVALID_TENANT_ID && ai_model_id_ != OB_INVALID_ID; }
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE uint64_t get_model_id() const { return ai_model_id_; }
  TO_STRING_KV(K_(tenant_id), K_(ai_model_id));

private:
  uint64_t tenant_id_;
  uint64_t ai_model_id_;
};

class ObAiModelSchema final : public ObSchema
{
  OB_UNIS_VERSION(1);
public:
  ObAiModelSchema() { reset(); }
  ~ObAiModelSchema() = default;
  explicit ObAiModelSchema(common::ObIAllocator *allocator) : ObSchema(allocator) { reset(); }
  bool is_valid() const override
  {
    return ObSchema::is_valid()
          && tenant_id_ != OB_INVALID_TENANT_ID
          && model_id_ != OB_INVALID_ID
          && !name_.empty()
          && type_ != EndpointType::MAX_TYPE
          && !model_name_.empty()
          && schema_version_ != OB_INVALID_VERSION;
  }

  void reset()
  {
    tenant_id_ = OB_INVALID_TENANT_ID;
    model_id_ = OB_INVALID_ID;
    name_.reset();
    type_ = EndpointType::MAX_TYPE;
    model_name_.reset();
    schema_version_ = OB_INVALID_VERSION;
    case_mode_ = common::OB_NAME_CASE_INVALID;
  }

  int assign(const ObAiModelSchema &other);
  int assign(const uint64_t tenant_id, const ObAiServiceModelInfo &model_info);
  int64_t get_convert_size() const override;

  OB_INLINE uint64_t get_model_id() const { return model_id_; }
  OB_INLINE uint64_t get_tenant_id() const { return tenant_id_; }
  OB_INLINE ObString get_name() const { return name_; }
  OB_INLINE EndpointType::TYPE get_type() const { return type_; }
  OB_INLINE ObString get_model_name() const { return model_name_; }
  OB_INLINE int64_t get_schema_version() const { return schema_version_; }
  OB_INLINE common::ObNameCaseMode get_case_mode() const { return case_mode_; }

  OB_INLINE void set_tenant_id(const uint64_t &tenant_id) { tenant_id_ = tenant_id; }
  OB_INLINE void set_model_id(const uint64_t &model_id) { model_id_ = model_id; }
  OB_INLINE int set_name(const ObString &name) { return deep_copy_str(name, name_); }
  OB_INLINE void set_type(const EndpointType::TYPE &type) { type_ = type; }
  OB_INLINE int set_model_name(const ObString &model_name) { return deep_copy_str(model_name, model_name_); }
  OB_INLINE void set_schema_version(const int64_t &schema_version) { schema_version_ = schema_version; }
  OB_INLINE void set_case_mode(const common::ObNameCaseMode &case_mode) { case_mode_ = case_mode; }

  TO_STRING_KV(K_(tenant_id),
               K_(model_id),
               K_(name),
               K_(type),
               K_(model_name),
               K_(schema_version));

private:
  uint64_t tenant_id_;
  uint64_t model_id_;
  ObString name_;
  EndpointType::TYPE type_;
  ObString model_name_;
  int64_t schema_version_;
  common::ObNameCaseMode case_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObAiModelSchema);
};

class ObAiModelHashWrapper final
{
public:
  ObAiModelHashWrapper() { reset(); };
  ObAiModelHashWrapper(uint64_t tenant_id, const ObString &name, common::ObNameCaseMode case_mode)
    : tenant_id_(tenant_id), name_(name), case_mode_(case_mode)
  {
  }
  ObAiModelHashWrapper(const ObAiModelHashWrapper &other) = default;
  ObAiModelHashWrapper &operator=(const ObAiModelHashWrapper &other) = default;
  ~ObAiModelHashWrapper() = default;

  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_ai_model_name(const common::ObString &ai_model_name) { name_ = ai_model_name; }
  inline void set_case_mode(const common::ObNameCaseMode &case_mode) { case_mode_ = case_mode; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline const common::ObString &get_ai_model_name() const { return name_; }

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

  inline bool operator==(const ObAiModelHashWrapper &other) const
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
struct ObGetAiModelKey;

template<>
struct ObGetAiModelKey<uint64_t, ObAiModelSchema*>
{
  uint64_t operator()(const ObAiModelSchema *ai_model_schema) const
  {
    return nullptr == ai_model_schema ? OB_INVALID_ID : ai_model_schema->get_model_id();
  }
};

template<>
struct ObGetAiModelKey<ObAiModelHashWrapper, ObAiModelSchema*>
{
  ObAiModelHashWrapper operator()(const ObAiModelSchema *ai_model_schema) const
  {
    ObAiModelHashWrapper wrapper;

    if (OB_NOT_NULL(ai_model_schema)) {
      wrapper.set_tenant_id(ai_model_schema->get_tenant_id());
      wrapper.set_ai_model_name(ai_model_schema->get_name());
      wrapper.set_case_mode(ai_model_schema->get_case_mode());
    }
    return wrapper;
  }
};

class ObAiModelMgr final
{
public:
  using AiModelInfos = ObSortedVector<ObAiModelSchema*>;
  using AiModelIdMap = common::hash::ObPointerHashMap<uint64_t, ObAiModelSchema*, ObGetAiModelKey, 80>;
  using AiModelNameMap = common::hash::ObPointerHashMap<ObAiModelHashWrapper, ObAiModelSchema*, ObGetAiModelKey, 80>;
  using AiModelIter = AiModelInfos::iterator;
  using AiModelConstIter = AiModelInfos::const_iterator;

  ObAiModelMgr();
  explicit ObAiModelMgr(common::ObIAllocator &allocator);
  virtual ~ObAiModelMgr() = default;

  int init();
  void reset();
  int assign(const ObAiModelMgr &other);
  int deep_copy(const ObAiModelMgr &other);
  int get_ai_model_schema_count(int64_t &ai_model_schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
  int get_ai_model_schema(const uint64_t ai_model_id, const ObAiModelSchema *&ai_model_schema) const;
  int get_ai_model_schema(const uint64_t tenant_id, const common::ObString &name, const common::ObNameCaseMode case_mode, const ObAiModelSchema *&ai_model_schema) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  int add_ai_model(const ObAiModelSchema &ai_model_schema, common::ObNameCaseMode case_mode);
  int add_ai_models(const common::ObIArray<ObAiModelSchema> &ai_model_schemas, common::ObNameCaseMode case_mode);
  int del_ai_model(const ObTenantAiModelId &tenant_ai_model_id);

private:
  int get_ai_model_schemas_in_tenant(const uint64_t tenant_id, common::ObIArray<const ObAiModelSchema *> &ai_model_schemas) const;
  int rebuild_ai_model_hashmap();

  OB_INLINE static bool compare_ai_model(const ObAiModelSchema *lhs, const ObAiModelSchema *rhs)
  {
    return lhs->get_model_id() < rhs->get_model_id();
  }

  OB_INLINE static bool equal_ai_model(const ObAiModelSchema *lhs, const ObAiModelSchema *rhs) {
    return lhs->get_model_id() == rhs->get_model_id();
  }
  OB_INLINE static bool equal_to_tenant_ai_model_id(const ObAiModelSchema *lhs, const ObTenantAiModelId &rhs) {
    return lhs->get_tenant_id() == rhs.get_tenant_id() && lhs->get_model_id() == rhs.get_model_id();
  }
  OB_INLINE static bool compare_with_tenant_ai_model_id(const ObAiModelSchema *lhs, const ObTenantAiModelId &rhs) {
    return lhs->get_tenant_id() < rhs.get_tenant_id() || (lhs->get_tenant_id() == rhs.get_tenant_id() && lhs->get_model_id() < rhs.get_model_id());
  }
private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  AiModelInfos ai_model_infos_;
  AiModelIdMap ai_model_id_map_;
  AiModelNameMap ai_model_name_map_;
  DISALLOW_COPY_AND_ASSIGN(ObAiModelMgr);
};

}
} // namespace share
} // namespace oceanbase

 #endif // OCEANBASE_SRC_SHARE_SCHEMA_OB_EXTERNAL_RESOURCE_MGR_H_
