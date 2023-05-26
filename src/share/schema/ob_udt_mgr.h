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

#ifndef OCEANBASE_SRC_SHARE_SCHEMA_OB_UDT_MGR_H_
#define OCEANBASE_SRC_SHARE_SCHEMA_OB_UDT_MGR_H_

#include "share/ob_define.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/container/ob_vector.h"
#include "lib/allocator/page_arena.h"
#include "ob_udt_info.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
struct ObTenantUDTId
{
public:
  ObTenantUDTId()
      : tenant_id_(common::OB_INVALID_ID),
        type_id_(common::OB_INVALID_ID) {}
  ObTenantUDTId(uint64_t tenant_id, uint64_t type_id)
      : tenant_id_(tenant_id),
        type_id_(type_id) {}
  ObTenantUDTId(const ObTenantUDTId &other)
      : tenant_id_(other.tenant_id_),
        type_id_(other.type_id_) {}
  ObTenantUDTId &operator =(const ObTenantUDTId &other)
  {
    tenant_id_ = other.tenant_id_;
    type_id_ = other.type_id_;
    return *this;
  }
  bool operator ==(const ObTenantUDTId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_)
           && (type_id_ == rhs.type_id_);
  }
  bool operator !=(const ObTenantUDTId &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator <(const ObTenantUDTId &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (tenant_id_ == rhs.tenant_id_) {
      bret = type_id_ < rhs.type_id_;
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&type_id_, sizeof(type_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID)
        && (type_id_ != common::OB_INVALID_ID);
  }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_type_id() const { return type_id_; }
  TO_STRING_KV(K_(tenant_id), K_(type_id));
private:
  uint64_t tenant_id_;
  uint64_t type_id_;
};

class ObSimpleUDTSchema : public ObSchema
{
public:
  ObSimpleUDTSchema();
  explicit ObSimpleUDTSchema(common::ObIAllocator *allocator);
  ObSimpleUDTSchema(const ObSimpleUDTSchema &src_schema);
  virtual ~ObSimpleUDTSchema();
  ObSimpleUDTSchema &operator =(const ObSimpleUDTSchema &other);
  bool operator ==(const ObSimpleUDTSchema &other) const;
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    database_id_ = common::OB_INVALID_ID;
    package_id_ = common::OB_INVALID_ID;
    type_id_ = common::OB_INVALID_ID;
    reset_string(type_name_);
    schema_version_ = common::OB_INVALID_VERSION;
    typecode_ = UDT_INVALID_TYPE_CODE;
    ObSchema::reset();
  }
  int64_t get_convert_size() const;
  bool is_valid() const
  {
    return (common::OB_INVALID_ID != tenant_id_
            && common::OB_INVALID_ID != database_id_
            && common::OB_INVALID_ID != type_id_
            && !type_name_.empty()
            && UDT_INVALID_TYPE_CODE != typecode_
            && schema_version_ >= 0);
  }
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline void set_udt_id(uint64_t udt_id) { type_id_ = udt_id; }
  inline uint64_t get_udt_id() const { return type_id_; }
  inline void set_type_id(uint64_t type_id) { type_id_ = type_id; }
  inline uint64_t get_type_id() const { return type_id_; }
  inline void set_package_id(uint64_t package_id) { package_id_ = package_id; }
  inline uint64_t get_package_id() const { return package_id_; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline void set_database_id(const uint64_t database_id) { database_id_ = database_id; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline int set_type_name(const common::ObString &type_name)
  { return deep_copy_str(type_name, type_name_); }
  inline const common::ObString &get_type_name() const { return type_name_; }
  inline ObTenantUDTId get_udt_key() const
  { return ObTenantUDTId(tenant_id_, type_id_); }
  ObUDTTypeCode get_typecode() const { return typecode_; }
  void set_typecode(ObUDTTypeCode typecode) { typecode_ = typecode; }
  TO_STRING_KV(K_(tenant_id),
               K_(database_id),
               K_(package_id),
               K_(type_id),
               K_(type_name),
               K_(typecode),
               K_(schema_version));
private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t package_id_;
  uint64_t type_id_;
  common::ObString type_name_;
  ObUDTTypeCode typecode_;
  int64_t schema_version_;
};

class ObUDTNameHashWrapper
{
public:
  ObUDTNameHashWrapper()
  : tenant_id_(common::OB_INVALID_ID),
    database_id_(common::OB_INVALID_ID),
    package_id_(common::OB_INVALID_ID) {}
  ObUDTNameHashWrapper(uint64_t tenant_id, uint64_t database_id, uint64_t package_id,
                       const common::ObString &type_name)
      : tenant_id_(tenant_id),
        database_id_(database_id),
        package_id_(package_id),
        type_name_(type_name) {
        }
  ~ObUDTNameHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator ==(const ObUDTNameHashWrapper &rv) const;
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_database_id(uint64_t database_id) { database_id_ = database_id; }
  inline void set_package_id(uint64_t package_id) { package_id_ = package_id; }
  inline void set_type_name(const common::ObString &type_name) { type_name_ = type_name; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline uint64_t get_package_id() const { return package_id_; }
  inline const common::ObString &get_type_name() const { return type_name_; }
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(package_id), K_(type_name));
private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t package_id_;
  common::ObString type_name_;
};

inline bool ObUDTNameHashWrapper::operator ==(const ObUDTNameHashWrapper &rv) const
{
  ObCompareNameWithTenantID name_cmp(tenant_id_);
  return (tenant_id_ == rv.tenant_id_)
      && (database_id_ == rv.database_id_)
      && (package_id_ == rv.package_id_)
      && (0 == name_cmp.compare(type_name_, rv.type_name_));
}
 
inline uint64_t ObUDTNameHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  common::ObCollationType cs_type = common::CS_TYPE_UTF8MB4_GENERAL_CI;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::murmurhash(&package_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::ObCharset::hash(cs_type, type_name_, hash_ret);
  return hash_ret;
}

template<class T, class V>
struct ObGetUDTKey
{
  void operator()(const T & t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetUDTKey<uint64_t, ObSimpleUDTSchema *>
{
  uint64_t operator()(const ObSimpleUDTSchema *udt_schema) const
  {
    return NULL != udt_schema ? udt_schema->get_type_id() : common::OB_INVALID_ID;
  }
};

template<>
struct ObGetUDTKey<ObUDTNameHashWrapper, ObSimpleUDTSchema *>
{
  ObUDTNameHashWrapper operator ()(const ObSimpleUDTSchema *udt_schema) const
  {
    ObUDTNameHashWrapper name_wrap;
    if (udt_schema != NULL) {
      name_wrap.set_tenant_id(udt_schema->get_tenant_id());
      name_wrap.set_database_id(udt_schema->get_database_id());
      name_wrap.set_package_id(udt_schema->get_package_id());
      name_wrap.set_type_name(udt_schema->get_type_name());
    }
    return name_wrap;
  }
};

class ObUDTMgr
{
  typedef common::ObSortedVector<ObSimpleUDTSchema *> UDTInfos;
  typedef common::hash::ObPointerHashMap<uint64_t, ObSimpleUDTSchema *,
      ObGetUDTKey, 512> UDTIdMap;
  typedef common::hash::ObPointerHashMap<ObUDTNameHashWrapper, ObSimpleUDTSchema *,
      ObGetUDTKey, 512> UDTNameMap;
  typedef UDTInfos::iterator UDTIter;
  typedef UDTInfos::const_iterator ConstUDTIter;
public:
  ObUDTMgr();
  explicit ObUDTMgr(common::ObIAllocator &allocator);
  virtual ~ObUDTMgr();
  int init();
  void reset();
  ObUDTMgr &operator =(const ObUDTMgr &other);
  int assign(const ObUDTMgr &other);
  int deep_copy(const ObUDTMgr &other);
  void dump() const;
  int get_udt_schema_count(int64_t &udt_schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;

  int add_udts(const common::ObIArray<ObSimpleUDTSchema> &udt_schemas);
  int del_udts(const common::ObIArray<ObTenantUDTId> &routines);
  int add_udt(const ObSimpleUDTSchema &udt_schema);
  int del_udt(const ObTenantUDTId &type_id);
  int get_udt_schema(uint64_t tenant_id, uint64_t database_id,
                     uint64_t package_id, const common::ObString &type_name,
                     const ObSimpleUDTSchema *&udt_schema) const;
  int get_udt_schema(uint64_t type_id, const ObSimpleUDTSchema *&udt_schema) const;
  int get_udt_schemas_in_tenant(uint64_t tenant_id,
                                common::ObIArray<const ObSimpleUDTSchema *> &udt_schemas) const;
  int get_udt_schemas_in_database(uint64_t tenant_id, uint64_t database_id,
                                  common::ObIArray<const ObSimpleUDTSchema *> &udt_schemas) const;
  int get_udt_schemas_in_package(uint64_t tenant_id, uint64_t package_id,
                                 common::ObIArray<const ObSimpleUDTSchema *> &udt_schemas) const;
  int del_udt_schemas_in_tenant(uint64_t tenant_id);
  int del_udt_schemas_in_database(uint64_t tenant_id, uint64_t database_id);
  int del_udt_schemas_in_package(uint64_t tenant_id, uint64_t package_id);

private:
  inline bool check_inner_stat() const;
  inline static bool compare_udt(const ObSimpleUDTSchema *lhs,
                                 const ObSimpleUDTSchema *rhs);
  inline static bool equal_udt(const ObSimpleUDTSchema *lhs,
                               const ObSimpleUDTSchema *rhs);
  inline static bool compare_with_tenant_type_id(const ObSimpleUDTSchema *lhs,
                                                 const ObTenantUDTId &tenant_type_id);
  inline static bool equal_with_tenant_type_id(const ObSimpleUDTSchema *lhs,
                                               const ObTenantUDTId &tenant_type_id);
  int rebuild_udt_hashmap();
private:
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  UDTInfos udt_infos_;
  UDTIdMap type_id_map_;
  UDTNameMap type_name_map_;
  bool is_inited_;
};
}  // namespace schema
}  // namespace share
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SHARE_SCHEMA_OB_UDT_MGR_H_ */
