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

#ifndef OCEANBASE_SRC_SHARE_SCHEMA_OB_ROUTINE_MGR_H_
#define OCEANBASE_SRC_SHARE_SCHEMA_OB_ROUTINE_MGR_H_

#include "share/ob_define.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/container/ob_vector.h"
#include "lib/allocator/page_arena.h"
#include "ob_routine_info.h"
namespace oceanbase
{
namespace share
{
namespace schema
{
struct ObTenantRoutineId
{
public:
  ObTenantRoutineId()
      : tenant_id_(common::OB_INVALID_ID),
        routine_id_(common::OB_INVALID_ID) {}
  ObTenantRoutineId(uint64_t tenant_id, uint64_t routine_id)
      : tenant_id_(tenant_id),
        routine_id_(routine_id) {}
  ObTenantRoutineId(const ObTenantRoutineId &other)
      : tenant_id_(other.tenant_id_),
        routine_id_(other.routine_id_) {}
  ObTenantRoutineId &operator =(const ObTenantRoutineId &other)
  {
    tenant_id_ = other.tenant_id_;
    routine_id_ = other.routine_id_;
    return *this;
  }
  bool operator ==(const ObTenantRoutineId &rhs) const
  {
    return (tenant_id_ == rhs.tenant_id_)
           && (routine_id_ == rhs.routine_id_);
  }
  bool operator !=(const ObTenantRoutineId &rhs) const
  {
    return !(*this == rhs);
  }
  bool operator <(const ObTenantRoutineId &rhs) const
  {
    bool bret = tenant_id_ < rhs.tenant_id_;
    if (tenant_id_ == rhs.tenant_id_) {
      bret = routine_id_ < rhs.routine_id_;
    }
    return bret;
  }
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_ret = common::murmurhash(&routine_id_, sizeof(routine_id_), hash_ret);
    return hash_ret;
  }
  bool is_valid() const
  {
    return (tenant_id_ != common::OB_INVALID_ID)
        && (routine_id_ != common::OB_INVALID_ID);
  }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_routine_id() const { return routine_id_; }
  TO_STRING_KV(K_(tenant_id), K_(routine_id));
private:
  uint64_t tenant_id_;
  uint64_t routine_id_;
};

class ObSimpleRoutineSchema : public ObSchema
{
public:
  ObSimpleRoutineSchema();
  explicit ObSimpleRoutineSchema(common::ObIAllocator *allocator);
  virtual ~ObSimpleRoutineSchema();
  bool operator ==(const ObSimpleRoutineSchema &other) const;
  int assign(const ObSimpleRoutineSchema &other);
  void reset()
  {
    tenant_id_ = common::OB_INVALID_ID;
    database_id_ = common::OB_INVALID_ID;
    package_id_ = common::OB_INVALID_ID;
    routine_id_ = common::OB_INVALID_ID;
    reset_string(routine_name_);
    schema_version_ = common::OB_INVALID_VERSION;
    routine_type_ = INVALID_ROUTINE_TYPE;
    overload_ = common::OB_INVALID_INDEX;
    reset_string(priv_user_);
    ObSchema::reset();
  }
  int64_t get_convert_size() const;
  bool is_valid() const
  {
    return (common::OB_INVALID_ID != tenant_id_
            && common::OB_INVALID_ID != database_id_
            && common::OB_INVALID_ID != routine_id_
            && !routine_name_.empty()
            && common::OB_INVALID_INDEX != overload_
            && INVALID_ROUTINE_TYPE != routine_type_
            && schema_version_ >= 0);
  }
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline void set_routine_id(uint64_t routine_id) { routine_id_ = routine_id; }
  inline uint64_t get_routine_id() const { return routine_id_; }
  inline void set_package_id(uint64_t package_id) { package_id_ = package_id; }
  inline uint64_t get_package_id() const { return package_id_; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline void set_database_id(const uint64_t database_id) { database_id_ = database_id; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline void set_overload(const uint64_t overload) { overload_ = overload; }
  inline uint64_t get_overload() const { return overload_; }
  inline int set_routine_name(const common::ObString &routine_name)
  { return deep_copy_str(routine_name, routine_name_); }
  inline const common::ObString &get_routine_name() const { return routine_name_; }
  inline ObTenantRoutineId get_routine_key() const
  { return ObTenantRoutineId(tenant_id_, routine_id_); }
  ObRoutineType get_routine_type() const { return routine_type_; }
  void set_routine_type(ObRoutineType routine_type) { routine_type_ = routine_type; }
  inline int set_priv_user(const common::ObString &priv_user)
  { return deep_copy_str(priv_user, priv_user_); }
  inline const common::ObString &get_priv_user() const { return priv_user_; }
  TO_STRING_KV(K_(tenant_id),
               K_(database_id),
               K_(package_id),
               K_(routine_id),
               K_(routine_name),
               K_(routine_type),
               K_(overload),
               K_(schema_version),
               K_(priv_user));
private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t package_id_;
  uint64_t routine_id_;
  common::ObString routine_name_;
  ObRoutineType routine_type_;
  uint64_t overload_;
  int64_t schema_version_;
  common::ObString priv_user_;
  DISABLE_COPY_ASSIGN(ObSimpleRoutineSchema);
};

class ObRoutineNameHashWrapper
{
public:
  ObRoutineNameHashWrapper()
  : tenant_id_(common::OB_INVALID_ID),
    database_id_(common::OB_INVALID_ID),
    package_id_(common::OB_INVALID_ID),
    overload_(common::OB_INVALID_INDEX),
    routine_type_(INVALID_ROUTINE_TYPE) {}
  ObRoutineNameHashWrapper(uint64_t tenant_id, uint64_t database_id, uint64_t package_id,
                           const common::ObString &routine_name, uint64_t overload, ObRoutineType routine_type)
      : tenant_id_(tenant_id),
        database_id_(database_id),
        package_id_(package_id),
        routine_name_(routine_name),
        overload_(overload),
        routine_type_(routine_type) {}
  ~ObRoutineNameHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator ==(const ObRoutineNameHashWrapper &rv) const;
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_database_id(uint64_t database_id) { database_id_ = database_id; }
  inline void set_package_id(uint64_t package_id) { package_id_ = package_id; }
  inline void set_routine_name(const common::ObString &routine_name) { routine_name_ = routine_name; }
  inline void set_overload(uint64_t overload) { overload_ = overload; }
  inline void set_routine_type(ObRoutineType routine_type) { routine_type_ = routine_type; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline uint64_t get_package_id() const { return package_id_; }
  inline const common::ObString &get_routine_name() const { return routine_name_; }
  inline uint64_t get_overload() const { return overload_; }
  inline ObRoutineType get_routine_type() { return routine_type_; }
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(package_id), K_(routine_name), K_(overload), K_(routine_type));
private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  uint64_t package_id_;
  common::ObString routine_name_;
  uint64_t overload_;
  ObRoutineType routine_type_;
};

inline bool ObRoutineNameHashWrapper::operator ==(const ObRoutineNameHashWrapper &rv) const
{
  ObCompareNameWithTenantID name_cmp(tenant_id_);
  return (tenant_id_ == rv.tenant_id_)
      && (database_id_ == rv.database_id_)
      && (package_id_ == rv.package_id_)
      && (0 == name_cmp.compare(routine_name_, rv.routine_name_))
      && (overload_ == rv.overload_)
      && (routine_type_ == rv.routine_type_);
}

inline uint64_t ObRoutineNameHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  common::ObCollationType cs_type = common::CS_TYPE_UTF8MB4_GENERAL_CI;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::murmurhash(&package_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::ObCharset::hash(cs_type, routine_name_, hash_ret);
  hash_ret = common::murmurhash(&overload_, sizeof(uint64_t), hash_ret);
  hash_ret = common::murmurhash(&routine_type_, sizeof(ObRoutineType), hash_ret);
  return hash_ret;
}

template<class T, class V>
struct ObGetRoutineKey
{
  void operator()(const T & t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetRoutineKey<uint64_t, ObSimpleRoutineSchema *>
{
  uint64_t operator()(const ObSimpleRoutineSchema *routine_schema) const
  {
    return NULL != routine_schema ? routine_schema->get_routine_id() : common::OB_INVALID_ID;
  }
};

template<>
struct ObGetRoutineKey<ObRoutineNameHashWrapper, ObSimpleRoutineSchema *>
{
  ObRoutineNameHashWrapper operator ()(const ObSimpleRoutineSchema *routine_schema) const
  {
    ObRoutineNameHashWrapper name_wrap;
    if (routine_schema != NULL) {
      name_wrap.set_tenant_id(routine_schema->get_tenant_id());
      name_wrap.set_database_id(routine_schema->get_database_id());
      name_wrap.set_package_id(routine_schema->get_package_id());
      name_wrap.set_routine_name(routine_schema->get_routine_name());
      name_wrap.set_overload(routine_schema->get_overload());
      name_wrap.set_routine_type(routine_schema->get_routine_type());
    }
    return name_wrap;
  }
};

class ObRoutineMgr
{
  typedef common::ObSortedVector<ObSimpleRoutineSchema *> RoutineInfos;
  typedef common::hash::ObPointerHashMap<uint64_t, ObSimpleRoutineSchema *,
      ObGetRoutineKey, 1024> RoutineIdMap;
  typedef common::hash::ObPointerHashMap<ObRoutineNameHashWrapper, ObSimpleRoutineSchema *,
      ObGetRoutineKey, 1024> RoutineNameMap;
  typedef RoutineInfos::iterator RoutineIter;
  typedef RoutineInfos::const_iterator ConstRoutineIter;

public:
  ObRoutineMgr();
  explicit ObRoutineMgr(common::ObIAllocator &allocator);
  virtual ~ObRoutineMgr();
  int init();
  void reset();
  ObRoutineMgr &operator =(const ObRoutineMgr &other);
  int assign(const ObRoutineMgr &other);
  int deep_copy(const ObRoutineMgr &other);
  void dump() const;

  int add_routines(const common::ObIArray<ObSimpleRoutineSchema> &routine_schemas);
  int del_routines(const common::ObIArray<ObTenantRoutineId> &routines);
  int add_routine(const ObSimpleRoutineSchema &routine_schema);
  int del_routine(const ObTenantRoutineId &routine_id);
  int get_routine_schema(uint64_t tenant_id, uint64_t database_id,
                         uint64_t package_id, const common::ObString &routine_name,
                         uint64_t overload, ObRoutineType routine_type,
                         const ObSimpleRoutineSchema *&routine_schema) const;
  int get_routine_schema(uint64_t routine_id, const ObSimpleRoutineSchema *&routine_schema) const;
  int get_routine_schemas_in_tenant(uint64_t tenant_id,
                                    common::ObIArray<const ObSimpleRoutineSchema *> &routine_schemas) const;
  int get_routine_schemas_in_database(
              uint64_t tenant_id, uint64_t database_id,
              common::ObIArray<const ObSimpleRoutineSchema *> &routine_schemas) const;
  int get_routine_schemas_in_udt(uint64_t tenant_id, uint64_t udt_id,
                                 common::ObIArray<const ObSimpleRoutineSchema *> &routine_schemas) const;
  int get_routine_schemas_in_package(uint64_t tenant_id, uint64_t package_id,
                                     common::ObIArray<const ObSimpleRoutineSchema *> &routine_schemas) const;
  int del_routine_schemas_in_tenant(uint64_t tenant_id);
  int del_routine_schemas_in_database(uint64_t tenant_id, uint64_t database_id);
  int del_routine_schemas_in_package(uint64_t tenant_id, uint64_t package_id);
  int get_routine_schema_count(int64_t &routine_schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
  int check_user_reffered_by_definer(const ObString &user_name, bool &ref) const;
private:
  inline bool check_inner_stat() const;
  inline static bool compare_routine(const ObSimpleRoutineSchema *lhs,
                                     const ObSimpleRoutineSchema *rhs);
  inline static bool equal_routine(const ObSimpleRoutineSchema *lhs,
                                   const ObSimpleRoutineSchema *rhs);
  inline static bool compare_with_tenant_routine_id(const ObSimpleRoutineSchema *lhs,
                                                    const ObTenantRoutineId &tenant_routine_id);
  inline static bool equal_with_tenant_routine_id(const ObSimpleRoutineSchema *lhs,
                                                  const ObTenantRoutineId &tenant_routine_id);
  int rebuild_routine_hashmap();
private:
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  RoutineInfos routine_infos_;
  RoutineIdMap routine_id_map_;
  RoutineNameMap routine_name_map_;
  bool is_inited_;
};
}  // namespace schema
}  // namespace share
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SHARE_SCHEMA_OB_ROUTINE_MGR_H_ */
