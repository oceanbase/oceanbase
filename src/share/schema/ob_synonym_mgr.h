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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_SYNONYM_MGR_H
#define OCEANBASE_SHARE_SCHEMA_OB_SYNONYM_MGR_H

#include "share/ob_define.h"
#include "lib/hash/ob_hashmap.h"
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

class ObSimpleSynonymSchema : public ObSchema
{
public:
  ObSimpleSynonymSchema();
  explicit ObSimpleSynonymSchema(common::ObIAllocator *allocator);
  ObSimpleSynonymSchema(const ObSimpleSynonymSchema &src_schema);
  virtual ~ObSimpleSynonymSchema();
  ObSimpleSynonymSchema &operator =(const ObSimpleSynonymSchema &other);
  TO_STRING_KV(K_(tenant_id),
               K_(synonym_id),
               K_(schema_version),
               K_(database_id),
               K_(synonym_name),
               K_(object_name),
               K_(object_database_id),
               K_(status));
  virtual void reset();
  inline bool is_valid() const;
  inline int64_t get_convert_size() const;
  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline void set_synonym_id(const uint64_t synonym_id) { synonym_id_ = synonym_id; }
  inline uint64_t get_synonym_id() const { return synonym_id_; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline void set_database_id(const uint64_t database_id) { database_id_ = database_id; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline void set_object_database_id(const uint64_t database_id) { object_database_id_ = database_id; }
  inline uint64_t get_object_database_id() const { return object_database_id_; }
  inline int set_synonym_name(const common::ObString &name)
  { return deep_copy_str(name, synonym_name_); }
  inline const char *get_synonym_name() const { return extract_str(synonym_name_); }
  inline const common::ObString &get_synonym_name_str() const { return synonym_name_; }
  inline common::ObString &get_synonym_name_str() { return synonym_name_; }
  inline int set_object_name(const common::ObString &object_name)
  { return deep_copy_str(object_name, object_name_); }
  inline const char *get_object_name() const { return extract_str(object_name_); }
  inline const common::ObString &get_object_name_str() const { return object_name_; }
  inline common::ObString &get_object_name_str() { return object_name_; }
  inline ObTenantSynonymId get_tenant_synonym_id() const
  { return ObTenantSynonymId(tenant_id_, synonym_id_); }
  inline void set_status(const ObObjectStatus status) { status_ = status; }
  inline void set_status(const int64_t status) { status_ = static_cast<ObObjectStatus> (status); }
  inline ObObjectStatus get_status() const { return status_; }
private:
  uint64_t tenant_id_;
  uint64_t synonym_id_;
  int64_t schema_version_;
  uint64_t database_id_;
  common::ObString synonym_name_;
  common::ObString object_name_;
  uint64_t object_database_id_;
  ObObjectStatus status_;
};

class ObSynonymHashWrapper
{
public:
  ObSynonymHashWrapper()
    : tenant_id_(common::OB_INVALID_ID),
      database_id_(common::OB_INVALID_ID),
      synonym_name_() {}
  ObSynonymHashWrapper(uint64_t tenant_id, uint64_t database_id,
                      const common::ObString &synonym_name)
    : tenant_id_(tenant_id),
      database_id_(database_id),
      synonym_name_(synonym_name) {}
  ~ObSynonymHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator==(const ObSynonymHashWrapper &rv) const;
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_database_id(uint64_t database_id) { database_id_ = database_id; }
  inline void set_synonym_name(const common::ObString &synonym_name) { synonym_name_ = synonym_name; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline const common::ObString &get_synonym_name() const { return synonym_name_; }
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(synonym_name));

private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString synonym_name_;
};

inline bool ObSynonymHashWrapper::operator == (const ObSynonymHashWrapper &rv) const
{
  return (tenant_id_ == rv.get_tenant_id())
      && (database_id_ == rv.get_database_id())
      && (synonym_name_ == rv.get_synonym_name());
}

inline uint64_t ObSynonymHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::murmurhash(synonym_name_.ptr(), synonym_name_.length(), hash_ret);
  return hash_ret;
}

template<class T, class V>
struct ObGetSynonymKey {
  void operator()(const T &t, const V &v) const {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetSynonymKey<uint64_t, ObSimpleSynonymSchema *>
{
  uint64_t operator()(const ObSimpleSynonymSchema *synonym_schema) const
  {
    return NULL != synonym_schema ? synonym_schema->get_synonym_id() : common::OB_INVALID_ID;
  }
};

template<>
struct ObGetSynonymKey<ObSynonymHashWrapper, ObSimpleSynonymSchema *>
{
  ObSynonymHashWrapper operator() (const ObSimpleSynonymSchema * synonym) const {
    ObSynonymHashWrapper hash_wrap;
    if (!OB_ISNULL(synonym)) {
      hash_wrap.set_tenant_id(synonym->get_tenant_id());
      hash_wrap.set_database_id(synonym->get_database_id());
      hash_wrap.set_synonym_name(synonym->get_synonym_name());
    }
    return hash_wrap;
  }
};

class ObSynonymMgr
{
public:
  typedef common::ObSortedVector<ObSimpleSynonymSchema *> SynonymInfos;
  typedef common::hash::ObPointerHashMap<uint64_t, ObSimpleSynonymSchema *,
                                         ObGetSynonymKey, 128> ObSynonymIdMap;
  typedef common::hash::ObPointerHashMap<ObSynonymHashWrapper, ObSimpleSynonymSchema *,
                                         ObGetSynonymKey, 128> ObSynonymNameMap;
  typedef SynonymInfos::iterator SynonymIter;
  typedef SynonymInfos::const_iterator ConstSynonymIter;
  ObSynonymMgr();
  explicit ObSynonymMgr(common::ObIAllocator &allocator);
  virtual ~ObSynonymMgr();
  int init();
  void reset();
  ObSynonymMgr &operator =(const ObSynonymMgr &other);
  int assign(const ObSynonymMgr &other);
  int deep_copy(const ObSynonymMgr &other);
  void dump() const;
  int get_synonym_schema_count(int64_t &synonym_schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
  int add_synonym(const ObSimpleSynonymSchema &synonym_schema);
  int add_synonyms(const common::ObIArray<ObSimpleSynonymSchema> &synonym_schema);
  int del_synonym(const ObTenantSynonymId &synonym);

  int get_synonym_schema(const uint64_t synonym_id,
                         const ObSimpleSynonymSchema *&synonym_schema) const;
  int get_synonym_schema_with_name(const uint64_t tenant_id,
                                   const uint64_t database_id,
                                   const common::ObString &name,
                                   const ObSimpleSynonymSchema *&synonym_schema) const;
  int get_object(const uint64_t tenant_id,
                 const uint64_t database_id,
                 const common::ObString &name,
                 uint64_t &obj_database_id,
                 uint64_t &synonym_id,
                 common::ObString &obj_table_name,
                 bool &do_exist,
                 bool search_public_schema = true,
                 bool *is_public = NULL) const;
  int get_synonym_schemas_in_tenant(const uint64_t tenant_id,
      common::ObIArray<const ObSimpleSynonymSchema *> &synonym_schemas) const;
  int get_synonym_schemas_in_database(const uint64_t tenant_id,
      const uint64_t database_id,
      common::ObIArray<const ObSimpleSynonymSchema *> &synonym_schemas) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  template<typename Filter, typename Acation, typename EarlyStopCondition>
  int for_each(Filter &filter, Acation &action, EarlyStopCondition &condition);
  inline static bool compare_synonym(const ObSimpleSynonymSchema *lhs,
                                     const ObSimpleSynonymSchema *rhs) {
    return lhs->get_synonym_id() < rhs->get_synonym_id();
  }
  inline static bool equal_synonym(const ObSimpleSynonymSchema *lhs,
                                   const ObSimpleSynonymSchema *rhs) {
    return lhs->get_synonym_id() == rhs->get_synonym_id();
  }
private:
  inline static bool compare_with_tenant_synonym_id(const ObSimpleSynonymSchema *lhs,
                                                    const ObTenantSynonymId &tenant_outline_id);
  inline static bool equal_to_tenant_synonym_id(const ObSimpleSynonymSchema *lhs,
                                                  const ObTenantSynonymId &tenant_outline_id);
  int rebuild_synonym_hashmap();
private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  SynonymInfos synonym_infos_;
  ObSynonymIdMap synonym_id_map_;
  ObSynonymNameMap synonym_name_map_;
};

} //end of schema
} //end of share
} //end of oceanbase
#endif //OCEANBASE_SHARE_SCHEMA_OB_SYNONYM_MGR_H
