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

#ifndef OB_UDF_MGR_H_
#define OB_UDF_MGR_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/container/ob_vector.h"
#include "lib/allocator/page_arena.h"
#include "share/schema/ob_udf.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObSimpleUDFSchema : public ObSchema
{
public:
  ObSimpleUDFSchema();
  explicit ObSimpleUDFSchema(common::ObIAllocator *allocator);
  ObSimpleUDFSchema(const ObSimpleUDFSchema &src_schema);
  virtual ~ObSimpleUDFSchema();
  ObSimpleUDFSchema &operator =(const ObSimpleUDFSchema &other);
  TO_STRING_KV(K_(tenant_id),
               K_(udf_id),
               K_(schema_version),
               K_(udf_name),
               K_(ret),
               K_(dl),
               K_(type));
  virtual void reset();
  inline bool is_valid() const;
  inline int64_t get_convert_size() const;
  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline void set_udf_id(const uint64_t udf_id) { udf_id_ = udf_id; }
  inline uint64_t get_udf_id() const { return udf_id_; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline int set_udf_name(const common::ObString &name)
  { return deep_copy_str(name, udf_name_); }
  inline const char *get_udf_name() const { return extract_str(udf_name_); }
  inline const common::ObString &get_udf_name_str() const { return udf_name_; }
  inline common::ObString get_udf_name_str() { return udf_name_; }
  inline ObTenantUDFId get_tenant_udf_id() const
  { return ObTenantUDFId(tenant_id_, udf_name_); }

  //the ObSimpleUDFSchema must have the same interface just like ObUDF.
  //ObSchemaRetrieveUtils's template function will use these interface.
  inline void set_ret(const enum ObUDF::UDFRetType ret) { ret_ = ret; }
  inline void set_type(const enum ObUDF::UDFType type) { type_ = type; }
  inline void set_ret(const int ret) { ret_ = ObUDF::UDFRetType(ret); }
  inline void set_type(const int type) { type_ = ObUDF::UDFType(type); }
  inline int set_name(const common::ObString &name) { return deep_copy_str(name, udf_name_); }
  inline int set_dl(const common::ObString &dl) { return deep_copy_str(dl, dl_); }

  inline enum ObUDF::UDFRetType get_ret() const { return ret_; }
  inline enum ObUDF::UDFType get_type() const { return type_; }

  inline const char *get_dl() const { return extract_str(dl_); }
  inline const common::ObString &get_dl_str() const { return dl_; }
  inline const char *get_name() const { return extract_str(udf_name_); }
  inline const common::ObString &get_name_str() const { return udf_name_; }

private:
  uint64_t tenant_id_;
  uint64_t udf_id_;
  common::ObString udf_name_;
  enum ObUDF::UDFRetType ret_;
  common::ObString dl_;
  enum ObUDF::UDFType type_;
  int64_t schema_version_;
};

template<class T, class V>
struct ObGetUDFKey {
  void operator()(const T &t, const V &v) const {
    UNUSED(t);
    UNUSED(v);
  }
};

class ObUDFHashWrapper
{
public:
  ObUDFHashWrapper()
    : tenant_id_(common::OB_INVALID_ID),
      udf_name_() {}
  ObUDFHashWrapper(uint64_t tenant_id, const common::ObString &udf_name)
    : tenant_id_(tenant_id),
      udf_name_(udf_name) {}
  ~ObUDFHashWrapper() {}
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
    hash_ret = common::murmurhash(get_udf_name().ptr(), get_udf_name().length(), hash_ret);
    return hash_ret;
  }
  inline bool operator==(const ObUDFHashWrapper &rv) const{
    return (tenant_id_ == rv.get_tenant_id())
        && (udf_name_ == rv.get_udf_name());
  }
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_udf_name(const common::ObString &udf_name) { udf_name_ = udf_name; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline const common::ObString &get_udf_name() const { return udf_name_; }
  TO_STRING_KV(K_(tenant_id), K_(udf_name));

private:
  uint64_t tenant_id_;
  common::ObString udf_name_;
};

template<>
struct ObGetUDFKey<ObUDFHashWrapper, ObSimpleUDFSchema *>
{
  ObUDFHashWrapper operator() (const ObSimpleUDFSchema * udf) const {
    ObUDFHashWrapper hash_wrap;
    if (!OB_ISNULL(udf)) {
      hash_wrap.set_tenant_id(udf->get_tenant_id());
      hash_wrap.set_udf_name(udf->get_udf_name());
    }
    return hash_wrap;
  }
};


class ObUDFMgr
{
public:
  typedef common::ObSortedVector<ObSimpleUDFSchema *> UDFInfos;
  typedef common::hash::ObPointerHashMap<ObUDFHashWrapper, ObSimpleUDFSchema *, ObGetUDFKey, 128> ObUDFMap;
  typedef UDFInfos::iterator UDFIter;
  typedef UDFInfos::const_iterator ConstUDFIter;
  ObUDFMgr();
  explicit ObUDFMgr(common::ObIAllocator &allocator);
  virtual ~ObUDFMgr();
  int init();
  void reset();
  ObUDFMgr &operator =(const ObUDFMgr &other);
  int assign(const ObUDFMgr &other);
  int deep_copy(const ObUDFMgr &other);
  void dump() const;
  int get_udf_schema_count(int64_t &udf_schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
  int add_udf(const ObSimpleUDFSchema &udf_schema);
  int add_udfs(const common::ObIArray<ObSimpleUDFSchema> &udf_schema);
  int del_udf(const ObTenantUDFId &udf);

  int get_udf_schema(const uint64_t udf_id,
                     const ObSimpleUDFSchema *&udf_schema) const;
  int get_udf_info_version(uint64_t udf_id, int64_t &udf_version) const;
  int get_udf_schema_with_name(const uint64_t tenant_id,
                               const common::ObString &name,
                               const ObSimpleUDFSchema *&udf_schema) const;
  int get_udf_schemas_in_tenant(const uint64_t tenant_id,
      common::ObIArray<const ObSimpleUDFSchema *> &udf_schemas) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  inline static bool compare_udf(const ObSimpleUDFSchema *lhs,
                                 const ObSimpleUDFSchema *rhs) {
    return lhs->get_tenant_udf_id() < rhs->get_tenant_udf_id();
  }
  inline static bool equal_udf(const ObSimpleUDFSchema *lhs,
                               const ObSimpleUDFSchema *rhs) {
    return lhs->get_tenant_udf_id() == rhs->get_tenant_udf_id();
  }
  static int rebuild_udf_hashmap(const UDFInfos &udf_infos,
                                 ObUDFMap &udf_map);
private:
  inline static bool compare_with_tenant_udf_id(const ObSimpleUDFSchema *lhs,
                                                const ObTenantUDFId &tenant_outline_id);
  inline static bool equal_to_tenant_udf_id(const ObSimpleUDFSchema *lhs,
                                            const ObTenantUDFId &tenant_outline_id);
private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  UDFInfos udf_infos_;
  ObUDFMap udf_map_;
};


} //end of schema
} //end of share
} //end of oceanbase

#endif
