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

#ifndef OB_CATALOG_MGR_H
#define OB_CATALOG_MGR_H

#include "lib/hash/ob_pointer_hashmap.h"
#include "share/ob_define.h"
#include "share/schema/ob_schema_struct.h"
#include "lib/container/ob_vector.h"
#include "share/schema/ob_catalog_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObCatalogNameHashKey
{
public:
  ObCatalogNameHashKey()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      name_case_mode_(common::OB_NAME_CASE_INVALID),
      name_()
  {}
  ObCatalogNameHashKey(uint64_t tenant_id,
                       const common::ObNameCaseMode mode,
                       common::ObString name)
    : tenant_id_(tenant_id),
      name_case_mode_(mode),
      name_(name)
  {}
  ~ObCatalogNameHashKey() {}
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
    common::ObCollationType cs_type = ObSchema::get_cs_type_with_cmp_mode(name_case_mode_);
    hash_ret = common::ObCharset::hash(cs_type, name_, hash_ret);
    return hash_ret;
  }
  inline bool operator == (const ObCatalogNameHashKey &rv) const
  {
    ObCompareNameWithTenantID name_cmp(tenant_id_, name_case_mode_);
    return (tenant_id_ == rv.tenant_id_)
           && (name_case_mode_ == rv.name_case_mode_)
           && (0 == name_cmp.compare(name_ ,rv.name_));
  }
private:
  uint64_t tenant_id_;
  common::ObNameCaseMode name_case_mode_;
  common::ObString name_;
};

template<class T, class V>
struct ObGetCatalogKey {
  void operator()(const T &t, const V &v) const {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetCatalogKey<ObCatalogNameHashKey, ObCatalogSchema *>
{
  ObCatalogNameHashKey operator() (const ObCatalogSchema *schema) const {
    return OB_ISNULL(schema)
           ? ObCatalogNameHashKey()
           : ObCatalogNameHashKey(schema->get_tenant_id(),
                                  schema->get_name_case_mode(),
                                  schema->get_catalog_name_str());
  }
};


template<>
struct ObGetCatalogKey<uint64_t, ObCatalogSchema *>
{
  uint64_t operator()(const ObCatalogSchema *schema) const
  {
    return OB_ISNULL(schema) ? common::OB_INVALID_ID : schema->get_catalog_id();
  }
};

class ObCatalogMgr
{
public:
  typedef common::ObSortedVector<ObCatalogSchema *> CatalogInfos;
  typedef common::hash::ObPointerHashMap<ObCatalogNameHashKey, ObCatalogSchema *,
                                         ObGetCatalogKey, 128> ObCatalogNameMap;
  typedef common::hash::ObPointerHashMap<uint64_t, ObCatalogSchema *,
                                         ObGetCatalogKey, 128> ObCatalogIdMap;
  typedef CatalogInfos::iterator CatalogIter;
  typedef CatalogInfos::const_iterator ConstCatalogIter;
  ObCatalogMgr();
  explicit ObCatalogMgr(common::ObIAllocator &allocator);
  virtual ~ObCatalogMgr();
  int init();
  void reset();
  ObCatalogMgr &operator = (const ObCatalogMgr &other);
  int assign(const ObCatalogMgr &other);
  int deep_copy(const ObCatalogMgr &other);
  int add_catalog(const ObCatalogSchema &schema, const ObNameCaseMode mode);
  int del_catalog(const ObTenantCatalogId &id);
  int get_schema_by_id(const uint64_t catalog_id,
                       const ObCatalogSchema *&schema) const;
  int get_schema_by_name(const uint64_t tenant_id,
                         const ObNameCaseMode mode,
                         const common::ObString &name,
                         const ObCatalogSchema *&schema) const;
  int get_schemas_in_tenant(const uint64_t tenant_id,
                            common::ObIArray<const ObCatalogSchema *> &schemas) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  inline static bool schema_cmp(const ObCatalogSchema *lhs,
                                const ObCatalogSchema *rhs) {
    return lhs->get_tenant_id() != rhs->get_tenant_id() ?
          lhs->get_tenant_id() < rhs->get_tenant_id()
        : lhs->get_catalog_id() < rhs->get_catalog_id();
  }
  inline static bool schema_equal(const ObCatalogSchema *lhs,
                                  const ObCatalogSchema *rhs) {
    return lhs->get_tenant_id() == rhs->get_tenant_id()
        && lhs->get_catalog_id() == rhs->get_catalog_id();
  }
  static bool compare_with_tenant_catalog_id(const ObCatalogSchema *lhs, const ObTenantCatalogId &id);
  static bool equal_to_tenant_catalog_id(const ObCatalogSchema *lhs, const ObTenantCatalogId &id);

  int get_schema_count(int64_t &schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  CatalogInfos schema_infos_;
  ObCatalogNameMap name_map_;
  ObCatalogIdMap id_map_;
};


}
}
}

#endif // OB_CATALOG_MGR_H
