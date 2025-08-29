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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_LOCATION_MGR_H_
#define OCEANBASE_SHARE_SCHEMA_OB_LOCATION_MGR_H_

#include "lib/utility/ob_macro_utils.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "share/ob_define.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_location_schema_struct.h"
#include "lib/container/ob_vector.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObLocationNameHashKey
{
public:
  ObLocationNameHashKey()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
    name_case_mode_(common::OB_NAME_CASE_INVALID),
    location_name_()
  {
  }
  ObLocationNameHashKey(uint64_t tenant_id,
                        const common::ObNameCaseMode mode,
                        common::ObString location_name)
    : tenant_id_(tenant_id), name_case_mode_(mode), location_name_(location_name)
  {
  }
  ~ObLocationNameHashKey()
  {
  }
  uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
    common::ObCollationType cs_type = ObSchema::get_cs_type_with_cmp_mode(name_case_mode_);
    hash_ret = common::ObCharset::hash(cs_type, location_name_, hash_ret);
    return hash_ret;
  }
  bool operator == (const ObLocationNameHashKey &rv) const
  {
    ObCompareNameWithTenantID name_cmp(tenant_id_, name_case_mode_);
    return (tenant_id_ == rv.tenant_id_)
           && (name_case_mode_ == rv.name_case_mode_)
           && (0 == name_cmp.compare(location_name_ ,rv.location_name_));
  }
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_location_name(const common::ObString &location_name) { location_name_ = location_name;}
  uint64_t get_tenant_id() const { return tenant_id_; }
  const common::ObString &get_location_name() const { return location_name_; }
private:
  uint64_t tenant_id_;
  common::ObNameCaseMode name_case_mode_;
  common::ObString location_name_;
};

template<class T, class V>
struct ObGetLocationKey
{
  void operator()(const T &t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetLocationKey<ObLocationNameHashKey, ObLocationSchema *>
{
  ObLocationNameHashKey operator()(const ObLocationSchema *schema) const
  {
    return OB_ISNULL(schema) ?
          ObLocationNameHashKey()
        : ObLocationNameHashKey(schema->get_tenant_id(),
                                schema->get_name_case_mode(),
                                schema->get_location_name_str());
  }
};

template<>
struct ObGetLocationKey<uint64_t, ObLocationSchema *>
{
  uint64_t operator()(const ObLocationSchema *schema) const
  {
    return OB_ISNULL(schema) ? common::OB_INVALID_ID : schema->get_location_id();
  }
};

class ObLocationMgr
{
public:
  ObLocationMgr();
  explicit ObLocationMgr(common::ObIAllocator &allocator);
  virtual ~ObLocationMgr();

  ObLocationMgr &operator=(const ObLocationMgr &other);

  int init();
  void reset();

  int assign(const ObLocationMgr &other);
  int deep_copy(const ObLocationMgr &other);
  int add_location(const ObLocationSchema &schema, const ObNameCaseMode mode);
  int del_location(const ObTenantLocationId &id);
  int get_location_schema_by_id(const uint64_t location_id,
                                const ObLocationSchema *&schema) const;
  int get_location_schema_by_name(const uint64_t tenant_id,
                                  const ObNameCaseMode mode,
                                  const common::ObString &name,
                                  const ObLocationSchema *&schema) const;
  int get_location_schema_by_prefix_match(const uint64_t tenant_id,
                                          const common::ObString &access_path,
                                          ObArray<const ObLocationSchema*> &match_schemas) const;
  int get_location_schemas_in_tenant(const uint64_t tenant_id,
                                      common::ObIArray<const ObLocationSchema *> &schemas) const;
  int del_location_schemas_in_tenant(const uint64_t tenant_id);
  int get_location_schema_count(int64_t &schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
private:
  int rebuild_location_hashmap();
  static bool schema_compare(const ObLocationSchema *lhs, const ObLocationSchema *rhs);
  static bool schema_equal(const ObLocationSchema *lhs, const ObLocationSchema *rhs);
  static bool compare_with_tenant_location_id(const ObLocationSchema *lhs, const ObTenantLocationId &id);
  static bool equal_to_tenant_location_id(const ObLocationSchema *lhs, const ObTenantLocationId &id);
public:
  typedef common::ObSortedVector<ObLocationSchema *> LocationInfos;
  typedef common::hash::ObPointerHashMap<ObLocationNameHashKey,
                                        ObLocationSchema *,
                                        ObGetLocationKey, 128> ObLocationNameMap;
  typedef common::hash::ObPointerHashMap<uint64_t,
                                        ObLocationSchema *,
                                        ObGetLocationKey, 128> ObLocationIdMap;
  typedef LocationInfos::iterator LocationIter;
  typedef LocationInfos::const_iterator ConstLocationIter;
private:
  static const char *LOCATION_MGR;
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  LocationInfos location_infos_;
  ObLocationNameMap location_name_map_;
  ObLocationIdMap location_id_map_;
};

OB_INLINE bool ObLocationMgr::schema_compare(const ObLocationSchema *lhs, const ObLocationSchema *rhs)
{
  return lhs->get_tenant_id() != rhs->get_tenant_id()
      ? lhs->get_tenant_id() < rhs->get_tenant_id()
      : lhs->get_location_id() < rhs->get_location_id();
}

OB_INLINE bool ObLocationMgr::schema_equal(const ObLocationSchema *lhs, const ObLocationSchema *rhs)
{
  return lhs->get_tenant_id() == rhs->get_tenant_id()
      && lhs->get_location_id() == rhs->get_location_id();
}

OB_INLINE bool ObLocationMgr::compare_with_tenant_location_id(const ObLocationSchema *lhs, const ObTenantLocationId &id)
{
  return NULL != lhs ? (lhs->get_tenant_location_id() < id) : false;
}

OB_INLINE bool ObLocationMgr::equal_to_tenant_location_id(const ObLocationSchema *lhs, const ObTenantLocationId &id)
{
  return NULL != lhs ? (lhs->get_tenant_location_id() == id) : false;
}
} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SCHEMA_OB_LOCATION_MGR_H_
