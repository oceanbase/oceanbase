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

#ifndef OB_PROFILE_MGR_H
#define OB_PROFILE_MGR_H

#include "lib/hash/ob_pointer_hashmap.h"
#include "share/ob_define.h"
#include "share/schema/ob_schema_struct.h"
#include "lib/container/ob_vector.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObProfileNameHashKey
{
public:
  ObProfileNameHashKey() : tenant_id_(common::OB_INVALID_TENANT_ID) {}
  ObProfileNameHashKey(uint64_t tenant_id, common::ObString name)
    : tenant_id_(tenant_id),
      name_(name)
  {}
  ~ObProfileNameHashKey() {}
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
    hash_ret = common::murmurhash(name_.ptr(),
                                  name_.length(),
                                  hash_ret);
    return hash_ret;
  }
  inline bool operator == (const ObProfileNameHashKey &rv) const
  {
    return tenant_id_ == rv.tenant_id_
          && name_ == rv.name_;
  }
private:
  uint64_t tenant_id_;
  common::ObString name_;
};

template<class T, class V>
struct ObGetProfileKey {
  void operator()(const T &t, const V &v) const {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetProfileKey<ObProfileNameHashKey, ObProfileSchema *>
{
  ObProfileNameHashKey operator() (const ObProfileSchema *schema) const {
    return OB_ISNULL(schema) ?
          ObProfileNameHashKey()
        : ObProfileNameHashKey(schema->get_tenant_id(), schema->get_profile_name_str());
  }
};


template<>
struct ObGetProfileKey<uint64_t, ObProfileSchema *>
{
  uint64_t operator()(const ObProfileSchema *schema) const
  {
    return OB_ISNULL(schema) ? common::OB_INVALID_ID : schema->get_profile_id();
  }
};

class ObProfileMgr
{
public:
  typedef common::ObSortedVector<ObProfileSchema *> ProfileInfos;
  typedef common::hash::ObPointerHashMap<ObProfileNameHashKey, ObProfileSchema *,
                                     ObGetProfileKey, 128> ObProfileNameMap;
  typedef common::hash::ObPointerHashMap<uint64_t, ObProfileSchema *,
                                       ObGetProfileKey, 128> ObProfileIdMap;
  typedef ProfileInfos::iterator ProfileIter;
  typedef ProfileInfos::const_iterator ConstProfileIter;
  ObProfileMgr();
  explicit ObProfileMgr(common::ObIAllocator &allocator);
  virtual ~ObProfileMgr();
  int init();
  void reset();
  ObProfileMgr &operator = (const ObProfileMgr &other);
  int assign(const ObProfileMgr &other);
  int deep_copy(const ObProfileMgr &other);
  int add_profile(const ObProfileSchema &schema);
  int add_profiles(const common::ObIArray<ObProfileSchema> &schemas);
  int del_profile(const ObTenantProfileId &id);
  int get_schema_by_id(const uint64_t profile_id,
                       const ObProfileSchema *&schema) const;
  int get_schema_version_by_id(uint64_t profile_id, int64_t &schema_version) const;
  int get_schema_by_name(const uint64_t tenant_id,
                         const common::ObString &name,
                         const ObProfileSchema *&schema) const;
  int get_schemas_in_tenant(const uint64_t tenant_id,
                            common::ObIArray<const ObProfileSchema *> &schemas) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  inline static bool schema_cmp(const ObProfileSchema *lhs,
                                const ObProfileSchema *rhs) {
    return lhs->get_tenant_id() != rhs->get_tenant_id() ?
          lhs->get_tenant_id() < rhs->get_tenant_id()
        : lhs->get_profile_id() < rhs->get_profile_id();
  }
  inline static bool schema_equal(const ObProfileSchema *lhs,
                                  const ObProfileSchema *rhs) {
    return lhs->get_tenant_id() == rhs->get_tenant_id()
        && lhs->get_profile_id() == rhs->get_profile_id();
  }
  static bool compare_with_tenant_profile_id(const ObProfileSchema *lhs, const ObTenantProfileId &id);
  static bool equal_to_tenant_profile_id(const ObProfileSchema *lhs, const ObTenantProfileId &id);

  int get_schema_count(int64_t &schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  ProfileInfos schema_infos_;
  ObProfileNameMap name_map_;
  ObProfileIdMap id_map_;
};


}
}
}

#endif // OB_PROFILE_MGR_H
