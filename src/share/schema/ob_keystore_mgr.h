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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_KEYSTORE_MGR_H
#define OCEANBASE_SHARE_SCHEMA_OB_KEYSTORE_MGR_H
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
class ObKeystoreHashWrapper
{
public:
  ObKeystoreHashWrapper()
    : tenant_id_(common::OB_INVALID_TENANT_ID) {}
  ObKeystoreHashWrapper(uint64_t tenant_id)
    : tenant_id_(tenant_id) {}
  ~ObKeystoreHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator==(const ObKeystoreHashWrapper &rv) const;
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  TO_STRING_KV(K_(tenant_id));
private:
  uint64_t tenant_id_;
};
inline bool ObKeystoreHashWrapper::operator == (const ObKeystoreHashWrapper &rv) const
{
  return (tenant_id_ == rv.get_tenant_id());
}
inline uint64_t ObKeystoreHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  return hash_ret;
}
template<class T, class V>
struct ObGetKeystoreKey {
  void operator()(const T &t, const V &v) const {
    UNUSED(t);
    UNUSED(v);
  }
};
template<>
struct ObGetKeystoreKey<ObKeystoreHashWrapper, ObKeystoreSchema *>
{
  ObKeystoreHashWrapper operator() (const ObKeystoreSchema * keystore) const {
    ObKeystoreHashWrapper hash_wrap;
    if (!OB_ISNULL(keystore)) {
      hash_wrap.set_tenant_id(keystore->get_tenant_id());
    }
    return hash_wrap;
  }
};
class ObKeystoreMgr
{
public:
  typedef common::ObSortedVector<ObKeystoreSchema *> KeystoreInfos;
  typedef common::hash::ObPointerHashMap<ObKeystoreHashWrapper, ObKeystoreSchema *,
                                         ObGetKeystoreKey, 128> ObKeystoreMap;
  typedef KeystoreInfos::iterator KeystoreIter;
  typedef KeystoreInfos::const_iterator ConstKeystoreIter;
  ObKeystoreMgr();
  explicit ObKeystoreMgr(common::ObIAllocator &allocator);
  virtual ~ObKeystoreMgr();
  int init();
  void reset();
  ObKeystoreMgr &operator =(const ObKeystoreMgr &other);
  int assign(const ObKeystoreMgr &other);
  int deep_copy(const ObKeystoreMgr &other);
  void dump() const;
  int get_keystore_schema_count(int64_t &keystore_schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
  int add_keystore(const ObKeystoreSchema &keystore_schema);
  int del_keystore(const ObTenantKeystoreId &keystore);
  int add_keystores(const common::ObIArray<ObKeystoreSchema> &keystore_schema);
  int get_keystore_info_version(uint64_t keystore_id, int64_t &keystore_version) const;
  int get_keystore_schema(const uint64_t tenant_id,
                          const ObKeystoreSchema *&keystore_schema) const;
  int get_keystore_schemas_in_tenant(const uint64_t tenant_id,
      common::ObIArray<const ObKeystoreSchema *> &keystore_schemas) const;
  static int rebuild_keystore_hashmap(const KeystoreInfos &keystore_infos,
      ObKeystoreMap &keystore_map);
  inline static bool compare_keystore(const ObKeystoreSchema *lhs,
      const ObKeystoreSchema *rhs) {
    return lhs->get_keystore_id() < rhs->get_keystore_id();
  }
  inline static bool equal_keystore(const ObKeystoreSchema *lhs,
      const ObKeystoreSchema *rhs) {
    return lhs->get_keystore_id() == rhs->get_keystore_id();
  }

TO_STRING_KV(K_(is_inited));
private:
  inline static bool compare_with_tenant_keystore_id(const ObKeystoreSchema *lhs,
                                                     const ObTenantKeystoreId &tenant_keystore_id);
  inline static bool equal_to_tenant_keystore_id(const ObKeystoreSchema *lhs,
                                                 const ObTenantKeystoreId &tenant_keystore_id);
private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  KeystoreInfos keystore_infos_;
  ObKeystoreMap keystore_map_;
};
} //end of schema
} //end of share
} //end of oceanbase
#endif //OCEANBASE_SHARE_SCHEMA_OB_KEYSTORE_MGR_H
