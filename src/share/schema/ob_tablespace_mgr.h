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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_TABLESPACE_MGR_H
#define OCEANBASE_SHARE_SCHEMA_OB_TABLESPACE_MGR_H

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


class ObTablespaceHashWrapper
{
public:
  ObTablespaceHashWrapper()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      tablespace_name_() {}
  ObTablespaceHashWrapper(uint64_t tenant_id,
                      const common::ObString &tablespace_name)
    : tenant_id_(tenant_id),
      tablespace_name_(tablespace_name) {}
  ~ObTablespaceHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator==(const ObTablespaceHashWrapper &rv) const;
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_tablespace_name(const common::ObString &tablespace_name) { tablespace_name_ = tablespace_name; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline const common::ObString &get_tablespace_name() const { return tablespace_name_; }
  TO_STRING_KV(K_(tenant_id), K_(tablespace_name));

private:
  uint64_t tenant_id_;
  common::ObString tablespace_name_;
};

inline bool ObTablespaceHashWrapper::operator == (const ObTablespaceHashWrapper &rv) const
{
  return (tenant_id_ == rv.get_tenant_id())
      && (tablespace_name_ == rv.get_tablespace_name());
}

inline uint64_t ObTablespaceHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(tablespace_name_.ptr(), tablespace_name_.length(), hash_ret);
  return hash_ret;
}

template<class T, class V>
struct ObGetTablespaceKey {
  void operator()(const T &t, const V &v) const {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetTablespaceKey<ObTablespaceHashWrapper, ObTablespaceSchema *>
{
  ObTablespaceHashWrapper operator() (const ObTablespaceSchema * tablespace) const {
    ObTablespaceHashWrapper hash_wrap;
    if (!OB_ISNULL(tablespace)) {
      hash_wrap.set_tenant_id(tablespace->get_tenant_id());
      hash_wrap.set_tablespace_name(tablespace->get_tablespace_name());
    }
    return hash_wrap;
  }
};

class ObTablespaceMgr
{
public:
  typedef common::ObSortedVector<ObTablespaceSchema *> TablespaceInfos;
  typedef common::hash::ObPointerHashMap<ObTablespaceHashWrapper, ObTablespaceSchema *,
                                         ObGetTablespaceKey, 128> ObTablespaceMap;
  typedef TablespaceInfos::iterator TablespaceIter;
  typedef TablespaceInfos::const_iterator ConstTablespaceIter;
  ObTablespaceMgr();
  explicit ObTablespaceMgr(common::ObIAllocator &allocator);
  virtual ~ObTablespaceMgr();
  int init();
  void reset();
  ObTablespaceMgr &operator =(const ObTablespaceMgr &other);
  int assign(const ObTablespaceMgr &other);
  int deep_copy(const ObTablespaceMgr &other);
  void dump() const;
  int get_tablespace_schema_count(int64_t &tablespace_schema_count) const;
  int add_tablespace(const ObTablespaceSchema &tablespace_schema);
  int add_tablespaces(const common::ObIArray<ObTablespaceSchema> &tablespace_schema);
  int del_tablespace(const ObTenantTablespaceId &tablespace);

  int get_tablespace_schema(const uint64_t tablespace_id,
      const ObTablespaceSchema *&tablespace_schema) const;
  int get_tablespace_info_version(uint64_t tablespace_id, int64_t &tablespace_version) const;
  int get_tablespace_schema_with_name(const uint64_t tenant_id,
      const common::ObString &name,
      const ObTablespaceSchema *&tablespace_schema) const;
  int get_tablespace_schemas_in_tenant(const uint64_t tenant_id,
      common::ObIArray<const ObTablespaceSchema *> &tablespace_schemas) const;
  int get_encryption_name(const uint64_t tenant_id,
      const common::ObString &tablespace_name,
      common::ObString &encryption_name,
      bool &do_exist) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  template<typename Filter, typename Acation, typename EarlyStopCondition>
  int for_each(Filter &filter, Acation &action, EarlyStopCondition &condition);
  inline static bool compare_tablespace(const ObTablespaceSchema *lhs,
      const ObTablespaceSchema *rhs) {
    return lhs->get_tablespace_id() < rhs->get_tablespace_id();
  }
  inline static bool equal_tablespace(const ObTablespaceSchema *lhs,
      const ObTablespaceSchema *rhs) {
    return lhs->get_tablespace_id() == rhs->get_tablespace_id();
  }
  static int rebuild_tablespace_hashmap(const TablespaceInfos &tablespace_infos,
      ObTablespaceMap &tablespace_map);
private:
  inline static bool compare_with_tenant_tablespace_id(const ObTablespaceSchema *lhs,
                                                    const ObTenantTablespaceId &tenant_outline_id);
  inline static bool equal_to_tenant_tablespace_id(const ObTablespaceSchema *lhs,
                                                  const ObTenantTablespaceId &tenant_outline_id);
private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  TablespaceInfos tablespace_infos_;
  ObTablespaceMap tablespace_map_;
};

} //end of schema
} //end of share
} //end of oceanbase
#endif //OCEANBASE_SHARE_SCHEMA_OB_TABLESPACE_MGR_H
