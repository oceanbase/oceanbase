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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_DIRECTORY_MGR_H_
#define OCEANBASE_SHARE_SCHEMA_OB_DIRECTORY_MGR_H_

#include "lib/utility/ob_macro_utils.h"
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
class ObDirectoryNameHashKey
{
public:
  ObDirectoryNameHashKey()
    : tenant_id_(common::OB_INVALID_TENANT_ID), directory_name_()
  {
  }
  ObDirectoryNameHashKey(uint64_t tenant_id, common::ObString directory_name)
    : tenant_id_(tenant_id), directory_name_(directory_name)
  {
  }
  ~ObDirectoryNameHashKey()
  {
  }
  uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
    hash_ret = common::murmurhash(directory_name_.ptr(), directory_name_.length(), hash_ret);
    return hash_ret;
  }
  bool operator == (const ObDirectoryNameHashKey &rv) const
  {
    return tenant_id_ == rv.tenant_id_ && directory_name_ == rv.directory_name_;
  }
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_directory_name(const common::ObString &directory_name) { directory_name_ = directory_name;}
  uint64_t get_tenant_id() const { return tenant_id_; }
  const common::ObString &get_directory_name() const { return directory_name_; }
private:
  uint64_t tenant_id_;
  common::ObString directory_name_;
};

template<class T, class V>
struct ObGetDirectoryKey
{
  void operator()(const T &t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetDirectoryKey<ObDirectoryNameHashKey, ObDirectorySchema *>
{
  ObDirectoryNameHashKey operator()(const ObDirectorySchema *schema) const
  {
    return OB_ISNULL(schema) ?
          ObDirectoryNameHashKey()
        : ObDirectoryNameHashKey(schema->get_tenant_id(), schema->get_directory_name_str());
  }
};

template<>
struct ObGetDirectoryKey<uint64_t, ObDirectorySchema *>
{
  uint64_t operator()(const ObDirectorySchema *schema) const
  {
    return OB_ISNULL(schema) ? common::OB_INVALID_ID : schema->get_directory_id();
  }
};

class ObDirectoryMgr
{
public:
  ObDirectoryMgr();
  explicit ObDirectoryMgr(common::ObIAllocator &allocator);
  virtual ~ObDirectoryMgr();

  ObDirectoryMgr &operator=(const ObDirectoryMgr &other);

  int init();
  void reset();

  int assign(const ObDirectoryMgr &other);
  int deep_copy(const ObDirectoryMgr &other);
  int add_directory(const ObDirectorySchema &schema);
  // TYPO, but we keep it as "directorys" other than "directories" for macro usage
  int add_directorys(const common::ObIArray<ObDirectorySchema> &schemas);
  int del_directory(const ObTenantDirectoryId &id);
  int get_directory_schema_by_id(const uint64_t directory_id,
                                 const ObDirectorySchema *&schema) const;
  int get_directory_schema_by_name(const uint64_t tenant_id,
                                   const common::ObString &name,
                                   const ObDirectorySchema *&schema) const;
  int get_directory_schemas_in_tenant(const uint64_t tenant_id,
                                      common::ObIArray<const ObDirectorySchema *> &schemas) const;
  int del_directory_schemas_in_tenant(const uint64_t tenant_id);
  int get_directory_schema_count(int64_t &schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
private:
  int rebuild_directory_hashmap();
  static bool schema_compare(const ObDirectorySchema *lhs, const ObDirectorySchema *rhs);
  static bool schema_equal(const ObDirectorySchema *lhs, const ObDirectorySchema *rhs);
  static bool compare_with_tenant_directory_id(const ObDirectorySchema *lhs, const ObTenantDirectoryId &id);
  static bool equal_to_tenant_directory_id(const ObDirectorySchema *lhs, const ObTenantDirectoryId &id);
public:
  typedef common::ObSortedVector<ObDirectorySchema *> DirectoryInfos;
  typedef common::hash::ObPointerHashMap<ObDirectoryNameHashKey,
                                         ObDirectorySchema *,
                                         ObGetDirectoryKey, 128> ObDirectoryNameMap;
  typedef common::hash::ObPointerHashMap<uint64_t,
                                         ObDirectorySchema *,
                                         ObGetDirectoryKey, 128> ObDirectoryIdMap;
  typedef DirectoryInfos::iterator DirectoryIter;
  typedef DirectoryInfos::const_iterator ConstDirectoryIter;
private:
  static const char *DIRECTORY_MGR;
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  DirectoryInfos directory_infos_;
  ObDirectoryNameMap directory_name_map_;
  ObDirectoryIdMap directory_id_map_;
};

OB_INLINE bool ObDirectoryMgr::schema_compare(const ObDirectorySchema *lhs, const ObDirectorySchema *rhs)
{
  return lhs->get_tenant_id() != rhs->get_tenant_id()
      ? lhs->get_tenant_id() < rhs->get_tenant_id()
      : lhs->get_directory_id() < rhs->get_directory_id();
}

OB_INLINE bool ObDirectoryMgr::schema_equal(const ObDirectorySchema *lhs, const ObDirectorySchema *rhs)
{
  return lhs->get_tenant_id() == rhs->get_tenant_id()
      && lhs->get_directory_id() == rhs->get_directory_id();
}

OB_INLINE bool ObDirectoryMgr::compare_with_tenant_directory_id(const ObDirectorySchema *lhs, const ObTenantDirectoryId &id)
{
  return NULL != lhs ? (lhs->get_tenant_directory_id() < id) : false;
}

OB_INLINE bool ObDirectoryMgr::equal_to_tenant_directory_id(const ObDirectorySchema *lhs, const ObTenantDirectoryId &id)
{
  return NULL != lhs ? (lhs->get_tenant_directory_id() == id) : false;
}
} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SCHEMA_OB_DIRECTORY_MGR_H_
