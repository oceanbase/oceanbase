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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_DBLINK_MGR_H
#define OCEANBASE_SHARE_SCHEMA_OB_DBLINK_MGR_H

#include "share/ob_define.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/container/ob_vector.h"
#include "lib/allocator/page_arena.h"
#include "share/schema/ob_schema_struct.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/net/ob_addr.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObDbLinkNameHashWrapper
{
public:
  ObDbLinkNameHashWrapper()
    : tenant_id_(common::OB_INVALID_ID),
      dblink_name_()
  {}
  ObDbLinkNameHashWrapper(const uint64_t tenant_id,
                          const common::ObString &dblink_name)
    : tenant_id_(tenant_id),
      dblink_name_(dblink_name)
  {}
  ~ObDbLinkNameHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator==(const ObDbLinkNameHashWrapper &rv) const;
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_dblink_name(const common::ObString &dblink_name) { dblink_name_ = dblink_name;}
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline const common::ObString &get_dblink_name() const { return dblink_name_; }
private:
  uint64_t tenant_id_;
  common::ObString dblink_name_;
};

inline uint64_t ObDbLinkNameHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  common::ObCollationType cs_type = common::CS_TYPE_UTF8MB4_GENERAL_CI;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::ObCharset::hash(cs_type, dblink_name_, hash_ret);
  return hash_ret;
}

inline bool ObDbLinkNameHashWrapper::operator==(const ObDbLinkNameHashWrapper &rv) const
{
  common::ObCollationType cs_type = common::CS_TYPE_UTF8MB4_GENERAL_CI;
  return (tenant_id_ == rv.tenant_id_)
      && (0 == common::ObCharset::strcmp(cs_type, dblink_name_, rv.dblink_name_));
}

template<class T, class V>
struct ObGetDbLinkKey
{
  void operator()(const T & t, const V &v) const
  {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetDbLinkKey<uint64_t, ObDbLinkSchema *>
{
  uint64_t operator()(const ObDbLinkSchema *dblink_schema) const
  {
    return !OB_ISNULL(dblink_schema) ? dblink_schema->get_dblink_id() : common::OB_INVALID_ID;
  }
};

template<>
struct ObGetDbLinkKey<ObDbLinkNameHashWrapper, ObDbLinkSchema *>
{
  ObDbLinkNameHashWrapper operator()(const ObDbLinkSchema *dblink_schema) const
  {
    ObDbLinkNameHashWrapper name_wrap;
    if (!OB_ISNULL(dblink_schema)) {
      name_wrap.set_tenant_id(dblink_schema->get_tenant_id());
      name_wrap.set_dblink_name(dblink_schema->get_dblink_name());
    }
    return name_wrap;
  }
};

class ObDbLinkMgr
{
  typedef common::ObSortedVector<ObDbLinkSchema *> DbLinkSchemas;
  typedef common::hash::ObPointerHashMap<uint64_t, ObDbLinkSchema *,
      ObGetDbLinkKey, 128> DbLinkIdMap;
  typedef common::hash::ObPointerHashMap<ObDbLinkNameHashWrapper, ObDbLinkSchema *,
      ObGetDbLinkKey, 128> DbLinkNameMap;
  typedef DbLinkSchemas::iterator DbLinkIter;
  typedef DbLinkSchemas::const_iterator ConstDbLinkIter;

public:
  ObDbLinkMgr();
  explicit ObDbLinkMgr(common::ObIAllocator &allocator);
  virtual ~ObDbLinkMgr();
  int init();
  void reset();
  ObDbLinkMgr &operator=(const ObDbLinkMgr &other);
  int assign(const ObDbLinkMgr &other);
  int deep_copy(const ObDbLinkMgr &other);
  void dump() const;

  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
  int add_dblinks(const common::ObIArray<ObDbLinkSchema> &dblink_schemas);
  int del_dblinks(const common::ObIArray<ObTenantDbLinkId> &tenant_dblink_ids);
  int add_dblink(const ObDbLinkSchema &dblink_schema);
  int del_dblink(const ObTenantDbLinkId &tenant_dblink_id);
  int get_dblink_schema(const uint64_t dblink_id,
                        const ObDbLinkSchema *&dblink_schema) const;
  int get_dblink_schema(const uint64_t tenant_id, const common::ObString &dblink_name,
                        const ObDbLinkSchema *&dblink_schema) const;
  int get_dblink_schemas_in_tenant(const uint64_t tenant_id,
                                   common::ObIArray<const ObDbLinkSchema *> &dblink_schemas) const;
  int del_dblink_schemas_in_tenant(const uint64_t tenant_id);
  int get_dblink_schema_count(int64_t &schema_count) const;

private:
  inline bool check_inner_stat() const;
  inline static bool compare_dblink(const ObDbLinkSchema *lhs,
                                    const ObDbLinkSchema *rhs);
  inline static bool equal_dblink(const ObDbLinkSchema *lhs,
                                  const ObDbLinkSchema *rhs);
  inline static bool compare_with_tenant_dblink_id(const ObDbLinkSchema *lhs,
                                                   const ObTenantDbLinkId &tenant_dblink_id);
  inline static bool equal_with_tenant_dblink_id(const ObDbLinkSchema *lhs,
                                                 const ObTenantDbLinkId &tenant_dblink_id);
  int rebuild_dblink_hashmap();

private:
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  DbLinkSchemas dblink_schemas_;
  DbLinkIdMap dblink_id_map_;
  DbLinkNameMap dblink_name_map_;
  // TODO(jiuren): temp code, dblink_pool_ should put in class ObServer.
  common::sqlclient::ObMySQLConnectionPool dblink_pool_;
  bool is_inited_;
};

} //end of schema
} //end of share
} //end of oceanbase
#endif //OCEANBASE_SHARE_SCHEMA_OB_DBLINK_MGR_H
