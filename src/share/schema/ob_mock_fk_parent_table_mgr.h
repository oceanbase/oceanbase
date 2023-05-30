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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_MOCK_FK_PARENT_TABLE_MGR_H
#define OCEANBASE_SHARE_SCHEMA_OB_MOCK_FK_PARENT_TABLE_MGR_H

#include "share/ob_define.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/container/ob_vector.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSimpleMockFKParentTableSchema;
class ObMockFKParentTableKey;
class ObSchemaStatisticsInfo;

class ObMockFKParentTableHashWrapper
{
public:
  ObMockFKParentTableHashWrapper()
    : tenant_id_(common::OB_INVALID_ID),
      database_id_(common::OB_INVALID_ID),
      mock_table_name_() {}
  ObMockFKParentTableHashWrapper(
      uint64_t tenant_id, uint64_t database_id, const common::ObString &mock_table_name)
    : tenant_id_(tenant_id), database_id_(database_id), mock_table_name_(mock_table_name) {}
  ~ObMockFKParentTableHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator==(const ObMockFKParentTableHashWrapper &rv) const;
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_database_id(uint64_t database_id) { database_id_ = database_id; }
  inline void set_mock_fk_parent_table_name(const common::ObString &name) { mock_table_name_ = name; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline const common::ObString &get_mock_table_name() const { return mock_table_name_; }
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(mock_table_name));

private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString mock_table_name_;
};

inline bool ObMockFKParentTableHashWrapper::operator == (
    const ObMockFKParentTableHashWrapper &rv) const
{
  return (tenant_id_ == rv.get_tenant_id())
      && (database_id_ == rv.get_database_id())
      && (mock_table_name_ == rv.get_mock_table_name());
}

inline uint64_t ObMockFKParentTableHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(mock_table_name_.ptr(), mock_table_name_.length(), hash_ret);
  return hash_ret;
}

template<class T, class V>
struct ObGetMockFKParentTableKey {
  void operator()(const T &t, const V &v) const {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetMockFKParentTableKey<ObMockFKParentTableHashWrapper, ObSimpleMockFKParentTableSchema *>
{
  ObMockFKParentTableHashWrapper operator() (const ObSimpleMockFKParentTableSchema * schema) const;
};

class ObMockFKParentTableMgr
{
public:
  typedef common::ObSortedVector<ObSimpleMockFKParentTableSchema *> MockFKParentTableInfos;
  typedef common::hash::ObPointerHashMap<ObMockFKParentTableHashWrapper,
                                         ObSimpleMockFKParentTableSchema *,
                                         ObGetMockFKParentTableKey, 128> MockFKParentTableMap;
  typedef MockFKParentTableInfos::iterator MockFKParentTableIter;
  typedef MockFKParentTableInfos::const_iterator ConstMockFKParentTableIter;
  ObMockFKParentTableMgr();
  explicit ObMockFKParentTableMgr(common::ObIAllocator &allocator);
  virtual ~ObMockFKParentTableMgr();
  int init();
  void reset();
  ObMockFKParentTableMgr &operator =(const ObMockFKParentTableMgr &other);
  int assign(const ObMockFKParentTableMgr &other);
  int deep_copy(const ObMockFKParentTableMgr &other);

  static bool compare_mock_fk_parent_table(
      const ObSimpleMockFKParentTableSchema *lhs,
      const ObSimpleMockFKParentTableSchema *rhs);
  static bool equal_mock_fk_parent_table(
      const ObSimpleMockFKParentTableSchema *lhs,
      const ObSimpleMockFKParentTableSchema *rhs);
  static int rebuild_mock_fk_parent_table_hashmap(
      const MockFKParentTableInfos &infos,
      MockFKParentTableMap &map);

  int add_mock_fk_parent_table(const ObSimpleMockFKParentTableSchema &schema);
  int add_mock_fk_parent_tables(const common::ObIArray<ObSimpleMockFKParentTableSchema> &schemas);
  int del_mock_fk_parent_table(const ObMockFKParentTableKey &key);
  int del_schemas_in_tenant(const uint64_t tenant_id);

  template<typename Filter, typename Acation, typename EarlyStopCondition>
    int for_each(Filter &filter, Acation &action, EarlyStopCondition &condition);

  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
  int get_mock_fk_parent_table_schema_count(int64_t &count) const;
  int get_mock_fk_parent_table_schema(
      const uint64_t tenant_id,
      const uint64_t mock_fk_parent_table_id,
      const ObSimpleMockFKParentTableSchema *&schema) const;
  int get_mock_fk_parent_table_schemas_in_tenant(
      const uint64_t tenant_id,
      common::ObIArray<const ObSimpleMockFKParentTableSchema *> &schemas) const;
  int get_mock_fk_parent_table_schemas_in_database(
      const uint64_t tenant_id,
      const uint64_t database_id,
      common::ObIArray<const ObSimpleMockFKParentTableSchema *> &schemas) const;
  int get_mock_fk_parent_table_schema_with_name(
      const uint64_t tenant_id,
      const uint64_t database_id,
      const common::ObString &mock_table_name,
      const ObSimpleMockFKParentTableSchema *&schema) const;

private:
  inline static bool compare_with_mock_fk_parent_table_key(
      const ObSimpleMockFKParentTableSchema *lhs,
      const ObMockFKParentTableKey &rhs);
  inline static bool equal_to_mock_fk_parent_table_key(
      const ObSimpleMockFKParentTableSchema *lhs,
      const ObMockFKParentTableKey &rhs);
private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  MockFKParentTableInfos mock_fk_parent_table_infos_;
  MockFKParentTableMap mock_fk_parent_table_map_;
};

} //end of schema
} //end of share
} //end of oceanbase
#endif //OCEANBASE_SHARE_SCHEMA_OB_MOCK_FK_PARENT_TABLE_MGR_H
