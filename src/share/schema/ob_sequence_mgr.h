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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_SEQUENCE_MGR_H
#define OCEANBASE_SHARE_SCHEMA_OB_SEQUENCE_MGR_H

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
class ObSequenceSchema;
class ObSchemaStatisticsInfo;
class ObTenantSequenceId;

class ObSequenceHashWrapper
{
public:
  ObSequenceHashWrapper()
    : tenant_id_(common::OB_INVALID_ID),
      database_id_(common::OB_INVALID_ID),
      sequence_name_() {}
  ObSequenceHashWrapper(uint64_t tenant_id, uint64_t database_id,
                      const common::ObString &sequence_name)
    : tenant_id_(tenant_id),
      database_id_(database_id),
      sequence_name_(sequence_name) {}
  ~ObSequenceHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator==(const ObSequenceHashWrapper &rv) const;
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_database_id(uint64_t database_id) { database_id_ = database_id; }
  inline void set_sequence_name(const common::ObString &sequence_name) { sequence_name_ = sequence_name; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline uint64_t get_database_id() const { return database_id_; }
  inline const common::ObString &get_sequence_name() const { return sequence_name_; }
  TO_STRING_KV(K_(tenant_id), K_(database_id), K_(sequence_name));

private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  common::ObString sequence_name_;
};

inline bool ObSequenceHashWrapper::operator == (const ObSequenceHashWrapper &rv) const
{
  return (tenant_id_ == rv.get_tenant_id())
      && (database_id_ == rv.get_database_id())
      && (sequence_name_ == rv.get_sequence_name());
}

inline uint64_t ObSequenceHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&database_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::murmurhash(sequence_name_.ptr(), sequence_name_.length(), hash_ret);
  return hash_ret;
}

template<class T, class V>
struct ObGetSequenceKey {
  void operator()(const T &t, const V &v) const {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetSequenceKey<ObSequenceHashWrapper, ObSequenceSchema *>
{
  ObSequenceHashWrapper operator() (const ObSequenceSchema * sequence) const;
};

class ObSequenceMgr
{
public:
  typedef common::ObSortedVector<ObSequenceSchema *> SequenceInfos;
  typedef common::hash::ObPointerHashMap<ObSequenceHashWrapper, ObSequenceSchema *,
                                         ObGetSequenceKey, 128> ObSequenceMap;
  typedef SequenceInfos::iterator SequenceIter;
  typedef SequenceInfos::const_iterator ConstSequenceIter;
  ObSequenceMgr();
  explicit ObSequenceMgr(common::ObIAllocator &allocator);
  virtual ~ObSequenceMgr();
  int init();
  void reset();
  ObSequenceMgr &operator =(const ObSequenceMgr &other);
  int assign(const ObSequenceMgr &other);
  int deep_copy(const ObSequenceMgr &other);
  void dump() const;
  int get_sequence_schema_count(int64_t &sequence_schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
  int add_sequence(const ObSequenceSchema &sequence_schema);
  int add_sequences(const common::ObIArray<ObSequenceSchema> &sequence_schema);
  int del_sequence(const ObTenantSequenceId &sequence);

  int get_sequence_schema(const uint64_t sequence_id,
                         const ObSequenceSchema *&sequence_schema) const;
  int get_sequence_info_version(uint64_t sequence_id, int64_t &sequence_version) const;
  int get_sequence_schema_with_name(const uint64_t tenant_id,
                                   const uint64_t database_id,
                                   const common::ObString &name,
                                   const ObSequenceSchema *&sequence_schema) const;
  int get_object(const uint64_t tenant_id,
                 const uint64_t database_id,
                 const common::ObString &name,
                 uint64_t &obj_database_id,
                 uint64_t &sequence_id,
                 common::ObString &obj_table_name,
                 bool &do_exist) const;
  int get_sequence_schemas_in_tenant(const uint64_t tenant_id,
      common::ObIArray<const ObSequenceSchema *> &sequence_schemas) const;
  int get_sequence_schemas_in_database(const uint64_t tenant_id,
      const uint64_t database_id,
      common::ObIArray<const ObSequenceSchema *> &sequence_schemas) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  template<typename Filter, typename Acation, typename EarlyStopCondition>
  int for_each(Filter &filter, Acation &action, EarlyStopCondition &condition);
  static bool compare_sequence(const ObSequenceSchema *lhs,
                                     const ObSequenceSchema *rhs);
  static bool equal_sequence(const ObSequenceSchema *lhs,
                                   const ObSequenceSchema *rhs);
  static int rebuild_sequence_hashmap(const SequenceInfos &sequence_infos,
                                     ObSequenceMap &sequence_map);
private:
  inline static bool compare_with_tenant_sequence_id(const ObSequenceSchema *lhs,
                                                    const ObTenantSequenceId &tenant_outline_id);
  inline static bool equal_to_tenant_sequence_id(const ObSequenceSchema *lhs,
                                                  const ObTenantSequenceId &tenant_outline_id);
private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  SequenceInfos sequence_infos_;
  ObSequenceMap sequence_map_;
};

} //end of schema
} //end of share
} //end of oceanbase
#endif //OCEANBASE_SHARE_SCHEMA_OB_SEQUENCE_MGR_H
