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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_CONTEXT_MGR_H
#define OCEANBASE_SHARE_SCHEMA_OB_CONTEXT_MGR_H

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
class ObContextSchema;
class ObSchemaStatisticsInfo;
class ObContextKey;

class ObContextHashWrapper
{
public:
  ObContextHashWrapper()
    : tenant_id_(common::OB_INVALID_ID),
      ctx_namespace_() {}
  ObContextHashWrapper(uint64_t tenant_id, const common::ObString &ctx_namespace)
    : tenant_id_(tenant_id),
      ctx_namespace_(ctx_namespace) {}
  ~ObContextHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator==(const ObContextHashWrapper &rv) const;
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_context_namespace(const common::ObString &ctx_namespace) { ctx_namespace_ = ctx_namespace; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline const common::ObString &get_context_namespace() const { return ctx_namespace_; }
  TO_STRING_KV(K_(tenant_id), K_(ctx_namespace));

private:
  uint64_t tenant_id_;
  common::ObString ctx_namespace_;
};

inline bool ObContextHashWrapper::operator == (const ObContextHashWrapper &rv) const
{
  return (tenant_id_ == rv.get_tenant_id())
      && (ctx_namespace_ == rv.get_context_namespace());
}

inline uint64_t ObContextHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(ctx_namespace_.ptr(), ctx_namespace_.length(), hash_ret);
  return hash_ret;
}

template<class T, class V>
struct ObGetContextKey {
  void operator()(const T &t, const V &v) const {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetContextKey<ObContextHashWrapper, ObContextSchema *>
{
  ObContextHashWrapper operator() (const ObContextSchema * context) const;
};

class ObContextMgr
{
public:
  typedef common::ObSortedVector<ObContextSchema *> ContextInfos;
  typedef common::hash::ObPointerHashMap<ObContextHashWrapper, ObContextSchema *,
                                         ObGetContextKey, 128> ObContextMap;
  typedef ContextInfos::iterator ContextIter;
  typedef ContextInfos::const_iterator ConstContextIter;
  ObContextMgr();
  explicit ObContextMgr(common::ObIAllocator &allocator);
  virtual ~ObContextMgr();
  int init();
  void reset();
  ObContextMgr &operator =(const ObContextMgr &other);
  int assign(const ObContextMgr &other);
  int deep_copy(const ObContextMgr &other);
  int get_context_schema_count(int64_t &context_schema_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
  int add_context(const ObContextSchema &context_schema);
  int add_contexts(const common::ObIArray<ObContextSchema> &context_schema);
  int del_context(const ObContextKey &context);

  int get_context_schema(uint64_t tenant_id,
                         uint64_t context_id,
                         const ObContextSchema *&context_schema) const;

  int get_context_schemas_in_tenant(const uint64_t tenant_id,
      common::ObIArray<const ObContextSchema *> &context_schemas) const;
  int get_context_schema_with_name(const uint64_t tenant_id,
                                const common::ObString &ctx_namespace,
                                const ObContextSchema *&context_schema) const;

  int del_schemas_in_tenant(const uint64_t tenant_id);
  template<typename Filter, typename Acation, typename EarlyStopCondition>
  int for_each(Filter &filter, Acation &action, EarlyStopCondition &condition);
  static bool compare_context(const ObContextSchema *lhs,
                                     const ObContextSchema *rhs);
  static bool equal_context(const ObContextSchema *lhs,
                                   const ObContextSchema *rhs);
  static int rebuild_context_hashmap(const ContextInfos &context_infos,
                                     ObContextMap &context_map);
private:
  inline static bool compare_with_context_key(const ObContextSchema *lhs,
                                              const ObContextKey &context_key);
  inline static bool equal_to_context_key(const ObContextSchema *lhs,
                                          const ObContextKey &context_key);
private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  ContextInfos context_infos_;
  ObContextMap context_map_;
};

} //end of schema
} //end of share
} //end of oceanbase
#endif //OCEANBASE_SHARE_SCHEMA_OB_CONTEXT_MGR_H
