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

#ifndef OCEANBASE_SHARE_SCHEMA_OB_SECURITY_AUDIT_MGR_H
#define OCEANBASE_SHARE_SCHEMA_OB_SECURITY_AUDIT_MGR_H

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

class ObAuditHashWrapper
{
public:
  ObAuditHashWrapper()
    : tenant_id_(common::OB_INVALID_TENANT_ID),
      audit_type_(AUDIT_INVALID),
      owner_id_(common::OB_INVALID_ID),
      operation_type_(AUDIT_OP_INVALID) {}

  ObAuditHashWrapper(const uint64_t tenant_id, const ObSAuditType audit_type,
      const uint64_t owner_id, const ObSAuditOperationType operation_type)
    : tenant_id_(tenant_id), audit_type_(audit_type),
      owner_id_(owner_id), operation_type_(operation_type) {}
  ~ObAuditHashWrapper() {}
  inline uint64_t hash() const;
  inline bool operator==(const ObAuditHashWrapper &rv) const;
  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline void set_audit_type(const ObSAuditType audit_type) { audit_type_ = audit_type; }
  inline void set_owner_id(const uint64_t owner_id) { owner_id_ = owner_id; }
  inline void set_operation_type(const ObSAuditOperationType operation_type) { operation_type_ = operation_type; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline ObSAuditType get_audit_type() const { return audit_type_; }
  inline uint64_t get_owner_id() const { return owner_id_; }
  inline ObSAuditOperationType get_operation_type() const { return operation_type_; }
  TO_STRING_KV(K_(tenant_id), K_(audit_type), K_(owner_id), K_(operation_type));

private:
  uint64_t tenant_id_;
  ObSAuditType audit_type_;
  uint64_t owner_id_;
  ObSAuditOperationType operation_type_;
};

inline uint64_t ObAuditHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&audit_type_, sizeof(ObSAuditType), hash_ret);
  hash_ret = common::murmurhash(&owner_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::murmurhash(&operation_type_, sizeof(ObSAuditOperationType), hash_ret);
  return hash_ret;
}

inline bool ObAuditHashWrapper::operator==(const ObAuditHashWrapper &rv) const
{
  return (tenant_id_ == rv.tenant_id_
          && audit_type_ == rv.audit_type_
          && owner_id_ == rv.owner_id_
          && operation_type_ == rv.operation_type_);
}



template<class T, class V>
struct ObGetAuditKey {
  void operator()(const T &t, const V &v) const {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetAuditKey<ObAuditHashWrapper, ObSAuditSchema *>
{
  ObAuditHashWrapper operator() (const ObSAuditSchema * audit) const {
    ObAuditHashWrapper hash_wrap;
    if (OB_NOT_NULL(audit)) {
      hash_wrap.set_tenant_id(audit->get_tenant_id());
      hash_wrap.set_audit_type(audit->get_audit_type());
      hash_wrap.set_owner_id(audit->get_owner_id());
      hash_wrap.set_operation_type(audit->get_operation_type());
    }
    return hash_wrap;
  }
};

class ObSAuditMgr
{
public:
  typedef common::ObSortedVector<ObSAuditSchema *> AuditInfos;
  typedef common::hash::ObPointerHashMap<ObAuditHashWrapper, ObSAuditSchema *,
                                         ObGetAuditKey, 128> ObAuditMap;
  typedef AuditInfos::iterator AuditIter;
  typedef AuditInfos::const_iterator ConstAuditIter;
  ObSAuditMgr();
  explicit ObSAuditMgr(common::ObIAllocator &allocator);
  virtual ~ObSAuditMgr();
  int init();
  void reset();
  ObSAuditMgr &operator =(const ObSAuditMgr &other);
  int assign(const ObSAuditMgr &other);
  int deep_copy(const ObSAuditMgr &other);
  void dump() const;
  int add_audit(const ObSAuditSchema &audit_schema);
  int add_audits(const common::ObIArray<ObSAuditSchema> &audit_schema);
  int del_audit(const ObTenantAuditKey &audit);
  int get_stmt_audit_schema(const uint64_t tenant_id,
                            const ObSAuditType audit_type,
                            const uint64_t user_id,
                            const int64_t audit_stmt_type,
                            const ObSAuditSchema *&audit_schema) const;
  int get_audit_schema(const uint64_t &audit_id,
                       const ObSAuditSchema *&audit_schema) const;
  int get_audit_schema(const uint64_t tenant_id,
                       const ObSAuditType audit_type,
                       const uint64_t owner_id,
                       const ObSAuditOperationType operation_type,
                       const ObSAuditSchema *&audit_schema) const;
  int get_audit_schemas_in_tenant(const uint64_t tenant_id,
                                  common::ObIArray<const ObSAuditSchema *> &audit_schemas) const;
  int get_audit_schemas_in_tenant(const uint64_t tenant_id,
                                  const ObSAuditType audit_type,
                                  const uint64_t owner_id,
                                  common::ObIArray<const ObSAuditSchema *> &audit_schemas) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
  int get_audit_schema_count(int64_t &audit_schema_count) const;
  int del_schemas_in_tenant(const uint64_t tenant_id);
  template<typename Filter, typename Acation, typename EarlyStopCondition>
  int for_each(Filter &filter, Acation &action, EarlyStopCondition &condition);
  inline static bool compare_adudit(const ObSAuditSchema *lhs,
                                    const ObSAuditSchema *rhs)
  {
    return (lhs->get_audit_key() < rhs->get_audit_key());
  }
  inline static bool equal_audit(const ObSAuditSchema *lhs,
                                    const ObSAuditSchema *rhs)
  {
    return (lhs->get_audit_key() == rhs->get_audit_key());
  }
  static int rebuild_audit_hashmap(const AuditInfos &audit_infos,
                                   ObAuditMap &audit_map);
private:
  inline static bool compare_with_tenant_audit_id(const ObSAuditSchema *lhs,
                                                  const ObTenantAuditKey &tenant_outline_id);
  inline static bool equal_to_tenant_audit_id(const ObSAuditSchema *lhs,
                                              const ObTenantAuditKey &tenant_outline_id);
private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  AuditInfos audit_infos_;
  ObAuditMap audit_map_;
};

} //end of schema
} //end of share
} //end of oceanbase
#endif //OCEANBASE_SHARE_SCHEMA_OB_SECURITY_AUDIT_MGR_H
