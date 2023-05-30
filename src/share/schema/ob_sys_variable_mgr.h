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

#ifndef OB_SYS_VARIABLE_MGR_H_
#define OB_SYS_VARIABLE_MGR_H_

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

class ObSimpleSysVariableSchema : public ObSchema
{
public:
  ObSimpleSysVariableSchema();
  explicit ObSimpleSysVariableSchema(common::ObIAllocator *allocator);
  ObSimpleSysVariableSchema(const ObSimpleSysVariableSchema &src_schema);
  virtual ~ObSimpleSysVariableSchema();
  ObSimpleSysVariableSchema &operator =(const ObSimpleSysVariableSchema &other);
  TO_STRING_KV(K_(tenant_id),
               K_(schema_version),
               K_(name_case_mode),
               K_(read_only));
  virtual void reset();
  bool is_valid() const;
  int64_t get_convert_size() const;
  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  inline void set_schema_version(const int64_t schema_version) { schema_version_ = schema_version; }
  inline int64_t get_schema_version() const { return schema_version_; }
  inline void set_name_case_mode(const common::ObNameCaseMode cmp_mode) { name_case_mode_ = cmp_mode; }
  inline common::ObNameCaseMode get_name_case_mode() const { return name_case_mode_; }
  inline void set_read_only(const bool read_only) { read_only_ = read_only; }
  inline bool get_read_only() const { return read_only_; }

private:
  uint64_t tenant_id_;
  int64_t schema_version_;
  common::ObNameCaseMode name_case_mode_;
  bool read_only_;
};

class ObSysVariableHashWrapper
{
public:
  ObSysVariableHashWrapper()
    : tenant_id_(common::OB_INVALID_ID) {}
  ObSysVariableHashWrapper(uint64_t tenant_id)
    : tenant_id_(tenant_id) {}
  ~ObSysVariableHashWrapper() {}
  inline uint64_t hash() const
  {
    uint64_t hash_ret = 0;
    hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
    return hash_ret;
  }
  inline bool operator==(const ObSysVariableHashWrapper &rv) const{
    return (tenant_id_ == rv.get_tenant_id());
  }
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  inline uint64_t get_tenant_id() const { return tenant_id_; }
  TO_STRING_KV(K_(tenant_id));

private:
  uint64_t tenant_id_;
};

template<class T, class V>
struct ObGetSysVariableKey {
  void operator()(const T &t, const V &v) const {
    UNUSED(t);
    UNUSED(v);
  }
};

template<>
struct ObGetSysVariableKey<ObSysVariableHashWrapper, ObSimpleSysVariableSchema *>
{
  ObSysVariableHashWrapper operator() (const ObSimpleSysVariableSchema *sys_variable) const {
    ObSysVariableHashWrapper hash_wrap;
    if (!OB_ISNULL(sys_variable)) {
      hash_wrap.set_tenant_id(sys_variable->get_tenant_id());
    }
    return hash_wrap;
  }
};


class ObSysVariableMgr
{
public:
  typedef common::ObSortedVector<ObSimpleSysVariableSchema *> SysVariableInfos;
  typedef common::hash::ObPointerHashMap<ObSysVariableHashWrapper, ObSimpleSysVariableSchema *, ObGetSysVariableKey, 128> ObSysVariableMap;
  typedef SysVariableInfos::iterator SysVariableIter;
  typedef SysVariableInfos::const_iterator ConstSysVariableIter;
  ObSysVariableMgr();
  explicit ObSysVariableMgr(common::ObIAllocator &allocator);
  virtual ~ObSysVariableMgr();
  int init();
  void reset();
  ObSysVariableMgr &operator =(const ObSysVariableMgr &other);
  int assign(const ObSysVariableMgr &other);
  int deep_copy(const ObSysVariableMgr &other);
  void dump() const;
  int get_sys_variable_schema_count(int64_t &sys_variable_count) const;
  int get_schema_statistics(ObSchemaStatisticsInfo &schema_info) const;
  int add_sys_variable(const ObSimpleSysVariableSchema &sys_variable);
  int add_sys_variables(const common::ObIArray<ObSimpleSysVariableSchema> &sys_variable_schemas);
  int del_sys_variable(const uint64_t tenant_id);
  int del_schemas_in_tenant(const uint64_t tenant_id);

  int get_sys_variable_schema(const uint64_t tenant_id,
                              const ObSimpleSysVariableSchema *&sys_variable) const;
  inline static bool compare_sys_variable(const ObSimpleSysVariableSchema *lhs,
                                          const ObSimpleSysVariableSchema *rhs) {
    return lhs->get_tenant_id() < rhs->get_tenant_id();
  }
  inline static bool equal_sys_variable(const ObSimpleSysVariableSchema *lhs,
                                        const ObSimpleSysVariableSchema *rhs) {
    return lhs->get_tenant_id() == rhs->get_tenant_id();
  }
  static int rebuild_sys_variable_hashmap(const SysVariableInfos &sys_var_infos,
                                          ObSysVariableMap &sys_var_map);
private:
  inline static bool compare_with_tenant_id(
                     const ObSimpleSysVariableSchema *lhs,
                     const uint64_t &tenant_id);
  inline static bool equal_to_tenant_id(
                     const ObSimpleSysVariableSchema *lhs,
                     const uint64_t &tenant_id);
private:
  bool is_inited_;
  common::ObArenaAllocator local_allocator_;
  common::ObIAllocator &allocator_;
  SysVariableInfos sys_variable_infos_;
  ObSysVariableMap sys_variable_map_;
};

} //end of schema
} //end of share
} //end of oceanbase
#endif
