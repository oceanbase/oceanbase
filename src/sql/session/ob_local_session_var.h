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


#ifndef OCEANBASE_SQL_LOCAL_SESSION_VAR_H_
#define OCEANBASE_SQL_LOCAL_SESSION_VAR_H_

#include "share/system_variable/ob_sys_var_class_type.h"
#include "lib/container/ob_se_array.h"
#include "common/object/ob_object.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{

namespace share
{
  enum ObSysVarClassType;
}

namespace sql
{
class ObBasicSessionInfo;

struct ObSessionSysVar {
  OB_UNIS_VERSION(1);
public:
  TO_STRING_KV(K_(type), K_(val));
  bool is_equal(const ObObj &other_val) const;
  int64_t get_deep_copy_size() const;
  static int get_sys_var_val_str(const share::ObSysVarClassType var_type,
                                 const ObObj &var_val,
                                 common::ObIAllocator &allocator,
                                 ObString &val_str);

  share::ObSysVarClassType type_;
  ObObj val_;
};

class ObLocalSessionVar {
  OB_UNIS_VERSION(1);
public:
  ObLocalSessionVar(common::ObIAllocator *alloc)
    :alloc_(alloc),
    local_session_vars_(alloc) {
    }
  ObLocalSessionVar ()
    :alloc_(NULL) {
    }
  ~ObLocalSessionVar() { reset(); }
  void set_allocator(common::ObIAllocator *allocator) {
    alloc_ = allocator;
    local_session_vars_.set_allocator(allocator);
  }
  void reset();
  int set_local_var_capacity(int64_t sz);
  template<class T>
  int set_local_vars(T &var_array);
  int add_local_var(share::ObSysVarClassType var_type, const ObObj &value);
  int add_local_var(const ObSessionSysVar *var);
  int get_local_var(share::ObSysVarClassType var_type, ObSessionSysVar *&sys_var) const;
  int remove_local_var(share::ObSysVarClassType var_type);
  int get_local_vars(common::ObIArray<const ObSessionSysVar *> &var_array) const;
  int load_session_vars(const sql::ObBasicSessionInfo *session);
  int reserve_max_local_vars_capacity();
  int update_session_vars_with_local(sql::ObBasicSessionInfo &session) const;
  int remove_vars_same_with_session(const ObBasicSessionInfo *session);
  int get_different_vars_from_session(const ObBasicSessionInfo *session,
                                      common::ObIArray<const ObSessionSysVar*> &local_diff_vars,
                                      common::ObIArray<ObObj> &session_vals) const;
  int check_var_same_with_session(const ObBasicSessionInfo &session,
                                  const ObSessionSysVar *local_var,
                                  bool &is_same,
                                  ObObj *diff_val = NULL) const;
  int deep_copy(const ObLocalSessionVar &other);
  int deep_copy_self();
  int assign(const ObLocalSessionVar &other);
  bool operator == (const ObLocalSessionVar& other) const;
  int64_t get_deep_copy_size() const ;
  int64_t get_var_count() const { return local_session_vars_.count(); }
  int gen_local_session_var_str(common::ObIAllocator &allocator, ObString &local_session_var_str) const;
  int fill_local_session_var_from_str(const ObString &local_session_var_str);
  DECLARE_TO_STRING;
private:
  const static share::ObSysVarClassType ALL_LOCAL_VARS[];
  common::ObIAllocator *alloc_;
  ObFixedArray<ObSessionSysVar *, common::ObIAllocator> local_session_vars_;
};

} // namespace sql
}//namespace oceanbase

#endif /* _OB_OCEANBASE_SCHEMA_SCHEMA_STRUCT_H */
