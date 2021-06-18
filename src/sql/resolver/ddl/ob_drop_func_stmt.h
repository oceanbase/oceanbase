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

#ifndef _OB_DROP_FUNC_STMT_H
#define _OB_DROP_FUNC_STMT_H 1

namespace oceanbase {
namespace sql {

class ObDropFuncStmt : public ObDDLStmt {
  public:
  ObDropFuncStmt() : ObDDLStmt(stmt::T_DROP_FUNC), drop_func_arg_()
  {}
  ~ObDropFuncStmt()
  {}
  void set_func_name(const common::ObString& func_name)
  {
    drop_func_arg_.name_ = func_name;
  }
  void set_tenant_id(uint64_t tenant_id)
  {
    drop_func_arg_.tenant_id_ = tenant_id;
  }
  obrpc::ObDropUserDefinedFunctionArg& get_drop_func_arg()
  {
    return drop_func_arg_;
  }
  obrpc::ObDDLArg& get_ddl_arg()
  {
    return drop_func_arg_;
  }
  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(K_(drop_func_arg));
    J_OBJ_END();
    return pos;
  }

  private:
  obrpc::ObDropUserDefinedFunctionArg drop_func_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObDropFuncStmt);
};

}  // namespace sql
}  // namespace oceanbase

#endif /* _OB_DROP_FUNC_STMT_H */
