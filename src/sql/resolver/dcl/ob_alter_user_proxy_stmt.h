/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_ALTER_USER_PROXY_STMT_H_
#define OB_ALTER_USER_PROXY_STMT_H_
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "lib/string/ob_strings.h"
namespace oceanbase
{
namespace sql
{


class ObAlterUserProxyStmt: public ObDDLStmt
{
public:
  ObAlterUserProxyStmt();
  explicit ObAlterUserProxyStmt(common::ObIAllocator *name_pool);
  virtual ~ObAlterUserProxyStmt();
  obrpc::ObAlterUserProxyArg &get_ddl_arg() { return arg_; }
  inline int set_proxy_user_id(const ObIArray<uint64_t>& proxy_id_array) { return arg_.proxy_user_ids_.assign(proxy_id_array); }
  inline int set_client_user_id(const ObIArray<uint64_t>& client_id_array) { return arg_.client_user_ids_.assign(client_id_array); }
  inline void set_tenant_id(const uint64_t tenant_id) { arg_.tenant_id_ = tenant_id; }
  inline void set_flags(const uint64_t flags) { arg_.flags_ = flags; }
  inline void add_flag(const uint64_t flags) { arg_.flags_ |= flags; }
  inline void set_is_grant(const uint64_t is_grant) { arg_.is_grant_ = is_grant; }

  int add_proxy_user_id(const uint64_t proxy_user_id) { return arg_.proxy_user_ids_.push_back(proxy_user_id); }
  int add_client_user_id(const uint64_t client_user_id) { return arg_.client_user_ids_.push_back(client_user_id); }
  int set_role_id_array(const ObIArray<uint64_t>& role_id_array) { return arg_.role_ids_.assign(role_id_array); }
  int add_role_id(const uint64_t role_id) { return arg_.role_ids_.push_back(role_id); }

  inline const ObIArray<uint64_t>& get_proxy_user_id() const { return arg_.proxy_user_ids_; }
  inline const ObIArray<uint64_t>& get_client_user_id() const { return arg_.client_user_ids_; }
  inline int get_tenant_id() const { return arg_.tenant_id_; }
  inline int get_flags() const { return arg_.flags_; }
  inline const ObIArray<uint64_t>& get_role_ids() const { return arg_.role_ids_; }

  // function members
  TO_STRING_KV(K_(stmt_type), K_(arg));
private:
  // data members
  obrpc::ObAlterUserProxyArg arg_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterUserProxyStmt);
};
} // end namespace sql
} // end namespace oceanbase

#endif //OB_ALTER_USER_PROXY_STMT_H_
