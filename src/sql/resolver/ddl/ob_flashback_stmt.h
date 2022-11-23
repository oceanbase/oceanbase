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

#ifndef OCEANBASE_SQL_OB_FLASHBACK_STMT_
#define OCEANBASE_SQL_OB_FLASHBACK_STMT_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace sql
{
/**
 * flashback table
 */

class ObFlashBackTableFromRecyclebinStmt : public ObDDLStmt
{
public:
  ObFlashBackTableFromRecyclebinStmt() : ObDDLStmt(stmt::T_FLASHBACK_TABLE_FROM_RECYCLEBIN) {}
  explicit ObFlashBackTableFromRecyclebinStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_FLASHBACK_TABLE_FROM_RECYCLEBIN)
  {}
  virtual ~ObFlashBackTableFromRecyclebinStmt() {}
  const obrpc::ObFlashBackTableFromRecyclebinArg& get_flashback_table_arg() const { return flashback_table_arg_; }
  inline void set_tenant_id(const uint64_t tenant_id);
  uint64_t get_tenant_id() const { return flashback_table_arg_.tenant_id_; }
  inline void set_origin_table_id(const uint64_t origin_table);
  uint64_t get_origin_table_id() const { return flashback_table_arg_.origin_table_id_; }
  common::ObString get_origin_db_name() const { return flashback_table_arg_.origin_db_name_; };
  common::ObString get_origin_table_name() const { return flashback_table_arg_.origin_table_name_; };
  void set_origin_table_name(const common::ObString &origin_table_name);
  void set_origin_db_name(const common::ObString &origin_db_name);
  void set_new_table_name(const common::ObString &new_table_name);
  void set_new_db_name(const common::ObString &new_db_name);
  virtual obrpc::ObDDLArg &get_ddl_arg() { return flashback_table_arg_; }
  TO_STRING_KV(K_(stmt_type),K_(flashback_table_arg));
private:
  obrpc::ObFlashBackTableFromRecyclebinArg flashback_table_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObFlashBackTableFromRecyclebinStmt);
};

inline void ObFlashBackTableFromRecyclebinStmt::set_tenant_id(const uint64_t tenant_id)
{
  flashback_table_arg_.tenant_id_ = tenant_id;
}

inline void ObFlashBackTableFromRecyclebinStmt::set_origin_db_name(
            const common::ObString &origin_db_name)
{
  flashback_table_arg_.origin_db_name_ = origin_db_name;
}

inline void ObFlashBackTableFromRecyclebinStmt::set_origin_table_id(const uint64_t origin_table_id)
{
  flashback_table_arg_.origin_table_id_ = origin_table_id;
}

inline void ObFlashBackTableFromRecyclebinStmt::set_origin_table_name(const common::ObString &origin_table_name)
{
  flashback_table_arg_.origin_table_name_ = origin_table_name;
}

inline void ObFlashBackTableFromRecyclebinStmt::set_new_table_name(const common::ObString &new_table_name)
{
  flashback_table_arg_.new_table_name_ = new_table_name;
}

inline void ObFlashBackTableFromRecyclebinStmt::set_new_db_name(const common::ObString &new_db_name)
{
  flashback_table_arg_.new_db_name_ = new_db_name;
}

class ObFlashBackTableToScnStmt : public ObDDLStmt
{ 
public:
  static const int TIME_INVALID = -1;
  static const int TIME_TIMESTAMP = 0;
  static const int TIME_SCN = 1;
public:
  ObFlashBackTableToScnStmt()
    : ObDDLStmt(stmt::T_FLASHBACK_TABLE_TO_SCN),
    expr_(nullptr),
    time_type_(TIME_INVALID) {}
  virtual ~ObFlashBackTableToScnStmt() {}
  void set_time_expr(ObRawExpr* expr) { expr_  = expr; }
  void set_time_type(int type) { time_type_ = type; }
  void set_tenant_id(const uint64_t tenant_id) { flashback_table_to_scn_arg_.tenant_id_ = tenant_id; }
  void set_query_end_time(const int64_t query_end_time)
  {
    flashback_table_to_scn_arg_.query_end_time_ = query_end_time;
  }
  ObRawExpr* get_time_expr() { return expr_; }
  int get_time_type() { return time_type_; }
  int64_t get_query_end_time() const
  {
    return flashback_table_to_scn_arg_.query_end_time_;
  }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return flashback_table_to_scn_arg_; }
  obrpc::ObFlashBackTableToScnArg flashback_table_to_scn_arg_;
  TO_STRING_KV(K_(stmt_type), K_(flashback_table_to_scn_arg));
private:
  ObRawExpr *expr_;
  int time_type_;
  DISALLOW_COPY_AND_ASSIGN(ObFlashBackTableToScnStmt);
};

/**
 * flashback index
 */
class ObFlashBackIndexStmt : public ObDDLStmt
{
public:
  ObFlashBackIndexStmt() : ObDDLStmt(stmt::T_FLASHBACK_INDEX) {}
  explicit ObFlashBackIndexStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_FLASHBACK_INDEX)
  {}
  virtual ~ObFlashBackIndexStmt() {}
  const obrpc::ObFlashBackIndexArg& get_flashback_index_arg() const { return flashback_index_arg_; }
  inline void set_tenant_id(const uint64_t tenant_id);
  uint64_t get_tenant_id() const { return flashback_index_arg_.tenant_id_; }
  inline void set_origin_table_id(const uint64_t origin_table_id);
  uint64_t get_origin_table_id() const { return flashback_index_arg_.origin_table_id_; }
  void set_origin_table_name(const common::ObString &origin_table_name);
  void set_origin_db_name(const common::ObString origin_db_name);
  void set_new_table_name(const common::ObString &new_table_name);
  void set_new_db_name(const common::ObString &new_db_name);
  virtual obrpc::ObDDLArg &get_ddl_arg() { return flashback_index_arg_; }
  TO_STRING_KV(K_(stmt_type),K_(flashback_index_arg));
private:
  obrpc::ObFlashBackIndexArg flashback_index_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObFlashBackIndexStmt);
};

inline void ObFlashBackIndexStmt::set_tenant_id(const uint64_t tenant_id)
{
  flashback_index_arg_.tenant_id_ = tenant_id;
}
inline void ObFlashBackIndexStmt::set_origin_table_id(const uint64_t origin_table_id)
{
  flashback_index_arg_.origin_table_id_ = origin_table_id;
}

inline void ObFlashBackIndexStmt::set_origin_table_name(const common::ObString &origin_table_name)
{
  flashback_index_arg_.origin_table_name_ = origin_table_name;
}

inline void ObFlashBackIndexStmt::set_new_table_name(const common::ObString &new_table_name)
{
  flashback_index_arg_.new_table_name_ = new_table_name;
}

inline void ObFlashBackIndexStmt::set_new_db_name(const common::ObString &new_db_name)
{
  flashback_index_arg_.new_db_name_ = new_db_name;
}

/**
 * flaskback database
 */

class ObFlashBackDatabaseStmt : public ObDDLStmt
{
public:
  ObFlashBackDatabaseStmt() : ObDDLStmt(stmt::T_FLASHBACK_DATABASE) {}
  explicit ObFlashBackDatabaseStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_FLASHBACK_DATABASE)
  {}
  virtual ~ObFlashBackDatabaseStmt() {}
  const obrpc::ObFlashBackDatabaseArg& get_flashback_database_arg() const { return flashback_db_arg_; }
  inline void set_tenant_id(const uint64_t tenant_id);
  uint64_t get_tenant_id() const { return flashback_db_arg_.tenant_id_; }
  const common::ObString &get_origin_db_name() const { return flashback_db_arg_.origin_db_name_; }
  const common::ObString &get_new_db_name() const { return flashback_db_arg_.new_db_name_; }
  void set_origin_db_name(const common::ObString origin_db_name);
  void set_new_db_name(const common::ObString &new_db_name);
  virtual obrpc::ObDDLArg &get_ddl_arg() { return flashback_db_arg_; }
  TO_STRING_KV(K_(stmt_type),K_(flashback_db_arg));
private:
  obrpc::ObFlashBackDatabaseArg flashback_db_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObFlashBackDatabaseStmt);
};

inline void ObFlashBackDatabaseStmt::set_tenant_id(const uint64_t tenant_id)
{
  flashback_db_arg_.tenant_id_ = tenant_id;
}

inline void ObFlashBackDatabaseStmt::set_origin_db_name(const common::ObString origin_db_name)
{
  flashback_db_arg_.origin_db_name_ = origin_db_name;
}

inline void ObFlashBackDatabaseStmt::set_new_db_name(const common::ObString &new_db_name)
{
  flashback_db_arg_.new_db_name_ = new_db_name;
}



/**
 * flaskback tenant
 */

class ObFlashBackTenantStmt : public ObDDLStmt
{
public:
  ObFlashBackTenantStmt() : ObDDLStmt(stmt::T_FLASHBACK_TENANT) {}
  explicit ObFlashBackTenantStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_FLASHBACK_TENANT)
  {}
  virtual ~ObFlashBackTenantStmt() {}
  const obrpc::ObFlashBackTenantArg& get_flashback_tenant_arg() const { return flashback_tenant_arg_; }
  inline void set_tenant_id(const uint64_t tenant_id);
  virtual obrpc::ObDDLArg &get_ddl_arg() { return flashback_tenant_arg_; }
  uint64_t get_tenant_id() const { return flashback_tenant_arg_.tenant_id_; }
  void set_origin_tenant_name(const common::ObString origin_tenant_name);
  void set_new_tenant_name(const common::ObString &new_tenant_name);
  TO_STRING_KV(K_(stmt_type),K_(flashback_tenant_arg));
private:
  obrpc::ObFlashBackTenantArg flashback_tenant_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObFlashBackTenantStmt);
};

inline void ObFlashBackTenantStmt::set_tenant_id(const uint64_t tenant_id)
{
  flashback_tenant_arg_.tenant_id_ = tenant_id;
}

inline void ObFlashBackTenantStmt::set_origin_tenant_name(const common::ObString origin_tenant_name)
{
  flashback_tenant_arg_.origin_tenant_name_ = origin_tenant_name;
}

inline void ObFlashBackTenantStmt::set_new_tenant_name(const common::ObString &new_tenant_name)
{
  flashback_tenant_arg_.new_tenant_name_ = new_tenant_name;
}


} // namespace sql
} // namespace oceanbase


#endif //OCEANBASE_SQL_OB_RENAME_TABLE_STMT_



