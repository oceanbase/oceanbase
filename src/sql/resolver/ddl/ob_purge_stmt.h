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

#ifndef OCEANBASE_SQL_OB_PURGE_STMT_
#define OCEANBASE_SQL_OB_PURGE_STMT_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace sql
{
/**
 * Purge table
 */

class ObPurgeTableStmt : public ObDDLStmt
{
public:
  ObPurgeTableStmt() : ObDDLStmt(stmt::T_PURGE_TABLE) {}
  explicit ObPurgeTableStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_PURGE_TABLE)
  {}
  virtual ~ObPurgeTableStmt() {}
  const obrpc::ObPurgeTableArg& get_purge_table_arg() const { return purge_table_arg_; }
  inline void set_tenant_id(const uint64_t tenant_id);
  inline uint64_t get_tenant_id() const { return purge_table_arg_.tenant_id_; }
  inline void set_database_id(const uint64_t database_id);
  inline uint64_t get_database_id() const { return purge_table_arg_.database_id_; }
  inline void set_table_name(const common::ObString &table_name);
  inline const common::ObString& get_table_name() const { return purge_table_arg_.table_name_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return purge_table_arg_; }
  TO_STRING_KV(K_(stmt_type),K_(purge_table_arg));
private:
  obrpc::ObPurgeTableArg purge_table_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObPurgeTableStmt);
};

inline void ObPurgeTableStmt::set_tenant_id(const uint64_t tenant_id)
{
  purge_table_arg_.tenant_id_ = tenant_id;
}

inline void ObPurgeTableStmt::set_database_id(const uint64_t database_id)
{
  purge_table_arg_.database_id_ = database_id;
}

inline void ObPurgeTableStmt::set_table_name(const common::ObString &table_name)
{
  purge_table_arg_.table_name_ = table_name;
}

/**
 * Purge index
 */

class ObPurgeIndexStmt : public ObDDLStmt
{
public:
  ObPurgeIndexStmt() : ObDDLStmt(stmt::T_PURGE_INDEX) {}
  explicit ObPurgeIndexStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_PURGE_INDEX)
  {}
  virtual ~ObPurgeIndexStmt() {}
  const obrpc::ObPurgeIndexArg& get_purge_index_arg() const { return purge_index_arg_; }
  inline void set_tenant_id(const uint64_t tenant_id);
  inline uint64_t get_tenant_id() const { return purge_index_arg_.tenant_id_; }
  inline void set_database_id(const uint64_t database_id_);
  inline uint64_t get_database_id() const { return purge_index_arg_.database_id_; }
  inline void set_table_id(const uint64_t table_id);
  inline uint64_t get_table_id() const { return purge_index_arg_.table_id_; }
  inline void set_table_name(const common::ObString &table_name);
  inline const common::ObString& get_table_name() const { return purge_index_arg_.table_name_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return purge_index_arg_; }
  TO_STRING_KV(K_(stmt_type),K_(purge_index_arg));
private:
  obrpc::ObPurgeIndexArg purge_index_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObPurgeIndexStmt);
};

inline void ObPurgeIndexStmt::set_tenant_id(const uint64_t tenant_id)
{
  purge_index_arg_.tenant_id_ = tenant_id;
}

inline void ObPurgeIndexStmt::set_database_id(const uint64_t database_id)
{
  purge_index_arg_.database_id_ = database_id;
}

inline void ObPurgeIndexStmt::set_table_id(const uint64_t table_id)
{
  purge_index_arg_.table_id_ = table_id;
}

inline void ObPurgeIndexStmt::set_table_name(const common::ObString &table_name)
{
  purge_index_arg_.table_name_ = table_name;
}

/**
 * flaskback database
 */

class ObPurgeDatabaseStmt : public ObDDLStmt
{
public:
  ObPurgeDatabaseStmt() : ObDDLStmt(stmt::T_PURGE_DATABASE) {}
  explicit ObPurgeDatabaseStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_PURGE_DATABASE)
  {}
  virtual ~ObPurgeDatabaseStmt() {}
  const obrpc::ObPurgeDatabaseArg& get_purge_database_arg() const { return purge_db_arg_; }
  inline void set_tenant_id(const uint64_t tenant_id);
  uint64_t get_tenant_id() const { return purge_db_arg_.tenant_id_; }
  void set_db_name(const common::ObString &db_name);
  virtual obrpc::ObDDLArg &get_ddl_arg() { return purge_db_arg_; }
  TO_STRING_KV(K_(stmt_type),K_(purge_db_arg));
private:
  obrpc::ObPurgeDatabaseArg purge_db_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObPurgeDatabaseStmt);
};

inline void ObPurgeDatabaseStmt::set_tenant_id(const uint64_t tenant_id)
{
  purge_db_arg_.tenant_id_ = tenant_id;
}

inline void ObPurgeDatabaseStmt::set_db_name(const common::ObString &db_name)
{
  purge_db_arg_.db_name_ = db_name;
}



/**
 * purge tenant
 */

class ObPurgeTenantStmt : public ObDDLStmt
{
public:
  ObPurgeTenantStmt() : ObDDLStmt(stmt::T_PURGE_TENANT) {}
  explicit ObPurgeTenantStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_PURGE_TENANT)
  {}
  virtual ~ObPurgeTenantStmt() {}
  const obrpc::ObPurgeTenantArg& get_purge_tenant_arg() const { return purge_tenant_arg_; }
  inline void set_tenant_id(const uint64_t tenant_id);
  uint64_t get_tenant_id() const { return purge_tenant_arg_.tenant_id_; }
  void set_tenant_name(const common::ObString &tenant_name);
  virtual obrpc::ObDDLArg &get_ddl_arg() { return purge_tenant_arg_; }
  TO_STRING_KV(K_(stmt_type),K_(purge_tenant_arg));
private:
  obrpc::ObPurgeTenantArg purge_tenant_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObPurgeTenantStmt);
};

inline void ObPurgeTenantStmt::set_tenant_id(const uint64_t tenant_id)
{
  purge_tenant_arg_.tenant_id_ = tenant_id;
}

inline void ObPurgeTenantStmt::set_tenant_name(const common::ObString &tenant_name)
{
  purge_tenant_arg_.tenant_name_ = tenant_name;
}

/**
 * purge recyclebin
 */
class ObPurgeRecycleBinStmt : public ObDDLStmt
{
public:
  ObPurgeRecycleBinStmt() : ObDDLStmt(stmt::T_PURGE_RECYCLEBIN) {}
  explicit ObPurgeRecycleBinStmt(common::ObIAllocator *name_pool)
    : ObDDLStmt(name_pool, stmt::T_PURGE_RECYCLEBIN)
  {}
  virtual ~ObPurgeRecycleBinStmt() {}
  const obrpc::ObPurgeRecycleBinArg& get_purge_recyclebin_arg() const { return purge_recyclebin_arg_; }
  inline void set_tenant_id(const uint64_t tenant_id) { purge_recyclebin_arg_.tenant_id_ = tenant_id; }
  inline void set_expire_time(const int64_t expire_time) { purge_recyclebin_arg_.expire_time_ = expire_time; }
  inline void set_purge_num(const int64_t purge_num) { purge_recyclebin_arg_.purge_num_ = purge_num; }
  uint64_t get_tenant_id() const { return purge_recyclebin_arg_.tenant_id_; }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return purge_recyclebin_arg_; }
  TO_STRING_KV(K_(stmt_type), K_(purge_recyclebin_arg));
private:
  obrpc::ObPurgeRecycleBinArg purge_recyclebin_arg_;
  DISALLOW_COPY_AND_ASSIGN(ObPurgeRecycleBinStmt);
};

} // namespace sql
} // namespace oceanbase


#endif //OCEANBASE_SQL_OB_RENAME_TABLE_STMT_




