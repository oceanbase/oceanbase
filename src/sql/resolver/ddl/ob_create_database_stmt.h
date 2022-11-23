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

#ifndef OCEANBASE_SQL_OB_CREATE_DATABASE_STMT_H_
#define OCEANBASE_SQL_OB_CREATE_DATABASE_STMT_H_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
namespace oceanbase
{
namespace sql
{
class ObCreateDatabaseStmt : public ObDDLStmt
  {
  public:
    ObCreateDatabaseStmt();
    explicit ObCreateDatabaseStmt(common::ObIAllocator *name_pool);
    virtual ~ObCreateDatabaseStmt();
    void set_if_not_exists(bool if_not_exists);
    void set_tenant_id(const uint64_t tenant_id);
    void set_database_id(const uint64_t database_id);
    int set_database_name(const common::ObString &database_name);
    void set_collation_type(const common::ObCollationType type);
    void set_charset_type(const common::ObCharsetType type);
    void set_in_recyclebin(bool is_in_recyclebin);
    common::ObCharsetType get_charset_type() const;
    common::ObCollationType get_collation_type() const;
    const common::ObString &get_database_name() const;
    void add_zone(const common::ObString &zone);
    int set_primary_zone(const common::ObString &zone);
    void set_read_only(const bool read_only);
    int set_default_tablegroup_name(const common::ObString &tablegroup_name);
    obrpc::ObCreateDatabaseArg &get_create_database_arg();
    virtual bool cause_implicit_commit() const { return true; }
    virtual obrpc::ObDDLArg &get_ddl_arg() { return create_database_arg_; }

    TO_STRING_KV(K_(create_database_arg));
  private:
    DISABLE_WARNING_GCC_PUSH
    DISABLE_WARNING_GCC_ATTRIBUTES
    bool is_charset_specify_ __maybe_unused;
    bool is_collation_specify_ __maybe_unused;
    DISABLE_WARNING_GCC_POP
    obrpc::ObCreateDatabaseArg create_database_arg_;
  };
}//namespace sql
}//namespace oceanbase
#endif //OCEANBASE_SQL_OB_CREATE_DATABASE_STMT_H_
