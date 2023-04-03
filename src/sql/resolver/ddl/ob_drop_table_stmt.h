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

#ifndef OCEANBASE_SQL_RESOLVER_DDL_DROP_TABLE_STMT_
#define OCEANBASE_SQL_RESOLVER_DDL_DROP_TABLE_STMT_

#include "share/ob_rpc_struct.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObDropTableStmt : public ObDDLStmt
{
public:
  explicit ObDropTableStmt(common::ObIAllocator *name_pool);
  ObDropTableStmt();
  virtual ~ObDropTableStmt();

  const obrpc::ObDropTableArg &get_drop_table_arg() const { return drop_table_arg_; }
  obrpc::ObDropTableArg &get_drop_table_arg() { return drop_table_arg_; }
  virtual bool cause_implicit_commit() const {
    //return share::schema::TMP_TABLE != drop_table_arg_.table_type_;
    /*
     * Can not handle situation when dropping PTT without committing
     * current transaction implicitly by the current session.
     * 
     * Example:
     * 
     * create temporary table ptt1(c1 int);
     * create table t1(c1 int);
     *
     * begin Tx
     * insert into ptt1 values(1);
     * insert into t1 values(1);
     * drop temporary table ptt1;
     * commit;
     *
     * In this case, while replaying log Tx in slave, ptt1 has already been dropped.
     *
     */
    return true;
  }
  int add_table_item(const obrpc::ObTableItem &table_item);
  virtual obrpc::ObDDLArg &get_ddl_arg() { return drop_table_arg_; }
  bool is_view_stmt() const { return is_view_stmt_; }
  void set_is_view_stmt(const bool is_view_stmt) { is_view_stmt_ = is_view_stmt; }

  TO_STRING_KV(K_(stmt_type),K_(drop_table_arg));
private:
  obrpc::ObDropTableArg drop_table_arg_;
  bool is_view_stmt_;
  DISALLOW_COPY_AND_ASSIGN(ObDropTableStmt);
};

}
}

#endif //OCEANBASE_SQL_RESOLVER_DDL_DROP_TABLE_STMT_
