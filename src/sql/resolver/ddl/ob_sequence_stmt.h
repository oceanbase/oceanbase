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

#ifndef OCEANBASE_SQL_OB_CREATE_SEQUENCE_STMT_H_
#define OCEANBASE_SQL_OB_CREATE_SEQUENCE_STMT_H_

#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/resolver/ob_stmt_resolver.h"
#include "sql/resolver/ddl/ob_sequence_resolver.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObSequenceDDLStmt : public ObDDLStmt
{
public:
  explicit ObSequenceDDLStmt(common::ObIAllocator *name_pool, stmt::StmtType type) :
      ObDDLStmt(name_pool, type),
      arg_()
  {
    arg_.set_stmt_type(type);
  }
  ObSequenceDDLStmt(stmt::StmtType type) :
      ObDDLStmt(type),
      arg_()
  {
    arg_.set_stmt_type(type);
  }
  virtual ~ObSequenceDDLStmt() = default;
  virtual void print(FILE *fp, int32_t level, int32_t index = 0)
  {
    UNUSED(index);
    UNUSED(fp);
    UNUSED(level);
  }
  void set_sequence_id(const uint64_t &sequence_id)
  {
    arg_.set_sequence_id(sequence_id);
  }
  void set_sequence_name(const common::ObString &sequence_name)
  {
    arg_.set_sequence_name(sequence_name);
  }
  void set_database_name(const common::ObString &db_name)
  {
    arg_.set_database_name(db_name);
  }
  void set_tenant_id(uint64_t tenant_id)
  {
    arg_.set_tenant_id(tenant_id);
  }
  void set_is_system_generated()
  {
    arg_.set_is_system_generated();
  }
  void set_ignore_exists_error(bool ignore_error) {
    arg_.set_ignore_exists_error(ignore_error);
  }
  share::ObSequenceOption &option() { return arg_.option(); }
  virtual obrpc::ObDDLArg &get_ddl_arg() { return arg_; }
  obrpc::ObSequenceDDLArg &get_arg() { return arg_; }
private:
  obrpc::ObSequenceDDLArg arg_;
  DISALLOW_COPY_AND_ASSIGN(ObSequenceDDLStmt);
};

class ObCreateSequenceStmt : public ObSequenceDDLStmt
{
public:
  explicit ObCreateSequenceStmt(common::ObIAllocator *name_pool) :
      ObSequenceDDLStmt(name_pool, stmt::T_CREATE_SEQUENCE)
  {
  }
  ObCreateSequenceStmt() :
      ObSequenceDDLStmt(stmt::T_CREATE_SEQUENCE)
  {
  }
  virtual ~ObCreateSequenceStmt() = default;
};

class ObAlterSequenceStmt : public ObSequenceDDLStmt
{
public:
  explicit ObAlterSequenceStmt(common::ObIAllocator *name_pool) :
      ObSequenceDDLStmt(name_pool, stmt::T_ALTER_SEQUENCE)
  {
  }
  ObAlterSequenceStmt() :
      ObSequenceDDLStmt(stmt::T_ALTER_SEQUENCE)
  {
  }
  virtual ~ObAlterSequenceStmt() = default;
};

class ObDropSequenceStmt : public ObSequenceDDLStmt
{
public:
  explicit ObDropSequenceStmt(common::ObIAllocator *name_pool) :
      ObSequenceDDLStmt(name_pool, stmt::T_DROP_SEQUENCE)
  {
  }
  ObDropSequenceStmt() :
      ObSequenceDDLStmt(stmt::T_DROP_SEQUENCE)
  {
  }
  virtual ~ObDropSequenceStmt() = default;
};

class ObColumnSequenceStmt : public ObSequenceDDLStmt
{
public:
  explicit ObColumnSequenceStmt(common::ObIAllocator *name_pool) :
      ObSequenceDDLStmt(name_pool, stmt::T_CREATE_SEQUENCE)
  {
  }
  ObColumnSequenceStmt() :
      ObSequenceDDLStmt(stmt::T_CREATE_SEQUENCE)
  {
  }
  virtual ~ObColumnSequenceStmt() = default;
};



} /* sql */
} /* oceanbase */
#endif //OCEANBASE_SQL_OB_CREATE_SEQUENCE_STMT_H_
