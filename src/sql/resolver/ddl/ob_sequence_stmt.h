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

namespace oceanbase {
namespace sql {

class ObSequenceDDLStmt : public ObDDLStmt {
public:
  explicit ObSequenceDDLStmt(common::ObIAllocator* name_pool, stmt::StmtType type) : ObDDLStmt(name_pool, type), arg_(), rpc_flag_(false)
  {
    arg_.set_stmt_type(type);
  }
  ObSequenceDDLStmt(stmt::StmtType type) : ObDDLStmt(type), arg_(), rpc_flag_(false)
  {
    arg_.set_stmt_type(type);
  }
  virtual ~ObSequenceDDLStmt() = default;
  virtual void print(FILE* fp, int32_t level, int32_t index = 0)
  {
    UNUSED(index);
    UNUSED(fp);
    UNUSED(level);
  }
  void set_rpc_flag(bool flag)
  {
    rpc_flag_ = flag;
  }
  bool get_rpc_flag()
  {
    return rpc_flag_;
  }
  virtual obrpc::ObDDLArg& get_ddl_arg()
  {
    return arg_;
  }
  obrpc::ObSequenceDDLArg& get_arg()
  {
    return arg_;
  }
  
private:
  obrpc::ObSequenceDDLArg arg_;
  // this flag means whether send a rpc request to rs
  // for creating sequence:
  // this flag will be false if the sequence exists and if_not_exist is used in ddl statement
  // this flag will be true if the sequence doesn't exist
  // in addition, if if_not_exist isn't used and the sequence exists, the ddl is invalid and should be detected in resolver
  // for altering sequence or dropping sequence:
  // this flag will be false if the sequence doesn't exists and if_exists is used in ddl statement
  // this flag will be true if the sequence exists
  // in addition, if if_exists isn't used and the sequence doesn't exist, the ddl is invalid and should be detected in resolver
  bool rpc_flag_;  
  DISALLOW_COPY_AND_ASSIGN(ObSequenceDDLStmt);
};

class ObCreateSequenceStmt : public ObSequenceDDLStmt {
public:
  explicit ObCreateSequenceStmt(common::ObIAllocator* name_pool) : ObSequenceDDLStmt(name_pool, stmt::T_CREATE_SEQUENCE)
  {}
  ObCreateSequenceStmt() : ObSequenceDDLStmt(stmt::T_CREATE_SEQUENCE)
  {}
  virtual ~ObCreateSequenceStmt() = default;
};

class ObAlterSequenceStmt : public ObSequenceDDLStmt {
public:
  explicit ObAlterSequenceStmt(common::ObIAllocator* name_pool) : ObSequenceDDLStmt(name_pool, stmt::T_ALTER_SEQUENCE)
  {}
  ObAlterSequenceStmt() : ObSequenceDDLStmt(stmt::T_ALTER_SEQUENCE)
  {}
  virtual ~ObAlterSequenceStmt() = default;
};

class ObDropSequenceStmt : public ObSequenceDDLStmt {
public:
  explicit ObDropSequenceStmt(common::ObIAllocator* name_pool) : ObSequenceDDLStmt(name_pool, stmt::T_DROP_SEQUENCE)
  {}
  ObDropSequenceStmt() : ObSequenceDDLStmt(stmt::T_DROP_SEQUENCE)
  {}
  virtual ~ObDropSequenceStmt() = default;
};

}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SQL_OB_CREATE_SEQUENCE_STMT_H_
