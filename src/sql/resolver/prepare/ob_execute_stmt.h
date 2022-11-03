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

#ifndef OCEANBASE_SQL_RESOLVER_PREPARE_EXECUTE_STMT_
#define OCEANBASE_SQL_RESOLVER_PREPARE_EXECUTE_STMT_

#include "lib/container/ob_array.h"
#include "sql/resolver/cmd/ob_cmd_stmt.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace sql
{
class ObExecuteStmt : public ObCMDStmt
{
public:
  ObExecuteStmt() : ObCMDStmt(stmt::T_EXECUTE), prepare_id_(common::OB_INVALID_ID), prepare_type_(stmt::T_NONE), params_() {}
  virtual ~ObExecuteStmt() {}

  inline ObPsStmtId get_prepare_id() const { return prepare_id_; }
  inline void set_prepare_id(ObPsStmtId id) { prepare_id_ = id; }
  inline stmt::StmtType get_prepare_type() const { return prepare_type_; }
  inline void set_prepare_type(stmt::StmtType type) { prepare_type_ = type; }
  inline const common::ObIArray<const sql::ObRawExpr*> &get_params() const { return params_; }
  inline int set_params(common::ObIArray<const sql::ObRawExpr*> &params) { return append(params_, params); }
  inline int add_param(const sql::ObRawExpr* param) { return params_.push_back(param); }

  TO_STRING_KV(N_SQL_ID, prepare_id_, N_STMT_TYPE, prepare_type_, N_PARAM, params_);
private:
  ObPsStmtId prepare_id_;
  stmt::StmtType prepare_type_;
  common::ObArray<const sql::ObRawExpr*> params_;
  DISALLOW_COPY_AND_ASSIGN(ObExecuteStmt);
};

}//end of sql
}//end of oceanbase

#endif //OCEANBASE_SQL_RESOLVER_PREPARE_EXECUTE_STMT_
