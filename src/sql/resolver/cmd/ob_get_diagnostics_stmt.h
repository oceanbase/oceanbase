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

#ifndef OCEANBASE_SQL_RESOLVER_CMD_GET_DIAGNOSTICS_STMT_
#define OCEANBASE_SQL_RESOLVER_CMD_GET_DIAGNOSTICS_STMT_

#include <utility>
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
#include "sql/resolver/ddl/ob_ddl_stmt.h"
#include "share/system_variable/ob_system_variable.h"
#include "sql/resolver/cmd/ob_variable_set_stmt.h"

namespace oceanbase
{
namespace sql
{

enum DIAG_INFO_TYPE {
  MYSQL_ERRNO_TYPE = 0,
  MESSAGE_TEXT_TYPE,
  RETURNED_SQLSTATE_TYPE,
  CLASS_ORIGIN_TYPE,
  SUBCLASS_ORIGIN_TYPE,
  TABLE_NAME_TYPE,
  COLUMN_NAME_TYPE,
  CONSTRAINT_CATALOG_TYPE,
  CONSTRAINT_SCHEMA_TYPE,
  CONSTRAINT_NAME_TYPE,
  CATALOG_NAME_TYPE,
  SCHEMA_NAME_TYPE,
  CURSOR_NAME_TYPE,
  NUMBER_TYPE,
  ROW_COUNT_TYPE,
};

class ObGetDiagnosticsStmt : public ObDDLStmt
{
public:
    ObGetDiagnosticsStmt() : ObDDLStmt(stmt::T_DIAGNOSTICS), 
                             type_(DiagnosticsType::DIAGNOSTICS_UNINITIALIZED),
                             params_(),
                             info_argument_(),
                             orgin_param_index_() {}
    ObGetDiagnosticsStmt(DiagnosticsType type) : 
        ObDDLStmt(stmt::T_DIAGNOSTICS), type_(type), params_(), info_argument_() {}
    void set_diagnostics_type(DiagnosticsType type) { type_ = type; }
    DiagnosticsType get_diagnostics_type() const { return type_; }
    int get_diag_info_type_by_name(const ObString &val, DIAG_INFO_TYPE &type);
    inline const common::ObIArray<sql::ObRawExpr*> &get_params() const { return params_; }
    inline common::ObIArray<sql::ObRawExpr*> &get_params() { return params_; }
    inline int add_param(sql::ObRawExpr* param) { return params_.push_back(param); }
    inline int add_info_argument(ObString arg) {return info_argument_.push_back(arg); }
    inline const common::ObIArray<ObString> &get_info_argument() const { return info_argument_; }
    inline int add_origin_param_index(int64_t idx) {return orgin_param_index_.push_back(idx); }
    inline const common::ObIArray<int64_t> &get_origin_param_indexs() const { return orgin_param_index_; }
    inline int64_t get_origin_param_index(int64_t idx) const
    {
      return idx < 0 || idx >= orgin_param_index_.count() ? OB_INVALID_INDEX : orgin_param_index_[idx];
    }
    inline void set_invalid_condition_name(ObString invalid_condition_name)
    {
      invalid_condition_name_ = invalid_condition_name;
    }
    inline ObString get_invalid_condition_name() const { return invalid_condition_name_; }

    virtual int get_cmd_type() const { return get_stmt_type(); }
    virtual ~ObGetDiagnosticsStmt() {}
    virtual bool cause_implicit_commit() const {
      return false;
    }
    virtual obrpc::ObDDLArg &get_ddl_arg() { return ddl_arg_; }
    TO_STRING_KV(K_(type), K_(params), K_(info_argument), K_(orgin_param_index));

private:
    DiagnosticsType type_;

    ObSEArray<sql::ObRawExpr*, 8> params_;
    ObSEArray<ObString, 8> info_argument_;
    ObSEArray<int64_t, 8> orgin_param_index_;
    ObString invalid_condition_name_;

    obrpc::ObDDLArg ddl_arg_; // return exec_tenant_id_
    DISALLOW_COPY_AND_ASSIGN(ObGetDiagnosticsStmt);
};
}//end of namespace sql
}//end of namespace oceanbase

#endif //OCEANBASE_SQL_RESOLVER_CMD_GET_DIAGNOSTICS_STMT_
