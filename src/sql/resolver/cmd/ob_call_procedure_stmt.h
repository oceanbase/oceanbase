/**
 * (C) Copyright 2014 Alibaba Inc. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version:
 *
 * Date: 2017年1月17日
 *
 * ob_call_procedure_stmt.h is for …
 *
 * Authors:
 *   Author Name <email address>
 *     RuDian<>
 */

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CALL_PROCEDURE_STMT_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CALL_PROCEDURE_STMT_H_

#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "common/object/ob_object.h"
#include "share/schema/ob_table_schema.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/cmd/ob_cmd_stmt.h"
#include "sql/plan_cache/ob_prepare_stmt_struct.h"
namespace oceanbase
{
namespace sql
{
class ObCallProcedureStmt : public ObCMDStmt
{
public:
  explicit ObCallProcedureStmt(common::ObIAllocator *name_pool)
      : ObCMDStmt(name_pool, stmt::T_CALL_PROCEDURE),
        can_direct_use_param_(false),
        package_id_(common::OB_INVALID_ID),
        routine_id_(common::OB_INVALID_ID),
        param_cnt_(0),
        params_(),
        out_idx_(),
        out_mode_(),
        out_name_(),
        out_type_(),
        db_name_(),
        is_udt_routine_(false),
        expr_factory_(NULL) {
  }
  ObCallProcedureStmt()
      : ObCMDStmt(stmt::T_CALL_PROCEDURE),
        can_direct_use_param_(false),
        package_id_(common::OB_INVALID_ID),
        routine_id_(common::OB_INVALID_ID),
        param_cnt_(0),
        params_(),
        out_idx_(),
        out_mode_(),
        out_name_(),
        out_type_(),
        db_name_(),
        is_udt_routine_(false),
        expr_factory_(NULL) {}
  virtual ~ObCallProcedureStmt() {
    if (OB_NOT_NULL(expr_factory_)) {
      expr_factory_->~ObRawExprFactory();
      expr_factory_ = NULL;
    }
  }

  inline uint64_t get_package_id() const { return package_id_; }
  inline void set_package_id(const uint64_t package_id) { package_id_ = package_id; }
  inline uint64_t get_routine_id() const { return routine_id_; }
  inline void set_routine_id(const uint64_t routine_id) { routine_id_ = routine_id; }
  inline const common::ObIArray<sql::ObRawExpr*> &get_params() const { return params_; }
  inline common::ObIArray<sql::ObRawExpr*> &get_params() { return params_; }
  inline int add_param(sql::ObRawExpr* param) { return params_.push_back(param); }
  int add_params(const common::ObIArray<sql::ObRawExpr*>& params);
  inline int64_t get_output_count() { return out_idx_.num_members(); }
  inline bool is_out_param(int64_t i) { return out_idx_.has_member(i); }
  inline ObBitSet<> &get_out_idx() { return out_idx_; }
  inline void set_out_idx(ObBitSet<> &v) { out_idx_ = v; }
  inline ObIArray<int64_t>& get_out_mode() { return out_mode_; }
  inline ObIArray<ObString> &get_out_name() { return out_name_; }
  inline ObIArray<pl::ObPLDataType> &get_out_type() { return out_type_; }
  inline ObIArray<ObString> &get_out_type_name() { return out_type_name_; }
  inline ObIArray<ObString> &get_out_type_owner() { return out_type_owner_; }
  //inline ObNewRow &get_output() { return output_; }
  int add_out_param(int64_t i, int64_t mode, const ObString &name,
                    const pl::ObPLDataType &type,
                    const ObString &out_type_name, const ObString &out_type_owner);

  const ParamTypeInfoArray& get_type_infos() const {
    return in_type_infos_;
  }
  inline void set_db_name(const ObString db_name) { db_name_ = db_name; }
  const ObString& get_db_name() const { return db_name_; }

  //int get_convert_size(int64_t &cv_size) const;
  void set_can_direct_use_param(bool v) { can_direct_use_param_ = v;}
  bool can_direct_use_param() const { return can_direct_use_param_; }
  void set_param_cnt(int64_t v) { param_cnt_ = v; }
  int64_t get_param_cnt() const { return param_cnt_; }

  void set_is_udt_routine(bool v) { is_udt_routine_ = v; }
  bool is_udt_routine() const { return is_udt_routine_; }

  int deep_copy(ObIAllocator *allocator, const ObCallProcedureStmt *other);

  //virtual obrpc::ObDDLArg &get_ddl_arg() { return ddl_arg_; }
  TO_STRING_KV(K_(can_direct_use_param),
               K_(package_id),
               K_(routine_id),
               K_(params),
               K_(out_idx),
               K_(out_name),
               K_(out_type),
               K_(out_type_name),
               K_(out_type_owner),
               K_(is_udt_routine));
private:
  bool can_direct_use_param_;
  uint64_t package_id_;
  uint64_t routine_id_;
  int64_t param_cnt_;
  common::ObArray<sql::ObRawExpr*> params_;
  ObBitSet<> out_idx_;
  ObSEArray<int64_t, 32> out_mode_;
  ObSEArray<ObString, 32> out_name_;
  ObSEArray<pl::ObPLDataType, 32> out_type_;
  ObSEArray<ObString, 32> out_type_name_;
  ObSEArray<ObString, 32> out_type_owner_;
  ParamTypeInfoArray in_type_infos_;
  ObString db_name_;
  bool is_udt_routine_;
  ObRawExprFactory *expr_factory_;

  DISALLOW_COPY_AND_ASSIGN(ObCallProcedureStmt);
};

}//namespace sql
}//namespace oceanbase



#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CALL_PROCEDURE_STMT_H_ */
