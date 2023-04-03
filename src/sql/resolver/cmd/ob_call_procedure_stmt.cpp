/**
 * (C) Copyright 2014 Alibaba Inc. All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version:
 *
 * Date: 2018年2月28日
 *
 * ob_call_procedure_stmt.cpp is for …
 *
 * Authors:
 *   Author Name <email address>
 *     RuDian<>
 */

#define USING_LOG_PREFIX SQL_RESV

#include "ob_call_procedure_stmt.h"
#include "pl/ob_pl_type.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

namespace oceanbase
{
namespace sql
{

int ObCallProcedureStmt::deep_copy(ObIAllocator *allocator,
                                   const ObCallProcedureStmt *other)
{
  int ret = OB_SUCCESS;
  if (this != other) {
    CK (OB_NOT_NULL(other));
    CK (OB_NOT_NULL(allocator));
    OX (can_direct_use_param_ = other->can_direct_use_param_);
    OX (package_id_ = other->package_id_);
    OX (routine_id_ = other->routine_id_);
    OX (param_cnt_ = other->param_cnt_);
    OX (out_idx_ = other->out_idx_);
    CK (other->out_name_.count() == other->out_type_.count());
    CK (other->out_name_.count() == other->out_mode_.count());
    CK (other->out_name_.count() == other->out_type_name_.count());
    CK (other->out_name_.count() == other->out_type_owner_.count());
    for (int64_t i = 0; OB_SUCC(ret) && i < other->out_name_.count(); ++i) {
      ObString copy;
      pl::ObPLDataType pl_copy_type;
      OZ (ob_write_string(*allocator, other->out_name_.at(i), copy));
      OZ (out_name_.push_back(copy));
      OZ (ob_write_string(*allocator, other->out_type_name_.at(i), copy));
      OZ (out_type_name_.push_back(copy));
      OZ (ob_write_string(*allocator, other->out_type_owner_.at(i), copy));
      OZ (out_type_owner_.push_back(copy));
      OZ (pl_copy_type.deep_copy(*allocator, other->out_type_.at(i)));
      OZ (out_type_.push_back(pl_copy_type));
      OZ (out_mode_.push_back(other->out_mode_.at(i)));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < other->in_type_infos_.count(); ++i) {
      TypeInfo copy;
      OZ (copy.deep_copy(allocator, &(other->in_type_infos_.at(i))));
      OZ (in_type_infos_.push_back(copy));
    }
    OZ (ob_write_string(*allocator, other->db_name_, db_name_));
  }
  return ret;
}

int ObCallProcedureStmt::add_out_param(
  int64_t i, int64_t mode, const ObString &name,
  const pl::ObPLDataType &type,
  const ObString &out_type_name, const ObString &out_type_owner)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(out_idx_.add_member(i))) {
    LOG_WARN("failed to add out index", K(i), K(name), K(type), K(ret));
  } else if (OB_FAIL(out_mode_.push_back(mode))) {
    LOG_WARN("failed to push mode", K(ret));
  } else if (OB_FAIL(out_name_.push_back(name))) {
    LOG_WARN("push back error", K(i), K(name), K(type), K(ret));
  } else if (OB_FAIL(out_type_.push_back(type))) {
    LOG_WARN("push back error", K(i), K(name), K(type), K(ret));
  } else if (OB_FAIL(out_type_name_.push_back(out_type_name))) {
    LOG_WARN("push back error", K(i), K(name), K(type), K(out_type_name), K(ret));
  } else if (OB_FAIL(out_type_owner_.push_back(out_type_owner))) {
    LOG_WARN("push back error", K(i), K(name), K(ret), K(out_type_name), K(out_type_owner), K(ret));
  } else { /*do nothing*/ }
  return ret;
}

int ObCallProcedureStmt::add_params(const common::ObIArray<sql::ObRawExpr*>& params)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < params.count(); ++i) {
    if (OB_ISNULL(params.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("param at i is null", K(ret), K(i));
    } else if (OB_FAIL(params_.push_back(params.at(i)))) {
      LOG_WARN("failed to push back", K(ret), K(i));
    } else {
      param_cnt_++;
    }
  }
  return ret;
}

}
}


