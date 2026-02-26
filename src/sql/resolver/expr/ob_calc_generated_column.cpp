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

#define USING_LOG_PREFIX SQL_DAS
#include "sql/resolver/expr/ob_calc_generated_column.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "lib/charset/ob_charset.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace sql
{

int ObCalcGeneratedColumn::init()
{
  int ret = OB_SUCCESS;

  void *buf = long_life_alloc_.alloc((sizeof(ObSQLSessionInfo)));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("default session buf allocated failed", K(ret));
  } else if (FALSE_IT(inner_session_ = new(buf) ObSQLSessionInfo())) {
  } else if (OB_FAIL(inner_session_->init(1, 1, &long_life_alloc_, &tz_info_map_))) {
    LOG_WARN("init session failed", K(ret));
  } else if (OB_FAIL(init_session_info())) {
    LOG_WARN("fail to init session info", K(ret));
  } else {
    ObMemAttr memattr(inner_session_->get_effective_tenant_id(), "virGeneratedCol", ObCtxIds::EXECUTE_CTX_ID);
    inner_alloc_.set_attr(memattr);
  }
  return ret;
}

int ObCalcGeneratedColumn::init_session_info()
{
  int ret = OB_SUCCESS;
  if (NULL == inner_session_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to init session info, not pointer", K(ret));
  } else {
    // called in init(), can not check inited_ flag.
    const bool print_info_log = false;
    const bool is_sys_tenant = true;
    ObPCMemPctConf pc_mem_conf;
    inner_session_->set_inner_session();
    ObObj mysql_mode;
    mysql_mode.set_int(0);
    ObObj mysql_sql_mode;
    mysql_sql_mode.set_uint(ObUInt64Type, DEFAULT_MYSQL_MODE);
    if (OB_FAIL(inner_session_->load_default_sys_variable(print_info_log, is_sys_tenant))) {
      LOG_WARN("session load default system variable failed", K(ret));
    } else if (OB_FAIL(inner_session_->update_max_packet_size())) {
      LOG_WARN("fail to update max packet size", K(ret));
    } else if (OB_FAIL(inner_session_->init_tenant(OB_SYS_TENANT_NAME, OB_SYS_TENANT_ID))) {
      LOG_WARN("fail to init tenant", K(ret));
    } else if (OB_FAIL(inner_session_->set_user(OB_SYS_USER_NAME, OB_SYS_HOST_NAME, OB_SYS_USER_ID))) {
      LOG_WARN("Set sys user in session error", K(ret));
    } else {
      inner_session_->set_user_priv_set(OB_PRIV_ALL | OB_PRIV_GRANT);
      inner_session_->set_database_id(OB_SYS_DATABASE_ID);
      inner_session_->set_shadow(true); // inner session will not be show
      inner_session_->set_real_inner_session(true);
    }

    if (OB_FAIL(ret)) {

    } else if (OB_FAIL(inner_session_->update_sys_variable(SYS_VAR_SQL_MODE, mysql_sql_mode))) {
      LOG_WARN("update sys variables failed", K(ret));
    } else if (OB_FAIL(inner_session_->update_sys_variable(SYS_VAR_OB_COMPATIBILITY_MODE, mysql_mode))) {
      LOG_WARN("update sys variables failed", K(ret));
    } else if (OB_FAIL(inner_session_->update_sys_variable(
        SYS_VAR_NLS_DATE_FORMAT, ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT))) {
      LOG_WARN("update sys variables failed", K(ret));
    } else if (OB_FAIL(inner_session_->update_sys_variable(
        SYS_VAR_NLS_TIMESTAMP_FORMAT, ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT))) {
      LOG_WARN("update sys variables failed", K(ret));
    } else if (OB_FAIL(inner_session_->update_sys_variable(
        SYS_VAR_NLS_TIMESTAMP_TZ_FORMAT, ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT))) {
      LOG_WARN("update sys variables failed", K(ret));
    } else {
      ObString database_name(OB_SYS_DATABASE_NAME);
      if (OB_FAIL(inner_session_->set_default_database(database_name))) {
        LOG_WARN("fail to set default database", K(ret), K(database_name));
      } else if (OB_FAIL(inner_session_->get_pc_mem_conf(pc_mem_conf))) {
        LOG_WARN("fail to get pc mem conf", K(ret));
      } else {
        inner_session_->set_database_id(OB_SYS_DATABASE_ID);
        //TODO timezone这些信息都使用默认的
        inner_session_->reset_timezone();
      }
    }
  }
  return ret;
}

int ObCalcGeneratedColumn::found_column_ref_expr(const ObString &column_name, bool &founded, ObColumnRefRawExpr *&ref_expr)
{
  int ret = OB_SUCCESS;
  ref_expr = nullptr;
  for (int i = 0; OB_SUCC(ret) && i < dependcy_column_exprs_.count(); i++) {
    if (OB_ISNULL(dependcy_column_exprs_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(column_name), K(i));
    } else if (ObCharset::case_sensitive_equal(column_name, dependcy_column_exprs_.at(i)->get_column_name())) {
      founded = true;
      ref_expr = dependcy_column_exprs_.at(i);
    }
  }
  return ret;
}

int ObCalcGeneratedColumn::found_dependcy_column_info(const ObCalcGeneratedColumnInfo &generated_column_info,
                                                           const ObString &column_name,
                                                           bool &founded,
                                                           ObCalcGeneratedColumnInfo *&dependcy_column_info)
{
  int ret = OB_SUCCESS;
  founded = false;
  int64_t dependcy_dolumn_cnt = generated_column_info.dependcy_column_infos_.count();
  dependcy_column_info = NULL;
  for (int i = 0; OB_SUCC(ret) && !founded && i < generated_column_info.dependcy_column_infos_.count(); i++) {
    ObCalcGeneratedColumnInfo *column_info = generated_column_info.dependcy_column_infos_.at(i);
    if (ObCharset::case_sensitive_equal(column_name, column_info->col_name_)) {
      founded = true;
      dependcy_column_info = column_info;
    } else {
      LOG_TRACE("get one column_name", K(i), K(column_name), K(column_info->col_name_));
    }
  }
  return ret;
}

uint32_t ObCalcGeneratedColumn::calc_column_result_flag(const ObCalcGeneratedColumnInfo &generated_column_info)
{
  uint32_t flag = 0;
  if (generated_column_info.is_autoincrement_) {
    flag |= AUTO_INCREMENT_FLAG;
  }

  if (generated_column_info.is_not_null_for_read_) {
    flag |= NOT_NULL_FLAG;
  }
  if (generated_column_info.is_not_null_for_write_) {
    flag |= NOT_NULL_WRITE_FLAG;
  }
  // only for create table as select c1, new column is not null only if not null constraint exists on c1
  if (generated_column_info.is_not_null_validate_column_) {
    flag |= HAS_NOT_NULL_VALIDATE_CONSTRAINT_FLAG;
  }
  if (generated_column_info.is_rowkey_column_) {
    flag |= PRI_KEY_FLAG;
    flag |= PART_KEY_FLAG;
  }
  if (generated_column_info.is_index_column_) {
    flag |= PART_KEY_FLAG;
    if (generated_column_info.index_position_ == 1) {
      flag |= MULTIPLE_KEY_FLAG;  /* 和UNIQUE_FLAG相对的概念 */
    }
  }
  if (generated_column_info.is_zero_fill_) {
    flag |= ZEROFILL_FLAG;
  }
  return flag;
}

int ObCalcGeneratedColumn::add_local_session_vars(ObIAllocator *alloc, const ObLocalSessionVar &local_session_var, int64_t &idx) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(all_local_session_vars_.push_back(ObLocalSessionVar()))) {
    LOG_WARN("push back local session var failed", K(ret));
  } else {
    idx = all_local_session_vars_.count() - 1;
    ObLocalSessionVar &local_var = all_local_session_vars_.at(idx);
    local_var.set_allocator(alloc);
    if (OB_FAIL(local_var.deep_copy(local_session_var))) {
      LOG_WARN("deep copy local session var failed", K(ret));
    }
  }
  return ret;
}

int ObCalcGeneratedColumn::build_pad_expr_recursively(ObRawExprFactory &expr_factory,
                                                           const ObSQLSessionInfo &session,
                                                           const ObCalcGeneratedColumnInfo &generated_column_info,
                                                           ObRawExpr *&expr,
                                                           const ObLocalSessionVar *local_vars,
                                                           int64_t local_var_id)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(expr));

  if (OB_SUCC(ret) && expr->get_param_count() > 0) {
    for (int i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
      OZ (SMART_CALL(build_pad_expr_recursively(expr_factory,
                                                session,
                                                generated_column_info,
                                                expr->get_param_expr(i),
                                                local_vars,
                                                local_var_id)));
    }
  }
  if (OB_SUCC(ret) && expr->is_column_ref_expr()) {
    ObColumnRefRawExpr *b_expr = static_cast<ObColumnRefRawExpr*>(expr);
    uint64_t column_id = b_expr->get_column_id();
    if (OB_SUCC(ret)) {
      // todo @kaizhan.dkz 需要从ObCalcGeneratedColumnInfo 的dependcy_column_info中获取对应的ObCalcGeneratedColumnInfo
      const ObCalcGeneratedColumnInfo *dependcy_column_info = nullptr;
      if (OB_ISNULL(dependcy_column_info = generated_column_info.get_dependcy_column_by_column_id(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(column_id));
      } else if (ObObjMeta::is_binary(dependcy_column_info->get_data_type(), dependcy_column_info->get_collation_type())) {
        if (OB_FAIL(build_pad_expr(expr_factory, false, dependcy_column_info, expr, &session, local_vars, local_var_id))) {
          LOG_WARN("fail to build pading expr for binary", K(ret));
        }
      } else if (ObCharType == dependcy_column_info->get_data_type()
                || ObNCharType == dependcy_column_info->get_data_type()) {
        if (generated_column_info.has_column_flag(PAD_WHEN_CALC_GENERATED_COLUMN_FLAG)) {
          if (OB_FAIL(build_pad_expr(expr_factory, true, dependcy_column_info, expr, &session, local_vars, local_var_id))) {
            LOG_WARN("fail to build pading expr for char", K(ret));
          }
        } else {
          if (OB_FAIL(build_trim_expr(dependcy_column_info, expr_factory, &session, expr, local_vars, local_var_id))) {
            LOG_WARN("fail to build trime expr for char", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObCalcGeneratedColumn::build_padding_expr(ObRawExprFactory &expr_factory,
                                                   const ObSQLMode sql_mode,
                                                   const sql::ObSQLSessionInfo *session_info,
                                                   const ObCalcGeneratedColumnInfo *generated_column_info,
                                                   ObRawExpr *&expr,
                                                   const ObLocalSessionVar *local_vars,
                                                   int64_t local_var_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(generated_column_info) || OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(generated_column_info), K(expr));
  } else if (ObObjMeta::is_binary(generated_column_info->get_data_type(), generated_column_info->get_collation_type())) {
    if (OB_FAIL(build_pad_expr(expr_factory,
                               false,
                               generated_column_info,
                               expr,
                               session_info,
                               local_vars,
                               local_var_id))) {
      LOG_WARN("fail to build pading expr for binary", K(ret));
    }
  } else if (ObCharType == generated_column_info->get_data_type()
             || ObNCharType == generated_column_info->get_data_type()) {
    if (is_pad_char_to_full_length(sql_mode)) {
      if (OB_FAIL(build_pad_expr(expr_factory,
                                 true,
                                 generated_column_info,
                                 expr,
                                 session_info,
                                 local_vars,
                                 local_var_id))) {
        LOG_WARN("fail to build pading expr for char", K(ret));
      }
    } else {
      if (OB_FAIL(build_trim_expr(generated_column_info,
                                  expr_factory,
                                  session_info,
                                  expr,
                                  local_vars,
                                  local_var_id))) {
        LOG_WARN("fail to build trime expr for char", K(ret));
      }
    }
  }
  return ret;
}

int ObCalcGeneratedColumn::build_pad_expr(ObRawExprFactory &expr_factory,
                                               bool is_char,
                                               const ObCalcGeneratedColumnInfo *generated_column_info,
                                               ObRawExpr *&expr,
                                               const sql::ObSQLSessionInfo *session_info,
                                               const ObLocalSessionVar *local_vars,
                                               int64_t local_var_id)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *pad_expr = NULL;
  ObConstRawExpr *pading_word_expr = NULL;
  ObConstRawExpr *length_expr = NULL;
  ObString padding_char(1, &OB_PADDING_CHAR);
  ObString padding_binary(1, &OB_PADDING_BINARY);
  ObObjType padding_expr_type = ObMaxType;
  ObCollationType padding_expr_collation = CS_TYPE_INVALID;
  if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_PAD, pad_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(pad_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pad expr is null", K(ret));
  } else if (FALSE_IT(padding_expr_type = ob_is_nstring_type(generated_column_info->get_data_type()) ?
                      ObNVarchar2Type : ObVarcharType)) {
  } else if (FALSE_IT(padding_expr_collation = ob_is_nstring_type(generated_column_info->get_data_type()) ?
                      CS_TYPE_UTF8MB4_BIN : generated_column_info->get_collation_type())) {
  } else if (is_char && OB_FAIL(ObRawExprUtils::build_const_string_expr(expr_factory,
                                                        padding_expr_type,
                                                        ObCharsetUtils::get_const_str(padding_expr_collation, OB_PADDING_CHAR),
                                                        padding_expr_collation,
                                                        pading_word_expr))) {
    LOG_WARN("fail to build pading word expr", K(ret));
  } else if (!is_char && OB_FAIL(ObRawExprUtils::build_const_string_expr(expr_factory,
                                                         padding_expr_type,
                                                         padding_binary,
                                                         padding_expr_collation,
                                                         pading_word_expr))) {
    LOG_WARN("fail to build pading word expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory,
                                                          ObIntType,
                                                          generated_column_info->get_data_length(),
                                                          length_expr))) {
    LOG_WARN("fail to build length expr", K(ret));
  } else if (OB_FAIL(pad_expr->set_param_exprs(expr, pading_word_expr, length_expr))) {
    LOG_WARN("fail to set param exprs", K(ret));
  } else {
    ObAccuracy padding_accuracy = pading_word_expr->get_accuracy();
    padding_accuracy.set_length_semantics(generated_column_info->accuracy_.get_length_semantics());
    pading_word_expr->set_accuracy(padding_accuracy);

    pad_expr->set_data_type(padding_expr_type);
    pad_expr->set_func_name(ObString::make_string(N_PAD));
    pad_expr->set_used_in_column_conv(1); //mark for column convert
    expr = pad_expr;
    if (NULL != local_vars) {
      if (OB_FAIL(expr->formalize_with_local_vars(session_info, local_vars, local_var_id))) {
        LOG_WARN("fail to formalize expr", K(ret));
      }
    } else if (OB_FAIL(expr->formalize(session_info))) {
      LOG_WARN("fail to extract info", K(ret));
    }
    LOG_TRACE("build pad expr", KPC(pading_word_expr), KPC(pad_expr));
  }
  return ret;
}

int ObCalcGeneratedColumn::build_trim_expr(const ObCalcGeneratedColumnInfo *generated_column_info,
                                                ObRawExprFactory &expr_factory,
                                                const ObSQLSessionInfo *session_info,
                                                ObRawExpr *&expr,
                                                const ObLocalSessionVar *local_vars,
                                                int64_t local_var_id)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *trim_expr = NULL;
  int64_t trim_type = 2;
  ObConstRawExpr *type_expr = NULL;
  ObConstRawExpr *pattern_expr = NULL;
  ObString padding_char(1, &OB_PADDING_CHAR);
  ObCollationType padding_char_cs_type = CS_TYPE_INVALID;
  if (NULL == generated_column_info || NULL == expr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(generated_column_info), K(expr));
  } else {
    bool is_cs_nonascii = ObCharset::is_cs_nonascii(generated_column_info->get_collation_type());
    padding_char_cs_type = is_cs_nonascii ? ObCharset::get_default_collation(CHARSET_UTF8MB4)
                                          : generated_column_info->get_collation_type();
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_INNER_TRIM, trim_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(trim_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to store expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_int_expr(expr_factory, ObIntType, trim_type, type_expr))) {
    LOG_WARN("fail to build type expr", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_const_string_expr(expr_factory, ObCharType, padding_char,
                                             padding_char_cs_type,
                                             pattern_expr))) {
    LOG_WARN("fail to build pattem expr", K(ret));
  } else if (FALSE_IT(static_cast<ObConstRawExpr*>(pattern_expr)->get_value().set_collation_level(CS_LEVEL_IMPLICIT))) {
    LOG_WARN("fail to set collation type", K(ret));
  } else if (OB_FAIL(trim_expr->set_param_exprs(type_expr, pattern_expr, expr))) {
    LOG_WARN("fail to set param exprs", K(ret), KPC(type_expr), KPC(pattern_expr), KPC(expr));
  } else {
    trim_expr->set_data_type(ObCharType);
    trim_expr->set_func_name(ObString::make_string(N_INNER_TRIM));
    expr = trim_expr;
    if (NULL != local_vars) {
      if (OB_FAIL(expr->formalize_with_local_vars(session_info, local_vars, local_var_id))) {
        LOG_WARN("fail to formalize expr", K(ret));
      }
    } else if (OB_FAIL(expr->formalize(session_info))) {
      LOG_WARN("fail to extract info", K(ret));
    }
  }
  return ret;
}

int ObCalcGeneratedColumn::print_diff_variables(ObLocalSessionVar &local_vars)
{
  int ret = OB_SUCCESS;
  ObSEArray<const ObSessionSysVar *, 4> var_array;
  if (OB_FAIL(local_vars.get_local_vars(var_array))) {
    LOG_WARN("extract sysvars failed", K(ret));
  } else {
    SMART_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], val_buf) {
      for (int64_t i = 0; OB_SUCC(ret) && i < var_array.count(); ++i) {
        ObString var_name;
        int64_t pos = 0;
        if (OB_ISNULL(var_array.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret));
        } else if (OB_FAIL(ObSysVarFactory::get_sys_var_name_by_id(var_array.at(i)->type_, var_name))) {
          LOG_WARN("get sysvar name failed", K(ret));
        } else if (OB_FAIL(var_array.at(i)->val_.print_sql_literal(val_buf, 100, pos))) {
          LOG_WARN("print value failed", K(ret));
        } else {
          LOG_WARN("session vars are different with the old vars which were solidified when creating generated columns",
                   K(ret), K(var_name));
        }
      }
    }
  }
  return ret;
}

int ObCalcGeneratedColumn::build_generated_column_expr_for_cdc(ObRawExprFactory &expr_factory,
                                                                    const ObSQLSessionInfo &session_info,
                                                                    ObSQLMode sql_mode,
                                                                    ObCollationType cs_type,
                                                                    ObCalcGeneratedColumnInfo &generated_column_info,
                                                                    ObColumnRefRawExpr *generated_column_expr,
                                                                    RowDesc &row_desc,
                                                                    ObRawExpr *&generated_ref_expr)
{
  int ret = OB_SUCCESS;
  const ParseNode *node = NULL;
  ObArray<ObVarInfo> sys_vars;
  ObArray<ObAggFunRawExpr*> aggr_exprs;
  ObArray<ObWinFunRawExpr*> win_exprs;
  ObArray<ObSubQueryInfo> sub_query_info;
  ObArray<ObUDFInfo> udf_info;
  ObArray<ObOpRawExpr*> op_exprs;
  ObArray<ObQualifiedName> columns;

  ObCharsets4Parser charsets4parser = session_info.get_charsets4parser();
  charsets4parser.string_collation_ = cs_type;
  if (OB_FAIL(ObRawExprUtils::parse_expr_node_from_str(generated_column_info.column_define_,
                                                       charsets4parser,
                                                       expr_factory.get_allocator(),
                                                       node))) {
    LOG_WARN("fail to parser generated_column str", K(ret), K(generated_column_info.column_define_));
  } else if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("node is null", K(ret));
  } else if (OB_FAIL(ObRawExprUtils::build_raw_expr(expr_factory,
                                                    session_info,
                                                    NULL, /* schema_checker */
                                                    NULL,
                                                    T_NONE_SCOPE,
                                                    NULL,
                                                    NULL,
                                                    NULL,
                                                    *node,
                                                    generated_ref_expr,
                                                    columns,
                                                    sys_vars,
                                                    aggr_exprs,
                                                    win_exprs,
                                                    sub_query_info,
                                                    udf_info,
                                                    op_exprs,
                                                    false,
                                                    TgTimingEvent::TG_TIMING_EVENT_INVALID,
                                                    true, /* use_def_collation */
                                                    cs_type,
                                                    true))) {
    LOG_WARN("fail to build raw expr", K(ret), K(generated_ref_expr));
  } else if (OB_FAIL(row_desc.init())) {
    LOG_WARN("Failed to init row desc", K(ret));
  }

  int64_t dependcy_dolumn_cnt = generated_column_info.dependcy_column_infos_.count();
  for (int i = 0; OB_SUCC(ret) && i < generated_column_info.dependcy_column_infos_.count(); i++) {
    bool founded = false;
    ObCalcGeneratedColumnInfo *dependcy_column_info = generated_column_info.dependcy_column_infos_.at(i);
    ObColumnRefRawExpr *column_expr = nullptr;
    if (OB_ISNULL(dependcy_column_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not found dependcy column info", K(ret));
    } else if (OB_FAIL(expr_factory_->create_raw_expr(T_REF_COLUMN, column_expr))) {
      LOG_WARN("create column expr failed", K(ret));
    } else if (OB_ISNULL(column_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column_expr is null");
    } else if (OB_FAIL(init_column_ref_expr(dependcy_column_info, session_info, column_expr))) {
      LOG_WARN("fail to init column ref expr", K(ret));
    } else if (OB_FAIL(dependcy_column_exprs_.push_back(column_expr))) {
      LOG_WARN("fail to push column_expr");
    } else if (OB_FAIL(row_desc.add_column(column_expr))) {
      LOG_WARN("add column to row desc failed", K(ret));
    } else {
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < columns.count(); ++j) {
      ObQualifiedName &q_name = columns.at(j);
      if (q_name.is_pl_udf()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("only support for generated column", K(ret), K(generated_column_info.column_define_));
      } else if (!ObCharset::case_insensitive_equal(q_name.col_name_, column_expr->get_column_name())) {

      } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(generated_ref_expr, q_name.ref_expr_, column_expr))) {
        LOG_WARN("replace reference column failed", K(ret));
      }
    } // end for columns
  } // end for dependcy_column_infos_

  int64_t var_array_idx = OB_INVALID_INDEX_INT64;
  ObLocalSessionVar local_vars(&inner_alloc_);
  ObSEArray<const ObSessionSysVar *, 4> var_array;
  if (OB_SUCC(ret)) {
    //set local session info for generate columns
    if (0 == generated_column_info.local_session_vars_.get_var_count()) {
      //do nothing
    } else if (OB_FAIL(local_vars.assign(generated_column_info.local_session_vars_))) {
      LOG_WARN("assign local vars failed", K(ret));
    } else if (OB_FAIL(local_vars.remove_vars_same_with_session(&session_info))) {
      LOG_WARN("remove vars same with session failed", K(ret));
    } else if (0 == local_vars.get_var_count()) {
      //do nothing if all local vars are same with cur session vars
    } else if (OB_FAIL(add_local_session_vars(&inner_alloc_, local_vars, var_array_idx))) {
      LOG_WARN("add local session var failed", K(ret));
    } else if (OB_FAIL(print_diff_variables(local_vars))) {
      LOG_WARN("fail to print different local vars", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(build_pad_expr_recursively(expr_factory,
                                           session_info,
                                           generated_column_info,
                                           generated_ref_expr,
                                           &local_vars,
                                           var_array_idx))) {
      LOG_WARN("fail to build pad expr recursively", K(ret));
    } else if (OB_FAIL(build_padding_expr(expr_factory,
                                          sql_mode,
                                          &session_info,
                                          &generated_column_info,
                                          generated_ref_expr,
                                          &local_vars,
                                          var_array_idx))) {
      LOG_WARN("build padding expr for self failed", K(ret));
    } else if (OB_FAIL(generated_ref_expr->formalize_with_local_vars(&session_info, &local_vars, var_array_idx))) {
      LOG_WARN("formalize expr failed", K(ret), KPC(generated_ref_expr));
    } else if (ObRawExprUtils::need_column_conv(generated_column_expr->get_result_type(), *generated_ref_expr, true)) {
      if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(expr_factory,
                                                         inner_alloc_,
                                                         *generated_column_expr,
                                                         generated_ref_expr,
                                                         &session_info,
                                                         true, /* used_for_generated_column */
                                                         &local_vars,
                                                         var_array_idx))) {
        LOG_WARN("build column convert expr failed", K(ret));
      }
    }
  }

  return ret;
}

int ObCalcGeneratedColumn::init_column_ref_expr(const ObCalcGeneratedColumnInfo *column_info,
                                                const ObSQLSessionInfo &session_info,
                                                ObColumnRefRawExpr *&column_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(column_info) || OB_ISNULL(column_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_expr is null");
  } else {
    // 初始化表达式的类型等信息
    const ObAccuracy &accuracy = column_info->accuracy_;
    column_expr->set_ref_id(column_info->table_id_, column_info->column_id_);
    column_expr->set_data_type(column_info->get_data_type());
    column_expr->set_result_flag(calc_column_result_flag(*column_info));
    column_expr->get_column_name().assign_ptr(column_info->col_name_.ptr(), column_info->col_name_.length());
    column_expr->set_column_flags(column_info->column_flags_);
    column_expr->set_hidden_column(column_info->is_hidden_);
    column_expr->set_lob_column(is_lob_storage(column_info->get_data_type()));
    column_expr->set_is_rowkey_column(column_info->is_rowkey_column_);
    column_expr->set_srs_id(column_info->srs_id_);
    column_expr->set_udt_set_id(column_info->udt_set_id_);
    if (ob_is_string_type(column_info->get_data_type())
        || ob_is_enumset_tc(column_info->get_data_type())
        || ob_is_json_tc(column_info->get_data_type())
        || ob_is_geometry_tc(column_info->get_data_type())) {
      column_expr->set_collation_type(column_info->get_collation_type());
    } else {
      column_expr->set_collation_type(CS_TYPE_BINARY);
    }
    // extract set collation level for reuse
    column_expr->set_collation_level(
        ObRawExprUtils::get_column_collation_level(column_info->get_data_type()));

    if (OB_SUCC(ret)) {
      column_expr->set_accuracy(accuracy);
      if (OB_FAIL(column_expr->extract_info())) {
        LOG_WARN("extract column expr info failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && column_info->is_enum_or_set()) {
      if (OB_FAIL(init_column_expr_subschema(*column_info, &session_info, *column_expr))) {
        LOG_WARN("failed to init column expr subschema", K(ret));
      }
    }
    if (OB_SUCC(ret) && column_info->is_xmltype()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("only support xml type for generated column", K(ret), KPC(column_info));
    }
  }
  return ret;
}

int ObCalcGeneratedColumn::init_column_expr_subschema(const ObCalcGeneratedColumnInfo &column_info,
                                                      const ObSQLSessionInfo *session_info,
                                                      ObColumnRefRawExpr &column_expr)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = NULL;
  if (OB_ISNULL(session_info) ||
      OB_ISNULL(exec_ctx = const_cast<ObSQLSessionInfo*>(session_info)->get_cur_exec_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), KP(session_info), KP(exec_ctx));
  } else if (column_info.is_enum_or_set()) {
    uint16_t subschema_id = 0;
    if (OB_FAIL(exec_ctx->get_subschema_id_by_type_info(column_expr.get_result_type().get_obj_meta(),
                                                        column_info.extended_type_info_,
                                                        subschema_id))) {
      LOG_WARN("failed to get subschema id by type info", K(ret));
    } else {
      column_expr.set_subschema_id(subschema_id);
      column_expr.mark_sql_enum_set_with_subschema();
    }
  }
  return ret;
}

int ObCalcGeneratedColumn::resolve_collation_from_solidified_var(ObCalcGeneratedColumnInfo &generated_column_info,
                                                                      ObCollationType &cs_type)
{
  int ret = OB_SUCCESS;
  ObSessionSysVar *local_var = NULL;
  if (OB_FAIL(generated_column_info.local_session_vars_.get_local_var(SYS_VAR_COLLATION_CONNECTION, local_var))) {
    LOG_WARN("get local session var failed", K(ret));
  } else if (NULL != local_var) {
    cs_type = static_cast<ObCollationType>(local_var->val_.get_int());
  }
  return ret;
}

int ObCalcGeneratedColumn::resolve_sql_mode_from_solidified_var(ObCalcGeneratedColumnInfo &generated_column_info,
                                                                     ObSQLMode &sql_mode)
{
  int ret = OB_SUCCESS;
  ObSessionSysVar *local_var = NULL;
  if (OB_FAIL(generated_column_info.local_session_vars_.get_local_var(SYS_VAR_SQL_MODE, local_var))) {
    LOG_WARN("get local session var failed", K(ret));
  } else if (OB_NOT_NULL(local_var)) {
    if (ObUInt64Type == local_var->val_.get_type()) {
      sql_mode = local_var->val_.get_uint64();
    } else if (ObIntType == local_var->val_.get_type()) {
      sql_mode = static_cast<uint64_t>(local_var->val_.get_int());
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid sql mode val type", K(ret), K(local_var->val_));
    }
  }
  return ret;
}

int ObCalcGeneratedColumn::resolve_generated_column_expr(ObCalcGeneratedColumnInfo &generated_column_info,
                                                              RowDesc &row_desc,
                                                              ObColumnRefRawExpr *&generated_column_expr,
                                                              ObRawExpr *&generated_ref_expr)
{
  int ret = OB_SUCCESS;
  ObSQLMode sql_mode = DEFAULT_MYSQL_MODE;
  ObCollationType cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
  generated_column_expr = nullptr;
  generated_ref_expr = nullptr;
  if (OB_ISNULL(inner_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner session is invalid", K(ret));
  } else if (OB_FAIL(resolve_sql_mode_from_solidified_var(generated_column_info, sql_mode))) {
    LOG_WARN("fail to resolve sql mode from solidified_var", K(ret), K(generated_column_info));
  } else if (OB_FAIL(resolve_collation_from_solidified_var(generated_column_info, cs_type))) {
    LOG_WARN("fail to resolve callation type from solidified_var", K(ret), K(generated_column_info));
  } else if (OB_FAIL(expr_factory_->create_raw_expr(T_REF_COLUMN, generated_column_expr))) {
    LOG_WARN("create column expr failed", K(ret));
  } else if (OB_ISNULL(generated_column_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_expr is null");
  } else if (OB_FAIL(init_column_ref_expr(&generated_column_info, *inner_session_, generated_column_expr))) {
    LOG_WARN("init column expr failed", K(ret));
  } else if (OB_FAIL(build_generated_column_expr_for_cdc(*expr_factory_,
                                                         *inner_session_,
                                                         sql_mode,
                                                         cs_type,
                                                         generated_column_info,
                                                         generated_column_expr,
                                                         row_desc,
                                                         generated_ref_expr))) {
    LOG_WARN("fail to build generated column expr", K(ret), K(generated_column_info));
  }
  return ret;
}

int ObCalcGeneratedColumn::create_ctx_before_calc()
{
  int ret = OB_SUCCESS;
  void *factory_buf = nullptr;
  void *exec_ctx_buf = nullptr;
  if (OB_ISNULL(inner_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inner session is invalid", K(ret));
  } else if (OB_UNLIKELY(NULL == (exec_ctx_buf = inner_alloc_.alloc(sizeof(ObExecContext))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("fail to alloc ObExecContext", K(ret));
  } else if (FALSE_IT(exec_ctx_ = new (exec_ctx_buf) ObExecContext(inner_alloc_))) {
    // do nothing
  } else if (OB_ISNULL(factory_buf = inner_alloc_.alloc((sizeof(ObRawExprFactory))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("default session buf allocated failed", K(ret));
  } else if (FALSE_IT(expr_factory_ = new(factory_buf)ObRawExprFactory(inner_alloc_))) {
    // do nothing
  } else if (OB_FAIL(exec_ctx_->create_physical_plan_ctx())) {
    LOG_WARN("create plan ctx error", K(ret));
  } else {
    exec_ctx_->set_my_session(inner_session_);
    typedef ObSQLSessionInfo::ExecCtxSessionRegister MyExecCtxSessionRegister;
    MyExecCtxSessionRegister ctx_register(*inner_session_, exec_ctx_);
    exec_ctx_->get_task_executor_ctx()->set_min_cluster_version(GET_MIN_CLUSTER_VERSION());
    exec_ctx_->set_sql_ctx(&sql_ctx_);
  }
  return ret;
}

int ObCalcGeneratedColumn::calc_generated_column(ObCalcGeneratedColumnInfo &column_info,
                                                      common::ObIAllocator &alloc,
                                                      ObTempExpr *&temp_expr,
                                                      ObObj &result_obj)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = nullptr;
  ObColumnRefRawExpr *generated_column_expr = nullptr;
  ObRawExpr *generated_ref_expr = nullptr;
  RowDesc row_desc;
  void *factory_buf = nullptr;
  void *exec_ctx_buf = nullptr;
  temp_expr = nullptr;
  if (OB_FAIL(create_ctx_before_calc())) {
    LOG_WARN("fail to alloc ObExecContext and expr_factory", K(ret));
  } else if (OB_FAIL(resolve_generated_column_expr(column_info,
                                                   row_desc,
                                                   generated_column_expr,
                                                   generated_ref_expr))) {
    LOG_WARN("fail to resolve generated column expr", K(ret));
  } else {
    LOG_TRACE("after resolve_generated_column_expr", KPC(generated_column_expr), KPC(generated_ref_expr), K(row_desc));
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(exec_ctx_->get_physical_plan_ctx()->set_all_local_session_vars(all_local_session_vars_))) {
    LOG_WARN("fail to assign session vars", K(ret));
  } else if (OB_FAIL(ObStaticEngineExprCG::gen_expr_with_row_desc(generated_ref_expr,
                                                                  row_desc,
                                                                  alloc,
                                                                  inner_session_,
                                                                  NULL,
                                                                  temp_expr))) {
    LOG_WARN("fail to generated temp expr", K(ret), KPC(generated_ref_expr));
  } else if (OB_FAIL(temp_expr->eval(*exec_ctx_, *column_info.dependcy_row_, result_obj))) {
    LOG_WARN("fail to calc value", K(ret), KPC(temp_expr));
  } else {
    LOG_TRACE("print after eval", K(result_obj), K(column_info));
  }
  return ret;
}

int ObCalcGeneratedColumn::calc_generated_column(ObTempExpr *temp_expr,
                                                      const ObNewRow &row,
                                                      ObObj &result_obj)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(temp_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(create_ctx_before_calc())) {
    LOG_WARN("fail to alloc ObExecContext and expr_factory", K(ret));
  } else if (OB_ISNULL(exec_ctx_->get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(exec_ctx_->get_physical_plan_ctx()->set_all_local_session_vars(all_local_session_vars_))) {
    LOG_WARN("fail to assign session vars", K(ret));
  } else if (OB_FAIL(temp_expr->eval(*exec_ctx_, row, result_obj))) {
    LOG_WARN("fail to calc value", K(ret), K(row), KPC(temp_expr));
  } else {
    LOG_TRACE("print after eval", K(result_obj), K(row));
  }
  return ret;
}

void ObCalcGeneratedColumn::reuse()
{
  all_local_session_vars_.reuse();
  dependcy_column_exprs_.reuse();
  if (OB_NOT_NULL(expr_factory_)) {
    expr_factory_->destory();
    expr_factory_ = nullptr;
  }
  if (OB_NOT_NULL(exec_ctx_)) {
    exec_ctx_->~ObExecContext();
    exec_ctx_ = nullptr;
  }
  inner_alloc_.reuse();
}

void ObCalcGeneratedColumn::destroy()
{
  all_local_session_vars_.destroy();
  dependcy_column_exprs_.destroy();
  if (OB_NOT_NULL(expr_factory_)) {
    expr_factory_->destory();
    expr_factory_->~ObRawExprFactory();
    expr_factory_ = nullptr;
  }
  if (OB_NOT_NULL(inner_session_)) {
    inner_session_->~ObSQLSessionInfo();
    inner_session_ = nullptr;
  }
  if (OB_NOT_NULL(exec_ctx_)) {
    exec_ctx_->~ObExecContext();
    exec_ctx_ = nullptr;
  }
  tz_info_map_.destroy();
  inner_alloc_.reset();
  long_life_alloc_.reset();
}

}  // namespace sql
}  // namespace oceanbase
