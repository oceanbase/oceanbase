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

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/ob_resolver_define.h"
#include "sql/resolver/expr/ob_raw_expr.h"

namespace oceanbase
{
namespace sql
{

int ObResolverParams::assign(const ObResolverParams &other)
{
  int ret = OB_SUCCESS;
  allocator_ = other.allocator_;
  schema_checker_ = other.schema_checker_;
  secondary_namespace_ = other.secondary_namespace_;
  session_info_ = other.session_info_;
  query_ctx_ = other.query_ctx_;
  param_list_ = other.param_list_;
  select_item_param_infos_ = other.select_item_param_infos_;
  prepare_param_count_ = other.prepare_param_count_;
  sql_proxy_ = other.sql_proxy_;
  database_id_ = other.database_id_;
  disable_privilege_check_ = other.disable_privilege_check_;
  force_trace_log_ = other.force_trace_log_;
  expr_factory_ = other.expr_factory_;
  stmt_factory_ = other.stmt_factory_;
  show_tenant_id_ = other.show_tenant_id_;
  show_seed_ = other.show_seed_;
  is_from_show_resolver_ = other.is_from_show_resolver_;
  is_restore_ = other.is_restore_;
  is_from_create_view_ = other.is_from_create_view_;
  is_mview_definition_sql_ = other.is_mview_definition_sql_;
  is_from_create_table_ = other.is_from_create_table_;
  is_prepare_protocol_ = other.is_prepare_protocol_;
  is_pre_execute_ = other.is_pre_execute_;
  is_prepare_stage_ = other.is_prepare_stage_;
  is_dynamic_sql_ = other.is_dynamic_sql_;
  is_dbms_sql_ = other.is_dbms_sql_;
  statement_id_ = other.statement_id_;
  resolver_scope_stmt_type_ = other.resolver_scope_stmt_type_;
  cur_sql_ = other.cur_sql_;
  contain_dml_ = other.contain_dml_;
  is_ddl_from_primary_ = other.is_ddl_from_primary_;
  is_cursor_ = other.is_cursor_;
  have_same_table_name_ = other.have_same_table_name_;
  is_default_param_ = other.is_default_param_;
  is_batch_stmt_ = other.is_batch_stmt_;
  batch_stmt_num_ = other.batch_stmt_num_;
  new_gen_did_ = other.new_gen_did_;
  new_gen_cid_ = other.new_gen_cid_;
  new_gen_qid_ = other.new_gen_qid_;
  new_cte_tid_ = other.new_cte_tid_;
  new_gen_wid_ = other.new_gen_wid_;
  is_resolve_table_function_expr_ = other.is_resolve_table_function_expr_;
  tg_timing_event_ = other.tg_timing_event_;
  is_column_ref_ = other.is_column_ref_;
  outline_parse_result_ = other.outline_parse_result_;
  outline_state_ = other.outline_state_;
  is_execute_call_stmt_ = other.is_execute_call_stmt_;
  enable_res_map_ = other.enable_res_map_;
  need_check_col_dup_ = other.need_check_col_dup_;
  is_specified_col_name_ = other.is_specified_col_name_;
  is_in_sys_view_ = other.is_in_sys_view_;
  is_expanding_view_ = other.is_expanding_view_;
  is_resolve_lateral_derived_table_ = other.is_resolve_lateral_derived_table_;
  package_guard_ = other.package_guard_;
  is_for_rt_mv_ = other.is_for_rt_mv_;
  is_resolve_fake_cte_table_ = other.is_resolve_fake_cte_table_;
  is_returning_ = other.is_returning_;
  is_in_view_ = other.is_in_view_;
  is_htable_ = other.is_htable_;

  if (OB_FAIL(global_hint_.assign(other.global_hint_))) {
    LOG_WARN("fail to assign global hint", K(ret));
  } else if (OB_FAIL(external_param_info_.assign(other.external_param_info_))) {
    LOG_WARN("fail to assign external param info", K(ret));
  } else if (OB_FAIL(star_expansion_infos_.assign(other.star_expansion_infos_))) {
    LOG_WARN("fail to assign star expansion infos", K(ret));
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
