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
#include "sql/resolver/dml/ob_sql_hint.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "common/ob_smart_call.h"
#include "sql/monitor/ob_sql_plan.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

void ObQueryHint::reset()
{
  cs_type_ = CS_TYPE_INVALID;
  global_hint_.reset();
  is_valid_outline_ = false;
  user_def_outline_ = false;
  outline_stmt_id_ = OB_INVALID_STMT_ID;
  qb_hints_.reuse();
  stmt_id_hints_.reuse();
  trans_list_.reuse();
  outline_trans_hints_.reuse();
  used_trans_hints_.reuse();
  qb_name_map_.reuse();
  stmt_id_map_.reuse();
  sel_start_id_ = 1;
  set_start_id_ = 1;
  other_start_id_ = 1;
}

int ObQueryHint::create_hint_table(ObIAllocator *allocator, ObTableInHint *&table)
{
  int ret = common::OB_SUCCESS;
  table = NULL;
  void *ptr = NULL;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(allocator));
  } else if (OB_ISNULL(ptr = allocator->alloc(sizeof(ObTableInHint)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    SQL_RESV_LOG(ERROR, "no more memory to create hint table");
  } else {
    table = new(ptr) ObTableInHint();
  }
  return ret;
}

int ObQueryHint::create_leading_table(ObIAllocator *allocator, ObLeadingTable *&table)
{
  int ret = common::OB_SUCCESS;
  table = NULL;
  void *ptr = NULL;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(allocator));
  } else if (OB_ISNULL(ptr = allocator->alloc(sizeof(ObLeadingTable)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    SQL_RESV_LOG(ERROR, "no more memory to create leading table");
  } else {
    table = new(ptr) ObLeadingTable();
  }
  return ret;
}

int ObQueryHint::get_qb_name_source_hash_value(const ObString &src_qb_name,
                                               const ObIArray<uint32_t> &src_hash_val,
                                               uint32_t &hash_val)
{
  int ret = OB_SUCCESS;
  hash_val = 0;
  if (OB_UNLIKELY(src_hash_val.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected src_hash_val", K(ret), K(src_hash_val));
  } else {
    hash_val = src_hash_val.at(src_hash_val.count() - 1);
    hash_val = fnv_hash2(src_qb_name.ptr(), src_qb_name.length(), hash_val);
  }
  return ret;
}

int ObQueryHint::set_outline_data_hints(const ObGlobalHint &global_hint,
                                        const int64_t stmt_id,
                                        const ObIArray<ObHint*> &hints)
{
  int ret = OB_SUCCESS;
  qb_hints_.reuse();
  stmt_id_hints_.reuse();
  trans_list_.reuse();
  if (OB_FAIL(global_hint_.assign(global_hint))) {
    LOG_WARN("failed to assign global hint.", K(ret));
  } else if (OB_FAIL(append_hints(stmt_id, hints))) {
    LOG_WARN("failed to assign global hint.", K(ret));
  } else {
    if (global_hint_.has_valid_opt_features_version()) {
      is_valid_outline_ = true;
      outline_stmt_id_ = stmt_id;
    } else {
      user_def_outline_ = true;
    }
    ObHint *cur_hint = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < hints.count(); ++i) {
      if (OB_ISNULL(cur_hint = hints.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL.", K(ret), K(cur_hint));
      } else if (cur_hint->is_transform_outline_hint()
                 && OB_FAIL(trans_list_.push_back(cur_hint))) {
        LOG_WARN("faild to push back hint.", K(ret));
      }
    }
    LOG_DEBUG("finish add outline data hints");
  }
  return ret;
}

int ObQueryHint::append_hints(int64_t stmt_id,
                              const ObIArray<ObHint*> &hints)
{
  int ret = OB_SUCCESS;
  const ObHints *stmt_id_hints = get_stmt_id_hints(stmt_id);
  ObHints *qb_hints = NULL;
  ObHint *cur_hint = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < hints.count(); ++i) {
    if (OB_ISNULL(cur_hint = hints.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL.", K(ret), K(cur_hint));
    } else if (cur_hint->get_qb_name().empty()) {
      if (NULL == (qb_hints = const_cast<ObHints*>(stmt_id_hints))) {
        if (OB_ISNULL(qb_hints = stmt_id_hints_.alloc_place_holder())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_ERROR("Allocate ObHints from array error", K(ret));
        } else {
          qb_hints->stmt_id_ = stmt_id;
          stmt_id_hints = qb_hints;
        }
      }
    } else if (NULL != (qb_hints = const_cast<ObHints*>(get_qb_hints(cur_hint->get_qb_name())))) {
      /* do nothing */
    } else if (OB_ISNULL(qb_hints = qb_hints_.alloc_place_holder())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Allocate ObHints from array error", K(ret));
    } else {
      qb_hints->qb_name_ = cur_hint->get_qb_name();
    }

    if (OB_SUCC(ret) && OB_FAIL(qb_hints->hints_.push_back(cur_hint))) {
      LOG_WARN("faild to push back hint.", K(ret));
    }
  }
  return ret;
}

const ObHints *ObQueryHint::get_qb_hints(const ObString &qb_name) const
{
  const ObHints *qb_hints = NULL;
  for (int64_t i = 0; NULL == qb_hints && i < qb_hints_.count(); ++i) {
    if (0 == qb_name.case_compare(qb_hints_.at(i).qb_name_)) {
      qb_hints = &qb_hints_.at(i);
    }
  }
  return qb_hints;
}

const ObHints *ObQueryHint::get_stmt_id_hints(int64_t stmt_id) const
{
  const ObHints *qb_hints = NULL;
  for (int64_t i = 0; NULL == qb_hints && i < stmt_id_hints_.count(); ++i) {
    if (stmt_id_hints_.at(i).stmt_id_ == stmt_id) {
      qb_hints = &stmt_id_hints_.at(i);
    }
  }
  return qb_hints;
}

int ObQueryHint::add_stmt_id_map(const int64_t stmt_id, stmt::StmtType stmt_type)
{
  int ret = OB_SUCCESS;
  QbNames *qb_names = NULL;
  if (OB_INVALID_STMT_ID == stmt_id) {
    /* do nothing */
  } else if (OB_UNLIKELY(stmt_id_map_.count() != stmt_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exists valid stmt id not added before", K(ret), K(stmt_id),
                                                      K(stmt_type), K(stmt_id_map_.count()));
  } else if (OB_ISNULL(qb_names = stmt_id_map_.alloc_place_holder())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate QbNames from array error", K(ret));
  } else {
    qb_names->stmt_type_ = stmt_type;
  }
  return ret;
}

int ObQueryHint::set_stmt_id_map_info(const ObDMLStmt &stmt, ObString &qb_name)
{
  int ret = OB_SUCCESS;
  const int64_t stmt_id = stmt.get_stmt_id();
  if (OB_UNLIKELY(0 > stmt_id || stmt_id_map_.count() <= stmt_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt id for dml stmt", K(ret), K(stmt_id), K(stmt_id_map_.count()));
  } else {
    QbNames &qb_names = stmt_id_map_.at(stmt_id);
    qb_names.stmt_type_ = stmt.get_stmt_type();
    qb_names.is_set_stmt_ = stmt.is_set_stmt();
    qb_names.is_from_hint_ = !qb_name.empty();
    qb_names.qb_names_.reuse();
    if (!qb_name.empty() && OB_FAIL(qb_names.qb_names_.push_back(qb_name))) {
      LOG_WARN("fail to push back qb name", K(ret), K(qb_name));
    }
  }
  return ret;
}

int ObQueryHint::check_and_set_params_from_hint(const ObResolverParams &params, const ObDMLStmt &stmt) const
{
  int ret = OB_SUCCESS;
  const ObSQLSessionInfo *session_info = NULL;
  bool has_enable_param = false;
  ObQueryCtx *query_ctx = NULL;
  if (OB_ISNULL(session_info = params.session_info_)
      || OB_ISNULL(query_ctx = params.query_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(session_info), K(query_ctx));
  } else if (T_NONE_SCOPE != params.hidden_column_scope_ &&
             OB_FAIL(global_hint_.opt_params_.has_enable_opt_param(ObOptParamHint::OptParamType::HIDDEN_COLUMN_VISIBLE, has_enable_param))) {
    LOG_WARN("failed to check has enable opt param", K(ret));
  } else if (OB_UNLIKELY(T_NONE_SCOPE != params.hidden_column_scope_ && !has_enable_param)) {
    ret = OB_ERR_BAD_FIELD_ERROR;
    LOG_WARN("hidden columns not allowed", K(ret));
    ObString column_name(OB_HIDDEN_PK_INCREMENT_COLUMN_NAME);
    ObString scope_name = ObString::make_string(get_scope_name(params.hidden_column_scope_));
    LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, column_name.length(), column_name.ptr(),
                                          scope_name.length(), scope_name.ptr());
  } else if (OB_FAIL(check_ddl_schema_version_from_hint(stmt))) {
    LOG_WARN("failed to check ddl schema version", K(ret));
  } else {
    if (global_hint_.query_timeout_ > 0) {
      THIS_WORKER.set_timeout_ts(session_info->get_query_start_time() + global_hint_.query_timeout_);
    }
    if (global_hint_.has_valid_opt_features_version()) {
      query_ctx->optimizer_features_enable_version_ = global_hint_.opt_features_version_;
    } else if (OB_FAIL(session_info->get_optimizer_features_enable_version(query_ctx->optimizer_features_enable_version_))) {
      LOG_WARN("failed to check ddl schema version", K(ret));
    }
  }
  return ret;
}

int ObQueryHint::check_ddl_schema_version_from_hint(const ObDMLStmt &stmt,
                                                    const ObDDLSchemaVersionHint& ddlSchemaVersionHint) const
{
  int ret = OB_SUCCESS;
  TableItem* item = NULL;
  if (OB_FAIL(get_basic_table_without_index_by_hint_table(stmt, ddlSchemaVersionHint.table_, item))) {
    LOG_WARN("failed to get table item by hint table", K(ret));
  } else if (OB_ISNULL(item)) {
    ObSEArray<ObSelectStmt*, 8> child_stmts;
    if (OB_FAIL(stmt.get_child_stmts(child_stmts))) {
      LOG_WARN("failed to get child stmts", K(ret));
    }
    for (int64_t index = 0; OB_SUCC(ret) && index < child_stmts.count(); ++index) {
      if (OB_FAIL(SMART_CALL(check_ddl_schema_version_from_hint(*child_stmts.at(index),
                                                        ddlSchemaVersionHint)))) {
        LOG_WARN("failed to check ddl schema version from hint", K(ret));
      }
    }
  } else if (OB_LIKELY(item->ddl_schema_version_ > 0) &&
             OB_UNLIKELY(ddlSchemaVersionHint.schema_version_ != item->ddl_schema_version_)) {
    ret = OB_DDL_SCHEMA_VERSION_NOT_MATCH;
    LOG_USER_ERROR(OB_DDL_SCHEMA_VERSION_NOT_MATCH);
    LOG_WARN("failed to check ddl schema version", K(ret), K(item->ddl_schema_version_), K(ddlSchemaVersionHint.schema_version_));
  }
  return ret;
}

int ObQueryHint::check_ddl_schema_version_from_hint(const ObDMLStmt &stmt) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < global_hint_.ob_ddl_schema_versions_.count(); i++) {
    if (OB_FAIL(check_ddl_schema_version_from_hint(stmt, 
                                                   global_hint_.ob_ddl_schema_versions_.at(i)))) {
      LOG_WARN("failed to check ddl schema version from hint", K(ret));
    }
  }
  return ret;
}

// init query hint after resolve all stmt.
int ObQueryHint::init_query_hint(ObIAllocator *allocator,
                                 ObSQLSessionInfo *session_info,
                                 ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator) || OB_ISNULL(session_info) || OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(allocator), K(session_info), K(stmt));
  } else if (OB_FAIL(session_info->get_collation_type_of_names(OB_TABLE_NAME_CLASS, cs_type_))) {
    LOG_WARN("fail to get collation type of name", K(OB_TABLE_NAME_CLASS), K(ret));
  } else if (OB_LIKELY(!qb_name_map_.created()) &&
             OB_FAIL(qb_name_map_.create(64, ObModIds::OB_SQL_COMPILE))) {
    LOG_WARN("failed to create qb name map", K(ret));
  } else if (OB_FAIL(reset_duplicate_qb_name())) {
    LOG_WARN("failed to reset duplicate qb name", K(ret));
  } else if (OB_FAIL(generate_orig_stmt_qb_name(*allocator, 0))) {
    LOG_WARN("failed to generate stmt name after resolve", K(ret));
  } else if (OB_FAIL(distribute_hint_to_orig_stmt(stmt))) {
    LOG_WARN("faild to distribute hint to orig stmt", K(ret));
  } else {
    LOG_TRACE("finish init query hint", K(*this));
  }
  return ret;
}

int ObQueryHint::distribute_hint_to_orig_stmt(ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObDMLStmt *root_stmt = stmt;
  ObSEArray<ObSelectStmt*, 16> all_child_stmts;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null stmt", K(ret), K(stmt));
  } else if (stmt->is_explain_stmt() &&
             OB_FALSE_IT(root_stmt = static_cast<ObExplainStmt*>(stmt)->get_explain_query_stmt())) {
  } else if (OB_FAIL(ObTransformUtils::get_all_child_stmts(root_stmt, all_child_stmts))) {
    LOG_WARN("failed to get all child stmt", K(ret));
  } else {
    ObDMLStmt *cur_stmt = NULL;
    for (int64_t i = -1; OB_SUCC(ret) && i < all_child_stmts.count(); ++i) {
      if (OB_ISNULL(cur_stmt = -1 == i ? root_stmt : all_child_stmts.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null stmt", K(ret), K(cur_stmt));
      } else if (OB_FAIL(cur_stmt->set_table_item_qb_name())) {
        LOG_WARN("failed to update table item qb name", K(ret));
      } else if (OB_FAIL(cur_stmt->get_stmt_hint().init_stmt_hint(*cur_stmt, *this, true))) {
        LOG_WARN("failed to init stmt hint", K(ret));
      }
    }
  }
  return ret;
}

//after generate a new stmt_id for stmt, need generate new qb_name and add hint from qb_hints
//  1. generate new qb name for stmt
//  2. add hint from qb_hints use new qb name
int ObQueryHint::adjust_qb_name_for_stmt(ObIAllocator &allocator,
                                         ObDMLStmt &stmt,
                                         const ObString &src_qb_name,
                                         const ObIArray<uint32_t> &src_hash_val,
                                         int64_t *sub_num /* default = NULL */)
{
  int ret = OB_SUCCESS;
  ObString qb_name;
  const ObHints *qb_hints = NULL;
  stmt.get_stmt_hint().set_query_hint(this);
  if (OB_FAIL(generate_qb_name_for_stmt(allocator, stmt, src_qb_name, src_hash_val, qb_name, sub_num))) {
    LOG_WARN("failed to generate qb name by str", K(ret));
  } else if (has_outline_data() || !stmt.get_stmt_hint().inited()) {
    // for outline data, stmts generated by transformer only use hints matched by qb_name
    if (OB_FAIL(stmt.get_stmt_hint().init_stmt_hint(stmt, *this, false))) {
      LOG_WARN("failed to init stmt hint", K(ret));
    }
  } else {
    ObStmtHint stmt_hint;
    if (OB_FAIL(stmt_hint.init_stmt_hint(stmt, *this, false))) {
      LOG_WARN("failed to init stmt hint", K(ret));
    } else if (OB_FAIL(stmt.get_stmt_hint().merge_stmt_hint(stmt_hint, RIGHT_HINT_DOMINATED))) {
      LOG_WARN("failed to merge stmt hint", K(ret));
    }
  }

  // set stmt hint
  // if (OB_SUCC(ret) && stmt.is_set_stmt() && OB_FAIL(stmt.get_stmt_hint().set_set_stmt_hint())) {
  //   LOG_WARN("failed to set set stmt hint", K(ret));
  // }
  return ret;
}

int ObQueryHint::generate_orig_stmt_qb_name(ObIAllocator &allocator, int64_t inited_stmt_count)
{
  int ret = OB_SUCCESS;
  char buf[OB_MAX_QB_NAME_LENGTH];
  int64_t buf_len = OB_MAX_QB_NAME_LENGTH;
  ObString qb_name;
  for (int64_t idx = inited_stmt_count; OB_SUCC(ret) && idx < stmt_id_map_.count(); ++idx) {
    QbNames &qb_names = stmt_id_map_.at(idx);
    const char *stmt_name = get_dml_stmt_name(qb_names.stmt_type_, qb_names.is_set_stmt_);
    int64_t pos = 0;
    if (qb_names.is_from_hint_) { /* add qb name from hint */
      if (OB_UNLIKELY(1 != qb_names.qb_names_.count() ||
                      qb_names.qb_names_.at(0).empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected qb names", K(ret), K(qb_names.qb_names_));
      } else if (OB_FAIL(qb_name_map_.set_refactored(qb_names.qb_names_.at(0), idx))) {
        LOG_WARN("failed to set refactored", K(ret));
      }
    } else if (NULL == stmt_name) {
      qb_names.qb_names_.reuse();
    } else if (OB_FAIL(BUF_PRINTF("%s", stmt_name))) {
      LOG_WARN("failed print buf stmt_name", K(ret));
    } else {
      int64_t &id_start = stmt::T_SELECT == qb_names.stmt_type_
                          ? (qb_names.is_set_stmt_ ? set_start_id_ : sel_start_id_)
                          : other_start_id_;
      int64_t old_pos = pos;
      int64_t cnt = 0;
      qb_name.reset();
      qb_names.qb_names_.reuse();
      while(qb_name.empty() && OB_SUCC(ret)) {
        pos = old_pos;
        if (OB_FAIL(BUF_PRINTF("%ld", id_start))) {
          LOG_WARN("failed to print buff", K(ret));
        } else if (OB_FAIL(try_add_new_qb_name(allocator, idx, buf, pos, cnt, qb_name))) {
          LOG_WARN("failed to try add new qb name", K(ret));
        } else {
          ++id_start;
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(qb_names.qb_names_.push_back(qb_name))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

// generate new qb name when transform stmt
int ObQueryHint::generate_qb_name_for_stmt(ObIAllocator &allocator,
                                              const ObDMLStmt &stmt,
                                              const ObString &src_qb_name,
                                              const ObIArray<uint32_t> &src_hash_val,
                                              ObString &qb_name,
                                              int64_t *sub_num /* default = NULL */)
{
  int ret = OB_SUCCESS;
  qb_name.reset();
  char buf[OB_MAX_QB_NAME_LENGTH];
  int64_t buf_len = OB_MAX_QB_NAME_LENGTH;
  int64_t pos = 0;
  const char *stmt_name = get_dml_stmt_name(stmt.get_stmt_type(), stmt.is_set_stmt());
  const int64_t stmt_id = stmt.get_stmt_id();
  uint32_t hash_val = 0;
  if (OB_ISNULL(stmt.get_query_ctx())
      || OB_ISNULL(stmt_name)
      || OB_UNLIKELY(0 > stmt_id || stmt_id_map_.count() <= stmt_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_TRACE("unexpected params", K(ret), K(stmt.get_query_ctx()), K(stmt.get_stmt_type()),
                                   K(stmt_id), K(stmt_id_map_.count()));
  } else if (OB_FAIL(BUF_PRINTF("%s", stmt_name))) {
    LOG_WARN("failed print buf stmt_name", K(ret));
  } else if (OB_FAIL(get_qb_name_source_hash_value(src_qb_name, src_hash_val, hash_val))) {
    LOG_WARN("failed get qb name source hash value", K(ret));
  } else {
    int64_t old_pos = pos;
    int64_t cnt = 0;
    QbNames &qb_names = stmt_id_map_.at(stmt_id);
    while(qb_name.empty() && OB_SUCC(ret)) {
      pos = old_pos;
      if (OB_FAIL(BUF_PRINTF("%08X", hash_val))) {
        LOG_WARN("failed to print buff", K(ret));
      } else if (NULL != sub_num && OB_FAIL(BUF_PRINTF("_%ld", *sub_num))) {
        LOG_WARN("failed to print buff", K(ret));
      } else if (OB_FAIL(try_add_new_qb_name(allocator, stmt.get_stmt_id(),
                                             buf, pos, cnt, qb_name))) {
        LOG_WARN("failed to try add new qb name", K(ret));
      } else if (NULL != sub_num) {
        ++*sub_num;
      } else {
        ++hash_val;
      }
    }

    if (OB_SUCC(ret)) {
      if (qb_names.qb_names_.empty()) {
        qb_names.reset();
        qb_names.stmt_type_ = stmt.get_stmt_type();
        qb_names.is_set_stmt_ = stmt.is_set_stmt();
        qb_names.parent_name_ = src_qb_name;
      }
      if (OB_FAIL(qb_names.qb_names_.push_back(qb_name))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObQueryHint::try_add_new_qb_name(ObIAllocator &allocator,
                                     int64_t stmt_id,
                                     const char *ptr,
                                     int64_t length,
                                     int64_t &cnt,
                                     ObString &qb_name)
{
  int ret = OB_SUCCESS;
  qb_name.reset();
  cnt = cnt < 0 ? 0 : cnt;
  int64_t tmp_stmt_id = OB_INVALID_STMT_ID;
  ObString tmp_name;
  tmp_name.assign_ptr(ptr, length);
  if (OB_FAIL(qb_name_map_.get_refactored(tmp_name, tmp_stmt_id))
      && OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
    LOG_WARN("failed to get refactored", K(ret));
  } else if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
    if (OB_FAIL(ob_write_string(allocator, tmp_name, qb_name))) {
      LOG_WARN("Write string error", K(ret));
    } else if (OB_FAIL(qb_name_map_.set_refactored(qb_name, stmt_id))) {
      LOG_WARN("failed to set refactored", K(ret));
    }
  } else {
    ++cnt;
    if (cnt%50 == 0) {
      LOG_TRACE("try generate qb_name by str too hard", K(cnt));
    }
    if (OB_UNLIKELY(cnt > 1000)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to generate a qb_name by str", K(ret), K(cnt));
    }
  }
  return ret;
}

// reset duplicate qb name from qb_name hint
int ObQueryHint::reset_duplicate_qb_name()
{
  int ret = OB_SUCCESS;
  int64_t idx = 0;
  qb_name_map_.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt_id_map_.count(); ++i) {
    QbNames &qb_names = stmt_id_map_.at(i);
    if (!qb_names.is_from_hint_) {
      qb_names.qb_names_.reuse();
    } else if (qb_names.qb_names_.empty()) {
      /* do nothing */
    } else if (OB_UNLIKELY(1 != qb_names.qb_names_.count() ||
                           qb_names.qb_names_.at(0).empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected qb names", K(ret), K(qb_names.qb_names_));
    } else if (OB_FAIL(qb_name_map_.get_refactored(qb_names.qb_names_.at(0), idx))) {
      if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
        LOG_WARN("get stmt id from hash map failed", K(ret));
      } else if (OB_FAIL(qb_name_map_.set_refactored(qb_names.qb_names_.at(0), i))) {
        LOG_WARN("failed to add name map", K(ret));
      }
    } else if (OB_UNLIKELY(idx < 0 || idx >= i)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected idx.", K(ret), K(idx), K(i));
    } else {
      qb_names.qb_names_.reuse();
      qb_names.is_from_hint_ = false;
      stmt_id_map_.at(idx).qb_names_.reuse();
      stmt_id_map_.at(idx).is_from_hint_ = false;
    }
  }
  qb_name_map_.reuse();
  return ret;
}

const char *ObQueryHint::get_dml_stmt_name(stmt::StmtType stmt_type, bool is_set_stmt) const
{
  switch (stmt_type) {
    case stmt::T_SELECT:
      if (is_set_stmt) {
        return "SET$";
      } else {
        return "SEL$";
      }
    case stmt::T_INSERT_ALL:  return "INS_ALL$";
    case stmt::T_INSERT:  return "INS$";
    case stmt::T_REPLACE: return "REP$";
    case stmt::T_UPDATE:  return "UPD$";
    case stmt::T_DELETE:  return "DEL$";
    case stmt::T_MERGE:   return "MRG$";
    default: return NULL;
  }
}

int ObQueryHint::get_qb_name_counts(const int64_t stmt_count, ObIArray<int64_t> &qb_name_counts) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(stmt_id_map_.count() != stmt_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt id map count", K(stmt_id_map_.count()), K(stmt_count));
  } else if (OB_FAIL(qb_name_counts.prepare_allocate(stmt_count))) {
    LOG_WARN("fail to prepare allocate", K(ret), K(stmt_count));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < stmt_count; ++i) {
      if (OB_UNLIKELY(stmt_id_map_.at(i).qb_names_.empty() &&
                      NULL != get_dml_stmt_name(stmt_id_map_.at(i).stmt_type_, false))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected qb names for stmt id", K(ret), K(i), K(stmt_id_map_.at(i).qb_names_));
      } else {
        qb_name_counts.at(i) = stmt_id_map_.at(i).qb_names_.count();
      }
    }
  }
  return ret;
}

int ObQueryHint::get_qb_name_info(const int64_t stmt_count,
                                  ObIArray<int64_t> &qb_name_counts,
                                  int64_t &sel_start_id,
                                  int64_t &set_start_id,
                                  int64_t &other_start_id) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_qb_name_counts(stmt_count, qb_name_counts))) {
    LOG_WARN("failed to get qb name counts", K(ret));
  } else {
    sel_start_id = sel_start_id_;
    set_start_id = set_start_id_;
    other_start_id = other_start_id_;
  }
  return ret;
}

int ObQueryHint::recover_qb_name_counts(const ObIArray<int64_t> &qb_name_counts, int64_t &stmt_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(stmt_id_map_.count() != stmt_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt id map count", K(stmt_id_map_.count()), K(stmt_count));
  } else {
    int64_t pre_qb_name_count = 0;
    for (int i = stmt_id_map_.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
      pre_qb_name_count = i < qb_name_counts.count() ? qb_name_counts.at(i) : 0;
      ObIArray<ObString> &qb_names = stmt_id_map_.at(i).qb_names_;
      for (int j = qb_names.count() - 1; OB_SUCC(ret) && j >= pre_qb_name_count; --j) {
        if (OB_FAIL(qb_name_map_.erase_refactored(qb_names.at(j)))) {
          LOG_WARN("unexpected qb names for stmt id", K(ret), K(i), K(qb_names.at(j)));
        } else {
          qb_names.pop_back();
        }
      }
      if (OB_SUCC(ret) && i >= qb_name_counts.count()) {
        stmt_id_map_.pop_back();
        --stmt_count;
      }
    }
  }
  return ret;
}

int ObQueryHint::recover_qb_name_info(const ObIArray<int64_t> &qb_name_counts,
                                      int64_t &stmt_count,
                                      int64_t sel_start_id,
                                      int64_t set_start_id,
                                      int64_t other_start_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(recover_qb_name_counts(qb_name_counts, stmt_count))) {
    LOG_WARN("failed to recover qb name counts", K(ret));
  } else {
    sel_start_id_ = sel_start_id;
    set_start_id_ = set_start_id;
    other_start_id_ = other_start_id;
  }
  return ret;
}

int ObQueryHint::print_qb_name_hints(PlanText &plan_text) const
{
  int ret = OB_SUCCESS;
  char *buf = plan_text.buf_;
  int64_t &buf_len = plan_text.buf_len_;
  int64_t &pos = plan_text.pos_;
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt_id_map_.count(); ++i) {
    if (stmt_id_map_.at(i).is_from_hint_) {
      const ObIArray<ObString> &qb_names = stmt_id_map_.at(i).qb_names_;
      if (OB_UNLIKELY(qb_names.empty() || qb_names.at(0).empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected qb name hint", K(ret), K(i), K(stmt_id_map_.at(i)));
      } else if (OB_FAIL(BUF_PRINTF("%sQB_NAME(%.*s)", get_outline_indent(plan_text.is_oneline_),
                                    qb_names.at(0).length(), qb_names.at(0).ptr()))) {
        LOG_WARN("failed to print qb_name hint", K(ret));
      }
    }
  }
  return ret;
}

// Used for stmt printer
// If outline_stmt_id_ is invalid stmt id and has_outline_data(), do not print hint.
//  This may happened for outline data from SPM.
int ObQueryHint::print_stmt_hint(PlanText &plan_text, const ObDMLStmt &stmt,
                                 const bool is_first_stmt_for_hint) const
{
  int ret = OB_SUCCESS;
  const int64_t stmt_id = stmt.get_stmt_id();
  if (OB_FAIL(print_qb_name_hint(plan_text, stmt_id))) {
    LOG_WARN("failed to print qb_name hint", K(ret));
  } else if (OB_INVALID_STMT_ID != stmt_id && stmt_id == outline_stmt_id_) {
    // Outline data resolved from this stmt, print outline data here.
    if (OB_FAIL(print_outline_data(plan_text))) {
      LOG_WARN("failed to print outline data", K(ret));
    }
  } else if (!has_outline_data()) {
    // Not outline data, print current stmt hint here.
    // If stmt is the first stmt can add hint, print global hint and hint with qb name.
    // For query "select_1 union all select_2", root stmt is "union all" and the first stmt to print hint is select_1
    if (is_first_stmt_for_hint &&
        OB_FAIL(get_global_hint().print_global_hint(plan_text))) {
      LOG_WARN("failed to print global hint", K(ret));
    } else if (OB_FAIL(stmt.get_stmt_hint().print_stmt_hint(plan_text))) {
      LOG_WARN("failed to print stmt hint", K(ret));
    } else if (is_first_stmt_for_hint) {
      int tmp = OB_SUCCESS;
      int64_t tmp_stmt_id = OB_INVALID_STMT_ID;
      for (int64_t i = 0; OB_SUCC(ret) && i < qb_hints_.count(); ++i) {
        if (OB_FAIL(qb_name_map_.get_refactored(qb_hints_.at(i).qb_name_, tmp_stmt_id))) {
          if (OB_UNLIKELY(OB_HASH_NOT_EXIST != ret)) {
            LOG_WARN("failed to check hash map exists", K(ret));
          } else if (OB_FAIL(qb_hints_.at(i).print_hints(plan_text))) {
            LOG_WARN("failed to print hint", K(ret));
          }
        }
      }
    }
  }/*  */
  return ret;
}

// used for stmt printer
int ObQueryHint::print_outline_data(PlanText &plan_text) const
{
  int ret = OB_SUCCESS;
  char *buf = plan_text.buf_;
  int64_t &buf_len = plan_text.buf_len_;
  int64_t &pos = plan_text.pos_;
  bool is_oneline = plan_text.is_oneline_;
  if (OB_UNLIKELY(1 < stmt_id_hints_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt id hints for outline data", K(ret), K(stmt_id_hints_));
  } else if (OB_FAIL(BUF_PRINTF("%sBEGIN_OUTLINE_DATA", ObQueryHint::get_outline_indent(is_oneline)))) {
  } else if (1 == stmt_id_hints_.count() &&
             OB_FAIL(stmt_id_hints_.at(0).print_hints(plan_text, true))) {
    LOG_WARN("failed to print hint", K(ret));
  } else {
    const ObHint *hint = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < qb_hints_.count(); ++i) {
      if (OB_FAIL(qb_hints_.at(i).print_hints(plan_text, true))) {
        LOG_WARN("failed to print hint", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < trans_list_.count(); ++i) {
      if (OB_ISNULL(hint = trans_list_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(i), K(trans_list_));
      } else if (OB_FAIL(hint->print_hint(plan_text))) {
        LOG_WARN("failed to print hint", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_global_hint().print_global_hint(plan_text))) {
      LOG_WARN("failed to print global hint", K(ret));
    } else if (OB_FAIL(BUF_PRINTF("%sEND_OUTLINE_DATA", ObQueryHint::get_outline_indent(is_oneline)))) {
      LOG_WARN("fail to print buf", K(ret));
    }
  }
  return ret;
}

int ObQueryHint::print_qb_name_hint(PlanText &plan_text, int64_t stmt_id) const
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_STMT_ID == stmt_id) {
    /* do nothing, this stmt is create for print stmt for mv */
  } else if (OB_UNLIKELY(stmt_id < 0 || stmt_id >= stmt_id_map_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt id", K(ret), K(stmt_id), K(stmt_id_map_.count()));
  } else if (stmt_id_map_.at(stmt_id).is_from_hint_) {
    const ObIArray<ObString> &qb_names = stmt_id_map_.at(stmt_id).qb_names_;
    char *buf = plan_text.buf_;
    int64_t &buf_len = plan_text.buf_len_;
    int64_t &pos = plan_text.pos_;
    if (OB_UNLIKELY(qb_names.empty() || qb_names.at(0).empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected qb name hint", K(ret), K(stmt_id), K(stmt_id_map_.at(stmt_id)));
    } else if (OB_FAIL(BUF_PRINTF("%sQB_NAME(%.*s)", get_outline_indent(plan_text.is_oneline_),
                                  qb_names.at(0).length(), qb_names.at(0).ptr()))) {
      LOG_WARN("failed to print qb_name hint", K(ret));
    }
  }
  return ret;
}

int ObQueryHint::print_transform_hints(PlanText &plan_text) const
{
  int ret = OB_SUCCESS;
  const ObIArray<const ObHint*> *hints = NULL;
  if (plan_text.is_used_hint_) {
    hints = &used_trans_hints_;
  } else {
    hints = &outline_trans_hints_;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < hints->count(); ++i) {
    if (OB_ISNULL(hints->at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret), K(hints->at(i)));
    } else if (OB_FAIL(hints->at(i)->print_hint(plan_text))) {
      LOG_WARN("failed to print transform hint", K(ret), K(*hints->at(i)));
    }
  }
  return ret;
}

int ObQueryHint::get_relids_from_hint_tables(const ObDMLStmt &stmt,
                                             const ObIArray<ObTableInHint> &tables,
                                             ObRelIds &rel_ids) const
{
  int ret = OB_SUCCESS;
  rel_ids.reuse();
  bool is_valid = true;
  int32_t index = OB_INVALID_INDEX;
  for (int64_t i = 0; is_valid && OB_SUCC(ret) && i < tables.count(); ++i) {
    if (OB_FAIL(get_table_bit_index_by_hint_table(stmt, tables.at(i), index))) {
      LOG_WARN("failed to get table bit index by hint table.", K(ret));
    } else if (OB_INVALID_INDEX == index) {
      is_valid = false;
    } else if (OB_FAIL(rel_ids.add_member(index))) {
      LOG_WARN("failed to add members", K(ret));
    }
  }
  if (OB_SUCC(ret) && !is_valid) {
    rel_ids.reuse();
  }
  return ret;
}

int ObQueryHint::get_table_bit_index_by_hint_table(const ObDMLStmt &stmt,
                                                   const ObTableInHint &table,
                                                   int32_t &idx) const
{
  int ret = OB_SUCCESS;
  idx = OB_INVALID_INDEX;
  TableItem *table_item = NULL;
  if (OB_FAIL(get_table_item_by_hint_table(stmt, table, table_item))) {
    LOG_WARN("failed to get table bit index by hint table.", K(ret));
  } else if (NULL == table_item) {
    /* do nothing */
  } else {
    idx = stmt.get_table_bit_index(table_item->table_id_);
  }
  return ret;
}

int ObQueryHint::get_table_item_by_hint_table(const ObDMLStmt &stmt,
                                              const ObTableInHint &table,
                                              TableItem *&table_item) const
{
  int ret = OB_SUCCESS;
  table_item = NULL;
  bool implicit_match_allowed = table.qb_name_.empty();
  const ObIArray<TableItem*> &table_items = stmt.get_table_items();
  TableItem *cur_table_item = NULL;
  TableItem *explicit_matched = NULL;
  TableItem *implicit_matched = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && NULL == explicit_matched && i < table_items.count(); ++i) {
    if (OB_ISNULL(cur_table_item = table_items.at(i)) || OB_UNLIKELY(cur_table_item->qb_name_.empty())) {
      ret = OB_ERR_UNEXPECTED;  // qb_name_ should not be empty
      LOG_WARN("unexpected table item.", K(ret), K(cur_table_item));
    } else if (!table.is_match_table_item(cs_type_, *cur_table_item)) {
      /* do nothing */
    } else if (0 == cur_table_item->qb_name_.case_compare(table.qb_name_)) {
      explicit_matched = cur_table_item;
    } else if (!implicit_match_allowed) {
      /* do nothing */
    } else if (NULL == implicit_matched) {
      implicit_matched = cur_table_item;
    } else {
      implicit_match_allowed = false;
      implicit_matched = NULL;
    }
  }
  if (OB_SUCC(ret)) {
    table_item = NULL != explicit_matched ? explicit_matched : implicit_matched;
    if (NULL == table_item) {
      LOG_TRACE("no table item matched hint table", K(table), K(table_items));
    }
  }
  return ret;
}

int ObQueryHint::get_basic_table_without_index_by_hint_table(const ObDMLStmt &stmt,
                                                             const ObTableInHint &table,
                                                             TableItem *&table_item) const
{
  int ret = OB_SUCCESS;
  table_item = NULL;
  const ObIArray<TableItem*> &table_items = stmt.get_table_items();
  int64_t num = table_items.count();
  TableItem *item = NULL;
  table_item = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && NULL == table_item && i < num; ++i) {
    if (OB_ISNULL(item = table_items.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected table item.", K(ret), K(item));
    } else if (!item->is_basic_table()) {
      /* do nothing */
    } else if (OB_LIKELY(item->ddl_table_id_ != OB_INVALID_ID) && OB_UNLIKELY(item->ddl_table_id_ != item->ref_id_)) {
      // do nothing
    } else if (!table.is_match_physical_table_item(cs_type_, *item)) {
      /* do nothing */
    } else {
      table_item = item;
    }
  }
  return ret;
}

int ObQueryHint::get_qb_name(int64_t stmt_id, ObString &qb_name) const
{
  int ret = OB_SUCCESS;
  qb_name.reset();
  if (OB_UNLIKELY(stmt_id < 0 || stmt_id >= stmt_id_map_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt id", K(ret), K(stmt_id), K(stmt_id_map_.count()), K(stmt_id_map_));
  } else {
    const ObIArray<ObString> &qb_names = stmt_id_map_.at(stmt_id).qb_names_;
    if (OB_UNLIKELY(qb_names.empty() || qb_names.at(qb_names.count() - 1).empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected qb names", K(ret), K(stmt_id), K(stmt_id_map_.at(stmt_id)));
    } else {
      qb_name = qb_names.at(qb_names.count() - 1);
    }
  }
  return ret;
}

int ObQueryHint::fill_tables(const TableItem &table, ObIArray<ObTableInHint> &hint_tables) const
{
  int ret = OB_SUCCESS;
  hint_tables.reuse();
  ObSEArray<const TableItem*, 4> tables;
  if (OB_FAIL(tables.push_back(&table))) {
    LOG_WARN("failed to push back", K(ret));
  } else {
    const TableItem *cur_table = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); ++i) {
      if (OB_ISNULL(cur_table = tables.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL", K(ret), K(i), K(tables));
      } else if (cur_table->is_joined_table()) {
        const JoinedTable *join_table = static_cast<const JoinedTable*>(cur_table);
        if (OB_FAIL(tables.push_back(join_table->left_table_))
            || OB_FAIL(tables.push_back(join_table->right_table_))) {
          LOG_WARN("failed to push back", K(ret));
        }
      } else if (OB_FAIL(hint_tables.push_back(ObTableInHint(cur_table->qb_name_,
                                                             cur_table->database_name_,
                                                             cur_table->get_object_name())))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

bool ObQueryHint::is_valid_outline_transform(int64_t trans_list_loc, const ObHint *cur_hint) const
{
  bool bret = false;
  const ObHint *trans_hint = NULL;
  if (NULL == cur_hint ||
      NULL == (trans_hint = get_outline_trans_hint(trans_list_loc)) ||
      trans_hint->get_orig_hint() != cur_hint->get_orig_hint()) {
    /* do nothing */
  } else {
    bret = true;
  }
  return bret;
}

void QbNames::reset()
{
  stmt_type_ = stmt::StmtType::T_NONE;
  is_set_stmt_ = false;
  is_from_hint_ = false;
  parent_name_.reset();
  qb_names_.reset();
}

int QbNames::print_qb_names(PlanText &plan_text) const
{
  int ret = OB_SUCCESS;
  char *buf = plan_text.buf_;
  int64_t &buf_len = plan_text.buf_len_;
  int64_t &pos = plan_text.pos_;
  int64_t idx = is_from_hint_ ? 1 : 0;
  if (OB_UNLIKELY(qb_names_.empty())) {
    ObString stmt_type = ObResolverUtils::get_stmt_type_string(stmt_type_);
    if (OB_FAIL(BUF_PRINTF("stmt_type:%.*s ", stmt_type.length(), stmt_type.ptr()))) {
      LOG_WARN("failed to print parent stmt type", K(ret));
    }
  } else if (is_from_hint_) {
    if (OB_UNLIKELY(qb_names_.empty() || qb_names_.at(0).empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected qb name hint", K(ret), K(*this));
    } else if (OB_FAIL(BUF_PRINTF("qb_name(%.*s) ",  qb_names_.at(0).length(), qb_names_.at(0).ptr()))) {
      LOG_WARN("failed to print qb_name from hint", K(ret));
    }
  } else if (!parent_name_.empty()) {
    if (OB_FAIL(BUF_PRINTF("parent:%.*s ", parent_name_.length(), parent_name_.ptr()))) {
      LOG_WARN("failed to print parent qb_name", K(ret));
    }
  }

  if (OB_SUCC(ret) && qb_names_.count() > idx) {
    if ((is_from_hint_ || !parent_name_.empty()) && OB_FAIL(BUF_PRINTF(" > "))) {
      LOG_WARN("failed to print parent qb_name", K(ret));
    }
    for (int64_t i = idx; OB_SUCC(ret) && i < qb_names_.count(); ++i) {
      if (OB_FAIL(BUF_PRINTF("%.*s", qb_names_.at(i).length(), qb_names_.at(i).ptr()))) {
        LOG_WARN("failed to print qb_name hint", K(ret));
      } else if ((i < qb_names_.count() - 1) && OB_FAIL(BUF_PRINTF(" > "))) {
        LOG_WARN("failed to print parent qb_name", K(ret));
      }
    }
  }
  return ret;
}

int ObHints::assign(const ObHints &other)
{
  int ret = OB_SUCCESS;
  stmt_id_ = other.stmt_id_;
  qb_name_ = other.qb_name_;
  if (OB_FAIL(hints_.assign(other.hints_))) {
    LOG_WARN("failed to assign hints", K(ret));
  }
  return ret;
}

int ObHints::print_hints(PlanText &plan_text, bool ignore_trans_hint /* default false */ ) const
{
  int ret = OB_SUCCESS;
  const ObHint *hint = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < hints_.count(); ++i) {
    if (OB_ISNULL(hint = hints_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret), K(hint));
    } else if (ignore_trans_hint && hint->is_transform_hint()) {
      /* do nothing */
    } else if (OB_FAIL(hint->print_hint(plan_text))) {
      LOG_WARN("failed to print transform hint", K(ret), K(*hint));
    }
  }
  return ret;
}

void ObStmtHint::reset()
{
  query_hint_ = NULL;
  normal_hints_.reuse();
  other_opt_hints_.reuse();
}

int ObStmtHint::assign(const ObStmtHint &other)
{
  int ret = OB_SUCCESS;
  query_hint_ = other.query_hint_;
  if (OB_FAIL(normal_hints_.assign(other.normal_hints_))) {
    LOG_WARN("failed to assign normal hints", K(ret));
  } else if (OB_FAIL(other_opt_hints_.assign(other.other_opt_hints_))) {
    LOG_WARN("failed to assign other opt hints", K(ret));
  }
  return ret;
}

DEF_TO_STRING(ObStmtHint)
{
  int64_t pos = 0;
  J_OBJ_START();
  const ObHint *hint = NULL;
  const int64_t N = get_hint_count();
  for (int64_t i = 0; i < N; ++i) {
    hint = get_hint_by_idx(i);
    if (NULL != hint) {
      J_KV(K(hint));
      J_COMMA();
    }
  }
  J_OBJ_END();
  return pos;
}

int ObStmtHint::print_stmt_hint(PlanText &plan_text) const
{
  int ret = OB_SUCCESS;
  const ObHint *hint = NULL;
  const int64_t N = get_hint_count();
  const bool ignore_parallel = EXPLAIN_DBLINK_STMT == plan_text.type_;
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    if (OB_ISNULL(hint = get_hint_by_idx(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected NULL", K(ret), K(hint));
    } else if (ignore_parallel && hint->is_table_parallel_hint()) {
      // do nothing
    } else if (OB_FAIL(hint->print_hint(plan_text))) {
      LOG_WARN("failed to print hint", K(ret));
    }
  }
  return ret;
}

int ObStmtHint::init_stmt_hint(const ObDMLStmt &stmt,
                               const ObQueryHint &query_hint,
                               bool use_stmt_id_hints)
{
  int ret = OB_SUCCESS;
  reset();
  set_query_hint(&query_hint);
  ObString qb_name;
  const ObHints *qb_hints = NULL;
  ObSEArray<ObHint*, 16> all_hints;
  if (OB_FAIL(stmt.get_qb_name(qb_name))) {
    LOG_WARN("failed to get qb name", K(ret));
  } else if (NULL != (qb_hints = query_hint.get_qb_hints(qb_name))
             && OB_FAIL(all_hints.assign(qb_hints->hints_))) {
    LOG_WARN("failed to assign hints", K(ret));
  } else if (use_stmt_id_hints
             && NULL != (qb_hints = query_hint.get_stmt_id_hints(stmt.get_stmt_id()))
             && OB_FAIL(append(all_hints, qb_hints->hints_))) {
    LOG_WARN("failed to append hints", K(ret));
  } else {
    ObSEArray<ObItemType, 4> conflict_hints;
    for (int64_t i = 0; OB_SUCC(ret) && i < all_hints.count(); ++i) {
      if (OB_ISNULL(all_hints.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(i), K(all_hints));
      } else if (OB_FAIL(merge_hint(*all_hints.at(i), HINT_DOMINATED_EQUAL, conflict_hints))) {
        LOG_WARN("failed to merge hint", K(ret));
      }
    }
    LOG_TRACE("finish init stmt hint", K(stmt.get_stmt_id()), K(qb_name), K(*this));
  }
  return ret;
}

int ObStmtHint::merge_stmt_hint(const ObStmtHint &other,
                                ObHintMergePolicy policy /* default HINT_DOMINATED_EQUAL */)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObItemType, 4> conflict_hints;
  for (int64_t i = 0; OB_SUCC(ret) && i < other.normal_hints_.count(); ++i) {
    if (OB_ISNULL(other.normal_hints_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(other.normal_hints_));
    } else if (OB_FAIL(merge_normal_hint(*other.normal_hints_.at(i), policy, conflict_hints))) {
      LOG_WARN("failed to merge normal hint", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(append_array_no_dup(other_opt_hints_, other.other_opt_hints_))) {
    LOG_WARN("failed to append other opt hint", K(ret));
  }
  return ret;
}

const ObHint *ObStmtHint::get_normal_hint(ObItemType hint_type, int64_t *idx /* default NULL */) const
{
  const ObHint *hint = NULL;
  if (NULL != idx) {  *idx = -1; }
  for (int64_t i = 0; NULL == hint && i < normal_hints_.count(); ++i) {
    if (OB_ISNULL(normal_hints_.at(i))) {
      /* do nothing */
    } else if (normal_hints_.at(i)->get_hint_type() == hint_type) {
      hint = normal_hints_.at(i);
      if (NULL != idx) {  *idx = i; }
    }
  }
  return hint;
}

ObHint *ObStmtHint::get_normal_hint(ObItemType hint_type, int64_t *idx /* default NULL */)
{
  ObHint *hint = NULL;
  if (NULL != idx) {  *idx = -1; }
  for (int64_t i = 0; NULL == hint && i < normal_hints_.count(); ++i) {
    if (OB_ISNULL(normal_hints_.at(i))) {
      /* do nothing */
    } else if (normal_hints_.at(i)->get_hint_type() == hint_type) {
      hint = normal_hints_.at(i);
      if (NULL != idx) {  *idx = i; }
    }
  }
  return hint;
}

const ObHint *ObStmtHint::get_hint_by_idx(int64_t idx) const
{
  const ObHint *hint = NULL;
  int64_t cur_idx = idx;
  if (cur_idx < normal_hints_.count()) {
    hint = normal_hints_.at(cur_idx);
  } else if ((cur_idx -= normal_hints_.count()) < other_opt_hints_.count()) {
    hint = other_opt_hints_.at(cur_idx);
  } else {
    hint = NULL;
  }
  return hint;
}

int ObStmtHint::set_hint(int64_t idx, ObHint *hint)
{
  int ret = OB_SUCCESS;
  int64_t cur_idx = idx;
  if (OB_UNLIKELY(idx < 0 || idx >= get_hint_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected idx", K(ret), K(idx), K(get_hint_count()));
  } else if (cur_idx < normal_hints_.count()) {
    normal_hints_.at(cur_idx) = hint;
  } else if ((cur_idx -= normal_hints_.count()) < other_opt_hints_.count()) {
    other_opt_hints_.at(cur_idx) = hint;
  }
  return ret;
}

int ObStmtHint::set_set_stmt_hint()
{
  int ret = OB_SUCCESS;
  static const int64_t VALID_HINT_SIZE = 6;
  ObItemType valid_hint[VALID_HINT_SIZE] = { T_NO_REWRITE,
                                             T_SIMPLIFY_SET,
                                             T_PQ_SET,
                                             T_USE_HASH_SET,
                                             T_USE_HASH_DISTINCT,
                                             T_DISTINCT_PUSHDOWN};
  const ObQueryHint *query_hint = query_hint_;
  ObSEArray<ObHint*, 4> hints;
  for (int64_t i = 0; OB_SUCC(ret) && i < normal_hints_.count(); ++i) {
    if (OB_ISNULL(normal_hints_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(i), K(normal_hints_.at(i)));
    } else if (!has_exist_in_array(valid_hint, VALID_HINT_SIZE, normal_hints_.at(i)->get_hint_type())) {
      /* do nothing */
    } else if (OB_FAIL(hints.push_back(normal_hints_.at(i)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    reset();
    query_hint_ = query_hint;
    if (OB_FAIL(normal_hints_.assign(hints))) {
      LOG_WARN("failed to assign hints", K(ret));
    }
  }
  return ret;
}

int ObStmtHint::set_simple_view_hint(const ObStmtHint *other /* default NULL */ )
{
  int ret = OB_SUCCESS;
  static const int64_t RESET_HINT_SIZE = 3;
  ObItemType reset_hint[RESET_HINT_SIZE] = { T_MERGE_HINT, T_USE_HASH_AGGREGATE, T_PLACE_GROUP_BY };
  if (NULL != other && OB_FAIL(assign(*other))) {
    LOG_WARN("failed to assign stmt hint", K(ret));
  } else if (OB_FAIL(remove_normal_hints(reset_hint, RESET_HINT_SIZE))) {
    LOG_WARN("failed to remove hints", K(ret));
  }
  return ret;
}

int ObStmtHint::remove_normal_hints(const ObItemType *hint_array, const int64_t num)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObHint*, 4> hints;
  for (int64_t i = 0; OB_SUCC(ret) && i < normal_hints_.count(); ++i) {
    if (OB_ISNULL(normal_hints_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(i), K(normal_hints_.at(i)));
    } else if (has_exist_in_array(hint_array, num, normal_hints_.at(i)->get_hint_type())) {
      /* do nothing */
    } else if (OB_FAIL(hints.push_back(normal_hints_.at(i)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  if (OB_SUCC(ret) && hints.count() != normal_hints_.count()
      && OB_FAIL(normal_hints_.assign(hints))) {
    LOG_WARN("failed to assign hints", K(ret));
  }
  return ret;
}

int ObStmtHint::replace_name_for_single_table_view(ObIAllocator *allocator,
                                                   const ObDMLStmt &stmt,
                                                   const TableItem &view_table)
{
  int ret = OB_SUCCESS;
  const TableItem *target_table = NULL;
  const ObDMLStmt *child_stmt = NULL;
  if (OB_ISNULL(query_hint_)
      || OB_ISNULL(stmt.get_table_item_by_id(view_table.table_id_))
      || OB_UNLIKELY(!view_table.is_generated_table() && !view_table.is_lateral_table())
      || OB_ISNULL(child_stmt = view_table.ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected params", K(ret), K(query_hint_), K(target_table), K(child_stmt));
  } else if (child_stmt->get_from_item_size() != 1
             || child_stmt->get_from_item(0).is_joined_) {
    /* do nothing */
  } else if (OB_ISNULL(target_table = child_stmt->get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected NULL", K(ret), K(target_table));
  } else if (0 == ObCharset::strcmp(query_hint_->cs_type_, view_table.get_object_name(),
                                    target_table->get_object_name())) {
    /* need not replace name */
  } else {
    LOG_DEBUG("replace name for single table view in hint", K(view_table), K(*target_table));
    const int64_t N = get_hint_count();
    ObHint *hint = NULL;
    ObHint *new_hint = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      if (OB_ISNULL(hint = const_cast<ObHint*>(get_hint_by_idx(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(i));
      } else if (OB_FAIL(hint->create_push_down_hint(allocator, query_hint_->cs_type_,
                                                     view_table, *target_table, new_hint))) {
        LOG_WARN("failed to create push down hint", K(ret), K(i), K(*hint));
      } else if (OB_FAIL(set_hint(i, new_hint))) {
        LOG_WARN("failed to set hint", K(ret), K(i), K(new_hint));
      }
    }
  }
  return ret;
}

// deal hint conflict and add hint
int ObStmtHint::merge_hint(ObHint &hint,
                           ObHintMergePolicy policy,
                           ObIArray<ObItemType> &conflict_hints)
{
  int ret = OB_SUCCESS;
  if (hint.is_access_path_hint()
      || hint.is_join_hint()
      || hint.is_join_filter_hint()
      || hint.is_table_parallel_hint()
      || hint.is_table_dynamic_sampling_hint()
      || hint.is_pq_subquery_hint()) {
    if (OB_FAIL(add_var_to_array_no_dup(other_opt_hints_, &hint))) {
      LOG_WARN("failed to add var to array", K(ret));
    }
  } else if (OB_FAIL(merge_normal_hint(hint, policy, conflict_hints))) {
    LOG_WARN("failed to merge normal hint", K(ret));
  }
  return ret;
}

int ObStmtHint::merge_normal_hint(ObHint &hint,
                                  ObHintMergePolicy policy,
                                  ObIArray<ObItemType> &conflict_hints)
{
  int ret = OB_SUCCESS;
  const ObHint *final_hint = NULL;
  int64_t idx = -1;
  const ObHint *cur_hint = get_normal_hint(hint.get_hint_type(), &idx);
  if (OB_FAIL(hint.merge_hint(cur_hint, &hint, policy, conflict_hints, final_hint))) {
    LOG_WARN("failed to merge hint", K(ret));
  } else if (cur_hint == final_hint) {
    /* do nothing */
  } else if (NULL != cur_hint && NULL == final_hint) {
    if (OB_FAIL(normal_hints_.remove(idx))) {
      LOG_WARN("failed to remove hint", K(ret));
    }
  } else if (NULL == cur_hint && NULL != final_hint) {
    if (OB_FAIL(normal_hints_.push_back(const_cast<ObHint*>(final_hint)))) {
      LOG_WARN("failed to push back", K(ret));
    }
  } else if (NULL != cur_hint && NULL != final_hint) {
    if (OB_FAIL(set_hint(idx, const_cast<ObHint*>(final_hint)))) {
      LOG_WARN("failed to set single hint", K(ret));
    }
  }
  return ret;
}

bool ObStmtHint::has_enable_hint(ObItemType hint_type) const
{
  const ObHint *cur_hint = get_normal_hint(hint_type);
  return NULL != cur_hint && cur_hint->is_enable_hint();
}

bool ObStmtHint::has_disable_hint(ObItemType hint_type) const
{
  const ObHint *cur_hint = get_normal_hint(hint_type);
  return NULL != cur_hint && cur_hint->is_disable_hint();
}

void ObLogPlanHint::reset()
{
  is_outline_data_ = false;
#ifdef OB_BUILD_SPM
  is_spm_evolution_ = false;
#endif
  join_order_.reset();
  table_hints_.reuse();
  join_hints_.reuse();
  normal_hints_.reuse();
  enable_index_prefix_ = false;
}

#ifndef OB_BUILD_SPM
int ObLogPlanHint::init_log_plan_hint(ObSqlSchemaGuard &schema_guard,
                                      const ObDMLStmt &stmt,
                                      const ObQueryHint &query_hint)
#else
int ObLogPlanHint::init_log_plan_hint(ObSqlSchemaGuard &schema_guard,
                                      const ObDMLStmt &stmt,
                                      const ObQueryHint &query_hint,
                                      const bool is_spm_evolution)
#endif
{
  int ret = OB_SUCCESS;
  reset();
  is_outline_data_ = query_hint.has_outline_data();
#ifdef OB_BUILD_SPM
  is_spm_evolution_ = is_spm_evolution;
#endif
  enable_index_prefix_ = (stmt.get_query_ctx()->optimizer_features_enable_version_ >= COMPAT_VERSION_4_2_3);
  const ObStmtHint &stmt_hint = stmt.get_stmt_hint();
  if (OB_FAIL(join_order_.init_leading_info(stmt, query_hint, stmt_hint.get_normal_hint(T_LEADING)))) {
    LOG_WARN("failed to get leading hint info", K(ret));
  } else if (OB_FAIL(init_normal_hints(stmt_hint.normal_hints_))) {
    LOG_WARN("failed to init normal hints", K(ret));
  } else if (OB_FAIL(init_other_opt_hints(schema_guard, stmt, query_hint,
                                          stmt_hint.other_opt_hints_))) {
    LOG_WARN("failed to init other opt hints", K(ret));
  } else {
    LOG_TRACE("finish init log plan hint", K(stmt.get_stmt_id()), K(*this), K(stmt_hint.normal_hints_), K(stmt_hint.other_opt_hints_));
  }
  return ret;
}

int ObLogPlanHint::init_normal_hints(const ObIArray<ObHint*> &normal_hints)
{
  int ret = OB_SUCCESS;
  const ObHint *hint = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < normal_hints.count(); ++i) {
    if (OB_ISNULL(hint = normal_hints.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(i), K(normal_hints));
    } else if (hint->is_transform_hint() || hint->is_join_order_hint()) {
      /* do nothing */
    } else if (OB_FAIL(normal_hints_.push_back(hint))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObLogPlanHint::init_other_opt_hints(ObSqlSchemaGuard &schema_guard,
                                        const ObDMLStmt &stmt,
                                        const ObQueryHint &query_hint,
                                        const ObIArray<ObHint*> &hints)
{
  int ret = OB_SUCCESS;
  const ObHint *hint = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < hints.count(); ++i) {
    if (OB_ISNULL(hint = hints.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), K(i));
    } else if (hint->is_access_path_hint()) {
      if (OB_FAIL(add_index_hint(stmt, query_hint, *static_cast<const ObIndexHint*>(hint)))) {
        LOG_WARN("failed to add index hint", K(ret));
      }
    } else if (hint->is_table_parallel_hint()) {
      if (OB_FAIL(add_table_parallel_hint(stmt, query_hint, *static_cast<const ObTableParallelHint*>(hint)))) {
        LOG_WARN("failed to add table parallel hint", K(ret));
      }
    } else if (hint->is_join_filter_hint()) {
      if (OB_FAIL(add_join_filter_hint(stmt, query_hint, *static_cast<const ObJoinFilterHint*>(hint)))) {
        LOG_WARN("failed to add join filter hint", K(ret));
      }
    } else if (hint->is_join_hint()) {
      if (OB_FAIL(add_join_hint(stmt, query_hint, *static_cast<const ObJoinHint*>(hint)))) {
        LOG_WARN("failed to add join hint", K(ret));
      }
    } else if (hint->is_table_dynamic_sampling_hint()) {
      if (OB_FAIL(add_table_dynamic_sampling_hint(stmt, query_hint,
                                                  *static_cast<const ObTableDynamicSamplingHint*>(hint)))) {
        LOG_WARN("failed to add dynamic sampling hint", K(ret));
      }
    } else if (hint->is_pq_subquery_hint()) {
      if (OB_FAIL(normal_hints_.push_back(hint))) {
        LOG_WARN("failed to push back", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected hint type in other_opt_hints_", K(ret), K(*hint));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_log_table_hints(schema_guard))) {
    LOG_WARN("failed to init log table hints", K(ret));
  } else if (OB_FAIL(init_log_join_hints())) {
    LOG_WARN("failed to init log join hints", K(ret));
  }
  return ret;
}

// init single table hints:
//  1. index hints;
//  2. imc hints;
//  3. table parallel hints;
//  4. dynamic sampling hint;
int ObLogPlanHint::init_log_table_hints(ObSqlSchemaGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  int64_t valid_cnt = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_hints_.count(); ++i) {
    if (OB_FAIL(table_hints_.at(i).init_index_hints(schema_guard))) {
      LOG_WARN("failed to init index hint for table.", K(ret));
    } else if (!table_hints_.at(i).is_valid()) {
      /* do nothing */
    } else if (valid_cnt != i && OB_FAIL(table_hints_.at(valid_cnt).assign(table_hints_.at(i)))) {
      LOG_WARN("failed to assign log index hint.", K(ret));
    } else {
      ++valid_cnt;
    }
  }
  if (OB_SUCC(ret)) {
    ObOptimizerUtil::revert_items(table_hints_, valid_cnt);
  }
  return ret;
}

int ObLogPlanHint::add_index_hint(const ObDMLStmt &stmt,
                                  const ObQueryHint &query_hint,
                                  const ObIndexHint &index_hint)
{
  int ret = OB_SUCCESS;
  LogTableHint *log_table_hint = NULL;
  if (OB_FAIL(get_log_table_hint_for_update(stmt, query_hint, index_hint.get_table(),
                                            true, log_table_hint))) {
    LOG_WARN("failed to get log table hint by hint", K(ret));
  } else if (NULL == log_table_hint) {
    /* do nothing */
  } else if (T_USE_DAS_HINT == index_hint.get_hint_type()) {
    if (NULL == log_table_hint->use_das_hint_ || index_hint.is_enable_hint()) {
      log_table_hint->use_das_hint_ = &index_hint;
    }
  } else if (T_USE_COLUMN_STORE_HINT == index_hint.get_hint_type()) {
    if (NULL == log_table_hint->use_column_store_hint_ || index_hint.is_enable_hint()) {
      log_table_hint->use_column_store_hint_ = &index_hint;
    }
  } else if (OB_FAIL(log_table_hint->index_hints_.push_back(&index_hint))) {
    LOG_WARN("failed to push back", K(ret));
  }
  return ret;
}

int ObLogPlanHint::add_table_parallel_hint(const ObDMLStmt &stmt,
                                           const ObQueryHint &query_hint,
                                           const ObTableParallelHint &table_parallel_hint)
{
  int ret = OB_SUCCESS;
  LogTableHint *log_table_hint = NULL;
  if (OB_FAIL(get_log_table_hint_for_update(stmt, query_hint, table_parallel_hint.get_table(),
                                            true, log_table_hint))) {
    LOG_WARN("failed to get log table hint by hint", K(ret));
  } else if (NULL == log_table_hint) {
    /* do nothing */
  } else if (NULL == log_table_hint->parallel_hint_ ||
             table_parallel_hint.get_parallel() > log_table_hint->parallel_hint_->get_parallel()) {
    log_table_hint->parallel_hint_ = &table_parallel_hint;
  }
  return ret;
}

int ObLogPlanHint::add_table_dynamic_sampling_hint(const ObDMLStmt &stmt,
                                                   const ObQueryHint &query_hint,
                                                   const ObTableDynamicSamplingHint &table_ds_hint)
{
  int ret = OB_SUCCESS;
  LogTableHint *log_table_hint = NULL;
  if (OB_FAIL(get_log_table_hint_for_update(stmt, query_hint, table_ds_hint.get_table(),
                                            true, log_table_hint))) {
    LOG_WARN("failed to get log table hint by hint", K(ret));
  } else if (NULL == log_table_hint) {
    /* do nothing */
  } else if (NULL == log_table_hint->dynamic_sampling_hint_ && !log_table_hint->is_ds_hint_conflict_) {
    log_table_hint->dynamic_sampling_hint_ = &table_ds_hint;
  } else if (log_table_hint->dynamic_sampling_hint_->get_dynamic_sampling() != table_ds_hint.get_dynamic_sampling() ||
             log_table_hint->dynamic_sampling_hint_->get_sample_block_cnt() != table_ds_hint.get_sample_block_cnt()) {
    //conflict will cause reset origin state compatible Oracle.
    log_table_hint->dynamic_sampling_hint_ = NULL;
    log_table_hint->is_ds_hint_conflict_ = true;
  }
  return ret;
}

int ObLogPlanHint::add_join_filter_hint(const ObDMLStmt &stmt,
                                        const ObQueryHint &query_hint,
                                        const ObJoinFilterHint &join_filter_hint)
{
  int ret = OB_SUCCESS;
  LogTableHint *log_table_hint = NULL;
  if (OB_FAIL(get_log_table_hint_for_update(stmt, query_hint, join_filter_hint.get_filter_table(),
                                            false, log_table_hint))) {
    LOG_WARN("failed to get log table hint by hint", K(ret));
  } else if (NULL == log_table_hint) {
    /* do nothing */
  } else if (OB_FAIL(log_table_hint->add_join_filter_hint(stmt, query_hint, join_filter_hint))) {
    LOG_WARN("failed to add join filter hint", K(ret));
  }
  return ret;
}

int ObLogPlanHint::get_log_table_hint_for_update(const ObDMLStmt &stmt,
                                                 const ObQueryHint &query_hint,
                                                 const ObTableInHint &table,
                                                 const bool basic_table_only,
                                                 LogTableHint *&log_table_hint)
{
  int ret = OB_SUCCESS;
  TableItem *table_item = NULL;
  log_table_hint = NULL;
  if (OB_FAIL(query_hint.get_table_item_by_hint_table(stmt, table, table_item))) {
    LOG_WARN("failed to get table item by hint table", K(ret));
  } else if (NULL != table_item && (table_item->is_basic_table() || !basic_table_only)) {
    for (int64_t i = 0; NULL == log_table_hint && i < table_hints_.count(); ++i) {
      if (OB_ISNULL(table_hints_.at(i).table_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(i), K(table_hints_));
      } else if (table_hints_.at(i).table_->table_id_ == table_item->table_id_) {
        log_table_hint = &table_hints_.at(i);
      }
    }
    if (NULL == log_table_hint) {
      if (OB_ISNULL(log_table_hint = table_hints_.alloc_place_holder())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Allocate log index hint from array error", K(ret));
      } else {
        log_table_hint->table_ = table_item;
      }
    }
  }
  return ret;
}

int ObLogPlanHint::add_join_hint(const ObDMLStmt &stmt,
                                 const ObQueryHint &query_hint,
                                 const ObJoinHint &join_hint)
{
  int ret = OB_SUCCESS;
  LogJoinHint *log_join_hint = NULL;
  ObRelIds join_tables;
  if (OB_FAIL(query_hint.get_relids_from_hint_tables(stmt, join_hint.get_tables(), join_tables))) {
    LOG_WARN("failed to get relids from hint tables", K(ret), K(join_hint.get_tables()));
  } else if (join_tables.is_empty()) {
    LOG_TRACE("get invalid join hint", K(ret), K(join_tables), K(join_hint));
  } else {
    LOG_TRACE("get valid join hint", K(join_hint));
    log_join_hint = NULL;
    for (int64_t i = 0; NULL == log_join_hint && i < join_hints_.count(); ++i) {
      if (join_tables.equal(join_hints_.at(i).join_tables_)) {
        log_join_hint = &join_hints_.at(i);
      }
    }
    if (NULL == log_join_hint) {
      if (OB_ISNULL(log_join_hint = join_hints_.alloc_place_holder())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("Allocate log join hint from array error", K(ret));
      } else if (OB_FAIL(log_join_hint->join_tables_.add_members(join_tables))) {
        LOG_WARN("failed to add members", K(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(log_join_hint->add_join_hint(join_hint))) {
      LOG_WARN("failed to add join hint", K(ret), K(join_hint));
    }
  }
  return ret;
}

// init log join hint for use join hint
int ObLogPlanHint::init_log_join_hints()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < join_hints_.count(); ++i) {
    if (OB_FAIL(join_hints_.at(i).init_log_join_hint())) {
      LOG_WARN("failed to init log join hint", K(ret));
    }
  }
  return ret;
}

const ObHint *ObLogPlanHint::get_normal_hint(ObItemType hint_type) const
{
  const ObHint *hint = NULL;
  for (int64_t i = 0; NULL == hint && i < normal_hints_.count(); ++i) {
    if (OB_ISNULL(normal_hints_.at(i))) {
      /* do nothing */
    } else if (normal_hints_.at(i)->get_hint_type() == hint_type) {
      hint = normal_hints_.at(i);
    }
  }
  return hint;
}

bool ObLogPlanHint::has_enable_hint(ObItemType hint_type) const
{
  const ObHint *cur_hint = get_normal_hint(hint_type);
  return NULL != cur_hint && cur_hint->is_enable_hint();
}

bool ObLogPlanHint::has_disable_hint(ObItemType hint_type) const
{
  const ObHint *cur_hint = get_normal_hint(hint_type);
  return NULL != cur_hint ? cur_hint->is_disable_hint() : is_outline_data_;
}

int ObLogPlanHint::get_aggregation_info(bool &force_use_hash,
                                        bool &force_use_merge,
                                        bool &force_part_sort,
                                        bool &force_normal_sort) const
{
  int ret = OB_SUCCESS;
  force_use_hash = false;
  force_use_merge = false;
  force_part_sort = false;
  force_normal_sort = false;
  const ObAggHint *agg_hint = static_cast<const ObAggHint*>(get_normal_hint(T_USE_HASH_AGGREGATE));
  if (NULL != agg_hint) {
    force_use_hash = agg_hint->is_enable_hint();
    force_use_merge = agg_hint->is_disable_hint();
    force_part_sort = agg_hint->force_partition_sort();
    force_normal_sort = agg_hint->force_normal_sort();
    if (force_use_merge && !force_part_sort && !force_normal_sort && is_outline_data_) {
      force_normal_sort = true;
    }
  } else if (is_outline_data_) {
    force_use_merge = true;
    force_normal_sort = true;
  }
  return ret;
}

const ObWindowDistHint *ObLogPlanHint::get_window_dist() const
{
  return static_cast<const ObWindowDistHint*>(
      get_normal_hint(T_PQ_DISTRIBUTE_WINDOW));
}


const LogTableHint *ObLogPlanHint::get_log_table_hint(uint64_t table_id) const
{
  const LogTableHint *log_table_hint = NULL;
  for (int64_t i = 0; NULL == log_table_hint && i < table_hints_.count(); ++i) {
    if (NULL != table_hints_.at(i).table_
        && table_hints_.at(i).table_->table_id_ == table_id) {
      log_table_hint = &table_hints_.at(i);
    }
  }
  return log_table_hint;
}

const LogTableHint* ObLogPlanHint::get_index_hint(uint64_t table_id) const
{
  const LogTableHint *log_table_hint = get_log_table_hint(table_id);
  return NULL != log_table_hint && !log_table_hint->index_hints_.empty() ? log_table_hint : NULL;
}

int64_t ObLogPlanHint::get_parallel(uint64_t table_id) const
{
  int64_t table_parallel = ObGlobalHint::UNSET_PARALLEL;
  const LogTableHint *log_table_hint = get_log_table_hint(table_id);
  if (NULL == log_table_hint || NULL == log_table_hint->parallel_hint_) {
    table_parallel = is_outline_data_ ? ObGlobalHint::DEFAULT_PARALLEL : ObGlobalHint::UNSET_PARALLEL;
  } else {
    table_parallel = log_table_hint->parallel_hint_->get_parallel();
  }
  return table_parallel;
}

int ObLogPlanHint::check_use_das(uint64_t table_id, bool &force_das, bool &force_no_das) const
{
  int ret = OB_SUCCESS;
  force_das = false;
  force_no_das = false;
  const LogTableHint *log_table_hint = get_log_table_hint(table_id);
  const ObHint *hint = NULL == log_table_hint ? NULL : log_table_hint->use_das_hint_;
  if (NULL != hint) {
    force_das = hint->is_enable_hint();
    force_no_das = hint->is_disable_hint();
  } else if (is_outline_data_) {
    force_no_das = true;
  }
  return ret;
}

int ObLogPlanHint::check_use_column_store(uint64_t table_id, bool &force_column_store, bool &force_no_column_store) const
{
  int ret = OB_SUCCESS;
  force_column_store = false;
  force_no_column_store = false;
  const LogTableHint *log_table_hint = get_log_table_hint(table_id);
  const ObHint *hint = NULL == log_table_hint ? NULL : log_table_hint->use_column_store_hint_;
  if (NULL != hint) {
    force_column_store = hint->is_enable_hint();
    force_no_column_store = hint->is_disable_hint();
  } else if (is_outline_data_) {
    force_no_column_store = true;
  }
  return ret;
}

int ObLogPlanHint::check_use_skip_scan(uint64_t table_id,
                                       uint64_t index_id,
                                       bool &force_skip_scan,
                                       bool &force_no_skip_scan) const
{
  int ret = OB_SUCCESS;
  force_skip_scan = false;
  force_no_skip_scan = false;
  const LogTableHint *log_table_hint = get_log_table_hint(table_id);
  int64_t pos = OB_INVALID_INDEX;
  if (NULL != log_table_hint &&
      ObOptimizerUtil::find_item(log_table_hint->index_list_, index_id, &pos)) {
    const ObIndexHint *hint = NULL;
    if (OB_UNLIKELY(pos >= log_table_hint->index_hints_.count() || pos < 0)
        || OB_ISNULL(hint = log_table_hint->index_hints_.at(pos))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected pos", K(ret), K(pos), K(log_table_hint->index_hints_.count()), K(hint));
    } else {
      force_skip_scan = hint->use_skip_scan();
      force_no_skip_scan = !force_skip_scan && hint->is_use_index_hint();
    }
  }
  if (OB_SUCC(ret) && !force_skip_scan && !force_no_skip_scan && is_outline_data_) {
    force_no_skip_scan = true;
  }
  return ret;
}

const ObTableDynamicSamplingHint *ObLogPlanHint::get_dynamic_sampling_hint(uint64_t table_id) const
{
  const LogTableHint *log_table_hint = get_log_table_hint(table_id);
  return NULL == log_table_hint ? NULL : log_table_hint->dynamic_sampling_hint_;
}

int ObLogPlanHint::check_use_join_filter(uint64_t filter_table_id,
                                         const ObRelIds &left_tables,
                                         bool part_join_filter,
                                         bool config_disable,
                                         bool &can_use,
                                         const ObJoinFilterHint *&force_hint) const
{
  int ret = OB_SUCCESS;
  const LogTableHint *log_table_hint = get_log_table_hint(filter_table_id);
  const ObJoinFilterHint *hint = NULL;
  if (NULL != log_table_hint &&
      OB_FAIL(log_table_hint->get_join_filter_hint(left_tables, part_join_filter, hint))) {
    LOG_WARN("failed to get join filter hint", K(ret));
  } else if (NULL != hint) {
    can_use = hint->is_enable_hint();
    force_hint = can_use ? hint : NULL;
  } else if (is_outline_data_) {
    can_use = false;
    force_hint = NULL;
  } else {
    can_use = !config_disable;
    force_hint = NULL;
  }
  return ret;
}

int ObLogPlanHint::get_pushdown_join_filter_hints(uint64_t filter_table_id,
                                                  const ObRelIds &left_tables,
                                                  bool config_disable,
                                                  JoinFilterPushdownHintInfo& info) const
{
  int ret = OB_SUCCESS;
  const LogTableHint *log_table_hint = get_log_table_hint(filter_table_id);
  info.filter_table_id_ = filter_table_id;
  info.config_disable_ = config_disable;
  if (NULL == log_table_hint) {
  } else if(OB_FAIL(log_table_hint->get_join_filter_hints(left_tables, false,
                                                          info.join_filter_hints_))) {
    LOG_WARN("failed to get join filter hints", K(ret));
  } else if (OB_FAIL(log_table_hint->get_join_filter_hints(left_tables, true,
                                                          info.part_join_filter_hints_))) {
    LOG_WARN("failed to get join filter hints", K(ret));
  }
  return ret;
}

const LogJoinHint *ObLogPlanHint::get_join_hint(const ObRelIds &join_tables) const
{
  const LogJoinHint *log_join_hint = NULL;
  for (int64_t i = 0; NULL == log_join_hint && i < join_hints_.count(); ++i) {
    if (join_tables.equal(join_hints_.at(i).join_tables_)) {
      log_join_hint = &join_hints_.at(i);
    }
  }
  return log_join_hint;
}

SetAlgo ObLogPlanHint::get_valid_set_algo() const
{
  SetAlgo set_algo = INVALID_SET_ALGO;  // default valid, can use MERGE_SET and HASH_SET
  const ObHint *cur_hint = get_normal_hint(T_USE_HASH_SET);
  if (NULL == cur_hint) {
    set_algo = is_outline_data_ ? MERGE_SET : INVALID_SET_ALGO;
  } else if (cur_hint->is_enable_hint()) {
    set_algo = HASH_SET;
  } else if (cur_hint->is_disable_hint()) {
    set_algo = MERGE_SET;
  }
  return set_algo;
}

DistAlgo ObLogPlanHint::get_valid_set_dist_algo(int64_t *random_none_idx /* default NULL */ ) const
{
  DistAlgo set_dist_algo = DistAlgo::DIST_INVALID_METHOD;
  const ObPQSetHint *pq_set_hint = static_cast<const ObPQSetHint*>(get_normal_hint(T_PQ_SET));
  if (NULL == pq_set_hint) {
    set_dist_algo = is_outline_data_ ? DistAlgo::DIST_BASIC_METHOD
                                     : DistAlgo::DIST_INVALID_METHOD;
  } else {
    int64_t idx = OB_INVALID_INDEX;
    set_dist_algo = pq_set_hint->get_dist_algo(idx);
    if (NULL != random_none_idx) {
      *random_none_idx = idx;
    }
  }
  return set_dist_algo;
}

int ObLogPlanHint::get_index_prefix(const uint64_t table_id,
                                    const uint64_t index_id,
                                    int64_t &index_prefix) const
{
  int ret = OB_SUCCESS;
  const LogTableHint *log_table_hint = NULL;
  if (!enable_index_prefix_ || OB_ISNULL(log_table_hint = get_index_hint(table_id))) {
    //do nothing
  } else if (!log_table_hint->is_use_index_hint()) {
    //do nothing
  } else if (OB_FAIL(log_table_hint->get_index_prefix(index_id, index_prefix))) {
    LOG_WARN("fail to get index prefix", K(ret));
  }
  return ret;
}
int ObLogPlanHint::check_valid_set_left_branch(const ObSelectStmt *select_stmt,
                                               bool &hint_valid,
                                               bool &need_swap) const
{
  int ret = OB_SUCCESS;
  hint_valid = false;
  need_swap = false;
  const ObPQSetHint *pq_set_hint = NULL;
  ObString left_child;
  ObString right_child;
  if (OB_UNLIKELY(!select_stmt->is_set_stmt()
                  || !select_stmt->is_set_distinct()
                  || 2 > select_stmt->get_set_query().count())
      || OB_ISNULL(select_stmt->get_set_query(0))
      || OB_ISNULL(select_stmt->get_set_query(1))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected set_op stmt", K(ret), K(select_stmt));
  } else if (NULL == (pq_set_hint = static_cast<const ObPQSetHint*>(get_normal_hint(T_PQ_SET)))) {
    hint_valid = is_outline_data_;
    need_swap = false;
  } else if (pq_set_hint->get_left_branch().empty()) {
    hint_valid = true;
    need_swap = false;
  } else if (OB_FAIL(select_stmt->get_set_query(0)->get_qb_name(left_child))
             || OB_FAIL(select_stmt->get_set_query(1)->get_qb_name(right_child))) {
    LOG_WARN("failed to get qb name", K(ret));
  } else if (0 == pq_set_hint->get_left_branch().case_compare(left_child)) {
    hint_valid = true;
    need_swap = false;
  } else if (0 == pq_set_hint->get_left_branch().case_compare(right_child)) {
    hint_valid = true;
    need_swap = true;
  } else {
    hint_valid = false;
    need_swap = false;
  }
  return ret;
}

int ObLogPlanHint::get_valid_pq_subquery_hint(const ObIArray<ObString> &sub_qb_names,
                                              const ObPQSubqueryHint *&explicit_hint,
                                              const ObPQSubqueryHint *&implicit_hint) const
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; NULL == explicit_hint && i < normal_hints_.count(); ++i) {
    if (NULL != normal_hints_.at(i) && normal_hints_.at(i)->get_hint_type() == T_PQ_SUBQUERY) {
      const ObPQSubqueryHint *cur_hint = static_cast<const ObPQSubqueryHint*>(normal_hints_.at(i));
      if (NULL == explicit_hint && cur_hint->is_match_subplans(sub_qb_names)) {
        explicit_hint = cur_hint;
      } else if (NULL == implicit_hint && cur_hint->get_sub_qb_names().empty()) {
        implicit_hint = cur_hint;
      }
    }
  }
  return ret;
}

DistAlgo ObLogPlanHint::get_valid_pq_subquery_dist_algo(const ObIArray<ObString> &sub_qb_names,
                                                        const bool implicit_allowed) const
{
  int ret = OB_SUCCESS; // ignore this ret
  DistAlgo dist_algo = DistAlgo::DIST_INVALID_METHOD;
  const ObPQSubqueryHint *explicit_hint = NULL;
  const ObPQSubqueryHint *implicit_hint = NULL;
  if (OB_FAIL(get_valid_pq_subquery_hint(sub_qb_names, explicit_hint, implicit_hint))) {
    LOG_WARN("failed to get valid subplan filter hint", K(ret));
  } else if (NULL != explicit_hint) {
    dist_algo = explicit_hint->get_dist_algo();
  } else if (is_outline_data_) {
    dist_algo = DistAlgo::DIST_BASIC_METHOD;
  } else if (NULL != implicit_hint && implicit_allowed) {
    dist_algo = implicit_hint->get_dist_algo();
  }
  return dist_algo;
}

// generate spm evolution plan, throw a error code when can not get valid hint
int ObLogPlanHint::check_status() const
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SPM
  if (is_spm_evolution_) {
    ret = OB_OUTLINE_NOT_REPRODUCIBLE;
    LOG_WARN("failed to generate plan use outline data for spm evolution", K(ret));
  }
#endif
  return ret;
}

bool ObLogPlanHint::is_spm_evolution() const
{
  bool bret = false;
#ifdef OB_BUILD_SPM
  bret = is_spm_evolution_;
#endif
  return bret;
}

int LogLeadingHint::init_leading_info(const ObDMLStmt &stmt,
                                      const ObQueryHint &query_hint,
                                      const ObHint *hint)
{
  int ret = OB_SUCCESS;
  reset();
  if (NULL == hint) {
    /* do nothing */
  } else if (OB_UNLIKELY(!hint->is_join_order_hint())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect hint type", K(ret), K(*hint));
  } else {
    ObRelIds table_set;
    hint_ = static_cast<const ObJoinOrderHint*>(hint);
    if (hint_->is_ordered_hint() &&
        OB_FAIL(init_leading_info_from_ordered_hint(stmt))) {
      LOG_TRACE("failed to init leading info from ordered hint.", K(ret));
    } else if (!hint_->is_ordered_hint() &&
               OB_FAIL(init_leading_info_from_leading_hint(stmt, query_hint, hint_->get_table(), table_set))) {
      LOG_TRACE("failed to init leading info from leading hint.", K(ret));
    } else if (NULL == hint_) {
      leading_tables_.reuse();
      leading_infos_.reuse();
    } else {
      LOG_TRACE("succeed to get leading infos", K(*this));
    }
  }
  return ret;
}

int LogLeadingHint::init_leading_info_from_leading_hint(const ObDMLStmt &stmt,
                                                        const ObQueryHint &query_hint,
                                                        const ObLeadingTable &cur_table,
                                                        ObRelIds& table_set)
{
  int ret = OB_SUCCESS;
  LeadingInfo leading_info;
  if (NULL != cur_table.table_) {
    int32_t index = OB_INVALID_INDEX;
    if (OB_FAIL(query_hint.get_table_bit_index_by_hint_table(stmt, *cur_table.table_, index))) {
      LOG_WARN("failed to get table id", K(ret));
    } else if (OB_INVALID_INDEX == index || leading_tables_.has_member(index)) {
      hint_ = NULL;
      LOG_WARN("failed to get table bit index by hint table", K(index), K(*cur_table.table_),
                                                              K(stmt.get_table_items()));
    } else if (OB_FAIL(leading_tables_.add_member(index))
               || OB_FAIL(table_set.add_member(index))) {
      LOG_WARN("failed to add members", K(ret));
    }
  } else if (OB_ISNULL(cur_table.left_table_) || OB_ISNULL(cur_table.right_table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null.", K(ret), K(cur_table.left_table_), K(cur_table.right_table_));
  } else if (OB_FAIL(SMART_CALL(init_leading_info_from_leading_hint(stmt, query_hint,
                                                                    *cur_table.left_table_,
                                                                    leading_info.left_table_set_)))) {
    LOG_WARN("failed to init leading info from leading", K(ret));
  } else if (NULL == hint_) {
    /* do nothing */
  } else if (OB_FAIL(SMART_CALL(init_leading_info_from_leading_hint(stmt, query_hint,
                                                                    *cur_table.right_table_,
                                                                    leading_info.right_table_set_)))) {
    LOG_WARN("failed to init leading info from leading", K(ret));
  } else if (NULL == hint_) {
    /* do nothing */
  } else if (OB_FAIL(leading_info.table_set_.add_members(leading_info.left_table_set_))
             || OB_FAIL(leading_info.table_set_.add_members(leading_info.right_table_set_))) {
    LOG_WARN("failed to add table ids", K(ret));
  } else if (OB_FAIL(leading_infos_.push_back(leading_info))) {
    LOG_WARN("failed to push back hint info", K(ret));
  } else if (OB_FAIL(table_set.add_members(leading_info.table_set_))) {
    LOG_WARN("failed to add table ids", K(ret));
  }
  return ret;
}

/**
 * for ordered hint, init leading hint info by the ordering of tables in stmt
 */
int LogLeadingHint::init_leading_info_from_ordered_hint(const ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;
  const ObIArray<FromItem> &from_items = stmt.get_from_items();
  const ObIArray<SemiInfo*> &semi_infos = stmt.get_semi_infos();
  SemiInfo *semi_info = NULL;
  ObRelIds leading_tables;
  const int64_t N = from_items.count() + semi_infos.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    LeadingInfo leading_info;
    if (i < from_items.count()) {
      const FromItem &from_item = from_items.at(i);
      TableItem *table_item = from_item.is_joined_
                              ? stmt.get_joined_table(from_item.table_id_)
                              : stmt.get_table_item_by_id(from_item.table_id_);
      if (OB_FAIL(init_leading_info_from_table(stmt, leading_infos_,
                                               table_item, leading_info.right_table_set_))) {
        LOG_WARN("failed to init leading infos from table.", K(ret));
      }
    } else if (OB_ISNULL(semi_info = semi_infos.at(i - from_items.count()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null semi info", K(ret));
    } else if (OB_FAIL(leading_info.right_table_set_.add_member(
                                    stmt.get_table_bit_index(semi_info->right_table_id_)))) {
      LOG_WARN("failed to add members", K(ret));
    }

    if (i == 0) {
      ret = leading_tables.add_members(leading_info.right_table_set_);
    } else if (OB_FAIL(leading_info.left_table_set_.add_members(leading_tables))) {
      LOG_WARN("failed to add table ids", K(ret));
    } else if (OB_FAIL(leading_tables.add_members(leading_info.right_table_set_))) {
      LOG_WARN("failed to get table ids", K(ret));
    } else if (OB_FAIL(leading_info.table_set_.add_members(leading_tables))) {
      LOG_WARN("failed to add table ids", K(ret));
    } else if (OB_FAIL(leading_infos_.push_back(leading_info))) {
      LOG_WARN("failed to push back hint info", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(leading_tables_.add_members(leading_tables))) {
    LOG_WARN("failed to get table ids", K(ret));
  }
  return ret;
}

/**
 * init leading hint info for table item
 */
int LogLeadingHint::init_leading_info_from_table(const ObDMLStmt &stmt,
                                                  ObIArray<LeadingInfo> &leading_infos,
                                                  TableItem *table,
                                                  ObRelIds &table_set)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null table item", K(ret));
  } else if (!table->is_joined_table()) {
    if (OB_FAIL(table_set.add_member(stmt.get_table_bit_index(table->table_id_)))) {
      LOG_WARN("failed to add members", K(ret));
    }
  } else {
    LeadingInfo leading_info;
    JoinedTable *joined_table = static_cast<JoinedTable*>(table);
    if (OB_FAIL(SMART_CALL(init_leading_info_from_table(stmt, leading_infos,
                                                        joined_table->left_table_,
                                                        leading_info.left_table_set_)))) {
      LOG_WARN("failed to get leading hint info", K(ret));
    } else if (OB_FAIL(SMART_CALL(init_leading_info_from_table(stmt, leading_infos,
                                                               joined_table->right_table_,
                                                               leading_info.right_table_set_)))) {
      LOG_WARN("failed to get leading hint info", K(ret));
    } else if (OB_FAIL(leading_info.table_set_.add_members(leading_info.left_table_set_))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(leading_info.table_set_.add_members(leading_info.right_table_set_))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(table_set.add_members(leading_info.table_set_))) {
      LOG_WARN("failed to add members", K(ret));
    } else if (OB_FAIL(leading_infos.push_back(leading_info))) {
      LOG_WARN("failed to push back hint info", K(ret));
    }
  }
  return ret;
}

int LogJoinHint::assign(const LogJoinHint &other)
{
  int ret = OB_SUCCESS;
  local_methods_ = other.local_methods_;
  dist_methods_ = other.dist_methods_;
  slave_mapping_ = other.slave_mapping_;
  nl_material_ = other.nl_material_;
  join_tables_ = other.join_tables_;
  if (OB_FAIL(local_method_hints_.assign(other.local_method_hints_))) {
    LOG_WARN("fail to assign local method hints", K(ret));
  } else if (OB_FAIL(dist_method_hints_.assign(other.dist_method_hints_))) {
    LOG_WARN("fail to assign dist method hints", K(ret));
  }
  return ret;
}

int LogJoinHint::add_join_hint(const ObJoinHint &join_hint)
{
  int ret = OB_SUCCESS;
  switch(join_hint.get_hint_type()) {
    case T_USE_NL:
    case T_USE_MERGE:
    case T_USE_HASH:  {
      if (OB_FAIL(local_method_hints_.push_back(&join_hint))) {
        LOG_WARN("fail to push back join hint", K(ret), K(join_hint));
      }
      break;
    }
    case T_USE_NL_MATERIALIZATION:  {
      //if both use_material and no_use_material, no_use_material worked.
      if (NULL == nl_material_ || (join_hint.is_disable_hint() && nl_material_->is_enable_hint())) {
        nl_material_ = &join_hint;
      }
      break;
    }
    case T_PQ_DISTRIBUTE: {
      if (dist_methods_ & join_hint.get_dist_algo()) {
        /* do nothing */
      } else if (OB_FAIL(dist_method_hints_.push_back(&join_hint))) {
        LOG_WARN("fail to push back join hint", K(ret), K(join_hint));
      } else {
        dist_methods_ |= join_hint.get_dist_algo();
      }
      break;
    }
    case T_PQ_MAP:  {
      if (NULL == slave_mapping_) {
        slave_mapping_ = &join_hint;
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected join hint type", K(ret), K(join_hint));
    }
  }
  return ret;
}

int LogJoinHint::init_log_join_hint()
{
  int ret = OB_SUCCESS;
  local_methods_ = JoinAlgo::INVALID_JOIN_ALGO;
  ObSEArray<const ObJoinHint*, 4> all_hints;
  if (OB_FAIL(all_hints.assign(local_method_hints_))) {
    LOG_WARN("fail to assign local method join hints", K(ret));
  } else {

  #define ADD_USE_JOIN_HINT(join_algo) {  \
    int64_t &cur_methods = join_hint->is_enable_hint() ? use_methods : no_use_methods;       \
    if (!(cur_methods & join_algo) && OB_FAIL(local_method_hints_.push_back(join_hint))) {  \
      LOG_WARN("fail to push back join hint", K(ret), K(*join_hint)); \
    } else { cur_methods |= join_algo; }} \

    local_method_hints_.reuse();
    int64_t use_methods = JoinAlgo::INVALID_JOIN_ALGO;
    int64_t no_use_methods = JoinAlgo::INVALID_JOIN_ALGO;
    const ObJoinHint *join_hint = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && local_method_hints_.count() < 6
                                     && i < all_hints.count(); ++i) {
      if (OB_ISNULL(join_hint = all_hints.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected NULL", K(ret), K(all_hints));
      } else {
        switch(join_hint->get_hint_type()) {
          case T_USE_NL:    ADD_USE_JOIN_HINT(NESTED_LOOP_JOIN);  break;
          case T_USE_HASH:  ADD_USE_JOIN_HINT(HASH_JOIN); break;
          case T_USE_MERGE: ADD_USE_JOIN_HINT(MERGE_JOIN);  break;
          default: {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected join hint type", K(ret), K(join_hint));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      use_methods = JoinAlgo::INVALID_JOIN_ALGO == use_methods ? NESTED_LOOP_JOIN | HASH_JOIN | MERGE_JOIN
                                                               : use_methods;
      local_methods_ = use_methods & ~no_use_methods;
      LOG_DEBUG("finish init local methods", K(local_methods_), K(all_hints),
                                             K(local_method_hints_));
    }
  }
  return ret;
}

int LogTableHint::assign(const LogTableHint &other)
{
  int ret = OB_SUCCESS;
  table_ = other.table_;
  parallel_hint_ = other.parallel_hint_;
  use_das_hint_ = other.use_das_hint_;
  use_column_store_hint_ = other.use_column_store_hint_;
  if (OB_FAIL(index_list_.assign(other.index_list_))) {
    LOG_WARN("failed to assign index list", K(ret));
  } else if (OB_FAIL(index_hints_.assign(other.index_hints_))) {
    LOG_WARN("failed to assign hints", K(ret));
  } else if (OB_FAIL(join_filter_hints_.assign(other.join_filter_hints_))) {
    LOG_WARN("failed to assign index list", K(ret));
  } else if (OB_FAIL(left_tables_.assign(other.left_tables_))) {
    LOG_WARN("failed to assign hints", K(ret));
  }
  return ret;
}

int LogTableHint::init_index_hints(ObSqlSchemaGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  uint64_t tids[OB_MAX_INDEX_PER_TABLE + 1];
  int64_t table_index_count = OB_MAX_INDEX_PER_TABLE + 1;
  if (OB_ISNULL(table_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected log index hint", K(ret), K(this));
  } else if (index_hints_.empty()) {
    /* do nothing */
  } else if (OB_FAIL(schema_guard.get_can_read_index_array(table_->ref_id_,
                                                           tids,
                                                           table_index_count,
                                                           false,
                                                           table_->access_all_part(),
                                                           false /*domain index*/,
                                                           false /*spatial index*/))) {
    LOG_WARN("failed to get can read index", K(ret));
  } else if (table_index_count > OB_MAX_INDEX_PER_TABLE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Table index count is bigger than OB_MAX_INDEX_PER_TABLE", K(ret), K(table_index_count));
  } else {
    LOG_TRACE("get readable index", K(table_index_count));
    const share::schema::ObTableSchema *index_schema = NULL;
    ObSEArray<uint64_t, 4> index_list;
    ObSEArray<uint64_t, 4> no_index_list;
    ObSEArray<const ObIndexHint*, 4> index_hints;
    ObSEArray<const ObIndexHint*, 4> no_index_hints;
    for (int64_t i = -1; OB_SUCC(ret) && i < table_index_count; ++i) {
      uint64_t index_id = -1 == i ? table_->ref_id_ : tids[i];
      ObString index_name;
      bool is_primary_key = false;
      if (-1 == i) {
        is_primary_key = true;
        index_name = ObIndexHint::PRIMARY_KEY;
      } else if (OB_FAIL(schema_guard.get_table_schema(index_id, index_schema)) ||
                 OB_ISNULL(index_schema)) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("fail to get table schema", K(index_id), K(ret));
      } else if (index_schema->is_fts_index()) {
        // just ignore domain index
      } else if (OB_FAIL(index_schema->get_index_name(index_name))) {
        LOG_WARN("fail to get index name", K(index_name), K(ret));
      }

      if (OB_SUCC(ret) && (!index_name.empty())) {
        int64_t no_index_hint_pos = OB_INVALID_INDEX;
        int64_t index_hint_pos = OB_INVALID_INDEX;
        int64_t index_ss_hint_pos = OB_INVALID_INDEX;
        const uint64_t N = index_hints_.count();
        const ObIndexHint *index_hint = NULL;
        for (int64_t hint_i = 0; OB_SUCC(ret) && hint_i < N; ++hint_i) {
          if (OB_ISNULL(index_hint = index_hints_.at(hint_i)) ||
              OB_UNLIKELY(!index_hint->is_access_path_hint())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null index hint", K(ret), K(index_hint));
          } else if (is_primary_key && T_FULL_HINT == index_hint->get_hint_type()) {
            index_hint_pos = hint_i;
          } else if (0 != index_hint->get_index_name().case_compare(index_name)) {
            /* do nothing */
          } else if (T_NO_INDEX_HINT == index_hint->get_hint_type()) {
            no_index_hint_pos = hint_i;
          } else if (T_INDEX_SS_HINT == index_hint->get_hint_type()) {
            index_ss_hint_pos = hint_i;
          } else {
            index_hint_pos = hint_i;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_INVALID_INDEX != no_index_hint_pos
                   && (OB_INVALID_INDEX != index_ss_hint_pos
                       || OB_INVALID_INDEX != index_hint_pos)) {
          /* conflict full/index/index_ss and no_index hint*/
        } else if (OB_INVALID_INDEX != no_index_hint_pos) {
          if (OB_FAIL(no_index_list.push_back(index_id))) {
            LOG_WARN("fail to push back", K(ret), K(index_id));
          } else if (OB_FAIL(no_index_hints.push_back(index_hints_.at(no_index_hint_pos)))) {
            LOG_WARN("fail to push back", K(ret), K(no_index_hint_pos));
          }
        } else if (OB_INVALID_INDEX != index_ss_hint_pos
                   || OB_INVALID_INDEX != index_hint_pos) {
          int64_t hint_pos = OB_INVALID_INDEX != index_ss_hint_pos
                             ? index_ss_hint_pos : index_hint_pos;
          if (OB_FAIL(index_list.push_back(index_id))) {
            LOG_WARN("fail to push back", K(ret), K(index_id));
          } else if (OB_FAIL(index_hints.push_back(index_hints_.at(hint_pos)))) {
            LOG_WARN("fail to push back", K(ret), K(hint_pos));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (!index_list.empty()) {
        if (OB_FAIL(index_list_.assign(index_list))) {
          LOG_WARN("failed to assign array", K(ret));
        } else if (OB_FAIL(index_hints_.assign(index_hints))) {
          LOG_WARN("failed to assign array", K(ret));
        }
      } else if (OB_FAIL(index_list_.assign(no_index_list))) {
        LOG_WARN("failed to assign array", K(ret));
      } else if (OB_FAIL(index_hints_.assign(no_index_hints))) {
        LOG_WARN("failed to assign array", K(ret));
      }
    }
  }
  return ret;
}

int LogTableHint::get_join_filter_hint(const ObRelIds &left_tables,
                                       bool part_join_filter,
                                       const ObJoinFilterHint *&hint) const
{
  int ret = OB_SUCCESS;
  hint = NULL;
  if (OB_UNLIKELY(left_tables.is_empty()) ||
      OB_UNLIKELY(left_tables_.count() != join_filter_hints_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(left_tables), K(left_tables_), K(join_filter_hints_));
  } else {
    const ObJoinFilterHint *cur_hint = NULL;
    bool finish = false;
    for (int64_t i = 0; OB_SUCC(ret) && !finish && i < left_tables_.count(); ++i) {
      if (OB_ISNULL(cur_hint = join_filter_hints_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (cur_hint->is_part_join_filter_hint() != part_join_filter) {
        /* do nothing */
      } else if (NULL == hint && left_tables_.at(i).is_empty()) {
        hint = cur_hint;
      } else if (left_tables.equal(left_tables_.at(i))) {
        hint = cur_hint;
        finish = true;
      }
    }
  }
  return ret;
}

int LogTableHint::get_join_filter_hints(const ObRelIds &left_tables,
                                        bool part_join_filter,
                                        ObIArray<const ObJoinFilterHint*> &hints) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(left_tables.is_empty()) ||
      OB_UNLIKELY(left_tables_.count() != join_filter_hints_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(left_tables), K(left_tables_), K(join_filter_hints_));
  } else {
    const ObJoinFilterHint *cur_hint = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < left_tables_.count(); ++i) {
      if (OB_ISNULL(cur_hint = join_filter_hints_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret));
      } else if (cur_hint->is_part_join_filter_hint() != part_join_filter) {
        /* do nothing */
      } else if (!left_tables.equal(left_tables_.at(i))) {
        /* do nothing */
      } else if (OB_FAIL(hints.push_back(cur_hint))) {
        LOG_WARN("failed to push back hints", K(ret));
      }
    }
  }
  return ret;
}

int LogTableHint::add_join_filter_hint(const ObDMLStmt &stmt,
                                       const ObQueryHint &query_hint,
                                       const ObJoinFilterHint &hint)
{
  int ret = OB_SUCCESS;
  ObRelIds left_tables;
  const bool has_left_tables = hint.has_left_tables();
  if (OB_UNLIKELY(left_tables_.count() != join_filter_hints_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected params", K(ret), K(left_tables_), K(join_filter_hints_));
  } else if (has_left_tables &&
             OB_FAIL(query_hint.get_relids_from_hint_tables(stmt, hint.get_left_tables(), left_tables))) {
    LOG_WARN("failed to get relids from hint tables", K(ret), K(hint.get_left_tables()));
  } else if (has_left_tables && left_tables.is_empty()) {
    LOG_TRACE("get invalid join hint", K(ret), K(left_tables), K(hint));
  } else {
    bool added = false;
    for (int64_t i = 0; OB_SUCC(ret) && !added && i < left_tables_.count(); ++i) {
      if (OB_ISNULL(join_filter_hints_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected params", K(ret), K(join_filter_hints_.at(i)));
      } else if (join_filter_hints_.at(i)->get_hint_type() == hint.get_hint_type()
                 && has_left_tables == join_filter_hints_.at(i)->has_left_tables()
                 && left_tables.equal(left_tables_.at(i))
                 && join_filter_hints_.at(i)->get_pushdown_filter_table().equal(
                                              hint.get_pushdown_filter_table())) {
        added = true;
        if (hint.is_disable_hint()) {
          join_filter_hints_.at(i) = &hint;
        }
      }
    }
    if (OB_FAIL(ret) || added) {
    } else if (OB_FAIL(left_tables_.push_back(left_tables))) {
      LOG_TRACE("failed to push back", K(ret), K(hint), K(left_tables));
    } else if (OB_FAIL(join_filter_hints_.push_back(&hint))) {
      LOG_TRACE("failed to push back", K(ret), K(hint));
    }
  }
  return ret;
}

int LogTableHint::allowed_skip_scan(const uint64_t index_id, bool &allowed) const
{
  int ret = OB_SUCCESS;
  allowed = false;
  if (!is_use_index_hint()) {
    /* do nothing */
  } else if (OB_UNLIKELY(index_list_.count() != index_hints_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count", K(ret), K(index_list_.count()), K(index_hints_.count()));
  } else {
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < index_list_.count(); ++i) {
      if (index_list_.at(i) != index_id)  {
        /* do nothing */
      } else if (OB_ISNULL(index_hints_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(i), K(index_hints_));
      } else {
        allowed = T_INDEX_SS_HINT == index_hints_.at(i)->get_hint_type();
        find = true;
      }
    }
  }
  return ret;
}

int LogTableHint::get_index_prefix(const uint64_t index_id, int64_t &index_prefix) const
{
  int ret = OB_SUCCESS;
  index_prefix = -1;
  if (!is_use_index_hint()) {
    /* do nothing */
  } else if (OB_UNLIKELY(index_list_.count() != index_hints_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count", K(ret), K(index_list_.count()), K(index_hints_.count()));
  } else {
    bool find = false;
    for (int64_t i = 0; OB_SUCC(ret) && !find && i < index_list_.count(); ++i) {
      if (index_list_.at(i) != index_id)  {
        /* do nothing */
      } else if (OB_ISNULL(index_hints_.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), K(i), K(index_hints_));
      } else if (T_INDEX_HINT == index_hints_.at(i)->get_hint_type()) {
        index_prefix = index_hints_.at(i)->get_index_prefix();
      }
    }
  }
  return ret;
}

int JoinFilterPushdownHintInfo::check_use_join_filter(const ObDMLStmt &stmt,
                                                      const ObQueryHint &query_hint,
                                                      uint64_t filter_table_id,
                                                      bool part_join_filter,
                                                      bool &can_use,
                                                      const ObJoinFilterHint *&force_hint) const
{
  int ret = OB_SUCCESS;
  const ObIArray<const ObJoinFilterHint*> &join_filters = part_join_filter ? part_join_filter_hints_ :
                                                                             join_filter_hints_;
  const TableItem* table_item;
  ObString qb_name;
  const ObJoinFilterHint* current_hint = NULL;
  bool found = false;
  if (OB_ISNULL(table_item = stmt.get_table_item_by_id(filter_table_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get filter table from stmt", K(ret));
  } else if(OB_FAIL(stmt.get_qb_name(qb_name))) {
    LOG_WARN("failed to get qb name", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && !found && i < join_filters.count(); ++i) {
    const ObJoinFilterHint *hint;
    if (OB_ISNULL(hint = join_filters.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (0 != hint->get_pushdown_filter_table().qb_name_.case_compare(qb_name)) {
    } else if (hint->get_pushdown_filter_table().is_match_table_item(query_hint.cs_type_, *table_item)) {
      current_hint = hint;
      if (hint->is_disable_hint()) {
        found = true;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (NULL != current_hint) {
    can_use = current_hint->is_enable_hint();
    force_hint = can_use ? current_hint : NULL;
  } else if (query_hint.has_outline_data()) {
    can_use = false;
    force_hint = NULL;
  } else {
    can_use = !config_disable_;
    force_hint = NULL;
  }
  return ret;
}

}//end of namespace sql
}//end of namespace oceanbase
