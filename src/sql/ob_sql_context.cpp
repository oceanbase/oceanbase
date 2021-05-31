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
#include "sql/ob_sql_context.h"
#include <algorithm>
#include "lib/container/ob_se_array_iterator.h"
#include "sql/resolver/dml/ob_sql_hint.h"
#include "sql/ob_sql_define.h"
#include "sql/optimizer/ob_log_plan.h"
#include "share/schema/ob_schema_getter_guard.h"

using namespace ::oceanbase::common;
namespace oceanbase {
using namespace share::schema;
namespace sql {
bool LocationConstraint::operator==(const LocationConstraint& other) const
{
  return key_ == other.key_ && phy_loc_type_ == other.phy_loc_type_ && constraint_flags_ == other.constraint_flags_;
}

bool LocationConstraint::operator!=(const LocationConstraint& other) const
{
  return !(*this == other);
}

int LocationConstraint::calc_constraints_inclusion(
    const ObLocationConstraint* left, const ObLocationConstraint* right, InclusionType& inclusion_result)
{
  int ret = OB_SUCCESS;
  inclusion_result = NotSubset;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(left), K(right));
  } else {
    const ObLocationConstraint *set1 = NULL, *set2 = NULL;
    bool is_subset = true;
    // insure set1.count() >= set2.count()
    if (left->count() >= right->count()) {
      inclusion_result = LeftIsSuperior;
      set1 = left;
      set2 = right;
    } else {
      inclusion_result = RightIsSuperior;
      set1 = right;
      set2 = left;
    }

    for (int64_t i = 0; is_subset && i < set2->count(); i++) {
      bool detected = false;
      for (int64_t j = 0; !detected && j < set1->count(); j++) {
        if (set2->at(i) == set1->at(j)) {
          detected = true;
        }
      }
      // if the element is not in set1, set1 can not contain all the elements in set2
      if (!detected) {
        is_subset = false;
      }
    }
    if (!is_subset) {
      inclusion_result = NotSubset;
    }
  }

  return ret;
}

int ObLocationConstraintContext::calc_constraints_inclusion(
    const ObPwjConstraint* left, const ObPwjConstraint* right, InclusionType& inclusion_result)
{
  int ret = OB_SUCCESS;
  inclusion_result = NotSubset;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(left), K(right));
  } else {
    const ObPwjConstraint *set1 = NULL, *set2 = NULL;
    bool is_subset = true;
    // insure set1.count() >= set2.count()
    if (left->count() >= right->count()) {
      inclusion_result = LeftIsSuperior;
      set1 = left;
      set2 = right;
    } else {
      inclusion_result = RightIsSuperior;
      set1 = right;
      set2 = left;
    }

    for (int64_t i = 0; is_subset && i < set2->count(); i++) {
      bool detected = false;
      for (int64_t j = 0; !detected && j < set1->count(); j++) {
        if (set2->at(i) == set1->at(j)) {
          detected = true;
        }
      }
      // if the element is not in set1, set1 can not contain all the elements in set2
      if (!detected) {
        is_subset = false;
      }
    }
    if (!is_subset) {
      inclusion_result = NotSubset;
    }
  }

  return ret;
}

int ObQueryRetryInfo::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("init twice", K(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObQueryRetryInfo::reset()
{
  inited_ = false;
  is_rpc_timeout_ = false;
  invalid_servers_.reset();
  last_query_retry_err_ = OB_SUCCESS;
}

void ObQueryRetryInfo::clear()
{
  // here cannot set inited_ as false
  is_rpc_timeout_ = false;
  invalid_servers_.reset();
  // last_query_retry_err_ = OB_SUCCESS;
}

void ObQueryRetryInfo::clear_state_before_each_retry()
{
  is_rpc_timeout_ = false;
  // The accumulated members of successive retry cannot be cleared here, such as invalid_servers_, last_query_retry_err_
}

// Combine retry information,
// used for the retry information combination of the main thread and the scheduling thread
int ObQueryRetryInfo::merge(const ObQueryRetryInfo& other)
{
  int ret = OB_SUCCESS;
  if (other.is_rpc_timeout_) {
    is_rpc_timeout_ = other.is_rpc_timeout_;
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < other.invalid_servers_.count(); ++i) {
    if (OB_FAIL(add_invalid_server_distinctly(other.invalid_servers_.at(i)))) {
      LOG_WARN("fail to add invalid server distinctly",
          K(ret),
          K(i),
          K(other.invalid_servers_.at(i)),
          K(other.invalid_servers_),
          K(invalid_servers_));
    }
  }
  // last_query_retry_err_ will not be modified on the scheduling thread, so don't worry about it here
  return ret;
}

void ObQueryRetryInfo::set_is_rpc_timeout(bool is_rpc_timeout)
{
  is_rpc_timeout_ = is_rpc_timeout;
}

bool ObQueryRetryInfo::is_rpc_timeout() const
{
  return is_rpc_timeout_;
}

int ObQueryRetryInfo::add_invalid_server_distinctly(const ObAddr& invalid_server, bool print_info_log /* = false*/)
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < invalid_servers_.count(); ++i) {
    if (invalid_server == invalid_servers_.at(i)) {
      is_found = true;
    }
  }
  if (OB_SUCC(ret) && !is_found) {
    if (OB_FAIL(invalid_servers_.push_back(invalid_server))) {
      LOG_WARN("fail to push back invalid server", K(ret), K(invalid_server));
    }
  }
  if (print_info_log) {
    LOG_INFO("add a server to invalid server list", K(ret), K(invalid_server), K(invalid_servers_));
  }
  return ret;
}

ObSqlCtx::ObSqlCtx()
    : multi_stmt_item_(),
      session_info_(NULL),
      schema_guard_(NULL),
      sql_proxy_(NULL),
      vt_iter_factory_(NULL),
      use_plan_cache_(true),
      plan_cache_hit_(false),
      self_add_plan_(false),
      disable_privilege_check_(PRIV_CHECK_FLAG_NORMAL),
      partition_table_operator_(NULL),
      session_mgr_(NULL),
      part_mgr_(NULL),
      partition_location_cache_(NULL),
      merged_version_(OB_MERGED_VERSION_INIT),
      force_print_trace_(false),
      is_show_trace_stmt_(false),
      retry_times_(OB_INVALID_COUNT),
      exec_type_(InvalidType),
      is_prepare_protocol_(false),
      is_prepare_stage_(false),
      is_dynamic_sql_(false),
      is_dbms_sql_(false),
      is_cursor_(false),
      is_remote_sql_(false),
      is_execute_async_(false),
      statement_id_(common::OB_INVALID_ID),
      stmt_type_(stmt::T_NONE),
      partition_infos_(),
      is_restore_(false),
      // retry_info_(),
      need_late_compile_(false),
      is_bushy_tree_(false),
      all_plan_const_param_constraints_(nullptr),
      all_possible_const_param_constraints_(nullptr),
      all_equal_param_constraints_(nullptr),
      trans_happened_route_(nullptr),
      is_ddl_from_primary_(false),
      cur_stmt_(NULL),
      can_reroute_sql_(false),
      reroute_info_()
{
  sql_id_[common::OB_MAX_SQL_ID_LENGTH] = '\0';
}

void ObSqlCtx::reset()
{
  multi_stmt_item_.reset();
  session_info_ = NULL;
  sql_schema_guard_.reset();
  schema_guard_ = NULL;
  sql_proxy_ = NULL;
  vt_iter_factory_ = NULL;
  use_plan_cache_ = false;
  plan_cache_hit_ = false;
  self_add_plan_ = false;
  disable_privilege_check_ = PRIV_CHECK_FLAG_NORMAL;
  partition_table_operator_ = NULL;
  session_mgr_ = NULL;
  partition_location_cache_ = NULL;
  merged_version_ = OB_MERGED_VERSION_INIT;
  force_print_trace_ = false;
  is_show_trace_stmt_ = false;
  retry_times_ = OB_INVALID_COUNT;
  sql_id_[common::OB_MAX_SQL_ID_LENGTH] = '\0';
  exec_type_ = InvalidType;
  is_prepare_protocol_ = false;
  is_prepare_stage_ = false;
  is_dynamic_sql_ = false;
  is_remote_sql_ = false;
  is_execute_async_ = false;
  is_restore_ = false;
  loc_sensitive_hint_.reset();
  need_late_compile_ = false;
  all_plan_const_param_constraints_ = nullptr;
  all_possible_const_param_constraints_ = nullptr;
  all_equal_param_constraints_ = nullptr;
  trans_happened_route_ = nullptr;
  is_bushy_tree_ = false;
  is_ddl_from_primary_ = false;
  can_reroute_sql_ = false;
  reroute_info_.reset();
  clear();
}

// release dynamic allocated memory
void ObSqlCtx::clear()
{
  cur_stmt_ = NULL;
  acs_index_infos_.reset();
  partition_infos_.reset();
  related_user_var_names_.reset();
  base_constraints_.reset();
  strict_constraints_.reset();
  non_strict_constraints_.reset();
  multi_stmt_rowkey_pos_.reset();
}

void ObSqlSchemaGuard::reset()
{
  table_schemas_.reset();
  schema_guard_ = NULL;
  allocator_.reset();
  next_link_table_id_ = 1;
}

int ObSqlSchemaGuard::get_table_schema(
    uint64_t dblink_id, const ObString& database_name, const ObString& table_name, const ObTableSchema*& table_schema)
{
  int ret = OB_SUCCESS;
  int64_t schema_count = table_schemas_.count();
  table_schema = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(table_schema) && i < schema_count; i++) {
    const ObTableSchema* tmp_schema = table_schemas_.at(i);
    OV(OB_NOT_NULL(tmp_schema));
    if (OB_SUCC(ret) && dblink_id == tmp_schema->get_dblink_id() &&
        0 == database_name.compare(tmp_schema->get_link_database_name()) &&
        0 == table_name.compare(tmp_schema->get_table_name_str())) {
      table_schema = tmp_schema;
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(table_schema)) {
    ObTableSchema* tmp_schema = NULL;
    OV(OB_NOT_NULL(schema_guard_), OB_NOT_INIT);
    OZ(schema_guard_->get_link_table_schema(dblink_id, database_name, table_name, allocator_, tmp_schema));
    OV(OB_NOT_NULL(tmp_schema));
    OX(tmp_schema->set_link_table_id(tmp_schema->get_table_id()));
    OX(tmp_schema->set_table_id(combine_link_table_id(dblink_id, next_link_table_id_++)));
    OV(tmp_schema->get_table_id() != OB_INVALID_ID, OB_ERR_UNEXPECTED, dblink_id, next_link_table_id_);
    OZ(table_schemas_.push_back(tmp_schema));
    OX(table_schema = tmp_schema);
  }
  return ret;
}

int ObSqlSchemaGuard::get_table_schema(uint64_t table_id, const ObTableSchema*& table_schema) const
{
  int ret = OB_SUCCESS;
  if (is_link_table_id(table_id)) {
    OZ(get_link_table_schema(table_id, table_schema), table_id);
  } else {
    OV(OB_NOT_NULL(schema_guard_));
    OZ(schema_guard_->get_table_schema(table_id, table_schema), table_id);
  }
  return ret;
}

int ObSqlSchemaGuard::get_column_schema(
    uint64_t table_id, const ObString& column_name, const ObColumnSchemaV2*& column_schema) const
{
  int ret = OB_SUCCESS;
  if (is_link_table_id(table_id)) {
    OZ(get_link_column_schema(table_id, column_name, column_schema), table_id, column_name);
  } else {
    OV(OB_NOT_NULL(schema_guard_));
    OZ(schema_guard_->get_column_schema(table_id, column_name, column_schema), table_id, column_name);
  }
  return ret;
}

int ObSqlSchemaGuard::get_column_schema(
    uint64_t table_id, uint64_t column_id, const ObColumnSchemaV2*& column_schema) const
{
  int ret = OB_SUCCESS;
  if (is_link_table_id(table_id)) {
    OZ(get_link_column_schema(table_id, column_id, column_schema), table_id, column_id);
  } else {
    OV(OB_NOT_NULL(schema_guard_));
    OZ(schema_guard_->get_column_schema(table_id, column_id, column_schema), table_id, column_id);
  }
  return ret;
}

int ObSqlSchemaGuard::get_table_schema_version(const uint64_t table_id, int64_t& schema_version) const
{
  int ret = OB_SUCCESS;
  if (is_link_table_id(table_id)) {
    const ObTableSchema* table_schema = NULL;
    OZ(get_link_table_schema(table_id, table_schema), table_id);
    OV(OB_NOT_NULL(table_schema), OB_TABLE_NOT_EXIST, table_id);
    OX(schema_version = table_schema->get_schema_version());
  } else {
    OV(OB_NOT_NULL(schema_guard_));
    OZ(schema_guard_->get_table_schema_version(table_id, schema_version), table_id);
  }
  return ret;
}

int ObSqlSchemaGuard::get_partition_cnt(uint64_t table_id, int64_t& part_cnt) const
{
  int ret = OB_SUCCESS;
  if (is_link_table_id(table_id)) {
    part_cnt = 1;
  } else {
    OV(OB_NOT_NULL(schema_guard_));
    OZ(schema_guard_->get_partition_cnt(table_id, part_cnt), table_id);
  }
  return ret;
}

int ObSqlSchemaGuard::get_can_read_index_array(uint64_t table_id, uint64_t* index_tid_array, int64_t& size,
    bool with_mv, bool with_global_index, bool with_domain_index)
{
  int ret = OB_SUCCESS;
  if (is_link_table_id(table_id)) {
    size = 0;
  } else {
    OV(OB_NOT_NULL(schema_guard_));
    OZ(schema_guard_->get_can_read_index_array(
        table_id, index_tid_array, size, with_mv, with_global_index, with_domain_index));
  }
  return ret;
}

int ObSqlSchemaGuard::get_link_table_schema(uint64_t table_id, const ObTableSchema*& table_schema) const
{
  int ret = OB_SUCCESS;
  int64_t schema_count = table_schemas_.count();
  const ObTableSchema* tmp_schema = NULL;
  table_schema = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && OB_ISNULL(table_schema) && i < schema_count; i++) {
    OX(tmp_schema = table_schemas_.at(i));
    OV(OB_NOT_NULL(tmp_schema));
    if (OB_SUCC(ret) && table_id == tmp_schema->get_table_id()) {
      table_schema = tmp_schema;
    }
  }
  return ret;
}

int ObSqlSchemaGuard::get_link_column_schema(
    uint64_t table_id, const ObString& column_name, const ObColumnSchemaV2*& column_schema) const
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  OZ(get_table_schema(table_id, table_schema), table_id);
  if (OB_NOT_NULL(table_schema)) {
    OX(column_schema = table_schema->get_column_schema(column_name));
  }
  return ret;
}

int ObSqlSchemaGuard::get_link_column_schema(
    uint64_t table_id, uint64_t column_id, const ObColumnSchemaV2*& column_schema) const
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  OZ(get_table_schema(table_id, table_schema), table_id);
  if (OB_NOT_NULL(table_schema)) {
    OX(column_schema = table_schema->get_column_schema(column_id));
  }
  return ret;
}

int ObQueryCtx::add_stmt_id_name(const int64_t stmt_id, const common::ObString& stmt_name, ObStmt* stmt)
{
  int ret = OB_SUCCESS;
  bool name_dup = false;
  if (stmt_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt id is invalid", K(ret), K(stmt_id));
  } else {
    // find name dup and equal id
    for (int64_t idx = 0; OB_SUCC(ret) && idx < stmt_id_name_map_.count(); ++idx) {
      if (!stmt_name.empty() && 0 == stmt_name.case_compare(stmt_id_name_map_.at(idx).name_)) {
        if (name_dup) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Names in array duplicate", K(ret));
        } else {
          name_dup = true;
        }
      }
      if (OB_SUCC(ret)) {
        if (stmt_id == stmt_id_name_map_.at(idx).id_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Stmt id in array duplicate", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObString final_name = name_dup ? ObString::make_empty_string() : stmt_name;
      if (!name_dup && !stmt_name.empty() && OB_FAIL(valid_qb_name_stmt_ids_.push_back(stmt_id))) {
        LOG_WARN("fail to push_back stmt_id", K(ret));
      } else if (OB_FAIL(stmt_id_name_map_.push_back(
                     IdNamePair(stmt_id, final_name, ObString::make_empty_string(), stmt->get_stmt_type())))) {
        SQL_LOG(WARN, "Add stmt id, name pair error", K(ret));
      } else if (OB_FAIL(id_stmt_map_.push_back(std::pair<int64_t, ObStmt*>(stmt_id, stmt)))) {
        LOG_WARN("Failed to add id stmt pair", K(ret));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObQueryCtx::generate_stmt_name(ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  uint64_t OB_MAX_QB_NAME_LENGTH = 20;
  char buf[OB_MAX_QB_NAME_LENGTH];
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Allocator should not be NULL", K(ret));
  } else if (stmt_id_name_map_.count() > 0) {
    std::sort(stmt_id_name_map_.begin(), stmt_id_name_map_.end());
    for (int64_t idx = 0; OB_SUCC(ret) && idx < stmt_id_name_map_.count(); ++idx) {
      IdNamePair& id_name_pair = stmt_id_name_map_.at(idx);
      if (!id_name_pair.origin_name_.empty()) {
        // do nothing
      } else if (stmt::T_SELECT == id_name_pair.stmt_type_ || stmt::T_INSERT == id_name_pair.stmt_type_ ||
                 stmt::T_REPLACE == id_name_pair.stmt_type_ || stmt::T_DELETE == id_name_pair.stmt_type_ ||
                 stmt::T_UPDATE == id_name_pair.stmt_type_) {
        int64_t pos = 0;
        if (OB_FAIL(get_dml_stmt_name(id_name_pair.stmt_type_, buf, OB_MAX_QB_NAME_LENGTH, pos))) {
          LOG_WARN("Get dml stmt name", K(ret));
        } else if (OB_FAIL(append_id_to_stmt_name(buf, OB_MAX_QB_NAME_LENGTH, pos))) {
          LOG_WARN("Failed to append id to stmt name", K(ret));
        } else {
          ObString generate_name(pos, buf);
          if (OB_FAIL(ob_write_string(*allocator, generate_name, id_name_pair.origin_name_))) {
            LOG_WARN("Write string error", K(ret));
          } else if (!id_name_pair.name_.empty()) {
            // do nothing
          } else if (OB_FAIL(ob_write_string(*allocator, generate_name, id_name_pair.name_))) {
            LOG_WARN("Write string error", K(ret));
          } else {
          }  // do nothing.
        }
      } else {
      }  // other stmt type need not generate stmt name
    }
  } else {
  }  // do nothing
  return ret;
}

int ObQueryCtx::get_dml_stmt_name(stmt::StmtType stmt_type, char* buf, int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Input buf should not be NULL", K(ret));
  } else if (pos < buf_len) {
    switch (stmt_type) {
      case stmt::T_SELECT: {
        if (OB_FAIL(BUF_PRINTF("SEL$"))) {
          LOG_WARN("Append name to buf error", K(ret));
        }
        break;
      }
      case stmt::T_INSERT: {
        if (OB_FAIL(BUF_PRINTF("INS$"))) {
          LOG_WARN("Append name to buf error", K(ret));
        }
        break;
      }
      case stmt::T_REPLACE: {
        if (OB_FAIL(BUF_PRINTF("REP$"))) {
          LOG_WARN("Append name to buf error", K(ret));
        }
        break;
      }
      case stmt::T_UPDATE: {
        if (OB_FAIL(BUF_PRINTF("UPD$"))) {
          LOG_WARN("Append name to buf error", K(ret));
        }
        break;
      }
      case stmt::T_DELETE: {
        if (OB_FAIL(BUF_PRINTF("DEL$"))) {
          LOG_WARN("Append name to buf error", K(ret));
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Invalid stmt type to get dml name", K(ret));
      }
    }
  } else {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("Buffer size not enough", K(ret));
  }
  return ret;
}

int ObQueryCtx::append_id_to_stmt_name(char* buf, int64_t buf_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Buf should not be NULL", K(ret));
  } else if (pos >= buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("Buf size not enough", K(ret));
  } else {
    bool find_unique = false;
    int64_t id = 1;
    int64_t old_pos = pos;
    while (!find_unique && OB_SUCC(ret)) {
      pos = old_pos;
      if (OB_FAIL(BUF_PRINTF("%ld", id))) {
        LOG_WARN("Append idx to stmt_name error", K(ret));
      } else {
        bool find_dup = false;
        for (int64_t i = 0; !find_dup && i < stmt_id_name_map_.count(); ++i) {
          if (0 == stmt_id_name_map_.at(i).origin_name_.case_compare(buf)) {
            find_dup = true;
          } else {
          }  // do nothing
        }    // end of for
        find_unique = !find_dup;
        ++id;
      }
    }
  }
  return ret;
}

int ObQueryCtx::find_stmt_id(const ObString& stmt_name, int64_t& stmt_id)
{
  int ret = OB_SUCCESS;
  stmt_id = -1;
  if (stmt_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Failed to find stmt id", K(ret));
  } else {
    bool find_stmt = false;
    for (int64_t idx = 0; OB_SUCC(ret) && !find_stmt && idx < stmt_id_name_map_.count(); ++idx) {
      if (0 == stmt_name.case_compare(stmt_id_name_map_.at(idx).name_)) {
        stmt_id = stmt_id_name_map_.at(idx).id_;
        find_stmt = true;
      }
    }
  }
  return ret;
}

ObStmt* ObQueryCtx::find_stmt_by_id(const int64_t stmt_id)
{
  ObStmt* stmt = NULL;
  if (stmt_id == OB_INVALID_STMT_ID) {
  } else {
    for (int64_t idx = 0; idx < id_stmt_map_.count(); ++idx) {
      if (stmt_id == id_stmt_map_.at(idx).first) {
        stmt = id_stmt_map_.at(idx).second;
        break;
      }
    }
  }
  return stmt;
}

int ObQueryCtx::replace_name_in_tables(const ObSQLSessionInfo& session_info, ObDMLStmt& stmt,
    ObIArray<ObTablesInHint>& arr, uint64_t table_id, ObString& to_db_name, ObString& to_table_name)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < arr.count(); i++) {
    if (OB_FAIL(ObStmtHint::replace_name_in_hint(
            session_info, stmt, arr.at(i).tables_, table_id, to_db_name, to_table_name))) {
      LOG_WARN("Failed to replace name", K(ret));
    }
  }
  return ret;
}

int ObQueryCtx::replace_name_for_single_table_view(const ObSQLSessionInfo& session_info, ObDMLStmt* stmt,
    uint64_t table_id, ObString& to_db_name, ObString& to_table_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL stmt", K(ret));
  } else if (OB_FAIL(replace_name_in_tables(session_info, *stmt, join_order_, table_id, to_db_name, to_table_name))) {
    LOG_WARN("failed to replace in join_order_", K(ret));
  } else if (OB_FAIL(replace_name_in_tables(session_info, *stmt, use_merge_, table_id, to_db_name, to_table_name))) {
    LOG_WARN("failed to replace in use_merge_", K(ret));
  } else if (OB_FAIL(replace_name_in_tables(session_info, *stmt, no_use_merge_, table_id, to_db_name, to_table_name))) {
    LOG_WARN("failed to replace in no use_merge_", K(ret));
  } else if (OB_FAIL(replace_name_in_tables(session_info, *stmt, use_hash_, table_id, to_db_name, to_table_name))) {
    LOG_WARN("failed to replace in use_hash_", K(ret));
  } else if (OB_FAIL(replace_name_in_tables(session_info, *stmt, no_use_hash_, table_id, to_db_name, to_table_name))) {
    LOG_WARN("failed to replace in no_use_hash_", K(ret));
  } else if (OB_FAIL(replace_name_in_tables(session_info, *stmt, use_nl_, table_id, to_db_name, to_table_name))) {
    LOG_WARN("failed to replace in use_nl_", K(ret));
  } else if (OB_FAIL(replace_name_in_tables(session_info, *stmt, no_use_nl_, table_id, to_db_name, to_table_name))) {
    LOG_WARN("failed to replace in no use_nl_", K(ret));
  } else if (OB_FAIL(replace_name_in_tables(session_info, *stmt, use_bnl_, table_id, to_db_name, to_table_name))) {
    LOG_WARN("failed to replace in use_bnl_", K(ret));
  } else if (OB_FAIL(replace_name_in_tables(session_info, *stmt, no_use_bnl_, table_id, to_db_name, to_table_name))) {
    LOG_WARN("failed to replace in no use_bnl_", K(ret));
  } else if (OB_FAIL(
                 replace_name_in_tables(session_info, *stmt, px_join_filter_, table_id, to_db_name, to_table_name))) {
    LOG_WARN("failed to replace in px join filter", K(ret));
  } else if (OB_FAIL(replace_name_in_tables(
                 session_info, *stmt, no_px_join_filter_, table_id, to_db_name, to_table_name))) {
    LOG_WARN("failed to replace in no px join filter", K(ret));
  }
  return ret;
}

int ObQueryCtx::distribute_hint_to_stmt()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(distribute_index_hint())) {
    LOG_WARN("Failed to distribute index hint", K(ret));
  } else if (OB_FAIL(distribute_pq_hint())) {
    LOG_WARN("Failed to distribute pq hint", K(ret));
  } else if (OB_FAIL(distribute_tables_hint(join_order_, ObStmtHint::LEADING))) {
    LOG_WARN("Failed to distribute leading hint", K(ret));
  } else if (OB_FAIL(distribute_tables_hint(use_merge_, ObStmtHint::USE_MERGE))) {
    LOG_WARN("Failed to distribute use merge hint", K(ret));
  } else if (OB_FAIL(distribute_tables_hint(no_use_merge_, ObStmtHint::NO_USE_MERGE))) {
    LOG_WARN("Failed to distribute no use merge hint", K(ret));
  } else if (OB_FAIL(distribute_tables_hint(use_hash_, ObStmtHint::USE_HASH))) {
    LOG_WARN("Failed to distribute use hash hint", K(ret));
  } else if (OB_FAIL(distribute_tables_hint(no_use_hash_, ObStmtHint::NO_USE_HASH))) {
    LOG_WARN("Failed to distribute no use hash hint", K(ret));
  } else if (OB_FAIL(distribute_tables_hint(use_nl_, ObStmtHint::USE_NL))) {
    LOG_WARN("Failed to distribute use nl hint", K(ret));
  } else if (OB_FAIL(distribute_tables_hint(no_use_nl_, ObStmtHint::NO_USE_NL))) {
    LOG_WARN("Failed to distribute no use nl hint", K(ret));
  } else if (OB_FAIL(distribute_tables_hint(use_bnl_, ObStmtHint::USE_BNL))) {
    LOG_WARN("Failed to distribute use bnl hint", K(ret));
  } else if (OB_FAIL(distribute_tables_hint(no_use_bnl_, ObStmtHint::NO_USE_BNL))) {
    LOG_WARN("Failed to distribute no use bnl hint", K(ret));
  } else if (OB_FAIL(distribute_tables_hint(use_nl_materialization_, ObStmtHint::USE_NL_MATERIALIZATION))) {
    LOG_WARN("Failed to distribute use material nl hint", K(ret));
  } else if (OB_FAIL(distribute_tables_hint(no_use_nl_materialization_, ObStmtHint::NO_USE_NL_MATERIALIZATION))) {
    LOG_WARN("Failed to distribute no use material nl hint", K(ret));
  } else if (OB_FAIL(distribute_tables_hint(px_join_filter_, ObStmtHint::PX_JOIN_FILTER))) {
    LOG_WARN("Failed to distribute leading hint", K(ret));
  } else if (OB_FAIL(distribute_tables_hint(no_px_join_filter_, ObStmtHint::NO_PX_JOIN_FILTER))) {
    LOG_WARN("Failed to distribute leading hint", K(ret));
  } else if (OB_FAIL(distribute_rewrite_hint(no_expand_, ObStmtHint::NO_EXPAND))) {
    LOG_WARN("Failed to distribute no expand hint", K(ret));
  } else if (OB_FAIL(distribute_rewrite_hint(use_concat_, ObStmtHint::USE_CONCAT))) {
    LOG_WARN("Failed to distribute use concat hint", K(ret));
  } else if (OB_FAIL(distribute_rewrite_hint(merge_, ObStmtHint::MERGE))) {
    LOG_WARN("Failed to distribute merge hint", K(ret));
  } else if (OB_FAIL(distribute_rewrite_hint(no_merge_, ObStmtHint::NO_MERGE))) {
    LOG_WARN("Failed to distribute no merge hint", K(ret));
  } else if (OB_FAIL(distribute_rewrite_hint(unnest_, ObStmtHint::UNNEST))) {
    LOG_WARN("Failed to distribute unnest hint", K(ret));
  } else if (OB_FAIL(distribute_rewrite_hint(no_unnest_, ObStmtHint::NO_UNNEST))) {
    LOG_WARN("Failed to distribute no unnest hint", K(ret));
  } else if (OB_FAIL(distribute_rewrite_hint(place_group_by_, ObStmtHint::PLACE_GROUPBY))) {
    LOG_WARN("Failed to distribute place group by hint", K(ret));
  } else if (OB_FAIL(distribute_rewrite_hint(no_place_group_by_, ObStmtHint::NO_PLACE_GROUPBY))) {
    LOG_WARN("Failed to distribute no place group by hint", K(ret));
  } else if (OB_FAIL(distribute_rewrite_hint(no_pred_deduce_, ObStmtHint::NO_PRED_DEDUCE))) {
    LOG_WARN("Failed to distribute no pred deduce hint", K(ret));
  } else {
  }  // do nothing.

  return ret;
}

int ObQueryCtx::distribute_index_hint()
{
  int ret = OB_SUCCESS;

  if (org_indexes_.count() > 0) {
    ObStmt* stmt = NULL;
    int64_t stmt_id = OB_INVALID_STMT_ID;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < org_indexes_.count(); ++idx) {
      ObQNameIndexHint& q_index_hint = org_indexes_.at(idx);
      if (q_index_hint.distributed_) {
        // been distributed.
      } else if (OB_FAIL(find_stmt_id(q_index_hint.qb_name_, stmt_id))) {
        LOG_WARN("Failed to find stmt id", K(ret));
      } else if (OB_INVALID_STMT_ID != stmt_id) {
        if (OB_UNLIKELY(NULL == (stmt = find_stmt_by_id(stmt_id)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Failed to find stmt", K(ret));
        } else if (stmt->is_dml_stmt()) {
          if (OB_FAIL(
                  static_cast<ObDMLStmt*>(stmt)->get_stmt_hint().org_indexes_.push_back(q_index_hint.index_hint_))) {
            LOG_WARN("Failed to add index hint to stmt", K(ret));
          } else {
            q_index_hint.distributed_ = true;
          }
        } else {
        }  // do nothing
      }
    }
  }

  return ret;
}

int ObQueryCtx::distribute_pq_hint()
{
  int ret = OB_SUCCESS;

  // pq distribute hint
  FOREACH_X(hint, org_pq_distributes_, OB_SUCC(ret))
  {
    ObStmt* stmt = NULL;
    int64_t stmt_id = OB_INVALID_STMT_ID;
    if (hint->distributed_) {
      // already distributed
    } else if (OB_FAIL(find_stmt_id(hint->qb_name_, stmt_id))) {
      LOG_WARN("failed to find stmt id", K(ret), "qb_name", hint->qb_name_);
    } else if (OB_INVALID_STMT_ID != stmt_id) {
      if (OB_ISNULL(stmt = find_stmt_by_id(stmt_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("find NULL stmt", K(ret), K(stmt_id));
      } else if (stmt->is_dml_stmt()) {
        if (OB_FAIL(static_cast<ObDMLStmt*>(stmt)->get_stmt_hint().org_pq_distributes_.push_back(*hint))) {
          LOG_WARN("array push back failed", K(ret));
        } else {
          hint->distributed_ = true;
        }
      }
    }
  }

  // pq map hint
  FOREACH_X(hint, org_pq_maps_, OB_SUCC(ret))
  {
    ObStmt* stmt = NULL;
    int64_t stmt_id = OB_INVALID_STMT_ID;
    if (hint->distributed_) {
      // already distributed
    } else if (OB_FAIL(find_stmt_id(hint->qb_name_, stmt_id))) {
      LOG_WARN("failed to find stmt id", K(ret), "qb_name", hint->qb_name_);
    } else if (OB_INVALID_STMT_ID != stmt_id) {
      if (OB_ISNULL(stmt = find_stmt_by_id(stmt_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("find NULL stmt", K(ret), K(stmt_id));
      } else if (stmt->is_dml_stmt()) {
        if (OB_FAIL(static_cast<ObDMLStmt*>(stmt)->get_stmt_hint().org_pq_maps_.push_back(*hint))) {
          LOG_WARN("array push back failed", K(ret));
        } else {
          hint->distributed_ = true;
        }
      }
    }
  }

  return ret;
}

int ObQueryCtx::distribute_rewrite_hint(const common::ObIArray<ObString>& qb_names, ObStmtHint::RewriteHint hint)
{
  int ret = OB_SUCCESS;
  ObStmt* stmt = NULL;
  int64_t stmt_id = OB_INVALID_STMT_ID;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < qb_names.count(); ++idx) {
    if (OB_FAIL(find_stmt_id(qb_names.at(idx), stmt_id))) {
      LOG_WARN("Failed to find stmt id", K(ret));
    } else if (OB_INVALID_STMT_ID != stmt_id) {
      if (OB_ISNULL(stmt = find_stmt_by_id(stmt_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to find stmt", K(ret));
      } else if (stmt->is_dml_stmt()) {
        ObStmtHint& stmt_hint = static_cast<ObDMLStmt*>(stmt)->get_stmt_hint();
        if (ObStmtHint::NO_EXPAND == hint) {
          stmt_hint.use_expand_ = ObUseRewriteHint::NO_EXPAND;
        } else if (ObStmtHint::USE_CONCAT == hint) {
          stmt_hint.use_expand_ = ObUseRewriteHint::USE_CONCAT;
        } else if (ObStmtHint::MERGE == hint) {
          stmt_hint.use_view_merge_ = ObUseRewriteHint::V_MERGE;
        } else if (ObStmtHint::NO_MERGE == hint) {
          stmt_hint.use_view_merge_ = ObUseRewriteHint::NO_V_MERGE;
        } else if (ObStmtHint::UNNEST == hint) {
          stmt_hint.use_unnest_ = ObUseRewriteHint::UNNEST;
        } else if (ObStmtHint::NO_UNNEST == hint) {
          stmt_hint.use_unnest_ = ObUseRewriteHint::NO_UNNEST;
        } else if (ObStmtHint::PLACE_GROUPBY == hint) {
          stmt_hint.use_place_groupby_ = ObUseRewriteHint::PLACE_GROUPBY;
        } else if (ObStmtHint::NO_PLACE_GROUPBY == hint) {
          stmt_hint.use_place_groupby_ = ObUseRewriteHint::NO_PLACE_GROUPBY;
        } else if (ObStmtHint::NO_PRED_DEDUCE == hint) {
          stmt_hint.use_pred_deduce_ = ObUseRewriteHint::NO_PRED_DEDUCE;
        } else {
        }  // do nothing.
      }
    }
  }
  return ret;
}

int ObQueryCtx::distribute_tables_hint(ObIArray<ObTablesInHint>& tables_hint_arr, ObStmtHint::TablesHint hint)
{
  int ret = OB_SUCCESS;
  if (tables_hint_arr.count() > 0) {
    ObStmt* stmt = NULL;
    int64_t stmt_id = OB_INVALID_STMT_ID;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < tables_hint_arr.count(); ++idx) {
      ObTablesInHint& tables_hint = tables_hint_arr.at(idx);
      if (tables_hint.distributed_) {
        // been distributed.
      } else if (OB_FAIL(find_stmt_id(tables_hint.qb_name_, stmt_id))) {
        LOG_WARN("Failed to find stmt id", K(ret));
      } else if (OB_INVALID_STMT_ID != stmt_id) {
        if (OB_UNLIKELY(NULL == (stmt = find_stmt_by_id(stmt_id)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Failed to find stmt", K(ret));
        } else if (stmt->is_dml_stmt()) {
          ObStmtHint& stmt_hint = static_cast<ObDMLStmt*>(stmt)->get_stmt_hint();
          ObIArray<ObTableInHint>* p_table_hint_arr = stmt_hint.get_join_tables(hint);
          ObIArray<std::pair<uint8_t, uint8_t>>* p_join_order_pair_arr = &stmt_hint.join_order_pairs_;
          if (OB_ISNULL(p_table_hint_arr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Hint not known", K(ret), K(hint));
          } else if (ObStmtHint::LEADING == hint && p_table_hint_arr->count() > 0 &&
                     (OB_FALSE_IT(p_table_hint_arr->reset()) || OB_FALSE_IT(p_join_order_pair_arr->reset()))) {
            /* prior leading hint ignored if query ctx has leading hint */
          } else if (OB_FAIL(append(*p_table_hint_arr, tables_hint.tables_))) {
            LOG_WARN("Failed to add index hint to stmt", K(ret));
          } else if (ObStmtHint::LEADING == hint &&
                     OB_FAIL(append(*p_join_order_pair_arr, tables_hint.join_order_pairs_))) {
            LOG_WARN("Failed to add join order hint to stmt", K(ret));
          } else if (ObStmtHint::USE_HASH == hint &&
                     OB_FAIL(append(stmt_hint.use_hash_order_pairs_, tables_hint.join_order_pairs_))) {
            LOG_WARN("Failed to add join order hint to stmt", K(ret));
          } else if (ObStmtHint::USE_MERGE == hint &&
                     OB_FAIL(append(stmt_hint.use_merge_order_pairs_, tables_hint.join_order_pairs_))) {
            LOG_WARN("Failed to add join order hint to stmt", K(ret));
          } else if (ObStmtHint::USE_NL == hint &&
                     OB_FAIL(append(stmt_hint.use_nl_order_pairs_, tables_hint.join_order_pairs_))) {
            LOG_WARN("Failed to add join order hint to stmt", K(ret));
          } else if (ObStmtHint::USE_BNL == hint &&
                     OB_FAIL(append(stmt_hint.use_bnl_order_pairs_, tables_hint.join_order_pairs_))) {
            LOG_WARN("Failed to add join order hint to stmt", K(ret));
          } else if (ObStmtHint::NO_USE_HASH == hint &&
                     OB_FAIL(append(stmt_hint.no_use_hash_order_pairs_, tables_hint.join_order_pairs_))) {
            LOG_WARN("Failed to add join order hint to stmt", K(ret));
          } else if (ObStmtHint::NO_USE_MERGE == hint &&
                     OB_FAIL(append(stmt_hint.no_use_merge_order_pairs_, tables_hint.join_order_pairs_))) {
            LOG_WARN("Failed to add join order hint to stmt", K(ret));
          } else if (ObStmtHint::NO_USE_NL == hint &&
                     OB_FAIL(append(stmt_hint.no_use_nl_order_pairs_, tables_hint.join_order_pairs_))) {
            LOG_WARN("Failed to add join order hint to stmt", K(ret));
          } else if (ObStmtHint::NO_USE_BNL == hint &&
                     OB_FAIL(append(stmt_hint.no_use_bnl_order_pairs_, tables_hint.join_order_pairs_))) {
            LOG_WARN("Failed to add join order hint to stmt", K(ret));
          } else if (ObStmtHint::USE_NL_MATERIALIZATION == hint &&
                     OB_FAIL(append(stmt_hint.use_nl_materialization_order_pairs_, tables_hint.join_order_pairs_))) {
            LOG_WARN("Failed to add join order hint to stmt", K(ret));
          } else if (ObStmtHint::NO_USE_NL_MATERIALIZATION == hint &&
                     OB_FAIL(append(stmt_hint.no_use_nl_materialization_order_pairs_, tables_hint.join_order_pairs_))) {
            LOG_WARN("Failed to add join order hint to stmt", K(ret));
          } else if (ObStmtHint::PX_JOIN_FILTER == hint &&
                     OB_FAIL(append(stmt_hint.px_join_filter_order_pairs_, tables_hint.join_order_pairs_))) {
            LOG_WARN("Failed to add join order hint to stmt", K(ret));
          } else if (ObStmtHint::NO_PX_JOIN_FILTER == hint &&
                     OB_FAIL(append(stmt_hint.no_px_join_filter_order_pairs_, tables_hint.join_order_pairs_))) {
            LOG_WARN("Failed to add join order hint to stmt", K(ret));
          } else {
            tables_hint.distributed_ = true;
          }
        } else {
          // do nothing
        }
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

int ObQueryCtx::get_stmt_name_by_id(const int64_t stmt_id, common::ObString& stmt_name) const
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < stmt_id_name_map_.count(); ++i) {
    if (stmt_id == stmt_id_name_map_.at(i).id_) {
      stmt_name = stmt_id_name_map_.at(i).name_;
      is_found = true;
    }
  }
  if (OB_SUCC(ret) && !is_found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObQueryCtx::get_stmt_org_name_by_id(const int64_t stmt_id, common::ObString& org_name) const
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < stmt_id_name_map_.count(); ++i) {
    if (stmt_id == stmt_id_name_map_.at(i).id_) {
      org_name = stmt_id_name_map_.at(i).origin_name_;
      is_found = true;
    }
  }
  if (OB_SUCC(ret) && !is_found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObQueryCtx::get_stmt_org_name(const common::ObString& stmt_name, common::ObString& org_name) const
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < stmt_id_name_map_.count(); ++i) {
    if (0 == stmt_name.case_compare(stmt_id_name_map_.at(i).name_)) {
      org_name = stmt_id_name_map_.at(i).origin_name_;
      is_found = true;
    }
  }
  if (OB_SUCC(ret) && !is_found) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObQueryCtx::print_qb_name_hint(planText& plan_text) const
{
  int ret = OB_SUCCESS;
  char* buf = plan_text.buf;
  int64_t& buf_len = plan_text.buf_len;
  int64_t& pos = plan_text.pos;
  bool is_oneline = plan_text.is_oneline_;
  OutlineType outline_type = plan_text.outline_type_;
  ObString qb_name;
  if (USED_HINT == outline_type) {
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_qb_name_stmt_ids_.count(); ++i) {
      uint64_t cur_stmt_id = valid_qb_name_stmt_ids_.at(i);
      qb_name.reset();
      if (OB_FAIL(get_stmt_name_by_id(cur_stmt_id, qb_name))) {
        LOG_WARN("fail to get stmt name", K(ret), K(cur_stmt_id));
      } else if (!is_oneline && OB_FAIL(BUF_PRINTF("\n"))) {
      } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::get_outline_indent(is_oneline)))) {
      } else if (OB_FAIL(BUF_PRINTF(ObStmtHint::QB_NAME_HINT))) {
      } else if (OB_FAIL(BUF_PRINTF("(%.*s)", qb_name.length(), qb_name.ptr()))) {
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}

bool ObQueryCtx::is_cross_tenant_query(uint64_t effective_tenant_id) const
{
  bool bret = false;
  for (int64_t i = 0; !bret && i < table_partition_infos_.count(); ++i) {
    if (table_partition_infos_.at(i) != NULL) {
      uint64_t cur_tenant_id = extract_tenant_id(table_partition_infos_.at(i)->get_ref_table_id());
      bret = (cur_tenant_id != effective_tenant_id);
    }
  }
  return bret;
}

int ObSqlCtx::set_partition_infos(const ObTablePartitionInfoArray& info, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  int64_t count = info.count();
  partition_infos_.reset();
  if (count > 0) {
    partition_infos_.set_allocator(&allocator);
    if (OB_FAIL(partition_infos_.init(count))) {
      LOG_WARN("init partition info failed", K(ret), K(count));
    } else {
      for (int64_t i = 0; i < count && OB_SUCC(ret); ++i) {
        if (OB_FAIL(partition_infos_.push_back(info.at(i)))) {
          LOG_WARN("push partition info failed", K(ret), K(count));
        }
      }
    }
  }
  return ret;
}

int ObSqlCtx::set_acs_index_info(const ObIArray<ObAcsIndexInfo*>& acs_index_infos, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (acs_index_infos.count() > 0) {
    acs_index_infos_.reset();
    acs_index_infos_.set_allocator(&allocator);
    if (OB_FAIL(acs_index_infos_.init(acs_index_infos.count()))) {
      LOG_WARN("init acs index infos failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < acs_index_infos.count(); i++) {
        if (OB_ISNULL(acs_index_infos.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(acs_index_infos_.push_back(acs_index_infos.at(i)))) {
          LOG_WARN("failed to push back acs index info", K(ret));
        } else { /*do nothing*/
        }
      }
    }
  }
  return ret;
}

int ObSqlCtx::set_related_user_var_names(const ObIArray<ObString>& user_var_names, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  if (user_var_names.count() > 0) {
    related_user_var_names_.reset();
    related_user_var_names_.set_allocator(&allocator);
    if (OB_FAIL(related_user_var_names_.init(user_var_names.count()))) {
      LOG_WARN("failed to init related_user_var_names", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < user_var_names.count(); i++) {
        if (OB_FAIL(related_user_var_names_.push_back(user_var_names.at(i)))) {
          LOG_WARN("failed to push back user var names", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    related_user_var_names_.reset();
  }
  return ret;
}

int ObSqlCtx::set_location_constraints(const ObLocationConstraintContext& location_constraint, ObIAllocator& allocator)
{
  int ret = OB_SUCCESS;
  base_constraints_.reset();
  strict_constraints_.reset();
  non_strict_constraints_.reset();
  const ObIArray<LocationConstraint>& base_constraints = location_constraint.base_table_constraints_;
  const ObIArray<ObPwjConstraint*>& strict_constraints = location_constraint.strict_constraints_;
  const ObIArray<ObPwjConstraint*>& non_strict_constraints = location_constraint.non_strict_constraints_;
  if (base_constraints.count() > 0) {
    base_constraints_.set_allocator(&allocator);
    if (OB_FAIL(base_constraints_.init(base_constraints.count()))) {
      LOG_WARN("init base constraints failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < base_constraints.count(); i++) {
        if (OB_FAIL(base_constraints_.push_back(base_constraints.at(i)))) {
          LOG_WARN("failed to push back base constraint", K(ret));
        } else {
          // sharding_info_ is only used in the plan generation phase
          base_constraints_.at(i).sharding_info_ = NULL;
        }
      }
      LOG_DEBUG("set base constraints", K(base_constraints.count()));
    }
  }
  if (OB_SUCC(ret) && strict_constraints.count() > 0) {
    strict_constraints_.set_allocator(&allocator);
    if (OB_FAIL(strict_constraints_.init(strict_constraints.count()))) {
      LOG_WARN("init strict constraints failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < strict_constraints.count(); i++) {
        if (OB_FAIL(strict_constraints_.push_back(strict_constraints.at(i)))) {
          LOG_WARN("failed to push back location constraint", K(ret));
        }
      }
      LOG_DEBUG("set strict constraints", K(strict_constraints.count()));
    }
  }
  if (OB_SUCC(ret) && non_strict_constraints.count() > 0) {
    non_strict_constraints_.set_allocator(&allocator);
    if (OB_FAIL(non_strict_constraints_.init(non_strict_constraints.count()))) {
      LOG_WARN("init non strict constraints failed", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < non_strict_constraints.count(); i++) {
        if (OB_FAIL(non_strict_constraints_.push_back(non_strict_constraints.at(i)))) {
          LOG_WARN("failed to push back location constraint", K(ret));
        }
      }
      LOG_DEBUG("set non strict constraints", K(non_strict_constraints.count()));
    }
  }
  return ret;
}

int ObSqlCtx::set_multi_stmt_rowkey_pos(
    const common::ObIArray<int64_t>& multi_stmt_rowkey_pos, common::ObIAllocator& alloctor)
{
  int ret = OB_SUCCESS;
  if (!multi_stmt_rowkey_pos.empty()) {
    multi_stmt_rowkey_pos_.set_allocator(&alloctor);
    if (OB_FAIL(multi_stmt_rowkey_pos_.init(multi_stmt_rowkey_pos.count()))) {
      LOG_WARN("failed to init rowkey count", K(ret));
    } else if (OB_FAIL(append(multi_stmt_rowkey_pos_, multi_stmt_rowkey_pos))) {
      LOG_WARN("failed to append multi stmt rowkey pos", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObQueryCtx::add_tracing_hint(ObIArray<uint64_t>& tracing_ids)
{
  int ret = OB_SUCCESS;
  bool find = false;
  ARRAY_FOREACH(tracing_ids, idx)
  {
    find = false;
    ARRAY_FOREACH(monitoring_ids_, n)
    {
      if (tracing_ids.at(idx) == monitoring_ids_.at(n).id_) {
        monitoring_ids_.at(n).flags_ = monitoring_ids_.at(n).flags_ & OB_MONITOR_TRACING;
        find = true;
      }
    }
    if (!find) {
      ObMonitorHint hint;
      hint.id_ = tracing_ids.at(idx);
      hint.flags_ = hint.flags_ | OB_MONITOR_TRACING;
      if (OB_FAIL(monitoring_ids_.push_back(hint))) {
        LOG_WARN("Failed to push back tracing", K(ret));
      }
    }
  }
  LOG_DEBUG("add tracing hint", K(monitoring_ids_));
  return ret;
}

int ObQueryCtx::add_stat_hint(ObIArray<uint64_t>& stat_ids)
{
  int ret = OB_SUCCESS;
  bool find = false;
  ARRAY_FOREACH(stat_ids, idx)
  {
    find = false;
    ARRAY_FOREACH(monitoring_ids_, n)
    {
      if (stat_ids.at(idx) == monitoring_ids_.at(n).id_) {
        monitoring_ids_.at(n).flags_ = monitoring_ids_.at(n).flags_ | OB_MONITOR_STAT;
        find = true;
      }
    }
    if (!find) {
      ObMonitorHint hint;
      hint.id_ = stat_ids.at(idx);
      hint.flags_ = hint.flags_ | OB_MONITOR_STAT;
      if (OB_FAIL(monitoring_ids_.push_back(hint))) {
        LOG_WARN("Failed to push back stat", K(ret));
      }
    }
  }
  LOG_DEBUG("add stat hint", K(monitoring_ids_));
  return ret;
}

int ObQueryCtx::add_pq_map_hint(common::ObString& qb_name, ObOrgPQMapHint& pq_map_hint)
{
  int ret = OB_SUCCESS;
  ObQBNamePQMapHint hint(qb_name, pq_map_hint);
  if (OB_FAIL(org_pq_maps_.push_back(hint))) {
    LOG_WARN("fail to push back pq map", K(ret), K(hint));
  }
  return ret;
}

int ObQueryCtx::add_temp_table(ObSqlTempTableInfo* temp_table_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(temp_table_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(temp_table_info), K(ret));
  } else if (OB_FAIL(temp_table_infos_.push_back(temp_table_info))) {
    LOG_WARN("failed to push back temp table info", K(ret));
  } else { /*do nothing*/
  }
  return ret;
}

int ObQueryCtx::get_temp_table_plan(const uint64_t temp_table_id, ObLogicalOperator*& temp_table_insert)
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < temp_table_infos_.count(); i++) {
    if (OB_ISNULL(temp_table_infos_.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (temp_table_infos_.at(i)->ref_table_id_ == temp_table_id) {
      temp_table_insert = temp_table_infos_.at(i)->table_plan_;
      is_found = true;
    } else { /*do nothing.*/
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase
