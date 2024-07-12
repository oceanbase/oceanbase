/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL
#include "lib/utility/ob_utility.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/ob_optimizer_trace_impl.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "share/ob_version.h"
#include "sql/resolver/ddl/ob_explain_stmt.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_log_values.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "lib/file/file_directory_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/optimizer/ob_dynamic_sampling.h"
#include "sql/optimizer/ob_skyline_prunning.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase
{
namespace sql
{

LogFileAppender::LogFileAppender()
  :allocator_("LogFileAppender"),
  identifier_(""),
  log_file_name_("OPT_TRACE")
{

}

int LogFileAppender::open()
{
  int ret = OB_SUCCESS;
  bool is_full = false;
  if (log_file_name_.empty() &&
      OB_FAIL(generate_log_file_name())) {
    LOG_WARN("failed to generate log file", K(ret));
  } else if (OB_FAIL(open_log_file())) {
    LOG_WARN("failed to open log file", K(ret));
  } else if (OB_FAIL(check_log_file_full(is_full))) {
    LOG_WARN("failed to check log file is full", K(ret));
  } else if (is_full) {
    close();
    if (OB_FAIL(generate_log_file_name())) {
      LOG_WARN("failed to generate log file", K(ret));
    } else if (OB_FAIL(open_log_file())) {
      LOG_WARN("failed to open log file", K(ret));
    }
  }
  return ret;
}

void LogFileAppender::close()
{
  if (log_handle_.is_opened()) {
    log_handle_.close();
  }
}

int LogFileAppender::set_identifier(const common::ObString &identifier)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ob_write_string(allocator_, identifier, identifier_))) {
    LOG_WARN("ObStringBuf write string error", K(ret));
  } else if (OB_FAIL(generate_log_file_name())) {
    LOG_WARN("failed to generate log file name", K(ret));
  }
  return ret;
}

int LogFileAppender::append(const char* buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (!log_handle_.is_opened()) {
    ret = OB_NOT_INIT;
    LOG_WARN("log file not open", K(ret));
  } else if (OB_FAIL(log_handle_.append(buf, buf_len, false))) {
    LOG_WARN("failed to append msg", K(ret));
  }
  return ret;
}

int LogFileAppender::check_log_file_full(bool &is_full)
{
  int ret = OB_SUCCESS;
  is_full = false;
  if (!log_handle_.is_opened()) {
    ret = OB_NOT_INIT;
    LOG_WARN("log file not open", K(ret));
  } else if (log_handle_.get_file_pos() > MAX_LOG_FILE_SIZE) {
    is_full = true;
  }
  return ret;
}

int LogFileAppender::generate_log_file_name()
{
  int ret = OB_SUCCESS;
  constexpr const char* dict = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  constexpr int word_base = 62; //length of dict
  const int64_t file_id_len = 6;
  char buf[6];
  const int try_count = 5;
  int i = 0;
  bool exists = true;

  while (i < try_count && exists) {
    log_file_name_.reuse();
    int64_t time = ObTimeUtil::current_time();
    uint32_t hash_ts = murmurhash2(&time, sizeof(time), 0);
    for (int j = 0; j < file_id_len; ++j) {
        buf[j] = dict[hash_ts % word_base];
        hash_ts /= word_base;
    }
    if (OB_FAIL(log_file_name_.append("log/optimizer_trace_"))) {
      LOG_WARN("failed to apend str", K(ret));
    } else if (OB_FAIL(log_file_name_.append(buf, file_id_len))) {
      LOG_WARN("failed to apend str", K(ret));
    } else if (!identifier_.empty() &&
              OB_FAIL(log_file_name_.append("_"))) {
      LOG_WARN("failed to apend str", K(ret));
    } else if (!identifier_.empty() &&
              OB_FAIL(log_file_name_.append(identifier_))) {
      LOG_WARN("failed to apend str", K(ret));
    } else if (OB_FAIL(log_file_name_.append(".trac"))) {
      LOG_WARN("failed to apend str", K(ret));
    } else if (OB_FAIL(FSU::is_exists(log_file_name_.ptr(), exists))) {
      LOG_WARN("failed to check log file exists", K(ret));
    } else if (exists) {
      ++i;
    }
  }

  if (OB_SUCC(ret) && exists) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not generate new log file", K(ret));
  }
  return ret;
}

int LogFileAppender::open_log_file()
{
  int ret = OB_SUCCESS;
  close();
  if (OB_FAIL(log_handle_.open(ObString(log_file_name_.length(),
                                        log_file_name_.ptr()),
                                        false,
                                        true))) {
    LOG_WARN("fail to open file", K(log_file_name_), K(ret));
  }
  return ret;
}

ObOptimizerTraceImpl::ObOptimizerTraceImpl()
  :allocator_("OptimizerTrace"),
    sql_id_(""),
    session_info_(NULL),
    start_time_us_(0),
    last_time_us_(0),
    last_mem_(0),
    section_(0),
    trace_level_(0),
    enable_(false),
    trace_state_(1),
    enable_trace_cost_model_(false)
{

}

ObOptimizerTraceImpl::~ObOptimizerTraceImpl()
{
  log_handle_.close();
}

int ObOptimizerTraceImpl::enable_trace(const common::ObString &identifier,
                                      const common::ObString &sql_id,
                                      const int trace_level)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_parameters(identifier, sql_id, trace_level))) {
    LOG_WARN("failed to set parameters", K(ret));
  } else {
    start_time_us_ = 0;
    last_time_us_ = 0;
    last_mem_ = 0;
    section_ = 0;
    trace_state_ = 1;
    enable_trace_cost_model_ = false;
    set_enable(true);
  }
  return ret;
}

int ObOptimizerTraceImpl::set_parameters(const common::ObString &identifier,
                                        const common::ObString &sql_id,
                                        const int trace_level)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(log_handle_.set_identifier(identifier))) {
    LOG_WARN("ObStringBuf write string error", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, sql_id, sql_id_))) {
    LOG_WARN("ObStringBuf write string error", K(ret));
  } else {
    trace_level_ = trace_level;
  }
  return ret;
}

void ObOptimizerTraceImpl::reset()
{
  log_handle_.close();
  sql_id_.reset();
  enable_ = false;
  trace_state_ = 1;
  enable_trace_cost_model_ = false;
}

bool ObOptimizerTraceImpl::enable(const common::ObString &sql_id)
{
  if (!sql_id_.empty()) {
    return enable() && (sql_id_.compare(sql_id) == 0);
  } else {
    return enable();
  }
}

int ObOptimizerTraceImpl::open()
{
  int ret = OB_SUCCESS;
  start_time_us_ = ObTimeUtil::current_time();
  last_time_us_ = start_time_us_;
  last_mem_ = 0;
  if (OB_FAIL(log_handle_.open())) {
    LOG_WARN("fail to open file", K(ret));
  }
  return ret;
}

void ObOptimizerTraceImpl::close()
{
  log_handle_.close();
}

void ObOptimizerTraceImpl::resume_trace()
{
  trace_state_ >>= 1;
}

void ObOptimizerTraceImpl::stop_trace()
{
  trace_state_ <<= 1;
}

void ObOptimizerTraceImpl::restart_trace()
{
  trace_state_ <<= 1;
  trace_state_ |= 1;
}

int ObOptimizerTraceImpl::new_line()
{
  int ret = OB_SUCCESS;
  ret = append("\n");
  for (int i = 0; OB_SUCC(ret) && i < section_; ++i) {
    ret = append("    ");
  }
  return ret;
}

int ObOptimizerTraceImpl::append_lower(const char* msg)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = strlen(msg);
  common::ObArenaAllocator allocator("OptimizerTrace");
  if (buf_len <= 0) {
  } else if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Failed to allocate buffer", "buffer size", buf_len, K(ret));
  } else {
    for (int64_t i = 0; i < buf_len; ++i) {
      if (msg[i] >= 'A' && msg[i] <= 'Z') {
        buf[i] = msg[i] + 'a' - 'A';
      } else {
        buf[i] = msg[i];
      }
    }
    if (OB_FAIL(log_handle_.append(buf, buf_len))) {
      LOG_WARN("failed to append msg", K(ret));
    }
  }
  return ret;
}

int ObOptimizerTraceImpl::append_ptr(const void *ptr)
{
  int ret = OB_SUCCESS;
  char buf[32] = {0};
  int64_t buf_len = 32;
  buf_len = snprintf(buf, buf_len, "ptr:%p", ptr);
  if (buf_len > 0) {
    if (OB_FAIL(log_handle_.append(buf, buf_len))) {
      LOG_WARN("failed to append value", K(ret));
    }
  }
  return ret;
}

int ObOptimizerTraceImpl::append()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObOptimizerTraceImpl::append(const bool &value)
{
  if (value) {
    return append("True");
  } else {
    return append("False");
  }
}

int ObOptimizerTraceImpl::append(const char *msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(log_handle_.append(msg, strlen(msg)))) {
    LOG_WARN("failed to append msg", K(ret));
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const common::ObString &msg)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(log_handle_.append(msg.ptr(), msg.length()))) {
    LOG_WARN("failed to append msg", K(ret));
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const int64_t &value)
{
  int ret = OB_SUCCESS;
  char buf[32] = {0};
  int64_t buf_len = 32;
  buf_len = snprintf(buf, buf_len, "%ld", value);
  if (buf_len > 0) {
    if (OB_FAIL(log_handle_.append(buf, buf_len))) {
      LOG_WARN("failed to append value", K(ret));
    }
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const uint64_t &value)
{
  int ret = OB_SUCCESS;
  char buf[32] = {0};
  int64_t buf_len = 32;
  buf_len = snprintf(buf, buf_len, "%lu", value);
  if (buf_len > 0) {
    if (OB_FAIL(log_handle_.append(buf, buf_len))) {
      LOG_WARN("failed to append value", K(ret));
    }
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const uint32_t &value)
{
  int ret = OB_SUCCESS;
  char buf[32] = {0};
  int64_t buf_len = 32;
  buf_len = snprintf(buf, buf_len, "%u", value);
  if (buf_len > 0) {
    if (OB_FAIL(log_handle_.append(buf, buf_len))) {
      LOG_WARN("failed to append value", K(ret));
    }
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const double & value)
{
  int ret = OB_SUCCESS;
  char buf[32] = {0};
  int64_t buf_len = 32;
  buf_len = snprintf(buf, buf_len, "%f", value);
  if (buf_len > 0) {
    if (OB_FAIL(log_handle_.append(buf, buf_len))) {
      LOG_WARN("failed to append value", K(ret));
    }
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const ObObj& value)
{
  int ret = OB_SUCCESS;
  char buf[1024] = {0};
  int64_t buf_len = 1024;
  int64_t pos = 0;
  if (value.is_invalid_type()) {
    ret = append(" ");
  } else if (OB_FAIL(value.print_sql_literal(buf, buf_len, pos))) {
    LOG_WARN("failed to print obj", K(ret));
  } else if (OB_FAIL(log_handle_.append(buf, pos))) {
    LOG_WARN("failed to append value", K(ret));
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const OpParallelRule& rule)
{
  int ret = OB_SUCCESS;
  switch(rule) {
    case OP_GLOBAL_DOP:
      ret = append("global dop");
      break;
    case OP_DAS_DOP:
      ret = append("das");
      break;
    case OP_HINT_DOP:
      ret = append("hint");
      break;
    case OP_TABLE_DOP:
      ret = append("table dop");
      break;
    case OP_AUTO_DOP:
      ret = append("auto dop");
      break;
    case OP_INHERIT_DOP:
      ret = append("inherit");
      break;
    case OP_DOP_RULE_MAX:
      ret = append("max rule");
      break;
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const ObTableLocationType& type)
{
  int ret = OB_SUCCESS;
  switch(type) {
    case OB_TBL_LOCATION_UNINITIALIZED:
      ret = append("uninitialized");
      break;
    case OB_TBL_LOCATION_LOCAL:
      ret = append("local");
      break;
    case OB_TBL_LOCATION_REMOTE:
      ret = append("remote");
      break;
    case OB_TBL_LOCATION_DISTRIBUTED:
      ret = append("distribute");
      break;
    case OB_TBL_LOCATION_ALL:
      ret = append("match all");
      break;
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const ObPhyPlanType& type)
{
  int ret = OB_SUCCESS;
  switch(type) {
    case OB_PHY_PLAN_UNINITIALIZED:
      ret = append("uninitialized");
      break;
    case OB_PHY_PLAN_LOCAL:
      ret = append("local");
      break;
    case OB_PHY_PLAN_REMOTE:
      ret = append("remote");
      break;
    case OB_PHY_PLAN_DISTRIBUTED:
      ret = append("distribute");
      break;
    case OB_PHY_PLAN_UNCERTAIN:
      ret = append("uncertain");
      break;
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const OptSystemStat& stat)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(append("cpu speed:", stat.get_cpu_speed(), "MHz"))) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(new_line())) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(append("disk seq read speed:", stat.get_disk_seq_read_speed(), "MB/s"))) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(new_line())) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(append("disk rnd read speed:", stat.get_disk_rnd_read_speed(), "MB/s"))) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(new_line())) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(append("network speed:", stat.get_network_speed(), "MB/s"))) {
    LOG_WARN("failed to append msg", K(ret));
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const ObLogPlan *log_plan)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t buf_len = 1024 * 1024;
  ObExplainDisplayOpt option;
  option.with_tree_line_ = true;
  const ObLogPlan *target_plan = log_plan;
  if (OB_NOT_NULL(target_plan) &&
      target_plan->get_stmt()->is_explain_stmt()) {
    const ObLogValues *op = static_cast<const ObLogValues*>(target_plan->get_plan_root());
    target_plan = op->get_explain_plan();
  }
  if (OB_NOT_NULL(target_plan)) {
    ObSqlPlan sql_plan(target_plan->get_allocator());
    ObSEArray<common::ObString, 64> plan_strs;
    if (OB_FAIL(sql_plan.print_sql_plan(const_cast<ObLogPlan*>(target_plan),
                                        EXPLAIN_EXTENDED,
                                        option,
                                        plan_strs))) {
      LOG_WARN("failed to store sql plan", K(ret));
    }
    OPT_TRACE_TITLE("Query Plan");
    for (int64_t i = 0; OB_SUCC(ret) && i < plan_strs.count(); ++i) {
      if (OB_FAIL(new_line())) {
        LOG_WARN("failed to append msg", K(ret));
      } else if (OB_FAIL(append(plan_strs.at(i)))) {
        LOG_WARN("failed to append plan", K(ret));
      }
    }
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const ObJoinOrder *join_order)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 4> array;
  const ObLogPlan *plan = NULL;
  if (OB_ISNULL(join_order) || OB_ISNULL(plan=join_order->get_plan()) ||
      OB_ISNULL(plan->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null param", K(ret));
  } else if (OB_FAIL(join_order->get_tables().to_array(array))) {
    LOG_WARN("failed to get array from bit set", K(ret));
  } else {
    append("[");
    for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); ++i) {
      const TableItem *table = plan->get_stmt()->get_table_item(array.at(i)-1);
      if (i > 0) {
        append(", ");
      }
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table item", K(ret));
      } else if (OB_FAIL(append(table->get_table_name()))) {
        LOG_WARN("failed to append msg", K(ret));
      }
    }
    append("]");
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const Path *path)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(path)) {
    increase_section();
    new_line();
    append_ptr(path);
    new_line();
    append("tables:", path->parent_);
    if (path->is_access_path()) {
      const AccessPath& ap = static_cast<const AccessPath&>(*path);
      const ObIndexMetaInfo &index_info = ap.est_cost_info_.index_meta_info_;
      append("index id:", ap.index_id_, ",global index:", ap.is_global_index_, ", use column store:", ap.use_column_store_);
      new_line();
      append("use das:", ap.use_das_, ",unique index:", index_info.is_unique_index_, ",index back:", index_info.is_index_back_);
      new_line();
      append("table rows:", ap.get_table_row_count(), ",phy_query_range_row_count:", ap.get_phy_query_range_row_count());
      new_line();
      append("query_range_row_count:", ap.get_logical_query_range_row_count(), ",index_back_row_count:", ap.get_index_back_row_count());
      new_line();
      append("partition count:", index_info.index_part_count_, ",micro block count:", index_info.get_micro_block_numbers());
      new_line();
      append("prefix filters:", ap.est_cost_info_.prefix_filters_, ",selectivity:", ap.est_cost_info_.prefix_filter_sel_);
      new_line();
      append("pushdown prefix filters:", ap.est_cost_info_.pushdown_prefix_filters_, ",selectivity:", ap.est_cost_info_.pushdown_prefix_filter_sel_);
      new_line();
      append("postfix filters:", ap.est_cost_info_.postfix_filters_, ",selectivity:", ap.est_cost_info_.postfix_filter_sel_);
      new_line();
      append("table filters:", ap.est_cost_info_.table_filters_, ",selectivity:", ap.est_cost_info_.table_filter_sel_);
    }
    new_line();
    append("cost:", path->cost_, ",card:", path->parent_->get_output_rows(),
          ",width:", path->parent_->get_output_row_size());
    new_line();
    append("parallel:", path->parallel_, ", parallel rule:", path->op_parallel_rule_);
    append(", server count:", path->server_cnt_);
    append(path->get_sharding());
    decrease_section();
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const JoinPath* join_path)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(join_path)) {
    increase_section();
    new_line();
    append_ptr(join_path);
    new_line();
    if (HASH_JOIN == join_path->join_algo_) {
      append("dist algo:", ob_dist_algo_str(join_path->join_dist_algo_),
            ",can use join filter:", !join_path->join_filter_infos_.empty(),
            ",is naaj:", join_path->is_naaj_, ",is sna:", join_path->is_sna_);
    } else if (NESTED_LOOP_JOIN == join_path->join_algo_) {
      append("dist algo:", ob_dist_algo_str(join_path->join_dist_algo_),
            ",right need materia:", join_path->need_mat_,
            ",use batch nlj:", join_path->can_use_batch_nlj_);
    } else {
      append("dist algo:", ob_dist_algo_str(join_path->join_dist_algo_),
            ",left need sort:", join_path->left_need_sort_, ",left prefix pos:", join_path->left_prefix_pos_,
            ",right need sort:", join_path->right_need_sort_, ",right prefix pos:", join_path->right_prefix_pos_);
    }
    new_line();
    append(join_path->get_sharding());
    new_line();
    append("left path:");
    if (OB_NOT_NULL(join_path->left_path_) && OB_NOT_NULL(join_path->left_path_->parent_)) {
      increase_section();
      new_line();
      append("tables:", join_path->left_path_->parent_);
      new_line();
      append_ptr(join_path->left_path_);
      new_line();
      append("cost:", join_path->left_path_->cost_, ",card:", join_path->left_path_->parent_->get_output_rows(),
            ",width:", join_path->left_path_->parent_->get_output_row_size());
      new_line();
      append("parallel:", join_path->left_path_->parallel_, ",server count:", join_path->left_path_->server_cnt_);
      if (NULL != join_path->left_path_->get_sharding()) {
        append(",part count:", join_path->left_path_->get_sharding()->get_part_cnt());
      }
      decrease_section();
    }
    new_line();
    append("right path:");
    if (OB_NOT_NULL(join_path->right_path_) && OB_NOT_NULL(join_path->right_path_->parent_)) {
      increase_section();
      new_line();
      append("tables:", join_path->right_path_->parent_);
      new_line();
      new_line();
      append_ptr(join_path->right_path_);
      append("cost:", join_path->right_path_->cost_, ",card:", join_path->right_path_->parent_->get_output_rows(),
            ",width:", join_path->right_path_->parent_->get_output_row_size());
      new_line();
      append("parallel:", join_path->right_path_->parallel_, ",server count:", join_path->right_path_->server_cnt_);
      if (NULL != join_path->right_path_->get_sharding()) {
        append(",part count:", join_path->right_path_->get_sharding()->get_part_cnt());
      }
      decrease_section();
    }
    decrease_section();
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const JoinInfo& info)
{
  int ret = OB_SUCCESS;
  new_line();
  append("join type:", ob_join_type_str(info.join_type_));
  if (INNER_JOIN == info.join_type_) {
    new_line();
    append("join conditions:", info.where_conditions_);
  } else {
    new_line();
    append("join conditions:", info.on_conditions_);
    new_line();
    append("filters:", info.where_conditions_);
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const TableItem *table)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table)) {
  } else if (table->is_joined_table()) {
    const JoinedTable* joined_table = static_cast<const JoinedTable*>(table);
    if (OB_FAIL(append("("))) {
      LOG_WARN("failed to append msg", K(ret));
    } else if (OB_FAIL(SMART_CALL(append(joined_table->left_table_,
                                        ob_join_type_str(joined_table->joined_type_),
                                        joined_table->right_table_)))) {
      LOG_WARN("failed to append msg", K(ret));
    } else if (OB_FAIL(append(")"))) {
      LOG_WARN("failed to append msg", K(ret));
    }
  } else if (OB_FAIL(append(table->get_table_name()))) {
    LOG_WARN("failed to append msg", K(ret));
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const ObShardingInfo *info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
  } else {
    new_line();
    append("location type:", info->get_location_type());
    append(", partion count:", info->get_part_cnt());
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const CandidatePlan &plan)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(plan.plan_tree_)) {
  } else {
    increase_section();
    new_line();
    append_ptr(plan.plan_tree_);
    new_line();
    append("cost:", plan.plan_tree_->get_cost(), ",card:", plan.plan_tree_->get_card(),
          ",width:", plan.plan_tree_->get_width());
    new_line();
    append("partition wise", plan.plan_tree_->is_partition_wise(), ", plan type:", plan.plan_tree_->get_phy_plan_type(),
    ", location type:", plan.plan_tree_->get_location_type());
    new_line();
    append("parallel:", plan.plan_tree_->get_parallel(), ", parallel rule:", plan.plan_tree_->get_op_parallel_rule());
    append(", server count:", plan.plan_tree_->get_server_cnt());
    append(plan.plan_tree_->get_sharding());
    decrease_section();
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const ObDSResultItem &ds_result)
{
  int ret = OB_SUCCESS;
  const ObOptDSStat *stat = ds_result.stat_handle_.stat_;
  if (NULL == stat) {
  } else if (OB_FAIL(append("table id:", ds_result.index_id_))) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_DS_BASIC_STAT == ds_result.type_ &&
             OB_FAIL(append(", tpye:basic"))) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_DS_OUTPUT_STAT == ds_result.type_ &&
             OB_FAIL(append(", tpye:output"))) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_DS_FILTER_OUTPUT_STAT == ds_result.type_ &&
             OB_FAIL(append(", tpye:filter and output"))) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FALSE_IT(increase_section())) {
  } else if (OB_FAIL(new_line())) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(append("rows:",
                            stat->get_rowcount()))) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(new_line())) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(append("macro_block_num:",
                            stat->get_macro_block_num()))) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(new_line())) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(append("micro_block_num:",
                            stat->get_micro_block_num()))) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(new_line())) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(append("sample_block_ratio:",
                            stat->get_sample_block_ratio()))) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(new_line())) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(append("ds_level:",
                            stat->get_ds_level()))) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(new_line())) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(append("dml_cnt:",
                            stat->get_dml_cnt()))) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(new_line())) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(append("ds_degree:",
                            stat->get_ds_degree()))) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(new_line())) {
    LOG_WARN("failed to append msg", K(ret));
  } else {
    for (int64_t j = 0; OB_SUCC(ret) && j < stat->get_ds_col_stats().count(); ++j) {
      const ObOptDSColStat &col_stat = stat->get_ds_col_stats().at(j);
      if (OB_FAIL(append("column id:", col_stat.column_id_, ":"))) {
        LOG_WARN("failed to append msg", K(ret));
      } else if (OB_FALSE_IT(increase_section())) {
      } else if (OB_FAIL(new_line())) {
        LOG_WARN("failed to append msg", K(ret));
      } else if (OB_FAIL(append("NDV:", col_stat.num_distinct_))) {
        LOG_WARN("failed to append msg", K(ret));
      } else if (OB_FAIL(new_line())) {
        LOG_WARN("failed to append msg", K(ret));
      } else if (OB_FAIL(append("Null:", col_stat.num_null_))) {
        LOG_WARN("failed to append msg", K(ret));
      } else if (OB_FAIL(new_line())) {
        LOG_WARN("failed to append msg", K(ret));
      } else if (OB_FAIL(append("degree:", col_stat.degree_))) {
        LOG_WARN("failed to append msg", K(ret));
      } else if (OB_FAIL(new_line())) {
        LOG_WARN("failed to append msg", K(ret));
      } else {
        decrease_section();
      }
    }
    decrease_section();
  }
  return ret;
}

int ObOptimizerTraceImpl::append(const ObSkylineDim &dim)
{
  int ret = OB_SUCCESS;
  switch (dim.get_dim_type()) {
    case ObSkylineDim::INDEX_BACK: {
      const ObIndexBackDim &index_dim = static_cast<const ObIndexBackDim &>(dim);
      append("[index back dim] need index back:", index_dim.need_index_back_);
      append(", has interesting order:", index_dim.has_interesting_order_);
      append(", can extract range:", index_dim.can_extract_range_);
      append(", filter columns:", common::ObArrayWrap<uint64_t>(index_dim.filter_column_ids_, index_dim.filter_column_cnt_));
      break;
    }
    case ObSkylineDim::INTERESTING_ORDER: {
      const ObInterestOrderDim &order_dim = static_cast<const ObInterestOrderDim &>(dim);
      append("[intersting order dim] is interesting order:", order_dim.is_interesting_order_);
      append(", need index back:", order_dim.need_index_back_);
      append(", can extract range:", order_dim.can_extract_range_);
      append(", column ids:", common::ObArrayWrap<uint64_t>(order_dim.column_ids_,  order_dim.column_cnt_));
      append(", filter columns:", common::ObArrayWrap<uint64_t>(order_dim.filter_column_ids_, order_dim.filter_column_cnt_));
      break;
    }
    case ObSkylineDim::QUERY_RANGE: {
      const ObQueryRangeDim &range_dim = static_cast<const ObQueryRangeDim &>(dim);
      append("[query range dim] contain false range:", range_dim.contain_always_false_);
      append(", rowkey ids:", common::ObArrayWrap<uint64_t>(range_dim.column_ids_, range_dim.column_cnt_));
      break;
    }
    default: {
      append("unknown dim");
      break;
    }
  }
  return ret;
}

int ObOptimizerTraceImpl::trace_env()
{
  int ret = OB_SUCCESS;
  char buf[1024+1] = {0};
  int64_t buf_len = 1024;
  if (OB_FAIL(get_package_and_svn(buf, buf_len))) {
    LOG_WARN("fail to get build_version", KR(ret));
  } else if (OB_FAIL(new_line())) {
    LOG_WARN("failed to append msg", K(ret));
  } else if (OB_FAIL(append_key_value("Version", ObString(strlen(buf), buf)))) {
    LOG_WARN("failed to append msg", K(ret));
  }
  return ret;
}

int ObOptimizerTraceImpl::trace_parameters()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(session_info_)) {
    int64_t tenant_id = session_info_->get_effective_tenant_id();
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (tenant_config.is_valid()) {
      new_line();
      append("tenant config:");
      tenant_config.trace_all_config();
    }
    new_line();
    append("system variables:");
    session_info_->trace_all_sys_vars();
  }
  return ret;
}

int ObOptimizerTraceImpl::trace_session_info()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(session_info_)) {
    ObSQLSessionInfo *session = session_info_;
    const common::ObAddr &client_addr = session->get_peer_addr();
    common::ObAddr proxy_addr = session->get_proxy_addr();
    char trace_id[common::OB_MAX_TRACE_ID_BUFFER_SIZE];
    int64_t trace_len = session->get_current_trace_id().to_string(trace_id, sizeof(trace_id));
    char buf[1024];
    const int32_t len = 1024;
    int32_t buf_len;
    if (OB_FAIL(new_line())) {
      LOG_WARN("failed to append msg", K(ret));
    } else if (OB_FAIL(client_addr.addr_to_buffer(buf, len, buf_len))) {
      LOG_WARN("failed to print addr", K(ret));
    } else if (OB_FAIL(append_key_value("Client Address", ObString(buf_len,buf)))) {
      LOG_WARN("failed to append msg", K(ret));
    } else if (OB_FAIL(new_line())) {
      LOG_WARN("failed to append msg", K(ret));
    } else if (OB_FAIL(proxy_addr.addr_to_buffer(buf, len, buf_len))) {
      LOG_WARN("failed to print addr", K(ret));
    } else if (OB_FAIL(append_key_value("Proxy Address", ObString(buf_len,buf)))) {
      LOG_WARN("failed to append msg", K(ret));
    } else if (OB_FAIL(new_line())) {
      LOG_WARN("failed to append msg", K(ret));
    } else if (OB_FAIL(append_key_value("Tenant Name", session->get_tenant_name()))) {
      LOG_WARN("failed to append msg", K(ret));
    } else if (OB_FAIL(new_line())) {
      LOG_WARN("failed to append msg", K(ret));
    } else if (OB_FAIL(append_key_value("User Name", session->get_user_name()))) {
      LOG_WARN("failed to append msg", K(ret));
    } else if (OB_FAIL(new_line())) {
      LOG_WARN("failed to append msg", K(ret));
    } else if (OB_FAIL(append_key_value("Trace ID", ObString(trace_len, trace_id)))) {
      LOG_WARN("failed to append msg", K(ret));
    }
  }
  return ret;
}

int ObOptimizerTraceImpl::trace_static(const ObDMLStmt *stmt, OptTableMetas &table_metas)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(stmt)) {
    ObSEArray<ColumnItem, 4> column_items;
    const TableItem *table = NULL;
    const OptTableMeta* table_meta = NULL;
    const OptColumnMeta* col_meta = NULL;
    if (OB_FAIL(new_line())) {
      LOG_WARN("failed to append msg", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_table_items().count(); ++i) {
      table = stmt->get_table_item(i);
      column_items.reuse();
      if (OB_ISNULL(table) ||
          OB_ISNULL(table_meta = table_metas.get_table_meta_by_table_id(table->table_id_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpect null table item", K(ret));
      } else if (OB_FAIL(stmt->get_column_items(table->table_id_, column_items))) {
        LOG_WARN("failed to get column ids", K(ret));
      } else if (OB_FAIL(new_line())) {
        LOG_WARN("failed to append msg", K(ret));
      } else if (OB_FAIL(append(table->get_table_name(), ":"))) {
        LOG_WARN("failed to append msg", K(ret));
      } else if (OB_FALSE_IT(increase_section())) {
      } else if (OB_FAIL(new_line())) {
        LOG_WARN("failed to append msg", K(ret));
      } else if (OB_FAIL(append("rows:",
                                table_meta->get_rows(),
                                "statis type:",
                                table_meta->use_default_stat() ? "DEFAULT" : "OPTIMIZER",
                                "version:",
                                table_meta->get_version()))) {
        LOG_WARN("failed to append msg", K(ret));
      } else if (OB_FAIL(new_line())) {
        LOG_WARN("failed to append msg", K(ret));
      } else if (OB_FAIL(append("used partitions:", table_meta->get_all_used_parts()))) {
        LOG_WARN("failed to append msg", K(ret));
      } else if (OB_FAIL(new_line())) {
        LOG_WARN("failed to append msg", K(ret));
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < column_items.count(); ++j) {
        ColumnItem &col = column_items.at(j);
        if (OB_ISNULL(col_meta = table_metas.get_column_meta_by_table_id(table->table_id_, col.column_id_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null column meta", K(ret));
        } else if (OB_FAIL(new_line())) {
          LOG_WARN("failed to append msg", K(ret));
        } else if (OB_FAIL(append(col.column_name_, ":"))) {
          LOG_WARN("failed to append msg", K(ret));
        } else if (OB_FALSE_IT(increase_section())) {
        } else if (OB_FAIL(new_line())) {
          LOG_WARN("failed to append msg", K(ret));
        } else if (OB_FAIL(append("NDV:", col_meta->get_ndv()))) {
          LOG_WARN("failed to append msg", K(ret));
        } else if (OB_FAIL(new_line())) {
          LOG_WARN("failed to append msg", K(ret));
        } else if (OB_FAIL(append("Null:", col_meta->get_num_null()))) {
          LOG_WARN("failed to append msg", K(ret));
        } else if (OB_FAIL(new_line())) {
          LOG_WARN("failed to append msg", K(ret));
        } else if (OB_FAIL(append("hist scale:", col_meta->get_hist_scale()))) {
          LOG_WARN("failed to append msg", K(ret));
        } else if (OB_FAIL(new_line())) {
          LOG_WARN("failed to append msg", K(ret));
        } else if (OB_FAIL(append("Min:", col_meta->get_min_value()))) {
          LOG_WARN("failed to append msg", K(ret));
        } else if (OB_FAIL(new_line())) {
          LOG_WARN("failed to append msg", K(ret));
        } else if (OB_FAIL(append("Max:", col_meta->get_max_value()))) {
          LOG_WARN("failed to append msg", K(ret));
        } else if (OB_FAIL(new_line())) {
          LOG_WARN("failed to append msg", K(ret));
        } else if (OB_FAIL(append("column group micro block count:", col_meta->get_cg_micro_blk_cnt()))) {
          LOG_WARN("failed to append msg", K(ret));
        } else if (OB_FAIL(new_line())) {
          LOG_WARN("failed to append msg", K(ret));
        } else {
          decrease_section();
        }
      }
      decrease_section();
    }
  }
  return ret;
}

int ObOptimizerTraceImpl::trace_trans_sql(const ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(stmt) && enable_trace_trans_sql()) {
    if (OB_FAIL(new_line())) {
      LOG_WARN("failed to append msg", K(ret));
    } else if (OB_FAIL(append(stmt))) {
      LOG_WARN("failed to append stmt", K(ret));
    }
  }
  return ret;
}

int ObOptimizerTraceImpl::trace_time_used()
{
  int ret = OB_SUCCESS;
  if (enable_trace_time_used()) {
    int64_t now = ObTimeUtil::current_time();
    if (OB_FAIL(new_line())) {
      LOG_WARN("failed to append msg", K(ret));
    } else if (OB_FAIL(append("SECTION TIME USAGE:",now-last_time_us_, "us"))) {
      LOG_WARN("failed to append msg", K(ret));
    } else if (OB_FAIL(new_line())) {
      LOG_WARN("failed to append msg", K(ret));
    } else if (OB_FAIL(append("TOTAL TIME USAGE:",now-start_time_us_, "us"))) {
      LOG_WARN("failed to append msg", K(ret));
    } else {
      last_time_us_ = now;
    }
  }
  return ret;
}

int ObOptimizerTraceImpl::trace_mem_used()
{
  int ret = OB_SUCCESS;
  if (enable_trace_mem_used() &&
      OB_NOT_NULL(session_info_)) {
    int64_t total = 0;
    if (OB_NOT_NULL(session_info_->get_cur_exec_ctx())) {
      total = session_info_->get_cur_exec_ctx()->get_allocator().total()/1024;
    }
    if (OB_FAIL(new_line())) {
      LOG_WARN("failed to append msg", K(ret));
    } else if (OB_FAIL(append("SECTION MEM USAGE:", total-last_mem_, "KB"))) {
      LOG_WARN("failed to append msg", K(ret));
    } else if (OB_FAIL(new_line())) {
      LOG_WARN("failed to append msg", K(ret));
    } else if (OB_FAIL(append("TOTAL MEM USAGE:", total, "KB"))) {
      LOG_WARN("failed to append msg", K(ret));
    } else {
      last_mem_ = total;
    }
  }
  return ret;
}

}
}
