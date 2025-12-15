/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_profile_util.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "share/diagnosis/ob_runtime_profile.h"
#include "lib/string/ob_sql_string.h"
#include "sql/engine/ob_phy_operator_type.h"

namespace oceanbase
{
using namespace sql;
using namespace observer;
using namespace common::sqlclient;
namespace common
{
enum FETCH_COLUMN
{
  SVR_IP = 0,           // varchar
  SVR_PORT,             // int
  THREAD_ID,            // int
  OP_ID,                // int
  PLAN_DEPTH,           // int
  PLAN_OPERATION,       // varchar
  OPEN_TIME,            // timestamp
  CLOSE_TIME,           // timestamp
  FIRST_ROW_TIME,       // timestamp
  LAST_ROW_TIME,        // timestamp
  OUTPUT_BATCHES,       // int
  SKIPPED_ROWS,         // int
  RESCAN_TIMES,         // int
  OUTPUT_ROWS,          // int
  DB_TIME,              // int
  IO_TIME,              // int
  WORKAREA_MEM,         // int
  WORKAREA_MAX_MEM,     // int
  WORKAREA_TEMPSEG,     // int
  WORKAREA_MAX_TEMPSEG, // int
  SQL_ID,               // varchar
  PLAN_HASH_VALUE,      // uint
  OTHERSTAT_1_ID,       // int
  OTHERSTAT_1_VALUE,    // int
  OTHERSTAT_2_ID,       // int
  OTHERSTAT_2_VALUE,    // int
  OTHERSTAT_3_ID,       // int
  OTHERSTAT_3_VALUE,    // int
  OTHERSTAT_4_ID,       // int
  OTHERSTAT_4_VALUE,    // int
  OTHERSTAT_5_ID,       // int
  OTHERSTAT_5_VALUE,    // int
  OTHERSTAT_6_ID,       // int
  OTHERSTAT_6_VALUE,    // int
  OTHERSTAT_7_ID,       // int
  OTHERSTAT_7_VALUE,    // int
  OTHERSTAT_8_ID,       // int
  OTHERSTAT_8_VALUE,    // int
  OTHERSTAT_9_ID,       // int
  OTHERSTAT_9_VALUE,    // int
  OTHERSTAT_10_ID,      // int
  OTHERSTAT_10_VALUE,   // int
  RAW_PROFILE           // varchar
};

int ObMergedProfileItem::init_from(ObIAllocator *alloc, const ObProfileItem &profile_item)
{
  int ret = OB_SUCCESS;
  op_id_ = profile_item.op_id_;
  plan_depth_ = profile_item.plan_depth_;
  plan_hash_value_ = profile_item.plan_hash_value_;
  sql_id_ = profile_item.sql_id_;
  max_db_time_ = 0;
  parallel_ = 0;
  profile_ = OB_NEWx(ObMergedProfile, alloc, profile_item.profile_->get_id(),
                                  alloc, profile_item.profile_->enable_rich_format());
  if (OB_ISNULL(profile_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate new profile");
  }
  return ret;
}

int ObProfileUtil::get_profile_by_id(ObIAllocator *alloc, int64_t session_tenant_id,
                                     const ObString &trace_id, const ObString &svr_ip,
                                     int64_t svr_port, int64_t param_tenant_id, bool fetch_all_op,
                                     int64_t op_id, ObIArray<ObProfileItem> &profile_items)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("SELECT \
                  SVR_IP, \
                  SVR_PORT, \
                  THREAD_ID, \
                  PLAN_LINE_ID OP_ID, \
                  PLAN_DEPTH, \
                  PLAN_OPERATION, \
                  FIRST_REFRESH_TIME OPEN_TIME, \
                  LAST_REFRESH_TIME CLOSE_TIME, \
                  FIRST_CHANGE_TIME FIRST_ROW_TIME, \
                  LAST_CHANGE_TIME LAST_ROW_TIME, \
                  OUTPUT_BATCHES, \
                  SKIPPED_ROWS_COUNT, \
                  STARTS RESCAN_TIMES, \
                  OUTPUT_ROWS, \
                  DB_TIME, \
                  USER_IO_WAIT_TIME IO_TIME, \
                  WORKAREA_MEM, \
                  WORKAREA_MAX_MEM, \
                  WORKAREA_TEMPSEG, \
                  WORKAREA_MAX_TEMPSEG, \
                  SQL_ID, \
                  PLAN_HASH_VALUE, \
                  OTHERSTAT_1_ID, \
                  OTHERSTAT_1_VALUE, \
                  OTHERSTAT_2_ID, \
                  OTHERSTAT_2_VALUE, \
                  OTHERSTAT_3_ID, \
                  OTHERSTAT_3_VALUE,\
                  OTHERSTAT_4_ID,\
                  OTHERSTAT_4_VALUE, \
                  OTHERSTAT_5_ID, \
                  OTHERSTAT_5_VALUE, \
                  OTHERSTAT_6_ID, \
                  OTHERSTAT_6_VALUE, \
                  OTHERSTAT_7_ID, \
                  OTHERSTAT_7_VALUE, \
                  OTHERSTAT_8_ID, \
                  OTHERSTAT_8_VALUE, \
                  OTHERSTAT_9_ID, \
                  OTHERSTAT_9_VALUE, \
                  OTHERSTAT_10_ID, \
                  OTHERSTAT_10_VALUE, \
                  RAW_PROFILE \
                  FROM OCEANBASE.__ALL_VIRTUAL_SQL_PLAN_MONITOR \
                  WHERE TENANT_ID=%ld \
                  AND TRACE_ID='%.*s' ",
                             param_tenant_id, trace_id.length(), trace_id.ptr()))) {
    LOG_WARN("failed to assign string");
  } else if (!svr_ip.empty()
             && OB_FAIL(sql.append_fmt("AND SVR_IP='%.*s' ", svr_ip.length(), svr_ip.ptr()))) {
    LOG_WARN("failed to append svr_ip");
  } else if (svr_port != 0 && OB_FAIL(sql.append_fmt("AND SVR_PORT=%ld ", svr_port))) {
    LOG_WARN("failed to append svr_port");
  } else if (!fetch_all_op && OB_FAIL(sql.append_fmt("AND PLAN_LINE_ID=%.ld ", op_id))) {
    LOG_WARN("failed to assign op_id");
  } else if (OB_FAIL(sql.append_fmt("ORDER BY PLAN_HASH_VALUE, SQL_ID, PLAN_LINE_ID "))) {
    LOG_WARN("failed to append order by op id");
  } else if (OB_FAIL(inner_get_profile(alloc, session_tenant_id, sql, profile_items))) {
    LOG_WARN("failed to get profile info");
  }
  LOG_INFO("print sql plan monitor sql", K(sql));
  return ret;
}

int ObProfileUtil::get_merged_profiles(ObIAllocator *alloc,
                                       const ObIArray<ObProfileItem> &profile_items,
                                       ObIArray<ObMergedProfileItem> &merged_profile_items,
                                       ObIArray<ExecutionBound> &execution_bounds)
{
  int ret = OB_SUCCESS;
  if (profile_items.empty()) {
  } else {
    int64_t idx = 0;
    ObMergedProfileItem merged_item;
    if (OB_FAIL(merged_item.init_from(alloc, profile_items.at(idx)))) {
      LOG_WARN("failed to init merge profile item");
    }
    while (idx < profile_items.count() && OB_SUCC(ret)) {
      const ObProfileItem &cur_item = profile_items.at(idx);
      // here we use sql_id and plan_hash_value to distinguish different sql execution plans
      if (cur_item.sql_id_ == merged_item.sql_id_
          && cur_item.plan_hash_value_ == merged_item.plan_hash_value_
          && cur_item.op_id_ == merged_item.op_id_) {
        // same op_id, merge
        if (OB_FAIL(merge_profile(*merged_item.profile_, cur_item.profile_, alloc))) {
          LOG_WARN("failed to merge profile");
        } else {
          merged_item.parallel_++;
        }
      } else {
        // different op_id, or another sql execution, save and new one
        const ObMergeMetric *metric = merged_item.profile_->get_metric(ObMetricId::DB_TIME);
        if (OB_NOT_NULL(metric)) {
          merged_item.max_db_time_ = metric->get_max_value();
        }
        if (OB_FAIL(merged_profile_items.push_back(merged_item))) {
          LOG_WARN("failed to pushback");
        } else if (OB_FAIL(merged_item.init_from(alloc, cur_item))) {
          LOG_WARN("failed to init merge profile item");
        } else if (OB_FAIL(merge_profile(*merged_item.profile_, cur_item.profile_, alloc))) {
          LOG_WARN("failed to merge profile");
        } else {
          merged_item.parallel_++;
        }
      }
      idx++;
    }
    // push back last merged profile
    if (OB_SUCC(ret)) {
      const ObMergeMetric *metric = merged_item.profile_->get_metric(ObMetricId::DB_TIME);
      if (OB_NOT_NULL(metric)) {
        merged_item.max_db_time_ = metric->get_max_value();
      }
      if (OB_FAIL(merged_profile_items.push_back(merged_item))) {
        LOG_WARN("failed to pushback");
      }
    }

    // multiple queries may executed with same trace id, we should split them
    if (OB_SUCC(ret)) {
      int64_t last_plan_hash = merged_profile_items.at(0).plan_hash_value_;
      ObString last_sql_id = merged_profile_items.at(0).sql_id_;
      int64_t start_idx = 0;
      int64_t end_idx = 0;
      int64_t execution_cnt = 1;
      for (int64_t i = 1; i < merged_profile_items.count() && OB_SUCC(ret); ++i) {
        const ObMergedProfileItem &cur_item = merged_profile_items.at(i);
        if (last_sql_id == cur_item.sql_id_ && last_plan_hash == cur_item.plan_hash_value_) {
          // in same plan execution, continue
          if (cur_item.op_id_ == -1) {
            // record the number of operator -1 (means sql compile) as the execution count of this
            // plan since operator -1 can not be paralleled
            execution_cnt++;
          }
        } else {
          // meet new plan, save previous plan's range
          last_sql_id = cur_item.sql_id_;
          last_plan_hash = cur_item.plan_hash_value_;
          end_idx = i - 1;
          if (OB_FAIL(execution_bounds.push_back({start_idx, end_idx, execution_cnt}))) {
            LOG_WARN("failed to push back");
          } else {
            start_idx = i;
            execution_cnt = 1;
          }
        }
      }
      // process last one
      if (OB_SUCC(ret)) {
        end_idx = merged_profile_items.count() - 1;
        if (OB_FAIL(execution_bounds.push_back({start_idx, end_idx, execution_cnt}))) {
          LOG_WARN("failed to push back");
        }
      }
    }
  }
  return ret;
}

int ObProfileUtil::merge_profile(ObMergedProfile &merged_profile, const ObProfile *piece_profile,
                                 ObIAllocator *alloc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(piece_profile)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("other profile is null");
  } else if (merged_profile.get_id() != piece_profile->get_id()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("profile name is not the same", K(merged_profile.get_id()), K(piece_profile->get_id()));
  } else {
    ObOpProfile<ObMetric>::MetricWrap *cur_metric_wrap = piece_profile->get_metric_head();
    while (OB_SUCC(ret) && nullptr != cur_metric_wrap) {
      ObMergeMetric *new_metric = nullptr;
      if (OB_FAIL(merged_profile.get_or_register_metric(
              static_cast<ObMetricId>(cur_metric_wrap->elem_.get_metric_id()), new_metric))) {
        LOG_WARN("failed to get or register metric", K(cur_metric_wrap->elem_.get_metric_id()));
      } else {
        new_metric->update(cur_metric_wrap->elem_.value());
      }
      cur_metric_wrap = cur_metric_wrap->next_;
    }
    if (OB_FAIL(ret)) {
    } else {
      ObOpProfile<ObMetric>::ProfileWrap *cur_child_wrap = piece_profile->get_child_head();
      while (OB_SUCC(ret) && nullptr != cur_child_wrap) {
        ObOpProfile<ObMergeMetric> *new_child = nullptr;
        if (OB_FAIL(merged_profile.get_or_register_child(cur_child_wrap->elem_->get_id(), new_child))) {
          LOG_WARN("failed to get or register child", K(cur_child_wrap->elem_->get_id()));
        } else if(OB_FAIL(merge_profile(*new_child, cur_child_wrap->elem_, alloc))) {
          LOG_WARN("failed to merge profile", K(cur_child_wrap->elem_->get_id()));
        }
        cur_child_wrap = cur_child_wrap->next_;
      }
    }
  }
  return ret;
}

int ObProfileUtil::inner_get_profile(ObIAllocator *alloc, int64_t tenant_id, const ObSqlString &sql,
                                     ObIArray<ObProfileItem> &profile_items)
{
  int ret = OB_SUCCESS;
  ObISQLClient *sql_proxy = GCTX.sql_proxy_;
  if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null sql proxy");
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      ObMySQLResult *mysql_result = nullptr;
      if (OB_FAIL(sql_proxy->read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("failed to execute sql", K(sql));
      } else if (OB_ISNULL(mysql_result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("execute sql fail", K(tenant_id), K(sql));
      }
      while (OB_SUCC(ret) && OB_SUCC(mysql_result->next())) {
        ObProfileItem profile_item;
        if (OB_FAIL(read_profile_from_result(alloc, *mysql_result, profile_item))) {
          LOG_WARN("failed to read profile info");
        } else if (nullptr == profile_item.profile_) {
          // without profile, skip it
        } else if (OB_FAIL(profile_items.push_back(profile_item))) {
          LOG_WARN("failed to push back info");
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObProfileUtil::read_profile_from_result(ObIAllocator *alloc, const ObMySQLResult &mysql_result,
                                            ObProfileItem &profile_item)
{
  int ret = OB_SUCCESS;
  int64_t int_value;
  ObString varchar_val;
  number::ObNumber num_val;

#define GET_NUM_VALUE(IDX, value)                                                                  \
  do {                                                                                             \
    if (OB_FAIL(ret)) {                                                                            \
    } else if (OB_FAIL(mysql_result.get_number(IDX, num_val))) {                                   \
      if (OB_ERR_NULL_VALUE == ret || OB_ERR_MIN_VALUE == ret || OB_ERR_MAX_VALUE == ret) {        \
        value = 0;                                                                                 \
        ret = OB_SUCCESS;                                                                          \
      } else {                                                                                     \
        LOG_WARN("failed to get number value", K(ret));                                            \
      }                                                                                            \
    } else if (OB_FAIL(num_val.cast_to_int64(int_value))) {                                        \
      LOG_WARN("failed to cast to int64", K(ret));                                                 \
    } else {                                                                                       \
      value = int_value;                                                                           \
    }                                                                                              \
  } while (0);

#define GET_INT_VALUE(IDX, value)                                                                  \
  do {                                                                                             \
    if (OB_FAIL(ret)) {                                                                            \
    } else if (OB_FAIL(mysql_result.get_int(IDX, int_value))) {                                    \
      if (OB_ERR_NULL_VALUE == ret || OB_ERR_MIN_VALUE == ret || OB_ERR_MAX_VALUE == ret) {        \
        value = 0;                                                                                 \
        ret = OB_SUCCESS;                                                                          \
      } else {                                                                                     \
        ret = OB_SUCCESS;                                                                          \
        /*retry number type*/                                                                      \
        GET_NUM_VALUE(IDX, value);                                                                 \
      }                                                                                            \
    } else {                                                                                       \
      value = int_value;                                                                           \
    }                                                                                              \
  } while (0);

#define GET_VARCHAR_VALUE(IDX, value)                                                              \
  do {                                                                                             \
    if (OB_FAIL(ret)) {                                                                            \
    } else if (OB_FAIL(mysql_result.get_varchar(IDX, varchar_val))) {                              \
      if (OB_ERR_NULL_VALUE == ret || OB_ERR_MIN_VALUE == ret || OB_ERR_MAX_VALUE == ret) {        \
        value = nullptr;                                                                           \
        ret = OB_SUCCESS;                                                                          \
      } else {                                                                                     \
        LOG_WARN("failed to get varchar value", K(ret));                                           \
      }                                                                                            \
    } else {                                                                                       \
      char *buf = nullptr;                                                                         \
      if (0 == varchar_val.length()) {                                                             \
        value = nullptr;                                                                           \
      } else if (OB_ISNULL(buf = (char *)alloc->alloc(varchar_val.length() + 1))) {                \
        ret = OB_ALLOCATE_MEMORY_FAILED;                                                           \
        LOG_WARN("failed to allocate memory", K(ret));                                             \
      } else {                                                                                     \
        MEMCPY(buf, varchar_val.ptr(), varchar_val.length());                                      \
        buf[varchar_val.length()] = '\0';                                                          \
        value.assign_ptr(buf, varchar_val.length());                                               \
      }                                                                                            \
    }                                                                                              \
  } while (0);


  GET_VARCHAR_VALUE(FETCH_COLUMN::SVR_IP, varchar_val);
  GET_INT_VALUE(FETCH_COLUMN::SVR_PORT, int_value);
  if (OB_SUCC(ret)) {
    profile_item.addr_.set_ip_addr(varchar_val, int_value);
  }
  GET_INT_VALUE(FETCH_COLUMN::THREAD_ID, profile_item.thread_id_);
  GET_INT_VALUE(FETCH_COLUMN::OP_ID, profile_item.op_id_);
  GET_INT_VALUE(FETCH_COLUMN::PLAN_DEPTH, profile_item.plan_depth_);
  GET_VARCHAR_VALUE(FETCH_COLUMN::PLAN_OPERATION, profile_item.op_name_);
  EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(mysql_result, FETCH_COLUMN::OPEN_TIME, profile_item.open_time_);
  EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(mysql_result, FETCH_COLUMN::CLOSE_TIME, profile_item.close_time_);
  EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(mysql_result, FETCH_COLUMN::FIRST_ROW_TIME, profile_item.first_row_time_);
  EXTRACT_TIMESTAMP_FIELD_MYSQL_SKIP_RET(mysql_result, FETCH_COLUMN::LAST_ROW_TIME, profile_item.last_row_time_);
  GET_INT_VALUE(FETCH_COLUMN::OUTPUT_BATCHES, profile_item.output_batches_);
  GET_INT_VALUE(FETCH_COLUMN::SKIPPED_ROWS, profile_item.skipped_rows_);
  GET_INT_VALUE(FETCH_COLUMN::RESCAN_TIMES, profile_item.rescan_times_);
  GET_INT_VALUE(FETCH_COLUMN::OUTPUT_ROWS, profile_item.output_rows_);
  GET_INT_VALUE(FETCH_COLUMN::DB_TIME, profile_item.db_time_);
  GET_INT_VALUE(FETCH_COLUMN::IO_TIME, profile_item.io_time_);
  if (OB_SUCC(ret)) {
    // db time and io time are in us, convert to ns
    profile_item.db_time_ = profile_item.db_time_ * 1000UL;
    profile_item.io_time_ = profile_item.io_time_ * 1000UL;
  }
  GET_INT_VALUE(FETCH_COLUMN::WORKAREA_MEM, profile_item.workarea_mem_);
  GET_INT_VALUE(FETCH_COLUMN::WORKAREA_MAX_MEM, profile_item.workarea_max_mem_);
  GET_INT_VALUE(FETCH_COLUMN::WORKAREA_TEMPSEG, profile_item.workarea_tempseg_);
  GET_INT_VALUE(FETCH_COLUMN::WORKAREA_MAX_TEMPSEG, profile_item.workarea_max_tempseg_);
  GET_VARCHAR_VALUE(FETCH_COLUMN::SQL_ID, profile_item.sql_id_);
  EXTRACT_UINT_FIELD_MYSQL_SKIP_RET(mysql_result, FETCH_COLUMN::PLAN_HASH_VALUE, profile_item.plan_hash_value_, uint64_t);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_1_ID, profile_item.other_1_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_1_VALUE, profile_item.other_1_value_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_2_ID, profile_item.other_2_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_2_VALUE, profile_item.other_2_value_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_3_ID, profile_item.other_3_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_3_VALUE, profile_item.other_3_value_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_4_ID, profile_item.other_4_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_4_VALUE, profile_item.other_4_value_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_5_ID, profile_item.other_5_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_5_VALUE, profile_item.other_5_value_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_6_ID, profile_item.other_6_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_6_VALUE, profile_item.other_6_value_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_7_ID, profile_item.other_7_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_7_VALUE, profile_item.other_7_value_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_8_ID, profile_item.other_8_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_8_VALUE, profile_item.other_8_value_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_9_ID, profile_item.other_9_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_9_VALUE, profile_item.other_9_value_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_10_ID, profile_item.other_10_id_);
  GET_INT_VALUE(FETCH_COLUMN::OTHERSTAT_10_VALUE, profile_item.other_10_value_);

  GET_VARCHAR_VALUE(FETCH_COLUMN::RAW_PROFILE, varchar_val);
  char *raw_profile = nullptr;
  int64_t raw_profile_len = 0;
  ObProfile *profile = nullptr;
  if (OB_SUCC(ret)) {
    raw_profile = varchar_val.ptr();
    raw_profile_len = varchar_val.length();
  }

  if (OB_FAIL(ret)) {
  } else if (raw_profile_len == 0) {
    // if profile is not collect, allocate a profile do display the common metrics and other stat
    bool enable_rich_format = false;
    ObProfileId profile_id = static_cast<ObProfileId>(sql::get_phy_type_from_name(
        profile_item.op_name_.ptr(), profile_item.op_name_.length(), enable_rich_format));
    profile_item.profile_ = OB_NEWx(ObProfile, alloc, profile_id, alloc, enable_rich_format);
    if (OB_ISNULL(profile_item.profile_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate new profile");
    }
  } else if (OB_FAIL(convert_persist_profile_to_realtime(
                 raw_profile, raw_profile_len, profile_item.profile_, alloc))) {
    LOG_WARN("failed to convert persist profile to realtime");
  }

  // fill common metrics and other stats into profile
  if (OB_FAIL(ret)) {
  // op_od == -1 means sqc compile process, do not fill common metrics into it
  } else if (profile_item.op_id_ != -1 && OB_FAIL(fill_metrics_into_profile(profile_item))) {
    LOG_WARN("failed to fill metrics");
  }
#undef GET_NUM_VALUE
#undef GET_INT_VALUE
#undef GET_VARCHAR_VALUE
  return ret;
}

int ObProfileUtil::fill_metrics_into_profile(ObProfileItem &profile_item)
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(profile_item.profile_);
  SET_METRIC_VAL(ObMetricId::OPEN_TIME, profile_item.open_time_);
  SET_METRIC_VAL(ObMetricId::CLOSE_TIME, profile_item.close_time_);
  // first row time and last row time may not filled by operator, skip it
  if (OB_SUCC(ret) && profile_item.first_row_time_ != 0) {
    SET_METRIC_VAL(ObMetricId::FIRST_ROW_TIME, profile_item.first_row_time_);
  }
  if (OB_SUCC(ret) && profile_item.last_row_time_ != 0) {
    SET_METRIC_VAL(ObMetricId::LAST_ROW_TIME, profile_item.last_row_time_);
  }
  SET_METRIC_VAL(ObMetricId::OUTPUT_BATCHES, profile_item.output_batches_);
  SET_METRIC_VAL(ObMetricId::SKIPPED_ROWS, profile_item.skipped_rows_);
  SET_METRIC_VAL(ObMetricId::RESCAN_TIMES, profile_item.rescan_times_);
  SET_METRIC_VAL(ObMetricId::OUTPUT_ROWS, profile_item.output_rows_);
  SET_METRIC_VAL(ObMetricId::DB_TIME, profile_item.db_time_);
  SET_METRIC_VAL(ObMetricId::TOTAL_IO_TIME, profile_item.io_time_);
  SET_METRIC_VAL(ObMetricId::WORKAREA_MEM, profile_item.workarea_mem_);
  SET_METRIC_VAL(ObMetricId::WORKAREA_MAX_MEM, profile_item.workarea_max_mem_);
  SET_METRIC_VAL(ObMetricId::WORKAREA_TEMPSEG, profile_item.workarea_tempseg_);
  SET_METRIC_VAL(ObMetricId::WORKAREA_MAX_TEMPSEG, profile_item.workarea_max_tempseg_);

#define FILL_OTHER_STAT(N)                                                                         \
  if (OB_SUCC(ret) && profile_item.other_##N##_id_ != 0) {                                         \
    SET_METRIC_VAL(static_cast<ObMetricId>(profile_item.other_##N##_id_),                          \
                   profile_item.other_##N##_value_);                                               \
  }
  FILL_OTHER_STAT(1);
  FILL_OTHER_STAT(2);
  FILL_OTHER_STAT(3);
  FILL_OTHER_STAT(4);
  FILL_OTHER_STAT(5);
  FILL_OTHER_STAT(6);
  FILL_OTHER_STAT(7);
  FILL_OTHER_STAT(8);
  FILL_OTHER_STAT(9);
  FILL_OTHER_STAT(10);
#undef FILL_OTHER_STAT
  return ret;
}

} // namespace common
} // namespace oceanbase
