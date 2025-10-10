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
#include "ob_dbms_xprofile.h"
#include "observer/ob_inner_sql_connection_pool.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_spi.h"
#include "share/diagnosis/ob_runtime_profile.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
using namespace sql;
using namespace common;
using namespace observer;
using namespace common::sqlclient;
namespace pl
{

int ObDbmsXprofile::display_profile(ObExecContext &ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  number::ObNumber num_val;
  char trace_id_buf[OB_MAX_TRACE_ID_BUFFER_SIZE];
  ObString trace_id;
  ObString format;
  ObString svr_ip;
  int64_t svr_port = 0;
  int64_t tenant_id = 0;
  int64_t op_id = 0;
  int64_t level = 1; // 0:CRITICAL, 1:STANDARD, 2:AD_HOC
  bool fetch_all_op = false;
  ObSQLSessionInfo *session = ctx.get_my_session();
  int idx = 0;
  if (7 != params.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params num not match");
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session");
  } else if (OB_FAIL(params.at(idx++).get_varchar(trace_id))) {
    LOG_WARN("failed to get trace_id");
  } else if (OB_FAIL(params.at(idx++).get_number(num_val))) {
    LOG_WARN("failed to get number value for tenant_id");
  } else if (OB_FAIL(num_val.cast_to_int64(tenant_id))) {
    LOG_WARN("failed to cast int");
  } else if (OB_FAIL(params.at(idx++).get_varchar(format))) {
    LOG_WARN("failed to get format");
  } else if (OB_FAIL(params.at(idx++).get_number(num_val))) {
    LOG_WARN("failed to get number value for level");
  } else if (OB_FAIL(num_val.cast_to_int64(level))) {
    LOG_WARN("failed to cast int");
  } else if (OB_FAIL(params.at(idx++).get_varchar(svr_ip))) {
    LOG_WARN("failed to get svr ip");
  } else if (OB_FAIL(params.at(idx++).get_number(num_val))) {
    LOG_WARN("failed to get number value for svr port");
  } else if (OB_FAIL(num_val.cast_to_int64(svr_port))) {
    LOG_WARN("failed to cast int");
  } else if (params.at(idx).is_null() && FALSE_IT(fetch_all_op = true)) {
  } else if (!fetch_all_op && OB_FAIL(params.at(idx++).get_number(num_val))) {
    LOG_WARN("failed to get number value for op_id", K(idx));
  } else if (!fetch_all_op && OB_FAIL(num_val.cast_to_int64(op_id))) {
    LOG_WARN("failed to cast int");
  } else {
    if (trace_id.empty()) {
      int len = session->get_last_trace_id().to_string(trace_id_buf, sizeof(trace_id_buf));
      trace_id.assign_ptr(trace_id_buf, len);
    }
    if (0 == tenant_id) {
      tenant_id = session->get_effective_tenant_id();
    }
    ProfileText profile_text;
    ObTMArray<ObProfileItem> profile_items;
    if (OB_FAIL(ObProfileUtil::get_profile_by_id(
            &ctx.get_allocator(), session->get_effective_tenant_id(), trace_id, svr_ip,
            svr_port, tenant_id, fetch_all_op, op_id, profile_items))) {
      LOG_WARN("failed to get profile info");
    } else if (OB_FAIL(set_display_type(format, profile_text.type_))) {
      LOG_WARN("failed to set profile display type");
    } else if (OB_FAIL(set_display_level(level, profile_text.display_level_))) {
      LOG_WARN("failed to set profile display level");
    } else if (OB_FAIL(format_profile_result(ctx, profile_items, trace_id, profile_text))) {
      LOG_WARN("failed to format sql profile");
    } else if (OB_FAIL(set_display_result(ctx, profile_text, result))) {
      LOG_WARN("failed to convert profile text to string");
    }
  }
  return ret;
}

int ObDbmsXprofile::set_display_type(const ObString &format, ProfileDisplayType &type)
{
  int ret = OB_SUCCESS;
  if (format.case_compare("AGGREGATED") == 0) {
    type = ProfileDisplayType::AGGREGATED;
  } else if (format.case_compare("ORIGINAL") == 0) {
    type = ProfileDisplayType::ORIGINAL;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "display format must be AGGREGATED or ORIGINAL");
    LOG_WARN("Invalid display format", K(format));
  }
  return ret;
}

int ObDbmsXprofile::set_display_level(int64_t level, metric::Level &display_level)
{
  int ret = OB_SUCCESS;
  if (level >= 0 && level <= 2) {
    display_level = static_cast<metric::Level>(level);
  } else {
    display_level = metric::Level::STANDARD;
  }
  return ret;
}

int ObDbmsXprofile::format_profile_result(ObExecContext &ctx,
                                          ObIArray<ObProfileItem> &profile_items,
                                          const ObString &trace_id, ProfileText &profile_text)
{
  int ret = OB_SUCCESS;
  profile_text.buf_len_ = 1024 * 1024;
  profile_text.pos_ = 0;
  if (profile_items.empty()) {
  } else if (OB_ISNULL(profile_text.buf_ =
                           static_cast<char *>(ctx.get_allocator().alloc(profile_text.buf_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("Failed to allocate buffer", "buffer size", profile_text.buf_len_);
  } else if (ProfileDisplayType::ORIGINAL == profile_text.type_) {
    if (OB_FAIL(flatten_op_profile(profile_items, profile_text))) {
      LOG_WARN("failed to flatten op profile");
    }
  } else if (ProfileDisplayType::AGGREGATED == profile_text.type_) {
    if (OB_FAIL(aggregate_op_profile(ctx, profile_items, trace_id, profile_text))) {
      LOG_WARN("failed to aggregate op profile");
    }
  }
  if (OB_SIZE_OVERFLOW == ret) {
    // overwrite error code to display truncated result.
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDbmsXprofile::flatten_op_profile(const ObIArray<ObProfileItem> &profile_items,
                                       ProfileText &profile_text)
{
  int ret = OB_SUCCESS;
  int64_t buf_len = profile_text.buf_len_;
  int64_t &pos = profile_text.pos_;
  char *buf = profile_text.buf_;
  metric::Level display_level = profile_text.display_level_;
  const char *json = nullptr;
  ObArenaAllocator arena_alloc;
  for (int64_t i = 0; i < profile_items.count() && OB_SUCC(ret); ++i) {
    if (i % 32 == 0) {
      arena_alloc.reset_remain_one_page();
    }
    const ObProfileItem &item = profile_items.at(i);
    int64_t format_size = item.profile_->get_format_size() + item.plan_depth_;
    if (format_size + pos > buf_len) {
      LOG_WARN("too many profile to print", K(profile_items.count()), K(pos), K(format_size),
               K(buf_len));
      break;
    } else if (OB_FAIL(item.profile_->to_format_json(&arena_alloc, json, true, display_level))) {
      LOG_WARN("failed to format profile", K(item.profile_->get_name_str()), K(buf_len), K(pos));
    } else {
      for (int64_t j = 0; j < item.plan_depth_ && OB_SUCC(ret); ++j) { OZ(BUF_PRINTF(" ")); }
      OZ(BUF_PRINTF("%ld ", item.op_id_));
      OZ(BUF_PRINTF("%s\n", json));
    }
  }
  return ret;
}

int ObDbmsXprofile::aggregate_op_profile(ObExecContext &ctx,
                                         const ObIArray<ObProfileItem> &profile_items,
                                         const ObString &trace_id,
                                         ProfileText &profile_text)
{
  int ret = OB_SUCCESS;
  ObTMArray<ObMergedProfileItem> merged_items;
  ObTMArray<ExecutionBound> execution_bounds;
  if (OB_FAIL(ObProfileUtil::get_merged_profiles(&ctx.get_allocator(), profile_items, merged_items,
                                                 execution_bounds))) {
    LOG_WARN("failed to get merged profiles");
  } else {
    int64_t buf_len = profile_text.buf_len_;
    int64_t &pos = profile_text.pos_;
    char *buf = profile_text.buf_;
    OZ(BUF_PRINTF("Trace ID: "));
    if (OB_SUCC(ret)) {
      pos += trace_id.to_string(buf + pos, buf_len - pos);
    }
    OZ(BUF_PRINTF("\n"));

    // for each execution plan, print its summary info and profile info
    for (int64_t i = 0; i < execution_bounds.count() && OB_SUCC(ret); ++i) {
      const ExecutionBound &execution_bound = execution_bounds.at(i);
      if (OB_FAIL(format_summary_info(merged_items, execution_bound, profile_text))) {
        LOG_WARN("failed to format summary info");
      } else if (OB_FAIL(format_agg_profiles(merged_items, execution_bound, profile_text))) {
        LOG_WARN("failed to format agg profile");
      }
    }
  }
  return ret;
}

int ObDbmsXprofile::format_summary_info(const ObIArray<ObMergedProfileItem> &merged_items,
                                        const ExecutionBound &execution_bound,
                                        ProfileText &profile_text)
{
  int ret = OB_SUCCESS;
  int64_t buf_len = profile_text.buf_len_;
  int64_t &pos = profile_text.pos_;
  char *buf = profile_text.buf_;

  uint64_t total_db_time = 0;
  int64_t start_idx = execution_bound.start_idx_;
  int64_t end_idx = execution_bound.end_idx_;
  ObTMArray<ObMergedProfileItem> copied_items;
  if (OB_FAIL(copied_items.reserve(end_idx - start_idx + 1))) {
    LOG_WARN("failed to reserve", K(end_idx - start_idx + 1));
  }

  for (int64_t i = start_idx; i <= end_idx && OB_SUCC(ret); ++i) {
    if (OB_FAIL(copied_items.push_back(merged_items.at(i)))) {
      LOG_WARN("failed to push back");
    } else {
      total_db_time += merged_items.at(i).max_db_time_;
    }
  }

  OZ(BUF_PRINTF("\nSQL ID: "));
  if (OB_SUCC(ret)) {
    const ObString sql_id = copied_items.at(0).sql_id_;
    int64_t execution_count = execution_bound.execution_count_;
    pos += sql_id.to_string(buf + pos, buf_len - pos);
    OZ(BUF_PRINTF(", Execution Count: %d", execution_count));
  }

  if (OB_SUCC(ret) && total_db_time > 0) {
    lib::ob_sort(copied_items.begin(), copied_items.end(), ObMergedProfileItem::cmp_by_db_time);
    OZ(BUF_PRINTF("\nTop Most Time-consuming Operators:\n"));
    static constexpr int64_t TOP_K = 10;
    double rate;
    for (int64_t k = 0; k < TOP_K && k < copied_items.count() && OB_SUCC(ret); ++k) {
      const ObMergedProfileItem &cur = copied_items.at(copied_items.count() - k - 1);
      rate = double(cur.max_db_time_) / total_db_time * 100;
      if (rate < 1) {
        break;
      }
      OZ(BUF_PRINTF("  %ld. ", k + 1));
      OZ(BUF_PRINTF("%s(id=%ld): %.3fms, (%.2f%%)\n", cur.profile_->get_name_str(), cur.op_id_,
                    double(cur.max_db_time_) / 1000 / 1000, rate));
    }
  }
  return ret;
}

int ObDbmsXprofile::format_agg_profiles(const ObIArray<ObMergedProfileItem> &merged_items,
                                        const ExecutionBound &execution_bound,
                                        ProfileText &profile_text)
{
  int ret = OB_SUCCESS;
  int64_t buf_len = profile_text.buf_len_;
  int64_t &pos = profile_text.pos_;
  char *buf = profile_text.buf_;
  OZ(BUF_PRINTF("Profile Details:\n"));

  metric::Level display_level = profile_text.display_level_;
  const char *json = nullptr;
  ObArenaAllocator arena_alloc;
  int64_t start_idx = execution_bound.start_idx_;
  int64_t end_idx = execution_bound.end_idx_;
  for (int64_t i = start_idx; i <= end_idx && OB_SUCC(ret); ++i) {
    if (i % 32 == 0) {
      arena_alloc.reset_remain_one_page();
    }
    const ObMergedProfileItem &item = merged_items.at(i);
    int64_t format_size = item.profile_->get_format_size() + item.plan_depth_;
    if (format_size + pos > buf_len) {
      LOG_WARN("too many profile to print", K(merged_items.count()), K(pos), K(format_size),
               K(buf_len));
      break;
    } else if (OB_FAIL(item.profile_->to_format_json(&arena_alloc, json, true, display_level))) {
      LOG_WARN("failed to format profile", K(item.profile_->get_name_str()), K(buf_len), K(pos));
    } else {
      for (int64_t j = 0; j < item.plan_depth_ && OB_SUCC(ret); ++j) { OZ(BUF_PRINTF(" ")); }
      OZ(BUF_PRINTF("%ld ", item.op_id_));
      OZ(BUF_PRINTF("%s\n", json));
    }
  }
  OZ(BUF_PRINTF("\n"));
  return ret;
}

int ObDbmsXprofile::set_display_result(ObExecContext &ctx, ProfileText &profile_text, ObObj &result)
{
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    if (OB_FAIL(set_display_result_for_oracle(ctx, profile_text, result))) {
      LOG_WARN("failed to set display result");
    }
  } else {
    if (OB_FAIL(set_display_result_for_mysql(ctx, profile_text, result))) {
      LOG_WARN("failed to set display result");
    }
  }
  return ret;
}

int ObDbmsXprofile::profile_text_to_strings(ProfileText &profile_text,
                                            ObIArray<common::ObString> &profile_strs)
{
  int ret = OB_SUCCESS;
  int64_t last_pos = 0;
  const char line_stop_symbol = '\n';
  for (int64_t i = 0; OB_SUCC(ret) && i < profile_text.pos_; ++i) {
    if (profile_text.buf_[i] != line_stop_symbol) {
      // keep going
    } else if (i > last_pos
               && OB_FAIL(profile_strs.push_back(
                      ObString(i - last_pos, profile_text.buf_ + last_pos)))) {
      LOG_WARN("failed to push back plan text", K(ret));
    } else {
      last_pos = i + 1;
    }
  }
  if (OB_SUCC(ret) && last_pos < profile_text.pos_) {
    if (OB_FAIL(profile_strs.push_back(
            ObString(profile_text.pos_ - last_pos, profile_text.buf_ + last_pos)))) {
      LOG_WARN("failed to push back plan text");
    }
  }
  return ret;
}

int ObDbmsXprofile::set_display_result_for_oracle(ObExecContext &exec_ctx,
                                                  ProfileText &profile_text, ObObj &result)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_ORACLE_PL
  ObSQLSessionInfo *session = exec_ctx.get_my_session();
  ObSEArray<ObString, 128> profile_strs;
  if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null param");
  } else if (OB_FAIL(profile_text_to_strings(profile_text, profile_strs))) {
    LOG_WARN("failed to convert profile text to string");
  } else {
    ObObj obj;
    ObSEArray<ObObj, 2> row;
    ObPLNestedTable *table = reinterpret_cast<ObPLNestedTable *>(
        exec_ctx.get_allocator().alloc(sizeof(ObPLNestedTable)));
    if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null param");
    } else {
      new (table) ObPLNestedTable();
      table->set_column_count(1);
      if (OB_FAIL(table->init_allocator(exec_ctx.get_allocator(), true))) {
        LOG_WARN("failed to init allocator");
      } else if (OB_FAIL(ObSPIService::spi_set_collection(session->get_effective_tenant_id(),
                                                          nullptr, *table, profile_strs.count()))) {
        LOG_WARN("failed to set collection size");
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < profile_strs.count(); ++i) {
      row.reuse();
      obj.set_varchar(profile_strs.at(i));
      if (OB_FAIL(row.push_back(obj))) {
        LOG_WARN("failed to push back object", K(ret));
      } else if (OB_FAIL(table->set_row(row, i))) {
        LOG_WARN("failed to set row", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      table->set_first(1);
      table->set_last(profile_strs.count());
      result.set_extend(reinterpret_cast<int64_t>(table), PL_NESTED_TABLE_TYPE,
                        table->get_init_size());
    }
  }
#endif // OB_BUILD_ORACLE_PL
  return ret;
}

int ObDbmsXprofile::set_display_result_for_mysql(ObExecContext &ctx, ProfileText &profile_text,
                                                 ObObj &result)
{
  int ret = OB_SUCCESS;
  ObString ret_str;
  ObTextStringResult text_res(ObTextType, true, &ctx.get_allocator());
  if (OB_FAIL(text_res.init(profile_text.pos_))) {
    LOG_WARN("failed to init text res", K(text_res), K(profile_text.pos_));
  } else if (OB_FAIL(text_res.append(profile_text.buf_, profile_text.pos_))) {
    LOG_WARN("failed to append ret_str", K(text_res));
  } else {
    text_res.get_result_buffer(ret_str);
    result.set_lob_value(ObTextType, ret_str.ptr(), ret_str.length());
    result.set_has_lob_header();
  }
  return ret;
}

} // namespace pl
} // namespace oceanbase
