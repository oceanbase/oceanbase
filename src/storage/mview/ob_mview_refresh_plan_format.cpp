/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/mview/ob_mview_refresh_plan_format.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/monitor/ob_sql_plan.h"
#include "share/diagnosis/ob_sql_monitor_statname.h"
#include "share/ob_time_utility2.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_parse.h"

using oceanbase::common::ObJsonArray;
using oceanbase::common::ObJsonBuffer;
using oceanbase::common::ObJsonInt;
using oceanbase::common::ObJsonNode;
using oceanbase::common::ObJsonNodeType;
using oceanbase::common::ObJsonObject;
using oceanbase::common::ObJsonParser;
using oceanbase::common::ObJsonString;
using oceanbase::common::ObJsonUint;

namespace oceanbase
{
namespace storage
{
using namespace sql;

namespace
{

// Column indices for the JOIN query result, must match SELECT order.
// Only columns actually used by format_merged_plan_table() are selected.
// FARM COMPAT WHITELIST: this enum is a file-local index into the JOIN result
// columns and is never serialized, so cross-version ID placeholders are not
// required.
enum PlanColumnIdx // FARM COMPAT WHITELIST
{
  // Plan structure from __ALL_VIRTUAL_SQL_PLAN
  COL_OPERATOR = 0,  // A.OPERATOR (varchar)
  COL_OBJECT_ALIAS,  // A.OBJECT_ALIAS (varchar)
  COL_ID,            // A.ID
  COL_DEPTH,         // A.DEPTH
  COL_CARDINALITY,   // A.CARDINALITY
  COL_COST,          // A.COST
  // Runtime stats from monitor subquery D
  COL_REAL_CARDINALITY,  // D.REAL_CARD
  COL_REAL_COST,
  COL_CPU_COST,
  COL_IO_COST,
  COL_OPEN_TIME_USEC,
  COL_CLOSE_TIME_USEC,
  COL_DOP,
  COL_MAX_ROWS,
  COL_SUM_DB_TIME,
  COL_STARTS,
  COL_MAX_WA_MEM,
  // OTHERSTAT pairs (10 pairs of ID+VALUE = 20 columns)
  COL_OTHERSTAT_1_ID,
  COL_OTHERSTAT_1_VALUE,
  COL_OTHERSTAT_2_ID,
  COL_OTHERSTAT_2_VALUE,
  COL_OTHERSTAT_3_ID,
  COL_OTHERSTAT_3_VALUE,
  COL_OTHERSTAT_4_ID,
  COL_OTHERSTAT_4_VALUE,
  COL_OTHERSTAT_5_ID,
  COL_OTHERSTAT_5_VALUE,
  COL_OTHERSTAT_6_ID,
  COL_OTHERSTAT_6_VALUE,
  COL_OTHERSTAT_7_ID,
  COL_OTHERSTAT_7_VALUE,
  COL_OTHERSTAT_8_ID,
  COL_OTHERSTAT_8_VALUE,
  COL_OTHERSTAT_9_ID,
  COL_OTHERSTAT_9_VALUE,
  COL_OTHERSTAT_10_ID,
  COL_OTHERSTAT_10_VALUE,
  // Server addresses from monitor subquery (GROUP_CONCAT of SVR_IP:SVR_PORT)
  COL_SERVER,
  // Predicates and plan-level info from __ALL_VIRTUAL_SQL_PLAN
  COL_FILTER_PREDICATES,
  COL_STARTUP_PREDICATES,
  COL_SPECIAL_PREDICATES,
  COL_OTHER_XML,
  COL_OPTIMIZER,
  COL_PARTITION_START,
  COL_PARTITION_STOP
};

// Per-operator monitor data (timestamps, DOP, skew metrics) from plan monitor,
// stored alongside ObSqlPlanItem* in a parallel array since ObSqlPlanItem
// has no such fields.
struct MViewPlanMonitorTime {
  int64_t open_time_;
  int64_t close_time_;
  int64_t dop_;
  int64_t max_rows_;
  int64_t sum_db_time_;
  int64_t starts_;
  int64_t max_wa_mem_;
  char *server_;

  MViewPlanMonitorTime()
      : open_time_(0),
        close_time_(0),
        dop_(0),
        max_rows_(0),
        sum_db_time_(0),
        starts_(0),
        max_wa_mem_(0),
        server_(NULL)
  {}

  TO_STRING_KV(K_(open_time),
               K_(close_time),
               K_(dop),
               K_(max_rows),
               K_(sum_db_time),
               K_(starts),
               K_(max_wa_mem),
               KP_(server));
};

// Display column definitions — single source of truth for column count and headers.
// To add a column: (1) append name + enum value here, (2) add format block in
// fill_plan_row_vals(). Phase1/Phase2 loops in format_merged_plan_table are generic.
static const char *const g_mview_col_names[] = {
"ID",
"OPERATOR",
"NAME",
"EST.ROWS",
"EST.TIME(us)",
"REAL.ROWS",
"REAL.TIME(us)",
"IO TIME(us)",
"CPU TIME(us)",
"OPEN TIME",
"CLOSE TIME",
"DOP",
"ROWS SKEW",
"TIME SKEW",
"STARTS",
"MAX MEM",
"SERVER",
};
static const int64_t MVIEW_COL_COUNT = ARRAYSIZEOF(g_mview_col_names);
static const int64_t PLAN_INDENT_PER_DEPTH = 2;

// Unicode box-drawing characters for plan tree, matching display_cursor output.
// Each prefix unit is 2 display characters wide.
static const char TREE_VERTICAL[] = "│ ";
static const int64_t TREE_VERTICAL_LEN = ARRAYSIZEOF(TREE_VERTICAL) - 1;
static const char TREE_BRANCH[]   = "├─";
static const int64_t TREE_BRANCH_LEN = ARRAYSIZEOF(TREE_BRANCH) - 1;
static const char TREE_CORNER[]   = "└─";
static const int64_t TREE_CORNER_LEN = ARRAYSIZEOF(TREE_CORNER) - 1;

// Number of OTHERSTAT slots per operator in __ALL_VIRTUAL_SQL_PLAN_MONITOR
// (fixed at 10 by ObMonitorNode definition).
static const int64_t OTHERSTAT_SLOT_COUNT = 10;

// Whitelist of OTHERSTAT IDs relevant to mview refresh troubleshooting.
// Only these stats are shown in operator detail output to keep it concise.
static const int64_t MVIEW_OTHERSTAT_WHITELIST[] = {
// Table scan I/O and row counts
sql::ObSqlMonitorStatIds::IO_READ_BYTES,
sql::ObSqlMonitorStatIds::BASE_READ_ROW_CNT,
sql::ObSqlMonitorStatIds::DELTA_READ_ROW_CNT,
sql::ObSqlMonitorStatIds::TOTAL_READ_ROW_COUNT,
sql::ObSqlMonitorStatIds::STORAGE_FILTERED_ROW_CNT,
sql::ObSqlMonitorStatIds::BLOCKSCAN_ROW_CNT,
sql::ObSqlMonitorStatIds::SKIP_INDEX_SKIP_BLOCK_CNT,
// Hash operator stats
sql::ObSqlMonitorStatIds::HASH_BUCKET_COUNT,
sql::ObSqlMonitorStatIds::HASH_NON_EMPTY_BUCKET_COUNT,
sql::ObSqlMonitorStatIds::HASH_ROW_COUNT,
// Sort operator stats
sql::ObSqlMonitorStatIds::SORT_SORTED_ROW_COUNT,
sql::ObSqlMonitorStatIds::SORT_MERGE_SORT_ROUND,
// Memory pressure
sql::ObSqlMonitorStatIds::MEMORY_DUMP,
};
static const int64_t MVIEW_OTHERSTAT_WHITELIST_COUNT = ARRAYSIZEOF(MVIEW_OTHERSTAT_WHITELIST);

enum MergedCol
{
  MCOL_ID = 0,
  MCOL_OPERATOR,
  MCOL_NAME,
  MCOL_EST_ROWS,
  MCOL_EST_TIME,
  MCOL_REAL_ROWS,
  MCOL_REAL_TIME,
  MCOL_IO_TIME,
  MCOL_CPU_TIME,
  MCOL_OPEN_TIME,
  MCOL_CLOSE_TIME,
  MCOL_DOP,
  MCOL_ROWS_SKEW,
  MCOL_TIME_SKEW,
  MCOL_STARTS,
  MCOL_MAX_WA_MEM,
  MCOL_SERVER,
};

struct MViewPlanRowVals {
  const char *vals_[MVIEW_COL_COUNT];
  // OPERATOR column uses Unicode tree chars whose byte length exceeds display
  // width. operator_extra_bytes_ = (byte_length - display_width), used to
  // compensate %-*s padding by bytes.
  int64_t operator_extra_bytes_;

  MViewPlanRowVals() : operator_extra_bytes_(0) { MEMSET(vals_, 0, sizeof(vals_)); }

  TO_STRING_KV("col_count", static_cast<int64_t>(MVIEW_COL_COUNT));
};

static int64_t get_plan_row_display_len(const MViewPlanRowVals &row, int64_t col)
{
  int64_t display_len = OB_NOT_NULL(row.vals_[col]) ? static_cast<int64_t>(STRLEN(row.vals_[col])) : 0;
  if (MCOL_OPERATOR == col) {
    display_len -= row.operator_extra_bytes_;
  }
  return display_len;
}

static int get_plan_row_pad_width(const MViewPlanRowVals &row, int64_t col, int col_width)
{
  int pad_width = col_width;
  if (MCOL_OPERATOR == col) {
    pad_width += static_cast<int>(row.operator_extra_bytes_);
  }
  return pad_width;
}

struct MViewPlanOtherStat {
  int64_t ids_[OTHERSTAT_SLOT_COUNT];
  int64_t values_[OTHERSTAT_SLOT_COUNT];

  MViewPlanOtherStat()
  {
    MEMSET(ids_, 0, sizeof(ids_));
    MEMSET(values_, 0, sizeof(values_));
  }

  TO_STRING_KV("slot_count", static_cast<int64_t>(OTHERSTAT_SLOT_COUNT));
};

/**
 * Read an int column from result, treating NULL/overflow as 0.
 * Falls back to ObNumber path for type-mismatched columns (e.g. CAST results).
 */
static int read_result_int(common::sqlclient::ObMySQLResult &result, int64_t idx, int64_t &out)
{
  int ret = OB_SUCCESS;
  out = 0;
  if (OB_FAIL(result.get_int(idx, out))) {
    if (OB_ERR_NULL_VALUE == ret || OB_ERR_MIN_VALUE == ret || OB_ERR_MAX_VALUE == ret) {
      out = 0;
      ret = OB_SUCCESS;
    } else {
      number::ObNumber num_val;
      int get_num_ret = OB_SUCCESS;
      if (OB_UNLIKELY(OB_SUCCESS != (get_num_ret = result.get_number(idx, num_val)))) {
        LOG_WARN("failed to get number value, fallback also failed",
                 KR(ret), K(get_num_ret));
      } else if (OB_FAIL(num_val.cast_to_int64(out))) {
        LOG_WARN("failed to cast to int64", KR(ret));
      }
    }
  }
  return ret;
}

/**
 * Read an int column from result, treating NULL/overflow as 0.
 * Unlike read_result_int, does not attempt ObNumber fallback.
 */
static int read_result_simple_int(common::sqlclient::ObMySQLResult &result, int64_t idx, int64_t &out)
{
  int ret = OB_SUCCESS;
  out = 0;
  if (OB_FAIL(result.get_int(idx, out))) {
    if (OB_ERR_NULL_VALUE == ret || OB_ERR_MIN_VALUE == ret || OB_ERR_MAX_VALUE == ret) {
      out = 0;
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

/**
 * Read a varchar column from result, arena-allocate a copy.
 * Treats NULL as empty (out_ptr=NULL, out_len=0).
 */
static int read_result_varchar(common::ObIAllocator &allocator,
                               common::sqlclient::ObMySQLResult &result,
                               int64_t idx,
                               char *&out_ptr,
                               int64_t &out_len)
{
  int ret = OB_SUCCESS;
  ObString varchar_val;
  out_ptr = NULL;
  out_len = 0;
  if (OB_FAIL(result.get_varchar(idx, varchar_val))) {
    if (OB_ERR_NULL_VALUE == ret || OB_ERR_MIN_VALUE == ret || OB_ERR_MAX_VALUE == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get varchar value", KR(ret));
    }
  } else if (varchar_val.length() > 0) {
    char *buf = NULL;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(varchar_val.length() + 1)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", KR(ret));
    } else {
      MEMCPY(buf, varchar_val.ptr(), varchar_val.length());
      buf[varchar_val.length()] = '\0';
      out_ptr = buf;
      out_len = varchar_val.length();
    }
  }
  return ret;
}

static int mview_read_plan_info_from_result(common::ObIAllocator &allocator,
                                            common::sqlclient::ObMySQLResult &mysql_result,
                                            ObSqlPlanItem &plan_info,
                                            MViewPlanMonitorTime &monitor_time,
                                            MViewPlanOtherStat &other_stat,
                                            char *&server_str)
{
  int ret = OB_SUCCESS;
  int64_t int_val = 0;
  int64_t server_len = 0;

  if (OB_FAIL(read_result_varchar(allocator,
                                  mysql_result,
                                  COL_OPERATOR,
                                  plan_info.operation_,
                                  plan_info.operation_len_))) {
    LOG_WARN("fail to read operator column", KR(ret));
  } else if (OB_FAIL(read_result_varchar(allocator,
                                         mysql_result,
                                         COL_OBJECT_ALIAS,
                                         plan_info.object_alias_,
                                         plan_info.object_alias_len_))) {
    LOG_WARN("fail to read object_alias column", KR(ret));
  } else if (OB_FAIL(read_result_int(mysql_result, COL_ID, int_val))) {
    LOG_WARN("fail to read id column", KR(ret));
  } else if (OB_FALSE_IT(plan_info.id_ = static_cast<int>(int_val))) {
  } else if (OB_FAIL(read_result_int(mysql_result, COL_DEPTH, int_val))) {
    LOG_WARN("fail to read depth column", KR(ret));
  } else if (OB_FALSE_IT(plan_info.depth_ = static_cast<int>(int_val))) {
  } else if (OB_FAIL(read_result_int(mysql_result, COL_CARDINALITY, plan_info.cardinality_))) {
    LOG_WARN("fail to read cardinality", KR(ret));
  } else if (OB_FAIL(read_result_int(mysql_result, COL_COST, plan_info.cost_))) {
    LOG_WARN("fail to read cost", KR(ret));
  } else if (OB_FAIL(read_result_int(mysql_result, COL_REAL_CARDINALITY, plan_info.real_cardinality_))) {
    LOG_WARN("fail to read real_cardinality", KR(ret));
  } else if (OB_FAIL(read_result_int(mysql_result, COL_REAL_COST, plan_info.real_cost_))) {
    LOG_WARN("fail to read real_cost", KR(ret));
  } else if (OB_FAIL(read_result_int(mysql_result, COL_CPU_COST, plan_info.cpu_cost_))) {
    LOG_WARN("fail to read cpu_cost", KR(ret));
  } else if (OB_FAIL(read_result_int(mysql_result, COL_IO_COST, plan_info.io_cost_))) {
    LOG_WARN("fail to read io_cost", KR(ret));
  } else if (OB_FAIL(read_result_simple_int(mysql_result, COL_OPEN_TIME_USEC, monitor_time.open_time_))) {
    LOG_WARN("fail to read open_time", KR(ret));
  } else if (OB_FAIL(read_result_simple_int(mysql_result, COL_CLOSE_TIME_USEC, monitor_time.close_time_))) {
    LOG_WARN("fail to read close_time", KR(ret));
  } else if (OB_FAIL(read_result_simple_int(mysql_result, COL_DOP, monitor_time.dop_))) {
    LOG_WARN("fail to read dop", KR(ret));
  } else if (OB_FAIL(read_result_simple_int(mysql_result, COL_MAX_ROWS, monitor_time.max_rows_))) {
    LOG_WARN("fail to read max_rows", KR(ret));
  } else if (OB_FAIL(read_result_simple_int(mysql_result, COL_SUM_DB_TIME, monitor_time.sum_db_time_))) {
    LOG_WARN("fail to read sum_db_time", KR(ret));
  } else if (OB_FAIL(read_result_simple_int(mysql_result, COL_STARTS, monitor_time.starts_))) {
    LOG_WARN("fail to read starts", KR(ret));
  } else if (OB_FAIL(read_result_simple_int(mysql_result, COL_MAX_WA_MEM, monitor_time.max_wa_mem_))) {
    LOG_WARN("fail to read max_wa_mem", KR(ret));
  } else if (OB_FAIL(read_result_varchar(allocator, mysql_result, COL_SERVER, server_str, server_len))) {
    LOG_WARN("fail to read server", KR(ret));
  } else if (OB_FAIL(read_result_varchar(allocator, mysql_result, COL_FILTER_PREDICATES,
                                         plan_info.filter_predicates_,
                                         plan_info.filter_predicates_len_))) {
    LOG_WARN("fail to read filter_predicates", KR(ret));
  } else if (OB_FAIL(read_result_varchar(allocator, mysql_result, COL_STARTUP_PREDICATES,
                                         plan_info.startup_predicates_,
                                         plan_info.startup_predicates_len_))) {
    LOG_WARN("fail to read startup_predicates", KR(ret));
  } else if (OB_FAIL(read_result_varchar(allocator, mysql_result, COL_SPECIAL_PREDICATES,
                                         plan_info.special_predicates_,
                                         plan_info.special_predicates_len_))) {
    LOG_WARN("fail to read special_predicates", KR(ret));
  } else if (OB_FAIL(read_result_varchar(allocator, mysql_result, COL_OTHER_XML,
                                         plan_info.other_xml_,
                                         plan_info.other_xml_len_))) {
    LOG_WARN("fail to read other_xml", KR(ret));
  } else if (OB_FAIL(read_result_varchar(allocator, mysql_result, COL_OPTIMIZER,
                                         plan_info.optimizer_,
                                         plan_info.optimizer_len_))) {
    LOG_WARN("fail to read optimizer", KR(ret));
  } else if (OB_FAIL(read_result_varchar(allocator, mysql_result, COL_PARTITION_START,
                                         plan_info.partition_start_,
                                         plan_info.partition_start_len_))) {
    LOG_WARN("fail to read partition_start", KR(ret));
  } else if (OB_FAIL(read_result_varchar(allocator, mysql_result, COL_PARTITION_STOP,
                                         plan_info.partition_stop_,
                                         plan_info.partition_stop_len_))) {
    LOG_WARN("fail to read partition_stop", KR(ret));
  }

  for (int64_t s = 0; OB_SUCC(ret) && s < OTHERSTAT_SLOT_COUNT; ++s) {
    if (OB_FAIL(read_result_simple_int(mysql_result, COL_OTHERSTAT_1_ID + s * 2, other_stat.ids_[s]))) {
      LOG_WARN("fail to read otherstat id", KR(ret), K(s));
    } else if (OB_FAIL(read_result_simple_int(mysql_result, COL_OTHERSTAT_1_VALUE + s * 2, other_stat.values_[s]))) {
      LOG_WARN("fail to read otherstat value", KR(ret), K(s));
    }
  }

  return ret;
}

/** Arena-allocate a null-terminated copy of src[0..len). Core building block for alloc_*_string. */
static int alloc_cstring(common::ObIAllocator &allocator, const char *src, int64_t len, const char *&out)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(len + 1)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc string buf", KR(ret), K(len));
  } else {
    if (len > 0 && src != NULL) {
      MEMCPY(buf, src, len);
    }
    buf[len] = '\0';
    out = buf;
  }
  return ret;
}

static int alloc_int64_string(common::ObIAllocator &allocator, int64_t val, const char *&out)
{
  char buf[32];
  snprintf(buf, sizeof(buf), "%ld", val);
  return alloc_cstring(allocator, buf, STRLEN(buf), out);
}

static int alloc_time_string(common::ObIAllocator &allocator, int64_t usec, const char *&out)
{
  int ret = OB_SUCCESS;
  char buf[64];
  int64_t pos = 0;
  if (OB_FAIL(share::ObTimeUtility2::usec_to_str(usec, buf, sizeof(buf), pos))) {
    LOG_WARN("fail to format usec to string", KR(ret), K(usec));
  } else {
    ret = alloc_cstring(allocator, buf, pos, out);
  }
  return ret;
}

/**
 * Derive is_last_child_ for every item in plan_infos from the depth sequence.
 * Walk pre-order with a stack of indices.
 */
static int compute_is_last_child(const ObIArray<ObSqlPlanItem *> &plan_infos)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<int64_t, 64> stack;
  int prev_depth = -1;
  for (int64_t i = 0; OB_SUCC(ret) && i < plan_infos.count(); ++i) {
    ObSqlPlanItem *item = plan_infos.at(i);
    if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null plan item", KR(ret), K(i));
    } else if (OB_UNLIKELY(item->depth_ < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected negative plan depth", KR(ret), K(i), KPC(item));
    } else if (OB_UNLIKELY(0 == i && 0 != item->depth_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected root plan depth", KR(ret), K(i), KPC(item));
    } else if (OB_UNLIKELY(i > 0 && item->depth_ > prev_depth + 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected discontinuous plan depth", KR(ret), K(i), K(prev_depth), KPC(item));
    } else {
      int d = item->depth_;
      bool done = false;
      for (; OB_SUCC(ret) && stack.count() > 0 && !done; ) {
        int64_t top_idx = stack.at(stack.count() - 1);
        ObSqlPlanItem *top = plan_infos.at(top_idx);
        if (OB_NOT_NULL(top) && top->depth_ < d) {
          done = true;
        } else if (OB_NOT_NULL(top)) {
          if (top->depth_ == d) {
            top->is_last_child_ = false;
          } else {
            top->is_last_child_ = true;
          }
          stack.pop_back();
        } else {
          stack.pop_back();
        }
      }
      item->is_last_child_ = true;
      if (OB_FAIL(stack.push_back(i))) {
        LOG_WARN("fail to push plan depth stack", KR(ret), K(i));
      } else {
        prev_depth = d;
      }
    }
  }
  for (int64_t s = 0; OB_SUCC(ret) && s < stack.count(); ++s) {
    ObSqlPlanItem *node = plan_infos.at(stack.at(s));
    if (OB_ISNULL(node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null plan item in stack", KR(ret), K(s));
    } else {
      node->is_last_child_ = true;
    }
  }
  return ret;
}

/**
 * Prepend Unicode tree-drawing prefix (│ ├─ └─) to operator text.
 * ancestor_indices[0..depth] is the ancestor chain; the node at depth is self.
 * Returns operator_extra_bytes = byte_len - display_width for %-*s compensation.
 */
static int alloc_tree_prefix_string(common::ObIAllocator &allocator,
                                    const ObIArray<ObSqlPlanItem *> &plan_infos,
                                    const int64_t *ancestor_indices,
                                    int64_t depth,
                                    const char *text,
                                    int64_t text_len,
                                    const char *&out,
                                    int64_t &extra_bytes)
{
  int ret = OB_SUCCESS;
  extra_bytes = 0;
  if (OB_ISNULL(ancestor_indices) || OB_UNLIKELY(depth < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tree prefix argument", KR(ret), KP(ancestor_indices), K(depth));
  } else {
    // Each depth level occupies 2 display chars but variable bytes in UTF-8.
    // When depth==0 the loop below is skipped and pos stays 0 (no prefix).
    int64_t max_prefix_bytes = static_cast<int64_t>(depth) * TREE_CORNER_LEN;
    int64_t alloc_size = max_prefix_bytes + text_len + 1;
    char *buf = NULL;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(alloc_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc tree prefix buf", KR(ret));
    } else {
      int64_t pos = 0;
      for (int64_t level = 1; OB_SUCC(ret) && level <= depth; ++level) {
        int64_t anc_idx = ancestor_indices[level];
        const ObSqlPlanItem *anc = NULL;
        bool last = false;
        if (OB_UNLIKELY(anc_idx < 0 || anc_idx >= plan_infos.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid ancestor index", KR(ret), K(level), K(anc_idx), K(plan_infos.count()));
        } else if (OB_ISNULL(anc = plan_infos.at(anc_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null ancestor item", KR(ret), K(level), K(anc_idx));
        } else if (OB_FALSE_IT(last = anc->is_last_child_)) {
        } else if (level == depth) {
          const char *ch = last ? TREE_CORNER : TREE_BRANCH;
          int64_t ch_len = last ? TREE_CORNER_LEN : TREE_BRANCH_LEN;
          MEMCPY(buf + pos, ch, ch_len);
          pos += ch_len;
        } else if (last) {
          buf[pos]     = ' ';
          buf[pos + 1] = ' ';
          pos += 2;
        } else {
          MEMCPY(buf + pos, TREE_VERTICAL, TREE_VERTICAL_LEN);
          pos += TREE_VERTICAL_LEN;
        }
      }
      if (OB_SUCC(ret)) {
        int64_t display_width = static_cast<int64_t>(depth) * PLAN_INDENT_PER_DEPTH;
        extra_bytes = pos - display_width;
        if (text_len > 0 && OB_NOT_NULL(text)) {
          MEMCPY(buf + pos, text, text_len);
        }
        buf[pos + text_len] = '\0';
        out = buf;
      }
    }
  }
  return ret;
}

static int alloc_skew_string(common::ObIAllocator &allocator,
                             int64_t dop,
                             double numerator,
                             double denominator,
                             const char *&out)
{
  int ret = OB_SUCCESS;
  if (dop <= 1 || denominator <= 0) {
    if (OB_FAIL(alloc_cstring(allocator, "-", 1, out))) {
      LOG_WARN("fail to alloc skew default string", KR(ret));
    }
  } else {
    char buf[32];
    snprintf(buf, sizeof(buf), "%.2f", numerator * dop / denominator);
    if (OB_FAIL(alloc_cstring(allocator, buf, STRLEN(buf), out))) {
      LOG_WARN("fail to alloc skew string", KR(ret));
    }
  }
  return ret;
}

/**
 * Populate row.vals_[] with arena-allocated null-terminated strings.
 * This is the single place to modify when adding a new display column
 * (together with g_mview_col_names[] and MergedCol enum).
 */
static int fill_plan_row_vals(common::ObIAllocator &allocator,
                              const ObIArray<ObSqlPlanItem *> &plan_infos,
                              const int64_t *ancestor_indices,
                              const ObSqlPlanItem &item,
                              const MViewPlanMonitorTime &mtime,
                              MViewPlanRowVals &row)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(alloc_int64_string(allocator, item.id_, row.vals_[MCOL_ID]))) {
    LOG_WARN("fail to alloc id string", KR(ret));
  } else if (OB_FAIL(alloc_tree_prefix_string(allocator,
                                              plan_infos,
                                              ancestor_indices,
                                              static_cast<int64_t>(item.depth_),
                                              item.operation_,
                                              item.operation_ != NULL ? item.operation_len_ : 0,
                                              row.vals_[MCOL_OPERATOR],
                                              row.operator_extra_bytes_))) {
    LOG_WARN("fail to alloc operator string", KR(ret));
  } else if (OB_FAIL(alloc_cstring(allocator,
                                   item.object_alias_,
                                   item.object_alias_ != NULL ? item.object_alias_len_ : 0,
                                   row.vals_[MCOL_NAME]))) {
    LOG_WARN("fail to alloc name string", KR(ret));
  } else if (OB_FAIL(alloc_int64_string(allocator, item.cardinality_, row.vals_[MCOL_EST_ROWS]))) {
    LOG_WARN("fail to alloc est_rows string", KR(ret));
  } else if (OB_FAIL(alloc_int64_string(allocator, item.cost_, row.vals_[MCOL_EST_TIME]))) {
    LOG_WARN("fail to alloc est_time string", KR(ret));
  } else if (OB_FAIL(alloc_int64_string(allocator, item.real_cardinality_, row.vals_[MCOL_REAL_ROWS]))) {
    LOG_WARN("fail to alloc real_rows string", KR(ret));
  } else if (OB_FAIL(alloc_int64_string(allocator, item.real_cost_, row.vals_[MCOL_REAL_TIME]))) {
    LOG_WARN("fail to alloc real_time string", KR(ret));
  } else if (OB_FAIL(alloc_int64_string(allocator, item.io_cost_, row.vals_[MCOL_IO_TIME]))) {
    LOG_WARN("fail to alloc io_time string", KR(ret));
  } else if (OB_FAIL(alloc_int64_string(allocator, item.cpu_cost_, row.vals_[MCOL_CPU_TIME]))) {
    LOG_WARN("fail to alloc cpu_time string", KR(ret));
  } else if (OB_FAIL(alloc_time_string(allocator, mtime.open_time_, row.vals_[MCOL_OPEN_TIME]))) {
    LOG_WARN("fail to alloc open_time string", KR(ret));
  } else if (OB_FAIL(alloc_time_string(allocator, mtime.close_time_, row.vals_[MCOL_CLOSE_TIME]))) {
    LOG_WARN("fail to alloc close_time string", KR(ret));
  } else if (OB_FAIL(alloc_int64_string(allocator, mtime.dop_, row.vals_[MCOL_DOP]))) {
    LOG_WARN("fail to alloc dop string", KR(ret));
  } else if (OB_FAIL(alloc_skew_string(allocator,
                                       mtime.dop_,
                                       static_cast<double>(mtime.max_rows_),
                                       static_cast<double>(item.real_cardinality_),
                                       row.vals_[MCOL_ROWS_SKEW]))) {
    LOG_WARN("fail to alloc rows_skew string", KR(ret));
  } else if (OB_FAIL(alloc_skew_string(allocator,
                                       mtime.dop_,
                                       static_cast<double>(item.cpu_cost_),
                                       static_cast<double>(mtime.sum_db_time_),
                                       row.vals_[MCOL_TIME_SKEW]))) {
    LOG_WARN("fail to alloc time_skew string", KR(ret));
  } else if (OB_FAIL(alloc_int64_string(allocator, mtime.starts_, row.vals_[MCOL_STARTS]))) {
    LOG_WARN("fail to alloc starts string", KR(ret));
  } else if (OB_FAIL(alloc_int64_string(allocator, mtime.max_wa_mem_, row.vals_[MCOL_MAX_WA_MEM]))) {
    LOG_WARN("fail to alloc max_wa_mem string", KR(ret));
  } else if (OB_FAIL(alloc_cstring(allocator,
                                   mtime.server_,
                                   OB_NOT_NULL(mtime.server_) ? STRLEN(mtime.server_) : 0,
                                   row.vals_[MCOL_SERVER]))) {
    LOG_WARN("fail to alloc server string", KR(ret));
  }

  return ret;
}

static int append_separator_line(ObSqlString &result, int64_t total_width,
                                    char ch, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  static const int64_t CHUNK_SIZE = 128;
  char *buf = static_cast<char *>(allocator.alloc(CHUNK_SIZE));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc separator buffer", KR(ret));
  } else {
    MEMSET(buf, static_cast<int>(ch), CHUNK_SIZE);
    int64_t remaining = total_width;
    while (OB_SUCC(ret) && remaining > 0) {
      int64_t len = (remaining > CHUNK_SIZE) ? CHUNK_SIZE : remaining;
      if (OB_FAIL(result.append(buf, len))) {
        LOG_WARN("fail to append separator", KR(ret));
      }
      remaining -= len;
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(result.append("\n"))) {
    LOG_WARN("fail to append newline", KR(ret));
  }
  return ret;
}

static bool is_whitelisted_otherstat(int64_t stat_id)
{
  bool found = false;
  for (int64_t i = 0; !found && i < MVIEW_OTHERSTAT_WHITELIST_COUNT; ++i) {
    if (MVIEW_OTHERSTAT_WHITELIST[i] == stat_id) {
      found = true;
    }
  }
  return found;
}

static int format_operator_detail_stats(const ObIArray<ObSqlPlanItem *> &plan_infos,
                                        const ObIArray<MViewPlanOtherStat> &other_stats,
                                        ObSqlString &result)
{
  int ret = OB_SUCCESS;
  bool has_header = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < plan_infos.count() && i < other_stats.count(); ++i) {
    const ObSqlPlanItem *item = plan_infos.at(i);
    const MViewPlanOtherStat &ostat = other_stats.at(i);
    if (OB_NOT_NULL(item)) {
      bool has_op_header = false;
      for (int64_t s = 0; OB_SUCC(ret) && s < OTHERSTAT_SLOT_COUNT; ++s) {
        int64_t stat_id = ostat.ids_[s];
        if (is_whitelisted_otherstat(stat_id)) {
          if (!has_header) {
            if (OB_FAIL(result.append("\nOperator Details:\n"))) {
              LOG_WARN("fail to append operator details header", KR(ret));
            } else {
              has_header = true;
            }
          }
          if (OB_SUCC(ret) && !has_op_header) {
            const char *op_name = "";
            int64_t op_name_len = 0;
            const char *obj_name = "";
            int64_t obj_name_len = 0;
            int op_id = item->id_;
            if (OB_NOT_NULL(item->operation_)) {
              op_name = item->operation_;
              op_name_len = item->operation_len_;
            }
            if (OB_NOT_NULL(item->object_alias_)) {
              obj_name = item->object_alias_;
              obj_name_len = item->object_alias_len_;
            }
            if (OB_FAIL(result.append_fmt("  [%d] %.*s (%.*s):\n",
                                          op_id,
                                          static_cast<int>(op_name_len),
                                          op_name,
                                          static_cast<int>(obj_name_len),
                                          obj_name))) {
              LOG_WARN("fail to append operator header", KR(ret));
            } else {
              has_op_header = true;
            }
          }
          if (OB_SUCC(ret) && stat_id > sql::ObSqlMonitorStatIds::MONITOR_STATNAME_BEGIN
              && stat_id < sql::ObSqlMonitorStatIds::MONITOR_STATNAME_END) {
            const char *stat_name = sql::OB_MONITOR_STATS[stat_id].name_;
            if (OB_FAIL(result.append_fmt("    - %s: %ld\n", stat_name, ostat.values_[s]))) {
              LOG_WARN("fail to append stat detail", KR(ret), K(stat_id));
            }
          }
        }
      }
    }
  }
  return ret;
}

// plan_infos and monitor_times must be index-aligned and same count.
static int format_merged_plan_table(common::ObIAllocator &allocator,
                                    const ObIArray<ObSqlPlanItem *> &plan_infos,
                                    const ObIArray<MViewPlanMonitorTime> &monitor_times,
                                    ObSqlString &result)
{
  int ret = OB_SUCCESS;
  int prev_depth = -1;

  // ancestor_indices[level] = index of the most recent node at each depth.
  // Pre-order guarantees that the most recent node at level < current_depth
  // is the current node's ancestor at that level.
  common::ObSEArray<int64_t, 128> ancestor_indices;
  ObSEArray<MViewPlanRowVals, 1> all_rows;
  if (plan_infos.count() != monitor_times.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan_infos and monitor_times count mismatch",
             KR(ret), K(plan_infos.count()), K(monitor_times.count()));
  } else if (OB_FAIL(all_rows.reserve(plan_infos.count()))) {
    LOG_WARN("fail to reserve all_rows", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < plan_infos.count(); ++i) {
    ObSqlPlanItem *item = plan_infos.at(i);
    if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null plan item", KR(ret), K(i));
    } else if (OB_UNLIKELY(item->depth_ < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected negative plan depth", KR(ret), K(i), KPC(item));
    } else if (OB_UNLIKELY(0 == i && 0 != item->depth_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected root plan depth", KR(ret), K(i), KPC(item));
    } else if (OB_UNLIKELY(i > 0 && item->depth_ > prev_depth + 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected discontinuous plan depth", KR(ret), K(i), K(prev_depth), KPC(item));
    } else {
      int64_t d = item->depth_;
      for (int64_t g = ancestor_indices.count(); OB_SUCC(ret) && g <= d; ++g) {
        if (OB_FAIL(ancestor_indices.push_back(-1))) {
          LOG_WARN("fail to grow ancestor_indices", KR(ret), K(g));
        }
      }
      if (OB_SUCC(ret)) {
        ancestor_indices.at(d) = i;
        MViewPlanRowVals row;
        if (OB_FAIL(fill_plan_row_vals(allocator, plan_infos,
                                       &ancestor_indices.at(0),
                                       *item, monitor_times.at(i), row))) {
          LOG_WARN("fail to fill plan row vals", KR(ret), K(i));
        } else if (OB_FAIL(all_rows.push_back(row))) {
          LOG_WARN("fail to push back row vals", KR(ret));
        } else {
          prev_depth = d;
        }
      }
    }
  }

  int col_widths[MVIEW_COL_COUNT];
  int64_t total_width = MVIEW_COL_COUNT + 1;
  for (int64_t c = 0; c < MVIEW_COL_COUNT; ++c) {
    col_widths[c] = static_cast<int>(STRLEN(g_mview_col_names[c]));
    total_width += col_widths[c];
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_rows.count(); ++i) {
    const MViewPlanRowVals &row = all_rows.at(i);
    for (int64_t c = 0; c < MVIEW_COL_COUNT; ++c) {
      int display_len = static_cast<int>(get_plan_row_display_len(row, c));
      if (display_len > col_widths[c]) {
        total_width += static_cast<int64_t>(display_len - col_widths[c]);
        col_widths[c] = display_len;
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(append_separator_line(result, total_width, '=', allocator))) {
    LOG_WARN("fail to append top separator", KR(ret));
  } else if (OB_FAIL(result.append("|"))) {
    LOG_WARN("fail to append header start", KR(ret));
  }
  for (int64_t c = 0; OB_SUCC(ret) && c < MVIEW_COL_COUNT; ++c) {
    if (OB_FAIL(result.append_fmt("%-*s|", col_widths[c], g_mview_col_names[c]))) {
      LOG_WARN("fail to append header column", KR(ret), K(c));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(result.append("\n"))) {
      LOG_WARN("fail to append header newline", KR(ret));
    } else if (OB_FAIL(append_separator_line(result, total_width, '-', allocator))) {
      LOG_WARN("fail to append header separator", KR(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < all_rows.count(); ++i) {
    const MViewPlanRowVals &row = all_rows.at(i);
    if (OB_FAIL(result.append("|"))) {
      LOG_WARN("fail to append row start", KR(ret), K(i));
    }
    for (int64_t c = 0; OB_SUCC(ret) && c < MVIEW_COL_COUNT; ++c) {
      const char *val = NULL != row.vals_[c] ? row.vals_[c] : "";
      int pad = get_plan_row_pad_width(row, c, col_widths[c]);
      if (OB_FAIL(result.append_fmt("%-*s|", pad, val))) {
        LOG_WARN("fail to append row value", KR(ret), K(i), K(c));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(result.append("\n"))) {
        LOG_WARN("fail to append row newline", KR(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append_separator_line(result, total_width, '=', allocator))) {
      LOG_WARN("fail to append bottom separator", KR(ret));
    }
  }
  return ret;
}

// JSON schema (snake_case keys):
//   { "plan_hash": <uint>,
//     "operators": [
//       { "id", "depth", "operation", "object_alias",
//         "est_rows", "est_cost", "real_rows", "real_cost", "cpu_cost", "io_cost",
//         "open_time", "close_time", "dop", "max_rows", "sum_db_time", "starts",
//         "max_wa_mem",
//         "server": "ip:port,...",
//         "other_stats": [ { "id", "value" }, ... ]
//       }, ...
//     ]
//   }
static const char *const JK_PLAN_HASH = "plan_hash";
static const char *const JK_OPERATORS = "operators";
static const char *const JK_ID = "id";
static const char *const JK_DEPTH = "depth";
static const char *const JK_OPERATION = "operation";
static const char *const JK_OBJECT_ALIAS = "object_alias";
static const char *const JK_EST_ROWS = "est_rows";
static const char *const JK_EST_COST = "est_cost";
static const char *const JK_REAL_ROWS = "real_rows";
static const char *const JK_REAL_COST = "real_cost";
static const char *const JK_CPU_COST = "cpu_cost";
static const char *const JK_IO_COST = "io_cost";
static const char *const JK_OPEN_TIME = "open_time";
static const char *const JK_CLOSE_TIME = "close_time";
static const char *const JK_DOP = "dop";
static const char *const JK_MAX_ROWS = "max_rows";
static const char *const JK_SUM_DB_TIME = "sum_db_time";
static const char *const JK_STARTS = "starts";
static const char *const JK_MAX_WA_MEM = "max_wa_mem";
static const char *const JK_SERVER = "server";
static const char *const JK_OTHER_STATS = "other_stats";
static const char *const JK_VALUE = "value";
static const char *const JK_OUTLINE_DATA = "outline_data";
static const char *const JK_OPTIMIZATION_INFO = "optimization_info";
static const char *const JK_FILTER_PREDICATES = "filter_predicates";
static const char *const JK_STARTUP_PREDICATES = "startup_predicates";
static const char *const JK_SPECIAL_PREDICATES = "special_predicates";
static const char *const JK_PARTITION_START = "partition_start";
static const char *const JK_PARTITION_STOP = "partition_stop";
static const char *const JK_IS_LAST_CHILD = "is_last_child";

static int add_int_member(common::ObIAllocator &alloc,
                          ObJsonObject *obj,
                          const char *key,
                          int64_t val)
{
  int ret = OB_SUCCESS;
  ObJsonInt *node = OB_NEWx(ObJsonInt, &alloc, val);
  if (OB_ISNULL(node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc json int", KR(ret), K(key), K(val));
  } else if (OB_FAIL(obj->add(ObString::make_string(key), node))) {
    LOG_WARN("fail to add int member", KR(ret), K(key));
  }
  return ret;
}

static int add_uint_member(common::ObIAllocator &alloc,
                           ObJsonObject *obj,
                           const char *key,
                           uint64_t val)
{
  int ret = OB_SUCCESS;
  ObJsonUint *node = OB_NEWx(ObJsonUint, &alloc, val);
  if (OB_ISNULL(node)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc json uint", KR(ret), K(key), K(val));
  } else if (OB_FAIL(obj->add(ObString::make_string(key), node))) {
    LOG_WARN("fail to add uint member", KR(ret), K(key));
  }
  return ret;
}

// Scan str[0..len) for bytes that are NOT part of a valid UTF-8 multi-byte
// sequence.  Valid UTF-8 is kept as-is (it is legal in JSON and
// ObJsonString::add_double_quote passes it through correctly).  Invalid
// bytes are replaced with \u00XX escapes.
static int sanitize_json_utf8(common::ObIAllocator &alloc,
                               const char *str, int64_t len,
                               const char *&out_ptr, int64_t &out_len)
{
  int ret = OB_SUCCESS;
  out_ptr = str;
  out_len = len;
  if (len > 0 && OB_NOT_NULL(str)) {
    int64_t bad_count = 0;
    int64_t i = 0;
    while (i < len) {
      const unsigned char ch = static_cast<unsigned char>(str[i]);
      if (ch < 0x80) {
        ++i;
      } else {
        int seq_len = 0;
        if ((ch & 0xE0) == 0xC0) {
          seq_len = 2;
        } else if ((ch & 0xF0) == 0xE0) {
          seq_len = 3;
        } else if ((ch & 0xF8) == 0xF0) {
          seq_len = 4;
        }
        if (seq_len > 0) {
          bool valid = true;
          for (int j = 1; valid && j < seq_len; ++j) {
            if (i + j >= len) {
              valid = false;
            } else {
              const unsigned char cb = static_cast<unsigned char>(str[i + j]);
              if (cb < 0x80 || cb > 0xBF) {
                valid = false;
              }
            }
          }
          if (valid) {
            i += seq_len;
          } else {
            ++bad_count;
            ++i;
          }
        } else {
          ++bad_count;
          ++i;
        }
      }
    }
    if (bad_count > 0) {
      // +1 for snprintf null terminator on the last invalid byte escape
      const int64_t new_len = len + bad_count * 5;
      char *buf = static_cast<char *>(alloc.alloc(new_len + 1));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc sanitize buf", KR(ret), K(len), K(new_len));
      } else {
        int64_t pos = 0;
        i = 0;
        while (OB_SUCC(ret) && i < len) {
          const unsigned char ch = static_cast<unsigned char>(str[i]);
          if (ch < 0x80) {
            buf[pos++] = str[i++];
          } else {
            int seq_len = 0;
            if ((ch & 0xE0) == 0xC0) {
              seq_len = 2;
            } else if ((ch & 0xF0) == 0xE0) {
              seq_len = 3;
            } else if ((ch & 0xF8) == 0xF0) {
              seq_len = 4;
            }
            bool valid = (seq_len > 0);
            if (valid) {
              for (int j = 1; valid && j < seq_len; ++j) {
                if (i + j >= len) {
                  valid = false;
                } else {
                  const unsigned char cb = static_cast<unsigned char>(str[i + j]);
                  if (cb < 0x80 || cb > 0xBF) {
                    valid = false;
                  }
                }
              }
            }
            if (valid) {
              for (int j = 0; j < seq_len; ++j) {
                buf[pos++] = str[i++];
              }
            } else {
              const int n = snprintf(buf + pos, 7, "\\u%04x", ch);
              if (n < 0 || n > 6) {
                ret = OB_BUF_NOT_ENOUGH;
              } else {
                pos += n;
                ++i;
              }
            }
          }
        }
        if (OB_SUCC(ret) && OB_UNLIKELY(pos != new_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sanitize length mismatch, counting and writing passes diverged",
                   K(pos), K(new_len), K(len), K(bad_count));
        }
        if (OB_SUCC(ret)) {
          out_ptr = buf;
          out_len = pos;
        }
      }
    }
  }
  return ret;
}

static int add_string_member(common::ObIAllocator &alloc,
                             ObJsonObject *obj,
                             const char *key,
                             const char *str,
                             int64_t len)
{
  int ret = OB_SUCCESS;
  const char *safe_str = NULL;
  int64_t safe_len = 0;
  if (OB_FAIL(sanitize_json_utf8(alloc, str, len, safe_str, safe_len))) {
    LOG_WARN("fail to sanitize json ascii", KR(ret), K(key));
  } else {
    ObString str_view(static_cast<int32_t>(safe_len), safe_str);
    ObJsonString *node = OB_NEWx(ObJsonString, &alloc, str_view);
    if (OB_ISNULL(node)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc json string", KR(ret), K(key));
    } else if (OB_FAIL(obj->add(ObString::make_string(key), node))) {
      LOG_WARN("fail to add string member", KR(ret), K(key));
    }
  }
  return ret;
}

} // namespace

int build_mview_plan_hash_json(common::ObIAllocator &allocator,
                               uint64_t plan_hash,
                               ObSqlString &result_json)
{
  int ret = OB_SUCCESS;
  ObJsonObject *root = OB_NEWx(ObJsonObject, &allocator, &allocator);
  ObJsonArray *ops = OB_NEWx(ObJsonArray, &allocator, &allocator);
  if (OB_ISNULL(root) || OB_ISNULL(ops)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc hash-only plan json containers", KR(ret), KP(root), KP(ops));
  } else if (OB_FAIL(add_uint_member(allocator, root, JK_PLAN_HASH, plan_hash))) {
    LOG_WARN("fail to add plan_hash", KR(ret));
  } else if (OB_FAIL(root->add(ObString::make_string(JK_OPERATORS), ops))) {
    LOG_WARN("fail to add operators", KR(ret));
  } else {
    ObJsonBuffer j_buf(&allocator);
    if (OB_FAIL(root->print(j_buf, false /*is_quoted*/))) {
      LOG_WARN("fail to print hash-only plan json", KR(ret));
    } else if (OB_FAIL(result_json.append(j_buf.ptr(), j_buf.length()))) {
      LOG_WARN("fail to append hash-only plan json", KR(ret));
    }
  }
  return ret;
}

namespace
{

static int format_merged_plan_json(common::ObIAllocator &alloc,
                                   uint64_t plan_hash,
                                   const ObIArray<ObSqlPlanItem *> &plan_infos,
                                   const ObIArray<MViewPlanMonitorTime> &monitor_times,
                                   const ObIArray<MViewPlanOtherStat> &other_stats,
                                   const ObIArray<const char *> &server_strs,
                                   ObSqlString &out_json)
{
  int ret = OB_SUCCESS;
  ObJsonObject *root = OB_NEWx(ObJsonObject, &alloc, &alloc);
  ObJsonArray *ops = OB_NEWx(ObJsonArray, &alloc, &alloc);
  if (OB_ISNULL(root) || OB_ISNULL(ops)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc json containers", KR(ret), KP(root), KP(ops));
  } else if (OB_FAIL(add_uint_member(alloc, root, JK_PLAN_HASH, plan_hash))) {
    LOG_WARN("fail to add plan_hash", KR(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (plan_infos.count() != monitor_times.count()
             || plan_infos.count() != other_stats.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan_infos/monitor_times/other_stats count mismatch",
             KR(ret), K(plan_infos.count()), K(monitor_times.count()), K(other_stats.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < plan_infos.count(); ++i) {
    const ObSqlPlanItem *item = plan_infos.at(i);
    if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null plan item", KR(ret), K(i));
    } else {
      const MViewPlanMonitorTime &mtime = monitor_times.at(i);
      const MViewPlanOtherStat &ostat = other_stats.at(i);
      const char *server_str = NULL;
      ObJsonObject *op = OB_NEWx(ObJsonObject, &alloc, &alloc);
      ObJsonArray *os_arr = OB_NEWx(ObJsonArray, &alloc, &alloc);
      if (OB_ISNULL(op) || OB_ISNULL(os_arr)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc json op objects", KR(ret));
      } else if (OB_FAIL(add_int_member(alloc, op, JK_ID, item->id_))) {
      } else if (OB_FAIL(add_int_member(alloc, op, JK_DEPTH, item->depth_))) {
      } else if (OB_FAIL(add_int_member(alloc, op, JK_IS_LAST_CHILD,
                                        item->is_last_child_ ? 1 : 0))) {
      } else if (OB_FAIL(add_string_member(alloc, op, JK_OPERATION,
                                           item->operation_,
                                           item->operation_ != NULL ? item->operation_len_ : 0))) {
      } else if (OB_FAIL(add_string_member(alloc, op, JK_OBJECT_ALIAS,
                                           item->object_alias_,
                                           item->object_alias_ != NULL ? item->object_alias_len_ : 0))) {
      } else if (OB_FAIL(add_int_member(alloc, op, JK_EST_ROWS, item->cardinality_))) {
      } else if (OB_FAIL(add_int_member(alloc, op, JK_EST_COST, item->cost_))) {
      } else if (OB_FAIL(add_int_member(alloc, op, JK_REAL_ROWS, item->real_cardinality_))) {
      } else if (OB_FAIL(add_int_member(alloc, op, JK_REAL_COST, item->real_cost_))) {
      } else if (OB_FAIL(add_int_member(alloc, op, JK_CPU_COST, item->cpu_cost_))) {
      } else if (OB_FAIL(add_int_member(alloc, op, JK_IO_COST, item->io_cost_))) {
      } else if (OB_FAIL(add_int_member(alloc, op, JK_OPEN_TIME, mtime.open_time_))) {
      } else if (OB_FAIL(add_int_member(alloc, op, JK_CLOSE_TIME, mtime.close_time_))) {
      } else if (OB_FAIL(add_int_member(alloc, op, JK_DOP, mtime.dop_))) {
      } else if (OB_FAIL(add_int_member(alloc, op, JK_MAX_ROWS, mtime.max_rows_))) {
      } else if (OB_FAIL(add_int_member(alloc, op, JK_SUM_DB_TIME, mtime.sum_db_time_))) {
      } else if (OB_FAIL(add_int_member(alloc, op, JK_STARTS, mtime.starts_))) {
      } else if (OB_FAIL(add_int_member(alloc, op, JK_MAX_WA_MEM, mtime.max_wa_mem_))) {
      } else if (OB_FALSE_IT(server_str = (i < server_strs.count()) ? server_strs.at(i) : NULL)) {
      } else if (OB_NOT_NULL(server_str) && server_str[0] != '\0'
                 && OB_FAIL(add_string_member(alloc, op, JK_SERVER, server_str,
                                              strlen(server_str)))) {
        LOG_WARN("fail to add server to op", KR(ret));
      } else if (OB_NOT_NULL(item->filter_predicates_) && item->filter_predicates_len_ > 0
                 && OB_FAIL(add_string_member(alloc, op, JK_FILTER_PREDICATES,
                                              item->filter_predicates_,
                                              item->filter_predicates_len_))) {
        LOG_WARN("fail to add filter_predicates", KR(ret));
      } else if (OB_NOT_NULL(item->startup_predicates_) && item->startup_predicates_len_ > 0
                 && OB_FAIL(add_string_member(alloc, op, JK_STARTUP_PREDICATES,
                                              item->startup_predicates_,
                                              item->startup_predicates_len_))) {
        LOG_WARN("fail to add startup_predicates", KR(ret));
      } else if (OB_NOT_NULL(item->special_predicates_) && item->special_predicates_len_ > 0
                 && OB_FAIL(add_string_member(alloc, op, JK_SPECIAL_PREDICATES,
                                              item->special_predicates_,
                                              item->special_predicates_len_))) {
        LOG_WARN("fail to add special_predicates", KR(ret));
      } else if (OB_NOT_NULL(item->partition_start_) && item->partition_start_len_ > 0
                 && OB_FAIL(add_string_member(alloc, op, JK_PARTITION_START,
                                              item->partition_start_,
                                              item->partition_start_len_))) {
        LOG_WARN("fail to add partition_start", KR(ret));
      } else if (OB_NOT_NULL(item->partition_stop_) && item->partition_stop_len_ > 0
                 && OB_FAIL(add_string_member(alloc, op, JK_PARTITION_STOP,
                                              item->partition_stop_,
                                              item->partition_stop_len_))) {
        LOG_WARN("fail to add partition_stop", KR(ret));
      }
      for (int64_t s = 0; OB_SUCC(ret) && s < OTHERSTAT_SLOT_COUNT; ++s) {
        if (0 != ostat.ids_[s] || 0 != ostat.values_[s]) {
          ObJsonObject *pair = OB_NEWx(ObJsonObject, &alloc, &alloc);
          if (OB_ISNULL(pair)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc otherstat pair", KR(ret));
          } else if (OB_FAIL(add_int_member(alloc, pair, JK_ID, ostat.ids_[s]))) {
          } else if (OB_FAIL(add_int_member(alloc, pair, JK_VALUE, ostat.values_[s]))) {
          } else if (OB_FAIL(os_arr->append(pair))) {
            LOG_WARN("fail to append otherstat pair", KR(ret));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(op->add(ObString::make_string(JK_OTHER_STATS), os_arr))) {
        LOG_WARN("fail to add other_stats to op", KR(ret));
      } else if (OB_FAIL(ops->append(op))) {
        LOG_WARN("fail to append op to operators", KR(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(root->add(ObString::make_string(JK_OPERATORS), ops))) {
      LOG_WARN("fail to add operators to root", KR(ret));
    }
    // Root-level outline_data from first operator's other_xml.
    if (OB_SUCC(ret) && plan_infos.count() > 0 && OB_NOT_NULL(plan_infos.at(0))) {
      const ObSqlPlanItem *first = plan_infos.at(0);
      if (OB_NOT_NULL(first->other_xml_) && first->other_xml_len_ > 0) {
        if (OB_FAIL(add_string_member(alloc, root, JK_OUTLINE_DATA,
                                      first->other_xml_, first->other_xml_len_))) {
          LOG_WARN("fail to add outline_data to root", KR(ret));
        }
      }
    }
    // optimization_info: only TABLE SCAN operators carry optimizer_, so
    // collect from all plan items instead of just the first one.
    if (OB_SUCC(ret)) {
      ObSqlString opt_buf;
      for (int64_t i = 0; OB_SUCC(ret) && i < plan_infos.count(); ++i) {
        if (OB_NOT_NULL(plan_infos.at(i))
            && OB_NOT_NULL(plan_infos.at(i)->optimizer_)
            && plan_infos.at(i)->optimizer_len_ > 0) {
          if (!opt_buf.empty()
              && OB_FAIL(opt_buf.append("\n"))) {
            LOG_WARN("fail to append optimizer info separator", KR(ret), K(i));
          } else if (OB_FAIL(opt_buf.append(plan_infos.at(i)->optimizer_,
                                            plan_infos.at(i)->optimizer_len_))) {
            LOG_WARN("fail to append optimizer info", KR(ret), K(i));
          }
        }
      }
      if (OB_SUCC(ret) && !opt_buf.empty()) {
        // opt_buf is an ObSqlString on ObMalloc whose scope ends before
        // root->print(), so copy it onto the arena to keep it valid.
        const char *opt_str = NULL;
        if (OB_FAIL(alloc_cstring(alloc, opt_buf.ptr(), opt_buf.length(), opt_str))) {
          LOG_WARN("fail to copy optimization_info to arena", KR(ret));
        } else if (OB_FAIL(add_string_member(alloc, root, JK_OPTIMIZATION_INFO,
                                             opt_str, opt_buf.length()))) {
          LOG_WARN("fail to add optimization_info to root", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObJsonBuffer j_buf(&alloc);
      if (OB_FAIL(root->print(j_buf, false /*is_quoted*/))) {
        LOG_WARN("fail to print json", KR(ret));
      } else if (OB_FAIL(out_json.append(j_buf.ptr(), j_buf.length()))) {
        LOG_WARN("fail to append json to out_json", KR(ret));
      }
    }
  }
  return ret;
}

// Read an integer-typed JSON member with best-effort type coercion.
// Returns 0 when the key is absent (keeps parse tolerant to older payloads).
static int read_json_int_member(const ObJsonObject *obj, const char *key, int64_t &out)
{
  int ret = OB_SUCCESS;
  out = 0;
  ObJsonNode *node = obj->get_value(ObString::make_string(key));
  if (OB_ISNULL(node)) {
    // Missing member: leave out at 0.
  } else {
    ObJsonNodeType t = node->json_type();
    if (ObJsonNodeType::J_INT == t) {
      out = static_cast<ObJsonInt *>(node)->value();
    } else if (ObJsonNodeType::J_UINT == t) {
      out = static_cast<int64_t>(static_cast<ObJsonUint *>(node)->value());
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected json type for int member",
               KR(ret), K(key), "json_type", static_cast<int>(t));
    }
  }
  return ret;
}

static int read_json_uint_member(const ObJsonObject *obj, const char *key, uint64_t &out)
{
  int ret = OB_SUCCESS;
  out = 0;
  ObJsonNode *node = obj->get_value(ObString::make_string(key));
  if (OB_ISNULL(node)) {
  } else {
    ObJsonNodeType t = node->json_type();
    if (ObJsonNodeType::J_UINT == t) {
      out = static_cast<ObJsonUint *>(node)->value();
    } else if (ObJsonNodeType::J_INT == t) {
      out = static_cast<uint64_t>(static_cast<ObJsonInt *>(node)->value());
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected json type for uint member",
               KR(ret), K(key), "json_type", static_cast<int>(t));
    }
  }
  return ret;
}

static int read_json_string_member(common::ObIAllocator &alloc,
                                   const ObJsonObject *obj,
                                   const char *key,
                                   char *&out_ptr,
                                   int64_t &out_len)
{
  int ret = OB_SUCCESS;
  out_ptr = NULL;
  out_len = 0;
  ObJsonNode *node = obj->get_value(ObString::make_string(key));
  if (OB_ISNULL(node)) {
  } else if (ObJsonNodeType::J_STRING != node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected json type for string member",
             KR(ret), K(key), "json_type", static_cast<int>(node->json_type()));
  } else {
    const ObString &val = static_cast<ObJsonString *>(node)->value();
    if (val.length() > 0) {
      char *buf = static_cast<char *>(alloc.alloc(val.length() + 1));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc json string copy", KR(ret), K(key), K(val.length()));
      } else {
        MEMCPY(buf, val.ptr(), val.length());
        buf[val.length()] = '\0';
        out_ptr = buf;
        out_len = val.length();
      }
    }
  }
  return ret;
}

// Root-level metadata extracted from plan JSON for rendering.
struct MViewPlanRootInfo {
  char *outline_data_;
  int64_t outline_data_len_;
  char *optimization_info_;
  int64_t optimization_info_len_;
  MViewPlanRootInfo()
    : outline_data_(NULL), outline_data_len_(0),
      optimization_info_(NULL), optimization_info_len_(0)
  {}
};

// Inverse of format_merged_plan_json(): populate plan_infos/monitor_times/other_stats from JSON.
static int parse_merged_plan_json(common::ObIAllocator &alloc,
                                  const ObString &plan_json,
                                  uint64_t &plan_hash,
                                  ObIArray<ObSqlPlanItem *> &plan_infos,
                                  ObIArray<MViewPlanMonitorTime> &monitor_times,
                                  ObIArray<MViewPlanOtherStat> &other_stats,
                                  MViewPlanRootInfo *root_info,
                                  bool need_predicates)
{
  int ret = OB_SUCCESS;
  plan_hash = 0;
  ObJsonNode *root_node = NULL;
  if (plan_json.empty()) {
    // Empty plan is valid — leave outputs empty.
  } else if (OB_FAIL(ObJsonParser::get_tree(&alloc, plan_json, root_node))) {
    LOG_WARN("fail to parse plan json", KR(ret), K(plan_json));
  } else if (OB_ISNULL(root_node) || ObJsonNodeType::J_OBJECT != root_node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("plan json root is not object", KR(ret));
  } else {
    ObJsonObject *root_obj = static_cast<ObJsonObject *>(root_node);
    if (OB_FAIL(read_json_uint_member(root_obj, JK_PLAN_HASH, plan_hash))) {
      LOG_WARN("fail to read plan_hash", KR(ret));
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(root_info)) {
      if (OB_FAIL(read_json_string_member(alloc, root_obj, JK_OUTLINE_DATA,
                                          root_info->outline_data_,
                                          root_info->outline_data_len_))) {
        LOG_WARN("fail to read outline_data", KR(ret));
      } else if (OB_FAIL(read_json_string_member(alloc, root_obj, JK_OPTIMIZATION_INFO,
                                                  root_info->optimization_info_,
                                                  root_info->optimization_info_len_))) {
        LOG_WARN("fail to read optimization_info", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObJsonNode *ops_node = root_obj->get_value(ObString::make_string(JK_OPERATORS));
      if (OB_ISNULL(ops_node) || ObJsonNodeType::J_ARRAY != ops_node->json_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("plan json operators node is null or not array",
                 KR(ret), KP(ops_node));
      } else {
        ObJsonArray *ops_arr = static_cast<ObJsonArray *>(ops_node);
        const int64_t op_count = static_cast<int64_t>(ops_arr->element_count());
        for (int64_t i = 0; OB_SUCC(ret) && i < op_count; ++i) {
          ObJsonNode *op_node = ops_arr->get_value(i);
          if (OB_ISNULL(op_node) || ObJsonNodeType::J_OBJECT != op_node->json_type()) {
            LOG_WARN("plan json operator node is null or not object, skip", K(i));
          } else {
            ObJsonObject *op_obj = static_cast<ObJsonObject *>(op_node);
            void *buf = alloc.alloc(sizeof(ObSqlPlanItem));
            if (OB_ISNULL(buf)) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to alloc plan item", KR(ret));
            } else {
              ObSqlPlanItem *item = new (buf) ObSqlPlanItem();
              // arena-allocated; destructor intentionally skipped, arena recycles all memory
              MViewPlanMonitorTime mtime;
              MViewPlanOtherStat ostat;
              int64_t int_val = 0;
              int64_t server_len = 0;
              if (OB_FAIL(read_json_int_member(op_obj, JK_ID, int_val))) {
              } else if (OB_FALSE_IT(item->id_ = static_cast<int>(int_val))) {
              } else if (OB_FAIL(read_json_int_member(op_obj, JK_DEPTH, int_val))) {
              } else if (OB_FALSE_IT(item->depth_ = static_cast<int>(int_val))) {
              } else if (OB_FAIL(read_json_int_member(op_obj, JK_IS_LAST_CHILD, int_val))) {
              } else if (OB_FALSE_IT(item->is_last_child_ = (int_val != 0))) {
              } else if (OB_FAIL(read_json_string_member(alloc, op_obj, JK_OPERATION,
                                                        item->operation_,
                                                        item->operation_len_))) {
              } else if (OB_FAIL(read_json_string_member(alloc, op_obj, JK_OBJECT_ALIAS,
                                                        item->object_alias_,
                                                        item->object_alias_len_))) {
              } else if (OB_FAIL(read_json_int_member(op_obj, JK_EST_ROWS, item->cardinality_))) {
              } else if (OB_FAIL(read_json_int_member(op_obj, JK_EST_COST, item->cost_))) {
              } else if (OB_FAIL(read_json_int_member(op_obj, JK_REAL_ROWS, item->real_cardinality_))) {
              } else if (OB_FAIL(read_json_int_member(op_obj, JK_REAL_COST, item->real_cost_))) {
              } else if (OB_FAIL(read_json_int_member(op_obj, JK_CPU_COST, item->cpu_cost_))) {
              } else if (OB_FAIL(read_json_int_member(op_obj, JK_IO_COST, item->io_cost_))) {
              } else if (OB_FAIL(read_json_int_member(op_obj, JK_OPEN_TIME, mtime.open_time_))) {
              } else if (OB_FAIL(read_json_int_member(op_obj, JK_CLOSE_TIME, mtime.close_time_))) {
              } else if (OB_FAIL(read_json_int_member(op_obj, JK_DOP, mtime.dop_))) {
              } else if (OB_FAIL(read_json_int_member(op_obj, JK_MAX_ROWS, mtime.max_rows_))) {
              } else if (OB_FAIL(read_json_int_member(op_obj, JK_SUM_DB_TIME, mtime.sum_db_time_))) {
              } else if (OB_FAIL(read_json_int_member(op_obj, JK_STARTS, mtime.starts_))) {
              } else if (OB_FAIL(read_json_int_member(op_obj, JK_MAX_WA_MEM, mtime.max_wa_mem_))) {
              } else if (OB_FAIL(read_json_string_member(alloc, op_obj, JK_SERVER, mtime.server_, server_len))) {
              } else if (need_predicates
                         && OB_FAIL(read_json_string_member(alloc, op_obj, JK_FILTER_PREDICATES,
                                                            item->filter_predicates_,
                                                            item->filter_predicates_len_))) {
              } else if (need_predicates
                         && OB_FAIL(read_json_string_member(alloc, op_obj, JK_STARTUP_PREDICATES,
                                                            item->startup_predicates_,
                                                            item->startup_predicates_len_))) {
              } else if (need_predicates
                         && OB_FAIL(read_json_string_member(alloc, op_obj, JK_SPECIAL_PREDICATES,
                                                            item->special_predicates_,
                                                            item->special_predicates_len_))) {
              } else if (need_predicates
                         && OB_FAIL(read_json_string_member(alloc, op_obj, JK_PARTITION_START,
                                                            item->partition_start_,
                                                            item->partition_start_len_))) {
              } else if (need_predicates
                         && OB_FAIL(read_json_string_member(alloc, op_obj, JK_PARTITION_STOP,
                                                            item->partition_stop_,
                                                            item->partition_stop_len_))) {
              }
              if (OB_SUCC(ret)) {
                ObJsonNode *os_node = op_obj->get_value(ObString::make_string(JK_OTHER_STATS));
                if (OB_NOT_NULL(os_node) && ObJsonNodeType::J_ARRAY == os_node->json_type()) {
                  ObJsonArray *os_arr = static_cast<ObJsonArray *>(os_node);
                  const int64_t pair_count = static_cast<int64_t>(os_arr->element_count());
                  for (int64_t s = 0; OB_SUCC(ret) && s < pair_count && s < OTHERSTAT_SLOT_COUNT; ++s) {
                    ObJsonNode *pair_node = os_arr->get_value(s);
                    if (OB_ISNULL(pair_node) || ObJsonNodeType::J_OBJECT != pair_node->json_type()) {
                      LOG_WARN("plan json otherstat pair is null or not object, skip", K(s));
                    } else {
                      ObJsonObject *pair_obj = static_cast<ObJsonObject *>(pair_node);
                      if (OB_FAIL(read_json_int_member(pair_obj, JK_ID, ostat.ids_[s]))) {
                      } else if (OB_FAIL(read_json_int_member(pair_obj, JK_VALUE, ostat.values_[s]))) {
                      }
                    }
                  }
                }
              }
              if (OB_FAIL(ret)) {
              } else if (OB_FAIL(plan_infos.push_back(item))) {
                LOG_WARN("fail to push back plan item", KR(ret));
              } else if (OB_FAIL(monitor_times.push_back(mtime))) {
                LOG_WARN("fail to push back monitor time", KR(ret));
              } else if (OB_FAIL(other_stats.push_back(ostat))) {
                LOG_WARN("fail to push back other stat", KR(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

static bool is_dml_operator(const ObSqlPlanItem &item)
{
  bool matched = false;
  if (OB_NOT_NULL(item.operation_) && item.operation_len_ > 0) {
    ObString op(static_cast<int32_t>(item.operation_len_), item.operation_);
    if (op.prefix_match_ci("PX ") && op.length() > STRLEN("PX ")) {
      op = ObString(static_cast<int32_t>(op.length() - STRLEN("PX ")), op.ptr() + STRLEN("PX "));
    }
    matched = (0 == op.case_compare("INSERT"))
              || (0 == op.case_compare("DISTRIBUTED INSERT"))
              || (0 == op.case_compare("INDEX INSERT"))
              || (0 == op.case_compare("INSERT_UP"))
              || (0 == op.case_compare("DISTRIBUTED INSERT_UP"))
              || (0 == op.case_compare("UPDATE"))
              || (0 == op.case_compare("DISTRIBUTED UPDATE"))
              || (0 == op.case_compare("INDEX UPDATE"))
              || (0 == op.case_compare("DELETE"))
              || (0 == op.case_compare("DISTRIBUTED DELETE"))
              || (0 == op.case_compare("INDEX DELETE"))
              || (0 == op.case_compare("MERGE"))
              || (0 == op.case_compare("DISTRIBUTED MERGE"))
              || (0 == op.case_compare("REPLACE"))
              || (0 == op.case_compare("DISTRIBUTED REPLACE"))
              || (0 == op.case_compare("MULTI TABLE INSERT"));
  }
  return matched;
}

static int append_predicate(common::ObSqlString &result,
                            const ObSqlPlanItem &item,
                            const char *predicate,
                            int64_t predicate_len,
                            bool &has_header,
                            bool &has_predicate)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(predicate) || predicate_len <= 0) {
  } else {
    if (!has_header) {
      if (OB_FAIL(result.append("\nPredicate Information:\n"))) {
        LOG_WARN("fail to append predicate header", KR(ret));
      } else {
        has_header = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (has_predicate && OB_FAIL(result.append(",\n      "))) {
      LOG_WARN("fail to append predicate sep", KR(ret));
    } else if (!has_predicate && OB_FAIL(result.append_fmt("  [%d] ", item.id_))) {
      LOG_WARN("fail to append operator id", KR(ret));
    } else if (OB_FAIL(result.append_fmt("%.*s", static_cast<int>(predicate_len), predicate))) {
      LOG_WARN("fail to append predicate content", KR(ret));
    } else {
      has_predicate = true;
    }
  }
  return ret;
}

static int format_predicate_information(const ObIArray<ObSqlPlanItem *> &plan_infos,
                                        ObSqlString &result)
{
  int ret = OB_SUCCESS;
  bool has_header = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < plan_infos.count(); ++i) {
    const ObSqlPlanItem *item = plan_infos.at(i);
    if (OB_NOT_NULL(item)) {
      bool has_predicate = false;
      if (OB_FAIL(append_predicate(result, *item, item->filter_predicates_,
                                   item->filter_predicates_len_, has_header, has_predicate))) {
        LOG_WARN("fail to append filter predicates", KR(ret));
      } else if (OB_FAIL(append_predicate(result, *item, item->startup_predicates_,
                                          item->startup_predicates_len_, has_header, has_predicate))) {
        LOG_WARN("fail to append startup predicates", KR(ret));
      } else if (OB_FAIL(append_predicate(result, *item, item->special_predicates_,
                                          item->special_predicates_len_, has_header, has_predicate))) {
        LOG_WARN("fail to append special predicates", KR(ret));
      } else if (OB_FAIL(append_predicate(result, *item, item->partition_start_,
                                          item->partition_start_len_, has_header, has_predicate))) {
        LOG_WARN("fail to append partition_start", KR(ret));
      } else if (OB_FAIL(append_predicate(result, *item, item->partition_stop_,
                                          item->partition_stop_len_, has_header, has_predicate))) {
        LOG_WARN("fail to append partition_stop", KR(ret));
      } else if (has_predicate && OB_FAIL(result.append("\n"))) {
        LOG_WARN("fail to append newline", KR(ret));
      }
    }
  }
  return ret;
}

// Aggregate per-operator data into step-level resource metrics. See header.
static int aggregate_resources_from_arrays(const ObIArray<ObSqlPlanItem *> &plan_infos,
                                           const ObIArray<MViewPlanMonitorTime> &monitor_times,
                                           const ObIArray<MViewPlanOtherStat> &other_stats,
                                           int64_t &cpu_time,
                                           int64_t &io_wait_time,
                                           int64_t &disk_reads,
                                           int64_t &memory_used)
{
  int ret = OB_SUCCESS;
  cpu_time = 0;
  io_wait_time = 0;
  disk_reads = 0;
  memory_used = 0;
  if (plan_infos.count() != monitor_times.count()
      || plan_infos.count() != other_stats.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plan_infos/monitor_times/other_stats count mismatch",
             KR(ret), K(plan_infos.count()), K(monitor_times.count()), K(other_stats.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < plan_infos.count(); ++i) {
    const ObSqlPlanItem *item = plan_infos.at(i);
    if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null plan item", KR(ret), K(i));
    } else {
      const MViewPlanMonitorTime &mtime = monitor_times.at(i);
      const MViewPlanOtherStat &ostat = other_stats.at(i);
      cpu_time += mtime.sum_db_time_;
      io_wait_time += item->io_cost_;
      if (mtime.max_wa_mem_ > memory_used) {
        memory_used = mtime.max_wa_mem_;
      }
      for (int64_t s = 0; s < OTHERSTAT_SLOT_COUNT; ++s) {
        if (sql::ObSqlMonitorStatIds::IO_READ_BYTES == ostat.ids_[s]) {
          disk_reads += ostat.values_[s];
        }
      }
    }
  }
  return ret;
}

}  // anonymous namespace

int get_mview_stmt_execution_plan(ObExecContext &ctx,
                                  uint64_t tenant_id,
                                  const ObString &sql_id,
                                  const ObString &trace_id,
                                  const ObString &svr_ip,
                                  int64_t svr_port,
                                  uint64_t plan_id,
                                  uint64_t plan_hash,
                                  ObSqlString &result_json)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null pointer", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    ObArenaAllocator allocator("MvExecPlan", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);

    // Query monitor + plan via LEFT JOIN so all plan operators are included.
    ObSEArray<ObSqlPlanItem *, 16> plan_infos;
    ObSEArray<MViewPlanMonitorTime, 2> monitor_times;
    ObSEArray<MViewPlanOtherStat, 1> other_stats;
    ObSEArray<const char *, 16> server_strs;
    ObSqlString join_sql;
    // Use LEFT JOIN so all plan operators are included even when the
    // current statement didn't produce monitor data for some of them.
    // IFNULL defaults missing monitor columns to 0.
    if (OB_FAIL(join_sql.assign_fmt("SELECT "
                                    "A.OPERATOR, A.OBJECT_ALIAS, A.ID, A.DEPTH, A.CARDINALITY, A.COST, "
                                    "IFNULL(D.REAL_CARD, 0), IFNULL(D.REAL_COST, 0), "
                                    "IFNULL(D.CPU_COST, 0), IFNULL(D.IO_COST, 0), "
                                    "IFNULL(D.OPEN_TIME, 0), IFNULL(D.CLOSE_TIME, 0), "
                                    "IFNULL(D.DOP, 0), IFNULL(D.MAX_ROWS, 0), IFNULL(D.SUM_DB_TIME, 0), "
                                    "IFNULL(D.STARTS, 0), IFNULL(D.MAX_WA_MEM, 0), "
                                    "IFNULL(D.OS1_ID, 0), IFNULL(D.OS1_VAL, 0), "
                                    "IFNULL(D.OS2_ID, 0), IFNULL(D.OS2_VAL, 0), "
                                    "IFNULL(D.OS3_ID, 0), IFNULL(D.OS3_VAL, 0), "
                                    "IFNULL(D.OS4_ID, 0), IFNULL(D.OS4_VAL, 0), "
                                    "IFNULL(D.OS5_ID, 0), IFNULL(D.OS5_VAL, 0), "
                                    "IFNULL(D.OS6_ID, 0), IFNULL(D.OS6_VAL, 0), "
                                    "IFNULL(D.OS7_ID, 0), IFNULL(D.OS7_VAL, 0), "
                                    "IFNULL(D.OS8_ID, 0), IFNULL(D.OS8_VAL, 0), "
                                    "IFNULL(D.OS9_ID, 0), IFNULL(D.OS9_VAL, 0), "
                                    "IFNULL(D.OS10_ID, 0), IFNULL(D.OS10_VAL, 0), "
                                    "D.SERVER, "
                                    "A.FILTER_PREDICATES, A.STARTUP_PREDICATES, "
                                    "A.SPECIAL_PREDICATES, A.OTHER_XML, A.OPTIMIZER, "
                                    "A.PARTITION_START, A.PARTITION_STOP "
                                    "FROM OCEANBASE.__ALL_VIRTUAL_SQL_PLAN A "
                                    "LEFT JOIN "
                                    "(SELECT PLAN_LINE_ID, "
                                    " CAST(SUM(OUTPUT_ROWS) AS SIGNED) REAL_CARD, "
                                    " CAST(MAX(GREATEST((DB_TIME - USER_IO_WAIT_TIME), 0)) AS SIGNED) CPU_COST, "
                                    " CAST(MAX(USER_IO_WAIT_TIME) AS SIGNED) IO_COST, "
                                    " CAST((UNIX_TIMESTAMP(MAX(LAST_CHANGE_TIME)) - UNIX_TIMESTAMP(MIN(FIRST_CHANGE_TIME))) * 1000000 AS SIGNED) REAL_COST, "
                                    " CAST(UNIX_TIMESTAMP(MIN(FIRST_REFRESH_TIME))*1000000 AS SIGNED) OPEN_TIME, "
                                    " CAST(UNIX_TIMESTAMP(MAX(LAST_REFRESH_TIME))*1000000 AS SIGNED) CLOSE_TIME, "
                                    " CAST(COUNT(*) AS SIGNED) DOP, "
                                    " CAST(MAX(OUTPUT_ROWS) AS SIGNED) MAX_ROWS, "
                                    " CAST(SUM(GREATEST((DB_TIME - USER_IO_WAIT_TIME), 0)) AS SIGNED) SUM_DB_TIME, "
                                    " CAST(SUM(STARTS) AS SIGNED) STARTS, "
                                    " CAST(MAX(WORKAREA_MAX_MEM) AS SIGNED) MAX_WA_MEM, "
                                    " CAST(MAX(OTHERSTAT_1_ID) AS SIGNED) OS1_ID, CAST(SUM(OTHERSTAT_1_VALUE) AS "
                                    "SIGNED) OS1_VAL, "
                                    " CAST(MAX(OTHERSTAT_2_ID) AS SIGNED) OS2_ID, CAST(SUM(OTHERSTAT_2_VALUE) AS "
                                    "SIGNED) OS2_VAL, "
                                    " CAST(MAX(OTHERSTAT_3_ID) AS SIGNED) OS3_ID, CAST(SUM(OTHERSTAT_3_VALUE) AS "
                                    "SIGNED) OS3_VAL, "
                                    " CAST(MAX(OTHERSTAT_4_ID) AS SIGNED) OS4_ID, CAST(SUM(OTHERSTAT_4_VALUE) AS "
                                    "SIGNED) OS4_VAL, "
                                    " CAST(MAX(OTHERSTAT_5_ID) AS SIGNED) OS5_ID, CAST(SUM(OTHERSTAT_5_VALUE) AS "
                                    "SIGNED) OS5_VAL, "
                                    " CAST(MAX(OTHERSTAT_6_ID) AS SIGNED) OS6_ID, CAST(SUM(OTHERSTAT_6_VALUE) AS "
                                    "SIGNED) OS6_VAL, "
                                    " CAST(MAX(OTHERSTAT_7_ID) AS SIGNED) OS7_ID, CAST(SUM(OTHERSTAT_7_VALUE) AS "
                                    "SIGNED) OS7_VAL, "
                                    " CAST(MAX(OTHERSTAT_8_ID) AS SIGNED) OS8_ID, CAST(SUM(OTHERSTAT_8_VALUE) AS "
                                    "SIGNED) OS8_VAL, "
                                    " CAST(MAX(OTHERSTAT_9_ID) AS SIGNED) OS9_ID, CAST(SUM(OTHERSTAT_9_VALUE) AS "
                                    "SIGNED) OS9_VAL, "
                                    " CAST(MAX(OTHERSTAT_10_ID) AS SIGNED) OS10_ID, CAST(SUM(OTHERSTAT_10_VALUE) AS "
                                    "SIGNED) OS10_VAL, "
                                    " GROUP_CONCAT(DISTINCT CONCAT(SVR_IP, ':', SVR_PORT) SEPARATOR ',') AS SERVER "
                                    " FROM OCEANBASE.__ALL_VIRTUAL_SQL_PLAN_MONITOR "
                                    " WHERE TENANT_ID = %lu AND TRACE_ID = '%.*s' AND SQL_ID = '%.*s' "
                                    " GROUP BY PLAN_LINE_ID"
                                    ") D "
                                    "ON A.ID = D.PLAN_LINE_ID "
                                    "WHERE A.TENANT_ID = %lu AND A.SVR_IP = '%.*s' AND A.SVR_PORT = %ld "
                                    "AND A.PLAN_ID = %lu AND A.PLAN_HASH = %lu "
                                    "ORDER BY A.ID",
                                    tenant_id,
                                    trace_id.length(), trace_id.ptr(),
                                    sql_id.length(), sql_id.ptr(),
                                    tenant_id,
                                    svr_ip.length(), svr_ip.ptr(),
                                    svr_port,
                                    plan_id,
                                    plan_hash))) {
      LOG_WARN("fail to build join query", KR(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res)
      {
        sqlclient::ObMySQLResult *mysql_result = NULL;
        if (OB_FAIL(GCTX.sql_proxy_->read(res, tenant_id, join_sql.ptr()))) {
          LOG_WARN("fail to execute join query", KR(ret), K(join_sql));
        } else if (OB_ISNULL(mysql_result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("null mysql result", KR(ret));
        }
        while (OB_SUCC(ret) && OB_SUCC(mysql_result->next())) {
          void *buf = NULL;
          ObSqlPlanItem *plan_info = NULL;
          MViewPlanMonitorTime mtime;
          MViewPlanOtherStat ostat;
          if (OB_ISNULL(buf = allocator.alloc(sizeof(ObSqlPlanItem)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory", KR(ret));
          } else {
            plan_info = new(buf) ObSqlPlanItem();
            // arena-allocated; destructor intentionally skipped, arena recycles all memory
            char *server_str = NULL;
            if (OB_FAIL(mview_read_plan_info_from_result(allocator, *mysql_result, *plan_info, mtime, ostat, server_str))) {
              LOG_WARN("failed to read plan info", KR(ret));
            } else if (OB_FAIL(plan_infos.push_back(plan_info))) {
              LOG_WARN("failed to push back plan info", KR(ret));
            } else if (OB_FAIL(monitor_times.push_back(mtime))) {
              LOG_WARN("failed to push back monitor time", KR(ret));
            } else if (OB_FAIL(other_stats.push_back(ostat))) {
              LOG_WARN("failed to push back other stat", KR(ret));
            } else if (OB_FAIL(server_strs.push_back(server_str))) {
              LOG_WARN("failed to push back server str", KR(ret));
            }
          }
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      }
    }

    if (OB_SUCC(ret) && plan_infos.count() > 0) {
      if (OB_FAIL(compute_is_last_child(plan_infos))) {
        LOG_WARN("fail to compute plan tree child flags", KR(ret));
      } else if (OB_FAIL(format_merged_plan_json(allocator,
                                                 plan_hash,
                                                 plan_infos,
                                                 monitor_times,
                                                 other_stats,
                                                 server_strs,
                                                 result_json))) {
        LOG_WARN("fail to format plan json", KR(ret));
      }
    }
  }

  return ret;
}

int render_mview_plan_text(common::ObIAllocator &allocator,
                           const ObString &plan_json,
                           uint64_t &plan_hash,
                           ObSqlString &result_text)
{
  int ret = OB_SUCCESS;
  plan_hash = 0;
  ObSEArray<ObSqlPlanItem *, 16> plan_infos;
  ObSEArray<MViewPlanMonitorTime, 2> monitor_times;
  ObSEArray<MViewPlanOtherStat, 1> other_stats;
  MViewPlanRootInfo root_info;
  if (OB_FAIL(parse_merged_plan_json(allocator, plan_json, plan_hash,
                                     plan_infos, monitor_times, other_stats,
                                     &root_info,
                                     true /*need_predicates*/))) {
    LOG_WARN("fail to parse plan json", KR(ret));
  } else if (0 == plan_infos.count()) {
    // Hash-only: operators array is empty, plan_hash returned via out param.
  } else if (OB_FAIL(compute_is_last_child(plan_infos))) {
    LOG_WARN("fail to compute plan tree child flags", KR(ret));
  } else if (OB_FAIL(format_merged_plan_table(allocator, plan_infos, monitor_times, result_text))) {
    LOG_WARN("fail to format merged plan table", KR(ret));
  } else if (OB_FAIL(format_operator_detail_stats(plan_infos, other_stats, result_text))) {
    LOG_WARN("fail to format operator detail stats", KR(ret));
  } else if (OB_FAIL(format_predicate_information(plan_infos, result_text))) {
    LOG_WARN("fail to format predicate information", KR(ret));
  } else {
    if (OB_NOT_NULL(root_info.outline_data_) && root_info.outline_data_len_ > 0) {
      if (OB_FAIL(result_text.append_fmt("\nOutline Data:\n  %.*s\n",
                                         static_cast<int>(root_info.outline_data_len_),
                                         root_info.outline_data_))) {
        LOG_WARN("fail to append outline data", KR(ret));
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(root_info.optimization_info_)
        && root_info.optimization_info_len_ > 0) {
      if (OB_FAIL(result_text.append_fmt("\nOptimization Info:\n  %.*s\n",
                                         static_cast<int>(root_info.optimization_info_len_),
                                         root_info.optimization_info_))) {
        LOG_WARN("fail to append optimization info", KR(ret));
      }
    }
  }
  return ret;
}

int aggregate_mview_plan_resources(common::ObIAllocator &allocator,
                                   const ObString &plan_json,
                                   int64_t &cpu_time,
                                   int64_t &io_wait_time,
                                   int64_t &disk_reads,
                                   int64_t &memory_used)
{
  int ret = OB_SUCCESS;
  cpu_time = 0;
  io_wait_time = 0;
  disk_reads = 0;
  memory_used = 0;
  uint64_t plan_hash = 0;
  ObSEArray<ObSqlPlanItem *, 16> plan_infos;
  ObSEArray<MViewPlanMonitorTime, 2> monitor_times;
  ObSEArray<MViewPlanOtherStat, 1> other_stats;
  if (OB_FAIL(parse_merged_plan_json(allocator, plan_json, plan_hash,
                                     plan_infos, monitor_times, other_stats,
                                     NULL,
                                     false /*need_predicates*/))) {
    LOG_WARN("fail to parse plan json for aggregation", KR(ret));
  } else if (OB_FAIL(aggregate_resources_from_arrays(plan_infos, monitor_times, other_stats,
                                                    cpu_time, io_wait_time, disk_reads,
                                                    memory_used))) {
    LOG_WARN("fail to aggregate resources", KR(ret));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
