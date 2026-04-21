/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_BASE_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_BASE_H_

#include "lib/ob_errno.h"
#define USING_LOG_PREFIX SQL

#include "unittest/sql/engine/op_tests/ob_op_test_engine.h"
#include "unittest/sql/engine/op_tests/ob_op_test_result.h"
#include "unittest/sql/engine/op_tests/ob_op_test_types.h"
#include "unittest/sql/engine/op_tests/ob_op_test_file_data.h"
// Note: ob_op_test_datahub.h excluded due to macro conflicts with #define private public
// Only include when testing datahub-dependent operators (Window Function with single_part_parallel)
// #include "unittest/sql/engine/op_tests/ob_op_test_datahub.h"
#include "sql/engine/basic/ob_material_vec_op.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/engine/aggregate/ob_aggregate_processor.h"
#include "sql/engine/aggregate/ob_groupby_op.h"
#include "sql/engine/sort/ob_sort_basic_info.h"
#include "sql/ob_sql_define.h"
#include "share/datum/ob_datum_funcs.h"
#include "objit/common/ob_item_type.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/random/ob_random.h"
#include "lib/utility/ob_tracepoint.h"  // for EventTable, EventItem
#include <string>
#include <vector>
#include <map>
#include <set>
#include <functional>
#include <type_traits>
#include <memory>
#include <initializer_list>
#include <utility>

namespace oceanbase
{
namespace sql
{
struct TLVars
{
  TLVars(): disabled_ret_check_(false) {}
  static bool ret_check_disabled()
  {
    return get_instance().disabled_ret_check_;
  }

private:
  friend struct DisableRetCheckGuard;
  static void set_ret_check_disabled(bool disabled)
  {
    get_instance().disabled_ret_check_ = disabled;
  }
  static TLVars &get_instance()
  {
    static thread_local TLVars instance;
    return instance;
  }

private:
  bool disabled_ret_check_;
};

struct DisableRetCheckGuard
{
  DisableRetCheckGuard()
  {
    stored_v_ = TLVars::ret_check_disabled();
    TLVars::set_ret_check_disabled(true);
  }
  ~DisableRetCheckGuard()
  {
    TLVars::set_ret_check_disabled(stored_v_);
  }

private:
  bool stored_v_;
};

// Temporarily disables op-tests return-code checking for a compound statement.
// Usage: DISABLE_OPTESTS_RET_CHECK { ... }  — equivalent to a block-local DisableRetCheckGuard.
// Uses C++17 if-init (this tree builds with -std=gnu++17); RAII object scope is exactly the `{ ... }`.
#define DISABLE_OPTESTS_RET_CHECK \
  if (::oceanbase::sql::DisableRetCheckGuard ob_op_tests_disable_ret_check_guard_; true)

// Thread-local singleton: stores tenant config overrides for testing.
// Each config item's default value is defined in parameter_seed.ipp via DEF_* macros,
// and add_tenant_config() initializes them when creating ObTenantConfig.
// This struct only stores non-default values that tests need to override.
struct TestDefaultParameterConf
{
  std::map<std::string, std::string> overrides_;

  static TestDefaultParameterConf &instance();

  // Set a config override. Validates:
  //   1) name exists in ObTenantConfig's container_ (valid config name)
  //   2) value is legal for the config type (trial set_value then restore)
  // On validation failure, triggers EXPECT_TRUE(false) and does NOT store.
  // Must be called after engine.init() (requires tenant config created).
  int set(const std::string &name, const std::string &value);

  void remove(const std::string &name)
  { overrides_.erase(name); }

  void clear() { overrides_.clear(); }
  bool empty() const { return overrides_.empty(); }
};

// RAII guard: saves TLS state on construction, restores on destruction.
struct TestParameterGuard
{
  TestParameterGuard()
  { saved_ = TestDefaultParameterConf::instance().overrides_; }

  // Applies config overrides after saving current overrides (same as default + chained set()).
  explicit TestParameterGuard(std::initializer_list<std::pair<const char *, const char *>> confs)
      : TestParameterGuard()
  {
    for (const auto &p : confs) {
      (void)set(p.first, p.second);
    }
  }

  TestParameterGuard &set(const std::string &name, const std::string &value)
  {
    TestDefaultParameterConf::instance().set(name, value);
    return *this;
  }

  ~TestParameterGuard()
  { TestDefaultParameterConf::instance().overrides_ = saved_; }

private:
  std::map<std::string, std::string> saved_;
};

// One key/value entry for WITH_TENANT_CONFS (comma-separated list of OB_TENANT_CONF(...) or {"k","v"}).
#define OB_TENANT_CONF(K, V) ::std::pair<const char *, const char *>((K), (V))

// Temporarily applies tenant config overrides for a compound statement (RAII like TestParameterGuard).
// Usage (C++ pair literals; colon between strings is not valid C++ syntax):
//   WITH_TENANT_CONFS(
//     {"_hash_area_size", "2M"},
//     {"_rowsets_max_rows", "512"}
//   ) { ... }
// or:
//   WITH_TENANT_CONFS(
//     OB_TENANT_CONF("_hash_area_size", "2M"),
//     OB_TENANT_CONF("_rowsets_max_rows", "512")
//   ) { ... }
#define WITH_TENANT_CONFS(...) \
  if (::oceanbase::sql::TestParameterGuard ob_op_tests_tenant_conf_guard__{{ __VA_ARGS__ }}; true)

// ===== Session Variable RAII Support =====

// Thread-local singleton: stores session variable overrides for testing.
// Applied via OpTestEngine::apply_session_variable_overrides() using session_info_.update_sys_variable().
struct TestSessionVarConf
{
  std::map<std::string, std::string> overrides_;

  static TestSessionVarConf &instance()
  {
    static thread_local TestSessionVarConf inst;
    return inst;
  }

  void set(const std::string &name, const std::string &value)
  { overrides_[name] = value; }

  void remove(const std::string &name)
  { overrides_.erase(name); }

  void clear() { overrides_.clear(); }
  bool empty() const { return overrides_.empty(); }
};

// RAII guard: saves TLS session variable state on construction, restores on destruction.
struct TestSessionVarGuard
{
  TestSessionVarGuard()
  { saved_ = TestSessionVarConf::instance().overrides_; }

  explicit TestSessionVarGuard(std::initializer_list<std::pair<const char *, const char *>> vars)
      : TestSessionVarGuard()
  {
    for (const auto &p : vars) {
      set(p.first, p.second);
    }
  }

  TestSessionVarGuard &set(const std::string &name, const std::string &value)
  {
    TestSessionVarConf::instance().set(name, value);
    return *this;
  }

  ~TestSessionVarGuard()
  { TestSessionVarConf::instance().overrides_ = saved_; }

private:
  std::map<std::string, std::string> saved_;
};

// One key/value entry for WITH_SESSION_VARS.
#define OB_SESSION_VAR(K, V) ::std::pair<const char *, const char *>((K), (V))

// Temporarily applies session variable overrides for a compound statement (RAII).
// Usage (C++17 if-init, same pattern as WITH_TENANT_CONFS):
//   WITH_SESSION_VARS(
//     {"ob_query_timeout", "10000000"},
//     {"group_concat_max_len", "1024"}
//   ) {
//     builder.run();
//   }
// or:
//   WITH_SESSION_VARS(
//     OB_SESSION_VAR("ob_query_timeout", "10000000"),
//     OB_SESSION_VAR("group_concat_max_len", "1024")
//   ) {
//     builder.run();
//   }
#define WITH_SESSION_VARS(...) \
  if (::oceanbase::sql::TestSessionVarGuard ob_op_tests_session_var_guard__{{ __VA_ARGS__ }}; true)

// ===== Tracepoint RAII Support =====

// Describes a tracepoint to be set for testing.
// Maps directly to EventItem fields (ob_tracepoint.h).
struct TestTracepoint {
  const char *name = nullptr;  // Tracepoint name (e.g. "EN_ENABLE_SQL_OP_DUMP")
  int64_t error_code = 0;    // Error code to return when triggered
  int64_t occur = 0;         // Max number of times to trigger (>0 = countdown, 0 = use trigger_freq)
  int64_t trigger_freq = 0;  // Trigger frequency (1 = always, N = 1/N probability)
  int64_t cond = 0;          // Condition value (0 = unconditional)
};

// RAII guard: sets tracepoints on construction, resets them on destruction.
// Uses EventTable::set_event(name, item) to set, and resets with zero EventItem on scope exit.
class TestTracepointGuard {
public:
  explicit TestTracepointGuard(std::initializer_list<TestTracepoint> tps)
  {
    for (const auto &tp : tps) {
      apply_tp(tp);
    }
  }

  explicit TestTracepointGuard(const std::vector<TestTracepoint> &tps)
  {
    for (const auto &tp : tps) {
      apply_tp(tp);
    }
  }

  ~TestTracepointGuard()
  {
    common::EventItem zero;  // Default constructed = all zeros = disabled
    for (const auto &n : names_) {
      ::oceanbase::common::EventTable::instance().set_event(n, zero);
    }
  }

  // Non-copyable, non-movable
  TestTracepointGuard(const TestTracepointGuard &) = delete;
  TestTracepointGuard &operator=(const TestTracepointGuard &) = delete;

private:
  void apply_tp(const TestTracepoint &tp)
  {
    common::EventItem item;
    item.error_code_  = tp.error_code;
    item.occur_       = tp.occur;
    item.trigger_freq_= tp.trigger_freq;
    item.cond_        = tp.cond;
    int ret = ::oceanbase::common::EventTable::instance().set_event(tp.name, item);
    if (OB_SUCCESS != ret) {
      LOG_WARN("TestTracepointGuard: set_event failed", K(ret), "name", tp.name);
    } else {
      names_.push_back(tp.name);
    }
  }

  std::vector<const char *> names_;  // Store raw pointers (string literals from macro)
};

// Helper macro for creating a TestTracepoint entry.
// Usage: OB_TP("EN_ENABLE_SQL_OP_DUMP", 0, 1, 1)
//        OB_TP("EN_SOME_TP", -4013, 3, 1)  -- trigger 3 times with error -4013
#define OB_TP(N, E, O, F) ::oceanbase::sql::TestTracepoint{ (N), (E), (O), (F), 0 }

// Temporarily applies tracepoint overrides for a compound statement (RAII).
// Usage (C++17 if-init, same pattern as WITH_TENANT_CONFS):
//   WITH_TRACEPOINTS(
//     OB_TP("EN_ENABLE_SQL_OP_DUMP", 0, 1, 1),
//     OB_TP("EN_SQL_MEMORY_MANAGER_CTX_EXTEND", -4013, 1, 1)
//   ) {
//     builder.run();
//   }
// Tracepoints are automatically reset when the scope exits.
#define WITH_TRACEPOINTS(...) \
  if (::oceanbase::sql::TestTracepointGuard ob_op_tests_tp_guard__{ __VA_ARGS__ }; true)

// RAII checker for fatal error detection.
// Sets thread-local trace_id at construction (random-based).
// Checks ret at scope exit and reports trace_id if not OB_SUCCESS.
// NOTE: Does NOT use LOG_ERROR_RET because OB_LOGGER may not be initialized.
class FatalErrorChecker {
public:
  explicit FatalErrorChecker(int &ret)
      : ret_(ret),
        trace_id_set_(false),
        trace_id_buf_() {
    common::ObRandom random;
    common::ObAddr local_addr(static_cast<int32_t>(random.get_int32()),
                               static_cast<int32_t>(random.get(1, 65535)));
    common::ObCurTraceId::TraceId tid;
    tid.init(local_addr);
    tid.to_string(trace_id_buf_, sizeof(trace_id_buf_));
    common::ObCurTraceId::set(tid);
    trace_id_set_ = true;
  }
  ~FatalErrorChecker() {
    if (OB_SUCCESS != ret_ && !TLVars::ret_check_disabled()) {
      const char* trace_id_str = trace_id_set_ ? trace_id_buf_ : common::ObCurTraceId::get_trace_id_str();
      fprintf(stderr, "\n[FatalErrorChecker] ERROR: ret=%d, trace_id=%s\n", ret_, trace_id_str);
      EXPECT_EQ(OB_SUCCESS, ret_);
    }
    if (trace_id_set_) {
      common::ObCurTraceId::reset();
    }
  }
private:
  int &ret_;
  bool trace_id_set_;
  char trace_id_buf_[common::OB_MAX_TRACE_ID_BUFFER_SIZE];
};

namespace {

// Helper: check if expr is in child_output (by pointer)
inline bool expr_in_child_output(ObExpr *expr, const ExprFixedArray &child_output)
{
  for (int64_t i = 0; i < child_output.count(); ++i) {
    ObExpr *e = child_output.at(i);
    if (OB_NOT_NULL(e) && e == expr) {
      return true;
    }
  }
  return false;
}

// Recursively collect calc exprs: expressions used by operator but not in child output.
// Excludes T_REF_COLUMN (column refs) and expressions already in child_output.
int collect_calc_exprs(ObExpr *expr,
                      const ExprFixedArray &child_output,
                      std::set<ObExpr *> &visited,
                      ExprFixedArray &calc_exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    return ret;
  }
  if (visited.count(expr) > 0) {
    return ret;
  }
  visited.insert(expr);

  // Skip if in child output (produced by child)
  if (expr_in_child_output(expr, child_output)) {
    return ret;
  }
  // Skip column references - they come from child
  if (T_REF_COLUMN == expr->type_) {
    return ret;
  }

  // Recurse into children first (dependency order)
  for (uint32_t i = 0; OB_SUCC(ret) && i < expr->arg_cnt_; ++i) {
    if (OB_NOT_NULL(expr->args_) && OB_NOT_NULL(expr->args_[i])) {
      if (OB_FAIL(collect_calc_exprs(expr->args_[i], child_output, visited, calc_exprs))) {
        LOG_WARN("collect calc exprs from child failed", K(ret), K(i));
      }
    }
  }

  // Add this expr to calc_exprs (including constants so their evaluated_ flag gets cleared)
  if (OB_SUCC(ret) && OB_FAIL(calc_exprs.push_back(expr))) {
    LOG_WARN("push calc expr failed", K(ret));
  }
  return ret;
}

// Helper: Get sort collation and cmp func for a single expression
int fill_sort_info_for_expr(ObExpr *expr,
                             const ObOrderDirection order_direction,
                             ObSortCollations &sort_collations,
                             ObSortFuncs &sort_cmp_funcs,
                             common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  FatalErrorChecker error_checker(ret);
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else {
    const bool is_ascending = is_ascending_direction(order_direction);
    const common::ObCmpNullPos null_pos = ((is_null_first(order_direction) ^ is_ascending)
        ? NULL_LAST : NULL_FIRST);
    ObSortFieldCollation field_collation(0, expr->datum_meta_.cs_type_, is_ascending, null_pos);
    ObSortCmpFunc cmp_func;
    cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(
        expr->datum_meta_.type_,
        expr->datum_meta_.type_,
        field_collation.null_pos_,
        field_collation.cs_type_,
        expr->datum_meta_.scale_,
        lib::is_oracle_mode(),
        expr->obj_meta_.has_lob_header(),
        expr->datum_meta_.precision_,
        expr->datum_meta_.precision_);
    if (OB_ISNULL(cmp_func.cmp_func_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cmp_func is null, check datatype is valid", K(ret),
               K(expr->datum_meta_.type_), K(field_collation.cs_type_));
    } else if (OB_FAIL(sort_collations.push_back(field_collation))) {
      LOG_WARN("failed to push back field collation", K(ret));
    } else if (OB_FAIL(sort_cmp_funcs.push_back(cmp_func))) {
      LOG_WARN("failed to push back cmp function", K(ret));
    }
  }
  return ret;
}

// Use #define to access private members for creating a minimal fake ObOptimizerContext
#define private public
#include "sql/optimizer/ob_optimizer_context.h"
#undef private

// Helper: Fill ObAggrInfo using ObStaticEngineCG::fill_aggr_info
// This is a thin wrapper that sets up the CG context properly
class OpTestAggrCG : public ObStaticEngineCG
{
public:
  OpTestAggrCG(ObPhysicalPlan &phy_plan, common::ObIAllocator &allocator, const uint64_t cur_cluster_version)
    : ObStaticEngineCG(cur_cluster_version),
      cur_op_exprs_(),
      fake_allocator_("OpTestAggrCG"),
      fake_expr_factory_(fake_allocator_),
      fake_global_hint_(fake_allocator_)
  {
    phy_plan_ = &phy_plan;
    // Create a minimal fake optimizer context to bypass null check
    // We only need it to be non-null for the has_dbms_stats_hint check
    // which always returns false for our tests
    fake_opt_ctx_ = static_cast<ObOptimizerContext*>(fake_allocator_.alloc(sizeof(ObOptimizerContext)));
    if (OB_NOT_NULL(fake_opt_ctx_)) {
      new (fake_opt_ctx_) ObOptimizerContext(nullptr, nullptr, nullptr, nullptr, fake_allocator_, nullptr,
                                              common::ObAddr(), nullptr, fake_global_hint_, fake_expr_factory_, nullptr, false);
      opt_ctx_ = fake_opt_ctx_;
    }
  }

  // Expose generate_rt_expr for direct use
  using ObStaticEngineCG::generate_rt_expr;

  // Wrapper for fill_aggr_info with unittest-friendly defaults
  int fill_aggr_info_for_test(ObAggFunRawExpr &raw_expr, ObExpr &rt_expr, ObAggrInfo &aggr_info,
                               const bool enable_rich_format = false)
  {
    // Clear cur_op_exprs_ before each call to avoid accumulating expressions
    cur_op_exprs_.reset();
    int ret = fill_aggr_info(raw_expr, rt_expr, aggr_info,
                          nullptr,  // group_exprs
                          nullptr,  // rollup_exprs
                          enable_rich_format,
                          nullptr,  // hash_rollup_info
                          nullptr); // grouping_set_info
    if (OB_FAIL(ret)) {
      LOG_WARN("fill_aggr_info failed", K(ret), "expr_type", raw_expr.get_expr_type());
    }
    return ret;
  }

private:
  ObSEArray<ObRawExpr*, 64> cur_op_exprs_;
  common::ObArenaAllocator fake_allocator_;
  ObRawExprFactory fake_expr_factory_;
  ObGlobalHint fake_global_hint_;
  ObOptimizerContext *fake_opt_ctx_;
};

/**
 * @brief Fill aggr_infos_ for GroupBy operators (ScalarAggregate, HashGroupBy, MergeGroupBy).
 *
 * This is a common helper function used by all GroupBy-related operators.
 * It uses the context set by run() (resolved_stmt_, phy_plan_) to fill aggr_infos_.
 *
 * @tparam GroupBySpecType The spec type (must have aggr_infos_ member, e.g., ObGroupBySpec)
 * @param alloc Allocator for creating OpTestAggrCG
 * @param groupby_spec The spec to fill
 * @param resolved_stmt The resolved select statement (for get_aggr_items())
 * @param phy_plan The physical plan (for OpTestAggrCG)
 * @param use_rich_format Whether to use rich format
 * @return OB_SUCCESS on success, error code otherwise
 */
template <typename GroupBySpecType>
int fill_aggr_infos_for_groupby(common::ObIAllocator &alloc,
                                  GroupBySpecType *groupby_spec,
                                  ObSelectStmt *resolved_stmt,
                                  ObPhysicalPlan *phy_plan,
                                  bool use_rich_format)
{
  int ret = OB_SUCCESS;
  FatalErrorChecker error_checker(ret);
  if (OB_ISNULL(groupby_spec) || OB_ISNULL(resolved_stmt) || OB_ISNULL(phy_plan)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), KP(groupby_spec), KP(resolved_stmt), KP(phy_plan));
  } else {
    const common::ObIArray<ObAggFunRawExpr*> &aggr_items = resolved_stmt->get_aggr_items();
    if (aggr_items.count() > 0) {
      // Initialize aggr_infos_ array
      if (OB_FAIL(groupby_spec->aggr_infos_.prepare_allocate(aggr_items.count()))) {
        LOG_WARN("prepare_allocate aggr_infos failed", K(ret));
      } else {
        // Create OpTestAggrCG to use ObStaticEngineCG::fill_aggr_info
        OpTestAggrCG aggr_cg(*phy_plan, alloc, GET_MIN_CLUSTER_VERSION());
        // Fill each ObAggrInfo
        for (int64_t i = 0; OB_SUCC(ret) && i < aggr_items.count(); ++i) {
          ObAggFunRawExpr *aggr_raw = aggr_items.at(i);
          if (OB_ISNULL(aggr_raw)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("aggr_raw is null", K(ret), K(i));
          } else {
            ObExpr *rt_expr = ObStaticEngineExprCG::get_rt_expr(*aggr_raw);
            if (OB_ISNULL(rt_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get_rt_expr returned null for aggr", K(ret), K(i),
                       "expr_type", aggr_raw->get_expr_type());
            } else {
              ObAggrInfo &aggr_info = groupby_spec->aggr_infos_.at(i);
              if (OB_FAIL(aggr_cg.fill_aggr_info_for_test(*aggr_raw, *rt_expr, aggr_info, use_rich_format))) {
                LOG_WARN("fill_aggr_info_for_test failed", K(ret), K(i));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

/**
 * @brief Build SortKeySpec list from order_desc by matching each expression string
 * against SELECT items (via get_name()) or column items (by column name).
 * Reuses existing ObExpr* pointers — does NOT re-parse or re-codegen anything,
 * so sub-expression pointer sharing is fully preserved.
 *
 * Priority:
 *  1. SELECT items: matched case-insensitively via expr_->get_name()
 *  2. Column items: matched case-insensitively by column_name_
 */
inline std::vector<SortKeySpec> build_sort_key_specs(
    const std::string &order_desc,
    const common::ObIArray<SelectItem> &select_items,
    const common::ObIArray<ColumnItem> &column_items)
{
  std::vector<SortKeySpec> specs;
  const auto parsed = parse_order_desc_strings(order_desc);

  auto to_upper = [](std::string s) -> std::string {
    std::transform(s.begin(), s.end(), s.begin(), ::toupper);
    return s;
  };
  auto trim_ws = [](const std::string &s) -> std::string {
    size_t a = s.find_first_not_of(" \t");
    size_t b = s.find_last_not_of(" \t");
    return (a == std::string::npos) ? "" : s.substr(a, b - a + 1);
  };
  // Remove all whitespace for expression matching (p%2, p%  2, p    %2 all match p % 2)
  auto remove_all_spaces = [](const std::string &s) -> std::string {
    std::string result;
    result.reserve(s.size());
    for (char c : s) {
      if (c != ' ' && c != '\t') {
        result += c;
      }
    }
    return result;
  };
  // Remove all table prefixes like "t." from expression (handles "t.a % t.b" -> "a % b")
  auto strip_all_table_prefixes = [](const std::string &s) -> std::string {
    std::string result = s;
    size_t pos = 0;
    while ((pos = result.find('.', pos)) != std::string::npos && pos > 0) {
      // Check if the part before dot is a valid identifier (table name)
      size_t start = pos;
      while (start > 0 && (isalnum(result[start-1]) || result[start-1] == '_')) {
        start--;
      }
      bool is_table_name = (start < pos);
      if (is_table_name) {
        result.erase(start, pos - start + 1);
        pos = start;
      } else {
        pos++;
      }
    }
    return result;
  };

  char name_buf[512];
  for (const auto &item : parsed) {
    const std::string &expr_str = std::get<0>(item);
    const bool ascending        = std::get<1>(item);
    const bool nulls_first      = std::get<2>(item);
    const std::string key_upper = to_upper(trim_ws(expr_str));
    const std::string key_no_space = remove_all_spaces(key_upper);  // For whitespace-insensitive matching
    ObExpr *found_expr = nullptr;

    // 1. Search top-level SELECT items by get_name()
    for (int64_t i = 0; i < select_items.count() && found_expr == nullptr; ++i) {
      ObRawExpr *raw = select_items.at(i).expr_;
      if (OB_ISNULL(raw)) continue;
      int64_t pos = 0;
      if (OB_SUCCESS == raw->get_name(name_buf, static_cast<int64_t>(sizeof(name_buf)) - 1, pos)) {
        std::string select_name(name_buf, pos);
        std::string select_upper = to_upper(trim_ws(select_name));
        std::string select_stripped = strip_all_table_prefixes(select_upper);
        std::string select_no_space = remove_all_spaces(select_upper);
        // Match: exact, stripped (no table prefix), or whitespace-insensitive
        if (select_upper == key_upper || select_stripped == key_upper ||
            select_no_space == key_no_space || remove_all_spaces(select_stripped) == key_no_space) {
          found_expr = ObStaticEngineExprCG::get_rt_expr(*raw);
        }
      }
    }

    // 2. Search column items by column_name_ (covers simple column references)
    if (found_expr == nullptr) {
      for (int64_t i = 0; i < column_items.count() && found_expr == nullptr; ++i) {
        const ColumnItem &col = column_items.at(i);
        if (OB_ISNULL(col.expr_)) continue;
        ObString cn = col.column_name_;
        if (to_upper(std::string(cn.ptr(), cn.length())) == key_upper) {
          found_expr = ObStaticEngineExprCG::get_rt_expr(*col.expr_);
        }
      }
    }

    if (found_expr == nullptr) {
      // sort key not found, skip silently
      continue;
    }
    specs.emplace_back(found_expr, ascending, nulls_first);
  }
  return specs;
}

}  // anonymous namespace

/**
 * @brief OpSpecBuilder - CRTP base class for operator test specifications.
 *
 * Usage pattern:
 *   MaterialTestSpec builder;
 *   builder.table("t", "a int, b int")
 *          .select("a, b")
 *          .with_data({{1, 2}, {3, 4}})
 *          .run();
 *
 * Derived classes can implement:
 *   - create_spec(ObIAllocator &alloc, MockDataSourceSpec *child_spec) - create parent spec
 *   - create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op) - create parent op
 *
 * If create_spec() returns nullptr, MockDataSourceSpec is used as root spec.
 * If create_op() returns nullptr, MockDataSourceOp is used as root op.
 */
template <typename Derived>
class OpSpecBuilder
{
public:
  OpSpecBuilder() : batch_size_(256), rescan_times_(0) {}
  virtual ~OpSpecBuilder() = default;

  // ===== Virtual Interface for Derived Classes =====

  /**
   * @brief Create parent operator spec (e.g., ObLimitVecSpec, ObScalarAggregateVecSpec).
   * Override in derived classes to return the actual operator spec.
   * Default implementation returns nullptr (use MockDataSourceSpec as root).
   * @param alloc Allocator for spec creation
   * @param child_spec The child spec (MockDataSourceSpec)
   * @param output_exprs The real output expressions (SELECT expressions)
   * @param limit_expr Optional LIMIT expression from stmt
   * @param offset_expr Optional OFFSET expression from stmt
   * @param use_rich_format Whether to use rich format (from session setting)
   * @return Pointer to created spec, or nullptr to use child_spec
   *
   * NOTE: Derived classes can access context via protected members:
   *   - resolved_stmt_: The resolved ObSelectStmt (for extracting window func exprs, aggr exprs)
   *   - child_output_exprs_: The column expressions from child (column_exprs)
   *   - phy_plan_: The physical plan (for OpTestWFCG/OpTestAggrCG)
   * These are set by run() before calling create_spec.
   */
  /**
   * @brief Post-resolve hook called after SQL resolution but before expression CG.
   * Override in derived classes to create pseudo columns (e.g., grouping_id for Expand).
   * Use engine.add_pending_raw_expr() to register extra raw exprs for CG.
   */
  int post_resolve_hook(OpTestEngine &engine, ObDMLStmt &stmt)
  {
    return OB_SUCCESS;
  }

  /**
   * @brief Adjust output expressions after prepare() but before build_and_execute().
   * Override in derived classes to add extra exprs to output (e.g., grouping_id).
   */
  void adjust_output_exprs()
  {
    // Default: no adjustment
  }

  ObOpSpec *create_spec(common::ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                         const ExprFixedArray &output_exprs,
                         ObExpr *limit_expr, ObExpr *offset_expr, bool use_rich_format)
  {
    // Default: no parent spec, use MockDataSourceSpec as root
    return nullptr;
  }

  /**
   * @brief Create operator input (ObOpInput) for operators that need it.
   * Override in derived classes to return custom ObOpInput (e.g., ObMaterialVecOpInput).
   * Default implementation returns nullptr (no OpInput needed).
   * @param ctx Execution context
   * @param spec Operator spec
   * @return Pointer to created ObOpInput, or nullptr if not needed
   */
  ObOpInput *create_input(ObExecContext &ctx, ObOpSpec &spec)
  {
    // Default: no OpInput needed
    return nullptr;
  }

  /**
   * @brief Template method to create operator with OpInput support.
   * This method:
   * 1. Calls create_input() to get OpInput (nullptr if not needed)
   * 2. Allocates OpType with the input
   * 3. Sets children pointer
   *
   * Usage in derived classes:
   *   ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op) {
   *     return default_create_op<ObLimitVecOp>(ctx, spec, child_op);
   *   }
   *
   * @tparam OpType The operator type (e.g., ObLimitVecOp, ObWindowFunctionVecOp)
   * @param ctx Execution context
   * @param spec Operator spec
   * @param child_op Child operator
   * @return Pointer to created operator
   */
  template <typename OpType>
  ObOperator *default_create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op)
  {
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);
    Derived *derived = static_cast<Derived *>(this);

    // Step 1: Get OpInput (calls create_input which may be overridden)
    ObOpInput *input = derived->create_input(ctx, spec);

    // Step 2: Allocate operator
    void *mem = ctx.get_allocator().alloc(sizeof(OpType));
    if (OB_ISNULL(mem)) {
      LOG_WARN("alloc operator failed", K(ret));
      return nullptr;
    }
    OpType *op = new (mem) OpType(ctx, spec, input);

    // Step 3: Set children pointer
    void *children_mem = ctx.get_allocator().alloc(sizeof(ObOperator *));
    if (OB_ISNULL(children_mem)) {
      LOG_WARN("alloc children array failed", K(ret));
      return nullptr;
    }
    ObOperator **children = reinterpret_cast<ObOperator **>(children_mem);
    children[0] = child_op;
    if (OB_FAIL(op->set_children_pointer(children, 1))) {
      LOG_WARN("set children pointer failed", K(ret));
      return nullptr;
    }

    return op;
  }

  /**
   * @brief Create parent operator (e.g., ObLimitVecOp, ObScalarAggregateVecOp).
   * Override in derived classes to return the actual operator.
   * Default implementation returns nullptr (use MockDataSourceOp as root).
   * @param ctx Execution context
   * @param spec The spec created by create_spec()
   * @param child_op The child operator (MockDataSourceOp)
   * @return Pointer to created operator, or nullptr to use child_op
   *
   * NOTE: For operators that need OpInput, use default_create_op<OpType>() template.
   */
  ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op)
  {
    // Default: no parent op, use MockDataSourceOp as root
    return nullptr;
  }

  // ===== SQL Description Interface =====

  /**
   * @brief Set table name and column definitions.
   * @param table_name Name of the table
   * @param col_defs Column definitions (e.g., "a int, b varchar(32)")
   */
  Derived& table(const char *table_name, const char *col_defs)
  {
    table_name_ = table_name;
    col_defs_ = col_defs;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set SELECT expressions.
   * @param exprs Comma-separated expression list
   */
  Derived& select(const char *exprs)
  {
    select_exprs_ = exprs;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set GROUP BY columns.
   * @param exprs Comma-separated column list
   */
  Derived& group_by(const char *exprs)
  {
    group_by_exprs_ = exprs;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set ORDER BY columns.
   * @param exprs Comma-separated column list with optional ASC/DESC
   */
  Derived& order_by(const char *exprs)
  {
    order_by_exprs_ = exprs;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set WHERE clause condition.
   * @param condition The WHERE condition (e.g., "a > 0 AND b < 10")
   */
  Derived& where(const char *condition)
  {
    where_clause_ = condition;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set explicit output expressions by matching with select_items.
   * @param exprs List of expression strings to match from select_items
   *
   * This method selects output columns from already-resolved select_items
   * by matching expression strings, keeping RawExpr pointers consistent.
   *
   * Example:
   *   .select("p%2, p, o, rank() over (partition by p%2, p order by o)")
   *   .with_output_exprs("p%2", "p", "rank() over (partition by p%2, p order by o)")
   *
   * Note: Expression strings must match exactly with those in select() (same spacing).
   */
  Derived& with_output_exprs(std::initializer_list<const char *> exprs)
  {
    output_expr_strs_.clear();
    for (const char *expr : exprs) {
      output_expr_strs_.push_back(std::string(expr));
    }
    return static_cast<Derived&>(*this);
  }

  // ===== Data Injection =====

  /**
   * @brief Set test data for the mock table.
   * @param rows Vector of TestRow data
   */
  Derived& with_data(const std::vector<TestRow> &rows)
  {
    test_data_ = rows;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set test data with move semantics.
   */
  Derived& with_data(std::vector<TestRow> &&rows)
  {
    test_data_ = std::move(rows);
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set test data from a CSV file.
   * @param table Table name (must match table() call)
   * @param file_path Path to the CSV file
   * @return Reference to derived class for chaining
   *
   * Note: with_file_data() takes priority over with_data().
   * If both are called, with_file_data() data will be used.
   */
  Derived& with_file_data(const char *table, const std::string &file_path)
  {
    file_data_map_[table] = file_path;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set test data with sorting specification.
   * @param rows Test data (will be sorted in place)
   * @param order_desc Sort order description (e.g., "b DESC", "a ASC, b DESC")
   *                   Uses column names from table definition
   * @return Reference to derived class for chaining
   *
   * Use this when testing operators that expect sorted input (e.g., LIMIT with ORDER BY).
   * The data will be sorted according to order_desc before being output.
   *
   * Example:
   *   .table("t", "a int, b int")
   *   .with_sorted_data({{1, 10}, {2, 30}}, "b DESC")
   */
  Derived& with_sorted_data(const std::vector<TestRow> &rows, const char *order_desc)
  {
    test_data_ = rows;
    is_sorted_data_ = true;
    if (order_desc) {
      sorted_desc_ = order_desc;
    }
    return static_cast<Derived&>(*this);
  }

  Derived& with_sorted_data(std::vector<TestRow> &&rows, const char *order_desc)
  {
    test_data_ = std::move(rows);
    is_sorted_data_ = true;
    if (order_desc) {
      sorted_desc_ = order_desc;
    }
    return static_cast<Derived&>(*this);
  }

  // ===== Generator-based Data Injection =====

  /**
   * @brief Generate test data using per-column generators.
   * Each ColumnGenerator produces values for one column.
   *
   * Example:
   *   .with_data_generator(5000, gen::sequential(1), gen::random_string(8))
   */
  template <typename... Generators>
  Derived& with_data_generator(int64_t row_num, Generators&&... generators)
  {
    generator_row_num_ = row_num;
    column_generators_ = {ColumnGenerator(std::forward<Generators>(generators))...};
    use_generator_mode_ = true;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Generate test data with auto-random generators based on column schema.
   * Parses col_defs_ to determine each column's type and creates appropriate generators.
   *
   * Example:
   *   .table("t", "a int, b double, c varchar(16)")
   *   .with_data_generator(10000)
   */
  Derived& with_data_generator(int64_t row_num)
  {
    generator_row_num_ = row_num;
    column_generators_.clear();
    use_generator_mode_ = true;
    return static_cast<Derived&>(*this);
  }

  // ===== Configuration =====

  /**
   * @brief Set batch size for vectorization.
   */
  Derived& with_batch_size(int64_t batch_size)
  {
    batch_size_ = batch_size;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set rescan times for operator rescan testing.
   * @param times Number of times to rescan the operator (0 = no rescan)
   */
  Derived& with_rescan_times(int64_t times)
  {
    rescan_times_ = times;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set vector format for variable-length columns.
   * Only applies to variable-length types (varchar, text, etc.).
   * Fixed-length types always use VEC_FIXED.
   * @param fmt Vector format (VEC_DISCRETE or VEC_CONTINUOUS)
   */
  Derived& with_vector_format(VectorFormat fmt)
  {
    vector_format_ = fmt;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set per-column vector formats, overriding the single vector_format_.
   * Column count must match the number of columns in the table definition.
   * Validation rules (checked at build time):
   *   - Fixed-length columns: only VEC_FIXED or VEC_UNIFORM allowed
   *   - Variable-length columns: only VEC_DISCRETE, VEC_CONTINUOUS, or VEC_UNIFORM allowed
   * In non-rich-format mode, col_formats_ is ignored (entire batch uses VEC_UNIFORM).
   * @param fmts One VectorFormat per column
   */
  template <typename... Fmts>
  Derived& with_col_formats(Fmts... fmts)
  {
    col_formats_ = {static_cast<VectorFormat>(fmts)...};
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set batch size via a function that returns size for each batch index.
   * The function receives batch index (0-based) and returns desired batch size.
   * Return <= 0 to use the default max_row_cnt for that batch.
   * Note: This only affects MockDataSource output batch size; OpTestEngine::collect_batch_results
   * still calls op->get_next_batch(INT64_MAX, ...) — the parent operator receives whatever
   * MockDataSource produces per batch.
   * @param fn Function taking (batch_idx) -> batch_size
   */
  template <typename F,
            typename = std::enable_if_t<std::is_invocable_r_v<int64_t, F, int64_t>>>
  Derived& with_batch_size(F &&fn)
  {
    batch_size_fn_ = std::forward<F>(fn);
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set session variable overrides for the test run.
   * Variables are applied via session_info_.update_sys_variable() in prepare().
   * @param vars Initializer list of (name, value) pairs
   *
   * Example:
   *   .with_session_vars({{"ob_query_timeout", "10000000"}, {"group_concat_max_len", "1024"}})
   */
  Derived& with_session_vars(std::initializer_list<std::pair<const char *, const char *>> vars)
  {
    for (const auto &p : vars) {
      session_vars_.emplace(p.first, p.second);
    }
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set tracepoints to be activated during run() via RAII guard.
   * Tracepoints are automatically reset when the run() scope exits.
   * @param tps Initializer list of TestTracepoint entries
   */
  Derived& with_tracepoints(std::initializer_list<TestTracepoint> tps)
  {
    tracepoints_.assign(tps.begin(), tps.end());
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Inject skip bits into each batch produced by MockDataSource.
   * The function receives (batch_idx, batch_size, skip_bitmap) and should set bits
   * for rows to be skipped. brs_.size_ remains unchanged; skipped rows are invisible
   * to downstream operators. Therefore, the actual number of rows participating in
   * computation is <= batch_size, and test expectations should account for filtered rows.
   * @param fn Function taking (batch_idx, batch_size, ObBitVector*) -> void
   */
  template <typename F>
  Derived& with_input_skips(F &&fn)
  {
    input_skip_fn_ = std::forward<F>(fn);
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Enable performance recording for operator execution.
   * When enabled, execute() measures wall-clock time and MockDataSource accumulates
   * its own timing. The difference (operator_rt_ns) represents pure operator execution time.
   * Results are available via result.get_perf_stats() after run().
   * @param enable True to enable (default: true)
   */
  Derived& with_perf_record(bool enable = true)
  {
    perf_record_ = enable;
    return static_cast<Derived&>(*this);
  }

  // ===== SQL Operator Dump Configuration =====

  /**
   * @brief Enable/disable SQL operator dump for large data scenarios.
   * When enabled, Material operator can spill to disk when memory limit is exceeded.
   * @param enable True to enable dump (default: false)
   */
  Derived& with_sql_operator_dump(bool enable = true)
  {
    enable_sql_operator_dump_ = enable;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set hash area size for work area memory limit (in bytes).
   * This controls the memory threshold before Material operator spills to disk.
   * @param size Hash area size in bytes (default: 1MB, use small values to force dump)
   */
  Derived& with_hash_area_size(int64_t size)
  {
    hash_area_size_ = size;
    return static_cast<Derived&>(*this);
  }

  // ===== Dump Verify Mode Support =====

  /**
   * @brief Set dump verify mode for large data scenarios.
   * - FULL_ROWS: Collect all rows in memory (default, for small data)
   * - CHECKSUM: Calculate checksum instead of storing rows (for large data)
   * - NONE: No result collection (for performance testing)
   * @param mode The verification mode to use
   */
  Derived& with_dump_verify_mode(OpTestEngine::DumpVerifyMode mode)
  {
    dump_verify_mode_ = mode;
    return static_cast<Derived&>(*this);
  }

  // ===== Rescan Memory Check Support =====

  /**
   * @brief Enable/disable rescan memory consistency check.
   * When enabled, memory usage is tracked during rescan and compared with first scan.
   * This helps detect memory leaks in operators that support rescan.
   * @param enable True to enable rescan memory check (default: true in engine)
   */
  Derived& with_rescan_memory_check(bool enable = true)
  {
    rescan_memory_check_ = enable;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set tolerance for rescan memory consistency check (in bytes).
   * Memory usage after rescan should be within first_scan + tolerance.
   * Default (0): auto-calculated as max(first_scan * 10%, 64KB)
   * @param tolerance_bytes Tolerance in bytes
   */
  Derived& with_rescan_memory_tolerance(int64_t tolerance_bytes)
  {
    rescan_memory_tolerance_bytes_ = tolerance_bytes;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set rich format (2.0 vec) mode for the operator under test.
   * Default is true (use 2.0 vec operator). Set to false to test 1.0 operator.
   * @param enable True for 2.0 vec, false for 1.0
   */
  Derived& with_rich_format(bool enable)
  {
    rich_format_ = enable;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Enable dual format check mode.
   * When enabled, run() executes both 2.0 (vec) and 1.0 versions and compares results.
   * Rows are compared positionally — use this for operators with deterministic output order.
   * @param enable True to enable (default: true)
   */
  Derived& enable_dual_format_check(bool enable = true)
  {
    dual_format_check_ = enable;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Enable dual format check mode with unordered (set-based) comparison.
   * Like enable_dual_format_check(), but sorts both result sets before comparing.
   * Use for operators whose output order is non-deterministic (Hash GroupBy with dump,
   * Hash Join, etc.) where 1.0 and 2.0 may return the same rows in different order.
   */
  Derived& enable_dual_format_unordered_check()
  {
    dual_format_check_ = true;
    dual_format_unordered_ = true;
    return static_cast<Derived&>(*this);
  }

  // ===== Limit/Offset Support =====

  /**
   * @brief Set LIMIT value.
   * @param n Number of rows to limit
   */
  Derived& limit(int64_t n)
  {
    limit_ = n;
    has_limit_ = true;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Set OFFSET value.
   * @param n Number of rows to skip
   */
  Derived& offset(int64_t n)
  {
    offset_ = n;
    has_offset_ = true;
    return static_cast<Derived&>(*this);
  }

  // ===== SQL Mode Support =====

  /**
   * @brief Set SQL compatibility mode (MySQL or Oracle).
   * @param mode MYSQL or ORACLE mode
   */
  Derived& sql_mode(SqlMode mode)
  {
    sql_mode_ = mode;
    return static_cast<Derived&>(*this);
  }

  // ===== Custom Eval Function Override =====

  /**
   * @brief Replace eval_func_ on SELECT output expressions after CG.
   * @param func_ptr The custom eval function (row-by-row evaluation)
   * @return Reference to derived class for chaining
   *
   * Use this to inject custom evaluation logic for expression testing.
   * Applied to all output expressions after code generation.
   */
  Derived& with_expr_eval_func(ObExpr::EvalFunc func_ptr)
  {
    custom_eval_func_ = func_ptr;
    return static_cast<Derived&>(*this);
  }

  /**
   * @brief Replace eval_vector_func_ on SELECT output expressions after CG.
   * @param func_ptr The custom eval vector function (vectorized evaluation)
   * @return Reference to derived class for chaining
   *
   * Use this to inject custom vectorized evaluation logic for expression testing.
   * Applied to all output expressions after code generation.
   */
  Derived& with_expr_eval_vector_func(ObExpr::EvalVectorFunc func_ptr)
  {
    custom_eval_vector_func_ = func_ptr;
    return static_cast<Derived&>(*this);
  }

  // ===== Execution =====

  /**
   * @brief Prepare (steps 0-6): engine config, SQL registration, resolve, CG, collect exprs.
   * Always runs with FORCE_ON (set by engine.init()). Must be called before build_and_execute().
   * @param engine The OpTestEngine instance
   * @return OB_SUCCESS on success, error code otherwise
   */
  int prepare(OpTestEngine &engine)
  {
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);

    // Step 0: Set SQL mode (must be set before init)
    engine.set_sql_mode(sql_mode_);

    // Step 0.1: Set batch size on engine for vectorized execution
    engine.set_batch_size(batch_size_);

    // Step 0.2: Enable row-by-row eval mode if custom eval_func is set
    // This is needed because eval_batch() uses eval_vector_func_, not eval_func_
    engine.set_use_row_by_row_eval(custom_eval_func_ != nullptr);

    // Step 0.3: Configure SQL operator dump for large data scenarios
    engine.set_enable_sql_operator_dump(enable_sql_operator_dump_);
    engine.set_dump_verify_mode(dump_verify_mode_);
    engine.set_rescan_memory_check(rescan_memory_check_);
    engine.set_rescan_memory_tolerance(rescan_memory_tolerance_bytes_);

    // Step 0.3.1: Configure performance recording
    engine.set_perf_record(perf_record_);

    // Step 0.4: Apply TLS tenant config overrides (including _hash_area_size pushed by run())
    engine.apply_tenant_config_overrides();

    // Step 0.5: Apply TLS session variable overrides (including those pushed by run()/with_session_vars())
    engine.apply_session_variable_overrides();

    // Step 1: Register mock table schema
    ret = engine.register_table(table_name_.c_str(), col_defs_.c_str());
    if (OB_FAIL(ret)) {
      LOG_WARN("register table failed", K(ret));
      return ret;
    }

    // Step 2: Build SQL string
    std::string sql = build_sql();

    // Step 3: Resolve SQL
    ObDMLStmt *stmt = nullptr;
    ret = engine.resolve_sql(sql, stmt);
    if (OB_FAIL(ret) || OB_ISNULL(stmt)) {
      LOG_WARN("resolve sql failed", K(ret));
      return OB_SUCC(ret) ? OB_ERR_UNEXPECTED : ret;
    }

    // Step 3.5: Post-resolve hook (e.g., Expand creates grouping_id pseudo column before CG)
    {
      Derived *derived = static_cast<Derived *>(this);
      ret = derived->post_resolve_hook(engine, *stmt);
      if (OB_FAIL(ret)) {
        LOG_WARN("post_resolve_hook failed", K(ret));
        return ret;
      }
    }

    // Step 4: Generate expressions via CG (always runs in FORCE_ON mode set by init())
    ret = engine.generate_exprs(*stmt);
    if (OB_FAIL(ret)) {
      LOG_WARN("generate exprs failed", K(ret));
      return ret;
    }

    ObSelectStmt *select_stmt = stmt->is_select_stmt() ?
                                 static_cast<ObSelectStmt *>(stmt) : nullptr;
    if (OB_ISNULL(select_stmt)) {
      LOG_WARN("stmt is not select stmt");
      return OB_ERR_UNEXPECTED;
    }

    // Set context (used by create_spec in derived classes and build_and_execute)
    resolved_stmt_ = select_stmt;
    phy_plan_ = &engine.get_phy_plan();

    // Step 5.1: Collect column exprs -> prepared_col_exprs_vec_
    const common::ObIArray<ColumnItem> &column_items = stmt->get_column_items();
    prepared_col_exprs_vec_.clear();
    for (int64_t i = 0; i < column_items.count(); ++i) {
      const ColumnItem &col_item = column_items.at(i);
      if (OB_NOT_NULL(col_item.expr_)) {
        ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*col_item.expr_);
        if (OB_NOT_NULL(expr)) {
          prepared_col_exprs_vec_.push_back(expr);
        } else {
          LOG_WARN("get_rt_expr returned null", K(i));
        }
      }
    }

    // Step 5.2: Collect output exprs -> prepared_out_exprs_vec_
    const common::ObIArray<SelectItem> &select_items = select_stmt->get_select_items();
    prepared_out_exprs_vec_.clear();
    if (!output_expr_strs_.empty()) {
      // Select matching expressions from select_items by string comparison
      char name_buf[256];
      for (const std::string &target_expr : output_expr_strs_) {
        bool found = false;
        for (int64_t i = 0; i < select_items.count() && !found; ++i) {
          if (OB_NOT_NULL(select_items.at(i).expr_)) {
            int64_t pos = 0;
            if (OB_SUCCESS == select_items.at(i).expr_->get_name(name_buf, sizeof(name_buf), pos)) {
              std::string expr_str(name_buf, pos);
              std::string target_trimmed = target_expr;
              target_trimmed.erase(0, target_trimmed.find_first_not_of(" \t"));
              target_trimmed.erase(target_trimmed.find_last_not_of(" \t") + 1);
              expr_str.erase(0, expr_str.find_first_not_of(" \t"));
              expr_str.erase(expr_str.find_last_not_of(" \t") + 1);

              // Try exact match first
              if (target_trimmed == expr_str) {
                ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*select_items.at(i).expr_);
                if (OB_NOT_NULL(expr)) {
                  prepared_out_exprs_vec_.push_back(expr);
                  found = true;
                  continue;
                }
              }

              // Try matching column name without table prefix
              // e.g., target="p" matches expr="t.p"
              size_t dot_pos = expr_str.find('.');
              if (dot_pos != std::string::npos) {
                std::string col_name = expr_str.substr(dot_pos + 1);
                if (target_trimmed == col_name) {
                  ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*select_items.at(i).expr_);
                  if (OB_NOT_NULL(expr)) {
                    prepared_out_exprs_vec_.push_back(expr);
                    found = true;
                    continue;
                  }
                }
              }

              // Try matching aggregate/window function by function name and argument
              // e.g., target contains "sum(win1)" matches "T_FUN_SUM(t.win1)"
              if (!found) {
                std::string target_lower = target_trimmed;
                std::transform(target_lower.begin(), target_lower.end(), target_lower.begin(), ::tolower);
                // Extract function name and argument from target, e.g., "sum(win1)" -> func="sum", arg="win1"
                size_t func_start = target_lower.find('(');
                if (func_start != std::string::npos && func_start > 0) {
                  std::string func_name = target_lower.substr(0, func_start);
                  // Find the closing paren
                  size_t func_end = target_lower.find(')', func_start);
                  if (func_end != std::string::npos) {
                    std::string func_arg = target_lower.substr(func_start + 1, func_end - func_start - 1);
                    // Trim spaces from func_arg
                    func_arg.erase(0, func_arg.find_first_not_of(" \t"));
                    func_arg.erase(func_arg.find_last_not_of(" \t") + 1);

                    std::string expr_lower = expr_str;
                    std::transform(expr_lower.begin(), expr_lower.end(), expr_lower.begin(), ::tolower);

                    // Check if expr contains "T_FUN_<funcname>(...<arg>...)"
                    if (expr_lower.find("t_fun_" + func_name) != std::string::npos) {
                      // Also check if the argument matches
                      // expr_lower format: "t_fun_sum(t.win1)" -> need to check "win1" in func_arg
                      size_t expr_paren_start = expr_lower.find('(');
                      size_t expr_paren_end = expr_lower.find(')');
                      if (expr_paren_start != std::string::npos && expr_paren_end != std::string::npos) {
                        std::string expr_args = expr_lower.substr(expr_paren_start + 1, expr_paren_end - expr_paren_start - 1);
                        // expr_args could be "t.win1" or "win1"
                        // Exact match: func_arg must equal expr_args OR equal col_name after dot
                        bool arg_match = false;
                        if (func_arg == expr_args) {
                          arg_match = true;
                        } else {
                          size_t arg_dot = expr_args.find('.');
                          if (arg_dot != std::string::npos) {
                            std::string arg_col = expr_args.substr(arg_dot + 1);
                            if (func_arg == arg_col) {
                              arg_match = true;
                            }
                          }
                        }
                        if (arg_match) {
                          ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*select_items.at(i).expr_);
                          if (OB_NOT_NULL(expr)) {
                            prepared_out_exprs_vec_.push_back(expr);
                            found = true;
                            continue;
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        if (!found) {
          LOG_WARN("output expression not found in select_items", "target", target_expr.c_str());
        }
      }
    } else {
      // Use all select_items for output expressions
      for (int64_t i = 0; i < select_items.count(); ++i) {
        if (OB_NOT_NULL(select_items.at(i).expr_)) {
          ObExpr *expr = ObStaticEngineExprCG::get_rt_expr(*select_items.at(i).expr_);
          if (OB_NOT_NULL(expr)) {
            prepared_out_exprs_vec_.push_back(expr);
          }
        }
      }
    }

    // Step 5.3: Override eval functions if custom ones are specified
    if (custom_eval_func_ != nullptr || custom_eval_vector_func_ != nullptr) {
      for (ObExpr *expr : prepared_out_exprs_vec_) {
        if (OB_NOT_NULL(expr)) {
          if (custom_eval_func_ != nullptr) {
            expr->eval_func_ = custom_eval_func_;
            expr->eval_vector_func_ = expr_default_eval_vector_func;
          }
          if (custom_eval_vector_func_ != nullptr) {
            expr->eval_vector_func_ = custom_eval_vector_func_;
          }
        }
      }
    }

    // Collect WHERE filter exprs -> prepared_flt_exprs_vec_
    // Push nullptr entries to preserve index alignment with condition list
    prepared_condition_size_ = stmt->get_condition_size();
    prepared_flt_exprs_vec_.clear();
    if (prepared_condition_size_ > 0) {
      for (int64_t i = 0; i < prepared_condition_size_; ++i) {
        ObRawExpr *raw_expr = stmt->get_condition_expr(i);
        ObExpr *expr = nullptr;
        if (OB_NOT_NULL(raw_expr)) {
          expr = ObStaticEngineExprCG::get_rt_expr(*raw_expr);
        }
        prepared_flt_exprs_vec_.push_back(expr);
      }
    }

    // Step 6: Collect LIMIT/OFFSET exprs
    prepared_limit_expr_ = nullptr;
    prepared_offset_expr_ = nullptr;
    if (select_stmt->has_limit()) {
      ObRawExpr *limit_raw_expr = stmt->get_limit_expr();
      ObRawExpr *offset_raw_expr = stmt->get_offset_expr();
      if (OB_NOT_NULL(limit_raw_expr)) {
        prepared_limit_expr_ = ObStaticEngineExprCG::get_rt_expr(*limit_raw_expr);
      }
      if (OB_NOT_NULL(offset_raw_expr)) {
        prepared_offset_expr_ = ObStaticEngineExprCG::get_rt_expr(*offset_raw_expr);
      }
    }

    return ret;
  }

  /**
   * @brief Build and execute (steps 5.4-8): create MockDataSourceOp, build op tree, run.
   * Must be called after prepare(). engine.enable_rich_format() should be called before this.
   * @param engine The OpTestEngine instance
   * @param use_rich_format Whether to use 2.0 vec (true) or 1.0 (false) operators
   * @return OpTestResult containing output rows
   */
  OpTestResult build_and_execute(OpTestEngine &engine, bool use_rich_format)
  {
    OpTestResult result;
    int ret = OB_SUCCESS;
    FatalErrorChecker error_checker(ret);

    // Reconstruct ExprFixedArrays from prepared vectors
    ExprFixedArray column_exprs(engine.get_phy_plan().get_allocator());
    int expr_ret = column_exprs.init(static_cast<int64_t>(prepared_col_exprs_vec_.size()));
    if (OB_FAIL(expr_ret)) {
      LOG_WARN("init column_exprs failed", K(expr_ret));
      return result;
    }
    expr_ret = column_exprs.prepare_allocate(static_cast<int64_t>(prepared_col_exprs_vec_.size()));
    if (OB_FAIL(expr_ret)) {
      LOG_WARN("prepare_allocate column_exprs failed", K(expr_ret));
      return result;
    }
    for (size_t i = 0; i < prepared_col_exprs_vec_.size(); ++i) {
      column_exprs.at(i) = prepared_col_exprs_vec_[i];
    }

    ExprFixedArray output_exprs(engine.get_phy_plan().get_allocator());
    expr_ret = output_exprs.init(static_cast<int64_t>(prepared_out_exprs_vec_.size()));
    if (OB_FAIL(expr_ret)) {
      LOG_WARN("init output_exprs failed", K(expr_ret));
      return result;
    }
    expr_ret = output_exprs.prepare_allocate(static_cast<int64_t>(prepared_out_exprs_vec_.size()));
    if (OB_FAIL(expr_ret)) {
      LOG_WARN("prepare_allocate output_exprs failed", K(expr_ret));
      return result;
    }
    for (size_t i = 0; i < prepared_out_exprs_vec_.size(); ++i) {
      output_exprs.at(i) = prepared_out_exprs_vec_[i];
    }

    ExprFixedArray filter_exprs(engine.get_phy_plan().get_allocator());
    if (prepared_condition_size_ > 0) {
      expr_ret = filter_exprs.init(static_cast<int64_t>(prepared_flt_exprs_vec_.size()));
      if (OB_FAIL(expr_ret)) {
        LOG_WARN("init filter_exprs failed", K(expr_ret));
        return result;
      }
      expr_ret = filter_exprs.prepare_allocate(static_cast<int64_t>(prepared_flt_exprs_vec_.size()));
      if (OB_FAIL(expr_ret)) {
        LOG_WARN("prepare_allocate filter_exprs failed", K(expr_ret));
        return result;
      }
      for (size_t i = 0; i < prepared_flt_exprs_vec_.size(); ++i) {
        filter_exprs.at(i) = prepared_flt_exprs_vec_[i];
      }
    }

    // Step 5.4: Create MockDataSourceOp
    // engine.enable_rich_format() has been called before this, so MockDataSourceSpec
    // will pick up the correct use_rich_format_ from session_info_.use_rich_format().
    MockDataSourceOp *mock_op = engine.create_mock_data_source(column_exprs, nullptr);
    if (OB_ISNULL(mock_op)) {
      LOG_WARN("create mock data source failed");
      return result;
    }

    // Step 5.5: Set test data
    // Priority: use_generator_mode_ > file_data_map_ > test_data_
    if (use_generator_mode_) {
      // GENERATOR mode: bridge ColumnGenerators -> RowGenerator
      std::vector<ColumnGenerator> col_gens = column_generators_;
      if (col_gens.empty()) {
        col_gens = create_auto_generators();
      }
      RowGenerator row_gen = [col_gens](int64_t row_idx) -> TestRow {
        std::vector<TestValue> values;
        values.reserve(col_gens.size());
        for (const auto &g : col_gens) {
          values.push_back(g(row_idx));
        }
        return TestRow(std::move(values));
      };
      mock_op->set_row_generator(std::move(row_gen), generator_row_num_);
      mock_op->set_vector_format(vector_format_);
    } else {
      std::vector<TestRow> data_to_use;
      auto file_it = file_data_map_.find(table_name_);
      if (file_it != file_data_map_.end()) {
        // Load data from CSV file
        CsvDataReader reader;
        ret = reader.open(file_it->second);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to open csv file", K(file_it->second.c_str()), K(ret));
          return result;
        }
        ret = reader.read_all(data_to_use);
        reader.close();
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to read csv file", K(file_it->second.c_str()), K(ret));
          return result;
        }
      } else {
        // Use inline test data
        data_to_use = test_data_;
      }

      // Apply sorting if with_sorted_data was called.
      // Build SortKeySpec from resolved ObExpr* (preserves sub-expression pointer sharing).
      if (is_sorted_data_ && !sorted_desc_.empty() && !data_to_use.empty()) {
        const common::ObIArray<SelectItem> &select_items_ref =
            resolved_stmt_->get_select_items();
        const common::ObIArray<ColumnItem> &column_items_ref =
            resolved_stmt_->get_column_items();
        std::vector<SortKeySpec> sort_specs =
            build_sort_key_specs(sorted_desc_, select_items_ref, column_items_ref);
        if (!sort_specs.empty()) {
          mock_op->set_sort_key_specs(sort_specs);
        }
      }

      mock_op->set_test_data(data_to_use);
      mock_op->set_vector_format(vector_format_);
    }

    // Step 5.6: Pass extended configuration to MockDataSourceOp
    if (!col_formats_.empty()) {
      // Validate per-column formats against column types
      const int64_t col_cnt = column_exprs.count();
      if (static_cast<int64_t>(col_formats_.size()) != col_cnt) {
        LOG_WARN("col_formats size mismatch", "expected", col_cnt,
                 "actual", col_formats_.size());
        EXPECT_EQ(col_cnt, static_cast<int64_t>(col_formats_.size()))
            << "with_col_formats: column count mismatch. Expected " << col_cnt
            << " formats, got " << col_formats_.size();
        return result;
      }
      for (int64_t i = 0; i < col_cnt; ++i) {
        ObExpr *expr = column_exprs.at(i);
        VectorFormat fmt = col_formats_[i];
        if (OB_NOT_NULL(expr) && expr->is_fixed_length_data_) {
          if (fmt != VEC_FIXED && fmt != VEC_UNIFORM) {
            LOG_WARN("invalid format for fixed-length column", K(i), K(fmt));
            EXPECT_TRUE(fmt == VEC_FIXED || fmt == VEC_UNIFORM)
                << "Column " << i << " is fixed-length but got format " << fmt;
            return result;
          }
        } else if (OB_NOT_NULL(expr)) {
          if (fmt != VEC_DISCRETE && fmt != VEC_CONTINUOUS && fmt != VEC_UNIFORM) {
            LOG_WARN("invalid format for variable-length column", K(i), K(fmt));
            EXPECT_TRUE(fmt == VEC_DISCRETE || fmt == VEC_CONTINUOUS || fmt == VEC_UNIFORM)
                << "Column " << i << " is variable-length but got format " << fmt;
            return result;
          }
        }
      }
      mock_op->set_col_formats(col_formats_);
    }
    if (batch_size_fn_) {
      mock_op->set_batch_size_fn(batch_size_fn_);
    }
    if (input_skip_fn_) {
      mock_op->set_input_skip_fn(input_skip_fn_);
    }
    if (perf_record_) {
      mock_op->set_perf_accumulator(engine.get_mock_perf_acc());
    }

    // Step 6.5: Set context for create_spec() before calling
    child_output_exprs_ = &column_exprs;

    MockDataSourceSpec *mock_spec = static_cast<MockDataSourceSpec *>(
        const_cast<ObOpSpec *>(&mock_op->get_spec()));
    Derived *derived = static_cast<Derived *>(this);
    ObOpSpec *root_spec = derived->create_spec(engine.get_allocator(), mock_spec,
                                               output_exprs, prepared_limit_expr_,
                                               prepared_offset_expr_, use_rich_format);

    // Step 6.7: Set filter expressions on the tested operator's spec (root_spec) if exists,
    // otherwise on mock_spec. This ensures filter_rows() is called on the operator under test.
    if (prepared_condition_size_ > 0 && filter_exprs.count() > 0) {
      ObOpSpec *target_spec = OB_NOT_NULL(root_spec) ? root_spec : mock_spec;
      if (OB_FAIL(target_spec->filters_.init(filter_exprs.count()))) {
        LOG_WARN("init filters failed", K(ret));
      } else if (OB_FAIL(target_spec->filters_.prepare_allocate(filter_exprs.count()))) {
        LOG_WARN("prepare_allocate filters failed", K(ret));
      } else {
        for (int64_t i = 0; i < filter_exprs.count(); ++i) {
          target_spec->filters_.at(i) = filter_exprs.at(i);
        }
      }
    }

    // Step 7: Call derived class to create operator
    // If create_spec returned a spec, create_op should use its spec
    // Otherwise, use MockDataSourceOp directly
    ObOperator *op = nullptr;
    if (OB_NOT_NULL(root_spec)) {
      root_spec->set_child(0, mock_spec);
      mock_spec->set_parent(root_spec);
      op = derived->create_op(engine.get_exec_ctx(), *root_spec, mock_op);
    } else {
      op = derived->create_op(engine.get_exec_ctx(), mock_op);
    }

    if (OB_ISNULL(op)) {
      LOG_WARN("create operator failed");
      return result;
    }

    // Step 7.5: Compute and set calc_exprs_ on the tested operator's spec.
    // calc_exprs_ are expressions that depend on child output (e.g. a+b in SELECT a+b)
    // and must have their eval_info cleared after each get_next_batch().
    constexpr int64_t MAX_CALC_EXPRS = 128;
    ExprFixedArray calc_exprs(engine.get_phy_plan().get_allocator());
    if (OB_SUCC(ret) && OB_FAIL(calc_exprs.init(MAX_CALC_EXPRS))) {
      LOG_WARN("init calc_exprs failed", K(ret));
    } else if (OB_SUCC(ret)) {
      std::set<ObExpr *> visited;
      for (size_t i = 0; i < prepared_out_exprs_vec_.size() && OB_SUCC(ret); ++i) {
        if (OB_NOT_NULL(prepared_out_exprs_vec_[i])) {
          ret = collect_calc_exprs(prepared_out_exprs_vec_[i], column_exprs, visited, calc_exprs);
        }
      }
      for (size_t i = 0; i < prepared_flt_exprs_vec_.size() && OB_SUCC(ret); ++i) {
        if (OB_NOT_NULL(prepared_flt_exprs_vec_[i])) {
          ret = collect_calc_exprs(prepared_flt_exprs_vec_[i], column_exprs, visited, calc_exprs);
        }
      }
      if (OB_SUCC(ret) && calc_exprs.count() > 0) {
        ObOpSpec &spec = const_cast<ObOpSpec &>(op->get_spec());
        if (OB_FAIL(spec.calc_exprs_.init(calc_exprs.count()))) {
          LOG_WARN("init spec calc_exprs failed", K(ret));
        } else if (OB_FAIL(spec.calc_exprs_.prepare_allocate(calc_exprs.count()))) {
          LOG_WARN("prepare_allocate spec calc_exprs failed", K(ret));
        } else {
          for (int64_t i = 0; i < calc_exprs.count(); ++i) {
            spec.calc_exprs_.at(i) = calc_exprs.at(i);
          }
        }
      }
    }

    // Step 8: Execute
    // Pass output_exprs for SELECT expression evaluation
    if (datahub_setup_fn_ && OB_NOT_NULL(root_spec)) {
      if (OB_SUCC(ret)) {
        ret = datahub_setup_fn_(engine.get_exec_ctx(), root_spec->id_);
      }
      if (OB_SUCC(ret)) {
        result = engine.execute(op, &output_exprs, rescan_times_);
      }
    } else {
      result = engine.execute(op, &output_exprs, rescan_times_);
    }

    return result;
  }

  /**
   * @brief Build and execute the test.
   * In single-run mode: runs once with the configured rich_format_ setting.
   * In dual-run mode (enable_dual_format_check()): runs both 2.0 and 1.0, compares results.
   * @param engine The OpTestEngine instance
   * @return OpTestResult containing output rows (2.0 result in dual mode)
   */
  OpTestResult run(OpTestEngine &engine)
  {
    OpTestResult result;
    int ret = OB_SUCCESS;

    // RAII checker: will LOG_ERROR and EXPECT if ret != OB_SUCCESS at scope exit
    FatalErrorChecker error_checker(ret);

    // RAII: push builder's tenant config overrides into TLS, auto-restore on scope exit
    TestParameterGuard param_guard;
    if (enable_sql_operator_dump_ && hash_area_size_ > 0) {
      // _hash_area_size is DEF_CAP type: requires unit suffix (e.g., "4M", "1G")
      // Convert bytes to megabytes string with "M" suffix
      int64_t mb = hash_area_size_ / (1024 * 1024);
      if (mb > 0) {
        param_guard.set("_hash_area_size", std::to_string(mb) + "M");
      } else {
        // Less than 1MB, use KB
        int64_t kb = hash_area_size_ / 1024;
        param_guard.set("_hash_area_size", std::to_string(kb) + "K");
      }
    }

    // RAII: push builder's session variable overrides into TLS, auto-restore on scope exit
    TestSessionVarGuard session_var_guard;
    for (const auto &kv : session_vars_) {
      session_var_guard.set(kv.first, kv.second);
    }

    // RAII: activate tracepoints for this run, auto-reset on scope exit
    std::unique_ptr<TestTracepointGuard> tp_guard;
    if (!tracepoints_.empty()) {
      tp_guard.reset(new TestTracepointGuard(tracepoints_));
    }

    // Prepare: SQL registration, resolve, CG, collect exprs (always in FORCE_ON mode)
    ret = prepare(engine);
    if (OB_FAIL(ret)) {
      LOG_WARN("prepare failed", K(ret));
      return result;
    }

    // Adjust output expressions (e.g., Expand adds grouping_id to output)
    {
      Derived *derived = static_cast<Derived *>(this);
      derived->adjust_output_exprs();
    }

    if (dual_format_check_) {
      // Dual-run mode: execute 2.0 then 1.0, compare results
      engine.enable_rich_format(true);
      OpTestResult res_2_0 = build_and_execute(engine, true);

      engine.enable_rich_format(false);
      OpTestResult res_1_0 = build_and_execute(engine, false);

      // Restore FORCE_ON for subsequent tests
      engine.enable_rich_format(true);

      // Compare results
      EXPECT_EQ(res_2_0.row_count(), res_1_0.row_count())
          << "dual_format_check: row count mismatch: 2.0=" << res_2_0.row_count()
          << " vs 1.0=" << res_1_0.row_count();
      if (res_2_0.row_count() == res_1_0.row_count()) {
        if (dual_format_unordered_) {
          // Unordered operators (e.g. Hash GroupBy with dump): sort both sets before comparing
          auto rows_2_0 = res_2_0.get_rows();
          auto rows_1_0 = res_1_0.get_rows();
          std::sort(rows_2_0.begin(), rows_2_0.end());
          std::sort(rows_1_0.begin(), rows_1_0.end());
          for (int64_t i = 0; i < res_2_0.row_count(); ++i) {
            EXPECT_EQ(rows_2_0[i], rows_1_0[i])
                << "dual_format_check: row " << i << " mismatch between 2.0 and 1.0 (unordered)";
          }
        } else {
          for (int64_t i = 0; i < res_2_0.row_count(); ++i) {
            EXPECT_EQ(res_2_0.get_row(i), res_1_0.get_row(i))
                << "dual_format_check: row " << i << " mismatch between 2.0 and 1.0";
          }
        }
      }
      return res_2_0;
    } else {
      // Single-run mode
      engine.enable_rich_format(rich_format_);
      result = build_and_execute(engine, rich_format_);
      if (!rich_format_) {
        // Restore FORCE_ON for subsequent tests
        engine.enable_rich_format(true);
      }
      return result;
    }
  }

protected:
  /**
   * @brief Build the complete SQL SELECT statement.
   */
  std::string build_sql() const
  {
    std::string sql = "SELECT ";

    // SELECT clause
    if (!select_exprs_.empty()) {
      sql += select_exprs_;
    } else {
      sql += "*";
    }

    // FROM clause
    sql += " FROM ";
    sql += table_name_;

    // WHERE clause
    if (!where_clause_.empty()) {
      sql += " WHERE ";
      sql += where_clause_;
    }

    // GROUP BY clause
    if (!group_by_exprs_.empty()) {
      sql += " GROUP BY ";
      sql += group_by_exprs_;
    }

    // ORDER BY clause
    if (!order_by_exprs_.empty()) {
      sql += " ORDER BY ";
      sql += order_by_exprs_;
    }

    // LIMIT clause
    if (has_limit_) {
      sql += " LIMIT ";
      sql += std::to_string(limit_);
    }

    // OFFSET clause
    if (has_offset_) {
      sql += " OFFSET ";
      sql += std::to_string(offset_);
    }

    return sql;
  }

  /**
   * @brief Parse col_defs_ and create random ColumnGenerators for each column.
   * Uses priority-ordered substring matching on lowercased type strings,
   * so more specific types (e.g. mediumint) match before broader ones (e.g. int).
   */
  std::vector<ColumnGenerator> create_auto_generators() const
  {
    std::vector<ColumnGenerator> generators;
    std::string col_defs_str(col_defs_);
    size_t pos = 0;

    // Helper: extract parenthesized args from lower (e.g. "decimal(10,2)" -> "10,2")
    auto extract_paren_args = [](const std::string &lower) -> std::string {
      size_t pp = lower.find('(');
      if (pp == std::string::npos) return "";
      size_t ep = lower.find(')', pp);
      if (ep == std::string::npos) return "";
      return lower.substr(pp + 1, ep - pp - 1);
    };

    // Helper: parse precision,scale from paren args
    auto parse_precision_scale = [](const std::string &args, int default_p, int default_s)
        -> std::pair<int, int> {
      if (args.empty()) return {default_p, default_s};
      size_t cp = args.find(',');
      if (cp != std::string::npos) {
        return {std::stoi(args.substr(0, cp)), std::stoi(args.substr(cp + 1))};
      }
      return {std::stoi(args), 0};
    };

    // Helper: parse single int from paren args
    auto parse_single_int = [](const std::string &args, int default_val) -> int {
      if (args.empty()) return default_val;
      return std::stoi(args);
    };

    while (pos < col_defs_str.size()) {
      // Skip whitespace
      while (pos < col_defs_str.size() && std::isspace(col_defs_str[pos])) pos++;
      if (pos >= col_defs_str.size()) break;

      // Skip column name
      while (pos < col_defs_str.size() && !std::isspace(col_defs_str[pos])
             && col_defs_str[pos] != ',') pos++;

      // Skip whitespace
      while (pos < col_defs_str.size() && std::isspace(col_defs_str[pos])) pos++;
      if (pos >= col_defs_str.size()) break;

      // Extract type string (until comma or end, respecting parentheses)
      size_t type_start = pos;
      int paren_depth = 0;
      while (pos < col_defs_str.size()
             && (col_defs_str[pos] != ',' || paren_depth > 0)) {
        if (col_defs_str[pos] == '(') paren_depth++;
        else if (col_defs_str[pos] == ')') paren_depth--;
        pos++;
      }
      std::string col_type = col_defs_str.substr(type_start, pos - type_start);
      while (!col_type.empty() && std::isspace(col_type.back())) col_type.pop_back();
      std::string lower = col_type;
      std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);

      uint64_t col_seed = 42 + static_cast<uint64_t>(generators.size());
      std::string paren_args = extract_paren_args(lower);

      // Priority-ordered type matching: most specific first
      // Integer types
      if (lower.find("mediumint") != std::string::npos) {
        generators.push_back(gen::sequential(1));
      } else if (lower.find("smallint") != std::string::npos) {
        generators.push_back(gen::sequential(1));
      } else if (lower.find("tinyint") != std::string::npos) {
        generators.push_back(gen::sequential(1));
      } else if (lower.find("bigint") != std::string::npos) {
        generators.push_back(gen::sequential(1));
      } else if (lower.find("int32") != std::string::npos) {
        generators.push_back(gen::sequential(1));
      } else if (lower.find("int") != std::string::npos) {
        generators.push_back(gen::sequential(1));
      }
      // Floating point types
      else if (lower.find("ufloat") != std::string::npos) {
        generators.push_back(gen::random_double(0.0, 100000.0, col_seed));
      } else if (lower.find("udouble") != std::string::npos) {
        generators.push_back(gen::random_double(0.0, 100000.0, col_seed));
      } else if (lower.find("float") != std::string::npos) {
        generators.push_back(gen::random_double(0.0, 100000.0, col_seed));
      } else if (lower.find("double") != std::string::npos) {
        generators.push_back(gen::random_double(0.0, 100000.0, col_seed));
      }
      // Decimal/number types
      else if (lower.find("number_float") != std::string::npos) {
        auto [p, s] = parse_precision_scale(paren_args, 10, 2);
        generators.push_back(gen::random_decimal(p, s, col_seed));
      } else if (lower.find("unumber") != std::string::npos) {
        generators.push_back(gen::random_decimal(10, 0, col_seed));
      } else if (lower.find("number") != std::string::npos) {
        auto [p, s] = parse_precision_scale(paren_args, 10, 2);
        generators.push_back(gen::random_decimal(p, s, col_seed));
      } else if (lower.find("decimal") != std::string::npos) {
        auto [p, s] = parse_precision_scale(paren_args, 10, 2);
        generators.push_back(gen::random_decimal(p, s, col_seed));
      }
      // Timestamp/datetime types
      else if (lower.find("timestamp_tz") != std::string::npos
               || lower.find("timestamp_ltz") != std::string::npos
               || lower.find("timestamp_nano") != std::string::npos
               || lower.find("timestamp") != std::string::npos) {
        generators.push_back(gen::random_datetime(col_seed));
      } else if (lower.find("mysql_datetime") != std::string::npos) {
        generators.push_back(gen::random_datetime(col_seed));
      } else if (lower.find("datetime") != std::string::npos) {
        generators.push_back(gen::random_datetime(col_seed));
      }
      // Date/time/year types
      else if (lower.find("mysql_date") != std::string::npos) {
        generators.push_back(gen::random_date(col_seed));
      } else if (lower.find("date") != std::string::npos) {
        generators.push_back(gen::random_date(col_seed));
      } else if (lower.find("time") != std::string::npos) {
        generators.push_back(gen::random_time(col_seed));
      } else if (lower.find("year") != std::string::npos) {
        generators.push_back(gen::random_year(col_seed));
      }
      // Binary large object types
      else if (lower.find("longblob") != std::string::npos
               || lower.find("mediumblob") != std::string::npos
               || lower.find("tinyblob") != std::string::npos
               || lower.find("blob") != std::string::npos) {
        generators.push_back(gen::random_string(8, col_seed));
      }
      // Text types
      else if (lower.find("longtext") != std::string::npos
               || lower.find("mediumtext") != std::string::npos
               || lower.find("tinytext") != std::string::npos
               || lower.find("text") != std::string::npos) {
        generators.push_back(gen::random_string(10, col_seed));
      }
      // National char types (must be before varchar/char)
      else if (lower.find("nvarchar2") != std::string::npos
               || lower.find("nchar") != std::string::npos) {
        generators.push_back(gen::random_string(16, col_seed));
      }
      // String types
      else if (lower.find("varchar") != std::string::npos
               || lower.find("char") != std::string::npos) {
        int64_t str_len = 16;
        if (!paren_args.empty()) {
          str_len = std::min(static_cast<int64_t>(parse_single_int(paren_args, 16)),
                             static_cast<int64_t>(16));
        }
        generators.push_back(gen::random_string(str_len, col_seed));
      }
      // Hex/raw types
      else if (lower.find("hex_string") != std::string::npos
               || lower.find("raw") != std::string::npos) {
        generators.push_back(gen::random_string(8, col_seed));
      }
      // Interval types
      else if (lower.find("interval_ym") != std::string::npos) {
        generators.push_back(gen::random_int(1, 120, col_seed));
      } else if (lower.find("interval_ds") != std::string::npos) {
        generators.push_back(gen::random_int(1, 86400000000000LL, col_seed));
      }
      // Bit type
      else if (lower.find("bit") != std::string::npos) {
        int max_bits = 64;
        if (!paren_args.empty()) {
          max_bits = parse_single_int(paren_args, 64);
        }
        generators.push_back(gen::random_bit(max_bits, col_seed));
      }
      // Enum/set types
      else if (lower.find("enum") != std::string::npos) {
        generators.push_back(gen::random_int(1, 5, col_seed));
      } else if (lower.find("set") != std::string::npos) {
        generators.push_back(gen::random_int(1, 5, col_seed));
      }
      // JSON/geometry/roaringbitmap
      else if (lower.find("json") != std::string::npos) {
        generators.push_back(gen::constant(std::string("{}")));
      } else if (lower.find("geometry") != std::string::npos) {
        generators.push_back(gen::constant(std::string("")));
      } else if (lower.find("roaringbitmap") != std::string::npos) {
        generators.push_back(gen::constant(std::string("")));
      }
      // Lob/urowid/udt/collection
      else if (lower.find("lob") != std::string::npos
               || lower.find("urowid") != std::string::npos
               || lower.find("udt") != std::string::npos
               || lower.find("collection") != std::string::npos) {
        generators.push_back(gen::random_string(10, col_seed));
      }
      // Default fallback: random string
      else {
        generators.push_back(gen::random_string(10, col_seed));
      }

      if (pos < col_defs_str.size() && col_defs_str[pos] == ',') pos++;
    }
    return generators;
  }

protected:
  // ===== Context for create_spec() =====
  // These are set by run() before calling create_spec(), so derived classes
  // can access them to fill operator-specific fields (e.g., wf_infos_, aggr_infos_)

  /**
   * @brief The resolved ObSelectStmt from SQL resolution.
   * Contains window function exprs (get_window_func_exprs()), aggregate exprs (get_aggr_items()),
   * group by exprs, etc. Used by WindowFunction/ScalarAggregate create_spec().
   */
  ObSelectStmt *resolved_stmt_ = nullptr;

  /**
   * @brief The column expressions from child output (column_exprs in run()).
   * These are the actual table column expressions that the mock data source outputs.
   * Used for filling all_expr_ in WindowFunction spec, etc.
   */
  const ExprFixedArray *child_output_exprs_ = nullptr;

  /**
   * @brief The physical plan pointer.
   * Used by OpTestWFCG/OpTestAggrCG to set phy_plan_ member.
   */
  ObPhysicalPlan *phy_plan_ = nullptr;

  // Table definition
  std::string table_name_;
  std::string col_defs_;

  // SQL clauses
  std::string select_exprs_;
  std::vector<std::string> output_expr_strs_;  // Output expression strings to match from select_items
  std::string where_clause_;
  std::string group_by_exprs_;
  std::string order_by_exprs_;

  // Test data
  std::vector<TestRow> test_data_;

  // Sorted data info (for documentation purposes)
  bool is_sorted_data_ = false;
  std::string sorted_desc_;

  // File-based test data (table_name -> file_path)
  // Takes priority over test_data_ if set
  std::map<std::string, std::string> file_data_map_;

  // Generator mode data
  int64_t generator_row_num_ = 0;
  std::vector<ColumnGenerator> column_generators_;
  bool use_generator_mode_ = false;

  // Configuration
  int64_t batch_size_;
  int64_t rescan_times_;
  VectorFormat vector_format_ = VEC_DISCRETE;  // Vector format for variable-length columns

  // Limit/Offset support
  int64_t limit_ = 0;
  int64_t offset_ = 0;
  bool has_limit_ = false;
  bool has_offset_ = false;

  // SQL mode support
  SqlMode sql_mode_ = SqlMode::MYSQL;

  // SQL operator dump configuration
  bool enable_sql_operator_dump_ = false;
  int64_t hash_area_size_ = 1024 * 1024;  // Default 1MB
  OpTestEngine::DumpVerifyMode dump_verify_mode_ = OpTestEngine::DumpVerifyMode::FULL_ROWS;

  // Rescan memory check configuration
  bool rescan_memory_check_ = true;  // Enable rescan memory check by default
  int64_t rescan_memory_tolerance_bytes_ = 0;  // 0 means auto (max(first_scan * 10%, 64KB))

  // Custom eval function overrides (applied to output exprs after CG)
  ObExpr::EvalFunc custom_eval_func_ = nullptr;
  ObExpr::EvalVectorFunc custom_eval_vector_func_ = nullptr;

  // Rich format / dual format check configuration
  bool rich_format_ = true;              // Default: use 2.0 vec operator
  bool dual_format_check_ = false;       // Default: single-run mode
  bool dual_format_unordered_ = false;   // When true, sort rows before comparing 1.0 vs 2.0

  // Per-column vector format override
  std::vector<VectorFormat> col_formats_;  // Empty = use vector_format_ fallback

  // Variable batch size function
  std::function<int64_t(int64_t)> batch_size_fn_;  // nullptr = use max_row_cnt

  // Tracepoint overrides (applied via RAII in run())
  std::vector<TestTracepoint> tracepoints_;

  // Input skip injection function
  std::function<void(int64_t, int64_t, ObBitVector *)> input_skip_fn_;  // nullptr = no skip

  // Session variable overrides (applied in run() via TestSessionVarGuard)
  std::map<std::string, std::string> session_vars_;

  // Performance recording
  bool perf_record_ = false;

  // Intermediate state for dual-run mode (set by prepare(), used by build_and_execute())
  std::vector<ObExpr *> prepared_col_exprs_vec_;
  std::vector<ObExpr *> prepared_out_exprs_vec_;
  std::vector<ObExpr *> prepared_flt_exprs_vec_;
  ObExpr *prepared_limit_expr_ = nullptr;
  ObExpr *prepared_offset_expr_ = nullptr;
  int64_t prepared_condition_size_ = 0;

  // ===== Datahub Mock Support =====

  /**
   * @brief Set datahub setup function (type-erased for header isolation).
   * The caller is responsible for initializing and destroying MockDatahubContext.
   */
  Derived& set_datahub_fn(std::function<int(ObExecContext&, uint64_t)> fn)
  {
    datahub_setup_fn_ = std::move(fn);
    return static_cast<Derived&>(*this);
  }

  std::function<int(ObExecContext&, uint64_t)> datahub_setup_fn_;
};

}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_BASE_H_