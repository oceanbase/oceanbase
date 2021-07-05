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

#include <gtest/gtest.h>
#define protected public
#define private public
#include "sql/test_sql_utils.h"
#include "lib/utility/ob_test_util.h"
#include "sql/rewrite/ob_query_range.h"
#include "sql/ob_sql_init.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "common/ob_clock_generator.h"
#include "lib/json/ob_json_print_utils.h"
#include <fstream>
#undef protected
#undef private
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::sql;
using namespace oceanbase::share::schema;

const int64_t BUF_LEN = 102400;
class ObQueryRangeTest : public ::testing::Test {
public:
  ObQueryRangeTest();
  virtual ~ObQueryRangeTest();
  virtual void SetUp();
  virtual void TearDown();
  int set_column_info(ColumnItem& column_item, ObObjType data_type, const char* column_name, int64_t pos);
  void resolve_condition(
      const ColumnIArray& range_columns, const char* expr_str, ObRawExpr*& expr, ParamStore* params = NULL)
  {
    ObArray<ObQualifiedName> columns;
    ObArray<ObVarInfo> sys_vars;
    ObArray<ObSubQueryInfo> sub_query_info;
    ObArray<ObAggFunRawExpr*> aggr_exprs;
    ObArray<ObWinFunRawExpr*> win_exprs;
    ObTimeZoneInfo tz_info;
    ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
    ObExprResolveContext ctx(expr_factory_, &tz_info, case_mode);
    ctx.dest_collation_ = CS_TYPE_UTF8MB4_BIN;
    ctx.connection_charset_ = CHARSET_UTF8MB4;
    ctx.param_list_ = params;
    ObSQLSessionInfo session;
    session.set_use_static_typing_engine(false);
    ctx.session_info_ = &session;

    OK(ObRawExprUtils::make_raw_expr_from_str(
        expr_str, strlen(expr_str), ctx, expr, columns, sys_vars, &sub_query_info, aggr_exprs, win_exprs));
    for (int64_t i = 0; i < columns.count(); ++i) {
      ObQualifiedName& col = columns.at(i);
      col.ref_expr_->set_data_type(ObIntType);
      for (int64_t j = 0; j < range_columns.count(); ++j) {
        ASSERT_FALSE(NULL == range_columns.at(j).expr_);
        if (col.col_name_ == range_columns.at(j).column_name_) {
          ObColumnRefRawExpr* b_expr = static_cast<ObColumnRefRawExpr*>(col.ref_expr_);
          ASSERT_FALSE(NULL == b_expr);
          (*b_expr).assign(*(range_columns.at(j).expr_));
          if (!ob_is_string_type(b_expr->get_data_type())) {
            b_expr->set_collation_type(CS_TYPE_BINARY);
          }
        }
      }
    }
    OK(expr->extract_info());
    OK(expr->deduce_type(&session));
  }

  void split_expr(ObRawExpr* expr, ObIArray<ObRawExpr*>& exprs)
  {
    if (NULL == expr) {

    } else if (T_OP_AND == expr->get_expr_type()) {
      if (T_OP_AND != expr->get_param_expr(0)->get_expr_type()) {
        OK(exprs.push_back(expr->get_param_expr(0)));
      } else {
        split_expr(expr->get_param_expr(0), exprs);
      }
      if (T_OP_AND != expr->get_param_expr(1)->get_expr_type()) {
        OK(exprs.push_back(expr->get_param_expr(1)));
      } else {
        split_expr(expr->get_param_expr(1), exprs);
      }
    } else {
      OK(exprs.push_back(expr));
    }
  }

  void resolve_expr(const char* expr_str, ObRawExpr*& expr, ColumnIArray& ob_range_columns, ParamStore& params)
  {
    resolve_expr(expr_str, expr, ob_range_columns, params, CS_TYPE_UTF8MB4_GENERAL_CI, CS_TYPE_UTF8MB4_GENERAL_CI);
  }

  void resolve_expr(const char* expr_str, ObRawExpr*& expr, ColumnIArray& ob_range_columns, ParamStore& params,
      ObCollationType con_type, ObCollationType col_type, const int64_t max_cols_num = 100)
  {
    ObArray<ObQualifiedName> columns;
    ObArray<ObVarInfo> sys_vars;
    ObArray<ObSubQueryInfo> sub_query_info;
    ObArray<ObAggFunRawExpr*> aggr_exprs;
    ObArray<ObWinFunRawExpr*> win_exprs;
    ObTimeZoneInfo tz_info;
    ObArray<ColumnItem> tmp_range_columns;
    ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
    ObExprResolveContext ctx(expr_factory_, &tz_info, case_mode);
    ctx.dest_collation_ = con_type;
    ctx.param_list_ = &params;
    ObSQLSessionInfo session;
    session.set_use_static_typing_engine(false);
    ctx.session_info_ = &session;

    for (int64_t i = 0; i < more_range_columns_.count(); ++i) {
      OK(tmp_range_columns.push_back(more_range_columns_.at(i)));
    }
    OK(ObRawExprUtils::make_raw_expr_from_str(
        expr_str, strlen(expr_str), ctx, expr, columns, sys_vars, &sub_query_info, aggr_exprs, win_exprs));
    bool init_flag[10] = {false};
    for (int64_t i = 0; i < columns.count(); ++i) {
      ObQualifiedName& col = columns.at(i);
      for (int64_t j = 0; j < more_range_columns_.count(); ++j) {
        if (col.col_name_ == more_range_columns_.at(j).column_name_) {
          ObColumnRefRawExpr* b_expr = static_cast<ObColumnRefRawExpr*>(col.ref_expr_);
          ASSERT_FALSE(NULL == b_expr);
          (*b_expr).assign(*(more_range_columns_.at(j).expr_));
          if (ob_is_string_type(b_expr->get_data_type())) {
            b_expr->set_collation_type(col_type);
          } else if (ob_is_float_tc(b_expr->get_data_type())) {
            b_expr->set_precision(10);
            b_expr->set_scale(5);
          }
          if (!init_flag[j]) {
            init_flag[j] = true;
            tmp_range_columns.at(j).table_id_ = b_expr->get_table_id();
            tmp_range_columns.at(j).column_id_ = b_expr->get_column_id();
            tmp_range_columns.at(j).expr_ = b_expr;
            tmp_range_columns.at(j).column_name_ = b_expr->get_column_name();
          }
        }
      }
    }
    for (int64_t i = 0; i < tmp_range_columns.count() && ob_range_columns.count() < max_cols_num; ++i) {
      if (init_flag[i]) {
        OK(ob_range_columns.push_back(tmp_range_columns.at(i)));
      }
    }

    if (100 != max_cols_num && ob_range_columns.count() < max_cols_num) {
      for (int64_t i = 0; i < max_cols_num - ob_range_columns.count(); ++i) {
        ColumnItem extra_item = ob_range_columns.at(ob_range_columns.count() - 1);
        extra_item.column_id_ = extra_item.column_id_ + 1;
        OK(ob_range_columns.push_back(extra_item));
      }
    }
    OK(expr->extract_info());
    OK(expr->deduce_type(&session));
  }
  void split_and_condition(ObRawExpr* expr, ObIArray<ObRawExpr*>& and_exprs)
  {
    OB_ASSERT(expr);
    if (expr->get_expr_type() == T_OP_AND) {
      ObOpRawExpr* and_expr = static_cast<ObOpRawExpr*>(expr);
      for (int64_t i = 0; i < and_expr->get_param_count(); ++i) {
        ObRawExpr* sub_expr = and_expr->get_param_expr(i);
        OB_ASSERT(sub_expr);
        split_and_condition(sub_expr, and_exprs);
      }
    } else {
      OK(and_exprs.push_back(expr));
    }
  }

  void except_result(const ColumnIArray& range_columns, ParamsIArray& params, const char* condition,
      const char* except_range, ObGetMethodArray& except_get_methods)
  {
    ObQueryRangeArray ranges;
    ObGetMethodArray get_methods;
    ObQueryRange enc_query_range;
    ObQueryRange dec_query_range1;
    ObQueryRange dec_query_range2;
    ObQueryRange multi_query_range;
    ObArenaAllocator allocator(ObModIds::TEST);
    const ObDataTypeCastParams dtc_params;
    ObRawExpr* expr = NULL;
    char buf[512 * 1024] = {'\0'};
    int64_t pos = 0;
    int64_t data_len = 0;
    resolve_condition(range_columns, condition, expr);
    OK(enc_query_range.preliminary_extract_query_range(range_columns, expr, NULL));
    OK(enc_query_range.serialize(buf, sizeof(buf), pos));
    data_len = pos;
    pos = 0;
    OK(dec_query_range1.deserialize(buf, data_len, pos));
    ASSERT_EQ(0, strcmp(to_cstring(enc_query_range), to_cstring(dec_query_range1)));
    if (dec_query_range1.need_deep_copy()) {
      OK(dec_query_range1.final_extract_query_range(params, NULL));
      pos = 0;
      OK(dec_query_range1.serialize(buf, sizeof(buf), pos));
      data_len = pos;
      pos = 0;
      OK(dec_query_range2.deserialize(buf, data_len, pos));
      ASSERT_EQ(0, strcmp(to_cstring(dec_query_range1), to_cstring(dec_query_range2)));
      _OB_LOG(INFO, "serialize_size = %ld\n", dec_query_range1.get_serialize_size());
      OK(dec_query_range1.get_tablet_ranges(ranges, get_methods, dtc_params));
    } else {
      OK(dec_query_range1.get_tablet_ranges(allocator, params, ranges, get_methods, NULL));
    }
    _OB_LOG(INFO, "ranges: %s, except_range: %s", to_cstring(ranges), except_range);

    ASSERT_EQ(0, strcmp(to_cstring(ranges), except_range));
    ASSERT_EQ(get_methods.count(), except_get_methods.count());
    for (int64_t j = 0; j < get_methods.count(); ++j) {
      EXPECT_EQ(get_methods.at(j), except_get_methods.at(j));
    }

    ranges.reset();
    get_methods.reset();
    OK(dec_query_range2.get_tablet_ranges(ranges, get_methods, dtc_params));
    _OB_LOG(DEBUG, "ranges: %s, except_range: %s", to_cstring(ranges), except_range);
    ASSERT_EQ(0, strcmp(to_cstring(ranges), except_range));
    ASSERT_EQ(get_methods.count(), except_get_methods.count());
    for (int64_t j = 0; j < get_methods.count(); ++j) {
      EXPECT_EQ(get_methods.at(j), except_get_methods.at(j));
    }

    ranges.reset();
    get_methods.reset();
    ObArray<ObRawExpr*> and_exprs;
    split_and_condition(expr, and_exprs);
    //    OK(multi_query_range.preliminary_extract_query_range(range_columns, and_exprs, NULL));
    //    OK(multi_query_range.final_extract_query_range(params, NULL));
    OK(multi_query_range.preliminary_extract_query_range(range_columns, and_exprs, dtc_params));
    OK(multi_query_range.final_extract_query_range(params, dtc_params));
    OK(multi_query_range.get_tablet_ranges(ranges, get_methods, dtc_params));
    _OB_LOG(DEBUG,
        "and_exprs_count: %ld, ranges: %s, except_range: %s",
        and_exprs.count(),
        to_cstring(ranges),
        except_range);
    ASSERT_EQ(0, strcmp(to_cstring(ranges), except_range));
  }

  void reset()
  {
    allocator_.reset();
  }

private:
  // disallow copy
  ObQueryRangeTest(const ObQueryRangeTest& other);
  ObQueryRangeTest& operator=(const ObQueryRangeTest& other);

protected:
  // data members
  uint64_t table_id_;
  uint64_t column_id1_;
  uint64_t column_id2_;
  uint64_t column_id3_;
  ObArenaAllocator allocator_;
  ObRawExprFactory expr_factory_;
  ParamStore params_;
  ObArray<ColumnItem> single_range_columns_;
  ObArray<ColumnItem> double_range_columns_;
  ObArray<ColumnItem> triple_range_columns_;
  ObArray<ColumnItem> more_range_columns_;
  ObArray<ColumnItem> extra_range_columns_;
  ObColumnRefRawExpr ref_col_;  // a
  ObQueryRange query_range;
  void get_query_range(const char* expr, const char*& json_expr);
  void get_query_range_filter(
      const char* sql_expr, const char*& json_expr, char* buf, int64_t& pos, const int64_t cols_num);
  void get_query_range_collation(const char* expr, const char*& json_expr, char* buf, int64_t& pos);
};

ObQueryRangeTest::ObQueryRangeTest()
    : table_id_(3003),
      column_id1_(16),
      column_id2_(17),
      column_id3_(18),
      allocator_(ObModIds::TEST),
      expr_factory_(allocator_),
      params_((ObWrapperAllocator(CURRENT_CONTEXT.get_arena_allocator()))),
      ref_col_(table_id_, column_id1_, T_REF_COLUMN)
{}

ObQueryRangeTest::~ObQueryRangeTest()
{}

const char* get_columns_char(int64_t i)
{
  const char* ptr = NULL;
  switch (i) {
    case 0:
      ptr = "a";
      break;
    case 1:
      ptr = "b";
      break;
    case 2:
      ptr = "c";
      break;
    case 3:
      ptr = "d";
      break;
    case 4:
      ptr = "e";
      break;
    case 5:
      ptr = "f";
      break;
    case 6:
      ptr = "g";
      break;
    case 7:
      ptr = "h";
      break;
    case 8:
      ptr = "i";
      break;
    case 9:
      ptr = "j";
      break;
    case 10:
      ptr = "k";
      break;
    case 11:
      ptr = "l";
      break;
    case 12:
      ptr = "m";
      break;
    case 13:
      ptr = "n";
      break;
    case 14:
      ptr = "o";
      break;
  }
  return ptr;
}

int ObQueryRangeTest::set_column_info(
    ColumnItem& column_item, ObObjType data_type, const char* column_name, int64_t pos)
{
  int ret = OB_SUCCESS;
  ObColumnRefRawExpr* col_expr = NULL;
  if (OB_FAIL(expr_factory_.create_raw_expr(T_REF_COLUMN, col_expr))) {
    SQL_REWRITE_LOG(WARN, "create raw expr failed", K(ret));
  } else {
    col_expr->add_flag(IS_COLUMN);
    col_expr->set_data_type(data_type);
    col_expr->set_ref_id(table_id_, column_id1_ + pos);
    col_expr->get_column_name() = ObString::make_string(column_name);
    if (ob_is_string_type(data_type)) {
      ObAccuracy accuracy;
      accuracy.set_length(512);
      col_expr->set_accuracy(accuracy);
      col_expr->set_collation_type(CS_TYPE_UTF8MB4_BIN);
    } else {
      col_expr->set_collation_type(CS_TYPE_BINARY);
    }
    column_item.table_id_ = col_expr->get_table_id();
    column_item.column_id_ = col_expr->get_column_id();
    column_item.expr_ = col_expr;
    column_item.column_name_ = col_expr->get_column_name();
  }
  return ret;
}

void ObQueryRangeTest::SetUp()
{
  ColumnItem col;
  OK(set_column_info(col, ObIntType, "a", 0));
  OK(single_range_columns_.push_back(col));
  OK(set_column_info(col, ObIntType, "a", 0));
  OK(double_range_columns_.push_back(col));
  OK(set_column_info(col, ObIntType, "b", 1));
  OK(double_range_columns_.push_back(col));
  OK(set_column_info(col, ObIntType, "a", 0));
  OK(triple_range_columns_.push_back(col));
  OK(set_column_info(col, ObIntType, "b", 1));
  OK(triple_range_columns_.push_back(col));
  OK(set_column_info(col, ObIntType, "c", 2));
  OK(triple_range_columns_.push_back(col));

  ref_col_.add_flag(IS_COLUMN);
  for (int64_t i = 0; i < 14; i++) {
    if (i < 3) {  // column('a'-'e') is IntType. column('f'-'j') is VarcharType. other type is dealed specially
      OK(set_column_info(col, ObIntType, get_columns_char(i), i));
    } else if (i == 3) {
      OK(set_column_info(col, ObFloatType, get_columns_char(i), i));
    } else if (i == 4) {
      OK(set_column_info(col, ObUInt64Type, get_columns_char(i), i));
    } else {
      OK(set_column_info(col, ObVarcharType, get_columns_char(i), i));
    }
    if (i < 10) {
      OK(more_range_columns_.push_back(col));
    } else {
      OK(extra_range_columns_.push_back(col));
    }
  }
}

void ObQueryRangeTest::TearDown()
{}
/*
 * get final_sql and params from case file : such as (a, b, c) < (?1, ?2, ?3) and (a, b, c) > (?0, ?2, ?3) ->
 * final_sql:(a, b, c) > (?, ? ,?)   params : 0,2,3
 */
void get_final_sql(const char* sql_expr, char* final_sql, ParamStore& params, char (*var)[100])
{
  memset(var, 0, sizeof(*var));
  int64_t val = 0;
  int64_t j = 0;
  double fval = 0.0;  // test cast

  bool num_flag = false;
  bool var_flag = false;
  bool is_minus = false;
  bool is_float = false;
  int len = 0;
  int cnt = 0;
  int y = 10;
  for (int64_t i = 0; i < (int64_t)strlen(sql_expr); ++i) {
    if (num_flag) {
      if (sql_expr[i] == '}') {
        ObObjParam obj;
        val = is_minus ? -val : val;
        if (is_float) {
          is_float = false;
          fval = is_minus ? -fval : fval;
          fval += static_cast<double>(val);
          obj.set_double(fval);
        } else {
          obj.set_int(val);
        }
        obj.set_param_meta();
        params.push_back(obj);
        num_flag = false;
        val = 0;
      } else if (is_float) {
        fval = fval + (sql_expr[i] - '0') * 1.0 / y;
        y *= 10;
      } else if (sql_expr[i] >= '0' && sql_expr[i] <= '9') {
        val = val * 10 + sql_expr[i] - '0';
      } else if (sql_expr[i] == '-') {
        is_minus = true;
      } else if (sql_expr[i] == '.') {
        fval = 0.0;
        is_float = true;
        y = 10;
      }
    } else if (var_flag) {
      if (sql_expr[i] == ']') {
        ObObjParam obj;
        var[cnt][len] = '\0';
        if (0 == strcasecmp(var[cnt], "NULL")) {
          obj.set_null();
        } else {
          obj.set_varchar(var[cnt]);
        }
        obj.set_collation_level(CS_LEVEL_COERCIBLE);
        obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
        obj.set_param_meta();
        params.push_back(obj);
        var_flag = false;
        len = 0;
        cnt++;
      } else {
        var[cnt][len++] = sql_expr[i];
      }
    } else {
      if (sql_expr[i] == '{') {
        num_flag = true;
      } else if (sql_expr[i] == '[') {
        var_flag = true;
      }
    }
    if (sql_expr[i] != '{' && sql_expr[i] != '}' && sql_expr[i] != '[' && sql_expr[i] != ']' && !num_flag &&
        !var_flag) {
      final_sql[j++] = sql_expr[i];
    }
  }
  final_sql[j] = '\0';
}

void ObQueryRangeTest::get_query_range(const char* sql_expr, const char*& json_expr)
{
  char var[100][100];
  ObQueryRangeArray ranges;
  ObGetMethodArray get_methods;
  ObRawExpr* expr = NULL;
  ObArray<ColumnItem> range_columns;
  ParamStore params((ObWrapperAllocator(CURRENT_CONTEXT.get_arena_allocator())));
  ObArenaAllocator allocator(ObModIds::TEST);
  const ObDataTypeCastParams dtc_params;
  ObQueryRange pre_query_range;
  char final_sql[100];
  get_final_sql(sql_expr, final_sql, params, var);
  resolve_expr(final_sql, expr, range_columns, params, CS_TYPE_UTF8MB4_GENERAL_CI, CS_TYPE_UTF8MB4_GENERAL_CI);
  OB_ASSERT(expr);
  //_OB_LOG(INFO,"-----%s\n", final_sql);
  //_OB_LOG(WARN, "expr: %s", CSJ(expr));
  query_range.reset();
  OB_LOG(INFO, "get query range sql", K(final_sql));
  OK(pre_query_range.preliminary_extract_query_range(range_columns, expr, dtc_params));
  char* ser_buf = NULL;
  int64_t ser_len = pre_query_range.get_serialize_size();
  int64_t pos = 0;
  ASSERT_FALSE(NULL == (ser_buf = (char*)allocator.alloc(ser_len)));
  OK(pre_query_range.serialize(ser_buf, ser_len, pos));
  ASSERT_EQ(ser_len, pos);
  pos = 0;
  OK(query_range.deserialize(ser_buf, ser_len, pos));
  ASSERT_EQ(0, strcmp(to_cstring(pre_query_range), to_cstring(query_range)));
  if (query_range.need_deep_copy()) {
    query_range.final_extract_query_range(params, NULL);
    OK(query_range.get_tablet_ranges(ranges, get_methods, dtc_params));
  } else {
    OK(query_range.get_tablet_ranges(allocator, params, ranges, get_methods, NULL));
  }

  char buf[BUF_LEN];
  pos = 0;
  bool flag = false;
  if (!query_range.need_deep_copy()) {
    databuff_printf(buf, BUF_LEN, pos, "query range not need to deep copy\n");
    query_range.final_extract_query_range(params, NULL);
  } else {
    databuff_printf(buf, BUF_LEN, pos, "query range need to deep copy\n");
  }
  OK(query_range.is_min_to_max_range(flag, NULL));
  if (flag) {
    databuff_printf(buf, BUF_LEN, pos, "is min_to_max_range\n");
  } else {
    databuff_printf(buf, BUF_LEN, pos, "is not min_to_max_range\n");
  }
  databuff_printf(buf, BUF_LEN, pos, "ranges.count() = %ld\n", ranges.count());
  databuff_printf(buf, BUF_LEN, pos, "get_methods.count() = %ld\n", get_methods.count());
  for (int64_t i = 0; i < get_methods.count(); ++i) {
    databuff_printf(buf, BUF_LEN, pos, "get_methods %ld: %d\n", i, get_methods.at(i));
  }
  for (int64_t i = 0; i < ranges.count(); ++i) {
    databuff_printf(buf, BUF_LEN, pos, "star_border_flag[%ld] = %d\n", i, ranges.at(i)->border_flag_.inclusive_start());
    databuff_printf(buf, BUF_LEN, pos, "end_border_flag[%ld] = %d\n", i, ranges.at(i)->border_flag_.inclusive_end());
  }
  databuff_printf(buf, BUF_LEN, pos, "count of rang columns = %ld\n", range_columns.count());
  databuff_printf(buf, BUF_LEN, pos, to_cstring(ranges));
  databuff_printf(buf, BUF_LEN, pos, "\n");
  json_expr = buf;
  OK(query_range.get_tablet_ranges(allocator_, params, ranges, flag, NULL));
  query_range.reset();
}

void ObQueryRangeTest::get_query_range_filter(
    const char* sql_expr, const char*& json_expr, char* buf, int64_t& pos, const int64_t cols_num)
{
  char var[100][100];
  ObQueryRangeArray ranges;
  ObGetMethodArray get_methods;
  ObRawExpr* expr = NULL;
  ObArray<ColumnItem> range_columns;
  ParamStore params((ObWrapperAllocator(CURRENT_CONTEXT.get_arena_allocator())));
  ObArenaAllocator allocator(ObModIds::TEST);
  const ObDataTypeCastParams dtc_params;
  ObQueryRange pre_query_range;
  char final_sql[100];
  get_final_sql(sql_expr, final_sql, params, var);
  resolve_expr(
      final_sql, expr, range_columns, params, CS_TYPE_UTF8MB4_GENERAL_CI, CS_TYPE_UTF8MB4_GENERAL_CI, cols_num);
  ObSEArray<ObRawExpr*, 16> exprs;
  split_expr(expr, exprs);
  OB_ASSERT(expr);
  query_range.reset();
  OB_LOG(INFO, "get query range sql", K(final_sql));
  OK(query_range.preliminary_extract_query_range(range_columns, exprs, dtc_params));
  databuff_printf(buf, BUF_LEN, pos, "\n**rowkey num = %ld**  \n", cols_num);
  databuff_printf(buf, BUF_LEN, pos, "**filter count = %ld**\n", query_range.get_range_exprs().count());

  bool flag = false;
  if (query_range.need_deep_copy()) {
    query_range.final_extract_query_range(params, NULL);
    OK(query_range.get_tablet_ranges(ranges, get_methods, dtc_params));
  } else {
    OK(query_range.get_tablet_ranges(allocator, params, ranges, get_methods, NULL));
  }

  OK(query_range.is_min_to_max_range(flag, NULL));
  if (flag) {
    databuff_printf(buf, BUF_LEN, pos, "is min_to_max_range\n");
  } else {
    databuff_printf(buf, BUF_LEN, pos, "is not min_to_max_range\n");
  }
  databuff_printf(buf, BUF_LEN, pos, "ranges.count() = %ld\n", ranges.count());
  databuff_printf(buf, BUF_LEN, pos, "get_methods.count() = %ld\n", get_methods.count());
  for (int64_t i = 0; i < get_methods.count(); ++i) {
    databuff_printf(buf, BUF_LEN, pos, "get_methods %ld: %d\n", i, get_methods.at(i));
  }
  for (int64_t i = 0; i < ranges.count(); ++i) {
    databuff_printf(buf, BUF_LEN, pos, "star_border_flag[%ld] = %d\n", i, ranges.at(i)->border_flag_.inclusive_start());
    databuff_printf(buf, BUF_LEN, pos, "end_border_flag[%ld] = %d\n", i, ranges.at(i)->border_flag_.inclusive_end());
  }
  databuff_printf(buf, BUF_LEN, pos, "count of rang columns = %ld\n", range_columns.count());
  databuff_printf(buf, BUF_LEN, pos, to_cstring(ranges));
  databuff_printf(buf, BUF_LEN, pos, "\n");
  json_expr = buf;
  OK(query_range.get_tablet_ranges(allocator_, params, ranges, flag, NULL));
  query_range.reset();
}

void ObQueryRangeTest::get_query_range_collation(const char* sql_expr, const char*& json_expr, char* buf, int64_t& pos)
{
  char var[100][100];
  ObQueryRangeArray ranges;
  ObGetMethodArray get_methods;
  ParamStore params((ObWrapperAllocator(CURRENT_CONTEXT.get_arena_allocator())));
  const ObDataTypeCastParams dtc_params;
  char final_sql[100];
  get_final_sql(sql_expr, final_sql, params, var);
  ObCollationType cs_type[3] = {CS_TYPE_UTF8MB4_GENERAL_CI, CS_TYPE_UTF8MB4_BIN, CS_TYPE_BINARY};
  for (int i = 0; i < 3; i++) {
    for (int j = 0; j < 3; j++) {
      ObArray<ColumnItem> range_columns;
      ObRawExpr* expr = NULL;
      ObCollationType conn_type = cs_type[i];
      ObCollationType col_type = cs_type[j];
      resolve_expr(final_sql, expr, range_columns, params, conn_type, col_type);
      // conn_type: connection_collation-->collation type of the params
      // col_type : the collation type of columns
      // each result_type of expr is deduced by param_obj_type and column_type
      databuff_printf(buf,
          BUF_LEN,
          pos,
          "%s--------------connection_collation = %d col_type = %d\n",
          sql_expr,
          conn_type,
          col_type);
      OB_ASSERT(expr);
      OK(query_range.preliminary_extract_query_range(range_columns, expr, dtc_params));
      OK(query_range.final_extract_query_range(params, NULL));
      OK(query_range.get_tablet_ranges(ranges, get_methods, dtc_params));
      bool flag = false;
      OK(query_range.is_min_to_max_range(flag, NULL));
      if (flag) {
        databuff_printf(buf, BUF_LEN, pos, "is min_to_max_range\n");
      } else {
        databuff_printf(buf, BUF_LEN, pos, "is not min_to_max_range\n");
      }
      databuff_printf(buf, BUF_LEN, pos, "ranges.count() = %ld\n", ranges.count());
      databuff_printf(buf, BUF_LEN, pos, "get_methods.count() = %ld\n", get_methods.count());
      for (int64_t i = 0; i < get_methods.count(); ++i) {
        databuff_printf(buf, BUF_LEN, pos, "get_methods %ld: %d\n", i, get_methods.at(i));
      }
      for (int64_t i = 0; i < ranges.count(); ++i) {
        databuff_printf(
            buf, BUF_LEN, pos, "star_border_flag[%ld] = %d\n", i, ranges.at(i)->border_flag_.inclusive_start());
        databuff_printf(
            buf, BUF_LEN, pos, "end_border_flag[%ld] = %d\n", i, ranges.at(i)->border_flag_.inclusive_end());
      }
      databuff_printf(buf, BUF_LEN, pos, "count of rang columns = %ld\n", range_columns.count());
      databuff_printf(buf, BUF_LEN, pos, to_cstring(ranges));
      databuff_printf(buf, BUF_LEN, pos, "\n");
      json_expr = buf;

      OK(query_range.get_tablet_ranges(allocator_, params, ranges, flag, NULL));
      query_range.reset();
    }
  }
}

/*
 * there set 10 columns a-j. if need more, please modify SetUp()
 * in case file, only support integer, if have '?', integer must be placed in the back of '?'
 * such as  (a, b, c) < (?1, ?2, ?3)
 */
TEST_F(ObQueryRangeTest, query_test)
{
  static const char* test_file = "./test_query_range.test";
  static const char* tmp_file = "./test_query_range.tmp";
  static const char* result_file = "./test_query_range.result";

  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::string line;
  const char* json_expr = NULL;
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  int64_t case_id = 0;
  while (std::getline(if_tests, line)) {
    of_result << '[' << case_id++ << "] " << line << std::endl;
    const char* line_ptr = line.c_str();
    while (*line_ptr == ' ' && *line_ptr != '\0') {
      ++line_ptr;
    }
    if (*line_ptr != '\0' && *line_ptr == '#') {
      continue;
    }
    get_query_range(line_ptr, json_expr);
    of_result << json_expr << std::endl;
  }
  of_result.close();
  // verify results
  fprintf(stderr, "If tests failed, use `diff %s %s' to see the differences. \n", result_file, tmp_file);
  std::ifstream if_result(tmp_file);
  ASSERT_TRUE(if_result.is_open());
  std::istream_iterator<std::string> it_result(if_result);
  std::ifstream if_expected(result_file);
  ASSERT_TRUE(if_expected.is_open());
  std::istream_iterator<std::string> it_expected(if_expected);
  ASSERT_TRUE(std::equal(it_result, std::istream_iterator<std::string>(), it_expected));
  std::remove(tmp_file);
}

TEST_F(ObQueryRangeTest, collation_test)
{
  static const char* test_file = "./test_query_range_collation.test";
  static const char* tmp_file = "./test_query_range_collation.tmp";
  static const char* result_file = "./test_query_range_collation.result";

  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::string line;
  const char* json_expr = NULL;
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  int64_t case_id = 0;
  while (std::getline(if_tests, line)) {
    of_result << '[' << case_id++ << "] " << line << std::endl;
    const char* line_ptr = line.c_str();
    while (*line_ptr == ' ' && *line_ptr != '\0') {
      ++line_ptr;
    }
    if (*line_ptr != '\0' && *line_ptr == '#') {
      continue;
    }
    char buf[BUF_LEN];
    int64_t pos = 0;
    get_query_range_collation(line_ptr, json_expr, buf, pos);
    of_result << json_expr << std::endl;
  }
  of_result.close();
  // verify results
  fprintf(stderr, "If tests failed, use `diff %s %s' to see the differences. \n", result_file, tmp_file);
  std::ifstream if_result(tmp_file);
  ASSERT_TRUE(if_result.is_open());
  std::istream_iterator<std::string> it_result(if_result);
  std::ifstream if_expected(result_file);
  ASSERT_TRUE(if_expected.is_open());
  std::istream_iterator<std::string> it_expected(if_expected);
  ASSERT_TRUE(std::equal(it_result, std::istream_iterator<std::string>(), it_expected));
  std::remove(tmp_file);
}

TEST_F(ObQueryRangeTest, filter_test)
{
  static const char* test_file = "./test_query_range_filter.test";
  static const char* tmp_file = "./test_query_range_filter.tmp";
  static const char* result_file = "./test_query_range_filter.result";

  std::ifstream if_tests(test_file);
  ASSERT_TRUE(if_tests.is_open());
  std::string line;
  const char* json_expr = NULL;
  std::ofstream of_result(tmp_file);
  ASSERT_TRUE(of_result.is_open());
  int64_t case_id = 0;
  while (std::getline(if_tests, line)) {
    const char* line_ptr = line.c_str();
    if (*line_ptr != '\0' && *line_ptr == '#') {
      continue;
    }
    of_result << '[' << case_id++ << "] " << line << std::endl;
    while (*line_ptr == ' ' && *line_ptr != '\0') {
      ++line_ptr;
    }
    char buf[BUF_LEN];
    int64_t pos = 0;
    get_query_range_filter(line_ptr, json_expr, buf, pos, 1);
    get_query_range_filter(line_ptr, json_expr, buf, pos, 2);
    get_query_range_filter(line_ptr, json_expr, buf, pos, 3);
    of_result << json_expr << std::endl;
  }
  of_result.close();
  // verify results
  fprintf(stderr, "If tests failed, use `diff %s %s' to see the differences. \n", result_file, tmp_file);
  std::ifstream if_result(tmp_file);
  ASSERT_TRUE(if_result.is_open());
  std::istream_iterator<std::string> it_result(if_result);
  std::ifstream if_expected(result_file);
  ASSERT_TRUE(if_expected.is_open());
  std::istream_iterator<std::string> it_expected(if_expected);
  ASSERT_TRUE(std::equal(it_result, std::istream_iterator<std::string>(), it_expected));
  std::remove(tmp_file);
}

TEST_F(ObQueryRangeTest, test_collation_type)
{
  _OB_LOG(INFO, "test collation type");
  ObQueryRangeArray ranges;
  ObGetMethodArray get_methods;
  ObQueryRange query_range;

  params_.reset();
  for (int i = CS_TYPE_UTF8MB4_GENERAL_CI; i < CS_TYPE_MAX; i++) {}
}
TEST_F(ObQueryRangeTest, single_filed_key_whole_range1)
{
  _OB_LOG(INFO, "single filed key and NULL condition");
  ObQueryRangeArray ranges;
  ObGetMethodArray get_methods;
  ObQueryRange query_range;
  const ObDataTypeCastParams dtc_params;

  params_.reset();
  OK(query_range.preliminary_extract_query_range(single_range_columns_, NULL, NULL));
  OK(query_range.final_extract_query_range(params_, NULL));
  OK(query_range.get_tablet_ranges(ranges, get_methods, dtc_params));
  ASSERT_EQ(0, strcmp(to_cstring(ranges), "[{\"range\":\"table_id:3003,(MIN;MAX)\"}]"));
}

TEST_F(ObQueryRangeTest, single_filed_key_whole_range2)
{
  _OB_LOG(INFO, "single filed key and empty condition array");
  ObQueryRangeArray ranges;
  ObGetMethodArray get_methods;
  ObQueryRange query_range;
  ObArray<ObRawExpr*> exprs;
  const ObDataTypeCastParams dtc_params;

  params_.reset();
  OK(query_range.preliminary_extract_query_range(single_range_columns_, exprs, dtc_params));
  OK(query_range.final_extract_query_range(params_, dtc_params));
  OK(query_range.get_tablet_ranges(ranges, get_methods, dtc_params));
  ASSERT_EQ(0, strcmp(to_cstring(ranges), "[{\"range\":\"table_id:3003,(MIN;MAX)\"}]"));
}

TEST_F(ObQueryRangeTest, double_filed_key_whole_range1)
{
  _OB_LOG(INFO, "double filed key and NULL condition");
  ObQueryRangeArray ranges;
  ObGetMethodArray get_methods;
  ObQueryRange query_range;
  const ObDataTypeCastParams dtc_params;

  params_.reset();
  OK(query_range.preliminary_extract_query_range(double_range_columns_, NULL, dtc_params));
  OK(query_range.final_extract_query_range(params_, dtc_params));
  OK(query_range.get_tablet_ranges(ranges, get_methods, dtc_params));
  ASSERT_EQ(0, strcmp(to_cstring(ranges), "[{\"range\":\"table_id:3003,(MIN,MIN;MAX,MAX)\"}]"));
}

TEST_F(ObQueryRangeTest, double_filed_key_whole_range2)
{
  _OB_LOG(INFO, "double filed key and empty condition array");
  ObQueryRangeArray ranges;
  ObGetMethodArray get_methods;
  ObQueryRange query_range;
  ObArray<ObRawExpr*> exprs;
  const ObDataTypeCastParams dtc_params;

  params_.reset();
  OK(query_range.preliminary_extract_query_range(double_range_columns_, exprs, dtc_params));
  OK(query_range.final_extract_query_range(params_, dtc_params));
  OK(query_range.get_tablet_ranges(ranges, get_methods, dtc_params));
  ASSERT_EQ(0, strcmp(to_cstring(ranges), "[{\"range\":\"table_id:3003,(MIN,MIN;MAX,MAX)\"}]"));
}

TEST_F(ObQueryRangeTest, range_column_with_like)
{
  _OB_LOG(INFO, "f like 'abc%s'", "%");
  ObArray<ColumnItem> single_range_columns;
  ObQueryRangeArray ranges;
  ObGetMethodArray get_methods;
  ObQueryRange query_range;
  ObRawExpr* expr = NULL;
  const ObDataTypeCastParams dtc_params;

  params_.reset();
  ASSERT_EQ(OB_INVALID_ARGUMENT, query_range.preliminary_extract_query_range(single_range_columns, expr, dtc_params));
  resolve_expr(
      "f like 'abc%'", expr, single_range_columns, params_, CS_TYPE_UTF8MB4_GENERAL_CI, CS_TYPE_UTF8MB4_GENERAL_CI);
  OK(query_range.preliminary_extract_query_range(single_range_columns, expr, dtc_params));
  OK(query_range.final_extract_query_range(params_, dtc_params));
  OK(query_range.get_tablet_ranges(ranges, get_methods, dtc_params));
  _OB_LOG(INFO, "range: %s", to_cstring(ranges));

  query_range.reset();
  ObConstRawExpr* escape_expr = dynamic_cast<ObConstRawExpr*>(expr->get_param_expr(2));
  ObObj escape_obj;
  escape_obj.set_null();
  escape_expr->set_value(escape_obj);
  OK(query_range.preliminary_extract_query_range(single_range_columns, expr, dtc_params));
  OK(query_range.final_extract_query_range(params_, dtc_params));
  OK(query_range.get_tablet_ranges(ranges, get_methods, dtc_params));
  _OB_LOG(INFO, "range: %s", to_cstring(ranges));

  query_range.reset();
  resolve_condition(single_range_columns, "'a' like 'a'", expr);
  OK(query_range.preliminary_extract_query_range(single_range_columns, expr, dtc_params));
  OK(query_range.final_extract_query_range(params_, dtc_params));
  OK(query_range.get_tablet_ranges(ranges, get_methods, dtc_params));
  _OB_LOG(INFO, "range: %s", to_cstring(ranges));
}

TEST_F(ObQueryRangeTest, range_column_with_like_prepare)
{
  _OB_LOG(INFO, "f like ?");
  ObArray<ColumnItem> single_range_columns;
  ObQueryRangeArray ranges;
  ObGetMethodArray get_methods;
  ObQueryRange query_range;
  ObRawExpr* expr = NULL;
  const ObDataTypeCastParams dtc_params;
  params_.reset();
  ObObjParam param;
  param.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  param.set_varchar("abc%", static_cast<int32_t>(strlen("abc%")));
  param.set_param_meta();
  OK(params_.push_back(param));
  resolve_expr("a like ?", expr, single_range_columns, params_, CS_TYPE_UTF8MB4_GENERAL_CI, CS_TYPE_UTF8MB4_GENERAL_CI);
  OK(query_range.preliminary_extract_query_range(single_range_columns, expr, dtc_params));
  OK(query_range.final_extract_query_range(params_, dtc_params));
  OK(query_range.get_tablet_ranges(ranges, get_methods, dtc_params));
  _OB_LOG(INFO, "range: %s", to_cstring(ranges));
}

TEST_F(ObQueryRangeTest, range_column_with_triple_key)
{
  char in_body[102400] = {'\0'};
  char sql_str[102400] = {'\0'};
  for (int64_t i = 0; i < 199; ++i) {
    strcat(in_body, "(?, ?, ?),");
  }
  strcat(in_body, "(?, ?, ?)");
  snprintf(sql_str, sizeof(sql_str), "(a, b, c) in (%s)", in_body);
  _OB_LOG(INFO, "%s", sql_str);

  ObQueryRangeArray ranges;
  ObGetMethodArray get_methods;
  ObQueryRange query_range;
  ObRawExpr* expr = NULL;
  int64_t param_array[600];
  for (int64_t i = 0; i < 600; ++i) {
    if (0 == i % 3) {
      param_array[i] = 1003809986;
    } else if (1 == i % 3) {
      param_array[i] = 1;
    } else {
      param_array[i] = 3732197360 + i;
    }
  }
  params_.reset();
  for (int64_t i = 0; i < 600; ++i) {
    ObObjParam param;
    param.set_int(param_array[i]);
    param.set_param_meta();
    OK(params_.push_back(param));
  }
  const ObDataTypeCastParams dtc_params;
  resolve_condition(triple_range_columns_, sql_str, expr, &params_);
  int64_t time1 = ObTimeUtility::current_time();
  OK(query_range.preliminary_extract_query_range(triple_range_columns_, expr, dtc_params));
  int64_t time2 = ObTimeUtility::current_time();
  OK(query_range.final_extract_query_range(params_, dtc_params));
  int64_t time3 = ObTimeUtility::current_time();
  OK(query_range.get_tablet_ranges(ranges, get_methods, dtc_params));
  _OB_LOG(INFO, "preliminary_extract_query_range(us): %ld", time2 - time1);
  _OB_LOG(INFO, "final_extract_query_range(us): %ld", time3 - time2);
  //_OB_LOG(INFO, "ranges: %s", to_cstring(ranges));
}

TEST_F(ObQueryRangeTest, simple_row_in)
{
  _OB_LOG(INFO, "start test: (a, d) in ((1 , 1.5), (2, 2), (1, 3), (2, 3), (0, 0), ('3', 3))");
  params_.reset();
  ObObjParam param;
  param.set_int(1);
  param.set_param_meta();
  OK(params_.push_back(param));
  param.set_double(1.5);
  param.set_param_meta();
  OK(params_.push_back(param));
  //  param.set_int(2);
  //  OK(params_.push_back(param));
  //  param.set_int(2);
  //  OK(params_.push_back(param));
  //  param.set_int(1);
  //  OK(params_.push_back(param));
  //  param.set_int(3);
  //  OK(params_.push_back(param));
  //  param.set_int(2);
  //  OK(params_.push_back(param));
  //  param.set_int(3);
  //  OK(params_.push_back(param));
  //  param.set_int(0);
  //  OK(params_.push_back(param));
  //  param.set_int(0);
  //  OK(params_.push_back(param));
  param.set_varchar("3");
  param.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  param.set_param_meta();
  OK(params_.push_back(param));
  param.set_int(3);
  param.set_param_meta();
  OK(params_.push_back(param));
  ObRawExpr* condition = NULL;
  const ObDataTypeCastParams dtc_params;

  resolve_condition(triple_range_columns_, "(a, d) in ((? , ?)) or (a, d) = (?, ?)", condition, &params_);
  OK(query_range.preliminary_extract_query_range(triple_range_columns_, condition, dtc_params));

  _OB_LOG(INFO, "XXXX %s", to_cstring(query_range));
  _OB_LOG(INFO, "XXXX params_: %s", to_cstring(params_));
  OK(query_range.final_extract_query_range(params_, dtc_params));
  _OB_LOG(INFO, "XXXX final: %s", to_cstring(query_range));

  ObQueryRangeArray ranges;
  ObGetMethodArray get_methods;

  OK(query_range.get_tablet_ranges(ranges, get_methods, dtc_params));
  _OB_LOG(INFO, "ranges: %s", to_cstring(ranges));
  //  ASSERT_EQ(1, ranges.count());
  //  int64_t value = 0;
  //
  //  EXPECT_TRUE(get_methods.at(0));
  //
  //  // expect: ((1, 1, 1), (1, 1, 2))
  //  EXPECT_FALSE(ranges.at(0)->border_flag_.inclusive_start());
  //  EXPECT_FALSE(ranges.at(0)->border_flag_.inclusive_end());
  //  ASSERT_EQ(1, ranges.count());
  //  ASSERT_EQ(ranges.at(0)->start_key_.get_obj_ptr()[0].get_int(value), OB_SUCCESS);
  //  ASSERT_EQ(1, value);
  //  ASSERT_EQ(ranges.at(0)->start_key_.get_obj_ptr()[1].get_int(value), OB_SUCCESS);
  //  ASSERT_EQ(1, value);
  //  ASSERT_EQ(ranges.at(0)->start_key_.get_obj_ptr()[2].get_int(value), OB_SUCCESS);
  //  ASSERT_EQ(1, value);
  //
  //  ASSERT_EQ(ranges.at(0)->end_key_.get_obj_ptr()[0].get_int(value), OB_SUCCESS);
  //  ASSERT_EQ(1, value);
  //  ASSERT_EQ(ranges.at(0)->end_key_.get_obj_ptr()[1].get_int(value), OB_SUCCESS);
  //  ASSERT_EQ(1, value);
  //  ASSERT_EQ(ranges.at(0)->end_key_.get_obj_ptr()[2].get_int(value), OB_SUCCESS);
  //  ASSERT_EQ(2, value);
}

TEST_F(ObQueryRangeTest, basic_test)
{
  _OB_LOG(INFO, "start test: ((b = 6 and a < 5.5) or (a > 8 and b = 15))=> (a < 5 and b = 6) or (a > 8 and b = 15)");
  params_.reset();
  ObGetMethodArray get_methods;
  OK(get_methods.push_back(false));
  OK(get_methods.push_back(false));
  except_result(double_range_columns_,
      params_,
      "(b = 6 and a < 5) or (a > 8 and b = 15)",
      "[{\"range\":\"table_id:3003,({\"NULL\":\"NULL\"},MAX;{\"BIGINT\":5},MIN)\"}, "
      "{\"range\":\"table_id:3003,({\"BIGINT\":8},MAX;MAX,{\"BIGINT\":15})\"}]",
      get_methods);
  query_range.reset();
}

TEST_F(ObQueryRangeTest, single_key_cost_time)
{
  _OB_LOG(INFO, "start test: a=?(500)");
  params_.reset();

  const char* condition = "a = ?";
  ObRawExpr* expr = NULL;
  ObObjParam param;
  param.set_int(500);
  param.set_param_meta();
  OK(params_.push_back(param));
  resolve_condition(single_range_columns_, condition, expr, &params_);
  const ObDataTypeCastParams dtc_params;

  ObQueryRange pre_query_range;
  OK(pre_query_range.preliminary_extract_query_range(single_range_columns_, expr, dtc_params));

  int64_t deep_copy_cost = 0;
  int64_t extract_cost = 0;
  int64_t get_range_cost = 0;
  for (int64_t i = 0; i < 1000; ++i) {
    ObQueryRange final_query_range;
    ObQueryRangeArray ranges;
    ObGetMethodArray get_methods;
    int64_t deep_copy_beg = ObTimeUtility::current_time();
    OK(final_query_range.deep_copy(pre_query_range));
    int64_t deep_copy_end = ObTimeUtility::current_time();
    deep_copy_cost += deep_copy_end - deep_copy_beg;
    OK(final_query_range.final_extract_query_range(params_, dtc_params));
    int64_t extract_end = ObTimeUtility::current_time();
    extract_cost += extract_end - deep_copy_end;
    OK(final_query_range.get_tablet_ranges(ranges, get_methods, dtc_params));
    int64_t get_range_end = ObTimeUtility::current_time();
    get_range_cost += get_range_end - extract_end;
  }
  _OB_LOG(INFO,
      "deep_copy_cost: %f, extract_cost: %f, get_range_cost: %f",
      (float)deep_copy_cost / (float)1000,
      (float)extract_cost / (float)1000,
      (float)get_range_cost / (float)1000);

  ObQueryRange query_range2;
  OK(query_range2.preliminary_extract_query_range(single_range_columns_, expr, dtc_params));
  get_range_cost = 0;
  for (int64_t i = 0; i < 1000; ++i) {
    ObQueryRangeArray ranges;
    ObGetMethodArray get_methods;
    int64_t get_range_beg = ObTimeUtility::current_time();
    ASSERT_TRUE(false == query_range2.need_deep_copy());
    OK(query_range2.get_tablet_ranges(allocator_, params_, ranges, get_methods, dtc_params));
    get_range_cost += ObTimeUtility::current_time() - get_range_beg;
  }
  _OB_LOG(INFO, "get range without deep copy cost time: %f", (float)get_range_cost / (float)1000);
  ObQueryRangeArray ranges2;
  ObGetMethodArray get_methods2;
  ASSERT_TRUE(true == query_range2.is_precise_get());
  OK(query_range2.get_tablet_ranges(allocator_, params_, ranges2, get_methods2, dtc_params));
  OB_LOG(INFO, "query range pure get", K(ranges2), K(get_methods2));

  int64_t REPEAT_TIMES = 1000000;
  int64_t ti = 0;
  int64_t get_time_start = ObTimeUtility::current_time();
  for (int64_t i = 0; i < REPEAT_TIMES; ++i) {
    ti = ObTimeUtility::current_time();
    UNUSED(ti);
  }
  int64_t time_used = ObTimeUtility::current_time() - get_time_start;
  OB_LOG(INFO, "system current time cost", K(time_used), K(ti));
  OK(ObClockGenerator::init());
  get_time_start = ObTimeUtility::current_time();
  for (int64_t i = 0; i < REPEAT_TIMES; ++i) {
    ti = ObClockGenerator::getClock();
    UNUSED(ti);
  }
  time_used = ObTimeUtility::current_time() - get_time_start;
  OB_LOG(INFO, "ClockGenerator current time cost", K(time_used), K(ti));
}

// TEST_F(ObQueryRangeTest, insert_test)
//{
//  ObQueryRangeArray ranges;
//  ObGetMethodArray get_methods;
//
//  ObArray<ObRawExpr*> exprs;
//  ObArray<ObObjParam> params;
//  ObRawExpr *const_expr1 = NULL;
//  ObRawExpr *const_expr2 = NULL;
//  ObRawExpr *const_expr3 = NULL;
//  ObRawExpr *const_expr4 = NULL;
//  resolve_condition(double_range_columns_, "1", const_expr1);
//  resolve_condition(double_range_columns_, "2", const_expr2);
//  resolve_condition(double_range_columns_, "3", const_expr3);
//  resolve_condition(double_range_columns_, "4", const_expr4);
//  exprs.push_back(const_expr1);
//  exprs.push_back(const_expr2);
//  exprs.push_back(const_expr3);
//  exprs.push_back(const_expr4);
//  query_range.extract_query_range(double_range_columns_, double_range_columns_, exprs, params, NULL);
//  bool flag = true;
//  OK(query_range.get_tablet_ranges(double_range_columns_, ranges, flag, NULL));
//  _OB_LOG(INFO, "insert_query_ranges----------%s", to_cstring(ranges));
//  ObQueryRange copy_range(query_range);
//  OK(copy_range.get_tablet_ranges(double_range_columns_, ranges, flag, NULL));
//  _OB_LOG(INFO, "copy_query_ranges------------%s", to_cstring(ranges));
//  OK(query_range.all_single_value_ranges(double_range_columns_, flag, NULL));
//  ASSERT_EQ(true, flag);
//  query_range.reset();
//  ObArray<ObObjParam> question_params;
//  ObObjParam obj;
//  obj.set_int(1);
//  question_params.push_back(obj);
//  obj.set_int(2);
//  question_params.push_back(obj);
//  obj.set_int(3);
//  question_params.push_back(obj);
//  obj.set_int(4);
//  question_params.push_back(obj);
//  resolve_condition(double_range_columns_, "?", const_expr1, &question_params);
//  resolve_condition(double_range_columns_, "?", const_expr2, &question_params);
//  resolve_condition(double_range_columns_, "?", const_expr3, &question_params);
//  resolve_condition(double_range_columns_, "?", const_expr4, &question_params);
//  ObArray<ObRawExpr*> question_exprs;
//  question_exprs.push_back(const_expr1);
//  question_exprs.push_back(const_expr2);
//  question_exprs.push_back(const_expr3);
//  question_exprs.push_back(const_expr4);
//  query_range.extract_query_range(double_range_columns_, double_range_columns_, question_exprs, question_params,
//  NULL); OK(query_range.get_tablet_ranges(double_range_columns_, ranges, NULL)); _OB_LOG(INFO,
//  "insert_query_ranges----------%s", to_cstring(ranges));
//}

int main(int argc, char** argv)
{
  init_sql_factories();
  OB_LOGGER.set_log_level("TRACE");
  int ret = 0;
  ContextParam param;
  param.set_mem_attr(1001, 0, ObCtxIds::WORK_AREA).set_page_size(OB_MALLOC_BIG_BLOCK_SIZE);

  ::testing::InitGoogleTest(&argc, argv);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    ret = RUN_ALL_TESTS();
  }
  return ret;
}
