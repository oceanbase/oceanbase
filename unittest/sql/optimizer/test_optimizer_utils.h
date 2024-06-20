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

#ifndef _TEST_OPTIMIZER_UTILS_H
#define _TEST_OPTIMIZER_UTILS_H 1

#undef private
#undef protected
#include <iostream>
#include <sys/time.h>
#include <iterator>
#define private public
#define protected public
#include "ob_mock_partition_service.h"
//#include "ob_mock_stat_manager.h"
#include "ob_mock_opt_stat_manager.h"
#include "../test_sql_utils.h"
#include "sql/ob_sql_init.h"
#include "sql/optimizer/ob_log_sort.h"
#include "sql/optimizer/ob_log_group_by.h"
#include "sql/optimizer/ob_log_exchange.h"
#include "sql/optimizer/ob_join_order.h"
#include "sql/optimizer/ob_log_limit.h"
#include "sql/optimizer/ob_table_location.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/ob_sql_context.h"
#include "sql/ob_sql.h"
#include "sql/rewrite/ob_transformer_impl.h"
#include "lib/json/ob_json.h"
#include "sql/optimizer/ob_log_join.h"
#include "sql/optimizer/ob_log_table_scan.h"

#define ObCacheObjectFactory test::MockCacheObjectFactory

#define BUF_LEN 102400 // 100K
using std::cout;
using namespace oceanbase::json;
using oceanbase::sql::ObTableLocation;
namespace test
{

class MockCacheObjectFactory {
public:
  static int alloc(ObILibCacheObject *&cache_obj, ObLibCacheNameSpace ns,
                   uint64_t tenant_id = common::OB_SERVER_TENANT_ID);
  static int alloc(ObPhysicalPlan *&plan,
                   uint64_t tenant_id = common::OB_SERVER_TENANT_ID);
  static int alloc(pl::ObPLFunction *&func, ObLibCacheNameSpace ns,
                   uint64_t tenant_id = common::OB_SERVER_TENANT_ID);
  static int alloc(pl::ObPLPackage *&package,
                   uint64_t tenant_id = common::OB_SERVER_TENANT_ID);
  static void free(ObILibCacheObject *cache_obj);

private:
  static void inner_free(ObILibCacheObject *);
};

class TestOptimizerUtils : public TestSqlUtils, public ::testing::Test {
 public:
  TestOptimizerUtils();
  virtual ~TestOptimizerUtils();
  virtual void init();
  virtual void SetUp();
  virtual void TearDown() {destroy();}
  virtual void destroy() {}
  // function members
  int generate_logical_plan(ObResultSet &result,//ObIAllocator &allocator,
                            ObString &stmt,
                            const char *query_str,
                            ObLogPlan *&logical_plan, bool &is_select,
                            bool parameterized = true);
  void explain_plan_json(ObLogPlan *plan, std::ofstream &of_result);
  void compile_sql(const char* query_str,
                   std::ofstream &of_result);
  void run_test(const char* test_file,
                const char* result_file,
                const char* tmp_file,
                bool default_stat_est = false);
  void run_fail_test(const char* test_file);
  void run_fail_test_ret_as(const char* test_file, int ret_val);
  void formalize_tmp_file(const char *tmp_file);

  void init_histogram(
    common::ObIAllocator &allocator,
    const ObHistType type,
    const double sample_size,
    const double density,
    const common::ObIArray<int64_t> &repeat_count,
    const common::ObIArray<int64_t> &value,
    const common::ObIArray<int64_t> &num_elements,
    ObHistogram &hist);

  inline static int64_t get_usec()
  {
    struct timeval time_val;
    gettimeofday(&time_val, NULL);
    return time_val.tv_sec*1000000 + time_val.tv_usec;
  }
  int convert_rowkey_type(common::ObIArray<ObObj> &rowkey, const common::ObRowkeyInfo &rowkey_info, const ObTimeZoneInfo *tz_info)
  {
    int ret = OB_SUCCESS;
    ObRowkeyColumn rowkey_col;
    if (OB_UNLIKELY(rowkey.count() != rowkey_info.get_size())) {
      ret = OB_ERR_UNEXPECTED;
      SQL_OPT_LOG(WARN, "rowkey is invalid", K(rowkey_info), K(rowkey));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey.count(); ++i) {
      if (rowkey.at(i).is_null()) {
        //do nothing
      } else if (OB_FAIL(rowkey_info.get_column(i, rowkey_col))) {
        SQL_OPT_LOG(WARN, "get column of rowkey info failed", K(ret), K(i), K(rowkey_info));
      } else {
        const ObDataTypeCastParams dtc_params(tz_info);
        ObCastCtx cast_ctx(&allocator_, &dtc_params, CM_WARN_ON_FAIL, rowkey_col.get_meta_type().get_collation_type());
        ObObj &tmp_start = rowkey.at(i);
        const ObObj *dest_val = NULL;
        EXPR_CAST_OBJ_V2(rowkey_col.get_meta_type().get_type(), tmp_start, dest_val);
        if (OB_SUCC(ret)) {
          rowkey.at(i) = *dest_val;
        }
      }
    }
    return ret;
  }
  int convert_row_array_to_rowkeys(ObIArray<ObSEArray<ObObj, 3> > &row_array,
                                   ObIArray<ObRowkey> &rowkeys,
                                   const common::ObRowkeyInfo &rowkey_info,
                                   const ObTimeZoneInfo *tz_info)
  {
    int ret = OB_SUCCESS;
    ARRAY_FOREACH(row_array, idx) {
      if (OB_FAIL(convert_rowkey_type(row_array.at(idx), rowkey_info, tz_info))) {
        SQL_OPT_LOG(WARN, "convert rowkey type failed", K(ret));
      } else {
        ObRowkey rowkey(&row_array.at(idx).at(0), row_array.at(idx).count());
        if (OB_FAIL(rowkeys.push_back(rowkey))) {
          SQL_OPT_LOG(WARN, "store rowkey failed", K(ret));
        }
      }
    }
    return ret;
  }
protected:
  int64_t case_id_;
  ObOptimizerContext *optctx_;
  bool is_json_;
  ExplainType explain_type_;
  //::test::MockStatManager stat_manager_;
  ::test::MockOptStatManager opt_stat_manager_;
  //::test::MockTableStatService ts_;
  //::test::MockColumnStatService cs_;
  // ::test::MockOptTableStatService opt_ts_;
  // ::test::MockOptColumnStatService opt_cs_;
  ::test::MockOptStatService opt_stat_;
//  ::test::MockPartitionService partition_service_;
  // data members
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestOptimizerUtils);
  // function members
};
}

#endif /* _TEST_OPTIMIZER_UTILS_H */
