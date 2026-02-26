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

#define USING_LOG_PREFIX RS
#include <gtest/gtest.h>
#define  private public
#define  protected public

#include "storage/ls/ob_ls.h"
#include "src/rootserver/ob_ls_recovery_stat_handler.h"
#include "env/ob_simple_cluster_test_base.h"

#define ASSERT_SUCCESS(x) ASSERT_EQ((x), OB_SUCCESS)

namespace oceanbase
{
namespace rootserver
{
class TestLSRecoveryStatHandler : public ObLSRecoveryStatHandler, public unittest::ObSimpleClusterTestBase
{
public:
  TestLSRecoveryStatHandler() : unittest::ObSimpleClusterTestBase("test_ls_recovery_stat_handler") {
    tenant_id_ = OB_SYS_TENANT_ID;
  }
  int push_scn_to_array(ObIArray<SCN> &scns, int64_t ts) {
    int ret = OB_SUCCESS;
    SCN tmp;
    tmp.convert_from_ts(ts);
    return scns.push_back(tmp);
  }
  template<typename ...Args>
  int push_scn_to_array(ObIArray<SCN> &scns, int64_t ts, Args... args) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(push_scn_to_array(scns, ts))) {
      LOG_WARN("push_scn_to_array failed", KR(ret), K(ts));
    } else if (OB_FAIL(push_scn_to_array(scns, args...))) {
      LOG_WARN("push_scn_to_array failed", KR(ret));
    }
    return ret;
  }
  template<typename ...Args>
  int get_scn_array(ObArray<SCN> &scns, Args... args) {
    int ret = OB_SUCCESS;
    scns.reset();
    if (OB_FAIL(push_scn_to_array(scns, args...))) {
      LOG_WARN("failed to push scn to array", KR(ret));
    } else {
      lib::ob_sort(scns.begin(), scns.end());
    }
    return ret;
  }
};
TEST_F(TestLSRecoveryStatHandler, calc_majority)
{
  ObArray<SCN> array;
  SCN tmp;
  ASSERT_SUCCESS(get_scn_array(array, 1_s));
  ASSERT_SUCCESS(do_calc_majority_min_readable_scn_(1 /*majority_cnt*/, array, tmp));
  ASSERT_EQ(tmp.convert_to_ts(), 1_s);

  ASSERT_SUCCESS(get_scn_array(array, 1_s, 2_s));
  ASSERT_SUCCESS(do_calc_majority_min_readable_scn_(2 /*majority_cnt*/, array, tmp));
  ASSERT_EQ(tmp.convert_to_ts(), 1_s);

  ASSERT_SUCCESS(get_scn_array(array, 1_s, 2_s, 3_s));
  ASSERT_SUCCESS(do_calc_majority_min_readable_scn_(2 /*majority_cnt*/, array, tmp));
  ASSERT_EQ(tmp.convert_to_ts(), 2_s);
}

TEST_F(TestLSRecoveryStatHandler, calc_all)
{
  ObArray<SCN> array;
  SCN tmp;
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  ASSERT_SUCCESS(sql.assign_fmt("alter system set _standby_readable_scn_all = True tenant = sys"));
  ASSERT_SUCCESS(sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_SUCCESS(sql.assign_fmt("alter system set max_stale_time_for_weak_consistency = '5s' tenant = sys"));
  ASSERT_SUCCESS(sql_proxy.write(sql.ptr(), affected_rows));
  ASSERT_SUCCESS(get_scn_array(array, 1_s));
  ASSERT_SUCCESS(do_calc_majority_min_readable_scn_(1 /*majority_cnt*/, array, tmp));
  ASSERT_EQ(tmp.convert_to_ts(), 1_s);

  ASSERT_SUCCESS(get_scn_array(array, 1_s, 2_s));
  ASSERT_SUCCESS(do_calc_majority_min_readable_scn_(2 /*majority_cnt*/, array, tmp));
  ASSERT_EQ(tmp.convert_to_ts(), 1_s);

  ASSERT_SUCCESS(get_scn_array(array, 1_s, 2_s, 3_s));
  ASSERT_SUCCESS(do_calc_majority_min_readable_scn_(2 /*majority_cnt*/, array, tmp));
  ASSERT_EQ(tmp.convert_to_ts(), 1_s);

  ASSERT_SUCCESS(get_scn_array(array, 15_s, 20_s, 30_s));
  ASSERT_SUCCESS(do_calc_majority_min_readable_scn_(2 /*majority_cnt*/, array, tmp));
  ASSERT_EQ(tmp.convert_to_ts(), 20_s);

  ASSERT_SUCCESS(get_scn_array(array, 14_s, 20_s, 30_s));
  ASSERT_SUCCESS(do_calc_majority_min_readable_scn_(2 /*majority_cnt*/, array, tmp));
  ASSERT_EQ(tmp.convert_to_ts(), 20_s);
}
}
}
int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
