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

#include "ob_simple_cluster_test_base.h"
#include "ob_simple_server_restart_helper.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_defer.h"
#include "logservice/palf/election/utils/election_common_define.h"
#define private public
#define protected public
#include "share/config/ob_server_config.h"
#undef private
#undef protected

namespace oceanbase
{
namespace unittest
{

int set_trace_id(char *buf)
{
  return ObCurTraceId::get_trace_id()->set(buf);
}


void init_log_and_gtest(int argc, char **argv)
{
  if (argc < 1) {
    abort();
  }

  std::string app_name = argv[0];
  app_name = app_name.substr(app_name.find_last_of("/\\") + 1);
  std::string app_log_name = app_name + ".log";
  std::string app_rs_log_name = app_name + "_rs.log";
  std::string app_ele_log_name = app_name + "_election.log";
  std::string app_gtest_log_name = app_name + "_gtest.log";
  std::string app_trace_log_name = app_name + "_trace.log";

  system(("rm -rf " + app_log_name + "*").c_str());
  system(("rm -rf " + app_rs_log_name + "*").c_str());
  system(("rm -rf " + app_ele_log_name + "*").c_str());
  system(("rm -rf " + app_gtest_log_name + "*").c_str());
  system(("rm -rf " + app_trace_log_name + "*").c_str());
  system(("rm -rf " + app_name + "_*").c_str());

  init_gtest_output(app_gtest_log_name);
  OB_LOGGER.set_file_name(app_log_name.c_str(), true, false, app_rs_log_name.c_str(), app_ele_log_name.c_str(), app_trace_log_name.c_str());


}

void init_gtest_output(std::string &gtest_log_name)
{
  // 判断是否处于Farm中
  char *mit_network_start_port_env = getenv("mit_network_start_port");
  char *mit_network_port_num_env = getenv("mit_network_port_num");
  if (mit_network_start_port_env != nullptr && mit_network_port_num_env != nullptr) {
    std::string gtest_file_name = gtest_log_name;
    int fd = open(gtest_file_name.c_str(), O_RDWR|O_CREAT, 0666);
    if (fd == 0) {
      ob_abort();
    }
    dup2(fd, STDOUT_FILENO);
    dup2(fd, STDERR_FILENO);
  }
}


std::shared_ptr<observer::ObSimpleServer> ObSimpleClusterTestBase::cluster_ = nullptr;
bool ObSimpleClusterTestBase::is_started_ = false;
std::string ObSimpleClusterTestBase::env_prefix_;
std::string ObSimpleClusterTestBase::curr_dir_;
bool ObSimpleClusterTestBase::enable_env_warn_log_ = false;
const char *ObSimpleClusterTestBase::UNIT_BASE ="box_ym_";
const char *ObSimpleClusterTestBase::POOL_BASE ="pool_ym_";

ObSimpleClusterTestBase::ObSimpleClusterTestBase(const std::string &env_prefix,
                                                 const char *log_disk_size,
                                                 const char *memory_limit)
{
  if (cluster_ == nullptr) {
    env_prefix_ = env_prefix + "_test_data"; //+ std::to_string(ObTimeUtility::current_time()) + "_";
    cluster_ = std::make_shared<observer::ObSimpleServer>(env_prefix_,
                                                          log_disk_size,
                                                          memory_limit);
    curr_dir_ = get_current_dir_name();
  }
}

ObSimpleClusterTestBase::~ObSimpleClusterTestBase()
{
}

void ObSimpleClusterTestBase::SetUp()
{
  auto case_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
  SERVER_LOG(INFO, "SetUp>>>>>>>>>>>>>>", K(case_name));
  int ret = OB_SUCCESS;
  if (!is_started_) {
    if (OB_FAIL(start())) {
      SERVER_LOG(WARN, "start simple server failed.", KR(ret));
      sleep(5);
      abort();
    }

    if (ObSimpleServerRestartHelper::is_restart_) {
      SERVER_LOG(INFO, "sleep %d seconds before run restart test cases.\n", KR(ret));
      fprintf(stdout, "sleep %d seconds before run restart test cases.\n",
              ObSimpleServerRestartHelper::sleep_sec_);
      sleep(ObSimpleServerRestartHelper::sleep_sec_);
    }
  }
}

void ObSimpleClusterTestBase::TearDown()
{
  auto case_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
  SERVER_LOG(INFO, "TearDown>>>>>>>>>>>>>>", K(case_name));
}

void ObSimpleClusterTestBase::TearDownTestCase()
{
  SERVER_LOG(INFO, "TearDownTestCase");

  int ret = OB_SUCCESS;
  if (false && OB_NOT_NULL(cluster_)) {
    ret = close();
    ASSERT_EQ(ret, OB_SUCCESS);
  }
  int fail_cnt = ::testing::UnitTest::GetInstance()->failed_test_case_count();
  if (chdir(curr_dir_.c_str()) == 0) {
    bool to_delete = true;
    if (ObSimpleServerRestartHelper::need_restart_ && !ObSimpleServerRestartHelper::is_restart_) {
      to_delete = false;
    }
    if (to_delete) {
      system((std::string("rm -rf ") + env_prefix_ + std::string("*")).c_str());
    }
  }
  _Exit(fail_cnt);
}

int ObSimpleClusterTestBase::start()
{
  SERVER_LOG(INFO, "start simple cluster test base");
  OB_LOGGER.set_enable_log_limit(false);
  oceanbase::palf::election::GLOBAL_INIT_ELECTION_MODULE();
  oceanbase::palf::election::INIT_TS = 1;
  oceanbase::palf::election::MAX_TST = 100 * 1000;
  GCONF.enable_perf_event = false;
  GCONF.enable_sql_audit = true;
  GCONF.enable_record_trace_log = false;

  int32_t log_level;
  bool change_log_level = false;
  if (enable_env_warn_log_) {
    if (OB_LOGGER.get_log_level() > OB_LOG_LEVEL_WARN) {
      change_log_level = true;
      log_level = OB_LOGGER.get_log_level();
      OB_LOGGER.set_log_level("WARN");
    }
  }

  int ret = cluster_->simple_start();
  is_started_ = true;
  if (change_log_level) {
    OB_LOGGER.set_log_level(log_level);
  }
  return ret;
}

int ObSimpleClusterTestBase::close()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(cluster_)) {
    ret = cluster_->simple_close();
  }
  return ret;
}

int ObSimpleClusterTestBase::create_tenant(const char *tenant_name,
                                           const char *memory_size,
                                           const char *log_disk_size,
                                           const bool oracle_mode,
                                           int64_t tenant_cpu)
{
  SERVER_LOG(INFO, "create tenant start");
  int32_t log_level;
  bool change_log_level = false;
  if (enable_env_warn_log_) {
    if (OB_LOGGER.get_log_level() > OB_LOG_LEVEL_WARN) {
      change_log_level = true;
      log_level = OB_LOGGER.get_log_level();
      OB_LOGGER.set_log_level("WARN");
    }
  }
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = cluster_->get_sql_proxy();
  int64_t affected_rows = 0;
  {
    ObSqlString sql;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.assign_fmt("set session ob_trx_timeout=1000000000000;"))) {
      SERVER_LOG(WARN, "set session", K(ret));
    } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
      SERVER_LOG(WARN, "set session", K(ret));
    }
  }
  {
    ObSqlString sql;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.assign_fmt("set session ob_query_timeout=1000000000000;"))) {
      SERVER_LOG(WARN, "set session", K(ret));
    } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
      SERVER_LOG(WARN, "set session", K(ret));
    }
  }
  {
    ObSqlString sql;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.assign_fmt("create resource unit %s%s max_cpu %ld, memory_size '%s', log_disk_size='%s';",
                                      UNIT_BASE, tenant_name, tenant_cpu, memory_size, log_disk_size))) {
      SERVER_LOG(WARN, "create_tenant", K(ret));
    } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
      SERVER_LOG(WARN, "create_tenant", K(ret));
    }
  }
  {
    ObSqlString sql;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.assign_fmt("create resource pool %s%s unit = '%s%s', unit_num = 1, zone_list = ('zone1');", POOL_BASE, tenant_name, UNIT_BASE, tenant_name))) {
      SERVER_LOG(WARN, "create_tenant", K(ret));
    } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
      SERVER_LOG(WARN, "create_tenant", K(ret));
    }
  }
  {
    ObSqlString sql;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql.assign_fmt("create tenant %s replica_num = 1, primary_zone='zone1', resource_pool_list=('%s%s') set ob_tcp_invited_nodes='%%'%s", tenant_name, POOL_BASE, tenant_name, oracle_mode ? ", ob_compatibility_mode='oracle'" : ";"))) {
      SERVER_LOG(WARN, "create_tenant", K(ret));
    } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
      SERVER_LOG(WARN, "create_tenant", K(ret));
    }
  }
  if (change_log_level) {
    OB_LOGGER.set_log_level(log_level);
  }
  SERVER_LOG(INFO, "create tenant finish", K(ret), K(tenant_name));
  return ret;
}

int ObSimpleClusterTestBase::delete_tenant(const char *tenant_name)
{
  ObSqlString sql;
  common::ObMySQLProxy &sql_proxy = cluster_->get_sql_proxy();
  sql.assign_fmt("drop tenant %s force", tenant_name);

  int64_t affected_rows = 0;
  return sql_proxy.write(sql.ptr(), affected_rows);
}

int ObSimpleClusterTestBase::get_tenant_id(uint64_t &tenant_id, const char *tenant_name)
{
  SERVER_LOG(INFO, "get_tenant_id");
  int ret = OB_SUCCESS;
  ObSqlString sql;
  common::ObMySQLProxy &sql_proxy = cluster_->get_sql_proxy();
  sql.assign_fmt("select tenant_id from oceanbase.__all_tenant where tenant_name = '%s'", tenant_name);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      SERVER_LOG(WARN, "get_tenant_id", K(ret));
    } else {
      sqlclient::ObMySQLResult *result = res.get_result();
      if (result != nullptr && OB_SUCC(result->next())) {
        ret = result->get_uint("tenant_id", tenant_id);
        SERVER_LOG(WARN, "get_tenant_id", K(ret));
      } else {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "get_tenant_id", K(ret));
      }
    }
  }
  return ret;
}

int ObSimpleClusterTestBase::exec_write_sql_sys(const char *sql_str, int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  return sql_proxy.write(sql_str, affected_rows);
}

int ObSimpleClusterTestBase::check_tenant_exist(bool &bool_ret, const char *tenant_name)
{
  int ret = OB_SUCCESS;
  bool_ret = true;
  uint64_t tenant_id;
  if (OB_FAIL(get_tenant_id(tenant_id, tenant_name))) {
    SERVER_LOG(WARN, "get_tenant_id failed", K(ret));
  } else {
    ObSqlString sql;
    common::ObMySQLProxy &sql_proxy = cluster_->get_sql_proxy();
    sql.assign_fmt("select tenant_id from oceanbase.gv$ob_units where tenant_id= '%" PRIu64"' ", tenant_id);
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
        SERVER_LOG(WARN, "get gv$ob_units", K(ret));
      } else {
        sqlclient::ObMySQLResult *result = res.get_result();
        if (result != nullptr && OB_SUCC(result->next())) {
          bool_ret = true;
        } else if (result == nullptr) {
          bool_ret = false;
        } else {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "get_tenant_id", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSimpleClusterTestBase::batch_create_table(
    const uint64_t tenant_id,
    ObMySQLProxy &sql_proxy,
    const int64_t TOTAL_NUM,
    ObIArray<ObTabletLSPair> &tablet_ls_pairs)
{
  int ret = OB_SUCCESS;
  tablet_ls_pairs.reset();
  ObSqlString sql;
  // batch create table
  int64_t affected_rows = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < TOTAL_NUM; ++i) {
    sql.reset();
    if (OB_FAIL(sql.assign_fmt("create table t%ld(c1 int)", i))) {
    } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
    }
  }
  // batch get table_id
  sql.reset();
  if (OB_FAIL(sql.assign_fmt("select TABLET_ID, LS_ID from oceanbase.DBA_OB_TABLE_LOCATIONS where table_name in ("))) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < TOTAL_NUM; ++i) {
      if (OB_FAIL(sql.append_fmt("%s't%ld'", 0 == i ? "" : ",", i))) {}
    }
    if (FAILEDx(sql.append_fmt(") order by TABLET_ID"))) {};
  }
  SMART_VAR(ObMySQLProxy::MySQLResult, result) {
    if (OB_FAIL(tablet_ls_pairs.reserve(TOTAL_NUM))) {
    } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(sql_proxy.read(result, sql.ptr()))) {
    } else if (OB_ISNULL(result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      sqlclient::ObMySQLResult &res = *result.get_result();
      uint64_t tablet_id = ObTabletID::INVALID_TABLET_ID;
      int64_t ls_id = ObLSID::INVALID_LS_ID;
      while(OB_SUCC(ret) && OB_SUCC(res.next())) {
        EXTRACT_INT_FIELD_MYSQL(res, "TABLET_ID", tablet_id, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(res, "LS_ID", ls_id, int64_t);
        if (OB_FAIL(tablet_ls_pairs.push_back(ObTabletLSPair(tablet_id, ls_id)))) {}
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        SERVER_LOG(WARN, "fail to generate data", K(sql));
      }
    }
  }
  return ret;
}

int ObSimpleClusterTestBase::batch_drop_table(
    const uint64_t tenant_id,
    ObMySQLProxy &sql_proxy,
    const int64_t TOTAL_NUM)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < TOTAL_NUM; ++i) {
    sql.reset();
    if (OB_FAIL(sql.assign_fmt("drop table t%ld", i))) {
    } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
    }
  }
  return ret;
}

} // end unittest
} // end oceanbase
