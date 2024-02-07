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

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "share/ls/ob_ls_status_operator.h"
#include "share/ls/ob_ls_recovery_stat_operator.h"
#include "share/ls/ob_ls_life_manager.h"
#include "share/scn.h"
#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"


namespace oceanbase
{
using namespace unittest;
using namespace palf;
namespace share
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace schema;
using namespace common;
class TestLSStatusOperator : public unittest::ObSimpleClusterTestBase
{
public:
  TestLSStatusOperator() : unittest::ObSimpleClusterTestBase("test_ls_status_operator") {}
protected:
  ObLSStatusOperator ls_status_op_;
  ObLSRecoveryStat ls_recovery_op_;
  uint64_t tenant_id_;
};

TEST_F(TestLSStatusOperator, SQLProxy)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = 1000002;
  const int64_t local_cluster_id = 1;
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2("sys", "oceanbase"));
  ObSqlString sql;
  sql.assign_fmt("select tenant_id, ls_id from oceanbase.__all_virtual_ls_status");
  ASSERT_EQ(OB_SUCCESS, ret);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    //mysql proxy 指定cluster_id报错，指定本集群也报错，不指定可以查询
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
    ret = sql_proxy.read(res, sql.ptr());
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = sql_proxy.read(res, cluster_id, OB_SYS_TENANT_ID, sql.ptr());
    ASSERT_EQ(OB_NOT_SUPPORTED, ret);
    ret = sql_proxy.read(res, local_cluster_id, OB_SYS_TENANT_ID, sql.ptr());
    ASSERT_EQ(OB_NOT_SUPPORTED, ret);
    ret = sql_proxy.read(res, OB_INVALID_CLUSTER_ID, OB_SYS_TENANT_ID, sql.ptr());
    ASSERT_EQ(OB_NOT_SUPPORTED, ret);


    //innser_sql不指定查询本集群，指定本集群查询本集群，指定非法报错
    common::ObMySQLProxy &inner_proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
    ret = inner_proxy.read(res, sql.ptr());
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = inner_proxy.read(res, cluster_id, OB_SYS_TENANT_ID, sql.ptr());
    ASSERT_EQ(OB_TIMEOUT, ret);
    ret = inner_proxy.read(res, local_cluster_id, OB_SYS_TENANT_ID, sql.ptr());
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = inner_proxy.read(res, OB_INVALID_CLUSTER_ID, OB_SYS_TENANT_ID, sql.ptr());
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);


    //trans不能指定远程集群，在mysql proxy开启的事务
    ObMySQLTransaction trans;
    ret = trans.start(&sql_proxy, OB_SYS_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = trans.read(res, sql.ptr());
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = trans.read(res, local_cluster_id, OB_SYS_TENANT_ID, sql.ptr());
    ASSERT_EQ(OB_NOT_SUPPORTED, ret);
    ObMySQLTransaction trans11;
    ret = trans11.start(&sql_proxy, OB_SYS_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = trans11.read(res, cluster_id, OB_SYS_TENANT_ID, sql.ptr());
    ASSERT_EQ(OB_NOT_SUPPORTED, ret);

    ObMySQLTransaction trans12;
    ret = trans12.start(&sql_proxy, OB_SYS_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = trans12.read(res, OB_INVALID_CLUSTER_ID, OB_SYS_TENANT_ID, sql.ptr());
    ASSERT_EQ(OB_NOT_SUPPORTED, ret);


    //trans 在innser_sql上开启的事务
    ObMySQLTransaction trans2;
    ret = trans2.start(&inner_proxy, OB_SYS_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = trans2.read(res, sql.ptr());
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = trans2.read(res, cluster_id, OB_SYS_TENANT_ID, sql.ptr());
    ASSERT_EQ(OB_ERR_UNEXPECTED, ret);
    ObMySQLTransaction trans21;
    ret = trans21.start(&inner_proxy, OB_SYS_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = trans21.read(res, local_cluster_id, OB_SYS_TENANT_ID, sql.ptr());
    ASSERT_EQ(OB_SUCCESS, ret);
    ObMySQLTransaction trans22;
    ret = trans22.start(&inner_proxy, OB_SYS_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, ret);

    ret = trans22.read(res, OB_INVALID_CLUSTER_ID, OB_SYS_TENANT_ID, sql.ptr());
    ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  }
}

TEST_F(TestLSStatusOperator, LSLifeAgent)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id_));
  ObSqlString sql;
  //common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();

  tenant_id_ = 1002;
  ObLSLifeAgentManager ls_life(get_curr_simple_server().get_observer().get_mysql_proxy());
  ObLSStatusOperator status_operator;
  ObLSStatusInfoArray ls_array;
  //非法参数检查
  share::SCN create_scn;
  ObLSStatusInfo info;
  ObZone zone_priority("z1");
  ret = ls_life.create_new_ls(info, create_scn, zone_priority.str(), share::NORMAL_SWITCHOVER_STATUS);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ObZone primary_zone("z1");
  ObLSFlag flag(share::ObLSFlag::NORMAL_FLAG);
  ret = info.init(tenant_id_, SYS_LS, 0, share::OB_LS_CREATING, 0, primary_zone, flag);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = status_operator.get_all_ls_status_by_order(OB_SYS_TENANT_ID, ls_array,
      get_curr_simple_server().get_observer().get_mysql_proxy());
  ASSERT_EQ(1, ls_array.count());
  SERVER_LOG(INFO, "ls status", K(ls_array));
  ret = ls_life.create_new_ls(info, create_scn, zone_priority.str(), share::NORMAL_SWITCHOVER_STATUS);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  create_scn.set_min();

  //创建新日志流
  ObLSID ls_id(1002);
  ret = info.init(tenant_id_, ls_id, 0, share::OB_LS_CREATING, 0, primary_zone, flag);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_life.create_new_ls(info, create_scn, zone_priority.str(), share::NORMAL_SWITCHOVER_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);

  //设置初始成员列表
  ObMemberList member_list;
  ObMember arb_member;
  common::GlobalLearnerList learner_list;
  ObAddr server1(common::ObAddr::IPV4, "127.1.1.1", 2882);
  ObAddr server2(common::ObAddr::IPV4, "127.1.1.1", 3882);
  ASSERT_EQ(OB_SUCCESS, member_list.add_server(server1));
  ASSERT_EQ(OB_SUCCESS, member_list.add_server(server2));
  ret = status_operator.update_init_member_list(tenant_id_, ls_id, member_list,
      get_curr_simple_server().get_observer().get_mysql_proxy(), arb_member, learner_list);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLSStatusInfo new_status_info;
  ObMemberList new_list;
  ret = status_operator.get_ls_init_member_list(tenant_id_, ls_id, new_list, new_status_info,
      get_curr_simple_server().get_observer().get_mysql_proxy(), arb_member, learner_list);
  ASSERT_EQ(OB_SUCCESS, ret);

  //创建新日志流
  ObLSStatusInfo new_status_info2;
  ObLSID ls_id3(1003);
  ret = new_status_info2.init(tenant_id_, ls_id3, 0, share::OB_LS_CREATING, 0, primary_zone, flag);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_life.create_new_ls(new_status_info2, create_scn, zone_priority.str(), share::NORMAL_SWITCHOVER_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObAddr server4(common::ObAddr::IPV4, "127.1.1.1", 4882);
  ObMember arb_member2(server4, 0);
  common::GlobalLearnerList learner_list2;

  ret = status_operator.update_init_member_list(tenant_id_, ls_id3, member_list,
      get_curr_simple_server().get_observer().get_mysql_proxy(), arb_member2, learner_list2);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObLSStatusInfo new_status_info3;
  ObMemberList new_list2;
  ObMember arb_member3;
  common::GlobalLearnerList learner_list3;
  ret = status_operator.get_ls_init_member_list(tenant_id_, ls_id3, new_list2, new_status_info3,
      get_curr_simple_server().get_observer().get_mysql_proxy(), arb_member3, learner_list3);
  ASSERT_EQ(OB_SUCCESS, ret);

  //设置日志流offline的参数检查
  share::SCN invalid_scn;
  ret = ls_life.set_ls_offline(tenant_id_, SYS_LS, share::OB_LS_DROPPING, invalid_scn, share::NORMAL_SWITCHOVER_STATUS);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = ls_life.set_ls_offline(tenant_id_, SYS_LS, share::OB_LS_NORMAL, share::SCN::min_scn(), share::NORMAL_SWITCHOVER_STATUS);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  //更新__all_ls_status表为空
  ret = ls_life.set_ls_offline(tenant_id_, ls_id, share::OB_LS_DROPPING, share::SCN::min_scn(), share::NORMAL_SWITCHOVER_STATUS);
  ASSERT_EQ(OB_NEED_RETRY, ret);

  //更新__all_ls_status表中1001日志流的状态
  ret = status_operator.update_ls_status(tenant_id_, ls_id, share::OB_LS_EMPTY, share::OB_LS_DROPPING, share::NORMAL_SWITCHOVER_STATUS,
      get_curr_simple_server().get_observer().get_mysql_proxy());
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = status_operator.update_ls_status(tenant_id_, ls_id, share::OB_LS_CREATING, share::OB_LS_EMPTY, share::NORMAL_SWITCHOVER_STATUS,
      get_curr_simple_server().get_observer().get_mysql_proxy());
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ret = status_operator.update_ls_status(tenant_id_, ls_id, share::OB_LS_DROPPING, share::OB_LS_DROPPING, share::NORMAL_SWITCHOVER_STATUS,
      get_curr_simple_server().get_observer().get_mysql_proxy());
  ASSERT_EQ(OB_NEED_RETRY, ret);

  ret = status_operator.update_ls_status(tenant_id_, ls_id, share::OB_LS_CREATING, share::OB_LS_NORMAL, share::NORMAL_SWITCHOVER_STATUS,
      get_curr_simple_server().get_observer().get_mysql_proxy());
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_life.set_ls_offline(tenant_id_, ls_id, share::OB_LS_NORMAL, share::SCN::min_scn(), share::NORMAL_SWITCHOVER_STATUS);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  //更新成dropping状态，才可以set_offline
  ret = status_operator.update_ls_status(tenant_id_, ls_id, share::OB_LS_NORMAL, share::OB_LS_DROPPING, share::NORMAL_SWITCHOVER_STATUS,
      get_curr_simple_server().get_observer().get_mysql_proxy());
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_life.set_ls_offline(tenant_id_, ls_id, share::OB_LS_TENANT_DROPPING, share::SCN::min_scn(), share::NORMAL_SWITCHOVER_STATUS);
  ASSERT_EQ(OB_NEED_RETRY, ret);
  share::SCN scn;
  scn.convert_for_logservice(100);
  ret = ls_life.set_ls_offline(tenant_id_, ls_id, share::OB_LS_DROPPING, scn, share::NORMAL_SWITCHOVER_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);

  //drop_ls，要等待sync等大于dropping
  ret = ls_life.drop_ls(tenant_id_, ls_id, share::NORMAL_SWITCHOVER_STATUS);
  ASSERT_EQ(OB_NEED_RETRY, ret);
  ObLSRecoveryStat recovery_stat;
  ObLSRecoveryStatOperator recovery_op;
  palf::LogConfigVersion config_version;
  ret = config_version.generate(1,2);
  ASSERT_EQ(OB_SUCCESS, ret);
  scn.convert_for_logservice(99);
  ret = recovery_stat.init_only_recovery_stat(tenant_id_, ls_id, scn, scn, config_version);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = recovery_op.update_ls_recovery_stat(recovery_stat, false, get_curr_simple_server().get_observer().get_mysql_proxy());
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_life.drop_ls(tenant_id_, ls_id, share::NORMAL_SWITCHOVER_STATUS);
  ASSERT_EQ(OB_NEED_RETRY, ret);
  scn.convert_for_logservice(100);
  share::SCN recovery_scn;
  recovery_scn.convert_for_logservice(98);
  //readable scn 大于sync_scn
  ret = recovery_stat.init_only_recovery_stat(tenant_id_, ls_id, scn, recovery_scn,config_version);
  ASSERT_EQ(OB_SUCCESS, ret);
  //recovery_stat的取最大值
  ret = recovery_op.update_ls_recovery_stat(recovery_stat, false, get_curr_simple_server().get_observer().get_mysql_proxy());
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = ls_life.drop_ls(tenant_id_, ls_id, share::NORMAL_SWITCHOVER_STATUS);
  ASSERT_EQ(OB_NEED_RETRY, ret);
  recovery_scn.convert_for_logservice(100);
  ret = recovery_stat.init_only_recovery_stat(tenant_id_, ls_id, scn, recovery_scn, config_version);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = recovery_op.update_ls_recovery_stat(recovery_stat, false, get_curr_simple_server().get_observer().get_mysql_proxy());
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ls_life.drop_ls(tenant_id_, ls_id, share::NORMAL_SWITCHOVER_STATUS);
  ASSERT_EQ(OB_SUCCESS, ret);
}

/*
TEST_F(TestLSStatusOperator, add_tenant)
{
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, create_tenant());
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id_));
  ObSqlString sql;
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sql.assign_fmt("select tenant_id, ls_id from oceanbase.__all_virtual_ls_status");
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      SERVER_LOG(WARN, "get_tenant_id", K(ret));
    } else {
      sqlclient::ObMySQLResult *result = res.get_result();
      uint64_t tenant_id = OB_INVALID_TENANT_ID;
      int64_t ls_id = -1;
      while (result != nullptr && OB_SUCC(result->next())) {
        ret = result->get_uint("tenant_id", tenant_id);
        ASSERT_EQ(OB_SUCCESS, ret);
        ret = result->get_int("ls_id", ls_id);
        SERVER_LOG(INFO, "get_tenant_id", K(ret), K(tenant_id), K(ls_id));
      }
    }
  }
  ASSERT_EQ(OB_ITER_END, ret);
}*/

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
