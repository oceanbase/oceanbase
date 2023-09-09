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
#include "share/tablet/ob_tablet_to_ls_operator.cpp"
#include "lib/string/ob_sql_string.h" // ObSqlString
#include "lib/mysqlclient/ob_mysql_proxy.h" // ObISqlClient, SMART_VAR
#include "observer/ob_sql_client_decorator.h" // ObSQLClientRetryWeak
#include "env/ob_simple_cluster_test_base.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase
{
using namespace unittest;
namespace share
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace schema;
using namespace common;

class TestTabletToLSOperator : public unittest::ObSimpleClusterTestBase
{
public:
  TestTabletToLSOperator() : unittest::ObSimpleClusterTestBase("test_ls_status_operator") {}
};

TEST_F(TestTabletToLSOperator, UpdateLSAndTransSeq)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletToLSInfo, 3> ls_infos;
  ObTabletToLSInfo info;
  ObLSID ls1 = ObLSID(1);
  ObLSID ls2 = ObLSID(2);
  ObLSID ls3 = ObLSID(3);
  ObTabletID t1 = ObTabletID(1);
  ObTabletID t2 = ObTabletID(2);
  ObTabletID t3 = ObTabletID(3);
  const uint64_t table_id = 1;
  int64_t old_transfer_seq = 0;
  int64_t new_transfer_seq = 1;
  int64_t ls_id = ObLSID::INVALID_LS_ID;
  int64_t transfer_seq = -1;
  const int32_t group_id = 0;
  info.reset();
  info.init(t1, ls1, table_id, old_transfer_seq);
  ls_infos.push_back(info);
  info.reset();
  info.init(t2, ls2, table_id, old_transfer_seq);
  ls_infos.push_back(info);
  info.reset();
  info.init(t3, ls3, table_id, old_transfer_seq);
  ls_infos.push_back(info);
  ASSERT_EQ(3, ls_infos.count());
  ret = ObTabletToLSTableOperator::batch_update(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      OB_SYS_TENANT_ID,
      ls_infos);
  ASSERT_EQ(OB_SUCCESS, ret);

  // test OB_INVALID_ARGUMENT: invalid tenant_id
  ret = ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      OB_INVALID_TENANT_ID,
      t1,
      old_transfer_seq,
      ls1,
      new_transfer_seq,
      ls2,
      group_id);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  // test OB_INVALID_ARGUMENT: invalid old_transfer_seq
  ret = ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      OB_SYS_TENANT_ID,
      t1,
      -1,
      ls1,
      new_transfer_seq,
      ls2,
      group_id);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  // test OB_INVALID_ARGUMENT: invalid new_transfer_seq
  ret = ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      OB_SYS_TENANT_ID,
      t1,
      old_transfer_seq,
      ls1,
      -1,
      ls2,
      group_id);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  // test OB_INVALID_ARGUMENT: old_transfer_seq == new_transfer_seq
  ret = ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      OB_SYS_TENANT_ID,
      t1,
      old_transfer_seq,
      ls1,
      old_transfer_seq,
      ls2,
      group_id);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  // test OB_INVALID_ARGUMENT: invalid tablet_id
  ret = ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      OB_SYS_TENANT_ID,
      ObTabletID(),
      old_transfer_seq,
      ls1,
      new_transfer_seq,
      ls2,
      group_id);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  // test OB_INVALID_ARGUMENT: invalid old_ls_id
  ret = ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      OB_SYS_TENANT_ID,
      t1,
      old_transfer_seq,
      ObLSID(),
      new_transfer_seq,
      ls2,
      group_id);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  // test OB_INVALID_ARGUMENT: invalid new_ls_id
  ret = ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      OB_SYS_TENANT_ID,
      t1,
      old_transfer_seq,
      ls1,
      new_transfer_seq,
      ObLSID(),
      group_id);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  // test OB_INVALID_ARGUMENT: old_ls_id == new_ls_id
  ret = ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      OB_SYS_TENANT_ID,
      t1,
      old_transfer_seq,
      ls1,
      new_transfer_seq,
      ls1,
      group_id);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);

  // test OB_ENTRY_NOT_EXIST: t1 is not in ls3
  ret = ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      OB_SYS_TENANT_ID,
      t1,
      old_transfer_seq,
      ls3,
      new_transfer_seq,
      ls2,
      group_id);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  // test OB_ENTRY_NOT_EXIST:  t1's transfer_seq should be 0, but here it's 3
  old_transfer_seq = 3;
  ret = ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      OB_SYS_TENANT_ID,
      t1,
      old_transfer_seq,
      ls1,
      new_transfer_seq,
      ls2,
      group_id);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  // test OB_SUCCESS: transfer t1 from ls1 to ls2
  old_transfer_seq = 0;
  ret = ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      OB_SYS_TENANT_ID,
      t1,
      old_transfer_seq,
      ls1,
      new_transfer_seq,
      ls2,
      group_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  // test OB_ENTRY_NOT_EXIST: t1 is not in ls1
  ret = ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      OB_SYS_TENANT_ID,
      t1,
      old_transfer_seq,
      ls1,
      new_transfer_seq,
      ls2,
      group_id);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  // test OB_SUCCESS: transfer t1 from ls2 to ls3
  old_transfer_seq = new_transfer_seq;
  new_transfer_seq = new_transfer_seq + 1;
  ret = ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      OB_SYS_TENANT_ID,
      t1,
      old_transfer_seq,
      ls2,
      new_transfer_seq,
      ls3,
      group_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  // test OB_SUCCESS: transfer t2 from ls2 to ls1
  old_transfer_seq = 0;
  new_transfer_seq = 1;
  ret = ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      OB_SYS_TENANT_ID,
      t2,
      old_transfer_seq,
      ls2,
      new_transfer_seq,
      ls1,
      group_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  // test OB_ENTRY_NOT_EXIST: t2 is not in ls2
  ret = ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      OB_SYS_TENANT_ID,
      t2,
      old_transfer_seq,
      ls2,
      new_transfer_seq,
      ls1,
      group_id);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  // test OB_SUCCESS: transfer t2 from ls1 to ls2
  old_transfer_seq = new_transfer_seq;
  new_transfer_seq = new_transfer_seq + 1;
  ret = ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      OB_SYS_TENANT_ID,
      t2,
      old_transfer_seq,
      ls1,
      new_transfer_seq,
      ls2,
      group_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  // test OB_SUCCESS: transfer t3 from ls3 to ls1
  old_transfer_seq = 0;
  new_transfer_seq = 1;
  ret = ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      OB_SYS_TENANT_ID,
      t3,
      old_transfer_seq,
      ls3,
      new_transfer_seq,
      ls1,
      group_id);
  ASSERT_EQ(OB_SUCCESS, ret);
  // test OB_ENTRY_NOT_EXIST: t3 is not in ls3
  ret = ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(
      get_curr_simple_server().get_observer().get_mysql_proxy(),
      OB_SYS_TENANT_ID,
      t3,
      old_transfer_seq,
      ls3,
      new_transfer_seq,
      ls1,
      group_id);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);

  // test final result in table __all_tablet_to_ls
  SMART_VAR(ObISQLClient::ReadResult, result) {
    ObSQLClientRetryWeak sql_client_retry_weak(
        &get_curr_simple_server().get_observer().get_mysql_proxy(),
        OB_SYS_TENANT_ID,
        OB_ALL_TABLET_TO_LS_TID);
    ObSqlString sql;
    // t1 should be in ls3 and its transfer_seq should be 2
    ret = sql.append_fmt("SELECT ls_id, transfer_seq FROM %s WHERE tablet_id=1",
        OB_ALL_TABLET_TO_LS_TNAME);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = sql_client_retry_weak.read(result, OB_SYS_TENANT_ID, sql.ptr());
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(false, OB_ISNULL(result.get_result()));
    ret = result.get_result()->get_int("ls_id", ls_id);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(3, ls_id);
    ret = result.get_result()->get_int("transfer_seq", transfer_seq);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(2, transfer_seq);
    // t2 should be in ls2 and its transfer_seq should be 2
    ret = sql.append_fmt("SELECT ls_id, transfer_seq FROM %s WHERE tablet_id=2",
        OB_ALL_TABLET_TO_LS_TNAME);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = sql_client_retry_weak.read(result, OB_SYS_TENANT_ID, sql.ptr());
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(false, OB_ISNULL(result.get_result()));
    ls_id = ObLSID::INVALID_LS_ID;
    transfer_seq = -1;
    ret = result.get_result()->get_int("ls_id", ls_id);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(2, ls_id);
    ret = result.get_result()->get_int("transfer_seq", transfer_seq);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(2, transfer_seq);
    // t3 should be in ls1 and its transfer_seq should be 1
    ret = sql.append_fmt("SELECT ls_id, transfer_seq FROM %s WHERE tablet_id=3",
        OB_ALL_TABLET_TO_LS_TNAME);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = sql_client_retry_weak.read(result, OB_SYS_TENANT_ID, sql.ptr());
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(false, OB_ISNULL(result.get_result()));
    ls_id = ObLSID::INVALID_LS_ID;
    transfer_seq = -1;
    ret = result.get_result()->get_int("ls_id", ls_id);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(1, ls_id);
    ret = result.get_result()->get_int("transfer_seq", transfer_seq);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(1, transfer_seq);
  }
}

class TestTabletToLSOperatorBatchGet : public unittest::ObSimpleClusterTestBase
{
public:
  TestTabletToLSOperatorBatchGet() : unittest::ObSimpleClusterTestBase("test_ls_status_operator_batch_get") {}
};

TEST_F(TestTabletToLSOperatorBatchGet, test_batch_get)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = 1002;
  ObArray<ObTabletToLSInfo> infos;
  ObTabletToLSInfo info1(ObTabletID(200001), ObLSID(1002), 123456, 0);
  ObTabletToLSInfo info2(ObTabletID(200002), ObLSID(1004), 654321, 1);
  ASSERT_EQ(OB_SUCCESS, infos.push_back(info1));
  ASSERT_EQ(OB_SUCCESS, infos.push_back(info2));
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_observer().get_mysql_proxy();
  ASSERT_EQ(OB_SUCCESS, ObTabletToLSTableOperator::batch_update(sql_proxy, tenant_id, infos));

  ObArray<ObTabletID> tablet_ids;
  ObArray<ObTabletToLSInfo> res_infos;
  ObArray<ObLSID> ls_ids;
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(info1.get_tablet_id()));
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(info2.get_tablet_id()));
  ASSERT_EQ(OB_SUCCESS, ObTabletToLSTableOperator::batch_get(sql_proxy, tenant_id, tablet_ids, res_infos));
  ASSERT_EQ(OB_SUCCESS, ObTabletToLSTableOperator::batch_get_ls(sql_proxy, tenant_id, tablet_ids, ls_ids));
  ASSERT_TRUE(2 == res_infos.count());
  ASSERT_TRUE(res_infos.at(0) == info1);
  ASSERT_TRUE(res_infos.at(1) == info2);
  ASSERT_TRUE(2 == ls_ids.count());
  ASSERT_TRUE(ls_ids.at(0) == info1.get_ls_id());
  ASSERT_TRUE(ls_ids.at(1) == info2.get_ls_id());

  // exist invalid tablet_ids
  res_infos.reset();
  ls_ids.reset();
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(ObTabletID()));
  ASSERT_EQ(OB_ITEM_NOT_MATCH, ObTabletToLSTableOperator::batch_get(sql_proxy, tenant_id, tablet_ids, res_infos));
  ASSERT_EQ(OB_ITEM_NOT_MATCH, ObTabletToLSTableOperator::batch_get_ls(sql_proxy, tenant_id, tablet_ids, ls_ids));

  // exist duplicate tablet_ids
  tablet_ids.reset();
  ls_ids.reset();
  res_infos.reset();
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(info1.get_tablet_id()));
  ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(info1.get_tablet_id()));
  ASSERT_EQ(OB_ITEM_NOT_MATCH, ObTabletToLSTableOperator::batch_get(sql_proxy, tenant_id, tablet_ids, res_infos));
  ASSERT_EQ(OB_ITEM_NOT_MATCH, ObTabletToLSTableOperator::batch_get_ls(sql_proxy, tenant_id, tablet_ids, ls_ids));

  const int64_t MAX_BATCH_COUNT = 200;
  ObArray<ObTabletToLSInfo> input_infos;
  tablet_ids.reset();
  res_infos.reset();
  ASSERT_EQ(OB_SUCCESS, input_infos.reserve(MAX_BATCH_COUNT + 1));
  ASSERT_EQ(OB_SUCCESS, tablet_ids.reserve(MAX_BATCH_COUNT + 1));
  ASSERT_EQ(OB_SUCCESS, res_infos.reserve(MAX_BATCH_COUNT + 1));
  for(int64_t i = 0; i < MAX_BATCH_COUNT - 1; ++i) {
    ObTabletID tmp_tablet_id(20000 + i);
    ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(tmp_tablet_id));
    ObTabletToLSInfo tmp_info(tmp_tablet_id, ObLSID(1002), 123456, 0);
    ASSERT_EQ(OB_SUCCESS, input_infos.push_back(tmp_info));
  }
  // MAX_BATCH_COUNT - 1
  ASSERT_EQ(OB_SUCCESS, ObTabletToLSTableOperator::batch_update(sql_proxy, tenant_id, input_infos));
  ASSERT_EQ(OB_SUCCESS, ObTabletToLSTableOperator::batch_get(sql_proxy, tenant_id, tablet_ids, res_infos));
  ASSERT_TRUE(MAX_BATCH_COUNT - 1 == res_infos.count());
  ARRAY_FOREACH(res_infos, idx) {
    ASSERT_TRUE(res_infos.at(idx) == input_infos.at(idx));
  }
  // MAX_BATCH_COUNT
  {
    ObTabletID tmp_tablet_id(20000 + MAX_BATCH_COUNT - 1);
    ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(tmp_tablet_id));
    ObTabletToLSInfo tmp_info(tmp_tablet_id, ObLSID(1002), 123456, 0);
    ASSERT_EQ(OB_SUCCESS, input_infos.push_back(tmp_info));
  }
  ASSERT_EQ(OB_SUCCESS, ObTabletToLSTableOperator::batch_update(sql_proxy, tenant_id, input_infos));
  ASSERT_EQ(OB_SUCCESS, ObTabletToLSTableOperator::batch_get(sql_proxy, tenant_id, tablet_ids, res_infos));
  ASSERT_TRUE(MAX_BATCH_COUNT == res_infos.count());
  ARRAY_FOREACH(res_infos, idx) {
    ASSERT_TRUE(res_infos.at(idx) == input_infos.at(idx));
  }
  // MAX_BATCH_COUNT + 1
  {
    ObTabletID tmp_tablet_id(20000 + MAX_BATCH_COUNT);
    ASSERT_EQ(OB_SUCCESS, tablet_ids.push_back(tmp_tablet_id));
    ObTabletToLSInfo tmp_info(tmp_tablet_id, ObLSID(1002), 123456, 0);
    ASSERT_EQ(OB_SUCCESS, input_infos.push_back(tmp_info));
  }
  ASSERT_EQ(OB_SUCCESS, ObTabletToLSTableOperator::batch_update(sql_proxy, tenant_id, input_infos));
  ASSERT_EQ(OB_SUCCESS, ObTabletToLSTableOperator::batch_get(sql_proxy, tenant_id, tablet_ids, res_infos));
  ASSERT_TRUE(MAX_BATCH_COUNT + 1 == res_infos.count());
  ARRAY_FOREACH(res_infos, idx) {
    ASSERT_TRUE(res_infos.at(idx) == input_infos.at(idx));
  }

  ObLSID ls_id;
  ASSERT_EQ(OB_SUCCESS, ObTabletToLSTableOperator::get_ls_by_tablet(sql_proxy, tenant_id, tablet_ids.at(0), ls_id));
  ASSERT_TRUE(ls_id == input_infos.at(0).get_ls_id());
  ls_id.reset();
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObTabletToLSTableOperator::get_ls_by_tablet(sql_proxy, tenant_id, ObTabletID(123456), ls_id));

  // test batch_get_tablet_ls_cache
  ObArray<ObTabletLSCache> tablet_ls_caches;
  ASSERT_EQ(OB_SUCCESS, ObTabletToLSTableOperator::batch_get_tablet_ls_cache(sql_proxy, tenant_id, tablet_ids, tablet_ls_caches));
  ASSERT_TRUE(tablet_ids.count() == tablet_ls_caches.count());
  ARRAY_FOREACH(tablet_ls_caches, idx) {
    ASSERT_TRUE(common::has_exist_in_array(tablet_ids, tablet_ls_caches.at(idx).get_tablet_id()));
  }

  // test batch_get_tablet_ls_pairs
  ObArray<ObTabletLSPair> tablet_ls_pairs;
  ASSERT_EQ(OB_SUCCESS, ObTabletToLSTableOperator::batch_get_tablet_ls_pairs(sql_proxy, tenant_id, tablet_ids, tablet_ls_pairs));
  ASSERT_TRUE(tablet_ids.count() == tablet_ls_pairs.count());
  ARRAY_FOREACH(tablet_ls_pairs, idx) {
    ASSERT_TRUE(common::has_exist_in_array(tablet_ids, tablet_ls_caches.at(idx).get_tablet_id()));
  }

}

} // share
} // oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
