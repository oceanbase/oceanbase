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

#include "gtest/gtest.h"
#define private public
#include "src/share/ls/ob_ls_status_operator.h"
#include "rootserver/standby/ob_protection_mode_mgr.h"
#undef private

#define ASSERT_SUCCESS(expr) ASSERT_EQ(OB_SUCCESS, (expr))

namespace oceanbase
{
namespace standby
{

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::rootserver;
using namespace oceanbase::obrpc;

class TestMAProtectionLevelAutoSwitchHelper : public ::testing::Test
{
public:
  static const uint64_t TEST_CLUSTER_ID = 1;
  static const uint64_t TEST_TENANT_ID = 1002;

  static int make_scn_from_now_delta_us(const int64_t delta_us, SCN &scn)
  {
    const int64_t now = ObTimeUtility::current_time();
    const int64_t ts = now + delta_us;
    return scn.convert_from_ts(ts > 0 ? ts : 1);
  }

  static ObLSStatusInfo make_ls_status(const int64_t ls_id)
  {
    ObLSStatusInfo info;
    info.ls_id_ = ObLSID(ls_id);
    return info;
  }

  static int make_res(const int64_t ls_id,
                      const SCN &palf_sync_scn,
                      const SCN &standby_sync_scn,
                      ObGetLSStandbySyncScnRes &res)
  {
    return res.init(TEST_TENANT_ID, ObLSID(ls_id), palf_sync_scn, standby_sync_scn);
  }

  static int make_arg(const int64_t ls_id, ObGetLSStandbySyncScnArg &arg)
  {
    return arg.init(TEST_TENANT_ID, ObLSID(ls_id));
  }

  static ObMAProtectionLevelAutoSwitchHelper::ObLSStandbySyncScnInfo make_ls_info(
      const int64_t ls_id,
      const SCN &standby_sync_scn,
      const SCN &palf_end_scn,
      const int64_t ls_add_ts)
  {
    ObMAProtectionLevelAutoSwitchHelper::ObLSStandbySyncScnInfo info;
    info.ls_id_ = ObLSID(ls_id);
    info.standby_sync_scn_ = standby_sync_scn;
    info.palf_end_scn_ = palf_end_scn;
    info.ls_add_ts_ = ls_add_ts;
    return info;
  }

  virtual void SetUp() override
  {
    ASSERT_SUCCESS(helper_.init(TEST_CLUSTER_ID, TEST_TENANT_ID));
  }

  ObMAProtectionLevelAutoSwitchHelper helper_;
};

// Verify fresh RPC result overrides old LS sync info.
TEST_F(TestMAProtectionLevelAutoSwitchHelper, get_new_ls_standby_sync_info_use_new_result)
{
  const int64_t now = ObTimeUtility::current_time();
  SCN palf_sync_scn;
  SCN standby_sync_scn;
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-10 * 1000, palf_sync_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-20 * 1000, standby_sync_scn));
  ObGetLSStandbySyncScnRes new_res;
  ASSERT_SUCCESS(make_res(1001, palf_sync_scn, standby_sync_scn, new_res));
  ObMAProtectionLevelAutoSwitchHelper::ObLSStandbySyncScnInfo old_info;
  ObMAProtectionLevelAutoSwitchHelper::ObLSStandbySyncScnInfo new_info;
  ASSERT_SUCCESS(helper_.get_new_ls_standby_sync_info_(
      ObLSID(1001), old_info, new_res, new_info, now));
  ASSERT_EQ(ObLSID(1001), new_info.ls_id_);
  ASSERT_EQ(new_res.get_standby_sync_scn(), new_info.standby_sync_scn_);
  ASSERT_EQ(new_res.get_palf_sync_scn(), new_info.palf_end_scn_);
  ASSERT_EQ(now, new_info.ls_add_ts_);
}

// Verify fallback behavior: reuse old info or initialize default max SCN.
TEST_F(TestMAProtectionLevelAutoSwitchHelper, get_new_ls_standby_sync_info_fallback_to_old_or_empty)
{
  const int64_t now = ObTimeUtility::current_time();
  ObGetLSStandbySyncScnRes invalid_res;
  SCN old_standby_sync_scn;
  SCN old_palf_end_scn;
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-2000, old_standby_sync_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-1000, old_palf_end_scn));
  auto old_info = make_ls_info(1001, old_standby_sync_scn, old_palf_end_scn, now - 123);
  ObMAProtectionLevelAutoSwitchHelper::ObLSStandbySyncScnInfo new_info;
  ASSERT_SUCCESS(helper_.get_new_ls_standby_sync_info_(
      ObLSID(1001), old_info, invalid_res, new_info, now));
  ASSERT_EQ(old_info.ls_id_, new_info.ls_id_);
  ASSERT_EQ(old_info.standby_sync_scn_, new_info.standby_sync_scn_);
  ASSERT_EQ(old_info.palf_end_scn_, new_info.palf_end_scn_);
  ASSERT_EQ(old_info.ls_add_ts_, new_info.ls_add_ts_);

  ObMAProtectionLevelAutoSwitchHelper::ObLSStandbySyncScnInfo empty_old_info;
  ObMAProtectionLevelAutoSwitchHelper::ObLSStandbySyncScnInfo empty_new_info;
  ASSERT_SUCCESS(helper_.get_new_ls_standby_sync_info_(
      ObLSID(1002), empty_old_info, invalid_res, empty_new_info, now));
  ASSERT_EQ(ObLSID(1002), empty_new_info.ls_id_);
  ASSERT_FALSE(empty_new_info.standby_sync_scn_.is_valid());
  ASSERT_FALSE(empty_new_info.palf_end_scn_.is_valid());
  ASSERT_EQ(now, empty_new_info.ls_add_ts_);
}

// Verify array update refreshes existing LS and appends newly discovered LS.
TEST_F(TestMAProtectionLevelAutoSwitchHelper, update_sync_info_array_updates_old_and_adds_new_ls)
{
  const int64_t now = ObTimeUtility::current_time();
  const int64_t old_ts = now - 120 * 1000 * 1000;
  SCN old_standby_sync_scn;
  SCN old_palf_end_scn;
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-80 * 1000 * 1000, old_standby_sync_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-70 * 1000 * 1000, old_palf_end_scn));
  auto old_info = make_ls_info(1001, old_standby_sync_scn, old_palf_end_scn, old_ts);
  ASSERT_SUCCESS(helper_.ls_standby_sync_scn_info_array_.push_back(old_info));

  ObArray<ObLSStatusInfo> ls_status_array;
  ASSERT_SUCCESS(ls_status_array.push_back(make_ls_status(1001)));
  ASSERT_SUCCESS(ls_status_array.push_back(make_ls_status(1002)));

  ObArray<int> return_code;
  ASSERT_SUCCESS(return_code.push_back(OB_SUCCESS));
  ASSERT_SUCCESS(return_code.push_back(OB_TIMEOUT));

  SCN res1_palf_sync_scn;
  SCN res1_standby_sync_scn;
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-30 * 1000, res1_palf_sync_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-20 * 1000, res1_standby_sync_scn));
  ObGetLSStandbySyncScnRes res1;
  ASSERT_SUCCESS(make_res(1001, res1_palf_sync_scn, res1_standby_sync_scn, res1));
  ObGetLSStandbySyncScnRes invalid_res2;
  ObArray<const ObGetLSStandbySyncScnRes *> res_array;
  ASSERT_SUCCESS(res_array.push_back(&res1));
  ASSERT_SUCCESS(res_array.push_back(&invalid_res2));
  ObGetLSStandbySyncScnArg arg1;
  ObGetLSStandbySyncScnArg arg2;
  ASSERT_SUCCESS(make_arg(1001, arg1));
  ASSERT_SUCCESS(make_arg(1002, arg2));
  ObArray<ObGetLSStandbySyncScnArg> args;
  ASSERT_SUCCESS(args.push_back(arg1));
  ASSERT_SUCCESS(args.push_back(arg2));

  ASSERT_SUCCESS(helper_.update_sync_info_array_(return_code, args, res_array, ls_status_array));
  ASSERT_EQ(2, helper_.ls_standby_sync_scn_info_array_.count());
  const auto &new_info1 = helper_.ls_standby_sync_scn_info_array_.at(0);
  const auto &new_info2 = helper_.ls_standby_sync_scn_info_array_.at(1);
  ASSERT_EQ(ObLSID(1001), new_info1.ls_id_);
  ASSERT_EQ(res1.get_standby_sync_scn(), new_info1.standby_sync_scn_);
  ASSERT_EQ(res1.get_palf_sync_scn(), new_info1.palf_end_scn_);
  ASSERT_GT(new_info1.ls_add_ts_, now);
  ASSERT_EQ(ObLSID(1002), new_info2.ls_id_);
  ASSERT_FALSE(new_info2.standby_sync_scn_.is_valid());
  ASSERT_FALSE(new_info2.palf_end_scn_.is_valid());
  ASSERT_GT(new_info2.ls_add_ts_, now);

  // Verify LS entries not present in status array are removed from refreshed array.
  helper_.ls_standby_sync_scn_info_array_.reset();
  ASSERT_SUCCESS(helper_.ls_standby_sync_scn_info_array_.push_back(make_ls_info(
      1001, old_standby_sync_scn, old_palf_end_scn, old_ts)));
  ASSERT_SUCCESS(helper_.ls_standby_sync_scn_info_array_.push_back(make_ls_info(
      1002, old_standby_sync_scn, old_palf_end_scn, old_ts)));
  ASSERT_SUCCESS(helper_.ls_standby_sync_scn_info_array_.push_back(make_ls_info(
      1003, old_standby_sync_scn, old_palf_end_scn, old_ts)));

  ObArray<ObLSStatusInfo> ls_status_array2;
  ASSERT_SUCCESS(ls_status_array2.push_back(make_ls_status(1001)));
  ASSERT_SUCCESS(ls_status_array2.push_back(make_ls_status(1003)));

  ObArray<int> return_code2;
  ASSERT_SUCCESS(return_code2.push_back(OB_SUCCESS));

  SCN res3_palf_sync_scn;
  SCN res3_standby_sync_scn;
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-15 * 1000, res3_palf_sync_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-5 * 1000, res3_standby_sync_scn));
  ObGetLSStandbySyncScnRes res3;
  ASSERT_SUCCESS(make_res(1003, res3_palf_sync_scn, res3_standby_sync_scn, res3));

  ObArray<const ObGetLSStandbySyncScnRes *> res_array2;
  ASSERT_SUCCESS(res_array2.push_back(&res3));
  ObGetLSStandbySyncScnArg arg3;
  ASSERT_SUCCESS(make_arg(1003, arg3));
  ObArray<ObGetLSStandbySyncScnArg> args2;
  ASSERT_SUCCESS(args2.push_back(arg3));

  ASSERT_SUCCESS(helper_.update_sync_info_array_(return_code2, args2, res_array2, ls_status_array2));
  ASSERT_EQ(2, helper_.ls_standby_sync_scn_info_array_.count());
  const auto &new_info3 = helper_.ls_standby_sync_scn_info_array_.at(0);
  const auto &new_info4 = helper_.ls_standby_sync_scn_info_array_.at(1);
  ASSERT_EQ(ObLSID(1001), new_info3.ls_id_);
  ASSERT_EQ(old_standby_sync_scn, new_info3.standby_sync_scn_);
  ASSERT_EQ(old_palf_end_scn, new_info3.palf_end_scn_);
  ASSERT_EQ(old_ts, new_info3.ls_add_ts_);
  ASSERT_EQ(ObLSID(1003), new_info4.ls_id_);
  ASSERT_EQ(res3.get_standby_sync_scn(), new_info4.standby_sync_scn_);
  ASSERT_EQ(res3.get_palf_sync_scn(), new_info4.palf_end_scn_);
}

TEST_F(TestMAProtectionLevelAutoSwitchHelper, get_ls_sync_res_by_ls_id_validate_arguments)
{
  ObGetLSStandbySyncScnRes out_res;
  ObArray<int> return_code;
  ObArray<ObGetLSStandbySyncScnArg> args;
  ObArray<const ObGetLSStandbySyncScnRes *> res_array;

  ASSERT_EQ(OB_INVALID_ARGUMENT, helper_.get_ls_sync_res_by_ls_id_(
      ObLSID(), return_code, args, res_array, out_res));

  ObGetLSStandbySyncScnArg arg;
  ASSERT_SUCCESS(make_arg(1001, arg));
  ASSERT_SUCCESS(args.push_back(arg));
  ASSERT_SUCCESS(return_code.push_back(OB_SUCCESS));

  ASSERT_EQ(OB_INVALID_ARGUMENT, helper_.get_ls_sync_res_by_ls_id_(
      ObLSID(1001), return_code, args, res_array, out_res));
}

// Verify per-LS downgrade decision with add_ts-first then sync_scn checks.
TEST_F(TestMAProtectionLevelAutoSwitchHelper, need_downgrade_by_ls_logic)
{
  const int64_t now = ObTimeUtility::current_time();
  const int64_t net_timeout_us = 5 * 1000 * 1000;
  // 1) add_ts is checked first:
  // even if standby_sync_scn is fresh, stale add_ts should trigger downgrade.
  SCN normal_recent_standby_sync_scn;
  SCN normal_recent_palf_end_scn;
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-1 * 1000 * 1000, normal_recent_standby_sync_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-1 * 1000 * 1000, normal_recent_palf_end_scn));
  auto normal_recent = make_ls_info(
      1001, normal_recent_standby_sync_scn, normal_recent_palf_end_scn, now - 1 * 1000 * 1000);
  // add_ts and standby_sync_scn are both fresh, no downgrade.
  ASSERT_FALSE(helper_.need_downgrade_by_ls_(normal_recent, now, net_timeout_us));

  auto stale_add_ts_fresh_sync = make_ls_info(
      1002, normal_recent_standby_sync_scn, normal_recent_palf_end_scn, now - 10 * 1000 * 1000);
  // add_ts is stale, so downgrade before checking standby_sync_scn freshness.
  ASSERT_TRUE(helper_.need_downgrade_by_ls_(stale_add_ts_fresh_sync, now, net_timeout_us));

  // 2) standby_sync_scn is checked after add_ts:
  // when add_ts is fresh, stale standby_sync_scn should still trigger downgrade.
  SCN normal_old_standby_sync_scn;
  SCN normal_old_palf_end_scn;
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-10 * 1000 * 1000, normal_old_standby_sync_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-10 * 1000 * 1000, normal_old_palf_end_scn));
  auto normal_old = make_ls_info(
      1003, normal_old_standby_sync_scn, normal_old_palf_end_scn, now - 1 * 1000 * 1000);
  // add_ts is fresh, then stale standby_sync_scn causes downgrade.
  ASSERT_TRUE(helper_.need_downgrade_by_ls_(normal_old, now, net_timeout_us));

  auto invalid_recent = make_ls_info(1004, SCN::invalid_scn(), SCN::invalid_scn(), now - 1 * 1000 * 1000);
  // invalid standby_sync_scn with fresh add_ts, no downgrade.
  ASSERT_FALSE(helper_.need_downgrade_by_ls_(invalid_recent, now, net_timeout_us));

  auto invalid_old = make_ls_info(1005, SCN::invalid_scn(), SCN::invalid_scn(), now - 10 * 1000 * 1000);
  // stale add_ts with invalid standby_sync_scn should trigger downgrade.
  ASSERT_TRUE(helper_.need_downgrade_by_ls_(invalid_old, now, net_timeout_us));
}

// Verify per-LS upgrade decision for freshness and SCN validity.
TEST_F(TestMAProtectionLevelAutoSwitchHelper, can_upgrade_by_ls_logic)
{
  const int64_t now = ObTimeUtility::current_time();
  const int64_t net_timeout_us = 5 * 1000 * 1000;
  SCN normal_recent_standby_sync_scn;
  SCN normal_recent_palf_end_scn;
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-1 * 1000 * 1000, normal_recent_standby_sync_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-1 * 1000 * 1000, normal_recent_palf_end_scn));
  auto normal_recent = make_ls_info(
      1001, normal_recent_standby_sync_scn, normal_recent_palf_end_scn, now - 1 * 1000 * 1000);
  // < net_timeout_us, so can upgrade
  ASSERT_TRUE(helper_.can_upgrade_by_ls_(normal_recent, now, net_timeout_us));

  SCN normal_old_standby_sync_scn;
  SCN normal_old_palf_end_scn;
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-10 * 1000 * 1000, normal_old_standby_sync_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-10 * 1000 * 1000, normal_old_palf_end_scn));
  auto normal_old = make_ls_info(
      1002, normal_old_standby_sync_scn, normal_old_palf_end_scn, now - 10 * 1000 * 1000);
  // > net_timeout_us, so can't upgrade
  ASSERT_FALSE(helper_.can_upgrade_by_ls_(normal_old, now, net_timeout_us));

  auto invalid = make_ls_info(1003, SCN::invalid_scn(), SCN::invalid_scn(), now - 1 * 1000 * 1000);
  // invalid, so can't upgrade
  ASSERT_FALSE(helper_.can_upgrade_by_ls_(invalid, now, net_timeout_us));
}

// Verify public downgrade interface behavior with init window and LS states.
TEST_F(TestMAProtectionLevelAutoSwitchHelper, need_downgrade_interface_logic)
{
  const int64_t net_timeout_us = 5 * 1000 * 1000;
  const int64_t now = ObTimeUtility::current_time();
  ASSERT_FALSE(helper_.need_downgrade(net_timeout_us));

  helper_.ls_standby_sync_scn_info_array_.reset();
  SCN need_downgrade_standby_sync_scn;
  SCN need_downgrade_palf_end_scn;
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-20 * 1000 * 1000, need_downgrade_standby_sync_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-20 * 1000 * 1000, need_downgrade_palf_end_scn));
  ASSERT_SUCCESS(helper_.ls_standby_sync_scn_info_array_.push_back(
      make_ls_info(1001, need_downgrade_standby_sync_scn, need_downgrade_palf_end_scn,
          now - 20 * 1000 * 1000)));
  ASSERT_TRUE(helper_.need_downgrade(net_timeout_us));

  helper_.ls_standby_sync_scn_info_array_.reset();
  SCN no_need_downgrade_standby_sync_scn;
  SCN no_need_downgrade_palf_end_scn;
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-1 * 1000 * 1000, no_need_downgrade_standby_sync_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-1 * 1000 * 1000, no_need_downgrade_palf_end_scn));
  ASSERT_SUCCESS(helper_.ls_standby_sync_scn_info_array_.push_back(
      make_ls_info(1002, no_need_downgrade_standby_sync_scn, no_need_downgrade_palf_end_scn,
          now - 1 * 1000 * 1000)));
  ASSERT_FALSE(helper_.need_downgrade(net_timeout_us));

  // Multi-LS case: one stale stream among fresh streams should still trigger downgrade.
  helper_.ls_standby_sync_scn_info_array_.reset();
  SCN multi_fresh1_standby_sync_scn;
  SCN multi_fresh1_palf_end_scn;
  SCN multi_stale_standby_sync_scn;
  SCN multi_stale_palf_end_scn;
  SCN multi_fresh2_standby_sync_scn;
  SCN multi_fresh2_palf_end_scn;
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-1 * 1000 * 1000, multi_fresh1_standby_sync_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-1 * 1000 * 1000, multi_fresh1_palf_end_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-20 * 1000 * 1000, multi_stale_standby_sync_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-20 * 1000 * 1000, multi_stale_palf_end_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-2 * 1000 * 1000, multi_fresh2_standby_sync_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-2 * 1000 * 1000, multi_fresh2_palf_end_scn));
  ASSERT_SUCCESS(helper_.ls_standby_sync_scn_info_array_.push_back(
      make_ls_info(1001, multi_fresh1_standby_sync_scn, multi_fresh1_palf_end_scn,
          now - 1 * 1000 * 1000)));
  ASSERT_SUCCESS(helper_.ls_standby_sync_scn_info_array_.push_back(
      make_ls_info(1002, multi_stale_standby_sync_scn, multi_stale_palf_end_scn,
          now - 20 * 1000 * 1000)));
  ASSERT_SUCCESS(helper_.ls_standby_sync_scn_info_array_.push_back(
      make_ls_info(1003, multi_fresh2_standby_sync_scn, multi_fresh2_palf_end_scn,
          now - 2 * 1000 * 1000)));
  ASSERT_TRUE(helper_.need_downgrade(net_timeout_us));

  // Multi-LS case: all streams are fresh, no downgrade.
  helper_.ls_standby_sync_scn_info_array_.reset();
  ASSERT_SUCCESS(helper_.ls_standby_sync_scn_info_array_.push_back(
      make_ls_info(2001, multi_fresh1_standby_sync_scn, multi_fresh1_palf_end_scn,
          now - 1 * 1000 * 1000)));
  ASSERT_SUCCESS(helper_.ls_standby_sync_scn_info_array_.push_back(
      make_ls_info(2002, multi_fresh2_standby_sync_scn, multi_fresh2_palf_end_scn,
          now - 2 * 1000 * 1000)));
  ASSERT_FALSE(helper_.need_downgrade(net_timeout_us));
}

// Verify public upgrade interface behavior and fail-check timestamp semantics.
TEST_F(TestMAProtectionLevelAutoSwitchHelper, can_upgrade_interface_logic)
{
  const int64_t net_timeout_us = 5 * 1000 * 1000;
  const int64_t health_check_time_us = 60 * 1000 * 1000;
  const int64_t now = ObTimeUtility::current_time();
  helper_.ls_standby_sync_scn_info_array_.reset();
  SCN cannot_upgrade_standby_sync_scn;
  SCN cannot_upgrade_palf_end_scn;
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-20 * 1000 * 1000, cannot_upgrade_standby_sync_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-20 * 1000 * 1000, cannot_upgrade_palf_end_scn));
  ASSERT_SUCCESS(helper_.ls_standby_sync_scn_info_array_.push_back(
      make_ls_info(1001, cannot_upgrade_standby_sync_scn, cannot_upgrade_palf_end_scn,
          now - 20 * 1000 * 1000)));
  ASSERT_FALSE(helper_.can_upgrade(net_timeout_us, health_check_time_us));
  ASSERT_GT(helper_.last_upgrade_check_fail_ts_, 0);

  helper_.ls_standby_sync_scn_info_array_.reset();
  SCN can_upgrade_standby_sync_scn;
  SCN can_upgrade_palf_end_scn;
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-1 * 1000 * 1000, can_upgrade_standby_sync_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-1 * 1000 * 1000, can_upgrade_palf_end_scn));
  ASSERT_SUCCESS(helper_.ls_standby_sync_scn_info_array_.push_back(
      make_ls_info(1002, can_upgrade_standby_sync_scn, can_upgrade_palf_end_scn,
          now - 1 * 1000 * 1000)));
  helper_.last_upgrade_check_fail_ts_ = ObTimeUtility::current_time() - 61 * 1000 * 1000;
  ASSERT_TRUE(helper_.can_upgrade(net_timeout_us, health_check_time_us));

  helper_.last_upgrade_check_fail_ts_ = OB_INVALID_TIMESTAMP;
  ASSERT_FALSE(helper_.can_upgrade(net_timeout_us, health_check_time_us));
  ASSERT_GT(helper_.last_upgrade_check_fail_ts_, 0);

  // Multi-LS case: if one stream cannot upgrade, overall upgrade should fail.
  helper_.ls_standby_sync_scn_info_array_.reset();
  SCN upgrade_multi_fresh1_standby_sync_scn;
  SCN upgrade_multi_fresh1_palf_end_scn;
  SCN upgrade_multi_stale_standby_sync_scn;
  SCN upgrade_multi_stale_palf_end_scn;
  SCN upgrade_multi_fresh2_standby_sync_scn;
  SCN upgrade_multi_fresh2_palf_end_scn;
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-1 * 1000 * 1000, upgrade_multi_fresh1_standby_sync_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-1 * 1000 * 1000, upgrade_multi_fresh1_palf_end_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-20 * 1000 * 1000, upgrade_multi_stale_standby_sync_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-20 * 1000 * 1000, upgrade_multi_stale_palf_end_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-2 * 1000 * 1000, upgrade_multi_fresh2_standby_sync_scn));
  ASSERT_SUCCESS(make_scn_from_now_delta_us(-2 * 1000 * 1000, upgrade_multi_fresh2_palf_end_scn));
  ASSERT_SUCCESS(helper_.ls_standby_sync_scn_info_array_.push_back(
      make_ls_info(3001, upgrade_multi_fresh1_standby_sync_scn, upgrade_multi_fresh1_palf_end_scn,
          now - 1 * 1000 * 1000)));
  ASSERT_SUCCESS(helper_.ls_standby_sync_scn_info_array_.push_back(
      make_ls_info(3002, upgrade_multi_stale_standby_sync_scn, upgrade_multi_stale_palf_end_scn,
          now - 20 * 1000 * 1000)));
  ASSERT_SUCCESS(helper_.ls_standby_sync_scn_info_array_.push_back(
      make_ls_info(3003, upgrade_multi_fresh2_standby_sync_scn, upgrade_multi_fresh2_palf_end_scn,
          now - 2 * 1000 * 1000)));
  helper_.last_upgrade_check_fail_ts_ = ObTimeUtility::current_time() - 61 * 1000 * 1000;
  ASSERT_FALSE(helper_.can_upgrade(net_timeout_us, health_check_time_us));

  // Multi-LS case: all streams are healthy and fail timestamp is old enough.
  helper_.ls_standby_sync_scn_info_array_.reset();
  ASSERT_SUCCESS(helper_.ls_standby_sync_scn_info_array_.push_back(
      make_ls_info(4001, upgrade_multi_fresh1_standby_sync_scn, upgrade_multi_fresh1_palf_end_scn,
          now - 1 * 1000 * 1000)));
  ASSERT_SUCCESS(helper_.ls_standby_sync_scn_info_array_.push_back(
      make_ls_info(4002, upgrade_multi_fresh2_standby_sync_scn, upgrade_multi_fresh2_palf_end_scn,
          now - 2 * 1000 * 1000)));
  helper_.last_upgrade_check_fail_ts_ = ObTimeUtility::current_time() - 61 * 1000 * 1000;
  ASSERT_TRUE(helper_.can_upgrade(net_timeout_us, health_check_time_us));
}

} // namespace standby
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
