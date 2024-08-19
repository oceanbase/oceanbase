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
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#define private public
#define protected public
#include "src/storage/tx/ob_trans_part_ctx.h"
#include "src/storage/tx/ob_tx_msg.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction;
using namespace obrpc;
namespace unittest
{

class TestObStandbyReadTransfer : public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
};

class MockLockForReadFunctor
{
public :
  MockLockForReadFunctor(const int64_t snapshot) :
    snapshot(snapshot), can_read(false),
    trans_version(OB_INVALID_TIMESTAMP)
  {}
  ~MockLockForReadFunctor() {}
  int64_t snapshot;
  bool can_read;
  int64_t trans_version;
};

class MockObPartTransCtx : public transaction::ObPartTransCtx
{
public :
  MockObPartTransCtx(const share::ObLSID &ls_id)
  {
    default_init_();
    ls_id_ = ls_id;
    lastest_snapshot_.reset();
    standby_part_collected_.reset();
    is_inited_ = true;
  }
  ~MockObPartTransCtx() {}
  int check_for_standby(const SCN &snapshot,
                        bool &can_read,
                        SCN &trans_version)
  {
    int ret = OB_ERR_SHARED_LOCK_CONFLICT;
    SCN min_snapshot = SCN::max_scn();
    ObStateInfo tmp_state_info;
    // for all parts has been prepared
    ObTxState state = ObTxState::PREPARE;
    SCN version = SCN::min_scn();
    int count = state_info_array_.count();
    ARRAY_FOREACH_NORET(state_info_array_, i) {
      tmp_state_info = state_info_array_.at(i);
      min_snapshot = MIN(tmp_state_info.snapshot_version_, min_snapshot);
      if (tmp_state_info.state_ != ObTxState::PREPARE) {
        state = tmp_state_info.state_;
      }
      switch (tmp_state_info.state_) {
        case ObTxState::UNKNOWN:
          break;
        case ObTxState::INIT:
        case ObTxState::REDO_COMPLETE:
        case ObTxState::PREPARE: {
          if (tmp_state_info.version_ > snapshot) {
            can_read = false;
            trans_version.set_min();
            ret = OB_SUCCESS;
          } else {
            version = MAX(version, tmp_state_info.version_);
          }
          break;
        }
        case ObTxState::ABORT: {
          can_read = false;
          trans_version.set_min();
          ret = OB_SUCCESS;
          break;
        }
        case ObTxState::COMMIT:
        case ObTxState::CLEAR: {
          if (tmp_state_info.version_ <= snapshot) {
            can_read = true;
          } else {
            can_read = false;
          }
          trans_version = tmp_state_info.version_;
          ret = OB_SUCCESS;
          break;
        }
        default:
          ret = OB_ERR_UNEXPECTED;
      }
    }
    if (count != 0 && OB_ERR_SHARED_LOCK_CONFLICT == ret && state == ObTxState::PREPARE && version <= snapshot) {
      can_read = true;
      trans_version = version;
      ret = OB_SUCCESS;
    }
    if (count == 0 || (OB_ERR_SHARED_LOCK_CONFLICT == ret && min_snapshot < snapshot)) {
      if (REACH_TIME_INTERVAL(100 * 1000)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = build_and_post_ask_state_msg(snapshot))) {
          TRANS_LOG(WARN, "ask state from coord fail", K(ret), K(snapshot), KPC(this));
        }
      }
    }
    TRANS_LOG(INFO, "check for standby", K(ret), K(can_read), K(trans_version), KPC(this));
    return ret;
  }

  int build_and_post_ask_state_msg(const SCN &snapshot)
  {
    int ret = OB_SUCCESS;
    if (is_root()) {
      build_and_post_collect_state_msg(snapshot);
    }
    return ret;
  }

  int handle_trans_ask_state(const SCN &snapshot, ObAskStateRespMsg &resp)
  {
    int ret = OB_SUCCESS;
    CtxLockGuard guard(lock_);

    build_and_post_collect_state_msg(snapshot);

    if (OB_FAIL(resp.state_info_array_.assign(state_info_array_))) {
      TRANS_LOG(WARN, "build ObAskStateRespMsg fail", K(ret), K(snapshot), KPC(this));
    }

    TRANS_LOG(INFO, "handle trans ask state", K(ret), K(resp), KPC(this));
    return ret;
  }

  void build_and_post_collect_state_msg(const SCN &snapshot)
  {
    int ret = OB_SUCCESS;

    if (state_info_array_.empty() && OB_FAIL(set_state_info_array_())) {
      TRANS_LOG(WARN, "merge participants fail", K(ret));
    }

    TRANS_LOG(INFO, "build and post collect state", K(ret), K(state_info_array_), K(lastest_snapshot_));
  }
};

TEST_F(TestObStandbyReadTransfer, trans_check_for_standby_transfer)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  SCN snapshot;
  snapshot.convert_for_tx(100);
  SCN compute_prepare_version;
  SCN max_decided_scn;
  max_decided_scn.convert_for_tx(10);
  bool can_read = false;
  SCN trans_version = SCN::min_scn();
  ObStateInfo state_info;
  ObAskStateRespMsg resp;

  share::ObLSID coord_ls = share::ObLSID(1);
  share::ObLSID part1_ls = share::ObLSID(1001);
  share::ObLSID part2_ls = share::ObLSID(1002);
  share::ObLSID part3_ls = share::ObLSID(1003);

  MockObPartTransCtx coord(coord_ls);
  MockObPartTransCtx part1(part1_ls), part2(part2_ls), part3(part3_ls);

  ObTxCommitParts parts;
  ASSERT_EQ(OB_SUCCESS, parts.push_back(ObTxExecPart(coord_ls, coord.epoch_, 0)));
  ASSERT_EQ(OB_SUCCESS, parts.push_back(ObTxExecPart(part1_ls, part1.epoch_, 0)));
  ASSERT_EQ(OB_SUCCESS, parts.push_back(ObTxExecPart(part2_ls, part2.epoch_, 0)));
  ASSERT_EQ(OB_SUCCESS, parts.push_back(ObTxExecPart(part3_ls, part3.epoch_, 0)));
  coord.set_2pc_participants_(parts);

  part1.set_2pc_upstream_(coord_ls);
  part2.set_2pc_upstream_(coord_ls);
  part3.set_2pc_upstream_(coord_ls);

  share::ObLSID part_transfer_ls = share::ObLSID(1004);
  MockObPartTransCtx transfer_part(part_transfer_ls);
  transfer_part.set_2pc_upstream_(part1_ls);
  ASSERT_EQ(OB_SUCCESS, transfer_part.exec_info_.transfer_parts_.push_back(ObTxExecPart(part1_ls, -1, 1)));

  ObTxCommitParts transfer_parts;
  ASSERT_EQ(OB_SUCCESS, transfer_parts.push_back(ObTxExecPart(part_transfer_ls, -1, 1)));
  part1.set_2pc_participants_(transfer_parts);

  TRANS_LOG(INFO, "test1:OB_ERR_SHARED_LOCK_CONFLICT with unknown prepare version");
  state_info.snapshot_version_ = snapshot;

  coord.set_downstream_state(ObTxState::PREPARE);
  coord.exec_info_.prepare_version_.convert_for_tx(10);
  part1.set_downstream_state(ObTxState::PREPARE);
  part1.exec_info_.prepare_version_.convert_for_tx(20);
  part2.set_downstream_state(ObTxState::PREPARE);
  part2.exec_info_.prepare_version_.convert_for_tx(30);
  part3.set_downstream_state(ObTxState::PREPARE);
  part3.exec_info_.prepare_version_.convert_for_tx(40);

  transfer_part.set_downstream_state(ObTxState::PREPARE);
  transfer_part.exec_info_.prepare_version_.convert_for_tx(50);

  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_ask_state(snapshot, resp));

  ObCollectStateMsg collect_state_req;
  ObCollectStateRespMsg collect_state_resp;
  collect_state_req.check_info_ = coord.state_info_array_.at(0).check_info_;
  collect_state_resp.sender_ = coord_ls;
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state(collect_state_resp, collect_state_req));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(collect_state_resp));
  ASSERT_EQ(4, coord.state_info_array_.count());
  collect_state_resp.transfer_parts_.reset();

  collect_state_req.check_info_ = coord.state_info_array_.at(1).check_info_;
  collect_state_resp.sender_ = part1_ls;
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_collect_state(collect_state_resp, collect_state_req));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(collect_state_resp));
  ASSERT_EQ(5, coord.state_info_array_.count());
  collect_state_resp.transfer_parts_.reset();

  collect_state_req.check_info_ = coord.state_info_array_.at(2).check_info_;
  collect_state_resp.sender_ = part2_ls;
  ASSERT_EQ(OB_SUCCESS, part2.handle_trans_collect_state(collect_state_resp, collect_state_req));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(collect_state_resp));
  ASSERT_EQ(5, coord.state_info_array_.count());

  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_ask_state(snapshot, resp));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_ask_state_resp(resp));
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version));

  collect_state_req.check_info_ = coord.state_info_array_.at(3).check_info_;
  collect_state_resp.sender_ = part3_ls;
  ASSERT_EQ(OB_SUCCESS, part3.handle_trans_collect_state(collect_state_resp, collect_state_req));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(collect_state_resp));
  ASSERT_EQ(5, coord.state_info_array_.count());

  collect_state_req.check_info_ = coord.state_info_array_.at(4).check_info_;
  collect_state_resp.sender_ = part_transfer_ls;
  ASSERT_EQ(OB_SUCCESS, transfer_part.handle_trans_collect_state(collect_state_resp, collect_state_req));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(collect_state_resp));
  ASSERT_EQ(5, coord.state_info_array_.count());

  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_ask_state(snapshot, resp));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_ask_state_resp(resp));
  ASSERT_EQ(OB_SUCCESS, part1.check_for_standby(snapshot, can_read, trans_version));
  ASSERT_EQ(true, can_read);
  compute_prepare_version.convert_for_sql(50);
  ASSERT_EQ(compute_prepare_version, trans_version);
}


}//end of unittest
}//end of oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_standby_read_transfer.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
