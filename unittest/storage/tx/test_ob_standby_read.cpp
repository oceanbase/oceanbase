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

class TestObStandbyRead : public ::testing::Test
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
    trans_version(OB_INVALID_TIMESTAMP),
    is_determined_state(false)
  {}
  ~MockLockForReadFunctor() {}
  int64_t snapshot;
  bool can_read;
  int64_t trans_version;
  bool is_determined_state;
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
  int check_for_standby(const SCN &snapshot, bool &can_read,
                        SCN &trans_version, bool &is_determined_state)
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
            is_determined_state = false;
            ret = OB_SUCCESS;
          } else {
            version = MAX(version, tmp_state_info.version_);
          }
          break;
        }
        case ObTxState::ABORT: {
          can_read = false;
          trans_version.set_min();
          is_determined_state = true;
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
          is_determined_state = true;
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
      is_determined_state = true;
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
    TRANS_LOG(INFO, "check for standby", K(ret), KPC(this));
    return ret;
  }
  int build_and_post_ask_state_msg(const SCN &snapshot)
  {
    int ret = OB_SUCCESS;
    if (is_root()) {
      if (snapshot > lastest_snapshot_) {
        build_and_post_collect_state_msg(snapshot);
      }
    }
    return ret;
  }
  int handle_trans_ask_state(const SCN &snapshot, ObAskStateRespMsg &resp)
  {
    int ret = OB_SUCCESS;
    CtxLockGuard guard(lock_);
    if (snapshot > lastest_snapshot_) {
      build_and_post_collect_state_msg(snapshot);
    } else if (snapshot == lastest_snapshot_ && standby_part_collected_.num_members() != state_info_array_.count() -1) {
      build_and_post_collect_state_msg(snapshot);
    }
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
    if (OB_SUCC(ret)) {
      lastest_snapshot_ = snapshot;
      standby_part_collected_.clear_all();
    }
    TRANS_LOG(INFO, "build and post collect state", K(ret), K(state_info_array_), K(lastest_snapshot_));
  }
  int handle_trans_collect_state(ObStateInfo &state_info, const share::SCN &max_decided_scn)
  {
    int ret = OB_SUCCESS;
    CtxLockGuard guard(lock_);
    state_info.ls_id_ = ls_id_;
    state_info.state_ = exec_info_.state_;
    switch (state_info.state_) {
      case ObTxState::INIT:
      case ObTxState::REDO_COMPLETE: {
        state_info.version_ = max_decided_scn;
        break;
      }
      case ObTxState::ABORT: {
        state_info.version_.set_min();
        break;
      }
      case ObTxState::COMMIT:
      case ObTxState::PRE_COMMIT:
      case ObTxState::CLEAR: {
        state_info.version_ = ctx_tx_data_.get_commit_version();
        break;
      }
      case ObTxState::PREPARE: {
        state_info.version_ = exec_info_.prepare_version_;
        break;
      }
      case ObTxState::UNKNOWN:
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
    }
    TRANS_LOG(INFO, "handle trans collect state", K(ret), K(state_info), KPC(this));
    return ret;
  }
  int handle_trans_collect_state_resp(const ObStateInfo &state_info)
  {
    int ret = OB_SUCCESS;
    CtxLockGuard guard(lock_);
    bool is_contain = false;
    int i = 0;
    for (; i<state_info_array_.count() && !is_contain; i++) {
      is_contain = state_info_array_.at(i).ls_id_ == state_info.ls_id_;
    }
    if (!is_contain) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "state info array has wrong participiants", K(ret), K(state_info));
    } else {
      state_info_array_.at(i-1) = state_info;
    }
    if (OB_SUCC(ret)) {
      standby_part_collected_.add_member(i-1);
    }
    TRANS_LOG(INFO, "handle trans collect state resp", K(ret), K(state_info), K(state_info_array_));
    return ret;
  }
};

TEST_F(TestObStandbyRead, trans_check_for_standby)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  SCN snapshot;
  snapshot.convert_for_tx(100);
  SCN max_decided_scn;
  max_decided_scn.convert_for_tx(10);
  bool can_read = false;
  SCN trans_version = SCN::min_scn();
  bool is_determined_state = false;
  ObStateInfo state_info;
  ObAskStateRespMsg resp;
  share::ObLSID coord_ls = share::ObLSID(1);
  share::ObLSID part1_ls = share::ObLSID(1001);
  share::ObLSID part2_ls = share::ObLSID(1002);
  share::ObLSID part3_ls = share::ObLSID(1003);
  MockObPartTransCtx coord(coord_ls);
  MockObPartTransCtx part1(part1_ls), part2(part2_ls), part3(part3_ls);
  share::ObLSArray parts;
  ASSERT_EQ(OB_SUCCESS, parts.push_back(coord_ls));
  ASSERT_EQ(OB_SUCCESS, parts.push_back(part1_ls));
  ASSERT_EQ(OB_SUCCESS, parts.push_back(part2_ls));
  ASSERT_EQ(OB_SUCCESS, parts.push_back(part3_ls));
  part1.set_2pc_upstream_(coord_ls);
  part2.set_2pc_upstream_(coord_ls);
  part3.set_2pc_upstream_(coord_ls);
  coord.set_2pc_participants_(parts);


  TRANS_LOG(INFO, "test1:OB_ERR_SHARED_LOCK_CONFLICT with unknown prepare version");
  state_info.snapshot_version_ = snapshot;
  coord.set_downstream_state(ObTxState::PREPARE);
  coord.exec_info_.prepare_version_.convert_for_tx(10);
  part1.set_downstream_state(ObTxState::PREPARE);
  part1.exec_info_.prepare_version_.set_min();
  part2.set_downstream_state(ObTxState::INIT);
  part3.set_downstream_state(ObTxState::UNKNOWN);
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_ask_state(snapshot, resp));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_ask_state_resp(resp));
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  ASSERT_EQ(OB_SUCCESS, part2.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  ASSERT_EQ(OB_SUCCESS, part3.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  resp.state_info_array_.reset();
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_ask_state(snapshot, resp));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_ask_state_resp(resp));
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));

  TRANS_LOG(INFO, "test2:can read = false with upper prepare version");
  coord.set_downstream_state(ObTxState::PREPARE);
  coord.exec_info_.prepare_version_.convert_for_tx(10);
  part1.set_downstream_state(ObTxState::PREPARE);
  part1.exec_info_.prepare_version_.convert_for_tx(200);
  part2.set_downstream_state(ObTxState::INIT);
  part3.set_downstream_state(ObTxState::UNKNOWN);
  can_read = true;
  part1.state_info_array_.reset();
  coord.state_info_array_.reset();
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_ask_state(snapshot, resp));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_ask_state_resp(resp));
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  ASSERT_EQ(OB_SUCCESS, part2.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  ASSERT_EQ(OB_SUCCESS, part3.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  resp.state_info_array_.reset();
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_ask_state(snapshot, resp));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_ask_state_resp(resp));
  ASSERT_EQ(OB_SUCCESS, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(false, can_read);

  TRANS_LOG(INFO, "test3:can read = true with commit");
  snapshot.convert_for_tx(200);
  state_info.snapshot_version_ = snapshot;
  coord.set_downstream_state(ObTxState::PREPARE);
  coord.exec_info_.prepare_version_.convert_for_tx(10);
  part1.set_downstream_state(ObTxState::PREPARE);
  part1.exec_info_.prepare_version_.convert_for_tx(90);
  part2.set_downstream_state(ObTxState::COMMIT);
  ObTxData part2_tx_data;
  ObSliceAlloc slice_allocator;
  part2_tx_data.ref_cnt_ = 1000;
  part2_tx_data.slice_allocator_ = &slice_allocator;
  part2.ctx_tx_data_.tx_data_guard_.init(&part2_tx_data);
  part2.ctx_tx_data_.tx_data_guard_.tx_data()->commit_version_.convert_for_tx(90);
  part3.set_downstream_state(ObTxState::UNKNOWN);
  can_read = false;
  part1.state_info_array_.reset();
  coord.state_info_array_.reset();
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_ask_state(snapshot, resp));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_ask_state_resp(resp));
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  ASSERT_EQ(OB_SUCCESS, part2.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  ASSERT_EQ(OB_SUCCESS, part3.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  resp.state_info_array_.reset();
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_ask_state(snapshot, resp));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_ask_state_resp(resp));
  ASSERT_EQ(OB_SUCCESS, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(true, can_read);

  TRANS_LOG(INFO, "test4:can read = false with commit");
  snapshot.convert_for_tx(200);
  state_info.snapshot_version_ = snapshot;
  coord.set_downstream_state(ObTxState::PREPARE);
  coord.exec_info_.prepare_version_.convert_for_tx(10);
  part1.set_downstream_state(ObTxState::PREPARE);
  coord.exec_info_.prepare_version_.convert_for_tx(90);
  part2.set_downstream_state(ObTxState::COMMIT);
  part2.ctx_tx_data_.tx_data_guard_.tx_data()->commit_version_.convert_for_tx(300);
  part3.set_downstream_state(ObTxState::UNKNOWN);
  can_read = true;
  part1.state_info_array_.reset();
  coord.state_info_array_.reset();
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_ask_state(snapshot, resp));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_ask_state_resp(resp));
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  ASSERT_EQ(OB_SUCCESS, part2.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  ASSERT_EQ(OB_SUCCESS, part3.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  resp.state_info_array_.reset();
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_ask_state(snapshot, resp));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_ask_state_resp(resp));
  ASSERT_EQ(OB_SUCCESS, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(false, can_read);

  TRANS_LOG(INFO, "test5:can read = true with all prepare");
  snapshot.convert_for_tx(300);
  state_info.snapshot_version_ = snapshot;
  coord.set_downstream_state(ObTxState::PREPARE);
  coord.exec_info_.prepare_version_.convert_for_tx(10);
  part1.set_downstream_state(ObTxState::PREPARE);
  part1.exec_info_.prepare_version_.convert_for_tx(90);
  part2.set_downstream_state(ObTxState::PREPARE);
  part2.exec_info_.prepare_version_.convert_for_tx(90);
  part3.set_downstream_state(ObTxState::PREPARE);
  part2.exec_info_.prepare_version_.convert_for_tx(100);
  can_read = false;
  part1.state_info_array_.reset();
  coord.state_info_array_.reset();
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_ask_state(snapshot, resp));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_ask_state_resp(resp));
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  ASSERT_EQ(OB_SUCCESS, part2.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  ASSERT_EQ(OB_SUCCESS, part3.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  resp.state_info_array_.reset();
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_ask_state(snapshot, resp));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_ask_state_resp(resp));
  ASSERT_EQ(OB_SUCCESS, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(true, can_read);

  TRANS_LOG(INFO, "test6:can read = false with all prepare");
  snapshot.convert_for_tx(300);
  state_info.snapshot_version_ = snapshot;
  coord.set_downstream_state(ObTxState::PREPARE);
  coord.exec_info_.prepare_version_.convert_for_tx(10);
  part1.set_downstream_state(ObTxState::PREPARE);
  part1.exec_info_.prepare_version_.convert_for_tx(90);
  part2.set_downstream_state(ObTxState::PREPARE);
  part2.exec_info_.prepare_version_.convert_for_tx(90);
  part3.set_downstream_state(ObTxState::PREPARE);
  part2.exec_info_.prepare_version_.convert_for_tx(400);
  can_read = true;
  part1.state_info_array_.reset();
  coord.state_info_array_.reset();
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_ask_state(snapshot, resp));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_ask_state_resp(resp));
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  ASSERT_EQ(OB_SUCCESS, part2.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  ASSERT_EQ(OB_SUCCESS, part3.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  resp.state_info_array_.reset();
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_ask_state(snapshot, resp));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_ask_state_resp(resp));
  ASSERT_EQ(OB_SUCCESS, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(false, can_read);

  TRANS_LOG(INFO, "test7:OB_ERR_SHARED_LOCK_CONFLICT with unknown state");
  snapshot.convert_for_tx(300);
  state_info.snapshot_version_ = snapshot;
  coord.set_downstream_state(ObTxState::PREPARE);
  coord.exec_info_.prepare_version_.convert_for_tx(10);
  part1.set_downstream_state(ObTxState::PREPARE);
  part1.exec_info_.prepare_version_.set_min();
  part2.set_downstream_state(ObTxState::PREPARE);
  part2.exec_info_.prepare_version_.set_min();
  part3.set_downstream_state(ObTxState::UNKNOWN);
  part1.state_info_array_.reset();
  coord.state_info_array_.reset();
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_ask_state(snapshot, resp));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_ask_state_resp(resp));
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  ASSERT_EQ(OB_SUCCESS, part2.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  ASSERT_EQ(OB_SUCCESS, part3.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  resp.state_info_array_.reset();
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_ask_state(snapshot, resp));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_ask_state_resp(resp));
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));

  TRANS_LOG(INFO, "test8:can read = false with abort");
  snapshot.convert_for_tx(300);
  state_info.snapshot_version_ = snapshot;
  coord.set_downstream_state(ObTxState::PREPARE);
  coord.exec_info_.prepare_version_.convert_for_tx(10);
  part1.set_downstream_state(ObTxState::PREPARE);
  part1.exec_info_.prepare_version_.set_min();
  part2.set_downstream_state(ObTxState::PREPARE);
  part2.exec_info_.prepare_version_.set_min();
  part3.set_downstream_state(ObTxState::ABORT);
  part1.state_info_array_.reset();
  coord.state_info_array_.reset();
  can_read = true;
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_ask_state(snapshot, resp));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_ask_state_resp(resp));
  ASSERT_EQ(OB_ERR_SHARED_LOCK_CONFLICT, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  ASSERT_EQ(OB_SUCCESS, part2.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  ASSERT_EQ(OB_SUCCESS, part3.handle_trans_collect_state(state_info, max_decided_scn));
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_collect_state_resp(state_info));
  resp.state_info_array_.reset();
  ASSERT_EQ(OB_SUCCESS, coord.handle_trans_ask_state(snapshot, resp));
  ASSERT_EQ(OB_SUCCESS, part1.handle_trans_ask_state_resp(resp));
  ASSERT_EQ(OB_SUCCESS, part1.check_for_standby(snapshot, can_read, trans_version, is_determined_state));
  ASSERT_EQ(false, can_read);
}


}//end of unittest
}//end of oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char **argv)
{
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_standby_read.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}
