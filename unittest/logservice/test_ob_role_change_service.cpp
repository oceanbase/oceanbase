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
#include "logservice/ob_log_handler.h"
#include "logservice/rcservice/ob_role_change_handler.h"
#include "logservice/rcservice/ob_role_change_service.h"
#include "logservice/replayservice/ob_log_replay_service.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace common;
using namespace palf;
using namespace logservice;
using namespace logservice;

namespace unittest
{
class MockApplyService : public AppendCbWorker
{
public:
  int is_apply_done(const share::ObLSID &id, bool &is_done, LSN &lsn)
  {
    is_done = true;
    lsn = end_lsn_;
    PALF_LOG(INFO, "MockApplyService is_apply_done", K(id), K(is_done), K(lsn));
    return OB_SUCCESS;
  }

  int push_append_cb(const int64_t id, AppendCb *cb, const LSN &lsn)
  {
    UNUSED(id);
    UNUSED(cb);
    UNUSED(lsn);
    return OB_SUCCESS;
  }
  void advance_end_lsn(const LSN &lsn)
  {
    end_lsn_ = lsn;
  }
private:
  LSN end_lsn_;
};

class MockReplayService : public logservice::ObILogReplayService
{
public:
  virtual int is_replay_done(const share::ObLSID &id, const palf::LSN &end_lsn, bool &is_done)
  {
    is_done = end_lsn <= end_lsn_;
    PALF_LOG(INFO, "MockReplayService is_replay_done", K(id), K(is_done), K(end_lsn));
    return OB_SUCCESS;
  }

  virtual int switch_to_follower(const share::ObLSID &id, const palf::LSN &begin_lsn)
  {
    PALF_LOG(INFO, "MockReplayService switch_to_follower", K(id), K(begin_lsn));
    return OB_SUCCESS;
  }

  virtual int switch_to_leader(const share::ObLSID &id)
  {
    PALF_LOG(INFO, "MockReplayService switch_to_leader", K(id));
    return OB_SUCCESS;
  }

  void advance_end_lsn(const LSN &end_lsn)
  {
    end_lsn_ = end_lsn;
  }
private:
  LSN end_lsn_;
};

class MockObLogHandler : public logservice::ObILogHandler
{
public:
  MockObLogHandler() : curr_role_(FOLLOWER),
                       curr_proposal_id_(-1),
                       new_role_(FOLLOWER),
                       new_proposal_id_(-1)
  {}
  virtual bool is_valid() const { return true; }
  virtual int append(const void *buffer,
                     const int64_t nbytes,
                     const share::SCN &ref_scn,
                     const bool need_nonblock,
                     logservice::AppendCb *cb,
                     palf::LSN &lsn,
                     share::SCN &scn)
  {
    UNUSED(need_nonblock);
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(ref_scn);
    UNUSED(cb);
    UNUSED(lsn);
    UNUSED(scn);
    return OB_SUCCESS;
  }
  virtual int get_role(common::ObRole &role, int64_t &proposal_id) const
  {
    if (FOLLOWER == curr_role_) {
      role = curr_role_;
      proposal_id = curr_proposal_id_;
    } else {
      role = new_role_;
      proposal_id = curr_proposal_id_;
    }
    return OB_SUCCESS;
  }
  virtual int prepare_switch_role(common::ObRole &curr_role,
                                  int64_t &curr_proposal_id,
                                  common::ObRole &new_role,
                                  int64_t &new_proposal_id)
  {
    curr_role = curr_role_;
    curr_proposal_id = curr_proposal_id_;
    new_role = new_role_;
    new_proposal_id = new_proposal_id_;
    PALF_LOG(INFO, "MockObLogHandler prepare_switch_role", K(curr_role), K(curr_proposal_id), K(new_role), K(new_proposal_id));
    return OB_SUCCESS;
  }
  virtual void switch_role(const common::ObRole &role, const int64_t proposal_id)
  {
    curr_role_ = role;
    curr_proposal_id_ = proposal_id;
    PALF_LOG(INFO, "MockObLogHandler switch_role", K(role), K(proposal_id));
  }
  virtual int change_leader_to(const common::ObAddr &dst_addr)
  {
    UNUSED(dst_addr);
    return OB_SUCCESS;
  }
  virtual int advance_election_epoch_and_downgrade_priority(const int64_t downgrade_priority_time_us, const char *reason)
  {
    UNUSED(downgrade_priority_time_us, reason);
    curr_role_ = FOLLOWER;
    new_role_ = FOLLOWER;
    new_proposal_id_++;
    return OB_SUCCESS;
  }
  virtual int get_begin_lsn(palf::LSN &begin_lsn) const
  {
    begin_lsn = LSN(1, 0);
    return OB_SUCCESS;
  }
  virtual int get_end_lsn(palf::LSN &end_lsn) const
  {
    end_lsn = LSN(1, 0);
    return OB_SUCCESS;
  }
  int seek(const palf::LSN &start_lsn, palf::PalfBufferIterator &iter)
  {UNUSED(start_lsn); UNUSED(iter); return OB_SUCCESS;};
  int seek(const palf::LSN &start_lsn, const int64_t iterate_size, palf::PalfGroupBufferIterator &iter)
  {UNUSED(start_lsn); UNUSED(iterate_size); UNUSED(iter); return OB_SUCCESS;};
  int seek(const share::SCN &start_scn, palf::PalfGroupBufferIterator &iter)
  {UNUSED(start_scn); UNUSED(iter); return OB_SUCCESS;};
  int set_initial_member_list(const common::ObMemberList &member_list, const int64_t paxos_replica_num)
  {UNUSED(member_list); UNUSED(paxos_replica_num); return OB_SUCCESS;}
  int locate_by_scn_coarsely(const share::SCN &scn, palf::LSN &lsn)
  {UNUSED(scn); UNUSED(lsn); return OB_SUCCESS;}
  int locate_by_lsn_coarsely(const palf::LSN &lsn, share::SCN &scn)
  {UNUSED(scn); UNUSED(lsn); return OB_SUCCESS;}
  int advance_base_lsn(const palf::LSN &lsn)
  {UNUSED(lsn); return OB_SUCCESS;}
  int get_base_lsn(palf::LSN &lsn)
  {UNUSED(lsn); return OB_SUCCESS;}
  int get_base_scn(share::SCN &base_lsn_scn) const
  {UNUSED(base_lsn_scn); return OB_SUCCESS;}
  int get_end_scn(share::SCN &scn) const
  {UNUSED(scn); return OB_SUCCESS;}
  int get_paxos_member_list(common::ObMemberList &member_list, int64_t &paxos_replica_num) const
  {UNUSED(member_list); UNUSED(paxos_replica_num); return OB_SUCCESS;}
  int get_palf_base_info(const palf::LSN &base_lsn, palf::PalfBaseInfo &palf_base_info)
  {
    UNUSED(base_lsn);
    UNUSED(palf_base_info);
    return OB_SUCCESS;
  }
  int is_in_sync(bool &is_log_sync, bool &is_need_rebuild) const
  {
    UNUSED(is_log_sync);
    UNUSED(is_need_rebuild);
    return OB_SUCCESS;
  }
  bool is_sync_enabled() const
  {
    return true;
  }
  bool is_replay_enabled() const
  {
    return true;
  }
  int enable_sync()
  {
    return OB_SUCCESS;
  }
  int disable_sync()
  {
    return OB_SUCCESS;
  }
  int advance_base_lsn_for_rebuild(const palf::PalfBaseInfo &palf_base_info)
  {
    UNUSED(palf_base_info);
    return OB_SUCCESS;
  }
  TO_STRING_KV(K_(curr_role), K_(curr_proposal_id), K_(new_role), K_(new_proposal_id));
  ObRole curr_role_;
  int64_t curr_proposal_id_;
  ObRole new_role_;
  int64_t new_proposal_id_;
  LSN end_lsn_;
};

class TestObRoleChangeService : public ::testing::Test {
public:
  TestObRoleChangeService() {}
  ~TestObRoleChangeService() {}
protected:
  virtual void SetUp()
  {
    auto get_log_handler = [&](const share::ObLSID &ls_id, ObILogHandler *&log_handler) -> int
    {
      log_handler = &log_handler_;
      PALF_LOG(INFO, "get_log_handler", K(ls_id), KP(log_handler), K(log_handler_), KP(&log_handler_));
      return OB_SUCCESS;
    };
    auto get_role_change_handler = [&](const share::ObLSID &ls_id, ObRoleChangeHandler *&role_change_handler) -> int
    {
      role_change_handler = &role_change_handler_;
      PALF_LOG(INFO, "get_role_change_handler", K(ls_id), KP(role_change_handler));
      return OB_SUCCESS;
    };
    role_change_service_.init(&ls_service_, &apply_service_, &replay_service_);
    role_change_service_.start();

  }
  virtual void TearDown()
  {
    role_change_service_.destroy();
  }
  static void SetUpTestCase()
  {
  }
  static void TearDownTestCase()
  {
  }

  void palf_to_leader()
  {
    log_handler_.new_role_ = LEADER;
    log_handler_.new_proposal_id_++;
    PALF_LOG(INFO, "MockObLogHandler palf_to_leader", K(log_handler_), KP(&log_handler_));
  }
  void palf_to_follower()
  {
    log_handler_.new_role_ = FOLLOWER;
    log_handler_.new_proposal_id_++;
    PALF_LOG(INFO, "MockObLogHandler palf_to_follower", K(log_handler_), KP(&log_handler_));
  }

  MockApplyService apply_service_;
  MockReplayService replay_service_;
  ObLSService ls_service_;
  ObRoleChangeService role_change_service_;
  MockObLogHandler log_handler_;
  ObRoleChangeHandler role_change_handler_;
};

TEST_F(TestObRoleChangeService, normal_case)
{
  ObLSID ls_id(1);
  EXPECT_EQ(OB_SUCCESS, role_change_service_.on_role_change(ls_id.id()));
  usleep(10*1000);
  // new_proposal_id_ == curr_proposal_id_, no need change role
  EXPECT_EQ(FOLLOWER, log_handler_.curr_role_);

  palf_to_leader();
  EXPECT_EQ(OB_SUCCESS, role_change_service_.on_role_change(ls_id.id()));
  LSN end_lsn(2, 0);
  usleep(10*1000);
  // before replay service replay done
  EXPECT_EQ(FOLLOWER, log_handler_.curr_role_);
  replay_service_.advance_end_lsn(end_lsn);
  usleep(10 * 1000);
  // follower to leader
  EXPECT_EQ(LEADER, log_handler_.curr_role_);

  palf_to_follower();
  palf_to_leader();
  // switch leader -> follower -> leader
  EXPECT_EQ(OB_SUCCESS, role_change_service_.on_role_change(ls_id.id()));
  usleep(10*1000);
  EXPECT_EQ(LEADER, log_handler_.curr_role_);

  palf_to_follower();
  EXPECT_EQ(OB_SUCCESS, role_change_service_.on_role_change(ls_id.id()));
  usleep(10*1000);
  EXPECT_EQ(FOLLOWER, log_handler_.curr_role_);
  EXPECT_EQ(OB_SUCCESS, role_change_service_.on_role_change(ls_id.id()));
  palf_to_follower();
  usleep(10*1000);
  EXPECT_EQ(FOLLOWER, log_handler_.curr_role_);
}

} // end of unittest
} // end of oceanbase

int main(int argc, char **argv)
{
  system("rm -rf test_ob_role_change_service.*");
  OB_LOGGER.set_file_name("test_ob_role_change_service.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
