// owner: zjf225077
// owner group: log

// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define private public
#include "env/ob_simple_log_cluster_env.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "log/ob_shared_log_garbage_collector.h"
#endif
#undef private

const std::string TEST_NAME = "shared_gc";

using namespace oceanbase::common;
using namespace oceanbase;
using namespace oceanbase::share;
using namespace oceanbase::palf;
namespace oceanbase
{
using namespace logservice;

namespace logservice
{

}

namespace unittest
{

class TestObSimpleSharedLogGC : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleSharedLogGC() :  ObSimpleLogClusterTestEnv()
  {}
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 5;
bool ObSimpleLogClusterTestBase::need_add_arb_server_ = false;
bool ObSimpleLogClusterTestBase::need_shared_storage_ = true;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;

TEST_F(TestObSimpleSharedLogGC, test_get_min_using_scn)
{
  int ret = OB_SUCCESS;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t TIMEOUT_US = 10 * 1000 * 1000L;
  SET_CASE_LOG_FILE(TEST_NAME, "test_get_min_using_scn");
  PALF_LOG(INFO, "begin test_get_min_using_scn", K(id));
  {
    int64_t leader_idx = 0;
    PalfHandleImplGuard leader;
    std::vector<PalfHandleImplGuard*> palf_list;
    EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_mock_election(id, leader_idx, leader));
    EXPECT_EQ(OB_SUCCESS, submit_log(leader, 200, id));
    EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));

    const int64_t b_idx = (leader_idx + 1) % 4;
    const int64_t c_idx = (leader_idx + 2) % 4;
    const int64_t d_idx = (leader_idx + 3) % 4;
    const common::ObAddr a_addr = get_cluster()[leader_idx]->get_addr();
    const common::ObAddr b_addr = get_cluster()[b_idx]->get_addr();
    const common::ObAddr c_addr = get_cluster()[c_idx]->get_addr();
    const common::ObAddr d_addr = get_cluster()[d_idx]->get_addr();
    PalfHandleImplGuard *a_handle = palf_list[leader_idx];
    PalfHandleImplGuard *b_handle = palf_list[b_idx];
    ObSharedLogGarbageCollector *shared_gc = dynamic_cast<ObSimpleLogServer*>(get_cluster()[leader_idx])->log_service_.get_shared_log_gc();
    MockLocCB loc_cb;
    loc_cb.leader_ = a_addr;
    shared_gc->location_cb_ = &loc_cb;

    // 1. dropped ls
    {
      share::ObLSAttr ls_attr;
      ls_attr.id_ = share::ObLSID(id);
      ls_attr.status_ = OB_LS_DROPPED;
      SCN min_using_scn;
      LSN min_using_upper_lsn;

      EXPECT_EQ(OB_SUCCESS, shared_gc->get_min_using_scn_(MTL_ID(), ls_attr, min_using_scn, min_using_upper_lsn));
      EXPECT_EQ(SCN::max_scn(), min_using_scn);
      EXPECT_EQ(LSN(LOG_MAX_LSN_VAL), min_using_upper_lsn);
    }
    // 2. common case
    {
      ObTenantEnv::set_tenant(get_cluster()[leader_idx]->get_tenant_base());
      SCN base_scn;
      std::vector<LSN> ckpt_lsn_list = {LSN(2000), LSN(1000), LSN(3000)};
      std::vector<SCN> ckpt_scn_list;
      base_scn.convert_from_ts(common::ObTimeUtility::current_time());
      ckpt_scn_list.push_back(SCN::plus(base_scn, 10000));
      ckpt_scn_list.push_back(base_scn);
      ckpt_scn_list.push_back(SCN::plus(base_scn, 20000));

      for (int i = 0; i < ckpt_lsn_list.size(); i++) {
        dynamic_cast<ObSimpleLogServer*>(get_cluster()[i])->set_mock_ls_ckpt(\
          id, ckpt_scn_list[i], ckpt_lsn_list[i]);
      }

      share::ObLSAttr ls_attr;
      ls_attr.id_ = share::ObLSID(id);
      ls_attr.status_ = OB_LS_NORMAL;
      SCN min_using_scn;
      LSN min_using_upper_lsn;

      EXPECT_EQ(OB_SUCCESS, shared_gc->get_min_using_scn_(MTL_ID(), ls_attr, min_using_scn, min_using_upper_lsn));
      EXPECT_EQ(min_using_scn, base_scn);
      EXPECT_EQ(min_using_upper_lsn, LSN(0));
    }
    // 3. block_net, migrating
    {
      block_net(leader_idx, b_idx);

      share::ObLSAttr ls_attr;
      ls_attr.id_ = share::ObLSID(id);
      ls_attr.status_ = OB_LS_NORMAL;
      SCN min_using_scn;
      LSN min_using_upper_lsn;

      EXPECT_EQ(OB_EAGAIN, shared_gc->get_min_using_scn_(MTL_ID(), ls_attr, min_using_scn, min_using_upper_lsn));
      unblock_net(leader_idx, b_idx);


      common::ObMember learner(d_addr, 1);
      learner.set_migrating();
      EXPECT_EQ(OB_SUCCESS, leader.palf_handle_impl_->add_learner(learner, TIMEOUT_US));
      EXPECT_EQ(OB_EAGAIN, shared_gc->get_min_using_scn_(MTL_ID(), ls_attr, min_using_scn, min_using_upper_lsn));
    }
    revert_cluster_palf_handle_guard(palf_list);
  }
  delete_paxos_group(id);
  PALF_LOG(INFO, "end test_get_min_using_scn", K(id));
}

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
