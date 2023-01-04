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

#include "logservice/palf_handle_guard.h"
#include "ob_simple_log_cluster_testbase.h"

namespace oceanbase
{
namespace logservice
{
class AppendCb;
class ObReplayStatus;
}
namespace unittest
{
class MockLSAdapter;

#define SET_CASE_LOG_FILE(TEST_NAME, CASE_NAME) \
  const std::string log_file_name = TEST_NAME + "/" + CASE_NAME + ".log";\
  const std::string ele_log_file_name = TEST_NAME + "/" + CASE_NAME + ".election.log";\
  OB_LOGGER.set_file_name(log_file_name.c_str(),\
                          true, false, NULL, \
                          ele_log_file_name.c_str(), NULL);

#define RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME) \
  void *ptr = malloc(SIG_STACK_SIZE); \
  abort_unless(ptr != nullptr); \
  stack_t nss; \
  stack_t oss; \
  bzero(&nss, sizeof(nss)); \
  bzero(&oss, sizeof(oss)); \
  nss.ss_sp = ptr; \
  nss.ss_size = SIG_STACK_SIZE; \
  abort_unless(0 == sigaltstack(&nss, &oss)); \
  DEFER(sigaltstack(&oss, nullptr)); \
  if (OB_SUCCESS != oceanbase::observer::ObSignalHandle::change_signal_mask()) { \
  } \
  ::oceanbase::common::g_redirect_handler = true; \
  oceanbase::palf::election::GLOBAL_INIT_ELECTION_MODULE(); \
  sleep(15); \
  const std::string rm_base_dir_cmd = "rm -rf " + TEST_NAME; \
  const std::string rm_log_cmd = "rm -f ./" + TEST_NAME + "*log*"; \
  const std::string mk_base_dir_cm = "mkdir " + TEST_NAME; \
  system(rm_base_dir_cmd.c_str()); \
  system(rm_log_cmd.c_str()); \
  system(mk_base_dir_cm.c_str()); \
  const std::string log_file_name = TEST_NAME+"/"+TEST_NAME + ".log"; \
  const std::string ele_log_file_name = TEST_NAME+"/"+TEST_NAME + ".election.log"; \
  const std::string rs_log_file_name = TEST_NAME+"/"+TEST_NAME + ".rs.log"; \
  OB_LOGGER.set_file_name(log_file_name.c_str(), true, false, rs_log_file_name.c_str(), \
      ele_log_file_name.c_str(), NULL); \
  OB_LOGGER.set_log_level("INFO"); \
  OB_LOGGER.set_enable_log_limit(false); \
  OB_LOGGER.set_enable_async_log(false); \
  SERVER_LOG(INFO, "begin unittest"); \
  ::testing::InitGoogleTest(&argc, argv); \
  return RUN_ALL_TESTS();

int generate_data(char *&buf, int buf_len, int &real_data_size, const int wanted_data_size);
int generate_specifice_size_data(char *&buf, int buf_len, int wanted_data_size);
int generate_blob_data(char *&buf, int buf_len, int &wanted_data_size);

class MockLocCB : public palf::PalfLocationCacheCb
{
public:
  ObAddr leader_;
  int get_leader(const int64_t id, common::ObAddr &leader) override final
  {
    leader = leader_;
    return OB_SUCCESS;
  }
  int nonblock_get_leader(const int64_t id, common::ObAddr &leader) override final
  {
    UNUSED(id);
    leader = leader_;
    return OB_SUCCESS;
  }
  int nonblock_renew_leader(const int64_t id) override final
  {
    UNUSED(id);
    return OB_SUCCESS;
  }
};

class ObSimpleLogClusterTestEnv : public ObSimpleLogClusterTestBase
{
public:
  ObSimpleLogClusterTestEnv();
  virtual ~ObSimpleLogClusterTestEnv();
  virtual void SetUp();
  virtual void TearDown();
  int create_paxos_group(const int64_t id, int64_t &leader_idx, PalfHandleGuard &leader);
  int create_paxos_group(const int64_t id, palf::PalfLocationCacheCb *loc_cb, int64_t &leader_idx, PalfHandleGuard &leader);
  int create_paxos_group(const int64_t id, const int64_t create_ts, int64_t &leader_idx, PalfHandleGuard &leader);
  int create_paxos_group(const int64_t id, const LSN &lsn, int64_t &leader_idx, PalfHandleGuard &leader);
  int create_paxos_group(const int64_t id, const PalfBaseInfo &info, int64_t &leader_idx, PalfHandleGuard &leader);
  int create_paxos_group(const int64_t id, const PalfBaseInfo &info, palf::PalfLocationCacheCb *loc_cb, int64_t &leader_idx, PalfHandleGuard &leader);
  int update_server_log_disk_size(const int64_t new_size);
  virtual int delete_paxos_group(const int64_t id);
  virtual int update_disk_options(const int64_t server_id, const int64_t log_block_number);
  virtual int restart_paxos_groups();
  virtual int restart_server(const int64_t server_id);
  virtual int remove_dir();
  virtual int get_leader(const int64_t id, PalfHandleGuard &leader, int64_t &leader_idx);
  virtual int get_cluster_palf_handle_guard(const int64_t palf_id, std::vector<PalfHandleGuard*> &palf_list);
  virtual int revert_cluster_palf_handle_guard(std::vector<PalfHandleGuard*> &palf_list);
  virtual int get_palf_handle_guard(const std::vector<PalfHandleGuard*> &palf_list, const common::ObAddr &server, PalfHandleGuard &palf_handle);
  virtual int switch_leader(const int64_t id, const int64_t new_leader_idx, PalfHandleGuard &leader);
  virtual void set_need_drop_packet(const int64_t id, const bool need_drop_packet);
  virtual int check_replica_sync(const int64_t id, PalfHandleGuard *pf1, PalfHandleGuard *pf2, const int64_t timeout_us);
  // Bidirectional network isolation
  virtual void block_net(const int64_t id1, const int64_t id2, const bool is_single_direction = false);
  virtual void unblock_net(const int64_t id1, const int64_t id2);
  virtual void set_rpc_loss(const int64_t id1, const int64_t id2, const int loss_rate);
  virtual void reset_rpc_loss(const int64_t id1, const int64_t id2);
  virtual int submit_log(PalfHandleGuard &leader, int count, int id);
  virtual int submit_log(PalfHandleGuard &leader, int count, int id, int data_len);
  virtual int submit_log(PalfHandleGuard &leader, int count, int id, std::vector<LSN> &lsn_array, std::vector<int64_t> &log_ts_ns_array);
  virtual int submit_log(PalfHandleGuard &leader, int count, int data_len, int id, std::vector<LSN> &lsn_array, std::vector<int64_t> &log_ts_ns_array);
  virtual int submit_log_impl(PalfHandleGuard &leader,
                              const int64_t count,
                              const int64_t id,
                              const int64_t wanted_data_size,
                              std::vector<LSN> &lsn_array,
                              std::vector<int64_t> &log_ts_ns_array);
  virtual int submit_log_impl(PalfHandleGuard &leader,
                              const int64_t count,
                              const int64_t id,
                              const int64_t wanted_data_size,
                              const int64_t ref_ts,
                              std::vector<LSN> &lsn_array,
                              std::vector<int64_t> &log_ts_ns_array);
  virtual int submit_log(PalfHandleGuard &leader,
                         LSN &lsn,
                         int64_t &log_ts);
  virtual int submit_log(PalfHandleGuard &leader,
                         const int64_t ref_ts,
                         LSN &lsn,
                         int64_t &log_ts);
  virtual int change_access_mode_to_raw_write(PalfHandleGuard &leader);
  virtual int raw_write(PalfHandleGuard &leader,
                        const LSN lsn,
                        const char *buf,
                        const int64_t buf_len);
  virtual int read_log(PalfHandleGuard &leader);
  virtual int read_log(PalfHandleGuard &leader, const LSN &lsn);
  virtual int read_group_log(PalfHandleGuard &leader, LSN lsn);
  virtual int read_and_submit_group_log(PalfHandleGuard &leader, PalfHandleGuard &leader_raw_write);
  virtual int read_log_from_memory(PalfHandleGuard &leader);
  virtual int advance_base_info(const int64_t id, const PalfBaseInfo &base_info);
  virtual int get_palf_env(const int64_t server_idx, PalfEnv *&palf_env);
  virtual int wait_until_has_committed(PalfHandleGuard &leader, const LSN &lsn);
  virtual int wait_lsn_until_flushed(const LSN &lsn, PalfHandleGuard &guard);
public:
  static int64_t palf_id_;
private:
  int64_t prev_leader_idx_;
};

} // end namespace unittest
} // end namespace oceanbase
