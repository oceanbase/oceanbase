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
#include "share/scn.h"
#include "ob_simple_log_cluster_testbase.h"

namespace oceanbase
{
namespace logservice
{
class AppendCb;
class ObReplayStatus;
}
using namespace palf;
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
  oceanbase::common::ObClockGenerator::init();\
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
  OB_LOGGER.set_log_level("DEBUG"); \
  OB_LOGGER.set_enable_log_limit(false); \
  OB_LOGGER.set_enable_async_log(false); \
  std::string app_gtest_log_name = std::string(TEST_NAME) + "_gtest.log"; \
  system(("rm -rf " + app_gtest_log_name + "*").c_str());   \
  oceanbase::unittest::init_gtest_output(app_gtest_log_name); \
  SERVER_LOG(INFO, "begin unittest"); \
  ::testing::InitGoogleTest(&argc, argv); \
  ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION); \
  ObClusterVersion::get_instance().update_cluster_version(DATA_CURRENT_VERSION); \
  return RUN_ALL_TESTS();

#define EXPECT_UNTIL_EQ(x, y) while(!(x == y))        \
        { usleep(500);                                \
          SERVER_LOG(INFO, "EXPECT_UNTIL_EQ WAIT",    \
          "file", oceanbase::common::occam::get_file_name_without_dir(__FILE__), \
          "line", __LINE__); }
#define EXPECT_UNTIL_NE(x, y) while(x == y)           \
        { usleep(500);                                \
          SERVER_LOG(INFO, "EXPECT_UNTIL_NE WAIT",    \
          "file", oceanbase::common::occam::get_file_name_without_dir(__FILE__), \
          "line", __LINE__); }

void init_gtest_output(std::string &gtest_log_name);
int generate_data(char *&buf, int buf_len, int &real_data_size, const int wanted_data_size);
int generate_specifice_size_data(char *&buf, int buf_len, int wanted_data_size);
int generate_blob_data(char *&buf, int buf_len, int &wanted_data_size);

struct PalfHandleImplGuard
{
  PalfHandleImplGuard();
  ~PalfHandleImplGuard();
  bool is_valid() const;
  PalfHandleImpl *get_palf_handle_impl() const { return palf_handle_impl_; }
  void reset();
  TO_STRING_KV(K_(palf_id), KP_(palf_handle_impl), KP_(palf_env_impl));

  int64_t  palf_id_;
  PalfHandleImpl *palf_handle_impl_;
  PalfEnvImpl *palf_env_impl_;
private:
  DISALLOW_COPY_AND_ASSIGN(PalfHandleImplGuard);
};
struct PalfHandleLiteGuard
{
  PalfHandleLiteGuard();
  ~PalfHandleLiteGuard();
  bool is_valid() const;
  palflite::PalfHandleLite *get_palf_handle_impl() const { return palf_handle_lite_; }
  void reset();
  TO_STRING_KV(K_(palf_id), KP_(palf_handle_lite), KP_(palf_env_lite));

  int64_t palf_id_;
  palflite::PalfHandleLite *palf_handle_lite_;
  palflite::PalfEnvLite *palf_env_lite_;
private:
  DISALLOW_COPY_AND_ASSIGN(PalfHandleLiteGuard);
};
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
  int create_paxos_group(const int64_t id, int64_t &leader_idx, PalfHandleImplGuard &leader);
  int create_paxos_group(const int64_t id, palf::PalfLocationCacheCb *loc_cb, int64_t &leader_idx, PalfHandleImplGuard &leader);
  int create_paxos_group(const int64_t id, const share::SCN &create_scn, int64_t &leader_idx, PalfHandleImplGuard &leader);
  int create_paxos_group(const int64_t id, const LSN &lsn, int64_t &leader_idx, PalfHandleImplGuard &leader);
  int create_paxos_group(const int64_t id, const PalfBaseInfo &info, int64_t &leader_idx, PalfHandleImplGuard &leader);
  int create_paxos_group(const int64_t id,
                         const PalfBaseInfo &info,
                         palf::PalfLocationCacheCb *loc_cb,
                         int64_t &leader_idx,
                         const bool with_mock_election,
                         PalfHandleImplGuard &leader);
  int create_paxos_group_with_arb(const int64_t id, int64_t &arb_replica_idx, int64_t &leader_idx, PalfHandleImplGuard &leader);
  int create_paxos_group_with_arb(const int64_t id,
                                  palf::PalfLocationCacheCb *loc_cb,
                                  int64_t &arb_replica_idx,
                                  int64_t &leader_idx,
                                  const bool with_mock_election,
                                  PalfHandleImplGuard &leader);
  int create_paxos_group_with_mock_election(const int64_t id,
                                            int64_t &leader_idx,
                                            PalfHandleImplGuard &leader);
  int create_paxos_group_with_arb_mock_election(const int64_t id,
                                                int64_t &arb_replica_idx,
                                                int64_t &leader_idx,
                                                PalfHandleImplGuard &leader);
  virtual int delete_paxos_group(const int64_t id);
  virtual int update_disk_options(const int64_t server_id, const int64_t log_block_number);
  virtual int update_disk_options(const int64_t server_id, const int64_t recycle_threshold, const int64_t write_stop_threshold);
  virtual int update_disk_options(const int64_t log_block_number);
  virtual int get_disk_options(const int64_t server_id, PalfDiskOptions &opts);
  virtual int restart_paxos_groups();
  virtual int restart_server(const int64_t server_id);
  virtual int remove_dir();
  virtual int get_log_pool(const int64_t leader_idx, logservice::ObServerLogBlockMgr *&pool);
  virtual int get_leader(const int64_t id, PalfHandleImplGuard &leader, int64_t &leader_idx);
  virtual int get_cluster_palf_handle_guard(const int64_t palf_id, std::vector<PalfHandleImplGuard*> &palf_list);
  virtual int get_arb_member_guard(const int64_t palf_id, PalfHandleLiteGuard &guard);
  virtual int revert_cluster_palf_handle_guard(std::vector<PalfHandleImplGuard*> &palf_list);
  virtual int get_palf_handle_guard(const std::vector<PalfHandleImplGuard*> &palf_list, const common::ObAddr &server, PalfHandleImplGuard &palf_handle);
  virtual int switch_leader(const int64_t id, const int64_t new_leader_idx, PalfHandleImplGuard &leader);
  virtual void set_need_drop_packet(const int64_t id, const bool need_drop_packet);
  virtual int check_replica_sync(const int64_t id, PalfHandleImplGuard *pf1, PalfHandleImplGuard *pf2, const int64_t timeout_us);
  // Bidirectional network isolation
  virtual void block_all_net(const int64_t id);
  virtual void unblock_all_net(const int64_t id);
  virtual void block_net(const int64_t id1, const int64_t id2, const bool is_single_direction = false);
  virtual void unblock_net(const int64_t id1, const int64_t id2);
  virtual void block_pcode(const int64_t id1, const ObRpcPacketCode &pcode);
  virtual void unblock_pcode(const int64_t id1, const ObRpcPacketCode &pcode);
  virtual void set_rpc_loss(const int64_t id1, const int64_t id2, const int loss_rate);
  virtual void reset_rpc_loss(const int64_t id1, const int64_t id2);
  virtual int submit_log(PalfHandleImplGuard &leader, int count, int id);
  virtual int submit_log(PalfHandleImplGuard &leader, int count, int id, int data_len);
  virtual int submit_log(PalfHandleImplGuard &leader, int count, int id, std::vector<LSN> &lsn_array, std::vector<SCN> &scn_array);
  virtual int submit_log(PalfHandleImplGuard &leader, int count, int data_len, int id, std::vector<LSN> &lsn_array, std::vector<SCN> &scn_array);
  virtual int submit_log_impl(PalfHandleImplGuard &leader,
                              const int64_t count,
                              const int64_t id,
                              const int64_t wanted_data_size,
                              std::vector<LSN> &lsn_array,
                              std::vector<SCN> &scn_array);
  virtual int submit_log_impl(PalfHandleImplGuard &leader,
                              const int64_t count,
                              const int64_t id,
                              const int64_t wanted_data_size,
                              const share::SCN &ref_scn,
                              std::vector<LSN> &lsn_array,
                              std::vector<SCN> &scn_array);
  virtual int submit_log(PalfHandleImplGuard &leader,
                         LSN &lsn,
                         share::SCN &scn);
  virtual int submit_log(PalfHandleImplGuard &leader,
                         const share::SCN &ref_scn,
                         LSN &lsn,
                         share::SCN &scn);
  virtual int change_access_mode_to_raw_write(PalfHandleImplGuard &leader);
  virtual int change_access_mode_to_append(PalfHandleImplGuard &leader);
  virtual int raw_write(PalfHandleImplGuard &leader,
                        const LSN lsn,
                        const char *buf,
                        const int64_t buf_len);
  virtual int read_log(PalfHandleImplGuard &leader);
  virtual int read_log(PalfHandleImplGuard &leader, const LSN &lsn);
  virtual int read_group_log(PalfHandleImplGuard &leader, LSN lsn);
  virtual int read_and_submit_group_log(PalfHandleImplGuard &leader, PalfHandleImplGuard &leader_raw_write, const LSN &start_lsn = LSN(0));
  virtual int read_log_from_memory(PalfHandleImplGuard &leader);
  virtual int advance_base_info(const int64_t id, const PalfBaseInfo &base_info);
  virtual int get_palf_env(const int64_t server_idx, PalfEnv *&palf_env);
  virtual int wait_until_has_committed(PalfHandleImplGuard &leader, const LSN &lsn);
  virtual int wait_lsn_until_flushed(const LSN &lsn, PalfHandleImplGuard &guard);
  //wait until all log task pushed into queue of LogIOWorker
  virtual int wait_lsn_until_submitted(const LSN &lsn, PalfHandleImplGuard &guard);
  virtual void wait_all_replcias_log_sync(const int64_t palf_id);
  int get_middle_scn(const int64_t log_num, PalfHandleImplGuard &leader, share::SCN &mid_scn, LogEntryHeader &log_entry_header);
  void switch_append_to_raw_write(PalfHandleImplGuard &leader, int64_t &mode_version);
  void switch_append_to_flashback(PalfHandleImplGuard &leader, int64_t &mode_version);
  void switch_flashback_to_append(PalfHandleImplGuard &leader, int64_t &mode_version);
  void set_disk_options_for_throttling(PalfEnvImpl &palf_env_impl);
  virtual bool is_degraded(const PalfHandleImplGuard &leader, const int64_t degraded_server_idx);
  virtual bool is_upgraded(PalfHandleImplGuard &leader, const int64_t palf_id);
  int wait_until_disk_space_to(const int64_t server_id, const int64_t expect_log_disk_space);
  int update_server_log_disk(const int64_t log_disk_size);
public:
  static int64_t palf_id_;
private:
  int64_t prev_leader_idx_;
};

class IOTaskCond : public LogIOTask {
public:
	IOTaskCond(const int64_t palf_id, const int64_t palf_epoch) : LogIOTask(palf_id, palf_epoch) {}
  virtual int do_task_(int tg_id, IPalfEnvImpl *palf_env_impl) override final
  {
    PALF_LOG(INFO, "before cond_wait");
    cond_.wait();
    PALF_LOG(INFO, "after cond_wait");
    return OB_SUCCESS;
  };
  virtual int after_consume_(IPalfEnvImpl *palf_env_impl) override final
  {
    return OB_SUCCESS;
  }
  virtual LogIOTaskType get_io_task_type_() const { return LogIOTaskType::FLUSH_META_TYPE; }
  int init(int64_t palf_id)
  {
    palf_id_ = palf_id;
    return OB_SUCCESS;
  };
  virtual void free_this_(IPalfEnvImpl *impl) {UNUSED(impl);}
  virtual int64_t get_io_size_() const {return 0;}
  bool need_purge_throttling_() const {return true;}
  ObCond cond_;
};

class IOTaskConsumeCond : public LogIOTask {
public:
	IOTaskConsumeCond(const int64_t palf_id, const int64_t palf_epoch) : LogIOTask(palf_id, palf_epoch) {}
  virtual int do_task_(int tg_id, IPalfEnvImpl *palf_env_impl) override final
  {
    int ret = OB_SUCCESS;
    PALF_LOG(INFO, "do_task_ success");
    if (OB_FAIL(push_task_into_cb_thread_pool_(tg_id, this))) {
      PALF_LOG(WARN, "push_task_into_cb_thread_pool failed", K(ret), K(tg_id), KP(this));
    }
    return ret;
  };
  virtual int after_consume_(IPalfEnvImpl *palf_env_impl) override final
  {
    PALF_LOG(INFO, "before cond_wait");
    cond_.wait();
    PALF_LOG(INFO, "after cond_wait");
    return OB_SUCCESS;
  }
  virtual LogIOTaskType get_io_task_type_() const { return LogIOTaskType::FLUSH_META_TYPE; }
  int init(int64_t palf_id)
  {
    palf_id_ = palf_id;
    return OB_SUCCESS;
  };
  virtual void free_this_(IPalfEnvImpl *impl) {UNUSED(impl);}
  virtual int64_t get_io_size_() const {return 0;}
  bool need_purge_throttling_() const {return true;}
  ObCond cond_;
};
} // end namespace unittest
} // end namespace oceanbase
