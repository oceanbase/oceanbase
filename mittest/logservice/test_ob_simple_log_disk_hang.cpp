// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include <cstdio>
#include <gtest/gtest.h>
#include <signal.h>
#define private public
#include "env/ob_simple_log_cluster_env.h"
#undef private

const std::string TEST_NAME = "disk_hang";

using namespace oceanbase::common;
using namespace oceanbase;
namespace oceanbase
{
using namespace logservice;
struct DiskFailureGenerator {
  DiskFailureGenerator() :
      need_generate_failure_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      min_added_rt_(0),
      max_added_rt_(0),
      added_rt_ratio_(0) { }
  ~DiskFailureGenerator()
  {
    reset();
  }

  void reset()
  {
    need_generate_failure_ = false;
    tenant_id_ = OB_INVALID_TENANT_ID;
    min_added_rt_ = 0;
    max_added_rt_ = 0;
    added_rt_ratio_ = 0;
    PALF_LOG(INFO, "reset_failure_info success");
  }

  void set_failure_info(const bool need_generate_failure,
                        const uint64_t tenant_id,
                        const int64_t min_added_rt,
                        const int64_t max_added_rt)
  {
    need_generate_failure_ = need_generate_failure;
    tenant_id_ = tenant_id;
    min_added_rt_ = min_added_rt;
    max_added_rt_ = max_added_rt;
    added_rt_ratio_ = 0;
    PALF_LOG(INFO, "set_failure_info success", K(need_generate_failure),
        K(tenant_id), K(min_added_rt), K(max_added_rt));
  }

  void set_failure_info(const bool need_generate_failure,
                        const uint64_t tenant_id,
                        const double added_rt_ratio)
  {
    need_generate_failure_ = need_generate_failure;
    tenant_id_ = tenant_id;
    min_added_rt_ = 0;
    max_added_rt_ = 0;
    PALF_LOG(INFO, "set_failure_info success", K(need_generate_failure),
        K(tenant_id), K(added_rt_ratio));
  }

  void run(const int64_t real_rt)
  {
    // inject absolate rt
    if (true == need_generate_failure_ && MTL_ID() == tenant_id_ && max_added_rt_ > 0) {
      int64_t added_rt = ObRandom::rand(min_added_rt_, max_added_rt_);
      while (need_generate_failure_ && added_rt > 0) {
        ::ob_usleep(10);
        added_rt -= 10;
      }
      PALF_LOG(INFO, "add additional rt", K_(tenant_id), K(real_rt),
          K_(min_added_rt), K_(max_added_rt), K(added_rt));
    }

    // inject relative rt
    if (true == need_generate_failure_ && MTL_ID() == tenant_id_ && max_added_rt_ == 0) {
      double added_rt = real_rt * added_rt_ratio_;
      while (need_generate_failure_ && added_rt > 0) {
        ::ob_usleep(10);
        added_rt -= 10;
      }
      PALF_LOG(INFO, "add additional rt", K_(tenant_id), K(real_rt), K(added_rt), K_(added_rt_ratio));
    }
  }

  bool need_generate_failure_;
  uint64_t tenant_id_;
  int64_t min_added_rt_;
  int64_t max_added_rt_;
  double added_rt_ratio_;
};

DiskFailureGenerator failure_generator_;

int64_t customized_detect_window_us = 3 * 1000 * 1000;  // 5s
int64_t customized_recovery_interval = 3;

namespace logservice
{
namespace coordinator
{

int64_t ObFailureDetector::PalfDiskHangDetector::min_recovery_interval_() const
{
  return customized_recovery_interval;
}
}
}

namespace palf
{
int LogBlockHandler::inner_write_impl_(const ObIOFd &io_fd, const char *buf, const int64_t count, const int64_t offset)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::fast_current_time();
  int64_t write_size = 0;
  int64_t time_interval = OB_INVALID_TIMESTAMP;
  do {
    if (count != (write_size = ob_pwrite(io_fd.second_id_, buf, count, offset))) {
      if (palf_reach_time_interval(1000 * 1000, time_interval)) {
        ret = convert_sys_errno();
        PALF_LOG(ERROR, "ob_pwrite failed", K(ret), K(io_fd), K(offset), K(count), K(errno));
      }
      ::ob_usleep(RETRY_INTERVAL);
    } else {
      ret = OB_SUCCESS;
      break;
    }
  } while (OB_FAIL(ret));
  failure_generator_.run(ObTimeUtility::fast_current_time() - start_ts);
  int64_t cost_ts = ObTimeUtility::fast_current_time() - start_ts;
  EVENT_TENANT_INC(ObStatEventIds::PALF_WRITE_IO_COUNT, MTL_ID());
  EVENT_ADD(ObStatEventIds::PALF_WRITE_SIZE, count);
  EVENT_ADD(ObStatEventIds::PALF_WRITE_TIME, cost_ts);
  ob_pwrite_used_ts_ += cost_ts;
  return ret;
}
}

namespace unittest
{

class TestObSimpleLogClusterArbMockEleService : public ObSimpleLogClusterTestEnv
{
public:
  TestObSimpleLogClusterArbMockEleService() :  ObSimpleLogClusterTestEnv()
  {}
  class LogGenerator : public share::ObThreadPool
  {
  public:
    LogGenerator() :
        env_(NULL),
        leader_(NULL),
        palf_id_(INVALID_PALF_ID),
        traffic_level_(1),
        log_size_level_(1),
        is_inited_(false) { }
    virtual ~LogGenerator() { destroy(); }
  public:
    int init(ObSimpleLogClusterTestEnv *env,
             PalfHandleImplGuard *leader,
             const int64_t palf_id)
    {
      int ret = OB_SUCCESS;
      if (IS_INIT) {
        ret = OB_INIT_TWICE;
      } else if (OB_ISNULL(env) || OB_ISNULL(leader) || INVALID_PALF_ID == palf_id) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        env_ = env;
        leader_ = leader;
        palf_id_ = palf_id;
        is_inited_ = true;
      }
      return ret;
    }

    int start()
    {
      int ret = OB_SUCCESS;
      if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
      } else if (OB_FAIL(share::ObThreadPool::start())) {
        PALF_LOG(ERROR, "LogGenerator thread failed to start");
      } else {
        PALF_LOG(INFO, "LogGenerator start success", K(ret));
      }
      return ret;
    }

    void destroy()
    {
      is_inited_ = false;
      env_ = NULL;
      leader_ = NULL;
      palf_id_ = NULL;
    }

    int set_traffic_level(int64_t traffic_level)
    {
      int ret = OB_SUCCESS;
      if (traffic_level < 0 || traffic_level > 100) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        traffic_level_ = traffic_level;
      }
      PALF_LOG(INFO, "set_traffic_level", K(ret), K_(traffic_level));
      return ret;
    }

    int set_log_size_level(int64_t log_size_level)
    {
      int ret = OB_SUCCESS;
      if (log_size_level < 0 || log_size_level > 100) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        log_size_level_ = log_size_level;
      }
      PALF_LOG(INFO, "set_log_size_level", K(ret), K_(log_size_level));
      return ret;
    }

    void run1()
    {
      lib::set_thread_name("LogGenerator");
      while (!has_set_stop()) {
        if (traffic_level_ > 0) {
          env_->submit_log(*leader_, 10 * traffic_level_, palf_id_, 10 * 1024 * log_size_level_);
          SERVER_LOG(INFO, "submit_log", KP_(leader), K_(palf_id));
          ::ob_usleep(100 * 1000);
        }
      }
    }
  public:
    ObSimpleLogClusterTestEnv *env_;
    PalfHandleImplGuard *leader_;
    int64_t palf_id_;
    int64_t traffic_level_;       //  0-100
    int64_t log_size_level_;      // 0-100
    bool is_inited_;
  };

  class DetectRunnable : public share::ObThreadPool
  {
  public:
    DetectRunnable() :
        detector_(NULL),
        ctx_(NULL),
        is_inited_(false) { }
    virtual ~DetectRunnable() { destroy(); }
  public:
    int init(logservice::coordinator::ObFailureDetector *detector,
             ObTenantBase *ctx)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(detector) || OB_ISNULL(ctx)) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        detector_ = detector;
        ctx_ = ctx;
        is_inited_ = true;
      }
      return ret;
    }

    int start()
    {
      int ret = OB_SUCCESS;
      if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
      } else if (OB_FAIL(share::ObThreadPool::start())) {
        PALF_LOG(ERROR, "DetectRunnable failed to start");
      } else {
        PALF_LOG(INFO, "DetectRunnable start success", K(ret), KP(detector_), "tid", ctx_->id());
      }
      return ret;
    }

    void destroy()
    {
      is_inited_ = false;
      detector_ = NULL;
    }

    void run1()
    {
      lib::set_thread_name("LogGenerator");
      ObTenantEnv::set_tenant(ctx_);
      while (!has_set_stop()) {
        int64_t sensitivity = 0;
        if (detector_->palf_disk_hang_detector_.is_clog_disk_hang(sensitivity)) {
          ATOMIC_SET(&(detector_->has_add_clog_hang_event_), true);
        } else {
          ATOMIC_SET(&(detector_->has_add_clog_hang_event_), false);
        }
        usleep(1000);
      }
    }
  public:
    logservice::coordinator::ObFailureDetector *detector_;
    ObTenantBase *ctx_;
    bool is_inited_;
  };
};

int64_t ObSimpleLogClusterTestBase::member_cnt_ = 3;
int64_t ObSimpleLogClusterTestBase::node_cnt_ = 3;
bool ObSimpleLogClusterTestBase::need_add_arb_server_ = true;
std::string ObSimpleLogClusterTestBase::test_name_ = TEST_NAME;

// test
// 1. disk long-time hang
// 2. higher write rt
// 3. varying traffic
// 4. sensitivity

// 1. disk long-time hang
TEST_F(TestObSimpleLogClusterArbMockEleService, disk_long_time_hang)
{
  int ret = OB_SUCCESS;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t TIMEOUT_US = 10 * 1000 * 1000L;
  SET_CASE_LOG_FILE(TEST_NAME, "disk_long_time_hang");
  PALF_LOG(INFO, "begin disk_long_time_hang", K(id));

  int64_t leader_idx = 0, arb_replica_idx = 0;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  LogGenerator log_generator;
  EXPECT_EQ(OB_SUCCESS, log_generator.init(this, &leader, id));
  EXPECT_EQ(OB_SUCCESS, log_generator.start());
  EXPECT_UNTIL_EQ(true, leader.palf_handle_impl_->get_end_lsn().val_ > 0);

  const int64_t b_idx = (leader_idx + 1) % 3;
  const int64_t c_idx = (leader_idx + 2) % 3;
  const common::ObAddr a_addr = get_cluster()[leader_idx]->get_addr();
  const common::ObAddr b_addr = get_cluster()[b_idx]->get_addr();
  const common::ObAddr c_addr = get_cluster()[c_idx]->get_addr();
  PalfHandleImplGuard *a_handle = palf_list[leader_idx];
  PalfHandleImplGuard *b_handle = palf_list[b_idx];
  ObTenantBase *tenant_base = get_cluster()[b_idx]->get_tenant_base();
  ObTenantEnv::set_tenant(tenant_base);
  logservice::coordinator::ObFailureDetector *detector = MTL(logservice::coordinator::ObFailureDetector *);
  EXPECT_TRUE(OB_NOT_NULL(detector));
  DetectRunnable detect_runnable;
  EXPECT_EQ(OB_SUCCESS, detect_runnable.init(detector, tenant_base));
  EXPECT_EQ(OB_SUCCESS, detect_runnable.start());

  // 1. default strategy
  {
    GCONF.log_storage_warning_trigger_percentage = 0;
    const uint64_t follower_tid = get_cluster()[b_idx]->get_tenant_base()->id();
    failure_generator_.set_failure_info(true, follower_tid, 1000 * 1000 * 1000, 1000 * 1000 * 1000);

    int64_t start_ts = common::ObTimeUtility::current_time();
    EXPECT_UNTIL_EQ(true, detector->has_add_clog_hang_event_);
    int64_t detect_duration = common::ObTimeUtility::current_time() - start_ts;
    EXPECT_UNTIL_EQ(true, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.degraded_learnerlist_.get_member_number() > 0);
    int64_t degrade_duration = common::ObTimeUtility::current_time() - start_ts;
    PALF_LOG(INFO, "CASE 1: detect_duration", K(id), K(detect_duration), K(degrade_duration));

    failure_generator_.reset();
    EXPECT_UNTIL_EQ(0, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.degraded_learnerlist_.get_member_number());
  }
  // 2. sensitive strategy
  {
    GCONF.log_storage_warning_trigger_percentage = 5;
    ::ob_usleep(customized_detect_window_us);
    const uint64_t follower_tid = get_cluster()[b_idx]->get_tenant_base()->id();
    failure_generator_.set_failure_info(true, follower_tid, 1000 * 1000 * 1000, 1000 * 1000 * 1000);

    int64_t start_ts = common::ObTimeUtility::current_time();
    EXPECT_UNTIL_EQ(true, detector->has_add_clog_hang_event_);
    int64_t detect_duration = common::ObTimeUtility::current_time() - start_ts;
    EXPECT_UNTIL_EQ(true, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.degraded_learnerlist_.get_member_number() > 0);
    int64_t degrade_duration = common::ObTimeUtility::current_time() - start_ts;
    PALF_LOG(INFO, "CASE 1: detect_duration", K(id), K(detect_duration), K(degrade_duration));

    start_ts = common::ObTimeUtility::current_time();
    failure_generator_.reset();
    EXPECT_UNTIL_EQ(false, detector->has_add_clog_hang_event_);
    int64_t recover_duration = common::ObTimeUtility::current_time() - start_ts;
    PALF_LOG(INFO, "CASE 1: recover_duration", K(id), K(recover_duration));
  }
  failure_generator_.reset();
  detect_runnable.stop();
  detect_runnable.wait();
  log_generator.stop();
  log_generator.wait();
  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end disk_long_time_hang", K(id));
}

TEST_F(TestObSimpleLogClusterArbMockEleService, disk_higher_rt)
{
  int ret = OB_SUCCESS;
  const int64_t id = ATOMIC_AAF(&palf_id_, 1);
  const int64_t TIMEOUT_US = 10 * 1000 * 1000L;
  SET_CASE_LOG_FILE(TEST_NAME, "disk_higher_rt");
  PALF_LOG(INFO, "begin disk_higher_rt", K(id));

  int64_t leader_idx = 0, arb_replica_idx = 0;
  PalfHandleImplGuard leader;
  std::vector<PalfHandleImplGuard*> palf_list;
  EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
  EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
  LogGenerator log_generator;
  EXPECT_EQ(OB_SUCCESS, log_generator.init(this, &leader, id));
  EXPECT_EQ(OB_SUCCESS, log_generator.start());
  EXPECT_UNTIL_EQ(true, leader.palf_handle_impl_->get_end_lsn().val_ > 0);

  const int64_t b_idx = (leader_idx + 1) % 3;
  const int64_t c_idx = (leader_idx + 2) % 3;
  const common::ObAddr a_addr = get_cluster()[leader_idx]->get_addr();
  const common::ObAddr b_addr = get_cluster()[b_idx]->get_addr();
  const common::ObAddr c_addr = get_cluster()[c_idx]->get_addr();
  PalfHandleImplGuard *a_handle = palf_list[leader_idx];
  PalfHandleImplGuard *b_handle = palf_list[b_idx];
  const uint64_t follower_tid = get_cluster()[b_idx]->get_tenant_base()->id();
  ObTenantBase *tenant_base = get_cluster()[b_idx]->get_tenant_base();
  ObTenantEnv::set_tenant(tenant_base);
  logservice::coordinator::ObFailureDetector *detector = MTL(logservice::coordinator::ObFailureDetector *);
  EXPECT_TRUE(OB_NOT_NULL(detector));
  DetectRunnable detect_runnable;
  EXPECT_EQ(OB_SUCCESS, detect_runnable.init(detector, tenant_base));
  EXPECT_EQ(OB_SUCCESS, detect_runnable.start());

  // 1. 500ms additional rt
  {
    GCONF.log_storage_warning_trigger_percentage = 5;
    ::ob_usleep(customized_detect_window_us);
    const int64_t additional_rt = 500 * 1000;
    failure_generator_.set_failure_info(true, follower_tid, additional_rt, additional_rt);

    int64_t start_ts = common::ObTimeUtility::current_time();
    EXPECT_UNTIL_EQ(true, detector->has_add_clog_hang_event_);
    int64_t detect_duration = common::ObTimeUtility::current_time() - start_ts;
    EXPECT_UNTIL_EQ(true, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.degraded_learnerlist_.get_member_number() > 0);
    int64_t degrade_duration = common::ObTimeUtility::current_time() - start_ts;
    PALF_LOG(INFO, "CASE 2.1: detect_duration", K(id), K(detect_duration), K(degrade_duration));

    start_ts = common::ObTimeUtility::current_time();
    failure_generator_.reset();
    EXPECT_UNTIL_EQ(false, detector->has_add_clog_hang_event_);
    int64_t recover_duration = common::ObTimeUtility::current_time() - start_ts;
    PALF_LOG(INFO, "CASE 2.1: recover_duration", K(id), K(recover_duration));
    EXPECT_UNTIL_EQ(true, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.degraded_learnerlist_.get_member_number() > 0);
  }
  // 2. 100ms-500ms additional rt
  {
    GCONF.log_storage_warning_trigger_percentage = 5;
    ::ob_usleep(customized_detect_window_us);
    const int64_t min_additional_rt = 100 * 1000;
    const int64_t max_additional_rt = 500 * 1000;
    failure_generator_.set_failure_info(true, follower_tid, min_additional_rt, max_additional_rt);

    int64_t start_ts = common::ObTimeUtility::current_time();
    EXPECT_UNTIL_EQ(true, detector->has_add_clog_hang_event_);
    int64_t detect_duration = common::ObTimeUtility::current_time() - start_ts;
    EXPECT_UNTIL_EQ(true, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.degraded_learnerlist_.get_member_number() > 0);
    int64_t degrade_duration = common::ObTimeUtility::current_time() - start_ts;
    PALF_LOG(INFO, "CASE 2.2: detect_duration", K(id), K(detect_duration), K(degrade_duration));

    start_ts = common::ObTimeUtility::current_time();
    failure_generator_.reset();
    EXPECT_UNTIL_EQ(false, detector->has_add_clog_hang_event_);
    int64_t recover_duration = common::ObTimeUtility::current_time() - start_ts;
    PALF_LOG(INFO, "CASE 2.2: recover_duration", K(id), K(recover_duration));
    EXPECT_UNTIL_EQ(true, leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.degraded_learnerlist_.get_member_number() > 0);
  }
  // // 3. 2x rt
  // {
  //   GCONF.log_storage_warning_trigger_percentage = 5;
  //   ::ob_usleep(customized_detect_window_us);
  //   const double added_rt_ratio = 2;
  //   failure_generator_.set_failure_info(true, follower_tid, added_rt_ratio);
  //   ::ob_usleep(3 * 1000 * 1000);
  //   EXPECT_EQ(false, detector->has_add_clog_hang_event_);
  // }
  failure_generator_.reset();
  detect_runnable.stop();
  detect_runnable.wait();
  log_generator.stop();
  log_generator.wait();
  revert_cluster_palf_handle_guard(palf_list);
  leader.reset();
  delete_paxos_group(id);
  PALF_LOG(INFO, "end disk_higher_rt", K(id));
}

// 3. varying traffic
// disable this case because farm's env is unstable
// TEST_F(TestObSimpleLogClusterArbMockEleService, disk_varying_traffic)
// {
//   int ret = OB_SUCCESS;
//   const int64_t id = ATOMIC_AAF(&palf_id_, 1);
//   const int64_t TIMEOUT_US = 10 * 1000 * 1000L;
//   SET_CASE_LOG_FILE(TEST_NAME, "disk_varying_traffic");
//   PALF_LOG(INFO, "begin disk_varying_traffic", K(id));

//   int64_t leader_idx = 0, arb_replica_idx = 0;
//   PalfHandleImplGuard leader;
//   std::vector<PalfHandleImplGuard*> palf_list;
//   EXPECT_EQ(OB_SUCCESS, create_paxos_group_with_arb(id, arb_replica_idx, leader_idx, leader));
//   EXPECT_EQ(OB_SUCCESS, get_cluster_palf_handle_guard(id, palf_list));
//   LogGenerator log_generator;
//   EXPECT_EQ(OB_SUCCESS, log_generator.init(this, &leader, id));
//   EXPECT_EQ(OB_SUCCESS, log_generator.start());
//   EXPECT_UNTIL_EQ(true, leader.palf_handle_impl_->get_end_lsn().val_ > 0);

//   const int64_t b_idx = (leader_idx + 1) % 3;
//   const int64_t c_idx = (leader_idx + 2) % 3;
//   const common::ObAddr a_addr = get_cluster()[leader_idx]->get_addr();
//   const common::ObAddr b_addr = get_cluster()[b_idx]->get_addr();
//   const common::ObAddr c_addr = get_cluster()[c_idx]->get_addr();
//   PalfHandleImplGuard *a_handle = palf_list[leader_idx];
//   PalfHandleImplGuard *b_handle = palf_list[b_idx];
//   const uint64_t follower_tid = get_cluster()[b_idx]->get_tenant_base()->id();
//   ObTenantBase *tenant_base = get_cluster()[b_idx]->get_tenant_base();
//   ObTenantEnv::set_tenant(tenant_base);
//   logservice::coordinator::ObFailureDetector *detector = MTL(logservice::coordinator::ObFailureDetector *);
//   EXPECT_TRUE(OB_NOT_NULL(detector));
//   DetectRunnable detect_runnable;
//   EXPECT_EQ(OB_SUCCESS, detect_runnable.init(detector, tenant_base));
//   EXPECT_EQ(OB_SUCCESS, detect_runnable.start());
//   const palf::LogConfigVersion start_config_version = leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.config_version_;

//   // 1. varying log number
//   {
//     GCONF.log_storage_warning_trigger_percentage = 1;
//     ::ob_usleep(customized_detect_window_us);
//     // low to high
//     EXPECT_EQ(OB_SUCCESS, log_generator.set_traffic_level(100));
//     ::ob_usleep(customized_detect_window_us);
//     // high to low
//     EXPECT_EQ(OB_SUCCESS, log_generator.set_traffic_level(1));
//     ::ob_usleep(customized_detect_window_us);
//     // low to high
//     EXPECT_EQ(OB_SUCCESS, log_generator.set_traffic_level(100));
//     ::ob_usleep(customized_detect_window_us);
//     // high to low
//     EXPECT_EQ(OB_SUCCESS, log_generator.set_traffic_level(1));
//     ::ob_usleep(customized_detect_window_us);
//     const palf::LogConfigVersion end_config_version = leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.config_version_;
//     EXPECT_EQ(start_config_version.config_seq_, end_config_version.config_seq_);
//   }

//   // 2. varying log size
//   {
//     GCONF.log_storage_warning_trigger_percentage = 1;
//     ::ob_usleep(customized_detect_window_us);
//     // small to big
//     EXPECT_EQ(OB_SUCCESS, log_generator.set_log_size_level(100));
//     ::ob_usleep(customized_detect_window_us);
//     // big to small
//     EXPECT_EQ(OB_SUCCESS, log_generator.set_log_size_level(1));
//     ::ob_usleep(customized_detect_window_us);
//     // small to big
//     EXPECT_EQ(OB_SUCCESS, log_generator.set_log_size_level(100));
//     ::ob_usleep(customized_detect_window_us);
//     // big to small
//     EXPECT_EQ(OB_SUCCESS, log_generator.set_log_size_level(1));
//     ::ob_usleep(customized_detect_window_us);
//     const palf::LogConfigVersion end_config_version = leader.palf_handle_impl_->config_mgr_.log_ms_meta_.curr_.config_.config_version_;
//     EXPECT_EQ(start_config_version.config_seq_, end_config_version.config_seq_);
//   }
//   failure_generator_.reset();
//   detect_runnable.stop();
//   detect_runnable.wait();
//   log_generator.stop();
//   log_generator.wait();
//   revert_cluster_palf_handle_guard(palf_list);
//   leader.reset();
//   delete_paxos_group(id);
//   PALF_LOG(INFO, "end disk_varying_traffic", K(id));
// }

} // end unittest
} // end oceanbase

int main(int argc, char **argv)
{
  RUN_SIMPLE_LOG_CLUSTER_TEST(TEST_NAME);
}
