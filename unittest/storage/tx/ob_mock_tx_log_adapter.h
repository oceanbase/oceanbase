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

#ifndef OCEANBASE_UNITTEST_STORAGE_TX_OB_MOCK_CLOG_ADAPTER
#define OCEANBASE_UNITTEST_STORAGE_TX_OB_MOCK_CLOG_ADAPTER

// #include "storage/tx/ob_clog_adapter.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_timer.h"
#include "storage/tx/ob_trans_submit_log_cb.h"
#include "storage/tx/ob_tx_log_adapter.h"
#include "lib/thread/ob_simple_thread_pool.h"
// #include "lib/container/ob_se_array.h"
// #include "lib/container/ob_dlist.h"
#include <string>
#include <map>
#include <list>

namespace oceanbase 
{

namespace logservice
{
class ObIReplaySubHandler;
}

namespace transaction 
{

class MockTxLogParam : public ObITxLogParam 
{
public:
  MockTxLogParam() : is_asc_cbs_(false), cb_thread_cnt_(4),wait_all_cb_(false) ,cb_time_(100*1000){}

public:
  bool is_asc_cbs_;
  int64_t cb_thread_cnt_;
  bool wait_all_cb_;
  int64_t cb_time_;

private:
};

class MockTxLogAdapter : public ObITxLogAdapter, public ObSimpleThreadPool 
{
public:
  MockTxLogAdapter():is_running_(false),max_allocated_log_ts_(0),lsn_(0),task_ptr_(nullptr),CB_CNT_(0) {}
  int init(ObITxLogParam *param);
  int start();
  void stop();
  void wait();
  void destroy();

  int push(void *task);
  void handle(void *task);

public:
  int submit_log(const char *buf,
                 const int64_t size,
                 const share::SCN &base_ts,
                 ObTxBaseLogCb *cb,
                 const bool need_block);
  int get_role(bool &is_leader, int64_t &epoch);
  int get_max_decided_scn(share::SCN &scn)
  {
    UNUSED(scn);
    return OB_SUCCESS;
  }
  int get_append_mode_initial_scn(share::SCN &ref_scn) {
    int ret = OB_SUCCESS;
    ref_scn = share::SCN::invalid_scn();
    return ret;
  }

  void invoke_all_cbs();

public:
  bool get_log(int64_t log_ts, std::string &log_string);
  int get_next_log(int64_t log_ts, std::string &log_string, int64_t &next_log_ts);
  int64_t get_cb_cnt();
  int64_t get_cb_time() { return submit_config_.cb_time_; }
private:
  bool is_cbs_finish_();

private:
  bool is_running_;
  MockTxLogParam submit_config_;
  ObSpinLock log_file_lock_;
  ObSpinLock cbs_lock_;
  int64_t unfinish_cbs_cnt_;
  uint64_t max_allocated_log_ts_;
  int64_t lsn_;
  // int64_t max_success_log_ts;
  // common::ObSEArray<ObString, 100, TransModulePageAllocator> mock_log_file_;
  // common::ObSEArray<int64_t, 100, TransModulePageAllocator> record_log_ts_;
  // common::ObSEArray<logservice::AppendCb *, 10, TransModulePageAllocator> waiting_cbs_;
  std::map<int64_t,std::string> mock_log_file_; //ts , log record
  std::list<logservice::AppendCb *> waiting_cbs_;

  ObTransTimer timer_;
  ObITimeoutTask *task_ptr_;

  int64_t CB_CNT_;
};

class MockCbTimeoutTask : public ObITimeoutTask
{
public:
  MockCbTimeoutTask() : adapter_(nullptr) {}
  virtual ~MockCbTimeoutTask() {}
  int init(MockTxLogAdapter *adapter)
  {
    int ret = OB_SUCCESS;
    adapter_ = adapter;
    return ret;
  }
  void reset() { adapter_ = nullptr; }

public:
  void runTimerTask() { adapter_->invoke_all_cbs(); }
  uint64_t hash() const { return 1; };

private:
  MockTxLogAdapter *adapter_;
};

// struct MockReplayInfo
// {
//   logservice::ObIReplaySubHandler *replay_target_;
//
//   int64_t replay_success_ts_;
//   int64_t replaying_ts_;
//   std::string replaying_log_;
//
//   void reset()
//   {
//     replay_target_ = nullptr;
//     replay_success_ts_ = -1;
//     replaying_ts_ = -1;
//     replaying_log_.clear();
//   }
//
//   MockReplayInfo() { reset(); }
// };
//
// class MockReplayMgr : public lib::ThreadPool
// {
// public:
//   int init(MockTxLogAdapter * log_adapter);
//   int start();
//   void stop();
//   void wait();
//   void destroy();
//
//   virtual void run1();
//
//   void register_replay_target(logservice::ObIReplaySubHandler *replay_target);
//   void unregister_replay_target(logservice::ObIReplaySubHandler *replay_target);
//
// private:
//   MockTxLogAdapter * log_adapter_ptr_;
//   std::list<MockReplayInfo> replay_target_list_;
// };

// class TestTxLogSubmitter : public ObSimpleThreadPool
// {
// public:
//   TestTxLogSubmitter() : thread_cnt_(4), adapter_(nullptr) {}
//   int init(int64_t thread_cnt, MockTxLogAdapter *adapter);
//   int start();
//   void stop();
//   void destroy();
//
// public:
//   int64_t thread_cnt_;
//   MockTxLogAdapter *adapter_;
// };

} // namespace transaction
} // namespace oceanbase


#endif
