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

#ifndef OCEANBASE_LOGSERVICE_OB_LOG_REPLAY_SERVICE_
#define OCEANBASE_LOGSERVICE_OB_LOG_REPLAY_SERVICE_

#include "ob_replay_status.h"
#include "logservice/ob_log_base_header.h"
#include "lib/thread/thread_mgr_interface.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "share/scn.h"

namespace oceanbase
{
namespace share
{
class SCN;
}
namespace palf
{
class PalfEnv;
}
namespace logservice
{
class ObLSAdapter;
class ReplayProcessStat : public common::ObTimerTask
{
public:
  ReplayProcessStat();
  virtual ~ReplayProcessStat();
public:
  int init(ObLogReplayService *rp_sv);
  int start();
  void stop();
  void wait();
  void destroy();
  virtual void runTimerTask();
private:
  static const int64_t SCAN_TIMER_INTERVAL = 10 * 1000 * 1000; //10s
  //上一次轮询时总回放日志量
  int64_t last_replayed_log_size_;
  ObLogReplayService *rp_sv_;
  int tg_id_;
  bool is_inited_;
};

class ObILogReplayService
{
public:
  virtual int is_replay_done(const share::ObLSID &id, const palf::LSN &end_lsn, bool &is_done) = 0;
  virtual int switch_to_follower(const share::ObLSID &id, const palf::LSN &begin_lsn) = 0;
  virtual int switch_to_leader(const share::ObLSID &id) = 0;
};

class ObLogReplayService: public lib::TGTaskHandler, public ObILogReplayService
{
public:
  ObLogReplayService();
  virtual ~ObLogReplayService();
  int init(palf::PalfEnv *palf_env,
           ObLSAdapter *ls_adapter,
           ObILogAllocator *allocator);
public:
  int start();
  void stop();
  void wait();
  void destroy();
public:
  class GetReplayStatusFunctor
  {
  public:
    GetReplayStatusFunctor(ObReplayStatusGuard &guard)
        : ret_code_(common::OB_SUCCESS), guard_(guard){}
    ~GetReplayStatusFunctor(){}
    bool operator()(const share::ObLSID &id, ObReplayStatus *replay_status);
    int get_ret_code() const { return ret_code_; }
    TO_STRING_KV(K(ret_code_));
  private:
    int ret_code_;
    ObReplayStatusGuard &guard_;
  };
  class RemoveReplayStatusFunctor
  {
  public:
    explicit RemoveReplayStatusFunctor()
        : ret_code_(common::OB_SUCCESS) {}
    ~RemoveReplayStatusFunctor(){}
    bool operator()(const share::ObLSID &id, ObReplayStatus *replay_status);
    int get_ret_code() const { return ret_code_; }
    TO_STRING_KV(K(ret_code_));
  private:
    int ret_code_;
  };
  class StatReplayProcessFunctor
  {
  public:
    explicit StatReplayProcessFunctor()
        : ret_code_(common::OB_SUCCESS),
          replayed_log_size_(0),
          unreplayed_log_size_(0) {}
    ~StatReplayProcessFunctor(){}
    bool operator()(const share::ObLSID &id, ObReplayStatus *replay_status);
    int get_ret_code() const { return ret_code_; }
    int64_t get_replayed_log_size() const { return replayed_log_size_; }
    int64_t get_unreplayed_log_size() const { return unreplayed_log_size_; }
    TO_STRING_KV(K(ret_code_), K(replayed_log_size_), K(unreplayed_log_size_));
  private:
    int ret_code_;
    int64_t replayed_log_size_;
    int64_t unreplayed_log_size_;
  };
  class FetchLogFunctor
  {
  public:
    explicit FetchLogFunctor()
        : ret_code_(common::OB_SUCCESS) {}
    ~FetchLogFunctor(){}
    bool operator()(const share::ObLSID &id, ObReplayStatus *replay_status);
    int get_ret_code() const { return ret_code_; }
    TO_STRING_KV(K(ret_code_));
  private:
    int ret_code_;
  };
public:
  void handle(void *task);
  int add_ls(const share::ObLSID &id);
  int remove_ls(const share::ObLSID &id);
  int enable(const share::ObLSID &id,
             const palf::LSN &base_lsn,
             const share::SCN &base_scn);
  int disable(const share::ObLSID &id);
  int is_enabled(const share::ObLSID &id, bool &is_enabled);
  int block_submit_log(const share::ObLSID &id);
  int unblock_submit_log(const share::ObLSID &id);
  int switch_to_leader(const share::ObLSID &id);
  int switch_to_follower(const share::ObLSID &id,
                         const palf::LSN &begin_lsn);
  int is_replay_done(const share::ObLSID &id,
                     const palf::LSN &end_lsn,
                     bool &is_done);
  int get_max_replayed_scn(const share::ObLSID &id, share::SCN &scn);
  int submit_task(ObReplayServiceTask *task);
  int update_replayable_point(const share::SCN &replayable_scn);
  int get_replayable_point(share::SCN &replayable_scn);
  int stat_for_each(const common::ObFunction<int (const ObReplayStatus &)> &func);
  int stat_all_ls_replay_process(int64_t &replayed_log_size, int64_t &unreplayed_log_size);
  int diagnose(const share::ObLSID &id, ReplayDiagnoseInfo &diagnose_info);
  void inc_pending_task_size(const int64_t log_size);
  void dec_pending_task_size(const int64_t log_size);
  int64_t get_pending_task_size() const;
  void *alloc_replay_task(const int64_t size);
  void free_replay_task(ObLogReplayTask *task);
  void free_replay_task_log_buf(ObLogReplayTask *task);
private:
  int get_replay_status_(const share::ObLSID &id,
                         ObReplayStatusGuard &guard);
  int pre_check_(ObReplayStatus &replay_status,
                 ObReplayServiceTask &task);
  void process_replay_ret_code_(const int ret_code,
                                ObReplayStatus &replay_status,
                                ObReplayServiceReplayTask &task_queue,
                                ObLogReplayTask &replay_task);
  void revert_replay_status_(ObReplayStatus *replay_status);
  int try_submit_remained_log_replay_task_(ObReplayServiceSubmitTask *submit_task);
  int fetch_and_submit_single_log_(ObReplayStatus &replay_status,
                                   ObReplayServiceSubmitTask *submit_task,
                                   palf::LSN &cur_lsn,
                                   share::SCN &cur_log_submit_scn,
                                   int64_t &log_size);
  int fetch_pre_barrier_log_(ObReplayStatus &replay_status,
                             ObReplayServiceSubmitTask *submit_task,
                             ObLogReplayTask *&replay_task,
                             const ObLogBaseHeader &header,
                             const char *log_buf,
                             const palf::LSN &cur_lsn,
                             const share::SCN &cur_log_submit_scn,
                             const int64_t log_size,
                             const bool is_raw_write);
  bool is_tenant_out_of_memory_() const;
  int handle_submit_task_(ObReplayServiceSubmitTask *submit_task,
                          bool &is_timeslice_run_out);
  int handle_replay_task_(ObReplayServiceReplayTask *task_queue,
                          bool &is_timeslice_run_out);
  int check_can_submit_log_replay_task_(ObLogReplayTask *replay_task,
                                        ObReplayStatus *replay_status);
  int do_replay_task_(ObLogReplayTask *replay_task,
                      ObReplayStatus *replay_status,
                      const int64_t replay_queue_idx);
  int submit_log_replay_task_(ObLogReplayTask &replay_task,
                              ObReplayStatus &replay_status);
  void statistics_replay_(const int64_t replay_task_used,
                          const int64_t destroy_task_used,
                          const int64_t retry_count);
  void statistics_submit_(const int64_t submit_task_used,
                          const int64_t log_size,
                          const int64_t log_count);
  int statistics_replay_cost_(const int64_t init_task_time,
                               const int64_t first_handle_time);
  void on_replay_error_(ObLogReplayTask &replay_task, int ret);
  void on_replay_error_();
  // 析构前调用,归还所有日志流的replay status计数
  int remove_all_ls_();
private:
  const int64_t MAX_REPLAY_TIME_PER_ROUND = 10 * 1000; //10ms
  const int64_t MAX_SUBMIT_TIME_PER_ROUND = 100 * 1000; //100ms
  const int64_t TASK_QUEUE_WAIT_IN_GLOBAL_QUEUE_TIME_THRESHOLD = 5 * 1000 * 1000; //5s
  const int64_t PENDING_TASK_MEMORY_LIMIT = 128 * (1LL << 20); //128MB
  //每个日志流累计拉日志到阈值时batch提交所有task queue
  static const int64_t BATCH_PUSH_REPLAY_TASK_COUNT_THRESOLD = 1024;
  static const int64_t BATCH_PUSH_REPLAY_TASK_SIZE_THRESOLD = 16 * (1LL << 20); //16MB
  // params of adaptive thread pool
  const int64_t LEAST_THREAD_NUM = 8;
  const int64_t ESTIMATE_TS = 200000;
  const int64_t EXPAND_RATE = 90;
  const int64_t SHRINK_RATE = 75;
private:
  bool is_inited_;
  bool is_running_;
  int tg_id_;
  ReplayProcessStat replay_stat_;
  ObLSAdapter *ls_adapter_;
  palf::PalfEnv *palf_env_;
  ObILogAllocator *allocator_;
  share::SCN replayable_point_;
  // 考虑到迁出迁入场景, 不能只通过map管理replay status的生命周期
  common::ObLinearHashMap<share::ObLSID, ObReplayStatus*> replay_status_map_;
  int64_t pending_replay_log_size_;
  ObMiniStat::ObStatItem wait_cost_stat_;
  ObMiniStat::ObStatItem replay_cost_stat_;
  DISALLOW_COPY_AND_ASSIGN(ObLogReplayService);
};

} // namespace replayservice
} // namespace oceanbase

#endif // OCEANBASE_LOGSERVICE_OB_LOG_REPLAY_SERVICE_
