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

#ifndef OCEANBASE_TRANSACTION_OB_ID_SERVICE_
#define OCEANBASE_TRANSACTION_OB_ID_SERVICE_

#include "share/ob_ls_id.h"
#include "storage/slog/ob_storage_log_struct.h"
#include "logservice/ob_append_callback.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/ob_log_handler.h"
#include "share/scn.h"

namespace oceanbase
{

namespace storage
{
class ObLS;
}
namespace transaction
{

class ObAllIDMeta;

class ObPresistIDLog
{
public:
  OB_UNIS_VERSION(1);

public:
  ObPresistIDLog() {}
  ObPresistIDLog(int64_t last_id, int64_t limit_id)
      : last_id_(last_id), limit_id_(limit_id)
  {}
  const int64_t &get_last_id() { return last_id_; }
  const int64_t &get_limit_id() { return limit_id_; }
  TO_STRING_KV(K(last_id_), K(limit_id_));

private:
  int64_t last_id_;
  int64_t limit_id_;
};

class ObPresistIDLogCb : public logservice::AppendCb
{
public:
  ObPresistIDLogCb() { reset(); }
  ~ObPresistIDLogCb() {}
  void reset();

  int serialize_ls_log(ObPresistIDLog &ls_log, int64_t service_type);
  char *get_log_buf() { return log_buf_; }
  int64_t get_log_pos() { return pos_; }
  void set_log_ts(const share::SCN &ts) { log_ts_ = ts; }
  void set_srv_type(const int64_t srv_type) { id_srv_type_ = srv_type; }
  void set_limited_id(const int64_t limited_id) { limited_id_ = limited_id; }
public:
  int on_success();
  int on_failure();

public:
  TO_STRING_KV(K_(log_ts), K_(log_buf), K_(pos), K_(id_srv_type), K_(limited_id));
  static const int64_t MAX_LOG_BUFF_SIZE = 128;
private:
  int64_t id_srv_type_;
  int64_t limited_id_;
  share::SCN log_ts_;
  char log_buf_[MAX_LOG_BUFF_SIZE];
  int64_t pos_;
};

class ObIDService : public logservice::ObIReplaySubHandler,
                    public logservice::ObICheckpointSubHandler,
                    public logservice::ObIRoleChangeSubHandler
{
public:
  ObIDService() : rwlock_(ObLatchIds::ID_SOURCE_LOCK), log_interval_(100 * 1000) { reset(); }
  virtual ~ObIDService() {}
  void destroy() { reset(); }
  void reset();

  enum ServiceType {
    INVALID_ID_SERVICE_TYPE = -1,
    TimestampService,
    TransIDService,
    DASIDService,
    MAX_SERVICE_TYPE,
  };

  static void get_all_id_service_type(int64_t *service_type);
  static int get_id_service(const int64_t id_service_type, ObIDService *&id_service);
  static int update_id_service(const ObAllIDMeta &id_meta);

  //获取数值或数值组
  int get_number(const int64_t range, const int64_t base_id, int64_t &start_id, int64_t &end_id);

  //日志处理
  int handle_submit_callback(const bool success, const int64_t limited_id, const share::SCN log_ts);
  void test_lock() { WLockGuard guard(rwlock_); }

  //获取回放数据
  int handle_replay_result(const int64_t last_id, const int64_t limited_id, const share::SCN log_ts);

  // clog checkpoint
  int flush(share::SCN &scn);
  share::SCN get_rec_scn();
  int64_t get_rec_log_ts();

  // for clog replay
  int replay(const void *buffer, const int64_t buf_size, const palf::LSN &lsn, const share::SCN &log_ts);

  //切主
  int switch_to_follower_gracefully();
  void switch_to_follower_forcedly() {}
  int resume_leader() { return OB_SUCCESS; }
  int switch_to_leader() { return OB_SUCCESS; }

  int check_leader(bool &leader);
  int check_and_fill_ls();
  void reset_ls();
  void update_limited_id(const int64_t limited_id, const share::SCN latest_log_ts);
  int update_ls_id_meta(const bool write_slog);

  // 虚表
  void get_virtual_info(int64_t &last_id, int64_t &limited_id, share::SCN &rec_log_ts,
                        share::SCN &latest_log_ts, int64_t &pre_allocated_range,
                        int64_t &submit_log_ts, bool &is_master);
  static const int64_t SUBMIT_LOG_ALARM_INTERVAL = 100 * 1000;
  static const int64_t MIN_LAST_ID = 1;
protected:
  typedef common::SpinWLockGuard  WLockGuard;
  typedef common::SpinRLockGuard  RLockGuard;
  //日志提交
  int submit_log_(const int64_t last_id, const int64_t limited_id);
  int submit_log_with_lock_(const int64_t last_id, const int64_t limited_id);
  int64_t max_pre_allocated_id_(const int64_t base_id);
protected:
  ServiceType service_type_;
  //预分配大小
  int64_t pre_allocated_range_;
  // 当前已经分配的最大值
  int64_t last_id_;
  // 回放出的临时状态
  int64_t tmp_last_id_;
  int64_t limited_id_;
  bool is_logging_;
  //checkpoint ts
  share::SCN rec_log_ts_;
  //最新持久化成功的ts
  share::SCN latest_log_ts_;
  // current time when submit log
  int64_t submit_log_ts_;
  mutable common::SpinRWLock rwlock_;
  ObLS *ls_;
  ObPresistIDLogCb cb_;
  common::ObAddr self_;
  common::ObTimeInterval log_interval_;
};

class ObIDMeta
{
public:
  ObIDMeta() : limited_id_(ObIDService::MIN_LAST_ID) {}
  ~ObIDMeta() {}
  int64_t limited_id_;
  share::SCN latest_log_ts_;
  TO_STRING_KV(K_(limited_id), K_(latest_log_ts));
  OB_UNIS_VERSION(1);
};

class ObAllIDMeta
{
public:
  ObAllIDMeta() : count_(ObIDService::MAX_SERVICE_TYPE) {}
  ~ObAllIDMeta() {}
  void update_all_id_meta(const ObAllIDMeta &all_id_meta);
  int update_id_meta(const int64_t service_type,
                     const int64_t limited_id,
                     const share::SCN &latest_log_ts);
  int get_id_meta(const int64_t service_type,
                  int64_t &limited_id,
                  share::SCN &latest_log_ts) const;
  int64_t to_string(char* buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_NAME("id_meta");
    J_COLON();
    (void)databuff_print_obj_array(buf, buf_len, pos, id_meta_, count_);
    J_OBJ_END();
    return pos;
  }
   OB_UNIS_VERSION(1);
private:
  mutable common::ObSpinLock lock_;
  int64_t count_;
  ObIDMeta id_meta_[ObIDService::MAX_SERVICE_TYPE];
};

}
}
#endif
