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

#ifndef OCEANBASE_CLOG_OB_EXTERNAL_TRAFFIC_CONTROLLER_
#define OCEANBASE_CLOG_OB_EXTERNAL_TRAFFIC_CONTROLLER_

namespace oceanbase {
namespace logservice {
// ext_log_service flow limiting controller
// TODO:
// flow control is currently not working,
// the flow control is temporarily banned, need add
//
// Subsequent consideration is to implement flow control at the request level,
// control whether ack packet needs to wait, and control the ack frequency.
class ObExtTrafficController {
public:
  ObExtTrafficController()
  {
    reset();
  }
  ~ObExtTrafficController()
  {}
  void reset()
  {
    last_limit_ts_ = common::ObTimeUtility::current_time() - TIRED_INTERVAL;
    traffic_size_ = 0;
    read_clog_disk_count_ = 0;
    read_ilog_disk_count_ = 0;
    read_info_block_disk_count_ = 0;
  }
  void add_traffic_size(const int64_t size)
  {
    ATOMIC_AAF(&traffic_size_, size);
  }
  void add_read_clog_disk_count(const int64_t count)
  {
    ATOMIC_AAF(&read_clog_disk_count_, count);
  }
  void add_read_ilog_disk_count(const int64_t count)
  {
    ATOMIC_AAF(&read_ilog_disk_count_, count);
  }
  void add_read_info_block_disk_count(const int64_t count)
  {
    ATOMIC_AAF(&read_info_block_disk_count_, count);
  }

  inline bool exceed_limit() const
  {
    return ATOMIC_LOAD(&traffic_size_) > TRAFFIC_SIZE_LIMIT ||
           (ATOMIC_LOAD(&read_clog_disk_count_) + ATOMIC_LOAD(&read_ilog_disk_count_) +
               ATOMIC_LOAD(&read_info_block_disk_count_)) > READ_DISK_COUNT_LIMIT;
  }

  inline void reset_statistic()
  {
    ATOMIC_STORE(&traffic_size_, 0);
    ATOMIC_STORE(&read_clog_disk_count_, 0);
    ATOMIC_STORE(&read_ilog_disk_count_, 0);
    ATOMIC_STORE(&read_info_block_disk_count_, 0);
  }

  bool is_limited()
  {
    // give a limit based on the monitor result, FIXME remove later
    if (REACH_TIME_INTERVAL(20 * 1000 * 1000)) {
      EXTLOG_LOG(INFO, "traffic controller monitor result", K(*this));
      reset_statistic();
    }

    bool tired = false;
    const int64_t now = common::ObTimeUtility::current_time();
    if (now < ATOMIC_LOAD(&last_limit_ts_) + TIRED_INTERVAL) {  // forbidden
      tired = true;
    } else {
      tired = exceed_limit();
      if (tired) {
        EXTLOG_LOG(INFO,
            "fetch log request is limited by traffic_controller_",
            "last_limit_ts_",
            ATOMIC_LOAD(&last_limit_ts_),
            "traffic_size_",
            ATOMIC_LOAD(&traffic_size_),
            "read_clog_disk_count_",
            ATOMIC_LOAD(&read_clog_disk_count_),
            "read_ilog_disk_count_",
            ATOMIC_LOAD(&read_ilog_disk_count_),
            "read_info_block_disk_count_",
            ATOMIC_LOAD(&read_info_block_disk_count_));
        ATOMIC_STORE(&last_limit_ts_, now);
        reset_statistic();
      }
    }
    return tired;
  }

  // to_string function
  TO_STRING_KV("last_limit_ts_", ATOMIC_LOAD(&last_limit_ts_), "traffic_size_", ATOMIC_LOAD(&traffic_size_),
      "read_clog_disk_count_", ATOMIC_LOAD(&read_clog_disk_count_), "read_ilog_disk_count_",
      ATOMIC_LOAD(&read_ilog_disk_count_), "read_info_block_disk_count_", ATOMIC_LOAD(&read_info_block_disk_count_));

private:
  static const int64_t C = 100;                                     // coefficient
  static const int64_t TIRED_INTERVAL = 1000 * 1000;                // 1 second
  static const int64_t TRAFFIC_SIZE_LIMIT = 800 * 1024 * 1024 * C;  // 800 M
  static const int64_t READ_DISK_COUNT_LIMIT = 8192 * C;            // 8K
private:
  int64_t last_limit_ts_;
  int64_t traffic_size_;
  int64_t read_clog_disk_count_;        // times of disk reads when reading clog
  int64_t read_ilog_disk_count_;        // times of disk reads when reading ilog
  int64_t read_info_block_disk_count_;  // times of disk reads when reading ilog InfoBlock
};
}  // namespace logservice
}  // namespace oceanbase

#endif
