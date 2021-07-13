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

#ifndef OCEANBASE_CLOG_OB_LOG_EXTERNAL_MONITOR_
#define OCEANBASE_CLOG_OB_LOG_EXTERNAL_MONITOR_

#include "ob_log_define.h"

namespace oceanbase {
namespace clog {
// statistic for external rpc request
class ObExtReqStatistic {
public:
  static void reset_all()
  {
    ts.reset();
    id.reset();
    fetch.reset();
    hb.reset();
  }
  // ts statistic
  static void ts_inc_req_count()
  {
    ATOMIC_INC(&(ts.req_count_));
  }
  static void ts_inc_err()
  {
    ATOMIC_INC(&(ts.err_count_));
  }
  static void ts_inc_pkey_count(const int64_t pkey_count)
  {
    (void)ATOMIC_FAA(&(ts.pkey_count_), pkey_count);
  }
  static void ts_inc_scan_file_count(int64_t file_count)
  {
    (void)ATOMIC_FAA(&(ts.scan_file_count_), file_count);
  }
  static void ts_inc_perf_total(const int64_t total_time)
  {
    (void)ATOMIC_FAA(&(ts.perf_total_), total_time);
  }

  // id statistic
  static void id_inc_req_count()
  {
    ATOMIC_INC(&(id.req_count_));
  }
  static void id_inc_err()
  {
    ATOMIC_INC(&(id.err_count_));
  }
  static void id_inc_pkey_count(const int64_t pkey_count)
  {
    (void)ATOMIC_FAA(&(id.pkey_count_), pkey_count);
  }
  static void id_inc_scan_file_count(int64_t file_count)
  {
    (void)ATOMIC_FAA(&(id.scan_file_count_), file_count);
  }
  static void id_inc_perf_total(const int64_t total_time)
  {
    (void)ATOMIC_FAA(&(id.perf_total_), total_time);
  }

  // fetch statistic
  static void fetch_inc_req_count()
  {
    ATOMIC_INC(&(fetch.req_count_));
  }
  static void fetch_inc_err()
  {
    ATOMIC_INC(&(fetch.err_count_));
  }
  static void fetch_inc_pkey_count(const int64_t pkey_count)
  {
    (void)ATOMIC_AAF(&(fetch.pkey_count_), pkey_count);
  }
  static void fetch_inc_scan_file_count(const int64_t file_count)
  {
    (void)ATOMIC_AAF(&(fetch.scan_file_count_), file_count);
  }
  static void fetch_inc_perf_total(int64_t total_time)
  {
    (void)ATOMIC_AAF(&(fetch.perf_total_), total_time);
  }
  static void fetch_inc_log_num(int64_t num)
  {
    (void)ATOMIC_AAF(&(fetch.log_num_), num);
  }

  // heartbeat statistic
  static void hb_inc_req()
  {
    ATOMIC_INC(&(hb.req_count_));
  }
  static void hb_inc_err()
  {
    ATOMIC_INC(&(hb.err_count_));
  }
  static void hb_inc_pkey_count(const int64_t pkey_count)
  {
    (void)ATOMIC_AAF(&(hb.pkey_count_), pkey_count);
  }
  static void hb_inc_disk_pkey_count(const int64_t disk_pkey_count)
  {
    (void)ATOMIC_AAF(&(hb.disk_pkey_count_), disk_pkey_count);
  }
  static void hb_inc_disk_count()
  {
    ATOMIC_INC(&(hb.disk_count_));
  }
  static void hb_inc_perf_total(int64_t total_time)
  {
    (void)ATOMIC_AAF(&(fetch.perf_total_), total_time);
  }

  // print
  static void print_statistic_info(bool force_print = false)
  {
    static const int64_t step = 500;
    static int64_t count = 0;
    if (force_print || (0 == ((ATOMIC_FAA(&count, 1)) % step))) {
      EXTLOG_LOG(INFO, "print statistic info:", K(ts), K(id), K(fetch), K(hb));
    }
  }

private:
  struct ReqByTsStatistic {
    int64_t req_count_;
    int64_t pkey_count_;
    int64_t err_count_;
    int64_t scan_file_count_;
    int64_t perf_total_;

    void reset()
    {
      req_count_ = 0;
      pkey_count_ = 0;
      err_count_ = 0;
      scan_file_count_ = 0;
      perf_total_ = 0;
    }
    TO_STRING_KV(K(req_count_), K(pkey_count_), K(err_count_), K(scan_file_count_), K(perf_total_));
  };

  struct ReqByIdStatistic {
    int64_t req_count_;
    int64_t pkey_count_;
    int64_t err_count_;
    int64_t scan_file_count_;
    int64_t perf_total_;

    void reset()
    {
      req_count_ = 0;
      pkey_count_ = 0;
      err_count_ = 0;
      scan_file_count_ = 0;
      perf_total_ = 0;
    }
    TO_STRING_KV(K(req_count_), K(pkey_count_), K(err_count_), K(scan_file_count_), K(perf_total_));
  };

  struct ReqFetchStatistic {
    int64_t req_count_;
    int64_t err_count_;
    int64_t pkey_count_;
    int64_t scan_file_count_;
    int64_t log_num_;
    int64_t offline_pkey_count_;
    int64_t perf_total_;

    void reset()
    {
      req_count_ = 0;
      err_count_ = 0;
      pkey_count_ = 0;
      scan_file_count_ = 0;
      log_num_ = 0;
      offline_pkey_count_ = 0;
      perf_total_ = 0;
    }
    TO_STRING_KV(
        K(req_count_), K(pkey_count_), K(err_count_), K(scan_file_count_), K(log_num_), K(offline_pkey_count_));
  };

  struct ReqHbStatistic {
    int64_t req_count_;
    int64_t err_count_;
    int64_t pkey_count_;
    int64_t disk_pkey_count_;
    int64_t disk_count_;

    void reset()
    {
      req_count_ = 0;
      err_count_ = 0;
      pkey_count_ = 0;
      disk_pkey_count_ = 0;
      disk_count_ = 0;
    }
    TO_STRING_KV(K(req_count_), K(pkey_count_), K(err_count_), K(disk_pkey_count_), K(disk_count_));
  };

private:
  static ReqByTsStatistic ts;
  static ReqByIdStatistic id;
  static ReqFetchStatistic fetch;
  static ReqHbStatistic hb;
  DISALLOW_COPY_AND_ASSIGN(ObExtReqStatistic);
};
}  // namespace clog
}  // namespace oceanbase
#endif
