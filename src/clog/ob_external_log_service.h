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

#ifndef OCEANBASE_CLOG_OB_EXTERNAL_LOG_SERVICE_
#define OCEANBASE_CLOG_OB_EXTERNAL_LOG_SERVICE_

#include "lib/task/ob_timer.h"
#include "ob_log_external_rpc.h"
#include "ob_external_fetcher.h"
#include "ob_archive_log_fetcher.h"
#include "ob_external_start_log_locator.h"
#include "ob_external_heartbeat_handler.h"
#include "ob_external_leader_heartbeat_handler.h"
#include "ob_log_line_cache.h"  // ObLogLineCache

namespace oceanbase {
namespace clog {
class ObILogEngine;
class ObICLogMgr;
}  // namespace clog
namespace storage {
class ObPartitionService;
}
namespace logservice {

/*
 * ObExtLogService is a log service interface class that serves liboblog,
 * and liboblog requests are routed to various service components through this interface.
 * It includes four components:
 * >  ObExtStartLogLocator: Given a timestamp (specified when liboblog restart) to determine
 *                          from which log_id each partition will be pulled.
 * >  ObExtLogFetcher: streaming pull operator
 * >  ObExtHeartbeatHandler: The old version of Heartbeat (supports querying the timestamp of older logs)
 *                           is obsolete and is reserved for compatibility consideration.
 * >  ObExtLeaderHeartbeatHandler: The new version of the heartbeat, called LeaderHeartbeat,
 *                                 returns the next log and forecast timestamp.
 */
class ObExtLogService {
  // Maximum number of CLOG files to be buffered in LINE CACHE
  static const int64_t LINE_CACHE_MAX_CACHE_FILE_COUNT = 32;  // 32 * 64M = 2G
  // Fixed size occupied by LINE CACHE (in the number of CLOG files)
  static const int64_t LINE_CACHE_FIXED_SIZE_IN_FILE_COUNT = 3;
  static const int64_t MINI_MODE_LINE_CACHE_FIXED_SIZE_IN_FILE_COUNT = 1;
  static const int64_t LINE_CACHE_FIXED_SIZE_IN_FILE_COUNT_FOR_LOG_ARCHIVE = 1;

public:
  /*
   * Timed tasks used by ExtLogService include two types of tasks:
   * >  regularly wash the old stream -- ObExtLogService::wash_expired_stream
   * >  regularly print information of each stream (debugging monitoring)--
   *    ObExtLogService::print_all_stream
   */
  class StreamTimerTask : public common::ObTimerTask {
  public:
    StreamTimerTask() : els_(NULL)
    {}
    ~StreamTimerTask()
    {
      els_ = NULL;
    }
    int init(ObExtLogService* els);
    virtual void runTimerTask();

  public:
    // misc sub-task frequency
    static const int64_t TIMER_INTERVAL = 1000 * 1000;

  private:
    ObExtLogService* els_;
  };
  class LineCacheTimerTask : public common::ObTimerTask {
  public:
    LineCacheTimerTask() : els_(NULL)
    {}
    ~LineCacheTimerTask()
    {
      els_ = NULL;
    }
    int init(ObExtLogService* els);
    virtual void runTimerTask();

  public:
    // misc sub-task frequency
    static const int64_t TIMER_INTERVAL = 100 * 1000;  // 100ms once
    static const int64_t LINE_CACHE_STAT_INTERVAL = 10 * 1000 * 1000;
    static const int64_t LINE_CACHE_WASH_INTERVAL = 100 * 1000;  // 100ms eliminate once
  private:
    ObExtLogService* els_;
  };

public:
  ObExtLogService()
      : is_inited_(false),
        clog_mgr_(NULL),
        line_cache_(),
        log_archive_line_cache_(),
        locator_(),
        fetcher_(),
        hb_handler_(),
        leader_hb_handler_()
  {}
  ~ObExtLogService()
  {
    destroy();
  }
  int init(storage::ObPartitionService* partition_service, clog::ObILogEngine* log_engine, clog::ObICLogMgr* clog_mgr,
      const common::ObAddr& addr);
  void destroy();
  // The following interface is the entrance to service liboblog,
  // and the internal implementation is to call the corresponding interface
  // of the corresponding component
  int req_start_log_id_by_ts_with_breakpoint(const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint& req_msg,
      obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint& result);
  int open_stream(const obrpc::ObLogOpenStreamReq& req, const common::ObAddr& addr, obrpc::ObLogOpenStreamResp& resp);
  int fetch_log(const obrpc::ObLogStreamFetchLogReq& req, obrpc::ObLogStreamFetchLogResp& resp, const int64_t send_ts,
      const int64_t recv_ts);
  // for log archive
  int archive_fetch_log(
      const common::ObPGKey& pg_key, const clog::ObReadParam& param, clog::ObReadBuf& rbuf, clog::ObReadRes& res);
  int req_heartbeat_info(
      const obrpc::ObLogReqHeartbeatInfoRequest& req_msg, obrpc::ObLogReqHeartbeatInfoResponse& response);
  int leader_heartbeat(const obrpc::ObLogLeaderHeartbeatReq& req_msg, obrpc::ObLogLeaderHeartbeatResp& resp);
  int wash_expired_stream();
  int report_all_stream();
  void line_cache_stat()
  {
    line_cache_.stat();
    log_archive_line_cache_.stat();
  }
  void line_cache_wash()
  {
    line_cache_.wash();
    log_archive_line_cache_.wash();
  }

private:
  bool is_inited_;
  clog::ObICLogMgr* clog_mgr_;
  // log service global Line Cache
  clog::ObLogLineCache line_cache_;              // for liboblog
  clog::ObLogLineCache log_archive_line_cache_;  // for log_archive
  ObExtStartLogLocator locator_;
  ObExtLogFetcher fetcher_;
  ObArchiveLogFetcher archive_log_fetcher_;
  ObExtHeartbeatHandler hb_handler_;
  ObExtLeaderHeartbeatHandler leader_hb_handler_;
};

}  // namespace logservice
}  // namespace oceanbase

#endif
