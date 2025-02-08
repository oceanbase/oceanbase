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

#ifndef OCEANBASE_LOGSERVICE_OB_LOGSTORE_MGR_
#define OCEANBASE_LOGSERVICE_OB_LOGSTORE_MGR_

#include "lib/utility/ob_macro_utils.h"     // DISALLOW_COPY_AND_ASSIGN
#include "lib/utility/ob_print_utils.h"     // TOS_TRING_KV
#include "lib/net/ob_addr.h"                // ObAddr
#include "share/config/ob_server_config.h"
namespace oceanbase
{
namespace logservice
{
class ObLogGrpcAdapter;

struct LogstoreServiceInfo {
  LogstoreServiceInfo()
    : memory_limit_(0), memory_used_(0), shm_limit_(0), shm_used_(0)
  {}
  ~LogstoreServiceInfo()
  {
    reset();
  }
  void reset();
  TO_STRING_KV(K_(memory_limit), K_(memory_used), K_(shm_limit), K_(shm_used));
  int64_t memory_limit_;
  int64_t memory_used_;
  int64_t shm_limit_;
  int64_t shm_used_;
};

class ObLogstoreMgr
{
public:
  static const int64_t GRPC_TIMEOUT_US = 100 * 1000;
  static bool is_logstore_mode() {
    return (0 == GCONF._ob_flush_log_at_trx_commit);
  }
  static ObLogstoreMgr &get_instance() {
    static ObLogstoreMgr instance;
    return instance;
  }
public:
  ObLogstoreMgr();
  ~ObLogstoreMgr();
  int init();
  void destroy();
  int get_logstore_service_info(LogstoreServiceInfo &logstore_info);

  TO_STRING_KV(K_(logstore_service_addr), K_(logstore_compatible_version), K_(is_inited));
public:
  static int get_logstore_service_status(ObAddr &logstore_service_addr, bool &is_active, int64_t &last_active_ts);
private:
  static int get_logstore_service_addr_(common::ObAddr &logstore_addr);
private:
  common::ObAddr logstore_service_addr_;
  ObLogGrpcAdapter *grpc_adapter_;
  int64_t logstore_compatible_version_;  // compatible version used for logstore
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogstoreMgr);
};

#define LOGSTORE_MGR ::oceanbase::logservice::ObLogstoreMgr::get_instance()
} // namespace logservice
} // namespace oceanbase
#endif
