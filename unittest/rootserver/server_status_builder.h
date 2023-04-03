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

#define USING_LOG_PREFIX RS

#include "share/config/ob_server_config.h"
#include "share/ob_server_status.h"
#include "rootserver/ob_server_manager.h"
#include "lib/container/ob_array_iterator.h"

namespace oceanbase
{
namespace rootserver
{
class ObServerStatusBuilder
{
public:
  ObServerStatusBuilder() : config_(NULL), server_statuses_(), server_id_(OB_INIT_SERVER_ID) {}
  int init(ObServerConfig &config) { config_ = &config; return OB_SUCCESS; }
  void set_resource_info(const double cpu, const int64_t mem, const int64_t disk);
  ObServerStatusBuilder &add(share::ObServerStatus::DisplayStatus status,
                             const int64_t now,
                             const ObAddr &server,
                             const ObZone &zone);
  ObServerManager::ObServerStatusArray &get_server_statuses() { return server_statuses_; }
  void reset() { server_statuses_.reset(); }
  int build(ObServerManager &server_mgr);
private:
  ObServerConfig *config_;
  share::ObServerResourceInfo resource_;
  ObServerManager::ObServerStatusArray server_statuses_;
  uint64_t server_id_;
};

void ObServerStatusBuilder::set_resource_info(const double cpu,
                                              const int64_t mem,
                                              const int64_t disk)
{
  resource_.cpu_ = cpu;
  resource_.mem_in_use_ = 0;
  resource_.mem_total_ = mem;
  resource_.disk_in_use_ = 0;
  resource_.disk_total_ = disk;
}

ObServerStatusBuilder &ObServerStatusBuilder::add(share::ObServerStatus::DisplayStatus status,
                                                  const int64_t now,
                                                  const ObAddr &server,
                                                  const ObZone &zone)
{
  share::ObServerStatus server_status;
  if (share::ObServerStatus::OB_SERVER_INACTIVE == status) {
    // lease outdate 4s
    server_status.last_hb_time_ = now - config_->lease_time - 4000000;
    server_status.admin_status_ = share::ObServerStatus::OB_SERVER_ADMIN_NORMAL;
    server_status.hb_status_ = share::ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED;
  } else if (share::ObServerStatus::OB_SERVER_ACTIVE == status) {
    server_status.last_hb_time_ = now - 1000000;
    server_status.admin_status_ = share::ObServerStatus::OB_SERVER_ADMIN_NORMAL;
    server_status.hb_status_ = share::ObServerStatus::OB_HEARTBEAT_ALIVE;
  } else if (share::ObServerStatus::OB_SERVER_DELETING == status) {
    server_status.last_hb_time_ = 0;
    server_status.admin_status_ = share::ObServerStatus::OB_SERVER_ADMIN_DELETING;
    server_status.hb_status_ = share::ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED;
  }
  OB_ASSERT(strlen("dev1.0") == snprintf(server_status.build_version_, OB_SERVER_VERSION_LENGTH,
      "%s", "dev1.0"));
  server_status.zone_ = zone;
  server_status.server_ = server;
  server_status.resource_info_ = resource_;
  server_status.id_ = server_id_;
  server_status.start_service_time_ = ObTimeUtility::current_time();
  OB_ASSERT(OB_SUCCESS == server_statuses_.push_back(server_status));
  ++server_id_;
  return *this;
}

inline int ObServerStatusBuilder::build(ObServerManager &server_mgr)
{
  int ret = server_mgr.load_server_statuses(server_statuses_);
  // build_server_status will change alive status to lease expired
  if (OB_SUCC(ret)) {
    FOREACH_X(s, server_statuses_, OB_SUCCESS == ret) {
      if (!s->is_alive()) {
        continue;
      }
      share::ObServerStatus ss;
      if (OB_FAIL(server_mgr.get_server_status(s->server_, ss))) {
      } else {
        ss.hb_status_ = share::ObServerStatus::OB_HEARTBEAT_ALIVE;
        if (OB_FAIL(server_mgr.update_server_status(ss))) {
        }
      }
    }
  }
  return ret;
}

}//end namespace rootserver
}//end namespace oceanbase
