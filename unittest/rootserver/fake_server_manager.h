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

#ifndef OCEANBASE_ROOTSERVER_TEST_FAKE_SERVER_MANAGER_H
#define OCEANBASE_ROOTSERVER_TEST_FAKE_SERVER_MANAGER_H
#include "rootserver/ob_server_manager.h"
#include "lib/container/ob_array_iterator.h"
#include "share/ob_server_status.h"
#include "rootserver/ob_zone_manager.h"
namespace oceanbase
{
namespace rootserver
{
class FakeServerMgr :  public ObServerManager
  {
  public:
    FakeServerMgr() : server_id_(0) {}
    ~FakeServerMgr() {}
    void fake_init(ObZoneManager *zone_mgr) { inited_ = true; zone_mgr_ = zone_mgr; }
    int add_server(const common::ObAddr &server, const ObZone &zone);
    virtual int get_server_statuses(const common::ObZone &zone,
                                    common::ObArray<share::ObServerStatus> &server_statuses) const
    {
      UNUSED(zone); // always get all zone
      return server_statuses.assign(server_statuses_);
    }

  private:
    int64_t server_id_;
    ObZoneManager * zone_mgr_;
  };
int FakeServerMgr::add_server(const common::ObAddr &server, const ObZone &zone)
{
  int ret = common::OB_SUCCESS;
  share::ObServerStatus *server_status = NULL;
  if (!server.is_valid() || OB_ISNULL(zone_mgr_)) {
    ret = OB_INVALID_ARGUMENT;
    RS_LOG(WARN, "invalid server", K(server), K(zone_mgr_), K(ret));
  } else if (!zone.is_empty()) {
    bool zone_active = false;
    if (OB_FAIL(zone_mgr_->check_zone_active(zone, zone_active))) {
      RS_LOG(WARN, "check_zone_active failed", K(zone), K(ret));
    } else if (!zone_active) {
      ret = common::OB_ZONE_NOT_ACTIVE;
      RS_LOG(WARN, "zone not active", K(zone), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_SUCC(find(server, server_status))) {
      if (NULL == server_status) {
        ret = common::OB_ERR_UNEXPECTED;
        RS_LOG(WARN, "server_status is null", K(ret));
      } else if (!server_status->is_status_valid()) {
        ret = common::OB_ERR_UNEXPECTED;
        RS_LOG(WARN, "status not valid", K(ret), "status", *server_status);
      } else {
        ret = common::OB_ENTRY_EXIST;
        RS_LOG(WARN, "server already added", K(server), K(ret));
      }
    } else if (common::OB_ENTRY_NOT_EXIST != ret) {
      RS_LOG(WARN, "find failed", K(server), K(ret));
    } else {
      ret = common::OB_SUCCESS;
      share::ObServerStatus new_server_status;
      new_server_status.id_ = server_id_++;
      new_server_status.server_ = server;
      new_server_status.zone_ = zone;
      new_server_status.admin_status_ = share::ObServerStatus::OB_SERVER_ADMIN_NORMAL;
      new_server_status.hb_status_ = share::ObServerStatus::OB_HEARTBEAT_ALIVE;
      new_server_status.merged_version_ = 2;
      if (OB_FAIL(server_statuses_.push_back(new_server_status))) {
        RS_LOG(WARN, "push_back failed", K(ret));
      }
    }
  }
  RS_LOG(INFO, "add new server", K(server), K(zone), K(ret));
  return ret;
}

class FakeServerManager : public ObServerManager
{
public:
  void set_is_inited(bool is_inited)
  {
    UNUSED(is_inited);
    //inited_ = is_inited;
  }

  void set(const common::ObZone &zone, const common::ObAddr &addr,
      share::ObServerStatus::DisplayStatus stat = share::ObServerStatus::OB_SERVER_ACTIVE)
  {
    share::ObServerStatus *server = NULL;
    FOREACH(s, all_) {
      if (s->server_ == addr) {
        server = &(*s);
      }
    }
    if (NULL == server) {
      share::ObServerStatus new_server;
      new_server.server_ = addr;
      all_.push_back(new_server);
      server = &all_.at(all_.count() - 1);
    }
    server->zone_ = zone;
    server->admin_status_ = share::ObServerStatus::OB_SERVER_ADMIN_NORMAL;
    if (share::ObServerStatus::OB_SERVER_ACTIVE == stat) {
      server->hb_status_ = share::ObServerStatus::OB_HEARTBEAT_ALIVE;
      server->start_service_time_ = common::ObTimeUtility::current_time();
    } else {
      server->hb_status_ = share::ObServerStatus::OB_HEARTBEAT_LEASE_EXPIRED;
      server->start_service_time_ = 0;
    }
    if (share::ObServerStatus::OB_SERVER_DELETING == stat) {
      server->admin_status_ = share::ObServerStatus::OB_SERVER_ADMIN_DELETING;
    }
  }

  virtual int check_server_alive(const common::ObAddr &server, bool &alive) const
  {
    int ret = common::OB_SUCCESS;
    alive = false;
    FOREACH(s, all_) {
      if (s->server_ == server) {
        alive = s->is_alive();
        break;
      }
    }
    return ret;
  }

  int set_server_version(const common::ObAddr &server, const char *version)
  {
    int ret = common::OB_SUCCESS;
    FOREACH(s, all_) {
      if (s->server_ == server) {
        MEMCPY(s->build_version_, version, common::OB_SERVER_VERSION_LENGTH);
        break;
      }
    }
    return ret;
  }
  int get_min_server_version(char min_server_version[OB_SERVER_VERSION_LENGTH])
  {
    int ret = OB_SUCCESS;
    ObClusterVersion version_parser;
    uint64_t cur_min_version = UINT64_MAX;
    FOREACH(s, all_) {
      char *saveptr = NULL;
      char *version = STRTOK_R(s->build_version_, "_", &saveptr);
      if (NULL == version || strlen(version) + 1 > OB_SERVER_VERSION_LENGTH) {
        ret = OB_INVALID_ARGUMENT;
        RS_LOG(WARN, "invalid build version format", "build_version", s->build_version_);
      } else if (OB_FAIL(version_parser.refresh_cluster_version(version))) {
        RS_LOG(WARN, "failed to parse version", "version", version);
      } else {
        if (version_parser.get_cluster_version() < cur_min_version) {
          size_t len = strlen(version);
          MEMCPY(min_server_version, version, len);
          min_server_version[len] = '\0';
          cur_min_version = version_parser.get_cluster_version();
        }
      }
    }
    return ret;
  }

  virtual int check_server_active(const common::ObAddr &server, bool &active) const
  {
    int ret = common::OB_SUCCESS;
    active = false;
    FOREACH(s, all_) {
      if (s->server_ == server) {
        active = s->is_active();
        break;
      }
    }
    return ret;
  }

  virtual int check_in_service(const common::ObAddr &server, bool &in_service) const
  {
    int ret = common::OB_SUCCESS;
    bool exist = false;
    in_service = false;
    FOREACH(s, all_) {
      if (s->server_ == server) {
        in_service = s->in_service();
        exist = true;
        break;
      }
    }
    if (!exist) {
      ret = common::OB_ENTRY_NOT_EXIST;
    }
    return ret;
  }

  virtual int check_migrate_in_blocked(const common::ObAddr &server, bool &blocked) const
  {
    int ret = common::OB_SUCCESS;
    bool exist = false;
    blocked = false;
    FOREACH(s, all_) {
      if (s->server_ == server) {
        blocked = s->is_migrate_in_blocked();
        exist = true;
        break;
      }
    }
    if (!exist) {
      ret = common::OB_ENTRY_NOT_EXIST;
    }
    return ret;
  }

  virtual int get_server_statuses(const common::ObZone &zone,
      common::ObArray<share::ObServerStatus> &server_statuses) const
  {
    UNUSED(zone); // always get all zone
    return server_statuses.assign(all_);
  }
  int get_servers_of_zone(const common::ObZone &zone,
      ObServerManager::ObServerArray &server_list) const
  {
    int ret = common::OB_SUCCESS;
    FOREACH_X(s, all_, common::OB_SUCCESS == ret) {
      if (zone.is_empty() || zone == s->zone_) {
        if (OB_FAIL(server_list.push_back(s->server_))) {
          RS_LOG(WARN, "add server failed", K(ret));
        }
      }
    }
    return ret;
  }

  virtual int get_server_status(const common::ObAddr &server,
      share::ObServerStatus &server_status)
  {
    int ret = common::OB_ENTRY_NOT_EXIST;
    FOREACH(s, all_) {
      if (s->server_ == server) {
        server_status = *s;
        ret = common::OB_SUCCESS;
      }
    }
    return ret;
  }

  int get_alive_servers(const common::ObZone &zone,
      ObServerManager::ObIServerArray &server_list) const
  {
    int ret = common::OB_SUCCESS;
    FOREACH_X(s, all_, common::OB_SUCCESS == ret) {
      if (!s->is_alive()) {
        continue;
      }
      if (zone.is_empty() || zone == s->zone_) {
        if (OB_FAIL(server_list.push_back(s->server_))) {
          RS_LOG(WARN, "add server failed", K(ret));
        }
      }
    }
    return ret;
  }
  int get_server_zone(const common::ObAddr &addr, common::ObZone &zone) const
  {
    int ret = common::OB_ENTRY_NOT_EXIST;
    FOREACH(s, all_) {
      if (s->server_ == addr) {
        zone = s->zone_;
        ret = common::OB_SUCCESS;
      }
    }
    return ret;
  }

  share::ObServerStatus *get(const common::ObAddr &addr)
  {
    share::ObServerStatus *stat = NULL;
    FOREACH(s, all_) {
      if (s->server_ == addr) {
        stat = &(*s);
      }
    }
    return stat;
  }

  int delete_server(const common::ObAddr &server, const common::ObZone &zone)
  {
    int ret = common::OB_SUCCESS;
    UNUSED(zone);
    FOREACH(s, all_) {
      if (s->server_ == server) {
        s->admin_status_ = share::ObServerStatus::OB_SERVER_ADMIN_DELETING;
        break;
      }
    }
    return ret;
  }

  int end_delete_server(const common::ObAddr &server, const common::ObZone &zone,
                        const bool commit)
  {
    int ret = common::OB_SUCCESS;
    UNUSED(zone);
    int64_t index = -1;
    for (int64_t i = 0; i < all_.count(); ++i) {
      if (all_.at(i).server_ == server) {
        index = i;
      }
    }
    if (-1 != index) {
      if (commit) {
        if (OB_FAIL(all_.remove(index))) {
          RS_LOG(WARN, "remove failed", K(index), K(ret));
        }
      } else {
        all_.at(index).admin_status_ = share::ObServerStatus::OB_SERVER_ADMIN_NORMAL;
      }
    }
    return ret;
  }

  int set_with_partition(const common::ObAddr &server)
  {
    UNUSED(server);
    return common::OB_SUCCESS;
  }

  int set_force_stop_hb(const common::ObAddr &server, const bool &force_stop_hb)
  {
    int ret = common::OB_SUCCESS;
    int64_t index = -1;
    for (int64_t i = 0; i < all_.count(); ++i) {
      if (all_.at(i).server_ == server) {
        index = i;
      }
    }
    if (-1 != index) {
      if (share::ObServerStatus::OB_SERVER_ADMIN_DELETING == all_.at(index).admin_status_) {
        all_.at(index).force_stop_hb_ = force_stop_hb;
      }
    }
    return ret;
  }

  void set_inited(bool inited)
  {
    inited_ = inited;
  }
public:
  common::ObArray<share::ObServerStatus> all_;
};

} // end namespace rootserver
} // end namespace oceanbase
#endif
