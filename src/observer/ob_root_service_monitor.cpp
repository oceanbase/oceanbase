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

#define USING_LOG_PREFIX SERVER

#include "ob_root_service_monitor.h"

#include "common/ob_partition_key.h"
#include "share/partition_table/ob_ipartition_table.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "storage/ob_partition_service.h"
#include "rootserver/ob_root_service.h"
#include "observer/ob_server_struct.h"
#include "share/ob_server_status.h"
#include "lib/thread/ob_thread_name.h"
namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace share;
using namespace rootserver;
using namespace storage;
namespace observer {
ObRootServiceMonitor::ObRootServiceMonitor(ObRootService& root_service, ObPartitionService& partition_service_)
    : inited_(false), root_service_(root_service), fail_count_(0), partition_service_(partition_service_)
{}

ObRootServiceMonitor::~ObRootServiceMonitor()
{
  if (inited_) {
    stop();
  }
}

int ObRootServiceMonitor::init()
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    const int thread_count = 1;
    this->set_thread_count(thread_count);
    inited_ = true;
  }
  return ret;
}

void ObRootServiceMonitor::run1()
{
  int ret = OB_SUCCESS;
  ObRSThreadFlag rs_work;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    LOG_INFO("root service monitor thread start");
    lib::set_thread_name("RSMonitor");
    while (!has_set_stop()) {
      if (OB_FAIL(monitor_root_service())) {
        LOG_WARN("monitor root service failed", K(ret));
      }
      if (!has_set_stop()) {
        usleep(MONITOR_ROOT_SERVICE_INTERVAL_US);
      }
    }
    LOG_INFO("root service monitor thread exit");
  }
}

int ObRootServiceMonitor::start()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(share::ObThreadPool::start())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start root service monitor thread failed", K(ret));
  }
  return ret;
}

void ObRootServiceMonitor::stop()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!has_set_stop()) {
    share::ObThreadPool::stop();
  }
}

int ObRootServiceMonitor::monitor_root_service()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const ObPartitionKey root_partition(combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
        ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID,
        ObIPartitionTable::ALL_CORE_TABLE_PARTITION_NUM);
    ObRole role = FOLLOWER;
    if (partition_service_.is_partition_exist(root_partition)) {
      if (OB_FAIL(partition_service_.get_role(root_partition, role))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          role = FOLLOWER;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get partition role failed", K(root_partition), K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (root_service_.is_stopping()) {
      // need exit
      if (OB_FAIL(root_service_.stop_service())) {
        LOG_WARN("root service stop service failed", K(ret));
      } else {
        LOG_INFO("root service stop service finish success");
      }
    } else if (root_service_.is_need_stop()) {
      LOG_INFO("root service is starting, stop_service need wait");
    } else {
      if (is_strong_leader(role)) {
        if (root_service_.in_service()) {
          // already started or is starting
          // nothing todo
        } else if (!root_service_.can_start_service()) {
          LOG_ERROR("bug here. root service can not start service");
        } else {
          LOG_INFO("try to start root service");
          DEBUG_SYNC(BEFORE_START_RS);
          if (OB_FAIL(try_start_root_service())) {
            LOG_WARN("fail to start root service", K(ret));
          } else {
            LOG_INFO("start root service success");
          }
        }
      } else {
        // possible follower or doesn't have role yet
        // DEBUG_SYNC(BEFORE_STOP_RS);
        // leader does not exist.
        if (!root_service_.is_start()) {
          // nothing todo
        } else {
          if (OB_FAIL(root_service_.revoke_rs())) {
            LOG_WARN("fail to revoke rootservice", KR(ret));
            if (root_service_.is_need_stop()) {
              // nothing todo
            } else if (root_service_.is_stopping()) {
              if (OB_FAIL(root_service_.stop_service())) {
                LOG_WARN("root service stop service failed", K(ret));
              } else {
                LOG_INFO("root service stop service finish success");
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("inalid root service status", KR(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObRootServiceMonitor::try_start_root_service()
{
  int ret = OB_SUCCESS;
  ObMemberList member_list;
  const ObPartitionKey root_partition(combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
      ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID,
      ObIPartitionTable::ALL_CORE_TABLE_PARTITION_NUM);
  if (OB_FAIL(partition_service_.get_leader_curr_member_list(root_partition, member_list))) {
    LOG_WARN("fail to get leader current member list", K(ret));
  } else if (OB_ISNULL(GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid svr_rpc_proxy", K(ret));
  } else {
    ObMember member;
    ObGetRootserverRoleResult result;
    bool stopped = true;
    const int64_t TIMEOUT = 1 * 1000 * 1000;
    const ObCommonRpcProxy& rpc_proxy = *GCTX.rs_rpc_proxy_;
    for (int64_t i = 0; i < member_list.get_member_number() && OB_SUCC(ret) && stopped; i++) {
      if (OB_FAIL(member_list.get_member_by_index(i, member))) {
        LOG_WARN("fail to get member", K(ret), K(i));
      } else if (OB_FAIL(rpc_proxy.to_addr(member.get_server()).timeout(TIMEOUT).get_root_server_status(result))) {
        LOG_WARN("fail to check root service stop", K(ret));
      } else {
        LOG_INFO("get rootserver status", K(result.status_), K(member.get_server()));
        stopped = (result.status_ == status::INIT);
      }
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("fail to check root service stopped, start service later", K(ret));
      ret = OB_SUCCESS;
      if (!has_set_stop()) {
        usleep(2 * MONITOR_ROOT_SERVICE_INTERVAL_US);
      }
    } else if (!stopped) {
      LOG_WARN("already have root service in service, start service later");
      ret = OB_SUCCESS;
      if (!has_set_stop()) {
        usleep(2 * MONITOR_ROOT_SERVICE_INTERVAL_US);
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("try to start root service");
      if (OB_FAIL(root_service_.start_service())) {
        LOG_WARN("root service start service failed", K(ret));
      } else {
        LOG_INFO("root service start service success", K(ret));
      }
    }
  }
  return ret;
}

}  // end namespace observer
}  // end namespace oceanbase
