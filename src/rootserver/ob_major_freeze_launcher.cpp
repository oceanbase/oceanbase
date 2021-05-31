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

#include "ob_major_freeze_launcher.h"

#include "share/ob_debug_sync.h"
#include "share/ob_common_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "rootserver/ob_freeze_info_manager.h"
#include "rootserver/ob_root_service.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace obrpc;
namespace rootserver {
ObMajorFreezeLauncher::ObMajorFreezeLauncher()
    : ObRsReentrantThread(true),
      inited_(false),
      root_service_(NULL),
      rpc_proxy_(NULL),
      config_(NULL),
      self_addr_(),
      same_minute_flag_(false),
      gc_freeze_info_last_timestamp_(0),
      freeze_info_manager_(NULL)
{}

ObMajorFreezeLauncher::~ObMajorFreezeLauncher()
{
  if (inited_) {
    stop();
  }
}

int ObMajorFreezeLauncher::init(ObRootService& root_service, ObCommonRpcProxy& rpc_proxy, ObServerConfig& config,
    const ObAddr& self_addr, ObFreezeInfoManager& freeze_info_manager)
{
  int ret = OB_SUCCESS;
  static const int64_t major_freeze_launcher_thread_cnt = 1;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (!self_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid self_addr", K(self_addr), K(ret));
  } else if (OB_FAIL(create(major_freeze_launcher_thread_cnt, "MajorLaunch"))) {
    LOG_WARN("create major freeze launcher thread failed", K(ret), K(major_freeze_launcher_thread_cnt));
  } else {
    root_service_ = &root_service;
    rpc_proxy_ = &rpc_proxy;
    config_ = &config;
    self_addr_ = self_addr;
    same_minute_flag_ = false;
    gc_freeze_info_last_timestamp_ = ObTimeUtility::current_time();
    freeze_info_manager_ = &freeze_info_manager;
    inited_ = true;
  }
  return ret;
}

void ObMajorFreezeLauncher::run3()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    LOG_INFO("major freeze launcher start");
    DEBUG_SYNC(BEFORE_CHECK_MAJOR_FREEZE_DONE);
    ObThreadCondGuard guard(get_cond());
    while (!stop_) {
      update_last_run_timestamp();
      if (OB_FAIL(try_launch_major_freeze())) {
        LOG_WARN("try_launch_major_freeze failed", K(ret));
      }
      if (OB_FAIL(try_gc_freeze_info())) {
        LOG_WARN("fail to gc freeze info", K(ret));
      }
      if (!stop_) {
        get_cond().wait(TRY_LAUNCH_MAJOR_FREEZE_INTERVAL_US / 1000);
      }
    }
    LOG_INFO("major freeze launcher stop");
  }
}

int ObMajorFreezeLauncher::try_launch_major_freeze()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (!config_->major_freeze_duty_time.disable()) {
      const int hour = config_->major_freeze_duty_time.hour();
      const int minute = config_->major_freeze_duty_time.minute();
      struct tm human_time;
      struct tm* human_time_ptr = NULL;
      time_t cur_time;
      time(&cur_time);
      int64_t expire_time = GCONF.recyclebin_object_expire_time;
      if (NULL == (human_time_ptr = (localtime_r(&cur_time, &human_time)))) {
        ret = OB_ERR_SYS;
        LOG_WARN("get localtime failed", K(ret));
      } else if (human_time_ptr->tm_hour == hour && human_time_ptr->tm_min == minute) {
        if (!same_minute_flag_) {
          const int64_t PURGE_EACH_TIME = 10000;
          int tmp_ret = OB_SUCCESS;
          if (OB_ISNULL(root_service_)) {  // ignore ret
            tmp_ret = OB_ERR_UNEXPECTED;
            LOG_WARN("rootservice is null", K(tmp_ret));
          } else if (GCONF._recyclebin_object_purge_frequency == 0 && expire_time > 0 &&
                     OB_SUCCESS != (tmp_ret = root_service_->purge_recyclebin_objects(PURGE_EACH_TIME))) {
            LOG_WARN("purge recyclebin failed", K(tmp_ret));
          }
          same_minute_flag_ = true;
          Int64 frozen_version;
          int64_t this_frozen_version = INT64_MAX;
          if (!freeze_info_manager_->is_leader_cluster()) {
            LOG_INFO("no need to launch new major freeze for slave cluster");
          } else {
            do {
              if (OB_FAIL(rpc_proxy_->to(self_addr_).get_frozen_version(frozen_version))) {
                LOG_WARN("get_frozen_version failed", K(ret));
              } else if (frozen_version > this_frozen_version) {
                // no need any more, some others help complete major freeze of this version
              } else {
                this_frozen_version = frozen_version;
                ObRootMajorFreezeArg arg;
                arg.try_frozen_version_ = frozen_version + 1;
                arg.launch_new_round_ = true;
                if (OB_FAIL(rpc_proxy_->to(self_addr_).root_major_freeze(arg))) {
                  LOG_WARN("major_freeze failed", K(arg), K(ret));
                }
              }
              if (OB_MAJOR_FREEZE_NOT_ALLOW == ret) {
                int64_t usleep_cnt = 0;
                update_last_run_timestamp();
                while (!stop_ && usleep_cnt < MAJOR_FREEZE_RETRY_LIMIT) {
                  usleep_cnt++;
                  usleep(MAJOR_FREEZE_RETRY_INTERVAL_US);
                }
              }
            } while (!stop_ && OB_MAJOR_FREEZE_NOT_ALLOW == ret);
          }
        } else {
          LOG_INFO("major freeze has already been launched, no need to do again");
        }
      } else {
        same_minute_flag_ = false;
        // do nothing
      }
    }
  }
  return ret;
}

int ObMajorFreezeLauncher::try_gc_freeze_info()
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  if (now - gc_freeze_info_last_timestamp_ < MODIFY_GC_FREEZE_INFO_INTERVAL) {
    // nothing todo
  } else if (OB_FAIL(freeze_info_manager_->gc_freeze_info())) {
    LOG_WARN("fail to set snapshot", K(ret));
  } else {
    gc_freeze_info_last_timestamp_ = now;
  }
  return ret;
}

int64_t ObMajorFreezeLauncher::get_schedule_interval() const
{
  int64_t schedule_interval = MAJOR_FREEZE_RETRY_INTERVAL_US * MAJOR_FREEZE_RETRY_LIMIT;
  if (OB_UNLIKELY(TRY_LAUNCH_MAJOR_FREEZE_INTERVAL_US > MAJOR_FREEZE_RETRY_INTERVAL_US * MAJOR_FREEZE_RETRY_LIMIT)) {
    schedule_interval = TRY_LAUNCH_MAJOR_FREEZE_INTERVAL_US;
  }
  return schedule_interval;
}

}  // end namespace rootserver
}  // end namespace oceanbase
