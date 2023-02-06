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

#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/ob_thread_mgr.h"
#include "ob_rl_mgr.h"

namespace oceanbase {
namespace share {

using namespace oceanbase::rpc::frame;

int __net_easy_update_s2r_map(void *args)
{
  int ret = common::OB_SUCCESS;
  ObRatelimitMgr *rl_mgr = NULL;

  OB_LOG(INFO, "__net_easy_update_s2r_map", K(args));
  if (OB_ISNULL(args)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    rl_mgr = static_cast<ObRatelimitMgr*>(args);
    rl_mgr->update_easy_s2r_map();
  }

  return ret;
}

ObRatelimitMgr::ObRatelimitMgr()
{
  thread_cnt_ = THREAD_COUNT;
  pending_rpc_count_ = 0;
  waiting_rpc_rsp_ = 0;
  stat_period_ = 1000 * 1000; // 1s by default
}

ObRatelimitMgr::~ObRatelimitMgr()
{}

const ObServer2RegionMapSEArray& ObRatelimitMgr::get_s2r_map_list()
{
  return s2r_map_list_;
}

const ObRegionBwStatSEArray& ObRatelimitMgr::get_peer_region_bw_list()
{
  return peer_region_bw_list_;
}

int ObRatelimitMgr::init(common::ObAddr& self_addr, observer::ObSrvNetworkFrame *net_frame, observer::ObGlobalContext *gctx)
{
  int ret = common::OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    OB_LOG(ERROR, "ObRatelimitMgr inited twice");
  } else if (OB_ISNULL(net_frame) || OB_ISNULL(gctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(ERROR, "invalid argument", KP(net_frame), KP(gctx));
  } else {
    self_addr_ = self_addr;
    net_frame_ = net_frame;
    net_ = net_frame_->get_net_easy();
    gctx_ = gctx;
    thread_cnt_ = THREAD_COUNT;
    pending_rpc_count_ = 0;

    if (OB_FAIL(reload_config())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(ERROR, "reload config failed.");
    } else if (OB_FAIL(rl_rpc_.init(self_addr, net_, gctx, net_frame_->get_req_transport(), this))) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(ERROR, "Failed to init rl_rpc.");
    } else if (OB_ISNULL(net_)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(ERROR, "Failed to init rl_rpc.");
    } else {
      net_->set_s2r_map_cb(__net_easy_update_s2r_map, this);
      is_inited_ = true;
    }
  }
  return ret;
}

void ObRatelimitMgr::destroy()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    OB_LOG(ERROR, "ObRatelimitMgr not inited.");
  } else {
    rl_rpc_.destroy();
    is_inited_ = false;
  }
}

int ObRatelimitMgr::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    OB_LOG(ERROR, "ObRatelimitMgr not inited.");
  } else if (OB_FAIL(TG_SET_RUNNABLE_AND_START(lib::TGDefIDs::RLMGR, *this))) {
    OB_LOG(ERROR, "failed to start ratelimit manager.");
  }

  return ret;
}

int ObRatelimitMgr::stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    OB_LOG(ERROR, "ObRatelimitMgr not inited.");
  } else {
    TG_STOP(lib::TGDefIDs::RLMGR);
  }
  return ret;
}

int ObRatelimitMgr::wait()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    OB_LOG(ERROR, "ObRatelimitMgr not inited.");
  } else {
    TG_WAIT(lib::TGDefIDs::RLMGR);
  }
  return ret;
}

int ObRatelimitMgr::reload_config()
{
  int ret = common::OB_SUCCESS;

  /*
   * reload_config can happen before ObRatelimitMgr::init.
   * So IS_NOT_INIT is not checked here.
   */
  if (enable_ob_ratelimit_ != GCONF.enable_ob_ratelimit) {
    enable_ob_ratelimit_ = GCONF.enable_ob_ratelimit;
    if (enable_ob_ratelimit_) {
      OB_LOG(INFO, "ob ratelimit enabled.");
    } else {
      OB_LOG(INFO, "ob ratelimit disabled.");
    }
    net_frame_->set_ratelimit_enable(enable_ob_ratelimit_);
    net_->set_ratelimit_enable(enable_ob_ratelimit_);
  }

  if (stat_period_ != GCONF.ob_ratelimit_stat_period) {
    stat_period_ = GCONF.ob_ratelimit_stat_period;
    OB_LOG(INFO, "ob ratelimit stat period reset.", K(stat_period_));
    net_->set_easy_ratelimit_stat_period(stat_period_);
  }
  return ret;
}

int ObRatelimitMgr::update_easy_s2r_map()
{
  int ret = common::OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    OB_LOG(ERROR, "ObRatelimitMgr not inited.");
  } else {
    ObLatchRGuard guard(s2r_map_lock_, ObLatchIds::RATELIMIT_LOCK);
    for (int i = 0; i < s2r_map_list_.count(); i++) {
      OB_LOG(DEBUG, "update_easy_s2r_map", K(s2r_map_list_[i].addr_), K(s2r_map_list_[i].region_));
      net_->set_easy_s2r_map(s2r_map_list_[i].addr_, s2r_map_list_[i].region_.ptr());
    }
  }
  return ret;
}

int ObRatelimitMgr::update_local_s2r_map()
{
  int ret = common::OB_SUCCESS;
  ObSqlString sql_str;
  common::ObMySQLProxy *sql_proxy = nullptr;
  sqlclient::ObMySQLResult *result = nullptr;
  HEAP_VAR(ObMySQLProxy::ReadResult, res) {
    common::ObAddr addr;
    char ip_addr[MAX_IP_ADDR_LENGTH];
    char region[MAX_REGION_LENGTH];
    int port;
    int s2r_changed = 0;
    int ip_str_len = 0;
    int region_str_len = 0;
    int line = 0;

    /* Todo: need to match region_self. */
    sql_proxy = gctx_->sql_proxy_;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      OB_LOG(ERROR, "ObRatelimitMgr not inited.");
    } else if (OB_FAIL(sql_str.assign_fmt("select a.svr_ip, a.svr_port, b.info "
                                   "from __all_server as a, __all_zone b "
                                   "where a.zone=b.zone and (b.name='region') "
                                   "order by a.svr_ip and a.svr_port "
                                   )))  {
      OB_LOG(WARN, "append sql failed", K(ret));
    } else if (OB_FAIL(sql_proxy->read(res, OB_SYS_TENANT_ID, sql_str.ptr()))) {
      OB_LOG(WARN, "fail to execute sql", KR(ret), K(sql_str));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", KR(ret), K(sql_str));
    } else {
      self_region_.reset();
      {
        ObLatchRGuard guard(s2r_map_lock_, ObLatchIds::RATELIMIT_LOCK);
        while (OB_SUCC(result->next())) {
          EXTRACT_STRBUF_FIELD_MYSQL((*result), "svr_ip",   ip_addr, MAX_IP_ADDR_LENGTH, ip_str_len);
          EXTRACT_STRBUF_FIELD_MYSQL((*result), "info",     region,  MAX_REGION_LENGTH,  region_str_len);
          EXTRACT_INT_FIELD_MYSQL((*result),    "svr_port", port, int);

          addr.set_ip_addr(ip_addr, port);
          OB_LOG(DEBUG, "update_local_s2r_map", K(addr), K(region), K(self_addr_), K(line), K(s2r_map_list_.count()));
          if (addr == self_addr_) {
            self_region_.assign(region);
          }

          if (line < s2r_map_list_.count()) {
            if (s2r_changed) {
                s2r_map_list_[line].addr_.set_ip_addr(ip_addr, port);
                s2r_map_list_[line].region_.assign(region);
            } else {
              if ((addr != s2r_map_list_[line].addr_) ||
                  (0 != strcmp(region, s2r_map_list_[line].region_.ptr()))) {
                s2r_changed = 1;
                s2r_map_list_[line].addr_.set_ip_addr(ip_addr, port);
                s2r_map_list_[line].region_.assign(region);
              }
            }
          } else {
            s2r_changed = 1;
            s2r_map_list_.push_back(ObServer2RegionMap(addr, region));
          }

          line++;
        } // while

        if (line < s2r_map_list_.count()) {
          s2r_changed = 1;
          for (int64_t i = (s2r_map_list_.count() - 1); i >= line; i--) {
            s2r_map_list_.remove(i);
          }
        }
      }
      OB_LOG(DEBUG, "update_local_s2r_map", K(ret), K(s2r_changed), K(self_addr_), K(self_region_), K(s2r_map_list_.count()));
      if (OB_ITER_END != ret) {
        OB_LOG(WARN, "fail to get next row", K(ret));
      } else if (self_region_.is_empty()) {
        ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "self_region should not be empty.");
      } else {
        if (s2r_changed) {
          /*
           * update the server list of current region.
           */
          local_server_list_.destroy();
          for (int i = 0; i < s2r_map_list_.count(); i++) {
            if (s2r_map_list_[i].region_ == self_region_) {
              local_server_list_.push_back(s2r_map_list_[i].addr_);
              if (s2r_map_list_[i].addr_ == self_addr_) {
                self_server_idx_ = local_server_list_.count() - 1;
              }
            }
          }
          net_->notify_easy_s2r_map_changed();
        }
      }
    } // else
  }

  return ret;
}

int ObRatelimitMgr::update_region_max_bw()
{
  int ret = common::OB_SUCCESS;
  ObSqlString sql_str;
  common::ObMySQLProxy *sql_proxy = nullptr;
  sqlclient::ObMySQLResult *result = nullptr;
  HEAP_VAR(ObMySQLProxy::ReadResult, res) {
    char region[MAX_REGION_LENGTH];
    int64_t max_bw;
    int region_list_changed = 0;
    int region_str_len = 0;
    int line = 0;
    ObRegionBwStat *region_bw_stat = nullptr;

    sql_proxy = gctx_->sql_proxy_;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      OB_LOG(ERROR, "ObRatelimitMgr not inited.");
    } else if (OB_FAIL(sql_str.assign_fmt("select dst_region,max_bw "
                                   "from __all_region_network_bandwidth_limit "
                                   "where src_region='%s' "
                                   "order by dst_region ",
                                   self_region_.ptr()
                                   )))  {
      OB_LOG(WARN, "append sql failed", K(ret));
    } else if (OB_FAIL(sql_proxy->read(res, OB_SYS_TENANT_ID, sql_str.ptr()))) {
      OB_LOG(WARN, "fail to execute sql", KR(ret), K(sql_str));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "result is NULL", KR(ret), K(sql_str));
    } else {
      while (OB_SUCC(result->next())) {
        EXTRACT_STRBUF_FIELD_MYSQL((*result), "dst_region", region,  MAX_REGION_LENGTH,  region_str_len);
        EXTRACT_INT_FIELD_MYSQL((*result),    "max_bw",     max_bw,  int64_t);

        OB_LOG(DEBUG, "Got results from table __all_region_network_bandwidth_limit", K(region), K(max_bw), K(line), K(peer_region_bw_list_.count()), K(region_list_changed));
        if (line < peer_region_bw_list_.count()) {
          if (region_list_changed) {
              peer_region_bw_list_[line].region_.assign(region);
              peer_region_bw_list_[line].max_bw_ = max_bw;
          } else {
            if ((0 != strcmp(region, peer_region_bw_list_[line].region_.ptr())) || (max_bw != peer_region_bw_list_[line].max_bw_)) {
              region_list_changed = 1;
              peer_region_bw_list_[line].region_.assign(region);
              peer_region_bw_list_[line].max_bw_ = max_bw;
            }
          }
          // peer_region_bw_list_[line].server_bw_list_.destroy();
        } else {
          region_list_changed = 1;
          peer_region_bw_list_.push_back(ObRegionBwStat(region, max_bw));
        }

        line++;
      } // while

      if (line < peer_region_bw_list_.count()) {
        region_list_changed = 1;
        for (int64_t i = (peer_region_bw_list_.count() - 1); i >= line; i--) {
          peer_region_bw_list_.remove(i);
        }
      }

      if (OB_ITER_END != ret) {
        OB_LOG(WARN, "fail to get next row", K(ret));
      }
      OB_LOG(DEBUG, "Got peer_region_bw_list_", K(peer_region_bw_list_.count()), K(local_server_list_.count()), K(region_list_changed));
      if (region_list_changed) {
        int64_t max_bw = 0;
        for (int i = 0; i < peer_region_bw_list_.count(); i++) {
          region_bw_stat = &peer_region_bw_list_[i];
          region_bw_stat->server_bw_list_.destroy();
          max_bw = region_bw_stat->max_bw_ / local_server_list_.count();
          for (int j = 0; j < local_server_list_.count(); j++) {
            region_bw_stat->server_bw_list_.push_back(ObServerBW(local_server_list_[j], 0, 0));
          }
          net_->set_easy_region_max_bw(region_bw_stat->region_.ptr(), max_bw);
        }
        net_->notify_easy_s2r_map_changed();
      }
    }
  }

  return ret;
}

int ObRatelimitMgr::get_server_bw_rpc_cb(common::ObAddr& ob_addr, int server_idx, ObRegionBwSEArray& region_bw_list)
{
  int ret = common::OB_SUCCESS;
  int result = 0;
  ObServerBwSEArray *server_bw_list = nullptr;
  ObRegionBwStat *peer_region_bw_stat = nullptr;

  OB_LOG(DEBUG, "Received RPC callback, get_server_bw_rpc_cb", K(ob_addr), K(pending_rpc_count_));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    OB_LOG(ERROR, "ObRatelimitMgr not inited.");
  } else {
    /*
     * Both peer_region_bw_list_ and region_bw_list are ordered by region name.
     */
    int i = 0, j = 0;
    if (region_bw_list.count() == 0) {
      while (i < peer_region_bw_list_.count()) {
        peer_region_bw_stat = &(peer_region_bw_list_[i]);
        OB_LOG(DEBUG, "get_server_bw_rpc_cb", K(ob_addr), K(peer_region_bw_stat->server_bw_list_.count()), K(peer_region_bw_list_.count()));
        peer_region_bw_stat->server_bw_list_[server_idx].max_bw_ =
          peer_region_bw_stat->max_bw_ / peer_region_bw_stat->server_bw_list_.count();
        peer_region_bw_stat->server_bw_list_[server_idx].cur_bw_ =
          peer_region_bw_stat->server_bw_list_[server_idx].max_bw_;
        i++;
      }
    } else {
      while ((i < peer_region_bw_list_.count()) && (j < region_bw_list.count())) {
        peer_region_bw_stat = &(peer_region_bw_list_[i]);
        result = strncmp(peer_region_bw_stat->region_.ptr(),
                        region_bw_list[j].region_.ptr(),
                        MAX_REGION_LENGTH);
        if (result == 0) {
          /*
           * rl_mgr should not change peer_region_bw_list_ before all the response of
           * RPC is returned.
           */
          ObLatchRGuard guard(peer_region_bw_stat->server_bw_list_spinlock_, ObLatchIds::RATELIMIT_LOCK);
          ObServerBwSEArray &server_bw_list = peer_region_bw_stat->server_bw_list_;
          // server_bw_list->push_back(ObServerBW(ob_addr, region_bw_list[j].max_bw_, region_bw_list[j].bw_));
          if ((server_idx < 0) || (server_idx >= server_bw_list.count())) {
            OB_LOG(WARN, "Wrong server_idx", K(server_idx), K(server_bw_list.count()));
          } else if (server_bw_list[server_idx].addr_ != ob_addr) {
            OB_LOG(WARN, "Wrong server addr", K(server_idx), K(ob_addr), K(server_bw_list[server_idx].addr_ ));
          } else {
            // server_bw_list[server_idx].addr_ = ob_addr;
            OB_LOG(DEBUG, "get_server_bw_rpc_cb", K(ob_addr), K(peer_region_bw_stat->region_), K(region_bw_list[j].bw_),
                K(region_bw_list[j].max_bw_), K(i), K(j));
            server_bw_list[server_idx].max_bw_ = region_bw_list[j].max_bw_;
            server_bw_list[server_idx].cur_bw_ = region_bw_list[j].bw_;
          }
          i++;
          j++;
        } else {
          OB_LOG(INFO, "Region ratelimit configuration changed.");
          if (result > 0) {
            j++;
          } else {
            ObLatchRGuard guard(peer_region_bw_stat->server_bw_list_spinlock_, ObLatchIds::RATELIMIT_LOCK);
            ObServerBwSEArray &server_bw_list = peer_region_bw_stat->server_bw_list_;
            // server_bw_list->push_back(ObServerBW(ob_addr, region_bw_list[j].max_bw_, region_bw_list[j].bw_));
            if ((server_idx < 0) || (server_idx >= server_bw_list.count())) {
              OB_LOG(WARN, "Wrong server_idx", K(server_idx), K(server_bw_list.count()));
            } else if (server_bw_list[server_idx].addr_ != ob_addr) {
              OB_LOG(WARN, "Wrong server addr", K(ob_addr), K(server_bw_list[server_idx].addr_ ));
            } else {
              server_bw_list[server_idx].max_bw_ =
                peer_region_bw_stat->max_bw_ / peer_region_bw_stat->server_bw_list_.count();
              server_bw_list[server_idx].cur_bw_ = server_bw_list[server_idx].max_bw_;
            }
            i++;
          }
        }
      } // while (1)
    }

    if (0 == ATOMIC_AAF(&pending_rpc_count_, -1)) {
      OB_LOG(DEBUG, "wake up swc", K(pending_rpc_count_));
      swc_.signal();
    }
  }

  return ret;
}

int ObRatelimitMgr::update_s2r_max_bw(ObRegionBwStat *region_bw_stat, int64_t& do_rpc)
{
  int ret = common::OB_SUCCESS;
  int64_t margin       = 0;
  int64_t delta        = 0;
  int64_t average_bw   = 0;
  ObServerBW *server_bw = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    OB_LOG(ERROR, "ObRatelimitMgr not inited.");
  } else if (OB_ISNULL(region_bw_stat)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(ERROR, "invalid argument", KP(region_bw_stat));
  } else if (OB_FAIL(net_->get_easy_region_latest_bw(region_bw_stat->region_.ptr(),
                                    &(region_bw_stat->local_server_cur_bw_),
                                    &(region_bw_stat->local_server_max_bw_)))) {
    OB_LOG(WARN, "failed to get region latest bandwidth", K(ret));
  } else {
    average_bw = region_bw_stat->max_bw_ / local_server_list_.count(); // self_addr not in local_server_list_;
    delta      = average_bw >> 2;
    margin     = REGION_BW_MARGIN;

    OB_LOG(DEBUG, "Got easy latest bandwidth to region.", K(region_bw_stat->region_), K(region_bw_stat->local_server_cur_bw_), K(region_bw_stat->local_server_max_bw_));
    region_bw_stat->need_balance_ = 0;
    if ((region_bw_stat->local_server_cur_bw_ + margin) < average_bw) {
      /*
      * Check if max_bw set to the dst region in the last speed statistic period is enough.
      */
      if ((region_bw_stat->local_server_cur_bw_ + margin) < region_bw_stat->local_server_max_bw_) {
        region_bw_stat->local_server_max_bw_ = region_bw_stat->local_server_cur_bw_ + margin;
      } else {
        region_bw_stat->local_server_max_bw_ = region_bw_stat->local_server_cur_bw_ + delta;
        if (region_bw_stat->local_server_max_bw_ > average_bw) {
          region_bw_stat->need_balance_ = 1;
        }
      }
    } else {
      region_bw_stat->need_balance_ = 1;
    }

    OB_LOG(DEBUG, "local bandwidth result.", K(region_bw_stat->region_), K(region_bw_stat->need_balance_));
    if (!region_bw_stat->need_balance_) {
      /*
      * Because current max_bw is less than average_bw, it is not necessary to check with other servers.
      */
      net_->set_easy_region_max_bw(region_bw_stat->region_.ptr(), region_bw_stat->local_server_max_bw_);
    } else {
      do_rpc = 1;
    }
  }

  return ret;
}

void ObRatelimitMgr::update_s2r_max_bw_all()
{
  int ret = common::OB_SUCCESS;
  int64_t do_rpc = 0;
  ObRegionBwStat *region_bw_stat = nullptr;

  if (IS_NOT_INIT) {
    OB_LOG(ERROR, "ObRatelimitMgr not inited.");
  } else {
    for (int i = 0; i < peer_region_bw_list_.count(); i++) {
      region_bw_stat = &(peer_region_bw_list_[i]);
      update_s2r_max_bw(region_bw_stat, do_rpc);
    }

    if (do_rpc) {
      OB_LOG(DEBUG, "Need to collect bandwidth information from other server within current region.", K(local_server_list_.count()));
      for (int i = 0; i < local_server_list_.count(); i++) {
        OB_LOG(DEBUG, "send rpc", K(local_server_list_[i]), K(i));
        if (local_server_list_[i] != self_addr_) {
          if (OB_FAIL(rl_rpc_.get_server_bw(local_server_list_[i], i))) {
            OB_LOG(WARN, "Failed to get_server_bw.", K(ret));
          } else {
            ATOMIC_INC(&pending_rpc_count_);
          }
        }
      }
      waiting_rpc_rsp_ = 1;
    }
  }
}

void ObRatelimitMgr::calculate_s2r_max_bw(ObRegionBwStat *region_bw_stat)
{
  int ret = OB_SUCCESS;
  int64_t margin     = 0;
  int64_t delta      = 0;
  int64_t balance    = 0;
  int64_t pendings   = 0;
  int64_t average_bw = 0;
  int64_t average_balance = 0;
  ObServerBW *server_bw = nullptr;
  bool need_balance = false;

  if (IS_NOT_INIT) {
    OB_LOG(ERROR, "ObRatelimitMgr not inited.");
  } else if (OB_ISNULL(region_bw_stat)) {
    OB_LOG(ERROR, "invalid argument", KP(region_bw_stat));
  } else if (OB_FAIL(net_->get_easy_region_latest_bw(region_bw_stat->region_.ptr(),
                                    &(region_bw_stat->local_server_cur_bw_),
                                    &(region_bw_stat->local_server_max_bw_)))) {
    OB_LOG(WARN, "failed to get region latest bandwidth", K(ret));
  } else {

    average_bw = region_bw_stat->max_bw_ / local_server_list_.count();
    delta      = average_bw >> 2;
    margin     = REGION_BW_MARGIN;

    /*
     * When local bandwidth less than the average bathwith of the region, it is not needed to
     * recalculate the MAX bandwidth based on other server's bandwidth.
     */
    if ((region_bw_stat->local_server_cur_bw_ + margin) < average_bw) {
      if ((region_bw_stat->local_server_cur_bw_ + margin) < region_bw_stat->local_server_max_bw_) {
        region_bw_stat->local_server_max_bw_ = region_bw_stat->local_server_cur_bw_ + margin;
      } else {
        region_bw_stat->local_server_max_bw_ = region_bw_stat->local_server_cur_bw_ + delta;
        if (region_bw_stat->local_server_max_bw_ > average_bw) {
          need_balance = true;
        }
      }
    } else {
      need_balance = true;
    }

    if (need_balance) {
      region_bw_stat->server_bw_list_[self_server_idx_].max_bw_ = region_bw_stat->local_server_max_bw_;
      region_bw_stat->server_bw_list_[self_server_idx_].cur_bw_ = region_bw_stat->local_server_cur_bw_;
      /*
       * 1st round of traverse to collect bandwidth balance.
       */
      for (int i = 0; i < region_bw_stat->server_bw_list_.count(); i++) {
        server_bw =  &(region_bw_stat->server_bw_list_[i]);
        OB_LOG(INFO, "calculate_s2r_max_bw", K(server_bw->addr_), K(region_bw_stat->region_), K(server_bw->cur_bw_), K(server_bw->max_bw_), K(average_bw));
        server_bw->pending_ = 0;
        if ((server_bw->cur_bw_ + margin) < average_bw) {
          if ((server_bw->cur_bw_ + margin) < server_bw->max_bw_) {
            server_bw->max_bw_ = server_bw->cur_bw_ + margin;
            balance += average_bw - server_bw->max_bw_;
          } else {
            server_bw->max_bw_ = server_bw->cur_bw_ + delta;
            if (server_bw->max_bw_ < average_bw) {
              balance += average_bw - server_bw->max_bw_;
            } else {
              server_bw->pending_ = 1;
              pendings++;
            }
          }
        } else {
          server_bw->pending_ = 1;
          pendings++;
        }
      } // for

      /*
       * 2nd round of traverse to get balance available for local server.
       */
      for (int i = 0; i < region_bw_stat->server_bw_list_.count(); i++) {
        server_bw =  &(region_bw_stat->server_bw_list_[i]);
        if (server_bw->pending_) {
          average_balance = average_bw + (balance / pendings);
          if ((server_bw->cur_bw_ + margin) < server_bw->max_bw_) {
            server_bw->max_bw_ = server_bw->cur_bw_ + margin;
          } else {
            server_bw->max_bw_ = server_bw->cur_bw_ + delta;
          }
          if (server_bw->max_bw_ > average_balance) {
            server_bw->max_bw_ = average_balance;
          }
          if (server_bw->addr_ == self_addr_) {
            region_bw_stat->local_server_max_bw_ = server_bw->max_bw_;
            break;
          }
          balance -= (average_balance - server_bw->max_bw_);
          pendings--;
        }
      } // for

      // region_bw_stat->local_server_max_bw_ = average_bw + balance;
      net_->set_easy_region_max_bw(region_bw_stat->region_.ptr(), region_bw_stat->local_server_max_bw_);
    }
  }
  return;
}

void ObRatelimitMgr::calculate_s2r_max_bw_all()
{
  ObRegionBwStat *region_bw_stat = nullptr;
  if (IS_NOT_INIT) {
    OB_LOG_RET(ERROR, OB_NOT_INIT, "ObRatelimitMgr not inited.");
  } else {
    for (int i = 0; i < peer_region_bw_list_.count(); i++) {
      region_bw_stat = &(peer_region_bw_list_[i]);
      if (region_bw_stat->need_balance_) {
        OB_LOG(DEBUG, "recalculate max_bw for region", K(region_bw_stat->region_), K(region_bw_stat->server_bw_list_), K(region_bw_stat->server_bw_list_.count()));
        calculate_s2r_max_bw(region_bw_stat);
        region_bw_stat->need_balance_ = 0;
      }
    }
  }
}

void ObRatelimitMgr::do_work()
{
  if (IS_NOT_INIT) {
    OB_LOG(INFO, "ObRatelimitMgr not inited.");
    // swc_.wait(stat_period_);
  } else {
    if (waiting_rpc_rsp_) {
      if (0 == ATOMIC_LOAD(&pending_rpc_count_)) {
        calculate_s2r_max_bw_all();
        waiting_rpc_rsp_ = 0;
        swc_.wait(stat_period_);
      } else {
        OB_LOG(INFO, "Have not got the resonses of all RPCs.");
      }
    } else {
      if (enable_ob_ratelimit_) {
        /*
        * Get the server-to-region mapping from __all_server and __all_zone table.
        */
        update_local_s2r_map();

        /*
        * Get the max_bw of each of the dst regions, which is configured in
        * __all_region_network_bandwidth_limit table.
        */
        update_region_max_bw();

        /*
        * Update the max_bw from current observer to the dst regions based on the real
        * bandwith of all of the servers of current region in the past period.
        */
        update_s2r_max_bw_all();
      }
    }
  }
  bool ready = false;
  ready = swc_.wait(stat_period_);
  OB_LOG(INFO, "swc wakeup.", K(stat_period_), K(ready));
}

void ObRatelimitMgr::run1()
{
  lib::set_thread_name("rl_mgr", 0);
  while(!has_set_stop()) {
    do_work();
  }
}

} // share
} // oceanbase
