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

#ifndef OCEANBASE_SHARE_OB_RL_MGR_H
#define OCEANBASE_SHARE_OB_RL_MGR_H

#include "lib/ob_define.h"
#include "lib/lock/ob_futex.h"
#include "lib/thread/ob_thread_name.h"
#include "lib/thread/thread_mgr_interface.h"
#include "rpc/frame/ob_net_easy.h"
#include "observer/ob_srv_network_frame.h"
#include "ob_rl_struct.h"
#include "ob_rl_rpc.h"

namespace oceanbase {
namespace share {

class ObRatelimitMgr: public lib::TGRunnable
{
public:
  enum {
    THREAD_COUNT = 1,
    MAX_REGION_NUM = 64,
    REGION_BW_MARGIN = 2 * 1000 * 1000, // 2MB
  };
  ObRatelimitMgr();
  virtual ~ObRatelimitMgr();
  int init(common::ObAddr& self_addr, observer::ObSrvNetworkFrame *net_frame_, observer::ObGlobalContext *gctx);
  void destroy();
  int start();
  int stop();
  int wait();
  virtual void run1();
  const ObServer2RegionMapSEArray& get_s2r_map_list();
  const ObRegionBwStatSEArray& get_peer_region_bw_list();
  int get_server_bw_rpc_cb(common::ObAddr& ob_addr, int server_idx, ObRegionBwSEArray& region_bw_list);
  int update_easy_s2r_map();
  int reload_config();

private:
  int update_local_s2r_map();
  int update_region_max_bw();
  int update_s2r_max_bw(ObRegionBwStat *region_max_bw, int64_t& do_rpc);
  void update_s2r_max_bw_all();
  void calculate_s2r_max_bw_all();
  void do_work();
  void calculate_s2r_max_bw(ObRegionBwStat *region_max_bw);

  bool is_inited_;
  int enable_ob_ratelimit_;
  int waiting_rpc_rsp_;
  int thread_cnt_;
  int64_t pending_rpc_count_;
  SingleWaitCond swc_;
  int64_t stat_period_;
  int64_t self_server_idx_;
  common::ObAddr   self_addr_;
  common::ObRegion self_region_;
  ObServerSEArray local_server_list_;
  ObServer2RegionMapSEArray s2r_map_list_;
  ObLatch s2r_map_lock_;
  ObRegionBwStatSEArray peer_region_bw_list_;
  ObRatelimitRPC rl_rpc_;
  rpc::frame::ObNetEasy *net_;
  observer::ObSrvNetworkFrame *net_frame_;
  const observer::ObGlobalContext *gctx_;
};
} // namespace share
} // namespace oceanbase

#endif
