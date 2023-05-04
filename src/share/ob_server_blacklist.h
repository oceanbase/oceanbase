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

#ifndef OCEANBASE_SHARE_OB_SERVER_BLACKLIST_H_
#define OCEANBASE_SHARE_OB_SERVER_BLACKLIST_H_
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/net/ob_addr.h"
#include "lib/ob_define.h"
#include "rpc/ob_blacklist_proxy.h"
#include "share/ob_cascad_member.h"
#include "share/ob_thread_pool.h"
#include "share/ob_thread_mgr.h"
#include "common/ob_simple_iterator.h"

namespace oceanbase
{
namespace rpc
{
namespace frame
{
class ObReqTransport;
}
}
namespace obrpc
{
class ObBatchRpc;
class ObBlackistReq;
class ObBlackistResp;
}
namespace share
{
struct ObDstServerInfo
{
public:
  ObDstServerInfo()
    : last_send_timestamp_(common::OB_INVALID_TIMESTAMP),
      last_recv_timestamp_(common::OB_INVALID_TIMESTAMP),
      is_in_blacklist_(false),
      add_timestamp_(common::OB_INVALID_TIMESTAMP), clean_on_time_(false),
      server_start_time_(0) {}
  ~ObDstServerInfo() {}
  void reset()
  {
    last_send_timestamp_ = common::OB_INVALID_TIMESTAMP;
    last_recv_timestamp_ = common::OB_INVALID_TIMESTAMP;
    is_in_blacklist_ = false;
    server_start_time_ = 0;
  }
  TO_STRING_KV(K_(last_send_timestamp), K_(last_recv_timestamp),
      K_(is_in_blacklist), K_(add_timestamp), K_(clean_on_time), K_(server_start_time));

  // Play the role of req_id for asynchronous messages, used to match <req, resp>
  int64_t last_send_timestamp_;
  int64_t last_recv_timestamp_;
  bool is_in_blacklist_;
  int64_t add_timestamp_;
  bool clean_on_time_;
  int64_t server_start_time_;
};

struct ObBlacklistInfo
{
public:
  ObBlacklistInfo() {}
  ~ObBlacklistInfo() {}
  void reset()
  {
    dst_svr_.reset();
    dst_info_.reset();
  }
  const common::ObAddr &get_addr() const { return dst_svr_; }
  const ObDstServerInfo &get_dst_info() const { return dst_info_; }
  TO_STRING_KV(K_(dst_svr), K_(dst_info));

  common::ObAddr dst_svr_;
  ObDstServerInfo dst_info_;
};

// TODO shanyan.g to be removed
class ObServerBlacklist : public lib::TGRunnable
{
public:
  ObServerBlacklist();
  virtual ~ObServerBlacklist();
public:
  typedef common::ObSimpleIterator<ObBlacklistInfo, common::ObModIds::OB_SERVER_BLACKLIST, 64> ObBlacklistInfoIterator;
  static ObServerBlacklist &get_instance();
  int init(const common::ObAddr &self,
           rpc::frame::ObReqTransport *transport);
  void reset();
  void destroy();
  bool is_in_blacklist(const share::ObCascadMember &member, bool add_server = false,
                      int64_t server_start_time = 0);
  bool is_clockdiff_error(const share::ObCascadMember &member) const { return false; }
  bool is_clog_disk_full(const share::ObCascadMember &member) const { return false; }
  bool is_clog_disk_error(const share::ObCascadMember &member) const { return false; }
  int handle_req(const int64_t src_cluster_id, const obrpc::ObBlacklistReq &req);
  int handle_resp(const obrpc::ObBlacklistResp &resp, const int64_t cluster_id);
  int iterate_blacklist_info(ObBlacklistInfoIterator &info_iter);
  bool is_empty() const;
  // for ob_admin
  void disable_blacklist();
  void enable_blacklist();
  void clear_blacklist();
private:
  void run1();
  void blacklist_loop_();
  int send_req_(const share::ObCascadMember &member, const obrpc::ObBlacklistReq &req);
  int send_resp_(const common::ObAddr &server, const int64_t dst_cluster_id, const obrpc::ObBlacklistResp &resp);
private:
  typedef common::ObLinearHashMap<share::ObCascadMember, ObDstServerInfo> DstInfoMap;

  class ObMapRemoveFunctor
  {
  public:
    ObMapRemoveFunctor(int64_t now) :
      now_(now), remove_cnt_(0) {}
    ~ObMapRemoveFunctor() {}
    bool operator() (const share::ObCascadMember &member, const ObDstServerInfo &info);
    int64_t get_remove_cnt() const { return remove_cnt_; }
  private:
    int64_t now_;
    int64_t remove_cnt_;
  };

  class ObMapResetFunctor
  {
  public:
    ObMapResetFunctor() : reset_cnt_(0) {}
    ~ObMapResetFunctor() {}
    bool operator() (const share::ObCascadMember &member, ObDstServerInfo &info);
    int64_t get_reset_cnt() const { return reset_cnt_; }
  private:
    int64_t reset_cnt_;
  };

  class ObMapMarkBlackFunctor
  {
  public:
    explicit ObMapMarkBlackFunctor()
      : mark_cnt_(0) {}
    ~ObMapMarkBlackFunctor() {}
    bool operator() (const share::ObCascadMember &ObCascadMember, ObDstServerInfo &info);
    int64_t get_mark_cnt() const { return mark_cnt_; }
  private:
    int64_t mark_cnt_;
  };

  class ObMapRespFunctor
  {
  public:
    explicit ObMapRespFunctor(const obrpc::ObBlacklistResp &resp)
      : resp_(resp) {}
    ~ObMapRespFunctor() {}
    bool operator() (const share::ObCascadMember &member, ObDstServerInfo &info);
  private:
    obrpc::ObBlacklistResp resp_;
  };

  class ObMapSendReqFunctor
  {
  public:
    explicit ObMapSendReqFunctor(ObServerBlacklist *blacklist, const common::ObAddr &self)
      : blacklist_(blacklist), self_(self), send_cnt_(0) {}
    ~ObMapSendReqFunctor() {}
    bool operator() (const share::ObCascadMember &ObCascadMember, ObDstServerInfo &info);
    int64_t get_send_cnt() const { return send_cnt_; }
  private:
    ObServerBlacklist *blacklist_;
    common::ObAddr self_;
    int64_t send_cnt_;
  };

  class ObMapIterFunctor
  {
  public:
    ObMapIterFunctor(ObBlacklistInfoIterator &info_iter) : info_iter_(info_iter) {}
    ~ObMapIterFunctor() {}
    bool operator() (const share::ObCascadMember &ObCascadMember, ObDstServerInfo &info);
  private:
    ObBlacklistInfoIterator &info_iter_;
  };

public:
  static uint64_t black_svr_cnt_;
private:
  // RPC latency threshold
  static const int64_t RPC_TRANS_TIME_THRESHOLD = 500 * 1000;
  // Thread polling interval
  static const int32_t BLACKLIST_LOOP_INTERVAL = 1 * 1000 * 1000;
  // Send request interval
  static const int32_t BLACKLIST_REQ_INTERVAL = 3 * 1000 * 1000;
  // Timeout threshold used to mark one server in blacklist
  static const int32_t BLACKLIST_MARK_THRESHOLD = 10 * 1000 * 1000;
  static const int64_t REMOVE_DST_SERVER_THRESHOLD = 60 * 60 * 1000000l;
private:
  bool is_inited_;
  bool is_enabled_;
  common::ObAddr self_;
  DstInfoMap dst_info_map_;
  obrpc::ObBlacklistRpcProxy blacklist_proxy_;
  obrpc::ObBatchRpc *batch_rpc_;
  DISALLOW_COPY_AND_ASSIGN(ObServerBlacklist);
};

#define SVR_BLACK_LIST (::oceanbase::share::ObServerBlacklist::get_instance())

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_SERVER_BLACKLIST_H_
