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

#ifndef _OB_SHARE_OB_GLOBAL_AUTOINC_SERVICE_H_
#define _OB_SHARE_OB_GLOBAL_AUTOINC_SERVICE_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_link_hashmap.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "logservice/ob_log_base_type.h"
#include "share/scn.h"
#include "share/ob_autoincrement_param.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_gais_msg.h"
#include "share/ob_gais_rpc.h"

namespace oceanbase
{
namespace obrpc
{
struct ObGAISReqRpcResult;
struct ObGAISGetRpcResult;
}

namespace share
{
struct ObGAISNextAutoIncValReq;
struct ObGAISAutoIncKeyArg;
struct ObGAISPushAutoIncValReq;
struct ObGAISBroadcastAutoIncCacheReq;

struct ObAutoIncCacheNode
{
  OB_UNIS_VERSION(1);
public:
  ObAutoIncCacheNode() : start_(0), end_(0), sync_value_(0), autoinc_version_(OB_INVALID_VERSION),
    is_received_(false) {}
  int init(const uint64_t start,
           const uint64_t end,
           const uint64_t sync_value,
           const int64_t autoinc_version);
  inline bool is_valid() const
  {
    return start_ > 0 && end_ >= start_ && sync_value_ <= start_;
  }
  inline bool need_fetch_next_node(const uint64_t base_value,
                                   const uint64_t desired_cnt,
                                   const uint64_t max_value) const;
  inline bool need_sync(const uint64_t new_sync_value) const
  {
    return new_sync_value > sync_value_ && new_sync_value > end_;
  }
  inline bool is_received() const { return is_received_; }
  int with_new_start(const uint64_t new_start);
  int with_new_end(const uint64_t new_end);
  int with_sync_value(const uint64_t sync_value);
  void reset() {
    start_ = 0;
    end_ = 0;
    sync_value_ = 0;
    autoinc_version_ = OB_INVALID_VERSION;
    is_received_ = false;
  }
  TO_STRING_KV(K_(start), K_(end), K_(sync_value), K_(autoinc_version), K_(is_received));

  uint64_t start_; // next auto_increment value can be used
  uint64_t end_;   // last available value in the cache(included)
  uint64_t sync_value_;
  int64_t autoinc_version_;
  bool is_received_;
};

class ObGlobalAutoIncService : public logservice::ObIReplaySubHandler,
                               public logservice::ObICheckpointSubHandler,
                               public logservice::ObIRoleChangeSubHandler
{
public:
  ObGlobalAutoIncService() : is_inited_(false), is_leader_(false),
    cache_ls_lock_(common::ObLatchIds::AUTO_INCREMENT_LEADER_LOCK), cache_ls_(NULL),
    gais_request_rpc_(NULL), is_switching_(false),
    switching_mutex_(common::ObLatchIds::AUTO_INCREMENT_LEADER_LOCK)
    {}
  virtual ~ObGlobalAutoIncService() {}

  const static int MUTEX_NUM = 1024;
  const static int INIT_HASHMAP_SIZE = 1000;
  const static int64_t BROADCAST_OP_TIMEOUT = 1000 * 1000; // 1000ms, for broadcast auto increment cache
  int init(const common::ObAddr &addr, common::ObMySQLProxy *mysql_proxy);
  static int mtl_init(ObGlobalAutoIncService *&gais);
  void destroy();
  int clear();

  /*
   * This method handles the request for getting next (batch) auto-increment value.
   * If the cache can satisfy the request, use the auto-increment in the cache to return,
   * otherwise, need to require auto-increment from inner table and fill it in the cache,
   * and then consume the auto-increment in the cache.
   */
  int handle_next_autoinc_request(const ObGAISNextAutoIncValReq &request,
                                  obrpc::ObGAISNextValRpcResult &result);

  /*
   * This method handles the request for getting current auto-increment value. If it exists
   * in the cache, it is taken from the cache, otherwise it is taken from inner table.
   * Note: the cache will not be updated during this method.
   */
  int handle_curr_autoinc_request(const ObGAISAutoIncKeyArg &request,
                                  obrpc::ObGAISCurrValRpcResult &result);

  /*
   * This method handles the request for push local sync value to global. If the local sync
   * is smaller than the sync value in the cache, no update is required. Otherwise,
   * both the cache and inner table need to be updated. Then returns the latest sync value.
   */
  int handle_push_autoinc_request(const ObGAISPushAutoIncValReq &request,
                                  uint64_t &sync_value);

  int handle_clear_autoinc_cache_request(const ObGAISAutoIncKeyArg &request);
  int receive_global_autoinc_cache(const ObGAISBroadcastAutoIncCacheReq &request);

    /*
   * This method handles the request for getting next (batch) sequence value.
   * If the cache can satisfy the request, use the sequence in the cache to return,
   * otherwise, need to require sequence from inner table and fill it in the cache,
   * and then consume the sequence in the cache.
   */
  int handle_next_sequence_request(const ObGAISNextSequenceValReq &request,
                                  obrpc::ObGAISNextSequenceValRpcResult &result);

public:
  void switch_to_follower_forcedly()
  {
    inner_switch_to_follower();
  }
  int switch_to_follower_gracefully()
  {
    return inner_switch_to_follower();
  }
  int resume_leader() {
    ATOMIC_STORE(&is_leader_, true);
    return common::OB_SUCCESS;
  }
  int switch_to_leader() {
    ATOMIC_STORE(&is_leader_, true);
    return common::OB_SUCCESS;
  }

  // for replay, do nothing
  int replay(const void *buffer,
             const int64_t nbytes,
             const palf::LSN &lsn,
             const SCN &scn) override final
  {
    int ret = OB_SUCCESS;
    UNUSED(buffer);
    UNUSED(nbytes);
    UNUSED(lsn);
    UNUSED(scn);
    return ret;
  }

  // for checkpoint, do nothing
  SCN get_rec_scn() override final
  {
    return share::SCN::max_scn();;
  }

  int flush(share::SCN &scn) override final
  {
    int ret = OB_SUCCESS;
    UNUSED(scn);
    return ret;
  }

  void set_cache_ls(storage::ObLS * ls_ptr) {
    ObSpinLockGuard lock(cache_ls_lock_);
    cache_ls_ = ls_ptr;
  }

  TO_STRING_KV(K_(is_inited), K_(is_leader), K_(self), K(autoinc_map_.size()), KP_(cache_ls),
    K_(is_switching));

private:
  int check_leader_(const uint64_t tenant_id, bool &is_leader);
  int fetch_next_node_(const ObGAISNextAutoIncValReq &request, ObAutoIncCacheNode &node);
  int read_value_from_inner_table_(const share::AutoincKey &key,
                                   const int64_t &inner_autoinc_version,
                                   uint64_t &sequence_val,
                                   uint64_t &sync_val);
  int sync_value_to_inner_table_(const ObGAISPushAutoIncValReq &request,
                                 ObAutoIncCacheNode &node,
                                 uint64_t &sync_value);
  static uint64_t calc_next_cache_boundary(const uint64_t insert_value,
                                           const uint64_t cache_size,
                                           const uint64_t max_value)
  {
    uint64_t next_cache_boundary = 0;
    if (max_value < cache_size || insert_value > max_value - cache_size) {
      next_cache_boundary = max_value;
    } else {
      next_cache_boundary = insert_value + cache_size;
    }
    return next_cache_boundary;
  }
  int inner_switch_to_follower();
  int broadcast_global_autoinc_cache();
  int64_t serialize_size_autoinc_cache();
  int serialize_autoinc_cache(SERIAL_PARAMS);
  int deserialize_autoinc_cache(DESERIAL_PARAMS);
  int wait_all_requests_to_finish();
  /*
   * The function will check whether the data of the received node and inner table are consistent.
   * If they are consistent, the inner table will be pushed up by increasing seq_value.
   * Otherwise, the node will be set as invalid.
   */
  int read_and_push_inner_table(const AutoincKey &key,
                                const uint64_t max_value,
                                ObAutoIncCacheNode &received_node);

private:
  bool is_inited_;
  bool is_leader_;
  common::ObAddr self_;
  share::ObAutoIncInnerTableProxy inner_table_proxy_;
  common::hash::ObHashMap<uint64_t, ObAutoIncCacheNode> autoinc_map_; // table_id -> node
  common::ObSpinLock cache_ls_lock_;
  storage::ObLS *cache_ls_;
  lib::ObMutex op_mutex_[MUTEX_NUM];
  ObGAISRequestRpc* gais_request_rpc_;
  bool is_switching_;
  lib::ObMutex switching_mutex_;
};

} // share
} // oceanbase

#endif // _OB_SHARE_OB_GLOBAL_AUTOINC_SERVICE_H_
