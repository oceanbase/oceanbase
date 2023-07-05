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

struct ObAutoIncCacheNode
{
  ObAutoIncCacheNode() : sequence_value_(0), last_available_value_(0), sync_value_(0), autoinc_version_(OB_INVALID_VERSION) {}
  int init(const uint64_t sequence_value,
           const uint64_t last_available_value,
           const uint64_t sync_value,
           const int64_t autoinc_version);
  bool is_valid() const {
    return sequence_value_ > 0 && last_available_value_ >= sequence_value_ &&
             sync_value_ <= sequence_value_;
  }
  bool is_continuous(const uint64_t next_sequence_val,
                     const uint64_t sync_val,
                     const uint64_t max_val) const
  {
    return !is_valid() || (sync_val == sync_value_ &&
      ((last_available_value_ == max_val && next_sequence_val == max_val) ||
        last_available_value_ == next_sequence_val - 1));
  }
  inline bool need_fetch_next_node(const uint64_t base_value,
                                   const uint64_t desired_cnt,
                                   const uint64_t max_value) const;
  inline bool need_sync(const uint64_t new_sync_value) const
  {
    return new_sync_value > sync_value_;
  }
  int update_sequence_value(const uint64_t sequence_value);
  int update_available_value(const uint64_t available_value);
  int update_sync_value(const uint64_t sync_value);
  void reset() {
    sequence_value_ = 0;
    last_available_value_ = 0;
    sync_value_ = 0;
    autoinc_version_ = OB_INVALID_VERSION;
  }
  TO_STRING_KV(K_(sequence_value), K_(last_available_value), K_(sync_value), K_(autoinc_version));

  uint64_t sequence_value_; // next auto_increment value can be used
  uint64_t last_available_value_; // last available value in the cache
  uint64_t sync_value_;
  int64_t autoinc_version_;
};

class ObGlobalAutoIncService : public logservice::ObIReplaySubHandler,
                               public logservice::ObICheckpointSubHandler,
                               public logservice::ObIRoleChangeSubHandler
{
public:
  ObGlobalAutoIncService() : is_inited_(false), is_leader_(false),
    cache_ls_lock_(common::ObLatchIds::AUTO_INCREMENT_LEADER_LOCK), cache_ls_(NULL) {}
  virtual ~ObGlobalAutoIncService() {}

  const static int MUTEX_NUM = 1024;
  const static int INIT_HASHMAP_SIZE = 1000;
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

public:
  void switch_to_follower_forcedly() {
    ATOMIC_STORE(&is_leader_, false);
    clear();
  }
  int switch_to_follower_gracefully() {
    ATOMIC_STORE(&is_leader_, false);
    return clear();
  }
  int resume_leader() {
    ATOMIC_STORE(&is_leader_, true);
    return common::OB_SUCCESS;
  }
  int switch_to_leader() {
    ATOMIC_STORE(&is_leader_, true);
    return clear();
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

private:
  bool is_inited_;
  bool is_leader_;
  common::ObAddr self_;
  share::ObAutoIncInnerTableProxy inner_table_proxy_;
  common::hash::ObHashMap<share::AutoincKey, ObAutoIncCacheNode> autoinc_map_;
  common::ObSpinLock cache_ls_lock_;
  storage::ObLS *cache_ls_;
  lib::ObMutex op_mutex_[MUTEX_NUM];
};

} // share
} // oceanbase

#endif // _OB_SHARE_OB_GLOBAL_AUTOINC_SERVICE_H_
