// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_TRANSACTION_DUP_TABLE_LEASE_H
#define OCEANBASE_TRANSACTION_DUP_TABLE_LEASE_H

#include "lib/hash/ob_hashmap.h"
#include "lib/list/ob_dlist.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/net/ob_addr.h"
#include "storage/tx/ob_dup_table_base.h"
#include "storage/tx/ob_dup_table_stat.h"


namespace oceanbase
{
namespace logservice
{
class ObLogHandler;
}
namespace transaction
{

class ObDupTableLSHandler;
class ObDupTableLeaseRequest;
class ObLSDupTableMeta;

class ObDupTableLSLeaseMgr
{
public:
  OB_UNIS_VERSION(1);

public:
  static const int64_t LEASE_UNIT;
  static const int64_t DEFAULT_LEASE_INTERVAL;
  static const int64_t MIN_LEASE_INTERVAL;

  TO_STRING_KV(K(leader_lease_map_.size()), K(follower_lease_info_));

public:
  ObDupTableLSLeaseMgr() : lease_diag_info_log_buf_(nullptr) { reset(); }

  int init(ObDupTableLSHandler *dup_ls_handle);
  int offline();
  void destroy() { reset(); };
  void reset();
  bool is_master() { return ATOMIC_LOAD(&is_master_); }

  // handle lease requests
  int leader_handle(bool &need_log);
  // post lease requests
  int follower_handle();

  int follower_try_acquire_lease(const share::SCN &lease_log_scn);

  int recive_lease_request(const ObDupTableLeaseRequest &lease_req);

  int leader_takeover(bool is_resume);
  int leader_revoke();

  int prepare_serialize(int64_t &max_ser_size, DupTableLeaseItemArray &lease_header_array);
  int serialize_lease_log(const DupTableLeaseItemArray &unique_id_array,
                          char *buf,
                          const int64_t buf_len,
                          int64_t &pos);
  int deserialize_lease_log(DupTableLeaseItemArray &lease_header_array,
                            const char *buf,
                            const int64_t data_len,
                            int64_t &pos);

  int lease_log_submitted(const bool submit_result,
                          const share::SCN &lease_log_scn,
                          const bool for_replay,
                          const DupTableLeaseItemArray &lease_header_array);
  int lease_log_synced(const bool sync_result,
                       const share::SCN &lease_log_scn,
                       const bool for_replay,
                       const DupTableLeaseItemArray &lease_header_array);

  // int log_cb_success();
  // int log_cb_failure();

  int get_lease_valid_array(LeaseAddrArray &lease_array);

  bool is_follower_lease_valid();

  bool check_follower_lease_serving(const bool election_is_leader,
                                    const share::SCN &max_replayed_scn);

  void print_lease_diag_info_log(const bool is_master);

  int get_lease_mgr_stat(FollowerLeaseMgrStatArr &collect_arr);

  int recover_lease_from_ckpt(const ObDupTableLSCheckpoint::ObLSDupTableMeta &dup_ls_meta);
private:
  bool can_grant_lease_(const common::ObAddr &addr,
                        const share::SCN &local_max_applyed_scn,
                        const DupTableLeaderLeaseInfo &lease_info);
  int update_durable_lease_info_(DupTableLeaderLeaseInfo &single_lease_info);
  int handle_lease_req_cache_(int64_t loop_start_time,
                              const share::SCN &local_max_applyed_scn,
                              const common::ObAddr &addr,
                              DupTableLeaderLeaseInfo &single_lease_info);

  int submit_lease_log_();

  // update request_ts_
  // 1. the follower try to get lease for th first time (request_ts_ = 0)
  // 2. the follower get lease failed for a long time
  void update_request_ts_(int64_t loop_start_time);

  bool need_post_lease_request_(int64_t loop_start_time);

  bool need_retry_lease_operation_(const int64_t cur_time, const int64_t last_time);

private:
  class LeaseReqCacheHandler
  {
  public:
    LeaseReqCacheHandler(ObDupTableLSLeaseMgr *lease_mgr,
                         int64_t loop_start_time,
                         const share::SCN &max_applyed_scn,
                         DupTableLeaseItemArray &item_array)
        : lease_item_array_(item_array)
    {
      lease_mgr_ptr_ = lease_mgr;
      lease_item_array_.reuse();
      renew_lease_count_ = 0;
      max_ser_size_ = 0;
      local_max_applyed_scn_ = max_applyed_scn;
      loop_start_time_ = loop_start_time;
      error_ret = OB_SUCCESS;
    }
    bool operator()(common::hash::HashMapPair<common::ObAddr, DupTableLeaderLeaseInfo> &hash_pair);
    int64_t get_max_ser_size() { return max_ser_size_; }
    int get_error_ret() { return error_ret; }
    int64_t get_renew_lease_count() { return renew_lease_count_; }

    void clear_ser_content()
    {
      lease_item_array_.reuse();
      max_ser_size_ = 0;
    }

    TO_STRING_KV(K(renew_lease_count_),
                 K(max_ser_size_),
                 K(loop_start_time_),
                 K(local_max_applyed_scn_),
                 K(error_ret));

  private:
    ObDupTableLSLeaseMgr *lease_mgr_ptr_;
    DupTableLeaseItemArray &lease_item_array_;
    int64_t renew_lease_count_;
    int64_t max_ser_size_;
    int64_t loop_start_time_;
    share::SCN local_max_applyed_scn_;
    int error_ret;
  };

  class GetLeaseValidAddrFunctor
  {
  public:
    GetLeaseValidAddrFunctor(LeaseAddrArray &addr_arr) : addr_arr_(addr_arr), cur_time_(INT64_MAX)
    {}
    int operator()(common::hash::HashMapPair<common::ObAddr, DupTableLeaderLeaseInfo> &hash_pair);

  private:
    LeaseAddrArray &addr_arr_;
    int64_t cur_time_;
  };

  class DiagInfoGenerator
  {
  public:
    DiagInfoGenerator(bool need_cache, char *info_buf, int64_t info_buf_len, int64_t cur_time)
        : need_cache_(need_cache), info_buf_(info_buf), info_buf_len_(info_buf_len),
          info_buf_pos_(0), cur_time_(cur_time)
    {}

    int64_t get_buf_pos() { return info_buf_pos_; }

    int
    operator()(const common::hash::HashMapPair<common::ObAddr, DupTableLeaderLeaseInfo> &hash_pair);

  private:
    bool need_cache_;
    char *info_buf_;
    int64_t info_buf_len_;
    int64_t info_buf_pos_;
    int64_t cur_time_;
  };

  class LeaderActiveLeaseFunctor
  {
  public:
    LeaderActiveLeaseFunctor() : cur_time_(INT64_MAX) {}
    int operator()(common::hash::HashMapPair<common::ObAddr, DupTableLeaderLeaseInfo> &hash_pair);

  private:
    int64_t cur_time_;
  };

  // class LeaseDurableHandler
  // {
  // public:
  //   LeaseDurableHandler(bool success, const DupTableLeaseItemArray &lease_array)
  //       : is_success_(success), item_array_(lease_array)
  //   {}
  //
  //   int operator()(common::hash::HashMapPair<common::ObAddr, DupTableLeaderLeaseInfo>
  //   &hash_pair);
  //
  // private:
  //   bool is_success_;
  //   const DupTableLeaseItemArray &item_array_;
  // };

  class LeaderLeaseInfoSerCallBack : public IHashSerCallBack
  {
  public:
    LeaderLeaseInfoSerCallBack(char *buf, int64_t buf_len, int64_t pos)
        : IHashSerCallBack(buf, buf_len, pos)
    {}
    int
    operator()(const common::hash::HashMapPair<common::ObAddr, DupTableLeaderLeaseInfo> &hash_pair);
  };

  class LeaderLeaseInfoDeSerCallBack : public IHashDeSerCallBack
  {
  public:
    LeaderLeaseInfoDeSerCallBack(const char *buf, int64_t buf_len, int64_t pos)
        : IHashDeSerCallBack(buf, buf_len, pos)
    {}
    int operator()(DupTableLeaderLeaseMap &lease_map);
  };

  class LeaderLeaseInfoGetSizeCallBack
  {
  public:
    int64_t
    operator()(const common::hash::HashMapPair<common::ObAddr, DupTableLeaderLeaseInfo> &hash_pair);
  };

  class LeaderLeaseMgrStatFunctor
  {
  public:
    LeaderLeaseMgrStatFunctor(FollowerLeaseMgrStatArr &collect_arr,
                              const uint64_t tenant_id,
                              const int64_t collect_ts,
                              const ObAddr &leader_addr,
                              const share::ObLSID ls_id)
                              :
                              collect_arr_(collect_arr),
                              tenant_id_(tenant_id),
                              collect_ts_(collect_ts),
                              leader_addr_(leader_addr),
                              ls_id_(ls_id),
                              cnt_(0) {}
    int operator()(const common::hash::HashMapPair<common::ObAddr, DupTableLeaderLeaseInfo> &hash_pair);

  private:
    FollowerLeaseMgrStatArr &collect_arr_;
    uint64_t tenant_id_;
    int64_t collect_ts_;
    const ObAddr leader_addr_;
    share::ObLSID ls_id_;
    int cnt_;
  };

private:
  SpinRWLock lease_lock_;

  share::ObLSID ls_id_;
  bool is_master_;
  bool is_stopped_;

  // bool is_serializing_; // TODO use lease array to serialize

  ObDupTableLSHandler *dup_ls_handle_ptr_;

  DupTableLeaderLeaseMap leader_lease_map_;

  // int64_t self_request_ts_;
  // DupTableLeaseInfo follower_lease_info_;
  DupTableFollowerLeaseInfo follower_lease_info_;

  int64_t last_lease_req_post_time_;
  int64_t last_lease_req_cache_handle_time_;

  char *lease_diag_info_log_buf_;
};

} // namespace transaction
} // namespace oceanbase
#endif
