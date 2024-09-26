// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.

#ifndef OCEANBASE_TRANSACTION_DUP_TABLE_BASE_H
#define OCEANBASE_TRANSACTION_DUP_TABLE_BASE_H

#include "lib/container/ob_se_array.h"
#include "lib/list/ob_list.h"
#include "logservice/ob_append_callback.h"
#include "observer/ob_server_struct.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "share/ob_ls_id.h"
#include "share/ob_rpc_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scn.h"
#include "storage/tx/ob_tx_big_segment_buf.h"

namespace oceanbase
{

namespace logservice
{
class ObLogHandler;
}

namespace transaction
{

class ObDupTableLSHandler;
class ObDupTableLSLeaseMgr;
class ObLSDupTabletsMgr;

typedef common::ObSEArrayImpl<palf::LSN, 3> LogLsnArray;
typedef common::ObSEArray<share::SCN, 3> LogScnArray;
typedef common::ObSEArray<common::ObAddr, 16> LeaseAddrArray;

class DupTableDiagStd
{
public:
  static const char *DUP_DIAG_INDENT_SPACE;
  static const char *DUP_DIAG_COMMON_PREFIX;

  enum TypeIndex
  {
    LEASE_INDEX = 0,
    TBALET_MEMBER_INDEX,
    TABLET_SET_INDEX,
    TS_SYNC_INDEX,
    MAX_INDEX,
  };
  static const uint64_t DUP_DIAG_INFO_LOG_BUF_LEN[TypeIndex::MAX_INDEX];
  static const int64_t DUP_DIAG_PRINT_INTERVAL[TypeIndex::MAX_INDEX];
};

struct DupTableInterfaceStat
{
  int64_t dup_table_follower_read_succ_cnt_;
  int64_t dup_table_follower_read_tablet_not_exist_cnt_;
  int64_t dup_table_follower_read_tablet_not_ready_cnt_;
  int64_t dup_table_follower_read_lease_expired_cnt_;

  int64_t dup_table_redo_sync_succ_cnt_;
  int64_t dup_table_redo_sync_fail_cnt_;

  share::SCN dup_table_max_applying_scn_;

  int64_t dup_table_log_entry_cnt_;
  int64_t dup_table_log_entry_total_size_;

  int64_t dup_table_log_replay_total_time_;
  int64_t dup_table_log_deser_total_time_;
  int64_t dup_table_lease_log_sync_total_time_;
  int64_t dup_table_tablet_log_sync_total_time_;

  int64_t dup_table_ls_leader_takeover_ts_;

  void reset()
  {
    dup_table_follower_read_succ_cnt_ = 0;
    dup_table_follower_read_tablet_not_exist_cnt_ = 0;
    dup_table_follower_read_tablet_not_ready_cnt_ = 0;
    dup_table_follower_read_lease_expired_cnt_ = 0;

    dup_table_max_applying_scn_.set_min();

    dup_table_redo_sync_succ_cnt_ = 0;
    dup_table_redo_sync_fail_cnt_ = 0;

    dup_table_log_entry_cnt_ = 0;
    dup_table_log_entry_total_size_ = 0;

    dup_table_log_replay_total_time_ = 0;
    dup_table_log_deser_total_time_ = 0;
    dup_table_lease_log_sync_total_time_ = 0;
    dup_table_tablet_log_sync_total_time_ = 0;

    dup_table_ls_leader_takeover_ts_ = 0;
  }

  TO_STRING_KV(K(dup_table_follower_read_succ_cnt_),
               K(dup_table_follower_read_tablet_not_exist_cnt_),
               K(dup_table_follower_read_tablet_not_ready_cnt_),
               K(dup_table_follower_read_lease_expired_cnt_),
               K(dup_table_redo_sync_succ_cnt_),
               K(dup_table_redo_sync_fail_cnt_),
               K(dup_table_max_applying_scn_),
               K(dup_table_log_entry_cnt_),
               K(dup_table_log_entry_total_size_),
               K(dup_table_log_replay_total_time_),
               K(dup_table_log_deser_total_time_),
               K(dup_table_lease_log_sync_total_time_),
               K(dup_table_tablet_log_sync_total_time_),
               K(dup_table_ls_leader_takeover_ts_));
};

#define DUP_LEASE_LIFE_PREFIX "[DupLeaseLife] "
#define DUP_TABLET_LIFE_PREFIX "[DupTabletLife] "

/*******************************************************
 *  Dup_Table LS Role State
 *******************************************************/
enum class ObDupTableLSRoleState : int64_t
{
  UNKNOWN = 0,

  ROLE_STATE_CHANGING = 1, // Follower

  LS_REVOKE_SUCC = 10,
  LS_TAKEOVER_SUCC = 11,

  LS_OFFLINE_SUCC = 20,
  LS_ONLINE_SUCC = 21,

  LS_STOP_SUCC = 30,
  LS_START_SUCC = 31,
};

struct ObDupTableLSRoleStateContainer
{
  int64_t role_state_;
  int64_t offline_state_;
  int64_t stop_state_;

  void reset()
  {
    role_state_ = static_cast<int64_t>(ObDupTableLSRoleState::UNKNOWN);
    offline_state_ = static_cast<int64_t>(ObDupTableLSRoleState::UNKNOWN);
    stop_state_ = static_cast<int64_t>(ObDupTableLSRoleState::UNKNOWN);
  }

  int64_t *get_target_state_ref(ObDupTableLSRoleState target_state);
  bool check_target_state(ObDupTableLSRoleState target_state);

  TO_STRING_KV(K(role_state_), K(offline_state_), K(stop_state_));
};

class ObDupTableLSRoleStateHelper
{
public:
  ObDupTableLSRoleStateHelper(const char *module_name) : module_name_(module_name)
  {
    cur_state_.reset();
  }

  void reset()
  {
    cur_state_.role_state_ = static_cast<int64_t>(ObDupTableLSRoleState::LS_REVOKE_SUCC);
    cur_state_.stop_state_ = static_cast<int64_t>(ObDupTableLSRoleState::LS_STOP_SUCC);
    cur_state_.offline_state_ = static_cast<int64_t>(ObDupTableLSRoleState::LS_ONLINE_SUCC);
  }

  bool is_leader();
  bool is_follower();
  bool is_offline();
  bool is_online();
  bool is_stopped();
  bool is_started();

  bool is_leader_serving() { return is_leader() && is_online() && is_started(); }
  bool is_follower_serving() { return is_follower() && is_online() && is_started(); }
  bool is_active_ls() { return is_online() && is_started(); }

  int prepare_state_change(const ObDupTableLSRoleState &target_state,
                           ObDupTableLSRoleStateContainer &restore_state);
  int restore_state(const ObDupTableLSRoleState &target_state,
                           ObDupTableLSRoleStateContainer &restore_state);
  int state_change_succ(const ObDupTableLSRoleState &target_state,
                           ObDupTableLSRoleStateContainer &restore_state);

  TO_STRING_KV("module_name", module_name_, K(cur_state_));

private:
  ObDupTableLSRoleStateContainer cur_state_;
  const char *module_name_;
};


/*******************************************************
 *  HashMapTool (not thread safe)
 *******************************************************/
template <typename HashMap, typename Update_CallBack>
int hash_for_each_update(HashMap &hash_map, Update_CallBack &callback)
{
  int ret = OB_SUCCESS;
  int64_t cnt = 0;
  typename HashMap::iterator iter;
  for (iter = hash_map.begin(); iter != hash_map.end(); iter++) {
    // int
    if (OB_FAIL(callback(*iter))) {
      break;
    }
    cnt++;
  }
  // DUP_TABLE_LOG(INFO, "hash for each update", K(ret), K(cnt));
  return ret;
}

template <typename HashMap, typename RemoveIF_CallBack, typename HashKeyType>
int hash_for_each_remove(HashKeyType tmp_key, HashMap &hash_map, RemoveIF_CallBack &callback)
{
  int ret = OB_SUCCESS;
  typename HashMap::iterator iter;
  // typedef typename HashMap::_key_type HashKey2;
  TransModulePageAllocator allocator;
  ObList<HashKeyType, TransModulePageAllocator> del_list(allocator);
  // ObList<typename HashMap>::iterator  del_iter;
  int64_t cnt = 0;

  for (iter = hash_map.begin(); iter != hash_map.end(); iter++) {
    // bool
    if (callback(*iter)) {
      if (OB_FAIL(del_list.push_back(iter->first))) {
        DUP_TABLE_LOG(WARN, "insert into del_list failed", K(ret), K(del_list.size()));
        break;
      }
    }
    cnt++;
  }

  if (OB_SUCC(ret)) {
    if (del_list.size() > 0) {
      while (OB_SUCC(del_list.pop_front(tmp_key))) {
        if (OB_FAIL(hash_map.erase_refactored(tmp_key))) {
          DUP_TABLE_LOG(WARN, "erase from hash map failed", K(ret), K(tmp_key));
          break;
        }
      }
      if (OB_ENTRY_NOT_EXIST == ret) {
        // when pop all list, rewrite ret code
        DUP_TABLE_LOG(INFO, "end del in while loop", K(ret));
        ret = OB_SUCCESS;
      }
      // DUP_TABLE_LOG(WARN, "del item from hash map failed", K(ret), K(tmp_key));
    }
  }
  // DUP_TABLE_LOG(DEBUG, "hash for each remove", K(ret), K(cnt), K(del_list.size()));
  return ret;
}

template <typename HashMap, typename RemoveIF_CallBack, typename HashKeyType>
int hash_for_each_remove_with_timeout(HashKeyType tmp_key,
                                      HashMap &hash_map,
                                      RemoveIF_CallBack &callback,
                                      int64_t &remain_time)
{
  int ret = OB_SUCCESS;
  typename HashMap::iterator iter;
  // typedef typename HashMap::_key_type HashKey2;
  TransModulePageAllocator allocator;
  ObList<HashKeyType, TransModulePageAllocator> del_list(allocator);
  // ObList<typename HashMap>::iterator  del_iter;
  int64_t cnt = 0;
  int64_t scan_start_time = ObTimeUtility::fast_current_time();
  int64_t cur_time = 0;
  const int64_t update_cur_cnt = 100;

  const bool enable_timeout_check = (remain_time == INT64_MAX);

  if (OB_SUCC(ret) && enable_timeout_check && remain_time <= 0) {
    ret = OB_TIMEOUT;
    DUP_TABLE_LOG(WARN, "the remain timeout is not enough", K(ret), K(remain_time));
  }

  for (iter = hash_map.begin(); iter != hash_map.end() && OB_SUCC(ret); iter++) {
    // bool
    if (callback(*iter)) {
      if (OB_FAIL(del_list.push_back(iter->first))) {
        DUP_TABLE_LOG(WARN, "insert into del_list failed", K(ret), K(del_list.size()));
      }
    }
    // check whether timeout
    if (OB_SUCC(ret) && enable_timeout_check) {
      if (OB_UNLIKELY(0 == ((++cnt) % update_cur_cnt))) {
        cur_time = ObTimeUtility::fast_current_time();
        if ((cur_time - scan_start_time) > remain_time) {
          ret = OB_TIMEOUT;
          DUP_TABLE_LOG(WARN, "scan map cost too much time", K(ret), K(cnt), K(remain_time),
                        K(del_list.size()));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (del_list.size() > 0) {
      while (OB_SUCC(del_list.pop_front(tmp_key))) {
        if (OB_FAIL(hash_map.erase_refactored(tmp_key))) {
          DUP_TABLE_LOG(WARN, "erase from hash map failed", K(ret), K(tmp_key));
          break;
        }
      }
      if (OB_ENTRY_NOT_EXIST == ret) {
        // when pop all list, rewrite ret code
        DUP_TABLE_LOG(DEBUG, "end del in while loop", K(ret));
        ret = OB_SUCCESS;
      }
    }
  }

  remain_time = remain_time - (ObTimeUtility::fast_current_time() - scan_start_time);
  if (OB_SUCC(ret) && enable_timeout_check && remain_time < 0) {
    ret = OB_TIMEOUT;
    DUP_TABLE_LOG(WARN, "the remain timeout is not enough", K(ret), K(remain_time));
  }
  // DUP_TABLE_LOG(DEBUG, "hash for each remove", K(ret), K(cnt), K(timeout), K(del_list.size()));
  return ret;
}

class IHashSerCallBack
{
public:
  IHashSerCallBack(char *buf, int64_t buf_len, int64_t pos)
  {
    header_pos_ = 0;
    buf_ = buf;
    pos_ = pos;
    buf_len_ = buf_len;
  }

  void reserve_header()
  {
    int32_t hash_size = INT32_MAX;
    header_pos_ = pos_;
    pos_ = pos_ + common::serialization::encoded_length_i32(hash_size);
  }

  int serialize_size(int32_t hash_size)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(common::serialization::encode_i32(buf_, buf_len_, pos_, hash_size))) {
      DUP_TABLE_LOG(WARN, "serialize tablets hash size failed", K(ret));
    }
    // DUP_TABLE_LOG(INFO, "serialize hash size", K(ret), K(buf_len_), K(pos_), K(hash_size));
    return ret;
  }

  int64_t get_pos() { return pos_; }

protected:
  int64_t header_pos_;

  char *buf_;
  int64_t pos_;
  int64_t buf_len_;
};

class IHashDeSerCallBack
{
public:
  IHashDeSerCallBack(const char *buf, int64_t buf_len, int64_t pos)
  {
    hash_size_ = 0;

    buf_ = buf;
    pos_ = pos;
    buf_len_ = buf_len;
  }

  // virtual int insert_deser_kv() = 0;

  int deserialize_size(int32_t &hash_size)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(common::serialization::decode_i32(buf_, buf_len_, pos_, &hash_size))) {
      DUP_TABLE_LOG(WARN, "deserialize tablets hash size failed", K(ret));
    }
    // DUP_TABLE_LOG(INFO, "deserialize hash size", K(ret), K(buf_len_), K(pos_), K(hash_size));
    return ret;
  }

  // int64_t get_hash_size() { return hash_size_; }

  int64_t get_pos() { return pos_; }

protected:
  int32_t hash_size_;

  const char *buf_;
  int64_t pos_;
  int64_t buf_len_;
};

template <typename HashClass, typename Ser_CallBack>
int hash_for_each_serialize(HashClass &hash_map, Ser_CallBack &callback)
{
  int ret = OB_SUCCESS;
  typedef typename HashClass::const_iterator ConstHashIter;

  ConstHashIter hash_iter = hash_map.begin();
  int32_t hash_size = hash_map.size();
  // callback.reserve_header();

  if (OB_FAIL(callback.serialize_size(hash_size))) {
    DUP_TABLE_LOG(WARN, "serialize hash size failed", K(ret));
  }

  if (OB_SUCC(ret)) {
    for (; OB_SUCC(ret) && hash_iter != hash_map.end(); hash_iter++) {
      // int
      if (OB_FAIL(callback(*hash_iter))) {
        break;
        DUP_TABLE_LOG(WARN, "serialize hash iter failed", K(ret));
      }
    }
  }

  // DUP_TABLE_LOG(INFO, "serialize hash item", K(ret), K(hash_size));
  return ret;
}

template <typename HashClass, typename DeSer_CallBack>
int hash_for_each_deserialize(HashClass &hash_map, DeSer_CallBack &callback)
{
  int ret = OB_SUCCESS;

  int32_t hash_size = 0;
  common::ObTabletID tmp_tablet_id;

  if (OB_FAIL(callback.deserialize_size(hash_size))) {
    DUP_TABLE_LOG(WARN, "deserialize hash_size failed", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < hash_size; i++) {
      // int
      if (OB_FAIL(callback(hash_map))) {
        DUP_TABLE_LOG(WARN, "deserialize kv failed", K(ret));
        // } else if (OB_FAIL(callback.insert_deser_kv(hash_map))) {
        //   DUP_TABLE_LOG(WARN, "insert into hash_struct failed", K(ret));
      }
    }
  }

  // DUP_TABLE_LOG(INFO, "deserialize hash item", K(ret), K(hash_size));
  return ret;
}

template <typename HashClass, typename Size_CallBack>
int64_t hash_for_each_serialize_size(HashClass &hash_map, Size_CallBack &callback)
{

  typename HashClass::const_iterator const_iter;
  int64_t total_size = 0;
  int32_t hash_size = 0;

  for (const_iter = hash_map.begin(); const_iter != hash_map.end(); const_iter++) {
    // int64_t
    hash_size++;
    total_size += callback(*const_iter);
  }

  total_size += common::serialization::encoded_length_i32(hash_size);
  return total_size;
}

/*******************************************************
 *  Dup_Table Lease
 *******************************************************/

enum class LeaseReqCacheState
{
  INVALID  = 0,
  PREPARE,
  READY,
};

struct DupTableLeaseReqCache
{
public:
  int64_t request_ts_;
  int64_t lease_acquire_ts_;
  int64_t lease_interval_us_;

private:
  LeaseReqCacheState state_;

public:
  // reset after log_cb
  void reset()
  {
    request_ts_ = -1;
    lease_acquire_ts_ = -1;
    lease_interval_us_ = -1;
    state_ = LeaseReqCacheState::INVALID;
  }
  void set_invalid() { state_ = LeaseReqCacheState::INVALID; }
  void set_ready() { state_ = LeaseReqCacheState::READY; }
  bool is_invalid() const { return state_ == LeaseReqCacheState::INVALID; }
  bool is_prepare() const { return state_ == LeaseReqCacheState::PREPARE; }
  bool is_ready() const { return state_ == LeaseReqCacheState::READY; }

  void renew_lease_req(const int64_t &request_ts, const int64_t &lease_interval)
  {
    request_ts_ = request_ts;
    lease_acquire_ts_ = 0;
    lease_interval_us_ = lease_interval;
    state_ = LeaseReqCacheState::PREPARE;
  }

  void grant_lease_failed()
  {
    state_ = LeaseReqCacheState::INVALID;
    lease_acquire_ts_ = -1;
  }

  void grant_lease_success(const int64_t &acquire_ts)
  {
    state_ = LeaseReqCacheState::READY;
    lease_acquire_ts_ = acquire_ts;
  }

  DupTableLeaseReqCache() { reset(); }

  TO_STRING_KV(K(request_ts_), K(lease_acquire_ts_), K(lease_interval_us_), K(state_));
};

struct DupTableDurableLease
{
  int64_t request_ts_;
  int64_t lease_interval_us_;

  void reset()
  {
    request_ts_ = -1;
    lease_interval_us_ = -1;
  }

  DupTableDurableLease() { reset(); }

  TO_STRING_KV(K(request_ts_), K(lease_interval_us_));

  OB_UNIS_VERSION(1);
};

struct DupTableLeaderLeaseInfo
{
  DupTableDurableLease confirmed_lease_info_;
  DupTableLeaseReqCache cache_lease_req_;
  int64_t lease_expired_ts_;

  void reset()
  {
    confirmed_lease_info_.reset();
    cache_lease_req_.reset();
    lease_expired_ts_ = -1;
  }

  DupTableLeaderLeaseInfo() { reset(); }
  TO_STRING_KV(K(confirmed_lease_info_), K(cache_lease_req_), K(lease_expired_ts_));
};

struct DupTableFollowerLeaseInfo
{
  DupTableDurableLease durable_lease_;
  int64_t lease_expired_ts_;
  share::SCN last_lease_scn_;
  share::SCN lease_acquire_scn_;

  void reset()
  {
    durable_lease_.reset();
    lease_expired_ts_ = -1;
    last_lease_scn_.reset();
    lease_acquire_scn_.reset();
  }

  DupTableFollowerLeaseInfo() { reset(); }

  TO_STRING_KV(K(durable_lease_), K(lease_expired_ts_), K(last_lease_scn_), K(lease_acquire_scn_));
};

typedef common::hash::
    ObHashMap<common::ObAddr, DupTableLeaderLeaseInfo, common::hash::NoPthreadDefendMode>
        DupTableLeaderLeaseMap;

class DupTableLeaseLogHeader
{
  OB_UNIS_VERSION(1);

public:
  static const uint64_t INVALID_LEASE_LOG_TYPE = 0;
  static const uint64_t DURABLE_LEASE_LOG_TYPE = 1;

public:
  DupTableLeaseLogHeader() : addr_() { reset(); }
  DupTableLeaseLogHeader(const common::ObAddr &addr)
      : addr_(addr), lease_log_code_(DURABLE_LEASE_LOG_TYPE)
  {}

  void reset()
  {
    lease_log_code_ = INVALID_LEASE_LOG_TYPE;
    addr_.reset();
  }

  void set_lease_owner(const common::ObAddr &addr)
  {
    addr_ = addr;
    lease_log_code_ = DURABLE_LEASE_LOG_TYPE;
  }
  const common::ObAddr &get_lease_owner() const { return addr_; }

  bool is_durable_lease_log() const { return lease_log_code_ == DURABLE_LEASE_LOG_TYPE; }

  TO_STRING_KV(K(addr_), K(lease_log_code_));

private:
  common::ObAddr addr_;
  uint64_t lease_log_code_;
};

struct DupTableLeaseItem
{
  DupTableLeaseLogHeader log_header_;
  DupTableDurableLease durable_lease_;

  DupTableLeaseItem()
  {
    log_header_.reset();
    durable_lease_.reset();
  }

  DupTableLeaseItem(const DupTableLeaseLogHeader &lease_log_header,
                    const DupTableDurableLease &durable_lease)
      : log_header_(lease_log_header), durable_lease_(durable_lease)
  {}

  TO_STRING_KV(K(log_header_), K(durable_lease_));

  OB_UNIS_VERSION(1);
};

typedef ObSEArray<DupTableLeaseItem, 16> DupTableLeaseItemArray;

class DupTableDurableLeaseLogBody
{
  OB_UNIS_VERSION(1);

public:
  DupTableDurableLeaseLogBody(DupTableDurableLease &durable_lease) : durable_lease_(durable_lease)
  {
    // reset();
  }
  void reset() { durable_lease_.reset(); }

  TO_STRING_KV(K(durable_lease_));

private:
  DupTableDurableLease &durable_lease_;
};

/*******************************************************
 *  Dup_Table Tablets
 *******************************************************/

class DupTabletSetCommonHeader
{
  OB_UNIS_VERSION(1);

public:
  static const int64_t MAX_TABLET_COUNT_IN_SINGLE_SET = 10 * 10000;

public:
  static const uint64_t INVALID_SET_TYPE = 0;
  static const uint64_t INVALID_UNIQUE_ID = 0;
  static const uint64_t INVALID_SPECIAL_OP = 0;
  static const uint64_t INVALID_COMMON_CODE = 0;

private:
  static const uint64_t DUP_FREE_SET_TYPE = 1;
  static const uint64_t DUP_OLD_SET_TYPE = 2;
  static const uint64_t DUP_NEW_SET_TYPE = 3;
  static const uint64_t DUP_READABLE_SET_TYPE = 4;

  // seralize a special op with a empty new set
  // confirm it and free in tablet_log_synced
  static const uint64_t DUP_SPECIAL_OP_CLEAN_ALL_READABLE_SET = 1;
  static const uint64_t DUP_SPECIAL_OP_CLEAN_DATA_CONFIRMING_SET = 2;
  static const uint64_t DUP_SPECIAL_OP_BLOCK_CONFIRMING = 3;

  // static const uint64_t UNIQUE_ID_BIT_COUNT = 32;
  // static const uint64_t TABLET_SET_BIT_COUNT = 4;
  // static const uint64_t SPECIAL_OP_BIT_COUNT = 8;
  // static const uint64_t UNIQUE_ID_BIT = static_cast<uint64_t>(0xFFFFFFFFULL);
  // static const uint64_t TABLET_SET_BIT = static_cast<uint64_t>(0xFULL) << UNIQUE_ID_BIT_COUNT;
  // static const uint64_t SPECIAL_OP_BIT = static_cast<uint64_t>(0xFFULL) << (UNIQUE_ID_BIT_COUNT +
  // TABLET_SET_BIT_COUNT);

public:
  TO_STRING_KV(K(unique_id_), K(tablet_set_type_), K(sp_op_type_));

  DupTabletSetCommonHeader(const uint64_t id) : unique_id_(id)
  {
    // set_free();
    // set_invalid_sp_op_type();
    reuse();
  }
  DupTabletSetCommonHeader() { reset(); }
  ~DupTabletSetCommonHeader() { reset(); }

  bool is_valid() const { return unique_id_is_valid() && tablet_set_type_is_valid(); }
  void reset()
  {
    set_invalid_unique_id();
    set_invalid_tablet_set_type();
    set_invalid_sp_op_type();
  }
  void reuse()
  {
    set_free();
    set_invalid_sp_op_type();
  }

  void set_invalid_unique_id() { set_unique_id_(INVALID_UNIQUE_ID); }
  void set_invalid_tablet_set_type() { change_tablet_set_type_(INVALID_SET_TYPE); }
  void set_invalid_sp_op_type() { set_special_op_(INVALID_SPECIAL_OP); }

  uint64_t get_unique_id() const { return unique_id_; }
  int64_t get_special_op() const { return sp_op_type_; }
  int64_t get_tablet_set_type() const { return tablet_set_type_; }
  bool unique_id_is_valid() const { return INVALID_UNIQUE_ID != unique_id_; }
  bool tablet_set_type_is_valid() const { return INVALID_SET_TYPE != tablet_set_type_; }

  bool is_free() const { return get_tablet_set_type_() == DUP_FREE_SET_TYPE; }
  bool is_readable_set() const { return get_tablet_set_type_() == DUP_READABLE_SET_TYPE; }
  bool is_new_set() const { return get_tablet_set_type_() == DUP_NEW_SET_TYPE; }
  bool is_old_set() const { return get_tablet_set_type_() == DUP_OLD_SET_TYPE; }
  void set_free() { change_tablet_set_type_(DUP_FREE_SET_TYPE); }
  void set_readable() { change_tablet_set_type_(DUP_READABLE_SET_TYPE); }
  void set_new() { change_tablet_set_type_(DUP_NEW_SET_TYPE); }
  void set_old() { change_tablet_set_type_(DUP_OLD_SET_TYPE); }

  void set_op_of_clean_all_readable_set()
  {
    set_special_op_(DUP_SPECIAL_OP_CLEAN_ALL_READABLE_SET);
  }
  bool need_clean_all_readable_set() const
  {
    return DUP_SPECIAL_OP_CLEAN_ALL_READABLE_SET == sp_op_type_;
  }
  void set_op_of_clean_data_confirming_set()
  {
    set_special_op_(DUP_SPECIAL_OP_CLEAN_DATA_CONFIRMING_SET);
  }
  bool need_clean_data_confirming_set() const
  {
    return DUP_SPECIAL_OP_CLEAN_DATA_CONFIRMING_SET == sp_op_type_;
  }
  void set_op_of_block_confirming() { set_special_op_(DUP_SPECIAL_OP_BLOCK_CONFIRMING); }
  bool need_block_confirming() const { return DUP_SPECIAL_OP_BLOCK_CONFIRMING == sp_op_type_; }
  // bool contain_special_op(uint64_t special_op) const { return get_special_op_() == special_op; }
  bool no_specail_op() const { return INVALID_SPECIAL_OP == sp_op_type_; }
  void copy_tablet_set_common_header(const DupTabletSetCommonHeader &src_common_header)
  {
    set_unique_id_(src_common_header.get_unique_id());
    set_special_op_(src_common_header.get_special_op());
    change_tablet_set_type_(src_common_header.get_tablet_set_type());
  }

public:
  bool operator==(const DupTabletSetCommonHeader &dup_common_header) const;
  bool operator!=(const DupTabletSetCommonHeader &dup_common_header) const;

private:
  void set_unique_id_(const uint64_t id)
  {
    uint64_t real_id = id;
    if (id > UINT32_MAX) {
      DUP_TABLE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "the unique_id is too large, set invalid", K(id),
                        K(UINT32_MAX));
      real_id = INVALID_UNIQUE_ID;
    }
    unique_id_ = id;
  }
  void change_tablet_set_type_(const int64_t set_type) { tablet_set_type_ = set_type; }
  int64_t get_tablet_set_type_() const { return tablet_set_type_; }

  void set_special_op_(const int64_t special_op_type) { sp_op_type_ = special_op_type; }
  int64_t get_special_op_() const
  {
    // return (common_code_ & SPECIAL_OP_BIT) >> (UNIQUE_ID_BIT_COUNT + TABLET_SET_BIT_COUNT);
    return sp_op_type_;
  }

private:
  uint64_t unique_id_;
  int64_t tablet_set_type_;
  int64_t sp_op_type_;
};

typedef common::ObSEArray<DupTabletSetCommonHeader, 3> DupTabletSetIDArray;

/*******************************************************
 *  Dup_Table Checkpoint
 *******************************************************/

class ObDupTableLSCheckpoint
{
public:
  class ObLSDupTableMeta
  {
  public:
    share::ObLSID ls_id_;

    DupTableLeaseItemArray lease_item_array_;
    share::SCN lease_log_applied_scn_;

    // the scn of a tablet log which contain all readable tablet sets.
    // If we want to get all readable sets, only need replay this log.
    share::SCN readable_tablets_base_scn_;

    // the scn of the first tablet log which modify any readable set
    // If we want to get all readable sets, we need replay from the log to the latest log
    // clear this scn after set base_scn
    share::SCN readable_tablets_min_base_applied_scn_;

    TO_STRING_KV(K(ls_id_),
                 K(lease_log_applied_scn_),
                 K(readable_tablets_base_scn_),
                 K(readable_tablets_min_base_applied_scn_),
                 K(lease_item_array_.count()));

    ObLSDupTableMeta() { reset(); }

    bool is_valid() const { return ls_id_.is_valid(); }
    void reset()
    {
      ls_id_.reset();
      lease_item_array_.reset();
      lease_log_applied_scn_.reset();
      readable_tablets_base_scn_.reset();
      readable_tablets_min_base_applied_scn_.reset();
    }

    int copy(const ObLSDupTableMeta &dup_ls_meta);

    DISALLOW_COPY_AND_ASSIGN(ObLSDupTableMeta);
    OB_UNIS_VERSION(1);
  };

  TO_STRING_KV(K(dup_ls_meta_), K(lease_log_rec_scn_), K(start_replay_scn_),
               K(readable_ckpt_base_scn_is_accurate_));

public:
  ObDupTableLSCheckpoint() { reset(); }
  void default_init(const share::ObLSID &ls_id) { dup_ls_meta_.ls_id_ = ls_id; }

  bool is_useful_meta() const;

  int get_dup_ls_meta(ObLSDupTableMeta &dup_ls_meta) const;
  int set_dup_ls_meta(const ObLSDupTableMeta &dup_ls_meta);
  share::SCN get_lease_log_rec_scn() const;
  int reserve_ckpt_memory(const DupTableLeaseItemArray &lease_log_items);
  int update_ckpt_after_lease_log_synced(const DupTableLeaseItemArray &lease_log_items,
                                         const share::SCN &scn,
                                         const bool modify_readable_sets,
                                         const bool contain_all_readable,
                                         const bool for_replay);
  bool contain_all_readable_on_replica() const;

  int flush();

  int offline();
  int online();

  void reset()
  {
    dup_ls_meta_.reset();
    lease_log_rec_scn_.reset();
    start_replay_scn_.reset();
    readable_ckpt_base_scn_is_accurate_ = true;
  }

private:
  SpinRWLock ckpt_rw_lock_;
  ObLSDupTableMeta dup_ls_meta_;
  share::SCN lease_log_rec_scn_;
  share::SCN start_replay_scn_;
  // ckpt_base_scn means, when replay dup log with base_scn,
  // this node cotain all readable set the same with leader.
  // but dup tale log can not write all readable set with the limit of size
  // we use this bool value to check base scn hold correct messages
  bool readable_ckpt_base_scn_is_accurate_;
};

/*******************************************************
 *  Dup_Table Log
 *******************************************************/
enum class DupTableLogEntryType
{
  TabletChangeLog = 1,
  LeaseListLog,
  DupTableStatLog,
  MAX

};

static const char *get_entry_type_str(const DupTableLogEntryType &entry_type)
{
  const char *entry_str = nullptr;
  switch (entry_type) {
  case DupTableLogEntryType::TabletChangeLog: {
    entry_str = "DupTabletLog";
    break;
  }
  case DupTableLogEntryType::LeaseListLog: {
    entry_str = "DupLeaseLog";
    break;
  }

  case DupTableLogEntryType::DupTableStatLog: {
    entry_str = "DupStatLog";
    break;
  }
  default: {
    entry_str = "UNKNOWN entry type";
    break;
  }
  }
  return entry_str;
}

typedef common::ObSEArray<DupTableLogEntryType, 2> DupLogTypeArray;

// record log entry size after log entry header by int64_t
struct DupTableLogEntryHeader
{
  DupTableLogEntryType entry_type_;

  const static int64_t RESERVED_LOG_ENTRY_SIZE_SPACE = sizeof(int64_t);

  TO_STRING_KV(K(entry_type_));
  OB_UNIS_VERSION(1);
};

struct DupTableStatLog
{
  int64_t lease_addr_cnt_;
  int64_t leader_readable_cnt_;
  int64_t all_tablet_set_cnt_;
  int64_t logging_readable_cnt_;

  TO_STRING_KV(K(lease_addr_cnt_), K(leader_readable_cnt_), K(all_tablet_set_cnt_),
               K(logging_readable_cnt_));

  DupTableStatLog() { reset(); }

  void reset()
  {
    lease_addr_cnt_ = -1;
    leader_readable_cnt_ = -1;
    all_tablet_set_cnt_ = -1;
    logging_readable_cnt_ = -1;
  }

  OB_UNIS_VERSION(1);
};

class ObDupTableLogOperator : public logservice::AppendCb
{
public:
  ObDupTableLogOperator(const share::ObLSID &ls_id,
                        logservice::ObLogHandler *log_handler,
                        ObDupTableLSCheckpoint *dup_ls_ckpt,
                        ObDupTableLSLeaseMgr *lease_mgr,
                        ObLSDupTabletsMgr *tablets_mgr,
                        DupTableInterfaceStat *interface_stat)
      : ls_id_(ls_id), block_buf_(nullptr), log_handler_(log_handler), dup_ls_ckpt_(dup_ls_ckpt),
        lease_mgr_ptr_(lease_mgr), tablet_mgr_ptr_(tablets_mgr), interface_stat_ptr_(interface_stat)
  {
    reset();
  }
  ~ObDupTableLogOperator() { reset(); }
  void reuse();
  void reset();

  int submit_log_entry();

  int merge_replay_block(const char *replay_buf, int64_t replay_buf_len);

  int deserialize_log_entry();

  bool is_busy();

  bool check_is_busy_without_lock();

  void rlock_for_log() { log_lock_.rdlock(); }
  void unlock_for_log() { log_lock_.unlock(); }

  int on_success();
  int on_failure();

  int replay_succ();

  void print_statistics_log();

public:
  void set_logging_scn(const share::SCN &scn);

  TO_STRING_KV(K(ls_id_),
               K(big_segment_buf_),
               K(logging_tablet_set_ids_),
               K(logging_lease_addrs_),
               K(logging_scn_),
               K(logging_lsn_));

private:
  static const int64_t MAX_LOG_BLOCK_SIZE;
  static const int64_t RESERVED_LOG_HEADER_SIZE; // 100 Byte
  int prepare_serialize_log_entry_(int64_t &max_ser_size, DupLogTypeArray &type_array);
  int serialize_log_entry_(const int64_t max_ser_size, const DupLogTypeArray &type_array);
  int deserialize_log_entry_();
  int retry_submit_log_block_();
  int sync_log_succ_(const bool for_replay);

private:
  void after_submit_log(const bool submit_result, const bool for_replay);

#define LOG_OPERATOR_INIT_CHECK                                                                  \
  if (OB_SUCC(ret)) {                                                                            \
    if (OB_ISNULL(log_handler_) || OB_ISNULL(lease_mgr_ptr_) || OB_ISNULL(tablet_mgr_ptr_)       \
        || OB_ISNULL(interface_stat_ptr_)) {                                                     \
      ret = OB_NOT_INIT;                                                                         \
      DUP_TABLE_LOG(ERROR, "invalid log operator", K(ret), KP(log_handler_), KP(lease_mgr_ptr_), \
                    KP(tablet_mgr_ptr_), KP(interface_stat_ptr_));                               \
    }                                                                                            \
  }

private:
  SpinRWLock log_lock_;

  share::ObLSID ls_id_;

  char *block_buf_;
  // int64_t block_buf_pos_;

  ObTxBigSegmentBuf big_segment_buf_;

  logservice::ObLogHandler *log_handler_;

  ObDupTableLSCheckpoint *dup_ls_ckpt_;

  ObDupTableLSLeaseMgr *lease_mgr_ptr_;
  ObLSDupTabletsMgr *tablet_mgr_ptr_;

  DupTableInterfaceStat *interface_stat_ptr_;

  DupTabletSetIDArray logging_tablet_set_ids_;
  DupTableLeaseItemArray logging_lease_addrs_;

  DupTableStatLog stat_log_;

  share::SCN first_part_scn_;

  share::SCN logging_scn_;
  palf::LSN logging_lsn_;

  // LogScnArray durable_block_scn_arr_;

  int64_t last_block_submit_us_;
  int64_t last_block_sync_us_;
  int64_t last_entry_submit_us_;
  int64_t last_entry_sync_us_;

  uint64_t total_cb_wait_time_;
  uint64_t append_block_count_;
  uint64_t log_entry_count_;
  uint64_t total_log_entry_wait_time_;
};
/*******************************************************
 *  Dup_Table Msg
 *******************************************************/

class ObDupTableMsgBase
{
  OB_UNIS_VERSION(1);

public:
  ObDupTableMsgBase() { reset(); }
  virtual ~ObDupTableMsgBase() {}
  void reset();

  void
  set_header(const ObAddr &src, const ObAddr &dst, const ObAddr &proxy, const share::ObLSID &ls_id);
  const ObAddr &get_src() const { return src_; }
  const ObAddr &get_dst() const { return dst_; }
  const ObAddr &get_proxy() const { return proxy_; }
  const share::ObLSID &get_ls_id() const { return ls_id_; }

  bool is_valid() const
  {
    return src_.is_valid() && dst_.is_valid() && proxy_.is_valid() && ls_id_.is_valid();
  }
  TO_STRING_KV(K_(src), K_(dst), K_(proxy), K(ls_id_));

protected:
  ObAddr src_;
  ObAddr dst_;
  ObAddr proxy_;
  share::ObLSID ls_id_;
};

class ObDupTableTsSyncRequest : public ObDupTableMsgBase
{
  OB_UNIS_VERSION(1);

public:
  ObDupTableTsSyncRequest() { reset(); }
  ObDupTableTsSyncRequest(const share::SCN &commit_scn) { max_commit_scn_ = commit_scn; }

  void reset()
  {
    ObDupTableMsgBase::reset();
    max_commit_scn_.reset();
  }

  bool is_valid() const { return ObDupTableMsgBase::is_valid() && max_commit_scn_.is_valid(); }

  const share::SCN &get_max_commit_scn() const { return max_commit_scn_; }
  INHERIT_TO_STRING_KV("ObDupTableMsgBase", ObDupTableMsgBase, K(max_commit_scn_));

private:
  share::SCN max_commit_scn_;
};

class ObDupTableTsSyncResponse : public ObDupTableMsgBase
{
  OB_UNIS_VERSION(1);

public:
  ObDupTableTsSyncResponse() { reset(); }
  ObDupTableTsSyncResponse(const share::SCN &replay_ts,
                           const share::SCN &commit_ts,
                           const share::SCN &read_ts)
  {
    max_replayed_scn_ = replay_ts;
    max_commit_scn_ = commit_ts;
    max_read_scn_ = read_ts;
  }

  void reset()
  {
    ObDupTableMsgBase::reset();
    max_replayed_scn_.reset();
    max_commit_scn_.reset();
    max_read_scn_.reset();
  }

  bool is_valid() const
  {
    return ObDupTableMsgBase::is_valid() && max_replayed_scn_.is_valid()
           && max_commit_scn_.is_valid() && max_read_scn_.is_valid();
  }

  const share::SCN &get_max_replayed_scn() const { return max_replayed_scn_; }
  const share::SCN &get_max_commit_scn() const { return max_commit_scn_; }
  const share::SCN &get_max_read_scn() const { return max_read_scn_; }
  INHERIT_TO_STRING_KV("ObDupTableMsgBase",
                       ObDupTableMsgBase,
                       K(max_replayed_scn_),
                       K(max_commit_scn_),
                       K(max_read_scn_));

private:
  share::SCN max_replayed_scn_;
  share::SCN max_commit_scn_;
  share::SCN max_read_scn_;
};

class ObDupTableLeaseRequest : public ObDupTableTsSyncResponse
{
  OB_UNIS_VERSION(1);

public:
  ObDupTableLeaseRequest() { reset(); }
  ObDupTableLeaseRequest(const share::SCN &replay_ts,
                         const share::SCN &commit_ts,
                         const share::SCN &read_ts,
                         const int64_t &request_ts,
                         const int64_t &lease_interval_us)
      : ObDupTableTsSyncResponse(replay_ts, commit_ts, read_ts), request_ts_(request_ts),
        lease_interval_us_(lease_interval_us){};

  void reset()
  {
    ObDupTableTsSyncResponse::reset();
    request_ts_ = lease_interval_us_ = OB_INVALID_TIMESTAMP;
  };

  bool is_valid() const
  {
    return ObDupTableTsSyncResponse::is_valid() && request_ts_ > 0 && lease_interval_us_ > 0;
  }

  int64_t get_request_ts() const { return request_ts_; }
  int64_t get_lease_interval_us() const { return lease_interval_us_; };

  INHERIT_TO_STRING_KV("ObDupTableTsSyncResponse",
                       ObDupTableTsSyncResponse,
                       K(request_ts_),
                       K(lease_interval_us_));

private:
  int64_t request_ts_;
  int64_t lease_interval_us_;
};

class ObDupTableBeforePrepareRequest : public ObDupTableMsgBase
{
  OB_UNIS_VERSION(1);

public:
  enum class BeforePrepareScnSrc : int64_t
  {
    UNKNOWN = -1,
    REDO_COMPLETE_SCN = 1,
    GTS = 2,
    MAX_DECIDED_SCN = 3
  };

public:
  ObDupTableBeforePrepareRequest() { reset(); }
  ObDupTableBeforePrepareRequest(const ObTransID &tx_id,
                                 const share::SCN &before_prepare_version,
                                 const BeforePrepareScnSrc &before_prepare_src)
      : tx_id_(tx_id), before_prepare_version_(before_prepare_version),
        before_prepare_scn_src_(before_prepare_src)
  {}

  void reset()
  {
    ObDupTableMsgBase::reset();
    tx_id_.reset();
    before_prepare_version_.reset();
    before_prepare_scn_src_ = BeforePrepareScnSrc::UNKNOWN;
  }

  bool is_valid() const
  {
    return ObDupTableMsgBase::is_valid() && tx_id_.is_valid() && before_prepare_version_.is_valid();
  }

  const ObTransID &get_tx_id() const { return tx_id_; }
  const share::SCN &get_before_prepare_version() const { return before_prepare_version_; }
  const BeforePrepareScnSrc &get_before_prepare_scn_src() const { return before_prepare_scn_src_; }

  INHERIT_TO_STRING_KV("ObDupTableMsgBase",
                       ObDupTableMsgBase,
                       K(tx_id_),
                       K(before_prepare_version_),
                       K(before_prepare_scn_src_));

private:
  ObTransID tx_id_;
  share::SCN before_prepare_version_;
  BeforePrepareScnSrc before_prepare_scn_src_;
};

} // namespace transaction

/*******************************************************
 *  Dup_Table RPC
 *******************************************************/
namespace obrpc
{
class ObDupTableProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObDupTableProxy);

  RPC_AP(PRZ post_msg, OB_DUP_TABLE_LEASE_REQUEST, (transaction::ObDupTableLeaseRequest));

  RPC_AP(PR3 post_msg, OB_DUP_TABLE_TS_SYNC_REQUEST, (transaction::ObDupTableTsSyncRequest));

  RPC_AP(PR3 post_msg, OB_DUP_TABLE_TS_SYNC_RESPONSE, (transaction::ObDupTableTsSyncResponse));

  RPC_AP(PR3 post_msg,
         OB_DUP_TABLE_BEFORE_PREPARE_REQUEST,
         (transaction::ObDupTableBeforePrepareRequest));
};

class ObDupTableLeaseRequestP
    : public ObRpcProcessor<obrpc::ObDupTableProxy::ObRpc<OB_DUP_TABLE_LEASE_REQUEST>>
{
public:
  explicit ObDupTableLeaseRequestP() {}

protected:
  int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObDupTableLeaseRequestP);

private:
};

class ObDupTableTsSyncRequestP
    : public ObRpcProcessor<obrpc::ObDupTableProxy::ObRpc<OB_DUP_TABLE_TS_SYNC_REQUEST>>
{
public:
  explicit ObDupTableTsSyncRequestP() {}

protected:
  int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObDupTableTsSyncRequestP);
};

class ObDupTableTsSyncResponseP
    : public ObRpcProcessor<obrpc::ObDupTableProxy::ObRpc<OB_DUP_TABLE_TS_SYNC_RESPONSE>>
{
public:
  explicit ObDupTableTsSyncResponseP() {}

protected:
  int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObDupTableTsSyncResponseP);
};

class ObDupTableBeforePrepareRequestP
    : public ObRpcProcessor<obrpc::ObDupTableProxy::ObRpc<OB_DUP_TABLE_BEFORE_PREPARE_REQUEST>>
{
public:
  explicit ObDupTableBeforePrepareRequestP() {}

protected:
  int process();

private:
  DISALLOW_COPY_AND_ASSIGN(ObDupTableBeforePrepareRequestP);
};

} // namespace obrpc

namespace transaction
{

class ObDupTableRpc
{
public:
  int init(rpc::frame::ObReqTransport *req_transport, const oceanbase::common::ObAddr &addr);

  template <typename DupTableMsgType>
  int post_msg(const common::ObAddr dst, DupTableMsgType &msg);

private:
  obrpc::ObDupTableProxy proxy_;
};

template <typename DupTableMsgType>
int ObDupTableRpc::post_msg(const common::ObAddr dst, DupTableMsgType &msg)
{
  int ret = OB_SUCCESS;
  if (!dst.is_valid() || !msg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    DUP_TABLE_LOG(WARN, "invalid msg or addr", K(dst), K(msg));
  } else if (OB_FAIL(proxy_.to(dst).by(MTL_ID()).post_msg(msg, nullptr))) {
    DUP_TABLE_LOG(WARN, "post msg error", K(ret), K(msg));
  }
  return ret;
}

class ObTxRedoSyncRetryTask : public ObTransTask
{
public:
  struct RedoSyncKey
  {
    share::ObLSID ls_id_;
    ObTransID tx_id_;

    int hash(uint64_t& res) const { res = ls_id_.hash() + tx_id_.hash(); return OB_SUCCESS; }
    uint64_t hash() const { return ls_id_.hash() + tx_id_.hash(); }
    bool operator==(const RedoSyncKey &other) const
    {
      return ls_id_ == other.ls_id_ && tx_id_ == other.tx_id_;
    }
    TO_STRING_KV(K(ls_id_), K(tx_id_));
  };

  typedef common::hash::ObHashSet<RedoSyncKey, common::hash::SpinReadWriteDefendMode>
      RedoSyncRetrySet;

class ObTxRedoSyncIterFunc
{
public:
  ObTxRedoSyncIterFunc(RedoSyncRetrySet &redo_sync_set, TransModulePageAllocator &trans_page_alloc)
      : del_list_(trans_page_alloc), ls_handle_(), redo_sync_retry_set_(redo_sync_set)
  {}
  int operator()(common::hash::HashSetTypes<RedoSyncKey>::pair_type &redo_sync_hash_pair);

  void remove_unused_redo_sync_key();

  void reset()
  {
    del_list_.destroy();
    ls_handle_.reset();
  }

private:
  ObList<RedoSyncKey, TransModulePageAllocator> del_list_;
  ObLSHandle ls_handle_;
  RedoSyncRetrySet &redo_sync_retry_set_;
};

public:
  ObTxRedoSyncRetryTask() : ObTransTask(ObTransRetryTaskType::DUP_TABLE_TX_REDO_SYNC_RETRY_TASK)
  {
    reset();
  }
  ~ObTxRedoSyncRetryTask() { destroy(); }
  void reset()
  {
    redo_sync_retry_set_.destroy();
    in_thread_pool_ = false;
  }
  void destroy() { reset(); }
  int init();
  int iter_tx_retry_redo_sync();
  int push_back_redo_sync_object(ObTransID tx_id, share::ObLSID ls_id);
  void clear_in_thread_pool_flag() { ATOMIC_STORE(&in_thread_pool_, false);}

public:
  RedoSyncRetrySet redo_sync_retry_set_;
  bool in_thread_pool_;
};

} // namespace transaction

} // namespace oceanbase
#endif
