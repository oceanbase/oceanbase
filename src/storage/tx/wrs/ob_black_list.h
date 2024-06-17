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

#ifndef OCEANBASE_TRANSACTION_OB_BLACK_LIST_
#define OCEANBASE_TRANSACTION_OB_BLACK_LIST_

#include "common/ob_queue_thread.h"           // ObCond
#include "common/ob_role.h"                   // ObRole
#include "storage/tx/ob_trans_define.h"       // ObLSID, LinkHashNode
#include "storage/high_availability/ob_storage_ha_struct.h"        // ObMigrateStatus

// 定期更新黑名单的时间间隔(us)
#define BLACK_LIST_REFRESH_INTERVAL       3000000     // 3s
// 判断时间戳是否赶上/落后的缓冲时间(ns)，避免阈值附近的日志流反复加入/移出黑名单
#define BLACK_LIST_WHITEWASH_INTERVAL_NS  1000000000L  // 1s
// 黑名单信息打印时间间隔(us)
#define BLACK_LIST_PRINT_INTERVAL         5000000    // 5s
// 清理超时对象的时间间隔(us)，这些对象不会出现在 SQLResult 中，比如切换server之后旧server上的日志流
#define BLACK_LIST_CLEAN_UP_INTERVAL      5000000     // 5s
// 最大连续失败次数，连续刷新黑名单失败 达到 该次数则清空黑名单
#define BLACK_LIST_MAX_FAIL_COUNT         3
// 执行内部sql的超时时间，内部sql的hint不生效，需要在接口中指定超时时间
#define INNER_SQL_QUERY_TIMEOUT           2000000L     // 2s

// 查询 __all_virtual_ls_info 的语句，设置了2s超时时间
// select /*+query_timeout(2000000)*/ a.svr_ip, a.svr_port, a.tenant_id, a.ls_id, a.role, nvl(b.weak_read_scn, 1) as weak_read_scn, nvl(b.migrate_status, 0) as migrate_status, nvl(b.tx_blocked, 0) as tx_blocked from oceanbase.__all_virtual_ls_meta_table a left join oceanbase.__all_virtual_ls_info b on a.svr_ip = b.svr_ip and a.svr_port = b.svr_port and a.tenant_id = b.tenant_id and a.ls_id = b.ls_id;
#define BLACK_LIST_SELECT_LS_INFO_STMT \
  "select a.svr_ip, a.svr_port, a.tenant_id, a.ls_id, a.role, \
  nvl(b.weak_read_scn, 1) as weak_read_scn, nvl(b.migrate_status, 0) as migrate_status, nvl(b.tx_blocked, 0) as tx_blocked \
  from oceanbase.__all_virtual_ls_meta_table a left join oceanbase.__all_virtual_ls_info b \
  on a.svr_ip = b.svr_ip and a.svr_port = b.svr_port and a.tenant_id = b.tenant_id and a.ls_id = b.ls_id;"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace storage;

namespace transaction
{
// blacklist type
enum BLType
{
  BLTYPE_UNKNOWN=0,
  BLTYPE_SERVER,
  BLTYPE_LS,
  BLTYPE_MAX
};

// blacklist key
class ObBLKey
{
public:
  ObBLKey() : server_(), tenant_id_(OB_INVALID_TENANT_ID), ls_id_(ObLSID::INVALID_LS_ID) {}
  ~ObBLKey() { reset(); }
  int init(const ObAddr &server, const uint64_t tenant_id, const ObLSID &ls_id)
  {
    int ret = OB_SUCCESS;
    if (!server.is_valid() || OB_INVALID_TENANT_ID == tenant_id || !ls_id.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      server_ = server;
      tenant_id_ = tenant_id;
      ls_id_ = ls_id;
    }
    return ret;
  }
  void reset()
  {
    server_.reset();
    tenant_id_ = OB_INVALID_TENANT_ID;
    ls_id_ = ObLSID::INVALID_LS_ID;
  }
  uint64_t hash() const {
    uint64_t hash_val = 0;
    int64_t server_hash = server_.hash();
    uint64_t ls_hash = ls_id_.hash();
    hash_val = murmurhash(&server_hash, sizeof(int64_t), 0);
    hash_val = murmurhash(&tenant_id_, sizeof(uint64_t), hash_val);
    hash_val = murmurhash(&ls_hash, sizeof(uint64_t), hash_val);
    return hash_val;
  }
  int hash(uint64_t &hash_val) const {
    hash_val = hash();
    return OB_SUCCESS;
  }
  int compare(const ObBLKey &other) const
  {
    int ret = 0;
    uint64_t hash_1 = hash();
    uint64_t hash_2 = other.hash();
    if (hash_1 > hash_2) {
      ret = 1;
    } else if (hash_1 < hash_2) {
      ret = -1;
    }
    return ret;
  }
  uint64_t get_tenant_id() const { return tenant_id_; }
  const ObLSID &get_ls_id() const { return ls_id_; }
  // TODO: different keys return different types, default return BLTYPE_UNKNOWN
  BLType get_type() const { return BLType::BLTYPE_LS; }
  const ObAddr &get_server() const { return server_; }
  bool is_valid() const {
    return server_.is_valid() && OB_INVALID_TENANT_ID != tenant_id_ && ls_id_.is_valid();
  }

  TO_STRING_KV(K_(server), K_(tenant_id), K_(ls_id));

private:
  ObAddr server_;
  uint64_t tenant_id_;
  ObLSID ls_id_;
};

typedef LinkHashNode<ObBLKey> ObBlackListHashNode;
typedef LinkHashValue<ObBLKey> ObBlackListHashValue;

struct ObLsInfo
{
public:
  ObLsInfo()
    : ls_state_(-1),
      weak_read_scn_(0),
      migrate_status_(OB_MIGRATION_STATUS_MAX),
      tx_blocked_(false)
      {}
  int init(const int64_t ls_state, int64_t weak_read_scn, ObMigrationStatus migrate_status, bool tx_blocked)
  {
    int ret = OB_SUCCESS;
    if (OB_MIGRATION_STATUS_MAX == migrate_status) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ls_state_ = ls_state;
      weak_read_scn_ = weak_read_scn;
      migrate_status_ = migrate_status;
      tx_blocked_ = tx_blocked;
    }
    return ret;
  }
  bool is_leader() const { return ls_state_ == 1; }
  bool is_valid() const
  {
    return OB_MIGRATION_STATUS_MAX != migrate_status_;
  }
  TO_STRING_KV(K_(ls_state), K_(weak_read_scn), K_(migrate_status), K_(tx_blocked));

  // 日志流状态（角色）：LEADER(1)、FOLLOWER(2)，其他角色对于日志流是没有意义的
  int64_t ls_state_;
  // 弱读时间戳，如果落后超过一定时间就要加入黑名单，单位ns
  int64_t weak_read_scn_;
  // 迁移状态，正在迁移的日志流一定不可读
  ObMigrationStatus migrate_status_;
  // transaction ls blocked
  bool tx_blocked_;
};

// blacklist value
class ObBLValue : public ObBlackListHashValue
{
public:
  ObBLValue() : update_ts_(0) {}
  int init(const ObBLKey &key)
  {
    int ret = OB_SUCCESS;
    if (!key.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      key_ = key;
      update_ts_ = ObTimeUtility::current_time();
    }
    return ret;
  }
  int init(const ObBLKey &key, const ObLsInfo &ls_info)
  {
    int ret = OB_SUCCESS;
    if (!key.is_valid() || !ls_info.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      key_ = key;
      ls_info_ = ls_info;
      update_ts_ = ObTimeUtility::current_time();
    }
    return ret;
  }
  int update(const ObLsInfo &ls_info)
  {
    int ret = OB_SUCCESS;
    if (!ls_info.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ls_info_ = ls_info;
      update_ts_ = ObTimeUtility::current_time();
    }
    return ret;
  }
  int64_t get_update_ts()
  {
    return update_ts_;
  }
  TO_STRING_KV(K_(key), K_(ls_info), KTIME_(update_ts));

private:
  ObBLKey key_;
  // ls相关信息
  ObLsInfo ls_info_;
  // 当前对象最后一次init/update的时间(us)
  int64_t update_ts_;
};

class ObBlackListAlloc
{
public:
  ObBLValue *alloc_value()
  {
    return op_alloc(ObBLValue);
  }
  void free_value(ObBLValue *val)
  {
    if (NULL != val) {
      op_free(val);
    }
  }
  ObBlackListHashNode* alloc_node(ObBLValue *val)
  {
    UNUSED(val);
    return op_alloc(ObBlackListHashNode);
  }
  void free_node(ObBlackListHashNode *node)
  {
    if (NULL != node) {
      op_free(node);
    }
  }
};

// blacklist manager
template<typename BLKey, typename BLValue>
class ObBLMgr
{
public:
  ObBLMgr() {}
  ~ObBLMgr() { destroy(); }
  int init()
  {
    int ret = OB_SUCCESS;

    if (OB_FAIL(map_.init())) {
      TRANS_LOG(ERROR, "BLMgr init failed", KR(ret));
    }
    return ret;
  }
  void reset()
  {
    map_.reset();
  }
  void destroy()
  {
    map_.destroy();
  }
  // create and insert
  int add(const BLKey &bl_key)
  {
    int ret = OB_SUCCESS;
    BLValue *value = NULL;

    if (!bl_key.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "bl_key is invalid", KR(ret), K(bl_key));
    } else if (OB_FAIL(map_.create(bl_key, value))) {
      TRANS_LOG(WARN, "map create error", KR(ret), K(bl_key));
    } else {
      if (OB_FAIL(value->init(bl_key))) {
        TRANS_LOG(WARN, "value init error", KR(ret), K(bl_key), KPC(value));
        map_.del(bl_key);
      }
      map_.revert(value);
    }
    return ret;
  }
  // update or create
  int update(const BLKey &bl_key, const ObLsInfo &ls_info, bool only_update = false)
  {
    int ret = OB_SUCCESS;
    BLValue *value = NULL;

    if (!bl_key.is_valid() || !ls_info.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "bl_key is invalid", KR(ret), K(bl_key), K(ls_info));
    } else if (OB_FAIL(map_.get(bl_key, value)) && OB_ENTRY_NOT_EXIST != ret) {
      TRANS_LOG(WARN, "map get error", KR(ret), K(bl_key), K(ls_info));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      // key不存在，创建value并插入map
      if (only_update) {
        ret = OB_SUCCESS;
      } else if (OB_FAIL(map_.create(bl_key, value))) {
        // 可能前面get时还没有这个key，但是在create之前别的线程把这个key插入map了
        TRANS_LOG(WARN, "map create error", KR(ret), K(bl_key), K(ls_info));
      } else {
        if (OB_FAIL(value->init(bl_key, ls_info))) {
          TRANS_LOG(WARN, "value init error", KR(ret), KPC(value), K(bl_key), K(ls_info));
          map_.del(bl_key);
        }
        map_.revert(value);
      }
    // key已存在，直接更新value
    } else if (OB_FAIL(value->update(ls_info))) {
      // 只要get成功，就要减去value的引用计数
      map_.revert(value);
      TRANS_LOG(WARN, "value update error", KR(ret), KPC(value), K(bl_key), K(ls_info));
    } else {
      map_.revert(value);
    }
    return ret;
  }
  int remove(const BLKey &bl_key)
  {
    int ret = OB_SUCCESS;
    if (!bl_key.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
    } else if(OB_FAIL(map_.del(bl_key))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        TRANS_LOG(ERROR, "map remove fail ", KR(ret), K(bl_key));
      }
    }
    return ret;
  }
  int check_in_black_list(const BLKey &bl_key, bool &in_black_list) const
  {
    int ret = OB_SUCCESS;

    if (OB_UNLIKELY(!bl_key.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
    } else if (0 == map_.size()) {
      in_black_list = false;
    } else if (OB_ENTRY_EXIST == map_.contains_key(bl_key)) {
      in_black_list = true;
    } else {
      in_black_list = false;
    }
    return ret;
  }
  template <typename Function> int for_each(Function &fn)
  {
    return map_.for_each(fn);
  }

private:
  typedef ObLinkHashMap<BLKey, BLValue, ObBlackListAlloc, RefHandle, 128> BLHashMap;
  BLHashMap map_;
};

typedef ObBLMgr<ObBLKey, ObBLValue> ObLsBLMgr;

class ObBLService : public ObThreadPool
{
public:
  ObBLService() : is_inited_(false), is_running_(false), thread_cond_() {}
  ~ObBLService() { destroy(); }
  int init();
  void reset();
  void destroy();
  int start();
  void stop();
  void wait();

  int add(const ObBLKey &bl_key);
  void remove(const ObBLKey &bl_key);
  int check_in_black_list(const ObBLKey &bl_key, bool &in_black_list);

  void run1();
  TO_STRING_KV(K_(is_inited), K_(is_running));

public:
  static ObBLService &get_instance()
  {
    static ObBLService instance_;
    return instance_;
  }

private:
  void do_thread_task_(const int64_t begin_tstamp, int64_t &last_print_stat_ts, int64_t &last_clean_up_ts);
  int do_black_list_check_(sqlclient::ObMySQLResult *result);
  int do_clean_up_();
  int get_info_from_result_(sqlclient::ObMySQLResult &result, ObBLKey &bl_key, ObLsInfo &ls_info);
  int64_t get_tenant_max_stale_time_(uint64_t tenant_id);
  bool check_need_skip_leader_(const uint64_t tenant_id);
  void print_stat_();

private:
  bool is_inited_;
  bool is_running_;
  common::ObCond thread_cond_;
  ObLsBLMgr ls_bl_mgr_;
};

class ObBLPrintFunctor
{
public:
  explicit ObBLPrintFunctor() {}
  bool operator()(const ObBLKey &key, ObBLValue *value)
  {
    TRANS_LOG(INFO, "blacklist info print ", KPC(value));
    return true;
  }
};

template<typename BLMgr>
class ObBLCleanUpFunctor
{
public:
  explicit ObBLCleanUpFunctor(BLMgr &bl_mgr) : bl_mgr_(bl_mgr) {}
  bool operator()(const ObBLKey &key, ObBLValue *value)
  {
    if (ObTimeUtility::current_time() > value->get_update_ts() + BLACK_LIST_CLEAN_UP_INTERVAL) {
      bl_mgr_.remove(key);
    }
    return true;
  }
private:
  BLMgr &bl_mgr_;
};

} // transaction
} // oceanbase

#endif // end of OCEANBASE_TRANSACTION_OB_BLACK_LIST_
