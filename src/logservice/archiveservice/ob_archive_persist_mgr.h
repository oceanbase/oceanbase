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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_PERSIST_INFO_MGR_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_PERSIST_INFO_MGR_H_

#include "lib/utility/ob_print_utils.h"     // print
#include "lib/hash/ob_link_hashmap.h"           // ObLinkHashMap
#include "lib/lock/ob_spin_rwlock.h"           // SpinRWLock
#include "share/ob_ls_id.h"   // ObLSID
#include "share/backup/ob_archive_struct.h"    // ObArchiveRoundState
#include "share/backup/ob_archive_struct.h"        // ObLSArchivePersistInfo
#include "share/backup/ob_archive_persist_helper.h"
#include "ob_archive_define.h"                      // ArchiveKey

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObSqlString;
}

namespace share
{
class ObTenantArchiveRoundAttr;
class ObLSID;
class ObLSArchivePersistInfo;
class SCN;
}

namespace storage
{
class ObLSService;
}

namespace palf
{
struct LSN;
}

namespace archive
{
class ObLSArchiveTask;
class ObArchiveRoundMgr;
class ObArchiveLSMgr;
using oceanbase::share::ObLSID;
using oceanbase::palf::LSN;
using oceanbase::storage::ObLSService;
using oceanbase::share::ObArchiveRoundState;
using oceanbase::share::ObTenantArchiveRoundAttr;
using oceanbase::share::ObLSArchivePersistInfo;
using oceanbase::share::ObArchivePersistHelper;

typedef common::LinkHashValue<ObLSID> ArchiveProValue;
struct ObArchivePersistValue : public ArchiveProValue
{
  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard  RLockGuard;
  typedef common::SpinWLockGuard  WLockGuard;
  ObArchivePersistValue() : info_(), last_update_ts_(OB_INVALID_TIMESTAMP), speed_(0) {}

  void get(bool &is_madatory, int64_t &speed, ObLSArchivePersistInfo &info);
  int set(const bool is_madatory, const ObLSArchivePersistInfo &info);

  ObLSArchivePersistInfo info_;
  int64_t last_update_ts_;
  int64_t speed_;              // Bytes/s
  bool is_madatory_;
  RWLock rwlock_;
};

typedef common::ObLinkHashMap<ObLSID, ObArchivePersistValue> LSArchiveProMap;
class ObArchivePersistMgr
{
public:
  ObArchivePersistMgr();
  ~ObArchivePersistMgr();
  int init(const uint64_t tenant_id,
      common::ObMySQLProxy *proxy,
      ObLSService *ls_svr,
      ObArchiveLSMgr *ls_mgr,
      ObArchiveRoundMgr *round_mgr);

  void destroy();

public:
  // 获取日志流归档持久化信息 for inner
  int get_archive_persist_info(const ObLSID &id,
      const ArchiveKey &key,
      ObLSArchivePersistInfo &info);

  // 实时查询租户是否在归档模式
  int check_tenant_in_archive(bool &in_archive);

  // 检查缓存归档进度信息
  int check_and_get_piece_persist_continuous(const ObLSID &id,
      ObLSArchivePersistInfo &info);

  // 获取租户归档配置信息 for inner
  int load_archive_round_attr(ObTenantArchiveRoundAttr &attr);

  // 获取日志流归档进度 for global
  int get_ls_archive_progress(const ObLSID &id, LSN &lsn, share::SCN &scn, bool &force, bool &ignore);

  int get_ls_archive_speed(const ObLSID &id, int64_t &speed, bool &force, bool &ignore);

  // 持久化以及加载本地归档进度cache接口
  void persist_and_load();

  // 获取日志流创建时间戳
  int get_ls_create_scn(const share::ObLSID &id, share::SCN &scn);

private:
  // 1. 持久化当前server做归档日志流归档进度
  //
  // base: 当前轮次需要持久化状态, 不需要的条件包括
  //       1. observer轮次与rs不同
  //       2. rs当前轮次状态为STOP
  //
  // what: 需要为哪些日志流需要持久化归档进度?
  //       需要为当前由该server做归档的日志流持久化归档进度
  //
  // how:  归档进度来源于归档模块ls_mgr_, 当存在该日志流则持久化, 没有则跳过
  //
  // when: 于本地缓存归档进度比较, 当本地进度与ls_mgr_值相同, 说明进度没有变化, 不需要持久化
  //
  // other: STOPPING/STOP状态, ls_mgr_日志流被日志流释放, 需要设置归档进度状态为STOP
  int persist_archive_progress_();

  // 2. 刷新本server全部日志流归档进度
  //
  // what: 哪些日志流需要加载归档进度到本地cache? 本地server有的全部日志流
  //
  // when: 当归档开启或者归档处于STOP但是本地cache的归档进度仍为DOING
  int load_archive_progress_(const ArchiveKey &key);

  // 3. 清理本server已经不存在日志流归档进度记录
  int clear_stale_ls_();

  bool state_in_archive_(const share::ObArchiveRoundState &state) const;
  int load_ls_archive_progress_(const ObLSID &id,
      const ArchiveKey &key,
      ObLSArchivePersistInfo &info,
      bool &record_exist);

  int load_dest_mode_(bool &is_madatory);
  int update_local_archive_progress_(const ObLSID &id, const bool is_madatory, const ObLSArchivePersistInfo &info);

  int check_round_state_if_do_persist_(const ObTenantArchiveRoundAttr &attr,
      ArchiveKey &key,
      ObArchiveRoundState &state,
      bool &need_do);

  bool check_persist_authority_(const ObLSID &id) const;

  int build_ls_archive_info_(const ObLSID &id,
      const ArchiveKey &key,
      ObLSArchivePersistInfo &info,
      bool &exist);

  int check_ls_archive_progress_advance_(const ObLSID &id,
      const ObLSArchivePersistInfo &info,
      bool &advanced);

  int do_persist_(const ObLSID &id, const bool exist, ObLSArchivePersistInfo &info);
  int do_wipe_suspend_status_(const ObLSID &id);

  bool need_persist_(const ObArchiveRoundState &state) const;
  bool need_stop_status_(const ObArchiveRoundState &state) const;
  bool need_suspend_status_(const ObArchiveRoundState &state) const;
  bool need_wipe_suspend_status_(const ObArchiveRoundState &state) const;

private:
  static const int64_t PRINT_INTERVAL = 1 * 1000 * 1000L;

private:
  class DeleteStaleLSFunctor;
private:
  typedef common::SpinRWLock RWLock;
  typedef common::SpinRLockGuard  RLockGuard;
  typedef common::SpinWLockGuard  WLockGuard;
private:
  bool inited_;
  uint64_t tenant_id_;
  ArchiveKey tenant_key_;
  int64_t dest_no_;
  ObArchiveRoundState state_;
  mutable RWLock state_rwlock_;
  common::ObMySQLProxy *proxy_;
  ObLSService *ls_svr_;
  ObArchiveLSMgr *ls_mgr_;
  ObArchiveRoundMgr *round_mgr_;
  int64_t last_update_ts_;
  ObArchivePersistHelper table_operator_;
  LSArchiveProMap map_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObArchivePersistMgr);
};

} // namespace archive
} // namespace oceanbase

#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_PERSIST_INFO_MGR_H_ */
