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

#ifndef OCEANBASE_ROOTSERVER_FREEZE_OB_ZONE_MERGE_MANAGER_
#define OCEANBASE_ROOTSERVER_FREEZE_OB_ZONE_MERGE_MANAGER_

#include "share/ob_zone_merge_info.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/scn.h"

namespace oceanbase
{
namespace rootserver
{

class ObZoneMergeManagerBase
{
public:
  friend class FakeZoneMergeManager;
  ObZoneMergeManagerBase();
  virtual ~ObZoneMergeManagerBase() {}

  int init(const uint64_t tenant_id, common::ObMySQLProxy &proxy);
  virtual int reload();
  virtual int try_reload();
  void reset_merge_info();
  void reset_merge_info_without_lock();

  int get_zone_merge_info(share::ObZoneMergeInfo &info) const;
  int get_zone_merge_info(const common::ObZone &zone, share::ObZoneMergeInfo &info) const;
  int get_zone(common::ObIArray<common::ObZone> &zone_list) const;
  int get_snapshot(share::ObGlobalMergeInfo &global_info,
                   common::ObIArray<share::ObZoneMergeInfo> &info_array);
  int get_snapshot(share::ObGlobalMergeInfo &global_info);
  virtual int finish_all_zone_merge(const int64_t expected_epoch,
                                    const uint64_t &merged_scn_val);

  virtual int start_zone_merge(const common::ObZone &zone, const int64_t expected_epoch);
  virtual int finish_zone_merge(const common::ObZone &zone,
                                const int64_t expected_epoch,
                                const share::SCN &new_last_merged_scn,
                                const share::SCN &new_all_merged_scn);
  int suspend_merge(const int64_t expected_epoch);
  int resume_merge(const int64_t expected_epoch);
  int set_merge_error(const int64_t merge_error, const int64_t expected_epoch);

  int set_zone_merging(const common::ObZone &zone, const int64_t expected_epoch);
  int check_need_broadcast(const share::SCN &frozen_scn, bool &need_broadcast);
  int set_global_freeze_info(const share::SCN &frozen_scn, const int64_t expected_epoch);

  int get_global_broadcast_scn(share::SCN &global_broadcast_scn) const;
  int get_global_last_merged_scn(share::SCN &global_last_merged_scn) const;
  int get_global_merge_status(share::ObZoneMergeInfo::MergeStatus &global_merge_status) const;
  int get_global_last_merged_time(int64_t &global_last_merged_time) const;
  int get_global_merge_start_time(int64_t &global_merge_start_time) const;

  virtual int generate_next_global_broadcast_scn(const int64_t expected_epoch, share::SCN &next_scn);
  virtual int try_update_global_last_merged_scn(const int64_t expected_epoch);
  virtual int update_global_merge_info_after_merge(const int64_t expected_epoch);
  virtual int try_update_zone_merge_info(const int64_t expected_epoch);
  virtual int adjust_global_merge_info(const int64_t expected_epoch);

private:
  int check_valid(const common::ObZone &zone, int64_t &idx) const;
  int find_zone(const common::ObZone &zone, int64_t &idx) const;
  inline int check_inner_stat() const;
  int check_freeze_service_epoch(common::ObMySQLTransaction &trans, const int64_t expected_epoch);
  void handle_trans_stat(common::ObMySQLTransaction &trans, int &ret);

  int suspend_or_resume_zone_merge(const bool suspend, const int64_t expected_epoch);

  int get_tenant_zone_list(common::ObIArray<ObZone> &zone_list);
  int str2zone_list(const char *str, common::ObIArray<ObZone> &zone_list);
  int handle_zone_merge_info_to_delete(common::ObMySQLTransaction &trans,
                                       const common::ObIArray<share::ObZoneMergeInfo> &ori_merge_infos,
                                       const common::ObIArray<common::ObZone> &zone_list,
                                       common::ObIArray<common::ObZone> &to_delete_infos);
  int handle_zone_merge_info_to_insert(common::ObMySQLTransaction &trans,
                                       const common::ObIArray<share::ObZoneMergeInfo> &ori_merge_infos,
                                       const common::ObIArray<common::ObZone> &zone_list,
                                       common::ObIArray<share::ObZoneMergeInfo> &to_insert_infos);
  int inner_adjust_global_merge_info(const share::SCN &frozen_scn,
                                     const int64_t expected_epoch);

protected:
  common::SpinRWLock lock_;
  static int copy_infos(ObZoneMergeManagerBase &dest, const ObZoneMergeManagerBase &src);

private:
  bool is_inited_;
  bool is_loaded_;
  uint64_t tenant_id_;
  int64_t zone_count_;
  share::ObZoneMergeInfo zone_merge_infos_[common::MAX_ZONE_NUM];
  share::ObGlobalMergeInfo global_merge_info_;
  common::ObMySQLProxy *proxy_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObZoneMergeManagerBase);
};

// destruct shadow_copy_guard before return
// otherwise the ret_ in shadow_copy_guard will never be returned
#define ZONE_MERGE_MANAGER_FUNC(func_name)                                     \
  template <typename... Args> int func_name(Args &&...args) {                  \
    int ret = OB_SUCCESS;                                                      \
    SpinWLockGuard guard(write_lock_);                                         \
    {                                                                          \
      ObZoneMergeMgrGuard shadow_guard(                                        \
          lock_, *(static_cast<ObZoneMergeManagerBase *>(this)), shadow_,      \
          ret);                                                                \
      if (OB_SUCC(ret)) {                                                      \
        ret = shadow_.func_name(std::forward<Args>(args)...);                  \
      }                                                                        \
    }                                                                          \
    return ret;                                                                \
  }

class ObZoneMergeManager : public ObZoneMergeManagerBase
{
public:
  ObZoneMergeManager();
  virtual ~ObZoneMergeManager();

  int init(const uint64_t tenant_id, common::ObMySQLProxy &proxy);
  ZONE_MERGE_MANAGER_FUNC(reload);
  ZONE_MERGE_MANAGER_FUNC(try_reload);
  ZONE_MERGE_MANAGER_FUNC(start_zone_merge);
  ZONE_MERGE_MANAGER_FUNC(finish_zone_merge);
  ZONE_MERGE_MANAGER_FUNC(finish_all_zone_merge);
  ZONE_MERGE_MANAGER_FUNC(suspend_merge);
  ZONE_MERGE_MANAGER_FUNC(resume_merge);
  ZONE_MERGE_MANAGER_FUNC(set_merge_error);
  ZONE_MERGE_MANAGER_FUNC(set_zone_merging);
  ZONE_MERGE_MANAGER_FUNC(check_need_broadcast);
  ZONE_MERGE_MANAGER_FUNC(set_global_freeze_info);
  ZONE_MERGE_MANAGER_FUNC(generate_next_global_broadcast_scn);
  ZONE_MERGE_MANAGER_FUNC(try_update_global_last_merged_scn);
  ZONE_MERGE_MANAGER_FUNC(update_global_merge_info_after_merge);
  ZONE_MERGE_MANAGER_FUNC(try_update_zone_merge_info);
  ZONE_MERGE_MANAGER_FUNC(adjust_global_merge_info);
public:
  class ObZoneMergeMgrGuard
  {
  public:
    ObZoneMergeMgrGuard(const common::SpinRWLock &lock,
                        ObZoneMergeManagerBase &zone_merge_mgr,
                        ObZoneMergeManagerBase &shadow,
                        int &ret);
    ~ObZoneMergeMgrGuard();

  private:
    common::SpinRWLock &lock_;
    ObZoneMergeManagerBase &zone_merge_mgr_;
    ObZoneMergeManagerBase &shadow_;
    int &ret_;
  private:
    DISALLOW_COPY_AND_ASSIGN(ObZoneMergeMgrGuard);
  };

private:
  common::SpinRWLock write_lock_;
  ObZoneMergeManagerBase shadow_;
  common::ObMySQLProxy illegal_proxy_;
};

} // end rootserver
} // end oceanbase

#endif  // OCEANBASE_ROOTSERVER_FREEZE_OB_ZONE_MERGE_MANAGER_
