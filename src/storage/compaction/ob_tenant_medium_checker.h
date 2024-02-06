/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SRC_STORAGE_COMPACTION_OB_TENANT_MEDIUM_CHECKER_H_
#define SRC_STORAGE_COMPACTION_OB_TENANT_MEDIUM_CHECKER_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "share/tablet/ob_tablet_info.h"
#include "share/compaction/ob_compaction_locality_cache.h"
#include "share/ob_occam_time_guard.h"
#include "storage/ob_i_store.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/compaction/ob_medium_compaction_mgr.h"

namespace oceanbase
{
namespace compaction
{
class ObTabletCheckInfo
{
public:
  ObTabletCheckInfo()
    : tablet_id_(),
      ls_id_(),
      check_medium_scn_(0)
  {}

  ObTabletCheckInfo(const common::ObTabletID &tablet_id, const share::ObLSID &ls_id, int64_t medium_scn)
    : tablet_id_(tablet_id),
      ls_id_(ls_id),
      check_medium_scn_(medium_scn)
  {}

  ~ObTabletCheckInfo() {}
  bool is_valid() const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  const ObTabletID &get_tablet_id() const { return tablet_id_; }
  const share::ObLSID &get_ls_id() const { return ls_id_; }
  int64_t get_medium_scn() const { return check_medium_scn_; }
  bool operator==(const ObTabletCheckInfo &other) const;
  TO_STRING_KV(K_(tablet_id), K_(ls_id), K_(check_medium_scn));

private:
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
  int64_t check_medium_scn_;
};

struct ObBatchFinishCheckStat
{
public:
  ObBatchFinishCheckStat()
    : succ_cnt_(0),
      finish_cnt_(0),
      fail_cnt_(0),
      filter_cnt_(0),
      failed_info_()
  {}
  ~ObBatchFinishCheckStat() {}
  DECLARE_TO_STRING;
  int64_t succ_cnt_;
  int64_t finish_cnt_;
  int64_t fail_cnt_;
  int64_t filter_cnt_;
  ObTabletCheckInfo failed_info_; // remain only one
};

class ObTenantMediumChecker
{
public:
  static int mtl_init(ObTenantMediumChecker *&tablet_medium_checker);
  ObTenantMediumChecker();
  virtual ~ObTenantMediumChecker();
  int init();
  void destroy();
  int check_medium_finish_schedule();
  int check_medium_finish(
      const ObIArray<ObTabletCheckInfo> &tablet_ls_infos,
      int64_t start_idx,
      int64_t end_idx,
      ObIArray<ObTabletCheckInfo> &check_tablet_ls_infos,
      ObIArray<ObTabletCheckInfo> &finish_tablet_ls_infos,
      ObBatchFinishCheckStat &stat);
  int add_tablet_ls(const ObTabletID &tablet_id, const share::ObLSID &ls_id, const int64_t medium_scn);
  bool locality_cache_empty();
  TO_STRING_KV(K_(is_inited), K_(ls_locality_cache));

private:
  int reput_check_info(ObIArray<ObTabletCheckInfo> &tablet_ls_infos);
  int check_ls_status(const share::ObLSID &ls_id, bool &is_leader, bool need_check);
  int refresh_ls_status();

public:
  static const int64_t LS_ID_ARRAY_CNT = 10;
#ifdef ERRSIM
  static const int64_t CHECK_LS_LOCALITY_INTERVAL = 30 * 1000 * 1000L; // 30s
#else
  static const int64_t CHECK_LS_LOCALITY_INTERVAL = 5 * 60 * 1000 * 1000L; // 5m
#endif
  static const int64_t DEFAULT_MAP_BUCKET = 1024;
  static const int64_t MAX_BATCH_CHECK_NUM = 3000;
  typedef common::ObArray<ObTabletCheckInfo> TabletLSArray;
  typedef hash::ObHashSet<ObTabletCheckInfo, hash::NoPthreadDefendMode> TabletLSSet;
  typedef hash::ObHashMap<share::ObLSID, share::ObLSInfo> LSInfoMap;
private:
  bool is_inited_;
  int64_t last_check_timestamp_;
  share::ObCompactionLocalityCache ls_locality_cache_;
  TabletLSSet tablet_ls_set_;
  LSInfoMap ls_info_map_; // ls leader
  lib::ObMutex lock_;
};

}
}
#endif