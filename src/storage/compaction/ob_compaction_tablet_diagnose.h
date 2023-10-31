/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SRC_STORAGE_COMPACTION_OB_COMPACTION_TABLET_DIAGNOSE_H_
#define SRC_STORAGE_COMPACTION_OB_COMPACTION_TABLET_DIAGNOSE_H_

#include "share/scheduler/ob_diagnose_config.h"

namespace oceanbase
{
namespace compaction
{
struct ObDiagnoseTablet {
  ObDiagnoseTablet()
    : ls_id_(),
      tablet_id_()
  {}
  ObDiagnoseTablet(const share::ObLSID &ls_id,  const ObTabletID &tablet_id)
    : ls_id_(ls_id),
      tablet_id_(tablet_id)
  {}
  ~ObDiagnoseTablet() {}
  inline bool operator == (const ObDiagnoseTablet &other) const
  {
    return ls_id_ == other.ls_id_ && tablet_id_ == other.tablet_id_;
  }
  inline int hash(uint64_t &hash_value) const
  {
    int ret = common::OB_SUCCESS;
    hash_value = murmurhash(&ls_id_, sizeof(ls_id_), 0);
    hash_value = murmurhash(&tablet_id_, sizeof(tablet_id_), hash_value);
    return ret;
  }
  inline bool is_valid() const
  {
    return ls_id_.is_valid() && tablet_id_.is_valid();
  }
  static bool is_flagged(int64_t flag, const share::ObDiagnoseTabletType type)
  {
    return flag & (1 << static_cast<int64_t>(type));
  }
  static void set_flag(int64_t &flag, const share::ObDiagnoseTabletType type)
  {
    flag |= (1 << static_cast<int64_t>(type));
  }
  static void del_flag(int64_t &flag, const share::ObDiagnoseTabletType type)
  {
    flag &= ~(1 << static_cast<int64_t>(type));
  }
  // input_flag = 100100, other_flag = 011100 -> input_flag  = 000100
  static void sub_flag(int64_t &input_flag, const int64_t other_flag)
  {
    int64_t bits = 0;
    while (bits < share::ObDiagnoseTabletType::TYPE_DIAGNOSE_TABLET_MAX) {
      const int64_t flag = other_flag & (1 << bits++);
      input_flag &= ~flag;
    }
  }
  TO_STRING_KV(K_(ls_id), K_(tablet_id));

  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
};

class ObDiagnoseTabletMgr {
public:
  static int mtl_init(ObDiagnoseTabletMgr *&diagnose_tablet_mgr);
  ObDiagnoseTabletMgr();
  virtual ~ObDiagnoseTabletMgr() { destroy(); }

  int init();
  void destroy();

  // for diagnose
  int add_diagnose_tablet(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const share::ObDiagnoseTabletType type);
  int get_diagnose_tablets(ObIArray<ObDiagnoseTablet> &diagnose_tablets);
  int delete_diagnose_tablet(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const share::ObDiagnoseTabletType type);
  void remove_diagnose_tablets(
      ObIArray<ObDiagnoseTablet> &tablets);

public:
  static const int64_t DEFAULT_DIAGNOSE_TABLET_COUNT = 128;
  static const int64_t MAX_DIAGNOSE_TABLET_BUCKET_NUM = 1024;
  typedef common::hash::ObHashMap<ObDiagnoseTablet, int64_t, common::hash::NoPthreadDefendMode> DiagnoseTabletMap;

private:
  bool is_inited_;
  DiagnoseTabletMap diagnose_tablet_map_;
  lib::ObMutex diagnose_lock_;
};

}
}

#endif
