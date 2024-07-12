//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_COMPACTION_SCHEDUL_ITERATOR_H_
#define OB_STORAGE_COMPACTION_COMPACTION_SCHEDUL_ITERATOR_H_
#include "storage/ls/ob_ls_get_mod.h"
#include "lib/container/ob_se_array.h"
namespace oceanbase
{
namespace share
{
class ObLSID;
}
namespace common
{
class ObTabletID;
}
namespace storage
{
class ObLSHandle;
class ObTabletHandle;
class ObLSTabletService;
}
namespace compaction
{
// record ls_id/tablet_id
class ObCompactionScheduleIterator
{
public:
  ObCompactionScheduleIterator(
    const bool is_major,
    storage::ObLSGetMod mod = storage::ObLSGetMod::COMPACT_MODE);
  ~ObCompactionScheduleIterator() { reset(); }
  int build_iter(const int64_t schedule_batch_size);
  int get_next_ls(ObLSHandle &ls_handle);
  int get_next_tablet(ObTabletHandle &tablet_handle);
  bool is_scan_finish() const { return scan_finish_; }
  bool tenant_merge_finish() const { return merge_finish_ & scan_finish_; }
  void update_merge_finish(const bool merge_finish) {
    merge_finish_ &= merge_finish;
  }
  void set_report_scn_flag() { report_scn_flag_ = true; }
  bool need_report_scn() const { return report_scn_flag_; }
  void reset();
  bool is_valid() const;
  void skip_cur_ls()
  {
    ++ls_idx_;
    tablet_idx_ = -1;
    ls_tablet_svr_ = nullptr;
    tablet_ids_.reuse();
  }
  void start_cur_batch()
  {
    schedule_tablet_cnt_ = 0;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  // virtual for unittest
  virtual int get_cur_ls_handle(ObLSHandle &ls_handle);
  virtual int get_tablet_ids();
  virtual int get_tablet_handle(const ObTabletID &tablet_id, ObTabletHandle &tablet_handle);

  static const int64_t LS_ID_ARRAY_CNT = 10;
  static const int64_t TABLET_ID_ARRAY_CNT = 2000;
  static const int64_t CHECK_REPORT_SCN_INTERVAL = 5 * 60 * 1000 * 1000L; // 600s

  ObLSGetMod mod_;
  bool is_major_;
  bool scan_finish_;
  bool merge_finish_;
  bool report_scn_flag_;
  int64_t ls_idx_;
  int64_t tablet_idx_;
  int64_t schedule_tablet_cnt_;
  int64_t max_batch_tablet_cnt_;
  storage::ObLSTabletService *ls_tablet_svr_;
  common::ObSEArray<share::ObLSID, LS_ID_ARRAY_CNT> ls_ids_;
  common::ObSEArray<ObTabletID, TABLET_ID_ARRAY_CNT> tablet_ids_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_COMPACTION_SCHEDUL_ITERATOR_H_
