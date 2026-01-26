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
#include "lib/literals/ob_literals.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "common/ob_tablet_id.h"
#include "storage/ls/ob_ls.h"
#include "storage/compaction/ob_tenant_medium_checker.h"
#include "storage/compaction/ob_window_compaction_utils.h"
#include "storage/compaction/ob_schedule_tablet_func.h"

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
class ObLS;
class ObLSHandle;
class ObTabletHandle;
class ObLSTabletService;
}
namespace compaction
{

class ObBasicMergeScheduleIterator
{
public:
  struct ObTabletArray
  {
    ObTabletArray();
    ~ObTabletArray() { reset(); }
    bool is_ls_iter_end() const
    {
      // not init means cur ls not iter before
      // when init, need check array
      return is_inited_ && (array_.empty() || tablet_idx_ >= array_.count());
    }
    void reset()
    {
      is_inited_ = false;
      array_.reuse();
      tablet_idx_ = 0;
    }
    void mark_inited()
    {
      is_inited_ = true;
      tablet_idx_ = 0;
    }
    int64_t count() const { return array_.count(); }
    int consume_tablet_id(ObTabletID &tablet_id);
    void to_string(char *buf, const int64_t buf_len, int64_t &pos) const;
    TO_STRING_KV(K_(tablet_idx), "tablet_cnt", count(), K_(array), K_(is_inited));
    static const int64_t TABLET_ID_ARRAY_CNT = 2000;
    int64_t tablet_idx_;
    // array may be empty after inited on SS
    common::ObSEArray<common::ObTabletID, TABLET_ID_ARRAY_CNT> array_;
    bool is_inited_;
  };
public:
  ObBasicMergeScheduleIterator();
  ~ObBasicMergeScheduleIterator() = default;
  int init(const int64_t schedule_batch_size);
  virtual int get_next_ls(storage::ObLSHandle &ls_handle);
  int get_next_tablet(storage::ObTabletHandle &tablet_handle);
  bool is_scan_finish() const { return scan_finish_; }
  bool tenant_merge_finish() const { return merge_finish_ & scan_finish_; }
  void update_merge_finish(const bool merge_finish) {
    merge_finish_ &= merge_finish;
  }
  void reset_basic_iter();
  bool is_valid() const;
  void skip_cur_ls()
  {
    ++ls_idx_;
    cur_ls_handle_.reset();
    tablet_ids_.reset();
  }
  void start_cur_batch()
  {
    schedule_tablet_cnt_ = 0;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;
protected:
  virtual int get_cur_ls_handle(storage::ObLSHandle &ls_handle) = 0;
  virtual int get_tablet_ids() = 0;
  virtual int get_tablet_handle(const ObTabletID &tablet_id, storage::ObTabletHandle &tablet_handle) = 0;
protected:
  static const int64_t LS_ID_ARRAY_CNT = 10;
  bool scan_finish_;
  bool merge_finish_;
  int64_t ls_idx_;
  int64_t schedule_tablet_cnt_;
  int64_t max_batch_tablet_cnt_;
  storage::ObLSHandle cur_ls_handle_;
  common::ObSEArray<share::ObLSID, LS_ID_ARRAY_CNT> ls_ids_;
  ObTabletArray tablet_ids_;
};


class ObCompactionScheduleIterator : public ObBasicMergeScheduleIterator
{
public:
  ObCompactionScheduleIterator(const bool is_major);
  ~ObCompactionScheduleIterator() { reset(); }
  int build_iter(const int64_t schedule_batch_size);
  void set_report_scn_flag() { report_scn_flag_ = true; }
  bool need_report_scn() const { return report_scn_flag_; }
  void reset();
protected:
  virtual int get_cur_ls_handle(storage::ObLSHandle &ls_handle) override;
  virtual int get_tablet_ids() override;
  virtual int get_tablet_handle(const ObTabletID &tablet_id, storage::ObTabletHandle &tablet_handle) override;
protected:
  static const int64_t CHECK_REPORT_SCN_INTERVAL = 5_min;
  bool is_major_;
  bool report_scn_flag_;
};

template<typename ItemType>
class ObLSSortedIterator
{
public:
  template<typename Processor, typename... Args>
  int iterate(
      ObScheduleTabletFunc &func,
      ObIArray<ItemType> &items,
      Processor &processor,
      const bool skip_follower,
      Args&&... args);
};

class LSIDExtractor
{
public:
  static int64_t get_ls_id(const ObTabletCheckInfo &item) { return item.get_ls_id().id(); }
  static int64_t get_ls_id(const storage::ObTabletStatAnalyzer &item) { return item.tablet_stat_.ls_id_; }
  static int64_t get_ls_id(ObTabletCompactionScore* const &item)  { return item->get_key().ls_id_.id(); }
};

class TabletIDExtractor
{
public:
  static int64_t get_tablet_id(const ObTabletCheckInfo &item) { return item.get_tablet_id().id(); }
  static int64_t get_tablet_id(const storage::ObTabletStatAnalyzer &item) { return item.tablet_stat_.tablet_id_; }
  static int64_t get_tablet_id(ObTabletCompactionScore* const &item)  { return item->get_key().tablet_id_.id(); }
};

template<typename ItemType>
template<typename Processor, typename... Args>
int ObLSSortedIterator<ItemType>::iterate(
    ObScheduleTabletFunc &func,
    ObIArray<ItemType> &items,
    Processor &processor,
    const bool skip_follower,
    Args&&... args)
{
  int ret = OB_SUCCESS;
  storage::ObLSHandle ls_handle;
  // TODO(chengkong): break loop when window compaction is stop
  for (int64_t i = 0; i < items.count(); i++) { // ignore OB_FAIL to iterate all items
    const ItemType &item = items.at(i);
    const int64_t ls_id = LSIDExtractor::get_ls_id(item);
    const int64_t tablet_id = TabletIDExtractor::get_tablet_id(item);
    ObTabletHandle tablet_handle;
    bool can_merge = false;
    if (func.get_ls_status().ls_id_.id() == ls_id) {
      // do nothing, use old ls_handle
    } else if (OB_FAIL(MTL(storage::ObLSService *)->get_ls(ObLSID(ls_id), ls_handle, storage::ObLSGetMod::COMPACT_MODE))) {
      if (OB_LS_NOT_EXIST == ret) {
        STORAGE_LOG(TRACE, "ls not exist, skip it", K(ret), K(ls_id));
      } else {
        STORAGE_LOG(WARN, "failed to get ls", K(ret), K(ls_id));
      }
    } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "ls handle is not valid", K(ret), K(ls_id));
    } else if (OB_FAIL(func.switch_ls(ls_handle))) {
      if (OB_STATE_NOT_MATCH != ret) {
        STORAGE_LOG(WARN, "failed to switch ls", K(ret), K(ls_id));
      } else {
        STORAGE_LOG(WARN, "not support schedule medium for ls", K(ret), K(ls_id), K(tablet_id), K(func));
      }
    } else if (func.is_window_compaction_func() && !func.is_window_compaction_active()) {
      STORAGE_LOG(INFO, "window compaction is not active, skip iterate", K(ret), K(ls_id), K(tablet_id), K(func));
      break;
    }

    // In window compaction, candidate in ready list should be removed if it's not leader, so don't need to skip follower
    if (OB_FAIL(ret)) {
    } else if (!func.get_ls_status().is_leader_ && skip_follower) {
      // not leader, can't schedule
      STORAGE_LOG(TRACE, "not ls leader, can't schedule medium", K(ret), K(ls_id), K(tablet_id), K(func));
    } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(
                 ObTabletID(tablet_id), tablet_handle, 0 /*timeout_us*/))) {
      STORAGE_LOG(WARN, "get tablet failed", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(func.iterate_switch_tablet(tablet_handle, can_merge))) {
      STORAGE_LOG(WARN, "failed to switch tablet", K(ret), K(ls_id), K(tablet_id));
    } else if (!can_merge && skip_follower) {
    } else if (OB_FAIL(processor(func, item, tablet_handle, std::forward<Args>(args)...))) {
      // ATTENTION:don't print item here, since it may be a pointer and be freed when process
      STORAGE_LOG(WARN, "failed to process item", K(ret), K(ls_id), K(tablet_id));
    }
    (void) func.destroy_tablet_status();
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_COMPACTION_SCHEDUL_ITERATOR_H_
