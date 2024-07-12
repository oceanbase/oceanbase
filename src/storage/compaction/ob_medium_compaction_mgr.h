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

#ifndef OB_STORAGE_COMPACTION_MEDIUM_COMPACTION_MGR_H_
#define OB_STORAGE_COMPACTION_MEDIUM_COMPACTION_MGR_H_

#include "storage/ob_storage_schema.h"
#include "storage/ob_storage_clog_recorder.h"
#include "lib/container/ob_array_array.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "storage/compaction/ob_extra_medium_info.h"

namespace oceanbase
{
namespace storage
{
class ObTablet;
namespace mds
{
class MdsCtx;
}
}
namespace compaction
{
class ObTabletMediumCompactionInfoRecorder : public storage::ObIStorageClogRecorder
{
public:
  ObTabletMediumCompactionInfoRecorder();
  ~ObTabletMediumCompactionInfoRecorder();
  int init(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t max_saved_version,
      logservice::ObLogHandler *log_handler);
  virtual void destroy() override;
  void reset();
  bool is_inited() const { return is_inited_; }
  // for leader
  int submit_medium_compaction_info(ObMediumCompactionInfo &medium_info, ObIAllocator &allocator);
  // follower
  int replay_medium_compaction_log(const share::SCN &scn, const char *buf, const int64_t size, int64_t &pos);
  static int64_t cal_buf_len(
    const common::ObTabletID &tablet_id,
    const ObMediumCompactionInfo &medium_info,
    const logservice::ObLogBaseHeader *log_header);
  INHERIT_TO_STRING_KV("ObIStorageClogRecorder", ObIStorageClogRecorder, K_(ignore_medium), K_(ls_id), K_(tablet_id));
private:
  virtual int inner_replay_clog(
      const int64_t update_version,
      const share::SCN &scn,
      const char *buf,
      const int64_t size,
      int64_t &pos) override;  virtual int sync_clog_succ_for_leader(const int64_t update_version) override;
  virtual void sync_clog_failed_for_leader() override;

  virtual int prepare_struct_in_lock(
      int64_t &update_version,
      ObIAllocator *allocator,
      char *&clog_buf,
      int64_t &clog_len) override;
  virtual int submit_log(
      const int64_t update_version,
      const char *clog_buf,
      const int64_t clog_len) override;
  virtual void free_struct_in_lock() override
  {
    free_allocated_info();
  }
  void free_allocated_info();
  OB_INLINE int submit_trans_on_mds_table(const bool is_commit);

private:
  bool is_inited_;
  bool ignore_medium_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  storage::ObTabletHandle *tablet_handle_ptr_;
  ObMediumCompactionInfo *medium_info_;
  common::ObIAllocator *allocator_;
  mds::MdsCtx *mds_ctx_;
};

class ObMediumCompactionInfoList final
{
public:
  ObMediumCompactionInfoList();
  ~ObMediumCompactionInfoList();

  typedef common::ObDList<ObMediumCompactionInfo> MediumInfoList;

  int init(common::ObIAllocator &allocator);

  int init(
      common::ObIAllocator &allocator,
      const ObMediumCompactionInfoList *input_list);

  int init(
      common::ObIAllocator &allocator,
      const ObExtraMediumInfo &extra_medium_info,
      const common::ObIArray<ObMediumCompactionInfo*> &medium_info_array);
  void reset();
  OB_INLINE bool is_empty() const { return 0 == medium_info_list_.get_size(); }
  OB_INLINE int64_t size() const { return medium_info_list_.get_size(); }

  OB_INLINE bool is_valid() const
  {
    return inner_is_valid();
  }
  OB_INLINE const MediumInfoList &get_list() const { return medium_info_list_; }
  OB_INLINE int64_t get_wait_check_medium_scn() const { return extra_info_.wait_check_flag_ ? extra_info_.last_medium_scn_ : 0; }
  bool need_check_finish() const;
  // check status on serialized medium list
  OB_INLINE bool could_schedule_next_round(const int64_t last_major_snapshot) const
  {
    bool exist = false;
    DLIST_FOREACH_NORET(info, get_list()) {
      if (info->medium_snapshot_ > last_major_snapshot) {
        exist = true;
        break;
      }
    }
    return !need_check_finish() && !exist;
  }
  OB_INLINE ObMediumCompactionInfo::ObCompactionType get_last_compaction_type() const
  {
    return static_cast<ObMediumCompactionInfo::ObCompactionType>(extra_info_.last_compaction_type_);
  }
  OB_INLINE int64_t get_last_compaction_scn() const
  {
    return extra_info_.last_medium_scn_;
  }
  const ObExtraMediumInfo& get_extra_medium_info() const
  {
    return extra_info_;
  }
  int get_max_sync_medium_scn(int64_t &max_received_medium_scn) const;

  // serialize & deserialize
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;

  void gene_info(char* buf, const int64_t buf_len, int64_t &pos) const;
  TO_STRING_KV(K_(is_inited), K_(extra_info), "list_size", size(), K_(medium_info_list));

private:
  void reset_list();
  OB_INLINE bool inner_is_valid() const
  {
    return extra_info_.last_compaction_type_ < ObMediumCompactionInfo::COMPACTION_TYPE_MAX
        && extra_info_.last_medium_scn_ >= 0 && size() >= 0;
  }

  OB_INLINE void set_basic_info(const ObMediumCompactionInfoList &input_list)
  {
    extra_info_ = input_list.extra_info_;
  }

private:
  bool is_inited_;
  common::ObIAllocator *allocator_;

  ObExtraMediumInfo extra_info_;

  MediumInfoList medium_info_list_; // need for compat, will not store any MediumCompactionInfo after 4.2
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_MEDIUM_COMPACTION_H_
