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

namespace oceanbase
{
namespace storage
{
class ObTablet;
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
      const ObTabletID &tablet_id,
      const int64_t max_saved_version,
      logservice::ObLogHandler *log_handler);
  virtual void destroy() override;
  void reset();
  bool is_inited() const { return is_inited_; }
  // for leader
  int submit_medium_compaction_info(ObMediumCompactionInfo &medium_info, ObIAllocator &allocator);
  // follower
  int replay_medium_compaction_log(const share::SCN &scn, const char *buf, const int64_t size, int64_t &pos);

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
  OB_INLINE int dec_ref_on_memtable(const bool sync_finish);
private:
  bool is_inited_;
  bool ignore_medium_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  storage::ObTabletHandle *tablet_handle_ptr_;
  ObMediumCompactionInfo *medium_info_;
  common::ObIAllocator *allocator_;
};

class ObMediumCompactionInfoList
{
public:
  ObMediumCompactionInfoList();
  ~ObMediumCompactionInfoList();

  typedef memtable::ObIMultiSourceDataUnit BasicNode;
  typedef common::ObDList<BasicNode> MediumInfoList;

  int init(common::ObIAllocator &allocator);

  int init(common::ObIAllocator &allocator,
      const ObMediumCompactionInfoList *old_list,
      const ObMediumCompactionInfoList *dump_list = nullptr,
      const int64_t finish_medium_scn = 0,
      const ObMergeType merge_type = MERGE_TYPE_MAX);

  int init_after_check_finish(
      ObIAllocator &allocator,
      const ObMediumCompactionInfoList &old_list);

  void reset();
  OB_INLINE bool is_empty() const { return 0 == medium_info_list_.get_size(); }
  OB_INLINE int64_t size() const { return medium_info_list_.get_size(); }

  OB_INLINE bool is_valid() const
  {
    return is_inited_ && inner_is_valid();
  }

  int add_medium_compaction_info(const ObMediumCompactionInfo &input_info);

  OB_INLINE const MediumInfoList &get_list() const { return medium_info_list_; }
  OB_INLINE int64_t get_wait_check_medium_scn() const { return wait_check_flag_ ? last_medium_scn_ : 0; }
  OB_INLINE bool need_check_finish() const { return wait_check_flag_; }
  // check status on serialized medium list
  OB_INLINE bool could_schedule_next_round() const
  {
    return !wait_check_flag_ && medium_info_list_.is_empty();
  }
  OB_INLINE ObMediumCompactionInfo::ObCompactionType get_last_compaction_type() const
  {
    return (ObMediumCompactionInfo::ObCompactionType)last_compaction_type_;
  }
  void get_schedule_scn(
    const int64_t major_compaction_scn,
    int64_t &schedule_scn,
    ObMediumCompactionInfo::ObCompactionType &compaction_type) const;

  int get_specified_scn_info(
      const int64_t snapshot,
      const ObMediumCompactionInfo *&compaction_info) const;
  OB_INLINE int64_t get_max_medium_snapshot() const
  {
    return is_empty() ? 0 : static_cast<const ObMediumCompactionInfo *>(medium_info_list_.get_last())->medium_snapshot_;
  }
  OB_INLINE int64_t get_min_medium_snapshot() const
  {
    return is_empty() ? -1 : static_cast<const ObMediumCompactionInfo *>(medium_info_list_.get_first())->medium_snapshot_;
  }
  const ObMediumCompactionInfo *get_first_medium_info() const
  {
    return is_empty() ? nullptr : static_cast<const ObMediumCompactionInfo *>(medium_info_list_.get_first());
  }

  // serialize & deserialize
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;

  void gene_info(char* buf, const int64_t buf_len, int64_t &pos) const;

  TO_STRING_KV(K_(is_inited), K_(info), K_(last_compaction_type), K_(wait_check_flag), K_(last_medium_scn),
      "list_size", size(), K_(medium_info_list));

private:
  void reset_list();
  OB_INLINE bool inner_is_valid() const
  {
    return last_compaction_type_ < ObMediumCompactionInfo::COMPACTION_TYPE_MAX
        && last_medium_scn_ >= 0 && size() >= 0;
  }
  OB_INLINE int append_list_with_deep_copy(
      const int64_t finish_scn,
      const ObMediumCompactionInfoList &input_list)
  {
    int ret = OB_SUCCESS;
    DLIST_FOREACH_X(input_info, input_list.medium_info_list_, OB_SUCC(ret)) {
      const ObMediumCompactionInfo *medium_info = static_cast<const ObMediumCompactionInfo *>(input_info);
      if (medium_info->medium_snapshot_ > finish_scn) {
        ret = inner_deep_copy_node(*medium_info);
      }
    }
    return ret;
  }
  int inner_deep_copy_node(const ObMediumCompactionInfo &medium_info);
  OB_INLINE void set_basic_info(const ObMediumCompactionInfoList &input_list)
  {
    last_compaction_type_ = input_list.last_compaction_type_;
    last_medium_scn_ = input_list.last_medium_scn_;
    wait_check_flag_ = input_list.wait_check_flag_;
  }

private:
  static const int64_t MEDIUM_LIST_VERSION = 1;
  static const int64_t MAX_SERIALIZE_SIZE = 2;
  static const int32_t MEDIUM_LIST_INFO_RESERVED_BITS = 51;

private:
  bool is_inited_;
  common::ObIAllocator *allocator_;

  // need serialize
  union {
    uint64_t info_;
    struct {
      uint64_t compat_                  : 8;
      uint64_t last_compaction_type_    : 4; // check inner_table when last_compaction is major
      uint64_t wait_check_flag_         : 1; // true: need check finish, false: don't need check
      uint64_t reserved_                : MEDIUM_LIST_INFO_RESERVED_BITS;
    };
  };
  int64_t last_medium_scn_;

  MediumInfoList medium_info_list_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_MEDIUM_COMPACTION_H_
