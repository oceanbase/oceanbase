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

namespace oceanbase
{
namespace storage
{
class ObTablet;
}
namespace compaction
{

struct ObParallelMergeInfo
{
public:
  ObParallelMergeInfo()
   : parallel_info_(0),
     parallel_end_key_list_(nullptr),
     allocator_(nullptr)
  {}
  ~ObParallelMergeInfo() { destroy(); } // attention!!! use destroy to free memory
  int init(common::ObIAllocator &allocator, const ObParallelMergeInfo &other);
  void destroy();
  bool is_valid() const
  {
    return list_size_ == 0 || nullptr != parallel_end_key_list_;
  }

  // serialize & deserialize
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;

  int generate_from_range_array(
      ObIAllocator &allocator,
      common::ObArrayArray<ObStoreRange> &paral_range);

  int64_t to_string(char* buf, const int64_t buf_len) const;
  static const int64_t MAX_PARALLEL_RANGE_SERIALIZE_LEN = 1 * 1024 * 1024;
  static const int64_t VALID_CONCURRENT_CNT = 1;

  union {
    uint32_t parallel_info_;
    struct {
      uint32_t compat_          : 4;
      uint32_t list_size_       : 8;
      uint32_t reserved_        : 20;
    };
  };
  ObStoreRowkey *parallel_end_key_list_; // concurrent_cnt - 1

  ObIAllocator *allocator_;
};

struct ObMediumCompactionInfo : public memtable::ObIMultiSourceDataUnit
{
public:
  enum ObCompactionType
  {
    MEDIUM_COMPACTION = 0,
    MAJOR_COMPACTION = 1,
    COMPACTION_TYPE_MAX,
  };
  const static char *ObCompactionTypeStr[];
  const static char *get_compaction_type_str(enum ObCompactionType type);
public:
  ObMediumCompactionInfo();
  ~ObMediumCompactionInfo();

  int init(ObIAllocator &allocator, const ObMediumCompactionInfo &medium_info);
  int save_storage_schema(ObIAllocator &allocator, const storage::ObStorageSchema &storage_schema);
  int gene_parallel_info(
      ObIAllocator &allocator,
      common::ObArrayArray<ObStoreRange> &paral_range);
  static inline bool is_medium_compaction(const ObCompactionType type) { return MEDIUM_COMPACTION == type; }
  static inline bool is_major_compaction(const ObCompactionType type) { return MAJOR_COMPACTION == type; }
  inline bool is_major_compaction() const { return is_major_compaction((ObCompactionType)compaction_type_); }
  inline bool is_medium_compaction() const { return is_medium_compaction((ObCompactionType)compaction_type_); }
  inline void clear_parallel_range()
  {
    parallel_merge_info_.list_size_ = 0;
    parallel_merge_info_.parallel_end_key_list_ = nullptr;
    contain_parallel_range_ = false;
  }

  // ObIMultiSourceDataUnit section
  virtual int deep_copy(const ObIMultiSourceDataUnit *src, ObIAllocator *allocator) override;
  virtual void reset() override;
  virtual bool is_valid() const override;
  virtual inline int64_t get_data_size() const override { return sizeof(ObMediumCompactionInfo); }
  virtual inline memtable::MultiSourceDataUnitType type() const override
  {
    return memtable::MultiSourceDataUnitType::MEDIUM_COMPACTION_INFO;
  }
  virtual int64_t get_version() const override { return medium_snapshot_; }
  virtual bool is_save_last() const override { return false; }
  bool from_cur_cluster() const { return cluster_id_ == GCONF.cluster_id; }

  // serialize & deserialize
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  int deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos);
  int64_t get_serialize_size() const;

  void gene_info(char* buf, const int64_t buf_len, int64_t &pos) const;
  TO_STRING_KV(K_(cluster_id), K_(medium_compat_version),
      "compaction_type", ObMediumCompactionInfo::get_compaction_type_str((ObCompactionType)compaction_type_),
      "medium_merge_reason", ObAdaptiveMergePolicy::merge_reason_to_str(medium_merge_reason_), K_(cluster_id),
      K_(medium_snapshot), K_(medium_scn), K_(storage_schema),
      K_(contain_parallel_range), K_(parallel_merge_info));
public:
  static const int64_t MEIDUM_COMPAT_VERSION = 1;

private:
  static const int32_t SCS_ONE_BIT = 1;
  static const int32_t SCS_RESERVED_BITS = 49;

public:
  union {
    uint64_t info_;
    struct {
      uint64_t medium_compat_version_           : 4;
      uint64_t compaction_type_                 : 2;
      uint64_t contain_parallel_range_          : SCS_ONE_BIT;
      uint64_t medium_merge_reason_             : 8;
      uint64_t reserved_                        : SCS_RESERVED_BITS;
    };
  };

  uint64_t cluster_id_; // for backup database to throw MEDIUM_COMPACTION clog
  int64_t medium_snapshot_;
  share::SCN medium_scn_; // for follower minor merge
  storage::ObStorageSchema storage_schema_;
  ObParallelMergeInfo parallel_merge_info_;
};

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
      const bool update_in_major_type_merge = false);

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
  OB_INLINE int64_t get_wait_check_medium_scn() const { return wait_check_medium_scn_; }
  OB_INLINE bool need_check_finish() const { return 0 != wait_check_medium_scn_; }
  // check status on serialized medium list
  OB_INLINE bool could_schedule_next_round() const
  {
    return 0 == wait_check_medium_scn_ && medium_info_list_.is_empty();
  }
  OB_INLINE ObMediumCompactionInfo::ObCompactionType get_last_compaction_type() const
  {
    return (ObMediumCompactionInfo::ObCompactionType)last_compaction_type_;
  }
  int64_t get_schedule_scn(const int64_t major_compaction_scn) const;

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

  TO_STRING_KV(K_(is_inited), K_(info), K_(last_compaction_type), K_(wait_check_medium_scn),
      "list_size", size(), K_(medium_info_list));

private:
  void reset_list();
  OB_INLINE bool inner_is_valid() const
  {
    return last_compaction_type_ < ObMediumCompactionInfo::COMPACTION_TYPE_MAX
        && wait_check_medium_scn_ >= 0 && size() >= 0;
  }
  OB_INLINE int append_list_with_deep_copy(
      const int64_t finish_scn,
      const bool update_in_major_type_merge,
      const ObMediumCompactionInfoList &input_list)
  {
    int ret = OB_SUCCESS;
    DLIST_FOREACH_X(input_info, input_list.medium_info_list_, OB_SUCC(ret)) {
      const ObMediumCompactionInfo *medium_info = static_cast<const ObMediumCompactionInfo *>(input_info);
      if (update_in_major_type_merge
          && medium_info->medium_snapshot_ == finish_scn) {
        last_compaction_type_ = medium_info->compaction_type_;
        wait_check_medium_scn_ = finish_scn;
      }
      if (medium_info->medium_snapshot_ > finish_scn) {
        ret = inner_deep_copy_node(*medium_info);
      }
    }
    return ret;
  }
  int inner_deep_copy_node(const ObMediumCompactionInfo &medium_info);

private:
  static const int64_t MEDIUM_LIST_VERSION = 1;
  static const int64_t MAX_SERIALIZE_SIZE = 2;
  static const int32_t MEDIUM_LIST_INFO_RESERVED_BITS = 52;

private:
  bool is_inited_;
  common::ObIAllocator *allocator_;

  // need serialize
  union {
    int64_t info_;
    struct {
      int64_t compat_                  : 8;
      int64_t last_compaction_type_    : 4; // check inner_table when last_compaction is major
      int64_t reserved_                : MEDIUM_LIST_INFO_RESERVED_BITS;
    };
  };
  int64_t wait_check_medium_scn_;

  MediumInfoList medium_info_list_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_MEDIUM_COMPACTION_H_
