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

#ifndef SRC_STORAGE_COMPACTION_OB_TENANT_COMPACTION_PROGRESS_H_
#define SRC_STORAGE_COMPACTION_OB_TENANT_COMPACTION_PROGRESS_H_

#include "ob_compaction_suggestion.h" // for ObInfoRingArray
#include "ob_partition_merge_progress.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
namespace oceanbase
{
namespace compaction
{
struct ObCompactionProgress
{
  ObCompactionProgress()
    : tenant_id_(OB_INVALID_TENANT_ID),
      merge_type_(compaction::INVALID_MERGE_TYPE),
      merge_version_(0),
      status_(share::ObIDag::DAG_STATUS_MAX),
      data_size_(0),
      unfinished_data_size_(0),
      original_size_(0),
      compressed_size_(0),
      start_time_(0),
      estimated_finish_time_(0),
      start_cg_idx_(0),
      end_cg_idx_(0)
  {
  }
  bool is_valid() const;
  void reset();

  TO_STRING_KV(K_(tenant_id), "merge_type", merge_type_to_str(merge_type_), K_(merge_version), K_(status), K_(data_size), K_(unfinished_data_size),
      K_(original_size), K_(compressed_size), K_(start_time), K_(estimated_finish_time),
      K_(start_cg_idx), K_(end_cg_idx));

  constexpr static double MERGE_SPEED = 1;  // almost 2 sec per macro_block
  constexpr static double EXTRA_TIME = 15 * 1000 * 1000; // 15 sec

  int64_t tenant_id_;
  compaction::ObMergeType merge_type_;
  int64_t merge_version_;
  share::ObIDag::ObDagStatus status_;
  int64_t data_size_;
  int64_t unfinished_data_size_;
  int64_t original_size_;
  int64_t compressed_size_;
  int64_t start_time_;
  int64_t estimated_finish_time_;
  int64_t start_cg_idx_;
  int64_t end_cg_idx_;
};

struct ObTenantCompactionProgress : public ObCompactionProgress
{
  ObTenantCompactionProgress()
    : ObCompactionProgress(),
      is_inited_(false),
      total_tablet_cnt_(0),
      unfinished_tablet_cnt_(0),
      real_finish_cnt_(0),
      sum_time_guard_()
  {
  }
  bool is_valid() const;
  ObTenantCompactionProgress & operator=(const ObTenantCompactionProgress &other);
  INHERIT_TO_STRING_KV("ObCompactionProgress", ObCompactionProgress, K_(is_inited), K_(total_tablet_cnt),
      K_(unfinished_tablet_cnt), K_(real_finish_cnt), K_(sum_time_guard));

  bool is_inited_;
  int64_t total_tablet_cnt_;
  int64_t unfinished_tablet_cnt_;
  int64_t real_finish_cnt_;
  ObStorageCompactionTimeGuard sum_time_guard_;
};

/*
 * ObTenantCompactionProgressMgr
 * */
class ObTenantCompactionProgressMgr : public ObInfoRingArray<ObTenantCompactionProgress> {
public:
  static const int64_t SERVER_PROGRESS_MAX_CNT = 30;

  ObTenantCompactionProgressMgr()
   : ObInfoRingArray(allocator_)
  {
    allocator_.set_attr(SET_USE_500("TenCompProgMgr"));
  }
  ~ObTenantCompactionProgressMgr() {}
  static int mtl_init(ObTenantCompactionProgressMgr* &progress_mgr);
  int init();
  void destroy();
  int init_progress(const int64_t major_snapshot_version);
  int finish_progress(const int64_t major_snapshot_version);
  int update_progress(
      const int64_t major_snapshot_version,
      const int64_t total_data_size_delta,
      const int64_t scanned_data_size_delta,
      const int64_t estimate_finish_time,
      const bool finish_flag,
      const ObCompactionTimeGuard *time_guard = nullptr,
      const bool co_merge = false);
  int update_unfinish_tablet(
      const int64_t major_snapshot_version,
      const int64_t reduce_tablet_cnt = 1,
      const int64_t reduce_data_size = 0);
  int update_compression_ratio(const int64_t major_snapshot_version, compaction::ObSSTableMergeHistory &merge_history);

private:
  int loop_major_sstable_(int64_t version, int64_t &cnt, int64_t &size);
  int finish_progress_(ObTenantCompactionProgress &progress);
  int get_pos_(const int64_t major_snapshot_version, int64_t &pos) const;

private:
  static const int64_t FINISH_TIME_UPDATE_FROM_SCHEDULER_INTERVAL = 10 * 1000 * 1000; // 1 second

private:
  ObArenaAllocator allocator_;
};

/*
 * ObCompactionSuggestionIterator
 * */

class ObTenantCompactionProgressIterator
{
public:
  ObTenantCompactionProgressIterator()
   : progress_array_(),
     cur_idx_(0),
     is_opened_(false)
  {
  }
  virtual ~ObTenantCompactionProgressIterator() { reset(); }
  int open(const int64_t tenant_id);
  int get_next_info(ObTenantCompactionProgress &info);
  void reset();

private:
  ObArray<ObTenantCompactionProgress> progress_array_;
  int64_t cur_idx_;
  bool is_opened_;
};


struct ObTabletCompactionProgress : public ObCompactionProgress
{
  ObTabletCompactionProgress()
    : ObCompactionProgress(),
      ls_id_(0),
      tablet_id_(0),
      dag_id_(),
      progressive_merge_round_(0),
      create_time_(0)
  {
  }
  bool is_valid() const;
  void reset();
  INHERIT_TO_STRING_KV("ObCompactionProgress", ObCompactionProgress, K_(ls_id),
      K_(tablet_id), K_(dag_id), K_(progressive_merge_round), K_(create_time));

  int64_t ls_id_;
  int64_t tablet_id_;
  share::ObDagId dag_id_;
  int64_t progressive_merge_round_;
  int64_t create_time_;
};

struct ObDiagnoseTabletCompProgress : public ObCompactionProgress
{
  ObDiagnoseTabletCompProgress()
    : ObCompactionProgress(),
      is_suspect_abormal_(false),
      dag_id_(),
      create_time_(0),
      latest_update_ts_(0),
      base_version_(0),
      snapshot_version_(0)
  {
  }
  bool is_valid() const;
  void reset();
  INHERIT_TO_STRING_KV("ObCompactionProgress", ObCompactionProgress, K_(is_suspect_abormal),
      K_(create_time), K_(latest_update_ts), K_(dag_id), K_(base_version), K_(snapshot_version), K_(status));

  bool is_suspect_abormal_;
  share::ObDagId dag_id_;
  int64_t create_time_;
  int64_t latest_update_ts_;
  int64_t base_version_;
  int64_t snapshot_version_;
};

/*
 * ObCompactionSuggestionIterator
 * */

class ObTabletCompactionProgressIterator
{
public:
  ObTabletCompactionProgressIterator()
   : allocator_("PartProgress"),
     progress_array_(),
     cur_idx_(0),
     is_opened_(false)
  {
  }
  virtual ~ObTabletCompactionProgressIterator() { reset(); }
  int open(const int64_t tenant_id);
  int get_next_info(ObTabletCompactionProgress &info);
  void reset();

private:
  ObArenaAllocator allocator_;
  common::ObArray<ObTabletCompactionProgress *> progress_array_;
  int64_t cur_idx_;
  bool is_opened_;
};

}//compaction
}//oceanbase

#endif
