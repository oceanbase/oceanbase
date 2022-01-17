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

#ifndef OB_GRANULE_PUMP_H_
#define OB_GRANULE_PUMP_H_
#include "sql/engine/ob_phy_operator.h"
#include "lib/container/ob_array.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_string.h"
#include "lib/lock/ob_spin_lock.h"
#include "sql/engine/ob_single_child_phy_operator.h"
#include "sql/engine/ob_no_children_phy_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/table/ob_table_scan.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/px/ob_granule_util.h"
#include "sql/engine/dml/ob_table_insert.h"
#include "sql/engine/ob_engine_op_traits.h"

namespace oceanbase {
namespace sql {

#define TSCOp typename ObEngineOpTraits<NEW_ENG>::TSC
#define ModifyOp typename ObEngineOpTraits<NEW_ENG>::TableModify

class ObGranulePumpArgs {
public:
  ObGranulePumpArgs(ObExecContext& ctx, const common::ObIArray<common::ObPartitionArray>& pkey_arrays,
      common::ObIArray<ObPxPartitionInfo>& partitions_info, storage::ObPartitionService& part_ser)
      : ctx_(ctx),
        pkey_arrays_(pkey_arrays),
        partition_service_(part_ser),
        partitions_info_(partitions_info),
        parallelism_(0),
        tablet_size_(0),
        gi_attri_flag_(0)
  {}
  virtual ~ObGranulePumpArgs() = default;

  TO_STRING_KV(K(partitions_info_), K(parallelism_), K(tablet_size_), K(gi_attri_flag_))

  bool partition_filter() const
  {
    return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_USE_PARTITION_FILTER);
  }
  bool pwj_gi() const
  {
    return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_PARTITION_WISE);
  }
  bool affinitize() const
  {
    return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_AFFINITIZE);
  }
  bool access_all() const
  {
    return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_ACCESS_ALL);
  }
  bool with_param_down() const
  {
    return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_NLJ_PARAM_DOWN);
  }
  bool asc_partition_order() const
  {
    return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_ASC_PARTITION_ORDER);
  }
  bool desc_partition_order() const
  {
    return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_DESC_PARTITION_ORDER);
  }
  bool force_partition_granule() const
  {
    return ObGranuleUtil::gi_has_attri(gi_attri_flag_, GI_FORCE_PARTITION_GRANULE);
  }

  ObExecContext& ctx_;
  const common::ObIArray<common::ObPartitionArray>& pkey_arrays_;
  storage::ObPartitionService& partition_service_;
  common::ObIArray<ObPxPartitionInfo>& partitions_info_;
  int64_t parallelism_;
  int64_t tablet_size_;
  uint64_t gi_attri_flag_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObGranulePumpArgs);
};

// TaskSet is used for multiple tables in one GI.
// For single table scan, there is only on element in partition_keys_ in ObGITaskSet.
// For Partition Wise N table scan(multiple tables in one GI ), there are N elements in partition_keys_ of ObGITaskSet.
class ObGITaskSet {
public:
  struct Pos {
    Pos() : task_idx_(0), partition_idx_(0)
    {}
    TO_STRING_KV(K(task_idx_), K(partition_idx_));

    int64_t task_idx_;
    int64_t partition_idx_;
  };

  ObGITaskSet() : partition_keys_(), ranges_(), offsets_(), partition_offsets_()
  {}
  TO_STRING_KV(K(partition_keys_), K(ranges_), K(ranges_.count()), K(offsets_), K(offsets_.count()),
      K(partition_offsets_), K(cur_));
  int get_task_at_pos(ObGranuleTaskInfo& info, const Pos& pos) const;
  int get_next_gi_task_pos(Pos& pos);
  int get_next_gi_task(ObGranuleTaskInfo& info);
  int assign(const ObGITaskSet& other);
  int set_pw_affi_partition_order(bool asc);

private:
  // reverse all tasks in the GI Task set.
  int reverse_task();

public:
  common::ObArray<ObPartitionKey> partition_keys_;
  common::ObArray<common::ObNewRange> ranges_;
  common::ObSEArray<int64_t, 2> offsets_;
  common::ObSEArray<int64_t, 2> partition_offsets_;
  Pos cur_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObGITaskSet);
};

static const int64_t OB_DEFAULT_GI_TASK_COUNT = 1;
typedef common::ObSEArray<ObGITaskSet, OB_DEFAULT_GI_TASK_COUNT> ObGITaskArray;
typedef common::ObIArray<ObGITaskSet> GITaskIArray;

struct GITaskArrayItem {
  TO_STRING_KV(K(tsc_op_id_));
  // table scan operator id or insert op id
  uint64_t tsc_op_id_;
  // gi task set array
  ObGITaskArray taskset_array_;
};

typedef common::ObArray<GITaskArrayItem> GITaskArrayMap;

/*
 * in most cases, the partition wise join has about 2 or 3 table scan below.
 * so eight hash bucket is large enough.
 * unfortunately, if we got more table scan, the hash map still work in a
 * inefficient way.
 * */
static const int64_t PARTITION_WISE_JOIN_TSC_HASH_BUCKET_NUM = 8;

// TODO  refactor
typedef common::hash::ObHashMap<uint64_t, ObGITaskSet, common::hash::NoPthreadDefendMode> TaskSetMap;

class ObGranuleSplitter {
public:
  ObGranuleSplitter() = default;
  virtual ~ObGranuleSplitter() = default;

  static int get_query_range(ObExecContext& ctx, const ObQueryRange& tsc_pre_query_range, ObIArray<ObNewRange>& ranges,
      int64_t table_id, bool partition_granule);

protected:
  int split_gi_task(ObExecContext& ctx, const ObQueryRange& tsc_pre_query_range, int64_t table_id,
      const common::ObIArray<common::ObPartitionKey>& pkeys, int64_t parallelism, int64_t tablet_size,
      storage::ObPartitionService& partition_service, bool partition_granule, ObGITaskSet& task_set);

public:
  ObSEArray<ObPxPartitionInfo, 8> partitions_info_;
};

class ObRandomGranuleSplitter : public ObGranuleSplitter {
public:
  ObRandomGranuleSplitter() = default;
  virtual ~ObRandomGranuleSplitter() = default;
  template <bool NEW_ENG>
  int split_granule(ObExecContext& ctx, common::ObIArray<const TSCOp*>& scan_ops,
      const common::ObIArray<common::ObPartitionArray>& pkey_arrays, int64_t parallelism, int64_t tablet_size,
      storage::ObPartitionService& partition_service, GITaskArrayMap& gi_task_array_result,
      bool partition_granule = true);

private:
};

class ObAccessAllGranuleSplitter : public ObGranuleSplitter {
public:
  ObAccessAllGranuleSplitter() = default;
  virtual ~ObAccessAllGranuleSplitter() = default;
  template <bool NEW_ENG>
  int split_granule(ObExecContext& ctx, common::ObIArray<const TSCOp*>& scan_ops,
      const common::ObIArray<common::ObPartitionArray>& pkey_arrays, int64_t parallelism, int64_t tablet_size,
      storage::ObPartitionService& partition_service, GITaskArrayMap& gi_task_array_result,
      bool partition_granule = true);

private:
  int split_tasks_access_all(ObGITaskSet& taskset, int64_t parallelism, ObGITaskArray& taskset_array);
};

class ObPartitionWiseGranuleSplitter : public ObGranuleSplitter {
public:
  ObPartitionWiseGranuleSplitter() = default;
  virtual ~ObPartitionWiseGranuleSplitter() = default;
  template <bool NEW_ENG>
  int split_granule(ObExecContext& ctx, ObIArray<const TSCOp*>& scan_ops,
      const common::ObIArray<common::ObPartitionArray>& pkey_arrays, int64_t parallelism, int64_t tablet_size,
      storage::ObPartitionService& partition_service, GITaskArrayMap& gi_task_array_result,
      bool partition_granule = true);
  template <bool NEW_ENG>
  int split_granule(ObExecContext& ctx, ObIArray<const TSCOp*>& scan_ops, const ModifyOp* modify_op,
      const common::ObIArray<common::ObPartitionArray>& pkey_arrays, int64_t parallelism, int64_t tablet_size,
      storage::ObPartitionService& partition_service, GITaskArrayMap& gi_task_array_result,
      bool partition_granule = true);

private:
  int split_insert_gi_task(ObExecContext& ctx, const uint64_t insert_table_id, const int64_t row_key_count,
      const common::ObIArray<common::ObPartitionKey>& pkeys, int64_t parallelism, int64_t tablet_size,
      storage::ObPartitionService& partition_service, bool partition_granule, ObGITaskSet& task_set);

  template <bool NEW_ENG>
  int split_tsc_gi_task(ObExecContext& ctx, ObIArray<const TSCOp*>& scan_ops,
      const common::ObIArray<common::ObPartitionArray>& pkey_arrays, int64_t parallelism, int64_t tablet_size,
      storage::ObPartitionService& partition_service, int64_t tsc_begin_idx, GITaskArrayMap& gi_task_array_result,
      bool partition_granule = true);
};

class ObAffinitizeGranuleSplitter : public ObGranuleSplitter {
public:
  ObAffinitizeGranuleSplitter() = default;
  virtual ~ObAffinitizeGranuleSplitter() = default;

protected:
  int split_tasks_affinity(ObExecContext& ctx, ObGITaskSet& taskset, int64_t parallelism, ObGITaskArray& taskset_array);
};

class ObNormalAffinitizeGranuleSplitter : public ObAffinitizeGranuleSplitter {
public:
  ObNormalAffinitizeGranuleSplitter() = default;
  virtual ~ObNormalAffinitizeGranuleSplitter() = default;
  template <bool NEW_ENG>
  int split_granule(ObExecContext& ctx, ObIArray<const TSCOp*>& scan_ops,
      const common::ObIArray<common::ObPartitionArray>& pkey_arrays, int64_t parallelism, int64_t tablet_size,
      storage::ObPartitionService& partition_service, GITaskArrayMap& gi_task_array_result,
      bool partition_granule = true);
};

class ObPWAffinitizeGranuleSplitter : public ObAffinitizeGranuleSplitter {
public:
  ObPWAffinitizeGranuleSplitter() = default;
  virtual ~ObPWAffinitizeGranuleSplitter() = default;
  template <bool NEW_ENG>
  int split_granule(ObExecContext& ctx, ObIArray<const TSCOp*>& scan_ops,
      const common::ObIArray<common::ObPartitionArray>& pkey_arrays, int64_t parallelism, int64_t tablet_size,
      storage::ObPartitionService& partition_service, GITaskArrayMap& gi_task_array_result,
      bool partition_granule = true);
  int adjust_task_order(bool asc, ObGITaskArray& taskset_array);
};

// A task will be send to many DFO and we use many threads to execute the DFO.
// the ObGranulePump object belong to a specified DFO, and many threads will
// use it to get granule task.
// the worker who revice the DFO will genrate a ObGranulePump object,
// and the worker who end last destroy this object.
class ObGranulePump {
private:
  static const int64_t OB_GRANULE_SHARED_POOL_POS = 0;
  enum ObGranuleSplitterType {
    GIT_UNINITIALIZED,
    GIT_PARTIAL_PARTITION_WISE_WITH_AFFINITY,
    GIT_ACCESS_ALL,
    GIT_FULL_PARTITION_WISE,
    GIT_FULL_PARTITION_WISE_WITH_AFFINITY,
    GIT_RANDOM,
  };

public:
  ObGranulePump()
      : lock_(),
        parallelism_(-1),
        tablet_size_(common::OB_DEFAULT_TABLET_SIZE),
        partition_wise_join_(false),
        gi_task_array_map_(),
        splitter_type_(GIT_UNINITIALIZED)
  {}
  virtual ~ObGranulePump()
  {
    destroy();
  }

  int add_new_gi_task(ObGranulePumpArgs& args, ObIArray<const ObTableScan*>& scan_ops, const ObTableModify* modify_op);
  int add_new_gi_task(
      ObGranulePumpArgs& args, ObIArray<const ObTableScanSpec*>& scan_ops, const ObTableModifySpec* modify_op);

  void destroy();

  int fetch_granule_task(const ObGITaskSet*& task_set, ObGITaskSet::Pos& pos, int64_t worker_id, uint64_t tsc_op_id);
  // get gi tasks with phy op ids.
  int try_fetch_pwj_tasks(ObIArray<ObGranuleTaskInfo>& infos, const ObIArray<int64_t>& op_ids, int64_t worker_id);
  DECLARE_TO_STRING;

private:
  template <bool NEW_ENG>
  int add_new_gi_task_inner(ObGranulePumpArgs& args, ObIArray<const TSCOp*>& scan_ops, const ModifyOp* modify_op);

  int fetch_granule_by_worker_id(
      const ObGITaskSet*& task_set, ObGITaskSet::Pos& pos, int64_t thread_id, uint64_t tsc_op_id);

  int fetch_granule_from_shared_pool(const ObGITaskSet*& task_set, ObGITaskSet::Pos& pos, uint64_t tsc_op_id);

  template <bool NEW_ENG>
  int fetch_pw_granule_by_worker_id(
      ObIArray<ObGranuleTaskInfo>& infos, const ObIArray<const TSCOp*>& tscs, int64_t thread_id);

  int fetch_pw_granule_from_shared_pool(ObIArray<ObGranuleTaskInfo>& infos, const ObIArray<int64_t>& op_ids);
  int check_pw_end(int64_t end_tsc_count, int64_t op_count, int64_t task_count);

  int find_taskset_by_tsc_id(uint64_t op_id, ObGITaskArray*& taskset_array);

private:
  common::ObSpinLock lock_;
  int64_t parallelism_;
  int64_t tablet_size_;
  bool partition_wise_join_;
  GITaskArrayMap gi_task_array_map_;
  ObGranuleSplitterType splitter_type_;
};

#undef TSCOp
#undef ModifyOp

}  // namespace sql
}  // namespace oceanbase

#endif
