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

#ifndef OCEANBASE_SQL_EXECUTOR_MULTISCAN_TASK_SPLITER_
#define OCEANBASE_SQL_EXECUTOR_MULTISCAN_TASK_SPLITER_

#include "share/schema/ob_table_schema.h"
#include "sql/executor/ob_task_spliter.h"
#include "lib/container/ob_array.h"
#include "lib/hash/ob_placement_hashmap.h"
#include "lib/hash/ob_iteratable_hashset.h"

namespace oceanbase {
namespace sql {
class ObIntraPartitionTaskSpliter : public ObTaskSpliter {
public:
  ObIntraPartitionTaskSpliter();
  virtual ~ObIntraPartitionTaskSpliter();
  virtual int get_next_task(ObTaskInfo*& task);
  virtual TaskSplitType get_type() const
  {
    return ObTaskSpliter::INTRA_PARTITION_SPLIT;
  }

private:
  int prepare();
  int get_part_and_ranges(
      const share::ObPartitionReplicaLocation*& part_rep_loc, const ObSplittedRanges*& splitted_ranges);
  int get_scan_ranges(const ObSplittedRanges& splitted_ranges, ObTaskInfo::ObPartLoc& part_loc);

private:
  const ObPhyTableLocation* table_loc_;
  const ObPartitionReplicaLocationIArray* part_rep_loc_list_;
  const ObSplittedRangesIArray* splitted_ranges_list_;
  int64_t next_task_id_;
  int64_t part_idx_;
  int64_t range_idx_;
  bool prepare_done_;
};

class ObDistributedTaskSpliter : public ObTaskSpliter {
private:
  enum ObMatchType {
    MT_ONLY_MATCH = 0,
    MT_ALL_PART = 1,
    MT_ALL_SLICE = 2,
    MT_ALL_BOTH = MT_ALL_PART | MT_ALL_SLICE,
  };
  struct ObPartComparer {
  public:
    ObPartComparer(common::ObIArray<ObShuffleKeys>& shuffle_keys, bool cmp_part, bool cmp_subpart, int sort_order);
    virtual ~ObPartComparer();
    bool operator()(int64_t idx1, int64_t idx2);
    int get_ret() const
    {
      return ret_;
    }

  private:
    common::ObIArray<ObShuffleKeys>& shuffle_keys_;
    bool cmp_part_;
    bool cmp_subpart_;
    int sort_order_;  // asc: 1, desc: -1.
    int ret_;
  };
  struct ObSliceComparer {
  public:
    ObSliceComparer(bool cmp_part, bool cmp_subpart, int sort_order);
    virtual ~ObSliceComparer();
    bool operator()(const ObSliceEvent* slice1, const ObSliceEvent* slice2);
    int get_ret() const
    {
      return ret_;
    }

  private:
    bool cmp_part_;
    bool cmp_subpart_;
    int sort_order_;  // asc: 1, desc: -1.
    int ret_;
  };
  struct ObPhyTableLoc {
  public:
    ObPhyTableLoc()
        : table_loc_(NULL),
          depend_table_keys_(common::ObModIds::OB_SQL_EXECUTOR_TASK_SPLITER, OB_MALLOC_NORMAL_BLOCK_SIZE)
    {}
    virtual ~ObPhyTableLoc()
    {}
    void reset()
    {
      table_loc_ = NULL;
      depend_table_keys_.reset();
    }
    bool is_valid() const
    {
      return NULL != table_loc_;
    }
    const ObPhyTableLocation* get_table_loc() const
    {
      return table_loc_;
    }
    int set_table_loc(const ObPhyTableLocation* table_loc)
    {
      int ret = common::OB_SUCCESS;
      if (OB_ISNULL(table_loc)) {
        ret = common::OB_INVALID_ARGUMENT;
        SQL_EXE_LOG(ERROR, "table loc is NULL", K(ret), K(table_loc));
      } else {
        table_loc_ = table_loc;
      }
      return ret;
    }
    const common::ObIArray<ObPartitionKey>& get_depend_table_keys() const
    {
      return depend_table_keys_;
    }
    int add_depend_table_key(ObPartitionKey& depend_table_key)
    {
      return depend_table_keys_.push_back(depend_table_key);
    }
    TO_STRING_KV(K_(table_loc), K_(depend_table_keys));

  private:
    const ObPhyTableLocation* table_loc_;
    common::ObSEArray<ObPartitionKey, 1> depend_table_keys_;
  };

public:
  ObDistributedTaskSpliter();
  virtual ~ObDistributedTaskSpliter();
  virtual int get_next_task(ObTaskInfo*& task);
  virtual TaskSplitType get_type() const
  {
    return ObTaskSpliter::DISTRIBUTED_SPLIT;
  }

private:
  int prepare();
  int init_match_type();
  int init_table_locations(ObPhyOperator* root_op);
  int check_table_locations();
  int init_part_shuffle_keys();
  int sort_part_shuffle_keys();
  int get_shuffle_keys(
      const share::schema::ObTableSchema& table_schema, const ObPartitionKey& part_key, ObShuffleKeys& shuffle_keys);
  int init_child_task_results();
  int sort_child_slice_shuffle_keys();
  int compare_head_part_slice(int& cmp);
  int task_add_head_part(ObTaskInfo*& task_info);
  int task_add_head_slices(ObTaskInfo& task_info);
  int task_add_empty_part(ObTaskInfo*& task_info);
  int task_add_empty_slice(ObTaskInfo& task_info);
  int get_task_location(const ObSliceID& ob_slice_id, ObTaskLocation& task_location);
  int calc_head_slice_count();
  bool need_all_part()
  {
    return match_type_ & MT_ALL_PART;
  }
  bool need_all_slice()
  {
    return match_type_ & MT_ALL_SLICE;
  }
  int get_or_create_task_info(const common::ObAddr& task_server, ObTaskInfo*& task_info);
  int64_t get_total_part_cnt() const;
  int get_task_runner_server(common::ObAddr& runner_server) const;
  int need_split_task_by_partition(bool& by_partition) const;

private:
  // table informations.
  common::ObSEArray<ObPhyTableLoc, 8> table_locations_;
  common::ObSEArray<ObShuffleKeys, 8> part_shuffle_keys_;
  common::ObSEArray<int64_t, 8> part_idxs_;
  // child task result informations.
  common::ObSEArray<const ObSliceEvent*, 16> child_slices_;
  // iteration informations.
  ObMatchType match_type_;  // like join type, inner, left/right outer, full.
  int64_t next_task_id_;
  int64_t head_part_idx_;
  int64_t head_slice_idx_;
  int64_t head_slice_count_;
  int sort_order_;  // asc: 1, desc: -1.
  bool head_slice_matched_;
  // others.
  bool repart_part_;
  bool repart_subpart_;
  bool prepare_done_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDistributedTaskSpliter);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_EXECUTOR_MULTISCAN_TASK_SPLITER_ */
//// end of header file
