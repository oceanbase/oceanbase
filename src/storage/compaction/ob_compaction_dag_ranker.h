/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef STORAGE_COMPACTION_OB_COMPACTION_DAG_RANKER_H_
#define STORAGE_COMPACTION_OB_COMPACTION_DAG_RANKER_H_

#include "lib/container/ob_se_array.h"
#include "ob_compaction_util.h"

namespace oceanbase
{

namespace compaction
{
enum ObMergeType;
struct ObCompactionParam;
class ObTabletMergeDag;


struct ObCompactionEstimator
{
public:
  static int estimate_compaction_memory(
    const int64_t priority,
    const ObCompactionParam &param,
    int64_t &estimate_mem_usage);
  static int64_t estimate_compaction_batch_size(
    const compaction::ObMergeType merge_type,
    const int64_t compaction_mem_limit,
    const int64_t concurrent_cnt,
    const int64_t sstable_cnt);

public:
  static constexpr int64_t DEFAULT_MERGE_THREAD_CNT = 6;
  static constexpr int64_t MAX_MEM_PER_THREAD = 8 * 1024L * 1024L; // 8MB for serial compaction
  static constexpr int64_t MINI_MEM_PER_THREAD = 7 * 1024L * 1024L; // 7MB
  static constexpr int64_t MINI_PARALLEL_BASE_MEM = 1 << 30; // 1GB
  static constexpr int64_t MINOR_MEM_PER_THREAD = 6 * 1024L * 1024L; // 6MB
  static constexpr int64_t MAJOR_MEM_PER_THREAD = 5 * 1024L * 1024L; // 5MB
  static constexpr int64_t CO_MAJOR_CG_BASE_MEM = 3 * 1024L * 1024L; // 3MB
  static constexpr int64_t COMPACTION_BLOCK_FIXED_MEM = 14 * 1024L * 1024L; // 14MB
  static constexpr int64_t COMPACTION_ITER_BASE_MEM = 4 * 1024L * 1024L; // 4MB
  static constexpr int64_t COMPACTION_RESERVED_MEM = 2 * 1024L * 1024L; // 2MB
  static constexpr int64_t COMPACTION_CONCURRENT_MEM_FACTOR = 6 * 1024L * 1024L; // 6MB
  static constexpr int64_t DEFAULT_COMPACTION_MEM = 22 * 1024L * 1024L; // 22MB
  static constexpr int64_t DEFAULT_BATCH_SIZE = 10;
};


struct ObCompactionRankHelper
{
public:
  explicit ObCompactionRankHelper(const int64_t rank_time);
  virtual ~ObCompactionRankHelper() = default;
  virtual bool is_valid() const;
  virtual bool check_need_rank() const;
  virtual void update(
      const int64_t current_time,
      const ObCompactionParam &param);
  virtual int get_rank_weighed_score(
      common::ObSEArray<compaction::ObTabletMergeDag *, 32> &dags) const = 0;
  VIRTUAL_TO_STRING_KV(K_(rank_time), K_(max_occupy_size), K_(min_occupy_size), K_(max_wait_time),
                       K_(min_wait_time), K_(max_sstable_cnt), K_(min_sstable_cnt));
public:
  // used for weight normalization
  const uint64_t rank_time_;
  uint64_t max_occupy_size_;
  uint64_t min_occupy_size_;
  int64_t max_wait_time_;
  int64_t min_wait_time_;
  uint16_t max_sstable_cnt_;
  uint16_t min_sstable_cnt_;
};

struct ObMiniCompactionRankHelper : public ObCompactionRankHelper
{
public:
  explicit ObMiniCompactionRankHelper(const int64_t rank_time);
  virtual ~ObMiniCompactionRankHelper() = default;
  virtual bool is_valid() const override;
  virtual bool check_need_rank() const override;
  virtual void update(
      const int64_t current_time,
      const ObCompactionParam &param) override;
  virtual int get_rank_weighed_score(
      common::ObSEArray<compaction::ObTabletMergeDag *, 32> &dags) const override;
  INHERIT_TO_STRING_KV("ObMiniCompactionRankHelper", ObCompactionRankHelper,
                       K_(max_replay_interval), K_(min_replay_interval));
public:
  int64_t max_replay_interval_;
  int64_t min_replay_interval_;
};

struct ObMinorCompactionRankHelper : public ObCompactionRankHelper
{
public:
  explicit ObMinorCompactionRankHelper(const int64_t rank_time);
  virtual ~ObMinorCompactionRankHelper() = default;
  virtual bool is_valid() const override;
  virtual bool check_need_rank() const override;
  virtual void update(
      const int64_t current_time,
      const ObCompactionParam &param) override;
  virtual int get_rank_weighed_score(
      common::ObSEArray<compaction::ObTabletMergeDag *, 32> &dags) const override;
  INHERIT_TO_STRING_KV("ObMinorCompactionRankHelper", ObCompactionRankHelper,
                       K_(max_parallel_dag_cnt), K_(min_parallel_dag_cnt));
public:
  uint64_t max_parallel_dag_cnt_;
  uint64_t min_parallel_dag_cnt_;
};


class ObCompactionDagRanker
{
public:
  struct ObCompactionRankScoreCompare
  {
    explicit ObCompactionRankScoreCompare(int &sort_ret)
      : result_code_(sort_ret) {}
    bool operator()(
      const compaction::ObTabletMergeDag *left,
      const compaction::ObTabletMergeDag *right) const;
    int compare_dags_with_score(
      const compaction::ObTabletMergeDag *left,
      const compaction::ObTabletMergeDag *right,
      bool &bret) const;

    int &result_code_;
  };

public:
  ObCompactionDagRanker();
  virtual ~ObCompactionDagRanker();
  void destroy();
  int init(const int64_t priority, const int64_t rank_time);
  void update(const int64_t current_time, const ObCompactionParam &param);
  bool is_valid() const { return is_inited_; }
  int sort(common::ObSEArray<compaction::ObTabletMergeDag *, 32> &dags);
  TO_STRING_KV(K_(rank_helper), K_(is_inited));
private:
  template<typename T>
  int create_rank_helper(const int64_t rank_time, ObCompactionRankHelper *&helper);

private:
  common::ObArenaAllocator allocator_;
  ObCompactionRankHelper *rank_helper_;
  bool is_inited_;
};


} //compaction
} //oceanbase


#endif //STORAGE_COMPACTION_OB_COMPACTION_DAG_RANKER_H_