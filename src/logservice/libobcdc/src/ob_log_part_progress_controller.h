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
 *
 * PartProgressController
 */

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_PART_PROGRESS_CONTROLLER_H__
#define OCEANBASE_LIBOBCDC_OB_LOG_PART_PROGRESS_CONTROLLER_H__

#include "share/ob_define.h"                  // OB_INVALID_TIMESTAMP
#include "lib/container/ob_array.h"         // ObArray
#include "lib/lock/ob_spin_lock.h"          // ObSpinLock
#include "lib/atomic/ob_atomic.h"           // ATOMIC_*
#include "lib/atomic/atomic128.h"           // uint128_t, CAS128, LOAD128
#include "ob_log_utils.h"

namespace oceanbase
{
namespace libobcdc
{
//////////////////////////// PartProgressController ////////////////////////////
// Partition progress controller
// 1. track the progress of each partition, providing an interface to get the minimum progress
// 2. progress is essentially a timestamp, the invalid value is OB_INVALID_TIMESTAMP
//
// TODO:
// 1. support any number of partitions
// 2. try to make it a lock-free data structure
class PartProgressController
{
  static const int64_t INVALID_PROGRESS = common::OB_INVALID_TIMESTAMP;
  // Print execution interval
  static const int64_t PRINT_GET_MIN_PROGRESS_INTERVAL =  1 * _MIN_;
public:
  PartProgressController();
  virtual ~PartProgressController();
  int init(const int64_t max_progress_cnt);
  void destroy();

  /// Assigning an ID to uniquely identify the progress and also setting the initial value of the progress
  ///
  /// Returns OB_NEED_RETRY if not enough identifiers are available
  int acquire_progress(int64_t &progress_id, const int64_t start_progress);

  /// Release progress ID into recycling pool
  int release_progress(const int64_t progress_id);

  /// update progress values with specify ID
  int update_progress(const int64_t progress_id, const int64_t progress);

  /// Get the current minimum progress value
  int get_min_progress(int64_t &progress);

  TO_STRING_KV(K_(progress_cnt),
      K_(valid_progress_cnt),
      "recycled_cnt", recycled_indices_.count(),
      K_(max_progress_cnt));
private:
  // Assign IDs to each thread
  int64_t get_itid_();

  // Update get_min_progress execution time
  void update_execution_time_(int64_t execution_time);
private:
  struct Item
  {
    int64_t progress_;

    void reset()
    {
      ATOMIC_STORE(&progress_, INVALID_PROGRESS);
    }

    void reset(const int64_t start_progress)
    {
      ATOMIC_STORE(&progress_, start_progress);
    }

    void update(const int64_t new_progress)
    {
      int64_t oldv = ATOMIC_LOAD(&(progress_));

      while ((oldv < new_progress) || (INVALID_PROGRESS == oldv)) {
        oldv = ATOMIC_VCAS(&(progress_), oldv, new_progress);
      }
    }

    int64_t get() const { return ATOMIC_LOAD(&progress_); }
  };

  typedef common::ObArray<int64_t> IndexArray;

private:
  bool                inited_;
  int64_t             max_progress_cnt_;
  Item                *progress_list_;

  // recycle array of progress id
  IndexArray          recycled_indices_;
  common::ObSpinLock  recycled_indices_lock_;

  // The maximum number of progress ids that have been allocated, including the reclaimed
  int64_t             progress_cnt_ CACHE_ALIGNED;

  // Number of valid progress ids, not including recycled
  int64_t             valid_progress_cnt_ CACHE_ALIGNED;

  // Used to assign serial numbers to threads
  int64_t thread_counter_;

  // Low 64 bits: records the number of times the array was scanned; High 64 bits: records the total time the array was scanned
  types::uint128_t last_global_count_and_timeval_;
  types::uint128_t global_count_and_timeval_;

private:
  DISALLOW_COPY_AND_ASSIGN(PartProgressController);
};

}
}

#endif
