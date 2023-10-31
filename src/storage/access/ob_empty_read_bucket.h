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

#ifndef OCEANBASE_STORAGE_OB_EMPTY_READ_BUCKET_H_
#define OCEANBASE_STORAGE_OB_EMPTY_READ_BUCKET_H_

#include <stdint.h>
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace storage
{
struct ObEmptyReadCell
{
  ObEmptyReadCell(): count_(0), hashcode_(0), build_time_(0) {}
  virtual ~ObEmptyReadCell() {}
  void reset()
  {
    count_ = 0;
    hashcode_ = 0;
    build_time_ = 0;
  }
  void set(
      const uint64_t hashcode,
      const int64_t inc_val)
  {
    count_ = static_cast<int32_t>(inc_val);
    hashcode_ = hashcode;
    build_time_ = ObTimeUtility::current_time();
  }
  int inc_and_fetch(
      const uint64_t hashcode,
      const int64_t inc_val,
      uint64_t &cur_cnt)
  {
    int ret = OB_SUCCESS;
    if(hashcode_ != hashcode) {
      if(!build_time_ || ObTimeUtility::current_time() - ELIMINATE_TIMEOUT_US > build_time_){
        set(hashcode, inc_val);
        cur_cnt = count_;
      } else {
        //bucket is in use recently,ignore in 2min
        cur_cnt = 1;
      }
    } else {
      count_ += inc_val;
      cur_cnt = count_;
    }
    return ret;
  }
  int inc_and_fetch(
      const uint64_t hashcode,
      uint64_t &cur_cnt)
  {
    return inc_and_fetch(hashcode, 1, cur_cnt);
  }
  bool check_timeout()
  {
    bool bool_ret = false;
    int64_t cur_time = ObTimeUtility::current_time();
    if (cur_time - ELIMINATE_TIMEOUT_US > build_time_) {
      bool_ret = true;
      count_ /= 2;
      build_time_ = cur_time;
    }
    return bool_ret;
  }
  TO_STRING_KV(K_(count), K_(hashcode), K_(build_time));
  static const int64_t ELIMINATE_TIMEOUT_US = 1000 * 1000 * 120; //2min
  volatile int32_t count_;
  volatile uint64_t hashcode_;
  volatile int64_t build_time_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObEmptyReadCell);
};

class ObEmptyReadBucket
{
public:
  ObEmptyReadBucket();
  virtual ~ObEmptyReadBucket();
  static int mtl_init(ObEmptyReadBucket *&bucket);
  static void mtl_destroy(ObEmptyReadBucket *&bucket);
  int init(const int64_t lower_bound);
  void destroy();
  OB_INLINE bool is_valid() const { return NULL != buckets_; }
  int get_cell(const uint64_t hashcode, ObEmptyReadCell *&cell);

  void reset();

private:
  OB_INLINE uint64_t get_bucket_size() const { return bucket_size_; }
  static const int64_t MIN_REBUILD_PERIOD_US = 1000 * 1000 * 120; //2min
  static constexpr int64_t BUCKET_SIZE_LIMIT = 1<<20;//1048576
  ObArenaAllocator allocator_;
  ObEmptyReadCell *buckets_;
  uint64_t bucket_size_;
};
} // namespace oceanbase
} // namespace storage

#endif // OCEANBASE_STORAGE_OB_EMPTY_READ_BUCKET_H_
