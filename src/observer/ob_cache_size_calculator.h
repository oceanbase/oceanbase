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

#ifndef OCEANBASE_OBSERVER_OB_CACHE_SIZE_CALCULATOR_H_
#define OCEANBASE_OBSERVER_OB_CACHE_SIZE_CALCULATOR_H_

#include "share/ob_define.h"
#include "share/ob_thread_pool.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace share {
namespace schema {
class ObMultiVersionSchemaService;
}
}  // namespace share

namespace observer {
class ObCacheSizeCalculator : public share::ObThreadPool {
public:
  ObCacheSizeCalculator();
  virtual ~ObCacheSizeCalculator();
  int init(storage::ObPartitionService& partition_service, share::schema::ObMultiVersionSchemaService& schema_service);
  virtual void run1();
  int start();
  void stop();

private:
  static const int64_t RENEW_CACHE_SIZE_INTERVAL_US = 10 * 1000 * 1000;
  static const int64_t MIN_HOLD_SIZE = 10L * 1024L * 1024L;         // 10MB
  static const int64_t MAX_HOLD_SIZE = 5L * 1024L * 1024L * 1024L;  // 5GB
  static const int64_t MEM_BLOCK_FRAGMENT_PERCENT = 10;
  int calc_loc_cache_size(int64_t& cache_size);
  int calc_schema_cache_size(int64_t& cache_size);
  int set_cache_size(const int64_t loc_cache_size, const int64_t schema_cache_size);
  int64_t normalize_cache_size(const int64_t cache_size);
  bool inited_;
  storage::ObPartitionService* partition_service_;
  share::schema::ObMultiVersionSchemaService* schema_service_;

  DISALLOW_COPY_AND_ASSIGN(ObCacheSizeCalculator);
};
}  // end namespace observer
}  // end namespace oceanbase

#endif  // OCEANBASE_OBSERVER_OB_CACHE_SIZE_CALCULATOR_H_
