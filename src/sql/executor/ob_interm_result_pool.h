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

#ifndef OCEANBASE_SQL_EXECUTOR_INTERM_RESULT_POOL_
#define OCEANBASE_SQL_EXECUTOR_INTERM_RESULT_POOL_

#include "lib/lock/ob_spin_lock.h"
#include "lib/allocator/ob_small_allocator.h"

namespace oceanbase {
namespace common {
class ObScanner;
}
namespace sql {
class ObIntermResult;
class ObIntermResultPool {
public:
  // Assume than one interm result is 1MB and we need manage 10TB intermediate data,
  // we need 10TB/1MB =  10M interm result.
  static const int64_t INTERM_RESULT_CAPACITY = 10L << 20;  // 10M
  // Scanner count is much less than interm result count, because it will be freed
  // before interm result add to manager.
  static const int64_t SCANNER_CAPACITY = 256L << 10;  // 256K

  static const int64_t SCANNER_MEM_LIMIT = 8 << 20;  // 8MB

  ObIntermResultPool();
  virtual ~ObIntermResultPool();

  static int build_instance();
  static ObIntermResultPool* get_instance();

  void reset();
  int alloc_interm_result(ObIntermResult*& interm_result);
  int free_interm_result(ObIntermResult* interm_result);

  int alloc_scanner(common::ObScanner*& scanner);
  void free_scanner(common::ObScanner* scanner);

private:
  int init();

private:
  static ObIntermResultPool* instance_;
  // this interm result pool is initialized
  bool inited_;
  // the small allocator
  common::ObSmallAllocator allocator_;
  common::ObSmallAllocator scanner_allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObIntermResultPool);
};
} /* namespace sql */
} /* namespace oceanbase */
#endif /* OCEANBASE_SQL_EXECUTOR_INTERM_RESULT_POOL_ */
