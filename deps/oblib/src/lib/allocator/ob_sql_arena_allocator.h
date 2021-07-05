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

#ifndef _OCEABASE_LIB_ALLOCATOR_OB_SQL_ARENA_ALLOCATOR_H_
#define _OCEABASE_LIB_ALLOCATOR_OB_SQL_ARENA_ALLOCATOR_H_

#include <cstdint>
#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase {
namespace common {
using common::ObArenaAllocator;

class ObSQLArenaAllocator : public ObIAllocator {
  const static int THRESHOLD_SIZE_ONCE = 1 << 20;            // 1M
  const static int DEFAULT_THRESHOLD_SIZE_TOTAL = 10 << 20;  // 10M
public:
  ObSQLArenaAllocator(const int64_t tenant_id = common::OB_SERVER_TENANT_ID);
  virtual ~ObSQLArenaAllocator();
  void set_tenant_id(const uint64_t tenant_id);
  void set_threshold_size_total(const int threshold_size_total);
  void* alloc(const int64_t size) override;
  void* alloc(const int64_t size, const ObMemAttr& attr) override;
  void free(void* ptr) override;
  void reset() override;
  int64_t total() const override;
  bool set_tracer();
  bool revert_tracer();
  void reset_remain_one_page();

private:
  // Use normal_application when the single and total size does not exceed the threshold
  // Apply for the rest with large_
  ObArenaAllocator normal_;
  ObArenaAllocator large_;
  int threshold_size_total_;
};

}  // end of namespace common
}  // end of namespace oceanbase

#endif /* _OCEABASE_LIB_ALLOCATOR_OB_SQL_ARENA_ALLOCATOR_H_ */
