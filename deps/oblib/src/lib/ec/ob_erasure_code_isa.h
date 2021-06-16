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

#ifndef OCEANBASE_COMMON_EC_OB_ERASURECODE_OB_ERASURECODE_ISA_H
#define OCEANBASE_COMMON_EC_OB_ERASURECODE_OB_ERASURECODE_ISA_H

#include <isa-l/erasure_code.h>
#include "lib/ob_errno.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "lib/ec/ob_erasure_code_table_cache.h"

namespace oceanbase {
namespace common {
class ObErasureCodeIsa {
public:
  static int encode(const int64_t data_count, const int64_t parity_count, const int64_t block_size,
      unsigned char** data_blocks, unsigned char** parity_blocks);

  static int decode(const int64_t data_count, const int64_t parity_count, const int64_t block_size,
      const ObIArray<int64_t>& erase_indexes, unsigned char** data_blocks, unsigned char** recovery_blocks);

  static int decode(const int64_t data_count, const int64_t parity_count, const int64_t block_size,
      const ObIArray<int64_t>& data_block_indexes, const ObIArray<int64_t>& erase_block_indexes,
      unsigned char** data_blocks, unsigned char** recovery_blocks);

  static int append_encode(const int64_t data_count, const int64_t parity_count, const int64_t block_size,
      const int64_t block_index, unsigned char* data_block, unsigned char** parity_blocks);

private:
  DISALLOW_COPY_AND_ASSIGN(ObErasureCodeIsa);
};

}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_COMMON_EC_OB_ERASURECODE_OB_ERASURECODE_ISA_H
