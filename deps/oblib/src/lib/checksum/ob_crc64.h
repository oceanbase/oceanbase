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

#ifndef  OCEANBASE_COMMON_CRC64_H_
#define  OCEANBASE_COMMON_CRC64_H_

#include <stdint.h>
#include <algorithm>
#include <string.h>
#include "lib/alloc/alloc_assist.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"

#define OB_DEFAULT_CRC64_POLYNOM 0xD800000000000000ULL

namespace oceanbase
{
namespace common
{
typedef uint64_t (*ObCRC64Func)(uint64_t, const char *, int64_t);
extern ObCRC64Func ob_crc64_sse42_func;

/**
  * create the crc64_table and optimized_crc64_table for calculate
  * must be called before ob_crc64
  * if not, the return value of ob_crc64 will be undefined
  * @param crc64 polynom
  */
void ob_init_crc64_table(const uint64_t polynom);

/**
  * Processes a multiblock of a CRC64 calculation.
  *
  * @returns Intermediate CRC64 value.
  * @param   uCRC64  Current CRC64 intermediate value.
  * @param   pv      The data block to process.
  * @param   cb      The size of the data block in bytes.
  */
uint64_t ob_crc64(uint64_t uCRC64, const void *pv, int64_t cb) ;

//Calculates CRC64 using CPU instructions.
inline uint64_t ob_crc64_sse42(uint64_t uCRC64, const void *pv, int64_t cb)
{
  return (*ob_crc64_sse42_func)(uCRC64, static_cast<const char *>(pv), cb);
}

/**
  * Calculate CRC64 for a memory block.
  *
  * @returns CRC64 for the memory block.
  * @param   pv      Pointer to the memory block.
  * @param   cb      Size of the memory block in bytes.
  */
uint64_t ob_crc64(const void *pv, int64_t cb);

//Calculates CRC64 using CPU instructions.
inline uint64_t ob_crc64_sse42(const void *pv, int64_t cb)
{
  return (*ob_crc64_sse42_func)(0, static_cast<const char *>(pv), cb);
}

uint64_t ob_crc64_isal(uint64_t uCRC64, const char* buf, int64_t cb);
uint64_t crc64_sse42(uint64_t uCRC64, const char* buf, int64_t len);
uint64_t crc64_sse42_manually(uint64_t crc, const char *buf, int64_t len);
uint64_t fast_crc64_sse42_manually(uint64_t crc, const char *buf, int64_t len);

/**
  * Get the static CRC64 table. This function is only used for testing purpose.
  *
  */
const uint64_t *ob_get_crc64_table();

class ObBatchChecksum
{
  OB_UNIS_VERSION(1);
public:
  // The ob_crc64 function has obvious advantages in calculating integer multiples of 64 bytes
  static const int64_t BUFFER_SIZE = 64;
public:
  ObBatchChecksum() : pos_(0), base_(0)
  {
  };
  ~ObBatchChecksum()
  {
  };
public:
  inline void reset()
  {
    pos_ = 0;
    base_ = 0;
  };
  inline void set_base(const uint64_t base)
  {
    base_ = base;
  };
  inline void fill(const void *pv, const int64_t cb)
  {
    if (NULL != pv
        && 0 < cb) {
      char *ptr = (char *)pv;
      int64_t size2fill = cb;
      int64_t size2copy = 0;
      while (size2fill > 0) {
        size2copy = std::min(BUFFER_SIZE - pos_, size2fill);
        MEMCPY(&buffer_[pos_], ptr + cb - size2fill, size2copy);
        size2fill -= size2copy;
        pos_ += size2copy;

        if (pos_ >= BUFFER_SIZE) {
          base_ = ob_crc64(base_, buffer_, BUFFER_SIZE);
          pos_ = 0;
        }
      }
    }
  };
  uint64_t calc()
  {
    if (0 < pos_) {
      base_ = ob_crc64(base_, buffer_, pos_);
      pos_ = 0;
    }
    return base_;
  };
  inline void deep_copy(const ObBatchChecksum& other)
  {
    pos_ = other.pos_;
    base_ = other.base_;
    MEMCPY(buffer_, other.buffer_, BUFFER_SIZE);
  }
  TO_STRING_KV(K_(pos), K_(base), KPHEX_(buffer, BUFFER_SIZE));
private:
  char buffer_[BUFFER_SIZE];
  int64_t pos_;
  uint64_t base_;
};
}
}

#endif
