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

#ifndef OCEANBASE_SHARE_STAT_OB_FM_SKETCH_UTILS_H_
#define OCEANBASE_SHARE_STAT_OB_FM_SKETCH_UTILS_H_

#include "lib/allocator/ob_allocator.h"
#include "share/stat/hive/ob_hive_fm_sketch.h"

namespace oceanbase {
namespace share {

class ObHiveFMSketchUtils {
public:
  // Magic string to identify serialized FMSketch stream
  static const uint8_t MAGIC[2];

  // Serialization format:
  // - 2 bytes: Magic string ("FM")
  // - 2 bytes: Number of bit vectors
  // - For each bit vector: 4 bytes (since BIT_VECTOR_SIZE=31, can use 32-bit
  // integer)

  /**
   * Serialize FMSketch to buffer
   * @param buf Buffer to write to
   * @param buf_len Buffer length
   * @param pos Current position in buffer (will be updated)
   * @param fm_sketch FMSketch object to serialize
   * @return OB_SUCCESS on success, error code otherwise
   */
  static int serialize_fm_sketch(char *buf, const int64_t buf_len, int64_t &pos,
                                 const ObHiveFMSketch *fm_sketch);

  /**
   * Deserialize FMSketch from buffer
   * @param buf Buffer to read from
   * @param data_len Buffer data length
   * @param pos Current position in buffer (will be updated)
   * @param fm_sketch Output FMSketch object
   * @return OB_SUCCESS on success, error code otherwise
   */
  static int deserialize_fm_sketch(const char *buf, const int64_t data_len,
                                   int64_t &pos, ObHiveFMSketch &fm_sketch);

  /**
   * Get serialized size of FMSketch
   * @param fm_sketch FMSketch object
   * @param serialize_size Output size
   * @return OB_SUCCESS on success, error code otherwise
   */
  static int get_serialize_size(const ObHiveFMSketch *fm_sketch,
                                int64_t &serialize_size);

  /**
   * Write a single bit vector to buffer (4 bytes)
   * Since BIT_VECTOR_SIZE is 31, we can use 32 bits to represent a BitVector
   * @param buf Buffer to write to
   * @param buf_len Buffer length
   * @param pos Current position in buffer (will be updated)
   * @param bit_vector Bit vector to write
   * @return OB_SUCCESS on success, error code otherwise
   */
  static int write_bit_vector(char *buf, const int64_t buf_len, int64_t &pos,
                              const ObHiveFMSketch::BitVectorType *bit_vector);

  /**
   * Read a single bit vector from buffer (4 bytes)
   * @param buf Buffer to read from
   * @param data_len Buffer data length
   * @param pos Current position in buffer (will be updated)
   * @param bit_vector Output bit vector (should be initialized)
   * @return OB_SUCCESS on success, error code otherwise
   */
  static int read_bit_vector(const char *buf, const int64_t data_len,
                             int64_t &pos,
                             ObHiveFMSketch::BitVectorType *bit_vector);
  /**
   * Check magic string in buffer
   * @param buf Buffer to read from
   * @param data_len Buffer data length
   * @param pos Current position in buffer (will be updated)
   * @return OB_SUCCESS on success, error code otherwise
   */
  static int check_magic_string(const char *buf, const int64_t data_len,
                                int64_t &pos);

private:
  DISALLOW_COPY_AND_ASSIGN(ObHiveFMSketchUtils);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_STAT_OB_FM_SKETCH_UTILS_H_