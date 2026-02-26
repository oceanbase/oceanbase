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

#ifndef OCEANBASE_SHARE_OB_HIVE_HLL_UTILS_H_
#define OCEANBASE_SHARE_OB_HIVE_HLL_UTILS_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/ob_define.h"
#include "share/stat/hive/ob_hive_hll.h"

namespace oceanbase {
namespace share {

/**
 * @brief HyperLogLog serialization utilities for OceanBase
 *
 * HyperLogLog is serialized using the following format:
 *
 * |-4 byte-|------varulong----|varulong (optional)|----------|
 * ---------------------------------------------------------
 * | header | estimated-count  | register-length   | register |
 * ---------------------------------------------------------
 *
 * 4 byte header is encoded like below:
 * 3 bytes - HLL magic string to identify serialized stream
 * 4 bits  - p (number of bits to be used as register index)
 * 1       - spare bit (not used)
 * 3 bits  - encoding (000 - sparse, 001..110 - n bit packing, 111 - no bit
 * packing)
 *
 * Followed by header are 3 fields that are required for reconstruction:
 * Estimated count - variable length long to store last computed estimated
 * count. This is just for quick lookup without deserializing registers Register
 * length - number of entries in the register (required only for sparse
 * representation. For bit-packing, the register length can be found from p)
 */
class ObHiveHLLUtils {
public:
  // Magic string to identify HLL serialized data
  static const int8_t MAGIC[3];
  static const int32_t MAGIC_SIZE = 3;
  static const int32_t HEADER_SIZE = 4;

  /**
   * @brief Serialize HyperLogLog to buffer
   * @param buf output buffer
   * @param buf_len buffer length
   * @param pos current position in buffer (input/output)
   * @param hll HyperLogLog to serialize
   * @return OB_SUCCESS on success, error code otherwise
   */
  static int serialize_hll(char *buf, const int64_t buf_len, int64_t &pos,
                           ObHiveHLL *hll);

  /**
   * @brief Deserialize HyperLogLog from buffer
   * @param buf input buffer
   * @param buf_len buffer length
   * @param pos current position in buffer (input/output)
   * @param allocator memory allocator
   * @param hll output HyperLogLog (output parameter)
   * @return OB_SUCCESS on success, error code otherwise
   */
  static int deserialize_hll(const char *buf, const int64_t buf_len,
                             int64_t &pos, common::ObIAllocator &allocator,
                             ObHiveHLL *&hll);

  /**
   * @brief Get serialized size of HyperLogLog
   * @param hll HyperLogLog to calculate size for
   * @return serialized size in bytes
   */
  static int64_t get_serialize_size(ObHiveHLL *hll);

  /**
   * @brief Get estimated count from serialized HLL without full deserialization
   * @param buf input buffer
   * @param buf_len buffer length
   * @param pos current position in buffer (input/output)
   * @param estimated_count output estimated count
   * @return OB_SUCCESS on success, error code otherwise
   */
  static int get_estimated_count_from_serialized_hll(const char *buf,
                                                     const int64_t buf_len,
                                                     int64_t &pos,
                                                     int64_t &estimated_count);

  /**
   * @brief Calculate relative error between actual and estimated cardinality
   * @param actual_count actual count
   * @param estimated_count estimated count
   * @return relative error as percentage
   */
  static float get_relative_error(int64_t actual_count,
                                  int64_t estimated_count);

private:
  /**
   * @brief Write variable length unsigned long to buffer
   * @param buf output buffer
   * @param buf_len buffer length
   * @param pos current position (input/output)
   * @param value value to write
   * @return OB_SUCCESS on success, error code otherwise
   */
  static int write_vulong(char *buf, const int64_t buf_len, int64_t &pos,
                          uint64_t value);

  /**
   * @brief Read variable length unsigned long from buffer
   * @param buf input buffer
   * @param buf_len buffer length
   * @param pos current position (input/output)
   * @param value output value
   * @return OB_SUCCESS on success, error code otherwise
   */
  static int read_vulong(const char *buf, const int64_t buf_len, int64_t &pos,
                         uint64_t &value);

  /**
   * @brief Get variable length encoding size
   * @param value value to calculate size for
   * @return size in bytes
   */
  static int64_t get_vulong_size(uint64_t value);

  /**
   * @brief Bit pack HLL register array to buffer
   * @param buf output buffer
   * @param buf_len buffer length
   * @param pos current position (input/output)
   * @param register_data register array
   * @param register_size size of register array
   * @param bit_width bit width for packing
   * @return OB_SUCCESS on success, error code otherwise
   */
  static int bitpack_hll_register(char *buf, const int64_t buf_len,
                                  int64_t &pos, const int8_t *register_data,
                                  int32_t register_size, int32_t bit_width);

  /**
   * @brief Unpack bit packed HLL register from buffer
   * @param buf input buffer
   * @param buf_len buffer length
   * @param pos current position (input/output)
   * @param register_size expected register size
   * @param bit_width bit width for unpacking
   * @param allocator memory allocator
   * @param register_data output register array
   * @return OB_SUCCESS on success, error code otherwise
   */
  static int unpack_hll_register(const char *buf, const int64_t buf_len,
                                 int64_t &pos, int32_t register_size,
                                 int32_t bit_width,
                                 common::ObIAllocator &allocator,
                                 int8_t *&register_data);

  /**
   * @brief Serialize sparse register map
   * @param buf output buffer
   * @param buf_len buffer length
   * @param pos current position (input/output)
   * @param sparse_register sparse register to serialize
   * @return OB_SUCCESS on success, error code otherwise
   */
  static int
  serialize_sparse_register(char *buf, const int64_t buf_len, int64_t &pos,
                            const ObHiveHLLSparseRegister *sparse_register);

  /**
   * @brief Deserialize sparse register map
   * @param buf input buffer
   * @param buf_len buffer length
   * @param pos current position (input/output)
   * @param num_entries number of entries to read
   * @param p p parameter for register
   * @param allocator memory allocator
   * @param sparse_register output sparse register
   * @return OB_SUCCESS on success, error code otherwise
   */
  static int
  deserialize_sparse_register(const char *buf, const int64_t buf_len,
                              int64_t &pos, int64_t num_entries, int32_t p,
                              ObHiveHLLSparseRegister *sparse_register);

  /**
   * @brief Check magic string in buffer
   * @param buf input buffer
   * @param buf_len buffer length
   * @param pos current position (input/output)
   * @return OB_SUCCESS if magic string matches, error code otherwise
   */
  static int check_magic_string(const char *buf, const int64_t buf_len,
                                int64_t &pos);

  /**
   * @brief Get minimum bits required to encode the specified value
   * @param val input value
   * @return number of bits required
   */
  static int32_t get_bit_width(int32_t val);

  /**
   * @brief Fast path write for byte array (no bit packing)
   * @param buf output buffer
   * @param buf_len buffer length
   * @param pos current position (input/output)
   * @param data data to write
   * @param data_len data length
   * @return OB_SUCCESS on success, error code otherwise
   */
  static int fast_path_write(char *buf, const int64_t buf_len, int64_t &pos,
                             const int8_t *data, int32_t data_len);

  /**
   * @brief Fast path read for byte array (no bit packing)
   * @param buf input buffer
   * @param buf_len buffer length
   * @param pos current position (input/output)
   * @param data_len data length to read
   * @param allocator memory allocator
   * @param data output data array
   * @return OB_SUCCESS on success, error code otherwise
   */
  static int fast_path_read(const char *buf, const int64_t buf_len,
                            int64_t &pos, int32_t data_len,
                            common::ObIAllocator &allocator, int8_t *&data);

private:
  ObHiveHLLUtils() = delete;
  ~ObHiveHLLUtils() = delete;
  DISALLOW_COPY_AND_ASSIGN(ObHiveHLLUtils);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_HIVE_HLL_UTILS_H_