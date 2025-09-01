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

#include "share/stat/hive/ob_hive_hll_utils.h"
#include "lib/container/ob_se_array.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_sort.h"
#include "share/stat/hive/ob_hive_hll_constants.h"

#define USING_LOG_PREFIX SQL_ENG

namespace oceanbase {
namespace share {

const int8_t ObHiveHLLUtils::MAGIC[3] = {'H', 'L', 'L'};

int ObHiveHLLUtils::serialize_hll(char *buf, const int64_t buf_len,
                                  int64_t &pos, ObHiveHLL *hll) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || OB_ISNULL(hll) || pos < 0 || buf_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid arguments", K(ret), KP(buf), KP(hll), K(pos), K(buf_len));
  } else {
    // Write magic string
    if (pos + MAGIC_SIZE > buf_len) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("Buffer overflow when writing magic", K(ret), K(pos),
               K(buf_len));
    } else {
      MEMCPY(buf + pos, MAGIC, MAGIC_SIZE);
      pos += MAGIC_SIZE;
    }

    // Write fourth byte of header (encoding info)
    int8_t fourth_byte = 0;
    if (OB_SUCC(ret)) {
      if (pos + 1 > buf_len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("Buffer overflow when writing header", K(ret), K(pos),
                 K(buf_len));
      } else {
        int32_t p = hll->get_num_register_index_bits();
        fourth_byte = (p & 0x0F) << 4; // 4 bits for p

        int32_t bit_width = 0;
        ObHiveHLL::EncodingType encoding = hll->get_encoding();

        // Determine bit width for bit packing and encode it in header
        if (encoding == ObHiveHLL::EncodingType::DENSE) {
          ObHiveHLLDenseRegister *dense_reg = hll->get_dense_register();
          if (OB_NOT_NULL(dense_reg)) {
            int32_t max_reg_val = dense_reg->get_max_register_value();
            bit_width = get_bit_width(max_reg_val);

            // The max value of number of zeroes for 64 bit hash can be encoded
            // using only 6 bits. So we will disable bit packing for any values
            // >6
            if (bit_width > 6) {
              fourth_byte |= 7; // 111 = no bit packing
              bit_width = 8;
            } else {
              fourth_byte |= (bit_width & 7); // 3 bits for encoding
            }
          }
        }
        // For SPARSE encoding, fourth_byte already has 000 in encoding bits

        buf[pos++] = fourth_byte;
      }
    }

    // Write estimated count
    if (OB_SUCC(ret)) {
      uint64_t est_count =
          static_cast<uint64_t>(hll->estimate_num_distinct_values());
      if (OB_FAIL(write_vulong(buf, buf_len, pos, est_count))) {
        LOG_WARN("Failed to write estimated count", K(ret));
      }
    }

    // Serialize dense/sparse registers
    if (OB_SUCC(ret)) {
      if (hll->get_encoding() == ObHiveHLL::EncodingType::DENSE) {
        ObHiveHLLDenseRegister *dense_reg = hll->get_dense_register();
        if (OB_ISNULL(dense_reg)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Dense register is null", K(ret));
        } else {
          const int8_t *register_data = dense_reg->get_register();
          int32_t register_size = dense_reg->size();
          int32_t bit_width = (fourth_byte & 7) == 7 ? 8 : (fourth_byte & 7);

          if (OB_FAIL(bitpack_hll_register(buf, buf_len, pos, register_data,
                                           register_size, bit_width))) {
            LOG_WARN("Failed to bitpack dense register", K(ret));
          }
        }
      } else {
        ObHiveHLLSparseRegister *sparse_reg = hll->get_sparse_register();
        if (OB_ISNULL(sparse_reg)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Sparse register is null", K(ret));
        } else if (OB_FAIL(serialize_sparse_register(buf, buf_len, pos,
                                                     sparse_reg))) {
          LOG_WARN("Failed to serialize sparse register", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObHiveHLLUtils::deserialize_hll(const char *buf, const int64_t buf_len,
                                    int64_t &pos,
                                    common::ObIAllocator &allocator,
                                    ObHiveHLL *&hll) {
  int ret = OB_SUCCESS;
  hll = nullptr;

  if (OB_ISNULL(buf) || pos < 0 || buf_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid arguments", K(ret), KP(buf), K(pos), K(buf_len));
  } else if (OB_FAIL(check_magic_string(buf, buf_len, pos))) {
    LOG_WARN("Failed to check magic string", K(ret));
  } else {
    // Read fourth byte
    if (pos >= buf_len) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("Buffer underflow when reading header", K(ret), K(pos),
               K(buf_len));
    } else {
      int8_t fourth_byte = buf[pos++];
      int32_t p = (fourth_byte >> 4) & 0x0F;

      // Read type of encoding
      int32_t enc = fourth_byte & 7;
      ObHiveHLL::EncodingType encoding;
      int32_t bit_size = 0;

      if (enc == 0) {
        encoding = ObHiveHLL::EncodingType::SPARSE;
      } else if (enc > 0 && enc < 7) {
        bit_size = enc;
        encoding = ObHiveHLL::EncodingType::DENSE;
      } else {
        // Bit packing disabled
        bit_size = 8;
        encoding = ObHiveHLL::EncodingType::DENSE;
      }

      // Read estimated count
      uint64_t est_count = 0;
      if (OB_FAIL(read_vulong(buf, buf_len, pos, est_count))) {
        LOG_WARN("Failed to read estimated count", K(ret));
      } else {
        // Create HLL based on encoding type
        ObHiveHLL::Builder builder;
        builder.set_num_register_index_bits(p).set_encoding(encoding);

        if (encoding == ObHiveHLL::EncodingType::DENSE) {
          if (bit_size == 8) {
            builder.enable_bit_packing(false);
          } else {
            builder.enable_bit_packing(true);
          }
        }

        if (OB_FAIL(builder.build(allocator, hll))) {
          LOG_WARN("Failed to build HLL", K(ret));
        } else if (OB_ISNULL(hll)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("HLL is null after build", K(ret));
        } else {
          // Deserialize register data
          if (encoding == ObHiveHLL::EncodingType::SPARSE) {
            uint64_t num_entries = 0;
            ObHiveHLLSparseRegister *sparse_reg = hll->get_sparse_register();
            if (OB_FAIL(read_vulong(buf, buf_len, pos, num_entries))) {
              LOG_WARN("Failed to read number of sparse entries", K(ret));
            } else if (OB_FAIL(deserialize_sparse_register(
                           buf, buf_len, pos, num_entries, p, sparse_reg))) {
              LOG_WARN("Failed to deserialize sparse register", K(ret));
            }
          } else {
            // Dense encoding
            int32_t m = 1 << p;
            int8_t *register_data = nullptr;
            if (OB_FAIL(unpack_hll_register(buf, buf_len, pos, m, bit_size,
                                            allocator, register_data))) {
              LOG_WARN("Failed to unpack dense register", K(ret));
            } else if (OB_FAIL(hll->set_dense_register(register_data, m))) {
              LOG_WARN("Failed to set dense register", K(ret));
            } else if (OB_NOT_NULL(register_data)) {
              allocator.free(register_data);
              register_data = nullptr;
            }
          }

          if (OB_SUCC(ret)) {
            hll->set_count(static_cast<int64_t>(est_count));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(hll)) {
    hll->~ObHiveHLL();
    allocator.free(hll);
    hll = nullptr;
  }

  return ret;
}

int64_t ObHiveHLLUtils::get_serialize_size(ObHiveHLL *hll) {
  int64_t size = 0;

  if (OB_NOT_NULL(hll)) {
    size += HEADER_SIZE; // Magic + fourth byte

    // Estimated count size
    uint64_t est_count =
        static_cast<uint64_t>(hll->estimate_num_distinct_values());
    size += get_vulong_size(est_count);

    if (hll->get_encoding() == ObHiveHLL::EncodingType::DENSE) {
      ObHiveHLLDenseRegister *dense_reg = hll->get_dense_register();
      if (OB_NOT_NULL(dense_reg)) {
        int32_t register_size = dense_reg->size();
        int32_t max_reg_val = dense_reg->get_max_register_value();
        int32_t bit_width = get_bit_width(max_reg_val);

        if (bit_width > 6) {
          bit_width = 8;
        }

        if (bit_width == 8) {
          size += register_size; // Fast path
        } else {
          // Bit packed size calculation
          int64_t total_bits = static_cast<int64_t>(register_size) * bit_width;
          size += (total_bits + 7) / 8; // Round up to bytes
        }
      }
    } else {
      ObHiveHLLSparseRegister *sparse_reg = hll->get_sparse_register();
      if (OB_NOT_NULL(sparse_reg)) {
        size += get_vulong_size(sparse_reg->get_size()); // Number of entries

        // Estimate size of delta-encoded sparse map
        // This is an approximation since we don't know the actual deltas
        int64_t map_size = sparse_reg->get_size();
        size += map_size *
                5; // Assume average 5 bytes per varint (conservative estimate)
      }
    }
  }

  return size;
}

int ObHiveHLLUtils::get_estimated_count_from_serialized_hll(
    const char *buf, const int64_t buf_len, int64_t &pos,
    int64_t &estimated_count) {
  int ret = OB_SUCCESS;
  estimated_count = 0;

  if (OB_ISNULL(buf) || pos < 0 || buf_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid arguments", K(ret), KP(buf), K(pos), K(buf_len));
  } else if (OB_FAIL(check_magic_string(buf, buf_len, pos))) {
    LOG_WARN("Failed to check magic string", K(ret));
  } else {
    // Skip fourth byte
    if (pos >= buf_len) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("Buffer underflow when skipping header", K(ret), K(pos),
               K(buf_len));
    } else {
      pos++; // Skip fourth byte

      // Read estimated count
      uint64_t est_count = 0;
      if (OB_FAIL(read_vulong(buf, buf_len, pos, est_count))) {
        LOG_WARN("Failed to read estimated count", K(ret));
      } else {
        estimated_count = static_cast<int64_t>(est_count);
      }
    }
  }

  return ret;
}

float ObHiveHLLUtils::get_relative_error(int64_t actual_count,
                                         int64_t estimated_count) {
  float ret = 0.0f;
  if (actual_count == 0) {
    ret = 0.0f;
  } else {
    ret = (1.0f - (static_cast<float>(estimated_count) /
                   static_cast<float>(actual_count))) *
          100.0f;
  }
  return ret;
}

int ObHiveHLLUtils::write_vulong(char *buf, const int64_t buf_len, int64_t &pos,
                                 uint64_t value) {
  int ret = OB_SUCCESS;

  while (OB_SUCC(ret)) {
    if ((value & ~0x7F) == 0) {
      // Last byte
      if (pos >= buf_len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("Buffer overflow when writing vulong", K(ret), K(pos),
                 K(buf_len));
      } else {
        buf[pos++] = static_cast<int8_t>(value);
        break;
      }
    } else {
      // More bytes to follow
      if (pos >= buf_len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("Buffer overflow when writing vulong", K(ret), K(pos),
                 K(buf_len));
      } else {
        buf[pos++] = static_cast<int8_t>(0x80 | (value & 0x7F));
        value >>= 7;
      }
    }
  }

  return ret;
}

int ObHiveHLLUtils::read_vulong(const char *buf, const int64_t buf_len,
                                int64_t &pos, uint64_t &value) {
  int ret = OB_SUCCESS;
  value = 0;

  uint64_t b = 0;
  int32_t offset = 0;

  do {
    if (pos >= buf_len) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("Buffer underflow when reading vulong", K(ret), K(pos),
               K(buf_len));
    } else {
      b = static_cast<uint8_t>(buf[pos++]);
      value |= (0x7F & b) << offset;
      offset += 7;
    }
  } while (OB_SUCC(ret) && b >= 0x80);

  return ret;
}

int64_t ObHiveHLLUtils::get_vulong_size(uint64_t value) {
  int64_t size = 0;

  do {
    size++;
    value >>= 7;
  } while (value != 0);

  return size;
}

int ObHiveHLLUtils::bitpack_hll_register(char *buf, const int64_t buf_len,
                                         int64_t &pos,
                                         const int8_t *register_data,
                                         int32_t register_size,
                                         int32_t bit_width) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || OB_ISNULL(register_data) || pos < 0 || buf_len <= 0 ||
      register_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid arguments", K(ret), KP(buf), KP(register_data), K(pos),
             K(buf_len), K(register_size));
  } else if (bit_width == 8) {
    // Fast path - no bit packing
    if (OB_FAIL(
            fast_path_write(buf, buf_len, pos, register_data, register_size))) {
      LOG_WARN("Failed to write register data", K(ret));
    }
  } else {
    // Bit packing
    int32_t bits_left = 8;
    int8_t current = 0;

    for (int32_t i = 0; OB_SUCC(ret) && i < register_size; ++i) {
      int8_t value = register_data[i];
      int32_t bits_to_write = bit_width;

      while (OB_SUCC(ret) && bits_to_write > bits_left) {
        // Add the bits to the bottom of the current word
        current |= value >> (bits_to_write - bits_left);
        // Subtract out the bits we just added
        bits_to_write -= bits_left;
        // Zero out the bits above bits_to_write
        value &= (1 << bits_to_write) - 1;

        if (pos >= buf_len) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("Buffer overflow when bit packing", K(ret), K(pos),
                   K(buf_len));
        } else {
          buf[pos++] = current;
          current = 0;
          bits_left = 8;
        }
      }

      if (OB_SUCC(ret)) {
        bits_left -= bits_to_write;
        current |= value << bits_left;
        if (bits_left == 0) {
          if (pos >= buf_len) {
            ret = OB_SIZE_OVERFLOW;
            LOG_WARN("Buffer overflow when bit packing", K(ret), K(pos),
                     K(buf_len));
          } else {
            buf[pos++] = current;
            current = 0;
            bits_left = 8;
          }
        }
      }
    }

    // Write any remaining bits
    if (OB_SUCC(ret) && bits_left != 8) {
      if (pos >= buf_len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("Buffer overflow when writing remaining bits", K(ret), K(pos),
                 K(buf_len));
      } else {
        buf[pos++] = current;
      }
    }
  }

  return ret;
}

int ObHiveHLLUtils::unpack_hll_register(const char *buf, const int64_t buf_len,
                                        int64_t &pos, int32_t register_size,
                                        int32_t bit_width,
                                        common::ObIAllocator &allocator,
                                        int8_t *&register_data) {
  int ret = OB_SUCCESS;
  register_data = nullptr;

  if (OB_ISNULL(buf) || pos < 0 || buf_len <= 0 || register_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid arguments", K(ret), KP(buf), K(pos), K(buf_len),
             K(register_size));
  } else if (bit_width == 8) {
    // Fast path - no bit packing
    if (OB_FAIL(fast_path_read(buf, buf_len, pos, register_size, allocator,
                               register_data))) {
      LOG_WARN("Failed to read register data", K(ret));
    }
  } else {
    // Bit unpacking
    void *ptr = allocator.alloc(register_size * sizeof(int8_t));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for register", K(ret),
               K(register_size));
    } else {
      register_data = static_cast<int8_t *>(ptr);

      int32_t mask = (1 << bit_width) - 1;
      int32_t bits_left = 8;
      int8_t current = 0;

      if (pos < buf_len) {
        current = buf[pos++];
      } else {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("Buffer underflow when reading for unpacking", K(ret), K(pos),
                 K(buf_len));
      }

      for (int32_t i = 0; OB_SUCC(ret) && i < register_size; ++i) {
        int8_t result = 0;
        int32_t bits_left_to_read = bit_width;

        while (OB_SUCC(ret) && bits_left_to_read > bits_left) {
          result <<= bits_left;
          result |= current & ((1 << bits_left) - 1);
          bits_left_to_read -= bits_left;

          if (pos < buf_len) {
            current = buf[pos++];
            bits_left = 8;
          } else {
            ret = OB_SIZE_OVERFLOW;
            LOG_WARN("Buffer underflow when unpacking", K(ret), K(pos),
                     K(buf_len));
          }
        }

        if (OB_SUCC(ret) && bits_left_to_read > 0) {
          result <<= bits_left_to_read;
          bits_left -= bits_left_to_read;
          result |= (current >> bits_left) & ((1 << bits_left_to_read) - 1);
        }

        if (OB_SUCC(ret)) {
          register_data[i] = result & mask;
        }
      }
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(register_data)) {
    allocator.free(register_data);
    register_data = nullptr;
  }

  return ret;
}

struct ObHiveSpareKeyVale {
  ObHiveSpareKeyVale() : key_(0), value_(0) {}
  ObHiveSpareKeyVale(int32_t key, int8_t value) : key_(key), value_(value) {}
  ObHiveSpareKeyVale(const ObHiveSpareKeyVale &other)
      : key_(other.key_), value_(other.value_) {}
  TO_STRING_KV(K_(key), K_(value));
  int32_t key_;
  int8_t value_;
};

struct ObHiveSpareKeyValeCmp {
  bool operator()(const ObHiveSpareKeyVale &a,
                  const ObHiveSpareKeyVale &b) const {
    return a.key_ < b.key_;
  }
};

int ObHiveHLLUtils::serialize_sparse_register(
    char *buf, const int64_t buf_len, int64_t &pos,
    const ObHiveHLLSparseRegister *sparse_register) {
  int ret = OB_SUCCESS;
  common::ObSEArray<ObHiveSpareKeyVale, 1> sorted_keys;
  if (OB_ISNULL(buf) || OB_ISNULL(sparse_register) || pos < 0 || buf_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid arguments", K(ret), KP(buf), KP(sparse_register), K(pos),
             K(buf_len));
  } else {
    const ObHiveHLLSparseRegister::SparseMapType &sparse_map =
        sparse_register->get_sparse_map();

    // Write the number of elements in sparse map
    uint64_t map_size = static_cast<uint64_t>(sparse_map.size());
    if (OB_FAIL(write_vulong(buf, buf_len, pos, map_size))) {
      LOG_WARN("Failed to write sparse map size", K(ret));
    } else {
      // Compute deltas and write the values as varints
      uint32_t prev = 0;
      bool first = true;

      // We need to iterate through the map in sorted order
      // Since hashmap doesn't guarantee order, we need to sort the keys
      if (OB_FAIL(sorted_keys.reserve(sparse_map.size()))) {
        LOG_WARN("Failed to init sorted keys", K(ret));
      } else {
        for (ObHiveHLLSparseRegister::SparseMapType::const_iterator iter =
                 sparse_map.begin();
             OB_SUCC(ret) && iter != sparse_map.end(); ++iter) {
          if (OB_FAIL(sorted_keys.push_back(
                  ObHiveSpareKeyVale(iter->first, iter->second)))) {
            LOG_WARN("Failed to add key to sorted array", K(ret));
          }
        }
      }

      if (OB_SUCC(ret) && sorted_keys.count() > 0) {
        // Sort the keys
        lib::ob_sort(&sorted_keys.at(0),
                     &sorted_keys.at(0) + sorted_keys.count(),
                     ObHiveSpareKeyValeCmp());

        // Write sorted entries
        for (int64_t i = 0; OB_SUCC(ret) && i < sorted_keys.count(); ++i) {
          int32_t key = sorted_keys.at(i).key_;
          int8_t value = sorted_keys.at(i).value_;

          uint32_t current = (static_cast<uint32_t>(key)
                              << ObHiveHLLConstants::Q_PRIME_VALUE) |
                             static_cast<uint32_t>(value);

          if (first) {
            if (OB_FAIL(write_vulong(buf, buf_len, pos, current))) {
              LOG_WARN("Failed to write first sparse entry", K(ret));
            } else {
              prev = current;
              first = false;
            }
          } else {
            uint32_t delta = current - prev;
            if (OB_FAIL(write_vulong(buf, buf_len, pos, delta))) {
              LOG_WARN("Failed to write sparse delta", K(ret));
            } else {
              prev = current;
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObHiveHLLUtils::deserialize_sparse_register(
    const char *buf, const int64_t buf_len, int64_t &pos, int64_t num_entries,
    int32_t p, ObHiveHLLSparseRegister *sparse_register) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || pos < 0 || buf_len <= 0 || num_entries < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid arguments", K(ret), KP(buf), K(pos), K(buf_len),
             K(num_entries));
  } else if (OB_ISNULL(sparse_register)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Sparse register is null", K(ret));
  } else if (num_entries > 0) {
    // Read and reconstruct sparse map from delta encoded input
    uint64_t prev_val = 0;
    if (OB_FAIL(read_vulong(buf, buf_len, pos, prev_val))) {
      LOG_WARN("Failed to read first sparse entry", K(ret));
    } else {
      uint32_t prev = static_cast<uint32_t>(prev_val);
      int32_t key = prev >> ObHiveHLLConstants::Q_PRIME_VALUE;
      int8_t value = prev & 0x3F;
      bool updated = false;

      if (OB_FAIL(sparse_register->set(key, value, updated))) {
        LOG_WARN("Failed to set sparse register entry", K(ret), K(key),
                 K(value));
      } else {
        // Read remaining entries
        for (int64_t i = 1; i < num_entries && OB_SUCC(ret); ++i) {
          uint64_t delta_val = 0;
          if (OB_FAIL(read_vulong(buf, buf_len, pos, delta_val))) {
            LOG_WARN("Failed to read sparse delta", K(ret));
          } else {
            uint32_t delta = static_cast<uint32_t>(delta_val);
            uint32_t current = prev + delta;

            key = current >> ObHiveHLLConstants::Q_PRIME_VALUE;
            value = current & 0x3F;

            if (OB_FAIL(sparse_register->set(key, value, updated))) {
              LOG_WARN("Failed to set sparse register entry", K(ret), K(key),
                       K(value));
            } else {
              prev = current;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObHiveHLLUtils::check_magic_string(const char *buf, const int64_t buf_len,
                                       int64_t &pos) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || pos < 0 || buf_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid arguments", K(ret), KP(buf), K(pos), K(buf_len));
  } else if (pos + MAGIC_SIZE > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("Buffer underflow when checking magic", K(ret), K(pos),
             K(buf_len));
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < MAGIC_SIZE; ++i) {
      if (buf[pos + i] != MAGIC[i]) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Magic string mismatch", K(ret), K(i), K(buf[pos + i]),
                 K(MAGIC[i]));
      }
    }

    if (OB_SUCC(ret)) {
      pos += MAGIC_SIZE;
    }
  }

  return ret;
}

int32_t ObHiveHLLUtils::get_bit_width(int32_t val) {
  int32_t count = 0;
  while (val != 0) {
    count++;
    val = static_cast<uint8_t>(val) >> 1;
  }
  return count;
}

int ObHiveHLLUtils::fast_path_write(char *buf, const int64_t buf_len,
                                    int64_t &pos, const int8_t *data,
                                    int32_t data_len) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || OB_ISNULL(data) || pos < 0 || buf_len <= 0 ||
      data_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid arguments", K(ret), KP(buf), KP(data), K(pos), K(buf_len),
             K(data_len));
  } else if (pos + data_len > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("Buffer overflow when writing data", K(ret), K(pos), K(data_len),
             K(buf_len));
  } else {
    MEMCPY(buf + pos, data, data_len);
    pos += data_len;
  }

  return ret;
}

int ObHiveHLLUtils::fast_path_read(const char *buf, const int64_t buf_len,
                                   int64_t &pos, int32_t data_len,
                                   common::ObIAllocator &allocator,
                                   int8_t *&data) {
  int ret = OB_SUCCESS;
  data = nullptr;

  if (OB_ISNULL(buf) || pos < 0 || buf_len <= 0 || data_len <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid arguments", K(ret), KP(buf), K(pos), K(buf_len),
             K(data_len));
  } else if (pos + data_len > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("Buffer underflow when reading data", K(ret), K(pos), K(data_len),
             K(buf_len));
  } else {
    void *ptr = allocator.alloc(data_len * sizeof(int8_t));
    if (OB_ISNULL(ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memory for data", K(ret), K(data_len));
    } else {
      data = static_cast<int8_t *>(ptr);
      MEMCPY(data, buf + pos, data_len);
      pos += data_len;
    }
  }

  return ret;
}

} // namespace share
} // namespace oceanbase