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

#include "share/stat/hive/ob_hive_fm_sketch_utils.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"

#define USING_LOG_PREFIX SQL_ENG

namespace oceanbase {
namespace share {

const uint8_t ObHiveFMSketchUtils::MAGIC[2] = {'F', 'M'};

int ObHiveFMSketchUtils::serialize_fm_sketch(char *buf, const int64_t buf_len,
                                             int64_t &pos,
                                             const ObHiveFMSketch *fm_sketch) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || OB_ISNULL(fm_sketch) || buf_len <= 0 || pos < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid arguments", K(ret), KP(buf), KP(fm_sketch), K(buf_len),
             K(pos));
  } else {
    // Write magic string (2 bytes)
    if (pos + 2 > buf_len) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("Buffer overflow when writing magic", K(ret), K(pos),
               K(buf_len));
    } else {
      buf[pos] = static_cast<char>(MAGIC[0]);
      buf[pos + 1] = static_cast<char>(MAGIC[1]);
      pos += 2;
    }

    // Write number of bit vectors (2 bytes)
    if (OB_SUCC(ret)) {
      int32_t num_bit_vectors = fm_sketch->get_num_bit_vectors();
      if (num_bit_vectors > 65535) { // 2^16 - 1
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Too many bit vectors", K(ret), K(num_bit_vectors));
      } else if (pos + 2 > buf_len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("Buffer overflow when writing num_bit_vectors", K(ret), K(pos),
                 K(buf_len));
      } else {
        buf[pos] = static_cast<char>(num_bit_vectors & 0xFF);
        buf[pos + 1] = static_cast<char>((num_bit_vectors >> 8) & 0xFF);
        pos += 2;
      }
    }

    // Write each bit vector (4 bytes each)
    for (int32_t i = 0; OB_SUCC(ret) && i < fm_sketch->get_num_bit_vectors();
         ++i) {
      const ObHiveFMSketch::BitVectorType *bit_vector =
          &(fm_sketch->bit_vectors_[i]);
      if (OB_ISNULL(bit_vector)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Bit vector is null", K(ret), K(i));
      } else if (OB_FAIL(write_bit_vector(buf, buf_len, pos, bit_vector))) {
        LOG_WARN("Failed to write bit vector", K(ret), K(i));
      }
    }
  }

  return ret;
}

int ObHiveFMSketchUtils::deserialize_fm_sketch(const char *buf,
                                               const int64_t data_len,
                                               int64_t &pos,
                                               ObHiveFMSketch &fm_sketch) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || data_len <= 0 || pos < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid arguments", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    // Check magic string
    if (OB_FAIL(check_magic_string(buf, data_len, pos))) {
      LOG_WARN("Failed to check magic string", K(ret));
    }

    // Read number of bit vectors
    int32_t num_bit_vectors = 0;
    if (OB_SUCC(ret)) {
      if (pos + 2 > data_len) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("Buffer underflow when reading num_bit_vectors", K(ret),
                 K(pos), K(data_len));
      } else {
        num_bit_vectors = static_cast<uint8_t>(buf[pos]) |
                          (static_cast<uint8_t>(buf[pos + 1]) << 8);
        pos += 2;
      }
    }

    // initialize FMSketch
    if (OB_SUCC(ret)) {
      if (OB_FAIL(fm_sketch.init(num_bit_vectors))) {
        LOG_WARN("Failed to initialize FMSketch", K(ret), K(num_bit_vectors));
      }
    }

    // Read each bit vector
    for (int32_t i = 0; OB_SUCC(ret) && i < num_bit_vectors; ++i) {
      ObHiveFMSketch::BitVectorType *bit_vector = &(fm_sketch.bit_vectors_[i]);
      if (OB_ISNULL(bit_vector)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Bit vector is null", K(ret), K(i));
      } else if (OB_FAIL(read_bit_vector(buf, data_len, pos, bit_vector))) {
        LOG_WARN("Failed to read bit vector", K(ret), K(i));
      }
    }
  }

  return ret;
}

int ObHiveFMSketchUtils::get_serialize_size(const ObHiveFMSketch *fm_sketch,
                                            int64_t &serialize_size) {
  int ret = OB_SUCCESS;
  serialize_size = 0;

  if (OB_ISNULL(fm_sketch)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("FMSketch is null", K(ret));
  } else {
    // Magic string: 2 bytes
    // Number of bit vectors: 2 bytes
    // Each bit vector: 4 bytes
    serialize_size = 2 + 2 + (fm_sketch->get_num_bit_vectors() * 4);
  }

  return ret;
}

int ObHiveFMSketchUtils::write_bit_vector(
    char *buf, const int64_t buf_len, int64_t &pos,
    const ObHiveFMSketch::BitVectorType *bit_vector) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || OB_ISNULL(bit_vector) || buf_len <= 0 || pos < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid arguments", K(ret), KP(buf), KP(bit_vector), K(buf_len),
             K(pos));
  } else if (pos + 4 > buf_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("Buffer overflow when writing bit vector", K(ret), K(pos),
             K(buf_len));
  } else {
    // Since BIT_VECTOR_SIZE is 31, we can use a 32-bit integer to represent the
    // bit vector
    uint32_t bit_data = 0;

    // Pack all bits into a 32-bit integer
    for (int32_t bit_pos = 0; bit_pos < ObHiveFMSketch::BIT_VECTOR_SIZE;
         ++bit_pos) {
      if (bit_vector->has_member(bit_pos)) {
        bit_data |= (1U << bit_pos);
      }
    }

    // Write 4 bytes in little-endian format
    for (int32_t j = 0; j < 4; ++j) {
      buf[pos + j] = static_cast<char>((bit_data >> (8 * j)) & 0xFF);
    }
    pos += 4;
  }

  return ret;
}

int ObHiveFMSketchUtils::read_bit_vector(
    const char *buf, const int64_t data_len, int64_t &pos,
    ObHiveFMSketch::BitVectorType *bit_vector) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || OB_ISNULL(bit_vector) || data_len <= 0 || pos < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid arguments", K(ret), KP(buf), KP(bit_vector), K(data_len),
             K(pos));
  } else if (pos + 4 > data_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("Buffer underflow when reading bit vector", K(ret), K(pos),
             K(data_len));
  } else {
    // Reset bit vector first
    bit_vector->reset();

    // Read 4 bytes and reconstruct bit vector
    for (int32_t i = 0; OB_SUCC(ret) && i < 4; ++i) {
      uint8_t byte_val = static_cast<uint8_t>(buf[pos + i]);

      // Check each bit in this byte
      for (int32_t j = 0; OB_SUCC(ret) && j < 8; ++j) {
        if ((byte_val & (1 << j)) != 0) {
          int32_t bit_pos = j + 8 * i;
          // Only set bits within the valid range
          if (bit_pos < ObHiveFMSketch::BIT_VECTOR_SIZE) {
            if (OB_FAIL(bit_vector->add_member(bit_pos))) {
              LOG_WARN("Failed to add member to bit vector", K(ret),
                       K(bit_pos));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      pos += 4;
    }
  }

  return ret;
}

int ObHiveFMSketchUtils::check_magic_string(const char *buf,
                                            const int64_t data_len,
                                            int64_t &pos) {
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf) || data_len <= 0 || pos < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid arguments", K(ret), KP(buf), K(data_len), K(pos));
  } else if (pos + 2 > data_len) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("Buffer underflow when reading magic", K(ret), K(pos),
             K(data_len));
  } else {
    uint8_t magic[2];
    magic[0] = static_cast<uint8_t>(buf[pos]);
    magic[1] = static_cast<uint8_t>(buf[pos + 1]);

    if (magic[0] != MAGIC[0] || magic[1] != MAGIC[1]) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Invalid magic string for FMSketch", K(ret), K(MAGIC[0]),
               K(MAGIC[1]), K(magic[0]), K(magic[1]));
    } else {
      pos += 2;
    }
  }

  return ret;
}

} // namespace share
} // namespace oceanbase