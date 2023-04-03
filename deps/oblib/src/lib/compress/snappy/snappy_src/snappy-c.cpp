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

#include "snappy.h"
#include "snappy-c.h"

extern "C" {

snappy_status snappy_compress(const char* input,
                              size_t input_length,
                              char* compressed,
                              size_t *compressed_length) {
  if (*compressed_length < snappy_max_compressed_length(input_length)) {
    return SNAPPY_BUFFER_TOO_SMALL;
  }
  snappy::RawCompress(input, input_length, compressed, compressed_length);
  return SNAPPY_OK;
}

snappy_status snappy_uncompress(const char* compressed,
                                size_t compressed_length,
                                char* uncompressed,
                                size_t* uncompressed_length) {
  size_t real_uncompressed_length;
  if (!snappy::GetUncompressedLength(compressed,
                                     compressed_length,
                                     &real_uncompressed_length)) {
    return SNAPPY_INVALID_INPUT;
  }
  if (*uncompressed_length < real_uncompressed_length) {
    return SNAPPY_BUFFER_TOO_SMALL;
  }
  if (!snappy::RawUncompress(compressed, compressed_length, uncompressed)) {
    return SNAPPY_INVALID_INPUT;
  }
  *uncompressed_length = real_uncompressed_length;
  return SNAPPY_OK;
}

size_t snappy_max_compressed_length(size_t source_length) {
  return snappy::MaxCompressedLength(source_length);
}

snappy_status snappy_uncompressed_length(const char *compressed,
                                         size_t compressed_length,
                                         size_t *result) {
  if (snappy::GetUncompressedLength(compressed,
                                    compressed_length,
                                    result)) {
    return SNAPPY_OK;
  } else {
    return SNAPPY_INVALID_INPUT;
  }
}

snappy_status snappy_validate_compressed_buffer(const char *compressed,
                                                size_t compressed_length) {
  if (snappy::IsValidCompressedBuffer(compressed, compressed_length)) {
    return SNAPPY_OK;
  } else {
    return SNAPPY_INVALID_INPUT;
  }
}

}  // extern "C"
