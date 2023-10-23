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

#ifndef OB_BYTEORDER_H
#define OB_BYTEORDER_H

#include <stdint.h>
#include "lib/charset/ob_template_helper.h"
#include <netinet/in.h>

/*
 Functions for big-endian loads and stores. These are safe to use
 no matter what the compiler, CPU or alignment, and also with -fstrict-aliasing.

 The stores return a pointer just past the value that was written.
*/

inline uint16_t load16be(const char *ptr) {
  uint16_t val;
  memcpy(&val, ptr, sizeof(val));
  return ntohs(val);
}

inline uint32_t load32be(const char *ptr) {
  uint32_t val;
  memcpy(&val, ptr, sizeof(val));
  return ntohl(val);
}

__attribute__((always_inline)) inline char *store16be(char *ptr, uint16_t val) {
#if defined(_MSC_VER)
  // _byteswap_ushort is an intrinsic on MSVC, but htons is not.
  val = _byteswap_ushort(val);
#else
  val = htons(val);
#endif
  memcpy(ptr, &val, sizeof(val));
  return ptr + sizeof(val);
}

inline char *store32be(char *ptr, uint32_t val) {
  val = htonl(val);
  memcpy(ptr, &val, sizeof(val));
  return ptr + sizeof(val);
}

// Adapters for using unsigned char * instead of char *.

inline uint16_t load16be(const unsigned char *ptr) {
  return load16be(pointer_cast<const char *>(ptr));
}

inline uint32_t load32be(const unsigned char *ptr) {
  return load32be(pointer_cast<const char *>(ptr));
}

__attribute__((always_inline)) inline unsigned char *store16be(unsigned char *ptr, uint16_t val) {
  return pointer_cast<unsigned char *>(store16be(pointer_cast<char *>(ptr), val));
}

inline unsigned char *store32be(unsigned char *ptr, uint32_t val) {
  return pointer_cast<unsigned char *>(store32be(pointer_cast<char *>(ptr), val));
}

#endif // OB_BYTEORDER_H
