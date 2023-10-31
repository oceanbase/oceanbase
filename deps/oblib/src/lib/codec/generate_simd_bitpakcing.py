#!/bin/env python2
# -*- coding: utf-8 -*-

# Copyright 2014 - 2018 Alibaba Inc. All Rights Reserved.
# Author:
#  shell> python2.6 generate_simd_bitpakcing.py
# author: oushen
import os

cpp_f = None
h_f = None

copyright = """/**
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
 """

def start_generate_cpp(cpp_file_name):
  global cpp_f
  cpp_f = open(cpp_file_name, 'w')
  head = copyright + """
#include "ob_generated_unalign_simd_bp_func.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/utility.h"

namespace oceanbase
{
namespace common
{
  """
  cpp_f.write(head)


def end_generate_cpp():
  global cpp_f
  end = """
} // end namespace share
} // end namespace oceanbase
  """
  cpp_f.write(end)
  cpp_f.close()

def start_generate_h(h_file_name):
  global h_f
  h_f = open(h_file_name, 'w')
  head = copyright + """

#ifndef OB_GENERATED_UNALIGN_SIMD_BP_FUNC_H_
#define OB_GENERATED_UNALIGN_SIMD_BP_FUNC_H_

#include <stdint.h>
#include <string.h>
#include "ob_sse_to_neon.h"

namespace oceanbase
{
namespace common
{
void uSIMD_fastpackwithoutmask_128_16(const uint16_t *__restrict__ in,
                                      __m128i *__restrict__ out, const uint32_t bit);
void uSIMD_fastunpack_128_16(const __m128i *__restrict__ in,
                             uint16_t *__restrict__ out, const uint32_t bit);


void uSIMD_fastpackwithoutmask_128_32(const uint32_t *__restrict__ in,
                                      __m128i *__restrict__ out, const uint32_t bit);
void uSIMD_fastunpack_128_32(const __m128i *__restrict__ in,
                             uint32_t *__restrict__ out, const uint32_t bit);


//void uSIMD_fastpackwithoutmask_256_32(const uint32_t *__restrict__ in,
//                                       __m256i *__restrict__ out, const uint32_t bit);
//void uSIMD_fastunpack_256_32(const __m256i *__restrict__ in,
//                             uint32_t *__restrict__ out, const uint32_t bit);

// TODO add avx512 and uint64_t packing method
  """
  h_f.write(head)

def end_generate_h():
  global h_f
  end = """
} // end namespace common
} // end namespace oceanbase
#endif /* OB_GENERATED_UNALIGN_SIMD_BP_FUNC_H_ */
  """
  h_f.write(end)
  h_f.close()

def generate_simd_pack_256_uint32(register_width, uint_width, bit_width) :
  global cpp_f
  line = """
static void __SIMD_fastpackwithoutmask_256_{0}_{1}(const uint32_t *__restrict__ _in, __m256i *__restrict__ out) {{
  const __m256i *in = reinterpret_cast<const __m256i *>(_in);
  __m256i OutReg;
  __m256i InReg = _mm256_loadu_si256(in);

  OutReg = InReg;
  InReg = _mm256_loadu_si256(++in);
 """
  cpp_f.write(line.format(bit_width, uint_width))
  if uint_width == bit_width :
    for i in range(1, 32) :
      line = """
  _mm256_storeu_si256(out, OutReg);
  ++out;
  OutReg = InReg;
  InReg = _mm256_loadu_si256(++in);
"""
      cpp_f.write(line)
    line = """
  OutReg = InReg;
  _mm256_storeu_si256(out, OutReg);
"""
    cpp_f.write(line)
  else :
    shifted_bits = bit_width
    for i in range(1, 32) :
      cur_end_bits = (i + 1) * bit_width;
      if (shifted_bits + bit_width) > uint_width :
        line = """
  OutReg = _mm256_or_si256(OutReg, _mm256_slli_epi32(InReg, {0}));
  _mm256_storeu_si256(out, OutReg);
  ++out;
  OutReg = _mm256_srli_epi32(InReg, {1} - {2});
  InReg = _mm256_loadu_si256(++in);
        """
        tmp = cur_end_bits%uint_width
        cpp_f.write(line.format(shifted_bits, bit_width, tmp))
        shifted_bits = tmp;
      elif (shifted_bits + bit_width) == uint_width :
        line = """
  OutReg = _mm256_or_si256(OutReg, _mm256_slli_epi32(InReg, {0}));
  _mm256_storeu_si256(out, OutReg);
        """
        cpp_f.write(line.format(shifted_bits))
        shifted_bits = 0
      else :
        line = """
  OutReg = _mm256_or_si256(OutReg, _mm256_slli_epi32(InReg, {0}));
  InReg = _mm256_loadu_si256(++in);
  """
        cpp_f.write(line.format(shifted_bits));
        shifted_bits += bit_width

  line = """
}
"""
  cpp_f.write(line);

def generate_simd_pack_uint16(register_width, uint_width, bit_width) :
  global cpp_f
  line = """
static void __SIMD_fastpackwithoutmask_128_{0}_{1}(const uint16_t *__restrict__ _in, __m128i *__restrict__ out) {{
  const __m128i *in = reinterpret_cast<const __m128i *>(_in);
  __m128i OutReg;
  __m128i InReg = _mm_loadu_si128(in);
"""
  cpp_f.write(line.format(bit_width, uint_width))

  if uint_width == bit_width :
    for i in range(1, 16) :
      line = """
  OutReg = InReg;
  _mm_storeu_si128(out, OutReg);
  ++out;
  InReg = _mm_loadu_si128(++in);
  """
      cpp_f.write(line)
    line = """
  OutReg = InReg;
  _mm_storeu_si128(out, OutReg);
    """
    cpp_f.write(line)
  else :
    line = """
  OutReg = InReg;
  InReg = _mm_loadu_si128(++in);
  """
    cpp_f.write(line)

    shifted_bits = bit_width
    for i in range(1, 16) :
      cur_end_bits = (i + 1) * bit_width;
      if (shifted_bits + bit_width) > uint_width :
        line = """
  OutReg = _mm_or_si128(OutReg, _mm_slli_epi16(InReg, {0}));
  _mm_storeu_si128(out, OutReg);
  ++out;
  OutReg = _mm_srli_epi16(InReg, {1} - {2});
  InReg = _mm_loadu_si128(++in);
        """
        tmp = cur_end_bits%uint_width
        cpp_f.write(line.format(shifted_bits, bit_width, tmp))
        shifted_bits = tmp;
      elif (shifted_bits + bit_width) == uint_width :
        if i == 15 :
          line = """
  OutReg = _mm_or_si128(OutReg, _mm_slli_epi16(InReg, {0}));
  _mm_storeu_si128(out, OutReg);
        """
          cpp_f.write(line.format(shifted_bits))
        else :
          line = """
  OutReg = _mm_or_si128(OutReg, _mm_slli_epi16(InReg, {0}));
  _mm_storeu_si128(out, OutReg);
  ++out;
  InReg = _mm_loadu_si128(++in);
        """
          cpp_f.write(line.format(shifted_bits))
        shifted_bits = 0
      else :
        if 0 == shifted_bits :
          line = """
  OutReg = InReg;
  InReg = _mm_loadu_si128(++in);
        """
          cpp_f.write(line);
        else:
          line = """
  OutReg = _mm_or_si128(OutReg, _mm_slli_epi16(InReg, {0}));
  InReg = _mm_loadu_si128(++in);
        """
          cpp_f.write(line.format(shifted_bits));
        shifted_bits += bit_width

  line = """
}
"""
  cpp_f.write(line);

def generate_simd_unpack_uint16(register_width, uint_width, bit_width) :
  global cpp_f
  if uint_width == bit_width :
    line = """
static void __SIMD_fastunpack_128_16_16(const __m128i *__restrict__ in,
                                        uint16_t *__restrict__ _out) {
  __m128i *out = reinterpret_cast<__m128i *>(_out);
  for (uint32_t outer = 0; outer < 16; ++outer) {
    _mm_storeu_si128(out++, _mm_loadu_si128(in++));
  }
}
    """
    cpp_f.write(line)
  else :
    line = """
static void __SIMD_fastunpack_128_{0}_{1}(const __m128i *__restrict__ in,
                                          uint16_t *__restrict__ _out) {{

  __m128i *out = reinterpret_cast<__m128i *>(_out);
  __m128i InReg = _mm_loadu_si128(in);
  __m128i OutReg;
  const __m128i mask = _mm_set1_epi16((1U << {2}) - 1);

  OutReg = _mm_and_si128(InReg, mask);
  _mm_storeu_si128(out++, OutReg);
    """
    cpp_f.write(line.format(bit_width, uint_width, bit_width))

    shifted_bits = bit_width;
    for i in range(1, 16) :
      cur_end_bits = (i + 1) * bit_width
      shifted_end_bits = shifted_bits + bit_width;
      if shifted_end_bits > uint_width :
        line = """
  OutReg = _mm_srli_epi16(InReg, {0});
  InReg = _mm_loadu_si128(++in);

  OutReg =
      _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi16(InReg, {1} - {2}), mask));
  _mm_storeu_si128(out++, OutReg);
        """
        tmp = cur_end_bits%uint_width
        tmp2 = tmp%bit_width
        cpp_f.write(line.format(shifted_bits, bit_width, tmp2))
        shifted_bits = tmp2
      elif shifted_end_bits == uint_width :
        if i == 15 :
          line = """
  OutReg = _mm_srli_epi16(InReg, {0});
  _mm_storeu_si128(out++, OutReg);
        """
          cpp_f.write(line.format(shifted_bits))
        else :
          line = """
  OutReg = _mm_srli_epi16(InReg, {0});
  InReg = _mm_loadu_si128(++in);

  _mm_storeu_si128(out++, OutReg);
        """
          cpp_f.write(line.format(shifted_bits))
        shifted_bits = 0
      elif shifted_end_bits == bit_width :
        line = """
  OutReg = _mm_and_si128(InReg, mask);
  _mm_storeu_si128(out++, OutReg);
        """
        cpp_f.write(line)
        shifted_bits = bit_width
      else :
        line = """
  OutReg = _mm_and_si128(_mm_srli_epi16(InReg, {0}), mask);
  _mm_storeu_si128(out++, OutReg);
        """
        cpp_f.write(line.format(shifted_bits));
        shifted_bits += bit_width
    line = """
}
"""
    cpp_f.write(line);


def generate_simd_pack(register_width, uint_width, bit_width) :
  global cpp_f
  line = """
static void __SIMD_fastpackwithoutmask_128_{0}_{1}(const uint32_t *__restrict__ _in, __m128i *__restrict__ out) {{
  const __m128i *in = reinterpret_cast<const __m128i *>(_in);
  __m128i OutReg;
  __m128i InReg = _mm_loadu_si128(in);
  """
  cpp_f.write(line.format(bit_width, uint_width))

  if uint_width == bit_width :
    for i in range(1, 32) :
      line = """
  OutReg = InReg;
  _mm_storeu_si128(out, OutReg);
  ++out;
  InReg = _mm_loadu_si128(++in);
  """
      cpp_f.write(line)
    line = """
  OutReg = InReg;
  _mm_storeu_si128(out, OutReg);
    """
    cpp_f.write(line)
  else :
    line = """
  OutReg = InReg;
  InReg = _mm_loadu_si128(++in);
  """
    cpp_f.write(line)
    # do next
    shifted_bits = bit_width
    for i in range(1, 32) :
      cur_end_bits = (i + 1) * bit_width;
      if (shifted_bits + bit_width) > uint_width :
        line = """
  OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, {0}));
  _mm_storeu_si128(out, OutReg);
  ++out;
  OutReg = _mm_srli_epi32(InReg, {1} - {2});
  InReg = _mm_loadu_si128(++in);
        """
        tmp = cur_end_bits%uint_width
        cpp_f.write(line.format(shifted_bits, bit_width, tmp))
        shifted_bits = tmp;
      elif (shifted_bits + bit_width) == uint_width :
        if i == 31 :
          line = """
  OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, {0}));
  _mm_storeu_si128(out, OutReg);
        """
          cpp_f.write(line.format(shifted_bits))
        else :
          line = """
  OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, {0}));
  _mm_storeu_si128(out, OutReg);
  ++out;
  InReg = _mm_loadu_si128(++in);
        """
          cpp_f.write(line.format(shifted_bits))
        shifted_bits = 0
      else :
        if 0 == shifted_bits :
          line = """
  OutReg = InReg;
  InReg = _mm_loadu_si128(++in);
        """
          cpp_f.write(line.format(shifted_bits));
        else :
          line = """
  OutReg = _mm_or_si128(OutReg, _mm_slli_epi32(InReg, {0}));
  InReg = _mm_loadu_si128(++in);
        """
          cpp_f.write(line.format(shifted_bits));
        shifted_bits += bit_width

  line = """
}
"""
  cpp_f.write(line);


def generate_simd_unpack(register_width, uint_width, bit_width) :
  global cpp_f
  if uint_width == bit_width :
    line = """
static void __SIMD_fastunpack_128_32_32(const __m128i *__restrict__ in,
                                        uint32_t *__restrict__ _out) {
  __m128i *out = reinterpret_cast<__m128i *>(_out);
  for (uint32_t outer = 0; outer < 32; ++outer) {
    _mm_storeu_si128(out++, _mm_loadu_si128(in++));
  }
}
    """
    cpp_f.write(line)
  else :
    line = """
static void __SIMD_fastunpack_128_{0}_{1}(const __m128i *__restrict__ in,
                                          uint32_t *__restrict__ _out) {{

  __m128i *out = reinterpret_cast<__m128i *>(_out);
  __m128i InReg = _mm_loadu_si128(in);
  __m128i OutReg;
  const __m128i mask = _mm_set1_epi32((1U << {2}) - 1);

  OutReg = _mm_and_si128(InReg, mask);
  _mm_storeu_si128(out++, OutReg);
    """
    cpp_f.write(line.format(bit_width, uint_width, bit_width))


    shifted_bits = bit_width;
    for i in range(1, 32) :
      cur_end_bits = (i + 1) * bit_width
      shifted_end_bits = shifted_bits + bit_width;
      if shifted_end_bits > uint_width :
        line = """
  OutReg = _mm_srli_epi32(InReg, {0});
  InReg = _mm_loadu_si128(++in);

  OutReg =
      _mm_or_si128(OutReg, _mm_and_si128(_mm_slli_epi32(InReg, {1} - {2}), mask));
  _mm_storeu_si128(out++, OutReg);
        """
        tmp = cur_end_bits%uint_width
        tmp2 = tmp%bit_width
        cpp_f.write(line.format(shifted_bits, bit_width, tmp2))
        shifted_bits = tmp2
      elif shifted_end_bits == uint_width :
        if i == 31 :
          line = """
  OutReg = _mm_srli_epi32(InReg, {0});
  _mm_storeu_si128(out++, OutReg);
        """
          cpp_f.write(line.format(shifted_bits))
        else :
          line = """
  OutReg = _mm_srli_epi32(InReg, {0});
  InReg = _mm_loadu_si128(++in);

  _mm_storeu_si128(out++, OutReg);
        """
          cpp_f.write(line.format(shifted_bits))
        shifted_bits = 0
      elif shifted_end_bits == bit_width :
        line = """
  OutReg = _mm_and_si128(InReg, mask);
  _mm_storeu_si128(out++, OutReg);
        """
        cpp_f.write(line)
        shifted_bits = bit_width
      else :
        line = """
  OutReg = _mm_and_si128(_mm_srli_epi32(InReg, {0}), mask);
  _mm_storeu_si128(out++, OutReg);
        """
        cpp_f.write(line.format(shifted_bits));
        shifted_bits += bit_width
    line = """
}
"""
    cpp_f.write(line);


def generate_simd_unpack_256_uint32(register_width, uint_width, bit_width) :
  global cpp_f
  if uint_width == bit_width :
    line = """
static void __SIMD_fastunpack_256_32_32(const __m256i *__restrict__ in,
                                        uint32_t *__restrict__ _out) {
  __m256i *out = reinterpret_cast<__m256i *>(_out);
  for (uint32_t outer = 0; outer < 32; ++outer) {
    _mm256_storeu_si256(out++, _mm256_loadu_si256(in++));
  }
}
    """
    cpp_f.write(line)
  else:
    line = """
static void __SIMD_fastunpack_256_{0}_{1}(const __m256i *__restrict__ in,
                                          uint32_t *__restrict__ _out) {{

  __m256i *out = reinterpret_cast<__m256i *>(_out);
  __m256i InReg = _mm256_loadu_si256(in);
  __m256i OutReg;
  const __m256i mask = _mm256_set1_epi32((1U << {2}) - 1);

  OutReg = _mm256_and_si256(InReg, mask);
  _mm256_storeu_si256(out++, OutReg);
    """
    cpp_f.write(line.format(bit_width, uint_width, bit_width))

    shifted_bits = bit_width;
    for i in range(1, 32) :
      cur_end_bits = (i + 1) * bit_width
      shifted_end_bits = shifted_bits + bit_width;
      if shifted_end_bits > uint_width :
        line = """
  OutReg = _mm256_and_si256(_mm256_srli_epi32(InReg, {0}), mask);
  _mm256_storeu_si256(out++, OutReg);

  OutReg =
      _mm256_or_si256(OutReg, _mm256_and_si256(_mm256_slli_epi32(InReg, {1} - {2}), mask));
  _mm256_storeu_si256(out++, OutReg);
        """
        tmp = cur_end_bits%uint_width
        tmp2 = tmp%bit_width
        cpp_f.write(line.format(shifted_bits, bit_width, tmp2))
        shifted_bits = tmp2
      elif shifted_end_bits == uint_width :
        line = """
  OutReg = _mm256_srli_epi32(InReg, {0});
  InReg = _mm256_loadu_si256(++in);

  _mm256_storeu_si256(out++, OutReg);
        """
        cpp_f.write(line.format(shifted_bits))
        shifted_bits = 0
      elif shifted_end_bits == bit_width :
        line = """
  OutReg = _mm256_and_si256(InReg, mask);
  _mm256_storeu_si256(out++, OutReg);
        """
        cpp_f.write(line)
        shifted_bits = bit_width
      else :
        line = """
  OutReg = _mm256_and_si256(_mm256_srli_epi32(InReg, {0}), mask);
  _mm256_storeu_si256(out++, OutReg);
        """
        cpp_f.write(line.format(shifted_bits));
        shifted_bits += bit_width
    line = """
}
"""
    cpp_f.write(line);

def generate_pack_128_switch_func_uint16() :
  global cpp_f
  line = """
void uSIMD_fastpackwithoutmask_128_16(const uint16_t *__restrict__ in,
                                      __m128i *__restrict__ out, const uint32_t bit)
{
  switch (bit) {
  case 0:
    return;
  """
  cpp_f.write(line);
  for idx in range(1, 17) :
    line = """
  case {0}:
    __SIMD_fastpackwithoutmask_128_{1}_{2}(in, out);
    return;
    """
    cpp_f.write(line.format(idx, idx, 16));
  line = """
  default: {
    int ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "unexpected", KP(in), KP(out), K(bit));
    break;
  }
  }
}
  """
  cpp_f.write(line);

def generate_unpack_128_switch_func_uint16() :
  global cpp_f
  line = """
void uSIMD_fastunpack_128_16(const __m128i *__restrict__ in,
                             uint16_t *__restrict__ out, const uint32_t bit)
{
  switch (bit) {
  case 0:
    memset(out, 0, 128 * 2);
    return;
  """
  cpp_f.write(line);
  for idx in range(1, 17) :
    line = """
  case {0}:
    __SIMD_fastunpack_128_{1}_{2}(in, out);
    return;
    """
    cpp_f.write(line.format(idx, idx, 16));
  line = """
  default: {
    int ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "unexpected", KP(in), KP(out), K(bit));
    break;
  }
  }
}
  """
  cpp_f.write(line);


def generate_pack_128_switch_func() :
  global cpp_f
  line = """
void uSIMD_fastpackwithoutmask_128_32(const uint32_t *__restrict__ in,
                                      __m128i *__restrict__ out, const uint32_t bit)
{
  switch (bit) {
  case 0:
    return;
  """
  cpp_f.write(line);
  for idx in range(1, 33) :
    line = """
  case {0}:
    __SIMD_fastpackwithoutmask_128_{1}_{2}(in, out);
    return;
    """
    cpp_f.write(line.format(idx, idx, 32));
  line = """
  default: {
    int ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "unexpected", KP(in), KP(out), K(bit));
    break;
  }
  }
}
  """
  cpp_f.write(line);

def generate_unpack_128_switch_func() :
  global cpp_f
  line = """
void uSIMD_fastunpack_128_32(const __m128i *__restrict__ in,
                             uint32_t *__restrict__ out, const uint32_t bit)
{
  switch (bit) {
  case 0:
    memset(out, 0, 128 * 4);
    return;
  """
  cpp_f.write(line);
  for idx in range(1, 33) :
    line = """
  case {0}:
    __SIMD_fastunpack_128_{1}_{2}(in, out);
    return;
    """
    cpp_f.write(line.format(idx, idx, 32));
  line = """
  default: {
    int ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "unexpected", KP(in), KP(out), K(bit));
    break;
  }
  }
}
  """
  cpp_f.write(line);


def generate_pack_256_switch_func() :
  global cpp_f
  line = """
void uSIMD_fastpackwithoutmask_256_32(const uint32_t *__restrict__ in,
                                       __m256i *__restrict__ out, const uint32_t bit)
{
  switch (bit) {
  case 0:
    return;
  """
  cpp_f.write(line);
  for idx in range(1, 33) :
    line = """
  case {0}:
    __SIMD_fastpackwithoutmask_256_{1}_{2}(in, out);
    return;
    """
    cpp_f.write(line.format(idx, idx, 32));
  line = """
  default: {
    int ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "unexpected", KP(in), KP(out), K(bit));
    break;
  }
  }
}
  """
  cpp_f.write(line);

def generate_unpack_256_switch_func() :
  global cpp_f
  line = """
void uSIMD_fastunpack_256_32(const __m256i *__restrict__ in,
                             uint32_t *__restrict__ out, const uint32_t bit)
{
  switch (bit) {
  case 0:
    memset(out, 0, 256 * 4);
  """
  cpp_f.write(line);
  for idx in range(1, 33) :
    line = """
  case {0}:
    __SIMD_fastunpack_256_{1}_{2}(in, out);
    return;
    """
    cpp_f.write(line.format(idx, idx, 32));
  line = """
  default: {
    int ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "unexpected", KP(in), KP(out), K(bit));
    break;
  }
  }
}
  """
  cpp_f.write(line);

if __name__ == "__main__":
  start_generate_h("ob_generated_unalign_simd_bp_func.h")
  end_generate_h()

  start_generate_cpp("ob_generated_unalign_simd_bp_func.cpp")
  #__m128i, uint8_t
  # not support

  #__m128i, uint16_t
  for i in range(1, 17) :
    generate_simd_pack_uint16(128, 16, i)
  for i in range(1, 17) :
    generate_simd_unpack_uint16(128, 16, i)

  #__m128i, uint32_t
  for i in range(1, 33) :
    generate_simd_pack(128, 32, i)
  for i in range(1, 33) :
    generate_simd_unpack(128, 32, i)

  #__m256i, uint32_t
  # not used yet
  #for i in range(1, 33) :
  #  generate_simd_pack_256_uint32(256, 32, i)
  #for i in range(1, 33) :
  #  generate_simd_unpack_256_uint32(256, 32, i)

  #__m256i, uint64_t
  # not support

  generate_pack_128_switch_func_uint16()
  generate_unpack_128_switch_func_uint16()

  generate_pack_128_switch_func()
  generate_unpack_128_switch_func()

  #generate_pack_256_switch_func()
  #generate_unpack_256_switch_func()
  end_generate_cpp()

  print "\nSuccess\n";
