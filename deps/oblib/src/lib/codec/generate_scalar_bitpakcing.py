#!/bin/env python2
# -*- coding: utf-8 -*-

# Copyright 2014 - 2018 Alibaba Inc. All Rights Reserved.
# Author:
#  shell> python2.6 generate_scalar_bitpakcing.py
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
#include "ob_generated_scalar_bp_func.h"
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

#ifndef OB_GENERATED_SCALAR_BP_FUNC_H_
#define OB_GENERATED_SCALAR_BP_FUNC_H_

#include <stdint.h>

namespace oceanbase
{
namespace common
{
// packing 8 uint8_t once
void scalar_fastpackwithoutmask_8(const uint8_t *__restrict__ in, uint8_t *__restrict__ out, const uint32_t bit);
void scalar_fastunpack_8(const uint8_t*__restrict__ in, uint8_t *__restrict__ out, const uint32_t bit);

// packing 32 uint8_t once
void scalar_fastpackwithoutmask_8_32_count(const uint8_t *__restrict__ in, uint32_t *__restrict__ _out, const uint32_t bit);
void scalar_fastunpack_8_32_count(const uint32_t *__restrict__ _in, uint8_t *__restrict__ out, const uint32_t bit);

// packing 16 uint16_t once
void scalar_fastpackwithoutmask_16(const uint16_t *__restrict__ in, uint16_t *__restrict__ out, const uint32_t bit);
void scalar_fastunpack_16(const uint16_t*__restrict__ in, uint16_t *__restrict__ out, const uint32_t bit);

// packing 32 uint16_t once
void scalar_fastpackwithoutmask_16_32_count(const uint16_t *__restrict__ in, uint32_t *__restrict__ _out, const uint32_t bit);
void scalar_fastunpack_16_32_count(const uint32_t *__restrict__ _in, uint16_t *__restrict__ out, const uint32_t bit);

// packing 32 uint32_t once
void scalar_fastpackwithoutmask_32(const uint32_t *__restrict__ in, uint32_t *__restrict__ out, const uint32_t bit);
void scalar_fastunpack_32(const uint32_t*__restrict__ in, uint32_t *__restrict__ out, const uint32_t bit);

// packing 64 uint64_t once
void scalar_fastpackwithoutmask_64(const uint64_t *__restrict__ in, uint64_t *__restrict__ out, const uint32_t bit);
void scalar_fastunpack_64(const uint64_t*__restrict__ in, uint64_t *__restrict__ out, const uint32_t bit);

  """
  h_f.write(head)

def end_generate_h():
  global h_f
  end = """
} // end namespace common
} // end namespace oceanbase
#endif /* OB_GENERATED_SCALAR_BP_FUNC_H_ */
  """
  h_f.write(end)
  h_f.close()

def generate_scalar_pack_switch_func(uint_width) :
  global cpp_f
  line = """
void scalar_fastpackwithoutmask_{0}(const uint{1}_t *__restrict__ in, uint{2}_t *__restrict__ out, const uint32_t bit)
{{
  switch (bit) {{
  case 0:
    return; """
  cpp_f.write(line.format(uint_width, uint_width, uint_width));
  for idx in range(1, uint_width + 1) :
    line = """
  case {0}: {{
    __scalar_fastpackwithoutmask_{1}_{2}(in, out);
    return;
  }} """
    cpp_f.write(line.format(idx, idx, uint_width));
  line = """
  default: {
    int ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "unexpected", KP(in), KP(out), K(bit));
    break;
  }
  }
} """
  cpp_f.write(line);

def generate_scalar_pack_switch_func_32_count(uint_width) :
  global cpp_f
  line = """
void scalar_fastpackwithoutmask_{0}_32_count(const uint{1}_t *__restrict__ in, uint32_t *__restrict__ _out, const uint32_t bit)
{{
  uint{2}_t *out = reinterpret_cast<uint{3}_t *>(_out);
  switch (bit) {{
  case 0:
    return; """
  cpp_f.write(line.format(uint_width, uint_width, uint_width, uint_width));
  for idx in range(1, uint_width + 1) :
    line = """
  case {0}: {{"""
    cpp_f.write(line.format(idx));
    for (count) in range(0, 32/uint_width) :
      line = """
    __scalar_fastpackwithoutmask_{0}_{1}(in + {2}*{3}, out + bit*{4});"""
      cpp_f.write(line.format(idx, uint_width, uint_width, count, count));
    line = """
    return;
  } """
    cpp_f.write(line);
  line = """
  default: {
    int ret = OB_ERR_UNEXPECTED;
    LIB_LOG(ERROR, "unexpected", KP(in), KP(out), K(bit));
    break;
  }
  }
} """
  cpp_f.write(line);


def generate_scalar_unpack_switch_func(uint_width) :
  global cpp_f
  line = """
void scalar_fastunpack_{0}(const uint{1} *__restrict__ in, uint{2}_t *__restrict__ out, const uint32_t bit)
{{
  switch (bit) {{
  case 0:
    memset(out, 0, {3} * ({4} / CHAR_BIT));
    return;
  """
  cpp_f.write(line.format(uint_width, uint_width, uint_width, uint_width, uint_width));
  for idx in range(1, uint_width + 1) :
    line = """
  case {0}: {{
    __scalar_fastunpack_{1}_{2}(in, out);
    return;
  }} """
    cpp_f.write(line.format(idx, idx, uint_width));
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

def generate_scalar_unpack_switch_func_32_count(uint_width) :
  global cpp_f
  line = """
void scalar_fastunpack_{0}_32_count(const uint32_t *__restrict__ _in, uint{1}_t *__restrict__ out, const uint32_t bit)
{{
  const uint{2}_t *in = reinterpret_cast<const uint{3}_t *>(_in);
  switch (bit) {{
  case 0:
    memset(out, 0, 32 * ({4} / CHAR_BIT));
    return;
  """
  cpp_f.write(line.format(uint_width, uint_width, uint_width, uint_width, uint_width));
  for idx in range(1, uint_width + 1) :
    line = """
  case {0}: {{ """
    cpp_f.write(line.format(idx));
    for (count) in range(0, 32/uint_width) :
      line = """
    __scalar_fastunpack_{0}_{1}(in + bit*{2}, out + {3}*{4}); """
      cpp_f.write(line.format(idx, uint_width, count, uint_width, count));
    line = """
    return;
  } """
    cpp_f.write(line);
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




def generate_scalar_pack(uint_width, bit_width) :
  global cpp_f
  line = """
static void __scalar_fastpackwithoutmask_{0}_{1}(const uint{2}_t *__restrict__ in, uint{3}_t *__restrict__ out) {{
  """
  cpp_f.write(line.format(bit_width, uint_width, uint_width, uint_width))

  if uint_width == bit_width :
    for i in range(1, bit_width + 1) :
      line = """
  *out = (*in);
  ++out;
  ++in;
  """
      cpp_f.write(line)
  else :
    line = """
  *out = (*in);
  ++in;
  """
    cpp_f.write(line)
    # do next
    shifted_bits = bit_width
    for i in range(1, uint_width) :
      cur_end_bits = (i + 1) * bit_width;
      if (shifted_bits + bit_width) > uint_width :
        line = """
  *out |= ((*in)) << {0};
  ++out;
  *out = ((*in)) >> ({1} - {2});
  ++in;
        """
        tmp = cur_end_bits%uint_width
        cpp_f.write(line.format(shifted_bits, bit_width, tmp))
        shifted_bits = tmp;
      elif (shifted_bits + bit_width) == uint_width :
        line = """
  *out |= ((*in)) << {0};
  ++out;
  ++in;
        """
        cpp_f.write(line.format(shifted_bits))
        shifted_bits = 0
      else :
        if (shifted_bits == 0) :
          line = """
  *out = ((*in));
  ++in;
        """
          cpp_f.write(line.format(shifted_bits));
        else :
          line = """
  *out |= ((*in)) << {0};
  ++in;
        """
          cpp_f.write(line.format(shifted_bits));
        shifted_bits += bit_width
  line = """
}
"""
  cpp_f.write(line);


def generate_scalar_unpack(uint_width, bit_width) :
  line = """
static void __scalar_fastunpack_{0}_{1}(const uint{2}_t *__restrict__ in, uint{3}_t *__restrict__ out) {{
    """
  cpp_f.write(line.format(bit_width, uint_width, uint_width, uint_width))

  shifted_bits = 0;
  for i in range(0, uint_width) :
    cur_end_bits = (i + 1) * bit_width
    shifted_end_bits = shifted_bits + bit_width;
    if shifted_end_bits > uint_width :
      line = """
  *out = ((*in) >> {0});
  ++in;
  *out |= ((*in) % (1U << {1})) << ({2} - {3});
  out++;
        """
      tmp = cur_end_bits%uint_width
      tmp2 = tmp%bit_width
      cpp_f.write(line.format(shifted_bits, tmp2, bit_width, tmp2))
      shifted_bits = tmp2
    elif shifted_end_bits == uint_width :
      line = """
  *out = ((*in) >> {0});
  ++in;
  out++;
        """
      cpp_f.write(line.format(shifted_bits))
      shifted_bits = 0
    else :
      line = """
  *out = ((*in) >> {0}) % (1U << {1});
  out++;
        """
      cpp_f.write(line.format(shifted_bits, bit_width));
      shifted_bits += bit_width
  line = """
}
"""
  cpp_f.write(line);

if __name__ == "__main__":
  start_generate_h("ob_generated_scalar_bp_func.h")
  end_generate_h()

  start_generate_cpp("ob_generated_scalar_bp_func.cpp")

  for i in range(1, 9) :
    generate_scalar_pack(8, i)
  for i in range(1, 9) :
    generate_scalar_unpack(8, i)

  for i in range(1, 17) :
    generate_scalar_pack(16, i)
  for i in range(1, 17) :
    generate_scalar_unpack(16, i)

  for i in range(1, 33) :
    generate_scalar_pack(32, i)
  for i in range(1, 33) :
    generate_scalar_unpack(32, i)

  #for i in range(1, 65) :
  #  generate_scalar_pack(64, i)
  #for i in range(1, 65) :
  #  generate_scalar_unpack(64, i)

  generate_scalar_pack_switch_func(8);
  generate_scalar_unpack_switch_func(8);
  generate_scalar_pack_switch_func_32_count(8);
  generate_scalar_unpack_switch_func_32_count(8);


  generate_scalar_pack_switch_func(16);
  generate_scalar_unpack_switch_func(16);
  generate_scalar_pack_switch_func_32_count(16);
  generate_scalar_unpack_switch_func_32_count(16);


  generate_scalar_pack_switch_func(32);
  generate_scalar_unpack_switch_func(32);

  #generate_scalar_pack_switch_func(64);
  #generate_scalar_unpack_switch_func(64);

  end_generate_cpp()

  print "\nSuccess\n";
