#!/usr/bin/env
# -*- coding: UTF-8 -*-

import os

DEFINED_COLLS = [
    "CS_TYPE_GBK_CHINESE_CI",
    "CS_TYPE_UTF8MB4_GENERAL_CI",
    "CS_TYPE_UTF8MB4_BIN",
    "CS_TYPE_UTF16_GENERAL_CI",
    "CS_TYPE_UTF16_BIN",
    "CS_TYPE_BINARY",
    "CS_TYPE_GBK_BIN",
    "CS_TYPE_UTF16_UNICODE_CI",
    "CS_TYPE_UTF8MB4_UNICODE_CI",
    "CS_TYPE_GB18030_CHINESE_CI",
    "CS_TYPE_GB18030_BIN",
    "CS_TYPE_UJIS_JAPANESE_CI",
    "CS_TYPE_UJIS_BIN",
    "CS_TYPE_EUCKR_KOREAN_CI",
    "CS_TYPE_EUCKR_BIN",
    "CS_TYPE_CP932_JAPANESE_CI",
    "CS_TYPE_CP932_BIN",
    "CS_TYPE_EUCJPMS_JAPANESE_CI",
    "CS_TYPE_EUCJPMS_BIN",
    "CS_TYPE_LATIN1_GERMAN1_CI",
    "CS_TYPE_LATIN1_SWEDISH_CI",
    "CS_TYPE_LATIN1_DANISH_CI",
    "CS_TYPE_LATIN1_GERMAN2_CI",
    "CS_TYPE_LATIN1_BIN",
    "CS_TYPE_LATIN1_GENERAL_CI",
    "CS_TYPE_LATIN1_GENERAL_CS",
    "CS_TYPE_LATIN1_SPANISH_CI",
    "CS_TYPE_GB2312_CHINESE_CI",
    "CS_TYPE_GB2312_BIN",
    "CS_TYPE_GB18030_2022_BIN",
    "CS_TYPE_GB18030_2022_PINYIN_CI",
    "CS_TYPE_GB18030_2022_PINYIN_CS",
    "CS_TYPE_GB18030_2022_RADICAL_CI",
    "CS_TYPE_GB18030_2022_RADICAL_CS",
    "CS_TYPE_GB18030_2022_STROKE_CI",
    "CS_TYPE_GB18030_2022_STROKE_CS",
    "CS_TYPE_ASCII_GENERAL_CI",
    "CS_TYPE_ASCII_BIN",
    "CS_TYPE_TIS620_THAI_CI",
    "CS_TYPE_TIS620_BIN",
    "CS_TYPE_UTF16LE_GENERAL_CI",
    "CS_TYPE_UTF16LE_BIN",
    "CS_TYPE_SJIS_JAPANESE_CI",
    "CS_TYPE_SJIS_BIN",
    "CS_TYPE_BIG5_CHINESE_CI",
    "CS_TYPE_BIG5_BIN",
    "CS_TYPE_HKSCS_BIN",
    "CS_TYPE_HKSCS31_BIN",
    "CS_TYPE_UTF8MB4_ICELANDIC_UCA_CI",
    "CS_TYPE_UTF8MB4_LATVIAN_UCA_CI",
    "CS_TYPE_UTF8MB4_ROMANIAN_UCA_CI",
    "CS_TYPE_UTF8MB4_SLOVENIAN_UCA_CI",
    "CS_TYPE_UTF8MB4_POLISH_UCA_CI",
    "CS_TYPE_UTF8MB4_ESTONIAN_UCA_CI",
    "CS_TYPE_UTF8MB4_SPANISH_UCA_CI",
    "CS_TYPE_UTF8MB4_SWEDISH_UCA_CI",
    "CS_TYPE_UTF8MB4_TURKISH_UCA_CI",
    "CS_TYPE_UTF8MB4_CZECH_UCA_CI",
    "CS_TYPE_UTF8MB4_DANISH_UCA_CI",
    "CS_TYPE_UTF8MB4_LITHUANIAN_UCA_CI",
    "CS_TYPE_UTF8MB4_SLOVAK_UCA_CI",
    "CS_TYPE_UTF8MB4_SPANISH2_UCA_CI",
    "CS_TYPE_UTF8MB4_ROMAN_UCA_CI",
    "CS_TYPE_UTF8MB4_PERSIAN_UCA_CI",
    "CS_TYPE_UTF8MB4_ESPERANTO_UCA_CI",
    "CS_TYPE_UTF8MB4_HUNGARIAN_UCA_CI",
    "CS_TYPE_UTF8MB4_SINHALA_UCA_CI",
    "CS_TYPE_UTF8MB4_GERMAN2_UCA_CI",
    "CS_TYPE_UTF8MB4_CROATIAN_UCA_CI",
    "CS_TYPE_UTF8MB4_UNICODE_520_CI",
    "CS_TYPE_UTF8MB4_VIETNAMESE_CI",
    "CS_TYPE_UTF16_ICELANDIC_UCA_CI",
    "CS_TYPE_UTF16_LATVIAN_UCA_CI",
    "CS_TYPE_UTF16_ROMANIAN_UCA_CI",
    "CS_TYPE_UTF16_SLOVENIAN_UCA_CI",
    "CS_TYPE_UTF16_POLISH_UCA_CI",
    "CS_TYPE_UTF16_ESTONIAN_UCA_CI",
    "CS_TYPE_UTF16_SPANISH_UCA_CI",
    "CS_TYPE_UTF16_SWEDISH_UCA_CI",
    "CS_TYPE_UTF16_TURKISH_UCA_CI",
    "CS_TYPE_UTF16_CZECH_UCA_CI",
    "CS_TYPE_UTF16_DANISH_UCA_CI",
    "CS_TYPE_UTF16_LITHUANIAN_UCA_CI",
    "CS_TYPE_UTF16_SLOVAK_UCA_CI",
    "CS_TYPE_UTF16_SPANISH2_UCA_CI",
    "CS_TYPE_UTF16_ROMAN_UCA_CI",
    "CS_TYPE_UTF16_PERSIAN_UCA_CI",
    "CS_TYPE_UTF16_ESPERANTO_UCA_CI",
    "CS_TYPE_UTF16_HUNGARIAN_UCA_CI",
    "CS_TYPE_UTF16_SINHALA_UCA_CI",
    "CS_TYPE_UTF16_GERMAN2_UCA_CI",
    "CS_TYPE_UTF16_CROATIAN_UCA_CI",
    "CS_TYPE_UTF16_UNICODE_520_CI",
    "CS_TYPE_UTF16_VIETNAMESE_CI",
    "CS_TYPE_UTF8MB4_0900_AI_CI",
    "CS_TYPE_UTF8MB4_DE_PB_0900_AI_CI",
    "CS_TYPE_UTF8MB4_IS_0900_AI_CI",
    "CS_TYPE_UTF8MB4_LV_0900_AI_CI",
    "CS_TYPE_UTF8MB4_RO_0900_AI_CI",
    "CS_TYPE_UTF8MB4_SL_0900_AI_CI",
    "CS_TYPE_UTF8MB4_PL_0900_AI_CI",
    "CS_TYPE_UTF8MB4_ET_0900_AI_CI",
    "CS_TYPE_UTF8MB4_ES_0900_AI_CI",
    "CS_TYPE_UTF8MB4_SV_0900_AI_CI",
    "CS_TYPE_UTF8MB4_TR_0900_AI_CI",
    "CS_TYPE_UTF8MB4_CS_0900_AI_CI",
    "CS_TYPE_UTF8MB4_DA_0900_AI_CI",
    "CS_TYPE_UTF8MB4_LT_0900_AI_CI",
    "CS_TYPE_UTF8MB4_SK_0900_AI_CI",
    "CS_TYPE_UTF8MB4_ES_TRAD_0900_AI_CI",
    "CS_TYPE_UTF8MB4_LA_0900_AI_CI",
    "CS_TYPE_UTF8MB4_EO_0900_AI_CI",
    "CS_TYPE_UTF8MB4_HU_0900_AI_CI",
    "CS_TYPE_UTF8MB4_HR_0900_AI_CI",
    "CS_TYPE_UTF8MB4_VI_0900_AI_CI",
    "CS_TYPE_UTF8MB4_0900_AS_CS",
    "CS_TYPE_UTF8MB4_DE_PB_0900_AS_CS",
    "CS_TYPE_UTF8MB4_IS_0900_AS_CS",
    "CS_TYPE_UTF8MB4_LV_0900_AS_CS",
    "CS_TYPE_UTF8MB4_RO_0900_AS_CS",
    "CS_TYPE_UTF8MB4_SL_0900_AS_CS",
    "CS_TYPE_UTF8MB4_PL_0900_AS_CS",
    "CS_TYPE_UTF8MB4_ET_0900_AS_CS",
    "CS_TYPE_UTF8MB4_ES_0900_AS_CS",
    "CS_TYPE_UTF8MB4_SV_0900_AS_CS",
    "CS_TYPE_UTF8MB4_TR_0900_AS_CS",
    "CS_TYPE_UTF8MB4_CS_0900_AS_CS",
    "CS_TYPE_UTF8MB4_DA_0900_AS_CS",
    "CS_TYPE_UTF8MB4_LT_0900_AS_CS",
    "CS_TYPE_UTF8MB4_SK_0900_AS_CS",
    "CS_TYPE_UTF8MB4_ES_TRAD_0900_AS_CS",
    "CS_TYPE_UTF8MB4_LA_0900_AS_CS",
    "CS_TYPE_UTF8MB4_EO_0900_AS_CS",
    "CS_TYPE_UTF8MB4_HU_0900_AS_CS",
    "CS_TYPE_UTF8MB4_HR_0900_AS_CS",
    "CS_TYPE_UTF8MB4_VI_0900_AS_CS",
    "CS_TYPE_UTF8MB4_JA_0900_AS_CS",
    "CS_TYPE_UTF8MB4_JA_0900_AS_CS_KS",
    "CS_TYPE_UTF8MB4_0900_AS_CI",
    "CS_TYPE_UTF8MB4_RU_0900_AI_CI",
    "CS_TYPE_UTF8MB4_RU_0900_AS_CS",
    "CS_TYPE_UTF8MB4_ZH_0900_AS_CS",
    "CS_TYPE_UTF8MB4_0900_BIN",
    "CS_TYPE_UTF8MB4_NB_0900_AI_CI",
    "CS_TYPE_UTF8MB4_NB_0900_AS_CS",
    "CS_TYPE_UTF8MB4_NN_0900_AI_CI",
    "CS_TYPE_UTF8MB4_NN_0900_AS_CS",
    "CS_TYPE_UTF8MB4_SR_LATN_0900_AI_CI",
    "CS_TYPE_UTF8MB4_SR_LATN_0900_AS_CS",
    "CS_TYPE_UTF8MB4_BS_0900_AI_CI",
    "CS_TYPE_UTF8MB4_BS_0900_AS_CS",
    "CS_TYPE_UTF8MB4_BG_0900_AI_CI",
    "CS_TYPE_UTF8MB4_BG_0900_AS_CS",
    "CS_TYPE_UTF8MB4_GL_0900_AI_CI",
    "CS_TYPE_UTF8MB4_GL_0900_AS_CS",
    "CS_TYPE_UTF8MB4_MN_CYRL_0900_AI_CI",
    "CS_TYPE_UTF8MB4_MN_CYRL_0900_AS_CS",
    "CS_TYPE_DEC8_SWEDISH_CI",
    "CS_TYPE_DEC8_BIN",
    "CS_TYPE_CP850_GENERAL_CI",
    "CS_TYPE_CP850_BIN",
    "CS_TYPE_HP8_ENGLISH_CI",
    "CS_TYPE_HP8_BIN",
    "CS_TYPE_MACROMAN_GENERAL_CI",
    "CS_TYPE_MACROMAN_BIN",
    "CS_TYPE_SWE7_SWEDISH_CI",
    "CS_TYPE_SWE7_BIN",
    ]

compile_template = '''/**
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

#include "ob_str_datum_funcs_compilation.ipp"

namespace oceanbase
{
namespace common
{
%COMPILE_FUN_LIST%
} // end common
} // end oceanbase'''

common_template = '''
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

#ifndef OCEANBASE_STR_DATUM_FUNCS_IPP
#define OCEANBASE_STR_DATUM_FUNCS_IPP

#include "share/datum/ob_datum_funcs.h"
#include "share/datum/ob_datum_cmp_func_def.h"
#include "common/object/ob_obj_funcs.h"
#include "sql/engine/ob_serializable_function.h"
#include "sql/engine/ob_bit_vector.h"
#include "share/ob_cluster_version.h"
#include "share/datum/ob_datum_funcs_impl.h"

namespace oceanbase
{
using namespace sql;
namespace common
{

#define DEF_STR_FUNC_INIT(COLLATION, unit_idx)                                                 \\
  void __init_str_func##unit_idx()                                                             \\
  {                                                                                            \\
    str_cmp_initer<COLLATION>::init_array();                                                   \\
    str_basic_initer<COLLATION, 0>::init_array();                                              \\
    str_basic_initer<COLLATION, 1>::init_array();                                              \\
  }

} // end common
} // end oceanbase
#endif // OCEANBASE_STR_DATUM_FUNCS_IPP'''

COMPILE_UNIT_CNT = 8

def rm_compile_part():
  rm_str = "rm -rf ob_str_datum_funcs_compilation_*.cpp"
  rm_str2 = "rm -rf ob_str_datum_funcs_compilation.ipp"
  rm_str3 = "rm -rf ob_str_datum_funcs_all.cpp"
  os.system(rm_str)
  os.system(rm_str2)
  os.system(rm_str3)


def generate_compile_parts():
  fname_temp = "ob_str_datum_funcs_compilation_%d.cpp"
  fn_cnt = int((len(DEFINED_COLLS) + COMPILE_UNIT_CNT  - 1) / COMPILE_UNIT_CNT)
  fn_list_text = ""
  for i in range(fn_cnt):
    fn_list_text += "DEF_STR_FUNC_INIT(%COLL_NAME" + str(i) + "%, %unit_idx" + str(i) + "%);\n"
  for start in range(0, len(DEFINED_COLLS), fn_cnt):
    text = compile_template.replace("%COMPILE_FUN_LIST%", fn_list_text)
    for i in range(fn_cnt):
      coll_temp = "%COLL_NAME" + str(i) + "%"
      idx_temp = "%unit_idx" + str(i) + "%"
      if start + i >= len(DEFINED_COLLS):
        text = text.replace(coll_temp, "CS_TYPE_MAX")
      else:
        text = text.replace(coll_temp, DEFINED_COLLS[start + i])
      text = text.replace(idx_temp, str(start + i))
    f_name = fname_temp % (start / fn_cnt)
    with open(f_name, 'a') as f:
      f.write(text)


def generate_ctrl_part():
  ctrl_text = '''/**
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
#include "lib/charset/ob_charset.h"
namespace oceanbase
{
namespace common
{
'''
  for i in range(0, len(DEFINED_COLLS)):
    ctrl_text += "extern void __init_str_func%d();\n" % i

  ctrl_text += "void __init_all_str_funcs() {\n"

  for i in range(0, len(DEFINED_COLLS)):
    ctrl_text += "  __init_str_func%d();\n" % i

  ctrl_text += '''}
} // end common
} // end oceanbase'''

  with open("ob_str_datum_funcs_all.cpp", 'a') as f:
    f.write(ctrl_text)


def generate_common():
  with open("ob_str_datum_funcs_compilation.ipp", 'a') as f:
    f.write(common_template)



if __name__ == "__main__":
  rm_compile_part()
  generate_common()
  generate_compile_parts()
  generate_ctrl_part()