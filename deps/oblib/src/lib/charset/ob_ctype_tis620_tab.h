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

#ifndef CHARSET_TIS620_TAB_H_
#define CHARSET_TIS620_TAB_H_

#include "lib/charset/ob_ctype.h"

#define TOT_LEVELS 5


/* level 1 symbols & order */
enum l1_symbols {
  L1_08 = TOT_LEVELS,
  L1_18,
  L1_28,
  L1_38,
  L1_48,
  L1_58,
  L1_68,
  L1_78,
  L1_88,
  L1_98,
  L1_A8,
  L1_B8,
  L1_C8,
  L1_D8,
  L1_E8,
  L1_F8,
  L1_G8,
  L1_H8,
  L1_I8,
  L1_J8,
  L1_K8,
  L1_L8,
  L1_M8,
  L1_N8,
  L1_O8,
  L1_P8,
  L1_Q8,
  L1_R8,
  L1_S8,
  L1_T8,
  L1_U8,
  L1_V8,
  L1_W8,
  L1_X8,
  L1_Y8,
  L1_Z8,
  L1_KO_KAI,
  L1_KHO_KHAI,
  L1_KHO_KHUAT,
  L1_KHO_KHWAI,
  L1_KHO_KHON,
  L1_KHO_RAKHANG,
  L1_NGO_NGU,
  L1_CHO_CHAN,
  L1_CHO_CHING,
  L1_CHO_CHANG,
  L1_SO_SO,
  L1_CHO_CHOE,
  L1_YO_YING,
  L1_DO_CHADA,
  L1_TO_PATAK,
  L1_THO_THAN,
  L1_THO_NANGMONTHO,
  L1_THO_PHUTHAO,
  L1_NO_NEN,
  L1_DO_DEK,
  L1_TO_TAO,
  L1_THO_THUNG,
  L1_THO_THAHAN,
  L1_THO_THONG,
  L1_NO_NU,
  L1_BO_BAIMAI,
  L1_PO_PLA,
  L1_PHO_PHUNG,
  L1_FO_FA,
  L1_PHO_PHAN,
  L1_FO_FAN,
  L1_PHO_SAMPHAO,
  L1_MO_MA,
  L1_YO_YAK,
  L1_RO_RUA,
  L1_RU,
  L1_LO_LING,
  L1_LU,
  L1_WO_WAEN,
  L1_SO_SALA,
  L1_SO_RUSI,
  L1_SO_SUA,
  L1_HO_HIP,
  L1_LO_CHULA,
  L1_O_ANG,
  L1_HO_NOKHUK,
  L1_NKHIT,
  L1_SARA_A,
  L1_MAI_HAN_AKAT,
  L1_SARA_AA,
  L1_SARA_AM,
  L1_SARA_I,
  L1_SARA_II,
  L1_SARA_UE,
  L1_SARA_UEE,
  L1_SARA_U,
  L1_SARA_UU,
  L1_SARA_E,
  L1_SARA_AE,
  L1_SARA_O,
  L1_SARA_AI_MAIMUAN,
  L1_SARA_AI_MAIMALAI
};

/* level 2 symbols & order */
enum l2_symbols {
  L2_BLANK = TOT_LEVELS,
  L2_THAII,
  L2_YAMAK,
  L2_PINTHU,
  L2_GARAN,
  L2_TYKHU,
  L2_TONE1,
  L2_TONE2,
  L2_TONE3,
  L2_TONE4
};

/* level 3 symbols & order */
enum l3_symbols {
  L3_BLANK = TOT_LEVELS,
  L3_SPACE,
  L3_NB_SACE,
  L3_LOW_LINE,
  L3_HYPHEN,
  L3_COMMA,
  L3_SEMICOLON,
  L3_COLON,
  L3_EXCLAMATION,
  L3_QUESTION,
  L3_SOLIDUS,
  L3_FULL_STOP,
  L3_PAIYAN_NOI,
  L3_MAI_YAMOK,
  L3_GRAVE,
  L3_CIRCUMFLEX,
  L3_TILDE,
  L3_APOSTROPHE,
  L3_QUOTATION,
  L3_L_PARANTHESIS,
  L3_L_BRACKET,
  L3_L_BRACE,
  L3_R_BRACE,
  L3_R_BRACKET,
  L3_R_PARENTHESIS,
  L3_AT,
  L3_BAHT,
  L3_DOLLAR,
  L3_FONGMAN,
  L3_ANGKHANKHU,
  L3_KHOMUT,
  L3_ASTERISK,
  L3_BK_SOLIDUS,
  L3_AMPERSAND,
  L3_NUMBER,
  L3_PERCENT,
  L3_PLUS,
  L3_LESS_THAN,
  L3_EQUAL,
  L3_GREATER_THAN,
  L3_V_LINE
};

/* level 4 symbols & order */
enum l4_symbols { L4_BLANK = TOT_LEVELS, L4_MIN, L4_CAP, L4_EXT };

enum level_symbols { L_UPRUPR = TOT_LEVELS, L_UPPER, L_MIDDLE, L_LOWER };

extern const int t_ctype[][TOT_LEVELS];
extern unsigned char ctype_tis620[257];
extern unsigned char to_lower_tis620[];
extern unsigned char to_upper_tis620[];
extern unsigned char sort_order_tis620[];
extern unsigned short cs_to_uni_tis620[256];
extern unsigned char *uni_to_cs_tis620[256];
#endif  // CHARSET_TIS620_TAB_H_
