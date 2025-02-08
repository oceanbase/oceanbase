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

#ifndef CHARSET_TIS620_H_
#define CHARSET_TIS620_H_

#include "lib/charset/ob_ctype.h"
#define LAST_LEVEL 4 /* TOT_LEVELS - 1 */
/* 这一段define会和别人冲突的厉害 */
#define _is(c) (t_ctype[(c)][LAST_LEVEL])
#define _level 8
#define _consnt 16
#define _ldvowel 32
#define _fllwvowel 64
#define _uprvowel 128
#define _lwrvowel 256
#define _tone 512
#define _diacrt1 1024
#define _diacrt2 2048
#define _combine 4096
#define _stone 8192
#define _tdig 16384
#define _rearvowel (_fllwvowel | _uprvowel | _lwrvowel)
#define _diacrt (_diacrt1 | _diacrt2)
#define levelof(c) (_is(c) & _level)
#define isthai(c) ((c) >= 128)
#define istalpha(c) \
  (_is(c) & (_consnt | _ldvowel | _rearvowel | _tone | _diacrt1 | _diacrt2))
#define isconsnt(c) (_is(c) & _consnt)
#define isldvowel(c) (_is(c) & _ldvowel)
#define isfllwvowel(c) (_is(c) & _fllwvowel)
#define ismidvowel(c) (_is(c) & (_ldvowel | _fllwvowel))
#define isuprvowel(c) (_is(c) & _uprvowel)
#define islwrvowel(c) (_is(c) & _lwrvowel)
#define isuprlwrvowel(c) (_is(c) & (_lwrvowel | _uprvowel))
#define isrearvowel(c) (_is(c) & _rearvowel)
#define isvowel(c) (_is(c) & (_ldvowel | _rearvowel))
#define istone(c) (_is(c) & _tone)
#define isunldable(c) (_is(c) & (_rearvowel | _tone | _diacrt1 | _diacrt2))
#define iscombinable(c) (_is(c) & _combine)
#define istdigit(c) (_is(c) & _tdig)
#define isstone(c) (_is(c) & _stone)
#define isdiacrt1(c) (_is(c) & _diacrt1)
#define isdiacrt2(c) (_is(c) & _diacrt2)
#define isdiacrt(c) (_is(c) & _diacrt)


#define M L_MIDDLE
//#define U L_UPPER
#define L L_LOWER
#define UU L_UPRUPR
#define X L_MIDDLE

#endif  // CHARSET_TIS620_H_
