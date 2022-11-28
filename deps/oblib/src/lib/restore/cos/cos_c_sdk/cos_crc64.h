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

#ifndef LIBCOS_CRC_H
#define LIBCOS_CRC_H

#include "cos_sys_define.h"


COS_CPP_START

uint64_t cos_crc64(uint64_t crc, void *buf, size_t len);
uint64_t cos_crc64_combine(uint64_t crc1, uint64_t crc2, uintmax_t len2);

COS_CPP_END

#endif
