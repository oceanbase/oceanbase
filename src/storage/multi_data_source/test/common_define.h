/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef UNITTEST_STORAGE_MULTI_DATA_SOURCE_COMMON_DEFINE_H
#define UNITTEST_STORAGE_MULTI_DATA_SOURCE_COMMON_DEFINE_H
#include "src/share/scn.h"

namespace oceanbase {
namespace unittest {

inline share::SCN mock_scn(int64_t val) { share::SCN scn; scn.convert_for_gts(val); return scn; }

}
}
#endif