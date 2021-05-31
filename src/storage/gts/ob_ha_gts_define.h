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

#ifndef OCEANBASE_GTS_OB_HA_GTS_DEFINE_H_
#define OCEANBASE_GTS_OB_HA_GTS_DEFINE_H_

#include "lib/utility/utility.h"

namespace oceanbase {
namespace gts {
typedef common::ObIntWarp ObGtsID;
typedef common::ObIntWarp ObTenantID;
typedef common::ObIntWarp ObGtsReqID;
const int64_t HA_GTS_SOURCE_LEASE = 60 * 1000 * 1000 - 2 * 1000 * 1000;  // 58s
const int64_t AUTO_CHANGE_MEMBER_INTERVAL = 1 * 1000 * 1000;             // 1s
const int64_t GTS_OFFLINE_THRESHOLD = 1 * 1000 * 1000;
const int64_t INTERVAL_WITH_CREATED_TS = 30 * 1000 * 1000;  // 30s
}  // namespace gts
}  // namespace oceanbase

#endif  // OCEANBASE_GTS_OB_HA_GTS_DEFINE_H_
