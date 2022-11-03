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

#include "storage/tablet/ob_tablet_common.h"

namespace oceanbase
{
namespace storage
{
const int64_t ObTabletCommon::DIRECT_GET_COMMITTED_TABLET_TIMEOUT_US;
const int64_t ObTabletCommon::NO_CHECK_GET_TABLET_TIMEOUT_US;
const int64_t ObTabletCommon::DEFAULT_GET_TABLET_TIMEOUT_US;
const int64_t ObTabletCommon::FINAL_TX_ID;
} // namespace storage
} // namespace oceanbase