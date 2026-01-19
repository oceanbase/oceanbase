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

#ifndef OCEANBASE_STORAGE_OB_TX_DATA_UTIL_H_
#define OCEANBASE_STORAGE_OB_TX_DATA_UTIL_H_

#include "share/scn.h"

namespace oceanbase {
namespace storage {
class ObTablet;

class ObTxDataUtil {
public:
  static bool tx_data_need_recycle(const share::SCN &new_fill_tx_scn, const share::SCN &min_filled_tx_scn);
  static bool tablet_is_normal_status(const ObTablet &tablet);
};
}  // namespace storage
}  // namespace oceanbase

#endif