/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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