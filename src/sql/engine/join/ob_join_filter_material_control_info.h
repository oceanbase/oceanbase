/** * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "lib/container/ob_fixed_array.h"
#include <utility>

namespace oceanbase
{
namespace sql
{

struct ObJoinFilterMaterialControlInfo
{
  OB_UNIS_VERSION_V(1);
public:
  TO_STRING_KV(K(enable_material_), K(hash_id_), K(is_controller_), K(join_filter_count_),
               K(extra_hash_count_), K(each_sqc_has_full_data_));

public:
  // these variables for all join filter
  bool enable_material_{false};
  int16_t hash_id_{-1}; // mark the hash value position in compact row

  // these variables for the controller and hash join
  bool is_controller_{false}; // only the top join filter become the controller
  uint16_t join_filter_count_{0}; // total join filter count in the left side of a hash join
  uint16_t extra_hash_count_{0}; // hash value count(one for hash join, several for join filter)
  bool each_sqc_has_full_data_{false}; // mark whether each sqc has complete data
  bool need_sync_row_count_{false}; // if at least one join filter is shared join filter, we need to
                                    // send datahub msg to synchronize row count
};


}
}