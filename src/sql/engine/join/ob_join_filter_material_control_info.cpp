/** * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "sql/engine/join/ob_join_filter_material_control_info.h"

using namespace oceanbase::lib;
using namespace oceanbase;

namespace oceanbase
{
namespace sql
{
OB_SERIALIZE_MEMBER(ObJoinFilterMaterialControlInfo, enable_material_, hash_id_, is_controller_,
                    join_filter_count_, extra_hash_count_, each_sqc_has_full_data_,
                    need_sync_row_count_);
}
}