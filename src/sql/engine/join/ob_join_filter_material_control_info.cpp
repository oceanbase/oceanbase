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