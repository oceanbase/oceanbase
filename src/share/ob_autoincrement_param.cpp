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

#include "share/ob_autoincrement_param.h"

namespace oceanbase {
namespace share {

OB_SERIALIZE_MEMBER(AutoincParam, tenant_id_, autoinc_table_id_, autoinc_table_part_num_, autoinc_col_id_,
    autoinc_col_index_, autoinc_update_col_index_, autoinc_col_type_, total_value_count_, autoinc_desired_count_,
    autoinc_old_value_index_, autoinc_increment_, autoinc_offset_, autoinc_first_part_num_, part_level_, pkey_,
    auto_increment_cache_size_);

}  // end namespace share
}  // end namespace oceanbase
