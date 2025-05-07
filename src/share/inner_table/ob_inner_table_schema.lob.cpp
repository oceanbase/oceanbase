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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "ob_inner_table_schema.h"

namespace oceanbase
{
namespace share
{
inner_lob_map_t inner_lob_map;
bool lob_mapping_init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_lob_map.create(299, ObModIds::OB_INNER_LOB_HASH_SET))) {
    SERVER_LOG(WARN, "fail to create inner lob map", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(lob_aux_table_mappings); ++i) {
      if (OB_FAIL(inner_lob_map.set_refactored(lob_aux_table_mappings[i].data_table_tid_, lob_aux_table_mappings[i]))) {
        SERVER_LOG(WARN, "fail to set inner lob map", K(ret), K(i));
      }
    }
  }
  return (ret == OB_SUCCESS);
} // end define lob_mappings

bool inited_lob = lob_mapping_init();

} // end namespace share
} // end namespace oceanbase
