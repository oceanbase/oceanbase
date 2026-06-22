/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  if (OB_FAIL(inner_lob_map.create(380, ObModIds::OB_INNER_LOB_HASH_SET))) {
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
