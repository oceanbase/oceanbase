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

#ifndef _OB_TIERED_METADATA_STORE_SCHEMA_H_
#define _OB_TIERED_METADATA_STORE_SCHEMA_H_

#include "share/ob_define.h"
#include "ob_inner_table_schema_constants.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{

namespace share
{

const uint64_t OB_ALL_TIERED_METADATA_STORE_TID = 578;
const char *const OB_ALL_TIERED_METADATA_STORE_TNAME = "__all_tiered_metadata_store";

class ObTieredMetadataStoreTableSchema
{
public:
  static int all_tiered_metadata_store_schema(share::schema::ObTableSchema &table_schema);
};

} // end namespace share
} // end namespace oceanbase
#endif /* _OB_TIERED_METADATA_STORE_SCHEMA_H_ */