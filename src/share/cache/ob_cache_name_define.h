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

#ifndef OCEANBASE_CACHE_OB_CACHE_NAME_DEFINE_H_
#define OCEANBASE_CACHE_OB_CACHE_NAME_DEFINE_H_

namespace oceanbase
{
namespace share
{
const char *const OB_LOCATION_CACHE_NAME = "location_cache";
const char *const OB_SCHEMA_CACHE_NAME = "schema_cache";
const char *const OB_SCHEMA_HISTORY_CACHE_NAME = "schema_history_cache";
const char *const OB_LS_LOCATION_CACHE_NAME = "ls_location_cache";
const char *const OB_TABLET_CACHE_NAME = "tablet_ls_cache";
const char *const OB_TABLET_TABLE_CACHE_NAME = "tablet_table_cache";
const char *const OB_VTABLE_CACHE_NAME = "vtable_cache";
}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_CACHE_OB_CACHE_NAME_DEFINE_H_
