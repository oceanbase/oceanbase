/**
 * Copyright (c) 2026 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

 #ifndef _OB_OCEANBASE_SCHEMA_SCHEMA_STRUCT_FTS_H
 #define _OB_OCEANBASE_SCHEMA_SCHEMA_STRUCT_FTS_H

 #include <cstdint>
 #include "lib/utility/ob_print_utils.h"

 namespace oceanbase
 {
 namespace share
 {
 namespace schema
 {

 enum ObFTSIndexType : uint8_t
 {
   OB_FTS_INDEX_TYPE_INVALID = 0,
   OB_FTS_INDEX_TYPE_FILTER = 1,
   OB_FTS_INDEX_TYPE_MATCH = 2,
   OB_FTS_INDEX_TYPE_PHRASE_MATCH = 3,
   OB_FTS_INDEX_TYPE_MAX
 };

 } // namespace schema
 } // namespace share
 } // namespace oceanbase

  #endif // _OB_OCEANBASE_SCHEMA_SCHEMA_STRUCT_FTS_H