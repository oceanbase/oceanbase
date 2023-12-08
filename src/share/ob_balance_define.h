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

#ifndef OCEANBASE_SHARE_OB_BALANCE_DEFINE_H_
#define OCEANBASE_SHARE_OB_BALANCE_DEFINE_H_

#include "share/ob_common_id.h"             // ObCommonID
#include "share/schema/ob_table_schema.h"   // ObSimpleTableSchemaV2

namespace oceanbase
{
namespace share
{
typedef ObCommonID ObBalanceJobID;
typedef ObCommonID ObBalanceTaskID;
typedef ObCommonID ObTransferTaskID;
typedef ObCommonID ObTransferPartitionTaskID;

// check Tables that need balance by RS
//
// 1. USER TABLE: user created table, need balance
// 2. GLOBAL INDEX: global index is distributed independently from the main table, need balance
// 3. TMP TABLE: temp table is created by user, need balance
bool need_balance_table(const schema::ObSimpleTableSchemaV2 &table_schema);
bool check_if_need_balance_table(
    const schema::ObSimpleTableSchemaV2 &table_schema,
    const char *&table_type_str);
}
}

#endif /* !OCEANBASE_SHARE_OB_BALANCE_DEFINE_H_ */
