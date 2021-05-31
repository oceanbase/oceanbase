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

#ifndef OCEANBASE_UNITTEST_OB_DML_CHECK_H_
#define OCEANBASE_UNITTEST_OB_DML_CHECK_H_

#include "common/row/ob_row_iterator.h"
#include "storage/ob_i_store.h"
#include "storage/ob_i_partition_storage.h"
#include "mock_ob_server.h"
#include "mock_ob_end_trans_callback.h"

namespace oceanbase {
namespace unittest {
void check_result_iter(common::ObNewRowIterator& result, storage::ObStoreRowIterator& expected);
void do_scan_check(const common::ObPartitionKey& pkey, const char* scan_str, storage::ObTableScanParam& scan_param,
    MockObServer& server);
// void do_create_check(
// const common::ObPartitionKey &pkey,
// const ObVersion &version,
// const ObMemberList &member_list,
// const int64_t replica_num,
// MockObServer &server);
void do_insert_check(const common::ObPartitionKey& pkey, const char* ins_str,
    const common::ObIArray<uint64_t>& column_ids, MockObServer& server);
void do_insert_duplicate_check(const common::ObPartitionKey& pkey, const common::ObIArray<uint64_t>& column_ids,
    const common::ObIArray<uint64_t>& duplicated_column_ids, const common::ObNewRow& row, const ObInsertFlag flag,
    const char* duplicate_str,  // NULL if no confliction
    MockObServer& server);

}  // namespace unittest
}  // namespace oceanbase

#endif
