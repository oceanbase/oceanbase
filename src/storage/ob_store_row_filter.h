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

#ifndef OB_STORE_ROW_FILTER_H_
#define OB_STORE_ROW_FILTER_H_

#include "lib/utility/ob_print_utils.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "common/ob_partition_key.h"

namespace oceanbase {
namespace common {
class ObPartMgr;
}
namespace sql {
class ObTableLocation;
class ObExecContext;
}  // namespace sql
namespace storage {
struct ObStoreRow;

class ObIStoreRowFilter {
public:
  virtual int check(const ObStoreRow& store_row, bool& is_filtered) const = 0;
  virtual int init(const sql::ObTableLocation* part_filter, sql::ObExecContext* exec_ctx, common::ObPartMgr* part_mgr,
      const common::ObPartitionKey& pkey) = 0;
  VIRTUAL_TO_STRING_KV("", "");
};

class ObStoreRowFilter : public ObIStoreRowFilter {
public:
  ObStoreRowFilter() : part_filter_(NULL), exec_ctx_(NULL), part_mgr_(NULL), pkey_()
  {}
  virtual ~ObStoreRowFilter()
  {}
  int init(const sql::ObTableLocation* part_filter, sql::ObExecContext* exec_ctx, common::ObPartMgr* part_mgr,
      const common::ObPartitionKey& pkey);
  int check(const ObStoreRow& store_row, bool& is_filtered) const override;
  TO_STRING_KV(KP_(part_filter), KP_(exec_ctx), KP_(part_mgr), K_(pkey));

private:
  const sql::ObTableLocation* part_filter_;
  sql::ObExecContext* exec_ctx_;
  common::ObPartMgr* part_mgr_;
  common::ObPartitionKey pkey_;
};

}  // namespace storage
}  // namespace oceanbase
#endif
