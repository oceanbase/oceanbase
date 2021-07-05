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

#ifndef OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_H_
#define OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/container/ob_array.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share
namespace transaction {
class ObTransService;
}
namespace observer {
class ObAllVirtualTimestampService : public common::ObVirtualTableScannerIterator {
public:
  explicit ObAllVirtualTimestampService(transaction::ObTransService* trans_service);
  virtual ~ObAllVirtualTimestampService();

public:
  virtual void reset();
  virtual void destroy();
  virtual int inner_get_next_row(common::ObNewRow*& row);
  int init(share::schema::ObMultiVersionSchemaService& schema_service);

private:
  int prepare_start_to_read_();
  int get_next_tenant_ts_info_();
  int fill_tenant_ids_();

private:
  bool init_;
  transaction::ObTransService* trans_service_;
  int64_t tenant_ids_index_;
  transaction::MonotonicTs stc_;
  int64_t expire_time_;
  int64_t cur_tenant_id_;
  int64_t ts_value_;
  int64_t ts_type_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  common::ObArray<uint64_t> all_tenants_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTimestampService);
};
}  // namespace observer
}  // namespace oceanbase
#endif  // OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_H_
