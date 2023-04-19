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
#include "storage/tx/ob_timestamp_access.h"
#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
}
}

namespace observer
{
class ObAllVirtualTimestampService: public common::ObVirtualTableScannerIterator
{
public:
  explicit ObAllVirtualTimestampService() { reset(); }
  virtual ~ObAllVirtualTimestampService() { destroy(); }
public:
  virtual void reset();
  virtual void destroy() { reset(); }
  virtual int inner_get_next_row(common::ObNewRow *&row);
  TO_STRING_KV("tenant_id", cur_tenant_id_, K_(ts_value),
               K_(service_role), K_(is_primary),
               K_(role), K_(service_epoch));
private:
  int prepare_start_to_read_();
  int get_next_tenant_id_info_();
  int fill_tenant_ids_();
private:
  bool init_;
  int64_t tenant_ids_index_;
  int64_t expire_time_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  int64_t cur_tenant_id_;
  int64_t ts_value_;
  transaction::ObTimestampAccess::ServiceType service_role_;
  bool is_primary_;
  common::ObRole role_;
  char role_str_[32];
  int64_t service_epoch_;
  common::ObArray<uint64_t> all_tenants_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTimestampService);
};
} // observer
} // oceanbase
#endif // OB_ALL_VIRTUAL_TIMESTAMP_SERVICE_H_
