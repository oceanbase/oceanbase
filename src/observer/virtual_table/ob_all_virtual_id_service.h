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

#ifndef OB_ALL_VIRTUAL_ID_SERVICE_H_
#define OB_ALL_VIRTUAL_ID_SERVICE_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/tx/ob_id_service.h"
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
class ObAllVirtualIDService: public common::ObVirtualTableScannerIterator
{
public:
  explicit ObAllVirtualIDService() { reset(); }
  virtual ~ObAllVirtualIDService() { destroy(); }
public:
  virtual void reset();
  virtual void destroy() { reset(); }
  virtual int inner_get_next_row(common::ObNewRow *&row);
  static const int64_t ID_SERVICE_NUM = 2;
  TO_STRING_KV("tenant_id", cur_tenant_id_,
               "service_type", service_types_index_,
               "last_id", last_id_,
               "limit_id", limit_id_,
               "rec_log_ts", rec_log_ts_,
               "latest_log_ts", latest_log_ts_,
               "pre_allocated_range", pre_allocated_range_,
               "submit_log_ts", submit_log_ts_,
               "is_master", is_master_);
private:
  int prepare_start_to_read_();
  int get_next_tenant_id_info_();
  int fill_tenant_ids_();
private:
  bool init_;
  int64_t tenant_ids_index_;
  int64_t service_types_index_;
  int64_t service_type_[transaction::ObIDService::MAX_SERVICE_TYPE];
  int64_t expire_time_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  int64_t cur_tenant_id_;
  int64_t last_id_;
  int64_t limit_id_;
  share::SCN rec_log_ts_;
  share::SCN latest_log_ts_;
  int64_t pre_allocated_range_;
  int64_t submit_log_ts_;
  bool is_master_;
  common::ObArray<uint64_t> all_tenants_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualIDService);
};
} // observer
} // oceanbase
#endif // OB_ALL_VIRTUAL_ID_SERVICE_H_
