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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_QUERY_RESPONSE_TIME_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_QUERY_RESPONSE_TIME_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_tenant_mgr.h"
#include "observer/mysql/ob_query_response_time.h"

namespace oceanbase {
namespace common {
class ObObj;

}
namespace share {
namespace schema {
class ObTableSchema;
class ObDatabaseSchema;

}  // namespace schema
}  // namespace share

namespace observer {

class ObInfoSchemaQueryResponseTimeTable : public common::ObVirtualTableScannerIterator {
public:
  ObInfoSchemaQueryResponseTimeTable();
  virtual ~ObInfoSchemaQueryResponseTimeTable();
  virtual int inner_open() override;
  virtual int inner_get_next_row(common::ObNewRow*& row) override;
  virtual void reset() override;
  int set_ip(common::ObAddr* addr);
  inline void set_addr(common::ObAddr& addr)
  {
    addr_ = &addr;
  }
  
private:
  enum SYS_COLUMN {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    SVR_IP,
    SVR_PORT,
    QUERY_RESPPONSE_TIME,
    COUNT,
    TOTAL
  };
  common::ObAddr* addr_;
  common::ObString ipstr_;
  int32_t port_;
  uint64_t tenant_id_;
  common::hash::ObHashMap<uint64_t, ObRSTTimeCollector*>::iterator collector_iter_;
  int32_t utility_iter_;
};

}  // namespace observer
}  // namespace oceanbase
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_INFORMATION_QUERY_RESPONSE_TIME_ */
