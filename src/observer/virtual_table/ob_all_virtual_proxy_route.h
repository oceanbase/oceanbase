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

#ifndef OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_ROUTE_
#define OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_ROUTE_

#include "ob_all_virtual_proxy_base.h"

namespace oceanbase {
namespace observer {
class ObAllVirtualProxyRoute : public ObAllVirtualProxyBaseIterator {
  enum ALL_VIRTUAL_PROXY_ROUTE_TABLE_COLUMNS {
    SQL_STRING = oceanbase::common::OB_APP_MIN_COLUMN_ID,
    TENANT_NAME,
    DATABASE_NAME,
    TABLE_NAME,
    TABLE_ID,
    CALCULATOR_BIN,
    RESULT_STATUS,
    SPARE1,
    SPARE2,
    SPARE3,
    SPARE4,
    SPARE5,
    SPARE6,
  };
  enum ALL_VIRTUAL_PROXY_ROUTE_ROWKEY_IDX {
    SQL_STRING_IDX = 0,
    TENANT_NAME_IDX,
    DATABASE_NAME_IDX,
    ROW_KEY_COUNT,
  };

public:
  ObAllVirtualProxyRoute();
  virtual ~ObAllVirtualProxyRoute();

  virtual int inner_open();
  virtual int inner_get_next_row();

  int fill_cells();

private:
  bool is_iter_end_;
  common::ObString sql_string_;
  common::ObString tenant_name_;
  common::ObString database_name_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualProxyRoute);
};

}  // end of namespace observer
}  // end of namespace oceanbase
#endif /* OCEANBASE_OBSERVER_ALL_VIRTUAL_PROXY_ROUTE_ */
