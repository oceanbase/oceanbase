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

#ifndef OB_ALL_VIRTUAL_HMS_CLIENT_POOL_STAT_H_
#define OB_ALL_VIRTUAL_HMS_CLIENT_POOL_STAT_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "share/ob_scanner.h"

namespace oceanbase
{
namespace share
{
  struct ObHMSClientPoolKey;
  class ObHMSClientPool;  // Forward declaration
}

namespace observer
{
using namespace oceanbase::share;
class ObHMSClientPoolGetter
{
public:
  explicit ObHMSClientPoolGetter(common::ObScanner &scanner,
                        common::ObIArray<uint64_t> &output_column_ids,
                        char *svr_ip,
                        int32_t port,
                        common::ObNewRow &cur_row,
                        uint64_t effective_tenant_id)
      : scanner_(scanner), output_column_ids_(output_column_ids), svr_ip_(svr_ip), port_(port),
        cur_row_(cur_row), effective_tenant_id_(effective_tenant_id)
  {
  }
  virtual ~ObHMSClientPoolGetter() {};
  int operator() (common::hash::HashMapPair<ObHMSClientPoolKey, share::ObHMSClientPool*> &entry);
private:
  common::ObScanner &scanner_;
  common::ObIArray<uint64_t> &output_column_ids_;
  char *svr_ip_;
  int32_t port_;
  common::ObNewRow &cur_row_;
  uint64_t effective_tenant_id_;
  DISALLOW_COPY_AND_ASSIGN(ObHMSClientPoolGetter);
};

class ObAllVirtualHMSClientPoolStat : public common::ObVirtualTableScannerIterator
{
  friend class ObHMSClientPoolGetter;
public:
  ObAllVirtualHMSClientPoolStat();
  virtual ~ObAllVirtualHMSClientPoolStat();
  void destroy();
  virtual void reset() override;
  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual int inner_open() override;
private:
  enum COLUMNS
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    CATALOG_ID,
    TOTAL_CLIENTS,
    IN_USE_CLIENTS,
    IDLE_CLIENTS,
  };
  int32_t port_;
  char svr_ip_[common::OB_IP_STR_BUFF];
  int fill_scanner(const uint64_t &tenant_id);
  common::ObSEArray<uint64_t, 16> tenant_ids_;
  int64_t tenant_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualHMSClientPoolStat);
};
} // namespace observer
} // namespace oceanbase

#endif // OB_ALL_VIRTUAL_HMS_CLIENT_POOL_STAT_H_