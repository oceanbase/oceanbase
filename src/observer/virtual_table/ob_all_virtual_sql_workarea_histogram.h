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

#ifndef OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_H
#define OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_H

#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/row/ob_row.h"

namespace oceanbase
{
namespace observer
{

class ObSqlWorkareaHistogramIterator
{
public:
  ObSqlWorkareaHistogramIterator();
  ~ObSqlWorkareaHistogramIterator() { destroy(); }
public:
  void destroy();
  void reset();
  int init(const uint64_t effective_tenant_id);
  int get_next_wa_histogram(sql::ObWorkareaHistogram *&wa_histogram, uint64_t &tenant_id);
private:
  int get_next_batch_wa_histograms();
private:
  common::ObSEArray<sql::ObWorkareaHistogram, 32> wa_histograms_;
  common::ObSEArray<uint64_t, 16> tenant_ids_;
  int64_t cur_nth_wa_hist_;
  int64_t cur_nth_tenant_;
};

class ObSqlWorkareaHistogram : public common::ObVirtualTableScannerIterator
{
public:
  ObSqlWorkareaHistogram();
  virtual ~ObSqlWorkareaHistogram() { destroy(); }

public:
  void destroy();
  void reset();
  int inner_get_next_row(common::ObNewRow *&row);

private:
  enum STORAGE_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    LOW_OPTIMAL_SIZE,
    HIGH_OPTIMAL_SIZE,
    OPTIMAL_EXECUTIONS,
    ONEPASS_EXECUTIONS, // OB_APP_MIN_COLUMN_ID + 5
    MULTIPASSES_EXECUTIONS,
    TOTAL_EXECUTIONS,
    TENAND_ID,         // OB_APP_MIN_COLUMN_ID + 18
  };
  int fill_row(
    uint64_t tenant_id,
    sql::ObWorkareaHistogram &wa_histogram,
    common::ObNewRow *&row);
  int get_server_ip_and_port();
private:
  common::ObString ipstr_;
  int32_t port_;
  ObSqlWorkareaHistogramIterator iter_;
};


} /* namespace observer */
} /* namespace oceanbase */

#endif /* OB_ALL_VIRTUAL_SQL_WORKAREA_HISTOGRAM_H */
