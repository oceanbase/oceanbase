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

#ifndef OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_H
#define OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_H

#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "common/row/ob_row.h"

namespace oceanbase
{
namespace observer
{

class ObSqlWorkareaMemoryInfoIterator
{
public:
  ObSqlWorkareaMemoryInfoIterator();
  ~ObSqlWorkareaMemoryInfoIterator() { destroy(); }
public:
  void destroy();
  void reset();
  int init(const uint64_t effective_tenant_id);
  int get_next_wa_memory_info(sql::ObSqlWorkareaCurrentMemoryInfo *&wa_stat, uint64_t &tenant_id);
private:
  int get_next_batch_wa_memory_info();
private:
  sql::ObSqlWorkareaCurrentMemoryInfo memory_info_;
  common::ObSEArray<uint64_t, 16> tenant_ids_;
  int64_t cur_nth_tenant_;
};

class ObSqlWorkareaMemoryInfo : public common::ObVirtualTableScannerIterator
{
public:
  ObSqlWorkareaMemoryInfo();
  virtual ~ObSqlWorkareaMemoryInfo() { destroy(); }

public:
  void destroy();
  void reset();
  int inner_get_next_row(common::ObNewRow *&row);
private:
  enum STORAGE_COLUMN
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    MAX_WORKAREA_SIZE,
    WORKAREA_HOLD_SIZE,
    MAX_AUTO_WORKAREA_SIZE,
    MEM_TARGET, // OB_APP_MIN_COLUMN_ID + 5
    TOTAL_MEM_USED,
    GLOBAL_MEM_BOUND,
    DRIFT_SIZE,
    WORKAREA_COUNT,
    MANUAL_CALC_COUNT,      // OB_APP_MIN_COLUMN_ID + 10
    TENAND_ID,         // OB_APP_MIN_COLUMN_ID + 11
  };
  int get_server_ip_and_port();
  int fill_row(
    uint64_t tenant_id,
    sql::ObSqlWorkareaCurrentMemoryInfo &memory_info,
    common::ObNewRow *&row);
private:
  common::ObString ipstr_;
  int32_t port_;
  ObSqlWorkareaMemoryInfoIterator iter_;
};

} /* namespace observer */
} /* namespace oceanbase */

#endif /* OB_ALL_VIRTUAL_SQL_WORKAREA_MEMORY_INFO_H */
