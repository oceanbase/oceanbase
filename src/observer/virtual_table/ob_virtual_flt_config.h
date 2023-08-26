/*
 * Copyright (c) 2022 OceanBase Technology Co.,Ltd.
 * OceanBase is licensed under Mulan PubL v1.
 * You can use this software according to the terms and conditions of the Mulan PubL v1.
 * You may obtain a copy of Mulan PubL v1 at:
 *          http://license.coscl.org.cn/MulanPubL-1.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v1 for more details.
 * ---------------------------------------------------------------------------------------
 * Authors:
 *   Juehui <>
 * ---------------------------------------------------------------------------------------
 */
#ifndef OB_VIRTUAL_FLT_CONF_H_
#define OB_VIRTUAL_FLT_CONF_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "sql/monitor/flt/ob_flt_control_info_mgr.h"
using namespace oceanbase::sql;
namespace oceanbase
{
namespace observer
{
  class ObVirtualFLTConfig : public common::ObVirtualTableScannerIterator
  {
    public:
      ObVirtualFLTConfig();
      virtual ~ObVirtualFLTConfig();
      int inner_open();
      virtual void reset();
      int inner_get_next_row() { return get_row_from_tenants(); }
      int get_row_from_tenants();
      int fill_cells(ObFLTConfRec &record);
      int get_row_from_specified_tenant(uint64_t tenant_id, bool &is_end);
      int inner_get_next_row(common::ObNewRow *&row);
    private:
      enum SYS_COLUMN
      {
        TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
        TYPE,
        MODULE_NAME,
        ACTION_NAME,
        CLIENT_IDENTIFIER,
        LEVEL,
        SAMPLE_PERCENTAGE,
        RECORD_POLICY
      };
      common::ObObj cells_[common::OB_ROW_MAX_COLUMNS_COUNT];
      //static const char FOLLOW[] = "FOLLOW";
      //static const char CHILD[] = "CHILD";
      DISALLOW_COPY_AND_ASSIGN(ObVirtualFLTConfig);
      ObSEArray<ObFLTConfRec, 16> rec_list_;
      int64_t rec_array_idx_;
      common::ObSEArray<uint64_t, 16> tenant_id_array_;
      int64_t tenant_id_array_idx_;
  };
} /* namespace observer */
} /* namespace oceanbase */
#endif /* OB_VIRTUAL_FLT_CONF_H_ */
