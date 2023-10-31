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

#ifndef OB_ALL_VIRTUAL_TABLET_STAT_H_
#define OB_ALL_VIRTUAL_TABLET_STAT_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/ob_tenant_tablet_stat_mgr.h"
#include "observer/omt/ob_multi_tenant_operator.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualTabletStat : public common::ObVirtualTableScannerIterator,
                               public omt::ObMultiTenantOperator
{
public:
  enum COLUMN_ID_LIST
  {
    SVR_IP = common::OB_APP_MIN_COLUMN_ID,
    SVR_PORT,
    TENANT_ID,
    LS_ID,
    TABLET_ID,
    QUERY_CNT,
    MINI_MERGE_CNT,
    SCAN_OUTPUT_ROW_CNT,
    SCAN_TOTAL_ROW_CNT,
    PUSHDOWN_MICRO_BLOCK_CNT,
    TOTAL_MICRO_BLOCK_CNT,
    EXIST_ITER_TABLE_CNT,
    EXIST_TOTAL_TABLE_CNT,
    INSERT_ROW_CNT,
    UPDATE_ROW_CNT,
    DELETE_ROW_CNT
  };
  ObAllVirtualTabletStat();
  virtual ~ObAllVirtualTabletStat();
  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
private:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual void release_last_tenant() override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
private:
  char ip_buf_[common::OB_IP_STR_BUFF];
  // each tenant has a maximum 20000 of stats, and each stat occupies 88bytes memory, so the maximum memory usage is around 2.75MB.
  common::ObSEArray<storage::ObTabletStat, 128> tablet_stats_;
  ObTabletStat cur_stat_;
  int64_t cur_idx_;
  bool need_collect_stats_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTabletStat);
};


} // observer
} // oceanbase

#endif // OB_ALL_VIRTUAL_TABLET_STAT_H_
