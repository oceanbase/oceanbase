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

#include "ob_all_virtual_tablet_stat.h"
#include "share/ob_errno.h"
#include "storage/ob_tenant_tablet_stat_mgr.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace observer
{
ObAllVirtualTabletStat::ObAllVirtualTabletStat()
  : ip_buf_(),
    tablet_stats_(),
    cur_stat_(),
    cur_idx_(0),
    need_collect_stats_(true)
{
}

ObAllVirtualTabletStat::~ObAllVirtualTabletStat()
{
  reset();
}

int ObAllVirtualTabletStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    if (ret != OB_ITER_END) {
      SERVER_LOG(WARN, "execute fail", K(ret));
    }
  }
  return ret;
}

void ObAllVirtualTabletStat::reset()
{
  omt::ObMultiTenantOperator::reset();
  MEMSET(ip_buf_, 0, sizeof(ip_buf_));
  tablet_stats_.reset();
  cur_idx_ = 0;
  ObVirtualTableScannerIterator::reset();
}

bool ObAllVirtualTabletStat::is_need_process(uint64_t tenant_id)
{
  bool bret = false;
  if (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_) {
    bret = true;
  }
  return bret;
}

void ObAllVirtualTabletStat::release_last_tenant()
{
  tablet_stats_.reset();
  cur_idx_ = 0;
  need_collect_stats_ = true;
}

int ObAllVirtualTabletStat::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  const uint64_t tenant_id = MTL_ID();
  if (need_collect_stats_) {
    tablet_stats_.reset();
    if (OB_FAIL(MTL(ObTenantTabletStatMgr *)->get_all_tablet_stats(tablet_stats_))) {
      SERVER_LOG(WARN, "failed to get all tablet stats", K(ret), K(tenant_id));
    } else {
      need_collect_stats_ = false;
      cur_idx_ = 0;
    }
  }
  if (OB_SUCC(ret)) {
    if (cur_idx_ < tablet_stats_.count()) {
      cur_stat_ = tablet_stats_.at(cur_idx_);
      cur_idx_++;
    } else {
      ret = OB_ITER_END;
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t col_cnt = output_column_ids_.count();
    ObObj *cells = cur_row_.cells_;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
      case SVR_IP:
        if (ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
          cells[i].set_varchar(ip_buf_);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      case SVR_PORT:
        cells[i].set_int(ObServerConfig::get_instance().self_addr_.get_port());
        break;
      case TENANT_ID:
        cells[i].set_int(tenant_id);
        break;
      case LS_ID:
        cells[i].set_int(cur_stat_.ls_id_);
        break;
      case TABLET_ID:
        cells[i].set_int(cur_stat_.tablet_id_);
        break;
      case QUERY_CNT:
        cells[i].set_int(cur_stat_.query_cnt_);
        break;
      case MINI_MERGE_CNT:
        cells[i].set_int(cur_stat_.merge_cnt_);
        break;
      case SCAN_OUTPUT_ROW_CNT:
        cells[i].set_int(cur_stat_.scan_logical_row_cnt_);
        break;
      case SCAN_TOTAL_ROW_CNT:
        cells[i].set_int(cur_stat_.scan_physical_row_cnt_);
        break;
      case PUSHDOWN_MICRO_BLOCK_CNT:
        cells[i].set_int(cur_stat_.scan_micro_block_cnt_);
        break;
      case TOTAL_MICRO_BLOCK_CNT:
        cells[i].set_int(cur_stat_.pushdown_micro_block_cnt_);
        break;
      case EXIST_ITER_TABLE_CNT:
        cells[i].set_int(cur_stat_.exist_row_total_table_cnt_);
        break;
      case EXIST_TOTAL_TABLE_CNT:
        cells[i].set_int(cur_stat_.exist_row_read_table_cnt_);
        break;
      case INSERT_ROW_CNT:
        cells[i].set_int(cur_stat_.insert_row_cnt_);
        break;
      case UPDATE_ROW_CNT:
        cells[i].set_int(cur_stat_.update_row_cnt_);
        break;
      case DELETE_ROW_CNT:
        cells[i].set_int(cur_stat_.delete_row_cnt_);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid column id", K(ret), K(col_id));
        break;
      } // end switch
    } // end for
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}


} // observer
} // oceanbase
