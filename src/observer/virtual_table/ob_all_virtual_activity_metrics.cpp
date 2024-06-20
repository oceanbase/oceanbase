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

#include "observer/virtual_table/ob_all_virtual_activity_metrics.h"
#include "observer/ob_server.h"
#include "share/rc/ob_tenant_base.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace observer
{

ObAllVirtualActivityMetric::ObAllVirtualActivityMetric()
    : ObVirtualTableScannerIterator(),
      current_pos_(0),
      length_(0),
      addr_(),
      ip_buffer_()
{
}

ObAllVirtualActivityMetric::~ObAllVirtualActivityMetric()
{
  reset();
}

void ObAllVirtualActivityMetric::reset()
{
  current_pos_ = 0;
  length_ = 0;
  addr_.reset();
  ip_buffer_[0] = '\0';
  omt::ObMultiTenantOperator::reset();
  ObVirtualTableScannerIterator::reset();
}

bool ObAllVirtualActivityMetric::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)) {
    return true;
  }
  return false;
}

void ObAllVirtualActivityMetric::release_last_tenant()
{
  current_pos_ = 0;
  length_ = 0;
  ip_buffer_[0] = '\0';
}

int ObAllVirtualActivityMetric::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "execute fail", K(ret));
  }
  return ret;
}

int ObAllVirtualActivityMetric::get_next_freezer_stat_(ObTenantFreezerStat& stat)
{
  int ret = OB_SUCCESS;
  storage::ObTenantFreezer *freezer = MTL(storage::ObTenantFreezer *);

  if (current_pos_ < length_) {
    (void)freezer->get_freezer_stat_from_history(current_pos_, stat);
    current_pos_++;
  } else {
    ret = OB_ITER_END;
  }

  return ret;
}

int ObAllVirtualActivityMetric::prepare_start_to_read_()
{
  int ret = OB_SUCCESS;
  storage::ObTenantFreezer *freezer = MTL(storage::ObTenantFreezer *);

  (void)freezer->get_freezer_stat_history_snapshot(length_);
  current_pos_ = 0;
  start_to_read_ = true;

  return ret;
}

int ObAllVirtualActivityMetric::process_curr_tenant(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObTenantFreezerStat stat;

  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K(allocator_), K(ret));
  } else if (!start_to_read_ && OB_FAIL(prepare_start_to_read_())) {
    SERVER_LOG(WARN, "prepare start to read", K(allocator_), K(ret));
  } else if (OB_FAIL(get_next_freezer_stat_(stat))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "get next freezer stat failed", KR(ret));
    }
  } else {
    ObObj *cells = NULL;
    // allocator_ is allocator of PageArena type, no need to free
    if (NULL == (cells = cur_row_.cells_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
        uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
        case SERVER_IP:
          if (!addr_.ip_to_string(ip_buffer_, common::OB_IP_STR_BUFF)) {
            STORAGE_LOG(ERROR, "ip to string failed");
            ret = OB_ERR_UNEXPECTED;
          } else {
            cells[i].set_varchar(ip_buffer_);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case SERVER_PORT:
          cells[i].set_int(addr_.get_port());
          break;
        case TENANT_ID:
          cells[i].set_int(MTL_ID());
          break;
        case ACTIVITY_TIMESTAMP:
          cells[i].set_timestamp(stat.last_captured_timestamp_);
          break;
        case MODIFICATION_SIZE:
          cells[i].set_int(stat.captured_data_size_);
          break;
        case FREEZE_TIMES:
          cells[i].set_int(stat.captured_freeze_times_);
          break;
        case MINI_MERGE_COST:
          cells[i].set_int(stat.captured_merge_time_cost_[storage::ObTenantFreezerStat::ObFreezerMergeType::MINI_MERGE]);
          break;
        case MINI_MERGE_TIMES:
          cells[i].set_int(stat.captured_merge_times_[storage::ObTenantFreezerStat::ObFreezerMergeType::MINI_MERGE]);
          break;
        case MINOR_MERGE_COST:
          cells[i].set_int(stat.captured_merge_time_cost_[storage::ObTenantFreezerStat::ObFreezerMergeType::MINOR_MERGE]);
          break;
        case MINOR_MERGE_TIMES:
          cells[i].set_int(stat.captured_merge_times_[storage::ObTenantFreezerStat::ObFreezerMergeType::MINOR_MERGE]);
          break;
        case MAJOR_MERGE_COST:
          cells[i].set_int(stat.captured_merge_time_cost_[storage::ObTenantFreezerStat::ObFreezerMergeType::MAJOR_MERGE]);
          break;
        case MAJOR_MERGE_TIMES:
          cells[i].set_int(stat.captured_merge_times_[storage::ObTenantFreezerStat::ObFreezerMergeType::MAJOR_MERGE]);
          break;
        default:
          // abnormal column id
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "unexpected column id", K(ret));
          break;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }

  return ret;
}

} // namepace observer
} // namespace observer
