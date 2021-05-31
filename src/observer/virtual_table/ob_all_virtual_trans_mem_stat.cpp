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

#include "observer/virtual_table/ob_all_virtual_trans_mem_stat.h"
#include "observer/ob_server.h"
#include "storage/ob_partition_group.h"

using namespace oceanbase::common;
using namespace oceanbase::transaction;

namespace oceanbase {
namespace observer {
void ObGVTransMemoryStat::reset()
{
  ip_buffer_[0] = '\0';
  mod_type_[0] = '\0';

  ObVirtualTableScannerIterator::reset();
}

void ObGVTransMemoryStat::destroy()
{
  trans_service_ = NULL;
  memset(ip_buffer_, 0, common::OB_IP_STR_BUFF);
  memset(mod_type_, 0, ObTransMemoryStat::OB_TRANS_MEMORY_MOD_TYPE_SIZE);

  ObVirtualTableScannerIterator::reset();
}

int ObGVTransMemoryStat::prepare_start_to_read_()
{
  int ret = OB_SUCCESS;
  ObObj* cells = NULL;

  if (NULL == allocator_ || NULL == trans_service_) {
    SERVER_LOG(WARN,
        "invalid argument, allocator_ or trans_service_ is null",
        "allocator",
        OB_P(allocator_),
        "trans_service",
        OB_P(trans_service_));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_SUCCESS != (ret = trans_service_->iterate_trans_memory_stat(mem_iter_))) {
    SERVER_LOG(WARN, "iterate transaction memory iterator error", K(ret));
  } else if (!mem_iter_.is_ready()) {
    SERVER_LOG(WARN, "ObTransMemStatIterator is not ready");
  } else {
    start_to_read_ = true;
  }

  return ret;
}

int ObGVTransMemoryStat::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObTransMemoryStat mem_stat;

  if (!start_to_read_ && OB_SUCCESS != (ret = prepare_start_to_read_())) {
    SERVER_LOG(WARN, "prepare start to read error", K(ret), K(start_to_read_));
  } else if (OB_SUCCESS != (ret = mem_iter_.get_next(mem_stat))) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "ObTransMemStatIterator get next error", K(ret));
    } else {
      SERVER_LOG(DEBUG, "ObTransMemStatIterator iter end success");
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID:
          // svr_ip
          (void)mem_stat.get_addr().ip_to_string(ip_buffer_, common::OB_IP_STR_BUFF);
          cur_row_.cells_[i].set_varchar(ip_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // svr_port
          cur_row_.cells_[i].set_int(mem_stat.get_addr().get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // mod_type : OB_TRANS_RPC, OB_SERVER_LOG, OB_TRANS_CTX, OB_TRANS_PARTITION_MGR,
          // OB_TRANS_VERSION_MGR
          if (NULL != mem_stat.get_mod_type()) {
            int64_t len = strlen(mem_stat.get_mod_type());
            len = ObTransMemoryStat::OB_TRANS_MEMORY_MOD_TYPE_SIZE - 1 > len
                      ? len
                      : ObTransMemoryStat::OB_TRANS_MEMORY_MOD_TYPE_SIZE - 1;
            strncpy(mod_type_, mem_stat.get_mod_type(), len);
            mod_type_[len] = '\0';
            cur_row_.cells_[i].set_varchar(mod_type_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          // alloc_count
          cur_row_.cells_[i].set_int(mem_stat.get_alloc_count());
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          // release_count
          cur_row_.cells_[i].set_int(mem_stat.get_release_count());
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column_id", K(ret), K(col_id));
          break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }

  return ret;
}

}  // namespace observer
}  // namespace oceanbase
