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

#include "observer/ob_server.h"
#include "storage/ob_partition_group.h"
#include "ob_all_virtual_election_mem_stat.h"

namespace oceanbase {
using namespace common;
using namespace election;
namespace observer {
void ObGVElectionMemStat::reset()
{
  ip_buffer_[0] = '\0';
  type_name_[0] = '\0';
  ObVirtualTableScannerIterator::reset();
}

void ObGVElectionMemStat::destroy()
{
  election_mgr_ = NULL;
  memset(ip_buffer_, 0, common::OB_IP_STR_BUFF);
  memset(type_name_, 0, ObElectionMemStat::OB_ELECTION_TYPE_NAME_LENGTH);
  ObVirtualTableScannerIterator::reset();
}

int ObGVElectionMemStat::prepare_to_read_()
{
  int ret = OB_SUCCESS;
  ObObj* cells = NULL;
  if (NULL == allocator_ || NULL == election_mgr_) {
    SERVER_LOG(WARN, "invalid argument", KP_(allocator), KP_(election_mgr));
    ret = OB_INVALID_ARGUMENT;
  } else if (NULL == (cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_SUCCESS != (ret = election_mgr_->iterate_election_mem_stat(election_mem_stat_iter_))) {
    SERVER_LOG(WARN, "iterate election mem stat error", K(ret));
  } else if (OB_SUCCESS != (ret = election_mem_stat_iter_.set_ready())) {
    SERVER_LOG(WARN, "iterate election mem stat begin error", K(ret));
  } else {
    start_to_read_ = true;
  }

  return ret;
}

int ObGVElectionMemStat::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObElectionMemStat election_mem_stat;

  if (!start_to_read_ && OB_SUCCESS != (ret = prepare_to_read_())) {
    SERVER_LOG(DEBUG, "prepare_to_read_ error.", K(ret), K_(start_to_read));
  } else if (OB_SUCCESS != (ret = election_mem_stat_iter_.get_next(election_mem_stat))) {
    if (OB_ITER_END == ret) {
      SERVER_LOG(DEBUG, "iterate election mem stat iter end", K(ret));
    } else {
      SERVER_LOG(WARN, "get next election mem stat error.", K(ret));
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID:
          // svr_ip
          (void)election_mem_stat.get_addr().ip_to_string(ip_buffer_, common::OB_IP_STR_BUFF);
          cur_row_.cells_[i].set_varchar(ip_buffer_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // svr_port
          cur_row_.cells_[i].set_int(election_mem_stat.get_addr().get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // type_name
          if (NULL != election_mem_stat.get_type_name()) {
            int64_t len = strlen(election_mem_stat.get_type_name());
            len = len > ObElectionMemStat::OB_ELECTION_TYPE_NAME_LENGTH - 1
                      ? ObElectionMemStat::OB_ELECTION_TYPE_NAME_LENGTH - 1
                      : len;
            strncpy(type_name_, election_mem_stat.get_type_name(), len);
            type_name_[len] = '\0';
            cur_row_.cells_[i].set_varchar(type_name_);
            cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          cur_row_.cells_[i].set_int(election_mem_stat.get_alloc_count());
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          cur_row_.cells_[i].set_int(election_mem_stat.get_release_count());
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(ERROR, "invalid coloum_id", K(ret), K(col_id));
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
