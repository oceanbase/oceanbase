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

#include "observer/virtual_table/ob_all_virtual_server_object_pool.h"
#include <observer/ob_server_utils.h>
#include <lib/objectpool/ob_server_object_pool.h>
#include <storage/tx/ob_trans_part_ctx.h>

using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::transaction;

ObAllVirtualServerObjectPool::ObAllVirtualServerObjectPool() : addr_(NULL)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObServerObjectPoolRegistry::alloc_iterator(iter_))) {
    COMMON_LOG(ERROR, "alloc_iterator failed", K(ret));
  }
}

void ObAllVirtualServerObjectPool::reset()
{
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualServerObjectPool::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  const char* object_type = NULL;
  ObPoolArenaHead *arena_head = NULL;
  ObString ipstr;
  ObObj *cells = NULL;
  if (iter_.is_end()) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
  } else if (OB_FAIL(iter_.get_pool_arena(object_type, arena_head))) {
    SERVER_LOG(WARN, "get_pool_arena failed", K(ret), K(iter_));
  } else if (NULL == object_type || NULL == arena_head) {
    SERVER_LOG(WARN, "object_type or arena_head is NULL", K(ret), K(iter_), K(object_type), K(arena_head));
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID: {
          // svr_ip
          if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
            SERVER_LOG(ERROR, "get server ip failed", K(ret));
          } else {
            cells[i].set_varchar(ipstr);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
        } break;
        case OB_APP_MIN_COLUMN_ID + 1:
          // svr_port
          cells[i].set_int(addr_->get_port());
          break;
        case OB_APP_MIN_COLUMN_ID + 2:
          // object_type
          cells[i].set_varchar(ObString(object_type));
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        case OB_APP_MIN_COLUMN_ID + 3:
          // arena_id
          cells[i].set_int(iter_.get_arena_idx());
          break;
        case OB_APP_MIN_COLUMN_ID + 4:
          // lock
          cells[i].set_int(*reinterpret_cast<uint32_t*>(&arena_head->lock));
          break;
        case OB_APP_MIN_COLUMN_ID + 5:
          // borrow_count
          cells[i].set_int(arena_head->borrow_cnt);
          break;
        case OB_APP_MIN_COLUMN_ID + 6:
          // return_count
          cells[i].set_int(arena_head->return_cnt);
          break;
        case OB_APP_MIN_COLUMN_ID + 7:
          // miss_count
          cells[i].set_int(arena_head->miss_cnt);
          break;
        case OB_APP_MIN_COLUMN_ID + 8:
          // miss_return_count
          cells[i].set_int(arena_head->miss_return_cnt);
          break;
        case OB_APP_MIN_COLUMN_ID + 9:
          // free_num
          cells[i].set_int(arena_head->free_num);
          break;
        case OB_APP_MIN_COLUMN_ID + 10:
          // last_borrow_ts
          cells[i].set_int(arena_head->last_borrow_ts);
          break;
        case OB_APP_MIN_COLUMN_ID + 11:
          // last_return_ts
          cells[i].set_int(arena_head->last_return_ts);
          break;
        case OB_APP_MIN_COLUMN_ID + 12:
          // last_miss_ts
          cells[i].set_int(arena_head->last_miss_ts);
          break;
        case OB_APP_MIN_COLUMN_ID + 13:
          // last_miss_return_ts
          cells[i].set_int(arena_head->last_miss_return_ts);
          break;
        case OB_APP_MIN_COLUMN_ID + 14:
          // next
          cells[i].set_int(reinterpret_cast<int64_t>(arena_head->next));
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid coloum_id", K(ret), K(col_id));
          break;
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(iter_.next())) {
      SERVER_LOG(WARN, "iter next failed", K(ret), K(iter_));
    }
  }
  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }

  return ret;
}

