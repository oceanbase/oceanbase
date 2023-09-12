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

#define USING_LOG_PREFIX RPC_OBMYSQL

#include "rpc/obmysql/ob_mysql_row.h"
#include "rpc/obmysql/ob_mysql_global.h"
#include "rpc/obmysql/ob_mysql_util.h"

using namespace oceanbase::common;
using namespace oceanbase::obmysql;

int ObMySQLRow::serialize(char *buf, const int64_t len, int64_t &pos) const
{
  int      ret          = OB_SUCCESS;
  int64_t  pos_bk       = pos;
  char    *bitmap       = NULL;
  const int64_t  column_num   = get_cells_cnt();

  if (column_num > 0 && NULL != buf) {
    //for binary protocol
    if (BINARY == type_ && !is_packed_) {
      //https://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html
      //one byte header alwasy 0x00
      const int64_t bitmap_bytes = (column_num + 7 + 2) / 8;
      if (len - pos < 1 + bitmap_bytes) {
        ret = OB_SIZE_OVERFLOW;
      } else {
        MEMSET(buf + pos, 0, 1);
        pos ++;
        //NULL-bitmap-bytes = (num-fields + 7 + offset) / 8
        //offset in binary row response is 2
        bitmap = buf + pos;
        MEMSET(bitmap, 0, bitmap_bytes);
        pos += bitmap_bytes;
      }
    }

    for (int64_t cell_idx = 0; cell_idx < column_num && OB_SUCC(ret); cell_idx++) {
      //    if (is_cell_null(cell_idx)) {
      //      if (OB_FAIL(ObMySQLUtil::null_cell_str(
      //                     buf, len, type_, pos, cell_idx, bitmap))) {
      //        break;
      //      }
      //    } else
      if (OB_FAIL(encode_cell(cell_idx, buf, len, pos, bitmap))) {
        LOG_WARN("failed to encode cell", K(ret), K(cell_idx), K(len), K(pos), K(bitmap));
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(column_num));
  }

  if (OB_FAIL(ret)) {
    if (OB_LIKELY(OB_SIZE_OVERFLOW != ret && OB_BUF_NOT_ENOUGH != ret)) {
      LOG_WARN("serialize ob row fail", K(ret), K(len), K(pos), K(pos_bk));
    }
    pos = pos_bk;
  }
  return ret;
}
