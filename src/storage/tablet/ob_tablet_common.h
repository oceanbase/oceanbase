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

#ifndef OCEANBASE_STORAGE_OB_TABLET_COMMON
#define OCEANBASE_STORAGE_OB_TABLET_COMMON

#include <stdint.h>
#include "lib/literals/ob_literals.h"

namespace oceanbase
{
namespace storage
{
/*
  Tablet is created/deleted through MDS which has transactional meaning.
  Consider creating tablet as inserting a row to database, and deleting tablet as
  remove a row from database. So below ObMDSGetTabletMode can be understood as read
  isolation level in a database.

  READ_ALL_COMMITED:
    Read committed row after transaction committed, except empty shell.
    Return tablet which has finished at least one MDS transaction(not including tablet in NORMAL not committed, TRANSFER_IN not committed status).
    Not return CREATING and DELETING who was abandoned from 4.2.
    In addition, you should NOT pass read timeout under this mode.

  READ_WITHOUT_CHECK:
    Read uncommitted row. Return tablet whatever it is in a MDS transaction or not.

  READ_READABLE_COMMITED:
    Read commited row, not include deleted one. The most frequently used mode. Return
    tablet in NORMAL, TRANSFER_IN status. Not return one in unreadable status.
    If latest tablet status is TRANSFER_OUT, we should check transfer scn to decide
    whether it is legal to return the tablet.
    If read operation reaches read timeout, you'll get OB_ERR_SHARED_LOCK_CONFLICT error.
*/
enum class ObMDSGetTabletMode
{
  READ_ALL_COMMITED = 0,
  READ_WITHOUT_CHECK = 1,
  READ_READABLE_COMMITED = 2,
};

class ObTabletCommon final
{
public:
  static const int64_t DEFAULT_ITERATOR_TABLET_ID_CNT = 128;
  static const int64_t BUCKET_LOCK_BUCKET_CNT = 10243L;
  static const int64_t TABLET_ID_SET_BUCKET_CNT = 10243L;
  static const int64_t DEFAULT_GET_TABLET_NO_WAIT = 0; // 0s
  static const int64_t DEFAULT_GET_TABLET_DURATION_US = 1_s;
  static const int64_t DEFAULT_GET_TABLET_DURATION_10_S = 10_s;
  static const int64_t FINAL_TX_ID = 0;
  // The length of tablet_addr contains first-level meta's length and inline-meta's length.
  // We ensures that the first-level meta's length will not exceed MAX_TABLET_FIRST_LEVEL_META_SIZE by implementation,
  // in fact, within 4k in most cases. So just use this length in the situation where only want to read first-level meta,
  // although there is some IO amplification, but avoid the trouble of recording the first-level meta's length.
  static const int64_t MAX_TABLET_FIRST_LEVEL_META_SIZE = 16 * 1024; // 16k
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_COMMON
