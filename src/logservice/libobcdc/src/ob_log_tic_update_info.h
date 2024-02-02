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
 *
 * TableIdCache update info data struct in OBCDC
 */

#ifndef OCEANBASE_LIBOBCDC_TIC_UPDATE_INFO_H__
#define OCEANBASE_LIBOBCDC_TIC_UPDATE_INFO_H__

#include <cstdint>
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace libobcdc
{

// TIC is TableIdCache, contains the table_id in tb_white_list and not in tb_black_list
// TICUpdateInfo is the update info which TIC should update
struct TICUpdateInfo
{
  enum TICUpdateReason
  {
    INVALID_REASON = -1,
    DROP_TABLE = 0,
    CREATE_TABLE = 1,
    RENAME_TABLE_ADD = 2,
    RENAME_TABLE_REMOVE = 3,
    DROP_DATABASE = 4,
  };
  static const char *print_tic_update_reason(const TICUpdateReason reason);
  TICUpdateReason reason_;
  uint64_t database_id_;
  uint64_t table_id_;

  TICUpdateInfo()
  {
    reset();
  }
  TICUpdateInfo(const TICUpdateReason reason,
      const uint64_t database_id,
      const uint64_t table_id) :
      reason_(reason), database_id_(database_id), table_id_(table_id) {}
  TICUpdateInfo(const TICUpdateReason reason, const uint64_t database_id) :
      TICUpdateInfo(reason, database_id, OB_INVALID_ID) {}
  ~TICUpdateInfo()
  {
    reset();
  }
  void reset();
  bool is_valid() const;
  TO_STRING_KV("tic_update_reason", print_tic_update_reason(reason_), K_(database_id), K_(table_id));
};

}
}

#endif