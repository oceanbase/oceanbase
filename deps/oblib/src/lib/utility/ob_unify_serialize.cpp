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


#ifdef ENABLE_SERIALIZATION_CHECK
#include "ob_unify_serialize.h"
namespace oceanbase
{
namespace lib
{
RLOCAL(SerializeDiagnoseRecord, ser_diag_record);
void begin_record_serialization()
{
  ser_diag_record.count = 0;
  ser_diag_record.check_index = 0;
  ser_diag_record.flag = CHECK_STATUS_RECORDING;
}

void finish_record_serialization()
{
  ser_diag_record.flag = CHECK_STATUS_WATING;
}

void begin_check_serialization()
{
  ser_diag_record.check_index = 0;
  if (ser_diag_record.count > 0) {
    ser_diag_record.flag = CHECK_STATUS_COMPARING;
  }
}

void finish_check_serialization()
{
  ser_diag_record.count = -1;
  ser_diag_record.check_index = -1;
  ser_diag_record.flag = CHECK_STATUS_WATING;
}

}  // namespace lib
}  // namespace oceanbase
#endif