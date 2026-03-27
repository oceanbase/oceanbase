/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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