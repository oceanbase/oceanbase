/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_tx_seq.h"

namespace oceanbase
{
namespace transaction
{

DEF_TO_STRING(ObTxSEQ)
{
  int64_t pos = 0;
  if (raw_val_ == INT64_MAX) {
    BUF_PRINTF("MAX");
  } else if (_sign_ == 0 && n_format_) {
    J_OBJ_START();
    J_KV(K_(branch), "seq", seq_);
    J_OBJ_END();
  } else {
    BUF_PRINTF("%lu", raw_val_);
  }
  return pos;
}

}
}