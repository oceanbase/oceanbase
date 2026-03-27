/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "rpc/obmysql/packet/ompk_row.h"

using namespace oceanbase::obmysql;

OMPKRow::OMPKRow(const ObMySQLRow &row)
    : row_(row)
{

}

int OMPKRow::serialize(char *buffer, int64_t len, int64_t &pos) const
{
  return row_.serialize(buffer, len, pos);
}
