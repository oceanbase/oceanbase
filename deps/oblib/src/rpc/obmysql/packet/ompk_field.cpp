/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "rpc/obmysql/packet/ompk_field.h"

using namespace oceanbase::obmysql;

OMPKField::OMPKField(ObMySQLField &field)
    : field_(field)
{}

int OMPKField::serialize(char *buffer, int64_t len, int64_t &pos) const
{
  return field_.serialize(buffer, len, pos);
}
