/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef _OB_TABLE_MODE_CONTROL_H
#define _OB_TABLE_MODE_CONTROL_H
#include "share/table/ob_table.h"
namespace oceanbase
{
namespace table
{

enum class ObKvModeType
{
  ALL,
  TABLEAPI,
  HBASE,
  REDIS,
  NONE
};

class ObTableModeCtrl
{
public:
  static int check_mode(ObKvModeType tenant_mode, ObTableEntityType entity_type);
  static bool is_hbase_entity_type(ObTableEntityType entity_type);
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLE_MODE_CONTROL_H */