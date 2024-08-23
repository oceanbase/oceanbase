/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLE_MODE_CONTROL_H */