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

#pragma once
#include "observer/table_load/backup/ob_table_load_backup_block_sstable_struct.h"
#include "observer/table_load/backup/ob_table_load_backup_icolumn_map.h"
#include "observer/table_load/backup/ob_table_load_backup_table.h"
#include "storage/ob_i_store.h"
#include "common/object/ob_obj_type.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

class ObIRowReader
{
public:
  const uint8_t STORE_TEXT_STORE_VERSION = 1;

  enum ObStoreAttr
  {
    STORE_WITHOUT_COLLATION = 0,
    STORE_WITH_COLLATION = 1
  };

  struct ObStoreMeta
  {
    ObStoreMeta(): type_(0), attr_(0) {}
    uint8_t type_: 5;
    uint8_t attr_: 3;
  };

  enum ObDataStoreType
  {
    ObNullStoreType = 0,
    ObIntStoreType = 1,
    ObNumberStoreType = 2,
    ObCharStoreType = 3,
    ObHexStoreType = 4,
    ObFloatStoreType = 5,
    ObDoubleStoreType = 6,
    ObTimestampStoreType = 7,
    ObBlobStoreType = 8,
    ObTextStoreType = 9,
    ObEnumStoreType = 10,
    ObSetStoreType = 11,
    ObBitStoreType = 12,
    ObTimestampTZStoreType = 13,
    ObRawStoreType = 14,
    ObIntervalYMStoreType = 15,
    ObIntervalDSStoreType = 16,
    ObRowIDStoreType = 17,
    ObJsonStoreType = 18,
    ObGeometryStoreType = 19,
    ObExtendStoreType = 31
  };

  ObIRowReader() {}
  virtual ~ObIRowReader() = default;
  // read row from flat storage(RowHeader | cells array | column index array)
  // @param (row_buf + pos) point to RowHeader
  // @param row_len is buffer capacity
  // @param column_map use when schema version changed use column map to read row
  // @param [out]row parsed row object.
  // @param out_type indicates the type of ouput row
  virtual int read_row(const char *row_buf,
                       const int64_t row_end_pos,
                       int64_t pos,
                       const ObTableLoadBackupVersion &backup_version,
                       const ObIColumnMap *column_map,
                       ObIAllocator &allocator,
                       common::ObNewRow &row) = 0;
};

} // table_load_backup
} // namespace observer
} // namespace oceanbase
