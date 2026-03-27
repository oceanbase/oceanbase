/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once
#include "observer/table_load/backup/ob_table_load_backup_block_sstable_struct.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

class ObIColumnIndexItem
{
public:
  ObIColumnIndexItem() {};
  virtual ~ObIColumnIndexItem() = default;
  virtual common::ObObjMeta get_request_column_type() const = 0;
  virtual int16_t get_store_index() const = 0;
  virtual bool get_is_column_type_matched() const = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

class ObIColumnMap
{
public:
  ObIColumnMap() {};
  virtual ~ObIColumnMap() = default;
  virtual void reset() = 0;
  virtual bool is_valid() const = 0;
  virtual int64_t get_request_count() const = 0;
  virtual int64_t get_store_count() const = 0;
  virtual int64_t get_rowkey_store_count() const = 0;
  virtual int64_t get_seq_read_column_count() const = 0;
  virtual const ObIColumnIndexItem *get_column_index(const int64_t &idx) const = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
};

} // table_load_backup
} // namespace observer
} // namespace oceanbase
