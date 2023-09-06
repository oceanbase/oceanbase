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

#ifndef OCEANBASE_SHARE_IMPORT_TABLE_ITEM_H
#define OCEANBASE_SHARE_IMPORT_TABLE_ITEM_H

#include "share/restore/ob_import_schema_item.h"

namespace oceanbase
{
namespace share
{

struct ObImportTableItem final : public ObIImportSchemaItem
{
  OB_UNIS_VERSION(1);
public:
  ObImportTableItem() :
      ObIImportSchemaItem(ItemType::TABLE),
      database_name_(),
      table_name_()
  {}

  ObImportTableItem(common::ObNameCaseMode mode, const char *db_name, const int64_t db_len,
      const char *table_name, const int64_t table_len) :
      ObIImportSchemaItem(ItemType::TABLE, mode),
      database_name_(db_len, db_name),
      table_name_(table_len, table_name)
  {}

  virtual void reset() override;
  virtual bool is_valid() const override;
  virtual bool case_mode_equal(const ObIImportItem &other) const override;
  // ignore case
  virtual int64_t get_format_serialize_size() const override;
  virtual int format_serialize(
      char *buf,
      const int64_t buf_len,
      int64_t &pos) const override;

  virtual int deep_copy(common::ObIAllocator &allocator, const ObIImportItem &src) override;

  using ObIImportSchemaItem::format_serialize;
  int assign(const ObImportTableItem &other);
  bool is_database_equal(const ObImportDatabaseItem &other) const;
  const ObImportDatabaseItem get_database() const;

  TO_STRING_KV(K_(mode), K_(database_name), K_(table_name));

public:
  // The following 2 names both are c_style string, and '\0' is not included
  // into the length.
  common::ObString database_name_;
  common::ObString table_name_;
};


class ObImportTableArray final : public ObImportSchemaItemArray<ObImportTableItem>
{
public:
  bool is_exist_table_in_database(const ObImportDatabaseItem &db_item, const ObImportTableItem *&out) const;
};


}
}
#endif