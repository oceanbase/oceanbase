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

#ifndef OCEANBASE_SHARE_REMAP_SCHEMA_ITEM_H
#define OCEANBASE_SHARE_REMAP_SCHEMA_ITEM_H

#include "share/restore/ob_import_table_item.h"
#include "share/restore/ob_import_partition_item.h"

namespace oceanbase
{
namespace share
{

template <typename S, typename T>
struct ObRemapSchemaItem : public ObIImportItem
{
  OB_UNIS_VERSION(1);
public:
  ObRemapSchemaItem() : ObIImportItem(ItemType::REMAP)
  {}

  virtual void reset() override;
  virtual bool is_valid() const override;
  virtual int64_t get_format_serialize_size() const override;
  virtual int format_serialize(
      char *buf,
      const int64_t buf_len,
      int64_t &pos) const override;
  virtual int deep_copy(common::ObIAllocator &allocator, const ObIImportItem &src) override;
  virtual bool operator==(const ObIImportItem &other) const override;

  using ObIImportItem::format_serialize;
  int assign(const ObRemapSchemaItem &other);

  TO_STRING_KV(K_(src), K_(target));

public:
  S src_;
  T target_;
};



template <typename S, typename T>
class ObRemapSchemaItemArray : public ObIImportItemFormatProvider, public ObImportItemHexFormatImpl
{
  OB_UNIS_VERSION(1);
public:
  ObRemapSchemaItemArray() {}
  virtual int64_t get_format_serialize_size() const override;
  virtual int format_serialize(
      char *buf,
      const int64_t buf_len,
      int64_t &pos) const override;

  virtual void reset();

  int assign(const ObRemapSchemaItemArray<S, T> &other);
  int add_item(const ObRemapSchemaItem<S, T> &item);
  bool is_empty() const { return remap_items_.empty(); }
  bool is_src_exist(const S &src) const;
  bool is_src_exist(const S &src, const S *&out) const;
  bool is_target_exist(const T &target) const;
  bool is_target_exist(const T &target, const T *&out) const;
  const common::ObSArray<ObRemapSchemaItem<S, T>> &get_remap_items() const { return remap_items_; }
  bool is_remap_target_exist(const S &src, const T *&out) const;

  template<class Operator>
  int foreach(Operator &op) const
  {
    int ret = common::OB_SUCCESS;
    ARRAY_FOREACH(remap_items_, idx) {
      const ObRemapSchemaItem<S, T> &remap_item = remap_items_.at(idx);
      if (OB_FAIL(op(remap_item))) {
        SHARE_LOG(WARN, "fail to do operator", K(ret), K(remap_item));
      }
    }
    return ret;
  }

  TO_STRING_KV(K_(remap_items));

protected:
  common::ObArenaAllocator allocator_;

private:
  common::ObSArray<ObRemapSchemaItem<S, T>> remap_items_;
};

using ObRemapDatabaseItem = ObRemapSchemaItem<ObImportDatabaseItem, ObImportDatabaseItem>;
using ObRemapTableItem = ObRemapSchemaItem<ObImportTableItem, ObImportTableItem>;
using ObRemapPartitionItem = ObRemapSchemaItem<ObImportPartitionItem, ObImportTableItem>;
using ObRemapTablespaceItem = ObRemapSchemaItem<ObImportTablespaceItem, ObImportTablespaceItem>;
using ObRemapTablegroupItem = ObRemapSchemaItem<ObImportTablegroupItem, ObImportTablegroupItem>;


using ObRemapDatabaseArray = ObRemapSchemaItemArray<ObImportDatabaseItem, ObImportDatabaseItem>;
using ObRemapTableArray = ObRemapSchemaItemArray<ObImportTableItem, ObImportTableItem>;
using ObRemapPartitionArray = ObRemapSchemaItemArray<ObImportPartitionItem, ObImportTableItem>;
using ObRemapTablespaceArray = ObRemapSchemaItemArray<ObImportTablespaceItem, ObImportTablespaceItem>;
using ObRemapTablegroupArray = ObRemapSchemaItemArray<ObImportTablegroupItem, ObImportTablegroupItem>;


}
}

#ifndef INCLUDE_OB_REMAP_SCHEMA_ITEM_HPP
#define INCLUDE_OB_REMAP_SCHEMA_ITEM_HPP
#include "ob_remap_schema_item.hpp"
#endif

#endif