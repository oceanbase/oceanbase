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

#ifndef OCEANBASE_SHARE_IMPORT_SCHEMA_ITEM_H
#define OCEANBASE_SHARE_IMPORT_SCHEMA_ITEM_H

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/container/ob_array_serialization.h"
#include "common/object/ob_object.h"
#include "share/restore/ob_import_item_format_provider.h"

namespace oceanbase
{
namespace share
{
class ObIImportItem : public ObIImportItemFormatProvider
{
  OB_UNIS_VERSION_PV(); // pure virtual
public:
  enum ItemType
  {
    DATABASE = 0,
    TABLE,
    PARTITION,
    TABLEGROUP,
    TABLESPACE,
    REMAP
  };

public:
  explicit ObIImportItem(ItemType item_type)
      : item_type_(item_type)
  {}
  virtual ~ObIImportItem() {}
  virtual void reset() = 0;
  virtual bool is_valid() const = 0;
  virtual int deep_copy(common::ObIAllocator &allocator, const ObIImportItem &src) = 0;
  virtual bool operator==(const ObIImportItem &other) const = 0;

  ItemType get_item_type() const { return item_type_; }

  DECLARE_PURE_VIRTUAL_TO_STRING;

private:
  ItemType item_type_;
};


struct ObIImportSchemaItem : public ObIImportItem
{
  ObIImportSchemaItem(ItemType item_type) :
      ObIImportItem(item_type),
      mode_(common::OB_NAME_CASE_INVALID)
  {}

  ObIImportSchemaItem(ItemType item_type, common::ObNameCaseMode mode) :
      ObIImportItem(item_type),
      mode_(mode)
  {}

  virtual bool case_mode_equal(const ObIImportItem &other) const = 0;
  virtual bool operator==(const ObIImportItem &other) const override
  {
    return case_mode_equal(other);
  }

public:
  common::ObNameCaseMode mode_; // for compare
};


// Used for these schema which can be named with only one string, such as database, tablespace, tablegroup.
template <int64_t MAX_SIZE, ObIImportItem::ItemType ITEM_TYPE>
struct ObImportSimpleSchemaItem final : public ObIImportSchemaItem
{
  OB_UNIS_VERSION(1);
public:
  ObImportSimpleSchemaItem() :
      ObIImportSchemaItem(ITEM_TYPE),
      name_()
  {}

  ObImportSimpleSchemaItem(common::ObNameCaseMode mode, const char *name, const int64_t len) :
        ObIImportSchemaItem(ITEM_TYPE, mode),
        name_(len, name)
  {}

  virtual void reset() override;
  virtual bool is_valid() const override;
  virtual bool case_mode_equal(const ObIImportItem &other) const override;
  virtual int64_t get_format_serialize_size() const override;
  virtual int format_serialize(
      char *buf,
      const int64_t buf_len,
      int64_t &pos) const override;

  virtual int deep_copy(common::ObIAllocator &allocator, const ObIImportItem &src);

  using ObIImportSchemaItem::format_serialize;
  int assign(const ObImportSimpleSchemaItem &other);

  TO_STRING_KV(K_(mode), K_(name));

public:
  // "name_" is c_style string, and '\0' is not included
  // into the length.
  common::ObString name_;
};


template <typename T>
class ObImportSchemaItemArray : public ObIImportItemFormatProvider, public ObImportItemHexFormatImpl
{
  OB_UNIS_VERSION(1);
public:
  ObImportSchemaItemArray() {}
  virtual int64_t get_format_serialize_size() const override;
  virtual int format_serialize(
      char *buf,
      const int64_t buf_len,
      int64_t &pos) const override;

  virtual void reset();

  int assign(const ObImportSchemaItemArray<T> &other);
  int add_item(const T& item);
  bool is_empty() const { return items_.empty(); }
  const common::ObSArray<T> &get_items() const { return items_; }
  bool is_exist(const T& item) const;
  bool is_exist(const T& item, const T *&out) const;

  template<class Operator>
  int foreach(Operator &op) const
  {
    int ret = common::OB_SUCCESS;
    ARRAY_FOREACH(items_, idx) {
      const T &item = items_.at(idx);
      if (OB_FAIL(op(item))) {
        SHARE_LOG(WARN, "fail to do operator", K(ret), K(item));
      }
    }
    return ret;
  }

  TO_STRING_KV(K_(items));

protected:
  common::ObArenaAllocator allocator_;
  common::ObSArray<T> items_;
};


using ObImportDatabaseItem = ObImportSimpleSchemaItem<OB_MAX_DATABASE_NAME_LENGTH, ObIImportItem::ItemType::DATABASE>;
using ObImportTablegroupItem = ObImportSimpleSchemaItem<OB_MAX_TABLEGROUP_NAME_LENGTH, ObIImportItem::ItemType::TABLEGROUP>;
using ObImportTablespaceItem = ObImportSimpleSchemaItem<OB_MAX_TABLESPACE_NAME_LENGTH, ObIImportItem::ItemType::TABLESPACE>;
using ObImportDatabaseArray = ObImportSchemaItemArray<ObImportDatabaseItem>;

}
}


#ifndef INCLUDE_OB_IMPORT_SCHEMA_ITEM_HPP
#define INCLUDE_OB_IMPORT_SCHEMA_ITEM_HPP
#include "ob_import_schema_item.hpp"
#endif

#endif
