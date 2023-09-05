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
#define USING_LOG_PREFIX SHARE
#include "share/restore/ob_import_partition_item.h"

namespace oceanbase
{
namespace share
{

// ObImportPartitionItem
OB_SERIALIZE_MEMBER(ObImportPartitionItem, mode_, database_name_, table_name_, partition_name_);

void ObImportPartitionItem::reset()
{
  mode_ = common::OB_NAME_CASE_INVALID;
  database_name_.reset();
  table_name_.reset();
  partition_name_.reset();
}

bool ObImportPartitionItem::is_valid() const
{
  return common::OB_NAME_CASE_INVALID != mode_
         && !database_name_.empty()
         && !table_name_.empty()
         && !partition_name_.empty();
}

bool ObImportPartitionItem::case_mode_equal(const ObIImportItem &other) const
{
  bool is_equal = false;
  if (get_item_type() == other.get_item_type()) {
    const ObImportPartitionItem &the_other = static_cast<const ObImportPartitionItem &>(other);
    is_equal = ObCharset::case_mode_equal(mode_, database_name_, the_other.database_name_)
               && ObCharset::case_mode_equal(mode_, table_name_, the_other.table_name_)
               && ObCharset::case_mode_equal(mode_, partition_name_, the_other.partition_name_);
  }
  return is_equal;
}

int64_t ObImportPartitionItem::get_format_serialize_size() const
{
  // For example, database name is SH, table name is SALES, and partition name is SALES_1998.
  // The format string is `SH`.`SALES`:`SALES_1998`.
  // Pre allocate twice the size to handle escape character.
  int64_t size = 0;
  size += database_name_.length() * 2;
  size += table_name_.length() * 2;
  size += partition_name_.length() * 2;
  size += 8;
  return size;
}

int ObImportPartitionItem::format_serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  // For example, database name is SH, table name is SALES, and partition name is SALES_1998.
  // The format string is `SH`.`SALES`:`SALES_1998`.
  int ret = OB_SUCCESS;
  if ((NULL == buf) || (buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", K(ret), KP(buf), K(buf_len));
  } else if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    const bool skip_escape = false;
    const bool do_oracle_mode_escape = false;
    int64_t string_size = 0;
    common::ObHexEscapeSqlStr hex_escape_db_name(database_name_, skip_escape, do_oracle_mode_escape);
    common::ObHexEscapeSqlStr hex_escape_table_name(table_name_, skip_escape, do_oracle_mode_escape);
    common::ObHexEscapeSqlStr hex_escape_part_name(partition_name_, skip_escape, do_oracle_mode_escape);
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", "`"))) {
      LOG_WARN("fail to format str", K(ret), K(pos), K(buf_len));
    } else if (OB_FALSE_IT(string_size = hex_escape_db_name.to_string(buf + pos, buf_len - pos))) {
    } else if (OB_FALSE_IT(pos += string_size)) {
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", "`.`"))) {
      LOG_WARN("fail to format str", K(ret), K(pos), K(buf_len));
    } else if (OB_FALSE_IT(string_size = hex_escape_table_name.to_string(buf + pos, buf_len - pos))) {
    } else if (OB_FALSE_IT(pos += string_size)) {
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", "`:`"))) {
      LOG_WARN("fail to format str", K(ret), K(pos), K(buf_len));
    } else if (OB_FALSE_IT(string_size = hex_escape_part_name.to_string(buf + pos, buf_len - pos))) {
    } else if (OB_FALSE_IT(pos += string_size)) {
    } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s", "`"))) {
      LOG_WARN("fail to format str", K(ret), K(pos), K(buf_len));
    }
  }

  return ret;
}

int ObImportPartitionItem::deep_copy(common::ObIAllocator &allocator, const ObIImportItem &src)
{
  int ret = OB_SUCCESS;
  if (!src.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src));
  } else if (get_item_type() != src.get_item_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("item type not match", K(ret), "src", src.get_item_type(), "dest", get_item_type());
  } else {
    const ObImportPartitionItem &other = static_cast<const ObImportPartitionItem &>(src);
    if (OB_FAIL(ob_write_string(allocator, other.database_name_, database_name_))) {
      LOG_WARN("failed to copy item", K(ret), K(other));
    } else if (OB_FAIL(ob_write_string(allocator, other.table_name_, table_name_))) {
      LOG_WARN("failed to copy item", K(ret), K(other));
    } else if (OB_FAIL(ob_write_string(allocator, other.partition_name_, partition_name_))) {
      LOG_WARN("failed to copy item", K(ret), K(other));
    } else {
      mode_ = other.mode_;
    }
  }

  return ret;
}

int ObImportPartitionItem::assign(const ObImportPartitionItem &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(other));
  } else {
    mode_ = other.mode_;
    database_name_ = other.database_name_;
    table_name_ = other.table_name_;
    partition_name_ = other.partition_name_;
  }

  return ret;
}

}
}