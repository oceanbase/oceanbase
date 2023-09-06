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

#ifndef OCEANBASE_SHARE_IMPORT_TABLE_ARG_H
#define OCEANBASE_SHARE_IMPORT_TABLE_ARG_H

#include "lib/ob_define.h"
#include "share/restore/ob_import_table_item.h"
#include "share/restore/ob_import_partition_item.h"

namespace oceanbase
{
namespace share
{

class ObImportTableArg final
{
  OB_UNIS_VERSION(1);
public:
  ObImportTableArg();
  void reset();
  int assign(const ObImportTableArg &other);
  int set_import_all();
  bool is_import_all() const { return is_import_all_; }
  const ObImportDatabaseArray &get_import_database_array() const { return database_array_; }
  const ObImportTableArray &get_import_table_array() const { return table_array_; }
  const ObImportPartitionArray &get_import_partition_array() const { return partition_array_; }
  ObImportDatabaseArray &get_import_database_array() { return database_array_; }
  ObImportTableArray &get_import_table_array() { return table_array_; }
  ObImportPartitionArray &get_import_partition_array() { return partition_array_; }
  int add_database(const ObImportDatabaseItem &item);
  int add_table(const ObImportTableItem &item);
  int add_partition(const ObImportPartitionItem &item);
  int check_database_dup(const ObImportDatabaseItem &item, bool &is_dup, ObSqlString &dup_item) const;
  int check_table_dup(const ObImportTableItem &item, bool &is_dup, ObSqlString &dup_item) const;
  int check_partion_dup(const ObImportPartitionItem &item, bool &is_dup, ObSqlString &dup_item) const;
  int get_db_list_format_str(common::ObIAllocator &allocator, common::ObString &str) const;
  int get_table_list_format_str(common::ObIAllocator &allocator, common::ObString &str) const;
  int get_partition_list_format_str(common::ObIAllocator &allocator, common::ObString &str) const;
  int get_db_list_hex_format_str(common::ObIAllocator &allocator, common::ObString &str) const;
  int get_table_list_hex_format_str(common::ObIAllocator &allocator, common::ObString &str) const;
  int get_partition_list_hex_format_str(common::ObIAllocator &allocator, common::ObString &str) const;

  int db_list_deserialize_hex_format_str(const common::ObString &str);
  int table_list_deserialize_hex_format_str(const common::ObString &str);
  int partition_list_deserialize_hex_format_str(const common::ObString &str);

  TO_STRING_KV(K_(is_import_all), K_(database_array), K_(table_array), K_(partition_array));

private:
  template <typename T>
  int get_item_array_format_str_(
      const T &array,
      common::ObIAllocator &allocator,
      common::ObString &str) const;

  template <typename T>
  int get_item_array_hex_format_str_(
      const T &array,
      common::ObIAllocator &allocator,
      common::ObString &str) const;

  template <typename T>
  int item_array_deserialize_hex_format_str_(
    T& array,
    const common::ObString &str);

private:
  bool is_import_all_;
  ObImportDatabaseArray database_array_;
  ObImportTableArray table_array_;
  ObImportPartitionArray partition_array_;
  DISALLOW_COPY_AND_ASSIGN(ObImportTableArg);
};



template <typename T>
int ObImportTableArg::get_item_array_format_str_(
    const T &array,
    common::ObIAllocator &allocator,
    common::ObString &str) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t str_buf_len = array.get_format_serialize_size();
  char *str_buf = NULL;

  if (str_buf_len >= OB_MAX_LONGTEXT_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
    SHARE_LOG(WARN, "format str is too long", K(ret), K(str_buf_len));
  } else if (OB_ISNULL(str_buf = static_cast<char *>(allocator.alloc(str_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SHARE_LOG(WARN, "fail to alloc buf", K(ret), K(str_buf_len));
  } else if (OB_FAIL(array.format_serialize(str_buf, str_buf_len, pos))) {
    SHARE_LOG(WARN, "fail to format import item array", K(ret), K(pos), K(str_buf_len));
  } else {
    str.assign_ptr(str_buf, str_buf_len);
    SHARE_LOG(INFO, "get format import item array str", K(str));
  }

  return ret;
}

template <typename T>
int ObImportTableArg::get_item_array_hex_format_str_(
    const T &array,
    common::ObIAllocator &allocator,
    common::ObString &str) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t str_buf_len = array.get_hex_format_serialize_size();
  char *str_buf = NULL;

  if (str_buf_len >= OB_MAX_LONGTEXT_LENGTH) {
    ret = OB_SIZE_OVERFLOW;
    SHARE_LOG(WARN, "hex format str is too long", K(ret), K(str_buf_len));
  } else if (OB_ISNULL(str_buf = static_cast<char *>(allocator.alloc(str_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SHARE_LOG(WARN, "fail to alloc buf", K(ret), K(str_buf_len));
  } else if (OB_FAIL(array.hex_format_serialize(str_buf, str_buf_len, pos))) {
    SHARE_LOG(WARN, "fail to hex format import item array", K(ret), K(pos), K(str_buf_len));
  } else {
    str.assign_ptr(str_buf, str_buf_len);
  }

  return ret;
}

template <typename T>
int ObImportTableArg::item_array_deserialize_hex_format_str_(
    T& array,
    const common::ObString &str)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(array.hex_format_deserialize(str.ptr(), str.length(), pos))) {
    SHARE_LOG(WARN, "fail to deserialize hex format import item array", K(ret), K(str));
  }
  return ret;
}

}
}

#endif