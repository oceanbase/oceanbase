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

#ifndef OCEANBASE_SHARE_IMPORT_REMAP_ARG_H
#define OCEANBASE_SHARE_IMPORT_REMAP_ARG_H

#include "share/restore/ob_remap_schema_item.h"

namespace oceanbase
{
namespace share
{

class ObImportRemapArg final
{
  OB_UNIS_VERSION(1);
public:
  ObImportRemapArg();
  void reset();
  int assign(const ObImportRemapArg &other);
  const ObRemapDatabaseArray &get_remap_database_array() const { return remap_database_array_; }
  const ObRemapTableArray &get_remap_table_array() const { return remap_table_array_; }
  const ObRemapPartitionArray &get_remap_partition_array() const { return remap_partition_array_; }
  const ObRemapTablegroupArray &get_remap_tablegroup_array() const { return remap_tablegroup_array_; }
  const ObRemapTablespaceArray &get_remap_tablespace_array() const { return remap_tablespace_array_; }
  ObRemapDatabaseArray &get_remap_database_array() { return remap_database_array_; }
  ObRemapTableArray &get_remap_table_array() { return remap_table_array_; }
  ObRemapPartitionArray &get_remap_partition_array() { return remap_partition_array_; }
  ObRemapTablegroupArray &get_remap_tablegroup_array() { return remap_tablegroup_array_; }
  ObRemapTablespaceArray &get_remap_tablespace_array() { return remap_tablespace_array_; }
  int add_remap_database(const ObRemapDatabaseItem &item);
  int add_remap_table(const ObRemapTableItem &item);
  int add_remap_parition(const ObRemapPartitionItem &item);
  int add_remap_tablegroup(const ObRemapTablegroupItem &item);
  int add_remap_tablespace(const ObRemapTablespaceItem &item);
  int check_remap_database_dup(const ObRemapDatabaseItem &item, bool &is_dup, ObSqlString &dup_item_str);
  int check_remap_table_dup(const ObRemapTableItem &item, bool &is_dup, ObSqlString &dup_item_str);
  int check_remap_partition_dup(const ObRemapPartitionItem &item, bool &is_dup, ObSqlString &dup_item_str);
  int check_remap_tablegroup_dup(const ObRemapTablegroupItem &item, bool &is_dup, ObSqlString &dup_item_str);
  int check_remap_tablespace_dup(const ObRemapTablespaceItem &item, bool &is_dup, ObSqlString &dup_item_str);
  int get_remap_database(const ObImportDatabaseItem &src, ObImportDatabaseItem &target) const;
  int get_remap_table(const ObImportTableItem &src, ObImportTableItem &target) const;
  int get_remap_partition(const ObImportPartitionItem &src, ObImportTableItem &target) const;
  int get_remap_tablegroup(const ObImportTablegroupItem &src, ObImportTablegroupItem &target) const;
  int get_remap_tablespace(const ObImportTablespaceItem &src, ObImportTablespaceItem &target) const;

  int get_remap_db_list_format_str(common::ObIAllocator &allocator, common::ObString &str) const;
  int get_remap_table_list_format_str(common::ObIAllocator &allocator, common::ObString &str) const;
  int get_remap_partition_list_format_str(common::ObIAllocator &allocator, common::ObString &str) const;
  int get_remap_tablegroup_list_format_str(common::ObIAllocator &allocator, common::ObString &str) const;
  int get_remap_tablespace_list_format_str(common::ObIAllocator &allocator, common::ObString &str) const;
  int get_remap_db_list_hex_format_str(common::ObIAllocator &allocator, common::ObString &str) const;
  int get_remap_table_list_hex_format_str(common::ObIAllocator &allocator, common::ObString &str) const;
  int get_remap_partition_list_hex_format_str(common::ObIAllocator &allocator, common::ObString &str) const;
  int get_remap_tablegroup_list_hex_format_str(common::ObIAllocator &allocator, common::ObString &str) const;
  int get_remap_tablespace_list_hex_format_str(common::ObIAllocator &allocator, common::ObString &str) const;

  int remap_db_list_deserialize_hex_format_str(const common::ObString &str);
  int remap_table_list_deserialize_hex_format_str(const common::ObString &str);
  int remap_partition_list_deserialize_hex_format_str(const common::ObString &str);
  int remap_tablegroup_list_deserialize_hex_format_str(const common::ObString &str);
  int remap_tablespace_list_deserialize_hex_format_str(const common::ObString &str);

  TO_STRING_KV(K_(remap_database_array), K_(remap_table_array), K_(remap_partition_array),
    K_(remap_tablegroup_array), K_(remap_tablespace_array));

private:
  template <typename T>
  int get_remap_item_array_format_str_(
      const T &array,
      common::ObIAllocator &allocator,
      common::ObString &str) const;

  template <typename T>
  int get_remap_item_array_hex_format_str_(
      const T &array,
      common::ObIAllocator &allocator,
      common::ObString &str) const;

  template <typename T>
  int remap_item_array_deserialize_hex_format_str_(
      T& array,
      const common::ObString &str);

private:
  ObRemapDatabaseArray remap_database_array_;
  ObRemapTableArray remap_table_array_;
  ObRemapPartitionArray remap_partition_array_;
  ObRemapTablegroupArray remap_tablegroup_array_;
  ObRemapTablespaceArray remap_tablespace_array_;
  DISALLOW_COPY_AND_ASSIGN(ObImportRemapArg);
};


template <typename T>
int ObImportRemapArg::get_remap_item_array_format_str_(
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
    SHARE_LOG(WARN, "fail to format remap item array", K(ret), K(pos), K(str_buf_len));
  } else {
    str.assign_ptr(str_buf, str_buf_len);
    SHARE_LOG(INFO, "get format remap item array str", K(str));
  }

  return ret;
}

template <typename T>
int ObImportRemapArg::get_remap_item_array_hex_format_str_(
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
    SHARE_LOG(WARN, "fail to hex format remap item array", K(ret), K(pos), K(str_buf_len));
  } else {
    str.assign_ptr(str_buf, str_buf_len);
  }

  return ret;
}

template <typename T>
int ObImportRemapArg::remap_item_array_deserialize_hex_format_str_(
    T& array,
    const common::ObString &str)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(array.hex_format_deserialize(str.ptr(), str.length(), pos))) {
    SHARE_LOG(WARN, "fail to deserialize hex format remap item array", K(ret), K(str));
  }
  return ret;
}

}
}

#endif