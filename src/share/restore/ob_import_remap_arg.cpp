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
#include "share/restore/ob_import_remap_arg.h"
#include "share/schema/ob_multi_version_schema_service.h"
//#include "share/schema/ob_column_schema.h"

namespace oceanbase
{
namespace share
{

// ObImportRemapArg
OB_SERIALIZE_MEMBER(
    ObImportRemapArg,
    remap_database_array_,
    remap_table_array_,
    remap_partition_array_,
    remap_tablegroup_array_,
    remap_tablespace_array_);

ObImportRemapArg::ObImportRemapArg()
  : remap_database_array_(),
    remap_table_array_(),
    remap_partition_array_(),
    remap_tablegroup_array_(),
    remap_tablespace_array_()
{}


void ObImportRemapArg::reset()
{
  remap_database_array_.reset();
  remap_table_array_.reset();
  remap_partition_array_.reset();
  remap_tablegroup_array_.reset();
  remap_tablespace_array_.reset();
}

int ObImportRemapArg::assign(const ObImportRemapArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(remap_database_array_.assign(other.get_remap_database_array()))) {
    LOG_WARN("failed to assign database array", K(ret), "database array", other.get_remap_database_array());
  } else if (OB_FAIL(remap_table_array_.assign(other.get_remap_table_array()))) {
    LOG_WARN("failed to assign table array", K(ret), "table array", other.get_remap_table_array());
  } else if (OB_FAIL(remap_partition_array_.assign(other.get_remap_partition_array()))) {
    LOG_WARN("failed to assign partition array", K(ret), "partition array", other.get_remap_partition_array());
  } else if (OB_FAIL(remap_tablegroup_array_.assign(other.get_remap_tablegroup_array()))) {
    LOG_WARN("failed to assign partition array", K(ret), "tablegroup array", other.get_remap_tablegroup_array());
  } else if (OB_FAIL(remap_tablespace_array_.assign(other.get_remap_tablespace_array()))) {
    LOG_WARN("failed to assign partition array", K(ret), "tablespace array", other.get_remap_tablespace_array());
  }
  return ret;
}

int ObImportRemapArg::add_remap_database(const ObRemapDatabaseItem &item)
{
  int ret = OB_SUCCESS;
  const ObImportDatabaseItem *dup_db_item = NULL;
  common::ObArenaAllocator allocator;
  ObString source_err_str;
  ObString target_err_str;

  if (remap_database_array_.is_src_exist(item.src_, dup_db_item)
      || remap_database_array_.is_target_exist(item.target_, dup_db_item)) {
    if (OB_FAIL(dup_db_item->format_serialize(allocator, source_err_str))) {
      LOG_WARN("failed to format serialize", K(ret), KPC(dup_db_item));
    } else if (OB_FAIL(item.format_serialize(allocator, target_err_str))) {
      LOG_WARN("failed to format serialize", K(ret), K(item));
    } else {
      ret = OB_BACKUP_CONFLICT_VALUE;
      LOG_WARN("input remap database is conflict", K(ret), K(item), KPC(dup_db_item));
      LOG_USER_ERROR(OB_BACKUP_CONFLICT_VALUE, target_err_str.length(), target_err_str.ptr(), source_err_str.length(), source_err_str.ptr());
    }
  } else if (OB_FAIL(remap_database_array_.add_item(item))) {
    LOG_WARN("failed to add remap database item", K(ret), K(item));
  } else {
    LOG_INFO("add one remap database", K(item));
  }
  return ret;
}

int ObImportRemapArg::add_remap_table(const ObRemapTableItem &item)
{
  int ret = OB_SUCCESS;
  const ObImportTableItem *dup_table_item = NULL;
  common::ObArenaAllocator allocator;
  ObString source_err_str;
  ObString target_err_str;

  if (remap_table_array_.is_src_exist(item.src_, dup_table_item)
      || remap_table_array_.is_target_exist(item.target_, dup_table_item)) {
    if (OB_FAIL(dup_table_item->format_serialize(allocator, source_err_str))) {
      LOG_WARN("failed to format serialize", K(ret), KPC(dup_table_item));
    } else if (OB_FAIL(item.format_serialize(allocator, target_err_str))) {
      LOG_WARN("failed to format serialize", K(ret), K(item));
    } else {
      ret = OB_BACKUP_CONFLICT_VALUE;
      LOG_WARN("input remap table is conflict", K(ret), K(item), KPC(dup_table_item));
      LOG_USER_ERROR(OB_BACKUP_CONFLICT_VALUE, target_err_str.length(), target_err_str.ptr(), source_err_str.length(), source_err_str.ptr());
    }
  } else if (OB_FAIL(remap_table_array_.add_item(item))) {
    LOG_WARN("failed to add remap table item", K(ret), K(item));
  } else {
    LOG_INFO("add one remap table", K(item));
  }
  return ret;
}

int ObImportRemapArg::add_remap_parition(const ObRemapPartitionItem &item)
{
  int ret = OB_SUCCESS;
  bool is_dup = false;
  ObSqlString dup_item;
  if (OB_FAIL(check_remap_partition_dup(item, is_dup, dup_item))) {
    LOG_WARN("failed to check remap tablespace dup", K(ret), K(item));
  } else if (is_dup) {
    ret = OB_ENTRY_EXIST;
    LOG_WARN("duplicate partition", K(ret), K(item));
    LOG_USER_ERROR(OB_ENTRY_EXIST, dup_item.ptr());
  } else if (OB_FAIL(remap_partition_array_.add_item(item))) {
    LOG_WARN("failed to add remap partition item", K(ret), K(item));
  }
  return ret;
}

int ObImportRemapArg::add_remap_tablegroup(const ObRemapTablegroupItem &item)
{
  int ret = OB_SUCCESS;
  const ObImportTablegroupItem *dup_tg_item = NULL;
  common::ObArenaAllocator allocator;
  ObString source_err_str;
  ObString target_err_str;

  if (remap_tablegroup_array_.is_src_exist(item.src_, dup_tg_item)
      || remap_tablegroup_array_.is_target_exist(item.target_, dup_tg_item)) {
    if (OB_FAIL(dup_tg_item->format_serialize(allocator, source_err_str))) {
      LOG_WARN("failed to format serialize", K(ret), KPC(dup_tg_item));
    } else if (OB_FAIL(item.format_serialize(allocator, target_err_str))) {
      LOG_WARN("failed to format serialize", K(ret), K(item));
    } else {
      ret = OB_BACKUP_CONFLICT_VALUE;
      LOG_WARN("input remap tablegroup is conflict", K(ret), K(item), KPC(dup_tg_item));
      LOG_USER_ERROR(OB_BACKUP_CONFLICT_VALUE, target_err_str.length(), target_err_str.ptr(), source_err_str.length(), source_err_str.ptr());
    }
  } else if (OB_FAIL(remap_tablegroup_array_.add_item(item))) {
    LOG_WARN("failed to add remap tablegroup item", K(ret), K(item));
  } else {
    LOG_INFO("add one remap tablegroup", K(item));
  }
  return ret;
}

int ObImportRemapArg::add_remap_tablespace(const ObRemapTablespaceItem &item)
{
  int ret = OB_SUCCESS;
  const ObImportTablespaceItem *dup_ts_item = NULL;
  common::ObArenaAllocator allocator;
  ObString source_err_str;
  ObString target_err_str;

  if (remap_tablespace_array_.is_src_exist(item.src_, dup_ts_item)
      || remap_tablespace_array_.is_target_exist(item.target_, dup_ts_item)) {
    if (OB_FAIL(dup_ts_item->format_serialize(allocator, source_err_str))) {
      LOG_WARN("failed to format serialize", K(ret), KPC(dup_ts_item));
    } else if (OB_FAIL(item.format_serialize(allocator, target_err_str))) {
      LOG_WARN("failed to format serialize", K(ret), K(item));
    } else {
      ret = OB_BACKUP_CONFLICT_VALUE;
      LOG_WARN("input remap tablespace is conflict", K(ret), K(item), KPC(dup_ts_item));
      LOG_USER_ERROR(OB_BACKUP_CONFLICT_VALUE, target_err_str.length(), target_err_str.ptr(), source_err_str.length(), source_err_str.ptr());
    }
  } else if (OB_FAIL(remap_tablespace_array_.add_item(item))) {
    LOG_WARN("failed to add remap tablespace item", K(ret), K(item));
  } else {
    LOG_INFO("add one remap tablespace", K(item));
  }
  return ret;
}

int ObImportRemapArg::check_remap_database_dup(
    const ObRemapDatabaseItem &item, bool &is_dup, ObSqlString &dup_item)
{
  int ret = OB_SUCCESS;
  const ObImportDatabaseItem *dup_item_ptr = nullptr;
  is_dup = false;
  if (remap_database_array_.is_src_exist(item.src_, dup_item_ptr)) {
    is_dup = true;
    if (OB_ISNULL(dup_item_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dup item ptr must not be nullptr", K(ret));
    } else if (OB_FAIL(dup_item.assign_fmt("remap duplicate src database, %.*s", item.src_.name_.length(), item.src_.name_.ptr()))) {
      LOG_WARN("failed to assign fmt", K(ret));
    }
  } else if (remap_database_array_.is_target_exist(item.target_, dup_item_ptr)) {
    is_dup = true;
    if (OB_ISNULL(dup_item_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dup item ptr must not be nullptr", K(ret));
    } else if (OB_FAIL(dup_item.assign_fmt("remap duplicate target database, %.*s", item.target_.name_.length(), item.target_.name_.ptr()))) {
      LOG_WARN("failed to assign fmt", K(ret));
    }
  }
  return ret;
}
int ObImportRemapArg::check_remap_table_dup(
    const ObRemapTableItem &item, bool &is_dup, ObSqlString &dup_item)
{
  int ret = OB_SUCCESS;
  const ObImportTableItem *dup_item_ptr = nullptr;
  is_dup = false;
  if (remap_table_array_.is_src_exist(item.src_, dup_item_ptr)) {
    is_dup = true;
    if (OB_ISNULL(dup_item_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dup item ptr must not be nullptr", K(ret));
    } else if (OB_FAIL(dup_item.assign_fmt("remap duplicate src table, %.*s.%.*s",
        item.src_.database_name_.length(), item.src_.database_name_.ptr(),
        item.src_.table_name_.length(), item.src_.table_name_.ptr()))) {
      LOG_WARN("failed to assign fmt", K(ret));
    }
  } else if (remap_table_array_.is_target_exist(item.target_, dup_item_ptr)) {
    is_dup = true;
    if (OB_ISNULL(dup_item_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dup item ptr must not be nullptr", K(ret));
    } else if (OB_FAIL(dup_item.assign_fmt("remap duplicate target table, %.*s.%.*s",
        item.target_.database_name_.length(), item.target_.database_name_.ptr(),
        item.target_.table_name_.length(), item.target_.table_name_.ptr()))) {
      LOG_WARN("failed to assign fmt", K(ret));
    }
  }
  return ret;
}
int ObImportRemapArg::check_remap_partition_dup(
    const ObRemapPartitionItem &item, bool &is_dup, ObSqlString &dup_item)
{
  int ret = OB_SUCCESS;
  const ObImportPartitionItem *src_dup_item_ptr = nullptr;
  const ObImportTableItem *target_dup_item_ptr = nullptr;
  is_dup = false;
  if (remap_partition_array_.is_src_exist(item.src_, src_dup_item_ptr)) {
    is_dup = true;
    if (OB_ISNULL(src_dup_item_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dup item ptr must not be nullptr", K(ret));
    } else if (OB_FAIL(dup_item.assign_fmt("remap duplicate src partition, %.*s.%.*s:%.*s",
        item.src_.database_name_.length(), item.src_.database_name_.ptr(),
        item.src_.table_name_.length(), item.src_.table_name_.ptr(),
        item.src_.partition_name_.length(), item.src_.partition_name_.ptr()))) {
      LOG_WARN("failed to assign fmt", K(ret));
    }
  } else if (remap_table_array_.is_target_exist(item.target_, target_dup_item_ptr)) {
    is_dup = true;
    if (OB_ISNULL(target_dup_item_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dup item ptr must not be nullptr", K(ret));
    } else if (OB_FAIL(dup_item.assign_fmt("remap duplicate target table, %.*s.%.*s",
        item.target_.database_name_.length(), item.target_.database_name_.ptr(),
        item.target_.table_name_.length(), item.target_.table_name_.ptr()))) {
      LOG_WARN("failed to assign fmt", K(ret));
    }
  }
  return ret;
}
int ObImportRemapArg::check_remap_tablegroup_dup(
    const ObRemapTablegroupItem &item, bool &is_dup, ObSqlString &dup_item)
{
  int ret = OB_SUCCESS;
  const ObImportTablegroupItem *dup_item_ptr = nullptr;
  is_dup = false;
  if (remap_tablegroup_array_.is_src_exist(item.src_, dup_item_ptr)) {
    is_dup = true;
    if (OB_ISNULL(dup_item_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dup item ptr must not be nullptr", K(ret));
    } else if (OB_FAIL(dup_item.assign_fmt("remap duplicate src tablegroup, %.*s", item.src_.name_.length(), item.src_.name_.ptr()))) {
      LOG_WARN("failed to assign fmt", K(ret));
    }
  } else if (remap_tablegroup_array_.is_target_exist(item.target_, dup_item_ptr)) {
    is_dup = true;
    if (OB_ISNULL(dup_item_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dup item ptr must not be nullptr", K(ret));
    } else if (OB_FAIL(dup_item.assign_fmt("remap duplicate target tablegroup, %.*s", item.target_.name_.length(), item.target_.name_.ptr()))) {
      LOG_WARN("failed to assign fmt", K(ret));
    }
  }
  return ret;
}
int ObImportRemapArg::check_remap_tablespace_dup(
    const ObRemapTablespaceItem &item, bool &is_dup, ObSqlString &dup_item)
{
  int ret = OB_SUCCESS;
  const ObImportTablespaceItem *dup_item_ptr = nullptr;
  is_dup = false;
  if (remap_tablespace_array_.is_src_exist(item.src_, dup_item_ptr)) {
    is_dup = true;
    if (OB_ISNULL(dup_item_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dup item ptr must not be nullptr", K(ret));
    } else if (OB_FAIL(dup_item.assign_fmt("remap duplicate src tablegroup, %.*s", item.src_.name_.length(), item.src_.name_.ptr()))) {
      LOG_WARN("failed to assign fmt", K(ret));
    }
  } else if (remap_tablespace_array_.is_target_exist(item.target_, dup_item_ptr)) {
    is_dup = true;
    if (OB_ISNULL(dup_item_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dup item ptr must not be nullptr", K(ret));
    } else if (OB_FAIL(dup_item.assign_fmt("remap duplicate target tablegroup, %.*s", item.target_.name_.length(), item.target_.name_.ptr()))) {
      LOG_WARN("failed to assign fmt", K(ret));
    }
  }
  return ret;
}

int ObImportRemapArg::get_remap_database(const ObImportDatabaseItem &src, ObImportDatabaseItem &target) const
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  const ObImportDatabaseItem *p = NULL;
  if (!remap_database_array_.is_remap_target_exist(src, p)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    target = *p;
  }
  return ret;
}

int ObImportRemapArg::get_remap_table(const ObImportTableItem &src, ObImportTableItem &target) const
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  const ObImportTableItem *p = NULL;
  if (!remap_table_array_.is_remap_target_exist(src, p)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    target = *p;
  }
  return ret;
}

int ObImportRemapArg::get_remap_partition(const ObImportPartitionItem &src, ObImportTableItem &target) const
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  const ObImportTableItem *p = NULL;
  if (!remap_partition_array_.is_remap_target_exist(src, p)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    target = *p;
  }
  return ret;
}

int ObImportRemapArg::get_remap_tablegroup(const ObImportTablegroupItem &src, ObImportTablegroupItem &target) const
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  const ObImportTablegroupItem *p = NULL;
  if (!remap_tablegroup_array_.is_remap_target_exist(src, p)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    target = *p;
  }
  return ret;
}

int ObImportRemapArg::get_remap_tablespace(const ObImportTablespaceItem &src, ObImportTablespaceItem &target) const
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  const ObImportTablespaceItem *p = NULL;
  if (!remap_tablespace_array_.is_remap_target_exist(src, p)) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    target = *p;
  }
  return ret;
}

int ObImportRemapArg::get_remap_db_list_format_str(
    common::ObIAllocator &allocator,
    common::ObString &str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_remap_item_array_format_str_(remap_database_array_, allocator, str))) {
    LOG_WARN("failed to get remap db list format str", K(ret), K_(remap_database_array));
  }
  return ret;
}

int ObImportRemapArg::get_remap_table_list_format_str(
    common::ObIAllocator &allocator,
    common::ObString &str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_remap_item_array_format_str_(remap_table_array_, allocator, str))) {
    LOG_WARN("failed to get remap table list format str", K(ret), K_(remap_table_array));
  }
  return ret;
}

int ObImportRemapArg::get_remap_partition_list_format_str(
    common::ObIAllocator &allocator,
    common::ObString &str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_remap_item_array_format_str_(remap_partition_array_, allocator, str))) {
    LOG_WARN("failed to get remap partition list format str", K(ret), K_(remap_partition_array));
  }
  return ret;
}

int ObImportRemapArg::get_remap_tablegroup_list_format_str(
    common::ObIAllocator &allocator,
    common::ObString &str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_remap_item_array_format_str_(remap_tablegroup_array_, allocator, str))) {
    LOG_WARN("failed to get remap tablegroup list format str", K(ret), K_(remap_tablegroup_array));
  }
  return ret;
}

int ObImportRemapArg::get_remap_tablespace_list_format_str(
    common::ObIAllocator &allocator,
    common::ObString &str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_remap_item_array_format_str_(remap_tablespace_array_, allocator, str))) {
    LOG_WARN("failed to get remap tablespace list format str", K(ret), K_(remap_tablespace_array));
  }
  return ret;
}

int ObImportRemapArg::get_remap_db_list_hex_format_str(
    common::ObIAllocator &allocator,
    common::ObString &str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_remap_item_array_hex_format_str_(remap_database_array_, allocator, str))) {
    LOG_WARN("failed to get remap db list hex_format str", K(ret), K_(remap_database_array));
  }
  return ret;
}

int ObImportRemapArg::get_remap_table_list_hex_format_str(
    common::ObIAllocator &allocator,
    common::ObString &str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_remap_item_array_hex_format_str_(remap_table_array_, allocator, str))) {
    LOG_WARN("failed to get remap table list hex_format str", K(ret), K_(remap_table_array));
  }
  return ret;
}

int ObImportRemapArg::get_remap_partition_list_hex_format_str(
    common::ObIAllocator &allocator,
    common::ObString &str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_remap_item_array_hex_format_str_(remap_partition_array_, allocator, str))) {
    LOG_WARN("failed to get remap partition list hex format str", K(ret), K_(remap_partition_array));
  }
  return ret;
}

int ObImportRemapArg::get_remap_tablegroup_list_hex_format_str(
    common::ObIAllocator &allocator,
    common::ObString &str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_remap_item_array_hex_format_str_(remap_tablegroup_array_, allocator, str))) {
    LOG_WARN("failed to get remap tablegroup list hex format str", K(ret), K_(remap_tablegroup_array));
  }
  return ret;
}

int ObImportRemapArg::get_remap_tablespace_list_hex_format_str(
    common::ObIAllocator &allocator,
    common::ObString &str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_remap_item_array_hex_format_str_(remap_tablespace_array_, allocator, str))) {
    LOG_WARN("failed to get remap tablespace list hex format str", K(ret), K_(remap_tablespace_array));
  }
  return ret;
}

int ObImportRemapArg::remap_db_list_deserialize_hex_format_str(const common::ObString &str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(remap_item_array_deserialize_hex_format_str_(remap_database_array_, str))) {
    LOG_WARN("fail to deserialize hex format remap database array", K(ret), K(str));
  }
  return ret;
}

int ObImportRemapArg::remap_table_list_deserialize_hex_format_str(const common::ObString &str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(remap_item_array_deserialize_hex_format_str_(remap_table_array_, str))) {
    LOG_WARN("fail to deserialize hex format remap table array", K(ret), K(str));
  }
  return ret;
}

int ObImportRemapArg::remap_partition_list_deserialize_hex_format_str(const common::ObString &str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(remap_item_array_deserialize_hex_format_str_(remap_partition_array_, str))) {
    LOG_WARN("fail to deserialize hex format remap partition array", K(ret), K(str));
  }
  return ret;
}

int ObImportRemapArg::remap_tablegroup_list_deserialize_hex_format_str(const common::ObString &str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(remap_item_array_deserialize_hex_format_str_(remap_tablegroup_array_, str))) {
    LOG_WARN("fail to deserialize hex format remap tablegroup array", K(ret), K(str));
  }
  return ret;
}

int ObImportRemapArg::remap_tablespace_list_deserialize_hex_format_str(const common::ObString &str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(remap_item_array_deserialize_hex_format_str_(remap_tablespace_array_, str))) {
    LOG_WARN("fail to deserialize hex format remap tablespace array", K(ret), K(str));
  }
  return ret;
}

}
}