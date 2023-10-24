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
#include "share/restore/ob_import_table_arg.h"
#include "lib/string/ob_sql_string.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace share
{
// ObImportTableArg
OB_SERIALIZE_MEMBER(
    ObImportTableArg,
    is_import_all_,
    database_array_,
    table_array_,
    partition_array_);

ObImportTableArg::ObImportTableArg()
  : is_import_all_(false),
    database_array_(),
    table_array_(),
    partition_array_()
{}

int ObImportTableArg::set_import_all()
{
  int ret = OB_SUCCESS;
  is_import_all_ = true;
  return ret;
}

void ObImportTableArg::reset()
{
  is_import_all_ = false;
  database_array_.reset();
  table_array_.reset();
  partition_array_.reset();
}

int ObImportTableArg::assign(const ObImportTableArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(database_array_.assign(other.get_import_database_array()))) {
    LOG_WARN("failed to assign database array", K(ret), "database array", other.get_import_database_array());
  } else if (OB_FAIL(table_array_.assign(other.get_import_table_array()))) {
    LOG_WARN("failed to assign database array", K(ret), "table array", other.get_import_table_array());
  } else if (OB_FAIL(partition_array_.assign(other.get_import_partition_array()))) {
    LOG_WARN("failed to assign database array", K(ret), "partition array", other.get_import_partition_array());
  } else {
    is_import_all_ = other.is_import_all();
  }
  return ret;
}

int ObImportTableArg::add_database(const ObImportDatabaseItem &item)
{
  int ret = OB_SUCCESS;
  const ObImportDatabaseItem *dup_db_item = NULL;
  const ObImportTableItem *dup_table_item = NULL;
  common::ObArenaAllocator allocator;
  ObString source_err_str;
  ObString target_err_str;

  if (database_array_.is_exist(item, dup_db_item)) {
    if (OB_FAIL(dup_db_item->format_serialize(allocator, source_err_str))) {
      LOG_WARN("failed to format serialize", K(ret), KPC(dup_db_item));
    } else if (OB_FAIL(item.format_serialize(allocator, target_err_str))) {
      LOG_WARN("failed to format serialize", K(ret), K(item));
    } else {
      ret = OB_BACKUP_CONFLICT_VALUE;
      LOG_WARN("input database is conflict", K(ret), K(item), KPC(dup_db_item));
      LOG_USER_ERROR(OB_BACKUP_CONFLICT_VALUE, target_err_str.length(), target_err_str.ptr(), source_err_str.length(), source_err_str.ptr());
    }
  } else if (table_array_.is_exist_table_in_database(item, dup_table_item)) {
    if (OB_FAIL(dup_table_item->format_serialize(allocator, source_err_str))) {
      LOG_WARN("failed to format serialize", K(ret), KPC(dup_table_item));
    } else if (OB_FAIL(item.format_serialize(allocator, target_err_str))) {
      LOG_WARN("failed to format serialize", K(ret), K(item));
    } else {
      ret = OB_BACKUP_CONFLICT_VALUE;
      LOG_WARN("input database is conflict", K(ret), K(item), KPC(dup_db_item));
      LOG_USER_ERROR(OB_BACKUP_CONFLICT_VALUE, target_err_str.length(), target_err_str.ptr(), source_err_str.length(), source_err_str.ptr());
    }
  } else if (OB_FAIL(database_array_.add_item(item))) {
    LOG_WARN("failed to add table", K(ret), K(item));
  } else {
    LOG_INFO("add one database", K(item));
  }
  return ret;
}

int ObImportTableArg::add_table(const ObImportTableItem &item)
{
  int ret = OB_SUCCESS;
  const ObImportDatabaseItem *dup_db_item = NULL;
  const ObImportTableItem *dup_table_item = NULL;
  common::ObArenaAllocator allocator;
  ObString source_err_str;
  ObString target_err_str;
  const ObImportDatabaseItem db_item = item.get_database();
  if (database_array_.is_exist(db_item, dup_db_item)) {
    if (OB_FAIL(dup_db_item->format_serialize(allocator, source_err_str))) {
      LOG_WARN("failed to format serialize", K(ret), KPC(dup_db_item));
    } else if (OB_FAIL(item.format_serialize(allocator, target_err_str))) {
      LOG_WARN("failed to format serialize", K(ret), K(item));
    } else {
      ret = OB_BACKUP_CONFLICT_VALUE;
      LOG_WARN("input table is conflict", K(ret), K(item), KPC(dup_db_item));
      LOG_USER_ERROR(OB_BACKUP_CONFLICT_VALUE, target_err_str.length(), target_err_str.ptr(), source_err_str.length(), source_err_str.ptr());
    }
  } else if (table_array_.is_exist(item, dup_table_item)) {
    if (OB_FAIL(dup_table_item->format_serialize(allocator, source_err_str))) {
      LOG_WARN("failed to format serialize", K(ret), KPC(dup_table_item));
    } else if (OB_FAIL(item.format_serialize(allocator, target_err_str))) {
      LOG_WARN("failed to format serialize", K(ret), K(item));
    } else {
      ret = OB_BACKUP_CONFLICT_VALUE;
      LOG_WARN("input table is conflict", K(ret), K(item), KPC(dup_db_item));
      LOG_USER_ERROR(OB_BACKUP_CONFLICT_VALUE, target_err_str.length(), target_err_str.ptr(), source_err_str.length(), source_err_str.ptr());
    }
  } else if (OB_FAIL(table_array_.add_item(item))) {
    LOG_WARN("failed to add table", K(ret), K(item));
  } else {
    LOG_INFO("add one table", K(item));
  }
  return ret;
}

int ObImportTableArg::add_partition(const ObImportPartitionItem &item)
{
  int ret = OB_SUCCESS;
  bool is_dup = false;
  ObSqlString dup_item;
  if (OB_FAIL(check_partion_dup(item, is_dup, dup_item))) {
    LOG_WARN("failed to check able dup", K(ret), K(item));
  } else if (is_dup) {
    ret = OB_ENTRY_EXIST;
    LOG_WARN("duplicate partition", K(ret), K(item));
    LOG_USER_ERROR(OB_ENTRY_EXIST, dup_item.ptr());
  } else if (OB_FAIL(partition_array_.add_item(item))) {
    LOG_WARN("failed to add table", K(ret), K(item));
  }
  return ret;
}

int ObImportTableArg::check_database_dup(const ObImportDatabaseItem &item, bool &is_dup, ObSqlString &dup_item) const
{
  int ret = OB_SUCCESS;
  is_dup = false;
  if (database_array_.is_exist(item)) {
    is_dup = true;
    if (OB_FAIL(dup_item.assign_fmt("duplicate recover database, %.*s", item.name_.length(), item.name_.ptr()))) {
      LOG_WARN("failed to assign fmt", K(ret));
    }
  }
  return ret;
}
int ObImportTableArg::check_table_dup(const ObImportTableItem &item, bool &is_dup, ObSqlString &dup_item) const
{
  int ret = OB_SUCCESS;
  is_dup = false;
  if (table_array_.is_exist(item)) {
    is_dup = true;
    if (OB_FAIL(dup_item.assign_fmt("duplicate recover table, %.*s.%.*s",
        item.database_name_.length(), item.database_name_.ptr(),
        item.table_name_.length(), item.table_name_.ptr()))) {
      LOG_WARN("failed to assign fmt", K(ret));
    }
  }
  return ret;
}
int ObImportTableArg::check_partion_dup(const ObImportPartitionItem &item, bool &is_dup, ObSqlString &dup_item) const
{
  int ret = OB_SUCCESS;
  is_dup = false;
  if (partition_array_.is_exist(item)) {
    is_dup = true;
    if (OB_FAIL(dup_item.assign_fmt("duplicate recover partition, %.*s.%.*s:%.*s",
        item.database_name_.length(), item.database_name_.ptr(),
        item.table_name_.length(), item.table_name_.ptr(),
        item.partition_name_.length(), item.partition_name_.ptr()))) {
      LOG_WARN("failed to assign fmt", K(ret));
    }
  }
  return ret;
}

int ObImportTableArg::get_db_list_format_str(
    common::ObIAllocator &allocator, common::ObString &str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_item_array_format_str_(database_array_, allocator, str))) {
    LOG_WARN("failed to get import database list format str", K(ret), K_(database_array));
  }
  return ret;
}

int ObImportTableArg::get_table_list_format_str(
    common::ObIAllocator &allocator, common::ObString &str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_item_array_format_str_(table_array_, allocator, str))) {
    LOG_WARN("failed to get import table list format str", K(ret), K_(table_array));
  }
  return ret;
}

int ObImportTableArg::get_partition_list_format_str(
    common::ObIAllocator &allocator, common::ObString &str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_item_array_format_str_(partition_array_, allocator, str))) {
    LOG_WARN("failed to get import partition list format str", K(ret), K_(partition_array));
  }
  return ret;
}


int ObImportTableArg::get_db_list_hex_format_str(
    common::ObIAllocator &allocator, common::ObString &str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_item_array_hex_format_str_(database_array_, allocator, str))) {
    LOG_WARN("failed to get import database list hex format str", K(ret), K_(database_array));
  }
  return ret;
}

int ObImportTableArg::get_table_list_hex_format_str(
    common::ObIAllocator &allocator, common::ObString &str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_item_array_hex_format_str_(table_array_, allocator, str))) {
    LOG_WARN("failed to get import table list hex format str", K(ret), K_(table_array));
  }
  return ret;
}

int ObImportTableArg::get_partition_list_hex_format_str(
    common::ObIAllocator &allocator, common::ObString &str) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_item_array_hex_format_str_(partition_array_, allocator, str))) {
    LOG_WARN("failed to get import partition list hex format str", K(ret), K_(partition_array));
  }
  return ret;
}

int ObImportTableArg::db_list_deserialize_hex_format_str(const common::ObString &str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(item_array_deserialize_hex_format_str_(database_array_, str))) {
    LOG_WARN("fail to deserialize hex format import database array", K(ret), K(str));
  }
  return ret;
}

int ObImportTableArg::table_list_deserialize_hex_format_str(const common::ObString &str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(item_array_deserialize_hex_format_str_(table_array_, str))) {
    LOG_WARN("fail to deserialize hex format import table array", K(ret), K(str));
  }
  return ret;
}

int ObImportTableArg::partition_list_deserialize_hex_format_str(const common::ObString &str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(item_array_deserialize_hex_format_str_(partition_array_, str))) {
    LOG_WARN("fail to deserialize hex format import partition array", K(ret), K(str));
  }
  return ret;
}

}
}