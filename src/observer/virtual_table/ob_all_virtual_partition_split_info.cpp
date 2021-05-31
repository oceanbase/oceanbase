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

#include "ob_all_virtual_partition_split_info.h"
#include "observer/ob_server.h"
#include "observer/ob_server_utils.h"
#include "share/ob_tenant_mgr.h"

namespace oceanbase {
using namespace common;
using namespace storage;
namespace observer {
ObAllVirtualPartitionSplitInfo::ObAllVirtualPartitionSplitInfo()
    : ObVirtualTableIterator(), ipstr_(), index_(0), split_infos_()
{}

ObAllVirtualPartitionSplitInfo::~ObAllVirtualPartitionSplitInfo()
{
  reset();
}

int ObAllVirtualPartitionSplitInfo::init(share::schema::ObMultiVersionSchemaService& schema_service)
{
  int ret = OB_SUCCESS;
  bool is_finish = true;
  ObPartitionStorage* storage = NULL;
  ObMultiVersionTableStore table_store;
  ObIPartitionArrayGuard partitions;
  ObTablesHandle handle;
  ObVirtualPartitionSplitInfo split_info;

  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator_ shouldn't be NULL", K_(allocator), K(ret));
  } else if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr_))) {
    SERVER_LOG(ERROR, "get server ip failed", K(ret));
  } else if (OB_FAIL(ObPartitionService::get_instance().get_all_partitions(partitions))) {
    SERVER_LOG(WARN, "Failed to get all partitions", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < partitions.count(); i++) {
      ObPartitionArray pkeys;
      if (OB_FAIL(partitions.at(i)->get_all_pg_partition_keys(pkeys))) {
        STORAGE_LOG(WARN, "get all pg partition keys", "pg_key", partitions.at(i)->get_partition_key(), K(pkeys));
      } else {
        for (int64_t key_index = 0; OB_SUCC(ret) && key_index < pkeys.count(); ++key_index) {
          ObPGPartitionGuard guard;
          const ObPartitionKey& pkey = pkeys.at(key_index);
          if (OB_FAIL(partitions.at(i)->get_pg_partition(pkey, guard))) {
            STORAGE_LOG(WARN, "get pg partition error", K(ret), K(pkey));
          } else if (OB_ISNULL(guard.get_pg_partition())) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "pg partition is null, unexpected error", K(ret), K(pkey));
          } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(guard.get_pg_partition()->get_storage()))) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "error unexpected, storage must not be NULL", K(ret), K(pkey));
          } else if (OB_FAIL(storage->get_partition_store().is_physical_split_finished(is_finish))) {
            STORAGE_LOG(WARN, "Failed to check physical split finish", K(ret));
          } else if (OB_SUCC(ret) && !is_finish) {
            if (OB_FAIL(storage->get_partition_store().get_physical_split_info(split_info))) {
              STORAGE_LOG(WARN, "Failed to get physical split info", K(ret));
            } else {
              int64_t split_schema_version = partitions.at(i)->get_split_info().get_schema_version();
              share::schema::ObSchemaGetterGuard schema_guard;
              const share::schema::ObTableSchema* table_schema = NULL;
              const uint64_t tenant_id = extract_tenant_id(split_info.table_id_);
              if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard, split_schema_version))) {
                STORAGE_LOG(WARN, "Fail to get schema mgr by version, ", K(ret), K(split_schema_version));
              } else if (OB_FAIL(schema_guard.get_table_schema(split_info.table_id_, table_schema))) {
                if (OB_ENTRY_NOT_EXIST == ret) {
                  ret = OB_TABLE_IS_DELETED;
                  STORAGE_LOG(INFO, "table is deleted", K(split_info.table_id_), K(split_schema_version));
                } else {
                  STORAGE_LOG(WARN, "Fail to get table schema, ", K(ret), K(split_info.table_id_));
                }
              } else if (OB_ISNULL(table_schema)) {
                ret = OB_ERR_UNEXPECTED;
                STORAGE_LOG(WARN, "talbe schema should not be NULL", K(ret));
              } else if (OB_FAIL(split_infos_.push_back(split_info))) {
                STORAGE_LOG(WARN, "failed to push back split info", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

void ObAllVirtualPartitionSplitInfo::reset()
{
  ipstr_.reset();
  index_ = 0;
  split_infos_.reset();
}

int ObAllVirtualPartitionSplitInfo::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObVirtualPartitionSplitInfo split_info;
  int64_t col_count = output_column_ids_.count();
  if (index_ >= split_infos_.count()) {
    ret = OB_ITER_END;
  } else {
    split_info = split_infos_.at(index_);
    index_++;
  }

  if (OB_SUCC(ret)) {
    ObObj* cells = cur_row_.cells_;
    if (OB_UNLIKELY(NULL == cells)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
        const uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
          case TENANT_ID: {
            cells[i].set_int(static_cast<int64_t>(extract_tenant_id(split_info.table_id_)));
            break;
          }
          case SVR_IP: {
            cells[i].set_varchar(ipstr_);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SVR_PORT: {
            cells[i].set_int(GCONF.self_addr_.get_port());
            break;
          }
          case ZONE: {
            cells[i].set_varchar(GCONF.zone);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case TABLE_ID: {
            cells[i].set_int(split_info.table_id_);
            break;
          }
          case PARTITION_ID: {
            cells[i].set_int(split_info.partition_id_);
            break;
          }
          case SPLIT_STATE: {
            if (split_info.is_major_split_) {
              cells[i].set_varchar("major split");
            } else {
              cells[i].set_varchar("minor split");
            }
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case MERGE_VERSION: {
            cells[i].set_int(split_info.merge_version_);
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "unexpected column id", K(col_id), K(i), K(ret));
            break;
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  } else {
    // do nothing;
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
