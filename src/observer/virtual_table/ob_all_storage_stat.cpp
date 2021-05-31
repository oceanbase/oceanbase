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

#include "observer/virtual_table/ob_all_storage_stat.h"
#include "storage/ob_partition_scheduler.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_pg_partition.h"
#include "observer/ob_server.h"
#include "lib/utility/utility.h"

using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;

namespace oceanbase {
namespace observer {
ObInfoSchemaStorageStatTable::ObInfoSchemaStorageStatTable()
    : ObVirtualTableScannerIterator(), addr_(NULL), sstable_iter_(0), ipstr_(), port_(0), pg_partition_iter_(NULL)
{
  memset(column_checksum_, 0, OB_MAX_VARCHAR_LENGTH + 1);
  memset(macro_blocks_, 0, OB_MAX_VARCHAR_LENGTH + 1);
}

ObInfoSchemaStorageStatTable::~ObInfoSchemaStorageStatTable()
{
  reset();
}

void ObInfoSchemaStorageStatTable::reset()
{
  ObVirtualTableScannerIterator::reset();
  if (NULL != pg_partition_iter_) {
    ObPartitionService::get_instance().revert_pg_partition_iter(pg_partition_iter_);
    pg_partition_iter_ = NULL;
  }
  addr_ = NULL;
  sstable_iter_ = 0;
  port_ = 0;
  ipstr_.reset();
  cur_partition_ = NULL;
  stores_handle_.reset();
  sstable_list_.reset();
  memset(column_checksum_, 0, OB_MAX_VARCHAR_LENGTH + 1);
  memset(macro_blocks_, 0, OB_MAX_VARCHAR_LENGTH + 1);
}

int ObInfoSchemaStorageStatTable::set_ip(common::ObAddr* addr)
{
  int ret = OB_SUCCESS;
  char ipbuf[common::OB_IP_STR_BUFF];
  if (NULL == addr) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!addr_->ip_to_string(ipbuf, sizeof(ipbuf))) {
    SERVER_LOG(ERROR, "ip to string failed");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ipstr_ = ObString::make_string(ipbuf);
    if (OB_FAIL(ob_write_string(*allocator_, ipstr_, ipstr_))) {
      SERVER_LOG(WARN, "failed to write string", K(ret));
    }
    port_ = addr_->get_port();
  }
  return ret;
}

int ObInfoSchemaStorageStatTable::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObObj* cells = cur_row_.cells_;
  if (OB_ISNULL(allocator_) || OB_ISNULL(cells)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is NULL", KP(allocator_), KP(allocator_), K(ret));
  } else {
    const int64_t col_count = output_column_ids_.count();
    if (NULL == pg_partition_iter_) {
      if (OB_FAIL(set_ip(addr_))) {
        SERVER_LOG(WARN, "can't get ip", K(ret));
      } else if (NULL == (pg_partition_iter_ = ObPartitionService::get_instance().alloc_pg_partition_iter())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SERVER_LOG(WARN, "Fail to allocate partition iter, ", K(ret));
      }
    }

    while (OB_SUCC(ret)) {
      if (OB_SUCCESS == ret && 0 == sstable_iter_) {
        stores_handle_.reset();
        if (OB_FAIL(pg_partition_iter_->get_next(cur_partition_))) {
          if (OB_ITER_END != ret) {
            SERVER_LOG(WARN, "Fail to get next partition, ", K(ret));
          }
        } else if (NULL == cur_partition_) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "The cur partition is NULL, ", K(ret));
        } else if (OB_FAIL(cur_partition_->get_storage()->get_all_tables(stores_handle_))) {
          SERVER_LOG(WARN, "can't get ssstores", K(ret));
        } else if (OB_FAIL(stores_handle_.get_all_sstables(sstable_list_))) {
          SERVER_LOG(WARN, "failed to get all sstables", K(ret));
        }
      }
      if (OB_SUCC(ret) && sstable_iter_ < sstable_list_.count()) {
        const common::ObPartitionKey pkey = cur_partition_->get_partition_key();
        const blocksstable::ObSSTableMeta& sstable_meta = sstable_list_.at(sstable_iter_)->get_meta();
        for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < col_count; ++cell_idx) {
          uint64_t col_id = output_column_ids_.at(cell_idx);
          switch (col_id) {
            case TENANT_ID: {
              cells[cell_idx].set_int(extract_tenant_id(pkey.get_table_id()));
              break;
            }
            case SVR_IP: {
              cells[cell_idx].set_varchar(ipstr_);
              cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case SVR_PORT: {
              cells[cell_idx].set_int(port_);
              break;
            }
            case TABLE_ID: {
              cells[cell_idx].set_int(pkey.get_table_id());
              break;
            }
            case PARTITION_CNT: {
              cells[cell_idx].set_int(pkey.get_partition_cnt());
              break;
            }
            case PARTITION_ID: {
              cells[cell_idx].set_int(pkey.get_partition_id());
              break;
            }
            case MAJOR_VERSION: {
              cells[cell_idx].set_int(ObVersion::get_major(sstable_meta.data_version_));
              break;
            }
            case MINOR_VERSION: {
              cells[cell_idx].set_int(ObVersion::get_minor(sstable_meta.data_version_));
              break;
            }
            case SSTABLE_ID: {
              cells[cell_idx].set_int(sstable_list_.at(sstable_iter_)->get_meta().index_id_);
              break;
            }
            case ROLE: {
              ObRole role;
              if (OB_FAIL(cur_partition_->get_pg()->get_role(role))) {
                SERVER_LOG(WARN, "get partition role failed", K(ret));
              } else {
                cells[cell_idx].set_int(role);
              }
              break;
            }
            case DATA_CHECKSUM: {
              if (sstable_list_.count() != 0) {
                cells[cell_idx].set_int(sstable_list_.at(sstable_iter_)->get_meta().data_checksum_);
              } else {
                cells[cell_idx].set_int(0);
              }
              break;
            }
            case COLUM_CHECKSUM: {
              memset(column_checksum_, 0, OB_MAX_VARCHAR_LENGTH + 1);
              if (sstable_list_.count() != 0) {
                int64_t pos = 0;
                for (int64_t j = 0; j < sstable_list_.at(sstable_iter_)->get_meta().column_metas_.count(); ++j) {
                  if (0 == j) {
                    databuff_printf(column_checksum_, static_cast<int64_t>(sizeof(column_checksum_)), pos, "[");
                  }
                  databuff_printf(column_checksum_,
                      static_cast<int64_t>(sizeof(column_checksum_)),
                      pos,
                      "%ld",
                      sstable_list_.at(sstable_iter_)->get_meta().column_metas_.at(j).column_checksum_);
                  if (sstable_list_.at(sstable_iter_)->get_meta().column_metas_.count() - 1 == j) {
                    databuff_printf(column_checksum_, static_cast<int64_t>(sizeof(column_checksum_)), pos, "]");
                  } else {
                    databuff_printf(column_checksum_, static_cast<int64_t>(sizeof(column_checksum_)), pos, ",");
                  }
                }
                cells[cell_idx].set_varchar(column_checksum_);
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              } else {
                cells[cell_idx].set_varchar("[]");
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              }
              break;
            }
            case MACRO_BLOCKS: {
              memset(macro_blocks_, 0, OB_MAX_VARCHAR_LENGTH + 1);
              if (sstable_list_.count() != 0) {
                sstable_list_.at(sstable_iter_)
                    ->get_total_macro_blocks()
                    .to_string(macro_blocks_, sizeof(macro_blocks_));
                cells[cell_idx].set_varchar(macro_blocks_);
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              } else {
                cells[cell_idx].set_varchar("[]");
                cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              }
              break;
            }
            case OCCUPY_SIZE: {
              if (sstable_list_.count() != 0) {
                cells[cell_idx].set_int(sstable_meta.occupy_size_);
              } else {
                cells[cell_idx].set_int(0);
              }
              break;
            }
            case USED_SIZE: {
              if (sstable_list_.count() != 0) {
                cells[cell_idx].set_int(
                    sstable_meta.get_total_macro_block_count() * OB_FILE_SYSTEM.get_macro_block_size());
              } else {
                cells[cell_idx].set_int(0);
              }
              break;
            }
            case ROW_COUNT: {
              if (sstable_list_.count() != 0) {
                cells[cell_idx].set_int(sstable_meta.row_count_);
              } else {
                cells[cell_idx].set_int(0);
              }
              break;
            }
            case STORE_TYPE: {
              cells[cell_idx].set_int(sstable_list_.at(sstable_iter_)->get_key().table_type_);
              break;
            }
            case PROGRESSIVE_MERGE_START_VERSION: {
              cells[cell_idx].set_int(sstable_meta.progressive_merge_start_version_);
              break;
            }
            case PROGRESSIVE_MERGE_END_VERSION: {
              cells[cell_idx].set_int(sstable_meta.progressive_merge_end_version_);
              break;
            }
            default: {
              ret = OB_ERR_UNEXPECTED;
              SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(output_column_ids_), K(col_id));
              break;
            }
          }
        }

        if (OB_SUCC(ret)) {
          sstable_iter_++;
          row = &cur_row_;
          if (sstable_iter_ >= sstable_list_.count()) {
            sstable_iter_ = 0;
          }
          break;
        }
      }
    }
  }
  return ret;
}
}  // namespace observer
}  // namespace oceanbase
