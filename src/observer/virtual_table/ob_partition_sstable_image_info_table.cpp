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

#define USING_LOG_PREFIX STORAGE
#include "observer/virtual_table/ob_partition_sstable_image_info_table.h"
#include "share/config/ob_server_config.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/ob_force_print_log.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_i_partition_group.h"
#include "storage/ob_partition_storage.h"
#include "storage/ob_partition_scheduler.h"
#include "storage/compaction/ob_partition_merge_util.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_array_iterator.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::compaction;
namespace oceanbase {
namespace observer {

ObPartitionSstableImageInfoTable::ObPartitionSstableImageInfoTable()
    : ObVirtualTableScannerIterator(), inited_(false), addr_(NULL), partition_service_(NULL), schema_service_(NULL)
{}

ObPartitionSstableImageInfoTable::~ObPartitionSstableImageInfoTable()
{}

int ObPartitionSstableImageInfoTable::init(ObPartitionService& partition_service,
    share::schema::ObMultiVersionSchemaService& schema_service, common::ObAddr& addr)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(ERROR, "init twice");
  } else {
    inited_ = true;
    partition_service_ = &partition_service;
    schema_service_ = &schema_service;
    addr_ = &addr;
  }
  return ret;
}

int64_t ObPartitionSstableImageInfoTable::find(
    ObVersion& version, ObArray<SSStoreVersionInfo>& ss_store_version_info_list) const
{
  int64_t pos = -1;
  for (int64_t i = 0; i < ss_store_version_info_list.count(); ++i) {
    if (version == ss_store_version_info_list.at(i).version_) {
      pos = i;
      break;
    }
  }
  return pos;
}

int ObPartitionSstableImageInfoTable::get_table_store_cnt(int64_t& table_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObPartitionService::get_instance().get_table_store_cnt(table_cnt))) {
    SERVER_LOG(WARN, "failed to get table store", K(ret));
  }
  return ret;
}

// TODO(): fix it
int ObPartitionSstableImageInfoTable::get_ss_store_version_info(ObArray<SSStoreVersionInfo>& ss_store_version_info_list)
{
  int ret = OB_SUCCESS;
  bool table_exist = false;
  ObPGPartitionIterator* partition_iter = NULL;
  ObPGPartition* partition = NULL;
  ObIPartitionStorage* storage = NULL;
  ObArray<PartitionSSStoreInfo> partition_ss_store_info_list;
  SSStoreVersionInfo info;
  uint64_t table_id = OB_INVALID_ID;
  int64_t table_schema_version = OB_INVALID_VERSION;

  if (NULL == (partition_iter = partition_service_->alloc_pg_partition_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "Fail to alloc partition iter, ", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      partition_ss_store_info_list.reset();
      table_exist = false;
      if (OB_FAIL(partition_iter->get_next(partition))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "Fail to get next partition, ", K(ret));
        }
      } else if (OB_ISNULL(partition)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "The partition is NULL", K(ret));
      } else if (NULL == (storage = partition->get_storage())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "The storage is NULL", K(ret));
      } else if (FALSE_IT(table_id = partition->get_partition_key().table_id_)) {
      } else if (OB_FAIL(schema_service_->check_table_exist(table_id, table_schema_version, table_exist))) {
        SERVER_LOG(WARN, "Fail to check if table exist, ", K(ret), K(table_id));
      } else if (!table_exist) {
        SERVER_LOG(INFO, "table is droped, no need to statistics", K(ret), K(table_id));
      } else {
        if (OB_FAIL(storage->get_partition_ss_store_info(partition_ss_store_info_list))) {
          SERVER_LOG(WARN, "fail to get storage version and macro block count", K(ret));
        } else {
          int64_t pos = 0;
          for (int64_t j = 0; OB_SUCC(ret) && j < partition_ss_store_info_list.count(); ++j) {
            if (-1 == (pos = find(partition_ss_store_info_list.at(j).version_, ss_store_version_info_list))) {
              info.reset();
              info.version_ = partition_ss_store_info_list.at(j).version_;
              if (OB_SUCCESS != (ret = ss_store_version_info_list.push_back(info))) {
                SERVER_LOG(WARN, "fail to add info", K(ret));
              } else {
                pos = ss_store_version_info_list.count() - 1;
              }
            }
            if (OB_SUCC(ret)) {
              ss_store_version_info_list.at(pos).ss_store_count_++;
              ss_store_version_info_list.at(pos).macro_block_count_ +=
                  partition_ss_store_info_list.at(j).macro_block_count_;
              ss_store_version_info_list.at(pos).use_old_macro_block_count_ +=
                  partition_ss_store_info_list.at(j).use_old_macro_block_count_;
              ss_store_version_info_list.at(pos).rewrite_macro_old_micro_block_count_ +=
                  partition_ss_store_info_list.at(j).rewrite_macro_old_micro_block_count_;
              ss_store_version_info_list.at(pos).rewrite_macro_total_micro_block_count_ +=
                  partition_ss_store_info_list.at(j).rewrite_macro_total_micro_block_count_;

              if (true == partition_ss_store_info_list.at(j).is_merged_) {
                ++ss_store_version_info_list.at(pos).merged_ss_store_count_;
                if (true == partition_ss_store_info_list.at(j).is_modified_) {
                  ++ss_store_version_info_list.at(pos).modified_ss_store_count_;
                }
              }
            }
          }
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      std::sort(ss_store_version_info_list.begin(), ss_store_version_info_list.end());
    }
  }
  if (NULL != partition_iter) {
    partition_service_->revert_pg_partition_iter(partition_iter);
  }

  return ret;
}

int ObPartitionSstableImageInfoTable::fill_scaner()
{
  int ret = OB_SUCCESS;
  ObMergeStatEntry merge_stat;
  ObArray<SSStoreVersionInfo> ss_store_version_info_list;
  ss_store_version_info_list.reset();
  int64_t col_count = output_column_ids_.count();
  ObObj* cells = NULL;
  char ipbuf[common::OB_IP_STR_BUFF] = {'\0'};
  ObString ipstr;
  if (!inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(ERROR, "not init");
  } else if (OB_SUCCESS != (ret = get_ss_store_version_info(ss_store_version_info_list))) {
    SERVER_LOG(WARN, "Fail to get ss_store version info, ", K(ret));
  } else if (0 == col_count) {
    SERVER_LOG(ERROR, "col count could not be zero");
    ret = OB_ERR_UNEXPECTED;
  } else if (NULL == (cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else {
    int64_t table_cnt = 0;
    if (OB_FAIL(get_table_store_cnt(table_cnt))) {
      SERVER_LOG(WARN, "failed to get table store", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ss_store_version_info_list.count(); ++i) {
      SSStoreVersionInfo& version_info = ss_store_version_info_list.at(i);
      const int64_t frozen_version = version_info.version_.major_;
      ObPartitionScheduler::get_instance().get_merge_stat(frozen_version, merge_stat);
      for (int64_t k = 0; OB_SUCC(ret) && k < output_column_ids_.count(); ++k) {
        uint64_t col_id = output_column_ids_.at(k);
        switch (col_id) {
          case 16: {
            // zone
            cells[k].set_varchar(GCONF.zone);
            cells[k].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case 17: {
            // svr_ip
            MEMSET(ipbuf, '\0', common::OB_IP_STR_BUFF);
            if (!addr_->ip_to_string(ipbuf, common::OB_IP_STR_BUFF)) {
              SERVER_LOG(ERROR, "ip to string failed");
              ret = OB_ERR_UNEXPECTED;
            } else {
              ipstr = ObString::make_string(ipbuf);
              if (OB_SUCCESS != (ret = ob_write_string(*allocator_, ipstr, ipstr))) {
                SERVER_LOG(WARN, "write string error", K(ret));
              } else {
                cells[k].set_varchar(ipstr);
                cells[k].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
              }
            }
            break;
          }
          case 18: {
            // svr_port
            const int32_t port = addr_->get_port();
            cells[k].set_int(port);
            break;
          }
          case 19: {
            // major_version
            const int64_t major_version = version_info.version_.major_;
            cells[k].set_int(major_version);
            break;
          }
          case 20: {
            // min_version
            const int64_t min_version = version_info.version_.minor_;
            cells[k].set_int(min_version);
            break;
          }
          case 21: {
            // ss_store_count
            cells[k].set_int(version_info.ss_store_count_);
            break;
          }
          case 22: {
            // merged_ss_store_count
            cells[k].set_int(version_info.merged_ss_store_count_);
            break;
          }
          case 23: {
            // modified_ss_store_count
            const int64_t modified_ss_store_count = version_info.modified_ss_store_count_;
            cells[k].set_int(modified_ss_store_count);
            break;
          }
          case 24: {
            // macro_block_count
            const int64_t macro_block_count = version_info.macro_block_count_;
            cells[k].set_int(macro_block_count);
            break;
          }
          case 25: {
            // use_old_macro_block_count
            const int64_t use_old_macro_block_count = version_info.use_old_macro_block_count_;
            cells[k].set_int(use_old_macro_block_count);
            break;
          }
          case 26: {
            // merge_start_time
            if (0 == merge_stat.start_time_) {
              cells[k].set_null();
            } else {
              cells[k].set_timestamp(merge_stat.start_time_);
            }
            break;
          }
          case 27: {
            // merge_finish_time
            if (0 == merge_stat.finish_time_) {
              cells[k].set_null();
            } else {
              cells[k].set_timestamp(merge_stat.finish_time_);
            }
            break;
          }
          case 28: {
            // merge_process
            int64_t merge_process = 0;
            if (i != ss_store_version_info_list.count() - 1 || 0 == i) {
              merge_process = 100;
            } else if (i > 0 && table_cnt > 0) {
              merge_process = 100 * version_info.merged_ss_store_count_ / table_cnt;
            }
            if (merge_process > 100) {
              merge_process = 100;
              if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
                FLOG_WARN("merge process is invalid", K(merge_process), K(table_cnt), K(version_info));
              }
            }
            cells[k].set_int(merge_process);
            break;
          }
          case 29: {
            // rewrite_macro_old_micro_block_count
            const int64_t rewrite_macro_old_micro_block_count = version_info.rewrite_macro_old_micro_block_count_;
            cells[k].set_int(rewrite_macro_old_micro_block_count);
            break;
          }
          case 30: {
            // rewrite_macro_total_micro_block_count
            const int64_t rewrite_macro_total_micro_block_count = version_info.rewrite_macro_total_micro_block_count_;
            cells[k].set_int(rewrite_macro_total_micro_block_count);
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(ret), K(output_column_ids_), K(col_id));
            break;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_SUCCESS != (ret = scanner_.add_row(cur_row_))) {
          SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
        }
      }
    }
  }
  return ret;
}

int ObPartitionSstableImageInfoTable::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(ERROR, "not init");
  } else {
    if (!start_to_read_) {
      if (OB_SUCCESS != (ret = fill_scaner())) {
        SERVER_LOG(WARN, "Fail to fill scaner, ", K(ret));
      } else {
        scanner_it_ = scanner_.begin();
        start_to_read_ = true;
      }
    }

    if (OB_SUCCESS == ret && start_to_read_) {
      if (OB_SUCCESS != (ret = scanner_it_.get_next_row(cur_row_))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next row", K(ret));
        }
      } else {
        row = &cur_row_;
      }
    }
  }

  return ret;
}
}  // namespace observer
}  // namespace oceanbase
