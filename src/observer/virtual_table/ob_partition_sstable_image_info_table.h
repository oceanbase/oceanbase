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

#ifndef SRC_OBSERVER_VIRTUAL_TABLE_OB_PARTITION_SSTABLE_IMAGE_INFO_TABLE_H_
#define SRC_OBSERVER_VIRTUAL_TABLE_OB_PARTITION_SSTABLE_IMAGE_INFO_TABLE_H_

#include "share/ob_define.h"
#include "share/ob_virtual_table_iterator.h"
#include "share/ob_scanner.h"
#include "common/row/ob_row.h"
#include "lib/net/ob_addr.h"
#include "storage/ob_partition_scheduler.h"
#include "share/ob_virtual_table_scanner_iterator.h"
#include "lib/utility/ob_print_utils.h"
namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace observer {
struct SSStoreVersionInfo {
  ObVersion version_;
  int64_t ss_store_count_;
  int64_t macro_block_count_;
  int64_t use_old_macro_block_count_;
  int64_t merged_ss_store_count_;
  int64_t modified_ss_store_count_;
  int64_t rewrite_macro_old_micro_block_count_;
  int64_t rewrite_macro_total_micro_block_count_;
  SSStoreVersionInfo()
      : version_(),
        ss_store_count_(0),
        macro_block_count_(0),
        use_old_macro_block_count_(0),
        merged_ss_store_count_(0),
        modified_ss_store_count_(0),
        rewrite_macro_old_micro_block_count_(0),
        rewrite_macro_total_micro_block_count_(0)
  {}
  TO_STRING_KV(K_(version), K_(ss_store_count), K_(macro_block_count), K_(use_old_macro_block_count),
      K_(merged_ss_store_count), K_(modified_ss_store_count), K_(rewrite_macro_old_micro_block_count),
      K_(rewrite_macro_total_micro_block_count));

  inline int compare(const SSStoreVersionInfo& rhs) const
  {
    int result = 0;
    if ((version_.major_ == rhs.version_.major_) && (version_.minor_ == rhs.version_.minor_)) {
      result = 0;
    } else if ((version_.major_ < rhs.version_.major_) ||
               ((version_.major_ == rhs.version_.major_) && version_.minor_ < rhs.version_.minor_)) {
      result = -1;
    } else {
      result = 1;
    }
    return result;
  }

  void reset()
  {
    version_.reset();
    ss_store_count_ = 0;
    macro_block_count_ = 0;
    use_old_macro_block_count_ = 0;
    merged_ss_store_count_ = 0;
    modified_ss_store_count_ = 0;
    rewrite_macro_old_micro_block_count_ = 0;
    rewrite_macro_total_micro_block_count_ = 0;
  }

  inline bool operator<(const SSStoreVersionInfo& rhs) const
  {
    return compare(rhs) < 0;
  }
};

class ObPartitionSstableImageInfoTable : public common::ObVirtualTableScannerIterator {
public:
  ObPartitionSstableImageInfoTable();
  virtual ~ObPartitionSstableImageInfoTable();
  int init(storage::ObPartitionService& partition_service, share::schema::ObMultiVersionSchemaService& schema_service,
      common::ObAddr& addr);
  virtual int inner_get_next_row(common::ObNewRow*& row);

private:
  int64_t find(ObVersion& version, ObArray<SSStoreVersionInfo>& ss_store_version_info_list) const;
  int get_ss_store_version_info(ObArray<SSStoreVersionInfo>& ss_store_version_info_list);
  int fill_scaner();
  int get_table_store_cnt(int64_t& table_cnt);

private:
  bool inited_;
  common::ObAddr* addr_;
  storage::ObPartitionService* partition_service_;
  share::schema::ObMultiVersionSchemaService* schema_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionSstableImageInfoTable);
};
}  // namespace observer
}  // namespace oceanbase

#endif /* SRC_OBSERVER_VIRTUAL_TABLE_OB_PARTITION_SSTABLE_IMAGE_INFO_TABLE_H_ */
