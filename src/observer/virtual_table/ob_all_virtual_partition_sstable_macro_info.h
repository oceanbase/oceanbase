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

#ifndef OB_ALL_VIRTUAL_PARTITION_SSTABLE_MACRO_INFO_H_
#define OB_ALL_VIRTUAL_PARTITION_SSTABLE_MACRO_INFO_H_
#include "share/ob_virtual_table_scanner_iterator.h"
#include "storage/ob_sstable.h"
#include "storage/ob_i_partition_group.h"
#include "storage/ob_pg_partition.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
class ObPGPartitionIterator;
}  // namespace storage
namespace observer {

class ObAllVirtualPartitionSSTableMacroInfo : public common::ObVirtualTableScannerIterator {
public:
  ObAllVirtualPartitionSSTableMacroInfo();
  virtual ~ObAllVirtualPartitionSSTableMacroInfo();
  int init(storage::ObPartitionService& partition_service);
  virtual int inner_get_next_row(common::ObNewRow*& row);
  virtual void reset();

private:
  int set_key_ranges(const common::ObIArray<common::ObNewRange>& key_ranges);
  int gen_row(common::ObNewRow*& row);
  int get_next_sstable();
  int check_need_ignore(bool& need_ignore);
  int gen_sstable_range(common::ObNewRange& range);

  storage::ObPartitionService* partition_service_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  char range_buf_[common::OB_MAX_RANGE_LENGTH + 1];  // extra byte for '\0'
  storage::ObPGPartitionIterator* partition_iter_;
  storage::ObPGPartition* curr_partition_;
  storage::ObSSTable* curr_sstable_;
  storage::ObTablesHandle tables_handler_;
  int64_t sstable_cursor_;
  storage::ObMacroBlockIterator macro_iter_;
  common::ObSEArray<storage::ObSSTable*, common::OB_MAX_SSTABLE_PER_TABLE> sstables_;
  storage::ObMacroBlockDesc macro_desc_;
  common::ObObj objs_[common::OB_MAX_ROWKEY_COLUMN_NUMBER];
  bool is_inited_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualPartitionSSTableMacroInfo);
};

} /* namespace observer */
} /* namespace oceanbase */

#endif /* OB_ALL_VIRTUAL_PARTITION_SSTABLE_MACRO_INFO_H_ */
