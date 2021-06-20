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

#ifndef OCEANBASE_STORAGE_OB_AUTO_PART_SCHEDULER_H_
#define OCEANBASE_STORAGE_OB_AUTO_PART_SCHEDULER_H_

#include "common/rowkey/ob_rowkey.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_thread_pool.h"

namespace oceanbase {
namespace share {
namespace schema {
class ObMultiVersionSchemaService;
class ObTableSchema;
}  // namespace schema
}  // namespace share
namespace memtable {
class ObMemtable;
}
namespace common {
class ObIAllocator;
class ObPartitionKey;
}  // namespace common
namespace storage {
class ObPartitionService;
class ObIPartitionGroup;
class ObSSTable;
class ObAutoPartScheduler : public share::ObThreadPool {
public:
  ObAutoPartScheduler();
  virtual ~ObAutoPartScheduler();

public:
  int init(storage::ObPartitionService* partition_service, share::schema::ObMultiVersionSchemaService* schema_service);
  int start();
  void stop();
  void wait();
  void destroy();

public:
  void run1();

private:
  typedef common::ObSEArray<common::ObRowkey, 1024> RowkeyArray;

private:
  void do_work_();
  // execute partition slipt including condition check, notifying rs execute split
  void handle_partition_(storage::ObIPartitionGroup* partition);
  // Check if need execute auto split
  // 1) is_auto_part
  // 2) is_leader
  // 3) the size of sstable/memtable has exceed auto_part_size
  // 4) not is_splitting
  bool need_auto_split_(storage::ObIPartitionGroup* partition);
  // check if the table is auto split table
  bool check_partition_is_auto_part_(
      storage::ObIPartitionGroup* partition, const share::schema::ObTableSchema* table_schema);
  // check leader
  bool check_partition_is_leader_(storage::ObIPartitionGroup* partition);
  // check the size of the partition
  bool check_partition_is_enough_large_(
      storage::ObIPartitionGroup* partition, const share::schema::ObTableSchema* table_schema);
  // execute the split action
  void execute_range_part_split_(storage::ObIPartitionGroup* partition);
  // get split rowkey
  int get_split_rowkey_(
      storage::ObIPartitionGroup* partition, common::ObRowkey& rowkey, common::ObIAllocator& allocator);
  int build_rowkey_array_(const common::ObPartitionKey& partition_key, storage::ObSSTable* sstable,
      RowkeyArray& rowkey_array, common::ObIAllocator& allocator);
  int build_rowkey_array_(const common::ObPartitionKey& partition_key, memtable::ObMemtable* memtable,
      RowkeyArray& rowkey_array, common::ObIAllocator& allocator);

  const static int64_t DO_WORK_INTERVAL = 10 * 1000 * 1000;  // 10s
private:
  bool is_inited_;
  storage::ObPartitionService* partition_service_;
  share::schema::ObMultiVersionSchemaService* schema_service_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAutoPartScheduler);
};
}  // namespace storage
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_AUTO_PART_SCHEDULER_H_
