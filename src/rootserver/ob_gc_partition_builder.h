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

#ifndef OCEANBASE_GC_PARTITION_BUILDER_H_
#define OCEANBASE_GC_PARTITION_BUILDER_H_

#include "lib/thread/ob_async_task_queue.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase {
namespace rootserver {
class ObGCPartitionBuilder;

class ObBuildGCPartitionTask : public share::ObAsyncTask {
public:
  explicit ObBuildGCPartitionTask(ObGCPartitionBuilder& gc_partition_builder)
      : gc_partition_builder_(&gc_partition_builder)
  {}
  virtual ~ObBuildGCPartitionTask()
  {}
  virtual int64_t get_deep_copy_size() const;
  share::ObAsyncTask* deep_copy(char* buf, const int64_t buf_size) const;
  virtual int process();

private:
  ObGCPartitionBuilder* gc_partition_builder_;
};

class ObGCPartitionBuilder {
public:
  ObGCPartitionBuilder();
  ~ObGCPartitionBuilder()
  {}
  int init(share::schema::ObMultiVersionSchemaService& schema_service, common::ObMySQLProxy& sql_proxy);
  // step 1. call before end upgrade
  int create_tables();
  // step 2. call after end upgrade
  int build();
  int can_build();

  void start();
  int stop();

private:
  int check_stop();
  int set_build_mark();

  int build_gc_partition_info();
  int build_tenant_gc_partition_info(uint64_t tenant_id);
  int batch_insert_partitions(common::ObIArray<common::ObPartitionKey>& partitions);
  int check_tenant_gc_partition_info(uint64_t tenant_id, int64_t partition_cnt);

private:
  static const int64_t BATCH_INSERT_COUNT = 100;

private:
  bool inited_;
  bool stopped_;
  bool build_;
  common::SpinRWLock rwlock_;
  common::ObMySQLProxy* sql_proxy_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  DISALLOW_COPY_AND_ASSIGN(ObGCPartitionBuilder);
};
}  // namespace rootserver
}  // namespace oceanbase
#endif  // OCEANBASE_GC_PARTITION_BUILDER_H_
