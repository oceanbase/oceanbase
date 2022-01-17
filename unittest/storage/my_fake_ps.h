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

#ifndef MY_FAKE_PS_
#define MY_FAKE_PS_

#include "lib/utility/ob_print_utils.h"

#include "mockcontainer/mock_ob_partition_service.h"
#include "mockcontainer/mock_ob_partition.h"
#include "mockcontainer/mock_ob_partition_storage.h"
#include "storage/ob_partition_log.h"
#include "storage/ob_partition_component_factory.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase {

namespace transaction {
class MockObTransService;
}

namespace unittest {
class MySchemaService;
}
using namespace unittest;
namespace storage {

class ObIPartitionStorage;
class ObBaseStorage;

class MyFakePartition : public storage::MockObIPartition, public blocksstable::ObIBaseStorageLogEntry {
public:
  MyFakePartition() : pmeta_(), smeta_(), arena_(ObModIds::OB_PARTITION_SERVICE), storage_(NULL), unused(0)
  {}
  virtual ~MyFakePartition()
  {}
  int set(const blocksstable::ObPartitionMeta& meta);
  int add_macro_block(const int64_t block_index);
  // write ssstore objects @version tree to data file , used by write_check_point
  virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const;
  // read ssstore objects from data file to construct partition storage's version tree.
  virtual int deserialize(const char* buf, const int64_t buf_len, int64_t& pos);
  virtual int64_t get_serialize_size() const;
  virtual ObIPartitionStorage* get_storage()
  {
    return storage_;
  }
  virtual void set_storage(ObIPartitionStorage* storage)
  {
    storage_ = storage;
  }
  bool is_valid() const
  {
    return true;
  }
  TO_STRING_EMPTY();

public:
  blocksstable::ObPartitionMeta pmeta_;
  blocksstable::ObSSTableMeta smeta_;
  ObArenaAllocator arena_;
  ObIPartitionStorage* storage_;
  int64_t unused;
  TO_STRING_KV(K(unused));
};

class MyFakePartitionService : public MockObIPartitionService {
public:
  MyFakePartitionService()
      : partition_list_(),
        cp_fty_(NULL),
        arena_(ObModIds::OB_PARTITION_SERVICE),
        base_storage_(NULL),
        service_(NULL),
        trans_(NULL)
  {
    // init();
  }
  virtual ~MyFakePartitionService()
  {}
  ObPartitionComponentFactory* get_component_factory() const
  {
    return cp_fty_;
  }
  int init();
  int destroy();
  int add_partition(storage::ObIPartition* partition);
  MyFakePartition* get_partition(ObPartitionKey pkey);
  MyFakePartition* create_partition(const blocksstable::ObPartitionMeta& meta);
  virtual int get_all_partitions(common::ObIArray<ObIPartitionGroup*>& partition_list);
  virtual int get_all_partitions(ObIPartitionArrayGuard& partitions);
  virtual int load_partition(const char* buf, const int64_t buf_len, int64_t& pos);
  virtual int replay_base_storage_log(
      const int64_t log_seq_num, const int64_t subcmd, const char* buf, const int64_t len, int64_t& pos);

  virtual void set_misc(
      ObBaseStorage* base_storage, MySchemaService* schema_service, transaction::MockObTransService* trans)
  {
    base_storage_ = base_storage;
    service_ = schema_service;
    trans_ = trans;
  }

private:
  ObSEArray<ObIPartitionGroup*, 4096> partition_list_;
  ObPartitionComponentFactory* cp_fty_;
  ObArenaAllocator arena_;
  ObBaseStorage* base_storage_;
  unittest::MySchemaService* service_;
  transaction::MockObTransService* trans_;

  hash::ObHashMap<ObPartitionKey, MyFakePartition*> pmap;
};

}  // namespace storage
}  // namespace oceanbase

#endif
