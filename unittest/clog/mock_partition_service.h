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

#ifndef OCEANBASE_UNITTEST_MOCK_PARTITION_MGR_H_
#define OCEANBASE_UNITTEST_MOCK_PARTITION_MGR_H_
#include "mock_ob_partition_log_service.h"
#include "../storage/mockcontainer/mock_ob_partition.h"
#include "../storage/mockcontainer/mock_ob_partition_service.h"

namespace oceanbase {
using namespace clog;
namespace storage {
class MockObPartition : public storage::MockObIPartition {
public:
  MockObPartition() : valid_(false)
  {}

public:
  int init(int32_t seed)
  {
    partition_key_.init(seed, seed, 1024);
    return common::OB_SUCCESS;
  }
  clog::ObIPartitionLogService* get_log_service()
  {
    return &mock_pls_;
  }
  void set_valid(bool valid)
  {
    valid_ = valid;
    return;
  }
  bool is_valid() const
  {
    return valid_;
  }
  const common::ObPartitionKey& get_partition_key() const
  {
    return partition_key_;
  }
  virtual int get_safe_publish_version(int64_t& publish_version)
  {
    UNUSED(publish_version);
    return 0;
  }

private:
  clog::MockPartitionLogService mock_pls_;
  common::ObPartitionKey partition_key_;
  bool valid_;
};

class MockObPartitionService : public storage::MockObIPartitionService {
public:
  MockObPartitionService() : list_(NULL)
  {}

public:
  struct MockPartitionNode {
    MockObPartition partition_;
    MockPartitionNode* next_;
    MockPartitionNode() : partition_(), next_(NULL)
    {}
    ~MockPartitionNode()
    {
      partition_.destroy();
      next_ = NULL;
    }
  };
  class MockObPartitionIter : public storage::ObIPartitionIterator {
  public:
    MockObPartitionIter() : mgr_(NULL), curr_node_(NULL)
    {}
    MockObPartitionIter(ObPartitionService* mgr)
        : mgr_(reinterpret_cast<MockObPartitionService*>(mgr)), curr_node_(NULL)
    {}
    virtual ~MockObPartitionIter()
    {
      mgr_ = NULL;
      curr_node_ = NULL;
    }
    int get_next(storage::ObIPartitionGroup*& partition)
    {
      int ret = OB_SUCCESS;
      MockPartitionNode* next_node = NULL;
      if (NULL == mgr_) {
        ret = OB_INVALID_ARGUMENT;
        CLOG_LOG(WARN, "mgr is NULL");
      } else if (OB_SUCCESS != (ret = mgr_->get_next(curr_node_, next_node))) {
        CLOG_LOG(WARN, "fail to get next node", "ret", ret);
      } else if (NULL == next_node) {
        ret = OB_ITER_END;
      } else {
        partition = &next_node->partition_;
        curr_node_ = next_node;
      }
      return ret;
    }

  private:
    MockObPartitionService* mgr_;
    MockPartitionNode* curr_node_;
  };

public:
  int create_partition(int32_t seed)
  // int create_partition(common::ObMemberList &list, const ObPartitionKey &key, const int64_t replica_num)
  {
    int ret = OB_SUCCESS;
    MockPartitionNode* node = NULL;
    if (NULL == (node = static_cast<oceanbase::storage::MockObPartitionService::MockPartitionNode*>(
                     ob_malloc(sizeof(MockPartitionNode), ObModIds::OB_UPS_LOG)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(WARN, "alloc MockPartitionNode fail");
    } else {
      new (node) MockPartitionNode();
      node->partition_.init(seed);
      if (NULL == list_) {
        list_ = node;
      } else {
        node->next_ = list_;
        list_ = node;
      }
    }
    return ret;
  }
  int get_next(MockPartitionNode* curr_node, MockPartitionNode*& next_node)
  {
    int ret = OB_SUCCESS;
    if (NULL == curr_node) {
      next_node = list_;
    } else {
      next_node = curr_node->next_;
    }
    return ret;
  }
  ObIPartitionIterator* alloc_scan_iter()
  {
    MockObPartitionService::MockObPartitionIter* iter = NULL;
    if (NULL == (iter = static_cast<oceanbase::storage::MockObPartitionService::MockObPartitionIter*>(
                     ob_malloc(sizeof(MockObPartitionService::MockObPartitionIter), ObModIds::OB_UPS_LOG)))) {
      CLOG_LOG(WARN, "OB_ALLOCATE_MEMORY_FAILED");
    } else {
      new (iter) MockObPartitionService::MockObPartitionIter(this);
    }
    return iter;
  }
  int revert_scan_iter(ObIPartitionIterator* iter)
  {
    ob_free(iter);
    return common::OB_SUCCESS;
  }
  int get_partition(const common::ObPartitionKey& pkey, storage::ObIPartitionGroup*& partition)
  {
    int ret = OB_ENTRY_NOT_EXIST;
    MockPartitionNode* curr_node = list_;
    while (NULL != curr_node) {
      if (curr_node->partition_.get_partition_key() == pkey) {
        ret = OB_SUCCESS;
        partition = &curr_node->partition_;
        break;
      }
      curr_node = curr_node->next_;
    }
    return ret;
  }

private:
  MockPartitionNode* list_;

  DISALLOW_COPY_AND_ASSIGN(MockObPartitionService);
};

/*class MockPartition: public ObIPartition
{
public:
  int init(int64_t seed)
  {
    seed_ = seed;
    log_cursor_.file_id_ = seed;
    log_cursor_.offset_ = static_cast<int32_t>(seed);
    partition_key_.init(seed, seed);
    append_log_count_ = 0;
    return common::OB_SUCCESS;
  }
  //virtual int get_log_cursor(ObLogCursor &cursor)
  //{
  //  cursor.deep_copy(log_cursor_);
  //  return common::OB_SUCCESS;
  //}
  virtual const common::ObPartitionKey &get_partition_key() const
  {
    return partition_key_;
  }
  virtual int append_disk_log(const ObLogEntry &log_entry, const ObLogCursor &log_cursor)
  {
    UNUSED(log_entry);
    UNUSED(log_cursor);
    int ret = common::OB_SUCCESS;
    if (append_log_count_ <= 100) {
      ret = common::OB_ERROR_OUT_OF_RANGE;
      ++append_log_count_;
    }
    return ret;
  }
  virtual int switch_log() {return common::OB_SUCCESS;}
  virtual int set_scan_disk_log_finished()
  {
    int ret = common::OB_SUCCESS;
    return ret;
  }
  virtual int switch_state()
  {
    int ret = common::OB_SUCCESS;
    if (seed_ % 3 == 1) {
      ret = common::OB_ERROR;
    }
    return ret;
  }
  virtual bool check_mc_timeout()
  {
    return true;
  }

  virtual int check_stale_member(common::ObServer &server)
  {
    UNUSED(server);
    int ret = common::OB_SUCCESS;
    if (seed_ % 3 == 0) {
      ret = common::OB_NOT_MASTER;
    }
    return ret;
  }

  virtual int remove_member(const common::ObServer &server)
  {
    UNUSED(server);
    return common::OB_SUCCESS;
  }

  virtual uint64_t get_curr_file_min_log_id()
  {
    uint64_t ret = common::OB_INVALID_ID;
    if (seed_ != 2) {
      ret = static_cast<uint64_t>(seed_);
    }
    return ret;
  }

  virtual uint64_t get_curr_file_max_log_id()
  {
    uint64_t ret = common::OB_INVALID_ID;
    if (seed_ != 2) {
      ret = static_cast<uint64_t>(seed_) * 2;
    }
    return ret;
  }

  virtual bool is_valid() const
  {
    bool bool_ret = true;
    if (4 == seed_) {
      bool_ret = false;
    }
    return bool_ret;
  }

  virtual ObIPartitionLogService *get_log_service()
  {
    return NULL;
  }

  virtual int flush_cb(const ObLogType type,
      const uint64_t log_id,
      const int64_t mc_timestamp,
      const int64_t proposal_id,
      const ObLogCursor &log_cursor)
  {
    UNUSED(type);
    UNUSED(log_id);
    UNUSED(mc_timestamp);
    UNUSED(proposal_id);
    UNUSED(log_cursor);
    return common::OB_SUCCESS;
  }

private:
  int64_t seed_;
  int64_t append_log_count_;
  ObLogCursor log_cursor_;
  common::ObPartitionKey partition_key_;
};

class MockPartitionIter: public ObIPartitionIter
{
public:
  MockPartitionIter(ObIPartitionMgr *partition_mgr): ObIPartitionIter(partition_mgr), num_(0)
  {
    partition1.init(1);
    partition2.init(2);
    partition3.init(3);
    partition4.init(4);
  }
  virtual int get_next(ObIPartitionGroup *&partition)
  {
    int ret = common::OB_SUCCESS;
    if (num_ == 0) {
      partition = &partition2;
      num_ ++;
    } else if (num_ == 1) {
      partition = &partition1;
      num_ ++;
    } else if (num_ == 2) {
      partition = &partition3;
      num_ ++;
    } else if (num_ == 3) {
      partition = &partition4;
      num_ ++;
    } else {
      ret = common::OB_ITER_END;
    }
    return ret;
  }
private:
  int num_;
  MockPartition partition1;
  MockPartition partition2;
  MockPartition partition3;
  MockPartition partition4;
};

class MockMorePartitionIter: public ObIPartitionIter
{
public:
  MockMorePartitionIter(ObIPartitionMgr *partition_mgr) : ObIPartitionIter(partition_mgr), num_(0)
  {
    for (int64_t i = 0; i < PARTITION_COUNT; ++i) {
      partitions[i].init(i);
    }
  }
  virtual int get_next(ObIPartitionGroup *&partition)
  {
    int ret = common::OB_SUCCESS;
    if (num_ < PARTITION_COUNT) {
      partition = &partitions[num_];
      num_++;
    } else {
      ret = common::OB_ITER_END;
    }
    return ret;
  }

private:
  static const int PARTITION_COUNT = 20;
  int num_;
  MockPartition partitions[PARTITION_COUNT];
};

class MockNullPartitionIter: public MockPartitionIter
{
public:
  MockNullPartitionIter(ObIPartitionMgr *partition_mgr): MockPartitionIter(partition_mgr)
  {
  }
public:
  virtual int get_next(ObIPartitionGroup **partition)
  {
    *partition = NULL;
    return common::OB_SUCCESS;
  }
};

class MockPartitionMgr: public ObIPartitionMgr
{
public:
  MockPartitionMgr()
  {
    partition1.init(1);
    partition2.init(2);
    partition3.init(3);
    partition3.init(4);
  }
public:
  virtual int get_partition(const common::ObPartitionKey &partition_key,
                            ObIPartitionGroup *&partition)
  {
    int ret = common::OB_SUCCESS;
    if (partition_key.table_id_ == 1) {
      partition = &partition1;
    } else if (partition_key.table_id_ == 2) {
      partition = &partition2;
    } else if (partition_key.table_id_ == 3) {
      partition = &partition3;
    } else if (partition_key.table_id_ == 4) {
      partition = &partition4;
    } else {
      ret = common::OB_ENTRY_NOT_EXIST;
    }
    return ret;
  }
  virtual void revert(ObIPartitionGroup *partition)
  {
    UNUSED(partition);
  }
  virtual ObIPartitionIter *alloc_scan_iter()
  {
    ObIPartitionIter *partition_iter = new MockPartitionIter(this);
    return partition_iter;
  }
  virtual int revert_scan_iter(ObIPartitionIter *iter)
  {
    delete iter;
    return common::OB_SUCCESS;
  }
private:
  MockPartition partition1;
  MockPartition partition2;
  MockPartition partition3;
  MockPartition partition4;
};

class MockMorePartitionMgr: public ObIPartitionMgr
{
public:
  MockMorePartitionMgr()
  {
  }
public:
  virtual int get_partition(const uint64_t table_id,
                            const int64_t partition_idx,
                            ObIPartitionGroup **partition)
  {
    UNUSED(table_id);
    UNUSED(partition_idx);
    UNUSED(partition);
    return common::OB_SUCCESS;
  }
  virtual void revert(const ObIPartitionGroup *partition)
  {
    UNUSED(partition);
  }
  virtual ObIPartitionIter *alloc_scan_iter()
  {
    ObIPartitionIter *partition_iter = new MockMorePartitionIter(this);
    return partition_iter;
  }
  virtual int revert_scan_iter(ObIPartitionIter *iter)
  {
    delete iter;
    return common::OB_SUCCESS;
  }
};

class MockNullPartitionMgr: public MockPartitionMgr
{
public:
  virtual ObIPartitionIter *alloc_scan_iter()
  {
    return NULL;
  }
};

class MockNull2PartitionMgr: public MockPartitionMgr
{
public:
  virtual ObIPartitionIter *alloc_scan_iter()
  {
    return new MockNullPartitionIter(this);
  }
};

class MockNull3PartitionMgr: public MockPartitionMgr
{
public:
  MockNull3PartitionMgr(): count_(0)
  {
  }
public:
  virtual ObIPartitionIter *alloc_scan_iter()
  {
    if (0 == count_) {
      ++count_;
      return new MockPartitionIter(this);
    } else {
      return NULL;
    }
  }
private:
  int64_t count_;
};

class MockNull4PartitionMgr: public MockPartitionMgr
{
public:
  MockNull4PartitionMgr(): count_(0)
  {
  }
public:
  virtual ObIPartitionIter *alloc_scan_iter()
  {
    if (0 == count_) {
      ++count_;
      return new MockPartitionIter(this);
    } else {
      return new MockNullPartitionIter(this);
    }
  }
private:
  int64_t count_;
};*/
}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_UNITTEST_MOCK_PARTITION_MGR_H_
