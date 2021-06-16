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

#ifndef OCEANBASE_TRANS_RESULT_INFO_MGR_H_
#define OCEANBASE_TRANS_RESULT_INFO_MGR_H_

#include "storage/transaction/ob_trans_define.h"
#include "lib/lock/ob_small_spin_lock.h"
#include "lib/oblog/ob_log.h"

namespace oceanbase {
namespace storage {
class ObPartitionService;
}
namespace transaction {
class ObTransResultInfoFactory;

class ObTransResultInfoLinkNode {
public:
  ObTransResultInfoLinkNode()
  {
    reset();
  }
  virtual ~ObTransResultInfoLinkNode()
  {
    reset();
  }
  virtual void reset()
  {
    next_ = NULL;
    prev_ = NULL;
  }
  int connect(ObTransResultInfoLinkNode* prev, ObTransResultInfoLinkNode* next);
  int connect(ObTransResultInfoLinkNode* next);
  int del();
  ObTransResultInfoLinkNode* get_next_node() const
  {
    return next_;
  }
  void set_next_node(ObTransResultInfoLinkNode* next)
  {
    next_ = next;
  }

protected:
  ObTransResultInfoLinkNode* next_;
  ObTransResultInfoLinkNode* prev_;
};

class ObTransResultInfo : public ObTransResultInfoLinkNode {
public:
  ObTransResultInfo()
  {
    reset();
  }
  ~ObTransResultInfo()
  {}
  void reset();
  void destroy()
  {
    reset();
  }
  int init(const int state, const int64_t commit_version, const int64_t min_log_id, const int64_t min_log_ts,
      const ObTransID& trans_id);
  int update(const int state, const int64_t commit_version, const int64_t min_log_id, const int64_t min_log_ts,
      const ObTransID& trans_id);
  bool is_valid() const;
  int64_t get_commit_version() const
  {
    return commit_version_;
  }
  uint64_t get_min_log_id() const
  {
    return min_log_id_;
  }
  int64_t get_min_log_ts() const
  {
    return min_log_ts_;
  }
  const ObTransID& get_trans_id() const
  {
    return trans_id_;
  }
  bool is_committed() const
  {
    return ObTransResultState::COMMIT == state_;
  }
  bool is_aborted() const
  {
    return ObTransResultState::ABORT == state_;
  }
  bool is_unknown() const
  {
    return ObTransResultState::UNKNOWN == state_;
  }
  bool is_found(const ObTransID& trans_id) const;
  int set_state(const int state);
  int get_state() const
  {
    return state_;
  }
  ObTransResultInfo* next() const
  {
    return (NULL == next_) ? NULL : static_cast<ObTransResultInfo*>(next_);
  }
  void set_next(ObTransResultInfo* next)
  {
    next_ = next;
  }
  TO_STRING_KV(K_(state), K_(commit_version), K_(min_log_id), K_(trans_id));

public:
  static const int64_t TOTAL_NUM = 1024;

private:
  int state_;
  int64_t commit_version_;
  uint64_t min_log_id_;
  int64_t min_log_ts_;
  ObTransID trans_id_;
};

class ObGetMinLogIdFunction {
public:
  ObGetMinLogIdFunction() : min_log_id_(UINT64_MAX), min_log_ts_(INT64_MAX)
  {}
  ~ObGetMinLogIdFunction()
  {}
  bool operator()(ObTransResultInfo* info)
  {
    if (info->get_min_log_id() < min_log_id_) {
      min_log_id_ = info->get_min_log_id();
      min_log_ts_ = info->get_min_log_ts();
    }
    return true;
  }
  uint64_t get_min_log_id() const
  {
    return min_log_id_;
  }
  int64_t get_min_log_ts() const
  {
    return min_log_ts_;
  }

private:
  uint64_t min_log_id_;
  int64_t min_log_ts_;
};

class ObITransResultInfoMgr {
public:
  ObITransResultInfoMgr()
  {}
  virtual ~ObITransResultInfoMgr()
  {}
  virtual int init(storage::ObPartitionService* partition_service, const common::ObPartitionKey& pkey) = 0;
  virtual int insert(ObTransResultInfo* info, bool& registered) = 0;
  virtual int get_state(const ObTransID& trans_id, int& state) = 0;
  virtual int get_min_log(uint64_t& min_log_id, int64_t& min_log_ts) = 0;
  virtual int try_gc_trans_result_info(const int64_t checkpoint_ts) = 0;
  virtual int update(const int state, const int64_t commit_version, const int64_t min_log_id, const int64_t min_log_ts,
      const ObTransID& trans_id) = 0;
  virtual int del(const ObTransID& trans_id) = 0;
};

struct ObTransResultInfoBucketHeader {
  ObTransResultInfoBucketHeader()
  {
    reset();
  }
  ~ObTransResultInfoBucketHeader()
  {
    destroy();
  }
  void reset()
  {
    trans_info_.reset();
    last_gc_ts_ = common::ObTimeUtility::current_time();
  }
  void destroy()
  {
    reset();
  }
  ObTransResultInfoLinkNode trans_info_;
  int64_t last_gc_ts_;
  common::SpinRWLock lock_;
} CACHE_ALIGNED;

class ObTransResultInfoMgr : public ObITransResultInfoMgr {
public:
  ObTransResultInfoMgr()
  {
    reset();
  }
  ~ObTransResultInfoMgr()
  {
    destroy();
  }
  int init(storage::ObPartitionService* partition_service, const common::ObPartitionKey& pkey);
  void reset();
  void destroy();
  int64_t get_result_info_count()
  {
    return result_info_count_;
  }
  int rdlock(const ObTransID& trans_id);
  int wrlock(const ObTransID& trans_id);
  int unlock(const ObTransID& trans_id);
  int find_unsafe(const ObTransID& trans_id, ObTransResultInfo*& result_info);
  int insert(ObTransResultInfo* info, bool& registered);
  int update(const int state, const int64_t commit_version, const int64_t min_log_id, const int64_t min_log_ts,
      const ObTransID& trans_id);
  int del(const ObTransID& trans_id);
  int get_state(const ObTransID& trans_id, int& state);
  int get_min_log(uint64_t& min_log_id, int64_t& min_log_ts);
  int try_gc_trans_result_info(const int64_t checkpoint_ts);
  template <typename Function>
  int for_each(Function& fn)
  {
    int ret = common::OB_SUCCESS;

    if (OB_UNLIKELY(!is_inited_)) {
      ret = common::OB_NOT_INIT;
      TRANS_LOG(WARN, "ObTransResultInfoMgr not init", K(ret));
    } else {
      for (int i = 0; i < TRANS_RESULT_INFO_BUCKET_COUNT; i++) {
        ObTransResultInfoBucketHeader& bucket_header = bucket_header_[i];
        common::SpinRLockGuard guard(bucket_header.lock_);
        if (!for_each_in_bucket_(i, fn)) {
          break;
        }
      }
    }

    return ret;
  }

private:
  int gc_by_checkpoint_(const int64_t checkpoint, ObTransResultInfo* info, ObTransResultInfo*& release_point);
  int release_trans_result_info_(ObTransResultInfo* info);
  int get_partition_checkpoint_version_(int64_t& checkpoint);
  int gen_bucket_index_(const ObTransID& trans_id);
  int find_item_in_bucket_(const int index, const ObTransID& trans_id, ObTransResultInfo*& result_info);
  int insert_item_in_bucket_(const int index, ObTransResultInfo* result_info);
  template <typename Function>
  bool for_each_in_bucket_(const int index, Function& fn)
  {
    bool bool_ret = true;

    ObTransResultInfoBucketHeader& bucket_header = bucket_header_[index];
    ObTransResultInfoLinkNode& dummy_node = bucket_header.trans_info_;
    ObTransResultInfoLinkNode* find = dummy_node.get_next_node();
    while (NULL != find) {
      if (!fn(static_cast<ObTransResultInfo*>(find))) {
        bool_ret = false;
        break;
      }
      find = find->get_next_node();
    }

    return bool_ret;
  }

private:
  // TODO: dynamic scale by transaction concurrency grade
  static const int64_t TRANS_RESULT_INFO_BUCKET_COUNT = 512;
  static const int64_t TRANS_RESULT_INFO_GC_INTERVAL_US = 1000 * 1000;

private:
  bool is_inited_;
  ObTransResultInfoBucketHeader bucket_header_[TRANS_RESULT_INFO_BUCKET_COUNT];
  storage::ObPartitionService* partition_service_;
  common::ObPartitionKey self_;
  int64_t result_info_count_;
};
#define TR_MGR (::oceanbase::transaction::ObTransResultInfoMgr::get_instance())

}  // namespace transaction
}  // namespace oceanbase

#endif
