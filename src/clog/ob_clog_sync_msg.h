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

#ifndef OCEANBASE_CLOG_OB_CLOG_SYN_MSG_
#define OCEANBASE_CLOG_OB_CLOG_SYN_MSG_
#include "ob_log_define.h"
#include "election/ob_election_priority.h"
#include "storage/transaction/ob_trans_define.h"

namespace oceanbase {
namespace clog {
static const int64_t OB_ARRAY_COUNT = 16;
struct McCtx {
  OB_UNIS_VERSION(1);

public:
  McCtx()
  {
    reset();
  }
  void reset()
  {
    partition_key_.reset();
    mc_timestamp_ = common::OB_INVALID_TIMESTAMP;
    max_confirmed_log_id_ = common::OB_INVALID_ID;
    is_normal_partition_ = true;
  }

  common::ObPartitionKey partition_key_;
  int64_t mc_timestamp_;
  uint64_t max_confirmed_log_id_;
  bool is_normal_partition_;
  TO_STRING_KV(K(partition_key_), K(mc_timestamp_), K(max_confirmed_log_id_), K(is_normal_partition_));
};
typedef common::ObSEArray<McCtx, OB_ARRAY_COUNT> McCtxArray;
}  // namespace clog

namespace obrpc {
class ObLogGetMCTsRequest {
  OB_UNIS_VERSION(1);

public:
  ObLogGetMCTsRequest()
  {
    reset();
  }
  ~ObLogGetMCTsRequest()
  {
    reset();
  }
  void reset()
  {
    partition_key_.reset();
  }
  const common::ObPartitionKey& get_partition_key() const
  {
    return partition_key_;
  }
  void set_partition_key(const common::ObPartitionKey& partition_key)
  {
    partition_key_ = partition_key;
  }
  TO_STRING_KV(K_(partition_key));

private:
  common::ObPartitionKey partition_key_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogGetMCTsRequest);
};

class ObLogGetMCTsResponse {
  OB_UNIS_VERSION(1);

public:
  ObLogGetMCTsResponse()
  {
    reset();
  }
  ~ObLogGetMCTsResponse()
  {
    reset();
  }
  void reset()
  {
    partition_key_.reset();
    membership_timestamp_ = common::OB_INVALID_TIMESTAMP;
    max_confirmed_log_id_ = common::OB_INVALID_ID;
    is_normal_partition_ = true;
  }
  const common::ObPartitionKey& get_partition_key() const
  {
    return partition_key_;
  }
  int64_t get_membership_timestamp() const
  {
    return membership_timestamp_;
  }
  uint64_t get_max_confirmed_log_id() const
  {
    return max_confirmed_log_id_;
  }
  bool get_is_normal_partition() const
  {
    return is_normal_partition_;
  }
  void set_partition_key(const common::ObPartitionKey& partition_key)
  {
    partition_key_ = partition_key;
  }
  void set_membership_timestamp(const int64_t membership_timestamp)
  {
    membership_timestamp_ = membership_timestamp;
  }
  void set_max_confirmed_log_id(const uint64_t max_confirmed_log_id)
  {
    max_confirmed_log_id_ = max_confirmed_log_id;
  }
  void set_is_normal_partition(const bool is_normal_partition)
  {
    is_normal_partition_ = is_normal_partition;
  }
  TO_STRING_KV(K_(partition_key), K_(membership_timestamp), K_(max_confirmed_log_id), K_(is_normal_partition));

private:
  common::ObPartitionKey partition_key_;
  int64_t membership_timestamp_;
  uint64_t max_confirmed_log_id_;
  // following conditions will lead to false:
  // 1. out of memory
  // 2. disk space not enougn
  bool is_normal_partition_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogGetMCTsResponse);
};

class ObLogGetPriorityArrayRequest {
  OB_UNIS_VERSION(1);

public:
  ObLogGetPriorityArrayRequest()
  {
    reset();
  }
  ~ObLogGetPriorityArrayRequest()
  {
    reset();
  }
  void reset()
  {
    partition_array_.reset();
  }
  const common::ObPartitionArray& get_partition_array() const
  {
    return partition_array_;
  }
  int set_partition_array(const common::ObPartitionIArray& partition_array)
  {
    int ret = common::OB_SUCCESS;
    ret = partition_array_.assign(partition_array);
    return ret;
  }
  TO_STRING_KV(K(partition_array_));

private:
  common::ObPartitionArray partition_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogGetPriorityArrayRequest);
};

class ObLogGetPriorityArrayResponse {
  OB_UNIS_VERSION(1);

public:
  ObLogGetPriorityArrayResponse()
  {
    reset();
  }
  ~ObLogGetPriorityArrayResponse()
  {
    reset();
  }
  void reset()
  {
    priority_array_.reset();
  }
  const election::PriorityArray& get_priority_array() const
  {
    return priority_array_;
  }
  void set_priority_array(const election::PriorityArray& priority_array)
  {
    priority_array_ = priority_array;
  }
  TO_STRING_KV(K(priority_array_));

private:
  election::PriorityArray priority_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogGetPriorityArrayResponse);
};

class ObLogGetMcCtxArrayRequest {
  OB_UNIS_VERSION(1);

public:
  ObLogGetMcCtxArrayRequest()
  {
    reset();
  }
  ~ObLogGetMcCtxArrayRequest()
  {
    reset();
  }
  void reset()
  {
    partition_array_.reset();
  }
  const common::ObPartitionArray& get_partition_array() const
  {
    return partition_array_;
  }
  void set_partition_array(const common::ObPartitionArray& partition_array)
  {
    partition_array_ = partition_array;
  }
  TO_STRING_KV(K(partition_array_));

private:
  common::ObPartitionArray partition_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogGetMcCtxArrayRequest);
};

class ObLogGetMcCtxArrayResponse {
  OB_UNIS_VERSION(1);

public:
  ObLogGetMcCtxArrayResponse()
  {
    reset();
  }
  ~ObLogGetMcCtxArrayResponse()
  {
    reset();
  }
  void reset()
  {
    mc_ctx_array_.reset();
  }
  const clog::McCtxArray& get_mc_ctx_array() const
  {
    return mc_ctx_array_;
  }
  void set_mc_ctx_array(const clog::McCtxArray& mc_ctx_array)
  {
    mc_ctx_array_ = mc_ctx_array;
  }
  TO_STRING_KV(K(mc_ctx_array_));

private:
  clog::McCtxArray mc_ctx_array_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogGetMcCtxArrayResponse);
};

class ObLogGetRemoteLogRequest {
  OB_UNIS_VERSION(1);

public:
  ObLogGetRemoteLogRequest()
  {
    reset();
  }
  ~ObLogGetRemoteLogRequest()
  {
    reset();
  }
  void reset()
  {
    partition_key_.reset();
    log_id_ = common::OB_INVALID_ID;
  }
  void set(const common::ObPartitionKey& partition_key, const uint64_t log_id)
  {
    partition_key_ = partition_key;
    log_id_ = log_id;
  }
  const common::ObPartitionKey& get_partition_key() const
  {
    return partition_key_;
  }
  uint64_t get_log_id() const
  {
    return log_id_;
  }
  TO_STRING_KV(K(partition_key_), K(log_id_));

private:
  common::ObPartitionKey partition_key_;
  uint64_t log_id_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogGetRemoteLogRequest);
};

class ObLogGetRemoteLogResponse {
  OB_UNIS_VERSION(1);

public:
  ObLogGetRemoteLogResponse()
  {
    reset();
  }
  ~ObLogGetRemoteLogResponse()
  {
    reset();
  }
  void reset()
  {
    partition_key_.reset();
    log_id_ = common::OB_INVALID_ID;
    trans_id_.reset();
    submit_timestamp_ = common::OB_INVALID_TIMESTAMP;
    ret_value_ = common::OB_ERR_UNEXPECTED;
  }
  void set(const common::ObPartitionKey& partition_key, const uint64_t log_id, const transaction::ObTransID& trans_id,
      const int64_t submit_timestamp, const int ret_value)
  {
    partition_key_ = partition_key;
    log_id_ = log_id;
    trans_id_ = trans_id;
    submit_timestamp_ = submit_timestamp;
    ret_value_ = ret_value;
  }
  const transaction::ObTransID& get_trans_id() const
  {
    return trans_id_;
  }
  int64_t get_submit_timestamp() const
  {
    return submit_timestamp_;
  }
  int get_ret_value() const
  {
    return ret_value_;
  }
  TO_STRING_KV(K(partition_key_), K(log_id_), K(trans_id_), K(submit_timestamp_), K(ret_value_));

private:
  common::ObPartitionKey partition_key_;
  uint64_t log_id_;
  transaction::ObTransID trans_id_;
  int64_t submit_timestamp_;
  int ret_value_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogGetRemoteLogResponse);
};
}  // namespace obrpc
}  // namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_CLOG_SYN_MSG_
