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

#ifndef OCEANBASE_CLOG_OB_LOG_INFO_BLOCK_
#define OCEANBASE_CLOG_OB_LOG_INFO_BLOCK_

#include <stdint.h>
#include "lib/hash/ob_array_hash_map.h"
#include "lib/container/ob_se_array.h"  // ObSEArray
#include "common/ob_range.h"
#include "ob_log_define.h"

namespace oceanbase {
namespace common {
class ObPartitionKey;
}
namespace storage {
class ObPartitionService;
class ObIPartitionGroup;
}  // namespace storage
namespace clog {
static const int16_t CLOG_INFO_BLOCK_VERSION = 1;
static const int16_t INDEX_LOG_INFO_BLOCK_VERSION = 1;

class ObIPartitionLogService;

class NeedFreezePartition {
public:
  NeedFreezePartition()
  {
    reset();
  }
  ~NeedFreezePartition()
  {}

public:
  void reset()
  {
    partition_key_.reset();
    max_log_id_ = common::OB_INVALID_ID;
  }
  bool is_valid() const
  {
    return partition_key_.is_valid() && (max_log_id_ > 0 && common::OB_INVALID_ID != max_log_id_);
  }
  common::ObPartitionKey get_partition_key() const
  {
    return partition_key_;
  }
  uint64_t get_log_id() const
  {
    return max_log_id_;
  }
  void set_partition_key(const common::ObPartitionKey& partition_key)
  {
    partition_key_ = partition_key;
  }
  void set_log_id(const uint64_t log_id)
  {
    max_log_id_ = log_id;
  }
  TO_STRING_KV(K(partition_key_), K(max_log_id_));

private:
  common::ObPartitionKey partition_key_;
  uint64_t max_log_id_;
};

typedef common::ObSEArray<NeedFreezePartition, 16> NeedFreezePartitionArray;

class ObIInfoBlockHandler {
public:
  ObIInfoBlockHandler();
  virtual ~ObIInfoBlockHandler()
  {}

public:
  static int get_min_submit_timestamp(storage::ObPartitionService* partition_service, int64_t& min_submit_timestamp);

public:
  virtual int build_info_block(char* buf, const int64_t buf_len, int64_t& pos) = 0;
  virtual int resolve_info_block(const char* buf, const int64_t buf_len, int64_t& pos) = 0;
  virtual int update_info(const int64_t max_submit_timestamp) = 0;
  virtual int64_t get_entry_cnt() const = 0;
  // just for test
  int get_max_log_id(const common::ObPartitionKey& partition_key, uint64_t& max_log_id);

  int can_skip_based_on_submit_timestamp(const int64_t min_submit_timestamp, bool& can_skip);

  int can_skip_based_on_log_id(storage::ObPartitionService* partition_service,
      NeedFreezePartitionArray& partition_array, const bool need_record, bool& can_skip);

  int can_skip_based_on_log_id(storage::ObPartitionService* partition_service, bool& can_skip);

  int get_max_submit_timestamp(int64_t& max_submit_timestamp) const;

public:
  // echo info_block_handler_max_submit_timestamp|sha1sum
  static const int64_t MAGIC_NUMBER_FOR_MAX_SUBMIT_TIMESTAMP = 0xfa06d5f2b8a1cce1ull;

protected:
  struct InfoEntryV2 {
    uint64_t max_log_id_;
    void reset()
    {
      max_log_id_ = 0;
    }
    TO_STRING_KV("max_log_id", max_log_id_);
  };
  typedef common::ObArrayHashMap<common::ObPartitionKey, InfoEntryV2> InfoHashV2;
  class InfoEntrySerializeFunctorV2 {
  public:
    InfoEntrySerializeFunctorV2(char* buf, const int64_t buf_len, int64_t& pos);
    bool operator()(const common::ObPartitionKey& partition_key, const InfoEntryV2& info_entry_v2);

  private:
    char* buf_;
    int64_t buf_len_;
    int64_t& pos_;
  };

  common::ObVersion freeze_version_;
  bool have_info_hash_v2_;
  InfoHashV2 info_hash_v2_;

  // quick commit transaction, used to determine the log can be reclaimed based
  // on commit_version, default value is OB_INVALID_TIMESTAMP.
  int64_t max_submit_timestamp_;
  int build_max_submit_timestamp_(char* buf, const int64_t buf_len, int64_t& pos);
  int resolve_max_submit_timestamp_(const char* buf, const int64_t buf_len, int64_t& pos);

  int update_info_hash_v2_(const common::ObPartitionKey& partition_key, const uint64_t log_id);
  int resolve_info_hash_v2_(const char* buf, const int64_t buf_len, int64_t& pos, const int64_t expected_magic_number);

private:
  static int record_need_freeze_partition(
      NeedFreezePartitionArray& partition_array, const common::ObPartitionKey& partition_key, const uint64_t log_id);

private:
  // adapt hashmap forech, used to traverse infoblock.
  // 1. during the restart phase, log_scan_runnable will compare max_log_id which
  // is record in infoblock with last_replay_log_id which is record in base_storage_info,
  // if last_replay_log_id is greator than max_log_id, means that the file needn't
  // to read.
  class CheckFileCanBeSkippedFunctor {
  public:
    CheckFileCanBeSkippedFunctor(storage::ObPartitionService* partition_service);
    bool operator()(const common::ObPartitionKey& pkey, const InfoEntryV2& entry);
    bool can_skip() const
    {
      return can_skip_;
    }
    int get_ret_code() const
    {
      return ret_code_;
    }

  private:
    storage::ObPartitionService* partition_service_;
    int64_t check_partition_not_exist_time_;
    bool can_skip_;
    int ret_code_;
  };

  // adapt hashmap forech, used to traverse infoblock.
  // 1. record partition which need to be minor freeze.
  class CheckPartitionNeedFreezeFunctor {
  public:
    CheckPartitionNeedFreezeFunctor(storage::ObPartitionService* partition_service,
        NeedFreezePartitionArray& partition_array, const bool need_record, const bool is_strict_recycle_mode);
    bool operator()(const common::ObPartitionKey& partition_key, const InfoEntryV2& entry_v2);
    bool can_skip() const
    {
      return can_skip_;
    }
    int get_ret_code() const
    {
      return ret_code_;
    }

  private:
    // check partition whether need to do minor freeze.
    // last_replay_log_id means that the max_log_id has beed replayed.
    // max_log_id means that the max_log_id in this file.
    int do_check_partition_need_freeze_(storage::ObIPartitionGroup* partition, ObIPartitionLogService* pls,
        const common::ObPartitionKey& partition_key, const common::ObReplicaType replica_type,
        const int64_t last_replay_log_id, const int64_t max_log_id);
    int do_check_full_partition_need_freeze_(storage::ObIPartitionGroup* partition, ObIPartitionLogService* pls,
        const common::ObPartitionKey& partition_key, const int64_t last_replay_log_id, const int64_t max_log_id);
    int do_check_log_only_partition_need_freeze_(ObIPartitionLogService* pls,
        const common::ObPartitionKey& partition_key, const int64_t last_replay_log_id, const int64_t max_log_id);

  private:
    storage::ObPartitionService* partition_service_;
    NeedFreezePartitionArray& partition_array_;
    int64_t check_partition_not_exist_time_;
    int ret_code_;
    const bool need_record_;
    const bool is_strict_recycle_mode_;
    bool can_skip_;
    bool is_log_print_;
  };
};

// the following two classes called by LogWriter, don't need to protect
// by lock.
class ObCommitInfoBlockHandler : public ObIInfoBlockHandler {
public:
  ObCommitInfoBlockHandler();
  virtual ~ObCommitInfoBlockHandler()
  {
    destroy();
  }

public:
  int init();
  void destroy();
  virtual int build_info_block(char* buf, const int64_t buf_len, int64_t& pos);
  virtual int resolve_info_block(const char* buf, const int64_t buf_len, int64_t& pos);
  virtual int update_info(const int64_t max_submit_timestamp);
  virtual int64_t get_entry_cnt() const
  {
    return info_hash_v2_.size();
  }

  int update_info(const common::ObPartitionKey& partition_key, const uint64_t log_id, const int64_t submit_timestamp);
  int reset();

public:
  // echo commit_info_block_handler|sha1sum
  // the first 16 bits is valid
  static const int64_t MAGIC_NUMBER = 0x13bd710f4b907562ull;

private:
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObCommitInfoBlockHandler);
};

// ilog Info Block Entry
struct IndexInfoBlockEntry {
  uint64_t min_log_id_;
  uint64_t max_log_id_;
  int64_t min_log_timestamp_;
  int64_t max_log_timestamp_;
  offset_t start_offset_;

  IndexInfoBlockEntry()
  {
    reset();
  }

  void reset()
  {
    min_log_id_ = common::OB_INVALID_ID;
    max_log_id_ = common::OB_INVALID_ID;
    min_log_timestamp_ = common::OB_INVALID_TIMESTAMP;
    max_log_timestamp_ = common::OB_INVALID_TIMESTAMP;
    start_offset_ = OB_INVALID_OFFSET;
  }

  // to be compatible with older versions, only require min_log_id
  // and min_log_timestamp valid.
  bool is_valid() const
  {
    return common::OB_INVALID_ID != min_log_id_ && common::OB_INVALID_TIMESTAMP != min_log_timestamp_;
  }

  TO_STRING_KV(K_(min_log_id), K_(max_log_id), K_(min_log_timestamp), K_(max_log_timestamp), K(start_offset_));
};

typedef common::ObArrayHashMap<common::ObPartitionKey, IndexInfoBlockEntry> IndexInfoBlockMap;

class ObIndexInfoBlockHandler : public ObIInfoBlockHandler {
public:
  ObIndexInfoBlockHandler();
  virtual ~ObIndexInfoBlockHandler()
  {
    destroy();
  }

private:
  struct InfoEntry {
    uint64_t min_log_id_;
    int64_t min_log_timestamp_;

    // NOTE:don't need to record max_log_id, ObIInfoBlockHandler has
    // max_log_id.
    int64_t max_log_timestamp_;

    void reset()
    {
      min_log_id_ = common::OB_INVALID_ID;
      min_log_timestamp_ = common::OB_INVALID_TIMESTAMP;
      max_log_timestamp_ = common::OB_INVALID_TIMESTAMP;
    }
    TO_STRING_KV(K_(min_log_id), K_(min_log_timestamp), K_(max_log_timestamp));
  };
  typedef common::ObArrayHashMap<common::ObPartitionKey, InfoEntry> InfoHash;
  typedef common::ObSEArray<int64_t, 128> TstampArray;

  class InfoEntrySerializeFunctor {
  public:
    InfoEntrySerializeFunctor(char* buf, const int64_t buf_len, int64_t& pos, TstampArray& max_log_tstamp_array);
    bool operator()(const common::ObPartitionKey& partition_key, const InfoEntry& info_entry);

  private:
    char* buf_;
    int64_t buf_len_;
    int64_t& pos_;

    // record all max_log_timestamp during serialization
    TstampArray& max_log_tstamp_array_;
  };

  // InfoEntry loader, inclued:
  // 1. min_log_id
  // 2. min_log_timestamp
  // 3. max_log_timestamp
  //
  // used to load each entry into map
  class InfoEntryLoader {
  public:
    InfoEntryLoader(IndexInfoBlockMap& map) : map_(map)
    {}
    bool operator()(const common::ObPartitionKey& partition_key, const InfoEntry& info_entry);

  private:
    IndexInfoBlockMap& map_;
  };

  // InfoEntryV2 loader, load max_log_id
  //
  // used to update entry in map
  class InfoEntryV2Loader {
  public:
    InfoEntryV2Loader(IndexInfoBlockMap& map) : map_(map)
    {}
    bool operator()(const common::ObPartitionKey& partition_key, const InfoEntryV2& info_entry);

  private:
    IndexInfoBlockMap& map_;
  };

public:
  typedef common::ObArrayHashMap<common::ObPartitionKey, uint64_t> MinLogIdInfo;
  class AppendMinLogIdInfoFunctor {
  public:
    AppendMinLogIdInfoFunctor(MinLogIdInfo& min_log_id_info);
    bool operator()(const common::ObPartitionKey& partition_key, const InfoEntry& info_entry);

  private:
    MinLogIdInfo& min_log_id_info_;
  };

public:
  int init(void);
  void destroy();
  virtual int build_info_block(char* buf, const int64_t buf_len, int64_t& pos);
  virtual int resolve_info_block(const char* buf, const int64_t buf_len, int64_t& pos);
  virtual int update_info(const int64_t max_submit_timestamp);
  virtual int64_t get_entry_cnt() const
  {
    return info_hash_.size();
  }

  int update_info(const common::ObPartitionKey& partition_key, const uint64_t log_id, const int64_t submit_timestamp);
  int get_min_log_id(const common::ObPartitionKey& partition_key, uint64_t& min_log_id);
  int get_min_log_id(const common::ObPartitionKey& partition_key, uint64_t& min_log_id, int64_t& submit_timestamp);
  int get_all_min_log_id_info(MinLogIdInfo& min_log_id_info);

  // get infoblock for all partition
  int get_all_entry(IndexInfoBlockMap& index_info_block_map);

  int reset();

private:
  int resolve_max_log_timestamp_(const char* buf, const int64_t buf_len, int64_t& pos,
      const common::ObPartitionKey* part_array, const int64_t part_number);

private:
  // echo index_info_block_handler|sha1sum
  // the first 16 bits is valid
  static const int64_t MAGIC_NUMBER = 0x20a10b0830216399ull;

  // record magic number for max_log_time_stamp
  // generation:
  // 1. echo index_info_block_handler_max_log_timestamp | sha1sum
  // 2. the first 16 bits is valid
  static const int64_t MAGIC_NUMBER_FOR_MAX_LOG_TIMESTAMP = 0x0b10e5bc8e3f82e8ll;

  bool is_inited_;
  InfoHash info_hash_;

  DISALLOW_COPY_AND_ASSIGN(ObIndexInfoBlockHandler);
};

}  // namespace clog
}  // namespace oceanbase
#endif  // OCEANBASE_CLOG_OB_LOG_INFO_BLOCK_
