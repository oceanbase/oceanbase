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

#include "lib/hash/ob_hashmap.h"
#include "lib/ob_define.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "share/ob_worker.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "lib/allocator/ob_allocator.h"
#ifndef __OB_DTL_INTERM_RESULT_MANAGER_H__
#define __OB_DTL_INTERM_RESULT_MANAGER_H__

namespace oceanbase {
namespace sql {
namespace dtl {

struct ObDTLIntermResultKey {
  int64_t channel_id_;
  int64_t time_us_;
  int64_t start_time_;
  inline uint64_t hash() const
  {
    return common::murmurhash(&channel_id_, sizeof(uint64_t), 0);
  }
  inline bool operator==(const ObDTLIntermResultKey& key) const
  {
    return channel_id_ == key.channel_id_;
  }
  TO_STRING_KV(K(channel_id_), K(time_us_), K(start_time_));
};

struct ObDTLIntermResultInfo {
  ObDTLIntermResultInfo() : is_datum_(false), row_store_(NULL), datum_store_(NULL), is_read_(false)
  {}
  ~ObDTLIntermResultInfo()
  {}
  bool is_store_valid()
  {
    return is_datum_ ? NULL != datum_store_ : NULL != row_store_;
  }

  bool is_datum_;  // data is ObNewRow or datum array (static engine) format.
  sql::ObChunkRowStore* row_store_;
  sql::ObChunkDatumStore* datum_store_;
  bool is_read_;
};

// helper macro to dispatch action to row_store_ or daum_store_
#define DTL_IR_STORE_DO(ir, act, ...) \
  ((ir).is_datum_ ? (ir).datum_store_->act(__VA_ARGS__) : (ir).row_store_->act(__VA_ARGS__))

class ObDTLIntermResultGC : public common::ObTimerTask {
  friend class ObDTLIntermResultManager;

public:
  ObDTLIntermResultGC()
      : cur_time_(0), expire_keys_(), gc_type_(NOT_INIT), dump_count_(0), interm_cnt_(0), clean_cnt_(0)
  {}
  virtual ~ObDTLIntermResultGC() = default;
  void reset();
  void operator()(common::hash::HashMapPair<ObDTLIntermResultKey, ObDTLIntermResultInfo>& entry);
  void runTimerTask();

public:
  const static int64_t REFRESH_INTERVAL = 10 * 1000L * 1000L;      // 10s interval
  const static int64_t DUMP_TIME_THRESHOLD = 10 * 1000L * 1000L;   // dump time threshold is 10s
  const static int64_t CLEAR_TIME_THRESHOLD = 10 * 1000L * 1000L;  // clean time threashold is 10s
public:
  enum ObGCType { NOT_INIT = 0, CLEAR = 1, DUMP = 2 };

private:
  DISALLOW_COPY_AND_ASSIGN(ObDTLIntermResultGC);

private:
  int64_t cur_time_;
  common::ObSEArray<ObDTLIntermResultKey, 1> expire_keys_;
  ObGCType gc_type_;
  int64_t dump_count_;
  int64_t interm_cnt_;
  int64_t clean_cnt_;
};

class ObAtomicGetIntermResultInfoCall {
public:
  ObAtomicGetIntermResultInfoCall() : result_info_()
  {}
  ~ObAtomicGetIntermResultInfoCall() = default;
  void operator()(common::hash::HashMapPair<ObDTLIntermResultKey, ObDTLIntermResultInfo>& entry);

public:
  ObDTLIntermResultInfo result_info_;
};

class ObAtomicAppendBlockCall {
public:
  ObAtomicAppendBlockCall(char* buf, int64_t size) : block_buf_(buf), size_(size), ret_(common::OB_SUCCESS)
  {}
  ~ObAtomicAppendBlockCall() = default;
  void operator()(common::hash::HashMapPair<ObDTLIntermResultKey, ObDTLIntermResultInfo>& entry);

public:
  char* block_buf_;
  int64_t size_;
  int ret_;
};

class ObDTLIntermResultManager {
  friend class ObDTLIntermResultGC;

public:
  static ObDTLIntermResultManager& getInstance();
  typedef common::hash::ObHashMap<ObDTLIntermResultKey, ObDTLIntermResultInfo> MAP;
  int get_interm_result_info(ObDTLIntermResultKey& key, ObDTLIntermResultInfo& result_info);
  int create_interm_result_info(ObMemAttr& attr, ObDTLIntermResultInfo& result_info);
  void free_interm_result_info(ObDTLIntermResultInfo& result_info);
  int erase_interm_result_info(ObDTLIntermResultKey& key);
  int insert_interm_result_info(ObDTLIntermResultKey& key, ObDTLIntermResultInfo& result_info);
  // the next two interfaces will hold read lock for bucket
  int clear_timeout_result_info(ObDTLIntermResultGC& gc);
  int dump_result_info(ObDTLIntermResultGC& gc);
  // atomic_get_interm_result_info interface
  // will hold an exclusive lock for result info
  // so that the thread doing the dump will ignore it
  int atomic_get_interm_result_info(ObDTLIntermResultKey& key, ObDTLIntermResultInfo& result_info);
  int atomic_append_block(ObDTLIntermResultKey& key, ObAtomicAppendBlockCall& call);
  int init();
  void destroy();

private:
  static const int64_t BUCKET_NUM = 500000;  // 50w
private:
  MAP map_;
  bool is_inited_;
  int64_t dir_id_;
  ObDTLIntermResultGC gc_;

private:
  ObDTLIntermResultManager();
  ~ObDTLIntermResultManager();
  DISALLOW_COPY_AND_ASSIGN(ObDTLIntermResultManager);
};

}  // namespace dtl
}  // namespace sql
}  // namespace oceanbase
#endif
