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

#ifndef __OB_DTL_INTERM_RESULT_MANAGER_H__
#define __OB_DTL_INTERM_RESULT_MANAGER_H__

#include "lib/hash/ob_hashmap.h"
#include "lib/ob_define.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "lib/worker.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "lib/allocator/ob_allocator.h"
#include "share/detect/ob_detectable_id.h"
#include "sql/engine/basic/ob_temp_column_store.h"
#include "sql/engine/ob_tenant_sql_memory_manager.h"
#include "src/sql/engine/ob_sql_mem_mgr_processor.h"

namespace oceanbase
{

namespace observer
{
  class ObDTLIntermResultMonitorInfoGetter;
}
namespace sql
{
namespace dtl
{
struct ObDTLIntermResultMonitorInfo
{
  ObDTLIntermResultMonitorInfo() : qc_id_(-1), dfo_id_(common::OB_INVALID_ID),
    sqc_id_(common::OB_INVALID_ID)
  { }
  ObDTLIntermResultMonitorInfo(int64_t qc_id, int64_t dfo_id, int64_t sqc_id) :
    qc_id_(qc_id), dfo_id_(dfo_id), sqc_id_(sqc_id)
  { }
  TO_STRING_KV(K_(qc_id), K_(dfo_id), K_(sqc_id));
  int64_t qc_id_;
  int64_t dfo_id_;
  int64_t sqc_id_;
};

class ObDtlLinkedBuffer;

struct ObDTLMemProfileKey {
  ObDTLMemProfileKey(uint64_t px_sequence_id, int64_t dfo_id)
    : px_sequence_id_(px_sequence_id), dfo_id_(dfo_id) {}

  ObDTLMemProfileKey()
    : px_sequence_id_(-1), dfo_id_(-1) {}

  explicit ObDTLMemProfileKey(const ObDTLMemProfileKey &key)
    : px_sequence_id_(key.px_sequence_id_), dfo_id_(key.dfo_id_) {}

  inline uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&px_sequence_id_, sizeof(uint64_t), 0);
    hash_val = common::murmurhash(&dfo_id_, sizeof(int64_t), hash_val);
    return hash_val;
  }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }

  inline bool operator==(const ObDTLMemProfileKey& key) const
  {
    return px_sequence_id_ == key.px_sequence_id_ && dfo_id_ == key.dfo_id_;
  }

  inline bool is_valid() {
    return ((px_sequence_id_ >= 0) && (dfo_id_ >= 0));
  }

  TO_STRING_KV(K(px_sequence_id_), K(dfo_id_));

  uint64_t px_sequence_id_;
  int64_t dfo_id_;
};

class ObDTLMemProfileInfo : public ObSqlMemoryCallback
{
public:
  ObDTLMemProfileInfo(const uint64_t tenant_id)
    : allocator_(tenant_id),
      profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
      sql_mem_processor_(profile_),
      ref_count_(0), row_count_(0),
      mutex_(common::ObLatchIds::SQL_MEMORY_MGR_MUTEX_LOCK) {}
  ~ObDTLMemProfileInfo() {}

  // The local channel and the rpc channel may modify the interm results concurrently,
  // and these interme results may be linked to the same profile.
  // Therefore, access to the profile needs to be protected by locks
  // to prevent concurrent modification issues.
  void alloc(int64_t size)
  {
    lib::ObMutexGuard guard(mutex_);
    sql_mem_processor_.alloc(size);
  }

  void free(int64_t size)
  {
    lib::ObMutexGuard guard(mutex_);
    sql_mem_processor_.free(size);
  }

  void dumped(int64_t size)
  {
    lib::ObMutexGuard guard(mutex_);
    sql_mem_processor_.dumped(size);
  }

  void set_number_pass(int32_t num_pass)
  {
    lib::ObMutexGuard guard(mutex_);
    sql_mem_processor_.set_number_pass(num_pass);
  }

  void update_row_count(int64_t size) {
    lib::ObMutexGuard guard(mutex_);
    row_count_ += size;
  }

  common::ObFIFOAllocator allocator_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;

  int64_t ref_count_;
  int64_t row_count_;

  static const int64_t CACHE_SIZE = 16 * 1024 * 1024; // 16M
  lib::ObMutex mutex_;

  TO_STRING_KV(K(ref_count_), K(row_count_));
};

struct ObDTLIntermResultKey
{
  ObDTLIntermResultKey() : channel_id_(0), timeout_ts_(0),
      start_time_(0), batch_id_(0) {}
  int64_t channel_id_;
  int64_t timeout_ts_;
  int64_t start_time_;
  int64_t batch_id_;
  inline uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = common::murmurhash(&channel_id_, sizeof(uint64_t), 0);
    hash_val = common::murmurhash(&batch_id_, sizeof(uint64_t), hash_val);
    return hash_val;
  }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }

  inline bool operator==(const ObDTLIntermResultKey &key) const
  {
    return channel_id_ == key.channel_id_ && batch_id_ == key.batch_id_;
  }
  TO_STRING_KV(K(channel_id_), K(batch_id_), K(timeout_ts_), K(start_time_));
};

struct ObDTLIntermResultInfo
{
  friend class ObDTLIntermResultManager;
  ObDTLIntermResultInfo()
      : datum_store_(NULL), col_store_(NULL), ret_(common::OB_SUCCESS),
      is_read_(false), is_eof_(false), ref_count_(0),
      trace_id_(), dump_time_(0), dump_cost_(0), unregister_dm_info_(),
      use_rich_format_(false), mem_profile_key_()
  {}
  ~ObDTLIntermResultInfo() {}
  bool is_store_valid() const { return use_rich_format_ ? NULL != col_store_ : NULL != datum_store_; }

  void reset() { datum_store_ = NULL; col_store_ = NULL; is_read_ = false; ret_ = common::OB_SUCCESS; }
  void set_eof(bool flag) { is_eof_ = flag; }
  void set_use_rich_format(bool use_rich_format) { use_rich_format_= use_rich_format; }
  int64_t get_ref_count() { return ATOMIC_LOAD(&ref_count_); }
  uint64_t get_tenant_id() const { return use_rich_format_ ? col_store_->get_tenant_id() : datum_store_->get_tenant_id(); }
private:
  void inc_ref_count() { ATOMIC_INC(&ref_count_); }
  int64_t dec_ref_count() { return ATOMIC_SAF(&ref_count_, 1); }
public:
  TO_STRING_KV(
    K_(ret),
    K_(is_read),
    K_(is_eof),
    K_(ref_count),
    K_(dump_cost),
    K_(monitor_info),
    K_(use_rich_format),
    K_(mem_profile_key)
  );

  sql::ObChunkDatumStore *datum_store_;
  sql::ObTempColumnStore *col_store_;
  int ret_;
  bool is_read_;
  bool is_eof_;
  int64_t ref_count_;
  common::ObCurTraceId::TraceId trace_id_;
  int64_t dump_time_;
  int64_t dump_cost_;
  common::ObUnregisterDmInfo unregister_dm_info_;
  ObDTLIntermResultMonitorInfo monitor_info_;
  bool use_rich_format_;
  ObDTLMemProfileKey mem_profile_key_;
};

struct ObDTLIntermResultInfoGuard
{
public:
  ObDTLIntermResultInfoGuard() : result_info_(NULL), interm_res_manager_(NULL) {}
  ~ObDTLIntermResultInfoGuard() {
    reset();
  }
  void set_result_info(ObDTLIntermResultInfo &result_info,
                       ObDTLIntermResultManager *interm_res_manager);
  void reset();
  ObDTLIntermResultInfo *result_info_;
private:
  ObDTLIntermResultManager *interm_res_manager_;
};

// helper macro to dispatch action to datum_store_ / col_store_
#define DTL_IR_STORE_DO(ir, act, ...) \
    ((ir).use_rich_format_ ? ((ir).col_store_->act(__VA_ARGS__)) : ((ir).datum_store_->act(__VA_ARGS__)))

#define DTL_IR_STORE_DO_APPEND_BLOCK(ir, buf, size, need_swizzling) \
    ((ir).use_rich_format_ ? ((ir).col_store_->append_block(buf, size)) :  \
    ((ir).datum_store_->append_block(buf, size, need_swizzling)))

#define DTL_IR_STORE_DO_APPEND_BLOCK_PAYLOAD(ir, payload, size, rows, need_swizzling) \
    ((ir).use_rich_format_ ? ((ir).col_store_->append_block_payload(payload, size, rows)) :  \
    ((ir).datum_store_->append_block_payload(payload, size, rows, need_swizzling)))

#define DTL_IR_STORE_DO_DUMP(ir, reuse, all_dump) \
    ((ir).use_rich_format_ ? ((ir).col_store_->dump(all_dump)) :  \
    ((ir).datum_store_->dump(reuse, all_dump)))

class ObDTLIntermResultGC
{
  friend class ObDTLIntermResultManager;
public:
  ObDTLIntermResultGC()
    : cur_time_(0), expire_keys_(),
      dump_count_(0), clean_cnt_(0) {}
  virtual ~ObDTLIntermResultGC() = default;
  int operator() (common::hash::HashMapPair<ObDTLIntermResultKey,
      ObDTLIntermResultInfo *> &entry);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDTLIntermResultGC);

public:
  const static int64_t CLEAR_TIME_THRESHOLD = 10 * 1000L * 1000L; // 超过10秒清理
private:
  int64_t cur_time_;
  common::ObSEArray<ObDTLIntermResultKey, 1> expire_keys_;
  int64_t dump_count_;
  int64_t clean_cnt_;
};

class ObDTLIntermResultCheckUpdateMem
{
public:
  explicit ObDTLIntermResultCheckUpdateMem(int64_t cur_row_count) : cur_row_count_(cur_row_count) {}
  bool operator()(int64_t max_row_count) {
    return cur_row_count_ > max_row_count;
  }
private:
  int64_t cur_row_count_;
};

class ObDTLIntermResultCheckDump
{
public:
  explicit ObDTLIntermResultCheckDump(int64_t cur_mem_size) : cur_mem_size_(cur_mem_size) {}
  bool operator()(int64_t max_memory_size) {
    return cur_mem_size_ > max_memory_size;
  }
private:
  int64_t cur_mem_size_;
};

class ObAtomicGetIntermResultInfoCall
{
public:
  explicit ObAtomicGetIntermResultInfoCall(ObDTLIntermResultInfoGuard &guard,
                                           ObDTLIntermResultManager *interm_res_manager) :
    result_info_guard_(guard), interm_res_manager_(interm_res_manager), ret_(OB_SUCCESS) {}
  ~ObAtomicGetIntermResultInfoCall() = default;
  void operator() (common::hash::HashMapPair<ObDTLIntermResultKey,
      ObDTLIntermResultInfo *> &entry);
public:
  ObDTLIntermResultInfoGuard &result_info_guard_;
  ObDTLIntermResultManager *interm_res_manager_;
  int ret_;
};

class ObAtomicAppendBlockCall
{
public:
  ObAtomicAppendBlockCall(char *buf, int64_t size, bool is_eof,
                          ObDTLIntermResultManager *interm_res_manager,
                          ObDTLMemProfileInfo *mem_profile_info)
    : block_buf_(buf), size_(size), ret_(common::OB_SUCCESS), is_eof_(is_eof),
      interm_res_manager_(interm_res_manager),
      mem_profile_info_(mem_profile_info)  {}
  ~ObAtomicAppendBlockCall() = default;
  void operator() (common::hash::HashMapPair<ObDTLIntermResultKey,
      ObDTLIntermResultInfo *> &entry);
public:
  char* block_buf_;
  int64_t start_pos_;
  int64_t size_;
  int ret_;
  bool is_eof_;
  ObDTLIntermResultManager *interm_res_manager_;
  ObDTLMemProfileInfo *mem_profile_info_;
};

class ObAtomicAppendPartBlockCall
{
public:
  ObAtomicAppendPartBlockCall(char *buf, int64_t start_pos, int64_t len,
                              int64_t rows, bool is_eof,
                              ObDTLIntermResultManager *interm_res_manager,
                              ObDTLMemProfileInfo *mem_profile_info)
    : block_buf_(buf), start_pos_(start_pos), length_(len), rows_(rows),
      ret_(common::OB_SUCCESS), is_eof_(is_eof),
      interm_res_manager_(interm_res_manager),
      mem_profile_info_(mem_profile_info) {}
  ~ObAtomicAppendPartBlockCall() = default;
  void operator() (common::hash::HashMapPair<ObDTLIntermResultKey,
      ObDTLIntermResultInfo *> &entry);
public:
  char* block_buf_;
  int64_t start_pos_;
  int64_t length_;
  int64_t rows_;
  int ret_;
  bool is_eof_;
  ObDTLIntermResultManager *interm_res_manager_;
  ObDTLMemProfileInfo *mem_profile_info_;
};

class ObDTLIntermResultGCTask : public common::ObTimerTask
{
public:
  ObDTLIntermResultGCTask() : dtl_interm_result_manager_(NULL), is_start_(false) {}
  virtual ~ObDTLIntermResultGCTask() {}
  virtual void runTimerTask() override;
  const static int64_t REFRESH_INTERVAL = 10 * 1000L * 1000L; // 10秒间隔
  ObDTLIntermResultManager *dtl_interm_result_manager_;
  bool is_start_;
};

class ObDTLIntermResultManager
{
  friend class ObDTLIntermResultGCTask;
  friend class ObAtomicAppendBlockCall;
  friend class ObAtomicAppendPartBlockCall;
  friend struct ObDTLIntermResultInfoGuard;
public:
  int process_interm_result(ObDtlLinkedBuffer *buffer, int64_t channel_id);
  int process_interm_result_inner(ObDtlLinkedBuffer &buffer,
                                  ObDTLIntermResultKey &key,
                                  int64_t start_pos,
                                  int64_t length,
                                  int64_t rows,
                                  bool is_eof,
                                  bool append_whole_block);
  int get_interm_result_info(ObDTLIntermResultKey &key, ObDTLIntermResultInfo &result_info);
  int create_interm_result_info(ObMemAttr &attr, ObDTLIntermResultInfoGuard &result_info_guard,
                                const ObDTLIntermResultMonitorInfo &monitor_info, bool use_rich_format = false);
  int erase_interm_result_info(const ObDTLIntermResultKey &key, bool need_unregister_check_item_from_dm=true);
  int insert_interm_result_info(ObDTLIntermResultKey &key, ObDTLIntermResultInfo *&result_info);
  // 以下两个接口会持有bucket读锁.
  int clear_timeout_result_info();
  // atomic_get_interm_result_info接口.
  // 将会持写锁标记result info为已读.
  // 后台的dump线程遇到已读的row_store将不会dump.
  int atomic_get_interm_result_info(ObDTLIntermResultKey &key, ObDTLIntermResultInfoGuard &guard);
  int atomic_append_block(ObDTLIntermResultKey &key, ObAtomicAppendBlockCall &call);
  int atomic_append_part_block(ObDTLIntermResultKey &key, ObAtomicAppendPartBlockCall &call);
  int init();
  static int mtl_init(ObDTLIntermResultManager* &dtl_interm_result_manager);
  void destroy();
  static void mtl_destroy(ObDTLIntermResultManager *&dtl_interm_result_manager);
  int generate_monitor_info_rows(observer::ObDTLIntermResultMonitorInfoGetter &monitor_info_getter);
  int erase_tenant_interm_result_info();
  static void free_interm_result_info_store(ObDTLIntermResultInfo *result_info);
  int free_interm_result_info(ObDTLIntermResultInfo *result_info);
  static void inc_interm_result_ref_count(ObDTLIntermResultInfo *result_info);
  int dec_interm_result_ref_count(ObDTLIntermResultInfo *&result_info);
  void runTimerTask();
  static int mtl_start(ObDTLIntermResultManager *&dtl_interm_result_manager);
  static void mtl_stop(ObDTLIntermResultManager *&dtl_interm_result_manager);
  static void mtl_wait(ObDTLIntermResultManager *&dtl_interm_result_manager);
  ObDTLIntermResultGCTask &get_gc_task() { return gc_task_; }

  ObDTLIntermResultManager();
  ~ObDTLIntermResultManager();

private:
  int access_mem_profile(const ObDTLMemProfileKey &mem_profile_key,
                         ObDTLMemProfileInfo *&mem_profile_info,
                         ObDTLIntermResultInfo &interm_res_info,
                         ObDtlLinkedBuffer &buffer);
  int init_mem_profile(const ObDTLMemProfileKey& key,
                       ObDTLMemProfileInfo *&info,
                       ObDtlLinkedBuffer &buffer);
  int destroy_mem_profile(const ObDTLMemProfileKey& key);

  void inc_mem_profile_ref_count(ObDTLMemProfileInfo *info);
  int dec_mem_profile_ref_count(const ObDTLMemProfileKey &key,
                                ObDTLMemProfileInfo *&info);
  void free_mem_profile(ObDTLMemProfileInfo *&info);

  int process_dump(ObDTLIntermResultInfo &result_info,
                   ObDTLMemProfileInfo *mem_profile_info);
  bool need_dump(ObDTLMemProfileInfo *mem_profile_info)
  { return mem_profile_info->sql_mem_processor_.get_data_size() >
           mem_profile_info->sql_mem_processor_.get_mem_bound(); }
  int clear_mem_profile_map();

private:
  // 由于此中间结果管理器是全局结构, 基于性能考虑, 减少锁冲突设置bucket_num为50w.
  static const int64_t DEFAULT_BUCKET_NUM = 500000; //50w

private:
  typedef common::hash::ObHashMap<ObDTLIntermResultKey, ObDTLIntermResultInfo *> IntermResMap;
  typedef common::hash::ObHashMap<ObDTLMemProfileKey, ObDTLMemProfileInfo *> MemProfileMap;
  typedef IntermResMap::iterator IntermResMapIter;
  typedef MemProfileMap::iterator MemProfileMapIter;
private:
  IntermResMap interm_res_map_;
  MemProfileMap mem_profile_map_;
  bool is_inited_;
  int64_t dir_id_;
  ObDTLIntermResultGC gc_;
  ObDTLIntermResultGCTask gc_task_;
  lib::ObMutex mem_profile_mutex_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDTLIntermResultManager);
};

}
}
}
#endif
