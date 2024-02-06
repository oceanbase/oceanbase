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
struct ObDTLIntermResultKey
{
  ObDTLIntermResultKey() : channel_id_(0), time_us_(0),
      start_time_(0), batch_id_(0) {}
  int64_t channel_id_;
  int64_t time_us_;
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

  inline bool operator==(const ObDTLIntermResultKey& key) const
  {
    return channel_id_ == key.channel_id_ && batch_id_ == key.batch_id_;
  }
  TO_STRING_KV(K(channel_id_), K(batch_id_), K(time_us_), K(start_time_));
};

struct ObDTLIntermResultInfo
{
  friend class ObDTLIntermResultManager;
  ObDTLIntermResultInfo()
      : datum_store_(NULL), ret_(common::OB_SUCCESS),
      is_read_(false), is_eof_(false), ref_count_(0),
      trace_id_(), dump_time_(0), dump_cost_(0), unregister_dm_info_()
  {}
  ~ObDTLIntermResultInfo() {}
  bool is_store_valid() const { return NULL != datum_store_; }

  void reset() { datum_store_ = NULL; is_read_ = false; ret_ = common::OB_SUCCESS; }
  void set_eof(bool flag) { is_eof_ = flag; }
  int64_t get_ref_count() { return ATOMIC_LOAD(&ref_count_); }
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
    K_(monitor_info)
  );

  sql::ObChunkDatumStore *datum_store_;
  int ret_;
  bool is_read_;
  bool is_eof_;
  int64_t ref_count_;
  common::ObCurTraceId::TraceId trace_id_;
  int64_t dump_time_;
  int64_t dump_cost_;
  common::ObUnregisterDmInfo unregister_dm_info_;
  ObDTLIntermResultMonitorInfo monitor_info_;
  uint64_t tenant_id_;
};

struct ObDTLIntermResultInfoGuard
{
  ObDTLIntermResultInfoGuard() : result_info_(NULL) {}
  ~ObDTLIntermResultInfoGuard() {
    reset();
  }
  void set_result_info(ObDTLIntermResultInfo &result_info);
  void reset();
  ObDTLIntermResultInfo *result_info_;
};

// helper macro to dispatch action to daum_store_
#define DTL_IR_STORE_DO(ir, act, ...) \
    ((ir).datum_store_->act(__VA_ARGS__))

class ObDTLIntermResultGC
{
  friend class ObDTLIntermResultManager;
public:
  ObDTLIntermResultGC()
    : cur_time_(0), expire_keys_(), gc_type_(NOT_INIT), dump_count_(0),
      interm_cnt_(0), clean_cnt_(0)
  {}
  virtual ~ObDTLIntermResultGC() = default;
  void reset();
  int operator() (common::hash::HashMapPair<ObDTLIntermResultKey,
      ObDTLIntermResultInfo *> &entry);
public:
  const static int64_t DUMP_TIME_THRESHOLD = 10 * 1000L * 1000L; // 超过10秒dump
  const static int64_t CLEAR_TIME_THRESHOLD = 10 * 1000L * 1000L; // 超过10秒清理
public:
  enum ObGCType {
    NOT_INIT = 0,
    CLEAR = 1,
    DUMP = 2
  };
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

class ObAtomicGetIntermResultInfoCall
{
public:
  ObAtomicGetIntermResultInfoCall(ObDTLIntermResultInfoGuard &guard) :
    result_info_guard_(guard), ret_(OB_SUCCESS) {}
  ~ObAtomicGetIntermResultInfoCall() = default;
  void operator() (common::hash::HashMapPair<ObDTLIntermResultKey,
      ObDTLIntermResultInfo *> &entry);
public:
  ObDTLIntermResultInfoGuard &result_info_guard_;
  int ret_;
};

class ObAtomicAppendBlockCall
{
public:
  ObAtomicAppendBlockCall(char *buf, int64_t size, bool is_eof) : block_buf_(buf),
      size_(size), ret_(common::OB_SUCCESS), is_eof_(is_eof) {}
  ~ObAtomicAppendBlockCall() = default;
  void operator() (common::hash::HashMapPair<ObDTLIntermResultKey,
      ObDTLIntermResultInfo *> &entry);
public:
  char* block_buf_;
  int64_t start_pos_;
  int64_t size_;
  int ret_;
  bool is_eof_;
};

class ObAtomicAppendPartBlockCall
{
public:
  ObAtomicAppendPartBlockCall(char *buf, int64_t start_pos, int64_t len, int64_t rows, bool is_eof)
    : block_buf_(buf), start_pos_(start_pos), length_(len), rows_(rows),
      ret_(common::OB_SUCCESS), is_eof_(is_eof) {}
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
};

class ObDTLIntermResultGCTask : public common::ObTimerTask
{
public:
  ObDTLIntermResultGCTask() : dtl_interm_result_manager_(NULL) {}
  virtual ~ObDTLIntermResultGCTask() {}
  virtual void runTimerTask() override;
  const static int64_t REFRESH_INTERVAL = 10 * 1000L * 1000L; // 10秒间隔
  ObDTLIntermResultManager *dtl_interm_result_manager_;
};

class ObDTLIntermResultManager
{
  friend class ObDTLIntermResultGCTask;
public:
  int process_interm_result(ObDtlLinkedBuffer *buffer, int64_t channel_id);
  int process_interm_result_inner(ObDtlLinkedBuffer &buffer,
                                  ObDTLIntermResultKey &key,
                                  int64_t start_pos,
                                  int64_t length,
                                  int64_t rows,
                                  bool is_eof,
                                  bool append_whole_block);
  typedef common::hash::ObHashMap<ObDTLIntermResultKey, ObDTLIntermResultInfo *> MAP;
  int get_interm_result_info(ObDTLIntermResultKey &key, ObDTLIntermResultInfo &result_info);
  int create_interm_result_info(ObMemAttr &attr, ObDTLIntermResultInfoGuard &result_info_guard,
                                const ObDTLIntermResultMonitorInfo &monitor_info);
  int erase_interm_result_info(ObDTLIntermResultKey &key, bool need_unregister_check_item_from_dm=true);
  int insert_interm_result_info(ObDTLIntermResultKey &key, ObDTLIntermResultInfo *&result_info);
  // 以下两个接口会持有bucket读锁.
  int clear_timeout_result_info(ObDTLIntermResultGC &gc);
  int dump_result_info(ObDTLIntermResultGC &gc);
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
  static void free_interm_result_info(ObDTLIntermResultInfo *result_info);
  static void inc_interm_result_ref_count(ObDTLIntermResultInfo *result_info);
  static void dec_interm_result_ref_count(ObDTLIntermResultInfo *&result_info);
  void runTimerTask();
  static int mtl_start(ObDTLIntermResultManager *&dtl_interm_result_manager);
  static void mtl_stop(ObDTLIntermResultManager *&dtl_interm_result_manager);
  static void mtl_wait(ObDTLIntermResultManager *&dtl_interm_result_manager);
  ObDTLIntermResultGCTask &get_gc_task() { return gc_task_; }

  ObDTLIntermResultManager();
  ~ObDTLIntermResultManager();
private:
  // 由于此中间结果管理器是全局结构, 基于性能考虑, 减少锁冲突设置bucket_num为50w.
  static const int64_t DEFAULT_BUCKET_NUM = 500000; //50w
  static const int64_t MAX_TENANT_MEM_LIMIT = 17179869184; //16G
private:
  MAP map_;
  bool is_inited_;
  int64_t dir_id_;
  ObDTLIntermResultGC gc_;
  ObDTLIntermResultGCTask gc_task_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDTLIntermResultManager);
};

}
}
}
#endif
