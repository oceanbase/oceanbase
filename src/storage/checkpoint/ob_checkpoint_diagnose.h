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

#ifndef OCEANBASE_STORAGE_OB_CHECKPOINT_DIAGNOSE_H_
#define OCEANBASE_STORAGE_OB_CHECKPOINT_DIAGNOSE_H_

#include "ob_common_checkpoint.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_errno.h"
#include "share/ob_define.h"

#define ADD_CHECKPOINT_DIAGNOSE_INFO_AND_SET_TRACE_ID(T, trace_id) \
  checkpoint::ObCheckpointDiagnoseMgr *cdm = MTL(checkpoint::ObCheckpointDiagnoseMgr*); \
  if (OB_NOT_NULL(cdm)) { \
    if (checkpoint::INVALID_TRACE_ID != trace_id) { \
      trace_id_ = trace_id; \
      checkpoint::ObCheckpointDiagnoseParam param(trace_id, get_tablet_id(), (void*)this); \
      cdm->add_diagnose_info<T>(param); \
    } \
  }

#define REPORT_CHECKPOINT_DIAGNOSE_INFO(func, unit_ptr, arg...) \
  checkpoint::ObCheckpointDiagnoseMgr *cdm = MTL(checkpoint::ObCheckpointDiagnoseMgr*); \
  if (OB_NOT_NULL(cdm)) { \
    if (checkpoint::INVALID_TRACE_ID != unit_ptr->get_trace_id()) { \
      checkpoint::ObCheckpointDiagnoseParam param(unit_ptr->get_trace_id(), unit_ptr->get_tablet_id(), unit_ptr); \
      cdm->func(param, ##arg); \
    } \
  }

namespace oceanbase
{
namespace storage
{
namespace checkpoint
{

// +-------------------------+-------------------------+-------------------------
// | tenant 1                | tenant 2                | tenant 3               |
// | ObCheckpointDiagnoseMgr | ObCheckpointDiagnoseMgr | ObCheckpointDiagnoseMgr|
// +-------------------------+------------+------------+-------------------------
//                                        |
//                                        V
//                   +-------------+-------------+-------------+
//                   | ObTraceInfo | ObTraceInfo | ObTraceInfo |
//                   +-------------+------+------+-------------+
//                                        +-----------------------------------------------------------+
//                                        |                                                           |
//                                        V                                                           V
//             +-----------------------------------------------------=-+     +-------------------------------------------------+
//             |           ObCheckpointUnitDiagnoseInfoMap             |     |               ObMemtableDiagnoseInfoMap         |
//             +-------------------------------------------------------+     +-------------------------------------------------+
//             | ObCheckpointDiagnoseKey|ObCheckpointUnitDiagnoseInfo* |     | ObCheckpointDiagnoseKey|ObMemtableDiagnoseInfo* |
//             +-------------------------------------------------------+     +-------------------------------------------------+
//             | ObCheckpointDiagnoseKey|ObCheckpointUnitDiagnoseInfo* |     | ObCheckpointDiagnoseKey|ObMemtableDiagnoseInfo* |
//             +-------------------------------------------------------+     +-------------------------------------------------+
//             | ObCheckpointDiagnoseKey|ObCheckpointUnitDiagnoseInfo* |     | ObCheckpointDiagnoseKey|ObMemtableDiagnoseInfo* |
//             +-------------------------------------------------------+     +-------------------------------------------------+
//
// 1. every tenant has a ObCheckpointDiagnoseMgr.
// 2. every checkpoint loop has a ObTraceInfo.
// 3. ObTraceInfo has two map(ObCheckpointUnitDiagnoseInfoMap/ObMemtableDiagnoseInfoMap).
// 4. memtable diagnose information is reported to ObMemtableDiagnoseInfoMap.
// 4. other checkpoint_unit diagnose information is reported to ObCheckpointUnitDiagnoseInfoMap.


static const uint64_t INVALID_TRACE_ID = -1;

struct ObCheckpointDiagnoseKey
{
  ObCheckpointDiagnoseKey(const ObTabletID &tablet_id,
      void *checkpoint_unit_ptr)
    : tablet_id_(tablet_id),
      checkpoint_unit_ptr_(checkpoint_unit_ptr) {}
  ObCheckpointDiagnoseKey()
    : tablet_id_(),
      checkpoint_unit_ptr_(NULL) {}

  int hash(uint64_t &hash_val) const
  {
    hash_val = tablet_id_.hash();
    hash_val = murmurhash(&checkpoint_unit_ptr_, sizeof(checkpoint_unit_ptr_), hash_val);
    return OB_SUCCESS;
  }
  bool operator==(const ObCheckpointDiagnoseKey &other) const
  { return tablet_id_ == other.tablet_id_ && checkpoint_unit_ptr_ == other.checkpoint_unit_ptr_; }
  bool is_valid() const { return tablet_id_.is_valid() && OB_NOT_NULL(checkpoint_unit_ptr_); }
  TO_STRING_KV(K_(tablet_id), K_(checkpoint_unit_ptr));

  ObTabletID tablet_id_;
  void *checkpoint_unit_ptr_;
};

struct ObCheckpointDiagnoseParam
{
  ObCheckpointDiagnoseParam(const int64_t trace_id,
      const ObTabletID &tablet_id,
      void *checkpoint_unit_ptr)
    : trace_id_(trace_id),
      key_(tablet_id, checkpoint_unit_ptr),
      is_freeze_clock_(false) {}
  // for the memtable of logstream freeze
  ObCheckpointDiagnoseParam(const int64_t ls_id,
      const uint32_t freeze_clock,
      const ObTabletID &tablet_id,
      void *checkpoint_unit_ptr)
    : ls_id_(ls_id),
      freeze_clock_(freeze_clock),
      key_(tablet_id, checkpoint_unit_ptr),
      is_freeze_clock_(true) {}
  TO_STRING_KV(K_(trace_id), K_(ls_id), K_(freeze_clock), K_(key), K_(is_freeze_clock));
  union
  {
    int64_t trace_id_;
    struct {
      int64_t ls_id_;
      uint32_t freeze_clock_;
    };
  };
  bool is_valid() const
  {
    if (is_freeze_clock_) {
      return key_.is_valid() && ls_id_ > 0;
    } else {
      return key_.is_valid() && trace_id_ != INVALID_TRACE_ID;
    }
  }
  ObCheckpointDiagnoseKey key_;
  bool is_freeze_clock_;
};

struct ObCheckpointUnitDiagnoseInfo
{
  const static int64_t MAX_TIME = 253402271999999999;
  friend class ObTraceInfo;
  ObCheckpointUnitDiagnoseInfo()
    : start_scn_(),
      end_scn_(),
      rec_scn_(),
      create_flush_dag_time_(MAX_TIME),
      merge_finish_time_(MAX_TIME),
      start_gc_time_(MAX_TIME) {}

  share::SCN start_scn_;
  share::SCN end_scn_;
  share::SCN rec_scn_;
  int64_t create_flush_dag_time_;
  int64_t merge_finish_time_;
  int64_t start_gc_time_;
  TO_STRING_KV(K_(start_scn), K_(end_scn), K_(rec_scn), K_(create_flush_dag_time),
    K_(merge_finish_time), K_(start_gc_time));
private:
  // lock/unlock in ObTraceInfo
  common::SpinRWLock lock_;
};

struct ObMemtableDiagnoseInfo : public ObCheckpointUnitDiagnoseInfo
{
  ObMemtableDiagnoseInfo()
    : frozen_finish_time_(MAX_TIME),
      memtable_occupy_size_(0),
      merge_start_time_(MAX_TIME),
      occupy_size_(0),
      concurrent_cnt_(0),
      release_time_(MAX_TIME) {}
  int64_t frozen_finish_time_;
  int64_t memtable_occupy_size_;
  int64_t merge_start_time_;
  int64_t occupy_size_;
  int64_t concurrent_cnt_;
  int64_t release_time_;
  INHERIT_TO_STRING_KV("ObCheckpointUnitDiagnoseInfo", ObCheckpointUnitDiagnoseInfo,
    K_(frozen_finish_time), K_(memtable_occupy_size), K_(merge_start_time), K_(occupy_size),
    K_(concurrent_cnt), K_(release_time));
};

typedef common::hash::ObHashMap<ObCheckpointDiagnoseKey, ObCheckpointUnitDiagnoseInfo*> ObCheckpointUnitDiagnoseInfoMap;
typedef common::hash::ObHashMap<ObCheckpointDiagnoseKey, ObMemtableDiagnoseInfo*> ObMemtableDiagnoseInfoMap;

class ObTraceInfo
{
  const static int64_t MALLOC_BLOCK_SIZE = (1LL << 12) - 128; // 4k
public:
  ObTraceInfo()
    : trace_id_(INVALID_TRACE_ID),
      freeze_clock_(0),
      ls_id_(),
      checkpoint_start_time_(0),
      arena_("CkpDgn", MALLOC_BLOCK_SIZE
#ifndef UNITTEST
          , MTL_ID()
#endif
          ),
      allocator_(arena_),
      checkpoint_unit_diagnose_info_map_(),
      memtable_diagnose_info_map_(),
      lock_()
  { memset(thread_name_, 0, oceanbase::OB_THREAD_NAME_BUF_LEN); }
  virtual ~ObTraceInfo() { reset(); }
  void init(const int64_t trace_id,
      const share::ObLSID &ls_id,
      const int64_t checkpoint_start_time);
  void reset()
  {
    SpinWLockGuard lock(lock_);
    reset_without_lock_();
  }
  void reset_without_lock_();
  void update_freeze_clock(const int64_t trace_id,
      const uint32_t logstream_clock);
  bool check_trace_id_(const int64_t trace_id);
  bool check_trace_id_(const ObCheckpointDiagnoseParam &param);

  template <typename T>
  int add_diagnose_info(const ObCheckpointDiagnoseParam &param);
  template <typename T, typename OP>
  int update_diagnose_info(const ObCheckpointDiagnoseParam &param,
      const OP &op);
  template <typename T, typename OP>
  int read_diagnose_info(const int64_t trace_id,
    const OP &op);

  template <typename OP>
  int read_trace_info(const int64_t trace_id,
      const OP &op);

  template<typename T>
  common::hash::ObHashMap<ObCheckpointDiagnoseKey, T*> &get_diagnose_info_map_();
  template<>
  ObCheckpointUnitDiagnoseInfoMap &get_diagnose_info_map_()
  {
    return checkpoint_unit_diagnose_info_map_;
  }
  template<>
  ObMemtableDiagnoseInfoMap &get_diagnose_info_map_()
  {
    return memtable_diagnose_info_map_;
  }
  TO_STRING_KV(K_(trace_id), K_(freeze_clock), K_(ls_id), K_(thread_name), K(checkpoint_unit_diagnose_info_map_.size()), K(memtable_diagnose_info_map_.size()));

  int64_t trace_id_;
  uint32_t freeze_clock_;
  share::ObLSID ls_id_;
  char thread_name_[oceanbase::OB_THREAD_NAME_BUF_LEN];
  int64_t checkpoint_start_time_;
  ObArenaAllocator arena_;
  ObSafeArenaAllocator allocator_;
  ObCheckpointUnitDiagnoseInfoMap checkpoint_unit_diagnose_info_map_;
  ObMemtableDiagnoseInfoMap memtable_diagnose_info_map_;
  common::SpinRWLock lock_;
};

#define DEF_UPDATE_TIME_FUNCTOR(function, diagnose_info_type, diagnose_info_time)         \
struct function                                                                           \
{                                                                                         \
public:                                                                                   \
  function(const ObCheckpointDiagnoseParam &param) : param_(param) {}                     \
  function& operator=(const function&) = delete;                                          \
  void operator()(diagnose_info_type &info) const               \
  {                                                                                       \
    const int64_t start_time = ObTimeUtility::current_time();                             \
    info.diagnose_info_time##_ = start_time;                                              \
    TRANS_LOG(DEBUG, ""#function"", K(info), K(param_));                                   \
  }                                                                                       \
private:                                                                                  \
  const ObCheckpointDiagnoseParam &param_;                                                      \
};

struct UpdateScheduleDagInfo
{
public:
  UpdateScheduleDagInfo(const ObCheckpointDiagnoseParam &param,
      const share::SCN &rec_scn,
      const share::SCN &start_scn,
      const share::SCN &end_scn)
    : param_(param),
      rec_scn_(rec_scn),
      start_scn_(start_scn),
      end_scn_(end_scn)
  {}
  UpdateScheduleDagInfo& operator=(const UpdateScheduleDagInfo&) = delete;
  void operator()(ObCheckpointUnitDiagnoseInfo &info) const;
private:
  const ObCheckpointDiagnoseParam &param_;
  const share::SCN &rec_scn_;
  const share::SCN &start_scn_;
  const share::SCN &end_scn_;
};

struct UpdateFreezeInfo
{
public:
  UpdateFreezeInfo(const ObCheckpointDiagnoseParam &param,
      const share::SCN &rec_scn,
      const share::SCN &start_scn,
      const share::SCN &end_scn,
      int64_t occupy_size)
    : param_(param),
      rec_scn_(rec_scn),
      start_scn_(start_scn),
      end_scn_(end_scn),
      occupy_size_(occupy_size)
  {}
  UpdateFreezeInfo& operator=(const UpdateFreezeInfo&) = delete;
  void operator()(ObMemtableDiagnoseInfo &info) const;
private:
  const ObCheckpointDiagnoseParam &param_;
  const share::SCN &rec_scn_;
  const share::SCN &start_scn_;
  const share::SCN &end_scn_;
  int64_t occupy_size_;
};

struct UpdateMergeInfoForMemtable
{
public:
  UpdateMergeInfoForMemtable(const ObCheckpointDiagnoseParam &param,
    int64_t merge_start_time,
    int64_t merge_finish_time,
    int64_t occupy_size,
    int64_t concurrent_cnt)
    : param_(param),
      merge_start_time_(merge_start_time),
      merge_finish_time_(merge_finish_time),
      occupy_size_(occupy_size),
      concurrent_cnt_(concurrent_cnt)
  {}
  UpdateMergeInfoForMemtable& operator=(const UpdateMergeInfoForMemtable&) = delete;
  void operator()(ObMemtableDiagnoseInfo &info) const;
private:
  const ObCheckpointDiagnoseParam &param_;
  int64_t merge_start_time_;
  int64_t merge_finish_time_;
  int64_t occupy_size_;
  int64_t concurrent_cnt_;
};

struct GetTraceInfoForMemtable
{
public:
  GetTraceInfoForMemtable(const ObCheckpointDiagnoseParam &param,
    ObTraceInfo *&ret)
    : param_(param),
      ret_(ret)
  {}
  GetTraceInfoForMemtable& operator=(const GetTraceInfoForMemtable&) = delete;
  int operator()(ObTraceInfo &trace_info) const;
private:
  const ObCheckpointDiagnoseParam &param_;
  ObTraceInfo *&ret_;
};

template <typename T>
int ObTraceInfo::add_diagnose_info(const ObCheckpointDiagnoseParam &param)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard lock(lock_);
  if (check_trace_id_(param)) {
    void *ptr = NULL;
    if (OB_ISNULL(ptr = allocator_.alloc(sizeof(T)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      TRANS_LOG(WARN, "failed to alloc", KR(ret));
    } else {
      T *diagnose_info = new (ptr)T();
      SpinWLockGuard lock(diagnose_info->lock_);
      if (OB_FAIL(get_diagnose_info_map_<T>().set_refactored(param.key_, diagnose_info,
              0 /* not overwrite */ ))) {
        TRANS_LOG(WARN, "failed to add diagnose info", KR(ret), K(param.key_), KPC(diagnose_info), KPC(this));
      }
    }
  }
  return ret;
}

template <typename T, typename OP>
int ObTraceInfo::update_diagnose_info(const ObCheckpointDiagnoseParam &param,
    const OP &op)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard lock(lock_);

  if (check_trace_id_(param)) {
    T *diagnose_info = NULL;
    if (OB_FAIL(get_diagnose_info_map_<T>().get_refactored(param.key_, diagnose_info))) {
      TRANS_LOG(WARN, "failed to get diagnose info", KR(ret), K(param.key_), KPC(this));
    } else if (OB_ISNULL(diagnose_info)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "diagnose info is NULL", KR(ret), K(param.key_), KPC(this));
    } else {
      SpinWLockGuard lock(diagnose_info->lock_);
      op(*diagnose_info);
    }
  }
  return ret;
}

template <typename T, typename OP>
int ObTraceInfo::read_diagnose_info(const int64_t trace_id,
    const OP &op)
{
  int ret = OB_SUCCESS;
  TRANS_LOG(INFO, "read_diagenose_info in ObTraceInfo", KPC(this), K(trace_id));
  SpinRLockGuard lock(lock_);
  if (check_trace_id_(trace_id)) {
    FOREACH(iter, get_diagnose_info_map_<T>()) {
      ObCheckpointDiagnoseKey key = iter->first;
      T *diagnose_info = iter->second;
      if (OB_ISNULL(diagnose_info)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "diagnose_info is NULL", KR(ret), K(key));
      } else {
        SpinRLockGuard diagnose_lock(diagnose_info->lock_);
        if (OB_FAIL(op(*this, key, *diagnose_info))) {
          TRANS_LOG(WARN, "failed to op in read_diagnose_info", KR(ret), K(key), K(diagnose_info));
        }
      }
    }
  }
  return ret;
}

template <typename OP>
int ObTraceInfo::read_trace_info(const int64_t trace_id, const OP &op)
{
  int ret = OB_SUCCESS;
  SpinRLockGuard lock(lock_);
  if (INVALID_TRACE_ID == trace_id
      || check_trace_id_(trace_id)) {
    if (OB_FAIL(op(*this))) {
      TRANS_LOG(WARN, "failed to op in read_trace_info", KR(ret));
    }
  }
  return ret;
}

class ObCheckpointDiagnoseMgr
{
private:
  const static int64_t MAX_TRACE_INFO_ARR_SIZE = 800;
public:
  ObCheckpointDiagnoseMgr(int64_t max_trace_info_size = 100)
    : first_pos_(0),
      last_pos_(-1),
      max_trace_info_size_(max_trace_info_size),
      is_inited_(false)
  {}
  virtual ~ObCheckpointDiagnoseMgr() {}
  static int mtl_init(ObCheckpointDiagnoseMgr* &m) { return m->init(); }
  int init();
  void destroy() { is_inited_ = false; };
  int update_max_trace_info_size(int64_t max_trace_info_size);

  // trace_info func
  int acquire_trace_id(const share::ObLSID &ls_id,
      int64_t &trace_id);
  int update_freeze_clock(const share::ObLSID &ls_id,
      const int64_t trace_id,
      const int logstream_clock);

  template <typename OP>
  int add_diagnose_info(const ObCheckpointDiagnoseParam &param);
  // checkpoint_unit func
  int update_schedule_dag_info(const ObCheckpointDiagnoseParam &param,
      const share::SCN &rec_scn,
      const share::SCN &start_scn,
      const share::SCN &end_scn);
  int update_merge_info_for_checkpoint_unit(const ObCheckpointDiagnoseParam &param);
  int update_start_gc_time_for_checkpoint_unit(const ObCheckpointDiagnoseParam &param);

  // memtable func
  int update_freeze_info(const ObCheckpointDiagnoseParam &param,
      const share::SCN &rec_scn,
      const share::SCN &start_scn,
      const share::SCN &end_scn,
      const int64_t occupy_size);
  int update_schedule_dag_time(const ObCheckpointDiagnoseParam &param);
  int update_release_time(const ObCheckpointDiagnoseParam &param);
  int update_merge_info_for_memtable(const ObCheckpointDiagnoseParam &param,
    int64_t merge_start_time,
    int64_t merge_finish_time,
    int64_t occupy_size,
    int64_t concurrent_cnt);
  int update_start_gc_time_for_memtable(const ObCheckpointDiagnoseParam &param);

  // virtual table iter
  template <typename OP>
  int read_trace_info(const OP &op);
  template<typename T, typename OP>
  int read_diagnose_info(const int64_t trace_id,
      const OP &op);

  int64_t get_trace_info_count() { return last_pos_ - first_pos_ + 1; }
  ObTraceInfo* get_trace_info_for_memtable(const ObCheckpointDiagnoseParam &param);
  TO_STRING_KV(K_(first_pos), K_(last_pos), K_(max_trace_info_size));

private:
  void reset_old_trace_infos_without_pos_lock_()
  {
    while (last_pos_ - first_pos_ >= max_trace_info_size_) {
      trace_info_arr_[first_pos_++ % MAX_TRACE_INFO_ARR_SIZE].reset();
    }
  }

private:
  int64_t first_pos_;
  int64_t last_pos_;
  ObTraceInfo trace_info_arr_[MAX_TRACE_INFO_ARR_SIZE];
  common::SpinRWLock pos_lock_;
  // max count of trace_info, others in trace_info_arr  will be reset
  int64_t max_trace_info_size_;
  bool is_inited_;
};

template<typename T>
int ObCheckpointDiagnoseMgr::add_diagnose_info(const ObCheckpointDiagnoseParam &param)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObCheckpointDiagnoseMgr not inited.", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "param is invalid", KR(ret), K(param));
  } else {
    ObTraceInfo *trace_info_ptr = get_trace_info_for_memtable(param);
    if (OB_ISNULL(trace_info_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "trace_info is NULL", KR(ret), K(param));
    } else if (OB_FAIL(trace_info_ptr->add_diagnose_info<T>(param))) {
      TRANS_LOG(WARN, "failed to add diagnose_info", KR(ret), K(param));
    } else {
      TRANS_LOG(INFO, "add_diagnose_info", KR(ret), K(param));
    }
  }
  return ret;
}

template<typename T, typename OP>
int ObCheckpointDiagnoseMgr::read_diagnose_info(const int64_t trace_id,
    const OP &op)
{
  int ret = OB_SUCCESS;
  TRANS_LOG(INFO, "read_diagnose_info in ObCheckpointDiagnoseMgr", K(first_pos_), K(last_pos_), K(get_trace_info_count()));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObCheckpointDiagnoseMgr not inited.", KR(ret));
  } else if (INVALID_TRACE_ID == trace_id) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "trace_id is invalid", KR(ret));
  } else {
    if (get_trace_info_count() > 0) {
      if (OB_FAIL(trace_info_arr_[trace_id % MAX_TRACE_INFO_ARR_SIZE].read_diagnose_info<T>(trace_id, op))) {
        TRANS_LOG(WARN, "failed to read_diagnose_info", KR(ret), K(trace_id));
      }
    }
  }
  return ret;
}

template <typename OP>
int ObCheckpointDiagnoseMgr::read_trace_info(const OP &op)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObCheckpointDiagnoseMgr not inited.", KR(ret));
  } else {
    SpinRLockGuard lock(pos_lock_);
    if (get_trace_info_count() > 0) {
      for (int64_t i = first_pos_; OB_SUCC(ret) && i <= last_pos_; i++) {
        ret = trace_info_arr_[i % MAX_TRACE_INFO_ARR_SIZE].read_trace_info(i, op);
      }
    }
  }
  return ret;
}

}
}
}

#endif // OCEANBASE_STORAGE_OB_CHECKPOINT_DIAGNOSE_H_
