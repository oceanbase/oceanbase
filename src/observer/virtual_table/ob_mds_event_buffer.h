/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef VEITUAL_TABLE_OB_MDS_EVENT_BUFFER_H
#define VEITUAL_TABLE_OB_MDS_EVENT_BUFFER_H

#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "share/cache/ob_vtable_event_recycle_buffer.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "storage/multi_data_source/runtime_utility/common_define.h"
#include "util/easy_time.h"
#include "share/ob_task_define.h"
#include "storage/tx/ob_tx_seq.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
class MdsTableBase;
template <typename K, typename V>
class MdsUnit;
template <typename K, typename V>
class MdsRow;
template <typename K, typename V>
class UserMdsNode;
}
}
namespace observer
{
class ObAllVirtualMdsEventHistory;
class ObMdsEventBuffer;
struct MdsEventKey {
  MdsEventKey() = default;
  MdsEventKey(uint64_t tenant_id, share::ObLSID ls_id, common::ObTabletID tablet_id)
  : tenant_id_(tenant_id),
  ls_id_(ls_id),
  tablet_id_(tablet_id) {}
  bool operator<(const MdsEventKey &rhs) {
    return tenant_id_ < rhs.tenant_id_ && ls_id_ < rhs.ls_id_ && tablet_id_ < rhs.tablet_id_;
  }
  bool operator==(const MdsEventKey &rhs) {
    return tenant_id_ == rhs.tenant_id_ && ls_id_ == rhs.ls_id_ && tablet_id_ == rhs.tablet_id_;
  }
  uint64_t hash() const {
    uint64_t hash = 0;
    hash = murmurhash(&tenant_id_, sizeof(tenant_id_), hash);
    hash = murmurhash(&tablet_id_, sizeof(tablet_id_), hash);
    hash = murmurhash(&ls_id_, sizeof(ls_id_), hash);
    return hash;
  }
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id));
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
};

struct MdsEvent {
  friend class storage::mds::MdsTableBase;
  template <typename K, typename V>
  friend class storage::mds::MdsUnit;
  template <typename K, typename V>
  friend class storage::mds::MdsRow;
  template <typename K, typename V>
  friend class storage::mds::UserMdsNode;
  friend class ObAllVirtualMdsEventHistory;
  friend class ObMdsEventBuffer;
  MdsEvent()
  : timestamp_(0),
  event_(nullptr),
  info_str_(),
  unit_id_(UINT8_MAX),
  writer_type_(storage::mds::WriterType::UNKNOWN_WRITER),
  writer_id_(0),
  seq_no_(),
  redo_scn_(),
  end_scn_(),
  trans_version_(),
  node_type_(storage::mds::MdsNodeType::UNKNOWN_NODE),
  state_(storage::mds::TwoPhaseCommitState::STATE_END),
  key_str_(),
  alloc_(nullptr) {}
  ~MdsEvent() {
    if (OB_NOT_NULL(alloc_)) {
      if (!key_str_.empty()) {
        alloc_->free(key_str_.ptr());
      }
      if (!info_str_.empty()) {
        alloc_->free(info_str_.ptr());
      }
    }
    new (this) MdsEvent();
  }
  int assign(ObIAllocator &alloc, const MdsEvent &rhs) {
    int ret = OB_SUCCESS;
    if (!rhs.is_valid_()) {
      ret = OB_INVALID_ARGUMENT;
      MDS_LOG(WARN, "invalid argument", KR(ret), K(rhs));
    } else if (OB_FAIL(set_(alloc, rhs.timestamp_, rhs.event_, rhs.info_str_, rhs.unit_id_, rhs.writer_type_,
                            rhs.writer_id_, rhs.seq_no_, rhs.redo_scn_, rhs.end_scn_, rhs.trans_version_,
                            rhs.node_type_, rhs.state_, rhs.key_str_))) {
      // don't report 4013, cause alloc use ring buffer, 4013 is expected
    } else {
      tid_ = rhs.tid_;
      trace_id_ = rhs.trace_id_;
      memcpy(tname_, rhs.tname_, 16);
    }
    return ret;
  }
  TO_STRING_KV(KP_(alloc), KTIME_(timestamp), K_(event), K_(info_str), K_(unit_id), K_(key_str), K_(writer_type), \
               K_(writer_id), K_(seq_no), K_(redo_scn), K_(end_scn), K_(trans_version), K_(node_type), K_(state));
private:
  bool is_valid_() const { return OB_NOT_NULL(event_); }
  int set_(ObIAllocator &alloc,
           int64_t timestamp,
           const char *event_str,
           const ObString &info_str,
           uint8_t unit_id,
           storage::mds::WriterType writer_type,
           int64_t writer_id,
           transaction::ObTxSEQ seq_no,
           share::SCN redo_scn,
           share::SCN end_scn,
           share::SCN trans_version,
           storage::mds::MdsNodeType node_type,
           storage::mds::TwoPhaseCommitState state,
           const ObString &key_str) {
    #define PRINT_WRAPPER K(ret), K(event_str), K(unit_id), K(writer_type), K(writer_id), K(seq_no), K(redo_scn),\
                          K(trans_version), K(node_type), K(state), K(key_str)
    int ret = OB_SUCCESS;
    if (is_valid_()) {
      ret = OB_INIT_TWICE;
      MDS_LOG(WARN, "this event has been valid", PRINT_WRAPPER, K(*this));
    }
    if (OB_SUCC(ret) && !info_str.empty()) {
      char *ptr = nullptr;
      if (nullptr == (ptr = (char *)alloc.alloc(info_str.length()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        // MDS_LOG(WARN, "fail to alloc memory", PRINT_WRAPPER);
      } else {
        memcpy(ptr, info_str.ptr(), info_str.length());
        info_str_.assign_ptr(ptr, info_str.length());
      }
    }
    if (OB_SUCC(ret) && !key_str.empty()) {
      char *ptr = nullptr;
      if (nullptr == (ptr = (char *)alloc.alloc(key_str.length()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        // MDS_LOG(WARN, "fail to alloc memory", PRINT_WRAPPER);
      } else {
        memcpy(ptr, key_str.ptr(), key_str.length());
        key_str_.assign_ptr(ptr, key_str.length());
      }
    }
    if (OB_FAIL(ret)) {
      if (!key_str_.empty()) {
        alloc.free(key_str_.ptr());
      }
      if (!info_str_.empty()) {
        alloc.free(info_str_.ptr());
      }
    } else {
      timestamp_ = timestamp;
      event_ = event_str;
      unit_id_ = unit_id;
      writer_type_ = writer_type;
      writer_id_ = writer_id;
      seq_no_ = seq_no;
      redo_scn_ = redo_scn;
      end_scn_ = end_scn;
      trans_version_ = trans_version;
      node_type_ = node_type;
      state_ = state;
      alloc_ = &alloc;
    }
    return ret;
    #undef PRINT_WRAPPER
  }
  void record_thread_info_() {
    tid_ = (uint32_t)GETTID();
    trace_id_ = *ObCurTraceId::get_trace_id();
    char *name = ob_get_tname();
    if (OB_NOT_NULL(name)) {
      int64_t name_len = strlen(name);
      int64_t copy_len = std::min<int64_t>(name_len, 15);
      memcpy(tname_, name, copy_len);
      tname_[copy_len] = '\0';
    }
    timestamp_ = fast_current_time();
  }
private:
  // recorded by call record_thread_info_()
  uint32_t tid_;
  ObCurTraceId::TraceId trace_id_;
  char tname_[16] = {0};
  int64_t timestamp_;
  // need fill
  const char *event_;
  ObString info_str_;
  uint8_t unit_id_;
  storage::mds::WriterType writer_type_;
  int64_t writer_id_;
  transaction::ObTxSEQ seq_no_;
  share::SCN redo_scn_;
  share::SCN end_scn_;
  share::SCN trans_version_;
  storage::mds::MdsNodeType node_type_;
  storage::mds::TwoPhaseCommitState state_;
  ObString key_str_;
  ObIAllocator *alloc_;
};

struct ObMdsEventBuffer {
  struct DefaultAllocator : public ObIAllocator
  {
    void *alloc(const int64_t size)  { return ob_malloc(size, "MDS"); }
    void *alloc(const int64_t size, const ObMemAttr &attr) { return ob_malloc(size, attr); }
    void free(void *ptr) { ob_free(ptr); }
    void set_label(const lib::ObLabel &) {}
    static DefaultAllocator &get_instance() { static DefaultAllocator alloc; return alloc; }
    static int64_t get_alloc_times() { return ATOMIC_LOAD(&get_instance().alloc_times_); }
    static int64_t get_free_times() { return ATOMIC_LOAD(&get_instance().free_times_); }
  private:
    DefaultAllocator() : alloc_times_(0), free_times_(0) {}
    int64_t alloc_times_;
    int64_t free_times_;
  };
  struct Singleton {
    int init() {
      int ret = OB_SUCCESS;
      if (OB_FAIL(mds_event_cache_.init("MdsEventCache", DefaultAllocator::get_instance(), 0, 128_MB, 8192))) {// not enable
        ret = OB_SUCCESS;
        OCCAM_LOG(WARN, "init failed", KR(ret));
      } else {
        is_inited_ = true;
        OCCAM_LOG(INFO, "init ObMdsEventBuffer success", KR(ret));
      }
      return ret;
    }
    void destroy() {
      mds_event_cache_.~ObVtableEventRecycleBuffer();
    }
    void append(const MdsEventKey &key, const MdsEvent &event, const storage::mds::MdsTableBase *mds_table, const char *file, const uint32_t line, const char *func) {
      if (OB_NOT_NULL(file) && OB_UNLIKELY(line != 0) && OB_NOT_NULL(func) && OB_NOT_NULL(event.event_)) {
        share::ObTaskController::get().allow_next_syslog();
        ::oceanbase::common::OB_PRINT("[MDS.EVENT]", OB_LOG_LEVEL_INFO, file, line, func, OB_LOG_LOCATION_HASH_VAL, OB_SUCCESS,
                                      event.event_, LOG_KVS(K(key), K(event), KPC(mds_table)));
      }
      if (is_inited_) {
        (void) mds_event_cache_.append(key, event, file, line, func);
      }
    }
    template <typename OP>
    int for_each(const MdsEventKey &key, OP &&op) {
      int ret = OB_SUCCESS;
      if (!is_inited_) {
        OCCAM_LOG(INFO, "ObMdsEventBuffer is not init", KR(ret));
      } else {
        ret = mds_event_cache_.for_each(key, std::forward<OP>(op));
      }
      return ret;
    }
    template <typename OP>
    int for_each(OP &&op) {
      int ret = OB_SUCCESS;
      if (!is_inited_) {
        OCCAM_LOG(INFO, "ObMdsEventBuffer is not init", KR(ret));
      } else {
        ret = mds_event_cache_.for_each(std::forward<OP>(op));
      }
      return ret;
    }
    void dump_statistics() {
      mds_event_cache_.dump_statistics();
    }
    static Singleton &get_instance() { static Singleton s; return s; }
  private:
    Singleton() : is_inited_(false), mds_event_cache_() {}
    bool is_inited_;
    common::cache::ObVtableEventRecycleBuffer<MdsEventKey, MdsEvent> mds_event_cache_;
  };
  static int init() { return Singleton::get_instance().init(); }
  static void destroy() { return Singleton::get_instance().destroy(); }
  static void append(const MdsEventKey &key,
                     const MdsEvent &event,
                     const storage::mds::MdsTableBase *mds_table,
                     const char *file,
                     const uint32_t line,
                     const char *func) {
    return Singleton::get_instance().append(key, event, mds_table, file, line, func);
  }
  template <typename OP>
  static int for_each(const MdsEventKey &key, OP &&op) {
    return Singleton::get_instance().for_each(key, std::forward<OP>(op));
  }
  template <typename OP>
  static int for_each(OP &&op) {
    return Singleton::get_instance().for_each(std::forward<OP>(op));
  }
  static void dump_statistics() {
    Singleton::get_instance().dump_statistics();
  }
};

}
}

#endif
