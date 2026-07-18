/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/utility/utility.h"
#define USING_LOG_PREFIX STORAGE
#include "ob_migration_vector_index_processor.h"
#include "ob_storage_ha_service.h"
#include "ob_storage_ha_utils.h"
#include "storage/ls/ob_ls.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_serialize.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "lib/time/ob_time_utility.h"
#include "lib/lock/ob_tc_rwlock.h"
#ifdef ERRSIM
#include "observer/ob_server_event_history_table_operator.h"
#endif

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;

typedef common::hash::ObHashMap<int64_t, ObMigrationVectorIndexProcessor *>::iterator ProcessorMapIter;
typedef common::hash::ObHashMap<int64_t, ObVectorIndexMigrationProcessorMgr::VecMigAdaptorHoldEntry>::iterator AdaptorGuardMapIter;

// ======================== do_serialize callback ========================
bool VecMigSerializeCbParam::is_valid() const
{
  return OB_NOT_NULL(ctrl_)
      && timeout_us_ >= 0
      && slot_cap_ > 0
      && OB_NOT_NULL(slot_chunk_)
      && OB_INVALID_ID != processor_id_
      && OB_INVALID_TENANT_ID != header_tenant_id_
      && ls_id_.is_valid();
}

// Pack header and body into the slot buffer and push it to the source sliding window.
static int push_chunk_(VecMigSerializeCbParam &p,
                       const ObVecIdxMigChunkHeader &header,
                       const char *body,
                       const int64_t body_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(header.serialize(p.slot_chunk_, p.slot_cap_, pos))) {
    LOG_WARN("[VEC MIG] serialize chunk header fail", K(ret), K(p.slot_cap_), K(header));
  } else if (OB_UNLIKELY(pos > ObVecIdxMigChunkHeader::MAX_BYTES)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[VEC MIG] header serialize size exceeds upper bound", K(ret), K(pos),
        "max", ObVecIdxMigChunkHeader::MAX_BYTES, K(header));
  } else if (OB_UNLIKELY(body_len <= 0 || OB_ISNULL(body) || body_len > p.slot_cap_ - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[VEC MIG] invalid chunk body", K(ret), K(body_len), KP(body), K(pos), K(p.slot_cap_));
  } else {
    MEMCPY(p.slot_chunk_ + pos, body, body_len);
    pos += body_len;
    if (OB_FAIL(p.ctrl_->generate_next_data(p.slot_chunk_, pos, p.timeout_us_))) {
      LOG_WARN("[VEC MIG] generate next data fail", K(ret), K(pos));
    }
  }
  return ret;
}

// Serialize segment meta and push it as chunks through the source sliding window.
static int push_meta_chunks_(VecMigSerializeCbParam &p,
                             share::ObVectorIndexSegmentHandle &seg_handle,
                             common::ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  int64_t meta_size = 0;
  share::ObVectorIndexSegment *seg = nullptr;
  if (!seg_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[VEC MIG] invalid segment handle in push_meta_chunks_", K(ret));
  } else if (OB_ISNULL(seg = seg_handle.get())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[VEC MIG] null segment in push_meta_chunks_", K(ret), KP(seg));
  } else if (OB_FALSE_IT(meta_size = seg->get_serialize_meta_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[VEC MIG] non-positive meta size", K(ret), K(meta_size));
  } else if (OB_UNLIKELY(meta_size <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[VEC MIG] non-positive meta size", K(ret), K(meta_size));
  } else {
    char *meta_buf = static_cast<char *>(alloc.alloc(meta_size));
    if (OB_ISNULL(meta_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[VEC MIG] failed to alloc meta buf", K(ret), K(meta_size));
    } else {
      int64_t pos = 0;
      if (OB_FAIL(seg->serialize_meta(meta_buf, meta_size, pos))) {
        LOG_WARN("[VEC MIG] segment serialize_meta fail", K(ret), K(meta_size));
      } else {
        ObVecIdxMigMetaChunkHeader header;
        header.meta_len_ = pos;
        header.processor_id_ = p.processor_id_;
        header.tenant_id_ = p.header_tenant_id_;
        header.ls_id_ = p.ls_id_;
        const int64_t header_size = header.get_serialize_size();
        if (OB_UNLIKELY(header_size <= 0
                        || header_size > ObVecIdxMigChunkHeader::MAX_BYTES
                        || header_size >= p.slot_cap_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[VEC MIG] meta chunk header size out of range", K(ret),
              K(header_size), K(p.slot_cap_));
        } else {
          const int64_t body_room = p.slot_cap_ - header_size;
          int64_t emitted = 0;
          while (OB_SUCC(ret) && emitted < pos) {
            const int64_t take = MIN(body_room, pos - emitted);
            if (OB_FAIL(push_chunk_(p, header, meta_buf + emitted, take))) {
              LOG_WARN("[VEC MIG] emit meta chunk fail", K(ret), K(emitted), K(take));
            } else {
              emitted += take;
            }
          }
        }
      }
    }
  }
  return ret;
}

// Meta is pushed directly from do_serialize before the
// serializer runs. this callback is called with a vsag chunk.
static int vec_mig_serialize_cb(const char *data,
                                const int64_t data_len,
                                share::ObOStreamBuf::CbParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(data_len < 0 || (data_len > 0 && OB_ISNULL(data)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[VEC MIG] invalid arguments in serialize cb", K(ret), K(data_len));
  } else if (0 == data_len) { // finalize no-op
    LOG_INFO("[VEC MIG] finalize no-op in serialize cb", K(data_len));
  } else if (OB_UNLIKELY(share::ObOStreamBuf::CbParam::CbParamType::VEC_MIG_SERIALIZE != param.get_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[VEC MIG] unexpected CbParam type", K(ret), "type", static_cast<int64_t>(param.get_type()));
  } else {
    VecMigSerializeCbParam &ser_param = static_cast<VecMigSerializeCbParam &>(param);
    if (OB_UNLIKELY(!ser_param.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[VEC MIG] invalid ser_param in serialize cb", K(ret),
          KP(ser_param.ctrl_), K(ser_param.timeout_us_), K(ser_param.slot_cap_),
          KP(ser_param.slot_chunk_), K(ser_param.processor_id_),
          K(ser_param.header_tenant_id_), K(ser_param.ls_id_));
    } else {
      ObVecIdxMigVsagChunkHeader header;
      header.processor_id_ = ser_param.processor_id_;
      header.tenant_id_ = ser_param.header_tenant_id_;
      header.ls_id_ = ser_param.ls_id_;
      if (OB_FAIL(push_chunk_(ser_param, header, data, data_len))) {
        LOG_WARN("[VEC MIG] emit vsag chunk fail", K(ret), K(data_len));
      }
    }
  }
  return ret;
}

// ======================== ObVectorIndexMigrationProcessorGuard ========================

ObVectorIndexMigrationProcessorGuard::ObVectorIndexMigrationProcessorGuard(
    ObVectorIndexMigrationProcessorMgr *mgr,
    ObMigrationVectorIndexProcessor *proc)
  : proc_(proc), mgr_(mgr)
{
  if (proc_ != nullptr && mgr_ != nullptr) {
    ObVectorIndexMigrationProcessorMgr::inc_ref_(proc_);
  } else {
    proc_ = nullptr;
    mgr_ = nullptr;
  }
}

ObVectorIndexMigrationProcessorGuard::ObVectorIndexMigrationProcessorGuard(const ObVectorIndexMigrationProcessorGuard &other)
  : proc_(other.proc_), mgr_(other.mgr_)
{
  if (proc_ != nullptr && mgr_ != nullptr) {
    ObVectorIndexMigrationProcessorMgr::inc_ref_(proc_);
  } else {
    proc_ = nullptr;
    mgr_ = nullptr;
  }
}

ObVectorIndexMigrationProcessorGuard &ObVectorIndexMigrationProcessorGuard::operator=(const ObVectorIndexMigrationProcessorGuard &other)
{
  if (this != &other) {
    reset();
    if (other.proc_ != nullptr && other.mgr_ != nullptr) {
      proc_ = other.proc_;
      mgr_ = other.mgr_;
      ObVectorIndexMigrationProcessorMgr::inc_ref_(proc_);
    }
  }
  return *this;
}

void ObVectorIndexMigrationProcessorGuard::reset()
{
  if (proc_ != nullptr && mgr_ != nullptr) {
    ObVectorIndexMigrationProcessorMgr::dec_ref_(proc_);
  }
  proc_ = nullptr;
  mgr_ = nullptr;
}

int ObVectorIndexMigrationProcessorGuard::set_processor(
    ObVectorIndexMigrationProcessorMgr *mgr,
    ObMigrationVectorIndexProcessor *proc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(proc) || OB_ISNULL(mgr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[VEC MIG] invalid args for set_processor", K(ret), KP(proc), KP(mgr));
  } else if (OB_UNLIKELY(proc_ != nullptr || mgr_ != nullptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[VEC MIG] guard already holds a processor", K(ret), KP(proc_), KP(mgr_));
  } else {
    ObVectorIndexMigrationProcessorMgr::inc_ref_(proc);
    proc_ = proc;
    mgr_ = mgr;
  }
  return ret;
}

// ======================== ObVectorIndexSerializeDag ========================

ObVectorIndexSerializeDag::ObVectorIndexSerializeDag()
  : ObIDag(ObDagType::DAG_TYPE_VECTOR_INDEX_SERIALIZE),
    is_inited_(false),
    processor_guard_()
{
}

ObVectorIndexSerializeDag::~ObVectorIndexSerializeDag()
{
}

int ObVectorIndexSerializeDag::init(const ObVectorIndexMigrationProcessorGuard &processor_guard)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[VEC MIG] vector index serialize dag init twice", K(ret));
  } else if (!processor_guard.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[VEC MIG] invalid processor guard for vector index serialize dag init", K(ret));
  } else {
    processor_guard_ = processor_guard;
    is_inited_ = true;
    LOG_INFO("vector index serialize dag init success", K(ret), KPC(this));
  }
  return ret;
}

int ObVectorIndexSerializeDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObVectorIndexSerializeTask *task = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[VEC MIG] vector index serialize dag not inited", K(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("[VEC MIG] fail to alloc vector index serialize task", K(ret));
  } else if (OB_FAIL(task->init())) {
    LOG_WARN("[VEC MIG] fail to init vector index serialize task", K(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("[VEC MIG] fail to add task", K(ret));
  } else {
    LOG_DEBUG("success to create first task", K(ret), KPC(this));
  }
  return ret;
}

bool ObVectorIndexSerializeDag::operator==(const share::ObIDag &other) const
{
  bool is_same = false;
  if (this == &other) {
    is_same = true;
  } else if (get_type() == other.get_type()) {
    const ObVectorIndexSerializeDag &other_dag =
        static_cast<const ObVectorIndexSerializeDag &>(other);
    ObMigrationVectorIndexProcessor *lhs = processor_guard_.get();
    ObMigrationVectorIndexProcessor *rhs = other_dag.processor_guard_.get();
    if (OB_NOT_NULL(lhs) && OB_NOT_NULL(rhs)) {
      is_same = (lhs->get_processor_id() == rhs->get_processor_id());
    }
  }
  return is_same;
}

uint64_t ObVectorIndexSerializeDag::hash() const
{
  ObMigrationVectorIndexProcessor *processor = processor_guard_.get();
  int64_t id = OB_NOT_NULL(processor) ? processor->get_processor_id() : 0;
  return common::murmurhash(&id, sizeof(id), 0);
}

int ObVectorIndexSerializeDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMigrationVectorIndexProcessor *processor = processor_guard_.get();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[VEC MIG] vector index serialize dag not inited", K(ret));
  } else if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[VEC MIG] invalid args for vector index serialize dag fill dag key", K(ret), KP(buf), K(buf_len));
  } else if (OB_ISNULL(processor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[VEC MIG] processor guard empty", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
      "vector_index_serialize: ls_id=%ld, tablet_id=%ld, processor_id=%ld",
      processor->get_ls_id().id(),
      processor->get_data_tablet_id().id(),
      processor->get_processor_id()))) {
    LOG_WARN("[VEC MIG] fail to fill dag key", K(ret));
  }
  return ret;
}

int ObVectorIndexSerializeDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param,
    ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  ObMigrationVectorIndexProcessor *processor = processor_guard_.get();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[VEC MIG] vector index serialize dag not inited", K(ret));
  } else if (OB_ISNULL(processor)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[VEC MIG] vector index serialize dag processor guard empty", K(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
      static_cast<int64_t>(MTL_ID()),
      processor->get_ls_id().id(),
      static_cast<int64_t>(processor->get_data_tablet_id().id()),
      processor->get_processor_id()))) {
    LOG_WARN("[VEC MIG] fail to fill vector index serialize dag info param", K(ret));
  }
  return ret;
}

// ======================== ObVectorIndexSerializeTask ========================

ObVectorIndexSerializeTask::ObVectorIndexSerializeTask()
  : ObITask(ObITask::TASK_TYPE_VEC_INDEX_SERIALIZE),
    is_inited_(false)
{
}

ObVectorIndexSerializeTask::~ObVectorIndexSerializeTask()
{
}

int ObVectorIndexSerializeTask::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[VEC MIG] vector index serialize task init twice", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObVectorIndexSerializeTask::process()
{
  int ret = OB_SUCCESS;
  ObVectorIndexSerializeDag *dag = nullptr;
  ObMigrationVectorIndexProcessor *processor = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[VEC MIG] vector index serialize task not inited", K(ret));
  } else if (OB_ISNULL(get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[VEC MIG] dag should not be NULL", K(ret));
  } else if (ObDagType::DAG_TYPE_VECTOR_INDEX_SERIALIZE != get_dag()->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[VEC MIG] dag type is unexpected", K(ret), KPC(get_dag()));
  } else if (FALSE_IT(dag = static_cast<ObVectorIndexSerializeDag *>(get_dag()))) {
  } else if (OB_ISNULL(processor = dag->get_processor())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[VEC MIG] dag has no processor", K(ret));
  } else {
    LOG_INFO("[VEC MIG] serialize task begin", KPC(processor));
    if (OB_FAIL(processor->do_serialize())) {
      LOG_WARN("[VEC MIG] serialize task failed", K(ret), KPC(processor));
    } else {
      LOG_INFO("[VEC MIG] serialize task completed", KPC(processor));
    }
  }
  return ret;
}

// ======================== ObMigrationVectorIndexProcessor ========================

ObMigrationVectorIndexProcessor::ObMigrationVectorIndexProcessor()
  : is_inited_(false),
    processor_id_(-1),
    ls_id_(),
    data_tablet_id_(),
    segment_idx_(OB_INVALID_INDEX_INT64),
    dest_addr_(),
    tenant_id_(OB_INVALID_TENANT_ID),
    adaptor_handle_id_(-1),
    dag_prio_(share::ObDagPrio::DAG_PRIO_VECTOR_INDEX),
    state_lock_(common::ObLatchIds::OB_STORAGE_HA_STRUCT_LOCK),
    state_(ObMigrationVectorIndexProcessor::State::INIT),
    last_access_time_(ObTimeUtility::current_time()),
    result_mgr_(),
    ref_count_(0),
    adaptor_guard_(),
    src_controller_handle_()
{
}

ObMigrationVectorIndexProcessor::~ObMigrationVectorIndexProcessor()
{
  destroy();
}

int ObMigrationVectorIndexProcessor::init(
    const int64_t processor_id,
    const ObLSID &ls_id,
    const ObTabletID &data_tablet_id,
    const int64_t segment_idx,
    const ObAddr &dest_addr,
    const uint64_t tenant_id,
    const int64_t adaptor_handle_id)
{
  int ret = OB_SUCCESS;
  ObStorageHAService *ha_service = nullptr;
  ObMigrationTenantWindowMgr *window_mgr = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[VEC MIG] processor init twice", K(ret), K(processor_id));
  } else if (!ObVectorIndexMigrationProcessorMgr::is_valid_processor_id(processor_id) || !ls_id.is_valid()
             || !data_tablet_id.is_valid() || segment_idx < 0 || !dest_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[VEC MIG] invalid argument", K(ret), K(processor_id), K(ls_id),
        K(data_tablet_id), K(segment_idx), K(dest_addr));
  } else if (OB_ISNULL(ha_service = MTL(ObStorageHAService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[VEC MIG] ha service is null", K(ret));
  } else if (FALSE_IT(window_mgr = &ha_service->get_vector_index_migration_window_mgr())) {
  } else {
    if (OB_FAIL(ObMigrationSlidingWindowSourceController::create(window_mgr, nullptr /*no callback while sliding window*/,
      share::ObDagPrio::DAG_PRIO_VECTOR_INDEX, src_controller_handle_))) {
      LOG_WARN("[VEC MIG] failed to create source controller", K(ret), K(processor_id));
    }
  }
  if (OB_SUCC(ret)) {
    processor_id_ = processor_id;
    ls_id_ = ls_id;
    data_tablet_id_ = data_tablet_id;
    segment_idx_ = segment_idx;
    dest_addr_ = dest_addr;
    tenant_id_ = tenant_id;
    adaptor_handle_id_ = adaptor_handle_id;
    dag_prio_ = share::ObDagPrio::DAG_PRIO_VECTOR_INDEX;
    set_state(ObMigrationVectorIndexProcessor::State::INIT);
    is_inited_ = true;
    LOG_INFO("[VEC MIG] processor init success", K(processor_id), K(ls_id),
        K(data_tablet_id), K(segment_idx), K(dest_addr), K(adaptor_handle_id));
  }
  return ret;
}

void ObMigrationVectorIndexProcessor::destroy()
{
  if (is_inited_) {
    LOG_INFO("[VEC MIG] processor destroy begin", K(processor_id_), K(ls_id_), K(data_tablet_id_),
        K(segment_idx_));
    // stop() wakes any blocked do_serialize() with OB_CANCELED; the source
    // controller ref is released here and will auto-cleanup when ref hits 0.
    src_controller_handle_->stop();
    src_controller_handle_->wakeup_waiters();
    src_controller_handle_.reset();
    adaptor_guard_.reset();
    is_inited_ = false;
    LOG_INFO("[VEC MIG] processor destroy done", K(processor_id_), K(ls_id_), K(data_tablet_id_));
  }
}

int ObMigrationVectorIndexProcessor::set_adaptor_from_guard(
    ObPluginVectorIndexAdapterGuard &guard)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[VEC MIG] processor not init when hold adaptor", K(ret));
  } else if (!guard.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[VEC MIG] invalid adaptor guard", K(ret));
  } else {
    ObPluginVectorIndexAdaptor *adaptor = guard.get_adatper();
    if (OB_ISNULL(adaptor)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[VEC MIG] adaptor is null", K(ret));
    } else if (OB_FAIL(adaptor_guard_.set_adapter(adaptor))) {
      LOG_WARN("[VEC MIG] failed to set adaptor in guard", K(ret), K(processor_id_));
    } else {
      LOG_INFO("[VEC MIG] processor hold adaptor success", K(processor_id_), K(data_tablet_id_));
    }
  }
  return ret;
}

// ======================== ObVectorIndexMigrationUtil ========================
int ObVectorIndexMigrationUtil::resolve_vec_idx_segment_for_migration(
    share::ObPluginVectorIndexAdaptor *adaptor,
    const int64_t segment_idx,
    share::ObVectorIndexSegmentHandle &out_handle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(adaptor)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[VEC MIG] adaptor is null", K(ret));
  } else {
    // segment_idx: bases_ then incrs_ in snap meta.
    share::ObVecIdxSnapshotDataHandle &snap = adaptor->get_snap_data();
    if (!snap.is_valid() || !snap->is_inited()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[VEC MIG] snap data is not available", K(ret));
    } else {
      {
        TCRLockGuard lock_guard(snap->mem_data_rwlock_);
        const int64_t n_base = snap->meta_.bases_.count();
        const int64_t n_incr = snap->meta_.incrs_.count();
        share::ObVectorIndexSegmentMeta *meta = nullptr;
        if (segment_idx < n_base) {
          meta = &snap->meta_.bases_.at(segment_idx);
        } else if (segment_idx - n_base < n_incr) {
          meta = &snap->meta_.incrs_.at(segment_idx - n_base);
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("[VEC MIG] segment idx out of range", K(ret), K(segment_idx), K(n_base), K(n_incr));
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(meta)) {
          if (!meta->segment_handle_.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[VEC MIG] snap segment handle is invalid", K(ret), K(segment_idx));
          } else {
            out_handle = meta->segment_handle_;
          }
        }
      }
    }
  }
  if (OB_SUCC(ret) && !out_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[VEC MIG] segment handle is invalid", K(ret), K(segment_idx));
  }
  return ret;
}

ERRSIM_POINT_DEF(EN_VEC_INDEX_MIGRATION_SRC_DO_SERIALIZE_FAILED);
int ObMigrationVectorIndexProcessor::do_serialize()
{
  int ret = OB_SUCCESS;
  const int64_t serialize_timeout_us = ObStorageHAUtils::get_rpc_timeout();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[VEC MIG] processor not init when do serialize", K(ret));
  } else if (is_failed()) {
    ret = get_fail_ret_code();
    LOG_WARN("[VEC MIG] processor already failed before do serialize", K(ret), K(processor_id_));
  } else {
#ifdef ERRSIM
    if (OB_FAIL(EN_VEC_INDEX_MIGRATION_SRC_DO_SERIALIZE_FAILED)) {
      LOG_INFO("[VEC MIG] errsim inject src serialize failure",
          K(ret), K(processor_id_), K(segment_idx_));
      SERVER_EVENT_ADD("storage_ha", "vec_mig_errsim_src_serialize",
          "tenant_id", MTL_ID(),
          "ls_id", ls_id_.id(),
          "data_tablet_id", data_tablet_id_.id(),
          "segment_idx", segment_idx_,
          "processor_id", processor_id_,
          "ret", ret);
    }
#endif
    if (OB_SUCC(ret)) {
      set_state(ObMigrationVectorIndexProcessor::State::SERIALIZING);
      LOG_INFO("[VEC MIG] do serialize begin", K(processor_id_), K(ls_id_),
               K(data_tablet_id_), K(segment_idx_));
    }

    ObPluginVectorIndexAdaptor *adaptor = adaptor_guard_.get_adatper();
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(adaptor)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[VEC MIG] adaptor is null during serialization", K(ret), K(processor_id_));
    } else {
      ObArenaAllocator alloc("VIMigSer");
      int64_t slot_cap = 0;
      char *slot_chunk = nullptr;
      if (OB_FAIL(src_controller_handle_->get_slot_buf_size(slot_cap))) {
        LOG_WARN("[VEC MIG] failed to get slot buf size from source controller", K(ret),
            K(processor_id_));
      } else if (OB_UNLIKELY(slot_cap < ObMigrationTenantWindowMgr::MIGRATION_WINDOW_DEFAULT_SLOT_BUF_SIZE)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected slot cap", K(ret), K(slot_cap));
      } else if (OB_ISNULL(slot_chunk = static_cast<char *>(alloc.alloc(slot_cap)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("[VEC MIG] alloc slot chunk buf fail", K(ret), K(slot_cap));
      } else {
        VecMigSerializeCbParam cb_param;
        cb_param.ctrl_ = src_controller_handle_.get();
        cb_param.timeout_us_ = serialize_timeout_us;
        cb_param.slot_cap_ = slot_cap;
        cb_param.slot_chunk_ = slot_chunk;
        cb_param.need_serde_meta_ = false;
        cb_param.processor_id_ = processor_id_;
        cb_param.header_tenant_id_ = tenant_id_;
        cb_param.ls_id_ = ls_id_;
        share::ObOStreamBuf::Callback serialize_cb(vec_mig_serialize_cb);
        share::ObVectorIndexSerializer serializer(alloc);
        const int64_t vsag_cap = ObMigrationTenantWindowMgr::MIGRATION_WINDOW_DEFAULT_SLOT_BUF_SIZE
                               - ObVecIdxMigChunkHeader::MAX_BYTES;
        if (OB_UNLIKELY(!cb_param.is_valid())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("[VEC MIG] invalid cb_param before serialize", K(ret), K(processor_id_),
              KP(cb_param.ctrl_), K(cb_param.timeout_us_), K(cb_param.slot_cap_),
              KP(cb_param.slot_chunk_), K(cb_param.processor_id_),
              K(cb_param.header_tenant_id_), K(cb_param.ls_id_));
        } else {
          share::ObVectorIndexSegmentHandle segment_handle;
          if (OB_FAIL(ObVectorIndexMigrationUtil::resolve_vec_idx_segment_for_migration(
                  adaptor, segment_idx_, segment_handle))) {
            LOG_WARN("[VEC MIG] failed to resolve segment for migration", K(ret), K(processor_id_),
                K(segment_idx_));
          } else if (!segment_handle.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[VEC MIG] segment handle is invalid", K(ret), K(processor_id_), K(segment_idx_));
          } else if (OB_FAIL(push_meta_chunks_(cb_param, segment_handle, alloc))) {
            LOG_WARN("[VEC MIG] failed to emit meta chunks", K(ret), K(processor_id_),
                K(segment_idx_));
          } else {
            LOG_INFO("[VEC MIG] begin segment serialization", K(processor_id_), K(segment_idx_), K(data_tablet_id_));
            if (OB_FAIL(serializer.serialize(segment_handle.get(), cb_param, serialize_cb,
                    adaptor->get_tenant_id(), vsag_cap))) {
              LOG_WARN("[VEC MIG] failed to serialize segment", K(ret), K(processor_id_), K(segment_idx_));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        // Set total chunk count so try_get_data ends with OB_ITER_END.
        if (OB_FAIL(src_controller_handle_->set_total_task_count(src_controller_handle_->get_next_generate_seq()))) {
          LOG_WARN("[VEC MIG] failed to set total task count on source controller", K(ret), K(processor_id_));
        } else {
          LOG_INFO("[VEC MIG] processor serialize complete", K(processor_id_), K(data_tablet_id_), K(segment_idx_),
              K(src_controller_handle_->get_next_generate_seq()));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    set_failed(ret);
  }
  return ret;
}

int ObMigrationVectorIndexProcessor::try_get_data(
    const int64_t seq_idx,
    char *out_buf,
    const int64_t out_buf_len,
    int64_t &data_len)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[VEC MIG] processor not init when try get data", K(ret));
  } else if (is_failed()) {
    ret = get_fail_ret_code();
    LOG_WARN("[VEC MIG] processor already failed, cannot try get data", K(ret), K(processor_id_), K(seq_idx));
  } else {
    if (OB_FAIL(src_controller_handle_->try_get_data(seq_idx, out_buf, out_buf_len, data_len))) {
      if (OB_EAGAIN == ret || OB_ITER_END == ret) {
      } else {
        LOG_WARN("[VEC MIG] failed to try get data from source controller", K(ret),
            K(seq_idx), K(processor_id_));
      }
    } else {
      update_last_access_time();
      LOG_DEBUG("try get data success", K(processor_id_), K(seq_idx), K(data_len));
    }
  }
  return ret;
}

void ObMigrationVectorIndexProcessor::set_state(
    const ObMigrationVectorIndexProcessor::State new_state)
{
  {
    common::ObSpinLockGuard g(state_lock_);
    state_ = new_state;
    last_access_time_ = ObTimeUtility::current_time();
  }
  LOG_INFO("[VEC MIG] processor state changed", K(processor_id_), K(data_tablet_id_),
      K(segment_idx_), K(new_state));
}

void ObMigrationVectorIndexProcessor::set_failed(const int ret_code)
{
  // First failure wins; stop() still unblocks waiters.
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(result_mgr_.set_result(static_cast<int32_t>(ret_code),
          false /*allow_retry*/, share::ObDagType::DAG_TYPE_VECTOR_INDEX_SERIALIZE))) {
    LOG_WARN_RET(tmp_ret, "failed to record result", K(ret_code), K(processor_id_));
  }
  // Always stop() (idempotent) to unblock any waiter in source controller.
  if (src_controller_handle_.is_valid()) {
    src_controller_handle_->stop();
  }
  LOG_WARN_RET(ret_code, "processor -> FAILED", K(processor_id_), K(ret_code),
      K(data_tablet_id_), K(segment_idx_));
}

void ObMigrationVectorIndexProcessor::update_last_access_time()
{
  common::ObSpinLockGuard g(state_lock_);
  last_access_time_ = ObTimeUtility::current_time();
}

bool ObMigrationVectorIndexProcessor::is_timeout(const int64_t timeout_us) const
{
  common::ObSpinLockGuard g(state_lock_);
  return ObTimeUtility::current_time() - last_access_time_ > timeout_us;
}

int ObMigrationVectorIndexProcessor::get_fail_ret_code() const
{
  int ret = OB_SUCCESS;
  int32_t result = OB_SUCCESS;
  if (OB_FAIL(result_mgr_.get_result(result))) {
    LOG_WARN("[VEC MIG] failed to get result from result_mgr", K(ret), K(processor_id_));
    result = static_cast<int32_t>(ret);
  }
  return static_cast<int>(result);
}

void ObMigrationVectorIndexProcessor::stop()
{
  if (is_inited_) {
    LOG_INFO("[VEC MIG] processor stop", K(processor_id_), K(data_tablet_id_),
        K(segment_idx_));
    if (src_controller_handle_.is_valid()) {
      src_controller_handle_->stop();
    }
  }
}

// ======================== ObVectorIndexMigrationProcessorMgr ========================

bool ObVectorIndexMigrationProcessorMgr::is_valid_processor_id(const int64_t processor_id)
{
  return processor_id >= FIRST_VALID_PROCESSOR_ID;
}

bool ObVectorIndexMigrationProcessorMgr::is_valid_adaptor_handle_id(const int64_t adaptor_handle_id)
{
  return adaptor_handle_id >= FIRST_VALID_ADAPTOR_HANDLE_ID;
}

ObVectorIndexMigrationProcessorMgr::ObVectorIndexMigrationProcessorMgr()
  : is_inited_(false),
    ls_(nullptr),
    next_processor_id_(FIRST_VALID_PROCESSOR_ID),
    next_adaptor_handle_id_(FIRST_VALID_ADAPTOR_HANDLE_ID),
    lock_(common::ObLatchIds::OB_STORAGE_HA_STRUCT_LOCK),
    adaptor_lock_(common::ObLatchIds::OB_STORAGE_HA_STRUCT_LOCK),
    processor_map_(),
    adaptor_guard_map_()
{
}

ObVectorIndexMigrationProcessorMgr::~ObVectorIndexMigrationProcessorMgr()
{
  destroy();
}

int ObVectorIndexMigrationProcessorMgr::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("[VEC MIG] processor mgr init twice", K(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[VEC MIG] invalid argument", K(ret), KP(ls));
  } else if (OB_FAIL(processor_map_.create(PROCESSOR_MAP_BUCKET_NUM, lib::ObMemAttr(MTL_ID(), "VIMigProcMap")))) {
    LOG_WARN("[VEC MIG] failed to create processor map", K(ret));
  } else if (OB_FAIL(adaptor_guard_map_.create(PROCESSOR_MAP_BUCKET_NUM, lib::ObMemAttr(MTL_ID(), "VIMigAdpMap")))) {
    LOG_WARN("[VEC MIG] failed to create adaptor guard map", K(ret));
  } else {
    ls_ = ls;
    next_processor_id_ = FIRST_VALID_PROCESSOR_ID;
    next_adaptor_handle_id_ = FIRST_VALID_ADAPTOR_HANDLE_ID;
    is_inited_ = true;
    LOG_INFO("[VEC MIG] processor mgr init success", "ls_id", ls->get_ls_id());
  }
  return ret;
}

void ObVectorIndexMigrationProcessorMgr::destroy()
{
  is_inited_ = false;
  ls_ = nullptr;
  if (processor_map_.created()) {
    for (ProcessorMapIter iter = processor_map_.begin();
          iter != processor_map_.end(); ++iter) {
      ObMigrationVectorIndexProcessor *processor = iter->second;
      if (OB_NOT_NULL(processor)) {
        processor->stop();
        dec_ref_(processor);
      }
    }
    processor_map_.destroy();
  }
  if (adaptor_guard_map_.created()) {
    for (AdaptorGuardMapIter iter = adaptor_guard_map_.begin();
          iter != adaptor_guard_map_.end(); ++iter) {
      if (OB_NOT_NULL(iter->second.guard_)) {
        OB_DELETE(ObPluginVectorIndexAdapterGuard, "VIMigAdpGrd", iter->second.guard_);
      }
    }
    adaptor_guard_map_.destroy();
  }

  LOG_INFO("[VEC MIG] processor mgr destroy done");
}

int ObVectorIndexMigrationProcessorMgr::register_processor(
    const ObTabletID &data_tablet_id,
    const int64_t segment_idx,
    const ObAddr &dest_addr,
    const int64_t adaptor_handle_id,
    int64_t &processor_id)
{
  int ret = OB_SUCCESS;
  processor_id = -1;
  ObMigrationVectorIndexProcessor *processor = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[VEC MIG] processor mgr not init", K(ret));
  } else if (!data_tablet_id.is_valid()
      || segment_idx < 0
      || !dest_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[VEC MIG] invalid argument", K(ret), K(data_tablet_id),
        K(segment_idx), K(dest_addr));
  } else if (OB_ISNULL(processor = OB_NEW(ObMigrationVectorIndexProcessor,
      lib::ObMemAttr(MTL_ID(), "VIMigProc")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[VEC MIG] failed to allocate memory for processor", K(ret));
  } else {
    int64_t new_id = ATOMIC_FAA(&next_processor_id_, 1);
    if (OB_FAIL(processor->init(new_id, ls_->get_ls_id(), data_tablet_id,
                                segment_idx, dest_addr, MTL_ID(),
                                adaptor_handle_id))) {
      LOG_WARN("[VEC MIG] failed to init processor", K(ret), K(new_id),
               K(adaptor_handle_id));
    } else {
      common::SpinWLockGuard guard(lock_);
      if (OB_UNLIKELY(!is_inited_)) {
        // destroy() ran between id alloc and here; must not publish into a
        // cleared map. The processor pointer is still owned by us and will
        // be freed via OB_DELETE in the dangling-processor branch.
        ret = OB_NOT_INIT;
        LOG_WARN("[VEC MIG] processor mgr destroyed before insert",
            K(ret), K(new_id));
      } else {
        // Mgr's own +1 for being in the map; each acquire_guard_ adds another.
        // Set ref BEFORE publishing the pointer via set_refactored so the
        // invariant "processor reachable through map => ref_count_ >= 1" always
        // holds. Otherwise any future lockless/R-locked reader doing
        // inc_ref/dec_ref could race a 0->1->0 and free the processor while we
        // still hold it.
        ATOMIC_STORE(&processor->ref_count_, static_cast<int64_t>(1));
        if (OB_FAIL(processor_map_.set_refactored(new_id, processor))) {
          LOG_WARN("[VEC MIG] failed to insert processor into map", K(ret), K(new_id));
          ATOMIC_STORE(&processor->ref_count_, static_cast<int64_t>(0));
        } else {
          processor_id = new_id;
          LOG_INFO("[VEC MIG] register processor success", K(new_id), K(data_tablet_id),
              K(segment_idx), K(dest_addr), K(adaptor_handle_id));
        }
      }
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(processor)) {
      OB_DELETE(ObMigrationVectorIndexProcessor, "VIMigProc", processor);
    }
  }
  return ret;
}

int ObVectorIndexMigrationProcessorMgr::mark_processor_failed(
    const int64_t processor_id,
    const int ret_code)
{
  int ret = OB_SUCCESS;
  ObMigrationVectorIndexProcessor *processor = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[VEC MIG] processor mgr not init when mark processor failed", K(ret));
  } else if (!is_valid_processor_id(processor_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[VEC MIG] invalid processor id", K(ret), K(processor_id));
  } else {
    {
      common::SpinRLockGuard guard(lock_);
      if (OB_FAIL(processor_map_.get_refactored(processor_id, processor))) {
        if (OB_HASH_NOT_EXIST == ret) {
          LOG_INFO("[VEC MIG] processor not found for mark failed, may already be released",
              K(ret), K(processor_id));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("[VEC MIG] failed to get processor for mark failed", K(ret), K(processor_id));
        }
      } else if (OB_ISNULL(processor)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[VEC MIG] processor is null when mark failed", K(ret), K(processor_id));
      } else {
        processor->set_failed(ret_code);
        LOG_INFO("[VEC MIG] mark processor failed done", K(processor_id), K(ret_code));
      }
    }
    // Release outside lock to avoid deadlock with the W-lock in release_processor.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(release_processor(processor_id))) {
      LOG_WARN("[VEC MIG] failed to release processor in mark processor failed", K(ret), K(processor_id));
    }
  }
  return ret;
}

void ObVectorIndexMigrationProcessorMgr::inc_ref_(ObMigrationVectorIndexProcessor *processor)
{
  if (OB_NOT_NULL(processor)) {
    const int64_t old_ref = ATOMIC_FAA(&processor->ref_count_, 1);
    if (OB_UNLIKELY(old_ref < 1)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "inc_ref on processor with invalid ref",
          K(old_ref), KP(processor));
    } else {
      processor->update_last_access_time();
    }
  }
}

void ObVectorIndexMigrationProcessorMgr::dec_ref_(ObMigrationVectorIndexProcessor *processor)
{
  if (OB_NOT_NULL(processor)) {
    const int64_t new_ref = ATOMIC_SAF(&processor->ref_count_, 1);
    if (0 == new_ref) {
      OB_DELETE(ObMigrationVectorIndexProcessor, "VIMigProc", processor);
    } else if (new_ref < 0) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "processor refcount underflow",
          K(new_ref), KP(processor));
    }
  }
}

int ObVectorIndexMigrationProcessorMgr::acquire_guard_(
    const int64_t processor_id,
    ObVectorIndexMigrationProcessorGuard &out_guard)
{
  int ret = OB_SUCCESS;
  ObMigrationVectorIndexProcessor *processor = nullptr;
  out_guard.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[VEC MIG] processor mgr not init", K(ret));
  } else if (!is_valid_processor_id(processor_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[VEC MIG] invalid processor id", K(ret), K(processor_id));
  } else {
    {
      common::SpinRLockGuard lock_guard(lock_);
      if (OB_FAIL(processor_map_.get_refactored(processor_id, processor))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("[VEC MIG] processor not found", K(ret), K(processor_id));
        } else {
          LOG_WARN("[VEC MIG] failed to get processor from map", K(ret), K(processor_id));
        }
      } else if (OB_ISNULL(processor)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[VEC MIG] processor is null", K(ret), K(processor_id));
      } else {
        // inc_ref is inside the R-lock critical section; release_processor
        // erases under W-lock, so the +1 we add can't race with a free.
        if (OB_FAIL(out_guard.set_processor(this, processor))) {
          LOG_WARN("[VEC MIG] failed to set processor on guard", K(ret), K(processor_id));
        }
      }
    }
  }
  return ret;
}

int ObVectorIndexMigrationProcessorMgr::release_processor(const int64_t processor_id)
{
  int ret = OB_SUCCESS;
  ObMigrationVectorIndexProcessor *processor = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[VEC MIG] processor mgr not init", K(ret));
  } else if (!is_valid_processor_id(processor_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[VEC MIG] invalid processor id", K(ret), K(processor_id));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(processor_map_.erase_refactored(processor_id, &processor))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("[VEC MIG] processor not found for release", K(ret), K(processor_id));
      } else {
        LOG_WARN("[VEC MIG] failed to erase processor from map", K(ret), K(processor_id));
      }
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(processor)) {
    // Free happens when the last ref drops — may be here, or later via the DAG's guard.
    processor->stop();
    dec_ref_(processor);
    LOG_INFO("[VEC MIG] release processor (mgr ref dropped)", K(processor_id));
  }
  return ret;
}

int ObVectorIndexMigrationProcessorMgr::setup_and_start_processor(
    const int64_t processor_id,
    ObPluginVectorIndexAdapterGuard &adaptor_guard,
    const int64_t segment_idx)
{
  int ret = OB_SUCCESS;
  ObVectorIndexMigrationProcessorGuard local_guard;
  ObVectorIndexSerializeDag *dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObPluginVectorIndexAdaptor *adaptor = nullptr;
  const share::ObDagPrio::ObDagPrioEnum dag_prio = share::ObDagPrio::DAG_PRIO_VECTOR_INDEX;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[VEC MIG] processor mgr not init", K(ret));
  } else if (!is_valid_processor_id(processor_id) || !adaptor_guard.is_valid() || segment_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[VEC MIG] invalid argument", K(ret), K(processor_id), K(segment_idx));
  } else if (OB_ISNULL(adaptor = adaptor_guard.get_adatper())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[VEC MIG] adaptor is null", K(ret));
  } else if (OB_FAIL(acquire_guard_(processor_id, local_guard))) {
    LOG_WARN("[VEC MIG] failed to acquire processor guard", K(ret), K(processor_id));
  } else if (ObMigrationVectorIndexProcessor::State::INIT != local_guard->get_state()) {
    // Check state BEFORE set_adaptor_from_guard. set_adapter() is set-once: a
    // duplicate setup call would otherwise fail at set_adaptor and incorrectly
    // mark the already-running processor as FAILED in the error rollback below.
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("[VEC MIG] processor state not INIT", K(ret), K(processor_id),
        "state", static_cast<int64_t>(local_guard->get_state()));
  } else if (OB_FAIL(local_guard->set_adaptor_from_guard(adaptor_guard))) {
    LOG_WARN("[VEC MIG] failed to set adaptor on processor", K(ret), K(processor_id));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[VEC MIG] tenant dag scheduler is null", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag_with_priority(dag_prio, dag))) {
    LOG_WARN("[VEC MIG] failed to alloc serialize dag", K(ret), K(processor_id), K(dag_prio));
  } else if (OB_FAIL(dag->init(local_guard))) {
    // dag copies local_guard — DAG holds its own +1, outlives local_guard.
    LOG_WARN("[VEC MIG] failed to init serialize dag", K(ret), K(processor_id));
  } else if (OB_FAIL(dag->create_first_task())) {
    LOG_WARN("[VEC MIG] failed to create first task", K(ret), K(processor_id));
  } else if (OB_FAIL(scheduler->add_dag(dag))) {
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("[VEC MIG] failed to add dag", K(ret), K(processor_id));
    }
  } else {
    dag = nullptr;  // ownership transferred to scheduler
    LOG_INFO("[VEC MIG] serialize dag scheduled", K(processor_id), K(segment_idx));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(dag) && OB_NOT_NULL(scheduler)) {
      // free_dag also drops the DAG's processor ref via processor_guard_.
      // scheduler null check is defensive: today dag != nullptr implies
      // scheduler != nullptr (dag can only come from alloc_dag), but making
      // the coupling explicit avoids a latent null-deref if alloc_dag's
      // failure contract ever changes.
      scheduler->free_dag(*dag);
      dag = nullptr;
    }
    if (local_guard.is_valid()) {
      // Drop the mgr's ref now; otherwise it sticks until the 30-min timeout
      // cleanup and pins sliding-window slots. local_guard still holds its +1
      // so the processor stays live until this function returns.
      local_guard->set_failed(ret);
      const int tmp_ret = release_processor(processor_id);
      if (OB_SUCCESS != tmp_ret && OB_ENTRY_NOT_EXIST != tmp_ret) {
        LOG_WARN("[VEC MIG] failed to release processor in setup rollback",
            K(tmp_ret), K(processor_id));
      }
    }
  }
  return ret;
}

int ObVectorIndexMigrationProcessorMgr::try_fetch_segment_data(const int64_t processor_id,
                                                    const int64_t seq_idx,
                                                    char *out_buf,
                                                    const int64_t out_buf_len,
                                                    int64_t &data_len) {
  int ret = OB_SUCCESS;
  ObVectorIndexMigrationProcessorGuard guard;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[VEC MIG] processor mgr not init", K(ret));
  } else if (!is_valid_processor_id(processor_id) || seq_idx < 0
             || OB_ISNULL(out_buf) || out_buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[VEC MIG] invalid argument", K(ret), K(processor_id), K(seq_idx), KP(out_buf), K(out_buf_len));
  } else if (OB_FAIL(acquire_guard_(processor_id, guard))) {
    LOG_WARN("[VEC MIG] failed to acquire processor guard", K(ret), K(processor_id));
  } else if (OB_FAIL(guard->try_get_data(seq_idx, out_buf, out_buf_len, data_len))) {
    if (OB_EAGAIN != ret && OB_ITER_END != ret) {
      LOG_WARN("[VEC MIG] failed to try get data", K(ret), K(processor_id), K(seq_idx));
    }
  }
  if (guard.is_valid()) {
    refresh_adaptor_hold_ts(guard->get_adaptor_handle_id());
  }
  return ret;
}

int ObVectorIndexMigrationProcessorMgr::cleanup_timeout_processors(
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> timeout_ids;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[VEC MIG] processor mgr not init", K(ret));
  } else {
    {
      common::SpinRLockGuard guard(lock_);
      for (ProcessorMapIter iter = processor_map_.begin();
           OB_SUCC(ret) && iter != processor_map_.end(); ++iter) {
        ObMigrationVectorIndexProcessor *processor = iter->second;
        if (OB_NOT_NULL(processor) && processor->is_timeout(timeout_us)) {
          if (OB_FAIL(timeout_ids.push_back(iter->first))) {
            LOG_WARN("[VEC MIG] failed to push back timeout id", K(ret));
          }
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < timeout_ids.count(); ++i) {
      const int64_t id = timeout_ids.at(i);
      int tmp_ret = release_processor(id);
      if (OB_SUCCESS == tmp_ret) {
        LOG_INFO("[VEC MIG] cleanup timeout processor", K(id), K(timeout_us));
      } else if (OB_ENTRY_NOT_EXIST == tmp_ret) {
      } else {
        LOG_WARN("[VEC MIG] failed to release timeout processor", K(tmp_ret), K(id));
      }
    }
  }
  return ret;
}

int ObVectorIndexMigrationProcessorMgr::cleanup_timeout_adaptor_handles(
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> timeout_ids;
  const int64_t now = ObTimeUtility::current_time();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[VEC MIG] processor mgr not init", K(ret));
  } else {
    {
      common::SpinRLockGuard guard(adaptor_lock_);
      for (AdaptorGuardMapIter iter = adaptor_guard_map_.begin();
           OB_SUCC(ret) && iter != adaptor_guard_map_.end(); ++iter) {
        if (now - iter->second.hold_ts_ > timeout_us) {
          if (OB_FAIL(timeout_ids.push_back(iter->first))) {
            LOG_WARN("[VEC MIG] failed to push back timeout adaptor handle id", K(ret));
          }
        }
      }
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < timeout_ids.count(); ++i) {
      const int64_t id = timeout_ids.at(i);
      int tmp_ret = release_adaptor(id);
      if (OB_SUCCESS == tmp_ret) {
        LOG_INFO("[VEC MIG] cleanup timeout adaptor handle", K(id), K(timeout_us));
      } else if (OB_HASH_NOT_EXIST == tmp_ret || OB_ENTRY_NOT_EXIST == tmp_ret) {
      } else {
        LOG_WARN("[VEC MIG] failed to release timeout adaptor handle", K(tmp_ret), K(id));
      }
    }
  }
  return ret;
}

int64_t ObVectorIndexMigrationProcessorMgr::get_processor_count() const
{
  common::SpinRLockGuard guard(lock_);
  return processor_map_.size();
}

int64_t ObVectorIndexMigrationProcessorMgr::get_adaptor_handle_count() const
{
  common::SpinRLockGuard guard(adaptor_lock_);
  return adaptor_guard_map_.size();
}

int ObVectorIndexMigrationProcessorMgr::hold_adaptor(
    share::ObPluginVectorIndexAdapterGuard &guard,
    int64_t &adaptor_handle_id)
{
  int ret = OB_SUCCESS;
  adaptor_handle_id = -1;
  share::ObPluginVectorIndexAdapterGuard *held_guard = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[VEC MIG] processor mgr not init", K(ret));
  } else if (!guard.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[VEC MIG] invalid guard", K(ret));
  } else if (OB_ISNULL(guard.get_adatper())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[VEC MIG] adaptor is null", K(ret));
  } else if (OB_ISNULL(held_guard = OB_NEW(share::ObPluginVectorIndexAdapterGuard,
                                            lib::ObMemAttr(MTL_ID(), "VIMigAdpGrd")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("[VEC MIG] failed to alloc adaptor guard", K(ret));
  } else if (OB_FAIL(held_guard->set_adapter(guard.get_adatper()))) {
    LOG_WARN("[VEC MIG] failed to set adaptor in guard", K(ret));
  } else {
    common::SpinWLockGuard wguard(adaptor_lock_);
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("[VEC MIG] processor mgr destroyed during hold_adaptor", K(ret));
    } else {
      const int64_t handle_id = next_adaptor_handle_id_++;
      VecMigAdaptorHoldEntry entry;
      entry.guard_ = held_guard;
      entry.hold_ts_ = ObTimeUtility::current_time();
      if (OB_FAIL(adaptor_guard_map_.set_refactored(handle_id, entry))) {
        LOG_WARN("[VEC MIG] failed to insert adaptor hold entry", K(ret), K(handle_id));
      } else {
        adaptor_handle_id = handle_id;
        held_guard = nullptr;  // ownership transferred to map
        LOG_INFO("[VEC MIG] hold adaptor success", K(handle_id),
            "inc_tablet_id", guard.get_adatper()->get_inc_tablet_id());
      }
    }
  }

  if (OB_NOT_NULL(held_guard)) {
    OB_DELETE(ObPluginVectorIndexAdapterGuard, "VIMigAdpGrd", held_guard);
  }
  return ret;
}

void ObVectorIndexMigrationProcessorMgr::refresh_adaptor_hold_ts(
    const int64_t adaptor_handle_id)
{
  if (is_inited_ && is_valid_adaptor_handle_id(adaptor_handle_id)) {
    common::SpinWLockGuard wguard(adaptor_lock_);
    VecMigAdaptorHoldEntry entry;
    if (OB_SUCCESS == adaptor_guard_map_.get_refactored(adaptor_handle_id, entry)) {
      entry.hold_ts_ = ObTimeUtility::current_time();
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(adaptor_guard_map_.set_refactored(
              adaptor_handle_id, entry, 1 /*overwrite*/))) {
        LOG_WARN_RET(tmp_ret, "[VEC MIG] failed to refresh adaptor hold ts",
            K(adaptor_handle_id));
      }
    }
  }
}

int ObVectorIndexMigrationProcessorMgr::get_held_adaptor(
    const int64_t adaptor_handle_id,
    share::ObPluginVectorIndexAdapterGuard &out_guard)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[VEC MIG] processor mgr not init", K(ret));
  } else if (!is_valid_adaptor_handle_id(adaptor_handle_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[VEC MIG] invalid adaptor handle id", K(ret));
  } else {
    common::SpinRLockGuard rguard(adaptor_lock_);
    VecMigAdaptorHoldEntry held_entry;
    if (OB_FAIL(adaptor_guard_map_.get_refactored(adaptor_handle_id, held_entry))) {
      LOG_WARN("[VEC MIG] adaptor handle not found", K(ret), K(adaptor_handle_id));
    } else if (OB_ISNULL(held_entry.guard_) || !held_entry.guard_->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[VEC MIG] held adaptor guard invalid", K(ret), K(adaptor_handle_id));
    } else if (OB_FAIL(out_guard.set_adapter(held_entry.guard_->get_adatper()))) {
      LOG_WARN("[VEC MIG] failed to set adaptor in out guard", K(ret), K(adaptor_handle_id));
    }
  }
  if (OB_SUCC(ret)) {
    refresh_adaptor_hold_ts(adaptor_handle_id);
  }
  return ret;
}

int ObVectorIndexMigrationProcessorMgr::release_adaptor(const int64_t adaptor_handle_id)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("[VEC MIG] processor mgr not init", K(ret));
  } else if (!is_valid_adaptor_handle_id(adaptor_handle_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[VEC MIG] invalid adaptor handle id", K(ret));
  } else {
    common::SpinWLockGuard wguard(adaptor_lock_);
    VecMigAdaptorHoldEntry held_entry;
    if (OB_FAIL(adaptor_guard_map_.get_refactored(adaptor_handle_id, held_entry))) {
      LOG_WARN("[VEC MIG] adaptor handle not found when release", K(ret), K(adaptor_handle_id));
    } else if (OB_FAIL(adaptor_guard_map_.erase_refactored(adaptor_handle_id))) {
      LOG_WARN("[VEC MIG] failed to erase adaptor guard", K(ret), K(adaptor_handle_id));
    } else {
      LOG_INFO("[VEC MIG] release adaptor success", K(adaptor_handle_id));
      OB_DELETE(ObPluginVectorIndexAdapterGuard, "VIMigAdpGrd", held_entry.guard_);
    }
  }
  return ret;
}
