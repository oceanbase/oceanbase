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

#define USING_LOG_PREFIX STORAGE

#include "storage/compaction/ob_medium_compaction_mgr.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "storage/tablet/ob_tablet.h"
#include "logservice/ob_log_base_header.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/multi_data_source/mds_writer.h"
#include "src/storage/tx/ob_trans_define.h"
#include "storage/tablet/ob_tablet_service_clog_replay_executor.h"

namespace oceanbase
{
using namespace storage;

namespace compaction
{
class ObTabletMediumClogReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObTabletMediumClogReplayExecutor(ObMediumCompactionInfo &medium_info);
  int init(const share::SCN &scn);
protected:
  bool is_replay_update_tablet_status_() const override
  {
    return false;
  }
  int do_replay_(ObTabletHandle &tablet_handle) override;
  virtual bool is_replay_update_mds_table_() const override
  {
    return true;
  }
private:
  ObMediumCompactionInfo &medium_info_;
  share::SCN scn_;
};

ObTabletMediumClogReplayExecutor::ObTabletMediumClogReplayExecutor(
    ObMediumCompactionInfo &medium_info)
    : medium_info_(medium_info),
      scn_()
{
}

int ObTabletMediumClogReplayExecutor::init(const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!medium_info_.is_valid() || !scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(medium_info_), K(scn));
  } else {
    scn_ = scn;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletMediumClogReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  mds::MdsCtx mds_ctx{mds::MdsWriter(mds::WriterType::MEDIUM_INFO)};
  if (OB_FAIL(replay_to_mds_table_(
    tablet_handle,
    ObMediumCompactionInfoKey(medium_info_.medium_snapshot_),
    medium_info_,
    mds_ctx,
    scn_))) {
    LOG_WARN("failed to replay to tablet", K(ret));
  } else {
    mds_ctx.single_log_commit(scn_, scn_);
  }
  return ret;
}

/*
 * ObTabletMediumCompactionInfoRecorder
 * */

ObTabletMediumCompactionInfoRecorder::ObTabletMediumCompactionInfoRecorder()
  : ObIStorageClogRecorder(),
    is_inited_(false),
    ignore_medium_(false),
    ls_id_(),
    tablet_id_(),
    tablet_handle_ptr_(nullptr),
    medium_info_(nullptr),
    allocator_(nullptr),
    mds_ctx_(nullptr)
{
#if defined(__x86_64__)
  STATIC_ASSERT(sizeof(ObTabletMediumCompactionInfoRecorder) <= 104, "size of medium recorder is oversize");
#endif
}

ObTabletMediumCompactionInfoRecorder::~ObTabletMediumCompactionInfoRecorder()
{
  destroy();
}

void ObTabletMediumCompactionInfoRecorder::destroy()
{
  is_inited_ = false;
  ignore_medium_ = false;
  ObIStorageClogRecorder::destroy();
  free_allocated_info();
  ls_id_.reset();
  tablet_id_.reset();
}

void ObTabletMediumCompactionInfoRecorder::reset()
{
  if (is_inited_) {
    ObIStorageClogRecorder::reset();
  }
}

int ObTabletMediumCompactionInfoRecorder::init(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t max_saved_version,
    logservice::ObLogHandler *log_handler)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(log_handler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(log_handler));
  } else if (OB_FAIL(ObIStorageClogRecorder::init(max_saved_version, log_handler))) {
    LOG_WARN("failed to init ObIStorageClogRecorder", K(ret), K(log_handler));
  } else {
    ignore_medium_ = tablet_id.is_special_merge_tablet();
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    is_inited_ = true;
    LOG_INFO("success to init medium clog recorder", K(ret), K_(ls_id), K_(tablet_id), K(max_saved_version));
  }
  return ret;
}
// this func is protected by lock in reserved_snapshot_map
int ObTabletMediumCompactionInfoRecorder::submit_medium_compaction_info(
    ObMediumCompactionInfo &medium_info,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  int64_t table_id = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (ignore_medium_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support to submit medium compaction clog", K(ret), K_(tablet_id));
  } else if (OB_UNLIKELY(!medium_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(medium_info));
  } else if (FALSE_IT(medium_info_ = &medium_info)) {
  } else if (OB_FAIL(try_update_for_leader(medium_info.medium_snapshot_, &allocator))) {
    LOG_WARN("failed to update for leader", K(ret), K(medium_info));
  }
  medium_info_ = nullptr;
  if (OB_ALLOCATE_MEMORY_FAILED == ret || OB_BLOCK_FROZEN == ret) {
    ret = OB_EAGAIN;
  }
  return ret;
}

void ObTabletMediumCompactionInfoRecorder::free_allocated_info()
{
  if (OB_NOT_NULL(allocator_)) {
    if (OB_NOT_NULL(logcb_ptr_)) {
      tablet_handle_ptr_->reset();
      tablet_handle_ptr_->~ObTabletHandle();
      allocator_->free(logcb_ptr_);
      logcb_ptr_ = nullptr;
      tablet_handle_ptr_ = nullptr;
      mds_ctx_ = nullptr;
    }
    allocator_ = nullptr;
  }
}

int ObTabletMediumCompactionInfoRecorder::replay_medium_compaction_log(
    const share::SCN &scn,
    const char *buf,
    const int64_t size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t update_version = OB_INVALID_VERSION;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (ignore_medium_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support to replay medium compaction clog", K(ret), K_(tablet_id));
  } else if (OB_FAIL(serialization::decode_i64(buf, size, pos, &update_version))) {
    LOG_WARN("fail to deserialize table_version", K(ret), K_(ls_id), K_(tablet_id));
  } else if (OB_FAIL(ObIStorageClogRecorder::replay_clog(update_version, scn, buf, size, pos))) {
    LOG_WARN("failed to replay clog", K(ret), K(scn), K_(ls_id), K_(tablet_id), K(update_version));
  }
  return ret;
}

int ObTabletMediumCompactionInfoRecorder::inner_replay_clog(
    const int64_t update_version,
    const share::SCN &scn,
    const char *buf,
    const int64_t size,
    int64_t &pos)
{
  UNUSED(update_version);
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  ObMediumCompactionInfo replay_medium_info;
  ObTabletHandle tmp_tablet_handle;
  if (OB_FAIL(replay_medium_info.deserialize(tmp_allocator, buf, size, pos))) {
    LOG_WARN("failed to deserialize medium compaction info", K(ret));
  } else if (!replay_medium_info.from_cur_cluster()
      && replay_medium_info.is_medium_compaction()) {
    // throw medium compaction clog from other cluster
  } else { // new mds path
    ObTabletMediumClogReplayExecutor replay_executor(replay_medium_info);
    if (OB_FAIL(replay_executor.init(scn))) {
      LOG_WARN("failed to init replay executor", K(ret), K(scn));
    } else if (OB_FAIL(replay_executor.execute(scn, ls_id_, tablet_id_))) {
      if (OB_TABLET_NOT_EXIST == ret || OB_NO_NEED_UPDATE == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to replay medium info", K(ret), K(replay_medium_info));
      }
    } else {
      FLOG_INFO("success to save medium info", K(ret), K_(tablet_id), K_(ls_id), K(replay_medium_info), K(max_saved_version_));
    }
  }

  tmp_tablet_handle.reset();
  return ret;
}

int ObTabletMediumCompactionInfoRecorder::sync_clog_succ_for_leader(const int64_t update_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(medium_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("medium info is invalid", K(ret), K_(clog_scn), KP_(medium_info));
  } else if (OB_UNLIKELY(medium_info_->medium_snapshot_ != update_version)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("medium snapshot not match", K(ret), KPC(medium_info_), K(update_version));
  } else if (OB_FAIL(submit_trans_on_mds_table(true/*is_commit*/))) {
    LOG_WARN("failed to dec ref on memtable", K(ret), K_(tablet_id), KPC(medium_info_));
  } else {
    FLOG_INFO("success to save medium info for leader", K(ret), K_(ls_id), K_(tablet_id), KPC(medium_info_),
        K(max_saved_version_), K_(clog_scn));
  }
  return ret;
}

void ObTabletMediumCompactionInfoRecorder::sync_clog_failed_for_leader()
{
  submit_trans_on_mds_table(false/*is_commit*/);
}

int ObTabletMediumCompactionInfoRecorder::submit_trans_on_mds_table(const bool is_commit)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == medium_info_
      || nullptr == tablet_handle_ptr_
      || !tablet_handle_ptr_->is_valid()
      || nullptr == mds_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("medium info or tablet handle is unexpected null", K(ret), K_(ls_id), K_(tablet_id),
        KP_(medium_info), K_(tablet_handle_ptr), KPC_(mds_ctx));
  } else if (is_commit) {
    mds_ctx_->single_log_commit(clog_scn_, clog_scn_);
  } else {
    mds_ctx_->single_log_abort();
  }
  return ret;
}

// log_header + tablet_id + medium_snapshot + medium_compaction_info
int ObTabletMediumCompactionInfoRecorder::prepare_struct_in_lock(
    int64_t &update_version,
    ObIAllocator *allocator,
    char *&clog_buf,
    int64_t &clog_len)
{
  UNUSED(update_version);
  int ret = OB_SUCCESS;
  clog_buf = nullptr;
  clog_len = 0;
  const logservice::ObLogBaseHeader log_header(
      logservice::ObLogBaseType::MEDIUM_COMPACTION_LOG_BASE_TYPE,
      logservice::ObReplayBarrierType::NO_NEED_BARRIER,
      tablet_id_.id());
  // record tablet_id as trans_id to make medium clogs of same tablet replay serially

  int64_t pos = 0;
  char *buf = nullptr;
  char *alloc_clog_buf = nullptr;
  int64_t alloc_buf_offset = 0;
  const int64_t buf_len = log_header.get_serialize_size()
      + tablet_id_.get_serialize_size()
      + serialization::encoded_length_i64(medium_info_->medium_snapshot_)
      + medium_info_->get_serialize_size();
  const int64_t alloc_buf_size = buf_len + sizeof(ObTabletHandle) + sizeof(ObStorageCLogCb) + sizeof(mds::MdsCtx);

  if (OB_UNLIKELY(nullptr == medium_info_ || nullptr == allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("medium_info or allocator is unexpected null", K(ret), KP_(medium_info), KP(allocator));
  } else if (buf_len >= common::OB_MAX_LOG_ALLOWED_SIZE) { // need be separated into several clogs
    ret = OB_ERR_DATA_TOO_LONG;
    LOG_WARN("medium info log too long", K(buf_len), LITERAL_K(common::OB_MAX_LOG_ALLOWED_SIZE));
  } else if (FALSE_IT(allocator_ = allocator)) {
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_->alloc(alloc_buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), KPC(medium_info_));
  } else {
    logcb_ptr_ = new(buf) ObStorageCLogCb(*this);
    alloc_buf_offset += sizeof(ObStorageCLogCb);
    tablet_handle_ptr_ = new (buf + alloc_buf_offset) ObTabletHandle();
    alloc_buf_offset += sizeof(ObTabletHandle);
    mds_ctx_ = new(buf + alloc_buf_offset) mds::MdsCtx(mds::MdsWriter(mds::WriterType::MEDIUM_INFO));
    alloc_buf_offset += sizeof(mds::MdsCtx);
    alloc_clog_buf = static_cast<char*>(buf) + alloc_buf_offset;
  }

  if (FAILEDx(get_tablet_handle(ls_id_, tablet_id_, *tablet_handle_ptr_))) {
    LOG_WARN("failed to get tablet handle", K(ret), K_(ls_id), K_(tablet_id));
  } else if (OB_FAIL(log_header.serialize(alloc_clog_buf, buf_len, pos))) {
    LOG_WARN("failed to serialize log header", K(ret));
  } else if (OB_FAIL(tablet_id_.serialize(alloc_clog_buf, buf_len, pos))) {
    LOG_WARN("fail to serialize tablet_id", K(ret), K_(tablet_id));
  } else if (OB_FAIL(serialization::encode_i64(alloc_clog_buf, buf_len, pos, medium_info_->medium_snapshot_))) {
    LOG_WARN("fail to serialize schema version", K(ret), K_(tablet_id));
  } else if (OB_FAIL(medium_info_->serialize(alloc_clog_buf, buf_len, pos))) {
    LOG_WARN("failed to serialize medium compaction info", K(ret), K(buf_len), K_(tablet_id), KPC(medium_info_));
  }
  if (OB_SUCC(ret)) {
    clog_buf = alloc_clog_buf;
    clog_len = pos;
  } else if (nullptr != buf && nullptr != allocator_) {
    free_allocated_info();
  }
  return ret;
}

int ObTabletMediumCompactionInfoRecorder::submit_log(
    const int64_t update_version,
    const char *clog_buf,
    const int64_t clog_len)
{
  UNUSED(update_version);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == medium_info_
      || nullptr == tablet_handle_ptr_
      || !tablet_handle_ptr_->is_valid()
      || nullptr == clog_buf
      || clog_len <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log handler or medium info is null", K(ret), KP(medium_info_),
        KP(clog_buf), K(clog_len), K(tablet_handle_ptr_));
  } else if (OB_FAIL(tablet_handle_ptr_->get_obj()->set(
      ObMediumCompactionInfoKey(medium_info_->medium_snapshot_),
      *medium_info_,
      *mds_ctx_))) {
    LOG_WARN("failed to save medium info on mds table", K(ret), K_(tablet_id), KPC(medium_info_));
  } else if (OB_FAIL(write_clog(clog_buf, clog_len))) {
    LOG_WARN("fail to submit log", K(ret), K_(tablet_id), K(medium_info_));
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(submit_trans_on_mds_table(false))) {
      LOG_ERROR("failed to dec ref on memtable", K(tmp_ret), K_(ls_id), K_(tablet_id));
    }
  } else {
    LOG_INFO("success to submit medium log", K(ret), K_(tablet_id), K(medium_info_), K_(clog_scn),
        "max_saved_version", get_max_saved_version());
  }

  return ret;
}

/*
 * ObMediumCompactionInfoList
 * */

ObMediumCompactionInfoList::ObMediumCompactionInfoList()
  : is_inited_(false),
    allocator_(nullptr),
    compat_(MEDIUM_LIST_VERSION),
    last_compaction_type_(0),
    wait_check_flag_(0),
    reserved_(0),
    last_medium_scn_(0)
{
}

ObMediumCompactionInfoList::~ObMediumCompactionInfoList()
{
  reset();
}

int ObMediumCompactionInfoList::init(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    compat_ = MEDIUM_LIST_VERSION;
    allocator_ = &allocator;
    is_inited_ = true;
  }
  return ret;
}

int ObMediumCompactionInfoList::init(
    common::ObIAllocator &allocator,
    const ObMediumCompactionInfoList *input_list)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(input_list)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(input_list));
  } else {
    allocator_ = &allocator;
    set_basic_info(*input_list);
    compat_ = MEDIUM_LIST_VERSION;
    is_inited_ = true;
  }
  return ret;
}

int ObMediumCompactionInfoList::init(
    common::ObIAllocator &allocator,
    const ObTaletExtraMediumInfo &extra_medium_info,
    const ObTabletDumpedMediumInfo &medium_info_list)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    allocator_ = &allocator;
    const common::ObIArray<ObMediumCompactionInfo*> &array = medium_info_list.medium_info_list_;
    for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); ++i) {
      const ObMediumCompactionInfo *src_medium_info = array.at(i);
      if (OB_ISNULL(src_medium_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, medium info is null", K(ret), K(i), KP(src_medium_info));
      } else {
        ObMediumCompactionInfo *medium_info = nullptr;
        void *buffer = allocator.alloc(sizeof(ObMediumCompactionInfo));
        if (OB_ISNULL(buffer)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory", K(ret), "size", sizeof(ObMediumCompactionInfo));
        } else {
          medium_info = new (buffer) ObMediumCompactionInfo();
          if (OB_FAIL(medium_info->init(allocator, *src_medium_info))) {
            LOG_WARN("failed to copy medium info", K(ret), KPC(src_medium_info));
          } else if (OB_UNLIKELY(!medium_info_list_.add_last(medium_info))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to add last", K(ret), KPC(medium_info));
          }

          if (OB_FAIL(ret)) {
            if (OB_NOT_NULL(medium_info)) {
              medium_info->~ObMediumCompactionInfo();
              allocator.free(medium_info);
            }
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      reset();
    } else {
      compat_ = MEDIUM_LIST_VERSION;
      last_compaction_type_ = extra_medium_info.last_compaction_type_;
      last_medium_scn_ = extra_medium_info.last_medium_scn_;
      wait_check_flag_ = extra_medium_info.wait_check_flag_;
      is_inited_ = true;
    }
  }

  return ret;
}

void ObMediumCompactionInfoList::reset_list()
{
  DLIST_REMOVE_ALL_NORET(info, medium_info_list_) {
    info->~ObMediumCompactionInfo();
    allocator_->free(info);
  }
  medium_info_list_.reset();
}

void ObMediumCompactionInfoList::reset()
{
  if (OB_NOT_NULL(allocator_)) {
    reset_list();
  }
  is_inited_ = false;
  info_ = 0;
  last_medium_scn_ = 0;
  allocator_ = nullptr;
}

int ObMediumCompactionInfoList::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  int64_t new_pos = pos;
  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("medium info list is invalid", K(ret), KPC(this));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, new_pos, info_))) {
    STORAGE_LOG(WARN, "failed to serialize info", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, new_pos, last_medium_scn_))) {
    STORAGE_LOG(WARN, "failed to serialize wait_check_medium_scn", K(ret), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, new_pos, medium_info_list_.get_size()))) {
    LOG_WARN("failed to serialize medium status", K(ret), K(buf_len));
  } else {
    DLIST_FOREACH_X(info, medium_info_list_, OB_SUCC(ret)) {
      if (OB_FAIL(static_cast<const ObMediumCompactionInfo *>(info)->serialize(buf, buf_len, new_pos))) {
        LOG_WARN("failed to serialize medium compaction info", K(ret), K(buf), K(buf_len), K(new_pos), KPC(info));
      } else {
        LOG_DEBUG("success to serialize medium info", K(ret), KPC(info));
      }
    }
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}

int ObMediumCompactionInfoList::deserialize(
    common::ObIAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int64_t deserialize_info = 0;
  int64_t list_count = 0;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(nullptr == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, new_pos, &deserialize_info))) {
    LOG_WARN("failed to deserialize cur medium snapshot", K(ret), K(data_len));
  } else if (0 == deserialize_info) {
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, new_pos, &list_count))) {
      LOG_WARN("failed to deserialize list count", K(ret), K(data_len));
    } else if (OB_UNLIKELY(0 != list_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("list count should be zero in old version medium list", K(ret), K(list_count));
    }
  } else if (FALSE_IT(info_ = deserialize_info)) {
  } else if (FALSE_IT(allocator_ = &allocator)) { // set allocator to call reset() when deserialize failed
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, new_pos, &last_medium_scn_))) {
    LOG_WARN("failed to deserialize wait_check_medium_scn", K(ret), K(data_len));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, new_pos, &list_count))) {
    LOG_WARN("failed to deserialize list count", K(ret), K(data_len));
  } else if (OB_UNLIKELY(list_count < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected list count", K(ret), K(list_count));
  } else if (list_count > 0) {
    void *alloc_buf = nullptr;
    for (int i = 0; OB_SUCC(ret) && i < list_count; ++i) {
      ObMediumCompactionInfo *new_info = nullptr;
      if (OB_ISNULL(alloc_buf = allocator.alloc(sizeof(ObMediumCompactionInfo)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory", K(ret));
      } else if (FALSE_IT(new_info = new (alloc_buf) ObMediumCompactionInfo())) {
      } else if (OB_FAIL(new_info->deserialize(allocator, buf, data_len, new_pos))) {
        LOG_WARN("failed to deserialize medium info", K(ret));
      } else if (!medium_info_list_.add_last(new_info)) {
        ret = OB_ERR_SYS;
        LOG_WARN("failed to add into medium info list", K(ret), KPC(new_info));
      } else {
        LOG_DEBUG("success to deserialize medium info", K(ret), K(new_info));
      }

      if (OB_FAIL(ret) && nullptr != new_info) {
        new_info->~ObMediumCompactionInfo();
        allocator.free(new_info);
        new_info = nullptr;
      }
    } // end of for
  }
  if (OB_FAIL(ret)) {
    reset();
  } else if (OB_UNLIKELY(!inner_is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("medium info list is invalid", K(ret), KPC(this));
  } else {
    compat_ = MEDIUM_LIST_VERSION;
    is_inited_ = true;
    pos = new_pos;
  }
  return ret;
}

int64_t ObMediumCompactionInfoList::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(info_);
  len += serialization::encoded_length_vi64(last_medium_scn_);
  len += serialization::encoded_length_vi64(medium_info_list_.get_size());
  DLIST_FOREACH_NORET(info, medium_info_list_){
    len += static_cast<const ObMediumCompactionInfo *>(info)->get_serialize_size();
  }
  return len;
}

void ObMediumCompactionInfoList::gene_info(
    char* buf, const int64_t buf_len, int64_t &pos) const
{
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_KV("size", size(), K_(last_compaction_type), K_(wait_check_flag), K_(last_medium_scn));
    J_COMMA();
    BUF_PRINTF("info_list");
    J_COLON();
    J_OBJ_START();
    if (size() > 0) {
      int i = 0;
      DLIST_FOREACH_NORET(info, medium_info_list_) {
        BUF_PRINTF("[%d]:", i++);
        static_cast<const ObMediumCompactionInfo *>(info)->gene_info(buf, buf_len, pos);
        if (i != size()) {
          BUF_PRINTF(";");
        }
      }
    } else {
      J_EMPTY_OBJ();
    }
    J_OBJ_END();
    J_OBJ_END();
  }
}

} //namespace compaction
} // namespace oceanbase
