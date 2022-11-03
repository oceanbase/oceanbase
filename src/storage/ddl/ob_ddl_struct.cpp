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

#include "ob_ddl_struct.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/blocksstable/ob_macro_block_struct.h"
#include "share/ob_force_print_log.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/compaction/ob_schedule_dag_func.h"

using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::clog;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObDDLMacroHandle::ObDDLMacroHandle()
  : block_id_()
{

}

ObDDLMacroHandle::ObDDLMacroHandle(const ObDDLMacroHandle &other)
{
  *this = other;
}

ObDDLMacroHandle &ObDDLMacroHandle::operator=(const ObDDLMacroHandle &other)
{
  if (&other != this) {
    (void)set_block_id(other.get_block_id());
  }
  return *this;
}

ObDDLMacroHandle::~ObDDLMacroHandle()
{
  reset_macro_block_ref();
}

int ObDDLMacroHandle::set_block_id(const blocksstable::MacroBlockId &block_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!block_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(reset_macro_block_ref())) {
    LOG_WARN("reset macro block reference failed", K(ret), K(block_id_));
  } else if (OB_FAIL(OB_SERVER_BLOCK_MGR.inc_ref(block_id))) {
    LOG_ERROR("failed to increase macro block ref cnt", K(ret), K(block_id));
  } else {
    block_id_ = block_id;
  }
  return ret;
}

int ObDDLMacroHandle::reset_macro_block_ref()
{
  int ret = OB_SUCCESS;
  if (block_id_.is_valid()) {
    if (OB_FAIL(OB_SERVER_BLOCK_MGR.dec_ref(block_id_))) {
      LOG_ERROR("failed to dec macro block ref cnt", K(ret), K(block_id_));
    } else {
      block_id_.reset();
    }
  }
  return ret;
}

ObDDLMacroBlock::ObDDLMacroBlock()
  : block_handle_(), logic_id_(), block_type_(DDL_MB_INVALID_TYPE), ddl_start_log_ts_(0), log_ts_(0), buf_(nullptr), size_(0)
{
}

ObDDLMacroBlock::~ObDDLMacroBlock()
{
}

int ObDDLMacroBlock::deep_copy(ObDDLMacroBlock &dst_block, common::ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  void *tmp_buf = nullptr;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(*this));
  } else if (OB_ISNULL(tmp_buf = allocator.alloc(size_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for macro block buffer failed", K(ret));
  } else {
    memcpy(tmp_buf, buf_, size_);
    dst_block.buf_ = reinterpret_cast<const char*>(tmp_buf);
    dst_block.size_ = size_;
    dst_block.block_type_ = block_type_;
    dst_block.block_handle_ = block_handle_;
    dst_block.logic_id_ = logic_id_;
    dst_block.ddl_start_log_ts_ = ddl_start_log_ts_;
    dst_block.log_ts_ = log_ts_;
  }
  return ret;
}

bool ObDDLMacroBlock::is_valid() const
{
  return block_handle_.get_block_id().is_valid()
    && logic_id_.is_valid()
    && DDL_MB_INVALID_TYPE != block_type_
    && ddl_start_log_ts_ > 0
    && log_ts_ > 0
    && nullptr != buf_
    && size_ > 0;
}


ObDDLKV::ObDDLKV()
  : is_inited_(false), ls_id_(), tablet_id_(), ddl_start_log_ts_(0), snapshot_version_(0),
    lock_(), allocator_("DDL_KV"), is_freezed_(false), is_closed_(false),
    last_freezed_log_ts_(0), min_log_ts_(INT64_MAX), max_log_ts_(0), freeze_log_ts_(INT64_MAX),
    pending_cnt_(0), cluster_version_(0), ref_cnt_(0),
    sstable_index_builder_(nullptr), index_block_rebuilder_(nullptr), is_rebuilder_closed_(false)
{
}

ObDDLKV::~ObDDLKV()
{
  destroy();
}

int ObDDLKV::init(const share::ObLSID &ls_id,
                  const common::ObTabletID &tablet_id,
                  const int64_t ddl_start_log_ts,
                  const int64_t snapshot_version,
                  const int64_t last_freezed_log_ts,
                  const int64_t cluster_version)

{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDDLKV has been inited twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid()
        || !tablet_id.is_valid()
        || ddl_start_log_ts <= 0
        || snapshot_version <= 0
        || last_freezed_log_ts <= 0
        || cluster_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(ddl_start_log_ts), K(snapshot_version), K(last_freezed_log_ts), K(cluster_version));
  } else {
    ObTabletDDLParam ddl_param;
    ddl_param.tenant_id_ = MTL_ID();
    ddl_param.ls_id_ = ls_id;
    ddl_param.table_key_.tablet_id_ = tablet_id;
    ddl_param.table_key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
    ddl_param.table_key_.version_range_.base_version_ = 0;
    ddl_param.table_key_.version_range_.snapshot_version_ = snapshot_version;
    ddl_param.start_log_ts_ = ddl_start_log_ts;
    ddl_param.snapshot_version_ = snapshot_version;
    ddl_param.cluster_version_ = cluster_version;
    if (OB_FAIL(ObTabletDDLUtil::prepare_index_builder(ddl_param, allocator_, sstable_index_builder_, index_block_rebuilder_))) {
      LOG_WARN("prepare index builder failed", K(ret));
    } else {
      ls_id_ = ls_id;
      tablet_id_ = tablet_id;
      ddl_start_log_ts_ = ddl_start_log_ts;
      snapshot_version_ = snapshot_version;
      last_freezed_log_ts_ = last_freezed_log_ts;
      cluster_version_ = cluster_version;
      is_inited_ = true;
      LOG_INFO("ddl kv init success", K(ls_id_), K(tablet_id_), K(ddl_start_log_ts_), K(snapshot_version_), K(last_freezed_log_ts_), K(cluster_version_));
    }
  }
  return ret;
}

void ObDDLKV::destroy()
{
  if (nullptr != index_block_rebuilder_) {
    index_block_rebuilder_->~ObIndexBlockRebuilder();
    allocator_.free(index_block_rebuilder_);
    index_block_rebuilder_ = nullptr;
  }
  if (nullptr != sstable_index_builder_) {
    sstable_index_builder_->~ObSSTableIndexBuilder();
    allocator_.free(sstable_index_builder_);
    sstable_index_builder_ = nullptr;
  }
  allocator_.reset();
}

int ObDDLKV::set_macro_block(const ObDDLMacroBlock &macro_block)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_DDL_BLOCK_COUNT = 10L * 1024L * 1024L * 1024L / OB_SERVER_BLOCK_MGR.get_macro_block_size();
  int64_t freeze_block_count = MAX_DDL_BLOCK_COUNT;
#ifdef ERRSIM
  if (0 != GCONF.errsim_max_ddl_block_count) {
    freeze_block_count = GCONF.errsim_max_ddl_block_count;
    LOG_INFO("ddl set macro block count", K(freeze_block_count));
  }
#endif
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl kv is not init", K(ret));
  } else if (OB_UNLIKELY(!macro_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_block));
  } else {
    const uint64_t tenant_id = MTL_ID();
    ObUnitInfoGetter::ObTenantConfig unit;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(GCTX.omt_->get_tenant_unit(tenant_id, unit))) {
      LOG_WARN("get tenant unit failed", K(tmp_ret), K(tenant_id));
    } else {
      const int64_t log_allowed_block_count = unit.config_.log_disk_size() * 0.2 / OB_SERVER_BLOCK_MGR.get_macro_block_size();
      if (log_allowed_block_count <= 0) {
        tmp_ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid macro block count by log disk size", K(tmp_ret), K(tenant_id), K(unit.config_));
      } else {
        freeze_block_count = min(freeze_block_count, log_allowed_block_count);
      }
    }
  }
  if (OB_SUCC(ret) && ddl_blocks_.count() >= freeze_block_count) {
    ObDDLTableMergeDagParam param;
    param.ls_id_ = ls_id_;
    param.tablet_id_ = tablet_id_;
    param.start_log_ts_ = ddl_start_log_ts_;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(param))) {
      LOG_WARN("try schedule ddl merge dag failed when ddl kv is full ",
          K(tmp_ret), K(ls_id_), K(tablet_id_), K(ddl_blocks_.count()));
    }
  }
  if (OB_SUCC(ret)) {
    ObDataMacroBlockMeta *data_macro_meta = nullptr;
    ObArenaAllocator meta_allocator;
    TCWLockGuard guard(lock_);
    if (macro_block.ddl_start_log_ts_ != ddl_start_log_ts_) {
      if (macro_block.ddl_start_log_ts_ > ddl_start_log_ts_) {
        ret = OB_EAGAIN;
        LOG_INFO("ddl start log ts too large, retry", K(ret),
            K(ls_id_), K(tablet_id_), K(ddl_start_log_ts_), K(macro_block));
      } else {
        // filter out and do nothing
        LOG_INFO("ddl start log ts too small, maybe from old build task, ignore", K(ret),
            K(ls_id_), K(tablet_id_), K(ddl_start_log_ts_), K(macro_block));
      }
    } else if (macro_block.log_ts_ > freeze_log_ts_) {
      ret = OB_EAGAIN;
      LOG_INFO("this ddl kv is freezed, retry other ddl kv", K(ret), K(ls_id_), K(tablet_id_), K(macro_block), K(freeze_log_ts_));
    } else if (OB_FAIL(index_block_rebuilder_->append_macro_row(
            macro_block.buf_, macro_block.size_, macro_block.get_block_id()))) {
      LOG_WARN("append macro meta failed", K(ret), K(macro_block));
    } else if (OB_FAIL(ddl_blocks_.push_back(macro_block.block_handle_))) {
      LOG_WARN("push back block handle failed", K(ret), K(macro_block.block_handle_));
    } else {
      min_log_ts_ = MIN(min_log_ts_, macro_block.log_ts_);
      max_log_ts_ = MAX(max_log_ts_, macro_block.log_ts_);
      LOG_INFO("succeed to set macro block into ddl kv", K(macro_block));
    }
  }
  return ret;
}

int ObDDLKV::freeze(const int64_t freeze_log_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl kv is not init", K(ret));
  } else {
    TCWLockGuard guard(lock_);
    if (is_freezed_) {
      // do nothing
    } else {
      if (freeze_log_ts > 0) {
        freeze_log_ts_ = freeze_log_ts;
      } else if (max_log_ts_ > 0) {
        freeze_log_ts_ = max_log_ts_;
      } else {
        ret = OB_EAGAIN;
        LOG_INFO("ddl kv not freezed, try again", K(ret), K(ls_id_), K(tablet_id_), K(ddl_blocks_.count()));
      }
      if (OB_SUCC(ret)) {
        ATOMIC_SET(&is_freezed_, true);
        LOG_INFO("ddl kv freezed", K(ret), K(ls_id_), K(tablet_id_), K(ddl_blocks_.count()));
      }
    }
  }
  return ret;
}

int ObDDLKV::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl kv is not init", K(ret));
  } else if (is_closed_) {
    // do nothing
    LOG_INFO("ddl kv already closed", K(*this));
  } else if (OB_FAIL(wait_pending())) {
    LOG_WARN("wait pending failed", K(ret));
  } else if (!is_rebuilder_closed_) {
    if (OB_FAIL(index_block_rebuilder_->close())) {
      LOG_WARN("index block rebuilder close failed", K(ret));
    } else {
      is_rebuilder_closed_ = true;
    }
  }
  if (OB_SUCC(ret) && !is_closed_) {
    ObTableHandleV2 table_handle;
    ObTabletDDLParam ddl_param;
    ddl_param.tenant_id_ = MTL_ID();
    ddl_param.ls_id_ = ls_id_;
    ddl_param.table_key_.tablet_id_ = tablet_id_;
    ddl_param.table_key_.table_type_ = ObITable::TableType::KV_DUMP_SSTABLE;
    ddl_param.table_key_.log_ts_range_.start_log_ts_ = last_freezed_log_ts_;
    ddl_param.table_key_.log_ts_range_.end_log_ts_ = freeze_log_ts_;
    ddl_param.start_log_ts_ = ddl_start_log_ts_;
    ddl_param.snapshot_version_ = snapshot_version_;
    ddl_param.cluster_version_ = cluster_version_;
    if (OB_FAIL(ObTabletDDLUtil::create_ddl_sstable(sstable_index_builder_, ddl_param, table_handle))) {
      LOG_WARN("create ddl sstable failed", K(ret));
    } else {
      is_closed_ = true;
      LOG_INFO("ddl kv closed success", K(*this));
    }
  }
  return ret;
}

void ObDDLKV::inc_pending_cnt()
{
  ATOMIC_INC(&pending_cnt_);
}

void ObDDLKV::dec_pending_cnt()
{
  ATOMIC_DEC(&pending_cnt_);
}

int ObDDLKV::wait_pending()
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_freezed())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ddl kv not freezed", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("get ls handle failed", K(ret), K(ls_id_));
  } else {
    int64_t max_decided_log_ts = 0;
    bool wait_ls_ts = true;
    bool wait_ddl_redo = true;
    const int64_t abs_timeout_ts = ObTimeUtility::fast_current_time() + 1000L * 1000L * 10L;
    while (OB_SUCC(ret)) {
      if (wait_ls_ts) {
        if (OB_FAIL(ls_handle.get_ls()->get_max_decided_log_ts_ns(max_decided_log_ts))) {
          LOG_WARN("get max decided log ts failed", K(ret), K(ls_id_));
        } else {
          // max_decided_log_ts is the left border log_ts - 1
          wait_ls_ts = max_decided_log_ts + 1 < freeze_log_ts_;
        }
      }
      if (OB_SUCC(ret)) {
        if (!wait_ls_ts) {
          wait_ddl_redo = is_pending();
        }
        if (wait_ls_ts || wait_ddl_redo) {
          if (ObTimeUtility::fast_current_time() > abs_timeout_ts) {
            ret = OB_TIMEOUT;
            LOG_WARN("wait pending ddl kv timeout", K(ret), K(*this), K(max_decided_log_ts), K(wait_ls_ts), K(wait_ddl_redo));
          } else {
            ob_usleep(1L * 1000L); // 1 ms
          }
          if (REACH_TIME_INTERVAL(1000L * 1000L * 1L)) {
            LOG_INFO("wait pending ddl kv", K(ret), K(*this));
          }
        } else {
          break;
        }
      }
    }
  }
  return ret;
}

ObDDLKVHandle::ObDDLKVHandle()
  : kv_(nullptr)
{
}

ObDDLKVHandle::~ObDDLKVHandle()
{
  reset();
}

int ObDDLKVHandle::set_ddl_kv(ObDDLKV *kv)
{
  int ret = OB_SUCCESS;
  if (nullptr != kv_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDDLKVHandle cannot be inited twice", K(ret));
  } else if (OB_ISNULL(kv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(kv));
  } else {
    kv->inc_ref();
    kv_ = kv;
  }
  return ret;
}

int ObDDLKVHandle::get_ddl_kv(ObDDLKV *&kv)
{
  int ret = OB_SUCCESS;
  kv = nullptr;
  if (!is_valid()) {
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    kv = kv_;
  }
  return ret;
}

void ObDDLKVHandle::reset()
{
  if (nullptr != kv_) {
    if (0 == kv_->dec_ref()) {
      op_free(kv_);
      STORAGE_LOG(INFO, "free a ddl kv", KP(kv_));
    }
    kv_ = nullptr;
  }
}

ObDDLKVsHandle::ObDDLKVsHandle()
  : kv_array_()
{
}

ObDDLKVsHandle::~ObDDLKVsHandle()
{
  reset();
}

void ObDDLKVsHandle::reset()
{
  for (int64_t i = 0; i < kv_array_.count(); ++i) {
    ObDDLKV *kv = kv_array_.at(i);
    if (nullptr != kv) {
      if (0 == kv->dec_ref()) {
        op_free(kv);
        STORAGE_LOG(INFO, "free a ddl kv");
      }
      kv_array_.at(i) = nullptr;
    }
  }
  kv_array_.reset();
}

int ObDDLKVsHandle::add_ddl_kv(ObDDLKV *kv)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(kv)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(kv));
  } else if (OB_FAIL(kv_array_.push_back(kv))) {
    LOG_WARN("fail to push back kv array", K(ret));
  } else {
    kv->inc_ref();
  }
  return ret;
}

int ObDDLKVsHandle::get_ddl_kv(const int64_t idx, ObDDLKV *&kv)
{
  int ret = OB_SUCCESS;
  if (idx >= kv_array_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(idx), K(kv_array_.count()));
  } else {
    kv = kv_array_.at(idx);
  }
  return ret;
}

ObDDLKVPendingGuard::ObDDLKVPendingGuard(ObTablet *tablet, const int64_t log_ts)
  : tablet_(tablet), log_ts_(log_ts), kv_handle_(), ret_(OB_SUCCESS)
{
  int ret = OB_SUCCESS;
  ObDDLKV *curr_kv = nullptr;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  if (OB_UNLIKELY(nullptr == tablet || log_ts <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(tablet), K(log_ts));
  } else if (OB_FAIL(tablet->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    LOG_WARN("get ddl kv mgr failed", K(ret));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_or_create_ddl_kv(log_ts, kv_handle_))) {
    LOG_WARN("acquire ddl kv failed", K(ret));
  } else if (OB_FAIL(kv_handle_.get_ddl_kv(curr_kv))) {
    LOG_WARN("fail to get ddl kv", K(ret));
  } else if (OB_ISNULL(curr_kv)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, active ddl kv must not be nullptr", K(ret));
  } else {
    curr_kv->inc_pending_cnt();
  }
  if (OB_FAIL(ret)) {
    kv_handle_.reset();
    ret_ = ret;
  }
}

int ObDDLKVPendingGuard::get_ddl_kv(ObDDLKV *&kv)
{
  int ret = OB_SUCCESS;
  kv = nullptr;
  if (OB_FAIL(ret_)) {
    // do nothing
  } else if (OB_FAIL(kv_handle_.get_ddl_kv(kv))) {
    LOG_WARN("fail to get ddl kv", K(ret));
  }
  return ret;
}

ObDDLKVPendingGuard::~ObDDLKVPendingGuard()
{
  int ret = OB_SUCCESS;
  if (OB_SUCCESS == ret_) {
    ObDDLKV *curr_kv = nullptr;
    if (OB_FAIL(kv_handle_.get_ddl_kv(curr_kv))) {
      LOG_ERROR("error unexpected, can not get ddl kv", K(ret));
    } else if (nullptr != curr_kv) {
      curr_kv->dec_pending_cnt();
    }
  }
  kv_handle_.reset();
}

int ObDDLKVPendingGuard::set_macro_block(ObTablet *tablet, const ObDDLMacroBlock &macro_block)
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_RETRY_COUNT = 10;
  if (OB_UNLIKELY(nullptr == tablet || !macro_block.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(tablet), K(macro_block));
  } else {
    int64_t try_count = 0;
    while ((OB_SUCCESS == ret || OB_EAGAIN == ret) && try_count < MAX_RETRY_COUNT) {
      ObDDLKV *ddl_kv = nullptr;
      ObDDLKVPendingGuard guard(tablet, macro_block.log_ts_);
      if (OB_FAIL(guard.get_ddl_kv(ddl_kv))) {
        LOG_WARN("get ddl kv failed", K(ret));
      } else if (OB_FAIL(ddl_kv->set_macro_block(macro_block))) {
        LOG_WARN("fail to set macro block info", K(ret));
      } else {
        break;
      }
      if (OB_EAGAIN == ret) {
        ++try_count;
        LOG_WARN("retry get ddl kv and set macro block", K(try_count));
      }
    }
  }
  return ret;
}
