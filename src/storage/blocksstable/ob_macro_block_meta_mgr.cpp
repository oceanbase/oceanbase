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

#include "ob_macro_block_meta_mgr.h"
#include "share/ob_thread_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;

namespace oceanbase {
namespace blocksstable {
/**
 * ----------------------------------------------ObMacroBlockMetaEntry-------------------------------------------------
 */
OB_SERIALIZE_MEMBER(ObMacroBlockMetaLogEntry, data_file_id_, disk_no_, block_index_, meta_ptr_);

bool ObMacroBlockMetaLogEntry::is_valid() const
{
  return meta_ptr_.is_valid();
}

int64_t ObMacroBlockMetaLogEntry::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  common::databuff_printf(buf, buf_len, pos, "\t|\tdata_file_id_=[%ld]\n", data_file_id_);
  common::databuff_printf(buf, buf_len, pos, "\t|\tdisk_no_=[%ld]\n", disk_no_);
  common::databuff_printf(buf, buf_len, pos, "\t|\tblock_index_[%ld]\n", block_index_);

  const ObMacroBlockMeta* macroBlockMeta = &meta_ptr_;
  common::databuff_printf(buf, buf_len, pos, "\t|\t+---------------ObMacroBlockMeta-start---------------+\n");
  common::databuff_printf(buf, buf_len, pos, "\t|\t| attr=[%d]\n", macroBlockMeta->attr_);
  if (macroBlockMeta->is_data_block()) {
    common::databuff_printf(buf, buf_len, pos, "\t|\t| data_version_=[%lu]\n", macroBlockMeta->data_version_);
    common::databuff_printf(
        buf, buf_len, pos, "\t|\t| major_version=[%lu]\n", ObVersion::get_major(macroBlockMeta->data_version_));
    common::databuff_printf(
        buf, buf_len, pos, "\t|\t| minor_version=[%lu]\n", ObVersion::get_minor(macroBlockMeta->data_version_));

    common::databuff_printf(buf, buf_len, pos, "\t|\t| column_number_[%d]\n", macroBlockMeta->column_number_);
    common::databuff_printf(
        buf, buf_len, pos, "\t|\t| rowkey_column_number=[%d]\n", macroBlockMeta->rowkey_column_number_);
    common::databuff_printf(
        buf, buf_len, pos, "\t|\t| column_index_scale_=[%d]\n", macroBlockMeta->column_index_scale_);
    common::databuff_printf(buf, buf_len, pos, "\t|\t| row_store_type=[%d]\n", macroBlockMeta->row_store_type_);
    common::databuff_printf(buf, buf_len, pos, "\t|\t| row_count_=[%d]\n", macroBlockMeta->row_count_);
    common::databuff_printf(buf, buf_len, pos, "\t|\t| occupy_size_=[%d]\n", macroBlockMeta->occupy_size_);
    common::databuff_printf(buf, buf_len, pos, "\t|\t| data_checksum_=[%ld]\n", macroBlockMeta->data_checksum_);
    common::databuff_printf(buf, buf_len, pos, "\t|\t| micro_block_count_=[%d]\n", macroBlockMeta->micro_block_count_);
    common::databuff_printf(
        buf, buf_len, pos, "\t|\t| micro_block_data_offet_=[%d]\n", macroBlockMeta->micro_block_data_offset_);
    common::databuff_printf(
        buf, buf_len, pos, "\t|\t| micro_block_index_offset_=[%d]\n", macroBlockMeta->micro_block_index_offset_);
    common::databuff_printf(
        buf, buf_len, pos, "\t|\t| micro_block_endKey_offset_=[%d]\n", macroBlockMeta->micro_block_endkey_offset_);
    common::databuff_printf(buf, buf_len, pos, "\t|\t| compreessor_=[%s]\n", macroBlockMeta->compressor_);

    common::databuff_printf(buf, buf_len, pos, "\t|\t|\t+-----------column-start-----------+\n");
    for (int i = 0; i < macroBlockMeta->column_number_; ++i) {
      common::databuff_printf(buf,
          buf_len,
          pos,
          "\t|\t|\t| i=[%d], column_id_=[%d], "
          "column_type=[type:%d,%s;collation:%d], column_checksum_=[%ld]\n",
          i,
          macroBlockMeta->column_id_array_[i],
          macroBlockMeta->column_type_array_[i].get_type(),
          ob_obj_type_str(static_cast<ObObjType>(macroBlockMeta->column_type_array_[i].get_type())),
          macroBlockMeta->column_type_array_[i].get_collation_type(),
          macroBlockMeta->column_checksum_[i]);
    }

    common::databuff_printf(buf, buf_len, pos, "\t|\t|\t+-----------column-end-------------+\n");

    common::databuff_printf(buf, buf_len, pos, "\t|\t|\t+--------endkey-start--------+\n");
    // const int buf_len = 2000;
    // char buf[buf_len];
    // int64_t len = 0;
    // int64_t pos = 0;
    common::databuff_printf(buf, buf_len, pos, "\t|\t|\t| ");
    for (int32_t i = 0; i < macroBlockMeta->rowkey_column_number_; ++i) {
      common::databuff_printf(buf, buf_len, pos, "column[%d]=", i);
      pos += macroBlockMeta->endkey_[i].to_string(buf + pos, buf_len - pos);
      common::databuff_printf(buf, buf_len, pos, ", ");
    }
    common::databuff_printf(buf, buf_len, pos, "\n");

    // common::databuff_printf(buf, buf_len, pos, "\t|\t|\t| endkey_[%d]=%s\n",
    // i,macroBlockMeta->endkey_[i].to_string(buf, buf_len));
    common::databuff_printf(buf, buf_len, pos, "\t|\t|\t+--------endkey-end----------+\n");

  } else {
    common::databuff_printf(
        buf, buf_len, pos, "\t|\t| previous_block_index_=[%ld]\n", macroBlockMeta->previous_block_index_);
  }

  common::databuff_printf(buf, buf_len, pos, "\t|\t+---------------ObMacroBlockMeta-end-----------------+\n");
  return pos;
}

/**
 * ---------------------ObMacroBlockMetaHandle----------------------------
 */
ObMacroBlockMetaHandle::ObMacroBlockMetaHandle() : meta_ctrl_(NULL)
{}

ObMacroBlockMetaHandle::~ObMacroBlockMetaHandle()
{
  reset();
}

ObMacroBlockMetaHandle::ObMacroBlockMetaHandle(const ObMacroBlockMetaHandle& other) : meta_ctrl_(NULL)
{
  *this = other;
}

ObMacroBlockMetaHandle& ObMacroBlockMetaHandle::operator=(const ObMacroBlockMetaHandle& other)
{
  if (&other != this) {
    reset();
    meta_ctrl_ = other.meta_ctrl_;
    if (NULL != meta_ctrl_) {
      ObMacroBlockMetaMgr::get_instance().inc_ref(*meta_ctrl_);
    }
  }
  return *this;
}

void ObMacroBlockMetaHandle::reset()
{
  if (NULL != meta_ctrl_) {
    ObMacroBlockMetaMgr::get_instance().dec_ref(*meta_ctrl_);
    meta_ctrl_ = NULL;
  }
}

ObMacroBlockMetaMgrGCTask::ObMacroBlockMetaMgrGCTask()
{}

ObMacroBlockMetaMgrGCTask::~ObMacroBlockMetaMgrGCTask()
{}

void ObMacroBlockMetaMgrGCTask::runTimerTask()
{
  ObMacroBlockMetaMgr::get_instance().gc();
}

/**
 * ------------------------ObMacroBlockMetaMgr-----------------------------
 */
ObMacroBlockMetaMgr::ObMacroBlockMetaMgr()
    : max_block_cnt_(0),
      buckets_cnt_(0),
      meta_buckets_(NULL),
      buckets_lock_(),
      allocator_(ObModIds::OB_MACRO_BLOCK_META),
      meta_allocator_(),
      node_queue_(),
      ctrl_queue_(),
      gc_task_(),
      is_inited_(false)
{}

ObMacroBlockMetaMgr::~ObMacroBlockMetaMgr()
{
  destroy();
}

ObMacroBlockMetaMgr& ObMacroBlockMetaMgr::get_instance()
{
  static ObMacroBlockMetaMgr instance_;
  return instance_;
}

int ObMacroBlockMetaMgr::init(const int64_t max_block_cnt)
{
  int ret = OB_SUCCESS;
  int64_t ctrl_cnt = max_block_cnt * 4;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObMacroBlockMetaMgr has been inited, ", K(ret));
  } else if (OB_UNLIKELY(max_block_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(max_block_cnt), K(ret));
  } else if (OB_FAIL(SLOGGER.register_redo_module(OB_REDO_LOG_MACROBLOCK, this))) {
    STORAGE_LOG(WARN, "Fail to register ObMacroBlockMetaMgr, ", K(ret));
  } else if (OB_UNLIKELY((buckets_cnt_ = cal_next_prime(max_block_cnt)) <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Invalid prime buckets cnt, ", K(ret), K_(buckets_cnt));
  } else if (OB_FAIL(buckets_lock_.init(static_cast<uint64_t>(buckets_cnt_)))) {
    STORAGE_LOG(WARN, "Fail to init buckets lock, ", K(ret), K_(buckets_cnt));
  } else if (OB_FAIL(meta_allocator_.init(OB_MALLOC_BIG_BLOCK_SIZE, ObModIds::OB_MACRO_BLOCK_META))) {
    STORAGE_LOG(WARN, "Fail to init meta allocator, ", K(ret));
  } else if (OB_FAIL(init_buckets(buckets_cnt_))) {
    STORAGE_LOG(WARN, "Fail to init buckets, ", K(ret), K_(buckets_cnt));
  } else if (OB_FAIL(init_node_queue(max_block_cnt))) {
    STORAGE_LOG(WARN, "Fail to init node queue, ", K(ret), K(max_block_cnt));
  } else if (OB_FAIL(init_ctrl_queue(ctrl_cnt))) {
    STORAGE_LOG(WARN, "Fail to init ctrl queue, ", K(ret), K(ctrl_cnt));
  } else {
    max_block_cnt_ = max_block_cnt;
    if (OB_FAIL(TG_START(lib::TGDefIDs::MacroMetaMgr))) {
      STORAGE_LOG(WARN, "Fail to init timer, ", K(ret));
    } else if (OB_FAIL(TG_SCHEDULE(lib::TGDefIDs::MacroMetaMgr, gc_task_, GC_DELAY_US, true))) {
      STORAGE_LOG(WARN, "Fail to schedule gc task, ", K(ret));
    } else {
      is_inited_ = true;
      STORAGE_LOG(INFO, "Success to init macro block meta mgr, ", K(max_block_cnt));
    }
  }

  if (!is_inited_) {
    destroy();
  }
  return ret;
}

int ObMacroBlockMetaMgr::init_buckets(const int64_t buckets_cnt)
{
  int ret = OB_SUCCESS;
  if (buckets_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(buckets_cnt));
  } else {
    char* buf = NULL;
    int64_t buf_len = buckets_cnt * sizeof(ObMacroBlockMetaNode*);
    if (NULL == (buf = (char*)allocator_.alloc(buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret), K(buf_len));
    } else {
      MEMSET(buf, 0, buf_len);
      meta_buckets_ = (ObMacroBlockMetaNode**)buf;
    }
  }
  return ret;
}

int ObMacroBlockMetaMgr::init_node_queue(const int64_t node_cnt)
{
  int ret = OB_SUCCESS;
  if (node_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(node_cnt));
  } else if (OB_FAIL(node_queue_.init(node_cnt, &allocator_, ObModIds::OB_MACRO_BLOCK_META))) {
    STORAGE_LOG(WARN, "Fail to init node queue, ", K(ret));
  } else {
    char* buf = NULL;
    int64_t buf_len = node_cnt * sizeof(ObMacroBlockMetaNode);
    int64_t pos = 0;

    if (NULL == (buf = (char*)allocator_.alloc(buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret), K(buf_len));
    } else {
      MEMSET(buf, 0, buf_len);
      ObMacroBlockMetaNode* node = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < node_cnt; ++i) {
        node = new (buf + pos) ObMacroBlockMetaNode();
        if (OB_FAIL(node_queue_.push(node))) {
          STORAGE_LOG(WARN, "Fail to push node to queue, ", K(ret));
        } else {
          pos += sizeof(ObMacroBlockMetaNode);
        }
      }
    }
  }
  return ret;
}

int ObMacroBlockMetaMgr::init_ctrl_queue(const int64_t ctrl_cnt)
{
  int ret = OB_SUCCESS;
  if (ctrl_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument, ", K(ret), K(ctrl_cnt));
  } else if (OB_FAIL(ctrl_queue_.init(ctrl_cnt, &allocator_, ObModIds::OB_MACRO_BLOCK_META))) {
    STORAGE_LOG(WARN, "Fail to init ctrl queue, ", K(ret));
  } else {
    char* buf = NULL;
    int64_t buf_len = ctrl_cnt * sizeof(ObMacroBlockMetaCtrl);
    int64_t pos = 0;

    if (NULL == (buf = (char*)allocator_.alloc(buf_len))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "Fail to allocate memory, ", K(ret), K(buf_len));
    } else {
      MEMSET(buf, 0, buf_len);
      ObMacroBlockMetaCtrl* ctrl = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < ctrl_cnt; ++i) {
        ctrl = new (buf + pos) ObMacroBlockMetaCtrl();
        if (OB_FAIL(ctrl_queue_.push(ctrl))) {
          STORAGE_LOG(WARN, "Fail to push ctrl to queue, ", K(ret));
        } else {
          pos += sizeof(ObMacroBlockMetaCtrl);
        }
      }
    }
  }
  return ret;
}

void ObMacroBlockMetaMgr::destroy()
{
  TG_STOP(lib::TGDefIDs::MacroMetaMgr);
  TG_WAIT(lib::TGDefIDs::MacroMetaMgr);

  if (NULL != meta_buckets_) {
    uint64_t pos = 0;
    ObMacroBlockMetaNode* node = NULL;
    while (pos < buckets_cnt_) {
      node = meta_buckets_[pos++];
      while (NULL != node) {
        dec_ref(*node->meta_ctrl_);
        node = node->next_;
      }
    }
    meta_buckets_ = NULL;
  }

  ctrl_queue_.destroy();
  node_queue_.destroy();
  meta_allocator_.destroy();
  allocator_.reset();
  buckets_lock_.destroy();
  buckets_cnt_ = 0;
  max_block_cnt_ = 0;
  is_inited_ = false;
}

int ObMacroBlockMetaMgr::get_old_meta(const MacroBlockId macro_id, ObMacroBlockMetaHandle& meta_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMacroBlocMetaMgr has not been inited, ", K(ret));
  } else {
    meta_handle.reset();
    uint64_t pos = macro_id.hash() % buckets_cnt_;
    ObMacroBlockMetaNode* node = NULL;
    ObBucketRLockGuard guard(buckets_lock_, pos);
    node = meta_buckets_[pos];
    while (NULL != node) {
      if (macro_id == node->macro_id_) {
        break;
      } else {
        node = node->next_;
      }
    }

    if (NULL == node) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      inc_ref(*(node->meta_ctrl_));
      meta_handle.meta_ctrl_ = node->meta_ctrl_;
    }
  }
  return ret;
}

int ObMacroBlockMetaMgr::set_meta(const MacroBlockId macro_id, const ObMacroBlockMeta& meta)
{
  int ret = OB_SUCCESS;
  ObMacroBlockMetaNode* node = NULL;
  ObMacroBlockMetaCtrl* ctrl = NULL;
  ObMacroBlockMeta* new_meta = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMacroBlocMetaMgr has not been inited, ", K(ret));
  } else {
    uint64_t pos = macro_id.hash() % buckets_cnt_;
    ObBucketWLockGuard guard(buckets_lock_, pos);
    node = meta_buckets_[pos];
    while (NULL != node) {
      if (macro_id == node->macro_id_) {
        break;
      } else {
        node = node->next_;
      }
    }

    if (OB_FAIL(ctrl_queue_.pop(ctrl))) {
      STORAGE_LOG(WARN, "Fail to pop ctrl from queue, ", K(ret), K(macro_id), K(ctrl_queue_.get_total()));
    } else if (OB_FAIL(meta.deep_copy(new_meta, meta_allocator_))) {
      STORAGE_LOG(WARN, "Fail to deep copy macro meta, ", K(ret), K(macro_id), K(meta));
    } else {
      if (NULL != node) {
        // exist same macro id
        dec_ref(*(node->meta_ctrl_));
        node->meta_ctrl_ = ctrl;
        ctrl->meta_ = new_meta;
        inc_ref(*ctrl);
      } else {
        // new macro id
        if (OB_FAIL(node_queue_.pop(node))) {
          STORAGE_LOG(WARN, "Fail to pop node from queue, ", K(ret));
        } else {
          ctrl->meta_ = new_meta;
          inc_ref(*ctrl);

          node->macro_id_ = macro_id;
          node->meta_ctrl_ = ctrl;
          node->next_ = meta_buckets_[pos];
          meta_buckets_[pos] = node;
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    if (NULL != ctrl) {
      ctrl_queue_.push(ctrl);
      ctrl = NULL;
    }
    if (NULL != new_meta) {
      meta_allocator_.free(new_meta);
      new_meta = NULL;
    }
  } else {
    STORAGE_LOG(
        TRACE, "Success to set meta, ", K(macro_id), K(meta), K(ctrl_queue_.get_total()), K(ctrl_queue_.get_free()));
  }
  return ret;
}

int ObMacroBlockMetaMgr::enable_write_log()
{
  int ret = OB_SUCCESS;
  MacroBlockId macro_id;
  ObMacroBlockMetaHandle meta_handle;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < max_block_cnt_; ++i) {
      macro_id.reset();
      macro_id.set_local_block_id(i);
      if (OB_FAIL(get_old_meta(macro_id, meta_handle))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          STORAGE_LOG(WARN, "failed to get meta", K(ret), K(macro_id));
        }
      } else if (OB_ISNULL(meta_handle.get_meta())) {
        ret = OB_ERR_SYS;
        STORAGE_LOG(ERROR, "meta must not null", K(ret), K(macro_id));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObIRedoModule::enable_write_log())) {
      STORAGE_LOG(WARN, "Fail to enable redo module write log, ", K(ret));
    }
  }
  return ret;
}

int ObMacroBlockMetaMgr::erase_meta(const MacroBlockId macro_id)
{
  int ret = OB_SUCCESS;
  ObMacroBlockMetaNode* prev = NULL;
  ObMacroBlockMetaNode* node = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "The ObMacroBlocMetaMgr has not been inited, ", K(ret));
  } else {
    uint64_t pos = macro_id.hash() % buckets_cnt_;
    {
      ObBucketWLockGuard guard(buckets_lock_, pos);
      prev = NULL;
      node = meta_buckets_[pos];
      while (NULL != node) {
        if (macro_id == node->macro_id_) {
          break;
        } else {
          prev = node;
          node = node->next_;
        }
      }

      if (NULL == node) {
        ret = OB_ENTRY_NOT_EXIST;
      } else {
        if (NULL == prev) {
          // first node
          meta_buckets_[pos] = node->next_;
        } else {
          prev->next_ = node->next_;
        }
        node->next_ = NULL;
      }
    }

    if (OB_SUCC(ret) && NULL != node) {
      int tmp_ret = OB_SUCCESS;
      dec_ref(*(node->meta_ctrl_));
      node->reset();
      if (OB_SUCCESS != (tmp_ret = node_queue_.push(node))) {
        // should not happen
        STORAGE_LOG(ERROR, "Fail to push node to queue, ", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObMacroBlockMetaMgr::replay(const ObRedoModuleReplayParam& replay_param)
{
  int ret = OB_SUCCESS;
  const int64_t subcmd = replay_param.subcmd_;
  const char* buf = replay_param.buf_;
  const int64_t len = replay_param.buf_len_;
  ObRedoLogMainType main_type = static_cast<ObRedoLogMainType>(0);
  int32_t sub_type = 0;
  int64_t pos = 0;
  ObMacroBlockMeta meta;
  ObObj endkey[OB_MAX_ROWKEY_COLUMN_NUMBER];
  meta.endkey_ = endkey;
  ObMacroBlockMetaLogEntry entry(0, 0, 0, meta);
  const ObISLogFilter* filter = nullptr;
  ObISLogFilter::Param param;
  bool is_filtered = false;
  param.entry_ = &entry;

  ObIRedoModule::parse_subcmd(replay_param.subcmd_, main_type, sub_type);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not initialized, cannot do replay.", K(ret));
  } else if (!replay_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument.", K(ret), K(replay_param));
  } else if (OB_REDO_LOG_MACROBLOCK != main_type || CHANGE_MACRO_BLOCK_META != sub_type) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "wrong redo log type.", K(main_type), K(sub_type), K(ret));
  } else if (OB_FAIL(entry.deserialize(buf, len, pos))) {
    STORAGE_LOG(WARN, "deserialize meta log entry error.", K(len), K(pos), K(ret));
  } else if (nullptr != (filter = get_filter()) && OB_FAIL(filter->filter(param, is_filtered))) {
    // do nothing
  } else if (is_filtered) {
    // slog is filtered, do not need to replay
  } else {
    MacroBlockId block_id(0, 0, 0, static_cast<int32_t>(entry.block_index_));
    if (OB_FAIL(set_meta(block_id, meta))) {
      STORAGE_LOG(WARN, "write meta to mgr error.", K(ret), K(meta));
    }
  }
  return ret;
}

int ObMacroBlockMetaMgr::parse(const int64_t subcmd, const char* buf, const int64_t len, FILE* stream)
{
  int ret = OB_SUCCESS;
  ObRedoLogMainType main_type = static_cast<ObRedoLogMainType>(0);
  int32_t sub_type = 0;
  int64_t pos = 0;
  ObMacroBlockMeta meta;
  ObObj endkey[OB_MAX_ROWKEY_COLUMN_NUMBER];
  meta.endkey_ = endkey;
  ObMacroBlockMetaLogEntry entry(0, 0, 0, meta);

  ObIRedoModule::parse_subcmd(subcmd, main_type, sub_type);
  if (NULL == buf || len < 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(len));
  } else if (OB_REDO_LOG_MACROBLOCK != main_type || CHANGE_MACRO_BLOCK_META != sub_type) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "wrong redo log type.", K(main_type), K(sub_type), K(ret));
  } else if (OB_FAIL(entry.deserialize(buf, len, pos))) {
    STORAGE_LOG(WARN, "deserialize meta log entry error.", K(len), K(pos), K(ret));
  } else {
    fprintf(stream, "macro block redo log, change macro block meta\n%s\n", to_cstring(entry));
  }
  return ret;
}

void ObMacroBlockMetaMgr::gc()
{
  if (is_inited_) {
    int ret = OB_SUCCESS;
    uint64_t pos = 0;
    ObMacroBlockMetaNode* node = NULL;
    const ObMacroBlockMeta* old_meta = NULL;
    ObMacroBlockMeta* new_meta = NULL;
    ObMacroBlockMetaCtrl* new_ctrl = NULL;
    ObMacroBlockMetaCtrl* old_ctrl = NULL;
    uint64_t rewrite_cnt = 0;

    STORAGE_LOG(INFO, "GC macro block meta begin, ", K(ctrl_queue_.get_total()), K(ctrl_queue_.get_free()));

    while (pos < buckets_cnt_ && ctrl_queue_.get_total() >= max_block_cnt_) {
      ObBucketWLockGuard guard(buckets_lock_, pos);
      node = meta_buckets_[pos];
      while (NULL != node) {
        ret = OB_SUCCESS;
        old_meta = node->meta_ctrl_->meta_;
        new_meta = NULL;
        old_ctrl = NULL;
        new_ctrl = NULL;
        if (meta_allocator_.is_fragment((void*)(old_meta))) {
          if (OB_FAIL(old_meta->deep_copy(new_meta, meta_allocator_))) {
            STORAGE_LOG(WARN, "Fail to deep copy meta, ", K(ret));
          } else if (OB_FAIL(ctrl_queue_.pop(new_ctrl))) {
            STORAGE_LOG(WARN, "Fail to pop meta ctrl, ", K(ret));
          } else {
            inc_ref(*new_ctrl);
            new_ctrl->meta_ = new_meta;
            old_ctrl = node->meta_ctrl_;
            node->meta_ctrl_ = new_ctrl;
            dec_ref(*old_ctrl);
            rewrite_cnt++;
          }

          if (OB_FAIL(ret)) {
            if (NULL != new_meta) {
              meta_allocator_.free((void*)new_meta);
            }
          }
        }

        node = node->next_;
      }
      pos++;
    }

    STORAGE_LOG(
        INFO, "GC macro block meta finish, ", K(rewrite_cnt), K(ctrl_queue_.get_total()), K(ctrl_queue_.get_free()));
  }
}

void ObMacroBlockMetaMgr::inc_ref(ObMacroBlockMetaCtrl& meta_ctrl)
{
  ATOMIC_AAF(&meta_ctrl.ref_cnt_, 1);
}

void ObMacroBlockMetaMgr::dec_ref(ObMacroBlockMetaCtrl& meta_ctrl)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    // do nothing
  } else if (0 == ATOMIC_SAF(&meta_ctrl.ref_cnt_, 1)) {
    if (NULL != meta_ctrl.meta_) {
      meta_allocator_.free((void*)(meta_ctrl.meta_));
    }
    meta_ctrl.reset();
    if (OB_FAIL(ctrl_queue_.push(&meta_ctrl))) {
      STORAGE_LOG(ERROR, "Fail to push meta ctrl to queue, ", K(ret));
    }
  }
}

uint64_t ObMajorMacroBlockKey::hash() const
{
  uint64_t hash_value = 0;

  hash_value = common::murmurhash(&table_id_, sizeof(table_id_), hash_value);
  hash_value = common::murmurhash(&partition_id_, sizeof(partition_id_), hash_value);
  hash_value = common::murmurhash(&data_version_, sizeof(data_version_), hash_value);
  hash_value = common::murmurhash(&data_seq_, sizeof(data_seq_), hash_value);
  return hash_value;
}

void ObMajorMacroBlockKey::reset()
{
  table_id_ = 0;
  partition_id_ = -1;
  data_version_ = 0;
  data_seq_ = -1;
}

bool ObMajorMacroBlockKey::operator==(const ObMajorMacroBlockKey& key) const
{
  return table_id_ == key.table_id_ && partition_id_ == key.partition_id_ && data_version_ == key.data_version_ &&
         data_seq_ == key.data_seq_;
}

}  // namespace blocksstable
}  // namespace oceanbase
