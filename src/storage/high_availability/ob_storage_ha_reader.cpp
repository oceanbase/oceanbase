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
#include "ob_storage_ha_reader.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/backup/ob_backup_factory.h"
#include "observer/omt/ob_tenant.h"
#include "storage/backup/ob_backup_meta_cache.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace obrpc;
using namespace blocksstable;

namespace storage
{
ERRSIM_POINT_DEF(EN_READER_RPC_NOT_SUPPORT);
ERRSIM_POINT_DEF(EN_ONLY_COPY_OLD_VERSION_MAJOR_SSTABLE);

/******************CopyMacroBlockReadInfo*********************/
ObICopyMacroBlockReader::CopyMacroBlockReadData::CopyMacroBlockReadData()
  : data_type_(ObCopyMacroBlockDataType::MAX),
    is_reuse_macro_block_(false),
    macro_data_(),
    macro_meta_(nullptr),
    macro_block_id_(),
    allocator_("CopyMacroRead")
{
}

ObICopyMacroBlockReader::CopyMacroBlockReadData::~CopyMacroBlockReadData()
{
  reset();
}

void ObICopyMacroBlockReader::CopyMacroBlockReadData::reset()
{
  data_type_ = ObCopyMacroBlockDataType::MAX;
  is_reuse_macro_block_ = false;
  macro_data_ = ObBufferReader(NULL, 0, 0);
  macro_meta_ = nullptr;
  macro_block_id_.reset();
  allocator_.reset();
}

bool ObICopyMacroBlockReader::CopyMacroBlockReadData::is_valid() const
{
  bool valid = false;

  if (ObCopyMacroBlockDataType::MACRO_META_ROW == data_type_) {
    valid = is_reuse_macro_block_ && OB_NOT_NULL(macro_meta_) && macro_meta_->is_valid();
  } else if (ObCopyMacroBlockDataType::MACRO_DATA == data_type_) {
    valid = !is_reuse_macro_block_ && macro_data_.is_valid();
  }

  return valid;
}

int ObICopyMacroBlockReader::CopyMacroBlockReadData::set_macro_meta(
  const blocksstable::ObDataMacroBlockMeta &macro_meta,
  const bool &is_reuse_macro_block)
{
  int ret = OB_SUCCESS;

  if (!macro_meta.is_valid() || !is_reuse_macro_block) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set macro meta get invalid argument", K(ret), K(macro_meta), K(is_reuse_macro_block));
  } else if (OB_FAIL(macro_meta.deep_copy(macro_meta_, allocator_))) {
    LOG_WARN("failed to deep copy macro meta", K(ret), K(macro_meta));
  } else {
    data_type_ = ObCopyMacroBlockDataType::MACRO_META_ROW;
    is_reuse_macro_block_ = is_reuse_macro_block;
  }

  return ret;
}

int ObICopyMacroBlockReader::CopyMacroBlockReadData::set_macro_data(
  const ObBufferReader &data,
  const bool &is_reuse_macro_block)
{
  int ret = OB_SUCCESS;

  if (!data.is_valid() || is_reuse_macro_block) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set macro data get invalid argument", K(ret), K(data), K(is_reuse_macro_block));
  } else {
    macro_data_ = data;
    data_type_ = ObCopyMacroBlockDataType::MACRO_DATA;
    is_reuse_macro_block_ = is_reuse_macro_block;
  }

  return ret;
}

void ObICopyMacroBlockReader::CopyMacroBlockReadData::set_macro_block_id(const MacroBlockId &macro_block_id)
{
  // won't check macro_block_id_ is valid
  macro_block_id_ = macro_block_id;
}


/******************ObCopyMacroBlockReaderInitParam*********************/
ObCopyMacroBlockReaderInitParam::ObCopyMacroBlockReaderInitParam()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    table_key_(),
    copy_macro_range_info_(),
    src_info_(),
    is_leader_restore_(false),
    restore_action_(ObTabletRestoreAction::RESTORE_NONE),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    restore_base_info_(nullptr),
    meta_index_store_(nullptr),
    second_meta_index_store_(nullptr),
    restore_macro_block_id_mgr_(nullptr),
    need_check_seq_(false),
    ls_rebuild_seq_(-1),
    backfill_tx_scn_(),
    data_version_(0),
    macro_block_reuse_mgr_(nullptr)
{
}

ObCopyMacroBlockReaderInitParam::~ObCopyMacroBlockReaderInitParam()
{
}

bool ObCopyMacroBlockReaderInitParam::is_valid() const
{
  bool bool_ret = false;
  bool_ret = tenant_id_ != OB_INVALID_ID && ls_id_.is_valid() && table_key_.is_valid() && OB_NOT_NULL(copy_macro_range_info_)
      && ((need_check_seq_ && ls_rebuild_seq_ >= 0) || !need_check_seq_) && data_version_ >= 0;
  if (bool_ret) {
    if (!is_leader_restore_) {
      bool_ret = src_info_.is_valid() && OB_NOT_NULL(bandwidth_throttle_) && OB_NOT_NULL(svr_rpc_proxy_)
          && backfill_tx_scn_.is_valid();
    } else if (OB_ISNULL(restore_base_info_)
        || OB_ISNULL(meta_index_store_)
        || OB_ISNULL(second_meta_index_store_)) {
      bool_ret = false;
    } else if (!ObTabletRestoreAction::is_restore_remote_sstable(restore_action_)
               && !ObTabletRestoreAction::is_restore_replace_remote_sstable(restore_action_)
               && OB_ISNULL(restore_macro_block_id_mgr_)) {
      bool_ret = false;
      LOG_WARN_RET(OB_INVALID_ARGUMENT, "restore_macro_block_id_mgr_ is null", K_(restore_action), KP_(restore_macro_block_id_mgr));
     }
  }
  return bool_ret;
}

void ObCopyMacroBlockReaderInitParam::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  table_key_.reset();
  copy_macro_range_info_ = nullptr;
  src_info_.reset();
  is_leader_restore_ = false;
  restore_action_ = ObTabletRestoreAction::RESTORE_NONE;
  bandwidth_throttle_ = nullptr;
  svr_rpc_proxy_ = nullptr;
  restore_base_info_ = nullptr;
  meta_index_store_ = nullptr;
  second_meta_index_store_ = nullptr;
  restore_macro_block_id_mgr_ = nullptr;
  need_check_seq_ = false;
  ls_rebuild_seq_ = -1;
  backfill_tx_scn_.reset();
}

int ObCopyMacroBlockReaderInitParam::assign(const ObCopyMacroBlockReaderInitParam &param)
{
  int ret = OB_SUCCESS;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign copy macro block reader init param get invalid argument", K(ret), K(param));
  } else {
    tenant_id_ = param.tenant_id_;
    ls_id_ = param.ls_id_;
    table_key_ = param.table_key_;
    src_info_ = param.src_info_;
    is_leader_restore_ = param.is_leader_restore_;
    restore_action_ = param.restore_action_;
    bandwidth_throttle_ = param.bandwidth_throttle_;
    svr_rpc_proxy_ = param.svr_rpc_proxy_;
    restore_base_info_ = param.restore_base_info_;
    meta_index_store_ = param.meta_index_store_;
    second_meta_index_store_ = param.second_meta_index_store_;
    restore_macro_block_id_mgr_ = param.restore_macro_block_id_mgr_;
    copy_macro_range_info_ = param.copy_macro_range_info_;
    need_check_seq_ = param.need_check_seq_;
    ls_rebuild_seq_ = param.ls_rebuild_seq_;
    backfill_tx_scn_ = param.backfill_tx_scn_;
  }
  return ret;
}

/******************ObCopyMacroBlockObReader*********************/
ObCopyMacroBlockObReader::ObCopyMacroBlockObReader()
  : is_inited_(false),
    handle_(),
    bandwidth_throttle_(NULL),
    data_buffer_(),
    rpc_buffer_(),
    rpc_buffer_parse_pos_(0),
    allocator_(),
    macro_block_mem_context_(),
    last_send_time_(0),
    data_size_(0),
    macro_block_reuse_mgr_(nullptr),
    table_key_()
{
  ObMemAttr attr(MTL_ID(), "CMBObReader");
  allocator_.set_attr(attr);
}

ObCopyMacroBlockObReader::~ObCopyMacroBlockObReader()
{
  void *ptr = reinterpret_cast<void*>(data_buffer_.data());
  if (nullptr != ptr && macro_block_mem_context_.get_allocator().contains(ptr)) {
    macro_block_mem_context_.free(ptr);
  }
}

int ObCopyMacroBlockObReader::init(
    const ObCopyMacroBlockReaderInitParam &param)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (!param.is_valid() || param.is_leader_restore_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else {
    const int64_t rpc_timeout = ObStorageHAUtils::get_rpc_timeout();

    SMART_VAR(ObCopyMacroBlockRangeArg, arg) {
      if (OB_FAIL(macro_block_mem_context_.init())) {
        LOG_WARN("failed to init macro block memory context", K(ret));
      } else if (OB_FAIL(alloc_buffers())) {
        LOG_WARN("failed to alloc buffers", K(ret));
      } else if (OB_FAIL(arg.copy_macro_range_info_.assign(*param.copy_macro_range_info_))) {
        LOG_WARN("failed to assign copy macro range info", K(ret), K(param));
      } else {
        arg.tenant_id_ = param.tenant_id_;
        arg.ls_id_ = param.ls_id_;
        arg.table_key_ = param.table_key_;
        arg.backfill_tx_scn_ = param.backfill_tx_scn_;
        arg.data_version_ = param.data_version_;
        arg.need_check_seq_ = param.need_check_seq_;
        arg.ls_rebuild_seq_ = param.ls_rebuild_seq_;
        LOG_INFO("init arg", K(param), K(arg));
      }

      if (OB_SUCC(ret)) {
        if (arg.get_serialize_size() > OB_MALLOC_BIG_BLOCK_SIZE) {
          ret = OB_ERR_SYS;
          LOG_ERROR("rpc arg must not larger than packet size", K(ret), K(arg.get_serialize_size()));
        } else if (OB_FAIL(param.svr_rpc_proxy_->to(param.src_info_.src_addr_).by(param.tenant_id_).dst_cluster_id(param.src_info_.cluster_id_)
                           .ratelimit(true).bg_flow(obrpc::ObRpcProxy::BACKGROUND_FLOW)
                           .timeout(rpc_timeout)
                           .group_id(share::OBCG_STORAGE_STREAM)
                           .fetch_macro_block(arg, rpc_buffer_, handle_))) {
          LOG_WARN("failed to send fetch macro block rpc", K(param), K(ret));
        } else {
          bandwidth_throttle_ = param.bandwidth_throttle_;
          rpc_buffer_parse_pos_ = 0;
          last_send_time_ = ObTimeUtility::current_time();
          data_size_ = rpc_buffer_.get_position();
          is_inited_ = true;
          macro_block_reuse_mgr_ = param.macro_block_reuse_mgr_;
          table_key_ = param.table_key_;
          LOG_INFO("get first package fetch macro block", K(rpc_buffer_));
        }
      }
    }
  }
  return ret;
}

int ObCopyMacroBlockObReader::alloc_buffers()
{
  int ret = OB_SUCCESS;
  char *buf = NULL;

  // used in init() func, should not check is_inited_
  if (NULL == (buf = reinterpret_cast<char*>(allocator_.alloc(OB_MALLOC_BIG_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret));
  } else if (!rpc_buffer_.set_data(buf, OB_MALLOC_BIG_BLOCK_SIZE)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to set rpc buffer", K(ret));
  } else if (OB_FAIL(alloc_from_memctx_first(buf))) {
    LOG_WARN("failed to alloc buf", K(ret));
  } else {
    data_buffer_.assign(buf, OB_DEFAULT_MACRO_BLOCK_SIZE);
  }
  return ret;
}

int ObCopyMacroBlockObReader::alloc_from_memctx_first(char* &buf)
{
  int ret = OB_SUCCESS;
  buf = NULL;
  if (OB_ISNULL(buf = reinterpret_cast<char*>(macro_block_mem_context_.alloc()))) {
    if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator_.alloc(OB_DEFAULT_MACRO_BLOCK_SIZE)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc buf", K(ret));
    }
  }
  return ret;
}

int ObCopyMacroBlockObReader::fetch_next_buffer_if_need()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(fetch_next_buffer())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to fetch next buffer", K(ret));
    }
  }
  return ret;
}

int ObCopyMacroBlockObReader::fetch_next_buffer()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(do_fetch_next_buffer_if_need(
      *bandwidth_throttle_, rpc_buffer_, rpc_buffer_parse_pos_, handle_, last_send_time_, data_size_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch next buffer if need", K(ret));
    }
  }
  return ret;
}

int ObCopyMacroBlockObReader::get_next_macro_block(ObICopyMacroBlockReader::CopyMacroBlockReadData &read_data)
{
  int ret = OB_SUCCESS;
  ObBufferReader data;
  ObCopyMacroBlockHeader header;
  read_data.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(fetch_next_buffer_if_need())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to fetch next buffer if need", K(ret));
    }
  } else if (OB_FAIL(serialization::decode(rpc_buffer_.get_data(),
      rpc_buffer_.get_position(),
      rpc_buffer_parse_pos_,
      header))) {
    STORAGE_LOG(WARN, "failed to decode macro block size", K(ret), K_(rpc_buffer), K_(rpc_buffer_parse_pos));
  } else if (OB_FAIL(data_buffer_.set_pos(0))) {
    LOG_WARN("failed to set data buffer pos", K(ret), K(data_buffer_));
  } else {
    while (OB_SUCC(ret)) {
      if (data_buffer_.length() > header.occupy_size_) {
        ret = OB_ERR_SYS;
        LOG_WARN("data buffer must not larger than occupy size", K(ret), K(data_buffer_), K(header));
      } else if (data_buffer_.length() == header.occupy_size_) {
        data.assign(data_buffer_.data(), data_buffer_.capacity(), data_buffer_.length());
        LOG_DEBUG("get_next_macro_block", K(rpc_buffer_), K(rpc_buffer_parse_pos_), K(header));
        break;
      } else if (OB_FAIL(fetch_next_buffer_if_need())) {
        LOG_WARN("failed to fetch next buffer if need", K(ret));
      } else {
        int64_t need_size = header.occupy_size_ - data_buffer_.length();
        int64_t rpc_remain_size = rpc_buffer_.get_position() - rpc_buffer_parse_pos_;
        int64_t copy_size = std::min(need_size, rpc_remain_size);
        if (copy_size > data_buffer_.remain()) {
          ret = OB_BUF_NOT_ENOUGH;
          LOG_ERROR("data buffer is not enough, macro block data must not larger than data buffer",
              K(ret), K(copy_size), K(data_buffer_), K(data_buffer_.remain()));
        } else {
          LOG_DEBUG("copy rpc to data buffer",
              K(need_size), K(rpc_remain_size), K(copy_size), "occupy_size", header.occupy_size_, K(rpc_buffer_parse_pos_));
          MEMCPY(data_buffer_.current(), rpc_buffer_.get_data() + rpc_buffer_parse_pos_, copy_size);
          if (OB_FAIL(data_buffer_.advance(copy_size))) {
            STORAGE_LOG(ERROR, "BUG here! data_buffer_ advance failed.",
                K(ret), K(data_buffer_.remain()), K(copy_size));
          } else {
            rpc_buffer_parse_pos_ += copy_size;
          }
        }
      }
    }

    // set read info
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_read_info_(header, data, read_data))) {
        LOG_WARN("failed to get read info", K(ret));
      }
    }
  }
  return ret;
}

int ObCopyMacroBlockObReader::get_read_info_(const ObCopyMacroBlockHeader &header, const ObBufferReader &data, CopyMacroBlockReadData &read_data)
{
  int ret = OB_SUCCESS;
  read_data.reset();

  if (header.data_type_ == ObCopyMacroBlockDataType::MACRO_DATA) {
    if (OB_FAIL(read_data.set_macro_data(data, header.is_reuse_macro_block_))) {
      LOG_WARN("failed to set macro data", K(ret), K(data), K(header));
    }
  } else if (header.data_type_ == ObCopyMacroBlockDataType::MACRO_META_ROW) {
    ObDatumRow macro_meta_row;
    ObDataMacroBlockMeta macro_meta;
    MacroBlockId macro_id;
    int64_t data_checksum = 0;
    int64_t pos = 0;

    if (OB_FAIL(macro_meta_row.init(OB_MAX_ROWKEY_COLUMN_NUMBER + 1))) {
      // use max row key cnt + 1 as capacity, because meta row is kv
      LOG_WARN("failed to init macro meta row", K(ret));
    } else if (OB_FAIL(macro_meta_row.deserialize(data.data(), header.occupy_size_, pos))) {
      LOG_WARN("failed to deserialize macro meta row", K(ret), K(data), K(header), K(pos));
    } else if (OB_FAIL(macro_meta.parse_row(macro_meta_row))) {
      LOG_WARN("failed to parse row", K(ret), K(macro_meta_row));
    } else if (macro_meta.get_macro_id().is_backup_id()) {
      // macro block is in backup media, it's migration when quick restore, skip modify macro id
    } else if (OB_ISNULL(macro_block_reuse_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro block reuse mgr is NULL", K(ret), KP(macro_block_reuse_mgr_));
    } else if (OB_FAIL(macro_block_reuse_mgr_->get_macro_block_reuse_info(table_key_, macro_meta.get_logic_id(), macro_id, data_checksum))) {
      LOG_WARN("failed to get macro block reuse info", K(ret), K(table_key_), K(macro_meta.get_logic_id()));
    } else if (macro_meta.get_meta_val().data_checksum_ != data_checksum) {
      ret = OB_CHECKSUM_ERROR;
      LOG_WARN("data checksum not match", K(ret), K(macro_meta.get_meta_val().data_checksum_), K(data_checksum));
    } else {
      // modify macro id of macro meta to real local macro id
      macro_meta.val_.macro_id_ = macro_id;
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(read_data.set_macro_meta(macro_meta, header.is_reuse_macro_block_))) {
        LOG_WARN("failed to set macro meta", K(ret), K(macro_meta));
      }
    }
  }
  return ret;
}

/******************ObCopyMacroBlockRestoreReader*********************/
ObCopyMacroBlockRestoreReader::ObCopyMacroBlockRestoreReader()
  : is_inited_(false),
    table_key_(),
    tablet_handle_(),
    copy_macro_range_info_(nullptr),
    restore_base_info_(nullptr),
    meta_index_store_(nullptr),
    second_meta_index_store_(nullptr),
    restore_macro_block_id_mgr_(nullptr),
    data_buffer_(),
    allocator_(),
    macro_block_index_(0),
    macro_block_count_(0),
    data_size_(0),
    datum_range_(),
    sec_meta_iterator_(nullptr),
    restore_action_(ObTabletRestoreAction::RESTORE_NONE),
    meta_row_buf_("CopyMacroMetaRow")
{
  ObMemAttr attr(MTL_ID(), "CMBReReader");
  allocator_.set_attr(attr);
}

ObCopyMacroBlockRestoreReader::~ObCopyMacroBlockRestoreReader()
{
  if (OB_NOT_NULL(sec_meta_iterator_)) {
    backup::ObLSBackupFactory::free(sec_meta_iterator_);
    sec_meta_iterator_ = nullptr;
  }
  tablet_handle_.reset();
  allocator_.reset();
}

int ObCopyMacroBlockRestoreReader::init(
    const ObCopyMacroBlockReaderInitParam &param)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (!param.is_valid() || !param.is_leader_restore_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else if (ObTabletRestoreAction::is_restore_remote_sstable(param.restore_action_)
             && ObBackupSetFileDesc::is_backup_set_not_support_quick_restore(param.restore_base_info_->backup_compatible_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("restore remote sstable, but backup set not support quick restore", K(ret), K(param));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(param.ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("fail to get log stream", K(ret), K(param));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", K(ret), K(param));
  } else if (OB_FAIL(ls->ha_get_tablet(param.table_key_.get_tablet_id(), tablet_handle_))) {
    LOG_WARN("failed to get tablet handle", K(ret), K(param));
  } else if (OB_FAIL(alloc_buffers())) {
    LOG_WARN("failed to alloc buffers", K(ret));
  } else {
    table_key_ = param.table_key_;
    copy_macro_range_info_ = param.copy_macro_range_info_;
    restore_base_info_ = param.restore_base_info_;
    meta_index_store_ = param.meta_index_store_;
    second_meta_index_store_ = param.second_meta_index_store_;
    restore_action_ = param.restore_action_;

    if (ObTabletRestoreAction::is_restore_remote_sstable(param.restore_action_)) {
      // iterate and return all macro mata using iterator to build index/meta tree.
      datum_range_.set_start_key(copy_macro_range_info_->start_macro_block_end_key_);
      datum_range_.end_key_.set_max_rowkey();
      datum_range_.set_left_closed();
      datum_range_.set_right_open();
      if (OB_FAIL(ObRestoreUtils::create_backup_sstable_sec_meta_iterator(param.tenant_id_,
                                                                          table_key_.get_tablet_id(),
                                                                          tablet_handle_,
                                                                          table_key_,
                                                                          datum_range_,
                                                                          *restore_base_info_,
                                                                          *meta_index_store_,
                                                                          sec_meta_iterator_))) {
        LOG_WARN("failed to create backup sstable sec meta iterator", K(ret), K(param));
      }
    } else {
      // otherwise, read macro data from backup with macro id from restore_macro_block_id_mgr_.
      restore_macro_block_id_mgr_ = param.restore_macro_block_id_mgr_;
      if (OB_FAIL(restore_macro_block_id_mgr_->get_block_id_index(copy_macro_range_info_->start_macro_block_id_, macro_block_index_))) {
        LOG_WARN("failed to get block id index", K(ret), KPC(copy_macro_range_info_));
      }
    }

    if (OB_SUCC(ret)) {
      macro_block_count_ = 0;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObCopyMacroBlockRestoreReader::alloc_buffers()
{
  int ret = OB_SUCCESS;
  const int64_t READ_BUFFER_SIZE = OB_DEFAULT_MACRO_BLOCK_SIZE * 2;
  char *buf = NULL;
  char *read_buf = NULL;

  // used in init() func, should not check is_inited_
  if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator_.alloc(OB_DEFAULT_MACRO_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret));
  } else if (OB_ISNULL(read_buf = reinterpret_cast<char*>(allocator_.alloc(READ_BUFFER_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc read_buf", K(ret));
  } else {
    data_buffer_.assign(buf, OB_DEFAULT_MACRO_BLOCK_SIZE);
    read_buffer_.assign(read_buf, READ_BUFFER_SIZE);
  }

  if (OB_FAIL(ret)) {
    allocator_.reset();
  }

  return ret;
}

void ObCopyMacroBlockRestoreReader::reset_buffers_()
{
  data_buffer_.set_pos(0);
  read_buffer_.set_pos(0);
  MEMSET(data_buffer_.data(), 0, data_buffer_.capacity());
  MEMSET(read_buffer_.data(), 0, read_buffer_.capacity());
}

int ObCopyMacroBlockRestoreReader::get_next_macro_block(ObICopyMacroBlockReader::CopyMacroBlockReadData &read_data)
{
  int ret = OB_SUCCESS;
  read_data.reset();
  ObLogicMacroBlockId logic_block_id;

#ifdef ERRSIM
  // Simulate minor macro data read failed.
  if (!table_key_.get_tablet_id().is_ls_inner_tablet()
    && table_key_.is_minor_sstable()) {
    ret = OB_E(EventTable::EN_MIGRATION_READ_REMOTE_MACRO_BLOCK_FAILED) OB_SUCCESS;
  }
#endif

  if (OB_FAIL(ret)) {
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (macro_block_count_ == copy_macro_range_info_->macro_block_count_) {
    ret = OB_ITER_END;
  } else if (ObTabletRestoreAction::is_restore_remote_sstable(restore_action_)) {
    // If restore remote sstable, just return backup macro meta to rebuild index/meta tree.
    blocksstable::ObDataMacroBlockMeta macro_meta;
    if (OB_FAIL(sec_meta_iterator_->get_next(macro_meta))) {
      LOG_WARN("failed to get next macro meta", K(ret), K(macro_block_count_), KPC(copy_macro_range_info_));
    } else if (OB_FAIL(read_data.set_macro_meta(macro_meta, true /* is_reuse_macro_block */))) {
      LOG_WARN("failed to set read data", K(ret), K(macro_meta));
    } else {
      logic_block_id = macro_meta.get_logic_id();
    }
  } else {
    // Otherwise, macro data should be read from backup device.
    share::ObBackupDataType data_type;
    share::ObBackupPath backup_path;
    const int64_t align_size = DIO_READ_ALIGN_SIZE;
    backup::ObBackupMacroBlockIndex macro_index;
    share::ObBackupStorageInfo storage_info;
    share::ObRestoreBackupSetBriefInfo backup_set_brief_info;
    share::ObBackupDest backup_set_dest;
    MacroBlockId macro_block_id;
    ObStorageIdMod mod;
    mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_RESTORE;
    int64_t dest_id = 0;

    reset_buffers_();

    blocksstable::ObBufferReader data;
    data_buffer_.set_pos(0);
    read_buffer_.set_pos(0);
    if (OB_FAIL(ObRestoreUtils::get_backup_data_type(table_key_, data_type))) {
      LOG_WARN("fail to get backup data type", K(ret), K(table_key_));
    } else if (OB_FAIL(fetch_macro_block_index_(macro_block_index_, data_type, logic_block_id, macro_index))) {
      LOG_WARN("failed to fetch macro block index", K(ret), K_(macro_block_index), K(data_type));
    } else if (OB_FAIL(convert_logical_id_to_shared_macro_id_(logic_block_id, macro_block_id))) {
      LOG_WARN("failed to convert logical id to shared macro id", K(ret), K(logic_block_id), K(table_key_));
    } else if (OB_FAIL(restore_base_info_->get_restore_backup_set_dest(macro_index.backup_set_id_, backup_set_brief_info))) {
      LOG_WARN("fail to get backup set dest", K(ret), K(macro_index));
    } else if (OB_FAIL(restore_base_info_->get_restore_data_dest_id(*GCTX.sql_proxy_, MTL_ID(), dest_id))) {
      LOG_WARN("fail to get restore data dest id", K(ret));
    } else if (OB_FALSE_IT(mod.storage_id_ = static_cast<uint64_t>(dest_id))) {
    } else if (OB_FAIL(backup_set_dest.set(backup_set_brief_info.backup_set_path_))) {
      LOG_WARN("fail to set backup set dest", K(ret));
    } else if (OB_FAIL(get_macro_block_backup_path_(backup_set_dest, macro_index, data_type, backup_path))) {
      LOG_WARN("failed to get macro block index", K(ret), K(restore_base_info_), K(macro_index), KPC(restore_base_info_));
    } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_macro_block_data(backup_path.get_obstr(),
        restore_base_info_->backup_dest_.get_storage_info(), mod, macro_index, align_size, read_buffer_, data_buffer_))) {
      LOG_WARN("failed to read macro block data", K(ret), K(table_key_), K(macro_index), KPC(restore_base_info_));
    } else if (FALSE_IT(data_size_ += data_buffer_.length())) {
    } else if (FALSE_IT(data.assign(data_buffer_.data(), data_buffer_.length(), data_buffer_.length()))) {
    } else if (OB_FAIL(read_data.set_macro_data(data, false /* is_reuse_macro_block */))) {
      LOG_WARN("failed to set read info", K(ret), K(data));
    } else {
      read_data.set_macro_block_id(macro_block_id);
    }
  }

  if (OB_SUCC(ret)){
      macro_block_count_++;
      macro_block_index_++;
      if (macro_block_count_ == copy_macro_range_info_->macro_block_count_) {
        if (logic_block_id != copy_macro_range_info_->end_macro_block_id_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get macro block end macro block id is not equal to macro block range",
              K(ret), K_(macro_block_count), K_(macro_block_index), K(logic_block_id),
              "end_macro_block_id", copy_macro_range_info_->end_macro_block_id_, K(table_key_));
      }
    }
  }

  return ret;
}

int ObCopyMacroBlockRestoreReader::get_macro_block_backup_path_(
    const share::ObBackupDest &backup_set_dest,
    const backup::ObBackupMacroBlockIndex &macro_index,
    const share::ObBackupDataType data_type,
    ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(restore_base_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("restore base info should not be null", K(ret));
  } else {
    const share::ObBackupSetFileDesc::Compatible &compatible = restore_base_info_->backup_compatible_;
    if (share::ObBackupSetFileDesc::is_backup_set_support_quick_restore(compatible)) {
      if (OB_FAIL(share::ObBackupPathUtilV_4_3_2::get_macro_block_backup_path(backup_set_dest, macro_index.ls_id_,
          data_type, macro_index.turn_id_, macro_index.retry_id_, macro_index.file_id_, backup_path))) {
        LOG_WARN("failed to get macro block index", K(ret), K(restore_base_info_), K(macro_index), KPC(restore_base_info_));
      }
    } else {
      if (OB_FAIL(share::ObBackupPathUtil::get_macro_block_backup_path(backup_set_dest, macro_index.ls_id_,
          data_type, macro_index.turn_id_, macro_index.retry_id_, macro_index.file_id_, backup_path))) {
        LOG_WARN("failed to get macro block index", K(ret), K(restore_base_info_), K(macro_index), KPC(restore_base_info_));
      }
    }
  }
  return ret;
}

int ObCopyMacroBlockRestoreReader::fetch_macro_block_index_(
    const int64_t block_id_idx,
    const share::ObBackupDataType &backup_data_type,
    blocksstable::ObLogicMacroBlockId &logic_block_id,
    backup::ObBackupMacroBlockIndex &macro_index)
{
  int ret = OB_SUCCESS;
  const share::ObBackupSetFileDesc::Compatible &compatible = restore_base_info_->backup_compatible_;
  if (share::ObBackupSetFileDesc::is_backup_set_support_quick_restore(compatible)) {
    backup::ObBackupDeviceMacroBlockId macro_id;
    if (OB_FAIL(restore_macro_block_id_mgr_->get_macro_block_id(block_id_idx, logic_block_id, macro_id))) {
      LOG_WARN("failed to get macro block id", K(ret), K(block_id_idx));
    } else if (OB_FAIL(macro_id.get_backup_macro_block_index(logic_block_id, macro_index))) {
      LOG_WARN("failed to get backup macro block index", K(ret));
    }
  } else {
    backup::ObBackupPhysicalID physic_block_id;
    if (OB_FAIL(restore_macro_block_id_mgr_->get_macro_block_id(block_id_idx, logic_block_id, physic_block_id))) {
      LOG_WARN("failed to get macro block id", K(ret), K(block_id_idx), K(table_key_), KPC(restore_base_info_));
    } else if (OB_FAIL(physic_block_id.get_backup_macro_block_index(logic_block_id, macro_index))) {
      LOG_WARN("failed to get backup macro block index", K(ret));
    }
  }
  return ret;
}

int ObCopyMacroBlockRestoreReader::convert_logical_id_to_shared_macro_id_(
    const ObLogicMacroBlockId &logic_block_id,
    MacroBlockId &macro_block_id)
{
  int ret = OB_SUCCESS;
  const bool is_shared_storage_mode = GCTX.is_shared_storage_mode();
  macro_block_id.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!logic_block_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("convert logical id to shared macro id get invalid argument", K(ret), K(logic_block_id));
  } else if (!is_shared_storage_mode || !table_key_.is_major_sstable()) {
    //do nothing
  } else {
    // logical id to change to macro block id in shared storage mode
    // second_id:tablet_id, third_id:seq_id, fourth_id:N/A
    // TODO(muwei.ym) this interface will be abandoned in quick restore
    // Here incarnation value is default value : 0
    macro_block_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_block_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);
    macro_block_id.set_second_id(logic_block_id.tablet_id_);
    macro_block_id.set_third_id(logic_block_id.data_seq_.get_data_seq());
  }
  return ret;
}


ObCopyDDLMacroBlockRestoreReader::ObCopyDDLMacroBlockRestoreReader()
  : is_inited_(false),
    table_key_(),
    copy_macro_range_info_(nullptr),
    restore_base_info_(nullptr),
    meta_index_store_(nullptr),
    data_buffer_(),
    allocator_(),
    macro_block_index_(0),
    macro_block_count_(0),
    data_size_(0),
    link_item_()
{
  ObMemAttr attr(MTL_ID(), "CMBReReader");
  allocator_.set_attr(attr);
}

ObCopyDDLMacroBlockRestoreReader::~ObCopyDDLMacroBlockRestoreReader()
{
  allocator_.reset();
}

int ObCopyDDLMacroBlockRestoreReader::init(
    const ObCopyMacroBlockReaderInitParam &param)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (!param.is_valid() || !param.is_leader_restore_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else if (OB_FAIL(alloc_buffers())) {
    LOG_WARN("failed to alloc buffers", K(ret));
  } else {
    table_key_ = param.table_key_;
    copy_macro_range_info_ = param.copy_macro_range_info_;
    restore_base_info_ = param.restore_base_info_;
    meta_index_store_ = param.meta_index_store_;
    if (OB_FAIL(prepare_link_item_())) {
      LOG_WARN("failed to prepare link item", K(ret));
    } else {
      macro_block_count_ = 0;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObCopyDDLMacroBlockRestoreReader::prepare_link_item_()
{
  int ret = OB_SUCCESS;
  share::ObBackupDataType data_type;
  share::ObBackupPath backup_path;
  const int64_t align_size = DIO_READ_ALIGN_SIZE;
  backup::ObBackupMacroBlockIndex macro_index;
  share::ObBackupStorageInfo storage_info;
  share::ObRestoreBackupSetBriefInfo backup_set_brief_info;
  share::ObBackupDest backup_set_dest;
  ObStorageIdMod mod;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_RESTORE;
  int64_t dest_id = 0;
  const backup::ObBackupMetaType meta_type = backup::ObBackupMetaType::BACKUP_SSTABLE_META;
  backup::ObBackupMetaIndex meta_index;
  ObArray<MacroBlockId> macro_block_id_array;
  share::ObBackupDataType user_data_type;
  user_data_type.set_user_data_backup();

  if (OB_FAIL(ObStorageHAUtils::extract_macro_id_from_datum(copy_macro_range_info_->start_macro_block_end_key_, macro_block_id_array))) {
    LOG_WARN("failed to extract macro id from datum", KPC(copy_macro_range_info_), K(table_key_));
  } else if (macro_block_id_array.empty()) {
    //do nothing
  } else if (FALSE_IT(data_type.set_minor_data_backup())) {
  } else if (OB_FAIL(ObRestoreUtils::get_backup_data_type(table_key_, data_type))) {
    LOG_WARN("fail to get backup data type", K(ret), K(table_key_));
  } else if (OB_FAIL(meta_index_store_->get_backup_meta_index(data_type, table_key_.get_tablet_id(), meta_type, meta_index))) {
    LOG_WARN("failed to get backup meta index", K(ret), K(table_key_), K(meta_type), K(data_type));
  } else if (OB_FAIL(restore_base_info_->get_restore_backup_set_dest(meta_index.backup_set_id_, backup_set_brief_info))) {
    LOG_WARN("fail to get backup set dest", K(ret), K(meta_index));
  } else if (OB_FAIL(restore_base_info_->get_restore_data_dest_id(*GCTX.sql_proxy_, MTL_ID(), dest_id))) {
    LOG_WARN("fail to get restore data dest id", K(ret));
  } else if (OB_FALSE_IT(mod.storage_id_ = static_cast<uint64_t>(dest_id))) {
  } else if (OB_FAIL(backup_set_dest.set(backup_set_brief_info.backup_set_path_))) {
    LOG_WARN("fail to set backup set dest", K(ret));
  } else if (OB_FAIL(share::ObBackupPathUtilV_4_3_2::get_macro_block_backup_path(backup_set_dest, meta_index.ls_id_,
      user_data_type, meta_index.turn_id_, meta_index.retry_id_, meta_index.file_id_, backup_path))) {
    LOG_WARN("failed to get macro block index", K(ret), K(restore_base_info_), K(meta_index), KPC(restore_base_info_));
  } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_ddl_sstable_other_block_id_list_in_ss_mode_with_batch(
      backup_set_dest, backup_path.get_obstr(), restore_base_info_->backup_dest_.get_storage_info(),
      mod, meta_index, table_key_, macro_block_id_array.at(0), copy_macro_range_info_->macro_block_count_, link_item_))) {
    LOG_WARN("failed to read ddl sstable other block id", K(ret), K(table_key_), K(meta_index));
  }
  return ret;
}

int ObCopyDDLMacroBlockRestoreReader::alloc_buffers()
{
  int ret = OB_SUCCESS;
  const int64_t READ_BUFFER_SIZE = OB_DEFAULT_MACRO_BLOCK_SIZE * 2;
  char *buf = NULL;
  char *read_buf = NULL;

  // used in init() func, should not check is_inited_
  if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator_.alloc(OB_DEFAULT_MACRO_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret));
  } else if (OB_ISNULL(read_buf = reinterpret_cast<char*>(allocator_.alloc(READ_BUFFER_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc read_buf", K(ret));
  } else {
    data_buffer_.assign(buf, OB_DEFAULT_MACRO_BLOCK_SIZE);
    read_buffer_.assign(read_buf, READ_BUFFER_SIZE);
  }

  if (OB_FAIL(ret)) {
    allocator_.reset();
  }

  return ret;
}

void ObCopyDDLMacroBlockRestoreReader::reset_buffers_()
{
  data_buffer_.set_pos(0);
  read_buffer_.set_pos(0);
  MEMSET(data_buffer_.data(), 0, data_buffer_.capacity());
  MEMSET(read_buffer_.data(), 0, read_buffer_.capacity());
}

int ObCopyDDLMacroBlockRestoreReader::get_next_macro_block(ObICopyMacroBlockReader::CopyMacroBlockReadData &read_data)
{
  int ret = OB_SUCCESS;
  int64_t occupy_size = 0;
  read_data.reset();

  if (OB_FAIL(ret)) {
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (macro_block_count_ == copy_macro_range_info_->macro_block_count_) {
    ret = OB_ITER_END;
  } else if (macro_block_count_ >= link_item_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro block count is bigger than link item count", K(ret), K(macro_block_count_), K(link_item_));
  } else {
    const ObLogicMacroBlockId logic_block_id(1, 1, 1); //TODO(yanfeng.yyy)Fake logical block id
    backup::ObBackupDeviceMacroBlockId physic_block_id;
    share::ObBackupDataType data_type;
    data_type.set_user_data_backup();
    share::ObBackupPath backup_path;
    const int64_t align_size = DIO_READ_ALIGN_SIZE;
    backup::ObBackupMacroBlockIndex macro_index;
    share::ObBackupStorageInfo storage_info;
    share::ObRestoreBackupSetBriefInfo backup_set_brief_info;
    share::ObBackupDest backup_set_dest;
    ObStorageIdMod mod;
    mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_RESTORE;
    int64_t dest_id = 0;

    reset_buffers_();

    physic_block_id = link_item_.at(macro_block_count_).backup_id_;
    if (OB_FAIL(physic_block_id.get_backup_macro_block_index(logic_block_id, macro_index))) {
      LOG_WARN("failed to get backup macro block index", K(ret), K(logic_block_id), K(physic_block_id));
    } else if (OB_FAIL(restore_base_info_->get_restore_backup_set_dest(macro_index.backup_set_id_, backup_set_brief_info))) {
      LOG_WARN("fail to get backup set dest", K(ret), K(macro_index));
    } else if (OB_FAIL(restore_base_info_->get_restore_data_dest_id(*GCTX.sql_proxy_, MTL_ID(), dest_id))) {
      LOG_WARN("fail to get restore data dest id", K(ret));
    } else if (OB_FALSE_IT(mod.storage_id_ = static_cast<uint64_t>(dest_id))) {
    } else if (OB_FAIL(backup_set_dest.set(backup_set_brief_info.backup_set_path_))) {
      LOG_WARN("fail to set backup set dest", K(ret));
    } else if (OB_FAIL(share::ObBackupPathUtilV_4_3_2::get_macro_block_backup_path(backup_set_dest, macro_index.ls_id_,
        data_type, macro_index.turn_id_, macro_index.retry_id_, macro_index.file_id_, backup_path))) {
      LOG_WARN("failed to get macro block index", K(ret), K(restore_base_info_), K(macro_index), KPC(restore_base_info_));
    } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_macro_block_data(backup_path.get_obstr(),
        restore_base_info_->backup_dest_.get_storage_info(), mod, macro_index, align_size, read_buffer_, data_buffer_))) {
      LOG_WARN("failed to read macro block data", K(ret), K(table_key_), K(macro_index), K(physic_block_id), KPC(restore_base_info_));
    } else if (OB_FAIL(read_data.set_macro_data(data_buffer_, false /* is_reuse_macro_block */))) {
      LOG_WARN("failed to set read info", K(ret), K(data_buffer_));
    } else if (FALSE_IT(read_data.set_macro_block_id(link_item_.at(macro_block_count_).macro_id_))) {
    } else {
      data_size_ += data_buffer_.length();
      macro_block_count_++;
      macro_block_index_++;
    }
  }
  return ret;
}

/******************ObCopyMacroBlockHandle*********************/
ObCopyMacroBlockHandle::ObCopyMacroBlockHandle()
  : is_reuse_macro_block_(false),
    read_handle_(),
    allocator_("CMacBlockHandle"),
    macro_meta_(nullptr)
{
}

void ObCopyMacroBlockHandle::reset()
{
  is_reuse_macro_block_ = false;
  read_handle_.reset();
  macro_meta_ = nullptr;
  allocator_.reset();
}

bool ObCopyMacroBlockHandle::is_valid() const
{
  return (is_reuse_macro_block_ || read_handle_.is_valid())
       && OB_NOT_NULL(macro_meta_)
       && macro_meta_->is_valid();
}

int ObCopyMacroBlockHandle::set_macro_meta(
    const blocksstable::ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  if (!macro_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set macro meta get invalid argument", K(ret), K(macro_meta));
  } else if (OB_FAIL(macro_meta.deep_copy(macro_meta_, allocator_))) {
    LOG_WARN("failed to deep copy macro meta", K(ret), K(macro_meta));
  }

  return ret;
}

/******************ObCopyMacroBlockObProducer*********************/
ObCopyMacroBlockObProducer::ObCopyMacroBlockObProducer()
  : is_inited_(false),
    copy_macro_range_info_(),
    data_version_(0),
    macro_idx_(0),
    handle_idx_(0),
    prefetch_meta_time_(0),
    tablet_allocator_(),
    tablet_handle_(),
    sstable_handle_(),
    sstable_(nullptr),
    datum_range_(),
    allocator_(),
    second_meta_iterator_(),
    io_allocator_("CMBP_IOUB", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    meta_row_buf_("CopyMacroMetaRow")
{
  ObMemAttr attr_tablet_alloc(MTL_ID(), "HaTabletHdl");
  tablet_allocator_.set_attr(attr_tablet_alloc);
  ObMemAttr attr_copy_macro_block(MTL_ID(), "CopyMacroBlock");
  allocator_.set_attr(attr_copy_macro_block);
}

ObCopyMacroBlockObProducer::~ObCopyMacroBlockObProducer()
{
  for (int64_t i = 0; i < MAX_PREFETCH_MACRO_BLOCK_NUM; ++i) {
    copy_macro_block_handle_[i].reset();
  }
  second_meta_iterator_.reset();
}

int ObCopyMacroBlockObProducer::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObITable::TableKey &table_key,
    const ObCopyMacroRangeInfo &copy_macro_range_info,
    const int64_t data_version,
    const SCN backfill_tx_scn)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  ObTablet* tablet = nullptr;
  const bool is_reverse_scan = false;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObSSTableMetaHandle meta_handle;
  common::ObSafeArenaAllocator allocator(allocator_);
  ObTabletMapKey map_key;
  map_key.ls_id_ = ls_id;
  map_key.tablet_id_ = table_key.get_tablet_id();

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !table_key.is_valid()
      || !copy_macro_range_info.is_valid() || data_version < 0 || !backfill_tx_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(ls_id), K(table_key),
        K(copy_macro_range_info), K(data_version), K(backfill_tx_scn));
  } else if (OB_FAIL(guard.switch_to(tenant_id))) {
    LOG_WARN("switch tenant failed", K(ret), K(tenant_id));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("fail to get log stream", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", KR(ret), K(tenant_id), K(ls_id), KPC(ls));
  } else if (OB_FAIL(ls->ha_get_tablet_without_memtables(
      WashTabletPriority::WTP_LOW, map_key, tablet_allocator_, tablet_handle_))) {
    LOG_WARN("failed to ha get tablet with allocator without memtables", K(ret), K(map_key));
  } else if (OB_UNLIKELY(nullptr == (tablet = tablet_handle_.get_obj()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet not be NULL", KR(ret), K(tenant_id), K(ls_id), KPC(tablet));
  } else if (OB_FAIL(tablet->get_table(table_key, sstable_handle_))) {
    LOG_WARN("failed to get table", K(ret), K(table_key));
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SSTABLE_NOT_EXIST;
    }
  } else if (OB_FAIL(sstable_handle_.get_sstable(sstable_))) {
    LOG_WARN("failed to get sstable", K(ret), K(table_key));
  } else if (OB_FAIL(sstable_->get_meta(meta_handle, &allocator))) {
    LOG_WARN("failed to get sstable meta", K(ret), K(table_key));
  } else if (backfill_tx_scn != meta_handle.get_sstable_meta().get_basic_meta().filled_tx_scn_) {
    ret = OB_SSTABLE_NOT_EXIST;
    LOG_WARN("sstable has been changed", K(ret), K(table_key), K(backfill_tx_scn), KPC(sstable_));
  } else if (OB_FAIL(copy_macro_range_info_.assign(copy_macro_range_info))) {
    LOG_WARN("failed to copy macro range info", K(ret), K(table_key), K(copy_macro_range_info));
  } else {
    datum_range_.set_start_key(copy_macro_range_info_.start_macro_block_end_key_);
    datum_range_.end_key_.set_max_rowkey();
    datum_range_.set_left_closed();
    datum_range_.set_right_open();

    const storage::ObITableReadInfo *index_read_info = NULL;

    if (OB_FAIL(tablet->get_sstable_read_info(sstable_, index_read_info))) {
      LOG_WARN("failed to get index read info ", KR(ret), K(sstable_));
    } else if (OB_FAIL(second_meta_iterator_.open(datum_range_, blocksstable::DATA_BLOCK_META,
         *sstable_, *index_read_info, allocator_, is_reverse_scan))) {
      LOG_WARN("failed to open second meta iterator", K(ret), K(ls_id), K(table_key), K(copy_macro_range_info));
    } else {
      data_version_ = data_version;
      macro_idx_ = -1;
      handle_idx_ = 0;
      is_inited_ = true;
      LOG_INFO("succeed to init macro block producer",
          K(table_key), K(data_version), K(backfill_tx_scn), K(copy_macro_range_info));
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < MAX_PREFETCH_MACRO_BLOCK_NUM; ++i) {
      if (OB_ISNULL(io_buf_[i] = reinterpret_cast<char*>(allocator_.alloc(OB_STORAGE_OBJECT_MGR.get_macro_block_size())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        int64_t io_size = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
        STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret), K(io_size));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(prefetch_())) {
      LOG_WARN("failed to prefetch", K(ret));
    }
  }
  return ret;
}

int ObCopyMacroBlockObProducer::get_next_macro_block(
    blocksstable::ObBufferReader &data,
    ObCopyMacroBlockHeader &copy_macro_block_header)
{
  int ret = OB_SUCCESS;
  copy_macro_block_header.reset();
  meta_row_buf_.reuse();
  int64_t occupy_size = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (macro_idx_ < 0 || macro_idx_ > copy_macro_range_info_.macro_block_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid macro_idx_", K(ret), K(macro_idx_), K(copy_macro_range_info_));
  } else if (copy_macro_range_info_.macro_block_count_ == macro_idx_) {
    ret = OB_ITER_END;
    LOG_INFO("get next macro block end");
  } else if (!copy_macro_block_handle_[handle_idx_].is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("copy macro block handle is not valid, cannot wait", K(ret), K(handle_idx_));
  } else if (!copy_macro_block_handle_[handle_idx_].is_reuse_macro_block_
      && OB_FAIL(copy_macro_block_handle_[handle_idx_].read_handle_.wait())) {
    LOG_WARN("failed to wait read handle", K(ret));
  } else {
    if (copy_macro_block_handle_[handle_idx_].is_reuse_macro_block_) {
      // only copy macro meta when reuse macro block
      blocksstable::ObDatumRow macro_meta_row;
      common::ObArenaAllocator meta_row_allocator; // use temporary allocator to get datum row
      int64_t pos = 0;
      uint64_t data_version = 0;

      if (OB_ISNULL(copy_macro_block_handle_[handle_idx_].macro_meta_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("macro meta is null", K(ret), K(handle_idx_));
      } else if (!copy_macro_block_handle_[handle_idx_].macro_meta_->is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("macro meta is not valid", K(ret), KPC(copy_macro_block_handle_[handle_idx_].macro_meta_));
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
        LOG_WARN("fail to get min data version", K(ret), K(MTL_ID()), K(data_version));
      } else if (OB_FAIL(macro_meta_row.init(copy_macro_block_handle_[handle_idx_].macro_meta_->get_meta_val().rowkey_count_ + 1))) {
        // meta row's cell: all row keys (key) + value column
        LOG_WARN("failed to init macro meta row", K(ret), KPC(copy_macro_block_handle_[handle_idx_].macro_meta_));
      } else if (OB_FAIL(copy_macro_block_handle_[handle_idx_].macro_meta_->build_row(macro_meta_row, meta_row_allocator, data_version))) {
        LOG_WARN("failed to build macro row", K(ret), KPC(copy_macro_block_handle_[handle_idx_].macro_meta_), K(data_version));
      } else if (OB_FAIL(meta_row_buf_.write_serialize(macro_meta_row))) {
        LOG_WARN("failed to write serialize macro meta row into meta row buf", K(ret), K(macro_meta_row), K_(meta_row_buf));
      } else if (FALSE_IT(occupy_size = meta_row_buf_.length())) {
      } else {
        data.assign(meta_row_buf_.data(), occupy_size);
        copy_macro_block_header.occupy_size_ = occupy_size;
        copy_macro_block_header.is_reuse_macro_block_ = true;
        copy_macro_block_header.data_type_ = ObCopyMacroBlockDataType::MACRO_META_ROW;
      }
    } else {
      blocksstable::ObMacroBlockCommonHeader common_header;
      int64_t pos = 0;

      if (OB_FAIL(common_header.deserialize(
          copy_macro_block_handle_[handle_idx_].read_handle_.get_buffer(),
          copy_macro_block_handle_[handle_idx_].read_handle_.get_data_size(), pos))) {
        STORAGE_LOG(ERROR, "Deserialize common header failed, ", K(ret), "read handle",
            copy_macro_block_handle_[handle_idx_].read_handle_, K(pos), K(common_header));
      } else if (OB_FAIL(common_header.check_integrity())) {
        ret = OB_INVALID_DATA;
        STORAGE_LOG(ERROR, "Invalid common header, ", K(ret), K(common_header));
      } else {
        occupy_size = common_header.get_header_size() + common_header.get_payload_size();
        data.assign(copy_macro_block_handle_[handle_idx_].read_handle_.get_buffer(), occupy_size);
        copy_macro_block_header.is_reuse_macro_block_ = false;
        copy_macro_block_header.occupy_size_ = occupy_size;
        copy_macro_block_header.data_type_ = ObCopyMacroBlockDataType::MACRO_DATA;
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(prefetch_())) {
      LOG_WARN("failed to do prefetch", K(ret));
    }
  }
  return ret;
}

int ObCopyMacroBlockObProducer::prefetch_()
{
  int ret = OB_SUCCESS;
  blocksstable::ObStorageObjectReadInfo read_info;
  prefetch_meta_time_ = ObTimeUtility::current_time();
  ++macro_idx_;
  handle_idx_ = (handle_idx_ + 1) % MAX_PREFETCH_MACRO_BLOCK_NUM;
  copy_macro_block_handle_[handle_idx_].reset();
  ObDataMacroBlockMeta macro_meta;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (macro_idx_ < 0 || macro_idx_ > copy_macro_range_info_.macro_block_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid macro_idx_", K(ret), K(macro_idx_), K(copy_macro_range_info_));
  } else if (macro_idx_ == copy_macro_range_info_.macro_block_count_) {
    // no need to
    LOG_INFO("has finish, no need do prefetch", K(macro_idx_), K(copy_macro_range_info_));
  } else {
    int64_t copy_snapshot_version = 0;

    if (OB_FAIL(second_meta_iterator_.get_next(macro_meta))) {
      LOG_WARN("failed to get next macro meta", K(ret), K(macro_idx_), K(copy_macro_range_info_));
    } else if (OB_FAIL(copy_macro_block_handle_[handle_idx_].set_macro_meta(macro_meta))) {
      LOG_WARN("failed to set macro meta", K(ret), K(macro_meta));
    } else if (macro_meta.get_logic_id().logic_version_ <= data_version_
               || macro_meta.get_macro_id().is_backup_id()) {
      copy_macro_block_handle_[handle_idx_].is_reuse_macro_block_ = true;
      // if macro block is local, reset macro meta id to default
      // DEFAULT_IDX_ROW_MACRO_ID is also local id
      if (macro_meta.get_macro_id().is_local_id()) {
        copy_macro_block_handle_[handle_idx_].macro_meta_->val_.macro_id_ = ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID;
      }
    } else {
      copy_macro_block_handle_[handle_idx_].is_reuse_macro_block_ = false;

      read_info.macro_block_id_ = macro_meta.get_macro_id();
      read_info.offset_ = sstable_->get_macro_offset();
      read_info.size_ = sstable_->get_macro_read_size();
      read_info.io_desc_.set_mode(ObIOMode::READ);
      read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_MIGRATE_READ);
      read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
      read_info.buf_ = io_buf_[handle_idx_];
      read_info.mtl_tenant_id_ = MTL_ID();
      read_info.io_desc_.set_sys_module_id(ObIOModule::HA_COPY_MACRO_BLOCK_IO);
      if (OB_FAIL(ObObjectManager::async_read_object(read_info, copy_macro_block_handle_[handle_idx_].read_handle_))) {
        STORAGE_LOG(WARN, "Fail to async read block, ", K(ret), K(read_info));
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("do prefetch", K(macro_idx_), "macro block count",copy_macro_range_info_.macro_block_count_ ,
          "logical id", macro_meta.get_logic_id(), "physical id", macro_meta.get_macro_id(), K(data_version_), "src macro version", copy_snapshot_version);
    }
  }
  return ret;
}

ObCopyTabletInfoObReader::ObCopyTabletInfoObReader()
  : is_inited_(false),
    rpc_reader_()
{
}

ObCopyTabletInfoObReader::~ObCopyTabletInfoObReader()
{
}

int ObCopyTabletInfoObReader::init(
    const ObStorageHASrcInfo &src_info,
    const obrpc::ObCopyTabletInfoArg &rpc_arg,
    obrpc::ObStorageRpcProxy &srv_rpc_proxy,
    common::ObInOutBandwidthThrottle &bandwidth_throttle)
{
  int ret = OB_SUCCESS;
  int64_t rpc_timeout = FETCH_TABLET_INFO_TIMEOUT;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (OB_UNLIKELY(!src_info.is_valid())
             || OB_UNLIKELY(!rpc_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src_info), K(rpc_arg));
  } else if (OB_FAIL(rpc_reader_.init(bandwidth_throttle))) {
    LOG_WARN("fail to init tablet info rpc reader", K(ret));
  } else if (FALSE_IT(rpc_timeout = ObStorageHAUtils::get_rpc_timeout())) {
  } else if (OB_FAIL(srv_rpc_proxy.to(src_info.src_addr_).by(rpc_arg.tenant_id_).timeout(rpc_timeout).dst_cluster_id(src_info.cluster_id_)
                .ratelimit(true).bg_flow(obrpc::ObRpcProxy::BACKGROUND_FLOW)
                .group_id(share::OBCG_STORAGE_STREAM)
                .fetch_tablet_info(rpc_arg, rpc_reader_.get_rpc_buffer(), rpc_reader_.get_handle()))) {
    LOG_WARN("failed to send fetch tablet info rpc", K(ret), K(src_info), K(rpc_arg));
  } else {
    is_inited_ = true;
    LOG_INFO("succeed to init copy tablet info reader", K(src_info), K(rpc_arg));
  }

  return ret;
}

int ObCopyTabletInfoObReader::fetch_tablet_info(obrpc::ObCopyTabletInfo &tablet_info)
{
  int ret = OB_SUCCESS;
  tablet_info.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("pg base data meta ob reader do not init", K(ret));
  } else if (OB_FAIL(rpc_reader_.fetch_and_decode(tablet_info))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to fetch and decode partition meta info", K(ret));
    }
  } else if (OB_FAIL(ObStorageHAUtils::check_server_version(tablet_info.version_))) {
    LOG_WARN("failed to check server version", K(ret));
  } else if (!tablet_info.is_valid()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid tablet info", K(ret), K(tablet_info));
  }
  return ret;
}

ObCopyTabletInfoRestoreReader::ObCopyTabletInfoRestoreReader()
  : is_inited_(false),
    restore_base_info_(nullptr),
    tablet_id_array_(),
    meta_index_store_(nullptr),
    tablet_id_index_(0)
{
}

ObCopyTabletInfoRestoreReader::~ObCopyTabletInfoRestoreReader()
{
}

int ObCopyTabletInfoRestoreReader::init(
    const ObRestoreBaseInfo &restore_base_info,
    const common::ObIArray<common::ObTabletID> &tablet_id_array,
    backup::ObBackupMetaIndexStoreWrapper &meta_index_store)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (OB_UNLIKELY(!restore_base_info.is_valid())
             || OB_UNLIKELY(tablet_id_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(restore_base_info), K(tablet_id_array));
  } else if (OB_FAIL(tablet_id_array_.assign(tablet_id_array))) {
    LOG_WARN("failed to assign tablet id array", K(ret), K(tablet_id_array));
  } else {
    restore_base_info_ = &restore_base_info;
    meta_index_store_ = &meta_index_store;
    tablet_id_index_ = 0;
    is_inited_ = true;
    LOG_INFO("succeed to init copy tablet info restore reader", K(restore_base_info), K(tablet_id_array));
  }

  return ret;
}

int ObCopyTabletInfoRestoreReader::fetch_tablet_info(obrpc::ObCopyTabletInfo &tablet_info)
{
  int ret = OB_SUCCESS;
  tablet_info.reset();
  backup::ObBackupMetaIndex tablet_meta_index;
  const backup::ObBackupMetaType tablet_meta_type = backup::ObBackupMetaType::BACKUP_TABLET_META;
  share::ObBackupPath tablet_meta_backup_path;
  backup::ObBackupTabletMeta backup_tablet_meta;
  share::ObBackupDataType data_type;
  share::ObBackupStorageInfo storage_info;
  const ObTabletRestoreStatus::STATUS restore_status = ObTabletRestoreStatus::EMPTY;
  const ObTabletDataStatus::STATUS data_status = ObTabletDataStatus::COMPLETE;
  ObStorageIdMod mod;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_RESTORE;
  int64_t dest_id = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy tablet info restore reader do not init", K(ret));
  } else if (tablet_id_index_ == tablet_id_array_.count()) {
    ret = OB_ITER_END;
  } else if (FALSE_IT(data_type.type_ = tablet_id_array_.at(tablet_id_index_).is_ls_inner_tablet()
      ? share::ObBackupDataType::BACKUP_SYS : share::ObBackupDataType::BACKUP_MINOR)) {
  } else {
    const common::ObTabletID &tablet_id = tablet_id_array_.at(tablet_id_index_);
    tablet_info.tablet_id_ = tablet_id;
    tablet_info.version_ = 0; // for restore this is invalid
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(restore_base_info_->get_restore_data_dest_id(*GCTX.sql_proxy_, MTL_ID(), dest_id))) {
      LOG_WARN("fail to get restore data dest id", K(ret));
    } else if (OB_FALSE_IT(mod.storage_id_ = static_cast<uint64_t>(dest_id))) {
    } else if (OB_FAIL(meta_index_store_->get_backup_meta_index(data_type, tablet_id, tablet_meta_type, tablet_meta_index))
               && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get backup meta index", K(ret), K(tablet_id), KPC(restore_base_info_));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      tablet_info.status_ = ObCopyTabletStatus::TABLET_NOT_EXIST;
      LOG_INFO("tablet not exist", K(tablet_id));
    } else if (OB_FAIL(get_macro_block_backup_path_(tablet_meta_index, data_type, tablet_meta_backup_path))) {
      LOG_WARN("failed to get macro block backup path", K(ret), KPC(restore_base_info_));
    } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_tablet_meta(tablet_meta_backup_path.get_obstr(),
        restore_base_info_->backup_dest_.get_storage_info(), mod, tablet_meta_index, backup_tablet_meta))) {
      LOG_WARN("failed to read tablet meta", K(ret), K(data_type), KPC(restore_base_info_));
    } else if (!backup_tablet_meta.is_valid()
        || !backup_tablet_meta.tablet_meta_.ha_status_.is_none()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup tablet meta is invalid", K(ret), K(backup_tablet_meta));
    } else if (OB_FAIL(tablet_info.param_.assign(backup_tablet_meta.tablet_meta_))) {
      LOG_WARN("failed to assign backup tablet meta", K(ret), K(backup_tablet_meta));
    } else {
      tablet_info.data_size_ = 0;
      tablet_info.status_ = ObCopyTabletStatus::TABLET_EXIST;
      if (OB_FAIL(tablet_info.param_.ha_status_.set_restore_status(restore_status))) {
        LOG_WARN("failed to set restore status", K(ret), K(restore_status));
      } else if (OB_FAIL(tablet_info.param_.ha_status_.set_data_status(data_status))) {
        LOG_WARN("failed to set data status", K(ret), K(data_status));
      } else if (!tablet_info.is_valid()) {
        ret = OB_ERR_SYS;
        LOG_ERROR("invalid tablet info", K(ret), K(tablet_info), K(backup_tablet_meta));
      }
      LOG_INFO("succeed to get tablet meta", K(backup_tablet_meta), K(tablet_info));
    }

    ++tablet_id_index_;
  }
  return ret;
}

int ObCopyTabletInfoRestoreReader::get_macro_block_backup_path_(
    const backup::ObBackupMetaIndex &tablet_meta_index,
    const share::ObBackupDataType data_type,
    ObBackupPath &tablet_meta_backup_path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(restore_base_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("restore base info should not be null", K(ret));
  } else {
    const share::ObBackupSetFileDesc::Compatible &compatible = restore_base_info_->backup_compatible_;
    if (share::ObBackupSetFileDesc::is_backup_set_support_quick_restore(compatible)) {
      if (OB_FAIL(share::ObBackupPathUtilV_4_3_2::get_macro_block_backup_path(restore_base_info_->backup_dest_,
          tablet_meta_index.ls_id_, data_type, tablet_meta_index.turn_id_, tablet_meta_index.retry_id_,
          tablet_meta_index.file_id_, tablet_meta_backup_path))) {
        LOG_WARN("failed to get macro block backup path", K(ret), KPC(restore_base_info_));
      }
    } else {
      if (OB_FAIL(share::ObBackupPathUtil::get_macro_block_backup_path(restore_base_info_->backup_dest_,
          tablet_meta_index.ls_id_, data_type, tablet_meta_index.turn_id_, tablet_meta_index.retry_id_,
          tablet_meta_index.file_id_, tablet_meta_backup_path))) {
        LOG_WARN("failed to get macro block backup path", K(ret), KPC(restore_base_info_));
      }
    }
  }
  return ret;
}

ObCopyTabletInfoObProducer::ObCopyTabletInfoObProducer()
  : is_inited_(false),
    tablet_id_array_(),
    tablet_index_(0),
    ls_handle_()
{
}

ObCopyTabletInfoObProducer::~ObCopyTabletInfoObProducer()
{
}

int ObCopyTabletInfoObProducer::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObIArray<common::ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("copy table info ob producer init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || tablet_id_array.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("copy tablet info ob producer init get invalid argument", K(ret), K(tenant_id),
        K(ls_id), K(tablet_id_array));
  } else if (tenant_id != MTL_ID()) {
    LOG_WARN("tenant is not match", K(ret), K(tenant_id));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle_, ObLSGetMod::HA_MOD))) {
    LOG_WARN("fail to get log stream", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_UNLIKELY(nullptr == (ls_handle_.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(tablet_id_array_.assign(tablet_id_array))) {
    LOG_WARN("failed to assign tablet id array", K(ret), K(tenant_id), K(ls_id), K(tablet_id_array));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObCopyTabletInfoObProducer::get_next_tablet_info(obrpc::ObCopyTabletInfo &tablet_info)
{
  int ret = OB_SUCCESS;
  tablet_info.reset();
  ObLS *ls = nullptr;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy tablet info ob producer do not init", K(ret));
  } else if (tablet_index_ == tablet_id_array_.count()) {
    ret = OB_ITER_END;
  } else {
    const ObTabletID &tablet_id = tablet_id_array_.at(tablet_index_);
    tablet_info.tablet_id_ = tablet_id;
    if (OB_ISNULL(ls = ls_handle_.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream should not be NULL", K(ret), KP(ls));
    } else if (OB_FAIL(ObStorageHAUtils::get_server_version(tablet_info.version_))) {
      LOG_WARN("failed to get server version", K(ret), K(tablet_info));
    } else if (OB_FAIL(ls->ha_get_tablet(tablet_id, tablet_handle))
               && OB_TABLET_NOT_EXIST != ret) {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_id), K(tablet_handle));
    } else if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      tablet_info.status_ = ObCopyTabletStatus::TABLET_NOT_EXIST;
      if (OB_FAIL(tablet_info.param_.build_deleted_tablet_info(ls->get_ls_id(), tablet_id))) {
        LOG_WARN("failed to build deleted tablet info", K(ret), K(tablet_id));
      } else {
        LOG_INFO("tablet not exist, build deleted tablet info", K(tablet_id));
      }
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(tablet_id));
    } else if (OB_FAIL(tablet->build_migration_tablet_param(tablet_info.param_))) {
      LOG_WARN("failed to build migration tablet param", K(ret), K(tablet_id));
    } else if (OB_FAIL(tablet->get_ha_sstable_size(tablet_info.data_size_))) {
      LOG_WARN("failed to get sstable size", K(ret), K(tablet_id));
    } else {
      tablet_info.status_ = ObCopyTabletStatus::TABLET_EXIST;
      LOG_INFO("succeed get copy tablet info", K(tablet_info), K(tablet_index_));
    }
    tablet_index_++;
  }
  return ret;
}

ObCopySSTableInfoObReader::ObCopySSTableInfoObReader()
  : is_inited_(false),
    rpc_reader_(),
    allocator_(),
    is_sstable_iter_end_(true),
    sstable_index_(0),
    sstable_count_(0)
{
  ObMemAttr attr(MTL_ID(), "CSSTObReader");
  allocator_.set_attr(attr);
}

int ObCopySSTableInfoObReader::init(
    const ObStorageHASrcInfo &src_info,
    const obrpc::ObCopyTabletsSSTableInfoArg &rpc_arg,
    obrpc::ObStorageRpcProxy &srv_rpc_proxy,
    common::ObInOutBandwidthThrottle &bandwidth_throttle)
{
  int ret = OB_SUCCESS;
  int64_t rpc_timeout = FETCH_TABLET_SSTABLE_INFO_TIMEOUT;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (OB_UNLIKELY(!src_info.is_valid())
             || OB_UNLIKELY(!rpc_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src_info), K(rpc_arg));
  } else if (OB_FAIL(rpc_reader_.init(bandwidth_throttle))) {
    LOG_WARN("fail to init tablet info rpc reader", K(ret));
  } else if (FALSE_IT(rpc_timeout = ObStorageHAUtils::get_rpc_timeout())) {
  } else if (OB_FAIL(srv_rpc_proxy.to(src_info.src_addr_).by(rpc_arg.tenant_id_)
                .timeout(rpc_timeout).dst_cluster_id(src_info.cluster_id_)
                .ratelimit(true).bg_flow(obrpc::ObRpcProxy::BACKGROUND_FLOW)
                .group_id(share::OBCG_STORAGE_STREAM)
                .fetch_tablet_sstable_info(rpc_arg, rpc_reader_.get_rpc_buffer(), rpc_reader_.get_handle()))) {
    LOG_WARN("failed to send fetch tablet info rpc", K(ret), K(src_info), K(rpc_arg));
  } else {
    is_inited_ = true;
    LOG_INFO("succeed to init copy tablet sstable info reader", K(src_info), K(rpc_arg));
  }
  return ret;
}

int ObCopySSTableInfoObReader::fetch_sstable_meta_(obrpc::ObCopyTabletSSTableInfo &sstable_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(rpc_reader_.fetch_and_decode(sstable_info))) {
    LOG_WARN("failed to fetch and decode sstable meta", K(ret));
  } else if (!sstable_info.is_valid()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid sstable_meta", K(ret), K(sstable_info));
  }

  return ret;
}

int ObCopySSTableInfoObReader::get_next_sstable_info(
    obrpc::ObCopyTabletSSTableInfo &sstable_info)
{
  int ret = OB_SUCCESS;
  sstable_info.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable info ob reader do not init", K(ret));
  } else if (sstable_index_ >= sstable_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable index is unexpected", K(ret), K(sstable_index_), K(sstable_count_));
  } else if (OB_FAIL(fetch_sstable_meta_(sstable_info))) {
    LOG_WARN("failed ot fetch sstable meta", K(ret), K(sstable_index_), K(sstable_count_));
  } else {
    sstable_index_++;
    if (sstable_index_ == sstable_count_) {
      is_sstable_iter_end_ = true;
    }
  }
  return ret;
}

int ObCopySSTableInfoObReader::get_next_tablet_sstable_header(
    obrpc::ObCopyTabletSSTableHeader &copy_header)
{
  int ret = OB_SUCCESS;
  copy_header.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable info ob reader do not init", K(ret));
  } else if (!is_sstable_iter_end_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable iter is not end, can not get next tablet sstable header",
        K(ret), K(is_sstable_iter_end_));
  } else if (OB_FAIL(rpc_reader_.fetch_and_decode(copy_header))) {
    if (OB_ITER_END == ret) {
      //do nothing
    } else {
      LOG_WARN("failed to fetch and decode copy header", K(ret));
    }
  } else if (!copy_header.is_valid()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("copy header is invalid", K(ret), K(copy_header));
  } else if (OB_FAIL(ObStorageHAUtils::check_server_version(copy_header.version_))) {
    LOG_WARN("failed to check server version", K(ret));
  } else {
    is_sstable_iter_end_ = copy_header.sstable_count_ > 0 ? false : true;
    sstable_index_ = 0;
    sstable_count_ = copy_header.sstable_count_;
  }
  return ret;
}


ObCopySSTableInfoRestoreReader::ObCopySSTableInfoRestoreReader()
  : is_inited_(false),
    restore_base_info_(nullptr),
    restore_action_(ObTabletRestoreAction::MAX),
    tablet_id_array_(),
    meta_index_store_(nullptr),
    tablet_index_(0),
    sstable_count_(0),
    sstable_index_(0),
    is_sstable_iter_end_(true),
    backup_sstable_meta_array_(),
    allocator_(),
    ls_id_(),
    ls_handle_(),
    remote_sstable_producer_()
{
  ObMemAttr attr(MTL_ID(), "CSSTREReader");
  allocator_.set_attr(attr);
}

int ObCopySSTableInfoRestoreReader::init(
    const share::ObLSID &ls_id,
    const ObRestoreBaseInfo &restore_base_info,
    const ObTabletRestoreAction::ACTION &restore_action,
    const common::ObIArray<common::ObTabletID> &tablet_id_array,
    backup::ObBackupMetaIndexStoreWrapper &meta_index_store)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
             || OB_UNLIKELY(!restore_base_info.is_valid())
             || OB_UNLIKELY(tablet_id_array.empty())
             || OB_UNLIKELY(!ObTabletRestoreAction::is_valid(restore_action))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(restore_base_info), K(tablet_id_array), K(restore_action));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle_, ObLSGetMod::HA_MOD))) {
    LOG_WARN("fail to get log stream", KR(ret), K(ls_id));
  } else if (OB_FAIL(tablet_id_array_.assign(tablet_id_array))) {
    LOG_WARN("failed to assign tablet id array", K(ret), K(tablet_id_array));
  } else {
    restore_base_info_ = &restore_base_info;
    restore_action_ = restore_action;
    meta_index_store_ = &meta_index_store;
    tablet_index_ = 0;
    sstable_count_ = 0;
    sstable_index_ = 0;
    is_sstable_iter_end_ = true;
    is_inited_ = true;
    LOG_INFO("succeed to init copy tablet sstable info restore reader", K(restore_base_info), K(tablet_id_array), K(restore_action));
  }
  return ret;
}

int ObCopySSTableInfoRestoreReader::fetch_sstable_meta_(
    const backup::ObBackupSSTableMeta &backup_sstable_meta,
    obrpc::ObCopyTabletSSTableInfo &sstable_info)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable info restore reader do not init", K(ret));
  } else if (!backup_sstable_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fetch sstable meta get invalid argument", K(ret), K(backup_sstable_meta));
  } else if (OB_FAIL(sstable_info.param_.assign(backup_sstable_meta.sstable_meta_))) {
    LOG_WARN("failed to assign sstable meta", K(ret), K(backup_sstable_meta));
  } else {
    sstable_info.table_key_ = backup_sstable_meta.sstable_meta_.table_key_;
    sstable_info.tablet_id_ = backup_sstable_meta.tablet_id_;
    if (!sstable_info.is_valid()) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid sstable_meta", K(ret), K(sstable_info), K(backup_sstable_meta));
    }
  }

  return ret;
}

int ObCopySSTableInfoRestoreReader::get_next_sstable_info(
    obrpc::ObCopyTabletSSTableInfo &sstable_info)
{
  int ret = OB_SUCCESS;
  sstable_info.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable info restore reader do not init", K(ret));
  } else if (sstable_index_ >= sstable_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable index is unexpected", K(ret), K(sstable_index_), K(backup_sstable_meta_array_));
  } else if (ObTabletRestoreAction::is_restore_replace_remote_sstable(restore_action_)) {
    if (OB_FAIL(get_next_sstable_info_from_local_(sstable_info))) {
      LOG_WARN("failed to get next sstable from local", K(ret));
    }
  } else if (OB_FAIL(get_next_sstable_info_from_backup_(sstable_info))) {
    LOG_WARN("failed to get next sstable from backup", K(ret));
  }
  return ret;
}

int ObCopySSTableInfoRestoreReader::get_tablet_sstable_header_from_backup_(
    const common::ObTabletID &tablet_id,
    obrpc::ObCopyTabletSSTableHeader &copy_header)
{
  int ret = OB_SUCCESS;
  copy_header.tablet_id_ = tablet_id;
  if (OB_FAIL(get_backup_tablet_meta_(tablet_id, copy_header))) {
    LOG_WARN("failed to get tablet meta", K(ret), K(tablet_id));
  } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == copy_header.status_) {
    sstable_count_ = 0;
    sstable_index_ = 0;
    is_sstable_iter_end_ = true;
    copy_header.sstable_count_ = 0;
    copy_header.version_ = restore_base_info_->backup_cluster_version_;
    tablet_index_++;
  } else if (OB_FAIL(get_backup_sstable_metas_(tablet_id))) {
    LOG_WARN("failed to get backup sstable metas", K(ret), K(tablet_id), KPC(restore_base_info_));
  } else {
    sstable_count_ = backup_sstable_meta_array_.count();
    sstable_index_ = 0;
    is_sstable_iter_end_ = sstable_count_ > 0 ? false : true;
    copy_header.sstable_count_ = sstable_count_;
    copy_header.version_ = restore_base_info_->backup_cluster_version_;
    tablet_index_++;
  }

  return ret;
}

int ObCopySSTableInfoRestoreReader::get_tablet_sstable_header_from_local_(
    const common::ObTabletID &tablet_id,
    obrpc::ObCopyTabletSSTableHeader &copy_header)
{
  int ret = OB_SUCCESS;
  remote_sstable_producer_.reset();
  copy_header.tablet_id_ = tablet_id;
  if (OB_FAIL(remote_sstable_producer_.init(tablet_id, ls_handle_.get_ls()))) {
    LOG_WARN("failed to init producer", K(ret), K(tablet_id));
  } else if (OB_FAIL(remote_sstable_producer_.get_copy_tablet_sstable_header(copy_header))) {
    LOG_WARN("failed to get copy tablet sstable header", K(ret), K(tablet_id));
  } else {
    is_sstable_iter_end_ = (0 == copy_header.sstable_count_);
    sstable_index_ = 0;
    sstable_count_ = copy_header.sstable_count_;
    tablet_index_++;
  }
  return ret;
}

int ObCopySSTableInfoRestoreReader::get_next_sstable_info_from_backup_(
    obrpc::ObCopyTabletSSTableInfo &sstable_info)
{
  int ret = OB_SUCCESS;
  const backup::ObBackupSSTableMeta &backup_sstable_meta = backup_sstable_meta_array_.at(sstable_index_);
  if (OB_FAIL(fetch_sstable_meta_(backup_sstable_meta, sstable_info))) {
    LOG_WARN("failed to fetch sstable meta", K(ret), K(backup_sstable_meta));
  } else {
    sstable_index_++;
    is_sstable_iter_end_ = (sstable_index_ == sstable_count_);
    LOG_INFO("succeed get sstable info", K(sstable_info));
  }
  LOG_INFO("dump fetch sstable info", K(sstable_count_), K(sstable_index_),
      "tablet_id_array_count", tablet_id_array_.count(), K(tablet_index_), K(is_sstable_iter_end_));
  return ret;
}

int ObCopySSTableInfoRestoreReader::get_next_sstable_info_from_local_(
    obrpc::ObCopyTabletSSTableInfo &sstable_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(remote_sstable_producer_.get_next_sstable_info(sstable_info))) {
    LOG_WARN("failed to get next sstable meta", K(ret), K(sstable_index_), K(sstable_count_));
  } else {
    sstable_index_++;
    is_sstable_iter_end_ = (sstable_index_ == sstable_count_);
    LOG_INFO("succeed get sstable info", K(sstable_info));
  }
  LOG_INFO("dump fetch sstable info", K(sstable_count_), K(sstable_index_),
      "tablet_id_array_count", tablet_id_array_.count(), K(tablet_index_), K(is_sstable_iter_end_));
  return ret;
}

int ObCopySSTableInfoRestoreReader::get_backup_sstable_metas_(
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  share::ObBackupDataType data_type;
  ObArray<backup::ObBackupSSTableMeta> backup_sstable_meta_array;

  backup_sstable_meta_array_.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable info restore reader get invalid argument", K(ret), K(tablet_id));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get backup sstable metas get invalid argument", K(ret), K(tablet_id));
  } else if (tablet_id.is_ls_inner_tablet()) {
    data_type.set_sys_data_backup();
    if (OB_FAIL(inner_get_backup_sstable_metas_(tablet_id, data_type, backup_sstable_meta_array))) {
      LOG_WARN("failed to inner get backup sstable metas", K(ret), K(tablet_id), K(data_type));
    } else if (OB_FAIL(backup_sstable_meta_array_.assign(backup_sstable_meta_array))) {
      LOG_WARN("failed to assign backup sstable meta array", K(ret), K(tablet_id), K(backup_sstable_meta_array));
    }
  } else {
    //get major sstable meta
    data_type.set_major_data_backup();
    if (OB_FAIL(inner_get_backup_sstable_metas_(tablet_id, data_type, backup_sstable_meta_array))) {
      LOG_WARN("failed to inner get backup sstable metas", K(ret), K(tablet_id), K(data_type));
    } else if (OB_FAIL(set_backup_sstable_meta_array_(backup_sstable_meta_array))) {
      LOG_WARN("failed to set backup sstable meta array", K(ret), K(tablet_id), K(backup_sstable_meta_array));
    }

    if (OB_SUCC(ret)) {
      //get minor sstable meta
      data_type.set_minor_data_backup();
      if (OB_FAIL(inner_get_backup_sstable_metas_(tablet_id, data_type, backup_sstable_meta_array))) {
        LOG_WARN("failed to inner get backup sstable metas", K(ret), K(tablet_id), K(data_type));
      } else if (OB_FAIL(set_backup_sstable_meta_array_(backup_sstable_meta_array))) {
        LOG_WARN("failed to set backup sstable meta array", K(ret), K(tablet_id), K(backup_sstable_meta_array));
      }
    }
  }
  return ret;
}

int ObCopySSTableInfoRestoreReader::inner_get_backup_sstable_metas_(
    const common::ObTabletID &tablet_id,
    const share::ObBackupDataType data_type,
    common::ObIArray<backup::ObBackupSSTableMeta> &backup_sstable_meta_array)
{
  int ret = OB_SUCCESS;
  backup::ObBackupMetaIndex sstable_meta_index;
  const backup::ObBackupMetaType sstable_meta_type = backup::ObBackupMetaType::BACKUP_SSTABLE_META;
  share::ObBackupPath sstable_meta_backup_path;
  backup_sstable_meta_array.reset();
  ObStorageIdMod mod;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_RESTORE;
  int64_t dest_id = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable info restore reader do not init", K(ret));
  } else if (!tablet_id.is_valid() || !data_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner get backup sstable metas get invalid argument", K(ret), K(tablet_id), K(data_type));
  } else if (OB_FAIL(restore_base_info_->get_restore_data_dest_id(*GCTX.sql_proxy_, MTL_ID(), dest_id))) {
      LOG_WARN("fail to get restore data dest id", K(ret));
  } else if (OB_FALSE_IT(mod.storage_id_ = static_cast<uint64_t>(dest_id))) {
  } else if (OB_FAIL(meta_index_store_->get_backup_meta_index(data_type, tablet_id, sstable_meta_type, sstable_meta_index))) {
      LOG_WARN("failed to get backup meta index", K(ret), K(tablet_id), KPC(restore_base_info_));
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
  } else if (OB_FAIL(get_macro_block_backup_path_(sstable_meta_index, data_type, sstable_meta_backup_path))) {
    LOG_WARN("failed to get macro block backup path", K(ret), KPC(restore_base_info_));
  } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_sstable_metas(sstable_meta_backup_path.get_obstr(),
      restore_base_info_->backup_dest_.get_storage_info(), mod, sstable_meta_index, &OB_BACKUP_META_CACHE, backup_sstable_meta_array))) {
    LOG_WARN("failed to read sstable meta", K(ret), KPC(restore_base_info_));
  } else if (OB_FAIL(filter_backup_sstable_meta_on_data_type_(data_type, backup_sstable_meta_array))) {
    LOG_WARN("failed to filter backup sstable meta on data type", K(ret), K(data_type));
  } else if (data_type.is_major_backup() && backup_sstable_meta_array.count() > 1) {
    bool check_valid = true;
    for (int64_t i = 0; check_valid && i < backup_sstable_meta_array.count(); ++i) {
      const backup::ObBackupSSTableMeta &sstable_meta = backup_sstable_meta_array.at(i);
      if (!sstable_meta.sstable_meta_.table_key_.is_column_store_sstable() && !sstable_meta.is_major_compaction_mview_dep_) {
        check_valid = false;
      }
    }
    if (!check_valid) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("major tablet should only has one sstable", K(ret), K(tablet_id), K(sstable_meta_index), K(backup_sstable_meta_array));
    }
  }
  return ret;
}

// after 4.3.2 minor and major sstable is stored together
int ObCopySSTableInfoRestoreReader::filter_backup_sstable_meta_on_data_type_(
    const share::ObBackupDataType data_type,
    common::ObIArray<backup::ObBackupSSTableMeta> &backup_sstable_meta_array)
{
  int ret = OB_SUCCESS;
  if (data_type.is_sys_backup()) {
    // do nothing
  } else {
    ObArray<backup::ObBackupSSTableMeta> tmp_array;
    ARRAY_FOREACH_X(backup_sstable_meta_array, idx, cnt, OB_SUCC(ret)) {
      const backup::ObBackupSSTableMeta &meta = backup_sstable_meta_array.at(idx);
      if ((data_type.is_minor_backup() && !meta.sstable_meta_.table_key_.is_major_sstable())
          || (data_type.is_major_backup() && meta.sstable_meta_.table_key_.is_major_sstable())) {
        if (OB_FAIL(tmp_array.push_back(meta))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
    if (FAILEDx(backup_sstable_meta_array.assign(tmp_array))) {
      LOG_WARN("failed to assign tmp array", K(ret));
    }
  }
  return ret;
}

int ObCopySSTableInfoRestoreReader::get_macro_block_backup_path_(
    const backup::ObBackupMetaIndex &sstable_meta_index,
    const share::ObBackupDataType data_type,
    ObBackupPath &sstable_meta_backup_path)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(restore_base_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("restore base info should not be null", K(ret));
  } else {
    const share::ObBackupSetFileDesc::Compatible &compatible = restore_base_info_->backup_compatible_;
    if (share::ObBackupSetFileDesc::is_backup_set_support_quick_restore(compatible)) {
      if (OB_FAIL(share::ObBackupPathUtilV_4_3_2::get_macro_block_backup_path(restore_base_info_->backup_dest_,
          sstable_meta_index.ls_id_, data_type, sstable_meta_index.turn_id_, sstable_meta_index.retry_id_,
          sstable_meta_index.file_id_, sstable_meta_backup_path))) {
        LOG_WARN("failed to get macro block backup path", K(ret), KPC(restore_base_info_));
      }
    } else {
      if (OB_FAIL(share::ObBackupPathUtil::get_macro_block_backup_path(restore_base_info_->backup_dest_,
          sstable_meta_index.ls_id_, data_type, sstable_meta_index.turn_id_, sstable_meta_index.retry_id_,
          sstable_meta_index.file_id_, sstable_meta_backup_path))) {
        LOG_WARN("failed to get macro block backup path", K(ret), KPC(restore_base_info_));
      }
    }
  }
  return ret;
}

int ObCopySSTableInfoRestoreReader::set_backup_sstable_meta_array_(
    const common::ObIArray<backup::ObBackupSSTableMeta> &backup_sstable_meta_array)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable info restore reader do not init");
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < backup_sstable_meta_array.count(); ++i) {
      const backup::ObBackupSSTableMeta &sstable_meta = backup_sstable_meta_array.at(i);
      if (OB_FAIL(backup_sstable_meta_array_.push_back(sstable_meta))) {
        LOG_WARN("failed to push sstable meta into array", K(ret), K(sstable_meta));
      }
    }
  }
  return ret;
}

int ObCopySSTableInfoRestoreReader::get_next_tablet_sstable_header(
    obrpc::ObCopyTabletSSTableHeader &copy_header)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable info restore reader do not init", K(ret));
  } else if (tablet_index_ == tablet_id_array_.count()) {
    ret = OB_ITER_END;
  } else if (!is_sstable_iter_end_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable iter do not reach end, unexpected", K(ret), K(tablet_index_));
  } else if (FALSE_IT(tablet_id = tablet_id_array_.at(tablet_index_))) {
  } else if (ObTabletRestoreAction::is_restore_replace_remote_sstable(restore_action_)) {
    if (OB_FAIL(get_tablet_sstable_header_from_local_(tablet_id, copy_header))) {
      LOG_WARN("failed to get tablet sstable header from local", K(ret), K(tablet_id));
    }
  } else if (OB_FAIL(get_tablet_sstable_header_from_backup_(tablet_id, copy_header))) {
    LOG_WARN("failed to get tablet sstable header from backup", K(ret), K(tablet_id));
  }

  return ret;
}


int ObCopySSTableInfoRestoreReader::get_backup_tablet_meta_(
    const common::ObTabletID &tablet_id,
    obrpc::ObCopyTabletSSTableHeader &copy_header)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObTabletHandle local_tablet_handle;
  ObTablet *local_tablet = nullptr;
  backup::ObBackupMetaIndex tablet_meta_index;
  share::ObBackupPath backup_path;

  if (OB_ISNULL(restore_base_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid args", K(ret), KP(restore_base_info_));
  } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_id, local_tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
  } else if (OB_ISNULL(local_tablet = local_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(local_tablet), K(tablet_id));
  } else {
    const ObBackupDest &backup_dest = restore_base_info_->backup_dest_;
    const share::ObBackupStorageInfo *storage_info = backup_dest.get_storage_info();
    ObBackupDataType backup_data_type;
    if (tablet_id.is_ls_inner_tablet()) {
      backup_data_type.set_sys_data_backup();
    } else if (ObTabletRestoreAction::is_restore_major(restore_action_)) {
      backup_data_type.set_major_data_backup();
    } else {
      backup_data_type.set_minor_data_backup();
    }
    backup::ObBackupTabletMeta backup_tablet_meta;
    if (OB_FAIL(fetch_backup_tablet_meta_index_(tablet_id, backup_data_type, tablet_meta_index))
        && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to fetch backup tablet meta index", K(ret), K(tablet_id), K(backup_data_type));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      copy_header.status_ = ObCopyTabletStatus::TABLET_NOT_EXIST;
      LOG_INFO("tablet not exist", K(tablet_id), K(backup_data_type));
    } else if (OB_FAIL(get_backup_tablet_meta_backup_path_(backup_dest, backup_data_type, tablet_meta_index, backup_path))) {
      LOG_WARN("failed to get tablet backup path", K(ret), K(backup_dest), K(tablet_meta_index));
    } else if (OB_FAIL(read_backup_tablet_meta_(backup_path, storage_info, backup_data_type, tablet_meta_index, backup_tablet_meta))) {
      LOG_WARN("failed to read backup major tablet meta", K(ret), K(backup_path), K(tablet_meta_index));
    } else if (!backup_tablet_meta.is_valid()
              || !backup_tablet_meta.tablet_meta_.ha_status_.is_none()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("backup tablet meta is invalid", K(ret), K(tablet_id), K(backup_data_type), K(backup_tablet_meta));
    } else if (!ObTabletRestoreAction::is_restore_major(restore_action_)
               && local_tablet->get_tablet_meta().transfer_info_.transfer_seq_ != backup_tablet_meta.tablet_meta_.transfer_info_.transfer_seq_) {
      // If transfer seq of backup tablet is not equal to local tablet,
      // treat it as tablet not exist when restore minor. But, transfer seq
      // should not be compared when restore major.
      copy_header.status_ = ObCopyTabletStatus::TABLET_NOT_EXIST;
      LOG_INFO("tablet not exist as transfer seq is not equal",
                K(tablet_id),
                K(backup_data_type),
                "local tablet meta", local_tablet->get_tablet_meta(),
                "backup tablet meta", backup_tablet_meta.tablet_meta_);
    } else if (OB_FAIL(copy_header.tablet_meta_.assign(backup_tablet_meta.tablet_meta_))) {
      LOG_WARN("failed to assign tablet meta", K(ret), K(backup_tablet_meta));
    } else {
      copy_header.status_ = ObCopyTabletStatus::TABLET_EXIST;
      LOG_INFO("succeed get backup tablet meta", K(tablet_id), K(backup_data_type), "meta", copy_header.tablet_meta_);
    }
  }
  return ret;
}

int ObCopySSTableInfoRestoreReader::fetch_backup_tablet_meta_index_(
    const common::ObTabletID &tablet_id,
    const share::ObBackupDataType &backup_data_type,
    backup::ObBackupMetaIndex &meta_index)
{
  int ret = OB_SUCCESS;
  meta_index.reset();
  backup::ObBackupMetaType meta_type = backup::ObBackupMetaType::BACKUP_TABLET_META;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(tablet_id));
  } else if (OB_ISNULL(meta_index_store_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not be null", K(ret));
  } else if (OB_FAIL(meta_index_store_->get_backup_meta_index(
                     backup_data_type,
                     tablet_id,
                     meta_type,
                     meta_index))) {
    LOG_WARN("failed to get meta index", K(ret), K(tablet_id), K(backup_data_type));
  } else {
    LOG_INFO("get backup meta index", K(tablet_id), K(backup_data_type), K(meta_index));
  }
  return ret;
}

int ObCopySSTableInfoRestoreReader::get_backup_tablet_meta_backup_path_(
    const share::ObBackupDest &backup_dest,
    const share::ObBackupDataType &backup_data_type,
    const backup::ObBackupMetaIndex &meta_index,
    ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  if (!backup_dest.is_valid() || !meta_index.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(meta_index), K(backup_dest));
  } else {
    const share::ObBackupSetFileDesc::Compatible &compatible = restore_base_info_->backup_compatible_;
    if (share::ObBackupSetFileDesc::is_backup_set_support_quick_restore(compatible)) {
      if (OB_FAIL(ObBackupPathUtilV_4_3_2::get_macro_block_backup_path(backup_dest,
          meta_index.ls_id_, backup_data_type, meta_index.turn_id_,
          meta_index.retry_id_, meta_index.file_id_, backup_path))) {
        LOG_WARN("failed to get macro block backup path", K(ret), K(backup_data_type), K(meta_index));
      }
    } else {
      if (OB_FAIL(ObBackupPathUtil::get_macro_block_backup_path(backup_dest,
          meta_index.ls_id_, backup_data_type, meta_index.turn_id_,
          meta_index.retry_id_, meta_index.file_id_, backup_path))) {
        LOG_WARN("failed to get macro block backup path", K(ret), K(backup_data_type), K(meta_index));
      }
    }
  }
  LOG_INFO("get macro block backup path", K(backup_data_type), K(backup_path), K(meta_index));
  return ret;
}

int ObCopySSTableInfoRestoreReader::read_backup_tablet_meta_(
    const share::ObBackupPath &backup_path,
    const share::ObBackupStorageInfo *storage_info,
    const share::ObBackupDataType &backup_data_type,
    const backup::ObBackupMetaIndex &meta_index,
    backup::ObBackupTabletMeta &tablet_meta)
{
  int ret = OB_SUCCESS;
  ObStorageIdMod mod;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_RESTORE;
  int64_t dest_id = 0;
  if (OB_FAIL(restore_base_info_->get_restore_data_dest_id(*GCTX.sql_proxy_, MTL_ID(), dest_id))) {
      LOG_WARN("fail to get restore data dest id", K(ret));
  } else if (OB_FALSE_IT(mod.storage_id_ = static_cast<uint64_t>(dest_id))) {
  } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_tablet_meta(
      backup_path.get_obstr(), storage_info, mod, meta_index, tablet_meta))) {
    LOG_WARN("failed to read tablet meta", K(ret), K(backup_path), K(meta_index), K(backup_data_type));
  }
  return ret;
}

int ObCopySSTableInfoRestoreReader::compare_storage_schema_(
    const common::ObTabletID &tablet_id,
    const ObTabletHandle &tablet_handle,
    const backup::ObBackupTabletMeta &tablet_meta,
    bool &need_update)
{
  int ret = OB_SUCCESS;
  need_update = false;
  int64_t old_storage_schema_stored_col_cnt = 0;
  const int64_t new_storage_schema_stored_col_cnt = tablet_meta.tablet_meta_.storage_schema_.store_column_cnt_;
  ObArenaAllocator temp_allocator(common::ObMemAttr(MTL_ID(), "RestoreReader"));
  ObStorageSchema *schema_on_tablet = nullptr;

  if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(temp_allocator, schema_on_tablet))) {
    LOG_WARN("failed to load storage schema", K(ret), K(tablet_handle));
  } else if (FALSE_IT(old_storage_schema_stored_col_cnt = schema_on_tablet->store_column_cnt_)) {
  } else if (tablet_meta.tablet_meta_.storage_schema_.compare_schema_newer(*schema_on_tablet)) { // schema_on_tablet is newer
    need_update = true;
    LOG_INFO("storage schema stored cnt compare", K(old_storage_schema_stored_col_cnt), K(new_storage_schema_stored_col_cnt), K(tablet_id));
#ifdef ERRSIM
    const int64_t old_multi_version_start = tablet_handle.get_obj()->get_multi_version_start();
    const int64_t old_snapshot_version = tablet_handle.get_obj()->get_snapshot_version();
    const int64_t new_multi_version_sstart = tablet_meta.tablet_meta_.multi_version_start_;
    const int64_t new_snapshot_version = tablet_meta.tablet_meta_.snapshot_version_;
    SERVER_EVENT_SYNC_ADD("storage_ha", "need_update_tablet_schema",
                          "tablet_id", tablet_id.id(),
                          "old_storage_stored_col_cnt", old_storage_schema_stored_col_cnt,
                          "new_storage_stored_col_cnt", new_storage_schema_stored_col_cnt,
                          "old_snapshot_version", old_snapshot_version,
                          "new_snapshot_version", new_snapshot_version,
                          "new_mult_version_start", new_multi_version_sstart);
#endif
  }
  ObTabletObjLoadHelper::free(temp_allocator, schema_on_tablet);
  return ret;
}

ObCopyTabletsSSTableInfoObProducer::ObCopyTabletsSSTableInfoObProducer()
  : is_inited_(false),
    ls_handle_(),
    tablet_sstable_info_array_(),
    tablet_index_(0)
{
}

ObCopyTabletsSSTableInfoObProducer::~ObCopyTabletsSSTableInfoObProducer()
{
}

int ObCopyTabletsSSTableInfoObProducer::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObIArray<obrpc::ObCopyTabletSSTableInfoArg> &tablet_sstable_info_array)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("copy tablets sstable info ob producer init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || tablet_sstable_info_array.count() < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("copy sstable info ob producer init get invalid argument", K(ret), K(tenant_id), K(ls_id));
  } else if (tenant_id != MTL_ID()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant is not match", K(ret), K(tenant_id));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle_, ObLSGetMod::HA_MOD))) {
    LOG_WARN("fail to get log stream", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_UNLIKELY(nullptr == (ls_handle_.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(tablet_sstable_info_array_.assign(tablet_sstable_info_array))) {
    LOG_WARN("failed to assign tablet sstable info", K(ret), K(tablet_sstable_info_array));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObCopyTabletsSSTableInfoObProducer::get_next_tablet_sstable_info(
    obrpc::ObCopyTabletSSTableInfoArg &arg)
{
  int ret = OB_SUCCESS;
  arg.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy tablets sstable info ob producer do not init", K(ret));
  } else if (tablet_index_ == tablet_sstable_info_array_.count()) {
    ret = OB_ITER_END;
  } else {
    arg = tablet_sstable_info_array_.at(tablet_index_);
    tablet_index_++;
  }
  return ret;
}

ObCopySSTableInfoObProducer::ObCopySSTableInfoObProducer()
  : is_inited_(false),
    ls_id_(),
    tablet_sstable_info_(),
    tablet_handle_(),
    iter_(),
    status_(ObCopyTabletStatus::MAX_STATUS)
{
}
#ifdef ERRSIM
void errsim_copy_new_sstable_array(const ObTabletID &tablet_id, ObTableStoreIterator &iter)
{
  int ret = EN_ONLY_COPY_OLD_VERSION_MAJOR_SSTABLE ? : OB_SUCCESS;
  if (OB_FAIL(ret) && tablet_id.id() > ObTabletID::MIN_USER_TABLET_ID) {
    ObTableStoreIterator tmp_iter;
    ObITable *table = nullptr;
    ObITable *old_major = nullptr;
    ret = OB_SUCCESS;
    while (OB_SUCC(ret)) { // loop all sstable to skip new version major
      bool push_flag = true;
      if (OB_FAIL(iter.get_next(table))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next table", K(ret), K(iter));
        }
      } else if (table->is_major_sstable()) {
        if (NULL == old_major) {
          old_major = table;
        } else {
          push_flag = false;
          FLOG_INFO("ERRSIM EN_ONLY_COPY_OLD_VERSION_MAJOR_SSTABLE, skip copy major sstable", KR(ret), KPC(table));
        }
      }
      if (OB_FAIL(ret) || !push_flag) {
      } else if (OB_FAIL(tmp_iter.add_table(table))) {
        LOG_WARN("failed to add table", KR(ret));
      }
    } // while
    ret = (OB_ITER_END == ret ? OB_SUCCESS : ret);
    if (OB_SUCC(ret)) {
      iter.reset();
      if (OB_FAIL(iter.assign(tmp_iter))) {
        LOG_WARN("failed to assgin tablet store iter", KR(ret), K(tmp_iter));
      } else {
        FLOG_INFO("get copy sstable after ERRSIM EN_ONLY_COPY_OLD_VERSION_MAJOR_SSTABLE", KR(ret), K(iter));
      }
    }
  }
}
#endif
int ObCopySSTableInfoObProducer::init(
    const obrpc::ObCopyTabletSSTableInfoArg &tablet_sstable_info,
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  bool is_ready_for_read = false;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("copy sstable info ob producer init twice", K(ret));
  } else if (!tablet_sstable_info.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("copy sstable info ob producer init get invalid argument",
        K(ret), K(tablet_sstable_info), KP(ls));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_sstable_info.tablet_id_, tablet_handle_))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      status_ = ObCopyTabletStatus::TABLET_NOT_EXIST;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet handle", K(ret), K(tablet_sstable_info));
    }
  } else if (OB_ISNULL(tablet = tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_sstable_info));
  } else if (!tablet_sstable_info.ddl_sstable_scn_range_.is_empty()) {
    if (tablet->get_tablet_meta().get_ddl_sstable_start_scn() < tablet_sstable_info.ddl_sstable_scn_range_.start_scn_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl start scn fall back", K(ret), K(tablet->get_tablet_meta()), K(tablet_sstable_info));
    } else if (tablet->get_tablet_meta().get_ddl_sstable_start_scn() == tablet_sstable_info.ddl_sstable_scn_range_.start_scn_) {
      if (tablet->get_tablet_meta().ddl_checkpoint_scn_ < tablet_sstable_info.ddl_sstable_scn_range_.end_scn_) {
        ret = OB_DDL_SSTABLE_RANGE_CROSS;
        LOG_WARN("ddl sstable not exist", K(ret), K(tablet_sstable_info), KPC(tablet));
      }
    } else {
      LOG_INFO("ddl start scn advanced, the expired ddl sstable has been cleaned", "tablet_id", tablet_sstable_info.tablet_id_,
          K(tablet->get_tablet_meta().ddl_start_scn_), K(tablet_sstable_info.ddl_sstable_scn_range_));
    }
  }
  if (OB_SUCC(ret) && nullptr != tablet) {
    if (OB_FAIL(tablet->get_ha_tables(iter_, is_ready_for_read))) {
      LOG_WARN("failed to get read tables", K(ret));
    } else {
      status_ = ObCopyTabletStatus::TABLET_EXIST;
    }
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    (void) errsim_copy_new_sstable_array(tablet_sstable_info.tablet_id_, iter_);
  }
#endif

  if (OB_FAIL(ret)) {
  } else {
    ls_id_ = ls->get_ls_id();
    tablet_sstable_info_ = tablet_sstable_info;
    is_inited_ = true;
  }
  return ret;
}

int ObCopySSTableInfoObProducer::get_next_sstable_info(
    obrpc::ObCopyTabletSSTableInfo &sstable_info)
{
  int ret = OB_SUCCESS;
  sstable_info.reset();
  ObLS *ls = nullptr;
  ObLSTabletService *tablet_svr = nullptr;
  ObITable *table = nullptr;
  bool is_ready_for_read = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable info ob producer do not init", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      ObITable *table = nullptr;
      ObSSTable *sstable = nullptr;
      bool need_copy_sstable = false;

      if (OB_FAIL(iter_.get_next(table))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next table", K(ret), K(tablet_sstable_info_));
        }
      } else if (OB_ISNULL(table) || table->is_memtable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table is null or table type is unexpected", K(ret), KPC(table));
      } else if (FALSE_IT(sstable = static_cast<ObSSTable *> (table))) {
      } else if (OB_FAIL(check_need_copy_sstable_(sstable, need_copy_sstable))) {
        LOG_WARN("failed to check need copy sstable", K(ret), K(tablet_sstable_info_), KPC(sstable));
      } else if (!need_copy_sstable) {
       //do nothing
        LOG_INFO("no need copy sstable", KPC(sstable), K(tablet_sstable_info_));
      } else if (OB_FAIL(tablet_handle_.get_obj()->build_migration_sstable_param(table->get_key(), sstable_info.param_))) {
        LOG_WARN("failed to build migration sstable param", K(ret), K(*table));
      } else {
        sstable_info.tablet_id_ = tablet_sstable_info_.tablet_id_;
        sstable_info.table_key_ = table->get_key();
        LOG_INFO("succeed get sstable info", K(sstable_info), K(tablet_sstable_info_));
        break;
      }
    }
  }
  return ret;
}

int ObCopySSTableInfoObProducer::check_need_copy_sstable_(
    blocksstable::ObSSTable *sstable,
    bool &need_copy_sstable)
{
  int ret = OB_SUCCESS;
  need_copy_sstable = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable info ob producer do not init", K(ret));
  } else if (OB_ISNULL(sstable) || !sstable->is_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check need copy sstable get invalid argument", K(ret), KPC(sstable), K(tablet_sstable_info_));
  } else {
    if (sstable->is_major_sstable()) {
      need_copy_sstable = sstable->get_key().get_snapshot_version()
          > tablet_sstable_info_.max_major_sstable_snapshot_;
    } else if (sstable->is_minor_sstable()) {
      need_copy_sstable = true;
    } else if (sstable->is_ddl_dump_sstable()) {
      const SCN ddl_sstable_start_scn = tablet_sstable_info_.ddl_sstable_scn_range_.start_scn_;
      const SCN ddl_sstable_end_scn = tablet_sstable_info_.ddl_sstable_scn_range_.end_scn_;
      if (tablet_sstable_info_.ddl_sstable_scn_range_.is_empty()) {
        need_copy_sstable = false;
      } else if (sstable->get_key().scn_range_.start_scn_ >= ddl_sstable_end_scn) {
        need_copy_sstable = false;
      } else if (sstable->get_key().scn_range_.start_scn_ >= ddl_sstable_start_scn
          && sstable->get_key().scn_range_.end_scn_ <= ddl_sstable_end_scn) {
        need_copy_sstable = true;
      } else {
        ret = OB_DDL_SSTABLE_RANGE_CROSS;
        LOG_WARN("ddl sstable version range across", K(ret), K(tablet_sstable_info_), KPC(sstable));
      }
#ifdef ERRSIM
      SERVER_EVENT_SYNC_ADD("storage_ha", "check_need_copy_ddl_sstable",
                            "tablet_id", tablet_sstable_info_.tablet_id_.id(),
                            "sstable_key", sstable->get_key(),
                            "need_copy_sstable", need_copy_sstable,
                            "need_copy_scn_range", tablet_sstable_info_.ddl_sstable_scn_range_);

#endif
    } else if (sstable->is_mds_sstable()) {
      need_copy_sstable = true;
    } else {
      need_copy_sstable = false;
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sstable type is unexpected, cannot check need copy sstable", K(ret),
          KPC(sstable), K(tablet_sstable_info_));
    }
  }
  return ret;
}

int ObCopySSTableInfoObProducer::get_copy_sstable_count_(int64_t &sstable_count)
{
  int ret = OB_SUCCESS;
  sstable_count = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable info ob producer do not init", K(ret));
  } else if (0 == iter_.count()) {
    sstable_count = 0;
  } else {
    while (OB_SUCC(ret)) {
      ObITable *table = nullptr;
      ObSSTable *sstable = nullptr;
      bool need_copy_sstable = false;

      if (OB_FAIL(iter_.get_next(table))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next table", K(ret), K(tablet_sstable_info_));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(table) || table->is_memtable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table is null or table type is unexpected", K(ret), KPC(table));
      } else if (FALSE_IT(sstable = static_cast<ObSSTable *> (table))) {
      } else if (OB_FAIL(check_need_copy_sstable_(sstable, need_copy_sstable))) {
        LOG_WARN("failed to check need copy sstable", K(ret), K(tablet_sstable_info_), KPC(sstable));
      } else if (!need_copy_sstable) {
       //do nothing
        LOG_INFO("no need copy sstable", KPC(sstable), K(tablet_sstable_info_));
      } else {
        sstable_count++;
      }
    }
    iter_.resume();
  }
  return ret;
}

int ObCopySSTableInfoObProducer::get_copy_tablet_sstable_header(
    obrpc::ObCopyTabletSSTableHeader &copy_header)
{
  int ret = OB_SUCCESS;
  copy_header.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable info ob producer do not init", K(ret));
  } else if (OB_FAIL(ObStorageHAUtils::get_server_version(copy_header.version_))) {
    LOG_WARN("failed to get server version", K(ret), K_(ls_id));
  } else {
    copy_header.tablet_id_ = tablet_sstable_info_.tablet_id_;
    copy_header.status_ = status_;
    if (ObCopyTabletStatus::TABLET_EXIST == status_) {
      if (OB_FAIL(get_tablet_meta_(copy_header.tablet_meta_))) {
        LOG_WARN("failed to get tablet meta", K(ret), K(tablet_sstable_info_));
      } else if (OB_FAIL(get_copy_sstable_count_(copy_header.sstable_count_))) {
        LOG_WARN("failed to get copy sstable count", K(ret), K(tablet_sstable_info_));
      }
    } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == status_) {
      if (OB_FAIL(fake_deleted_tablet_meta_(copy_header.tablet_meta_))) {
        LOG_WARN("failed to fake deleted tablet meta", K(ret), K(copy_header));
      } else {
        copy_header.sstable_count_ = 0;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("copy tablet status is unexpected", K(ret), K(status_), K(tablet_sstable_info_));
    }
  }
  return ret;
}

int ObCopySSTableInfoObProducer::get_tablet_meta_(ObMigrationTabletParam &tablet_meta)
{
  int ret = OB_SUCCESS;
  tablet_meta.reset();
  ObTablet *tablet = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable info ob producer do not init", K(ret));
  } else if (OB_ISNULL(tablet = tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet));
  } else if (OB_FAIL(tablet->build_migration_tablet_param(tablet_meta))) {
    LOG_WARN("failed to build migration tablet param", K(ret), KPC(tablet));
  }
  return ret;
}

int ObCopySSTableInfoObProducer::fake_deleted_tablet_meta_(
    ObMigrationTabletParam &tablet_meta)
{
  int ret = OB_SUCCESS;
  tablet_meta.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable info ob producer do not init", K(ret));
  } else if (OB_FAIL(tablet_meta.build_deleted_tablet_info(ls_id_, tablet_sstable_info_.tablet_id_))) {
    LOG_WARN("failed to build deleted tablet info", K(ret), K(ls_id_), K(tablet_sstable_info_));
  }
  return ret;
}


ObCopySSTableMacroObReader::ObCopySSTableMacroObReader()
  : is_inited_(false),
    rpc_reader_(),
    allocator_()
{
  ObMemAttr attr(MTL_ID(), "CSSTMBObReader");
  allocator_.set_attr(attr);
}

int ObCopySSTableMacroObReader::init(
    const ObStorageHASrcInfo &src_info,
    const obrpc::ObCopySSTableMacroRangeInfoArg &rpc_arg,
    obrpc::ObStorageRpcProxy &srv_rpc_proxy,
    common::ObInOutBandwidthThrottle &bandwidth_throttle)
{
  int ret = OB_SUCCESS;
  int64_t rpc_timeout = FETCH_SSTABLE_MACRO_INFO_TIMEOUT;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (OB_UNLIKELY(!src_info.is_valid())
             || OB_UNLIKELY(!rpc_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src_info), K(rpc_arg));
  } else if (OB_FAIL(rpc_reader_.init(bandwidth_throttle))) {
    LOG_WARN("fail to init tablet info rpc reader", K(ret));
  } else if (FALSE_IT(rpc_timeout = ObStorageHAUtils::get_rpc_timeout())) {
  } else if (OB_FAIL(srv_rpc_proxy.to(src_info.src_addr_).by(rpc_arg.tenant_id_)
                .timeout(rpc_timeout).dst_cluster_id(src_info.cluster_id_)
                .ratelimit(true).bg_flow(obrpc::ObRpcProxy::BACKGROUND_FLOW)
                .group_id(share::OBCG_STORAGE_STREAM)
                .fetch_sstable_macro_info(rpc_arg, rpc_reader_.get_rpc_buffer(), rpc_reader_.get_handle()))) {
    LOG_WARN("failed to send fetch tablet info rpc", K(ret), K(src_info), K(rpc_arg));
  } else {
    is_inited_ = true;
    LOG_INFO("succeed to init copy sstable macro info reader", K(src_info), K(rpc_arg));
  }
  return ret;
}

int ObCopySSTableMacroObReader::get_next_sstable_range_info(
    ObCopySSTableMacroRangeInfo &sstable_macro_range_info)
{
  int ret = OB_SUCCESS;
  sstable_macro_range_info.reset();
  obrpc::ObCopySSTableMacroRangeInfoHeader header;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable macro ob reader do not init", K(ret));
  } else if (OB_FAIL(fetch_sstable_macro_range_header_(header))) {
    LOG_WARN("failed to fetch sstable macro range header", K(ret), K(header));
  } else if (OB_FAIL(fetch_sstable_macro_range_(header, sstable_macro_range_info.copy_macro_range_array_))) {
    LOG_WARN("failed to fetch sstable macro range", K(ret), K(header), K(header));
  } else if (header.macro_range_count_ != sstable_macro_range_info.copy_macro_range_array_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable macro range count is not equal to array count", K(ret), K(header), K(sstable_macro_range_info));
  } else {
    sstable_macro_range_info.copy_table_key_ = header.copy_table_key_;
  }
  return ret;
}

int ObCopySSTableMacroObReader::fetch_sstable_macro_range_header_(
    obrpc::ObCopySSTableMacroRangeInfoHeader &header)
{
  int ret = OB_SUCCESS;
  header.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable macro ob reader do not init", K(ret));
  } else if (OB_FAIL(rpc_reader_.fetch_and_decode(header))) {
    LOG_WARN("failed to fetch and decode sstable macro range info header", K(ret));
  } else if (!header.is_valid()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid macro range info header", K(ret), K(header));
  }
  return ret;
}

int ObCopySSTableMacroObReader::fetch_sstable_macro_range_(
    const obrpc::ObCopySSTableMacroRangeInfoHeader &header,
    common::ObIArray<ObCopyMacroRangeInfo> &macro_range_info_array)
{
  int ret = OB_SUCCESS;
  macro_range_info_array.reset();
  ObCopyMacroRangeInfo macro_range_info;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable macro ob reader do not init", K(ret));
  } else if (!header.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fetch sstable macro range get invalid argument", K(ret), K(header));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < header.macro_range_count_; ++i) {
      macro_range_info.reuse();

      if (OB_FAIL(rpc_reader_.fetch_and_decode(macro_range_info))) {
        LOG_WARN("failed to fetch and decode sstable macro range info", K(ret), K(header));
      } else if (OB_FAIL(macro_range_info_array.push_back(macro_range_info))) {
        LOG_WARN("failed to push macro range info into array", K(ret), K(macro_range_info));
      } else {
        LOG_INFO("succeed get macro range info", K(header), K(macro_range_info));
      }
    }
  }
  return ret;
}

ObCopySSTableMacroRestoreReader::ObCopySSTableMacroRestoreReader()
  : is_inited_(false),
    rpc_arg_(),
    restore_base_info_(nullptr),
    meta_index_store_(nullptr),
    second_meta_index_store_(nullptr),
    sstable_index_(0),
    restore_action_(ObTabletRestoreAction::RESTORE_NONE),
    sstable_macro_producer_()
{
}

int ObCopySSTableMacroRestoreReader::init(
    const obrpc::ObCopySSTableMacroRangeInfoArg &rpc_arg,
    const ObRestoreBaseInfo &restore_base_info,
    const ObTabletRestoreAction::ACTION &restore_action,
    backup::ObBackupMetaIndexStoreWrapper &meta_index_store,
    backup::ObBackupMetaIndexStoreWrapper &second_meta_index_store)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable macro restore reader is init twice", K(ret));
  } else if (!rpc_arg.is_valid() || !restore_base_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init copy sstable macro restore reader get invalid argument", K(ret), K(rpc_arg), K(restore_base_info));
  } else if (OB_FAIL(rpc_arg_.assign(rpc_arg))) {
    LOG_WARN("failed to assign macro range info arg", K(ret), K(rpc_arg));
  } else if (ObTabletRestoreAction::is_restore_replace_remote_sstable(restore_action)
             && OB_FAIL(sstable_macro_producer_.init(rpc_arg.tenant_id_,
                                                     rpc_arg.ls_id_,
                                                     rpc_arg.tablet_id_,
                                                     rpc_arg.copy_table_key_array_,
                                                     rpc_arg.macro_range_max_marco_count_))) {
    LOG_WARN("failed to init sstable macro info producer", K(ret), K(rpc_arg));
  } else {
    restore_base_info_ = &restore_base_info;
    meta_index_store_ = &meta_index_store;
    second_meta_index_store_ = &second_meta_index_store;
    restore_action_ = restore_action;
    sstable_index_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObCopySSTableMacroRestoreReader::get_next_sstable_range_info(
    ObCopySSTableMacroRangeInfo &sstable_macro_range_info)
{
  int ret = OB_SUCCESS;
  sstable_macro_range_info.reset();
  const bool is_shared_storage_mode = GCTX.is_shared_storage_mode();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable macro restore reader do not init", K(ret));
  } else if (sstable_index_ == rpc_arg_.copy_table_key_array_.count()) {
    ret = OB_ITER_END;
  } else {
    const ObITable::TableKey &table_key = rpc_arg_.copy_table_key_array_.at(sstable_index_);
    const bool is_shared_ddl_sstable = is_shared_storage_mode && table_key.is_ddl_dump_sstable();

    if (is_shared_ddl_sstable) {
      if (OB_FAIL(get_next_shared_ddl_sstable_range_info_(table_key, sstable_macro_range_info))) {
        LOG_WARN("failed to get next shared ddl sstable range info", K(ret), K(table_key));
      }
    } else if (ObTabletRestoreAction::is_restore_replace_remote_sstable(restore_action_)) {
      if (OB_FAIL(get_next_sstable_range_info_from_local_(sstable_macro_range_info))) {
        LOG_WARN("failed to get sstable range info from local", K(ret));
      }
    } else {
      if (OB_FAIL(get_next_sstable_range_info_from_backup_(sstable_macro_range_info))) {
        LOG_WARN("failed to get sstable range info from backup", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      sstable_index_++;
    }
  }

  return ret;
}

int ObCopySSTableMacroRestoreReader::get_next_sstable_range_info_from_local_(
    ObCopySSTableMacroRangeInfo &sstable_macro_range_info)
{
  int ret = OB_SUCCESS;
  // If restore action is RESTORE_REPLACE_REMOTE_SSTABLE, macro block list can be obtained by
  // remote sstable in table store.
  obrpc::ObCopySSTableMacroRangeInfoHeader header;
  if (OB_FAIL(sstable_macro_producer_.get_next_sstable_macro_range_info(header))) {
    if (OB_ITER_END == ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get next sstable macro return unexpected end", K(ret));
    }
    LOG_WARN("failed to get next sstable macro range info", K(ret));
  } else if (OB_FAIL(fetch_sstable_macro_range_from_local_(header, sstable_macro_range_info.copy_macro_range_array_))) {
    LOG_WARN("failed to fetch sstable macro range", K(ret), K(header));
  } else if (header.macro_range_count_ != sstable_macro_range_info.copy_macro_range_array_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable macro range count is not equal to array count", K(ret), K(header), K(sstable_macro_range_info));
  } else {
    sstable_macro_range_info.copy_table_key_ = header.copy_table_key_;
  }

  return ret;
}

int ObCopySSTableMacroRestoreReader::fetch_sstable_macro_range_from_local_(
    const obrpc::ObCopySSTableMacroRangeInfoHeader &header,
    common::ObIArray<ObCopyMacroRangeInfo> &macro_range_info_array)
{
  int ret = OB_SUCCESS;

  if (!header.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fetch sstable macro range info get invalid argument", K(ret) ,K(header));
  } else {
    SMART_VARS_2((ObCopySSTableMacroRangeObProducer, macro_range_producer), (ObCopyMacroRangeInfo, macro_range_info)) {
      if (OB_FAIL(macro_range_producer.init(
                    rpc_arg_.tenant_id_,
                    rpc_arg_.ls_id_,
                    rpc_arg_.tablet_id_,
                    header,
                    rpc_arg_.macro_range_max_marco_count_))) {
        LOG_WARN("failed to init macro range producer", K(ret), K(rpc_arg_), K(header));
      } else {
        while (OB_SUCC(ret)) {
          macro_range_info.reuse();
          if (OB_FAIL(macro_range_producer.get_next_macro_range_info(macro_range_info))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("failed to get next macro range info", K(ret), K(header), K(rpc_arg_));
            }
          } else if (OB_FAIL(macro_range_info_array.push_back(macro_range_info))) {
            LOG_WARN("failed to push back macro range info", K(ret), K(macro_range_info), K(rpc_arg_));
          }
        }
      }
    }
  }
  return ret;
}

int ObCopySSTableMacroRestoreReader::get_next_sstable_range_info_from_backup_(ObCopySSTableMacroRangeInfo &sstable_macro_range_info)
{
  int ret = OB_SUCCESS;
  const ObITable::TableKey &table_key = rpc_arg_.copy_table_key_array_.at(sstable_index_);
  if (OB_FAIL(get_next_sstable_range_info_(table_key, sstable_macro_range_info))) {
    LOG_WARN("failed to get next sstable range info", K(ret), K(table_key));
  }
  return ret;
}

int ObCopySSTableMacroRestoreReader::get_next_sstable_range_info_(
    const ObITable::TableKey &table_key,
    ObCopySSTableMacroRangeInfo &sstable_macro_range_info)
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);

  ObLSHandle ls_handle;
  ObLSService *ls_service = NULL;
  ObLS *ls = NULL;
  ObTabletHandle tablet_handle;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable macro restore reader do not init", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get next sstable range info get invalid argument", K(ret), K(table_key));
  } else if (OB_FAIL(guard.switch_to(rpc_arg_.tenant_id_))) {
    LOG_WARN("switch tenant failed", K(ret), K_(rpc_arg));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(rpc_arg_.ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("fail to get log stream", KR(ret), K_(rpc_arg));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", KR(ret), K_(rpc_arg));
  } else if (OB_FAIL(ls->ha_get_tablet(rpc_arg_.tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K_(rpc_arg));
  } else if (ObBackupSetFileDesc::is_backup_set_not_support_quick_restore(restore_base_info_->backup_compatible_)) {
    if (OB_FAIL(build_sstable_range_info_(rpc_arg_.tablet_id_,
                                          tablet_handle,
                                          table_key,
                                          sstable_macro_range_info))) {
      LOG_WARN("failed to build sstable range info", K(ret), K(rpc_arg_), K(table_key));
    }
  } else if (OB_FAIL(build_sstable_range_info_using_iterator_(rpc_arg_.tablet_id_,
                                                              tablet_handle,
                                                              table_key,
                                                              sstable_macro_range_info))) {
    LOG_WARN("failed to build sstable range info using iterator", K(ret), K(rpc_arg_), K(table_key));
  }
  return ret;
}

int ObCopySSTableMacroRestoreReader::build_sstable_range_info_using_iterator_(
    const common::ObTabletID &tablet_id,
    const storage::ObTabletHandle &tablet_handle,
    const ObITable::TableKey &table_key,
    ObCopySSTableMacroRangeInfo &sstable_macro_range_info)
{
  int ret = OB_SUCCESS;
  backup::ObBackupSSTableSecMetaIterator *sstable_sec_meta_iterator = nullptr;

  sstable_macro_range_info.copy_table_key_ = table_key;

  if (OB_FAIL(ObRestoreUtils::create_backup_sstable_sec_meta_iterator(rpc_arg_.tenant_id_,
                                                                      tablet_id,
                                                                      tablet_handle,
                                                                      table_key,
                                                                      *restore_base_info_,
                                                                      *meta_index_store_,
                                                                      sstable_sec_meta_iterator))) {
    LOG_WARN("failed to create backup sstable sec meta iterator", K(ret), K(rpc_arg_));
  } else {
    int64_t macro_block_count = 0;
    SMART_VARS_3((blocksstable::ObDataMacroBlockMeta, macro_meta),
                 (ObCopyMacroRangeInfo, macro_range_info),
                 (ObDatumRowkey, end_key)) {
      while (OB_SUCC(ret)) {
        macro_meta.reset();
        if (OB_FAIL(sstable_sec_meta_iterator->get_next(macro_meta))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get next", K(ret));
          }
        } else if (0 == macro_block_count) {
          // first macro within the range
          macro_range_info.start_macro_block_id_ = macro_meta.get_logic_id();
          macro_range_info.is_leader_restore_ = true;
          end_key.reset();
          if (OB_FAIL(macro_meta.get_rowkey(end_key))) {
            LOG_WARN("failed to get rowkey", K(ret), K(table_key), K(macro_meta));
          } else if (OB_FAIL(macro_range_info.deep_copy_start_end_key(end_key))) {
            LOG_WARN("failed to deep copy start end key", K(ret), K(end_key), K(table_key), K(macro_meta));
          } else {
            LOG_INFO("succeed get start logical id end key", K(end_key), K(macro_meta), K(table_key));
          }
        }

        if (OB_SUCC(ret)) {
          ++macro_block_count;
          macro_range_info.end_macro_block_id_ = macro_meta.get_logic_id();
          macro_range_info.macro_block_count_ = macro_block_count;
          if (macro_block_count < rpc_arg_.macro_range_max_marco_count_) {
          } else if (OB_FAIL(sstable_macro_range_info.copy_macro_range_array_.push_back(macro_range_info))) {
            LOG_WARN("failed to push macro range info into array", K(ret), K(macro_range_info));
          } else {
            macro_block_count = 0;
            macro_range_info.reuse();
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (0 == macro_block_count) {
      } else if (OB_FAIL(sstable_macro_range_info.copy_macro_range_array_.push_back(macro_range_info))) {
        LOG_WARN("failed to push macro range info into array", K(ret), K(macro_range_info));
      }
    }
  }

  if (OB_NOT_NULL(sstable_sec_meta_iterator)) {
    backup::ObLSBackupFactory::free(sstable_sec_meta_iterator);
  }

  return ret;
}

int ObCopySSTableMacroRestoreReader::build_sstable_range_info_(
    const common::ObTabletID &tablet_id,
    const storage::ObTabletHandle &tablet_handle,
    const ObITable::TableKey &table_key,
    ObCopySSTableMacroRangeInfo &sstable_macro_range_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObRestoreMacroBlockId> block_id_array;
  sstable_macro_range_info.copy_table_key_ = table_key;
  SMART_VAR(ObRestoreMacroBlockIdMgr, restore_block_id_mgr) {
    if (OB_FAIL(restore_block_id_mgr.init(tablet_id,
                                          tablet_handle,
                                          table_key,
                                          *restore_base_info_,
                                          *meta_index_store_,
                                          *second_meta_index_store_))) {
      LOG_WARN("failed to init restore block id mgr", K(ret), K(rpc_arg_), K(table_key));
    } else if (OB_FAIL(restore_block_id_mgr.get_restore_macro_block_id_array(block_id_array))) {
      LOG_WARN("failed to get restore macro block id array", K(ret), K(rpc_arg_), K(table_key));
    } else if (block_id_array.empty()) {
      //do nothing
      LOG_INFO("sstable do not has any macro block", K(table_key));
    } else {
      ObCopyMacroRangeInfo macro_range_info;
      int64_t index = 0;
      while (OB_SUCC(ret) && index < block_id_array.count()) {
        macro_range_info.reuse();
        int64_t macro_block_count = 0;
        for (; OB_SUCC(ret)
            && index < block_id_array.count()
            && macro_block_count < rpc_arg_.macro_range_max_marco_count_; ++index, ++macro_block_count) {
          const ObRestoreMacroBlockId &pair = block_id_array.at(index);
          if (0 == macro_block_count) {
            macro_range_info.start_macro_block_id_ = pair.logic_block_id_;
          }
          macro_range_info.end_macro_block_id_ = pair.logic_block_id_;
        }

        if (OB_SUCC(ret)) {
          macro_range_info.is_leader_restore_ = true;
          macro_range_info.macro_block_count_ = macro_block_count;
          if (OB_FAIL(sstable_macro_range_info.copy_macro_range_array_.push_back(macro_range_info))) {
            LOG_WARN("failed to push macro range info into array", K(ret), K(macro_range_info));
          }
        }
      }
    }
  }

  return ret;
}

int ObCopySSTableMacroRestoreReader::get_next_shared_ddl_sstable_range_info_(
    const ObITable::TableKey &table_key,
    ObCopySSTableMacroRangeInfo &sstable_macro_range_info)
{
  int ret = OB_SUCCESS;
  const bool is_shared_ddl_sstable = GCTX.is_shared_storage_mode() && table_key.is_ddl_dump_sstable();
  share::ObBackupDataType backup_data_type;
  const backup::ObBackupMetaType meta_type = backup::ObBackupMetaType::BACKUP_SSTABLE_META;
  backup::ObBackupMetaIndex meta_index;
  share::ObBackupStorageInfo storage_info;
  share::ObRestoreBackupSetBriefInfo backup_set_brief_info;
  share::ObBackupDest backup_set_dest;
  ObStorageIdMod mod;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_RESTORE;
  int64_t dest_id = 0;
  share::ObBackupPath backup_path;
  ObArray<backup::ObBackupLinkedItem> link_item;
  share::ObBackupDataType user_data_type;
  user_data_type.set_user_data_backup();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable macro restore reader do not init", K(ret));
  } else if (!is_shared_ddl_sstable) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table is not ddl or storage is not in shared mode, unexpected", K(ret), K(table_key), K(is_shared_ddl_sstable));
  } else if (OB_FAIL(ObRestoreUtils::get_backup_data_type(table_key, backup_data_type))) {
    LOG_WARN("fail to get backup data type", K(ret), K(table_key));
  } else if (OB_FAIL(meta_index_store_->get_backup_meta_index(backup_data_type, table_key.get_tablet_id(), meta_type, meta_index))) {
    LOG_WARN("failed to get backup meta index", K(ret), K(table_key), K(meta_type), K(backup_data_type));
  } else if (OB_FAIL(restore_base_info_->get_restore_backup_set_dest(meta_index.backup_set_id_, backup_set_brief_info))) {
    LOG_WARN("fail to get backup set dest", K(ret), K(meta_index));
  } else if (OB_FAIL(restore_base_info_->get_restore_data_dest_id(*GCTX.sql_proxy_, MTL_ID(), dest_id))) {
    LOG_WARN("fail to get restore data dest id", K(ret));
  } else if (OB_FALSE_IT(mod.storage_id_ = static_cast<uint64_t>(dest_id))) {
  } else if (OB_FAIL(backup_set_dest.set(backup_set_brief_info.backup_set_path_))) {
    LOG_WARN("fail to set backup set dest", K(ret));
  } else if (OB_FAIL(share::ObBackupPathUtilV_4_3_2::get_macro_block_backup_path(backup_set_dest, meta_index.ls_id_,
      user_data_type, meta_index.turn_id_, meta_index.retry_id_, meta_index.file_id_, backup_path))) {
    LOG_WARN("failed to get macro block index", K(ret), K(restore_base_info_), K(meta_index), KPC(restore_base_info_));
  } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_ddl_sstable_other_block_id_list_in_ss_mode(
      backup_set_dest, backup_path.get_obstr(), restore_base_info_->backup_dest_.get_storage_info(), mod, meta_index, table_key, link_item))) {
    LOG_WARN("failed to read ddl sstable other block id", K(ret), K(table_key), K(meta_index));
  } else if (OB_FAIL(build_shared_ddl_sstable_range_info_(table_key, link_item, sstable_macro_range_info))) {
    LOG_WARN("failed to build shared ddl sstable range info", K(ret), K(table_key), K(meta_index));
  }
  return ret;
}

int ObCopySSTableMacroRestoreReader::build_shared_ddl_sstable_range_info_(
    const ObITable::TableKey &table_key,
    const common::ObIArray<backup::ObBackupLinkedItem> &link_item,
    ObCopySSTableMacroRangeInfo &sstable_macro_range_info)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_DDL_MACRO_RANGE_MACRO_COUNT = ObDDLCopySSTableMacroRangeObProducer::MACRO_RANGE_MAX_MACRO_COUNT;
  ObArray<MacroBlockId> macro_block_id_array;
  const int64_t buf_size = ObDDLCopySSTableMacroRangeObProducer::MAX_BUF_SIZE;
  char buf[buf_size] = {0};

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable macro restore reader do not init", K(ret));
  } else if (FALSE_IT(sstable_macro_range_info.copy_table_key_ = table_key)) {
  } else if (link_item.empty()) {
    LOG_INFO("ddl sstable do not has any link item", K(table_key));
  } else {
    SMART_VAR(ObCopyMacroRangeInfo, macro_range_info) {
      int64_t index = 0;
      while (OB_SUCC(ret) && index < link_item.count()) {
        macro_range_info.reuse();
        macro_block_id_array.reset();
        int64_t macro_block_count = 0;
        for (; OB_SUCC(ret)
            && index < link_item.count()
            && macro_block_count < MAX_DDL_MACRO_RANGE_MACRO_COUNT; ++index, ++macro_block_count) {
          const backup::ObBackupLinkedItem &item = link_item.at(index);
          if (OB_FAIL(macro_block_id_array.push_back(item.macro_id_))) {
            LOG_WARN("failed to push macro id into array", K(ret), K(item));
          } else if (0 == macro_block_count) {
            macro_range_info.start_macro_block_id_.data_seq_.data_seq_ = index * MAX_DDL_MACRO_RANGE_MACRO_COUNT;
            macro_range_info.start_macro_block_id_.logic_version_ = table_key.get_snapshot_version();
            macro_range_info.start_macro_block_id_.tablet_id_ = table_key.get_tablet_id().id();
          }

          if (OB_FAIL(ret)) {
          } else {
            macro_range_info.end_macro_block_id_.data_seq_.data_seq_ = index * MAX_DDL_MACRO_RANGE_MACRO_COUNT + macro_block_count;
            macro_range_info.end_macro_block_id_.logic_version_ = table_key.get_snapshot_version();
            macro_range_info.end_macro_block_id_.tablet_id_ = table_key.get_tablet_id().id();
          }
        }

        if (OB_SUCC(ret)) {
          macro_range_info.is_leader_restore_ = true;
          macro_range_info.macro_block_count_ = macro_block_count;
          MEMSET(buf, 0, buf_size);
          if (OB_FAIL(ObStorageHAUtils::make_macro_id_to_datum(macro_block_id_array, buf, buf_size, macro_range_info.start_macro_block_end_key_))) {
            LOG_WARN("failed to make macro id to datum", K(ret), K(table_key));
          } else if (OB_FAIL(sstable_macro_range_info.copy_macro_range_array_.push_back(macro_range_info))) {
            LOG_WARN("failed to push macro range info into array", K(ret), K(macro_range_info));
          }
        }
      }
    }
  }
  return ret;
}


ObCopySSTableMacroObProducer::ObCopySSTableMacroObProducer()
  : is_inited_(false),
    copy_table_key_array_(),
    sstable_index_(0),
    is_sstable_iter_init_(false),
    ls_handle_(),
    tablet_handle_(),
    macro_range_max_marco_count_(0)
{
}

int ObCopySSTableMacroObProducer::init(
    const uint64_t tenant_id,
    const share::ObLSID & ls_id,
    const common::ObTabletID &tablet_id,
    const common::ObIArray<ObITable::TableKey> &copy_table_key_array,
    const int64_t macro_range_max_marco_count)
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("copy sstable macro ob producer init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !tablet_id.is_valid()
      || copy_table_key_array.empty() || macro_range_max_marco_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init copy sstable macro ob producer get invalid argument", K(ret), K(tenant_id),
        K(ls_id), K(tablet_id), K(copy_table_key_array), K(macro_range_max_marco_count));
  } else if (OB_FAIL(guard.switch_to(tenant_id))) {
    LOG_WARN("switch tenant failed", K(ret), K(tenant_id));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle_, ObLSGetMod::HA_MOD))) {
    LOG_WARN("fail to get log stream", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle_.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_id, tablet_handle_))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
  } else if (OB_FAIL(copy_table_key_array_.assign(copy_table_key_array))) {
    LOG_WARN("failed to assign sstable array", K(ret), K(tenant_id), K(ls_id), K(tablet_id), K(copy_table_key_array));
  } else {
    macro_range_max_marco_count_ = macro_range_max_marco_count;
    sstable_index_ = 0;
    is_sstable_iter_init_ = false;
    is_inited_ = true;
  }
  return ret;
}

int ObCopySSTableMacroObProducer::get_next_sstable_macro_range_info(
    obrpc::ObCopySSTableMacroRangeInfoHeader &macro_range_info_header)
{
  int ret = OB_SUCCESS;
  macro_range_info_header.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable macro ob producer do not init", K(ret));
  } else if (sstable_index_ == copy_table_key_array_.count()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(get_next_sstable_macro_range_info_(macro_range_info_header))) {
    LOG_WARN("failed to get next sstable macro range info", K(ret), K(copy_table_key_array_), K(sstable_index_));
  } else {
    sstable_index_++;
  }
  return ret;
}

int ObCopySSTableMacroObProducer::get_next_sstable_macro_range_info_(
    obrpc::ObCopySSTableMacroRangeInfoHeader &macro_range_info_header)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObTableHandleV2 table_handle;
  ObSSTable *sstable = nullptr;
  ObSSTableMetaHandle meta_handle;
  int64_t macro_block_count = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable macro ob producer do not init", K(ret));
  } else {
    const ObITable::TableKey &copy_table_key = copy_table_key_array_.at(sstable_index_);
    const bool is_shared_ddl_sstable = GCTX.is_shared_storage_mode() && copy_table_key.is_ddl_dump_sstable();
    const int64_t max_range_max_macro_count = is_shared_ddl_sstable
        ? ObDDLCopySSTableMacroRangeObProducer::MACRO_RANGE_MAX_MACRO_COUNT : macro_range_max_marco_count_;
    if (copy_table_key.is_memtable()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table type is unexpected", K(ret), K(copy_table_key));
    } else if (OB_ISNULL(tablet = tablet_handle_.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet should not be NULL", K(ret), K(copy_table_key));
    } else if (OB_FAIL(tablet->get_table(copy_table_key, table_handle))) {
      LOG_WARN("failed to get table handle", K(ret), K(copy_table_key));
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SSTABLE_NOT_EXIST;
      }
    } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
      LOG_WARN("failed to get sstable", K(ret), K(copy_table_key));
    } else if (OB_ISNULL(sstable))  {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sstable should not be NULL", K(ret), K(copy_table_key), KP(sstable));
    } else if (!is_shared_ddl_sstable) {
      macro_block_count = sstable->get_data_macro_block_count();
    } else if (OB_FAIL(sstable->get_meta(meta_handle))) {
      LOG_WARN("failed to get meta", K(ret), K(copy_table_key), KPC(sstable));
    } else {
      macro_block_count = meta_handle.get_sstable_meta().get_macro_info().get_other_block_count();
    }

    if (OB_FAIL(ret)) {
    } else {
      macro_range_info_header.copy_table_key_ = copy_table_key;
      if (0 == macro_block_count) {
        macro_range_info_header.macro_range_count_ = 0;
      } else {
        macro_range_info_header.macro_range_count_ =
            (macro_block_count + max_range_max_macro_count - 1) / max_range_max_macro_count;
      }
    }
  }
  return ret;
}

ObCopySSTableMacroRangeObProducer::ObCopySSTableMacroRangeObProducer()
  : is_inited_(false),
    table_key_(),
    macro_range_count_(0),
    macro_range_index_(0),
    macro_range_max_marco_count_(0),
    tablet_handle_(),
    table_handle_(),
    datum_range_(),
    allocator_(),
    second_meta_iterator_()
{
  ObMemAttr attr(MTL_ID(), "CopySSTMacro");
  allocator_.set_attr(attr);
}

int ObCopySSTableMacroRangeObProducer::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const obrpc::ObCopySSTableMacroRangeInfoHeader &header,
    const int64_t macro_range_max_marco_count)
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  ObTablet *tablet = nullptr;
  ObSSTable *sstable = nullptr;
  const bool is_reverse_scan = false;
  const storage::ObITableReadInfo *index_read_info = nullptr;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("copy sstable macro range ob producer init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id
      || !ls_id.is_valid() || !tablet_id.is_valid() || !header.is_valid() || macro_range_max_marco_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init copy sstable macro range get invalid argument",
        K(ret), K(tenant_id), K(ls_id), K(tablet_id), K(header), K(macro_range_max_marco_count));
  } else if (OB_FAIL(guard.switch_to(tenant_id))) {
    LOG_WARN("switch tenant failed", K(ret), K(tenant_id));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("fail to get log stream", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_id, tablet_handle_))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
  } else if (OB_ISNULL(tablet = tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(tenant_id), K(ls_id), K(tablet_id), K(header));
  } else if (OB_FAIL(tablet->get_table(header.copy_table_key_, table_handle_))) {
    LOG_WARN("failed to get table", K(ret), K(tablet_id), K(header));
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SSTABLE_NOT_EXIST;
    }
  } else if (OB_FAIL(table_handle_.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", K(ret), K(header), K(tablet_id), K(ls_id));
  } else if (FALSE_IT(datum_range_.set_whole_range())) {
  } else if (OB_FAIL(tablet->get_sstable_read_info(sstable, index_read_info))) {
    LOG_WARN("failed to get index read info ", KR(ret), K(sstable));
  } else if (OB_FAIL(second_meta_iterator_.open(datum_range_, blocksstable::DATA_BLOCK_META,
      *sstable, *index_read_info, allocator_, is_reverse_scan))) {
    LOG_WARN("failed to open second meta iterator", K(ret), K(header), K(tablet_id));
  } else {
    table_key_ = header.copy_table_key_;
    macro_range_count_ = header.macro_range_count_;
    macro_range_index_ = 0;
    macro_range_max_marco_count_ = macro_range_max_marco_count;
    is_inited_ = true;
  }
  return ret;
}

int ObCopySSTableMacroRangeObProducer::get_next_macro_range_info(
    ObCopyMacroRangeInfo &macro_range_info)
{
  int ret = OB_SUCCESS;
  macro_range_info.reuse();
  const ObSSTable *sstable = nullptr;
  int64_t macro_block_count = 0;
  ObDataMacroBlockMeta macro_meta;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable macro range ob producer do not init", K(ret));
  } else if (OB_FAIL(table_handle_.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", K(ret), K(table_key_));
  } else if (macro_range_index_ == macro_range_count_) {
    if (OB_ITER_END != second_meta_iterator_.get_next(macro_meta)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("second meta iter has not reach end but macro range index reach macro range count",
          K(ret), K(table_key_), K(macro_range_index_), K(macro_range_count_));
    } else {
      ret = OB_ITER_END;
    }
  } else {
    ObLogicMacroBlockId end_macro_block_id;
    while (OB_SUCC(ret) && macro_block_count < macro_range_max_marco_count_) {
      if (OB_FAIL(second_meta_iterator_.get_next(macro_meta))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next second meta", K(ret), K(macro_range_index_), K(macro_range_count_), K(table_key_));
        }
      } else if (0 == macro_block_count) {
        macro_range_info.start_macro_block_id_ = macro_meta.get_logic_id();
        ObDatumRowkey end_key;
        if (OB_FAIL(macro_meta.get_rowkey(end_key))) {
          LOG_WARN("failed to get rowkey", K(ret), K(table_key_), K(macro_range_index_), K(macro_range_count_));
        } else if (OB_FAIL(macro_range_info.deep_copy_start_end_key(end_key))) {
          LOG_WARN("failed to deep copy start end key", K(ret), K(end_key), K(table_key_), K(macro_range_index_), K(macro_range_count_));
        } else {
          LOG_INFO("succeed get start logical id end key",
              K(end_key), K(macro_meta), K(table_key_), K(macro_range_index_), K(macro_range_count_));
        }
      }

      if (OB_FAIL(ret)) {
      } else {
        end_macro_block_id = macro_meta.get_logic_id();
        macro_block_count++;
      }
    }

    if (OB_SUCC(ret)) {
      macro_range_info.end_macro_block_id_ = end_macro_block_id;
      macro_range_info.macro_block_count_ = macro_block_count;
      macro_range_index_++;
    }
  }
  return ret;
}

ObDDLCopySSTableMacroRangeObProducer::ObDDLCopySSTableMacroRangeObProducer()
  : is_inited_(false),
    table_key_(),
    macro_range_count_(0),
    macro_range_index_(0),
    macro_range_max_marco_count_(0),
    allocator_("CopyDDLMacro"),
    tablet_handle_(),
    table_handle_(),
    meta_handle_(),
    iterator_()
{
  MEMSET(buf_, 0, MAX_BUF_SIZE);
}

int ObDDLCopySSTableMacroRangeObProducer::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const obrpc::ObCopySSTableMacroRangeInfoHeader &header,
    const int64_t macro_range_max_marco_count)
{
  int ret = OB_SUCCESS;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  ObTablet *tablet = nullptr;
  ObSSTable *sstable = nullptr;
  const bool is_reverse_scan = false;
  const storage::ObITableReadInfo *index_read_info = nullptr;
  const bool is_shared_mode = GCTX.is_shared_storage_mode();

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ddl copy sstable macro range ob producer init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id
      || !ls_id.is_valid() || !tablet_id.is_valid() || !header.is_valid() || macro_range_max_marco_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ddl copy sstable macro range get invalid argument",
        K(ret), K(tenant_id), K(ls_id), K(tablet_id), K(header), K(macro_range_max_marco_count));
  } else if (!is_shared_mode || !header.copy_table_key_.is_ddl_dump_sstable()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("init ddl copy sstable macro range ob producer get unexpected sstable type", K(ret),
        K(is_shared_mode), K(header), K(tablet_id));
  } else if (OB_FAIL(guard.switch_to(tenant_id))) {
    LOG_WARN("switch tenant failed", K(ret), K(tenant_id));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("fail to get log stream", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_id, tablet_handle_))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
  } else if (OB_ISNULL(tablet = tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(tenant_id), K(ls_id), K(tablet_id), K(header));
  } else if (OB_FAIL(tablet->get_table(header.copy_table_key_, table_handle_))) {
    LOG_WARN("failed to get table", K(ret), K(tablet_id), K(header));
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SSTABLE_NOT_EXIST;
    }
  } else if (OB_FAIL(table_handle_.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", K(ret), K(header), K(tablet_id), K(ls_id));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable should not be NULL", K(ret), K(header), K(tablet_id), KP(sstable));
  } else if (OB_FAIL(sstable->get_meta(meta_handle_))) {
    LOG_WARN("failed to get sstable meta", K(ret), K(header), K(tablet_id));
  } else if (OB_FAIL(meta_handle_.get_sstable_meta().get_macro_info().get_other_block_iter(iterator_))) {
    LOG_WARN("failed to get other block iter", K(ret), K(header), K(tablet_id));
  } else {
    table_key_ = header.copy_table_key_;
    macro_range_count_ = header.macro_range_count_;
    macro_range_index_ = 0;
    macro_range_max_marco_count_ = macro_range_max_marco_count;
    is_inited_ = true;
  }
  return ret;
}

int ObDDLCopySSTableMacroRangeObProducer::get_next_macro_range_info(
    ObCopyMacroRangeInfo &macro_range_info)
{
  int ret = OB_SUCCESS;
  macro_range_info.reuse();
  const ObSSTable *sstable = nullptr;
  int64_t macro_block_count = 0;
  MacroBlockId macro_id;
  ObArray<MacroBlockId> macro_block_id_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl copy sstable macro range ob producer do not init", K(ret));
  } else if (OB_FAIL(table_handle_.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", K(ret), K(table_key_));
  } else if (macro_range_index_ == macro_range_count_) {
    int32_t tmp_ret = iterator_.get_next_macro_id(macro_id);
    if (OB_ITER_END != tmp_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("other block iter has not reach end but macro range index reach macro range count",
          K(ret), K(tmp_ret), K(table_key_), K(macro_range_index_), K(macro_range_count_));
    } else {
      ret = tmp_ret;
    }
  } else {
    MEMSET(buf_, 0, MAX_BUF_SIZE);
    ObLogicMacroBlockId end_macro_block_id;
    while (OB_SUCC(ret) && macro_block_count < macro_range_max_marco_count_) {
      if (OB_FAIL(iterator_.get_next_macro_id(macro_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next second meta", K(ret), K(macro_range_index_), K(macro_range_count_), K(table_key_));
        }
      } else if (0 == macro_block_count) {
        //logical id data seq record the index of the macro block in other block id array.
        macro_range_info.start_macro_block_id_.data_seq_.data_seq_ = macro_range_index_ * macro_range_max_marco_count_;
        macro_range_info.start_macro_block_id_.logic_version_ = table_key_.get_snapshot_version();
        macro_range_info.start_macro_block_id_.tablet_id_ = table_key_.get_tablet_id().id();
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(macro_block_id_array.push_back(macro_id))) {
        LOG_WARN("failed to push macro block id into array", K(ret), K(macro_id));
      } else {
        //logical id data seq record the index of the macro block in other block id array.
        end_macro_block_id.data_seq_.data_seq_ = macro_range_index_ * macro_range_max_marco_count_ + macro_block_count;
        end_macro_block_id.logic_version_ = table_key_.get_snapshot_version();
        end_macro_block_id.tablet_id_ = table_key_.get_tablet_id().id();
        macro_block_count++;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObStorageHAUtils::make_macro_id_to_datum(macro_block_id_array, buf_, MAX_BUF_SIZE,  macro_range_info.start_macro_block_end_key_))) {
        LOG_WARN("failed to make macro id to datum", K(ret), K(macro_block_id_array), K(table_key_));
      } else {
        macro_range_info.end_macro_block_id_ = end_macro_block_id;
        macro_range_info.macro_block_count_ = macro_block_count;
        macro_range_index_++;
      }
    }
  }
  return ret;
}

// ObCopyLSViewInfoObReader
ObCopyLSViewInfoObReader::ObCopyLSViewInfoObReader()
  : is_inited_(false),
    rpc_reader_(),
    allocator_()
{
  ObMemAttr attr(MTL_ID(), "CPLSVObReader");
  allocator_.set_attr(attr);
}

int ObCopyLSViewInfoObReader::init(
    const ObStorageHASrcInfo &src_info,
    const obrpc::ObCopyLSViewArg &rpc_arg,
    obrpc::ObStorageRpcProxy &srv_rpc_proxy,
    common::ObInOutBandwidthThrottle &bandwidth_throttle)
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = NULL;
  share::ObTenantBase *tenant_base = MTL_CTX();
  omt::ObTenant *tenant = NULL;
  ObLSService *ls_service = NULL;
  ObLS *ls = NULL;
  ObLSHandle ls_handle;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (OB_UNLIKELY(!src_info.is_valid())
             || OB_UNLIKELY(!rpc_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src_info), K(rpc_arg));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_ISNULL(tenant_base)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant base should not be NULL", K(ret), KP(tenant_base));
  } else if (FALSE_IT(tenant = static_cast<omt::ObTenant *>(tenant_base))) {
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(rpc_arg.ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(rpc_arg));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), K(rpc_arg));
  } else if (OB_FAIL(rpc_reader_.init(bandwidth_throttle))) {
    LOG_WARN("fail to init tablet info rpc reader", K(ret));
  } else {
    const int64_t WAIT_GC_LOCK_TIMEOUT = 3 * 60 * 1000 * 1000; // 3 min
    const int64_t CHECK_GC_LOCK_INTERVAL = 1000000; // 1s
    const int64_t wait_gc_lock_start_ts = ObTimeUtility::current_time();
    int64_t cost_ts = 0;
    int64_t rpc_timeout = FETCH_LS_VIEW_INFO_TIMEOUT;

    do {
      if (ls->is_stopped()) {
        ret = OB_NOT_RUNNING;
        LOG_WARN("ls is not running, stop send get ls view rpc", K(ret), KPC(ls));
      } else if (scheduler->has_set_stop()) {
        ret = OB_SERVER_IS_STOPPING;
        LOG_WARN("tenant dag scheduler has set stop, stop send get ls view rpc", K(ret), KPC(ls));
      } else if (tenant->has_stopped()) {
        ret = OB_TENANT_HAS_BEEN_DROPPED;
        LOG_WARN("tenant has been stopped, stop send get ls view rpc", K(ret), KPC(ls));
      } else if (FALSE_IT(rpc_timeout = ObStorageHAUtils::get_rpc_timeout())) {
      } else if (OB_FAIL(srv_rpc_proxy.to(src_info.src_addr_).by(rpc_arg.tenant_id_)
                .timeout(rpc_timeout).dst_cluster_id(src_info.cluster_id_)
                .ratelimit(true).bg_flow(obrpc::ObRpcProxy::BACKGROUND_FLOW)
                .group_id(share::OBCG_STORAGE_STREAM)
                .fetch_ls_view(rpc_arg, rpc_reader_.get_rpc_buffer(), rpc_reader_.get_handle()))) {
        if (OB_TABLET_GC_LOCK_CONFLICT != ret) {
          LOG_WARN("failed to send fetch ls view info rpc", K(ret), K(src_info), K(rpc_arg));
        } else {
          cost_ts = ObTimeUtility::current_time() - wait_gc_lock_start_ts;
          if (WAIT_GC_LOCK_TIMEOUT <= cost_ts) {
            ret = OB_EAGAIN;
            LOG_WARN("copy ls view wait gc lock timeout, need try again.", K(ret), K(src_info), K(rpc_arg));
          } else {
            ob_usleep(CHECK_GC_LOCK_INTERVAL);
          }
        }
      } else {
        cost_ts = ObTimeUtility::current_time() - wait_gc_lock_start_ts;
        LOG_INFO("succeed to init copy ls view", K(src_info), K(rpc_arg), K(cost_ts));
      }
    } while (OB_TABLET_GC_LOCK_CONFLICT == ret);


    if (FAILEDx(rpc_reader_.fetch_and_decode(ls_meta_))) {
      LOG_WARN("fail to fetch and decode ls meta", K(ret));
    } else if (!ls_meta_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ls meta", K(ret), K_(ls_meta));
    } else {
      is_inited_ = true;
      LOG_INFO("succeed to init fetch ls view info reader", K(src_info), K(rpc_arg));
    }
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = EN_READER_RPC_NOT_SUPPORT ? : OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(ERROR, "fake EN_READER_RPC_NOT_SUPPORT", K(ret));
    }
  }
#endif
  return ret;
}

int ObCopyLSViewInfoObReader::get_ls_meta(
    ObLSMetaPackage &ls_meta)
{
  int ret = OB_SUCCESS;
  ls_meta.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCopyLSViewInfoObReader not init", K(ret));
  } else {
    ls_meta = ls_meta_;
  }

  return ret;
}

int ObCopyLSViewInfoObReader::get_next_tablet_info(
    obrpc::ObCopyTabletInfo &tablet_info)
{
  int ret = OB_SUCCESS;
  tablet_info.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCopyLSViewInfoObReader not init", K(ret));
  } else if (OB_FAIL(rpc_reader_.fetch_and_decode(tablet_info))) {
    if (OB_ITER_END == ret) {
      //do nothing
    } else {
      LOG_WARN("fail to fetch and decode tablet meta", K(ret));
    }
  } else if (!tablet_info.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet meta", K(ret), K(tablet_info));
  }

  return ret;
}


ObCopyLSViewInfoRestoreReader::ObCopyLSViewInfoRestoreReader()
  : is_inited_(false),
    ls_id_(),
    restore_base_info_(nullptr),
    reader_()
{
}

int ObCopyLSViewInfoRestoreReader::init(
    const share::ObLSID &ls_id,
    const ObRestoreBaseInfo &restore_base_info,
    backup::ObBackupMetaIndexStoreWrapper *meta_index_store)
{
  int ret = OB_SUCCESS;
  common::ObStorageIdMod mod;
  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_RESTORE;
  int64_t dest_id = 0;
  bool is_final_fuse = false;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
             || OB_UNLIKELY(!restore_base_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(restore_base_info));
  } else if (FALSE_IT(is_final_fuse = share::ObBackupSetFileDesc::is_backup_set_support_quick_restore(restore_base_info.backup_compatible_))) {
  } else if (OB_FAIL(restore_base_info.get_restore_data_dest_id(*GCTX.sql_proxy_, MTL_ID(), dest_id))) {
      LOG_WARN("fail to get restore data dest id", K(ret));
  } else if (OB_FALSE_IT(mod.storage_id_ = static_cast<uint64_t>(dest_id))) {
  } else if (restore_base_info.backup_data_version_ >= DATA_VERSION_4_2_0_0
          && OB_FAIL(reader_.init(restore_base_info.backup_dest_, mod, ls_id, is_final_fuse))) {
    LOG_WARN("fail to init reader", K(ret), K(restore_base_info), K(ls_id), K(is_final_fuse));
  } else if (restore_base_info.backup_data_version_ < DATA_VERSION_4_2_0_0
          && OB_FAIL(init_for_4_1_x_(ls_id, restore_base_info, *meta_index_store))) {
    LOG_WARN("fail to init reader", K(ret), K(restore_base_info), K(ls_id));
  } else {
    ls_id_ = ls_id;
    restore_base_info_ = &restore_base_info;
    is_inited_ = true;
    LOG_INFO("succeed to init copy ls view info restore reader", K(ls_id), K(restore_base_info));
  }
  return ret;
}

int ObCopyLSViewInfoRestoreReader::get_ls_meta(
    ObLSMetaPackage &ls_meta)
{
  int ret = OB_SUCCESS;
  ObBackupDataStore store;
  ls_meta.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCopyLSViewInfoRestoreReader not init", K(ret));
  } else if (OB_FAIL(store.init(restore_base_info_->backup_dest_))) {
    LOG_WARN("fail to init backup data store", K(ret), KPC_(restore_base_info));
  } else if (OB_FAIL(store.read_ls_meta_infos(ls_id_, ls_meta))) {
    LOG_WARN("fail to read ls meta info", K(ret), K_(ls_id));
  } else {
    LOG_INFO("read ls meta info", K_(ls_id), K(ls_meta));
  }

  return ret;
}

int ObCopyLSViewInfoRestoreReader::get_next_tablet_info(
    obrpc::ObCopyTabletInfo &tablet_info)
{
  int ret = OB_SUCCESS;
  tablet_info.reset();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCopyLSViewInfoRestoreReader not init", K(ret));
  } else if (restore_base_info_->backup_data_version_ < DATA_VERSION_4_2_0_0) {
    if (OB_FAIL(reader_41x_.fetch_tablet_info(tablet_info))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next tablet meta", K(ret));
      }
    }
  } else if (OB_FAIL(reader_.get_next(tablet_info.param_))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get next tablet meta", K(ret));
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_RESTORE_FETCH_TABLET_INFO) OB_SUCCESS;
    LOG_WARN("errsim restore fetch tablet info", K(ret));
  }
#endif

  if (OB_SUCC(ret)) {
    tablet_info.data_size_ = 0;
    tablet_info.status_ = ObCopyTabletStatus::TABLET_EXIST;
    tablet_info.tablet_id_ = tablet_info.param_.tablet_id_;
    tablet_info.version_ = 0;
  }

  return ret;
}

int ObCopyLSViewInfoRestoreReader::init_for_4_1_x_(
    const share::ObLSID &ls_id,
    const ObRestoreBaseInfo &restore_base_info,
    backup::ObBackupMetaIndexStoreWrapper &meta_index_store)
{
  int ret = OB_SUCCESS;
  ObBackupDataStore store;
  ObArray<common::ObTabletID> tablet_id_array;
  ObArray<common::ObTabletID> deleted_tablet_id_array;
  ObArray<common::ObTabletID> tablet_need_restore_array;
  const int64_t base_turn_id = 1; // for restore, read all tablet from turn 1
  if (OB_FAIL(store.init(restore_base_info.backup_dest_))) {
    LOG_WARN("failed to init backup data store", K(ret));
  } else if (OB_FAIL(store.read_tablet_to_ls_info_v_4_1_x(base_turn_id, ls_id, tablet_id_array))) {
    LOG_WARN("failed to read tablet to ls info from 4_1_x backup set", K(ret), K(ls_id));
  } else if (OB_FAIL(store.read_deleted_tablet_info_v_4_1_x(ls_id, deleted_tablet_id_array))) {
    LOG_WARN("failed to read deleted tablet info from 4_1_x backup set", K(ret), K(ls_id));
  } else if (OB_FAIL(get_difference(tablet_id_array, deleted_tablet_id_array, tablet_need_restore_array))) {
    LOG_WARN("failed to get difference", K(ret), K(tablet_id_array), K(deleted_tablet_id_array));
  } else if (OB_FAIL(reader_41x_.init(restore_base_info, tablet_need_restore_array, meta_index_store))) {
    LOG_WARN("failed to init copy tablet info restore reader", K(ret), K(restore_base_info));
  }
  return ret;
}

// ObCopyRemoteSSTableInfoObProducer
ObCopyRemoteSSTableInfoObProducer::ObCopyRemoteSSTableInfoObProducer()
  : is_inited_(false),
    ls_id_(),
    tablet_id_(),
    tablet_handle_(),
    iter_()
{
}

int ObCopyRemoteSSTableInfoObProducer::init(
    const common::ObTabletID tablet_id,
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  bool is_ready_for_read = false;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("copy remote sstable info ob producer init twice", K(ret));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("copy remote sstable info ob producer init get invalid argument",
        K(ret), K(tablet_id), KP(ls));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_id, tablet_handle_))) {
    LOG_WARN("failed to get tablet handle", K(ret), K(tablet_id), KPC(ls));
  } else if (OB_ISNULL(tablet = tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_handle_));
  } else if (OB_FAIL(tablet->get_ha_tables(iter_, is_ready_for_read))) {
    LOG_WARN("failed to get read tables", K(ret), KPC(tablet));
  } else {
    ls_id_ = ls->get_ls_id();
    tablet_id_ = tablet_id;
    is_inited_ = true;
  }

  return ret;
}

int ObCopyRemoteSSTableInfoObProducer::get_next_sstable_info(
    obrpc::ObCopyTabletSSTableInfo &sstable_info)
{
  int ret = OB_SUCCESS;
  sstable_info.reset();
  ObLSTabletService *tablet_svr = nullptr;
  ObITable *table = nullptr;
  bool is_ready_for_read = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy remote sstable info ob producer do not init", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      ObITable *table = nullptr;
      ObSSTable *sstable = nullptr;
      bool need_copy_sstable = false;

      if (OB_FAIL(iter_.get_next(table))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next table", K(ret));
        }
      } else if (OB_ISNULL(table) || table->is_memtable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table is null or table type is unexpected", K(ret), KPC(table));
      } else if (FALSE_IT(sstable = static_cast<ObSSTable *> (table))) {
      } else if (OB_FAIL(check_need_copy_sstable_(sstable, need_copy_sstable))) {
        LOG_WARN("failed to check need copy sstable", K(ret), KPC(sstable));
      } else if (!need_copy_sstable) {
       //do nothing
        LOG_INFO("no need copy sstable", KPC(sstable));
      } else if (OB_FAIL(tablet_handle_.get_obj()->build_migration_sstable_param(table->get_key(), sstable_info.param_))) {
        LOG_WARN("failed to build migration sstable param", K(ret), K(*table));
      } else {
        sstable_info.tablet_id_ = tablet_id_;
        sstable_info.table_key_ = table->get_key();
        LOG_INFO("succeed get sstable info", K(sstable_info));
        break;
      }
    }
  }
  return ret;
}

void ObCopyRemoteSSTableInfoObProducer::reset()
{
  is_inited_ = false;
  ls_id_.reset();
  tablet_id_.reset();
  tablet_handle_.reset();
  iter_.reset();
}

int ObCopyRemoteSSTableInfoObProducer::check_need_copy_sstable_(
    blocksstable::ObSSTable *sstable,
    bool &need_copy_sstable)
{
  int ret = OB_SUCCESS;
  need_copy_sstable = false;
  ObSSTableMetaHandle sst_meta_hdl;
  if (OB_ISNULL(sstable) || !sstable->is_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check need copy sstable get invalid argument", K(ret), KP(sstable));
  } else if (sstable->is_column_store_sstable()) {
    // need copy, otherwise co sstable maybe incomplete
    need_copy_sstable = true;
  } else if (OB_FAIL(sstable->get_meta(sst_meta_hdl))) {
    LOG_WARN("failed to get sstable meta handle", K(ret), KPC(sstable));
  } else if (sst_meta_hdl.get_sstable_meta().get_table_backup_flag().has_backup()) {
    need_copy_sstable = true;
  }

  return ret;
}

int ObCopyRemoteSSTableInfoObProducer::get_copy_sstable_count_(int64_t &sstable_count)
{
  int ret = OB_SUCCESS;
  sstable_count = 0;

  if (0 == iter_.count()) {
    sstable_count = 0;
  } else {
    while (OB_SUCC(ret)) {
      ObITable *table = nullptr;
      ObSSTable *sstable = nullptr;
      bool need_copy_sstable = false;

      if (OB_FAIL(iter_.get_next(table))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next table", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(table) || table->is_memtable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table is null or table type is unexpected", K(ret), KPC(table));
      } else if (FALSE_IT(sstable = static_cast<ObSSTable *> (table))) {
      } else if (OB_FAIL(check_need_copy_sstable_(sstable, need_copy_sstable))) {
        LOG_WARN("failed to check need copy sstable", K(ret), KPC(sstable));
      } else if (!need_copy_sstable) {
       //do nothing
        LOG_INFO("no need copy sstable", KPC(sstable));
      } else {
        sstable_count++;
      }
    }
    iter_.resume();
  }
  return ret;
}

int ObCopyRemoteSSTableInfoObProducer::get_copy_tablet_sstable_header(
    obrpc::ObCopyTabletSSTableHeader &copy_header)
{
  int ret = OB_SUCCESS;
  copy_header.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy remote sstable info ob producer do not init", K(ret));
  } else if (OB_FAIL(ObStorageHAUtils::get_server_version(copy_header.version_))) {
    LOG_WARN("failed to get server version", K(ret), K_(ls_id));
  } else {
    copy_header.tablet_id_ = tablet_id_;
    copy_header.status_ = ObCopyTabletStatus::TABLET_EXIST;
    if (OB_FAIL(get_tablet_meta_(copy_header.tablet_meta_))) {
      LOG_WARN("failed to get tablet meta", K(ret));
    } else if (OB_FAIL(get_copy_sstable_count_(copy_header.sstable_count_))) {
      LOG_WARN("failed to get copy sstable count", K(ret));
    }
  }
  return ret;
}

int ObCopyRemoteSSTableInfoObProducer::get_tablet_meta_(ObMigrationTabletParam &tablet_meta)
{
  int ret = OB_SUCCESS;
  tablet_meta.reset();
  ObTablet *tablet = nullptr;

  if (OB_ISNULL(tablet = tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet));
  } else if (OB_FAIL(tablet->build_migration_tablet_param(tablet_meta))) {
    LOG_WARN("failed to build migration tablet param", K(ret), KPC(tablet));
  }
  return ret;
}

// ObCopyRemoteSSTableMacroBlockRestoreReader
ObCopyRemoteSSTableMacroBlockRestoreReader::ObCopyRemoteSSTableMacroBlockRestoreReader()
  : is_inited_(false),
    table_key_(),
    data_version_(0),
    copy_macro_range_info_(nullptr),
    restore_base_info_(nullptr),
    second_meta_index_store_(nullptr),
    backup_macro_data_buffer_(),
    backup_macro_read_buffer_(),
    local_macro_data_buffer_(nullptr),
    allocator_(),
    tablet_handle_(),
    sstable_handle_(),
    sstable_(nullptr),
    second_meta_iterator_(),
    datum_range_(),
    macro_block_count_(0),
    data_size_(0),
    meta_row_buf_("CopyMacroMetaRow"),
    macro_block_reuse_mgr_(nullptr)
{
  ObMemAttr attr(MTL_ID(), "CMBReReader");
  allocator_.set_attr(attr);
}

ObCopyRemoteSSTableMacroBlockRestoreReader::~ObCopyRemoteSSTableMacroBlockRestoreReader()
{
  second_meta_iterator_.reset();
}

int ObCopyRemoteSSTableMacroBlockRestoreReader::init(
    const ObCopyMacroBlockReaderInitParam &param)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = nullptr;
  ObLS *ls = nullptr;
  ObTablet* tablet = nullptr;
  const bool is_reverse_scan = false;
  ObSSTableMetaHandle meta_handle;
  common::ObSafeArenaAllocator allocator(allocator_);

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot init twice", K(ret));
  } else if (!param.is_valid() || !param.is_leader_restore_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else if (OB_FAIL(alloc_buffers_())) {
    LOG_WARN("failed to alloc buffers", K(ret));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be null", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(param.ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("fail to get log stream", KR(ret), K(param));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream should not be NULL", KR(ret), K(param));
  } else if (OB_FAIL(ls->ha_get_tablet(param.table_key_.get_tablet_id(), tablet_handle_))) {
    LOG_WARN("failed to get tablet handle", K(ret), K(param));
  } else if (OB_UNLIKELY(nullptr == (tablet = tablet_handle_.get_obj()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet not be NULL", KR(ret), K(param));
  } else if (OB_FAIL(tablet->get_table(param.table_key_, sstable_handle_))) {
    LOG_WARN("failed to get table", K(ret), K(param));
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SSTABLE_NOT_EXIST;
    }
  } else if (OB_FAIL(sstable_handle_.get_sstable(sstable_))) {
    LOG_WARN("failed to get sstable", K(ret), K(param));
  } else if (OB_FAIL(sstable_->get_meta(meta_handle, &allocator))) {
    LOG_WARN("failed to get sstable meta", K(ret), K(param));
  } else if (param.backfill_tx_scn_ != meta_handle.get_sstable_meta().get_basic_meta().filled_tx_scn_) {
    ret = OB_SSTABLE_NOT_EXIST;
    LOG_WARN("sstable has been changed", K(ret), K(param), KPC(sstable_));
  } else if (FALSE_IT(copy_macro_range_info_ = param.copy_macro_range_info_)) {
  } else {
    datum_range_.set_start_key(copy_macro_range_info_->start_macro_block_end_key_);
    datum_range_.end_key_.set_max_rowkey();
    datum_range_.set_left_closed();
    datum_range_.set_right_open();

    const storage::ObITableReadInfo *index_read_info = NULL;
    if (OB_FAIL(tablet->get_sstable_read_info(sstable_, index_read_info))) {
      LOG_WARN("failed to get index read info ", KR(ret), K(sstable_));
    }

    if (FAILEDx(second_meta_iterator_.open(datum_range_, blocksstable::DATA_BLOCK_META,
         *sstable_, *index_read_info, allocator_, is_reverse_scan))) {
      LOG_WARN("failed to open second meta iterator", K(ret), K(param));
    } else {
      table_key_ = param.table_key_;
      copy_macro_range_info_ = param.copy_macro_range_info_;
      restore_base_info_ = param.restore_base_info_;
      second_meta_index_store_ = param.second_meta_index_store_;
      data_version_ = param.data_version_;
      macro_block_reuse_mgr_ = param.macro_block_reuse_mgr_;
      is_inited_ = true;
      LOG_INFO("succeed to init macro block producer", K(param));
    }
  }

  return ret;
}

int ObCopyRemoteSSTableMacroBlockRestoreReader::get_next_macro_block(ObICopyMacroBlockReader::CopyMacroBlockReadData &read_data)
{
  int ret = OB_SUCCESS;
  blocksstable::ObDataMacroBlockMeta macro_meta;
  MacroBlockId macro_block_id;
  ObLogicMacroBlockId logic_block_id;
  int occupy_size = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (macro_block_count_ >= copy_macro_range_info_->macro_block_count_) {
    ret = OB_ITER_END;
    LOG_INFO("get next macro block end", K(table_key_), KPC(copy_macro_range_info_));
  } else if (OB_FAIL(second_meta_iterator_.get_next(macro_meta))) {
    LOG_WARN("failed to get next macro meta", K(ret), K(macro_block_count_), KPC(copy_macro_range_info_));
  } else if (FALSE_IT(macro_block_id = macro_meta.get_macro_id())) {
  } else if (FALSE_IT(read_data.set_macro_block_id(macro_block_id))) {
  } else if (!macro_meta.get_macro_id().is_backup_id()) {
    // This is a local macro block, reuse it.
    if (!macro_meta.get_macro_id().is_local_id()) {
      // for now, SS should not use this reader
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro block is not local", K(ret), K(macro_meta));
    } else if (table_key_.is_column_store_sstable() && sstable_->is_small_sstable()) {
      // TODO:(wangxiaohui.wxh): fix me
      // avoid reusing macro block in small sstable, as rebuild index will be failed.
      if (OB_FAIL(read_local_macro_block_data_(macro_meta, read_data))) {
        LOG_WARN("failed to read local macro block data", K(ret), K(macro_meta));
      }
    } else if (OB_FAIL(read_data.set_macro_meta(macro_meta, true /* is_reuse_macro_block */))){
      LOG_WARN("failed to set macro meta", K(ret), K(macro_meta));
    }
  } else if (macro_meta.get_logic_id().logic_version_ <= data_version_) {
    int64_t data_checksum = 0;
    MacroBlockId macro_id;

    if (OB_ISNULL(macro_block_reuse_mgr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro block reuse mgr is NULL", K(ret), K(macro_meta), KP(macro_block_reuse_mgr_));
    } else if (OB_FAIL(macro_block_reuse_mgr_->get_macro_block_reuse_info(table_key_, macro_meta.get_logic_id(), macro_id, data_checksum))) {
      LOG_WARN("failed to get macro block", K(ret), K(macro_meta));
    } else if (data_checksum != macro_meta.get_meta_val().data_checksum_) {
      ret = OB_INVALID_DATA;
      LOG_WARN("data checksum get from reuse mgr not match", K(ret), K(macro_meta), K(data_checksum));
    } else if (FALSE_IT(macro_meta.val_.macro_id_ = macro_id)) {
    } else if (OB_FAIL(read_data.set_macro_meta(macro_meta, true /* is_reuse_macro_block */))) {
      LOG_WARN("failed to set macro meta", K(ret), K(macro_meta));
    }
  } else {
    // This is a backup macro block.
    if (OB_FAIL(read_backup_macro_block_data_(macro_meta, read_data))) {
      LOG_WARN("failed to read backup macro block data", K(ret), K(macro_meta));
    }
  }

  if (OB_SUCC(ret)) {
    macro_block_count_++;
    logic_block_id = macro_meta.get_logic_id();
    if (macro_block_count_ == copy_macro_range_info_->macro_block_count_) {
        if (logic_block_id != copy_macro_range_info_->end_macro_block_id_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get macro block end macro block id is not equal to macro block range",
            K(ret), K_(macro_block_count), K(logic_block_id),
            "end_macro_block_id", copy_macro_range_info_->end_macro_block_id_, K(table_key_));
      }
    }
  }

  return ret;
}

int ObCopyRemoteSSTableMacroBlockRestoreReader::alloc_buffers_()
{
  int ret = OB_SUCCESS;
  char *backup_macro_data_buf = NULL;
  char *backup_macro_read_buf = NULL;
  char *local_macro_read_buf = NULL;

  const int64_t READ_BACKUP_MACRO_BUFFER_SIZE = OB_DEFAULT_MACRO_BLOCK_SIZE * 2;

  // used in init() func, should not check is_inited_
  if (OB_ISNULL(backup_macro_data_buf = reinterpret_cast<char*>(allocator_.alloc(OB_DEFAULT_MACRO_BLOCK_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc backup_macro_data_buf", K(ret));
  } else if (OB_ISNULL(backup_macro_read_buf = reinterpret_cast<char*>(allocator_.alloc(READ_BACKUP_MACRO_BUFFER_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc backup_macro_read_buf", K(ret));
  } else if (OB_ISNULL(local_macro_read_buf = reinterpret_cast<char*>(allocator_.alloc(OB_STORAGE_OBJECT_MGR.get_macro_block_size())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    int64_t io_size = OB_STORAGE_OBJECT_MGR.get_macro_block_size();
    LOG_WARN("failed to alloc local_macro_read_buf", K(ret), K(io_size));
  } else {
    local_macro_data_buffer_ = local_macro_read_buf;
    backup_macro_data_buffer_.assign(backup_macro_data_buf, OB_DEFAULT_MACRO_BLOCK_SIZE);
    backup_macro_read_buffer_.assign(backup_macro_read_buf, READ_BACKUP_MACRO_BUFFER_SIZE);
  }

  if (OB_FAIL(ret)) {
    allocator_.reset();
  }

  return ret;
}

int ObCopyRemoteSSTableMacroBlockRestoreReader::read_local_macro_block_data_(
    blocksstable::ObDataMacroBlockMeta &macro_meta,
    ObICopyMacroBlockReader::CopyMacroBlockReadData &read_data)
{
  int ret = OB_SUCCESS;
  blocksstable::ObStorageObjectReadInfo read_info;
  blocksstable::ObStorageObjectHandle read_handle;
  blocksstable::ObMacroBlockCommonHeader common_header;
  int64_t pos = 0;
  int64_t occupy_size = 0;
  read_data.reset();

  read_info.macro_block_id_ = macro_meta.get_macro_id();
  read_info.offset_ = sstable_->get_macro_offset();
  read_info.size_ = sstable_->get_macro_read_size();
  read_info.io_desc_.set_mode(ObIOMode::READ);
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_MIGRATE_READ);
  read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
  read_info.buf_ = local_macro_data_buffer_;
  read_info.io_desc_.set_sys_module_id(ObIOModule::HA_COPY_MACRO_BLOCK_IO);
  read_info.mtl_tenant_id_ = MTL_ID();
  if (OB_FAIL(ObObjectManager::async_read_object(read_info, read_handle))) {
    LOG_WARN("failed to async read block", K(ret), K(read_info));
  } else if (OB_FAIL(read_handle.wait())) {
    LOG_WARN("failed to wait io finish", K(ret), K(read_info));
  } else if (OB_FAIL(common_header.deserialize(read_handle.get_buffer(), read_handle.get_data_size(), pos))) {
    LOG_ERROR("deserialize common header failed", K(ret), K(read_handle), K(pos), K(common_header));
  } else if (OB_FAIL(common_header.check_integrity())) {
    ret = OB_INVALID_DATA;
    LOG_ERROR("invalid common header", K(ret), K(read_handle), K(pos), K(common_header));
  } else {
    blocksstable::ObBufferReader data;
    occupy_size = common_header.get_header_size() + common_header.get_payload_size();
    data.assign(read_handle.get_buffer(), occupy_size, occupy_size);

    if (OB_FAIL(read_data.set_macro_data(data, false /* is_reuse_macro_block */))) {
      LOG_WARN("failed to set read_info macro data", K(ret), K(read_handle), K(common_header), K(occupy_size));
    } else {
      LOG_INFO("local small sstable macro data", K(read_handle), K(common_header), K(occupy_size));
    }
  }
  return ret;
}

int ObCopyRemoteSSTableMacroBlockRestoreReader::read_backup_macro_block_data_(
    blocksstable::ObDataMacroBlockMeta &macro_meta,
    ObICopyMacroBlockReader::CopyMacroBlockReadData &read_data)
{
  int ret = OB_SUCCESS;
  ObRestoreMacroBlockId restore_macro_id;
  backup::ObBackupDeviceMacroBlockId backup_macro_id;
  backup::ObBackupMacroBlockIndex macro_index;
  blocksstable::ObBufferReader data;

  if (OB_FAIL(backup_macro_id.set(macro_meta.get_macro_id()))) {
    LOG_WARN("failed to set restore macro id", K(ret), K(macro_meta));
  } else if (OB_FAIL(restore_macro_id.set(macro_meta.get_logic_id(), backup_macro_id))) {
    LOG_WARN("failed to set restore macro id", K(ret), K(macro_meta));
  } else if (OB_FAIL(get_backup_macro_block_index_(restore_macro_id, macro_index))) {
    LOG_WARN("failed to get macro block index", K(ret), K(restore_macro_id));
  } else if (FALSE_IT(backup_macro_data_buffer_.set_pos(0))) {
  } else if (OB_FAIL(do_read_backup_macro_block_data_(macro_index, backup_macro_data_buffer_))) {
    LOG_WARN("failed to read backup macro block data", K(ret), K(restore_macro_id), K(macro_index));
  } else if (FALSE_IT(data.assign(backup_macro_data_buffer_.data(), backup_macro_data_buffer_.length(), backup_macro_data_buffer_.length()))) {
  } else if (OB_FAIL(read_data.set_macro_data(data, false /* is_reuse_macro_block */))) {
    LOG_WARN("failed to set read_info macro data", K(ret), K(data), K(backup_macro_data_buffer_));
  } else {
    data_size_ += backup_macro_data_buffer_.length();
  }

  return ret;
}

int ObCopyRemoteSSTableMacroBlockRestoreReader::get_backup_macro_block_index_(
    const ObRestoreMacroBlockId &macro_id,
    backup::ObBackupMacroBlockIndex &macro_index)
{
  int ret = OB_SUCCESS;
  share::ObBackupDataType data_type;
  if (OB_FAIL(ObRestoreUtils::get_backup_data_type(table_key_, data_type))) {
    LOG_WARN("fail to get backup data type", K(ret), K(table_key_));
  } else if (OB_FAIL(macro_id.macro_id_.get_backup_macro_block_index(macro_id.logic_block_id_, macro_index))) {
    LOG_WARN("failed to get backup macro block index", K(ret), K(data_type), K(macro_id));
  }
  return ret;
}

int ObCopyRemoteSSTableMacroBlockRestoreReader::do_read_backup_macro_block_data_(
    const backup::ObBackupMacroBlockIndex &macro_index,
    blocksstable::ObBufferReader &data_buffer)
{
  int ret = OB_SUCCESS;

  const int64_t align_size = DIO_READ_ALIGN_SIZE;
  share::ObBackupDataType data_type;
  share::ObBackupStorageInfo storage_info;
  share::ObRestoreBackupSetBriefInfo backup_set_brief_info;
  share::ObBackupDest backup_set_dest;
  share::ObBackupPath backup_path;
  ObStorageIdMod mod;
  int64_t dest_id = 0;

  mod.storage_used_mod_ = ObStorageUsedMod::STORAGE_USED_RESTORE;
  backup_macro_read_buffer_.set_pos(0);

  if (OB_FAIL(ObRestoreUtils::get_backup_data_type(table_key_, data_type))) {
    LOG_WARN("fail to get backup data type", K(ret), K(table_key_));
  } else if (OB_FAIL(restore_base_info_->get_restore_backup_set_dest(
                     macro_index.backup_set_id_,
                     backup_set_brief_info))) {
    LOG_WARN("fail to get backup set dest", K(ret), K(macro_index));
  } else if (OB_FAIL(restore_base_info_->get_restore_data_dest_id(*GCTX.sql_proxy_, MTL_ID(), dest_id))) {
    LOG_WARN("fail to get restore data dest id", K(ret));
  } else if (OB_FALSE_IT(mod.storage_id_ = static_cast<uint64_t>(dest_id))) {
  } else if (OB_FAIL(backup_set_dest.set(backup_set_brief_info.backup_set_path_))) {
    LOG_WARN("fail to set backup set dest", K(ret));
  } else if (OB_FAIL(share::ObBackupPathUtilV_4_3_2::get_macro_block_backup_path(
                     backup_set_dest,
                     macro_index.ls_id_,
                     data_type,
                     macro_index.turn_id_,
                     macro_index.retry_id_,
                     macro_index.file_id_,
                     backup_path))) {
    LOG_WARN("failed to get macro block index", K(ret), K(restore_base_info_), K(macro_index), KPC(restore_base_info_));
  } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_macro_block_data(
                     backup_path.get_obstr(),
                     restore_base_info_->backup_dest_.get_storage_info(),
                     mod,
                     macro_index,
                     align_size,
                     backup_macro_read_buffer_,
                     data_buffer))) {
    LOG_WARN("failed to read macro block data", K(ret), K(table_key_), K(macro_index), KPC(restore_base_info_));
  }

  return ret;
}

ObRebuildTabletSSTableInfoObReader::ObRebuildTabletSSTableInfoObReader()
  : is_inited_(false),
    rpc_reader_(),
    allocator_("RTObReader"),
    is_sstable_iter_end_(true),
    sstable_index_(0),
    sstable_count_(0)
{
}

int ObRebuildTabletSSTableInfoObReader::init(
    const ObStorageHASrcInfo &src_info,
    const obrpc::ObRebuildTabletSSTableInfoArg &rpc_arg,
    obrpc::ObStorageRpcProxy &srv_rpc_proxy,
    common::ObInOutBandwidthThrottle &bandwidth_throttle)
{
  int ret = OB_SUCCESS;
  int64_t rpc_timeout = FETCH_TABLET_SSTABLE_INFO_TIMEOUT;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (OB_UNLIKELY(!src_info.is_valid())
             || OB_UNLIKELY(!rpc_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src_info), K(rpc_arg));
  } else if (OB_FAIL(rpc_reader_.init(bandwidth_throttle))) {
    LOG_WARN("fail to init tablet info rpc reader", K(ret));
  } else if (FALSE_IT(rpc_timeout = ObStorageHAUtils::get_rpc_timeout())) {
  } else if (OB_FAIL(srv_rpc_proxy.to(src_info.src_addr_)
                .timeout(rpc_timeout).dst_cluster_id(src_info.cluster_id_)
                .ratelimit(true).bg_flow(obrpc::ObRpcProxy::BACKGROUND_FLOW)
                .by(rpc_arg.tenant_id_)
                .group_id(share::OBCG_STORAGE_STREAM)
                .fetch_rebuild_tablet_sstable_info(rpc_arg, rpc_reader_.get_rpc_buffer(), rpc_reader_.get_handle()))) {
    LOG_WARN("failed to send fetch tablet info rpc", K(ret), K(src_info), K(rpc_arg));
  } else {
    is_inited_ = true;
    LOG_INFO("succeed to init rebuild tablet sstable info reader", K(src_info), K(rpc_arg));
  }
  return ret;
}

int ObRebuildTabletSSTableInfoObReader::fetch_sstable_meta_(obrpc::ObCopyTabletSSTableInfo &sstable_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(rpc_reader_.fetch_and_decode(sstable_info))) {
    LOG_WARN("failed to fetch and decode sstable meta", K(ret));
  } else if (!sstable_info.is_valid()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("invalid sstable_meta", K(ret), K(sstable_info));
  }
  return ret;
}

int ObRebuildTabletSSTableInfoObReader::get_next_sstable_info(
    obrpc::ObCopyTabletSSTableInfo &sstable_info)
{
  int ret = OB_SUCCESS;
  sstable_info.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable info ob reader do not init", K(ret));
  } else if (sstable_index_ >= sstable_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable index is unexpected", K(ret), K(sstable_index_), K(sstable_count_));
  } else if (OB_FAIL(fetch_sstable_meta_(sstable_info))) {
    LOG_WARN("failed ot fetch sstable meta", K(ret), K(sstable_index_), K(sstable_count_));
  } else {
    sstable_index_++;
    if (sstable_index_ == sstable_count_) {
      is_sstable_iter_end_ = true;
    }
  }
  return ret;
}

int ObRebuildTabletSSTableInfoObReader::get_next_tablet_sstable_header(
    obrpc::ObCopyTabletSSTableHeader &copy_header)
{
  int ret = OB_SUCCESS;
  copy_header.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable info ob reader do not init", K(ret));
  } else if (!is_sstable_iter_end_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable iter is not end, can not get next tablet sstable header",
        K(ret), K(is_sstable_iter_end_));
  } else if (OB_FAIL(rpc_reader_.fetch_and_decode(copy_header))) {
    if (OB_ITER_END == ret) {
      //do nothing
    } else {
      LOG_WARN("failed to fetch and decode copy header", K(ret));
    }
  } else if (!copy_header.is_valid()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("copy header is invalid", K(ret), K(copy_header));
  } else if (OB_FAIL(ObStorageHAUtils::check_server_version(copy_header.version_))) {
    LOG_WARN("failed to check server version", K(ret));
  } else {
    is_sstable_iter_end_ = copy_header.sstable_count_ > 0 ? false : true;
    sstable_index_ = 0;
    sstable_count_ = copy_header.sstable_count_;
  }
  return ret;
}

ObRebuildTabletSSTableProducer::ObRebuildTabletSSTableProducer()
  : is_inited_(false),
    ls_id_(),
    tablet_sstable_info_(),
    tablet_handle_(),
    table_store_wrapper_(),
    status_(ObCopyTabletStatus::MAX_STATUS),
    sstable_count_(0),
    sstable_index_(0),
    major_table_key_()
{
}

int ObRebuildTabletSSTableProducer::init(
    const obrpc::ObRebuildTabletSSTableInfoArg &tablet_sstable_info,
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  bool is_ready_for_read = false;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("rebuild tablet sstable producer init twice", K(ret));
  } else if (!tablet_sstable_info.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rebuild tablet sstable info ob producer init get invalid argument",
        K(ret), K(tablet_sstable_info), KP(ls));
  } else if (OB_FAIL(ls->ha_get_tablet(tablet_sstable_info.tablet_id_, tablet_handle_))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      status_ = ObCopyTabletStatus::TABLET_NOT_EXIST;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get tablet handle", K(ret), K(tablet_sstable_info));
    }
  } else if (OB_ISNULL(tablet = tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_sstable_info));
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper_))) {
    LOG_WARN("fail to fetch table store", K(ret), K(tablet_sstable_info));
  } else if (OB_FAIL(get_copy_sstable_info_(tablet_sstable_info))) {
    LOG_WARN("failed to get copy sstable count", K(ret), K(tablet_sstable_info));
  } else {
    status_ = ObCopyTabletStatus::TABLET_EXIST;
    ls_id_ = ls->get_ls_id();
    tablet_sstable_info_ = tablet_sstable_info;
    sstable_index_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObRebuildTabletSSTableProducer::get_next_sstable_info(
    obrpc::ObCopyTabletSSTableInfo &sstable_info)
{
  int ret = OB_SUCCESS;
  sstable_info.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild tablet sstable producer do not init", K(ret));
  } else if (sstable_index_ == sstable_count_) {
    ret = OB_ITER_END;
  } else if (sstable_index_ > sstable_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable index is unexpected", K(ret), K(sstable_index_), K(sstable_count_));
  } else if (OB_FAIL(tablet_handle_.get_obj()->build_migration_sstable_param(major_table_key_, sstable_info.param_))) {
    LOG_WARN("failed to build migration sstable param", K(ret), K(major_table_key_));
  } else {
    sstable_info.tablet_id_ = tablet_sstable_info_.tablet_id_;
    sstable_info.table_key_ = major_table_key_;
    sstable_index_++;
    LOG_INFO("succeed get sstable info", K(sstable_info), K(tablet_sstable_info_));
  }
  return ret;
}

int ObRebuildTabletSSTableProducer::get_copy_sstable_info_(const obrpc::ObRebuildTabletSSTableInfoArg &tablet_sstable_info)
{
  int ret = OB_SUCCESS;
  const ObSSTableArray &sstable_array = table_store_wrapper_.get_member()->get_major_sstables();
  bool found = false;

  for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
    const ObSSTable *sstable = sstable_array.at(i);
    if (OB_ISNULL(sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rebuild tablet do not has any major sstable", K(ret), KP(sstable));
    } else if (sstable->get_snapshot_version() == tablet_sstable_info.dest_major_sstable_snapshot_) {
      major_table_key_ = sstable->get_key();
      sstable_count_ = 1;
      break;
    }
  }
  return ret;
}

int ObRebuildTabletSSTableProducer::get_copy_tablet_sstable_header(
    obrpc::ObCopyTabletSSTableHeader &copy_header)
{
  int ret = OB_SUCCESS;
  copy_header.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild tablet sstable producer do not init", K(ret));
  } else if (OB_FAIL(ObStorageHAUtils::get_server_version(copy_header.version_))) {
    LOG_WARN("failed to get server version", K(ret), K_(ls_id));
  } else {
    copy_header.tablet_id_ = tablet_sstable_info_.tablet_id_;
    copy_header.status_ = status_;
    if (ObCopyTabletStatus::TABLET_EXIST == status_) {
      if (OB_FAIL(get_tablet_meta_(copy_header.tablet_meta_))) {
        LOG_WARN("failed to get tablet meta", K(ret), K(tablet_sstable_info_));
      } else {
        copy_header.sstable_count_ = sstable_count_;
      }
    } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == status_) {
      if (OB_FAIL(fake_deleted_tablet_meta_(copy_header.tablet_meta_))) {
        LOG_WARN("failed to fake deleted tablet meta", K(ret), K(copy_header));
      } else {
        copy_header.sstable_count_ = 0;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("copy tablet status is unexpected", K(ret), K(status_), K(tablet_sstable_info_));
    }
  }
  return ret;
}

int ObRebuildTabletSSTableProducer::get_tablet_meta_(ObMigrationTabletParam &tablet_meta)
{
  int ret = OB_SUCCESS;
  tablet_meta.reset();
  ObTablet *tablet = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild tablet sstable producer do not init", K(ret));
  } else if (OB_ISNULL(tablet = tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet));
  } else if (OB_FAIL(tablet->build_migration_tablet_param(tablet_meta))) {
    LOG_WARN("failed to build migration tablet param", K(ret), KPC(tablet));
  }
  return ret;
}

int ObRebuildTabletSSTableProducer::fake_deleted_tablet_meta_(
    ObMigrationTabletParam &tablet_meta)
{
  int ret = OB_SUCCESS;
  tablet_meta.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("rebuild tablet sstable info do not init", K(ret));
  } else if (OB_FAIL(tablet_meta.build_deleted_tablet_info(ls_id_, tablet_sstable_info_.tablet_id_))) {
    LOG_WARN("failed to build deleted tablet info", K(ret), K(ls_id_), K(tablet_sstable_info_));
  }
  return ret;
}

}
}
