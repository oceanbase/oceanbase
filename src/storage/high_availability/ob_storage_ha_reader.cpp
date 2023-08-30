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

#include "lib/ob_errno.h"
#define USING_LOG_PREFIX STORAGE
#include "ob_storage_ha_reader.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/rc/ob_tenant_base.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "observer/omt/ob_tenant.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace obrpc;
using namespace blocksstable;

namespace storage
{

template <ObRpcPacketCode RPC_CODE>
int do_fetch_next_buffer_if_need(
    common::ObInOutBandwidthThrottle &bandwidth_throttle,
    common::ObDataBuffer &rpc_buffer,
    int64_t &rpc_buffer_parse_pos,
    obrpc::ObStorageRpcProxy::SSHandle<RPC_CODE> &handle,
    int64_t &last_send_time,
    int64_t &total_data_size)
{
  int ret = OB_SUCCESS;
  const int64_t max_idle_time = OB_DEFAULT_STREAM_WAIT_TIMEOUT - OB_DEFAULT_STREAM_RESERVE_TIME;

  if (rpc_buffer_parse_pos < 0 || last_send_time < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(rpc_buffer_parse_pos), K(last_send_time));
  } else if (rpc_buffer.get_position() - rpc_buffer_parse_pos > 0) {
    // do nothing
    LOG_DEBUG("has left data, no need to get more", K(rpc_buffer), K(rpc_buffer_parse_pos));
  } else {
    int tmp_ret = bandwidth_throttle.limit_in_and_sleep(rpc_buffer.get_position(), last_send_time, max_idle_time);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("failed to sleep_for_bandlimit", K(tmp_ret));
    }

    rpc_buffer.get_position() = 0;
    rpc_buffer_parse_pos = 0;
    if (handle.has_more()) {
      if (OB_FAIL(handle.get_more(rpc_buffer))) {
        STORAGE_LOG(WARN, "get_more(send request) failed", K(ret));
      } else if (rpc_buffer.get_position() < 0) {
        ret = OB_ERR_SYS;
        LOG_ERROR("rpc buffer has no data", K(ret), K(rpc_buffer));
      } else if (0 == rpc_buffer.get_position()) {
        if (!handle.has_more()) {
          ret = OB_ITER_END;
          LOG_DEBUG("empty rpc buffer, no more data", K(rpc_buffer), K(rpc_buffer_parse_pos));
        } else {
          ret = OB_ERR_SYS;
          LOG_ERROR("rpc buffer has no data", K(ret), K(rpc_buffer));
        }
      } else {
        LOG_DEBUG("get more data", K(rpc_buffer), K(rpc_buffer_parse_pos));
        total_data_size += rpc_buffer.get_position();
      }
      last_send_time = ObTimeUtility::current_time();
    } else {
      ret = OB_ITER_END;
      LOG_DEBUG("no more data", K(rpc_buffer), K(rpc_buffer_parse_pos));
    }
  }
  return ret;
}

/******************ObCopyMacroBlockReaderInitParam*********************/
ObCopyMacroBlockReaderInitParam::ObCopyMacroBlockReaderInitParam()
  : tenant_id_(OB_INVALID_ID),
    ls_id_(),
    table_key_(),
    copy_macro_range_info_(),
    src_info_(),
    is_leader_restore_(false),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    restore_base_info_(nullptr),
    meta_index_store_(nullptr),
    second_meta_index_store_(nullptr),
    restore_macro_block_id_mgr_(nullptr),
    need_check_seq_(false),
    ls_rebuild_seq_(-1),
    backfill_tx_scn_()
{
}

ObCopyMacroBlockReaderInitParam::~ObCopyMacroBlockReaderInitParam()
{
}

bool ObCopyMacroBlockReaderInitParam::is_valid() const
{
  bool bool_ret = false;
  bool_ret = tenant_id_ != OB_INVALID_ID && ls_id_.is_valid() && table_key_.is_valid() && OB_NOT_NULL(copy_macro_range_info_)
      && ((need_check_seq_ && ls_rebuild_seq_ >= 0) || !need_check_seq_);
  if (bool_ret) {
    if (!is_leader_restore_) {
      bool_ret = src_info_.is_valid() && OB_NOT_NULL(bandwidth_throttle_) && OB_NOT_NULL(svr_rpc_proxy_)
          && backfill_tx_scn_.is_valid();
    } else if (OB_ISNULL(restore_base_info_)
        || OB_ISNULL(meta_index_store_)
        || OB_ISNULL(second_meta_index_store_)
        || OB_ISNULL(restore_macro_block_id_mgr_)) {
      bool_ret = false;
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
    allocator_("CMBObReader"),
    macro_block_mem_context_(),
    last_send_time_(0),
    data_size_(0)
{
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
        arg.data_version_ = 0;
        arg.need_check_seq_ = param.need_check_seq_;
        arg.ls_rebuild_seq_ = param.ls_rebuild_seq_;
        LOG_INFO("init arg", K(param), K(arg));
      }

      if (OB_SUCC(ret)) {
        if (arg.get_serialize_size() > OB_MALLOC_BIG_BLOCK_SIZE) {
          ret = OB_ERR_SYS;
          LOG_ERROR("rpc arg must not larger than packet size", K(ret), K(arg.get_serialize_size()));
        } else if (OB_FAIL(param.svr_rpc_proxy_->to(param.src_info_.src_addr_).by(OB_DATA_TENANT_ID).dst_cluster_id(param.src_info_.cluster_id_)
                           .ratelimit(true).bg_flow(obrpc::ObRpcProxy::BACKGROUND_FLOW)
                           .timeout(rpc_timeout)
                           .fetch_macro_block(arg, rpc_buffer_, handle_))) {
          LOG_WARN("failed to send fetch macro block rpc", K(param), K(ret));
        } else {
          bandwidth_throttle_ = param.bandwidth_throttle_;
          rpc_buffer_parse_pos_ = 0;
          last_send_time_ = ObTimeUtility::current_time();
          data_size_ = rpc_buffer_.get_position();
          is_inited_ = true;
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
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf from mem ctx", K(ret));
  }

  if (OB_ISNULL(buf)) {
    ret = OB_SUCCESS;
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

int ObCopyMacroBlockObReader::get_next_macro_block(
    obrpc::ObCopyMacroBlockHeader &header,
    blocksstable::ObBufferReader &data)
{
  int ret = OB_SUCCESS;
  header.reset();

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
  }
  return ret;
}


/******************ObCopyMacroBlockRestoreReader*********************/
ObCopyMacroBlockRestoreReader::ObCopyMacroBlockRestoreReader()
  : is_inited_(false),
    table_key_(),
    copy_macro_range_info_(nullptr),
    restore_base_info_(nullptr),
    second_meta_index_store_(nullptr),
    restore_macro_block_id_mgr_(nullptr),
    data_buffer_(),
    allocator_(),
    macro_block_index_(0),
    macro_block_count_(0),
    data_size_(0)
{
  ObMemAttr attr(MTL_ID(), "CMBReReader");
  allocator_.set_attr(attr);
}

ObCopyMacroBlockRestoreReader::~ObCopyMacroBlockRestoreReader()
{
  allocator_.reset();
}

int ObCopyMacroBlockRestoreReader::init(
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
    second_meta_index_store_ = param.second_meta_index_store_;
    restore_macro_block_id_mgr_ = param.restore_macro_block_id_mgr_;
    if (OB_FAIL(restore_macro_block_id_mgr_->get_block_id_index(copy_macro_range_info_->start_macro_block_id_, macro_block_index_))) {
      LOG_WARN("failed to get block id index", K(ret), KPC(copy_macro_range_info_));
    } else {
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

int ObCopyMacroBlockRestoreReader::get_next_macro_block(
    obrpc::ObCopyMacroBlockHeader &header,
    blocksstable::ObBufferReader &data)
{
  int ret = OB_SUCCESS;
  int64_t occupy_size = 0;
  header.reset();

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
  } else {
    ObLogicMacroBlockId logic_block_id;
    backup::ObBackupPhysicalID physic_block_id;
    share::ObBackupDataType data_type;
    share::ObBackupPath backup_path;
    const int64_t align_size = DIO_READ_ALIGN_SIZE;
    backup::ObBackupMacroBlockIndex macro_index;
    share::ObBackupStorageInfo storage_info;
    share::ObRestoreBackupSetBriefInfo backup_set_brief_info;
    share::ObBackupDest backup_set_dest;
    data_buffer_.set_pos(0);
    read_buffer_.set_pos(0);
    if (OB_FAIL(restore_macro_block_id_mgr_->get_macro_block_id(macro_block_index_, logic_block_id, physic_block_id))) {
      LOG_WARN("failed to get macro block id", K(ret), K(macro_block_index_), K(table_key_), KPC(restore_base_info_));
    } else if (OB_FAIL(physic_block_id.get_backup_macro_block_index(logic_block_id, macro_index))) {
      LOG_WARN("failed to get backup macro block index", K(ret), K(logic_block_id), K(physic_block_id));
    } else if (OB_FAIL(ObRestoreUtils::get_backup_data_type(table_key_, data_type))) {
      LOG_WARN("fail to get backup data type", K(ret), K(table_key_));
    } else if (OB_FAIL(restore_base_info_->get_restore_backup_set_dest(macro_index.backup_set_id_, backup_set_brief_info))) {
      LOG_WARN("fail to get backup set dest", K(ret), K(macro_index));
    } else if (OB_FAIL(backup_set_dest.set(backup_set_brief_info.backup_set_path_))) {
      LOG_WARN("fail to set backup set dest", K(ret));
    } else if (OB_FAIL(share::ObBackupPathUtil::get_macro_block_backup_path(backup_set_dest, macro_index.ls_id_,
        data_type, macro_index.turn_id_, macro_index.retry_id_, macro_index.file_id_, backup_path))) {
      LOG_WARN("failed to get macro block index", K(ret), K(restore_base_info_), K(macro_index), KPC(restore_base_info_));
    } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_macro_block_data(backup_path.get_obstr(),
        restore_base_info_->backup_dest_.get_storage_info(), macro_index, align_size, read_buffer_, data_buffer_))) {
      LOG_WARN("failed to read macro block data", K(ret), K(table_key_), K(macro_index), K(physic_block_id), KPC(restore_base_info_));
    } else {
      data_size_ += data_buffer_.length();
      data.assign(data_buffer_.data(), data_buffer_.length(), data_buffer_.length());
      header.is_reuse_macro_block_ = false;
      header.occupy_size_ = data_buffer_.length();

      macro_block_count_++;
      macro_block_index_++;
      if (macro_block_count_ == copy_macro_range_info_->macro_block_count_) {
        if (logic_block_id != copy_macro_range_info_->end_macro_block_id_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get macro block end macro block id is not equal to macro block range",
              K(ret), K_(macro_block_count), K_(macro_block_index), K(logic_block_id),
              "end_macro_block_id", copy_macro_range_info_->end_macro_block_id_, K(physic_block_id), K(table_key_));
        }
      }
    }
  }
  return ret;
}

/******************ObCopyMacroBlockHandle*********************/
ObCopyMacroBlockHandle::ObCopyMacroBlockHandle()
  : is_reuse_macro_block_(false),
    end_key_buf_("CMacBlockHandle"),
    read_handle_()
{
}

void ObCopyMacroBlockHandle::reset()
{
  is_reuse_macro_block_ = false;
  end_key_buf_.reuse();
  read_handle_.reset();
}

bool ObCopyMacroBlockHandle::is_valid() const
{
  return is_reuse_macro_block_ ? end_key_buf_.is_valid() : read_handle_.is_valid();
}

int ObCopyMacroBlockHandle::set_end_key(
    const blocksstable::ObDatumRowkey &end_key)
{
  int ret = OB_SUCCESS;
  if (!end_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set end key get invalid argument", K(ret), K(end_key));
  } else if (OB_FAIL(end_key_buf_.write_serialize(end_key))) {
    LOG_WARN("failed to write seralize end key", K(ret), K(end_key));
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
    tablet_handle_(),
    sstable_handle_(),
    sstable_(nullptr),
    datum_range_(),
    allocator_("CopyMacroBlock"),
    second_meta_iterator_()
{
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
  } else if (OB_FAIL(ls->ha_get_tablet(table_key.get_tablet_id(), tablet_handle_))) {
    LOG_WARN("failed to get tablet handle", K(ret), K(table_key), K(ls_id));
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
    if (OB_FAIL(second_meta_iterator_.open(datum_range_, blocksstable::DATA_BLOCK_META,
         *sstable_, tablet->get_rowkey_read_info(), allocator_, is_reverse_scan))) {
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
  const int64_t io_timeout_ms = GCONF._data_storage_io_timeout / 1000L;
  copy_macro_block_header.reset();
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
      && OB_FAIL(copy_macro_block_handle_[handle_idx_].read_handle_.wait(io_timeout_ms))) {
    LOG_WARN("failed to wait read handle", K(ret));
  } else {
    if (copy_macro_block_handle_[handle_idx_].is_reuse_macro_block_) {
      occupy_size = copy_macro_block_handle_[handle_idx_].end_key_buf_.pos();
      data.assign(copy_macro_block_handle_[handle_idx_].end_key_buf_.data(), occupy_size);
      copy_macro_block_header.is_reuse_macro_block_ = true;
      copy_macro_block_header.occupy_size_ = occupy_size;
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
  blocksstable::ObMacroBlockReadInfo read_info;
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
    if (OB_FAIL(second_meta_iterator_.get_next(macro_meta))) {
      LOG_WARN("failed to get next macro meta", K(ret), K(macro_idx_), K(copy_macro_range_info_));
    } else if (macro_meta.get_logic_id().logic_version_ <= data_version_) {
      copy_macro_block_handle_[handle_idx_].is_reuse_macro_block_ = true;
      if (OB_FAIL(copy_macro_block_handle_[handle_idx_].set_end_key(macro_meta.end_key_))) {
        LOG_WARN("failed to set end key", K(ret), K(macro_meta), K(macro_idx_), K(copy_macro_range_info_));
      }
    } else {
      copy_macro_block_handle_[handle_idx_].is_reuse_macro_block_ = false;
      read_info.macro_block_id_ = macro_meta.get_macro_id();
      read_info.offset_ = sstable_->get_macro_offset();
      read_info.size_ = sstable_->get_macro_read_size();
      read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_MIGRATE_READ);
      read_info.io_desc_.set_group_id(ObIOModule::HA_COPY_MACRO_BLOCK_IO);
      if (OB_FAIL(ObBlockManager::async_read_block(read_info, copy_macro_block_handle_[handle_idx_].read_handle_))) {
        STORAGE_LOG(WARN, "Fail to async read block, ", K(ret), K(read_info));
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("do prefetch", K(macro_idx_), "macro block count",copy_macro_range_info_.macro_block_count_ ,
          "logical id", macro_meta.get_logic_id(), "physical id", macro_meta.get_macro_id());
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
  } else if (OB_FAIL(srv_rpc_proxy.to(src_info.src_addr_).by(OB_DATA_TENANT_ID).timeout(rpc_timeout).dst_cluster_id(src_info.cluster_id_)
                .ratelimit(true).bg_flow(obrpc::ObRpcProxy::BACKGROUND_FLOW)
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
#ifdef ERRSIM
    if (!tablet_id.is_ls_inner_tablet() && tablet_id_array_.count() > 10 && 5 == tablet_id_index_) {
      ret = OB_E(EventTable::EN_RESTORE_FETCH_TABLET_INFO) OB_SUCCESS;
      LOG_WARN("errsim restore fetch tablet info", K(ret), K(tablet_id), K_(tablet_id_index), K(tablet_id_array_.count()), K(tablet_id_array_));
    }
#endif
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(meta_index_store_->get_backup_meta_index(data_type, tablet_id, tablet_meta_type, tablet_meta_index))
               && OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to get backup meta index", K(ret), K(tablet_id), KPC(restore_base_info_));
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      tablet_info.status_ = ObCopyTabletStatus::TABLET_NOT_EXIST;
      LOG_INFO("tablet not exist", K(tablet_id));
    } else if (OB_FAIL(share::ObBackupPathUtil::get_macro_block_backup_path(restore_base_info_->backup_dest_,
        tablet_meta_index.ls_id_, data_type, tablet_meta_index.turn_id_, tablet_meta_index.retry_id_,
        tablet_meta_index.file_id_, tablet_meta_backup_path))) {
      LOG_WARN("failed to get macro block backup path", K(ret), KPC(restore_base_info_));
    } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_tablet_meta(tablet_meta_backup_path.get_obstr(),
        restore_base_info_->backup_dest_.get_storage_info(), data_type, tablet_meta_index, backup_tablet_meta))) {
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
    allocator_("CSSTObReader"),
    is_sstable_iter_end_(true),
    sstable_index_(0),
    sstable_count_(0)
{
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
  } else if (OB_FAIL(srv_rpc_proxy.to(src_info.src_addr_).by(OB_DATA_TENANT_ID)
                .timeout(rpc_timeout).dst_cluster_id(src_info.cluster_id_)
                .ratelimit(true).bg_flow(obrpc::ObRpcProxy::BACKGROUND_FLOW)
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
    sstable_index_(0),
    is_sstable_iter_end_(true),
    backup_sstable_meta_array_(),
    allocator_("CSSTREReader")
{
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
    sstable_index_ = 0;
    is_sstable_iter_end_ = true;
    is_inited_ = true;
    LOG_INFO("succeed to init copy tablet sstable info restore reader", K(restore_base_info), K(tablet_id_array));
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
  } else if (sstable_index_ >= backup_sstable_meta_array_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable index is unexpected", K(ret), K(sstable_index_), K(backup_sstable_meta_array_));
  } else {
    const backup::ObBackupSSTableMeta &backup_sstable_meta = backup_sstable_meta_array_.at(sstable_index_);
    if (OB_FAIL(fetch_sstable_meta_(backup_sstable_meta, sstable_info))) {
      LOG_WARN("failed to fetch sstable meta", K(ret), K(backup_sstable_meta));
    } else {
      sstable_index_++;
      LOG_INFO("succeed get sstable info", K(sstable_info));
      if (sstable_index_ == backup_sstable_meta_array_.count()) {
        is_sstable_iter_end_ = true;
      }
    }
    LOG_INFO("dump fetch sstable info", "backup_sstable_meta_array_count", backup_sstable_meta_array_.count(),
        K(sstable_index_), "tablet_id_array_count", tablet_id_array_.count(), K(tablet_index_), K(is_sstable_iter_end_));
  }
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
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable info restore reader do not init", K(ret));
  } else if (!tablet_id.is_valid() || !data_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner get backup sstable metas get invalid argument", K(ret), K(tablet_id), K(data_type));
  } else if (OB_FAIL(meta_index_store_->get_backup_meta_index(data_type, tablet_id, sstable_meta_type, sstable_meta_index))) {
      LOG_WARN("failed to get backup meta index", K(ret), K(tablet_id), KPC(restore_base_info_));
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
  } else if (OB_FAIL(share::ObBackupPathUtil::get_macro_block_backup_path(restore_base_info_->backup_dest_,
      sstable_meta_index.ls_id_, data_type, sstable_meta_index.turn_id_, sstable_meta_index.retry_id_,
      sstable_meta_index.file_id_, sstable_meta_backup_path))) {
    LOG_WARN("failed to get macro block backup path", K(ret), KPC(restore_base_info_));
  } else if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_sstable_metas(sstable_meta_backup_path.get_obstr(),
      restore_base_info_->backup_dest_.get_storage_info(), sstable_meta_index, backup_sstable_meta_array))) {
    LOG_WARN("failed to read sstable meta", K(ret), KPC(restore_base_info_));
  } else if (data_type.is_major_backup() && backup_sstable_meta_array.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("major tablet should only has one sstable", K(ret), K(tablet_id), K(sstable_meta_index), K(backup_sstable_meta_array));
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
  } else if (FALSE_IT(copy_header.tablet_id_ = tablet_id)) {
  } else if (OB_FAIL(get_backup_tablet_meta_(tablet_id, copy_header))) {
    LOG_WARN("failed to get tablet meta", K(ret), K(tablet_id));
  } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == copy_header.status_) {
    sstable_index_ = 0;
    is_sstable_iter_end_ = true;
    copy_header.sstable_count_ = 0;
    copy_header.version_ = restore_base_info_->backup_cluster_version_;
    tablet_index_++;
  } else if (OB_FAIL(get_backup_sstable_metas_(tablet_id))) {
    LOG_WARN("failed to get backup sstable metas", K(ret), K(tablet_id), KPC(restore_base_info_));
  } else {
    sstable_index_ = 0;
    is_sstable_iter_end_ = backup_sstable_meta_array_.count() > 0 ? false : true;
    copy_header.sstable_count_ = backup_sstable_meta_array_.count();
    copy_header.version_ = restore_base_info_->backup_cluster_version_;
    tablet_index_++;
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
  } else if (OB_FAIL(ObBackupPathUtil::get_macro_block_backup_path(backup_dest,
      meta_index.ls_id_, backup_data_type, meta_index.turn_id_,
      meta_index.retry_id_, meta_index.file_id_, backup_path))) {
    LOG_WARN("failed to get macro block backup path", K(ret), K(backup_data_type), K(meta_index));
  } else {
    LOG_INFO("get macro block backup path", K(backup_data_type), K(backup_path), K(meta_index));
  }
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
  if (OB_FAIL(backup::ObLSBackupRestoreUtil::read_tablet_meta(
      backup_path.get_obstr(), storage_info, backup_data_type, meta_index, tablet_meta))) {
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
  ObArenaAllocator temp_allocator("RestoreReader", MTL_ID());
  const ObStorageSchema *schema_on_tablet = nullptr;

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
  ObTablet::free_storage_schema(temp_allocator, schema_on_tablet);
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
  } else if (!tablet_sstable_info.minor_sstable_scn_range_.is_empty()
      && tablet->get_tablet_meta().clog_checkpoint_scn_ < tablet_sstable_info.minor_sstable_scn_range_.end_scn_) {
    ret = OB_SSTABLE_NOT_EXIST;
    LOG_WARN("src tablet clog_checkpoint_scn is smaller than dest needed log ts",
        K(ret), K(tablet_sstable_info), KPC(tablet));
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
      if (tablet_sstable_info_.minor_sstable_scn_range_.is_empty()) {
        need_copy_sstable = false;
      } else if (sstable->get_key().scn_range_.end_scn_ <= tablet_sstable_info_.minor_sstable_scn_range_.start_scn_) {
        need_copy_sstable = false;
      } else {
        need_copy_sstable = true;
      }
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
    allocator_("CSSTMBObReader")
{
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
  } else if (OB_FAIL(srv_rpc_proxy.to(src_info.src_addr_).by(OB_DATA_TENANT_ID)
                .timeout(rpc_timeout).dst_cluster_id(src_info.cluster_id_)
                .ratelimit(true).bg_flow(obrpc::ObRpcProxy::BACKGROUND_FLOW)
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
    second_meta_index_store_(nullptr),
    macro_block_id_map_(),
    sstable_index_(0)
{
}

int ObCopySSTableMacroRestoreReader::init(
    const obrpc::ObCopySSTableMacroRangeInfoArg &rpc_arg,
    const ObRestoreBaseInfo &restore_base_info,
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
  } else {
    restore_base_info_ = &restore_base_info;
    second_meta_index_store_ = &second_meta_index_store;
    is_inited_ = true;
  }
  return ret;
}

int ObCopySSTableMacroRestoreReader::get_next_sstable_range_info(
    ObCopySSTableMacroRangeInfo &sstable_macro_range_info)
{
  int ret = OB_SUCCESS;
  sstable_macro_range_info.reset();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable macro restore reader do not init", K(ret));
  } else if (sstable_index_ == rpc_arg_.copy_table_key_array_.count()) {
    ret = OB_ITER_END;
  } else {
    const ObITable::TableKey &table_key = rpc_arg_.copy_table_key_array_.at(sstable_index_);
    if (OB_FAIL(get_next_sstable_range_info_(table_key, sstable_macro_range_info))) {
      LOG_WARN("failed to get next sstable range info", K(ret), K(table_key));
    } else {
      sstable_index_++;
    }
  }
  return ret;
}

int ObCopySSTableMacroRestoreReader::get_next_sstable_range_info_(
    const ObITable::TableKey &table_key,
    ObCopySSTableMacroRangeInfo &sstable_macro_range_info)
{
  int ret = OB_SUCCESS;
  ObArray<ObRestoreMacroBlockId> block_id_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable macro restore reader do not init", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get next sstable range info get invalid argument", K(ret), K(table_key));
  } else {
    SMART_VAR(ObRestoreMacroBlockIdMgr, restore_block_id_mgr) {
      if (OB_FAIL(restore_block_id_mgr.init(rpc_arg_.tablet_id_, table_key, *restore_base_info_, *second_meta_index_store_))) {
        LOG_WARN("failed to init restore block id mgr", K(ret), K(rpc_arg_), K(table_key));
      } else if (OB_FAIL(restore_block_id_mgr.get_restore_macro_block_id_array(block_id_array))) {
        LOG_WARN("failed to get restore macro block id array", K(ret), K(rpc_arg_), K(table_key));
      } else if (OB_FAIL(build_sstable_range_info_(table_key, block_id_array, sstable_macro_range_info))) {
        LOG_WARN("failed to build sstable range info", K(ret), K(rpc_arg_), K(table_key));
      }
    }
  }
  return ret;
}

int ObCopySSTableMacroRestoreReader::build_sstable_range_info_(
    const ObITable::TableKey &table_key,
    const common::ObIArray<ObRestoreMacroBlockId> &block_id_array,
    ObCopySSTableMacroRangeInfo &sstable_macro_range_info)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable macro restore reader do not init", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build sstable range info get invalid argument", K(ret), K(table_key));
  } else if (FALSE_IT(sstable_macro_range_info.copy_table_key_ = table_key)) {
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

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("copy sstable macro ob producer do not init", K(ret));
  } else {
    const ObITable::TableKey &copy_table_key = copy_table_key_array_.at(sstable_index_);
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
    } else {
      macro_range_info_header.copy_table_key_ = copy_table_key;
      const int64_t macro_block_count = sstable->get_data_macro_block_count();
      if (0 == macro_block_count) {
        macro_range_info_header.macro_range_count_ = 0;
      } else {
        macro_range_info_header.macro_range_count_ =
            (macro_block_count + macro_range_max_marco_count_ - 1) / macro_range_max_marco_count_;
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
    table_handle_(),
    tablet_handle_(),
    datum_range_(),
    allocator_("CopySSTMacro"),
    second_meta_iterator_()
{
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
  } else if (OB_FAIL(second_meta_iterator_.open(datum_range_, blocksstable::DATA_BLOCK_META,
      *sstable, tablet->get_rowkey_read_info(), allocator_, is_reverse_scan))) {
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

// ObCopyLSViewInfoObReader
ObCopyLSViewInfoObReader::ObCopyLSViewInfoObReader()
  : is_inited_(false),
    rpc_reader_(),
    allocator_("CPLSVObReader")
{
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
      } else if (OB_FAIL(srv_rpc_proxy.to(src_info.src_addr_).by(OB_DATA_TENANT_ID)
                .timeout(rpc_timeout).dst_cluster_id(src_info.cluster_id_)
                .ratelimit(true).bg_flow(obrpc::ObRpcProxy::BACKGROUND_FLOW)
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
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("can not init twice", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
             || OB_UNLIKELY(!restore_base_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(restore_base_info));
  } else if (restore_base_info.backup_data_version_ >= DATA_VERSION_4_2_0_0
          && OB_FAIL(reader_.init(restore_base_info.backup_dest_, ls_id))) {
    LOG_WARN("fail to init reader", K(ret), K(restore_base_info), K(ls_id));
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
}
}
