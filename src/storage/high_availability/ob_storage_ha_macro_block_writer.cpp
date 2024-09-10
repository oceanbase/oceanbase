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
#include "ob_storage_ha_macro_block_writer.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace blocksstable;

namespace storage
{
ObStorageHAMacroBlockWriter::ObStorageHAMacroBlockWriter()
 : is_inited_(false),
   tenant_id_(OB_INVALID_ID),
   ls_id_(),
   tablet_id_(),
   dag_id_(),
   sstable_param_(nullptr),
   reader_(NULL),
   index_block_rebuilder_(nullptr),
   macro_checker_()
{
}

int ObStorageHAMacroBlockWriter::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const ObDagId &dag_id,
    const ObMigrationSSTableParam *sstable_param,
    ObICopyMacroBlockReader *reader,
    ObIndexBlockRebuilder *index_block_rebuilder)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("writer should not be init twice", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id
	     || !ls_id.is_valid()
             || !tablet_id.is_valid()
	     || dag_id.is_invalid()
             || OB_ISNULL(sstable_param)
             || OB_ISNULL(reader)
             || OB_ISNULL(index_block_rebuilder)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(tablet_id), KP(sstable_param),
        KP(reader), KP(index_block_rebuilder));
  } else if (OB_FAIL(check_sstable_param_for_init_(sstable_param))) {
    LOG_WARN("failed to check sstable param", K(ret));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    dag_id_.set(dag_id);
    sstable_param_ = sstable_param;
    reader_ = reader;
    index_block_rebuilder_ = index_block_rebuilder;
    is_inited_ = true;
  }
  return ret;
}

int ObStorageHAMacroBlockWriter::check_macro_block_(
    const blocksstable::ObBufferReader &data)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (NULL == data.data() || data.length() < 0 || data.length() > data.capacity()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data", K(ret), K(data));
  } else {
    const int64_t migrate_verify_level = GCONF._migrate_block_verify_level;
    ObMacroBlockCheckLevel check_level = ObMacroBlockCheckLevel::CHECK_LEVEL_MAX;
    switch (migrate_verify_level) {
      case 0:
        check_level = ObMacroBlockCheckLevel::CHECK_LEVEL_NONE;
        break;
      case 1:
        check_level = ObMacroBlockCheckLevel::CHECK_LEVEL_PHYSICAL;
        break;
      case 2:
        //check_level = ObMacroBlockCheckLevel::CHECK_LEVEL_LOGICAL;
        //Here using logical has a bug.
        check_level = ObMacroBlockCheckLevel::CHECK_LEVEL_PHYSICAL;
        break;
      default:
        check_level = ObMacroBlockCheckLevel::CHECK_LEVEL_MAX;
        ret = OB_ERR_UNEXPECTED; // it's ok to override this ret code
        LOG_WARN("invalid check level", K(ret), K(migrate_verify_level));
        break;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(macro_checker_.check(data.data(), data.length(), check_level))) {
      LOG_ERROR("failed to check macro block", K(ret), K(data), K(check_level));
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_RESTORE_MACRO_CRC_ERROR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      LOG_INFO("ERRSIM check_macro_block", K(ret));
    }
  }
#endif
  return ret;
}

int ObStorageHAMacroBlockWriter::process(
    blocksstable::ObMacroBlocksWriteCtx &copied_ctx,
    ObIHADagNetCtx &ha_dag_net_ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  blocksstable::ObBufferReader data(NULL, 0, 0);
  ObStorageObjectOpt opt;
  blocksstable::ObStorageObjectWriteInfo write_info;
  blocksstable::ObStorageObjectHandle write_handle;
  blocksstable::ObDataMacroBlockMeta macro_meta;
  blocksstable::ObDatumRow macro_meta_row;
  copied_ctx.reset();
  int64_t write_count = 0;
  int64_t reuse_count = 0;
  int64_t log_seq_num = 0;
  int64_t data_size = 0;
  int64_t write_size = 0;
  int64_t macro_meta_row_pos = 0;
  bool is_cancel = false;
  obrpc::ObCopyMacroBlockHeader header;
  MacroBlockId macro_block_id;
  int32_t result = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(macro_meta_row.init(OB_MAX_ROWKEY_COLUMN_NUMBER + 1))) {
    // use max row key cnt + 1 as capacity, because meta row is kv
    STORAGE_LOG(WARN, "failed to init macro meta row", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      macro_block_id.reset();
      if (!GCTX.omt_->has_tenant(tenant_id_)) {
        ret = OB_TENANT_NOT_EXIST;
        LOG_WARN("tenant not exists, stop migrate", K(ret), K(tenant_id_));
        break;
      } else if (OB_FAIL(SYS_TASK_STATUS_MGR.is_task_cancel(dag_id_, is_cancel))) {
        STORAGE_LOG(WARN, "failed to check is task canceled", K(ret), K_(dag_id));
      } else if (is_cancel) {
        ret = OB_CANCELED;
        STORAGE_LOG(WARN, "copy task has been canceled, skip remaining macro blocks",
          K(ret), K_(dag_id), "finished_macro_block_count", copied_ctx.macro_block_list_.count());
        break;
      } else if (OB_FAIL(ObStorageHAUtils::check_log_status(tenant_id_, ls_id_, result))) {
        LOG_WARN("failed to check log status", K(ret), K(tenant_id_), K(ls_id_));
      } else if (OB_SUCCESS != result) {
        LOG_INFO("can not replay log, it will retry", K(result), K(ha_dag_net_ctx));
        if (OB_FAIL(ha_dag_net_ctx.set_result(result/*result*/, true/*need_retry*/))) {
          LOG_WARN("failed to set result", K(ret), K(ha_dag_net_ctx));
        } else {
          ret = result;
          LOG_WARN("log sync or replay error, need retry", K(ret), K(tenant_id_), K(ls_id_), K(ha_dag_net_ctx));
        }
      } else if (OB_FAIL(dag_yield())) {
        STORAGE_LOG(WARN, "fail to yield dag", KR(ret));
      } else if (OB_FAIL(reader_->get_next_macro_block(header, data, macro_block_id))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next macro block", K(ret));
        } else {
          LOG_INFO("get next macro block end");
          ret = OB_SUCCESS;
        }
        break;
      } else if (!header.is_valid()
        || (!header.is_reuse_macro_block_ && header.data_type_ != obrpc::ObCopyMacroBlockHeader::DataType::MACRO_DATA)
        || (header.is_reuse_macro_block_ && header.data_type_ != obrpc::ObCopyMacroBlockHeader::DataType::MACRO_META_ROW)) {
        // invalid argument, if not reuse macro block, buffer must contain whole macro block data
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(ERROR, "invalid header", K(ret), K(header));
      } else if (header.is_reuse_macro_block_) {
        macro_meta_row_pos = 0;
        macro_meta_row.reuse();
        macro_meta.reset();

        if (OB_FAIL(macro_meta_row.deserialize(data.data(), header.occupy_size_, macro_meta_row_pos))) {
          STORAGE_LOG(WARN, "failed to deserialize macro meta row", K(ret), K(data), K(header));
        } else if (OB_FAIL(macro_meta.parse_row(macro_meta_row))) {
          STORAGE_LOG(WARN, "failed to parse macro meta row", K(ret), K(macro_meta_row));
        } else if (macro_meta.get_macro_id() == ObIndexBlockRowHeader::DEFAULT_IDX_ROW_MACRO_ID) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "macro id from src has been set to default", K(ret), K(macro_meta));
        } else if (OB_FAIL(copied_ctx.add_macro_block_id(macro_meta.get_macro_id()))) {
          STORAGE_LOG(WARN, "fail to add macro id", K(ret), K(macro_meta));
        } else if (OB_FAIL(index_block_rebuilder_->append_macro_row(macro_meta))) {
          STORAGE_LOG(WARN, "failed to append macro row", K(ret), K(macro_meta));
        } else {
          ++reuse_count;
        }
      } else if (OB_FAIL(check_macro_block_(data))) {
        LOG_ERROR("failed to check macro block, fatal error", K(ret), K(write_count), K(data));
        ret = OB_INVALID_DATA;// overwrite ret
      } else if (!write_handle.is_empty() && OB_FAIL(write_handle.wait())) {
        LOG_WARN("failed to wait write handle", K(ret), K(write_info));
      } else if (OB_FAIL(set_macro_write_info_(macro_block_id, write_info, opt)))  {
        LOG_WARN("failed to set macro write info", K(ret), K(macro_block_id));
      } else if (OB_FAIL(write_macro_block_(opt, write_info, write_handle, copied_ctx, data))) {
        LOG_WARN("failed to write macro block", K(ret), K(opt), K(macro_block_id));
      } else {
        ObTaskController::get().allow_next_syslog();
        ++write_count;
        write_size += data.capacity();
        LOG_INFO("success copy macro block", K(write_count));
      }
    }

    if (!write_handle.is_empty()) {
      int tmp_ret = write_handle.wait();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("failed to wait write handle", K(ret), K(tmp_ret), K(write_info));
        if (OB_SUCC(ret)) {
          ret = tmp_ret;
        }
      }
    }

    int64_t cost_time_ms = (ObTimeUtility::current_time() - start_time) / 1000;
    int64_t data_size_KB = reader_->get_data_size() / 1024;
    int64_t write_size_KB = write_size / 1024;

    int64_t rspeed_KB = 0;
    int64_t wspeed_KB = 0;
    if (cost_time_ms > 0) {
      rspeed_KB = data_size_KB * 1000 / cost_time_ms;
      wspeed_KB = write_size_KB * 1000 / cost_time_ms;
    }
    data_size += reader_->get_data_size();
    LOG_INFO("finish copy macro block data", K(ret),
                "macro_count", copied_ctx.get_macro_block_count(), K(write_count), K(reuse_count),
                K(cost_time_ms), "read_size_B",  reader_->get_data_size(), K(write_size), K(rspeed_KB), K(wspeed_KB));
  }

  return ret;
}

int ObStorageHAMacroBlockWriter::write_macro_block_(
    const ObStorageObjectOpt &opt,
    blocksstable::ObStorageObjectWriteInfo &write_info,
    blocksstable::ObStorageObjectHandle &write_handle,
    blocksstable::ObMacroBlocksWriteCtx &copied_ctx,
    blocksstable::ObBufferReader &data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    write_info.buffer_ = data.data();
    write_info.size_ = data.upper_align_length();
    write_handle.reset();

    if (OB_FAIL(ObObjectManager::async_write_object(opt, write_info, write_handle))) {
      LOG_WARN("fail to async write block", K(ret), K(write_info), K(write_handle));
    } else if (OB_FAIL(copied_ctx.add_macro_block_id(write_handle.get_macro_id()))) {
      LOG_WARN("fail to add macro id", K(ret), "macro id", write_handle.get_macro_id());
    } else if (OB_FAIL(append_macro_row_(data.data(), data.capacity(), write_handle.get_macro_id()))) {
      LOG_WARN("failed to append macro row", K(ret), K(write_handle));
    }
  }
  return ret;
}


// ObStorageHALocalMacroBlockWriter
int ObStorageHALocalMacroBlockWriter::check_sstable_param_for_init_(const ObMigrationSSTableParam *sstable_param) const
{
  int ret = OB_SUCCESS;
  if (sstable_param->is_shared_macro_blocks_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable has shared macro blocks", K(ret), KPC(sstable_param));
  }
  return ret;
}

int ObStorageHALocalMacroBlockWriter::set_macro_write_info_(
    const MacroBlockId &macro_block_id,
    blocksstable::ObStorageObjectWriteInfo &write_info,
    blocksstable::ObStorageObjectOpt &opt)
{
  int ret = OB_SUCCESS;
  UNUSED(macro_block_id);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_MIGRATE_WRITE);
    write_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
    write_info.io_desc_.set_sys_module_id(ObIOModule::HA_MACRO_BLOCK_WRITER_IO);
    write_info.io_desc_.set_sealed();
    write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    write_info.mtl_tenant_id_ = MTL_ID();
    write_info.offset_ = 0;
    opt.set_private_object_opt(tablet_id_.id());
  }
  return ret;
}

int ObStorageHALocalMacroBlockWriter::append_macro_row_(
    const char *buf,
    const int64_t size,
    const blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(index_block_rebuilder_->append_macro_row(buf, size, macro_id, -1 /*absolute_row_offset*/))) {
    LOG_WARN("failed to append macro row", K(ret), K(macro_id));
  }
  return ret;
}


#ifdef OB_BUILD_SHARED_STORAGE
// ObStorageHASharedMacroBlockWriter
int ObStorageHASharedMacroBlockWriter::check_sstable_param_for_init_(const ObMigrationSSTableParam *sstable_param) const
{
  int ret = OB_SUCCESS;
  if (!sstable_param->is_shared_macro_blocks_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sstable has no shared macro blocks", K(ret), KPC(sstable_param));
  }
  return ret;
}

int ObStorageHASharedMacroBlockWriter::set_macro_write_info_(
    const MacroBlockId &macro_block_id,
    blocksstable::ObStorageObjectWriteInfo &write_info,
    blocksstable::ObStorageObjectOpt &opt)
{
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id(macro_block_id.second_id());
  if (!macro_block_id.is_valid() || !macro_block_id.is_id_mode_share()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("prepare shared macro write info get invalid argument", K(ret), K(macro_block_id));
  } else if (tablet_id_ != tablet_id) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro tablet id in shared storage is not match", K(ret), K(macro_block_id), K(tablet_id), K(tablet_id_));
  } else {
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_MIGRATE_WRITE);
    write_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
    write_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
    write_info.io_desc_.set_unsealed();
    write_info.mtl_tenant_id_ = MTL_ID();
    write_info.offset_ = 0;
    write_info.ls_epoch_id_ = 0;
    opt.set_ss_share_data_macro_object_opt(macro_block_id.second_id(), macro_block_id.third_id(), macro_block_id.column_group_id());
  }
  return ret;
}

int ObStorageHASharedMacroBlockWriter::append_macro_row_(
    const char *buf,
    const int64_t size,
    const blocksstable::MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  if (sstable_param_->is_shared_sstable()) {
    if (OB_FAIL(index_block_rebuilder_->append_macro_row(buf, size, macro_id, -1 /*absolute_row_offset*/))) {
      LOG_WARN("failed to append macro row", K(ret), K(macro_id));
    }
  } else {
    // sstable which only shared macro blocks need not rebuild index.
  }

  return ret;
}

#endif

} // storage
} // oceanbase

