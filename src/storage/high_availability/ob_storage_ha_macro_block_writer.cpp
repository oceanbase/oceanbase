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
   dag_id_(),
   reader_(NULL),
   macro_checker_(),
   index_block_rebuilder_(nullptr)
{
}

int ObStorageHAMacroBlockWriter::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const ObDagId &dag_id,
    ObICopyMacroBlockReader *reader,
    ObIndexBlockRebuilder *index_block_rebuilder)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("writer should not be init twice", K(ret));
  } else if (OB_ISNULL(reader)
            || OB_INVALID_ID == tenant_id
            || !ls_id.is_valid()
            || dag_id.is_invalid()
            || OB_ISNULL(index_block_rebuilder)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(reader), KP(index_block_rebuilder), K(dag_id), K(ls_id));
  } else {
    reader_ = reader;
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    dag_id_.set(dag_id);
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
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (NULL == data.data() || data.length() < 0 || data.length() > data.capacity()) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid data", K(ret), K(data));
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
        STORAGE_LOG(WARN, "invalid check level", K(ret), K(migrate_verify_level));
        break;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(macro_checker_.check(data.data(), data.length(), check_level))) {
      STORAGE_LOG(ERROR, "failed to check macro block", K(ret), K(data), K(check_level));
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_RESTORE_MACRO_CRC_ERROR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(INFO, "ERRSIM check_macro_block", K(ret));
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
  common::ObArenaAllocator allocator("HAMBWriter");
  blocksstable::ObBufferReader data(NULL, 0, 0);
  blocksstable::MacroBlockId macro_id;
  blocksstable::ObMacroBlockWriteInfo write_info;
  blocksstable::ObMacroBlockHandle write_handle;
  copied_ctx.reset();
  int64_t write_count = 0;
  int64_t log_seq_num = 0;
  int64_t data_size = 0;
  bool is_cancel = false;
  obrpc::ObCopyMacroBlockHeader header;
  write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_MIGRATE_WRITE);
  write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
  write_info.io_desc_.set_resource_group_id(THIS_WORKER.get_group_id());
  write_info.io_desc_.set_sys_module_id(ObIOModule::HA_MACRO_BLOCK_WRITER_IO);
  int32_t result = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      if (!GCTX.omt_->has_tenant(tenant_id_)) {
        ret = OB_TENANT_NOT_EXIST;
        STORAGE_LOG(WARN, "tenant not exists, stop migrate", K(ret), K(tenant_id_));
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
      } else if (OB_FAIL(reader_->get_next_macro_block(header, data))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "failed to get next macro block", K(ret));
        } else {
          STORAGE_LOG(INFO, "get next macro block end");
          ret = OB_SUCCESS;
        }
        break;
      } else if (OB_FAIL(check_macro_block_(data))) {
        STORAGE_LOG(ERROR, "failed to check macro block, fatal error", K(ret), K(write_count), K(data));
        ret = OB_INVALID_DATA;// overwrite ret
      } else if (!write_handle.is_empty() && OB_FAIL(write_handle.wait())) {
        STORAGE_LOG(WARN, "failed to wait write handle", K(ret), K(write_info));
      } else if (header.is_reuse_macro_block_) {
        //TODO(muwei.ym) reuse macro block in 4.3
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("header is reuse macro block", K(ret));
      } else {
        write_info.buffer_ = data.data();
        write_info.size_ = data.upper_align_length();
        write_handle.reset();

        if (OB_FAIL(ObBlockManager::async_write_block(write_info, write_handle))) {
          STORAGE_LOG(WARN, "fail to async write block", K(ret), K(write_info), K(write_handle));
        } else if (OB_FAIL(copied_ctx.add_macro_block_id(write_handle.get_macro_id()))) {
          STORAGE_LOG(WARN, "fail to add macro id", K(ret), "macro id", write_handle.get_macro_id());
        } else if (OB_FAIL(index_block_rebuilder_->append_macro_row(data.data(), data.capacity(),
            write_handle.get_macro_id()))) {
          LOG_WARN("failed to append macro row", K(ret), K(write_handle));
        } else {
          ObTaskController::get().allow_next_syslog();
          STORAGE_LOG(INFO, "success copy macro block", K(write_count));
          ++write_count;
        }
      }
    }

    if (!write_handle.is_empty()) {
      int tmp_ret = write_handle.wait();
      if (OB_SUCCESS != tmp_ret) {
        STORAGE_LOG(WARN, "failed to wait write handle", K(ret), K(tmp_ret), K(write_info));
        if (OB_SUCC(ret)) {
          ret = tmp_ret;
        }
      }
    }

    int64_t cost_time_ms = (ObTimeUtility::current_time() - start_time) / 1000;
    int64_t data_size_KB = reader_->get_data_size() / 1024;
    int64_t speed_KB = 0;
    if (cost_time_ms > 0) {
      speed_KB = data_size_KB * 1000 / cost_time_ms;
    }
    data_size += reader_->get_data_size();
    STORAGE_LOG(INFO, "finish copy macro block data", K(ret),
                "macro_count", copied_ctx.get_macro_block_count(),
                K(cost_time_ms), "data_size_B",  reader_->get_data_size(), K(speed_KB));
  }

  return ret;
}


} // storage
} // oceanbase

