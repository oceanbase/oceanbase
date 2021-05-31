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
#include "ob_migrate_macro_block_writer.h"
#include "ob_partition_migrator.h"
#include "share/ob_task_define.h"
#include "blocksstable/ob_macro_block_checker.h"
#include "storage/blocksstable/ob_store_file.h"
#include "observer/omt/ob_multi_tenant.h"
#include "storage/ob_partition_base_data_ob_reader.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace blocksstable;

namespace storage {
ObMigrateMacroBlockWriter::ObMigrateMacroBlockWriter()
    : is_inited_(false), tenant_id_(OB_INVALID_ID), reader_(NULL), file_handle_()
{}

int ObMigrateMacroBlockWriter::init(
    ObIPartitionMacroBlockReader* reader, const uint64_t tenant_id, blocksstable::ObStorageFile* pg_file)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("writer should not be init twice", K(ret));
  } else if (OB_ISNULL(reader) || OB_INVALID_ID == tenant_id || OB_ISNULL(pg_file)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("reader should not be null ", K(ret));
  } else {
    ObTenantFileKey file_key(pg_file->get_tenant_id(), pg_file->get_file_id());
    if (OB_FAIL(OB_SERVER_FILE_MGR.get_tenant_file(file_key, file_handle_))) {
      LOG_WARN("fail to get tenant file", K(ret), K(file_handle_));
    } else {
      reader_ = reader;
      tenant_id_ = tenant_id;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObMigrateMacroBlockWriter::check_macro_block(const ObFullMacroBlockMeta& meta, const ObBufferReader& data)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (data.length() < meta.meta_->micro_block_index_offset_) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN,
        "invalid macro block buf len must not less then micro block index offset",
        K(ret),
        K(data),
        K(meta.meta_->micro_block_index_offset_),
        K(meta));
  } else if (data.length() != meta.meta_->occupy_size_) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN,
        "macro block len is not as same as meta occupy size",
        K(ret),
        K(data),
        K(meta.meta_->occupy_size_),
        K(meta));
  } else if (NULL == data.data() || data.length() < 0 || data.length() > data.capacity()) {
    ret = OB_INVALID_DATA;
    STORAGE_LOG(WARN, "invalid data", K(ret), K(data));
  } else {
    const int64_t migrate_verify_level = GCONF._migrate_block_verify_level;
    ObMacroBlockCheckLevel check_level = ObMacroBlockCheckLevel::CHECK_LEVEL_AUTO;
    switch (migrate_verify_level) {
      case 0:
        check_level = ObMacroBlockCheckLevel::CHECK_LEVEL_NOTHING;
        break;
      case 1:
        // TODO() need other check level, replace table_id in meta
        check_level = ObMacroBlockCheckLevel::CHECK_LEVEL_AUTO;
        break;
      case 2:
        check_level = ObMacroBlockCheckLevel::CHECK_LEVEL_ROW;
        break;
      default:
        check_level = ObMacroBlockCheckLevel::CHECK_LEVEL_AUTO;
        ret = OB_ERR_UNEXPECTED;  // it's ok to override this ret code
        STORAGE_LOG(WARN, "invalid check level", K(ret), K(migrate_verify_level));
        break;
    }

    if (OB_FAIL(macro_checker_.check(data.data(), data.length(), meta, check_level))) {
      STORAGE_LOG(ERROR, "failed to check macro block", K(ret), K(meta), K(data), K(check_level));
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_RESTORE_MACRO_CRC_ERROR) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(INFO, "ERRSIM check_macro_block", K(ret));
    }
  }
#endif
  return ret;
}

int ObMigrateMacroBlockWriter::process(blocksstable::ObMacroBlocksWriteCtx& copied_ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  common::ObArenaAllocator allocator(ObNewModIds::OB_PARTITION_MIGRATE);
  blocksstable::ObFullMacroBlockMeta meta;
  blocksstable::ObBufferReader data(NULL, 0, 0);
  blocksstable::MacroBlockId macro_id;
  blocksstable::ObMacroBlockWriteInfo write_info;
  blocksstable::ObMacroBlockHandle write_handle;
  blocksstable::ObStorageFile* file = NULL;
  copied_ctx.reset();
  int64_t write_count = 0;
  int64_t log_seq_num = 0;
  int64_t data_size = 0;

  write_info.io_desc_.category_ = SYS_IO;
  write_info.io_desc_.wait_event_no_ = ObWaitEventIds::DB_FILE_MIGRATE_WRITE;
  write_info.block_write_ctx_ = &copied_ctx;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM.init_file_ctx(STORE_FILE_MACRO_BLOCK, copied_ctx.file_ctx_))) {
    LOG_WARN("failed to init write ctx", K(ret));
  } else if (OB_ISNULL(file = file_handle_.get_storage_file())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get storage file", K(ret), K(file_handle_));
  } else if (OB_FAIL(SLOGGER.begin(OB_LOG_CS_MIGRATE_MACRO_BLOCK))) {
    STORAGE_LOG(WARN, "fail to begin commit log.", K(ret));
  } else {
    const int64_t io_timeout_ms = GCONF._data_storage_io_timeout / 1000L;
    while (OB_SUCC(ret)) {
      if (ObPartGroupMigrator::get_instance().is_stop()) {
        ret = OB_CANCELED;
        STORAGE_LOG(WARN, "server is stop, interrupts copy data.", K(ret));
        break;
      } else if (!GCTX.omt_->has_tenant(tenant_id_)) {
        ret = OB_TENANT_NOT_EXIST;
        STORAGE_LOG(WARN, "tenant not exists, stop migrate", K(ret), K(tenant_id_));
        break;
      }
      dag_yield();
      if (OB_FAIL(reader_->get_next_macro_block(meta, data, macro_id))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "failed to get next macro block", K(ret));
        } else {
          STORAGE_LOG(INFO, "get next macro block end");
          ret = OB_SUCCESS;
        }
        break;
      } else if (OB_FAIL(check_macro_block(meta, data))) {
        STORAGE_LOG(ERROR, "failed to check macro block, fatal error", K(ret), K(write_count), K(data));
        ret = OB_INVALID_DATA;  // overwrite ret
      } else if (!write_handle.is_empty() && OB_FAIL(write_handle.wait(io_timeout_ms))) {
        STORAGE_LOG(WARN, "failed to wait write handle", K(ret));
      } else {
        write_info.buffer_ = data.data();
        write_info.size_ = data.capacity();
        write_info.meta_ = meta;
        write_handle.reset();
        write_handle.set_file(file);
        if (!write_info.block_write_ctx_->file_handle_.is_valid() &&
            OB_FAIL(write_info.block_write_ctx_->file_handle_.assign(file_handle_))) {
          LOG_WARN("failed to set file handle", K(ret), K(file_handle_));
        } else if (OB_FAIL(file->async_write_block(write_info, write_handle))) {
          STORAGE_LOG(WARN, "write to data file failed, ", K(ret));
        } else {
          ObTaskController::get().allow_next_syslog();
          STORAGE_LOG(INFO, "success copy macro block", K(write_count), K(meta));
          ++write_count;
        }
      }
    }

    if (!write_handle.is_empty()) {
      int tmp_ret = write_handle.wait(io_timeout_ms);
      if (OB_SUCCESS != tmp_ret) {
        STORAGE_LOG(WARN, "failed to wait write handle", K(ret), K(tmp_ret));
        if (OB_SUCC(ret)) {
          ret = tmp_ret;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(SLOGGER.commit(log_seq_num))) {
        STORAGE_LOG(WARN, "fail to commit log.", K(ret));
      }
    } else {
      if (OB_SUCCESS != (tmp_ret = SLOGGER.abort())) {
        STORAGE_LOG(WARN, "fail to abort log.", K(ret));
      }
    }

    int64_t cost_time_ms = (ObTimeUtility::current_time() - start_time) / 1000;
    int64_t data_size_KB = reader_->get_data_size() / 1024;
    int64_t speed_KB = 0;
    if (cost_time_ms > 0) {
      speed_KB = data_size_KB * 1000 / cost_time_ms;
    }
    data_size += reader_->get_data_size();
    STORAGE_LOG(INFO,
        "finish copy macro block data",
        K(ret),
        "macro_count",
        copied_ctx.get_macro_block_count(),
        K(cost_time_ms),
        "data_size_B",
        reader_->get_data_size(),
        K(speed_KB));
  }

  return ret;
}
}  // namespace storage
}  // namespace oceanbase
