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
#include "storage/ddl/ob_cg_macro_block_writer.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/ob_storage_schema.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/ddl/ob_ddl_inc_redo_log_writer.h"
#include "storage/blocksstable/ob_logic_macro_id.h" // for ObMacroDataSeq
#include "storage/blocksstable/index_block/ob_macro_meta_temp_store.h"
#include "storage/ddl/ob_macro_meta_store_manager.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::sql;
using namespace oceanbase::transaction;

/**
* -----------------------------------ObCgMacroBlockWriter-----------------------------------
*/
ObCgMacroBlockWriter::ObCgMacroBlockWriter()
  : is_inited_(false),
    data_desc_(),
    index_builder_(true/*use_double_write_macro_buffer*/),
    ddl_redo_callback_(),
    macro_block_writer_(true/*use_double_write_macro_buffer*/)
{

}

ObCgMacroBlockWriter::~ObCgMacroBlockWriter()
{
  reset();
}

int ObCgMacroBlockWriter::init(
    const ObWriteMacroParam &param,
    const ObITable::TableKey &table_key,
    const ObMacroDataSeq &start_sequence,
    const int64_t row_offset,
    const int64_t lob_start_seq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initialized twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!param.is_valid()
        || !start_sequence.is_valid()
        || row_offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid argument", K(ret), K(param), K(start_sequence), K(row_offset));
  } else if (is_incremental_direct_load(param.direct_load_type_)) { // 增量
    const bool is_inc_major = is_incremental_major_direct_load(param.direct_load_type_);
    const ObWriteTabletParam &tablet_param =
      table_key.tablet_id_ != param.tablet_id_ ? param.lob_meta_tablet_param_ : param.tablet_param_;
    const int64_t parallel_idx = param.slice_idx_;
    const ObTxSEQ seq_no = ObTxSEQ::cast_from_int(param.tx_info_.seq_no_);

    share::SCN mock_start_scn;
    ObMacroSeqParam macro_seq_param;
    ObPreWarmerParam pre_warm_param;
    ObSSTablePrivateObjectCleaner *object_cleaner = nullptr;
    ObDDLIncRedoLogWriterCallback *inc_redo_callback = nullptr;

    IGNORE_RETURN mock_start_scn.convert_for_tx(SS_DDL_START_SCN_VAL);
    macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    macro_seq_param.start_ = start_sequence.macro_data_seq_;
    compaction::ObExecMode exec_mode = GCTX.is_shared_storage_mode() && is_inc_major
                                       ? compaction::ObExecMode::EXEC_MODE_OUTPUT
                                       : compaction::ObExecMode::EXEC_MODE_LOCAL;
    int64_t cg_idx = table_key.get_column_group_id();
    blocksstable::ObMacroMetaTempStore *macro_meta_store = nullptr;
    ObDDLWriteStat *ddl_write_stat = nullptr;

    if (OB_FAIL(pre_warm_param.init(param.ls_id_, table_key.tablet_id_))) {
      LOG_WARN("fail to init pre warm param", KR(ret), K(param.ls_id_), K(table_key.tablet_id_));
    } else if (OB_FAIL(data_desc_.init(true/*is ddl*/,
                                       *tablet_param.storage_schema_,
                                       param.ls_id_,
                                       table_key.get_tablet_id(),
                                       is_inc_major ? compaction::ObMergeType::MAJOR_MERGE
                                                    : compaction::ObMergeType::MINOR_MERGE,
                                       is_inc_major ? table_key.get_snapshot_version()
                                                    : 1L,
                                       param.tenant_data_version_,
                                       tablet_param.is_micro_index_clustered_,
                                       tablet_param.tablet_transfer_seq_,
                                       0/*concurrent_cnt*/,
                                       tablet_param.reorganization_scn_,
                                       table_key.get_end_scn(),
                                       nullptr,
                                       table_key.column_group_idx_/*table_cg_idx*/,
                                       exec_mode,
                                       true/*need_submit_io*/,
                                       is_inc_major))) {
      LOG_WARN("fail to init data store desc", KR(ret), K(param.direct_load_type_));
    } else if (FALSE_IT(data_desc_.get_static_desc().schema_version_ = param.schema_version_)) {
      /* set as a fixed schema version */
    } else if (OB_FAIL(index_builder_.init(data_desc_.get_desc(),
                                           ObSSTableIndexBuilder::DISABLE /*small sstable op*/))) {
      LOG_WARN("fail to init index builder", KR(ret), K(param), K(table_key), K(data_desc_));
    } else {
      // for build the tail index block in macro block
      data_desc_.get_desc().sstable_index_builder_ = &index_builder_;
    }

#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_SUCC(ret) && is_inc_major && GCTX.is_shared_storage_mode()) {
      ObMacroMetaStoreManager *macro_meta_store_mgr = const_cast<ObMacroMetaStoreManager *>(param.macro_meta_store_mgr_);
      if (OB_ISNULL(macro_meta_store_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("macro meta store manager in shared storage mode is null", K(ret), KP(macro_meta_store_mgr), K(param));
      } else if (OB_FAIL(macro_meta_store_mgr->add_macro_meta_store(table_key.tablet_id_, cg_idx, parallel_idx, lob_start_seq, macro_meta_store))) {
        LOG_WARN("fail to add macro meta store", K(ret), K(cg_idx), K(parallel_idx));
      } else {
        data_desc_.get_static_desc().schema_version_ = param.schema_version_;
      }
    }
#endif

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDDLUtil::get_ddl_write_stat(param, table_key, ddl_write_stat))) {
      LOG_WARN("get ddl write stat failed", KR(ret), K(table_key), K(param), KPC(ddl_write_stat));
    } else if (OB_ISNULL(ddl_redo_callback_ = inc_redo_callback = OB_NEW(
                           ObDDLIncRedoLogWriterCallback, ObMemAttr(MTL_ID(), "ddl_redo_cb")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDDLIncRedoLogWriterCallback", KR(ret));
    } else {
      ObDDLRedoLogWriterCallbackInitParam init_param;
      init_param.ls_id_ = param.ls_id_;
      init_param.tablet_id_ = table_key.tablet_id_;
      init_param.direct_load_type_ = param.direct_load_type_;
      init_param.block_type_ = (is_inc_major && param.is_no_logging_) ? DDL_MB_SS_EMPTY_DATA_TYPE : DDL_MB_DATA_TYPE;
      init_param.table_key_ = table_key;
      init_param.start_scn_ = mock_start_scn;
      init_param.task_id_ = param.task_id_;
      init_param.data_format_version_ = param.tenant_data_version_;
      init_param.parallel_cnt_ = param.get_logic_parallel_count();
      init_param.cg_cnt_ = tablet_param.storage_schema_->get_column_group_count();
      init_param.tx_desc_ = param.tx_info_.tx_desc_;
      init_param.trans_id_ = param.tx_info_.trans_id_;
      init_param.seq_no_ = seq_no;
      init_param.with_cs_replica_ = tablet_param.with_cs_replica_;
      init_param.is_inc_major_log_ =
          param.ddl_dag_ ? param.ddl_dag_->is_inc_major_log() : false;
      init_param.macro_meta_store_ = macro_meta_store;
      init_param.write_stat_ = ddl_write_stat;
      if (OB_FAIL(inc_redo_callback->init(init_param))) {
        LOG_WARN("fail to init inc redo callback", KR(ret), K(init_param));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObSSTablePrivateObjectCleaner::get_cleaner_from_data_store_desc(
                 data_desc_.get_desc(), object_cleaner))) {
      LOG_WARN("fail to get cleaner from data store desc", KR(ret));
    } else if (OB_ISNULL(object_cleaner)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("object cleaner is nullptr", KR(ret));
    }
#ifdef OB_BUILD_SHARED_STORAGE
    else if (GCTX.is_shared_storage_mode()) {
      if (!is_inc_major && OB_FAIL(object_cleaner->mark_succeed())) {
        LOG_WARN("fail to mark succeed", KR(ret), K(is_inc_major));
      } else if (OB_FAIL(macro_block_writer_.open_for_ss_ddl(data_desc_.get_desc(),
                                                             parallel_idx,
                                                             macro_seq_param,
                                                             pre_warm_param,
                                                             *object_cleaner,
                                                             ddl_redo_callback_))) {
        LOG_WARN("fail to open for ss ddl", KR(ret));
      }
    }
#endif
    else if (OB_FAIL(macro_block_writer_.open(data_desc_.get_desc(),
                                                parallel_idx,
                                                macro_seq_param,
                                                pre_warm_param,
                                                *object_cleaner,
                                                ddl_redo_callback_))) {
      LOG_WARN("fail to open macro block writer", KR(ret), K(param), K(table_key), K(data_desc_),
               K(start_sequence), KPC(object_cleaner));
    }
  } else { // 全量
    share::SCN mock_start_scn;
    IGNORE_RETURN mock_start_scn.convert_for_tx(SS_DDL_START_SCN_VAL);
    const ObWriteTabletParam &tablet_param = table_key.tablet_id_ != param.tablet_id_ ?
                                                          param.lob_meta_tablet_param_ :
                                                          param.tablet_param_;

    const ObLSID ls_id = param.ls_id_;
    const uint64_t tenant_data_version = param.tenant_data_version_;
    const ObDDLMacroBlockType block_type = param.is_no_logging_ ? DDL_MB_SS_EMPTY_DATA_TYPE : DDL_MB_DATA_TYPE;
    const bool with_cs_replica = tablet_param.with_cs_replica_;
    const bool need_submit_io = true;
    int64_t cg_idx = table_key.column_group_idx_;
    const ObStorageColumnGroupSchema &cg_schema = tablet_param.storage_schema_->get_column_groups().at(cg_idx);
    ObMacroSeqParam macro_seq_param;
    ObPreWarmerParam pre_warm_param;
    ObSSTablePrivateObjectCleaner *object_cleaner = nullptr;
    ObDDLRedoLogWriterCallback *ddl_redo_callback = nullptr;
    compaction::ObExecMode exec_mode = GCTX.is_shared_storage_mode() ?
                                       compaction::ObExecMode::EXEC_MODE_OUTPUT :
                                       compaction::ObExecMode::EXEC_MODE_LOCAL;
    ObSSTableIndexBuilder::ObSpaceOptimizationMode space_opt_mode = GCTX.is_shared_storage_mode() ?
                                                                    ObSSTableIndexBuilder::DISABLE :
                                                                    ObSSTableIndexBuilder::ENABLE;
    blocksstable::ObMacroMetaTempStore *macro_meta_store = nullptr;
    ObDDLWriteStat *ddl_write_stat = nullptr;
    const int64_t parallel_idx = param.slice_idx_;

    macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    macro_seq_param.start_ = start_sequence.macro_data_seq_;

    if (OB_FAIL(pre_warm_param.init(ls_id, table_key.tablet_id_))) {
      LOG_WARN("fail to initialize pre warm param", K(ret), K(ls_id), K(table_key.tablet_id_));
    } else if (OB_FAIL(data_desc_.init(true/*is ddl*/,
                                       *tablet_param.storage_schema_,
                                       ls_id,
                                       table_key.get_tablet_id(),
                                       compaction::ObMergeType::MAJOR_MERGE,
                                       table_key.get_snapshot_version(),
                                       tenant_data_version,
                                       tablet_param.is_micro_index_clustered_,
                                       tablet_param.tablet_transfer_seq_,
                                       0/*concurrent_cnt*/,
                                       SCN::min_scn()/*reorganization_scn : only for ss*/,
                                       SCN::min_scn(),
                                       &cg_schema,
                                       cg_idx,
                                       exec_mode,
                                       need_submit_io))) {
      LOG_WARN("fail to initialize data store desc", K(ret));
    } else if (FALSE_IT(data_desc_.get_static_desc().schema_version_ = param.schema_version_)) {
      /* set as a fixed schema version */
    } else if (OB_FAIL(index_builder_.init(data_desc_.get_desc(), space_opt_mode/*small sstable op*/))) {
      LOG_WARN("fail to initialize sstable index builder", K(ret), K(ls_id), K(table_key), K(data_desc_));
    } else {
      // for build the tail index block in macro block
      data_desc_.get_desc().sstable_index_builder_ = &index_builder_;
    }

#ifdef OB_BUILD_SHARED_STORAGE
    if (OB_SUCC(ret) && GCTX.is_shared_storage_mode()) {
      ObMacroMetaStoreManager *macro_meta_store_mgr = const_cast<ObMacroMetaStoreManager *>(param.macro_meta_store_mgr_);
      if (OB_ISNULL(macro_meta_store_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("macro meta store manager in shared storage mode is null", K(ret), KP(macro_meta_store_mgr), K(param));
      } else if (OB_FAIL(macro_meta_store_mgr->add_macro_meta_store(table_key.tablet_id_, cg_idx, parallel_idx, lob_start_seq, macro_meta_store))) {
        LOG_WARN("fail to add macro meta store", K(ret), K(cg_idx), K(parallel_idx));
      } else {
        data_desc_.get_static_desc().schema_version_ = param.schema_version_;
      }
    }
#endif
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDDLUtil::get_ddl_write_stat(param, table_key, ddl_write_stat))) {
      LOG_WARN("get ddl write stat failed", K(ret), K(table_key), K(with_cs_replica), K(param), KPC(ddl_write_stat));
    } else if (OB_ISNULL(ddl_redo_callback_ = ddl_redo_callback = OB_NEW(
                           ObDDLRedoLogWriterCallback, ObMemAttr(MTL_ID(), "ddl_redo_cb")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObDDLRedoLogWriterCallback", KR(ret));
    } else {
      ObDDLRedoLogWriterCallbackInitParam init_param;
      init_param.ls_id_ = ls_id;
      init_param.tablet_id_ = table_key.tablet_id_;
      init_param.direct_load_type_ = param.direct_load_type_;
      init_param.block_type_ = block_type;
      init_param.table_key_ = table_key;
      init_param.start_scn_ = mock_start_scn;
      init_param.task_id_ = param.task_id_;
      init_param.data_format_version_ = tenant_data_version;
      init_param.parallel_cnt_ = param.get_logic_parallel_count();
      init_param.cg_cnt_ = tablet_param.storage_schema_->get_column_group_count();
      init_param.row_id_offset_ = row_offset;
      init_param.need_delay_ = false;
      init_param.with_cs_replica_ = with_cs_replica;
      init_param.need_submit_io_ = need_submit_io;
      init_param.macro_meta_store_ = macro_meta_store;
      init_param.write_stat_ = ddl_write_stat;
      init_param.is_inc_major_log_ = param.ddl_dag_ ? param.ddl_dag_->is_inc_major_log() : false ;
      if (OB_FAIL(ddl_redo_callback->init(init_param))) {
        LOG_WARN("fail to initialize redo log callback", K(ret), K(init_param));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObSSTablePrivateObjectCleaner::get_cleaner_from_data_store_desc(data_desc_.get_desc(), object_cleaner))) {
      LOG_WARN("fail to get cleaner from data store desc", K(ret), K(data_desc_.get_desc()));
#ifdef OB_BUILD_SHARED_STORAGE
    } else if (GCTX.is_shared_storage_mode()) {
      if (OB_FAIL(macro_block_writer_.open_for_ss_ddl(data_desc_.get_desc(),
                                                      parallel_idx,
                                                      macro_seq_param,
                                                      pre_warm_param,
                                                      *object_cleaner,
                                                      ddl_redo_callback_))) {
        LOG_WARN("fail to open macro block writer in ss mode",
            K(ret), K(ls_id), K(table_key), K(data_desc_), K(start_sequence), KPC(object_cleaner));
      }
#endif
    } else {
      if (OB_FAIL(macro_block_writer_.open(data_desc_.get_desc(),
                                           parallel_idx,
                                           macro_seq_param,
                                           pre_warm_param,
                                           *object_cleaner,
                                           ddl_redo_callback_))) {
        LOG_WARN("fail to open macro block writer",
            K(ret), K(ls_id), K(table_key), K(data_desc_), K(start_sequence), KPC(object_cleaner));
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  return ret;
}

int ObCgMacroBlockWriter::append_row(const ObDatumRow &cg_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret), K(is_inited_));
  } else if (OB_FAIL(macro_block_writer_.append_row(cg_row))) {
    LOG_WARN("write column group row failed", K(ret), K(cg_row));
  }
  return ret;
}

int ObCgMacroBlockWriter::append_batch(const blocksstable::ObBatchDatumRows &cg_rows)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret), K(is_inited_));
  } else if (OB_FAIL(macro_block_writer_.append_batch(cg_rows))) {
    LOG_WARN("write column group row failed", K(ret), K(cg_rows));
  }
  return ret;
}

int ObCgMacroBlockWriter::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not initialized", K(ret), K(is_inited_));
  } else if (OB_FAIL(macro_block_writer_.close())) {
    LOG_WARN("fail to close macro block writer", K(ret));
  }
  return ret;
}

void ObCgMacroBlockWriter::reset()
{
  is_inited_ = false;
  data_desc_.reset();
  index_builder_.reset();
  OB_DELETE(ObIMacroBlockFlushCallback, ObMemAttr(MTL_ID(), "ddl_redo_cb"), ddl_redo_callback_);
  macro_block_writer_.reset();
}
