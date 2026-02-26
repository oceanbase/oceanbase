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

#include "storage/ddl/ob_writer_args_struct.h"
#include "storage/ddl/ob_macro_meta_store_manager.h"
#include "storage/ddl/ob_ddl_inc_redo_log_writer.h"
#include "storage/ddl/ob_ddl_independent_dag.h"

namespace oceanbase
{
namespace storage
{
// TODO@xijin: better use the same function with the cg macro block writer
int ObWriterArgs::init(const ObWriteMacroParam &param,
                       const ObWriterType writer_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("the ObOpenWriterArgs is been initialized", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid() || !is_valid_type(writer_type) )) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid argument", K(ret), K(param), K(writer_type));
  } else {
    const ObWriteTabletParam &tablet_param = param.tablet_param_;
    const bool with_cs_replica = tablet_param.with_cs_replica_;
    ObStorageSchema *storage_schema = with_cs_replica ?
                                      tablet_param.cs_replica_storage_schema_ :
                                      tablet_param.storage_schema_;
    const bool is_inc_major = is_incremental_major_direct_load(param.direct_load_type_);
    if (OB_UNLIKELY(nullptr == storage_schema || storage_schema->is_row_store())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected storage schema", K(ret), K(storage_schema));
    } else {
      const int64_t cg_count = storage_schema->get_column_group_count();
      if (OB_UNLIKELY(param.cg_idx_ < 0 || param.cg_idx_ >= cg_count)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the cg idx is invalid", K(ret), K(param.cg_idx_), K(cg_count));
      } else {
        const bool is_inc_major = is_incremental_major_direct_load(param.direct_load_type_);
        const bool is_inc_minor = is_incremental_minor_direct_load(param.direct_load_type_);
        const ObLSID ls_id = param.ls_id_;
        const uint64_t tenant_data_version = param.tenant_data_version_;
        const bool need_submit_io = !with_cs_replica;
        const ObStorageColumnGroupSchema &cg_schema = storage_schema->get_column_groups().at(param.cg_idx_);
        ObITable::TableKey cg_table_key;
        compaction::ObExecMode exec_mode = GCTX.is_shared_storage_mode() && !is_inc_minor
                                           ? compaction::ObExecMode::EXEC_MODE_OUTPUT
                                           : compaction::ObExecMode::EXEC_MODE_LOCAL;
        ObSSTableIndexBuilder::ObSpaceOptimizationMode space_opt_mode = GCTX.is_shared_storage_mode() ?
                                                                        ObSSTableIndexBuilder::DISABLE :
                                                                        ObSSTableIndexBuilder::ENABLE;
        ObMacroMetaTempStore *macro_meta_store = nullptr;

        cg_table_key.tablet_id_ = param.tablet_id_;
        cg_table_key.version_range_.snapshot_version_ = param.snapshot_version_;
        cg_table_key.column_group_idx_ = param.cg_idx_;
        if (cg_schema.is_rowkey_column_group() || cg_schema.is_all_column_group()) {
          cg_table_key.table_type_ = is_inc_major ? ObITable::INC_COLUMN_ORIENTED_SSTABLE
                                                  : ObITable::COLUMN_ORIENTED_SSTABLE;
          cg_table_key.slice_range_.start_slice_idx_ = param.slice_idx_;
          cg_table_key.slice_range_.end_slice_idx_ = param.slice_idx_;
        } else {
          cg_table_key.table_type_ = is_inc_major ? ObITable::INC_NORMAL_COLUMN_GROUP_SSTABLE
                                                  : ObITable::NORMAL_COLUMN_GROUP_SSTABLE;
          cg_table_key.slice_range_.start_slice_idx_ = param.slice_idx_;
          cg_table_key.slice_range_.end_slice_idx_ = param.slice_idx_;
        }
        if (is_need_difference_start_seqence(writer_type)) {
          parallel_idx_ = param.slice_idx_;
          macro_seq_param_.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
          macro_seq_param_.start_ = param.start_sequence_.macro_data_seq_;
        } else {
          ObMacroDataSeq start_sequence;
          if (OB_FAIL(ObDDLUtil::init_macro_block_seq(param.direct_load_type_,
                                                      param.tablet_id_,
                                                      param.slice_idx_,
                                                      start_sequence))) {
            LOG_WARN("fail to initialize start sequence", K(ret), K(param.direct_load_type_),
                                                          K(param.tablet_id_),
                                                          K(param.slice_idx_));
          } else {
            parallel_idx_ = param.slice_idx_;
            macro_seq_param_.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
            macro_seq_param_.start_ = start_sequence.macro_data_seq_;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(data_desc_.init(true/*is ddl*/,
                                          *storage_schema,
                                          ls_id,
                                          cg_table_key.get_tablet_id(),
                                          compaction::ObMergeType::MAJOR_MERGE,
                                          cg_table_key.get_snapshot_version(),
                                          tenant_data_version,
                                          tablet_param.is_micro_index_clustered_,
                                          tablet_param.tablet_transfer_seq_,
                                          0/*concurrent_cnt*/,
                                          SCN::min_scn()/*reorganization_scn : only for ss*/,
                                          SCN::min_scn(),
                                          &cg_schema,
                                          param.cg_idx_,
                                          exec_mode,
                                          need_submit_io,
                                          is_inc_major))) {
          LOG_WARN("fail to initialize data store desc", K(ret), K(is_inc_major));
        } else if (OB_FAIL(index_builder_.init(data_desc_.get_desc(),
                                              space_opt_mode/*small sstable op*/))) {
          LOG_WARN("fail to initialize sstable index builder",
              K(ret), K(ls_id), K(cg_table_key), K(data_desc_), K(space_opt_mode));
        } else {
          // for build the tail index block in macro block
          data_desc_.get_desc().sstable_index_builder_ = &index_builder_;
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObSSTablePrivateObjectCleaner::get_cleaner_from_data_store_desc(data_desc_.get_desc(), object_cleaner_))) {
            LOG_WARN("fail to get cleaner from data store desc", K(ret), K(data_desc_.get_desc()));
          } else if (OB_ISNULL(object_cleaner_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("object cleaner is nullptr", KR(ret));
          }
#ifdef OB_BUILD_SHARED_STORAGE
          else if (GCTX.is_shared_storage_mode() && is_inc_minor && OB_FAIL(object_cleaner_->mark_succeed())) {
            LOG_WARN("fail to mark succeed", KR(ret), K(is_inc_minor));
          }
#endif
        }

      #ifdef OB_BUILD_SHARED_STORAGE
        if (OB_SUCC(ret) && GCTX.is_shared_storage_mode()) {
          ObMacroMetaStoreManager *macro_meta_store_mgr = param.macro_meta_store_mgr_;
          data_desc_.get_static_desc().schema_version_ = param.schema_version_;
          if (is_need_ddl_redo_callback_type(writer_type)) {
            if (OB_ISNULL(macro_meta_store_mgr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("macro meta store manager in shared storage mode is null", K(ret));
            } else if (OB_FAIL(macro_meta_store_mgr->get_macro_meta_store(cg_table_key.tablet_id_,
                                                                          param.cg_idx_,
                                                                          parallel_idx_,
                                                                          macro_meta_store))) {
              if (OB_ENTRY_NOT_EXIST == ret) {
                ret = OB_SUCCESS;
                if (nullptr != macro_meta_store) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("macro meta store is not null", K(ret));
                } else if (OB_FAIL(macro_meta_store_mgr->add_macro_meta_store(cg_table_key.tablet_id_,
                                                                              param.cg_idx_,
                                                                              parallel_idx_,
                                                                              0/*lob_start_seq*/,
                                                                              macro_meta_store))) {
                  LOG_WARN("fail to add macro meta store", K(ret), K(param.cg_idx_), K(parallel_idx_));
                }
              } else {
                LOG_WARN("fail to get macro meta store",
                    K(ret), K(cg_table_key.tablet_id_), K(param.cg_idx_), K(parallel_idx_));
              }
            }
          }
        }
      #endif

        if (OB_SUCC(ret) && is_need_ddl_redo_callback_type(writer_type)) {
          const int64_t row_offset = param.row_offset_;
          SCN start_scn;
          const ObDDLMacroBlockType block_type = !is_inc_minor && param.is_no_logging_ ? DDL_MB_SS_EMPTY_DATA_TYPE : DDL_MB_DATA_TYPE;
          IGNORE_RETURN start_scn.convert_for_tx(SS_DDL_START_SCN_VAL);
          ObDDLRedoLogWriterCallbackInitParam init_param;
          init_param.ls_id_ = ls_id;
          init_param.tablet_id_ = cg_table_key.tablet_id_;
          init_param.direct_load_type_ = param.direct_load_type_;
          init_param.block_type_ = block_type;
          init_param.table_key_ = cg_table_key;
          init_param.start_scn_ = start_scn;
          init_param.task_id_ = param.task_id_;
          init_param.data_format_version_ = tenant_data_version;
          init_param.parallel_cnt_ = param.get_logic_parallel_count();
          init_param.cg_cnt_ = storage_schema->get_column_group_count();
          init_param.row_id_offset_ = row_offset;
          init_param.need_delay_ = false;
          init_param.with_cs_replica_ = with_cs_replica;
          init_param.need_submit_io_ = need_submit_io;
          init_param.macro_meta_store_ = macro_meta_store;
          init_param.is_inc_major_log_ = param.ddl_dag_ ? param.ddl_dag_->is_inc_major_log() : false ;
          ObDDLWriteStat *ddl_write_stat = nullptr;
          if (OB_FAIL(ObDDLUtil::get_ddl_write_stat(param, cg_table_key, ddl_write_stat))) {
            LOG_WARN("get ddl write stat failed", K(ret), K(cg_table_key), K(param), KPC(ddl_write_stat));
          } else {
            init_param.write_stat_ = ddl_write_stat;
          }

          if (OB_FAIL(ret)) {
          } else if (is_inc_major) {
            init_param.tx_desc_ = param.tx_info_.tx_desc_;
            init_param.trans_id_ = param.tx_info_.trans_id_;
            init_param.seq_no_ = transaction::ObTxSEQ::cast_from_int(param.tx_info_.seq_no_);
            if (OB_ISNULL(ddl_redo_callback_ = OB_NEW(ObDDLIncRedoLogWriterCallback, ObMemAttr(MTL_ID(), "DDL_MBSS")))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to alloc memory", K(ret));
            } else if (OB_FAIL(static_cast<ObDDLIncRedoLogWriterCallback *>(ddl_redo_callback_)->init(
                init_param))) {
              LOG_WARN("fail to init inc ddl_redo_callback_", KR(ret), K(init_param));
            }
          } else {
            if (OB_ISNULL(ddl_redo_callback_ = OB_NEW(ObDDLRedoLogWriterCallback, ObMemAttr(MTL_ID(), "DDL_MBSS")))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to alloc memory", K(ret));
            } else if (OB_FAIL(static_cast<ObDDLRedoLogWriterCallback *>(ddl_redo_callback_)->init(init_param))) {
              LOG_WARN("fail to init full ddl_redo_callback_", KR(ret), K(init_param));
            }
          }
        }
        if (OB_SUCC(ret)) {
          is_inited_ = true;
        } else if (OB_NOT_NULL(ddl_redo_callback_)) {
          common::ob_delete(ddl_redo_callback_);
          ddl_redo_callback_ = nullptr;
        }
      }
    }
  }
  return ret;
}

void ObWriterArgs::reset()
{
  is_inited_ = false;
  parallel_idx_ = -1;
  data_desc_.reset();
  index_builder_.reset();
  macro_seq_param_.reset();
  pre_warm_param_.reset();
  object_cleaner_ = nullptr;
  if (OB_NOT_NULL(ddl_redo_callback_)) {
    common::ob_delete(ddl_redo_callback_);
    ddl_redo_callback_ = nullptr;
  }
}

}
}
