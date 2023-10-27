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

#include "storage/ddl/ob_ddl_struct.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "share/scn.h"
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
#include "storage/blocksstable/ob_datum_rowkey.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"

using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::clog;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;



ObBlockMetaTree::ObBlockMetaTree()
  : is_inited_(false), arena_(), tree_allocator_(arena_), block_tree_(tree_allocator_)
{

}

ObBlockMetaTree::~ObBlockMetaTree()
{
  destroy();
}

int ObBlockMetaTree::init(ObTablet &tablet,
                          const ObITable::TableKey &table_key,
                          const share::SCN &ddl_start_scn,
                          const int64_t data_format_version)
{
  int ret = OB_SUCCESS;
  const ObMemAttr mem_attr(MTL_ID(), "BlockMetaTree");
  ObTableStoreIterator ddl_table_iter;
  ObITable *first_ddl_sstable = nullptr; // get compressor_type of macro block for query
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!table_key.is_valid() || data_format_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_key));
  } else if (FALSE_IT(arena_.set_attr(mem_attr))) {
  } else if (OB_FAIL(block_tree_.init())) {
    LOG_WARN("init block tree failed", K(ret));
  } else if (OB_FAIL(tablet.get_ddl_sstables(ddl_table_iter))) {
    LOG_WARN("get ddl sstable handles failed", K(ret));
  } else if (ddl_table_iter.count() > 0 && OB_FAIL(ddl_table_iter.get_boundary_table(false/*is_last*/, first_ddl_sstable))) {
    LOG_WARN("failed to get boundary table", K(ret));
  } else if (OB_FAIL(ObTabletDDLUtil::prepare_index_data_desc(tablet,
                                                              table_key.get_snapshot_version(),
                                                              data_format_version,
                                                              static_cast<ObSSTable *>(first_ddl_sstable),
                                                              data_desc_))) {
      LOG_WARN("prepare data store desc failed", K(ret), K(table_key), K(data_format_version));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObDDLKV::init_sstable_param(ObTablet &tablet,
                                const ObITable::TableKey &table_key,
                                const share::SCN &ddl_start_scn,
                                ObTabletCreateSSTableParam &sstable_param)
{
  int ret = OB_SUCCESS;
  const ObStorageSchema *storage_schema_ptr = nullptr;
  ObArenaAllocator allocator;
  if (OB_UNLIKELY(!table_key.is_valid() || !ddl_start_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_key), K(ddl_start_scn));
  } else if (OB_FAIL(tablet.load_storage_schema(allocator, storage_schema_ptr))) {
    LOG_WARN("load storage schema fail", K(ret), K(table_key));
  } else {
    int64_t column_count = 0;
    const ObStorageSchema &storage_schema = *storage_schema_ptr;
    const int64_t root_block_size = sizeof(ObBlockMetaTree);
    const ObDataStoreDesc &data_desc = block_meta_tree_.get_data_desc();
    if (OB_FAIL(storage_schema.get_stored_column_count_in_sstable(column_count))) {
      LOG_WARN("fail to get stored column count in sstable", K(ret));
    } else {
      sstable_param.table_key_ = table_key;
      sstable_param.table_key_.table_type_ = ObITable::DDL_MEM_SSTABLE;
      sstable_param.is_ready_for_read_ = true;
      sstable_param.table_mode_ = storage_schema.get_table_mode_struct();
      sstable_param.index_type_ = storage_schema.get_index_type();
      sstable_param.rowkey_column_cnt_ = storage_schema.get_rowkey_column_num() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
      sstable_param.schema_version_ = storage_schema.get_schema_version();
      sstable_param.latest_row_store_type_ = storage_schema.get_row_store_type();
      sstable_param.create_snapshot_version_ = table_key.get_snapshot_version();
      sstable_param.max_merged_trans_version_ = table_key.get_snapshot_version();
      sstable_param.ddl_scn_ = ddl_start_scn;
      sstable_param.root_row_store_type_ = data_desc.row_store_type_;
      sstable_param.data_index_tree_height_ = 2; // fixed tree height, because there is only one root block
      sstable_param.column_cnt_ = column_count;
      sstable_param.contain_uncommitted_row_ = false; // ddl build major sstable with committed rows only
      sstable_param.compressor_type_ = data_desc.compressor_type_;
      sstable_param.encrypt_id_ = data_desc.encrypt_id_;
      sstable_param.master_key_id_ = data_desc.master_key_id_;
      MEMCPY(sstable_param.encrypt_key_, data_desc.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
      sstable_param.use_old_macro_block_count_ = 0; // all new, no reuse
      sstable_param.index_blocks_cnt_ = 0; // index macro block count, the index is in memory, so be 0.
      sstable_param.other_block_ids_.reset(); // other blocks contains only index macro blocks now, so empty.
    }

    if (OB_SUCC(ret)) {
      // set root block for data tree
      if (OB_FAIL(sstable_param.root_block_addr_.set_mem_addr(0/*offset*/, root_block_size/*size*/))) {
        LOG_WARN("set root block address for data tree failed", K(ret));
      } else {
        sstable_param.root_block_data_.type_ = ObMicroBlockData::DDL_BLOCK_TREE;
        sstable_param.root_block_data_.buf_ = reinterpret_cast<char *>(&block_meta_tree_);
        sstable_param.root_block_data_.size_ = root_block_size;
      }
    }

    if (OB_SUCC(ret)) {
      // set root block for secondary meta tree
      if (OB_FAIL(sstable_param.data_block_macro_meta_addr_.set_mem_addr(0/*offset*/, root_block_size/*size*/))) {
        LOG_WARN("set root block address for secondary meta tree failed", K(ret));
      } else {
        sstable_param.data_block_macro_meta_.type_ = ObMicroBlockData::DDL_BLOCK_TREE;
        sstable_param.data_block_macro_meta_.buf_ = reinterpret_cast<char *>(&block_meta_tree_);
        sstable_param.data_block_macro_meta_.size_ = root_block_size;
      }
    }
  }
  ObTablet::free_storage_schema(allocator, storage_schema_ptr);
  return ret;
}

void ObBlockMetaTree::destroy()
{
  is_inited_ = false;
  macro_blocks_.reset();
  block_tree_.destroy(false /*is_batch_destroy*/);
  data_desc_.reset();
  for (int64_t i = 0; i < sorted_rowkeys_.count(); ++i) {
    const ObDataMacroBlockMeta *cur_meta = sorted_rowkeys_.at(i).block_meta_;
    if (OB_NOT_NULL(cur_meta)) {
      cur_meta->~ObDataMacroBlockMeta();
    }
  }
  sorted_rowkeys_.reset();
  tree_allocator_.reset();
  arena_.reset();
}

int ObBlockMetaTree::insert_macro_block(const ObDDLMacroHandle &macro_handle,
                                        const blocksstable::ObDatumRowkey *rowkey,
                                        const blocksstable::ObDataMacroBlockMeta *meta)
{
  int ret = OB_SUCCESS;
  ObDataMacroBlockMeta *insert_meta = const_cast<ObDataMacroBlockMeta *>(meta);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!macro_handle.is_valid() || nullptr == rowkey || nullptr == meta)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_handle), K(rowkey), KP(meta));
  } else if (OB_FAIL(macro_blocks_.push_back(macro_handle))) {
    LOG_WARN("push back macro handle failed", K(ret), K(macro_handle));
  } else if (OB_FAIL(block_tree_.insert(ObDatumRowkeyWrapper(rowkey, &data_desc_.datum_utils_), insert_meta))) {
    LOG_WARN("insert block tree failed", K(ret), K(rowkey), KPC(meta));
  }
  return ret;
}

// TODO@wenqu: direct use btree iterator
int ObBlockMetaTree::build_sorted_rowkeys()
{
  int ret = OB_SUCCESS;
  const int64_t version = INT64_MAX;
  BtreeIterator iter;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (sorted_rowkeys_.count() > 0) {
    // already sorted, do nothing
  } else if (OB_FAIL(block_tree_.set_key_range(iter,
                                               ObDatumRowkeyWrapper(&ObDatumRowkey::MIN_ROWKEY, &data_desc_.datum_utils_),
                                               false,
                                               ObDatumRowkeyWrapper(&ObDatumRowkey::MAX_ROWKEY, &data_desc_.datum_utils_),
                                               false,
                                               version))) {
    LOG_WARN("locate range failed", K(ret));
  } else if (OB_FAIL(sorted_rowkeys_.reserve(get_macro_block_cnt()))) {
    LOG_WARN("reserve sorted rowkeys failed", K(ret), K(get_macro_block_cnt()));
  } else {
    while (OB_SUCC(ret)) {
      ObDatumRowkeyWrapper rowkey_wrapper;
      ObDataMacroBlockMeta *block_meta = nullptr;
      if (OB_FAIL(iter.get_next(rowkey_wrapper, block_meta))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next failed", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(block_meta)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("block_meta is null", K(ret), KP(block_meta));
      } else if (((uint64_t)(block_meta) & 7ULL) != 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid btree value", K(ret), KP(block_meta));
      } else {
        IndexItem cur_item(rowkey_wrapper.rowkey_, block_meta);
        cur_item.header_.version_ = ObIndexBlockRowHeader::INDEX_BLOCK_HEADER_V1;
        cur_item.header_.row_store_type_ = static_cast<uint8_t>(data_desc_.row_store_type_);
        cur_item.header_.compressor_type_ = static_cast<uint8_t>(data_desc_.compressor_type_);
        cur_item.header_.is_data_index_ = true;
        cur_item.header_.is_data_block_ = false;
        cur_item.header_.is_leaf_block_ = true;
        cur_item.header_.is_macro_node_ = true;
        cur_item.header_.is_major_node_ = true;
        cur_item.header_.is_deleted_ = block_meta->val_.is_deleted_;
        cur_item.header_.contain_uncommitted_row_ = block_meta->val_.contain_uncommitted_row_;
        cur_item.header_.macro_id_ = block_meta->val_.macro_id_;
        cur_item.header_.block_offset_ = block_meta->val_.block_offset_;
        cur_item.header_.block_size_ = block_meta->val_.block_size_;
        cur_item.header_.macro_block_count_ = 1;
        cur_item.header_.micro_block_count_ = block_meta->val_.micro_block_count_;
        cur_item.header_.master_key_id_ = data_desc_.master_key_id_;
        cur_item.header_.encrypt_id_ = data_desc_.encrypt_id_;
        MEMCPY(cur_item.header_.encrypt_key_, data_desc_.encrypt_key_, sizeof(cur_item.header_.encrypt_key_));
        cur_item.header_.schema_version_ = data_desc_.schema_version_;
        cur_item.header_.row_count_ = block_meta->val_.row_count_;
        if (OB_UNLIKELY(!cur_item.header_.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Built an invalid index block row", K(ret), K(cur_item));
        } else if (OB_FAIL(sorted_rowkeys_.push_back(cur_item))) {
          LOG_WARN("push back index item failed", K(ret), K(rowkey_wrapper), KPC(block_meta));
        }
      }
    }
  }
  return ret;
}

int ObBlockMetaTree::get_sorted_meta_array(ObIArray<const ObDataMacroBlockMeta *> &meta_array) const
{
  int ret = OB_SUCCESS;
  meta_array.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (sorted_rowkeys_.count() != macro_blocks_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sorted array not ready", K(ret), K(sorted_rowkeys_.count()), K(macro_blocks_.count()));
  } else if (OB_FAIL(meta_array.reserve(sorted_rowkeys_.count()))) {
    LOG_WARN("reserve meta array failed", K(ret), K(sorted_rowkeys_.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sorted_rowkeys_.count(); ++i) {
      if (OB_FAIL(meta_array.push_back(sorted_rowkeys_.at(i).block_meta_))) {
        LOG_WARN("push back block meta failed", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObBlockMetaTree::exist(const blocksstable::ObDatumRowkey *rowkey, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  ObDataMacroBlockMeta *dummp_meta = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(rowkey)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(rowkey));
  } else if (OB_FAIL(block_tree_.get(ObDatumRowkeyWrapper(rowkey, &data_desc_.datum_utils_), dummp_meta))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get value from block meta tree failed", K(ret), KPC(rowkey));
    } else {
      is_exist = false;
      ret = OB_SUCCESS;
    }
  } else {
    is_exist = true;
  }
  return ret;
}

bool ObBlockMetaTree::CompareFunctor::operator ()(const IndexItem &item,
                                                  const blocksstable::ObDatumRowkey &rowkey)
{
  int cmp_ret = 0;
  item.rowkey_->compare(rowkey, datum_utils_, cmp_ret);
  return cmp_ret < 0;
}

bool ObBlockMetaTree::CompareFunctor::operator ()(const blocksstable::ObDatumRowkey &rowkey,
                                                  const IndexItem &item)
{
  int cmp_ret = 0;
  item.rowkey_->compare(rowkey, datum_utils_, cmp_ret);
  return cmp_ret > 0;
}

int ObBlockMetaTree::locate_range(const blocksstable::ObDatumRange &range,
                                  const blocksstable::ObStorageDatumUtils &datum_utils,
                                  const bool is_left_border,
                                  const bool is_right_border,
                                  int64_t &begin_idx,
                                  int64_t &end_idx)
{
  int ret = OB_SUCCESS;
  begin_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  end_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (sorted_rowkeys_.empty()) {
    // do nothing
  } else {
    CompareFunctor cmp(datum_utils);
    if (!is_left_border || range.get_start_key().is_min_rowkey()) {
      begin_idx = 0;
    } else {
      if (range.is_left_closed()) {
        begin_idx = std::lower_bound(sorted_rowkeys_.begin(), sorted_rowkeys_.end(), range.get_start_key(), cmp) - sorted_rowkeys_.begin();
      } else {
        begin_idx = std::upper_bound(sorted_rowkeys_.begin(), sorted_rowkeys_.end(), range.get_start_key(), cmp) - sorted_rowkeys_.begin();
      }
      if (sorted_rowkeys_.count() == begin_idx) {
        ret = OB_BEYOND_THE_RANGE;
      }
    }
    if (OB_SUCC(ret)) {
      if (!is_right_border || range.get_end_key().is_max_rowkey()) {
        end_idx = sorted_rowkeys_.count() - 1;
      } else {
        end_idx = std::lower_bound(sorted_rowkeys_.begin(), sorted_rowkeys_.end(), range.get_end_key(), cmp) - sorted_rowkeys_.begin();
        if (sorted_rowkeys_.count() == end_idx) {
          end_idx = sorted_rowkeys_.count() - 1;
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    begin_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
    end_idx = ObIMicroBlockReaderInfo::INVALID_ROW_INDEX;
  }
  return ret;
}

int ObBlockMetaTree::get_index_block_row_header(const int64_t idx,
                                                const ObIndexBlockRowHeader *&idx_header,
                                                const blocksstable::ObDatumRowkey *&endkey)
{
  int ret = OB_SUCCESS;
  idx_header = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(idx < 0 || idx >= sorted_rowkeys_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(idx), K(sorted_rowkeys_.count()));
  } else {
    IndexItem &cur_item = sorted_rowkeys_.at(idx);
    endkey = &cur_item.block_meta_->end_key_;
    idx_header = &cur_item.header_;
  }
  return ret;
}

int ObBlockMetaTree::get_macro_block_meta(const int64_t idx,
                                         ObDataMacroBlockMeta &macro_meta)
{
  int ret = OB_SUCCESS;
  macro_meta.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(idx < 0 || idx >= sorted_rowkeys_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    const ObDataMacroBlockMeta &found_meta = *sorted_rowkeys_.at(idx).block_meta_;
    if (OB_FAIL(macro_meta.assign(found_meta))) {
      LOG_WARN("assign macro meta failed", K(ret), K(found_meta));
    }
  }
  return ret;
}

int ObBlockMetaTree::get_last_rowkey(const ObDatumRowkey *&last_rowkey)
{
  int ret = OB_SUCCESS;
  if (sorted_rowkeys_.count() > 0) {
    last_rowkey = sorted_rowkeys_.at(sorted_rowkeys_.count() - 1).rowkey_;
  } else {
    last_rowkey = &ObDatumRowkey::MAX_ROWKEY;
  }
  return ret;
}


ObDDLKV::ObDDLKV()
  : is_inited_(false), ls_id_(), tablet_id_(), ddl_start_scn_(SCN::min_scn()), snapshot_version_(0),
    lock_(), arena_allocator_("DDL_KV"), is_freezed_(false), is_closed_(false), last_freezed_scn_(SCN::min_scn()),
    min_scn_(SCN::max_scn()), max_scn_(SCN::min_scn()), freeze_scn_(SCN::max_scn()), pending_cnt_(0), data_format_version_(0)
{
}

ObDDLKV::~ObDDLKV()
{
  reset();
}

void ObDDLKV::inc_ref()
{
  ATOMIC_AAF(&ref_cnt_, 1);
  // FLOG_INFO("DDLKV inc_ref", K(ref_cnt_), KP(this), K(tablet_id_));
}

int64_t ObDDLKV::dec_ref()
{
  // FLOG_INFO("DDLKV dec_ref", K(ref_cnt_), KP(this), K(tablet_id_));
  return ATOMIC_SAF(&ref_cnt_, 1 /* just sub 1 */);
}

int ObDDLKV::init(ObTablet &tablet,
                  const SCN &ddl_start_scn,
                  const int64_t snapshot_version,
                  const SCN &last_freezed_scn,
                  const int64_t data_format_version)

{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDDLKV has been inited twice", K(ret), KP(this));
  } else if (OB_UNLIKELY(!ls_id.is_valid()
        || !tablet_id.is_valid()
        || !ddl_start_scn.is_valid_and_not_min()
        || snapshot_version <= 0
        || !last_freezed_scn.is_valid_and_not_min()
        || data_format_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(ddl_start_scn), K(snapshot_version), K(last_freezed_scn), K(data_format_version));
  } else {
    ObTabletDDLParam ddl_param;
    ddl_param.tenant_id_ = MTL_ID();
    ddl_param.ls_id_ = ls_id;
    ddl_param.table_key_.tablet_id_ = tablet_id;
    ddl_param.table_key_.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
    ddl_param.table_key_.version_range_.base_version_ = 0;
    ddl_param.table_key_.version_range_.snapshot_version_ = snapshot_version;
    ddl_param.start_scn_ = ddl_start_scn;
    ddl_param.snapshot_version_ = snapshot_version;
    ddl_param.data_format_version_ = data_format_version;
    ObTabletCreateSSTableParam sstable_param;
    if (OB_FAIL(block_meta_tree_.init(tablet, ddl_param.table_key_, ddl_start_scn, data_format_version))) {
      LOG_WARN("init mem index sstable failed", K(ret), K(ddl_param));
    } else if (OB_FAIL(init_sstable_param(tablet, ddl_param.table_key_, ddl_start_scn, sstable_param))) {
      LOG_WARN("init sstable param failed", K(ret));
    } else if (OB_FAIL(ObSSTable::init(sstable_param, &arena_allocator_))) {
      LOG_WARN("init sstable failed", K(ret));
    } else {
      ls_id_ = ls_id;
      tablet_id_ = tablet_id;
      ddl_start_scn_ = ddl_start_scn;
      snapshot_version_ = snapshot_version;
      last_freezed_scn_ = last_freezed_scn;
      data_format_version_ = data_format_version;
      is_inited_ = true;
      LOG_INFO("ddl kv init success", K(ls_id_), K(tablet_id_), K(ddl_start_scn_), K(snapshot_version_), K(last_freezed_scn_), K(data_format_version_), KP(this));
    }
  }
  return ret;
}

void ObDDLKV::reset()
{
  FLOG_INFO("ddl kv reset", KP(this), K(*this));
  is_inited_ = false;
  ObSSTable::reset();
  ls_id_.reset();
  tablet_id_.reset();
  ddl_start_scn_ = SCN::min_scn();
  snapshot_version_ = 0;
  is_freezed_ = false;
  is_closed_ = false;
  last_freezed_scn_ = SCN::min_scn();
  min_scn_ = SCN::max_scn();
  max_scn_ = SCN::min_scn();
  freeze_scn_ = SCN::max_scn();
  pending_cnt_ = 0;
  data_format_version_ = 0;
  block_meta_tree_.destroy();
  arena_allocator_.reset();
}

int ObDDLKV::set_macro_block(ObTablet &tablet, const ObDDLMacroBlock &macro_block)
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
  if (OB_SUCC(ret) && get_macro_block_cnt() >= freeze_block_count) {
    ObDDLKvMgrHandle ddl_kv_mgr_handle;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(tablet.get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
      LOG_WARN("failed to get ddl kv mgr", K(ret));
    } else if (OB_TMP_FAIL(ddl_kv_mgr_handle.get_obj()->schedule_ddl_dump_task(tablet, ddl_start_scn_, SCN::min_scn()))) {
      LOG_WARN("try schedule ddl merge dag failed when ddl kv is full ",
          K(tmp_ret), K(ls_id_), K(tablet_id_), K(get_macro_block_cnt()));
    }
  }
  if (OB_SUCC(ret)) {
    ObDataMacroBlockMeta *data_macro_meta = nullptr;
    TCWLockGuard guard(lock_);
    if (macro_block.ddl_start_scn_ != ddl_start_scn_) {
      if (macro_block.ddl_start_scn_ > ddl_start_scn_) {
        ret = OB_EAGAIN;
        LOG_INFO("ddl start scn too large, retry", K(ret),
            K(ls_id_), K(tablet_id_), K(ddl_start_scn_), K(macro_block));
      } else {
        // filter out and do nothing
        LOG_INFO("ddl start scn too small, maybe from old build task, ignore", K(ret),
            K(ls_id_), K(tablet_id_), K(ddl_start_scn_), K(macro_block));
      }
    } else if (macro_block.scn_ > freeze_scn_) {
      ret = OB_EAGAIN;
      LOG_INFO("this ddl kv is freezed, retry other ddl kv", K(ret), K(ls_id_), K(tablet_id_), K(macro_block), K(freeze_scn_));
    } else if (OB_FAIL(ObIndexBlockRebuilder::get_macro_meta(macro_block.buf_, macro_block.size_, macro_block.get_block_id(), arena_allocator_, data_macro_meta))) {
      LOG_WARN("get macro meta failed", K(ret), K(macro_block));
    } else if (OB_FAIL(insert_block_meta_tree(macro_block.block_handle_, data_macro_meta))) {
      LOG_WARN("insert macro block failed", K(ret), K(macro_block), KPC(data_macro_meta));
    } else {
      min_scn_ = SCN::min(min_scn_, macro_block.scn_);
      max_scn_ = SCN::max(max_scn_, macro_block.scn_);
      LOG_INFO("succeed to set macro block into ddl kv", K(macro_block), KPC(data_macro_meta));
    }
  }
  return ret;
}

int ObDDLKV::insert_block_meta_tree(const ObDDLMacroHandle &macro_handle, blocksstable::ObDataMacroBlockMeta *data_macro_meta)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(block_meta_tree_.insert_macro_block(macro_handle, &data_macro_meta->end_key_, data_macro_meta))) {
    LOG_WARN("insert macro block failed", K(ret), K(macro_handle), KPC(data_macro_meta));
  } else {
    const ObDataBlockMetaVal &meta_val = data_macro_meta->get_meta_val();
    meta_->get_basic_meta().data_macro_block_count_ += 1;
    data_macro_block_count_ =  meta_->get_basic_meta().data_macro_block_count_;
    meta_->get_basic_meta().data_micro_block_count_ += meta_val.micro_block_count_;
    meta_->get_basic_meta().max_merged_trans_version_ = max(meta_->get_basic_meta().max_merged_trans_version_, meta_val.max_merged_trans_version_);
    max_merged_trans_version_ = meta_->get_basic_meta().max_merged_trans_version_ ;
    meta_->get_basic_meta().row_count_ += meta_val.row_count_;
    meta_->get_basic_meta().data_checksum_ = ob_crc64_sse42(meta_->get_basic_meta().data_checksum_, &meta_val.data_checksum_, sizeof(meta_val.data_checksum_));
    meta_->get_basic_meta().occupy_size_ += meta_val.occupy_size_;
    meta_->get_basic_meta().original_size_ += meta_val.original_size_;
  }
  return ret;
}

int ObDDLKV::freeze(const SCN &freeze_scn)
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
      if (freeze_scn.is_valid_and_not_min()) {
        freeze_scn_ = freeze_scn;
      } else if (max_scn_.is_valid_and_not_min()) {
        freeze_scn_ = max_scn_;
      } else {
        ret = OB_EAGAIN;
        LOG_INFO("ddl kv not freezed, try again", K(ret), K(ls_id_), K(tablet_id_), K(get_macro_block_cnt()));
      }
      if (OB_SUCC(ret)) {
        ATOMIC_SET(&is_freezed_, true);
        LOG_INFO("ddl kv freezed", K(ret), K(ls_id_), K(tablet_id_), K(get_macro_block_cnt()));
      }
    }
  }
  return ret;
}

int ObDDLKV::prepare_sstable(const bool need_check/*=true*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl kv is not init", K(ret));
  } else if (!is_freezed()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("ddl kv not freezed", K(ret), K(*this));
  } else if (need_check && OB_FAIL(wait_pending())) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("wait pending failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    TCWLockGuard guard(lock_);
    if (OB_FAIL(block_meta_tree_.build_sorted_rowkeys())) {
      LOG_WARN("build sorted keys failed", K(ret), K(block_meta_tree_));
    } else {
      key_.scn_range_.start_scn_ = last_freezed_scn_;
      key_.scn_range_.end_scn_ = freeze_scn_;
    }
  }
  return ret;
}

int ObDDLKV::close(ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  ObArray<const ObDataMacroBlockMeta *> meta_array;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ddl kv is not init", K(ret));
  } else if (is_closed_) {
    // do nothing
    LOG_INFO("ddl kv already closed", K(*this));
  } else if (OB_FAIL(prepare_sstable())) {
    LOG_WARN("prepare sstable failed", K(ret));
  } else if (OB_FAIL(block_meta_tree_.get_sorted_meta_array(meta_array))) {
    LOG_WARN("get sorted meta array failed", K(ret));
  } else {
    ObArenaAllocator allocator("DDLUpTabStore");
    ObSSTable sstable;
    ObTabletDDLParam ddl_param;
    ddl_param.tenant_id_ = MTL_ID();
    ddl_param.ls_id_ = ls_id_;
    ddl_param.table_key_.tablet_id_ = tablet_id_;
    ddl_param.table_key_.table_type_ = ObITable::TableType::DDL_DUMP_SSTABLE;
    ddl_param.table_key_.scn_range_.start_scn_ = last_freezed_scn_;
    ddl_param.table_key_.scn_range_.end_scn_ = freeze_scn_;
    ddl_param.start_scn_ = ddl_start_scn_;
    ddl_param.snapshot_version_ = snapshot_version_;
    ddl_param.data_format_version_ = data_format_version_;
    ObSSTable *first_sstable = nullptr;
    if (OB_FAIL(ObTabletDDLUtil::try_get_first_ddl_sstable(tablet, first_sstable))) {
      LOG_WARN("fail to get first sstable", K(ret), K(tablet));
    } else if (OB_FAIL(ObTabletDDLUtil::create_ddl_sstable(tablet, ddl_param, meta_array, first_sstable, allocator, sstable))) {
      LOG_WARN("create ddl sstable failed", K(ret), K(ddl_param), KP(first_sstable));
    } else if (OB_FAIL(ObTabletDDLUtil::update_ddl_table_store(tablet, ddl_param, allocator, sstable))) {
      LOG_WARN("update ddl table store failed", K(ret), K(ddl_param), K(sstable));
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
    SCN max_decided_scn;
    if (OB_FAIL(ls_handle.get_ls()->get_max_decided_scn(max_decided_scn))) {
      LOG_WARN("get max decided log ts failed", K(ret), K(ls_id_));
      if (OB_STATE_NOT_MATCH == ret) {
        ret = OB_NEED_RETRY;
      }
    } else {
      // max_decided_scn is the left border scn - 1
      // the min deciding(replay or apply) scn (aka left border) is max_decided_scn + 1
      const bool pending_finished = SCN::plus(max_decided_scn, 1) >= freeze_scn_ && !is_pending();
      if (!pending_finished) {
        ret = OB_EAGAIN;
        //if (REACH_TIME_INTERVAL(1000L * 1000L)) {
          LOG_INFO("wait pending not finish", K(ret), K(*this), K(max_decided_scn));
        //}
      }
    }
  }
  return ret;
}
