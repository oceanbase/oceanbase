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

#include "ob_all_virtual_tablet_sstable_macro_info.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_mds_schema_helper.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace storage;
using namespace blocksstable;
using namespace common;
using namespace share;
using namespace share::schema;
namespace observer
{

ObAllVirtualTabletSSTableMacroInfo::MacroInfo::MacroInfo()
  : data_seq_(0),
    macro_logic_version_(0),
    macro_block_index_(-1),
    micro_block_count_(0),
    data_checksum_(0),
    occupy_size_(0),
    original_size_(0),
    data_size_(0),
    data_zsize_(0),
    store_range_(),
    row_count_(0),
    compressor_type_(ObCompressorType::INVALID_COMPRESSOR),
    row_store_type_(ObRowStoreType::MAX_ROW_STORE)
{
}

ObAllVirtualTabletSSTableMacroInfo::MacroInfo::~MacroInfo()
{
  reset();
}

void ObAllVirtualTabletSSTableMacroInfo::MacroInfo::reset()
{
  data_seq_ = 0;
  macro_logic_version_ = 0;
  macro_block_index_ = -1;
  micro_block_count_ = 0;
  data_checksum_ = 0;
  occupy_size_ = 0;
  original_size_ = 0;
  data_size_ = 0;
  data_zsize_ = 0;
  store_range_.reset();
  row_count_ = 0;
  compressor_type_ = ObCompressorType::INVALID_COMPRESSOR;
  row_store_type_ = ObRowStoreType::MAX_ROW_STORE;
}

ObAllVirtualTabletSSTableMacroInfo::ObAllVirtualTabletSSTableMacroInfo()
  : ObVirtualTableScannerIterator(),
    addr_(),
    tablet_iter_(nullptr),
    tablet_allocator_("VTTable"),
    tablet_handle_(),
    cols_desc_(),
    ls_id_(share::ObLSID::INVALID_LS_ID),
    table_store_iter_(),
    curr_sstable_(nullptr),
    curr_sstable_meta_handle_(),
    macro_iter_(nullptr),
    other_blk_iter_(),
    iter_allocator_(),
    rowkey_allocator_(),
    curr_range_(),
    block_idx_(0),
    iter_buf_(nullptr),
    io_buf_(nullptr)
{
}

ObAllVirtualTabletSSTableMacroInfo::~ObAllVirtualTabletSSTableMacroInfo()
{
  reset();
}

void ObAllVirtualTabletSSTableMacroInfo::reset()
{
  omt::ObMultiTenantOperator::reset();
  addr_.reset();
  ls_id_ = share::ObLSID::INVALID_LS_ID;

  if (OB_NOT_NULL(iter_buf_)) {
    allocator_->free(iter_buf_);
    iter_buf_ = nullptr;
  }
  if (OB_NOT_NULL(io_buf_)) {
    allocator_->free(io_buf_);
    io_buf_ = nullptr;
  }
  memset(objs_, 0, sizeof(objs_));

  ObVirtualTableScannerIterator::reset();
}
int ObAllVirtualTabletSSTableMacroInfo::init(common::ObIAllocator *allocator, common::ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (start_to_read_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "cannot init twice", K(ret));
  } else if (OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(ret), KP(allocator));
  } else if (OB_ISNULL(iter_buf_ = allocator->alloc(sizeof(ObTenantTabletIterator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(WARN, "fail to alloc tablet iter buf", K(ret));
  } else if (OB_UNLIKELY(!addr.ip_to_string(ip_buf_, sizeof(ip_buf_)))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
  } else {
    allocator_ = allocator;
    addr_ = addr;
    start_to_read_ = true;
  }
  return ret;
}

int ObAllVirtualTabletSSTableMacroInfo::get_next_macro_info(MacroInfo &info)
{
  int ret = OB_SUCCESS;
  blocksstable::ObMacroBlockDesc macro_desc;
  blocksstable::ObDataMacroBlockMeta macro_meta;
  macro_desc.macro_meta_ = &macro_meta;
  while (OB_SUCC(ret)) {
    if (OB_ISNULL(macro_iter_) && !other_blk_iter_.is_valid() && OB_FAIL(get_next_sstable())) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next sstable", K(ret));
      }
    } else if (OB_ISNULL(curr_sstable_)) {
      clean_cur_sstable();
    } else if (other_blk_iter_.is_valid()) {
      blocksstable::MacroBlockId macro_id;
      if (OB_FAIL(other_blk_iter_.get_next_macro_id(macro_id))) {
        if (OB_ITER_END != ret) {
          SERVER_LOG(WARN, "fail to get next macro id", K(ret), K(other_blk_iter_));
        } else {
          other_blk_iter_.reset();
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(get_macro_info(macro_id, info))) {
        SERVER_LOG(WARN, "fail to get macro info", K(ret), "macro_id", macro_id);
      } else {
        break;
      }
    } else if (OB_NOT_NULL(macro_iter_) && OB_FAIL(macro_iter_->get_next_macro_block(macro_desc))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "get data next macro block failed", K(ret));
      } else {
        macro_iter_->~ObIMacroBlockIterator();
        macro_iter_ = nullptr;
        if (OB_FAIL(curr_sstable_meta_handle_.get_sstable_meta().get_macro_info().get_other_block_iter(
            other_blk_iter_))) {
          STORAGE_LOG(WARN, "fail get other block iterator", K(ret), KPC(curr_sstable_));
        }
      }
    } else if (OB_FAIL(get_macro_info(macro_desc, info))) {
      SERVER_LOG(WARN, "fail to get macro info", K(ret), K(macro_desc));
    } else {
      break;
    }
  }
  return ret;
}

int ObAllVirtualTabletSSTableMacroInfo::get_macro_info(
    const blocksstable::MacroBlockId &macro_id,
    MacroInfo &info)
{
  int ret = OB_SUCCESS;
  ObMacroBlockHandle macro_handle;
  ObMacroBlockReadInfo macro_read_info;
  macro_read_info.macro_block_id_ = macro_id;
  macro_read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
  macro_read_info.offset_ = 0;
  macro_read_info.size_ = OB_SERVER_BLOCK_MGR.get_macro_block_size();
  macro_read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
  macro_handle.reset();
  if (OB_ISNULL(io_buf_) && OB_ISNULL(io_buf_ =
      reinterpret_cast<char*>(allocator_->alloc(OB_SERVER_BLOCK_MGR.get_macro_block_size())))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    int64_t io_size = OB_SERVER_BLOCK_MGR.get_macro_block_size();
    STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret), K(io_size));
  } else {
    macro_read_info.buf_ = io_buf_;
    if (OB_UNLIKELY(!macro_id.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "invalid argument", K(ret), K(macro_id));
    } else if (OB_FAIL(ObBlockManager::read_block(macro_read_info, macro_handle))) {
      SERVER_LOG(WARN, "fail to read macro block", K(ret), K(macro_read_info));
    } else {
      ObMacroBlockCommonHeader common_header;
      ObSSTableMacroBlockHeader macro_header;
      const char *buf = macro_read_info.buf_;
      const int64_t size = macro_handle.get_data_size();
      int64_t pos = 0;
      if (OB_FAIL(common_header.deserialize(buf, size, pos))) {
        STORAGE_LOG(ERROR, "fail to deserialize common header", K(ret), KP(buf), K(size), K(pos));
      } else if (OB_FAIL(common_header.check_integrity())) {
        STORAGE_LOG(WARN, "invalid common header", K(ret), K(common_header));
      } else if (OB_FAIL(macro_header.deserialize(buf, size, pos))) {
        STORAGE_LOG(ERROR, "fail to deserialize macro header", K(ret), KP(buf), K(size), K(pos));
      } else if (OB_UNLIKELY(!macro_header.is_valid())) {
        ret = OB_INVALID_DATA;
        STORAGE_LOG(WARN, "invalid macro header", K(ret), K(macro_header));
      } else {
        info.data_seq_ = macro_header.fixed_header_.data_seq_;
        info.macro_logic_version_ = macro_header.fixed_header_.logical_version_;
        info.macro_block_index_ = macro_id.block_index();
        info.row_count_ = macro_header.fixed_header_.row_count_;
        info.original_size_ = macro_header.fixed_header_.occupy_size_;
        info.data_size_ = macro_header.fixed_header_.occupy_size_;
        info.data_zsize_ = macro_header.fixed_header_.occupy_size_;
        info.occupy_size_ = macro_header.fixed_header_.occupy_size_;
        info.micro_block_count_ = macro_header.fixed_header_.micro_block_count_;
        info.data_checksum_ = macro_header.fixed_header_.data_checksum_;
        info.compressor_type_ = macro_header.fixed_header_.compressor_type_;
        info.row_store_type_ = static_cast<ObRowStoreType>(macro_header.fixed_header_.row_store_type_);
      }
    }
  }
  return ret;
}

int ObAllVirtualTabletSSTableMacroInfo::get_macro_info(
    const blocksstable::ObMacroBlockDesc &macro_desc,
    MacroInfo &info)
{
  int ret = OB_SUCCESS;
  rowkey_allocator_.reuse();
  if (OB_UNLIKELY(!macro_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(ret), K(macro_desc));
  } else if (curr_sstable_->is_normal_cg_sstable()) {
    const storage::ObITableReadInfo *index_read_info = nullptr;
    if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(index_read_info))) {
      SERVER_LOG(WARN, "failed to get index read info from ObTenantCGReadInfoMgr", KR(ret));
    } else if (OB_FAIL(macro_desc.range_.to_store_range(index_read_info->get_columns_desc(),
                                                 rowkey_allocator_,
                                                 info.store_range_))) {
      SERVER_LOG(WARN, "fail to get store range", K(ret), K(macro_desc.range_));
    }
  } else if (curr_sstable_->is_mds_sstable()) {
    const storage::ObITableReadInfo *index_read_info = storage::ObMdsSchemaHelper::get_instance().get_rowkey_read_info();
    if (OB_FAIL(macro_desc.range_.to_store_range(index_read_info->get_columns_desc(),
                                                 rowkey_allocator_,
                                                 info.store_range_))) {
      SERVER_LOG(WARN, "fail to get store range", K(ret), K(macro_desc.range_));
    }
  } else if (OB_FAIL(macro_desc.range_.to_store_range(cols_desc_,
                                                      rowkey_allocator_,
                                                      info.store_range_))) {
    SERVER_LOG(WARN, "fail to get store range", K(ret), K(macro_desc.range_));
  }

  if (OB_SUCC(ret)) {
    ObDataMacroBlockMeta *macro_meta = macro_desc.macro_meta_;
    info.data_seq_ = macro_meta->get_logic_id().data_seq_.macro_data_seq_;
    info.macro_logic_version_ = macro_meta->get_logic_id().logic_version_;
    info.macro_block_index_ = macro_desc.macro_block_id_.block_index();
    info.row_count_ = macro_desc.row_count_;
    info.original_size_ = macro_meta->val_.original_size_;
    info.data_size_ = macro_meta->val_.data_size_;
    info.data_zsize_ = macro_meta->val_.data_zsize_;
    info.occupy_size_ = macro_meta->val_.occupy_size_;
    info.micro_block_count_ = macro_meta->val_.micro_block_count_;
    info.data_checksum_ = macro_meta->val_.data_checksum_;
    info.compressor_type_ = macro_meta->val_.compressor_type_;
    info.row_store_type_ = static_cast<ObRowStoreType>(macro_desc.row_store_type_);
    ObStoreRowkey &start_key = info.store_range_.get_start_key();
    ObStoreRowkey &end_key = info.store_range_.get_end_key();
    const int64_t extra_rowkey_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    if (start_key != ObStoreRowkey::MIN_STORE_ROWKEY
        && OB_FAIL(start_key.assign(start_key.get_obj_ptr(),
            start_key.get_obj_cnt() > extra_rowkey_cnt ? start_key.get_obj_cnt() - extra_rowkey_cnt : start_key.get_obj_cnt()))) {
      SERVER_LOG(WARN, "fail to set start key", K(ret), K(start_key));
    } else if (end_key != ObStoreRowkey::MAX_STORE_ROWKEY
        && OB_FAIL(end_key.assign(end_key.get_obj_ptr(),
            end_key.get_obj_cnt() > extra_rowkey_cnt ? end_key.get_obj_cnt() - extra_rowkey_cnt : end_key.get_obj_cnt()))) {
      SERVER_LOG(WARN, "fail to set end key", K(ret), K(end_key));
    }
  }
  return ret;
}

int ObAllVirtualTabletSSTableMacroInfo::set_key_ranges(const ObIArray<ObNewRange> &key_ranges)
{
  int ret = OB_SUCCESS;
  if (key_ranges.empty()) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid key_ranges", K(ret), K(key_ranges));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges.count(); ++i) {
      const ObNewRange &key_range = key_ranges.at(i);
      ObNewRange range = key_range;
      range.start_key_.set_length(range.start_key_.length() - 1);
      range.end_key_.set_length(range.end_key_.length() - 1);
      range.border_flag_.set_inclusive_start();
      range.border_flag_.set_inclusive_end();
      if (key_range.start_key_.ptr()[key_range.start_key_.length() -1].is_max_value()) {
        range.border_flag_.unset_inclusive_start();
      }
      if (key_range.end_key_.ptr()[key_range.end_key_.length() -1].is_min_value()) {
        range.border_flag_.unset_inclusive_end();
      }
      if (OB_FAIL(key_ranges_.push_back(range))) {
        SERVER_LOG(WARN, "push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObAllVirtualTabletSSTableMacroInfo::gen_row(
    const MacroInfo &macro_info,
    ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(curr_sstable_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected null curr sstable", K(ret));
  } else {
    const ObITable::TableKey &table_key = curr_sstable_->get_key();
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch(col_id) {
      case SVR_IP:
        //svr_ip
        cur_row_.cells_[i].set_varchar(ip_buf_);
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case SVR_PORT:
        //svr_port
        cur_row_.cells_[i].set_int(addr_.get_port());
        break;
      case TENANT_ID:
        //tenant_id
        cur_row_.cells_[i].set_int(MTL_ID());
        break;
      case LS_ID:
        //ls_id
        cur_row_.cells_[i].set_int(ls_id_);
        break;
      case TABLET_ID:
        //tablet_id
        cur_row_.cells_[i].set_int(table_key.tablet_id_.id());
        break;
      case MACRO_IDX_IN_SSTABLE:
        //macro_idx_in_sstable
        cur_row_.cells_[i].set_int(block_idx_);
        break;
      case END_LOG_SCN:
        //end_log_scn
        cur_row_.cells_[i].set_uint64(!table_key.get_end_scn().is_valid() ? 0 : table_key.get_end_scn().get_val_for_inner_table_field());
        break;
      case MACRO_LOGIC_VERSION:
        //macro_logic_version
        cur_row_.cells_[i].set_uint64(macro_info.macro_logic_version_ < 0 ? 0 : macro_info.macro_logic_version_);
        break;
      case MACRO_BLOCK_IDX:
        //macro_block_index
        cur_row_.cells_[i].set_int(macro_info.macro_block_index_);
        break;
      case DATA_SEQ:
        //data_seq_
        cur_row_.cells_[i].set_int(macro_info.data_seq_);
        break;
      case ROW_COUNT: {
        //row_count
        cur_row_.cells_[i].set_int(macro_info.row_count_);
        break;
      }
      case ORIGINAL_SIZE:
        //original_size
        cur_row_.cells_[i].set_int(macro_info.original_size_);
        break;
      case ENCODING_SIZE:
        //encoding_size
        cur_row_.cells_[i].set_int(macro_info.data_size_);
        break;
      case COMPRESSED_SIZE:
        //compressed_size
        cur_row_.cells_[i].set_int(macro_info.data_zsize_);
        break;
      case OCCUPY_SIZE:
        //occupy_size
        cur_row_.cells_[i].set_int(macro_info.occupy_size_);
        break;
      case MICRO_BLOCK_CNT:
        //micro_block_count
        cur_row_.cells_[i].set_int(macro_info.micro_block_count_);
        break;
      case DATA_CHECKSUM:
        //data_checksum
        cur_row_.cells_[i].set_int(macro_info.data_checksum_);
        break;
      case START_KEY: {
        if (macro_info.store_range_.get_start_key().to_plain_string(start_key_buf_, sizeof(start_key_buf_)) >= 0) {
          cur_row_.cells_[i].set_varchar(start_key_buf_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        } else {
          // if error occur, set to null
          cur_row_.cells_[i].set_null();
        }
        break;
      }
      case END_KEY: {
        if (macro_info.store_range_.get_end_key().to_plain_string(end_key_buf_, sizeof(end_key_buf_)) >= 0) {
          cur_row_.cells_[i].set_varchar(end_key_buf_);
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        } else {
          // if error occur, set to null
          cur_row_.cells_[i].set_null();
        }
        break;
      }
      case BLOCK_TYPE: {
        //block type
        blocksstable::ObMacroDataSeq macro_data_seq(macro_info.data_seq_);
        if (macro_data_seq.is_data_block()) {
          cur_row_.cells_[i].set_varchar(ObString::make_string("data_block"));
        } else if (macro_data_seq.is_index_block()) {
          cur_row_.cells_[i].set_varchar(ObString::make_string("index_block"));
        } else if (macro_data_seq.is_meta_block()) {
          cur_row_.cells_[i].set_varchar(ObString::make_string("meta_block"));
        } else {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "unexpected block type, ", K(ret), K(macro_data_seq));
        }
        break;
      }
      case COMPRESSOR_NAME: {
        //compressor name
        cur_row_.cells_[i].set_varchar(all_compressor_name[macro_info.compressor_type_]);
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case ROW_STORE_TYPE:
        //row_store_type
        cur_row_.cells_[i].set_varchar(ObString::make_string(ObStoreFormat::get_row_store_name(static_cast<ObRowStoreType>(macro_info.row_store_type_))));
        break;
      case CG_IDX:
        //cg_idx
        cur_row_.cells_[i].set_int(table_key.get_column_group_id());
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid column id, ", K(ret), K(col_id));
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }
  return ret;
}

void ObAllVirtualTabletSSTableMacroInfo::clean_cur_sstable()
{
  if (OB_NOT_NULL(macro_iter_)) {
    macro_iter_->~ObIMacroBlockIterator();
    macro_iter_ = nullptr;
  }
  iter_allocator_.reuse();
  curr_range_.set_whole_range();
  curr_sstable_ = nullptr;
  curr_sstable_meta_handle_.reset();
  block_idx_ = 0;
  other_blk_iter_.reset();
}

int ObAllVirtualTabletSSTableMacroInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(execute(row))) {
    SERVER_LOG(WARN, "fail to execute", K(ret));
  }
  return ret;
}

void ObAllVirtualTabletSSTableMacroInfo::release_last_tenant()
{
  clean_cur_sstable();
  cols_desc_.reset();
  table_store_iter_.reset();
  tablet_handle_.reset();
  if (OB_NOT_NULL(tablet_iter_)) {
    tablet_iter_->~ObTenantTabletIterator();
    tablet_iter_ = nullptr;
  }
  iter_allocator_.reset();
  rowkey_allocator_.reset();
  tablet_allocator_.reset();
}

bool ObAllVirtualTabletSSTableMacroInfo::is_need_process(uint64_t tenant_id)
{
  if (!is_virtual_tenant_id(tenant_id) &&
      (is_sys_tenant(effective_tenant_id_) || tenant_id == effective_tenant_id_)){
    bool need_ignore = check_tenant_need_ignore(tenant_id);
    return !need_ignore;
  }
  return false;
}

int ObAllVirtualTabletSSTableMacroInfo::process_curr_tenant(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  MacroInfo macro_info;
  if (!start_to_read_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObAllVirtualTabletSSTableMacroInfo not inited, ", K(ret));
  } else if (NULL == cur_row_.cells_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "cur row cell is NULL", K(ret));
  } else if (OB_FAIL(get_next_macro_info(macro_info))) {
    SERVER_LOG(WARN, "fail to get next macro info", K(ret));
  } else if (OB_FAIL(gen_row(macro_info, row))) {
    SERVER_LOG(WARN, "gen_row failed", K(ret));
  } else {
    ++block_idx_;
  }
  return ret;
}

int ObAllVirtualTabletSSTableMacroInfo::get_next_tablet()
{
  int ret = OB_SUCCESS;
  tablet_handle_.reset();
  tablet_allocator_.reuse();
  if (nullptr == tablet_iter_) {
    tablet_allocator_.set_tenant_id(MTL_ID());
    iter_allocator_.set_tenant_id(MTL_ID());
    rowkey_allocator_.set_tenant_id(MTL_ID());
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    if (OB_ISNULL(tablet_iter_ = new (iter_buf_) ObTenantTabletIterator(*t3m, tablet_allocator_, nullptr/*no op*/))) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "fail to new tablet_iter_", K(ret));
    }
  }
  while(OB_SUCC(ret)) {
    tablet_handle_.reset();
    tablet_allocator_.reuse();
    if (OB_FAIL(tablet_iter_->get_next_tablet(tablet_handle_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        SERVER_LOG(WARN, "fail to get tablet iter", K(ret));
      }
    } else if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected invalid tablet", K(ret), K(tablet_handle_));
    } else if (tablet_handle_.get_obj()->is_empty_shell()) {
    } else {
      bool need_ignore = check_tablet_need_ignore(tablet_handle_.get_obj()->get_tablet_meta());
      if (!need_ignore) {
	  const ObIArray<ObColDesc> &cols_desc = tablet_handle_.get_obj()->get_rowkey_read_info().get_columns_desc();

	  cols_desc_.reuse();
	  if (OB_FAIL(cols_desc_.assign(cols_desc))) {
          SERVER_LOG(WARN, "fail to assign rowkey col desc, ", K(ret));
	  } else if (OB_FAIL(ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(cols_desc_))) {
	    SERVER_LOG(WARN, "fail to add extra rowkey info, ", K(ret));
	  } else {
          ls_id_ = tablet_handle_.get_obj()->get_tablet_meta().ls_id_.id();
	    break;
	  }
      }
    }
  }
  return ret;
}

int ObAllVirtualTabletSSTableMacroInfo::get_next_sstable()
{
  int ret = OB_SUCCESS;
  bool need_ignore = false;
  clean_cur_sstable();
  blocksstable::ObDatumRange curr_range;
  ObITable *table = nullptr;
  if (OB_FAIL(table_store_iter_.get_next(table))) {
    if (OB_UNLIKELY(ret != OB_ITER_END)) {
      SERVER_LOG(WARN, "fail to iterate next table", K(ret));
    } else {
      ret = OB_SUCCESS;
      while (OB_SUCC(ret)) {
        table_store_iter_.reset();
        if (OB_FAIL(get_next_tablet())) {
          if (OB_ITER_END != ret) {
            SERVER_LOG(WARN, "fail to get next tablet", K(ret));
          }
        } else if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "unexpected invalid tablet", K(ret), K_(tablet_handle));
        } else if (OB_FAIL(tablet_handle_.get_obj()->get_all_sstables(table_store_iter_, true/*unpack co table*/))) {
          SERVER_LOG(WARN, "fail to get all tables", K(ret), K_(tablet_handle), K_(table_store_iter));
        } else if (0 != table_store_iter_.count()) {
          break;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(table_store_iter_.get_next(table))) {
        SERVER_LOG(WARN, "fail to get table after switch tablet", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(curr_sstable_ = static_cast<ObSSTable *>(table))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected null curr sstable", K(ret));
  } else {
    if (curr_sstable_->is_empty()
        || check_sstable_need_ignore(curr_sstable_->get_key())) {
      clean_cur_sstable();
    } else if (OB_FAIL(curr_sstable_->get_meta(curr_sstable_meta_handle_))) {
      SERVER_LOG(WARN, "fail to get curr sstable meta handle", K(ret));
    } else {
      const storage::ObITableReadInfo *index_read_info = nullptr;
      if (OB_FAIL(tablet_handle_.get_obj()->get_sstable_read_info(curr_sstable_, index_read_info))) {
        SERVER_LOG(WARN, "failed to get index read info ", KR(ret), KPC_(curr_sstable));
      } else if (OB_FAIL(curr_sstable_->scan_macro_block(
          curr_range_,
          *index_read_info,
          iter_allocator_,
          macro_iter_,
          false,
          false,
          true/*need_scan_sec_meta*/))) {
        SERVER_LOG(WARN, "Fail to scan macro block", K(ret), K(curr_range_));
      }
    }
  }
  return ret;
}

bool ObAllVirtualTabletSSTableMacroInfo::check_tenant_need_ignore(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObNewRange range;
  bool need_ignore = true;

  int index = 0;
  objs_[index].set_varchar(ip_buf_);
  objs_[index].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  index++;
  objs_[index++].set_int(ObServerConfig::get_instance().self_addr_.get_port());
  objs_[index++].set_int(tenant_id); // Do not use MTL_ID here

  ObRowkey rowkey(objs_, index + 4);
  for (int64_t i = 0; i < key_ranges_.count() && need_ignore; ++i) {
    if (key_ranges_.at(i).border_flag_.inclusive_start()) {
      objs_[index] = key_ranges_.at(i).start_key_.get_obj_ptr()[index]; // ls_id
      objs_[index+1] = key_ranges_.at(i).start_key_.get_obj_ptr()[index+1]; // tablet_id
      objs_[index+2] = key_ranges_.at(i).start_key_.get_obj_ptr()[index+2]; // end_log_scn
      objs_[index+3] = key_ranges_.at(i).start_key_.get_obj_ptr()[index+3]; // macro_idx_in_sstable
    } else {
      objs_[index].set_int(key_ranges_.at(i).start_key_.get_obj_ptr()[index].get_int() + 1);
      objs_[index+1].set_int(key_ranges_.at(i).start_key_.get_obj_ptr()[index+1].get_int() + 1);
      objs_[index+2].set_uint64(key_ranges_.at(i).start_key_.get_obj_ptr()[index+2].get_uint64() + 1);
      objs_[index+3].set_int(key_ranges_.at(i).start_key_.get_obj_ptr()[index+3].get_int() + 1);
    }
    if (OB_FAIL(range.build_range(OB_ALL_VIRTUAL_TABLET_SSTABLE_MACRO_INFO_TID, rowkey))) {
      SERVER_LOG(WARN, "build_range failed", K(ret), K(rowkey));
    } else if (key_ranges_.at(i).include(range)) {
      need_ignore = false;
    }
  }
  if (OB_FAIL(ret)) {
    need_ignore = false; // if error, wont skip
  }
  SERVER_LOG(DEBUG, "sstable_macro_info try to skip tenant", K(ret), K(need_ignore), K(range), K(tenant_id));
  return need_ignore;
}

bool ObAllVirtualTabletSSTableMacroInfo::check_tablet_need_ignore(const ObTabletMeta &tablet_meta)
{
  int ret = OB_SUCCESS;
  ObNewRange range;
  bool need_ignore = true;

  int index = 0;
  objs_[index].set_varchar(ip_buf_);
  objs_[index].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  index++;
  objs_[index++].set_int(ObServerConfig::get_instance().self_addr_.get_port());
  objs_[index++].set_int(MTL_ID()); // tenant_id
  objs_[index++].set_int(tablet_meta.ls_id_.id()); // ls_id
  objs_[index++].set_int(tablet_meta.tablet_id_.id()); // tablet_id

  ObRowkey rowkey(objs_, index + 2);
  for (int64_t i = 0; i < key_ranges_.count() && need_ignore; ++i) {
    if (key_ranges_.at(i).border_flag_.inclusive_start()) {
      objs_[index] = key_ranges_.at(i).start_key_.get_obj_ptr()[index]; // end_log_scn
      objs_[index+1] = key_ranges_.at(i).start_key_.get_obj_ptr()[index+1]; // macro_idx_in_sstable
    } else {
      objs_[index].set_uint64(key_ranges_.at(i).start_key_.get_obj_ptr()[index].get_uint64() + 1);
      objs_[index+1].set_int(key_ranges_.at(i).start_key_.get_obj_ptr()[index+1].get_int() + 1);
    }
    if (OB_FAIL(range.build_range(OB_ALL_VIRTUAL_TABLET_SSTABLE_MACRO_INFO_TID, rowkey))) {
      SERVER_LOG(WARN, "build_range failed", K(ret), K(rowkey));
    } else if (key_ranges_.at(i).include(range)) {
      need_ignore = false;
    }
  }
  if (OB_FAIL(ret)) {
    need_ignore = false; // if error, wont skip
  }
  SERVER_LOG(DEBUG, "sstable_macro_info try to skip tablet", K(ret), K(need_ignore), K(range), K(tablet_meta));
  return need_ignore;
}


bool ObAllVirtualTabletSSTableMacroInfo::check_sstable_need_ignore(const ObITable::TableKey &table_key)
{
  int ret = OB_SUCCESS;
  ObNewRange range;
  bool need_ignore = true;
  int index = 0;
  objs_[index].set_varchar(ip_buf_);
  objs_[index].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
  index++;
  objs_[index++].set_int(ObServerConfig::get_instance().self_addr_.get_port());
  objs_[index++].set_int(MTL_ID()); // tenant_id
  objs_[index++].set_int(ls_id_); // ls_id
  objs_[index++].set_int(table_key.tablet_id_.id()); // tablet_id
  objs_[index++].set_uint64(!table_key.get_end_scn().is_valid() ? 0 : table_key.get_end_scn().get_val_for_inner_table_field());

  ObRowkey rowkey(objs_, index + 1);
  for (int64_t i = 0; i < key_ranges_.count() && need_ignore; ++i) {
    if (key_ranges_.at(i).border_flag_.inclusive_start()) {
      objs_[index] = key_ranges_.at(i).start_key_.get_obj_ptr()[index]; // macro_idx_in_sstable
    } else {
      objs_[index].set_int(key_ranges_.at(i).start_key_.get_obj_ptr()[index].get_int() + 1);
    }
    if (OB_FAIL(range.build_range(OB_ALL_VIRTUAL_TABLET_SSTABLE_MACRO_INFO_TID, rowkey))) {
      SERVER_LOG(WARN, "build_range failed", K(ret), K(rowkey));
    } else if (key_ranges_.at(i).include(range)) {
      need_ignore = false;
    }
  }
  if (OB_FAIL(ret)) {
    need_ignore = false; // if error, wont skip
  }
  SERVER_LOG(DEBUG, "sstable_macro_info try to skip sstable", K(ret), K(need_ignore), K(range), K(table_key));
  return need_ignore;
}

} /* namespace observer */
} /* namespace oceanbase */
