/*************************************************************************
  * Copyright (c) 2022 OceanBase
  * OceanBase is licensed under Mulan PubL v2.
  * You can use this software according to the terms and conditions of the Mulan PubL v2
  * You may obtain a copy of Mulan PubL v2 at:
  *          http://license.coscl.org.cn/MulanPubL-2.0
  * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
  * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
  * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
  * See the Mulan PubL v2 for more details.
  * File Name   : ob_column_oriented_sstable.cpp
  * Created  on : 09/05/2022
 ************************************************************************/
#define USING_LOG_PREFIX STORAGE

#include "ob_column_oriented_sstable.h"
#include "ob_co_sstable_row_getter.h"
#include "ob_co_sstable_row_scanner.h"
#include "ob_co_sstable_row_multi_getter.h"
#include "ob_co_sstable_row_multi_scanner.h"
#include "ob_cg_scanner.h"
#include "ob_cg_tile_scanner.h"
#include "ob_cg_aggregated_scanner.h"
#include "ob_cg_group_by_scanner.h"
#include "ob_virtual_cg_scanner.h"
#include "storage/ob_storage_struct.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "common/ob_tablet_id.h"
#include "share/schema/ob_table_schema.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/access/ob_sstable_row_multi_scanner.h"
#include "storage/blocksstable/index_block/ob_ddl_index_block_row_iterator.h"
#include "storage/tablet/ob_tablet_table_store.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share::schema;

namespace oceanbase
{
namespace storage
{

ObSSTableWrapper::ObSSTableWrapper()
  : meta_handle_(),
    sstable_(nullptr)
{
}

void ObSSTableWrapper::reset()
{
  meta_handle_.reset();
  sstable_ = nullptr;
}

int ObSSTableWrapper::set_sstable(
    ObSSTable *sstable,
    ObStorageMetaHandle *meta_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == sstable)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), KPC(sstable), KPC(meta_handle));
  } else if (FALSE_IT(sstable_ = sstable)) {
  } else if (nullptr != meta_handle) {
    meta_handle_ = *meta_handle;
  }
  return ret;
}

int ObSSTableWrapper::get_loaded_column_store_sstable(ObSSTable *&table)
{
  int ret = OB_SUCCESS;
  ObSSTable *meta_sstable = nullptr;
  ObSSTableMetaHandle co_meta_handle;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrapper not valid", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!sstable_->is_column_store_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("This func can only be used when fetching column store SSTable", K(ret), KPC(sstable_));
  } else if (sstable_->is_loaded()) {
    table = sstable_;
  } else if (OB_UNLIKELY(!meta_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta handle is unexpected not valid", K(ret), KPC(sstable_), K(meta_handle_));
  } else if (OB_FAIL(meta_handle_.get_sstable(meta_sstable))) {
    LOG_WARN("failed to get sstable", K(ret), KPC(this));
  } else if (sstable_->get_key() == meta_sstable->get_key()) {
    table = meta_sstable;
  } else if (OB_UNLIKELY(!sstable_->is_cg_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected not cg sstable", K(ret), KPC(sstable_), KPC(meta_sstable));
  } else if (OB_FAIL(meta_sstable->get_meta(co_meta_handle))) {
    LOG_WARN("failed to get co meta handle", K(ret), KPC(meta_sstable), KPC(sstable_));
  } else {
    const ObSSTableArray &cg_sstables = co_meta_handle.get_sstable_meta().get_cg_sstables();
    for (int64_t idx = 0; OB_SUCC(ret) && idx < cg_sstables.count(); ++idx) {
      if (sstable_->get_key() == cg_sstables[idx]->get_key()) {
        table = cg_sstables[idx];
        break;
      }
    }
  }
  return ret;
}

int ObSSTableWrapper::get_merge_row_cnt(const ObTableIterParam &iter_param, int64_t &row_cnt)
{
  int ret = OB_SUCCESS;
  row_cnt = 0;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("wrapper not valid", K(ret), KPC(this));
  } else if (!sstable_->is_ddl_merge_sstable()) {
    row_cnt = sstable_->get_row_count();
  } else {
    ObArenaAllocator allocator("DDL_row_cnt", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    if (OB_UNLIKELY(!iter_param.is_valid()) || OB_ISNULL(iter_param.tablet_handle_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid iter param", K(ret), K(iter_param), K(iter_param.tablet_handle_));
    } else {
      ObMicroBlockData root_block;
      ObIndexBlockIterParam index_iter_param(sstable_, iter_param.tablet_handle_->get_obj());
      ObDatumRange range;
      range.set_start_key(ObDatumRowkey::MIN_ROWKEY);
      range.set_end_key(ObDatumRowkey::MAX_ROWKEY);
      range.set_left_open();
      range.set_right_open();
      int64_t index_row_count = 0;
      int64_t data_row_count = 0;


      blocksstable::ObSSTable *cur_sstable = nullptr;
      ObDDLMergeBlockRowIterator ddl_merge_iter;
      if (OB_FAIL(get_loaded_column_store_sstable(cur_sstable))) {
        LOG_WARN("fail to get sstable", K(ret), K(*this));
      } else if (OB_FAIL((cur_sstable->get_index_tree_root(root_block)))) {
        LOG_WARN("fail to get index tree root", K(ret), K(root_block), K(*this));
      } else if (OB_FAIL(ddl_merge_iter.init(root_block, &(iter_param.tablet_handle_->get_obj()->get_rowkey_read_info().get_datum_utils()), &allocator, false/*is_reverse_scan*/, index_iter_param))) {
        LOG_WARN("fail to init ddl_merge_iter", K(ret), K(root_block), K(index_iter_param));
      } else if (OB_FAIL(ddl_merge_iter.get_index_row_count(range, false/*left border*/, false/*right border*/, index_row_count, data_row_count))) {
        LOG_WARN("fail to get row cnt", K(ret), K(index_row_count), K(data_row_count));
      } else if (INT64_MAX == data_row_count) {
        // INT64_MAX means only one sstable without kv, just get from meta_cache
        row_cnt = sstable_->get_row_count();
      } else {
        row_cnt = data_row_count;
      }
    }
    LOG_INFO("get ddl merge row cnt", K(ret), K(row_cnt));
  }
  return ret;
}

/************************************* ObCOSSTableMeta *************************************/
int64_t ObCOSSTableMeta::get_serialize_size() const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      data_macro_block_cnt_,
      use_old_macro_block_cnt_,
      data_micro_block_cnt_,
      index_macro_block_cnt_,
      occupy_size_,
      original_size_,
      data_checksum_,
      column_group_cnt_,
      full_column_cnt_);
  return len;
}

int ObCOSSTableMeta::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int64_t len = get_serialize_size();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < 0 || pos + len > buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(buf_len), K(len));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
        data_macro_block_cnt_,
        use_old_macro_block_cnt_,
        data_micro_block_cnt_,
        index_macro_block_cnt_,
        occupy_size_,
        original_size_,
        data_checksum_,
        column_group_cnt_,
        full_column_cnt_);
    }
  return ret;
}

int ObCOSSTableMeta::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len < 0 || data_len < pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    LST_DO_CODE(OB_UNIS_DECODE,
        data_macro_block_cnt_,
        use_old_macro_block_cnt_,
        data_micro_block_cnt_,
        index_macro_block_cnt_,
        occupy_size_,
        original_size_,
        data_checksum_,
        column_group_cnt_,
        full_column_cnt_);
  }
  return ret;
}


/************************************* ObCOSSTableV2 *************************************/
ObCOSSTableV2::ObCOSSTableV2()
  : ObSSTable(),
    cs_meta_(),
    base_type_(ObCOSSTableBaseType::INVALID_TYPE),
    is_cgs_empty_co_(false),
    valid_for_cs_reading_(false),
    tmp_allocator_("CGAlloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
}

ObCOSSTableV2::~ObCOSSTableV2()
{
  reset();
}

void ObCOSSTableV2::reset()
{
  ObSSTable::reset();
  cs_meta_.reset();
  valid_for_cs_reading_ = false;
  tmp_allocator_.reset();
}

int ObCOSSTableV2::init(
    const ObTabletCreateSSTableParam &param,
    common::ObArenaAllocator *allocator)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!param.is_valid() ||
                  ObCOSSTableBaseType::INVALID_TYPE >= param.co_base_type_ ||
                  ObCOSSTableBaseType::MAX_TYPE <= param.co_base_type_ ||
                  1 >= param.column_group_cnt_ ||
                  NULL == allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(param), K(allocator));
  } else if (OB_FAIL(ObSSTable::init(param, allocator))) {
    LOG_WARN("failed to init basic ObSSTable", K(ret), K(param));
  } else if (param.is_co_table_without_cgs_) {
    // current co sstable is empty, or the normal cg is redundant, no need to init cg sstable
    cs_meta_.column_group_cnt_ = param.column_group_cnt_; // other cs meta is zero.
    is_cgs_empty_co_ = true;
    if (OB_FAIL(build_cs_meta_without_cgs())) {
      LOG_WARN("failed to build cs meta without cgs", K(ret), K(param), KPC(this));
    }
  }

  if (OB_SUCC(ret)) {
    base_type_ = static_cast<ObCOSSTableBaseType>(param.co_base_type_);
    valid_for_cs_reading_ = param.is_co_table_without_cgs_;
    cs_meta_.full_column_cnt_ = param.full_column_cnt_;
  } else {
    reset();
  }
  return ret;
}

int ObCOSSTableV2::fill_cg_sstables(const common::ObIArray<ObITable *> &cg_tables)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!valid_for_reading_ || valid_for_cs_reading_ || is_cgs_empty_co_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("this co sstable can't init cg sstables", K(ret),
        K(valid_for_reading_), K(valid_for_cs_reading_), K(is_cgs_empty_co_));
  } else if (OB_UNLIKELY(!is_loaded())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("co sstable must be loaded before persist", K(ret), KPC(this));
  } else if (OB_FAIL(meta_->fill_cg_sstables(tmp_allocator_, cg_tables))) {
    LOG_WARN("failed to fill cg sstables to sstable meta", K(ret), KPC(this), KPC(meta_));
  } else if (OB_FAIL(build_cs_meta())) {
    LOG_WARN("failed to build cs meta", K(ret), KPC(this));
  } else {
    valid_for_cs_reading_ = true;
    FLOG_INFO("success to init co sstable", K(ret), K_(cs_meta), KPC(this)); // tmp debug code
  }
  return ret;
}

int ObCOSSTableV2::build_cs_meta_without_cgs()
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle sstable_meta_handle;
  if (OB_UNLIKELY(!is_cgs_empty_co_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("no need to build cs meta for co table without cg sstables", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!is_loaded())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("co table is unexpected not loaded", K(ret), KPC(this));
  } else if (OB_FAIL(ObSSTable::get_meta(sstable_meta_handle))) {
    LOG_WARN("failed to get meta handle", K(ret), KPC(this));
  } else {
    const ObSSTableBasicMeta &basic_meta = sstable_meta_handle.get_sstable_meta().get_basic_meta();
    cs_meta_.data_macro_block_cnt_ = basic_meta.data_macro_block_count_;
    cs_meta_.use_old_macro_block_cnt_ = basic_meta.use_old_macro_block_count_;
    cs_meta_.data_micro_block_cnt_ = basic_meta.data_micro_block_count_;
    cs_meta_.index_macro_block_cnt_ = basic_meta.index_macro_block_count_;
    cs_meta_.occupy_size_ = basic_meta.occupy_size_;
    cs_meta_.original_size_ = basic_meta.original_size_;
    cs_meta_.data_checksum_ = basic_meta.data_checksum_;
    // cs_meta_.column_group_cnt_ and cs_meta_.full_column_cnt_ are assigned in ObCOSSTableV2::init
    LOG_INFO("[RowColSwitch] finish build cs meta without cg sstables", K_(cs_meta), K(basic_meta), KPC(this));
  }
  return ret;
}

int ObCOSSTableV2::build_cs_meta()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_cgs_empty_co_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("no need to build cs meta for co table with empty cg sstables", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!is_loaded())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("co table is unexpected not loaded", K(ret), KPC(this));
  } else {
    const ObSSTableArray &cg_sstables = meta_->get_cg_sstables();
    const int64_t cg_table_cnt = cg_sstables.count() + 1/*base_cg_table*/;
    cs_meta_.column_group_cnt_ = cg_table_cnt;

    for (int64_t idx = 0; OB_SUCC(ret) && idx < cg_table_cnt; ++idx) {
      ObSSTable *cg_sstable = (cg_table_cnt - 1 == idx) ? this : cg_sstables[idx];
      ObSSTableMetaHandle cg_meta_handle;
      if (OB_ISNULL(cg_sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null cg sstable", K(ret));
      } else if (OB_UNLIKELY(cg_sstable->is_rowkey_cg_sstable()
          && ObCOSSTableBaseType::ROWKEY_CG_TYPE == base_type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected rowkey cg table", K(ret), K(base_type_), KPC(cg_sstable));
      } else if (OB_UNLIKELY(cg_sstable->get_end_scn() != get_end_scn())) { // ddl sstable may only contain partial data
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the snapshot version of cg sstables must be equal", K(ret));
      } else if (OB_FAIL(cg_sstable->get_meta(cg_meta_handle))) {
        LOG_WARN("Failed to get cg sstable meta", K(ret), KPC(cg_sstable));
      } else if (OB_UNLIKELY(cg_meta_handle.get_sstable_meta().get_schema_version() != meta_->get_schema_version())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the schema version of cg sstables must be equal", K(ret), KPC(meta_), K(cg_meta_handle));
      } else if (OB_UNLIKELY(cg_sstable->is_major_sstable() && cg_meta_handle.get_sstable_meta().get_row_count() != meta_->get_row_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the row count of cg sstables must be equal", K(ret), KPC(cg_sstable), KPC(meta_), K(cg_meta_handle));
      } else {
        cs_meta_.data_macro_block_cnt_ += cg_meta_handle.get_sstable_meta().get_basic_meta().data_macro_block_count_;
        cs_meta_.use_old_macro_block_cnt_ += cg_meta_handle.get_sstable_meta().get_basic_meta().use_old_macro_block_count_;
        cs_meta_.data_micro_block_cnt_ += cg_meta_handle.get_sstable_meta().get_basic_meta().data_micro_block_count_;
        cs_meta_.index_macro_block_cnt_ += cg_meta_handle.get_sstable_meta().get_basic_meta().index_macro_block_count_;
        cs_meta_.occupy_size_ += cg_meta_handle.get_sstable_meta().get_basic_meta().occupy_size_;
        cs_meta_.original_size_ += cg_meta_handle.get_sstable_meta().get_basic_meta().original_size_;
        cs_meta_.data_checksum_ += cg_meta_handle.get_sstable_meta().get_basic_meta().data_checksum_;
      }
    }
  }
  return ret;
}

int64_t ObCOSSTableV2::get_serialize_size() const
{
  int64_t len = 0;
  len += ObSSTable::get_serialize_size();
  len += serialization::encoded_length_i32(base_type_);
  len += serialization::encoded_length_bool(is_cgs_empty_co_);
  len += cs_meta_.get_serialize_size();
  return len;
}

int ObCOSSTableV2::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int64_t old_pos = pos;

  if (OB_UNLIKELY(!is_cs_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("co sstable not init", K(ret), KPC(this));
  } else if (OB_UNLIKELY(NULL == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(ObSSTable::serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize basic ObSSTable", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, base_type_))) {
    LOG_WARN("failed to serialize base type", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_bool(buf, buf_len, pos, is_cgs_empty_co_))) {
    LOG_WARN("failed to serialize is empty co", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(cs_meta_.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize cs meta", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    LOG_INFO("succeed to serialize co sstable", K(ret), KPC(this), K(buf_len), K(old_pos), K(pos));
  }
  return ret;
}

int ObCOSSTableV2::deserialize(
    common::ObArenaAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t old_pos = pos;

  if (OB_UNLIKELY(is_cs_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("co sstable has been inited", K(ret), KPC(this));
  } else if (OB_UNLIKELY(NULL == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments.", KP(buf),K(data_len), K(pos), K(ret));
  } else if (OB_FAIL(ObSSTable::deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize basic ObSSTable", K(ret), KP(buf),K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, reinterpret_cast<int32_t *>(&base_type_)))) {
    LOG_WARN("failed to decode base type", K(ret), KP(buf),K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_bool(buf, data_len, pos, &is_cgs_empty_co_))) {
    LOG_WARN("failed to decode is empty co", K(ret), KP(buf),K(data_len), K(pos));
  } else if (OB_FAIL(cs_meta_.deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize cs meta", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    valid_for_cs_reading_ = true;
    LOG_DEBUG("success to deserialize co sstable", K(ret), KPC(this), K(data_len), K(pos), K(old_pos));
  }
  return ret;
}

int ObCOSSTableV2::serialize_full_table(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int64_t old_pos = pos;

  if (OB_UNLIKELY(!is_cs_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("co sstable not init", K(ret), KPC(this), K(key_.column_group_idx_), K(cs_meta_.column_group_cnt_));
  } else if (OB_UNLIKELY(NULL == buf || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("argument is invalid", K(ret), KP(buf), K(buf_len));
  } else if (OB_FAIL(ObSSTable::serialize_full_table(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize full sstable", K(ret));
  } else if (OB_FAIL(serialization::encode_i32(buf, buf_len, pos, base_type_))) {
    LOG_WARN("failed to serialize base type", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_bool(buf, buf_len, pos, is_cgs_empty_co_))) {
    LOG_WARN("failed to serialize is empty co", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(cs_meta_.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to deserialize cs meta", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    LOG_INFO("succeed to serialize co sstable", K(ret), KPC(this), K(buf_len), K(old_pos), K(pos));
  }
  return ret;
}

int64_t ObCOSSTableV2::get_full_serialize_size() const
{
  int64_t len = 0;
  len += ObSSTable::get_full_serialize_size();
  if (len > 0) {
    len += serialization::encoded_length_i32(base_type_);
    len += serialization::encoded_length_bool(is_cgs_empty_co_);
    len += cs_meta_.get_serialize_size();
  }
  return len;
}

int ObCOSSTableV2::deep_copy(char *buf, const int64_t buf_len, ObIStorageMetaObj *&value) const
{
  int ret = OB_SUCCESS;
  value = nullptr;
  const int64_t deep_copy_size = get_deep_copy_size();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < deep_copy_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(deep_copy_size));
  } else {
    ObCOSSTableV2 *new_co_table = new (buf) ObCOSSTableV2();
    int64_t pos = sizeof(ObCOSSTableV2);
    // deep copy
    new_co_table->key_ = key_;
    new_co_table->addr_ = addr_;
    new_co_table->meta_cache_ = meta_cache_;
    new_co_table->is_tmp_sstable_ = false;
    new_co_table->valid_for_reading_ = valid_for_reading_;
    if (is_loaded()) {
      if (OB_FAIL(meta_->deep_copy(buf, buf_len, pos, new_co_table->meta_))) {
        LOG_WARN("fail to deep copy for tiny memory", K(ret), KP(buf), K(buf_len), K(pos), KPC(meta_));
      }
    }

    if (OB_SUCC(ret)) {
      MEMCPY(&new_co_table->cs_meta_, &cs_meta_, sizeof(ObCOSSTableMeta));
      new_co_table->is_cgs_empty_co_ = is_cgs_empty_co_;
      new_co_table->base_type_ = base_type_;
      new_co_table->valid_for_cs_reading_ = true;

      value = new_co_table;
    }
  }
  return ret;
}

// co sstable must be full memory
int ObCOSSTableV2::deep_copy(
    common::ObArenaAllocator &allocator,
    const common::ObIArray<ObMetaDiskAddr> &cg_addrs,
    ObCOSSTableV2 *&dst)
{
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = get_deep_copy_size();
  char *buf = nullptr;
  ObCOSSTableV2 *new_co_table = nullptr;
  ObIStorageMetaObj *meta_obj = nullptr;
  ObSSTableArray &cg_sstables = meta_->get_cg_sstables();

  if (OB_UNLIKELY(!valid_for_cs_reading_ || !cg_sstables.is_valid())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("this co sstable can't set cg table addr", K(ret), K_(valid_for_cs_reading), K(cg_sstables));
  } else if (OB_UNLIKELY(cg_addrs.count() != cg_sstables.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(cg_addrs.count()), K(cg_sstables));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(deep_copy_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for deep copy sstable", K(ret), K(deep_copy_size));
  } else if (OB_FAIL(deep_copy(buf, deep_copy_size, meta_obj))) {
    LOG_WARN("failed to deep copy co sstable", K(ret));
  } else {
    new_co_table = static_cast<ObCOSSTableV2 *>(meta_obj);

    // set cg sstable addr
    ObSSTableArray &new_cg_sstables = new_co_table->meta_->get_cg_sstables();
    for (int64_t idx = 0; OB_SUCC(ret) && idx < new_cg_sstables.count(); ++idx) {
      ObSSTable *cg_table = new_cg_sstables[idx];
      const ObMetaDiskAddr &cg_addr = cg_addrs.at(idx);
      if (OB_ISNULL(cg_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null cg table", K(ret), KPC(this));
      } else if (OB_FAIL(cg_table->set_addr(cg_addr))) {
        LOG_WARN("failed to set cg addr", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    dst = new_co_table;
  }
  return ret;
}

int ObCOSSTableV2::fetch_cg_sstable(
    const uint32_t cg_idx,
    ObSSTableWrapper &cg_wrapper) const
{
  int ret = OB_SUCCESS;
  cg_wrapper.reset();

  uint32_t real_cg_idx = cg_idx < cs_meta_.column_group_cnt_ ? cg_idx : key_.column_group_idx_;
  if (OB_UNLIKELY(is_cgs_empty_co_ && real_cg_idx != key_.get_column_group_id())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("co sstable is empty, cannot fetch cg sstable", K(ret), K(cg_idx), K(real_cg_idx), KPC(this));
  } else if (OB_FAIL(get_cg_sstable(real_cg_idx, cg_wrapper))) {
    LOG_WARN("failed to get cg sstable", K(ret));
  } else if (OB_ISNULL(cg_wrapper.sstable_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null cg table", K(ret), K(cg_wrapper));
  } else if (cg_wrapper.sstable_->is_cg_sstable() || cg_wrapper.sstable_->is_loaded()) {
    // do nothing
  } else if (OB_FAIL(ObTabletTableStore::load_sstable(cg_wrapper.sstable_->get_addr(),
                                                      true/*load_co_sstable*/,
                                                      cg_wrapper.meta_handle_))) {
    LOG_WARN("failed to load sstable", K(ret), K(cg_wrapper));
  }
  return ret;
}

int ObCOSSTableV2::get_cg_sstable(
    const uint32_t cg_idx,
    ObSSTableWrapper &cg_wrapper) const
{
  int ret = OB_SUCCESS;
  cg_wrapper.reset();
  ObSSTableMetaHandle co_meta_handle;

  if (OB_UNLIKELY(!is_cs_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("co sstable has not inited", K(ret), KPC(this));
  } else if (cg_idx >= cs_meta_.column_group_cnt_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(cg_idx), K(cs_meta_));
  } else if (OB_FAIL(get_meta(co_meta_handle))) {
    LOG_WARN("failed to get co meta handle", K(ret), KPC(this));
  } else if (cg_idx == key_.get_column_group_id()) {
    cg_wrapper.sstable_ = const_cast<ObCOSSTableV2 *>(this);
  } else if (OB_UNLIKELY(is_cgs_empty_co_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("co sstable is all_cg only, cannot fetch normal cg sstable", K(ret), K(cg_idx), KPC(this));
  } else {
    const ObSSTableArray &cg_sstables = co_meta_handle.get_sstable_meta().get_cg_sstables();
    cg_wrapper.sstable_ = cg_idx < key_.column_group_idx_
                        ? cg_sstables[cg_idx]
                        : cg_sstables[cg_idx - 1]; // deal with that the rowkey/all cg idx is at the middle when add column online
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(cg_wrapper.sstable_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected null sstable", K(ret));
  } else if (cg_wrapper.sstable_->is_co_sstable()) {
    // do nothing
  } else if (cg_wrapper.sstable_->is_loaded()) {
    if (co_meta_handle.get_storage_handle().is_valid()) {
      // cg sstable lifetime guranteed by co meta handle
      cg_wrapper.meta_handle_ = co_meta_handle.get_storage_handle();
    } else {
      // co sstable and cg sstable is all loaded, no need to store meta handle
    }
  } else if (OB_FAIL(ObTabletTableStore::load_sstable(cg_wrapper.sstable_->get_addr(),
                                                      false/*load_co_sstable*/,
                                                      cg_wrapper.meta_handle_))) {
    LOG_WARN("failed to load sstable", K(ret), K(cg_wrapper));
  } else if (OB_FAIL(cg_wrapper.meta_handle_.get_sstable(cg_wrapper.sstable_))) { // should update cg sstable ptr in wrapper after load full cg sstable
    LOG_WARN("failed to get sstable from meta handle", K(ret), K(cg_idx), K(cg_wrapper));
  }
  return ret;
}

/*
 * Returning ObITable* is no longer safe due to the load demand of CG sstable.
 */
int ObCOSSTableV2::get_all_tables(common::ObIArray<ObSSTableWrapper> &table_wrappers) const
{
  int ret = OB_SUCCESS;
  ObSSTableMetaHandle meta_handle;

  if (is_cgs_empty_co_) {
    ObSSTableWrapper co_wrapper;
    co_wrapper.sstable_ = const_cast<ObCOSSTableV2 *>(this);
    if (OB_FAIL(table_wrappers.push_back(co_wrapper))) {
      LOG_WARN("failed to push back", K(ret), K(is_cgs_empty_co_));
    }
  } else if (OB_FAIL(get_meta(meta_handle))) {
    LOG_WARN("failed to get meta handle", K(ret), KPC(this));
  } else {
    // cg_idx is the offset of column group, not the array index of cg_sstables_. Use <= to cover *this(row key or all cg).
    // By default, row key or all cg is set in the last of cg group, but after adding column group, it will split the offset.
    const ObSSTableArray &cg_sstables = meta_handle.get_sstable_meta().get_cg_sstables();
    for (int64_t cg_idx = 0; OB_SUCC(ret) && cg_idx <= cg_sstables.count(); ++cg_idx) {
      ObSSTableWrapper cg_wrapper;
      if (OB_FAIL(get_cg_sstable(cg_idx, cg_wrapper))) {
        LOG_WARN("failed to get cg sstable", K(ret), K(cg_idx));
      } else if (OB_FAIL(table_wrappers.push_back(cg_wrapper))) {
        LOG_WARN("failed to push back", K(ret));
      }
    }
  }
  return ret;
}

int ObCOSSTableV2::scan(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const blocksstable::ObDatumRange &key_range,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_cs_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("SSTable is not ready for accessing", K(ret), K_(valid_for_cs_reading), K_(key), K_(base_type), K_(meta));
  } else if (OB_UNLIKELY(param.tablet_id_ != key_.tablet_id_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Tablet id is not match", K(ret), K(*this), K(param));
  } else if (OB_UNLIKELY(!param.is_valid() || !context.is_valid() || !key_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param), K(context), K(key_range));
  } else if (is_row_store_only_co_table() || (is_all_cg_base() && !param.is_use_column_store())) { // param.is_use_column_store() will core if param.read_info is instance of ObReadInfoStruct
    if (OB_FAIL(ObSSTable::scan(param, context, key_range, row_iter))) {
      LOG_WARN("Fail to scan in row store sstable", K(ret));
    }
  } else {
    // TODO: check whether use row_store/rowkey sstable when primary keys accessed only
    ObStoreRowIterator *row_scanner = nullptr;
    ALLOCATE_TABLE_STORE_ROW_IETRATOR(context, ObCOSSTableRowScanner, row_scanner);
    if (OB_SUCC(ret) && OB_NOT_NULL(row_scanner) && OB_FAIL(row_scanner->init(param, context, this, &key_range))) {
      LOG_WARN("Fail to open row scanner", K(ret), K(param), K(context), K(key_range), K(*this));
    }

    if (OB_FAIL(ret)) {
      if (nullptr != row_scanner) {
        row_scanner->~ObStoreRowIterator();
        FREE_TABLE_STORE_ROW_IETRATOR(context, row_scanner);
        row_scanner = nullptr;
      }
    } else {
      row_iter = row_scanner;
    }
  }
  return ret;
}

int ObCOSSTableV2::multi_scan(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const common::ObIArray<blocksstable::ObDatumRange> &ranges,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_cs_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("SSTable is not ready for accessing", K(ret), K_(valid_for_reading), K_(meta));
  } else if (OB_UNLIKELY(param.tablet_id_ != key_.tablet_id_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Tablet id is not match", K(ret), K(*this), K(param));
  } else if (OB_UNLIKELY(!param.is_valid() || !context.is_valid() || 0 >= ranges.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param), K(context), K(ranges));
  } else if ((!param.is_use_column_store() && is_all_cg_base()) || is_row_store_only_co_table()) {
    if (OB_FAIL(ObSSTable::multi_scan(param, context, ranges, row_iter))) {
      LOG_WARN("Fail to scan in row store sstable", K(ret));
    }
  } else {
    // TODO: check whether use row_store/rowkey sstable when primary keys accessed only
    ObStoreRowIterator *row_scanner = nullptr;
    ALLOCATE_TABLE_STORE_ROW_IETRATOR(context, ObCOSSTableRowMultiScanner, row_scanner);
    if (OB_SUCC(ret) && OB_NOT_NULL(row_scanner) && OB_FAIL(row_scanner->init(param, context, this, &ranges))) {
      LOG_WARN("Fail to open row scanner", K(ret), K(param), K(context), K(ranges), K(*this));
    }

    if (OB_FAIL(ret)) {
      if (nullptr != row_scanner) {
        row_scanner->~ObStoreRowIterator();
        FREE_TABLE_STORE_ROW_IETRATOR(context, row_scanner);
        row_scanner = nullptr;
      }
    } else {
      row_iter = row_scanner;
    }
  }
  return ret;
}

int ObCOSSTableV2::cg_scan(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    ObICGIterator *&cg_iter,
    const bool is_projector,
    const bool project_single_row)
{
  int ret = OB_SUCCESS;
  cg_iter = nullptr;
  ObICGIterator *cg_scanner = nullptr;
  ObSSTableWrapper table_wrapper;

  if (OB_UNLIKELY(!is_cs_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("SSTable is not ready for accessing", K(ret), K_(valid_for_reading), K_(meta));
  } else if (OB_UNLIKELY(!context.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(context));
  } else if (is_virtual_cg(param.cg_idx_)) {
    ALLOCATE_TABLE_STORE_CG_IETRATOR(context, param.cg_idx_, ObVirtualCGScanner, cg_scanner);
  } else if (OB_FAIL(fetch_cg_sstable(param.cg_idx_, table_wrapper))) {
    LOG_WARN("failed to fetch cg table wrapper", K(ret), K(param), KPC(this));
  } else if (project_single_row) {
    if (param.cg_idx_ >= cs_meta_.column_group_cnt_) {
      ALLOCATE_TABLE_STORE_CG_IETRATOR(context, param.cg_idx_, ObDefaultCGScanner, cg_scanner);
    } else {
      ALLOCATE_TABLE_STORE_CG_IETRATOR(context, param.cg_idx_, ObCGSingleRowScanner, cg_scanner);
    }
  } else if (param.cg_idx_ >= cs_meta_.column_group_cnt_) {
    if (param.enable_pd_group_by() && is_projector) {
      ALLOCATE_TABLE_STORE_CG_IETRATOR(context, param.cg_idx_, ObDefaultCGGroupByScanner, cg_scanner);
    } else {
      ALLOCATE_TABLE_STORE_CG_IETRATOR(context, param.cg_idx_, ObDefaultCGScanner, cg_scanner);
    }
  } else if (param.enable_pd_group_by() && is_projector) {
    ALLOCATE_TABLE_STORE_CG_IETRATOR(context, param.cg_idx_, ObCGGroupByScanner, cg_scanner);
  } else if (param.enable_pd_aggregate()) {
    ALLOCATE_TABLE_STORE_CG_IETRATOR(context, param.cg_idx_, ObCGAggregatedScanner, cg_scanner);
  } else if (is_projector) {
    ALLOCATE_TABLE_STORE_CG_IETRATOR(context, param.cg_idx_, ObCGRowScanner, cg_scanner);
  } else {
    ALLOCATE_TABLE_STORE_CG_IETRATOR(context, param.cg_idx_, ObCGScanner, cg_scanner);
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(cg_scanner)) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Fail to alloc cg scanner", K(ret));
    } else if (cg_scanner->is_valid()) {
      if (OB_UNLIKELY(param.cg_idx_ != cg_scanner->get_cg_idx())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected cg scanner", K(ret), K(param.cg_idx_), K(cg_scanner->get_cg_idx()));
      } else if (OB_FAIL(cg_scanner->switch_context(param, context, table_wrapper))) {
        LOG_WARN("Fail to switch context for cg scanner", K(ret));
      }
    } else if (OB_FAIL(cg_scanner->init(param, context, table_wrapper))) {
      LOG_WARN("Fail to init cg scanner", K(ret));
    }
    if (OB_SUCC(ret)) {
      cg_iter = cg_scanner;
    } else {
      if (nullptr != cg_scanner) {
        cg_scanner->~ObICGIterator();
        FREE_TABLE_STORE_CG_IETRATOR(context, cg_scanner);
      }
    }
  }
  return ret;
}

int ObCOSSTableV2::get(
    const storage::ObTableIterParam &param,
    storage::ObTableAccessContext &context,
    const blocksstable::ObDatumRowkey &rowkey,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_cs_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("SSTable is not ready for accessing", K(ret), K_(valid_for_reading), K_(meta));
  } else if (OB_UNLIKELY(!param.is_valid() || param.tablet_id_ != key_.tablet_id_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Tablet id is not match", K(ret), K(*this), K(param));
  } else if (param.read_info_->is_access_rowkey_only() || is_all_cg_base()) {
    if (OB_FAIL(ObSSTable::get(param, context, rowkey, row_iter))) {
      LOG_WARN("Fail to scan in row store sstable", K(ret));
    }
  } else if (OB_UNLIKELY(!context.is_valid() || !rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param), K(context), K(rowkey));
  } else {
    ObStoreRowIterator *row_getter = nullptr;
    ALLOCATE_TABLE_STORE_ROW_IETRATOR(context, ObCOSSTableRowGetter, row_getter);
    if (OB_SUCC(ret) && OB_NOT_NULL(row_getter) && OB_FAIL(row_getter->init(param, context, this, &rowkey))) {
      LOG_WARN("Fail to open row scanner", K(ret), K(param), K(context), K(rowkey), K(*this));
    }

    if (OB_FAIL(ret)) {
      if (nullptr != row_getter) {
        row_getter->~ObStoreRowIterator();
        FREE_TABLE_STORE_ROW_IETRATOR(context, row_getter);
        row_getter = nullptr;
      }
    } else {
      row_iter = row_getter;
    }
  }
  return ret;
}

int ObCOSSTableV2::multi_get(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const common::ObIArray<blocksstable::ObDatumRowkey> &rowkeys,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_cs_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("SSTable is not ready for accessing", K(ret), K_(valid_for_reading), K_(meta));
  } else if (OB_UNLIKELY(param.tablet_id_ != key_.tablet_id_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("Tablet id is not match", K(ret), K(*this), K(param));
  } else if (OB_UNLIKELY(!param.is_valid() || !context.is_valid() || 0 >= rowkeys.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(param), K(context), K(rowkeys));
  } else if (param.read_info_->is_access_rowkey_only() || is_all_cg_base()) {
    if (OB_FAIL(ObSSTable::multi_get(param, context, rowkeys, row_iter))) {
      LOG_WARN("Fail to scan in row store sstable", K(ret));
    }
  } else {
    ObStoreRowIterator *row_getter = nullptr;
    ALLOCATE_TABLE_STORE_ROW_IETRATOR(context, ObCOSSTableRowMultiGetter, row_getter);
    if (OB_SUCC(ret) && OB_NOT_NULL(row_getter) && OB_FAIL(row_getter->init(param, context, this, &rowkeys))) {
      LOG_WARN("Fail to open row scanner", K(ret), K(param), K(context), K(rowkeys), K(*this));
    }

    if (OB_FAIL(ret)) {
      if (nullptr != row_getter) {
        row_getter->~ObStoreRowIterator();
        FREE_TABLE_STORE_ROW_IETRATOR(context, row_getter);
        row_getter = nullptr;
      }
    } else {
      row_iter = row_getter;
    }
  }
  return ret;
}

} /* storage */
} /* oceanbase */
