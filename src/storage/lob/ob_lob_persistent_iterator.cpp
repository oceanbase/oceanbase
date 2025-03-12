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

#include "ob_lob_persistent_iterator.h"
#include "storage/access/ob_table_scan_iterator.h"

namespace oceanbase
{
namespace storage
{

int ObLobMetaBaseIterator::build_rowkey_range(ObLobAccessParam &param, ObRowkey &min_row_key, ObRowkey &max_row_key, ObNewRange &range)
{
  int ret = OB_SUCCESS;
  range.table_id_ = 0; // make sure this is correct
  range.start_key_ = min_row_key;
  range.end_key_ = max_row_key;
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  return ret;
}

int ObLobMetaBaseIterator::build_rowkey_range(ObLobAccessParam &param, ObObj key_objs[4], ObNewRange &range)
{
  const char *lob_id_ptr = reinterpret_cast<char*>(&param.lob_data_->id_);
  key_objs[0].reset();
  key_objs[0].set_varchar(lob_id_ptr, sizeof(ObLobId)); // lob_id
  key_objs[0].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  key_objs[1] = ObObj::make_min_obj(); // seq_id set min
  ObRowkey min_row_key(key_objs, 2);

  key_objs[2].reset();
  key_objs[2].set_varchar(lob_id_ptr, sizeof(ObLobId)); // lob_id
  key_objs[2].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  key_objs[3] = ObObj::make_max_obj(); // seq_id set max
  ObRowkey max_row_key(key_objs + 2, 2);
  return build_rowkey_range(param, min_row_key, max_row_key, range);
}

int ObLobMetaBaseIterator::build_rowkey(ObLobAccessParam &param, ObObj key_objs[4], ObString &seq_id, ObNewRange &range)
{
  int ret = OB_SUCCESS;
  const char *lob_id_ptr = reinterpret_cast<char*>(&param.lob_data_->id_);
  key_objs[0].reset();
  key_objs[0].set_varchar(lob_id_ptr, sizeof(ObLobId)); // lob_id
  key_objs[0].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  key_objs[1].set_varchar(seq_id); // seq_id set min
  key_objs[1].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  ObRowkey min_row_key(key_objs, 2);

  key_objs[2].reset();
  key_objs[2].set_varchar(lob_id_ptr, sizeof(ObLobId)); // lob_id
  key_objs[2].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  key_objs[3].set_varchar(seq_id); // seq_id set max
  key_objs[3].set_collation_type(common::ObCollationType::CS_TYPE_BINARY);
  ObRowkey max_row_key(key_objs + 2, 2);

  return build_rowkey_range(param, min_row_key, max_row_key, range);
}

int ObLobMetaBaseIterator::build_rowkey(ObLobAccessParam &param, ObObj key_objs[4], ObNewRange &range)
{
  int ret = OB_SUCCESS;
  ObString seq_id(sizeof(seq_id_local_buf_), (char*)(&seq_id_local_buf_));
  if (OB_FAIL(ObLobSeqId::get_seq_id(0, seq_id))) {
    LOG_WARN("get_seq_id failed.", K(ret), K(seq_id), K_(seq_id_local_buf));
  } else if (OB_FAIL(build_rowkey(param, key_objs, seq_id, range))) {
    LOG_WARN("build_rowkey_range fail", K(ret), K(seq_id), K_(seq_id_local_buf), K(param));
  }
  return ret;
}

int ObLobMetaBaseIterator::build_range(ObLobAccessParam &param, ObObj key_objs[4], ObNewRange &range)
{
  int ret = OB_SUCCESS;
  if (param.has_single_chunk()) {
    if (OB_FAIL(build_rowkey(param, key_objs, range))) {
      LOG_WARN("build_rowkey fail", K(ret));
    }
  } else if (OB_FAIL(build_rowkey_range(param, key_objs, range))) {
    LOG_WARN("build_rowkey_range fail", K(ret));
  }
  return ret;
}

int ObLobMetaBaseIterator::revert_scan_iter()
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = nullptr;
  if (OB_ISNULL(row_iter_)) { // skip when is null
  } else if (OB_ISNULL(oas = MTL(ObAccessService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get access service fail", K(ret));
  } else if (OB_FALSE_IT(row_iter_->reset())) {
  } else if (OB_FAIL(oas->revert_scan_iter(row_iter_))) {
    LOG_ERROR("revert scan iterator failed", K(ret), KPC(row_iter_));
  } else {
    row_iter_ = nullptr;
    LOG_DEBUG("release lob meta tablet scan iter success", K_(main_tablet_id), K_(lob_meta_tablet_id), K_(lob_piece_tablet_id));
  }
  return ret;
}

int ObLobMetaBaseIterator::scan(ObLobAccessParam &param, const bool is_get, ObIAllocator *scan_allocator)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService*);
  if (OB_ISNULL(oas)) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_ERROR("access service is null", K(ret), K(param), KPC(this));
  } else if (OB_FAIL(adaptor_->prepare_lob_tablet_id(param))) {
    LOG_WARN("prepare_lob_tablet_id fail", K(ret));
  } else if (OB_FAIL(adaptor_->prepare_scan_param_schema_version(param, scan_param_))) {
    LOG_WARN("prepare_lob_tablet_id fail", K(ret));
  } else if (OB_FAIL(adaptor_->prepare_table_scan_param(param, is_get, scan_param_, scan_allocator))) {
    LOG_WARN("build common scan param fail", K(ret), K(is_get), K(param), KPC(this));
  } else if (OB_FAIL(oas->table_scan(scan_param_, row_iter_))) {
    LOG_WARN("do table scan fail", K(ret), K(is_get), K(param), KPC(this));
  } else {
    main_tablet_id_ = param.tablet_id_;
    lob_meta_tablet_id_ = param.lob_meta_tablet_id_;
    lob_piece_tablet_id_ = param.lob_piece_tablet_id_;
    LOG_DEBUG("scan lob meta table sucess",  K_(main_tablet_id),
        K_(lob_meta_tablet_id), K_(lob_piece_tablet_id),
        K(is_get), K(param), KPC(this));
  }
  return ret;
}

int ObLobMetaBaseIterator::rescan(ObLobAccessParam &param)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService*);
  if (param.tablet_id_ != main_tablet_id_ || param.lob_meta_tablet_id_ != lob_meta_tablet_id_ || param.lob_piece_tablet_id_ != lob_piece_tablet_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet_id not match", K(ret), K(param), KPC(this));
  } else if (! main_tablet_id_.is_valid() || ! lob_meta_tablet_id_.is_valid() || ! lob_piece_tablet_id_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet_id is invalid", K(ret), K(param), KPC(this));
  } else if (OB_ISNULL(oas)) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_ERROR("access service is null", K(ret), K(param), KPC(this));
  } else if (OB_FAIL(oas->reuse_scan_iter(false/*tablet id same*/, row_iter_))) {
    LOG_WARN("reuse scan iter fail", K(ret), KPC(this), K(param));
  } else if (OB_FAIL(oas->table_rescan(scan_param_, row_iter_))) {
    LOG_WARN("do table rescan fail", K(ret), K(param), KPC(this));
  } else {
    LOG_DEBUG("rescan lob meta table sucess", K(param), KPC(this));
  }
  return ret;
}

int ObLobMetaIterator::open(ObLobAccessParam &param, ObPersistentLobApator* adaptor, ObIAllocator *scan_allocator)
{
  int ret = OB_SUCCESS;
  ObNewRange range;
  adaptor_ = adaptor;
  if (! param.tablet_id_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablet_id invalid", KR(ret), K(param));
  } else if (OB_FAIL(build_range(param, rowkey_objs_, range))) {
    LOG_WARN("build_range fail", KR(ret), K(param));
  } else if (OB_FAIL(scan_param_.key_ranges_.push_back(range))) {
    LOG_WARN("push key range fail", K(ret), K(scan_param_), K(range), K(param));
  } else if (OB_FAIL(scan(param, param.has_single_chunk(), scan_allocator))) {
    LOG_WARN("scan fail", K(ret), K(param), KPC(this));
  }
  return ret;
}

int ObLobMetaIterator::rescan(ObLobAccessParam &param)
{
  int ret = OB_SUCCESS;
  ObNewRange range;
  param.lob_meta_tablet_id_ =  lob_meta_tablet_id_;
  param.lob_piece_tablet_id_ =  lob_piece_tablet_id_;
  scan_param_.key_ranges_.reuse();

  // update scan order
  scan_param_.scan_flag_.scan_order_ = param.scan_backward_ ? ObQueryFlag::Reverse : ObQueryFlag::Forward;
  // update timeout
  scan_param_.timeout_ = param.timeout_;
  scan_param_.for_update_wait_timeout_ = scan_param_.timeout_;

  if (OB_FAIL(build_range(param, rowkey_objs_, range))) {
    LOG_WARN("build_range fail", K(ret), K(param), KPC(this));
  } else if (OB_FAIL(scan_param_.key_ranges_.push_back(range))) {
    LOG_WARN("push key range fail", K(ret), K(scan_param_), K(range));
  } else if (OB_FAIL(ObLobMetaBaseIterator::rescan(param))) {
    LOG_WARN("rescan fail", K(ret), K(param), KPC(this));
  }
  return ret;
}

int ObLobMetaIterator::get_next_row(ObLobMetaInfo &row)
{
  int ret = OB_SUCCESS;
  blocksstable::ObDatumRow* datum_row = nullptr;
  ObTableScanIterator *table_scan_iter = static_cast<ObTableScanIterator *>(row_iter_);
  if (OB_ISNULL(row_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob meta scan iter is null", K(ret), KPC(this));
  } else if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("get next lob meta row fail", K(ret), KPC(this));
    }
  } else if(OB_ISNULL(datum_row)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("lob meta row is null", K(ret), KPC(this));
  } else if (OB_FAIL(ObLobMetaUtil::transform_from_row_to_info(datum_row, row, false))) {
    LOG_WARN("get meta info from row fail", K(ret), K(datum_row), KPC(this));
  }
  return ret;
}

int ObLobMetaIterator::reset()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(revert_scan_iter())) {
    LOG_WARN("revert_scan_iter fail", K(ret), KPC(this));
  }
  return ret;
}

int ObLobMetaSingleGetter::reset()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(revert_scan_iter())) {
    LOG_WARN("revert_scan_iter fail", K(ret), KPC(this));
  }
  return ret;
}

int ObLobMetaSingleGetter::open(ObLobAccessParam &param, ObPersistentLobApator* adaptor)
{
  int ret = OB_SUCCESS;
  adaptor_ = adaptor;
  param_ = &param;
  return ret;
}

int ObLobMetaSingleGetter::get_next_row(ObString &seq_id, ObLobMetaInfo &info)
{
  int ret = OB_SUCCESS;
  blocksstable::ObDatumRow* row = nullptr;
  common::ObNewRange range;
  ObTableScanIterator *table_scan_iter = nullptr;
  scan_param_.key_ranges_.reuse();
  if (OB_ISNULL(param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob access param is null", K(ret), KPC(this));
  } else if (OB_FAIL(build_rowkey(*param_, rowkey_objs_, seq_id, range))) {
    LOG_WARN("build_rowkey_range fail", K(ret), KPC(this));
  } else if (OB_FAIL(scan_param_.key_ranges_.push_back(range))) {
    LOG_WARN("failed to push key range.", K(ret), K(range), KPC(this));
  } else if (OB_NOT_NULL(row_iter_)) {
    if (OB_FAIL(rescan(*param_))) {
      LOG_WARN("rescan fali", K(ret), K(range), KPC(this));
    }
  } else if (OB_FAIL(scan(*param_, true /*is_get*/, param_->allocator_))) {
    LOG_WARN("scan fali", K(ret), K(range), KPC(this));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(table_scan_iter = static_cast<ObTableScanIterator *>(row_iter_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_scan_iter is null", K(ret), KPC(this));
  } else if (OB_FAIL(table_scan_iter->get_next_row(row))) {
    LOG_WARN("get next row fail.", K(ret), K(range), KPC(this));
  } else if (OB_FAIL(ObLobMetaUtil::transform_from_row_to_info(row, info, false))) {
    LOG_WARN("transform row fail", K(ret), K(range), KPC(this), KPC(row));
  }

  if (OB_FAIL(ret)) {
    revert_scan_iter();
  }
  return ret;
}

int ObLobMetaSingleGetter::get_next_row(int idx, ObLobMetaInfo &info)
{
  int ret = OB_SUCCESS;
  seq_id_local_buf_ = 0;
  ObString seq_id(sizeof(seq_id_local_buf_), (char*)(&seq_id_local_buf_));
  if (OB_FAIL(ObLobSeqId::get_seq_id(idx, seq_id))) {
    LOG_WARN("get_seq_id failed.", K(ret), K(seq_id), K_(seq_id_local_buf));
  } else if (OB_FAIL(get_next_row(seq_id, info))) {
    LOG_WARN("get_next_row fail", K(ret), K(seq_id), K_(seq_id_local_buf), KPC(this));
  } else {
    LOG_TRACE("get success", K_(seq_id_local_buf), K(idx), K(info), K(seq_id));
  }
  return ret;
}

int ObLobPersistWriteIter::update_seq_no()
{
  int ret = OB_SUCCESS;
  if (param_->seq_no_st_.is_valid()) {
    if (param_->used_seq_cnt_ < param_->total_seq_cnt_) {
      param_->dml_base_param_->spec_seq_no_ = param_->seq_no_st_ + param_->used_seq_cnt_;
      param_->used_seq_cnt_++;
      LOG_DEBUG("dml lob meta with seq no", K(param_->dml_base_param_->spec_seq_no_));
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to get seq no from param.", K(ret), KPC(param_));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid seq no from param.", K(ret), KPC(param_));
  }
  return ret;
}

int ObLobPersistUpdateSingleRowIter::init(ObLobAccessParam *param, blocksstable::ObDatumRow *old_row, blocksstable::ObDatumRow *new_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_ISNULL(old_row) || OB_ISNULL(new_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param or row is null", K(ret), KP(param), KP(old_row), KP(new_row));
  } else {
    param_ = param;
    old_row_ = old_row;
    new_row_ = new_row;
  }
  return ret;
}

int ObLobPersistUpdateSingleRowIter::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(old_row_) || OB_ISNULL(new_row_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("get next row failed, null val.", K(ret), K(old_row_), K(new_row_));
  } else if (is_iter_end_) {
    ret = OB_ITER_END;
  } else if (!got_old_row_) {
    row = old_row_;
    got_old_row_ = true;
  } else if (OB_FAIL(update_seq_no())) {
    LOG_WARN("update_seq_no fail", K(ret));
  } else {
    row = new_row_;
    got_old_row_ = false;
    is_iter_end_ = true;
  }
  return ret;
}

int ObLobPersistInsertSingleRowIter::init(ObLobAccessParam *param, blocksstable::ObDatumRow *row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param or row is null", K(ret), KP(param), KP(row));
  } else {
    param_ = param;
    row_ = row;
  }
  return ret;
}

int ObLobPersistInsertSingleRowIter::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_) || OB_ISNULL(row_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("param or row is null", K(ret), KP(param_), KP(row_));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(update_seq_no())) {
    LOG_WARN("update_seq_no fail", K(ret));
  } else {
    row = row_;
    iter_end_ = true;
  }
  return ret;
}

int ObLobPersistDeleteSingleRowIter::init(ObLobAccessParam *param, blocksstable::ObDatumRow *row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_ISNULL(row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param or row is null", K(ret), KP(param), KP(row));
  } else {
    param_ = param;
    row_ = row;
  }
  return ret;
}

int ObLobPersistDeleteSingleRowIter::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param_) || OB_ISNULL(row_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("param or row is null", K(ret), KP(param_), KP(row_));
  } else if (iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(update_seq_no())) {
    LOG_WARN("update_seq_no fail", K(ret));
  } else {
    row = row_;
    iter_end_ = true;
  }
  return ret;
}

int ObLobPersistInsertIter::init(ObLobAccessParam *param, ObLobMetaWriteIter *meta_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_ISNULL(meta_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param or meta_iter is null", K(ret), KP(param), KP(meta_iter));
  } else if (OB_FAIL(new_row_.init(ObLobMetaUtil::LOB_META_COLUMN_CNT))) {
    LOG_WARN("init new datum row failed", K(ret));
  } else {
    param_ = param;
    meta_iter_ = meta_iter;
  }
  return ret;
}

int ObLobPersistInsertIter::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(meta_iter_->get_next_row(result_))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("get next meta info failed.", K(ret));
    }
  } else if (OB_FALSE_IT(result_.info_.lob_data_.assign_ptr(result_.data_.ptr(), result_.data_.length()))) {
  } else if (! param_->is_store_char_len_ && result_.info_.char_len_ != UINT32_MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("char length invalid", K(ret), K(result_.info_), KPC(param_));
  } else if (0 == result_.info_.byte_len_ || result_.info_.byte_len_ != result_.info_.lob_data_.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("byte length invalid", K(ret), K(result_.info_));
  } else if (OB_FAIL(update_seq_no())) {
    LOG_WARN("update_seq_no fail", K(ret));
  } else if (OB_FAIL(param_->update_out_row_ctx(nullptr/*old_info*/, result_.info_/*new_info*/))) { // new row
    LOG_WARN("failed update checksum.", K(ret));
  } else if (OB_FAIL(param_->update_handle_data_size(nullptr/*old_info*/, &result_.info_/*new_info*/))) {
    LOG_WARN("inc_handle_data_size fail", K(ret));
  } else {
    ObPersistentLobApator::set_lob_meta_row(new_row_, result_.info_);
    row = &new_row_;
    LOG_TRACE("insert one lob meta row", K(new_row_), K(param_->dml_base_param_->spec_seq_no_));
  }
  return ret;
}

int ObLobPersistDeleteIter::init(ObLobAccessParam *param, ObLobMetaScanIter *meta_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param) || OB_ISNULL(meta_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param or meta_iter is null", K(ret), KP(param), KP(meta_iter));
  } else if (OB_FAIL(new_row_.init(ObLobMetaUtil::LOB_META_COLUMN_CNT))) {
    LOG_WARN("init new datum row failed", K(ret));
  } else {
    param_ = param;
    meta_iter_ = meta_iter;
  }
  return ret;
}

int ObLobPersistDeleteIter::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(meta_iter_->get_next_row(result_))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("get next meta info failed.", K(ret));
    }
  } else if (FALSE_IT(result_.info_.char_len_ = meta_iter_->get_cur_info().char_len_)) { // get ori char len
  } else if (OB_FAIL(update_seq_no())) {
    LOG_WARN("update_seq_no fail", K(ret));
  // TODO aozeliu.azl old_info is null, may be incorrect
  } else if (OB_FAIL(param_->update_out_row_ctx(nullptr/*old_info*/, result_.info_/*new_info*/))) { // new row
    LOG_WARN("failed update checksum.", K(ret));
  } else if (OB_FAIL(param_->update_handle_data_size(&result_.info_/*old_info*/, nullptr/*old_info*/))) {
    LOG_WARN("dec_handle_data_size fail", K(ret));
  } else {
    ObPersistentLobApator::set_lob_meta_row(new_row_, result_.info_);
    row = &new_row_;
    LOG_TRACE("delete one lob meta row", K(new_row_), K(param_->dml_base_param_->spec_seq_no_));
  }
  return ret;
}

int ObLobSimplePersistInsertIter::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(new_row_.init(ObLobMetaUtil::LOB_META_COLUMN_CNT))) {
    LOG_WARN("init new datum row failed", K(ret));
  }
  return ret;
}

int ObLobSimplePersistInsertIter::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (pos_ >= lob_meta_list_.count()) {
    ret = OB_ITER_END;
  } else {
    ObLobMetaInfo &info = lob_meta_list_[pos_];
    ObString cur_seq_id;
    if (OB_FAIL(seq_id_.get_next_seq_id(cur_seq_id))) {
      LOG_WARN("get_next_seq_id fail", K(ret));
    } else if (OB_FALSE_IT(info.lob_id_ = param_->lob_data_->id_)) {
    } else if (OB_FALSE_IT(info.seq_id_ = cur_seq_id)) {
    } else if (OB_FAIL(update_seq_no())) {
      LOG_WARN("update_seq_no fail", K(ret));
    } else if (OB_FAIL(param_->update_out_row_ctx(nullptr/*old_info*/, info/*new_info*/))) { // new row
      LOG_WARN("failed update checksum.", K(ret));
    } else if (OB_FAIL(param_->update_handle_data_size(nullptr/*old_info*/, &info/*new_info*/))) {
      LOG_WARN("dec_handle_data_size fail", K(ret));
    } else {
      ++pos_;
      ObPersistentLobApator::set_lob_meta_row(new_row_, info);
      row = &new_row_;
      LOG_TRACE("row", K(info));
    }
  }
  return ret;
}

} // storage
} // oceanbase
