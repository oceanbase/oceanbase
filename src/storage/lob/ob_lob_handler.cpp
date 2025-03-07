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
#include "ob_lob_handler.h"
#include "storage/lob/ob_lob_persistent_iterator.h"
#include "storage/lob/ob_lob_retry.h"
#include "storage/lob/ob_lob_iterator.h"
#include "storage/lob/ob_lob_write_buffer.h"

namespace oceanbase
{
namespace storage
{

/*************ObLobQueryBaseHandler*****************/

int ObLobQueryBaseHandler::init_base(ObLobMetaManager *lob_meta_mngr)
{
  int ret = OB_SUCCESS;
  if (param_.lob_locator_ != nullptr && !param_.lob_locator_->is_persist_lob()) {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("Unsupport outrow tmp lob.", K(ret), K(param_));
  } else if (!param_.lob_common_->is_init_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob common header for out row.", K(ret), KPC(param_.lob_common_));
  } else if (OB_ISNULL(param_.lob_data_ = reinterpret_cast<ObLobData*>(param_.lob_common_->buffer_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob data is null", K(ret), K(param_));
  } else {
    lob_meta_mngr_ = lob_meta_mngr;
  }
  return ret;
}

int ObLobQueryBaseHandler::execute()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t retry_cnt = 0;
  bool is_continue = true;
  oceanbase::lib::Thread::WaitGuard guard(oceanbase::lib::Thread::WAIT_FOR_LOCAL_RETRY);
  do {
    if (OB_FAIL(do_execute())) {
      LOG_WARN("do_execute fail, check need rerty", KR(ret), K(retry_cnt), K(param_));
      is_continue = false;
      if (param_.no_need_retry_) {
        LOG_INFO("no need retry", K(ret), K(is_continue), K(retry_cnt), K(param_));
        is_continue = false;
      } else if (OB_ISNULL(param_.lob_locator_)) {
        LOG_INFO("lob locator is null, so can not retry", K(ret), K(is_continue), K(retry_cnt), K(param_));
        is_continue = false;
      } else if (OB_TMP_FAIL(ObLobRetryUtil::check_need_retry(param_, ret, retry_cnt, is_continue))) {
        LOG_WARN("check_need_retry fail", K(tmp_ret), K(ret), K(is_continue), K(retry_cnt), K(param_));
        // check fail, do not retry
        is_continue = false;
      }
    } else {
      is_continue = false;
    }
    if (is_continue) {
      ++retry_cnt;
      LOG_INFO("[LOB RETRY] retry again", K(ret), K(retry_cnt), K(param_), K(param_.allocator_->total()), K(param_.allocator_->used()));
    }
  } while (is_continue);
  return ret;
}

/*************ObLobQueryIterHandler*****************/
int ObLobQueryIterHandler::init(ObLobMetaManager *lob_meta_mngr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_base(lob_meta_mngr))) {
    LOG_WARN("init_base fail", K(ret), K(param_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLobQueryIterHandler::do_execute()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler not init", K(ret));
  } else if (OB_ISNULL(result_ = OB_NEW(ObLobOutRowQueryIter, ObMemAttr(MTL_ID(), "LobQueryIter")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc lob meta scan iterator fail", K(ret));
  } else if (OB_FAIL(result_->open(param_, lob_meta_mngr_))) {
    LOG_WARN("open local meta scan iter failed", K(ret), K(param_));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(result_)) {
    result_->reset();
    OB_DELETE(ObLobOutRowQueryIter, "unused", result_);
    result_ = nullptr;
  }
  return ret;
}

/*************ObLobQueryDataHandler*****************/
int ObLobQueryDataHandler::init(ObLobMetaManager *lob_meta_mngr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_base(lob_meta_mngr))) {
    LOG_WARN("init_base fail", K(ret), K(param_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLobQueryDataHandler::do_execute()
{
  int ret = OB_SUCCESS;
  ObLobMetaScanIter meta_iter;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler not init", K(ret));
  } else if (OB_FAIL(lob_meta_mngr_->scan(param_, meta_iter))) {
    LOG_WARN("fail to do lob query with retry", K(ret));
  } else if (param_.is_full_read()) {
    meta_iter.set_not_calc_char_len(true);
    meta_iter.set_not_need_last_info(true);
  }

  if (OB_SUCC(ret)) {
    ObString block_data;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(meta_iter.get_next_row(block_data))) {
        if (ret != OB_ITER_END) {
          LOG_WARN("failed to get next row.", K(ret), K(param_));
        }
      } else if (OB_FAIL(param_.is_timeout())) {
        LOG_WARN("query timeout", K(ret), K(param_));
      } else if (OB_FAIL(write_data_to_buffer(result_, block_data))) {
        LOG_WARN("write_data_to_buffer fail", K(ret), K(param_), K(meta_iter), K(result_), K(block_data));
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
  }

  if (OB_SUCC(ret) && param_.is_full_read() && param_.byte_size_ != result_.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read size not macth", K(ret), "current_length", result_.length(), "data_length", param_.byte_size_, K(param_), K(result_));
  }
  return ret;
}

int ObLobQueryDataHandler::write_data_to_buffer(ObString &buffer, ObString &data)
{
  int ret = OB_SUCCESS;
  if (param_.scan_backward_ && buffer.write_front(data.ptr(), data.length()) != data.length()) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("failed to write buffer to output_data.", K(ret), K(param_),
              K(buffer.length()), K(buffer.remain()), K(data));
  } else if (!param_.scan_backward_ && buffer.write(data.ptr(), data.length()) != data.length()) {
    ret = OB_ERR_INTERVAL_INVALID;
    LOG_WARN("failed to write buffer to output_data.", K(ret), K(param_),
              K(buffer.length()), K(buffer.remain()), K(data.length()));
  }
  return ret;
}

/*************ObLobQueryLengthHandler*****************/
int ObLobQueryLengthHandler::init(ObLobMetaManager *lob_meta_mngr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_base(lob_meta_mngr))) {
    LOG_WARN("init_base fail", K(ret), K(param_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLobQueryLengthHandler::do_execute()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler not init", K(ret));
  } else {
    param_.offset_ = 0;
    param_.len_ = UINT64_MAX;
    if (OB_FAIL(lob_meta_mngr_->getlength(param_, result_))) {
      LOG_WARN("fail to get length", K(ret));
    }
  }
  return ret;
}

/*************ObLobWriteBaseHandler*****************/
int ObLobWriteBaseHandler::init_base(ObLobMetaManager *lob_meta_mngr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(lob_meta_mngr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lob meta mngr is null", K(ret));
  } else if (OB_ISNULL(param_.lob_common_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("lob common is null", K(ret), K(param_));
  } else if (! param_.lob_common_->is_init_) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("lob common is not init", K(ret), K(param_));
  } else if (param_.lob_locator_ != nullptr && ! param_.lob_locator_->is_persist_lob()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invlaid lob locator", K(ret), K(param_));
  } else if (OB_ISNULL(param_.lob_data_ = reinterpret_cast<ObLobData*>(param_.lob_common_->buffer_))) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("lob data is null", K(ret), K(param_));
  } else if (OB_FAIL(param_.get_store_chunk_size(store_chunk_size_))) {
    LOG_WARN("get_store_chunk_size fail", K(ret), K(param_));
  } else {
    lob_meta_mngr_ = lob_meta_mngr;
  }
  return ret;
}

/*************ObLobFullInsertHandler*****************/
int ObLobFullInsertHandler::init(ObLobMetaManager *lob_meta_mngr)
{
  int ret = OB_SUCCESS;
  if (! param_.is_full_insert()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not full insert situation", K(ret), K(param_));
  } else if (OB_FAIL(init_base(lob_meta_mngr))) {
    LOG_WARN("init_base fail", KR(ret), K(param_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLobFullInsertHandler::do_insert(ObLobMetaWriteIter &iter)
{
  int ret = OB_SUCCESS;
  ObLobPersistInsertIter insert_iter;
  if (OB_FAIL(insert_iter.init(&param_, &iter))) {
    LOG_WARN("init insert iter fail", K(ret), K(param_));
  } else if (OB_FAIL(lob_meta_mngr_->batch_insert(param_, insert_iter))) {
    LOG_WARN("bach insert fail", K(ret), K(param_));
  }
  return ret;
}

int ObLobFullInsertHandler::execute(ObString &data)
{
  int ret = OB_SUCCESS;
  ObLobMetaWriteIter iter(param_.get_tmp_allocator(), store_chunk_size_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler not init", K(ret));
  } else if (OB_ISNULL(param_.get_tmp_allocator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param allocator is null", K(ret), K(param_));
  } else if (OB_FAIL(param_.init_out_row_ctx(data.length()))) {
    LOG_WARN("init lob data out row ctx failed", K(ret));
  } else if (OB_FAIL(iter.open(param_, data))) {
    LOG_WARN("Failed to open lob meta write iter.", K(ret), K(param_));
  } else if (OB_FAIL(do_insert(iter))) {
    LOG_WARN("do insert fail", K(ret), K(param_));
  }
  return ret;
}

int ObLobFullInsertHandler::execute(ObLobQueryIter *iter, int64_t append_lob_len, ObString& ori_inrow_data)
{
  int ret = OB_SUCCESS;
  bool need_get_last_info = ! (ori_inrow_data.length() > 0 || param_.byte_size_ == 0);
  ObString read_buffer;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler not init", K(ret));
  } else if (OB_ISNULL(param_.get_tmp_allocator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param tmp allocator is null", K(ret), K(param_));
  } else if (OB_FAIL(param_.init_out_row_ctx(append_lob_len))) {
    LOG_WARN("init lob data out row ctx failed", K(ret));
  } else if(OB_FAIL(prepare_data_buffer(param_, read_buffer, store_chunk_size_))) {
    LOG_WARN("fail to prepare reaed buffer", K(ret), K(param_));
  // } else if (ori_inrow_data.length() != 0) {
  //   ret = OB_INVALID_ARGUMENT;
  //   LOG_WARN("invlaid", K(ret), K(param_));
  } else {
    ObLobMetaWriteIter write_iter(param_.get_tmp_allocator(), store_chunk_size_);
    ObString remain_buf;
    ObString seq_id_st;
    ObString seq_id_ed;
    if (OB_FAIL(write_iter.open(
        param_, iter, read_buffer, 0/*padding_size*/,
        ori_inrow_data, remain_buf, seq_id_st, seq_id_ed, nullptr))) {
      LOG_WARN("failed to open meta writer", K(ret), K(param_), K(write_iter));
    } else if (OB_FAIL(do_insert(write_iter))) {
      LOG_WARN("do insert fail", K(ret), K(param_));
    }
  }
  return ret;
}

/*************ObLobAppendHandler*****************/
int ObLobAppendHandler::init(ObLobMetaManager *lob_meta_mngr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_base(lob_meta_mngr))) {
    LOG_WARN("init_base fail", KR(ret), K(param_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLobAppendHandler::execute(ObString &data, bool ori_is_inrow)
{
  int ret = OB_SUCCESS;
  ObLobPersistInsertIter insert_iter;
  ObLobMetaWriteIter iter(param_.get_tmp_allocator(), store_chunk_size_);
  bool need_get_last_info = ! (ori_is_inrow || param_.byte_size_ == 0);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler not init", K(ret));
  } else if (OB_FAIL(param_.init_out_row_ctx(data.length()))) {
    LOG_WARN("init lob data out row ctx failed", K(ret));
  } else if (OB_FAIL(iter.open(param_, data, need_get_last_info ? lob_meta_mngr_ : nullptr))) {
    LOG_WARN("Failed to open lob meta write iter.", K(ret), K(param_));
  } else if (OB_FAIL(write_outrow_result(param_, iter))) {
    LOG_WARN("write_outrow_result fail", K(ret), K(param_));
  }
  return ret;
}

int ObLobAppendHandler::execute(
    ObLobQueryIter *iter,
    int64_t append_lob_len,
    ObString& ori_inrow_data)
{
  int ret = OB_SUCCESS;
  ObString read_buffer;
  bool need_get_last_info = ! (ori_inrow_data.length() > 0 || param_.byte_size_ == 0);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler not init", K(ret));
  } else if (OB_FAIL(param_.init_out_row_ctx(append_lob_len))) {
    LOG_WARN("init lob data out row ctx failed", K(ret), K(param_));
  } else if(OB_FAIL(prepare_data_buffer(param_, read_buffer, store_chunk_size_))) {
    LOG_WARN("fail to prepare reaed buffer", K(ret), K(param_));
  } else {
    ObLobMetaWriteIter write_iter(param_.get_tmp_allocator(), store_chunk_size_);
    ObString remain_buf;
    ObString seq_id_st;
    ObString seq_id_ed;
    if (OB_FAIL(write_iter.open(
        param_, iter, read_buffer, 0/*padding_size*/,
        ori_inrow_data, remain_buf, seq_id_st, seq_id_ed,
        need_get_last_info ? lob_meta_mngr_ : nullptr))) {
      LOG_WARN("failed to open meta writer", K(ret), K(param_), K(write_iter));
    } else if (OB_FAIL(write_outrow_result(param_, write_iter))) {
      LOG_WARN("failed to write outrow result", K(ret), K(param_), K(write_iter));
    }
  }
  return ret;
}

/*************ObLobFullDeleteHandler*****************/
int ObLobFullDeleteHandler::init(ObLobMetaManager *lob_meta_mngr)
{
  int ret = OB_SUCCESS;
  if (! param_.is_full_delete()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not full insert situation", K(ret), K(param_));
  } else if (OB_FAIL(init_base(lob_meta_mngr))) {
    LOG_WARN("init_base fail", KR(ret), K(param_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLobFullDeleteHandler::do_delete(ObLobMetaScanIter &iter)
{
  int ret = OB_SUCCESS;
  ObLobPersistDeleteIter delete_iter;
  iter.set_not_calc_char_len(true);
  if (OB_FAIL(delete_iter.init(&param_, &iter))) {
    LOG_WARN("init insert iter fail", K(ret));
  } else if (OB_FAIL(lob_meta_mngr_->batch_delete(param_, delete_iter))) {
    LOG_WARN("write lob meta row failed.", K(ret));
  }
  return ret;
}

int ObLobFullDeleteHandler::execute()
{
  int ret = OB_SUCCESS;
  ObLobMetaScanIter meta_iter;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler not init", K(ret));
  } else if (OB_FAIL(param_.init_out_row_ctx(param_.lob_data_->byte_size_))) {
    LOG_WARN("init lob data out row ctx failed", K(ret));
  } else if (OB_FAIL(lob_meta_mngr_->scan(param_, meta_iter))) {
    LOG_WARN("do lob meta scan failed.", K(ret), K(param_));
  } else if (OB_FAIL(do_delete(meta_iter))) {
    LOG_WARN("do insert fail", K(ret), K(param_));
  }
  return ret;
}

/*************ObLobEraseHandler*****************/
int ObLobEraseHandler::init(ObLobMetaManager *lob_meta_mngr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_base(lob_meta_mngr))) {
    LOG_WARN("init_base fail", KR(ret), K(param_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLobEraseHandler::execute()
{
  int ret = OB_SUCCESS;
  ObLobMetaScanIter meta_iter;
  ObString write_buf;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler not init", K(ret));
  } else if (OB_FAIL(param_.init_out_row_ctx(param_.lob_data_->byte_size_))) {
    LOG_WARN("init lob data out row ctx failed", K(ret));
  } else if (OB_FAIL(lob_meta_mngr_->scan(param_, meta_iter))) {
    LOG_WARN("do lob meta scan failed.", K(ret), K(param_));
  } else if(OB_FAIL(prepare_data_buffer(param_, write_buf, store_chunk_size_))) {
    LOG_WARN("fail to prepare buffers", K(ret), K(param_));
  } else {
    ObLobMetaScanResult result;
    while (OB_SUCC(ret)) {
      ret = meta_iter.get_next_row(result);
      if (OB_FAIL(ret)) {
        if (ret == OB_ITER_END) {
        } else {
          LOG_WARN("failed to get next row.", K(ret));
        }
      } else if (OB_FAIL(param_.is_timeout())) {
        LOG_WARN("access timeout", K(ret), K(param_));
      } else if (OB_FAIL(erase_process_meta_info(meta_iter, result, write_buf))) {
        LOG_WARN("process erase meta info failed.", K(ret), K(param_), K(result));
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLobEraseHandler::erase_process_meta_info(
    ObLobMetaScanIter &meta_iter,
    ObLobMetaScanResult &result,
    ObString &write_buf)
{
  int ret = OB_SUCCESS;
  bool is_char = param_.is_char();
  bool del_piece = false;

  uint64_t cur_piece_end = meta_iter.get_cur_pos();
  uint64_t cur_piece_begin = cur_piece_end - (is_char ? result.info_.char_len_ : result.info_.byte_len_);

  int64_t piece_write_begin = param_.offset_ > cur_piece_begin ? param_.offset_ - cur_piece_begin : 0;
  int64_t piece_write_end = param_.offset_ + param_.len_ > cur_piece_end ? cur_piece_end - cur_piece_begin : param_.offset_ + param_.len_ - cur_piece_begin;

  uint32_t piece_byte_len = result.info_.byte_len_;
  uint32_t piece_char_len = result.info_.char_len_;

  ObLobMetaInfo new_meta_row = result.info_;

  ObLobWriteBuffer buffer(param_.coll_type_, store_chunk_size_, param_.is_store_char_len_);

  if (piece_write_begin != result.st_ || piece_write_end - piece_write_begin != result.len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("offset invalid", K(ret), K(piece_write_begin), K(piece_write_begin), K(result));
  } else if (result.len_ == result.info_.char_len_) {
    del_piece = true;
    LOG_DEBUG("just delete", K(piece_write_begin), K(piece_write_end), K(piece_char_len));
  } else if (meta_iter.is_range_begin(result.info_)) {
    if (OB_FAIL(buffer.char_append(result.info_.lob_data_, 0/*char_offset*/, result.st_ /*char_len*/))) { // is_range_begin
      LOG_WARN("char_append fail", K(ret));
    }
  } else if (meta_iter.is_range_end(result.info_)) {
    // use buffer because need copy data
    if (OB_FAIL(buffer.set_buffer(write_buf.ptr(), write_buf.size()))) {
      LOG_WARN("set_buffer fail", K(ret));
    } else if (OB_FAIL(buffer.char_append(result.info_.lob_data_, result.st_, result.len_))) {
      LOG_WARN("char_append fail", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unkown piece fail", K(ret), K(result), K(cur_piece_begin), K(cur_piece_end), K(piece_write_begin), K(piece_write_end));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(buffer.to_lob_meta_info(new_meta_row))) {
    LOG_WARN("to_lob_meta_info fail", K(ret));
  } else if (del_piece) {
    if (OB_FAIL(erase_one_piece(param_, result.info_))) {
      LOG_WARN("failed erase one piece", K(ret), K(result),
          K(cur_piece_begin), K(cur_piece_end), K(piece_write_begin), K(piece_write_end), K(buffer));
    }
  } else if (OB_FAIL(update_one_piece(param_,
                                  result.info_,
                                  new_meta_row))) {
    LOG_WARN("failed to update.", K(ret), K(result), K(new_meta_row),
        K(cur_piece_begin), K(cur_piece_end), K(piece_write_begin), K(piece_write_end), K(buffer));
  }
  return ret;
}

/*************ObLobFillZeroHandler*****************/
int ObLobFillZeroHandler::init(ObLobMetaManager *lob_meta_mngr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_base(lob_meta_mngr))) {
    LOG_WARN("init_base fail", KR(ret), K(param_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLobFillZeroHandler::execute()
{
  int ret = OB_SUCCESS;
  ObLobMetaScanIter meta_iter;
  ObString write_data_buffer;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler not init", K(ret));
  } else if (OB_FAIL(param_.init_out_row_ctx(param_.lob_data_->byte_size_))) {
    LOG_WARN("init lob data out row ctx failed", K(ret));
  } else if (OB_FAIL(lob_meta_mngr_->scan(param_, meta_iter))) {
    LOG_WARN("do lob meta scan failed.", K(ret), K(param_));
  } else if(OB_FAIL(prepare_data_buffer(param_, write_data_buffer, ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE))) {
    LOG_WARN("fail to prepare buffers", K(ret), K(param_));
  } else {
    ObLobMetaScanResult result;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(meta_iter.get_next_row(result))) {
        if (ret == OB_ITER_END) {
        } else {
          LOG_WARN("failed to get next row.", K(ret));
        }
      } else if (OB_FAIL(param_.is_timeout())) {
        LOG_WARN("access timeout", K(ret), K(param_));
      } else if (OB_FAIL(do_fill_zero_outrow(meta_iter, result, write_data_buffer))) {
        LOG_WARN("process erase meta info failed.", K(ret), K(param_), K(result));
      }
    }
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLobFillZeroHandler::do_fill_zero_outrow(
    ObLobMetaScanIter &meta_iter,
    ObLobMetaScanResult &result,
    ObString &write_buf)
{
  int ret = OB_SUCCESS;
  bool is_char = param_.is_char();

  ObLobWriteBuffer buffer(param_.coll_type_, store_chunk_size_, param_.is_store_char_len_);

  int64_t cur_piece_end = meta_iter.get_cur_pos();
  int64_t cur_piece_begin = cur_piece_end - (is_char ? result.info_.char_len_ : result.info_.byte_len_);

  int64_t piece_write_begin = param_.offset_ > cur_piece_begin ? param_.offset_ - cur_piece_begin : 0;
  int64_t piece_write_end = param_.offset_ + param_.len_ > cur_piece_end ? cur_piece_end - cur_piece_begin : param_.offset_ + param_.len_ - cur_piece_begin;

  ObLobMetaInfo new_meta_row = result.info_;

  if (piece_write_begin != result.st_ || piece_write_end - piece_write_begin != result.len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("offset invalid", K(ret), K(piece_write_begin), K(piece_write_begin), K(result));
  } else if (OB_FAIL(buffer.set_buffer(write_buf.ptr(), write_buf.size()))) {
    LOG_WARN("set_buffer fail", K(ret));
  } else if (OB_FAIL(buffer.append(result.info_.lob_data_.ptr(), result.info_.lob_data_.length()))) {
    LOG_WARN("append fail", K(ret), K(param_));
  } else if (OB_FAIL(buffer.char_fill_zero(
      result.st_,
      result.len_))) {
    LOG_WARN("fill_zero empty", K(ret), K(result));
  } else if (OB_FAIL(buffer.to_lob_meta_info(new_meta_row))) {
    LOG_WARN("to_lob_meta_info fail", K(ret));
  } else if (OB_FAIL(update_one_piece(
        param_,
        result.info_,
        new_meta_row))) {
    LOG_WARN("update_one_piece fail", K(ret), K(result), K(new_meta_row),
        K(cur_piece_begin), K(cur_piece_end), K(piece_write_begin), K(piece_write_end));
  }
  return ret;
}

/*************ObLobWriteHandler*****************/
int ObLobWriteHandler::init(ObLobMetaManager *lob_meta_mngr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_base(lob_meta_mngr))) {
    LOG_WARN("init_base fail", KR(ret), K(param_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLobWriteHandler::execute(ObLobQueryIter *iter, ObString& read_buf, ObString& old_data)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler not init", K(ret));
  } else if (OB_ISNULL(param_.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param allocator is null", K(ret), K(param_));
  } else {
  SMART_VAR(ObLobMetaScanIter, meta_iter) {
    uint64_t modified_len = param_.len_;
    int64_t mbmaxlen = 1;
    if (param_.coll_type_ != CS_TYPE_BINARY) {
      if (OB_FAIL(ObCharset::get_mbmaxlen_by_coll(param_.coll_type_, mbmaxlen))) {
        LOG_WARN("fail to get mbmaxlen", K(ret), K(param_.coll_type_));
      } else {
        modified_len *= mbmaxlen;
      }
    }

    // consider offset is bigger than char len, add padding size modified len
    int64_t least_char_len = param_.byte_size_ / mbmaxlen;
    if (param_.lob_handle_has_char_len()) {
      least_char_len = *param_.get_char_len_ptr();
    }
    if (param_.offset_ > least_char_len) {
      modified_len += (param_.offset_ - least_char_len);
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(param_.init_out_row_ctx(modified_len + old_data.length()))) {
      LOG_WARN("init lob data out row ctx failed", K(ret));
    } else {
      bool found_begin = false;
      bool found_end = false;
      ObLobMetaInfo range_begin;
      ObLobMetaInfo range_end;
      ObString post_data;
      ObString remain_buf;
      ObString tmp_buf; // use for read piece data in replace_process_meta_info
      uint64_t padding_size = 0;
      uint64_t pos = 0;
      if (old_data.length() == 0) {
        if (param_.scan_backward_) {
          LOG_INFO("param scan_backward is true. Make it be false.", K(param_));
          param_.scan_backward_ = false;
        }
        if(OB_FAIL(prepare_data_buffer(param_, remain_buf, store_chunk_size_))) {
          LOG_WARN("fail to prepare buffers", K(ret), K(param_));
        } else if(OB_FAIL(prepare_data_buffer(param_, tmp_buf, store_chunk_size_))) {
          LOG_WARN("fail to prepare buffers", K(ret), K(param_));
        } else if (OB_FAIL(lob_meta_mngr_->scan(param_, meta_iter))) {
          LOG_WARN("do lob meta scan failed.", K(ret), K(param_));
        } else {
          // 1. do replace and get range begin and range end when old data out row
          ObLobMetaScanResult result;
          while (OB_SUCC(ret)) {
            ret = meta_iter.get_next_row(result);
            if (OB_FAIL(ret)) {
              if (ret != OB_ITER_END) {
                LOG_WARN("failed to get next row.", K(ret));
              }
            } else if (OB_FAIL(param_.is_timeout())) {
              LOG_WARN("access timeout", K(ret), K(param_));
            } else {
              if (meta_iter.is_range_begin(result.info_)) {
                if (OB_FAIL(range_begin.deep_copy(*param_.allocator_, result.info_))) {
                  LOG_WARN("deep copy meta info failed", K(ret), K(meta_iter));
                } else {
                  found_begin = true;
                }
              }
              if (OB_SUCC(ret) && meta_iter.is_range_end(result.info_)) {
                if (OB_FAIL(range_end.deep_copy(*param_.allocator_, result.info_))) {
                  LOG_WARN("deep copy meta info failed", K(ret), K(meta_iter));
                } else {
                  found_end = true;
                }
              }
              if (OB_SUCC(ret) && OB_FAIL(replace_process_meta_info(meta_iter, result, iter, read_buf, remain_buf, tmp_buf))) {
                LOG_WARN("process erase meta info failed.", K(ret), K(param_), K(result));
              }
            }
          }
          if (ret == OB_ITER_END) {
            ret = OB_SUCCESS;
          }
        }
      } else {
        // process inrow to outrow
        int64_t old_char_len = ObCharset::strlen_char(param_.coll_type_, old_data.ptr(), old_data.length());
        if (param_.offset_ > old_char_len) {
          // calc padding size
          padding_size = param_.offset_ - old_char_len;
          // do append => [old_data][padding][data]
          post_data = old_data;
        } else {
          // here has four situation
          // [old][new][old]       --> cover part of old data
          // [new_data][old_data]  --> cover front part
          // [old_data][new_data]  --> cover back part
          // [new_data]            --> full cover old data
          int64_t offset_byte_len = ObCharset::charpos(param_.coll_type_,
                                                       old_data.ptr(),
                                                       old_data.length(),
                                                       param_.offset_);
          if (offset_byte_len > 0) { // offset is not 0, must have some old data at front
            post_data.assign_ptr(old_data.ptr(), offset_byte_len);
          }
          if (param_.offset_ + param_.len_ < old_char_len) { // not full cover, must have some old data at back
            int64_t end_byte_len = ObCharset::charpos(param_.coll_type_,
                                                      old_data.ptr(),
                                                      old_data.length(),
                                                      param_.offset_ + param_.len_);
            if (end_byte_len >= old_data.length()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get byte len is bigger then data length", K(ret), K(end_byte_len), K(old_data.length()), K(param_));
            } else {
              remain_buf.assign_ptr(old_data.ptr() + end_byte_len, old_data.length() - end_byte_len);
            }
          }
        }
      }

      // insert situation for range begin and end
      // found_begin  found end  => result
      // true         true          do range insert, seq_id in [end, next]
      // false        false         do padding and append in [end, max]
      // true         false         do range append, seq_id in [end, max]
      // other situations are invalid
      uint32_t inrow_st = 0;
      ObString seq_id_st, seq_id_ed;
      if (old_data.length() > 0) {
        // inrow to outrow, set st 0, set ed null
        seq_id_st.assign_ptr(reinterpret_cast<char*>(&inrow_st), sizeof(uint32_t));
        seq_id_ed.assign_ptr(nullptr, 0);
      } else if (found_begin && found_end) {
        seq_id_st = range_end.seq_id_;
        seq_id_ed = meta_iter.get_cur_info().seq_id_;
        if (seq_id_ed.compare(seq_id_st) == 0) {
          // only found one and this is the last lob meta, just set end to max
          seq_id_ed.assign_ptr(nullptr, 0);
        }
      } else if (found_begin && !found_end) {
        seq_id_st = meta_iter.get_cur_info().seq_id_;
        seq_id_ed.assign_ptr(nullptr, 0);
      } else if (!found_begin && !found_end) {
        uint64_t total_char_len = meta_iter.get_cur_pos();
        padding_size = param_.offset_ - total_char_len;
        seq_id_st = meta_iter.get_cur_info().seq_id_;
        seq_id_ed.assign_ptr(nullptr, 0);
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown state for range.", K(ret), K(found_begin), K(found_end));
      }

      if (OB_FAIL(ret)) {
      } else {
        // prepare write iter
        ObLobMetaWriteIter write_iter(param_.allocator_, store_chunk_size_);
        if (OB_FAIL(write_iter.open(param_, iter, read_buf, padding_size, post_data, remain_buf, seq_id_st, seq_id_ed))) {
          LOG_WARN("failed to open meta writer", K(ret), K(write_iter), K(meta_iter), K(found_begin), K(found_end),
                   K(range_begin), K(range_end));
        } else if (OB_FAIL(write_outrow_result(param_, write_iter))) {
          LOG_WARN("failed to write outrow result", K(ret), K(write_iter), K(meta_iter), K(found_begin), K(found_end),
                   K(range_begin), K(range_end));
        }
        write_iter.close();
      }
    }
  }
  }
  return ret;
}

int ObLobWriteHandler::replace_process_meta_info(
    ObLobMetaScanIter &meta_iter,
    ObLobMetaScanResult &result,
    ObLobQueryIter *iter,
    ObString& read_buf,
    ObString &remain_data,
    ObString &write_buf)
{
  int ret = OB_SUCCESS;
  bool is_char = param_.is_char();
  bool del_piece = false;
  ObString temp_read_buf;
  ObLobMetaInfo new_meta_row = result.info_;

  ObLobWriteBuffer buffer(param_.coll_type_, store_chunk_size_, param_.is_store_char_len_);

  uint64_t cur_piece_end = meta_iter.get_cur_pos();
  uint64_t cur_piece_begin = cur_piece_end - (is_char ? result.info_.char_len_ : result.info_.byte_len_);

  int64_t piece_write_begin = param_.offset_ > cur_piece_begin ? param_.offset_ - cur_piece_begin : 0;
  int64_t piece_write_end = param_.offset_ + param_.len_ > cur_piece_end ? cur_piece_end - cur_piece_begin : param_.offset_ + param_.len_ - cur_piece_begin;

  int64_t replace_byte_st = ObCharset::charpos(param_.coll_type_, result.info_.lob_data_.ptr(), result.info_.lob_data_.length(), result.st_);
  int64_t temp_read_size = store_chunk_size_ - replace_byte_st;
  temp_read_buf.assign_buffer(read_buf.ptr(), temp_read_size);

  if (piece_write_begin != result.st_ || piece_write_end - piece_write_begin != result.len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("offset invalid", K(ret), K(piece_write_begin), K(piece_write_begin), K(result));
  } else if (iter->is_end()) {
  } else if (OB_FAIL(iter->get_next_row(temp_read_buf))) {
    if (ret != OB_ITER_END) {
      LOG_WARN("fail to do get next read buffer", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (iter->is_end() && result.len_ == result.info_.char_len_) {
    del_piece = true;
  } else if (result.len_ != result.info_.char_len_) {
    // use buffer because need concat data
    if (OB_FAIL(buffer.set_buffer(write_buf.ptr(), write_buf.size()))) {
      LOG_WARN("set_buffer fail", K(ret));
    } else if (OB_FAIL(buffer.append(result.info_.lob_data_.ptr(), result.info_.lob_data_.length()))) {
      LOG_WARN("append fail", K(ret), K(param_));
    }
  }

  if (OB_FAIL(ret) || del_piece) {
  } else if (OB_FAIL(buffer.char_write(result.st_, result.len_, temp_read_buf, remain_data))) {
    LOG_WARN("char_write fail", K(ret), K(cur_piece_begin), K(cur_piece_end), K(piece_write_begin), K(piece_write_end));
  } else if (OB_FAIL(buffer.to_lob_meta_info(new_meta_row))) {
    LOG_WARN("to_lob_meta_info fail", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (del_piece) {
    if (OB_FAIL(erase_one_piece(param_,
                                result.info_))) {
      LOG_WARN("failed erase one piece", K(ret), K(result),
          K(cur_piece_begin), K(cur_piece_end), K(piece_write_begin), K(piece_write_end), K(buffer));
    }
  } else if (OB_FAIL(update_one_piece(param_,
                                  result.info_,
                                  new_meta_row))) {
    LOG_WARN("failed to update.", K(ret), K(result), K(new_meta_row),
        K(cur_piece_begin), K(cur_piece_end), K(piece_write_begin), K(piece_write_end), K(buffer));
  }
  return ret;
}

/*************ObLobDiffUpdateHandler*****************/
int ObLobDiffUpdateHandler::init(ObLobMetaManager *lob_meta_mngr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(param_.set_lob_locator(param_.lob_locator_))) {
    LOG_WARN("failed to set lob locator for param", K(ret), K(param_));
  } else if (param_.coll_type_ != ObCollationType::CS_TYPE_BINARY) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("delta lob coll_type must be binary", K(ret), K(param_));
  } else if (OB_FAIL(init_base(lob_meta_mngr))) {
    LOG_WARN("init_base fail", KR(ret), K(param_));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObLobDiffUpdateHandler::execute(ObLobLocatorV2& delta_locator, ObLobDiffHeader *diff_header)
{
  int ret = OB_SUCCESS;
  ObTabletID piece_tablet_id;
  ObLobDiff *diffs = diff_header->get_diff_ptr();
  int64_t pos = 0;
  ObString new_lob_data(diff_header->persist_loc_size_, diff_header->data_);
  ObString extra_diff_data;
  ObLobPartialUpdateRowIter iter;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("handler not init", K(ret));
  } else if (OB_ISNULL(param_.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param tmp allocator is null", K(ret), K(param_));
  } else if (OB_FAIL(get_extra_diff_data(delta_locator, diff_header, extra_diff_data))) {
    LOG_WARN("get_extra_diff_data", K(ret), K(param_));
  } else if (OB_FAIL(iter.open(param_, delta_locator, diff_header))) {
    LOG_WARN("open iter fail", K(ret), K(param_), K(diff_header));
  } else if (iter.get_chunk_size() != store_chunk_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("chunk size not match", K(ret), K(iter.get_chunk_size()), K(store_chunk_size_), KPC(param_.lob_common_), K(param_));
  } else {
    int64_t seq_cnt = iter.get_modified_chunk_cnt();
    param_.used_seq_cnt_ = 0;
    param_.total_seq_cnt_ = seq_cnt;
    param_.op_type_ = ObLobDataOutRowCtx::OpType::DIFF_V2;
    if (OB_FAIL(param_.tx_desc_->get_and_inc_tx_seq(param_.parent_seq_no_.get_branch(),
                                                   seq_cnt,
                                                   param_.seq_no_st_))) {
      LOG_WARN("get and inc tx seq failed", K(ret), K(seq_cnt));
    } else if (OB_FAIL(param_.init_out_row_ctx(0))) {
      LOG_WARN("init lob data out row ctx failed", K(ret));
    }

    while(OB_SUCC(ret)) {
      ObLobMetaInfo *new_meta_row = nullptr;
      ObLobMetaInfo *old_meta_row = nullptr;
      int64_t offset = 0;
      if (OB_FAIL(iter.get_next_row(offset, old_meta_row, new_meta_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get_next_row fail", K(ret), K(param_), K(diff_header));
        }
      } else if (OB_ISNULL(old_meta_row)) {
        int32_t seq_id_int = 0;
        ObString seq_id_st(sizeof(seq_id_int), reinterpret_cast<char*>(&seq_id_int));
        ObString seq_id_ed;
        ObLobMetaWriteIter write_iter(param_.allocator_, store_chunk_size_);
        ObString post_data;
        ObString remain_buf;
        if (OB_FAIL(ObLobSeqId::get_seq_id(offset/store_chunk_size_ - 1, seq_id_st))) {
          LOG_WARN("get_seq_id fail", K(ret), K(offset));
        } else if (OB_FAIL(write_iter.open(param_, new_meta_row->lob_data_, 0, post_data, remain_buf, seq_id_st, seq_id_ed))) {
          LOG_WARN("failed to open meta writer", K(ret), K(write_iter), K(param_.byte_size_), K(offset), K(store_chunk_size_));
        } else if (OB_FAIL(write_outrow_result(param_, write_iter))) {
          LOG_WARN("failed to write outrow result", K(ret), K(write_iter), K(param_.byte_size_), K(offset), K(store_chunk_size_));
        }
        write_iter.close();
      } else if (OB_FAIL(update_one_piece(
          param_,
          *old_meta_row,
          *new_meta_row))) {
        LOG_WARN("update_one_piece fail", K(ret), K(offset), K(store_chunk_size_));
      }
    }
  }
  if (ret == OB_ITER_END) {
    ret = OB_SUCCESS;
  }
  if (OB_FAIL(ret)) {
  } else {
    param_.ext_info_log_.set_raw(extra_diff_data.ptr(), extra_diff_data.length());
  }
  return ret;
}

int ObLobDiffUpdateHandler::get_extra_diff_data(ObLobLocatorV2 &lob_locator, ObLobDiffHeader *diff_header, ObString &extra_diff_data)
{
  INIT_SUCC(ret);
  char *data_ptr = diff_header->get_inline_data_ptr();
  int64_t extra_data_len = lob_locator.size_ -  (data_ptr - lob_locator.ptr_);
  if (extra_data_len < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid extra data length", K(ret), K(extra_data_len), K(lob_locator), K(*diff_header));
  } else {
    extra_diff_data.assign_ptr(data_ptr, extra_data_len);
  }
  return ret;
}

/*************ObLobWriteBaseHandler*****************/
int ObLobWriteBaseHandler::write_one_piece(ObLobAccessParam& param, ObLobMetaInfo& meta_row)
{
  int ret = OB_SUCCESS;
  if (param.is_store_char_len_ && meta_row.char_len_ > meta_row.byte_len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("char len should not bigger than byte len", K(ret), K(meta_row));
  } else if (0 == meta_row.byte_len_ || meta_row.byte_len_ != meta_row.lob_data_.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("byte length invalid", K(ret), K(meta_row));
  } else if (OB_FAIL(lob_meta_mngr_->write(param, meta_row))) {
    LOG_WARN("write lob meta row failed.", K(ret));
  } else if (OB_FAIL(param.update_out_row_ctx(nullptr, meta_row))) {
    LOG_WARN("failed update checksum.", K(ret), K(param), K(meta_row));
  } else if (OB_FAIL(param.update_handle_data_size(nullptr/*old_info*/, &meta_row/*new_info*/))) {
    LOG_WARN("failed update handle", K(ret), K(param), K(meta_row));
  } else {
    LOG_TRACE("write success", K(param), K(meta_row));
  }
  return ret;
}

int ObLobWriteBaseHandler::update_one_piece(ObLobAccessParam& param, ObLobMetaInfo& old_meta_info, ObLobMetaInfo& new_meta_info)
{
  int ret = OB_SUCCESS;

  // TODO rmeove
  if (!param.is_store_char_len_ && param.lob_handle_has_char_len_field()) {
    old_meta_info.char_len_ = UINT32_MAX;
  }

  if (param.is_store_char_len_ && new_meta_info.char_len_ > new_meta_info.byte_len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("char len should not bigger than byte len", K(ret), K(new_meta_info));
  } else if (! param.is_store_char_len_ && new_meta_info.char_len_ != UINT32_MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("char length invalid", K(ret), K(new_meta_info), K(param));
  } else if (0 == new_meta_info.byte_len_ || new_meta_info.byte_len_ != new_meta_info.lob_data_.length()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("byte length invalid", K(ret), K(new_meta_info));
  } else if (OB_FAIL(lob_meta_mngr_->update(param, old_meta_info, new_meta_info))) {
    LOG_WARN("write lob meta row failed.", K(ret));
  } else if (OB_FAIL(param.update_out_row_ctx(&old_meta_info, new_meta_info))) {
    LOG_WARN("failed update checksum.", K(ret), K(old_meta_info), K(new_meta_info));
  } else if (OB_FAIL(param.update_handle_data_size(&old_meta_info, &new_meta_info))) {
    LOG_WARN("failed update handle", K(ret), K(old_meta_info), K(new_meta_info));
  } else {
    LOG_TRACE("update success", K(param), K(old_meta_info), K(new_meta_info));
  }
  return ret;
}

int ObLobWriteBaseHandler::erase_one_piece(ObLobAccessParam& param, ObLobMetaInfo& meta_info)
{
  int ret = OB_SUCCESS;
  ObLobPieceInfo piece_info;
  // TODO remove
  if (!param.lob_handle_has_char_len() && param.lob_handle_has_char_len_field()) {
    meta_info.char_len_ = UINT32_MAX;
  }

  if (OB_FAIL(lob_meta_mngr_->erase(param, meta_info))) {
    LOG_WARN("write lob meta row failed.", K(ret));
  // TODO aozeliu.azl
  // old_info is null, but this is erase. and behavier is same as write_one_piece
  } else if (OB_FAIL(param.update_out_row_ctx(nullptr, meta_info))) {
    LOG_WARN("failed update checksum.", K(ret), K(param), K(meta_info));
  } else if (OB_FAIL(param.update_handle_data_size(&meta_info/*old_info*/, nullptr/*new_info*/))) {
    LOG_WARN("failed update handle", K(ret), K(param), K(meta_info));
  } else {
    LOG_TRACE("erase success", K(param), K(meta_info));
  }
  return ret;
}

int ObLobWriteBaseHandler::write_outrow_result(ObLobAccessParam& param, ObLobMetaWriteIter &write_iter)
{
  int ret = OB_SUCCESS;
  ObLobMetaWriteResult result;
  int cnt = 0;
  while (OB_SUCC(ret)) {
    // split append data into data pieces 250k/piece
    ret = write_iter.get_next_row(result);
    if (OB_FAIL(ret)) {
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next row.", K(ret), K(cnt));
      }
    } else if (OB_FAIL(param.is_timeout())) {
      LOG_WARN("access timeout", K(ret), K(param));
    } else {
      cnt++;
      if (result.is_update_) {
        if (OB_FAIL(update_one_piece(param, result.old_info_, result.info_))) {
          LOG_WARN("failed to update.", K(ret), K(cnt), K(result.info_), K(result.old_info_));
        }
      } else {
        if (OB_FAIL(write_one_piece(param, result.info_))) {
          LOG_WARN("failed write data.", K(ret), K(cnt), K(result.info_));
        }
      }
    }
  }
  return ret;
}

int ObLobWriteBaseHandler::prepare_data_buffer(ObLobAccessParam& param, ObString &buffer, int64_t buffer_size)
{
  int ret = OB_SUCCESS;
  char *ptr = nullptr;
  if (OB_ISNULL(param.get_tmp_allocator())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret), K(param));
  } else if (OB_ISNULL(ptr = static_cast<char*>(param.get_tmp_allocator()->alloc(buffer_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc buffer failed.", K(ret));
  } else {
    buffer.assign_buffer(ptr, buffer_size);
  }
  return ret;
}

} // storage
} // oceanbase