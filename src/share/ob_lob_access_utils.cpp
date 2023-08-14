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
 * This file contains implementation for lob_access_utils.
 */

#define USING_LOG_PREFIX COMMON

#include "share/ob_lob_access_utils.h"
#include "lib/objectpool/ob_server_object_pool.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "storage/lob/ob_lob_util.h"
#include "storage/tx/ob_trans_define_v4.h"
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{
namespace common
{

void ObLobTextIterCtx::init(bool is_clone /* false */)
{
  buff_byte_len_ = MAX(buff_byte_len_, OB_LOB_ITER_DEFAULT_BUFFER_LEN);
  if (is_clone) {
    // not used currently, waititng for temp lob based on temporary file
    is_cloned_temporary_ = is_clone;
  }
}

void ObLobTextIterCtx::reuse()
{
  content_byte_len_ = 0;
  content_len_ = 0;
  accessed_byte_len_ = 0;
  accessed_len_ = 0;
  last_accessed_byte_len_ = 0;
  last_accessed_len_ = 0;
  iter_count_ = 0;
  if (OB_NOT_NULL(lob_query_iter_)) {
    lob_query_iter_->reset();
    OB_DELETE(ObLobQueryIter, "unused", lob_query_iter_);
    lob_query_iter_ = NULL;
  }
}

// ----- implementations of ObTextStringIter -----

ObTextStringIter::~ObTextStringIter()
{
  if (is_outrow_ && OB_NOT_NULL(ctx_)) {
    if (OB_NOT_NULL(ctx_->lob_query_iter_)) {
      ctx_->lob_query_iter_->reset();
      OB_DELETE(ObLobQueryIter, "unused", ctx_->lob_query_iter_);
      ctx_->lob_query_iter_ = NULL;
    }
  }
}

int ObTextStringIter::init(uint32_t buffer_len,
                           const sql::ObBasicSessionInfo *session,
                           ObIAllocator *allocator,
                           bool clone_remote /* false */)
{
  int ret = OB_SUCCESS;
  if (is_init_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: iter already initiated", K(ret));
  } else if (!(is_lob_storage(type_))) {
    is_lob_ = false;
    is_outrow_ = false;
  } else if (datum_str_.length() == 0) {
    COMMON_LOG(DEBUG, "Lob: iter with null input str", K(ret), K(*this));
  } else {
    ObLobLocatorV2 locator(datum_str_, has_lob_header_);
    is_lob_ = true;
    if (!has_lob_header_) {
      is_outrow_ = false;

    } else if (!locator.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN,"Lob: invalid lob", K(ret));
    } else if (FALSE_IT(is_outrow_ = !locator.has_inrow_data())) {
    } else if (!is_outrow_) { // inrow lob always get full data, no need ctx_
    } else if (OB_ISNULL(allocator)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Lob: iter with null allocator", K(ret));
    } else { // outrow lob
      char *ctx_buffer = static_cast<char *>(allocator->alloc(sizeof(ObLobTextIterCtx)));
      if (OB_ISNULL(ctx_buffer)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        COMMON_LOG(WARN,"Lob: failed to alloc output buffer", K(ret), KP(ctx_buffer));
      } else {
        ctx_ = new (ctx_buffer) ObLobTextIterCtx(locator, session, allocator, buffer_len);
        ctx_ ->init(clone_remote);
      }
    }
  }
  if (OB_SUCC(ret)) {
    state_ = TEXTSTRING_ITER_INIT;
    is_init_ = true;
  }
  return ret;
}

static int init_lob_access_param(storage::ObLobAccessParam &param,
                                 ObLobTextIterCtx *lob_iter_ctx,
                                 ObCollationType cs_type,
                                 ObIAllocator *allocator = nullptr)
{
  int ret = OB_SUCCESS;
  int64_t query_timeout = 0;
  ObMemLobTxInfo *tx_info = NULL;
  ObMemLobLocationInfo *location_info = NULL;
  ObLobCommon *disk_loc = NULL;
  ObString disk_loc_str;

  if (OB_ISNULL(lob_iter_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: invalid lob iter ctx.", K(ret));
  } else if (!lob_iter_ctx->locator_.is_persist_lob()) {
    ret = OB_NOT_IMPLEMENT;
    COMMON_LOG(WARN, "Lob: outrow temp lob is not supported", K(ret), K(lob_iter_ctx->locator_));
  } else if (OB_FAIL(lob_iter_ctx->locator_.get_disk_locator(disk_loc_str))) {
    COMMON_LOG(WARN, "Lob: get disk locator failed.", K(ret));
  } else if (FALSE_IT(disk_loc = reinterpret_cast<ObLobCommon *>(disk_loc_str.ptr()))){
  } else if (OB_ISNULL(disk_loc) || disk_loc->is_init_ == false) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: invalid disk_locator.", K(ret), KP(disk_loc));
  } else {
    if (OB_ISNULL(lob_iter_ctx->session_)) {
      query_timeout = ObTimeUtility::current_time() + 60 * USECS_PER_SEC;
    } else if (OB_FAIL(lob_iter_ctx->session_->get_query_timeout(query_timeout))) {
      COMMON_LOG(WARN, "Lob: get_query_timeout failed.", K(ret), K(*lob_iter_ctx));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(lob_iter_ctx->locator_.get_tx_info(tx_info))) {
  } else if (OB_FAIL(lob_iter_ctx->locator_.get_location_info(location_info))) {
  } else {
    param.tx_desc_ = NULL;
    param.snapshot_.core_.tx_id_ = tx_info->snapshot_tx_id_;
    param.snapshot_.core_.version_.convert_for_tx(tx_info->snapshot_version_);
    param.snapshot_.core_.scn_ = transaction::ObTxSEQ::cast_from_int(tx_info->snapshot_seq_);
    param.snapshot_.valid_= true;
    param.snapshot_.source_ = transaction::ObTxReadSnapshot::SRC::LS;
    param.snapshot_.snapshot_lsid_ = share::ObLSID(location_info->ls_id_);

    param.ls_id_ = share::ObLSID(location_info->ls_id_);
    param.sql_mode_ = (lob_iter_ctx->session_ == NULL) ? SMO_DEFAULT : lob_iter_ctx->session_->get_sql_mode();

    param.tablet_id_ = ObTabletID(location_info->tablet_id_);
    if (allocator == nullptr) {
      allocator = lob_iter_ctx->alloc_;
    }
    param.allocator_ = allocator;
    int64_t disk_loc_handle_size = disk_loc_str.length();
    param.lob_common_ = disk_loc;
    param.handle_size_ = disk_loc_handle_size;
    param.byte_size_ = param.lob_common_->get_byte_size(param.handle_size_);
    param.coll_type_ = cs_type;
    if (!lob_iter_ctx->locator_.is_lob_disk_locator()) {
      param.lob_locator_ = &lob_iter_ctx->locator_;
    }
    param.timeout_ = (lob_iter_ctx->session_ == NULL)
                      ? query_timeout
                      : (lob_iter_ctx->session_->get_query_start_time() + query_timeout);
    param.scan_backward_ = false;
    param.offset_ = 0; // use 0 offset when reading full lob data
    param.len_ = 0;
  }

  return ret;
}

int ObTextStringIter::get_outrow_lob_full_data(ObIAllocator *allocator /*nullptr*/ )
{
  int ret = OB_SUCCESS;
  storage::ObLobManager* lob_mngr = MTL(storage::ObLobManager*);

  if (!has_lob_header_ || !is_outrow_ || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->alloc_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: error condition",
      K(ret), K(has_lob_header_), K(is_outrow_), KP(ctx_->session_), KP(ctx_));
  } else if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "Lob: get lob manager failed.", K(ret));
  } else { // outrow persist lob
    storage::ObLobAccessParam param;
    if (OB_SUCC(init_lob_access_param(param, ctx_, cs_type_, allocator))) {
      param.len_ = (ctx_->total_access_len_ == 0 ? param.byte_size_ : ctx_->total_access_len_);

      if (!param.ls_id_.is_valid() || !param.tablet_id_.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "Lob: invalid param.", K(ret), K(param));
      } else if (param.byte_size_ == 0) {
        // empty lob
        ctx_->content_byte_len_ = 0;
      } else if (param.byte_size_ < 0 || param.len_ == 0) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN,"Lob: calc byte size is negative.", K(ret), K(param));
      } else if (param.byte_size_ > OB_MAX_LONGTEXT_LENGTH) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN,"Lob: unable to read full data over 512M lob.", K(ret), K(param));
      } else {
        ctx_->total_byte_len_ = param.byte_size_;
        ctx_->buff_byte_len_ = static_cast<uint32_t>(param.byte_size_);//TODO(gehao.wh): check convert from 64 to 32
        ctx_->buff_ = static_cast<char *>(ctx_->alloc_->alloc(ctx_->buff_byte_len_));
        ObString output_data;

        if (OB_ISNULL(ctx_->buff_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          COMMON_LOG(WARN,"Lob: failed to alloc output buffer",
              K(ret), KP(ctx_->buff_), K(ctx_->buff_byte_len_));
        } else {
          output_data.assign_buffer(ctx_->buff_, ctx_->buff_byte_len_);
          if (OB_FAIL(lob_mngr->query(param, output_data))) {
            COMMON_LOG(WARN,"Lob: falied to query lob tablets.", K(ret), K(param));
          } else {
            ctx_->content_byte_len_ = output_data.length();
            // Notice: content_len_ (char len) is not updated!
            COMMON_LOG(DEBUG,"Lob: read output for obstring iter.", K(output_data));
          }
        }
      }
    }
  }
  return ret;
}

int ObTextStringIter::get_outrow_prefix_data(uint32_t prefix_char_len)
{
  int ret = OB_SUCCESS;
  storage::ObLobManager* lob_mngr = MTL(storage::ObLobManager*);

  if (!has_lob_header_ || !is_outrow_ || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->alloc_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: error condition",
      K(ret), K(has_lob_header_), K(is_outrow_), KP(ctx_->session_), KP(ctx_));
  } else if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "Lob: get lob manager failed.", K(ret));
  } else { // outrow persist lob
    storage::ObLobAccessParam param;
    if (OB_SUCC(init_lob_access_param(param, ctx_, cs_type_))) {
      param.len_ = prefix_char_len;

      if (!param.ls_id_.is_valid() || !param.tablet_id_.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "Lob: invalid param.", K(ret), K(param));
      } else if (param.byte_size_ < 0 || param.len_ == 0) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN,"Lob: calc byte size is negative.", K(ret), K(param));
      } else {
        ctx_->total_byte_len_ = param.byte_size_;
        ctx_->buff_byte_len_ = prefix_char_len * MAX_CHAR_MULTIPLIER;
        ctx_->buff_ = static_cast<char *>(ctx_->alloc_->alloc(ctx_->buff_byte_len_));
        ObString output_data;
        if (OB_ISNULL(ctx_->buff_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          COMMON_LOG(WARN,"Lob: failed to alloc output buffer",
              K(ret), KP(ctx_->buff_), K(ctx_->buff_byte_len_));
        } else {
          output_data.assign_buffer(ctx_->buff_, ctx_->buff_byte_len_);
          if (OB_FAIL(lob_mngr->query(param, output_data))) {
            COMMON_LOG(WARN,"Lob: falied to query lob tablets.", K(ret), K(param));
          } else {
            ctx_->content_byte_len_ = output_data.length();
            // Notice: content_len_ (char len) is not updated!
            COMMON_LOG(DEBUG,"Lob: read output for obstring iter.", K(output_data));
          }
        }
      }
    }
  }
  return ret;
}

int ObTextStringIter::get_current_block(ObString &str)
{
  int ret = OB_SUCCESS;
  if (!is_init_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: iter not initiated", K(ret));
  } else if (state_ <= TEXTSTRING_ITER_INIT) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: iter not accessed", K(ret));
  } else {
    str.assign(ctx_->buff_, static_cast<int32_t>(ctx_->content_byte_len_));
  }
  return ret;
}

int ObTextStringIter::get_full_data(ObString &data_str, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (!is_init_ || state_ != TEXTSTRING_ITER_INIT) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: iter state error", K(ret), K(is_init_), K(state_));
  } else if (datum_str_.length() == 0) { // maybe from datum->is_nop/is_null
    data_str.assign(NULL, 0);
    COMMON_LOG(DEBUG, "Lob: iter with null input", K(ret), K(*this));
  } else if (!is_lob_ || !has_lob_header_) { // string types or 4.0 compatiable text
    data_str.assign_ptr(datum_str_.ptr(), datum_str_.length());
  } else if (!is_outrow_) { // inrow lob
    ObLobLocatorV2 loc(datum_str_, has_lob_header_);
    if (OB_FAIL(loc.get_inrow_data(data_str))) {
      COMMON_LOG(WARN, "Lob: get lob inrow data failed", K(ret));
    }
  } else {
    // outrow lob, read full data into a inrow local tmp lob currently
    if (OB_FAIL(get_outrow_lob_full_data(allocator))) {
      COMMON_LOG(WARN, "Lob: get lob outrow data failed", K(ret));
    } else {
      data_str.assign_ptr(ctx_->buff_, ctx_->content_byte_len_);
    }
  }
  if (OB_SUCC(ret)) {
    state_ = TEXTSTRING_ITER_NEXT;
  }
  COMMON_LOG(DEBUG, "Lob: get_full_data", K(ret), K(data_str), KP(data_str.ptr()), K(data_str.length()));
  return ret;
}

// Notice: always get full data for string tc & inrow lobs
int ObTextStringIter::get_inrow_or_outrow_prefix_data(ObString &data_str, uint32_t prefix_char_len)
{
  int ret = OB_SUCCESS;
  if (!is_init_ || state_ != TEXTSTRING_ITER_INIT) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: iter state error", K(ret), K(is_init_), K(state_));
  } else if (datum_str_.length() == 0) {
    data_str.assign(NULL, 0);
    COMMON_LOG(DEBUG, "Lob: iter with null input", K(ret), K(*this));
  } else if (!is_lob_ || !has_lob_header_) { // string types
    data_str.assign_ptr(datum_str_.ptr(), datum_str_.length());
  } else if (!is_outrow_) { // inrow lob
    ObLobLocatorV2 loc(datum_str_, has_lob_header_);
    if (OB_FAIL(loc.get_inrow_data(data_str))) {
      COMMON_LOG(WARN, "Lob: get lob inrow data failed", K(ret));
    }
  } else {
    // outrow lob, read full data into a inrow local tmp lob currently
    if (OB_FAIL(get_outrow_prefix_data(prefix_char_len))) {
      COMMON_LOG(WARN, "Lob: get lob outrow prefix failed", K(ret), K(prefix_char_len));
    } else {
      data_str.assign_ptr(ctx_->buff_, ctx_->content_byte_len_);
    }
  }
  if (OB_SUCC(ret)) {
    state_ = TEXTSTRING_ITER_NEXT;
  }
  COMMON_LOG(DEBUG, "Lob: get_inrow_or_outrow_prefix_data",
    K(ret), K(data_str), KP(data_str.ptr()), K(data_str.length()));
  return ret;
}

void ObTextStringIter::reset()
{
  if(!is_init_) {
  } else if (is_lob_ && OB_NOT_NULL(ctx_)) {
    // Notice: memory of ctx_ itself will be released when allocator destruction
    ctx_->reuse();
  }
  state_ = TEXTSTRING_ITER_INIT;
}

int ObTextStringIter::get_first_block(ObString &str)
{
  int ret = OB_SUCCESS;
  storage::ObLobManager* lob_mngr = MTL(storage::ObLobManager*);

  if (!is_outrow_ || OB_ISNULL(ctx_) || !has_lob_header_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: error condition",
      K(ret), K(is_outrow_), KP(ctx_), K(has_lob_header_));
  } else if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "Lob: get lob manager failed.", K(ret));
  } else {
    storage::ObLobAccessParam param;
    if (OB_SUCC(init_lob_access_param(param, ctx_, cs_type_))) {
      param.scan_backward_ = ctx_->is_backward_;
      param.offset_ = ctx_->start_offset_;
      param.len_ = (ctx_->total_access_len_ == 0 ? param.byte_size_ : ctx_->total_access_len_);

      // update buffer len according to reserve length config
      ctx_->total_byte_len_ = param.byte_size_;
      if (ctx_->reserved_byte_len_ > 0 || ctx_->reserved_len_ > 0) {
        int64_t max_reserved_byte = MAX(ctx_->reserved_byte_len_, ctx_->reserved_len_ * MAX_CHAR_MULTIPLIER);
        if (ctx_->buff_byte_len_ < max_reserved_byte) {
          COMMON_LOG(INFO,"Lob: buffer size changed due to configurations",
            K(ctx_->buff_byte_len_), K(ctx_->reserved_byte_len_),
            K(ctx_->reserved_len_), K(max_reserved_byte));
          ctx_->buff_byte_len_ = static_cast<uint32_t>(max_reserved_byte);
        }
      }

      if (!param.ls_id_.is_valid() || !param.tablet_id_.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "Lob: invalid param.", K(ret), K(param));
      } else if (param.byte_size_ == 0) {
        state_ = TEXTSTRING_ITER_END;
      } else if (param.byte_size_ < 0 || param.len_ == 0) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN,"Lob: calc byte size is negative.", K(ret), K(param));
      } else {
        if (OB_ISNULL(ctx_->buff_)) {
          ctx_->buff_ = static_cast<char *>(ctx_->alloc_->alloc(ctx_->buff_byte_len_));
        }
        ObString output_data;
        output_data.assign_buffer(ctx_->buff_, ctx_->buff_byte_len_);

        // 1. start query iter, and query one time
        // 2. update access param
        if (OB_ISNULL(ctx_->buff_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          COMMON_LOG(WARN,"Lob: failed to alloc output buffer",
              K(ret), KP(ctx_->buff_), K(ctx_->buff_byte_len_));
        } else if (OB_FAIL(lob_mngr->query(param, ctx_->lob_query_iter_))) {
          COMMON_LOG(WARN,"Lob: falied to query lob iter.", K(ret), K(param));
        } else if (OB_FAIL(ctx_->lob_query_iter_->get_next_row(output_data))) {
          COMMON_LOG(WARN,"Lob: falied to get first block.", K(ret), K(param));
        } else {
          ctx_->content_byte_len_ = output_data.length();
          // ToDo: @gehao get char len directly from lob mngr ?
          ctx_->content_len_ = static_cast<uint32_t>(ObCharset::strlen_char(cs_type_,
                                                    output_data.ptr(),
                                                    static_cast<int64_t>(output_data.length())));
          ctx_->last_accessed_byte_len_ = 0;
          ctx_->last_accessed_len_ = 0;
          ctx_->accessed_byte_len_ = ctx_->content_byte_len_;
          ctx_->accessed_len_ = ctx_->content_len_;
          ctx_->iter_count_++;
          str.assign_ptr(ctx_->buff_, ctx_->content_byte_len_);
          state_ = TEXTSTRING_ITER_NEXT;
        }
      }
    }
  }

  return ret;
}

int ObTextStringIter::reserve_data()
{
  int ret = OB_SUCCESS;
  if (!is_outrow_ || !has_lob_header_ || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->lob_query_iter_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: error condition", K(ret), K(is_outrow_), K(has_lob_header_), KP(ctx_));
  } else if (ctx_->reserved_len_ == 0 && ctx_->reserved_byte_len_ == 0) {
    // do nothing
  } else if (ctx_->accessed_byte_len_ == ctx_->total_byte_len_) {
    // 1. already the last block, should get iter end;
    // 2. if not the last block, this function fail if access_len_ < reserved_len_
  } else if (ctx_->reserved_len_ != 0) {
    // reserve_char_data()
    if (cs_type_ == CS_TYPE_BINARY) {
      ctx_->reserved_byte_len_ = ctx_->reserved_len_;
    } else if (ctx_->reserved_len_ > ctx_->content_len_) {
      ret = OB_SIZE_OVERFLOW;
      COMMON_LOG(WARN, "Lob: reserved length oversized", K(ret), K(*ctx_));
    } else if (!ctx_->is_backward_) {
      uint32 reserved_char_start = ctx_->content_len_ - ctx_->reserved_len_;
      uint32 reserved_char_pos =
        static_cast<uint32_t>(ObCharset::charpos(cs_type_, ctx_->buff_, ctx_->content_byte_len_, reserved_char_start));
      ctx_->reserved_byte_len_ = ctx_->content_byte_len_ - reserved_char_pos;
      MEMMOVE(ctx_->buff_, ctx_->buff_ + reserved_char_pos, ctx_->reserved_byte_len_);
    } else if (ctx_->is_backward_) {
      uint32 reserved_char_end = ctx_->reserved_len_ + 1;
      uint32 reserved_char_end_pos =
        static_cast<uint32_t>(ObCharset::charpos(cs_type_, ctx_->buff_, ctx_->content_byte_len_, reserved_char_end));
      ctx_->reserved_byte_len_ = reserved_char_end_pos;
      MEMMOVE(ctx_->buff_ + ctx_->content_byte_len_ - ctx_->reserved_byte_len_,
              ctx_->buff_,
              ctx_->reserved_byte_len_);
    } else {}
  } else if (ctx_->reserved_byte_len_ != 0) {
    if (OB_FAIL(reserve_byte_data())) {
      COMMON_LOG(WARN, "Lob: reserved byte data failed", K(ret), K(*ctx_));
    }
  }
  return ret;
}

int ObTextStringIter::reserve_byte_data()
{
  int ret = OB_SUCCESS;
  if (ctx_->reserved_byte_len_ > ctx_->content_byte_len_) {
    ret = OB_SIZE_OVERFLOW;
    COMMON_LOG(WARN, "Lob: reserved byte length oversized", K(ret), K(*ctx_));
  } else if (!ctx_->is_backward_) {
    uint32 reserved_byte_start = ctx_->content_byte_len_ - ctx_->reserved_byte_len_;
    MEMMOVE(ctx_->buff_, ctx_->buff_ + reserved_byte_start, ctx_->reserved_byte_len_);
  } else if (ctx_->is_backward_) {
    MEMMOVE(ctx_->buff_ + ctx_->content_byte_len_ - ctx_->reserved_byte_len_,
            ctx_->buff_,
            ctx_->reserved_byte_len_);
  } else {}
  return ret;
}

int ObTextStringIter::get_next_block_inner(ObString &str)
{
  // 1. calc reserved len and memmove
  // 2. update query buffer and query agin
  // 3. update access param
  int ret = OB_SUCCESS;
  if (!is_outrow_ || !has_lob_header_ || OB_ISNULL(ctx_) || OB_ISNULL(ctx_->lob_query_iter_)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: error condition",
      K(ret), K(is_outrow_), K(has_lob_header_), KP(ctx_));
  } else if (ctx_->content_byte_len_ == 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: no data", K(ret), K(is_outrow_), KP(ctx_));
  } else if (OB_FAIL(reserve_data())) {
    COMMON_LOG(WARN, "Lob: reserve_data failed", K(ret), K(is_outrow_), KP(ctx_));
  } else {
    ObString output_data;
    if (!ctx_->is_backward_) {
      output_data.assign_buffer(ctx_->buff_ + ctx_->reserved_byte_len_,
                                ctx_->buff_byte_len_ - ctx_->reserved_byte_len_);
    } else {
      output_data.assign_buffer(ctx_->buff_,
                                ctx_->buff_byte_len_ - ctx_->reserved_byte_len_);
    }
    if (OB_FAIL(ctx_->lob_query_iter_->get_next_row(output_data))) {
      if (ret == OB_ITER_END) {
        state_ = TEXTSTRING_ITER_END; // iter finished
        ctx_->lob_query_iter_->reset();
        OB_DELETE(ObLobQueryIter, "unused", ctx_->lob_query_iter_);
        ctx_->lob_query_iter_ = NULL;
        ret = OB_SUCCESS;
      } else {
        COMMON_LOG(WARN,"Lob: falied to get first block.", K(ret));
      }
    } else {
      ctx_->content_byte_len_ = ctx_->reserved_byte_len_ + output_data.length();
      // ToDo: @gehao get directly from lob mngr ?
      uint32 cur_out_len = static_cast<uint32_t>(ObCharset::strlen_char(cs_type_,
                                                  output_data.ptr(),
                                                  static_cast<int64_t>(output_data.length())));
      ctx_->content_len_ = ctx_->reserved_len_ + cur_out_len;
      ctx_->last_accessed_byte_len_ = ctx_->accessed_byte_len_;
      ctx_->last_accessed_len_ = ctx_->accessed_len_;
      ctx_->accessed_byte_len_ += output_data.length();
      ctx_->accessed_len_ += cur_out_len;\
      ctx_->iter_count_++;
      str.assign_ptr(ctx_->buff_, ctx_->content_byte_len_);
    }
  }
  return ret;
}

ObTextStringIterState ObTextStringIter::get_next_block(ObString &str)
{
  int ret = OB_SUCCESS;
  str.reset();
  if (!is_init_ || state_ < TEXTSTRING_ITER_INIT) {
    state_ = TEXTSTRING_ITER_INVALID;
    COMMON_LOG(WARN, "Lob: iter not initiated", K(ret), K(*this));
  } else if (!is_lob_ || !is_outrow_ || !has_lob_header_) { // if not outrow lob, get full data
    switch (state_) {
      case TEXTSTRING_ITER_INIT: {
        OZ(get_full_data(str));
        break;
      }
      case TEXTSTRING_ITER_NEXT: {
        state_ = TEXTSTRING_ITER_END;
        break;
      }
      default: {
        COMMON_LOG(WARN, "Lob: error state for common string or inrow lob", K(state_));
        break;
      }
    }
  } else if (is_outrow_) {
    switch (state_) {
      case TEXTSTRING_ITER_INIT: {
        OZ(get_first_block(str));
        break;
      }
      case TEXTSTRING_ITER_NEXT: {
        OZ(get_next_block_inner(str));
        break;
      }
      default: {
        COMMON_LOG(WARN, "Lob: error state for common string or inrow lob", K(state_));
        break;
      }
    }
  } else {
    COMMON_LOG(WARN, "Lob: error case in of iter", K(ret), K(*this));
    state_ = TEXTSTRING_ITER_INVALID;
  }
  if (OB_FAIL(ret)) {
    COMMON_LOG(WARN, "Lob: iter get_next_block failed", K(ret), K(*this));
    state_ = TEXTSTRING_ITER_INVALID;
    err_ret_ = ret;
  }
  return state_;
}

void ObTextStringIter::set_start_offset(uint64_t offset)
{
  if (is_valid_for_config()) {
    ctx_->start_offset_ = offset;
  }
}

void ObTextStringIter::set_access_len(int64_t char_len)
{
  if (is_valid_for_config()) {
    ctx_->total_access_len_ = char_len;
  }
}

void ObTextStringIter::set_reserved_len(uint32_t reserved_len)
{
  if (is_valid_for_config()) {
    ctx_->reserved_len_ = reserved_len;
    ctx_->reserved_byte_len_ = 0;
  }
}

void ObTextStringIter::set_reserved_byte_len(uint32_t reserved_byte_len)
{
  if (is_valid_for_config()) {
    ctx_->reserved_byte_len_ = reserved_byte_len;
    ctx_->reserved_len_ = 0;
  }
}

void ObTextStringIter::reset_reserve_len()
{
  if (is_valid_for_config()) {
    ctx_->reserved_byte_len_ = 0;
    ctx_->reserved_len_ = 0;
  }
}

void ObTextStringIter::set_backward()
{
  if (is_valid_for_config()) {
    ctx_->is_backward_ = true;
  }
}

void ObTextStringIter::set_forward()
{
  if (is_valid_for_config()) {
    ctx_->is_backward_ = false;
  }
}

uint64_t ObTextStringIter::get_start_offset()
{
  uint64_t start_offset = 0;
  if (!is_init_ || !is_outrow_ || !has_lob_header_
      || state_ <= TEXTSTRING_ITER_INIT || OB_ISNULL(ctx_)) {
  } else {
    start_offset = ctx_->start_offset_;
  }
  return start_offset;
}

uint32_t ObTextStringIter::get_last_accessed_len()
{
  uint32_t last_accessed_len = 0;
  if (!is_init_ || !is_outrow_ || !has_lob_header_
      || state_ <= TEXTSTRING_ITER_INIT || OB_ISNULL(ctx_)) {
  } else {
    last_accessed_len = ctx_->last_accessed_len_;
  }
  return last_accessed_len;
}

uint32_t ObTextStringIter::get_iter_count()
{
  uint32_t iter_count = 0;
  if (!is_init_ || !is_outrow_ || !has_lob_header_
      || state_ <= TEXTSTRING_ITER_INIT || OB_ISNULL(ctx_)) {
  } else {
    iter_count = ctx_->iter_count_;
  }
  return iter_count;
}

uint32_t ObTextStringIter::get_reserved_char_len()
{
  uint32_t reserved_char_len = 0;
  if (!is_init_ || !is_outrow_ || !has_lob_header_
      || state_ <= TEXTSTRING_ITER_INIT || OB_ISNULL(ctx_)) {
  } else {
    reserved_char_len = ctx_->reserved_len_;
  }
  return reserved_char_len;
}

uint32_t ObTextStringIter::get_reserved_byte_len()
{
  uint32_t reserved_byte_len = 0;
  if (!is_init_ || !is_outrow_ || !has_lob_header_
      || state_ <= TEXTSTRING_ITER_INIT || OB_ISNULL(ctx_)) {
  } else {
    reserved_byte_len = ctx_->reserved_byte_len_;
  }
  return reserved_byte_len;
}


uint32_t ObTextStringIter::get_last_accessed_byte_len()
{
  uint32_t last_accessed_byte_len = 0;
  if (!is_init_ || !is_outrow_ || !has_lob_header_
      || state_ <= TEXTSTRING_ITER_INIT || OB_ISNULL(ctx_)) {
  } else {
    last_accessed_byte_len = ctx_->last_accessed_byte_len_;
  }
  return last_accessed_byte_len;
}

uint32_t ObTextStringIter::get_accessed_len()
{
  int32 ret = OB_SUCCESS;
  uint32_t accessed_len = 0;
  if (!is_init_ || state_ <= TEXTSTRING_ITER_INIT) {
  } else if (!is_lob_ || !is_outrow_ || !has_lob_header_) { // string types
    int64_t total_char_len = 0;
    if (OB_FAIL(get_char_len(total_char_len))) {
      COMMON_LOG(WARN, "Lob: get lob inrow data failed", K(ret));
    } else {
      accessed_len = static_cast<uint32_t>(total_char_len);
    }
  } else if (is_outrow_) {
    accessed_len = ctx_->accessed_len_;
  } else { // should not come here.
    ret = OB_UNEXPECT_INTERNAL_ERROR;
    COMMON_LOG(WARN, "Lob: get access len failed", K(ret), K(*this), K(lbt()));
  }
  return accessed_len;
}

uint32_t ObTextStringIter::get_accessed_byte_len()
{
  int32 ret = OB_SUCCESS;
  uint32_t accessed_byte_len = 0;
  if (!is_init_ || state_ <= TEXTSTRING_ITER_INIT) {
  } else if (!is_lob_ || !is_outrow_ || !has_lob_header_) { // string types
    int64_t total_byte_len = 0;
    if (OB_FAIL(get_byte_len(total_byte_len))) {
      COMMON_LOG(WARN, "Lob: get lob inrow data failed", K(ret));
    } else {
      accessed_byte_len = static_cast<uint32_t>(total_byte_len);
    }
  } else if (is_outrow_) {
    accessed_byte_len = ctx_->accessed_byte_len_;
  } else { // should not come here.
    ret = OB_UNEXPECT_INTERNAL_ERROR;
    COMMON_LOG(WARN, "Lob: get access len failed", K(ret), K(*this), K(lbt()));
  }
  return accessed_byte_len;
}

int ObTextStringIter::get_byte_len(int64_t &byte_len)
{
  int ret = OB_SUCCESS;
  if (!is_init_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: iter state error", K(ret), K(is_init_), K(state_));
  } else if (!is_lob_ || !is_outrow_ || !has_lob_header_) {
    ObString data_str = datum_str_;
    if (is_lob_ && has_lob_header_) {
      ObLobLocatorV2 loc(data_str, has_lob_header_);
      if (OB_FAIL(loc.get_inrow_data(data_str))) {
        COMMON_LOG(WARN, "Lob: get lob inrow data failed", K(ret));
      }
    }
    byte_len = data_str.length();
  } else { // outrow lob
    if (OB_ISNULL(ctx_)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Lob: iter state error", K(ret), K(is_init_), K(state_), K(ctx_));
    } else if (!ctx_->locator_.is_persist_lob()) {
      ret = OB_NOT_IMPLEMENT;
      COMMON_LOG(WARN, "Lob: outrow temp lob is not implement", K(ret), K(ctx_->locator_));
    } else if (OB_FAIL(ctx_->locator_.get_lob_data_byte_len(byte_len))) {
      // if buffer length is zero, use lob byte length
      COMMON_LOG(WARN, "Lob: get outrow lob byte len failed.", K(ret), K(ctx_->buff_byte_len_));
    }
  }
  return ret;
}

int ObTextStringIter::get_char_len(int64_t &char_length)
{
  int ret = OB_SUCCESS;
  if (!is_init_) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "Lob: iter state error", K(ret), K(is_init_), K(state_));
  } else if (!is_lob_ || !is_outrow_ || !has_lob_header_) {
    ObString data_str = datum_str_;
    if (is_lob_ && has_lob_header_) {
      ObLobLocatorV2 loc(data_str, has_lob_header_);
      if (OB_FAIL(loc.get_inrow_data(data_str))) {
        COMMON_LOG(WARN, "Lob: get lob inrow data failed", K(ret));
      }
    }
    char_length = ObCharset::strlen_char(cs_type_, data_str.ptr(), static_cast<int64_t>(data_str.length()));
  } else { // outrow lob
    ObString disk_loc_str;
    storage::ObLobManager* lob_mngr = MTL(storage::ObLobManager*);
    if (OB_ISNULL(ctx_)) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Lob: error condition", K(ret), K(is_outrow_), KP(ctx_->session_), KP(ctx_));
    } else if (OB_ISNULL(lob_mngr)) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "Lob: get lob manager failed.", K(ret));
    } else {
      storage::ObLobAccessParam param;
      if (OB_SUCC(init_lob_access_param(param, ctx_, cs_type_))) {
        uint64_t length = 0;
        if (!param.ls_id_.is_valid() || !param.tablet_id_.is_valid()) {
          ret = OB_INVALID_ARGUMENT;
          COMMON_LOG(WARN, "Lob: invalid param.", K(ret), K(param));
        } else if (OB_FAIL(lob_mngr->getlength(param, length))) {
          COMMON_LOG(WARN,"Lob: falied to get outrow lob char len.", K(ret), K(param));
        } else {
          char_length = static_cast<int64_t>(length);
        }
      }
    }
  }
  return ret;
}

// Only used to response client, append full lob data after outrow lob locator, disk locator is not changed
int ObTextStringIter::append_outrow_lob_fulldata(ObObj &obj,
                                                 const sql::ObBasicSessionInfo *session,
                                                 ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObObjType type = obj.get_type();
  ObString raw_string = obj.get_string();
  int64_t lob_data_byte_len = 0;
  int64_t res_byte_len = 0;
  int64_t pos = 0;
  char *buff = NULL;

  if (!obj.has_lob_header()) {
  } else if (!is_lob_storage(type)) {
  } else if (obj.is_null() || obj.is_nop_value()) {
  } else {
    bool data_changed = false;
    ObLobLocatorV2 loc(obj.get_string(), obj.has_lob_header());
    if (!loc.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Lob: invalid lob locator", K(ret), K(obj));
    } else if (loc.is_delta_temp_lob()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Lob: converting delta lob!", K(ret));
    } else if (loc.has_inrow_data()) {
      int64_t real_loc_len = 0;
      if (OB_FAIL(loc.get_real_locator_len(real_loc_len))) {
        COMMON_LOG(WARN, "Lob: failed to get real locator len", K(loc));
      } else {
        raw_string.assign_ptr(raw_string.ptr(), real_loc_len);
        data_changed = true; // should be old lob data from client, need refresh lobdata
      }
    }

    if (OB_FAIL(ret)) { // do noting
    } else if (loc.is_inrow() && !data_changed) { // do nothing
    } else if (OB_FAIL(loc.get_lob_data_byte_len(lob_data_byte_len))) {
      COMMON_LOG(WARN, "Lob: failed to get lob data byte length", K(ret), K(obj));
    } else if (OB_FALSE_IT((res_byte_len = lob_data_byte_len + raw_string.length()))) {
    } else if (res_byte_len > UINT32_MAX) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Lob: inrow lob data too long",
          K(raw_string.length()), K(lob_data_byte_len));
    } else if (OB_ISNULL((buff = static_cast<char *>(allocator.alloc(res_byte_len))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(WARN,"Lob: failed to alloc res buffer",
          K(raw_string.length()), K(lob_data_byte_len));
    } else {
      // copy mem lob locator
      MEMCPY(buff, raw_string.ptr(), raw_string.length());
      pos += raw_string.length();

      // config mem lob locator
      ObMemLobCommon *mem_loc = reinterpret_cast<ObMemLobCommon *>(buff);
      mem_loc->set_has_inrow_data(true); // Notice: inrow flag in disk locator ObLobCommon is still outrow!
      if (!mem_loc->has_extern() || pos < sizeof(ObLobLocator)) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN,"Lob: invalid mem locator", K(*mem_loc), K(pos));
      } else {
        ObMemLobExternHeader *mem_loc_extern = reinterpret_cast<ObMemLobExternHeader *>(mem_loc->data_);
        mem_loc_extern->payload_offset_ = static_cast<uint32_t>(pos - sizeof(ObLobLocator));
        mem_loc_extern->payload_size_ = static_cast<uint32_t>(lob_data_byte_len);

        // copy inrow data
        ObTextStringIterState state;
        ObString src_block_data;
        ObTextStringIter instr_iter(obj);
        if (OB_FAIL(instr_iter.init(0, session, &allocator))) {
          COMMON_LOG(WARN, "Lob: init text string iter failed", K(instr_iter));
        } else {
          while (OB_SUCC(ret)
                && pos < res_byte_len
                && (state = instr_iter.get_next_block(src_block_data)) == TEXTSTRING_ITER_NEXT) {
            MEMCPY(buff + pos, src_block_data.ptr(), src_block_data.length());
            pos += src_block_data.length();
          }
          OB_ASSERT(pos == res_byte_len);
          obj.set_lob_value(obj.get_type(), buff, static_cast<int32_t>(res_byte_len));
          obj.set_has_lob_header(); // must has lob header
        }
      }
    }
  }
  return ret;
}

// Notice: if input is an outrow lob, the result will be a templob!
int ObTextStringIter::convert_outrow_lob_to_inrow_templob(const ObObj &in_obj,
                                                          ObObj &out_obj,
                                                          const sql::ObBasicSessionInfo *session,
                                                          ObIAllocator *allocator,
                                                          bool allow_persist_inrow,
                                                          bool need_deep_copy)
{
  int ret = OB_SUCCESS;
  ObObjType type = in_obj.get_type();
  int64_t lob_data_byte_len = 0;
  int64_t pos = 0;
  bool is_pass_thougth = true;

  if (!in_obj.has_lob_header()) {
  } else if (!is_lob_storage(type)) {
  } else if (in_obj.is_null() || in_obj.is_nop_value()) {
  } else {
    ObLobLocatorV2 loc(in_obj.get_string(), in_obj.has_lob_header());
    if (!loc.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Lob: invalid lob loc", K(ret), K(loc), K(in_obj));
    } else if ((!loc.is_persist_lob() || allow_persist_inrow) &&
               (loc.is_inrow() || loc.is_simple())) { // do nothing
    } else if (loc.is_delta_temp_lob()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Lob: converting delta lob!", K(ret));
    } else if (OB_FAIL(loc.get_lob_data_byte_len(lob_data_byte_len))) {
      COMMON_LOG(WARN, "Lob: failed to get lob data byte length", K(ret), K(in_obj));
    } else if (lob_data_byte_len < 0 || lob_data_byte_len > UINT32_MAX) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "Lob: inrow lob data length error", K(lob_data_byte_len));
    } else {
      is_pass_thougth = false;
      ObTextStringResult new_tmp_lob(in_obj.get_type(), in_obj.has_lob_header(), allocator);
      if (OB_FAIL(new_tmp_lob.init(lob_data_byte_len))) {
        COMMON_LOG(WARN, "Lob: init tmp lob failed", K(ret), K(lob_data_byte_len));
      } else {
        // copy inrow data
        ObTextStringIterState state;
        ObString src_block_data;
        ObTextStringIter instr_iter(in_obj);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(instr_iter.init(0, session, allocator))) {
          COMMON_LOG(WARN, "Lob: init text string iter failed", K(instr_iter));
        } else {
          while (OB_SUCC(ret)
                && pos < lob_data_byte_len
                && (state = instr_iter.get_next_block(src_block_data)) == TEXTSTRING_ITER_NEXT) {
            if (OB_FAIL(new_tmp_lob.append(src_block_data))) {
              COMMON_LOG(WARN, "Lob: tmp lob append failed",
                K(ret), K(lob_data_byte_len), K(pos), K(src_block_data), K(new_tmp_lob));
            } else {
              pos += src_block_data.length();
            }
          }
          OB_ASSERT(pos == lob_data_byte_len);
          ObString res;
          new_tmp_lob.get_result_buffer(res);
          out_obj = in_obj; // copy meta
          out_obj.set_lob_value(in_obj.get_type(), res.ptr(), res.length());
          out_obj.set_has_lob_header(); // must has lob header
        }
      }
    }

    if (OB_SUCC(ret) && is_pass_thougth) {
      if (need_deep_copy) {
        if (OB_FAIL(ob_write_obj(*allocator, in_obj, out_obj))) {
          LOG_WARN("do deepy copy obj failed.", K(ret), K(in_obj));
        }
      } else {
        out_obj = in_obj;
      }
    }
  }
  return ret;
}

// ----- implementations of ObTextStringResult -----

int ObTextStringResult::calc_buffer_len(int64_t res_len)
{
  int ret = OB_SUCCESS;
  if (!(is_lob_storage(type_))) { // tinytext no has lob header
    buff_len_ = res_len;
  } else {
    if (!has_lob_header_) {
      buff_len_ = res_len;
    } else if (res_len < OB_MAX_LONGTEXT_LENGTH - MAX_TMP_LOB_HEADER_LEN) {
      // inrow lob with lob header
      bool has_extern = lib::is_oracle_mode(); // even oracle may not need extern for temp data
      ObMemLobExternFlags extern_flags(has_extern);
      res_len += sizeof(ObLobCommon);
      buff_len_ = ObLobLocatorV2::calc_locator_full_len(extern_flags, 0, static_cast<uint32_t>(res_len), false);
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("Lob: out row temp lob not implemented, not support length bigger than 512M",
        K(ret), K(this), K(pos_), K(buff_len_), K(res_len));
    }
  }
  return ret;
}

int ObTextStringResult::fill_temp_lob_header(const int64_t res_len)
{
  int ret = OB_SUCCESS;
  if (!has_lob_header_) { // do nothing
  } else if (OB_ISNULL(buffer_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("Lob: allocate memory for lob result failed", K(type_), K(buff_len_), K(ret));
  } else if (!(is_lob_storage(type_))) { // do nothing
  } else if (res_len <= OB_MAX_LONGTEXT_LENGTH - MAX_TMP_LOB_HEADER_LEN) {
    ObLobLocatorV2 locator(buffer_, static_cast<uint32_t>(buff_len_), has_lob_header_);
    // temp lob in oracle mode not need extern neither, for it does not have rowkey
    // However we mock extern failed in case of return it to old client
    ObMemLobExternFlags extern_flags(lib::is_oracle_mode());
    ObString rowkey_str;
    ObString empty_str;
    ObLobCommon lob_common;
    if (OB_FAIL(locator.fill(TEMP_FULL_LOB,
                             extern_flags,
                             rowkey_str,
                             &lob_common,
                             static_cast<uint32_t>(res_len + sizeof(ObLobCommon)),
                             0,
                             false))) {
      LOG_WARN("Lob: fill temp lob locator failed", K(type_), K(ret));
    } else if (OB_FAIL((locator.set_payload_data(&lob_common, empty_str)))) {
      LOG_WARN("Lob: set temp lob locator payload failed", K(type_), K(ret));
    }
    pos_ = buff_len_ - res_len; // only res_len could be used later
  } else { // outrow
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("Lob: out row temp lob not implemented", K(this), K(pos_), K(buff_len_), K(ret));
  }
  return ret;
}

int ObTextStringResult::init(int64_t res_len, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(buffer_) || is_init_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Lob: textstring result init already", K(ret), K(*this));
  } else if (!(ob_is_string_or_lob_type(type_) || is_lob_storage(type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Lob: unexpected expr result type for textstring result", K(ret), K(type_));
  } else if (OB_FAIL(calc_buffer_len(res_len))) {
    LOG_WARN("fail to calc buffer len", K(ret), K(res_len));
  } else if (buff_len_ == 0) {
    OB_ASSERT(has_lob_header_ == false); // empty result without header
  } else {
    buffer_ = OB_ISNULL(allocator)
              ? (char *)alloc_->alloc(buff_len_) : (char *)allocator->alloc(buff_len_);
    if (OB_ISNULL(buffer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Lob: allocation failed", K(ret), K(type_), K(buff_len_));
    } else if (OB_FAIL(fill_temp_lob_header(res_len))) { // string types will not fill lob header
      LOG_WARN("Lob: fill_temp_lob_header failed", K(ret), K(type_));
    }
  }
  if (OB_SUCC(ret)) {
    is_init_ = true;
  }
  return ret;
}

int ObTextStringResult::copy(const ObLobLocatorV2 *loc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(loc) || !loc->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (loc->size_ == 0) {
  } else {
    buff_len_ = loc->size_;
    buffer_ = (char *)alloc_->alloc(buff_len_);
    if (OB_ISNULL(buffer_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Lob: allocate memory for copy locator failed", K(loc), K(loc->size_), K(ret));
    } else {
      MEMCPY(buffer_, loc->ptr_, buff_len_);
      has_lob_header_ = loc->has_lob_header_;
	  }
  }
  return ret;
}

int ObTextStringResult::append(const char *buffer, int64_t len)
{
  int ret = OB_SUCCESS;
  if (!is_outrow_templob_) {
    if (len == 0) {
    } else if (pos_ + len > buff_len_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Lob: append content length too long", K(pos_), K(buff_len_), K(len), K(ret));
    } else {
      MEMCPY(buffer_ + pos_, buffer, len);
      pos_ += len;
    }
  } else {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("Lob: out row temp lob not implemented", K(this), K(pos_), K(buff_len_), K(ret));
  }
  return ret;
}

int ObTextStringResult::fill(int64_t pos, int c, int64_t len)
{
  int ret = OB_SUCCESS;
  if (!is_outrow_templob_) {
    if (len == 0) {
    } else if (pos + len > buff_len_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Lob: append content length too long", K(this), K(pos), K(len), K(ret));
    } else {
      MEMSET(buffer_ + pos_ + pos, c, len);
    }
  } else {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("Lob: out row temp lob not implemented", K(this), K(pos_), K(buff_len_), K(ret));
  }
  return ret;
}

int ObTextStringResult::write(const char *buffer, int64_t pos, int64_t len)
{
  int ret = OB_SUCCESS;
  if (!is_outrow_templob_) {
    if (len == 0) { // do nothing
    } else if (pos + len > buff_len_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Lob: append content length too long", K(this), K(pos), K(len), K(ret));
    } else {
      MEMCPY(buffer_ + pos, buffer, len);
    }
  } else {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("Lob: out row temp lob not implemented", K(this), K(pos_), K(buff_len_), K(ret));
  }
  return ret;
}

// ToDo: add state for curr, or beginning, or end, adjust payload size?
int ObTextStringResult::lseek(int64_t offset, int state)
{
  int ret = OB_SUCCESS;
  if (pos_ + offset < 0 || pos_ + offset > buff_len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Lob: lseek to invalid position", K(this), K(pos_), K(offset), K(state), K(ret));
  } else {
    pos_ += offset;
  }
  return ret;
}

// Notice: should move pos_ to new pos after write buffer (lseek) !
int ObTextStringResult::get_reserved_buffer(char *&empty_start, int64_t &empty_len)
{
  int ret = OB_SUCCESS;
  if (!is_outrow_templob_) {
    empty_start = buffer_ + pos_;
    empty_len = buff_len_ - pos_;
    if (empty_len < 0) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("Lob: no remaining", K(this), K(pos_), K(buff_len_), K(ret));
    }
  } else {
    ret = OB_NOT_IMPLEMENT;
    LOG_WARN("Lob: out row temp lob not implemented", K(this), K(pos_), K(buff_len_), K(ret));
  }
  return ret;
}

int ObTextStringResult::ob_convert_obj_temporay_lob(ObObj &obj, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObObjType type = obj.get_type();
  if (!is_lob_storage(type)) {
  } else if (obj.has_lob_header()) {
    ret = OB_UNEXPECT_INTERNAL_ERROR;
    COMMON_LOG(WARN, "Lob: not support to convert obj has lob header to tempory lob", K(ret), K(obj));
  } else if (obj.is_null() || obj.is_nop_value()) {
  } else {
    ObString row_str = obj.get_string();
    ObTextStringResult new_tmp_lob(obj.get_type(), true, &allocator);
    if (OB_FAIL(new_tmp_lob.init(row_str.length()))) {
      COMMON_LOG(WARN, "Lob: init tmp lob failed", K(ret), K(row_str.length()));
    } else if (OB_FAIL(new_tmp_lob.append(row_str))) {
      COMMON_LOG(WARN, "Lob: append failed", K(ret), K(new_tmp_lob), K(row_str));
    } else {
      ObString result;
      new_tmp_lob.get_result_buffer(result);
      obj.set_lob_value(type, result.ptr(), static_cast<int32_t>(result.length()));
      if (new_tmp_lob.has_lob_header()) {
        obj.set_has_lob_header();
      }
    }
  }
  return ret;
}

int ObTextStringResult::ob_convert_datum_temporay_lob(ObDatum &datum,
                                                      const ObObjMeta &in_obj_meta,
                                                      const ObObjMeta &out_obj_meta,
                                                      ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObObjType type = in_obj_meta.get_type();
  if (!is_lob_storage(type)) {
  } else if (in_obj_meta.has_lob_header()
             || !out_obj_meta.has_lob_header()
             || (in_obj_meta.get_type_class() != out_obj_meta.get_type_class())) {
    ret = OB_UNEXPECT_INTERNAL_ERROR;
    COMMON_LOG(WARN, "Lob: not support to convert obj has lob header to tempory lob",
               K(ret), K(datum), K(in_obj_meta), K(out_obj_meta));
  } else if (datum.is_null() || datum.is_nop()) {
  } else {
    ObString row_str = datum.get_string();
    ObTextStringResult new_tmp_lob(out_obj_meta.get_type(), true, &allocator);
    if (OB_FAIL(new_tmp_lob.init(row_str.length()))) {
      COMMON_LOG(WARN, "Lob: init tmp lob failed", K(ret), K(row_str.length()));
    } else if (OB_FAIL(new_tmp_lob.append(row_str))) {
      COMMON_LOG(WARN, "Lob: append failed", K(ret), K(new_tmp_lob), K(row_str));
    } else {
      ObString result;
      new_tmp_lob.get_result_buffer(result);
      datum.set_string(result);
    }
  }
  return ret;
}

}
}
