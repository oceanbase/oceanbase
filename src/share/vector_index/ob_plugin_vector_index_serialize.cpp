/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE
#include "ob_plugin_vector_index_serialize.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{
namespace share
{
/*
 * ObOStreamBuf implement
 * */
std::streamsize ObOStreamBuf::xsputn(const char* s, std::streamsize count)
{
  std::streamsize written_size = 0;
  std::streamsize left_size = 0;
  if (count == 0) {
    // do nothing
  } else if (OB_ISNULL(s)) {
    last_error_code_ = OB_INVALID_ARGUMENT;
  }
  while (is_valid() && is_success() && written_size < count) {
    left_size = epptr() - pptr();
    std::streamsize sub_size = std::min(count - written_size, left_size);
    MEMCPY(pptr(), s + written_size, sub_size);
    pbump(static_cast<int>(sub_size));
    written_size += sub_size;
    if (written_size < count) {
      last_error_code_ = do_callback();
    }
  }
  return written_size;
}

ObOStreamBuf::int_type ObOStreamBuf::overflow(int_type ch)
{
  if (is_valid() && is_success()) {
    if (ch != traits_type::eof()) {
      *pptr() = traits_type::to_char_type(ch);
      pbump(1);
    }
    last_error_code_ = do_callback();
  }
  return ch;
}

int ObOStreamBuf::do_callback()
{
  int ret = OB_SUCCESS;
  int64_t data_size = pptr() - pbase();
  if (0 < data_size) {
    if (OB_FAIL(cb_(pbase(), data_size, cb_param_))) {
      LOG_WARN("failed to do callback", K(ret));
    } else {
      setp(data_, data_ + capacity_ - 1); // reset to clear write buffer
    }
  }
  return ret;
}

void ObOStreamBuf::check_finish()
{
  if (is_valid() && is_success()) {
    last_error_code_ = do_callback();
  }
}

/*
 * ObIStreamBuf implement
 * */
int ObIStreamBuf::init()
{
  int ret = OB_SUCCESS;
  if (is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init istreambuf twice", K(ret));
  } else if (OB_FAIL(do_callback())) {
    last_error_code_ = ret;
    LOG_WARN("failed to do callback", K(ret));
  }
  return ret;
}

ObIStreamBuf::pos_type ObIStreamBuf::seekoff(off_type off, std::ios_base::seekdir dir, std::ios_base::openmode mode)
{
  UNUSED(mode);
  pos_type ret = 0;
  if (is_success()) {
    if (!is_valid()) {
      last_error_code_ = do_callback();
    }
    if (is_valid() && is_success()) {
      if (std::ios_base::cur == dir) {
        gbump(static_cast<int>(off));
      } else if (std::ios_base::end == dir) {
        setg(eback(), egptr() + off, egptr());
      } else if (std::ios_base::beg == dir) {
        setg(eback(), eback() + off, egptr());
      }
      ret = gptr() - eback();
    }
  }
  return ret;
}

ObIStreamBuf::pos_type ObIStreamBuf::seekpos(pos_type pos, std::ios_base::openmode mode)
{
  return seekoff(pos, std::ios_base::beg, mode);
}

std::streamsize ObIStreamBuf::xsgetn(char* s, std::streamsize n)
{
  std::streamsize get_size = 0;
  std::streamsize data_size = 0;
  if (n == 0) {
    // do nothing
  } else if (OB_ISNULL(s)) {
    last_error_code_ = OB_INVALID_ARGUMENT;
  } else if (is_success() && !is_valid()) {
    last_error_code_ = do_callback();
  }
  while (is_valid() && is_success() && get_size < n) {
    data_size = egptr() - gptr();
    std::streamsize sub_size = std::min(n - get_size, data_size);
    MEMCPY(s + get_size, gptr(), sub_size);
    gbump(static_cast<int>(sub_size));
    get_size += sub_size;
    if (get_size < n) {
      last_error_code_ = do_callback();
    }
  }
  return get_size;
}

ObIStreamBuf::int_type ObIStreamBuf::underflow()
{
  int_type ch = traits_type::eof();
  if (is_success()) {
    if (!is_valid()) {
      last_error_code_ = do_callback();
    }
    if (is_success() && is_valid()) {
      if (gptr() < egptr()) { // at least one readable char
        ch = traits_type::to_int_type(*gptr());
      } else {
        last_error_code_ = do_callback();
        if (is_success() && gptr() < egptr()) {
          ch = traits_type::to_int_type(*gptr());
        }
      }
    }
  }
  return ch;
}

int ObIStreamBuf::do_callback()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cb_(data_, capacity_, capacity_, cb_param_))) { // use data_ instead of eback() to change data_
    LOG_WARN("failed to do callback", K(ret));
  } else {
    setg(data_, data_, data_ + capacity_); // fill the read buffer
  }
  return ret;
}

int ObVectorIndexSerializer::serialize_meta(ObVectorIndexSegment *segment, ObOStreamBuf::CbParam &cb_param, ObOStreamBuf::Callback &cb)
{
  int ret = OB_SUCCESS;
  int64_t serde_size = segment->get_serialize_meta_size();
  char* serde_buf = nullptr;
  int64_t pos = 0;
  if (OB_ISNULL(serde_buf = reinterpret_cast<char*>(allocator_.alloc(serde_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory fail", K(ret), K(serde_size));
  } else if (OB_FAIL(segment->serialize_meta(serde_buf, serde_size, pos))) {
    LOG_WARN("serialize meta fail", K(ret), K(serde_size), KP(serde_buf), K(segment->get_serialize_meta_size()), KPC(segment));
  } else if (OB_FAIL(cb(serde_buf, pos, cb_param))) {
    LOG_WARN("failed to do callback", K(ret), K(serde_size), K(pos));
  } else {
    LOG_INFO("[VECTOR INDEX] build segment meta serde data success",
        K(serde_size), K(pos), KP(serde_buf), KPC(segment));
  }
  return ret;
}

/*
 * ObVectorIndexSerializer implement
 * */
int ObVectorIndexSerializer::serialize(ObVectorIndexSegment *segment, ObOStreamBuf::CbParam &cb_param, ObOStreamBuf::Callback &cb, uint64_t tenant_id, const int64_t capacity)
{
  int ret = OB_SUCCESS;
  ObHNSWSerializeCallback::CbParam &param = static_cast<ObHNSWSerializeCallback::CbParam&>(cb_param);
  char *data = nullptr;
  if (OB_ISNULL(segment) || 0 > capacity) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(segment), K(capacity));
  } else if (OB_ISNULL(data = static_cast<char*>(allocator_.alloc(capacity * sizeof(char))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc serialize buffer", K(ret), K(capacity));
  } else if (param.need_serde_meta_ && OB_FAIL(serialize_meta(segment, cb_param, cb))) {
    LOG_WARN("serialize segment meta fail", K(ret), KPC(segment));
  } else {
    ObOStreamBuf streambuf(data, capacity, cb_param, cb);
    std::ostream out(&streambuf);
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, "VIndexVsagADP"));
    lib::ObLightBacktraceGuard light_backtrace_guard(false);
    if (OB_FAIL(segment->fserialize(out))) {
      LOG_WARN("fail to do vsag serialize", K(ret));
      if (streambuf.get_error_code() != OB_SUCCESS && streambuf.get_error_code() != OB_ITER_END) {
        ret = streambuf.get_error_code();
        LOG_WARN("serialize streambuf has fail", K(ret));
      }
    } else {
      streambuf.check_finish(); // do last callback to ensure all the data is written
      if (OB_FAIL(streambuf.get_error_code())) {
        LOG_WARN("failed to serialize", K(ret));
      }
    }
  }
  return ret;
}

int ObVectorIndexSerializer::deserialize(ObVectorIndexSegmentHandle &segment_handle, ObIStreamBuf::CbParam &cb_param, ObIStreamBuf::Callback &cb, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  char *data = nullptr;
  ObIStreamBuf streambuf(nullptr, 0, cb_param, cb);
  std::istream in(&streambuf);
  if (OB_FAIL(streambuf.init())) {
    if (ret == OB_ITER_END) {
      LOG_INFO("[vec index deserialize] read table is empty, just return");
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to init istreambuf", K(ret));
    }
  } else {
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, "VIndexVsagADP"));
    lib::ObLightBacktraceGuard light_backtrace_guard(false);
    if (OB_FAIL(segment_handle->fdeserialize(in))) {
      LOG_WARN("fail to do vsag deserialize", K(ret));
      if (streambuf.get_error_code() != OB_SUCCESS && streambuf.get_error_code() != OB_ITER_END) {
        ret = streambuf.get_error_code();
        LOG_WARN("deserialize streambuf has fail", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(streambuf.get_error_code())) {
    if (ret == OB_ITER_END) {
      LOG_INFO("[vec index deserialize] read table finish, just return");
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to deserialize", K(ret));
    }
  }
  return ret;
}

int ObVectorIndexSerializer::deserialize(ObVectorIndexMeta& meta, ObIStreamBuf::CbParam &cb_param, ObHNSWDeserializeCallback &callback, uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObIStreamBuf::Callback cb = callback;
  char *data = nullptr;
  ObIStreamBuf streambuf(nullptr, 0, cb_param, cb);
  std::istream in(&streambuf);
  ObHNSWDeserializeCallback::CbParam &param = static_cast<ObHNSWDeserializeCallback::CbParam&>(cb_param);
  ObPluginVectorIndexAdaptor *adp_ptr = static_cast<ObPluginVectorIndexAdaptor*>(callback.adp_);
  if (OB_FAIL(streambuf.init())) {
    if (ret == OB_ITER_END) {
      LOG_INFO("[vec index deserialize] read table is empty, just return");
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to init istreambuf", K(ret));
    }
  } else if (OB_ISNULL(adp_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid adp", K(ret));
  } else if (meta.incrs_.count() > 0 || meta.bases_.count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meta is invalid for old format", K(ret), K(meta));
  } else {
    // TODO: add old index segment compatibility logic here
    lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, "VIndexVsagADP"));
    lib::ObLightBacktraceGuard light_backtrace_guard(false);
    ObVectorIndexSegmentMeta &seg_meta = meta.bases_.at(0);
    if (OB_FAIL(seg_meta.segment_handle_->fdeserialize(in))) {
      LOG_WARN("fail to do vsag deserialize", K(ret));
      if (streambuf.get_error_code() != OB_SUCCESS && streambuf.get_error_code() != OB_ITER_END) {
        ret = streambuf.get_error_code();
        LOG_WARN("deserialize streambuf has fail", K(ret));
      }
    } else if (OB_FALSE_IT(seg_meta.segment_handle_->set_is_base())) {
    } else if (OB_FAIL(seg_meta.segment_handle_->immutable_optimize())) {
      LOG_WARN("fail to index immutable_optimize", K(ret));
    } else if (OB_FAIL(seg_meta.deep_copy_seg_key(param.start_key_, param.end_key_))) {
      LOG_WARN("failed to deep copy seg key", K(ret), K(seg_meta));
    } else {
      // MARK AS legacy base segment (backward compatibility)
      seg_meta.seg_type_ = ObVectorIndexSegmentType::LEGACY_BASE;
      seg_meta.index_type_ = param.index_type_;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(streambuf.get_error_code())) {
    if (ret == OB_ITER_END) {
      LOG_INFO("[vec index deserialize] read table finish, just return");
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to deserialize", K(ret));
    }
  }
  return ret;
}

int ObHNSWDeserializeCallback::operator()(char*& data, const int64_t data_size, int64_t &read_size, share::ObIStreamBuf::CbParam &cb_param)
{
  UNUSED(data_size);
  int ret = OB_SUCCESS;
  blocksstable::ObDatumRow *row = nullptr;
  ObDatum key_datum;
  ObDatum data_datum;
  ObHNSWDeserializeCallback::CbParam &param = static_cast<ObHNSWDeserializeCallback::CbParam&>(cb_param);
  ObTableScanIterator *row_iter = static_cast<ObTableScanIterator *>(param.iter_);
  ObIAllocator *allocator = param.allocator_;
  ObTextStringIter *&str_iter = param.str_iter_;
  bool is_vec_tablet_rebuild = param.is_vec_tablet_rebuild_;
  bool is_need_unvisible_row = param.is_need_unvisible_row_;
  ObVectorIndexSegmentMeta *seg_meta = param.seg_meta_;
  ObTextStringIterState state;
  ObString src_block_data;
  if (!param.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid row_iter", K(ret), K(row_iter));
  } else {
    data = nullptr;
    read_size = 0;
    do {
      if (OB_NOT_NULL(str_iter)) {
        // try to get current next block
        state = str_iter->get_next_block(src_block_data);
        if (state == TEXTSTRING_ITER_NEXT) {
          // get next block success
          data = src_block_data.ptr();
          read_size = src_block_data.length();
        } else if (state == TEXTSTRING_ITER_END) {
          // current lob is end, need to switch to next lob
          // release current str iter
          str_iter->~ObTextStringIter();
          allocator->free(str_iter);
          str_iter = nullptr;
          allocator->reuse();
        } else {
          ret = (str_iter->get_inner_ret() != OB_SUCCESS) ?
                str_iter->get_inner_ret() : OB_INVALID_DATA;
          LOG_WARN("iter state invalid", K(ret), K(state), KPC(str_iter));
          // return error, release current str iter
          str_iter->~ObTextStringIter();
          allocator->free(str_iter);
          str_iter = nullptr;
          allocator->reuse();
        }
      }
      if (OB_SUCC(ret) && OB_ISNULL(str_iter)) {
        // we should get next str_iter
        if (OB_FAIL(row_iter->get_next_row(row))) {
          LOG_WARN("failed to get next row", K(ret));
        } else if (OB_ISNULL(row) || row->get_column_count() < 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid row", K(ret), K(row));
        } else {
          // TODO: old index segment compatibility, count execute times as blocks_cnt
          bool skip_this_row = false;
          key_datum = row->storage_datums_[0];
          data_datum = row->storage_datums_[1];

          LOG_INFO("[vec index debug] show key and data for vsag deserialize", K(key_datum), K(data_datum), K(is_need_unvisible_row), K(is_vec_tablet_rebuild));

          const ObString key_data = key_datum.get_string();
          MEMCPY(param.end_key_buf_, key_data.ptr(), key_data.length());
          param.end_key_.assign(param.end_key_buf_, key_data.length());

          if (OB_FALSE_IT(ObVecIndexAsyncTaskUtil::get_row_need_skip_for_compatibility(*row, is_need_unvisible_row, skip_this_row))) {
          } else if (skip_this_row) {
            LOG_INFO("skip deseriable row", K(key_datum), K(is_need_unvisible_row), K(is_vec_tablet_rebuild)); // continue;
          } else if (key_datum.get_string().suffix_match("_meta_data")) {
            ObPluginVectorIndexAdaptor *adp_ptr = static_cast<ObPluginVectorIndexAdaptor*>(adp_);
            ObString meta_data = data_datum.get_string();
            if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(allocator, ObLongTextType, true, meta_data, nullptr))) {
              LOG_WARN("read real data fail", K(ret), K(meta_data.length()));
            } else if (OB_FAIL(adp_ptr->deserialize_snap_meta(meta_data))) {
              LOG_WARN("derserialize meta data fail", K(ret), K(meta_data.length()));
            } else {
              LOG_INFO("vector index meta data", K(key_datum.get_string()));
            }
          } else if (OB_ISNULL(str_iter = OB_NEWx(ObTextStringIter, allocator, ObLongTextType, CS_TYPE_BINARY, data_datum.get_string(), true))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to new ObTextStringIter", KR(ret));
          } else if (OB_FAIL(str_iter->init(0, NULL, allocator))) {
            LOG_WARN("init lob str iter failed ", K(ret));
          } else if (index_type_ == VIAT_MAX) {
            // TODO: old index segment compatibility, use startkey/index_type get index_type
            ObPluginVectorIndexAdaptor *adp = static_cast<ObPluginVectorIndexAdaptor*>(adp_);
            ObCollationType calc_cs_type = CS_TYPE_UTF8MB4_GENERAL_CI;
            uint32_t idx_ipivf_sq = ObCharset::locate(calc_cs_type, key_datum.get_string().ptr(), key_datum.get_string().length(),
                                       "ipivf_sq", 8, 1);
            uint32_t idx_ipivf = ObCharset::locate(calc_cs_type, key_datum.get_string().ptr(), key_datum.get_string().length(),
                                       "ipivf", 5, 1);
            uint32_t idx_sq = ObCharset::locate(calc_cs_type, key_datum.get_string().ptr(), key_datum.get_string().length(),
                                       "hnsw_sq", 7, 1);
            uint32_t idx_bq = ObCharset::locate(calc_cs_type, key_datum.get_string().ptr(), key_datum.get_string().length(),
                                       "hnsw_bq", 7, 1);
            uint32_t hgraph_idx = ObCharset::locate(calc_cs_type, key_datum.get_string().ptr(), key_datum.get_string().length(),
                                       "hgraph", 6, 1);
            if (OB_ISNULL(adp)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get invalid adp", K(ret));
            } else if (OB_FALSE_IT(MEMCPY(param.start_key_buf_, key_data.ptr(), key_data.length()))) {
            } else if (OB_FALSE_IT(param.start_key_.assign(param.start_key_buf_, key_data.length()))) {
            } else if (nullptr != seg_meta && key_datum.get_string().compare(seg_meta->start_key_) != 0) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("first row of vector index is not expected row", K(ret), KPC(seg_meta), KPC(row));
            } else if (idx_ipivf_sq > 0) {
              index_type_ = VIAT_IPIVF_SQ;
            } else if (idx_ipivf > 0) {
              index_type_ = VIAT_IPIVF;
            } else if (idx_sq > 0) {
              index_type_ = VIAT_HNSW_SQ;
            } else if (idx_bq > 0) {
              index_type_ = VIAT_HNSW_BQ;
            } else if (hgraph_idx > 0) {
              index_type_ = VIAT_HGRAPH;
            } else {
              index_type_ = VIAT_HNSW;
            }
            if (OB_FAIL(ret)) {
            } else if (nullptr != seg_meta) {
              ObString real_data;
              int64_t pos = 0;
              if (OB_FAIL(adp->create_snap_segment(index_type_, *seg_meta))) {
                LOG_WARN("init seg data fail", K(ret), K(index_type_), KPC(seg_meta));
              } else if (! seg_meta->has_segment_meta_row_) { // skip if no segment meta row
              } else if (OB_FAIL(str_iter->get_full_data(real_data))) {
                LOG_WARN("get full data fail", K(ret));
              } else if (OB_FAIL(seg_meta->segment_handle_->deserialize_meta(real_data.ptr(), real_data.length(), pos))) {
                LOG_WARN("deserialize meta fail", K(ret), K(real_data.length()), K(pos));
              } else {
                str_iter->~ObTextStringIter();
                allocator->free(str_iter);
                str_iter = nullptr;
                allocator->reuse();
              }
            } else if (OB_FAIL(adp->try_init_snap_for_deserialize(index_type_))) {
              LOG_WARN("failed to init vector snap data", K(ret), K(index_type_));
            } else {
              param.index_type_ = index_type_;
            }

            LOG_INFO("HgraphIndex vector index get key data from snap_index_table", K(ret), K(is_vec_tablet_rebuild), K(index_type_), K(key_datum.get_string()));
            if (OB_FAIL(ret)) {
            } else if (is_vec_tablet_rebuild) { // only do once
              if (OB_FAIL(ObVecIndexAsyncTaskUtil::init_tablet_rebuild_new_adapter(adp, key_datum.get_string()))) {
                LOG_WARN("fail to init tablet rebuild new adpater", K(ret));
              } else {
                LOG_INFO("init tablet rebuild new adapter", K(ret), KPC(adp));
              }
            }
          }
        }
      }
    } while (OB_SUCC(ret) && OB_ISNULL(data));

    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
      ObPluginVectorIndexAdaptor *adp_ptr = static_cast<ObPluginVectorIndexAdaptor*>(adp_);
      if (OB_ISNULL(adp_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid adp", K(ret));
      } else if (nullptr != seg_meta && index_type_ == VIAT_MAX) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("segment donot read data", KPC(seg_meta));
      } else if (!adp_ptr->is_snap_inited() && !is_vec_tablet_rebuild) {
        // If it’s vec tablet rebuild and nothing was deserialized, then there’s no need to create snap_index here; the outer layer will create it.
        // Otherwise, it may cause a mismatch between the index data and the index type.
        if (OB_FAIL(adp_ptr->init_snap_data_without_lock(VIAT_HNSW))) {
          LOG_WARN("failed to init hnsw mem data", K(ret));
        } else {
          ret = OB_ITER_END;
        }
      } else {
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}

int ObHNSWSerializeCallback::operator()(const char *data, const int64_t data_size, share::ObOStreamBuf::CbParam &cb_param)
{
  int ret = OB_SUCCESS;
  ObLobLocatorV2 src_lob(const_cast<char*>(data), data_size, false); // data from vsag must has no header
  ObHNSWSerializeCallback::CbParam &param = static_cast<ObHNSWSerializeCallback::CbParam&>(cb_param);
  ObVecIdxSnapshotDataWriteCtx *vctx = reinterpret_cast<ObVecIdxSnapshotDataWriteCtx*>(param.vctx_);
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  ObLobAccessParam lob_param;
  lob_param.set_tmp_allocator(param.tmp_allocator_);
  lob_param.allocator_ = param.allocator_;
  lob_param.ls_id_ = vctx->get_ls_id();
  lob_param.tablet_id_ = vctx->get_data_tablet_id();
  lob_param.lob_meta_tablet_id_ = vctx->get_lob_meta_tablet_id();
  lob_param.lob_piece_tablet_id_ = vctx->get_lob_piece_tablet_id();
  lob_param.inrow_threshold_ = param.lob_inrow_threshold_;
  lob_param.src_tenant_id_ = MTL_ID(); // 补数据不会跨租户
  lob_param.coll_type_ = CS_TYPE_BINARY;
  lob_param.offset_ = 0;
  lob_param.scan_backward_ = false;
  lob_param.is_total_quantity_log_ = true;
  lob_param.sql_mode_ = SMO_DEFAULT;
  lob_param.timeout_ = param.timeout_;
  lob_param.lob_common_ = nullptr;
  lob_param.specified_tablet_id_ = vctx->get_snap_tablet_id();
  ret = lob_param.snapshot_.assign(*reinterpret_cast<transaction::ObTxReadSnapshot*>(param.snapshot_));
  if (OB_FAIL(ret)) {
    LOG_WARN("assign snapshot fail", K(ret));
  } else {
    lob_param.tx_desc_ = reinterpret_cast<transaction::ObTxDesc*>(param.tx_desc_);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get lob manager nullptr", K(ret));
  } else if (OB_FAIL(lob_mngr->append(lob_param, src_lob))) {
    LOG_WARN("lob append failed.", K(ret));
  } else {
    LOG_INFO("[vec index debug] success write one data into lob tablet", K(src_lob),
              K(lob_param.lob_meta_tablet_id_), KPC(lob_param.tx_desc_));
    ObString dest_str(lob_param.handle_size_, (char*)lob_param.lob_common_);
    if (OB_FAIL(vctx->get_vals().push_back(ObVecIdxSnapshotBlockData(false/*is_meta*/, dest_str)))) {
      LOG_WARN("fail to push dest lob into ctx val array", K(ret));
    }
  }
  return ret;
}

};
};
