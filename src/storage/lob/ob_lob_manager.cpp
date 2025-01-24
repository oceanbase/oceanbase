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

#include "ob_lob_manager.h"
#include "observer/ob_server.h"
#include "storage/lob/ob_lob_location.h"
#include "storage/lob/ob_lob_handler.h"
#include "storage/lob/ob_lob_locator_struct.h"
#include "storage/lob/ob_lob_tablet_dml.h"

namespace oceanbase
{
namespace storage
{

static int check_write_length(ObLobAccessParam& param, int64_t expected_len)
{
  int ret = OB_SUCCESS;
  if (ObLobDataOutRowCtx::OpType::SQL != param.op_type_) {
    // skip not full write
  } else if (param.byte_size_ != expected_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("size not match", K(ret), K(expected_len), K(param.byte_size_));
  }
  return ret;
}

const ObLobCommon ObLobManager::ZERO_LOB = ObLobCommon();

// for only one lob meta in mysql mode, we can have no char len here
static int is_store_char_len(ObLobAccessParam& param, int64_t store_chunk_size, int64_t add_len)
{
  int ret = OB_SUCCESS;
  if (! lib::is_mysql_mode()) {
    LOG_DEBUG("not mysql mode", K(add_len), K(store_chunk_size), K(param));
  } else if (! param.is_char()) {
    LOG_DEBUG("not text", K(add_len), K(store_chunk_size), K(param));
  } else if (store_chunk_size <= (param.byte_size_ + add_len)) {
    LOG_DEBUG("not single", K(add_len), K(store_chunk_size), K(param));
  } else if (param.tablet_id_.is_inner_tablet()) {
    LOG_DEBUG("inner table skip", K(add_len), K(store_chunk_size), K(param));
  } else {
    uint64_t tenant_id = param.tenant_id_;
    uint64_t data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("failed to get data version", K(ret), K(store_chunk_size), K(tenant_id));
    } else if (data_version < MOCK_DATA_VERSION_4_2_3_0) {
      LOG_DEBUG("before 4.2.3", K(add_len), K(store_chunk_size), K(param));
    } else if (data_version >= DATA_VERSION_4_3_0_0 && data_version < DATA_VERSION_4_3_2_0) {
      LOG_DEBUG("before 4.3.2", K(add_len), K(store_chunk_size), K(param));
    } else {
      // sinlge not need char_len after >= 4.3.2 or (>= 4.2.3 and < 4.3.0
      param.is_store_char_len_ = false;
      LOG_DEBUG("not store char_len for single piece", K(add_len), K(store_chunk_size), K(param));
    }
  }
  return ret;
}

int ObLobManager::mtl_new(ObLobManager *&m) {
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  auto attr = SET_USE_500("LobManager");
  m = OB_NEW(ObLobManager, attr, tenant_id);
  if (OB_ISNULL(m)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(tenant_id));
  }
  return ret;
}

void ObLobManager::mtl_destroy(ObLobManager *&m)
{
  if (OB_UNLIKELY(nullptr == m)) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "meta mem mgr is nullptr", KP(m));
  } else {
    OB_DELETE(ObLobManager, oceanbase::ObModIds::OMT_TENANT, m);
    m = nullptr;
  }
}

int ObLobManager::mtl_init(ObLobManager* &m)
{
  return m->init();
}

int ObLobManager::init()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  lib::ObMemAttr mem_attr(tenant_id, "LobAllocator", ObCtxIds::LOB_CTX_ID);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLobManager init twice.", K(ret));
  } else if (OB_FAIL(allocator_.init(common::ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE, mem_attr))) {
    LOG_WARN("init allocator failed.", K(ret));
  } else if (OB_FAIL(ext_info_log_allocator_.init(
      common::ObMallocAllocator::get_instance(),
      OB_MALLOC_NORMAL_BLOCK_SIZE,
      lib::ObMemAttr(tenant_id, "ExtInfoLog", ObCtxIds::LOB_CTX_ID)))) {
    LOG_WARN("init ext info log allocator failed.", K(ret));
  } else {
    OB_ASSERT(sizeof(ObLobCommon) == sizeof(uint32));
    lob_ctx_.lob_meta_mngr_ = &meta_manager_;
    lob_ctx_.lob_piece_mngr_ = &piece_manager_;
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

int ObLobManager::start()
{
  int ret = OB_SUCCESS;
  // TODO
  return ret;
}

int ObLobManager::stop()
{
  STORAGE_LOG(INFO, "[LOB]stop");
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else {
    // TODO
    // 1. 触发LobOperator中内存数据的异步flush
    // 2. 清理临时LOB
  }
  return ret;
}

void ObLobManager::wait()
{
  STORAGE_LOG(INFO, "[LOB]wait");
  // TODO
  // 1. 等待LobOperator中内存数据的异步flush完成
}

void ObLobManager::destroy()
{
  STORAGE_LOG(INFO, "[LOB]destroy");
  // TODO
  // 1. LobOperator.destroy()
  allocator_.reset();
  is_inited_ = false;
}

// Only use for default lob col val
int ObLobManager::fill_lob_header(ObIAllocator &allocator, ObString &data, ObString &out)
{
  int ret = OB_SUCCESS;
  void* buf = allocator.alloc(data.length() + sizeof(ObLobCommon));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for lob data", K(ret), K(data));
  } else {
    ObLobCommon *lob_data = new(buf)ObLobCommon();
    MEMCPY(lob_data->buffer_, data.ptr(), data.length());
    out.assign_ptr(reinterpret_cast<char*>(buf), data.length() + sizeof(ObLobCommon));
  }
  return ret;
}

// Only use for default lob col val.
int ObLobManager::fill_lob_header(
    ObIAllocator &allocator,
    ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (datum.is_null() || datum.is_nop_value()) {
  } else {
    ObString data = datum.get_string();
    ObString out;
    if (OB_FAIL(ObLobManager::fill_lob_header(allocator, data, out))) {
      LOG_WARN("failed to fill lob header for column.", K(data));
    } else {
      datum.set_string(out);
    }
  }
  return ret;
}

// Only use for default lob col val
int ObLobManager::fill_lob_header(ObIAllocator &allocator,
    const ObIArray<share::schema::ObColDesc> &column_ids,
    blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
    if (column_ids.at(i).col_type_.is_lob_storage()) {
      if (OB_FAIL(fill_lob_header(allocator, datum_row.storage_datums_[i]))) {
        LOG_WARN("failed to fill lob header for column.", K(i), K(column_ids), K(datum_row));
      }
    }
  }
  return ret;
}


// delta tmp lob locator
// Content:
// ObMemLobCommon |
// tmp delta disk locator | -> [ObLobCommon : {inrow : 1, init : 0}]
// inline buffer | [tmp_header][persis disk locator][tmp_diff][inline_data]
int ObLobManager::build_tmp_delta_lob_locator(ObIAllocator &allocator,
    ObLobLocatorV2 *persist,
    const ObString &data,
    bool is_locator,
    ObLobDiffFlags flags,
    uint8_t op,
    uint64_t offset, // ori offset
    uint64_t len, // ori len
    uint64_t dst_offset,
    ObLobLocatorV2 &out)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(persist) || !persist->is_persist_lob()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid persist lob locator", K(ret), KPC(persist));
  } else {
    // calc res len
    uint64_t res_len = ObLobLocatorV2::MEM_LOB_COMMON_HEADER_LEN;
    uint64_t data_len = data.length();
    ObString persist_disk_loc;
    bool need_out_row = false;
    if (need_out_row) {
      ret = OB_NOT_IMPLEMENT;
    } else {
      if (OB_FAIL(persist->get_disk_locator(persist_disk_loc))) {
        LOG_WARN("get persist disk locator failed.", K(ret));
      } else {
        if (!is_locator) {
          data_len += sizeof(ObMemLobCommon) + sizeof(ObLobCommon);
        }
        res_len += sizeof(ObLobCommon) + persist_disk_loc.length() + sizeof(ObLobDiffHeader) + sizeof(ObLobDiff) + data_len;
      }
    }
    char *buf = nullptr;
    if (OB_SUCC(ret)) {
      buf = reinterpret_cast<char*>(allocator.alloc(res_len));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for new lob locator", K(ret), K(res_len));
      }
    }

    // build lob locator common
    if (OB_SUCC(ret)) {
      ObMemLobCommon *mem_common = new(buf)ObMemLobCommon(ObMemLobType::TEMP_DELTA_LOB, false);
      // build disk locator
      ObLobCommon *lob_common = new(mem_common->data_)ObLobCommon();
      // build inline buffer
      ObLobDiffHeader *diff_header = new(lob_common->buffer_)ObLobDiffHeader();
      diff_header->diff_cnt_ = 1;
      diff_header->persist_loc_size_ = persist_disk_loc.length();

      // copy persist locator
      MEMCPY(diff_header->data_, persist_disk_loc.ptr(), persist_disk_loc.length());
      char *diff_st = diff_header->data_ + persist_disk_loc.length();
      ObLobDiff *diff = new(diff_st)ObLobDiff();
      diff->ori_offset_ = offset;
      diff->ori_len_ = len;
      diff->offset_ = 0;
      diff->byte_len_ = data_len;
      diff->dst_offset_ = dst_offset;
      diff->type_ = static_cast<ObLobDiff::DiffType>(op);
      diff->flags_ = flags;

      char *diff_data = diff_header->get_inline_data_ptr();
      if (!is_locator) {
        ObMemLobCommon *diff_mem_common = new(diff_data)ObMemLobCommon(ObMemLobType::TEMP_FULL_LOB, false);
        ObLobCommon *diff_lob_common = new(diff_mem_common->data_)ObLobCommon();
        diff_data = diff_lob_common->buffer_;
      }
      MEMCPY(diff_data, data.ptr(), data.length());

      out.ptr_ = buf;
      out.size_ = res_len;
      out.has_lob_header_ = true;
    }
  }
  return ret;
}

// full tmp lob locator
// Content:
// ObMemLobCommon |
// ObMemLobOraCommon |
// disk locator | -> [ObLobCommon : {inrow : 1, init : 0}]
// inline buffer | [inline_data]
int ObLobManager::build_tmp_full_lob_locator(ObIAllocator &allocator,
    const ObString &data,
    common::ObCollationType coll_type,
    ObLobLocatorV2 &out)
{
  int ret = OB_SUCCESS;
  uint64_t res_len = ObLobLocatorV2::MEM_LOB_COMMON_HEADER_LEN;
  bool need_outrow = false;
  if (need_outrow) {
    ret = OB_NOT_IMPLEMENT;
  } else {
    res_len += sizeof(ObLobCommon) + data.length();
    char *buf = reinterpret_cast<char*>(allocator.alloc(res_len));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for new lob locator", K(ret), K(res_len));
    } else {
      ObMemLobCommon *mem_common = new(buf)ObMemLobCommon(ObMemLobType::TEMP_FULL_LOB, false);
      mem_common->set_read_only(false);
      char *next_ptr = mem_common->data_;
      // build disk locator
      ObLobCommon *lob_common = new(next_ptr)ObLobCommon();
      // copy data
      if (data.length() > 0) {
        MEMCPY(lob_common->buffer_, data.ptr(), data.length());
      }
      out.ptr_ = buf;
      out.size_ = res_len;
      out.has_lob_header_ = true;
    }
  }
  return ret;
}

int ObLobManager::query(
    ObLobAccessParam& param,
    ObString& output_data)
{
  int ret = OB_SUCCESS;
  bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else if (OB_FAIL(param.prepare())) {
    LOG_WARN("param prepare fail", K(ret), K(param));
  } else {
    ObLobCommon *lob_common = param.lob_common_;
    if (OB_ISNULL(lob_common)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("get lob data null.", K(ret));
    } else if (OB_FAIL(param.check_handle_size())) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (lob_common->in_row_ || (param.lob_locator_ != nullptr && param.lob_locator_->has_inrow_data())) {
      ObString data;
      if (param.lob_locator_ != nullptr && param.lob_locator_->has_inrow_data()) {
        if (OB_FAIL(param.lob_locator_->get_inrow_data(data))) {
          LOG_WARN("fail to get inrow data", K(ret), KPC(param.lob_locator_));
        }
      } else { // lob_common->in_row_
        if (lob_common->is_init_) {
          param.lob_data_ = reinterpret_cast<ObLobData*>(lob_common->buffer_);
          data.assign_ptr(param.lob_data_->buffer_, param.lob_data_->byte_size_);
        } else {
          data.assign_ptr(lob_common->buffer_, param.byte_size_);
        }
      }
      uint32_t byte_offset = param.offset_ > data.length() ? data.length() : param.offset_;
      uint32_t max_len = ObCharset::strlen_char(param.coll_type_, data.ptr(), data.length()) - byte_offset;
      uint32_t byte_len = (param.len_ > max_len) ? max_len : param.len_;
      ObLobCharsetUtil::transform_query_result_charset(param.coll_type_, data.ptr(), data.length(), byte_len, byte_offset);
      if (OB_UNLIKELY(data.length() < byte_offset + byte_len)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("data length is not enough.", K(ret), KPC(lob_common), KPC(param.lob_data_), K(byte_offset), K(byte_len));
      } else if (param.inrow_read_nocopy_) {
        output_data.assign_ptr(data.ptr() + byte_offset, byte_len);
      } else if (output_data.write(data.ptr() + byte_offset, byte_len) != byte_len) {
        ret = OB_ERR_INTERVAL_INVALID;
        LOG_WARN("failed to write buffer to output_data.", K(ret), K(output_data), K(byte_offset), K(byte_len));
      }
    } else if (OB_FAIL(query_outrow(param, output_data))) {
      LOG_WARN("query outrow fail", K(ret), K(param));
    }
  }
  return ret;
}

int ObLobManager::query_inrow_get_iter(
    ObLobAccessParam& param,
    ObString &data,
    uint32_t offset,
    bool scan_backward,
    ObLobQueryIter *&result)
{
  int ret = OB_SUCCESS;
  bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  uint32_t byte_offset = offset;
  uint32_t byte_len = param.len_;
  if (byte_offset > data.length()) {
    byte_offset = data.length();
  }
  if (byte_len + byte_offset > data.length()) {
    byte_len = data.length() - byte_offset;
  }
  if (is_char) {
    ObLobCharsetUtil::transform_query_result_charset(param.coll_type_, data.ptr(), data.length(), byte_len, byte_offset);
  }
  if (OB_UNLIKELY(data.length() < byte_offset + byte_len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("data length is not enough.", K(ret), K(byte_offset), K(param.len_));
  } else {
    ObLobInRowQueryIter* iter = OB_NEW(ObLobInRowQueryIter, ObMemAttr(MTL_ID(), "LobQueryIter"));
    if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alloc lob meta scan iterator fail", K(ret));
    } else if (OB_FAIL(iter->open(data, byte_offset, byte_len, param.coll_type_, scan_backward))) {
      LOG_WARN("do lob meta scan failed.", K(ret), K(data));
    } else {
      result = iter;
    }
  }
  return ret;
}

int ObLobManager::query(
    ObLobAccessParam& param,
    ObLobQueryIter *&result)
{
  int ret = OB_SUCCESS;
  bool is_in_row = false;
  bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else if (OB_FAIL(param.prepare())) {
    LOG_WARN("param prepare fail", K(ret), K(param));
  } else {
    ObLobCommon *lob_common = param.lob_common_;
    if (OB_ISNULL(lob_common)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("get lob data null.", K(ret));
    } else if (OB_FAIL(param.check_handle_size())) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (param.lob_locator_ != nullptr && param.lob_locator_->has_inrow_data()) {
      ObString data;
      if (OB_FAIL(param.lob_locator_->get_inrow_data(data))) {
        LOG_WARN("fail to get inrow data", K(ret), KPC(param.lob_locator_));
      } else if (OB_FAIL(query_inrow_get_iter(param, data, param.offset_, param.scan_backward_, result))) {
        LOG_WARN("fail to get inrow query iter", K(ret));
        if (OB_NOT_NULL(result)) {
          result->reset();
          OB_DELETE(ObLobQueryIter, "unused", result);
          result = nullptr;
        }
      }
    } else if (lob_common->in_row_) {
      ObString data;
      if (lob_common->is_init_) {
        param.lob_data_ = reinterpret_cast<ObLobData*>(lob_common->buffer_);
        data.assign_ptr(param.lob_data_->buffer_, param.lob_data_->byte_size_);
      } else {
        data.assign_ptr(lob_common->buffer_, param.byte_size_);
      }
      if (OB_FAIL(query_inrow_get_iter(param, data, param.offset_, param.scan_backward_, result))) {
        LOG_WARN("fail to get inrow query iter", K(ret));
        if (OB_NOT_NULL(result)) {
          result->reset();
          OB_DELETE(ObLobQueryIter, "unused", result);
          result = nullptr;
        }
      }
    } else if (OB_FAIL(query_outrow(param, result))) {
      LOG_WARN("query outrow fail", K(ret), K(param));
    }
  }
  return ret;
}

int ObLobManager::query(ObString& data, ObLobQueryIter *&result)
{
  INIT_SUCC(ret);
  ObLobInRowQueryIter* iter = OB_NEW(ObLobInRowQueryIter, ObMemAttr(MTL_ID(), "LobQueryIter"));
  if (OB_ISNULL(iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alloc lob meta scan iterator fail", K(ret));
  } else if (OB_FAIL(iter->open(data, 0, data.length(), CS_TYPE_BINARY, false))) {
    LOG_WARN("do lob meta scan failed.", K(ret), K(data));
  } else {
    result = iter;
  }
  return ret;
}

int ObLobManager::load_all(ObLobAccessParam &param, ObLobPartialData &partial_data)
{
  INIT_SUCC(ret);
  char *output_buf = nullptr;
  uint64_t output_len = param.byte_size_;
  ObString output_data;
  if (OB_ISNULL(output_buf = static_cast<char*>(param.allocator_->alloc(output_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc fail", K(ret), K(param));
  } else if (OB_FALSE_IT(output_data.assign_buffer(output_buf, output_len))) {
  } else if (OB_FAIL(query(param, output_data))) {
    LOG_WARN("do remote query fail", K(ret), K(param), K(output_len));
  } else if (OB_FAIL(partial_data.data_.push_back(ObLobChunkData(output_data)))) {
    LOG_WARN("push_back lob chunk data fail", KR(ret));
  } else {
    ObLobSeqId seq_id_generator(param.allocator_);
    ObString seq_id;
    uint64_t offset = 0;
    int64_t chunk_count = (param.byte_size_ + partial_data.chunk_size_ - 1)/partial_data.chunk_size_;
    for (int64_t i = 0; OB_SUCC(ret) && i < chunk_count; ++i) {
      ObLobChunkIndex chunk_index;
      chunk_index.offset_ = offset;
      chunk_index.pos_ = offset;
      chunk_index.byte_len_ = std::min(output_len, offset + partial_data.chunk_size_) - offset;
      chunk_index.data_idx_ = 0;
      if (OB_FAIL(seq_id_generator.get_next_seq_id(seq_id))) {
        LOG_WARN("failed to next seq id", K(ret), K(chunk_index));
      } else if (OB_FAIL(ob_write_string(*param.allocator_, seq_id, chunk_index.seq_id_))) {
        LOG_WARN("ob_write_stringt seq id fail", K(ret), K(chunk_count), K(output_len), K(partial_data.chunk_size_), K(chunk_index), K(seq_id));
      } else if (OB_FAIL(partial_data.push_chunk_index(chunk_index))) {
        LOG_WARN("push_back lob chunk index fail", KR(ret), K(chunk_count), K(output_len), K(partial_data.chunk_size_), K(chunk_index));
      } else {
        offset += partial_data.chunk_size_;
      }
    }
  }
  return ret;
}

int ObLobManager::query(
    ObIAllocator *allocator,
    ObLobLocatorV2 &locator,
    int64_t query_timeout_ts,
    bool is_load_all,
    ObLobPartialData *partial_data,
    ObLobCursor *&cursor)
{
  INIT_SUCC(ret);
  ObLobAccessParam *param = nullptr;
  bool is_remote_lob = false;
  bool is_partial_data_alloc = false;
  common::ObAddr dst_addr;
  if (! locator.has_lob_header() || ! locator.is_persist_lob() || locator.is_inrow()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid locator", KR(ret), K(locator));
  } else if (OB_ISNULL(cursor = OB_NEWx(ObLobCursor, allocator))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alloc fail", K(ret), "size", sizeof(ObLobCursor));
  } else if (OB_ISNULL(param = OB_NEWx(ObLobAccessParam, allocator))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("alloc fail", K(ret), "size", sizeof(ObLobAccessParam));
  } else if (OB_FAIL(build_lob_param(*param, *allocator, CS_TYPE_BINARY,
                      0, UINT64_MAX, query_timeout_ts, locator))) {
    LOG_WARN("build_lob_param fail", K(ret));
  } else if (! param->lob_common_->is_init_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob common not init", K(ret), KPC(param->lob_common_), KPC(param));
  } else if (OB_ISNULL(param->lob_data_ = reinterpret_cast<ObLobData*>(param->lob_common_->buffer_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob data is null", K(ret), KPC(param->lob_common_), KPC(param));
  } else if (OB_FAIL(ObLobLocationUtil::is_remote(*param, is_remote_lob, dst_addr))) {
    LOG_WARN("check is remote fail", K(ret), K(param));
  } else if (OB_ISNULL(partial_data)) {
    is_partial_data_alloc = true;
    if (OB_ISNULL(partial_data = OB_NEWx(ObLobPartialData, allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc lob param fail", K(ret), "size", sizeof(ObLobPartialData));
    } else if (OB_FAIL(partial_data->init())) {
      LOG_WARN("map create fail", K(ret));
    } else if (OB_FAIL(locator.get_chunk_size(partial_data->chunk_size_))) {
      LOG_WARN("get_chunk_size fail", K(ret), K(locator));
    } else {
      partial_data->data_length_ = param->byte_size_;
      partial_data->locator_.assign_ptr(locator.ptr_, locator.size_);
      // new alloc partial_data do load data if need
      if ((is_load_all || is_remote_lob) && OB_FAIL(load_all(*param, *partial_data))) {
        LOG_WARN("load_all fail", K(ret));
      }
    }
  }
  if (is_remote_lob) {
    LOG_INFO("remote_lob", KPC(param->lob_common_), KPC(param->lob_data_), K(dst_addr));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(cursor->init(allocator, param, partial_data, lob_ctx_.lob_meta_mngr_))) {
    LOG_WARN("cursor init fail", K(ret));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(cursor)) {
      cursor->~ObLobCursor();
      cursor = nullptr;
    }
    if (OB_NOT_NULL(partial_data) && is_partial_data_alloc) {
      partial_data->~ObLobPartialData();
      partial_data = nullptr;
    }
  }
  return ret;
}

int ObLobManager::equal(ObLobLocatorV2& lob_left,
                        ObLobLocatorV2& lob_right,
                        ObLobCompareParams& cmp_params,
                        bool& result)
{
  INIT_SUCC(ret);
  int64_t old_len = 0;
  int64_t new_len = 0;
  int64_t cmp_res = 0;
  if (OB_FAIL(lob_left.get_lob_data_byte_len(old_len))) {
    LOG_WARN("fail to get old byte len", K(ret), K(lob_left));
  } else if (OB_FAIL(lob_right.get_lob_data_byte_len(new_len))) {
    LOG_WARN("fail to get new byte len", K(ret), K(lob_right));
  } else if (new_len != old_len) {
    result = false;
  } else if (lob_left.has_inrow_data() && lob_right.has_inrow_data()) {
    // do both inrow check
    ObString left_str;
    ObString right_str;
    if (OB_FAIL(lob_left.get_inrow_data(left_str))) {
      LOG_WARN("fail to get old inrow data", K(ret), K(lob_left));
    } else if (OB_FAIL(lob_right.get_inrow_data(right_str))) {
      LOG_WARN("fail to get new inrow data", K(ret), K(lob_left));
    } else {
      result = (0 == MEMCMP(left_str.ptr(), right_str.ptr(), left_str.length()));
    }
  } else if (OB_FAIL(compare(lob_left, lob_right, cmp_params, cmp_res))) {
    LOG_WARN("fail to compare lob", K(ret), K(lob_left), K(lob_right));
  } else {
    result = (0 == cmp_res);
  }
  return ret;
}

int ObLobManager::compare(ObLobLocatorV2& lob_left,
                          ObLobLocatorV2& lob_right,
                          ObLobCompareParams& cmp_params,
                          int64_t& result) {
  INIT_SUCC(ret);
  ObArenaAllocator tmp_allocator("LobCmp", OB_MALLOC_MIDDLE_BLOCK_SIZE, MTL_ID());
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get lob manager handle null.", K(ret));
  } else if(!lob_left.has_lob_header() || !lob_right.has_lob_header()) {
    ret = OB_ERR_ARG_INVALID;
    LOG_WARN("invalid lob. should have lob locator", K(ret));
  } else {
    // get lob access param
    ObLobAccessParam param_left;
    ObLobAccessParam param_right;
    if (OB_FAIL(build_lob_param(param_left, tmp_allocator, cmp_params.collation_left_,
                cmp_params.offset_left_, cmp_params.compare_len_, cmp_params.timeout_, lob_left))) {
      LOG_WARN("fail to build read param left", K(ret), K(lob_left), K(cmp_params));
    } else if(OB_FAIL(build_lob_param(param_right, tmp_allocator, cmp_params.collation_right_,
                cmp_params.offset_right_, cmp_params.compare_len_, cmp_params.timeout_, lob_right))) {
      LOG_WARN("fail to build read param new", K(ret), K(lob_right));
    } else if(OB_FAIL(compare(param_left, param_right, result))) {
      LOG_WARN("fail to compare lob", K(ret), K(lob_left), K(lob_right), K(cmp_params));
    }
  }
  return ret;
}

int ObLobManager::compare(ObLobAccessParam& param_left,
                          ObLobAccessParam& param_right,
                          int64_t& result) {
  INIT_SUCC(ret);
  common::ObCollationType collation_left = param_left.coll_type_;
  common::ObCollationType collation_right = param_right.coll_type_;
  common::ObCollationType cmp_collation = collation_left;
  ObIAllocator* tmp_allocator = param_left.allocator_;
  ObLobQueryIter *iter_left = nullptr;
  ObLobQueryIter *iter_right = nullptr;
  if(OB_ISNULL(tmp_allocator)) {
    ret = OB_ERR_ARG_INVALID;
    LOG_WARN("invalid alloctor param", K(ret), K(param_left));
  } else if((collation_left == CS_TYPE_BINARY && collation_right != CS_TYPE_BINARY)
            || (collation_left != CS_TYPE_BINARY && collation_right == CS_TYPE_BINARY)) {
    ret = OB_ERR_ARG_INVALID;
    LOG_WARN("invalid collation param", K(ret), K(param_left), K(param_right));
  } else if (OB_FAIL(query(param_left, iter_left))) {
    LOG_WARN("query param left by iter failed.", K(ret), K(param_left));
  } else if (OB_FAIL(query(param_right, iter_right))) {
    LOG_WARN("query param right by iter failed.", K(ret), K(param_right));
  } else {
    uint64_t read_buff_size = OB_MALLOC_MIDDLE_BLOCK_SIZE; // 64KB
    char *read_buff = nullptr;
    char *charset_convert_buff_ptr = nullptr;
    bool need_convert_charset = (collation_left != CS_TYPE_BINARY);
    uint64_t charset_convert_buff_size = need_convert_charset ?
                                         read_buff_size * ObCharset::CharConvertFactorNum : 0;

    if (OB_ISNULL((read_buff = static_cast<char*>(tmp_allocator->alloc(read_buff_size * 2))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc read buffer failed.", K(ret), K(read_buff_size));
    } else if (need_convert_charset &&
               OB_ISNULL((charset_convert_buff_ptr = static_cast<char*>(tmp_allocator->alloc(charset_convert_buff_size))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc charset convert buffer failed.", K(ret), K(charset_convert_buff_size));
    } else {
      ObDataBuffer charset_convert_buff(charset_convert_buff_ptr, charset_convert_buff_size);
      ObString read_buffer_left;
      ObString read_buffer_right;
      read_buffer_left.assign_buffer(read_buff, read_buff_size);
      read_buffer_right.assign_buffer(read_buff + read_buff_size, read_buff_size);

      // compare right after charset convert
      ObString convert_buffer_right;
      convert_buffer_right.assign_ptr(nullptr, 0);

      while (OB_SUCC(ret) && result == 0) {
        if (read_buffer_left.length() == 0) {
          // reset buffer and read next block
          read_buffer_left.assign_buffer(read_buff, read_buff_size);
          if (OB_FAIL(iter_left->get_next_row(read_buffer_left))) {
            if (ret != OB_ITER_END) {
              LOG_WARN("failed to get next buffer for left lob.", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
          }
        }

        if (OB_SUCC(ret) && convert_buffer_right.length() == 0) {
          read_buffer_right.assign_buffer(read_buff + read_buff_size, read_buff_size);
          charset_convert_buff.set_data(charset_convert_buff_ptr, charset_convert_buff_size);
          convert_buffer_right.assign_ptr(nullptr, 0);

          if (OB_FAIL(iter_right->get_next_row(read_buffer_right))) {
            if (ret != OB_ITER_END) {
              LOG_WARN("failed to get next buffer for right lob", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
          } else if (need_convert_charset) {
            // convert right lob to left charset if necessary
            if(OB_FAIL(ObExprUtil::convert_string_collation(
                                  read_buffer_right, collation_right,
                                  convert_buffer_right, cmp_collation,
                                  charset_convert_buff))) {
                LOG_WARN("fail to convert string collation", K(ret),
                          K(read_buffer_right), K(collation_right),
                          K(convert_buffer_right), K(cmp_collation));
            }
          } else {
            convert_buffer_right.assign_ptr(read_buffer_right.ptr(), read_buffer_right.length());
          }
        }
        if (OB_SUCC(ret)) {
          if (read_buffer_left.length() == 0 && convert_buffer_right.length() == 0) {
            result = 0;
            ret = OB_ITER_END;
          } else if (read_buffer_left.length() == 0 && convert_buffer_right.length() > 0) {
            result = -1;
          } else if (read_buffer_left.length() > 0 && convert_buffer_right.length() == 0) {
            result = 1;
          } else {
            uint64_t cmp_len = read_buffer_left.length() > convert_buffer_right.length() ?
                                    convert_buffer_right.length() : read_buffer_left.length();
            ObString substr_lob_left;
            ObString substr_lob_right;
            substr_lob_left.assign_ptr(read_buffer_left.ptr(), cmp_len);
            substr_lob_right.assign_ptr(convert_buffer_right.ptr(), cmp_len);
            result = common::ObCharset::strcmp(cmp_collation, substr_lob_left, substr_lob_right);
            if (result > 0) {
              result = 1;
            } else if (result < 0) {
              result = -1;
            }

            read_buffer_left.assign_ptr(read_buffer_left.ptr() + cmp_len, read_buffer_left.length() - cmp_len);
            convert_buffer_right.assign_ptr(convert_buffer_right.ptr() + cmp_len, convert_buffer_right.length() - cmp_len);
          }
        }
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      }
    }
    if (OB_NOT_NULL(read_buff)) {
      tmp_allocator->free(read_buff);
    }
    if (OB_NOT_NULL(charset_convert_buff_ptr)) {
      tmp_allocator->free(charset_convert_buff_ptr);
    }
  }
  if (OB_NOT_NULL(iter_left)) {
    iter_left->reset();
    OB_DELETE(ObLobQueryIter, "unused", iter_left);
  }
  if (OB_NOT_NULL(iter_right)) {
    iter_right->reset();
    OB_DELETE(ObLobQueryIter, "unused", iter_right);
  }
  return ret;
}

void ObLobManager::transform_lob_id(uint64_t src, uint64_t &dst)
{
  dst = htonll(src << 1);
  char *bytes = reinterpret_cast<char*>(&dst);
  bytes[7] |= 0x01;
}

int ObLobManager::check_need_out_row(
    ObLobAccessParam& param,
    int64_t add_len,
    ObString &data,
    bool need_combine_data,
    bool alloc_inside,
    bool &need_out_row)
{
  int ret = OB_SUCCESS;
  need_out_row = (param.byte_size_ + add_len) > param.get_inrow_threshold();
  if (param.lob_locator_ != nullptr) {
    // TODO @lhd remove after tmp lob support outrow
    if (!param.lob_locator_->is_persist_lob()) {
      need_out_row = false;
    }
  }
  // in_row : 0 | need_out_row : 0  --> invalid
  // in_row : 0 | need_out_row : 1  --> do nothing, keep out_row
  // in_row : 1 | need_out_row : 0  --> do nothing, keep in_row
  // in_row : 1 | need_out_row : 1  --> in_row to out_row
  if (!param.lob_common_->in_row_ && !need_out_row) {
    if (!param.lob_common_->is_init_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lob data", K(ret), KPC(param.lob_common_), K(data));
    } else if (param.byte_size_ > 0) {
      LOG_DEBUG("update keey outrow ", K(param.byte_size_), K(add_len), KPC(param.lob_common_),KPC(param.lob_data_));
      need_out_row = true;
    } else {
      // currently only insert support outrow -> inrow
      LOG_DEBUG("insert outrow to inrow", K(param.byte_size_), K(add_len), KPC(param.lob_common_),KPC(param.lob_data_));
      ObLobCommon *lob_common = nullptr;
      if (OB_ISNULL(lob_common = OB_NEWx(ObLobCommon, param.allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc buf failed.", K(ret), "size", sizeof(ObLobCommon));
      } else {
        lob_common->in_row_ = 1;
        param.lob_common_ = lob_common;
        param.lob_data_ = nullptr;
        param.lob_locator_ = nullptr;
        param.handle_size_ = sizeof(ObLobCommon);
      }
    }
  } else if (param.lob_common_->in_row_ && need_out_row) {
    // combine lob_data->buffer and data
    if (need_combine_data) {
      if (param.byte_size_ > 0) {
        uint64_t total_size = param.byte_size_ + data.length();
        char *buf = static_cast<char*>(param.allocator_->alloc(total_size));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc buf failed.", K(ret), K(total_size));
        } else {
          MEMCPY(buf, param.lob_common_->get_inrow_data_ptr(), param.byte_size_);
          MEMCPY(buf + param.byte_size_, data.ptr(), data.length());
          data.assign_ptr(buf, total_size);
        }
      }
    } else {
      data.assign_ptr(param.lob_common_->get_inrow_data_ptr(), param.byte_size_);
    }

    // alloc full lob out row header
    if (OB_SUCC(ret)) {
      char *buf = static_cast<char*>(param.allocator_->alloc(ObLobConstants::LOB_OUTROW_FULL_SIZE));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc buf failed.", K(ret));
      } else if (OB_FAIL(is_store_char_len(param, param.get_schema_chunk_size(), add_len))) {
        LOG_WARN("cacl is_store_char_len failed.", K(ret), K(add_len), K(param));
      } else {
        MEMCPY(buf, param.lob_common_, sizeof(ObLobCommon));
        ObLobCommon *new_lob_common = reinterpret_cast<ObLobCommon*>(buf);
        if (new_lob_common->is_init_) {
          MEMCPY(new_lob_common->buffer_, param.lob_common_->buffer_, sizeof(ObLobData));
        } else {
          // init lob data and alloc lob id(when not init)
          ObLobData *new_lob_data = new(new_lob_common->buffer_)ObLobData();
          if (OB_FAIL(lob_ctx_.lob_meta_mngr_->fetch_lob_id(param, new_lob_data->id_.lob_id_))) {
            LOG_WARN("get lob id failed.", K(ret), K(param));
          } else if (! param.lob_meta_tablet_id_.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("lob_meta_tablet_id is invalid", K(ret), K(param));
          } else {
            new_lob_data->id_.tablet_id_ = param.lob_meta_tablet_id_.id();
            transform_lob_id(new_lob_data->id_.lob_id_, new_lob_data->id_.lob_id_);
            new_lob_common->is_init_ = true;
          }
        }
        if (OB_SUCC(ret)) {
          if (alloc_inside) {
            param.allocator_->free(param.lob_common_);
          }
          param.lob_common_ = new_lob_common;
          param.lob_data_ = reinterpret_cast<ObLobData*>(param.lob_common_->buffer_);
          // refresh in_row flag
          param.lob_common_->in_row_ = 0;
          // init out row ctx
          ObLobDataOutRowCtx *ctx = new(param.lob_data_->buffer_)ObLobDataOutRowCtx();
          ctx->chunk_size_ = param.get_schema_chunk_size() / ObLobDataOutRowCtx::OUTROW_LOB_CHUNK_SIZE_UNIT;
          // init char len
          uint64_t *char_len = reinterpret_cast<uint64_t*>(ctx + 1);
          *char_len = (param.is_store_char_len_) ? 0 : UINT64_MAX;
          param.handle_size_ = ObLobConstants::LOB_OUTROW_FULL_SIZE;
        }
      }
    }
  } else if (! param.lob_common_->in_row_ && need_out_row) {
    // outrow -> outrow : keep outrow
    int64_t store_chunk_size = 0;
    bool has_char_len = param.lob_handle_has_char_len();
    if (add_len < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("add_len is negative", K(ret), K(param));
    } else if (add_len == 0) {
      // no data add, keep char_len state
      param.is_store_char_len_ = has_char_len;
    } else if (OB_FAIL(param.get_store_chunk_size(store_chunk_size))) {
      LOG_WARN("get_store_chunk_size fail", K(ret), K(param));
    } else if (OB_FAIL(is_store_char_len(param, store_chunk_size, add_len))) {
      LOG_WARN("cacl is_store_char_len failed.", K(ret), K(store_chunk_size), K(add_len), K(param));
    } else if (param.op_type_ != ObLobDataOutRowCtx::OpType::SQL) {
      if (! param.is_store_char_len_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected case", K(ret), K(param), K(has_char_len));
      }
    } else if (0 != param.offset_ || 0 != param.byte_size_) {
      if (! param.is_store_char_len_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected case", K(ret), K(param), K(has_char_len));
      }
    } else if (has_char_len && param.is_store_char_len_) {
      // keep char_len
    } else if (! has_char_len && ! param.is_store_char_len_) {
      // keep no char_len
    } else if (has_char_len && ! param.is_store_char_len_) {
      // old data has char , but new data no char_len
      // reset char_len to UINT64_MAX from 0
      int64_t *char_len = param.get_char_len_ptr();
      if (OB_ISNULL(char_len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("char_len ptr is null", K(ret), K(param), K(has_char_len));
      } else if (*char_len != 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("char_len should be zero", K(ret), K(param), K(has_char_len), K(*char_len));
      } else {
        *char_len = UINT64_MAX;
        LOG_DEBUG("has_char_len to no_char_len", K(param));
      }
    } else if (! has_char_len && param.is_store_char_len_) {
      if (param.handle_size_ < ObLobConstants::LOB_OUTROW_FULL_SIZE) {
        LOG_INFO("old old data", K(param));
        param.is_store_char_len_ = true;
      } else if (param.is_full_insert()) {
        // reset char_len to 0 from UINT64_MAX
        int64_t *char_len = param.get_char_len_ptr();
        if (OB_ISNULL(char_len)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("char_len ptr is null", K(ret), K(param), K(has_char_len));
        } else if (*char_len != UINT64_MAX) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("char_len should be zero", K(ret), K(param), K(has_char_len), K(*char_len));
        } else {
          *char_len = 0;
          LOG_DEBUG("no_char_len to has_char_len", K(param));
        }
      } else {
        // partial update aloways store char_len beacaure is only support in oracle mode
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unsupport situation", K(ret), K(param), K(has_char_len));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unkown situation", K(ret), K(param), K(has_char_len));
    }
  }
  return ret;
}

int ObLobManager::append(
    ObLobAccessParam& param,
    ObLobLocatorV2 &lob)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("LobTmp", OB_MALLOC_MIDDLE_BLOCK_SIZE, MTL_ID());
  param.set_tmp_allocator(&tmp_allocator);

  bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else if (OB_FAIL(param.prepare())) {
    LOG_WARN("param prepare fail", K(ret), K(param));
  } else if (!lob.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob locator", K(ret));
  } else if (!lob.has_lob_header()) { // 4.0 text tc compatiable
    ObString data;
    data.assign_ptr(lob.ptr_, lob.size_);
    if (OB_FAIL(append(param, data))) {
      LOG_WARN("[STORAGE_LOB]lob append failed.", K(ret), K(param), K(data));
    }
  } else if (lob.is_delta_temp_lob()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob locator", K(ret));
  } else if (lob.has_inrow_data()) {
    ObString data;
    if (OB_FAIL(lob.get_inrow_data(data))) {
      LOG_WARN("get inrow data int insert lob col failed", K(lob), K(data));
    } else if (OB_FAIL(append(param, data))) {
      LOG_WARN("[STORAGE_LOB]lob append failed.", K(ret), K(param), K(data));
    }
  } else {
    bool alloc_inside = false;
    bool need_out_row = false;
    if (OB_FAIL(prepare_lob_common(param, alloc_inside))) {
      LOG_WARN("fail to prepare lob common", K(ret), K(param));
    }
    ObLobCommon *lob_common = param.lob_common_;
    ObLobData *lob_data = param.lob_data_;
    bool is_remote_lob = false;
    common::ObAddr dst_addr;
    int64_t append_lob_len = 0;
    ObString ori_inrow_data;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(param.check_handle_size())) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (OB_FAIL(ObLobLocationUtil::is_remote(param, is_remote_lob, dst_addr))) {
      LOG_WARN("check is remote failed.", K(ret), K(param));
    } else if (is_remote_lob) {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("Unsupport remote append", K(ret), K(param));
    } else if (OB_FAIL(lob.get_lob_data_byte_len(append_lob_len))) {
      LOG_WARN("fail to get append lob byte len", K(ret), K(lob));
    } else if (OB_FAIL(check_need_out_row(param, append_lob_len, ori_inrow_data, false, alloc_inside, need_out_row))) {
      LOG_WARN("process out row check failed.", K(ret), K(param), KPC(lob_common), KPC(lob_data), K(lob));
    } else if (OB_ISNULL(lob_common = param.lob_common_)) { // check_need_out_row may change lob_common
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob_commob is nul", K(ret), K(param), KPC(lob_common), KPC(lob_data), K(lob));
    } else if (!need_out_row) {
      // do inrow append
      int32_t cur_handle_size = lob_common->get_handle_size(param.byte_size_);
      int32_t ptr_offset = 0;
      if (OB_NOT_NULL(param.lob_locator_)) {
        ptr_offset = reinterpret_cast<char*>(param.lob_common_) - reinterpret_cast<char*>(param.lob_locator_->ptr_);
        cur_handle_size += ptr_offset;
      }
      uint64_t total_size = cur_handle_size + append_lob_len;
      char *buf = static_cast<char*>(param.allocator_->alloc(total_size));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc buf failed.", K(ret), K(total_size));
      } else {
        if (OB_NOT_NULL(param.lob_locator_)) {
          MEMCPY(buf, param.lob_locator_->ptr_, ptr_offset);
        }
        ObLobCommon *new_lob_common = reinterpret_cast<ObLobCommon*>(buf + ptr_offset);
        MEMCPY(new_lob_common, lob_common, cur_handle_size - ptr_offset);
        ObString data;
        data.assign_buffer(buf + cur_handle_size, append_lob_len);
        SMART_VAR(ObLobAccessParam, read_param) {
          read_param.tenant_id_ = param.src_tenant_id_;
          if (OB_FAIL(build_lob_param(read_param, *param.get_tmp_allocator(), param.coll_type_,
                      0, UINT64_MAX, param.timeout_, lob))) {
            LOG_WARN("fail to build read param", K(ret), K(lob));
          } else if (OB_FAIL(query(read_param, data))) {
            LOG_WARN("fail to read src lob", K(ret), K(read_param));
          }
        }
        if (OB_SUCC(ret)) {
          // refresh lob info
          param.byte_size_ += data.length();
          if (new_lob_common->is_init_) {
            ObLobData *new_lob_data = reinterpret_cast<ObLobData*>(new_lob_common->buffer_);
            new_lob_data->byte_size_ += data.length();
          }
          if (alloc_inside) {
            param.allocator_->free(param.lob_common_);
          }
          param.lob_common_ = new_lob_common;
          param.handle_size_ = total_size;
          if (OB_NOT_NULL(param.lob_locator_)) {
            param.lob_locator_->ptr_ = buf;
            param.lob_locator_->size_ = total_size;
            if (OB_FAIL(fill_lob_locator_extern(param))) {
              LOG_WARN("fail to fill lob locator extern", K(ret), KPC(param.lob_locator_));
            }
          }
        }
      }
    } else if (OB_FAIL(append_outrow(param, lob, append_lob_len, ori_inrow_data))) {
      LOG_WARN("failed to process write out row", K(ret), K(param), K(lob), K(append_lob_len), K(ori_inrow_data));
    } else if (OB_FAIL(check_write_length(param, append_lob_len))) {
      LOG_WARN("check_write_length fail", K(ret), K(param), K(lob), K(append_lob_len));
    }
  }
  param.set_tmp_allocator(nullptr);
  return ret;
}

int ObLobManager::append(ObLobAccessParam& param, ObLobLocatorV2& lob, ObLobMetaWriteIter &iter)
{
  int ret = OB_SUCCESS;
  bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else if (OB_FAIL(param.set_lob_locator(param.lob_locator_))) {
    LOG_WARN("failed to set lob locator for param", K(ret), K(param));
  } else if (!lob.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob locator", K(ret));
  } else if (lob.is_delta_temp_lob()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob locator", K(ret));
  } else {
    bool alloc_inside = false;
    bool need_out_row = false;
    if (OB_FAIL(prepare_lob_common(param, alloc_inside))) {
      LOG_WARN("fail to prepare lob common", K(ret), K(param));
    }
    ObLobCommon *lob_common = param.lob_common_;
    ObLobData *lob_data = param.lob_data_;
    bool is_remote_lob = false;
    common::ObAddr dst_addr;
    int64_t append_lob_len = 0;
    ObString ori_inrow_data;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(param.check_handle_size())) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (OB_FAIL(ObLobLocationUtil::is_remote(param, is_remote_lob, dst_addr))) {
      LOG_WARN("check is remote failed.", K(ret), K(param));
    } else if (is_remote_lob) {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("Unsupport remote append", K(ret), K(param));
    } else if (OB_FAIL(lob.get_lob_data_byte_len(append_lob_len))) {
      LOG_WARN("fail to get append lob byte len", K(ret), K(lob));
    } else if (OB_FAIL(check_need_out_row(param, append_lob_len, ori_inrow_data, false, alloc_inside, need_out_row))) {
      LOG_WARN("process out row check failed.", K(ret), K(param), KPC(lob_common), KPC(lob_data), K(lob));
    } else if (!need_out_row) {
      // do inrow append
      int32_t cur_handle_size = lob_common->get_handle_size(param.byte_size_);
      int32_t ptr_offset = 0;
      if (OB_NOT_NULL(param.lob_locator_)) {
        ptr_offset = reinterpret_cast<char*>(param.lob_common_) - reinterpret_cast<char*>(param.lob_locator_->ptr_);
        cur_handle_size += ptr_offset;
      }
      uint64_t total_size = cur_handle_size + append_lob_len;
      char *buf = static_cast<char*>(param.allocator_->alloc(total_size));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc buf failed.", K(ret), K(total_size));
      } else {
        if (OB_NOT_NULL(param.lob_locator_)) {
          MEMCPY(buf, param.lob_locator_->ptr_, ptr_offset);
        }
        ObLobCommon *new_lob_common = reinterpret_cast<ObLobCommon*>(buf + ptr_offset);
        MEMCPY(new_lob_common, lob_common, cur_handle_size - ptr_offset);
        ObString data;
        data.assign_buffer(buf + cur_handle_size, append_lob_len);
        SMART_VAR(ObLobAccessParam, read_param) {
          read_param.tenant_id_ = param.src_tenant_id_;
          if (OB_FAIL(build_lob_param(read_param, *param.get_tmp_allocator(), param.coll_type_,
                      0, UINT64_MAX, param.timeout_, lob))) {
            LOG_WARN("fail to build read param", K(ret), K(lob));
          } else if (OB_FAIL(query(read_param, data))) {
            LOG_WARN("fail to read src lob", K(ret), K(read_param));
          }
        }
        if (OB_SUCC(ret)) {
          // refresh lob info
          param.byte_size_ += data.length();
          if (new_lob_common->is_init_) {
            ObLobData *new_lob_data = reinterpret_cast<ObLobData*>(new_lob_common->buffer_);
            new_lob_data->byte_size_ += data.length();
          }
          param.lob_common_ = new_lob_common;
          param.handle_size_ = total_size;
          if (OB_NOT_NULL(param.lob_locator_)) {
            param.lob_locator_->ptr_ = buf;
            param.lob_locator_->size_ = total_size;
            if (OB_FAIL(fill_lob_locator_extern(param))) {
              LOG_WARN("fail to fill lob locator extern", K(ret), KPC(param.lob_locator_));
            }
          }
        }
        iter.set_end();
      }
    } else if (!lob.has_lob_header()) {
      ObString data;
      data.assign_ptr(lob.ptr_, lob.size_);
      ObLobCtx lob_ctx = lob_ctx_;
      if (OB_FAIL(lob_ctx.lob_meta_mngr_->append(param, iter))) {
        LOG_WARN("Failed to open lob meta write iter.", K(ret), K(param));
      }
    } else {
      // prepare out row ctx
      ObLobCtx lob_ctx = lob_ctx_;
      int64_t store_chunk_size = 0;
      if (OB_FAIL(param.init_out_row_ctx(append_lob_len))) {
        LOG_WARN("init lob data out row ctx failed", K(ret));
      } else if (OB_FAIL(param.get_store_chunk_size(store_chunk_size))) {
        LOG_WARN("get_store_chunk_size fail", K(ret), K(param));
      }
      // prepare read buffer
      ObString read_buffer;
      uint64_t read_buff_size = OB_MIN(store_chunk_size, append_lob_len);
      char *read_buff = nullptr;
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(read_buff = static_cast<char*>(param.get_tmp_allocator()->alloc(read_buff_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc read buffer failed.", K(ret), K(read_buff_size));
      } else {
        read_buffer.assign_buffer(read_buff, read_buff_size);
      }

      // prepare read full lob
      if (OB_SUCC(ret)) {
        ObLobLocatorV2* copy_locator = nullptr;
        ObLobAccessParam *read_param = reinterpret_cast<ObLobAccessParam*>(param.get_tmp_allocator()->alloc(sizeof(ObLobAccessParam)));
        if (OB_ISNULL(read_param)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc read param failed.", K(ret), K(sizeof(ObLobAccessParam)));
        } else if (OB_ISNULL(copy_locator = OB_NEWx(ObLobLocatorV2, param.get_tmp_allocator()))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc ObLobLocatorV2 failed.", K(ret), K(sizeof(ObLobLocatorV2)));
        } else {
          read_param = new(read_param)ObLobAccessParam();
          read_param->tenant_id_ = param.src_tenant_id_;
          *copy_locator = lob;
          if (OB_FAIL(build_lob_param(*read_param, *param.get_tmp_allocator(), param.coll_type_,
                      0, UINT64_MAX, param.timeout_, *copy_locator))) {
            LOG_WARN("fail to build read param", K(ret), K(lob), KPC(copy_locator));
          } else {
            ObLobQueryIter *qiter = nullptr;
            if (OB_FAIL(query(*read_param, qiter))) {
              LOG_WARN("do query src by iter failed.", K(ret), K(read_param));
            } else if (OB_FAIL(iter.open(param, qiter, read_param, read_buffer))) {
              LOG_WARN("open lob meta write iter failed.", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLobManager::prepare_lob_common(ObLobAccessParam& param, bool &alloc_inside)
{
  int ret = OB_SUCCESS;
  alloc_inside = false;
  if (OB_ISNULL(param.lob_common_)) {
    // alloc new lob_data
    void *tbuf = param.allocator_->alloc(ObLobConstants::LOB_OUTROW_FULL_SIZE);
    if (OB_ISNULL(tbuf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for LobData", K(ret));
    } else {
      // init full out row
      param.lob_common_ = new(tbuf)ObLobCommon();
      param.lob_data_ = new(param.lob_common_->buffer_)ObLobData();
      ObLobDataOutRowCtx *outrow_ctx = new(param.lob_data_->buffer_)ObLobDataOutRowCtx();
      outrow_ctx->chunk_size_ = param.get_schema_chunk_size() / ObLobDataOutRowCtx::OUTROW_LOB_CHUNK_SIZE_UNIT;
      // init char len
      uint64_t *char_len = reinterpret_cast<uint64_t*>(outrow_ctx + 1);
      *char_len = 0;
      param.handle_size_ = ObLobConstants::LOB_OUTROW_FULL_SIZE;
      alloc_inside = true;
    }
  } else if (param.lob_common_->is_init_) {
    param.lob_data_ = reinterpret_cast<ObLobData*>(param.lob_common_->buffer_);

    if (0 == param.lob_data_->byte_size_) {
      // that is insert when lob_data_->byte_size_ is zero.
      // so should update chunk size
      ObLobDataOutRowCtx *outrow_ctx = reinterpret_cast<ObLobDataOutRowCtx*>(param.lob_data_->buffer_);
      outrow_ctx->chunk_size_ = param.get_schema_chunk_size() / ObLobDataOutRowCtx::OUTROW_LOB_CHUNK_SIZE_UNIT;
    }
  }
  return ret;
}

int ObLobManager::append(
    ObLobAccessParam& param,
    ObString& data)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("LobTmp", OB_MALLOC_MIDDLE_BLOCK_SIZE, MTL_ID());
  param.set_tmp_allocator(&tmp_allocator);
  bool save_is_reverse = param.scan_backward_;
  uint64_t save_param_len = param.len_;
  bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else if (OB_FAIL(param.prepare())) {
    LOG_WARN("param prepare fail", K(ret), K(param));
  } else {
    bool alloc_inside = false;
    bool need_out_row = false;
    if (OB_FAIL(prepare_lob_common(param, alloc_inside))) {
      LOG_WARN("fail to prepare lob common", K(ret), K(param));
    }
    ObLobCommon *lob_common = param.lob_common_;
    ObLobData *lob_data = param.lob_data_;
    bool is_remote_lob = false;
    bool ori_is_inrow = (lob_common == nullptr) ? false : (lob_common->in_row_ == 1);
    common::ObAddr dst_addr;
    int64_t store_chunk_size = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(param.check_handle_size())) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (OB_FAIL(ObLobLocationUtil::is_remote(param, is_remote_lob, dst_addr))) {
      LOG_WARN("check is remote failed.", K(ret), K(param));
    } else if (is_remote_lob) {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("Unsupport remote append", K(ret), K(param));
    } else if (OB_FAIL(check_need_out_row(param, data.length(), data, true, alloc_inside, need_out_row))) {
      LOG_WARN("process out row check failed.", K(ret), K(param), KPC(lob_common), KPC(lob_data), K(data));
    } else if (OB_ISNULL(lob_common = param.lob_common_)) { // check_need_out_row may change lob_common
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob_common is nul", K(ret), K(param), KPC(lob_common), KPC(lob_data), K(data));
    } else if (!need_out_row) {
      // do inrow append
      int32_t cur_handle_size = lob_common->get_handle_size(param.byte_size_);
      int32_t ptr_offset = 0;
      if (OB_NOT_NULL(param.lob_locator_)) {
        ptr_offset = reinterpret_cast<char*>(param.lob_common_) - reinterpret_cast<char*>(param.lob_locator_->ptr_);
        cur_handle_size += ptr_offset;
      }
      uint64_t total_size = cur_handle_size + data.length();
      char *buf = static_cast<char*>(param.allocator_->alloc(total_size));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc buf failed.", K(ret), K(total_size));
      } else {
        if (OB_NOT_NULL(param.lob_locator_)) {
          MEMCPY(buf, param.lob_locator_->ptr_, ptr_offset);
        }
        ObLobCommon *new_lob_common = reinterpret_cast<ObLobCommon*>(buf + ptr_offset);
        MEMCPY(new_lob_common, lob_common, cur_handle_size - ptr_offset);
        MEMCPY(buf + cur_handle_size, data.ptr(), data.length());
        // refresh lob info
        param.byte_size_ += data.length();
        if (new_lob_common->is_init_) {
          ObLobData *new_lob_data = reinterpret_cast<ObLobData*>(new_lob_common->buffer_);
          new_lob_data->byte_size_ += data.length();
        }
        if (alloc_inside) {
          param.allocator_->free(param.lob_common_);
        }
        param.lob_common_ = new_lob_common;
        param.handle_size_ = total_size;
        if (OB_NOT_NULL(param.lob_locator_)) {
          param.lob_locator_->ptr_ = buf;
          param.lob_locator_->size_ = total_size;
          if (OB_FAIL(fill_lob_locator_extern(param))) {
            LOG_WARN("fail to fill lob locator extern", K(ret), KPC(param.lob_locator_));
          }
        }
      }
    } else if (OB_FAIL(append_outrow(param, ori_is_inrow, data))) {
      LOG_WARN("append_outrow fail", K(ret), K(param), K(data));
    } else if (OB_FAIL(check_write_length(param, data.length()))) {
      LOG_WARN("check_write_length fail", K(ret), K(param), K(data.length()));
    }
  }
  if (OB_SUCC(ret)) {
    param.len_ = save_param_len;
    param.scan_backward_ = save_is_reverse;
  }
  param.set_tmp_allocator(nullptr);
  return ret;
}

int ObLobManager::prepare_for_write(
    ObLobAccessParam& param,
    ObString &old_data,
    bool &need_out_row)
{
  int ret = OB_SUCCESS;
  int64_t max_bytes_in_char = 4;
  uint64_t modified_end = param.offset_ + param.len_;
  if (param.coll_type_ != CS_TYPE_BINARY) {
    modified_end *= max_bytes_in_char;
  }
  uint64_t total_size = param.byte_size_ > modified_end ? param.byte_size_ : modified_end;
  need_out_row = (total_size > param.get_inrow_threshold());
  if (param.lob_common_->in_row_) {
    old_data.assign_ptr(param.lob_common_->get_inrow_data_ptr(), param.byte_size_);
  }
  if (param.lob_locator_ != nullptr) {
    // @lhd remove after tmp lob support outrow
    if (!param.lob_locator_->is_persist_lob()) {
      need_out_row = false;
    }
  }
  // in_row : 0 | need_out_row : 0  --> invalid
  // in_row : 0 | need_out_row : 1  --> do nothing, keep out_row
  // in_row : 1 | need_out_row : 0  --> do nothing, keep in_row
  // in_row : 1 | need_out_row : 1  --> in_row to out_row
  if (!param.lob_common_->in_row_ && !need_out_row) {
    if (!param.lob_common_->is_init_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lob data", K(ret), KPC(param.lob_common_));
    } else {
      need_out_row = true;
    }
  } else if (param.lob_common_->in_row_ && need_out_row) {
    // alloc full lob out row header
    if (OB_SUCC(ret)) {
      char* buf = static_cast<char*>(param.allocator_->alloc(ObLobConstants::LOB_OUTROW_FULL_SIZE));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc buf failed.", K(ret), K(total_size));
      } else {
        MEMCPY(buf, param.lob_common_, sizeof(ObLobCommon));
        ObLobCommon *new_lob_common = reinterpret_cast<ObLobCommon*>(buf);
        new_lob_common->in_row_ = 0;
        if (new_lob_common->is_init_) {
          MEMCPY(new_lob_common->buffer_, param.lob_common_->buffer_, sizeof(ObLobData));
        } else {
          // init lob data and alloc lob id(when not init)
          ObLobData *new_lob_data = new(new_lob_common->buffer_)ObLobData();
          if (OB_FAIL(lob_ctx_.lob_meta_mngr_->fetch_lob_id(param, new_lob_data->id_.lob_id_))) {
            LOG_WARN("get lob id failed.", K(ret), K(param));
          } else if (! param.lob_meta_tablet_id_.is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("lob_meta_tablet_id is invalid", K(ret), K(param));
          } else {
            new_lob_data->id_.tablet_id_ = param.lob_meta_tablet_id_.id();
            transform_lob_id(new_lob_data->id_.lob_id_, new_lob_data->id_.lob_id_);
            new_lob_common->is_init_ = true;
          }
        }
        if (OB_SUCC(ret)) {
          param.lob_common_ = new_lob_common;
          param.lob_data_ = reinterpret_cast<ObLobData*>(param.lob_common_->buffer_);
          // init out row ctx
          ObLobDataOutRowCtx *ctx = new(param.lob_data_->buffer_)ObLobDataOutRowCtx();
          // init char len
          uint64_t *char_len = reinterpret_cast<uint64_t*>(ctx + 1);
          *char_len = 0;
          param.handle_size_ = ObLobConstants::LOB_OUTROW_FULL_SIZE;
        }
      }
    }
  }
  return ret;
}

int ObLobManager::process_delta(ObLobAccessParam& param, ObLobLocatorV2& lob_locator)
{
  int ret = OB_SUCCESS;
  if (lob_locator.is_delta_temp_lob()) {
    ObString data;
    ObLobCommon *lob_common = nullptr;
    if (OB_FAIL(lob_locator.get_disk_locator(lob_common))) {
      LOG_WARN("get disk locator failed.", K(ret), K(lob_locator));
    } else if (!lob_common->in_row_) {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("Unsupport out row delta tmp lob locator", K(ret), KPC(lob_common));
    } else {
      ObLobDiffHeader *diff_header = reinterpret_cast<ObLobDiffHeader*>(lob_common->buffer_);
      if (param.lob_common_ == nullptr) {
        ObLobCommon *persis_lob = diff_header->get_persist_lob();
        param.lob_locator_ = nullptr;
        param.lob_common_ = persis_lob;
        param.handle_size_ = diff_header->persist_loc_size_;
        param.byte_size_ = persis_lob->get_byte_size(param.handle_size_);
      }
      ObLobDiff *diffs = diff_header->get_diff_ptr();
      char *data_ptr = diff_header->get_inline_data_ptr();
      // process diffs
      for (int64_t i = 0 ; OB_SUCC(ret) && i < diff_header->diff_cnt_; ++i) {
        ObString tmp_data(diffs[i].byte_len_, data_ptr + diffs[i].offset_);
        param.offset_ = diffs[i].ori_offset_;
        switch (diffs[i].type_) {
          case ObLobDiff::DiffType::APPEND: {
            param.op_type_ = ObLobDataOutRowCtx::OpType::APPEND;
            param.len_ = diffs[i].ori_len_;
            ObLobLocatorV2 src_lob(tmp_data);
            if (OB_FAIL(append(param, src_lob))) {
              LOG_WARN("failed to do lob append", K(ret), K(param), K(src_lob));
            }
            if (ret == OB_SNAPSHOT_DISCARDED && src_lob.is_persist_lob()) {
              ret = OB_ERR_LOB_SPAN_TRANSACTION;
              LOG_WARN("fail to read src lob, make update inner sql do not retry", K(ret));
            }
            break;
          }
          case ObLobDiff::DiffType::WRITE: {
            param.op_type_ = ObLobDataOutRowCtx::OpType::WRITE;
            param.len_ = diffs[i].ori_len_;
            bool can_do_append = false;
            if (diffs[i].flags_.can_do_append_) {
              if (param.lob_handle_has_char_len()) {
                int64_t *len = param.get_char_len_ptr();
                if (*len == param.offset_) {
                  can_do_append = true;
                  param.offset_ = 0;
                }
              }
            }

            ObLobLocatorV2 src_lob(tmp_data);
            if (can_do_append) {
              if (OB_FAIL(append(param, src_lob))) {
                LOG_WARN("failed to do lob append", K(ret), K(param), K(src_lob));
              }
            } else {
              if (OB_FAIL(write(param, src_lob, diffs[i].dst_offset_))) {
                LOG_WARN("failed to do lob write", K(ret), K(param), K(src_lob));
              }
            }
            if (ret == OB_SNAPSHOT_DISCARDED && src_lob.is_persist_lob()) {
              ret = OB_ERR_LOB_SPAN_TRANSACTION;
              LOG_WARN("fail to read src lob, make update inner sql do not retry", K(ret));
            }
            break;
          }
          case ObLobDiff::DiffType::ERASE: {
            param.op_type_ = ObLobDataOutRowCtx::OpType::ERASE;
            param.len_ = diffs[i].ori_len_;
            if (OB_FAIL(erase(param))) {
              LOG_WARN("failed to do lob erase", K(ret), K(param));
            }
            break;
          }
          case ObLobDiff::DiffType::ERASE_FILL_ZERO: {
            param.op_type_ = ObLobDataOutRowCtx::OpType::WRITE;
            param.len_ = diffs[i].ori_len_;
            param.is_fill_zero_ = true;
            if (OB_FAIL(erase(param))) {
              LOG_WARN("failed to do lob erase", K(ret), K(param));
            }
            break;
          }
          case ObLobDiff::DiffType::WRITE_DIFF : {
            if (i != 0) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("first type must be write_diff", K(ret), K(i), K(diff_header), K(diffs[i]));
            } else if (OB_FAIL(process_diff(param, lob_locator, diff_header))) {
              LOG_WARN("process_diff fail", K(ret), K(param), K(i), K(*diff_header));
            } else {
              i = diff_header->diff_cnt_;
            }
            break;
          }
          default: {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid diff type", K(ret), K(i), K(diffs[i]));
          }
        }
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob locator type", K(ret), K(lob_locator));
  }
  return ret;
}

int ObLobManager::process_diff(ObLobAccessParam& param, ObLobLocatorV2& delta_locator, ObLobDiffHeader *diff_header)
{
  int ret = OB_SUCCESS;
  ObLobDiffUpdateHandler handler(param);
  if (OB_FAIL(handler.init(lob_ctx_.lob_meta_mngr_))) {
    LOG_WARN("init handler fail", K(ret), K(param));
  } else if (OB_FAIL(handler.execute(delta_locator, diff_header))) {
    LOG_WARN("execute fail", K(ret), K(param));
  }
  return ret;
}

int ObLobManager::fill_lob_locator_extern(ObLobAccessParam& param)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(param.lob_locator_)) {
    if (param.lob_locator_->has_extern()) {
      ObMemLobExternHeader *ext_header = nullptr;
      if (OB_FAIL(param.lob_locator_->get_extern_header(ext_header))) {
        LOG_WARN("get extern header failed", K(ret), KPC(param.lob_locator_));
      } else {
        ext_header->payload_size_ = param.byte_size_;
      }
    }
  }
  return ret;
}

int ObLobManager::getlength(ObLobAccessParam& param, uint64_t &len)
{
  int ret = OB_SUCCESS;
  bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else if (OB_FAIL(param.prepare())) {
    LOG_WARN("param prepare fail", K(ret), K(param));
  } else {
    ObLobCommon *lob_common = param.lob_common_;
    if (OB_ISNULL(lob_common)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("get lob data null.", K(ret));
    } else if (OB_FAIL(param.check_handle_size())) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (!is_char) { // return byte len
      len = lob_common->get_byte_size(param.handle_size_);
    } else if (param.lob_handle_has_char_len()) {
      len = *param.get_char_len_ptr();
    } else if (lob_common->in_row_ || // calc char len
               (param.lob_locator_ != nullptr && param.lob_locator_->has_inrow_data())) {
      ObString data;
      if (param.lob_locator_ != nullptr && param.lob_locator_->has_inrow_data()) {
        if (OB_FAIL(param.lob_locator_->get_inrow_data(data))) {
          LOG_WARN("fail to get inrow data", K(ret), KPC(param.lob_locator_));
        }
      } else {
        if (lob_common->is_init_) {
          param.lob_data_ = reinterpret_cast<ObLobData*>(lob_common->buffer_);
          data.assign_ptr(param.lob_data_->buffer_, param.lob_data_->byte_size_);
        } else {
          data.assign_ptr(lob_common->buffer_, param.byte_size_);
        }
      }
      if (OB_SUCC(ret)) {
        len = ObCharset::strlen_char(param.coll_type_, data.ptr(), data.length());
      }
    } else { // do meta scan
      ObLobQueryLengthHandler handler(param);
      if (OB_FAIL(handler.init(lob_ctx_.lob_meta_mngr_))) {
        LOG_WARN("init handler fail", K(ret), K(param));
      } else  if (OB_FAIL(handler.execute())) {
        LOG_WARN("handler execute fail", K(ret), K(param));
      } else {
        len = handler.result_;
      }
    }
  }
  return ret;
}

int ObLobManager::write_inrow_inner(ObLobAccessParam& param, ObString& data, ObString& old_data)
{
  int ret = OB_SUCCESS;
  ObLobCommon *lob_common = param.lob_common_;
  int64_t cur_handle_size = lob_common->get_handle_size(param.byte_size_) - param.byte_size_;
  int64_t ptr_offset = 0;
  if (OB_NOT_NULL(param.lob_locator_)) {
    ptr_offset = reinterpret_cast<char*>(param.lob_common_) - reinterpret_cast<char*>(param.lob_locator_->ptr_);
    cur_handle_size += ptr_offset;
  }
  int64_t lob_cur_mb_len = ObCharset::strlen_char(param.coll_type_, lob_common->get_inrow_data_ptr(), param.byte_size_);
  int64_t offset_byte_len = 0;
  int64_t amount_byte_len = 0;
  int64_t lob_replaced_byte_len = 0;
  int64_t res_len = 0;
  if (param.offset_ >= lob_cur_mb_len) {
    offset_byte_len = param.byte_size_ + (param.offset_ - lob_cur_mb_len);
    amount_byte_len = ObCharset::charpos(param.coll_type_, data.ptr(), data.length(), param.len_);
    res_len = offset_byte_len + amount_byte_len;
  } else {
    offset_byte_len = ObCharset::charpos(param.coll_type_,
                                          old_data.ptr(),
                                          old_data.length(),
                                          param.offset_);
    amount_byte_len = ObCharset::charpos(param.coll_type_, data.ptr(), data.length(), param.len_);
    lob_replaced_byte_len = ObCharset::charpos(param.coll_type_,
                                                old_data.ptr() + offset_byte_len,
                                                old_data.length() - offset_byte_len,
                                                (param.len_ + param.offset_ > lob_cur_mb_len) ? (lob_cur_mb_len - param.offset_) : param.len_);
    res_len = old_data.length() - lob_replaced_byte_len + amount_byte_len;
  }

  res_len += cur_handle_size;
  char *buf = static_cast<char*>(param.allocator_->alloc(res_len));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc buf failed.", K(ret), K(res_len));
  } else {
    ObString space = ObCharsetUtils::get_const_str(param.coll_type_, ' ');
    if (param.coll_type_ == CS_TYPE_BINARY) {
      MEMSET(buf, 0x00, res_len);
    } else {
      uint32_t space_len = space.length();
      if (space_len == 0 || res_len%space_len != 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid space_len or res-len", K(ret), K(res_len), K(space_len));
      } else if (space_len > 1) {
        for (int i = 0; i < res_len/space_len; i++) {
          MEMCPY(buf + i * space_len, space.ptr(), space_len);
        }
      } else {
        MEMSET(buf, *space.ptr(), res_len);
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      if (OB_NOT_NULL(param.lob_locator_)) {
        MEMCPY(buf, param.lob_locator_->ptr_, ptr_offset);
      }
      ObLobCommon *new_lob_common = reinterpret_cast<ObLobCommon*>(buf + ptr_offset);
      MEMCPY(new_lob_common, lob_common, cur_handle_size - ptr_offset);
      char* new_data_ptr = const_cast<char*>(new_lob_common->get_inrow_data_ptr());
      if (offset_byte_len >= old_data.length()) {
        MEMCPY(new_data_ptr, old_data.ptr(), old_data.length());
        MEMCPY(new_data_ptr + offset_byte_len, data.ptr(), amount_byte_len);
      } else {
        MEMCPY(new_data_ptr, old_data.ptr(), offset_byte_len);
        MEMCPY(new_data_ptr + offset_byte_len, data.ptr(), amount_byte_len);
        if (offset_byte_len + amount_byte_len < old_data.length()) {
          MEMCPY(new_data_ptr + offset_byte_len + amount_byte_len,
                  old_data.ptr() + offset_byte_len + lob_replaced_byte_len,
                  old_data.length() - offset_byte_len - lob_replaced_byte_len);
        }
      }

      // refresh lob info
      param.byte_size_ = res_len - cur_handle_size;
      if (new_lob_common->is_init_) {
        ObLobData *new_lob_data = reinterpret_cast<ObLobData*>(new_lob_common->buffer_);
        new_lob_data->byte_size_ = res_len - cur_handle_size;
      }
      param.lob_common_ = new_lob_common;
      param.handle_size_ = res_len;
      if (OB_NOT_NULL(param.lob_locator_)) {
        param.lob_locator_->ptr_ = buf;
        param.lob_locator_->size_ = res_len;
        if (OB_FAIL(fill_lob_locator_extern(param))) {
          LOG_WARN("fail to fill lob locator extern", K(ret), KPC(param.lob_locator_));
        }
      }
    }
  }
  return ret;
}

int ObLobManager::write_inrow(ObLobAccessParam& param, ObLobLocatorV2& lob, uint64_t offset, ObString& old_data)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObLobAccessParam, read_param) {
    if (OB_ISNULL(param.allocator_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("param tmp allocator is null", K(ret), K(param));
    } else if (OB_FAIL(build_lob_param(read_param, *param.allocator_, param.coll_type_,
                offset, param.len_, param.timeout_, lob))) {
      LOG_WARN("fail to build read param", K(ret), K(lob));
    } else {
      ObLobQueryIter *iter = nullptr;
      if (OB_FAIL(query(read_param, iter))) {
        LOG_WARN("do query src by iter failed.", K(ret), K(read_param));
      } else {
        // prepare read buffer
        ObString read_buffer;
        uint64_t read_buff_size = OB_MIN(LOB_READ_BUFFER_LEN, read_param.byte_size_);
        char *read_buff = static_cast<char*>(param.allocator_->alloc(read_buff_size));
        if (OB_ISNULL(read_buff)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc buf failed.", K(ret), K(read_buff_size));
        } else {
          read_buffer.assign_buffer(read_buff, read_buff_size);
        }

        uint64_t write_offset = param.offset_;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(iter->get_next_row(read_buffer))) {
            if (ret != OB_ITER_END) {
              LOG_WARN("failed to get next buffer.", K(ret));
            }
          } else {
            param.offset_ = write_offset;
            uint64_t read_char_len = ObCharset::strlen_char(param.coll_type_, read_buffer.ptr(), read_buffer.length());
            param.len_ = read_char_len;
            if (OB_FAIL(write_inrow_inner(param, read_buffer, old_data))) {
              LOG_WARN("failed to do write", K(ret), K(param));
            } else {
              // update offset and len
              write_offset += read_char_len;
              old_data.assign_ptr(param.lob_common_->get_inrow_data_ptr(), param.byte_size_);
            }
          }
        }
        if (ret == OB_ITER_END) {
          ret = OB_SUCCESS;
        }
      }
      if (OB_NOT_NULL(iter)) {
        iter->reset();
        OB_DELETE(ObLobQueryIter, "unused", iter);
      }
    }
  }
  return ret;
}

int ObLobManager::write_outrow(ObLobAccessParam& param, ObLobLocatorV2& lob, uint64_t offset, ObString& old_data)
{
  int ret = OB_SUCCESS;
  ObLobQueryIter *iter = nullptr;
  SMART_VAR(ObLobAccessParam, read_param) {
    if (OB_ISNULL(param.allocator_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("param tmp allocator is null", K(ret), K(param));
    } else if (OB_FAIL(build_lob_param(read_param, *param.allocator_, param.coll_type_,
                offset, param.len_, param.timeout_, lob))) {
      LOG_WARN("fail to build read param", K(ret), K(lob));
    } else if (OB_FAIL(query(read_param, iter))) {
      LOG_WARN("do query src by iter failed.", K(ret), K(read_param));
    } else {
      ObLobWriteHandler handler(param);
      // prepare read buffer
      ObString read_buffer;
      uint64_t read_buff_size = OB_MIN(LOB_READ_BUFFER_LEN, read_param.byte_size_);
      char *read_buff = static_cast<char*>(param.allocator_->alloc(read_buff_size));
      if (OB_ISNULL(read_buff)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc buf failed.", K(ret), K(read_buff_size));
      } else if (FALSE_IT(read_buffer.assign_buffer(read_buff, read_buff_size))) {
      } else if (OB_FAIL(handler.init(lob_ctx_.lob_meta_mngr_))) {
        LOG_WARN("init handle fail", K(ret), K(handler));
      } else if (OB_FAIL(handler.execute(iter, read_buffer, old_data))) {
        LOG_WARN("fail to write outrow", K(ret), K(param));
      }
    }
  }

  if (OB_NOT_NULL(iter)) {
    iter->reset();
    OB_DELETE(ObLobQueryIter, "unused", iter);
  }
  return ret;
}

int ObLobManager::write(ObLobAccessParam& param, ObLobLocatorV2& lob, uint64_t offset)
{
  int ret = OB_SUCCESS;
  bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else if (OB_FAIL(param.prepare())) {
    LOG_WARN("param prepare fail", K(ret), K(param));
  } else {
    bool is_remote_lob = false;
    common::ObAddr dst_addr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(param.check_handle_size())) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (OB_FAIL(ObLobLocationUtil::is_remote(param, is_remote_lob, dst_addr))) {
      LOG_WARN("check is remote failed.", K(ret), K(param));
    } else if (is_remote_lob) {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("Unsupport remote write", K(ret), K(param));
    } else {
      ObString old_data;
      bool out_row = false;
      if (OB_FAIL(prepare_for_write(param, old_data, out_row))) {
        LOG_WARN("prepare for write failed.", K(ret));
      } else {
        if (!out_row) {
          if (OB_FAIL(write_inrow(param, lob, offset, old_data))) {
            LOG_WARN("failed to process write out row", K(ret), K(param), K(lob), K(offset));
          }
        } else if (OB_FAIL(write_outrow(param, lob, offset, old_data))) {
          LOG_WARN("failed to process write out row", K(ret), K(param), K(lob), K(offset));
        }
      }
    }
  }
  return ret;
}

int ObLobManager::write(ObLobAccessParam& param, ObString& data)
{
  int ret = OB_SUCCESS;
  bool save_is_reverse = param.scan_backward_;
  uint64_t save_param_len = param.len_;
  bool is_char = param.coll_type_ != common::ObCollationType::CS_TYPE_BINARY;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else if (OB_FAIL(param.prepare())) {
    LOG_WARN("param prepare fail", K(ret), K(param));
  } else {
    bool is_remote_lob = false;
    common::ObAddr dst_addr;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(param.check_handle_size())) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (OB_FAIL(ObLobLocationUtil::is_remote(param, is_remote_lob, dst_addr))) {
      LOG_WARN("check is remote failed.", K(ret), K(param));
    } else if (is_remote_lob) {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("Unsupport remote write", K(ret), K(param));
    } else {
      ObString old_data;
      bool out_row = false;
      if (OB_FAIL(prepare_for_write(param, old_data, out_row))) {
        LOG_WARN("prepare for write failed.", K(ret));
      } else if (!out_row) {
        if (OB_FAIL(write_inrow_inner(param, data, old_data))) {
          LOG_WARN("fail to write inrow inner", K(param), K(data));
        }
      } else if (param.lob_locator_ != nullptr && !param.lob_locator_->is_persist_lob()) {
        ret = OB_NOT_IMPLEMENT;
        LOG_WARN("Unsupport outrow tmp lob.", K(ret), K(param));
      } else if (OB_ISNULL(param.allocator_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("param allocator is null", K(ret), K(param));
      } else {
        ObLobWriteHandler handler(param);
        ObLobQueryIter *iter = nullptr;
        // prepare read buffer
        ObString read_buffer;
        uint64_t read_buff_size = ObLobMetaUtil::LOB_OPER_PIECE_DATA_SIZE;
        char *read_buff = static_cast<char*>(param.allocator_->alloc(read_buff_size));
        if (OB_ISNULL(read_buff)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc buf failed.", K(ret), K(read_buff_size));
        } else if (FALSE_IT(read_buffer.assign_buffer(read_buff, read_buff_size))) {
        } else if (OB_FAIL(query_inrow_get_iter(param, data, 0, false, iter))) {
          LOG_WARN("fail to get query iter", K(param), K(data));
        } else if (OB_FAIL(handler.init(lob_ctx_.lob_meta_mngr_))) {
          LOG_WARN("init handle fail", K(ret), K(handler));
        } else if (OB_FAIL(handler.execute(iter, read_buffer, old_data))) {
          LOG_WARN("fail to write outrow", K(ret), K(param));
        }
        if (OB_NOT_NULL(iter)) {
          iter->reset();
          OB_DELETE(ObLobQueryIter, "unused", iter);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    param.len_ = save_param_len;
    param.scan_backward_ = save_is_reverse;
  }
  return ret;
}

int ObLobManager::fill_zero(char *ptr, uint64_t length, bool is_char,
  const ObCollationType coll_type, uint32_t byte_len, uint32_t byte_offset, uint32_t &char_len)
{
  int ret = OB_SUCCESS;
  ObString space = ObCharsetUtils::get_const_str(coll_type, ' ');
  uint32_t space_len = space.length();
  uint32_t converted_len = space.length() * char_len;
  if (converted_len > byte_len) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to fill zero for length invalid", K(ret), K(space_len), K(char_len), K(byte_len));
  } else {
    char* dst_start = ptr + byte_offset + converted_len;
    char* src_start = ptr + byte_offset + byte_len;
    uint32_t cp_len = length - (byte_len + byte_offset);
    if (cp_len > 0 && dst_start != src_start) {
      MEMMOVE(dst_start, src_start, cp_len);
    }
    if (!is_char) {
      MEMSET(ptr + byte_offset, 0x00, converted_len);
    } else {
      if (space_len > 1) {
        for (int i = 0; i < char_len; i++) {
          MEMCPY(ptr + byte_offset + i * space_len, space.ptr(), space_len);
        }
      } else {
        MEMSET(ptr + byte_offset, ' ', char_len);
      }
    }
    char_len = converted_len;
  }
  return ret;
}

int ObLobManager::erase_outrow(ObLobAccessParam& param)
{
  int ret = OB_SUCCESS;
  if (param.is_full_delete()) {
    ObLobFullDeleteHandler handler(param);
    if (OB_FAIL(handler.init(lob_ctx_.lob_meta_mngr_))) {
      LOG_WARN("init fail", K(ret), K(handler));
    } else if (OB_FAIL(handler.execute())) {
      LOG_WARN("execute fail", K(ret), K(handler));
    }
  } else {
    ObLobEraseHandler handler(param);
    if (OB_FAIL(handler.init(lob_ctx_.lob_meta_mngr_))) {
      LOG_WARN("init fail", K(ret), K(handler));
    } else if (OB_FAIL(handler.execute())) {
      LOG_WARN("execute fail", K(ret), K(handler));
    }
  }
  return ret;
}

int ObLobManager::erase(ObLobAccessParam& param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else if (OB_FAIL(param.prepare())) {
    LOG_WARN("param prepare fail", K(ret), K(param));
  } else {
    bool is_remote_lob = false;
    common::ObAddr dst_addr;
    if (OB_FAIL(OB_ISNULL(param.lob_common_))) {
      LOG_WARN("get lob locator null.", K(ret));
    } else if (OB_FAIL(param.check_handle_size())) {
      LOG_WARN("check handle size failed.", K(ret));
    } else if (OB_FAIL(ObLobLocationUtil::is_remote(param, is_remote_lob, dst_addr))) {
      LOG_WARN("check is remote failed.", K(ret), K(param));
    } else if (is_remote_lob) {
      ret = OB_NOT_IMPLEMENT;
      LOG_WARN("Unsupport remote erase", K(ret), K(param));
    } else if (param.lob_common_->in_row_) {
      if (param.lob_common_->is_init_) {
        param.lob_data_ = reinterpret_cast<ObLobData*>(param.lob_common_->buffer_);
      }
      ObString data;
      if (param.lob_data_ != nullptr) {
        data.assign_ptr(param.lob_data_->buffer_, param.lob_data_->byte_size_);
      } else {
        data.assign_ptr(param.lob_common_->buffer_, param.byte_size_);
      }
      uint32_t byte_offset = param.offset_;
      if (OB_UNLIKELY(data.length() < byte_offset)) {
        // offset overflow, do nothing
      } else {
        // allow erase len oversize, get max(param.len_, actual_len)
        uint32_t max_len = ObCharset::strlen_char(param.coll_type_, data.ptr(), data.length()) - byte_offset;
        uint32_t char_len = (param.len_ > max_len) ? max_len : param.len_;
        uint32_t byte_len = char_len;
        ObLobCharsetUtil::transform_query_result_charset(param.coll_type_, data.ptr(), data.length(), byte_len, byte_offset);
        if (OB_UNLIKELY(data.length() < byte_offset + byte_len)) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("data length is not enough.", K(ret), KPC(param.lob_data_), K(byte_offset), K(byte_len));
        } else {
          if (param.is_fill_zero_) { // do fill zero
            bool is_char = (param.coll_type_ != CS_TYPE_BINARY);
            if (OB_FAIL(fill_zero(data.ptr(), data.length(), is_char, param.coll_type_, byte_len, byte_offset, char_len))) {
              LOG_WARN("failed to fill zero", K(ret));
            } else {
              param.byte_size_ = param.byte_size_ - byte_len + char_len;
              if (param.lob_data_ != nullptr) {
                param.lob_data_->byte_size_ = param.byte_size_;
              }
              if (OB_NOT_NULL(param.lob_locator_)) {
                param.lob_locator_->size_ = param.lob_locator_->size_ - byte_len + char_len;
                if (OB_FAIL(fill_lob_locator_extern(param))) {
                  LOG_WARN("fail to fill lob locator extern", K(ret), KPC(param.lob_locator_));
                }
              }
            }
          } else { // do erase
            char* dst_start = data.ptr() + byte_offset;
            char* src_start = data.ptr() + byte_offset + byte_len;
            uint32_t cp_len = data.length() - (byte_len + byte_offset);
            if (cp_len > 0) {
              MEMMOVE(dst_start, src_start, cp_len);
            }
            param.byte_size_ -= byte_len;
            param.handle_size_ -= byte_len;
            if (param.lob_data_ != nullptr) {
              param.lob_data_->byte_size_ = param.byte_size_;
            }
            if (OB_NOT_NULL(param.lob_locator_)) {
              param.lob_locator_->size_ -= byte_len;
              if (OB_FAIL(fill_lob_locator_extern(param))) {
                LOG_WARN("fail to fill lob locator extern", K(ret), KPC(param.lob_locator_));
              }
            }
          }
        }
      }
    } else if (param.is_fill_zero_) {
      ObLobFillZeroHandler handler(param);
      if (OB_FAIL(handler.init(lob_ctx_.lob_meta_mngr_))) {
        LOG_WARN("init fail", K(ret), K(param));
      } else if (OB_FAIL(handler.execute())) {
        LOG_WARN("fill_zero_outrow fail", K(ret), K(param));
      }
    } else if (OB_FAIL(erase_outrow(param))) {
      LOG_WARN("failed erase", K(ret));
    }
  }
  return ret;
}

int ObLobManager::build_lob_param(ObLobAccessParam& param,
                                  ObIAllocator &allocator,
                                  ObCollationType coll_type,
                                  uint64_t offset,
                                  uint64_t len,
                                  int64_t timeout,
                                  ObLobLocatorV2 &lob)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(param.set_lob_locator(&lob))) {
    LOG_WARN("set lob locator failed");
  } else {
    param.coll_type_ = coll_type;
    if (param.coll_type_ == CS_TYPE_INVALID) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get collation type failed.", K(ret));
    } else {
      // common arg
      param.allocator_ = &allocator;
      param.byte_size_ = param.lob_common_->get_byte_size(param.handle_size_);
      param.offset_ = offset;
      param.len_ = len;
      param.timeout_ = timeout;
      // outrow arg for do lob meta scan
      if (OB_SUCC(ret) && lob.is_persist_lob() && !lob.has_inrow_data()) {
        ObMemLobLocationInfo *location_info = nullptr;
        ObMemLobExternHeader *extern_header = NULL;
        bool is_remote = false;
        if (OB_FAIL(lob.get_extern_header(extern_header))) {
          LOG_WARN("failed to get extern header", K(ret), K(lob));
          LOG_WARN("failed to get tx info", K(ret), K(lob));
        } else if (OB_FAIL(lob.get_location_info(location_info))) {
          LOG_WARN("failed to get location info", K(ret), K(lob));
        } else if (OB_FALSE_IT(param.ls_id_ = share::ObLSID(location_info->ls_id_))) {
        } else if (OB_FALSE_IT(param.tablet_id_ = ObTabletID(location_info->tablet_id_))) {
        } else if (OB_FAIL(param.set_tx_read_snapshot(lob))) {
          LOG_WARN("set_tx_read_snapshot fail", K(ret), K(param), K(lob));
        } else if (OB_FAIL(ObLobLocationUtil::is_remote(param, is_remote, param.addr_))) {
          LOG_WARN("get lob addr fail", K(ret), K(param), K(lob));
        }
      }
    }
  }
  return ret;
}

int ObLobManager::append_outrow(ObLobAccessParam& param, bool ori_is_inrow, ObString &data)
{
  int ret = OB_SUCCESS;
  if (param.is_full_insert()) {
    ObLobFullInsertHandler handler(param);
    if (OB_FAIL(handler.init(lob_ctx_.lob_meta_mngr_))) {
      LOG_WARN("init fail", K(ret), K(handler));
    } else if (OB_FAIL(handler.execute(data))) {
      LOG_WARN("execute fail", K(ret), K(handler));
    }
  } else {
    ObLobAppendHandler handler(param);
    if (OB_FAIL(handler.init(lob_ctx_.lob_meta_mngr_))) {
      LOG_WARN("init fail", K(ret), K(handler));
    } else if (OB_FAIL(handler.execute(data, ori_is_inrow))) {
      LOG_WARN("execute fail", K(ret), K(handler));
    }
  }
  return ret;
}

int ObLobManager::append_outrow(
    ObLobAccessParam& param,
    ObLobLocatorV2& lob,
    int64_t append_lob_len,
    ObString& ori_inrow_data)
{
  int ret = OB_SUCCESS;
  ObLobQueryIter *iter = nullptr;
  SMART_VAR(ObLobAccessParam, read_param) {
    read_param.tenant_id_ = param.src_tenant_id_;
    if (OB_ISNULL(param.get_tmp_allocator())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("param allocator is null", K(ret), K(param));
    } else if (OB_FAIL(build_lob_param(read_param, *param.get_tmp_allocator(), param.coll_type_,
                0, UINT64_MAX, param.timeout_, lob))) {
      LOG_WARN("fail to build read param", K(ret), K(lob));
    } else if (OB_FAIL(query(read_param, iter))) {
      LOG_WARN("do query src by iter failed.", K(ret), K(read_param));
    } else {
      if (param.is_full_insert()) {
        ObLobFullInsertHandler handler(param);
        if (OB_FAIL(handler.init(lob_ctx_.lob_meta_mngr_))) {
          LOG_WARN("init fail", K(ret), K(handler));
        } else if (OB_FAIL(handler.execute(iter, append_lob_len, ori_inrow_data))) {
          LOG_WARN("execute fail", K(ret), K(handler), K(param), K(read_param));
        }
      } else {
        ObLobAppendHandler handler(param);
        if (OB_FAIL(handler.init(lob_ctx_.lob_meta_mngr_))) {
          LOG_WARN("init fail", K(ret), K(handler));
        } else if(OB_FAIL(handler.execute(iter, append_lob_len, ori_inrow_data))) {
          LOG_WARN("execute fail", K(ret), K(handler), K(read_param));
        }
      }
    }
    // finish query, release resource
    if (OB_NOT_NULL(iter)) {
      iter->reset();
      OB_DELETE(ObLobQueryIter, "unused", iter);
    }
  }
  return ret;
}

int ObLobManager::query_outrow(ObLobAccessParam& param, ObLobQueryIter *&result)
{
  int ret = OB_SUCCESS;
  ObLobQueryIterHandler handler(param);
  if (OB_FAIL(handler.init(lob_ctx_.lob_meta_mngr_))) {
    LOG_WARN("init handler fail", K(ret), K(param));
  } else  if (OB_FAIL(handler.execute())) {
    LOG_WARN("handler execute fail", K(ret), K(param));
  } else {
    result = handler.result_;
  }
  return ret;
}

int ObLobManager::query_outrow(ObLobAccessParam& param, ObString &buffer)
{
  int ret = OB_SUCCESS;
  ObLobQueryDataHandler handler(param, buffer);
  if (OB_FAIL(handler.init(lob_ctx_.lob_meta_mngr_))) {
    LOG_WARN("init handler fail", K(ret), K(param));
  } else  if (OB_FAIL(handler.execute())) {
    LOG_WARN("handler execute fail", K(ret), K(param));
  }
  return ret;
}

int ObLobManager::insert(ObLobAccessParam& param, const ObLobLocatorV2 &src_data_locator, ObArray<ObLobMetaInfo> &lob_meta_list)
{
  int ret = OB_SUCCESS;
  int64_t new_byte_len = 0;
  if (OB_FAIL(src_data_locator.get_lob_data_byte_len(new_byte_len))) {
    LOG_WARN("fail to get lob byte len", K(ret), K(src_data_locator));
  } else if (OB_ISNULL(param.lob_common_)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("null lob common", K(ret), K(param));
  } else if (! param.lob_common_->is_init_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob common not init", K(ret), KPC(param.lob_common_), K(param));
  } else if (OB_ISNULL(param.lob_data_ = reinterpret_cast<ObLobData*>(param.lob_common_->buffer_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob_data_ is null", KR(ret), K(param));
  } else if (OB_FAIL(param.init_out_row_ctx(new_byte_len))) {
    LOG_WARN("init lob data out row ctx failed", K(ret));
  } else {
    ObLobSimplePersistInsertIter insert_iter(&param, param.allocator_, lob_meta_list);
    if (OB_FAIL(insert_iter.init())) {
      LOG_WARN("init iter fail", K(ret), K(param));
    } else if (OB_FAIL(lob_ctx_.lob_meta_mngr_->batch_insert(param, insert_iter))) {
      LOG_WARN("write lob meta row failed.", K(ret));
    }
  }
  return ret;
}

int ObLobManager::prepare_insert_task(
    ObLobAccessParam& param,
    bool &is_outrow,
    ObLobDataInsertTask &task)
{
  int ret = OB_SUCCESS;
  // old inrow  | new inrow   --> alloc new locator and but no need lob id
  // old inrow  | new outrow  --> alloc new locator and need new lob id
  // old outrow | new inrow   --> alloc new locator, but no need lob id
  // old outrow | new outrow  --> keep locator

  const int64_t lob_inrow_threshold = param.get_inrow_threshold();
  int64_t new_byte_len = 0;
  if (OB_FAIL(task.src_data_locator_.get_lob_data_byte_len(new_byte_len))) {
    LOG_WARN("fail to get lob byte len", K(ret), K(task));
  } else if (new_byte_len <= lob_inrow_threshold) {
    // skip if inrow store
  } else if (OB_FAIL(prepare_outrow_locator(param, task))) {
    LOG_WARN("prepare_outrow_locator fail", K(ret), K(task), K(param));
  } else {
    is_outrow = true;
  }
  return ret;
}

int ObLobManager::prepare_outrow_locator(ObLobAccessParam& param, ObLobDataInsertTask &task)
{
  int ret = OB_SUCCESS;
  const ObLobLocatorV2 &src_data_locator = task.src_data_locator_;
  ObLobDiskLocatorBuilder locator_builder;
  int64_t new_byte_len = 0;
  const int64_t lob_chunk_size = param.get_schema_chunk_size();
  if (OB_FAIL(src_data_locator.get_lob_data_byte_len(new_byte_len))) {
    LOG_WARN("fail to get lob byte len", K(ret), K(src_data_locator));
  } else if (OB_FAIL(locator_builder.init(*param.allocator_))) {
    LOG_WARN("prepare_locator fail", K(ret), K(param));
  } else if (OB_FAIL(prepare_lob_id(param, locator_builder))) {
    LOG_WARN("prepare_lob_id fail", K(ret), K(param));
  } else if (OB_FAIL(locator_builder.set_chunk_size(lob_chunk_size))) {
    LOG_WARN("set chunk size fail", K(ret), K(param));
  } else if (OB_FAIL(locator_builder.set_byte_len(new_byte_len))) {
    LOG_WARN("set byte len fail", K(ret), K(new_byte_len), K(locator_builder), K(param));
  } else if (OB_FAIL(prepare_char_len(param, locator_builder, task))) {
    LOG_WARN("prepare_char_len fail", K(ret), K(locator_builder), K(param));
  } else if (OB_FAIL(prepare_seq_no(param, locator_builder, task))) {
    LOG_WARN("prepare_seq_no fail", K(ret), K(locator_builder));
  } else if (OB_FAIL(locator_builder.to_locator(task.cur_data_locator_))) {
    LOG_WARN("to locator fail", K(ret), K(locator_builder), K(param));
  } else {
    LOG_DEBUG("prepare disk lob locator success", K(locator_builder));
  }
  return ret;
}

int ObLobManager::prepare_char_len(ObLobAccessParam& param, ObLobDiskLocatorBuilder &locator_builder, ObLobDataInsertTask &task)
{
  int ret = OB_SUCCESS;
  const ObLobLocatorV2 &src_data_locator = task.src_data_locator_;
  int64_t new_byte_len = 0;
  uint64_t char_len = 0;
  const int64_t lob_chunk_size = param.get_schema_chunk_size();
  if (OB_FAIL(src_data_locator.get_lob_data_byte_len(new_byte_len))) {
    LOG_WARN("fail to get lob byte len", K(ret), K(src_data_locator));
  } else if (OB_FAIL(is_store_char_len(param, lob_chunk_size, new_byte_len))) {
    LOG_WARN("calc is_store_char_len failed.", K(ret), K(new_byte_len), K(param));
  } else if (! param.is_store_char_len_) {
    char_len = UINT64_MAX;
  } else if (param.is_blob()) {
    // blob char_len is equal byte_len
    char_len = new_byte_len;
  } else {
    ObString inrow_data;
    ObInRowLobDataSpliter spilter(task.lob_meta_list_);
    if (! src_data_locator.has_inrow_data()) {
      if (OB_FAIL(ObLobDiskLocatorWrapper::get_char_len(src_data_locator, char_len))) {
        LOG_WARN("get_char_len fail", K(ret), K(src_data_locator));
      }
    } else if (OB_FAIL(src_data_locator.get_inrow_data(inrow_data))) {
      LOG_WARN("get inrow data fail", K(ret), K(src_data_locator));
    } else if (inrow_data.length() != new_byte_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("byte len is not match", K(ret), K(new_byte_len), "inrow_data_length", inrow_data.length());
    } else if (OB_FAIL(spilter.split(param.coll_type_, param.get_schema_chunk_size(), inrow_data))) {
      LOG_WARN("init spilter fail", K(ret), K(src_data_locator));
    } else {
      char_len = spilter.char_pos();
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(locator_builder.set_char_len(char_len))) {
    LOG_WARN("set char_len fail", K(ret), K(char_len), K(locator_builder), K(param));
  }
  return ret;
}

int ObLobManager::prepare_lob_id(ObLobAccessParam& param, ObLobDiskLocatorBuilder &locator_builder)
{
  int ret = OB_SUCCESS;
  ObLobId lob_id;
  if (OB_ISNULL(param.lob_common_)) {
    if (OB_FAIL(alloc_lob_id(param, lob_id))) {
      LOG_WARN("alloc_lob_id fail", K(ret));
    }
  } else {
    const ObLobCommon *lob_common = param.lob_common_;
    if (lob_common->in_row_ || ! lob_common->is_init_) {
      if (OB_FAIL(alloc_lob_id(param, lob_id))) {
        LOG_WARN("alloc_lob_id fail", K(ret));
      }
    } else {
      const ObLobData *lob_data = reinterpret_cast<const ObLobData*>(lob_common->buffer_);
      lob_id = lob_data->id_;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (! lob_id.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob id is invalid", K(ret), K(lob_id), K(param));
  } else if (OB_FAIL(locator_builder.set_lob_id(lob_id))) {
    LOG_WARN("set lob id fail", K(ret), K(lob_id), K(locator_builder), K(param));
  }
  return ret;
}

int ObLobManager::alloc_lob_id(ObLobAccessParam& param, ObLobId &lob_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(lob_ctx_.lob_meta_mngr_->fetch_lob_id(param, lob_id.lob_id_))) {
    LOG_WARN("get lob id failed.", K(ret), K(param));
  } else if (! param.lob_meta_tablet_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob_meta_tablet_id is invalid", K(ret), K(param));
  } else {
    lob_id.tablet_id_ = param.lob_meta_tablet_id_.id();
    // used for lob order
    transform_lob_id(lob_id.lob_id_, lob_id.lob_id_);
  }
  return ret;
}


int ObLobManager::prepare_seq_no(ObLobAccessParam& param, ObLobDiskLocatorBuilder &locator_builder, ObLobDataInsertTask &task)
{
  int ret = OB_SUCCESS;
  ObLobDataOutRowCtx::OpType type = ObLobDataOutRowCtx::OpType::EXT_INFO_LOG;
  int64_t seq_no_cnt = 1;
  transaction::ObTxSEQ seq_no_st;
  int64_t new_byte_len = 0;
  const int64_t lob_chunk_size = param.get_schema_chunk_size();
  if (OB_FAIL(task.src_data_locator_.get_lob_data_byte_len(new_byte_len))) {
    LOG_WARN("fail to get lob byte len", K(ret), K(task));
  } else if (new_byte_len < lob_chunk_size && (OB_ISNULL(param.lob_common_) || param.lob_common_->in_row_)) {
    // means insert, not update
    type = ObLobDataOutRowCtx::OpType::SQL;
  } else if (OB_FAIL(locator_builder.set_ext_info_log_length(ObLobManager::LOB_OUTROW_FULL_SIZE + 1 /*ext info log type*/))) {
    LOG_WARN("set_ext_info_log_length fail", K(ret), K(locator_builder));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(param.tx_desc_->get_and_inc_tx_seq(param.parent_seq_no_.get_branch(), seq_no_cnt, seq_no_st))) {
    LOG_WARN("alloc seq_no fail", K(ret), K(seq_no_cnt), K(param));
  } else if (OB_FAIL(locator_builder.set_seq_no(type, seq_no_st, seq_no_cnt))) {
    LOG_WARN("set seq_no fail", K(ret), K(param));
  }
  return ret;
}

} // storage
} // oceanbase
