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

#include "ob_string_prefix_encoder.h"
#include "lib/container/ob_array_iterator.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "ob_bit_stream.h"
#include "ob_integer_array.h"
#include "ob_multi_prefix_tree.h"

namespace oceanbase
{
namespace blocksstable
{

using namespace common;
const ObColumnHeader::Type ObStringPrefixEncoder::type_;

ObStringPrefixEncoder::ObStringPrefixEncoder() : meta_header_(NULL),
    prefix_count_(0), prefix_index_byte_(0), prefix_length_(0),
    prefix_tree_(NULL), calc_size_(0), col_ctx_(NULL)
{
}

ObStringPrefixEncoder::~ObStringPrefixEncoder()
{
}

int ObStringPrefixEncoder::init(
    const ObColumnEncodingCtx &ctx,
    const int64_t column_index,
    const ObConstDatumRowArray &rows)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(ObIColumnEncoder::init(ctx, column_index, rows))) {
    LOG_WARN("init base column encoder failed", K(ret), K(ctx), K(column_index), "row_count", rows.count());
  } else {
    const ObObjTypeStoreClass sc = get_store_class_map()[
        ob_obj_type_class(column_type_.get_type())];
    if (OB_UNLIKELY(!is_string_encoding_valid(sc))) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support type for string prefix", K(ret), K(sc), K_(column_index));
    }
    column_header_.type_ = type_;
    prefix_tree_ = ctx.prefix_tree_;
    col_ctx_ = &ctx;
  }
  return ret;
}

void ObStringPrefixEncoder::reuse()
{
  ObIColumnEncoder::reuse();
  meta_header_ = NULL;
  prefix_count_ = 0;
  prefix_index_byte_ = 0;
  prefix_length_ = 0;
  prefix_tree_ = NULL;
  MEMSET(&hex_string_map_, 0, sizeof(ObHexStringMap));
  calc_size_ = 0;
  col_ctx_ = NULL;
}

int ObStringPrefixEncoder::traverse(bool &suitable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    suitable = true;
    prefix_tree_->reuse();
    prefix_tree_->set_cnode_cnt(rows_->count());
    if (OB_FAIL(prefix_tree_->build_tree(col_ctx_->col_datums_,
        col_ctx_->last_prefix_length_, suitable, prefix_count_, prefix_length_))) {
      LOG_WARN("failed to build prefix tree", K(ret), K_(column_index));
    } else if (prefix_count_ > UINT8_MAX) {
      suitable = false;
    } else if (suitable) {
      if (0 < prefix_length_) {
        prefix_index_byte_ = prefix_length_ <= UINT8_MAX ? 1 : 2;
        // meta length could not be larger than UINT16_MAX
        if (OB_UNLIKELY(prefix_length_ > UINT16_MAX)) {
          suitable = false;
        }
      } else {
        suitable = false;
      }

      if (suitable) {
        // hex packing check
        bool hex_packing = true;
        ObMultiPrefixTree::CellNode *cnodes = prefix_tree_->get_cell_nodes();
        for (int64_t cid = 0; cid < rows_->count(); ++cid) {
          const ObMultiPrefixTree::CellNode &cnode = cnodes[cid];
          // for null and nope, cnode.len_ < 0
          for (int64_t i = cnode.len_; 0 <= i && hex_packing && i < cnode.datum_->len_; ++i) {
            hex_string_map_.mark(static_cast<unsigned char>(cnode.datum_->ptr_[i]));
          }
          hex_packing = hex_string_map_.can_packing();
        }

        desc_.need_data_store_ = true;
        desc_.is_var_data_ = true;
        desc_.fix_data_length_ = 0;
        desc_.has_null_ = prefix_tree_->get_null_cnt() > 0;
        desc_.has_nope_ = prefix_tree_->get_nope_cnt() > 0;
        desc_.need_extend_value_bit_store_ = desc_.has_null_ || desc_.has_nope_;
        if (desc_.need_extend_value_bit_store_) {
          column_header_.set_has_extend_value_attr();
        }
      }
    }
  }
  return ret;
}

int64_t ObStringPrefixEncoder::calc_size() const
{
  if (calc_size_ == 0) {
    calc_size_ += sizeof(ObStringPrefixMetaHeader);
    calc_size_ += (prefix_count_ - 1) * prefix_index_byte_;
    calc_size_ += prefix_length_;
    if (desc_.need_extend_value_bit_store_) {
      calc_size_ += (rows_->count() * ctx_->extend_value_bit_ + 1) / CHAR_BIT;
    }
    ObMultiPrefixTree::CellNode *cnodes = prefix_tree_->get_cell_nodes();
    for (int64_t cid = 0; cid < rows_->count(); ++cid) {
      if (0 <= cnodes[cid].len_) {
        calc_size_ += sizeof(ObStringPrefixCellHeader);
        if (hex_string_map_.can_packing()) {
          calc_size_ += (cnodes[cid].datum_->len_ - cnodes[cid].len_ + 1) / 2;
        } else {
          calc_size_ += cnodes[cid].datum_->len_ - cnodes[cid].len_;
        }
      }
    }
  }
  return calc_size_;
}

int ObStringPrefixEncoder::set_data_pos(const int64_t offset, const int64_t length)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(meta_header_)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("set data pos is called before store meta", K(ret));
  } else if (OB_UNLIKELY(offset < 0 || length < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(offset), K(length));
  } else {
    meta_header_->offset_ = static_cast<uint32_t>(offset);
    meta_header_->length_ = static_cast<uint32_t>(length);
  }
  return ret;
}

int ObStringPrefixEncoder::get_var_length(const int64_t row_id, int64_t &length)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(0 > row_id || rows_->count() <= row_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row_id));
  } else {
    ObMultiPrefixTree::CellNode &cnode = prefix_tree_->get_cell_nodes()[row_id];
    if (0 > cnode.len_) { // null or nope
      length = 0;
    } else {
      length = cnode.datum_->len_ - cnode.len_;
      if (meta_header_->is_hex_packing()) {
        length = (length + 1) / 2 + sizeof(ObStringPrefixCellHeader);
      } else {
        length += sizeof(ObStringPrefixCellHeader);
      }
    }
  }
  return ret;
}

int ObStringPrefixEncoder::store_meta(ObBufferWriter &buf_writer)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    bool hex_packing = hex_string_map_.can_packing();
    char *buf = buf_writer.current();
    meta_header_ = reinterpret_cast<ObStringPrefixMetaHeader *>(buf);

    // store as var
    int64_t size = sizeof(ObStringPrefixMetaHeader);
    size += hex_packing ? hex_string_map_.size_ : 0;
    buf += size; // index buf position
    size += (prefix_count_ - 1) * prefix_index_byte_ + prefix_length_;

    ObIntegerArrayGenerator gen;
    if (OB_FAIL(buf_writer.advance_zero(size))) {
      LOG_WARN("failed to advance buf", K(ret), K(size));
    } else if (OB_FAIL(gen.init(buf, prefix_index_byte_))) {
      LOG_WARN("failed to init integer array generator", K(ret),
          K(prefix_index_byte_), KP(buf));
    } else {
      int64_t offset = 0;
      ObMultiPrefixTree::TreeNode *tnodes = prefix_tree_->get_tree_nodes();
      buf += (prefix_count_ - 1) * prefix_index_byte_;

      MEMSET(reinterpret_cast<char *>(meta_header_), 0, size);
      if (hex_packing) {
        meta_header_->set_hex_char_array_size(hex_string_map_.size_);
        hex_string_map_.build_index(reinterpret_cast<unsigned char *>(
            meta_header_->hex_char_array_));
      }

      // have to from end to begin,
      // since in complete build, ref is builded from end to begin
      for (int64_t tid = prefix_tree_->get_tnode_cnt() - 1; 0 <= tid; --tid) {
        const ObMultiPrefixTree::TreeNode *tnode = &tnodes[tid];
        if (tnode->is_leaf()) {
          MEMCPY(buf + offset, tnode->prefix_->ptr_, tnode->prefix_->len_);
          _LOG_DEBUG("debug: meta ref=%ld, offset=%ld, string=%.*s", tnode->ref_, offset,
              tnode->prefix_->len_, tnode->prefix_->ptr_);
          if (0 < tnode->ref_) {
            gen.get_array().set(tnode->ref_ - 1, offset);
          }
          offset += tnode->prefix_->len_;
        }
      }
    }

    if (OB_SUCC(ret)) {
      meta_header_->set_prefix_index_byte(prefix_index_byte_);
      meta_header_->count_ = static_cast<uint8_t>(prefix_count_);
      meta_header_->max_string_size_ = static_cast<uint32_t>(ctx_->max_string_size_);
    }
    LOG_DEBUG("debug: header", K_(*meta_header), K_(column_header), K(size));
  }
  return ret;
}

int ObStringPrefixEncoder::store_data(const int64_t row_id, ObBitStream &bs,
    char *buf, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(0 > row_id || rows_->count() <= row_id || 0 > len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(row_id), K(len));
  } else {
    const ObMultiPrefixTree::CellNode &cnode = prefix_tree_->get_cell_nodes()[row_id];
    const ObDatum &datum = *cnode.datum_;
    const ObStoredExtValue ext_val = get_stored_ext_value(datum);
    if (STORED_NOT_EXT != ext_val) {
      if (OB_FAIL(bs.set(column_header_.extend_value_index_,
              extend_value_bit_, static_cast<int64_t>(ext_val)))) {
        LOG_WARN("store extend value bit failed",
            K(ret), K_(column_header), K_(extend_value_bit), K(ext_val));
      }
    } else {
      ObStringPrefixCellHeader *cell_header = reinterpret_cast<ObStringPrefixCellHeader *>(buf);
      cell_header->reset();
      cell_header->set_ref(cnode.ref_);
      cell_header->len_ = static_cast<uint16_t>(cnode.len_);
      if (meta_header_->is_hex_packing()) {
        cell_header->set_odd((datum.len_ - cnode.len_) % 2);
        ObHexStringPacker packer(hex_string_map_,
            reinterpret_cast<unsigned char *>(buf + sizeof(ObStringPrefixCellHeader)));
        for (int64_t i = cnode.len_; i < datum.len_; ++i) {
          packer.pack(static_cast<unsigned char>(datum.ptr_[i]));
        }
        //LOG_DEBUG("debug: hex data", K(cell_header->get_ref()),
            //K(cell_header->get_odd()), K(cell_header->len_),
            //K(cell.v_.string_ + cnode.len_), K(cell.val_len_ - cnode.len_), K(len));
      } else {
        if (0 < len) {
          MEMCPY(buf + sizeof(ObStringPrefixCellHeader),
              datum.ptr_ + cnode.len_, len - sizeof(ObStringPrefixCellHeader));
        }
        //LOG_DEBUG("debug: data", K(cell_header->get_ref()), K(cell_header->len_),
            //K(cell.v_.string_ + cnode.len_), K(cell.val_len_ - cnode.len_), K(len));
      }
    }
  }
  return ret;
}

int ObStringPrefixEncoder::store_fix_data(ObBufferWriter &buf_writer)
{
  UNUSED(buf_writer);
  return OB_NOT_SUPPORTED;
}

} // end namespace blocksstable
} // end namespace oceanbase
