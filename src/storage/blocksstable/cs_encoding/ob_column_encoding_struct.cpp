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

#include "ob_column_encoding_struct.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "ob_cs_encoding_util.h"


namespace oceanbase
{
namespace blocksstable
{
using namespace common;

const bool ObCSEncodingOpt::STREAM_ENCODINGS_DEFAULT[ObIntegerStream::EncodingType::MAX_TYPE]
  = {
       false, // MIN_TYPE
       true,  // RAW
       true,  // DOUBLE_DELTA_ZIGZAG_RLE
       true,  // DOUBLE_DELTA_ZIGZAG_PFOR
       true,  // DELTA_ZIGZAG_RLE
       true,  // DELTA_ZIGZAG_PFOR
       true,  // SIMD_FIXEDPFOR
       true,  // UNIVERSAL_COMPRESS
       true  // XOR_FIXED_PFOR
      };
const bool ObCSEncodingOpt::STREAM_ENCODINGS_NONE[ObIntegerStream::EncodingType::MAX_TYPE]
    = {false, false, false, false, false, false, false, false, false};

int ObPreviousCSEncoding::init(const int32_t col_count)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(previous_encoding_of_columns_.prepare_allocate(col_count))) {
    LOG_WARN("fail to prepare_allocate", K(ret), K(col_count));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObPreviousCSEncoding::check_and_set_state(const int32_t column_idx,
                                              const ObColumnEncodingIdentifier identifier,
                                              const int64_t cur_block_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    ObPreviousColumnEncoding &previous_info = previous_encoding_of_columns_.at(column_idx);
    if (previous_info.is_valid_) {
      if (previous_info.identifier_ != identifier) {
        previous_info.is_valid_ = false;
      } else {
        if (0 == cur_block_count % previous_info.redetect_cycle_) {
          previous_info.need_redetect_ = true;
        } else {
          previous_info.need_redetect_ = false;
        }
        if (previous_info.force_no_redetect_) {
          previous_info.need_redetect_ = false;
        }
      }
    }
    previous_info.cur_block_count_ = cur_block_count;
  }
  return ret;
}

const int32_t ObPreviousCSEncoding::MAX_REDETECT_CYCLE = 64;

int ObPreviousCSEncoding::update_column_encoding_types(
                        const int32_t column_idx,
                        const ObColumnEncodingIdentifier identifier,
                        const ObIntegerStream::EncodingType *stream_types,
                        bool force_no_redetect)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(stream_types == nullptr && identifier.int_stream_count_ != 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argumnet", KR(ret), KP(stream_types), K(identifier));
  } else {
    ObPreviousColumnEncoding &previous = previous_encoding_of_columns_.at(column_idx);
    if (previous.is_valid_ && previous.identifier_ != identifier) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("identifier must be same", KR(ret), K(column_idx), K(identifier), K(previous));
    } else if (previous.is_valid_) {
      bool stream_encoding_type_not_change = true;
      for (int32_t i = 0; i < identifier.int_stream_count_; i++) {
        if (previous.stream_encoding_types_[i] != stream_types[i]) {
          stream_encoding_type_not_change = false;
          break;
        }
      }
      if (stream_encoding_type_not_change) {
        previous.redetect_cycle_ = previous.redetect_cycle_ << 1;
        previous.redetect_cycle_ = std::min(previous.redetect_cycle_, MAX_REDETECT_CYCLE);
      } else {
        for (int32_t i = 0; i < identifier.int_stream_count_; i++) {
          previous.stream_encoding_types_[i] = stream_types[i];
        }
        previous.redetect_cycle_ = 2;
      }
      previous.force_no_redetect_ = force_no_redetect;
    } else {
      previous.identifier_ = identifier;
      for (int32_t i = 0; i < identifier.int_stream_count_; i++) {
        previous.stream_encoding_types_[i] = stream_types[i];
      }
      previous.is_valid_ = true;
      previous.redetect_cycle_ = 2;
      previous.force_no_redetect_ = force_no_redetect;
    }
  }

  return ret;
}

void ObColumnCSEncodingCtx::try_set_need_sort(const ObCSColumnHeader::Type type, const int64_t column_index,
                                              const bool micro_block_has_lob_out_row)
{
  ObObjTypeClass col_tc = encoding_ctx_->col_descs_->at(column_index).col_type_.get_type_class();
  ObObjTypeStoreClass col_sc = get_store_class_map()[col_tc];
  const bool encoding_type_need_sort = (type == ObCSColumnHeader::INT_DICT || type == ObCSColumnHeader::STR_DICT);
  const bool col_is_lob_out_row = micro_block_has_lob_out_row && store_class_might_contain_lob_locator(col_sc);
  if (!encoding_type_need_sort) {
    need_sort_ = false;
  } else if (col_is_lob_out_row) {
    need_sort_ = false;
  } else if (ObCSEncodingUtil::is_no_need_sort_lob(col_sc)) {
    need_sort_ = false;
  } else {
    need_sort_ = true;
  }
}

}
}
