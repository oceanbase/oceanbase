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

int ObPreviousCSEncoding::update_column_detect_info(const int32_t column_idx,
                                                 const ObColumnEncodingIdentifier identifier,
                                                 const int64_t cur_block_count,
                                                 const int64_t major_working_cluster_version)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else {
    ObPreviousColumnEncoding &previous = previous_encoding_of_columns_.at(column_idx);
    if (previous.is_stream_encoding_type_valid_) {
      if (previous.identifier_ != identifier) {
        previous.is_stream_encoding_type_valid_ = false;
      } else {
        if (previous.force_no_redetect_) {
          previous.stream_need_redetect_ = false;
        } else if (0 == cur_block_count % previous.stream_redetect_cycle_) {
          previous.stream_need_redetect_ = true;
        } else {
          previous.stream_need_redetect_ = false;
        }
      }
    }

    const int32_t max_redect_cycle = 128;
    const int32_t min_redect_cycle = 16;

    if (major_working_cluster_version <= DATA_VERSION_4_3_5_0) {
      // For compatibility, awlays redect column encoding type when version <= 435bp0
      previous.column_need_redetect_ = true;
    } else  if (previous.is_column_encoding_type_valid()) {
      if (previous.force_no_redetect_) {
        previous.column_need_redetect_ = false;
      } else if (previous.identifier_.column_encoding_type_ == identifier.column_encoding_type_) {
        if (previous.column_need_redetect_) { // has done redection and column encoding type not changed
          previous.column_need_redetect_ = false;
          previous.column_redetect_cycle_ = previous.column_redetect_cycle_ << 1;
          previous.column_redetect_cycle_ = std::min(previous.column_redetect_cycle_, max_redect_cycle);
        } else if (0 == cur_block_count % previous.column_redetect_cycle_) {
          previous.column_need_redetect_ = true;
        }
      } else {
        previous.identifier_.column_encoding_type_ = identifier.column_encoding_type_;
        previous.column_redetect_cycle_ = min_redect_cycle;
      }
    } else {
      previous.identifier_.column_encoding_type_ = identifier.column_encoding_type_;
      previous.column_redetect_cycle_ = min_redect_cycle;
    }

    previous.column_idx_ = column_idx;
    previous.cur_block_count_ = cur_block_count;
  }
  return ret;
}


int ObPreviousCSEncoding::update_stream_detect_info(
                        const int32_t column_idx,
                        const ObColumnEncodingIdentifier identifier,
                        const ObIntegerStream::EncodingType *stream_types,
                        const int64_t major_working_cluster_version,
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
    int32_t max_redect_cycle = 0;
    int32_t min_redect_cycle = 0;
    if (major_working_cluster_version <= DATA_VERSION_4_3_5_0) {
      max_redect_cycle = 64;
      min_redect_cycle = 2;
    } else {
      max_redect_cycle = 128;
      min_redect_cycle = 16;
    }
    ObPreviousColumnEncoding &previous = previous_encoding_of_columns_.at(column_idx);
    if (previous.is_stream_encoding_type_valid_ && previous.identifier_ != identifier) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("identifier must be same", KR(ret), K(column_idx), K(identifier), K(previous));
    } else if (previous.is_stream_encoding_type_valid_) {
      if (major_working_cluster_version > DATA_VERSION_4_3_5_0 && !previous.stream_need_redetect_) {
        // Previous stream encoding is valid and no redetection occurs, which represent the previous
        // encoding has been reused. There is no need to increase the redect_cycle. We fix this case
        // in OB435bp1.
      } else {
        bool stream_encoding_type_not_change = true;
        for (int32_t i = 0; i < identifier.int_stream_count_; i++) {
          if (previous.stream_encoding_types_[i] != stream_types[i]) {
            stream_encoding_type_not_change = false;
            break;
          }
        }
        if (stream_encoding_type_not_change) {
          previous.stream_redetect_cycle_ = previous.stream_redetect_cycle_ << 1;
          previous.stream_redetect_cycle_ = std::min(previous.stream_redetect_cycle_, max_redect_cycle);
        } else {
          for (int32_t i = 0; i < identifier.int_stream_count_; i++) {
            previous.stream_encoding_types_[i] = stream_types[i];
          }
          previous.stream_redetect_cycle_ = min_redect_cycle;
        }
      }
      previous.force_no_redetect_ = force_no_redetect;
    } else {
      // column_encoding_type_ has been update in check_and_set_identifier, so must be equal here
      OB_ASSERT(previous.identifier_.column_encoding_type_ == identifier.column_encoding_type_);
      previous.identifier_ = identifier;
      for (int32_t i = 0; i < identifier.int_stream_count_; i++) {
        previous.stream_encoding_types_[i] = stream_types[i];
      }
      previous.is_stream_encoding_type_valid_ = true;
      previous.stream_redetect_cycle_ = min_redect_cycle;
      previous.force_no_redetect_ = force_no_redetect;
    }
  }

  return ret;
}

void ObColumnCSEncodingCtx::try_set_need_sort(const ObCSColumnHeader::Type type,
                                              const int64_t column_index,
                                              const bool micro_block_has_lob_out_row,
                                              const int64_t major_working_cluster_version)
{
  if (major_working_cluster_version <= DATA_VERSION_4_3_5_0) {
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
  } else {
    // only sort int dict when version > DATA_VERSION_4_3_5_0
    need_sort_ = (type == ObCSColumnHeader::INT_DICT);
  }
}

}
}
