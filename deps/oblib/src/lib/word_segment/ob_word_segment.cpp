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

#define USING_LOG_PREFIX LIB_WS

#include "ob_word_segment.h"

namespace oceanbase {
namespace common {
const char* ObWordSegment::TokenizerName[WS_MAX + 1] = {
    "TAOBAO_CHN",
    "INTERNET_CHN",
    "DAOGOU_CHN",
    "B2B_CHN",
    "INTERNET_JPN",
    "INTERNET_ENG",
    "CHAR",
    "MAX",
};

int ObWordSegment::init(const ObString& tokenizer)
{
  int ret = OB_SUCCESS;
  bool find = false;
  int64_t i = 0;
  for (; OB_SUCC(ret) && !find && i < WS_MAX + 1; ++i) {
    if (0 == tokenizer.case_compare(TokenizerName[i])) {
      find = true;
    }
  }
  return ret;
}

int ObWordSegment::segment(const ObString& string, ObIArray<ObString>& words)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Word Segmenter has not been inited", K(segmenter_), K(ret));
  } else {
    ret = segmenter_->segment(string, words);
  }
  return ret;
}

int ObWordSegment::segment(const ObObj& string, ObIArray<ObObj>& words)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Word Segmenter has not been inited", K(segmenter_), K(ret));
  } else {
    ret = segmenter_->segment(string, words);
  }
  return ret;
}

int ObWordSegment::reset()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Word Segmenter has not been inited", K(segmenter_), K(ret));
  } else {
    segmenter_->reset();
  }
  return ret;
}

int ObWordSegment::destory()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Word Segmenter has not been inited", K(segmenter_), K(ret));
  } else if (OB_FAIL(segmenter_->destory())) {
    LOG_ERROR("failed to distory segmenter", K(segmenter_), K(ret));
  } else {
    segmenter_->~ObIWordSegmentImpl();
    segmenter_ = NULL;
  }
  return ret;
}

}  // namespace common
}  // namespace oceanbase
