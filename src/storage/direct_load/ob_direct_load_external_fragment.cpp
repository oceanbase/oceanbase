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

#include "storage/direct_load/ob_direct_load_external_fragment.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

/**
 * ObDirectLoadExternalFragment
 */

ObDirectLoadExternalFragment::ObDirectLoadExternalFragment()
  : file_size_(0), row_count_(0), max_data_block_size_(0)
{
}

ObDirectLoadExternalFragment::~ObDirectLoadExternalFragment()
{
  reset();
}

void ObDirectLoadExternalFragment::reset()
{
  file_handle_.reset();
  file_size_ = 0;
  row_count_ = 0;
  max_data_block_size_ = 0;
}

bool ObDirectLoadExternalFragment::is_valid() const
{
  return file_size_ > 0 && row_count_ > 0 && max_data_block_size_ > 0 && file_handle_.is_valid();
}

int ObDirectLoadExternalFragment::assign(const ObDirectLoadExternalFragment &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(file_handle_.assign(other.file_handle_))) {
    LOG_WARN("fail to assign file handle", KR(ret));
  } else {
    file_size_ = other.file_size_;
    row_count_ = other.row_count_;
    max_data_block_size_ = other.max_data_block_size_;
  }
  return ret;
}

/**
 * ObDirectLoadExternalFragmentArray
 */

ObDirectLoadExternalFragmentArray::ObDirectLoadExternalFragmentArray()
{
}

ObDirectLoadExternalFragmentArray::~ObDirectLoadExternalFragmentArray()
{
  reset();
}

void ObDirectLoadExternalFragmentArray::reset()
{
  fragments_.reset();
}

int ObDirectLoadExternalFragmentArray::assign(const ObDirectLoadExternalFragmentArray &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fragments_.assign(other.fragments_))) {
    LOG_WARN("fail to assign vector", KR(ret));
  }
  return ret;
}

int ObDirectLoadExternalFragmentArray::push_back(const ObDirectLoadExternalFragment &fragment)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fragments_.push_back(fragment))) {
    LOG_WARN("fail to push back fragment", KR(ret));
  }
  return ret;
}

int ObDirectLoadExternalFragmentArray::push_back(const ObDirectLoadExternalFragmentArray &other)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < other.count(); ++i) {
    if (OB_FAIL(fragments_.push_back(other.at(i)))) {
      LOG_WARN("fail to push back fragment", KR(ret));
    }
  }
  return ret;
}

/**
 * ObDirectLoadExternalFragmentCompare
 */

ObDirectLoadExternalFragmentCompare::ObDirectLoadExternalFragmentCompare()
  : result_code_(OB_SUCCESS)
{
}

ObDirectLoadExternalFragmentCompare::~ObDirectLoadExternalFragmentCompare()
{
}

bool ObDirectLoadExternalFragmentCompare::operator()(const ObDirectLoadExternalFragment *lhs,
                                                     const ObDirectLoadExternalFragment *rhs)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (OB_UNLIKELY(nullptr == lhs || nullptr == rhs || !lhs->is_valid() || !rhs->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(lhs), KPC(rhs));
  } else {
    cmp_ret = lhs->row_count_ - rhs->row_count_;
  }
  if (OB_FAIL(ret)) {
    result_code_ = ret;
  }
  return cmp_ret < 0;
}

} // namespace storage
} // namespace oceanbase
