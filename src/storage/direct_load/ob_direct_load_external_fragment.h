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
#pragma once

#include "storage/direct_load/ob_direct_load_tmp_file.h"

namespace oceanbase
{
namespace storage
{

struct ObDirectLoadExternalFragment
{
public:
  ObDirectLoadExternalFragment();
  ~ObDirectLoadExternalFragment();
  void reset();
  bool is_valid() const;
  int assign(const ObDirectLoadExternalFragment &fragment);
  TO_STRING_KV(K_(file_size), K_(row_count), K_(max_data_block_size), K_(file_handle));
public:
  int64_t file_size_;
  int64_t row_count_;
  int64_t max_data_block_size_;
  ObDirectLoadTmpFileHandle file_handle_;
};

class ObDirectLoadExternalFragmentArray
{
public:
  ObDirectLoadExternalFragmentArray();
  ~ObDirectLoadExternalFragmentArray();
  void reset();
  int assign(const ObDirectLoadExternalFragmentArray &other);
  int push_back(const ObDirectLoadExternalFragment &fragment);
  int push_back(const ObDirectLoadExternalFragmentArray &other);
  int64_t count() const { return fragments_.count(); }
  bool empty() const { return fragments_.empty(); }
  ObDirectLoadExternalFragment &at(int64_t idx)
  {
    return fragments_.at(idx);
  }
  const ObDirectLoadExternalFragment &at(int64_t idx) const
  {
    return fragments_.at(idx);
  }
  TO_STRING_KV(K_(fragments));
private:
  common::ObArray<ObDirectLoadExternalFragment> fragments_;
};

class ObDirectLoadExternalFragmentCompare
{
public:
  ObDirectLoadExternalFragmentCompare();
  ~ObDirectLoadExternalFragmentCompare();
  bool operator()(const ObDirectLoadExternalFragment *lhs, const ObDirectLoadExternalFragment *rhs);
  int get_error_code() const { return result_code_; }
  int result_code_;
};

} // namespace storage
} // namespace oceanbase
