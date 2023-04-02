// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

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
