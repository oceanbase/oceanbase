// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{
class ObDirectLoadTmpFileHandle;

template <typename T>
class ObDirectLoadExternalIterator
{
public:
  virtual ~ObDirectLoadExternalIterator() = default;
  virtual int get_next_item(const T *&item) = 0;
  TO_STRING_EMPTY();
};

template <typename T>
class ObDirectLoadExternalWriter
{
public:
  virtual ~ObDirectLoadExternalWriter() = default;
  virtual int open(const ObDirectLoadTmpFileHandle &file_handle) = 0;
  virtual int write_item(const T &item) = 0;
  virtual int close() = 0;
  TO_STRING_EMPTY();
};

} // namespace storage
} // namespace oceanbase
