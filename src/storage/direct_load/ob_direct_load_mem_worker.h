// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{

class ObIDirectLoadPartitionTable;

class ObDirectLoadMemWorker
{
public:
  virtual ~ObDirectLoadMemWorker() {}

  virtual int add_table(ObIDirectLoadPartitionTable *table) = 0;
  virtual int work() = 0;

  DECLARE_PURE_VIRTUAL_TO_STRING;
};

} // namespace storage
} // namespace oceanbase
