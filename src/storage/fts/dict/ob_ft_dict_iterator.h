
/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_ITERATOR_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_ITERATOR_H_

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace storage
{
// TOOD: will make it a template with data, for now there's no data
class ObIFTDictIterator
{
public:
  ObIFTDictIterator() {}
  virtual ~ObIFTDictIterator() {}

  // vaild until it returns OB_ITER_END.
  virtual int next() = 0;
  // get key
  virtual int get_key(ObString &str) = 0;
  // get value by template, current no use
  virtual int get_value() = 0;
};

} //  namespace storage
} //  namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_ITERATOR_H_