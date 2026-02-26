/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_OB_I_TOKEN_STREAM_H_
#define OCEANBASE_SHARE_OB_I_TOKEN_STREAM_H_

#include "share/vector/ob_i_vector.h"

namespace oceanbase
{
namespace share
{

// ObITokenStreamData is an abstract class that provides an interface for token stream data.
struct ObITokenStreamData
{
public:
  ObITokenStreamData() = default;
  virtual ~ObITokenStreamData() = 0;

  // @brief: get the vector at the specified index, every implementation pick this relavent vector for processing
  // @todo: add other interface like get_sort_vector, get_aggregate_vector if needed.
  virtual int get_vector(const int64_t idx, ObIVector *&vector) = 0;
private:
  ObArray<ObIVector *> vectors_;
  DISALLOW_COPY_AND_ASSIGN(ObITokenStreamData);
};

// ObITokenStream is an abstract class that provides an interface for batch row handling stream.
// It's designed to conviniently apply different operations on a batch of tokens.
// For example, you can create a pipeline like ObSEArray<ObITokenStream *, 4> analyze_pipeline_; to handle a batch of tokens.
class ObITokenStream
{
public:
  ObITokenStream() = default;
  virtual ~ObITokenStream() = 0;

  // get the next batch of token stream data, the caller is responsible for managing the lifecycle of the returned data object.
  virtual int get_next(ObITokenStreamData *&data) = 0;
private:
  DISALLOW_COPY_AND_ASSIGN(ObITokenStream);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_I_TOKEN_STREAM_H_