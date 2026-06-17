/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_MERGE_ENGINE_UPPER_VERSION_H
#define _OB_MERGE_ENGINE_UPPER_VERSION_H

#include "common/ob_store_format.h"
#include "lib/container/ob_se_array.h"
#include "share/scn.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObMergeEngineUpperVersion
{
  OB_UNIS_VERSION(1);
  static constexpr uint8_t MERGE_ENGINE_UPPER_VERSION_V1 = 1;
public:
  ObMergeEngineUpperVersion();
  ~ObMergeEngineUpperVersion() = default;
  void reset();
  int assign(const ObMergeEngineUpperVersion &other);
  int write_string(ObString &str, ObIAllocator &allocator) const;
  inline int64_t get_convert_size() const
  {
    int64_t convert_size = sizeof(*this);
    convert_size += upper_versions_.get_data_size();
    return convert_size;
  }
  TO_STRING_KV(K_(version), K_(original_merge_engine_type), K_(upper_versions));
private:
  uint8_t version_;
  ObMergeEngineType original_merge_engine_type_;
  // the right boundary of row's trans version of each merge engine
  ObSEArray<share::SCN, 4> upper_versions_;
  DISALLOW_COPY_AND_ASSIGN(ObMergeEngineUpperVersion);
};

} // end namespace schema
} // end namespace share
} // end namespace oceanbase

#endif /* _OB_MERGE_ENGINE_UPPER_VERSION_H */
