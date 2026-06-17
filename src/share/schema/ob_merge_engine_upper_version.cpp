/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "ob_merge_engine_upper_version.h"
#include "share/ob_cluster_version.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
ObMergeEngineUpperVersion::ObMergeEngineUpperVersion()
  : version_(MERGE_ENGINE_UPPER_VERSION_V1),
    original_merge_engine_type_(ObMergeEngineType::OB_MERGE_ENGINE_MAX),
    upper_versions_()
{
}

void ObMergeEngineUpperVersion::reset()
{
  upper_versions_.reset();
  original_merge_engine_type_ = ObMergeEngineType::OB_MERGE_ENGINE_MAX;
  version_ = MERGE_ENGINE_UPPER_VERSION_V1;
}

int ObMergeEngineUpperVersion::assign(const ObMergeEngineUpperVersion &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(upper_versions_.assign(other.upper_versions_))) {
      LOG_WARN("Fail to assign enable versions", K(ret));
    } else {
      original_merge_engine_type_ = other.original_merge_engine_type_;
      version_ = other.version_;
    }
  }
  return ret;
}

int ObMergeEngineUpperVersion::write_string(ObString &str, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  // TODO: add implementation
  return ret;
}

OB_SERIALIZE_MEMBER(ObMergeEngineUpperVersion,
                    version_,
                    original_merge_engine_type_,
                    upper_versions_)

}
}
}
