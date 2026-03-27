/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIBOBCDC_GLOBAL_INFO_H_
#define OCEANBASE_LIBOBCDC_GLOBAL_INFO_H_

#include "ob_cdc_lob_aux_table_schema_info.h"

namespace oceanbase
{
namespace libobcdc
{
class ObCDCGlobalInfo
{
public:
  ObCDCGlobalInfo();
  ~ObCDCGlobalInfo() { reset(); }
  void reset();
  int init();

  OB_INLINE const ObCDCLobAuxTableSchemaInfo &get_lob_aux_table_schema_info() const { return lob_aux_table_schema_info_; }

  OB_INLINE uint64_t get_min_cluster_version() const { return min_cluster_version_; }
  OB_INLINE void update_min_cluster_version(const uint64_t min_cluster_version) { min_cluster_version_ = min_cluster_version; }

private:
  ObCDCLobAuxTableSchemaInfo lob_aux_table_schema_info_;
  uint64_t min_cluster_version_;

  DISALLOW_COPY_AND_ASSIGN(ObCDCGlobalInfo);
};
} // namespace libobcdc
} // namespace oceanbase

#endif
