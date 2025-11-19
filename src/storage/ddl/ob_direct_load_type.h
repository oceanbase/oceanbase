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

#ifndef OB_STORAGE_DDL_DIRECT_LOAD_TYPE_H
#define OB_STORAGE_DDL_DIRECT_LOAD_TYPE_H

#include "common/ob_version_def.h"

namespace oceanbase
{
namespace storage
{

enum ObDirectLoadType {
  DIRECT_LOAD_INVALID = 0,
  DIRECT_LOAD_DDL = 1,
  DIRECT_LOAD_LOAD_DATA = 2,
  DIRECT_LOAD_INCREMENTAL = 3,
  DIRECT_LOAD_DDL_V2 = 4,
  DIRECT_LOAD_LOAD_DATA_V2 = 5,
  SN_IDEM_DIRECT_LOAD_DDL = 6,
  SN_IDEM_DIRECT_LOAD_DATA = 7,
  SS_IDEM_DIRECT_LOAD_DDL = 8,
  SS_IDEM_DIRECT_LOAD_DATA = 9,
  DIRECT_LOAD_INCREMENTAL_MAJOR = 10,
  DIRECT_LOAD_MAX
};
/* TODO@zhuoran.zzr wait to set as newest master version*/
static int64_t DDL_IDEM_DATA_FORMAT_VERSION = DATA_VERSION_4_5_0_0;
static int64_t DDL_TABLET_BUCKET_NUM = 1007;
static int64_t DDL_SLICE_BUCKET_NUM = 1007;
static inline bool is_complete_logic(const ObDirectLoadType &type)
{
  return ObDirectLoadType::SN_IDEM_DIRECT_LOAD_DDL == type ||
         ObDirectLoadType::SN_IDEM_DIRECT_LOAD_DATA == type;
}
static inline bool is_valid_direct_load(const ObDirectLoadType &type)
{
  return ObDirectLoadType::DIRECT_LOAD_INVALID < type && ObDirectLoadType::DIRECT_LOAD_MAX > type;
}

static inline bool is_ddl_direct_load(const ObDirectLoadType &type)
{
  return ObDirectLoadType::DIRECT_LOAD_DDL == type ||
         ObDirectLoadType::DIRECT_LOAD_DDL_V2 == type ||
         SN_IDEM_DIRECT_LOAD_DDL == type ||
         SS_IDEM_DIRECT_LOAD_DDL == type;
}

static inline bool is_full_direct_load(const ObDirectLoadType &type)
{
  return ObDirectLoadType::DIRECT_LOAD_DDL == type
      || ObDirectLoadType::DIRECT_LOAD_LOAD_DATA == type
      || ObDirectLoadType::DIRECT_LOAD_DDL_V2 == type
      || ObDirectLoadType::DIRECT_LOAD_LOAD_DATA_V2 == type
      || ObDirectLoadType::SN_IDEM_DIRECT_LOAD_DDL == type
      || ObDirectLoadType::SN_IDEM_DIRECT_LOAD_DATA == type
      || ObDirectLoadType::SS_IDEM_DIRECT_LOAD_DDL == type
      || ObDirectLoadType::SS_IDEM_DIRECT_LOAD_DATA == type;
}

static inline bool is_data_direct_load(const ObDirectLoadType &type)
{
  return ObDirectLoadType::DIRECT_LOAD_LOAD_DATA == type
      || ObDirectLoadType::DIRECT_LOAD_INCREMENTAL == type
      || ObDirectLoadType::DIRECT_LOAD_LOAD_DATA_V2 == type
      || ObDirectLoadType::SN_IDEM_DIRECT_LOAD_DATA == type
      || ObDirectLoadType::SS_IDEM_DIRECT_LOAD_DATA == type
      || ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR == type;
}

static inline bool is_incremental_minor_direct_load(const ObDirectLoadType &type)
{
  return ObDirectLoadType::DIRECT_LOAD_INCREMENTAL == type;
}

static inline bool is_incremental_major_direct_load(const ObDirectLoadType &type)
{
  return ObDirectLoadType::DIRECT_LOAD_INCREMENTAL_MAJOR == type;
}

static inline bool is_incremental_direct_load(const ObDirectLoadType &type)
{
  return is_incremental_minor_direct_load(type)
            || is_incremental_major_direct_load(type);
}

static inline bool is_shared_storage_dempotent_mode(const ObDirectLoadType &type)
{
  return ObDirectLoadType::DIRECT_LOAD_DDL_V2 == type ||
         ObDirectLoadType::DIRECT_LOAD_LOAD_DATA_V2 == type;
}
static inline bool is_idem_type(const ObDirectLoadType &type)
{
  return SN_IDEM_DIRECT_LOAD_DDL == type || SN_IDEM_DIRECT_LOAD_DATA == type ||
         SS_IDEM_DIRECT_LOAD_DDL == type || SS_IDEM_DIRECT_LOAD_DATA == type ||
         DIRECT_LOAD_DDL_V2 == type      || DIRECT_LOAD_LOAD_DATA_V2 == type;
}

static inline bool is_data_version_support_inc_major_direct_load(const uint64_t data_format_version)
{
  return (data_format_version >= DATA_VERSION_4_5_0_0);
}

}  // end namespace storage
}  // end namespace oceanbase
   //
#endif  // OB_STORAGE_DDL_DIRECT_LOAD_TYPE_H
