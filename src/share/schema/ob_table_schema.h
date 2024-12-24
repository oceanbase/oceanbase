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

#ifndef OCEANBASE_SCHEMA_TABLE_SCHEMA
#define OCEANBASE_SCHEMA_TABLE_SCHEMA

#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <algorithm>
#include "lib/utility/utility.h"
#include "lib/charset/ob_charset.h"
#include "lib/hash/ob_pointer_hashmap.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/objectpool/ob_pool.h"
#include "common/row/ob_row.h"
#include "common/rowkey/ob_rowkey_info.h"
#include "common/object/ob_object.h"
#include "common/ob_range.h"
#include "common/ob_store_format.h"
#include "common/ob_tablet_id.h"
#include "share/ob_define.h"
#include "share/ob_get_compat_mode.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_trigger_info.h"
#include "lib/compress/ob_compress_util.h"

namespace oceanbase
{

namespace storage
{
  class ObStorageSchema;
  struct ObCreateSSTableParamExtraInfo;
}

namespace blocksstable
{
  struct ObSSTableColumnMeta;
  struct ObDatumRow;
}

namespace common
{
  class ObTabletID;
}

namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
class ObColDesc;
class ObConstraint;
class ObColumnSchemaV2;
class ObColumnParam;
struct ObColumnIdKey
{
  uint64_t column_id_;

  explicit ObColumnIdKey() : column_id_(common::OB_INVALID_ID) {}
  explicit ObColumnIdKey(const uint64_t column_id) : column_id_(column_id) {}

  ObColumnIdKey &operator=(const uint64_t column_id)
  {
    column_id_ = column_id;
    return *this;
  }

  inline operator uint64_t() const { return column_id_; }

  inline uint64_t hash() const { return ((column_id_ * 29 + 7) & 0xFFFF); }

  inline int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return common::OB_SUCCESS;
  }
};

struct ObColumnGroupIdKey
{
  uint64_t column_group_id_;

  explicit ObColumnGroupIdKey() : column_group_id_(common::OB_INVALID_ID) {}
  explicit ObColumnGroupIdKey(const uint64_t column_group_id)
    : column_group_id_(column_group_id) {}

  ObColumnGroupIdKey &operator=(const uint64_t column_group_id)
  {
    column_group_id_ = column_group_id;
    return *this;
  }

  inline operator uint64_t() const { return column_group_id_; }

  inline uint64_t hash() const { return ((column_group_id_ * 29 + 7) & 0xFFFF); }
};

template<class K, class V>
struct ObGetColumnKey
{
  void operator()(const K &k, const V &v) const
  {
    UNUSED(k);
    UNUSED(v);
  }
};

template<>
struct ObGetColumnKey<ObColumnIdKey, ObColumnSchemaV2 *>
{
  ObColumnIdKey operator()(const ObColumnSchemaV2 *column_schema) const;
};

template<>
struct ObGetColumnKey<ObColumnSchemaHashWrapper, ObColumnSchemaV2 *>
{
  ObColumnSchemaHashWrapper operator()(const ObColumnSchemaV2 *column_schema) const;
};

template<>
struct ObGetColumnKey<ObColumnGroupIdKey, ObColumnGroupSchema *>
{
  ObColumnGroupIdKey operator()(const ObColumnGroupSchema *column_group_schema) const;
};

template<>
struct ObGetColumnKey<ObColumnGroupSchemaHashWrapper, ObColumnGroupSchema *>
{
  ObColumnGroupSchemaHashWrapper operator()(const ObColumnGroupSchema *column_group_schema) const;
};

typedef common::hash::ObPointerHashArray<ObColumnIdKey, ObColumnSchemaV2 *, ObGetColumnKey>
IdHashArray;
typedef common::hash::ObPointerHashArray<ObColumnSchemaHashWrapper, ObColumnSchemaV2 *, ObGetColumnKey>
NameHashArray;
typedef common::hash::ObPointerHashArray<ObColumnGroupIdKey, ObColumnGroupSchema *, ObGetColumnKey>
CgIdHashArray;
typedef common::hash::ObPointerHashArray<ObColumnGroupSchemaHashWrapper, ObColumnGroupSchema *, ObGetColumnKey>
CgNameHashArray;
typedef const common::ObObj& (ObColumnSchemaV2::*get_default_value)() const;

extern const uint64_t HIDDEN_PK_COLUMN_IDS[3];
extern const char* HIDDEN_PK_COLUMN_NAMES[3];
const uint64_t BORDER_COLUMN_ID = 0;

typedef struct TableJoinType_ {
  std::pair<uint64_t, uint64_t> table_pair_;
  //sql::ObJoinType type_;
  int join_type_;
  TO_STRING_KV(K_(table_pair), K_(join_type));
} TableJoinType;

enum ObTableModeFlag
{
  TABLE_MODE_NORMAL = 0,
  TABLE_MODE_QUEUING = 1,
  TABLE_MODE_PRIMARY_AUX_VP = 2,
  TABLE_MODE_QUEUING_MODERATE = 3,
  TABLE_MODE_QUEUING_SUPER = 4,
  TABLE_MODE_QUEUING_EXTREME = 5,
  TABLE_MODE_MAX,
};

inline bool is_valid_table_mode_flag(const ObTableModeFlag &table_mode)
{
  return TABLE_MODE_NORMAL <= table_mode && table_mode < TABLE_MODE_MAX;
}
inline bool is_new_queuing_mode(const ObTableModeFlag &table_mode)
{
  return TABLE_MODE_QUEUING_MODERATE <= table_mode && table_mode <= TABLE_MODE_QUEUING_EXTREME;
}
inline bool is_queuing_table_mode(const ObTableModeFlag &table_mode)
{
  return TABLE_MODE_QUEUING ==  table_mode || is_new_queuing_mode(table_mode);
}
inline bool not_compat_for_queuing_mode_42x(const uint64_t min_data_version)
{
  return (min_data_version < MOCK_DATA_VERSION_4_2_1_5)
        || (DATA_VERSION_4_2_2_0 <= min_data_version && min_data_version < MOCK_DATA_VERSION_4_2_3_0);
}
inline bool not_compat_for_queuing_mode(const uint64_t min_data_version)
{
  return not_compat_for_queuing_mode_42x(min_data_version)
      || (DATA_VERSION_4_3_0_0 <= min_data_version && min_data_version < DATA_VERSION_4_3_2_0);
}
const char *table_mode_flag_to_str(const ObTableModeFlag &table_mode);
#define QUEUING_MODE_NOT_COMPAT_WARN_STR "moderate/super/extreme table mode is not supported in data_version < 4.2.1.5 or 4.2.2 <= data_version < 4.2.3 or 4.3.0 <= data_version < 4.3.2"
#define QUEUING_MODE_NOT_COMPAT_USER_ERROR_STR            "moderate/super/extreme table mode in data_version < 4.2.1.5 or 4.2.2 <= data_version < 4.2.3 or 4.3.0 <= data_version < 4.3.2"

enum ObTablePKMode
{
  TPKM_OLD_NO_PK= 0,
  TPKM_NEW_NO_PK = 1,
  TPKM_TABLET_SEQ_PK = 2,
  TPKM_MAX,
};

enum ObTableStateFlag
{
  TABLE_STATE_NORMAL = 0, // normal table
  TABLE_STATE_OFFLINE_DDL = 1, // the table is in offline ddl
  TABLE_STATE_HIDDEN = 2, // the table is hidden
  TABLE_STATE_HIDDEN_OFFLINE_DDL = 3, // the table is hidden and in offline ddl
  TABLE_STATE_MAX = 4,
};

enum ObTableStateBitMask
{
  TABLE_STATE_INVALID_MASK = 0,
  TABLE_STATE_IS_DDL_MASK = 1,
  TABLE_STATE_IS_HIDDEN_MASK = 2,
  TABLE_STATE_MAX_MASK,
};

enum ObTableOrganizationMode
{
  TOM_INDEX_ORGANIZED = 0,
  TOM_HEAP_ORGANIZED = 1,
  TOM_MAX,
};

enum ObViewCreatedMethodFlag
{
  VIEW_CREATED_BY_NORMAL = 0, // view created by 'create view'
  VIEW_CREATED_BY_OR_REPLACE = 1, // view created by 'create or replace view' or 'create force view' or 'create or replace force view'
  VIEW_CREATED_METHOD_MAX = 2,
};

enum ObTableAutoIncrementMode
{
  ORDER = 0,
  NOORDER = 1,
};

enum ObTableRowidMode
{
  ROWID_NORMAL = 0,
  ROWID_EXTENDED = 1,
};

enum ObViewColumnFilledFlag
{
  NOT_FILLED = 0,
  FILLED = 1,
};

enum ObMVContainerTableFlag
{
  IS_NOT_MV_CONTAINER_TABLE = 0,
  IS_MV_CONTAINER_TABLE = 1,
};

enum ObMVAvailableFlag
{
  IS_MV_UNAVAILABLE = 0,
  IS_MV_AVAILABLE = 1,
};

enum ObTableReferencedByMVFlag
{
  IS_NOT_REFERENCED_BY_MV = 0,
  IS_REFERENCED_BY_MV = 1,
};

enum ObMVEnableQueryRewriteFlag
{
  IS_MV_DISABLE_QUERY_REWRITE = 0,
  IS_MV_ENABLE_QUERY_REWRITE = 1,
};

enum ObMVOnQueryComputationFlag
{
  IS_NOT_MV_ON_QUERY_COMPUTATION = 0,
  IS_MV_ON_QUERY_COMPUTATION = 1,
};

enum ObDDLIgnoreSyncCdcFlag
{
  DO_SYNC_LOG_FOR_CDC = 0,
  DONT_SYNC_LOG_FOR_CDC = 1,
};

enum ObMVMajorRefreshFlag
{
  IS_NOT_MV_MAJOR_REFRESH = 0,
  IS_MV_MAJOR_REFRESH = 1
};

enum ObTableReferencedByFastLSMMVFlag
{
  IS_NOT_REFERENCED_BY_FAST_LSM_MV = 0,
  IS_REFERENCED_BY_FAST_LSM_MV = 1
};

struct ObTableMode {
  OB_UNIS_VERSION_V(1);
private:
  static const int32_t TM_MODE_FLAG_OFFSET = 0;
  static const int32_t TM_MODE_FLAG_BITS = 8;
  static const int32_t TM_PK_MODE_OFFSET = 8;
  static const int32_t TM_PK_MODE_BITS = 4;
  static const int32_t TM_TABLE_STATE_FLAG_OFFSET = 12;
  static const int32_t TM_TABLE_STATE_FLAG_BITS = 4;
  static const int32_t TM_TABLE_ORGANIZATION_MODE_OFFSET = 16;
  static const int32_t TM_TABLE_ORGANIZATION_MODE_BITS = 1;
  static const int32_t TM_VIEW_CREATED_METHOD_FLAG_OFFSET = 17;
  static const int32_t TM_VIEW_CREATED_METHOD_FLAG_BITS = 4;
  static const int32_t TM_TABLE_AUTO_INCREMENT_MODE_OFFSET = 21;
  static const int32_t TM_TABLE_AUTO_INCREMENT_MODE_BITS = 1;
  static const int32_t TM_TABLE_ROWID_MODE_OFFSET = 22;
  static const int32_t TM_TABLE_ROWID_MODE_BITS = 1;
  static const int32_t TM_VIEW_COLUMN_FILLED_OFFSET = 23;
  static const int32_t TM_VIEW_COLUMN_FILLED_BITS = 1;
  static const int32_t TM_MV_CONTAINER_TABLE_OFFSET = 24;
  static const int32_t TM_MV_CONTAINER_TABLE_BITS = 1;
  static const int32_t TM_MV_AVAILABLE_OFFSET = 25;
  static const int32_t TM_MV_AVAILABLE_BITS = 1;
  static const int32_t TM_TABLE_REFERENCED_BY_MV_OFFSET = 26;
  static const int32_t TM_TABLE_REFERENCED_BY_MV_BITS = 1;
  static const int32_t TM_MV_ENABLE_QUERY_REWRITE_OFFSET = 27;
  static const int32_t TM_MV_ENABLE_QUERY_REWRITE_BITS = 1;
  static const int32_t TM_MV_ON_QUERY_COMPUTATION_OFFSET = 28;
  static const int32_t TM_MV_ON_QUERY_COMPUTATION_BITS = 1;
  static const int32_t TM_DDL_IGNORE_SYNC_CDC_OFFSET = 29;
  static const int32_t TM_DDL_IGNORE_SYNC_CDC_BITS = 1;
  static const int32_t TM_RESERVED = 2;

  static const uint32_t MODE_FLAG_MASK = (1U << TM_MODE_FLAG_BITS) - 1;
  static const uint32_t PK_MODE_MASK = (1U << TM_PK_MODE_BITS) - 1;
  static const uint32_t STATE_FLAG_MASK = (1U << TM_TABLE_STATE_FLAG_BITS) - 1;
  static const uint32_t ORGANIZATION_MODE_MASK = (1U << TM_TABLE_ORGANIZATION_MODE_BITS) - 1;
  static const uint32_t VIEW_CREATED_METHOD_FLAG_MASK = (1U << TM_TABLE_STATE_FLAG_BITS) - 1;
  static const uint32_t AUTO_INCREMENT_MODE_MASK = (1U << TM_TABLE_AUTO_INCREMENT_MODE_BITS) - 1;
  static const uint32_t ROWID_MODE_MASK = (1U << TM_TABLE_ROWID_MODE_BITS) - 1;
  static const uint32_t VIEW_COLUMN_FILLED_MASK = (1U << TM_VIEW_COLUMN_FILLED_BITS) - 1;
  static const uint32_t MV_CONTAINER_TABLE_MASK = (1U << TM_MV_CONTAINER_TABLE_BITS) - 1;
  static const uint32_t MV_AVAILABLE_MASK = (1U << TM_MV_AVAILABLE_BITS) - 1;
  static const uint32_t TABLE_REFERENCED_BY_MV_MASK = (1U << TM_TABLE_REFERENCED_BY_MV_BITS) - 1;
  static const uint32_t MV_ENABLE_QUERY_REWRITE_MASK = (1U << TM_MV_ENABLE_QUERY_REWRITE_BITS) - 1;
  static const uint32_t MV_ON_QUERY_COMPUTATION_MASK = (1U << TM_MV_ON_QUERY_COMPUTATION_BITS) - 1;
  static const uint32_t DDL_IGNORE_SYNC_CDC_MASK = (1U << TM_DDL_IGNORE_SYNC_CDC_BITS) - 1;
public:
  ObTableMode() { reset(); }
  virtual ~ObTableMode() { reset(); }
  void reset() { mode_ = 0; }
  bool operator ==(const ObTableMode &other) const
  {
    return mode_ == other.mode_;
  }
  int assign(const ObTableMode &other);
  ObTableMode &operator=(const ObTableMode &other);
  bool is_valid() const;

  static ObTableModeFlag get_table_mode_flag(int32_t table_mode)
  {
    return (ObTableModeFlag)(table_mode & MODE_FLAG_MASK);
  }
  static ObTablePKMode get_table_pk_mode(int32_t table_mode)
  {
    return (ObTablePKMode)((table_mode >> TM_PK_MODE_OFFSET) & PK_MODE_MASK);
  }
  static ObTableStateFlag get_table_state_flag(int32_t table_mode)
  {
    return (ObTableStateFlag)((table_mode >> TM_TABLE_STATE_FLAG_OFFSET) & STATE_FLAG_MASK);
  }
  static ObTableOrganizationMode get_table_organization_flag(int32_t table_mode)
  {
    return (ObTableOrganizationMode)((table_mode >> TM_TABLE_ORGANIZATION_MODE_OFFSET) & ORGANIZATION_MODE_MASK);
  }
  static ObViewCreatedMethodFlag get_view_created_method_flag(int32_t table_mode)
  {
    return (ObViewCreatedMethodFlag)((table_mode >> TM_VIEW_CREATED_METHOD_FLAG_OFFSET) & VIEW_CREATED_METHOD_FLAG_MASK);
  }
  static ObTableAutoIncrementMode get_auto_increment_mode(int32_t table_mode)
  {
    return (ObTableAutoIncrementMode)((table_mode >> TM_TABLE_AUTO_INCREMENT_MODE_OFFSET) & AUTO_INCREMENT_MODE_MASK);
  }
  static ObTableRowidMode get_rowid_mode(int32_t table_mode)
  {
    return (ObTableRowidMode)((table_mode >> TM_TABLE_ROWID_MODE_OFFSET) & ROWID_MODE_MASK);
  }
  static ObViewColumnFilledFlag get_view_column_filled_flag(int32_t table_mode)
  {
    return (ObViewColumnFilledFlag)((table_mode >> TM_VIEW_COLUMN_FILLED_OFFSET) & VIEW_COLUMN_FILLED_MASK);
  }
  static ObMVContainerTableFlag get_mv_container_table_flag(int32_t table_mode)
  {
    return (ObMVContainerTableFlag)((table_mode >> TM_MV_CONTAINER_TABLE_OFFSET) & MV_CONTAINER_TABLE_MASK);
  }
  static ObMVAvailableFlag get_mv_available_flag(int32_t table_mode)
  {
    return (ObMVAvailableFlag)((table_mode >> TM_MV_AVAILABLE_OFFSET) & MV_AVAILABLE_MASK);
  }
  static ObTableReferencedByMVFlag get_table_referenced_by_mv_flag(int32_t table_mode)
  {
      return (ObTableReferencedByMVFlag)((table_mode >> TM_TABLE_REFERENCED_BY_MV_OFFSET) & TABLE_REFERENCED_BY_MV_MASK);
  }
  static ObMVEnableQueryRewriteFlag get_mv_enable_query_rewrite_flag(int32_t table_mode)
  {
      return (ObMVEnableQueryRewriteFlag)((table_mode >> TM_MV_ENABLE_QUERY_REWRITE_OFFSET) & MV_ENABLE_QUERY_REWRITE_MASK);
  }
  static ObMVOnQueryComputationFlag get_mv_on_query_computation_flag(int32_t table_mode)
  {
      return (ObMVOnQueryComputationFlag)((table_mode >> TM_MV_ON_QUERY_COMPUTATION_OFFSET) & MV_ON_QUERY_COMPUTATION_MASK);
  }
  inline bool is_user_hidden_table() const
  { return TABLE_STATE_IS_HIDDEN_MASK & state_flag_; }
  TO_STRING_KV("table_mode_flag", mode_flag_,
               "pk_mode", pk_mode_,
               "table_state_flag", state_flag_,
               "view_created_method_flag", view_created_method_flag_,
               "table_organization_mode", organization_mode_,
               "auto_increment_mode", auto_increment_mode_,
               "rowid_mode", rowid_mode_,
               "view_column_filled_flag", view_column_filled_flag_,
               "mv_container_table_flag", mv_container_table_flag_,
               "mv_available_flag", mv_available_flag_,
               "table_referenced_by_mv_flag", table_referenced_by_mv_flag_,
               "mv_enable_query_rewrite_flag", mv_enable_query_rewrite_flag_,
               "mv_on_query_computation_flag", mv_on_query_computation_flag_,
               "ddl_table_ignore_sync_cdc_flag", ddl_table_ignore_sync_cdc_flag_);
  union {
    int32_t mode_;
    struct {
      uint32_t mode_flag_ :TM_MODE_FLAG_BITS;
      uint32_t pk_mode_ :TM_PK_MODE_BITS;
      uint32_t state_flag_ :TM_TABLE_STATE_FLAG_BITS;
      uint32_t organization_mode_: TM_TABLE_ORGANIZATION_MODE_BITS;
      uint32_t view_created_method_flag_ :TM_VIEW_CREATED_METHOD_FLAG_BITS;
      uint32_t auto_increment_mode_: TM_TABLE_AUTO_INCREMENT_MODE_BITS;
      uint32_t rowid_mode_: TM_TABLE_ROWID_MODE_BITS;
      uint32_t view_column_filled_flag_ : TM_VIEW_COLUMN_FILLED_BITS;
      uint32_t mv_container_table_flag_ : TM_MV_CONTAINER_TABLE_BITS;
      uint32_t mv_available_flag_ : TM_MV_AVAILABLE_BITS;
      uint32_t table_referenced_by_mv_flag_ : TM_TABLE_REFERENCED_BY_MV_BITS;
      uint32_t mv_enable_query_rewrite_flag_ : TM_MV_ENABLE_QUERY_REWRITE_BITS;
      uint32_t mv_on_query_computation_flag_ : TM_MV_ON_QUERY_COMPUTATION_BITS;
      uint32_t ddl_table_ignore_sync_cdc_flag_ : TM_DDL_IGNORE_SYNC_CDC_BITS;
      uint32_t reserved_ : TM_RESERVED;
    };
  };
};

struct ObMvMode {
  OB_UNIS_VERSION_V(1);
private:
  static const int32_t MM_MV_MAJOR_REFRESH_OFFSET = 0;
  static const int32_t MM_MV_MAJOR_REFRESH_BITS = 1;
  static const uint32_t MM_MV_MAJOR_REFRESH_MASK = (1U << MM_MV_MAJOR_REFRESH_BITS) - 1;
  static const int32_t MM_TABLE_REFERENCED_BY_FAST_LSM_MV_OFFSET = 1;
  static const int32_t MM_TABLE_REFERENCED_BY_FAST_LSM_MV_BITS = 1;
  static const uint32_t MM_TABLE_REFERENCED_BY_FAST_LSM_MV_MASK =
      (1U << MM_TABLE_REFERENCED_BY_FAST_LSM_MV_BITS) - 1;
  static const int32_t MM_RESERVED = 62;
public:
  ObMvMode() { reset(); }
  virtual ~ObMvMode() { reset(); }
  void reset() { mode_ = 0; }
  bool operator==(const ObMvMode &other) const { return mode_ == other.mode_; }
  int assign(const ObMvMode &other);
  ObMvMode &operator=(const ObMvMode &other);
  bool is_valid() const;
  static ObMVMajorRefreshFlag get_mv_major_refresh_flag(int64_t mv_mode)
  {
    return (ObMVMajorRefreshFlag)((mv_mode >> MM_MV_MAJOR_REFRESH_OFFSET) &
                                  MM_MV_MAJOR_REFRESH_MASK);
  }
  static ObTableReferencedByFastLSMMVFlag get_table_referenced_by_fast_lsm_mv_flag(int64_t mv_mode)
  {
    return (ObTableReferencedByFastLSMMVFlag)(
        (mv_mode >> MM_TABLE_REFERENCED_BY_FAST_LSM_MV_OFFSET) &
        MM_TABLE_REFERENCED_BY_FAST_LSM_MV_MASK);
  }
  union
  {
    int64_t mode_;
    struct
    {
      uint64_t mv_major_refresh_flag_ : MM_MV_MAJOR_REFRESH_BITS;
      uint64_t table_referenced_by_fast_lsm_mv_flag_ : MM_TABLE_REFERENCED_BY_FAST_LSM_MV_BITS;
      uint64_t reserved_ : MM_RESERVED;
    };
  };
  TO_STRING_KV("mv_major_refresh_flag", mv_major_refresh_flag_,
               "table_referenced_by_fast_lsm_mv_flag", table_referenced_by_fast_lsm_mv_flag_);
};

struct ObBackUpTableModeOp
{
  /*
      "NEW_NO_PK_MODE":TPKM_NEW_NO_PK
      "HEAP_ORGANIZED_TABLE":TOM_HEAP_ORGANIZED
      "INDEX_ORGANIZED_TABLE":TOM_INDEX_ORGANIZED
      "QUEUING":TABLE_MODE_QUEUING
      "QUEUING|NEW_NO_PK_MODE": TABLE_MODE_QUEUING && TPKM_NEW_NO_PK
      "QUEUING|HEAP_ORGANIZED_TABLE":TABLE_MODE_QUEUING && TOM_HEAP_ORGANIZED
      "QUEUING|INDEX_ORGANIZED_TABLE":TABLE_MODE_QUEUING && TOM_INDEX_ORGANIZED
      "MODERATE":TABLE_MODE_QUEUING_MODERATE
      "MODERATE|NEW_NO_PK_MODE": TABLE_MODE_QUEUING_MODERATE && TPKM_NEW_NO_PK
      "MODERATE|HEAP_ORGANIZED_TABLE":TABLE_MODE_QUEUING_MODERATE && TOM_HEAP_ORGANIZED
      "MODERATE|INDEX_ORGANIZED_TABLE":TABLE_MODE_QUEUING_MODERATE && TOM_INDEX_ORGANIZED
      "SUPER":TABLE_MODE_QUEUING_SUPER
      "SUPER|NEW_NO_PK_MODE": TABLE_MODE_QUEUING_SUPER && TPKM_NEW_NO_PK
      "SUPER|HEAP_ORGANIZED_TABLE":TABLE_MODE_QUEUING_SUPER && TOM_HEAP_ORGANIZED
      "SUPER|INDEX_ORGANIZED_TABLE":TABLE_MODE_QUEUING_SUPER && TOM_INDEX_ORGANIZED
      "EXTREME":TABLE_MODE_QUEUING_EXTREME
      "EXTREME|NEW_NO_PK_MODE": TABLE_MODE_QUEUING_EXTREME && TPKM_NEW_NO_PK
      "EXTREME|HEAP_ORGANIZED_TABLE":TABLE_MODE_QUEUING_EXTREME && TOM_HEAP_ORGANIZED
      "EXTREME|INDEX_ORGANIZED_TABLE":TABLE_MODE_QUEUING_EXTREME && TOM_INDEX_ORGANIZED
  */
  #define SET_QUEUING_TABLE_MODE_WITH_OTHER_MODE(mode, queuing_mode_str) \
    if (TPKM_NEW_NO_PK == mode.pk_mode_) {                               \
      ret_str = queuing_mode_str"|NEW_NO_PK_MODE";                       \
    } else if (TOM_HEAP_ORGANIZED == mode.organization_mode_) {          \
      ret_str = queuing_mode_str"|HEAP_ORGANIZED_TABLE";                 \
    } else if (TOM_INDEX_ORGANIZED == mode.organization_mode_) {         \
      ret_str = queuing_mode_str"|INDEX_ORGANIZED_TABLE";                \
    } else {                                                             \
      ret_str = queuing_mode_str;                                        \
    }

  static common::ObString get_table_mode_str(const ObTableMode mode) {
    common::ObString ret_str = "";
    const ObTableModeFlag flag = static_cast<ObTableModeFlag>(mode.mode_flag_);
    if (is_queuing_table_mode(flag)) {
      if (TABLE_MODE_QUEUING == mode.mode_flag_) {
        SET_QUEUING_TABLE_MODE_WITH_OTHER_MODE(mode, "QUEUING");
      } else if (TABLE_MODE_QUEUING_MODERATE == mode.mode_flag_) {
        SET_QUEUING_TABLE_MODE_WITH_OTHER_MODE(mode, "MODERATE");
      } else if (TABLE_MODE_QUEUING_SUPER == mode.mode_flag_) {
        SET_QUEUING_TABLE_MODE_WITH_OTHER_MODE(mode, "SUPER");
      } else if (TABLE_MODE_QUEUING_EXTREME == mode.mode_flag_) {
        SET_QUEUING_TABLE_MODE_WITH_OTHER_MODE(mode, "EXTREME");
      }
    } else if (TPKM_NEW_NO_PK == mode.pk_mode_) {
      ret_str = "NEW_NO_PK_MODE";
    } else if (TOM_HEAP_ORGANIZED == mode.organization_mode_) {
      ret_str = "HEAP_ORGANIZED_TABLE";
    } else if (TOM_INDEX_ORGANIZED == mode.organization_mode_) {
      ret_str = "INDEX_ORGANIZED_TABLE";
    }
    return ret_str;
  }

  static int get_table_mode(const common::ObString str, ObTableMode &ret_mode, uint64_t tenant_data_version) {
    int ret = common::OB_SUCCESS;
    ret_mode.reset();
    char * flag = nullptr;
    const char *delim = "|";
    char *save_ptr = NULL;
    char table_mode_str[str.length() + 1] ;
    MEMSET(table_mode_str, '\0', str.length() + 1);
    std::strncpy(table_mode_str, str.ptr(), str.length());
    flag = strtok_r(table_mode_str, delim, &save_ptr);
    while (OB_SUCC(ret) && OB_NOT_NULL(flag))
    {
      common::ObString flag_str(0, static_cast<int32_t>(strlen(flag)), flag);
       if (0 == flag_str.case_compare("normal")) {
         // do nothing
       } else if (0 == flag_str.case_compare("queuing")) {
         ret_mode.mode_flag_ = TABLE_MODE_QUEUING;
       } else if (0 == flag_str.case_compare("moderate")) {
         ret_mode.mode_flag_ = TABLE_MODE_QUEUING_MODERATE;
       } else if (0 == flag_str.case_compare("super")) {
         ret_mode.mode_flag_ = TABLE_MODE_QUEUING_SUPER;
       } else if (0 == flag_str.case_compare("extreme")) {
         ret_mode.mode_flag_ = TABLE_MODE_QUEUING_EXTREME;
       } else if (0 == flag_str.case_compare("new_no_pk_mode")) {
         ret_mode.pk_mode_ = TPKM_NEW_NO_PK;
       } else if (0 == flag_str.case_compare("heap_organized_table")) {
         ret_mode.organization_mode_ = TOM_HEAP_ORGANIZED;
         ret_mode.pk_mode_ = TPKM_TABLET_SEQ_PK;
       } else if (0 == flag_str.case_compare("index_organized_table")) {
         ret_mode.organization_mode_ = TOM_INDEX_ORGANIZED;
       } else {
         ret = common::OB_ERR_PARSER_SYNTAX;
       }
       if (OB_SUCC(ret) && not_compat_for_queuing_mode(tenant_data_version) && is_new_queuing_mode(static_cast<ObTableModeFlag>(ret_mode.mode_flag_))) {
        ret = OB_NOT_SUPPORTED;
        SHARE_SCHEMA_LOG(WARN, QUEUING_MODE_NOT_COMPAT_WARN_STR, K(ret), K(flag_str), K(tenant_data_version));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, QUEUING_MODE_NOT_COMPAT_USER_ERROR_STR);
       }
       flag = strtok_r(NULL, delim, &save_ptr);
    }
    return ret;
  }
};

// add virtual function in ObMergeSchema, should edit ObStorageSchema & ObTableSchema
class ObMergeSchema
{
public:
  ObMergeSchema() {}
  virtual ~ObMergeSchema() {}
  virtual bool is_valid() const = 0;

  /* merge related function*/
  virtual inline uint64_t get_tenant_id() const { return OB_INVALID_ID; }
  virtual inline int64_t get_tablet_size() const { return INVAID_RET; }
  virtual inline int64_t get_rowkey_column_num() const { return INVAID_RET; }
  virtual inline int64_t get_column_count() const { return INVAID_RET; }
  virtual inline int64_t get_schema_version() const { return INVAID_RET; }
  virtual inline int64_t get_pctfree() const { return INVAID_RET; }
  virtual inline uint64_t get_master_key_id() const { return OB_INVALID_ID; }
  virtual inline bool is_use_bloomfilter() const { return false; }
  virtual inline bool is_primary_aux_vp_table() const { return false; }
  virtual inline bool is_primary_vp_table() const { return false; }
  virtual inline bool is_aux_vp_table() const { return false; }
  virtual inline bool is_column_info_simplified() const { return false; }
  virtual inline bool is_storage_index_table() const = 0;
  virtual inline int64_t get_block_size() const { return INVAID_RET;}
  virtual inline const common::ObString &get_encrypt_key() const { return EMPTY_STRING; }
  virtual inline const char *get_encrypt_key_str() const = 0;
  virtual inline int64_t get_encrypt_key_len() const { return INVAID_RET; }
  virtual int get_encryption_id(int64_t &encrypt_id) const = 0;
  virtual const common::ObString &get_encryption_str() const = 0;
  virtual bool need_encrypt() const = 0;
  virtual inline bool is_global_index_table() const = 0;
  virtual inline common::ObRowStoreType get_row_store_type() const { return common::MAX_ROW_STORE; }
  virtual inline const char *get_compress_func_name() const { return all_compressor_name[ObCompressorType::NONE_COMPRESSOR]; }
  virtual inline common::ObCompressorType get_compressor_type() const { return ObCompressorType::NONE_COMPRESSOR; }
  virtual inline int64_t get_progressive_merge_round() const { return INVAID_RET; }
  virtual inline int64_t get_progressive_merge_num() const { return INVAID_RET; }
  virtual inline ObTableModeFlag get_table_mode_flag() const { return TABLE_MODE_MAX; }
  virtual inline ObTableType get_table_type() const { return MAX_TABLE_TYPE; }
  virtual inline ObTableMode get_table_mode_struct() const = 0;
  virtual inline ObIndexType get_index_type() const { return INDEX_TYPE_MAX; }
  virtual inline ObIndexStatus get_index_status() const { return INDEX_STATUS_MAX; }
  virtual inline bool is_index_table() const = 0;

  virtual int get_store_column_ids(common::ObIArray<ObColDesc> &column_ids, const bool full_col) const
  {
    UNUSED(column_ids);
    UNUSED(full_col);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int get_aux_vp_tid_array(common::ObIArray<uint64_t> &aux_vp_tid_array) const
  {
    UNUSED(aux_vp_tid_array);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int has_lob_column(bool &has_lob, const bool check_large = false) const
  {
    UNUSED(has_lob);
    UNUSED(check_large);
    return common::OB_NOT_SUPPORTED;
  }

  virtual int get_store_column_count(int64_t &column_count, const bool full_col) const
  {
    UNUSED(column_count);
    UNUSED(full_col);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int get_column_ids(common::ObIArray<share::schema::ObColDesc> &column_ids, bool no_virtual) const
  {
    UNUSED(column_ids);
    UNUSED(no_virtual);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int get_column_ids(common::ObIArray<uint64_t> &column_ids) const
  {
    UNUSED(column_ids);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int get_rowkey_column_ids(common::ObIArray<share::schema::ObColDesc> &column_ids) const
  {
    UNUSED(column_ids);
    return common::OB_NOT_SUPPORTED;
  }
  int get_mulit_version_rowkey_column_ids(common::ObIArray<share::schema::ObColDesc> &column_ids) const;
  virtual int set_precision_to_column_desc(common::ObIArray<share::schema::ObColDesc> &column_ids) const
  {
    UNUSED(column_ids);
    return common::OB_SUCCESS;
  }
  virtual int get_column_encodings(common::ObIArray<int64_t> &col_encodings) const
  {
    UNUSED(col_encodings);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int init_column_meta_array(
      common::ObIArray<blocksstable::ObSSTableColumnMeta> &meta_array) const
  {
    UNUSED(meta_array);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int get_multi_version_column_descs(common::ObIArray<share::schema::ObColDesc> &column_descs) const
  {
    UNUSED(column_descs);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int get_skip_index_col_attr(common::ObIArray<ObSkipIndexColumnAttr> &skip_idx_attrs) const
  {
    UNUSED(skip_idx_attrs);
    return common::OB_NOT_SUPPORTED;
  }
  virtual int get_mv_mode_struct(ObMvMode &mv_mode) const
  {
    UNUSED(mv_mode);
    return common::OB_NOT_SUPPORTED;
  }
  DECLARE_PURE_VIRTUAL_TO_STRING;
  const static int64_t INVAID_RET = -1;
  static common::ObString EMPTY_STRING;
};

/*TODO: Delete the following interfaces
int ObSimpleTableSchemaV2::get_zone_list(）
int ObSimpleTableSchemaV2::get_first_primary_zone_inherit()
int ObSimpleTableSchemaV2::get_paxos_replica_num()
int ObSimpleTableSchemaV2::get_zone_replica_attr_array_inherit()
int ObSimpleTableSchemaV2::get_primary_zone_inherit()
int ObSimpleTableSchemaV2::get_full_replica_num()
int ObSimpleTableSchemaV2::get_all_replica_num()
int ObSimpleTableSchemaV2::check_has_all_server_readonly_replica()
int ObSimpleTableSchemaV2::check_is_readonly_at_all()
int ObSimpleTableSchemaV2::check_is_all_server_readonly_replica()
int ObSimpleTableSchemaV2::get_locality_str_inherit()
*/
class ObSimpleTableSchemaV2 : public ObPartitionSchema, public ObMergeSchema
{
public:
  ObSimpleTableSchemaV2();
  explicit ObSimpleTableSchemaV2(common::ObIAllocator *allocator);
  ObSimpleTableSchemaV2(const ObSimpleTableSchemaV2 &src_schema) = delete;
  virtual ~ObSimpleTableSchemaV2();
  ObSimpleTableSchemaV2 &operator=(const ObSimpleTableSchemaV2 &other) = delete;
  int assign(const ObSimpleTableSchemaV2 &src_schema);
  bool operator ==(const ObSimpleTableSchemaV2 &other) const;
  void reset();
  virtual void reset_partition_schema();
  bool is_valid() const;
  bool is_link_valid() const;
  int64_t get_convert_size() const;
  inline void set_tenant_id(const uint64_t tenant_id) override { tenant_id_ = tenant_id; }
  inline uint64_t get_tenant_id() const override { return tenant_id_; }
  inline virtual void set_table_id(const uint64_t table_id) override { table_id_ = table_id; }
  inline virtual uint64_t get_table_id() const { return table_id_; }
  inline void set_tablet_id(const ObTabletID &tablet_id) { tablet_id_ = tablet_id; }
  inline void set_tablet_id(const uint64_t tablet_id) { tablet_id_ = tablet_id; }
  inline void set_object_status(const ObObjectStatus status) { object_status_ = status; }
  inline void set_object_status(const int64_t status) { object_status_ = static_cast<ObObjectStatus> (status); }
  inline ObObjectStatus get_object_status() const { return object_status_; }
  inline void set_force_view(const bool flag) { is_force_view_ = flag; }
  inline bool is_force_view() const { return is_force_view_; }
  virtual ObObjectID get_object_id() const override;
  inline ObTabletID get_tablet_id() const { return tablet_id_; }
  inline void set_association_table_id(const uint64_t table_id) { association_table_id_ = table_id; }
  inline uint64_t get_association_table_id() const { return association_table_id_; }
  inline void set_max_dependency_version(const int64_t schema_version)
  { max_dependency_version_ = schema_version; }
  inline int64_t get_max_dependency_version() const { return max_dependency_version_; }
  inline void set_schema_version(const int64_t schema_version) override { schema_version_ = schema_version; }
  inline int64_t get_schema_version() const override { return schema_version_; }
  inline void set_database_id(const uint64_t database_id) { database_id_ = database_id; }
  inline uint64_t get_database_id() const { return database_id_; }
  virtual void set_tablegroup_id(const uint64_t tablegroup_id) override { tablegroup_id_ = tablegroup_id; }
  virtual uint64_t get_tablegroup_id() const override { return tablegroup_id_; }
  inline void set_data_table_id(const uint64_t data_table_id) { data_table_id_ = data_table_id; }
  virtual inline uint64_t get_data_table_id() const { return data_table_id_; }
  inline int set_table_name(const common::ObString &table_name)
  { return deep_copy_str(table_name, table_name_); }
  inline const char *get_table_name() const { return extract_str(table_name_); }
  virtual const char *get_entity_name() const override { return extract_str(table_name_); }
  inline const common::ObString &get_table_name_str() const { return table_name_; }
  inline const common::ObString &get_origin_index_name_str() const { return origin_index_name_; }
  inline void set_name_case_mode(const common::ObNameCaseMode cmp_mode) { name_case_mode_ = cmp_mode; }
  inline common::ObNameCaseMode get_name_case_mode() const { return name_case_mode_; }
  inline void set_table_type(const ObTableType table_type) { table_type_ = table_type; }
  virtual inline ObTableType get_table_type() const override { return table_type_; }
  inline void set_table_mode(const int32_t table_mode) { table_mode_.mode_ = table_mode; }
  inline int32_t get_table_mode() const { return table_mode_.mode_; }
  inline void set_table_mode_struct(const ObTableMode table_mode) { table_mode_ = table_mode; }
  virtual inline ObTableMode get_table_mode_struct() const override { return table_mode_; }
  inline ObTableModeFlag get_table_mode_flag() const
  { return (ObTableModeFlag)table_mode_.mode_flag_; }
  inline ObTablePKMode get_table_pk_mode() const
  { return (ObTablePKMode)table_mode_.pk_mode_; }
  inline void set_table_mode_flag(const ObTableModeFlag mode_flag)
  { table_mode_.mode_flag_ =  mode_flag; }
  inline void set_table_pk_mode(const ObTablePKMode pk_mode)
  { table_mode_.pk_mode_ =  pk_mode; }
  inline void set_table_organization_mode(const ObTableOrganizationMode organization_mode)
  { table_mode_.organization_mode_ = organization_mode; }
  inline void set_view_created_method_flag(const ObViewCreatedMethodFlag view_created_method_flag)
    { table_mode_.view_created_method_flag_ =  view_created_method_flag; }
  inline ObViewCreatedMethodFlag get_view_created_method_flag() const
    { return (ObViewCreatedMethodFlag)table_mode_.view_created_method_flag_; }
  inline bool is_view_created_by_or_replace_force() const
  { return VIEW_CREATED_BY_OR_REPLACE == (ObViewCreatedMethodFlag)table_mode_.view_created_method_flag_; }

  inline void set_table_auto_increment_mode(const ObTableAutoIncrementMode table_auto_increment_mode)
    { table_mode_.auto_increment_mode_ =  table_auto_increment_mode; }
  inline ObTableAutoIncrementMode get_table_auto_increment_mode() const
    { return (ObTableAutoIncrementMode)table_mode_.auto_increment_mode_; }
  inline bool is_order_auto_increment_mode() const
  { return ORDER == (ObTableAutoIncrementMode)table_mode_.auto_increment_mode_; }

  inline void set_table_rowid_mode(const ObTableRowidMode table_rowid_mode)
    { table_mode_.rowid_mode_ = table_rowid_mode; }
  inline ObTableRowidMode get_table_rowid_mode() const
    { return (ObTableRowidMode)table_mode_.rowid_mode_; }
  inline bool is_extended_rowid_mode() const
  { return ROWID_EXTENDED == (ObTableRowidMode)table_mode_.rowid_mode_; }

  inline ObTableStateFlag get_table_state_flag() const
  { return (ObTableStateFlag)table_mode_.state_flag_; }
  inline bool is_offline_ddl_table() const
  { return TABLE_STATE_IS_DDL_MASK & table_mode_.state_flag_; }
  inline bool is_user_hidden_table() const
  { return table_mode_.is_user_hidden_table(); }
  inline bool is_offline_ddl_original_table() const
  { return is_offline_ddl_table() && !is_user_hidden_table(); }
  inline bool check_can_do_ddl() const
  { return TABLE_STATE_NORMAL == (ObTableStateFlag)table_mode_.state_flag_ || in_offline_ddl_white_list_; }
  inline void set_table_state_flag(const ObTableStateFlag flag)
  { table_mode_.state_flag_ = flag; }
  inline bool is_queuing_table() const
  { return is_queuing_table_mode(static_cast<ObTableModeFlag>(table_mode_.mode_flag_)); }
  inline bool is_iot_table() const
  { return TOM_INDEX_ORGANIZED == (enum ObTableOrganizationMode)table_mode_.organization_mode_; }
  inline bool is_heap_table() const
  { return TOM_HEAP_ORGANIZED == (enum ObTableOrganizationMode)table_mode_.organization_mode_; }
  inline bool view_column_filled() const
  { return FILLED == (enum ObViewColumnFilledFlag)table_mode_.view_column_filled_flag_; }
  inline void set_view_column_filled_flag(const ObViewColumnFilledFlag flag)
  { table_mode_.view_column_filled_flag_ = flag; }
  inline bool mv_container_table() const
  { return IS_MV_CONTAINER_TABLE == (enum ObMVContainerTableFlag)table_mode_.mv_container_table_flag_; }
  inline void set_mv_container_table(const ObMVContainerTableFlag flag)
  { table_mode_.mv_container_table_flag_ = flag; }
  inline bool mv_available() const
  { return IS_MV_AVAILABLE == (enum ObMVAvailableFlag)table_mode_.mv_available_flag_; }
  inline void set_mv_available(const ObMVAvailableFlag flag)
  { table_mode_.mv_available_flag_ = flag; }
  inline bool table_referenced_by_mv() const
  { return IS_REFERENCED_BY_MV == (enum ObTableReferencedByMVFlag)table_mode_.table_referenced_by_mv_flag_; }
  inline void set_table_referenced_by_mv(const ObTableReferencedByMVFlag flag)
  { table_mode_.table_referenced_by_mv_flag_ = flag; }
  inline bool mv_enable_query_rewrite() const
  { return IS_MV_ENABLE_QUERY_REWRITE == (enum ObMVEnableQueryRewriteFlag)table_mode_.mv_enable_query_rewrite_flag_; }
  inline void set_mv_enable_query_rewrite(const ObMVEnableQueryRewriteFlag flag)
  { table_mode_.mv_enable_query_rewrite_flag_ = flag; }
  inline bool mv_on_query_computation() const
  { return IS_MV_ON_QUERY_COMPUTATION == (enum ObMVOnQueryComputationFlag)table_mode_.mv_on_query_computation_flag_; }
  inline void set_mv_on_query_computation(const ObMVOnQueryComputationFlag flag)
  { table_mode_.mv_on_query_computation_flag_ = flag; }
  inline void set_ddl_ignore_sync_cdc_flag(const ObDDLIgnoreSyncCdcFlag flag)
  { table_mode_.ddl_table_ignore_sync_cdc_flag_ = flag; }
  inline bool is_ddl_table_ignored_to_sync_cdc() const
  { return DONT_SYNC_LOG_FOR_CDC == table_mode_.ddl_table_ignore_sync_cdc_flag_; }

  inline void set_session_id(const uint64_t id)  { session_id_ = id; }
  inline uint64_t get_session_id() const { return session_id_; }
  inline void set_truncate_version(const int64_t truncate_version ) { truncate_version_ = truncate_version; }
  inline int64_t get_truncate_version() const {return truncate_version_; }
  virtual int get_zone_list(
      share::schema::ObSchemaGetterGuard &schema_guard,
      common::ObIArray<common::ObZone> &zone_list) const override;
  virtual int get_primary_zone_inherit(
      share::schema::ObSchemaGetterGuard &schema_guard,
      share::schema::ObPrimaryZone &primary_zone) const override;
  virtual int get_paxos_replica_num(
      share::schema::ObSchemaGetterGuard &guard,
      int64_t &num) const override;
  virtual int get_first_primary_zone_inherit(
      share::schema::ObSchemaGetterGuard &schema_guard,
      const common::ObIArray<rootserver::ObReplicaAddr> &replica_addrs,
      common::ObZone &first_primary_zone) const override;
  virtual int get_zone_replica_attr_array_inherit(
      ObSchemaGetterGuard &guard,
      ZoneLocalityIArray &locality) const override;
  virtual int get_locality_str_inherit(
      share::schema::ObSchemaGetterGuard &guard,
      const common::ObString *&locality_str) const override;
  int get_tablet_ids(
      common::ObIArray<ObTabletID> &tablet_ids) const;
  int get_part_idx_by_tablet(
      const ObTabletID &tablet_id,
      int64_t &part_id,
      int64_t &subpart_id) const;
  int get_part_id_by_tablet(
      const ObTabletID &tablet_id,
      int64_t &part_id,
      int64_t &subpart_id) const;
  int get_all_first_level_part_ids(
      ObIArray<int64_t> &first_level_part_ids) const;
  int get_part_ids_by_subpart_ids(
      const ObIArray<int64_t> &subpart_ids,
      ObIArray<int64_t> &part_ids,
      int64_t &subpart_cnt_in_parts) const;
  int get_part_idx_by_part_id(
      const ObIArray<int64_t> &part_ids,
      ObIArray<int64_t> &part_idx,
      ObIArray<int64_t> &subpart_idx) const;
  int get_hidden_part_id_by_tablet_id(
      const ObTabletID &tablet_id,
      int64_t &part_id) const;
  /**
   * first_level_part_id represent the first level part id of subpartition,
   * otherwise its value is OB_INVALID_ID
   * e.g.
   *  PARTITION_LEVEL_ZERO
   *    - object_id = table_id
   *    - first_level_part_id = OB_INVALID_ID
   *  PARTITION_LEVEL_ONE
   *    - object_id = part_id
   *    - first_level_part_id = OB_INVALID_ID
   * PARTITION_LEVEL_TWO
   *    - object_id = sub_part_id
   *    - first_level_part_id = part_id
  */
  int get_part_id_and_tablet_id_by_idx(
      const int64_t part_idx,
      const int64_t subpart_idx,
      common::ObObjectID &object_id,
      common::ObObjectID &first_level_part_id,
      common::ObTabletID &tablet_id) const;
  int get_part_by_idx(
      const int64_t part_id,
      const int64_t subpart_id,
      ObBasePartition *&partition) const;
  // return tablet_ids with specified partition's object_id
  int get_tablet_ids_by_part_object_id(
      const ObObjectID &part_object_id,
      common::ObIArray<ObTabletID> &tablet_ids) const;
  int get_tablet_id_by_object_id(
      const ObObjectID &object_id,
      ObTabletID &tablet_id) const;
  int set_specific_replica_attr_array(
      share::SchemaReplicaAttrArray &schema_replica_set,
      const common::ObIArray<ReplicaAttr> &src);
  int get_all_replica_num(
      share::schema::ObSchemaGetterGuard &guard,
      int64_t &num) const; // except R{all_server}
  int get_full_replica_num(
      share::schema::ObSchemaGetterGuard &guard,
      int64_t &num) const;
  int check_is_readonly_at_all(
      share::schema::ObSchemaGetterGuard &guard,
      const common::ObZone &zone,
      const common::ObRegion &region,
      bool &readonly_at_all) const;
  int check_has_all_server_readonly_replica(
      share::schema::ObSchemaGetterGuard &guard,
      bool &has) const;
  int check_is_all_server_readonly_replica(
      share::schema::ObSchemaGetterGuard &guard,
      bool &is) const;
  static int compare_partition_option(const schema::ObSimpleTableSchemaV2 &t1,
                                      const schema::ObSimpleTableSchemaV2 &t2,
                                      bool check_subpart,
                                      bool &is_matched,
                                      ObSqlString *user_error = NULL);
  int check_if_tablet_exists(const common::ObTabletID &tablet_id, bool &exists) const;

  int add_simple_foreign_key_info(const uint64_t tenant_id,
                                  const uint64_t database_id,
                                  const uint64_t table_id,
                                  const int64_t foreign_key_id,
                                  const common::ObString &foreign_key_name);
  int set_simple_foreign_key_info_array(const common::ObIArray<ObSimpleForeignKeyInfo> &simple_fk_info_array);
  inline const common::ObIArray<ObSimpleForeignKeyInfo> &get_simple_foreign_key_info_array() const { return simple_foreign_key_info_array_; }
  int add_simple_constraint_info(const uint64_t tenant_id,
                                 const uint64_t database_id,
                                 const uint64_t table_id,
                                 const int64_t constraint_id,
                                 const common::ObString &constraint_name);
  int set_simple_constraint_info_array(const common::ObIArray<ObSimpleConstraintInfo> &simple_cst_info_array);
  inline const common::ObIArray<ObSimpleConstraintInfo> &get_simple_constraint_info_array() const { return simple_constraint_info_array_; }
  // dblink.
  inline bool is_link_table() const { return dblink_id_ != OB_INVALID_ID; }
  inline void set_dblink_id(const uint64_t dblink_id) { dblink_id_ = dblink_id; }
  inline uint64_t get_dblink_id() const { return dblink_id_; }
  inline void set_link_table_id(const uint64_t link_table_id) { link_table_id_ = link_table_id; }
  inline uint64_t get_link_table_id() const { return link_table_id_; }
  inline void set_link_schema_version(const int64_t version) { link_schema_version_ = version; }
  inline int64_t get_link_schema_version() const { return link_schema_version_; }
  inline void save_local_schema_version(const int64_t local_version)
  {
    link_schema_version_ = schema_version_;
    schema_version_ = local_version;
  }
  inline int set_link_database_name(const common::ObString &database_name)
  { return deep_copy_str(database_name, link_database_name_); }
  inline const common::ObString &get_link_database_name() const { return link_database_name_; }

  // only index table schema can invoke this function
  int get_index_name(common::ObString &index_name) const;
  int get_mlog_name(common::ObString &mlog_name) const;
  template <typename Allocator>
  static int get_index_name(Allocator &allocator, uint64_t table_id,
      const common::ObString &src, common::ObString &dst);
  static int get_index_name(const common::ObString &src, common::ObString &dst);

  static uint64_t extract_data_table_id_from_index_name(const common::ObString &index_name);
  int generate_origin_index_name();
  virtual int check_if_oracle_compat_mode(bool &is_oracle_mode) const;
  // interface derived
  // TODO: dup code, need merge with ObTableSchema
  //
  // Caution! is_table() does not include tmp tables. is_table() and is_tmp_table() are two mutually-exclusive functions.
  inline bool is_table() const { return is_user_table() || is_sys_table() || is_vir_table(); }
  inline bool is_user_view() const { return share::schema::ObTableType::USER_VIEW == table_type_; }
  inline bool is_sys_view() const { return share::schema::ObTableType::SYSTEM_VIEW == table_type_; }
  inline bool is_storage_index_table() const override { return is_index_table(); }
  inline static bool is_storage_index_table(share::schema::ObTableType table_type)
  { return share::schema::is_index_table(table_type); }
  inline bool is_storage_local_index_table() const { return is_index_local_storage(); }
  inline bool is_user_table() const { return share::schema::ObTableType::USER_TABLE == table_type_; }
  inline bool is_sys_table() const { return share::schema::ObTableType::SYSTEM_TABLE == table_type_; }
  inline bool is_vir_table() const { return share::schema::ObTableType::VIRTUAL_TABLE == table_type_; }
  inline bool is_view_table() const { return share::schema::is_view_table(table_type_); }
  inline bool is_index_table()  const { return share::schema::is_index_table(table_type_); }
  inline bool is_tmp_table() const { return is_mysql_tmp_table() || share::schema::ObTableType::TMP_TABLE_ORA_SESS == table_type_ || share::schema::ObTableType::TMP_TABLE_ORA_TRX == table_type_; }
  inline bool is_ctas_tmp_table() const { return 0 != session_id_ && !is_tmp_table(); }
  inline bool is_mysql_tmp_table() const { return share::schema::is_mysql_tmp_table(table_type_); }
  inline bool is_oracle_tmp_table() const { return share::schema::ObTableType::TMP_TABLE_ORA_SESS == table_type_ || share::schema::ObTableType::TMP_TABLE_ORA_TRX == table_type_; }
  inline bool is_oracle_sess_tmp_table() const { return share::schema::ObTableType::TMP_TABLE_ORA_SESS == table_type_; }
  inline bool is_oracle_trx_tmp_table() const { return share::schema::ObTableType::TMP_TABLE_ORA_TRX == table_type_; }
  virtual inline bool is_aux_vp_table() const override { return share::schema::ObTableType::AUX_VERTIAL_PARTITION_TABLE == table_type_; }
  inline bool is_aux_lob_piece_table() const { return share::schema::is_aux_lob_piece_table(table_type_); }
  inline bool is_aux_lob_meta_table() const { return share::schema::is_aux_lob_meta_table(table_type_); }
  inline bool is_aux_lob_table() const { return is_aux_lob_meta_table() || is_aux_lob_piece_table(); }
  inline bool is_aux_table() const { return share::schema::ObTableType::USER_INDEX == table_type_ || share::schema::ObTableType::AUX_VERTIAL_PARTITION_TABLE == table_type_ || share::schema::ObTableType::AUX_LOB_PIECE == table_type_ || share::schema::ObTableType::AUX_LOB_META == table_type_; }
  // Primary partition table judgment: still USER_TABLE, but data_table_id_ is the same as itself,
  // the default data_table_id_ is 0
  virtual inline bool is_primary_vp_table() const override { return (share::schema::ObTableType::USER_TABLE == table_type_) && (table_id_ == data_table_id_); }
  // when support global index, do not modify this local index interface
  inline bool is_materialized_view() const { return is_materialized_view(table_type_); }
  inline static bool is_materialized_view(share::schema::ObTableType table_type)
  { return MATERIALIZED_VIEW == table_type; }
  inline bool is_mlog_table() const { return is_mlog_table(table_type_); }
  inline static bool is_mlog_table(share::schema::ObTableType table_type)
  { return MATERIALIZED_VIEW_LOG == table_type; }
  inline static bool is_user_data_table(share::schema::ObTableType table_type)
  { return USER_TABLE == table_type; }
  inline bool is_in_recyclebin() const
  { return common::OB_RECYCLEBIN_SCHEMA_ID == database_id_; }
  virtual inline bool is_external_table() const override { return EXTERNAL_TABLE == table_type_; }
  inline ObTenantTableId get_tenant_table_id() const
  { return ObTenantTableId(tenant_id_, table_id_); }
  inline ObTenantTableId get_tenant_data_table_id() const
  { return ObTenantTableId(tenant_id_, data_table_id_); }
  inline bool should_not_validate_data_index_ckm() const;
  inline bool should_check_major_merge_progress() const;
  inline bool is_multivalue_index() const;
  inline bool is_multivalue_index_aux() const;
  inline bool is_spatial_index() const;
  inline static bool is_spatial_index(const ObIndexType index_type);
  inline bool is_vec_index() const;
  inline static bool is_vec_index(const ObIndexType index_type);
  inline bool is_built_in_vec_index() const;
  inline bool is_vec_rowkey_vid_type() const;
  inline bool is_vec_vid_rowkey_type() const;
  inline bool is_vec_delta_buffer_type() const;
  inline bool is_vec_index_id_type() const;
  inline bool is_vec_index_snapshot_data_type() const;
  inline bool is_fts_index() const;
  inline bool is_built_in_fts_index() const;
  inline bool is_built_in_multivalue_index() const;
  inline bool is_built_in_index() const;  // fts / vector index
  inline bool is_rowkey_doc_id() const;
  inline bool is_doc_id_rowkey() const;
  inline bool is_fts_index_aux() const;
  inline bool is_fts_doc_word_aux() const;
  inline bool is_fts_or_multivalue_index() const;
  inline bool is_normal_index() const;
  inline bool is_unique_index() const;
  inline static bool is_unique_index(ObIndexType index_type);
  virtual inline bool is_global_index_table() const override;
  inline static bool is_global_index_table(const ObIndexType index_type);
  inline bool is_global_local_index_table() const;
  inline bool is_global_normal_index_table() const;
  inline bool is_global_unique_index_table() const;
  inline static bool is_global_unique_index_table(const ObIndexType index_type);
  inline bool is_local_unique_index_table() const;
  inline bool is_domain_index() const;
  inline static bool is_domain_index(const ObIndexType index_type);
  inline bool is_index_local_storage() const;
  virtual bool has_tablet() const override;
  inline bool has_partition() const
  { return !(is_vir_table() || is_view_table() || is_index_local_storage() || is_aux_vp_table() || is_aux_lob_table()); }
  // Introduced by pg, the stand alone table has its own physical partition
  virtual bool has_self_partition() const override { return has_partition(); }
  inline bool is_unavailable_index() const { return INDEX_STATUS_UNAVAILABLE == index_status_; }
  inline bool can_read_index() const { return can_read_index(index_status_); }
  inline static bool can_read_index(ObIndexStatus index_status)
  { return INDEX_STATUS_AVAILABLE == index_status; }
  inline bool is_available_mlog() const { return is_mlog_table() && (INDEX_STATUS_AVAILABLE == index_status_); }
  inline bool is_final_invalid_index() const;
  inline void set_index_status(const ObIndexStatus index_status) { index_status_ = index_status; }
  inline void set_index_type(const ObIndexType index_type) { index_type_ = index_type; }
  inline ObIndexStatus get_index_status() const { return index_status_; }
  virtual inline ObIndexType get_index_type() const override { return index_type_; }
  virtual bool is_hidden_schema() const override { return is_user_hidden_table(); }
  virtual bool is_normal_schema() const override { return !is_hidden_schema(); }

  virtual bool is_user_partition_table() const override;
  virtual bool is_user_subpartition_table() const override;
  inline bool is_partitioned_table() const { return PARTITION_LEVEL_ONE == get_part_level() || PARTITION_LEVEL_TWO == get_part_level(); }
  virtual ObPartitionLevel get_part_level() const override;
  virtual share::ObDuplicateScope get_duplicate_scope() const override { return duplicate_scope_; }
  inline void set_duplicate_attribute(const share::ObDuplicateScope duplicate_scope,
                                      const share::ObDuplicateReadConsistency duplicate_read_consistency) {
    duplicate_scope_ = duplicate_scope;
    duplicate_read_consistency_ = duplicate_read_consistency;
  }
  inline void set_duplicate_read_consistency(const share::ObDuplicateReadConsistency duplicate_read_consistency) { duplicate_read_consistency_ = duplicate_read_consistency; }
  inline share::ObDuplicateReadConsistency get_duplicate_read_consistency() const { return duplicate_read_consistency_; }

  inline bool is_duplicate_table() const {
    return duplicate_scope_ == ObDuplicateScope::DUPLICATE_SCOPE_CLUSTER
           && duplicate_read_consistency_ == ObDuplicateReadConsistency::STRONG;
  }
  inline bool is_broadcast_table() const {
    return duplicate_scope_ == ObDuplicateScope::DUPLICATE_SCOPE_CLUSTER
           && duplicate_read_consistency_ == ObDuplicateReadConsistency::WEAK;
  }
  // for encrypt
  int set_encryption_str(const common::ObString &str) { return deep_copy_str(str, encryption_); }
  virtual const common::ObString &get_encryption_str() const override { return encryption_; }
  int get_encryption_id(int64_t &encrypt_id) const;
  bool need_encrypt() const;
  bool is_equal_encryption(const ObSimpleTableSchemaV2 &t) const;
  inline virtual void set_tablespace_id(const uint64_t id) { tablespace_id_ = id; }
  inline virtual uint64_t get_tablespace_id() const { return tablespace_id_; }
  virtual inline uint64_t get_master_key_id() const override { return master_key_id_; }
  virtual inline const common::ObString &get_encrypt_key() const override { return encrypt_key_; }
  virtual inline const char *get_encrypt_key_str() const override { return extract_str(encrypt_key_); }
  virtual inline int64_t get_encrypt_key_len() const override { return encrypt_key_.length(); }
  inline void set_master_key_id(uint64_t id) { master_key_id_ = id; }
  inline int set_encrypt_key(const common::ObString &key) { return deep_copy_str(key, encrypt_key_); }
  inline void set_in_offline_ddl_white_list(const bool in_offline_ddl_white_list) { in_offline_ddl_white_list_ = in_offline_ddl_white_list; }

  inline bool get_in_offline_ddl_white_list() const { return in_offline_ddl_white_list_; }

  inline bool has_rowid() const { return is_user_table() || is_tmp_table(); }
  inline bool gen_normal_tablet() const { return has_rowid() && !is_extended_rowid_mode(); }
  inline bool is_new_queuing_table_mode() const { return is_new_queuing_mode(static_cast<ObTableModeFlag>(table_mode_.mode_flag_)); }
  /**
  * During the construction of a local index in a partition table, the included columns may not contain the partition key.
  * Since it's necessary to calculate which partition the data will be stored in, the partition key must be supplied to augment the index.
  *
  * For the function to return true, the following conditions must be met:
  * 1) Local index
  * 2) The indexed table does not contain the partition key.
  *
  * Current case:
  * 1) the local index of the heap
  * 2) two of the fts related table : fts_index_aux and fts_doc_word_aux.
  *    Note: other tables of the fts related table (doc_rowkey table and rowkey_doc table) contain the partition key.
  **/
  inline bool need_partition_key_for_build_local_index(const ObSimpleTableSchemaV2 &data_table_schema) const
  {
    const bool heap_case =  is_index_local_storage() && data_table_schema.is_heap_table();
    const bool fts_case = is_partitioned_table() && is_index_local_storage() && (is_fts_index_aux() || is_fts_doc_word_aux());
    const bool multivalue_case = is_partitioned_table() && is_index_local_storage() && is_multivalue_index_aux();
    const bool vec_case = is_partitioned_table() && is_index_local_storage() && (is_vec_delta_buffer_type() || is_vec_index_id_type() || is_vec_index_snapshot_data_type());
    return heap_case || fts_case || vec_case || multivalue_case;
  }
  DECLARE_VIRTUAL_TO_STRING;
protected:
  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t schema_version_;
  uint64_t database_id_;
  uint64_t tablegroup_id_;
  uint64_t data_table_id_;
  // Only in the process of querying the creation of the table and assigning the value of the temporary table to the session_id
  // at the time of creation, it is 0 in other occasions;
  uint64_t session_id_;
  common::ObString table_name_;
  common::ObNameCaseMode name_case_mode_;
  ObTableType table_type_;
  ObTableMode table_mode_;
  ObIndexStatus index_status_;
  ObIndexType index_type_;
  common::ObArray<ObSimpleForeignKeyInfo> simple_foreign_key_info_array_;
  common::ObArray<ObSimpleConstraintInfo> simple_constraint_info_array_;
  // Only the type of index is valid in oracle mode, which means the original index name without prefix (__idx__table_id_)
  common::ObString origin_index_name_;
  share::ObDuplicateScope duplicate_scope_;
  share::ObDuplicateReadConsistency duplicate_read_consistency_;
  common::ObString encryption_;
  uint64_t tablespace_id_;
  common::ObString encrypt_key_;
  uint64_t master_key_id_;
  int64_t truncate_version_;


  // dblink.
  // No serialization required
  uint64_t dblink_id_;
  uint64_t link_table_id_;
  int64_t link_schema_version_;
  common::ObString link_database_name_;
  // TODO(jiuren): need link_table_name_?
  int64_t max_dependency_version_;
  uint64_t association_table_id_;
  bool in_offline_ddl_white_list_;
  ObTabletID tablet_id_;
  ObObjectStatus object_status_;
  bool is_force_view_; // only record in create view path, do not persist to disk
};

class ObTableSchema : public ObSimpleTableSchemaV2
{
  OB_UNIS_VERSION(1);

public:
  friend struct AlterTableSchema;
  friend class ObPrintableTableSchema;
  enum ObIndexAttributesFlag
  {
    INDEX_VISIBILITY = 0,
    INDEX_DROP_INDEX = 1,
    INDEX_VISIBILITY_SET_BEFORE = 2,
    INDEX_ROW_MOVEABLE = 3,
    INDEX_IS_IN_DELETING = 4,
    MAX_INDEX_ATTRIBUTE = 64,
  };

  enum ObColumnCheckMode
  {
    CHECK_MODE_ONLINE = 0,
    CHECK_MODE_OFFLINE = 1,
  };

  static const int64_t MIN_COLUMN_COUNT_WITH_PK_TABLE = 1;
  static const int64_t MIN_COLUMN_COUNT_WITH_HEAP_TABLE = 2;
  static const int64_t DEFAULT_COLUMN_GROUP_ARRAY_CAPACITY = 8;
  bool cmp_table_id(const ObTableSchema *a, const ObTableSchema *b)
  {
    return a->get_tenant_id() < b->get_tenant_id() ||
        a->get_database_id() < b->get_database_id() ||
        a->get_table_id() < b->get_table_id();
  }
  static void construct_partition_key_column(const ObColumnSchemaV2 &column,
                                             common::ObPartitionKeyColumn &partition_key_column);
  static int create_idx_name_automatically_oracle(common::ObString &idx_name,
                                                  const common::ObString &table_name,
                                                  common::ObIAllocator &allocator);
  static int create_cons_name_automatically(common::ObString &cst_name,
                                            const common::ObString &table_name,
                                            common::ObIAllocator &allocator,
                                            ObConstraintType cst_type,
                                            const bool is_oracle_mode);
  static int create_cons_name_automatically_with_dup_check(common::ObString &cst_name,
                                                  const common::ObString &table_name,
                                                  common::ObIAllocator &allocator,
                                                  ObConstraintType cst_type,
                                                  share::schema::ObSchemaGetterGuard &schema_guard,
                                                  const uint64_t tenant_id,
                                                  const uint64_t database_id,
                                                  const int64_t retry_times,
                                                  bool &cst_name_generated,
                                                  const bool is_oracle_mode);
  static int create_new_idx_name_after_flashback(ObTableSchema &new_table_schema,
                                                 common::ObString &new_idx_name,
                                                 common::ObIAllocator &allocator,
                                                 ObSchemaGetterGuard &guard);
  static int get_xml_hidden_column_id(const ObTableSchema *data_table_schema,
                                      const ObColumnSchemaV2 *data_column_schema,
                                      int64_t &data_column_id);
  static int find_xml_hidden_column_index(const ObTableSchema *table_schema,
                                          const ObColumnSchemaV2 *column_schema,
                                          const ObArray<ObColDesc> &desc_col_ids,
                                          int64_t &dst_index_col);
public:
  typedef ObColumnSchemaV2* const *const_column_iterator;
  typedef ObConstraint * const *const_constraint_iterator;
  typedef ObConstraint **constraint_iterator;
  typedef ObColumnGroupSchema* const *const_column_group_iterator;
  ObTableSchema();
  explicit ObTableSchema(common::ObIAllocator *allocator);
  ObTableSchema(const ObTableSchema &src_schema) = delete;
  virtual ~ObTableSchema();
  ObTableSchema &operator=(const ObTableSchema &src_schema) = delete;
  void reset_partition_schema() override;
  void reset_column_part_key_info();
  int assign(const ObTableSchema &src_schema);
  int get_view_column_comment(ObIArray<ObString> &column_comments);
  //part splitting filter is needed during physical splitting
  bool need_part_filter() const
  {
    // At present, the conditions for supporting partition split are OLD tables without primary key, and user tables,
    // and do not include check constraints
    //is_in_physical_split() The interface does not take effect temporarily, please comment it out first
    return is_user_table()
           && !has_check_constraint()
           && is_partitioned_table()
           && is_in_splitting()
           /*&& is_in_physical_split()*/;
  }
  //set methods
  inline void set_max_used_column_id(const uint64_t id)  { max_used_column_id_ = id; }
  inline void set_sess_active_time(const int64_t t)  { sess_active_time_ = t; }
  inline void set_index_attributes_set(const uint64_t id)  { index_attributes_set_ = id; }
  inline void set_index_visibility(const uint64_t index_visibility)
  {
    index_attributes_set_ &= ~((uint64_t)(1) << INDEX_VISIBILITY);
    index_attributes_set_ |= index_visibility << INDEX_VISIBILITY;
  }
  inline void set_enable_row_movement(const bool enable_row_move)
  {
    index_attributes_set_ &= ~((uint64_t)(1) << INDEX_ROW_MOVEABLE);
    if (enable_row_move) {
      index_attributes_set_ |= (1 << INDEX_ROW_MOVEABLE);
    }
  }
  inline void set_rowkey_column_num(const int64_t rowkey_column_num) { rowkey_column_num_ = rowkey_column_num; }
  inline void set_index_column_num(const int64_t index_column_num) { index_column_num_ = index_column_num; }
  inline void set_rowkey_split_pos(int64_t pos) { rowkey_split_pos_ = pos;}
  inline void set_part_key_column_num(const int64_t part_key_column_num) { part_key_column_num_ = part_key_column_num; }
  inline void set_subpart_key_column_num(const int64_t subpart_key_column_num) { subpart_key_column_num_ = subpart_key_column_num; }
  inline void set_progressive_merge_num(const int64_t progressive_merge_num) { progressive_merge_num_ = progressive_merge_num; }
  inline void set_progressive_merge_round(const int64_t progressive_merge_round) { progressive_merge_round_ = progressive_merge_round; }
  inline void set_tablet_size(const int64_t tablet_size) { tablet_size_ = tablet_size; }
  inline void set_pctfree(const int64_t pctfree) { pctfree_ = pctfree; }
  inline void set_autoinc_column_id(const int64_t autoinc_column_id) { autoinc_column_id_ = autoinc_column_id; }
  inline void set_auto_increment(const uint64_t auto_increment) { auto_increment_ = auto_increment; }
  inline void set_load_type(ObTableLoadType load_type) { load_type_ = load_type;}
  inline void set_def_type(ObTableDefType type) { def_type_ = type; }
  inline void set_partition_num(int64_t partition_num) { partition_num_ = partition_num; }
  inline void set_charset_type(const common::ObCharsetType type) { charset_type_ = type; }
  inline void set_collation_type(const common::ObCollationType type) { collation_type_ = type; }
  inline void set_code_version(const int64_t code_version) {code_version_ = code_version; }
  inline void set_index_using_type(const ObIndexUsingType index_using_type) { index_using_type_ = index_using_type; }
  inline void set_max_column_id(const uint64_t id) { max_used_column_id_ = id; }
  inline void set_is_use_bloomfilter(const bool is_use_bloomfilter) { is_use_bloomfilter_ = is_use_bloomfilter; }
  inline void set_block_size(const int64_t block_size) { block_size_ = block_size; }
  inline void set_read_only(const bool read_only) { read_only_ = read_only; }
  inline void set_store_format(const common::ObStoreFormatType store_format) { store_format_ = store_format; }
  inline void set_storage_format_version(const int64_t storage_format_version) { storage_format_version_ = storage_format_version; }
  int set_store_format(const common::ObString &store_format);
  inline void set_row_store_type(const common::ObRowStoreType row_store_type) { row_store_type_ = row_store_type; }
  int set_row_store_type(const common::ObString &row_store);
  int set_tablegroup_name(const char *tablegroup_name) { return deep_copy_str(tablegroup_name, tablegroup_name_); }
  int set_tablegroup_name(const common::ObString &tablegroup_name) { return deep_copy_str(tablegroup_name, tablegroup_name_); }
  int set_comment(const char *comment) { return deep_copy_str(comment, comment_); }
  int set_comment(const common::ObString &comment) { return deep_copy_str(comment, comment_); }
  int set_pk_comment(const char *comment) { return deep_copy_str(comment, pk_comment_); }
  int set_pk_comment(const common::ObString &comment) { return deep_copy_str(comment, pk_comment_); }
  int set_create_host(const char *create_host) { return deep_copy_str(create_host, create_host_); }
  int set_create_host(const common::ObString &create_host) { return deep_copy_str(create_host, create_host_); }
  int set_expire_info(const common::ObString &expire_info) { return deep_copy_str(expire_info, expire_info_); }
  int set_compress_func_name(const char *compressor);
  int set_compress_func_name(const common::ObString &compressor);
  inline void set_dop(int64_t table_dop) { table_dop_ = table_dop; }
  int set_external_file_location(const common::ObString &location) { return deep_copy_str(location, external_file_location_); }
  int set_external_file_location_access_info(const common::ObString &access_info) { return deep_copy_str(access_info, external_file_location_access_info_); }
  int set_external_file_format(const common::ObString &format) { return deep_copy_str(format, external_file_format_); }
  int set_external_file_pattern(const common::ObString &pattern) { return deep_copy_str(pattern, external_file_pattern_); }
  int set_external_properties(const common::ObString &format) { return deep_copy_str(format, external_properties_); }
  void set_external_table_auto_refresh(const int64_t flag) { table_flags_ |= (flag << EXTERNAL_TABLE_AUTO_REFRESH_FLAG_OFFSET); }
  inline void set_user_specified_partition_for_external_table() { table_flags_ |= EXTERNAL_TABLE_USER_SPECIFIED_PARTITION_FLAG; }
  template<typename ColumnType>
  int add_column(const ColumnType &column);
  int delete_column(const common::ObString &column_name);
  int delete_all_view_columns();
  int alter_all_view_columns_type_undefined(bool &already_invalid);
  int alter_column(ObColumnSchemaV2 &column, ObColumnCheckMode check_mode, const bool for_view);

  int alter_mysql_table_columns(
    common::ObIArray<ObColumnSchemaV2> &columns,
    common::ObIArray<common::ObString> &orig_names,
    ObColumnCheckMode check_mode);
  int reorder_column(const ObString &column_name, const bool is_first, const ObString &prev_column_name, const ObString &next_column_name);
  int add_aux_vp_tid(const uint64_t aux_vp_tid);
  int add_partition_key(const common::ObString &column_name);
  int add_partition_key(const uint64_t column_id);
  int add_subpartition_key(const common::ObString &column_name);
  int add_zone(const common::ObString &zone);
  int set_view_definition(const common::ObString &view_definition);
  int set_parser_name(const common::ObString &parser_name) { return deep_copy_str(parser_name, parser_name_); }
  int set_rowkey_info(const ObColumnSchemaV2 &column);
  int set_foreign_key_infos(const common::ObIArray<ObForeignKeyInfo> &foreign_key_infos_);
  void clear_foreign_key_infos();
  int set_trigger_list(const common::ObIArray<uint64_t> &trigger_list);
  int set_simple_index_infos(const common::ObIArray<ObAuxTableMetaInfo> &simple_index_infos);
  int set_aux_vp_tid_array(const common::ObIArray<uint64_t> &aux_vp_tid_array);
  // constraint related
  int add_constraint(const ObConstraint &constraint);
  int delete_constraint(const common::ObString &constraint_name);
  // Copy all constraint information in src_schema
  int assign_constraint(const ObTableSchema &other);
  int assign_partition_schema_without_auto_part_attr(const ObTableSchema &other);
  int enable_auto_partition(const int64_t auto_part_size, const ObPartitionFuncType &part_func_type);
  int detect_auto_part_func_type(ObPartitionFuncType &part_func_type);

  int is_range_col_part_type(bool &is_range_column_type) const;
  void forbid_auto_partition();
  void clear_constraint();
  int set_ttl_definition(const common::ObString &ttl_definition) { return deep_copy_str(ttl_definition, ttl_definition_); }
  int set_kv_attributes(const common::ObString &kv_attributes) { return deep_copy_str(kv_attributes, kv_attributes_); }
  int set_index_params(const common::ObString &index_params) { return deep_copy_str(index_params, index_params_); }
  int set_exec_env(const common::ObString &exec_env) { return deep_copy_str(exec_env, exec_env_); }
  void set_lob_inrow_threshold(const int64_t lob_inrow_threshold) { lob_inrow_threshold_ = lob_inrow_threshold;}
  inline void set_auto_increment_cache_size(const int64_t auto_increment_cache_size)
  { auto_increment_cache_size_ = auto_increment_cache_size; }
//get methods
  bool is_valid() const;
  int check_valid(const bool count_varchar_size_by_byte) const;

  int get_generated_column_by_define(const common::ObString &col_def,
                                     const bool only_hidden_column,
                                     share::schema::ObColumnSchemaV2 *&gen_col);
  int get_aux_vp_tid_array(common::ObIArray<uint64_t> &aux_vp_tid_array) const;
  int get_aux_vp_tid_array(uint64_t *aux_vp_tid_array, int64_t &aux_vp_cnt) const;
  void get_column_name_by_column_id(const uint64_t column_id, common::ObString &column_name, bool &is_column_exist) const;
  const ObColumnSchemaV2 *get_column_schema(const uint64_t column_id) const;
  const ObColumnSchemaV2 *get_column_schema(const char *column_name) const;
  const ObColumnSchemaV2 *get_column_schema(const common::ObString &column_name) const;
  const ObColumnSchemaV2 *get_column_schema_by_idx(const int64_t idx) const;
  const ObColumnSchemaV2 *get_column_schema(uint64_t table_id, uint64_t column_id) const;

  const ObColumnSchemaV2 *get_fulltext_column(const ColumnReferenceSet &column_set) const;
  ObColumnSchemaV2 *get_column_schema(const uint64_t column_id);
  ObColumnSchemaV2 *get_column_schema(const char *column_name);
  ObColumnSchemaV2 *get_column_schema(const common::ObString &column_name);
  ObColumnSchemaV2 *get_column_schema_by_idx(const int64_t idx);
  ObColumnSchemaV2 *get_column_schema_by_prev_next_id(const uint64_t column_id);
  const ObColumnSchemaV2 *get_column_schema_by_prev_next_id(const uint64_t column_id) const;
  static uint64_t gen_materialized_view_column_id(uint64_t column_id);
  static uint64_t get_materialized_view_column_id(uint64_t column_id);
  static uint64_t gen_mlog_col_id_from_ref_col_id(const uint64_t column_id);
  static uint64_t gen_ref_col_id_from_mlog_col_id(const uint64_t column_id);

  const ObConstraint *get_constraint(const uint64_t constraint_id) const;
  const ObConstraint *get_constraint(const common::ObString &constraint_name) const;
  int get_pk_constraint_name(common::ObString &pk_name) const;
  const ObConstraint *get_pk_constraint() const;
  int64_t get_index_count() const;
  int64_t get_column_idx(const uint64_t column_id, const bool ignore_hidden_column = false) const;
  int64_t get_replica_num() const;
  int64_t get_tablet_size() const { return tablet_size_; }
  int64_t get_pctfree() const { return pctfree_; }
  inline ObTenantTableId get_tenant_table_id() const {return ObTenantTableId(tenant_id_, table_id_);}
  inline int64_t get_index_tid_count() const { return simple_index_infos_.count(); }
  inline int64_t get_aux_vp_tid_count() const { return aux_vp_tid_array_.count(); }
  virtual inline bool is_primary_aux_vp_table() const override { return aux_vp_tid_array_.count() > 0 && is_primary_vp_table(); }
  inline int64_t get_index_column_number() const { return index_column_num_; }
  inline uint64_t get_max_used_column_id() const { return max_used_column_id_; }
  inline int64_t get_sess_active_time() const { return sess_active_time_; }
  // Whether it is a temporary table created by ob proxy 64bit > uint max
  inline bool is_obproxy_create_tmp_tab() const { return is_tmp_table() && get_session_id() > 0xFFFFFFFFL;}
  inline int64_t get_rowkey_split_pos() const { return rowkey_split_pos_; }
  inline int64_t get_block_size() const { return block_size_;}
  virtual inline bool is_use_bloomfilter() const override { return is_use_bloomfilter_; }
  virtual inline int64_t get_progressive_merge_num() const override { return progressive_merge_num_; }
  virtual inline int64_t get_progressive_merge_round() const override { return progressive_merge_round_; }
  inline uint64_t get_autoinc_column_id() const { return autoinc_column_id_; }
  inline uint64_t get_auto_increment() const { return auto_increment_; }
  inline int64_t get_rowkey_column_num() const { return rowkey_info_.get_size(); }
  inline int64_t get_shadow_rowkey_column_num() const { return shadow_rowkey_info_.get_size(); }
  inline int64_t get_index_column_num() const { return index_info_.get_size(); }
  inline int64_t get_partition_key_column_num() const { return partition_key_info_.get_size(); }
  inline int64_t get_subpartition_key_column_num() const { return subpartition_key_info_.get_size(); }
  inline ObTableLoadType get_load_type() const { return load_type_; }
  inline ObIndexUsingType get_index_using_type() const { return index_using_type_; }
  inline ObTableDefType get_def_type() const { return def_type_; }

  virtual inline const char *get_compress_func_name() const override { return all_compressor_name[compressor_type_]; }
  virtual inline common::ObCompressorType get_compressor_type() const override { return compressor_type_; }
  inline bool is_compressed() const { return compressor_type_ > ObCompressorType::NONE_COMPRESSOR; }
  inline common::ObStoreFormatType get_store_format() const { return store_format_; }
  virtual inline common::ObRowStoreType get_row_store_type() const override { return row_store_type_; }
  inline int64_t get_storage_format_version() const { return storage_format_version_; }
  inline const char *get_tablegroup_name_str() const { return extract_str(tablegroup_name_); }
  inline const common::ObString &get_tablegroup_name() const { return tablegroup_name_; }
  inline const char *get_comment() const { return extract_str(comment_); }
  inline const common::ObString &get_comment_str() const { return comment_; }
  inline const char *get_pk_comment() const { return extract_str(pk_comment_); }
  inline const common::ObString &get_pk_comment_str() const { return pk_comment_; }
  inline const char *get_create_host() const { return extract_str(create_host_); }
  inline const common::ObString &get_create_host_str() const { return create_host_; }
  inline const common::ObRowkeyInfo &get_rowkey_info() const { return rowkey_info_; }
  inline const common::ObRowkeyInfo &get_shadow_rowkey_info() const { return shadow_rowkey_info_; }
  inline const common::ObIndexInfo &get_index_info() const { return index_info_; }
  inline const common::ObPartitionKeyInfo &get_partition_key_info() const { return partition_key_info_; }
  inline const common::ObPartitionKeyInfo &get_subpartition_key_info() const { return subpartition_key_info_; }
  inline common::ObCharsetType get_charset_type() const { return charset_type_; }
  inline common::ObCollationType get_collation_type() const { return collation_type_; }
  inline common::ObNameCaseMode get_name_case_mode() const { return name_case_mode_; }
  inline int64_t get_code_version() const { return code_version_; }
  inline const common::ObString &get_expire_info() const { return expire_info_; }
  inline ObViewSchema &get_view_schema() { return view_schema_; }
  inline const ObViewSchema &get_view_schema() const { return view_schema_; }
  inline const common::ObString &get_ttl_definition() const { return ttl_definition_; }
  inline const common::ObString &get_kv_attributes() const { return kv_attributes_; }
  inline const common::ObString &get_index_params() const { return index_params_; }
  inline const common::ObString &get_exec_env() const { return exec_env_; }
  inline int64_t get_lob_inrow_threshold() const { return lob_inrow_threshold_; }
  inline int64_t get_auto_increment_cache_size() const { return auto_increment_cache_size_; }
  bool has_check_constraint() const;
  inline bool has_constraint() const { return cst_cnt_ > 0; }
  bool is_column_in_check_constraint(const uint64_t col_id) const;
  bool is_column_in_foreign_key(const uint64_t col_id) const;
  int is_column_in_partition_key(const uint64_t col_id, bool &is_in_partition_key) const;
  inline bool is_read_only() const { return read_only_; }
  inline const char *get_parser_name() const { return extract_str(parser_name_); }
  inline const common::ObString &get_parser_name_str() const { return parser_name_; }

  inline uint64_t get_index_attributes_set() const { return index_attributes_set_; }
  inline int64_t get_dop() const  { return table_dop_; }
  const ObString &get_external_file_location() const { return external_file_location_; }
  const ObString &get_external_file_location_access_info() const { return external_file_location_access_info_; }
  const ObString &get_external_file_format() const { return external_file_format_; }
  const ObString &get_external_file_pattern() const { return external_file_pattern_; }
  const ObString &get_external_properties() const { return external_properties_; }
  int64_t get_external_table_auto_refresh() const { return (table_flags_ >> EXTERNAL_TABLE_AUTO_REFRESH_FLAG_OFFSET) & ((1 << EXTERNAL_TABLE_AUTO_REFRESH_FLAG_BITS) - 1); }
  bool is_external_table_immediate_refresh() const { return get_external_table_auto_refresh() == 1; }
  bool is_external_table_interval_refresh() const { return get_external_table_auto_refresh() == 2; }
  bool is_external_table_auto_refresh_off() const { return get_external_table_auto_refresh() == 0; }
  inline void set_name_generated_type(const ObNameGeneratedType is_sys_generated) {
    name_generated_type_ = is_sys_generated;
  }
  inline ObNameGeneratedType get_name_generated_type() const { return name_generated_type_; }
  bool is_sys_generated_name(bool check_unknown) const;
  inline bool is_user_specified_partition_for_external_table() const { return (table_flags_ & EXTERNAL_TABLE_USER_SPECIFIED_PARTITION_FLAG) != 0; }
  inline bool is_index_visible() const
  {
    return 0 == (index_attributes_set_ & ((uint64_t)(1) << INDEX_VISIBILITY));
  }
  inline bool is_enable_row_movement() const
  {
    return 0 != (index_attributes_set_ & ((uint64_t)(1) << INDEX_ROW_MOVEABLE));
  }
  inline void reset_rowkey_info()
  {
    rowkey_column_num_ = 0;
    rowkey_info_.reset();
  }

  uint64 get_index_attributes_set() { return index_attributes_set_; }

  bool has_depend_table(uint64_t table_id) const;
  int get_orig_default_row(const common::ObIArray<share::schema::ObColDesc> &column_ids,
      common::ObNewRow &default_row) const;
  int get_orig_default_row(const common::ObIArray<share::schema::ObColDesc> &column_ids, blocksstable::ObDatumRow &default_row) const;
  int get_cur_default_row(const common::ObIArray<share::schema::ObColDesc> &column_ids,
      common::ObNewRow &default_row) const;
  void reset_column_info();
  inline int64_t get_column_count() const { return column_cnt_; }
  inline void reset_column_count() { column_cnt_ = 0; }
  void reset_column_group_info();
  inline int64_t get_column_group_count() const { return column_group_cnt_; }
  int get_is_row_store(bool &is_row_store) const;
  inline void reset_column_group_count() { column_group_cnt_ = 0; }
  inline int64_t get_constraint_count() const { return cst_cnt_; }
  inline bool get_micro_index_clustered() const { return micro_index_clustered_; }
  inline void set_micro_index_clustered(const bool micro_index_clustered) { micro_index_clustered_ = micro_index_clustered; }
  inline int64_t get_virtual_column_cnt() const { return virtual_column_cnt_; }
  inline const_column_iterator column_begin() const { return column_array_; }
  inline const_column_iterator column_end() const { return NULL == column_array_ ? NULL : &(column_array_[column_cnt_]); }
  inline const_constraint_iterator constraint_begin() const { return cst_array_; }
  inline const_constraint_iterator constraint_end() const { return NULL == cst_array_ ? NULL : &(cst_array_[cst_cnt_]); }
  inline constraint_iterator constraint_begin_for_non_const_iter() const { return cst_array_; }
  inline constraint_iterator constraint_end_for_non_const_iter() const { return NULL == cst_array_ ? NULL : &(cst_array_[cst_cnt_]); }
  inline const_column_group_iterator column_group_begin() const { return column_group_arr_; }
  inline const_column_group_iterator column_group_end() const { return NULL == column_group_arr_ ? NULL : &(column_group_arr_[column_group_cnt_]); }
  int fill_column_collation_info();
  int has_column(const uint64_t column_id, bool &has) const;
  int has_column(const ObString col_name, bool &has) const;
  int has_lob_column(bool &has_lob, const bool check_large = false) const;
  virtual int get_column_ids(common::ObIArray<uint64_t> &column_ids) const override;
  int get_index_and_rowkey_column_ids(common::ObIArray<uint64_t> &column_ids) const;
  virtual int get_column_ids(common::ObIArray<share::schema::ObColDesc> &column_ids, const bool no_virtual = false) const override;
  virtual int get_rowkey_column_ids(common::ObIArray<share::schema::ObColDesc> &column_ids) const override;
  virtual int set_precision_to_column_desc(common::ObIArray<share::schema::ObColDesc> &column_ids) const override;
  int get_rowkey_column_ids(common::ObIArray<uint64_t> &column_ids) const;
  int get_rowkey_partkey_column_ids(common::ObIArray<uint64_t> &column_ids) const;
  int get_column_ids_without_rowkey(common::ObIArray<share::schema::ObColDesc> &column_ids, const bool no_virtual = false) const;
  int get_generated_column_ids(common::ObIArray<uint64_t> &column_ids) const;
  inline bool has_generated_column() const { return generated_columns_.num_members() > 0; }
  int has_not_null_unique_key(ObSchemaGetterGuard &schema_guard, bool &bool_result) const;
  // The table has a generated column that is a partition key.
  bool has_generated_and_partkey_column() const;
  int check_is_stored_generated_column_base_column(uint64_t column_id, bool &is_stored_base_col) const;
  // Check whether the data table column has prefix index column deps.
  int check_prefix_index_columns_depend(const ObColumnSchemaV2 &data_column_schema, ObSchemaGetterGuard &schema_guard, bool &has_prefix_idx_col_deps) const;
  int check_functional_index_columns_depend(const ObColumnSchemaV2 &data_column_schema, ObSchemaGetterGuard &schema_guard, bool &has_prefix_idx_col_deps) const;
  int check_column_has_multivalue_index_depend(const ObColumnSchemaV2 &data_column_schema, bool &has_func_idx_col_deps) const;
  int add_base_table_id(uint64_t base_table_id) { return base_table_ids_.push_back(base_table_id); }
  int add_depend_table_id(uint64_t depend_table_id) { return depend_table_ids_.push_back(depend_table_id); }
  int add_depend_mock_fk_parent_table_id(uint64_t depend_table_id) { return depend_mock_fk_parent_table_ids_.push_back(depend_table_id); }
  const common::ObIArray<uint64_t>& get_base_table_ids() const { return base_table_ids_; }
  const common::ObIArray<uint64_t>& get_depend_table_ids() const { return depend_table_ids_; }
  const common::ObIArray<uint64_t>& get_depend_mock_fk_parent_table_ids() const { return depend_mock_fk_parent_table_ids_; }
  inline void set_define_user_id(const uint64_t user_id) { define_user_id_ = user_id; }
  inline uint64_t get_define_user_id() const { return define_user_id_; }

  // Return all vertical partition columns, including the vertical partition column that is the primary key
  int get_vp_column_ids(common::ObIArray<share::schema::ObColDesc> &column_ids) const;
  // Return all vertical partition columns, including primary key + vertical partition column
  int get_vp_store_column_ids(common::ObIArray<share::schema::ObColDesc> &column_ids) const;
  // Only used for the primary partition table, returns all vertical partition columns,
  // including the primary key + vertical partition column
  int get_vp_column_ids_with_rowkey(common::ObIArray<share::schema::ObColDesc> &column_ids,
      const bool no_virtual = false) const;
  int get_spatial_geo_column_id(uint64_t &geo_column_id) const;
  int get_spatial_index_column_ids(common::ObIArray<uint64_t> &column_ids) const;
  int get_fulltext_column_ids(uint64_t &doc_id_col_id, uint64_t &ft_col_id) const;
  int get_multivalue_column_id(uint64_t &multivalue_col_id) const;

  int get_vec_index_column_id(uint64_t &vec_vector_id) const;
  int get_vec_index_vid_col_id(uint64_t &vec_id_col_id) const;
  // get columns for building rowid
  int get_column_ids_serialize_to_rowid(common::ObIArray<uint64_t> &col_ids,
                                        int64_t &rowkey_cnt) const;

  int get_rowid_version(int64_t rowkey_cnt,
                        int64_t serialize_col_cnt,
                        int64_t &version) const;

  // only used by storage layer, return all columns that need to be stored in sstable
  // 1. for storage_index_table (user_index or mv):
  //    return all index columns plus rowkey (including virtual columns)
  // 2. for primary vp(vertical partition)
  //    2.1. is_minor = true,  return all not virtual columns for all table columns
  //    2.2. is_minor = false, return all not virtual columns for primary vp columns only
  // 3. for aux vp(vertical partition)
  //    return all not virtual columns defined in the current vertical partition plus rowkey
  // 4. for user table:
  //    return all not virtual columns of the current table
  // PLUS: filter is_not_included_in_minor_column from column ids when doing minor freeze(is_minor = true)

  virtual int get_store_column_ids(common::ObIArray<ObColDesc> &column_ids, const bool full_col = false) const override;
  // return the number of columns that will be stored in sstable
  // it is equal to the size of column_ids array returned by get_store_column_ids

  virtual int get_store_column_count(int64_t &column_count, const bool full_col = false) const override;

  // whether table should check merge progress
  int is_need_check_merge_progress(bool &need_check) const;
  int get_multi_version_column_descs(common::ObIArray<ObColDesc> &column_descs) const;
  virtual int get_skip_index_col_attr(common::ObIArray<ObSkipIndexColumnAttr> &skip_idx_attrs) const override;
  template <typename Allocator>
  static int build_index_table_name(Allocator &allocator,
                                    const uint64_t data_table_id,
                                    const common::ObString &index_name,
                                    common::ObString &index_table_name);

  //
  // materialized view related
  //
  int convert_to_depend_table_column(
    uint64_t column_id,
    uint64_t &convert_table_id,
    uint64_t &convert_column_id) const;

  bool is_depend_column(uint64_t column_id) const;

  bool has_table(uint64_t table_id) const;
  bool is_drop_index() const;
  void set_drop_index(const uint64_t drop_index_value);
  bool is_invisible_before() const;
  void set_invisible_before(const uint64_t invisible_before);

  //
  // column group related
  //
  bool is_column_store_supported() const { return is_column_store_supported_; }
  void set_column_store(const bool support_column_store) { is_column_store_supported_ = support_column_store; }
  int get_is_column_store(bool &is_column_store) const;
  uint64_t get_max_used_column_group_id() const { return max_used_column_group_id_; }
  uint64_t get_next_single_column_group_id() const { return max_used_column_group_id_ > ROWKEY_COLUMN_GROUP_ID ? max_used_column_group_id_ + 1 : ROWKEY_COLUMN_GROUP_ID + 1; }
  int check_is_normal_cgs_at_the_end(bool &is_normal_cgs_at_the_end) const;
  void set_max_used_column_group_id(const uint64_t id) { max_used_column_group_id_ = id; }
  int add_column_group(const ObColumnGroupSchema &other);
  // This function is only used when add default cg for sys_schema in 'hard-code' python script
  // or when we need to mock default column group for compatibility
  int add_default_column_group();

  int get_store_column_group_count(int64_t &column_group_cnt,
                                   const bool filter_empty_cg = true) const;
  int get_store_column_groups(ObIArray<const ObColumnGroupSchema *> &column_groups,
                              const bool filter_empty_cg = true) const;
  int remove_column_group(const uint64_t column_group_id);
  int has_all_column_group(bool &has_all_column_group) const;
  int has_non_default_column_group(bool &has_non_default_column_group) const;
  int adjust_column_group_array();
  // materialized view log related
  template <typename Allocator>
  static int build_mlog_table_name(Allocator &allocator,
                                   const common::ObString &base_table_name,
                                   common::ObString &mlog_table_name,
                                   const bool is_oracle_mode);

  //other methods
  int64_t get_convert_size() const;
  void reset();
  //int64_t to_string(char *buf, const int64_t buf_len) const;
  //whether the primary key or index is ordered
  inline bool is_ordered() const { return USING_BTREE == index_using_type_; }
  virtual int serialize_columns(char *buf, const int64_t data_len, int64_t &pos) const;
  virtual int deserialize_columns(const char *buf, const int64_t data_len, int64_t &pos);

  /**
   * FIXME: move to ObPartitionSchema
   * this function won't reset tablet_ids/partition_ids first, should be careful!!!
   *
   * first_level_part_ids represent the first level part id of subpartition,
   * otherwise its value is OB_INVALID_ID
   * e.g.
   *  PARTITION_LEVEL_ZERO
   *    - partition_id = table_id
   *    - first_level_part_id = OB_INVALID_ID
   *  PARTITION_LEVEL_ONE
   *    - partition_id = part_id
   *    - first_level_part_id = OB_INVALID_ID
   * PARTITION_LEVEL_TWO
   *    - partition_id = sub_part_id
   *    - first_level_part_id = part_id
  */
  int get_all_tablet_and_object_ids(common::ObIArray<ObTabletID> &tablet_ids,
                                    common::ObIArray<ObObjectID> &partition_ids,
                                    ObIArray<ObObjectID> *first_level_part_ids = NULL) const;

  virtual int alloc_partition(const ObPartition *&partition);
  virtual int alloc_partition(const ObSubPartition *&subpartition);
  int check_enable_split_partition(bool is_auto_partitioning) const;
  int check_validity_for_auto_partition() const;
  int check_can_do_manual_split_partition() const;
  ObPartitionLevel get_target_part_level_for_auto_partitioned_table() const;
  int get_part_func_expr_str(ObString &part_func_expr, common::ObIAllocator &allocator,
                             const bool using_auto_partitioned_mode) const;
  int get_presetting_partition_keys(common::ObIArray<uint64_t> &partition_key_ids) const;
  int get_partition_keys_by_part_func_expr(common::ObString &part_func_expr_str, common::ObIArray<uint64_t> &partition_key_ids) const;
  int extract_actual_index_rowkey_columns_name(ObIArray<ObString> &rowkey_columns_name) const;
  int is_presetting_partition_key(const uint64_t partition_key_id, bool &is_presetting_partition_key) const;
  int check_primary_key_cover_partition_column();
  int check_rowkey_cover_partition_keys(const common::ObPartitionKeyInfo &part_key);
  int check_index_table_cover_partition_keys(const common::ObPartitionKeyInfo &part_key) const;
  int check_create_index_on_hidden_primary_key(const ObTableSchema &index_table) const;
  int check_skip_index_valid() const;

  int get_subpart_ids(const int64_t part_id, common::ObIArray<int64_t> &subpart_ids) const;

  virtual int calc_part_func_expr_num(int64_t &part_func_expr_num) const;
  virtual int calc_subpart_func_expr_num(int64_t &subpart_func_expr_num) const;

  // checking the column is partition key or subpartition key.
  // if the ignore_presetting_key == true, the following functions are equal to that of ObColumnSchemaV2,
  // otherwise, they will check whether the column is presetting key.
  int is_tbl_partition_key(const uint64_t column_id, bool &result,
                           const bool ignore_presetting_key=true) const;
  int is_tbl_partition_key(const share::schema::ObColumnSchemaV2 &orig_column_schema,
                           bool& result,
                           const bool ignore_presetting_key=true) const;
  int is_partition_key(const share::schema::ObColumnSchemaV2 &orig_column_schema,
                       bool& result,
                       const bool ignore_presetting_key=true) const;
  int is_subpartition_key(const share::schema::ObColumnSchemaV2 &orig_column_schema,
                          bool& result,
                          const bool ignore_presetting_key=true) const;
  inline void reset_simple_index_infos() { simple_index_infos_.reset(); }
  inline const common::ObIArray<ObAuxTableMetaInfo> &get_simple_index_infos() const
  {
    return simple_index_infos_;
  }
  int get_simple_index_infos(
      common::ObIArray<ObAuxTableMetaInfo> &simple_index_infos_array) const;

  // Foreign key
  inline const common::ObIArray<ObForeignKeyInfo> &get_foreign_key_infos() const
  {
    return foreign_key_infos_;
  }
  inline common::ObIArray<ObForeignKeyInfo> &get_foreign_key_infos()
  {
    return foreign_key_infos_;
  }
  // This function is used in ObCodeGeneratorImpl::convert_foreign_keys
  // For self-referential foreign keys:
  //   In foreign_key_infos_.count() only counts a foreign key
  //   This function will count two foreign keys
  int64_t get_foreign_key_real_count() const;
  bool is_parent_table() const;
  bool is_child_table() const;
  bool is_foreign_key(uint64_t column_id) const;
  int add_foreign_key_info(const ObForeignKeyInfo &foreign_key_info);
  int remove_foreign_key_info(const uint64_t foreign_key_id);
  inline void reset_foreign_key_infos() { foreign_key_infos_.reset(); }
  int add_simple_index_info(const ObAuxTableMetaInfo &simple_index_info);

  int get_fk_check_index_tid(ObSchemaGetterGuard &schema_guard, const common::ObIArray<uint64_t> &parent_column_ids, uint64_t &scan_index_tid) const;
  int check_rowkey_column(const common::ObIArray<uint64_t> &parent_column_ids, bool &is_rowkey) const;

  // trigger
  inline const common::ObIArray<uint64_t> &get_trigger_list() const
  {
    return trigger_list_;
  }
  inline common::ObIArray<uint64_t> &get_trigger_list()
  {
    return trigger_list_;
  }
  inline void reset_trigger_list() { trigger_list_.reset(); }
  int has_before_insert_row_trigger(ObSchemaGetterGuard &schema_guard,
                                    bool &trigger_exist) const;
  int has_before_update_row_trigger(ObSchemaGetterGuard &schema_guard,
                                    bool &trigger_exist) const;
  int is_allow_parallel_of_trigger(ObSchemaGetterGuard &schema_guard,
                                    bool &is_forbid_parallel) const;

  //label security
  inline bool has_label_se_column() const { return label_se_column_ids_.count() > 0; }
  const common::ObIArray<uint64_t> &get_label_se_column_ids() const { return label_se_column_ids_; }

  // only for size_size test
  int set_column_encodings(const common::ObIArray<int64_t> &col_encodings);
  virtual int get_column_encodings(common::ObIArray<int64_t> &col_encodings) const override;

  int get_column_group_by_id(const uint64_t column_group_id, ObColumnGroupSchema *&column_group) const;
  int get_column_group_by_name(const ObString &cg_name, ObColumnGroupSchema *&column_group) const;
  int get_all_cg_type_column_group(const ObColumnGroupSchema *&column_group) const;
  int get_each_column_group(ObIArray<ObColumnGroupSchema*> &each_cgs) const;
  int is_partition_key_match_rowkey_prefix(bool &is_prefix) const;
  int is_presetting_partition_key_match_rowkey_prefix(bool &is_prefix) const;
  int get_column_group_index(const share::schema::ObColumnParam &param,
                             const bool need_calculate_cg_idx,
                             int32_t &cg_idx) const;
  int is_column_group_exist(const common::ObString &cg_name, bool &exist) const;

  int get_all_column_ids(ObIArray<uint64_t> &column_ids) const;
  virtual int init_column_meta_array(
      common::ObIArray<blocksstable::ObSSTableColumnMeta> &meta_array) const override;
  int check_column_can_be_altered_online(const ObColumnSchemaV2 *src_schema,
                                         ObColumnSchemaV2 *dst_schema) const;
  int check_column_can_be_altered_offline(const ObColumnSchemaV2 *src_schema,
                                          ObColumnSchemaV2 *dst_schema) const;
  int check_alter_column_is_offline(const ObColumnSchemaV2 *src_schema,
                                    ObColumnSchemaV2 *dst_schema,
                                    ObSchemaGetterGuard &schema_guard,
                                    bool &is_offline) const;
  int check_prohibition_rules(const ObColumnSchemaV2 &src_schema,
                              const ObColumnSchemaV2 &dst_schema,
                              ObSchemaGetterGuard &schema_guard,
                              const bool is_oracle_mode,
                              const bool is_offline) const;
  int check_ddl_type_change_rules(const ObColumnSchemaV2 &src_schema,
                                  const ObColumnSchemaV2 &dst_schema,
                                  ObSchemaGetterGuard &schema_guard,
                                  const bool is_oracle_mode,
                                  bool &is_offline) const;
  static int check_is_exactly_same_type(const ObColumnSchemaV2 &src_column,
                                        const ObColumnSchemaV2 &dst_column,
                                        bool &is_same);
  int check_alter_column_in_index(const ObColumnSchemaV2 &src_column,
                                  const ObColumnSchemaV2 &dst_column,
                                  ObSchemaGetterGuard &schema_guard,
                                  bool &is_in_index) const;
  int check_alter_column_in_rowkey(const ObColumnSchemaV2 &src_column,
                                   const ObColumnSchemaV2 &dst_column,
                                   bool &is_in_rowkey) const;
  int check_alter_column_accuracy(const ObColumnSchemaV2 &src_column,
                                  ObColumnSchemaV2 &dst_column,
                                  const int32_t src_col_byte_len,
                                  const int32_t dst_col_byte_len,
                                  const bool is_oracle_mode,
                                  bool &is_offline) const;
  int check_alter_column_type(const ObColumnSchemaV2 &src_column,
                              ObColumnSchemaV2 &dst_column,
                              const int32_t src_col_byte_len,
                              const int32_t dst_col_byte_len,
                              const bool is_oracle_mode,
                              bool &is_offline) const;


  int get_column_schema_in_same_col_group(uint64_t column_id, uint64_t udt_set_id,
                                          common::ObIArray<ObColumnSchemaV2 *> &column_group) const;
  ObColumnSchemaV2* get_xml_hidden_column_schema(uint64_t column_id, uint64_t udt_set_id) const;
  ObColumnSchemaV2* get_xml_hidden_column_parent_col_schema(uint64_t column_id, uint64_t udt_set_id) const;
  bool is_same_type_category(const ObColumnSchemaV2 &src_column,
                             const ObColumnSchemaV2 &dst_column) const;
  int check_has_trigger_on_table(ObSchemaGetterGuard &schema_guard,
                                 bool &is_enable,
                                 uint64_t trig_event = ObTriggerEvents::get_all_event()) const;
  int get_not_null_constraint_map(hash::ObHashMap<uint64_t, uint64_t> &cst_map) const;
  int is_need_padding_for_generated_column(bool &need_padding) const;
  int has_generated_column_using_udf_expr(bool &ans) const;
  int generate_new_column_id_map(common::hash::ObHashMap<uint64_t, uint64_t> &column_id_map) const;
  int convert_column_ids_for_ddl(const hash::ObHashMap<uint64_t, uint64_t> &column_id_map);
  int sort_column_array_by_column_id();
  int check_column_array_sorted_by_column_id(const bool skip_rowkey) const;
  int check_has_local_index(ObSchemaGetterGuard &schema_guard, bool &has_local_index) const;
  int check_has_fts_index(ObSchemaGetterGuard &schema_guard, bool &has_fts_index) const;
  int check_has_multivalue_index(ObSchemaGetterGuard &schema_guard, bool &has_multivalue_index) const;
  int check_has_vector_index(ObSchemaGetterGuard &schema_guard, bool &has_vector_index) const;
  int is_real_unique_index_column(ObSchemaGetterGuard &schema_guard,
                                  uint64_t column_id,
                                  bool &is_uni) const;
  int is_unique_key_column(ObSchemaGetterGuard &schema_guard,
                           uint64_t column_id,
                           bool &is_uni) const;
  int is_multiple_key_column(ObSchemaGetterGuard &schema_guard,
                             uint64_t column_id,
                             bool &is_mul) const;
  int get_doc_id_rowkey_tid(uint64_t &doc_id_rowkey_tid) const;
  int get_rowkey_doc_id_tid(uint64_t &rowkey_doc_id_tid) const;
  int get_vec_id_rowkey_tid(uint64_t &doc_id_rowkey_tid) const;
  void set_aux_lob_meta_tid(const uint64_t& table_id) { aux_lob_meta_tid_ = table_id; }
  void set_aux_lob_piece_tid(const uint64_t& table_id) { aux_lob_piece_tid_ = table_id; }
  int get_rowkey_doc_tid(uint64_t &index_table_id) const;
  int get_rowkey_vid_tid(uint64_t &index_table_id) const;
  uint64_t get_aux_lob_meta_tid() const { return aux_lob_meta_tid_; }
  uint64_t get_aux_lob_piece_tid() const { return aux_lob_piece_tid_; }
  bool has_lob_column() const;
  int64_t get_lob_columns_count() const;
  bool has_lob_aux_table() const { return (aux_lob_meta_tid_ != OB_INVALID_ID && aux_lob_piece_tid_ != OB_INVALID_ID); }
  bool has_mlog_table() const { return (OB_INVALID_ID != mlog_tid_); }
  bool required_by_mview_refresh() const { return has_mlog_table() || table_referenced_by_fast_lsm_mv(); }
  // ObColumnIterByPrevNextID's column id is not in order, it means table has add column instant and return true
  int has_add_column_instant(bool &add_column_instant) const;
  inline void add_table_flag(uint64_t flag) { table_flags_ |= flag; }
  inline void del_table_flag(uint64_t flag) { table_flags_ &= ~flag; }
  inline void add_or_del_table_flag(uint64_t flag, bool is_add)
  {
    if (is_add) {
      add_table_flag(flag);
    } else {
      del_table_flag(flag);
    }
  }
  inline bool has_table_flag(uint64_t flag) const { return table_flags_ & flag; }
  inline void set_table_flags(uint64_t flags) { table_flags_ = flags; }
  inline uint64_t get_table_flags() const { return table_flags_; }
  inline const common::ObIArray<uint64_t> &get_rls_policy_ids() const { return rls_policy_ids_; }
  inline common::ObIArray<uint64_t> &get_rls_policy_ids() { return rls_policy_ids_; }
  inline const common::ObIArray<uint64_t> &get_rls_group_ids() const { return rls_group_ids_; }
  inline common::ObIArray<uint64_t> &get_rls_group_ids() { return rls_group_ids_; }
  inline const common::ObIArray<uint64_t> &get_rls_context_ids() const { return rls_context_ids_; }
  inline common::ObIArray<uint64_t> &get_rls_context_ids() { return rls_context_ids_; }
  int assign_rls_objects(const ObTableSchema &other);
  inline void reset_rls_objecs()
  {
    rls_policy_ids_.reset();
    rls_group_ids_.reset();
    rls_context_ids_.reset();
  }
  void set_mlog_tid(const uint64_t& table_id) { mlog_tid_ = table_id; }
  uint64_t get_mlog_tid() const { return mlog_tid_; }
  inline sql::ObLocalSessionVar &get_local_session_var() { return local_session_vars_; }
  inline const sql::ObLocalSessionVar &get_local_session_var() const { return local_session_vars_; }
  inline void set_mv_mode(const int64_t mv_mode) { mv_mode_.mode_ = mv_mode; }
  inline int64_t get_mv_mode() const { return mv_mode_.mode_; }
  virtual int get_mv_mode_struct(ObMvMode &mv_mode) const override
  {
    mv_mode = mv_mode_;
    return OB_SUCCESS;
  }
  inline bool mv_major_refresh() const
  {
    return IS_MV_MAJOR_REFRESH == (enum ObMVMajorRefreshFlag)mv_mode_.mv_major_refresh_flag_;
  }
  inline void set_mv_major_refresh(const ObMVMajorRefreshFlag flag)
  {
    mv_mode_.mv_major_refresh_flag_ = flag;
  }
  inline bool table_referenced_by_fast_lsm_mv() const
  {
    return IS_REFERENCED_BY_FAST_LSM_MV ==
           (enum ObTableReferencedByFastLSMMVFlag)mv_mode_.table_referenced_by_fast_lsm_mv_flag_;
  }
  inline void set_table_referenced_by_fast_lsm_mv(const ObTableReferencedByFastLSMMVFlag flag)
  {
    mv_mode_.table_referenced_by_fast_lsm_mv_flag_ = flag;
  }
  DECLARE_VIRTUAL_TO_STRING;

protected:
  int add_col_to_id_hash_array(ObColumnSchemaV2 *column);
  int remove_col_from_id_hash_array(const ObColumnSchemaV2 *column);
  int add_col_to_name_hash_array(const bool is_oracle_mode,
                                 ObColumnSchemaV2 *column);
  int remove_col_from_name_hash_array(const bool is_oracle_mode,
                                      const ObColumnSchemaV2 *column);
  int add_col_to_column_array(ObColumnSchemaV2 *column);
  int remove_col_from_column_array(const ObColumnSchemaV2 *column);
  int add_column_update_prev_id(ObColumnSchemaV2 *local_column);
  int delete_column_update_prev_id(ObColumnSchemaV2 *column);

  int assign_column_group(const ObTableSchema &other);
  int do_add_column_group(const ObColumnGroupSchema &other);
  int add_column_group_to_array(ObColumnGroupSchema *column_group);
  template <typename KeyType, typename ArrayType>
  int add_column_group_to_hash_array(ObColumnGroupSchema *column_group,
                                     const KeyType &key,
                                     ArrayType *&array);

protected:
  // constraint related
  int add_cst_to_cst_array(ObConstraint *cst);
  int remove_cst_from_cst_array(const ObConstraint *cst);
  //label security
  int remove_column_id_from_label_se_array(const uint64_t column_id);

private:
  const IdHashArray *get_id_hash_array() const {return id_hash_array_;}
  const CgIdHashArray *get_cg_id_hash_array() const { return cg_id_hash_arr_; }

  int insert_col_to_column_array(ObColumnSchemaV2 *column);
  int get_default_row(
      get_default_value func,
      const common::ObIArray<share::schema::ObColDesc> &column_ids,
      common::ObNewRow &default_row) const;
  inline int64_t get_id_hash_array_mem_size(const int64_t column_cnt) const;
  inline int64_t get_name_hash_array_mem_size(const int64_t column_cnt) const;
  template <typename ArrayType>
  int64_t get_hash_array_mem_size(const int64_t element_cnt) const;
  int delete_column_internal(ObColumnSchemaV2 *column_schema, const bool for_view);
  ObColumnSchemaV2 *get_column_schema_by_id_internal(const uint64_t column_id) const;
  ObColumnSchemaV2 *get_column_schema_by_name_internal(const common::ObString &column_name) const;
  int check_rowkey_column_can_be_altered(const ObColumnSchemaV2 *src_schema,
                                         const ObColumnSchemaV2 *dst_schema) const;
  int check_row_length(const bool is_oracle_mode,
                       const ObColumnSchemaV2 *src_schema,
                       const ObColumnSchemaV2 *dst_schema) const;
  ObConstraint *get_constraint_internal(
      std::function<bool(const ObConstraint *val)> func);
  const ObConstraint *get_constraint_internal(
      std::function<bool(const ObConstraint *val)> func) const;
  int check_alter_column_in_foreign_key(const ObColumnSchemaV2 &src_schema,
                                        const ObColumnSchemaV2 &dst_schema,
                                        const bool is_oracle_mode) const;
  int convert_char_to_byte_semantics(const ObColumnSchemaV2 *col_schema,
                                     const bool is_oracle_mode,
                                     int32_t &col_byte_len) const;
  int check_need_convert_id_hash_array(bool &need_convert_id_hash_array) const;
  int convert_basic_column_ids(
      const common::hash::ObHashMap<uint64_t, uint64_t> &column_id_map);
  int convert_autoinc_column_id(
      const common::hash::ObHashMap<uint64_t, uint64_t> &column_id_map);
  int convert_column_ids_in_generated_columns(
      const common::hash::ObHashMap<uint64_t, uint64_t> &column_id_map);
  int convert_column_ids_in_constraint(
      const common::hash::ObHashMap<uint64_t, uint64_t> &column_id_map);
  int convert_column_udt_set_ids(
      const common::hash::ObHashMap<uint64_t, uint64_t> &column_id_map);
  int convert_geo_generated_col_ids(
      const common::hash::ObHashMap<uint64_t, uint64_t> &column_id_map);
  static int convert_column_ids_in_info(
      const common::hash::ObHashMap<uint64_t, uint64_t> &column_id_map,
      ObRowkeyInfo &rowkey_info);
  int alter_view_column_internal(ObColumnSchemaV2 &column_schema);
  int add_partition_key_(ObColumnSchemaV2 &column);
  int get_base_rowkey_column_group_index(int32_t &cg_idx) const;
  int calc_column_group_index_(const uint64_t column_id, int32_t &cg_idx) const;

protected:
  uint64_t max_used_column_id_;
  // Only temporary table settings, according to the last active time of the session
  // to determine whether the table needs to be cleaned up;
  int64_t sess_active_time_;
  int64_t rowkey_column_num_;
  int64_t index_column_num_;
  int64_t rowkey_split_pos_;//not used so far;reserved
  int64_t part_key_column_num_;
  int64_t subpart_key_column_num_;
  int64_t block_size_; //KB
  bool is_use_bloomfilter_; //used for prebuild bloomfilter when merge
  int64_t progressive_merge_num_;
  int64_t tablet_size_;
  int64_t pctfree_;
  uint64_t autoinc_column_id_;
  uint64_t auto_increment_;
  bool read_only_;
  ObTableLoadType load_type_; // not used yet
  ObIndexUsingType index_using_type_;
  ObTableDefType def_type_;
  common::ObCharsetType charset_type_;//default:utf8mb4
  common::ObCollationType collation_type_;//default:utf8mb4_general_ci
  int64_t code_version_;//for compatible use, the version of the whole schema system

  //just use one uint64 to store index attributes,
  // The lowest bit indicates the visibility of the index, the default value is 0, which means the index is visible,
  // and 1 means the index is invisible
  uint64_t index_attributes_set_;

  common::ObString tablegroup_name_;
  common::ObString comment_;
  common::ObString pk_comment_;
  common::ObString create_host_;
  common::ObCompressorType compressor_type_;
  common::ObString expire_info_;
  common::ObString parser_name_; //fulltext index parser name
  common::ObRowStoreType row_store_type_;
  common::ObStoreFormatType store_format_;
  int64_t storage_format_version_;
  int64_t progressive_merge_round_;

  //view schema
  ObViewSchema view_schema_;

  // all base table ids for materialized view
  common::ObArray<uint64_t> base_table_ids_;
  common::ObSArray<uint64_t> depend_table_ids_;

  common::ObSArray<ObAuxTableMetaInfo> simple_index_infos_;

  // aux_vp_tid_array_ also contains the primary partition id, which is the primary table itself
  common::ObSArray<uint64_t> aux_vp_tid_array_;

  // Should encapsulate an Array structure, push calls T (allocator) construction
  int64_t column_cnt_;
  ObColumnSchemaV2 **column_array_;

  int64_t column_array_capacity_;
  //generated data
  common::ObRowkeyInfo rowkey_info_;
  common::ObRowkeyInfo shadow_rowkey_info_;
  common::ObIndexInfo index_info_;
  common::ObPartitionKeyInfo partition_key_info_;
  common::ObPartitionKeyInfo subpartition_key_info_;
  IdHashArray *id_hash_array_;
  NameHashArray *name_hash_array_;
  ColumnReferenceSet generated_columns_;
  int64_t virtual_column_cnt_;

  // constraint related
  ObConstraint **cst_array_;
  int64_t cst_array_capacity_;
  int64_t cst_cnt_;
  common::ObSArray<ObForeignKeyInfo> foreign_key_infos_;
  common::ObArray<uint64_t> label_se_column_ids_;

  // trigger
  common::ObSArray<uint64_t> trigger_list_;

  // table dop
  int64_t table_dop_;
  uint64_t define_user_id_;

  // table id for aux lob table
  uint64_t aux_lob_meta_tid_;
  uint64_t aux_lob_piece_tid_;

  common::ObSArray<uint64_t> depend_mock_fk_parent_table_ids_;

  uint64_t table_flags_;

  // rls
  common::ObSArray<uint64_t> rls_policy_ids_;
  common::ObSArray<uint64_t> rls_group_ids_;
  common::ObSArray<uint64_t> rls_context_ids_;

  //external table
  common::ObString external_file_format_;
  common::ObString external_file_location_;
  common::ObString external_file_location_access_info_;
  common::ObString external_file_pattern_;
  common::ObString external_properties_;

  // table ttl
  common::ObString ttl_definition_;
  // kv attributes
  common::ObString kv_attributes_;

  ObNameGeneratedType name_generated_type_;
  int64_t lob_inrow_threshold_;
  int64_t auto_increment_cache_size_;
  bool micro_index_clustered_;

  // column group
  bool is_column_store_supported_;
  uint64_t max_used_column_group_id_;
  int64_t column_group_cnt_;
  ObColumnGroupSchema **column_group_arr_;
  int64_t column_group_arr_capacity_;
  CgIdHashArray *cg_id_hash_arr_;
  CgNameHashArray *cg_name_hash_arr_;
  uint64_t mlog_tid_;
  sql::ObLocalSessionVar local_session_vars_;
  // vector index
  common::ObString index_params_;
  // exec_env
  common::ObString exec_env_;
  ObMvMode mv_mode_;
};

class ObPrintableTableSchema final : public ObTableSchema
{
public:
  DECLARE_VIRTUAL_TO_STRING;
private:
  ObPrintableTableSchema() = delete;
};

// The data storage form of the index is local storage, that is,
// the storage of the index and the main table are put together
inline bool ObSimpleTableSchemaV2::is_index_local_storage() const
{
  return USER_INDEX == table_type_
         && schema::is_index_local_storage(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_global_index_table() const
{
  return is_global_index_table(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_global_index_table(const ObIndexType index_type)
{
  return INDEX_TYPE_NORMAL_GLOBAL == index_type
        || INDEX_TYPE_UNIQUE_GLOBAL == index_type
        || INDEX_TYPE_SPATIAL_GLOBAL == index_type
        || is_global_fts_index(index_type);
}

inline bool ObSimpleTableSchemaV2::is_global_normal_index_table() const
{
  return INDEX_TYPE_NORMAL_GLOBAL == index_type_;
}

inline bool ObSimpleTableSchemaV2::is_global_unique_index_table() const
{
  return INDEX_TYPE_UNIQUE_GLOBAL == index_type_;
}

inline bool ObSimpleTableSchemaV2::is_global_unique_index_table(const ObIndexType index_type)
{
  return INDEX_TYPE_UNIQUE_GLOBAL == index_type;
}

inline bool ObSimpleTableSchemaV2::is_local_unique_index_table() const
{
  //
  return share::schema::is_local_unique_index_table(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_global_local_index_table() const
{
  return INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE == index_type_
         || INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE == index_type_
         || INDEX_TYPE_SPATIAL_GLOBAL_LOCAL_STORAGE == index_type_
         || is_global_local_fts_index(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_spatial_index(const ObIndexType index_type)
{
  return INDEX_TYPE_SPATIAL_LOCAL == index_type
         || INDEX_TYPE_SPATIAL_GLOBAL == index_type
         || INDEX_TYPE_SPATIAL_GLOBAL_LOCAL_STORAGE == index_type;
}

inline bool ObSimpleTableSchemaV2::is_spatial_index() const
{
  return is_spatial_index(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_multivalue_index() const
{
  return share::schema::is_multivalue_index(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_multivalue_index_aux() const
{
  return share::schema::is_multivalue_index_aux(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_vec_index() const
{
  return share::schema::is_vec_index(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_vec_index(const ObIndexType index_type)
{
  return share::schema::is_vec_index(index_type);
}

inline bool ObSimpleTableSchemaV2::is_built_in_vec_index() const
{
  return share::schema::is_built_in_vec_index(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_vec_rowkey_vid_type() const
{
  return share::schema::is_vec_rowkey_vid_type(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_vec_vid_rowkey_type() const
{
  return share::schema::is_vec_vid_rowkey_type(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_vec_delta_buffer_type() const
{
  return share::schema::is_vec_delta_buffer_type(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_vec_index_id_type() const
{
  return share::schema::is_vec_index_id_type(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_vec_index_snapshot_data_type() const
{
  return share::schema::is_vec_index_snapshot_data_type(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_fts_index() const
{
  return share::schema::is_fts_index(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_built_in_fts_index() const
{
  return share::schema::is_built_in_fts_index(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_built_in_multivalue_index() const
{
  return share::schema::is_built_in_multivalue_index(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_built_in_index() const
{
  return share::schema::is_built_in_index(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_rowkey_doc_id() const
{
  return share::schema::is_rowkey_doc_aux(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_doc_id_rowkey() const
{
  return share::schema::is_doc_rowkey_aux(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_fts_index_aux() const
{
  return share::schema::is_fts_index_aux(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_fts_doc_word_aux() const
{
  return share::schema::is_fts_doc_word_aux(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_normal_index() const
{
  return INDEX_TYPE_NORMAL_LOCAL == index_type_
         || INDEX_TYPE_NORMAL_GLOBAL == index_type_
         || INDEX_TYPE_NORMAL_GLOBAL_LOCAL_STORAGE == index_type_;
}

inline bool ObSimpleTableSchemaV2::is_unique_index() const
{
  return is_unique_index(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_unique_index(ObIndexType index_type)
{
  return INDEX_TYPE_UNIQUE_LOCAL == index_type
         || INDEX_TYPE_UNIQUE_GLOBAL == index_type
         || INDEX_TYPE_UNIQUE_GLOBAL_LOCAL_STORAGE == index_type
         || INDEX_TYPE_UNIQUE_MULTIVALUE_LOCAL == index_type;
}

inline bool ObSimpleTableSchemaV2::is_domain_index() const
{
  return is_domain_index(index_type_);
}

inline bool ObSimpleTableSchemaV2::is_domain_index(const ObIndexType index_type)
{
  return is_spatial_index(index_type) ||
         share::schema::is_fts_index_aux(index_type) ||
         share::schema::is_fts_doc_word_aux(index_type) ||
         share::schema::is_multivalue_index_aux(index_type) ||
         share::schema::is_vec_index_id_type(index_type) ||
         share::schema::is_vec_delta_buffer_type(index_type) ||
         share::schema::is_vec_index_snapshot_data_type(index_type);
}

inline bool ObSimpleTableSchemaV2::is_fts_or_multivalue_index() const
{
  return share::schema::is_fts_or_multivalue_index(index_type_);
}

inline bool ObSimpleTableSchemaV2::should_not_validate_data_index_ckm() const
{
  // spatial index column is different from data table column, should not validate data & index column checksum
  // fulltext index cannot validate data by simply column checksum comparision
  // multi-value index column is different from data table column, should not validate data & index column checksum
  return is_domain_index();
}

inline bool ObSimpleTableSchemaV2::should_check_major_merge_progress() const
{
  // only include primary table except aux lob tables
  // aux lob tables only need to perform replicas verification, but don't need to be verified with primary table
  return is_normal_schema() && (is_sys_table()
          || is_user_table()
          || is_tmp_table()
          || is_aux_lob_table());
}

inline int64_t ObTableSchema::get_id_hash_array_mem_size(const int64_t column_cnt) const
{
  return common::max(IdHashArray::MIN_HASH_ARRAY_ITEM_COUNT,
    column_cnt * 2) * sizeof(void*) + sizeof(IdHashArray);
}

inline int64_t ObTableSchema::get_name_hash_array_mem_size(const int64_t column_cnt) const
{
  return common::max(NameHashArray::MIN_HASH_ARRAY_ITEM_COUNT,
    column_cnt * 2) * sizeof(void *) + sizeof(NameHashArray);
}

template <typename ArrayType>
int64_t ObTableSchema::get_hash_array_mem_size(const int64_t element_cnt) const
{
  return common::max(NameHashArray::MIN_HASH_ARRAY_ITEM_COUNT,
    element_cnt * 2) * sizeof(void *) + sizeof(ArrayType);
}

template <typename Allocator>
int ObTableSchema::build_index_table_name(Allocator &allocator,
                                          const uint64_t data_table_id,
                                          const common::ObString &index_name,
                                          common::ObString &index_table_name)
{
  int ret = common::OB_SUCCESS;
  int nwrite = 0;
  const int64_t buf_size = 64;
  char buf[buf_size];
  if ((nwrite = snprintf(buf, buf_size, "%lu", data_table_id)) >= buf_size || nwrite < 0) {
    ret = common::OB_BUF_NOT_ENOUGH;
    SHARE_SCHEMA_LOG(WARN, "buf is not large enough", K(buf_size), K(data_table_id), K(ret));
  } else {
    common::ObString table_id_str = common::ObString::make_string(buf);
    int32_t src_len = table_id_str.length() + index_name.length()
        + static_cast<int32_t>(strlen(common::OB_INDEX_PREFIX)) + 1;
    char *ptr = NULL;
    //TODO(jingqian): refactor following code, use snprintf instead
    if (OB_UNLIKELY(0 >= src_len)) {
      index_table_name.assign(NULL, 0);
    } else if (NULL == (ptr = static_cast<char *>(allocator.alloc(src_len)))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      SHARE_SCHEMA_LOG(WARN, "alloc memory failed", K(ret), "size", src_len);
    } else {
      int64_t pos = 0;
      MEMCPY(ptr + pos, common::OB_INDEX_PREFIX, strlen(common::OB_INDEX_PREFIX));
      pos += strlen(common::OB_INDEX_PREFIX);
      MEMCPY(ptr + pos, table_id_str.ptr(), table_id_str.length());
      pos += table_id_str.length();
      MEMCPY(ptr + pos, "_", 1);
      pos += 1;
      MEMCPY(ptr + pos, index_name.ptr(), index_name.length());
      pos += index_name.length();
      if (pos == src_len) {
        index_table_name.assign_ptr(ptr, src_len);
      } else {
        ret = common::OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "length mismatch", K(ret));
      }
    }
  }

  return ret;
}

template <typename Allocator>
int ObSimpleTableSchemaV2::get_index_name(Allocator &allocator, uint64_t table_id,
    const common::ObString &src, common::ObString &dst)
{
  int ret = common::OB_SUCCESS;
  common::ObString::obstr_size_t dst_len = 0;
  char *ptr = NULL;
  common::ObString::obstr_size_t pos = 0;
  const int64_t BUF_SIZE = 64; //table_id max length
  char table_id_buf[BUF_SIZE] = {'\0'};
  if (common::OB_INVALID_ID == table_id || src.empty()) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "invalid argument", K(ret), K(table_id), K(src));
  } else {
    int64_t n = snprintf(table_id_buf, BUF_SIZE, "%lu", table_id);
    if (n < 0 || n >= BUF_SIZE) {
      ret = common::OB_BUF_NOT_ENOUGH;
      SHARE_SCHEMA_LOG(WARN, "buffer not enough", K(ret), K(n), LITERAL_K(BUF_SIZE));
    } else {
      common::ObString table_id_str = common::ObString::make_string(table_id_buf);
      pos += static_cast<int32_t>(strlen(common::OB_INDEX_PREFIX));
      pos += table_id_str.length();
      pos += 1;
      dst_len = src.length() - pos;
      if (OB_UNLIKELY(0 >= dst_len)) {
        dst.assign(NULL, 0);
      } else if (NULL == (ptr = static_cast<char *>(allocator.alloc(dst_len)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        SHARE_SCHEMA_LOG(WARN, "alloc memory failed", K(ret), "size", dst_len);
      } else {
        MEMCPY(ptr, src.ptr() + pos, dst_len);
        dst.assign_ptr(ptr, dst_len);
      }
    }
  }
  return ret;
}

template<typename ColumnType>
int ObTableSchema::add_column(const ColumnType &column)
{
  using namespace common;
  int ret = common::OB_SUCCESS;
  char *buf = NULL;
  ColumnType *local_column = NULL;
  bool is_oracle_mode = false;
  const char* thread_name = ob_get_origin_thread_name();
  const bool in_replay_thread = OB_NOT_NULL(thread_name)
                                && 0 == STRCMP(thread_name, REPLAY_SERVICE_THREAD_NAME);
  const uint64_t mtl_tenant_id = MTL_ID();
  if (!column.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "The column is not valid", KR(ret));
  } else if (is_user_table() && column_cnt_ > common::OB_USER_ROW_MAX_COLUMNS_COUNT) {
    ret = common::OB_ERR_TOO_MANY_COLUMNS;
  } else if (column.is_autoincrement() && (autoinc_column_id_ != 0)
             && (autoinc_column_id_ != column.get_column_id())) {
    ret = common::OB_ERR_WRONG_AUTO_KEY;
    SHARE_SCHEMA_LOG(WARN, "Only one auto increment row is allowed", KR(ret));
  } else if (NULL == (buf = static_cast<char*>(alloc(sizeof(ColumnType))))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    SHARE_SCHEMA_LOG(ERROR, "Fail to allocate memory", KR(ret), "size", sizeof(ColumnType));
  } else if (static_cast<int64_t>(table_id_) > 0
             && is_ls_reserved_table(table_id_)) {
    // create or replay ls inner tablet (which should use tenant's compat mode
    if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(mtl_tenant_id, is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "check if_oracle_compat_mode failed",
                       KR(ret), K(mtl_tenant_id), K(tenant_id_), K(table_id_));
    }
  } else if (in_replay_thread) {
    // For replay thread in physical restore/standby tenant,
    // either tenant_id which is persisted in log or lib::is_oracle_mode() is not credible.
    // Compat mode should be fetched by MTL_ID().
    // bugfix: 52769689
    if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_table_id(mtl_tenant_id, table_id_, is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "check if_oracle_compat_mode failed",
                       KR(ret), K(mtl_tenant_id), K(tenant_id_), K(table_id_));
    }
  } else if (static_cast<int64_t>(table_id_) > 0) {
    if (OB_FAIL(check_if_oracle_compat_mode(is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "failed to get tenant's compat_mode",
                       KR(ret), K(mtl_tenant_id), K(tenant_id_), K(table_id_));
    }
  } else if (static_cast<int64_t>(table_id_) <= 0) {
    // arg deserialize or ddl resolver
    if (OB_FAIL(ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(tenant_id_, is_oracle_mode))) {
      SHARE_SCHEMA_LOG(WARN, "check if_oracle_compat_mode failed",
                       KR(ret), K(mtl_tenant_id), K(tenant_id_), K(table_id_));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!is_view_table() && OB_FAIL(check_row_length(is_oracle_mode, NULL, &column))) {
    SHARE_SCHEMA_LOG(WARN, "check row length failed", KR(ret), K(tenant_id_), K(table_id_), K(column));
  } else {
    if (NULL == (local_column = new (buf) ColumnType(allocator_))) {
      ret = common::OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "Fail to new local_column", KR(ret));
    } else {
      if (OB_FAIL(local_column->assign(column))) {
        SHARE_SCHEMA_LOG(WARN, "failed copy assign column", KR(ret), K(column));
      } else if (FALSE_IT(local_column->set_table_id(table_id_))) {
      } else if (!local_column->is_valid()) {
        ret = common::OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "The local column is not valid", KR(ret));
      } else if (OB_FAIL(add_column_update_prev_id(local_column))) {
        SHARE_SCHEMA_LOG(WARN, "Fail to update previous next column id", KR(ret));
      } else if (OB_FAIL(add_col_to_id_hash_array(local_column))) {
        SHARE_SCHEMA_LOG(WARN, "Fail to add column to id_hash_array", KR(ret));
      } else if (OB_FAIL(add_col_to_name_hash_array(is_oracle_mode, local_column))) {
        SHARE_SCHEMA_LOG(WARN, "Fail to add column to name_hash_array", KR(ret));
      } else if (OB_FAIL(add_col_to_column_array(local_column))) {
        SHARE_SCHEMA_LOG(WARN, "Fail to push column to array", KR(ret));
      } else {
        if (column.is_rowkey_column()) {
          if (OB_FAIL(set_rowkey_info(column))) {
            SHARE_SCHEMA_LOG(WARN, "set rowkey info to table schema failed", KR(ret));
          }
        }
        if (OB_SUCC(ret) && column.is_index_column()) {
          common::ObIndexColumn index_column;
          index_column.column_id_ = column.get_column_id();
          index_column.length_ = column.get_data_length();
          index_column.type_ = column.get_meta_type();
          index_column.fulltext_flag_ = column.is_fulltext_column();
          index_column.spatial_flag_ = column.is_spatial_generated_column();
          index_column.multivalue_flag_ = column.is_multivalue_generated_column();
          if (index_column.type_.is_decimal_int()) {
            index_column.type_.set_scale(column.get_accuracy().get_scale());
          }
          if (OB_FAIL(index_info_.set_column(column.get_index_position() - 1, index_column))) {
            SHARE_SCHEMA_LOG(WARN, "Fail to set column to index info", KR(ret));
          } else {
            if (index_column_num_ < index_info_.get_size()) {
              index_column_num_ = index_info_.get_size();
              if (is_user_table() && index_column_num_ > common::OB_MAX_ROWKEY_COLUMN_NUMBER) {
                ret = common::OB_ERR_TOO_MANY_ROWKEY_COLUMNS;
                LOG_USER_ERROR(OB_ERR_TOO_MANY_ROWKEY_COLUMNS, common::OB_MAX_ROWKEY_COLUMN_NUMBER);
              }
            }
          }
        }
        if (OB_SUCC(ret) && column.is_generated_column()) {
          if (OB_FAIL(generated_columns_.add_member(column.get_column_id() - common::OB_APP_MIN_COLUMN_ID))) {
            SHARE_SCHEMA_LOG(WARN, "add column id to generated columns failed", KR(ret), K(column));
          } else if (!column.is_column_stored_in_sstable()) {
            ++virtual_column_cnt_;
          }
        }
        if (OB_SUCC(ret) && column.is_label_se_column()) {
          if (OB_FAIL(label_se_column_ids_.push_back(column.get_column_id()))) {
            SHARE_SCHEMA_LOG(WARN, "fail to do array push back", KR(ret), K_(label_se_column_ids));
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (column.is_tbl_part_key_column()) {
          local_column->set_tbl_part_key_pos(column.get_tbl_part_key_pos());
          common::ObPartitionKeyColumn partition_key_column;
          construct_partition_key_column(column, partition_key_column);

          if (column.is_part_key_column()) {
            if (OB_FAIL(partition_key_info_.set_column(column.get_part_key_pos() - 1,
                    partition_key_column))) {
              SHARE_SCHEMA_LOG(WARN, "Failed to set partition coumn", KR(ret));
            } else {
              part_key_column_num_ = partition_key_info_.get_size();
            }
          }

          if (OB_SUCC(ret)) {
            if (column.is_subpart_key_column()) {
              if (OB_FAIL(subpartition_key_info_.set_column(column.get_subpart_key_pos() - 1,
                partition_key_column))) {
                SHARE_SCHEMA_LOG(WARN, "Failed to set subpartition column", KR(ret));
              } else {
                subpart_key_column_num_ = subpartition_key_info_.get_size();
              }
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (column.is_autoincrement()) {
          autoinc_column_id_ = column.get_column_id();
        }
      }
    }
  }

  // add shadow rowkey info
  if (OB_SUCC(ret)) {
    int64_t shadow_pk_pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info_.get_size(); ++i) {
      const common::ObRowkeyColumn *tmp_column =  NULL;
      if (NULL == (tmp_column = rowkey_info_.get_column(i))) {
        ret = common::OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "the column is NULL, ", KR(ret), K(i));
      } else if (is_shadow_column(tmp_column->column_id_)) {
        if (OB_FAIL(shadow_rowkey_info_.set_column(shadow_pk_pos, *tmp_column))) {
          SHARE_SCHEMA_LOG(WARN, "fail to set column to shadow rowkey info", KR(ret), KPC(tmp_column));
        } else {
          ++shadow_pk_pos;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if ((column.get_column_id() > get_max_used_column_id())
        && (column.get_column_id() <= common::OB_MAX_TMP_COLUMN_ID)) {
      set_max_used_column_id(column.get_column_id());
    }
  }
  if (OB_FAIL(ret)) {
    SHARE_SCHEMA_LOG(WARN, "add column failed", KR(ret), K(mtl_tenant_id),
                     K(tenant_id_), K(table_id_), K(is_oracle_mode), K(in_replay_thread),
                     "thead_name", OB_NOT_NULL(thread_name) ? thread_name : "NULL", K(column));
  } else {
    SHARE_SCHEMA_LOG(TRACE, "add column success", KR(ret), K(mtl_tenant_id),
                     K(tenant_id_), K(table_id_), K(is_oracle_mode), K(in_replay_thread),
                     "thead_name", OB_NOT_NULL(thread_name) ? thread_name : "NULL", K(column));
  }
  return ret;
}

template <typename KeyType, typename ArrayType>
int ObTableSchema::add_column_group_to_hash_array(
    ObColumnGroupSchema *column_group,
    const KeyType &key,
    ArrayType *&array)
{
  int ret = common::OB_SUCCESS;
  char *buf = NULL;
  int hash_ret = 0;
  int64_t arr_mem_size = 0;
  if (OB_ISNULL(column_group)) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "invalid argument", KR(ret));
  } else if (!column_group->is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    SHARE_SCHEMA_LOG(WARN, "column_group is invalid", KR(ret), KPC(column_group));
  } else {
    if (OB_ISNULL(array)) {
      arr_mem_size = get_hash_array_mem_size<ArrayType>(get_column_group_count());
      if (OB_ISNULL(buf = static_cast<char*>(alloc(arr_mem_size)))) {
        ret = common::OB_ALLOCATE_MEMORY_FAILED;
        SHARE_SCHEMA_LOG(ERROR, "fail to allocate memory for hash array", KR(ret), K(arr_mem_size));
      } else if (OB_ISNULL(array = new (buf) ArrayType(arr_mem_size))){
        ret = common::OB_ERR_UNEXPECTED;
        SHARE_SCHEMA_LOG(WARN, "fail to new hash array", KR(ret));
      } else {
        if (common::OB_SUCCESS != (hash_ret = array->set_refactored(key, column_group))) {
          ret = hash_ret;
          SHARE_SCHEMA_LOG(WARN, "fail to set column_group to hash array", KR(ret), KPC(column_group));
        }
      }
    } else if (common::OB_SUCCESS != (hash_ret = array->set_refactored(key, column_group))) {
      if (common::OB_HASH_FULL == hash_ret) {
        arr_mem_size = get_hash_array_mem_size<ArrayType>(array->count() * 2);
        // if reserved size is not enough, alloc two times more memory
        if (OB_ISNULL(buf = static_cast<char*>(alloc(arr_mem_size)))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          SHARE_SCHEMA_LOG(ERROR, "fail to alloc memory", KR(ret), K(arr_mem_size));
        } else {
          ArrayType *new_array = new (buf) ArrayType(arr_mem_size);
          if (OB_ISNULL(new_array)) {
            ret = common::OB_ERR_UNEXPECTED;
            SHARE_SCHEMA_LOG(WARN, "fail to new hash array", KR(ret));
          }
          typename ArrayType::Iterator iter;
          for (iter = array->begin(); OB_SUCC(ret) && iter != array->end(); ++iter) {
            if (OB_FAIL(new_array->set_refactored(array->get_key(iter), *iter))) {
              SHARE_SCHEMA_LOG(WARN, "fail to set refactored", KR(ret), K(*iter));
            }
          }
          if (OB_SUCC(ret)) {
            if (common::OB_SUCCESS != (hash_ret = new_array->set_refactored(key, column_group))) {
              ret = hash_ret;
              SHARE_SCHEMA_LOG(WARN, "fail to add column_group into hash array", KR(hash_ret), KPC(column_group));
            } else {
              // free old hash array
              free(array);
              array = new_array;
            }
          }
        }
      } else {
        ret = hash_ret;
        SHARE_SCHEMA_LOG(WARN, "fail to add column_group into hash array", KR(ret), KPC(column_group));
      }
    }
  }
  return ret;
}

template <typename Allocator>
int ObTableSchema::build_mlog_table_name(Allocator &allocator,
                                         const common::ObString &base_table_name,
                                         common::ObString &mlog_table_name,
                                         const bool is_oracle_mode)
{
  int ret = OB_SUCCESS;
  const ObString prefix(is_oracle_mode ? common::OB_MLOG_PREFIX_ORACLE : common::OB_MLOG_PREFIX_MYSQL);
  int32_t buf_len = prefix.length() + base_table_name.length() + 1;
  char *name_buf = nullptr;
  if (OB_ISNULL(name_buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SHARE_SCHEMA_LOG(WARN, "failed to allocate memory", KR(ret));
  } else {
    name_buf[buf_len - 1] = '\0';
    mlog_table_name.assign_buffer(name_buf, buf_len);
    if (prefix.length() != mlog_table_name.write(prefix.ptr(), prefix.length())) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "failed to write string", KR(ret));
    } else if (base_table_name.length() !=
        mlog_table_name.write(base_table_name.ptr(), base_table_name.length())) {
      ret = OB_ERR_UNEXPECTED;
      SHARE_SCHEMA_LOG(WARN, "failed to write string", KR(ret));
    }
  }
  return ret;
}

class ObColumnIterByPrevNextID
{
public:
  explicit ObColumnIterByPrevNextID(const ObTableSchema &table_schema) :
      is_end_(false), table_schema_(table_schema),
      last_column_schema_(NULL), last_iter_(NULL) { }
  ~ObColumnIterByPrevNextID() {}
  int next(const ObColumnSchemaV2 *&column_schema);
  const ObColumnSchemaV2 *get_first_column() const;
private:
  bool is_end_;
  const ObTableSchema &table_schema_;
  const ObColumnSchemaV2 *last_column_schema_;
  ObTableSchema::const_column_iterator last_iter_;
};


inline bool ObSimpleTableSchemaV2::is_final_invalid_index() const
{
  return is_final_invalid_index_status(index_status_);
}



}//end of namespace schema
}//end of namespace share
}//end of namespace oceanbase
#endif //OCEANBASE_SCHEMA_TABLE_SCHEMA
