/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "share/ob_table_range.h"
#include "share/schema/ob_dependency_info.h"
#include "share/schema/ob_mlog_info.h"
#include "share/schema/ob_mview_info.h"
#include "share/schema/ob_mview_refresh_stats_params.h"

namespace oceanbase
{
namespace storage
{
class ObMViewTransaction;

struct ObMViewRefreshCtx
{
public:
  ObMViewRefreshCtx()
    : allocator_("MVRefCtx"),
      tenant_id_(OB_INVALID_TENANT_ID),
      mview_id_(OB_INVALID_ID),
      trans_(nullptr),
      refresh_type_(share::schema::ObMVRefreshType::MAX),
      is_oracle_mode_(false)
  {
  }
  ~ObMViewRefreshCtx() = default;
  DISABLE_COPY_ASSIGN(ObMViewRefreshCtx);

  void reuse()
  {
    trans_ = nullptr;
    mview_info_.reset();
    refresh_stats_params_.reset();
    dependency_infos_.reset();
    based_schema_object_infos_.reset();
    mlog_infos_.reset();
    refresh_scn_range_.reset();
    refresh_type_ = share::schema::ObMVRefreshType::MAX;
    refresh_sqls_.reset();
    is_oracle_mode_ = false;
    allocator_.reuse();
  }

  TO_STRING_KV(K_(tenant_id), K_(mview_id), KP_(trans), K_(mview_info), K_(refresh_stats_params),
               K_(dependency_infos), K_(based_schema_object_infos), K_(mlog_infos),
               K_(refresh_scn_range), K_(refresh_type), K_(refresh_sqls), K_(is_oracle_mode));

public:
  ObArenaAllocator allocator_;
  uint64_t tenant_id_;
  uint64_t mview_id_;
  ObMViewTransaction *trans_;
  share::schema::ObMViewInfo mview_info_;
  share::schema::ObMViewRefreshStatsParams refresh_stats_params_;
  ObArray<share::schema::ObDependencyInfo> dependency_infos_;
  ObArray<share::schema::ObBasedSchemaObjectInfo> based_schema_object_infos_;
  ObArray<share::schema::ObMLogInfo> mlog_infos_;
  share::ObScnRange refresh_scn_range_; // [last_refresh_scn, current_refresh_scn]
  share::schema::ObMVRefreshType refresh_type_;
  ObArray<ObString> refresh_sqls_;
  bool is_oracle_mode_;
};

} // namespace storage
} // namespace oceanbase
