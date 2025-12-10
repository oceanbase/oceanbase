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

#ifndef OCEANBASE_SHARE_TABLE_OB_TABLE_TTL_COMMON_
#define OCEANBASE_SHARE_TABLE_OB_TABLE_TTL_COMMON_

#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "share/rc/ob_tenant_base.h"
#include "src/share/table/redis/ob_redis_common.h"

namespace oceanbase
{
namespace table
{

class ObTTLTaskParam
{
public:
  ObTTLTaskParam()
  : ttl_(0),
    max_version_(0),
    is_htable_(false),
    tenant_id_(OB_INVALID_ID),
    user_id_(OB_INVALID_ID),
    database_id_(OB_INVALID_ID),
    table_id_(OB_INVALID_ID),
    is_redis_table_(false),
    is_redis_ttl_(false),
    has_cell_ttl_(false),
    redis_model_(ObRedisDataModel::MODEL_MAX),
    scan_index_()
  {
    MEMSET(scan_index_buf_, 0, sizeof(scan_index_buf_));
  }

  // Copy constructor
  ObTTLTaskParam(const ObTTLTaskParam& other)
  : ttl_(other.ttl_),
    max_version_(other.max_version_),
    is_htable_(other.is_htable_),
    tenant_id_(other.tenant_id_),
    user_id_(other.user_id_),
    database_id_(other.database_id_),
    table_id_(other.table_id_),
    is_redis_table_(other.is_redis_table_),
    is_redis_ttl_(other.is_redis_ttl_),
    has_cell_ttl_(other.has_cell_ttl_),
    redis_model_(other.redis_model_),
    scan_index_()
  {
    MEMCPY(scan_index_buf_, other.scan_index_buf_, sizeof(scan_index_buf_));
    // If scan_index_ points to other's buffer, point to our own buffer
    if (other.scan_index_.ptr() == other.scan_index_buf_) {
      scan_index_.assign_ptr(scan_index_buf_, other.scan_index_.length());
    } else {
      scan_index_ = other.scan_index_;
    }
  }

  // Assignment operator
  ObTTLTaskParam& operator=(const ObTTLTaskParam& other)
  {
    if (this != &other) {
      ttl_ = other.ttl_;
      max_version_ = other.max_version_;
      is_htable_ = other.is_htable_;
      tenant_id_ = other.tenant_id_;
      user_id_ = other.user_id_;
      database_id_ = other.database_id_;
      table_id_ = other.table_id_;
      is_redis_table_ = other.is_redis_table_;
      is_redis_ttl_ = other.is_redis_ttl_;
      has_cell_ttl_ = other.has_cell_ttl_;
      redis_model_ = other.redis_model_;

      MEMCPY(scan_index_buf_, other.scan_index_buf_, sizeof(scan_index_buf_));
      // If scan_index_ points to other's buffer, point to our own buffer
      if (other.scan_index_.ptr() == other.scan_index_buf_) {
        scan_index_.assign_ptr(scan_index_buf_, other.scan_index_.length());
      } else {
        scan_index_ = other.scan_index_;
      }
    }
    return *this;
  }

  bool is_valid() const
  {
    return tenant_id_ != OB_INVALID_ID &&
           user_id_ != OB_INVALID_ID &&
           database_id_ != OB_INVALID_ID &&
           table_id_ != OB_INVALID_ID;
  }

  bool operator==(const ObTTLTaskParam& param) const
  {
    return ttl_ == param.ttl_ &&
           max_version_ == param.max_version_ &&
           is_htable_ == param.is_htable_ &&
           tenant_id_ == param.tenant_id_ &&
           database_id_ == param.database_id_ &&
           user_id_ == param.user_id_ &&
           table_id_ == param.table_id_ &&
           is_redis_table_ == param.is_redis_table_ &&
           is_redis_ttl_ == param.is_redis_ttl_ &&
           redis_model_ == param.redis_model_ &&
           has_cell_ttl_ == param.has_cell_ttl_;
  }

  TO_STRING_KV(K_(ttl), K_(max_version), K_(is_htable), K_(tenant_id),
               K_(user_id), K_(database_id), K_(table_id), K_(is_redis_table),
              K_(is_redis_ttl), K_(redis_model), K_(has_cell_ttl));
public:
  int32_t  ttl_;
  int32_t  max_version_;
  bool is_htable_;
  int64_t tenant_id_;
  int64_t user_id_;
  int64_t database_id_;
  uint64_t table_id_;
  // for ob redis
  bool is_redis_table_;
  bool is_redis_ttl_;
  bool has_cell_ttl_;
  ObRedisDataModel redis_model_;
  // for ttl scan index
  char scan_index_buf_[OB_MAX_OBJECT_NAME_LENGTH];
  ObString scan_index_;
};



class ObTTLHRowkeyTaskParam : public ObTTLTaskParam
{
public:
  explicit ObTTLHRowkeyTaskParam(ObTTLTaskParam &task_param, common::ObIArray<common::ObString> &rowkeys)
    : ObTTLTaskParam(task_param),
      rowkeys_(rowkeys)
  {}
  OB_INLINE const common::ObIArray<common::ObString>& get_rowkeys() const { return rowkeys_; }
public:
  common::ObIArray<common::ObString> &rowkeys_;
};


class ObTTLTaskInfo final
{
public:
  ObTTLTaskInfo()
  : task_id_(OB_INVALID_ID),
    tablet_id_(),
    table_id_(OB_INVALID_ID),
    is_user_trigger_(true),
    row_key_(),
    ttl_del_cnt_(),
    max_version_del_cnt_(0),
    scan_cnt_(0),
    err_code_(OB_SUCCESS),
    tenant_id_(common::OB_INVALID_TENANT_ID),
    ls_id_(),
    consumer_group_id_(0),
    scan_index_()
  {
  }

  bool is_valid() const
  {
    return common::OB_INVALID_ID != task_id_;
  }
  const common::ObTabletID &get_tablet_id() const { return tablet_id_; }

  bool operator==(const ObTTLTaskInfo& other) const
  {
    return ((tenant_id_ == other.tenant_id_) &&
           (task_id_ == other.task_id_) &&
           (table_id_ == other.table_id_) &&
           (tablet_id_ == other.tablet_id_));
  }

  void reset_cnt()
  {
    ttl_del_cnt_ = 0;
    max_version_del_cnt_ = 0;
    scan_cnt_ = 0;
  }

  TO_STRING_KV(K_(task_id), K_(tablet_id), K_(table_id), K_(is_user_trigger),
               K_(is_user_trigger), K_(row_key), K_(ttl_del_cnt),
               K_(max_version_del_cnt), K_(scan_cnt), K_(err_code),
               K_(tenant_id), K_(ls_id), K_(consumer_group_id), K_(scan_index));

  int64_t          task_id_;
  common::ObTabletID       tablet_id_;
  uint64_t         table_id_;
  bool             is_user_trigger_;
  common::ObString row_key_;
  int64_t          ttl_del_cnt_;
  int64_t          max_version_del_cnt_;
  int64_t          scan_cnt_;
  int64_t          err_code_;
  int64_t          tenant_id_;
  share::ObLSID    ls_id_;
  int64_t          consumer_group_id_;
  common::ObString scan_index_;
};

} // end namespace table
} // end namespace oceanbase

#endif /*  OCEANBASE_SHARE_TABLE_OB_TABLE_TTL_COMMON_ */
