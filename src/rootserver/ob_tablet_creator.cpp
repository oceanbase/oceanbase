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

#define USING_LOG_PREFIX RS
#include "ob_tablet_creator.h"
//#include "ob_freeze_info_manager.h"
#include "storage/tx/ob_trans_service.h"
#include "ob_root_service.h"
#include "lib/allocator/page_arena.h"
#include "share/ob_share_util.h"
#include "observer/ob_inner_sql_connection.h"
#include "storage/tx/ob_tx_log.h"

namespace oceanbase
{
using namespace share;
namespace rootserver
{


bool ObTabletCreatorArg::is_valid() const
{
  bool is_valid = ls_key_.is_valid()
                  && table_schemas_.count() > 0
                  && table_schemas_.count() == tablet_ids_.count()
                  && lib::Worker::CompatMode::INVALID != compat_mode_
                  && tenant_data_version_ > 0
                  && need_create_empty_majors_.count() == table_schemas_.count();
  for (int64_t i = 0; i < tablet_ids_.count() && is_valid; i++) {
    is_valid = tablet_ids_.at(i).is_valid();
  }
  return is_valid;
}

void ObTabletCreatorArg::reset()
{
  tablet_ids_.reset();
  ls_key_.reset();
  table_schemas_.reset();
  data_tablet_id_.reset();
  compat_mode_ = lib::Worker::CompatMode::INVALID;
  is_create_bind_hidden_tablets_ = false;
  tenant_data_version_ = 0;
  need_create_empty_majors_.reset();
}

int ObTabletCreatorArg::assign(const ObTabletCreatorArg &arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", KR(ret), K(arg));
  } else if (OB_FAIL(table_schemas_.assign(arg.table_schemas_))) {
    LOG_WARN("failed to assign table schemas", KR(ret), K(arg));
  } else if (OB_FAIL(tablet_ids_.assign(arg.tablet_ids_))) {
    LOG_WARN("failed to assign table schemas", KR(ret), K(arg));
  } else if (OB_FAIL(need_create_empty_majors_.assign(arg.need_create_empty_majors_))) {
    LOG_WARN("failed to assign need create empty majors", KR(ret), K(arg));
  } else {
    data_tablet_id_ = arg.data_tablet_id_;
    ls_key_ = arg.ls_key_;
    compat_mode_ = arg.compat_mode_;
    is_create_bind_hidden_tablets_ = arg.is_create_bind_hidden_tablets_;
    tenant_data_version_ = arg.tenant_data_version_;
  }
  return ret;
}

int ObTabletCreatorArg::init(
    const ObIArray<common::ObTabletID> &tablet_ids,
    const share::ObLSID &ls_key,
    const ObTabletID data_tablet_id,
    const ObIArray<const share::schema::ObTableSchema*> &table_schemas,
    const lib::Worker::CompatMode &mode,
    const bool is_create_bind_hidden_tablets,
    const uint64_t tenant_data_version,
    const ObIArray<bool> &need_create_empty_majors)
{
  int ret = OB_SUCCESS;
  bool is_valid = ls_key.is_valid() && table_schemas.count() > 0
                  && table_schemas.count() == tablet_ids.count()
                  && tenant_data_version > 0
                  && need_create_empty_majors.count() == table_schemas.count();
  for (int64_t i = 0; i < tablet_ids.count() && is_valid; i++) {
    is_valid = tablet_ids.at(i).is_valid();
  }
  if (OB_UNLIKELY(!is_valid)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_ids),
             "count", table_schemas.count(), K(tablet_ids), K(ls_key),
             K(tenant_data_version), "count_to_create_empty_major", need_create_empty_majors.count());
  } else if (OB_FAIL(tablet_ids_.assign(tablet_ids))) {
    LOG_WARN("failed to assign table schemas", KR(ret), K(tablet_ids));
  } else if (OB_FAIL(table_schemas_.assign(table_schemas))) {
    LOG_WARN("failed to assign table schemas", KR(ret), K(table_schemas));
  } else if (OB_FAIL(need_create_empty_majors_.assign(need_create_empty_majors))) {
    LOG_WARN("failed to assign need create empty majors", K(ret), K(need_create_empty_majors));
  } else {
    data_tablet_id_ = data_tablet_id;
    ls_key_ = ls_key;
    compat_mode_ = mode;
    is_create_bind_hidden_tablets_ = is_create_bind_hidden_tablets;
    tenant_data_version_ = tenant_data_version;
  }
  return ret;
}

DEF_TO_STRING(ObTabletCreatorArg)
{
  int64_t pos = 0;
  J_KV(K_(compat_mode), K_(tablet_ids), K_(data_tablet_id), K_(ls_key), K_(table_schemas), K_(is_create_bind_hidden_tablets),
    K_(tenant_data_version), K_(need_create_empty_majors));
  return pos;
}

/////////////////////////////////////////////////////////

int ObBatchCreateTabletHelper::init(
  const share::ObLSID &ls_key,
  const int64_t tenant_id,
  const SCN &major_frozen_scn,
  const bool need_check_tablet_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t bucket_count = hash::cal_next_prime(100);
  if (OB_UNLIKELY(!ls_key.is_valid()
                  || OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_key));
  } else if (OB_FAIL(batch_arg_.init_create_tablet(ls_key, major_frozen_scn, need_check_tablet_cnt))) {
    LOG_WARN("failed to init create tablet", KR(ret), K(tenant_id), K(ls_key), K(major_frozen_scn));
  } else if (OB_FAIL(table_schemas_map_.create(bucket_count, "CreateTablet", "CreateTablet"))) {
    LOG_WARN("failed to create hashmap", KR(ret));
  }
  return ret;
}

int ObBatchCreateTabletHelper::add_arg_to_batch_arg(
    const ObTabletCreatorArg &tablet_arg)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", KR(ret), K(tablet_arg));
  } else {
    ObArray<int64_t> index_array;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_arg.table_schemas_.count(); ++i) {
      const share::schema::ObTableSchema *table_schema = tablet_arg.table_schemas_.at(i);
      const uint64_t tenant_data_version = tablet_arg.tenant_data_version_;
      const bool need_create_empty_major = tablet_arg.need_create_empty_majors_.at(i);
      int64_t index = OB_INVALID_INDEX;
      if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", KR(ret), K(i), K(tablet_arg));
      } else if (OB_FAIL(try_add_table_schema(table_schema, tenant_data_version,
          need_create_empty_major, index, tablet_arg.compat_mode_))) {
        LOG_WARN("failed to add table schema to batch", KR(ret), K(table_schema), K(need_create_empty_major), K(index), K(batch_arg_));
      } else if (OB_UNLIKELY(OB_INVALID_INDEX == index)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index can not be invalid", KR(ret), K(index), K(tablet_arg), K(batch_arg_));
      } else if (OB_FAIL(index_array.push_back(index))) {
        LOG_WARN("failed to push back index", KR(ret), K(index));
      }
    }
    if (OB_SUCC(ret)) {
      obrpc::ObCreateTabletInfo info;
      if (OB_FAIL(info.init(tablet_arg.tablet_ids_,
                            tablet_arg.data_tablet_id_,
                            index_array,
                            tablet_arg.compat_mode_,
                            tablet_arg.is_create_bind_hidden_tablets_))) {
        LOG_WARN("failed to init create tablet info", KR(ret), K(index_array), K(tablet_arg));
      } else if (OB_FAIL(batch_arg_.tablets_.push_back(info))) {
        LOG_WARN("failed to push back info", KR(ret), K(info));
      }
    }
  }
  return ret;
}

int ObBatchCreateTabletHelper::add_table_schema_(
    const share::schema::ObTableSchema &table_schema,
    const lib::Worker::CompatMode compat_mode,
    const uint64_t tenant_data_version,
    const bool need_create_empty_major,
    int64_t &index)
{
  int ret = OB_SUCCESS;
  if (tenant_data_version < DATA_VERSION_4_2_2_0) {
    // compatibility with DATA_VERSION_4_2_1.
    index = batch_arg_.table_schemas_.count();
    if (OB_FAIL(batch_arg_.table_schemas_.push_back(table_schema))) {
      LOG_WARN("failed to push back table schema", KR(ret), K(table_schema));
    }
  } else {
    index = batch_arg_.create_tablet_schemas_.count();
    ObCreateTabletSchema *create_tablet_schema = NULL;
    void *create_tablet_schema_ptr = batch_arg_.allocator_.alloc(sizeof(ObCreateTabletSchema));
    obrpc::ObCreateTabletExtraInfo create_tablet_extr_info;
    if (OB_ISNULL(create_tablet_schema_ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate storage schema", KR(ret), K(table_schema));
    } else if (FALSE_IT(create_tablet_schema = new (create_tablet_schema_ptr)ObCreateTabletSchema())) {
    } else if (OB_FAIL(create_tablet_schema->init(batch_arg_.allocator_, table_schema, compat_mode,
         false/*skip_column_info*/, tenant_data_version < DATA_VERSION_4_3_0_0 ? ObCreateTabletSchema::STORAGE_SCHEMA_VERSION_V2 : ObCreateTabletSchema::STORAGE_SCHEMA_VERSION_V3))) {
      LOG_WARN("failed to init storage schema", KR(ret), K(table_schema));
    } else if (OB_FAIL(batch_arg_.create_tablet_schemas_.push_back(create_tablet_schema))) {
      LOG_WARN("failed to push back table schema", KR(ret), K(table_schema));
    } else if (OB_FAIL(create_tablet_extr_info.init(tenant_data_version, need_create_empty_major))) {
      LOG_WARN("init create table extra info failed", K(ret), K(tenant_data_version), K(need_create_empty_major), K(table_schema));
    } else if (OB_FAIL(batch_arg_.tablet_extra_infos_.push_back(create_tablet_extr_info))) {
      LOG_WARN("failed to push back tablet extra infos", K(ret), K(create_tablet_extr_info));
    }
  }
  return ret;
}

int ObBatchCreateTabletHelper::try_add_table_schema(
    const share::schema::ObTableSchema *table_schema,
    const uint64_t tenant_data_version,
    const bool need_create_empty_major,
    int64_t &index,
    const lib::Worker::CompatMode compat_mode)
{
  int ret = OB_SUCCESS;
  index = OB_INVALID_INDEX;
  if (OB_ISNULL(table_schema)
             || OB_UNLIKELY(!table_schema->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table schema is invlaid", KR(ret), KPC(table_schema));
  } else if (OB_SUCC(table_schemas_map_.get_refactored(table_schema->get_table_id(), index))) {
    //nothing
  } else if(OB_HASH_NOT_EXIST == ret)  {
    ret = OB_SUCCESS;
    HEAP_VAR(ObTableSchema, temp_table_schema) {
      if (OB_FAIL(temp_table_schema.assign(*table_schema))) {
        LOG_WARN("failed to assign temp_table_schema", KR(ret), KPC(table_schema));
      } else if (FALSE_IT(temp_table_schema.reset_partition_schema())) {
      } else if (OB_FAIL(add_table_schema_(temp_table_schema, compat_mode,
          tenant_data_version, need_create_empty_major, index))) {
        LOG_WARN("failed to push back table schema", KR(ret), K(temp_table_schema));
      } else if (OB_FAIL(table_schemas_map_.set_refactored(temp_table_schema.get_table_id(), index))) {
        LOG_WARN("failed to set table schema map", KR(ret), K(index), K(temp_table_schema));
      }
    }
  } else {
    LOG_WARN("failed to find table schema in map", KR(ret), KP(table_schema));
  }
  return ret;
}

DEF_TO_STRING(ObBatchCreateTabletHelper)
{
  int64_t pos = 0;
  J_KV(K_(batch_arg), K_(result));
  return pos;

}

/////////////////////////////////////////////////////////

ObTabletCreator::~ObTabletCreator()
{
  reset();
}

void ObTabletCreator::reset()
{
  FOREACH(iter, args_map_) {
    ObBatchCreateTabletHelper *batch_arg = iter->second;
    while (OB_NOT_NULL(batch_arg)) {
      ObBatchCreateTabletHelper *tmp = batch_arg;
      batch_arg = batch_arg->next_;
      tmp->~ObBatchCreateTabletHelper();
    }
  }
  args_map_.clear();
  need_check_tablet_cnt_ = false;
}

int ObTabletCreator::init(const bool need_check_tablet_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletCreator init twice", KR(ret));
  } else if (OB_FAIL(args_map_.create(MAP_BUCKET_NUM, "TabletCtr"))) {
    LOG_WARN("fail to create args map", KR(ret));
  } else {
    need_check_tablet_cnt_ = need_check_tablet_cnt;
    inited_ = true;
  }
  return ret;
}

int ObTabletCreator::add_create_tablet_arg(const ObTabletCreatorArg &arg)
{
  int ret = OB_SUCCESS;
  ObBatchCreateTabletHelper *batch_arg = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletCreator not init", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arg is invalid", KR(ret), K(arg));
  } else if (OB_SUCC(args_map_.get_refactored(arg.ls_key_, batch_arg))) {
    //already exist
  } else if (OB_HASH_NOT_EXIST == ret) {
    //create new arg
    void *arg_buf = allocator_.alloc(sizeof(ObBatchCreateTabletHelper));
    if (OB_ISNULL(arg_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate new arg", KR(ret), KP(batch_arg));
    } else if (FALSE_IT(batch_arg = new (arg_buf)ObBatchCreateTabletHelper())) {
    } else if (OB_FAIL(batch_arg->init(arg.ls_key_, tenant_id_, major_frozen_scn_,
            need_check_tablet_cnt_))) {
      LOG_WARN("failed to init batch arg helper", KR(ret), K(arg));
    } else if (OB_FAIL(args_map_.set_refactored(arg.ls_key_, batch_arg, 0/*not overwrite*/))) {
      LOG_WARN("fail to set refactored", KR(ret), K(arg));
    } else {
      LOG_INFO("new log stream", "ls_key", arg.ls_key_);
    }
  } else {
    LOG_WARN("failed to get batch arg", KR(ret), K(arg));
  }

  if (OB_FAIL(ret)) {
  } else if (batch_arg->batch_arg_.get_serialize_size() > BATCH_ARG_SIZE) {
    LOG_INFO("batch arg is more than 1M", KR(ret), K(batch_arg->batch_arg_.tablets_.count()), K(batch_arg->batch_arg_));
    void *arg_buf = allocator_.alloc(sizeof(ObBatchCreateTabletHelper));
    ObBatchCreateTabletHelper *new_arg = NULL;
    if (OB_ISNULL(arg_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate new arg", KR(ret));
    } else if (FALSE_IT(new_arg = new (arg_buf)ObBatchCreateTabletHelper())) {
    } else if (OB_FAIL(new_arg->init(arg.ls_key_, tenant_id_, major_frozen_scn_,
            need_check_tablet_cnt_))) {
      LOG_WARN("failed to init batch arg helper", KR(ret), K(arg));
    } else if (FALSE_IT(new_arg->next_ = batch_arg)) {
    } else if (OB_FAIL(args_map_.set_refactored(arg.ls_key_, new_arg, 1/*overwrite*/))) {
      LOG_WARN("fail to set refactored", KR(ret), K(arg));
    } else {
      batch_arg = new_arg;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(batch_arg->add_arg_to_batch_arg(arg))) {
    LOG_WARN("failed to add arg to batch", KR(ret), K(arg));
  }
  return ret;
}

int ObTabletCreator::execute()
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  const int64_t default_timeout_ts = GCONF.rpc_timeout;
  const int64_t SLEEP_INTERVAL = 100 * 1000L; // 100ms
  observer::ObInnerSQLConnection *conn = NULL;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletCreator not init", KR(ret));
  } else if (OB_ISNULL(conn = dynamic_cast<observer::ObInnerSQLConnection *>
                       (trans_.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("conn_ is NULL", KR(ret));
  } else if (OB_UNLIKELY(0 >= args_map_.size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch arg count is invalid", KR(ret));
  } else {
    FOREACH_X(iter, args_map_, OB_SUCC(ret)) {
      ObBatchCreateTabletHelper *batch_arg = iter->second;
      if (OB_ISNULL(batch_arg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("batch arg not be null", KR(ret));
      } else {
        while (OB_SUCC(ret) && OB_NOT_NULL(batch_arg)) {
          int64_t buf_len = batch_arg->batch_arg_.get_serialize_size();
          int64_t pos = 0;
          char *buf = (char*)allocator_.alloc(buf_len);
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail alloc memory", KR(ret));
          } else if (OB_FAIL(batch_arg->batch_arg_.serialize(buf, buf_len, pos))) {
            LOG_WARN("fail to serialize", KR(ret), K(batch_arg->batch_arg_));
          } else if (OB_FAIL(share::ObShareUtil::set_default_timeout_ctx(ctx, default_timeout_ts))) {
            LOG_WARN("fail to set timeout ctx", KR(ret), K(default_timeout_ts));
          } else {
            do {
              int64_t start_time = ObTimeUtility::current_time();
              if (ctx.is_timeouted()) {
                ret = OB_TIMEOUT;
                LOG_WARN("already timeout", KR(ret), K(ctx));
              } else if (OB_FAIL(conn->register_multi_data_source(tenant_id_, iter->first,
                                 transaction::ObTxDataSourceType::CREATE_TABLET_NEW_MDS, buf, buf_len))) {
                if (need_retry(ret)) {
                  LOG_INFO("fail to register_tx_data, try again", KR(ret), K_(tenant_id), K(batch_arg->batch_arg_));
                  ob_usleep(SLEEP_INTERVAL);
                } else {
                  LOG_WARN("fail to register_tx_data", KR(ret), K(batch_arg->batch_arg_), K(buf), K(buf_len));
                }
              }
              int64_t end_time = ObTimeUtility::current_time();
              LOG_INFO("generate create arg", KR(ret), K(buf_len), K(batch_arg->batch_arg_.tablets_.count()),
                                              K(batch_arg->batch_arg_), "cost_ts", end_time - start_time);
            } while (need_retry(ret));
            if (OB_SUCC(ret) && batch_arg->batch_arg_.set_binding_info_outside_create()) {
              const int64_t start_time = ObTimeUtility::current_time();
              if (OB_FAIL(ObTabletBindingMdsHelper::modify_tablet_binding_for_create(tenant_id_,
                      batch_arg->batch_arg_, ctx.get_abs_timeout(), trans_))) {
                LOG_WARN("failed to modify tablet binding for create", K(ret));
              }
              const int64_t end_time = ObTimeUtility::current_time();
              LOG_INFO("modify binding for create", KR(ret), K(buf_len), K(batch_arg->batch_arg_.tablets_.count()),
                                                    "cost_ts", end_time - start_time);
            }
          }
          batch_arg = batch_arg->next_;
        } // end while
      }
    } // end for
  }
  reset();
  return ret;
}

bool ObTabletCreator::need_retry(int ret)
{
  return is_location_service_renew_error(ret) || OB_NOT_MASTER == ret;
}

} // rootserver
} // oceanbase
