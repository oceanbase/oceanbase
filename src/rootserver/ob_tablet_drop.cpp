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
#include "ob_tablet_drop.h"
#include "ob_root_service.h"
#include "share/ob_share_util.h"
#include "share/tablet/ob_tablet_to_table_history_operator.h" // ObTabletToTableHistoryOperator
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "observer/ob_inner_sql_connection.h"

namespace oceanbase
{
namespace rootserver
{

ObTabletDrop::~ObTabletDrop()
{
  FOREACH(iter, args_map_) {
    if (OB_NOT_NULL(iter->second)
        && FALSE_IT(iter->second->~ObIArray<ObTabletID>())) {
    }
  }
}

void ObTabletDrop::reset()
{
  FOREACH(iter, args_map_) {
    if (OB_NOT_NULL(iter->second)
        && FALSE_IT(iter->second->~ObIArray<ObTabletID>())) {
    }
  }
  args_map_.clear();
}

int ObTabletDrop::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTabletDrop init twice", KR(ret));
  } else if (OB_FAIL(args_map_.create(MAP_BUCKET_NUM, "TabletCtr"))) {
    LOG_WARN("fail to args map", KR(ret));
  } else {
    inited_ = true;
  }
  return ret;
}

int ObTabletDrop::add_drop_tablets_of_table_arg(
                  const common::ObIArray<const share::schema::ObTableSchema*> &schemas)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObLSID> ls_ids;
  int64_t all_part_num = 0;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletCreator not init", KR(ret));
  } else if (schemas.count() < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schemas count is less 1", KR(ret), K(schemas));
  } else if (OB_ISNULL(schemas.at(0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("NULL ptr", KR(ret), K(schemas));
  } else {
    const share::schema::ObTableSchema &table_schema = *schemas.at(0);
    if (is_inner_table(table_schema.get_table_id())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sys table cannot drop", K(table_schema), KR(ret));
    } else if (schemas.count() > 1) {
      int64_t data_table_id = OB_INVALID_ID;
      if (table_schema.is_index_local_storage()
          || table_schema.is_aux_lob_table()
          || table_schema.is_mlog_table()) {
        data_table_id = table_schema.get_data_table_id();
      } else {
        data_table_id = table_schema.get_table_id();
      }
      for (int64_t i = 1; OB_SUCC(ret) && i < schemas.count(); ++i) {
        const share::schema::ObTableSchema *aux_table_schema = schemas.at(i);
        if (OB_ISNULL(aux_table_schema)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("ptr is null", KR(ret), K(schemas));
        } else if (is_inner_table(aux_table_schema->get_table_id())) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("sys table cannot drop", KPC(aux_table_schema), KR(ret));
        } else if (!aux_table_schema->is_index_local_storage()
            && !aux_table_schema->is_aux_lob_table()
            && !aux_table_schema->is_mlog_table()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("aux_table_schema must be local index or aux lob table", KR(ret), K(schemas), KPC(aux_table_schema));
        } else if (data_table_id != aux_table_schema->get_data_table_id()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("aux table schema must be of same data table", KR(ret), K(schemas), KPC(aux_table_schema));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_ls_from_table(table_schema, ls_ids))) {
      LOG_WARN("fail to get ls from table", KR(ret));
    } else {
      int64_t ls_idx = 0;
      ObPartitionLevel part_level = table_schema.get_part_level();
      if (PARTITION_LEVEL_ZERO == part_level) {
        if (OB_FAIL(drop_tablet_(schemas, ls_ids.at(ls_idx++),
                    OB_INVALID_INDEX, OB_INVALID_INDEX))) {
          LOG_WARN("fail to drop tablet", K(table_schema), KR(ret));
        }
      } else {
        ObPartition **part_array = table_schema.get_part_array();
        int64_t part_num = table_schema.get_partition_num();
        if (OB_ISNULL(part_array)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part array is null", K(table_schema), KR(ret));
        } else {
          for (int64_t i = 0; i < part_num && OB_SUCC(ret); ++i) {
            if (OB_ISNULL(part_array[i])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("NULL ptr", K(i), K(table_schema), KR(ret));
            } else if (PARTITION_LEVEL_ONE == part_level) {
              if (OB_FAIL(drop_tablet_(schemas, ls_ids.at(ls_idx++), i,
                          OB_INVALID_INDEX))) {
                LOG_WARN("fail to drop tablet", K(table_schema), KR(ret));
              }
            } else if (PARTITION_LEVEL_TWO == part_level) {
              ObSubPartition **subpart_array = part_array[i]->get_subpart_array();
              int64_t sub_part_num = part_array[i]->get_subpartition_num();
              if (OB_ISNULL(subpart_array)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("part array is null", K(table_schema), KR(ret));
              } else {
                for (int64_t j = 0; j < sub_part_num && OB_SUCC(ret); j++) {
                  if (OB_ISNULL(subpart_array[j])) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("NULL ptr", K(j), K(table_schema), KR(ret));
                  } else {
                    if (OB_FAIL(drop_tablet_(schemas, ls_ids.at(ls_idx++), i, j))) {
                      LOG_WARN("fail to drop tablet", K(table_schema), KR(ret));
                    }
                  }
                }
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("4.0 not support part type", K(table_schema), KR(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTabletDrop::get_ls_from_table(const share::schema::ObTableSchema &table_schema,
                                    common::ObIArray<share::ObLSID> &assign_ls_id_array)
{
  int ret = OB_SUCCESS;
  int64_t all_part_num = 0;
  if (-1 == (all_part_num = table_schema.get_all_part_num())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get tablet num", K(table_schema), KR(ret));
  } else if (OB_FAIL(assign_ls_id_array.reserve(all_part_num))) {
    LOG_WARN("fail to reserve array", KR(ret), K(all_part_num));
  } else if (is_sys_tenant(table_schema.get_tenant_id())) {
    for (int64_t i = 0; i < all_part_num && OB_SUCC(ret); i++) {
      if (OB_FAIL(assign_ls_id_array.push_back(share::SYS_LS))) {
        LOG_WARN("failed to push_back", KR(ret));
      }
    }
  } else {
    ObArray<ObTabletID> tablet_ids;
    if (OB_FAIL(table_schema.get_tablet_ids(tablet_ids))) {
      LOG_WARN("fail to get tablet ids", KR(ret), K(table_schema));
    } else if (OB_FAIL(share::ObTabletToLSTableOperator::batch_get_ls(trans_, tenant_id_, tablet_ids, assign_ls_id_array))) {
      LOG_WARN("fail to batch_get_ls", KR(ret), K(tenant_id_), K(table_schema));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (assign_ls_id_array.count() != all_part_num) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the ls of tablet is not equal partition num",
              KR(ret), K(assign_ls_id_array.count()), K(all_part_num));
  }
  return ret;
}

int ObTabletDrop::drop_tablet_(
    const common::ObIArray<const share::schema::ObTableSchema *> &table_schema_ptr_array,
    const share::ObLSID &ls_id,
    const int64_t part_idx,
    const int64_t subpart_idx)
{
  int ret = OB_SUCCESS;
  common::ObIArray<ObTabletID> *tablet_id_array = NULL;

  if (table_schema_ptr_array.count() < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_schema_ptr_array is empty", K(table_schema_ptr_array), KR(ret));
  } else if (OB_SUCC(args_map_.get_refactored(ls_id, tablet_id_array))) {
    //already exist
  } else if (OB_HASH_NOT_EXIST == ret) {
    //create new tablet_id_array
    void *ptr1 = allocator_.alloc(sizeof(ObArray<ObTabletID>));
    void *ptr2 = allocator_.alloc(sizeof(ObArray<ObTabletID>));
    if (OB_ISNULL(ptr1) || OB_ISNULL(ptr2)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail alloc memory", KR(ret));
    } else {
      tablet_id_array = new (ptr1)ObArray<ObTabletID, ObIAllocator &>(
                                OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_);
      if (OB_FAIL(args_map_.set_refactored(ls_id, tablet_id_array, 0/*not overwrite*/))) {
        LOG_WARN("fail to set refactored", KR(ret), K(ls_id));
      }
    }
  } else {
    LOG_WARN("fail to get element from args_map", KR(ret), K(ls_id));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(tablet_id_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(table_schema_ptr_array), KR(ret));
  } else if (OB_ISNULL(table_schema_ptr_array.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", K(table_schema_ptr_array), KR(ret));
  } else {
    bool only_drop_index = table_schema_ptr_array.at(0)->is_index_local_storage();
    ObBasePartition *first_table_part = NULL;
    ObBasePartition *part = NULL;
    for (int r = 0; r < table_schema_ptr_array.count() && OB_SUCC(ret); r++) {
      const share::schema::ObTableSchema *table_schema_ptr = table_schema_ptr_array.at(r);
      if (OB_ISNULL(table_schema_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL ptr", K(r), K(table_schema_ptr_array), KR(ret));
      } else if (PARTITION_LEVEL_ZERO == table_schema_ptr->get_part_level()) {
        ObTabletID tablet_id = table_schema_ptr->get_tablet_id();
        if (OB_FAIL(tablet_id_array->push_back(tablet_id))) {
          LOG_WARN("failed to assign table schema point", KR(ret), KPC(table_schema_ptr));
        }
      } else if (OB_FAIL(table_schema_ptr->get_part_by_idx(part_idx, subpart_idx, part))) {
        LOG_WARN("fail to get index part", KR(ret), KPC(table_schema_ptr), K(part_idx), K(subpart_idx));
      } else if (OB_ISNULL(part)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("NULL ptr", KR(ret), KPC(table_schema_ptr), K(part_idx), K(subpart_idx));
      } else {
        if (r == 0) {
          first_table_part = part;
        } else {
          if (OB_UNLIKELY(!first_table_part->same_base_partition(*part))) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("parts in table and index table is not equal", KR(ret), KPC(first_table_part), KPC(part));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(tablet_id_array->push_back(part->get_tablet_id()))) {
          LOG_WARN("failed to assign table schema point", KR(ret), K(part_idx), K(subpart_idx));
        }
      }
    }
  }
  return ret;
}
int ObTabletDrop::execute()
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
      common::ObIArray<ObTabletID> *tablet_ids = iter->second;
      obrpc::ObBatchRemoveTabletArg arg;
      if (OB_ISNULL(tablet_ids)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet_ids is NULL ptr", KR(ret));
      } else if (tablet_ids->count() < 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet_ids is empty", KR(ret));
      } else if (OB_FAIL(share::ObTabletToLSTableOperator::batch_remove(trans_, tenant_id_, *tablet_ids))) {
        LOG_WARN("tablet_ids count less than 1", KR(ret));
      } else if (OB_FAIL(share::ObTabletToTableHistoryOperator::drop_tablet_to_table_history(
                         trans_, tenant_id_, schema_version_, *tablet_ids))) {
        LOG_WARN("fail to create tablet to table history", KR(ret), K_(tenant_id), K(schema_version_));
      } else if (OB_FAIL(arg.init(*(tablet_ids), iter->first))) {
        LOG_WARN("failed to find leader", KR(ret), K(iter->first), KPC(tablet_ids));
      } else {
        LOG_INFO("generate remove arg", K(arg), K(lbt()), KPC(tablet_ids));
        int64_t buf_len = arg.get_serialize_size();
        int64_t pos = 0;
        char *buf = (char*)allocator_.alloc(buf_len);
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail alloc memory", KR(ret));
        } else if (OB_FAIL(arg.serialize(buf, buf_len, pos))) {
          LOG_WARN("fail to serialize", KR(ret), K(arg));
        } else if (OB_FAIL(share::ObShareUtil::set_default_timeout_ctx(ctx, default_timeout_ts))) {
          LOG_WARN("fail to set timeout ctx", KR(ret), K(default_timeout_ts));
        } else {
          do {
            if (ctx.is_timeouted()) {
              ret = OB_TIMEOUT;
              LOG_WARN("already timeout", KR(ret), K(ctx));
            } else if (OB_FAIL(conn->register_multi_data_source(tenant_id_, iter->first,
                                transaction::ObTxDataSourceType::DELETE_TABLET_NEW_MDS, buf, buf_len))) {
              LOG_WARN("fail to register_tx_data", KR(ret), K(arg), K(buf), K(buf_len));
              if (OB_LS_LOCATION_LEADER_NOT_EXIST == ret || OB_NOT_MASTER == ret) {
                LOG_INFO("fail to find leader, try again", K_(tenant_id), K(arg));
                ob_usleep(SLEEP_INTERVAL);
              }
            }
          } while (OB_LS_LOCATION_LEADER_NOT_EXIST == ret || OB_NOT_MASTER == ret);
        }
      }
    }
  }
  return ret;
}

} // rootserver
} // oceanbase

