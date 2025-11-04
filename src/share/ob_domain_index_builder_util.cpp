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

#define USING_LOG_PREFIX COMMON
#include "ob_domain_index_builder_util.h"
#include "src/rootserver/ob_root_service.h"
#include "share/ob_fts_index_builder_util.h"
#include "share/ob_vec_index_builder_util.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;
namespace share
{

#define LOCATE_INDEX_IDX(INDEX_TYPE, IDX_VALUE)                                                  \
  do {                                                                                           \
    if (OB_FAIL(ret)) {                                                                          \
    } else if (OB_FAIL(locate_aux_index_schema_by_name(domain_index_name,                        \
                                                       new_data_table_id,                        \
                                                       aux_schema_array,                         \
                                                       INDEX_TYPE,                               \
                                                       allocator,                                \
                                                       IDX_VALUE))) {                            \
      LOG_WARN("fail to locate aux index schema by name",                                        \
          K(ret), K(domain_index_name), K(INDEX_TYPE), K(aux_schema_array));                     \
    }                                                                                            \
  } while (0)


int ObDomainIndexBuilderUtil::prepare_aux_table(bool &task_submitted,
                                                uint64_t &aux_table_id,
                                                int64_t &res_task_id,
                                                const common::TCRWLock& lock,
                                                const uint64_t& data_table_id,
                                                const uint64_t& tenant_id,
                                                const int64_t& task_id,
                                                obrpc::ObCreateIndexArg& index_arg,
                                                rootserver::ObRootService *root_service,
                                                common::hash::ObHashMap<uint64_t, ObDomainDependTaskStatus> &map,
                                                const oceanbase::common::ObAddr &addr,
                                                int map_num,
                                                const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root_service is nullptr", K(ret));
  } else {
    int64_t ddl_rpc_timeout = 0;
    rootserver::ObDDLService &ddl_service = root_service->get_ddl_service();
    obrpc::ObCommonRpcProxy *common_rpc = nullptr;
    if (!map.created() &&
      OB_FAIL(map.create(map_num, lib::ObLabel("DepTasMap")))) {
      LOG_WARN("create dependent task map failed", K(ret));
    } else if (OB_ISNULL(root_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("root_service is nullptr", K(ret));
    } else if (OB_FALSE_IT(common_rpc = root_service->get_ddl_service().get_common_rpc())) {
    } else if (OB_ISNULL(common_rpc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("common rpc is nullptr", K(ret));
    } else if (OB_FAIL(ObDDLUtil::get_ddl_rpc_timeout(tenant_id,
                                                      data_table_id,
                                                      ddl_rpc_timeout))) {
      LOG_WARN("get ddl rpc timeout fail", K(ret));
    } else {
      SMART_VARS_2((obrpc::ObCreateAuxIndexArg, arg),
                    (obrpc::ObCreateAuxIndexRes, res)) {
      arg.tenant_id_ = tenant_id;
      arg.exec_tenant_id_ = tenant_id;
      arg.data_table_id_ = data_table_id;
      arg.task_id_ = task_id;
      if (task_submitted) {
          // do nothing
      } else if (OB_FAIL(arg.create_index_arg_.assign(index_arg))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to assign create index arg", K(ret));
      } else if (OB_FALSE_IT(arg.snapshot_version_ = snapshot_version)) {
      } else if (OB_FAIL(common_rpc-> to(addr).
                          timeout(ddl_rpc_timeout).create_aux_index(arg, res))) {
        LOG_WARN("generate aux index schema failed", K(ret), K(arg));
      } else if (res.schema_generated_) {
        task_submitted = true;
        aux_table_id = res.aux_table_id_;
        if (res.ddl_task_id_ < 0) {
          // res_task_id is int64, while OB_INVALID_ID is unit64_max, so use "res.ddl_task_id_ < 0" other than "res.ddl_task_id_ == OB_INVALID_ID"
          // rowkey_vid/vid_rowkey table already exist and data is ready
          res_task_id = OB_INVALID_ID;
        } else { // need to wait data complement finish
          res_task_id = res.ddl_task_id_;
          TCWLockGuard guard(lock);
          share::ObDomainDependTaskStatus status;
          // check if child task is already added
          if (OB_FAIL(map.get_refactored(aux_table_id, status))) {
            if (OB_HASH_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
              status.task_id_ = res.ddl_task_id_;
              if (OB_FAIL(map.set_refactored(aux_table_id, status))) {
                  LOG_WARN("set dependent task map failed", K(ret), K(aux_table_id));
              }
            } else {
              LOG_WARN("get from dependent task map failed", K(ret));
            }
          }
        }
      }
      } // SMART_VAR
    }
  }
  return ret;
}

int ObDomainIndexBuilderUtil::retrieve_complete_domain_index(
    const ObIArray<ObTableSchema> &shared_schema_array,
    const ObIArray<ObTableSchema> &domain_schema_array,
    const ObIArray<ObTableSchema> &aux_schema_array,
    ObArenaAllocator &allocator,
    const uint64_t new_data_table_id,
    ObIArray<ObTableSchema> &rebuid_index_schemas,
    const bool need_doc_id,
    const bool need_vid)
{
  int ret = OB_SUCCESS;
  bool has_complete_fts = false;
  bool has_complete_nhsw_vec = false;
  int64_t rowkey_doc_idx = -1;
  int64_t doc_rowkey_idx = -1;
  int64_t rowkey_vid_idx = -1;
  int64_t vid_rowkey_idx = -1;
  ObSArray<ObTableSchema> complete_index_schemas;
  if (OB_UNLIKELY(OB_INVALID_ID == new_data_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(new_data_table_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < shared_schema_array.count(); ++i) {
      const ObTableSchema &shared_schema = shared_schema_array.at(i);
      if (shared_schema.is_rowkey_doc_id()) {
        rowkey_doc_idx = i;
      } else if (shared_schema.is_doc_id_rowkey()) {
        doc_rowkey_idx = i;
      } else if (shared_schema.is_vec_rowkey_vid_type()) {
        rowkey_vid_idx = i;
      } else if (shared_schema.is_vec_vid_rowkey_type()) {
        vid_rowkey_idx = i;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected schema type", K(shared_schema));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < domain_schema_array.count(); ++i) {
      const ObTableSchema &domain_index_schema = domain_schema_array.at(i);
      const ObString &domain_index_name = domain_index_schema.get_table_name_str();
      if (domain_index_schema.is_multivalue_index_aux() ||
          domain_index_schema.is_fts_index_aux()) {
        if (!need_doc_id || (rowkey_doc_idx >= 0 && doc_rowkey_idx >= 0)) {
          if (domain_index_schema.is_fts_index_aux()) {
            int64_t index_aux_schema_idx = -1;
            LOCATE_INDEX_IDX(INDEX_TYPE_FTS_DOC_WORD_LOCAL, index_aux_schema_idx);
            if (OB_FAIL(ret)) {
            } else if (index_aux_schema_idx >= 0) {
              if (OB_FAIL(complete_index_schemas.push_back(domain_index_schema))) {
                LOG_WARN("fail to push back domain index schema", K(ret));
              } else if (OB_FAIL(complete_index_schemas.push_back(aux_schema_array.at(index_aux_schema_idx)))) {
                LOG_WARN("fail to push back aux index schema", K(ret));
              }
            }
          } else {
            if (OB_FAIL(complete_index_schemas.push_back(domain_index_schema))) {
              LOG_WARN("fail to push back domain index schema", K(ret));
            }
          }
        }
      } else if (domain_index_schema.is_vec_domain_index()) {
        if (domain_index_schema.is_vec_delta_buffer_type()) {
          if (!need_vid || (rowkey_vid_idx >= 0 && vid_rowkey_idx >= 0)) {
            int64_t vec_index_id_idx = -1;
            int64_t vec_index_snapshot_data_idx = -1;
            LOCATE_INDEX_IDX(INDEX_TYPE_VEC_INDEX_ID_LOCAL, vec_index_id_idx);
            LOCATE_INDEX_IDX(INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL, vec_index_snapshot_data_idx);
            if (OB_FAIL(ret)) {
            } else if (vec_index_id_idx >= 0 && vec_index_snapshot_data_idx >= 0) {
              if (OB_FAIL(complete_index_schemas.push_back(domain_index_schema))) {
                LOG_WARN("fail to push back domain index schema", K(ret));
              } else if (OB_FAIL(complete_index_schemas.push_back(aux_schema_array.at(vec_index_id_idx)))) {
                LOG_WARN("fail to push back aux index schema", K(ret));
              } else if (OB_FAIL(complete_index_schemas.push_back(aux_schema_array.at(vec_index_snapshot_data_idx)))) {
                LOG_WARN("fail to push back aux index schema", K(ret));
              }
            }
          }
        } else if (domain_index_schema.is_vec_ivfflat_centroid_index()) {
          int64_t flat_rowkey_cid_idx = -1;
          int64_t flat_cid_vector_idx = -1;
          LOCATE_INDEX_IDX(INDEX_TYPE_VEC_IVFFLAT_ROWKEY_CID_LOCAL, flat_rowkey_cid_idx);
          LOCATE_INDEX_IDX(INDEX_TYPE_VEC_IVFFLAT_CID_VECTOR_LOCAL, flat_cid_vector_idx);
          if (OB_FAIL(ret)) {
          } else if (flat_rowkey_cid_idx >= 0 && flat_cid_vector_idx >= 0) {
            if (OB_FAIL(complete_index_schemas.push_back(domain_index_schema)) ||
                OB_FAIL(complete_index_schemas.push_back(aux_schema_array.at(flat_rowkey_cid_idx))) ||
                OB_FAIL(complete_index_schemas.push_back(aux_schema_array.at(flat_cid_vector_idx)))) {
              LOG_WARN("fail to push back domain index schemas", K(ret));
            }
          }
        } else if (domain_index_schema.is_vec_ivfsq8_centroid_index()) {
          int64_t sq8_rowkey_cid_idx = -1;
          int64_t sq8_cid_vector_idx = -1;
          int64_t sq8_meta_idx = -1;
          LOCATE_INDEX_IDX(INDEX_TYPE_VEC_IVFSQ8_ROWKEY_CID_LOCAL, sq8_rowkey_cid_idx);
          LOCATE_INDEX_IDX(INDEX_TYPE_VEC_IVFSQ8_CID_VECTOR_LOCAL, sq8_cid_vector_idx);
          LOCATE_INDEX_IDX(INDEX_TYPE_VEC_IVFSQ8_META_LOCAL, sq8_meta_idx);
          if (OB_FAIL(ret)) {
          } else if (sq8_rowkey_cid_idx >= 0 && sq8_cid_vector_idx >= 0 && sq8_meta_idx >= 0) {
            if (OB_FAIL(complete_index_schemas.push_back(domain_index_schema)) ||
                OB_FAIL(complete_index_schemas.push_back(aux_schema_array.at(sq8_rowkey_cid_idx))) ||
                OB_FAIL(complete_index_schemas.push_back(aux_schema_array.at(sq8_cid_vector_idx))) ||
                OB_FAIL(complete_index_schemas.push_back(aux_schema_array.at(sq8_meta_idx)))) {
              LOG_WARN("fail to push back domain index schemas", K(ret));
            }
          }
        } else if (domain_index_schema.is_vec_ivfpq_centroid_index()) {
          int64_t pq_rowkey_cid_idx = -1;
          int64_t pq_pq_centroid_idx = -1;
          int64_t pq_code_idx = -1;
          LOCATE_INDEX_IDX(INDEX_TYPE_VEC_IVFPQ_ROWKEY_CID_LOCAL, pq_rowkey_cid_idx);
          LOCATE_INDEX_IDX(INDEX_TYPE_VEC_IVFPQ_PQ_CENTROID_LOCAL, pq_pq_centroid_idx);
          LOCATE_INDEX_IDX(INDEX_TYPE_VEC_IVFPQ_CODE_LOCAL, pq_code_idx);
          if (OB_FAIL(ret)) {
          } else if (pq_rowkey_cid_idx >= 0 && pq_pq_centroid_idx >= 0 && pq_code_idx >= 0) {
            if (OB_FAIL(complete_index_schemas.push_back(domain_index_schema)) ||
                OB_FAIL(complete_index_schemas.push_back(aux_schema_array.at(pq_rowkey_cid_idx))) ||
                OB_FAIL(complete_index_schemas.push_back(aux_schema_array.at(pq_pq_centroid_idx))) ||
                OB_FAIL(complete_index_schemas.push_back(aux_schema_array.at(pq_code_idx)))) {
              LOG_WARN("fail to push back domain index schemas", K(ret));
            }
          }
        } else if (domain_index_schema.is_hybrid_vec_index_log_type()) {
          if (!need_vid || (rowkey_vid_idx >= 0 && vid_rowkey_idx >= 0)) {
            int64_t vec_index_id_idx = -1;
            int64_t vec_index_snapshot_data_idx = -1;
            int64_t hybrid_vec_index_embedded_idx = -1;
            LOCATE_INDEX_IDX(INDEX_TYPE_VEC_INDEX_ID_LOCAL, vec_index_id_idx);
            LOCATE_INDEX_IDX(INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL, vec_index_snapshot_data_idx);
            LOCATE_INDEX_IDX(INDEX_TYPE_HYBRID_INDEX_EMBEDDED_LOCAL, hybrid_vec_index_embedded_idx);
            if (OB_FAIL(ret)) {
            } else if (vec_index_id_idx >= 0 && vec_index_snapshot_data_idx >= 0 && hybrid_vec_index_embedded_idx >= 0) {
              if (OB_FAIL(complete_index_schemas.push_back(domain_index_schema))) {
                LOG_WARN("fail to push back domain index schema", K(ret));
              } else if (OB_FAIL(complete_index_schemas.push_back(aux_schema_array.at(vec_index_id_idx)))) {
                LOG_WARN("fail to push back aux index schema", K(ret));
              } else if (OB_FAIL(complete_index_schemas.push_back(aux_schema_array.at(vec_index_snapshot_data_idx)))) {
                LOG_WARN("fail to push back aux index schema", K(ret));
              } else if (OB_FAIL(complete_index_schemas.push_back(aux_schema_array.at(hybrid_vec_index_embedded_idx)))) {
                LOG_WARN("fail to push back aux index schema", K(ret));
              }
            }
          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < complete_index_schemas.count(); ++i) {
      const ObTableSchema &aux_index_schema = complete_index_schemas.at(i);
      if (aux_index_schema.is_fts_index_aux() || aux_index_schema.is_multivalue_index_aux()) {
        has_complete_fts = true;
      } else if (aux_index_schema.is_vec_delta_buffer_type() || aux_index_schema.is_hybrid_vec_index_log_type()) {
        has_complete_nhsw_vec = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (has_complete_fts && need_doc_id) {
      if (OB_FAIL(rebuid_index_schemas.push_back(shared_schema_array.at(rowkey_doc_idx))) ||
          OB_FAIL(rebuid_index_schemas.push_back(shared_schema_array.at(doc_rowkey_idx)))) {
        LOG_WARN("fail to push back rowkey doc/doc rowkey schema", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (has_complete_nhsw_vec && need_vid) {
      if (OB_FAIL(rebuid_index_schemas.push_back(shared_schema_array.at(rowkey_vid_idx))) ||
          OB_FAIL(rebuid_index_schemas.push_back(shared_schema_array.at(vid_rowkey_idx)))) {
        LOG_WARN("fail to push back rowkey vid/vid rowkey schema", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < complete_index_schemas.count(); ++i) {
      const ObTableSchema &aux_index_schema = complete_index_schemas.at(i);
      if (OB_FAIL(rebuid_index_schemas.push_back(aux_index_schema))) {
        LOG_WARN("fail to push back aux index schema", K(ret), K(aux_index_schema));
      }
    }
  }
  return ret;
}

int ObDomainIndexBuilderUtil::locate_aux_index_schema_by_name(
    const ObString &inner_index_name,
    const uint64_t new_data_table_id,
    const ObIArray<ObTableSchema> &aux_schema_array,
    const share::schema::ObIndexType type,
    ObArenaAllocator &allocator,
    int64_t &index_aux_schema_idx)
{
  int ret = OB_SUCCESS;
  index_aux_schema_idx = -1;
  if (OB_UNLIKELY(inner_index_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(inner_index_name));
  } else {
    ObString user_index_name;
    ObString index_aux_name;
    ObString inner_index_aux_name;
    if (OB_FAIL(ObTableSchema::get_index_name(allocator,
                                              new_data_table_id,
                                              inner_index_name,
                                              user_index_name))) {
      LOG_WARN("fail to get index name",
          K(ret), K(new_data_table_id), K(inner_index_name));
    } else if (share::schema::is_built_in_fts_index(type) &&
        OB_FAIL(ObFtsIndexBuilderUtil::generate_fts_aux_index_name(&allocator,
                                                                   type,
                                                                   user_index_name,
                                                                   index_aux_name))) {
      LOG_WARN("fail to generate fts aux index name", K(ret), K(user_index_name));
    } else if (share::schema::is_built_in_vec_index(type) &&
               OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                      type,
                                                                      user_index_name,
                                                                      index_aux_name))) {
      LOG_WARN("fail to generate vec index name", K(ret), K(user_index_name));
    } else if (index_aux_name.empty()){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index aux name is empty, maybe the index type is incorrect",
          K(ret), K(user_index_name), K(type));
    } else if (OB_FAIL(ObTableSchema::build_index_table_name(allocator,
                                                             new_data_table_id,
                                                             index_aux_name,
                                                             inner_index_aux_name))) {
      LOG_WARN("fail to build inner index table name", K(ret), K(index_aux_name));
    } else {
      for (int64_t i = 0; i < aux_schema_array.count(); ++i) {
        const ObTableSchema &aux_index_schema = aux_schema_array.at(i);
        if (inner_index_aux_name == aux_index_schema.get_table_name_str()) {
          index_aux_schema_idx = i;
          break;
        }
      }
    }
  }
  return ret;
}

}//end namespace share
}//end namespace oceanbase