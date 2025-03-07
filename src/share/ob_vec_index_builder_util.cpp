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
#include "ob_vec_index_builder_util.h"
#include "ob_index_builder_util.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;

namespace share
{
// hnsw
const char * ObVecIndexBuilderUtil::ROWKEY_VID_TABLE_NAME = "rowkey_vid_table";
const char * ObVecIndexBuilderUtil::VID_ROWKEY_TABLE_NAME = "vid_rowkey_table";
const char * ObVecIndexBuilderUtil::DELTA_BUFFER_TABLE_NAME_SUFFIX = "";
const char * ObVecIndexBuilderUtil::INDEX_ID_TABLE_NAME_SUFFIX = "_index_id_table";
const char * ObVecIndexBuilderUtil::SNAPSHOT_DATA_TABLE_NAME_SUFFIX = "_index_snapshot_data_table";

// ivf
//const char * ObVecIndexBuilderUtil::IVFSQ8_CENTROID_TABLE_NAME_SUFFIX = "";
//const char * ObVecIndexBuilderUtil::IVFPQ_CENTROID_TABLE_NAME_SUFFIX = "";
const char * ObVecIndexBuilderUtil::IVF_CENTROID_TABLE_NAME_SUFFIX = "";
const char * ObVecIndexBuilderUtil::IVF_ROWKEY_CID_TABLE_NAME_SUFFIX = "_ivf_rowkey_cid";
const char * ObVecIndexBuilderUtil::IVF_CID_VECTOR_TABLE_NAME_SUFFIX = "_ivf_cid_vector";
const char * ObVecIndexBuilderUtil::IVF_SQ_META_TABLE_NAME_SUFFIX = "_ivf_sq_meta";
const char * ObVecIndexBuilderUtil::IVF_PQ_CENTROID_TABLE_NAME_SUFFIX = "_ivf_pq_centroid";
const char * ObVecIndexBuilderUtil::IVF_PQ_CODE_TABLE_NAME_SUFFIX = "_ivf_pq_code";
const char * ObVecIndexBuilderUtil::IVF_PQ_ROWKEY_CID_TABLE_NAME_SUFFIX = "_ivf_pq_rowkey_cid";
const char * ObVecIndexBuilderUtil::IVF_PQ_CENTER_IDS_COL_TYPE_NAME = "ARRAY(VARBINARY)";


int ObVecIndexBuilderUtil::append_vec_args(
    const sql::ObPartitionResolveResult &resolve_result,
    const obrpc::ObCreateIndexArg &index_arg,
    bool &vec_common_aux_table_exist,
    ObIArray<sql::ObPartitionResolveResult> &resolve_results,
    ObIArray<ObCreateIndexArg> &index_arg_list,
    ObIAllocator *allocator,
    const ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  uint64_t tenant_version = 0;
  if (OB_ISNULL(allocator) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret), KP(allocator), KP(session_info));
  } else if (FALSE_IT(tenant_id = session_info->get_effective_tenant_id())) {
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_version))) {
    LOG_WARN("failed to get data version", K(ret), K(tenant_id));
  } else if (share::schema::is_vec_hnsw_index(index_arg.index_type_) &&
             tenant_version < DATA_VERSION_4_3_3_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "vec hnsw index before 4.3.3.0 is");
    LOG_WARN("vec hnsw index is not supported before 4.3.3.0", K(ret), K(tenant_version));
  } else if (share::schema::is_vec_ivf_index(index_arg.index_type_) &&
             tenant_version < DATA_VERSION_4_3_5_1) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "vec ivf index before 4.3.5.1 is");
    LOG_WARN("vec ivf index is not supported before 4.3.5.1", K(ret), K(tenant_version));
  } else {
    if (index_arg.index_type_ == INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL &&
        OB_FAIL(ObVecIndexBuilderUtil::append_vec_hnsw_args(resolve_result,
                                                            index_arg,
                                                            vec_common_aux_table_exist,
                                                            resolve_results,
                                                            index_arg_list,
                                                            allocator,
                                                            session_info))) {
      LOG_WARN("fail to append vec hnsw args", K(ret));
    } else if (index_arg.index_type_ == INDEX_TYPE_VEC_IVFFLAT_CENTROID_LOCAL &&
        OB_FAIL(ObVecIndexBuilderUtil::append_vec_ivfflat_args(resolve_result,
                                                            index_arg,
                                                            resolve_results,
                                                            index_arg_list,
                                                            allocator))) {
      LOG_WARN("fail to append vec ivfflat args", K(ret));
    } else if (index_arg.index_type_ == INDEX_TYPE_VEC_IVFSQ8_CENTROID_LOCAL &&
        OB_FAIL(ObVecIndexBuilderUtil::append_vec_ivfsq8_args(resolve_result,
                                                            index_arg,
                                                            resolve_results,
                                                            index_arg_list,
                                                            allocator))) {
      LOG_WARN("fail to append vec ivfsq8 args", K(ret));
    } else if (index_arg.index_type_ == INDEX_TYPE_VEC_IVFPQ_CENTROID_LOCAL &&
        OB_FAIL(ObVecIndexBuilderUtil::append_vec_ivfpq_args(resolve_result,
                                                            index_arg,
                                                            resolve_results,
                                                            index_arg_list,
                                                            allocator))) {
      LOG_WARN("fail to append vec ivfpq args", K(ret));
    }
  }
  LOG_DEBUG("finish append vec index args", K(index_arg), K(index_arg_list));
  return ret;
}

int ObVecIndexBuilderUtil::append_vec_hnsw_args(
    const sql::ObPartitionResolveResult &resolve_result,
    const obrpc::ObCreateIndexArg &index_arg,
    bool &vec_common_aux_table_exist,
    ObIArray<sql::ObPartitionResolveResult> &resolve_results,
    ObIArray<ObCreateIndexArg> &index_arg_list,
    ObIAllocator *allocator,
    const sql::ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else if (!vec_common_aux_table_exist) {
    const int64_t num_vec_args = 5;
    // append domain table first
    if (OB_FAIL(append_vec_delta_buffer_arg(index_arg, allocator, session_info, index_arg_list))) {
      LOG_WARN("failed to append vec delta_buffer_table arg", K(ret));
    } else if (OB_FAIL(append_vec_rowkey_vid_arg(index_arg, allocator, index_arg_list))) {
      LOG_WARN("failed to append vec rowkey_vid_table arg", K(ret));
    } else if (OB_FAIL(append_vec_vid_rowkey_arg(index_arg, allocator, index_arg_list))) {
      LOG_WARN("failed to append vec vid_rowkey_table arg", K(ret));
    } else if (OB_FAIL(append_vec_index_id_arg(index_arg, allocator, index_arg_list))) {
      LOG_WARN("failed to append vec index_id_table arg", K(ret));
    } else if (OB_FAIL(append_vec_index_snapshot_data_arg(index_arg, allocator, index_arg_list))) {
      LOG_WARN("failed to append vec index_snapshot_data_table arg", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < num_vec_args; ++i) {
      if (OB_FAIL(resolve_results.push_back(resolve_result))) {
        LOG_WARN("fail to push back index_stmt_list", K(ret), K(resolve_result));
      }
    }
    if (OB_SUCC(ret)) {
      vec_common_aux_table_exist = true;
    }
  } else {
    const int64_t num_vec_args = 3; // 如果一个主表中已经创建过向量索引，那么只需要新增 3 张非共享索引辅助表
    if (OB_FAIL(append_vec_delta_buffer_arg(index_arg, allocator, session_info, index_arg_list))) {
      LOG_WARN("failed to append vec delta_buffer_table arg", K(ret));
    } else if (OB_FAIL(append_vec_index_id_arg(index_arg, allocator, index_arg_list))) {
      LOG_WARN("failed to append vec index_id_table arg", K(ret));
    } else if (OB_FAIL(append_vec_index_snapshot_data_arg(index_arg, allocator, index_arg_list))) {
      LOG_WARN("failed to append vec index_snapshot_data_table arg", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < num_vec_args; ++i) {
      if (OB_FAIL(resolve_results.push_back(resolve_result))) {
        LOG_WARN("fail to push back index_stmt_list", K(ret), K(resolve_result));
      }
    }
  }
  LOG_DEBUG("finish append vec index args", K(index_arg), K(index_arg_list));
  return ret;
}

int ObVecIndexBuilderUtil::append_vec_ivfflat_args(
    const sql::ObPartitionResolveResult &resolve_result,
    const obrpc::ObCreateIndexArg &index_arg,
    ObIArray<sql::ObPartitionResolveResult> &resolve_results,
    ObIArray<ObCreateIndexArg> &index_arg_list,
    ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else {
    const int64_t num_vec_args = 3;
    // append domain table first
    if (OB_FAIL(append_vec_ivf_arg(index_arg, INDEX_TYPE_VEC_IVFFLAT_CENTROID_LOCAL, allocator, index_arg_list))) {
      LOG_WARN("failed to append vec delta_buffer_table arg", K(ret));
    } else if (OB_FAIL(append_vec_ivf_arg(index_arg, INDEX_TYPE_VEC_IVFFLAT_CID_VECTOR_LOCAL, allocator, index_arg_list))) {
      LOG_WARN("failed to append vec rowkey_vid_table arg", K(ret));
    } else if (OB_FAIL(append_vec_ivf_arg(index_arg, INDEX_TYPE_VEC_IVFFLAT_ROWKEY_CID_LOCAL, allocator, index_arg_list))) {
      LOG_WARN("failed to append vec rowkey_vid_table arg", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < num_vec_args; ++i) {
      if (OB_FAIL(resolve_results.push_back(resolve_result))) {
        LOG_WARN("fail to push back index_stmt_list", K(ret), K(resolve_result));
      }
    }
  }
  LOG_DEBUG("finish append vec ivfflat index args", K(index_arg), K(index_arg_list));
  return ret;
}

int ObVecIndexBuilderUtil::append_vec_ivfsq8_args(
    const sql::ObPartitionResolveResult &resolve_result,
    const obrpc::ObCreateIndexArg &index_arg,
    ObIArray<sql::ObPartitionResolveResult> &resolve_results,
    ObIArray<ObCreateIndexArg> &index_arg_list,
    ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else {
    const int64_t num_vec_args = 4;
    // append domain table first
    if (OB_FAIL(append_vec_ivf_arg(index_arg, INDEX_TYPE_VEC_IVFSQ8_CENTROID_LOCAL, allocator, index_arg_list))) {
      LOG_WARN("failed to append vec delta_buffer_table arg", K(ret));
    } else if (OB_FAIL(append_vec_ivf_arg(index_arg, INDEX_TYPE_VEC_IVFSQ8_META_LOCAL, allocator, index_arg_list))) {
      LOG_WARN("failed to append vec rowkey_vid_table arg", K(ret));
    } else if (OB_FAIL(append_vec_ivf_arg(index_arg, INDEX_TYPE_VEC_IVFSQ8_CID_VECTOR_LOCAL, allocator, index_arg_list))) {
      LOG_WARN("failed to append vec rowkey_vid_table arg", K(ret));
    } else if (OB_FAIL(append_vec_ivf_arg(index_arg, INDEX_TYPE_VEC_IVFSQ8_ROWKEY_CID_LOCAL, allocator, index_arg_list))) {
      LOG_WARN("failed to append vec rowkey_vid_table arg", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < num_vec_args; ++i) {
      if (OB_FAIL(resolve_results.push_back(resolve_result))) {
        LOG_WARN("fail to push back index_stmt_list", K(ret), K(resolve_result));
      }
    }
  }
  LOG_DEBUG("finish append vec ivfsq8 index args", K(index_arg), K(index_arg_list));
  return ret;
}

int ObVecIndexBuilderUtil::append_vec_ivfpq_args(
    const sql::ObPartitionResolveResult &resolve_result,
    const obrpc::ObCreateIndexArg &index_arg,
    ObIArray<sql::ObPartitionResolveResult> &resolve_results,
    ObIArray<ObCreateIndexArg> &index_arg_list,
    ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else {
    const int64_t num_vec_args = 4;
    // append domain table first
    if (OB_FAIL(append_vec_ivf_arg(index_arg, INDEX_TYPE_VEC_IVFPQ_CENTROID_LOCAL, allocator, index_arg_list))) {
      LOG_WARN("failed to append vec ivf arg", K(ret));
    } else if (OB_FAIL(append_vec_ivf_arg(index_arg, INDEX_TYPE_VEC_IVFPQ_PQ_CENTROID_LOCAL, allocator, index_arg_list))) {
      LOG_WARN("failed to append vec ivf arg", K(ret));
    } else if (OB_FAIL(append_vec_ivf_arg(index_arg, INDEX_TYPE_VEC_IVFPQ_CODE_LOCAL, allocator, index_arg_list))) {
      LOG_WARN("failed to append vec ivf arg", K(ret));
    } else if (OB_FAIL(append_vec_ivf_arg(index_arg, INDEX_TYPE_VEC_IVFPQ_ROWKEY_CID_LOCAL, allocator, index_arg_list))) {
      LOG_WARN("failed to append vec ivf arg", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < num_vec_args; ++i) {
      if (OB_FAIL(resolve_results.push_back(resolve_result))) {
        LOG_WARN("fail to push back index_stmt_list", K(ret), K(resolve_result));
      }
    }
  }
  LOG_DEBUG("finish append vec ivfpq index args", K(index_arg), K(index_arg_list));
  return ret;
}

int ObVecIndexBuilderUtil::append_vec_ivf_arg(
    const ObCreateIndexArg &index_arg,
    const ObIndexType index_type,
    ObIAllocator *allocator,
    ObIArray<ObCreateIndexArg> &index_arg_list)
{
  int ret = OB_SUCCESS;
  ObCreateIndexArg vec_index_arg;
  ObString domain_index_name = index_arg.index_name_;
  if (OB_ISNULL(allocator) || !(is_vec_index(index_type)) || !(is_vec_index(index_arg.index_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret), K(index_arg.index_type_), K(index_type));
  } else if (OB_FAIL(vec_index_arg.assign(index_arg))) {
    LOG_WARN("failed to assign to vec ivf index arg", K(ret));
  } else if (FALSE_IT(vec_index_arg.index_type_ = index_type)) {
  } else if (OB_FAIL(generate_vec_index_name(allocator,
                                             vec_index_arg.index_type_,
                                             domain_index_name,
                                             vec_index_arg.index_name_))) {
    LOG_WARN("failed to generate vec ivf index name", K(ret));
  } else if (OB_FAIL(index_arg_list.push_back(vec_index_arg))) {
    LOG_WARN("failed to push back vec ivf index arg", K(ret));
  }
  return ret;
}

int ObVecIndexBuilderUtil::append_vec_rowkey_vid_arg(
    const ObCreateIndexArg &index_arg,
    ObIAllocator *allocator,
    ObIArray<ObCreateIndexArg> &index_arg_list)
{
  int ret = OB_SUCCESS;
  ObCreateIndexArg vec_rowkey_vid_arg;
  ObString empty_domain_index_name;
  if (OB_ISNULL(allocator) || !(is_vec_index(index_arg.index_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret), K(index_arg.index_type_));
  } else if (OB_FAIL(vec_rowkey_vid_arg.assign(index_arg))) {
    LOG_WARN("failed to assign to vec rowkey vid arg", K(ret));
  } else if (FALSE_IT(vec_rowkey_vid_arg.index_type_ = INDEX_TYPE_VEC_ROWKEY_VID_LOCAL)) {
  } else if (OB_FAIL(generate_vec_index_name(allocator,
                                             vec_rowkey_vid_arg.index_type_,
                                             empty_domain_index_name,
                                             vec_rowkey_vid_arg.index_name_))) {
    LOG_WARN("failed to generate vec index name", K(ret));
  } else if (OB_FAIL(index_arg_list.push_back(vec_rowkey_vid_arg))) {
    LOG_WARN("failed to push back vec rowkey vid arg", K(ret));
  }
  return ret;
}

int ObVecIndexBuilderUtil::append_vec_vid_rowkey_arg(
    const obrpc::ObCreateIndexArg &index_arg,
    ObIAllocator *allocator,
    ObIArray<obrpc::ObCreateIndexArg> &index_arg_list)
{
  int ret = OB_SUCCESS;
  ObCreateIndexArg vec_vid_rowkey_arg;
  ObString empty_domain_index_name;
  if (OB_ISNULL(allocator) || !(is_vec_index(index_arg.index_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret), K(index_arg.index_type_));
  } else if (OB_FAIL(vec_vid_rowkey_arg.assign(index_arg))) {
    LOG_WARN("failed to assign to vec vid rowkey arg", K(ret));
  } else if (FALSE_IT(vec_vid_rowkey_arg.index_type_ = INDEX_TYPE_VEC_VID_ROWKEY_LOCAL)) {
  } else if (OB_FAIL(generate_vec_index_name(allocator,
                                             vec_vid_rowkey_arg.index_type_,
                                             empty_domain_index_name,
                                             vec_vid_rowkey_arg.index_name_))) {
    LOG_WARN("failed to generate vec index name", K(ret));
  } else if (OB_FAIL(index_arg_list.push_back(vec_vid_rowkey_arg))) {
    LOG_WARN("failed to push back vec vid rowkey arg", K(ret));
  }
  return ret;
}

int ObVecIndexBuilderUtil::append_vec_delta_buffer_arg(
    const obrpc::ObCreateIndexArg &index_arg,
    ObIAllocator *allocator,
    const sql::ObSQLSessionInfo *session_info,
    ObIArray<obrpc::ObCreateIndexArg> &index_arg_list)
{
  int ret = OB_SUCCESS;
  ObCreateIndexArg vec_delta_buffer_arg;
  char* buf = nullptr;
  int64_t pos = 0;
  ObString domain_index_name = index_arg.index_name_;
  if (OB_ISNULL(allocator) || OB_ISNULL(session_info) || !(is_vec_index(index_arg.index_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret), K(index_arg.index_type_));
  } else if (OB_ISNULL(buf = reinterpret_cast<char*>(allocator->alloc(sizeof(char) * OB_MAX_PROC_ENV_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buffer", KR(ret), K(OB_MAX_PROC_ENV_LENGTH));
  } else if (OB_FAIL(ObExecEnv::gen_exec_env(*session_info, buf, OB_MAX_PROC_ENV_LENGTH, pos))) {
    LOG_WARN("fail to gen exec env", KR(ret));
  } else if (OB_FAIL(vec_delta_buffer_arg.assign(index_arg))) {
    LOG_WARN("failed to assign to vec delta buffer arg", K(ret));
  } else if (FALSE_IT(vec_delta_buffer_arg.index_type_ = INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL)) {
  } else if (OB_FAIL(generate_vec_index_name(allocator,
                                             vec_delta_buffer_arg.index_type_,
                                             domain_index_name,
                                             vec_delta_buffer_arg.index_name_))) {
    LOG_WARN("failed to generate vec index name", K(ret));
  } else if (FALSE_IT(vec_delta_buffer_arg.vidx_refresh_info_.exec_env_.assign_ptr(buf, pos))) {
  } else if (OB_FAIL(index_arg_list.push_back(vec_delta_buffer_arg))) {
    LOG_WARN("failed to push back vec delta buffer arg", K(ret));
  }
  return ret;
}


int ObVecIndexBuilderUtil::append_vec_index_id_arg(
    const obrpc::ObCreateIndexArg &index_arg,
    ObIAllocator *allocator,
    ObIArray<obrpc::ObCreateIndexArg> &index_arg_list)
{
  int ret = OB_SUCCESS;
  ObCreateIndexArg vec_index_id_arg;
  ObString domain_index_name = index_arg.index_name_;
  if (OB_ISNULL(allocator) || !(is_vec_index(index_arg.index_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret), K(index_arg.index_type_));
  } else if (OB_FAIL(vec_index_id_arg.assign(index_arg))) {
    LOG_WARN("failed to assign to vec index id arg", K(ret));
  } else if (FALSE_IT(vec_index_id_arg.index_type_ = INDEX_TYPE_VEC_INDEX_ID_LOCAL)) {
  } else if (OB_FAIL(generate_vec_index_name(allocator,
                                             vec_index_id_arg.index_type_,
                                             domain_index_name,
                                             vec_index_id_arg.index_name_))) {
    LOG_WARN("failed to generate vec index name", K(ret));
  } else if (OB_FAIL(index_arg_list.push_back(vec_index_id_arg))) {
    LOG_WARN("failed to push back vec index id arg", K(ret));
  }
  return ret;
}


int ObVecIndexBuilderUtil::append_vec_index_snapshot_data_arg(
    const obrpc::ObCreateIndexArg &index_arg,
    ObIAllocator *allocator,
    ObIArray<obrpc::ObCreateIndexArg> &index_arg_list)
{
  int ret = OB_SUCCESS;
  ObCreateIndexArg vec_index_snapshot_data_arg;
  ObString domain_index_name = index_arg.index_name_;
  if (OB_ISNULL(allocator) || !(is_vec_index(index_arg.index_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret), K(index_arg.index_type_));
  } else if (OB_FAIL(vec_index_snapshot_data_arg.assign(index_arg))) {
    LOG_WARN("failed to assign to snapshot data arg", K(ret));
  } else if (FALSE_IT(vec_index_snapshot_data_arg.index_type_ = INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL)) {
  } else if (OB_FAIL(generate_vec_index_name(allocator,
                                             vec_index_snapshot_data_arg.index_type_,
                                             domain_index_name,
                                             vec_index_snapshot_data_arg.index_name_))) {
    LOG_WARN("failed to generate vec index name", K(ret));
  } else if (OB_FAIL(index_arg_list.push_back(vec_index_snapshot_data_arg))) {
    LOG_WARN("failed to push back vec snapshot data arg", K(ret));
  }
  return ret;
}

int ObVecIndexBuilderUtil::check_vec_index_allowed(
    ObTableSchema &data_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!data_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema));
  } else if (data_schema.is_partitioned_table() && data_schema.is_table_without_pk()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "create vector index on partition table without primary key");
  }
  return ret;
}

int ObVecIndexBuilderUtil::generate_vec_hnsw_index_name(
    const share::schema::ObIndexType type,
    const ObString &index_name,
    char *name_buf,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(name_buf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to generate vec ivf index name", K(ret));
  } else if (share::schema::is_vec_rowkey_vid_type(type) &&
             OB_FAIL(databuff_printf(name_buf,
                                     OB_MAX_TABLE_NAME_LENGTH,
                                     pos,
                                     "%s",
                                     ROWKEY_VID_TABLE_NAME))) {
    LOG_WARN("failed to print", K(ret));
  } else if (share::schema::is_vec_vid_rowkey_type(type) &&
             OB_FAIL(databuff_printf(name_buf,
                                     OB_MAX_TABLE_NAME_LENGTH,
                                     pos,
                                     "%s",
                                     VID_ROWKEY_TABLE_NAME))) {
    LOG_WARN("failed to print", K(ret));
  } else if (share::schema::is_vec_delta_buffer_type(type) &&
             OB_FAIL(databuff_printf(name_buf,
                                     OB_MAX_TABLE_NAME_LENGTH,
                                     pos,
                                     "%.*s%s",
                                     index_name.length(),
                                     index_name.ptr(),
                                     DELTA_BUFFER_TABLE_NAME_SUFFIX))) {
    LOG_WARN("failed to print", K(ret));
  } else if (share::schema::is_vec_index_id_type(type) &&
             OB_FAIL(databuff_printf(name_buf,
                                     OB_MAX_TABLE_NAME_LENGTH,
                                     pos,
                                     "%.*s%s",
                                     index_name.length(),
                                     index_name.ptr(),
                                     INDEX_ID_TABLE_NAME_SUFFIX))) {
    LOG_WARN("failed to print", K(ret));
  } else if (share::schema::is_vec_index_snapshot_data_type(type) &&
             OB_FAIL(databuff_printf(name_buf,
                                     OB_MAX_TABLE_NAME_LENGTH,
                                     pos,
                                     "%.*s%s",
                                     index_name.length(),
                                     index_name.ptr(),
                                     SNAPSHOT_DATA_TABLE_NAME_SUFFIX))) {
    LOG_WARN("failed to print", K(ret));
  }
  return ret;
}

int ObVecIndexBuilderUtil::generate_vec_ivf_index_name(
    const share::schema::ObIndexType type,
    const ObString &index_name,
    char *name_buf,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(name_buf)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to generate vec ivf index name", K(ret));
  } else if (share::schema::is_vec_ivfflat_centroid_index(type) &&
             OB_FAIL(databuff_printf(name_buf,
                                     OB_MAX_TABLE_NAME_LENGTH,
                                     pos,
                                     "%.*s%s",
                                     index_name.length(),
                                     index_name.ptr(),
                                     IVF_CENTROID_TABLE_NAME_SUFFIX))) {
    LOG_WARN("failed to print ivf index table name", K(ret));
  } else if (share::schema::is_vec_ivfsq8_centroid_index(type) &&
             OB_FAIL(databuff_printf(name_buf,
                                     OB_MAX_TABLE_NAME_LENGTH,
                                     pos,
                                     "%.*s%s",
                                     index_name.length(),
                                     index_name.ptr(),
                                     IVF_CENTROID_TABLE_NAME_SUFFIX))) {
    LOG_WARN("failed to print ivf index table name", K(ret));
  } else if (share::schema::is_vec_ivfpq_centroid_index(type) &&
             OB_FAIL(databuff_printf(name_buf,
                                     OB_MAX_TABLE_NAME_LENGTH,
                                     pos,
                                     "%.*s%s",
                                     index_name.length(),
                                     index_name.ptr(),
                                     IVF_CENTROID_TABLE_NAME_SUFFIX))) {
    LOG_WARN("failed to print ivf index table name", K(ret));
  } else if (share::schema::is_vec_ivfflat_rowkey_cid_index(type) &&
             OB_FAIL(databuff_printf(name_buf,
                                     OB_MAX_TABLE_NAME_LENGTH,
                                     pos,
                                     "%.*s%s",
                                     index_name.length(),
                                     index_name.ptr(),
                                     IVF_ROWKEY_CID_TABLE_NAME_SUFFIX))) {
    LOG_WARN("failed to print ivf index table name", K(ret));
  } else if (share::schema::is_vec_ivfsq8_rowkey_cid_index(type) &&
             OB_FAIL(databuff_printf(name_buf,
                                     OB_MAX_TABLE_NAME_LENGTH,
                                     pos,
                                     "%.*s%s",
                                     index_name.length(),
                                     index_name.ptr(),
                                     IVF_ROWKEY_CID_TABLE_NAME_SUFFIX))) {
    LOG_WARN("failed to print ivf index table name", K(ret));
  } else if (share::schema::is_vec_ivfpq_rowkey_cid_index(type) &&
             OB_FAIL(databuff_printf(name_buf,
                                     OB_MAX_TABLE_NAME_LENGTH,
                                     pos,
                                     "%.*s%s",
                                     index_name.length(),
                                     index_name.ptr(),
                                     IVF_PQ_ROWKEY_CID_TABLE_NAME_SUFFIX))) {
    LOG_WARN("failed to print ivf index table name", K(ret));
  } else if (share::schema::is_vec_ivfflat_cid_vector_index(type) &&
             OB_FAIL(databuff_printf(name_buf,
                                     OB_MAX_TABLE_NAME_LENGTH,
                                     pos,
                                     "%.*s%s",
                                     index_name.length(),
                                     index_name.ptr(),
                                     IVF_CID_VECTOR_TABLE_NAME_SUFFIX))) {
    LOG_WARN("failed to print ivf index table name", K(ret));
  } else if (share::schema::is_vec_ivfsq8_cid_vector_index(type) &&
             OB_FAIL(databuff_printf(name_buf,
                                     OB_MAX_TABLE_NAME_LENGTH,
                                     pos,
                                     "%.*s%s",
                                     index_name.length(),
                                     index_name.ptr(),
                                     IVF_CID_VECTOR_TABLE_NAME_SUFFIX))) {
    LOG_WARN("failed to print ivf index table name", K(ret));
  } else if (share::schema::is_vec_ivfpq_pq_centroid_index(type) &&
             OB_FAIL(databuff_printf(name_buf,
                                     OB_MAX_TABLE_NAME_LENGTH,
                                     pos,
                                     "%.*s%s",
                                     index_name.length(),
                                     index_name.ptr(),
                                     IVF_PQ_CENTROID_TABLE_NAME_SUFFIX))) {
    LOG_WARN("failed to print ivf index table name", K(ret));
  } else if (share::schema::is_vec_ivfpq_code_index(type) &&
             OB_FAIL(databuff_printf(name_buf,
                                     OB_MAX_TABLE_NAME_LENGTH,
                                     pos,
                                     "%.*s%s",
                                     index_name.length(),
                                     index_name.ptr(),
                                     IVF_PQ_CODE_TABLE_NAME_SUFFIX))) {
    LOG_WARN("failed to print ivf index table name", K(ret));
  } else if (share::schema::is_vec_ivfsq8_meta_index(type) &&
             OB_FAIL(databuff_printf(name_buf,
                                     OB_MAX_TABLE_NAME_LENGTH,
                                     pos,
                                     "%.*s%s",
                                     index_name.length(),
                                     index_name.ptr(),
                                     IVF_SQ_META_TABLE_NAME_SUFFIX))) {
    LOG_WARN("failed to print ivf index table name", K(ret));
  }
  return ret;
}


int ObVecIndexBuilderUtil::generate_vec_index_name(
    ObIAllocator *allocator,
    const share::schema::ObIndexType type,
    const ObString &index_name,
    ObString &new_index_name)
{
  int ret = OB_SUCCESS;
  char *name_buf = nullptr;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is nullptr", K(ret));
  } else if (!share::schema::is_vec_index(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type));
  } else if (OB_ISNULL(name_buf = static_cast<char *>(allocator->alloc(OB_MAX_TABLE_NAME_LENGTH)))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc mem", K(ret));
  } else {
    int64_t pos = 0;
    if (share::schema::is_vec_ivf_index(type)) {
      if (OB_FAIL(generate_vec_ivf_index_name(type, index_name, name_buf, pos))) {
        LOG_WARN("fail to generate vec ivf index name", K(ret), K(type));
      }
    } else if (share::schema::is_vec_hnsw_index(type)) {
      if (OB_FAIL(generate_vec_hnsw_index_name(type, index_name, name_buf, pos))) {
        LOG_WARN("fail to generate vec ivf index name", K(ret), K(type));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected index type", K(ret), K(type));
    }
    if (OB_SUCC(ret)) {
      new_index_name.assign_ptr(name_buf, static_cast<int32_t>(pos));
    } else {
      LOG_WARN("failed to generate vec aux index name", K(ret));
    }
  }
  LOG_DEBUG("finish generate_vec_index_name", K(ret), K(index_name), K(new_index_name));
  return ret;
}

int ObVecIndexBuilderUtil::check_ivf_store_column_count(const ObCreateIndexArg &arg)
{
  int ret = OB_SUCCESS;
  ObIndexType index_type = arg.index_type_;
  const int64_t count = arg.store_columns_.count();
  if (share::schema::is_vec_ivfflat_centroid_index(index_type) && count != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ivf centroid store_column cnt", K(ret), K(count));
  } else if (share::schema::is_vec_ivfflat_cid_vector_index(index_type) && count != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ivf centroid store_column cnt", K(ret), K(count));
  } else if (share::schema::is_vec_ivfflat_rowkey_cid_index(index_type) && count != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ivf centroid store_column cnt", K(ret), K(count));
  } else if (share::schema::is_vec_ivfsq8_meta_index(index_type) && count != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ivf centroid store_column cnt", K(ret), K(count));
  } else if (share::schema::is_vec_ivfsq8_centroid_index(index_type) && count != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ivf centroid store_column cnt", K(ret), K(count));
  } else if (share::schema::is_vec_ivfsq8_cid_vector_index(index_type) && count != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ivf centroid store_column cnt", K(ret), K(count));
  } else if (share::schema::is_vec_ivfsq8_rowkey_cid_index(index_type) && count != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ivf centroid store_column cnt", K(ret), K(count));
  } else if (share::schema::is_vec_ivfpq_centroid_index(index_type) && count != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ivf centroid store_column cnt", K(ret), K(count));
  } else if (share::schema::is_vec_ivfpq_pq_centroid_index(index_type) && count != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ivf centroid store_column cnt", K(ret), K(count));
  } else if (share::schema::is_vec_ivfpq_code_index(index_type) && count != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ivf centroid store_column cnt", K(ret), K(count));
  } else if (share::schema::is_vec_ivfpq_rowkey_cid_index(index_type) && count != 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ivf centroid store_column cnt", K(ret), K(count));
  }

  return ret;
}

int ObVecIndexBuilderUtil::set_vec_ivf_table_columns(
    const ObCreateIndexArg &arg,
    const ObTableSchema &data_schema,
    ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  if (!data_schema.is_valid() || !share::schema::is_vec_ivf_index(arg.index_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), K(arg.index_type_));
  } else if (OB_FAIL(check_ivf_store_column_count(arg))) {
    LOG_WARN("fail to check store column count", K(ret), K(arg));
  } else {
    HEAP_VAR(ObRowDesc, row_desc) {
      // 1. add rowkey columns
      for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_columns_.count(); ++i) {
        const ObColumnSchemaV2 *rowkey_column = nullptr;
        const ObColumnSortItem &rowkey_col_item = arg.index_columns_.at(i);
        const ObString &rowkey_col_name = rowkey_col_item.column_name_;
        if (rowkey_col_name.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column name is empty", K(ret), K(rowkey_col_name));
        } else if (OB_ISNULL(rowkey_column = data_schema.get_column_schema(rowkey_col_name))) {
          ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
          LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS,
                        rowkey_col_name.length(),
                        rowkey_col_name.ptr());
          LOG_WARN("get_column_schema failed",
                  "tenant_id", data_schema.get_tenant_id(),
                  "database_id", data_schema.get_database_id(),
                  "table_name", data_schema.get_table_name(),
                  "column name", rowkey_col_name, K(ret));
        } else if (OB_FAIL(ObIndexBuilderUtil::add_column(rowkey_column,
                                                          true/*is_index_column*/,
                                                          true/*is_rowkey*/,
                                                          arg.index_columns_.at(i).order_type_,
                                                          row_desc,
                                                          index_schema,
                                                          false/*is_hidden*/,
                                                          false/*is_specified_storing_col*/))) {
          LOG_WARN("failed to add column", K(ret), KPC(rowkey_column), K(row_desc));
        }
      }
      if (OB_SUCC(ret)) {
        index_schema.set_rowkey_column_num(row_desc.get_column_num());
        index_schema.set_index_column_num(row_desc.get_column_num());
      }
        // 2. add store column
      for (int64_t i = 0; OB_SUCC(ret) && i < arg.store_columns_.count(); ++i) {
        const ObColumnSchemaV2 *tmp_column = nullptr;
        const ObString &tmp_col_name = arg.store_columns_.at(i);
        const ObOrderType order_in_rowkey = ObOrderType::DESC;
        if (tmp_col_name.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column name is empty", K(ret), K(tmp_col_name));
        } else if (OB_ISNULL(tmp_column = data_schema.get_column_schema(tmp_col_name))) {
          ret = OB_ERR_BAD_FIELD_ERROR;
          LOG_WARN("get_column_schema failed", K(ret), K(arg.index_type_), K(tmp_col_name),  K(data_schema));
        } else if (OB_FAIL(ObIndexBuilderUtil::add_column(tmp_column,
                                                          false/*is_index_column*/,
                                                          false/*is_rowkey*/,
                                                          order_in_rowkey,
                                                          row_desc,
                                                          index_schema,
                                                          false/*is_hidden*/,
                                                          true/*is_specified_storing_col*/))) {
          LOG_WARN("failed to add column", K(ret), KPC(tmp_column), K(row_desc));
        }
      }
      // 3. add part key column
      bool need_part_key_column = share::schema::is_local_vec_ivf_centroid_index(arg.index_type_) ||
                                  share::schema::is_vec_ivfsq8_meta_index(arg.index_type_) ||
                                  share::schema::is_vec_ivfpq_pq_centroid_index(arg.index_type_);
      if (OB_FAIL(ret)) {
      } else if (need_part_key_column &&
          OB_FAIL(set_part_key_columns(data_schema, index_schema))) {
        // centroid/pq_centroid/sq8_meta need part key
        LOG_WARN("fail to generate part key columns", K(ret));
      }
      if (FAILEDx(index_schema.sort_column_array_by_column_id())) {
        LOG_WARN("failed to sort column", K(ret));
      } else {
        LOG_INFO("succeed to set ivf table columns", K(arg.index_type_), K(index_schema));
      }
    } // ObRowDesc
  }
  LOG_DEBUG("finish set ivf table column", K(ret), K(arg), K(index_schema), K(data_schema));
  return ret;
}

int ObVecIndexBuilderUtil::set_vec_rowkey_vid_table_columns(
    const ObCreateIndexArg &arg,
    const ObTableSchema &data_schema,
    ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  if (!data_schema.is_valid() ||
      arg.store_columns_.count() != 1 ||    /* vid column */
      !share::schema::is_vec_rowkey_vid_type(arg.index_type_)) {
    // expect vid column in store columns
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),
      K(data_schema), K(arg.store_columns_.count()), K(arg.index_type_));
  }
  HEAP_VAR(ObRowDesc, row_desc) {
    // 1. add rowkey_vid_table rowkey columns
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_columns_.count(); ++i) {
      const ObColumnSchemaV2 *rowkey_column = nullptr;
      const ObColumnSortItem &rowkey_col_item = arg.index_columns_.at(i);
      const ObString &rowkey_col_name = rowkey_col_item.column_name_;
      if (rowkey_col_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(rowkey_col_name));
      } else if (OB_ISNULL(rowkey_column = data_schema.get_column_schema(rowkey_col_name))) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS,
                       rowkey_col_name.length(),
                       rowkey_col_name.ptr());
        LOG_WARN("get_column_schema failed",
                 "tenant_id", data_schema.get_tenant_id(),
                 "database_id", data_schema.get_database_id(),
                 "table_name", data_schema.get_table_name(),
                 "column name", rowkey_col_name, K(ret));
      } else if (OB_FAIL(ObIndexBuilderUtil::add_column(rowkey_column,
                                                        true/*is_index_column*/,
                                                        true/*is_rowkey*/,
                                                        arg.index_columns_.at(i).order_type_,
                                                        row_desc,
                                                        index_schema,
                                                        false/*is_hidden*/,
                                                        false/*is_specified_storing_col*/))) {
        LOG_WARN("add column failed", "rowkey_column", *rowkey_column,
                 "rowkey_order_type", arg.index_columns_.at(i).order_type_,
                 K(row_desc), K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      index_schema.set_rowkey_column_num(row_desc.get_column_num());
      index_schema.set_index_column_num(row_desc.get_column_num());
      // 2. add rowkey_vid_table vid column
      const ObColumnSchemaV2 *vid_column = nullptr;
      const ObString &vid_col_name = arg.store_columns_.at(0);
      // is_rowkey is false, order_in_rowkey will not be used
      const ObOrderType order_in_rowkey = ObOrderType::DESC;
      if (vid_col_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(vid_col_name));
      } else if (OB_ISNULL(vid_column = data_schema.get_column_schema(vid_col_name))) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("get_column_schema failed",
                 "tenant_id", data_schema.get_tenant_id(),
                 "database_id", data_schema.get_database_id(),
                 "table_name", data_schema.get_table_name(),
                 "column name", vid_col_name, K(ret));
      } else if (OB_FAIL(ObIndexBuilderUtil::add_column(vid_column,
                                                        false/*is_index_column*/,
                                                        false/*is_rowkey*/,
                                                        order_in_rowkey,
                                                        row_desc,
                                                        index_schema,
                                                        false/*is_hidden*/,
                                                        true/*is_specified_storing_col*/))) {
        LOG_WARN("add_column failed", "vid_column", *vid_column, K(row_desc), K(ret));
      } else if (OB_FAIL(index_schema.sort_column_array_by_column_id())) {
        LOG_WARN("failed to sort column", K(ret));
      } else {
        LOG_INFO("succeed to set rowkey vid table columns", K(index_schema));
      }
    }
  }
  LOG_DEBUG("finish set rowkey vid table column", K(ret), K(arg), K(index_schema), K(data_schema));
  return ret;
}

int ObVecIndexBuilderUtil::set_vec_vid_rowkey_table_columns(
    const ObCreateIndexArg &arg,
    const ObTableSchema &data_schema,
    ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  if (!data_schema.is_valid() ||
      arg.index_columns_.count() != 1 ||
      !share::schema::is_vec_vid_rowkey_type(arg.index_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),
      K(data_schema), K(arg.index_columns_.count()), K(arg.index_type_));
  }
  HEAP_VAR(ObRowDesc, row_desc) {
    // 1. add vid_rowkey_table vid id column
    const ObColumnSchemaV2 *vid_column = nullptr;
    const ObColumnSortItem &vid_col_item = arg.index_columns_.at(0);
    const ObString &vid_col_name = vid_col_item.column_name_;
    if (OB_FAIL(ret)) {
    } else if (vid_col_name.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column name is empty", K(ret), K(vid_col_name));
    } else if (OB_ISNULL(vid_column = data_schema.get_column_schema(vid_col_name))) {
      ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
      LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS,
                     vid_col_name.length(), vid_col_name.ptr());
      LOG_WARN("get_column_schema failed", "tenant_id", data_schema.get_tenant_id(),
               "database_id", data_schema.get_database_id(),
               "table_name", data_schema.get_table_name(),
               "column name", vid_col_name, K(ret));
    } else if (OB_FAIL(ObIndexBuilderUtil::add_column(vid_column,
                                                      true/*is_index_column*/,
                                                      true/*is_rowkey*/,
                                                      vid_col_item.order_type_,
                                                      row_desc,
                                                      index_schema,
                                                      false/*is_hidden*/,
                                                      false/*is_specified_storing_col*/))) {
      LOG_WARN("add column failed ", "vid_column", *vid_column,
          "rowkey_order_type", vid_col_item.order_type_, K(row_desc), K(ret));
    } else {
      index_schema.set_rowkey_column_num(row_desc.get_column_num());
      index_schema.set_index_column_num(row_desc.get_column_num());

      // 2. add vid_rowkey_table rowkey column
      const ObColumnSchemaV2 *rowkey_column = nullptr;
      const ObRowkeyInfo &rowkey_info = data_schema.get_rowkey_info();
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
        uint64_t column_id = OB_INVALID_ID;
        if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
          LOG_WARN("get_column_id failed", "index", i, K(ret));
        } else if (OB_ISNULL(rowkey_column = data_schema.get_column_schema(column_id))) {
          ret = OB_ERR_BAD_FIELD_ERROR;
          LOG_WARN("get_column_schema failed", "table_id", data_schema.get_table_id(),
              K(column_id), K(ret));
        } else if (OB_FAIL(ObIndexBuilderUtil::add_column(rowkey_column,
                                                          false/*is_index_column*/,
                                                          false/*is_rowkey*/,
                                                          rowkey_column->get_order_in_rowkey(),
                                                          row_desc,
                                                          index_schema,
                                                          false/*is_hidden*/,
                                                          false/*is_specified_storing_col*/))) {
          LOG_WARN("add column failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(index_schema.sort_column_array_by_column_id())) {
        LOG_WARN("failed to sort column", K(ret));
      } else {
        LOG_INFO("succeed to set vec vid rowkey table columns", K(index_schema));
      }
    }
  }
  LOG_DEBUG("finish set vec vid rowkey table columns", K(ret), K(arg), K(index_schema), K(data_schema));
  return ret;
}

/*
  bigint  char(1)   vector_type
  vid     type      vector
*/
int ObVecIndexBuilderUtil::set_vec_delta_buffer_table_columns(
    const ObCreateIndexArg &arg,
    const ObTableSchema &data_schema,
    ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  if (!data_schema.is_valid() ||
      (!share::schema::is_vec_delta_buffer_type(arg.index_type_)) ||
      arg.index_columns_.count() != 2 ||  /*vid, type column */
      arg.store_columns_.count() != 1) {  /* vector column */ /* 不算伪列 ora_rowscn */
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), K(arg.index_type_),
        K(arg.index_columns_.count()), K(arg.store_columns_.count()),
        K(arg.index_columns_), K(arg.store_columns_));
  }
  HEAP_VAR(ObRowDesc, row_desc) {
    // 1. add delta_buffer_table vid, type column
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_columns_.count(); ++i) {
      const ObColumnSchemaV2 *vec_column = nullptr;
      const ObColumnSortItem &vec_col_item = arg.index_columns_.at(i);
      const ObString &vec_col_name = vec_col_item.column_name_;
      if (vec_col_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(vec_col_name));
      } else if (OB_ISNULL(vec_column = data_schema.get_column_schema(vec_col_name))) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS,
                       vec_col_name.length(), vec_col_name.ptr());
        LOG_WARN("get_column_schema failed",
                 "tenant_id", data_schema.get_tenant_id(),
                 "database_id", data_schema.get_database_id(),
                 "table_name", data_schema.get_table_name(),
                 "column name", vec_col_name, K(ret));
      } else if (OB_FAIL(ObIndexBuilderUtil::add_column(vec_column,
                                                        true/*is_index_column*/,
                                                        true/*is_rowkey*/,
                                                        arg.index_columns_.at(i).order_type_,
                                                        row_desc,
                                                        index_schema,
                                                        false/*is_hidden*/,
                                                        false/*is_specified_storing_col*/))) {
        LOG_WARN("add column failed", "vec_column", *vec_column,
                 "rowkey_order_type", arg.index_columns_.at(i).order_type_,
                 K(row_desc), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      index_schema.set_rowkey_column_num(row_desc.get_column_num());
      index_schema.set_index_column_num(row_desc.get_column_num());
    }
    // 2. add delta_buffer_table vector column
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.store_columns_.count(); ++i) {
      const ObColumnSchemaV2 *store_column = nullptr;
      const ObString &store_column_name = arg.store_columns_.at(i);
      // is_rowkey is false, order_in_rowkey will not be used
      const ObOrderType order_in_rowkey = ObOrderType::DESC;
      if (OB_UNLIKELY(store_column_name.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(store_column_name));
      } else if (OB_ISNULL(store_column = data_schema.get_column_schema(store_column_name))) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("get_column_schema failed", "tenant_id", data_schema.get_tenant_id(),
                 "database_id", data_schema.get_database_id(),
                 "table_name", data_schema.get_table_name(),
                 "column name", store_column_name, K(ret));
      } else if (OB_FAIL(ObIndexBuilderUtil::add_column(store_column,
                                                        false/*is_index_column*/,
                                                        false/*is_rowkey*/,
                                                        order_in_rowkey,
                                                        row_desc,
                                                        index_schema,
                                                        false/*is_hidden*/,
                                                        true/*is_specified_storing_col*/))) {
        LOG_WARN("add_column failed", K(store_column), K(row_desc), K(ret));
      }
    }
    // 3. add part key column
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(set_part_key_columns(data_schema, index_schema))) {
      LOG_WARN("fail to generate part key columns", K(ret));
    }
    //
    if (FAILEDx(index_schema.sort_column_array_by_column_id())) {
      LOG_WARN("failed to sort column", K(ret));
    } else {
      LOG_INFO("succeed to set vec delta buffer table columns", K(index_schema));
    }
  }
  LOG_DEBUG("finish set vec delta buffer table column", K(ret), K(arg), K(index_schema), K(data_schema));
  return ret;
}


/*
  bigint  bigint   char(1)  vector_type
  scn     vid      type     vector
*/
int ObVecIndexBuilderUtil::set_vec_index_id_table_columns(
    const ObCreateIndexArg &arg,
    const ObTableSchema &data_schema,
    ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  if (!data_schema.is_valid() ||
      (!share::schema::is_vec_index_id_type(arg.index_type_)) ||
      arg.index_columns_.count() != 3 ||  /* scn, vid, type column */
      arg.store_columns_.count() != 1) {  /* vector column */
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), K(arg.index_type_),
        K(arg.index_columns_.count()), K(arg.store_columns_.count()),
        K(arg.index_columns_), K(arg.store_columns_));
  }
  HEAP_VAR(ObRowDesc, row_desc) {
    // 1. add index_id_table scn, vid column
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_columns_.count(); ++i) {
      const ObColumnSchemaV2 *vec_column = nullptr;
      const ObColumnSortItem &vec_col_item = arg.index_columns_.at(i);
      const ObString &vec_col_name = vec_col_item.column_name_;
      if (vec_col_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(vec_col_name));
      } else if (OB_ISNULL(vec_column = data_schema.get_column_schema(vec_col_name))) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS,
                       vec_col_name.length(), vec_col_name.ptr());
        LOG_WARN("get_column_schema failed",
                 "tenant_id", data_schema.get_tenant_id(),
                 "database_id", data_schema.get_database_id(),
                 "table_name", data_schema.get_table_name(),
                 "column name", vec_col_name, K(ret));
      } else if (OB_FAIL(ObIndexBuilderUtil::add_column(vec_column,
                                                        true/*is_index_column*/,
                                                        true/*is_rowkey*/,
                                                        arg.index_columns_.at(i).order_type_,
                                                        row_desc,
                                                        index_schema,
                                                        false/*is_hidden*/,
                                                        false/*is_specified_storing_col*/))) {
        LOG_WARN("add column failed", "vec_column", *vec_column,
                 "rowkey_order_type", arg.index_columns_.at(i).order_type_,
                 K(row_desc), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      index_schema.set_rowkey_column_num(row_desc.get_column_num());
      index_schema.set_index_column_num(row_desc.get_column_num());
    }
    // 2. add index_id_table vector column
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.store_columns_.count(); ++i) {
      const ObColumnSchemaV2 *store_column = nullptr;
      const ObString &store_column_name = arg.store_columns_.at(i);
      // is_rowkey is false, order_in_rowkey will not be used
      const ObOrderType order_in_rowkey = ObOrderType::DESC;
      if (OB_UNLIKELY(store_column_name.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(store_column_name));
      } else if (OB_ISNULL(store_column = data_schema.get_column_schema(store_column_name))) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("get_column_schema failed", "tenant_id", data_schema.get_tenant_id(),
                 "database_id", data_schema.get_database_id(),
                 "table_name", data_schema.get_table_name(),
                 "column name", store_column_name, K(ret));
      } else if (OB_FAIL(ObIndexBuilderUtil::add_column(store_column,
                                                        false/*is_index_column*/,
                                                        false/*is_rowkey*/,
                                                        order_in_rowkey,
                                                        row_desc,
                                                        index_schema,
                                                        false/*is_hidden*/,
                                                        true/*is_specified_storing_col*/))) {
        LOG_WARN("add_column failed", K(store_column), K(row_desc), K(ret));
      }
    }
    // 3. add part key column
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(set_part_key_columns(data_schema, index_schema))) {
      LOG_WARN("fail to generate part key columns", K(ret));
    }
    //
    if (FAILEDx(index_schema.sort_column_array_by_column_id())) {
      LOG_WARN("failed to sort column", K(ret));
    } else {
      LOG_INFO("succeed to set vec index id table columns", K(index_schema));
    }
  }
  LOG_DEBUG("finish set vec index id table column", K(ret), K(arg), K(index_schema), K(data_schema));
  return ret;
}


/*
  varchar  blob
  key      data
*/
int ObVecIndexBuilderUtil::set_vec_index_snapshot_data_table_columns(
    const ObCreateIndexArg &arg,
    const ObTableSchema &data_schema,
    ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  if (!data_schema.is_valid() ||
      (!share::schema::is_vec_index_snapshot_data_type(arg.index_type_)) ||
      arg.index_columns_.count() != 1 ||  /* key column */
      arg.store_columns_.count() != 3) {  /* data , vid, vector column */
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), K(arg.index_type_),
        K(arg.index_columns_.count()), K(arg.store_columns_.count()),
        K(arg.index_columns_), K(arg.store_columns_));
  }
  HEAP_VAR(ObRowDesc, row_desc) {
    // 1. add index_snapshot_data_table key column
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_columns_.count(); ++i) {
      const ObColumnSchemaV2 *vec_column = nullptr;
      const ObColumnSortItem &vec_col_item = arg.index_columns_.at(i);
      const ObString &vec_col_name = vec_col_item.column_name_;
      if (vec_col_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(vec_col_name));
      } else if (OB_ISNULL(vec_column = data_schema.get_column_schema(vec_col_name))) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS,
                       vec_col_name.length(), vec_col_name.ptr());
        LOG_WARN("get_column_schema failed",
                 "tenant_id", data_schema.get_tenant_id(),
                 "database_id", data_schema.get_database_id(),
                 "table_name", data_schema.get_table_name(),
                 "column name", vec_col_name, K(ret));
      } else if (OB_FAIL(ObIndexBuilderUtil::add_column(vec_column,
                                                        true/*is_index_column*/,
                                                        true/*is_rowkey*/,
                                                        arg.index_columns_.at(i).order_type_,
                                                        row_desc,
                                                        index_schema,
                                                        false/*is_hidden*/,
                                                        false/*is_specified_storing_col*/))) {
        LOG_WARN("add column failed", "vec_column", *vec_column,
                 "rowkey_order_type", arg.index_columns_.at(i).order_type_,
                 K(row_desc), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      index_schema.set_rowkey_column_num(row_desc.get_column_num());
      index_schema.set_index_column_num(row_desc.get_column_num());
    }
    // 2. add index_snapshot_data_table data column
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.store_columns_.count(); ++i) {
      const ObColumnSchemaV2 *store_column = nullptr;
      const ObString &store_column_name = arg.store_columns_.at(i);
      // is_rowkey is false, order_in_rowkey will not be used
      const ObOrderType order_in_rowkey = ObOrderType::DESC;
      if (OB_UNLIKELY(store_column_name.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(store_column_name));
      } else if (OB_ISNULL(store_column = data_schema.get_column_schema(store_column_name))) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("get_column_schema failed", "tenant_id", data_schema.get_tenant_id(),
                 "database_id", data_schema.get_database_id(),
                 "table_name", data_schema.get_table_name(),
                 "column name", store_column_name, K(ret));
      } else if (OB_FAIL(ObIndexBuilderUtil::add_column(store_column,
                                                        false/*is_index_column*/,
                                                        false/*is_rowkey*/,
                                                        order_in_rowkey,
                                                        row_desc,
                                                        index_schema,
                                                        false/*is_hidden*/,
                                                        true/*is_specified_storing_col*/))) {
        LOG_WARN("add_column failed", K(store_column), K(row_desc), K(ret));
      }
    }
    if (FAILEDx(index_schema.sort_column_array_by_column_id())) {
      LOG_WARN("failed to sort column", K(ret));
    } else {
      LOG_INFO("succeed to set vec index table columns", K(index_schema));
    }
  }
  LOG_DEBUG("finish set vec index snapshot data table column", K(ret), K(arg), K(index_schema), K(data_schema));
  return ret;
}

int ObVecIndexBuilderUtil::adjust_vec_args(
    obrpc::ObCreateIndexArg &index_arg,
    ObTableSchema &data_schema, // not const since will add column to data schema
    ObIAllocator &allocator,
    ObIArray<ObColumnSchemaV2 *> &gen_columns)
{
  int ret = OB_SUCCESS;
  const ObIndexType &index_type = index_arg.index_type_;
  if (share::schema::is_vec_hnsw_index(index_type)) {
    if (OB_FAIL(adjust_vec_hnsw_args(index_arg, data_schema, allocator, gen_columns))) {
      LOG_WARN("fail to adjust vec hnsw args", K(ret));
    }
  } else if (share::schema::is_vec_ivfflat_index(index_type)) {
    if (OB_FAIL(adjust_vec_ivfflat_args(index_arg, data_schema, allocator, gen_columns))) {
      LOG_WARN("fail to adjust vec hnsw args", K(ret));
    }
  } else if (share::schema::is_vec_ivfsq8_index(index_type)) {
    if (OB_FAIL(adjust_vec_ivfsq8_args(index_arg, data_schema, allocator, gen_columns))) {
      LOG_WARN("fail to adjust vec hnsw args", K(ret));
    }
  } else if (share::schema::is_vec_ivfpq_index(index_type)) {
    if (OB_FAIL(adjust_vec_ivfpq_args(index_arg, data_schema, allocator, gen_columns))) {
      LOG_WARN("fail to adjust vec hnsw args", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to adjust vec args", K(ret), K(index_type));
  }
  LOG_DEBUG("finish adjust_vec_args", K(ret), K(index_type), K(index_arg), K(gen_columns));
  return ret;
}

/*
 * 1. 生成辅助表的列
   2. 把辅助表的对应的列放入index_arg （主键放入index_column，非主键放入store_column）
   3. 根据index_arg的index_id，生成对应表的虚拟生成列gen_columns
*/
int ObVecIndexBuilderUtil::adjust_vec_hnsw_args(
    obrpc::ObCreateIndexArg &index_arg,
    ObTableSchema &data_schema, // not const since will add column to data schema
    ObIAllocator &allocator,
    ObIArray<ObColumnSchemaV2 *> &gen_columns)
{
  int ret = OB_SUCCESS;
  const ObIndexType &index_type = index_arg.index_type_;

  uint64_t vid_col_id = OB_INVALID_ID;
  uint64_t type_col_id = OB_INVALID_ID;
  uint64_t vector_col_id = OB_INVALID_ID;
  uint64_t scn_col_id = OB_INVALID_ID;
  uint64_t key_col_id = OB_INVALID_ID;
  uint64_t data_col_id = OB_INVALID_ID;

  const ObColumnSchemaV2 *existing_vid_col = nullptr;
  const ObColumnSchemaV2 *existing_type_col = nullptr;
  const ObColumnSchemaV2 *existing_vector_col = nullptr;
  const ObColumnSchemaV2 *existing_scn_col = nullptr;
  const ObColumnSchemaV2 *existing_key_col = nullptr;
  const ObColumnSchemaV2 *existing_data_col = nullptr;

  ObArray<const ObColumnSchemaV2 *> tmp_cols;
  uint64_t available_col_id = 0;
  bool is_rowkey_vid = false;
  bool is_vid_rowkey = false;
  bool is_delta_buffer = false;
  bool is_index_id = false;
  bool is_index_snapshot_data = false;

  if (!data_schema.is_valid() || !share::schema::is_vec_hnsw_index(index_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), K(index_type));
  } else if (FALSE_IT(available_col_id = data_schema.get_max_used_column_id() + 1)) {
  } else if (FALSE_IT(is_rowkey_vid = share::schema::is_vec_rowkey_vid_type(index_type))) {
  } else if (FALSE_IT(is_vid_rowkey = share::schema::is_vec_vid_rowkey_type(index_type))) {
  } else if (FALSE_IT(is_delta_buffer = share::schema::is_vec_delta_buffer_type(index_type))) {
  } else if (FALSE_IT(is_index_id = share::schema::is_vec_index_id_type(index_type))) {
  } else if (FALSE_IT(is_index_snapshot_data = share::schema::is_vec_index_snapshot_data_type(index_type))) {
  } else if (OB_FAIL(check_vec_cols(&index_arg, data_schema))) {
    LOG_WARN("check cols check failed", K(ret));
  } else if (OB_FAIL(get_vec_vid_col(data_schema, existing_vid_col))) {
    LOG_WARN("failed to get vid id col", K(ret));
  } else if (OB_FAIL(get_vec_type_col(data_schema, &index_arg, existing_type_col))) {
    LOG_WARN("failed to get vec type col", K(ret));
  } else if (OB_FAIL(get_vec_vector_col(data_schema, &index_arg, existing_vector_col))) {
    LOG_WARN("fail to get vec vector column", K(ret));
  } else if (OB_FAIL(get_vec_scn_col(data_schema, &index_arg, existing_scn_col))) {
    LOG_WARN("failed to get vec scn col", K(ret));
  } else if (OB_FAIL(get_vec_key_col(data_schema, &index_arg, existing_key_col))) {
    LOG_WARN("failed to get vec key col", K(ret));
  } else if (OB_FAIL(get_vec_data_col(data_schema, &index_arg, existing_data_col))) {
    LOG_WARN("failed to get vec data col", K(ret));
  } else {
    ObColumnSchemaV2 *generated_vid_col = nullptr;
    ObColumnSchemaV2 *generated_type_col = nullptr;
    ObColumnSchemaV2  *generated_vector_col = nullptr;
    ObColumnSchemaV2 *generated_scn_col = nullptr;
    ObColumnSchemaV2 *generated_key_col = nullptr;
    ObColumnSchemaV2 *generated_data_col = nullptr;
    if (OB_ISNULL(existing_vid_col)) { // need to generate vid column
      vid_col_id = available_col_id++;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(generate_vid_column(&index_arg, vid_col_id, data_schema, generated_vid_col))) {
        LOG_WARN("failed to generate vid column", K(ret));
      } else if (OB_FAIL(gen_columns.push_back(generated_vid_col))) {
        LOG_WARN("failed to push back vid column", K(ret));
      }
    }
    if (is_rowkey_vid || is_vid_rowkey) {
    } else if (is_delta_buffer) { // generate type, vector
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(existing_type_col)) {
        type_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_type_column(&index_arg, type_col_id, data_schema, generated_type_col))) {
          LOG_WARN("failed to generate type column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_type_col))) {
          LOG_WARN("failed to push type column", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(existing_vector_col)) {
        vector_col_id = available_col_id++;
        if (OB_FAIL(generate_vector_column(&index_arg, vector_col_id, data_schema, generated_vector_col))) {
          LOG_WARN("failed to generate vector column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_vector_col))) {
          LOG_WARN("failed to push back vector column", K(ret));
        }
      }
    } else if (is_index_id) {   // generate type, vector, scn
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(existing_type_col)) {
        type_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_type_column(&index_arg, type_col_id, data_schema, generated_type_col))) {
          LOG_WARN("failed to generate type column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_type_col))) {
          LOG_WARN("failed to push type column", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(existing_vector_col)) {
        vector_col_id = available_col_id++;
        if (OB_FAIL(generate_vector_column(&index_arg, vector_col_id, data_schema, generated_vector_col))) {
          LOG_WARN("failed to generate vector column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_vector_col))) {
          LOG_WARN("failed to push back vector column", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(existing_scn_col)) {
        scn_col_id = available_col_id++;
        if (OB_FAIL(generate_scn_column(&index_arg, scn_col_id, data_schema, generated_scn_col))) {
          LOG_WARN("fail to generate scn column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_scn_col))) {
          LOG_WARN("fail to push back generated scn column", K(ret));
        }
      }
    } else if (is_index_snapshot_data) {  // generate key, data, vector
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(existing_vector_col)) {
        vector_col_id = available_col_id++;
        if (OB_FAIL(generate_vector_column(&index_arg, vector_col_id, data_schema, generated_vector_col))) {
          LOG_WARN("failed to generate vector column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_vector_col))) {
          LOG_WARN("failed to push back vector column", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(existing_key_col)) {
        key_col_id = available_col_id++;
        if (OB_FAIL(generate_key_column(&index_arg, key_col_id, data_schema, generated_key_col))) {
          LOG_WARN("fail to generate key column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_key_col))) {
          LOG_WARN("fail to push back generated key column", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(existing_data_col)) {
        data_col_id = available_col_id++;
        if (OB_FAIL(generate_data_column(&index_arg, data_col_id, data_schema, generated_data_col))) {
          LOG_WARN("fail to generate data column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_data_col))) {
          LOG_WARN("fail to push back generated data column", K(ret));
        }
      }
    }
    // generate index_arg
    if (OB_FAIL(ret)) {
    } else if (is_rowkey_vid || is_vid_rowkey) {
      if (OB_FAIL(push_back_gen_col(tmp_cols, existing_vid_col, generated_vid_col))) {
        LOG_WARN("failed to push back vid column", K(ret));
      } else if (OB_FAIL(adjust_vec_arg(&index_arg, data_schema, allocator, tmp_cols))) {
        LOG_WARN("failed to append vec index arg", K(ret));
      }
    } else if (is_delta_buffer) {
      if (OB_FAIL(push_back_gen_col(tmp_cols, existing_vid_col, generated_vid_col))) {
        LOG_WARN("failed to push back vid col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_type_col, generated_type_col))) {
        LOG_WARN("failed to push back type col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_vector_col, generated_vector_col))) {
        LOG_WARN("failed to push back vector col", K(ret));
      } else if (OB_FAIL(adjust_vec_arg(&index_arg, data_schema, allocator, tmp_cols))) {
        LOG_WARN("failed to append vec index arg", K(ret));
      }
    } else if (is_index_id) {
      if (OB_FAIL(push_back_gen_col(tmp_cols, existing_scn_col, generated_scn_col))) {
        LOG_WARN("failed to push back scn col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_vid_col, generated_vid_col))) {
        LOG_WARN("failed to push back vid col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_type_col, generated_type_col))) {
        LOG_WARN("failed to push back type col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_vector_col, generated_vector_col))) {
        LOG_WARN("fail to push back vector col", K(ret));
      } else if (OB_FAIL(adjust_vec_arg(&index_arg, data_schema, allocator, tmp_cols))) {
        LOG_WARN("failed to append vec index arg", K(ret));
      }
    } else if (is_index_snapshot_data) {
      if (OB_FAIL(push_back_gen_col(tmp_cols, existing_key_col, generated_key_col))) {
        LOG_WARN("failed to push back key col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_data_col, generated_data_col))) {
        LOG_WARN("failed to push back data col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_vid_col, generated_vid_col))) {
        LOG_WARN("failed to push back vid col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_vector_col, generated_vector_col))) {
        LOG_WARN("failed to push back vector col", K(ret));
      } else if (OB_FAIL(adjust_vec_arg(&index_arg, data_schema, allocator, tmp_cols))) {
        LOG_WARN("failed to append vec index arg", K(ret));
      }
    }
  }
  return ret;
}

int ObVecIndexBuilderUtil::adjust_vec_ivfflat_args(
    obrpc::ObCreateIndexArg &index_arg,
    ObTableSchema &data_schema, // not const since will add column to data schema
    ObIAllocator &allocator,
    ObIArray<ObColumnSchemaV2 *> &gen_columns)
{
  int ret = OB_SUCCESS;
  const ObIndexType &index_type = index_arg.index_type_;

  uint64_t center_id_col_id = OB_INVALID_ID;
  uint64_t center_vector_col_id = OB_INVALID_ID;
  uint64_t data_vector_col_id = OB_INVALID_ID;
  const ObColumnSchemaV2 *existing_center_id_col = nullptr;
  const ObColumnSchemaV2 *existing_center_vector_col = nullptr;
  const ObColumnSchemaV2 *existing_data_vector_col = nullptr;

  ObArray<const ObColumnSchemaV2 *> tmp_cols;
  uint64_t available_col_id = 0;
  bool is_centroid_table = false;
  bool is_cid_vector_table = false;
  bool is_rowkey_cid_table = false;

  if (!data_schema.is_valid() || !share::schema::is_vec_ivfflat_index(index_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), K(index_type));
  } else if (FALSE_IT(available_col_id = data_schema.get_max_used_column_id() + 1)) {
  } else if (FALSE_IT(is_centroid_table = share::schema::is_vec_ivfflat_centroid_index(index_type))) {
  } else if (FALSE_IT(is_cid_vector_table = share::schema::is_vec_ivfflat_cid_vector_index(index_type))) {
  } else if (FALSE_IT(is_rowkey_cid_table = share::schema::is_vec_ivfflat_rowkey_cid_index(index_type))) {
  } else if (OB_FAIL(check_vec_cols(&index_arg, data_schema))) {
    LOG_WARN("check cols check failed", K(ret));
  } else if (OB_FAIL(get_vec_ivfflat_col(data_schema, &index_arg,
      existing_center_id_col, existing_center_vector_col, existing_data_vector_col))) {
    LOG_WARN("failed to get ivfflat column", K(ret));
  } else {
    ObColumnSchemaV2 *generated_center_id_col = nullptr;
    ObColumnSchemaV2 *generated_center_vector_col = nullptr;
    ObColumnSchemaV2 *generated_data_vector_col = nullptr;
    // 1. generate index table (generate) columns
    if (is_centroid_table) {    // generate centroid_id, center_vector column
      if (OB_ISNULL(existing_center_id_col)) {
        center_id_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_vec_ivf_column(&index_arg, center_id_col_id, IVF_CENTER_ID_COL, data_schema, generated_center_id_col))) {
          LOG_WARN("failed to generate vid column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_center_id_col))) {
          LOG_WARN("failed to push back vid column", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(existing_center_vector_col)) {
        center_vector_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_vec_ivf_column(&index_arg, center_vector_col_id, IVF_CENTER_VECTOR_COL, data_schema, generated_center_vector_col))) {
          LOG_WARN("failed to generate type column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_center_vector_col))) {
          LOG_WARN("failed to push type column", K(ret));
        }
      }
    } else if (is_cid_vector_table) { // generate centroid_id, data_vector column
      if (OB_ISNULL(existing_center_id_col)) {
        center_id_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_vec_ivf_column(&index_arg, center_id_col_id, IVF_CENTER_ID_COL, data_schema, generated_center_id_col))) {
          LOG_WARN("failed to generate vid column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_center_id_col))) {
          LOG_WARN("failed to push back vid column", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(existing_data_vector_col)) {
        data_vector_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_vec_ivf_column(&index_arg, data_vector_col_id, IVF_FLAT_DATA_VECTOR_COL, data_schema, generated_data_vector_col))) {
          LOG_WARN("failed to generate type column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_data_vector_col))) {
          LOG_WARN("failed to push type column", K(ret));
        }
      }
    } else if (is_rowkey_cid_table) {   // generate centroid_id column
      if (OB_ISNULL(existing_center_id_col)) {
        center_id_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_vec_ivf_column(&index_arg, center_id_col_id, IVF_CENTER_ID_COL, data_schema, generated_center_id_col))) {
          LOG_WARN("failed to generate vid column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_center_id_col))) {
          LOG_WARN("failed to push back vid column", K(ret));
        }
      }
    }
    // 2. push back columns to every single index table
    if (OB_FAIL(ret)) {
    } else if (is_centroid_table) {
      int64_t rowkey_size = 0;
      if (OB_FAIL(push_back_gen_col(tmp_cols, existing_center_id_col, generated_center_id_col))) {
        LOG_WARN("failed to push back vid col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_center_vector_col, generated_center_vector_col))) {
        LOG_WARN("failed to push back type col", K(ret));
      } else if (OB_FAIL(adjust_vec_ivf_arg(&index_arg, data_schema, rowkey_size, allocator, tmp_cols))) {
        LOG_WARN("failed to append vec index arg", K(ret));
      }
    } else if (is_cid_vector_table) {
      int64_t rowkey_size = 0;
      if (OB_FAIL(push_back_gen_col(tmp_cols, existing_center_id_col, generated_center_id_col))) {
        LOG_WARN("failed to push back center id col", K(ret));
      } else if (OB_FAIL(push_back_rowkey_col(tmp_cols, data_schema, rowkey_size))) {
        LOG_WARN("failed to push back rowkey col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_data_vector_col, generated_data_vector_col))) {
        LOG_WARN("failed to push back data vector col", K(ret));
      } else if (OB_FAIL(adjust_vec_ivf_arg(&index_arg, data_schema, rowkey_size, allocator, tmp_cols))) {
        LOG_WARN("failed to adjust vec ivfflat arg", K(ret));
      }
    } else if (is_rowkey_cid_table) {
      int64_t rowkey_size = 0;
      if (OB_FAIL(push_back_rowkey_col(tmp_cols, data_schema, rowkey_size))) {
        LOG_WARN("failed to push back rowkey col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_center_id_col, generated_center_id_col))) {
        LOG_WARN("failed to push back center id col", K(ret));
      } else if (OB_FAIL(adjust_vec_ivf_arg(&index_arg, data_schema, rowkey_size, allocator, tmp_cols))) {
        LOG_WARN("failed to adjust vec ivfflat arg", K(ret));
      }
    }
  }
  LOG_DEBUG("finish adjust_vec_ivfflat_args", K(ret), K(index_arg.index_type_), K(data_schema));
  return ret;
}

int ObVecIndexBuilderUtil::adjust_vec_ivfsq8_args(
    obrpc::ObCreateIndexArg &index_arg,
    ObTableSchema &data_schema, // not const since will add column to data schema
    ObIAllocator &allocator,
    ObIArray<ObColumnSchemaV2 *> &gen_columns)
{
  int ret = OB_SUCCESS;
  const ObIndexType &index_type = index_arg.index_type_;

  uint64_t center_id_col_id = OB_INVALID_ID;
  uint64_t center_vector_col_id = OB_INVALID_ID;
  uint64_t data_vector_col_id = OB_INVALID_ID;
  uint64_t meta_id_col_id = OB_INVALID_ID;
  uint64_t meta_vector_col_id = OB_INVALID_ID;
  const ObColumnSchemaV2 *existing_meta_id_col = nullptr;
  const ObColumnSchemaV2 *existing_meta_vector_col = nullptr;
  const ObColumnSchemaV2 *existing_center_id_col = nullptr;
  const ObColumnSchemaV2 *existing_center_vector_col = nullptr;
  const ObColumnSchemaV2 *existing_data_vector_col = nullptr;

  ObArray<const ObColumnSchemaV2 *> tmp_cols;
  uint64_t available_col_id = 0;
  bool is_centroid_table = false;
  bool is_cid_vector_table = false;
  bool is_rowkey_cid_table = false;
  bool is_sq_meta_table = false;

  if (!data_schema.is_valid() || !share::schema::is_vec_ivfsq8_index(index_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), K(index_type));
  } else if (FALSE_IT(available_col_id = data_schema.get_max_used_column_id() + 1)) {
  } else if (FALSE_IT(is_centroid_table = share::schema::is_vec_ivfsq8_centroid_index(index_type))) {
  } else if (FALSE_IT(is_cid_vector_table = share::schema::is_vec_ivfsq8_cid_vector_index(index_type))) {
  } else if (FALSE_IT(is_rowkey_cid_table = share::schema::is_vec_ivfsq8_rowkey_cid_index(index_type))) {
  } else if (FALSE_IT(is_sq_meta_table = share::schema::is_vec_ivfsq8_meta_index(index_type))) {
  } else if (OB_FAIL(check_vec_cols(&index_arg, data_schema))) {
    LOG_WARN("check cols check failed", K(ret));
  } else if (OB_FAIL(get_vec_ivfsq8_col(data_schema, &index_arg, existing_meta_id_col, existing_meta_vector_col,
      existing_center_id_col, existing_center_vector_col, existing_data_vector_col))) {
    LOG_WARN("failed to get ivfsq8 column", K(ret));
  } else {
    ObColumnSchemaV2 *generated_center_id_col = nullptr;
    ObColumnSchemaV2 *generated_center_vector_col = nullptr;
    ObColumnSchemaV2 *generated_data_vector_col = nullptr;
    ObColumnSchemaV2 *generated_meta_id_col = nullptr;
    ObColumnSchemaV2 *generated_meta_vector_col = nullptr;
    // 1. generate index table (generate) columns
    if (is_centroid_table) {    // generate center_id, center_vector column
      if (OB_ISNULL(existing_center_id_col)) {
        center_id_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_vec_ivf_column(&index_arg, center_id_col_id, IVF_CENTER_ID_COL, data_schema, generated_center_id_col))) {
          LOG_WARN("failed to generate vid column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_center_id_col))) {
          LOG_WARN("failed to push back vid column", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(existing_center_vector_col)) {
        center_vector_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_vec_ivf_column(&index_arg, center_vector_col_id, IVF_CENTER_VECTOR_COL, data_schema, generated_center_vector_col))) {
          LOG_WARN("failed to generate type column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_center_vector_col))) {
          LOG_WARN("failed to push type column", K(ret));
        }
      }
    } else if (is_cid_vector_table) {   // generate center_id, data_vector column
      if (OB_ISNULL(existing_center_id_col)) {
        center_id_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_vec_ivf_column(&index_arg, center_id_col_id, IVF_CENTER_ID_COL, data_schema, generated_center_id_col))) {
          LOG_WARN("failed to generate vid column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_center_id_col))) {
          LOG_WARN("failed to push back vid column", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(existing_data_vector_col)) {
        data_vector_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_vec_ivf_column(&index_arg, data_vector_col_id, IVF_SQ8_DATA_VECTOR_COL, data_schema, generated_data_vector_col))) {
          LOG_WARN("failed to generate type column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_data_vector_col))) {
          LOG_WARN("failed to push type column", K(ret));
        }
      }
    } else if (is_rowkey_cid_table) {   // generate center_id column
      if (OB_ISNULL(existing_center_id_col)) {
        center_id_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_vec_ivf_column(&index_arg, center_id_col_id, IVF_CENTER_ID_COL, data_schema, generated_center_id_col))) {
          LOG_WARN("failed to generate vid column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_center_id_col))) {
          LOG_WARN("failed to push back vid column", K(ret));
        }
      }
    } else if (is_sq_meta_table) {  // generate meta_id, meta_vector column
      if (OB_ISNULL(existing_meta_id_col)) {
        meta_id_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_vec_ivf_column(&index_arg, meta_id_col_id, IVF_META_ID_COL, data_schema, generated_meta_id_col))) {
          LOG_WARN("failed to generate type column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_meta_id_col))) {
          LOG_WARN("failed to push type column", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(existing_meta_vector_col)) {
        meta_vector_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_vec_ivf_column(&index_arg, meta_vector_col_id, IVF_META_VECTOR_COL, data_schema, generated_meta_vector_col))) {
          LOG_WARN("failed to generate type column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_meta_vector_col))) {
          LOG_WARN("failed to push type column", K(ret));
        }
      }
    }
    // 2. push back columns to every single index table
    if (OB_FAIL(ret)) {
    } else if (is_centroid_table) {
      int64_t rowkey_size = 0;
      if (OB_FAIL(push_back_gen_col(tmp_cols, existing_center_id_col, generated_center_id_col))) {
        LOG_WARN("failed to push back vid col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_center_vector_col, generated_center_vector_col))) {
        LOG_WARN("failed to push back type col", K(ret));
      } else if (OB_FAIL(adjust_vec_ivf_arg(&index_arg, data_schema, rowkey_size, allocator, tmp_cols))) {
        LOG_WARN("failed to append vec index arg", K(ret));
      }
    } else if (is_cid_vector_table) {
      int64_t rowkey_size = 0;
      if (OB_FAIL(push_back_gen_col(tmp_cols, existing_center_id_col, generated_center_id_col))) {
        LOG_WARN("failed to push back scn col", K(ret));
      } else if (OB_FAIL(push_back_rowkey_col(tmp_cols, data_schema, rowkey_size))) {
        LOG_WARN("fail to push back rowkey col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_data_vector_col, generated_data_vector_col))) {
        LOG_WARN("failed to push back vid col", K(ret));
      } else if (OB_FAIL(adjust_vec_ivf_arg(&index_arg, data_schema, rowkey_size, allocator, tmp_cols))) {
        LOG_WARN("failed to append vec index arg", K(ret));
      }
    } else if (is_rowkey_cid_table) {
      int64_t rowkey_size = 0;
      if (OB_FAIL(push_back_rowkey_col(tmp_cols, data_schema, rowkey_size))) {
        LOG_WARN("fail to push back rowkey col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_center_id_col, generated_center_id_col))) {
        LOG_WARN("failed to push back scn col", K(ret));
      } else if (OB_FAIL(adjust_vec_ivf_arg(&index_arg, data_schema, rowkey_size, allocator, tmp_cols))) {
        LOG_WARN("failed to append vec index arg", K(ret));
      }
    } else if (is_sq_meta_table) {
      int64_t rowkey_size = 0;
      if (OB_FAIL(push_back_gen_col(tmp_cols, existing_meta_id_col, generated_meta_id_col))) {
        LOG_WARN("failed to push back scn col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_meta_vector_col, generated_meta_vector_col))) {
        LOG_WARN("failed to push back scn col", K(ret));
      } else if (OB_FAIL(adjust_vec_ivf_arg(&index_arg, data_schema, rowkey_size, allocator, tmp_cols))) {
        LOG_WARN("failed to append vec index arg", K(ret));
      }
    }
  }
  LOG_DEBUG("finish adjust_vec_ivfsq8_args", K(ret), K(index_arg.index_type_), K(data_schema));
  return ret;
}

int ObVecIndexBuilderUtil::adjust_vec_ivfpq_args(
    obrpc::ObCreateIndexArg &index_arg,
    ObTableSchema &data_schema, // not const since will add column to data schema
    ObIAllocator &allocator,
    ObIArray<ObColumnSchemaV2 *> &gen_columns)
{
  int ret = OB_SUCCESS;
  const ObIndexType &index_type = index_arg.index_type_;

  uint64_t center_id_col_id = OB_INVALID_ID;
  uint64_t center_vector_col_id = OB_INVALID_ID;
  uint64_t pq_center_id_col_id = OB_INVALID_ID;
  uint64_t pq_center_ids_col_id = OB_INVALID_ID;
  const ObColumnSchemaV2 *existing_center_id_col = nullptr;
  const ObColumnSchemaV2 *existing_center_vector_col = nullptr;
  const ObColumnSchemaV2 *existing_pq_center_id_col = nullptr;
  const ObColumnSchemaV2 *existing_pq_center_ids_col = nullptr;

  ObArray<const ObColumnSchemaV2 *> tmp_cols;
  uint64_t available_col_id = 0;
  bool is_centroid_table = false;
  bool is_pq_centroid_table = false;
  bool is_pq_code_table = false;
  bool is_pq_rowkey_cid_table = false;

  if (!data_schema.is_valid() || !share::schema::is_vec_ivfpq_index(index_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), K(index_type));
  } else if (FALSE_IT(available_col_id = data_schema.get_max_used_column_id() + 1)) {
  } else if (FALSE_IT(is_centroid_table = share::schema::is_vec_ivfpq_centroid_index(index_type))) {
  } else if (FALSE_IT(is_pq_centroid_table = share::schema::is_vec_ivfpq_pq_centroid_index(index_type))) {
  } else if (FALSE_IT(is_pq_code_table = share::schema::is_vec_ivfpq_code_index(index_type))) {
  } else if (FALSE_IT(is_pq_rowkey_cid_table = share::schema::is_vec_ivfpq_rowkey_cid_index(index_type))) {
  } else if (OB_FAIL(check_vec_cols(&index_arg, data_schema))) {
    LOG_WARN("check cols check failed", K(ret));
  } else if (OB_FAIL(get_vec_ivfpq_col(data_schema, &index_arg,
      existing_center_id_col, existing_center_vector_col, existing_pq_center_id_col, existing_pq_center_ids_col))) {
    LOG_WARN("failed to get ivfpq column", K(ret));
  } else {
    ObColumnSchemaV2 *generated_center_id_col = nullptr;
    ObColumnSchemaV2 *generated_center_vector_col = nullptr;
    ObColumnSchemaV2 *generated_pq_center_id_col = nullptr;
    ObColumnSchemaV2 *generated_pq_center_ids_col = nullptr;
    // 1. generate index table (generate) columns
    if (is_centroid_table) {    // generate center_id, center_vector column
      if (OB_ISNULL(existing_center_id_col)) {
        center_id_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_vec_ivf_column(&index_arg, center_id_col_id, IVF_CENTER_ID_COL, data_schema, generated_center_id_col))) {
          LOG_WARN("failed to generate ivf column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_center_id_col))) {
          LOG_WARN("failed to push back ivf column", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(existing_center_vector_col)) {
        center_vector_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_vec_ivf_column(&index_arg, center_vector_col_id, IVF_CENTER_VECTOR_COL, data_schema, generated_center_vector_col))) {
          LOG_WARN("failed to generate ivf column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_center_vector_col))) {
          LOG_WARN("failed to push ivf column", K(ret));
        }
      }
    } else if (is_pq_centroid_table) {  // generate center_vector, pq_center_id column
      if (OB_ISNULL(existing_center_vector_col)) {
        center_vector_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_vec_ivf_column(&index_arg, center_vector_col_id, IVF_CENTER_VECTOR_COL, data_schema, generated_center_vector_col))) {
          LOG_WARN("failed to generate ivf column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_center_vector_col))) {
          LOG_WARN("failed to push ivf column", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(existing_pq_center_id_col)) {
        pq_center_id_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_vec_ivf_column(&index_arg, pq_center_id_col_id, IVF_PQ_CENTER_ID_COL, data_schema, generated_pq_center_id_col))) {
          LOG_WARN("failed to generate ivf column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_pq_center_id_col))) {
          LOG_WARN("failed to push ivf column", K(ret));
        }
      }
    } else if (is_pq_code_table || is_pq_rowkey_cid_table) {  // generate center_id, pq_center_ids column
      if (OB_ISNULL(existing_center_id_col)) {
        center_id_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_vec_ivf_column(&index_arg, center_id_col_id, IVF_CENTER_ID_COL, data_schema, generated_center_id_col))) {
          LOG_WARN("failed to generate ivf column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_center_id_col))) {
          LOG_WARN("failed to push back ivf column", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(existing_pq_center_ids_col)) {
        pq_center_ids_col_id = available_col_id++;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(generate_vec_ivf_column(&index_arg, pq_center_ids_col_id, IVF_PQ_CENTER_IDS_COL, data_schema, generated_pq_center_ids_col))) {
          LOG_WARN("failed to generate ivf column", K(ret));
        } else if (OB_FAIL(gen_columns.push_back(generated_pq_center_ids_col))) {
          LOG_WARN("failed to push ivf column", K(ret));
        }
      }
    }
    // 2. push back columns to every single index table
    if (OB_FAIL(ret)) {
    } else if (is_centroid_table) {
      int64_t rowkey_size = 0;
      if (OB_FAIL(push_back_gen_col(tmp_cols, existing_center_id_col, generated_center_id_col))) {
        LOG_WARN("failed to push back center id col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_center_vector_col, generated_center_vector_col))) {
        LOG_WARN("failed to push back center vector col", K(ret));
      } else if (OB_FAIL(adjust_vec_ivf_arg(&index_arg, data_schema, rowkey_size, allocator, tmp_cols))) {
        LOG_WARN("failed to adjust vec ivfpq arg", K(ret));
      }
    } else if (is_pq_centroid_table) {
      int64_t rowkey_size = 0;
      if (OB_FAIL(push_back_gen_col(tmp_cols, existing_pq_center_id_col, generated_pq_center_id_col))) {
        LOG_WARN("failed to push back pq center id col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_center_vector_col, generated_center_vector_col))) {
        LOG_WARN("failed to push back center vector col", K(ret));
      } else if (OB_FAIL(adjust_vec_ivf_arg(&index_arg, data_schema, rowkey_size, allocator, tmp_cols))) {
        LOG_WARN("failed to adjust vec ivfpq arg", K(ret));
      }
    } else if (is_pq_code_table) {
      int64_t rowkey_size = 0;
      if (OB_FAIL(push_back_gen_col(tmp_cols, existing_center_id_col, generated_center_id_col))) {
        LOG_WARN("failed to push back center id col", K(ret));
      } else if (OB_FAIL(push_back_rowkey_col(tmp_cols, data_schema, rowkey_size))) {
        LOG_WARN("fail to push back rowkey col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_pq_center_ids_col, generated_pq_center_ids_col))) {
        LOG_WARN("failed to push back pq center id col", K(ret));
      } else if (OB_FAIL(adjust_vec_ivf_arg(&index_arg, data_schema, rowkey_size, allocator, tmp_cols))) {
        LOG_WARN("failed to adjust vec ivfpq arg", K(ret));
      }
    } else if (is_pq_rowkey_cid_table) {
      int64_t rowkey_size = 0;
      if (OB_FAIL(push_back_rowkey_col(tmp_cols, data_schema, rowkey_size))) {
        LOG_WARN("fail to push back rowkey col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_center_id_col, generated_center_id_col))) {
        LOG_WARN("failed to push back center id col", K(ret));
      } else if (OB_FAIL(push_back_gen_col(tmp_cols, existing_pq_center_ids_col, generated_pq_center_ids_col))) {
        LOG_WARN("failed to push back pq center id col", K(ret));
      } else if (OB_FAIL(adjust_vec_ivf_arg(&index_arg, data_schema, rowkey_size, allocator, tmp_cols))) {
        LOG_WARN("failed to adjust vec ivfpq arg", K(ret));
      }
    }
  }
  LOG_DEBUG("finish adjust_vec_ivfpq_args", K(ret), K(index_arg.index_type_), K(data_schema));
  return ret;
}

int ObVecIndexBuilderUtil::get_ivf_column_cnt(
    const ObIndexType index_type,
    const int64_t main_table_rowkey_size,
    int64_t &total_column_cnt,
    int64_t &index_column_cnt)
{
  int ret = OB_SUCCESS;
  if (share::schema::is_vec_ivfflat_centroid_index(index_type) ||
      share::schema::is_vec_ivfsq8_centroid_index(index_type) ||
      share::schema::is_vec_ivfpq_centroid_index(index_type) ||
      share::schema::is_vec_ivfpq_pq_centroid_index(index_type) ||
      share::schema::is_vec_ivfsq8_meta_index(index_type)) {
    total_column_cnt = 2;                           /* 没有冗余主表主键，共2列 */
    index_column_cnt = 1;                           /* 只有1列索引列 */
  } else if (share::schema::is_vec_ivfflat_cid_vector_index(index_type) ||
             share::schema::is_vec_ivfsq8_cid_vector_index(index_type)) {
    total_column_cnt = main_table_rowkey_size + 2;  /* 除主表主键外，索引辅助表列数为2*/
    index_column_cnt = total_column_cnt - 1;        /* 索引辅助表非主键列数量为1 */
  } else if (share::schema::is_vec_ivfflat_rowkey_cid_index(index_type) ||
             share::schema::is_vec_ivfsq8_rowkey_cid_index(index_type)) {
    total_column_cnt = main_table_rowkey_size + 1;  /* 除主表主键外，索引辅助表列数为1 */
    index_column_cnt = total_column_cnt - 1;        /* 索引辅助表非主键列数量为1 */
  } else if (share::schema::is_vec_ivfpq_rowkey_cid_index(index_type)) {
    total_column_cnt = main_table_rowkey_size + 2;  /* 除主表主键外，索引辅助表列数为2 */
    index_column_cnt = total_column_cnt - 2;        /* 索引辅助表非主键列数量为2 */
  } else if (share::schema::is_vec_ivfpq_code_index(index_type)) {
    total_column_cnt = main_table_rowkey_size + 2;  /* 除主表主键外，索引辅助表列数为2 */
    index_column_cnt = total_column_cnt - 1;        /* 索引辅助表非主键列数量为1 */
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index type", K(ret), K(index_type));
  }
  LOG_DEBUG("finish get_ivf_column_cnt", K(ret),
    K(index_type), K(total_column_cnt), K(index_column_cnt), K(main_table_rowkey_size));
  return ret;
}

/*
  设置index_arg的index_column（辅助表rowkey）和store_column（辅助表普通列）
  外层调用该函数的时候，需要保证vec_cols数组元素push_back的顺序是先主键列（主表rowkey以及索引辅助表rowkey），后普通列
*/
int ObVecIndexBuilderUtil::adjust_vec_ivf_arg(
    ObCreateIndexArg *index_arg,
    const ObTableSchema &data_schema,
    const int64_t rowkey_size,
    ObIAllocator &allocator,
    const ObIArray<const ObColumnSchemaV2 *> &vec_cols)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_vec_ivf_index(index_arg->index_type_) ||
      !data_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema));
  } else {
    int64_t total_column_cnt = 0;
    int64_t index_column_cnt = 0;
    const ObIndexType &index_type = index_arg->index_type_;

    if (OB_FAIL(get_ivf_column_cnt(index_type, rowkey_size, total_column_cnt, index_column_cnt))) {
      LOG_WARN("fail to get ivf column cnt", K(ret), K(index_type));
    } else if ((vec_cols.count() != total_column_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vec cols count not expected", K(ret), K(index_type), K(vec_cols), K(rowkey_size));
    } else {
      index_arg->index_columns_.reuse();
      index_arg->store_columns_.reuse();
      // 1. add index rowkey column to arg->index_columns
      for (int64_t i = 0; OB_SUCC(ret) && i < index_column_cnt; ++i) {
        ObColumnSortItem tmp_column;
        const ObColumnSchemaV2 *col_schema = vec_cols.at(i);
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("vec col is null", K(ret));
        } else if (OB_FAIL(ob_write_string(allocator, col_schema->get_column_name_str(), tmp_column.column_name_))) {
          //to keep the memory lifetime of column_name consistent with index_arg
          LOG_WARN("deep copy column name failed", K(ret));
        } else if (OB_FAIL(index_arg->index_columns_.push_back(tmp_column))) {
          LOG_WARN("failed to push back vid id column", K(ret));
        }
      }
      // 2. // 2. add index common column to arg->store_columns_
      for (int64_t i = index_column_cnt; OB_SUCC(ret) && i < total_column_cnt; ++i) {
        ObString tmp_column_name;
        const ObColumnSchemaV2 *col_schema = vec_cols.at(i);
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("vec col is null", K(ret));
        } else if (OB_FAIL(ob_write_string(allocator, col_schema->get_column_name_str(), tmp_column_name))) {
          //to keep the memory lifetime of column_name consistent with index_arg
          LOG_WARN("deep copy column name failed", K(ret));
        } else if (OB_FAIL(index_arg->store_columns_.push_back(tmp_column_name))) {
          LOG_WARN("failed to push back vid id column", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObVecIndexBuilderUtil::adjust_vec_arg(
    ObCreateIndexArg *index_arg,
    const ObTableSchema &data_schema,
    ObIAllocator &allocator,
    const ObIArray<const ObColumnSchemaV2 *> &vec_cols)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_vec_index(index_arg->index_type_) ||
      !data_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema));
  } else {
    const ObIndexType &index_type = index_arg->index_type_;
    const bool is_vec_rowkey_vid = share::schema::is_vec_rowkey_vid_type(index_arg->index_type_);
    const bool is_vec_vid_rowkey = share::schema::is_vec_vid_rowkey_type(index_arg->index_type_);
    const bool is_vec_delta_buffer = share::schema::is_vec_delta_buffer_type(index_arg->index_type_);
    const bool is_vec_index_id = share::schema::is_vec_index_id_type(index_arg->index_type_);
    const bool is_vec_index_snapshot_data = share::schema::is_vec_index_snapshot_data_type(index_arg->index_type_);

    if ((is_vec_rowkey_vid && vec_cols.count() != 1) ||   /* rowkey_vid_table 的生成列数，由于不需要生成主表主键列，因此只有1列 */
        (is_vec_vid_rowkey && vec_cols.count() != 1) ||   /* vid_rowkey_table 的生成列数，由于不需要生成主表主键列，因此只有1列*/
        (is_vec_delta_buffer && vec_cols.count() != 3) || /* delta_buffer_table 的生成列数，不算伪列，共3列 */
        (is_vec_index_id && vec_cols.count() != 4) ||     /* index_table_id 的生成列数，共4列 */
        (is_vec_index_snapshot_data && vec_cols.count() != 4) ) { /* index_snapshot_data_table 的生成列数，共2列*/
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vec cols count not expected", K(ret), K(index_type), K(vec_cols));
    } else {
      index_arg->index_columns_.reuse();
      index_arg->store_columns_.reuse();
      if (is_vec_rowkey_vid) {
        // 1. add rowkey column to arg->index_columns
        const ObRowkeyInfo &rowkey_info = data_schema.get_rowkey_info();
        for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
          ObColumnSortItem rowkey_column;
          const ObColumnSchemaV2 *rowkey_col = NULL;
          uint64_t column_id = OB_INVALID_ID;
          if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
            LOG_WARN("get_column_id failed", "index", i, K(ret));
          } else if (NULL == (rowkey_col = data_schema.get_column_schema(column_id))) {
            ret = OB_ERR_BAD_FIELD_ERROR;
            LOG_WARN("get_column_schema failed", "table_id",
                data_schema.get_table_id(), K(column_id), K(ret));
          } else if (OB_FAIL(ob_write_string(allocator,
                                             rowkey_col->get_column_name_str(),
                                             rowkey_column.column_name_))) {
            //to keep the memory lifetime of column_name consistent with index_arg
            LOG_WARN("deep copy column name failed", K(ret));
          } else if (OB_FAIL(index_arg->index_columns_.push_back(rowkey_column))) {
            LOG_WARN("failed to push back rowkey column", K(ret));
          }
        }
        // 2. add vid column to arg->store_columns
        const ObColumnSchemaV2 *vid_col = vec_cols.at(0);
        ObString vid_col_name;
        if (FAILEDx(ob_write_string(allocator, vid_col->get_column_name_str(), vid_col_name))) {
          LOG_WARN("fail to deep copy vid id column name", K(ret));
        } else if (OB_FAIL(index_arg->store_columns_.push_back(vid_col_name))) {
          LOG_WARN("failed to push back vid id column", K(ret));
        }
      } else if (is_vec_vid_rowkey) {
        // add vid column to index_columns
        ObColumnSortItem vid_column;
        const ObColumnSchemaV2 *vid_col = vec_cols.at(0);
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(vid_col)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("vec col is null", K(ret));
        } else if (OB_FAIL(ob_write_string(allocator,
                                           vid_col->get_column_name_str(),
                                           vid_column.column_name_))) {
          //to keep the memory lifetime of column_name consistent with index_arg
          LOG_WARN("deep copy column name failed", K(ret));
        } else if (OB_FAIL(index_arg->index_columns_.push_back(vid_column))) {
          LOG_WARN("failed to push back vid id column", K(ret));
        }

      } else if (is_vec_delta_buffer) {
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(inner_adjust_vec_arg(index_arg,
                                                vec_cols,
                                                OB_VEC_DELTA_BUFFER_TABLE_INDEX_COL_CNT,
                                                &allocator))) {
          LOG_WARN("failed to inner_adjust_vec_arg", K(ret));
        }
      } else if (is_vec_index_id) {
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(inner_adjust_vec_arg(index_arg,
                                                vec_cols,
                                                OB_VEC_INDEX_ID_TABLE_INDEX_COL_CNT,
                                                &allocator))) {
          LOG_WARN("failed to inner_adjust_vec_arg", K(ret));
        }
      } else if (is_vec_index_snapshot_data) {
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(inner_adjust_vec_arg(index_arg,
                                                vec_cols,
                                                OB_VEC_INDEX_SNAPSHOT_DATA_TABLE_INDEX_COL_CNT,
                                                &allocator))) {
          LOG_WARN("failed to inner_adjust_vec_arg", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObVecIndexBuilderUtil::inner_adjust_vec_arg(
    obrpc::ObCreateIndexArg *vec_arg,
    const ObIArray<const ObColumnSchemaV2 *> &vec_cols,
    const int index_column_cnt,   // 辅助表的主键列数
    ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vec_arg) || OB_ISNULL(allocator) ||
     (!share::schema::is_vec_delta_buffer_type(vec_arg->index_type_) &&
      !share::schema::is_vec_index_id_type(vec_arg->index_type_) &&
      !share::schema::is_vec_index_snapshot_data_type(vec_arg->index_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), KPC(vec_arg), KP(allocator));
  } else if ((share::schema::is_vec_delta_buffer_type(vec_arg->index_type_) ||
              share::schema::is_vec_index_id_type(vec_arg->index_type_)) &&
              vec_cols.count() != index_column_cnt + 1) { // index_rowkey_column_cnt + common_col_cnt。 delta_buffer_table 和 index_id_table 的非主键列为1
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(vec_cols.count()), K(index_column_cnt));
  } else if (share::schema::is_vec_index_snapshot_data_type(vec_arg->index_type_) &&
             vec_cols.count() != index_column_cnt + 3) {  // index_rowkey_column_cnt + common_col_cnt , snapshot_data 的非主键列为3
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", K(ret), K(vec_cols.count()), K(index_column_cnt));
  } else {
    // 1. add assistant table rowkey column to arg->index_columns
    for (int64_t i = 0; OB_SUCC(ret) && i < index_column_cnt; ++i) {
      ObColumnSortItem vec_column;
      const ObColumnSchemaV2 *vec_col = vec_cols.at(i);
      if (OB_ISNULL(vec_col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("vec_col is null", K(ret), K(i));
      } else if (OB_FAIL(ob_write_string(*allocator,
              vec_col->get_column_name_str(),
              vec_column.column_name_))) {
        //to keep the memory lifetime of column_name consistent with index_arg
        LOG_WARN("deep copy column name failed", K(ret));
      } else if (OB_FAIL(vec_arg->index_columns_.push_back(vec_column))) {
        LOG_WARN("failed to push back index column", K(ret));
      }
    }
    // 2. add none assistant table none rowkey column to arg->store_columns
    for (int64_t i = index_column_cnt; i < vec_cols.count(); ++i) {
      const ObColumnSchemaV2 *other_col = vec_cols.at(i);
      ObString other_col_name;
      if (FAILEDx(ob_write_string(*allocator, other_col->get_column_name_str(), other_col_name))) {
        LOG_WARN("fail to deep copy other column name", K(ret));
      } else if (OB_FAIL(vec_arg->store_columns_.push_back(other_col_name))) {
        LOG_WARN("failed to push back other column", K(ret));
      }
    }
  }
  return ret;
}

int ObVecIndexBuilderUtil::push_back_rowkey_col(
    ObIArray<const ObColumnSchemaV2 *> &cols,
    const ObTableSchema &data_schema,
    int64_t &rowkey_size)
{
  int ret = OB_SUCCESS;
  const ObRowkeyInfo &rowkey_info = data_schema.get_rowkey_info();
  rowkey_size = rowkey_info.get_size();
  if (rowkey_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rowkey size", K(ret), K(rowkey_size));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_size; ++i) {
      ObColumnSortItem rowkey_column;
      const ObColumnSchemaV2 *rowkey_col = NULL;
      uint64_t column_id = OB_INVALID_ID;
      if (OB_FAIL(rowkey_info.get_column_id(i, column_id))) {
        LOG_WARN("get_column_id failed", "index", i, K(ret));
      } else if (NULL == (rowkey_col = data_schema.get_column_schema(column_id))) {
        ret = OB_ERR_BAD_FIELD_ERROR;
        LOG_WARN("get_column_schema failed", K(ret), K(column_id), K(data_schema));
      } else if (OB_FAIL(cols.push_back(rowkey_col))) {
        LOG_WARN("fail to push back rowkey col", K(ret));
      }
    }
  }
  return ret;
}


int ObVecIndexBuilderUtil::push_back_gen_col(
    ObIArray<const ObColumnSchemaV2 *> &cols,
    const ObColumnSchemaV2 *existing_col,
    ObColumnSchemaV2 *generated_col)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(existing_col)) {
    if (OB_FAIL(cols.push_back(existing_col))) {
      LOG_WARN("failed to push back existing col", K(ret));
    }
  } else {
    if (OB_ISNULL(generated_col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generated col is nullptr", K(ret));
    } else if (OB_FAIL(cols.push_back(generated_col))) {
      LOG_WARN("failed to push back generated col", K(ret));
    }
  }
  return ret;
}

int ObVecIndexBuilderUtil::construct_ivf_partial_column_info(
    char *vec_expr_def,
    const VecColType col_type,
    int64_t &def_pos,
    ObCollationType &collation_type,
    ObObjType &obj_type,
    int64_t &col_flag)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vec_expr_def)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected expr def", K(ret));
  } else {
    switch (col_type) {
      case IVF_CENTER_ID_COL: {
        if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos, "VEC_IVF_CENTER_ID("))) {
          LOG_WARN("print generate expr definition prefix failed", K(ret));
        } else {
          collation_type = CS_TYPE_BINARY;
          obj_type = ObVarcharType;
          col_flag = GENERATED_VEC_IVF_CENTER_ID_COLUMN_FLAG;
        }
        break;
      }
      case IVF_CENTER_VECTOR_COL: {
        if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos, "VEC_IVF_CENTER_VECTOR("))) {
          LOG_WARN("print generate expr definition prefix failed", K(ret));
        } else {
          collation_type = CS_TYPE_BINARY;
          obj_type = ObCollectionSQLType;
          col_flag = GENERATED_VEC_IVF_CENTER_VECTOR_COLUMN_FLAG;
        }
        break;
      }
      case IVF_FLAT_DATA_VECTOR_COL: {
        if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos, "VEC_IVF_FLAT_DATA_VECTOR("))) {
          LOG_WARN("print generate expr definition prefix failed", K(ret));
        } else {
          collation_type = CS_TYPE_BINARY;
          obj_type = ObCollectionSQLType;
          col_flag = GENERATED_VEC_IVF_DATA_VECTOR_COLUMN_FLAG;
        }
        break;
      }
      case IVF_SQ8_DATA_VECTOR_COL: {
        if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos, "VEC_IVF_SQ8_DATA_VECTOR("))) {
          LOG_WARN("print generate expr definition prefix failed", K(ret));
        } else {
          collation_type = CS_TYPE_BINARY;
          obj_type = ObCollectionSQLType;
          col_flag = GENERATED_VEC_IVF_DATA_VECTOR_COLUMN_FLAG;
        }
        break;
      }
      case IVF_META_ID_COL: {
        if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos, "VEC_IVF_META_ID("))) {
          LOG_WARN("print generate expr definition prefix failed", K(ret));
        } else {
          collation_type = CS_TYPE_BINARY;
          obj_type = ObVarcharType;
          col_flag = GENERATED_VEC_IVF_META_ID_COLUMN_FLAG;
        }
        break;
      }
      case IVF_META_VECTOR_COL: {
        if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos, "VEC_IVF_META_VECTOR("))) {
          LOG_WARN("print generate expr definition prefix failed", K(ret));
        } else {
          collation_type = CS_TYPE_BINARY;
          obj_type = ObCollectionSQLType;
          col_flag = GENERATED_VEC_IVF_META_VECTOR_COLUMN_FLAG;
        }
        break;
      }
      case IVF_PQ_CENTER_ID_COL: {
        if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos, "VEC_IVF_PQ_CENTER_ID("))) {
          LOG_WARN("print generate expr definition prefix failed", K(ret));
        } else {
          collation_type = CS_TYPE_BINARY;
          obj_type = ObVarcharType;
          col_flag = GENERATED_VEC_IVF_PQ_CENTER_ID_COLUMN_FLAG;
        }
        break;
      }
      case IVF_PQ_CENTER_IDS_COL: {
        if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos, "VEC_IVF_PQ_CENTER_IDS("))) {
          LOG_WARN("print generate expr definition prefix failed", K(ret));
        } else {
          collation_type = CS_TYPE_BINARY;
          obj_type = ObCollectionSQLType;
          col_flag = GENERATED_VEC_IVF_PQ_CENTER_IDS_COLUMN_FLAG;
        }
        break;
      }
      case IVF_PQ_CENTER_VECTOR_COL: {
        if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos, "VEC_IVF_PQ_CENTER_VECTOR("))) {
          LOG_WARN("print generate expr definition prefix failed", K(ret));
        } else {
          collation_type = CS_TYPE_BINARY;
          obj_type = ObCollectionSQLType;
          col_flag = GENERATED_VEC_IVF_CENTER_VECTOR_COLUMN_FLAG;
        }
        break;
      }
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected col type", K(ret), K(col_type));
        break;
    }
  }
  return ret;
}

int ObVecIndexBuilderUtil::generate_vec_ivf_column(
    const ObCreateIndexArg *index_arg,
    const uint64_t col_id,
    const VecColType col_type,
    ObTableSchema &data_schema,
    ObColumnSchemaV2 *&col_schema)
{
  int ret = OB_SUCCESS;
  col_schema = nullptr;
  char col_name_buf[OB_MAX_COLUMN_NAME_LENGTH] = {'\0'};
  int64_t name_pos = 0;
  bool col_exists = false;
  if (OB_ISNULL(index_arg) || !data_schema.is_valid() || col_id == OB_INVALID_ID ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema), K(col_id));
  } else if (OB_FAIL(construct_ivf_col_name(index_arg, data_schema, col_type, col_name_buf, OB_MAX_COLUMN_NAME_LENGTH, name_pos))) {
    LOG_WARN("failed to construct vector column name", K(ret));
  } else if (OB_FAIL(check_vec_gen_col(data_schema, col_id, col_name_buf, name_pos, col_exists))) {
    LOG_WARN("check vec gen column failed", K(ret));
  } else if (!col_exists) {
    ObCollationType collection_type;
    ObObjType obj_type;
    int64_t col_flag;
    ObColumnSchemaV2 column_schema;
    SMART_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], vec_expr_def) {
      ObArray<ObString> extend_type_info;
      ObArenaAllocator tmp_alloc;
      MEMSET(vec_expr_def, 0, sizeof(vec_expr_def));
      int64_t def_pos = 0;
      if (OB_FAIL(construct_ivf_partial_column_info(vec_expr_def, col_type, def_pos, collection_type, obj_type, col_flag))) {
        LOG_WARN("fail to get ivf column def", K(ret), K(col_type));
      }
      // 这里的 index_arg->index_columns_ 包含了向量索引列，目前仅支持单列
      for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
        const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
        ObColumnSchemaV2 *tmp_col_schema = nullptr;
        if (column_name.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column name is empty", K(ret), K(column_name));
        } else if (OB_ISNULL(tmp_col_schema = data_schema.get_column_schema(column_name))) {
          ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
          LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(), column_name.ptr());
        } else if (OB_FAIL(column_schema.add_cascaded_column_id(tmp_col_schema->get_column_id()))) {
          LOG_WARN("add cascaded column to generated column failed", K(ret));
        } else if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos, "`%s`, ", tmp_col_schema->get_column_name()))) {
          LOG_WARN("print column name to buffer failed", K(ret));
        } else if (ObCollectionSQLType == obj_type) {
          if (tmp_col_schema->get_extended_type_info().count() != 1) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("count of extended_type_info should be 1", K(ret), K(tmp_col_schema->get_extended_type_info().count()));
          } else if (col_type == IVF_PQ_CENTER_IDS_COL) {
            // pq center ids col type is ARRAY(VARBINARY)
            if (OB_FAIL(extend_type_info.push_back(ObVecIndexBuilderUtil::IVF_PQ_CENTER_IDS_COL_TYPE_NAME))) {
              LOG_WARN("fail to push back conv type str", K(ret));
            }
          } else if (col_type == IVF_SQ8_DATA_VECTOR_COL) {
            // sq8 data col type is VECTOR(UTINYINT, <vec dim>)
            // NOTE(liyao): use tmp alloc since 'column_schema.set_extended_type_info' will deep copy
            ObSqlCollectionInfo tmp_type_info(tmp_alloc);
            tmp_type_info.set_name(tmp_col_schema->get_extended_type_info().at(0));
            if (OB_FAIL(tmp_type_info.parse_type_info())) {
              LOG_WARN("fail to parse type info", K(ret), K(tmp_col_schema->get_extended_type_info().at(0)));
            } else {
              ObCollectionArrayType *arr_type = static_cast<ObCollectionArrayType *>(tmp_type_info.collection_meta_);
              ObString new_type_info;
              if (OB_ISNULL(arr_type) || OB_ISNULL(arr_type->element_type_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("arr should be vector", K(ret), KP(arr_type));
              } else if (OB_FAIL(arr_type->generate_spec_type_info("UTINYINT", new_type_info))) {
                LOG_WARN("fail to generate spec type info", K(ret));
              } else if (OB_FAIL(extend_type_info.push_back(new_type_info))) {
                LOG_WARN("fail to push back conv type str", K(ret), K(new_type_info));
              }
            }
          } else {
            if (OB_FAIL(extend_type_info.assign(tmp_col_schema->get_extended_type_info()))) {
              LOG_WARN("fail to assign extend type info", K(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        def_pos -= 2; // remove last ", "
        if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos, ")"))) {
          LOG_WARN("print generate expr definition suffix failed", K(ret));
        } else {
          ObObj default_value;
          default_value.set_varchar(vec_expr_def, static_cast<int32_t>(def_pos));
          column_schema.set_rowkey_position(0); //非主键列
          column_schema.set_index_position(0); //非索引列
          column_schema.set_tbl_part_key_pos(0); //非partition key
          column_schema.set_tenant_id(data_schema.get_tenant_id());
          column_schema.set_table_id(data_schema.get_table_id());
          column_schema.set_column_id(col_id);
          column_schema.add_column_flag(col_flag);
          column_schema.add_column_flag(VIRTUAL_GENERATED_COLUMN_FLAG);
          column_schema.set_is_hidden(true);
          column_schema.set_data_type(obj_type);
          column_schema.set_data_length(0);
          column_schema.set_collation_type(collection_type);
          column_schema.set_prev_column_id(UINT64_MAX);
          column_schema.set_next_column_id(UINT64_MAX);
          column_schema.set_nullable(true);
          if (OB_FAIL(column_schema.set_extended_type_info(extend_type_info))) {
            LOG_WARN("fail to set extend type info", K(ret), K(extend_type_info));
          } else if (OB_FAIL(column_schema.set_column_name(col_name_buf))) {
            LOG_WARN("set column name failed", K(ret));
          } else if (OB_FAIL(column_schema.set_orig_default_value(default_value))) {
            LOG_WARN("set orig default value failed", K(ret));
          } else if (OB_FAIL(column_schema.set_cur_default_value(default_value, column_schema.is_default_expr_v2_column()))) {
            LOG_WARN("set current default value failed", K(ret));
          } else if (OB_FAIL(data_schema.add_column(column_schema))) {
            LOG_WARN("add column schema to data table failed", K(ret));
          } else {
            col_schema = data_schema.get_column_schema(column_schema.get_column_id());
            if (OB_ISNULL(col_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("generate ivf column failed", K(ret), KP(col_schema));
            } else {
              LOG_INFO("succeed to generate ivf column", KCSTRING(col_name_buf), K(col_id), K(data_schema));
            }
          }
        }
      }
    }
  }
  return ret;
}


int ObVecIndexBuilderUtil::generate_vid_column(
    const ObCreateIndexArg *index_arg,
    const uint64_t col_id,
    ObTableSchema &data_schema,
    ObColumnSchemaV2 *&vid_col)
{
  int ret = OB_SUCCESS;
  vid_col = nullptr;
  char col_name_buf[OB_MAX_COLUMN_NAME_LENGTH] = {'\0'};
  int64_t name_pos = 0;
  bool col_exists = false;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_vec_index(index_arg->index_type_) ||
      !data_schema.is_valid() ||
      col_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema), K(col_id));
  } else if (OB_FAIL(construct_vid_col_name(col_name_buf, OB_MAX_COLUMN_NAME_LENGTH, name_pos))) {
    LOG_WARN("failed to construct vid column name", K(ret));
  } else if (OB_FAIL(check_vec_gen_col(data_schema, col_id, col_name_buf, name_pos, col_exists))) {
    LOG_WARN("check vid column failed", K(ret));
  } else if (!col_exists) {
    const ObRowkeyInfo &rowkey_info = data_schema.get_rowkey_info();
    const ObColumnSchemaV2 *col_schema = nullptr;
    SMART_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], vec_expr_def) {
      MEMSET(vec_expr_def, 0, sizeof(vec_expr_def));
      int64_t def_pos = 0;
      if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos, "VEC_VID()"))) {
        LOG_WARN("print generate expr definition prefix failed", K(ret));
      } else {
        ObColumnSchemaV2 column_schema;
        ObObj default_value;
        default_value.set_varchar(vec_expr_def, static_cast<int32_t>(def_pos));
        column_schema.set_rowkey_position(0); //非主键列
        column_schema.set_index_position(0); //非索引列
        column_schema.set_tbl_part_key_pos(0); //非partition key
        column_schema.set_tenant_id(data_schema.get_tenant_id());
        column_schema.set_table_id(data_schema.get_table_id());
        column_schema.set_column_id(col_id);
        column_schema.add_column_flag(GENERATED_VEC_VID_COLUMN_FLAG);
        column_schema.add_column_flag(VIRTUAL_GENERATED_COLUMN_FLAG);
        column_schema.set_is_hidden(true);
        column_schema.set_nullable(false);
        column_schema.set_data_type(ObIntType);
        column_schema.set_data_length(0);
        column_schema.set_collation_type(CS_TYPE_BINARY);
        column_schema.set_prev_column_id(UINT64_MAX);
        column_schema.set_next_column_id(UINT64_MAX);
        if (OB_FAIL(column_schema.set_column_name(col_name_buf))) {
          LOG_WARN("set column name failed", K(ret));
        } else if (OB_FAIL(column_schema.set_orig_default_value(default_value))) {
          LOG_WARN("set orig default value failed", K(ret));
        } else if (OB_FAIL(column_schema.set_cur_default_value(default_value, column_schema.is_default_expr_v2_column()))) {
          LOG_WARN("set current default value failed", K(ret));
        } else if (OB_FAIL(data_schema.add_column(column_schema))) {
          LOG_WARN("add column schema to data table failed", K(ret));
        } else {
          vid_col = data_schema.get_column_schema(column_schema.get_column_id());
          if (OB_ISNULL(vid_col)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("generate vid column schema failed", K(ret), KP(vid_col));
          } else {
            LOG_INFO("succeed to generate vid column schema", KCSTRING(col_name_buf), K(col_id), K(data_schema));
          }
        }
      }
    }
  }
  return ret;
}


int ObVecIndexBuilderUtil::generate_type_column(
    const ObCreateIndexArg *index_arg,
    const uint64_t col_id,
    ObTableSchema &data_schema,
    ObColumnSchemaV2 *&type_col)
{
  int ret = OB_SUCCESS;
  type_col = nullptr;
  char col_name_buf[OB_MAX_COLUMN_NAME_LENGTH] = {'\0'};
  int64_t name_pos = 0;
  bool col_exists = false;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_vec_index(index_arg->index_type_) ||
      !data_schema.is_valid() ||
      col_id == OB_INVALID_ID ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema), K(col_id));
  } else if (OB_FAIL(construct_type_col_name(index_arg, data_schema, col_name_buf,
      OB_MAX_COLUMN_NAME_LENGTH, name_pos))) {
    LOG_WARN("failed to construct type col name", K(ret));
  } else if (OB_FAIL(check_vec_gen_col(data_schema, col_id, col_name_buf, name_pos, col_exists))) {
    LOG_WARN("check vec gen col failed", K(ret));
  } else if (!col_exists) {
    ObColumnSchemaV2 column_schema;
    SMART_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], vec_expr_def) {
      ObArray<ObString> extend_type_info;
      MEMSET(vec_expr_def, 0, sizeof(vec_expr_def));
      int64_t def_pos = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos,
                                         "VEC_TYPE("))) {
        LOG_WARN("print generate expr definition prefix failed", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
        const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
        ObColumnSchemaV2 *col_schema = nullptr;
        if (column_name.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column name is empty", K(ret), K(column_name));
        } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
          ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
          LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(),
              column_name.ptr());
        } else if (OB_FAIL(column_schema.add_cascaded_column_id(col_schema->get_column_id()))) {
          LOG_WARN("add cascaded column to generated column failed", K(ret));
        } else if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos,
                                           "`%s`, ",
                                           col_schema->get_column_name()))) {
          LOG_WARN("print column name to buffer failed", K(ret));
        } else if (OB_FAIL(extend_type_info.assign(col_schema->get_extended_type_info()))) {
          LOG_WARN("fail to assign extend type info");
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        def_pos -= 2; // remove last ", "
        if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos, ")"))) {
          LOG_WARN("print generate expr definition suffix failed", K(ret));
        } else {
          ObObj default_value;
          default_value.set_varchar(vec_expr_def, static_cast<int32_t>(def_pos));
          column_schema.set_rowkey_position(0); //非主键列
          column_schema.set_index_position(0); //非索引列
          column_schema.set_tbl_part_key_pos(0); //非partition key
          column_schema.set_tenant_id(data_schema.get_tenant_id());
          column_schema.set_table_id(data_schema.get_table_id());
          column_schema.set_column_id(col_id);
          column_schema.add_column_flag(GENERATED_VEC_TYPE_COLUMN_FLAG);
          column_schema.add_column_flag(VIRTUAL_GENERATED_COLUMN_FLAG);
          column_schema.set_is_hidden(true);
          column_schema.set_data_type(ObCharType);    // char(1)
          column_schema.set_data_length(1);
          column_schema.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
          column_schema.set_prev_column_id(UINT64_MAX);
          column_schema.set_next_column_id(UINT64_MAX);
          if (OB_FAIL(column_schema.set_column_name(col_name_buf))) {
            LOG_WARN("set column name failed", K(ret));
          } else if (OB_FAIL(column_schema.set_orig_default_value(default_value))) {
            LOG_WARN("set orig default value failed", K(ret));
          } else if (OB_FAIL(column_schema.set_cur_default_value(default_value, column_schema.is_default_expr_v2_column()))) {
            LOG_WARN("set current default value failed", K(ret));
          } else if (OB_FAIL(data_schema.add_column(column_schema))) {
            LOG_WARN("add column schema to data table failed", K(ret));
          } else {
            type_col = data_schema.get_column_schema(column_schema.get_column_id());
            if (OB_ISNULL(type_col)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("generate type column failed", K(ret), KP(type_col));
            } else {
              LOG_INFO("succeed to generate type column", KCSTRING(col_name_buf), K(col_id), K(data_schema));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObVecIndexBuilderUtil::generate_vector_column(
    const ObCreateIndexArg *index_arg,
    const uint64_t col_id,
    ObTableSchema &data_schema,
    ObColumnSchemaV2 *&vector_col)
{
  int ret = OB_SUCCESS;
  vector_col = nullptr;
  char col_name_buf[OB_MAX_COLUMN_NAME_LENGTH] = {'\0'};
  int64_t name_pos = 0;
  bool col_exists = false;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_vec_index(index_arg->index_type_) ||
      !data_schema.is_valid() ||
      col_id == OB_INVALID_ID ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema), K(col_id));
  } else if (OB_FAIL(construct_vector_col_name(index_arg, data_schema, col_name_buf,
      OB_MAX_COLUMN_NAME_LENGTH, name_pos))) {
    LOG_WARN("failed to construct vector column name", K(ret));
  } else if (OB_FAIL(check_vec_gen_col(data_schema, col_id, col_name_buf, name_pos, col_exists))) {
    LOG_WARN("check vec gen column failed", K(ret));
  } else if (!col_exists) {
    ObColumnSchemaV2 column_schema;
    ObArray<ObString> extend_type_info;
    SMART_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], vec_expr_def) {
      MEMSET(vec_expr_def, 0, sizeof(vec_expr_def));
      int64_t def_pos = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos,
                                         "VEC_VECTOR("))) {
        LOG_WARN("print generate expr definition prefix failed", K(ret));
      }
      // 这里的 index_arg->index_columns_ 包含了向量索引列，目前仅支持单列
      for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
        const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
        ObColumnSchemaV2 *col_schema = nullptr;
        if (column_name.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column name is empty", K(ret), K(column_name));
        } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
          ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
          LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(),
              column_name.ptr());
        } else if (OB_FAIL(column_schema.add_cascaded_column_id(col_schema->get_column_id()))) {
          LOG_WARN("add cascaded column to generated column failed", K(ret));
        } else if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos,
                                           "`%s`, ",
                                           col_schema->get_column_name()))) {
          LOG_WARN("print column name to buffer failed", K(ret));
        } else if (OB_FAIL(extend_type_info.assign(col_schema->get_extended_type_info()))) {
          LOG_WARN("fail to assign extend type info", K(ret), KPC(col_schema));
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        def_pos -= 2; // remove last ", "
        if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos, ")"))) {
          LOG_WARN("print generate expr definition suffix failed", K(ret));
        } else {
          ObObj default_value;
          default_value.set_varchar(vec_expr_def, static_cast<int32_t>(def_pos));
          column_schema.set_rowkey_position(0); //非主键列
          column_schema.set_index_position(0); //非索引列
          column_schema.set_tbl_part_key_pos(0); //非partition key
          column_schema.set_tenant_id(data_schema.get_tenant_id());
          column_schema.set_table_id(data_schema.get_table_id());
          column_schema.set_column_id(col_id);
          column_schema.add_column_flag(GENERATED_VEC_VECTOR_COLUMN_FLAG);
          column_schema.add_column_flag(VIRTUAL_GENERATED_COLUMN_FLAG);
          column_schema.set_is_hidden(true);
          column_schema.set_data_type(ObCollectionSQLType);    // vector type
          column_schema.set_data_length(0);
          column_schema.set_collation_type(CS_TYPE_BINARY);
          column_schema.set_prev_column_id(UINT64_MAX);
          column_schema.set_next_column_id(UINT64_MAX);
          column_schema.set_nullable(true);
          if (OB_FAIL(column_schema.set_extended_type_info(extend_type_info))) {
            LOG_WARN("fail to set extend type info", K(ret), K(extend_type_info));
          } else if (OB_FAIL(column_schema.set_column_name(col_name_buf))) {
            LOG_WARN("set column name failed", K(ret));
          } else if (OB_FAIL(column_schema.set_orig_default_value(default_value))) {
            LOG_WARN("set orig default value failed", K(ret));
          } else if (OB_FAIL(column_schema.set_cur_default_value(default_value, column_schema.is_default_expr_v2_column()))) {
            LOG_WARN("set current default value failed", K(ret));
          } else if (OB_FAIL(data_schema.add_column(column_schema))) {
            LOG_WARN("add column schema to data table failed", K(ret));
          } else {
            vector_col = data_schema.get_column_schema(column_schema.get_column_id());
            if (OB_ISNULL(vector_col)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("generate vector column failed", K(ret), KP(vector_col));
            } else {
              LOG_INFO("succeed to generate vector column", KCSTRING(col_name_buf), K(col_id), K(data_schema));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObVecIndexBuilderUtil::generate_scn_column(
    const ObCreateIndexArg *index_arg,
    const uint64_t col_id,
    ObTableSchema &data_schema, // not const since will add column to data schema
    ObColumnSchemaV2 *&scn_col)
{
  int ret = OB_SUCCESS;
  scn_col = nullptr;
  char col_name_buf[OB_MAX_COLUMN_NAME_LENGTH] = {'\0'};
  int64_t name_pos = 0;
  bool col_exists = false;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_vec_index(index_arg->index_type_) ||
      !data_schema.is_valid() ||
      col_id == OB_INVALID_ID ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema), K(col_id));
  } else if (OB_FAIL(construct_scn_col_name(index_arg, data_schema, col_name_buf,
      OB_MAX_COLUMN_NAME_LENGTH, name_pos))) {
    LOG_WARN("failed to construct scn column name", K(ret));
  } else if (OB_FAIL(check_vec_gen_col(data_schema, col_id, col_name_buf, name_pos, col_exists))) {
    LOG_WARN("check scn column failed", K(ret));
  } else if (!col_exists) {
    ObColumnSchemaV2 column_schema;
    SMART_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], vec_expr_def) {
      MEMSET(vec_expr_def, 0, sizeof(vec_expr_def));
      int64_t def_pos = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos,
                                         "VEC_SCN("))) {
        LOG_WARN("print generate expr definition prefix failed", K(ret));
      }
      // 这里的 index_arg->index_columns_ 包含了向量索引列，目前仅支持单列
      for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
        const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
        ObColumnSchemaV2 *col_schema = nullptr;
        if (column_name.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column name is empty", K(ret), K(column_name));
        } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
          ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
          LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(),
              column_name.ptr());
        } else if (OB_FAIL(column_schema.add_cascaded_column_id(col_schema->get_column_id()))) {
          LOG_WARN("add cascaded column to generated column failed", K(ret));
        } else if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos,
                                           "`%s`, ",
                                           col_schema->get_column_name()))) {
          LOG_WARN("print column name to buffer failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        def_pos -= 2; // remove last ", "
        if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos, ")"))) {
          LOG_WARN("print generate expr definition suffix failed", K(ret));
        } else {
          ObObj default_value;
          default_value.set_varchar(vec_expr_def, static_cast<int32_t>(def_pos));
          column_schema.set_rowkey_position(0); //非主键列
          column_schema.set_index_position(0); //非索引列
          column_schema.set_tbl_part_key_pos(0); //非partition key
          column_schema.set_tenant_id(data_schema.get_tenant_id());
          column_schema.set_table_id(data_schema.get_table_id());
          column_schema.set_column_id(col_id);
          column_schema.add_column_flag(GENERATED_VEC_SCN_COLUMN_FLAG);
          column_schema.add_column_flag(VIRTUAL_GENERATED_COLUMN_FLAG);
          column_schema.set_is_hidden(true);
          column_schema.set_data_type(ObIntType);     // bigint
          column_schema.set_data_length(0);           // TODO@xiain: what length ?
          column_schema.set_collation_type(CS_TYPE_BINARY);
          column_schema.set_prev_column_id(UINT64_MAX);
          column_schema.set_next_column_id(UINT64_MAX);
          column_schema.set_nullable(true);
          if (OB_FAIL(column_schema.set_column_name(col_name_buf))) {
            LOG_WARN("set column name failed", K(ret));
          } else if (OB_FAIL(column_schema.set_orig_default_value(default_value))) {
            LOG_WARN("set orig default value failed", K(ret));
          } else if (OB_FAIL(column_schema.set_cur_default_value(default_value, column_schema.is_default_expr_v2_column()))) {
            LOG_WARN("set current default value failed", K(ret));
          } else if (OB_FAIL(data_schema.add_column(column_schema))) {
            LOG_WARN("add column schema to data table failed", K(ret));
          } else {
            scn_col = data_schema.get_column_schema(column_schema.get_column_id());
            if (OB_ISNULL(scn_col)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("generate scn column failed", K(ret), KP(scn_col));
            } else {
              LOG_INFO("succeed to generate scn column", KCSTRING(col_name_buf), K(col_id), K(data_schema));
            }
          }
        }
      }
    }
  }
  return ret;
}


int ObVecIndexBuilderUtil::generate_key_column(
    const ObCreateIndexArg *index_arg,
    const uint64_t col_id,
    ObTableSchema &data_schema, // not const since will add column to data schema
    ObColumnSchemaV2 *&key_col)
{
  int ret = OB_SUCCESS;
  key_col = nullptr;
  char col_name_buf[OB_MAX_COLUMN_NAME_LENGTH] = {'\0'};
  int64_t name_pos = 0;
  bool col_exists = false;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_vec_index(index_arg->index_type_) ||
      !data_schema.is_valid() ||
      col_id == OB_INVALID_ID ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema), K(col_id));
  } else if (OB_FAIL(construct_key_col_name(index_arg, data_schema, col_name_buf,
      OB_MAX_COLUMN_NAME_LENGTH, name_pos))) {
    LOG_WARN("failed to construct key col name", K(ret));
  } else if (OB_FAIL(check_vec_gen_col(data_schema, col_id, col_name_buf, name_pos, col_exists))) {
    LOG_WARN("check key col failed", K(ret));
  } else if (!col_exists) {
    ObColumnSchemaV2 column_schema;
    SMART_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], vec_expr_def) {
      MEMSET(vec_expr_def, 0, sizeof(vec_expr_def));
      int64_t def_pos = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos,
                                         "VEC_KEY("))) {
        LOG_WARN("print generate expr definition prefix failed", K(ret));
      }
      // 这里的 index_arg->index_columns_ 包含了向量索引列，目前仅支持单列
      for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
        const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
        ObColumnSchemaV2 *col_schema = nullptr;
        if (column_name.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column name is empty", K(ret), K(column_name));
        } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
          ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
          LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(),
              column_name.ptr());
        } else if (OB_FAIL(column_schema.add_cascaded_column_id(col_schema->get_column_id()))) {
          LOG_WARN("add cascaded column to generated column failed", K(ret));
        } else if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos,
                                           "`%s`, ",
                                           col_schema->get_column_name()))) {
          LOG_WARN("print column name to buffer failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        def_pos -= 2; // remove last ", "
        if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos, ")"))) {
          LOG_WARN("print generate expr definition suffix failed", K(ret));
        } else {
          ObObj default_value;
          default_value.set_varchar(vec_expr_def, static_cast<int32_t>(def_pos));
          column_schema.set_rowkey_position(0); //非主键列
          column_schema.set_index_position(0); //非索引列
          column_schema.set_tbl_part_key_pos(0); //非partition key
          column_schema.set_tenant_id(data_schema.get_tenant_id());
          column_schema.set_table_id(data_schema.get_table_id());
          column_schema.set_column_id(col_id);
          column_schema.add_column_flag(GENERATED_VEC_KEY_COLUMN_FLAG);
          column_schema.add_column_flag(VIRTUAL_GENERATED_COLUMN_FLAG);
          column_schema.set_is_hidden(true);
          column_schema.set_data_type(ObVarcharType);     // bigint
          column_schema.set_data_length(0);           // TODO@xiain: what length is fixed ?
          column_schema.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
          column_schema.set_prev_column_id(UINT64_MAX);
          column_schema.set_next_column_id(UINT64_MAX);
          if (OB_FAIL(column_schema.set_column_name(col_name_buf))) {
            LOG_WARN("set column name failed", K(ret));
          } else if (OB_FAIL(column_schema.set_orig_default_value(default_value))) {
            LOG_WARN("set orig default value failed", K(ret));
          } else if (OB_FAIL(column_schema.set_cur_default_value(default_value, column_schema.is_default_expr_v2_column()))) {
            LOG_WARN("set current default value failed", K(ret));
          } else if (OB_FAIL(data_schema.add_column(column_schema))) {
            LOG_WARN("add column schema to data table failed", K(ret));
          } else {
            key_col = data_schema.get_column_schema(column_schema.get_column_id());
            if (OB_ISNULL(key_col)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("generate key col failed", K(ret), KP(key_col));
            } else {
              LOG_INFO("succeed to generate key column", KCSTRING(col_name_buf), K(col_id), K(data_schema));
            }
          }
        }
      }
    }
  }
  return ret;
}


int ObVecIndexBuilderUtil::generate_data_column(
    const ObCreateIndexArg *index_arg,
    const uint64_t col_id,
    ObTableSchema &data_schema, // not const since will add column to data schema
    ObColumnSchemaV2 *&data_col)
{
  int ret = OB_SUCCESS;
  data_col = nullptr;
  char col_name_buf[OB_MAX_COLUMN_NAME_LENGTH] = {'\0'};
  int64_t name_pos = 0;
  bool col_exists = false;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_vec_index(index_arg->index_type_) ||
      !data_schema.is_valid() ||
      col_id == OB_INVALID_ID ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema), K(col_id));
  } else if (OB_FAIL(construct_data_col_name(index_arg, data_schema, col_name_buf,
      OB_MAX_COLUMN_NAME_LENGTH, name_pos))) {
    LOG_WARN("failed to construct data col name", K(ret));
  } else if (OB_FAIL(check_vec_gen_col(data_schema, col_id, col_name_buf, name_pos, col_exists))) {
    LOG_WARN("check vec column failed", K(ret));
  } else if (!col_exists) {
    ObColumnSchemaV2 column_schema;
    SMART_VAR(char[OB_MAX_DEFAULT_VALUE_LENGTH], vec_expr_def) {
      MEMSET(vec_expr_def, 0, sizeof(vec_expr_def));
      int64_t def_pos = 0;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos,
                                         "VEC_DATA("))) {
        LOG_WARN("print generate expr definition prefix failed", K(ret));
      }
      // 这里的 index_arg->index_columns_ 包含了向量索引列，目前仅支持单列
      for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
        const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
        ObColumnSchemaV2 *col_schema = nullptr;
        if (column_name.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column name is empty", K(ret), K(column_name));
        } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
          ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
          LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(),
              column_name.ptr());
        } else if (OB_FAIL(column_schema.add_cascaded_column_id(col_schema->get_column_id()))) {
          LOG_WARN("add cascaded column to generated column failed", K(ret));
        } else if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos,
                                           "`%s`, ",
                                           col_schema->get_column_name()))) {
          LOG_WARN("print column name to buffer failed", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        def_pos -= 2; // remove last ", "
        if (OB_FAIL(databuff_printf(vec_expr_def, OB_MAX_DEFAULT_VALUE_LENGTH, def_pos, ")"))) {
          LOG_WARN("print generate expr definition suffix failed", K(ret));
        } else {
          ObObj default_value;
          default_value.set_varchar(vec_expr_def, static_cast<int32_t>(def_pos));
          column_schema.set_rowkey_position(0); //非主键列
          column_schema.set_index_position(0); //非索引列
          column_schema.set_tbl_part_key_pos(0); //非partition key
          column_schema.set_tenant_id(data_schema.get_tenant_id());
          column_schema.set_table_id(data_schema.get_table_id());
          column_schema.set_column_id(col_id);
          column_schema.add_column_flag(GENERATED_VEC_DATA_COLUMN_FLAG);
          column_schema.add_column_flag(VIRTUAL_GENERATED_COLUMN_FLAG);
          column_schema.set_is_hidden(true);
          column_schema.set_data_type(ObLongTextType);     // bigint
          column_schema.set_data_length(0);           // TODO@xiain: what length is fixed ?
          column_schema.set_collation_type(CS_TYPE_BINARY);
          column_schema.set_prev_column_id(UINT64_MAX);
          column_schema.set_next_column_id(UINT64_MAX);
          if (OB_FAIL(column_schema.set_column_name(col_name_buf))) {
            LOG_WARN("set column name failed", K(ret));
          } else if (OB_FAIL(column_schema.set_orig_default_value(default_value))) {
            LOG_WARN("set orig default value failed", K(ret));
          } else if (OB_FAIL(column_schema.set_cur_default_value(default_value, column_schema.is_default_expr_v2_column()))) {
            LOG_WARN("set current default value failed", K(ret));
          } else if (OB_FAIL(data_schema.add_column(column_schema))) {
            LOG_WARN("add column schema to data table failed", K(ret));
          } else {
            data_col = data_schema.get_column_schema(column_schema.get_column_id());
            if (OB_ISNULL(data_col)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("generate data col failed", K(ret), KP(data_col));
            } else {
              LOG_INFO("succeed to generate data column", KCSTRING(col_name_buf), K(col_id), K(data_schema));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObVecIndexBuilderUtil::set_part_key_columns(
    const ObTableSchema &data_schema,
    ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  ObTableSchema::const_column_iterator tmp_begin = data_schema.column_begin();
  ObTableSchema::const_column_iterator tmp_end = data_schema.column_end();
  HEAP_VAR(ObRowDesc, row_desc) {
  for (; OB_SUCC(ret) && tmp_begin != tmp_end; tmp_begin++) {
    ObColumnSchemaV2 *col_schema = (*tmp_begin);
    if (OB_ISNULL(col_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret), KP(col_schema));
    } else if (!col_schema->is_tbl_part_key_column()) {
    } else if (is_part_key_column_exist(index_schema, *col_schema)) {
    } else if (OB_FAIL(ObIndexBuilderUtil::add_column(col_schema,
                                                      false/*is_index_column*/,
                                                      false/*is_rowkey*/,
                                                      ObOrderType::DESC,
                                                      row_desc,
                                                      index_schema,
                                                      false/*is_hidden*/,
                                                      true/*is_specified_storing_col*/))) {
      LOG_WARN("add_column failed", K(ret), KPC(col_schema), K(index_schema));
    } else {
      LOG_INFO("success to add part key column", K(ret), KPC(col_schema));
    }
  }
  } // row_desc

  return ret;
}


bool ObVecIndexBuilderUtil::is_part_key_column_exist(
    const ObTableSchema &index_schema, const ObColumnSchemaV2 &part_key_col)
{
  int ret = OB_SUCCESS;
  bool is_exists = false;
  const ObColumnSchemaV2 *vec_col = nullptr;
  const uint64_t col_id = part_key_col.get_column_id();
  if (OB_NOT_NULL(vec_col = index_schema.get_column_schema(col_id))) {
    is_exists = true;
    LOG_WARN("adding column is exist", K(index_schema), K(part_key_col));
  }
  return is_exists;
}

int ObVecIndexBuilderUtil::construct_ivf_col_name(
    const ObCreateIndexArg *index_arg,
    const ObTableSchema &data_schema,
    const VecColType col_type,
    char *col_name_buf,
    const int64_t buf_len,
    int64_t &name_pos)
{
  int ret = OB_SUCCESS;
  name_pos = 0;
  if (OB_ISNULL(index_arg) || !data_schema.is_valid() || OB_ISNULL(col_name_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema), K(col_name_buf));
  } else {
    MEMSET(col_name_buf, 0, buf_len);
    const ObColumnSchemaV2 *col_schema = NULL;
    switch (col_type) {
      case IVF_CENTER_ID_COL: {
        if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos,
            OB_VEC_IVF_CENTER_ID_COLUMN_NAME_PREFIX))) {
          LOG_WARN("print generate column prefix name failed", K(ret));
        }
        break;
      }
      case IVF_CENTER_VECTOR_COL:
      case IVF_PQ_CENTER_VECTOR_COL: {
        if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos,
            OB_VEC_IVF_CENTER_VECTOR_COLUMN_NAME_PREFIX))) {
          LOG_WARN("print generate column prefix name failed", K(ret));
        }
        break;
      }
      case IVF_FLAT_DATA_VECTOR_COL:
      case IVF_SQ8_DATA_VECTOR_COL: {
        if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos,
            OB_VEC_IVF_DATA_VECTOR_COLUMN_NAME_PREFIX))) {
          LOG_WARN("print generate column prefix name failed", K(ret));
        }
        break;
      }
      case IVF_META_ID_COL: {
        if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos,
            OB_VEC_IVF_META_ID_COLUMN_NAME_PREFIX))) {
          LOG_WARN("print generate column prefix name failed", K(ret));
        }
        break;
      }
      case IVF_META_VECTOR_COL: {
        if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos,
            OB_VEC_IVF_META_VECTOR_COLUMN_NAME_PREFIX))) {
          LOG_WARN("print generate column prefix name failed", K(ret));
        }
        break;
      }
      case IVF_PQ_CENTER_ID_COL: {
        if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos,
            OB_VEC_IVF_PQ_CENTER_ID_COLUMN_NAME_PREFIX))) {
          LOG_WARN("print generate column prefix name failed", K(ret));
        }
        break;
      }
      case IVF_PQ_CENTER_IDS_COL: {
        if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos,
            OB_VEC_IVF_PQ_CENTER_IDS_COLUMN_NAME_PREFIX))) {
          LOG_WARN("print generate column prefix name failed", K(ret));
        }
        break;
      }
      default :
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column type", K(ret), K(col_type));
    }
    // 这里的index_arg->index_columns_表示的是向量索引列，构造辅助表列名时，需要加上索引列id
    for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
      const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
      if (column_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(column_name));
      } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(), column_name.ptr());
      } else if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos, "_%ld", col_schema->get_column_id()))) {
        LOG_WARN("print column id to buffer failed", K(ret), K(col_schema->get_column_id()));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos, "_%lu", ObTimeUtility::current_time()))){
      LOG_WARN("fail to printf current time", K(ret));
    }
  }
  return ret;
}

int ObVecIndexBuilderUtil::construct_vid_col_name(
    char *col_name_buf,
    const int64_t buf_len,
    int64_t &name_pos)
{
  int ret = OB_SUCCESS;
  name_pos = 0;
  if (OB_ISNULL(col_name_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(col_name_buf));
  } else {
    MEMSET(col_name_buf, 0, buf_len);
    if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos,
                                OB_VEC_VID_COLUMN_NAME))) {
      LOG_WARN("print generate column name failed", K(ret));
    } else if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos, "_%lu", ObTimeUtility::current_time()))){
      LOG_WARN("fail to printf current time", K(ret));
    }
  }
  return ret;
}


int ObVecIndexBuilderUtil::construct_type_col_name(
    const ObCreateIndexArg *index_arg,
    const ObTableSchema &data_schema,
    char *col_name_buf,
    const int64_t buf_len,
    int64_t &name_pos)
{
  int ret = OB_SUCCESS;
  name_pos = 0;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_vec_index(index_arg->index_type_) ||
      !data_schema.is_valid() ||
      OB_ISNULL(col_name_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret),
      KPC(index_arg), K(data_schema), K(col_name_buf));
  } else {
    MEMSET(col_name_buf, 0, buf_len);
    if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos,
                                OB_VEC_TYPE_COLUMN_NAME_PREFIX))) {
      LOG_WARN("print generate column prefix name failed", K(ret));
    }
    const ObColumnSchemaV2 *col_schema = NULL;
    // 这里的index_arg->index_columns_表示的是向量索引列，构造辅助表列名时，需要加上索引列id
    for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
      const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
      if (column_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(column_name));
      } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(),
                       column_name.ptr());
      } else if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos,
                                         "_%ld",
                                         col_schema->get_column_id()))) {
        LOG_WARN("print column id to buffer failed", K(ret), K(col_schema->get_column_id()));
      }
    }
    if (FAILEDx(databuff_printf(col_name_buf, buf_len, name_pos, "_%lu", ObTimeUtility::current_time()))){
      LOG_WARN("fail to printf current time", K(ret));
    }
  }
  return ret;
}


int ObVecIndexBuilderUtil::construct_vector_col_name(
    const ObCreateIndexArg *index_arg,
    const ObTableSchema &data_schema,
    char *col_name_buf,
    const int64_t buf_len,
    int64_t &name_pos)
{
  int ret = OB_SUCCESS;
  name_pos = 0;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_vec_index(index_arg->index_type_) ||
      !data_schema.is_valid() ||
      OB_ISNULL(col_name_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema),
        K(col_name_buf));
  } else {
    MEMSET(col_name_buf, 0, buf_len);
    if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos,
                                OB_VEC_VECTOR_COLUMN_NAME_PREFIX))) {
      LOG_WARN("print generate column prefix name failed", K(ret));
    }
    const ObColumnSchemaV2 *col_schema = NULL;
    // 这里的index_arg->index_columns_表示的是向量索引列，构造辅助表列名时，需要加上索引列id
    for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
      const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
      if (column_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(column_name));
      } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(),
                       column_name.ptr());
      } else if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos,
                                         "_%ld",
                                         col_schema->get_column_id()))) {
        LOG_WARN("print column id to buffer failed", K(ret), K(col_schema->get_column_id()));
      }
    }
    if (FAILEDx(databuff_printf(col_name_buf, buf_len, name_pos, "_%lu", ObTimeUtility::current_time()))){
      LOG_WARN("fail to printf current time", K(ret));
    }
  }
  return ret;
}


int ObVecIndexBuilderUtil::construct_scn_col_name(
    const ObCreateIndexArg *index_arg,
    const ObTableSchema &data_schema,
    char *col_name_buf,
    const int64_t buf_len,
    int64_t &name_pos)
{
  int ret = OB_SUCCESS;
  name_pos = 0;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_vec_index(index_arg->index_type_) ||
      !data_schema.is_valid() ||
      OB_ISNULL(col_name_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema),
        K(col_name_buf));
  } else {
    MEMSET(col_name_buf, 0, buf_len);
    if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos,
                                OB_VEC_SCN_COLUMN_NAME_PREFIX))) {
      LOG_WARN("print generate column prefix name failed", K(ret));
    }
    const ObColumnSchemaV2 *col_schema = NULL;
    // 这里的index_arg->index_columns_表示的是向量索引列，构造辅助表列名时，需要加上索引列id
    for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
      const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
      if (column_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(column_name));
      } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(),
                       column_name.ptr());
      } else if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos,
                                         "_%ld",
                                         col_schema->get_column_id()))) {
        LOG_WARN("print column id to buffer failed", K(ret), K(col_schema->get_column_id()));
      }
    }
    if (FAILEDx(databuff_printf(col_name_buf, buf_len, name_pos, "_%lu", ObTimeUtility::current_time()))){
      LOG_WARN("fail to printf current time", K(ret));
    }
  }
  return ret;
}


int ObVecIndexBuilderUtil::construct_key_col_name(
    const ObCreateIndexArg *index_arg,
    const ObTableSchema &data_schema,
    char *col_name_buf,
    const int64_t buf_len,
    int64_t &name_pos)
{
  int ret = OB_SUCCESS;
  name_pos = 0;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_vec_index(index_arg->index_type_) ||
      !data_schema.is_valid() ||
      OB_ISNULL(col_name_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema),
        K(col_name_buf));
  } else {
    MEMSET(col_name_buf, 0, buf_len);
    if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos,
                                OB_VEC_KEY_COLUMN_NAME_PREFIX))) {
      LOG_WARN("print generate column prefix name failed", K(ret));
    }
    const ObColumnSchemaV2 *col_schema = NULL;
    // 这里的index_arg->index_columns_表示的是向量索引列，构造辅助表列名时，需要加上索引列id
    for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
      const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
      if (column_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(column_name));
      } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(),
                       column_name.ptr());
      } else if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos,
                                         "_%ld",
                                         col_schema->get_column_id()))) {
        LOG_WARN("print column id to buffer failed", K(ret), K(col_schema->get_column_id()));
      }
    }
    if (FAILEDx(databuff_printf(col_name_buf, buf_len, name_pos, "_%lu", ObTimeUtility::current_time()))){
      LOG_WARN("fail to printf current time", K(ret));
    }
  }
  return ret;
}


int ObVecIndexBuilderUtil::construct_data_col_name(
    const ObCreateIndexArg *index_arg,
    const ObTableSchema &data_schema,
    char *col_name_buf,
    const int64_t buf_len,
    int64_t &name_pos)
{
  int ret = OB_SUCCESS;
  name_pos = 0;
  if (OB_ISNULL(index_arg) ||
      !share::schema::is_vec_index(index_arg->index_type_) ||
      !data_schema.is_valid() ||
      OB_ISNULL(col_name_buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema),
        K(col_name_buf));
  } else {
    MEMSET(col_name_buf, 0, buf_len);
    if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos,
                                OB_VEC_DATA_COLUMN_NAME_PREFIX))) {
      LOG_WARN("print generate column prefix name failed", K(ret));
    }
    const ObColumnSchemaV2 *col_schema = NULL;
    // 这里的index_arg->index_columns_表示的是向量索引列，构造辅助表列名时，需要加上索引列id
    for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
      const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
      if (column_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column name is empty", K(ret), K(column_name));
      } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(),
                       column_name.ptr());
      } else if (OB_FAIL(databuff_printf(col_name_buf, buf_len, name_pos,
                                         "_%ld",
                                         col_schema->get_column_id()))) {
        LOG_WARN("print column id to buffer failed", K(ret), K(col_schema->get_column_id()));
      }
    }
    if (FAILEDx(databuff_printf(col_name_buf, buf_len, name_pos, "_%lu", ObTimeUtility::current_time()))){
      LOG_WARN("fail to printf current time", K(ret));
    }
  }
  return ret;
}

int ObVecIndexBuilderUtil::check_vec_cols(
    const ObCreateIndexArg *index_arg,
    ObTableSchema &data_schema)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 *col_schema = NULL;
  if (OB_ISNULL(index_arg) || !data_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(index_arg), K(data_schema.is_valid()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < index_arg->index_columns_.count(); ++i) {
    const ObString &column_name = index_arg->index_columns_.at(i).column_name_;
    if (column_name.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column name is empty", K(ret), K(column_name));
    } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
      ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
      LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(),
                     column_name.ptr());
    } else if (!col_schema->is_collection()) {  // vector index is collection column type
      ret = OB_ERR_BAD_VEC_INDEX_COLUMN;
      LOG_USER_ERROR(OB_ERR_BAD_VEC_INDEX_COLUMN, column_name.length(), column_name.ptr());
    } else {
      col_schema->add_column_flag(GENERATED_DEPS_CASCADE_FLAG);
    }
  }
  return ret;
}

/*
  检查在主表上是否已经创建了index_arg里索引的隐藏列
*/
int ObVecIndexBuilderUtil::get_vec_ivfflat_col(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg *index_arg,
    const ObColumnSchemaV2 *&center_id_col,
    const ObColumnSchemaV2 *&center_vector_col,
    const ObColumnSchemaV2 *&data_vector_col)
{
  int ret = OB_SUCCESS;
  schema::ColumnReferenceSet index_col_set;
  if (!data_schema.is_valid() || OB_ISNULL(index_arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), KPC(index_arg));
  } else if (OB_FAIL(get_index_column_ids(data_schema, *index_arg, index_col_set))) {
    LOG_WARN("fail to get index column ids", K(ret), K(data_schema), KPC(index_arg));
  } else {
    for (ObTableSchema::const_column_iterator iter = data_schema.column_begin();
         OB_SUCC(ret) &&
         (OB_ISNULL(center_id_col) || OB_ISNULL(center_vector_col) || OB_ISNULL(data_vector_col)) &&
         iter != data_schema.column_end();
         iter++) {
      const ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(data_schema));
      } else if (column_schema->is_vec_ivf_center_id_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          center_id_col = column_schema;
        }
      } else if (column_schema->is_vec_ivf_center_vector_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          center_vector_col = column_schema;
        }
      } else if (column_schema->is_vec_ivf_data_vector_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          data_vector_col = column_schema;
        }
      }
    }
  }
  return ret;
}

int ObVecIndexBuilderUtil::get_vec_ivfsq8_col(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg *index_arg,
    const ObColumnSchemaV2 *&meta_id_col,
    const ObColumnSchemaV2 *&meta_vector_col,
    const ObColumnSchemaV2 *&center_id_col,
    const ObColumnSchemaV2 *&center_vector_col,
    const ObColumnSchemaV2 *&data_vector_col)
{
  int ret = OB_SUCCESS;
  schema::ColumnReferenceSet index_col_set;
  if (!data_schema.is_valid() || OB_ISNULL(index_arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), KPC(index_arg));
  } else if (OB_FAIL(get_index_column_ids(data_schema, *index_arg, index_col_set))) {
    LOG_WARN("fail to get index column ids", K(ret), K(data_schema), KPC(index_arg));
  } else {
    for (ObTableSchema::const_column_iterator iter = data_schema.column_begin();
         OB_SUCC(ret) &&
         (OB_ISNULL(meta_id_col) || OB_ISNULL(meta_vector_col) || OB_ISNULL(center_id_col) ||
          OB_ISNULL(center_vector_col) || OB_ISNULL(data_vector_col)) &&
         iter != data_schema.column_end();
         iter++) {
      const ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(data_schema));
      } else if (column_schema->is_vec_ivf_meta_id_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          meta_id_col = column_schema;
        }
      } else if (column_schema->is_vec_ivf_meta_vector_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          meta_vector_col = column_schema;
        }
      } else if (column_schema->is_vec_ivf_center_id_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          center_id_col = column_schema;
        }
      } else if (column_schema->is_vec_ivf_center_vector_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          center_vector_col = column_schema;
        }
      } else if (column_schema->is_vec_ivf_data_vector_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          data_vector_col = column_schema;
        }
      }
    }
  }
  return ret;
}

int ObVecIndexBuilderUtil::get_vec_ivfpq_col(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg *index_arg,
    const ObColumnSchemaV2 *&center_id_col,
    const ObColumnSchemaV2 *&center_vector_col,
    const ObColumnSchemaV2 *&pq_center_id_col,
    const ObColumnSchemaV2 *&pq_center_ids_col)
{
  int ret = OB_SUCCESS;
  schema::ColumnReferenceSet index_col_set;
  if (!data_schema.is_valid() || OB_ISNULL(index_arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), KPC(index_arg));
  } else if (OB_FAIL(get_index_column_ids(data_schema, *index_arg, index_col_set))) {
    LOG_WARN("fail to get index column ids", K(ret), K(data_schema), KPC(index_arg));
  } else {
    for (ObTableSchema::const_column_iterator iter = data_schema.column_begin();
         OB_SUCC(ret) &&
         (OB_ISNULL(center_id_col) || OB_ISNULL(center_vector_col) || OB_ISNULL(pq_center_id_col) || OB_ISNULL(pq_center_ids_col)) &&
         iter != data_schema.column_end();
         iter++) {
      const ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(data_schema));
      } else if (column_schema->is_vec_ivf_center_id_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          center_id_col = column_schema;
        }
      } else if (column_schema->is_vec_ivf_center_vector_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          center_vector_col = column_schema;
        }
      } else if (column_schema->is_vec_ivf_pq_center_id_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          pq_center_id_col = column_schema;
        }
      } else if (column_schema->is_vec_ivf_pq_center_ids_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          pq_center_ids_col = column_schema;
        }
      }
    }
  }
  return ret;
}

/*
  非共享的辅助表字段，一张表中只有唯一一个column
*/
int ObVecIndexBuilderUtil::get_vec_vid_col(
    const ObTableSchema &data_schema,
    const ObColumnSchemaV2 *&vid_col)
{
  int ret = OB_SUCCESS;
  vid_col = nullptr;
  if (!data_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema));
  } else {
    for (ObTableSchema::const_column_iterator iter = data_schema.column_begin();
         OB_SUCC(ret) && OB_ISNULL(vid_col) && iter != data_schema.column_end();
         iter++) {
      const ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(data_schema));
      } else if (column_schema->is_vec_hnsw_vid_column()) {
        vid_col = column_schema;
      }
    }
  }
  return ret;
}

/*
  非共享辅助表中的column，由于一张主表上可能存在多个索引，有多个隐藏列，因此需要遍历查找
*/
int ObVecIndexBuilderUtil::get_vec_type_col(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg *index_arg,
    const ObColumnSchemaV2 *&type_col)
{
  int ret = OB_SUCCESS;
  schema::ColumnReferenceSet index_col_set;
  type_col = nullptr;
  if (!data_schema.is_valid() ||
      OB_ISNULL(index_arg) ||
      !share::schema::is_vec_index(index_arg->index_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), KPC(index_arg));
  } else if (OB_FAIL(get_index_column_ids(data_schema, *index_arg, index_col_set))) {
    LOG_WARN("fail to get index column ids", K(ret), K(data_schema), KPC(index_arg));
  } else {
    for (ObTableSchema::const_column_iterator iter = data_schema.column_begin();
         OB_SUCC(ret) && OB_ISNULL(type_col) && iter != data_schema.column_end();
         iter++) {
      const ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(data_schema));
      } else if (column_schema->is_vec_hnsw_type_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          type_col = column_schema;
        }
      }
    }
  }
  return ret;
}


int ObVecIndexBuilderUtil::get_vec_vector_col(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg *index_arg,
    const ObColumnSchemaV2 *&vector_col)
{
  int ret = OB_SUCCESS;
  schema::ColumnReferenceSet index_col_set;
  vector_col = nullptr;
  if (!data_schema.is_valid() ||
      OB_ISNULL(index_arg) ||
      !share::schema::is_vec_index(index_arg->index_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), KPC(index_arg));
  } else if (OB_FAIL(get_index_column_ids(data_schema, *index_arg, index_col_set))) {
    LOG_WARN("fail to get index column ids", K(ret), K(data_schema), KPC(index_arg));
  } else {
    for (ObTableSchema::const_column_iterator iter = data_schema.column_begin();
         OB_SUCC(ret) && OB_ISNULL(vector_col) && iter != data_schema.column_end();
         iter++) {
      const ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(data_schema));
      } else if (column_schema->is_vec_hnsw_vector_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          vector_col = column_schema;
        }
      }
    }
  }
  return ret;
}


int ObVecIndexBuilderUtil::get_vec_scn_col(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg *index_arg,
    const ObColumnSchemaV2 *&scn_col)
{
  int ret = OB_SUCCESS;
  schema::ColumnReferenceSet index_col_set;
  scn_col = nullptr;
  if (!data_schema.is_valid() ||
      OB_ISNULL(index_arg) ||
      !share::schema::is_vec_index(index_arg->index_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), KPC(index_arg));
  } else if (OB_FAIL(get_index_column_ids(data_schema, *index_arg, index_col_set))) {
    LOG_WARN("fail to get index column ids", K(ret), K(data_schema), KPC(index_arg));
  } else {
    for (ObTableSchema::const_column_iterator iter = data_schema.column_begin();
         OB_SUCC(ret) && OB_ISNULL(scn_col) && iter != data_schema.column_end();
         iter++) {
      const ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(data_schema));
      } else if (column_schema->is_vec_hnsw_scn_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          scn_col = column_schema;
        }
      }
    }
  }
  return ret;
}


int ObVecIndexBuilderUtil::get_vec_key_col(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg *index_arg,
    const ObColumnSchemaV2 *&key_col)
{
  int ret = OB_SUCCESS;
  schema::ColumnReferenceSet index_col_set;
  key_col = nullptr;
  if (!data_schema.is_valid() ||
      OB_ISNULL(index_arg) ||
      !share::schema::is_vec_index(index_arg->index_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), KPC(index_arg));
  } else if (OB_FAIL(get_index_column_ids(data_schema, *index_arg, index_col_set))) {
    LOG_WARN("fail to get index column ids", K(ret), K(data_schema), KPC(index_arg));
  } else {
    for (ObTableSchema::const_column_iterator iter = data_schema.column_begin();
         OB_SUCC(ret) && OB_ISNULL(key_col) && iter != data_schema.column_end();
         iter++) {
      const ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(data_schema));
      } else if (column_schema->is_vec_hnsw_key_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          key_col = column_schema;
        }
      }
    }
  }
  return ret;
}


int ObVecIndexBuilderUtil::get_vec_data_col(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg *index_arg,
    const ObColumnSchemaV2 *&data_col)
{
  int ret = OB_SUCCESS;
  schema::ColumnReferenceSet index_col_set;
  data_col = nullptr;
  if (!data_schema.is_valid() ||
      OB_ISNULL(index_arg) ||
      !share::schema::is_vec_index(index_arg->index_type_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), KPC(index_arg));
  } else if (OB_FAIL(get_index_column_ids(data_schema, *index_arg, index_col_set))) {
    LOG_WARN("fail to get index column ids", K(ret), K(data_schema), KPC(index_arg));
  } else {
    for (ObTableSchema::const_column_iterator iter = data_schema.column_begin();
         OB_SUCC(ret) && OB_ISNULL(data_col) && iter != data_schema.column_end();
         iter++) {
      const ObColumnSchemaV2 *column_schema = *iter;
      if (OB_ISNULL(column_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(data_schema));
      } else if (column_schema->is_vec_hnsw_data_column()) {
        bool is_match = false;
        if (OB_FAIL(check_index_match(*column_schema, index_col_set, is_match))) {
          LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
        } else if (is_match) {
          data_col = column_schema;
        }
      }
    }
  }
  return ret;
}

int ObVecIndexBuilderUtil::get_index_column_ids(
    const ObTableSchema &data_schema,
    const obrpc::ObCreateIndexArg &arg,
    schema::ColumnReferenceSet &index_column_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!share::schema::is_vec_index(arg.index_type_) || !data_schema.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(arg), K(data_schema));
  } else {
    const ObColumnSchemaV2 *col_schema = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.index_columns_.count(); ++i) {
      const ObString &column_name = arg.index_columns_.at(i).column_name_;
      if (column_name.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, column name is empty", K(ret), K(column_name));
      } else if (OB_ISNULL(col_schema = data_schema.get_column_schema(column_name))) {
        ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
        LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(), column_name.ptr());
      } else if (OB_FAIL(index_column_ids.add_member(col_schema->get_column_id()))) {
        LOG_WARN("fail to add index column id", K(ret), K(col_schema->get_column_id()));
      }
    }
  }
  return ret;
}
int ObVecIndexBuilderUtil::check_index_match(
    const schema::ObColumnSchemaV2 &column,
    const schema::ColumnReferenceSet &index_column_ids,
    bool &is_match)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 4> cascaded_col_ids;
  is_match = false;
  if (OB_UNLIKELY(!column.is_valid() || index_column_ids.is_empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(column), K(index_column_ids));
  } else if (OB_FAIL(column.get_cascaded_column_ids(cascaded_col_ids))) {
    LOG_WARN("fail to get cascaded column ids", K(ret), K(column));
  } else if (cascaded_col_ids.count() == index_column_ids.num_members()) {
    bool mismatch = false;
    for (int64_t i = 0; !mismatch && i < cascaded_col_ids.count(); ++i) {
      if (!index_column_ids.has_member(cascaded_col_ids.at(i))) {
        mismatch = true;
      }
    }
    is_match = !mismatch;
  }
  return ret;
}


int ObVecIndexBuilderUtil::check_vec_gen_col(
    const ObTableSchema &data_schema,
    const uint64_t col_id,
    const char *col_name_buf,
    const int64_t name_pos,
    bool &col_exists)
{
  int ret = OB_SUCCESS;
  col_exists = false;
  if (!data_schema.is_valid() ||
      OB_INVALID_ID == col_id ||
      OB_ISNULL(col_name_buf) ||
      name_pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(data_schema), K(col_id),
        KP(col_name_buf), K(name_pos));
  } else {
    // another fulltext index could have created the generated column
    const ObColumnSchemaV2 *vec_col = data_schema.get_column_schema(col_name_buf);
    if (OB_NOT_NULL(vec_col) && vec_col->get_column_id() != col_id) {
      // check the specified column id is consistent with the existed column schema
      ret = OB_ERR_INVALID_COLUMN_ID;
      LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_ID, static_cast<int>(name_pos),
          col_name_buf);
      LOG_WARN("Column id specified by create vector index mismatch "
               "with column schema id", K(ret), K(col_id), K(*vec_col));
    } else if (OB_ISNULL(vec_col) && OB_NOT_NULL(data_schema.get_column_schema(col_id))) {
      // check the specified column id is not used by others
      ret = OB_ERR_INVALID_COLUMN_ID;
      LOG_USER_ERROR(OB_ERR_INVALID_COLUMN_ID, static_cast<int>(name_pos),
          col_name_buf);
      LOG_WARN("Column id specified by create vector index has been used",
          K(ret), K(col_id));
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_NOT_NULL(vec_col)) {
      // the generated colum is created
      col_exists = true;
    } else {
      col_exists = false;
    }
  }
  return ret;
}

/*
  通过索引名和类型，获取3/4/5号表的table_schema
*/
int ObVecIndexBuilderUtil::get_vec_table_schema_by_name(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const int64_t tenant_id,
    const int64_t database_id,
    const ObString &index_name, /* domain index name */
    const share::schema::ObIndexType index_type,
    ObIAllocator *allocator,
    const ObTableSchema *&index_schema)
{
  int ret = OB_SUCCESS;
  ObString full_index_name;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == database_id ||
                  index_name.empty() || OB_ISNULL(allocator))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(database_id), K(index_name), KP(allocator));
  } else if (OB_FAIL(generate_vec_index_name(allocator,
                                             index_type,
                                             index_name,
                                             full_index_name))) {
    LOG_WARN("fail to generate vec index name", K(ret), K(index_type));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                                   database_id,
                                                   full_index_name,
                                                   true, /* is_index */
                                                   index_schema,
                                                   false, /* is_hidden_flag */
                                                   true/* is_built_in_flag */))) {
    LOG_WARN("fail to get table schema",
      K(ret), K(tenant_id), K(database_id), K(index_name), K(full_index_name), K(index_type));
  } else if (OB_ISNULL(index_schema)) {
    LOG_INFO("get vec table schema is null, maybe index has been drop", K(ret), K(full_index_name));
  }
  return ret;
}


}//end namespace rootserver
}//end namespace oceanbase
