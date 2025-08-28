/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_TABLE_MODELS_OB_HBASE_MODEL_H_
#define OCEANBASE_OBSERVER_TABLE_MODELS_OB_HBASE_MODEL_H_

#include "ob_i_model.h"
#include "observer/table/cf_service/ob_hbase_column_family_service.h"
#include "observer/table/common/ob_table_sequential_grouper.h"
#include "observer/table/group/ob_i_table_struct.h"
#include "observer/table/common/ob_hbase_common_struct.h"

namespace oceanbase
{   
namespace table
{

class ObHBaseModel : public ObIModel
{
public:
  explicit ObHBaseModel()
      : ObIModel(),
        query_session_id_(INVALID_SESSION_ID),
        timeout_ts_(0),
        is_multi_cf_req_(false),
        flags_(0)
  {}
  virtual ~ObHBaseModel() {}

  virtual int prepare(ObTableExecCtx &ctx,
                      const ObTableLSOpRequest &req,
                      ObTableLSOpResult &res) override;
  int prepare(ObTableExecCtx &ctx,
              const ObTableQueryAndMutateRequest &req,
              ObTableQueryAndMutateResult &res) override;

  int prepare(ObTableExecCtx &ctx,
              const ObTableQueryRequest &req,
              ObTableQueryResult &res) override;
  int prepare(ObTableExecCtx &arg_ctx,
              const ObTableQueryAsyncRequest &req,
              ObTableQueryAsyncResult &res,
              ObTableExecCtx *&ctx);
  virtual int work(ObTableExecCtx &ctx,
                   const ObTableLSOpRequest &req,
                   ObTableLSOpResult &res) override;
  virtual int work(ObTableExecCtx &ctx,
                   const common::ObIArray<ObTableLSOpRequest*> &reqs,
                   common::ObIArray<ObTableLSOpResult*> &results) override;
  virtual int work(ObTableExecCtx &ctx,
                   const ObTableQueryRequest &req,
                   ObTableQueryResult &res) override;
  virtual int work(ObTableExecCtx &ctx,
                   const ObTableQueryAndMutateRequest &req,
                   ObTableQueryAndMutateResult &res) override;
  virtual int after_work(ObTableExecCtx &ctx,
                         const ObTableLSOpRequest &req,
                         ObTableLSOpResult &res) override;
  virtual int before_response(ObTableExecCtx &ctx,
                              const ObTableLSOpRequest &req,
                              ObTableLSOpResult &res) override;

private:
  int sync_query(ObTableExecCtx &ctx, ObHbaseQuery* query, ObHbaseColumnFamilyService *cf_service, ObTableQueryResult &result);

  int put(ObHbaseTableCells &table_cells,
          ObTableExecCtx &exec_ctx,
          bool is_multi_cf_req);
  int query(const ObIArray<ObHbaseQuery*> &queries,
            ObTableExecCtx &exec_ctx,
            ObTableQueryResult &res,
            bool is_multi_cf_req);
  int check_and_mutate(const ObTableQMParam &request,
                       const ObTableExecCtx &exec_ctx,
                       ObTableQueryAndMutateResult &result);
  // group commit
  int multi_put(ObTableExecCtx &ctx,
                ObIArray<ObITableOp*> &ops);
  int modify_htable_timestamp(ObHbaseTabletCells *tablet_cell);
  int process_ls_op_result(ObTableExecCtx &ctx,
                           const ObTableLSOp &ls_op,
                           ObTableLSOpResult &ls_op_result);

private:
  using LSOPGrouper =  GenericSequentialGrouper<ObTableSingleOp, common::ObTabletID>;
  using BatchGrouper =  GenericSequentialGrouper<ObTableOperation, common::ObTabletID>;
  // LSOP
  int process_mutation_group(ObTableExecCtx &ctx,
                             LSOPGrouper::BatchGroupType &group,
                             ObHbaseColumnFamilyService &cf_service);

  int process_query_and_mutate_group(ObTableExecCtx &ctx,
                                     LSOPGrouper::BatchGroupType &group,
                                     ObHbaseColumnFamilyService &cf_service);
  
  int process_check_and_mutate_group(ObTableExecCtx &ctx,
                                     LSOPGrouper::BatchGroupType &group,
                                     ObHbaseColumnFamilyService &cf_service);

  int process_scan_group(ObTableExecCtx &ctx,
                         LSOPGrouper::BatchGroupType &group,
                         ObHbaseColumnFamilyService &cf_service,
                         ObTableLSOpResult &res);
  int aggregate_scan_result(ObTableExecCtx &ctx,
                            ObHbaseQueryResultIterator &result_iter,
                            ObTableSingleOpResult &single_op_result);
  // BATCH
  int process_batch_mutation_group(ObTableExecCtx &ctx,
                                   BatchGrouper::BatchGroupType &group,
                                   ObHbaseColumnFamilyService &cf_service);

  // CHECK_AND_MUTATE
  int process_increment_append(ObTableExecCtx &ctx,
                               const ObTableQueryAndMutateRequest &req,
                               ObTableQueryAndMutateResult &res,
                               ObHbaseColumnFamilyService &cf_service);
  int process_check_and_mutate(ObTableExecCtx &ctx,
                               const ObTableQueryAndMutateRequest &req,
                               ObTableQueryAndMutateResult &res,
                               ObHbaseColumnFamilyService &cf_service);
  
  int replace_timestamp(ObTableExecCtx &ctx, ObTableLSOpRequest &req);
  int replace_timestamp(ObTableExecCtx &ctx, ObTableQueryAndMutateRequest &req);
  int calc_tablets(ObTableExecCtx &ctx, const ObTableQueryAndMutateRequest &req);
  int lock_rows(ObTableExecCtx &ctx, const ObTableLSOpRequest &req);
  int lock_rows(ObTableExecCtx &ctx, const ObTableQueryAndMutateRequest &req);

  // INCREMENT/APPEND
  int check_passed(ObTableExecCtx &ctx,
                   ObTableQueryResult *wide_rows,
                   ObHbaseQueryResultIterator *result_iter,
                   bool &passed);

  int generate_new_incr_append_table_cells(ObTableExecCtx &ctx,
                                           const ObTableQueryAndMutateRequest &req,
                                           ObTableQueryAndMutateResult &result,
                                           ObTableQueryResult *wide_rows,
                                           ObHbaseTableCells &table_cells);
  int sort_qualifier(common::ObIArray<std::pair<common::ObString, int32_t>> &columns,
                     const ObTableBatchOperation &mutations);
  int generate_new_value(ObTableExecCtx &ctx,
                         const ObTableQueryAndMutateRequest &req,
                         const ObITableEntity *old_entity,
                         const ObITableEntity &src_entity,
                         bool is_increment,
                         ObTableEntity &new_entity,
                         ObTableQueryAndMutateResult &result);
  int add_query_columns(ObTableExecCtx &ctx, ObTableQueryResult &res);
  int add_to_results(const ObTableEntity &new_entity,
                     const ObTableQueryAndMutateRequest &req,
                     ObTableQueryAndMutateResult &result);

  int check_expected_value_is_null(ObTableExecCtx &ctx, ObHbaseQueryResultIterator *result_iter, bool &is_null);
  int check_result_value_is_null(ObTableExecCtx &ctx, ObTableQueryResult *wide_rows, bool &is_null);

private:
  // for async query
  virtual int get_query_session(uint64_t sessid,
                                const ObQueryOperationType query_type,
                                ObTableNewQueryAsyncSession *&query_session) override;
  int prepare_ls_result(const ObTableLSOp &req, ObTableLSOpResult &res);
  int construct_del_query(ObHbaseTableCells &table_cells, 
                          ObTableExecCtx &exec_ctx, 
                          ObHbaseColumnFamilyService &cf_service,
                          ObHbaseQuery &query);
  int add_dict_and_bm_to_result_entity(const ObTableLSOp &ls_op,
                                       ObTableLSOpResult &ls_result,
                                       const ObTableSingleOpEntity &req_entity,
                                       ObTableSingleOpEntity &result_entity);
  int check_mode_defense(ObTableExecCtx &ctx);
  int check_ls_op_defense(ObTableExecCtx &ctx, const ObTableLSOpRequest &req);
  int check_mode_defense(ObTableExecCtx &ctx, const ObTableQueryRequest &req);
  bool compare_part_key(ObITableEntity &first_entity, ObITableEntity &second_entity, bool is_secondary_part);
  int check_is_same_part_key(ObTableExecCtx &ctx, const ObTableLSOpRequest &req, bool &is_same);
  int init_put_request_result(ObTableExecCtx &ctx, ObTableLSOpRequest &req, ObTableLSOpResult &res);
private:
  static const uint64_t INVALID_SESSION_ID = 0;
  uint64_t query_session_id_;
  int64_t timeout_ts_;
  bool is_multi_cf_req_;
  union {
    uint64_t flags_;
    struct {
      uint64_t is_full_table_scan_           : 1;
      uint64_t is_tablegroup_req_            : 1;
      uint64_t reserved_                     : 62;
    };
  };
};

} // end of namespace table
} // end of namespace oceanbase

#endif /* OCEANBASE_OBSERVER_TABLE_MODELS_OB_HBASE_MODEL_H_ */
