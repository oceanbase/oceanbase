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

#ifndef OCEANBASE_TABLE_OB_INDEX_HNSW_ANN_OP_H_
#define OCEANBASE_TABLE_OB_INDEX_HNSW_ANN_OP_H_

#include "sql/engine/ob_operator.h"
#include "share/vector_index/ob_hnsw_index_builder.h"

namespace oceanbase
{
namespace sql
{

class ObIndexHnswAnnOp : public ObOperator
{
public:
    ObIndexHnswAnnOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input);
    virtual ~ObIndexHnswAnnOp() {}

    int open_inner_conn();
    int close_inner_conn();
protected:
    virtual int inner_open();
    virtual int inner_close();
    virtual int inner_rescan() override;
    virtual int inner_get_next_row() override;
protected:
    common::ObMySQLProxy *sql_proxy_;
    observer::ObInnerSQLConnection *inner_conn_;
    uint64_t tenant_id_;

    share::ObHNSWIndexBuilder builder_;
};

}
}

#endif