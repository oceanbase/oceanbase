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

#ifndef __OB_KEYHNSW_H__
#define __OB_KEYHNSW_H_

#include "lib/vector/ob_vector.h"
#include "storage/memtable/mvcc/ob_keybtree_deps.h"

namespace oceanbase
{
namespace common
{
namespace keyhnsw
{

template<typename VectorWrapper>
struct HNSWElement
{
    // TODO:(@wangmiao)
};

template<typename VectorWrapper>
class ObKeyHNSW
{
public:
    typedef int (*VectorIndexDistanceFunc) (const ObTypeVector&, const ObTypeVector&, double &distance);
private:
    size_t M_;
    size_t maxM_;
    size_t maxM0_;
    size_t ef_construction_;
    size_t ef_;

    size_t data_size_;
    VectorIndexDistanceFunc dist_fn_;
};

}
}
}

#endif