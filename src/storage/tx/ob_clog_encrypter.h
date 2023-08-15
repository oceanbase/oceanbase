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

#ifndef OCEANBASE_TRANSACTION_OB_CLOG_ENCRYPTER_
#define OCEANBASE_TRANSACTION_OB_CLOG_ENCRYPTER_

#include "ob_clog_encrypt_info.h"

namespace oceanbase
{
namespace transaction
{

#ifdef OB_BUILD_TDE_SECURITY
class ObTransMutator;

class ObCLogEncrypter
{
public:
    // static int decrypt_log_mutator(const common::ObPartitionKey &pkey, ObTransMutator &mutator,
    //                 const ObCLogEncryptInfo &encrypt_info, ObTransMutator &new_mutator,
    //                 const bool need_extract_encrypt_meta, share::ObEncryptMeta &final_encrypt_meta,
    //                 share::ObCLogEncryptStatMap &encrypt_stat_map);
    // static int archive_encrypt_log_mutator(const common::ObPartitionKey &pkey, ObTransMutator &mutator,
    //                 const ObCLogEncryptInfo &encrypt_info, ObTransMutator &new_mutator);
};
#endif

}
}

#endif
