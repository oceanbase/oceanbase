/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
