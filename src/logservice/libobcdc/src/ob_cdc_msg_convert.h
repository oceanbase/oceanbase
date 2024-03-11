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

#ifndef OCEANBASE_CDC_MSG_CONVERT_H__
#define OCEANBASE_CDC_MSG_CONVERT_H__

#include <oblogmsg/LogRecord.h>
#include <oblogmsg/LogMsgFactory.h>
#include <oblogmsg/MetaInfo.h>
#include <oblogmsg/StrArray.h>

using namespace oceanbase::logmessage;
namespace oceanbase
{
namespace liboblog
{
// class
#define DRCMessageFactory oceanbase::logmessage::LogMsgFactory
#define IBinlogRecord oceanbase::logmessage::ILogRecord
#define BinlogRecordImpl oceanbase::logmessage::LogRecordImpl
#define RecordType oceanbase::logmessage::RecordType
#define IDBMeta oceanbase::logmessage::IDBMeta
#define ITableMeta oceanbase::logmessage::ITableMeta
#define IColMeta oceanbase::logmessage::IColMeta
#define binlogBuf oceanbase::logmessage::BinLogBuf
#define IStrArray oceanbase::logmessage::StrArray
#define drcmsg_field_types oceanbase::logmessage::logmsg_field_types
#define DRCMSG_TYPE_ORA_BINARY_FLOAT LOGMSG_TYPE_ORA_BINARY_FLOAT
#define DRCMSG_TYPE_ORA_BINARY_DOUBLE LOGMSG_TYPE_ORA_BINARY_DOUBLE
#define DRCMSG_TYPE_ORA_XML LOGMSG_TYPE_ORA_XML
// method
#define createBinlogRecord createLogRecord

} // namespace liboblog
} // namespace oceanbase
#endif /* OCEANBASE_CDC_MSG_CONVERT_H__ */
