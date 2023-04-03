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
// method
#define createBinlogRecord createLogRecord

} // namespace liboblog
} // namespace oceanbase
#endif /* OCEANBASE_CDC_MSG_CONVERT_H__ */
