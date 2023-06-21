#include "mds_writer.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

MdsWriter::MdsWriter(const WriterType writer_type, const int64_t writer_id) : writer_type_(writer_type), writer_id_(writer_id) {}

MdsWriter::MdsWriter(const transaction::ObTransID &tx_id) : writer_type_(WriterType::TRANSACTION), writer_id_(tx_id.get_id()) {}

bool MdsWriter::is_valid() const
{
  return writer_type_ != WriterType::UNKNOWN_WRITER && writer_type_ < WriterType::END;
}

}
}
}