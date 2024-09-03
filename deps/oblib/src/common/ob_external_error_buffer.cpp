#include "common/ob_external_error_buffer.h"
namespace oceanbase
{
namespace common
{
  // this is only for demo and should be removed after session method is done
  ObExternalErrorBuffer::ExternalErrorWrapper ObExternalErrorBuffer::external_error_wrapper = {ObString(""),ObString(""),ObString(""),-1,ObString("")};
  char* ObExternalErrorBuffer::filename = new char[MAX_EXYERNAL_ERROR_LENGTH];
  char* ObExternalErrorBuffer::data = new char[MAX_EXYERNAL_ERROR_LENGTH];
  char* ObExternalErrorBuffer::colname = new char[MAX_EXYERNAL_ERROR_LENGTH];
}
}
