#ifndef OCEANBASE_LIB_OBLOG_OB_EXTERNAL_ERROR_BUFFER_
#define OCEANBASE_LIB_OBLOG_OB_EXTERNAL_ERROR_BUFFER_

#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_array.h"
#include "string.h"
#define MAX_EXYERNAL_ERROR_LENGTH 1024
namespace oceanbase
{
namespace common
{
class ObExternalErrorBuffer
{
public:
  struct ExternalErrorWrapper {
    ObString error_description;
    ObString error_file_name;
    ObString error_data;
    int error_row_number;
    ObString error_column_name;
  };
  
  

  // ObExternalErrorBuffer(int error_row_id, int error_column_id, int error_code, ObString err_file_name, ObString err_column_name, ObString err_data){
  //   this->error_row_id = error_row_id;
  //   this->error_column_id = error_column_id;
  //   this->error_code = error_code;
  //   this->err_file_name = err_file_name;
  //   this->err_column_name = err_column_name;
  //   this->err_data = err_data;
  //   ;
  // }
  ObExternalErrorBuffer(common::ObIAllocator &allocator) : error_row_id(-1), error_column_id(-1), error_code(OB_SUCCESS), allocator_(allocator) {
    errorfilename = static_cast<char*>(allocator_.alloc(sizeof(char)*MAX_EXYERNAL_ERROR_LENGTH));
    errordata = static_cast<char*>(allocator_.alloc(sizeof(char)*MAX_EXYERNAL_ERROR_LENGTH));
    errorcolname = static_cast<char*>(allocator_.alloc(sizeof(char)*MAX_EXYERNAL_ERROR_LENGTH));
    desc = ObString::make_string("type conversion failed");
  };
  ~ObExternalErrorBuffer(){};
  void reset() {
    error_row_id = -1;
    error_column_id = -1;
    error_code = OB_SUCCESS;
    err_file_name.reset();
    err_column_name.reset();
    err_data.reset();
  }
  void set_err_row(int err_row){ error_row_id = err_row;}
  void set_err_col(int err_col){ error_column_id = err_col; }
  void set_err_code(int err_code){ error_code = err_code; }
  void set_err_file_name(ObString err_file_name){ memcpy(errorfilename, err_file_name.ptr(), err_file_name.length()); this->err_file_name = ObString(err_file_name.length(), errorfilename);}
  void set_err_column_name(ObString err_column_name){ memcpy(errorcolname, err_column_name.ptr(), err_column_name.length()); this->err_column_name = ObString(err_column_name.length(), errorcolname); }
  void set_err_data(ObString err_data){ memcpy(errordata, err_data.ptr(), err_data.length()); this->err_data = ObString(err_data.length(), errordata); }
  int get_err_row(){ return error_row_id; }
  int get_err_col(){ return error_column_id; }
  int get_err_code(){ return error_code; }
  ObString get_err_data(){ return err_data; }
  ObString get_err_file_name(){ return err_file_name; }
  ObString get_err_column_name() { return err_column_name; }
  ObString get_err_desc() { return desc; }
public:
  static void get_ext_wrapper(ExternalErrorWrapper &ext_wrapper){
    ext_wrapper = external_error_wrapper;
  } 
  static void set_ext_wrapper_filename(ObString s){
    uint64_t length = s.length();
    if (length > MAX_EXYERNAL_ERROR_LENGTH){
      length = MAX_EXYERNAL_ERROR_LENGTH;
    }
    std::memcpy(filename, s.ptr(), length);
    external_error_wrapper.error_file_name = ObString(length,filename);
  }
  static void set_ext_wrapper_desc(ObString s) {
    external_error_wrapper.error_description = s;
  }
  static void set_ext_wrapper_data(ObString s) {
    uint64_t length = s.length();
    if (length > MAX_EXYERNAL_ERROR_LENGTH){
      length = MAX_EXYERNAL_ERROR_LENGTH;
    }
    std::memcpy(data, s.ptr(), length);
    external_error_wrapper.error_data = ObString(length, data);
  }
  static void set_ext_wrapper_row(int row) {
    external_error_wrapper.error_row_number = row;
  }
  static void set_ext_wrapper_colname(ObString s){
    uint64_t length = s.length();
    if (length > MAX_EXYERNAL_ERROR_LENGTH){
      length = MAX_EXYERNAL_ERROR_LENGTH;
    }
    std::memcpy(colname, s.ptr(), length);
    external_error_wrapper.error_column_name = ObString(length, colname);
  }
  static void set_ext_wrapper(ExternalErrorWrapper ext_wrapper) {
    external_error_wrapper = ext_wrapper;
  }
private:
  
  int error_row_id;
  int error_column_id;
  int error_code;
  ObString err_file_name;
  ObString err_column_name;
  ObString err_data;
  ObString desc;
  char* errorfilename;
  char* errordata;
  char* errorcolname;
  static ExternalErrorWrapper external_error_wrapper;
  static char* filename;
  static char* data;
  static char* colname;
  common::ObIAllocator &allocator_;


};

}
}



#endif