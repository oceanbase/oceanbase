#include <iostream>
#include <dlfcn.h>

using namespace std;

#define LOG(msg) \
    do { \
      std::cout << msg << std::endl; \
    } while (0)

int main(int argc, char **argv)
{
  const char* lib_path = "lib/libobcdc.so";
  void *libobcdc = dlopen(lib_path, RTLD_LOCAL | RTLD_LAZY);
  char *errstr;
  errstr = dlerror();
  if (errstr != NULL) {
    LOG("[ERROR][DL]A dynamic linking error occurred");
    LOG(errstr);
    return 1;
  }
  dlclose(libobcdc);
  return 0;
}
