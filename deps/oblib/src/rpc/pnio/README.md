# pkt-nio

## API
see interface/group.h

## test
```
make test/test-group
test/test-group
```

## example
```
#define rk_log_macro(level, format, ...) _OB_LOG(level, "PNIO " format, ##__VA_ARGS__)
#include "interface/group.h"
...
#include "interface/pkt-nio.c"
```
