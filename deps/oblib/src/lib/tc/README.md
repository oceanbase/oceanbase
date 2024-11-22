# obtc
traffic control for OB. `man tc` if you do not know traffic control.

本项目是一个用户态库，目的是仿照tc的思想，通过提供各种队列, 并允许用户拼接这些队列实现灵活的流控，并不是真的和tc对标。

当然目前只支持三种队列:
1. BufferQueue: 就是fifo队列。
2. WeightedQueue: 支持权重的队列。
3. QDiscRoot: 本质是个WeightedQueue，只是新增了timer, 另外封装了refresh逻辑。

## quick start
copy `obtc` to where you like, configure include path properlly, add ob-tc.cpp to your build list.

use `test/test-tc.cpp` as a demo.

## test notes
test-plimit2: tt1和tt2权重是1:2, 同时tt1有一个较小的limit，tt2下面有一个group受同样的limit，另两个group不受limit，tt2下面受limit的group会被饿死。
