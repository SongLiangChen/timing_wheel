# timing_wheel
时间轮算法go语言实现

### 用例

```go
package main

import (
    "fmt"
    "time"

    "github.com/SongLiangChen/timing_wheel"
)

func timeout(a interface{}) {
    fmt.Println(a)
}

func main() {
    t := timing_wheel.New(time.Second)
    go t.Start()
    defer t.Stop()

    t.NewTimer(time.Second*2, timeout, "hello, timing-wheel")
    time.Sleep(time.Second * 4)
}

```
