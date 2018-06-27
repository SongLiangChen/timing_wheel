package timing_wheel

import (
	"container/list"
	"sync"
	"time"
)

// the timing-wheel algorithm coding with golang
// referer http://www.cnblogs.com/zhongwencool/p/timing_wheel.html?utm_source=tuicool&utm_medium=referral
// referer https://github.com/cloudwu/skynet/blob/master/skynet-src/skynet_timer.c

const (
	TIME_NEAR_SHIFT = 8
	TIME_NEAR       = 1 << TIME_NEAR_SHIFT // 256  2^8
	TIME_NEAR_MASK  = TIME_NEAR - 1        // 255

	TIME_LEVEL_SHIFT = 6
	TIME_LEVEL       = 1 << TIME_LEVEL_SHIFT // 64  2^6
	TIME_LEVEL_MASK  = TIME_LEVEL - 1        // 63
)

// Timer's bucket:
//
// ____  ____  ____  ____  ____
// |  |  |  |  |  |  |  |  |  |
// |  |  |  |  |  |  |  |  |  |
// |  |  |  |  |  |  |  |  |  |
// |  |  |  |  |  |  |  |  |  |
// ----  ----  ----  ----  |  |
//  64    64    64    64   |  |
//                         |  |
//                         |  |
//                         ----
//                          256
//
// 从右往左，第一个bucket中一个刻度代表一个jeffies单位
// 第二个bucket中一个刻度代表2^8个jeffies单位
// 第三个bucket中一个刻度代表2^8^6个jeffies单位
// 第四个第五个类推，一共2^32个jeffies单位，这是时间轮所能处理的最大长度

type Timer struct {
	near [TIME_NEAR]*list.List     // 最右边的bucket
	t    [4][TIME_LEVEL]*list.List // 0-3分表代表从右到左的bucket

	time       uint32        // 当前时间
	tick       time.Duration // 一个jeffies单位
	quit       chan struct{} // 结束信号
	sync.Mutex               // 互斥锁
}

// Node 一个时间任务节点
type Node struct {
	expire uint32            // 任务到期时间
	f      func(interface{}) // 任务函数
	a      interface{}       // 任务参数
}

// New 创建一个的时间轮，参数d代表jiffies单位
func New(d time.Duration) *Timer {
	t := new(Timer)
	t.time = 0
	t.tick = d
	t.quit = make(chan struct{})

	var i, j int
	for i = 0; i < TIME_NEAR; i++ {
		t.near[i] = list.New()
	}

	for i = 0; i < 4; i++ {
		for j = 0; j < TIME_LEVEL; j++ {
			t.t[i][j] = list.New()
		}
	}

	return t
}

// NewTimer 创建一个新的时间任务
func (t *Timer) NewTimer(d time.Duration, f func(interface{}), a interface{}) *list.Element {
	n := new(Node)
	n.f = f
	n.a = a
	t.Lock()
	n.expire = uint32(d/t.tick) + t.time
	e := t.addNode(n)
	t.Unlock()
	return e
}

// StopTimer 取消时间任务
func (t *Timer) StopTimer(e *list.Element) {
	t.Lock()
	n := e.Value.(*Node)
	branch := t.getBranch(n, t.time)
	if branch != nil {
		branch.Remove(e)
	}
	t.Unlock()
}

func (t *Timer) addNode(n *Node) *list.Element {
	branch := t.getBranch(n, t.time)
	if branch != nil {
		return branch.PushBack(n)
	}
	return nil
}

// getBranch 获取任务节点适合挂载的链表
func (t *Timer) getBranch(n *Node, time uint32) *list.List {
	expire := n.expire
	current := time

	// 先判断是否挂载在256刻度的bucket
	// 判断是否挂载到某一个bucket用二进制与操作来判断
	// 例如
	// 当前时间为10，到期时间为15，那么10|255 == 15|255，换言之，任何在255范围内的数值，和255相或都会等与255
	// 再例如当前时间为10，到期时间为300，那么10|255肯定不等于300|255了，所以将被或数向左偏移6位，移动到下一个bucket
	// 这时10|2^14==300|2^14，可以确定这个任务应该挂载到第二个bucket中
	// 确定了bucket，再来看具体挂载在哪个刻度上
	// 先取出到期时间落在该bucket中的值
	// 例如256，它的二进制是10000000，应该挂载在第二个bucket上，同时它落在第二个bucket中的值只有最前面的1，后面的0000000属于第一个bucket
	// 再将1&2^6即得到具体刻度(2^6是第二个bucket的长度，如果是第一个bucket，那么就与上2^8)
	if (expire | TIME_NEAR_MASK) == (current | TIME_NEAR_MASK) {
		return t.near[expire&TIME_NEAR_MASK]
	} else {
		var i uint32
		var mask uint32 = TIME_NEAR << TIME_LEVEL_SHIFT // mask = 2^14
		for i = 0; i < 3; i++ {
			if (expire | (mask - 1)) == (current | (mask - 1)) {
				break
			}
			mask <<= TIME_LEVEL_SHIFT // mask = 2^20、2^26、2^32
		}
		return t.t[i][(expire>>(TIME_NEAR_SHIFT+i*TIME_LEVEL_SHIFT))&TIME_LEVEL_MASK]
	}
	return nil
}

// dispatchList 执行任务链表
func dispatchList(front *list.Element) {
	for e := front; e != nil; e = e.Next() {
		node := e.Value.(*Node)
		if node == nil {
			continue
		}
		go node.f(node.a)
	}
}

// moveList 清空当前链表，并将链表中的时间任务重新添加到其他链表(也可能还是本链表)
func (t *Timer) moveList(level, idx int) {
	vec := t.t[level][idx]
	front := vec.Front()
	vec.Init()
	for e := front; e != nil; e = e.Next() {
		node := e.Value.(*Node)
		t.addNode(node)
	}
}

// shift 当bucket的指针又回到刻度位0，说明上一个bucket需要进位，可以理解为秒针走了60，分钟应该走1
// 从右到左一次处理bucket中需要进位的数据
// 处理方式是从右到左判断当前bucket刻度指针是否又回到了0，是的话则处理左边一个bucket需要进位的数据
func (t *Timer) shift() {
	t.Lock()
	var mask uint32 = TIME_NEAR
	t.time++
	ct := t.time
	if ct == 0 {
		t.moveList(3, 0)
	} else {
		time := ct >> TIME_NEAR_SHIFT
		var i int = 0
		for (ct & (mask - 1)) == 0 {
			idx := int(time & TIME_LEVEL_MASK)
			if idx != 0 {
				t.moveList(i, idx)
				break
			}
			mask <<= TIME_LEVEL_SHIFT
			time >>= TIME_LEVEL_SHIFT
			i++
		}
	}
	t.Unlock()
}

// 执行时间任务
func (t *Timer) execute() {
	t.Lock()
	idx := t.time & TIME_NEAR_MASK
	vec := t.near[idx]
	if vec.Len() > 0 {
		front := vec.Front()
		vec.Init()
		t.Unlock()
		// dispatch_list don't need lock
		dispatchList(front)
		return
	}

	t.Unlock()
}

// update 更新时间轮，时间轮每一次前进一个jeffies单位，都会执行该函数
func (t *Timer) update() {
	// try to dispatch timeout 0 (rare condition)
	t.execute()

	// shift time first, and then dispatch timer message
	t.shift()

	t.execute()

}

func (t *Timer) Start() {
	tick := time.NewTicker(t.tick)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			t.update()
		case <-t.quit:
			return
		}
	}
}

func (t *Timer) Stop() {
	close(t.quit)
}
