package escheduler

import (
	"time"
)

// Debounce 限定fn在未来一段时间内最多执行一次，执行一次fn后从下次调用返回值开始重新计时
// 防抖动，非线程安全
func Debounce(d time.Duration, fn func()) func() {
	var start time.Time // 只用作打印，可删除
	var end time.Time
	var count int
	return func() {
		if time.Now().Sub(end) > 0 {
			// 初始化
			count = 0
			start = time.Now()
			end = start.Add(d)
		}
		if count == 0 {
			time.AfterFunc(time.Until(end), func() {
				fn()
			})
		}
		// AfterFunc existed, and now<=end, return directly.
		count++
	}
}
