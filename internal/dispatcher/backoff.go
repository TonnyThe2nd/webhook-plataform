package dispatcher

import "time"

func Backoff(attempt int, base, cap time.Duration) time.Duration {
	if base <= 0 || cap <= 0 {
		return 0
	}
	if base >= cap {
		return cap
	}
	delay := base
	for i := 1; i < attempt; i++ {
		if delay >= cap-delay {
			return cap
		}
		delay *= 2
	}
	return delay
}
