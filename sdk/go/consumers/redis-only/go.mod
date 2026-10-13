module dilock-consumer-redis

go 1.26.0

require (
	example.invalid/dilock/core v0.1.0
	example.invalid/dilock/redis v0.1.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/redis/go-redis/v9 v9.21.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)

replace (
	example.invalid/dilock/core => ../..
	example.invalid/dilock/redis => ../../redis
)
