module dilock.example/go-redis

go 1.26.0

require dilock v0.0.0

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/redis/go-redis/v9 v9.21.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)

replace dilock => ../../../sdk/go
