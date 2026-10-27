module example.invalid/dilock/postgres

go 1.26.0

require (
	example.invalid/dilock/core v0.1.0
	github.com/jackc/pgx/v5 v5.9.2
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	golang.org/x/text v0.29.0 // indirect
)

replace example.invalid/dilock/core v0.1.0 => ..
