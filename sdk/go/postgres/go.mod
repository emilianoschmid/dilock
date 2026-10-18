module example.invalid/dilock/postgres

go 1.26.0

require (
    example.invalid/dilock/core v0.1.0
    github.com/jackc/pgx/v5 v5.7.2
)

replace example.invalid/dilock/core v0.1.0 => ..
