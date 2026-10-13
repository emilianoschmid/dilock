.dilock_monotonic <- function() {
  .Call("dilock_steady_time", PACKAGE = "dilock")
}
