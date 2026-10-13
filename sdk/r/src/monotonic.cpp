#include <R.h>
#include <Rinternals.h>
#include <R_ext/Rdynload.h>

#include <chrono>

extern "C" SEXP dilock_steady_time() {
  using Clock = std::chrono::steady_clock;
  const auto now = Clock::now().time_since_epoch();
  return Rf_ScalarReal(std::chrono::duration<double>(now).count());
}

static const R_CallMethodDef call_methods[] = {
  {"dilock_steady_time", reinterpret_cast<DL_FUNC>(&dilock_steady_time), 0},
  {nullptr, nullptr, 0}
};

extern "C" void R_init_dilock(DllInfo *dll) {
  R_registerRoutines(dll, nullptr, call_methods, nullptr, nullptr);
  R_useDynamicSymbols(dll, FALSE);
}
