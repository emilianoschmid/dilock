test_that("draft 1 identity matches shared golden vector", {
  expect_identical(dilock_resource_id("default", "camera/123/reindex"),
                   "e0cdd484a99b6aee4162335939d2a2d128a490c097069fd85b2fc1e540a940b6")
  expect_error(dilock_resource_id("Bad", "resource"))
  expect_error(dilock_resource_id("default", ""))
})
