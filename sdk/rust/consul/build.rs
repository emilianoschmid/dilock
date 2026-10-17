use std::env;

fn main() {
    println!("cargo:rerun-if-env-changed=DILOCK_C_LIB_DIR");
    if let Ok(path) = env::var("DILOCK_C_LIB_DIR") {
        println!("cargo:rustc-link-search=native={path}");
    }
    println!("cargo:rustc-link-lib=dylib=dilock_c");
}
