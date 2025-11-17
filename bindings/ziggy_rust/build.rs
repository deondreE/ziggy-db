use std::env;

fn main() {
    let lib_dir = "target/debug";

    println!("cargo:rustc-link-search=native={lib_dir}");

    println!("cargo:rustc-link-lib=dylib=ziggy_bindings");

    if cfg!(target_os = "windows") {
        let path = env::var("PATH").unwrap_or_default();
        println!("cargo:warning=Remember to copy ziggy_bindings.dll into {lib_dir} or add it to PATH");
        println!("cargo:rustc-env=PATH={lib_dir};{path}");
    }
}