extern crate prost_build;

fn main() {
    prost_build::compile_protos(
        &["src/cp_model.proto", "src/sat_parameters.proto"],
        &["src/"],
    )
    .unwrap();

    if std::env::var("DOCS_RS").is_err() {
        let ortools_prefix = ortools_prefix();
        let mut build = cc::Build::new();
        build
            .cpp(true)
            .flag("-std=c++17")
            .file("src/cp_sat_wrapper.cpp")
            .include(format!("{ortools_prefix}/include"));

        for include in extra_include_paths() {
            build.include(include);
        }

        build.compile("cp_sat_wrapper.a");

        println!("cargo:rustc-link-lib=dylib=ortools");
        println!("cargo:rustc-link-lib=dylib=protobuf");
        println!("cargo:rustc-link-search=native={}/lib", ortools_prefix);
        for lib in extra_lib_paths() {
            println!("cargo:rustc-link-search=native={lib}");
        }
    }
}

fn ortools_prefix() -> String {
    if let Ok(value) = std::env::var("ORTOOLS_PREFIX") {
        return value;
    }

    let candidates = [
        "/opt/homebrew/opt/or-tools",
        "/usr/local/opt/or-tools",
        "/opt/ortools",
    ];

    for candidate in candidates {
        if std::path::Path::new(candidate).join("include").exists() {
            return candidate.to_string();
        }
    }

    "/opt/ortools".into()
}

fn extra_include_paths() -> Vec<String> {
    let mut paths = Vec::new();
    if cfg!(target_os = "macos") {
        for candidate in ["/opt/homebrew/include", "/usr/local/include"] {
            if std::path::Path::new(candidate).exists() {
                paths.push(candidate.to_string());
            }
        }
    }
    paths
}

fn extra_lib_paths() -> Vec<String> {
    let mut paths = Vec::new();
    if cfg!(target_os = "macos") {
        for candidate in ["/opt/homebrew/lib", "/usr/local/lib"] {
            if std::path::Path::new(candidate).exists() {
                paths.push(candidate.to_string());
            }
        }
    }
    paths
}
