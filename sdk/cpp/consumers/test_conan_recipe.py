import ast
from pathlib import Path


RECIPE = Path(__file__).resolve().parents[1] / "conanfile.py"


def _recipe_class() -> ast.ClassDef:
    tree = ast.parse(RECIPE.read_text(encoding="utf-8"))
    return next(
        node for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "DilockCppConan"
    )


def _method(recipe: ast.ClassDef, name: str) -> ast.FunctionDef:
    return next(
        node for node in recipe.body
        if isinstance(node, ast.FunctionDef) and node.name == name
    )


def _requires(method: ast.FunctionDef) -> list[str]:
    return [
        ast.literal_eval(node.args[0])
        for node in ast.walk(method)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "requires"
    ]


def test_conan_redis_profile_omits_etcd_dependencies() -> None:
    recipe = _recipe_class()
    options = ast.literal_eval(next(node.value for node in recipe.body if isinstance(node, ast.Assign)
                                    and any(isinstance(target, ast.Name) and target.id == "options"
                                            for target in node.targets)))
    defaults = ast.literal_eval(next(node.value for node in recipe.body if isinstance(node, ast.Assign)
                                     and any(isinstance(target, ast.Name) and target.id == "default_options"
                                             for target in node.targets)))
    requirements = _method(recipe, "requirements")
    etcd_branch = next(
        node for node in ast.walk(requirements)
        if isinstance(node, ast.If) and ast.unparse(node.test) == "self.options.with_etcd"
    )
    always_required = [
        ast.literal_eval(node.args[0])
        for node in ast.walk(requirements)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "requires"
        and node not in ast.walk(etcd_branch)
    ]

    assert options == {"with_etcd": [True, False], "shared": [True, False]}
    assert defaults == {"with_etcd": False, "shared": False}
    assert always_required == [
        "redis-plus-plus/1.3.15", "openssl/3.5.8", "nlohmann_json/3.12.0",
    ]
    assert _requires(etcd_branch) == ["protobuf/3.21.12", "grpc/1.54.3"]


def test_conan_recipe_configures_and_exports_the_selected_profiles() -> None:
    recipe = _recipe_class()
    generate = ast.unparse(_method(recipe, "generate"))
    package_info_method = _method(recipe, "package_info")
    package_info = ast.unparse(package_info_method)
    package_info = package_info.replace('"', "'")
    etcd_info_branch = next(
        node for node in ast.walk(package_info_method)
        if isinstance(node, ast.If) and ast.unparse(node.test) == "self.options.with_etcd"
    )
    etcd_info = ast.unparse(etcd_info_branch).replace('"', "'")

    assert "DILOCK_BUILD_ETCD" in generate
    assert "bool(self.options.with_etcd)" in generate
    assert "self.cpp_info.components['redis']" in package_info
    assert "'Dilock::redis'" in package_info
    assert "self.cpp_info.components['etcd']" in etcd_info
    assert "'Dilock::etcd'" in etcd_info
    for dependency in (
        "openssl::crypto", "openssl::ssl", "nlohmann_json::nlohmann_json",
        "protobuf::libprotobuf", "grpc::grpc++",
    ):
        assert dependency in etcd_info
