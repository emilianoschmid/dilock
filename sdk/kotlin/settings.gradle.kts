pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        providers.gradleProperty("dilockLocalMavenRepo").orNull?.let { repository ->
            maven { url = uri(repository) }
        }
        mavenCentral()
        maven { url = uri("../../.tools/m2repo") }
    }
}

rootProject.name = "dilock-kotlin"
include(":worker")
