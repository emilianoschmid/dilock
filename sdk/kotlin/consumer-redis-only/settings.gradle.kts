pluginManagement { repositories { gradlePluginPortal(); mavenCentral() } }
dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    val repo = providers.gradleProperty("dilockMavenRepo").orNull ?: error("missing local repo")
    repositories {
        maven { url = uri(repo); metadataSources { mavenPom(); artifact() } }
        mavenCentral()
    }
}
rootProject.name = "dilock-kotlin-redis-consumer"
