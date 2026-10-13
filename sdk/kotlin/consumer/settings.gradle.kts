pluginManagement {
    repositories { gradlePluginPortal(); mavenCentral() }
}
dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    val dilockMavenRepo = providers.gradleProperty("dilockMavenRepo").orNull
        ?: error("Pass -PdilockMavenRepo=<local Maven repository containing Dilock artifacts>")
    repositories {
        maven {
            url = uri(dilockMavenRepo)
            // Exercise the Maven POM contract instead of Gradle-specific module metadata.
            metadataSources { mavenPom(); artifact() }
        }
        mavenCentral()
    }
}
rootProject.name = "dilock-kotlin-local-artifact-consumer"
