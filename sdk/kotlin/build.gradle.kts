buildscript {
    if (project.hasProperty("withDokka")) {
        repositories { mavenCentral() }
        dependencies { classpath("org.jetbrains.dokka:dokka-gradle-plugin:2.0.0") }
    }
}

plugins {
    kotlin("jvm") version "2.3.20"
    `maven-publish`
}

if (providers.gradleProperty("withDokka").isPresent) apply(plugin = "org.jetbrains.dokka")

group = "org.dilock"
version = "0.1.0"

dependencies {
    api("org.dilock:dilock-core:0.1.0")
    compileOnly("org.dilock:dilock-redis:0.1.0") {
        exclude(group = "redis.clients", module = "jedis")
        exclude(group = "com.fasterxml.jackson.core", module = "jackson-databind")
    }
    compileOnly("org.dilock:dilock-consul:0.1.0") { exclude(group = "com.fasterxml.jackson.core", module = "jackson-databind") }
    compileOnly("org.dilock:dilock-postgres:0.1.0")
    compileOnly("org.dilock:dilock-etcd:0.1.0") {
        exclude(group = "io.etcd", module = "jetcd-grpc")
        exclude(group = "com.fasterxml.jackson.core", module = "jackson-databind")
    }
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    testImplementation("org.dilock:dilock-redis:0.1.0") {
        exclude(group = "redis.clients", module = "jedis")
        exclude(group = "com.fasterxml.jackson.core", module = "jackson-databind")
    }
    testImplementation("org.dilock:dilock-consul:0.1.0")
    testImplementation("org.dilock:dilock-postgres:0.1.0")
    testImplementation("org.dilock:dilock-etcd:0.1.0") {
        exclude(group = "io.etcd", module = "jetcd-grpc")
        exclude(group = "com.fasterxml.jackson.core", module = "jackson-databind")
    }
    testImplementation("io.etcd:jetcd-grpc:0.8.7") {
        exclude(group = "io.vertx", module = "vertx-grpc")
    }
    testImplementation("redis.clients:jedis:8.0.1")
    testImplementation("com.fasterxml.jackson.core:jackson-databind:2.21.2")
    testImplementation(kotlin("test-junit5"))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.10.1")
}

kotlin {
    jvmToolchain(21)
    compilerOptions {
        allWarningsAsErrors.set(true)
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}

java { withSourcesJar() }

val liveBackendEndpointsConfigured = providers.environmentVariable("DILOCK_TEST_REDIS_ENDPOINT").isPresent ||
    providers.environmentVariable("DILOCK_TEST_ETCD_ENDPOINT").isPresent ||
    providers.environmentVariable("DILOCK_TEST_CONSUL_ENDPOINT").isPresent ||
    providers.environmentVariable("DILOCK_POSTGRES_DSN").isPresent

tasks.test {
    useJUnitPlatform()
    // Gradle does not infer environment-variable inputs. Never reuse a skipped
    // or stale result when a live backend endpoint opts this task into integration tests.
    if (liveBackendEndpointsConfigured) outputs.upToDateWhen { false }
}

// A caller-supplied filesystem path enables isolated local artifact consumers.
// There is deliberately no configured remote publication repository.
val localMavenRepository = providers.gradleProperty("dilockMavenRepo").orNull

publishing {
    repositories {
        if (localMavenRepository != null) {
            maven {
                name = "localValidation"
                url = uri(file(localMavenRepository))
            }
        }
    }
    publications {
        create<MavenPublication>("mavenJava") { from(components["java"]) }
    }
}
