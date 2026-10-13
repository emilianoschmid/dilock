plugins {
    kotlin("jvm")
    application
}

dependencies {
    implementation(project(":"))
    implementation("org.dilock:dilock-redis:0.1.0")
    implementation("org.dilock:dilock-etcd:0.1.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.21.2")
    testImplementation(kotlin("test-junit5"))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.10.1")
}

application { mainClass.set("org.dilock.kotlin.worker.ConformanceWorkerKt") }
tasks.test {
    useJUnitPlatform()
    systemProperty("dilock.worker.runtimeClasspath", sourceSets.main.get().runtimeClasspath.asPath)
    // Endpoint variables opt the process tests into live Redis/etcd checks.
    // Gradle does not track environment variables automatically.
    if (providers.environmentVariable("DILOCK_TEST_REDIS_ENDPOINT").isPresent ||
        providers.environmentVariable("DILOCK_TEST_ETCD_ENDPOINT").isPresent) {
        outputs.upToDateWhen { false }
    }
}
