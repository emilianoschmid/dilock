plugins {
    kotlin("jvm") version "2.3.20"
    application
}

kotlin { jvmToolchain(21) }

dependencies {
    implementation("org.dilock:dilock-kotlin:0.1.0")
    // Backend adapters stay optional in the facade POM and are selected by consumers.
    implementation("org.dilock:dilock-redis:0.1.0")
    implementation("org.dilock:dilock-etcd:0.1.0")
}

application { mainClass.set("example.ConsumerSmokeKt") }
