plugins {
    kotlin("jvm") version "2.3.20"
    application
}
kotlin { jvmToolchain(21) }
dependencies {
    implementation("org.dilock:dilock-kotlin:0.1.0")
    implementation("org.dilock:dilock-redis:0.1.0")
}
application { mainClass.set("example.RedisConsumerKt") }
