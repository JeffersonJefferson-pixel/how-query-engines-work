plugins {
    kotlin("jvm") version "1.8.20"
    application
}

group = "org.example.kquery"
version = "1.0-SNAPSHOT"

allprojects {
    repositories {
        mavenLocal()
        mavenCentral()
    }
}

subprojects {
    apply {
        plugin("org.jetbrains.kotlin.jvm")
    }

    dependencies {
        testImplementation(kotlin("test"))
    }
}

tasks.test {
    useJUnitPlatform()
}


kotlin {
    jvmToolchain(11)
}

application {
    mainClass.set("MainKt")
}