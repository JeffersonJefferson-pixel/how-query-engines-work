plugins {
    kotlin("jvm")
}

group = "org.example.kquery"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":datatypes"))
    implementation(project(":logical-plan"))

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}