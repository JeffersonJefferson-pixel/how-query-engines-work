plugins {
    kotlin("jvm")
}

group = "org.example.kquery"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":logical-plan"))
    implementation(project(":datatypes"))
    implementation(project(":datasource"))

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}