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

    implementation("com.univocity:univocity-parsers:2.8.4")
    implementation("org.apache.arrow:arrow-vector:0.17.0")
    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}