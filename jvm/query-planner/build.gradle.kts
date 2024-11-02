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
    implementation(project(":datasource"))
    implementation(project(":logical-plan"))
    implementation(project(":physical-plan"))

    implementation("org.apache.arrow:arrow-vector:0.17.0")

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}