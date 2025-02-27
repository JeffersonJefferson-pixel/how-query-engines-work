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
    implementation(project(":optimizer"))
    implementation(project(":query-planner"))
    implementation(project(":physical-plan"))

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}