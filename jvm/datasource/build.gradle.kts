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

    implementation("org.apache.hadoop:hadoop-common:3.1.0")
    implementation("org.apache.parquet:parquet-arrow:1.11.0")
    implementation("org.apache.parquet:parquet-common:1.11.0")
    implementation("org.apache.parquet:parquet-column:1.11.0")
    implementation("org.apache.parquet:parquet-hadoop:1.11.0")

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}