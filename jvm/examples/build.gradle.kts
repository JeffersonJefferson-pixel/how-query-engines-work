plugins {
    id("java")
}

group = "org.example.kquery"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation(project(":execution"))
    implementation(project(":logical-plan"))
    implementation(project(":datatypes"))
    implementation(project(":optimizer"))

    implementation("org.apache.arrow:arrow-vector:0.17.0")

    testImplementation(platform("org.junit:junit-bom:5.10.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}