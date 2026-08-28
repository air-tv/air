import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.jetbrains.kotlin.gradle.ExperimentalWasmDsl
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.android.library)
    alias(libs.plugins.sqldelight)
}

group = "com.getair"
version = "0.1.0-SNAPSHOT"

@OptIn(ExperimentalWasmDsl::class)
kotlin {
    jvmToolchain(17)
    androidTarget { compilerOptions.jvmTarget.set(JvmTarget.JVM_17) }
    jvm { compilerOptions.jvmTarget.set(JvmTarget.JVM_17) }
    linuxX64()
    mingwX64 {
        compilations.getByName("main") {
            cinterops.create("wincred")
        }
    }
    macosX64()
    macosArm64()
    iosX64()
    iosArm64()
    iosSimulatorArm64()
    js(IR) { browser(); nodejs() }
    wasmJs { browser(); nodejs() }

    sourceSets {
        commonMain.dependencies {
            api(libs.getair.iptv)
            api(libs.getair.stremio)
            api(libs.getair.video)
            api(libs.kotlinx.coroutines.core)
            api(libs.kotlinx.datetime)
            implementation(libs.kotlinx.serialization.json)
            implementation(libs.sqldelight.runtime)
        }
        commonTest.dependencies {
            implementation(kotlin("test"))
            implementation(libs.kotlinx.coroutines.test)
        }
        wasmJsMain.dependencies { implementation(libs.kotlinx.browser) }
        androidMain.dependencies { implementation(libs.sqldelight.android.driver) }
        jvmMain.dependencies {
            implementation(libs.sqldelight.sqlite.driver)
            runtimeOnly(libs.sqlite.jdbc)
        }
        nativeMain.dependencies { implementation(libs.sqldelight.native.driver) }
    }
}

sqldelight {
    databases {
        create("AirCatalogDatabase") {
            packageName.set("com.getair.core.catalog.db")
            srcDirs.setFrom("src/commonMain/sqldelight")
            schemaOutputDirectory.set(file("src/commonMain/sqldelight/databases"))
            verifyMigrations.set(true)
        }
    }
}

android {
    namespace = "com.getair.core"
    compileSdk = 36
    defaultConfig { minSdk = 24 }
}

tasks.withType<Test>().configureEach {
    testLogging {
        events("failed")
        exceptionFormat = TestExceptionFormat.FULL
        showCauses = true
        showStackTraces = true
    }
}

val jvmCatalogRuntime = configurations.named("jvmRuntimeClasspath")

tasks.register("assertJvmCatalogDependencies") {
    group = "verification"
    description = "Asserts the catalog store's effective JVM persistence dependency versions."
    inputs.files(jvmCatalogRuntime)
    doLast {
        val artifacts = inputs.files.files.map { it.name }.toSet()
        check("runtime-jvm-2.1.0.jar" in artifacts) {
            "Expected SQLDelight runtime 2.1.0"
        }
        check("sqlite-driver-2.1.0.jar" in artifacts) {
            "Expected SQLDelight sqlite-driver 2.1.0"
        }
        check("sqlite-jdbc-3.51.3.0.jar" in artifacts) {
            "Expected effective Xerial SQLite JDBC 3.51.3.0"
        }
        check("kotlin-stdlib-2.1.10.jar" in artifacts) {
            "Expected Kotlin stdlib 2.1.10"
        }
    }
}

tasks.configureEach {
    if (
        name == "verifyCommonMainAirCatalogDatabaseMigration" ||
        name == "generateCommonMainAirCatalogDatabaseInterface"
    ) {
        mustRunAfter("generateCommonMainAirCatalogDatabaseSchema")
    }
}
