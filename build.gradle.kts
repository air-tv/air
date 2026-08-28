import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.jetbrains.kotlin.gradle.ExperimentalWasmDsl
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.multiplatform)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.android.library)
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
        }
        commonTest.dependencies {
            implementation(kotlin("test"))
            implementation(libs.kotlinx.coroutines.test)
        }
        wasmJsMain.dependencies { implementation(libs.kotlinx.browser) }
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
