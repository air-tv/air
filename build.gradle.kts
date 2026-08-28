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
    linuxX64 {
        providers.environmentVariable("AIR_SQLITE_LIBRARY_DIR").orNull?.let { sqliteLibraryDirectory ->
            binaries.all {
                linkerOpts(
                    "-L$sqliteLibraryDirectory",
                    "-lsqlite3",
                    "-ldl",
                    "-lpthread",
                    "-lm",
                )
            }
        }
    }
    mingwX64 {
        providers.environmentVariable("AIR_SQLITE_LIBRARY_DIR").orNull?.let { sqliteLibraryDirectory ->
            binaries.all {
                linkerOpts("-L$sqliteLibraryDirectory")
            }
        }
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
        val nativeMain by creating {
            dependsOn(commonMain.get())
            dependencies { implementation(libs.sqldelight.native.driver) }
        }
        val nativeTest by creating {
            dependsOn(commonTest.get())
        }
        listOf(
            linuxX64Main,
            mingwX64Main,
            macosX64Main,
            macosArm64Main,
            iosX64Main,
            iosArm64Main,
            iosSimulatorArm64Main,
        ).forEach { it.get().dependsOn(nativeMain) }
        listOf(
            linuxX64Test,
            mingwX64Test,
            macosX64Test,
            macosArm64Test,
            iosX64Test,
            iosArm64Test,
            iosSimulatorArm64Test,
        ).forEach { it.get().dependsOn(nativeTest) }
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
    }
}

val indexedDbRuntimeSource = layout.projectDirectory.file(
    "src/browserIndexedDb/AirIndexedDbRuntime.js",
)
val indexedDbEs5RuntimeSource = layout.projectDirectory.file(
    "src/browserIndexedDb/es5/AirIndexedDbRuntime.js",
)
val generatedIndexedDbRoot = layout.buildDirectory.dir("generated/indexeddb")

val generateIndexedDbInterop by tasks.registering {
    description = "Embeds the audited IndexedDB runtime in the JS and WasmJS artifacts."
    notCompatibleWithConfigurationCache("Reads and embeds a JavaScript source artifact")
    inputs.file(indexedDbRuntimeSource)
    inputs.file(indexedDbEs5RuntimeSource)
    outputs.dir(generatedIndexedDbRoot)
    doLast {
        val expression = indexedDbRuntimeSource.asFile.readText().trim()
        val es5Expression = indexedDbEs5RuntimeSource.asFile.readText().trim().removeSuffix(";")
        require("\"\"\"" !in expression && "\"\"\"" !in es5Expression) {
            "IndexedDB runtime cannot contain a Kotlin triple quote"
        }
        val packagePath = "com/getair/core/catalog"
        val jsFile = generatedIndexedDbRoot.get().file(
            "jsMain/kotlin/$packagePath/GeneratedIndexedDbInterop.kt",
        ).asFile
        val wasmFile = generatedIndexedDbRoot.get().file(
            "wasmJsMain/kotlin/$packagePath/GeneratedIndexedDbInterop.kt",
        ).asFile
        jsFile.parentFile.mkdirs()
        wasmFile.parentFile.mkdirs()
        val tripleQuote = "\"\"\""
        jsFile.writeText(
            "package com.getair.core.catalog\n\n" +
                "import kotlin.js.Promise\n\n" +
                "@Suppress(\"UNUSED_PARAMETER\")\n" +
                "internal fun executeIndexedDbCommandRaw(\n" +
                "    databaseName: String,\n" +
                "    commandJson: String,\n" +
                "): Promise<String> = js(" + tripleQuote + "(" + es5Expression +
                ")(databaseName, commandJson)" + tripleQuote + ")\n",
        )
        wasmFile.writeText(
            "package com.getair.core.catalog\n\n" +
                "import kotlin.js.JsString\n" +
                "import kotlin.js.Promise\n" +
                "import kotlin.js.toJsString\n\n" +
                "@JsFun(" + tripleQuote + expression + tripleQuote + ")\n" +
                "private external fun executeIndexedDbCommandRawExternal(\n" +
                "    databaseName: JsString,\n" +
                "    commandJson: JsString,\n" +
                "): Promise<JsString>\n\n" +
                "internal fun executeIndexedDbCommandRaw(\n" +
                "    databaseName: String,\n" +
                "    commandJson: String,\n" +
                "): Promise<JsString> = executeIndexedDbCommandRawExternal(\n" +
                "    databaseName.toJsString(),\n" +
                "    commandJson.toJsString(),\n" +
                ")\n",
        )
    }
}

kotlin.sourceSets.named("jsMain").get().kotlin.srcDir(
    generatedIndexedDbRoot.map { it.dir("jsMain/kotlin") },
)
kotlin.sourceSets.named("wasmJsMain").get().kotlin.srcDir(
    generatedIndexedDbRoot.map { it.dir("wasmJsMain/kotlin") },
)

tasks.matching { it.name == "compileKotlinJs" || it.name == "compileKotlinWasmJs" }
    .configureEach { dependsOn(generateIndexedDbInterop) }

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
