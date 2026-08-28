import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.DefaultTask
import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.TaskAction
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

val useLocalAirBuilds = providers.gradleProperty("useLocalAirBuilds")
    .orElse(providers.environmentVariable("AIR_USE_LOCAL_BUILDS"))
    .map { value ->
        when (value.trim().lowercase()) {
            "true", "1", "yes" -> true
            "false", "0", "no" -> false
            else -> error("useLocalAirBuilds/AIR_USE_LOCAL_BUILDS must be true or false")
        }
    }
    .getOrElse(false)
val packageRepositoryOverride = providers.gradleProperty("getAirPackageRepository")
    .orElse(providers.environmentVariable("GET_AIR_PACKAGE_REPOSITORY"))
    .orNull
    ?.trim()
    ?.takeIf(String::isNotEmpty)
val stablePackageVersion = Regex("^(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)\\.(0|[1-9][0-9]*)$")

fun getAirContractVersion(property: String, environment: String): String {
    val explicit = providers.gradleProperty(property)
        .orElse(providers.environmentVariable(environment))
        .orNull
        ?.trim()
        ?.takeIf(String::isNotEmpty)
    if (useLocalAirBuilds) return explicit ?: "0.0.0-local"
    require(explicit != null) {
        "$property/$environment is required when local Air composites are disabled"
    }
    if (packageRepositoryOverride == null) {
        require(stablePackageVersion.matches(explicit)) {
            "$property must be a stable MAJOR.MINOR.PATCH GitHub package version"
        }
    } else {
        require(explicit == "0.0.0-ci" || stablePackageVersion.matches(explicit)) {
            "$property must be 0.0.0-ci or stable MAJOR.MINOR.PATCH for a file repository"
        }
    }
    return explicit
}

val getAirStremioVersion = getAirContractVersion("getAirStremioVersion", "GET_AIR_STREMIO_VERSION")
val getAirIptvVersion = getAirContractVersion("getAirIptvVersion", "GET_AIR_IPTV_VERSION")
val getAirVideoVersion = getAirContractVersion("getAirVideoVersion", "GET_AIR_VIDEO_VERSION")

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
                    "-Wl,--allow-shlib-undefined",
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
    js(IR) {
        browser { testTask { useKarma { useChromeHeadless() } } }
        nodejs()
    }
    wasmJs {
        browser { testTask { useKarma { useChromeHeadless() } } }
        nodejs()
    }

    sourceSets {
        val webMain by creating {
            dependsOn(commonMain.get())
        }
        jsMain.get().dependsOn(webMain)
        wasmJsMain.get().dependsOn(webMain)
        val webTest by creating {
            dependsOn(commonTest.get())
        }
        jsTest.get().dependsOn(webTest)
        wasmJsTest.get().dependsOn(webTest)
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
            api("com.getair:iptv:$getAirIptvVersion")
            api("com.getair:stremio-addon-client:$getAirStremioVersion")
            api("com.getair:video:$getAirVideoVersion")
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
                ")(arguments[0], arguments[1])" + tripleQuote + ")\n",
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

abstract class AssertPackageDependencyMode : DefaultTask() {
    @get:Input
    abstract val localBuildsEnabled: Property<Boolean>

    @get:Input
    abstract val includedBuildNames: ListProperty<String>

    @TaskAction
    fun verify() {
        check(!localBuildsEnabled.get()) {
            "Package verification cannot run with local composite substitution enabled"
        }
        check(includedBuildNames.get().isEmpty()) {
            "Package verification found an included composite build"
        }
    }
}

tasks.register<AssertPackageDependencyMode>("assertPackageDependencyMode") {
    group = "verification"
    description = "Fails unless Air is resolving its pinned com.getair package coordinates."
    localBuildsEnabled.set(
        providers.gradleProperty("useLocalAirBuilds")
            .orElse(providers.environmentVariable("AIR_USE_LOCAL_BUILDS"))
            .map { value ->
                when (value.trim().lowercase()) {
                    "true", "1", "yes" -> true
                    "false", "0", "no" -> false
                    else -> error("useLocalAirBuilds/AIR_USE_LOCAL_BUILDS must be true or false")
                }
            }
            .orElse(false),
    )
    includedBuildNames.set(
        gradle.includedBuilds.map { includedBuild -> includedBuild.name },
    )
}

tasks.configureEach {
    if (
        name == "verifyCommonMainAirCatalogDatabaseMigration" ||
        name == "generateCommonMainAirCatalogDatabaseInterface"
    ) {
        mustRunAfter("generateCommonMainAirCatalogDatabaseSchema")
    }
}
