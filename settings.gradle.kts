pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
    }
}

rootProject.name = "air"

listOf(
    "../stremio-addon-client",
    "../iptv",
    "../video",
).map(::file)
    .filter { it.resolve("settings.gradle.kts").isFile }
    .forEach { includeBuild(it) }
