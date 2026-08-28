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

        val packageUser = providers.gradleProperty("githubPackagesUser")
            .orElse(providers.environmentVariable("GITHUB_ACTOR"))
        val packageToken = providers.gradleProperty("githubPackagesToken")
            .orElse(providers.environmentVariable("GITHUB_TOKEN"))

        fun org.gradle.api.artifacts.dsl.RepositoryHandler.getAirPackage(
            repository: String,
            modulePattern: String,
        ) {
            exclusiveContent {
                forRepository {
                    maven {
                        name = "getAir${repository.replaceFirstChar(Char::uppercaseChar)}"
                        url = uri("https://maven.pkg.github.com/get-air/$repository")
                        credentials {
                            username = packageUser.orNull
                            password = packageToken.orNull
                        }
                    }
                }
                // KMP metadata redirects to target modules such as `iptv-jvm`
                // and `video-linuxx64`, which live in the same package repo.
                filter { includeModuleByRegex("com\\.getair", modulePattern) }
            }
        }

        getAirPackage("stremio-addon-client", "stremio-addon-client(?:-.*)?")
        getAirPackage("iptv", "iptv(?:-.*)?")
        getAirPackage("video", "video(?:-.*)?")
    }
}

rootProject.name = "air"

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

val localContractBuilds = listOf(
    "../stremio-addon-client",
    "../iptv",
    "../video",
).map(::file)

if (useLocalAirBuilds) {
    localContractBuilds.forEach { build ->
        require(build.resolve("settings.gradle.kts").isFile) {
            "Local Air contract build is missing: ${build.canonicalPath}"
        }
        includeBuild(build)
    }
}
