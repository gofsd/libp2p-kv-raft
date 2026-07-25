import java.util.Properties

plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

// Release signing lives in android-app/keystore.properties, gitignored --
// see that file's sibling android-app/keystore/ dir for the .jks itself.
// Absent entirely on a checkout that only ever runs assembleDebug (CI,
// e2e, a fresh clone); release/bundleRelease fail with a clear Gradle
// error pointing at this file instead of silently falling back to debug
// signing.
val keystorePropertiesFile = rootProject.file("keystore.properties")
val keystoreProperties = Properties().apply {
    if (keystorePropertiesFile.exists()) {
        keystorePropertiesFile.inputStream().use { load(it) }
    }
}

android {
    namespace = "com.gofsd.kvdemo"
    compileSdk = 36

    defaultConfig {
        applicationId = "com.gofsd.kvdemo"
        minSdk = 26 // ASharedMemory_create's minimum (see pkg/ipc/ipc_android.go)
        targetSdk = 36
        versionCode = 1
        versionName = "1.0"
        // Drives E2ETest (see src/androidTest) -- pkg/e2erun's Android
        // execution path runs it via `adb shell am instrument`.
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }

    signingConfigs {
        if (keystorePropertiesFile.exists()) {
            create("release") {
                storeFile = rootProject.file(keystoreProperties.getProperty("storeFile"))
                storePassword = keystoreProperties.getProperty("storePassword")
                keyAlias = keystoreProperties.getProperty("keyAlias")
                keyPassword = keystoreProperties.getProperty("keyPassword")
            }
        }
    }

    buildTypes {
        debug {
            isMinifyEnabled = false
        }
        release {
            isMinifyEnabled = false
            if (keystorePropertiesFile.exists()) {
                signingConfig = signingConfigs.getByName("release")
            }
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }

    kotlinOptions {
        jvmTarget = "17"
    }

    ndkVersion = "28.2.13676358"
}

dependencies {
    implementation(files("libs/kvmobile.aar"))
    androidTestImplementation(files("libs/kvmobile.aar"))
    androidTestImplementation("androidx.test.ext:junit:1.2.1")
    androidTestImplementation("androidx.test:runner:1.6.2")
}
