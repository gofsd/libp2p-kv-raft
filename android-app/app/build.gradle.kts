import java.util.Properties

plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
    id("org.jetbrains.kotlin.plugin.compose")
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

    buildFeatures {
        compose = true
    }

    // CameraX's ImageAnalysis.Analyzer (@ExperimentalGetImage) and
    // Camera2Interop are the only experimental camera APIs the scanner
    // uses -- matches object-history-app's own lint config for the same
    // reason (see MainScannerWidget.kt's @OptIn annotations).
    lint {
        disable += "UnsafeOptInUsageError"
    }
}

// Compose BOM version string every androidx.compose.* artifact below
// resolves against -- keeps ui/material3/foundation/runtime and the
// androidTest ui-test artifacts on one consistent Compose release.
val composeBomCoordinates = "androidx.compose:compose-bom:2024.12.01"

dependencies {
    implementation(files("libs/kvmobile.aar"))
    androidTestImplementation(files("libs/kvmobile.aar"))
    androidTestImplementation("androidx.test.ext:junit:1.2.1")
    androidTestImplementation("androidx.test:runner:1.6.2")
    // Drives UiCommandE2ETest (see src/androidTest) -- real clicks through
    // the app's Compose screens for every CommandCatalog.kt entry, unlike
    // E2ETest's direct Kvmobile.sendEvent calls.
    androidTestImplementation("androidx.test.espresso:espresso-core:3.6.1")
    androidTestImplementation("androidx.test:core:1.6.1")

    // Jetpack Compose -- single-Activity UI shell + navigation, so the
    // scanner (below) can be mounted once, above the nav graph, and stay
    // alive across every screen instead of tearing down/rebinding its
    // camera on each Activity transition (see docs/android.md and
    // CommandCatalog.kt/CommandDetailScreen for the screens themselves).
    implementation(platform(composeBomCoordinates))
    androidTestImplementation(platform(composeBomCoordinates))
    implementation("androidx.compose.ui:ui")
    implementation("androidx.compose.ui:ui-tooling-preview")
    implementation("androidx.compose.material3:material3")
    implementation("androidx.compose.material:material-icons-extended")
    implementation("androidx.compose.foundation:foundation")
    implementation("androidx.compose.runtime:runtime")
    implementation("androidx.activity:activity-compose:1.9.3")
    implementation("androidx.navigation:navigation-compose:2.8.4")
    implementation("androidx.lifecycle:lifecycle-runtime-ktx:2.8.7")
    implementation("androidx.lifecycle:lifecycle-runtime-compose:2.8.7")
    debugImplementation("androidx.compose.ui:ui-tooling")
    // UiCommandE2ETest drives the Compose tree via ComposeTestRule
    // (onNodeWithTag/performClick/performTextInput) instead of Espresso's
    // View-only finders, which don't reach into a Compose tree.
    androidTestImplementation("androidx.compose.ui:ui-test-junit4")
    debugImplementation("androidx.compose.ui:ui-test-manifest")

    // ScannerCoordinator's SharedFlow (see ScannerCoordinator.kt) --
    // fanning a decoded scan out to whichever screen/dialog wants it.
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.9.0")

    // DataMatrix scan (CameraX frame -> ZXing decode) and generate
    // (ZXing encode -> Bitmap), ported from object-history-app's
    // MainScannerWidget.kt/DataMatrixComposable.kt -- see those files'
    // doc comments for why ZXing+CameraX over MLKit.
    implementation("com.google.zxing:core:3.5.3")
    implementation("androidx.camera:camera-core:1.4.1")
    implementation("androidx.camera:camera-camera2:1.4.1")
    implementation("androidx.camera:camera-lifecycle:1.4.1")
    implementation("androidx.camera:camera-view:1.4.1")
    implementation("androidx.camera:camera-extensions:1.4.1")

    // Plain JVM unit tests (no emulator/instrumentation) -- e.g. proving
    // the ByteArray<->ISO-8859-1-String round trip through ZXing's
    // DataMatrix writer/reader is byte-exact before any camera code
    // depends on it (see src/test/.../DataMatrixCodecTest.kt).
    testImplementation("junit:junit:4.13.2")
    testImplementation("com.google.zxing:core:3.5.3")
}
