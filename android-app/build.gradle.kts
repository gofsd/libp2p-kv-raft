plugins {
    id("com.android.application") version "8.13.2" apply false
    id("org.jetbrains.kotlin.android") version "2.0.21" apply false
    // Kotlin 2.0+ moved the Jetpack Compose compiler into the Kotlin
    // repo itself, applied via this plugin instead of a separate
    // "compose-compiler" artifact/version -- must match the Kotlin
    // version above exactly.
    id("org.jetbrains.kotlin.plugin.compose") version "2.0.21" apply false
}
