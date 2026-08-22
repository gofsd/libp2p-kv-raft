package e2erun

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// androidTarget names one Android app this package's Android paths can drive:
// which package to install and instrument, where its Gradle project lives, and
// which path inside that project its build expects a gomobile-built
// kvmobile.aar at.
//
// This exists because there are now two apps built on mobile/kvmobile that
// carry the same instrumented test classes: this repo's own android-app, and
// github.com/gofsd/object-history-app, which the whole android-app feature set
// (the command catalog, the scan-to-run pipeline, E2ETest/
// ChannelFileTransferTest/UiCommandE2ETest) was ported into. Both are driven by
// the same test plan -- test/e2e/testdata.json's rows and android_optical_cases
// stay the single source of truth rather than being forked per app -- so the
// only thing that varies is where to send the adb and Gradle commands.
//
// Every field defaults to android-app, so an existing invocation behaves
// exactly as it did before this type existed.
type androidTarget struct {
	// AppID is the installed application id -- AGP derives the instrumented
	// test APK's id from it as "<AppID>.test" (neither app overrides
	// testApplicationId), and the device-side results files this package pulls
	// live under /sdcard/Android/data/<AppID>/files.
	AppID string

	// Dir is the Gradle project root (the directory holding gradlew), absolute.
	// Absolute rather than relative to this repo specifically so a target can
	// live outside it -- object-history-app is a sibling checkout, not a
	// subdirectory.
	Dir string

	// TestPkg is the package the instrumented test classes live in. Not
	// derivable from AppID: android-app keeps them in its application package,
	// object-history-app in a ".e2e" subpackage beside its own ClusterE2ETest.
	TestPkg string

	// AarPath is where a freshly gomobile-bound kvmobile.aar has to be written
	// for this app's build to pick it up, relative to Dir. The two apps differ:
	// android-app declares implementation(files("libs/kvmobile.aar")), while
	// object-history-app's own buildKvmobileAar task consumes
	// app/build/kvmobile/kvmobile.aar -- and that task early-returns when the
	// file already exists, so writing the identity-baked AAR there first is
	// exactly what stops it building an identity-less one over the top.
	AarPath string

	// GradleArgs are extra flags every Gradle invocation for this target gets.
	// object-history-app needs -Pabi=<abi> on a device whose storage cannot
	// take its four-ABI debug APK (~146 MB); android-app needs nothing.
	GradleArgs []string
}

// androidTargetPresets are the named targets E2E_ANDROID_TARGET selects
// between. Dir is relative to this repo's root and made absolute by
// resolveAndroidTarget.
var androidTargetPresets = map[string]androidTarget{
	"kvdemo": {
		AppID:   "com.gofsd.kvdemo",
		Dir:     "android-app",
		TestPkg: "com.gofsd.kvdemo",
		AarPath: filepath.Join("app", "libs", "kvmobile.aar"),
	},
	"mes": {
		AppID:      "com.object_history.mes",
		Dir:        filepath.Join("..", "object-history-app"),
		TestPkg:    "com.object_history.mes.e2e",
		AarPath:    filepath.Join("app", "build", "kvmobile", "kvmobile.aar"),
		GradleArgs: []string{"-Pabi=x86_64"},
	},
}

const defaultAndroidTarget = "kvdemo"

// resolveAndroidTarget returns the target every Android path in this package
// should drive, from the environment:
//
//	E2E_ANDROID_TARGET      one of androidTargetPresets' keys (default "kvdemo")
//	E2E_ANDROID_APP_ID      override AppID
//	E2E_ANDROID_DIR         override Dir (absolute, or relative to repoRoot)
//	E2E_ANDROID_TEST_PKG    override TestPkg
//	E2E_ANDROID_AAR         override AarPath (relative to Dir)
//	E2E_ANDROID_GRADLE_ARGS override GradleArgs (space-separated; "-" for none)
//
// Environment rather than a flag threaded through every call site because the
// Android paths are reached from three different entry points -- mage e2e:*,
// the manual hardware-gated Go tests, and runOpticalMethod -- and an env var is
// the one mechanism all three already have in common.
//
// repoRoot may be empty for a caller that only needs the identity fields
// (AppID/TestPkg) and never touches Dir -- instrumenting, pulling results,
// force-stopping. Dir is then left as configured rather than made absolute.
func resolveAndroidTarget(repoRoot string) (androidTarget, error) {
	name := strings.TrimSpace(os.Getenv("E2E_ANDROID_TARGET"))
	if name == "" {
		name = defaultAndroidTarget
	}
	t, ok := androidTargetPresets[name]
	if !ok {
		return androidTarget{}, fmt.Errorf(
			"e2erun: unknown E2E_ANDROID_TARGET %q (known: %s)", name, knownAndroidTargets())
	}

	if v := strings.TrimSpace(os.Getenv("E2E_ANDROID_APP_ID")); v != "" {
		t.AppID = v
	}
	if v := strings.TrimSpace(os.Getenv("E2E_ANDROID_DIR")); v != "" {
		t.Dir = v
	}
	if v := strings.TrimSpace(os.Getenv("E2E_ANDROID_TEST_PKG")); v != "" {
		t.TestPkg = v
	}
	if v := strings.TrimSpace(os.Getenv("E2E_ANDROID_AAR")); v != "" {
		t.AarPath = v
	}
	// "-" and not "" for the empty case: an unset variable has to mean "keep
	// the preset's args", so there has to be some other way to say "none".
	if v := strings.TrimSpace(os.Getenv("E2E_ANDROID_GRADLE_ARGS")); v != "" {
		if v == "-" {
			t.GradleArgs = nil
		} else {
			t.GradleArgs = strings.Fields(v)
		}
	}

	if repoRoot != "" && !filepath.IsAbs(t.Dir) {
		abs, err := filepath.Abs(filepath.Join(repoRoot, t.Dir))
		if err != nil {
			return androidTarget{}, fmt.Errorf("e2erun: resolve android target dir: %w", err)
		}
		t.Dir = abs
	}
	return t, nil
}

// knownAndroidTargets lists the preset names, for an error message.
func knownAndroidTargets() string {
	names := make([]string, 0, len(androidTargetPresets))
	for n := range androidTargetPresets {
		names = append(names, n)
	}
	// Small and fixed; sorted only so the message is stable between runs.
	for i := range names {
		for j := i + 1; j < len(names); j++ {
			if names[j] < names[i] {
				names[i], names[j] = names[j], names[i]
			}
		}
	}
	return strings.Join(names, ", ")
}

// mustResolveAndroidTarget is resolveAndroidTarget for the call sites that have
// no error to return (an `am instrument` argument builder, a results path).
// A misspelled E2E_ANDROID_TARGET falls back to the default rather than
// silently addressing nothing -- and says so, since a run that quietly drove
// the wrong app would be far harder to work out than one noisy line.
func mustResolveAndroidTarget() androidTarget {
	t, err := resolveAndroidTarget("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2erun: %v -- falling back to %q\n", err, defaultAndroidTarget)
		return androidTargetPresets[defaultAndroidTarget]
	}
	return t
}

// testAppID is the instrumented test APK's application id -- AGP's default of
// "<applicationId>.test", which neither app overrides.
func (t androidTarget) testAppID() string { return t.AppID + ".test" }

// runner is the `am instrument` component string.
func (t androidTarget) runner() string {
	return t.testAppID() + "/androidx.test.runner.AndroidJUnitRunner"
}

func (t androidTarget) rowTestClass() string     { return t.TestPkg + ".E2ETest" }
func (t androidTarget) uiTestClass() string      { return t.TestPkg + ".UiCommandE2ETest" }
func (t androidTarget) channelTestClass() string { return t.TestPkg + ".ChannelFileTransferTest" }

// deviceResultsPath is where an instrumented test writes results for this
// package to adb pull: the app's own external files dir, which needs no
// run-as to read.
func (t androidTarget) deviceResultsPath(name string) string {
	return fmt.Sprintf("/sdcard/Android/data/%s/files/%s", t.AppID, name)
}

// aarFile is the absolute path a gomobile-built kvmobile.aar goes to.
func (t androidTarget) aarFile() string { return filepath.Join(t.Dir, t.AarPath) }

// gradlew is the absolute path to this target's Gradle wrapper.
func (t androidTarget) gradlew() string { return filepath.Join(t.Dir, "gradlew") }
