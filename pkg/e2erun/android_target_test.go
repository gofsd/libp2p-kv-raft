package e2erun

import (
	"path/filepath"
	"testing"
)

// TestAndroidTargetDefaultsToKvdemo pins the property the whole change rests on:
// with nothing set, every Android path drives exactly what it drove before
// androidTarget existed. A regression here silently repoints a real e2e run at
// the wrong app.
func TestAndroidTargetDefaultsToKvdemo(t *testing.T) {
	got, err := resolveAndroidTarget("/repo")
	if err != nil {
		t.Fatalf("resolveAndroidTarget: %v", err)
	}
	if got.AppID != "com.gofsd.kvdemo" {
		t.Errorf("AppID = %q, want com.gofsd.kvdemo", got.AppID)
	}
	if want := filepath.Join("/repo", "android-app"); got.Dir != want {
		t.Errorf("Dir = %q, want %q", got.Dir, want)
	}
	if want := "com.gofsd.kvdemo.test"; got.testAppID() != want {
		t.Errorf("testAppID = %q, want %q", got.testAppID(), want)
	}
	if want := "com.gofsd.kvdemo.E2ETest"; got.rowTestClass() != want {
		t.Errorf("rowTestClass = %q, want %q", got.rowTestClass(), want)
	}
	if want := "com.gofsd.kvdemo.UiCommandE2ETest"; got.uiTestClass() != want {
		t.Errorf("uiTestClass = %q, want %q", got.uiTestClass(), want)
	}
	if want := "com.gofsd.kvdemo.test/androidx.test.runner.AndroidJUnitRunner"; got.runner() != want {
		t.Errorf("runner = %q, want %q", got.runner(), want)
	}
	if want := "/sdcard/Android/data/com.gofsd.kvdemo/files/e2e_results.json"; got.deviceResultsPath("e2e_results.json") != want {
		t.Errorf("deviceResultsPath = %q, want %q", got.deviceResultsPath("e2e_results.json"), want)
	}
	if want := filepath.Join("/repo", "android-app", "app", "libs", "kvmobile.aar"); got.aarFile() != want {
		t.Errorf("aarFile = %q, want %q", got.aarFile(), want)
	}
	if len(got.GradleArgs) != 0 {
		t.Errorf("GradleArgs = %v, want none", got.GradleArgs)
	}
}

// TestAndroidTargetMesPreset covers the second app, and specifically the two
// fields that are not derivable from the app id: the test package (a ".e2e"
// subpackage there, the application package here) and the AAR path its Gradle
// build actually reads.
func TestAndroidTargetMesPreset(t *testing.T) {
	t.Setenv("E2E_ANDROID_TARGET", "mes")
	got, err := resolveAndroidTarget("/repo")
	if err != nil {
		t.Fatalf("resolveAndroidTarget: %v", err)
	}
	if got.AppID != "com.object_history.mes" {
		t.Errorf("AppID = %q", got.AppID)
	}
	if want := "com.object_history.mes.e2e.UiCommandE2ETest"; got.uiTestClass() != want {
		t.Errorf("uiTestClass = %q, want %q", got.uiTestClass(), want)
	}
	// A sibling checkout, so the resolved dir must climb out of this repo.
	if want, _ := filepath.Abs(filepath.Join("/repo", "..", "object-history-app")); got.Dir != want {
		t.Errorf("Dir = %q, want %q", got.Dir, want)
	}
	if want := filepath.Join(got.Dir, "app", "build", "kvmobile", "kvmobile.aar"); got.aarFile() != want {
		t.Errorf("aarFile = %q, want %q", got.aarFile(), want)
	}
}

func TestAndroidTargetEnvOverrides(t *testing.T) {
	t.Setenv("E2E_ANDROID_APP_ID", "com.example.other")
	t.Setenv("E2E_ANDROID_TEST_PKG", "com.example.other.tests")
	t.Setenv("E2E_ANDROID_DIR", "/somewhere/else")
	t.Setenv("E2E_ANDROID_AAR", "libs/x.aar")
	t.Setenv("E2E_ANDROID_GRADLE_ARGS", "-Pa=1 -Pb=2")

	got, err := resolveAndroidTarget("/repo")
	if err != nil {
		t.Fatalf("resolveAndroidTarget: %v", err)
	}
	if got.AppID != "com.example.other" {
		t.Errorf("AppID = %q", got.AppID)
	}
	if want := "com.example.other.tests.E2ETest"; got.rowTestClass() != want {
		t.Errorf("rowTestClass = %q, want %q", got.rowTestClass(), want)
	}
	// Already absolute: left alone, not joined onto repoRoot.
	if got.Dir != "/somewhere/else" {
		t.Errorf("Dir = %q, want /somewhere/else", got.Dir)
	}
	if want := "/somewhere/else/libs/x.aar"; got.aarFile() != want {
		t.Errorf("aarFile = %q, want %q", got.aarFile(), want)
	}
	if len(got.GradleArgs) != 2 || got.GradleArgs[0] != "-Pa=1" || got.GradleArgs[1] != "-Pb=2" {
		t.Errorf("GradleArgs = %v", got.GradleArgs)
	}
}

// A preset carrying Gradle args has to be able to say "none", which an unset
// variable cannot mean -- that has to keep the preset's own args.
func TestAndroidTargetGradleArgsCanBeCleared(t *testing.T) {
	t.Setenv("E2E_ANDROID_TARGET", "mes")
	t.Setenv("E2E_ANDROID_GRADLE_ARGS", "-")
	got, err := resolveAndroidTarget("/repo")
	if err != nil {
		t.Fatalf("resolveAndroidTarget: %v", err)
	}
	if len(got.GradleArgs) != 0 {
		t.Errorf("GradleArgs = %v, want none", got.GradleArgs)
	}
}

func TestAndroidTargetUnknownNameIsAnError(t *testing.T) {
	t.Setenv("E2E_ANDROID_TARGET", "nope")
	if _, err := resolveAndroidTarget("/repo"); err == nil {
		t.Fatal("expected an error for an unknown target name")
	}
}

// mustResolveAndroidTarget has no error to return, so it must fall back to the
// default rather than address a nonexistent app.
func TestMustResolveFallsBackOnUnknownName(t *testing.T) {
	t.Setenv("E2E_ANDROID_TARGET", "nope")
	if got := mustResolveAndroidTarget(); got.AppID != "com.gofsd.kvdemo" {
		t.Errorf("AppID = %q, want the default", got.AppID)
	}
}
