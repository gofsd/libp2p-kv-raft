//go:build mage

package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/Masterminds/semver/v3"
)

// Status prints the current latest version based on Git tags
func Status() error {
	v, err := getCurrentVersion()
	if err != nil {
		fmt.Println("No current version found. Start by bumping a version (e.g., mage patch).")
		return nil
	}
	fmt.Printf("Current Version: %s\n", v.String())
	return nil
}

// Patch bumps the patch version (e.g., 1.0.0 -> 1.0.1)
func Patch() error {
	return bump(func(v *semver.Version) semver.Version { return v.IncPatch() }, "")
}

// Minor bumps the minor version (e.g., 1.0.0 -> 1.1.0)
func Minor() error {
	return bump(func(v *semver.Version) semver.Version { return v.IncMinor() }, "")
}

// Major bumps the major version (e.g., 1.0.0 -> 2.0.0)
func Major() error {
	return bump(func(v *semver.Version) semver.Version { return v.IncMajor() }, "")
}

// Alpha bumps or creates an alpha prerelease stage (e.g., 1.0.0 -> 1.0.1-alpha.1)
func Alpha() error {
	return bump(func(v *semver.Version) semver.Version { return v.IncPatch() }, "alpha")
}

// Beta bumps or creates a beta prerelease stage (e.g., 1.0.0 -> 1.0.1-beta.1)
func Beta() error {
	return bump(func(v *semver.Version) semver.Version { return v.IncPatch() }, "beta")
}

// RC bumps or creates a release candidate stage (e.g., 1.0.0 -> 1.0.1-rc.1)
func RC() error {
	return bump(func(v *semver.Version) semver.Version { return v.IncPatch() }, "rc")
}

// --- Helper Functions ---

func getCurrentVersion() (*semver.Version, error) {
	// Get the latest tag from git sorted by creation date
	cmd := exec.Command("git", "describe", "--tags", "--abbrev=0")
	out, err := cmd.Output()
	if err != nil {
		// If no tags exist, start at v0.0.0
		return semver.NewVersion("v0.0.0")
	}

	tag := strings.TrimSpace(string(out))
	return semver.NewVersion(tag)
}

func bump(bumpFn func(*semver.Version) semver.Version, stage string) error {
	current, err := getCurrentVersion()
	if err != nil {
		return err
	}

	var next semver.Version

	if stage != "" {
		// If it's already on the same prerelease stage, increment the prerelease number (e.g., alpha.1 -> alpha.2)
		if strings.Contains(current.Prerelease(), stage) {
			// FIX: IncPatch() only returns 1 value, no error.
			next = current.IncPatch()

			// Re-apply the pre-release increment strategy
			parts := strings.Split(current.Prerelease(), ".")
			if len(parts) == 2 {
				var num int
				fmt.Sscanf(parts[1], "%d", &num)
				next, _ = current.SetPrerelease(fmt.Sprintf("%s.%d", stage, num+1))
			}
		} else {
			// Brand new prerelease stage: Bump patch and append stage.1 (e.g., 1.0.1-alpha.1)
			temp := bumpFn(current)
			next, err = temp.SetPrerelease(stage + ".1")
			if err != nil {
				return err
			}
		}
	} else {
		// Standard production release (clears out any alpha/beta tags)
		next = bumpFn(current)
	}

	nextTag := "v" + next.String()
	fmt.Printf("Bumping version: %s ➡️ %s\n", "v"+current.String(), nextTag)

	// Create git tag
	gitTag := exec.Command("git", "tag", "-a", nextTag, "-m", fmt.Sprintf("Release %s", nextTag))
	gitTag.Stdout = os.Stdout
	gitTag.Stderr = os.Stderr
	if err := gitTag.Run(); err != nil {
		return fmt.Errorf("failed to create git tag: %w", err)
	}

	fmt.Printf("Successfully created tag %s! Run 'git push --tags' to deploy.\n", nextTag)
	return nil
}
