package kvctl

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// BackupNode implements `mage backup <peerID> <destArchive>`: writes a
// tar.gz of peerID's entire data directory -- identity key, sqlite store,
// raft log/snapshots, everything DeleteNode also treats as this node's
// whole on-disk state (see that function's doc comment) -- to destArchive.
//
// Refuses while the node's daemon process still appears to be running (the
// same liveness check DeleteNode/ResumeNode use): archiving sqlite/boltdb
// files out from under a live process could catch them mid-write and
// produce a torn, unusable backup. A follower can be backed up by stopping
// just that one process -- raft keeps serving from the remaining voters --
// there is no cluster-wide quiescing step needed.
//
// If this node was created via AddNodeWithKey/AddFollowerWithKey (an
// identity key stored outside its data directory), that external key file
// is not included here -- back it up separately, the same way DeleteNode
// doesn't touch it either.
func BackupNode(peerID, destArchive string) error {
	reg, err := registry.Open()
	if err != nil {
		return err
	}
	info, ok, err := reg.Get(peerID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("backup: unknown peer id %s (not created on this machine)", peerID)
	}
	if err := ensureNotAlreadyRunning(info); err != nil {
		return fmt.Errorf("backup: %w -- backing up a live node's files risks a torn copy", err)
	}
	if err := archiveDir(info.DataDir, destArchive); err != nil {
		return fmt.Errorf("backup: %w", err)
	}
	return nil
}

// RestoreNode implements `mage restore <archive> <destDir>`: extracts a
// BackupNode archive to destDir.
//
// Deliberately does not touch the registry or start anything.
// registry.NodeDataDir/ClusterDataDir compute a node's expected data
// directory purely from its peer id (see that package's doc comment), so
// restoring into that exact same deterministic path is all a later `mage
// resumenode <peerID>` needs to pick this data back up as that node again;
// restoring under a different path is exactly what a deliberate "recover
// this data under a new registry entry instead" scenario needs. Either
// way, this command stays a plain extract-to-path rather than guessing
// which case applies.
//
// Refuses to extract into an existing non-empty destDir, so it can't
// silently interleave a backup's files with whatever is already there --
// including, worst case, a live node's own data.
func RestoreNode(archivePath, destDir string) error {
	if entries, err := os.ReadDir(destDir); err == nil && len(entries) > 0 {
		return fmt.Errorf("restore: %s already exists and is not empty -- refusing to extract into it", destDir)
	} else if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("restore: %w", err)
	}
	if err := extractArchive(archivePath, destDir); err != nil {
		return fmt.Errorf("restore: %w", err)
	}
	return nil
}

// archiveDir writes a gzip-compressed tar of every regular file and
// directory under srcDir (symlinks and other special file types are
// skipped, not expected inside a data directory this project itself
// writes) to destArchive, with paths relative to srcDir.
func archiveDir(srcDir, destArchive string) error {
	out, err := os.Create(destArchive)
	if err != nil {
		return fmt.Errorf("create %s: %w", destArchive, err)
	}
	defer out.Close()

	gw := gzip.NewWriter(out)
	tw := tar.NewWriter(gw)

	walkErr := filepath.Walk(srcDir, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if path == srcDir {
			return nil
		}
		if !fi.Mode().IsRegular() && !fi.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		hdr, err := tar.FileInfoHeader(fi, "")
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		if fi.IsDir() {
			hdr.Name += "/"
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if fi.IsDir() {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})
	if walkErr != nil {
		_ = tw.Close()
		_ = gw.Close()
		return walkErr
	}
	if err := tw.Close(); err != nil {
		return err
	}
	return gw.Close()
}

// extractArchive extracts a gzip-compressed tar written by archiveDir into
// destDir, rejecting any entry whose path would resolve outside destDir
// (a malformed or maliciously crafted archive) rather than silently
// following it.
func extractArchive(archivePath, destDir string) error {
	in, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("open %s: %w", archivePath, err)
	}
	defer in.Close()

	gr, err := gzip.NewReader(in)
	if err != nil {
		return fmt.Errorf("gzip: %w", err)
	}
	defer gr.Close()

	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return err
	}

	tr := tar.NewReader(gr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		cleanName := filepath.Clean(hdr.Name)
		if cleanName == "." || strings.HasPrefix(cleanName, "..") || filepath.IsAbs(cleanName) {
			return fmt.Errorf("archive entry %q escapes destination", hdr.Name)
		}
		target := filepath.Join(destDir, cleanName)
		if !strings.HasPrefix(target, filepath.Clean(destDir)+string(os.PathSeparator)) {
			return fmt.Errorf("archive entry %q escapes destination", hdr.Name)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				_ = f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		default:
			// Not written by archiveDir -- ignore rather than fail the whole
			// restore over an unexpected entry type.
		}
	}
}
