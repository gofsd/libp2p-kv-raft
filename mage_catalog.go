//go:build mage

package main

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
)

// PutGroup implements `mage creategroup`/`mage updategroup <id> <name>
// <public>`: creates or updates the Group record id=name -- see
// pkg/kvctl.PutGroup's doc comment. public ("true"/"false") grants
// unconditional access to this group's linked commands to any peer, with
// no addpeertogroup membership needed, if true. Only a current raft voter
// may do this.
// Usage: mage creategroup <id> <name> <public: true|false>
func CreateGroup(id, name, public string) error {
	pub, err := strconv.ParseBool(public)
	if err != nil {
		return fmt.Errorf("public: %w", err)
	}
	if err := kvctl.PutGroup(id, name, pub); err != nil {
		return err
	}
	fmt.Println("✅ group put")
	return nil
}

// UpdateGroup is CreateGroup's alias for the "this id already exists"
// case -- see pkg/kvctl.PutGroup's doc comment (single-step Put, no
// separate create/update distinction).
// Usage: mage updategroup <id> <name> <public: true|false>
func UpdateGroup(id, name, public string) error {
	return CreateGroup(id, name, public)
}

// DeleteGroup implements `mage deletegroup <id>`: deletes Group id,
// cascading to every GroupCommand/PeerGroup record referencing it. Only a
// current raft voter may do this.
// Usage: mage deletegroup <id>
func DeleteGroup(id string) error {
	if err := kvctl.DeleteGroup(id); err != nil {
		return err
	}
	fmt.Println("✅ group deleted")
	return nil
}

// GetGroup implements `mage getgroup <id>`: prints id's current
// definition as one JSON object.
// Usage: mage getgroup <id>
func GetGroup(id string) error {
	group, err := kvctl.GetGroup(id)
	if err != nil {
		return err
	}
	out, err := json.Marshal(group)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

// ListGroups implements `mage listgroups`: prints every Group, one JSON
// object per line.
// Usage: mage listgroups
func ListGroups() error {
	groups, err := kvctl.ListGroups()
	if err != nil {
		return err
	}
	for _, g := range groups {
		out, err := json.Marshal(g)
		if err != nil {
			return err
		}
		fmt.Println(string(out))
	}
	return nil
}

// AddPeerToGroup implements `mage addpeertogroup <peerID> <groupID>`:
// grants peerID membership in groupID -- peers in a group linked to a
// command (see AddCommandToGroup) become permitted to submit/execute it.
// Only a current raft voter may do this.
// Usage: mage addpeertogroup <peerID> <groupID>
func AddPeerToGroup(peerID, groupID string) error {
	if err := kvctl.AddPeerToGroup(peerID, groupID); err != nil {
		return err
	}
	fmt.Println("✅ peer added to group")
	return nil
}

// RemovePeerFromGroup implements `mage removepeerfromgroup <peerID>
// <groupID>`: revokes peerID's membership in groupID.
// Usage: mage removepeerfromgroup <peerID> <groupID>
func RemovePeerFromGroup(peerID, groupID string) error {
	if err := kvctl.RemovePeerFromGroup(peerID, groupID); err != nil {
		return err
	}
	fmt.Println("✅ peer removed from group")
	return nil
}

// ListGroupsForPeer implements `mage listgroupsforpeer <peerID>`: prints
// every group id peerID belongs to, one per line.
// Usage: mage listgroupsforpeer <peerID>
func ListGroupsForPeer(peerID string) error {
	groupIDs, err := kvctl.ListGroupsForPeer(peerID)
	if err != nil {
		return err
	}
	for _, id := range groupIDs {
		fmt.Println(id)
	}
	return nil
}

// PutCommand implements `mage createcommand`/`mage updatecommand <id>
// <name> <peerID>`: creates or updates the Command record
// id={name, peerID} (peerID is where it may be executed). Only a current
// raft voter may do this.
// Usage: mage createcommand <id> <name> <peerID>
func CreateCommand(id, name, peerID string) error {
	if err := kvctl.PutCommand(id, name, peerID); err != nil {
		return err
	}
	fmt.Println("✅ command put")
	return nil
}

// UpdateCommand is CreateCommand's alias for the "this id already exists"
// case.
// Usage: mage updatecommand <id> <name> <peerID>
func UpdateCommand(id, name, peerID string) error {
	return CreateCommand(id, name, peerID)
}

// CreateCommandSpec is CreateCommand carrying the command's form definition
// as well -- an opaque string (JSON, in practice) the cluster replicates
// verbatim and every client renders its inputs from, so a new command reaches
// every device without any of them gaining new code. peerID may be empty for
// a command not yet bound to a station; submitting one then fails naming the
// missing target.
//
// Usage: mage createcommandspec <id> <name> <peerID|""> '<specJSON>'
func CreateCommandSpec(id, name, peerID, spec string) error {
	if err := kvctl.PutCommandWithSpec(id, name, peerID, spec); err != nil {
		return err
	}
	fmt.Println("✅ command put")
	return nil
}

// ClearCommandSpec removes a command's form definition while keeping the
// command. A plain updatecommand preserves the stored spec, so this is the
// only way to remove one deliberately.
//
// Usage: mage clearcommandspec <id> <name> <peerID|"">
func ClearCommandSpec(id, name, peerID string) error {
	if err := kvctl.ClearCommandSpec(id, name, peerID); err != nil {
		return err
	}
	fmt.Println("✅ command spec cleared")
	return nil
}

// CreateStation implements `mage createstation <peerID> <name> [attrsJSON]`:
// describes a device in operational terms, so every record naming it by peer
// id can be shown as something a person can read. Only a current raft voter
// may do this -- a device cannot name itself.
//
// Usage: mage createstation <peerID> <name> <attrsJSON|"">
func CreateStation(peerID, name, attrs string) error {
	if err := kvctl.PutStation(peerID, name, attrs); err != nil {
		return err
	}
	fmt.Println("✅ station put")
	return nil
}

// UpdateStation is CreateStation's alias for the "this peer id is already
// described" case.
// Usage: mage updatestation <peerID> <name> <attrsJSON|"">
func UpdateStation(peerID, name, attrs string) error {
	return CreateStation(peerID, name, attrs)
}

// DeleteStation removes a device's description. Its cluster membership and
// group memberships are untouched.
// Usage: mage deletestation <peerID>
func DeleteStation(peerID string) error {
	if err := kvctl.DeleteStation(peerID); err != nil {
		return err
	}
	fmt.Println("🗑️  station deleted")
	return nil
}

// GetStation prints one station description as JSON.
// Usage: mage getstation <peerID>
func GetStation(peerID string) error {
	station, err := kvctl.GetStation(peerID)
	if err != nil {
		return err
	}
	out, err := json.Marshal(station)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

// ListStations prints every station description, one JSON object per line.
// Usage: mage liststations
func ListStations() error {
	stations, err := kvctl.ListStations()
	if err != nil {
		return err
	}
	for _, s := range stations {
		out, err := json.Marshal(s)
		if err != nil {
			return err
		}
		fmt.Println(string(out))
	}
	return nil
}

// DeleteCommand implements `mage deletecommand <id>`: deletes Command id,
// cascading to every GroupCommand record referencing it. Only a current
// raft voter may do this.
// Usage: mage deletecommand <id>
func DeleteCommand(id string) error {
	if err := kvctl.DeleteCommand(id); err != nil {
		return err
	}
	fmt.Println("✅ command deleted")
	return nil
}

// GetCommand implements `mage getcommand <id>`: prints id's current
// definition as one JSON object.
// Usage: mage getcommand <id>
func GetCommand(id string) error {
	cmd, err := kvctl.GetCommand(id)
	if err != nil {
		return err
	}
	out, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}

// ListCommands implements `mage listcommands`: prints every Command, one
// JSON object per line.
// Usage: mage listcommands
func ListCommands() error {
	commands, err := kvctl.ListCommands()
	if err != nil {
		return err
	}
	for _, c := range commands {
		out, err := json.Marshal(c)
		if err != nil {
			return err
		}
		fmt.Println(string(out))
	}
	return nil
}

// AddCommandToGroup implements `mage addcommandtogroup <commandID>
// <groupID>`: links commandID to groupID -- peers added to groupID
// (AddPeerToGroup) become permitted to submit/execute commandID. Only a
// current raft voter may do this.
// Usage: mage addcommandtogroup <commandID> <groupID>
func AddCommandToGroup(commandID, groupID string) error {
	if err := kvctl.CreateGroupCommand(commandID, groupID); err != nil {
		return err
	}
	fmt.Println("✅ command linked to group")
	return nil
}

// RemoveCommandFromGroup implements `mage removecommandfromgroup
// <commandID> <groupID>`: unlinks commandID from groupID.
// Usage: mage removecommandfromgroup <commandID> <groupID>
func RemoveCommandFromGroup(commandID, groupID string) error {
	if err := kvctl.DeleteGroupCommand(commandID, groupID); err != nil {
		return err
	}
	fmt.Println("✅ command unlinked from group")
	return nil
}

// ListGroupsForCommand implements `mage listgroupsforcommand
// <commandID>`: prints every group id commandID is linked to, one per
// line.
// Usage: mage listgroupsforcommand <commandID>
func ListGroupsForCommand(commandID string) error {
	groupIDs, err := kvctl.ListGroupsForCommand(commandID)
	if err != nil {
		return err
	}
	for _, id := range groupIDs {
		fmt.Println(id)
	}
	return nil
}
