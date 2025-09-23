// Copyright 2025 Daytona Platforms Inc.
// SPDX-License-Identifier: AGPL-3.0

package models

import "time"

type SandboxCleanupInfo struct {
	ID   string
	Name string
}

type DomainSandboxResponse struct {
	ID     string `json:"id"`
	Domain string `json:"domain"`
}

type SnapshotCleanupInfo struct {
	ID        string
	Name      string
	CreatedAt time.Time
}

type DomainSnapshotResponse struct {
	RunnerSnapshotID string `json:"runnerSnapshotId"`
	RunnerID         string `json:"runnerId"`
	RunnerDomain     string `json:"runnerDomain"`
}
