// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/kv"
	"github.com/grafana/dskit/ring"

	"github.com/grafana/mimir/pkg/compartments"
)

// registrar keeps every fake in this process registered: one instance-ring entry each (ACTIVE,
// fresh heartbeat) and one ACTIVE partition with an owner each. It sends one CAS per key per
// heartbeat for the whole process, rather than one lifecycler per fake.
//
// Point it at a KV prefix the real write path doesn't use (for example fake-collectors/), and point
// only the queriers under test at the same prefix.
type registrar struct {
	instances      kv.Client
	partitions     kv.Client
	fakes          []*fake
	compartmentsOn bool
	logger         log.Logger
}

// Keys the querier reads, as in pkg/ingester (IngesterRingKey, PartitionRingKey).
const (
	instanceRingKey      = "ring"
	partitionRingBaseKey = "ingester-partitions"
)

func partitionRingKey(compartmentsEnabled bool, compartment int) string {
	if !compartmentsEnabled {
		return partitionRingBaseKey
	}
	return compartments.WithReadCompartmentSuffix(partitionRingBaseKey, compartment)
}

// heartbeat refreshes every fake's registration. The CAS callbacks return (nil, …) to skip an
// update; their second result asks for a retry on error, it doesn't mean "changed".
func (r *registrar) heartbeat(ctx context.Context) error {
	now := time.Now()
	err := r.instances.CAS(ctx, instanceRingKey, func(in any) (any, bool, error) {
		desc := ring.GetOrCreateRingDesc(in)
		for _, f := range r.fakes {
			inst, ok := desc.Ingesters[f.id]
			if !ok {
				inst = desc.AddIngester(f.id, f.addr, f.zone, nil, ring.ACTIVE, now, false, time.Time{}, nil)
			}
			inst.Addr = f.addr
			inst.Zone = f.zone
			inst.State = ring.ACTIVE
			inst.Timestamp = now.Unix()
			desc.Ingesters[f.id] = inst
		}
		return desc, true, nil
	})
	if err != nil {
		return err
	}

	byCompartment := map[int][]*fake{}
	for _, f := range r.fakes {
		byCompartment[f.compartment] = append(byCompartment[f.compartment], f)
	}
	var errs []error
	for c, fs := range byCompartment {
		key := partitionRingKey(r.compartmentsOn, c)
		err := r.partitions.CAS(ctx, key, func(in any) (any, bool, error) {
			desc := ring.GetOrCreatePartitionRingDesc(in)
			changed := false
			for _, f := range fs {
				pid := int32(f.partition)
				p, ok := desc.Partitions[pid]
				if !ok {
					// Stored spread-minimizing tokens: the read path skips tokenless partitions.
					desc.AddPartition(pid, ring.PartitionActive, now)
					changed = true
				} else if p.State != ring.PartitionActive {
					if _, err := desc.UpdatePartitionState(pid, ring.PartitionActive, now); err != nil {
						return nil, false, err
					}
					changed = true
				}
				if desc.AddOrUpdateOwner(f.id, ring.OwnerActive, pid, now) {
					changed = true
				}
			}
			if !changed {
				// Nothing to update. memberlist rejects a CAS that changes nothing, so return nil to skip it.
				return nil, false, nil
			}
			return desc, true, nil
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key, err))
		}
	}
	return errors.Join(errs...)
}

func (r *registrar) run(ctx context.Context, period time.Duration) {
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		if err := r.heartbeat(ctx); err != nil {
			level.Warn(r.logger).Log("msg", "ring heartbeat failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// unregister removes this process's instances and owners. Partitions are left ACTIVE: queriers
// fail queries for ACTIVE partitions with no healthy owner, so restarting fakes would cause errors
// until they re-register. Pass removePartitions to delete them too, on final teardown.
func (r *registrar) unregister(ctx context.Context, removePartitions bool) error {
	err := r.instances.CAS(ctx, instanceRingKey, func(in any) (any, bool, error) {
		desc := ring.GetOrCreateRingDesc(in)
		for _, f := range r.fakes {
			desc.RemoveIngester(f.id)
		}
		return desc, true, nil
	})
	if err != nil {
		return err
	}
	byCompartment := map[int][]*fake{}
	for _, f := range r.fakes {
		byCompartment[f.compartment] = append(byCompartment[f.compartment], f)
	}
	for c, fs := range byCompartment {
		err := r.partitions.CAS(ctx, partitionRingKey(r.compartmentsOn, c), func(in any) (any, bool, error) {
			desc := ring.GetOrCreatePartitionRingDesc(in)
			for _, f := range fs {
				desc.RemoveOwner(f.id)
				if removePartitions {
					desc.RemovePartition(int32(f.partition))
				}
			}
			return desc, true, nil
		})
		if err != nil {
			return err
		}
	}
	return nil
}
