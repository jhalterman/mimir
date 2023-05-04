// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ruler/manager.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package ruler

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/concurrency"
	"github.com/grafana/dskit/services"
	ot "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/rulefmt"
	"github.com/prometheus/prometheus/notifier"
	promRules "github.com/prometheus/prometheus/rules"
	"github.com/weaveworks/common/httpgrpc/server"
	"github.com/weaveworks/common/user"
	"go.uber.org/atomic"
	"golang.org/x/net/context/ctxhttp"

	"github.com/grafana/mimir/pkg/alertmanager"
	"github.com/grafana/mimir/pkg/alertmanager/alertmanagerdiscovery"
	"github.com/grafana/mimir/pkg/ruler/rulespb"
	"github.com/grafana/mimir/pkg/util/httpgrpcutil"
	"github.com/grafana/mimir/pkg/util/servicediscovery"
)

type DefaultMultiTenantManager struct {
	services.Service
	cfg Config

	// Alertmanager discovery related fields
	discoveryMtx           sync.Mutex                  // Guards dynamic config changes
	discoveryConfigs       map[string]discovery.Config // Guarded by discoveryMtx
	alertmanagerHTTPPrefix string                      // Used with ring based discovery
	discoveryService       services.Service
	discoveryWatcher       *services.FailureWatcher

	managerFactory ManagerFactory
	mapper         *mapper

	// Struct for holding per-user Prometheus rules Managers.
	userManagerMtx sync.RWMutex
	userManagers   map[string]RulesManager

	// Prometheus rules managers metrics.
	userManagerMetrics *ManagerMetrics

	// Per-user notifiers with separate queues.
	notifiersMtx sync.Mutex                // Guards notifiers and their config
	notifierCfg  *config.Config            // Guarded by notifiersMtx
	notifiers    map[string]*rulerNotifier // Guarded by notifiersMtx

	managersTotal                 prometheus.Gauge
	lastReloadSuccessful          *prometheus.GaugeVec
	lastReloadSuccessfulTimestamp *prometheus.GaugeVec
	configUpdatesTotal            *prometheus.CounterVec
	registry                      prometheus.Registerer
	logger                        log.Logger

	rulerIsRunning atomic.Bool
}

func NewDefaultMultiTenantManager(cfg Config, alertmanagerHTTPPrefix string, managerFactory ManagerFactory, reg prometheus.Registerer, logger log.Logger) (*DefaultMultiTenantManager, error) {
	userManagerMetrics := NewManagerMetrics(logger)
	if reg != nil {
		reg.MustRegister(userManagerMetrics)
	}

	m := &DefaultMultiTenantManager{
		cfg:                    cfg,
		discoveryConfigs:       make(map[string]discovery.Config),
		alertmanagerHTTPPrefix: alertmanagerHTTPPrefix,
		discoveryWatcher:       services.NewFailureWatcher(),
		managerFactory:         managerFactory,
		notifiers:              map[string]*rulerNotifier{},
		mapper:                 newMapper(cfg.RulePath, logger),
		userManagers:           map[string]RulesManager{},
		userManagerMetrics:     userManagerMetrics,
		managersTotal: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "ruler_managers_total",
			Help:      "Total number of managers registered and running in the ruler",
		}),
		lastReloadSuccessful: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "ruler_config_last_reload_successful",
			Help:      "Boolean set to 1 whenever the last configuration reload attempt was successful.",
		}, []string{"user"}),
		lastReloadSuccessfulTimestamp: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "cortex",
			Name:      "ruler_config_last_reload_successful_seconds",
			Help:      "Timestamp of the last successful configuration reload.",
		}, []string{"user"}),
		configUpdatesTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "ruler_config_updates_total",
			Help:      "Total number of config updates triggered by a user",
		}, []string{"user"}),
		registry: reg,
		logger:   logger,
	}

	var err error
	switch cfg.AlertManagerDiscovery.Mode {
	case alertmanagerdiscovery.ModeRing:
		level.Info(logger).Log("msg", "using ring based alert manager discovery")
		m.discoveryService, err = alertmanager.NewRing(cfg.AlertManagerRing, "ruler", m, logger, reg)
		if err != nil {
			return nil, err
		}

	default:
		level.Info(logger).Log("msg", "using dns based alert manager discovery")
		dnsProvider := alertmanagerdiscovery.NewDNSProvider(reg)
		err = alertmanagerdiscovery.BuildDiscoveryConfigs(cfg.AlertmanagerURL, m.discoveryConfigs, cfg.AlertmanagerRefreshInterval, dnsProvider)
		if err != nil {
			return nil, err
		}
	}

	m.notifierCfg, err = buildNotifierConfig(&cfg, m.discoveryConfigs)
	if err != nil {
		return nil, err
	}

	m.Service = services.NewBasicService(m.starting, m.running, m.stopping)
	return m, nil
}

func (r *DefaultMultiTenantManager) starting(ctx context.Context) error {
	if r.discoveryService != nil {
		r.discoveryWatcher.WatchService(r.discoveryService)
		return services.StartAndAwaitRunning(ctx, r.discoveryService)
	}
	return nil
}

func (r *DefaultMultiTenantManager) running(ctx context.Context) error {
	if r.discoveryService != nil {
		select {
		case <-ctx.Done():
			return nil
		case err := <-r.discoveryWatcher.Chan():
			return errors.Wrap(err, "alertmanager watcher subservice failed")
		}
	} else {
		<-ctx.Done()
		return nil
	}
}

func (r *DefaultMultiTenantManager) stopping(_ error) error {
	if r.discoveryService != nil {
		return services.StopAndAwaitTerminated(context.Background(), r.discoveryService)
	}
	return nil
}

// SyncFullRuleGroups implements MultiTenantManager.
// It's not safe to call this function concurrently with SyncFullRuleGroups() or SyncPartialRuleGroups().
func (r *DefaultMultiTenantManager) SyncFullRuleGroups(ctx context.Context, ruleGroupsByUser map[string]rulespb.RuleGroupList) {
	if !r.cfg.TenantFederation.Enabled {
		removeFederatedRuleGroups(ruleGroupsByUser, r.logger)
	}

	if err := r.syncRulesToManagerConcurrently(ctx, ruleGroupsByUser); err != nil {
		// We don't log it because the only error we could get here is a context canceled.
		return
	}

	// Check for deleted users and remove them.
	r.removeUsersIf(func(userID string) bool {
		_, exists := ruleGroupsByUser[userID]
		return !exists
	})
}

// SyncPartialRuleGroups implements MultiTenantManager.
// It's not safe to call this function concurrently with SyncFullRuleGroups() or SyncPartialRuleGroups().
func (r *DefaultMultiTenantManager) SyncPartialRuleGroups(ctx context.Context, ruleGroupsByUser map[string]rulespb.RuleGroupList) {
	if !r.cfg.TenantFederation.Enabled {
		removeFederatedRuleGroups(ruleGroupsByUser, r.logger)
	}

	// Filter out tenants with no rule groups.
	ruleGroupsByUser, removedUsers := filterRuleGroupsByNotEmptyUsers(ruleGroupsByUser)

	if err := r.syncRulesToManagerConcurrently(ctx, ruleGroupsByUser); err != nil {
		// We don't log it because the only error we could get here is a context canceled.
		return
	}

	// Check for deleted users and remove them.
	r.removeUsersIf(func(userID string) bool {
		_, removed := removedUsers[userID]
		return removed
	})
}

func (r *DefaultMultiTenantManager) Start() {
	r.userManagerMtx.Lock()
	defer r.userManagerMtx.Unlock()

	// Skip starting the user managers if the ruler is already running.
	if r.rulerIsRunning.Load() {
		return
	}

	for _, mngr := range r.userManagers {
		go mngr.Run()
	}
	// set rulerIsRunning to true once user managers are started.
	r.rulerIsRunning.Store(true)
}

// syncRulesToManagerConcurrently calls syncRulesToManager() concurrently for each user in the input
// ruleGroups. The max concurrency is limited. This function is expected to return an error only if
// the input ctx is canceled.
func (r *DefaultMultiTenantManager) syncRulesToManagerConcurrently(ctx context.Context, ruleGroups map[string]rulespb.RuleGroupList) error {
	// Sync the rules to disk and then update the user's Prometheus Rules Manager.
	// Since users are different, we can sync rules in parallel.
	users := make([]string, 0, len(ruleGroups))
	for userID := range ruleGroups {
		users = append(users, userID)
	}

	// concurrenty.ForEachJob is a helper function that runs a function for each job in parallel.
	// It cancel context of jobFunc once iteration is done.
	// That is why the context passed to syncRulesToManager should be the global context not the context of jobFunc.
	err := concurrency.ForEachJob(ctx, len(users), 10, func(_ context.Context, idx int) error {
		userID := users[idx]
		r.syncRulesToManager(ctx, userID, ruleGroups[userID])
		return nil
	})

	// Update the metric even in case of error.
	r.userManagerMtx.RLock()
	r.managersTotal.Set(float64(len(r.userManagers)))
	r.userManagerMtx.RUnlock()

	return err
}

// InstanceAdded handles the addition of an instance to the alertmanager ring.
func (r *DefaultMultiTenantManager) InstanceAdded(instance servicediscovery.Instance) {
	if instance.InUse {
		r.discoveryMtx.Lock()
		defer r.discoveryMtx.Unlock()
		level.Info(r.logger).Log("msg", "adding alertmanager instance", "addr", instance.Address)
		r.discoveryConfigs[r.alertManagerHTTPAddress(instance)] = alertmanagerdiscovery.NewDiscoveryConfig(instance.Address)
		r.updateNotifierConfig()
	}
}

// InstanceRemoved handles the removal of an instance from the alertmanager ring.
func (r *DefaultMultiTenantManager) InstanceRemoved(instance servicediscovery.Instance) {
	r.discoveryMtx.Lock()
	defer r.discoveryMtx.Unlock()
	level.Info(r.logger).Log("msg", "removing alertmanager instance", "addr", instance.Address)
	delete(r.discoveryConfigs, r.alertManagerHTTPAddress(instance))
	r.updateNotifierConfig()
}

// InstanceChanged handles a change to an instance in the alertmanager ring.
func (r *DefaultMultiTenantManager) InstanceChanged(instance servicediscovery.Instance) {
	if instance.InUse {
		r.InstanceAdded(instance)
	} else {
		r.InstanceRemoved(instance)
	}
}

// alertManagerHTTPAddress returns an alert manager HTTP address for the instance.
func (r *DefaultMultiTenantManager) alertManagerHTTPAddress(instance servicediscovery.Instance) string {
	return "http://" + instance.Address + r.alertmanagerHTTPPrefix
}

// updateNotifierConfig builds a new notifier config and applies it to existing notifiers.
// Must lock discoveryMtx prior to calling.
func (r *DefaultMultiTenantManager) updateNotifierConfig() {
	ncfg, err := buildNotifierConfig(&r.cfg, r.discoveryConfigs)
	if err != nil {
		level.Error(r.logger).Log("msg", "unable to build updated notifier config", "err", err)
		return
	}
	r.notifierCfg = ncfg

	r.notifiersMtx.Lock()
	defer r.notifiersMtx.Unlock()
	for _, n := range r.notifiers {
		if err = n.applyConfig(r.notifierCfg); err != nil {
			level.Error(r.logger).Log("msg", "unable to update notifier config", "err", err)
		}
	}
}

// syncRulesToManager maps the rule files to disk, detects any changes and will create/update
// the user's Prometheus Rules Manager. Since this method writes to disk it is not safe to call
// concurrently for the same user.
func (r *DefaultMultiTenantManager) syncRulesToManager(ctx context.Context, user string, groups rulespb.RuleGroupList) {
	// Map the files to disk and return the file names to be passed to the users manager if they
	// have been updated
	update, files, err := r.mapper.MapRules(user, groups.Formatted())
	if err != nil {
		r.lastReloadSuccessful.WithLabelValues(user).Set(0)
		level.Error(r.logger).Log("msg", "unable to map rule files", "user", user, "err", err)
		return
	}

	manager, created, err := r.getOrCreateManager(ctx, user)
	if err != nil {
		r.lastReloadSuccessful.WithLabelValues(user).Set(0)
		level.Error(r.logger).Log("msg", "unable to create rule manager", "user", user, "err", err)
		return
	}

	// We need to update the manager only if it was just created or rules on disk have changed.
	if !(created || update) {
		level.Debug(r.logger).Log("msg", "rules have not changed, skipping rule manager update", "user", user)
		return
	}

	level.Debug(r.logger).Log("msg", "updating rules", "user", user)
	r.configUpdatesTotal.WithLabelValues(user).Inc()

	err = manager.Update(r.cfg.EvaluationInterval, files, labels.EmptyLabels(), r.cfg.ExternalURL.String(), nil)
	if err != nil {
		r.lastReloadSuccessful.WithLabelValues(user).Set(0)
		level.Error(r.logger).Log("msg", "unable to update rule manager", "user", user, "err", err)
		return
	}

	r.lastReloadSuccessful.WithLabelValues(user).Set(1)
	r.lastReloadSuccessfulTimestamp.WithLabelValues(user).SetToCurrentTime()
}

// getOrCreateManager retrieves the user manager. If it doesn't exist, it will create and start it first.
func (r *DefaultMultiTenantManager) getOrCreateManager(ctx context.Context, user string) (RulesManager, bool, error) {
	// Check if it already exists. Since rules are synched frequently, we expect to already exist
	// most of the times.
	r.userManagerMtx.RLock()
	manager, exists := r.userManagers[user]
	r.userManagerMtx.RUnlock()

	if exists {
		return manager, false, nil
	}

	// The manager doesn't exist. We take an exclusive lock to create it.
	r.userManagerMtx.Lock()
	defer r.userManagerMtx.Unlock()

	// Ensure it hasn't been created in the meanwhile.
	manager, exists = r.userManagers[user]
	if exists {
		return manager, false, nil
	}

	level.Debug(r.logger).Log("msg", "creating rule manager for user", "user", user)
	manager, err := r.newManager(ctx, user)
	if err != nil {
		return nil, false, err
	}

	// manager.Run() starts running the manager and blocks until Stop() is called.
	// Hence run it as another goroutine.
	// We only start the rule manager if the ruler is in running state.
	if r.rulerIsRunning.Load() {
		go manager.Run()
	}

	r.userManagers[user] = manager
	return manager, true, nil
}

// newManager creates a prometheus rule manager wrapped with a user id
// configured storage, appendable, notifier, and instrumentation
func (r *DefaultMultiTenantManager) newManager(ctx context.Context, userID string) (RulesManager, error) {
	notifier, err := r.getOrCreateNotifier(userID)
	if err != nil {
		return nil, err
	}

	// Create a new Prometheus registry and register it within
	// our metrics struct for the provided user.
	reg := prometheus.NewRegistry()
	r.userManagerMetrics.AddUserRegistry(userID, reg)

	return r.managerFactory(ctx, userID, notifier, r.logger, reg), nil
}

func (r *DefaultMultiTenantManager) getOrCreateNotifier(userID string) (*notifier.Manager, error) {
	r.notifiersMtx.Lock()
	defer r.notifiersMtx.Unlock()

	n, ok := r.notifiers[userID]
	if ok {
		return n.notifier, nil
	}

	reg := prometheus.WrapRegistererWith(prometheus.Labels{"user": userID}, r.registry)
	reg = prometheus.WrapRegistererWithPrefix("cortex_", reg)
	n = newRulerNotifier(&notifier.Options{
		QueueCapacity: r.cfg.NotificationQueueCapacity,
		Registerer:    reg,
		Do: func(ctx context.Context, client *http.Client, req *http.Request) (*http.Response, error) {
			// Note: The passed-in context comes from the Prometheus notifier
			// and does *not* contain the userID. So it needs to be added to the context
			// here before using the context to inject the userID into the HTTP request.
			ctx = user.InjectOrgID(ctx, userID)
			if err := user.InjectOrgIDIntoHTTPRequest(ctx, req); err != nil {
				return nil, err
			}
			// Jaeger complains the passed-in context has an invalid span ID, so start a new root span
			sp := ot.GlobalTracer().StartSpan("notify", ot.Tag{Key: "organization", Value: userID})
			defer sp.Finish()
			ctx = ot.ContextWithSpan(ctx, sp)
			_ = ot.GlobalTracer().Inject(sp.Context(), ot.HTTPHeaders, ot.HTTPHeadersCarrier(req.Header))

			// When ring discovery mode is enabled, the address we discover is the alertmanager's GRPC address
			// So we need to convert the request to GRPC before sending
			if r.cfg.AlertManagerDiscovery.Mode == alertmanagerdiscovery.ModeRing {
				grpcReq, err := server.HTTPRequest(req)
				if err != nil {
					return nil, err
				}
				grpcReq.Url = req.URL.String()
				grpcClient, err := httpgrpcutil.NewGRPCClient(req.Host)
				if err != nil {
					return nil, err
				}
				grpcResponse, err := grpcClient.Handle(ctx, grpcReq)
				if err != nil {
					return nil, errors.Wrap(err, "failed to send notification via grpc")
				}
				return httpgrpcutil.GrpcToHTTPResponse(grpcResponse), nil
			}

			return ctxhttp.Do(ctx, client, req)
		},
	}, log.With(r.logger, "user", userID))

	n.run()

	// This should never fail, unless there's a programming mistake.
	if err := n.applyConfig(r.notifierCfg); err != nil {
		return nil, err
	}

	r.notifiers[userID] = n
	return n.notifier, nil
}

// removeUsersIf stops the manager and cleanup the resources for each user for which
// the input shouldRemove() function returns true.
func (r *DefaultMultiTenantManager) removeUsersIf(shouldRemove func(userID string) bool) {
	r.userManagerMtx.Lock()
	defer r.userManagerMtx.Unlock()

	// Check for deleted users and remove them
	for userID, mngr := range r.userManagers {
		if !shouldRemove(userID) {
			continue
		}

		go mngr.Stop()
		delete(r.userManagers, userID)

		r.mapper.cleanupUser(userID)
		r.lastReloadSuccessful.DeleteLabelValues(userID)
		r.lastReloadSuccessfulTimestamp.DeleteLabelValues(userID)
		r.configUpdatesTotal.DeleteLabelValues(userID)
		r.userManagerMetrics.RemoveUserRegistry(userID)
		level.Info(r.logger).Log("msg", "deleted rule manager and local rule files", "user", userID)
	}

	r.managersTotal.Set(float64(len(r.userManagers)))
}

func (r *DefaultMultiTenantManager) GetRules(userID string) []*promRules.Group {
	r.userManagerMtx.RLock()
	mngr, exists := r.userManagers[userID]
	r.userManagerMtx.RUnlock()

	if exists {
		return mngr.RuleGroups()
	}
	return nil
}

func (r *DefaultMultiTenantManager) Stop() {
	r.notifiersMtx.Lock()
	for _, n := range r.notifiers {
		n.stop()
	}
	r.notifiersMtx.Unlock()

	level.Info(r.logger).Log("msg", "stopping user managers")
	wg := sync.WaitGroup{}
	r.userManagerMtx.Lock()
	for userID, manager := range r.userManagers {
		level.Debug(r.logger).Log("msg", "shutting down user manager", "user", userID)
		wg.Add(1)
		go func(manager RulesManager, user string) {
			manager.Stop()
			wg.Done()
			level.Debug(r.logger).Log("msg", "user manager shut down", "user", user)
		}(manager, userID)
		delete(r.userManagers, userID)
	}
	wg.Wait()
	r.userManagerMtx.Unlock()
	level.Info(r.logger).Log("msg", "all user managers stopped")

	// cleanup user rules directories
	r.mapper.cleanup()
}

func (r *DefaultMultiTenantManager) ValidateRuleGroup(g rulefmt.RuleGroup) []error {
	var errs []error

	if g.Name == "" {
		errs = append(errs, errors.New("invalid rules configuration: rule group name must not be empty"))
		return errs
	}

	if len(g.Rules) == 0 {
		errs = append(errs, fmt.Errorf("invalid rules configuration: rule group '%s' has no rules", g.Name))
		return errs
	}

	if !r.cfg.TenantFederation.Enabled && len(g.SourceTenants) > 0 {
		errs = append(errs, fmt.Errorf("invalid rules configuration: rule group '%s' is a federated rule group, "+
			"but rules federation is disabled; please contact your service administrator to have it enabled", g.Name))
	}

	for i, r := range g.Rules {
		for _, err := range r.Validate() {
			var ruleName string
			if r.Alert.Value != "" {
				ruleName = r.Alert.Value
			} else {
				ruleName = r.Record.Value
			}
			errs = append(errs, &rulefmt.Error{
				Group:    g.Name,
				Rule:     i,
				RuleName: ruleName,
				Err:      err,
			})
		}
	}

	return errs
}

// filterRuleGroupsByNotEmptyUsers filters out all the tenants that have no rule groups.
// The returned removed map may be nil if no user was removed from the input configs.
//
// This function doesn't modify the input configs in place (even if it could) in order to reduce the likelihood of introducing
// future bugs, in case the rule groups will be cached in memory.
func filterRuleGroupsByNotEmptyUsers(configs map[string]rulespb.RuleGroupList) (filtered map[string]rulespb.RuleGroupList, removed map[string]struct{}) {
	// Find tenants to remove.
	for userID, ruleGroups := range configs {
		if len(ruleGroups) > 0 {
			continue
		}

		// Ensure the map is initialised.
		if removed == nil {
			removed = make(map[string]struct{})
		}

		removed[userID] = struct{}{}
	}

	// Nothing to do if there are no users to remove.
	if len(removed) == 0 {
		return configs, removed
	}

	// Filter out tenants to remove.
	filtered = make(map[string]rulespb.RuleGroupList, len(configs)-len(removed))
	for userID, ruleGroups := range configs {
		if _, isRemoved := removed[userID]; !isRemoved {
			filtered[userID] = ruleGroups
		}
	}

	return filtered, removed
}
