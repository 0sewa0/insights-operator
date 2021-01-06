package periodic

import (
	"context"
	"fmt"
	"sort"
	"time"

	"k8s.io/klog/v2"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/openshift/insights-operator/pkg/config"
	"github.com/openshift/insights-operator/pkg/controllerstatus"
	"github.com/openshift/insights-operator/pkg/gather"
	"github.com/openshift/insights-operator/pkg/record"
)

type Configurator interface {
	Config() *config.Controller
	ConfigChanged() (<-chan struct{}, func())
}

type Controller struct {
	configurator Configurator
	recorder     record.FlushInterface
	gatherers    map[string]gather.Interface
	statuses     map[string]*controllerstatus.Simple

	workers		 int  // Not used, but I have plans for it
	initialDelay time.Duration

}

func New(configurator Configurator, recorder record.FlushInterface, gatherers map[string]gather.Interface) *Controller {
	statuses := make(map[string]*controllerstatus.Simple)
	for k := range gatherers {
		statuses[k] = &controllerstatus.Simple{Name: fmt.Sprintf("periodic-%s", k)}
	}
	c := &Controller{
		configurator: configurator,
		recorder:     recorder,
		gatherers:    gatherers,
		statuses:     statuses,
	}
	return c
}

func (c *Controller) Sources() []controllerstatus.Interface {
	keys := make([]string, 0, len(c.statuses))
	for key := range c.statuses {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	sources := make([]controllerstatus.Interface, 0, len(keys))
	for _, key := range keys {
		sources = append(sources, c.statuses[key])
	}
	return sources
}


func (c *Controller) Run(workers int, stopCh <-chan struct{}, initialDelay time.Duration) {
	defer utilruntime.HandleCrash()
	defer klog.Info("Shutting down")
	c.workers = workers
	c.initialDelay = initialDelay

	go wait.Until(func() { c.periodicTrigger(stopCh) }, time.Second, stopCh)

	<-stopCh
}

func (c *Controller) Gather() {
	for name := range c.gatherers {
		start := time.Now()
		err := c.runGatherer(name)
		if err == nil {
			klog.V(4).Infof("Periodic gather %s completed in %s", name, time.Since(start).Truncate(time.Millisecond))
			c.statuses[name].UpdateStatus(controllerstatus.Summary{Healthy: true})
			return
		}
		utilruntime.HandleError(fmt.Errorf("%v failed after %s with: %v", name, time.Since(start).Truncate(time.Millisecond), err))
		c.statuses[name].UpdateStatus(controllerstatus.Summary{Reason: "PeriodicGatherFailed", Message: fmt.Sprintf("Source %s could not be retrieved: %v", name, err)})
	}
}


func (c *Controller) runGatherer(name string) error {
	gatherer, ok := c.gatherers[name]
	if !ok {
		klog.V(2).Infof("No such gatherer %s", name)
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.configurator.Config().Interval/2)
	defer cancel()
	defer func() {
		if err := c.recorder.Flush(ctx); err != nil {
			klog.Errorf("Unable to flush recorder: %v", err)
		}
	}()
	klog.V(4).Infof("Running %s", name)
	return gatherer.Gather(ctx, c.configurator.Config().Gather, c.recorder)
}

func (c *Controller) periodicTrigger(stopCh <-chan struct{}) {
	configCh, closeFn := c.configurator.ConfigChanged()
	defer closeFn()

	if c.initialDelay > 0 {
		select {
		case <-stopCh:
			return
		case <-time.After(c.initialDelay):
			c.initialDelay = 0
			c.Gather()
		}
	}

	interval := c.configurator.Config().Interval
	expireCh := time.After(wait.Jitter(interval, 0.5))
	klog.Infof("Gathering cluster info every %s", interval)
	for {
		select {
		case <-stopCh:
			return

		case <-configCh:
			newInterval := c.configurator.Config().Interval
			if newInterval == interval {
				continue
			}
			interval = newInterval
			klog.Infof("Gathering cluster info every %s", interval)

		case <-time.After(wait.Jitter(interval/4, 2)):
			c.Gather()

		case <-expireCh:
		}
		expireCh = time.After(wait.Jitter(interval, 0.5))
	}
}
