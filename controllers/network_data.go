/*
Copyright 2024 The Beskar7 Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/cluster-api/util/conditions"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infrav1 "github.com/projectbeskar/beskar7/api/v1beta2"
)

// networkDataError is a Secret that cannot be served, with the
// NetworkDataReady reason that names why. Its message names the Secret and
// never its content.
type networkDataError struct {
	reason  string
	message string
}

func (e *networkDataError) Error() string { return e.message }

// loadNetworkData returns the network data of the Secret host's
// NetworkDataSecretRef names, or nil when it names none. The bytes are checked
// only for presence and size, never parsed. A Secret that cannot be served
// returns a *networkDataError; any other error is a failed read.
func loadNetworkData(ctx context.Context, c client.Reader, host *infrav1.PhysicalHost) ([]byte, error) {
	name := host.Spec.NetworkDataSecretRef
	if name == "" {
		return nil, nil
	}
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: host.Namespace, Name: name}, secret); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("get network data Secret %s: %w", name, err)
		}
		return nil, &networkDataError{
			reason:  infrav1.NetworkDataSecretNotFoundReason,
			message: fmt.Sprintf("network data Secret %q not found", name),
		}
	}
	if secret.Annotations[NetworkDataHostAnnotation] != host.Name {
		return nil, &networkDataError{
			reason:  infrav1.NetworkDataSecretNotOptedInReason,
			message: fmt.Sprintf("network data Secret %q does not name host %q in its %s annotation", name, host.Name, NetworkDataHostAnnotation),
		}
	}
	data := secret.Data[infrav1.NetworkDataSecretKey]
	if len(data) == 0 {
		return nil, &networkDataError{
			reason:  infrav1.NetworkDataKeyMissingReason,
			message: fmt.Sprintf("network data Secret %q has no %q key", name, infrav1.NetworkDataSecretKey),
		}
	}
	if len(data) > infrav1.MaxNetworkDataSize {
		return nil, &networkDataError{
			reason:  infrav1.NetworkDataTooLargeReason,
			message: fmt.Sprintf("network data Secret %q holds %d bytes, over the %d byte limit", name, len(data), infrav1.MaxNetworkDataSize),
		}
	}
	return data, nil
}

// reconcileNetworkData sets NetworkDataReady from the Secret the host names,
// and removes the condition when the host names none. A failed read leaves the
// condition as it was; the Secret watch or the next resync retries.
func (r *PhysicalHostReconciler) reconcileNetworkData(ctx context.Context, logger logr.Logger, host *infrav1.PhysicalHost) {
	if host.Spec.NetworkDataSecretRef == "" {
		conditions.Delete(host, infrav1.NetworkDataReadyCondition)
		return
	}
	_, err := loadNetworkData(ctx, r.Client, host)
	var ndErr *networkDataError
	switch {
	case err == nil:
		setTrue(host, infrav1.NetworkDataReadyCondition, infrav1.NetworkDataReadyReason)
	case errors.As(err, &ndErr):
		setFalse(host, infrav1.NetworkDataReadyCondition, ndErr.reason, "%s", ndErr.message)
	default:
		logger.Error(err, "Failed to read the network data Secret", "secret", host.Spec.NetworkDataSecretRef)
	}
}

// hostWaitingForNetworkData reports whether host names a network data Secret
// that its NetworkDataReady condition does not report ready for the current
// spec. A condition from an earlier generation may describe a Secret the host
// no longer names, so it does not count.
func hostWaitingForNetworkData(host *infrav1.PhysicalHost) bool {
	if host.Spec.NetworkDataSecretRef == "" {
		return false
	}
	cond := conditions.Get(host, infrav1.NetworkDataReadyCondition)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		return true
	}
	return cond.ObservedGeneration != host.Generation
}

// networkDataWaitMessage says why a claim of host waits for its network data.
func networkDataWaitMessage(host *infrav1.PhysicalHost) string {
	cond := conditions.Get(host, infrav1.NetworkDataReadyCondition)
	if cond == nil || cond.Status == metav1.ConditionTrue {
		return fmt.Sprintf("Waiting for PhysicalHost %q to validate its network data Secret %q", host.Name, host.Spec.NetworkDataSecretRef)
	}
	return fmt.Sprintf("Waiting for PhysicalHost %q network data: %s", host.Name, cond.Message)
}
