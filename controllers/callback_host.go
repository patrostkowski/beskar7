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
	"errors"
	"fmt"
	"net/http"
	"time"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	infrav1 "github.com/projectbeskar/beskar7/api/v1beta2"
	"github.com/projectbeskar/beskar7/internal/auth"
)

// errCallbackTokenStale is returned when the request's bearer token does not
// match the credentials bound to the host's current claim.
var errCallbackTokenStale = errors.New("bearer token does not match the host's current credentials")

// boundCallbackHost is a host a GET callback serves from, read after the
// bearer middleware ran, with the Beskar7Machine its credentials are bound to.
type boundCallbackHost struct {
	host     *infrav1.PhysicalHost
	consumer types.NamespacedName
}

// resolveBoundCallbackHost re-reads the PhysicalHost the request path names
// and checks the request's bearer token again against the credentials bound
// to its current claim (boundBootstrapCredentials).
//
// The bearer verifier already did both, but against its own reads: a
// ConsumerRef re-pointed, or a release and re-mint landing, after it ran would
// otherwise pair the old token with the new claim. Every GET that serves data
// for a claim walks from here, so a change to the binding rules changes all of
// them. The error is for server-side logs only and carries no token material.
func resolveBoundCallbackHost(r *http.Request, c client.Reader) (boundCallbackHost, error) {
	namespace := r.PathValue("namespace")
	hostName := r.PathValue("hostName")
	if namespace == "" || hostName == "" {
		return boundCallbackHost{}, fmt.Errorf("missing namespace or hostName in request path")
	}
	host := &infrav1.PhysicalHost{}
	if err := c.Get(r.Context(), types.NamespacedName{Namespace: namespace, Name: hostName}, host); err != nil {
		return boundCallbackHost{}, fmt.Errorf("get PhysicalHost: %w", err)
	}
	creds, consumer, err := boundBootstrapCredentials(r.Context(), c, host)
	if err != nil {
		return boundCallbackHost{}, err
	}
	token, ok := auth.BearerToken(r)
	if !ok || !creds.tokenValid(time.Now()) || !auth.Verify(token, auth.Hash(creds.token)) {
		return boundCallbackHost{}, errCallbackTokenStale
	}
	return boundCallbackHost{host: host, consumer: consumer}, nil
}
