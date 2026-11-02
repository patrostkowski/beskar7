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
	"net/http"

	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/projectbeskar/beskar7/internal/auth"
)

// networkDataRoute is the callback route NetworkDataHandler serves (contract §4.6).
const networkDataRoute = "GET /api/v1/networkdata/{namespace}/{hostName}"

// NetworkDataHandler serves the network data Secret a PhysicalHost names in
// spec.networkDataSecretRef, verbatim, to the inspector holding the host's
// bearer token (contract v4.3, §4.6).
//
// Responses:
//   - 200: the Secret's networkData bytes.
//   - 404: the host names no Secret. An older controller without the route
//     answers the same, by design.
//   - 409: the host names a Secret that cannot be served (loadNetworkData), so
//     the inspector refuses to install the host without its network.
//   - 401: the token does not match the credentials bound to the host's
//     current claim, as from the bearer middleware.
//   - 500: the Secret could not be read; the inspector retries.
//
// The checks are repeated here at serve time rather than pinned at the
// Beskar7Machine's gate: an edit between the gate and the fetch is served as
// edited, like an edit to the bootstrap data Secret. The bytes are never
// logged.
type NetworkDataHandler struct {
	Client client.Client
	Log    logr.Logger
}

// ServeHTTP serves the network data of the host the URL path names.
func (h *NetworkDataHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log := h.Log.WithValues("method", r.Method, "namespace", r.PathValue("namespace"),
		"host", r.PathValue("hostName"), "remote", r.RemoteAddr)

	// A 404 here would read as "no network data" and install the host without
	// it, so a failed walk answers like a failed bearer check.
	bound, err := resolveBoundCallbackHost(r, h.Client)
	if err != nil {
		log.V(1).Info("networkdata GET: the host's bound credentials do not authorise this request", "err", err.Error())
		auth.WriteUnauthorized(w)
		return
	}
	if bound.host.Spec.NetworkDataSecretRef == "" {
		log.V(1).Info("networkdata GET: the host names no network data Secret")
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	data, err := loadNetworkData(r.Context(), h.Client, bound.host)
	var ndErr *networkDataError
	switch {
	case errors.As(err, &ndErr):
		log.Info("networkdata GET: the host's network data Secret cannot be served", "reason", ndErr.reason, "err", ndErr.message)
		http.Error(w, "network data unavailable", http.StatusConflict)
		return
	case err != nil:
		log.Error(err, "networkdata GET: failed to read the network data Secret")
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(data); err != nil {
		log.V(1).Info("networkdata GET: response write failed", "err", err.Error())
		return
	}
	log.V(1).Info("networkdata GET: served network data")
}
