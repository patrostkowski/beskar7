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
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/cluster-api/util/conditions"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	infrav1 "github.com/projectbeskar/beskar7/api/v1beta2"
	"github.com/projectbeskar/beskar7/internal/auth"
)

const (
	ndTestNamespace = "nd-test"
	ndTestHost      = "host-01"
	ndTestSecret    = "host-01-network-data"
	ndTestMachine   = "b7m-01"
	ndTestToken     = "nd-test-token"
	ndTestNetplan   = "network:\n  version: 2\n"
)

// ndTestHostObject returns a host claimed by ndTestMachine that names
// secretRef as its network data Secret.
func ndTestHostObject(secretRef string) *infrav1.PhysicalHost {
	return &infrav1.PhysicalHost{
		ObjectMeta: metav1.ObjectMeta{Name: ndTestHost, Namespace: ndTestNamespace, UID: "host-uid", Generation: 1},
		Spec: infrav1.PhysicalHostSpec{
			RedfishConnection: infrav1.RedfishConnection{Address: "https://192.0.2.10", CredentialsSecretRef: "bmc-creds"},
			ConsumerRef: &corev1.ObjectReference{
				Kind: "Beskar7Machine", APIVersion: InfrastructureAPIVersion,
				Name: ndTestMachine, Namespace: ndTestNamespace,
			},
			NetworkDataSecretRef: secretRef,
		},
	}
}

// ndTestSecretObject returns the network data Secret annotated for host
// (none when host is empty) and holding data under the networkData key (no
// key when data is nil).
func ndTestSecretObject(host string, data []byte) *corev1.Secret {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: ndTestSecret, Namespace: ndTestNamespace}}
	if host != "" {
		secret.Annotations = map[string]string{NetworkDataHostAnnotation: host}
	}
	if data != nil {
		secret.Data = map[string][]byte{infrav1.NetworkDataSecretKey: data}
	}
	return secret
}

// ndTestCredentialSecret returns host's bootstrap-token Secret, owned by host
// and bound to ndTestMachine with ndTestToken.
func ndTestCredentialSecret(host *infrav1.PhysicalHost) *corev1.Secret {
	isController := true
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: bootstrapTokenSecretName(host.Name), Namespace: host.Namespace,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: infrav1.GroupVersion.String(), Kind: "PhysicalHost",
				Name: host.Name, UID: host.UID, Controller: &isController,
			}},
		},
		Data: map[string][]byte{
			bootstrapConsumerSecretKey:       []byte(ndTestMachine),
			bootstrapTokenSecretKey:          []byte(ndTestToken),
			bootstrapTokenExpiresAtSecretKey: []byte(formatCredentialTime(time.Now().Add(time.Hour))),
		},
	}
}

func ndTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(newSchemeForTest(t)).WithObjects(objs...).Build()
}

func TestLoadNetworkData(t *testing.T) {
	t.Parallel()
	maxed := bytes.Repeat([]byte("a"), infrav1.MaxNetworkDataSize)
	cases := []struct {
		name       string
		secretRef  string
		secret     *corev1.Secret
		wantData   []byte
		wantReason string
	}{
		{name: "no reference", secretRef: "", wantData: nil},
		{name: "secret not found", secretRef: ndTestSecret, wantReason: infrav1.NetworkDataSecretNotFoundReason},
		{name: "not annotated", secretRef: ndTestSecret, secret: ndTestSecretObject("", []byte(ndTestNetplan)), wantReason: infrav1.NetworkDataSecretNotOptedInReason},
		{name: "annotated for another host", secretRef: ndTestSecret, secret: ndTestSecretObject("host-02", []byte(ndTestNetplan)), wantReason: infrav1.NetworkDataSecretNotOptedInReason},
		{name: "key missing", secretRef: ndTestSecret, secret: ndTestSecretObject(ndTestHost, nil), wantReason: infrav1.NetworkDataKeyMissingReason},
		{name: "key empty", secretRef: ndTestSecret, secret: ndTestSecretObject(ndTestHost, []byte{}), wantReason: infrav1.NetworkDataKeyMissingReason},
		{name: "over the cap", secretRef: ndTestSecret, secret: ndTestSecretObject(ndTestHost, append(maxed, 'a')), wantReason: infrav1.NetworkDataTooLargeReason},
		{name: "at the cap", secretRef: ndTestSecret, secret: ndTestSecretObject(ndTestHost, maxed), wantData: maxed},
		{name: "served verbatim", secretRef: ndTestSecret, secret: ndTestSecretObject(ndTestHost, []byte(ndTestNetplan)), wantData: []byte(ndTestNetplan)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var objs []client.Object
			if tc.secret != nil {
				objs = append(objs, tc.secret)
			}
			data, err := loadNetworkData(context.Background(), ndTestClient(t, objs...), ndTestHostObject(tc.secretRef))
			if tc.wantReason == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if !bytes.Equal(data, tc.wantData) {
					t.Fatalf("data = %q, want %q", data, tc.wantData)
				}
				return
			}
			var ndErr *networkDataError
			if !errors.As(err, &ndErr) {
				t.Fatalf("err = %v, want a networkDataError with reason %s", err, tc.wantReason)
			}
			if ndErr.reason != tc.wantReason {
				t.Fatalf("reason = %s, want %s", ndErr.reason, tc.wantReason)
			}
			if strings.Contains(ndErr.message, ndTestNetplan) {
				t.Fatal("the error message carries the Secret's content")
			}
		})
	}
}

func TestReconcileNetworkDataCondition(t *testing.T) {
	t.Parallel()
	r := &PhysicalHostReconciler{Log: ctrl.Log.WithName("nd-test")}
	ctx := context.Background()

	host := ndTestHostObject(ndTestSecret)
	r.Client = ndTestClient(t)
	r.reconcileNetworkData(ctx, r.Log, host)
	if cond := conditions.Get(host, infrav1.NetworkDataReadyCondition); cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != infrav1.NetworkDataSecretNotFoundReason {
		t.Fatalf("condition = %+v, want False/%s", cond, infrav1.NetworkDataSecretNotFoundReason)
	}

	r.Client = ndTestClient(t, ndTestSecretObject(ndTestHost, []byte(ndTestNetplan)))
	r.reconcileNetworkData(ctx, r.Log, host)
	if !conditions.IsTrue(host, infrav1.NetworkDataReadyCondition) {
		t.Fatalf("condition = %+v, want True once the Secret is fixed", conditions.Get(host, infrav1.NetworkDataReadyCondition))
	}

	host.Spec.NetworkDataSecretRef = ""
	r.reconcileNetworkData(ctx, r.Log, host)
	if conditions.Has(host, infrav1.NetworkDataReadyCondition) {
		t.Fatal("the condition outlived the reference")
	}
}

func TestHostWaitingForNetworkData(t *testing.T) {
	t.Parallel()
	withCondition := func(status metav1.ConditionStatus, generation int64) *infrav1.PhysicalHost {
		h := ndTestHostObject(ndTestSecret)
		h.Status.Conditions = []metav1.Condition{{
			Type: infrav1.NetworkDataReadyCondition, Status: status, Reason: infrav1.NetworkDataReadyReason,
			ObservedGeneration: generation,
		}}
		return h
	}
	cases := []struct {
		name string
		host *infrav1.PhysicalHost
		want bool
	}{
		{name: "no reference", host: ndTestHostObject(""), want: false},
		{name: "no condition yet", host: ndTestHostObject(ndTestSecret), want: true},
		{name: "condition False", host: withCondition(metav1.ConditionFalse, 1), want: true},
		{name: "condition True for an earlier spec", host: withCondition(metav1.ConditionTrue, 0), want: true},
		{name: "condition True", host: withCondition(metav1.ConditionTrue, 1), want: false},
	}
	for _, tc := range cases {
		if got := hostWaitingForNetworkData(tc.host); got != tc.want {
			t.Errorf("%s: hostWaitingForNetworkData = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestNetworkDataGateHoldsTheClaim checks that a claimed host whose network
// data is not ready is not booted: no BMC credentials exist here, so any
// attempt to reach the BMC would fail the pass.
func TestNetworkDataGateHoldsTheClaim(t *testing.T) {
	t.Parallel()
	host := ndTestHostObject(ndTestSecret)
	host.Status.State = infrav1.StateInUse
	setFalse(host, infrav1.NetworkDataReadyCondition, infrav1.NetworkDataSecretNotOptedInReason, "not opted in")
	machine := &infrav1.Beskar7Machine{ObjectMeta: metav1.ObjectMeta{Name: ndTestMachine, Namespace: ndTestNamespace}}
	r := &Beskar7MachineReconciler{Client: ndTestClient(t), Log: ctrl.Log.WithName("nd-gate-test")}

	result, err := r.handlePhysicalHostState(context.Background(), r.Log, machine, host)
	if err != nil {
		t.Fatalf("the gated pass failed, so it tried to boot the host: %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Fatal("the gated pass does not requeue")
	}
	cond := conditions.Get(machine, infrav1.InfrastructureReadyCondition)
	if cond == nil || cond.Reason != infrav1.WaitingForNetworkDataReason || !strings.Contains(cond.Message, "not opted in") {
		t.Fatalf("InfrastructureReady = %+v, want False/%s naming the host's reason", cond, infrav1.WaitingForNetworkDataReason)
	}

	setTrue(host, infrav1.NetworkDataReadyCondition, infrav1.NetworkDataReadyReason)
	if _, err := r.handlePhysicalHostState(context.Background(), r.Log, machine, host); err == nil {
		t.Fatal("the pass after the Secret is fixed did not go on to the BMC")
	}
}

// ndTestServe sends a GET /networkdata for ndTestHost with token through the
// route as SetupCallbackServer registers it.
func ndTestServe(t *testing.T, c client.Client, token string) *httptest.ResponseRecorder {
	t.Helper()
	log := ctrl.Log.WithName("nd-handler-test")
	mux := http.NewServeMux()
	mux.Handle(networkDataRoute, auth.RequireBearer(log, newBearerTokenVerifier(c, log), &NetworkDataHandler{Client: c, Log: log}))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/networkdata/"+ndTestNamespace+"/"+ndTestHost, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	return w
}

func TestNetworkDataHandler(t *testing.T) {
	t.Parallel()
	served := ndTestHostObject(ndTestSecret)
	cases := []struct {
		name       string
		host       *infrav1.PhysicalHost
		secret     *corev1.Secret
		token      string
		wantStatus int
		wantBody   string
	}{
		{name: "served", host: served, secret: ndTestSecretObject(ndTestHost, []byte(ndTestNetplan)), token: ndTestToken, wantStatus: http.StatusOK, wantBody: ndTestNetplan},
		{name: "no reference", host: ndTestHostObject(""), token: ndTestToken, wantStatus: http.StatusNotFound},
		{name: "secret missing", host: served, token: ndTestToken, wantStatus: http.StatusConflict},
		{name: "not opted in", host: served, secret: ndTestSecretObject("host-02", []byte(ndTestNetplan)), token: ndTestToken, wantStatus: http.StatusConflict},
		{name: "key missing", host: served, secret: ndTestSecretObject(ndTestHost, nil), token: ndTestToken, wantStatus: http.StatusConflict},
		{name: "oversized", host: served, secret: ndTestSecretObject(ndTestHost, bytes.Repeat([]byte("a"), infrav1.MaxNetworkDataSize+1)), token: ndTestToken, wantStatus: http.StatusConflict},
		{name: "wrong token", host: served, secret: ndTestSecretObject(ndTestHost, []byte(ndTestNetplan)), token: "not-the-token", wantStatus: http.StatusUnauthorized},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			objs := []client.Object{tc.host, ndTestCredentialSecret(tc.host)}
			if tc.secret != nil {
				objs = append(objs, tc.secret)
			}
			w := ndTestServe(t, ndTestClient(t, objs...), tc.token)
			if w.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body %q)", w.Code, tc.wantStatus, w.Body.String())
			}
			if tc.wantStatus != http.StatusOK {
				if strings.Contains(w.Body.String(), ndTestNetplan) {
					t.Fatal("a refusal carries the Secret's content")
				}
				return
			}
			if w.Body.String() != tc.wantBody {
				t.Fatalf("body = %q, want %q", w.Body.String(), tc.wantBody)
			}
			if got := w.Header().Get("Cache-Control"); got != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", got)
			}
			if got := w.Header().Get("Content-Type"); got != "application/octet-stream" {
				t.Fatalf("Content-Type = %q, want application/octet-stream", got)
			}
		})
	}
}

// TestNetworkDataHandlerServesTheSecretAsItIsNow checks there is no pinning:
// an edit after the Machine's gate passed is what the inspector gets.
func TestNetworkDataHandlerServesTheSecretAsItIsNow(t *testing.T) {
	t.Parallel()
	host := ndTestHostObject(ndTestSecret)
	secret := ndTestSecretObject(ndTestHost, []byte(ndTestNetplan))
	c := ndTestClient(t, host, ndTestCredentialSecret(host), secret)

	const edited = "network:\n  version: 2\n  ethernets: {}\n"
	secret.Data[infrav1.NetworkDataSecretKey] = []byte(edited)
	if err := c.Update(context.Background(), secret); err != nil {
		t.Fatalf("edit the Secret: %v", err)
	}
	if w := ndTestServe(t, c, ndTestToken); w.Code != http.StatusOK || w.Body.String() != edited {
		t.Fatalf("got %d %q, want 200 with the edited content", w.Code, w.Body.String())
	}
}

// TestNetworkDataHandlerRefusesAStaleClaim checks that a token bound to an
// earlier claim gets the opaque 401, never a 404 the inspector would read as
// "no network data".
func TestNetworkDataHandlerRefusesAStaleClaim(t *testing.T) {
	t.Parallel()
	host := ndTestHostObject(ndTestSecret)
	c := ndTestClient(t, host, ndTestCredentialSecret(host), ndTestSecretObject(ndTestHost, []byte(ndTestNetplan)))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/networkdata/"+ndTestNamespace+"/"+ndTestHost, nil)
	req.Header.Set("Authorization", "Bearer "+ndTestToken)
	req.SetPathValue("namespace", ndTestNamespace)
	req.SetPathValue("hostName", ndTestHost)

	// The claim moves on after the bearer middleware let the request through.
	moved := &infrav1.PhysicalHost{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ndTestNamespace, Name: ndTestHost}, moved); err != nil {
		t.Fatalf("get host: %v", err)
	}
	moved.Spec.ConsumerRef.Name = "b7m-02"
	if err := c.Update(context.Background(), moved); err != nil {
		t.Fatalf("re-point the claim: %v", err)
	}

	w := httptest.NewRecorder()
	(&NetworkDataHandler{Client: c, Log: ctrl.Log.WithName("nd-stale-test")}).ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", w.Code)
	}
}
