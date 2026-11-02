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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	infrav1 "github.com/projectbeskar/beskar7/api/v1beta2"
	"github.com/projectbeskar/beskar7/internal/auth"
)

// goldenNetworkDataArtifactPath is the canonical descriptor of the
// inspector-written /oem/beskar7/network-data COS_OEM artifact (contract v4.3).
const goldenNetworkDataArtifactPath = "../test/contract/golden_network_data_artifact.json"

// goldenNetworkDataArtifact mirrors the JSON shape of
// test/contract/golden_network_data_artifact.json.
type goldenNetworkDataArtifact struct {
	Namespace      string `json:"namespace"`
	Host           string `json:"host"`
	Endpoint       string `json:"endpoint"`
	SecretKey      string `json:"secretKey"`
	HostAnnotation string `json:"hostAnnotation"`
	ArtifactPath   string `json:"artifactPath"`
	Content        string `json:"content"`
	Mode           string `json:"mode"`
	Owner          string `json:"owner"`
}

// TestGoldenNetworkDataArtifact_MatchesServedBytes serves the fixture's
// content from a Secret shaped as the fixture says, through the real route,
// and asserts the controller adds or strips nothing.
func TestGoldenNetworkDataArtifact_MatchesServedBytes(t *testing.T) {
	raw, err := os.ReadFile(filepath.Clean(goldenNetworkDataArtifactPath))
	if err != nil {
		t.Fatalf("read golden network data artifact: %v", err)
	}
	var fixture goldenNetworkDataArtifact
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&fixture); err != nil {
		t.Fatalf("decode golden network data artifact: %v", err)
	}

	if fixture.SecretKey != infrav1.NetworkDataSecretKey {
		t.Errorf("secretKey = %q, want %q", fixture.SecretKey, infrav1.NetworkDataSecretKey)
	}
	if fixture.HostAnnotation != NetworkDataHostAnnotation {
		t.Errorf("hostAnnotation = %q, want %q", fixture.HostAnnotation, NetworkDataHostAnnotation)
	}
	if fixture.ArtifactPath != "/oem/beskar7/network-data" {
		t.Errorf("artifactPath = %q, want /oem/beskar7/network-data (contract v4.3 §9.1 step 5.4b)", fixture.ArtifactPath)
	}
	if fixture.Mode != "0600" || fixture.Owner != "root" {
		t.Errorf("mode/owner = %s/%s, want 0600/root", fixture.Mode, fixture.Owner)
	}

	const token = "golden-token"
	isController := true
	host := &infrav1.PhysicalHost{
		ObjectMeta: metav1.ObjectMeta{Name: fixture.Host, Namespace: fixture.Namespace, UID: "golden-host-uid"},
		Spec: infrav1.PhysicalHostSpec{
			RedfishConnection: infrav1.RedfishConnection{Address: "https://192.0.2.10", CredentialsSecretRef: "bmc-creds"},
			ConsumerRef: &corev1.ObjectReference{
				Kind: "Beskar7Machine", APIVersion: InfrastructureAPIVersion, Name: "golden-machine", Namespace: fixture.Namespace,
			},
			NetworkDataSecretRef: "golden-network-data",
		},
	}
	c := ndTestClient(t, host,
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name: bootstrapTokenSecretName(host.Name), Namespace: host.Namespace,
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: infrav1.GroupVersion.String(), Kind: "PhysicalHost",
					Name: host.Name, UID: host.UID, Controller: &isController,
				}},
			},
			Data: map[string][]byte{
				bootstrapConsumerSecretKey:       []byte("golden-machine"),
				bootstrapTokenSecretKey:          []byte(token),
				bootstrapTokenExpiresAtSecretKey: []byte(formatCredentialTime(time.Now().Add(time.Hour))),
			},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name: host.Spec.NetworkDataSecretRef, Namespace: host.Namespace,
				Annotations: map[string]string{fixture.HostAnnotation: fixture.Host},
			},
			Data: map[string][]byte{fixture.SecretKey: []byte(fixture.Content)},
		},
	)

	log := ctrl.Log.WithName("golden-network-data")
	mux := http.NewServeMux()
	mux.Handle(networkDataRoute, auth.RequireBearer(log, newBearerTokenVerifier(c, log), &NetworkDataHandler{Client: c, Log: log}))
	req := httptest.NewRequest(http.MethodGet, fixture.Endpoint, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GET %s: status %d, want 200", fixture.Endpoint, w.Code)
	}
	if w.Body.String() != fixture.Content {
		t.Fatalf("served bytes differ from the fixture's content:\n got %q\nwant %q", w.Body.String(), fixture.Content)
	}
}
