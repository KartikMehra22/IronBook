// Command admission-webhook is a validating admission webhook that enforces
// IronBook submission-pod constraints (see apps/admission-webhook/policy).
package main

import (
	"crypto/tls"
	"encoding/json"
	"log"
	"net/http"
	"os"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/KartikMehra22/IronBook/apps/admission-webhook/policy"
)

const (
	defaultAddr     = ":8443"
	defaultCertPath = "/tls/tls.crt"
	defaultKeyPath  = "/tls/tls.key"
)

func main() {
	addr := envOr("IRONBOOK_ADDR", defaultAddr)
	certPath := envOr("IRONBOOK_TLS_CERT", defaultCertPath)
	keyPath := envOr("IRONBOOK_TLS_KEY", defaultKeyPath)

	mux := http.NewServeMux()
	mux.HandleFunc("/validate", handle)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		log.Fatalf("load cert: %v", err)
	}
	srv := &http.Server{
		Addr:      addr,
		Handler:   mux,
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12},
	}
	log.Printf("admission-webhook listening on %s", addr)
	if err := srv.ListenAndServeTLS("", ""); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var review admissionv1.AdmissionReview
	if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if review.Request == nil {
		http.Error(w, "missing AdmissionRequest", http.StatusBadRequest)
		return
	}
	resp := &admissionv1.AdmissionResponse{UID: review.Request.UID}

	var pod corev1.Pod
	if err := json.Unmarshal(review.Request.Object.Raw, &pod); err != nil {
		resp.Allowed = false
		resp.Result = &metav1.Status{Code: http.StatusBadRequest, Message: "could not decode Pod: " + err.Error()}
	} else {
		res := policy.Validate(&pod)
		resp.Allowed = res.Allowed
		if !res.Allowed {
			resp.Result = &metav1.Status{Code: http.StatusForbidden, Message: res.Reason}
		}
	}

	out := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"},
		Response: resp,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		log.Printf("encode response: %v", err)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
