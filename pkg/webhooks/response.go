package webhooks

import (
	"encoding/json"
	"net/http"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func sendAdmissionResponse(w http.ResponseWriter, uid types.UID, allowed bool, patch []byte, message string) {
	resp := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			Kind:       "AdmissionReview",
			APIVersion: "admission.k8s.io/v1",
		},
		Response: &admissionv1.AdmissionResponse{
			UID:     uid,
			Allowed: allowed,
		},
	}

	if !allowed && message != "" {
		resp.Response.Result = &metav1.Status{
			Message: message,
			Code:    http.StatusBadRequest,
		}
	}

	if allowed && len(patch) > 0 {
		patchType := admissionv1.PatchTypeJSONPatch
		resp.Response.PatchType = &patchType
		resp.Response.Patch = patch
	}

	respBytes, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(respBytes)
}
