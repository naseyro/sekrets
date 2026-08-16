package webhooks

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/naseyro/ssc/pkg/utils"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
)

func (ws *WebhookServer) MutateHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read body: %v", err), http.StatusBadRequest)
		return
	}

	admissionReview := &admissionv1.AdmissionReview{}
	obj, _, err := codecs.UniversalDeserializer().Decode(body, nil, admissionReview)
	if err != nil {
		klog.Errorf("Failed to decode AdmissionReview: %v", err)
		http.Error(w, fmt.Sprintf("failed to decode admission review: %v", err), http.StatusBadRequest)
		return
	}
	admissionReview, ok := obj.(*admissionv1.AdmissionReview)
	if !ok {
		http.Error(w, "expected AdmissionReview", http.StatusBadRequest)
		return
	}

	request := admissionReview.Request
	if request == nil {
		klog.Errorf("AdmissionReview.Request is nil")
		sendAdmissionResponse(w, "", true, nil, "")
		return
	}

	if request.Operation != admissionv1.Create && request.Operation != admissionv1.Update {
		sendAdmissionResponse(w, request.UID, true, nil, "")
		return
	}

	var unstructuredObject unstructured.Unstructured
	if err := json.Unmarshal(request.Object.Raw, &unstructuredObject); err != nil {
		klog.Errorf("Failed to unmarshal raw object: %v", err)
		sendAdmissionResponse(w, request.UID, true, nil, "")
		return
	}

	templateMap, found, err := unstructured.NestedMap(unstructuredObject.Object, "spec", "template")
	if err != nil || !found {
		sendAdmissionResponse(w, request.UID, true, nil, "")
		return
	}

	var before corev1.PodTemplateSpec
	err = runtime.DefaultUnstructuredConverter.FromUnstructured(templateMap, &before)
	if err != nil {
		klog.Errorf("Failed to convert to PodTemplateSpec: %v", err)
		sendAdmissionResponse(w, request.UID, true, nil, "")
		return
	}

	secretsManagers, err := ws.SecretsManagerLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("Failed to list SecretsManagers from cache: %v", err)
		sendAdmissionResponse(w, request.UID, true, nil, "")
		return
	}
	if len(secretsManagers) == 0 {
		sendAdmissionResponse(w, request.UID, true, nil, "")
		return
	}

	workloadName := unstructuredObject.GetName()
	workloadKind := unstructuredObject.GetKind()
	workloadAPIVersion := unstructuredObject.GetAPIVersion()
	workloadNamespace := request.Namespace
	if workloadNamespace == "" {
		workloadNamespace = unstructuredObject.GetNamespace()
	}

	after := before.DeepCopy()
	var isMutated bool

	for _, sm := range secretsManagers {
		for _, workload := range sm.Spec.TargetWorkloads {
			targetNamespace := workload.Namespace
			if targetNamespace == "" {
				targetNamespace = sm.Namespace
			}

			if workload.Name != workloadName ||
				workload.Kind != workloadKind ||
				workload.APIVersion != workloadAPIVersion ||
				targetNamespace != workloadNamespace {
				continue
			}

			for _, s := range workload.Secrets {
				if utils.IsSecretMounted(after, &s) {
					continue
				}
				utils.InjectSecret(after, &s)
				isMutated = true
			}
		}
	}

	if !isMutated {
		sendAdmissionResponse(w, request.UID, true, nil, "")
		return
	}

	patchOps, err := scopedTemplatePatch(&before, after)
	if err != nil {
		klog.Errorf("Failed to build template patch: %v", err)
		sendAdmissionResponse(w, request.UID, true, nil, "")
		return
	}

	patchBytes, err := json.Marshal(patchOps)
	if err != nil {
		klog.Errorf("Failed to marshal patch: %v", err)
		sendAdmissionResponse(w, request.UID, true, nil, "")
		return
	}

	sendAdmissionResponse(w, request.UID, true, patchBytes, "")
}
