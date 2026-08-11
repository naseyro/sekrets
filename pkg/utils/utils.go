package utils

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"

	corev1 "k8s.io/api/core/v1"
)

func ComputeSecretHash(secret *corev1.Secret) string {
	if secret == nil || len(secret.Data) == 0 {
		return ""
	}

	keys := make([]string, 0, len(secret.Data))
	for key := range secret.Data {
		keys = append(keys, key)
	}

	sort.Strings(keys)
	hasher := sha256.New()

	for _, key := range keys {
		hasher.Write([]byte(key))
		hasher.Write([]byte("="))
		hasher.Write(secret.Data[key])
		hasher.Write([]byte(";"))
	}

	return hex.EncodeToString(hasher.Sum(nil))
}
