package webhooks

import (
	"context"
	"net/http"
	"time"

	secretsmanagerslisters "github.com/naseyro/ssc/pkg/clientset/listers"
	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	secretsv1 "github.com/naseyro/ssc/pkg/apis/secrets.management.io/v1"
)

var (
	mainScheme = runtime.NewScheme()
	codecs     = serializer.NewCodecFactory(mainScheme)
)

func init() {
	utilruntime.Must(admissionv1.AddToScheme(mainScheme))
	utilruntime.Must(secretsv1.AddKnownTypes(mainScheme))
}

type WebhookServer struct {
	SecretsManagerInformer cache.SharedIndexInformer
	SecretsManagerLister   secretsmanagerslisters.SecretsManagerLister
	mapper                 meta.RESTMapper
	tlsCertFile            string
	tlsKeyFile             string
}

func NewWebhookServer(secretsManagerInformer cache.SharedIndexInformer, secretsManagerLister secretsmanagerslisters.SecretsManagerLister, mapper meta.RESTMapper, tlsCertFile, tlsKeyFile string) *WebhookServer {
	return &WebhookServer{
		SecretsManagerInformer: secretsManagerInformer,
		SecretsManagerLister:   secretsManagerLister,
		mapper:                 mapper,
		tlsCertFile:            tlsCertFile,
		tlsKeyFile:             tlsKeyFile,
	}
}

func (ws *WebhookServer) Serve(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/mutate", ws.MutateHandler)
	klog.Info("Starting Webhook server on :8443")

	srv := &http.Server{
		Addr:              ":8443",
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServeTLS(ws.tlsCertFile, ws.tlsKeyFile)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	}
}
