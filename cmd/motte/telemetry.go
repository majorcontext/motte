package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"runtime"
	"time"

	promclient "github.com/prometheus/client_golang/prometheus"
	promebridge "go.opentelemetry.io/contrib/bridges/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const flushTimeout = 5 * time.Second

// startTraces exports spans over OTLP to the endpoint of OTEL_EXPORTER_OTLP_ENDPOINT. The SDK reads the sampler, the
// service name and the headers from the standard OTEL_* variables. The returned function flushes and stops it.
func startTraces(ctx context.Context) (func(), error) {
	exp, err := otlptracehttp.New(ctx)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp))
	otel.SetTracerProvider(tp)
	return func() { flush(tp.Shutdown) }, nil
}

// startMetrics exports the metrics of reg over OTLP, on the interval of OTEL_METRIC_EXPORT_INTERVAL, so /metrics and
// the exporter report the same values.
func startMetrics(ctx context.Context, reg promclient.Gatherer) (func(), error) {
	exp, err := otlpmetrichttp.New(ctx)
	if err != nil {
		return nil, err
	}
	reader := sdkmetric.NewPeriodicReader(exp, sdkmetric.WithProducer(promebridge.NewMetricProducer(promebridge.WithGatherer(reg))))
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(mp)
	return func() { flush(mp.Shutdown) }, nil
}

func flush(shutdown func(context.Context) error) {
	ctx, cancel := context.WithTimeout(context.Background(), flushTimeout)
	defer cancel()
	_ = shutdown(ctx)
}

// servePprof serves the profiles on addr, a listener of its own: the public handler never carries them.
// It also turns on the mutex and block profiles.
func servePprof(log *slog.Logger, addr string) {
	runtime.SetMutexProfileFraction(100)
	runtime.SetBlockProfileRate(1000000)
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	log.Info("motte profiles listening", "addr", addr)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Error("profile listener stopped", "error", err)
	}
}
