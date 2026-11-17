//! Tracing of the jj operations the shims serve. It is a no-op unless an OTLP endpoint is set in the
//! standard `OTEL_*` variables. A span carries counts and sizes only, never paths or file contents.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use opentelemetry::trace::{
    Span as _, SpanContext, SpanId, TraceContextExt as _, TraceFlags, TraceId, TraceState,
    Tracer as _,
};
use opentelemetry::{Context, KeyValue};
use opentelemetry_otlp::{
    Protocol, RetryPolicy, SpanExporter, WithExportConfig as _, WithHttpConfig as _,
};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::{SdkTracer, SdkTracerProvider, Span};

const EXPORT_TIMEOUT: Duration = Duration::from_millis(500);

struct Frame {
    ctx: SpanContext,
    objects: AtomicU64,
    bytes: AtomicU64,
}

struct State {
    provider: Option<SdkTracerProvider>,
    tracer: Option<SdkTracer>,
    env_parent: Option<SpanContext>,
    stack: Mutex<Vec<Arc<Frame>>>,
    root: Mutex<Option<Span>>,
}

static STATE: OnceLock<State> = OnceLock::new();

fn parse_traceparent(s: &str) -> Option<SpanContext> {
    let mut parts = s.trim().split('-');
    let (version, trace, span, flags) =
        (parts.next()?, parts.next()?, parts.next()?, parts.next()?);
    if version != "00" || trace.len() != 32 || span.len() != 16 || flags.len() != 2 {
        return None;
    }
    let trace = TraceId::from_hex(trace)
        .ok()
        .filter(|t| *t != TraceId::INVALID)?;
    let span = SpanId::from_hex(span)
        .ok()
        .filter(|s| *s != SpanId::INVALID)?;
    let flags = TraceFlags::new(u8::from_str_radix(flags, 16).ok()?);
    Some(SpanContext::new(
        trace,
        span,
        flags,
        true,
        TraceState::default(),
    ))
}

fn format_traceparent(c: &SpanContext) -> String {
    format!(
        "00-{}-{}-{:02x}",
        c.trace_id(),
        c.span_id(),
        c.trace_flags().to_u8()
    )
}

fn exporter_configured() -> bool {
    let set = |k: &str| std::env::var(k).is_ok_and(|v| !v.is_empty());
    !std::env::var("OTEL_SDK_DISABLED").is_ok_and(|v| v.eq_ignore_ascii_case("true"))
        && !std::env::var("OTEL_TRACES_EXPORTER").is_ok_and(|v| v == "none")
        && (set("OTEL_EXPORTER_OTLP_ENDPOINT") || set("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"))
}

fn build_provider() -> Option<SdkTracerProvider> {
    let exporter = SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_timeout(EXPORT_TIMEOUT)
        .with_retry_policy(RetryPolicy::disabled())
        .build()
        .ok()?;
    let mut resource = Resource::builder();
    if std::env::var_os("OTEL_SERVICE_NAME").is_none() {
        resource = resource.with_service_name("motte-jj");
    }
    Some(
        SdkTracerProvider::builder()
            .with_resource(resource.build())
            .with_batch_exporter(exporter)
            .build(),
    )
}

/// Starts tracing for this process. `TRACEPARENT` names the parent of the command span.
pub fn init() {
    STATE.get_or_init(|| {
        let env_parent = std::env::var("TRACEPARENT")
            .ok()
            .and_then(|v| parse_traceparent(&v));
        let provider = exporter_configured().then(build_provider).flatten();
        let tracer = provider.as_ref().map(|p| {
            use opentelemetry::trace::TracerProvider as _;
            p.tracer("motte-jj")
        });
        let mut stack = Vec::new();
        let mut root = None;
        if let Some(t) = &tracer {
            let cx = parent_context(env_parent.as_ref());
            let span = t.start_with_context("jj.command", &cx);
            stack.push(Arc::new(Frame {
                ctx: span.span_context().clone(),
                objects: AtomicU64::new(0),
                bytes: AtomicU64::new(0),
            }));
            root = Some(span);
        }
        State {
            provider,
            tracer,
            env_parent,
            stack: Mutex::new(stack),
            root: Mutex::new(root),
        }
    });
}

fn parent_context(parent: Option<&SpanContext>) -> Context {
    match parent {
        Some(p) => Context::new().with_remote_span_context(p.clone()),
        None => Context::new(),
    }
}

/// Ends the command span and flushes the exporter.
pub fn shutdown() {
    let Some(st) = STATE.get() else { return };
    if let Some(mut root) = st.root.lock().unwrap().take() {
        if let Some(f) = st.stack.lock().unwrap().first() {
            root.set_attribute(KeyValue::new(
                "objects",
                f.objects.load(Ordering::Relaxed) as i64,
            ));
            root.set_attribute(KeyValue::new(
                "bytes",
                f.bytes.load(Ordering::Relaxed) as i64,
            ));
        }
        root.end();
    }
    if let Some(p) = &st.provider {
        let _ = p.shutdown_with_timeout(EXPORT_TIMEOUT);
    }
}

/// The W3C `traceparent` for the next daemon request.
pub fn traceparent() -> Option<String> {
    let st = STATE.get()?;
    let top = st.stack.lock().unwrap().last().map(|f| f.ctx.clone());
    top.or_else(|| st.env_parent.clone())
        .map(|c| format_traceparent(&c))
}

/// Adds to the counts of the innermost open span.
pub fn count(objects: u64, bytes: u64) {
    let Some(st) = STATE.get() else { return };
    if let Some(f) = st.stack.lock().unwrap().last() {
        f.objects.fetch_add(objects, Ordering::Relaxed);
        f.bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

/// One jj operation. The span ends when the guard drops.
pub struct Op {
    span: Option<(Span, Arc<Frame>)>,
}

pub fn span(name: &'static str) -> Op {
    let Some(st) = STATE.get() else {
        return Op { span: None };
    };
    let Some(tracer) = &st.tracer else {
        return Op { span: None };
    };
    let mut stack = st.stack.lock().unwrap();
    let parent = stack
        .last()
        .map(|f| f.ctx.clone())
        .or_else(|| st.env_parent.clone());
    let span = tracer.start_with_context(name, &parent_context(parent.as_ref()));
    let frame = Arc::new(Frame {
        ctx: span.span_context().clone(),
        objects: AtomicU64::new(0),
        bytes: AtomicU64::new(0),
    });
    stack.push(frame.clone());
    Op {
        span: Some((span, frame)),
    }
}

impl Op {
    pub fn set(&mut self, key: &'static str, value: i64) {
        if let Some((s, _)) = &mut self.span {
            s.set_attribute(KeyValue::new(key, value));
        }
    }

    pub fn set_str(&mut self, key: &'static str, value: &str) {
        if let Some((s, _)) = &mut self.span {
            s.set_attribute(KeyValue::new(key, value.to_owned()));
        }
    }
}

impl Drop for Op {
    fn drop(&mut self) {
        let Some((mut span, frame)) = self.span.take() else {
            return;
        };
        span.set_attribute(KeyValue::new(
            "objects",
            frame.objects.load(Ordering::Relaxed) as i64,
        ));
        span.set_attribute(KeyValue::new(
            "bytes",
            frame.bytes.load(Ordering::Relaxed) as i64,
        ));
        span.end();
        if let Some(st) = STATE.get() {
            let mut stack = st.stack.lock().unwrap();
            if let Some(i) = stack.iter().rposition(|f| Arc::ptr_eq(f, &frame)) {
                stack.remove(i);
            }
        }
    }
}
