{{- define "motte.image" -}}
{{- $image := required "image.repository is required" .Values.image.repository -}}
{{- if .Values.image.digest -}}
{{- if not (hasPrefix "sha256:" .Values.image.digest) }}{{ fail "image.digest must be a sha256 digest" }}{{ end -}}
{{- printf "%s@%s" $image .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" $image (required "image.digest or image.tag is required" .Values.image.tag) -}}
{{- end -}}
{{- end -}}

{{- define "motte.serviceAccountName" -}}
{{- default .Release.Name .Values.serviceAccount.name -}}
{{- end -}}

{{- define "motte.labels" -}}
app.kubernetes.io/name: motte
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end -}}

{{- define "motte.selector" -}}
app.kubernetes.io/name: motte
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
