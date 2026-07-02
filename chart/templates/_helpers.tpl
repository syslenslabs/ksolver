{{/*
Expand the name of the chart.
*/}}
{{- define "solver.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "solver.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "solver.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "solver.labels" -}}
helm.sh/chart: {{ include "solver.chart" . }}
{{ include "solver.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "solver.selectorLabels" -}}
app.kubernetes.io/name: {{ include "solver.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: solver
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "solver.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "solver.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Whether a scheduler binding rollout mode string is recognized.
*/}}
{{- define "solver.bindingRolloutModeValid" -}}
{{- $mode := lower (toString .) -}}
{{- if or (eq $mode "observe") (eq $mode "observe-only") (eq $mode "observe_only") (eq $mode "shadow") (eq $mode "dry-run") (eq $mode "dry_run") (eq $mode "dryrun") (eq $mode "validate") (eq $mode "bind-low-risk") (eq $mode "bind_low_risk") (eq $mode "low-risk") (eq $mode "canary") (eq $mode "bind-all") (eq $mode "bind_all") (eq $mode "live") (eq $mode "all") -}}true{{- else -}}false{{- end -}}
{{- end }}

{{/*
Whether a scheduler binding rollout mode can create pod bindings when the kill switch is off.
*/}}
{{- define "solver.bindingRolloutCanWrite" -}}
{{- $mode := lower (toString .) -}}
{{- if or (eq $mode "dry-run") (eq $mode "dry_run") (eq $mode "dryrun") (eq $mode "validate") (eq $mode "bind-low-risk") (eq $mode "bind_low_risk") (eq $mode "low-risk") (eq $mode "canary") (eq $mode "bind-all") (eq $mode "bind_all") (eq $mode "live") (eq $mode "all") -}}true{{- else -}}false{{- end -}}
{{- end }}
