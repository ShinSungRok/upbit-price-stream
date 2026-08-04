{{/*
Renders a Deployment for one of the three app components.
Expects a dict: name, image, resources, autoscaling, root, and optionally port.
*/}}
{{- define "upbit.appDeployment" -}}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ .name }}
  labels:
    app: {{ .name }}
spec:
  {{- if not .autoscaling.enabled }}
  replicas: 1
  {{- end }}
  selector:
    matchLabels:
      app: {{ .name }}
  template:
    metadata:
      labels:
        app: {{ .name }}
    spec:
      containers:
        - name: {{ .name }}
          image: "{{ .image.repository }}:{{ .image.tag }}"
          imagePullPolicy: Never
          {{- if .port }}
          ports:
            - containerPort: {{ .port }}
          {{- end }}
          env:
            - name: KAFKA_BOOTSTRAP_SERVERS
              value: "{{ .root.Values.kafka.clusterName }}-kafka-bootstrap:9092"
          resources:
{{ toYaml .resources | indent 12 }}
{{- end -}}

{{/*
Renders a HorizontalPodAutoscaler targeting the same-named Deployment.
Expects a dict: name, autoscaling. No-op if autoscaling.enabled is false.
*/}}
{{- define "upbit.appHpa" -}}
{{- if .autoscaling.enabled }}
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {{ .name }}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: {{ .name }}
  minReplicas: {{ .autoscaling.minReplicas }}
  maxReplicas: {{ .autoscaling.maxReplicas }}
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: {{ .autoscaling.targetCPUUtilizationPercentage }}
{{- end -}}
{{- end -}}
