# Vendored charts

`strimzi-kafka-operator/` is a straight copy of the official Strimzi Helm chart
(`oci://quay.io/strimzi-helm/strimzi-kafka-operator:1.1.0`, pulled via
`helm pull ... --untar`). It's vendored here — not fetched live — because
`argocd-repo-server` fetching an OCI chart directly from `quay.io` fails on
this network (the same TLS-inspecting proxy issue documented in
`k8s/README.md`; unlike a `docker pull`/image import, this is a live HTTPS
call the running repo-server pod makes itself, so it can't be worked around
by pre-importing an image). Git-over-HTTPS to GitHub works fine from inside
the cluster, so pointing the `strimzi-operator` Application at this vendored
copy in our own repo sidesteps the problem entirely while staying fully
GitOps — git is still the only thing ArgoCD reads from.

To bump the version: `helm pull oci://quay.io/strimzi-helm/strimzi-kafka-operator --version <new> --untar --untardir argocd/vendor`.
