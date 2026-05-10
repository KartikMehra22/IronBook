# RUNBOOK-06: Activate gVisor (Layer 2) on a Linux host

The IronBook admission-webhook already enforces `runtimeClassName: gvisor` on every
production submission pod (test pods labelled `ironbook.io/test=true` are exempted
for the kind/Mac demo per ADR-011). The `RuntimeClass` resource ships in
`deploy/runtimeclasses/gvisor.yaml`. On a Linux host (e.g. a Hetzner ARM VM
provisioned via `deploy/terraform/envs/prod/`), `runsc` activates Layer 2 of the
seven-layer isolation chain.

## Steps (on the Linux host)

```bash
ARCH=$(uname -m | sed s/aarch64/arm64/)
URL="https://storage.googleapis.com/gvisor/releases/release/latest/${ARCH}"
wget ${URL}/runsc ${URL}/runsc.sha512 \
     ${URL}/containerd-shim-runsc-v1 ${URL}/containerd-shim-runsc-v1.sha512
sha512sum -c runsc.sha512 -c containerd-shim-runsc-v1.sha512
chmod a+rx runsc containerd-shim-runsc-v1
sudo mv runsc containerd-shim-runsc-v1 /usr/local/bin/

# k3s: extend containerd config
sudo mkdir -p /var/lib/rancher/k3s/agent/etc/containerd/
sudo tee /var/lib/rancher/k3s/agent/etc/containerd/config.toml.tmpl > /dev/null <<'EOF'
{{ template "base" . }}

[plugins."io.containerd.grpc.v1.cri".containerd.runtimes.runsc]
  runtime_type = "io.containerd.runsc.v1"
EOF
sudo systemctl restart k3s
```

## Verify

```bash
crictl info | grep -A3 runtimes        # 'runsc' listed
kubectl apply -f deploy/runtimeclasses/gvisor.yaml
kubectl get runtimeclass gvisor        # exists
```

## Hackathon-time fallback

For the kind-on-Mac demo we *do not* run this. Submission pods land on `runc`
(the kind default). Layer 2 is documented as future-work in spec §10.1 and in
ADR-011. The other six isolation layers are deployed and active:

1. pod `securityContext` (runAsNonRoot, drop ALL caps, readOnly rootfs, etc.)
2. *gVisor — inactive on demo host*
3. seccomp-bpf
4. AppArmor MAC
5. cgroups v2
6. iptables host backstop
7. NetworkPolicy
