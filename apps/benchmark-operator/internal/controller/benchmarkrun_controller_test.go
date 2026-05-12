/*
Copyright 2026 Kartik Mehra.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	ironbookv1 "github.com/KartikMehra22/IronBook/apps/benchmark-operator/api/v1"
)

var _ = Describe("BenchmarkRun Controller", func() {
	Context("when refs are missing", func() {
		It("transitions to INVALID after PENDING", func() {
			ctx := context.Background()
			nsn := types.NamespacedName{Name: "br-missing", Namespace: "default"}

			br := &ironbookv1.BenchmarkRun{
				ObjectMeta: metav1.ObjectMeta{Name: nsn.Name, Namespace: nsn.Namespace},
				Spec: ironbookv1.BenchmarkRunSpec{
					SubmissionRef: corev1.LocalObjectReference{Name: "does-not-exist"},
					ScenarioRef:   corev1.LocalObjectReference{Name: "does-not-exist"},
					BotSwarmRef:   corev1.LocalObjectReference{Name: "does-not-exist"},
					Seed:          42,
				},
			}
			Expect(k8sClient.Create(ctx, br)).To(Succeed())
			DeferCleanup(func() {
				_ = k8sClient.Delete(ctx, br)
			})

			r := &BenchmarkRunReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nsn})
			Expect(err).NotTo(HaveOccurred())
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: nsn})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, nsn, br)).To(Succeed())
			Expect(br.Status.Phase).To(Equal("PENDING"))
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: nsn})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, nsn, br)).To(Succeed())
			Expect(br.Status.Phase).To(Equal("INVALID"))
		})
	})

	Context("when refs are ready", func() {
		It("transitions PENDING → ALLOCATING → PRIMING and creates the four pods", func() {
			ctx := context.Background()

			sub := &ironbookv1.Submission{
				ObjectMeta: metav1.ObjectMeta{Name: "sub-ok", Namespace: "default"},
				Spec: ironbookv1.SubmissionSpec{
					Sha256:   "0000000000000000000000000000000000000000000000000000000000000000",
					Language: "rust",
				},
			}
			Expect(k8sClient.Create(ctx, sub)).To(Succeed())
			sub.Status = ironbookv1.SubmissionStatus{
				Phase:       "READY",
				ImageDigest: "ironbook/submission@sha256:00",
			}
			Expect(k8sClient.Status().Update(ctx, sub)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, sub) })

			scn := &ironbookv1.Scenario{
				ObjectMeta: metav1.ObjectMeta{Name: "scn-ok", Namespace: "default"},
				Spec: ironbookv1.ScenarioSpec{
					YAMLSpec:        "kind: scenario\n",
					Seed:            1,
					DurationSeconds: 5,
					Targets:         ironbookv1.ScenarioTargets{P50Us: 10, P99Us: 100, TPS: 1000},
				},
			}
			Expect(k8sClient.Create(ctx, scn)).To(Succeed())
			scn.Status = ironbookv1.ScenarioStatus{ContentHash: "deadbeef"}
			Expect(k8sClient.Status().Update(ctx, scn)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, scn) })

			swarm := &ironbookv1.BotSwarm{
				ObjectMeta: metav1.ObjectMeta{Name: "swarm-ok", Namespace: "default"},
				Spec: ironbookv1.BotSwarmSpec{
					MaxWorkers: 4,
					Protocols:  []string{"REST"},
					OrderMix: ironbookv1.OrderMixProfile{
						LimitFraction:  "0.7",
						IocFraction:    "0.2",
						CancelFraction: "0.1",
					},
				},
			}
			Expect(k8sClient.Create(ctx, swarm)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, swarm) })

			nsn := types.NamespacedName{Name: "br-ok", Namespace: "default"}
			br := &ironbookv1.BenchmarkRun{
				ObjectMeta: metav1.ObjectMeta{Name: nsn.Name, Namespace: nsn.Namespace},
				Spec: ironbookv1.BenchmarkRunSpec{
					SubmissionRef: corev1.LocalObjectReference{Name: sub.Name},
					ScenarioRef:   corev1.LocalObjectReference{Name: scn.Name},
					BotSwarmRef:   corev1.LocalObjectReference{Name: swarm.Name},
					Seed:          7,
				},
			}
			Expect(k8sClient.Create(ctx, br)).To(Succeed())
			DeferCleanup(func() { _ = k8sClient.Delete(ctx, br) })

			r := &BenchmarkRunReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
			for i := 0; i < 5; i++ {
				_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nsn})
				Expect(err).NotTo(HaveOccurred())
			}
			Expect(k8sClient.Get(ctx, nsn, br)).To(Succeed())
			Expect(br.Status.Phase).To(Equal("PRIMING"))
			Expect(br.Status.OracleEndpoint).To(ContainSubstring("reference-oracle-br-ok"))
			Expect(br.Status.SubmissionEndpoint).To(ContainSubstring("submission-br-ok"))
			Expect(br.Status.GatewayEndpoint).To(ContainSubstring("fairness-gateway-br-ok"))

			var pods corev1.PodList
			Expect(k8sClient.List(ctx, &pods,
				client.InNamespace(nsn.Namespace),
				client.MatchingLabels{labelRun: br.Name},
			)).To(Succeed())
			Expect(pods.Items).To(HaveLen(4))

			By("deleting the BenchmarkRun triggers pod cleanup then finalizer removal")
			Expect(k8sClient.Delete(ctx, br)).To(Succeed())
			// First delete reconcile: deletes pods (envtest has no kubelet/GC
			// so we manually drive a second pass after the pods disappear).
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nsn})
			Expect(err).NotTo(HaveOccurred())
			// Manually clear pods (no kubelet in envtest to honor DeletionTimestamp).
			Expect(k8sClient.List(ctx, &pods,
				client.InNamespace(nsn.Namespace),
				client.MatchingLabels{labelRun: br.Name},
			)).To(Succeed())
			for i := range pods.Items {
				Expect(k8sClient.Delete(ctx, &pods.Items[i],
					client.GracePeriodSeconds(0),
				)).To(Or(Succeed(), MatchError(ContainSubstring("not found"))))
			}
			// Second pass: no pods → finalizer removed → CR deleted.
			_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: nsn})
			Expect(err).NotTo(HaveOccurred())
			err = k8sClient.Get(ctx, nsn, br)
			Expect(client.IgnoreNotFound(err)).To(Succeed())
		})
	})
})
