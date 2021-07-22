/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.NodeSelectorTerm;
import io.fabric8.kubernetes.api.model.NodeSelectorRequirement;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KubernetesLocalExpr {
    private static final Logger LOG = LoggerFactory.getLogger(KubernetesLocalExpr.class);
    private final List<ListenableFuture<?>> deletions = new ArrayList<>();
    private final ListeningScheduledExecutorService scheduledExecutorService =
            MoreExecutors.listeningDecorator(Executors.newScheduledThreadPool(100));

    public void runSimple(final NamespacedKubernetesClient client, final String configFileName,
                          final IPodDeployer deployer)
            throws Exception {
        // TODO: rich log info
        LOG.info("Creating local deployment");
        final int startTimeSec = (int) System.currentTimeMillis() * 1000;

        // Load configuration from file
        try (final InputStream inputStream = getClass().getClassLoader().getResourceAsStream(configFileName)) {
            final Yaml yaml = new Yaml();
            final Map<String, Object> config = yaml.load(inputStream);
            final int podsPerPriority = (int) config.get("highPriorityPods");
            final int pr1StartTimeSec = (int) config.get("pr1StartTimeSec");
            final int pr2StartTimeSec = (int) config.get("pr2StartTimeSec");
            final int pr2EndTimeSec = (int) config.get("pr2EndTimeSec");
            final int pr1EndTimeSec = (int) config.get("pr1EndTimeSec");
            final int waitTimeSec = (int) config.get("waitTimeSec");
            final String podFile = "priority-test/pod-nodeaf-nodeantiaf.yml";

            final List<Pod> deployment1 = getDeploymentNodeAffinity(client, "priority-level1",
                                                                    podsPerPriority, podFile);
            final List<Pod> deployment2 = getDeploymentNodeAffinity(client, "priority-level2",
                                                                    podsPerPriority, podFile);

            final ListenableFuture<?> scheduledStart1 = scheduledExecutorService.schedule(
                    deployer.startDeployment(deployment1), pr1StartTimeSec, TimeUnit.SECONDS);
            final ListenableFuture<?> scheduledStart2 = scheduledExecutorService.schedule(
                    deployer.startDeployment(deployment2), pr2StartTimeSec, TimeUnit.SECONDS);

            final SettableFuture<Boolean> onComplete2 = SettableFuture.create();
            scheduledStart2.addListener(() -> {
                    final ListenableScheduledFuture<?> deletion =
                        scheduledExecutorService.schedule(deployer.endDeployment(deployment2),
                                                          pr2EndTimeSec, TimeUnit.SECONDS);
                    deletion.addListener(() -> onComplete2.set(true), scheduledExecutorService);
                }, scheduledExecutorService);
            deletions.add(onComplete2);
            final SettableFuture<Boolean> onComplete1 = SettableFuture.create();
            scheduledStart1.addListener(() -> {
                    final ListenableScheduledFuture<?> deletion =
                        scheduledExecutorService.schedule(deployer.endDeployment(deployment1),
                                                          pr1EndTimeSec, TimeUnit.SECONDS);
                    deletion.addListener(() -> onComplete1.set(true), scheduledExecutorService);
                }, scheduledExecutorService);
            deletions.add(onComplete1);

        final int exprTimeSec = (int) System.currentTimeMillis() * 1000 - startTimeSec;
        final List<Object> objects = Futures.successfulAsList(deletions)
            .get(pr2EndTimeSec + waitTimeSec + exprTimeSec, TimeUnit.SECONDS);
            assert objects.size() != 0;

        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void runFull(final NamespacedKubernetesClient client, final String configFileName,
                        final IPodDeployer deployer)
            throws Exception {
        // TODO: rich log info
        LOG.info("Creating local deployment");
        final int startTimeSec = (int) System.currentTimeMillis() * 1000;

        // Load configuration from file
        try (final InputStream inputStream = getClass().getClassLoader().getResourceAsStream(configFileName)) {
            final Yaml yaml = new Yaml();
            final Map<String, Object> config = yaml.load(inputStream);
            final int highPriorityPods = (int) config.get("highPriorityPods");
            final int medPriorityPods = (int) config.get("medPriorityPods");
            final int lowPriorityPods = (int) config.get("lowPriorityPods");
            final int schStartTimeSec = (int) config.get("pr1StartTimeSec");
            final int schEndTimeSec = (int) config.get("pr1EndTimeSec");
            final int waitTimeSec = (int) config.get("waitTimeSec");

            final List<Pod> deployment1 = getDeployment(client, "priority-level1", lowPriorityPods,
                                                        "priority-test/pod-lowprior.yml");
            final List<Pod> deployment2 = getDeployment(client, "priority-level2", medPriorityPods,
                                                        "priority-test/pod-lowprior.yml");
            final List<Pod> deployment3 = getDeployment(client, "priority-level3", highPriorityPods,
                                                        "priority-test/pod-highprior.yml");

            final ListenableFuture<?> scheduledStart1 = scheduledExecutorService.schedule(
                    deployer.startDeployment(deployment1), schStartTimeSec, TimeUnit.SECONDS);
            final ListenableFuture<?> scheduledStart2 = scheduledExecutorService.schedule(
                    deployer.startDeployment(deployment2), schStartTimeSec, TimeUnit.SECONDS);
            final ListenableFuture<?> scheduledStart3 = scheduledExecutorService.schedule(
                    deployer.startDeployment(deployment3), schStartTimeSec, TimeUnit.SECONDS);

            final SettableFuture<Boolean> onComplete1 = SettableFuture.create();
            scheduledStart1.addListener(() -> {
                    final ListenableScheduledFuture<?> deletion =
                        scheduledExecutorService.schedule(deployer.endDeployment(deployment1),
                                                          schEndTimeSec, TimeUnit.SECONDS);
                    deletion.addListener(() -> onComplete1.set(true), scheduledExecutorService);
                }, scheduledExecutorService);
            deletions.add(onComplete1);
            final SettableFuture<Boolean> onComplete2 = SettableFuture.create();
            scheduledStart2.addListener(() -> {
                    final ListenableScheduledFuture<?> deletion =
                        scheduledExecutorService.schedule(deployer.endDeployment(deployment2),
                                                          schEndTimeSec, TimeUnit.SECONDS);
                    deletion.addListener(() -> onComplete2.set(true), scheduledExecutorService);
                }, scheduledExecutorService);
            deletions.add(onComplete2);
            final SettableFuture<Boolean> onComplete3 = SettableFuture.create();
            scheduledStart3.addListener(() -> {
                    final ListenableScheduledFuture<?> deletion =
                        scheduledExecutorService.schedule(deployer.endDeployment(deployment3),
                                                          schEndTimeSec, TimeUnit.SECONDS);
                    deletion.addListener(() -> onComplete3.set(true), scheduledExecutorService);
                }, scheduledExecutorService);
            deletions.add(onComplete3);

        final int exprTimeSec = (int) System.currentTimeMillis() * 1000 - startTimeSec;
        final List<Object> objects = Futures.successfulAsList(deletions)
            .get(schEndTimeSec + waitTimeSec + exprTimeSec, TimeUnit.SECONDS);
            assert objects.size() != 0;

        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private List<Pod> getDeployment(final NamespacedKubernetesClient client, final String priority,
                                        final int totalPods, final String podFile) {
        // Load the template file and update its contents to generate a new deployment template
        return IntStream.range(0, totalPods)
                .mapToObj(podCount -> {
                    try (final InputStream fileStream =
                                 getClass().getClassLoader().getResourceAsStream(podFile)) {
                        final Pod pod = client.pods().load(fileStream).get();

                        final String podName = "pod-" + priority + "-" + podCount;
                        pod.getMetadata().setName(podName);
                        pod.getSpec().setPriorityClassName(priority);

                        return pod;
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
    }

    private List<Pod> getDeploymentNodeAffinity(final NamespacedKubernetesClient client,
                                                final String priority, final int totalPods,
                                                final String podFile) {
        // Load the template file and update its contents to generate a new deployment template
        return IntStream.range(0, totalPods)
                .mapToObj(podCount -> {
                    try (final InputStream fileStream =
                                 getClass().getClassLoader().getResourceAsStream(podFile)) {
                        final Pod pod = client.pods().load(fileStream).get();

                        final String podName = "pod-" + priority + "-" + podCount;
                        pod.getMetadata().setName(podName);
                        pod.getSpec().setPriorityClassName(priority);

                        final List<NodeSelectorTerm> nodeAffinityTerms = pod.getSpec().getAffinity()
                            .getNodeAffinity().getRequiredDuringSchedulingIgnoredDuringExecution()
                            .getNodeSelectorTerms();
                        for (final NodeSelectorTerm term: nodeAffinityTerms)  {
                            final List<NodeSelectorRequirement> requirements =
                                term.getMatchExpressions();
                            for (final NodeSelectorRequirement requirement: requirements) {
                                final String nodeSuffix = (podCount > 0) ?
                                    String.valueOf(podCount + 1) : "";
                                
                                final List<String> newValues = new ArrayList<>();
                                requirement.getValues().forEach(v -> newValues.add(v + nodeSuffix));
                                requirement.setValues(newValues);
                            }
                        }

                        return pod;
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toList());
    }
}

