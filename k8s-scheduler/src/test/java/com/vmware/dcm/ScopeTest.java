/*
 * Copyright 2018-2020 VMware, Inc. All Rights Reserved.
 *
 * SPDX-License-Identifier: BSD-2
 */

package com.vmware.dcm;

import com.vmware.dcm.backend.ortools.OrToolsSolver;
import com.vmware.dcm.k8s.generated.Tables;
import com.vmware.dcm.k8s.generated.tables.records.PodInfoRecord;
import io.fabric8.kubernetes.api.model.Node;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import org.jooq.DSLContext;
import org.jooq.Result;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.vmware.dcm.SchedulerTest.newPod;
import static com.vmware.dcm.SchedulerTest.newNode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for Scope
 */
@ExtendWith({})
public class ScopeTest {
    private static final int NUM_THREADS = 1;

    @Test
    public void testSpareCapacityFilter() {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final DSLContext conn = dbConnectionPool.getConnectionToDb();
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);
        final PodEventsToDatabase eventHandler = new PodEventsToDatabase(dbConnectionPool);
        final PodResourceEventHandler handler = new PodResourceEventHandler(eventHandler::handle);

        final Model model = buildModel(conn);
        final ScopedModel scopedModel = new ScopedModel(conn, model);

        final int numNodes = 10;
        final int numPods = 4;

        for (int i = 0; i < numNodes; i++) {
            final Node node = newNode("n" + i, Collections.emptyMap(), Collections.emptyList());
            // nodes have more spare capacity as i increases
            node.getStatus().getCapacity().put("cpu", new Quantity(String.valueOf(10 + i)));
            node.getStatus().getCapacity().put("memory", new Quantity(String.valueOf(1000)));
            node.getStatus().getCapacity().put("pods", new Quantity(String.valueOf(100)));
            nodeResourceEventHandler.onAddSync(node);
        }
        for (int i = 0; i < numPods; i++) {
            handler.onAddSync(newPod("p" + i));
        }

        final Set<String> scopedNodes = scopedModel.getScopedNodes();

        assertEquals(numPods, scopedNodes.size());
        // check that the last, least loaded nodes are selected
        scopedNodes.forEach(n -> assertTrue(Integer.parseInt(n.substring(1)) >= numNodes - numPods));
    }

    /*
     * E2E test with scheduler:
     * Test if Scope limits candidate nodes according to spare resources
     */
    @Test
    public void testScopedSchedulerSimple() {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final DSLContext conn = dbConnectionPool.getConnectionToDb();
        final List<String> policies = Policies.getDefaultPolicies();
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);

        final PodEventsToDatabase eventHandler = new PodEventsToDatabase(dbConnectionPool);
        final PodResourceEventHandler handler = new PodResourceEventHandler(eventHandler::handle);

        final int numNodes = 100;
        final int numPods = 40;

        for (int i = 0; i < numNodes; i++) {
            final Node node = newNode("n" + i, Collections.emptyMap(), Collections.emptyList());
            // nodes have more spare capacity as i increases
            node.getStatus().getCapacity().put("cpu", new Quantity(String.valueOf(10)));
            node.getStatus().getCapacity().put("memory", new Quantity(String.valueOf(1000 + (1000 * i) / numNodes)));
            node.getStatus().getCapacity().put("pods", new Quantity(String.valueOf(100)));
            nodeResourceEventHandler.onAddSync(node);

            // Add one system pod per node
            final String podName = "system-pod-n" + i;
            final Pod pod = newPod(podName, "Running");
            pod.getSpec().setNodeName("n" + i);
            handler.onAddSync(pod);
        }

        for (int i = 0; i < numPods; i++) {
            handler.onAddSync(newPod("p" + i));
        }

        final Scheduler scheduler = new Scheduler(dbConnectionPool, policies, "ORTOOLS", true, NUM_THREADS);
        scheduler.setScopeOn();
        scheduler.scheduleAllPendingPods(new EmulatedPodToNodeBinder(dbConnectionPool));

        // Check that all pods have been scheduled to a node eligible by the scope filtering
        final Result<PodInfoRecord> fetch = conn.selectFrom(Tables.POD_INFO).fetch();
        assertEquals(numNodes + numPods, fetch.size());
        fetch.forEach(e -> assertTrue(e.getNodeName() != null
                && e.getNodeName().startsWith("n")
                // check that new pods have been scheduled to the last, least loaded nodes
                && (Integer.parseInt(e.getNodeName().substring(1)) >= numNodes - numPods
                || !e.getPodName().startsWith("p"))));
    }

    /*
     * E2E test with scheduler:
     * Test if Scope filters out nodes maintaining while maintaining matching labels.
     * Labeled nodes have low spare capacity.
     */
    @Test
    public void testScopedSchedulerNodeLabels() {
        final DBConnectionPool dbConnectionPool = new DBConnectionPool();
        final DSLContext conn = dbConnectionPool.getConnectionToDb();
        final List<String> policies = Policies.getDefaultPolicies();
        final NodeResourceEventHandler nodeResourceEventHandler = new NodeResourceEventHandler(dbConnectionPool);

        final PodEventsToDatabase eventHandler = new PodEventsToDatabase(dbConnectionPool);
        final PodResourceEventHandler handler = new PodResourceEventHandler(eventHandler::handle);

        final int numNodes = 1000;
        final int numPods = 20;

        final List<Integer> gpuNodesIdx = List.of(0, 1);
        final List<Integer> ssdNodesIdx = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12);
        for (int i = 0; i < numNodes; i++) {
            final String nodeName = "n" + i;
            final Map<String, String> nodeLabels = new HashMap<>();
            if (gpuNodesIdx.contains(i)) {
                nodeLabels.put("gpu", "true");
            }
            if (ssdNodesIdx.contains(i)) {
                nodeLabels.put("ssd", "true");
            }
            final Node node = newNode(nodeName, nodeLabels, Collections.emptyList());

            // nodes have more spare capacity as i increases (favoring unlabeled nodes)
            node.getStatus().getCapacity().put("cpu", new Quantity(String.valueOf(10 + 10 * i / numNodes)));
            node.getStatus().getCapacity().put("memory", new Quantity(String.valueOf(1000 + (1000 * i) / numNodes)));
            node.getStatus().getCapacity().put("pods", new Quantity(String.valueOf(100)));
            nodeResourceEventHandler.onAddSync(node);
        }

        final List<Integer> gpuPodsIdx = List.of(0, 1, 2, 3, 4, 5, 6, 7);
        final List<Integer> ssdPodsIdx = List.of(2, 3, 4, 5, 6, 7, 8, 9);
        for (int i = 0; i < numPods; i++) {
            final String podName = "p" + i;
            final Map<String, String> selectorLabels = new HashMap<>();
            if (gpuPodsIdx.contains(i)) {
                selectorLabels.put("gpu", "true");
            }
            if (ssdPodsIdx.contains(i)) {
                selectorLabels.put("ssd", "true");
            }
            final Pod newPod = newPod(podName, UUID.randomUUID(), "Pending", selectorLabels, Collections.emptyMap());
            handler.onAddSync(newPod);
        }

        final Scheduler scheduler = new Scheduler(dbConnectionPool, policies, "ORTOOLS", true, NUM_THREADS);
        scheduler.setScopeOn();
        scheduler.scheduleAllPendingPods(new EmulatedPodToNodeBinder(dbConnectionPool));

        // Check that all pods have been scheduled to a node eligible by the scope filtering
        final Result<PodInfoRecord> fetch = conn.selectFrom(Tables.POD_INFO).fetch();
        assertEquals(numPods, fetch.size());
        fetch.forEach(e -> assertTrue(e.getNodeName() != null
                // gpuPod => gpuNode
                && (!gpuPodsIdx.contains(Integer.parseInt(e.getPodName().substring(1)))
                || gpuNodesIdx.contains(Integer.parseInt(e.getNodeName().substring(1))))
                // ssdPod => ssdNode
                && (!ssdPodsIdx.contains(Integer.parseInt(e.getPodName().substring(1)))
                || ssdNodesIdx.contains(Integer.parseInt(e.getNodeName().substring(1))))));
    }


    private Model buildModel(final DSLContext conn) {
        final OrToolsSolver orToolsSolver = new OrToolsSolver.Builder()
                .setPrintDiagnostics(true)
                .build();
        return Model.build(conn, orToolsSolver, Policies.getDefaultPolicies());
    }
}
