package org.inesctec.flexcomm.fwd.weights;

import static org.onlab.util.Tools.groupedThreads;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.inesctec.flexcomm.energyclient.api.EnergyPeriod;
import org.inesctec.flexcomm.energyclient.api.EnergyService;
import org.inesctec.flexcomm.ofexp.api.FlexcommEvent;
import org.inesctec.flexcomm.ofexp.api.FlexcommEvent.Type;
import org.inesctec.flexcomm.ofexp.api.FlexcommListener;
import org.inesctec.flexcomm.ofexp.api.FlexcommService;
import org.inesctec.flexcomm.ofexp.api.FlexcommStatistics;
import org.inesctec.flexcomm.ofexp.api.GlobalStatistics;
import org.onlab.graph.Weight;
import org.onosproject.net.DeviceId;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.device.PortStatistics;
import org.onosproject.net.topology.LinkWeigher;
import org.onosproject.net.topology.TopologyEdge;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;

import com.google.common.collect.Maps;

public class FlexWeightCalc implements LinkWeigher {

  @Reference(cardinality = ReferenceCardinality.MANDATORY)
  protected static DeviceService deviceService;

  @Reference(cardinality = ReferenceCardinality.MANDATORY)
  protected static FlexcommService flexcommService;

  @Reference(cardinality = ReferenceCardinality.MANDATORY)
  protected static EnergyService energyService;

  // TODO:
  // check if distributed maps are needed
  private final ConcurrentMap<DeviceId, FlexWeight> weights = Maps.newConcurrentMap();
  private ExecutorService eventExecutor;

  private final FlexcommListener listener = new InternalFlexcommListener();

  public FlexWeightCalc() {
  }

  public void startWeightUpdater() {
    eventExecutor = Executors.newFixedThreadPool(4, groupedThreads("onos/flexcomm/fwd", "weightcalc"));
    flexcommService.addListener(listener);
  }

  public void stopWeightUpdater() {
    flexcommService.removeListener(listener);
    eventExecutor.shutdown();
  }

  public Weight weight(DeviceId deviceId) {
    return weights.get(deviceId);
  }

  @Override
  public Weight weight(TopologyEdge edge) {
    DeviceId src = edge.src().deviceId();
    DeviceId dst = edge.dst().deviceId();

    FlexWeight weight_src = weights.get(src);
    FlexWeight weight_dst = weights.get(dst);

    return new FlexWeight(weight_src.drops() + weight_dst.drops(), weight_src.value() + weight_dst.value(), 1);
  }

  @Override
  public Weight getInitialWeight() {
    return new FlexWeight();
  }

  @Override
  public Weight getNonViableWeight() {
    return FlexWeight.NON_VIABLE_WEIGHT;
  }

  private class InternalFlexcommListener implements FlexcommListener {

    @Override
    public void event(FlexcommEvent event) {
      eventExecutor.execute(() -> processEventInternal(event));
    }

    private void processEventInternal(FlexcommEvent event) {
      FlexcommStatistics flexcommStatistics = event.subject();
      DeviceId deviceId = flexcommStatistics.deviceId();
      if (event.type() == Type.GLOBAL_STATS_UPDATED) {
        GlobalStatistics deltaStats = flexcommService.getGlobalDeltaStatistics(deviceId);
        EnergyPeriod energy = energyService.getCurrentEnergyPeriod(deviceId);
        double max_power_drawn = (energy.estimate() + energy.flexibility()) / 180;
        double value = max_power_drawn - deltaStats.powerDrawn();

        long received = 0;
        long sent = 0;
        for (PortStatistics stats : deviceService.getPortDeltaStatistics(deviceId)) {
          received += stats.packetsReceived();
          sent += stats.packetsSent();
        }

        weights.put(deviceId, new FlexWeight(received - sent, value < 0 ? 1 : 0));
      }
    }
  }
}
