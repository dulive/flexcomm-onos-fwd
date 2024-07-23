package org.inesctec.flexcomm.fwd.weights;

import java.util.concurrent.ConcurrentMap;

import org.onlab.graph.Weight;
import org.onosproject.net.DeviceId;
import org.onosproject.net.topology.LinkWeigher;
import org.onosproject.net.topology.TopologyEdge;

import com.google.common.collect.Maps;

public class FlexWeightCalc implements LinkWeigher {

  // TODO:
  // check if distributed maps are needed
  private final ConcurrentMap<DeviceId, FlexWeight> weights = Maps.newConcurrentMap();

  public FlexWeightCalc() {
  }

  public void setWeight(DeviceId deviceId, FlexWeight weight) {
    weights.put(deviceId, weight);
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
    return new FlexWeight(weight_src.value() + weight_dst.value(), 1);
  }

  @Override
  public Weight getInitialWeight() {
    return new FlexWeight();
  }

  @Override
  public Weight getNonViableWeight() {
    return FlexWeight.NON_VIABLE_WEIGHT;
  }
}
