package org.inesctec.flexcomm.fwd.weights;

import org.onlab.graph.Weight;

import com.google.common.base.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class FlexWeight implements Weight {

  public static final FlexWeight NON_VIABLE_WEIGHT = new FlexWeight();

  // packets received - packed transmitted
  private final long drops;
  private final int value;
  private final int hops;

  public FlexWeight() {
    drops = 0;
    value = 0;
    hops = 0;
  }

  public FlexWeight(long drops, int value) {
    this.drops = drops;
    this.value = value;
    this.hops = 0;
  }

  public FlexWeight(long drops, int value, int hops) {
    this.drops = drops;
    this.value = value;
    this.hops = hops;
  }

  public long drops() {
    return drops;
  }

  public int value() {
    return value;
  }

  public int hops() {
    return hops;
  }

  @Override
  public Weight merge(Weight otherWeight) {
    FlexWeight otherFlex = (FlexWeight) otherWeight;
    return new FlexWeight(drops + otherFlex.drops, value + otherFlex.value, hops + otherFlex.hops);
  }

  @Override
  public Weight subtract(Weight otherWeight) {
    FlexWeight otherFlex = (FlexWeight) otherWeight;
    return new FlexWeight(drops - otherFlex.drops, value - otherFlex.value, hops - otherFlex.hops);
  }

  @Override
  public boolean isViable() {
    return !this.equals(NON_VIABLE_WEIGHT);
  }

  @Override
  public boolean isNegative() {
    return drops < 0 || value < 0 || hops < 0;
  }

  public static FlexWeight getNonViableWeight() {
    return NON_VIABLE_WEIGHT;
  }

  @Override
  public int compareTo(Weight o) {
    FlexWeight weight = (FlexWeight) o;

    if (drops != 0 && weight.drops == 0) {
      return 1;
    } else if (drops == 0 && weight.drops != 0) {
      return -1;
    } else {
      return value == weight.value
          ? hops == weight.hops ? Long.compare(drops, weight.drops) : Integer.compare(hops, weight.hops)
          : Integer.compare(value, weight.value);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FlexWeight)) {
      return false;
    }

    FlexWeight flexWeight = (FlexWeight) obj;

    return drops == flexWeight.drops && value == flexWeight.value && hops == flexWeight.hops;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(drops, value, hops);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("drops", drops).add("value", value).add("hops", hops).toString();
  }

}
