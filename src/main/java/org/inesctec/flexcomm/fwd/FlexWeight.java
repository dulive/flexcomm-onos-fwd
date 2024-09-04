package org.inesctec.flexcomm.fwd;

import org.onlab.graph.Weight;

import com.google.common.base.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class FlexWeight implements Weight {

  public static final FlexWeight NON_VIABLE_WEIGHT = new FlexWeight(-1, -1);

  private final double value;
  private final int hops;

  public FlexWeight() {
    value = 0;
    hops = 0;
  }

  public FlexWeight(double value, int hops) {
    this.value = value;
    this.hops = hops;
  }

  public double value() {
    return value;
  }

  public int hops() {
    return hops;
  }

  @Override
  public Weight merge(Weight otherWeight) {
    FlexWeight otherFlex = (FlexWeight) otherWeight;
    return new FlexWeight(value + otherFlex.value, hops + otherFlex.hops);
  }

  @Override
  public Weight subtract(Weight otherWeight) {
    FlexWeight otherFlex = (FlexWeight) otherWeight;
    return new FlexWeight(value - otherFlex.value, hops - otherFlex.hops);
  }

  @Override
  public boolean isViable() {
    return !this.equals(NON_VIABLE_WEIGHT);
  }

  @Override
  public boolean isNegative() {
    return value < 0 || hops < 0;
  }

  public static FlexWeight getNonViableWeight() {
    return NON_VIABLE_WEIGHT;
  }

  @Override
  public int compareTo(Weight o) {
    FlexWeight weight = (FlexWeight) o;

    return value == weight.value
        ? Integer.compare(hops, weight.hops)
        : Double.compare(value, weight.value);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FlexWeight)) {
      return false;
    }

    FlexWeight flexWeight = (FlexWeight) obj;

    return value == flexWeight.value && hops == flexWeight.hops;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value, hops);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("value", value).add("hops", hops).toString();
  }

}
