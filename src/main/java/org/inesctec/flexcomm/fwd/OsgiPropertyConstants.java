package org.inesctec.flexcomm.fwd;

public final class OsgiPropertyConstants {
  private OsgiPropertyConstants() {

  }

  static final String PACKET_OUT_ONLY = "packetOutOnly";
  static final boolean PACKET_OUT_ONLY_DEFAULT = false;

  static final String PACKET_OUT_OFPP_TABLE = "packetOutOfppTable";
  static final boolean PACKET_OUT_OFPP_TABLE_DEFAULT = false;

  static final String FLOW_TIMEOUT = "flowTimeout";
  static final int FLOW_TIMEOUT_DEFAULT = 10;

  static final String FLOW_PRIORITY = "flowPriority";
  static final int FLOW_PRIORITY_DEFAULT = 10;

  static final String IPV6_FORWARDING = "ipv6Forwarding";
  static final boolean IPV6_FORWARDING_DEFAULT = false;

  static final String INHERIT_FLOW_TREATMENT = "inheritFlowTreatment";
  static final boolean INHERIT_FLOW_TREATMENT_DEFAULT = false;
}
