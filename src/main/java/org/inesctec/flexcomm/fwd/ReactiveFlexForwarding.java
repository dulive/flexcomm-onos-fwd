package org.inesctec.flexcomm.fwd;

import static org.inesctec.flexcomm.fwd.OsgiPropertyConstants.FLOW_PRIORITY;
import static org.inesctec.flexcomm.fwd.OsgiPropertyConstants.FLOW_PRIORITY_DEFAULT;
import static org.inesctec.flexcomm.fwd.OsgiPropertyConstants.FLOW_TIMEOUT;
import static org.inesctec.flexcomm.fwd.OsgiPropertyConstants.FLOW_TIMEOUT_DEFAULT;
import static org.inesctec.flexcomm.fwd.OsgiPropertyConstants.INHERIT_FLOW_TREATMENT;
import static org.inesctec.flexcomm.fwd.OsgiPropertyConstants.INHERIT_FLOW_TREATMENT_DEFAULT;
import static org.inesctec.flexcomm.fwd.OsgiPropertyConstants.IPV6_FORWARDING;
import static org.inesctec.flexcomm.fwd.OsgiPropertyConstants.IPV6_FORWARDING_DEFAULT;
import static org.inesctec.flexcomm.fwd.OsgiPropertyConstants.PACKET_OUT_OFPP_TABLE;
import static org.inesctec.flexcomm.fwd.OsgiPropertyConstants.PACKET_OUT_OFPP_TABLE_DEFAULT;
import static org.inesctec.flexcomm.fwd.OsgiPropertyConstants.PACKET_OUT_ONLY;
import static org.inesctec.flexcomm.fwd.OsgiPropertyConstants.PACKET_OUT_ONLY_DEFAULT;
import static org.onlab.util.Tools.groupedThreads;

import java.util.Dictionary;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.inesctec.flexcomm.energy.api.EnergyPeriod;
import org.inesctec.flexcomm.energy.api.FlexcommEnergyService;
import org.inesctec.flexcomm.fwd.weights.FlexWeight;
import org.inesctec.flexcomm.fwd.weights.FlexWeightCalc;
import org.inesctec.flexcomm.statistics.api.FlexcommStatisticsEvent;
import org.inesctec.flexcomm.statistics.api.FlexcommStatisticsEvent.Type;
import org.inesctec.flexcomm.statistics.api.FlexcommStatisticsListener;
import org.inesctec.flexcomm.statistics.api.FlexcommStatisticsService;
import org.inesctec.flexcomm.statistics.api.GlobalStatistics;
import org.onlab.packet.Ethernet;
import org.onlab.packet.IPv4;
import org.onlab.packet.IPv6;
import org.onlab.packet.Ip4Address;
import org.onlab.packet.Ip4Prefix;
import org.onlab.packet.Ip6Prefix;
import org.onlab.packet.IpAddress;
import org.onlab.packet.MacAddress;
import org.onlab.packet.TCP;
import org.onlab.packet.TpPort;
import org.onlab.packet.UDP;
import org.onlab.util.Tools;
import org.onosproject.cfg.ComponentConfigService;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.event.Event;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Host;
import org.onosproject.net.HostId;
import org.onosproject.net.Link;
import org.onosproject.net.Path;
import org.onosproject.net.PortNumber;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.device.PortStatistics;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.FlowEntry;
import org.onosproject.net.flow.FlowRule;
import org.onosproject.net.flow.FlowRuleService;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.TrafficTreatment;
import org.onosproject.net.flow.criteria.Criterion;
import org.onosproject.net.flow.criteria.EthCriterion;
import org.onosproject.net.flow.instructions.Instruction;
import org.onosproject.net.flow.instructions.Instructions;
import org.onosproject.net.flowobjective.DefaultForwardingObjective;
import org.onosproject.net.flowobjective.FlowObjectiveService;
import org.onosproject.net.flowobjective.ForwardingObjective;
import org.onosproject.net.host.HostService;
import org.onosproject.net.link.LinkEvent;
import org.onosproject.net.packet.InboundPacket;
import org.onosproject.net.packet.PacketContext;
import org.onosproject.net.packet.PacketPriority;
import org.onosproject.net.packet.PacketProcessor;
import org.onosproject.net.packet.PacketService;
import org.onosproject.net.topology.TopologyEvent;
import org.onosproject.net.topology.TopologyListener;
import org.onosproject.net.topology.TopologyService;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

@Component(immediate = true, service = ReactiveFlexForwarding.class, property = {
    PACKET_OUT_ONLY + ":Boolean=" + PACKET_OUT_ONLY_DEFAULT,
    PACKET_OUT_OFPP_TABLE + ":Boolean=" + PACKET_OUT_OFPP_TABLE_DEFAULT,
    FLOW_TIMEOUT + ":Integer=" + FLOW_TIMEOUT_DEFAULT,
    FLOW_PRIORITY + ":Integer=" + FLOW_PRIORITY_DEFAULT,
    IPV6_FORWARDING + ":Boolean=" + IPV6_FORWARDING_DEFAULT,
    INHERIT_FLOW_TREATMENT + ":Boolean=" + INHERIT_FLOW_TREATMENT_DEFAULT })
public class ReactiveFlexForwarding {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private static final String APP_NAME = "org.inesctec.flexcomm.routing.partialpath";

  @Reference(cardinality = ReferenceCardinality.MANDATORY)
  protected CoreService coreService;

  @Reference(cardinality = ReferenceCardinality.MANDATORY)
  protected FlowRuleService flowRuleService;

  @Reference(cardinality = ReferenceCardinality.MANDATORY)
  protected FlowObjectiveService flowObjectiveService;

  @Reference(cardinality = ReferenceCardinality.MANDATORY)
  protected TopologyService topologyService;

  @Reference(cardinality = ReferenceCardinality.MANDATORY)
  protected PacketService packetService;

  @Reference(cardinality = ReferenceCardinality.MANDATORY)
  protected HostService hostService;

  @Reference(cardinality = ReferenceCardinality.MANDATORY)
  protected DeviceService deviceService;

  @Reference(cardinality = ReferenceCardinality.MANDATORY)
  protected FlexcommStatisticsService statisticsService;

  @Reference(cardinality = ReferenceCardinality.MANDATORY)
  protected FlexcommEnergyService energyService;

  @Reference(cardinality = ReferenceCardinality.MANDATORY)
  protected ComponentConfigService cfgService;

  private final PacketProcessor processor = new InternalPacketProcessor();
  private final TopologyListener topologyListener = new InternalTopologyListener();
  private final FlexcommStatisticsListener statisticsListener = new InternalStatisticsListener();

  private ExecutorService weightCalcExecutor;
  private ExecutorService linkRemovedExecutor;
  private FlexWeightCalc weigher;

  protected ApplicationId appId;

  private boolean packetOutOnly = PACKET_OUT_ONLY_DEFAULT;

  private boolean packetOutOfppTable = PACKET_OUT_OFPP_TABLE_DEFAULT;

  private int flowTimeout = FLOW_TIMEOUT_DEFAULT;

  private int flowPriority = FLOW_PRIORITY_DEFAULT;

  private boolean ipv6Forwarding = IPV6_FORWARDING_DEFAULT;

  private boolean inheritFlowTreatment = INHERIT_FLOW_TREATMENT_DEFAULT;

  @Activate
  protected void activate(ComponentContext context) {
    appId = coreService.registerApplication(APP_NAME);
    cfgService.registerProperties(getClass());

    weigher = new FlexWeightCalc();

    weightCalcExecutor = Executors.newFixedThreadPool(4, groupedThreads("onos/flexcomm/fwd", "weight-calc", log));
    linkRemovedExecutor = Executors.newSingleThreadExecutor(groupedThreads("onos/flexcomm/fwd", "link-removed", log));

    statisticsService.addListener(statisticsListener);
    packetService.addProcessor(processor, PacketProcessor.director(2));
    topologyService.addListener(topologyListener);

    readComponentConfiguration(context);
    requestIntercepts();

    log.info("Started", appId.id());
  }

  @Deactivate
  protected void deactivate() {
    cfgService.unregisterProperties(getClass(), false);
    withdrawIntercepts();
    packetService.removeProcessor(processor);
    topologyService.removeListener(topologyListener);
    statisticsService.removeListener(statisticsListener);
    weightCalcExecutor.shutdown();
    weightCalcExecutor = null;
    linkRemovedExecutor.shutdown();
    linkRemovedExecutor = null;
    flowRuleService.removeFlowRulesById(appId);
    log.info("Stopped", appId.id());
  }

  @Modified
  public void modified(ComponentContext context) {
    readComponentConfiguration(context);
    requestIntercepts();
  }

  private void readComponentConfiguration(ComponentContext context) {
    Dictionary<?, ?> properties = context.getProperties();

    Boolean packetOutOnlyEnabled = Tools.isPropertyEnabled(properties, PACKET_OUT_ONLY);
    if (packetOutOnlyEnabled == null) {
      log.info("Packet-out is not configured, using current value of {}", packetOutOnly);
    } else {
      packetOutOnly = packetOutOnlyEnabled;
      log.info("Configured. Packet-out only forwarding is {}", packetOutOnly ? "enabled" : "disabled");
    }

    Boolean packetOutOfppTableEnabled = Tools.isPropertyEnabled(properties, PACKET_OUT_OFPP_TABLE);
    if (packetOutOfppTableEnabled == null) {
      log.info("OFPP_TABLE port is not configured, using current value of {}", packetOutOfppTable);
    } else {
      packetOutOfppTable = packetOutOfppTableEnabled;
      log.info("Configured. Forwarding using OFPP_TABLE port is {}", packetOutOfppTable ? "enabled" : "disabled");
    }

    flowTimeout = Tools.getIntegerProperty(properties, FLOW_TIMEOUT, FLOW_TIMEOUT_DEFAULT);
    log.info("Configured. Flow Timeout is configured to {} seconds", flowTimeout);

    flowPriority = Tools.getIntegerProperty(properties, FLOW_PRIORITY, FLOW_PRIORITY_DEFAULT);
    log.info("Configured. Flow Priority is configured to {}", flowPriority);

    Boolean ipv6ForwardingEnabled = Tools.isPropertyEnabled(properties, IPV6_FORWARDING);
    if (ipv6ForwardingEnabled == null) {
      log.info("IPv6 forwarding is not configured, using current value of {}", ipv6Forwarding);
    } else {
      ipv6Forwarding = ipv6ForwardingEnabled;
      log.info("Configured. IPv6 forwarding is {}", ipv6Forwarding ? "enabled" : "disabled");
    }

    Boolean inheritFlowTreatmentEnabled = Tools.isPropertyEnabled(properties, INHERIT_FLOW_TREATMENT);
    if (inheritFlowTreatmentEnabled == null) {
      log.info("Inherit flow treatment is not configured, using current value of {}", inheritFlowTreatment);
    } else {
      inheritFlowTreatment = inheritFlowTreatmentEnabled;
      log.info("Configured. Inherit flow treatment is {}", inheritFlowTreatment ? "enabled" : "disabled");
    }
  }

  private void requestIntercepts() {
    TrafficSelector.Builder selector = DefaultTrafficSelector.builder();
    selector.matchEthType(Ethernet.TYPE_IPV4);
    packetService.requestPackets(selector.build(), PacketPriority.REACTIVE, appId);

    selector.matchEthType(Ethernet.TYPE_IPV6);
    if (ipv6Forwarding) {
      packetService.requestPackets(selector.build(), PacketPriority.REACTIVE, appId);
    } else {
      packetService.cancelPackets(selector.build(), PacketPriority.REACTIVE, appId);
    }
  }

  private void withdrawIntercepts() {
    TrafficSelector.Builder selector = DefaultTrafficSelector.builder();
    selector.matchEthType(Ethernet.TYPE_IPV4);
    packetService.cancelPackets(selector.build(), PacketPriority.REACTIVE, appId);
    selector.matchEthType(Ethernet.TYPE_IPV6);
    packetService.cancelPackets(selector.build(), PacketPriority.REACTIVE, appId);
  }

  private void flood(PacketContext context) {
    if (topologyService.isBroadcastPoint(topologyService.currentTopology(),
        context.inPacket().receivedFrom())) {
      packetOut(context, PortNumber.FLOOD);
    } else {
      context.block();
    }
  }

  private void packetOut(PacketContext context, PortNumber portNumber) {
    context.treatmentBuilder().setOutput(portNumber);
    context.send();
  }

  private Path pickForwardPathIfPossible(Set<Path> paths, PortNumber notToPort) {
    for (Path path : paths) {
      if (!path.src().port().equals(notToPort)) {
        return path;
      }
    }
    return null;
  }

  private void installRules(PacketContext context, PortNumber portNumber) {
    Ethernet inPkt = context.inPacket().parsed();
    TrafficSelector.Builder selectorBuilder = DefaultTrafficSelector.builder();

    if (packetOutOnly || inPkt.getEtherType() == Ethernet.TYPE_ARP) {
      packetOut(context, portNumber);
      return;
    }

    if (inPkt.getEtherType() == Ethernet.TYPE_IPV4) {
      IPv4 ipv4Packet = (IPv4) inPkt.getPayload();
      byte proto = ipv4Packet.getProtocol();
      selectorBuilder.matchEthType(Ethernet.TYPE_IPV4)
          .matchIPSrc(Ip4Prefix.valueOf(ipv4Packet.getSourceAddress(), Ip4Prefix.MAX_MASK_LENGTH))
          .matchIPDst(Ip4Prefix.valueOf(ipv4Packet.getDestinationAddress(), Ip4Prefix.MAX_MASK_LENGTH))
          .matchIPProtocol(proto);

      if (proto == IPv4.PROTOCOL_TCP) {
        TCP tcpPacket = (TCP) ipv4Packet.getPayload();
        selectorBuilder.matchTcpSrc(TpPort.tpPort(tcpPacket.getSourcePort()))
            .matchTcpDst(TpPort.tpPort(tcpPacket.getDestinationPort()));
      }
      if (proto == IPv4.PROTOCOL_UDP) {
        UDP udpPacket = (UDP) ipv4Packet.getPayload();
        selectorBuilder.matchUdpSrc(TpPort.tpPort(udpPacket.getSourcePort()))
            .matchUdpDst(TpPort.tpPort(udpPacket.getDestinationPort()));
      }
    }

    if (inPkt.getEtherType() == Ethernet.TYPE_IPV6) {
      IPv6 ipv6Packet = (IPv6) inPkt.getPayload();
      byte nextHeader = ipv6Packet.getNextHeader();
      selectorBuilder.matchEthType(Ethernet.TYPE_IPV6)
          .matchIPSrc(Ip6Prefix.valueOf(ipv6Packet.getSourceAddress(), Ip6Prefix.MAX_MASK_LENGTH))
          .matchIPDst(Ip4Prefix.valueOf(ipv6Packet.getDestinationAddress(), Ip6Prefix.MAX_MASK_LENGTH))
          .matchIPProtocol(nextHeader);

      if (nextHeader == IPv6.PROTOCOL_TCP) {
        TCP tcpPacket = (TCP) ipv6Packet.getPayload();
        selectorBuilder.matchTcpSrc(TpPort.tpPort(tcpPacket.getSourcePort()))
            .matchTcpDst(TpPort.tpPort(tcpPacket.getDestinationPort()));
      }
      if (nextHeader == IPv6.PROTOCOL_UDP) {
        UDP udpPacket = (UDP) ipv6Packet.getPayload();
        selectorBuilder.matchUdpSrc(TpPort.tpPort(udpPacket.getSourcePort()))
            .matchUdpDst(TpPort.tpPort(udpPacket.getDestinationPort()));
      }
    }

    TrafficTreatment treatment;
    if (inheritFlowTreatment) {
      treatment = context.treatmentBuilder().setOutput(portNumber).build();
    } else {
      treatment = DefaultTrafficTreatment.builder().setOutput(portNumber).build();
    }

    ForwardingObjective forwardingObjective = DefaultForwardingObjective.builder()
        .withSelector(selectorBuilder.build())
        .withTreatment(treatment)
        .withPriority(flowPriority)
        .withFlag(ForwardingObjective.Flag.VERSATILE)
        .fromApp(appId)
        .makeTemporary(flowTimeout)
        .add();

    flowObjectiveService.forward(context.inPacket().receivedFrom().deviceId(), forwardingObjective);

    if (packetOutOfppTable) {
      packetOut(context, PortNumber.TABLE);
    } else {
      packetOut(context, portNumber);
    }
  }

  private boolean isControlPacket(Ethernet eth) {
    short type = eth.getEtherType();
    return type == Ethernet.TYPE_LLDP || type == Ethernet.TYPE_BSN;
  }

  // packet in
  private class InternalPacketProcessor implements PacketProcessor {

    @Override
    public void process(PacketContext context) {
      if (context.isHandled()) {
        return;
      }

      InboundPacket pkt = context.inPacket();
      Ethernet ethPkt = pkt.parsed();

      if (ethPkt == null || isControlPacket(ethPkt)) {
        return;
      }

      HostId id = HostId.hostId(ethPkt.getDestinationMAC());

      if (id.mac().isLldp()) {
        return;
      }

      Host dst = hostService.getHost(id);
      if (dst == null) {
        IPv4 ipv4Packet = (IPv4) ethPkt.getPayload();
        IpAddress dstIp = Ip4Address.valueOf(ipv4Packet.getDestinationAddress());

        dst = hostService.getHostsByIp(dstIp).stream().findFirst().orElse(null);
        if (dst == null) {
          flood(context);
          return;
        }
      }

      if (pkt.receivedFrom().deviceId().equals(dst.location().deviceId())) {
        if (!context.inPacket().receivedFrom().port().equals(dst.location().port())) {
          installRules(context, dst.location().port());
        }
        return;
      }

      Set<Path> paths = topologyService.getPaths(topologyService.currentTopology(), pkt.receivedFrom().deviceId(),
          dst.location().deviceId(), weigher);

      if (paths.isEmpty()) {
        flood(context);
        return;
      }

      Path path = pickForwardPathIfPossible(paths, pkt.receivedFrom().port());
      if (path == null) {
        log.warn("Don't know where to go from here {} for {} -> {}",
            pkt.receivedFrom(), ethPkt.getSourceMAC(), ethPkt.getDestinationMAC());
        flood(context);
        return;
      }

      installRules(context, path.src().port());
    }
  }

  private class InternalTopologyListener implements TopologyListener {

    @Override
    public void event(TopologyEvent event) {
      List<Event> reasons = event.reasons();
      if (reasons != null) {
        reasons.forEach(re -> {
          if (re instanceof LinkEvent) {
            LinkEvent le = (LinkEvent) re;
            if (le.type() == LinkEvent.Type.LINK_REMOVED && linkRemovedExecutor != null) {
              linkRemovedExecutor.submit(() -> fixLinkRemoved(le.subject().src()));
            }
          }
        });
      }
    }

    private void fixLinkRemoved(ConnectPoint egress) {
      Set<FlowEntry> rules = getFlowRulesFrom(egress);
      Set<SrcDstPair> pairs = findSrcDstPairs(rules);

      Map<DeviceId, Set<Path>> srcPaths = new HashMap<>();

      for (SrcDstPair sd : pairs) {
        Host srcHost = hostService.getHost(HostId.hostId(sd.src));
        Host dstHost = hostService.getHost(HostId.hostId(sd.dst));
        if (srcHost != null && dstHost != null) {
          DeviceId srcId = srcHost.location().deviceId();
          DeviceId dstId = dstHost.location().deviceId();
          log.trace("SRC ID is {}, DST ID is {}", srcId, dstId);

          cleanFlowRules(sd, egress.deviceId());
          Set<Path> shortestPaths = srcPaths.get(srcId);
          if (shortestPaths == null) {
            shortestPaths = topologyService.getPaths(topologyService.currentTopology(), egress.deviceId(), srcId,
                weigher);
            srcPaths.put(srcId, shortestPaths);
          }
          backTrackBadNodes(shortestPaths, dstId, sd);
        }
      }
    }

    private Set<FlowEntry> getFlowRulesFrom(ConnectPoint egress) {
      ImmutableSet.Builder<FlowEntry> builder = ImmutableSet.builder();
      flowRuleService.getFlowEntries(egress.deviceId()).forEach(r -> {
        if (r.appId() == appId.id()) {
          r.treatment().allInstructions().forEach(i -> {
            if (i.type() == Instruction.Type.OUTPUT) {
              if (((Instructions.OutputInstruction) i).port().equals(egress.port())) {
                builder.add(r);
              }
            }
          });
        }
      });

      return builder.build();
    }

    private Set<SrcDstPair> findSrcDstPairs(Set<FlowEntry> rules) {
      ImmutableSet.Builder<SrcDstPair> builder = ImmutableSet.builder();
      for (FlowEntry r : rules) {
        MacAddress src = null, dst = null;
        for (Criterion cr : r.selector().criteria()) {
          if (cr.type() == Criterion.Type.ETH_DST) {
            dst = ((EthCriterion) cr).mac();
          } else if (cr.type() == Criterion.Type.ETH_SRC) {
            src = ((EthCriterion) cr).mac();
          }
        }
        builder.add(new SrcDstPair(src, dst));
      }
      return builder.build();
    }

    private void cleanFlowRules(SrcDstPair pair, DeviceId id) {
      log.trace("Searching for flow rules to remove from: {}", id);
      log.trace("Removing flows w/ SRC={}, DST={}", pair.src, pair.dst);
      for (FlowEntry r : flowRuleService.getFlowEntries(id)) {
        boolean matchesSrc = false, matchesDst = false;
        for (Instruction i : r.treatment().allInstructions()) {
          if (i.type() == Instruction.Type.OUTPUT) {
            for (Criterion cr : r.selector().criteria()) {
              if (cr.type() == Criterion.Type.ETH_DST) {
                if (((EthCriterion) cr).mac().equals(pair.dst)) {
                  matchesDst = true;
                }
              } else if (cr.type() == Criterion.Type.ETH_SRC) {
                if (((EthCriterion) cr).mac().equals(pair.src)) {
                  matchesSrc = true;
                }
              }
            }
          }
        }
        if (matchesDst && matchesSrc) {
          log.trace("Removed flow rule from device: {}", id);
          flowRuleService.removeFlowRules((FlowRule) r);
        }
      }
    }

    private void backTrackBadNodes(Set<Path> shortestPaths, DeviceId dstId, SrcDstPair sd) {
      for (Path p : shortestPaths) {
        List<Link> pathLinks = p.links();
        for (int i = 0; i < pathLinks.size(); i = i + 1) {
          Link curLink = pathLinks.get(i);
          DeviceId curDevice = curLink.src().deviceId();

          if (i != 0) {
            cleanFlowRules(sd, curDevice);
          }

          Set<Path> pathsFromCurDevice = topologyService.getPaths(topologyService.currentTopology(), curDevice, dstId);
          if (pickForwardPathIfPossible(pathsFromCurDevice, curLink.src().port()) != null) {
            break;
          } else {
            if (i + 1 == pathLinks.size()) {
              cleanFlowRules(sd, curLink.dst().deviceId());
            }
          }
        }
      }
    }
  }

  private class InternalStatisticsListener implements FlexcommStatisticsListener {

    @Override
    public void event(FlexcommStatisticsEvent event) {
      if (event.type() == Type.GLOBAL_STATS_UPDATED && weightCalcExecutor != null) {
        weightCalcExecutor.submit(() -> calculateWeight(event.subject()));
      }
    }

    private void calculateWeight(DeviceId deviceId) {
      GlobalStatistics deltaStats = statisticsService.getGlobalDeltaStatistics(deviceId);

      double value = 0;
      EnergyPeriod energy = energyService.getCurrentEnergyPeriod(deviceId);
      if (energy != null) {
        double max_power_drawn = (energy.estimate() + energy.flexibility()) / 180;
        value = max_power_drawn - deltaStats.powerDrawn();
      }

      long received = 0;
      long sent = 0;
      for (PortStatistics stats : deviceService.getPortDeltaStatistics(deviceId)) {
        received += stats.packetsReceived();
        sent += stats.packetsSent();
      }

      weigher.setWeight(deviceId, new FlexWeight(received - sent, value < 0 ? 1 : 0));
    }

  }

  private final class SrcDstPair {
    final MacAddress src;
    final MacAddress dst;

    private SrcDstPair(MacAddress src, MacAddress dst) {
      this.src = src;
      this.dst = dst;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SrcDstPair that = (SrcDstPair) o;
      return Objects.equals(src, that.src) && Objects.equals(dst, that.dst);
    }

    @Override
    public int hashCode() {
      return Objects.hash(src, dst);
    }
  }
}
