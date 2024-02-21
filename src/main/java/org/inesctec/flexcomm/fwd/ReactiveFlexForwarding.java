package org.inesctec.flexcomm.fwd;

import static org.onlab.util.Tools.groupedThreads;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.inesctec.flexcomm.energyclient.api.EnergyPeriod;
import org.inesctec.flexcomm.energyclient.api.EnergyService;
import org.inesctec.flexcomm.fwd.weights.FlexWeight;
import org.inesctec.flexcomm.fwd.weights.FlexWeightCalc;
import org.inesctec.flexcomm.ofexp.api.FlexcommEvent;
import org.inesctec.flexcomm.ofexp.api.FlexcommEvent.Type;
import org.inesctec.flexcomm.ofexp.api.FlexcommListener;
import org.inesctec.flexcomm.ofexp.api.FlexcommService;
import org.inesctec.flexcomm.ofexp.api.FlexcommStatistics;
import org.inesctec.flexcomm.ofexp.api.GlobalStatistics;
import org.onlab.packet.Ethernet;
import org.onlab.packet.IPv4;
import org.onlab.packet.Ip4Address;
import org.onlab.packet.Ip4Prefix;
import org.onlab.packet.IpAddress;
import org.onlab.packet.TCP;
import org.onlab.packet.TpPort;
import org.onlab.packet.UDP;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Host;
import org.onosproject.net.HostId;
import org.onosproject.net.Path;
import org.onosproject.net.PortNumber;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.device.PortStatistics;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.FlowRuleService;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.TrafficTreatment;
import org.onosproject.net.flowobjective.DefaultForwardingObjective;
import org.onosproject.net.flowobjective.FlowObjectiveService;
import org.onosproject.net.flowobjective.ForwardingObjective;
import org.onosproject.net.host.HostService;
import org.onosproject.net.packet.InboundPacket;
import org.onosproject.net.packet.PacketContext;
import org.onosproject.net.packet.PacketPriority;
import org.onosproject.net.packet.PacketProcessor;
import org.onosproject.net.packet.PacketService;
import org.onosproject.net.topology.TopologyEvent;
import org.onosproject.net.topology.TopologyListener;
import org.onosproject.net.topology.TopologyService;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO:
// add suport for link failures
// add optional support for ipv6
// add relevant properties as timeout, priorities, ipv6 forwarding, inherit flow treatment, packet out of pipeline table
@Component(immediate = true, service = ReactiveFlexForwarding.class)
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
  protected FlexcommService flexcommService;

  @Reference(cardinality = ReferenceCardinality.MANDATORY)
  protected EnergyService energyService;

  private final PacketProcessor processor = new InternalPacketProcessor();
  private final TopologyListener topologyListener = new InternalTopologyListener();
  private final FlexcommListener flexcommListener = new InternalFlexcommListener();

  private ExecutorService eventExecutor;
  private FlexWeightCalc weigher;

  protected ApplicationId appId;

  @Activate
  protected void activate() {
    appId = coreService.registerApplication(APP_NAME);

    weigher = new FlexWeightCalc();

    eventExecutor = Executors.newFixedThreadPool(4, groupedThreads("onos/flexcomm/fwd", "weightcalc"));
    flexcommService.addListener(flexcommListener);
    packetService.addProcessor(processor, PacketProcessor.director(2));
    topologyService.addListener(topologyListener);

    TrafficSelector.Builder selector = DefaultTrafficSelector.builder();
    selector.matchEthType(Ethernet.TYPE_IPV4);
    packetService.requestPackets(selector.build(), PacketPriority.REACTIVE, appId);

    log.info("Started", appId.id());
  }

  @Deactivate
  protected void deactivate() {
    TrafficSelector.Builder selector = DefaultTrafficSelector.builder();
    selector.matchEthType(Ethernet.TYPE_IPV4);
    packetService.cancelPackets(selector.build(), PacketPriority.REACTIVE, appId);

    packetService.removeProcessor(processor);
    topologyService.removeListener(topologyListener);
    flexcommService.removeListener(flexcommListener);
    eventExecutor.shutdown();

    flowRuleService.removeFlowRulesById(appId);

    log.info("Stopped", appId.id());
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
    } else if (proto == IPv4.PROTOCOL_TCP) {
      UDP udpPacket = (UDP) ipv4Packet.getPayload();
      selectorBuilder.matchUdpSrc(TpPort.tpPort(udpPacket.getSourcePort()))
          .matchUdpDst(TpPort.tpPort(udpPacket.getDestinationPort()));
    }

    TrafficTreatment treatment = DefaultTrafficTreatment.builder().setOutput(portNumber).build();

    ForwardingObjective forwardingObjective = DefaultForwardingObjective.builder().withSelector(selectorBuilder.build())
        .withTreatment(treatment).withPriority(10).withFlag(ForwardingObjective.Flag.VERSATILE).fromApp(appId)
        .makeTemporary(10).add();

    flowObjectiveService.forward(context.inPacket().receivedFrom().deviceId(), forwardingObjective);
    packetOut(context, portNumber);
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

      if (ethPkt == null) {
        return;
      }

      HostId id = HostId.hostId(ethPkt.getDestinationMAC());

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

  // TODO: handle link fails
  private class InternalTopologyListener implements TopologyListener {

    @Override
    public void event(TopologyEvent event) {
      // TODO Auto-generated method stub
      throw new UnsupportedOperationException("Unimplemented method 'event'");
    }

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

        weigher.setWeight(deviceId, new FlexWeight(received - sent, value < 0 ? 1 : 0));
      }
    }

  }

}
