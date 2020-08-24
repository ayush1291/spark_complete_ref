package chap8;

import java.io.Serializable;

public class MapModel implements Comparable<MapModel>, Serializable {

  public Object imsi;
  public Object networkTripId;
  public Object partnernetworkid;
  public Object partnernetworkpreference;
  public Object steeringcos;

  public Long eventtime;

  public MapModel(Object imsi, Object networkTripId, Object partnernetworkid, Object partnernetworkpreference, Object steeringcos,
      Long eventtime) {
    this.imsi = imsi;
    this.networkTripId = networkTripId;
    this.partnernetworkid = partnernetworkid;
    this.partnernetworkpreference = partnernetworkpreference;
    this.steeringcos = steeringcos;
    this.eventtime = eventtime;
  }

  @Override
  public int compareTo(MapModel o) {
    if (this.eventtime > o.eventtime)
      return 1;
    if (this.eventtime == o.eventtime)
      return 0;
    return -1;
  }

  public Object getImsi() {
    return imsi;
  }

  public void setImsi(Object imsi) {
    this.imsi = imsi;
  }

  public Object getNetworkTripId() {
    return networkTripId;
  }

  public void setNetworkTripId(Object networkTripId) {
    this.networkTripId = networkTripId;
  }

  public Object getPartnernetworkid() {
    return partnernetworkid;
  }

  public void setPartnernetworkid(Object partnernetworkid) {
    this.partnernetworkid = partnernetworkid;
  }

  public Object getPartnernetworkpreference() {
    return partnernetworkpreference;
  }

  public void setPartnernetworkpreference(Object partnernetworkpreference) {
    this.partnernetworkpreference = partnernetworkpreference;
  }

  public Object getSteeringcos() {
    return steeringcos;
  }

  public void setSteeringcos(Object steeringcos) {
    this.steeringcos = steeringcos;
  }

  public Long getEventtime() {
    return eventtime;
  }

  public void setEventtime(Long eventtime) {
    this.eventtime = eventtime;
  }
}
