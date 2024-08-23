package org.uni.potsdam.p1;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

public abstract class Rate<T extends Number> implements Serializable {

  protected Map<String, T> events;
  protected T total;

  public abstract InputRate parse(String s);

  public abstract void countEvent(String key, int count);

  public T getEvent(String key) {
    return events.get(key);
  }

  public Collection<T> getAllEvents() {
    return events.values();
  }

  public abstract int getTotal();

  public abstract boolean isValid(String key, double prob);

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Rate<?> rate = (Rate<?>) o;
    return Objects.equals(events, rate.events) && Objects.equals(total, rate.total);
  }

  @Override
  public int hashCode() {
    return Objects.hash(events, total);
  }

  public Map<String, T> getEvents() {
    return events;
  }

  public void setEvents(Map<String, T> events) {
    this.events = events;
  }

  public void setTotal(T total) {
    this.total = total;
  }
}
