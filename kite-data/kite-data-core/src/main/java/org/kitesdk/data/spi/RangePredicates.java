/*
 * Copyright 2013 Cloudera.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kitesdk.data.spi;

import com.google.common.base.Objects;
import javax.annotation.Nullable;

public class RangePredicates {

  private static final RangePredicate UNDEFINED_RANGE_PREDICATE = new RangePredicate() {
    @Override
    public boolean apply(@Nullable Marker input) {
      return false;
    }
    @Override
    public MarkerRange getRange() {
      return MarkerRange.UNDEFINED;
    }
  };

  public static RangePredicate undefined() {
    return UNDEFINED_RANGE_PREDICATE;
  }

  public static RangePredicate all(final MarkerComparator markerComparator) {
    return new AllRangePredicate(markerComparator);
  }

  private static class MarkerRangePredicate implements RangePredicate {

    private final MarkerRange range;

    public MarkerRangePredicate(MarkerRange range) {
      this.range = range;
    }

    @Override
    public boolean apply(@Nullable Marker input) {
      return range.contains(input);
    }

    @Override
    public MarkerRange getRange() {
      return range;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof MarkerRangePredicate)) {
        return false;
      }

      MarkerRangePredicate that = (MarkerRangePredicate) o;
      return (Objects.equal(this.range, that.range));
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(range);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("range", range)
          .toString();
    }
  }

  private static class AllRangePredicate extends MarkerRangePredicate {

    public AllRangePredicate(MarkerComparator markerComparator) {
      super(new MarkerRange(markerComparator));
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("range", getRange())
          .toString();

    }
  }

  private static class EqualityRangePredicate extends MarkerRangePredicate {

    private final String name;
    private final Object value;

    public EqualityRangePredicate(MarkerComparator markerComparator, String name, Object value) {
      super(new MarkerRange(markerComparator));
      this.name = name;
      this.value = value;
    }

    @Override
    public boolean apply(@Nullable Marker input) {
      return input != null && input.has(name) && Objects.equal(input.get(name), value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof EqualityRangePredicate)) {
        return false;
      }

      EqualityRangePredicate that = (EqualityRangePredicate) o;
      return (Objects.equal(this.name, that.name) &&
        Objects.equal(this.value, that.value) &&
        Objects.equal(this.getRange(), that.getRange()));
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, value, getRange());
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("name", name)
          .add("value", value)
          .add("range", getRange())
          .toString();
    }
  }

  private static class FieldSetRangePredicate extends MarkerRangePredicate {

    private final String name;

    public FieldSetRangePredicate(MarkerComparator markerComparator, String name) {
      super(new MarkerRange(markerComparator));
      this.name = name;
    }

    @Override
    public boolean apply(@Nullable Marker input) {
      return input != null && input.has(name);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof FieldSetRangePredicate)) {
        return false;
      }

      FieldSetRangePredicate that = (FieldSetRangePredicate) o;
      return (Objects.equal(this.name, that.name) &&
          Objects.equal(this.getRange(), that.getRange()));
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(name, getRange());
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("name", name)
          .add("range", getRange())
          .toString();
    }
  }

  private static class AndRangePredicate implements RangePredicate {

    private final RangePredicate p1;
    private final RangePredicate p2;
    private final MarkerRange range;

    public AndRangePredicate(RangePredicate p1, RangePredicate p2) {
      this.p1 = p1;
      this.p2 = p2;
      this.range = p1.getRange().intersection(p2.getRange());
    }

    @Override
    public boolean apply(@Nullable Marker input) {
      return p1.apply(input) && p2.apply(input);
    }

    @Override
    public MarkerRange getRange() {
      return range;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof AndRangePredicate)) {
        return false;
      }

      AndRangePredicate that = (AndRangePredicate) o;
      return (Objects.equal(this.p1, that.p1) &&
          Objects.equal(this.p2, that.p2) &&
          Objects.equal(this.range, that.range));
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(p1, p2, range);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("predicate1", p1)
          .add("predicate2", p2)
          .add("range", range)
          .toString();
    }
  }

  private static class OrRangePredicate implements RangePredicate {

    private final RangePredicate p1;
    private final RangePredicate p2;
    private final MarkerRange range;

    public OrRangePredicate(RangePredicate p1, RangePredicate p2) {
      this.p1 = p1;
      this.p2 = p2;
      this.range = p1.getRange().span(p2.getRange());
    }

    @Override
    public boolean apply(@Nullable Marker input) {
      return p1.apply(input) || p2.apply(input);
    }

    @Override
    public MarkerRange getRange() {
      return range;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof OrRangePredicate)) {
        return false;
      }

      OrRangePredicate that = (OrRangePredicate) o;
      return (Objects.equal(this.p1, that.p1) &&
          Objects.equal(this.p2, that.p2) &&
          Objects.equal(this.range, that.range));
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(p1, p2, range);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("predicate1", p1)
          .add("predicate2", p2)
          .add("range", range)
          .toString();
    }
  }

  private static class NotRangePredicate implements RangePredicate {

    private final RangePredicate p;
    private final MarkerRange range;

    public NotRangePredicate(RangePredicate p) {
      this.p = p;
      this.range = p.getRange().complement();
    }

    @Override
    public boolean apply(@Nullable Marker input) {
      return !p.apply(input);
    }

    @Override
    public MarkerRange getRange() {
      return range;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }

      if (!(o instanceof NotRangePredicate)) {
        return false;
      }

      NotRangePredicate that = (NotRangePredicate) o;
      return (Objects.equal(this.p, that.p) &&
          Objects.equal(this.range, that.range));
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(p, range);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("predicate", p)
          .add("range", range)
          .toString();
    }
  }

  public static RangePredicate from(MarkerComparator comparator, Marker start) {
    return new MarkerRangePredicate(new MarkerRange(comparator).from(start));
  }

  public static RangePredicate fromAfter(MarkerComparator comparator, Marker start) {
    return new MarkerRangePredicate(new MarkerRange(comparator).fromAfter(start));
  }

  public static RangePredicate to(MarkerComparator comparator, Marker end) {
    return new MarkerRangePredicate(new MarkerRange(comparator).to(end));
  }

  public static RangePredicate toBefore(MarkerComparator comparator, Marker end) {
    return new MarkerRangePredicate(new MarkerRange(comparator).toBefore(end));
  }

  public static RangePredicate of(MarkerComparator comparator, Marker partial) {
    return new MarkerRangePredicate(new MarkerRange(comparator).of(partial));
  }

  public static RangePredicate with(MarkerComparator comparator, String name, Object value) {
    return new EqualityRangePredicate(comparator, name, value);
  }

  public static RangePredicate with(MarkerComparator comparator, String name) {
    return new FieldSetRangePredicate(comparator, name);
  }

  public static RangePredicate from(MarkerComparator comparator, String name, Object value) {
    Marker marker = new Marker.Builder(name, value).build();
    return new MarkerRangePredicate(new MarkerRange(comparator).from(marker));
  }

  public static RangePredicate fromAfter(MarkerComparator comparator, String name, Object value) {
    Marker marker = new Marker.Builder(name, value).build();
    return new MarkerRangePredicate(new MarkerRange(comparator).fromAfter(marker));
  }

  public static RangePredicate to(MarkerComparator comparator, String name, Object value) {
    Marker marker = new Marker.Builder(name, value).build();
    return new MarkerRangePredicate(new MarkerRange(comparator).to(marker));
  }

  public static RangePredicate toBefore(MarkerComparator comparator, String name, Object value) {
    Marker marker = new Marker.Builder(name, value).build();
    return new MarkerRangePredicate(new MarkerRange(comparator).toBefore(marker));
  }

  public static RangePredicate and(RangePredicate p1, RangePredicate p2) {
    return new AndRangePredicate(p1, p2);
  }

  public static RangePredicate or(RangePredicate p1, RangePredicate p2) {
    return new OrRangePredicate(p1, p2);
  }

  public static RangePredicate not(RangePredicate p) {
    return new NotRangePredicate(p);
  }

}
