package org.uni.potsdam.p1.types;

import java.util.Objects;

// what if an operator arrives with multiple messages from the same
// originating pattern? The same pattern would still receive a
// result, so not an issue?
public class TargetMember {
    public final String operator;
    public final String pattern;
    public boolean hasBeenServed;
    public final double registeredSelectivity;
    public final long timestamp;

    public TargetMember(String op, String pattern, long currentTime, double aggSel) {
        operator = op;
        this.registeredSelectivity = aggSel;
        this.pattern = pattern;
        this.timestamp = currentTime;
    }
    
    public String getTargetOperator() {
        return this.operator;
    }

    public String getTargetPattern() {
        return this.pattern;
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public double getRegisteredSelectivity() {
        return this.registeredSelectivity;
    }

    @Override
    public boolean equals(Object o){
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TargetMember compared = (TargetMember) o;
        return Objects.equals(operator, compared.operator) &&
            Objects.equals(pattern, compared.pattern) &&
            Objects.equals(timestamp, compared.timestamp);
    }

    @Override
    public int hashCode(){
        return Objects.hash(operator, pattern, timestamp);
    }

}
