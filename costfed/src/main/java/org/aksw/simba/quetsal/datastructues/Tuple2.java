package org.aksw.simba.quetsal.datastructues;

public class Tuple2<T0, T1> {
    T0 value0;
    T1 value1;
    
    public Tuple2(T0 value0, T1 value1) {
        super();
        this.value0 = value0;
        this.value1 = value1;
    }
    
    public T0 getValue0() {
        return value0;
    }
    public void setValue0(T0 value0) {
        this.value0 = value0;
    }
    public T1 getValue1() {
        return value1;
    }
    public void setValue1(T1 value1) {
        this.value1 = value1;
    }

    @Override
    public String toString() {
        return "Tuple3 [value0=" + value0 + ", value1=" + value1 + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((value0 == null) ? 0 : value0.hashCode());
        result = prime * result + ((value1 == null) ? 0 : value1.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        @SuppressWarnings("rawtypes")
        Tuple3 other = (Tuple3) obj;
        if (value0 == null) {
            if (other.value0 != null)
                return false;
        } else if (!value0.equals(other.value0))
            return false;
        if (value1 == null) {
            if (other.value1 != null)
                return false;
        } else if (!value1.equals(other.value1))
            return false;
        return true;
    }
}
